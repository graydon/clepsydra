// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use clepsydra::{
    Clock, Database, Entry, ExtVal, GlobalTime, KeyVer, Lang, PeerID, Sdw, Store, TestClock,
};

use async_std::task;
use duplexify::Duplex;
use futures::{stream::FuturesUnordered, StreamExt};
use tracing::{info, warn};

#[cfg(feature = "tracy")]
use tracing_subscriber::{self, layer::SubscriberExt};

#[cfg(feature = "tracy")]
use tracing_tracy;

use serde::{Deserialize, Serialize};
use sluice::pipe::{pipe, PipeReader, PipeWriter};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum TExpr {
    Add(Box<TExpr>, Box<TExpr>),
    Var(String),
    Lit(u64),
}

impl Default for TExpr {
    fn default() -> Self {
        TExpr::Lit(0)
    }
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum TStmt {
    Set(String, TExpr),
    Get(String),
    Pass,
}

impl Default for TStmt {
    fn default() -> Self {
        TStmt::Pass
    }
}

#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct TLang;
impl Lang for TLang {
    type Key = String;
    type Val = u64;
    type Expr = TExpr;
    type Stmt = TStmt;
    fn get_write_set(s: &Self::Stmt) -> BTreeMap<Self::Key, Self::Expr> {
        let mut m = BTreeMap::new();
        match s {
            TStmt::Get(_) | TStmt::Pass => (),
            TStmt::Set(v, e) => {
                m.insert(v.clone(), e.clone());
            }
        }
        m
    }
    fn get_read_set(e: &Self::Expr) -> BTreeSet<Self::Key> {
        fn get_vars(s: &mut BTreeSet<String>, x: &TExpr) {
            match x {
                TExpr::Add(a, b) => {
                    get_vars(s, &**a);
                    get_vars(s, &**b);
                }
                TExpr::Var(v) => {
                    s.insert((*v).clone());
                }
                TExpr::Lit(_) => (),
            }
        }
        let mut s = BTreeSet::new();
        get_vars(&mut s, e);
        s
    }
    fn get_eval_set(s: &Self::Stmt) -> BTreeSet<Self::Key> {
        let mut m = BTreeSet::new();
        match s {
            TStmt::Set(_, _) | TStmt::Pass => (),
            TStmt::Get(v) => {
                m.insert(v.clone());
            }
        }
        m
    }
    fn eval_expr(
        e: &Self::Expr,
        _vals: &[Self::Val],
        env: &BTreeMap<Self::Key, ExtVal<Self>>,
    ) -> ExtVal<Self> {
        match e {
            TExpr::Add(a, b) => match (
                Self::eval_expr(&**a, &[], env),
                Self::eval_expr(&**b, &[], env),
            ) {
                (ExtVal::Defined(p), ExtVal::Defined(q)) => ExtVal::Defined(p + q),
                _ => ExtVal::Initial,
            },
            TExpr::Var(v) => match env.get(v) {
                None => ExtVal::Initial,
                Some(x) => x.clone(),
            },
            TExpr::Lit(x) => ExtVal::Defined(*x),
        }
    }
}

struct TStore<L: Lang> {
    map: BTreeMap<KeyVer<L>, Entry<L>>,
    delay_counts: BTreeMap<GlobalTime, usize>,
}
impl<L: Lang> TStore<L> {
    pub fn new() -> Self {
        TStore {
            map: BTreeMap::new(),
            delay_counts: BTreeMap::new(),
        }
    }
}
impl<L: Lang> Store<L> for TStore<L> {
    fn get_key_at_or_before_time(&self, kv: &KeyVer<L>) -> Option<(GlobalTime, Entry<L>)> {
        let lo: Bound<KeyVer<L>> = Bound::Unbounded;
        let hi: Bound<KeyVer<L>> = Bound::Included(kv.clone());
        let lookup = self.map.range((lo, hi)).next_back();
        if let Some((k, e)) = lookup {
            if k.key == kv.key {
                return Some((k.ver, e.clone()));
            }
        }
        None
    }
    fn put_key_at_time(&mut self, kv: &KeyVer<L>, v: &Entry<L>) {
        let oldval = self.map.insert(kv.clone(), v.clone());
        let delta: isize = match (oldval, v) {
            (None, Entry::Delayed(_, _, _)) => 1,
            (None, _) => return,
            (Some(Entry::Delayed(_, _, _)), Entry::Delayed(_, _, _)) => return,
            (Some(_), Entry::Delayed(_, _, _)) => 1, // shouldn't happen?
            (Some(Entry::Delayed(_, _, _)), _) => -1,
            (Some(_), _) => return,
        };
        let entry = self.delay_counts.entry(kv.ver).or_insert(0);
        if delta > 0 {
            *entry += 1;
        } else if delta < 0 {
            if *entry == 0 {
                warn!("delay-count underflow");
            } else {
                *entry -= 1;
            }
            if *entry == 0 {
                self.delay_counts.remove(&kv.ver);
            }
        }
    }

    fn get_delayed_watermark(&self) -> Option<Sdw> {
        match self.delay_counts.iter().next() {
            None => None,
            Some((ts, _)) => Some(Sdw(*ts)),
        }
    }
}

fn mk_db(i: u64) -> Database<TLang, TStore<TLang>> {
    let clock: Box<dyn Clock> = Box::new(TestClock::new());
    let peer = PeerID(i);
    let store = TStore::new();
    Database::new(peer, clock, store)
}

type PipeRw = Duplex<PipeReader, PipeWriter>;
fn duplex_pair() -> (PipeRw, PipeRw) {
    let (a_recv, b_send) = pipe();
    let (b_recv, a_send) = pipe();
    let a_end = Duplex::new(a_recv, a_send);
    let b_end = Duplex::new(b_recv, b_send);
    (a_end, b_end)
}

async fn connect_dbs<L: Lang, S: Store<L>>(a: &mut Database<L, S>, b: &mut Database<L, S>) {
    let (a_end, b_end) = duplex_pair();
    a.connect(b.self_id, a_end).await;
    b.connect(a.self_id, b_end).await;
}

#[cfg(feature = "tracy")]
fn setup_tracing_subscriber() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(tracing_tracy::TracyLayer::new()),
        )
        .unwrap();
    });
}

#[cfg(not(feature = "tracy"))]
fn setup_tracing_subscriber() {
    let _ = tracing_subscriber::fmt::try_init();
}

pub fn multi_txn_test() {
    setup_tracing_subscriber();

    let mut db_1 = mk_db(1);
    let mut db_2 = mk_db(2);
    let mut db_3 = mk_db(3);
    let sz = 75;

    task::block_on(async move {
        connect_dbs(&mut db_1, &mut db_2).await;
        connect_dbs(&mut db_2, &mut db_3).await;
        connect_dbs(&mut db_1, &mut db_3).await;

        db_1.launch_workers().await;
        db_2.launch_workers().await;
        db_3.launch_workers().await;

        let var = "a".to_string();
        fn add(a: TExpr, b: TExpr) -> TExpr {
            TExpr::Add(Box::new(a), Box::new(b))
        }
        fn lit(i: u64) -> TExpr {
            TExpr::Lit(i)
        }
        let stmt_0 = TStmt::Set(var.clone(), lit(0));
        let stmt_n = TStmt::Set(var.clone(), add(lit(1), TExpr::Var(var.clone())));
        let final_stmt = TStmt::Get(var.clone());

        let mut fu = FuturesUnordered::new();
        for i in 1..sz {
            let stmt = if i == 1 {
                stmt_0.clone()
            } else {
                stmt_n.clone()
            };
            let vals = Vec::new();
            fu.push(db_1.coordinate(stmt, vals));
        }
        // db_1.publish_an_hour_from_now().await;
        while let Some(r) = fu.next().await {
            info!("resolved txn {:?}", r);
        }
        let final_res = db_1.coordinate(final_stmt, Vec::new()).await;
        info!("resolved final txn val {:?}", final_res);
    });
}
