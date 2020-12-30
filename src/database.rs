// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{
    agreement, network, Clock, Error, ExtVal, GlobalTime, GroupWatermarks, KeyVer, Lang, PeerID,
    RWatermark, Sdw, Store, Svw, SyncBoxFuture, TidMgr, Txn, VWatermark,
};
use async_std::{
    sync::{Arc, Condvar, Mutex, RwLock},
    task,
};
use futures::{stream::FuturesUnordered, Future, TryStreamExt};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tracing::{debug, debug_span, instrument, Instrument};

/// Main object that clients instantiate. Encapsulates the remainder of the
/// system. Clients need to provide a [Clock] and [Store], as well as some
/// number of async IO connections to other peers.
pub struct Database<L: Lang, S: Store<L>> {
    pub self_id: PeerID,

    /// The current tidmgr, used to hand out transaction IDs / timestamps, and track
    /// the set of outstanding timestamps in various phases, and their minima.
    pub(crate) tidmgr: Arc<RwLock<TidMgr>>,

    // The Condvar here is used to wait on the completion of the S-phase
    // of a transaction. When the GroupWatermarks advances, transactions
    // waiting on it are woken and can consider whether it's time for them
    // keep waiting or proceed to E-phase.
    pub(crate) group_wm: Arc<(Mutex<GroupWatermarks>, Condvar)>,

    /// The current concorde state machine, used to propagate the watermark gossip.
    pub(crate) participant: Arc<RwLock<agreement::Participant>>,

    /// The map of network connections, served by network service loops.
    pub(crate) connections: Arc<RwLock<HashMap<PeerID, network::Connection<L>>>>,

    // A set of locks we hold during RPC demand-evaluation of a keyver to avoid
    // reentry; theoretically harmless to re-evaluate but since this is a lazy
    // system it can easily create multiplicative churn that's better avoided by
    // waiting for an existing evaluator call to finish.
    //
    // Paired with a usize to count the number of lock accesses; once every
    // LOCK_GC_FREQUENCY we purge old eval-locks below the delay watermark
    // since they will never be accessed again.
    pub(crate) eval_locks: Arc<Mutex<(usize, HashMap<KeyVer<L>, Arc<Mutex<()>>>)>>,

    // Finally the store itself, where we read and write Store::Entries.
    pub(crate) store: Arc<RwLock<S>>,
}

impl<L: Lang, S: Store<L>> Clone for Database<L, S> {
    fn clone(&self) -> Self {
        Database {
            self_id: self.self_id.clone(),
            tidmgr: self.tidmgr.clone(),
            group_wm: self.group_wm.clone(),
            participant: self.participant.clone(),
            connections: self.connections.clone(),
            eval_locks: self.eval_locks.clone(),
            store: self.store.clone(),
        }
    }
}

impl<L: Lang, S: Store<L>> Database<L, S> {
    /// Construct a new Database for a given [Clock] and [Store]. The [PeerID]
    /// should be unique among peers in a group, and all peers should use
    /// compatible and roughly-synchronized clocks.
    pub fn new(self_id: PeerID, clock: Box<dyn Clock>, store: S) -> Self {
        let gwm = GroupWatermarks::new_from_peer_zero_time(self_id);
        Database {
            self_id: self_id.clone(),
            tidmgr: Arc::new(RwLock::new(TidMgr::new(self_id, clock))),
            group_wm: Arc::new((Mutex::new(gwm), Condvar::new())),
            participant: Arc::new(RwLock::new(concorde::Participant::new(self_id))),
            connections: Arc::new(RwLock::new(HashMap::new())),
            eval_locks: Arc::new(Mutex::new((0, HashMap::new()))),
            store: Arc::new(RwLock::new(store)),
        }
    }

    const LOCK_GC_FREQUENCY: usize = 128;

    async fn get_delayed_watermark(&self) -> Sdw {
        match self.store.read().await.get_delayed_watermark() {
            None => Sdw(self.tidmgr.write().await.create_timestamp()),
            Some(sdw) => sdw,
        }
    }

    pub(crate) async fn get_keylock(&self, kv: &KeyVer<L>) -> Arc<Mutex<()>> {
        let mut guard = self.eval_locks.lock().await;

        // Maybe GC locks before issuing new one.
        guard.0 += 1;
        if guard.0 > Self::LOCK_GC_FREQUENCY {
            let pre_count = guard.1.len();
            let Sdw(min_delayed_ver) = self.get_delayed_watermark().await;
            guard.1.retain(|k, _| k.ver >= min_delayed_ver);
            let post_count = guard.1.len();
            debug!(
                "GC'ed {:?} key-locks, {:?} remaining",
                pre_count - post_count,
                post_count
            );
            guard.0 = 0;
        }

        guard
            .1
            .entry(kv.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    #[instrument(skip(self, quorum_futs))]
    async fn finalize_quorum_write_or_abort<QF, RF>(
        &self,
        ver: GlobalTime,
        quorum_futs: FuturesUnordered<QF>,
    ) -> Result<(), Error>
    where
        QF: Future<Output = Result<RF, Error>>,
        RF: Future<Output = Result<Vec<PeerID>, Error>> + Send + Sync + 'static,
    {
        // First we wait for all the quorum-{write,abort} futures to yield their
        // residual-{write,abort} futures. If anything goes wrong here we'll
        // return an Error and quorum-{write,abort} will not have succeeded.
        let residual_futs: FuturesUnordered<_> = quorum_futs.try_collect().await?;

        // Then we inform tidmgr that we've successfully made a quorum-{write,abort} so it
        // can advance the server-local visibility watermark Svw.
        debug!(
            "marking {:?} as stored (un-gating local server visibility watermark)",
            ver
        );
        self.tidmgr.write().await.stored(ver);

        // Finally we spawn a sub-task that will drain the residual futures and
        // advance the tidmgr's server-local replication watermark Srw when we
        // have finished the {write,abort}-all phase.
        let this = self.clone();
        task::spawn(async move {
            // TODO: the paper gives no guidance on what to do if there's a
            // failure while resolving the residual futures when finishing
            // up full replication.
            let _ = residual_futs.try_collect::<Vec<_>>().await;
            debug!(
                "marking {:?} as fully replicated (un-gating local server repilcation watermark)",
                ver
            );
            this.tidmgr.write().await.fully_replicated(ver);
        });
        Ok(())
    }

    /// Main entrypoint for clients submitting transactions. Call this and await
    /// the response. If the response is `Ok`, a transaction containing `stmt`
    /// and `vals` was successfully replicated to a quorum of peers and executed
    /// at a specific [GlobalTime] in the consensus sequential order. The result
    /// will contain the that timestamp as a map populated with any keys and
    /// their values (evaluated at the transaction's time) specified by
    /// `Lang::get_eval_set(stmt)`.
    #[instrument(skip(self))]
    pub fn coordinate(
        &self,
        stmt: L::Stmt,
        vals: Vec<L::Val>,
    ) -> SyncBoxFuture<Result<(GlobalTime, BTreeMap<L::Key, ExtVal<L>>), Error>> {
        let this = self.clone();
        Box::pin(async move {
            let ver = this
                .tidmgr
                .write()
                .await
                .create_watermark_and_start_s_phase();
            let span = debug_span!("coordinate", peer=?this.self_id, tid=?ver);
            this.coordinate_(ver, stmt, vals).instrument(span).await
        })
    }

    // From paper -- Algorithm 1, procedure 'Coordinate'
    // Called from client initiating a new txn.
    async fn coordinate_(
        &self,
        ver: GlobalTime,
        stmt: L::Stmt,
        vals: Vec<L::Val>,
    ) -> Result<(GlobalTime, BTreeMap<L::Key, ExtVal<L>>), Error> {
        let txn = Txn {
            time: ver.clone(),
            stmt,
            vals,
        };
        debug!("begin coordinate");
        {
            debug!("begin S-phase");
            let write_set = L::get_write_set(&txn.stmt);
            let quorum_write_futs: FuturesUnordered<_> = write_set
                .iter()
                .map(|(k, e)| {
                    self.write(
                        KeyVer {
                            key: k.clone(),
                            ver,
                        },
                        e.clone(),
                        txn.vals.clone(),
                    )
                })
                .collect();
            match self
                .finalize_quorum_write_or_abort(ver, quorum_write_futs)
                .await
            {
                Err(_) => {
                    // We had a failure during quorum-write, so we now have to
                    // quorum-abort.
                    debug!("begin abort");
                    let quorum_abort_futs: FuturesUnordered<_> = write_set
                        .keys()
                        .map(|k| {
                            self.abort_txn(KeyVer {
                                key: k.clone(),
                                ver,
                            })
                        })
                        .collect();

                    // TODO: the paper is not at all clear about what we should
                    // do if there's an error during quorum-abort, besides "this
                    // cannot happen". For now we just ignore them.
                    let _ = self
                        .finalize_quorum_write_or_abort(ver, quorum_abort_futs)
                        .await;
                    return Err(Error::TxnAbort);
                }
                Ok(()) => (),
            }
            debug!("end S-phase");
        }
        // In the paper this step is a bit unclear/incoherent, and
        // the actual call to "execute()" is launched from publish().
        // This doesn't really make sense in our implementation, so we
        // pause here until it's time and then call execute() ourselves.
        // publish() un-gates us instead. It's _fairly_ close to the same.
        self.wait_for_visibility_watermark(txn.time).await;
        self.execute(txn, ver).await
    }

    pub(crate) async fn wait_for_visibility_watermark(&self, ver: GlobalTime) {
        let (lock, cvar) = &*self.group_wm;
        let tsvw = VWatermark(ver);
        debug!("waiting for VWatermark to advance past {:?}", ver);
        cvar.wait_until(lock.lock().await, |gw| tsvw < gw.visibility_watermark)
            .await;
        debug!("Vwatermark advanced past {:?}", ver);
    }

    #[instrument(skip(self))]
    fn execute(
        &self,
        tx: Txn<L>,
        ts: GlobalTime,
    ) -> SyncBoxFuture<Result<(GlobalTime, BTreeMap<L::Key, ExtVal<L>>), Error>> {
        let this = self.clone();
        Box::pin(async move { this.execute_(tx, ts).await })
    }

    // From paper -- Algorithm 1, procedure 'Execute'
    // Called from publish, below.
    async fn execute_(
        &self,
        tx: Txn<L>,
        ts: GlobalTime,
    ) -> Result<(GlobalTime, BTreeMap<L::Key, ExtVal<L>>), Error> {
        debug!("begin E-phase");
        let write_set = L::get_write_set(&tx.stmt);
        let read_set: BTreeSet<L::Key> = write_set
            .iter()
            .map(|(_, expr)| L::get_read_set(expr))
            .flatten()
            .collect();

        let eval_set = L::get_eval_set(&tx.stmt);
        let read_set: BTreeSet<L::Key> = read_set.union(&eval_set).cloned().collect();
        let ts_pre = ts.prev_event();

        debug!("acquiring {:?} eval-locks", write_set.len());
        let mut eval_arcs = Vec::new();
        for k in write_set.keys() {
            let kv = KeyVer {
                key: k.clone(),
                ver: ts,
            };
            let arc = self.get_keylock(&kv).await;
            eval_arcs.push(arc);
        }
        let mut eval_locks = Vec::new();
        for arc in eval_arcs.iter() {
            eval_locks.push(arc.lock().await);
        }

        debug!("retrieving read set from previous timestamp {:?}", ts_pre);
        let read_futs: FuturesUnordered<_> = read_set
            .iter()
            .map(|k| {
                self.read(KeyVer {
                    key: k.clone(),
                    ver: ts_pre,
                })
            })
            .collect();
        let env: BTreeMap<_, _> = read_futs.try_collect().await?;
        let mut out = BTreeMap::new();
        debug!("evaluating exprs into write set");
        for (key, expr) in write_set {
            out.insert(key, L::eval_expr(&expr, &tx.vals, &env));
        }
        debug!("putting write set");
        self.put(ts, &out).await?;
        // Finally return what the query asked for as a return value.
        debug!("calculating final value");
        let mut res = BTreeMap::new();
        for k in eval_set {
            match out.get(&k) {
                Some(v) => {
                    res.insert(k, v.clone());
                }
                None => match env.get(&k) {
                    Some(v) => {
                        res.insert(k, v.clone());
                    }
                    None => return Err(Error::MissingKey),
                },
            }
        }
        debug!("end E-phase");
        Ok((ts, res))
    }

    // Testing-only interface; bypasses consensus!
    pub(crate) async fn publish_an_hour_from_now(&self) -> Svw {
        let mut ts = self.tidmgr.write().await.create_timestamp();
        ts.milli_secs += 3600 * 1000;
        let gwm = GroupWatermarks {
            visibility_watermark: VWatermark(ts),
            replication_watermark: RWatermark(ts),
        };
        self.publish(gwm).await
    }
}
