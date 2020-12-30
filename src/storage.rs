// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{ExtVal, GlobalTime, Lang, ReplicationTag, Sdw};
use serde::{Deserialize, Serialize};

/// An [Entry] is associated with each [KeyVer] (i.e. a [Lang::Key] at some
/// [GlobalTime]) in the [Store], and is either an unevaluated expression of
/// type [Entry::Delayed] (at one of two possible [ReplicationTag] levels), or
/// an [Entry::Aborted] tombstone (if replication fails), or an [Entry::Settled]
/// value carrying a fully-evaluated [ExtVal].
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Entry<L: Lang> {
    Delayed(L::Expr, Vec<L::Val>, ReplicationTag),
    Settled(ExtVal<L>, ReplicationTag),
    Aborted,
}

/// A `KeyVer` is a [Lang::Key] augmented with a [GlobalTime]. All reads and
/// writes -- both inside the distributed protocol and against the [Store] --
/// happen in terms of `KeyVer`s.
#[derive(Clone, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyVer<L: Lang> {
    pub key: L::Key,
    pub ver: GlobalTime,
}

impl<L: Lang> std::fmt::Debug for KeyVer<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}:{:?}", self.key, self.ver))
    }
}

/// A `Store` is responsible for durable storage. Clients of the library should
/// provide an implementation and pass an instance in to the constructor of
/// [crate::Database].
///
/// Stores are presumed to model something like maps over [KeyVer]s. That is,
/// they are "multi-version" maps, supporting the "multi-version concurrency
/// control" (MVCC) protocols, of which Ocean Vista is a distributed variant.
///
/// When writing, initially an un-evaluated expression-entry of type
/// [Entry::Delayed] is written to the `Store`. Later the same `KeyVer` will be
/// _updated_ with an [Entry::Settled], when watermark time has advanced to
/// the point that it's safe to [Lang::eval_expr] the delayed expression. If at
/// any point the writing phase of the transaction aborts, an [Entry::Aborted]
/// entry will be written instead. All this happens inside the [crate::Database]
/// though; all Store has to do is write to some backing store.
pub trait Store<L: Lang>: Send + Sync + 'static {
    fn get_key_at_or_before_time(&self, kv: &KeyVer<L>) -> Option<(GlobalTime, Entry<L>)>;
    fn put_key_at_time(&mut self, kv: &KeyVer<L>, v: &Entry<L>);
    fn get_delayed_watermark(&self) -> Option<Sdw>;
}
