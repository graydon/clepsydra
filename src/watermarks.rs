// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{GlobalTime, PeerID};
use pergola::{LatticeElt, MaxDef, MaxUnitDefault, Tuple2};
/**
 * All nodes track and propagate a pair of watermarks. These are
 * monotonically increasing global timestamps.
 *
 * The visibility watermark supports a so-called "asynchronous concurrency
 * control (ACC) protocol", which runs transactions in 2 phases:
 *
 *   1. "S-phase": "storage phase" in which blind writes happen. These write
 *      deterministic _thunks_ (called "functors" in the paper) into the
 *      database at globally unique versions/timestamps, and replicate them to
 *      other peers for fault-tolerance. This phase establishes the global order
 *      of transactions, but does not _run_ them.
 *
 *   2. "E-phase": "execute phase" in which stored thunks are forced, in order
 *      to produce any _reads_ (to return to the client) and/or write final
 *      values back to the version allocated to the thunk. This phase is gated
 *      by collective commitment to advancing a timestamp watermark, and runs
 *      those transactions beneath the watermark against a world in which the
 *      meaning of _any earlier version_ has been fixed by that advance (if not
 *      yet computed -- thunks may force one another recursively).
 *
 * Concurrency control consists solely of a monotonically increasing watermark
 * that gates the transition of transactions from S-phase to E-phase: neither
 * phase needs any other concurrency control mechanisms inside of it, and can
 * execute in full parallelism; the only thing worth adding is a bit of (local
 * inter-thread) coordination on the forcing of each thunk, to avoid wasting CPU
 * by forcing the same thunk multiple times in parallel if it's needed as input
 * to multiple transactions.
 *
 * In addition, the progress of _replication_ is also tracked using a watermark,
 * the "replication watermark", and versions below it can be read off from any
 * replica (eg. a local replica) without requiring a quorum read. This gives us
 * horizontal scaling for snapshot reads in the past.
 *
 */
use serde::{Deserialize, Serialize};

// GlobalTimes are themselves reasoned about in a few different forms, and we
// want to carry as much information about those forms as we can in the type
// system, because it's important not to get them confused!

/// A server-specific visibility watermark, denoting the minimum of the server's
/// issued transactions that are still being replicated to a quorum.
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Svw(pub GlobalTime);

/// A server-specific replication watermark, denoting the minimum of the server's
/// issued transactions that are still being replicated to all peers.
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Srw(pub GlobalTime);

/// A server-specific delayed-evaluation watermark, denoting the minimum of the
/// [crate::KeyVer]s held in the server's [crate::Store] that remain in state
/// [crate::Entry::Delayed] (not-yet-evaluated).
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Sdw(pub GlobalTime);

/// A group-wide visibility watermark, denoting the minimum of _all_ Svws
/// observed across the group of servers.
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VWatermark(pub GlobalTime);

/// A group-wide replication watermark, denoting the minimum of _all_ Srws
/// observed across the group of servers.
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RWatermark(pub GlobalTime);

// This says "GlobalTime::default() is the lattice unit for pergola::MaxDef<Svw>".
impl MaxUnitDefault for Svw {}

// This says "GlobalTime::default() is the lattice unit for pergola::MaxDef<Srw>".
impl MaxUnitDefault for Srw {}

/// A structure carrying the group-wide watermarks, the first of which gates tx
/// execution phase, the second of which switches replicated-read strategies.
#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupWatermarks {
    /// Watermark below which all transactions have completed their write-
    /// only operations (S-phase), and can thereby enter E-phase.
    pub visibility_watermark: VWatermark,

    /// Watermark below which all transactions have _fully replicated_ their
    /// write-only (S-phase) operations on all participant replicas. Read-only
    /// operations for versions below replication_watermark can access the value
    /// directly from any replica; until then they must use quorum reads.
    pub replication_watermark: RWatermark,
}

impl GroupWatermarks {
    pub fn new_from_peer_zero_time(peer: PeerID) -> GroupWatermarks {
        let z = GlobalTime::time_zero_for(peer);
        GroupWatermarks {
            visibility_watermark: VWatermark(z.clone()),
            replication_watermark: RWatermark(z.clone()),
        }
    }
}

pub type ServerWatermarksLD = Tuple2<MaxDef<Svw>, MaxDef<Srw>>;
pub type ServerWatermarksLE = LatticeElt<ServerWatermarksLD>;
pub trait ServerWatermarksLEExt {
    fn new(svw: Svw, srw: Srw) -> Self;
    fn new_from_peer_zero_time(p: PeerID) -> Self;
    fn svw(&self) -> &LatticeElt<MaxDef<Svw>>;
    fn svw_mut(&mut self) -> &mut LatticeElt<MaxDef<Svw>>;
    fn srw(&self) -> &LatticeElt<MaxDef<Srw>>;
    fn srw_mut(&mut self) -> &mut LatticeElt<MaxDef<Srw>>;
}

impl ServerWatermarksLEExt for LatticeElt<ServerWatermarksLD> {
    fn new(svw: Svw, srw: Srw) -> Self {
        LatticeElt::from((svw.into(), srw.into()))
    }
    fn new_from_peer_zero_time(p: PeerID) -> Self {
        let z = GlobalTime::time_zero_for(p);
        Self::new(Svw(z), Srw(z))
    }
    fn svw(&self) -> &LatticeElt<MaxDef<Svw>> {
        &self.value.0
    }
    fn svw_mut(&mut self) -> &mut LatticeElt<MaxDef<Svw>> {
        &mut self.value.0
    }
    fn srw(&self) -> &LatticeElt<MaxDef<Srw>> {
        &self.value.1
    }
    fn srw_mut(&mut self) -> &mut LatticeElt<MaxDef<Srw>> {
        &mut self.value.1
    }
}
