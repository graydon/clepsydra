// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{GlobalTime, PeerID, ServerWatermarksLE, ServerWatermarksLEExt, Srw, Svw};
use async_std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

/// Trait to support multiple sorts of clock-source.
pub trait Clock: Send + Sync {
    fn current_time(&self) -> SystemTime;
}

/// An implementation of [Clock] that calls [std::time::SystemTime::now].
pub struct RealClock;
impl Clock for RealClock {
    fn current_time(&self) -> SystemTime {
        std::time::SystemTime::now()
    }
}

/// An implementation of [Clock] that holds a shared [AtomicU64] representing
/// the current millisecond count since the epoch, that increments on each
/// call to `Clock::current_time`.
pub struct TestClock(Arc<AtomicU64>);
impl TestClock {
    pub fn new() -> Self {
        TestClock(Arc::new(AtomicU64::from(0)))
    }
}
impl Clock for TestClock {
    fn current_time(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis(self.0.fetch_add(1, Ordering::SeqCst))
    }
}
/// As in the paper: TidMgr tracks the set of "Transaction IDs" ([GlobalTime]s)
/// coordinated by the current peer. This includes the local server visibility
/// watermark ([Svw]) and server replication watermark ([Srw]) values.
pub struct TidMgr {
    /// The clock source we'll use to draw system time from.
    clock: Box<dyn Clock>,

    /// The previously-issued GlobalTime, will only advance monotonically
    /// regardless of movement of the clock.
    prev: GlobalTime,

    /// Set of all locally-issued timestamps of transactions that are writing
    /// but not yet written to a quorum, so still in S-phase. The svw is derived
    /// as the minimum value of this set (or the next-issuable timestamp if the
    /// set is empty). This is called the TSset in the paper, not very
    /// helpfully.
    pub(crate) wq_set: BTreeSet<GlobalTime>,

    /// Set of all locally-issued timestamps of transactions that are writing
    /// but not yet written to all peers, may or may not still be in S-phase.
    /// The srw is derived as the minimum value of this set (or the
    /// next-issueable timestamp if the set is empty). This set isn't named in
    /// the paper, it's left as an exercise to the reader.
    wa_set: BTreeSet<GlobalTime>,
}

impl TidMgr {
    pub(crate) fn new(self_id: PeerID, clock: Box<dyn Clock>) -> Self {
        TidMgr {
            clock,
            prev: GlobalTime::time_zero_for(self_id),
            wq_set: Default::default(),
            wa_set: Default::default(),
        }
    }

    pub(crate) fn current_time(&self) -> SystemTime {
        self.clock.current_time()
    }

    pub(crate) fn self_id(&self) -> PeerID {
        self.prev.peer
    }

    /// Issues a monotonically-increasing timestamp for the current server,
    /// adding the new timestamp to the tset for this TidMgr.
    ///
    /// Usually this moves forward with the system clock, but if the system
    /// clock stalls, goes backwards, or otherwise misbehaves, we just call
    /// `GlobalTime.next_event()` on the previously-issued timestamp, which in
    /// the worst case may increment the millisecond count if the
    /// per-millisecond event count overflows. This is the best we can do.
    pub(crate) fn create_timestamp(&mut self) -> GlobalTime {
        let now = self.clock.current_time();
        let next_millis = match now.duration_since(UNIX_EPOCH) {
            Err(_) => None,
            Ok(dur) => {
                let secs: u64 = dur.as_secs();
                let millis: u32 = dur.subsec_millis();
                // Seconds since the unix epoch here should be _way_ less
                // than 64bit; it won't exceed 33 bits in my lifetime
                // or that of anyone currently living.
                let mut ms = secs
                    .checked_mul(1000)
                    .expect("create_timestamp sec-to-ms overflow");
                ms = ms
                    .checked_add(millis as u64)
                    .expect("create_timestamp ms-addition overflow");
                Some(ms)
            }
        };
        self.prev = match next_millis {
            // As with "time zero" we issue timestamps counting from event 1 because it's slightly
            // nicer to read "the previous event" in logs when it's 0 not u64::max.
            Some(millis) if millis > self.prev.milli_secs => {
                self.prev.with_milli_sec(millis).with_event(1)
            }
            _ => self.prev.next_event(),
        };
        self.prev
    }

    pub(crate) fn create_watermark_and_start_s_phase(&mut self) -> GlobalTime {
        let ts = self.create_timestamp();
        self.wq_set.insert(ts);
        self.wa_set.insert(ts);
        ts
    }

    /// Removes `ts` from `self.wq_set`, indicating that the transaction in
    /// question has been stored on a quorum, and is therefore finished S-phase.
    /// Panics if the `ts` was not in `self.wq_set`.
    pub(crate) fn stored(&mut self, ts: GlobalTime) {
        assert!(self.wq_set.remove(&ts));
    }

    /// Removes `ts` from `self.rset`, indicating that the transaction in
    /// question has been stored on _all_ peers, and can therefore be read
    /// from any peer directly, without performing a quorum-read.
    pub(crate) fn fully_replicated(&mut self, ts: GlobalTime) {
        assert!(!self.wq_set.contains(&ts));
        assert!(self.wa_set.remove(&ts));
    }

    /// Returns the local server visibility watermark to gossip to other
    /// peers, describing the minimum transaction coordinated by this peer
    /// and still in S-phase. If no txn is in S-phase, we return a new
    /// timestamp to indicate a lower bound on any txn we _could_ issue
    /// in the future.
    pub(crate) fn svw(&mut self) -> Svw {
        match self.wq_set.iter().next() {
            None => Svw(self.create_timestamp()),
            Some(ts) => Svw(ts.clone()),
        }
    }

    pub(crate) fn srw(&mut self) -> Srw {
        match self.wa_set.iter().next() {
            None => Srw(self.create_timestamp()),
            Some(ts) => Srw(ts.clone()),
        }
    }

    pub(crate) fn server_watermarks(&mut self) -> ServerWatermarksLE {
        ServerWatermarksLE::new(self.svw(), self.srw())
    }
}
