// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::PeerID;
use serde::{Deserialize, Serialize};

/// GlobalTimes are the fundamental timekeeping type in the system.
///
/// They have some interesting properties:
///
///   - They are totally ordered.
///   - They are assumed globally unique: there's a unique peer-ID component in
///     each.
///   - They are issued monotonicaly-increasing: there's an event-counter at the
///     end in case real time moves backwards or stalls.
///   - They are issued at each peer, _without_ coordination.
///   - They do assume some level of real-time-clock availability on all peers
///     generating them, but those clocks do not need to be very tightly
///     synchronized. Larger skew will imply longer latency on each transaction
///     but will not affect throughput as transactions execute asynchronously
///     and in parallel.
///
/// The `GlobalTime` type is used in a variety of contexts, some of which are
/// wrapped in newtypes to make it a bit less likely to misuse them. They are
/// sometimes called "transaction IDs" or "tids", or "timestamps", or "ts", or
/// "versions", or the `ver` component of a [crate::KeyVer]. All these synonyms
/// arise from the OV paper and are reproduced here to attempt to retain similar
/// terminology in each algorithm and protocol message. The various "watermarks"
/// tracking the sets of transactions in each phase across the system are also
/// `GlobalTime`s.
#[derive(Clone, Copy, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GlobalTime {
    pub milli_secs: u64,
    pub peer: PeerID,
    pub event: u64,
}

impl std::fmt::Debug for GlobalTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}.{}@{}",
            self.milli_secs, self.event, self.peer.0
        ))
    }
}

impl GlobalTime {
    pub fn new(peer: PeerID, milli_secs: u64, event: u64) -> GlobalTime {
        GlobalTime {
            peer,
            milli_secs,
            event,
        }
    }
    pub fn max_for(peer: PeerID) -> GlobalTime {
        GlobalTime::new(peer, u64::MAX, u64::MAX)
    }
    pub fn time_zero_for(peer: PeerID) -> GlobalTime {
        // We issue "time zero" at 1,1 because it's slightly nicer
        // to read logs that ask for "the previous event" when it
        // doesn't wrap around to u64::MAX
        GlobalTime::new(peer, 0, 1)
    }
    pub fn with_milli_sec(&self, milli_secs: u64) -> GlobalTime {
        Self {
            milli_secs,
            ..*self
        }
    }
    pub fn prev_milli_sec(&self) -> GlobalTime {
        let mut t = *self;
        t.milli_secs = t
            .milli_secs
            .checked_sub(1)
            .expect("globaltime millisecond underflow");
        t
    }
    pub fn next_milli_sec(&self) -> GlobalTime {
        let mut t = *self;
        t.milli_secs = t
            .milli_secs
            .checked_add(1)
            .expect("globaltime millisecond overflow");
        t
    }
    pub fn with_event(&self, event: u64) -> GlobalTime {
        Self { event, ..*self }
    }
    pub fn prev_event(&self) -> GlobalTime {
        match self.event {
            0 => self.prev_milli_sec().with_event(u64::MAX),
            v => self.with_event(v - 1),
        }
    }
    pub fn next_event(&self) -> GlobalTime {
        match self.event {
            u64::MAX => self.next_milli_sec().with_event(0),
            v => self.with_event(v + 1),
        }
    }
}
