# clepsydra

## Overview

This is a work-in-progress implementation of a core protocol for a minimalist
distributed database. It strives to be as small and simple as possible while
attempting to provide relatively challenging features:

  - Strict Serializability

  - Online Reconfiguration

  - Fault tolerance

  - High throughput

The implementation is based on a simplified version of the "Ocean Vista"
(OV) protocol, and uses its terminology wherever possible. OV combines
replication, transaction commitment and concurrency control into a single
protocol.

### Summary

The short version of the protocol is:

  - Transactions are represented as deterministic thunks over snapshots.

  - Each transaction is assigned a globally-unique timestamp.

  - Transactions are separated into two phases: S-phase and E-phase.

  - S-phase (storage) consists of coordination-free "blind quorum-writes"
    replicating the thunks into their MVCC order on each replica.

  - A watermark tracking minimum transaction timestamps-being-written is
    gossiped between peers, increasing as quorum-writes complete.

  - A transaction only enters E-phase after the watermark advances past it.

  - E-phase (evaluation) quorum-reads and evaluates thunks from consistent
    snapshots below the watermark, lazily resolving any earlier thunks.
    Everything below the watermark is coordination-free and deterministic.

### Caveats

Nothing's perfect, and this crate is anything but:

 - This crate is very incomplete and does not work yet. Don't use it for
   anything other than experiments and toys. Recovery, reconfiguration,
   timeouts and nontrivial fault tolerance paths _definitely_ don't work.

 - It also (somewhat recklessly) attempts to combine OV's reconfiguration
   and gossip protocols into an instance of the [concorde] reconfigurable
   lattice agreement protocol. This might not even be _theoretically_ safe.

 - It is much more minimal than the full OV protocol: there's no support
   for sharding, nor the two-level peer-vs-datacenter locality organization.
   This crate treats its whole peer group as a single symmetric shard.

 - As a result, performance won't be "webscale" or anything. It will scale
   vertically if you throw cores at it, but no better, and its latency will
   always have speed-of-light WAN RTT factors in it. It's distributed for
   fault tolerance, not horizontal scaling.

 - As with OV, this crate does require partial clock synchronization. It
   doesn't need to be very tight: clock drift only causes increased
   latency as the watermarks progress as the minimum of all times; it
   doesn't affect correctness. Normal weak-NTP-level sync should be ok.

 - As with OV, Calvin, and all deterministic databases: your txns have to be
   deterministic and must have deterministic _read and write sets_. If they
   cannot have their read and write sets statically computed (eg. if they
   rely on the data to decide read and write set) you have to build slightly
   awkward multi-phase txns. The term in the literature is "reconnaisance
   queries".

### Reference

Hua Fan and Wojciech Golab. Ocean Vista: Gossip-Based Visibility Control for
Speedy Geo-Distributed Transactions. PVLDB, 12(11): 1471-1484, 2019.

DOI: <https://doi.org/10.14778/3342263.3342627>

<http://www.vldb.org/pvldb/vol12/p1471-fan.pdf>

### Name

Wikipedia:

> A water clock or clepsydra (Greek κλεψύδρα from κλέπτειν kleptein, 'to
> steal'; ὕδωρ hydor, 'water') is any timepiece by which time is measured by
> the regulated flow of liquid into (inflow type) or out from (outflow type)
> a vessel, and where the amount is then measured.


License: MIT OR Apache-2.0
