Module structure
================

Due to the way many parts of the OV protocol feed back on one another, we wind
up defining basically everything _on_ this singular Database type. Methods are
grouped by sub-protocols into separate modules (agreement, replication, etc.)
but essentially there's no good way around the "god object" model here because
that's just how the protocol is defined.


Clone-self pattern on Database
==============================

Database is Clone -- perhaps counterintuitively -- because all of its fields are
`Arc<...>` and we clone it _entirely_ in order to capture its state into various
task closures we spawn and futures we build in async move blocks.

Subsequent methods are defined directly on Database rather than being
free-standing functions that take a slew of cloned Arc<>s. Thanks to
https://www.philipdaniels.com/blog/2020/self-cloning-for-multiple-threads-in-rust/
for the suggestion.


Large number of traits required for Lang (pergola::Deftraits)
=============================================================

There's no _good_ reason for Lang itself to extend all the bounds it requires,
but it's necessary (as per bug https://github.com/rust-lang/rust/issues/26925)
to make derive(...) work on structs that are themselves parameterized by a Lang
instance.


Concurrency bugs from dropped quorum futures
============================================

A note concerning sources of deadlock or other concurrency bugs in this program:
if you're debugging what looks like a deadlock which manifests as service code
for some RPC mysteriously "not finishing" (eg. not writing back to store, not
releasing locks, etc.) there is a good chance this is because you've got a
quorum-read or quorum-write threshold involved and the RPC's future is literally
being abandoned part way through execution (at some await). Ideally this would
never happen (tasks spawned, futures polled to completion, etc.) but it's an
easy mistake to make, I've made it repeatedly.


Strict serializability
======================

A note concerning strict serializability. Taking the jepsen.io definition:

> if operation A completes before operation B begins,
> then A should appear to precede B in the serialization order.

Suppose A and B are both write-only transactions (the simplest kind) issued by
different hosts with skewed clocks, so A is _issued_ before B in real time, but
A has a later timestamp and B has an earlier timestamp, such that the
serialization order winds up being B, A. Can we have A _complete_ before B? No:
even write-only txs are only _acknowledged to the client_ after the
visibility-watermark advances above the timestamp, so the fact that the
timestamp order differs from the real-time order makes the txs appear to be
"concurrent" from the perspective of the system, and the order in which it
_completes_ a transaction will be the order of the watermark advancing past the
txs, which will be the same _in real time_ as the serialization order. So it is
in fact strictly serializable. The clock skew causes the system to have higher
latency / a larger window of txs considered "concurrent" and thereby potentially
having a different completion order than their issue order; so you do want the
window to be kept small! But you get strict serializability regardless of any
skew window.
