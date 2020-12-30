
# TODO

Short term:
===========

  - [ ] find non-opt + debug-log-level concurrency bug (endless test execution)
  - [ ] refactor replication read and write functions
  - [ ] avoid double-read on store during eval -- integrate store-read with locks?
  - [ ] figure out sensible trigger conditions on lattice agreement rounds
  - [ ] maybe clean up the communication plumbing a bit, it's a mess
  - [ ] address question of straggling in watermarks. do we definitely wait on everyone? I guess so?
  - [ ] figure out where the reconfiguration safe-points are and how to detect /
        what to do about in-progress txns during reconfigurations.
  - [ ] switch everything to sorted_vec_maps

Longer term
===========

  - [ ] evaluate the paper's "GC watermark" concept, see if it fits
  - [ ] implement some nontrivial Lang for testing at least
  - [ ] write preliminary log-structured storage backend with column sketches for Store
  - [ ] figure out if there's a nice way to have epochs in the peer-set lattice (&
        peer ID space -- or just stick with big-and-random IDs?)
  - [ ] add some kind of table / database / application-space identifiers
  - [ ] everything else... many TODOs scattered through code

Done
====

  - [X] clean up and document at least the public parts
  - [X] switch everything to tracing
  - [X] add fully-evaluated watermark to collect dead eval locks
  - [X] track successful write-set, and only finalize each after it's actually been written
  - [X] switch edelcrantz to postcard
  - [X] implement put() at end of evaluation
  - [X] implement read-from-local when under fully-replicated watermark
  - [X] fix locking / implement proper lock manager / deal with deadlock
  - [X] figure out correct arrangement of test/bin
  - [X] WONTFIX: fix lattice agreement / sending gets before vwatermark advances
  - [X] decide on a representation for GlobalTime. Maybe avoid being clever with
        bit-packing and let the storage layer do that per-segment via range and dictionary coding.
