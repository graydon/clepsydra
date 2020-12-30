// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

//! The replication sub-protocol of OV attempts to ensure that:
//!
//!   - Every write is sufficiently replicated during S-phase. The writing tx is
//!     un-blocked once all its writes are acknowledged from a quorum ("WQ") but
//!     writing (and finalizing) then continues _asynchronously_ to all
//!     remaining peers including stragglers ("WA"). If the WQ part hears
//!     success from a super-quorum it can unblock its writing tx after only 1
//!     WAN RTT, otherwise (if only a simple-majority quorum responds in time)
//!     it takes a second RTTs to finalize and receive confirmations.
//!
//!   - Every read succeeds (up to fault-tolerance limit) while being serviced
//!     by as small a number of replicas as possible: if the read is below
//!     Rwatermark, we can fetch from any store directly on any single peer and
//!     expect success, including the local peer ("RO"). If the read is above
//!     Rwatermark, we have to do a quorum-read ("RQ") because the value might
//!     not be fully replicated yet.
//!
//! In other words, in the good/common case we should be able to get by with
//! 1-RTT WQRO, which is the best you can really do if you want replication at
//! all.

use crate::{
    majority_quorum, network, super_quorum, Database, Entry, Error, ExtVal, KeyVer, PeerID,
    RWatermark, Store, SyncBoxFuture,
};
use crate::{GlobalTime, Lang};
use async_std::future;
use futures::TryStreamExt;
use futures_util::{stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::SystemTime,
};
use tracing::{debug, debug_span, error, Instrument};

#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum WriteRequest<L: Lang> {
    Write(KeyVer<L>, L::Expr, Vec<L::Val>),
    Abort(KeyVer<L>),
    Finalize(KeyVer<L>),
}

#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum WriteResponse {
    Success, // First-round response to Write or Abort (writes can't fail)
    Confirm, // Second-round response to Finalize (tentative writes _can_ time out)
    Failure, // Something about write failed; should be impossible, represents a
             // corrupt packet or logic error or more failed nodes than we can tolerate
             // or something.
}

/// Designates which phase of quorum-writing a given [crate::Entry] was stored
/// in. Must be stored and retieved along with entries in the [crate::Store].
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicationTag {
    Tentative,
    Finalized,
}

/// At the end of evaluation, coordinators `Put` the finished ExtVal of each key
/// back to each replica. This is a one-way message and if it fails or is lost
/// the ExtVal can and will be lazily but deterministically re-evaluated on each
/// replica as needed.
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct PutOneWay<L: Lang>(KeyVer<L>, ExtVal<L>);

#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct GetRequest<L: Lang>(pub KeyVer<L>);

#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum GetResponse<L: Lang> {
    SettledEntry(GlobalTime, ReplicationTag, ExtVal<L>),
    // TODO: it's not clear that an explicit AbortedEntry response is necessary
    // or even makes sense; maybe storage should just return the last
    // non-aborted before query?
    AbortedEntry(GlobalTime),
    Failure, // Again, something unforeseen failed. Probably a quorum-read failing because
             // of too many dead nodes while doing a recursive read within get.
}

fn drain_responses<L, F>(
    fu: FuturesUnordered<F>,
    expected: WriteResponse,
) -> SyncBoxFuture<Result<Vec<PeerID>, Error>>
where
    L: Lang,
    F: Future<Output = (PeerID, Result<network::Res<L>, edelcrantz::Error>)>
        + 'static
        + Send
        + Sync,
{
    Box::pin(drain_responses_(fu, expected))
}

async fn drain_responses_<L, F>(
    mut fu: FuturesUnordered<F>,
    expected: WriteResponse,
) -> Result<Vec<PeerID>, Error>
where
    L: Lang,
    F: Future<Output = (PeerID, Result<network::Res<L>, edelcrantz::Error>)>,
{
    let mut peers = Vec::new();
    while let Some(res) = fu.next().await {
        match res {
            (peer, Ok(network::Res::WriteRes(x))) if x == expected => {
                peers.push(peer);
            }
            (_, Ok(_)) => return Err(Error::UnexpectedResponse),
            (_, Err(_)) => return Err(Error::NetworkingError),
        }
    }
    Ok(peers)
}

impl<L: Lang, S: Store<L>> Database<L, S> {
    // This async function, like write(), resolves to a `Result::Ok(fu)` when it
    // has successfully replicated an abort command to a quorum; the residual
    // value `fu` is a future that will drain the remaining aborts-in-flight or
    // return an error if anything unexpected happens while draining.
    //
    // All such futures should be placed into an asynchronous task that drains
    // them along with all the other aborts for this txn, then calls
    // tidmgr.fullyReplicated to move the Srw local replication-watermark.

    pub(crate) fn abort_txn(
        &self,
        kv: KeyVer<L>,
    ) -> SyncBoxFuture<Result<SyncBoxFuture<Result<Vec<PeerID>, Error>>, Error>> {
        let this = self.clone();
        let span = debug_span!("abort_txn", ?kv);
        Box::pin(async move { this.abort_txn(kv).instrument(span).await })
    }

    pub(crate) async fn abort_txn_(
        &self,
        kv: KeyVer<L>,
    ) -> Result<SyncBoxFuture<Result<Vec<PeerID>, Error>>, Error> {
        let mut responses = self.send_abort_to_all(kv.clone()).await;
        let replica_count: usize = responses.len();
        debug!(
            "sent broadcast-abort of {:?} to {:?} peers",
            kv, replica_count
        );
        let mut n_aborted_received: usize = 0;
        // TODO: handle timeout / "majority unavailable" case.
        while let Some(res) = responses.next().await {
            match res {
                (_, Ok(network::Res::WriteRes(WriteResponse::Success))) => {
                    n_aborted_received += 1;
                    if n_aborted_received >= majority_quorum(replica_count) {
                        return Ok(drain_responses(responses, WriteResponse::Success));
                    }
                }

                (_, Ok(_)) => return Err(Error::UnexpectedResponse),
                (_, Err(_)) => return Err(Error::NetworkingError),
            }
        }
        Err(Error::TooFewReplicas)
    }

    // Section 4.1 Write-only operations. Ensures that a txn is sufficiently
    // replicated to allow the local visibility watermark to advance past it,
    // either through a super-quorum fast path or a majority-quorum slow path.
    //
    // If successful -- either writing a super-quorum or majority-quorum -- it
    // returns Ok(fu) where fu is a future containing the _residual_
    // writes-in-flight (and the residual finalizations). The caller should
    // spawn a background task containing all such residual futures from all
    // writes, then call tidmgr.fullyReplicated when the last of the writes
    // completes: this will move the Srw replication watermark.
    pub(crate) fn write(
        &self,
        kv: KeyVer<L>,
        e: L::Expr,
        vals: Vec<L::Val>,
    ) -> SyncBoxFuture<Result<SyncBoxFuture<Result<Vec<PeerID>, Error>>, Error>> {
        let this = self.clone();
        let span = debug_span!("write", ?kv);
        Box::pin(async move { this.write_(kv, e, vals).instrument(span).await })
    }

    // TODO: refactor this, maybe introduce a helper type that holds both the
    // ongoing writes and ongoing finalizes.
    pub(crate) async fn write_(
        &self,
        kv: KeyVer<L>,
        e: L::Expr,
        vals: Vec<L::Val>,
    ) -> Result<SyncBoxFuture<Result<Vec<PeerID>, Error>>, Error> {
        let start = self.tidmgr.read().await.current_time();

        let mut write_responses = self.send_write_to_all(kv.clone(), e, vals).await;
        let replica_count = write_responses.len();
        debug!(
            "sent broadcast-write of {:?} to {:?} peers",
            kv.clone(),
            replica_count
        );
        let mut successes: BTreeSet<PeerID> = BTreeSet::new();
        while let Some(res) = write_responses.next().await {
            debug!(
                "write got response {:?} ({:?}/{:?})",
                res,
                successes.len(),
                replica_count
            );
            match res {
                (peer, Ok(network::Res::WriteRes(WriteResponse::Success))) => {
                    successes.insert(peer);
                    if self.within_fast_path(start).await {
                        if successes.len() >= super_quorum(replica_count) {
                            // We received a super-quorum within the fast-path
                            // timeout, we can consider it safely replicated and
                            // un-gate our caller, sending the finalizes
                            // asynchronously and draining both.
                            let early_write_peers: Vec<PeerID> =
                                successes.iter().cloned().collect();
                            let early_finalize_responses = self
                                .send_finalize_to_peers(kv.clone(), early_write_peers)
                                .await;
                            let this = self.clone();
                            let drainer = async move {
                                let late_write_peers: Vec<PeerID> =
                                    drain_responses(write_responses, WriteResponse::Success)
                                        .await?;
                                let late_finalize_responses =
                                    this.send_finalize_to_peers(kv, late_write_peers).await;
                                drain_responses(early_finalize_responses, WriteResponse::Confirm)
                                    .await?;
                                drain_responses(late_finalize_responses, WriteResponse::Confirm)
                                    .await
                            };
                            return Ok(Box::pin(drainer));
                        } else {
                            // We're in the fast-path still but haven't got a
                            // super-majority, keep waiting.
                        }
                    } else {
                        // We're past the fast-path so we're waiting for a
                        // majority.
                        if successes.len() >= majority_quorum(replica_count) {
                            // We got a majority, proceed to majority-finalize below.
                            break;
                        }
                        // TODO: handle timeout / "majority unavailable" case on the slow path.
                    }
                }
                (_, Ok(r)) => {
                    error!("unexpected write response: {:?}", r);
                    return Err(Error::UnexpectedResponse);
                }
                (_, Err(e)) => {
                    error!("networking error while writing: {:?}", e);
                    return Err(Error::NetworkingError);
                }
            }
        }

        if successes.len() < majority_quorum(replica_count) {
            error!(
                "write hit too few replicas: {:?}/{:?}, needed quorum of {:?}",
                successes.len(),
                replica_count,
                majority_quorum(replica_count)
            );
            return Err(Error::TooFewReplicas);
        }

        let early_write_peers = successes.iter().cloned().collect();
        let mut early_finalize_responses = self
            .send_finalize_to_peers(kv.clone(), early_write_peers)
            .await;
        let mut n_finalize_received: usize = 0;
        while let Some(res) = early_finalize_responses.next().await {
            match res {
                (_, Ok(network::Res::WriteRes(WriteResponse::Confirm))) => {
                    n_finalize_received += 1;
                    if n_finalize_received >= majority_quorum(replica_count) {
                        let this = self.clone();
                        let drainer = async move {
                            let late_write_peers: Vec<PeerID> =
                                drain_responses(write_responses, WriteResponse::Success).await?;
                            let late_finalize_responses =
                                this.send_finalize_to_peers(kv, late_write_peers).await;
                            drain_responses(early_finalize_responses, WriteResponse::Confirm)
                                .await?;
                            drain_responses(late_finalize_responses, WriteResponse::Confirm).await
                        };
                        return Ok(Box::pin(drainer));
                    }
                }
                (_, Ok(_)) => return Err(Error::UnexpectedResponse),
                (_, Err(_)) => return Err(Error::NetworkingError),
            }
        }

        Err(Error::TooFewReplicas)
    }

    const MAX_RETRIES: usize = 64;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    const FAST_PATH: std::time::Duration = std::time::Duration::from_millis(200);

    // See crate::SyncBoxFuture for explanation of this wrapper function.
    pub(crate) fn read(&self, kv: KeyVer<L>) -> SyncBoxFuture<Result<(L::Key, ExtVal<L>), Error>> {
        let this = self.clone();
        let span = debug_span!("read", ?kv);
        Box::pin(async move { this.read_(kv).instrument(span).await })
    }

    // Section 4.3 The Read-Only Operation (a.k.a. "Algorithm 3")
    pub(crate) async fn read_(&self, kv: KeyVer<L>) -> Result<(L::Key, ExtVal<L>), Error> {
        // If we're reading from below the fully-replicated watermark, we can just
        // read from our local copy.
        {
            let RWatermark(rw) = self.group_wm.0.lock().await.replication_watermark.clone();
            if kv.ver <= rw {
                let mut target = kv.ver;
                debug!(
                    "read of {:?} is below RWatermark {:?}, reading locally",
                    kv, rw
                );
                loop {
                    let gr = GetRequest(KeyVer {
                        key: kv.key.clone(),
                        ver: target,
                    });
                    let res = self.serve_get(gr).await;
                    match res {
                        GetResponse::SettledEntry(_, _, ev) => return Ok((kv.key, ev)),
                        GetResponse::AbortedEntry(time) => target = time.prev_event(),
                        GetResponse::Failure => return Err(Error::ReadFailed),
                    }
                }
            }
        }

        // We loop until we meet a "MATCH CONDITION" which is defined as
        //
        //     the highest version ver <= ts for which at least one
        //     replica is FINALIZED or at least f+1 replicas are TENTATIVE

        #[derive(Debug)]
        struct ReadResponse<L: Lang> {
            num_tentative: usize,
            num_finalized: usize,
            num_aborted: usize,
            value: Option<ExtVal<L>>,
        }

        impl<L: Lang> Default for ReadResponse<L> {
            fn default() -> Self {
                Self {
                    num_tentative: 0,
                    num_finalized: 0,
                    num_aborted: 0,
                    value: None,
                }
            }
        }

        let mut num_retries = 0;
        let mut target = kv.ver;
        'target: loop {
            debug!("read fetching keyver {:?}, target={:?}", kv, target);
            // We get() the target version. Every version we're requesting
            // here is _visible_, which means consensus visible-time has
            // advanced enough that one of the following conditions holds:
            //
            //    - write() failed at target, abort_txn succeeded at target,
            //      so there are at most f TENTATIVE replicas for target,
            //      and no FINALIZED replicas for target.
            //    - write() succeeded at target:
            //      - in the fast path, producing super_quorum
            //        TENTATIVE-or-FINALIZED replicas; or else
            //      - in the slow path, producing majority_quorum
            //        FINALIZED replicas.
            //
            // If target was aborted, we read the previous version _before_
            // target, because new-values-at-target are not part of the
            // consensus history.
            //
            // We therefore loop here from target=ts downward until we find
            // a version that _did_ get written.
            let rkv = KeyVer {
                key: kv.key.clone(),
                ver: target,
            };
            let mut futs: FuturesUnordered<_> = self.send_get_to_all(rkv.clone()).await;
            let replica_count: usize = futs.len();
            if replica_count == 0 {
                return Err(crate::Error::TooFewReplicas);
            }
            let mut responses = BTreeMap::new();
            debug!(
                "sent broadcast-get of {:?} to {:?} peers (retry count {:?})",
                rkv, replica_count, num_retries
            );
            while let Some(result) = futs.next().await {
                debug!("read response {:?}", result);
                match result {
                    (_, Ok(network::Res::GetRes(GetResponse::AbortedEntry(ts)))) => {
                        let rr = responses
                            .entry(ts)
                            .or_insert_with(|| ReadResponse::<L>::default());
                        rr.num_aborted += 1;
                    }
                    (_, Ok(network::Res::GetRes(GetResponse::SettledEntry(ts, tag, val)))) => {
                        let rr = responses
                            .entry(ts)
                            .or_insert_with(|| ReadResponse::<L>::default());
                        match tag {
                            ReplicationTag::Tentative => rr.num_tentative += 1,
                            ReplicationTag::Finalized => rr.num_finalized += 1,
                        }
                        match &rr.value {
                            None => rr.value = Some(val),
                            Some(other) if *other != val => {
                                return Err(Error::InconsistentReplicas)
                            }
                            Some(_) => (),
                        }
                    }
                    _ => {
                        // Replicated reads are fault tolerant, we're going to
                        // retry until we hit a retry limit or a successful quorum-read.
                    }
                }
                // Each time we receive a response, we run from max-to-min in
                // the set of received responses checking to see if we've got a
                // threshold-to-act on any of the versions yet.
                for (ts, rr) in responses.iter().rev() {
                    if rr.num_aborted >= majority_quorum(replica_count) {
                        debug!(
                            "read explicit quorum-read aborted key at {:?}, trying earlier version",
                            ts
                        );
                        target = responses.iter().next_back().unwrap().0.prev_event();
                        continue 'target;
                    }
                    if rr.num_tentative + rr.num_finalized >= super_quorum(replica_count)
                        || rr.num_finalized >= majority_quorum(replica_count)
                    {
                        if let Some(v) = &rr.value {
                            let kv = KeyVer::<L> {
                                key: kv.key,
                                ver: *ts,
                            };
                            debug!("read quorum-read {:?}={:?}", kv, v);
                            return Ok((kv.key, v.clone()));
                        }
                    }
                }
            }
            num_retries += 1;
            if num_retries > Self::MAX_RETRIES {
                error!("read of {:?} retried {:?} times, failing", kv, num_retries);
                return Err(Error::ReadFailed);
            }
            let never = future::pending::<()>();
            error!(
                "read of {:?} failed to find quorum, retrying after {:?}",
                kv,
                Self::RETRY_DELAY
            );
            let _ = future::timeout(Self::RETRY_DELAY, never).await;
        }
    }

    pub(crate) async fn put(
        &self,
        ver: GlobalTime,
        out: &BTreeMap<L::Key, ExtVal<L>>,
    ) -> Result<(), Error> {
        for (_, queue) in self.connections.read().await.values() {
            for (k, v) in out {
                let kv = KeyVer {
                    key: k.clone(),
                    ver,
                };
                let ow = network::OneWay::PutMsg(PutOneWay(kv, v.clone()));
                queue.enqueue_oneway(ow).await?
            }
        }
        Ok(())
    }

    async fn within_fast_path(&self, start: SystemTime) -> bool {
        let now = self.tidmgr.read().await.current_time();
        match now.duration_since(start) {
            Ok(dur) => dur < Self::FAST_PATH,
            Err(_) => {
                // If the clock moves backwards, we treat it as timeout.
                false
            }
        }
    }

    // See crate::SyncBoxFuture for explanation of this wrapper function.
    pub(crate) fn serve_write(&self, wr: WriteRequest<L>) -> SyncBoxFuture<WriteResponse> {
        let this = self.clone();
        let (ty, kv) = match &wr {
            WriteRequest::Write(kv, _, _) => ("write", kv),
            WriteRequest::Abort(kv) => ("abort", kv),
            WriteRequest::Finalize(kv) => ("finalize", kv),
        };
        let span = debug_span!("serve_write", ?kv, ty);
        Box::pin(async move { this.serve_write_(wr).instrument(span).await })
    }

    pub(crate) async fn serve_write_(&self, wr: WriteRequest<L>) -> WriteResponse {
        use crate::Entry::{Aborted, Delayed, Settled};
        use ReplicationTag::{Finalized, Tentative};
        match wr {
            WriteRequest::Write(kv, x, vals) => {
                let e = Delayed(x, vals, Tentative);
                self.store.write().await.put_key_at_time(&kv, &e);
                WriteResponse::Success
            }
            WriteRequest::Abort(kv) => {
                self.store.write().await.put_key_at_time(&kv, &Aborted);
                WriteResponse::Success
            }
            WriteRequest::Finalize(kv) => {
                let mut guard = self.store.write().await;
                match guard.get_key_at_or_before_time(&kv) {
                    None => {
                        error!("finalizing {:?}: no key found", kv);
                        WriteResponse::Failure
                    }
                    Some((existing_ts, existing_entry)) => {
                        if existing_ts != kv.ver {
                            // Sender sent us an erroneous request, or our store
                            // is corrupt. Either way, we're done for.
                            error!(
                                "finalizing {:?}: timestamp mismatch with existing {:?}",
                                kv, existing_ts
                            );
                            WriteResponse::Failure
                        } else {
                            match existing_entry {
                                Settled(_, Finalized) | Aborted | Delayed(_, _, Finalized) => (), // Nothing to do
                                Delayed(x, vals, Tentative) => {
                                    guard.put_key_at_time(&kv, &Delayed(x, vals, Finalized));
                                }
                                Settled(v, Tentative) => {
                                    guard.put_key_at_time(&kv, &Settled(v, Finalized))
                                }
                            };
                            WriteResponse::Confirm
                        }
                    }
                }
            }
        }
    }

    // See lib::SyncBoxFuture for explanation of this wrapper function.
    pub(crate) fn serve_put(&self, pow: PutOneWay<L>) -> SyncBoxFuture<Result<(), Error>> {
        let this = self.clone();
        let span = debug_span!("serve_put", kv=?pow.0);
        Box::pin(async move { this.serve_put_(pow).instrument(span).await })
    }

    pub(crate) async fn serve_put_(&self, pow: PutOneWay<L>) -> Result<(), Error> {
        let PutOneWay(kv, ev) = pow;
        let mut guard = self.store.write().await;
        let tagopt = match guard.get_key_at_or_before_time(&kv) {
            // TODO: is a put to a version we haven't seen yet tentative? Unsure.
            // It probably should never happen but we're not presently tracking the
            // quorum-write set between write() and put().
            Some((ts, _)) if ts != kv.ver => Some(ReplicationTag::Tentative),
            Some((_, Entry::Delayed(_, _, tag))) => Some(tag),
            Some((_, Entry::Settled(ov, _))) if ov != ev => {
                return Err(Error::InconsistentReplicas)
            }
            _ => None,
        };
        match tagopt {
            None => (),
            Some(tag) => {
                guard.put_key_at_time(&kv, &Entry::Settled(ev, tag));
            }
        }
        Ok(())
    }

    // See lib::SyncBoxFuture for explanation of this wrapper function.
    pub(crate) fn serve_get(&self, gr: GetRequest<L>) -> SyncBoxFuture<GetResponse<L>> {
        let this = self.clone();
        let span = debug_span!("serve_get", kv=?gr.0);
        Box::pin(async move { this.serve_get_(gr).instrument(span).await })
    }

    // The contract with 'get' is that it's never called with a timestamp
    // later (greater) than Vwatermark (the global minimum) which was only advanced
    // when there's lattice-agreement from a quorum to advance it; so we
    // can definitely expect to be able to resolve all the thunks (recursively)
    // that the `get` depends on. If this is not so, there's a protocol-level
    // error.
    pub(crate) async fn serve_get_(&self, gr: GetRequest<L>) -> GetResponse<L> {
        use crate::Entry::{Aborted, Delayed, Settled};
        let GetRequest(kv) = gr;

        // NB: the version being requested is always "below VWatermark" in the
        // sense of an omnicient observer (or indeed in the eyes of the peer
        // that sent the get request); but _this peer_ might not have received
        // enough information from lattice agreement to know that yet -- the get
        // request might be racing with the watermark propagation message -- so
        // we might need to pause briefly here to see our own evidence of that
        // watermark advance.
        //
        // TODO: this wait might be superfluous. Argument: if VWatermark
        // advanced on the sender of the request, we can assume it's because
        // they saw all server watermarks advance past kv.ver, so the causal
        // order below kv.ver is fixed and it doesn't really matter if this peer
        // knows that yet.
        self.wait_for_visibility_watermark(kv.ver).await;

        // NB: this needs to occur before the match-head in order to release the
        // read() lock inside the match arms; inside the head it's an lval and
        // the rwlock guard's lifetime is extended to the whole match expr.
        let first_read = self.store.read().await.get_key_at_or_before_time(&kv);
        match first_read {
            None => {
                debug!("don't have {:?} yet", kv);
                GetResponse::Failure
            }
            Some((ver, e)) => {
                match e {
                    Delayed(x, vals, tag) => {
                        let kv = KeyVer { key: kv.key, ver };
                        let kl = self.get_keylock(&kv).await;
                        let _lockguard = kl.lock().await;
                        // re-check that someone didn't resolve on us while we
                        // were locking.
                        let second_read = self.store.read().await.get_key_at_or_before_time(&kv);
                        match second_read {
                            Some((v, Settled(val, tag))) if v == ver => {
                                return GetResponse::SettledEntry(ver, tag, val);
                            }
                            _ => (),
                        }
                        let ver_pre = ver.prev_event();
                        let read_set: BTreeSet<L::Key> = L::get_read_set(&x);
                        debug!(
                            "resolving delayed expr {:?} for {:?} by fetching read-set {:?}",
                            x, kv, read_set
                        );
                        let read_futs: FuturesUnordered<_> = read_set
                            .iter()
                            .map(|k| {
                                self.read(KeyVer {
                                    key: k.clone(),
                                    ver: ver_pre,
                                })
                            })
                            .collect();
                        let maybe_env: Result<BTreeMap<_, _>, Error> =
                            read_futs.try_collect().await;
                        match maybe_env {
                            Ok(env) => {
                                let settled = L::eval_expr(&x, &vals, &env);
                                self.store
                                    .write()
                                    .await
                                    .put_key_at_time(&kv, &Settled(settled.clone(), tag.clone()));
                                GetResponse::SettledEntry(ver, tag, settled)
                            }
                            Err(_) => GetResponse::Failure,
                        }
                    }
                    Settled(val, tag) => GetResponse::SettledEntry(ver, tag, val),
                    Aborted => GetResponse::AbortedEntry(ver),
                }
            }
        }
    }
}
