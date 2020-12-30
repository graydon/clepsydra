// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

//! This module glues together a set of peer connections and their IO service
//! loops provided by edelcrantz, a set of RPC sending and receiving async
//! methods on Database, and the concorde lattice agreement state machine. It's
//! a mess of uninteresting plumbing that should be cleaned up eventually.

use crate::{
    replication::{self},
    Database, Store, SyncBoxFuture,
};
use crate::{KeyVer, Lang};
use async_std::{
    sync::{Arc, Mutex},
    task,
};
use futures::{
    channel::mpsc::{self, UnboundedSender},
    stream::FuturesUnordered,
    StreamExt,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, debug_span, trace, warn, Instrument};

/// A simple "peer identifier" which should be unique across any present or
/// future configuration of a peer group. A randomly-chosen u64 should suffice.
///
/// Used when tracking quorums and configuration members both in this crate and
/// as a parameter to the lattice agreement protocol in [concorde].
#[derive(Clone, Copy, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerID(pub u64);

impl std::fmt::Debug for PeerID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("#{}", self.0))
    }
}

type Conn<L> = edelcrantz::Connection<OneWay<L>, Req<L>, Res<L>>;
type Queue<L> = edelcrantz::Queue<OneWay<L>, Req<L>, Res<L>>;

pub(crate) type Connection<L> = (Arc<Mutex<Conn<L>>>, Queue<L>);

pub(crate) type ResponseFuture<L> = SyncBoxFuture<(PeerID, Result<Res<L>, edelcrantz::Error>)>;

pub(crate) type ResponseFutures<L> = FuturesUnordered<ResponseFuture<L>>;

#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Req<L: Lang> {
    WriteReq(replication::WriteRequest<L>),
    GetReq(replication::GetRequest<L>),
}
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum Res<L: Lang> {
    WriteRes(replication::WriteResponse),
    GetRes(replication::GetResponse<L>),
}
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum OneWay<L: Lang> {
    LatticeAgreementMsg(crate::agreement::Msg),
    PutMsg(replication::PutOneWay<L>),
}

impl<L: Lang, S: Store<L>> Database<L, S> {
    /// Adds a connection to a remote [PeerID]. The `io` handle should be a
    /// full-duplex `AsyncRead+AsyncWrite` type.
    pub async fn connect<IO: edelcrantz::AsyncReadWrite>(&self, peer: PeerID, io: IO) {
        debug!(
            "connecting peers self={:?} to other={:?}",
            self.self_id, peer
        );
        let conn = edelcrantz::Connection::new(io);
        let queue = conn.queue.clone();
        let pair = (Arc::new(Mutex::new(conn)), queue);
        let mut net_guard = self.connections.write().await;
        net_guard.insert(peer, pair);
    }

    pub(crate) fn serve_req(&self, req: Req<L>) -> SyncBoxFuture<Res<L>> {
        let this = self.clone();
        Box::pin(async move { this.serve_req_(req).await })
    }

    pub(crate) async fn serve_req_(&self, req: Req<L>) -> Res<L> {
        match req {
            Req::WriteReq(wr) => {
                trace!("write request {:?}", wr);
                let res = self.serve_write(wr).await;
                trace!("write response {:?}", res);
                Res::WriteRes(res)
            }
            Req::GetReq(gr) => {
                trace!("get request {:?}", gr);
                let res = self.serve_get(gr).await;
                trace!("get response {:?}", res);
                Res::GetRes(res)
            }
        }
    }

    pub(crate) fn serve_oneway(
        &self,
        remote: PeerID,
        agreement_send: UnboundedSender<crate::agreement::Msg>,
        putmsg_send: UnboundedSender<(PeerID, crate::replication::PutOneWay<L>)>,
        ow: OneWay<L>,
    ) {
        match ow {
            OneWay::LatticeAgreementMsg(msg) => {
                trace!("agreement msg {:?}", msg);
                agreement_send.unbounded_send(msg).unwrap();
            }
            OneWay::PutMsg(put) => {
                trace!("put msg {:?}", put);
                putmsg_send.unbounded_send((remote, put)).unwrap();
            }
        }
    }

    /// Start network-service worker tasks on this database. Should be called
    /// before any calls to [Database::coordinate].
    pub fn launch_workers(&self) -> SyncBoxFuture<()> {
        let this: Database<L, S> = self.clone();
        let fut = async move {
            // TODO: the plumbing around the oneways is awkward. They
            // could/should share service task and/or the agreement
            // state-machine's requests and responses could be routed into
            // edelcrantz requests and responses also. And/or oneway processing
            // inside edelcrantz could itself be made async with an internal
            // mpsc. Many options for tidying this up.
            let (agreement_send, agreement_recv) = mpsc::unbounded();
            let (putmsg_send, mut putmsg_recv) = mpsc::unbounded();
            task::spawn({
                let this = this.clone();
                let span = debug_span!("put", peer=?this.self_id);
                (async move {
                    let mut p: usize = 0;
                    while let Some((remote, msg)) = putmsg_recv.next().await {
                        // TODO: maybe don't ignore put-failures.
                        p += 1;
                        let this = this.clone();
                        let span = debug_span!("put-rpc-task", peer=?this.self_id, ?remote, ?p);
                        task::spawn(async move { this.serve_put(msg).await }.instrument(span));
                    }
                })
                .instrument(span)
            });
            task::spawn({
                let this = this.clone();
                let span = debug_span!("agreement", peer=?this.self_id);
                async move {
                    this.run_proposal_loop(agreement_recv)
                        .instrument(span)
                        .await;
                }
            });
            for (id, conn) in this.connections.read().await.iter() {
                let id = id.clone();
                let conn = conn.0.clone();
                let a_send = agreement_send.clone();
                let p_send = putmsg_send.clone();
                task::spawn({
                    let this = this.clone();
                    async move {
                        let mut n: usize = 0;
                        loop {
                            n += 1;
                            let sreq = {
                                let this = this.clone();
                                let span =
                                    debug_span!("rpc-task", peer=?this.self_id, remote=?id, ?n);
                                move |req| {
                                    task::spawn(
                                        async move { this.serve_req(req).await }.instrument(span),
                                    )
                                }
                            };
                            let sow = {
                                let this = this.clone();
                                let a_send = a_send.clone();
                                let p_send = p_send.clone();
                                move |ow| this.serve_oneway(id, a_send, p_send, ow)
                            };
                            let mut guard = conn.lock().await;
                            trace!(
                                "service worker task on {:?} talking to {:?} advancing (step {:?})",
                                this.self_id,
                                id,
                                n
                            );
                            let span = debug_span!("service", peer=?this.self_id, remote=?id);
                            let res = guard.advance(sreq, sow).instrument(span).await;
                            trace!(
                                "service worker task on {:?} talking to {:?} advanced (step {:?}), got result {:?}",
                                this.self_id,
                                id,
                                n,
                                res
                            );
                            match res {
                                Ok(()) => (),
                                // TODO: A dropped response-channel means that someone
                                // sent a request (eg. a quorum-read) and then
                                // stopped listening for the response (eg.
                                // because they already got a quorum from some
                                // other responses). This is not an error from
                                // our perspective, currently. It might be in
                                // the future, if we rework things.
                                Err(edelcrantz::Error::ResponseChannelDropped(_)) => (),
                                Err(_) => break,
                            }
                        }
                        warn!(
                            "service worker task on {:?} talking to {:?} exited",
                            this.self_id, id
                        );
                    }
                });
            }
        };
        Box::pin(fut)
    }

    pub(crate) async fn send_write_to_all(
        &self,
        kv: KeyVer<L>,
        e: L::Expr,
        vals: Vec<L::Val>,
    ) -> ResponseFutures<L> {
        let req = Req::WriteReq(replication::WriteRequest::Write(kv, e, vals));
        self.send_req_to_all(req).await
    }

    pub(crate) async fn send_abort_to_all(&self, kv: KeyVer<L>) -> ResponseFutures<L> {
        let req = Req::WriteReq(replication::WriteRequest::Abort(kv));
        self.send_req_to_all(req).await
    }

    pub(crate) async fn send_finalize_to_peers(
        &self,
        kv: KeyVer<L>,
        peers: Vec<PeerID>,
    ) -> ResponseFutures<L> {
        let req = Req::WriteReq(replication::WriteRequest::Finalize(kv));
        self.send_req_to_peers(req, peers).await
    }

    pub(crate) async fn send_get_to_all(&self, kv: KeyVer<L>) -> ResponseFutures<L> {
        let req = Req::GetReq(replication::GetRequest(kv));
        self.send_req_to_all(req).await
    }

    pub(crate) fn send_req_to_self(&self, req: Req<L>) -> ResponseFuture<L> {
        let this = self.clone();
        Box::pin(async move { (this.self_id, Ok(this.serve_req(req).await)) })
    }

    pub(crate) async fn send_req_to_all(&self, req: Req<L>) -> ResponseFutures<L> {
        let mut peers: Vec<PeerID> = self.connections.read().await.keys().cloned().collect();
        peers.push(self.self_id);
        self.send_req_to_peers(req, peers).await
    }

    pub(crate) async fn send_req_to_peer(&self, req: Req<L>, peer: PeerID) -> ResponseFuture<L> {
        if peer == self.self_id {
            self.send_req_to_self(req.clone())
        } else {
            match self.connections.read().await.get(&peer.clone()) {
                None => Box::pin({
                    let peer = peer.clone();
                    async move { (peer, Err(edelcrantz::Error::Queue)) }
                }),
                Some((_, queue)) => {
                    let fut = queue.enqueue_request(req.clone());
                    Box::pin(async move { (peer, fut.await) })
                }
            }
        }
    }

    pub(crate) async fn send_req_to_peers(
        &self,
        req: Req<L>,
        peers: Vec<PeerID>,
    ) -> ResponseFutures<L> {
        trace!("broadcasting request {:?} to peers {:?}", req, peers);
        let futs: ResponseFutures<L> = FuturesUnordered::new();
        for peer in peers {
            futs.push(self.send_req_to_peer(req.clone(), peer).await);
        }
        futs
    }
}
