// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

//! This module instantiates the [concorde] reconfigurable lattice agreement
//! crate with the [PeerID] and [crate::watermarks] types in clepsydra, such that
//! we can use lattice agreement in place of the watermark-gossip and
//! peer-configuration protocols in Ocean Vista. At least in theory. In practice
//! this is still quite rough, but it does cause time to advange.

use std::collections::BTreeMap;

use crate::{
    network::OneWay, Database, GlobalTime, GroupWatermarks, Lang, PeerID, RWatermark,
    ServerWatermarksLD, ServerWatermarksLE, ServerWatermarksLEExt, Store, Svw, VWatermark,
};
use concorde::{CfgLE, CfgLEExt, StateLE, StateLEExt};
use futures::{channel::mpsc::UnboundedReceiver, stream::FusedStream, StreamExt};
use pergola::{BTreeMapWithUnion, LatticeElt};
use tracing::{debug, trace};

pub(crate) type Msg = concorde::Message<ObjLD, PeerID>;
pub(crate) type Participant = concorde::Participant<ObjLD, PeerID>;

/// The object lattice of our reconfigurable lattice agreement is a map from
/// peer to the maximum watermarks on that peer. This means that during lattice
/// agreement we'll "see" the advance of some peer's local Svw and Srw
/// watermarks _when a quorum sees them_; but we'll later take the _minimum_ of
/// all such "seen" per-peer watermarks when calculating the [[GroupWatermarks]]
/// we decide to advance-to ourselves.
pub(crate) type ObjLD = BTreeMapWithUnion<PeerID, ServerWatermarksLD>;
pub(crate) type ObjLE = LatticeElt<ObjLD>;
pub(crate) trait ObjLEExt {
    /// Return the minimum of all the per-peer [[Svw]] and [[Srw]] local watermarks in
    /// the system. This should be used to advance the local watermarks.
    ///
    /// Panics if any expected peer is missing. To avoid this, build a new ObjLE
    /// with default values for all expected peers and join it with this ObjLE.
    fn get_min_watermarks(&self, expected_peers: &Vec<PeerID>) -> GroupWatermarks;

    /// Update the watermark for the local peer. Panics if existing watermarks lattice
    /// element for local peer is not <= new element.
    fn advance_local_watermark(
        &mut self,
        local_peer: PeerID,
        server_watermarks: &ServerWatermarksLE,
    );

    /// Builds a new ObjLE with a default watermark value for each peer.
    fn default_for_peers(peers: &Vec<PeerID>) -> Self;
}

impl ObjLEExt for LatticeElt<ObjLD> {
    fn default_for_peers(peers: &Vec<PeerID>) -> Self {
        let mut m = BTreeMap::new();
        for p in peers.iter() {
            m.insert(*p, ServerWatermarksLE::new_from_peer_zero_time(*p));
        }
        LatticeElt::from(m)
    }

    fn advance_local_watermark(
        &mut self,
        local_peer: PeerID,
        server_watermarks: &ServerWatermarksLE,
    ) {
        let newle = ServerWatermarksLE::from(server_watermarks.clone());
        match self.value.get(&local_peer) {
            None => (),
            Some(oldle) => {
                assert!(oldle <= &newle);
            }
        }
        self.value.insert(local_peer, newle);
    }

    fn get_min_watermarks(&self, expected_peers: &Vec<PeerID>) -> GroupWatermarks {
        if self.value.len() == 0 {
            return GroupWatermarks {
                visibility_watermark: VWatermark(GlobalTime::default()),
                replication_watermark: RWatermark(GlobalTime::default()),
            };
        }
        for p in expected_peers.iter() {
            if !self.value.contains_key(p) {
                panic!("missing expected peer {:?}", p);
            }
        }
        let mut vw = GlobalTime::max_for(expected_peers[0]);
        let mut rw = GlobalTime::max_for(expected_peers[0]);
        for (_, v) in self.value.iter() {
            vw = std::cmp::min(vw, v.svw().value.0);
            rw = std::cmp::min(rw, v.srw().value.0);
        }
        GroupWatermarks {
            visibility_watermark: VWatermark(vw),
            replication_watermark: RWatermark(rw),
        }
    }
}

impl<L: Lang, S: Store<L>> Database<L, S> {
    async fn get_expected_peers(&self) -> Vec<PeerID> {
        let guard = self.connections.read().await;
        let mut ps: Vec<_> = guard.keys().cloned().collect();
        if !guard.contains_key(&self.self_id) {
            ps.push(self.self_id);
        }
        ps
    }

    /// Retrieves the final _group_ watermarks -- the minimum of all the server watermarks we
    /// learned about from lattice agreement. This should be sent to [[publish]].
    pub(crate) async fn get_current_agreement(&self) -> Option<GroupWatermarks> {
        let expected_peers: Vec<PeerID> = self.get_expected_peers().await;
        match &self.participant.read().await.final_state {
            None => None,
            Some(statele) => Some(statele.object().get_min_watermarks(&expected_peers)),
        }
    }

    /// Starts a new proposal cycle of lattice-agreement based on the last lattice-agreement
    /// state and the current tidmgr's watermarks.
    pub(crate) async fn begin_new_proposal(&self) {
        let expected_peers: Vec<PeerID> = self.get_expected_peers().await;
        let server_watermarks = self.tidmgr.write().await.server_watermarks();
        let (cfg, mut obj): (CfgLE<PeerID>, ObjLE) =
            match &self.participant.read().await.final_state {
                Some(sle) => (sle.config().clone(), sle.object().clone()),
                None => {
                    let mut cfg = CfgLE::default();
                    let obj = ObjLE::default_for_peers(&expected_peers);
                    for p in expected_peers.iter() {
                        cfg.added_peers_mut().insert(*p);
                    }
                    (cfg, obj)
                }
            };
        obj.advance_local_watermark(self.self_id, &server_watermarks);
        let prop: StateLE<ObjLD, PeerID> = (obj, cfg).into();
        trace!(
            "begin new lattice-agreement proposal with {:?} members in config",
            prop.config().members().len()
        );
        trace!("tidmgr wq_set: {:?}", self.tidmgr.read().await.wq_set);
        self.participant.write().await.propose(&prop)
    }

    pub(crate) async fn propose_step(&self, msgs: &[Msg]) {
        let mut out: Vec<crate::agreement::Msg> = Vec::new();
        self.participant
            .write()
            .await
            .propose_step(msgs.iter(), &mut out);
        trace!(
            "lattice-agreement propose step, {} msgs in, {} msgs out",
            msgs.len(),
            out.len()
        );
        for msg in out {
            match msg {
                concorde::Message::Request { to, .. } | concorde::Message::Response { to, .. } => {
                    let ow = OneWay::LatticeAgreementMsg(msg);
                    self.connections
                        .read()
                        .await
                        .get(&to)
                        .unwrap()
                        .1
                        .enqueue_oneway(ow)
                        .await
                        .unwrap();
                }
                concorde::Message::Commit { .. } => {
                    for pair in self.connections.read().await.values().cloned() {
                        let ow = OneWay::LatticeAgreementMsg(msg.clone());
                        pair.1.enqueue_oneway(ow).await.unwrap();
                    }
                }
            }
        }
    }

    // Loops continuously reading from agreement_recv and feeding the concorde
    // state machine.
    // TODO: integrate better with edelcrantz messaging model
    // TODO: maybe insert a delay before cycling, and/or wait on received
    //       changes, or changes to own tidmgr.

    const AGREE_LOOP_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    pub(crate) async fn run_proposal_loop(&self, mut agreement_recv: UnboundedReceiver<Msg>) {
        while !agreement_recv.is_terminated() {
            self.begin_new_proposal().await;
            self.propose_step(&[]).await;
            while let Some(msg) = agreement_recv.next().await {
                self.propose_step(&[msg]).await;
                if self.participant.read().await.propose_is_fini() {
                    break;
                }
            }
            match self.get_current_agreement().await {
                None => (),
                Some(gwm) => {
                    self.publish(gwm).await;
                }
            }
            // TODO: This isn't how we want to throttle proposal loops.
            // let never = future::pending::<()>();
            // let _ = future::timeout(Self::AGREE_LOOP_DELAY, never).await;
        }
    }

    /// From paper - Algorithm 1, procedure 'Publish'
    /// Called from network receiving an updated gossip watermark.
    //
    // TODO: maybe don't return Svw, seems pointless / related to DC-exchange we're not doing.
    // TODO: "the DB servers automatically set all versions below Rwatermark as finalized"
    pub(crate) async fn publish(&self, new_gwm: GroupWatermarks) -> Svw {
        {
            trace!(
                "saw lattice-agreement publish group watermarks {:?}",
                new_gwm
            );
            let (lock, cvar) = &*self.group_wm;
            let mut gwm = lock.lock().await;
            let max_vwm = new_gwm
                .visibility_watermark
                .max(gwm.visibility_watermark.clone());
            let max_rwm = new_gwm
                .replication_watermark
                .max(gwm.replication_watermark.clone());
            if max_rwm != gwm.replication_watermark {
                debug!("Rwatermark advancing to {:?}", max_rwm.0);
                gwm.replication_watermark = max_rwm;
            }
            if max_vwm != gwm.visibility_watermark {
                debug!("Vwatermark advancing to {:?}", max_vwm.0);
                gwm.visibility_watermark = max_vwm;
                // Wake up transactions that are waiting on vwatermark.
                cvar.notify_all();
            }
        }
        return self.tidmgr.write().await.svw();
    }
}
