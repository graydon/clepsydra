// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use pergola::DefTraits;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// An `ExtVal` extends the normal [Lang::Val] type with two extra sentinel
/// values to represent not-yet-written or deleted data in a [crate::Store].
#[serde(bound = "")]
#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExtVal<L: Lang> {
    Initial,
    Defined(L::Val),
    Deleted,
}

/// A `Lang` provides a skeletal interface between the database and a given
/// client's query language. Clients of this crate need to define a `Lang` that
/// models their language at least enough that the database can call it back to
/// extract key-sets and perform (deterministic) evaluation.
pub trait Lang: DefTraits + 'static {
    type Key: DefTraits + Send + Sync;
    type Val: DefTraits + Send + Sync;
    type Stmt: DefTraits + Send + Sync;
    type Expr: DefTraits + Send + Sync;

    fn get_write_set(s: &Self::Stmt) -> BTreeMap<Self::Key, Self::Expr>;
    fn get_read_set(e: &Self::Expr) -> BTreeSet<Self::Key>;
    fn get_eval_set(s: &Self::Stmt) -> BTreeSet<Self::Key>;
    fn eval_expr(
        e: &Self::Expr,
        vals: &[Self::Val],
        env: &BTreeMap<Self::Key, ExtVal<Self>>,
    ) -> ExtVal<Self>;
}
