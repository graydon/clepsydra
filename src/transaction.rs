// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

//! A Transaction has a timestamp which also serves as its unique identifier --
//! the system assigns a GlobalTime per transaction and increments the
//! GlobalTime event counter (at least) after each such assignment.
//!
//! A Transaction also declares read and write sets (of keys), a set of
//! positional parameter values, and a language statement.

use crate::{GlobalTime, Lang};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Txn<L: Lang> {
    pub time: GlobalTime,
    pub stmt: L::Stmt,
    pub vals: Vec<L::Val>,
}
