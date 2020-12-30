// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

pub use clepsydra::*;

#[cfg(test)]
mod test;

// We have this test external from the crate for two reasons:
//
//  1. to make sure the public API is usable without accidentally relying on
//     crate-level-visibility stuff.
//
//  2. to use `cargo llvm-lines` in the llvm-lines/ subdirectory, to measure
//     footprint of final codegen when everything's actually instantiated.

#[test]
fn multi_txn_test() {
    test::multi_txn_test();
}

pub fn main() {
    println!("please run `cargo test` instead");
}
