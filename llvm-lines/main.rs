pub use clepsydra::*;

#[path = "../src/test.rs"]
mod test;

// This version of the test is a non-#[cfg(test)] binary so that
// cargo llvm-lines can find it. See also ../src/main.rs

pub fn main() {
    test::multi_txn_test();
}