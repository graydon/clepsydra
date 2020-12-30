// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

//! This module defines arithmetic of quorum sizes and tests that that
//! arithmetic has expected values. It's very small but this is one of the minor
//! bits of fiddly arithmetic that it's easy to mess up in a quorum-based
//! system.

// Many systems derive their operational thresholds from a user-supplied value
// `f` for the number of tolerable failures, then reporting to the user a given
// set of peers to provide.
//
// We take a slightly different approach and derive our thresholds from the
// number of replicas that exist, _reporting_ the number of tolerable failures
// with that number of replicas.
//
// We do this because peers may join or leave the system dynamically and we want
// to be able to answer questions about quorum-sizes based on the number of
// replicas we currently _have_ rather than the ideal configuration.
//
// The user can be notified -- and the system can optionally halt -- if we're
// operating with fewer peers than one needs to achieve the user's preference
// for failure-tolerance, but if we're at-or-above it we still want to calculate
// the thresholds for switching between algorithm modes based on the number of
// peers we _have_.
//
//
// Formulating in terms of failures tolerated gives:
//
//  failures     | total     | majority | super                |
//  tolerated    | replicas  | quorum   | quorum               |
//  `f`          | `2f + 1`  | `f + 1`  | 'ceil(3/2 * f) + 1`  |
//  -------------|-----------|----------|----------------------|
//  0            |  1        |  1       |  1                   |
//  1            |  3        |  2       |  3                   |
//  2            |  5        |  3       |  4                   |
//  3            |  7        |  4       |  6                   |
//  4            |  9        |  5       |  7                   |
//
//
// Reformulated in terms of number of replicas gives:
//
//  failures     | total     | majority     | super         |
//  tolerated    | replicas  | quorum       | quorum        |
//  `(n-1)/2`    | `n`       | `(n/2) + 1`  | `(3n/4) + 1`  |
//  -------------|-----------|--------------|---------------|
//  0            |  1        |  1           |  1            |
//  0            |  2        |  2           |  2            |
//  1            |  3        |  2           |  3            |
//  1            |  4        |  3           |  4            |
//  2            |  5        |  3           |  4            |
//  2            |  6        |  4           |  5            |
//  3            |  7        |  4           |  6            |
//  3            |  8        |  5           |  7            |
//  4            |  9        |  5           |  7            |
//  4            | 10        |  6           |  8            |
//

// Returns the usize corresponding to ceil(x/y)
fn ceil_div(x: usize, y: usize) -> usize {
    if x == 0 {
        0
    } else {
        1 + ((x - 1) / y)
    }
}

pub(crate) fn failures_tolerated(replica_count: usize) -> usize {
    if replica_count == 0 {
        0
    } else {
        (replica_count - 1) / 2
    }
}

pub(crate) fn majority_quorum(replica_count: usize) -> usize {
    (replica_count / 2) + 1
}

pub(crate) fn super_quorum(replica_count: usize) -> usize {
    ((3 * replica_count) / 4) + 1
}

#[test]
fn test_quorums() {
    assert_eq!(failures_tolerated(1), 0);
    assert_eq!(failures_tolerated(2), 0);
    assert_eq!(failures_tolerated(3), 1);
    assert_eq!(failures_tolerated(4), 1);
    assert_eq!(failures_tolerated(5), 2);
    assert_eq!(failures_tolerated(6), 2);
    assert_eq!(failures_tolerated(7), 3);
    assert_eq!(failures_tolerated(8), 3);
    assert_eq!(failures_tolerated(9), 4);
    assert_eq!(failures_tolerated(10), 4);

    assert_eq!(majority_quorum(1), 1);
    assert_eq!(majority_quorum(2), 2);
    assert_eq!(majority_quorum(3), 2);
    assert_eq!(majority_quorum(4), 3);
    assert_eq!(majority_quorum(5), 3);
    assert_eq!(majority_quorum(6), 4);
    assert_eq!(majority_quorum(7), 4);
    assert_eq!(majority_quorum(8), 5);
    assert_eq!(majority_quorum(9), 5);
    assert_eq!(majority_quorum(10), 6);

    assert_eq!(super_quorum(1), 1);
    assert_eq!(super_quorum(2), 2);
    assert_eq!(super_quorum(3), 3);
    assert_eq!(super_quorum(4), 4);
    assert_eq!(super_quorum(5), 4);
    assert_eq!(super_quorum(6), 5);
    assert_eq!(super_quorum(7), 6);
    assert_eq!(super_quorum(8), 7);
    assert_eq!(super_quorum(9), 7);
    assert_eq!(super_quorum(10), 8);
}
