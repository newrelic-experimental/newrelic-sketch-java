// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

public interface BucketIndexer {
    // Map a number to a bucket. Returned index may be negative.
    long getBucketIndex(final double value);

    // Returns a bucket's start bound.
    double getBucketStart(final long index);

    // Returns a bucket's end bound.
    double getBucketEnd(final long index);
}
