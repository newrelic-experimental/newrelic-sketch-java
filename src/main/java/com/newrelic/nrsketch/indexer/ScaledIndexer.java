// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

// Scaled indexer maps between a value and a bucket index.
// Higher scales have smaller buckets. When moving from scale n to n - 1, we do a 2 to 1 bucket merging.
// Specifically, if a value is mapped to index i at scale n, at scale n - 1, it will be mapped to "i >> 1".
// Index may be negative. "i >> 1" preserves the sign.
// This property allows a histogram to down scale without introducing artifacts.

public interface ScaledIndexer {
    // Returns scale of the indexer
    int getScale();

    // Returns relative error upper bound for percentiles generated from this histogram.
    // relativeError = Math.abs(reportedValue - actualValue) / reportedValue
    double getPercentileRelativeError();

    // Map a number to a bucket. Returned index may be negative.
    long getBucketIndex(final double value);

    // Returns a bucket's start bound.
    double getBucketStart(final long index);

    // Returns a bucket's end bound.
    double getBucketEnd(final long index);
}
