// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

// Calls Math.log() to map a number into a subbucket.
// SubBucketLogIndexer has higher precision than plain LogIndexer because it limits floating point computation
// in the small range of 1 to 2. It can be used for scales up to 52 (max meaningful scale for double).

public class SubBucketLogIndexer extends SubBucketIndexer {
    final double scaleFactor; // See LogIndexer.

    public SubBucketLogIndexer(final int scale) {
        super(scale);
        scaleFactor = Math.scalb(1 / Math.log(2), scale);
    }

    @Override
    long getSubBucketIndex(final long mantissa) {
        final double value = DoubleFormat.makeDouble1To2(mantissa);
        return (long) (Math.log(value) * scaleFactor);
    }

    @Override
    long getSubBucketStartMantissa(final long subBucketIndex) {
        return DoubleFormat.getMantissa(scaledBasePower(scale, subBucketIndex));
    }
}
