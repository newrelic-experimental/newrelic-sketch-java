// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Calls Math.log() to map a number into a subbucket.
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
        return DoubleFormat.getMantissa(getBucketStart(scale, subBucketIndex));
    }
}
