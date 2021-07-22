// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Calls Math.log() to map a number into a subbucket.
public class SubBucketLogIndexer extends SubBucketIndexer {
    final double base;
    final double baseLog;

    public SubBucketLogIndexer(final int scale) {
        super(scale);
        base = LogIndexer.getBase(scale);
        baseLog = Math.log(base);
    }

    @Override
    long getSubBucketIndex(final long mantissa) {
        final double value = DoubleFormat.makeDouble1To2(mantissa);
        return (long) (Math.log(value) / baseLog);
    }

    @Override
    long getSubBucketStartMantissa(final long subBucketIndex) {
        return DoubleFormat.getMantissa(Math.pow(base, subBucketIndex)); // base^index. Entry 0 is always 1
    }
}
