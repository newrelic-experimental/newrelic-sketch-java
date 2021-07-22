// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

// Use Math.log(). This is the canonical exponential indexer
public class LogIndexer extends ScaledExpIndexer {
    protected final double base;
    protected final double baseLog;

    public LogIndexer(final int scale) {
        super(scale);
        base = getBase(scale);
        baseLog = Math.log(base);
    }

    @Override
    public long getBucketIndex(final double value) {
        // Use floor() to round toward -Infinity. Plain "(long)" rounds toward 0.
        // Example: (long) -1.5 = -1; floor(-1.5) = -2
        return (long) Math.floor(Math.log(value) / baseLog);
    }

    @Override
    public double getBucketStart(final long index) {
        return Math.pow(base, index); // base^index
    }
}
