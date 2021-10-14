// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

// Use Math.log(). This is the canonical exponential indexer.
// However, due to floating point computational errors, it is only good from scale -10 to 43.
// See BucketIndexerTest.testLogIndexerScales()
// In contrast, SubBucketLogIndexer can be used for scales up to 52 (max meaningful scale for double),
// because it limits floating point computation in the small range of 1 to 2.

public class LogIndexer extends ScaledExpIndexer {
    // See also ScaledExpIndexer.scaledBasePower() on avoiding using base as intermediate result.
    // index = log(value) / log(base)
    // = log(value) / log(2^(2^-scale))
    // = log(value) / (2^-scale * log(2))
    // = log(value) * (1/log(2) * 2^scale)
    // = log(value) * scaleFactor  // scaleFactor = (1/log(2) * 2^scale)
    // Because multiplication is faster than division, we define scaleFactor as a multiplier.
    protected final double scaleFactor;

    public LogIndexer(final int scale) {
        super(scale);
        scaleFactor = Math.scalb(1 / Math.log(2), scale);
    }

    @Override
    public long getBucketIndex(final double value) {
        // Use floor() to round toward -Infinity. Plain "(long)" rounds toward 0.
        // Example: (long) -1.5 = -1; floor(-1.5) = -2
        return (long) Math.floor(Math.log(Math.abs(value)) * scaleFactor);
    }

    @Override
    public double getBucketStart(final long index) {
        return scaledBasePower(scale, index);
    }
}
