// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Handles negative scale histogram, where index can be simply derived from exponent in double representation.
//
public class ExponentIndexer extends ScaledExpIndexer {
    private final int exponentShift;

    public static final int MIN_SCALE = -DoubleFormat.EXPONENT_BITS;

    // Scale must be 0 or negative. base = 2 ^ (2 ^ -scale).
    public ExponentIndexer(final int scale) {
        super(scale);
        if (scale > 0 || scale < MIN_SCALE) {
            throw new IllegalArgumentException("Scale " + scale + " out of valid range of " + MIN_SCALE + " and " + 0);
        }
        this.exponentShift = -scale;
    }

    // Sign bit of value is ignored
    @Override
    public long getBucketIndex(final double value) {
        return DoubleFormat.getExponent(value) >> exponentShift; // Use ">>" to preserve sign of exponent.
    }

    @Override
    public double getBucketStart(final long index) {
        // Guard against exponent underflow.
        final long exponent = Math.max((index << exponentShift), Double.MIN_EXPONENT);
        return Double.longBitsToDouble((exponent + DoubleFormat.EXPONENT_BIAS) << DoubleFormat.MANTISSA_BITS);
    }
}
