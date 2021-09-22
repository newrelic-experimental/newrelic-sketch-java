// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

import static com.newrelic.nrsketch.DoubleFormat.MANTISSA_SHIFT;

// Handles negative scale histogram, where index can be simply derived from exponent in double representation.
//
public class ExponentIndexer extends ScaledExpIndexer {
    private final int exponentShift;

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
        final long asLong = Double.doubleToRawLongBits(value);
        if ((asLong & DoubleFormat.EXPONENT_MASK) != 0) { // Normal doubles. Most likely branch first.
            return DoubleFormat.getExponentFromLong(asLong) >> exponentShift; // Use ">>" to preserve sign of exponent.
        }
        // Subnormals
        final int extraExponent = Long.numberOfLeadingZeros(asLong << MANTISSA_SHIFT) + 1;
        final long exponent = Double.MIN_EXPONENT - extraExponent; // Normalized
        return exponent >> exponentShift;
    }

    @Override
    public double getBucketStart(final long index) {
        final long exponentFloor = index << exponentShift;
        if (exponentFloor >= Double.MIN_EXPONENT) { // Normal double
            return Double.longBitsToDouble((exponentFloor + DoubleFormat.EXPONENT_BIAS) << DoubleFormat.MANTISSA_BITS);
        }
        // Subnormals
        final long exponent = Math.max(exponentFloor, DoubleFormat.MIN_SUBNORMAL_EXPONENT);
        final int extraExponent = (int) (Double.MIN_EXPONENT - exponent);
        return Double.longBitsToDouble(1L << (DoubleFormat.MANTISSA_BITS - extraExponent));
    }
}
