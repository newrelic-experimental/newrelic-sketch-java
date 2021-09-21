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

        // Combine the uncommon case of zero and subnormals into one inexpensive check.
        if (DoubleFormat.isSubnormalOrZeroFromLong(asLong)) {
            if (value == 0) {
                throw new IllegalArgumentException("ExponentIndexer cannot handle zero");
            }
            final int extraExponent = Long.numberOfLeadingZeros(asLong << MANTISSA_SHIFT) + 1;
            final long exponent = Double.MIN_EXPONENT - extraExponent; // Normalized
            return exponent >> exponentShift;
        }
        return DoubleFormat.getExponentFromLong(asLong) >> exponentShift; // Use ">>" to preserve sign of exponent.
    }

    @Override
    public double getBucketStart(final long index) {
        if (index < getMinIndexNormal(scale)) {
            final long exponent = Math.max((index << exponentShift), DoubleFormat.MIN_SUBNORMAL_EXPONENT);
            final int extraExponent = (int)(Double.MIN_EXPONENT - exponent);
            return Double.longBitsToDouble(1L << (DoubleFormat.MANTISSA_BITS - extraExponent));
        }
        // index << exponentShift produces a number ending with "scale" zero bits.
        // But MIN_EXPONENT -1022 does not always end on "scale" zero bits.
        // Thus we need to pull exponent to the actual min exponent.
        //
        final long exponent = Math.max((index << exponentShift), Double.MIN_EXPONENT);
        return Double.longBitsToDouble((exponent + DoubleFormat.EXPONENT_BIAS) << DoubleFormat.MANTISSA_BITS);
    }
}
