// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import static com.newrelic.nrsketch.indexer.DoubleFormat.MANTISSA_SHIFT;

// IEEE format represents a double as a binary floating point number in the form of mantissa * 2^exponent,
// where mantissa is normally between 1 and 2. This indexer divides the mantissa space into log scale subbuckets.
// Bucket index is made of the concatenation of exponent and subbucket index.
//
public abstract class SubBucketIndexer extends ScaledExpIndexer {
    protected final int exponentShift;
    protected final long subBucketIndexMask; // Subbucket index bits in bucket index.
    protected final long indexBiasOffset;

    // Mantissa space is divided into 2^scale subbuckets. base = 2 ^ (2 ^ -scale). Scale must be positive.
    public SubBucketIndexer(final int scale) {
        super(scale);
        if (scale < 1 || scale > MAX_SCALE) {
            throw new IllegalArgumentException("Scale " + scale + " out of valid range of " + 1 + " and " + MAX_SCALE);
        }
        exponentShift = DoubleFormat.MANTISSA_BITS - scale;
        subBucketIndexMask = (1L << scale) - 1;
        indexBiasOffset = DoubleFormat.EXPONENT_BIAS << scale;
    }

    abstract long getSubBucketIndex(final long mantissa);

    abstract long getSubBucketStartMantissa(final long subBucketIndex);

    // Normal vs subnormal numbers.
    // Normal:    value = 1.XXXX * 2^exponent, where XXXX is mantissa (52 bit) and "1." in mantissa is implicit.
    // Subnormal: value = 0.XXXX * 2^-1022, where -1022 is Double.MIN_EXPONENT and XXXX is mantissa (52 bit)
    // Physically, the exponent field in a subnormal double is 0, below the lowest normal exponent (physically 1, logically -1022)
    // Thus a physical exponent of 0 indicates subnormal double or zero. Since the indexer does not handle 0,
    // the 0 exponent condition is used to detect subnormals.
    @Override
    public long getBucketIndex(final double d) {
        final long asLong = Double.doubleToRawLongBits(d);
        if ((asLong & DoubleFormat.EXPONENT_MASK) != 0) { // Normal doubles. Most likely branch first.
            return (((asLong & DoubleFormat.EXPONENT_MASK) >>> exponentShift)
                    | getSubBucketIndex(DoubleFormat.getMantissaFromLong(asLong))) - indexBiasOffset;
        }
        return getBucketIndexForSubnormalAsLong(asLong);
    }

    @Override
    public double getBucketStart(long index) {
        if (index >= getMinIndexNormal(scale)) { // Normal doubles
            index += indexBiasOffset;
            return Double.longBitsToDouble(
                    ((index << exponentShift) & DoubleFormat.EXPONENT_MASK)
                            | getSubBucketStartMantissa(index & subBucketIndexMask));
        }
        return getBucketStartForSubnormal(index);
    }

    private long getBucketIndexForSubnormalAsLong(final long asLong) {
        final int extraExponent = Long.numberOfLeadingZeros(asLong << MANTISSA_SHIFT) + 1;
        final long exponent = Double.MIN_EXPONENT - extraExponent; // Normalized
        final long mantissa = (asLong << (MANTISSA_SHIFT + extraExponent)) >>> MANTISSA_SHIFT; // Normalized
        return (exponent << scale) | getSubBucketIndex(mantissa);
    }

    private double getBucketStartForSubnormal(long index) {
        final int extraExponent = Double.MIN_EXPONENT - (int) (index >> scale);
        return Double.longBitsToDouble((
                getSubBucketStartMantissa(index & subBucketIndexMask) >>> extraExponent)
                | (1L << (DoubleFormat.MANTISSA_BITS - extraExponent)));
    }
}
