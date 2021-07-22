// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// IEEE format represents a double as a binary floating point number in the form of mantissa * 2^exponent,
// where mantissa is normally between 1 and 2. This indexer divides the mantissa space into log scale subbuckets.
// Bucket index is made of the concatenation of exponent and subbucket index.
//
// In the "subnormal" case, mantissa is logically 0 to 1, instead of 1 to 2. This indexer has no special processing
// on subnormal numbers. In other words, it uses the [1, 2] mantissa bounds to divide the subnormal [0, 1] range.
// Although the value to index and index to bucket start mapping still works in the subnormal range,
// the bucket bounds in the range does not meet the "bound = base ^ index" formula.
// Applications have 2 choices:
// 1. Divert subnormal numbers to a special bucket like the bucket for value 0, bypassing the usual indexing.
//    This option has low resolution on subnormal numbers, but preserves bucket bound formula on all indexed buckets.
// 2. Send subnormal numbers to this indexer and then to the relevant buckets. There are 2^scale buckets
//    in the subnormal range, but these subnormal buckets do not meet the normal "bound = base ^ index" formula.
//
// Changing this indexer to interpret subnormal numbers using the [0, 1] mantissa range to return normal buckets is
// possible. But the additional complexity and performance cost (adding an "if subnormal" test on critical path) is
// deemed not worth the benefit at this time.
//
public abstract class SubBucketIndexer extends ScaledExpIndexer {
    protected final int exponentShift;
    protected final long subBucketIndexMask; // Subbucket index bits in bucket index.
    protected final long indexBiasOffset;

    public static final int MAX_SCALE = DoubleFormat.MANTISSA_BITS; // Max resolution of mantissa

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

    // Sign bit is ignored.
    @Override
    public long getBucketIndex(final double d) {
        final long asLong = Double.doubleToRawLongBits(d);
        return (((asLong & DoubleFormat.EXPONENT_MASK) >>> exponentShift)
                | getSubBucketIndex(DoubleFormat.getMantissaFromLong(asLong))) - indexBiasOffset;
    }

    @Override
    public double getBucketStart(long index) {
        index += indexBiasOffset;
        return Double.longBitsToDouble(
                ((index << exponentShift) & DoubleFormat.EXPONENT_MASK)
                        | getSubBucketStartMantissa(index & subBucketIndexMask));
    }
}
