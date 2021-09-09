// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Convert linear subbuckets to log subbuckets via lookup tables.
public class SubBucketLookupIndexer extends SubBucketIndexer {
    // This indexer runs more efficiently up to this scale.
    // To do: use static arrays for scales up to this level.
    public static final int PREFERRED_MAX_SCALE = 6;

    private final int[] logBucketIndexArray; // Index of the log bucket where a linear bucket starts.
    private final long[] logBucketEndArray;  // End bound of the log buckets.
    private final int mantissaShift;

    public SubBucketLookupIndexer(final int scale) {
        super(scale);
        final long[] logBounds = getLogBoundMantissas(scale);

        // Linear bucket width must be equal or smaller than smallest (1st) log bucket.
        int linearScale = scale;
        while ((1L << (DoubleFormat.MANTISSA_BITS - linearScale)) > logBounds[1]) {
            linearScale++;
        }

        // Expect nLinearSubBuckets = nLogSubBuckets * 2
        if (linearScale != scale + 1) {
            throw new RuntimeException("linearScale=" + linearScale + " scale=" + scale);
        }

        // nLinearSubBuckets must fit in an integer.
        if ((1L << linearScale) > Integer.MAX_VALUE) {
            throw new RuntimeException("Scale is too big. linearScale=" + linearScale + " scale=" + scale);
        }

        logBucketIndexArray = new int[1 << linearScale];
        logBucketEndArray = new long[1 << scale];

        mantissaShift = DoubleFormat.MANTISSA_BITS - linearScale;

        for (int i = 0; i < logBucketIndexArray.length; i++) {
            final long linearBucketStart = ((long) i) << mantissaShift;
            logBucketIndexArray[i] = binarySearch(logBounds, linearBucketStart);
        }

        for (int i = 0; i < logBucketEndArray.length; i++) {
            logBucketEndArray[i] = i + 1 < logBounds.length ? logBounds[i + 1] : Long.MAX_VALUE;
        }
    }

    @Override
    long getSubBucketIndex(final long mantissa) {
        final int linearArrayIndex = (int) (mantissa >>> mantissaShift);
        final int logBucketIndex = logBucketIndexArray[linearArrayIndex];
        return mantissa >= logBucketEndArray[logBucketIndex] ? logBucketIndex + 1 : logBucketIndex;
    }

    @Override
    long getSubBucketStartMantissa(final long index) {
        // Convert bucket end array to bucket start array.
        return index == 0 ? 0 : logBucketEndArray[(int) (index - 1)];
    }

    // Subdivide the [1, 2] space into nSubBuckets log subbuckets.
    // Return lower bound of each subbucket. Entry 0 starts at 1.
    public static long[] getLogBoundMantissas(final int scale) {
        final int nSubBuckets = 1 << scale;
        final long[] bounds = new long[nSubBuckets];
        for (int i = 0; i < nSubBuckets; i++) {
            bounds[i] = DoubleFormat.getMantissa(getBucketStart(scale, i) );
        }
        return bounds;
    }

    // Returns index where array[index] <= key < array[index + 1]
    public static int binarySearch(final long[] array, final long key) {
        int low = 0;
        int high = array.length - 1;

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midVal = array[mid];

            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return low - 1;  // key not found.
    }
}