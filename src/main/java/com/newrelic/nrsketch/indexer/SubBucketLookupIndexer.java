// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Convert linear subbuckets to log subbuckets via lookup tables.
// This indexer is fast because it uses simple integer operations for mapping,
// but it costs more space because of the lookup tables.

public class SubBucketLookupIndexer extends SubBucketIndexer {
    // Up to this scale, we use static lookup tables, so there is no extra runtime memory cost.
    // Above this level, each instance of SubBucketLookupIndexer costs 16 * 2^scale bytes on lookup tables.
    public static final int PREFERRED_MAX_SCALE = 6;

    // LookupTable size at scale 6 is about 1KB. Total static table size is less than 2KB.
    // The static tables cover commonly used scales, with relative error from .5% (scale 6) to 4% (scale 3)
    private static final int MIN_STATIC_TABLE_SCALE = 3; // inclusive
    private static final int MAX_STATIC_TABLE_SCALE = PREFERRED_MAX_SCALE; // inclusive

    // LookupTable[] array is indexed by "scale - MIN_STATIC_TABLE_SCALE"
    private static final LookupTable[] STATIC_TABLES = initStaticTables();

    private static class LookupTable {
        private final long[] logBucketEndArray;  // End bound of the log buckets. 2^scale entries
        private final int[] logBucketIndexArray; // Index of the log bucket where a linear bucket starts. 2^(scale+1) entries
        private final int mantissaShift;         // "mantissa >>> mantissaShift" yields linear bucket index.

        private LookupTable(final int scale) {
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

            logBucketEndArray = new long[1 << scale];
            logBucketIndexArray = new int[1 << linearScale];

            mantissaShift = DoubleFormat.MANTISSA_BITS - linearScale;

            for (int i = 0; i < logBucketEndArray.length; i++) {
                logBucketEndArray[i] = i + 1 < logBounds.length ? logBounds[i + 1] : Long.MAX_VALUE;
            }

            for (int i = 0; i < logBucketIndexArray.length; i++) {
                final long linearBucketStart = ((long) i) << mantissaShift;
                logBucketIndexArray[i] = binarySearch(logBounds, linearBucketStart);
            }
        }
    }

    private static LookupTable[] initStaticTables() {
        final LookupTable[] lookupTables = new LookupTable[MAX_STATIC_TABLE_SCALE - MIN_STATIC_TABLE_SCALE + 1];

        for (int i = 0; i < lookupTables.length; i++) {
            lookupTables[i] = new LookupTable(MIN_STATIC_TABLE_SCALE + i);
        }
        return lookupTables;
    }

    private static LookupTable getLookupTable(final int scale) {
        if (scale >= MIN_STATIC_TABLE_SCALE && scale <= MAX_STATIC_TABLE_SCALE) {
            return STATIC_TABLES[scale - MIN_STATIC_TABLE_SCALE];
        }
        return new LookupTable(scale);
    }

    // Not using a LookupTable object, in order to avoid a level of indirection.
    private final long[] logBucketEndArray;  // End bound of the log buckets.
    private final int[] logBucketIndexArray; // Index of the log bucket where a linear bucket starts.
    private final int mantissaShift;

    public SubBucketLookupIndexer(final int scale) {
        super(scale);
        final LookupTable lookupTable = getLookupTable(scale);

        logBucketEndArray = lookupTable.logBucketEndArray;
        logBucketIndexArray = lookupTable.logBucketIndexArray;
        mantissaShift = lookupTable.mantissaShift;
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
            bounds[i] = DoubleFormat.getMantissa(scaledBasePower(scale, i));
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