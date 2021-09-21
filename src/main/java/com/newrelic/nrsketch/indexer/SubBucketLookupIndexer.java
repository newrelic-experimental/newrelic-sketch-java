// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;

// Convert linear subbuckets to log subbuckets via lookup tables.
// This indexer is fast because it uses simple integer operations for mapping,
// but it costs more space because of the lookup tables.
// See Indexer.md in this repo for detailed documentation on this method.

public class SubBucketLookupIndexer extends SubBucketIndexer {
    // Up to this scale, we use static lookup tables, so there is no extra runtime memory cost.
    // Above this scale, each instance of SubBucketLookupIndexer costs 16 * 2^scale bytes on lookup tables.
    public static final int PREFERRED_MAX_SCALE = 6;

    // LookupTable size at scale 6 is about 1KB. Total static table size is less than 2KB.
    // The static tables cover commonly used scales, with relative error from .5% (scale 6) to 4% (scale 3)
    static final int MIN_STATIC_TABLE_SCALE = 3; // inclusive
    static final int MAX_STATIC_TABLE_SCALE = PREFERRED_MAX_SCALE; // inclusive

    // LookupTable[] array is indexed by "scale - MIN_STATIC_TABLE_SCALE"
    static final LookupTable[] STATIC_TABLES = initStaticTables();

    static class LookupTable {
        final long[] logBucketEndArray;  // End bound of the log buckets. Exclusive end.
        final int[] logBucketIndexArray; // Index of the log bucket where a linear bucket starts.

        private LookupTable(final int scale) {
            // "+ 1" to guarantee that linear bucket width is smaller than the smallest (1st) log bucket.
            // See Indexer.md for more info.
            int linearScale = scale + 1;

            // Array size must fit in an Integer.
            if ((1L << linearScale) > Integer.MAX_VALUE) {
                throw new RuntimeException("Scale is too big. linearScale=" + linearScale + " scale=" + scale);
            }

            logBucketEndArray = new long[1 << scale];
            logBucketIndexArray = new int[1 << linearScale];

            long prevLogBucketEnd = 0;

            for (int i = 0; i < logBucketEndArray.length; i++) {
                logBucketEndArray[i] = (i == logBucketEndArray.length - 1) ? 1L << DoubleFormat.MANTISSA_BITS
                        : DoubleFormat.getMantissa(scaledBasePower(scale, i + 1));

                if (logBucketEndArray[i] < prevLogBucketEnd) {
                    throw new RuntimeException("logBucketEnd going backward");
                }
                prevLogBucketEnd = logBucketEndArray[i];
            }

            final int mantissaShift = DoubleFormat.MANTISSA_BITS - linearScale;
            int logBucketIndex = 0;

            if ((1L << mantissaShift) >= logBucketEndArray[0]) {
                throw new RuntimeException("Linear bucket width >= log bucket width");
            }

            for (int i = 0; i < logBucketIndexArray.length; i++) {
                final long linearBucketStart = ((long) i) << mantissaShift;
                while (logBucketEndArray[logBucketIndex] <= linearBucketStart) {
                    logBucketIndex++;
                }
                logBucketIndexArray[i] = logBucketIndex;
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

    static LookupTable getLookupTable(final int scale) {
        if (scale >= MIN_STATIC_TABLE_SCALE && scale <= MAX_STATIC_TABLE_SCALE) {
            return STATIC_TABLES[scale - MIN_STATIC_TABLE_SCALE];
        }
        return new LookupTable(scale);
    }

    // Not using a LookupTable object, in order to avoid a level of indirection.
    private final long[] logBucketEndArray;  // End bound of the log buckets. Exclusive end.
    private final int[] logBucketIndexArray; // Index of the log bucket where a linear bucket starts.
    private final int mantissaShift;

    public SubBucketLookupIndexer(final int scale) {
        super(scale);
        final LookupTable lookupTable = getLookupTable(scale);

        logBucketEndArray = lookupTable.logBucketEndArray;
        logBucketIndexArray = lookupTable.logBucketIndexArray;

        mantissaShift = DoubleFormat.MANTISSA_BITS - (scale + 1);
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
}