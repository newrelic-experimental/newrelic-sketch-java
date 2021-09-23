// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BucketIndexerTest {
    public static final double DELTA = 1e-14; // Floating point comparison relative delta.
    public static final double FUDGE = 1 + 1e-13; // When value is on or near a bound, apply the fudge factor to make it fall on the expected side a bound for all indexers.

    @Test
    @SuppressFBWarnings(value = "JUA_DONT_ASSERT_INSTANCEOF_IN_TESTS")
    public void testIndexerOptions() {
        assertTrue(IndexerOption.LOG_INDEXER.getIndexer(8) instanceof LogIndexer);
        assertTrue(IndexerOption.LOG_INDEXER.getIndexer(0) instanceof LogIndexer);
        assertTrue(IndexerOption.LOG_INDEXER.getIndexer(-2) instanceof LogIndexer);

        assertTrue(IndexerOption.SUB_BUCKET_LOOKUP_INDEXER.getIndexer(8) instanceof SubBucketLookupIndexer);
        assertTrue(IndexerOption.SUB_BUCKET_LOOKUP_INDEXER.getIndexer(0) instanceof ExponentIndexer);
        assertTrue(IndexerOption.SUB_BUCKET_LOOKUP_INDEXER.getIndexer(-2) instanceof ExponentIndexer);

        assertTrue(IndexerOption.SUB_BUCKET_LOG_INDEXER.getIndexer(8) instanceof SubBucketLogIndexer);
        assertTrue(IndexerOption.SUB_BUCKET_LOG_INDEXER.getIndexer(0) instanceof ExponentIndexer);
        assertTrue(IndexerOption.SUB_BUCKET_LOG_INDEXER.getIndexer(-2) instanceof ExponentIndexer);

        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(12) instanceof SubBucketLogIndexer);
        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(8) instanceof SubBucketLogIndexer);
        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(SubBucketLookupIndexer.PREFERRED_MAX_SCALE) instanceof SubBucketLookupIndexer);
        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(4) instanceof SubBucketLookupIndexer);
        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(0) instanceof ExponentIndexer);
        assertTrue(IndexerOption.AUTO_SELECT.getIndexer(-2) instanceof ExponentIndexer);
    }

    @Test
    public void testGetBase() {
        assertEquals(1.0442737824274138, ScaledExpIndexer.getBase(4), 0);
        assertEquals(1.0905077326652577, ScaledExpIndexer.getBase(3), 0);
        assertEquals(1.189207115002721, ScaledExpIndexer.getBase(2), 0);
        assertEquals(1.4142135623730951, ScaledExpIndexer.getBase(1), 0);
        assertEquals(2, ScaledExpIndexer.getBase(0), 0);
        assertEquals(4, ScaledExpIndexer.getBase(-1), 0);
        assertEquals(16, ScaledExpIndexer.getBase(-2), 0);
        assertEquals(256, ScaledExpIndexer.getBase(-3), 0);
        assertEquals(65536, ScaledExpIndexer.getBase(-4), 0);
    }

    private static void assertTwice(final long expected, final long expected2, final long actual) {
        assertEquals(expected, actual);
        assertEquals(expected2, actual);
    }

    @Test
    public void testMinMaxScale() {
        assertEquals(-11, ScaledExpIndexer.MIN_SCALE);
        assertEquals(52, ScaledExpIndexer.MAX_SCALE);
    }

    @Test
    public void testGetMaxIndex() {
        // Exactly at max exponent
        assertTwice(1023, 0x3FF, ScaledExpIndexer.getMaxIndex(0));
        assertTwice(16383, 0x3FFF, ScaledExpIndexer.getMaxIndex(4));
        assertTwice(63, 0x3F, ScaledExpIndexer.getMaxIndex(-4));

        assertEquals(0, ScaledExpIndexer.getMaxIndex(ScaledExpIndexer.MIN_SCALE));
    }

    @Test
    public void testGetMinIndex() {
        //System.out.println(String.format("0x%XL", ScaledExpIndexer.getMinIndex(-4)));
        assertTwice(-1022, 0xFFFFFFFFFFFFFC02L, ScaledExpIndexer.getMinIndexNormal(0));
        assertTwice(-16352, 0xFFFFFFFFFFFFC020L, ScaledExpIndexer.getMinIndexNormal(4));
        assertTwice(-64, 0xFFFFFFFFFFFFFFC0L, ScaledExpIndexer.getMinIndexNormal(-4));

        assertTwice(-1074, 0xFFFFFFFFFFFFFBCEL, ScaledExpIndexer.getMinIndex(0));
        assertTwice(-17184, 0xFFFFFFFFFFFFBCE0L, ScaledExpIndexer.getMinIndex(4));
        assertTwice(-68, 0xFFFFFFFFFFFFFFBCL, ScaledExpIndexer.getMinIndex(-4));

        assertEquals(-1, ScaledExpIndexer.getMinIndexNormal(ScaledExpIndexer.MIN_SCALE));
        assertEquals(-1, ScaledExpIndexer.getMinIndex(ScaledExpIndexer.MIN_SCALE));
    }

    @Test
    public void testLookupTable() {
        for (int scale = 1; scale <= 6; scale++) {
            testLookupTable(scale);
        }
    }

    private void testLookupTable(final int scale) {
        final SubBucketLookupIndexer.LookupTable lookupTable = SubBucketLookupIndexer.getLookupTable(scale);
        final int nLogBuckets = 1 << scale;
        final int nLinearBuckets = 1 << (scale + 1);

        assertEquals(nLogBuckets, lookupTable.logBucketEndArray.length);
        assertEquals(nLinearBuckets, lookupTable.logBucketIndexArray.length);

        double base = 0;
        for (int i = 0; i < nLogBuckets; i++) {
            final long mantissa = lookupTable.logBucketEndArray[i];
            final double d = DoubleFormat.makeDouble1To2(mantissa);
            if (i == 0) {
                base = d;
            } else if (i == nLogBuckets - 1) {
                assertEquals(1L << DoubleFormat.MANTISSA_BITS, mantissa);
            } else {
                assertDoubleEquals(d, DoubleFormat.makeDouble1To2(lookupTable.logBucketEndArray[i - 1]) * base, DELTA);
            }
        }
        assertDoubleEquals(2.0, Math.pow(base, nLogBuckets), DELTA);

        for (int i = 0; i < nLinearBuckets; i++) {
            final double linearBucketStart = 1 + ((double) i / nLinearBuckets);
            final int logBucket = lookupTable.logBucketIndexArray[i];

            final double logBucketStart = logBucket == 0 ? 1 : DoubleFormat.makeDouble1To2(lookupTable.logBucketEndArray[logBucket - 1]);
            final double logBucketEnd = logBucket == nLogBuckets - 1 ? 2 : DoubleFormat.makeDouble1To2(lookupTable.logBucketEndArray[logBucket]);

            assertTrue(linearBucketStart >= logBucketStart);
            assertTrue(linearBucketStart < logBucketEnd);
        }
    }

    @Test
    public void testGetLookupTable() {
        int staticCount = 0;
        for (int scale = 1; scale <= SubBucketLookupIndexer.MAX_STATIC_TABLE_SCALE + 3; scale++) {
            final SubBucketLookupIndexer.LookupTable lookupTable = SubBucketLookupIndexer.getLookupTable(scale);
            if (scale >= SubBucketLookupIndexer.MIN_STATIC_TABLE_SCALE && scale <= SubBucketLookupIndexer.MAX_STATIC_TABLE_SCALE) {
                assertSame(lookupTable, SubBucketLookupIndexer.STATIC_TABLES[scale - SubBucketLookupIndexer.MIN_STATIC_TABLE_SCALE]);
                staticCount++;
            }
        }
        assertEquals(4, staticCount);
    }

    @Test
    public void testSubBucketLogIndexer() {
        for (int scale = 1; scale <= 10; scale++) {
            compareIndexers(new LogIndexer(scale), new SubBucketLogIndexer(scale));
        }
    }

    @Test
    public void testSubBucketLookupIndexer() {
        for (int scale = 1; scale <= 10; scale++) {
            compareIndexers(new LogIndexer(scale), new SubBucketLookupIndexer(scale));
        }
    }

    @Test
    public void testExponentIndexer() {
        // Scales below -8 will cause number overflow in testSelectedValues() because of very large base.
        for (int scale = 0; scale >= -8; scale--) {
            compareIndexers(new LogIndexer(scale), new ExponentIndexer(scale));
        }
    }

    private static void testSelectedValues(final ScaledExpIndexer idx) {
        final double base = idx.getBase();
        final double baseSquareRoot = Math.sqrt(base);

        assertLongEquals(2, idx.getBucketIndex(base * base * baseSquareRoot), 0);
        assertLongEquals(2, idx.getBucketIndex(base * base * FUDGE), 0);
        assertLongEquals(1, idx.getBucketIndex(base * baseSquareRoot), 0);
        assertLongEquals(1, idx.getBucketIndex(base * FUDGE), 0);
        assertLongEquals(0, idx.getBucketIndex(baseSquareRoot), 0);
        assertLongEquals(0, idx.getBucketIndex(1), 0);

        assertLongEquals(-1, idx.getBucketIndex(1 / baseSquareRoot), 0);
        assertLongEquals(-1, idx.getBucketIndex(1 / base * FUDGE), 0);
        assertLongEquals(-2, idx.getBucketIndex(1 / base / baseSquareRoot), 0);
        assertLongEquals(-2, idx.getBucketIndex(1 / base / base * FUDGE), 0);

        assertDoubleEquals(Double.MAX_VALUE, idx.getBucketEnd(idx.getMaxIndex()), 0);

        assertLongEquals(idx.getMaxIndex(), idx.getBucketIndex(Double.MAX_VALUE / FUDGE), 0);
        assertLongEquals(idx.getMinIndexNormal(), idx.getBucketIndex(Double.MIN_NORMAL), 0);
        assertLongEquals(idx.getMinIndex(), idx.getBucketIndex(Double.MIN_VALUE), 0);
    }

    private static void compareIndexers(final ScaledExpIndexer idx1, final ScaledExpIndexer idx2) {
        testSelectedValues(idx1);
        testSelectedValues(idx2);

        assertEquals(idx1.getScale(), idx2.getScale());
        assertEquals(idx1.getBase(), idx2.getBase(), 0);

        for (double value = 1e-6; value < 1; value += 1e-6) {
            compareAtValue(idx1, idx2, value);
        }

        for (double value = 1; value < 1e6; value += 1) {
            compareAtValue(idx1, idx2, value * FUDGE);
        }
    }

    private static void compareAtValue(final ScaledExpIndexer idx1, final ScaledExpIndexer idx2, final double value) {

        final long index = idx1.getBucketIndex(value);
        final double base = idx1.getBase();
        final double bucketStart = idx1.getBucketStart(index);
        final double bucketEnd = idx1.getBucketEnd(index);

        assertTrue(value >= bucketStart);
        assertTrue(value < bucketEnd);

        assertDoubleEquals(bucketStart * base, bucketEnd, DELTA);

        assertLongEquals(idx1.getBucketIndex(value), idx2.getBucketIndex(value), 0);

        assertDoubleEquals(idx1.getBucketStart(index), idx2.getBucketStart(index), DELTA);
        assertDoubleEquals(idx1.getBucketEnd(index), idx2.getBucketEnd(index), DELTA);
    }

    public static void assertLongEquals(final long a, final long b, final long delta) {
        final long diff = Math.abs(a - b);
        if (diff > delta) {
            fail("a=" + a + " b=" + b + " delta=" + delta + " diff=" + diff);
        }
    }

    // Delta is relative
    public static void assertDoubleEquals(final double a, final double b, final double delta) {
        final double diff = Math.abs(a - b);
        if (diff > Math.abs(a * delta)) {
            fail("a=" + a + " b=" + b + " delta=" + delta + " actual_delta=" + Math.abs(diff / a));
        }
    }

    @Test
    public void testLogIndexerScales() {
        // Limit to scale 43, because LogIndexer.getBucketStart() calls scaledPower(), which cannot handle
        // more than 52 significant bits on index, and index needs about 10 bits for input exponent.
        // This leaves 43 bits to resolve the input mantissa part.

        testScales(LogIndexer::new,
                1, // fromScale
                43, // toScale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
                1, // roundTripIndexDelta
                2); // powerOf2IndexDelta

        testScales(LogIndexer::new,
                1, // fromScale
                43, // toScale
                -500, // fromExponent, higher exponent needs higher powerOf2IndexDelta
                500, // toExponent
                1, // roundTripIndexDelta
                1); // powerOf2IndexDelta

        testScales(LogIndexer::new,
                -10, // fromScale. LogIndexer gives wrong result at scale -11 at Double.MIN end, likely because of float point overflow or underflow.
                0, // toScale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
                1, // roundTripIndexDelta
                1); // powerOf2IndexDelta

        testScalesSubnormal(LogIndexer::new,
                ScaledExpIndexer.MIN_SCALE, // fromScale
                43, // toScale
                1, // roundTripIndexDelta
                0); // powerOf2IndexDelta
    }

    @Test
    public void testSubBucketLogIndexerScales() {
        testScales(SubBucketLogIndexer::new,
                1, // fromScale
                ScaledExpIndexer.MAX_SCALE, // toScale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
                1, // roundTripIndexDelta
                0); // powerOf2IndexDelta

        testScalesSubnormal(SubBucketLogIndexer::new,
                1, // fromScale
                ScaledExpIndexer.MAX_SCALE, // toScale
                1, // roundTripIndexDelta
                0); // powerOf2IndexDelta
    }

    @Test
    public void testSubBucketLookupIndexerScales() {
        testScales(SubBucketLookupIndexer::new,
                1, // fromScale
                20, // toScale. Scales above 20 would use too much memory
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
                0, // roundTripIndexDelta
                0); // powerOf2IndexDelta

        testScalesSubnormal(SubBucketLookupIndexer::new,
                1, // fromScale
                20, // toScale. Scales above 20 would use too much memory
                1, // roundTripIndexDelta
                0); // powerOf2IndexDelta
    }

    @Test
    public void testExponentIndexerScales() {
        testScales(ExponentIndexer::new,
                ScaledExpIndexer.MIN_SCALE,
                0, // toScale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
                0, // roundTripIndexDelta
                0); // powerOf2IndexDelta

        testScalesSubnormal(ExponentIndexer::new,
                ScaledExpIndexer.MIN_SCALE,
                0, // toScale
                0, // roundTripIndexDelta
                0); // powerOf2IndexDelta
    }

    private void testScales(final Function<Integer, ScaledExpIndexer> indexerMaker,
                            final int fromScale,
                            final int toScale,
                            final int fromExponent,
                            final int toExponent,
                            final long roundTripIndexDelta,
                            final long powerOf2IndexDelta
    ) {
        final double squareRootOf2 = Math.pow(2, .5);

        for (int scale = fromScale; scale <= toScale; ++scale) {
            final ScaledExpIndexer indexer = indexerMaker.apply(scale);
            final long indexesPerPowerOf2 = scale >= 0 ? (1L << scale) : 0;

            // Test bucket start at 1 and 2
            assertEquals(1, indexer.getBucketStart(0), 0);
            if (scale >= 0) {
                assertEquals(2, indexer.getBucketStart(indexesPerPowerOf2), 0);
            }

            // Test bucket -1 and 0.
            assertEquals(-1, indexer.getBucketIndex(Math.nextDown(1D)));
            assertEquals(0, indexer.getBucketIndex(1D));

            // Test min and max.
            final long maxIndex = ScaledExpIndexer.getMaxIndex(scale);
            final long minIndexNormal = ScaledExpIndexer.getMinIndexNormal(scale);
            final long minIndex = ScaledExpIndexer.getMinIndex(scale);

            // Max, min normal, min value to index
            assertLongEquals(maxIndex, indexer.getBucketIndex(Double.MAX_VALUE), powerOf2IndexDelta); // LogIndexer needs this delta
            assertLongEquals(minIndexNormal, indexer.getBucketIndex(Double.MIN_NORMAL), 0);
            assertLongEquals(minIndex, indexer.getBucketIndex(Double.MIN_VALUE), 0);

            // Max, min normal round trip
            assertLongEquals(maxIndex, indexer.getBucketIndex(indexer.getBucketStart(maxIndex)), roundTripIndexDelta);
            assertLongEquals(minIndexNormal, indexer.getBucketIndex(indexer.getBucketStart(minIndexNormal)), roundTripIndexDelta);

            // Max index bucket end to value
            assertDoubleEquals(Double.MAX_VALUE, indexer.getBucketEnd(maxIndex), 0);

            // Min index to value. LogIndexer is not accurate on such small numbers
            if (!(indexer instanceof LogIndexer)) {
                assertDoubleEquals(Double.MIN_VALUE, indexer.getBucketStart(minIndex), 0);
                if (scale > 0) {
                    assertDoubleEquals(Double.MIN_NORMAL, indexer.getBucketStart(minIndexNormal), 0);
                }
            }

            // Test power of 2
            for (int exponent = fromExponent; exponent <= toExponent; ++exponent) {
                final double value = Math.scalb(1D, exponent);
                final long expectedIndex = scale >= 0 ? indexesPerPowerOf2 * exponent : exponent >> (-scale);

                assertLongEquals(expectedIndex, indexer.getBucketIndex(value), powerOf2IndexDelta);

                if (scale > 0) {
                    if (value > Double.MIN_NORMAL) {
                        // Test one bucket down
                        assertLongEquals(expectedIndex - 1, indexer.getBucketIndex(Math.nextDown(value)), powerOf2IndexDelta);
                        assertLongEquals(expectedIndex - 1, indexer.getBucketIndex(indexer.getBucketStart(expectedIndex - 1)), roundTripIndexDelta);
                    }

                    // Test middle of bucket
                    assertLongEquals(expectedIndex + indexesPerPowerOf2 / 2, indexer.getBucketIndex(value * squareRootOf2), powerOf2IndexDelta);

                    // Sample 10 indexes in a cycle
                    for (long index = expectedIndex;
                         index < expectedIndex + indexesPerPowerOf2;
                         index += Math.max(1, indexesPerPowerOf2 / 10)) {
                        assertLongEquals(index, indexer.getBucketIndex(indexer.getBucketStart(index)), roundTripIndexDelta);
                    }
                }
            }
        }
    }

    private void testScalesSubnormal(final Function<Integer, ScaledExpIndexer> indexerMaker,
                                     final int fromScale,
                                     final int toScale,
                                     final long roundTripIndexDelta,
                                     final long powerOf2IndexDelta
    ) {
        final double squareRootOf2 = Math.pow(2, .5);

        for (int scale = fromScale; scale <= toScale; ++scale) {
            final ScaledExpIndexer indexer = indexerMaker.apply(scale);
            final long indexesPerPowerOf2 = scale >= 0 ? (1L << scale) : 0;

            for (int significantBits = 1; significantBits <= 52; significantBits++) {
                // Test start of power of 2. Value is Double.MIN_VALUE when significantBits is 1
                final double start = Double.longBitsToDouble(1L << (significantBits - 1));
                assertTrue(start < Double.MIN_NORMAL);

                final int exponent = DoubleFormat.MIN_SUBNORMAL_EXPONENT + significantBits - 1;
                final long startIndex = scale >= 0 ? indexesPerPowerOf2 * exponent : exponent >> (-scale);
                assertLongEquals(startIndex, indexer.getBucketIndex(start), powerOf2IndexDelta);

                if (scale > 0 && significantBits > scale + 1) {
                    // Test middle of power of 2
                    final long midIndex = startIndex + indexesPerPowerOf2 / 2;
                    assertLongEquals(midIndex, indexer.getBucketIndex(start * squareRootOf2), roundTripIndexDelta);
                    assertLongEquals(midIndex, indexer.getBucketIndex(indexer.getBucketStart(midIndex)), roundTripIndexDelta);

                    // Test end of power of 2. All significant bits are 1's.
                    final double end = Double.longBitsToDouble((1L << significantBits) - 1);
                    assertTrue(end < Double.MIN_NORMAL);

                    final long endIndex = startIndex + indexesPerPowerOf2 - 1;
                    assertLongEquals(endIndex, indexer.getBucketIndex(end), roundTripIndexDelta);
                    assertLongEquals(endIndex, indexer.getBucketIndex(indexer.getBucketStart(endIndex)), roundTripIndexDelta);
                }
            }
        }
    }
}
