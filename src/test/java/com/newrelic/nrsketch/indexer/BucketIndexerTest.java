// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import com.newrelic.nrsketch.DoubleFormat;
import org.junit.Test;

import java.util.function.Function;

import static com.newrelic.nrsketch.indexer.SubBucketLookupIndexer.binarySearch;
import static com.newrelic.nrsketch.indexer.SubBucketLookupIndexer.getLogBoundMantissas;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BucketIndexerTest {
    public static final double DELTA = 1e-14; // Floating point comparison relative delta.
    public static final double FUDGE = 1 + 1e-13; // When value is on or near a bound, apply the fudge factor to make it fall on the expected side a bound for all indexers.

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
    public void testGetMaxIndex() {
        // Exactly at max exponent
        assertTwice(1023, 0x3FF, ScaledExpIndexer.getMaxIndex(0));
        assertTwice(16383, 0x3FFF, ScaledExpIndexer.getMaxIndex(4));
        assertTwice(63, 0x3F, ScaledExpIndexer.getMaxIndex(-4));
    }

    @Test
    public void testGetMinIndex() {
        //System.out.println(String.format("%X", ScaledExpIndexer.getMinIndexNormal(4)));
        // Exactly at min exponent
        assertTwice(-1022, 0xFFFFFFFFFFFFFC02L, ScaledExpIndexer.getMinIndexNormal(0));
        assertTwice(-16352, 0xFFFFFFFFFFFFC020L, ScaledExpIndexer.getMinIndexNormal(4));
        assertTwice(-64, 0xFFFFFFFFFFFFFFC0L, ScaledExpIndexer.getMinIndexNormal(-4));
    }

    @Test
    public void testLogBounds() {
        final int scale = 4;
        final int nSubBuckets = 1 << scale;
        final long[] logBounds = getLogBoundMantissas(scale);
        double base = 0;
        for (int i = 0; i < logBounds.length; i++) {
            final double d = DoubleFormat.makeDouble1To2(logBounds[i]);
            switch (i) {
                case 0:
                    assertEquals(1.0, d, 0);
                    break;
                case 1:
                    base = d;
                    break;
                case nSubBuckets - 1:
                    assertDoubleEquals(2.0, d * base, DELTA);
                    break;
            }
            if (i > 0) {
                assertDoubleEquals(d, DoubleFormat.makeDouble1To2(logBounds[i - 1]) * base, DELTA);
            }
        }
        assertDoubleEquals(2.0, Math.pow(base, nSubBuckets), DELTA);
    }

    @Test
    public void testBinarySearch() {
        final long[] lookup = {0, 10, 20, 30, 40, 50, 60, 70};
        assertEquals(8, lookup.length);

        for (long l = 0; l < 80; l++) {
            //System.out.println(l);
            assertEquals(l / 10, binarySearch(lookup, l));
        }
        assertEquals(0, binarySearch(lookup, 0));
        assertEquals(1, binarySearch(lookup, 10));
        assertEquals(1, binarySearch(lookup, 15));

        assertEquals(-1, binarySearch(lookup, -100));
        assertEquals(7, binarySearch(lookup, 100));
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
        assertLongEquals(idx.getMinIndexNormal(), idx.getBucketIndex(Double.MIN_NORMAL * FUDGE), 0);
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
    }

    @Test
    public void testSubBucketLogIndexerScales() {
        testScales(SubBucketLogIndexer::new,
                1, // fromScale
                52, // toScale, highest possible scale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
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
    }

    @Test
    public void testExponentIndexerScales() {
        testScales(ExponentIndexer::new,
                -11, // fromScale, lowest possible scale
                0, // toScale
                Double.MIN_EXPONENT, // fromExponent
                Double.MAX_EXPONENT, // toExponent
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
            final long minIndex = ScaledExpIndexer.getMinIndexNormal(scale);

            assertLongEquals(maxIndex, indexer.getBucketIndex(Double.MAX_VALUE), powerOf2IndexDelta); // LogIndexer needs this delta
            assertLongEquals(minIndex, indexer.getBucketIndex(Double.MIN_NORMAL), 0);

            assertLongEquals(maxIndex, indexer.getBucketIndex(indexer.getBucketStart(maxIndex)), roundTripIndexDelta);
            assertLongEquals(minIndex, indexer.getBucketIndex(indexer.getBucketStart(minIndex)), roundTripIndexDelta);

            // Test power of 2
            for (int exponent = fromExponent; exponent <= toExponent; ++exponent) {
                final double value = Math.scalb(1D, exponent);
                final long expectedIndex = scale >= 0 ? indexesPerPowerOf2 * exponent : exponent >> (-scale);

                assertLongEquals(expectedIndex, indexer.getBucketIndex(value), powerOf2IndexDelta);

                if (scale > 0) {
                    // Test one bucket down
                    assertLongEquals(expectedIndex - 1, indexer.getBucketIndex(Math.nextDown(value)), powerOf2IndexDelta);
                    assertLongEquals(expectedIndex - 1, indexer.getBucketIndex(indexer.getBucketStart(expectedIndex - 1)), roundTripIndexDelta);

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
}
