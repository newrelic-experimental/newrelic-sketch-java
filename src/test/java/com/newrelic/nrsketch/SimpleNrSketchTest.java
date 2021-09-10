// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.NrSketch.Bucket;
import com.newrelic.nrsketch.indexer.ExponentIndexer;
import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.LogIndexer;
import com.newrelic.nrsketch.indexer.ScaledExpIndexer;
import com.newrelic.nrsketch.indexer.SubBucketLogIndexer;
import com.newrelic.nrsketch.indexer.SubBucketLookupIndexer;
import org.junit.Test;

import java.util.Iterator;

import static com.newrelic.nrsketch.ComboNrSketch.maxWithNan;
import static com.newrelic.nrsketch.indexer.BucketIndexerTest.assertDoubleEquals;
import static com.newrelic.nrsketch.indexer.BucketIndexerTest.assertLongEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleNrSketchTest {
    public static final double DELTA = 1e-14; // Floating point comparison relative delta.
    public static final double ERROR_DELTA = 1.001; // for relative error comparison.
    public static final double SCALE4_ERROR = 0.02165746232622625;

    @Test
    public void testIndexOptions() {
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

    // Verify relative error for max/min contrast of 1M, with default number of buckets.
    @Test
    public void testInsert1M() {
        final int nBuckets = 320;
        final SimpleNrSketch histogram = new SimpleNrSketch(); // Using default number of buckets

        final double min = 1.0 / (1 << 16);
        final double interval = min;
        final int n = 1024 * 1024 - 1;

        double d = min;
        for (int i = 0; i < n; i++) {
            histogram.insert(d);
            d += interval;
        }

        assertEquals(4, histogram.getScale());
        verifyRelativeError(histogram, 4, SCALE4_ERROR);

        assertEquals(min, histogram.getMin(), 0);
        assertEquals(min + interval * (n - 1), histogram.getMax(), 0);

        assertEquals(nBuckets, histogram.getBucketWindowSize());
        assertEquals(nBuckets, histogram.getMaxNumOfBuckets());

        verifySerialization(histogram, 4, 2, 723);
    }

    // Test that ">>" can produce a new index during downscaling.
    @Test
    public void testDownscaling() {
        final double delta = 1e-12;
        for (int scale = 12; scale >= -3; scale--) {
            for (int scaleDelta = 1; scaleDelta <= 5; scaleDelta++) {
                final ScaledExpIndexer indexer1 = IndexerOption.AUTO_SELECT.getIndexer(scale);
                final ScaledExpIndexer indexer2 = IndexerOption.AUTO_SELECT.getIndexer(scale - scaleDelta);

                final double base1 = indexer1.getBase();
                final double base2 = indexer2.getBase();

                assertDoubleEquals(base2, Math.pow(base1, 1 << scaleDelta), DELTA);

                for (long idx1 = indexer1.getMinIndexNormal(); idx1 <= indexer1.getMaxIndex(); idx1 += 3) {
                    final long idx2 = idx1 >> scaleDelta;

                    final double bucket1Start = indexer1.getBucketStart(idx1);
                    final double bucket1End = indexer1.getBucketEnd(idx1);

                    final double bucket2Start = indexer2.getBucketStart(idx2);
                    final double bucket2End = indexer2.getBucketEnd(idx2);

                    if (bucket1Start > Double.MIN_NORMAL) {
                        assertDoubleEquals(base1, bucket1End / bucket1Start, delta);
                    }
                    if (bucket2Start > Double.MIN_NORMAL) {
                        assertDoubleEquals(base2, bucket2End/ bucket2Start, delta);
                    }

                    assertDoubleGreater(bucket1Start, bucket2Start, delta);
                    assertDoubleGreater(bucket2End, bucket1Start, delta);

                    assertDoubleGreater(bucket1End, bucket2Start, delta);
                    assertDoubleGreater(bucket2End, bucket1End, delta);
                }
            }
        }
    }

    // Asserts a >= b. Delta is relative
    public static void assertDoubleGreater(final double a, final double b, final double delta) {
        if (a + Math.abs(a * delta) < b) {
            fail("a=" + a + " b=" + b + " delta=" + delta);
        }
    }

    private static void verifyRelativeError(final SimpleNrSketch histogram, final int expectedscale, final double expectedRelativeError) {
        final double error = dumpBuckets(histogram);
        if (!Double.isNaN(error)) {
            assertTrue(expectedRelativeError * ERROR_DELTA >= error);
        }
        assertEquals(expectedscale, histogram.getScale());
        assertDoubleEquals(expectedRelativeError, histogram.getPercentileRelativeError(), ERROR_DELTA);
    }

    private static void verifySerialization(final SimpleNrSketch histogram, final int expectedBytesPerBucketInMemory, final int expectedBytesPerBucketSerialized, final int expectedBufferSize) {
        // TODO: test protobuf
    }

    static final double INITIAL_ERROR = 8.4612692737477E-5;

    @Test
    public void testRelativeErrorAndSerialization() {
        final SimpleNrSketch histogram = new SimpleNrSketch(10);

        verifyRelativeError(histogram, 12, INITIAL_ERROR);
        verifySerialization(histogram, 1, 0, 82);
        assertEquals(0, histogram.getBucketWindowSize());

        assertEquals("totalCount=0, sum=0.0, min=NaN, max=NaN, bucketHoldsPositiveNumbers=true, scale=12, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808}", histogram.toString());

        histogram.insert(1);
        verifyRelativeError(histogram, 12, INITIAL_ERROR);
        verifySerialization(histogram, 1, 1, 84);
        assertEquals(1, histogram.getBucketWindowSize());

        histogram.insert(2);
        verifyRelativeError(histogram, 3, 0.0625);
        verifySerialization(histogram, 1, 1, 88);
        assertEquals(9, histogram.getBucketWindowSize());

        histogram.insert(128);
        verifyRelativeError(histogram, 0, .5);
        verifySerialization(histogram, 1, 1, 87); // Buf size is smaller because there is more shift and smaller window size.
        assertEquals(8, histogram.getBucketWindowSize());

        assertEquals("totalCount=3, sum=131.0, min=1.0, max=128.0, bucketHoldsPositiveNumbers=true, scale=0, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=0, indexStart=0, indexEnd=7, array={1,1,0,0,0,0,0,1,}}", histogram.toString());
    }

    @Test
    public void testPercentile() {
        final int nBuckets = 320;
        final SimpleNrSketch histogram = new SimpleNrSketch(nBuckets);

        final double min = 5;
        final double interval = 1;
        final double max = 1000_000;

        for (double d = min; d <= max; d += interval) {
            histogram.insert(d);
        }

        assertDoubleEquals(SCALE4_ERROR, histogram.getPercentileRelativeError(), ERROR_DELTA);

        assertEquals(min, histogram.getMin(), 0);
        assertEquals(max, histogram.getMax(), 0);

        assertEquals(282, histogram.getBucketWindowSize());
        assertEquals(nBuckets, histogram.getMaxNumOfBuckets());

        verifyPercentile(histogram, new double[]{0}, new double[]{min});
        verifyPercentile(histogram, new double[]{100}, new double[]{max});

        verifyPercentile(histogram, new double[]{0, 100}, new double[]{min, max});
        verifyPercentile(histogram, new double[]{-1, 0, 0, 100, 100, 110}, new double[]{min, min, min, max, max, max});

        verifyPercentile(histogram, new double[]{0.1}, new double[]{1002.2928797176692});
        verifyPercentile(histogram, new double[]{1}, new double[]{9957.641941304133});
        verifyPercentile(histogram, new double[]{50}, new double[]{491417.0623172916});
        verifyPercentile(histogram, new double[]{99}, new double[]{980774.2158036901});
        verifyPercentile(histogram, new double[]{99.9}, new double[]{980774.2158036901});

        // Test out of order thresholds.
        verifyPercentile(histogram, new double[]{99, 50}, new double[]{491417.0623172916, 980774.2158036901});

        verifyPercentile(histogram, new double[]{50, 90, 95, 99}, new double[]{491417.0623172916, 901262.8660894368, 941165.1821325879, 980774.2158036901});
    }

    @Test
    public void testPercentileOnEmptyHistogram() {
        final int nBuckets = 320;
        final SimpleNrSketch histogram = new SimpleNrSketch(nBuckets);

        final double min = Double.NaN;
        final double max = Double.NaN;

        verifyPercentile(histogram, new double[]{0}, new double[]{min});
        verifyPercentile(histogram, new double[]{100}, new double[]{max});

        verifyPercentile(histogram, new double[]{0, 100}, new double[]{min, max});
        verifyPercentile(histogram, new double[]{-1, 0, 0, 100, 100, 110}, new double[]{min, min, min, max, max, max});

        verifyPercentile(histogram, new double[]{50, 90, 95, 99}, new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN});
    }

    static void verifyPercentile(final NrSketch histogram, final double[] thresholds, final double[] expectedPercentiles) {
        final double[] actualPercentiles = histogram.getPercentiles(thresholds);
        assertEquals(expectedPercentiles.length, actualPercentiles.length);

        for (int i = 0; i < expectedPercentiles.length; i++) {
            if (Double.isNaN(expectedPercentiles[i])) {
                assertTrue(Double.isNaN(actualPercentiles[i]));
            } else {
                assertDoubleEquals(expectedPercentiles[i], actualPercentiles[i], DELTA);
            }
        }
    }

    @Test
    public void happyPath() {
        // scale=-1 base=4
        testHistogram(5, 0, 100, 100, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 4.0, 3), // bucket 2
                new Bucket(4.0, 16.0, 12), // bucket 3
                new Bucket(16.0, 64.0, 48), // bucket 4
                new Bucket(64.0, 99.0, 36), // bucket 5
        });

        // scale=0 base=2
        testHistogram(5, 1, 10, 9, new Bucket[]{
                new Bucket(1.0, 2.0, 1), // bucket 1
                new Bucket(2.0, 4.0, 2), // bucket 2
                new Bucket(4.0, 8.0, 4), // bucket 3
                new Bucket(8.0, 9.0, 2), // bucket 4
        });

        // scale=2 base=1.19
        testHistogram(5, 1000, 2000, 100, new Bucket[]{
                new Bucket(1000.0, 1024.0, 3), // bucket 1
                new Bucket(1024.0, 1217.7480857627863, 19), // bucket 2
                new Bucket(1217.7480857627863, 1448.1546878700492, 23), // bucket 3
                new Bucket(1448.1546878700492, 1722.1558584396073, 28), // bucket 4
                new Bucket(1722.1558584396073, 1990.0, 27), // bucket 5
        });

        // scale=11 base=1.000338508  Large absolute value, but small relative range
        testHistogram(5, 1001000, 1002000, 100, new Bucket[]{
                new Bucket(1001000.0, 1001065.8651347584, 7), // bucket 1
                new Bucket(1001065.8651347584, 1001404.7339913719, 34), // bucket 2
                new Bucket(1001404.7339913719, 1001743.7175578222, 34), // bucket 3
                new Bucket(1001743.7175578222, 1001990.0, 25), // bucket 4
        });

        // scale=1 base=1.41
        testHistogram(10, 0, 10, 10, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 1.4142135623730951, 1), // bucket 2
                new Bucket(2.0, 2.8284271247461903, 1), // bucket 3
                new Bucket(2.8284271247461903, 4.0, 1), // bucket 4
                new Bucket(4.0, 5.656854249492381, 2), // bucket 5
                new Bucket(5.656854249492381, 8.0, 2), // bucket 6
                new Bucket(8.0, 9.0, 2), // bucket 7
        });

        // scale=-1 base=4, values < 1
        testHistogram(5, .125, 10.125, 80, new Bucket[]{
                new Bucket(0.125, 0.25, 1), // bucket 1
                new Bucket(0.25, 1.0, 6), // bucket 2
                new Bucket(1.0, 4.0, 24), // bucket 3
                new Bucket(4.0, 10.0, 49), // bucket 4
        });
    }

    @Test
    public void testMergeNoOverlap() {
        final SimpleNrSketch h1 = testHistogram(20, 0, 100, 100, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 1.4142135623730951, 1), // bucket 2
                new Bucket(2.0, 2.8284271247461903, 1), // bucket 3
                new Bucket(2.8284271247461903, 4.0, 1), // bucket 4
                new Bucket(4.0, 5.656854249492381, 2), // bucket 5
                new Bucket(5.656854249492381, 8.0, 2), // bucket 6
                new Bucket(8.0, 11.313708498984761, 4), // bucket 7
                new Bucket(11.313708498984761, 16.0, 4), // bucket 8
                new Bucket(16.0, 22.627416997969522, 7), // bucket 9
                new Bucket(22.627416997969522, 32.0, 9), // bucket 10
                new Bucket(32.0, 45.254833995939045, 14), // bucket 11
                new Bucket(45.254833995939045, 64.0, 18), // bucket 12
                new Bucket(64.0, 90.50966799187809, 27), // bucket 13
                new Bucket(90.50966799187809, 99.0, 9), // bucket 14
        });

        final SimpleNrSketch h2 = testHistogram(10, 1000, 2000, 100, new Bucket[]{
                new Bucket(1000.0, 1024.0, 3), // bucket 1
                new Bucket(1024.0, 1116.6799182492239, 9), // bucket 2
                new Bucket(1116.6799182492239, 1217.7480857627863, 10), // bucket 3
                new Bucket(1217.7480857627863, 1327.963703962634, 11), // bucket 4
                new Bucket(1327.963703962634, 1448.1546878700494, 12), // bucket 5
                new Bucket(1448.1546878700494, 1579.2238852177315, 13), // bucket 6
                new Bucket(1579.2238852177315, 1722.1558584396078, 15), // bucket 7
                new Bucket(1722.1558584396078, 1878.024280483167, 15), // bucket 8
                new Bucket(1878.024280483167, 1990.0, 12), // bucket 9
        });

        h1.insert(99);
        h2.insert(1990);
        h2.insert(1990);

        SimpleNrSketch.merge(h1, h2);

        verifyHistogram(h1, 203, 0, 1990, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 128.0, 37), // bucket 8
                new Bucket(512.0, 1024.0, 3), // bucket 9
                new Bucket(1024.0, 1990.0, 99), // bucket 10
        });
    }

    @Test
    public void testMergeOverlap() {
        final Bucket[] h1Buckets = new Bucket[]{
                new Bucket(100.0, 128.0, 4), // bucket 1
                new Bucket(128.0, 181.01933598375618, 6), // bucket 2
                new Bucket(181.01933598375618, 256.0, 8), // bucket 3
                new Bucket(256.0, 362.03867196751236, 12), // bucket 4
                new Bucket(362.03867196751236, 512.0, 16), // bucket 5
                new Bucket(512.0, 724.0773439350247, 24), // bucket 6
                new Bucket(724.0773439350247, 991.0, 30), // bucket 7
        };

        final SimpleNrSketch h1 = testHistogram(10, 100, 1000, 100, h1Buckets);

        // Merge h1 into empty histogram
        final SimpleNrSketch h1copy = new SimpleNrSketch(10);
        SimpleNrSketch.merge(h1copy, h1);

        verifyHistogram(h1copy, 100, 100, 991, h1Buckets);

        final Bucket[] h2Buckets = new Bucket[]{
                new Bucket(500.0, 512.0, 1), // bucket 1
                new Bucket(512.0, 608.8740428813932, 7), // bucket 2
                new Bucket(608.8740428813932, 724.0773439350246, 7), // bucket 3
                new Bucket(724.0773439350246, 861.0779292198037, 10), // bucket 4
                new Bucket(861.0779292198037, 1024.0, 10), // bucket 5
                new Bucket(1024.0, 1217.7480857627863, 13), // bucket 6
                new Bucket(1217.7480857627863, 1448.1546878700492, 16), // bucket 7
                new Bucket(1448.1546878700492, 1722.1558584396073, 18), // bucket 8
                new Bucket(1722.1558584396073, 1985.0, 18), // bucket 9
        };

        final SimpleNrSketch h2 = testHistogram(10, 500, 2000, 100, h2Buckets);

        // Merge empty histogram into h2
        final SimpleNrSketch dummy = new SimpleNrSketch(10);
        SimpleNrSketch.merge(h2, dummy);

        verifyHistogram(h2, 100, 500, 1985, h2Buckets);

        h2.insert(1985);
        h2.insert(1985);

        SimpleNrSketch.merge(h1, h2);
        dumpBuckets(h1);

        final Bucket[] mergedBuckets = new Bucket[]{
                new Bucket(100.0, 128.0, 4), // bucket 1
                new Bucket(128.0, 181.01933598375618, 6), // bucket 2
                new Bucket(181.01933598375618, 256.0, 8), // bucket 3
                new Bucket(256.0, 362.03867196751236, 12), // bucket 4
                new Bucket(362.03867196751236, 512.0, 17), // bucket 5
                new Bucket(512.0, 724.0773439350247, 38), // bucket 6
                new Bucket(724.0773439350247, 1024.0, 50), // bucket 7
                new Bucket(1024.0, 1448.1546878700494, 29), // bucket 8
                new Bucket(1448.1546878700494, 1985.0, 38), // bucket 9
        };

        verifyHistogram(h1, 202, 100, 1985, mergedBuckets);

        // Now test reverse order of h2 + h1, using h1copy. Result should still match mergedBuckets.
        SimpleNrSketch.merge(h2, h1copy);
        verifyHistogram(h2, 202, 100, 1985, mergedBuckets);
    }

    static final Bucket[] EMPTY_BUCKET_LIST = new Bucket[]{};

    @Test
    public void mergeEmptyHistograms() {
        // merge(empty, empty)
        final SimpleNrSketch empty1 = testHistogram(10, 0, 0, 0, EMPTY_BUCKET_LIST);
        final SimpleNrSketch empty2 = testHistogram(10, 0, 0, 0, EMPTY_BUCKET_LIST);

        final SimpleNrSketch mergedHistogram = SimpleNrSketch.merge(empty1, empty2);
        assertEquals(empty1, mergedHistogram);

        verifyHistogram(empty1, 0, 0, 0, EMPTY_BUCKET_LIST);

        final Bucket[] buckets = new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        };

        final SimpleNrSketch h3 = testHistogram(10, 0, 100, 100, buckets);

        // merge(nonEmpty, empty)
        verifyHistogram(SimpleNrSketch.merge(h3, empty1), 100, 0, 99, buckets);

        // merge(empty, nonEmpty)
        verifyHistogram(SimpleNrSketch.merge(empty2, h3), 100, 0, 99, buckets);
    }

    @Test
    public void mergeZeros() {
        final SimpleNrSketch h1 = testHistogram(10, 0, 0, 10, new Bucket[]{new Bucket(0.000000, 0, 10)});
        final SimpleNrSketch h2 = testHistogram(10, 0, 0, 5, new Bucket[]{new Bucket(0.000000, 0, 5)});

        final SimpleNrSketch mergedHistogram = SimpleNrSketch.merge(h1, h2);
        assertEquals(h1, mergedHistogram);
        dumpBuckets(h1);

        verifyHistogram(h1, 15, 0, 0, new Bucket[]{new Bucket(0.000000, 0, 15)});

        final SimpleNrSketch h3 = testHistogram(10, 0, 100, 100, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        SimpleNrSketch.merge(h3, h1);
        verifyHistogram(h3, 115, 0, 99, new Bucket[]{
                new Bucket(0.0, 0.0, 16), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        SimpleNrSketch.merge(h2, h3);
        verifyHistogram(h2, 120, 0, 99, new Bucket[]{
                new Bucket(0.0, 0.0, 21), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });
    }

    @Test
    public void mergeZeroAndEmptyHistogram() {
        final SimpleNrSketch h1 = testHistogram(10, 0, 0, 10, new Bucket[]{new Bucket(0.000000, 0, 10)});
        final SimpleNrSketch h2 = testHistogram(10, 0, 0, 0, EMPTY_BUCKET_LIST);

        SimpleNrSketch.merge(h1, h2);
        verifyHistogram(h1, 10, 0, 0, new Bucket[]{new Bucket(0.000000, 0, 10)});

        final SimpleNrSketch h3 = testHistogram(10, 0, 0, 0, EMPTY_BUCKET_LIST);

        SimpleNrSketch.merge(h3, h1);
        verifyHistogram(h3, 10, 0, 0, new Bucket[]{new Bucket(0.000000, 0, 10)});
    }

    @Test
    public void testMinMaxAndNegative() {
        final SimpleNrSketch h1 = testHistogram(5, 100, 200, 100, new Bucket[]{
                new Bucket(100.0, 107.63474115247546, 8), // bucket 1
                new Bucket(107.63474115247546, 128.0, 20), // bucket 2
                new Bucket(128.0, 152.2185107203483, 25), // bucket 3
                new Bucket(152.2185107203483, 181.01933598375615, 29), // bucket 4
                new Bucket(181.01933598375615, 199.0, 18), // bucket 5
        });

        h1.insert(95);
        h1.insert(95);
        h1.insert(300);
        h1.insert(300);
        h1.insert(300);

        verifyHistogram(h1, 105, 95, 300, new Bucket[]{
                new Bucket(95.0, 128.0, 30), // bucket 1
                new Bucket(128.0, 181.01933598375618, 54), // bucket 2
                new Bucket(181.01933598375618, 256.0, 18), // bucket 3
                new Bucket(256.0, 300.0, 3), // bucket 4
        });

        h1.insert(0);
        h1.insert(0);

        verifyHistogram(h1, 107, 0, 300, new Bucket[]{
                new Bucket(0.0, 0.0, 2), // bucket 1
                new Bucket(90.50966799187809, 128.0, 30), // bucket 2
                new Bucket(128.0, 181.01933598375618, 54), // bucket 3
                new Bucket(181.01933598375618, 256.0, 18), // bucket 4
                new Bucket(256.0, 300.0, 3), // bucket 5
        });

        h1.insert(-1);

        verifyHistogram(h1, 108, -1, 300, new Bucket[]{
                new Bucket(-1.0, 0.0, 1), // bucket 1
                new Bucket(0.0, 0.0, 2), // bucket 2
                new Bucket(90.50966799187809, 128.0, 30), // bucket 3
                new Bucket(128.0, 181.01933598375618, 54), // bucket 4
                new Bucket(181.01933598375618, 256.0, 18), // bucket 5
                new Bucket(256.0, 300.0, 3), // bucket 6
        });

        h1.insert(-1);
        h1.insert(-200.75);

        verifyHistogram(h1, 110, -200.75, 300, new Bucket[]{
                new Bucket(-200.75, 0.0, 3), // bucket 1
                new Bucket(0.0, 0.0, 2), // bucket 2
                new Bucket(90.50966799187809, 128.0, 30), // bucket 3
                new Bucket(128.0, 181.01933598375618, 54), // bucket 4
                new Bucket(181.01933598375618, 256.0, 18), // bucket 5
                new Bucket(256.0, 300.0, 3), // bucket 6
        });

        final SimpleNrSketch h2 = new SimpleNrSketch(10);
        h2.insert(-10);

        verifyHistogram(h2, 1, -10, -10, new Bucket[]{
                new Bucket(-10.000000, -10, 1), // bucket 1
        });

        SimpleNrSketch.merge(h1, h2);
        verifyHistogram(h1, 111, -200.75, 300, new Bucket[]{
                new Bucket(-200.75, 0.0, 4), // bucket 1
                new Bucket(0.0, 0.0, 2), // bucket 2
                new Bucket(90.50966799187809, 128.0, 30), // bucket 3
                new Bucket(128.0, 181.01933598375618, 54), // bucket 4
                new Bucket(181.01933598375618, 256.0, 18), // bucket 5
                new Bucket(256.0, 300.0, 3), // bucket 6
        });

        final SimpleNrSketch h3 = testHistogram(5, 150, 250, 100, new Bucket[]{
                new Bucket(150.0, 152.2185107203483, 3), // bucket 1
                new Bucket(152.2185107203483, 181.01933598375615, 29), // bucket 2
                new Bucket(181.01933598375615, 215.2694823049509, 34), // bucket 3
                new Bucket(215.2694823049509, 249.0, 34), // bucket 4
        });

        SimpleNrSketch.merge(h3, h1);
        verifyHistogram(h3, 211, -200.75, 300, new Bucket[]{
                new Bucket(-200.75, 0.0, 4), // bucket 1
                new Bucket(0.0, 0.0, 2), // bucket 2
                new Bucket(90.50966799187809, 128.0, 30), // bucket 3
                new Bucket(128.0, 181.01933598375618, 86), // bucket 4
                new Bucket(181.01933598375618, 256.0, 86), // bucket 5
                new Bucket(256.0, 300.0, 3), // bucket 6
        });
    }

    @Test
    public void testSingleEntryHistogram() {
        final SimpleNrSketch h1 = new SimpleNrSketch(10);
        h1.insert(-10);
        verifyHistogram(h1, 1, -10, -10, new Bucket[]{
                new Bucket(-10.000000, -10, 1), // bucket 1
        });

        final SimpleNrSketch h2 = new SimpleNrSketch(10);
        h2.insert(0);
        verifyHistogram(h2, 1, 0, 0, new Bucket[]{
                new Bucket(0.000000, 0, 1),
        });

        final SimpleNrSketch h3 = new SimpleNrSketch(10);
        h3.insert(100);
        verifyHistogram(h3, 1, 100, 100, new Bucket[]{
                new Bucket(100, 100, 1),
        });

        final SimpleNrSketch h4 = new SimpleNrSketch(10);
        h4.insert(128);
        verifyHistogram(h4, 1, 128, 128, new Bucket[]{
                new Bucket(128, 128, 1),
        });

        SimpleNrSketch.merge(h1, h2);
        verifyHistogram(h1, 2, -10, 0, new Bucket[]{
                new Bucket(-10.000000, 0.000000, 1), // bucket 1
                new Bucket(0.000000, 0.000000, 1), // bucket 2
        });

        SimpleNrSketch.merge(h1, h3);
        verifyHistogram(h1, 3, -10, 100, new Bucket[]{
                new Bucket(-10.0, 0.0, 1), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(99.9960240724642, 100.0, 1), // bucket 3
        });

        SimpleNrSketch.merge(h1, h4);
        verifyHistogram(h1, 4, -10, 128, new Bucket[]{
                new Bucket(-10.0, 0.0, 1), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(98.70149282610814, 103.07138124475219, 1), // bucket 3
                new Bucket(128.0, 128.0, 1), // bucket 4
        });
    }

    @Test
    public void testNegatives() {
        final SimpleNrSketch h1 = new SimpleNrSketch(10);
        h1.insert(-10);
        verifyHistogram(h1, 1, -10, -10, new Bucket[]{
                new Bucket(-10.000000, -10.000000, 1), // bucket 1
        });

        h1.insert(-15);
        h1.insert(-20);
        verifyHistogram(h1, 3, -20, -10, new Bucket[]{
                new Bucket(-20.000000, -10.000000, 3), // bucket 1
        });

        h1.insert(50);
        verifyHistogram(h1, 4, -20, 50, new Bucket[]{
                new Bucket(-20.0, 0.0, 3), // bucket 1
                new Bucket(49.9980120362321, 50.0, 1), // bucket 2
        });

        h1.insert(0);
        verifyHistogram(h1, 5, -20, 50, new Bucket[]{
                new Bucket(-20.0, 0.0, 3), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(49.9980120362321, 50.0, 1), // bucket 3
        });
    }

    // Test that counters on max are properly merged.
    @Test
    public void testMergeEqualMaxAndSum() {
        final SimpleNrSketch h1 = new SimpleNrSketch(5);
        h1.insert(200);
        h1.insert(200);
        verifyHistogram(h1, 2, 200, 200, new Bucket[]{
                new Bucket(200, 200, 2), // bucket 1
        });
        assertEquals(400, h1.getSum(), 0);

        final SimpleNrSketch h2 = new SimpleNrSketch(5);
        h2.insert(100);
        h2.insert(200);
        h2.insert(200);
        verifyHistogram(h2, 3, 100, 200, new Bucket[]{
                new Bucket(100.0, 107.63474115247546, 1), // bucket 1
                new Bucket(181.01933598375615, 200.0, 2), // bucket 2
        });
        assertEquals(500, h2.getSum(), 0);

        verifyHistogram(SimpleNrSketch.merge(h1, h2), 5, 100, 200, new Bucket[]{
                new Bucket(100.0, 107.63474115247546, 1), // bucket 1
                new Bucket(181.01933598375615, 200.0, 4), // bucket 2
        });
        assertEquals(900, h1.getSum(), 0);
    }

    @Test
    public void negativeHistogramHappyPath() {
        testNegativeHistogram(10, -100, 0, 100, new NrSketch.Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 32), // bucket 2
                new Bucket(-32.000000, -16.000000, 16), // bucket 3
                new Bucket(-16.000000, -8.000000, 8), // bucket 4
                new Bucket(-8.000000, -4.000000, 4), // bucket 5
                new Bucket(-4.000000, -2.000000, 2), // bucket 6
                new Bucket(-2.000000, -1.000000, 1), // bucket 7
        });

        testNegativeHistogram(20, -128, 0, 1024, new NrSketch.Bucket[]{
                new Bucket(-128.000000, -128.000000, 1), // bucket 1
                new Bucket(-128.000000, -64.000000, 512), // bucket 2
                new Bucket(-64.000000, -32.000000, 256), // bucket 3
                new Bucket(-32.000000, -16.000000, 128), // bucket 4
                new Bucket(-16.000000, -8.000000, 64), // bucket 5
                new Bucket(-8.000000, -4.000000, 32), // bucket 6
                new Bucket(-4.000000, -2.000000, 16), // bucket 7
                new Bucket(-2.000000, -1.000000, 8), // bucket 8
                new Bucket(-1.000000, -0.500000, 4), // bucket 9
                new Bucket(-0.500000, -0.250000, 2), // bucket 10
                new Bucket(-0.250000, -0.125000, 1), // bucket 11
        });
    }

    @Test
    public void negativeHistogramSmallDataSet() {
        final SimpleNrSketch histogram = SimpleNrSketch.newNegativeHistogram(10);
        verifyHistogram(histogram, 0, Double.NaN, Double.NaN, EMPTY_BUCKET_LIST);
        verifySerialization(histogram, 1, 0, 82);
        assertEquals(0, histogram.getBucketWindowSize());

        assertEquals("totalCount=0, sum=0.0, min=NaN, max=NaN, bucketHoldsPositiveNumbers=false, scale=12, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808}", histogram.toString());

        histogram.insert(-10);
        verifyHistogram(histogram, 1, -10, -10, new Bucket[]{
                new Bucket(-10.000000, -10, 1), // bucket 1
        });
        verifySerialization(histogram, 1, 1, 84);
        assertEquals(1, histogram.getBucketWindowSize());

        histogram.insert(-100);
        verifyHistogram(histogram, 2, -100, -10, new Bucket[]{
                new Bucket(-100.0, -90.50966799187809, 1), // bucket 1
                new Bucket(-11.313708498984761, -10.0, 1), // bucket 2
        });
        verifySerialization(histogram, 1, 1, 91);
        assertEquals(8, histogram.getBucketWindowSize());

        assertEquals("totalCount=2, sum=-110.0, min=-100.0, max=-10.0, bucketHoldsPositiveNumbers=false, scale=1, countForNegatives=2, countForZero=0, buckets={maxSize=10, indexBase=6, indexStart=6, indexEnd=13, array={1,0,0,0,0,0,0,1,}}", histogram.toString());
    }

    private static SimpleNrSketch testNegativeHistogram(final int numBuckets, final double from, final double to, final int numDataPoints, final Bucket[] expectedBuckets) {
        final SimpleNrSketch histogram = SimpleNrSketch.newNegativeHistogram(numBuckets);
        final double max = insertData(histogram, from, to, numDataPoints);
        verifyHistogram(histogram, numDataPoints, from, max, expectedBuckets);
        return histogram;
    }

    private static SimpleNrSketch testHistogram(final int numBuckets, final double from, final double to, final int numDataPoints, final Bucket[] expectedBuckets) {
        final SimpleNrSketch histogram = new SimpleNrSketch(numBuckets);
        final double max = insertData(histogram, from, to, numDataPoints);
        verifyHistogram(histogram, numDataPoints, from, max, expectedBuckets);
        return histogram;
    }

    // Dump in a format ready for new test. Returns max relative error.
    static double dumpBuckets(final NrSketch histogram) {
        final Iterator<Bucket> iterator = histogram.iterator();
        int i = 1;
        double maxRelativeError = Double.NaN;
        final boolean checkErrorOnNegatives = !(histogram instanceof SimpleNrSketch && ((SimpleNrSketch) histogram).isBucketHoldsPositiveNumbers());

        while (iterator.hasNext()) {
            final Bucket bucket = iterator.next();
            final double bucketWidth = bucket.endValue - bucket.startValue;
            if (bucketWidth < 0) {
                throw new RuntimeException("Negative bucketWidth");
            }
            final double relativeError;
            final double bucketMiddle = (bucket.endValue + bucket.startValue) / 2;
            if (bucket.startValue > 0) {
                relativeError = bucketWidth / 2 / bucketMiddle;
            } else if (bucket.endValue < 0 && checkErrorOnNegatives) {
                relativeError = bucketWidth / 2 / -bucketMiddle;
            } else {
                relativeError = Double.NaN;
            }
            maxRelativeError = maxWithNan(maxRelativeError, relativeError);

            if (bucket.count == 0) {
                throw new RuntimeException("Zero count");
            }
            System.out.printf("new Bucket(" + bucket.startValue + ", " + bucket.endValue + ", " + bucket.count + "), // bucket " + i++ + "\n");
        }

        final double reportedRelativeError = histogram.getPercentileRelativeError();

        if (histogram instanceof SimpleNrSketch) {
            System.out.println("scale=" + ((SimpleNrSketch) histogram).getScale() + " ");
        }
        System.out.println("min=" + histogram.getMin() + " max=" + histogram.getMax()
                + " reportedError=" + reportedRelativeError + " actualError=" + maxRelativeError);

        if (maxRelativeError > reportedRelativeError * ERROR_DELTA) {
            throw new RuntimeException("maxRelativeError " + maxRelativeError + " > reportedRelativeError " + reportedRelativeError);
        }

        return maxRelativeError;
    }

    public static void assertBucketEquals(final Bucket a, final Bucket b, final double delta) {
        assertDoubleEquals(a.startValue , b.startValue, delta);
        assertDoubleEquals(a.endValue , b.endValue, delta);
        assertLongEquals(a.count , b.count, 0);
    }

    static void verifyHistogram(final NrSketch histogram, final long expectedCount, final double expectedMin, final double expectedMax, final Bucket[] expectedBuckets) {
        dumpBuckets(histogram);

        final Iterator<Bucket> iterator = histogram.iterator();
        int index = 0;
        int countSum = 0;

        while (iterator.hasNext()) {
            final Bucket bucket = iterator.next();
            assertBucketEquals(expectedBuckets[index], bucket, DELTA);
            index++;
            countSum += bucket.count;
        }

        assertEquals(expectedBuckets.length, index);

        assertEquals(expectedCount, histogram.getCount());
        assertEquals(expectedCount, countSum);

        if (expectedCount > 0) {
            assertTrue(expectedMin == histogram.getMin());
            assertTrue(expectedMax == histogram.getMax());
        } else {
            assertTrue(Double.isNaN(histogram.getMin()));
            assertTrue(Double.isNaN(histogram.getMax()));
        }
    }

    // Returns max value. Min is always "from".
    static double insertData(final NrSketch histogram, final double from, final double to, final int numDataPoints) {
        final double step = (to - from) / numDataPoints;
        double value = Double.NaN;
        for (int i = 0; i < numDataPoints; i++) {
            value = from + step * i;
            histogram.insert(value);
        }
        return value;
    }
}
