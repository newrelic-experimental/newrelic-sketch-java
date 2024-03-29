// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.NrSketch.Bucket;
import com.newrelic.nrsketch.indexer.DoubleFormat;
import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledIndexer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.newrelic.nrsketch.SimpleNrSketch.DEFAULT_INIT_SCALE;
import static com.newrelic.nrsketch.SimpleNrSketchTest.EMPTY_BUCKET_LIST;
import static com.newrelic.nrsketch.SimpleNrSketchTest.INITIAL_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.SCALE0_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.SCALE1_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.SCALE2_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.SCALE4_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.TEST_INIT_SCALE;
import static com.newrelic.nrsketch.SimpleNrSketchTest.TEST_MAX_BUCKETS;
import static com.newrelic.nrsketch.SimpleNrSketchTest.insertData;
import static com.newrelic.nrsketch.SimpleNrSketchTest.verifyHistogram;
import static com.newrelic.nrsketch.SimpleNrSketchTest.verifyPercentile;
import static com.newrelic.nrsketch.SimpleNrSketchTest.verifySerialization;
import static com.newrelic.nrsketch.indexer.IndexerOption.AUTO_SELECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ComboNrSketchTest {
    @Test(expected = IllegalArgumentException.class)
    public void testInitializedWithZeroMaxBucketCount() {
        new ComboNrSketch(0);
    }

    @Test
    public void testConstructors() {
        ComboNrSketch sketch = new ComboNrSketch();
        assertParams(sketch, SimpleNrSketch.DEFAULT_MAX_BUCKETS, DEFAULT_INIT_SCALE, SimpleNrSketch.DEFAULT_INDEXER_MAKER);

        sketch = new ComboNrSketch(99);
        assertParams(sketch, 99, DEFAULT_INIT_SCALE, SimpleNrSketch.DEFAULT_INDEXER_MAKER);

        sketch = new ComboNrSketch(99, 43);
        assertParams(sketch, 99, 43, SimpleNrSketch.DEFAULT_INDEXER_MAKER);

        for (IndexerOption option : IndexerOption.values()) {
            sketch = new ComboNrSketch(99, 7, option);
            assertParams(sketch, 99, 7, option);

            final NrSketch readback = verifySerialization(sketch, 193);
            assertParams((ComboNrSketch) readback, 99, 7, option);
        }
    }

    private void assertParams(final ComboNrSketch sketch,
                              final int expectedNumBucketsPerHistogram,
                              final int expectedInitScale,
                              final Function<Integer, ScaledIndexer> expectedIndexerMaker) {
        sketch.insert(10);
        sketch.insert(-20);

        final List<NrSketch> sketches = sketch.getHistograms();
        assertEquals(2, sketches.size());

        for (NrSketch subSketch : sketches) {
            final SimpleNrSketch simpleNrSketch = (SimpleNrSketch) subSketch;

            assertEquals(expectedNumBucketsPerHistogram, simpleNrSketch.getMaxNumOfBuckets());
            assertEquals(expectedInitScale, simpleNrSketch.getScale());
            assertEquals(expectedIndexerMaker, simpleNrSketch.getIndexerMaker());
        }
    }

    // Equality is also tested in verifySerialization()
    @Test
    public void testEqualAndHash() {
        final ComboNrSketch s1 = new ComboNrSketch(TEST_MAX_BUCKETS);
        final ComboNrSketch s2 = new ComboNrSketch(TEST_MAX_BUCKETS);
        final ComboNrSketch s3 = new ComboNrSketch(99);

        assertNotEquals(s1, s3);

        assertEquals(s1, s2);
        assertEquals(10901, s1.hashCode());
        assertEquals(10901, s2.hashCode());

        s1.insert(11);
        assertNotEquals(s1, s2);
        assertEquals(-2832216, s1.hashCode());

        s2.insert(11);
        assertEquals(s1, s2);
        assertEquals(-2832216, s2.hashCode());

        s1.insert(-22);
        assertNotEquals(s1, s2);
        assertEquals(2028041486, s1.hashCode());

        s2.insert(-22);
        assertEquals(s1, s2);
        assertEquals(2028041486, s2.hashCode());
    }

    @Test
    public void happyPath() {
        testComboHistogram(10, -100, 100, 200, new NrSketch.Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 32), // bucket 2
                new Bucket(-32.000000, -16.000000, 16), // bucket 3
                new Bucket(-16.000000, -8.000000, 8), // bucket 4
                new Bucket(-8.000000, -4.000000, 4), // bucket 5
                new Bucket(-4.000000, -2.000000, 2), // bucket 6
                new Bucket(-2.000000, -1.000000, 1), // bucket 7
                new Bucket(0.000000, 0.000000, 1), // bucket 8
                new Bucket(1.000000, 2.000000, 1), // bucket 9
                new Bucket(2.000000, 4.000000, 2), // bucket 10
                new Bucket(4.000000, 8.000000, 4), // bucket 11
                new Bucket(8.000000, 16.000000, 8), // bucket 12
                new Bucket(16.000000, 32.000000, 16), // bucket 13
                new Bucket(32.000000, 64.000000, 32), // bucket 14
                new Bucket(64.000000, 99.000000, 36), // bucket 15
        });
    }

    // Also tests serialization and relative error
    @Test
    public void positiveFirst() {
        final ComboNrSketch histogram = new ComboNrSketch(10, TEST_INIT_SCALE);
        verifyHistogram(histogram, 0, Double.NaN, Double.NaN, EMPTY_BUCKET_LIST);
        verifySerialization(histogram, 9);
        assertEquals(0, histogram.getPercentileRelativeError(), 0);
        assertEquals("maxNumBucketsPerHistogram=10, histograms.size()=0", histogram.toString());

        histogram.insert(10);
        verifyHistogram(histogram, 1, 10, 10, new Bucket[]{
                new Bucket(10.000000, 10, 1), // bucket 1
        });
        verifySerialization(histogram, 85);
        assertEquals(INITIAL_ERROR, histogram.getPercentileRelativeError(), 0);
        assertEquals("maxNumBucketsPerHistogram=10, histograms.size()=1\n" +
                "totalCount=1, sum=10.0, min=10.0, max=10.0, bucketHoldsPositiveNumbers=true, scale=12, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=13606, indexStart=13606, indexEnd=13606, array={1,}}", histogram.toString());

        histogram.insert(100);
        verifyHistogram(histogram, 2, 10, 100, new Bucket[]{
                new Bucket(10.0, 11.313708498984761, 1), // bucket 1
                new Bucket(90.50966799187809, 100.0, 1), // bucket 2
        });
        verifySerialization(histogram, 92);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        histogram.insert(-5);
        verifyHistogram(histogram, 3, -5, 100, new Bucket[]{
                new Bucket(-5.0, -5.0, 1), // bucket 1
                new Bucket(10.0, 11.313708498984761, 1), // bucket 2
                new Bucket(90.50966799187809, 100.0, 1), // bucket 3
        });
        verifySerialization(histogram, 200);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);
        assertEquals("maxNumBucketsPerHistogram=10, histograms.size()=2\ntotalCount=1, sum=-5.0, min=-5.0, max=-5.0, bucketHoldsPositiveNumbers=false, scale=12, countForNegatives=1, countForZero=0, buckets={maxSize=10, indexBase=9510, indexStart=9510, indexEnd=9510, array={1,}}\n" +
                "totalCount=2, sum=110.0, min=10.0, max=100.0, bucketHoldsPositiveNumbers=true, scale=1, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=6, indexStart=6, indexEnd=13, array={1,0,0,0,0,0,0,1,}}", histogram.toString());

        histogram.insert(-50);
        verifyHistogram(histogram, 4, -50, 100, new Bucket[]{
                new Bucket(-50.0, -45.254833995939045, 1), // bucket 1
                new Bucket(-5.656854249492381, -5.0, 1), // bucket 2
                new Bucket(10.0, 11.313708498984761, 1), // bucket 3
                new Bucket(90.50966799187809, 100.0, 1), // bucket 4
        });
        verifySerialization(histogram, 207);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        final double max = insertData(histogram, 1, 1000, 100);
        verifyHistogram(histogram, 104, -50, max, new Bucket[]{
                new Bucket(-50.0, -45.254833995939045, 1), // bucket 1
                new Bucket(-5.656854249492381, -5.0, 1), // bucket 2
                new Bucket(1.0, 2.0, 1), // bucket 3
                new Bucket(8.0, 16.0, 2), // bucket 4
                new Bucket(16.0, 32.0, 2), // bucket 5
                new Bucket(32.0, 64.0, 3), // bucket 6
                new Bucket(64.0, 128.0, 7), // bucket 7
                new Bucket(128.0, 256.0, 13), // bucket 8
                new Bucket(256.0, 512.0, 26), // bucket 9
                new Bucket(512.0, 990.01, 48), // bucket 10
        });
        verifySerialization(histogram, 209);
        assertEquals(SCALE0_ERROR, histogram.getPercentileRelativeError(), 0);

        histogram.insert(0, 100);
        verifyHistogram(histogram, 204, -50, max, new Bucket[]{
                new Bucket(-50.0, -45.254833995939045, 1), // bucket 1
                new Bucket(-5.656854249492381, -5.0, 1), // bucket 2
                new Bucket(0.0, 0.0, 100), // bucket 3
                new Bucket(1.0, 2.0, 1), // bucket 4
                new Bucket(8.0, 16.0, 2), // bucket 5
                new Bucket(16.0, 32.0, 2), // bucket 6
                new Bucket(32.0, 64.0, 3), // bucket 7
                new Bucket(64.0, 128.0, 7), // bucket 8
                new Bucket(128.0, 256.0, 13), // bucket 9
                new Bucket(256.0, 512.0, 26), // bucket 10
                new Bucket(512.0, 990.01, 48), // bucket 11
        });
        verifySerialization(histogram, 209);
        assertEquals(SCALE0_ERROR, histogram.getPercentileRelativeError(), 0);
    }

    // Also tests serialization and relative error
    @Test
    public void negativeFirst() {
        final ComboNrSketch histogram = new ComboNrSketch(10, TEST_INIT_SCALE);
        verifyHistogram(histogram, 0, Double.NaN, Double.NaN, EMPTY_BUCKET_LIST);
        verifySerialization(histogram, 9);
        assertEquals(0, histogram.getPercentileRelativeError(), 0);

        histogram.insert(-10);
        verifyHistogram(histogram, 1, -10, -10, new Bucket[]{
                new Bucket(-10.000000, -10, 1), // bucket 1
        });
        verifySerialization(histogram, 85);
        assertEquals(INITIAL_ERROR, histogram.getPercentileRelativeError(), 0);

        histogram.insert(-100);
        verifyHistogram(histogram, 2, -100, -10, new Bucket[]{
                new Bucket(-100.0, -90.50966799187809, 1), // bucket 1
                new Bucket(-11.313708498984761, -10.0, 1), // bucket 2
        });
        verifySerialization(histogram, 92);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        histogram.insert(5);
        verifyHistogram(histogram, 3, -100, 5, new Bucket[]{
                new Bucket(-100.0, -90.50966799187809, 1), // bucket 1
                new Bucket(-11.313708498984761, -10.0, 1), // bucket 2
                new Bucket(5.0, 5.0, 1), // bucket 3
        });
        verifySerialization(histogram, 200);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        histogram.insert(50);
        verifyHistogram(histogram, 4, -100, 50, new Bucket[]{
                new Bucket(-100.0, -90.50966799187809, 1), // bucket 1
                new Bucket(-11.313708498984761, -10.0, 1), // bucket 2
                new Bucket(5.0, 5.656854249492381, 1), // bucket 3
                new Bucket(45.254833995939045, 50.0, 1), // bucket 4
        });
        verifySerialization(histogram, 207);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        insertData(histogram, -128, 0, 256);
        verifyHistogram(histogram, 260, -128, 50, new Bucket[]{
                new Bucket(-128.0, -128.0, 1), // bucket 1
                new Bucket(-128.0, -64.0, 129), // bucket 2
                new Bucket(-64.0, -32.0, 64), // bucket 3
                new Bucket(-32.0, -16.0, 32), // bucket 4
                new Bucket(-16.0, -8.0, 17), // bucket 5
                new Bucket(-8.0, -4.0, 8), // bucket 6
                new Bucket(-4.0, -2.0, 4), // bucket 7
                new Bucket(-2.0, -1.0, 2), // bucket 8
                new Bucket(-1.0, -0.5, 1), // bucket 9
                new Bucket(5.0, 5.656854249492381, 1), // bucket 10
                new Bucket(45.254833995939045, 50.0, 1), // bucket 11
        });
        verifySerialization(histogram, 209);
        assertEquals(SCALE0_ERROR, histogram.getPercentileRelativeError(), 0);
    }

    @Test
    public void testSubnormals() {
        final ComboNrSketch h1 = new ComboNrSketch(10);

        final double positiveSubnormal = DoubleFormat.makeDouble(0, 0, 100);
        assertTrue(positiveSubnormal > 0 && positiveSubnormal < Double.MIN_NORMAL);

        final double negativeSubnormal = DoubleFormat.makeDouble(1, 0, 200);
        assertTrue(negativeSubnormal < 0 && negativeSubnormal > Double.MIN_NORMAL * -1);

        h1.insert(positiveSubnormal);
        verifyHistogram(h1, 1, positiveSubnormal, positiveSubnormal, new Bucket[]{
                new Bucket(positiveSubnormal, positiveSubnormal, 1), // bucket 1
        });

        h1.insert(negativeSubnormal);
        verifyHistogram(h1, 2, negativeSubnormal, positiveSubnormal, new Bucket[]{
                new Bucket(-9.9E-322, -9.9E-322, 1), // bucket 1
                new Bucket(4.94E-322, 4.94E-322, 1), // bucket 2
        });

        h1.insert(0);
        verifyHistogram(h1, 3, negativeSubnormal, positiveSubnormal, new Bucket[]{
                new Bucket(-9.9E-322, -9.9E-322, 1), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(4.9E-322, 4.94E-322, 1), // bucket 3
        });

        h1.insert(10);
        verifyHistogram(h1, 4, negativeSubnormal, 10, new Bucket[]{
                new Bucket(-9.9E-322, -9.9E-322, 1), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(4.9E-324, 5.562684646268003E-309, 1), // bucket 3
                new Bucket(1.0, 10.0, 1), // bucket 4
        });

        h1.insert(20);
        verifyHistogram(h1, 5, negativeSubnormal, 20, new Bucket[]{
                new Bucket(-9.9E-322, -9.9E-322, 1), // bucket 1
                new Bucket(0.0, 0.0, 1), // bucket 2
                new Bucket(4.9E-324, 5.562684646268003E-309, 1), // bucket 3
                new Bucket(1.0, 20.0, 2), // bucket 4
        });

        h1.insert(-50);
        verifyHistogram(h1, 6, -50, 20, new Bucket[]{
                new Bucket(-50.0, -1.0, 1), // bucket 1
                new Bucket(-5.562684646268003E-309, -9.9E-322, 1), // bucket 2
                new Bucket(0.0, 0.0, 1), // bucket 3
                new Bucket(4.9E-324, 5.562684646268003E-309, 1), // bucket 4
                new Bucket(1.0, 20.0, 2), // bucket 5
        });

        h1.insert(-40);
        verifyHistogram(h1, 7, -50, 20, new Bucket[]{
                new Bucket(-50.0, -1.0, 2), // bucket 1
                new Bucket(-5.562684646268003E-309, -9.9E-322, 1), // bucket 2
                new Bucket(0.0, 0.0, 1), // bucket 3
                new Bucket(4.9E-324, 5.562684646268003E-309, 1), // bucket 4
                new Bucket(1.0, 20.0, 2), // bucket 5
        });
    }

    @Test
    public void testLargeNumbers() {
        final ComboNrSketch histogram = new ComboNrSketch(320, TEST_INIT_SCALE);

        double value = -1e6;
        histogram.insert(value);
        verifyHistogram(histogram, 1, value, value, new Bucket[]{
                new Bucket(-1000000.000000, -1000000.000000, 1), // bucket 1
        });
        verifySerialization(histogram, 85);
        assertEquals(INITIAL_ERROR, histogram.getPercentileRelativeError(), 0);

        value = -1e12;
        histogram.insert(value);
        verifyHistogram(histogram, 2, value, -1e6, new Bucket[]{
                new Bucket(-1.0E12, -9.655098358185806E11, 1), // bucket 1
                new Bucket(-1004119.8176617863, -1000000.0, 1), // bucket 2
        });
        verifySerialization(histogram, 404);
        assertEquals(SCALE4_ERROR, histogram.getPercentileRelativeError(), 0);

        value = -1e24;
        histogram.insert(value);
        verifyHistogram(histogram, 3, value, -1e6, new Bucket[]{
                new Bucket(-1.0E24, -8.548396450010091E23, 1), // bucket 1
                new Bucket(-1.099511627776E12, -9.245753863266149E11, 1), // bucket 2
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 3
        });
        verifySerialization(histogram, 324);
        assertEquals(SCALE2_ERROR, histogram.getPercentileRelativeError(), 0);

        value = -1e48;
        histogram.insert(value);
        verifyHistogram(histogram, 4, value, -1e6, new Bucket[]{
                new Bucket(-1.0E48, -7.3075081866545146E47, 1), // bucket 1
                new Bucket(-1.2089258196146292E24, -8.548396450010093E23, 1), // bucket 2
                new Bucket(-1.099511627776E12, -7.774721279938688E11, 1), // bucket 3
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 4
        });
        verifySerialization(histogram, 364);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        value = 1e6;
        histogram.insert(value);
        verifyHistogram(histogram, 5, -1e48, value, new Bucket[]{
                new Bucket(-1.0E48, -7.3075081866545146E47, 1), // bucket 1
                new Bucket(-1.2089258196146292E24, -8.548396450010093E23, 1), // bucket 2
                new Bucket(-1.099511627776E12, -7.774721279938688E11, 1), // bucket 3
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 4
                new Bucket(1000000.0, 1000000.0, 1), // bucket 5
        });
        verifySerialization(histogram, 472);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        value = 1e12;
        histogram.insert(value);
        verifyHistogram(histogram, 6, -1e48, value, new Bucket[]{
                new Bucket(-1.0E48, -7.3075081866545146E47, 1), // bucket 1
                new Bucket(-1.2089258196146292E24, -8.548396450010093E23, 1), // bucket 2
                new Bucket(-1.099511627776E12, -7.774721279938688E11, 1), // bucket 3
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 4
                new Bucket(1000000.0, 1004119.8176617863, 1), // bucket 5
                new Bucket(9.655098358185806E11, 1.0E12, 1), // bucket 6
        });
        verifySerialization(histogram, 791);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        value = 1e24;
        histogram.insert(value);
        verifyHistogram(histogram, 7, -1e48, value, new Bucket[]{
                new Bucket(-1.0E48, -7.3075081866545146E47, 1), // bucket 1
                new Bucket(-1.2089258196146292E24, -8.548396450010093E23, 1), // bucket 2
                new Bucket(-1.099511627776E12, -7.774721279938688E11, 1), // bucket 3
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 4
                new Bucket(1000000.0, 1048576.0, 1), // bucket 5
                new Bucket(9.245753863266149E11, 1.099511627776E12, 1), // bucket 6
                new Bucket(8.548396450010091E23, 1.0E24, 1), // bucket 7
        });
        verifySerialization(histogram, 711);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);

        value = 1e48;
        histogram.insert(value);
        verifyHistogram(histogram, 8, -1e48, value, new Bucket[]{
                new Bucket(-1.0E48, -7.3075081866545146E47, 1), // bucket 1
                new Bucket(-1.2089258196146292E24, -8.548396450010093E23, 1), // bucket 2
                new Bucket(-1.099511627776E12, -7.774721279938688E11, 1), // bucket 3
                new Bucket(-1048576.0, -1000000.0, 1), // bucket 4
                new Bucket(1000000.0, 1048576.0, 1), // bucket 5
                new Bucket(7.774721279938688E11, 1.099511627776E12, 1), // bucket 6
                new Bucket(8.548396450010093E23, 1.2089258196146292E24, 1), // bucket 7
                new Bucket(7.3075081866545146E47, 1.0E48, 1), // bucket 8
        });
        verifySerialization(histogram, 751);
        assertEquals(SCALE1_ERROR, histogram.getPercentileRelativeError(), 0);
    }

    private static void verifyAggregates(final NrSketch histogram, final double expectedSum, final int expectedMaxNumOfBuckets) {
        // count, min, max are already tested in verifyHistogram().
        assertEquals(expectedSum, histogram.getSum(), 0);
        assertEquals(expectedMaxNumOfBuckets, histogram.getMaxNumOfBuckets());
    }

    @Test
    public void testMerge() {
        final ComboNrSketch empty1 = new ComboNrSketch(10);
        final ComboNrSketch empty2 = new ComboNrSketch(10);

        final ComboNrSketch negativeOnly1 = new ComboNrSketch(10);
        insertData(negativeOnly1, -100, 0, 100);
        verifyHistogram(negativeOnly1, 100, -100, -1, new Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 32), // bucket 2
                new Bucket(-32.000000, -16.000000, 16), // bucket 3
                new Bucket(-16.000000, -8.000000, 8), // bucket 4
                new Bucket(-8.000000, -4.000000, 4), // bucket 5
                new Bucket(-4.000000, -2.000000, 2), // bucket 6
                new Bucket(-2.000000, -1.000000, 1), // bucket 7
        });
        verifyAggregates(negativeOnly1, -5050, 10);

        final ComboNrSketch negativeOnly2 = new ComboNrSketch(10);
        insertData(negativeOnly2, -150, -50, 100);
        verifyHistogram(negativeOnly2, 100, -150, -51, new Bucket[]{
                new Bucket(-150.0, -128.0, 23), // bucket 1
                new Bucket(-128.0, -107.63474115247546, 20), // bucket 2
                new Bucket(-107.63474115247546, -90.50966799187808, 17), // bucket 3
                new Bucket(-90.50966799187808, -76.10925536017415, 14), // bucket 4
                new Bucket(-76.10925536017415, -64.0, 13), // bucket 5
                new Bucket(-64.0, -53.81737057623773, 10), // bucket 6
                new Bucket(-53.81737057623773, -51.0, 3), // bucket 7
        });
        verifyAggregates(negativeOnly2, -10050.0, 10);

        final ComboNrSketch positiveOnly1 = new ComboNrSketch(10);
        insertData(positiveOnly1, 0, 100, 100);
        verifyHistogram(positiveOnly1, 100, 0, 99, new Bucket[]{
                new Bucket(0.000000, 0.000000, 1), // bucket 1
                new Bucket(1.000000, 2.000000, 1), // bucket 2
                new Bucket(2.000000, 4.000000, 2), // bucket 3
                new Bucket(4.000000, 8.000000, 4), // bucket 4
                new Bucket(8.000000, 16.000000, 8), // bucket 5
                new Bucket(16.000000, 32.000000, 16), // bucket 6
                new Bucket(32.000000, 64.000000, 32), // bucket 7
                new Bucket(64.000000, 99.000000, 36), // bucket 8
        });
        verifyAggregates(positiveOnly1, 4950.0, 10);

        final ComboNrSketch positiveOnly2 = new ComboNrSketch(10);
        insertData(positiveOnly2, 50, 150, 100);
        verifyHistogram(positiveOnly2, 100, 50, 149, new Bucket[]{
                new Bucket(50.0, 53.81737057623773, 4), // bucket 1
                new Bucket(53.81737057623773, 64.0, 10), // bucket 2
                new Bucket(64.0, 76.10925536017415, 13), // bucket 3
                new Bucket(76.10925536017415, 90.50966799187808, 14), // bucket 4
                new Bucket(90.50966799187808, 107.63474115247546, 17), // bucket 5
                new Bucket(107.63474115247546, 128.0, 20), // bucket 6
                new Bucket(128.0, 149.0, 22), // bucket 7
        });
        verifyAggregates(positiveOnly2, 9950.0, 10);

        final ComboNrSketch both1 = new ComboNrSketch(10);
        insertData(both1, -100, 100, 200);
        verifyHistogram(both1, 200, -100, 99, new Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 32), // bucket 2
                new Bucket(-32.000000, -16.000000, 16), // bucket 3
                new Bucket(-16.000000, -8.000000, 8), // bucket 4
                new Bucket(-8.000000, -4.000000, 4), // bucket 5
                new Bucket(-4.000000, -2.000000, 2), // bucket 6
                new Bucket(-2.000000, -1.000000, 1), // bucket 7
                new Bucket(0.000000, 0.000000, 1), // bucket 8
                new Bucket(1.000000, 2.000000, 1), // bucket 9
                new Bucket(2.000000, 4.000000, 2), // bucket 10
                new Bucket(4.000000, 8.000000, 4), // bucket 11
                new Bucket(8.000000, 16.000000, 8), // bucket 12
                new Bucket(16.000000, 32.000000, 16), // bucket 13
                new Bucket(32.000000, 64.000000, 32), // bucket 14
                new Bucket(64.000000, 99.000000, 36), // bucket 15
        });
        verifyAggregates(both1, -100.0, 20);

        final ComboNrSketch both2 = new ComboNrSketch(10);
        insertData(both2, -50, 150, 200);
        verifyHistogram(both2, 200, -50, 149, new Bucket[]{
                new Bucket(-50.000000, -32.000000, 19), // bucket 1
                new Bucket(-32.000000, -16.000000, 16), // bucket 2
                new Bucket(-16.000000, -8.000000, 8), // bucket 3
                new Bucket(-8.000000, -4.000000, 4), // bucket 4
                new Bucket(-4.000000, -2.000000, 2), // bucket 5
                new Bucket(-2.000000, -1.000000, 1), // bucket 6
                new Bucket(0.000000, 0.000000, 1), // bucket 7
                new Bucket(1.000000, 2.000000, 1), // bucket 8
                new Bucket(2.000000, 4.000000, 2), // bucket 9
                new Bucket(4.000000, 8.000000, 4), // bucket 10
                new Bucket(8.000000, 16.000000, 8), // bucket 11
                new Bucket(16.000000, 32.000000, 16), // bucket 12
                new Bucket(32.000000, 64.000000, 32), // bucket 13
                new Bucket(64.000000, 128.000000, 64), // bucket 14
                new Bucket(128.000000, 149.000000, 22), // bucket 15
        });
        verifyAggregates(both2, 9900.0, 20);

        verifyHistogram(empty1.merge(empty2), 0, Double.NaN, Double.NaN, EMPTY_BUCKET_LIST);

        verifyHistogram(negativeOnly1.merge(negativeOnly2), 200, -150, -1, new Bucket[]{
                new Bucket(-150.000000, -128.000000, 23), // bucket 1
                new Bucket(-128.000000, -64.000000, 101), // bucket 2
                new Bucket(-64.000000, -32.000000, 45), // bucket 3
                new Bucket(-32.000000, -16.000000, 16), // bucket 4
                new Bucket(-16.000000, -8.000000, 8), // bucket 5
                new Bucket(-8.000000, -4.000000, 4), // bucket 6
                new Bucket(-4.000000, -2.000000, 2), // bucket 7
                new Bucket(-2.000000, -1.000000, 1), // bucket 8
        });

        verifyHistogram(positiveOnly1.merge(positiveOnly2), 200, 0, 149, new Bucket[]{
                new Bucket(0.000000, 0.000000, 1), // bucket 1
                new Bucket(1.000000, 2.000000, 1), // bucket 2
                new Bucket(2.000000, 4.000000, 2), // bucket 3
                new Bucket(4.000000, 8.000000, 4), // bucket 4
                new Bucket(8.000000, 16.000000, 8), // bucket 5
                new Bucket(16.000000, 32.000000, 16), // bucket 6
                new Bucket(32.000000, 64.000000, 46), // bucket 7
                new Bucket(64.000000, 128.000000, 100), // bucket 8
                new Bucket(128.000000, 149.000000, 22), // bucket 9
        });

        verifyHistogram(both1.merge(both2), 400, -100, 149, new Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 51), // bucket 2
                new Bucket(-32.000000, -16.000000, 32), // bucket 3
                new Bucket(-16.000000, -8.000000, 16), // bucket 4
                new Bucket(-8.000000, -4.000000, 8), // bucket 5
                new Bucket(-4.000000, -2.000000, 4), // bucket 6
                new Bucket(-2.000000, -1.000000, 2), // bucket 7
                new Bucket(0.000000, 0.000000, 2), // bucket 8
                new Bucket(1.000000, 2.000000, 2), // bucket 9
                new Bucket(2.000000, 4.000000, 4), // bucket 10
                new Bucket(4.000000, 8.000000, 8), // bucket 11
                new Bucket(8.000000, 16.000000, 16), // bucket 12
                new Bucket(16.000000, 32.000000, 32), // bucket 13
                new Bucket(32.000000, 64.000000, 64), // bucket 14
                new Bucket(64.000000, 128.000000, 100), // bucket 15
                new Bucket(128.000000, 149.000000, 22), // bucket 16
        });

        verifyHistogram(positiveOnly1.merge(negativeOnly1), 400, -150, 149, new Bucket[]{
                new Bucket(-150.000000, -128.000000, 23), // bucket 1
                new Bucket(-128.000000, -64.000000, 101), // bucket 2
                new Bucket(-64.000000, -32.000000, 45), // bucket 3
                new Bucket(-32.000000, -16.000000, 16), // bucket 4
                new Bucket(-16.000000, -8.000000, 8), // bucket 5
                new Bucket(-8.000000, -4.000000, 4), // bucket 6
                new Bucket(-4.000000, -2.000000, 2), // bucket 7
                new Bucket(-2.000000, -1.000000, 1), // bucket 8
                new Bucket(0.000000, 0.000000, 1), // bucket 9
                new Bucket(1.000000, 2.000000, 1), // bucket 10
                new Bucket(2.000000, 4.000000, 2), // bucket 11
                new Bucket(4.000000, 8.000000, 4), // bucket 12
                new Bucket(8.000000, 16.000000, 8), // bucket 13
                new Bucket(16.000000, 32.000000, 16), // bucket 14
                new Bucket(32.000000, 64.000000, 46), // bucket 15
                new Bucket(64.000000, 128.000000, 100), // bucket 16
                new Bucket(128.000000, 149.000000, 22), // bucket 17
        });

        verifyHistogram(negativeOnly2.merge(positiveOnly2), 200, -150, 149, new Bucket[]{
                new Bucket(-150.0, -128.0, 23), // bucket 1
                new Bucket(-128.0, -107.63474115247546, 20), // bucket 2
                new Bucket(-107.63474115247546, -90.50966799187808, 17), // bucket 3
                new Bucket(-90.50966799187808, -76.10925536017415, 14), // bucket 4
                new Bucket(-76.10925536017415, -64.0, 13), // bucket 5
                new Bucket(-64.0, -53.81737057623773, 10), // bucket 6
                new Bucket(-53.81737057623773, -51.0, 3), // bucket 7
                new Bucket(50.0, 53.81737057623773, 4), // bucket 8
                new Bucket(53.81737057623773, 64.0, 10), // bucket 9
                new Bucket(64.0, 76.10925536017415, 13), // bucket 10
                new Bucket(76.10925536017415, 90.50966799187808, 14), // bucket 11
                new Bucket(90.50966799187808, 107.63474115247546, 17), // bucket 12
                new Bucket(107.63474115247546, 128.0, 20), // bucket 13
                new Bucket(128.0, 149.0, 22), // bucket 14
        });

        verifyHistogram(both1.merge(empty1), 400, -100, 149, new Bucket[]{
                new Bucket(-100.000000, -64.000000, 37), // bucket 1
                new Bucket(-64.000000, -32.000000, 51), // bucket 2
                new Bucket(-32.000000, -16.000000, 32), // bucket 3
                new Bucket(-16.000000, -8.000000, 16), // bucket 4
                new Bucket(-8.000000, -4.000000, 8), // bucket 5
                new Bucket(-4.000000, -2.000000, 4), // bucket 6
                new Bucket(-2.000000, -1.000000, 2), // bucket 7
                new Bucket(0.000000, 0.000000, 2), // bucket 8
                new Bucket(1.000000, 2.000000, 2), // bucket 9
                new Bucket(2.000000, 4.000000, 4), // bucket 10
                new Bucket(4.000000, 8.000000, 8), // bucket 11
                new Bucket(8.000000, 16.000000, 16), // bucket 12
                new Bucket(16.000000, 32.000000, 32), // bucket 13
                new Bucket(32.000000, 64.000000, 64), // bucket 14
                new Bucket(64.000000, 128.000000, 100), // bucket 15
                new Bucket(128.000000, 149.000000, 22), // bucket 16
        });

        verifyHistogram(empty2.merge(both2), 200, -50, 149, new Bucket[]{
                new Bucket(-50.000000, -32.000000, 19), // bucket 1
                new Bucket(-32.000000, -16.000000, 16), // bucket 2
                new Bucket(-16.000000, -8.000000, 8), // bucket 3
                new Bucket(-8.000000, -4.000000, 4), // bucket 4
                new Bucket(-4.000000, -2.000000, 2), // bucket 5
                new Bucket(-2.000000, -1.000000, 1), // bucket 6
                new Bucket(0.000000, 0.000000, 1), // bucket 7
                new Bucket(1.000000, 2.000000, 1), // bucket 8
                new Bucket(2.000000, 4.000000, 2), // bucket 9
                new Bucket(4.000000, 8.000000, 4), // bucket 10
                new Bucket(8.000000, 16.000000, 8), // bucket 11
                new Bucket(16.000000, 32.000000, 16), // bucket 12
                new Bucket(32.000000, 64.000000, 32), // bucket 13
                new Bucket(64.000000, 128.000000, 64), // bucket 14
                new Bucket(128.000000, 149.000000, 22), // bucket 15
        });
    }

    @Test
    public void testSubtractionAndDeepCopy() {
        final ComboNrSketch h1 = new ComboNrSketch(10);
        final ComboNrSketch h2 = new ComboNrSketch(10);

        assertEquals(h1, h1.deepCopy());

        h1.subtract(h2);
        verifyHistogram(h1, 0, Double.NaN, Double.NaN, EMPTY_BUCKET_LIST);

        insertData(h1, 0, 200, 200);
        verifyHistogram(h1, 200, 0, 199, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 128.0, 64), // bucket 8
                new Bucket(128.0, 199.0, 72), // bucket 9
        });

        assertEquals(h1.deepCopy(), h1.subtract(h2));

        final ComboNrSketch h3 = new ComboNrSketch(20);
        insertData(h3, 100, 300, 200);

        verifyHistogram(h3, 200, 100, 299, new Bucket[]{
                new Bucket(100.0, 107.63474115247546, 8), // bucket 1
                new Bucket(107.63474115247546, 117.37651753019792, 10), // bucket 2
                new Bucket(117.37651753019792, 128.0, 10), // bucket 3
                new Bucket(128.0, 139.58498978115298, 12), // bucket 4
                new Bucket(139.58498978115298, 152.2185107203483, 13), // bucket 5
                new Bucket(152.2185107203483, 165.99546299532923, 13), // bucket 6
                new Bucket(165.99546299532923, 181.01933598375618, 16), // bucket 7
                new Bucket(181.01933598375618, 197.40298565221642, 16), // bucket 8
                new Bucket(197.40298565221642, 215.2694823049509, 18), // bucket 9
                new Bucket(215.2694823049509, 234.75303506039583, 19), // bucket 10
                new Bucket(234.75303506039583, 256.0, 21), // bucket 11
                new Bucket(256.0, 279.16997956230597, 24), // bucket 12
                new Bucket(279.16997956230597, 299.0, 20), // bucket 13
        });

        final NrSketch h4 = h1.deepCopy().merge(h3);

        verifyHistogram(h4, 400, 0, 299, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 128.0, 92), // bucket 8
                new Bucket(128.0, 256.0, 200), // bucket 9
                new Bucket(256.0, 299.0, 44), // bucket 10
        });

        verifyHistogram(h4.deepCopy().subtract(h3), 200, 0, 256, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 128.0, 64), // bucket 8
                new Bucket(128.0, 256.0, 72), // bucket 9
        });

        final NrSketch h5 = h4.deepCopy().subtract(h1);
        verifyHistogram(h5, 200, 64, 299, new Bucket[]{
                new Bucket(64.0, 128.0, 28), // bucket 1
                new Bucket(128.0, 256.0, 128), // bucket 2
                new Bucket(256.0, 299.0, 44), // bucket 3
        });

        assertEquals("maxNumBucketsPerHistogram=10, histograms.size()=1\n" +
                "totalCount=200, sum=39900.0, min=64.0, max=299.0, bucketHoldsPositiveNumbers=true, scale=0, countForNegatives=0, countForZero=0, buckets={maxSize=10, indexBase=6, indexStart=6, indexEnd=8, array={28,128,44,}}", h5.toString());

        insertData(h4, -200, -100, 100);
        final NrSketch h6 = h4.deepCopy().subtract(h1);
        verifyHistogram(h6, 300, -200, 299, new Bucket[]{
                new Bucket(-200.0, -197.40298565221642, 3), // bucket 1
                new Bucket(-197.40298565221642, -181.01933598375618, 16), // bucket 2
                new Bucket(-181.01933598375618, -165.99546299532923, 16), // bucket 3
                new Bucket(-165.99546299532923, -152.2185107203483, 13), // bucket 4
                new Bucket(-152.2185107203483, -139.58498978115298, 13), // bucket 5
                new Bucket(-139.58498978115298, -128.0, 12), // bucket 6
                new Bucket(-128.0, -117.37651753019792, 10), // bucket 7
                new Bucket(-117.37651753019792, -107.63474115247546, 10), // bucket 8
                new Bucket(-107.63474115247546, -101.0, 7), // bucket 9
                new Bucket(64.0, 128.0, 28), // bucket 10
                new Bucket(128.0, 256.0, 128), // bucket 11
                new Bucket(256.0, 299.0, 44), // bucket 12
        });

        final NrSketch h7 = new ComboNrSketch(10);
        insertData(h7, -200, -150, 50);

        verifyHistogram(h7, 50, -200, -151, new Bucket[]{
                new Bucket(-200.0, -197.40298565221642, 3), // bucket 1
                new Bucket(-197.40298565221642, -189.03374668025592, 8), // bucket 2
                new Bucket(-189.03374668025592, -181.01933598375618, 8), // bucket 3
                new Bucket(-181.01933598375618, -173.34471000792226, 8), // bucket 4
                new Bucket(-173.34471000792226, -165.99546299532923, 8), // bucket 5
                new Bucket(-165.99546299532923, -158.95779994540595, 7), // bucket 6
                new Bucket(-158.95779994540595, -152.2185107203483, 6), // bucket 7
                new Bucket(-152.2185107203483, -151.0, 2), // bucket 8
        });

        verifyHistogram(h6.subtract(h7), 250, -152.2185107203483, 299, new Bucket[]{
                new Bucket(-152.2185107203483, -139.58498978115298, 11), // bucket 1
                new Bucket(-139.58498978115298, -128.0, 12), // bucket 2
                new Bucket(-128.0, -117.37651753019792, 10), // bucket 3
                new Bucket(-117.37651753019792, -107.63474115247546, 10), // bucket 4
                new Bucket(-107.63474115247546, -101.0, 7), // bucket 5
                new Bucket(64.0, 128.0, 28), // bucket 6
                new Bucket(128.0, 256.0, 128), // bucket 7
                new Bucket(256.0, 299.0, 44), // bucket 8
        });
    }

    @Test
    public void testHistogramIsCompactedWhenEmpty() {
        final NrSketch emptyHistogram = new ComboNrSketch(10);

        final NrSketch histogramBoth = new ComboNrSketch(10);
        insertData(histogramBoth, 0, 1024, 4096);
        insertData(histogramBoth, -1024, 0, 4096);

        final NrSketch histogramPositiveOnly = new ComboNrSketch(10);
        insertData(histogramPositiveOnly, 0, 1024, 4096);

        final NrSketch histogramNegativeOnly = new ComboNrSketch(10);
        insertData(histogramNegativeOnly, -1024, 0, 4096);

        assertEquals(
                histogramBoth.deepCopy().subtract(histogramNegativeOnly),
                histogramPositiveOnly
        );
        assertEquals(
                histogramBoth.deepCopy().subtract(histogramPositiveOnly),
                histogramNegativeOnly
        );
        assertEquals(
                histogramBoth.deepCopy().subtract(histogramBoth),
                emptyHistogram
        );
        // Ensure that we can still insert and interact with the compacted histogram and subtract/merge are inverse operations
        assertEquals(
                histogramBoth.deepCopy().subtract(histogramNegativeOnly).merge(histogramNegativeOnly),
                histogramBoth
        );
        assertEquals(
                histogramBoth.deepCopy().subtract(histogramPositiveOnly).merge(histogramPositiveOnly),
                histogramBoth
        );
    }

    @Test
    public void testPercentile() {
        final ComboNrSketch histogram = new ComboNrSketch(TEST_MAX_BUCKETS);

        final double min = -10_000;
        final double interval = 1;
        final double max = 1000_000;

        for (double d = min; d <= max; d += interval) {
            histogram.insert(d);
        }

        assertEquals(SCALE4_ERROR, histogram.getPercentileRelativeError(), 0);

        assertEquals(min, histogram.getMin(), 0);
        assertEquals(max, histogram.getMax(), 0);

        assertEquals(TEST_MAX_BUCKETS * 2, histogram.getMaxNumOfBuckets());

        verifyPercentile(histogram, new double[]{0}, new double[]{min});
        verifyPercentile(histogram, new double[]{100}, new double[]{max});

        verifyPercentile(histogram, new double[]{0, 100}, new double[]{min, max});
        verifyPercentile(histogram, new double[]{-1, 0, 0, 100, 100, 110}, new double[]{min, min, min, max, max, max});

        verifyPercentile(histogram, new double[]{0.01}, new double[]{-9870.992343051144});
        verifyPercentile(histogram, new double[]{0.1}, new double[]{-9131.197920960301});
        verifyPercentile(histogram, new double[]{1}, new double[]{100.88643703543016});
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
        final ComboNrSketch histogram = new ComboNrSketch(nBuckets);

        final double min = Double.NaN;
        final double max = Double.NaN;

        verifyPercentile(histogram, new double[]{0}, new double[]{min});
        verifyPercentile(histogram, new double[]{100}, new double[]{max});

        verifyPercentile(histogram, new double[]{0, 100}, new double[]{min, max});
        verifyPercentile(histogram, new double[]{-1, 0, 0, 100, 100, 110}, new double[]{min, min, min, max, max, max});

        verifyPercentile(histogram, new double[]{50, 90, 95, 99}, new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN});
    }

    private static ComboNrSketch testComboHistogram(final int numBuckets, final double from, final double to, final int numDataPoints, final NrSketch.Bucket[] expectedBuckets) {
        final ComboNrSketch histogram = new ComboNrSketch(numBuckets);
        final double max = insertData(histogram, from, to, numDataPoints);
        verifyHistogram(histogram, numDataPoints, from, max, expectedBuckets);
        return histogram;
    }
}
