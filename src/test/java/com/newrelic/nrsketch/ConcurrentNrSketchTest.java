// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.NrSketch.Bucket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.newrelic.nrsketch.SimpleNrSketchTest.SCALE4_ERROR;
import static com.newrelic.nrsketch.SimpleNrSketchTest.TEST_MAX_BUCKETS;
import static com.newrelic.nrsketch.SimpleNrSketchTest.insertData;
import static com.newrelic.nrsketch.SimpleNrSketchTest.verifyHistogram;
import static com.newrelic.nrsketch.SimpleNrSketchTest.verifyPercentile;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ConcurrentNrSketchTest {
    public interface SketchMaker {
        NrSketch getSketch(final int maxNumBucketsPerHistogram);
    }

    @Parameterized.Parameters(name = "baseSketch {0}")
    public static Collection<Object[]> data() {
        final Collection<Object[]> collection = new ArrayList<>();
        collection.add(new SketchMaker[]{SimpleNrSketch::new});
        collection.add(new SketchMaker[]{ComboNrSketch::new});
        return collection;
    }

    @Parameterized.Parameter()
    public SketchMaker sketchMaker;

    // Equality is also tested in verifySerialization()
    @Test
    public void testEqualAndHashAndDeepCopy() {
        final ConcurrentNrSketch s1 = new ConcurrentNrSketch(sketchMaker.getSketch(TEST_MAX_BUCKETS));
        final ConcurrentNrSketch s2 = new ConcurrentNrSketch(sketchMaker.getSketch(TEST_MAX_BUCKETS));
        final ConcurrentNrSketch s3 = new ConcurrentNrSketch(sketchMaker.getSketch(99));

        assertEquals(s1.deepCopy(), s1);

        assertNotEquals(s1, s3);

        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());

        s1.insert(11);
        assertNotEquals(s1, s2);
        assertNotEquals(s1.hashCode(), s2.hashCode());

        assertEquals(s1.deepCopy(), s1);

        s2.insert(11);
        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());

        s1.insert(-22);
        assertNotEquals(s1, s2);
        assertNotEquals(s1.hashCode(), s2.hashCode());

        assertEquals(s1.deepCopy(), s1);

        s2.insert(-22);
        assertEquals(s1, s2);
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    @Test
    public void happyPath() throws Exception {
        final int numThreads = 3;
        final long valuesPerThread = 10000;
        final long expectedCount = valuesPerThread * numThreads;

        final ConcurrentNrSketch sketch = new ConcurrentNrSketch(sketchMaker.getSketch(TEST_MAX_BUCKETS));

        verifySerialization(sketch, 77, 11); // Test empty sketch

        final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            threadPool.execute(() -> {
                for (long count = 0; count < valuesPerThread; count++) {
                    sketch.insert(count);
                }
            });
        }

        while (sketch.getCount() < expectedCount) {
            Thread.sleep(100);
        }
        threadPool.shutdown();

        assertEquals(expectedCount, sketch.getCount());

        verifyPercentile(sketch, new double[]{0, 25, 50, 90, 100},
                new double[]{0, 2489.4104853260333, 4978.8209706520665, 9131.197920960301, valuesPerThread - 1});

        assertEquals(320, sketch.getMaxNumOfBuckets());

        assertEquals(0, sketch.getMin(), 0);
        assertEquals(valuesPerThread - 1, sketch.getMax(), 0);

        assertEquals(1.49985E8, sketch.getSum(), 0);

        assertEquals(SCALE4_ERROR, sketch.getPercentileRelativeError(), 0);

        verifySerialization(sketch, 344, 353);
    }

    @Test
    public void testBucketIterator() {
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

        final ConcurrentNrSketch histogram = testConcurrentHistogram(10, 0, 100, 100, buckets);

        // Test that we can iterate inside a synchronized block.
        synchronized (histogram) {
            verifyHistogram(histogram, 100, 0, 99, buckets);
            verifySerialization(histogram, 84, 93);
        }
    }

    @Test
    public void testMergeAndSubtractionSameClass() {
        final ConcurrentNrSketch h1 = testConcurrentHistogram(10, 0, 100, 100, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        final ConcurrentNrSketch h2 = testConcurrentHistogram(10, 0, 100, 100, new Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        assertThat(h1.merge(h2), instanceOf(ConcurrentNrSketch.class)); // Both are ConcurrentNrSketch

        verifyHistogram(h1, 200, 0, 99, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 2), // bucket 1
                new Bucket(1.0, 2.0, 2), // bucket 2
                new Bucket(2.0, 4.0, 4), // bucket 3
                new Bucket(4.0, 8.0, 8), // bucket 4
                new Bucket(8.0, 16.0, 16), // bucket 5
                new Bucket(16.0, 32.0, 32), // bucket 6
                new Bucket(32.0, 64.0, 64), // bucket 7
                new Bucket(64.0, 99.0, 72), // bucket 8
        });

        verifySerialization(h1, 84, 93);

        assertThat(h1.subtract(h2), instanceOf(ConcurrentNrSketch.class));

        verifyHistogram(h1, 100, 0, 99, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
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
    public void testMergeAndSubtractionDifferentClass() {
        final ConcurrentNrSketch h1 = testConcurrentHistogram(10, 0, 100, 100, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        final ConcurrentNrSketch h2 = testConcurrentHistogram(10, 0, 100, 100, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });

        assertThat(h1.merge(h2.getSketch()), instanceOf(ConcurrentNrSketch.class)); // Merging ConcurrentNrSketch with wrapped sketch.

        verifyHistogram(h1, 200, 0, 99, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 2), // bucket 1
                new Bucket(1.0, 2.0, 2), // bucket 2
                new Bucket(2.0, 4.0, 4), // bucket 3
                new Bucket(4.0, 8.0, 8), // bucket 4
                new Bucket(8.0, 16.0, 16), // bucket 5
                new Bucket(16.0, 32.0, 32), // bucket 6
                new Bucket(32.0, 64.0, 64), // bucket 7
                new Bucket(64.0, 99.0, 72), // bucket 8
        });

        verifySerialization(h1, 84, 93);

        // Subtract a wrapped sketch from a ConcurrentNrSketch.
        assertThat(h1.subtract(h2.getSketch()), instanceOf(ConcurrentNrSketch.class));

        verifyHistogram(h1, 100, 0, 99, new NrSketch.Bucket[]{
                new Bucket(0.0, 0.0, 1), // bucket 1
                new Bucket(1.0, 2.0, 1), // bucket 2
                new Bucket(2.0, 4.0, 2), // bucket 3
                new Bucket(4.0, 8.0, 4), // bucket 4
                new Bucket(8.0, 16.0, 8), // bucket 5
                new Bucket(16.0, 32.0, 16), // bucket 6
                new Bucket(32.0, 64.0, 32), // bucket 7
                new Bucket(64.0, 99.0, 36), // bucket 8
        });
    }

    public static void verifySerialization(final ConcurrentNrSketch sketch, final int expectedSimpleNrSketchBufferSize, final int expectedComboSketchBufferSize) {
        if (sketch.getSketch() instanceof SimpleNrSketch) {
            SimpleNrSketchTest.verifySerialization(sketch, expectedSimpleNrSketchBufferSize);
        } else if (sketch.getSketch() instanceof ComboNrSketch) {
            SimpleNrSketchTest.verifySerialization(sketch, expectedComboSketchBufferSize);
        } else {
            throw new IllegalArgumentException("Unknown subSketch class " + sketch.getSketch().getClass().getName());
        }
    }

    private ConcurrentNrSketch testConcurrentHistogram(final int numBuckets, final double from, final double to, final int numDataPoints, final NrSketch.Bucket[] expectedBuckets) {
        final ConcurrentNrSketch histogram = new ConcurrentNrSketch(sketchMaker.getSketch(numBuckets));
        final double max = insertData(histogram, from, to, numDataPoints);
        verifyHistogram(histogram, numDataPoints, from, max, expectedBuckets);
        return histogram;
    }
}
