// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledExpIndexer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

// The bucketHoldsPositiveNumbers flag controls whether positive or negative numbers go to the bucket array.
// It controls which kind of numbers are favored. The kind going into the bucket array will have high
// distribution resolution, while the other kind will be counted by only one counter, "countForNegatives",
// or "totalCount - countForNegatives", in the case of positive numbers.

public class SimpleNrSketch implements NrSketch {
    public static final int DEFAULT_MAX_BUCKETS = 320; // 2.17% relative error (scale 4) for max/min contrast up to 1M
    public static final int DEFAULT_INIT_SCALE = 12; // .0085% relative error

    public static final Function<Integer, ScaledExpIndexer> DEFAULT_INDEXER_MAKER = IndexerOption.AUTO_SELECT;

    private WindowedCounterArray buckets;
    private final boolean bucketHoldsPositiveNumbers;
    private ScaledExpIndexer indexer;
    private final Function<Integer, ScaledExpIndexer> indexerMaker;

    private long totalCount;
    private long countForNegatives;
    private long countForZero;

    private double min = Double.NaN;
    private double max = Double.NaN;
    private double sum = 0;

    public SimpleNrSketch() {
        this(DEFAULT_MAX_BUCKETS);
    }

    public SimpleNrSketch(final int maxNumBuckets) {
        this(maxNumBuckets, DEFAULT_INIT_SCALE);
    }

    public SimpleNrSketch(final int maxNumBuckets, final int initialScale) {
        this(maxNumBuckets, initialScale, true, DEFAULT_INDEXER_MAKER);
    }

    public static SimpleNrSketch newNegativeHistogram(final int maxNumBuckets, final int initialScale) {
        return new SimpleNrSketch(maxNumBuckets, initialScale, false, DEFAULT_INDEXER_MAKER);
    }

    public SimpleNrSketch(final int maxNumBuckets,
                          final int initialScale,
                          final boolean bucketHoldsPositiveNumbers,
                          final Function<Integer, ScaledExpIndexer> indexerMaker) {
        buckets = new WindowedCounterArray(maxNumBuckets);
        this.bucketHoldsPositiveNumbers = bucketHoldsPositiveNumbers;
        this.indexerMaker = indexerMaker;
        this.indexer = indexerMaker.apply(initialScale);
    }

    // For deserialization only
    public SimpleNrSketch(final WindowedCounterArray buckets,
                          final boolean bucketHoldsPositiveNumbers,
                          final int scale,
                          final Function<Integer, ScaledExpIndexer> indexerMaker,
                          final long totalCount,
                          final long countForNegatives,
                          final long countForZero,
                          final double min,
                          final double max,
                          final double sum) {
        this.buckets = buckets;
        this.bucketHoldsPositiveNumbers = bucketHoldsPositiveNumbers;
        this.indexer = indexerMaker.apply(scale);
        this.indexerMaker = indexerMaker;

        this.totalCount = totalCount;
        this.countForNegatives = countForNegatives;
        this.countForZero = countForZero;

        this.min = min;
        this.max = max;
        this.sum = sum;
    }

    @SuppressFBWarnings(value = "FE_FLOATING_POINT_EQUALITY")
    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof SimpleNrSketch)) {
            return false;
        }
        final SimpleNrSketch other = (SimpleNrSketch) obj;

        // Only scale matters. Indexer is not used in equals().
        return totalCount == other.totalCount
                && sum == other.sum
                && equalsWithNaN(min, other.min)
                && equalsWithNaN(max, other.max)
                && bucketHoldsPositiveNumbers == other.bucketHoldsPositiveNumbers
                && getScale() == other.getScale()
                && indexerMaker.equals(other.indexerMaker)
                && countForNegatives == other.countForNegatives
                && countForZero == other.countForZero
                && buckets.equals(other.buckets);
    }

    public int getScale() {
        return indexer.getScale();
    }

    public Function<Integer, ScaledExpIndexer> getIndexerMaker() {
        return indexerMaker;
    }

    public static boolean equalsWithNaN(final double a, final double b) {
        return a == b || Double.isNaN(a) && Double.isNaN(b); // Need this function because NaN does not equal anything.
    }

    @Override
    public int hashCode() { // Defined just to keep spotbugs happy.
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("totalCount=" + totalCount);
        builder.append(", sum=" + sum);
        builder.append(", min=" + min);
        builder.append(", max=" + max);

        builder.append(", bucketHoldsPositiveNumbers=" + bucketHoldsPositiveNumbers);
        builder.append(", scale=" + getScale());
        builder.append(", countForNegatives=" + countForNegatives);
        builder.append(", countForZero=" + countForZero);

        builder.append(", buckets={");
        builder.append(buckets.toString());
        builder.append("}");
        return builder.toString();
    }

    private long valueToIndex(final double value) {
        return indexer.getBucketIndex(value);
    }

    private double indexToBucketStart(final long index) {
        return bucketHoldsPositiveNumbers ? indexer.getBucketStart(index) : -indexer.getBucketStart(index);
    }

    private double indexToBucketEnd(final long index) {
        return bucketHoldsPositiveNumbers ? indexer.getBucketEnd(index) : -indexer.getBucketEnd(index);
    }

    @Override
    public void insert(final double d, final long instances) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return; // Ignore Nan and positive and negative infinities
        }

        updateMin(d);
        updateMax(d);

        sum += d * instances;
        totalCount += instances;

        if (d == 0) {
            countForZero += instances;
            return;
        }

        if (d < 0) {
            countForNegatives += instances;
            if (bucketHoldsPositiveNumbers) {
                return;
            }
        } else {
            if (!bucketHoldsPositiveNumbers) {
                return;
            }
        }

        final long index = valueToIndex(d);

        if (buckets.increment(index, instances)) {
            return;
        }

        // Failed to increment bucket. Need to down scale.
        final int scaleReduction = getScaleReduction(
                Math.min(index, buckets.getIndexStart()),
                Math.max(index, buckets.getIndexEnd()));

        downScale(scaleReduction);

        // Retry with reduced index value.
        if (buckets.increment(index >> scaleReduction, instances)) {
            return;
        }
        throw new RuntimeException("insert(): failed to increment bucket after down scaling");
    }

    private void updateMax(final double d) {
        if (Double.isNaN(max) || d > max) {
            max = d;
        }
    }

    private void updateMin(final double d) {
        if (Double.isNaN(min) || d < min) {
            min = d;
        }
    }

    // Returns amount of downscaling to make the new window fit in maxSize.
    // Input parameters newStart and newEnd represent the index range of the new window.
    private int getScaleReduction(long newStart, long newEnd) {
        if (newStart == WindowedCounterArray.NULL_INDEX || newEnd == WindowedCounterArray.NULL_INDEX) {
            return 0;
        }

        int scaleReduction = 0;

        while (newEnd - newStart + 1 > buckets.getMaxSize()) {
            newStart >>= 1;
            newEnd >>= 1;
            scaleReduction++;
        }

        return scaleReduction;
    }

    // In-place merging in the original bucket array is possible, but more complex.
    // For simplicity, we just create a new array and write to it.
    //
    private void downScale(final int scaleReduction) {
        if (scaleReduction <= 0) {
            return;
        }
        if (!buckets.isEmpty()) {
            final WindowedCounterArray newBuckets = new WindowedCounterArray(buckets.getMaxSize());

            for (long index = buckets.getIndexStart(); index <= buckets.getIndexEnd(); index++) {
                if (!newBuckets.increment(index >> scaleReduction, buckets.get(index))) {
                    throw new RuntimeException("downScale(): failed to increment bucket");
                }
            }
            buckets = newBuckets;
        }
        indexer = indexerMaker.apply(getScale() - scaleReduction);
    }

    @Override
    public NrSketch merge(final NrSketch other) {
        if (!(other instanceof SimpleNrSketch)) {
            throw new RuntimeException("SimpleNrSketch cannot merge with " + other.getClass().getName());
        }
        return merge(this, (SimpleNrSketch) other);
    }

    // Merge 2 histograms. Output in "a". Does not modify "b". Returns a.
    // a and b need not have the same maxSize.
    //
    public static SimpleNrSketch merge(final SimpleNrSketch a, final SimpleNrSketch b) {
        if (a.bucketHoldsPositiveNumbers != b.bucketHoldsPositiveNumbers) {
            throw new IllegalArgumentException("SimpleNrSketch merge not allowed when bucketHoldsPositiveNumbers are different");
        }
        final int commonScale = Math.min(a.getScale(), b.getScale());
        int deltaA = a.getScale() - commonScale;
        int deltaB = b.getScale() - commonScale;

        // Find out newStart and newEnd of combined window
        if (!b.buckets.isEmpty()) {
            final long newStartB = b.buckets.getIndexStart() >> deltaB;
            final long newEndB = b.buckets.getIndexEnd() >> deltaB;
            final long newStart;
            final long newEnd;

            if (!a.buckets.isEmpty()) {
                newStart = Math.min(a.buckets.getIndexStart() >> deltaA, newStartB);
                newEnd = Math.max(a.buckets.getIndexEnd() >> deltaA, newEndB);
            } else {
                newStart = newStartB;
                newEnd = newEndB;
            }
            // Combined window may not fit at commonScale. Add additional reduction if needed.
            deltaA += a.getScaleReduction(newStart, newEnd);
        }

        a.downScale(deltaA); // Apply final "a" reduction

        deltaB = b.getScale() - a.getScale(); // Finalize "b" reduction

        // Now merge b buckets into a.
        if (!b.buckets.isEmpty()) {
            for (long indexB = b.buckets.getIndexStart(); indexB <= b.buckets.getIndexEnd(); indexB++) {
                if (!a.buckets.increment(indexB >> deltaB, b.buckets.get(indexB))) {
                    throw new RuntimeException("merge(): failed to increment bucket");
                }
            }
        }

        a.totalCount += b.totalCount;
        a.countForNegatives += b.countForNegatives;
        a.countForZero += b.countForZero;

        if (!Double.isNaN(b.min)) {
            a.updateMin(b.min);
        }

        if (!Double.isNaN(b.max)) {
            a.updateMax(b.max);
        }

        a.sum += b.sum;

        return a;
    }

    private enum IteratorState {
        NEGATIVE,
        ZERO,
        POSITIVE,
        NO_MORE_BUCKETS
    }

    // BucketIterator.next() reuses Bucket object. Caller must read the bucket before the next call.
    //
    private class PositiveBucketBucketIterator implements Iterator<Bucket> {
        protected IteratorState state;
        protected long cursor;
        protected final Bucket bucket;

        public PositiveBucketBucketIterator() {
            if (Double.isNaN(min)) {
                state = IteratorState.NO_MORE_BUCKETS;
            } else if (min < 0) {
                state = IteratorState.NEGATIVE;
            } else if (min == 0) {
                state = IteratorState.ZERO;
            } else {
                state = IteratorState.POSITIVE;
            }
            cursor = buckets.getIndexStart();
            bucket = new Bucket(0, 0, 0);
        }

        @Override
        public boolean hasNext() {
            switch (state) {
                case NEGATIVE:
                case ZERO:
                    return true;
                case POSITIVE:
                    return cursor != WindowedCounterArray.NULL_INDEX && cursor <= buckets.getIndexEnd();
                case NO_MORE_BUCKETS:
                    return false;
                default:
                    throw new RuntimeException("Unknown state " + state);
            }
        }

        @Override
        public Bucket next() {
            switch (state) {
                case NEGATIVE:
                    bucket.startValue = min;
                    if (max < 0) {
                        bucket.endValue = max;
                        state = IteratorState.NO_MORE_BUCKETS;
                    } else {
                        bucket.endValue = 0;
                        state = countForZero > 0 ? IteratorState.ZERO : IteratorState.POSITIVE;
                    }
                    bucket.count = countForNegatives;
                    return bucket;
                case ZERO:
                    bucket.startValue = 0;
                    bucket.endValue = 0;
                    bucket.count = countForZero;
                    state = IteratorState.POSITIVE;
                    return bucket;
                case POSITIVE:
                    do {
                        bucket.startValue = cursor == buckets.getIndexStart() && min > 0 ? min : indexToBucketStart(cursor);
                        bucket.endValue = cursor == buckets.getIndexEnd() ? max : indexToBucketEnd(cursor);
                        bucket.count = buckets.get(cursor);
                        cursor++;
                    } while (bucket.count == 0 && cursor <= buckets.getIndexEnd());
                    return bucket;
                case NO_MORE_BUCKETS:
                    throw new NoSuchElementException("SimpleNrSketch.BucketIterator: no more elements for next()");
                default:
                    throw new RuntimeException("Unknown state " + state);
            }
        }
    }

    private class NegativeBucketIterator extends PositiveBucketBucketIterator {

        public NegativeBucketIterator() {
            // Iterates from buckets.getIndexEnd() to getIndexStart() so that negative numbers with higher absolute value
            // (ie. lower logical value) are returned first.
            cursor = buckets.getIndexEnd();
        }

        @Override
        public boolean hasNext() {
            switch (state) {
                case NEGATIVE:
                    return cursor != WindowedCounterArray.NULL_INDEX && cursor >= buckets.getIndexStart();
                case ZERO:
                case POSITIVE:
                    return true;
                case NO_MORE_BUCKETS:
                    return false;
                default:
                    throw new RuntimeException("Unknown state " + state);
            }
        }

        @Override
        public Bucket next() {
            switch (state) {
                case NEGATIVE:
                    do {
                        bucket.startValue = cursor == buckets.getIndexEnd() ? min : indexToBucketEnd(cursor);
                        bucket.endValue = cursor == buckets.getIndexStart() && max < 0 ? max : indexToBucketStart(cursor);
                        bucket.count = buckets.get(cursor);
                        cursor--;
                    } while (bucket.count == 0 && cursor >= buckets.getIndexStart());

                    if (cursor < buckets.getIndexStart()) {
                        state = countForZero > 0 ? IteratorState.ZERO :
                                (getCountForPositives() > 0 ? IteratorState.POSITIVE : IteratorState.NO_MORE_BUCKETS);
                    }
                    return bucket;
                case ZERO:
                    bucket.startValue = 0;
                    bucket.endValue = 0;
                    bucket.count = countForZero;
                    state = getCountForPositives() > 0 ? IteratorState.POSITIVE : IteratorState.NO_MORE_BUCKETS;
                    return bucket;
                case POSITIVE:
                    bucket.endValue = max;
                    if (min > 0) {
                        bucket.startValue = min;
                    } else {
                        bucket.startValue = 0;
                    }
                    bucket.count = getCountForPositives();
                    state = IteratorState.NO_MORE_BUCKETS;
                    return bucket;
                case NO_MORE_BUCKETS:
                    throw new NoSuchElementException("SimpleNrSketch.BucketIterator: no more elements for next()");
                default:
                    throw new RuntimeException("Unknown state " + state);
            }
        }
    }

    // Returns relative error upper bound for percentiles generated from this histogram.
    // relativeError = Math.abs(reportedValue - actualValue) / reportedValue
    //
    // When a requested percentile falls into a bucket, the actual percentile value can be anywhere within this bucket.
    // The percentile function shall return the mid point of the bucket for symmetric +/- error margin.
    // The relative error upper bound is (bucketWidth / 2) / bucketMiddle
    //
    @Override
    public double getPercentileRelativeError() {
        final double base = indexer.getBase();
        return (base - 1) / (base + 1);
    }

    public boolean isBucketHoldsPositiveNumbers() {
        return bucketHoldsPositiveNumbers;
    }

    @Override
    public long getCount() {
        return totalCount;
    }

    public long getCountForZero() {
        return countForZero;
    }

    public long getCountForNegatives() {
        return countForNegatives;
    }

    public long getCountForPositives() {
        return totalCount - countForZero - countForNegatives;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double getSum() {
        return sum;
    }

    @Override
    public int getMaxNumOfBuckets() {
        return (int) buckets.getMaxSize();
    }

    public int getBucketWindowSize() {
        return (int) buckets.getWindowSize();
    }

    public WindowedCounterArray getBuckets() {
        return buckets;
    }

    @Override
    @NotNull
    public Iterator<Bucket> iterator() {
        return bucketHoldsPositiveNumbers ? new PositiveBucketBucketIterator() : new NegativeBucketIterator();
    }
}
