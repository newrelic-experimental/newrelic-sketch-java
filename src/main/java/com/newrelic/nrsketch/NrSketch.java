// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import java.util.Arrays;
import java.util.Iterator;

public interface NrSketch extends Iterable<NrSketch.Bucket> {

    // Insert a single value. ie. increment bucket count by 1.
    default void insert(final double d) {
        insert(d, 1);
    }

    // Insert a value with arbitrary count. The bucket count is incremented by "instances".
    void insert(final double d, final long instances);

    // Merge two histograms. Merge result goes into "this". Always returns "this".
    // An implementation should not modify "other".
    NrSketch merge(final NrSketch other);

    // While merge() implements addition, subtract() implements subtraction.
    // Subtraction is useful to produce a delta histogram from accumulative histograms.
    // This function subtracts "other" from "this". Always returns "this".
    //
    // Implementations should not modify "other".
    //
    // In cases where the source data was not recorded directly into an NrSketch instance,
    // there will likely be some kind of translation logic involving floating point error
    // when interpolating source buckets into NrSketch buckets that needs to be accounted for.
    // That accounting may end up with valid cumulative source data getting translated into
    // NrSketch buckets where some buckets appear to decrease by one. This happens because the
    // place where the extra bucket count lands during translation may end up in a different
    // bucket in a subsequent translation.
    //
    // Implementations should allow for this, and "borrow" the count from a neighboring bucket
    // to keep the totalCount == sum(bucketCounts).
    NrSketch subtract(final NrSketch other);

    // Returns a deep copy of the sketch.
    NrSketch deepCopy();

    // Get maximal number of buckets. An implementation should not use more than this number of buckets.
    int getMaxNumOfBuckets();

    // Returns total count across all buckets
    long getCount();

    // Returns min of inserted values. Returns NaN if histogram is empty.
    double getMin();

    // Returns max of inserted values. Returns NaN if histogram is empty.
    double getMax();

    // Returns sum of inserted values. Returns 0 if histogram is empty.
    double getSum();

    // Bucket class. The interface provides an iterator on histogram buckets.
    // - An implementation may reuse Bucket objects when returning from iterator next() call.
    //      Caller must evaluate the bucket before calling next() again.
    // - An implementation must return buckets sorted by startValue from low to high.
    // - Buckets shall not overlap.
    // - Gap between endValue and next bucket's startValue allowed.
    // - Single value bucket (startValue == endValue) allowed.
    // - Empty bucket (count == 0) allowed
    class Bucket {
        public double startValue;
        public double endValue;
        public long count;

        public Bucket(final double startValue, final double endValue, final long count) {
            this.startValue = startValue;
            this.endValue = endValue;
            this.count = count;
        }

        public Bucket deepCopy() {
            return new Bucket(startValue, endValue, count);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof Bucket)) {
                return false;
            }
            final Bucket other = (Bucket) obj;
            return startValue == other.startValue && endValue == other.endValue && count == other.count;
        }

        @Override
        public int hashCode() {
            int result = Double.hashCode(startValue);
            result = 31 * result + Double.hashCode(endValue);
            result = 31 * result + Long.hashCode(count);
            return result;
        }

        @Override
        public String toString() {
            return "{start=" + startValue + ", end=" + endValue + ", count=" + count + "}";
        }
    }

    // Returns the worst case relative error when computing percentiles from the histogram.
    // relativeError = Math.abs(reportedValue - actualValue) / Math.abs(reportedValue)
    double getPercentileRelativeError();

    // Default function to compute percentiles from histogram buckets.
    //
    // Input is an array of percentile thresholds. Each threshold is in the range of [0, 100]
    // Calling this function with an array of thresholds is more efficient than calling it multiple times,
    // one threshold at a time, because the function uses a single iteration on buckets to compute all thresholds.
    //
    // NOTE: This function sorts the input thresholds array. This will modify the array if it is not already sorted.
    //
    // Returns an array of percentile values, matching thresholds in the sorted thresholds array.
    // When histogram is empty, all elements in returned array will be NaN.
    //
    default double[] getPercentiles(final double[] thresholds) {
        return getPercentiles(this, thresholds);
    }

    static double[] getPercentiles(final NrSketch histogram, final double[] thresholds) {
        Arrays.sort(thresholds);

        final double[] output = new double[thresholds.length];
        final long totalCount = histogram.getCount();
        long accumulativeCount = 0;
        int index = 0; // Index on thresholds array.

        final Iterator<Bucket> iterator = histogram.iterator();

        // First, fill in zero and negative thresholds.
        while (index < thresholds.length && thresholds[index] <= 0) {
            output[index] = histogram.getMin();
            index++;
        }

        // Iterate through buckets
        while (iterator.hasNext() && index < thresholds.length) {
            final Bucket bucket = iterator.next();
            accumulativeCount += bucket.count;
            final double bucketEndPercentile = (double) accumulativeCount / totalCount * 100;
            final double bucketMidValue = (bucket.startValue + bucket.endValue) / 2;

            while (index < thresholds.length && bucketEndPercentile >= thresholds[index]) {
                // Return bucket mid value for symmetric +/- error range.
                output[index] = thresholds[index] < 100 ? bucketMidValue : histogram.getMax();
                index++;
            }
        }

        if (!iterator.hasNext()) {
            // Reached end of histogram. Check if totalCount matches accumulativeCount.
            if (accumulativeCount != totalCount) {
                throw new IllegalArgumentException("accumulativeCount " + accumulativeCount + " does not match totalCount " + totalCount);
            }
        }

        // Fill remaining entries with max. If histogram is not empty, these will be thresholds above 100%.
        // If histogram is empty, these will be all entries above 0%, filled with NaN.
        while (index < thresholds.length) {
            output[index] = histogram.getMax();
            index++;
        }

        return output;
    }
}

