// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

// Uses a SimpleNrSketch for positive numbers and zero, and another one for negative numbers,
// for high resolution distribution on both negative and positive numbers.

public class ComboNrSketch implements NrSketch {
    private final int maxNumBucketsPerHistogram;
    private final int initialScale;

    // Holds negative and/or positive histograms. When both are present, negative one always precedes positive one.
    private final List<NrSketch> histograms;

    @Nullable
    private NrSketch positiveHistogram; // For positive numbers and zero
    @Nullable
    private NrSketch negativeHistogram; // For negative numbers

    public ComboNrSketch() {
        this(SimpleNrSketch.DEFAULT_MAX_BUCKETS);
    }

    public ComboNrSketch(final int maxNumBucketsPerHistogram) {
        this(maxNumBucketsPerHistogram, SimpleNrSketch.DEFAULT_INIT_SCALE);
    }

    public ComboNrSketch(final int maxNumBucketsPerHistogram, final int initialScale) {
        this.maxNumBucketsPerHistogram = maxNumBucketsPerHistogram;
        this.initialScale = initialScale;
        histograms = new ArrayList<>(2);
    }

    // For deserialization only
    public ComboNrSketch(final int maxNumBucketsPerHistogram, final int initialScale, final List<NrSketch> histograms) {
        this.maxNumBucketsPerHistogram = maxNumBucketsPerHistogram;
        this.initialScale = initialScale;
        this.histograms = histograms;

        switch (histograms.size()) {
            case 0:
                break;
            case 1:
                final NrSketch subSketch = histograms.get(0);
                if (subSketch instanceof SimpleNrSketch) {
                    if (((SimpleNrSketch) subSketch).isBucketHoldsPositiveNumbers()) {
                        positiveHistogram = subSketch;
                    } else {
                        negativeHistogram = subSketch;
                    }
                } else {
                    throw new IllegalArgumentException("Unknown subsketch class " + subSketch.getClass().getName());
                };
                break;
            case 2:
                negativeHistogram = histograms.get(0);
                positiveHistogram = histograms.get(1);
                break;
            default:
                throw new IllegalStateException("Unexpected histograms list size " + histograms.size());
        }
    }

    private void setPositiveHistogram(final NrSketch histogram) {
        if (positiveHistogram != null) {
            throw new IllegalArgumentException("Attempting to set positiveHistogram twice");
        }
        positiveHistogram = histogram;
        histograms.add(histogram); // Always append
    }

    private NrSketch getOrCreatePositveHistogram() {
        if (positiveHistogram == null) {
            setPositiveHistogram(new SimpleNrSketch(maxNumBucketsPerHistogram, initialScale));
        }
        return positiveHistogram;
    }

    private void setNegativeHistogram(final NrSketch histogram) {
        if (negativeHistogram != null) {
            throw new IllegalArgumentException("Attempting to set negativeHistogram twice");
        }
        negativeHistogram = histogram;
        histograms.add(0, histogram); // Always prepend
    }

    private NrSketch getOrCreateNegativeHistogram() {
        if (negativeHistogram == null) {
            setNegativeHistogram(SimpleNrSketch.newNegativeHistogram(maxNumBucketsPerHistogram, initialScale));
        }
        return negativeHistogram;
    }

    @Override
    public void insert(final double d, final long instances) {
        if (d >= 0) {
            getOrCreatePositveHistogram().insert(d, instances);
        } else {
            getOrCreateNegativeHistogram().insert(d, instances);
        }
    }

    @Override
    public NrSketch merge(final NrSketch otherInterface) {
        if (!(otherInterface instanceof ComboNrSketch)) {
            throw new IllegalArgumentException("ComboNrSketch cannot merge with " + otherInterface.getClass().getName());
        }
        final ComboNrSketch other = (ComboNrSketch) otherInterface;

        if (other.negativeHistogram != null) {
            getOrCreateNegativeHistogram().merge(other.negativeHistogram);
        }

        if (other.positiveHistogram != null) {
            getOrCreatePositveHistogram().merge(other.positiveHistogram);
        }
        return this;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ComboNrSketch)) {
            return false;
        }
        final ComboNrSketch other = (ComboNrSketch) obj;
        if (maxNumBucketsPerHistogram != other.maxNumBucketsPerHistogram
                || histograms.size() != other.histograms.size()) {
            return false;
        }
        for (int i = 0; i < histograms.size(); i++) {
            if (!histograms.get(i).equals(other.histograms.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() { // Defined just to keep findbugs happy.
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("maxNumBucketsPerHistogram=" + maxNumBucketsPerHistogram);
        builder.append(", histograms.size()=" + histograms.size());
        for (final NrSketch h : histograms) {
            builder.append("\n" + h.toString());
        }
        return builder.toString();
    }

    private <R> R mergeField(final R nullValue, final Function<NrSketch, R> extractor, final BiFunction<R, R, R> merger) {
        switch (histograms.size()) {
            case 0:
                return nullValue;
            case 1:
                return extractor.apply(histograms.get(0));
            case 2:
                return merger.apply(extractor.apply(histograms.get(0)), extractor.apply(histograms.get(1)));
            default:
                throw new IllegalStateException("Unexpected histograms list size " + histograms.size());
        }
    }

    public static double maxWithNan(final double a, final double b) {
        return Double.isNaN(a) ? b : (Double.isNaN(b) ? a : Math.max(a, b));
    }

    public static double minWithNan(final double a, final double b) {
        return Double.isNaN(a) ? b : (Double.isNaN(b) ? a : Math.min(a, b));
    }

    @Override
    public int getMaxNumOfBuckets() {
        return mergeField(0, NrSketch::getMaxNumOfBuckets, Integer::sum);
    }

    public int getMaxNumBucketsPerHistogram() {
        return maxNumBucketsPerHistogram;
    }

    public int getInitialScale() {
        return initialScale;
    }

    @Override
    public double getPercentileRelativeError() {
        return mergeField(0D, NrSketch::getPercentileRelativeError, Math::max);
    }

    @Override
    public long getCount() {
        return mergeField(0L, NrSketch::getCount, Long::sum);
    }

    @Override
    public long getCountForNegatives() {
        return mergeField(0L, NrSketch::getCountForNegatives, Long::sum);
    }

    @Override
    public double getMin() {
        return mergeField(Double.NaN, NrSketch::getMin, ComboNrSketch::minWithNan);
    }

    @Override
    public double getMax() {
        return mergeField(Double.NaN, NrSketch::getMax, ComboNrSketch::maxWithNan);
    }

    @Override
    public double getSum() {
        return mergeField(0D, NrSketch::getSum, Double::sum);
    }

    // We could just use com.google.common.collect.Iterators.concat. But to avoid dependency on Guava,
    // we are reinventing the wheel here.
    protected class ComboIterator implements Iterator<Bucket> {
        private final List<Iterator<Bucket>> iterators;
        int cursor = 0;

        protected ComboIterator() {
            iterators = new ArrayList<>(histograms.size());
            for (NrSketch histogram : histograms) {
                iterators.add(histogram.iterator());
            }
        }

        @Override
        public boolean hasNext() {
            if (cursor >= iterators.size()) {
                return false;
            }
            final boolean hasMore = iterators.get(cursor).hasNext();
            if (!hasMore) {
                cursor++;
                return hasNext(); // Recursion. This class is only good for a small number of iterators.
            }
            return hasMore;
        }

        @Override
        public Bucket next() {
            return iterators.get(cursor).next();
        }
    }

    @NotNull
    @Override
    public Iterator<Bucket> iterator() {
        return new ComboIterator();
    }

    // List size can be
    //      0: empty sketch
    //      1: negative histogram only, or positive histogram only
    //      2: negative (list[0]) and positiive (list[1]) histograms
    public List<NrSketch> getHistograms() {
        return histograms;
    }
}
