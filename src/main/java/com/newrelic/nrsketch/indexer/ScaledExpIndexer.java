// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

// Base class for scaled base2 exponential histogram
public abstract class ScaledExpIndexer implements BucketIndexer {
    protected int scale;

    public ScaledExpIndexer(final int scale) {
        this.scale = scale;
    }

    // base = 2 ^ (2 ^ -scale)
    public static double getBase(final int scale) {
        return Math.pow(2, Math.pow(2, -scale));
    }

    public static long getMaxIndex(final int scale) {
        // Scale > 0: max exponent followed by max subbucket index.
        // Scale <= 0: max exponent with -scale bits truncated.
        return scale > 0 ? (((long) Double.MAX_EXPONENT << scale) | ((1L << scale) - 1))
                : ((long) Double.MAX_EXPONENT >>> -scale);
    }

    // For numbers down to Double.MIN_NORMAL
    public static long getMinIndexNormal(final int scale) {
        // Scale > 0: min exponent followed by min subbucket index, which is 0.
        // Scale <= 0: min exponent with -scale bits truncated.
        return scale > 0 ? (((long) Double.MIN_EXPONENT << scale))
                : ((long) Double.MIN_EXPONENT >> -scale); // Use ">>" to preserve sign of exponent.
    }

    public int getScale() {
        return scale;
    }

    public double getBase() {
        return getBase(scale);
    }

    public long getMaxIndex() {
        return getMaxIndex(scale);
    }

    public long getMinIndexNormal() {
        return getMinIndexNormal(scale);
    }

    @Override
    public double getBucketEnd(final long index) {
        return index == getMaxIndex(scale) ? Double.MAX_VALUE : getBucketStart(index + 1);
    }
}
