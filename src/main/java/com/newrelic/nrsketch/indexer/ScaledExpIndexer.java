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

    // When scale is high, base is very close to 1, in the binary form like 1.000000XXXX,
    // where there are many leading zero bits before the non-zero portion of XXXX.
    // At scale N, only about 52 - N significant bits are left, resulting in large errors as N approaches 52.
    // Any inaccuracy in base is then magnified when computing value to index or index to value mapping.
    // Therefore we should avoid using base as intermediate result.
    //
    // LIMITATION: "index" must not have more than 52 significant bits,
    //             because this function converts it to double for computation.
    public static double scaledBasePower(final int scale, final long index) {
        // result = base ^ index
        // = (2^(2^-scale))^index
        // = 2^(2^-scale * index)
        // = 2^(index * 2^-scale))
        return Math.pow(2, Math.scalb((double) index, -scale));
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
