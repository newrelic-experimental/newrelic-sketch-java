// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

// A concurrency wrapper for NrSketch. Methods are defined as "synchronized" for multi-thread access.
// NOTES:
// 1. For bucket iteration, caller must explicitly use "synchronized" on the whole iterator block.
//    Example:
//   NrSketch sketch;
//   synchronized(sketch) {
//       final Iterator<NrSketch.Bucket> iterator = sketch.iterator();
//       while (iterator.hasNext())
//           NrSketch.Bucket bucket = iterator.next();
//   }
// 2. When calling merge(), caller must ensure that "other" is also protected from concurrent modification.
//
public class ConcurrentNrSketch implements NrSketch {
    protected final NrSketch sketch;

    public ConcurrentNrSketch(final NrSketch sketch) {
        this.sketch = sketch;
    }

    public NrSketch getSketch() {
        return sketch;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof ConcurrentNrSketch)) {
            return false;
        }
        return sketch.equals(((ConcurrentNrSketch) obj).sketch);
    }

    @Override
    public synchronized void insert(final double d, final long instances) {
        sketch.insert(d, instances);
    }

    // Caller must ensure that "other" is also protected from concurrent modification.
    @Override
    public synchronized NrSketch merge(final NrSketch other) {
        return (other instanceof ConcurrentNrSketch) ? sketch.merge(((ConcurrentNrSketch) other).sketch) : sketch.merge(other);
    }

    @Override
    public synchronized int getMaxNumOfBuckets() {
        return sketch.getMaxNumOfBuckets();
    }

    @Override
    public synchronized long getCount() {
        return sketch.getCount();
    }

    @Override
    public synchronized long getCountForNegatives() {
        return sketch.getCountForNegatives();
    }

    @Override
    public synchronized double getMin() {
        return sketch.getMin();
    }

    @Override
    public synchronized double getMax() {
        return sketch.getMax();
    }

    @Override
    public synchronized double getSum() {
        return sketch.getSum();
    }

    @Override
    public synchronized double getPercentileRelativeError() {
        return sketch.getPercentileRelativeError();
    }

    // The whole iterator block must be protected by "synchronized". See comment at beginning of this class.
    // The "iterator()" method is also defined as "synchronized" for extra safety.
    @NotNull
    @Override
    public synchronized Iterator<Bucket> iterator() {
        return sketch.iterator();
    }

    @Override
    public synchronized double[] getPercentiles(final double[] thresholds) {
        return sketch.getPercentiles(thresholds);
    }
}