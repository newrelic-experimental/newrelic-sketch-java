// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.newrelic.nrsketch.SimpleNrSketch.DEFAULT_INIT_SCALE;
import static com.newrelic.nrsketch.SimpleNrSketch.DEFAULT_MAX_BUCKETS;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)

public class InsertBenchmark {
    @Param("1000000")
    int valueArrayLength;

    double[] values;
    int valueIndex;

    public enum DataType {
        POSITIVE_AND_ZERO,
        NEGATIVE,
        COMBO
    }

    @Param({"POSITIVE_AND_ZERO"})
    DataType dataType;

    public enum SketchType {
        SIMPLE,
        SIMPLE_LOG,
        SIMPLE_LOOKUP,
        SIMPLE_SUB_BUCKET_LOG,
        COMBO,
        CONCURRENT_COMBO
    }

    @Param({"SIMPLE", "SIMPLE_LOG", "SIMPLE_LOOKUP", "SIMPLE_SUB_BUCKET_LOG", "COMBO", "CONCURRENT_COMBO"})
    SketchType _sketchType; // Starts with "_" to make it the 1st param in alphabetic param name sort order.

    NrSketch sketch;

    @Setup
    public void setup() {
        switch (_sketchType) {
            case SIMPLE:
                sketch = new SimpleNrSketch();
                break;
            case SIMPLE_LOG:
                sketch = new SimpleNrSketch(DEFAULT_MAX_BUCKETS, DEFAULT_INIT_SCALE, true,
                        IndexerOption.LOG_INDEXER);
                break;
            case SIMPLE_LOOKUP:
                sketch = new SimpleNrSketch(DEFAULT_MAX_BUCKETS, DEFAULT_INIT_SCALE, true,
                        IndexerOption.SUB_BUCKET_LOOKUP_INDEXER);
                break;
            case SIMPLE_SUB_BUCKET_LOG:
                sketch = new SimpleNrSketch(DEFAULT_MAX_BUCKETS, DEFAULT_INIT_SCALE, true,
                        IndexerOption.SUB_BUCKET_LOG_INDEXER);
                break;
            case COMBO:
                sketch = new ComboNrSketch();
                break;
            case CONCURRENT_COMBO:
                sketch = new ConcurrentNrSketch(new ComboNrSketch());
                break;
        }

        values = new double[valueArrayLength];

        final int startValue;

        switch (dataType) {
            case POSITIVE_AND_ZERO:
                startValue = 0;
                break;
            case NEGATIVE:
                startValue = -values.length;
                break;
            case COMBO:
                startValue = -values.length / 2;
                break;
            default:
                startValue = 0;
        }

        for (int i = 0; i < values.length; i++) {
            values[i] = ThreadLocalRandom.current().nextInt(startValue, startValue + values.length);
        }
    }

    @Benchmark
    public Object insert() {
        sketch.insert(values[valueIndex]);
        if (++valueIndex >= values.length) {
            valueIndex = 0;
        }
        return sketch;
    }

    volatile boolean terminate = false;

    // Multi thread testing.
    public static void main(final String[] args) throws Exception {
        final InsertBenchmark benchmark = new InsertBenchmark();

        benchmark.valueArrayLength = 1000_000;
        benchmark.dataType = DataType.POSITIVE_AND_ZERO;
        benchmark._sketchType = SketchType.SIMPLE;

        int threads = 1;
        int seconds = 5;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-data":
                    benchmark.dataType = DataType.valueOf(args[++i]);
                    break;
                case "-sketch":
                    benchmark._sketchType = SketchType.valueOf(args[++i]);
                    break;
                case "-seconds":
                    seconds = Integer.parseInt(args[++i]);
                    break;
                case "-threads":
                    threads = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option " + args[i]);
            }
        }

        benchmark.setup();

        System.out.println("sketch=" + benchmark.sketch.getClass().getName());

        final ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        final AtomicLong totalCount = new AtomicLong();

        final long startTime = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            threadPool.execute(() -> {
                int valueIndex = 0;
                long count = 0;
                while (!benchmark.terminate) {
                    benchmark.sketch.insert(benchmark.values[valueIndex]);
                    count++;
                    if (++valueIndex >= benchmark.values.length) {
                        valueIndex = 0;
                    }
                }
                totalCount.addAndGet(count);
                System.out.println("Thread insert count = " + count);
            });
        }

        Thread.sleep(seconds * 1000);
        benchmark.terminate = true;
        threadPool.shutdown();

        final long elapsedTime = System.currentTimeMillis() - startTime;
        final double nsPerInsert = (double) elapsedTime / totalCount.get() * 1000_000;
        System.out.println("elapsedTime=" + elapsedTime
                + "  nsPerInsert=" + nsPerInsert);

        if (totalCount.get() != benchmark.sketch.getCount()) {
            throw new RuntimeException("client side count = " + totalCount.get()
                    + ", server side count = " + benchmark.sketch.getCount());
        }

        System.out.println("count=" + benchmark.sketch.getCount()
                + " min=" + benchmark.sketch.getMin()
                + " max=" + benchmark.sketch.getMax());

        //System.out.println(benchmark.sketch.toString());

        final double[] thresholds = new double[]{0, 25, 50, 90, 100};
        final double[] percentiles = benchmark.sketch.getPercentiles(thresholds);

        for (int i = 0; i < thresholds.length; i++) {
            System.out.print(thresholds[i] + "%=" + percentiles[i] + " ");
        }
        System.out.println();
    }
}
