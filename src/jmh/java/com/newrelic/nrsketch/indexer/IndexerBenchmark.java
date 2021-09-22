// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

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

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)

public class IndexerBenchmark {
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

    public enum IndexerType {
        LOG_FUNCTION,
        SUBBUCKET_LOG,
        SUBBUCKET_LOOKUP,
        EXPONENT,
    }

    @Param({"LOG_FUNCTION", "SUBBUCKET_LOG", "SUBBUCKET_LOOKUP", "EXPONENT"})
    IndexerType _indexerType; // Starts with "_" to make it the 1st param in alphabetic param name sort order.

    ScaledIndexer indexer;

    @Setup
    public void setup() {
        switch (_indexerType) {
            case LOG_FUNCTION:
                indexer = new LogIndexer(4);
                break;
            case SUBBUCKET_LOG:
                indexer = new SubBucketLogIndexer(4);
                break;
            case SUBBUCKET_LOOKUP:
                indexer = new SubBucketLookupIndexer(4);
                break;
            case EXPONENT:
                indexer = new ExponentIndexer(-1);
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
    public long insert() {
        final long index = indexer.getBucketIndex(values[valueIndex]);
        if (++valueIndex >= values.length) {
            valueIndex = 0;
        }
        return index;
    }

    volatile boolean terminate = false;

    public static void main(final String[] args) throws Exception {
        final IndexerBenchmark benchmark = new IndexerBenchmark();

        benchmark.valueArrayLength = 1000_000;
        benchmark.dataType = DataType.POSITIVE_AND_ZERO;
        benchmark._indexerType = IndexerType.LOG_FUNCTION;

        final int threads = 1;
        int seconds = 5;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-data":
                    benchmark.dataType = DataType.valueOf(args[++i]);
                    break;
                case "-indexer":
                    benchmark._indexerType = IndexerType.valueOf(args[++i]);
                    break;
                case "-seconds":
                    seconds = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown option " + args[i]);
            }
        }

        benchmark.setup();

        System.out.println("indexer=" + benchmark.indexer.getClass().getName());

        final ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        final AtomicLong totalCount = new AtomicLong();

        final long startTime = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            threadPool.execute(() -> {
                long count = 0;
                long indexSum = 0;
                while (!benchmark.terminate) {
                    // Compute a sum to avoid JVM optimizing the insert() away because it has no side effect.
                    indexSum += benchmark.insert();
                    count++;
                }
                totalCount.addAndGet(count);
                System.out.println("count = " + count + " indexSum = " + indexSum);
            });
        }

        Thread.sleep(seconds * 1000);
        benchmark.terminate = true;
        threadPool.shutdown();

        final long elapsedTime = System.currentTimeMillis() - startTime;
        final double nsPerInsert = (double) elapsedTime / totalCount.get() * 1000_000;
        System.out.println("elapsedTime=" + elapsedTime
                + "  nsPerInsert=" + nsPerInsert);
    }
}
