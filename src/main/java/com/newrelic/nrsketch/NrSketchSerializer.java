// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledExpIndexer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.newrelic.nrsketch.WindowedCounterArraySerializer.getWindowedCounterArraySerializeBufferSize;
import static com.newrelic.nrsketch.WindowedCounterArraySerializer.serializeWindowedCounterArray;

// Not implemented as member functions of the classes so that multiple "third party" serializer and deserializers
// can be written and they are all created equal (no one has more privileged access to the classes)
//
// This serializer uses a 2 pass method. The 1st pass computes buffer size. The 2nd pass writes to the buffer.
// This allows it to allocate a buffer of the exact size.
// An alternate method may use a dynamic buffer that grows as we write to it. The alternate method avoids the 1st pass,
// but may end up allocating 2x space in the worst case, and has to copy data over each time the buffer grows.
//
// This serializer uses the default Java byte order of big endian.

public class NrSketchSerializer {
    // Each histogram object has a 2 byte version number at the beginning.
    // The versions can distinguish among SimpleNrSketch, ComboNrSketch, and ConcurrentNrSketch.
    // Thus given a serialized blob, the deserializer can create the correct object class from it.
    //
    // Versions 0 to 0x1FF are reserved for New Relic internal formats.
    private static final short SIMPLE_NRSKETCH_VERSION = 0x200;     // SimpleNrSketch:      0x200 to 0x2FF
    private static final short COMBO_NRSKETCH_VERSION = 0x300;      // ComboNrSketch:       0x300 to 0x3FF
    private static final short CONCURRENT_NRSKETCH_VERSION = 0x400; // ConcurrentNrSketch:  0x400 to 0x4FF

    public static ByteBuffer serializeNrSketch(final NrSketch sketch) {
        final ByteBuffer buffer = ByteBuffer.allocate(getSerializeBufferSize(sketch));
        serializeNrSketch(sketch, buffer);
        buffer.rewind(); // Rewind to beginning of buffer for the convenience of readers
        return buffer;
    }

    public static ByteBuffer serializeNrSketch(final NrSketch sketch, final ByteBuffer buffer) {
        if (sketch instanceof SimpleNrSketch) {
            return serializeSimpleNrSketch((SimpleNrSketch) sketch, buffer);
        } else if (sketch instanceof ComboNrSketch) {
            return serializeComboNrSketch((ComboNrSketch) sketch, buffer);
        } else if (sketch instanceof ConcurrentNrSketch) {
            return serializeConcurrentNrSketch((ConcurrentNrSketch) sketch, buffer);
        } else {
            throw new IllegalArgumentException("Unknown NrSketch class " + sketch.getClass().getName());
        }
    }

    public static int getSerializeBufferSize(final NrSketch sketch) {
        if (sketch instanceof SimpleNrSketch) {
            return getSimpleNrSketchSerializeBufferSize((SimpleNrSketch) sketch);
        } else if (sketch instanceof ComboNrSketch) {
            return getComboNrSketchSerializeBufferSize((ComboNrSketch) sketch);
        } else if (sketch instanceof ConcurrentNrSketch) {
            return getConcurrentNrSketchSerializeBufferSize((ConcurrentNrSketch) sketch);
        } else {
            throw new IllegalArgumentException("Unknown NrSketch class " + sketch.getClass().getName());
        }
    }

    public static NrSketch deserializeNrSketch(final ByteBuffer buffer) {
        final int savedPos = buffer.position();
        final short version = buffer.getShort();
        buffer.position(savedPos); // Restore position

        switch (version) {
            case SIMPLE_NRSKETCH_VERSION:
                return deserializeSimpleNrSketch(buffer);
            case COMBO_NRSKETCH_VERSION:
                return deserializeComboNrSketch(buffer);
            case CONCURRENT_NRSKETCH_VERSION:
                return deserializeConcurrentNrSketch(buffer);
            default:
                throw new RuntimeException("Unknown NrSketch version " + version);
        }
    }

    public static ByteBuffer serializeSimpleNrSketch(final SimpleNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(SIMPLE_NRSKETCH_VERSION);

        // Write out summary fields first, so that we can easily deserialize summary only
        buffer.putLong(sketch.getCount());
        buffer.putDouble(sketch.getSum());
        buffer.putDouble(sketch.getMin());
        buffer.putDouble(sketch.getMax());

        buffer.put((byte) (sketch.isBucketHoldsPositiveNumbers() ? 1 : 0));
        buffer.put((byte) sketch.getScale()); // 1 byte is enough for scale
        buffer.put(getIndexerMakerCode(sketch.getIndexerMaker()));

        buffer.putLong(sketch.getCountForNegatives());
        buffer.putLong(sketch.getCountForZero());

        serializeWindowedCounterArray(sketch.getBuckets(), buffer);

        return buffer;
    }

    public static int getSimpleNrSketchSerializeBufferSize(final SimpleNrSketch sketch) {
        return Short.BYTES // Version
                + Long.BYTES + Double.BYTES * 3 // Summary fields: count, sum, min, max
                + Byte.BYTES // bucketHoldsPositiveNumbers
                + Byte.BYTES // scale
                + Byte.BYTES // indexer maker code
                + Long.BYTES * 2 // countForNegatives, countForZero
                + getWindowedCounterArraySerializeBufferSize(sketch.getBuckets());
    }

    public static NrSketch deserializeSimpleNrSketch(final ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != SIMPLE_NRSKETCH_VERSION) {
            throw new RuntimeException("Unknown SimpleNrSketch version " + version);
        }

        final long count = buffer.getLong();
        final double sum = buffer.getDouble();
        final double min = buffer.getDouble();
        final double max = buffer.getDouble();

        final boolean bucketHoldsPositiveNumbers = buffer.get() == 1;
        final int scale = buffer.get();
        final Function<Integer, ScaledExpIndexer> indexerMaker = getIndexerMakeFromCode(buffer.get());

        final long countForNegatives = buffer.getLong();
        final long countForZero = buffer.getLong();

        final WindowedCounterArray buckets = WindowedCounterArraySerializer.deserializeWindowedCounterArray(buffer);

        return new SimpleNrSketch(
                buckets,
                bucketHoldsPositiveNumbers,
                scale,
                indexerMaker,
                count,
                countForNegatives,
                countForZero,
                min,
                max,
                sum);
    }

    private static byte getIndexerMakerCode(Function<Integer, ScaledExpIndexer> indexMaker) {
        if (indexMaker == (Function<Integer, ScaledExpIndexer>) IndexerOption.LOG_INDEXER::getIndexer) {
            return 0;
        } else if (indexMaker == (Function<Integer, ScaledExpIndexer>) IndexerOption.SUB_BUCKET_LOG_INDEXER::getIndexer) {
            return 0;
        } else if (indexMaker == (Function<Integer, ScaledExpIndexer>) IndexerOption.SUB_BUCKET_LOOKUP_INDEXER::getIndexer) {
            return 0;
        } else if (indexMaker == (Function<Integer, ScaledExpIndexer>) IndexerOption.AUTO_SELECT::getIndexer) {
            return 0;
        } else {
            throw new IllegalArgumentException("Unknown indexer maker " + indexMaker);
        }
    }

    private static Function<Integer, ScaledExpIndexer> getIndexerMakeFromCode(final byte code) {
        switch (code) {
            case 0:
                return IndexerOption.LOG_INDEXER::getIndexer;
            case 1:
                return IndexerOption.SUB_BUCKET_LOG_INDEXER::getIndexer;
            case 2:
                return IndexerOption.SUB_BUCKET_LOOKUP_INDEXER::getIndexer;
            case 3:
                return IndexerOption.AUTO_SELECT::getIndexer;
            default:
                throw new IllegalArgumentException("Unknown indexer maker code " + code);
        }
    }

    public static ByteBuffer serializeComboNrSketch(final ComboNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(COMBO_NRSKETCH_VERSION);
        buffer.putInt(sketch.getMaxNumOfBuckets());
        buffer.putInt(sketch.getInitialScale());
        buffer.put((byte)sketch.getHistograms().size());

        for (NrSketch subSketch : sketch.getHistograms()) {
            serializeNrSketch(subSketch, buffer);
        }
        return buffer;
    }

    public static int getComboNrSketchSerializeBufferSize(final ComboNrSketch sketch) {
        int size = Short.BYTES // Version
                + Integer.BYTES // getMaxNumOfBuckets
                + Integer.BYTES // getInitialScale
                + Byte.BYTES    // histogram list size
                + getSerializeBufferSize(sketch);

        for (NrSketch subSketch : sketch.getHistograms()) {
            size += getSerializeBufferSize(subSketch);
        }
        return size;
    }

    public static NrSketch deserializeComboNrSketch(final ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != COMBO_NRSKETCH_VERSION) {
            throw new RuntimeException("Unknown ComboNrSketch version " + version);
        }

        final int maxNumOfBuckets = buffer.getInt();
        final int initialScale = buffer.getInt();
        final int histogramSize = buffer.get();

        final List<NrSketch> subSketches = new ArrayList<>(histogramSize);

        for (int i = 0; i < histogramSize; i++) {
            subSketches.add(deserializeNrSketch(buffer));
        }

        return new ComboNrSketch(
                maxNumOfBuckets,
                initialScale,
                subSketches);
    }

    public static ByteBuffer serializeConcurrentNrSketch(final ConcurrentNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(CONCURRENT_NRSKETCH_VERSION);
        serializeNrSketch(sketch, buffer);
        return buffer;
    }

    public static int getConcurrentNrSketchSerializeBufferSize(final ConcurrentNrSketch sketch) {
        return Short.BYTES // Version
                + getSerializeBufferSize(sketch);
    }

    public static NrSketch deserializeConcurrentNrSketch(final ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != CONCURRENT_NRSKETCH_VERSION) {
            throw new RuntimeException("Unknown ConcurrentNrSketch version " + version);
        }
        return new ConcurrentNrSketch(deserializeNrSketch(buffer));
    }
}
