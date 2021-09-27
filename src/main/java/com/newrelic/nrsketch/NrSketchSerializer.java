// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledIndexer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

// Not implemented as member functions of the classes so that multiple "third party" serializers and deserializers
// can be written and they are all created equal (no one has more privileged access to the classes)
//
// This serializer uses a 2 pass method. The 1st pass computes buffer size. The 2nd pass writes to the buffer.
// This allows it to allocate a buffer of the exact size.
// An alternate method could use a dynamic buffer that grows as we write to it. The alternate method avoids the 1st pass,
// but may end up allocating 2x space in the worst case, and has to copy data over each time the buffer grows.
//
// This serializer uses the default Java byte order of big endian.
// The elements in the counter array are written as varint.

public class NrSketchSerializer {
    // Each histogram object has a 2 byte version number at the beginning.
    // The versions can distinguish among SimpleNrSketch, ComboNrSketch, and ConcurrentNrSketch.
    // Given a serialized blob, the deserializer can restore the original object type.
    //
    // Versions 0 to 0x1FF are reserved for New Relic internal formats.
    private static final short SIMPLE_NRSKETCH_VERSION = 0x200;     // SimpleNrSketch:      0x200 to 0x2FF
    private static final short COMBO_NRSKETCH_VERSION = 0x300;      // ComboNrSketch:       0x300 to 0x3FF
    private static final short CONCURRENT_NRSKETCH_VERSION = 0x400; // ConcurrentNrSketch:  0x400 to 0x4FF

    private static final int SUMMARY_SIZE = Long.BYTES + Double.BYTES * 3; // Summary fields: count, sum, min, max

    public static ByteBuffer serializeNrSketch(final NrSketch sketch) {
        final ByteBuffer buffer = ByteBuffer.allocate(getNrSketchSerializeBufferSize(sketch));
        serializeNrSketch(sketch, buffer);
        buffer.flip(); // Flip position to 0 to be reader ready.
        return buffer;
    }

    public static void serializeNrSketch(final NrSketch sketch, final ByteBuffer buffer) {
        if (sketch instanceof SimpleNrSketch) {
            serializeSimpleNrSketch((SimpleNrSketch) sketch, buffer);
        } else if (sketch instanceof ComboNrSketch) {
            serializeComboNrSketch((ComboNrSketch) sketch, buffer);
        } else if (sketch instanceof ConcurrentNrSketch) {
            serializeConcurrentNrSketch((ConcurrentNrSketch) sketch, buffer);
        } else {
            throw new IllegalArgumentException("Unknown NrSketch class " + sketch.getClass().getName());
        }
    }

    public static int getNrSketchSerializeBufferSize(final NrSketch sketch) {
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

    public static void serializeSimpleNrSketch(final SimpleNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(SIMPLE_NRSKETCH_VERSION);

        // Write out summary fields first, so that we can easily deserialize summary only
        buffer.putLong(sketch.getCount());
        buffer.putDouble(sketch.getSum());
        buffer.putDouble(sketch.getMin());
        buffer.putDouble(sketch.getMax());

        buffer.put((byte) (sketch.isBucketHoldsPositiveNumbers() ? 1 : 0));

        final int scale = sketch.getScale();
        if (scale > Byte.MAX_VALUE || scale < Byte.MIN_VALUE) {
            // Today scaled exponential indexer scale is limited to [-11, 52], which fits in one byte.
            // But to treat the indexer as a black box, the Sketch classes shall not assume scale's range.
            // As a compromise, we do a check here to future-proof serializer logic.
            throw new RuntimeException("Scale " + scale + " cannot be written with 1 byte");
        }
        buffer.put((byte) scale);

        buffer.put(getIndexerMakerCode(sketch.getIndexerMaker()));

        buffer.putLong(sketch.getCountForNegatives());
        buffer.putLong(sketch.getCountForZero());

        WindowedCounterArraySerializer.serializeWindowedCounterArray(sketch.getBuckets(), buffer);
    }

    public static int getSimpleNrSketchSerializeBufferSize(final SimpleNrSketch sketch) {
        return Short.BYTES // Version
                + SUMMARY_SIZE // Summary fields: count, sum, min, max
                + Byte.BYTES // bucketHoldsPositiveNumbers
                + Byte.BYTES // scale
                + Byte.BYTES // indexer maker code
                + Long.BYTES * 2 // countForNegatives, countForZero
                + WindowedCounterArraySerializer.getWindowedCounterArraySerializeBufferSize(sketch.getBuckets());
    }

    public static SimpleNrSketch deserializeSimpleNrSketch(final ByteBuffer buffer) {
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
        final Function<Integer, ScaledIndexer> indexerMaker = getIndexerMakerFromCode(buffer.get());

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

    static byte getIndexerMakerCode(Function<Integer, ScaledIndexer> indexMaker) {
        // Using enum ordinal number is error prone. Somebody changing the order may not be aware that the order is
        // used in serialization format. Thus we hardcode the codes here.
        if (indexMaker == IndexerOption.LOG_INDEXER) {
            return 0;
        } else if (indexMaker == IndexerOption.SUB_BUCKET_LOG_INDEXER) {
            return 1;
        } else if (indexMaker == IndexerOption.SUB_BUCKET_LOOKUP_INDEXER) {
            return 2;
        } else if (indexMaker == IndexerOption.AUTO_SELECT) {
            return 3;
        } else {
            throw new IllegalArgumentException("Unknown indexer maker " + indexMaker);
        }
    }

    static Function<Integer, ScaledIndexer> getIndexerMakerFromCode(final byte code) {
        switch (code) {
            case 0:
                return IndexerOption.LOG_INDEXER;
            case 1:
                return IndexerOption.SUB_BUCKET_LOG_INDEXER;
            case 2:
                return IndexerOption.SUB_BUCKET_LOOKUP_INDEXER;
            case 3:
                return IndexerOption.AUTO_SELECT;
            default:
                throw new IllegalArgumentException("Unknown indexer maker code " + code);
        }
    }

    public static void serializeComboNrSketch(final ComboNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(COMBO_NRSKETCH_VERSION);
        buffer.putInt(sketch.getMaxNumBucketsPerHistogram());
        buffer.put((byte) sketch.getInitialScale()); // 1 byte is enough for any scale
        buffer.put(getIndexerMakerCode(sketch.getIndexerMaker()));
        buffer.put((byte) sketch.getHistograms().size());

        // When there is only 1 histogram, we can load summary directly from it. So no need for a separate summary section.
        // For multiple histograms, write a summary here for quick reading.
        if (sketch.getHistograms().size() > 1) {
            buffer.putLong(sketch.getCount());
            buffer.putDouble(sketch.getSum());
            buffer.putDouble(sketch.getMin());
            buffer.putDouble(sketch.getMax());
        }

        for (NrSketch subSketch : sketch.getHistograms()) {
            serializeNrSketch(subSketch, buffer);
        }
    }

    public static int getComboNrSketchSerializeBufferSize(final ComboNrSketch sketch) {
        int size = Short.BYTES  // Version
                + Integer.BYTES // getMaxNumOfBuckets
                + Byte.BYTES    // getInitialScale
                + Byte.BYTES    // indexer maker code
                + Byte.BYTES;   // histogram list size

        if (sketch.getHistograms().size() > 1) {
            size += SUMMARY_SIZE;
        }

        for (NrSketch subSketch : sketch.getHistograms()) {
            size += getNrSketchSerializeBufferSize(subSketch);
        }
        return size;
    }

    public static ComboNrSketch deserializeComboNrSketch(final ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != COMBO_NRSKETCH_VERSION) {
            throw new RuntimeException("Unknown ComboNrSketch version " + version);
        }

        final int maxNumOfBuckets = buffer.getInt();
        final int initialScale = buffer.get();
        final Function<Integer, ScaledIndexer> indexerMaker = getIndexerMakerFromCode(buffer.get());
        final int histogramSize = buffer.get();

        if (histogramSize > 1) {
            buffer.position(buffer.position() + SUMMARY_SIZE); // Skip summary section
        }

        final List<NrSketch> subSketches = new ArrayList<>(histogramSize);

        for (int i = 0; i < histogramSize; i++) {
            subSketches.add(deserializeNrSketch(buffer));
        }

        return new ComboNrSketch(maxNumOfBuckets, initialScale, indexerMaker, subSketches);
    }

    public static void serializeConcurrentNrSketch(final ConcurrentNrSketch sketch, final ByteBuffer buffer) {
        buffer.putShort(CONCURRENT_NRSKETCH_VERSION);
        synchronized (sketch) {
            serializeNrSketch(sketch.getSketch(), buffer);
        }
    }

    public static int getConcurrentNrSketchSerializeBufferSize(final ConcurrentNrSketch sketch) {
        return Short.BYTES // Version
                + getNrSketchSerializeBufferSize(sketch.getSketch());
    }

    public static ConcurrentNrSketch deserializeConcurrentNrSketch(final ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != CONCURRENT_NRSKETCH_VERSION) {
            throw new RuntimeException("Unknown ConcurrentNrSketch version " + version);
        }
        return new ConcurrentNrSketch(deserializeNrSketch(buffer));
    }
}
