// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import java.nio.ByteBuffer;

import static com.newrelic.nrsketch.WindowedCounterArray.NULL_INDEX;

// This serializer is a logical writer that only writes elements in the window, and it shifts indexBase to indexStart.
// The deserialized object won't be identical to the original, but is logically equivalent.
//
// For a given array, this serializer writes each counter with the same "bytes per counter" from MultiTypeCounterArray.
// Alternatively, it might write in varint format (element size may vary within one array), but that adds complexity and cpu cost.
//
// This serializer uses the default Java byte order of big endian.

public class WindowedCounterArraySerializer {
    private static final byte WINDOWED_ARRAY_VERSION = 1; // This is a relatively simple data structure. Use one byte for version.

    public static ByteBuffer serializeWindowedCounterArray(final WindowedCounterArray array, final ByteBuffer buffer) {
        buffer.put(WINDOWED_ARRAY_VERSION);
        buffer.putInt(array.getMaxSize());

        buffer.putLong(array.getIndexStart());
        buffer.putLong(array.getIndexEnd());

        final byte bytesPerCounter = array.getBytesPerCounter();
        buffer.put(bytesPerCounter);

        if (array.getIndexStart() != NULL_INDEX) {
            for (long index = array.getIndexStart(); index <= array.getIndexEnd(); index++) {
                putLong(buffer, array.get(index), bytesPerCounter);
            }
        }
        return buffer;
    }

    public static int getWindowedCounterArraySerializeBufferSize(final WindowedCounterArray array) {
        int size = Byte.BYTES // version
                + Integer.BYTES // max size;
                + Long.BYTES * 2  // index start and end
                + Byte.BYTES; // bytesPerCounter

        if (array.getIndexStart() != NULL_INDEX) {
            size += array.getWindowSize() * array.getBytesPerCounter();
        }
        return size;
    }

    public static WindowedCounterArray deserializeWindowedCounterArray(final ByteBuffer buffer) {
        final byte format = buffer.get();
        if (format != WINDOWED_ARRAY_VERSION) {
            throw new IllegalArgumentException("WindowedCounterArray serialization format: expected " + WINDOWED_ARRAY_VERSION + ", actual " + format);
        }

        final int maxSize = buffer.getInt();
        final long indexStart = buffer.getLong();
        final long indexEnd = buffer.getLong();

        final byte bytesPerCounter = buffer.get();

        final WindowedCounterArray array = new WindowedCounterArray(maxSize, bytesPerCounter);

        if (indexStart != NULL_INDEX) {
            for (long index = indexStart; index <= indexEnd; index++) {
                array.increment(index, getLong(buffer, bytesPerCounter));
            }
        }
        return array;
    }

    private static void putLong(final ByteBuffer buffer, long value, int bytesPerCounter) {
        switch (bytesPerCounter) {
            case Byte.BYTES:
                buffer.put((byte) value);
                break;
            case Short.BYTES:
                buffer.putShort((short) value);
                break;
            case Integer.BYTES:
                buffer.putInt((int) value);
                break;
            case Long.BYTES:
                buffer.putLong(value);
                break;
            default:
                throw new RuntimeException("Unknown bytesPerCounter " + bytesPerCounter);
        }
    }

    private static long getLong(final ByteBuffer buffer, final int bytesPerCounter) {
        switch (bytesPerCounter) {
            case Byte.BYTES:
                return buffer.get();
            case Short.BYTES:
                return buffer.getShort();
            case Integer.BYTES:
                return buffer.getInt();
            case Long.BYTES:
                return buffer.getLong();
            default:
                throw new RuntimeException("Unknown bytesPerCounter " + bytesPerCounter);
        }
    }
}
