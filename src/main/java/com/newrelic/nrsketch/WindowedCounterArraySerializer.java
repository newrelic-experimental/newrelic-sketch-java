// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import java.nio.ByteBuffer;

import static com.newrelic.nrsketch.WindowedCounterArray.NULL_INDEX;

// This serializer is a logical writer that only writes elements in the window, and it shifts indexBase to indexStart.
// The deserialized object won't be physically identical to the original, but is logically equivalent.
//
// The counter array is written as varint.
//
// This serializer uses the default Java byte order of big endian.

public class WindowedCounterArraySerializer {
    private static final byte WINDOWED_ARRAY_VERSION = 1; // This is a relatively simple data structure. Use one byte for version.

    public static void serializeWindowedCounterArray(final WindowedCounterArray array, final ByteBuffer buffer) {
        buffer.put(WINDOWED_ARRAY_VERSION);
        buffer.putInt(array.getMaxSize());

        // indexBase is not serialized
        buffer.putLong(array.getIndexStart());
        buffer.putLong(array.getIndexEnd());

        buffer.put(array.getBytesPerCounter());

        if (!array.isEmpty()) {
            for (long index = array.getIndexStart(); index <= array.getIndexEnd(); index++) {
                writeVarint64(array.get(index), buffer);
            }
        }
    }

    public static int getWindowedCounterArraySerializeBufferSize(final WindowedCounterArray array) {
        int size = Byte.BYTES     // version
                + Integer.BYTES   // max size;
                + Long.BYTES * 2  // index start and end
                + Byte.BYTES;     // bytesPerCounter

        if (!array.isEmpty()) {
            for (long index = array.getIndexStart(); index <= array.getIndexEnd(); index++) {
                size += getVarint64EncodedLength(array.get(index));
            }
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
                array.increment(index, readVarint64(buffer));
            }
        }
        return array;
    }

    public static void writeVarint64(long value, final ByteBuffer buffer) {
        while ((value & -128L) != 0L) {
            buffer.put((byte) (value | 128L));
            value >>>= 7;
        }
        buffer.put((byte) value);
    }

    public static int getVarint64EncodedLength(long value) {
        int length = 0;
        while ((value & -128L) != 0L) {
            length++;
            value >>>= 7;
        }
        length++;
        return length;
    }

    public static long readVarint64(final ByteBuffer buffer) {
        int shift = 0;
        long result = 0L;

        while (true) {
            final byte b1 = buffer.get();
            result |= (long) (b1 & 127) << shift;
            if ((b1 & 128) != 128) {
                break;
            }
            shift += 7;
            if (shift >= 64) {
                throw new RuntimeException("Exceeded max length of varint64");
            }
        }
        return result;
    }
}
