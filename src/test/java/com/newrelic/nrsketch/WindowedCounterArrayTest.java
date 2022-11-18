// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class WindowedCounterArrayTest {
    @Test(expected = IllegalArgumentException.class)
    public void testInitializedWithZeroMaxBucketCount() {
        new WindowedCounterArray(0);
    }

    @Test
    public void happyPath() {
        final WindowedCounterArray array = new WindowedCounterArray(10);

        assertEquals(10, array.getMaxSize());
        assertTrue(array.isEmpty());
        assertEquals(0, array.getTotalCount());

        assertMeta(array, 0, WindowedCounterArray.NULL_INDEX, WindowedCounterArray.NULL_INDEX);

        assertTrue(array.increment(100, 1));
        assertMeta(array, 1, 100, 100);

        assertTrue(array.increment(104, 2));
        assertMeta(array, 5, 100, 104);

        assertTrue(array.increment(95, 3));
        assertMeta(array, 10, 95, 104);

        assertTrue(array.increment(104, 10));

        assertFalse(array.increment(105, 2));
        assertFalse(array.increment(94, 2));

        assertMeta(array, 10, 95, 104);

        final long[] expectedArray = new long[]{3, 0, 0, 0, 0, 1, 0, 0, 0, 12};

        for (int i = 95; i <= 104; i++) {
            assertEquals(expectedArray[i - 95], array.get(i));
        }
        assertEquals(16, array.getTotalCount());
    }

    @Test
    public void testSerialization() {
        final WindowedCounterArray array = new WindowedCounterArray(10);
        testSerialization(array, 22); // test empty array

        assertEquals("maxSize=10, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808", array.toString());

        array.increment(100, 1);
        testSerialization(array, 23);

        assertEquals("maxSize=10, indexBase=100, indexStart=100, indexEnd=100, array={1,}", array.toString());

        array.increment(100, 254);
        assertEquals(255, array.get(100)); // 1 byte counter
        testSerialization(array, 24);

        array.increment(100, 1);
        assertEquals(256, array.get(100)); // 2 byte counter
        testSerialization(array, 24);

        array.increment(101, 0x0A0B0CL); // 3 byte counter
        assertEquals(0x0A0B0CL, array.get(101));
        testSerialization(array, 27);

        array.increment(104, 0x0A0B0C0DL); // 4 byte counter. test endian.
        assertEquals(0x0A0B0C0DL, array.get(104));
        testSerialization(array, 33);

        array.increment(102, 0xFFFFFFFFL); // test FF for sign extension (should treat as unsigned)
        assertEquals(4294967295L, array.get(102));
        testSerialization(array, 37);

        array.increment(99, 0x0A0B0C0D01L); // 5 byte counter
        assertEquals(0x0A0B0C0D01L, array.get(99));
        testSerialization(array, 43);

        array.increment(95, 0x0A0B0C0D01020304L); // 8 byte counter. test endian.
        assertEquals(0x0A0B0C0D01020304L, array.get(95));
        testSerialization(array, 55);

        assertEquals("maxSize=10, indexBase=100, indexStart=95, indexEnd=104, array={723685415114113796,0,0,0,43135012097,256,658188,4294967295,0,168496141,}", array.toString());
    }

    // Write to buffer, then read back and compare.
    private void testSerialization(final WindowedCounterArray array, final int expectedBufferSize) {
        assertEquals(expectedBufferSize, WindowedCounterArraySerializer.getWindowedCounterArraySerializeBufferSize(array));
        final ByteBuffer buffer = ByteBuffer.allocate(expectedBufferSize);
        WindowedCounterArraySerializer.serializeWindowedCounterArray(array, buffer);
        assertEquals(expectedBufferSize, buffer.position());
        buffer.flip();
        assertEquals(array, WindowedCounterArraySerializer.deserializeWindowedCounterArray(buffer));
    }

    private void assertMeta(final WindowedCounterArray array, final int expectedSize, final long expectedStart, final long expectedEnd) {
        assertEquals(expectedSize, array.getWindowSize());
        assertEquals(expectedStart, array.getIndexStart());
        assertEquals(expectedEnd, array.getIndexEnd());
    }

    @Test
    public void testVarint() {
        testVarint(-1, 10);
        testVarint(-2, 10);
        testVarint(-100, 10);
        testVarint(-300, 10);

        testVarint(0, 1);
        testVarint(100, 1);
        testVarint(300, 2);

        testVarint(Integer.MAX_VALUE, 5);

        testVarint(Long.MAX_VALUE, 9);
        testVarint(Long.MIN_VALUE, 10);

        for (int i = 0; i <= 63; i++) {
            final long value = 1L << i;
            testVarint(value, expectedVarint64EncodedLength(value));
        }

        for (long value = 0; value < 1000_000; value++) {
            testVarint(value, expectedVarint64EncodedLength(value));
        }
    }

    // Alternate method for getVarint64EncodedLength(). To do: figure out which method is faster.
    public int expectedVarint64EncodedLength(final long value) {
        final int nBits = 64 - Long.numberOfLeadingZeros(value);
        return nBits >= 8 ? (nBits + 6) / 7 : 1;
    }

    private void testVarint(final long value, final int expectedSize) {
        assertEquals(expectedSize, WindowedCounterArraySerializer.getVarint64EncodedLength(value));
        final ByteBuffer buffer = ByteBuffer.allocate(20);
        WindowedCounterArraySerializer.writeVarint64(value, buffer);
        buffer.flip();
        assertEquals(expectedSize, buffer.limit());
        assertEquals(value, WindowedCounterArraySerializer.readVarint64(buffer));
    }

    @Test
    public void testEqualAndHash() {
        final WindowedCounterArray a1 = new WindowedCounterArray(5);
        final WindowedCounterArray a2 = new WindowedCounterArray(5);
        final WindowedCounterArray a3 = new WindowedCounterArray(6);

        assertNotEquals(a1, a3);

        assertEquals(a1, a2);
        assertEquals(34596, a1.hashCode());
        assertEquals(34596, a2.hashCode());

        a1.increment(3, 1);
        assertNotEquals(a1, a2);
        assertEquals(1075453, a1.hashCode());

        a2.increment(3, 1);
        assertEquals(a1, a2);
        assertEquals(1075453, a2.hashCode());
    }

    @Test
    public void testDeepCopy() {
        final WindowedCounterArray a1 = new WindowedCounterArray(5);
        WindowedCounterArray a2 = a1.deepCopy();

        assertEquals(a1, a2);
        assertEquals("maxSize=5, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808", a1.toString());
        assertEquals("maxSize=5, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808", a2.toString());

        a1.increment(100, 20);
        assertNotEquals(a1, a2);

        a2 = a1.deepCopy();
        assertEquals(a1, a2);
        assertEquals("maxSize=5, indexBase=100, indexStart=100, indexEnd=100, array={20,}", a1.toString());
        assertEquals("maxSize=5, indexBase=100, indexStart=100, indexEnd=100, array={20,}", a2.toString());

        a1.increment(98, 1000);
        a2 = a1.deepCopy();
        assertEquals(a1, a2);
        assertEquals("maxSize=5, indexBase=100, indexStart=98, indexEnd=100, array={1000,0,20,}", a1.toString());
        assertEquals("maxSize=5, indexBase=98, indexStart=98, indexEnd=100, array={1000,0,20,}", a2.toString());
    }
}
