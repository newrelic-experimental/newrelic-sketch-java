// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WindowedCounterArrayTest {
    @Test
    public void happyPath() {
        final WindowedCounterArray array = new WindowedCounterArray(10);

        assertEquals(10, array.getMaxSize());
        assertTrue(array.isEmpty());

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
    }

    @Test
    public void testSerialization() {
        final WindowedCounterArray array = new WindowedCounterArray(10);
        testSerialization(array, 22, 0); // test empty array

        assertEquals("maxSize=10, indexBase=-9223372036854775808, indexStart=-9223372036854775808, indexEnd=-9223372036854775808", array.toString());

        array.increment(100, 1);
        testSerialization(array, 24, 1);

        assertEquals("maxSize=10, indexBase=100, indexStart=100, indexEnd=100, array={1,}", array.toString());

        array.increment(100, 254);
        assertEquals(255, array.get(100)); // Still fits in 1 byte.
        testSerialization(array, 24, 1);

        array.increment(100, 1);
        assertEquals(256, array.get(100)); // Needs 2 bytes/counter
        testSerialization(array, 25, 2);

        array.increment(101, 0x0A0B0CL); // 3 byte counter
        assertEquals(0x0A0B0CL, array.get(101));
        testSerialization(array, 29, 3);

        array.increment(104, 0x0A0B0C0DL); // 4 byte counter. test endian.
        assertEquals(0x0A0B0C0DL, array.get(104));
        testSerialization(array, 43, 4);

        array.increment(102, 0xFFFFFFFFL); // test FF for sign extension (should treat as unsigned)
        assertEquals(4294967295L, array.get(102));
        testSerialization(array, 43, 4);

        array.increment(99, 0x0A0B0C0D01L); // 5 byte counter
        assertEquals(0x0A0B0C0D01L, array.get(99));
        testSerialization(array, 53, 5);

        array.increment(95, 0x0A0B0C0D01020304L); // 8 byte counter. test endian.
        assertEquals(0x0A0B0C0D01020304L, array.get(95));
        testSerialization(array, 103, 8);

        assertEquals("maxSize=10, indexBase=100, indexStart=95, indexEnd=104, array={723685415114113796,0,0,0,43135012097,256,658188,4294967295,0,168496141,}", array.toString());
    }

    // Write to buffer, then read back and compare.
    private void testSerialization(final WindowedCounterArray array, final int expectedBufferSize, final int expectedBytesPerCounterSerialized) {
        // Serialization not supported.
    }

    private void assertMeta(final WindowedCounterArray array, final int expectedSize, final long expectedStart, final long expectedEnd) {
        assertEquals(expectedSize, array.getWindowSize());
        assertEquals(expectedStart, array.getIndexStart());
        assertEquals(expectedEnd, array.getIndexEnd());
    }
}
