// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MultiTypeCounterArrayTest {
    @Test
    public void testSequentialScale() {
        final MultiTypeCounterArray array = new MultiTypeCounterArray(5);

        verifyArray(array, Byte.BYTES, new long[]{0, 0, 0, 0, 0});

        array.increment(3, 20);
        array.increment(3, 5);
        verifyArray(array, Byte.BYTES, new long[]{0, 0, 0, 25, 0});

        array.increment(3, 200);
        verifyArray(array, Short.BYTES, new long[]{0, 0, 0, 225, 0});

        array.increment(0, 1);
        array.increment(0, 10);
        array.increment(2, 100);
        array.increment(4, 200);
        verifyArray(array, Short.BYTES, new long[]{11, 0, 100, 225, 200});

        array.increment(0, 40_000);
        verifyArray(array, Integer.BYTES, new long[]{40_011, 0, 100, 225, 200});

        array.increment(1, 15);
        array.increment(2, 50_000);
        verifyArray(array, Integer.BYTES, new long[]{40_011, 15, 50_100, 225, 200});

        array.increment(0, 3_000_000_000L);
        verifyArray(array, Long.BYTES, new long[]{3_000_040_011L, 15, 50_100, 225, 200});

        array.increment(1, 20);
        array.increment(4, 1000);
        verifyArray(array, Long.BYTES, new long[]{3_000_040_011L, 35, 50_100, 225, 1200});
    }

    @Test
    public void testBoundaryConditions() {
        final MultiTypeCounterArray array = new MultiTypeCounterArray(5);

        verifyArray(array, Byte.BYTES, new long[]{0, 0, 0, 0, 0}); // Empty array

        array.increment(2, Byte.MAX_VALUE); // Stays on Byte
        verifyArray(array, Byte.BYTES, new long[]{0, 0, 127, 0, 0});

        array.increment(2, 1); // Move to Short
        verifyArray(array, Short.BYTES, new long[]{0, 0, 128, 0, 0});

        array.increment(0, Short.MAX_VALUE); // Stays Short.
        verifyArray(array, Short.BYTES, new long[]{32767, 0, 128, 0, 0});

        array.increment(0, 1); // Move to Int
        verifyArray(array, Integer.BYTES, new long[]{32768, 0, 128, 0, 0});

        array.increment(1, Integer.MAX_VALUE); // Stays Int.
        verifyArray(array, Integer.BYTES, new long[]{32768, 2147483647, 128, 0, 0});

        array.increment(1, 1); // Move to Long
        verifyArray(array, Long.BYTES, new long[]{32768, 2147483648L, 128, 0, 0});
    }

    @Test
    public void testByteToInt() {
        final MultiTypeCounterArray array = new MultiTypeCounterArray(5);

        verifyArray(array, Byte.BYTES, new long[]{0, 0, 0, 0, 0});

        array.increment(0, 3_000_000L);
        verifyArray(array, Integer.BYTES, new long[]{3_000_000, 0, 0, 0, 0});

        array.increment(0, 10);
        array.increment(1, 1);
        verifyArray(array, Integer.BYTES, new long[]{3_000_010L, 1, 0, 0, 0});
    }

    @Test
    public void testByteToLong() {
        final MultiTypeCounterArray array = new MultiTypeCounterArray(5);

        verifyArray(array, Byte.BYTES, new long[]{0, 0, 0, 0, 0});

        array.increment(0, 3_000_000_000L);
        verifyArray(array, Long.BYTES, new long[]{3_000_000_000L, 0, 0, 0, 0});

        array.increment(0, 10);
        array.increment(1, 1);
        verifyArray(array, Long.BYTES, new long[]{3_000_000_010L, 1, 0, 0, 0});
    }

    @Test
    public void testShortToLong() {
        final MultiTypeCounterArray array = new MultiTypeCounterArray(5);

        array.increment(4, 300);
        verifyArray(array, Short.BYTES, new long[]{0, 0, 0, 0, 300});

        array.increment(0, 3_000_000_000L);
        verifyArray(array, Long.BYTES, new long[]{3_000_000_000L, 0, 0, 0, 300});

        array.increment(0, 10);
        array.increment(1, 1);
        verifyArray(array, Long.BYTES, new long[]{3_000_000_010L, 1, 0, 0, 300});
    }

    private void verifyArray(final MultiTypeCounterArray array, final int expectedBytesPerCounter, final long[] expectedArray) {
        assertEquals(expectedBytesPerCounter, array.getBytesPerCounter());
        assertEquals(expectedArray.length, array.getMaxSize());
        for (int i = 0; i < expectedArray.length; i++) {
            assertEquals(expectedArray[i], array.get(i));
        }
    }

    @Test
    public void testEqualAndHash() {
        final MultiTypeCounterArray a1 = new MultiTypeCounterArray(5);
        final MultiTypeCounterArray a2 = new MultiTypeCounterArray(5);
        final MultiTypeCounterArray a3 = new MultiTypeCounterArray(6);

        assertNotEquals(a1, a3);

        assertEquals(a1, a2);
        assertEquals(28629151, a1.hashCode());
        assertEquals(28629151, a2.hashCode());

        a1.increment(3, 1);
        assertNotEquals(a1, a2);
        assertEquals(28629182, a1.hashCode());

        a2.increment(3, 1);
        assertEquals(a1, a2);
        assertEquals(28629182, a2.hashCode());
    }

    @Test
    public void testDeepCopy() {
        final MultiTypeCounterArray a1 = new MultiTypeCounterArray(5);
        MultiTypeCounterArray a2 = a1.deepCopy();

        assertEquals(a1, a2);
        assertEquals("bytesPerCounter=1, array={0,0,0,0,0,}", a1.toString());
        assertEquals("bytesPerCounter=1, array={0,0,0,0,0,}", a2.toString());

        a1.increment(2, 20);
        assertNotEquals(a1, a2);

        a2 = a1.deepCopy();
        assertEquals(a1, a2);
        assertEquals("bytesPerCounter=1, array={0,0,20,0,0,}", a1.toString());
        assertEquals("bytesPerCounter=1, array={0,0,20,0,0,}", a2.toString());

        a1.increment(4, 1000);
        a2 = a1.deepCopy();
        assertEquals(a1, a2);
        assertEquals("bytesPerCounter=2, array={0,0,20,0,1000,}", a1.toString());
        assertEquals("bytesPerCounter=2, array={0,0,20,0,1000,}", a2.toString());
    }
}
