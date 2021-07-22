// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import org.junit.Test;

import static com.newrelic.nrsketch.DoubleFormat.NEGATIVE_SIGN;
import static com.newrelic.nrsketch.DoubleFormat.POSITIVE_SIGN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DoubleFormatTest {
    private void testDoubleWithBiasedExponent(final double d, final long expectedSign, final long expectedBiasedExponent, final long expectedExponent, final long expectedMantissa) {
        assertEquals(expectedSign, DoubleFormat.getSignBit(d));
        assertEquals(expectedBiasedExponent, DoubleFormat.getBiasedExponent(d));
        assertEquals(expectedExponent, DoubleFormat.getExponent(d));
        assertEquals(expectedMantissa, DoubleFormat.getMantissa(d));

        final double composedDouble = DoubleFormat.makeDouble(expectedSign, expectedBiasedExponent, expectedMantissa);
        if (Double.isNaN(d)) {
            assertTrue(Double.isNaN(composedDouble));
        } else {
            assertTrue(d == composedDouble);
        }
    }

    private void testDouble(final double d, final long expectedSign, final long expectedExponent, final long expectedMantissa) {
        testDoubleWithBiasedExponent(d, expectedSign, expectedExponent + 1023, expectedExponent, expectedMantissa);
    }

    private void testDouble(final double d, final long expectedSign, final long expectedExponent, final long expectedMantissa, final long expectedMantissaHighest4Bits) {
        testDouble(d, expectedSign, expectedExponent, expectedMantissa);
        assertEquals(expectedMantissaHighest4Bits, DoubleFormat.getMantissaHighestNBits(d, 4));
    }

    @Test
    public void testSpecialValues() {
        testDoubleWithBiasedExponent(0, 0, 0, -1023, 0);
        testDoubleWithBiasedExponent(Double.POSITIVE_INFINITY, POSITIVE_SIGN, 2047, 1024, 0);
        testDoubleWithBiasedExponent(Double.NEGATIVE_INFINITY, NEGATIVE_SIGN, 2047, 1024, 0);
        testDoubleWithBiasedExponent(Double.NaN, 0, 2047, 1024, 0x8000000000000L);

        assertFalse(Double.isNaN(0));
        assertFalse(Double.isNaN(Double.POSITIVE_INFINITY));
        assertFalse(Double.isNaN(Double.NEGATIVE_INFINITY));
        assertTrue(Double.isNaN(Double.NaN));

        assertFalse(Double.isInfinite(0));
        assertTrue(Double.isInfinite(Double.POSITIVE_INFINITY));
        assertTrue(Double.isInfinite(Double.NEGATIVE_INFINITY));
        assertFalse(Double.isInfinite(Double.NaN));
    }

    @Test
    public void testValues() {
        testDouble(-1, NEGATIVE_SIGN, 0, 0);
        testDouble(1, POSITIVE_SIGN, 0, 0);

        testDouble(0.5, POSITIVE_SIGN, -1, 0);
        testDouble(0.25, POSITIVE_SIGN, -2, 0);
        testDouble(0.75, POSITIVE_SIGN, -1, 0x8000000000000L, 0x8);

        testDouble(1.5, POSITIVE_SIGN, 0, 0x8000000000000L, 0x8);
        testDouble(2.5, POSITIVE_SIGN, 1, 0x4000000000000L, 0x4);
        testDouble(10, POSITIVE_SIGN, 3, 0x4000000000000L, 0x4);
    }

    // Negative zero is a real thing in double format.
    @Test
    public void testNegativeZero() {
        final double d = -0.0;
        assertEquals(0x1L << 63, Double.doubleToRawLongBits(d)); // Sign bit only
        testDoubleWithBiasedExponent(d, NEGATIVE_SIGN, 0, -1023, 0);
        assertTrue(d == 0);
        assertFalse(d < 0);
        assertFalse(d > 0);
    }
}
