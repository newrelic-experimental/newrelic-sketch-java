// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

// Functions to manipulate double in its native binary format.
// See https://en.wikipedia.org/wiki/Double-precision_floating-point_format and Double.doubleToRawLongBits()
public class DoubleFormat {
    public static final int MANTISSA_BITS = 52;
    public static final int EXPONENT_BITS = 11;
    public static final long EXPONENT_BIAS = 1023;

    // Number of bits to the left of mantissa
    public static final int MANTISSA_SHIFT = DoubleFormat.EXPONENT_BITS + 1; // Exponent plus one sign bit

    // Subnormal extends double min from 2^-1022 to 2^1074.
    public static final int MIN_SUBNORMAL_EXPONENT = Double.MIN_EXPONENT - DoubleFormat.MANTISSA_BITS;

    public static final long EXPONENT_MASK = 0x7ff0000000000000L;
    public static final long MANTISSA_MASK = 0x000fffffffffffffL;

    // getSignBit() return values
    public static final long POSITIVE_SIGN = 0;
    public static final long NEGATIVE_SIGN = 1;

    // Sign is on the highest one bit
    public static long getSignBit(final double d) {
        return Double.doubleToRawLongBits(d) >>> 63;
    }

    // 11 bits: Bits 62-52, as unsigned integer 0 to 2047
    public static long getBiasedExponentFromLong(final long doubleAsLong) {
        return (doubleAsLong & EXPONENT_MASK) >>> MANTISSA_BITS;
    }

    public static long getBiasedExponent(final double d) {
        return getBiasedExponentFromLong(Double.doubleToRawLongBits(d));
    }

    // Apply the bias. Output as integer from −1022 to +1023 because exponents of −1023 (all 0s) is
    // reserved for logical 0 and +1024 (all 1s) is reserved for infinity.
    public static long getExponentFromLong(final long doubleAsLong) {
        return getBiasedExponentFromLong(doubleAsLong) - EXPONENT_BIAS;
    }

    public static long getExponent(final double d) {
        return getExponentFromLong(Double.doubleToRawLongBits(d));
    }

    // 52 bits: Bits 51-0
    // Logical value of double in binary point form: (1.b51 b50 ... b0) * 2 ^ getExpoent()
    public static long getMantissaFromLong(final long doubleAsLong) {
        return doubleAsLong & MANTISSA_MASK;
    }

    public static long getMantissa(final double d) {
        return getMantissaFromLong(Double.doubleToRawLongBits(d));
    }

    public static long getMantissaHighestNBits(final double d, final int n) {
        return getMantissa(d) >>> (MANTISSA_BITS - n);
    }

    public static double makeDouble(final long sign, final long biasedExponent, final long mantissa) {
        return Double.longBitsToDouble((sign << 63) | (biasedExponent << MANTISSA_BITS) | mantissa);
    }

    // Returns a double in [1, 2) range.
    public static double makeDouble1To2(final long mantissa) {
        return makeDouble(POSITIVE_SIGN, EXPONENT_BIAS, mantissa);
    }
}
