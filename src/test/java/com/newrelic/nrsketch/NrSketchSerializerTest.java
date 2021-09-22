// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledExpIndexer;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;

import static com.newrelic.nrsketch.SimpleNrSketchTest.verifySerialization;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

// Most serialization test is done in SimpleNrSketchTest, ComboNrSketchTest, and ConcurrentNrSketchTest.
// This class here covers the more unusual cases.
public class NrSketchSerializerTest {
    @Test
    public void testIndexMakerCode() {
        for (IndexerOption option : IndexerOption.values()) {
            final byte code = NrSketchSerializer.getIndexerMakerCode(option);
            final Function<Integer, ScaledExpIndexer> maker = NrSketchSerializer.getIndexerMakerFromCode(code);
            assertSame(option, maker);
        }
    }

    @Test
    public void negativeScaleSimpleNrSketchTest() {
        final int scale = -2;
        final SimpleNrSketch sketch = new SimpleNrSketch(10, scale);
        assertEquals(scale, sketch.getScale());

        sketch.insert(500);
        sketch.insert(5000);
        assertEquals(scale, sketch.getScale());

        final NrSketch readback = verifySerialization(sketch, 77);
        readback.insert(2000);
        assertEquals(3, readback.getCount());
        assertEquals(scale, ((SimpleNrSketch) readback).getScale());
    }

    @Test
    public void negativeScaleComboNrSketchTest() {
        final int scale = -2;
        final ComboNrSketch sketch = new ComboNrSketch(320, scale);
        sketch.insert(-500);
        sketch.insert(5000);

        List<NrSketch> sketches = sketch.getHistograms();
        assertEquals(scale, ((SimpleNrSketch) sketches.get(0)).getScale());
        assertEquals(scale, ((SimpleNrSketch) sketches.get(1)).getScale());

        final NrSketch readback = verifySerialization(sketch, 193);
        readback.insert(2000);
        assertEquals(3, readback.getCount());
        sketches = ((ComboNrSketch) readback).getHistograms();
        assertEquals(scale, ((SimpleNrSketch) sketches.get(0)).getScale());
        assertEquals(scale, ((SimpleNrSketch) sketches.get(1)).getScale());
    }
}
