// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch;

import com.newrelic.nrsketch.indexer.IndexerOption;
import com.newrelic.nrsketch.indexer.ScaledExpIndexer;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

// Most serialization test is done in SimpleNrSketchTest, ComboNrSketchTest, and ConcurrentNrSketchTest.
// This class here only covers the more specific cases.
public class NrSketchSerializerTest {
    @Test
    public void testIndexMakerCode() {
        for (IndexerOption option : IndexerOption.values()) {
            final byte code = NrSketchSerializer.getIndexerMakerCode(option);
            final Function<Integer, ScaledExpIndexer> maker = NrSketchSerializer.getIndexerMakeFromCode(code);
            assertSame(option, maker);
        }
    }

    @Test
    public void testIndexerOptions() {
        for (IndexerOption option : IndexerOption.values()) {
            final SimpleNrSketch sketch = new SimpleNrSketch(20, 10, true, option);
            assertEquals(option, sketch.getIndexerMaker());
            SimpleNrSketchTest.verifySerialization(sketch, 75);
            sketch.insert(100);
            sketch.insert(200);
            SimpleNrSketchTest.verifySerialization(sketch, 92);
        }
    }
}
