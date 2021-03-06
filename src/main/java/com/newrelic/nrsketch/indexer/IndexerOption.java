// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import java.util.function.Function;

// The option enum is recorded as a code by NrSketchSerializer and restored on deserialization.
// Be careful when changing the semantic of an option.
public enum IndexerOption implements Function<Integer, ScaledIndexer> {
    LOG_INDEXER {
        public ScaledIndexer getIndexer(final int scale) {
            return new LogIndexer(scale);
        }
    },
    SUB_BUCKET_LOG_INDEXER {
        public ScaledIndexer getIndexer(final int scale) {
            return scale > 0 ? new SubBucketLogIndexer(scale) : new ExponentIndexer(scale);
        }
    },
    SUB_BUCKET_LOOKUP_INDEXER {
        public ScaledIndexer getIndexer(final int scale) {
            return scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale);
        }
    },
    AUTO_SELECT {
        public ScaledIndexer getIndexer(final int scale) {
            // At higher scales, use SubBucketLogIndexer instead of LogIndexer, for more consistency with
            // SubBucketLookupIndexer. And it is slightly faster then LogIndexer.
            return scale > SubBucketLookupIndexer.PREFERRED_MAX_SCALE ? new SubBucketLogIndexer(scale)
                    : (scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale));
        }
    };

    abstract public ScaledIndexer getIndexer(final int scale);

    @Override
    public ScaledIndexer apply(final Integer scale) {
        return getIndexer(scale);
    }
}