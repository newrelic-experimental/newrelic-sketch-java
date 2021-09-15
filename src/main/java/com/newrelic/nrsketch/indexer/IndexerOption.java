// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

import java.util.function.Function;

public enum IndexerOption implements Function<Integer, ScaledExpIndexer> {
    LOG_INDEXER {
        public ScaledExpIndexer getIndexer(final int scale) {
            return new LogIndexer(scale);
        }
    },
    SUB_BUCKET_LOG_INDEXER {
        public ScaledExpIndexer getIndexer(final int scale) {
            return scale > 0 ? new SubBucketLogIndexer(scale) : new ExponentIndexer(scale);
        }
    },
    SUB_BUCKET_LOOKUP_INDEXER {
        public ScaledExpIndexer getIndexer(final int scale) {
            return scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale);
        }
    },
    AUTO_SELECT {
        public ScaledExpIndexer getIndexer(final int scale) {
            // At higher scales, use SubBucketLogIndexer instead of LogIndexer, for more consistency with
            // SubBucketLookupIndexer. And it is slightly faster then LogIndexer.
            return scale > SubBucketLookupIndexer.PREFERRED_MAX_SCALE ? new SubBucketLogIndexer(scale)
                    : (scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale));
        }
    };

    abstract public ScaledExpIndexer getIndexer(final int scale);

    @Override
    public ScaledExpIndexer apply(final Integer scale) {
        return getIndexer(scale);
    }
}