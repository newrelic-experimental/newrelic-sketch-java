// Copyright 2021 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// This file is part of the NrSketch project.

package com.newrelic.nrsketch.indexer;

public enum IndexerOption {
    LOG_INDEXER,
    SUB_BUCKET_LOG_INDEXER,
    SUB_BUCKET_LOOKUP_INDEXER,
    AUTO_SELECT;

    public ScaledExpIndexer getIndexer(final int scale) {
        switch (this) {
            case LOG_INDEXER:
                return new LogIndexer(scale);
            case SUB_BUCKET_LOOKUP_INDEXER:
                return scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale);
            case SUB_BUCKET_LOG_INDEXER:
                return scale > 0 ? new SubBucketLogIndexer(scale) : new ExponentIndexer(scale);
            case AUTO_SELECT:
                // At higher scales, use SubBucketLogIndexer instead of LogIndexer, for more consistency with
                // SubBucketLookupIndexer. And it is slightly faster then LogIndexer.
                return scale > SubBucketLookupIndexer.PREFERRED_MAX_SCALE ? new SubBucketLogIndexer(scale)
                        : (scale > 0 ? new SubBucketLookupIndexer(scale) : new ExponentIndexer(scale));
            default:
                throw new IllegalArgumentException("Unknown option " + this);
        }
    }
}
