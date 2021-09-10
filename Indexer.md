# Bucket Indexer

## Class Hierarchy

The indexers are in its own [Indexer Java package](src/main/java/com/newrelic/nrsketch/indexer). The package is self-contained.
It can be used without the rest of NrSketch. The class hierarchy in the package is:

    BucketIndexer // Interface
        |--- ScaledExpIndexer // Abstract class
                 |--- LogIndexer
                 |--- ExponentIndexer
                 |--- SubBucketIndexer // Abstract class
                          |--- SubBucketLogIndexer
                          |--- SubBucketLookupIndexer

The **BucketIndexer** interface is the root of the hierarchy. It defines the API of an indexer, which is very simple,
basically, mapping a "double" value to a bucket index, and mapping a bucket index to the bucket's start and end bound.

**ScaledExpIndexer** is the abstract base class for scaled exponential indexers, which have the following properties:

* bucket_lower_bound = base ^ bucket_index
* base = 2 ^ (2 ^ -scale)

**SubBucketIndexer** is the abstract base class for all subBucket indexers, which divide the
mantissa into log scale subbuckets for each binary exponent in "double". It works only on scales greater than 0.

The followings are concrete classes you can actually instantiate:

* **LogIndexer**: Calls Math.log() for mapping. This is the canonical indexer derived directly from the mathematically
  definition of the scaled base2 indexer.
    * It is relatively slow, because of the log() function
    * It is less accurate than SubBucketLogIndexer because the log() function has to handle the full "double" range.
      Computational error grows with scale and the absolute value of input's exponent (less accurate on larger numbers
      and numbers closer to 0).
* **ExponentIndexer**: Computes index from the exponent part of "double" representation.
    * It is very fast because it uses simple integer operations only.
    * It has no computational error at all because it uses integer operations only.
    * It can only be used on 0 and negative scales, where base >= 2.
* **SubBucketLogIndexer**: Does subbucketing using the Math.log() function.
    * It is more accurate than plain LogIndexer because it limits log() calls to values from 1 to 2. Computational error
      is not sensitive to scale or exponent of input values. It can go all the way to scale 52.
    * Performance is slightly faster than LogIndexer.
* **SubBucketLookupIndexer**: Does subbucketing using lookup tables. Log scale boundaries from 1 to 2 are precomputed.
    * It is very fast because mapping uses integer operations only.
    * But it costs more space due to the lookup tables, which costs about 16 * 2^scale bytes. For example, scale 20
      costs 16MB. Though it is not practical on higher scales, it is practical for most telemetry use cases, where the
      scale is around 6 and memory cost is a few kilo bytes.

As shown above, no indexer excels in all aspects. The **IndexerOption** enum class provides an AUTO_SELECT option to
select the best indexer based on scale. It is the default option for NrSketch. The selection criteria is:

* Scale > 6: SubBucketLogIndexer. Slower, but uses no extra memory.
* Scale in (0, 6]: SubBucketLookupIndexer. Fast mapping at the memory cost of 1KB or less.
* Scale <= 0: ExponentIndexer. Fastest mapping, no extra memory.

At the default initial scale of 12, AUTO_SELECT starts with SubBucketLogIndexer. At the default 320 buckets, most
datasets will quickly downscale to scale 6 (when contrast exceeds 5.66) and start using the more efficient
SubBucketLookupIndexer.

Note that the LogIndexer class is not used by AUTO_SELECT at all. It was written mostly as a reference to test other
indexers.

## Subnormal numbers

The subbucket indexers (SubBucketLogIndexer and SubBucketLookupIndexer) and the ExponentIndexer do not have special
logic for subnormal numbers. Thus they do not satisfy the "bound = base ^ index" formula in the subnormal range (see
SubBucketIndexer.java for details). The LogIndexer does handle subnormal numbers properly. To be standard conforming,
NrSketch folds subnormal numbers into the special bucket for 0, instead of the indexed buckets. This is not a limitation
of the scaled histogram or the subbucket methods. NrSketch does this only because:

* The author is too lazy to write the special subnormal logic
* Subnormal numbers are rarely used. They are at the rarified bottom end of the "double" range. They extend the double
  range at the cost of fewer significant digits.
* Supporting subnormal numbers will add a small performance cost to the subbucket indexers. They will need a "if
  subnormal"
  branch in the critical path.

## How the lookup indexer works

The "double" [IEEE representation](https://en.wikipedia.org/wiki/Double-precision_floating-point_format)
is in binary floating point format of mantissa * 2^exponent. The "exponent" field is effectively scale 0 (base=2)
bucketing. The mantissa is always in the range of [1, 2). We only need to divide the mantissa range into log scale
subbuckets to get to higher scales.

The diagram below shows dividing the mantissa range into 8 log subbuckets, shown with blue lines. At the same time, the
range is also divided into 16 linear subbuckets, shown as black ticks along the x axis.

![Lookup table chart](./LookupTable.svg)

With 2 times more linear subbuckets than log scale subbuckets, linear subbuckets are narrower. A linear subbucket is
either completely enclosed in a log subbucket, or spans 2 log subbuckets. The linear to log bucket ratio can probably be
mathematically proven. The code empirically increases the number of linear buckets until linear bucket width is equal to
or less than the width of the first log bucket (therefore any log bucket, because log bucket width increases
monotonically). The runs always return a ratio of 2.

Two lookup tables are used. The first one, logBucketIndexArray is indexed by linear subbucket index. The array content
is the log bucket index where start of the linear bucket falls into. The 2nd one, logBucketEndArray is indexed by log
subbucket index. The content is the end bound of the log bucket. Below is code from SubBucketLookupIndexer to map a
mantissa to a log subbucket:

```
long getSubBucketIndex(final long mantissa) {
    final int linearArrayIndex = (int) (mantissa >>> mantissaShift);
    final int logBucketIndex = logBucketIndexArray[linearArrayIndex];
    return mantissa >= logBucketEndArray[logBucketIndex] ? logBucketIndex + 1 : logBucketIndex;
}
```

The code first shifts the mantissa to get linear array index. Then looks up in logBucketIndexArray to get
logBucketIndex. If the linear bucket spans two log buckets, we need to determine which log bucket the value falls into.
This is simply done by comparing the value against the end bound of the log bucket. If the linear bucket is completely
enclosed in one log bucket, then the value will be smaller than the log bucket end and the function will return
logBucketIndex as is.

The final log bucket index can be quickly computed from the log subbucket index and exponent from "double". During value
to index mapping, only simple integer operations are used. It is much faster than calling Math.log(). Below are some
indexer benchmark numbers (indexer is coupled with SimpleNrSketch):

| Index option            | insert speed (ns/insert) |
| ----------------------- | ------------------------ |
| LogIndexer              | 32 |
| SubBucketLogIndexer     | 29 |
| SubBucketLookupIndexer  | 19 |
| AUTO_SELECT             | 19 | 

As shown above, LogIndexer is significantly slower. SubBucketLogIndexer is slightly faster than LogIndexer.
SubBucketLogIndexer is used in AUTO_SELECT on larger scales because of this, and also because it is more consistent with
SubBucketLookupIndexer. AUTO_SELECT showed no difference from SubBucketLookupIndexer in this dataset (random numbers
from 1 to 1M). This is expected because the dataset downscales into SubBucketLookupIndexer quickly.

The memory cost of the lookup indexer is small at the scales of interest to telemetry. AUTO_SELECT limits lookup indexer
to scale 6, where the array sizes are 128 * 4 and 64 * 8, for a total of 1024 bytes. The logBucketIndexArray is now "
int[ ]". For lower scales, we could optimize it to "byte[ ]", or "short[ ]" to save space. And the code can also use
static arrays computed on program start or defined at compile time.

If we publish the logBucketEndArray content for commonly used scales, all implementations using the published array will
produce consistent result on any platform. In contrast, calling Math.log() may produce different results on boundary
values due to differences in floating point processing and log() function implementation.
