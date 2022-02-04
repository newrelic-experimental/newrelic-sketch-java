[![New Relic Experimental header](https://github.com/newrelic/opensource-website/raw/master/src/images/categories/Experimental.png)](https://opensource.newrelic.com/oss-category/#new-relic-experimental)

# New Relic Sketch

## Introduction

New Relic Sketch (aka NrSketch)  is a scaled base2 exponential histogram. It is adapted from the histogram code used at
[New Relic](https://newrelic.com/). It is an implementation of the histogram described in the Open Telemetry Enhancement
Proposal
149 [Add exponential bucketing to histogram protobuf](https://github.com/open-telemetry/oteps/blob/main/text/0149-exponential-histogram.md)
. The proposal has been accepted as "ExponentialHistogram" in Open
Telemetry [data model](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md)
and [transport protocol](https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto)
. Though NrSketch conforms to Open Telemetry standards, it is written as a general purpose library, with no dependency
on any third party, including any Open Telemetry code.

A scaled base2 exponential histogram has these properties:

* bucket_lower_bound = base ^ bucket_index
* base = 2 ^ (2 ^ -scale)

Bucket index is an integer that can be positive (bound > 1), 0 (bound = 1), or negative (bound < 1). Base is a floating
point number greater than 1. It is further restricted by scale. Scale is an integer that can be positive (base < 2), 0 (
base = 2), or negative (base > 2). The following table shows bases at selected scales.

| scale | base          | relative error | dataset contrast at 160 buckets |
| ----- | ------------- | -------------- | ------------------------------- |
| 20    | 1.000 000 661 | 0.000 033%     | 1.000 106                       |
| 19    | 1.000 001 322 | 0.000 066%     | 1.000 212                       |
| 18    | 1.000 002 644 | 0.000 132%     | 1.000 423                       |
| 17    | 1.000 005 288 | 0.000 264%     | 1.000 846                       |
| 16    | 1.000 011     | 0.000 529%     | 1.001 694                       |
| 15    | 1.000 021     | 0.001 058%     | 1.003 390                       |
| 14    | 1.000 042     | 0.002 115%     | 1.006 792                       |
| 13    | 1.000 085     | 0.004 231%     | 1.013 630                       |
| 12    | 1.000 169     | 0.008 461%     | 1.027 446                       |
| 11    | 1.000 339     | 0.017%         | 1.055 645                       |
| 10    | 1.000 677     | 0.034%         | 1.114                         |
| 9     | 1.001 355     | 0.068%         | 1.241                         |
| 8     | 1.002 711     | 0.135%         | 1.542                         |
| 7     | 1.005 430     | 0.271%         | 2.378                         |
| 6     | 1.010 889     | 0.542%         | 5.656                         |
| 5     | 1.021 897     | 1.083%         | 32                              |
| 4     | 1.044 274     | 2.166%         | 1,024 (1K)                          |
| 3     | 1.090 508     | 4.329%         | 1,048,576 (1M)                      |
| 2     | 1.189 207     | 8.643%         | 1.10E+12 (1T)                       |
| 1     | 1.414 214     | 17.157%        | 1.21E+24                        |
| 0     | 2.000 000     | 33.333%        | 1.46E+48                        |
| \-1   | 4.000 000     | 60.000%        | 2.14E+96                        |
| \-2   | 16.000 000    | 88.235%        | 4.56E+192                       |

"Relative error" here is the relative error of percentile or quantile calculated from the histogram. Relative error is
defined as "Math.abs(reportedValue - actualValue) / reportedValue". To minimize error, percentile calculation returns
the middle point of a bucket when a requested percentage falls within a bucket. Thus  
error = (bucketWidth/2) / bucketMidPoint = ((base - 1) / 2) / ((base + 1) / 2) = (base - 1) / (base + 1).

The scales of interest to most telemetry applications is around 3, 4, and 5, where relative error is around a few
percent. As a series, the bases can be defined as:  
```base[0] = 2```  
```base[scale - 1] = square(base[scale])```  
This property of the base series allows downscaling: merging every 2 neighboring buckets to move down one scale. And any
two histograms can be merged without artifact. The one with higher scale will be downscaled to match the one with lower
scale, if they are on different scales. At the same scale, two histograms' bucket bounds exactly align up. Merging is
simple as adding up the bucket counts.

In addition to implementing a standard conforming scaled base2 exponential histogram, NrSketch has the following
features:

* Allow users to configure maximal number of histogram buckets and initial scale (ie. histogram resolution).
* Auto down scale to fit dataset into the configured number of buckets
* ComboNrSketch supports high resolution bucketing for both positive and negative numbers
* ConcurrentNrSketch supports multi-thread concurrency
* Tracks min, max, count, and sum of the dataset
* Fast insert performance via lookup tables, bypassing floating point operations and Math.log() call
* Small memory footprint. It uses the smallest integer type (1, 2, 4, or 8 byte per counter) that can hold the bucket
  counts.
* Histogram merge code included
* Histogram subtraction code included. Subtraction can be used to compute delta from accumulative metrics.
* Percentile calculation code included
* Serialization and deserialization code included
* Full "double" range, including subnormal numbers are supported, at all meaningful scales (-11 to 52, inclusive).

With the default maximal number of buckets at 160, a histogram can fit a dataset with contrast (maxValue / minValue) up
to 1M at scale 3, for a 4.3% relative error. Here contrast = 2 ^ (numBuckets / 2^scale). The memory cost of 160 buckets
is modest. Because NrSketch uses variable size counters, and most use cases need no more than 4 bytes per bucket count,
the in memory footprint of an NrSketch is typically less than 160 * 4 = 540 bytes. The included serializer
uses [varint](https://en.wikipedia.org/wiki/Variable-length_quantity) to encode the counters, which is even more space
efficient. Serialized size is often just a few hundred bytes. At this level of cost, many use cases will not need to
override the default max number of buckets. The default no argument constructor will often just work.

NrSketch defaults to an initial scale of 20. At this scale and below, bucket index can fit into a signed 32 bit integer.
This makes the generated histogram compatible with systems where index is limited to 32 bit, even though nrSketch itself
can go all the way to scale 52 and 64 bit index. The relatively high initial scale makes sure that the default config
works from very low contrast to very high contrast dataset. Typically within the first a few values in the dataset,
nrSketch can quickly downscale to fit the dataset. Because nrSketch can downscale multiple scales at once, the cost to
fit high contrast is small. For example, if the first value is 1, the second value is 5, nrSketch can downscale directly
from the initial scale of 20 to scale 6, where the max dataset contrast is 5.6 at the default 160 buckets. In most
cases, you will not need to override the default initial scale.

Note that contrast is ratio of maxValue / minNonZeroValue in a dataset. It is not the absolute value of max value. For
example, the datasets of [1, 1M] and [10, 10M] both have a contrast of 1M. The value of 0 does not fall into any of the
indexed buckets. It has its own special counter.

## NrSketch API

NrSketch provides the following interfaces and classes:

* **NrSketch**: This is an interface. All NrSketches implement this interface. The interface provides methods to insert
  values into a histogram, to merge histograms, to iterate over the buckets, and to compute percentiles.

* **SimpleNrSketch**: This is a simple sketch containing a single histogram, where the indexed buckets can be configured
  to hold either positive or negative numbers. When configured for positive numbers, all negative numbers fall into a
  single special bucket. Percentiles in the positive number range are still correct and meet relative error guarantee.
  But the negative range cannot be resolved beyond the single bucket. Similarly, when configured for negative numbers,
  negative numbers get high resolution, but positive numbers fall into a single bucket. This sketch is useful when you
  expect only positive or negative numbers, or don't care about resolution of the other range, and want to keep memory
  and cpu cost to minimum.

* **ComboNrSketch**: This sketch is the combination of two SimpleNrSketches, one for positive and one for negative
  numbers. Both get high resolution. Note that each SimpleNrSketch is created only on demand. So if input has only
  positive or negative numbers, only one SimpleNrSketch is created. This is the recommended class for most use cases.
  You never know what your data will look like. By catching "unexpected" numbers, it helps debugging your app.

* **ConcurrentNrSketch**: This is a concurrency wrapper on any class implementing the NrSketch interface. It adds "
  synchronized" for all methods of the interface. It can be used on top of SimpleNrSketch or ComboNrSketch. It does add
  some cpu overhead. Exact amount depends on the particular platform.

* **NrSketchSerializer**: This class serializes a sketch to a ByteBuffer, or deserializes a sketch from a ByteBuffer.
  The bucket counters are written as [varint](https://en.wikipedia.org/wiki/Variable-length_quantity) to save space. At
  the default max 160 buckets, typical serialized object size is less than 500 bytes.

For your convenience, Jmh benchmark is included in this project to measure cpu cost (see jmhInsert.sh). To give you some
rough idea on the relative cpu cost of the different classes, below are some numbers. As always, benchmark numbers
should be taken with a grain of salt. It is most useful when looking at the performance of the classes relative to each
other.

| class | insert speed (ns/insert) | Notes |
| ----- | ------------------------ | ------ |
| SimpleNrSketch | 19 |
| ComboNrSketch  | 20 |
| ConcurrentNrSketch | 28 | Wrapper on ComboNrSketch. Single thread. No lock contention

As shown above, ComboNrSketch only adds a little cpu cost to SimpleNrSketch. ComboNrSketch should be used as a general
purpose class. SimpleNrSketch should be used only under extreme cpu or memory requirement. And of course, when
multi-thread concurrent access to a sketch is needed, ConcurrentNrSketch has to be used. It costs more, but in most
cases the cost should be still acceptable. The number above was from a single thread, which never hits lock contention.
The number will be higher when there are actually multiple threads competing for access.

Research was done on alternate methods for concurrency control, such as read/write lock, or piping data to a single
processing thread. The simple "synchronized" method beats the alternatives by a long shot, most likely because the
critical window is so short (10 to 20 ns), any fancy synchronization methods will introduce higher overhead themselves.
You are better off to just use a simple lock.

## Internal classes

This section describes the internal classes used by the API classes.

### Bucket Indexer

See [Bucket Indexer](Indexer.md)

### WindowedCounterArray

WindowedCounterArray is a logical array with an index window. Window can start anywhere in the "long" range, including
negative numbers. The array is backed by a physical array indexed from 0 to windowSize - 1. The first write is mapped to
physical index 0. It defines the offset between logical index and physical index. Additional writes can be above or
below this offset. A logical index is mapped to a physical index in a circular array fashion to avoid shifting the
array. Window start (lowest logical index) and end (highest logical index)
are auto updated on array write. Write will fail when window size exceeds a configured max size.

The backing array today is a MultiTypeCounterArray.

The WindowedCounterArray allows NrSketch indexers to use "long" as index type to map any "double" value to an index.

### MultiTypeCounterArray

MultiTypeCounterArray provides a "long" array element API. Under the hood, it is backed by a byte, short, int, or long
array. It starts from a byte array. As counters are incremented, it auto scales to the next larger type.

This array allows NrSketch to support counters up to full range of "long", but use less space in most cases. In
practice, counter type rarely reaches long. It usually stops at short (2 byte) or int (4 byte).

## Build and testing

This is a gradle project. Run "./gradlew build" to build and test the project.

IntelliJ is recommended for development. Run "./gradlew idea" to create IntelliJ project files.

Main code is in src/main.

Unit tests are included. See src/test

For benchmarking, see jmhIndexer.sh, jmhInserrt.sh and src/jmh.

## Publishing Artifacts

NrSketch publishes artifacts to Maven, in the "com.newrelic" group. To use nrSketch from Maven, you may include such 
lines in a gradle file:
```
dependencies {
    implementation "com.newrelic:nrsketch:1.1"
```

To publish a new version of NrSketch, see [Publish.md](Publish.md)

## Support

NrSketch is experimental right now. No support yet.

## Contributing

We encourage your contributions to improve NrSketch. Keep in mind when you submit your pull request, you'll need to sign
the CLA via the click-through using CLA-Assistant. You only have to sign the CLA one time per project. If you have any
questions, or to execute our corporate CLA, required if your contribution is on behalf of a company, please drop us an
email at opensource@newrelic.com.

**A note about vulnerabilities**

As noted in our [security policy](../../security/policy), New Relic is committed to the privacy and security of our
customers and their data. We believe that providing coordinated disclosure by security researchers and engaging with the
security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we
welcome and greatly appreciate you reporting it to New Relic through [HackerOne](https://hackerone.com/newrelic).

## License

NrSketch is licensed under the [Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License.

NrSketch does not use any third party code or libraries. It only uses standard Java library classes such as ArrayList.
