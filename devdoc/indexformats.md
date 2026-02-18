# Index Representation in the HPCC Systems Platform

The indexes used by the HPCC systems are based on B-trees.  The B-trees are stored in fixed-size nodes (typically 8KB), and there are 3 main types of node:
- Branch nodes.\
  These are the top levels of the search index, used to find the leaf nodes containing the records.  There may be any number of levels of branch nodes, but typical values range from 0 (a very small index) to 3 for a very large index.
  The branch nodes have references to the next nodes in the search tree, which consist of the keyed fields for the first entry in the node and the file offset.
- Leaf nodes.\
  These contain multiple index records - both the keyed and payload fields.
- Blob nodes\
  Each row in an index is limited to 32K uncompressed.  If a larger payload is required, it is stored separately in one or more blob nodes.

All these nodes are compressed (in different ways) when stored on disk.  The compressed representation can be anything from 1× to 50× smaller; 5× is a typical compression ratio.

## Factors Affecting Index performance

There are four main factors that affect the performance of the indexes:

### In-memory size

When a node has been read from disk and is ready to be searched, how much memory does it occupy?

The system has a cache of branch and leaf nodes that have been read from disk.  If nodes take up less space in main memory, then more nodes can be held in the leaf cache.  That reduces the time spent reading from disk and decompressing.

### Disk size

#### Benefits

The size on disk is interesting for a few reasons:
- Smaller files cost less to store on the cloud.\
  They also cost less to copy and create.  On bare-metal systems, it reduces the likelihood of running out of disk space.
- The size of Roxie clusters is often determined by the size of the data.\
  If the data shrinks, then the Roxie cluster could run on less expensive compute.  For some single-node environments enabling all the data to fit on the local disk has significant cost and deployment advantages.

See the further discussion under the compression ratio below.

#### Effect on performance

The effect of disk size on performance is less clear.

If an index lookup reads entries from many consecutive leaf nodes then increased compression will reduce the number of reads.  However, if a search only tends to read a few items from one node there will be no benefit.

If the reduced disk size has a corresponding increase in in-memory size for each node then it may have a negative effect, because the internal cache will be less effective.  However, it will increase the effectiveness of the Linux page cache since more data will be held in the compressed pages.
 
The effect is highly dependent on the pattern of data access.  If there is an exponential decay in the access frequency, then better compression will help because the pages in the Linux page cache will be used multiple times, avoiding disk reads.  If the access pattern is uniform across the dataset then the Linux page cache will give little benefit, and higher compression will reduce the performance.  Past evidence suggests the former.

### Decompression time and compression ratio

Each time a node is read from disk it needs to be decompressed before it can be used.  Here are some ballpark ideas of the relative performance of some different decompression algorithms (actual numbers for compression ratios vary significantly depending on the data being compressed):

| Algorithm | Decompression Time | Compression Ratio |
|-----------|-------------------|------------------|
| lzw       | 300 µs            | 5×               |
| lz4       | 50 µs             | 4×               |
| zstd      | 100 µs            | 7×               |

#### Decompression time

Fast decompression will obviously reduce the processing time.  This will improve the latency and the throughput, and will potentially allow the same workload to be performed on fewer Roxie nodes because of the reduced CPU load.

#### Compression ratio

The interaction between decompression times and compression ratios is complex once you take the Linux page cache into account...

Linux has a page cache which uses any spare memory to cache previous reads from disk.  Often when loading a node from disk it will already be in the page cache because it has previously been read.  That typically reduces the read time from ~150 µs (NVMe/SSD) to <10 µs.

The Linux page cache contains compressed nodes, while the Roxie internal node-caches contain decompressed pages.  For the same amount of memory, more nodes can be kept in the Linux page cache than the internal cache.  Increasing memory allocated to the page cache reduces the number of nodes that are actually read from disk - which is more significant the slower the disk.

The faster the decompression, the greater the benefit of the Linux page cache over the internal cache.  When index formats are changed you will need to re-tune the system to find the best balance.

The compression ratio further complicates the picture.  As noted above, a high compression ratio means that fewer nodes can be held in memory, so the internal cache is less effective.  However, the Linux page cache will be more effective so it may be a net benefit.

Also see the notes above about disk size and performance.

There is one special case worth noting.  If the size in memory is the same as the size on disk then it is worth making the in-memory cache as large as possible.  (Inplace indexes with expansion on demand also provide this.)  If this is the case, then the worst performance is when the internal cache and the Linux page cache are the same size - since they are likely to be storing the same data, half the memory will be wasted.

### Search time

This has not currently been explored in detail.  If the majority of operations do not require nodes to be loaded from disk then the search time will be critical.  As soon as nodes need to be loaded from disk and decompressed those times are likely to dominate.  An approach that is slower to search, but reduces the number of disk reads, is likely to outperform a representation that is quicker to search.

There is scope for profiling and optimizing the search code in the future.

### Balancing the factors

The ideal solution would be a format that had very high compression, no time to decompress and the in-memory size matched the disk size.  That is unattainable, so what is the relative significance of the different factors?

1. Minimize disk size.\
   If cost is the ultimate priority, then minimizing the disk size is likely to provide the largest benefit - since that often defines the size of the compute required.

2. Minimize real disk reads.\
   Disk reads take much longer than anything else, so reducing them will improve latency.  If the disk reads are completely offloaded from the CPU and it is possible to increase the number of concurrent reads, then reducing the number of disk reads may not significantly improve throughput.
   The number of disk reads is most likely to be reduced by reducing the disk size, and setting aside a large proportion of the memory for compressed nodes.

3. Reduce decompression time.\
   If the compressed nodes are stored in the Linux page cache, then reducing the decompression time will help when they are loaded into memory.  Reducing this improves both latency and throughput.

4. Decrease the in-memory size.\
   This will reduce the number of times pages will need to be read from page-cache/disk and decompressed.  This will improve both latency and throughput.

It isn't clear which of (3) or (4) is the most significant - it is highly dependent on the internal cache sizes and the data access patterns - but I think (2) probably is most important.  That has implications for the default compression.

#### Batch v interactive systems

Batch systems are most concerned with throughput, interactive systems are concerned with minimizing latency.  This can lead to different priorities for the two systems.
* Anything that reduces the CPU load is likely to improve both the latency and the throughput.
* Reducing IO times will have a large effect on latency.  If sufficient operations can be executed in parallel it may not affect throughput.
* Reducing IO operations similarly has a much larger effect on latency than throughput.

##### Interactive

For interactive systems, minimizing the disk reads is the priority - since that is the operation with the highest latency.  That suggests a relatively large Linux page cache, and relatively small Roxie node caches will be optimal - since that will minimize the number of nodes that must be fetched from disk.

##### Batch

Reducing CPU load is the priority.  It is possible that throughput is higher with a larger Roxie node cache - since it will reduce the time to decompress pages.  That reduced decompression load will need to be balanced with extra CPU load reading pages from the Linux page cache or disk.  Theoretically reducing the Linux page cache should only reduce latency rather than throughput.

## Index formats

What are the different index formats supported by the system? What are their advantages and disadvantages?

### Legacy

In memory:
* Nodes store all the expanded rows in a contiguous block of memory.

On disk:
* Branch nodes\
  Use a very lightweight compression - compressing common leading strings.  Very fast to decompress, but typically compress by a factor of 2.
* Leaf nodes\
  Use our own implementation of LZW to compress the block of rows.  Relatively slow to decompress, but provides fairly good compression.
* Blob nodes\
  Use LZW compression.

### Inplace

In memory:
* Branch nodes\
  Use a proprietary compression format that is essentially a serialized prefix tree.  This compression format works well for sorted, related data.  It does not compress unrelated data very well.  No decompression needed.
* Leaf nodes\
  The keyed portion uses the same compression format as the branch nodes.  The payload is stored separately - all the uncompressed payload fields in a contiguous block of memory.  This format also has an option to keep the payload compressed, and only expand it when it is actually required (i.e., a matching entry has been found).

On disk:
* Branch nodes\
  Essentially identical to in memory.  No time to decompress, compresses by a factor of ~5 if fields are related.
* Leaf nodes\
  Compression algorithm is configurable.  Currently defaults to LZ4HC3S - which is the high compression version of LZ4, with the compression level set to 3.  It uses the streamed API to reduce the time to build the indexes.  Very fast to decompress.
* Blob nodes\
  Currently use LZW compression.

Version 9.14.48 adds support for inplace:zstds, which further reduces the size of the indexes.  It is likely that the reduced disk size will outweigh the extra decompression time, and the default for compressed('inplace') will be changed to compressed('inplace:zstds') in version 10.4.0.

### Hybrid

This format uses the same representation as inplace compression for branch nodes, and an improved version of the legacy compression for leaf nodes.

* Branch nodes\
  Uses the same format as inplace.  Pages can be searched while compressed.
* Leaf nodes\
  The whole leaf node - key and payload - is compressed as a single block.  The compression algorithm can be configured.  (Defaults to zstds6.)
* Blob nodes\
  Use ZStd compression.

Initial results suggest that the on-disk sizes of hybrid indexes are notably smaller than the legacy format, and generally smaller than the current "inplace".  They are very similar size to inplace:zstds for those indexes where inplace compression works well, but also compress well even when inplace compression is not as effective.

The hybrid format is supported for reading in builds 9.14.48, 10.0.22 and 10.2.0.  The indexes should be created in version 10.0.22 or later.

### Further ways of reducing the index sizes

Some of the following reduce in-memory size, some on-disk and some both

* Special case common leading text for all entries in a leaf node.\
  HPCC-35477.  This requires compressing the data twice - which significantly increases the time to create the indexes.  Initial experiments suggest a further 1% saving.  It would also reduce the in-memory sizes.  The trade off does not currently seem worthwhile, so it is not considered a priority at the moment - the code to implement the compression changes (but not reading) is in branch ghalliday/issue35477.
* Encode fixed size strings as variable length.\
  HPCC-35386.  This also relies on support for single-byte prefixed variable length strings.  Initial work suggests that it can have a noticeable effect on some indexes, and no effect on others.  The changes are likely to be released as a new ECL syntax in 10.2.x.
* Use zstd for blobs\
  Blobs are not very common, but this is likely to significantly cut the sizes of files that do use them.
* Use dictionaries\
  This has the potential of increasing compression.  No further work has been done yet.

## Current Recommendations for using the new index formats

What are the recommendations for index formats?  The first question is what is the priority - reducing costs or improving performance?

### Reducing costs

Almost all indexes should use `compress('hybrid')`.  This is likely to give the smallest  disk sizes, with good decompression performance.  A relatively small subset of indexes will create smaller files by using `compress('inplace:zstds,blob(zstd)')`.  (This will become the default for inplace indexes in a future version.)  Good candidates for inplace compression are indexes with few index fields that are fairly densely populated (e.g. phone numbers).

### Improving performance

It will require careful profiling to determine the best approach.  The first approximation should be to follow the recommendations for reducing costs.  Then explore the following:

* Using `hybrid:lz4shc` for indexes with many keyed fields and `inplace:lz4shc` for indexes with few (or very dense) keyed fields.

This will trade increased disk space for reduced decompression time.  If all files fit on local fast NVMe storage, and there is no opportunity to reduce costs by reducing the cluster size (e.g. the cluster is already CPU constrained), then using lz4hcs rather than zstds compression will reduce the CPU load.  Whether that outweighs the increased time to read from disk will be data and query dependent.

### Indexes with payload fields in the keyed portion

There are other indexes where there are fields in the keyed portion that should be in the payload.  This causes two problems:

* Branch nodes are larger than they need to be since they contain extra data.
* The inplace keyed compression does not work well on unrelated data, leading to bigger inplace indexes.

Moving the fields is likely to be a worthwhile exercise:

- It will reduce the size and number of the branch nodes - occasionally significantly.
- It may marginally speed up searching on the indexes because fewer fields need to be compared when performing a keyed match.
- It will have a notable effect on the size of inplace indexes, but relatively little on hybrid indexes.
- It potentially allows the payload fields to be implicitly trimmed, possibly improving compression.

### Small key fields and large complex payload

There are some keys that have a very small keyed portion, and a very large payload.  If the compression ratio for the leaf nodes is >~20% (?) then the lz4hc3 compression may not be as good as lzw compression, leading to larger indexes.  The best approach would be to compress with 'inplace:zstds' instead.  This will reduce the decompression time and the disk size.

### Highly compressible payload

There are some indexes that are pathologically compressible - e.g., leaves take up less than 5% of the size on disk.  This can cause problems when they are read - because an 8KB page can expand to 400KB in memory.  That can cause fragmentation problems with the memory manager and means the leaf cache can only hold a few entries.  In general the benefits of improved compression are likely to outweigh these problems, so the compression is limited to 100x by default.

If anything can be done to reduce the size of the payload in memory that is likely to improve performance.

Solutions:
- Use variable size strings in the payload.\
  The most likely cause of extreme compression is large fixed-size string fields that are mainly blank or contain short strings.  Where that is the case, it is better to trim the fields and store them as varstrings.
  This will significantly increase the effectiveness of the internal cache - because the uncompressed size will be much smaller.  It will also reduce the decompression time.
- Enhance the platform to automatically compress padded strings.\
  This feature is coming soon.

## What about remote file storage?

For batch systems, the throughput should be very similar when retrieving data from remote storage rather than locally, provided the number of concurrent requests can be increased to balance the increased read time.

For interactive systems, the extra latency is problematic.  Configuring the local NVMe cache should significantly reduce the average latency, and fewer remote requests may also reduce the maximum latency.

## Current State of Development

* Inplace\
  Implemented.  Default changed to inplace:lz4hc3.  Should consider changing the default to inplace:zstd.
  Expansion of inplace index payloads on demand (rather than at index load time) is implemented, but its performance has not yet been evaluated.
* Hybrid\
  Implemented for reading in 9.14.48 and writing in 10.0.22 and later.
* Trim compression\
  HPCC-35386. Work in progress - likely to be available in 10.2.x, with reading supported in 10.0.22 and later.
* Support for moving keyed fields.\
  This should now be complete.
* Zstd compression of blobs\
  Complete.  Can be configured for inplace indexes using the blob(zstd) option, it is the default for hybrid.
* Local NVMe file cache\
  Implemented
* Explicitly managed page cache\
  HPCC-35577.  Not yet implemented.

## Compression options

The compression specification has the following format:

`compress('<format>[:[compression[(compress-options)]|compression[(compress-options)],index-options|index-options]]')`

Compression formats (for details see above):
* legacy - the format that has been historically used
* inplace - keyed portion is searchable without decompression
* hybrid - inplace branches, legacy-style leaf nodes

compression:
* lzw - historical compression method.  Not recommended.
* lz4s - fast to decompress, does not compress as well as lzw.
* lz4shc - equally fast to decompress, slower to compress but more compact
* zstds - compresses very well, relatively fast to decompress.
* zstds3 - alias for zstds(level=3)
* zstds6 - alias for zstds(level=6)
* zstds9 - alias for zstds(level=9)

NOTE: lz4s, lz4shc and zstds are the streaming versions and have an 's' suffix.  They should be used for the node compression.

index-options:
* blob(compression) - control the format that is used to compress blobs.  Note the options should be lzw, lz4, lz4hc and zstd (without the 's' suffix).

index-options for inplace indexes, many are only for developers
* recompress=bool - should the payload be recompressed to see if it reduces in size? (default false)
* maxCompressionFactor=n - Do not compress payload more than a factor of n (default 100)
* reuse=bool - reuse compressor (defaults to true)
* compressThreshold=n - use uncompressed if compressed > threshold% of uncompressed (default 95%)
* uncompressed=bool - avoid compressing the payload

compress-options:  Passed directly to the compression algorithm
* level=n - Which lz4/zstd compression level should be used?

Examples:

`compress('hybrid:zstds(level=9),blob(lz4hc)')`

## Default index options

Version 9.14.x of the platform onwards provide 3 options for configuring the default compression the system uses when building an index:

* **defaultIndexCompression**\
  The compression format used if there is no COMPRESS attribute on the INDEX or BUILD statement.
* **defaultInplaceCompression**\
  Which form of compression is used by the inplace compression for compressing the payload portion.
* **overrideIndexCompression**\
  Ignore any COMPRESS settings specified on a BUILD and use this compression format instead.  Primarily for testing.

The options can be configured in bare-metal and containerized builds.  The examples below illustrate using the option to match the new system defaults which will be adopted in 10.4.0.

### Containerized

The following will work in all versions:

```
global:
  expert:
    defaultIndexCompression: "hybrid:zstds(level=6)"
    defaultInplaceCompression: "zstds(level=6),blob(zstd)"
```

NOTE: The different use of zstds and zstd is deliberate.  zstds is a streaming version of zstd that is used for index pages.

The following simpler options will work in 10.0.x and later:

```
global:
  expert:
    defaultIndexCompression: "hybrid"
    defaultInplaceCompression: "zstds6,blob(zstd)"
```

The compression of blobs and unused file positions is slightly better in 10.0.x than 9.14.x, but the differences are relatively minor.

### Bare-metal

```
<Environment>
 <Software>
  <Globals
    defaultIndexCompression="hybrid:zstds(level=6)"
    defaultInplaceCompression="zstds(level=6),blob(zstd)"
  />
 </Software>
<Environment>
```

### Debug option

The default compression for a workunit can also be specified using a #option:

```
#option ('defaultIndexCompression', 'hybrid:zstds(level=6)');
```

The priority order is:

* overrideIndexCompression global option
* COMPRESS option in the ECL
* defaultIndexCompression workunit option
* overrideIndexCompression global option
* system default
