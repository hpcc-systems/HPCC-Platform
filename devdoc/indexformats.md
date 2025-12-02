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

#### Effect on performance

The effect of disk size on performance is less clear.

If an index lookup reads entries from many consecutive leaf nodes then increased compression will reduce the number of reads.  However, if a search only tends to read a few items from one node there will be no benefit.

If the reduced disk size has a corresponding increase in in-memory size for each node then it may have a negative effect, because the internal cache will be less effective.  However, it will increase the effectiveness of the Linux page cache since more data will be held in the compressed pages.
 
The effect is highly dependent on the pattern of data access.  If there is an exponential decay in the access frequency, then better compression will help because the pages in the Linux page cache will be used multiple times, avoiding disk reads.  If the access pattern is uniform across the dataset then the Linux page cache will give little benefit, and higher compression will reduce the performance.  Past evidence suggests the former.

### Decompression time and compression ratio

Each time a node is read from disk it needs to be decompressed before it can be used.  Here are some ballpark ideas of the relative performance of some different decompression algorithms (actual numbers for compression vary much more):

| Algorithm | Decompression Time | Compression Ratio |
|-----------|-------------------|------------------|
| lzw       | 300 µs            | 5×               |
| lz4       | 50 µs             | 4×               |
| zstd      | 100 µs            | 7×               |

Fast decompression will obviously reduce the processing time.  But the decompression times and compression ratios have a more complex effect once you take the Linux page cache into account...

Linux has a page cache which uses any spare memory to cache previous reads from disk.  Often when loading a node from disk it will already be in the page cache because it has previously been read.  That typically reduces the read time from ~150 µs (NVMe/SSD) to <10 µs.

The Linux page cache contains compressed nodes, while the internal cache contains expanded pages.  For the same amount of memory, more nodes can be kept in the Linux page cache than the internal cache.  This reduces the number of nodes that are actually read from disk - which is more significant the slower the disk.

The faster the decompression, the greater the benefit of the Linux page cache over the internal cache.  When index formats are changed you will need to re-tune the system to find the best balance.

The compression ratio further complicates the picture.  As noted above, a high compression ratio means that fewer nodes can be held in memory, so the internal cache is less effective.  However, the Linux page cache will be more effective so it may be a net benefit.

Also see the notes above about disk size and performance.

There is one special case worth noting.  If the size in memory is the same as the size on disk then it is worth making the in-memory cache as large as possible.  (Inplace indexes with expansion on demand also provide this.)  If this is the case, then the worst performance is when the internal cache and the Linux page cache are the same size - since they are likely to be storing the same data, so half the memory will be wasted.

### Search time

This has not currently been explored in detail.  If the majority of operations do not require nodes to be loaded from disk then the search time will be critical.  As soon as nodes need to be loaded from disk and decompressed those times are likely to dominate.  An approach that is slower to search, but reduces the number of disk reads, is likely to outperform a representation that is quicker to search.

There is scope for profiling and optimizing the search code in the future.

### Balancing the factors

The ideal solution would be a format that had very high compression, no time to decompress and the in-memory size matched the disk size.  That is unattainable, so what is the relative significance of the different factors?

One subtlety is the difference between batch and interactive systems.  Batch systems are most concerned with throughput, interactive systems are concerned with minimizing latency.  Sometimes optimizing for those goals needs different solutions.

1. Minimize real disk reads.\
   Disk reads take much longer than anything else, so reducing them will improve latency.  If the disk reads are completely offloaded from the CPU and it is possible to increase the number of concurrent reads, then reducing the number of disk reads may not significantly improve throughput.
   The number of disk reads is most likely to be reduced by reducing the disk size, and setting aside a large proportion of the memory for compressed nodes.

2. Reduce decompression time.\
   If the compressed nodes are stored in the Linux page cache, then reducing the decompression time will help when they are loaded into memory.  Reducing this improves both latency and throughput.

3. Decrease the in-memory size.\
   This will reduce the number of times pages will need to be read from page-cache/disk and decompressed.  This will improve both latency and throughput.

It isn't clear which of (2) or (3) is the most significant - it is highly dependent on the internal cache sizes and the data access patterns - but I think (1) probably is most important.  That has implications for the default compression.

For a batch system it may be better to have a larger internal cache and smaller page cache - because a larger internal cache will improve throughput, but reducing the Linux page cache should only reduce latency rather than throughput.

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

Version 9.14.x adds support for inplace:zstds, which further reduces the size of the indexes.  It is likely that the reduced disk size will outweigh the extra decompression time, and the default for compressed('inplace') should be compressed('inplace:zstd') once systems that support that format are widely deployed.

### Hybrid

This format uses the same representation as inplace compression for branch nodes, and an improved version of the legacy compression for leaf nodes.  The most significant change is the compression algorithm can be configured - allowing the use of zstd.

Initial results suggest that the on-disk sizes of hybrid:zstds indexes are notably smaller than the legacy format, and generally smaller than the current "inplace:lz4hc3s".  They are very similar size to inplace:zstds for those indexes where inplace compression works well, but also compress well even when inplace compression does not.

Once the format is stable and systems that support it (in a future 9.14.x release) are deployed, the default compression should be changed to hybrid:zstds

### Further ways of reducing the index sizes

Some of the following reduce in-memory size, some on-disk and some both

* Better encoding of variable length rows.
  (Rows < 127 bytes larger than the keyed portion, or even the fixed portion.)  Saves <.5%
* Special case common leading text for all entries in a leaf node.
  It would require compressing the data twice, but initial experiments suggest a further 1% saving.  It should be added to hybrid indexes as an option.  It would also reduce the in-memory sizes.
* Encode fixed size strings as variable length.
  This also relies on support for single-byte prefixed variable length strings.  Initial work suggests that the disk size does not change much, but the in-memory size may nearly halve.  Whether this is significant enough to be worthwhile remains to be seen.
* Use zstd for blobs
  Blobs are not very common, but this is likely to significantly cut the sizes of files that do use them.
* Use dictionaries
  This has the potential of increasing compression.

## Recommendations for using the new index formats

What are the recommendations for index formats, and what pathological indexes require a different approach?

### Use inplace by default

When the inplace indexes compress well, it should give the best performance.  Branch nodes are much more efficient.  Decompression times and in-memory sizes should be notably smaller.

However, given the thought process above, it may be better to default to inplace:zstds instead of the current inplace:lz4hc3s.  Decompression times would be higher, but there should be fewer disk reads.  (Note: for batch, inplace:lz4hc3 may be better because the disk reads have less impact on throughput and faster decompression will increase throughput!)

### Indexes with many keyed fields

There are some indexes that have very large numbers of keyed fields, where all the fields are used for keyed searches.  If the trailing fields tend to be unrelated, then these indexes will not compress as well as the legacy format.

In this case, using the hybrid format would be a better approach.  The branch nodes will likely compress better, and zstd will improve the decompression speed and reduce the disk size - which will reduce the number of disk reads.

### Indexes with payload fields in the keyed portion

There are other indexes where there are fields in the keyed portion that should be in the payload.  This causes two problems:
* Branch nodes are larger than they need to be since they contain extra data.
* The inplace keyed compression does not work well on unrelated data, leading to bigger inplace indexes.

There are two solutions.  The short-term solution would be to use hybrid compression - that should improve performance and reduce the index size.  A long-term solution is to migrate fields that are never keyed to the payload.  This is currently hard to implement incrementally, but see the state of play below.

### Small key fields and large complex payload

There are some keys that have a very small keyed portion, and a very large payload.  If the compression ratio for the leaf nodes is >~20% (?) then the lz4hc3 compression may not be as good as lzw compression, leading to larger indexes.  The best approach would be to compress with 'inplace:zstds' instead.  This will reduce the decompression time and the disk size.

### Highly compressible payload

There are some indexes that are pathologically compressible - e.g., leaves take up less than 5% of the size on disk.  This can cause problems when they are read - because an 8KB page can expand to 400KB in memory.  That can cause fragmentation problems with the memory manager and means the leaf cache can only hold a few entries.

By default, the inplace compression restricts compression to a factor of 25 to help avoid these problems.  However, that means the indexes are larger with the inplace format.

Solutions:
- Use "inplace:maxCompressionFactor=50"\
  This circumvents the limit on compression to allow similar compression to legacy indexes.
- Use "inplace:zstds,maxCompressionFactor=50"\
  Same as above but also use zstd compression - if lz4hc3 does not compress as well, and disk size is critical.
- Use variable size strings in the payload.\
  The most likely cause of extreme compression is large fixed-size string fields that are mainly blank or contain short strings.  Where that is the case, it is better to trim the fields and store them as varstrings.
  This will significantly increase the effectiveness of the internal cache - because the uncompressed size will be much smaller.  It will also reduce the decompression time.
- Enhance the platform to automatically compress padded strings.\
  This functionality doesn't exist yet, but if the platform can compress strings behind the scenes you will gain the benefits without having to make any changes to the ECL (and avoid any backward compatibility issues.)

## What about remote file storage?

For batch systems, the throughput should be very similar when retrieving data from remote storage rather than locally, provided the number of concurrent requests can be increased to balance the increased read time.

For interactive systems, the extra latency is problematic.  However, an upcoming change allows data that has been read from remote storage to be stored on a local SSD/NVMe drive.  This should significantly reduce the average latency, and fewer remote requests may also reduce the maximum latency.

## Current State of Development

* Inplace\
  Implemented.  Default changed to inplace:lz4hc3.  Should consider changing the default to inplace:zstd.
  Expansion on demand, rather than on load, is implemented but performance has not been evaluated.
* Hybrid\
  PR containing the initial work is available.  Expected to be included at some point in version 9.14.x.  Read support may be backported to 9.12.x if it has minimal impact.
* Trim compression\
  A relatively new idea (see above).  Should be implementable with a week or two's work.
* Support for moving keyed fields.\
  Currently, moving a field from the key to the payload will cause problems reading the indexes.  There is work in progress to allow the ECL to specify a field in the payload that is actually in the keyed portion.  This would allow the query ECL to be updated in preparation for changing the format when the key is built.  There is a PR open, with most of the functionality working for hthor and roxie.  It needs more testing, especially for unusual edge cases (e.g., superfiles containing a mixture of translated and untranslated.)
* Zstd compression of blobs\
  No work has been done in this area, but it would be relatively trivial to use zstd to improve the decompression speed and compressibility of the data.
