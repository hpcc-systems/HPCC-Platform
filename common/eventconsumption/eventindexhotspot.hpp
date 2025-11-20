/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#pragma once

#include "eventconsumption.h"
#include "eventindex.hpp"
#include "eventvisitor.h"
#include <set>

#define defaultBucketCount 10
constexpr static byte bucketArrayBits = 10; // 1K buckets per activity key
constexpr static unsigned short bucketArrayMask = ((1 << bucketArrayBits) - 1);

constexpr static byte defaultPageBits = 13; // 8K page size

using page_type = offset_t;
using bucket_type = offset_t;
using stat_type = __uint64;

// Convert index file offset to page number.
inline page_type offset2page(offset_t offset, byte pageBits)
{
    return offset >> pageBits;
}

// Convert index file page number to bucket number.
inline bucket_type page2bucket(page_type page, byte granularityBits)
{
    return page >> granularityBits;
}

// Convert index file bucket number to lowest page number in the bucket.
inline page_type bucket2page(bucket_type bucket, byte granularityBits)
{
    return bucket << granularityBits;
}

// Convert index file page number to file position.
inline offset_t page2Offset(page_type page, byte pageBits)
{
    return page << pageBits;
}

// Interface of an observer of index file activity.
interface IBucketVisitor : IInterface
{
    // Begin a hotspot analysis activity.
    virtual void begin(const std::set<EventType>& observedEvents, unsigned granularity) = 0;
    // Perform any setup required before visiting hotspot activity buckets for one file.
    virtual void arrive(unsigned id, const char* path) = 0;
    // Observe a hotspot activity bucket.
    virtual void visitBucket(bucket_type bucket, NodeKind nodeKind, stat_type stat) = 0;
    // Perform any cleanup, or other post-processing, after visting all hotspot activity buckets.
    virtual void depart() = 0;
    // End a hotspot analysis activity.
    virtual void end() = 0;
};

extern IBucketVisitor* createTopBucketVisitor(IBufferedSerialOutputStream& out, byte limit);
extern IBucketVisitor* createAllBucketVisitor(IBufferedSerialOutputStream& out);