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

#include "evtool.hpp"
#include "jevent.hpp"
#include "jstream.hpp"

constexpr static byte defaultPageBits = 13; // 8K page size
constexpr static byte defaultGranularityBits = 0; // 1 page per bucket

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

// Index node kind(s) found in a bucket.
enum BucketKind
{
    BucketBranch, // correlates to nodeKind in the event recorder interface
    BucketLeaf, // correlates to nodeKind in the event recorder interface
    BucketAmbiguous, // inactive between leaves and branches, or both leaf and branch activity
    BucketKindMax
};

// Interface of an observer of index file activity.
interface IBucketVisitor : IInterface
{
    // Begin a hotspot analysis activity.
    virtual void begin(EventType observedEvent, unsigned granularity) = 0;
    // Perform any setup required before visiting hotspot activity buckets for one file.
    virtual void arrive(unsigned id, const char* path) = 0;
    // Observe a hotspot activity bucket.
    virtual void visitBucket(bucket_type bucket, BucketKind bucketKind, stat_type stat) = 0;
    // Perform any cleanup, or other post-processing, after visting all hotspot activity buckets.
    virtual void depart() = 0;
    // End a hotspot analysis activity.
    virtual void end() = 0;
    // Does the visitor want ambiguous buckets, where a single bucket represents both leaves and
    // branches? If not, the bucket will be reported twice, once as a leaf and once as a branch.
    virtual bool wantAmbiguous() const = 0;
};

// Abstract base implementation of `IBucketVisitor` that outputs bucket activity to a buffered
// stream as YAML-formatted text. Each subclass performs a specific analysis of the bucket
// activity data.
class CStreamingBucketVisitor : public CInterfaceOf<IBucketVisitor>
{
protected: // Abstract interface
    // Append a brief description of the analysis performed to the buffer.
    virtual void appendAnalysis(StringBuffer& lines) const = 0;

public: // IBucketVisitor
    virtual void begin(EventType observedEvent, unsigned granularity) override;
    virtual void arrive(unsigned id, const char* path) override;
    virtual void depart() override;
    virtual void end() override;
public:
    CStreamingBucketVisitor(IBufferedSerialOutputStream& _out);
protected:
    struct Stats
    {
        stat_type minEventsPerBucket;
        stat_type maxEventsPerBucket;
        __uint64 totalEvents;
        bucket_type totalBuckets;

        Stats();
        void recordEvent(stat_type value);
        void clear();
        StringBuffer& toString(StringBuffer& lines, byte indent, const char* label = nullptr) const;
    };
    Linked<IBufferedSerialOutputStream> out;
    Stats stats[BucketKindMax]; // keep statistics for each bucket kind
    unsigned currentGranularity{0};
    bool arrived{false};
};

extern IEvToolCommand* createIndexSummaryCommand();
extern IEvToolCommand* createIndexHotspotCommand();
