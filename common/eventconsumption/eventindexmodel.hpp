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

#include "eventmodeling.h"
#include "jqueue.hpp"
#include <memory>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

// Node expansion strategy indicator. Each value reflects the expected stategy used during event
// recording and the strategy applied by the index model.
// - OnLoad: event attributes are assumed to reflect node expansion on load both  before and after
//           modeling.
// - OnLoadToOnDemand: event attributes are assumed to reflect node expansion on load before
//           modeling, and on-demand expansion after modeling.
// - OnDemand: event attributes are assumed to reflect node expansion on-demand both before and
//           after modeling.
//
// Note that no value for on-demand on input and on-load on output is defined. Transformation of
// InMemorySize and ExpandTime attributes could only be estimated before visiting an IndexPayload
// event for the node. With estimation an optional configuration, and IndexPayload events not
// guaranteed for each loaded node, the transformation can't be reliably modeled.
enum class ExpansionMode
{
    OnLoad,
    OnLoadToOnDemand,
    OnDemand
};

// The index event model configuration conforms to this structure:
//   kind: index-event
//   storage:
//     cacheReadTime: nanosecond time to read one page
//     cacheCapacity: maximum bytes to use for the cache
//     plane:
//     - name: unique, non-empty, identifier
//       readTime: nanosecond time to read one page
//     file:
//     - path: empty or unique index file path
//       plane: name of place in which the file resides, if different from the default
//       branchPlane: name of place in which index branches reside, if different from the default
//       leafPlane: name of place in which index leaves reside, if different from the default
//   memory:
//     node:
//     - kind: node kind
//       sizeFactor: floating point multiplier for estimated node size
//       sizeToTimeFactor: floating point multiplier for estimated node expansionMode time
//       expansionMode: choice of `ll`, `ld`, or `dd`
//       cacheCapacity: maximum bytes to use for the cache
//     observed:
//     - FileId: unique index file identifier
//       FileOffset: index page offset
//       NodeKind: node kind
//       InMemorySize: size of the node in bytes
//       ExpandTime: time in nanoseconds to expand the node
//
// - `kind` is optional; as the first model the value is implied.
// - `cacheReadTime` is optional; if zero, empty, or omitted, the cache is disabled.
// - `cacheCapacity` is optional; if zero, empty, or omitted, the cache is unlimited.
// - The first `plane` declared is assumed to be the default plane for files not explicitly assigned
//   an alternate choice.
// - `file` is optional; omission implies all files exist in the default plane.
// - `file/path` is optional; omission, or empty, assigns the storage plane for all files not
//   explicitly configured.
// - `file/plane` is optional; omission, or empty, implies the file resides in the default storage
//   plane.
// - `file/branchPlane` is optional; omission, or empty, implies the file's index branches reside in
//   the file's default storage plane.
// - `file/leafPlane` is optional; omission, or empty, implies the file's index leaves reside in the
//   file's default storage plane.
// - `memory` is optional; omission prevents any memory modeling.
// - `memory/node` is optional; omission prevents estimating algorithm performance. If present,
//   one instance for each type of index node is required.
// - `memory/node/kind` is a required choice of `branch` or `leaf`.
// - `memory/node/sizeFactor` is a required positive floating point multiplier for algorithm
//   performance estimation. The on disk page size is multiplied by this value.
// - `memory/node/sizeToTimeFactor` is a required positive floating point multiplier for
//   algorithm performance estimation. The estimated size is multipled by this factor to estimate
//   the expansion time.
// - `expansion/node/mode` is optional; omission and empty are equivalent to `ll`. The value is
//   ignored for branch nodes, which are always treated as `ll`. Accepted options are:
//   - `ll`: model input and output are both on-load
//   - `ld`: model input is on-load and output is on-demand
//   - `dd`: model input and output are both on-demand
// - `memory/observed` is an optional mechanism to pre-populate the memory caches. It enables
//   unit test case scenarios to be set up without requiring explicit events to be visited.
// - `memory/observed/NodeKind` is expected and corresponds to input event NodeKind values.
// - `memory/observed/FileId` is expected only when history is used for the node kind.
// - `memory/observed/FileOffset` is expected only when history is used for the node kind.
// - `memory/observed/InMemorySize` is expected only when the node cache for the node kind is
//   enabled, which implies that history is also used for the node kind.
// - `memory/observed/ExpandTime` is expected only when the node cache for the node kind is
//   enabled, which implies that history is also used for the node kind.

// Encapsulation of all modeled information about given file page. Note that the file path is not
// included as the model does not retain that information.
struct ModeledPage
{
    struct Size
    {
        __uint64 size{0};
        bool estimated{false}; // included for unit tests
    };
    __uint64 fileId{0};
    __uint64 offset{0};
    __uint64 nodeKind{1};
    __uint64 readTime{0};
    Size compressed;
    Size expanded;
    __uint64 expansionTime{0};
    ExpansionMode expansionMode{ExpansionMode::OnLoad};
    bool cacheHit{false};
};

class IndexHashKey
{
public:
    __uint64 fileId{0};
    __uint64 offset{0};
    IndexHashKey() = default;
    IndexHashKey(__uint64 _fileId, __uint64 _offset) : fileId(_fileId), offset(_offset) {}
    IndexHashKey(const CEvent& event) : fileId(event.queryNumericValue(EvAttrFileId)), offset(event.queryNumericValue(EvAttrFileOffset)) {}
    bool operator == (const IndexHashKey& other) const { return fileId == other.fileId && offset == other.offset; }
};

class IndexHashKeyHash
{
public:
    std::size_t operator()(const IndexHashKey& key) const
    {
        return hashc_fnv1a((byte*)&key, sizeof(key), fnvInitialHash32);
    }
};

interface IndexMRUCacheReporter
{
    virtual void reportDropped(const IndexHashKey& key, __uint64 size) = 0;
};

class IndexMRUNullCacheReporter : public IndexMRUCacheReporter
{
public:
    virtual void reportDropped(const IndexHashKey& key, __uint64 size) override {}
};

// Abstract base class implementing a most recently used cache of index pages for modeling purposes.
// The cache tracks which index pages are in memory. Subclasses associate a relevant size with each
// page. No page data is stored in the cache.
//
// For example, a storage page cache assumes a fixed page size while a node cache relies on
// expension data for each page.
//
// Subclasses must implement:
// - enabled: returns true if the cache is configured for use in the model
// - size: returns the size of a page in bytes
// - description: returns a string description of the cache for use in error messages
class IndexMRUCache
{
public:
    class Value
    {
    public:
        const IndexHashKey key; // enables mapping from MRU to hash table on MRU removals
        struct Value* prev{nullptr};
        struct Value* next{nullptr};
        Value(const IndexHashKey& _key) : key(_key) {}
    };
    using Hash = std::unordered_map<const IndexHashKey, std::unique_ptr<Value>, IndexHashKeyHash>;
public:
    virtual ~IndexMRUCache() = default;
    virtual void configure(const IPropertyTree& config);
    virtual bool enabled() const = 0;
    virtual void resize(__uint64 newCapacity);
    inline bool exists(__uint64 fileId, __uint64 offset) { return exists({fileId, offset}); }
    bool exists(const IndexHashKey& key);
    inline bool insert(__uint64 fileId, __uint64 offset, IndexMRUCacheReporter& reporter) { return insert({fileId, offset}, reporter); }
    bool insert(const IndexHashKey& key, IndexMRUCacheReporter& reporter);
protected:
    virtual __uint64 size(const IndexHashKey& key) = 0;
    virtual const char* description() const = 0;
private:
    void reserve(__uint64 needed, IndexMRUCacheReporter& reporter);
public:
    __uint64 used{0};
    __uint64 capacity{0};
    Hash entries;
    DListOf<Value> mru;
};

interface IIndexMRUCacheContainer
{
    virtual bool isCacheEnabled() const = 0;
    virtual __uint64 getCacheSize() const = 0;
    virtual void resizeCache(__uint64 newSize) = 0;
};

// Encapsulation of the configuration's `storage` element.
class Storage : public IIndexMRUCacheContainer
{
public:
    static constexpr __uint64 DefaultPageSize = 8192; // 8K page size
private:
    friend class IndexModelStorageTest;

    // Encapsulation of modeled information describing a named storage plane.
    struct Plane
    {
        StringAttr name;
        __uint64 readTime{0};
    };
    friend bool operator < (const Plane& left, const Plane& right); // required for set insertion
    friend bool operator < (const Plane& left, const char* right);  // required to search a set by name
    friend bool operator < (const char* left, const Plane& right);  // required to search a set by name

    // Encapsulation of modeled information describing either a single named file or all unnamed
    // files.
    class File
    {
    public:
        static constexpr size_t NumPlanes = 2;
    public:
        StringAttr path;
        const Plane* planes[NumPlanes]{nullptr,};

    public:
        File() = default;
        File(const char* _path, const Plane& _plane);

        // Returns the storage plane associated with the given node kind.
        const Plane& lookupPlane(__uint64 nodeKind) const;
    };
    friend bool operator < (const File& left, const File& right); // required for set insertion
    friend bool operator < (const File& left, const char* right); // required to search a set by path
    friend bool operator < (const char* left, const File& right); // required to search a set by path

    class PageCache : public IndexMRUCache
    {
    public:
        virtual void configure(const IPropertyTree& config) override;
        virtual bool enabled() const override;
        inline __uint64 getReadTimeIfExists(__uint64 fileId, __uint64 offset) { return getReadTimeIfExists({fileId, offset}); }
        __uint64 getReadTimeIfExists(const IndexHashKey& key);
    protected:
        virtual __uint64 size(const IndexHashKey& key) override;
        virtual const char* description() const override;
    public:
        __uint64 readTime{0};
    };

    // An ordered set of storage planes, with key transparency enabled.
    using Planes = std::set<Plane, std::less<>>;
    // An ordered set of file specifications, with key transparency enabled.
    using Files = std::set<File, std::less<>>;
    // An association of file IDs to file specifications.
    using ObservedFiles = std::unordered_map<__uint64, const File*>;

public:
    virtual bool isCacheEnabled() const override { return cache.enabled(); }
    virtual __uint64 getCacheSize() const override { return cache.capacity; }
    virtual void resizeCache(__uint64 newSize) override { cache.resize(newSize); }

public:
    // Extract storage plane and file specifications from the configuration.
    // - at least one storage plane is required
    // - files are optional; a default for all paths not configured is assumed unless a file
    //   with no path is configured
    void configure(const IPropertyTree& config);

    // Associate an onserved file ID with a configured file specification. If the path matches a
    // configured file specification exactly, future appearances of the file ID will resolve to
    // that specification. If no such match is found, the file ID will resolve to the default file
    // specification. Pattern matching, to associate multiple file IDs with a single, non-default,
    // file specification, is not supported.
    void observeFile(__uint64 fileId, const char* path);

    // Fill in the page data with storage information known about the file and offset. An enabled
    // page cache will be updated when the page was not the most recently used. A file ID not
    // previously obseved will use information from the default file specification.
    void useAndDescribePage(__uint64 fileId, __uint64 offset, __uint64 nodeKind, ModeledPage& page);

private:
    void configurePlanes(const IPropertyTree& config);
    const Plane& lookupPlane(const char* name, const char* forFile) const;
    void configureFiles(const IPropertyTree& config);
    const File& lookupFile(const char* path) const;
    const File& lookupFile(__uint64 fileId) const;

private:
    PageCache cache;
    Planes planes;
    const Plane* defaultPlane{nullptr};
    Files configuredFiles;
    ObservedFiles observedFiles;
};

// Encapsulation of the configuration's `memory` element.
class MemoryModel : public IIndexMRUCacheContainer
{
public:
    static constexpr size_t NumKinds = 2;
    struct Estimate
    {
        __uint64 compressed{8'192}; // all nodes assumed to be 8K on disk
        __uint64 expanded{0};
        __uint64 time{0};
    };
    class ActualValue
    {
    public:
        __uint64 size{0};
        __uint64 time{0};
    };
    using ActualHistory = std::unordered_map<IndexHashKey, ActualValue, IndexHashKeyHash>;
    using EstimatedHistory = std::unordered_set<IndexHashKey, IndexHashKeyHash>;
    class NodeCache : public IndexMRUCache
    {
    public:
        virtual void configure(const IPropertyTree& config) override;
    public:
        virtual bool enabled() const override;
        virtual __uint64 size(const IndexHashKey& key) override;
        virtual const char* description() const override;
    public:
        NodeCache(MemoryModel& _parent, __uint64 _kind);
    private:
        MemoryModel& parent;
        __uint64 kind{0};
    };

public:
    virtual bool isCacheEnabled() const override;
    virtual __uint64 getCacheSize() const override;
    virtual void resizeCache(__uint64 newSize) override { UNIMPLEMENTED; }

public:
    MemoryModel();
    void configure(const IPropertyTree& config);

    inline bool caching(const CEvent& event) const { return caching(event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1); }
    bool caching(__uint64 nodeKind) const { assertex(nodeKind < NumKinds); return caches[nodeKind].enabled(); }

    // Record, if necessary, that indicated index node has been observed. Intended to be called only
    // for IndexCacheHit and IndexCacheMiss events, but not enforced.
    void observePage(const CEvent& event);

    // Determines if the indicated index node has been observed in a previous event. Called for index
    // events that do not include data needed for historical caching, such as IndexEviction and
    // IndexPayload.
    bool checkObservedPage(const CEvent& event) const;

    // Determines if the indicated index node has been observed in a previous event. If it has then
    // the historical cache may be updated with this event's data. Called for index events that
    // contain historical data that might not have been available in previous events, specifically
    // IndexLoad.
    bool refreshObservedPage(const CEvent& event);

    // Applies the memory configuration to the indicated index node, updading `page` with the
    // modeled sizes (both compressed and expanded) and the time to expand.
    void describePage(const CEvent& event, ModeledPage& page) const;

    // Update the node cache with the indicated page. The page must have been described first.
    void cachePage(ModeledPage& page, IndexMRUCacheReporter& reporter);

protected:
    inline bool usingHistory(const CEvent& event) const { return usingHistory(event.hasAttribute(EvAttrNodeKind) ? event.queryNumericValue(EvAttrNodeKind) : 1); }
    inline bool usingHistory(__uint64 nodeKind) const { assertex(nodeKind < NumKinds); return ((ExpansionMode::OnLoadToOnDemand == modes[nodeKind] && !estimating) || caches[nodeKind].enabled()); }
    void refreshPage(ActualHistory::iterator& it, const CEvent& event);
    __uint64 nodeEntrySize(const IndexHashKey& key, __uint64) const;
public:
    ActualHistory actualHistory;
    EstimatedHistory estimatedHistory;
    Estimate estimates[NumKinds];
    ExpansionMode modes[NumKinds] = {ExpansionMode::OnLoad,};
    bool estimating{false};
    mutable NodeCache caches[NumKinds]; // testing presence in the cache can modify the cache
};
