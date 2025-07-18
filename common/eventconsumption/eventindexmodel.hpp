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

// The index event model configuration conforms to this structure:
//   kind: index-event
//   storage:
//     cache-read: nanosecond time to read one page
//     cache-capacity: maximum bytes to use for the cache
//     plane:
//     - name: unique, non-empty, identifier
//       readTime: nanosecond time to read one page
//     file:
//     - path: empty or unique index file path
//       plane: name of place in which the file resides, if different from the default
//       branchPlane: name of place in which index branches reside, if different from the default
//       leafPlane: name of place in which index leaves reside, if different from the default
//   expansion:
//     node:
//     - kind: node kind
//       sizeFactor: floating point multiplier for estimated node size
//       sizeToTimeFactor: floating point multiplier for estimated node expansion time
//
// - `kind` is optional; as the first model the value is implied.
// - `cache-read` is optional; if zero, empty, or omitted, the cache is disabled.
// - `cache-capacity` is optional; if zero, empty, or omitted, the cache is unlimited.
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
// - `expansion` is optional; omission prevents any expansion modeling.
// - `expansion/node` is optional; omission prevents estimating algorithm performance. If present,
//   one instance for each type of index node is required.
// - `expansion/node/kind` is a required choice of `branch` or `leaf`.
// - `expansion/node/sizeFactor` is a required positive floating point multiplier for algorithm
//   performance estimation. The on disk page size is multiplied by this value.
// - `expansion/node/sizeToTimeFactor` is a required positive floating point multiplier for
//   algorithm performance estimation. The estimated size is multipled by this factor to estimate
//   the expansion time.

// Encapsulation of all modeled information about given file page. Note that the file path is not
// included as the model does not retain that information.
struct ModeledPage
{
    __uint64 fileId{0};
    __uint64 offset{0};
    __uint64 nodeKind{1};
    __uint64 readTime{0};
    __uint64 expandedSize{0};
    __uint64 expansionTime{0};
    bool expansionIsEstimated{false};
};

// Encapsulation of the configuration's `storage` element.
class Storage
{
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

    class PageCache
    {
    public:
        static constexpr __uint64 DefaultPageSize = 8192; // 8K page size
        class Key
        {
        public:
            __uint64 fileId{0};
            __uint64 offset{0};
            Key(__uint64 _fileId, __uint64 _offset) : fileId(_fileId), offset(_offset) {}
            bool operator == (const Key& other) const { return fileId == other.fileId && offset == other.offset; }
        };
        class KeyEqual
        {
        public:
            bool operator () (const Key& left, const Key& right) const
            {
                return ((left == right) || (left.fileId == right.fileId && left.offset == right.offset));
            }
        };
        class KeyHash
        {
        public:
            std::size_t operator()(const Key& key) const
            {
                struct { __uint64 f; __uint64 s; } buf{key.fileId, key.offset};
                return hashc_fnv1a((byte*)&buf, sizeof(buf), fnvInitialHash32);
            }
        };
        class Value
        {
        public:
            const Key     key; // enables mapping from MRU to hash table on MRU removals
            struct Value* prev{nullptr};
            struct Value* next{nullptr};
            Value(const Key& _key) : key(_key) {}
        };
        using Hash = std::unordered_map<const Key, std::unique_ptr<Value>, KeyHash, KeyEqual>;
    public:
        void configure(const IPropertyTree& config);
        inline __uint64 getReadTimeIfExists(__uint64 fileId, __uint64 offset) { return getReadTimeIfExists({fileId, offset}); }
        __uint64 getReadTimeIfExists(const Key& key);
        inline bool insert(__uint64 fileId, __uint64 offset) { return insert({fileId, offset}); }
        bool insert(const Key& key);
    private:
        void reserve(__uint64 size);
    public:
        __uint64 used{0};
        __uint64 capacity{0};
        __uint64 readTime{0};
        Hash hash;
        DListOf<Value> mru;
    };

    // An ordered set of storage planes, with key transparency enabled.
    using Planes = std::set<Plane, std::less<>>;
    // An ordered set of file specifications, with key transparency enabled.
    using Files = std::set<File, std::less<>>;
    // An association of file IDs to file specifications.
    using ObservedFiles = std::unordered_map<__uint64, const File*>;

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

// Encapsulation of the configuration's `expansion` element.
class Expansion
{
public:
    static constexpr size_t NumKinds = 2;
    struct Estimate
    {
        __uint64 size{0};
        __uint64 time{0};
    };

public:
    void configure(const IPropertyTree& config);
    void describePage(const CEvent& event, ModeledPage& page) const;

public:
    Estimate estimates[NumKinds];
    bool estimating{false};
};
