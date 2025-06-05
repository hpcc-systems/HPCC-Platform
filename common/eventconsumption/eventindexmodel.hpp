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
#include <set>
#include <unordered_map>

// The index event model configuration conforms to this structure:
//   kind: index-event
//   storage:
//     plane:
//     - name: unique, non-empty, identifier
//       readTime: nanosecond time to read one page
//     file:
//     - path: empty or unique index file path
//       plane: name of place in which the file resides
//
// - `kind` is optional; as the first model the value is implied.
// - The first `plane` declared is assumed to be the default plane for files not explicitly assigned
//   an alternate choice.
// - `file` is optional; omission implies all files exist in the default plane.
// - `file/path` is optional; omission, or empty, assigns the storage plane for all files not
//   explicitly configured.
// - `file/planes` is optional; omission, or empty, implies the file resides in the default storage
//   plane.

// Encapsulation of all modeled information about given file page. Note that the file path is not
// included as the model does not retain that information.
struct ModeledPage
{
    __uint64 fileId{0};
    __uint64 offset{0};
    __uint64 readTime{0};
};

// Encapsulation of the configuration's `storage` element.
class Storage
{
private:
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
        StringAttr path;
        const Plane* plane{nullptr};

    public:
        File() = default;
        File(const char* _path, const Plane& _plane) : path(_path), plane(&_plane) {}

        // Returns plane unless the page exceptions are defined and the offset is within an
        // exception, in which case the exception's plane would be returned.
        const Plane& lookupPlane(__uint64 offset) const;
    };
    friend bool operator < (const File& left, const File& right); // required for set insertion
    friend bool operator < (const File& left, const char* right); // required to search a set by path
    friend bool operator < (const char* left, const File& right); // required to search a set by path

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

    // Fill in the page data with storage information known about the file and offset. A file ID
    // not previously obseved will use information from the default file specification.
    void describePage(__uint64 fileId, __uint64 offset, ModeledPage& page) const;

private:
    void configurePlanes(const IPropertyTree& config);
    const Plane& lookupPlane(const char* name, const char* forFile) const;
    void configureFiles(const IPropertyTree& config);
    const File& lookupFile(const char* path) const;
    const File& lookupFile(__uint64 fileId) const;

private:
    Planes planes;
    const Plane* defaultPlane{nullptr};
    Files configuredFiles;
    ObservedFiles observedFiles;
};
