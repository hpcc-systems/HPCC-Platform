/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef S3FILE_HPP
#define S3FILE_HPP

#include "jfile.hpp"
#include "jptree.hpp"

#ifdef S3FILE_EXPORTS
#define S3FILE_API DECL_EXPORT
#else
#define S3FILE_API DECL_IMPORT
#endif

/*
 * Modern S3 file access implementation
 *
 * Features:
 * - Uses latest AWS C++ SDK with modern patterns
 * - Thread-safe operations with improved error handling
 * - Configurable read-ahead buffering and caching
 * - Support for multipart uploads for large files
 * - Proper credential management via AWS credential chain
 * - Comprehensive logging and metrics
 * - Support for S3-compatible services (MinIO, etc.)
 */

// Forward declarations
class StringAttr;

// Configuration structure for S3 operations
struct S3Config
{
    StringAttr region;
    StringAttr endpoint;  // For S3-compatible services
    bool useSSL = true;
    bool useVirtualHosting = true;
    size32_t readAheadSize = 4 * 1024 * 1024; // 4MB default
    size32_t writeBufferSize = 5 * 1024 * 1024; // 5MB minimum for multipart
    unsigned maxRetries = 3;
    unsigned timeoutMs = 30000; // 30 seconds

    S3Config() = default;
    S3Config(IPropertyTree* _config);
    void loadFromConfig(IPropertyTree* _config);
    bool operator==(const S3Config& other) const
    {
        return (region == other.region) &&
               (endpoint == other.endpoint) &&
               (useSSL == other.useSSL) &&
               (useVirtualHosting == other.useVirtualHosting) &&
               (readAheadSize == other.readAheadSize) &&
               (writeBufferSize == other.writeBufferSize) &&
               (maxRetries == other.maxRetries) &&
               (timeoutMs == other.timeoutMs);
    }
};

// Hash specialization for S3Config to use in unordered_map
namespace std {
    template<>
    struct hash<S3Config>
    {
        size_t operator()(const S3Config& config) const noexcept
        {
            unsigned h = 0;
            if (config.region.str())
                h = hashc((const unsigned char*)config.region.str(), config.region.length(), h);
            if (config.endpoint.str())
                h = hashc((const unsigned char*)config.endpoint.str(), config.endpoint.length(), h);
            h = hashvalue((unsigned)config.useSSL, h);
            h = hashvalue((unsigned)config.useVirtualHosting, h);
            h = hashvalue(config.readAheadSize, h);
            h = hashvalue(config.writeBufferSize, h);
            h = hashvalue(config.maxRetries, h);
            h = hashvalue(config.timeoutMs, h);
            return h;
        }
    };
}

// Modern S3 file interface
class S3File;

// Forward declarations for implementation classes
class S3FileReadIO;
class S3FileWriteIO;

extern "C" {
    extern S3FILE_API void installFileHook();
    extern S3FILE_API void removeFileHook();
    extern S3FILE_API IFile *createS3File(const char* s3FileName);
    extern S3FILE_API bool isS3FileName(const char* fileName);

};


#endif // S3FILE_HPP
