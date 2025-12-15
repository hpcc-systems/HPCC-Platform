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
 * Modern S3 file access implementation using storage planes
 *
 * Provides S3 file access for filenames of the form s3:planeName/bucketName/path
 * Configuration is retrieved from storage plane definitions.
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

// Modern S3 file interface
extern "C" {
    extern S3FILE_API void installFileHook();
    extern S3FILE_API void removeFileHook();
    extern S3FILE_API IFile *createS3File(const char* s3FileName);
    extern S3FILE_API bool isS3FileName(const char* fileName);
};

#endif // S3FILE_HPP
