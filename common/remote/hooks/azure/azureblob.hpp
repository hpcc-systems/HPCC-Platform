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

#pragma once

#include <string>
#include "jfile.hpp"
#include "azureapiutils.hpp"

/*
 * Direct access to files in Azure blobs
 * Provides Azure Blob Storage implementation for filenames of the form azureblob:<storagePlane>/<containerName>/<path>
 * File hooks are installed by azureapi.hpp which handles both blob and file storage
 */

struct StorageApiConfig;

// Internal factories for the azurefile library and its tests.
// Intentionally not exported; external callers should use createIFile()
// with the installed Azure file hooks.
IFile * createAzureBlob(const char * filename);
IFile * createAzureBlob(const char * filename, const DirEntry &entry);
// Direct URL factories use host-style parsing by default. Set pathStyle=true for
// account-prefixed path-style URLs such as Azurite endpoints.
// A container URL without a blob path can be used for createDirectory()/directoryFiles().
IFile * createAzureBlob(const char *blobUrl, const char *accountName, const char *accountKey,
                        bool pathStyle = false);
// Direct URL access using the configured managed identity. accountName is required.
IFile * createAzureBlob(const char *blobUrl, const char *accountName,
                        bool pathStyle = false);
// Direct URL access using managed identity and caller-supplied storage settings.
IFile * createAzureBlob(const char *blobUrl, const char *accountName, const StorageApiConfig &config,
                        bool pathStyle = false);

constexpr unsigned maxAzureBlockCount = 50000;

std::string makeAzureBlockId(const std::string &baseBlockId, unsigned sequence);

// Sequences are 1-based. commitBlockList commits the ordered prefix 1..count.
// Parallel writers may stage distinct sequences concurrently. All staging has
// finished before commit. Implementations must stop using data on return.
interface IAzureBlockOperations : extends IInterface
{
    virtual void initializeEmptyBlob() = 0;
    virtual void stageBlock(unsigned sequence, const byte *data, size32_t length) = 0;
    virtual void commitBlockList(unsigned count) = 0;
};

// A null operations argument selects real Azure operations; tests can inject a mock.
// Calls on a writer must be serialized by the caller. Concurrency 0 stages writes
// synchronously; N > 0 buffers N + 1 chunks and stages up to N concurrently.
// Workers and buffers grow on demand. Only close() commits; flush() neither commits
// nor waits for staging. close() joins workers and releases buffers even on failure.
// Buffer acquisition and staging failures prevent close() from committing a prefix.
// getStatistic() exposes buffer wait time, peak staging concurrency, and cumulative
// initialization, staging and commit times, including failed calls. Stage times
// sum across threads rather than measuring wall time.
IFile * createAzureBlob(const char *blobUrl, const char *accountName, const char *accountKey,
                        const StorageApiConfig &config, IAzureBlockOperations *operations,
                        bool pathStyle = false);
