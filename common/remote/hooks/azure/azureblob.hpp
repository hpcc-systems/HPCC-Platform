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

#ifndef AZURE_BLOB_HPP
#define AZURE_BLOB_HPP

#include "jfile.hpp"

/*
 * Direct access to files in Azure blobs
 * Provides Azure Blob Storage implementation for filenames of the form azureblob:<storagePlane>/<containerName>/<path>
 * File hooks are installed by azureapi.hpp which handles both blob and file storage
 */

IFile * createAzureBlob(const char * filename);

#endif
