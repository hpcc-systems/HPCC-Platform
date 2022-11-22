/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef __THORFILE_HPP_
#define __THORFILE_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif
#include "eclhelper.hpp"
#include "dadfs.hpp"

THORHELPER_API void setExpiryTime(IPropertyTree & properties, unsigned expireDays);
THORHELPER_API IHThorDiskReadArg * createWorkUnitReadArg(const char * filename, IHThorWorkunitReadArg * wuRead);
THORHELPER_API void setRtlFormat(IPropertyTree & properties, IOutputMetaData * meta);

class THORHELPER_API DerivedIndexInformation
{
public:
    double getBranchCompression() const { return sizeOriginalBranches ? (double)sizeDiskBranches / sizeOriginalBranches : 0.0; }
    double getDataCompression() const { return sizeOriginalData ? (double)(sizeDiskLeaves + sizeDiskBlobs) / sizeOriginalData : 0.0; }

public:
    offset_t numLeafNodes = 0;
    offset_t numBlobNodes = 0;
    offset_t numBranchNodes = 0;
    offset_t sizeDiskLeaves = 0;
    offset_t sizeDiskBlobs = 0;
    offset_t sizeDiskBranches = 0;
    offset_t sizeOriginalData = 0;
    offset_t sizeOriginalBranches = 0;
    offset_t sizeMemoryLeaves = 0;
    offset_t sizeMemoryBranches = 0;
    bool knownLeafCount = false;
};

interface IKeyIndex;
THORHELPER_API bool checkIndexMetaInformation(IDistributedFile * file, bool force);
THORHELPER_API bool calculateDerivedIndexInformation(DerivedIndexInformation & result, IDistributedFile * file, bool force);
THORHELPER_API void mergeDerivedInformation(DerivedIndexInformation & result, const DerivedIndexInformation & other);
THORHELPER_API IKeyIndex *openKeyFile(IDistributedFilePart & keyFile);


#endif
