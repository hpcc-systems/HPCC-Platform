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

#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "azureapi.hpp"
#include "azureapiutils.hpp"

// Forward declarations from the individual implementations
extern IFile * createAzureBlob(const char * filename);
extern IFile * createAzureFile(const char * filename);

//---------------------------------------------------------------------------------------------------------------------

class AzureAPIFileHook : public CInterfaceOf<IContainedFileHook>
{
public:
    virtual IFile * createIFile(const char *fileName) override
    {
        if (isAzureBlobName(fileName))
            return createAzureBlob(fileName);
        else if (isAzureFileName(fileName))
            return createAzureFile(fileName);
        else
            return nullptr;
    }
    virtual IAPICopyClient * getCopyApiClient(IStorageApiInfo * source, IStorageApiInfo * target) override
    {
        UNIMPLEMENTED;
    }

protected:
    static bool isAzureBlobName(const char *fileName)
    {
        return startsWith(fileName, azureBlobPrefix);
    }

    static bool isAzureFileName(const char *fileName)
    {
        return startsWith(fileName, azureFilePrefix);
    }
} *azureAPIFileHook;

extern AZUREAPI_API void installFileHook()
{
    if (!azureAPIFileHook)
    {
        azureAPIFileHook = new AzureAPIFileHook;
        addContainedFileHook(azureAPIFileHook);
    }
}

extern AZUREAPI_API void removeFileHook()
{
    if (azureAPIFileHook)
    {
        removeContainedFileHook(azureAPIFileHook);
        delete azureAPIFileHook;
        azureAPIFileHook = nullptr;
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    azureAPIFileHook = nullptr;
    return true;
}

MODULE_EXIT()
{
    if (azureAPIFileHook)
    {
        removeContainedFileHook(azureAPIFileHook);
        ::Release(azureAPIFileHook);
        azureAPIFileHook = nullptr;
    }
}
