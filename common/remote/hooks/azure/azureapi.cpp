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

using namespace Azure::Storage;
using namespace Azure::Storage::Files;
using namespace Azure::Storage::Blobs;

// Forward declarations from the individual implementations
extern IFile * createAzureBlob(const char * filename);
extern IFile * createAzureFile(const char * filename);

//---------------------------------------------------------------------------------------------------------------------

class AzureAPICopyClientBase : public CInterfaceOf<IAPICopyClientOp>
{
    ApiCopyStatus status = ApiCopyStatus::NotStarted;
    virtual void doStartCopy(const char * source) = 0;
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) = 0;
    virtual void doAbortCopy() = 0;
    virtual void doDelete() = 0;

public:
    virtual void startCopy(const char * source) override
    {
        try
        {
            doStartCopy(source);
            status = ApiCopyStatus::Pending;
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            IERRLOG("AzureFileClient startCopy failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
            status = ApiCopyStatus::Failed;
            throw makeStringException(MSGAUD_operator, -1, e.ReasonPhrase.c_str());
        }
    }
    virtual ApiCopyStatus getProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        dateTime.clear();
        outputLength=0;

        if (status!=ApiCopyStatus::Pending)
            return status;
        try
        {
            status = doGetProgress(dateTime, outputLength);
        }
        catch(const Azure::Core::RequestFailedException& e)
        {
            IERRLOG("Transfer using API .Poll() failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
            status = ApiCopyStatus::Failed;
        }
        return status;
    }
    virtual ApiCopyStatus abortCopy() override
    {
        if (status==ApiCopyStatus::Aborted || status==ApiCopyStatus::Failed)
            return status;
        int64_t outputLength;
        CDateTime dateTime;
        status = getProgress(dateTime, outputLength);
        switch (status)
        {
        case ApiCopyStatus::Pending:
            try
            {
                doAbortCopy();
                status = ApiCopyStatus::Aborted;
            }
            catch(const Azure::Core::RequestFailedException& e)
            {
                IERRLOG("Abort copy operation failed: %s (code %d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
                status = ApiCopyStatus::Failed;
            }
            // fallthrough to delete any file remnants
            [[fallthrough]];
        case ApiCopyStatus::Success:
            // already copied-> need to delete
            try
            {
                doDelete();
                status = ApiCopyStatus::Aborted;
            }
            catch(const Azure::Core::RequestFailedException& e)
            {
                // Ignore exceptions as file may not exist (may have been aborted already)
            }
            break;
        }
        return status;
    }
    virtual ApiCopyStatus getStatus() const override
    {
        return status;
    }
};

class AzureFileClient : public AzureAPICopyClientBase
{
    std::unique_ptr<Shares::ShareFileClient> fileClient;
    Shares::StartFileCopyOperation fileCopyOp;

    virtual void doStartCopy(const char * source) override
    {
        fileCopyOp = fileClient->StartCopy(source);
    }
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        fileCopyOp.Poll();
        Shares::Models::FileProperties props = fileCopyOp.Value();
        dateTime.setString(props.LastModified.ToString().c_str());
        outputLength = props.FileSize;
        Shares::Models::CopyStatus tstatus = props.CopyStatus.HasValue()?props.CopyStatus.Value():(Shares::Models::CopyStatus::Pending);
        if (tstatus==Shares::Models::CopyStatus::Success) // FYI. CopyStatus is an object so can't use switch statement
            return ApiCopyStatus::Success;
        else if (tstatus==Shares::Models::CopyStatus::Pending)
            return ApiCopyStatus::Pending;
        else if (tstatus==Shares::Models::CopyStatus::Aborted)
            return ApiCopyStatus::Aborted;
        return ApiCopyStatus::Failed;
    }
    virtual void doAbortCopy() override
    {
        if (fileCopyOp.HasValue() && fileCopyOp.Value().CopyId.HasValue())
            fileClient->AbortCopy(fileCopyOp.Value().CopyId.Value().c_str());
        else
            IERRLOG("AzureFileClient::AbortCopy() failed: CopyId is empty");
    }
    virtual void doDelete() override
    {
        fileClient->Delete();
    }
public:
    AzureFileClient(const char *target)
    {
        fileClient.reset(new Shares::ShareFileClient(target));
    }
};

class AzureBlobClient : public AzureAPICopyClientBase
{
    std::unique_ptr<BlobClient> blobClient;
    StartBlobCopyOperation blobCopyOp;

    virtual void doStartCopy(const char * source) override
    {
        blobCopyOp = blobClient->StartCopyFromUri(source);
    }
    virtual ApiCopyStatus doGetProgress(CDateTime & dateTime, int64_t & outputLength) override
    {
        blobCopyOp.Poll();
        Models::BlobProperties props = blobCopyOp.Value();
        dateTime.setString(props.LastModified.ToString().c_str());
        outputLength = props.BlobSize;
        Blobs::Models::CopyStatus tstatus = props.CopyStatus.HasValue()?props.CopyStatus.Value():(Blobs::Models::CopyStatus::Pending);
        if (tstatus==Blobs::Models::CopyStatus::Success) // FYI. CopyStatus is an object so can't use switch statement
            return ApiCopyStatus::Success;
        else if (tstatus==Blobs::Models::CopyStatus::Pending)
            return ApiCopyStatus::Pending;
        else if (tstatus==Blobs::Models::CopyStatus::Aborted)
            return ApiCopyStatus::Aborted;
        return ApiCopyStatus::Failed;
    }
    virtual void doAbortCopy() override
    {
        if (blobCopyOp.HasValue() && blobCopyOp.Value().CopyId.HasValue())
            blobClient->AbortCopyFromUri(blobCopyOp.Value().CopyId.Value().c_str());
        else
            IERRLOG("AzureBlobClient::AbortCopy() failed: CopyId is empty");
    }
    virtual void doDelete() override
    {
        blobClient->Delete();
    }
public:
    AzureBlobClient(const char * target)
    {
        blobClient.reset(new BlobClient(target));
    }
};


class CAzureApiCopyClient : public CInterfaceOf<IAPICopyClient>
{
public:
    CAzureApiCopyClient(IStorageApiInfo *_sourceApiInfo, IStorageApiInfo *_targetApiInfo): sourceApiInfo(_sourceApiInfo), targetApiInfo(_targetApiInfo)
    {
        dbgassertex(isAzureBlob(sourceApiInfo->getStorageType())||isAzureFile(sourceApiInfo->getStorageType()));
        dbgassertex(isAzureBlob(targetApiInfo->getStorageType())||isAzureFile(targetApiInfo->getStorageType()));
    }
    virtual const char * name() const override
    {
        return "Azure API copy client";
    }
    static bool canCopy(IStorageApiInfo *_sourceApiInfo, IStorageApiInfo *_targetApiInfo)
    {
        if (_sourceApiInfo && _targetApiInfo)
        {
            const char * stSource = _sourceApiInfo->getStorageType();
            const char * stTarget = _targetApiInfo->getStorageType();
            if (stSource && stTarget)
            {
                if ((isAzureFile(stSource) || isAzureBlob(stSource))
                    && (isAzureFile(stTarget) || isAzureBlob(stTarget)))
                return true;
            }
        }
        return false;
    }
    virtual IAPICopyClientOp * startCopy(const char *srcPath, unsigned srcStripeNum,  const char *tgtPath, unsigned tgtStripeNum) const override
    {
        StringBuffer targetURI;
        getAzureURI(targetURI, tgtStripeNum,  tgtPath, targetApiInfo);
        Owned<IAPICopyClientOp> apiClient;
        if (isAzureFile(targetApiInfo->getStorageType()))
            apiClient.setown(new AzureFileClient(targetURI.str()));
        else
            apiClient.setown(new AzureBlobClient(targetURI.str()));

        StringBuffer sourceURI;
        getAzureURI(sourceURI, srcStripeNum, srcPath, sourceApiInfo);
        apiClient->startCopy(sourceURI.str());
        return apiClient.getClear();
    }
protected:
    void getAzureURI(StringBuffer & uri, unsigned stripeNum, const char *filePath, const IStorageApiInfo *apiInfo) const
    {
        const char *accountName = apiInfo->queryStorageApiAccount(stripeNum);
        uri.appendf("https://%s", accountName);

        if (isAzureFile(apiInfo->getStorageType()))
            uri.append(".file");
        else
            uri.append(".blob");
        uri.append(".core.windows.net/");

        StringBuffer tmp, token;
        const char * container = apiInfo->queryStorageContainerName(stripeNum);
        uri.appendf("%s%s%s", container, encodeURL(tmp, filePath).str(), apiInfo->getSASToken(stripeNum, token).str());
    }
    static inline bool isAzureFile(const char *storageType)
    {
        return strsame(storageType, "azurefile");
    }
    static inline bool isAzureBlob(const char *storageType)
    {
        return strsame(storageType, "azureblob");
    }

    Linked<IStorageApiInfo> sourceApiInfo, targetApiInfo;
};

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
        if (CAzureApiCopyClient::canCopy(source, target))
            return new CAzureApiCopyClient(source, target);
        return nullptr;
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
