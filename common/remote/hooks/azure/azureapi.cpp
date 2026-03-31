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

#include <map>
#include <set>

using namespace Azure::Storage;
using namespace Azure::Storage::Files;
using namespace Azure::Storage::Blobs;

// Forward declarations from the individual implementations
extern IFile * createAzureBlob(const char * filename);
extern IFile * createAzureFile(const char * filename);

// Cache of Azure Files directory URLs already created/verified, to avoid
// redundant CreateIfNotExists metadata calls across multi-part copies.
// Scoped to the owning CAzureApiCopyClient so it is freed when the copy session ends.
struct CreatedDirsCache
{
    bool contains(const std::string &path) const
    {
        CriticalBlock block(lock);
        return dirs.count(path) != 0;
    }
    void insert(const std::string &path)
    {
        CriticalBlock block(lock);
        dirs.insert(path);
    }
private:
    mutable CriticalSection lock;
    std::set<std::string> dirs;
};

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
    bool sourceIsBlob = false;
    Shares::ShareClientOptions clientOptions;
    std::shared_ptr<const Azure::Core::Credentials::TokenCredential> credential;
    std::string targetUrl;
    CreatedDirsCache &createdDirsCache;

    // Azure Files requires parent directories to exist before creating a file.
    // Walk the path components and create each directory level if it doesn't exist.
    void ensureParentDirectories()
    {
        // Parse the target URL to extract share and path components
        // URL format: https://account.file.core.windows.net/share/dir1/dir2/filename
        Azure::Core::Url parsed(targetUrl);
        std::string urlPath = parsed.GetPath(); // e.g., "share/dir1/dir2/filename"

        // Split into components
        std::vector<std::string> components;
        size_t start = 0;
        while (start < urlPath.size())
        {
            size_t pos = urlPath.find('/', start);
            if (pos == std::string::npos)
                break; // last component is the filename — skip it
            if (pos > start)
                components.push_back(urlPath.substr(start, pos - start));
            start = pos + 1;
        }

        if (components.size() < 2)
            return; // No parent directories to create (just share/filename)

        // First component is the share name, remaining are directories
        std::string baseUrl = parsed.GetScheme() + "://" + parsed.GetHost() + "/" + components[0];
        // Preserve the query string (contains the SAS token for non-managed-identity auth)
        std::string queryString = parsed.GetAbsoluteUrl();
        auto queryPos = queryString.find('?');
        std::string querySuffix = (queryPos != std::string::npos) ? queryString.substr(queryPos) : "";
        std::string dirUrl = baseUrl;
        for (size_t i = 1; i < components.size(); i++)
        {
            dirUrl += "/" + components[i];
            if (createdDirsCache.contains(dirUrl))
                continue;
            try
            {
                std::string authUrl = dirUrl + querySuffix;
                if (credential)
                {
                    Shares::ShareDirectoryClient dirClient(authUrl, credential, clientOptions);
                    dirClient.CreateIfNotExists();
                }
                else
                {
                    Shares::ShareDirectoryClient dirClient(authUrl, clientOptions);
                    dirClient.CreateIfNotExists();
                }
                createdDirsCache.insert(dirUrl);
            }
            catch (const Azure::Core::RequestFailedException& e)
            {
                IERRLOG("Failed to create directory '%s': %s (code %d)", dirUrl.c_str(), e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
                throw;
            }
        }
    }

    virtual void doStartCopy(const char * source) override
    {
        ensureParentDirectories();

        Shares::StartFileCopyOptions options;
        if (sourceIsBlob)
        {
            // Azure Blob storage does not have SMB properties. Explicitly set to none; otherwise,
            // the copy will fail because it could not retrieve the SMB properties from the source.
            options.SmbPropertiesToCopy = Shares::CopyableFileSmbPropertyFlags::None;
        }
        fileCopyOp = fileClient->StartCopy(source, options);
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
    AzureFileClient(const char *target, bool _sourceIsBlob, bool useManagedIdentity, CreatedDirsCache &_createdDirsCache)
        : sourceIsBlob(_sourceIsBlob), targetUrl(target), createdDirsCache(_createdDirsCache)
    {
        if (useManagedIdentity)
        {
            // ShareTokenIntent is required when using token authentication with Azure Files
            clientOptions.ShareTokenIntent = Shares::Models::ShareTokenIntent::Backup;
            credential = getAzureManagedIdentityCredential();
            fileClient.reset(new Shares::ShareFileClient(target, credential, clientOptions));
        }
        else
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
    AzureBlobClient(const char * target, bool useManagedIdentity)
    {
        if (useManagedIdentity)
            blobClient.reset(new BlobClient(target, getAzureManagedIdentityCredential()));
        else
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
        bool tgtUseManagedIdentity = targetApiInfo->useManagedIdentity();
        if (isAzureFile(targetApiInfo->getStorageType()))
            apiClient.setown(new AzureFileClient(targetURI.str(), isAzureBlob(sourceApiInfo->getStorageType()), tgtUseManagedIdentity, createdDirsCache));
        else
            apiClient.setown(new AzureBlobClient(targetURI.str(), tgtUseManagedIdentity));

        StringBuffer sourceURI;
        getAzureURI(sourceURI, srcStripeNum, srcPath, sourceApiInfo);

        // For async server-side copy (StartCopy), the source URL must be self-authenticating
        // because the Azure service fetches it independently. If the source uses managed identity
        // (no SAS token), generate a short-lived User Delegation SAS so the target service can read it.
        if (sourceApiInfo->useManagedIdentity() && isAzureBlob(sourceApiInfo->getStorageType()))
        {
            std::string delegationSas = generateUserDelegationSas(srcStripeNum, srcPath, sourceApiInfo);
            // GenerateSasToken() returns a string starting with '?' so append directly
            sourceURI.append(delegationSas.c_str());
        }

        apiClient->startCopy(sourceURI.str());
        return apiClient.getClear();
    }
protected:
    // Generate a short-lived User Delegation SAS for reading a source blob using managed identity.
    // This allows the async StartCopy service to access the source without a stored secret.
    std::string generateUserDelegationSas(unsigned stripeNum, const char *filePath, const IStorageApiInfo *apiInfo) const
    {
        const char *accountName = apiInfo->queryStorageApiAccount(stripeNum);

        auto delegationKey = getCachedDelegationKey(accountName);

        Sas::BlobSasBuilder sasBuilder;
        sasBuilder.Protocol = Sas::SasProtocol::HttpsOnly;
        sasBuilder.ExpiresOn = delegationKey->SignedExpiresOn;
        sasBuilder.BlobContainerName = apiInfo->queryStorageContainerName(stripeNum);
        sasBuilder.BlobName = stripDevicePrefix(filePath);
        sasBuilder.Resource = Sas::BlobSasResource::Blob;
        sasBuilder.SetPermissions(Sas::BlobSasPermissions::Read);

        return sasBuilder.GenerateSasToken(*delegationKey, accountName);
    }

    // Return a cached UserDelegationKey for the given account, refreshing if expired or absent.
    // The key is valid for 1 hour; we refresh with 5 minutes of margin to avoid using a nearly-expired key.
    std::shared_ptr<const Blobs::Models::UserDelegationKey> getCachedDelegationKey(const char *accountName) const
    {
        CriticalBlock block(delegationKeyCS);
        auto it = delegationKeyCache.find(accountName);
        auto now = std::chrono::system_clock::now();
        if (it != delegationKeyCache.end())
        {
            // Reuse if the key still has at least 5 minutes of validity
            if (it->second.fetchedAt + std::chrono::minutes(55) > now)
                return it->second.key;
        }

        std::string serviceUrl = std::string("https://") + accountName + ".blob.core.windows.net";
        BlobServiceClient serviceClient(serviceUrl, getAzureManagedIdentityCredential());

        auto expiresOn = Azure::DateTime(now + std::chrono::hours(1));
        auto key = std::make_shared<Blobs::Models::UserDelegationKey>(serviceClient.GetUserDelegationKey(expiresOn).Value);

        CachedDelegationKey entry;
        entry.key = key;
        entry.fetchedAt = now;
        delegationKeyCache[accountName] = std::move(entry);

        return key;
    }

    void getAzureURI(StringBuffer & uri, unsigned stripeNum, const char *filePath, const IStorageApiInfo *apiInfo) const
    {
        const char *accountName = apiInfo->queryStorageApiAccount(stripeNum);
        uri.appendf("https://%s", accountName);

        if (isAzureFile(apiInfo->getStorageType()))
            uri.append(".file");
        else
            uri.append(".blob");
        uri.append(".core.windows.net/");

        const char *path = stripDevicePrefix(filePath);

        StringBuffer tmp, token;
        const char * container = apiInfo->queryStorageContainerName(stripeNum);
        uri.appendf("%s/%s%s", container, encodeURL(tmp, path).str(), apiInfo->getSASToken(stripeNum, token).str());
    }

    // Strip leading '/' and device/stripe prefix (e.g., "d2/") from a file path.
    // The stripe number selects the account/container, not the storage path.
    static const char * stripDevicePrefix(const char *path)
    {
        if (path && path[0] == '/')
            path++;
        if (path && path[0] == 'd' && isdigit(path[1]))
        {
            const char *slash = strchr(path, '/');
            if (slash)
                path = slash + 1;
        }
        return path;
    }
    static inline bool isAzureFile(const char *storageType)
    {
        return strsame(storageType, "azurefile");
    }
    static inline bool isAzureBlob(const char *storageType)
    {
        return strsame(storageType, "azureblob");
    }

    struct CachedDelegationKey
    {
        std::shared_ptr<const Blobs::Models::UserDelegationKey> key;
        std::chrono::system_clock::time_point fetchedAt;
    };

    Linked<IStorageApiInfo> sourceApiInfo, targetApiInfo;
    mutable CreatedDirsCache createdDirsCache;
    mutable CriticalSection delegationKeyCS;
    mutable std::map<std::string, CachedDelegationKey> delegationKeyCache;
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
