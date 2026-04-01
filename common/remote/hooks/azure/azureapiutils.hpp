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

#ifndef AZURE_API_UTILS_HPP
#define AZURE_API_UTILS_HPP

#include "jlib.hpp"
#include "jlog.hpp"
#include "jmutex.hpp"
#include "jstring.hpp"
#include "jfile.hpp"
#include "jregexp.hpp"

#include <azure/core.hpp>
#include <azure/core/http/http.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/shares.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <azure/identity.hpp>

#include <exception>
#include <optional>
#include <vector>

/*
 * Common utility functions and constants shared by Azure Blob and File implementations
 */

constexpr const char * azureBlobPrefix = "azureblob:";
constexpr const char * azureFilePrefix = "azurefile:";

// Helper functions for creating Azure credentials
std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> getAzureSharedKeyCredential(const char * accountName, const char * secretName);
std::shared_ptr<Azure::Core::Credentials::TokenCredential> getAzureManagedIdentityCredential();
std::shared_ptr<Azure::Core::Http::HttpTransport> getHttpTransport();

bool areManagedIdentitiesEnabled();
bool isBase64Char(char c);
void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries);
void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename);
void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len);
void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename);
void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len);

//---------------------------------------------------------------------------------------------------------------------
// Base class for Azure directory iterators (Blob and File).
// Subclasses implement fetchPage() to populate the items vector using the
// appropriate Azure SDK listing API (with MoveToNextPage() for pagination),
// resetPaging() to discard stored paged-response state, and createFile() to
// construct the correct IFile type for each entry.

class AzureDirectoryIteratorBase : implements IDirectoryIterator, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    struct DirEntry
    {
        std::string name;
        bool isDir;
        int64_t size;
        time_t modifiedTime;
    };

    AzureDirectoryIteratorBase(const char *_fullPrefix, const char *_containerOrShare, const char *_mask, bool _includeDirs)
        : fullPrefix(_fullPrefix), containerOrShare(_containerOrShare), mask(_mask), includeDirs(_includeDirs) {}

    virtual bool first() override
    {
        items.clear();
        itemIndex = 0;
        hasMorePages = true;
        resetPaging();
        return loadNextPage() && advance();
    }
    virtual bool next() override                        { itemIndex++; return advance(); }
    virtual bool isValid() override                     { return curValid; }
    virtual IFile &query() override
    {
        if (!curFile)
        {
            StringBuffer fullPath(fullPrefix);
            fullPath.append(curName);
            curFile.setown(createFile(fullPath.str(), items[itemIndex]));
        }
        return *curFile;
    }
    virtual StringBuffer &getName(StringBuffer &buf) override { if (isValid()) buf.append(curName); return buf; }
    virtual bool isDir() override                       { return curIsDir; }
    virtual __int64 getFileSize() override              { return curIsDir ? -1 : curSize; }
    virtual bool getModifiedTime(CDateTime &ret) override
    {
        if (curIsDir || curModifiedTime == 0)
        {
            ret.clear();
            return false;
        }
        ret.set(curModifiedTime);
        return true;
    }

protected:
    // Subclass populates 'items' and sets 'hasMorePages' via the SDK's PagedResponse
    virtual void fetchPage() = 0;
    // Subclass discards any stored paged-response state so iteration can restart
    virtual void resetPaging() = 0;
    // Subclass creates the appropriate IFile (AzureBlob or AzureFile) for the given path
    virtual IFile *createFile(const char *fullPath, const DirEntry &entry) = 0;

    bool matchesMask(const char *name) const { return !mask.length() || WildMatch(name, mask, false); }

    static time_t toTimeT(const Azure::DateTime &dt)
    {
        return std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point(dt));
    }

    StringAttr fullPrefix;
    StringAttr containerOrShare;
    StringAttr mask;
    bool includeDirs;
    std::vector<DirEntry> items;
    unsigned itemIndex = 0;
    bool hasMorePages = false;

private:
    bool loadNextPage()
    {
        if (!hasMorePages)
            return !items.empty();
        constexpr unsigned maxRetries = 4;
        unsigned attempt = 0;
        for (;;)
        {
            try
            {
                fetchPage();
                break;
            }
            catch (const Azure::Core::RequestFailedException &e)
            {
                attempt++;
                VStringBuffer msg("%s (container: %s)", fullPrefix.str(), containerOrShare.str());
                handleRequestException(e, "Azure::directoryFiles", attempt, maxRetries, msg.str());
            }
            catch (const std::exception &e)
            {
                attempt++;
                VStringBuffer msg("%s (container: %s)", fullPrefix.str(), containerOrShare.str());
                handleRequestException(e, "Azure::directoryFiles", attempt, maxRetries, msg.str());
            }
        }
        return !items.empty() || hasMorePages;
    }

    bool advance()
    {
        while (itemIndex >= items.size())
        {
            if (!hasMorePages)
            {
                curFile.clear();
                curValid = false;
                return false;
            }
            items.clear();
            itemIndex = 0;
            if (!loadNextPage())
            {
                curFile.clear();
                curValid = false;
                return false;
            }
        }
        const DirEntry &entry = items[itemIndex];
        curName.set(entry.name.c_str());
        curIsDir = entry.isDir;
        curSize = entry.size;
        curModifiedTime = entry.modifiedTime;
        curFile.clear(); // Created lazily in query() - scanDirectory never calls it
        curValid = true;
        return true;
    }

    Owned<IFile> curFile;
    StringAttr curName;
    bool curValid = false;
    bool curIsDir = false;
    int64_t curSize = -1;
    time_t curModifiedTime = 0;
};

#endif
