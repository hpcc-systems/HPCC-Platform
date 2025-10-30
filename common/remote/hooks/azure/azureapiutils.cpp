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
#include "azureapiutils.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jplane.hpp"
#include "jsecrets.hpp"
#include <cstdlib>

using namespace std::chrono;

// Common utility functions shared by both blob and file implementations
//---------------------------------------------------------------------------------------------------------------------

bool areManagedIdentitiesEnabled()
{
    // Check for Azure AD Workload Identity or legacy managed identity
    static bool hasWorkloadIdentity = std::getenv("AZURE_CLIENT_ID") &&
                                     std::getenv("AZURE_TENANT_ID") &&
                                     std::getenv("AZURE_FEDERATED_TOKEN_FILE");

    static bool hasManagedIdentity = std::getenv("MSI_ENDPOINT") || std::getenv("IDENTITY_ENDPOINT");

    return hasWorkloadIdentity || hasManagedIdentity;
}

std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> getAzureSharedKeyCredential(const char * accountName, const char * secretName)
{
    // MORE: Should we create a cache of credentials?  We would need to be careful about the lifetime of the shared key credential

    StringBuffer key;
    getSecretValue(key, "storage", secretName, "key", true);
    //Trim trailing whitespace/newlines in case the secret has been entered by hand e.g. on bare metal
    size32_t len = key.length();
    for (;;)
    {
        if (!len)
            break;
        if (isBase64Char(key.charAt(len-1)))
            break;
        len--;
    }
    key.setLength(len);

    try
    {
        return std::make_shared<Azure::Storage::StorageSharedKeyCredential>(accountName, key.str());
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        IException * error = makeStringExceptionV(-1, "Azure access: %s (%d)", e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        throw error;
    }
}

std::shared_ptr<Azure::Core::Credentials::TokenCredential> getAzureManagedIdentityCredential()
{
    // MORE: Should we create a cache of credentials?  We would need to be careful about the lifetime of the managed identity credential

    // Azure SDK credential objects handle token refresh automatically
    const char * federatedTokenFile = std::getenv("AZURE_FEDERATED_TOKEN_FILE");
    if (federatedTokenFile)
    {
        // Workload Identity
        const char * clientId = std::getenv("AZURE_CLIENT_ID");
        const char * tenantId = std::getenv("AZURE_TENANT_ID");
        try
        {
#ifdef AZURE_HAS_WORKLOAD_IDENTITY_CREDENTIAL
            DBGLOG("Using Azure Workload Identity authentication (clientId=%s, tenantId=%s, tokenFile=%s)",
                clientId ? clientId : "<none>", tenantId ? tenantId : "<none>", federatedTokenFile);
            return std::make_shared<Azure::Identity::WorkloadIdentityCredential>();
#else
            // SDK doesn't have WorkloadIdentityCredential - check if workload identity environment is configured
            DBGLOG("Using DefaultAzureCredential for Workload Identity (SDK < 1.6.0) (clientId=%s, tenantId=%s, tokenFile=%s)",
                clientId ? clientId : "<none>", tenantId ? tenantId : "<none>", federatedTokenFile);
            return std::make_shared<Azure::Identity::DefaultAzureCredential>();
#endif
        }
        catch (const Azure::Core::RequestFailedException& e)
        {
            throw makeStringExceptionV(-1, "Azure authentication failed: %s (%d)",
                e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
        }
    }
    // else fall through

    // Use ManagedIdentityCredential for legacy managed identity
    // Only pass clientId if MSI/IDENTITY endpoints are set (true managed identity scenario)
    const char * msiEndpoint = std::getenv("MSI_ENDPOINT");
    const char * identityEndpoint = std::getenv("IDENTITY_ENDPOINT");
    const char * clientId = (msiEndpoint || identityEndpoint) ? std::getenv("AZURE_CLIENT_ID") : nullptr;
    DBGLOG("Using Azure Managed Identity authentication (clientId=%s, MSI_ENDPOINT=%s, IDENTITY_ENDPOINT=%s)",
           clientId ? clientId : "<none>",
           msiEndpoint ? msiEndpoint : "<none>",
           identityEndpoint ? identityEndpoint : "<none>");
    try
    {
        if (clientId)
            return std::make_shared<Azure::Identity::ManagedIdentityCredential>(clientId);
        else
            return std::make_shared<Azure::Identity::ManagedIdentityCredential>();
    }
    catch (const Azure::Core::RequestFailedException& e)
    {
        throw makeStringExceptionV(-1, "Azure Managed Identity authentication failed: %s (%d)",
            e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));
    }
}

//---------------------------------------------------------------------------------------------------------------------

// Global transport instance for connection reuse across all Azure blob operations
// This allows HTTP connection pooling to work effectively across different blobs/containers/accounts
static std::shared_ptr<Azure::Core::Http::HttpTransport> globalAzureTransport;
static CriticalSection globalTransportCS;

std::shared_ptr<Azure::Core::Http::HttpTransport> getHttpTransport()
{
    CriticalBlock block(globalTransportCS);
    if (!globalAzureTransport)
    {
        // Create shared transport with optimized settings for all Azure operations
        Azure::Core::Http::CurlTransportOptions transportOptions;
        transportOptions.ConnectionTimeout = std::chrono::milliseconds(10000);  // 10 second connection timeout
        transportOptions.NoSignal = true;  // Avoid signal interference
        // Note: libcurl automatically handles connection pooling and keep-alive
        // Sharing the transport instance ensures maximum connection reuse
        globalAzureTransport = std::make_shared<Azure::Core::Http::CurlTransport>(transportOptions);
    }
    return globalAzureTransport;
}

//---------------------------------------------------------------------------------------------------------------------

bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries)
{
    OWARNLOG("%s", message);

    if (attempt >= maxRetries)
        throw makeStringException(1234, message);

    // Exponential backoff with jitter
    unsigned backoffMs = (1U << attempt) * 100 + (rand() % 100);
    Sleep(backoffMs);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s (%d)",
                      op, attempt, maxRetries, filename, pos, len, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s",
                      op, attempt, maxRetries, filename, pos, len, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s (%d)",
                      op, attempt, maxRetries, filename, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s",
                      op, attempt, maxRetries, filename, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}
