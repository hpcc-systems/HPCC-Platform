#include "platform.h"
#include "jcurl.hpp"
#include "jlog.hpp"
#include "jexcept.hpp"

#include <curl/curl.h>
#include <memory>

static bool initialisedCurl;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    CURLcode rc = curl_global_init(CURL_GLOBAL_ALL);
    initialisedCurl = (rc == CURLE_OK);
    if (!initialisedCurl)
        ERRLOG("curl_global_init failed: %s", curl_easy_strerror(rc));
    return true;
}
MODULE_EXIT()
{
    curl_global_cleanup();
}

// Custom deleter for curl_slist
struct CurlSlistDeleter
{
    void operator()(curl_slist* h) const
    {
        if (h)
            curl_slist_free_all(h);
    }
};

using CurlHeadersPtr = std::unique_ptr<curl_slist, CurlSlistDeleter>;

class CCurlHttpClient : public CInterfaceOf<ISimpleHttpClient>
{
private:
    CURL* curl = nullptr;
    std::string baseUrl;
    std::string clientCaCertPath;
    std::string errorMsg;
    int lastErrorCode = 0;
    bool verifyServer = true;
    long connectTimeoutMs = 10000;
    long transferTimeoutMs = 60000;

    bool buildCurlHeaders(const SimpleHttpClientHeaderMap& headers, const char* contentType, CurlHeadersPtr &curlHeaders)
    {
        curl_slist *rawHeaders = nullptr;
        for (const auto& pair : headers)
        {
            std::string headerLine = pair.first + ": " + pair.second;
            curl_slist* next = curl_slist_append(rawHeaders, headerLine.c_str());
            if (!next)
            {
                if (rawHeaders)
                    curl_slist_free_all(rawHeaders);
                lastErrorCode = CURLE_OUT_OF_MEMORY;
                errorMsg = "Failed to build curl headers";
                return false;
            }
            rawHeaders = next;
        }
        if (contentType)
        {
            std::string ctLine = std::string("Content-Type: ") + contentType;
            curl_slist* next = curl_slist_append(rawHeaders, ctLine.c_str());
            if (!next)
            {
                if (rawHeaders)
                    curl_slist_free_all(rawHeaders);
                lastErrorCode = CURLE_OUT_OF_MEMORY;
                errorMsg = "Failed to build curl headers";
                return false;
            }
            rawHeaders = next;
        }
        curlHeaders.reset(rawHeaders);
        return true;
    }

    static size_t writeCallback(void* contents, size_t size, size_t nmemb, void* userp)
    {
        size_t realsize = size * nmemb;
        StringBuffer* buf = static_cast<StringBuffer*>(userp);
        buf->append(realsize, (const char*)contents);
        return realsize;
    }

    int performRequest(const char* path, curl_slist* headers, StringBuffer& responseBody)
    {
        assertex(path);

        errorMsg.clear();
        lastErrorCode = 0;
        responseBody.clear();

        std::string fullUrl = baseUrl + path;
        curl_easy_setopt(curl, CURLOPT_URL, fullUrl.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &responseBody);
        curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, verifyServer ? 1L : 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, verifyServer ? 2L : 0L);

        // Configure CA cert path on every request so prior values do not remain sticky.
        const char *caInfo = clientCaCertPath.empty() ? nullptr : clientCaCertPath.c_str();
        curl_easy_setopt(curl, CURLOPT_CAINFO, caInfo);

        // Configure timeout options on every request so they can be reset to defaults.
        curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, connectTimeoutMs > 0 ? connectTimeoutMs : 0L);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, transferTimeoutMs > 0 ? transferTimeoutMs : 0L);

        char errbuf[CURL_ERROR_SIZE];
        errbuf[0] = 0;
        curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);

        CURLcode res = curl_easy_perform(curl);
        // Clear headers immediately after perform so curl doesn't hold a stale pointer
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, nullptr);

        if (res != CURLE_OK)
        {
            lastErrorCode = res;
            size_t len = strlen(errbuf);
            if (len)
                errorMsg = errbuf;
            else
                errorMsg = curl_easy_strerror(res);
            return -1;
        }

        long httpCode = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &httpCode);
        return static_cast<int>(httpCode);
    }

public:
    CCurlHttpClient(const char* _baseUrl) : baseUrl(_baseUrl ? _baseUrl : "")
    {
        curl = curl_easy_init();
        // Do not perform any actions on curl if nullptr
    }

    CCurlHttpClient(const char* _baseUrl, const char* clientCertPath, const char* clientKeyPath)
        : baseUrl(_baseUrl ? _baseUrl : "")
    {
        curl = curl_easy_init();
        if (!curl)
            return;

        // Set client certificate immediately if provided
        if (clientCertPath && clientKeyPath)
            setClientCert(clientCertPath, clientKeyPath);
    }

    ~CCurlHttpClient()
    {
        if (curl)
            curl_easy_cleanup(curl);
    }

    void checkInitialised()
    {
        if (!curl)
            throw makeStringException(-1, "libcurl initialization failed");
    }

    virtual void setBasicAuth(const char* username, const char* password) override
    {
        if (!username || !password)
        {
            WARNLOG("Basic auth credentials cannot be null");
            return;
        }
        // Validate input lengths to prevent excessive memory allocation
        size_t userLen = strlen(username);
        size_t passLen = strlen(password);
        if (userLen > 4096 || passLen > 4096)
        {
            WARNLOG("Basic auth credentials exceed maximum length");
            return;
        }
        std::string auth = std::string(username) + ":" + password;
        curl_easy_setopt(curl, CURLOPT_USERPWD, auth.c_str());
    }

    virtual void setVerifyServer(bool verify) override
    {
        verifyServer = verify;
    }

    virtual void setClientCert(const char* certPath, const char* keyPath) override
    {
        curl_easy_setopt(curl, CURLOPT_SSLCERT, certPath);
        curl_easy_setopt(curl, CURLOPT_SSLKEY, keyPath);
    }

    virtual void setClientCaCertPath(const char* certPath) override
    {
        clientCaCertPath = certPath ? certPath : "";
    }

    virtual void setTimeouts(long _connectTimeoutMs, long _transferTimeoutMs) override
    {
        connectTimeoutMs = _connectTimeoutMs > 0 ? _connectTimeoutMs : 0;
        transferTimeoutMs = _transferTimeoutMs > 0 ? _transferTimeoutMs : 0;
    }

    // New methods with per-request headers support
    virtual int get(const char* path, const SimpleHttpClientHeaderMap& headers, StringBuffer& responseBody) override
    {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, nullptr);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, nullptr);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0L);
        curl_easy_setopt(curl, CURLOPT_POST, 0L);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        CurlHeadersPtr curlHeaders;
        if (!buildCurlHeaders(headers, nullptr, curlHeaders))
            return -1;
        return performRequest(path, curlHeaders.get(), responseBody);
    }

    virtual int post(const char* path, const SimpleHttpClientHeaderMap& headers,
                     const char* body, const char* contentType, StringBuffer& responseBody) override
    {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, nullptr);
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body ? static_cast<long>(strlen(body)) : 0L);

        CurlHeadersPtr curlHeaders;
        if (!buildCurlHeaders(headers, contentType, curlHeaders))
            return -1;
        return performRequest(path, curlHeaders.get(), responseBody);
    }

    virtual int put(const char* path, const SimpleHttpClientHeaderMap& headers,
                    const char* body, const char* contentType, StringBuffer& responseBody) override
    {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
        curl_easy_setopt(curl, CURLOPT_POST, 0L);
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, body ? static_cast<long>(strlen(body)) : 0L);
        CurlHeadersPtr curlHeaders;
        if (!buildCurlHeaders(headers, contentType, curlHeaders))
            return -1;
        return performRequest(path, curlHeaders.get(), responseBody);
    }

    // Convenience overloads for requests without custom headers
    virtual int get(const char* path, StringBuffer& responseBody) override
    {
        SimpleHttpClientHeaderMap emptyHeaders;
        return get(path, emptyHeaders, responseBody);
    }

    virtual int post(const char* path, const char* body, const char* contentType, StringBuffer& responseBody) override
    {
        SimpleHttpClientHeaderMap emptyHeaders;
        return post(path, emptyHeaders, body, contentType, responseBody);
    }

    virtual int put(const char* path, const char* body, const char* contentType, StringBuffer& responseBody) override
    {
        SimpleHttpClientHeaderMap emptyHeaders;
        return put(path, emptyHeaders, body, contentType, responseBody);
    }

    virtual const char* queryErrorMessage() const override
    {
        // Always return a valid C-string pointer (empty string if no error)
        return errorMsg.c_str();
    }

    virtual int getErrorCode() const override
    {
        return lastErrorCode;
    }
};

ISimpleHttpClient* createSimpleHttpClient(const char* baseUrl)
{
    if (!baseUrl)
        throw makeStringException(-1, "baseUrl cannot be null");
    Owned<CCurlHttpClient> result = new CCurlHttpClient(baseUrl);
    result->checkInitialised();
    return result.getClear();
}

ISimpleHttpClient* createSimpleHttpClient(const char* baseUrl,
                                          const char* clientCertPath,
                                          const char* clientKeyPath)
{
    if (!baseUrl)
        throw makeStringException(-1, "baseUrl cannot be null");
    Owned<CCurlHttpClient> result = new CCurlHttpClient(baseUrl, clientCertPath, clientKeyPath);
    result->checkInitialised();
    return result.getClear();
}
