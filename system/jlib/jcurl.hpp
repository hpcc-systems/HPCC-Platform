#ifndef JCURL_HPP
#define JCURL_HPP

#include "jiface.hpp"
#include "jstring.hpp"

#include <map>
#include <string>

// HTTP header map type for requests
using SimpleHttpClientHeaderMap = std::map<std::string, std::string>;

// Simple HTTP client for basic operations.
// Instances are not thread-safe and should not be shared across threads.
interface ISimpleHttpClient : extends IInterface
{
    virtual void setBasicAuth(const char* username, const char* password) = 0;
    virtual void setVerifyServer(bool verify) = 0;

    // Client certificate for mTLS (file paths for now; future: support in-memory PEM data via jsecrets)
    virtual void setClientCert(const char* certPath, const char* keyPath) = 0;

    // CA certificate for server verification (file path for now; future: support in-memory PEM data)
    virtual void setClientCaCertPath(const char* certPath) = 0;

    // Timeouts in milliseconds.
    // connectTimeoutMs controls TCP/TLS connection establishment.
    // transferTimeoutMs controls the entire request/response transfer.
    virtual void setTimeouts(long connectTimeoutMs, long transferTimeoutMs) = 0;

    // HTTP Verbs - returns HTTP status code
    // Callers must pass all required headers with each request
    virtual int get(const char* path, const SimpleHttpClientHeaderMap& headers, StringBuffer& responseBody) = 0;
    // body is treated as NUL-terminated text. Binary payloads require a length-aware API.
    virtual int post(const char* path, const SimpleHttpClientHeaderMap& headers,
                     const char* body, const char* contentType, StringBuffer& responseBody) = 0;
    virtual int put(const char* path, const SimpleHttpClientHeaderMap& headers,
                    const char* body, const char* contentType, StringBuffer& responseBody) = 0;

    // Convenience overloads for requests without custom headers
    virtual int get(const char* path, StringBuffer& responseBody) = 0;
    virtual int post(const char* path, const char* body, const char* contentType, StringBuffer& responseBody) = 0;
    virtual int put(const char* path, const char* body, const char* contentType, StringBuffer& responseBody) = 0;

    virtual const char* queryErrorMessage() const = 0;
    virtual int getErrorCode() const = 0;
};

// Factory methods
extern jlib_decl ISimpleHttpClient* createSimpleHttpClient(const char* baseUrl);
// Factory with mTLS certificate/key (paths for now; future: support in-memory PEM data)
extern jlib_decl ISimpleHttpClient* createSimpleHttpClient(const char* baseUrl,
                                                            const char* clientCertPath,
                                                            const char* clientKeyPath);

#endif // JCURL_HPP
