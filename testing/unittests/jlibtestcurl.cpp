/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "jcurl.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"

#include <atomic>
#include <deque>
#include <map>
#include <mutex>
#include <string>
#include <vector>

static unsigned short randomCurlTestPort()
{
    return 32768 + getRandom() % 10000;
}

struct CurlTestHttpRequest
{
    std::string method;
    std::string path;
    std::map<std::string, std::string> headers;
    std::string body;
};

struct CurlTestHttpResponse
{
    unsigned statusCode = 200;
    std::string reason = "OK";
    std::string body = "{}";
    std::string contentType = "application/json";
    unsigned delayMs = 0;
    std::map<std::string, std::string> headers;
};

class CurlTestHttpServer : public Thread
{
public:
    CurlTestHttpServer()
        : Thread("CurlTestHttpServer")
    {
    }

    ~CurlTestHttpServer()
    {
        stop();
    }

    void startServer()
    {
        started = false;
        running = true;
        Thread::start(false);
        startedSignal.wait();
    }

    void stop()
    {
        running = false;
        if (listenSocket)
        {
            try
            {
                listenSocket->cancel_accept();
            }
            catch (...)
            {
            }
        }

        join();
    }

    unsigned short queryPort() const
    {
        return port;
    }

    std::vector<CurlTestHttpRequest> snapshotRequests() const
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        return requests;
    }

    void clearRequests()
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        requests.clear();
    }

    void queueResponse(unsigned statusCode, const char *reason, const char *body, const char *contentType, unsigned delayMs=0, const std::map<std::string, std::string> &headers = {})
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        CurlTestHttpResponse response;
        response.statusCode = statusCode;
        response.reason = reason ? reason : "";
        response.body = body ? body : "";
        response.contentType = contentType ? contentType : "text/plain";
        response.delayMs = delayMs;
        response.headers = headers;
        responseQueue.emplace_back(std::move(response));
    }

protected:
    virtual int run() override
    {
        try
        {
            // Retry a few times to find an available port (avoid TIME_WAIT conflicts)
            for (int retries = 0; retries < 10; retries++)
            {
                try
                {
                    listenSocket.setown(ISocket::create(32768 + getRandom() % 10000));
                    SocketEndpoint ep;
                    listenSocket->getEndpoint(ep);
                    port = ep.port;
                    break;  // Success
                }
                catch (IException *e)
                {
                    if (retries < 9)
                    {
                        e->Release();
                        continue;  // Retry with different port
                    }
                    throw;  // All retries exhausted
                }
            }

            started = true;
            startedSignal.signal();

            while (running)
            {
                try
                {
                    Owned<ISocket> client = listenSocket->accept(true);
                    if (!client || !running)
                        break;

                    CurlTestHttpRequest request;
                    if (readHttpRequest(*client, request))
                    {
                        CurlTestHttpResponse response;
                        {
                            std::lock_guard<std::mutex> lock(stateMutex);
                            requests.emplace_back(std::move(request));
                            if (!responseQueue.empty())
                            {
                                response = std::move(responseQueue.front());
                                responseQueue.pop_front();
                            }
                            else
                                response = defaultResponse;
                        }

                        if (response.delayMs)
                            MilliSleep(response.delayMs);

                        sendSimpleResponse(*client, response.statusCode, response.reason.c_str(), response.body.c_str(), response.contentType.c_str(), response.headers);
                    }

                    client->close();
                }
                catch (IJSOCK_Exception *e)
                {
                    if (e->errorCode() == JSOCKERR_cancel_accept || e->errorCode() == JSOCKERR_graceful_close)
                        running = false;
                    e->Release();
                }
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("CurlTestHttpServer error: %s", msg.str());
            e->Release();
            startedSignal.signal();
        }

        return 0;
    }

private:
    Owned<ISocket> listenSocket;
    std::atomic<bool> running = false;
    std::atomic<bool> started = false;
    unsigned short port = 0;
    Semaphore startedSignal{SYNC_LOCATION};

    mutable std::mutex stateMutex;
    std::vector<CurlTestHttpRequest> requests;
    std::deque<CurlTestHttpResponse> responseQueue;
    CurlTestHttpResponse defaultResponse;

    static bool readHttpRequest(ISocket &socket, CurlTestHttpRequest &request)
    {
        std::string raw;
        raw.reserve(1024);

        constexpr char headerTerminator[] = "\r\n\r\n";
        size_t headerEnd = std::string::npos;

        while (headerEnd == std::string::npos)
        {
            char buffer[1024];
            size32_t bytesRead = 0;
            socket.read(buffer, 1, sizeof(buffer), bytesRead, 5);
            if (bytesRead == 0)
                return false;

            raw.append(buffer, bytesRead);
            headerEnd = raw.find(headerTerminator);
        }

        const size_t bodyOffset = headerEnd + strlen(headerTerminator);
        std::string headerBlock = raw.substr(0, headerEnd);

        if (!parseStartLineAndHeaders(headerBlock, request))
            return false;

        size_t contentLength = 0;
        auto it = request.headers.find("Content-Length");
        if (it != request.headers.end())
            contentLength = static_cast<size_t>(atoi(it->second.c_str()));

        if (raw.length() < bodyOffset + contentLength)
        {
            const size_t remaining = bodyOffset + contentLength - raw.length();
            std::string bodyRemainder;
            bodyRemainder.reserve(remaining);

            while (bodyRemainder.length() < remaining)
            {
                char buffer[1024];
                size32_t bytesRead = 0;
                socket.read(buffer, 1, sizeof(buffer), bytesRead, 5);
                if (bytesRead == 0)
                    break;
                bodyRemainder.append(buffer, bytesRead);
            }
            raw.append(bodyRemainder);
        }

        if (contentLength)
            request.body = raw.substr(bodyOffset, contentLength);

        return true;
    }

    static bool parseStartLineAndHeaders(const std::string &headerBlock, CurlTestHttpRequest &request)
    {
        size_t lineEnd = headerBlock.find("\r\n");
        std::string firstLine = (lineEnd == std::string::npos) ? headerBlock : headerBlock.substr(0, lineEnd);

        const size_t firstSpace = firstLine.find(' ');
        if (firstSpace == std::string::npos)
            return false;
        const size_t secondSpace = firstLine.find(' ', firstSpace + 1);
        if (secondSpace == std::string::npos)
            return false;

        request.method = firstLine.substr(0, firstSpace);
        request.path = firstLine.substr(firstSpace + 1, secondSpace - firstSpace - 1);

        if (lineEnd == std::string::npos)
            return true;

        size_t cursor = lineEnd + 2;
        while (cursor < headerBlock.length())
        {
            size_t next = headerBlock.find("\r\n", cursor);
            std::string line = (next == std::string::npos) ? headerBlock.substr(cursor) : headerBlock.substr(cursor, next - cursor);
            if (line.empty())
                break;

            size_t colon = line.find(':');
            if (colon != std::string::npos)
            {
                std::string name = line.substr(0, colon);
                std::string value = line.substr(colon + 1);
                while (!value.empty() && value[0] == ' ')
                    value.erase(0, 1);
                request.headers[name] = value;
            }

            if (next == std::string::npos)
                break;
            cursor = next + 2;
        }

        return true;
    }

    static void sendSimpleResponse(ISocket &socket, unsigned statusCode, const char *reason, const char *body, const char *contentType, const std::map<std::string, std::string> &headers)
    {
        VStringBuffer responseHeader(
            "HTTP/1.1 %u %s\r\n"
            "Content-Length: %u\r\n"
            "Content-Type: %s\r\n"
            "Connection: close\r\n",
            statusCode,
            reason,
            static_cast<unsigned>(strlen(body)),
            contentType);

        for (const auto &header: headers)
            responseHeader.appendf("%s: %s\r\n", header.first.c_str(), header.second.c_str());

        responseHeader.append("\r\n");
        responseHeader.append(body);

        socket.write(responseHeader.str(), responseHeader.length());
    }
};

class JlibCurlTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibCurlTest);
        CPPUNIT_TEST(testPutThenGetMethodReset);
        CPPUNIT_TEST(testPostThenGetBodyCleared);
        CPPUNIT_TEST(testGetThenPutMethodReset);
        CPPUNIT_TEST(testPostHeadersAndBody);
        CPPUNIT_TEST(testHeaderReplacement);
        CPPUNIT_TEST(testAddHeaderOverride);
        CPPUNIT_TEST(testNullContentTypePost);
        CPPUNIT_TEST(testNon200StatusPassThrough);
        CPPUNIT_TEST(testTimeoutFailure);
        CPPUNIT_TEST(testTimeoutResetAllowsSubsequentRequest);
        CPPUNIT_TEST(testLargePayloadBody);
        CPPUNIT_TEST(testMultipleSequentialRequests);
        CPPUNIT_TEST(testBasicAuthHeaderPresent);
        CPPUNIT_TEST(testConnectFailureSetsError);
    CPPUNIT_TEST_SUITE_END();

    void testPutThenGetMethodReset()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);
        CPPUNIT_ASSERT(client);

        StringBuffer response;
        int status = client->put("/v1/put", "{\"k\":1}", "application/json", response);
        CPPUNIT_ASSERT_EQUAL(200, status);

        response.clear();
        status = client->get("/v1/get", response);
        CPPUNIT_ASSERT_EQUAL(200, status);

        auto requests = waitForRequests(server, 2);
        assertMethodPath(requests[0], "PUT", "/v1/put");
        assertMethodPath(requests[1], "GET", "/v1/get");

        server.stop();
        END_TEST
    }

    void testPostThenGetBodyCleared()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        StringBuffer response;
        int status = client->post("/v1/post", "post-body", "text/plain", response);
        CPPUNIT_ASSERT_EQUAL(200, status);
        response.clear();
        CPPUNIT_ASSERT_EQUAL(200, client->get("/v1/get", response));

        auto requests = waitForRequests(server, 2);
        CPPUNIT_ASSERT_EQUAL(std::string("post-body"), requests[0].body);
        CPPUNIT_ASSERT(requests[1].body.empty());
        assertMethodPath(requests[1], "GET", "/v1/get");

        server.stop();
        END_TEST
    }

    void testGetThenPutMethodReset()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->get("/v1/get", response));
        response.clear();
        CPPUNIT_ASSERT_EQUAL(200, client->put("/v1/put", "put-body", "text/plain", response));

        auto requests = waitForRequests(server, 2);
        assertMethodPath(requests[0], "GET", "/v1/get");
        assertMethodPath(requests[1], "PUT", "/v1/put");
        CPPUNIT_ASSERT_EQUAL(std::string("put-body"), requests[1].body);

        server.stop();
        END_TEST
    }

    void testPostHeadersAndBody()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);
        CPPUNIT_ASSERT(client);

        SimpleHttpClientHeaderMap headers;
        headers["X-Test-Header"] = "abc123";

        StringBuffer response;
        int status = client->post("/v1/post", headers, "hello-body", "text/plain", response);
        CPPUNIT_ASSERT_EQUAL(200, status);

        auto requests = waitForRequests(server, 1);
        assertMethodPath(requests[0], "POST", "/v1/post");
        CPPUNIT_ASSERT_EQUAL(std::string("hello-body"), requests[0].body);
        CPPUNIT_ASSERT_EQUAL(std::string("abc123"), headerValue(requests[0], "X-Test-Header"));
        CPPUNIT_ASSERT_EQUAL(std::string("text/plain"), headerValue(requests[0], "Content-Type"));

        server.stop();
        END_TEST
    }

    void testHeaderReplacement()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        SimpleHttpClientHeaderMap headers{{"X-Second", "two"}};

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->get("/v1/headers", headers, response));

        auto requests = waitForRequests(server, 1);
        // Note: X-First is not sent in this test (only X-Second is set)
        CPPUNIT_ASSERT_EQUAL(std::string("two"), headerValue(requests[0], "X-Second"));

        server.stop();
        END_TEST
    }

    void testAddHeaderOverride()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        SimpleHttpClientHeaderMap headers{{"X-Override", "new"}};

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->get("/v1/override", headers, response));

        auto requests = waitForRequests(server, 1);
        CPPUNIT_ASSERT_EQUAL(std::string("new"), headerValue(requests[0], "X-Override"));

        server.stop();
        END_TEST
    }

    void testNullContentTypePost()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->post("/v1/no-content-type", "body", nullptr, response));

        auto requests = waitForRequests(server, 1);
        CPPUNIT_ASSERT_EQUAL(std::string("body"), requests[0].body);

        server.stop();
        END_TEST
    }

    void testNon200StatusPassThrough()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        server.queueResponse(401, "Unauthorized", "u", "text/plain");
        server.queueResponse(403, "Forbidden", "f", "text/plain");
        server.queueResponse(404, "Not Found", "n", "text/plain");
        server.queueResponse(500, "Server Error", "e", "text/plain");

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(401, client->get("/status/401", response));
        response.clear();
        CPPUNIT_ASSERT_EQUAL(403, client->get("/status/403", response));
        response.clear();
        CPPUNIT_ASSERT_EQUAL(404, client->get("/status/404", response));
        response.clear();
        CPPUNIT_ASSERT_EQUAL(500, client->get("/status/500", response));

        auto requests = waitForRequests(server, 4);
        CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(4), requests.size());

        server.stop();
        END_TEST
    }

    void testTimeoutFailure()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);
        client->setTimeouts(100, 100);

        server.queueResponse(200, "OK", "slow", "text/plain", 750);

        StringBuffer response;
        int status = client->get("/timeout", response);
        CPPUNIT_ASSERT_EQUAL(-1, status);
        CPPUNIT_ASSERT(client->getErrorCode() != 0);

        server.stop();
        END_TEST
    }

    void testTimeoutResetAllowsSubsequentRequest()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);
        client->setTimeouts(100, 100);

        server.queueResponse(200, "OK", "slow", "text/plain", 750);

        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(-1, client->get("/timeout-reset/slow", response));
        CPPUNIT_ASSERT(client->getErrorCode() != 0);

        client->setTimeouts(0, 0);
        response.clear();
        server.queueResponse(200, "OK", "fast", "text/plain", 0);
        CPPUNIT_ASSERT_EQUAL(200, client->get("/timeout-reset/fast", response));

        auto requests = waitForRequests(server, 2);
        CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(2), requests.size());

        server.stop();
        END_TEST
    }

    void testLargePayloadBody()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        std::string payload(16384, 'x');
        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->post("/v1/large", payload.c_str(), "application/octet-stream", response));

        auto requests = waitForRequests(server, 1);
        CPPUNIT_ASSERT_EQUAL(payload, requests[0].body);

        server.stop();
        END_TEST
    }

    void testMultipleSequentialRequests()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        SimpleHttpClientHeaderMap headers1{{"X-Seq", "1"}};
        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->get("/seq/1", headers1, response));
        response.clear();
        SimpleHttpClientHeaderMap headers2;
        CPPUNIT_ASSERT_EQUAL(200, client->put("/seq/2", headers2, "two", "text/plain", response));
        response.clear();
        SimpleHttpClientHeaderMap headers3{{"X-Seq", "3"}};
        CPPUNIT_ASSERT_EQUAL(200, client->post("/seq/3", headers3, "three", "text/plain", response));

        auto requests = waitForRequests(server, 3);
        assertMethodPath(requests[0], "GET", "/seq/1");
        assertMethodPath(requests[1], "PUT", "/seq/2");
        assertMethodPath(requests[2], "POST", "/seq/3");
        CPPUNIT_ASSERT_EQUAL(std::string("3"), headerValue(requests[2], "X-Seq"));

        server.stop();
        END_TEST
    }

    void testBasicAuthHeaderPresent()
    {
        START_TEST
        CurlTestHttpServer server;
        server.startServer();
        Owned<ISimpleHttpClient> client = createClient(server);

        client->setBasicAuth("curlUser", "curlPass");
        StringBuffer response;
        CPPUNIT_ASSERT_EQUAL(200, client->get("/auth", response));

        auto requests = waitForRequests(server, 1);
        const std::string auth = headerValue(requests[0], "Authorization");
        CPPUNIT_ASSERT(!auth.empty());
        CPPUNIT_ASSERT(auth.rfind("Basic ", 0) == 0);

        server.stop();
        END_TEST
    }

    void testConnectFailureSetsError()
    {
        START_TEST
        unsigned short unusedPort = reserveUnusedPort();
        VStringBuffer baseUrl("http://127.0.0.1:%u", unusedPort);

        Owned<ISimpleHttpClient> client = createSimpleHttpClient(baseUrl.str());
        CPPUNIT_ASSERT(client);
        client->setTimeouts(200, 200);

        StringBuffer response;
        int status = client->get("/unavailable", response);
        CPPUNIT_ASSERT_EQUAL(-1, status);
        CPPUNIT_ASSERT(client->getErrorCode() != 0);
        END_TEST
    }

private:
    static ISimpleHttpClient *createClient(const CurlTestHttpServer &server)
    {
        VStringBuffer baseUrl("http://127.0.0.1:%u", server.queryPort());
        return createSimpleHttpClient(baseUrl.str());
    }

    static std::vector<CurlTestHttpRequest> waitForRequests(const CurlTestHttpServer &server, size_t targetCount, unsigned timeoutMs=2000)
    {
        const unsigned iterations = timeoutMs / 10;
        for (unsigned i = 0; i < iterations; i++)
        {
            auto current = server.snapshotRequests();
            if (current.size() >= targetCount)
                return current;
            MilliSleep(10);
        }
        auto requests = server.snapshotRequests();
        CPPUNIT_ASSERT(requests.size() >= targetCount);
        return requests;
    }

    static void assertMethodPath(const CurlTestHttpRequest &request, const char *method, const char *path)
    {
        CPPUNIT_ASSERT_EQUAL(std::string(method), request.method);
        CPPUNIT_ASSERT_EQUAL(std::string(path), request.path);
    }

    static std::string headerValue(const CurlTestHttpRequest &request, const char *name)
    {
        auto it = request.headers.find(name);
        if (it == request.headers.end())
            return "";
        return it->second;
    }

    static unsigned short reserveUnusedPort()
    {
        Owned<ISocket> temp = ISocket::create(randomCurlTestPort());
        SocketEndpoint ep;
        temp->getEndpoint(ep);
        unsigned short p = ep.port;
        temp->close();
        return p;
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibCurlTest);

#endif
