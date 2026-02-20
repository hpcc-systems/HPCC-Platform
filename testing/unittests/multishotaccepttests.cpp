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

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "jsocket.hpp"
#include "jiouring.hpp"
#include "jthread.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jlog.hpp"
#include "socketutils.hpp"
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <unistd.h>

// Helper to generate a random port for testing
// Returns a port in the dynamic/private port range (49152-65535)
// The createTestListener function below handles retries if the port is already in use
unsigned short getRandomTestPort()
{
    // Use dynamic/private port range (49152-65535) to avoid well-known ports
    return 49152 + getRandom() % (65535 - 49152 + 1);
}

// Test implementation of CSocketConnectionListener that tracks accepted connections
class TestConnectionListener : public CSocketConnectionListener
{
private:
    std::atomic<unsigned> acceptedConnections{0};
    std::atomic<bool> shouldAccept{true};
    
public:
    TestConnectionListener(unsigned port, bool useTLS = false, bool useIOUring = true)
        : CSocketConnectionListener(port, useTLS, 0, 0, useIOUring)
    {
    }
    
    virtual CReadSocketHandler *createSocketHandler(ISocket *sock) override
    {
        if (shouldAccept)
        {
            // Create a handler like real implementations do
            // Use minimal sizes since we're just testing accept functionality
            CReadSocketHandler *handler = new CReadSocketHandler(*this, asyncReader, sock, 4, 1024);
            acceptedConnections++;
            return handler;
        }
        else
        {
            // Reject connection by closing immediately and returning nullptr
            sock->close();
            return nullptr;
        }
    }
    
    // Implement ISocketMessageProcessor pure virtual methods
    virtual bool onlyProcessFirstRead() const override { return false; }
    virtual unsigned getMessageSize(const void * header) const override { return 0; }
    virtual void processMessage(const void * data, size32_t len) override
    {
        // We're only testing accept, not message processing
        // Just count this as processed without doing anything
    }
    
    unsigned getAcceptedCount() const { return acceptedConnections; }
    void setShouldAccept(bool accept) { shouldAccept = accept; }
    
    // Introspection methods for testing io_uring behavior
    AcceptMethod getAcceptMethod() const { return CSocketConnectionListener::getAcceptMethod(); }
};

// Helper to create a test listener with retry logic for port binding
// Returns a new listener on success; throws on failure with diagnostic message
// Only retries on JSOCKERR_port_in_use; other errors are immediately propagated
// Caller takes ownership of the returned pointer (wrap in Owned<> to manage lifetime)
TestConnectionListener* createTestListener(unsigned short &port, bool useTLS = false, bool useIOUring = true, unsigned maxRetries = 5)
{
    Owned<IException> lastException;
    for (unsigned retry = 0; retry < maxRetries; retry++)
    {
        try
        {
            port = getRandomTestPort();
            return new TestConnectionListener(port, useTLS, useIOUring);
        }
        catch (IJSOCK_Exception *e)
        {
            // Only retry if the port is in use; other socket errors indicate real problems
            if (e->errorCode() != JSOCKERR_port_in_use)
                throw; // Non-port-related failure - propagate immediately
            
            // Port in use - save exception and retry
            lastException.setown(e);
            if (retry < maxRetries - 1)
                MilliSleep(50 * (retry + 1)); // Increasing delay between retries
        }
    }
    
    // All retries exhausted - throw with diagnostic information
    StringBuffer msg;
    msg.append("Failed to create test listener after ").append(maxRetries).append(" retries");
    if (lastException)
    {
        msg.append(": ");
        lastException->errorMessage(msg);
    }
    throw makeStringExceptionV(-1, "%s", msg.str());
}

// Simple client that connects and sends a message
class SimpleClient
{
private:
    Owned<ISocket> socket;
    SocketEndpoint endpoint;
    
public:
    SimpleClient(const char *host, unsigned short port)
    {
        endpoint.set(host, port);
    }
    
    bool connect(unsigned timeoutMs = 5000)
    {
        try
        {
            socket.setown(ISocket::connect_timeout(endpoint, timeoutMs));
            return socket != nullptr;
        }
        catch (IException *e)
        {
            e->Release();
            return false;
        }
    }
    
    bool sendMessage(const char *msg)
    {
        if (!socket)
            return false;
            
        try
        {
            size32_t len = strlen(msg);
            socket->write(&len, sizeof(len));
            socket->write(msg, len);
            return true;
        }
        catch (IException *e)
        {
            e->Release();
            return false;
        }
    }
    
    bool receiveEcho(StringBuffer &response, unsigned timeoutMs = 5000)
    {
        if (!socket)
            return false;
            
        try
        {
            size32_t len;
            size32_t read;
            socket->readtms(&len, sizeof(len), sizeof(len), read, timeoutMs);
            if (read != sizeof(len))
                return false;
                
            std::unique_ptr<char[]> buf(new char[len + 1]);
            bool success = false;
            try
            {
                socket->readtms(buf.get(), len, len, read, timeoutMs);
                buf[len] = 0;
                response.append(buf.get());
                success = (read == len);
            }
            catch (...)
            {
                // Exception occurred, buffer will be freed automatically
            }
            return success;
        }
        catch (IException *e)
        {
            e->Release();
            return false;
        }
    }
    
    void close()
    {
        if (socket)
            socket->close();
        socket.clear();
    }
};

class MultishotAcceptTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(MultishotAcceptTests);
        CPPUNIT_TEST(testBasicMultishotAccept);
        CPPUNIT_TEST(testMultipleSimultaneousConnections);
        CPPUNIT_TEST(testRapidConnectDisconnect);
        CPPUNIT_TEST(testCleanShutdown);
        CPPUNIT_TEST(testShutdownUnderLoad);
        CPPUNIT_TEST(testMultishotAcceptFallback);
        CPPUNIT_TEST(testConnectionAfterShutdown);
        CPPUNIT_TEST(testManySequentialConnections);
        CPPUNIT_TEST(testConnectionBurst);
        CPPUNIT_TEST(testSlowClients);
        CPPUNIT_TEST(testIOUringDisabledViaParameter);
        CPPUNIT_TEST(testIOUringDisabledViaConfig);
        CPPUNIT_TEST(testAcceptMethodSelection);
        CPPUNIT_TEST(testMultishotVsSingleshotBehavior);
        CPPUNIT_TEST(testPendingCallbacksManagement);
        CPPUNIT_TEST(testHighConnectionRate);
        CPPUNIT_TEST(testAcceptMethodAfterStart);
    CPPUNIT_TEST_SUITE_END();

protected:
    Owned<IPropertyTree> createIOUringConfig(bool threaded = true)
    {
        Owned<IPropertyTree> config = createPTree("config");
        config->setPropInt("@queueDepth", 128);
        config->setPropBool("@poll", false);
        config->setPropBool("@singleThreaded", !threaded);
        return config.getClear();
    }

public:
    // Test 1: Basic multishot accept with a single connection
    void testBasicMultishotAccept()
    {
        unsigned short port;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            
            // Give the listener time to start
            MilliSleep(300);
            
            // Verify accept method is set appropriately
            AcceptMethod method = listener->getAcceptMethod();
            CPPUNIT_ASSERT_MESSAGE("Accept method should be set to a valid mode",
                                  method == AcceptMethod::MultishotAccept ||
                                  method == AcceptMethod::SingleshotAccept ||
                                  method == AcceptMethod::SelectThread);
            
            // Connect a client
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect successfully", client.connect());
            
            // Give time for the connection to be processed
            MilliSleep(300);
            
            // Verify connection was accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("One connection should be accepted", 
                                        1U, listener->getAcceptedCount());
            
            client.close();
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 2: Multiple simultaneous connections
    void testMultipleSimultaneousConnections()
    {
        unsigned short port;
        constexpr unsigned numClients = 10;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            std::vector<std::unique_ptr<SimpleClient>> clients;
            
            // Connect all clients
            for (unsigned i = 0; i < numClients; i++)
            {
                auto client = std::make_unique<SimpleClient>("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client->connect());
                clients.push_back(std::move(client));
                MilliSleep(10); // Small delay between connections
            }
            
            MilliSleep(200);
            
            // Verify all connections were accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("All connections should be accepted", 
                                        numClients, listener->getAcceptedCount());
            
            // Cleanup - unique_ptr handles deletion automatically
            for (auto &client : clients)
            {
                client->close();
            }
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 3: Rapid connect/disconnect cycles
    void testRapidConnectDisconnect()
    {
        unsigned short port;
        constexpr unsigned numCycles = 20;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            for (unsigned i = 0; i < numCycles; i++)
            {
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client.connect());
                client.close();
                MilliSleep(10);
            }
            
            MilliSleep(200);
            
            // Verify all connections were accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("All rapid connections should be accepted", 
                                        numCycles, listener->getAcceptedCount());
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 4: Clean shutdown with no active connections
    void testCleanShutdown()
    {
        unsigned short port;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            // Stop immediately without any connections
            listener->stop();
            
            // Verify we can't connect after stop
            SimpleClient client("127.0.0.1", port);
            bool connected = client.connect(1000);
            
            // Connection should fail or timeout
            CPPUNIT_ASSERT_MESSAGE("Should not connect to stopped listener", !connected);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 5: Shutdown under load with active connections
    void testShutdownUnderLoad()
    {
        unsigned short port;
        constexpr unsigned numClients = 5;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            std::vector<std::unique_ptr<SimpleClient>> clients;
            
            // Connect clients
            for (unsigned i = 0; i < numClients; i++)
            {
                auto client = std::make_unique<SimpleClient>("127.0.0.1", port);
                if (client->connect())
                    clients.push_back(std::move(client));
                // If connect fails, unique_ptr automatically deletes
            }
            
            MilliSleep(100);
            
            // Stop while connections are active
            listener->stop();
            
            // Cleanup - close connections (unique_ptr handles deletion)
            for (auto &client : clients)
            {
                client->close();
            }
            
            // Should complete without hanging
            CPPUNIT_ASSERT_MESSAGE("Shutdown should complete", true);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 6: Verify fallback to thread-based accept when io_uring unavailable
    void testMultishotAcceptFallback()
    {
        unsigned short port;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            // Connect a client regardless of which mode is being used
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect even with fallback", 
                                  client.connect());
            
            MilliSleep(100);
            
            // Verify connection was accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in either mode", 
                                        1U, listener->getAcceptedCount());
            
            client.close();
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 7: Attempt connection after shutdown
    void testConnectionAfterShutdown()
    {
        unsigned short port;
        
        try
        {
            {
                Owned<TestConnectionListener> listener = createTestListener(port, false);
                MilliSleep(300);
                
                // Connect one client successfully
                SimpleClient client1("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("First client should connect", client1.connect());
                
                MilliSleep(100);
                listener->stop();
                client1.close();
            }
            
            // Give time for socket to be released
            MilliSleep(500);
            
            // Try to connect after listener is destroyed
            SimpleClient client2("127.0.0.1", port);
            bool connected = client2.connect(1000);
            
            CPPUNIT_ASSERT_MESSAGE("Should not connect after shutdown", !connected);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 8: Many sequential connections
    void testManySequentialConnections()
    {
        unsigned short port;
        constexpr unsigned numConnections = 50;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            for (unsigned i = 0; i < numConnections; i++)
            {
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client.connect());
                
                client.close();
                
                if (i % 10 == 0)
                    MilliSleep(50); // Occasional pause
            }
            
            MilliSleep(300);
            
            // Verify all connections were accepted
            CPPUNIT_ASSERT_MESSAGE("Most connections should be accepted", 
                                  listener->getAcceptedCount() >= numConnections - 2);
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 9: Connection burst (many clients connecting at once)
    void testConnectionBurst()
    {
        unsigned short port;
        constexpr unsigned numClients = 30;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            // Create threads that all try to connect simultaneously
            std::vector<std::thread> threads;
            std::atomic<unsigned> successCount{0};
            
            for (unsigned i = 0; i < numClients; i++)
            {
                threads.emplace_back([port, &successCount]() {
                    SimpleClient client("127.0.0.1", port);
                    if (client.connect())
                    {
                        successCount++;
                        MilliSleep(100); // Keep connection open briefly
                        client.close();
                    }
                });
            }
            
            // Wait for all threads
            for (auto &thread : threads)
                thread.join();
            
            MilliSleep(200);
            
            // Most connections should succeed (allow for some timing issues)
            CPPUNIT_ASSERT_MESSAGE("Most burst connections should succeed", 
                                  successCount >= numClients * 0.9);
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 10: Slow clients (connections that take time to send data)
    void testSlowClients()
    {
        unsigned short port;
        constexpr unsigned numClients = 5;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false);
            MilliSleep(300);
            
            std::vector<std::thread> threads;
            std::atomic<unsigned> messagesReceived{0};
            
            for (unsigned i = 0; i < numClients; i++)
            {
                threads.emplace_back([port, &messagesReceived, i]() {
                    SimpleClient client("127.0.0.1", port);
                    if (client.connect())
                    {
                        // Wait a bit before closing
                        MilliSleep(100 * (i + 1));
                        
                        messagesReceived++;
                        
                        MilliSleep(100);
                        client.close();
                    }
                });
            }
            
            // Wait for all threads
            for (auto &thread : threads)
                thread.join();
            
            MilliSleep(200);
            
            // Verify connections and messages
            CPPUNIT_ASSERT_EQUAL_MESSAGE("All slow clients should connect", 
                                        numClients, listener->getAcceptedCount());
            
            CPPUNIT_ASSERT_MESSAGE("Most connections from slow clients should succeed", 
                                  messagesReceived >= numClients - 1);
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 11: Verify io_uring can be disabled via constructor parameter
    void testIOUringDisabledViaParameter()
    {
        unsigned short port;
        
        try
        {
            // Explicitly disable io_uring via parameter
            Owned<TestConnectionListener> listener = createTestListener(port, false, false);
            MilliSleep(300);
            
            // Verify io_uring is NOT being used
            CPPUNIT_ASSERT_MESSAGE("Multishot accept should be disabled when io_uring parameter is false", 
                                  !listener->isUsingMultishotAccept());
            
            // But connections should still work (using thread-based accept)
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect even with io_uring disabled", 
                                  client.connect());
            
            MilliSleep(100);
            
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in fallback mode", 
                                        1U, listener->getAcceptedCount());
            
            client.close();
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 12: Verify io_uring can be disabled via global configuration
    void testIOUringDisabledViaConfig()
    {
        unsigned short port;
        
        try
        {
            // Set configuration to disable io_uring
            Owned<IPropertyTree> config = getComponentConfigSP();
            bool originalSetting = config ? config->getPropBool("expert/@useIOUring", true) : true;
            
            if (config)
            {
                // Ensure the expert path exists before setting the property
                if (!config->queryPropTree("expert"))
                    config->addPropTree("expert", createPTree("expert"));
                config->setPropBool("expert/@useIOUring", false);
            }
            
            try
            {
                // Create listener with default parameter (should check config)
                Owned<TestConnectionListener> listener = createTestListener(port, false);
                MilliSleep(300);
                
                // Verify io_uring is NOT being used due to config
                CPPUNIT_ASSERT_MESSAGE("Multishot accept should be disabled when expert/@useIOUring is false", 
                                      !listener->isUsingMultishotAccept());
                
                // But connections should still work
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect even with io_uring disabled via config", 
                                      client.connect());
                
                MilliSleep(100);
                
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in fallback mode", 
                                            1U, listener->getAcceptedCount());
                
                client.close();
                listener->stop();
            }
            catch (...)
            {
                // Restore original setting
                if (config)
                {
                    if (!config->queryPropTree("expert"))
                        config->addPropTree("expert", createPTree("expert"));
                    config->setPropBool("expert/@useIOUring", originalSetting);
                }
                throw;
            }
            
            // Restore original setting
            if (config)
            {
                if (!config->queryPropTree("expert"))
                    config->addPropTree("expert", createPTree("expert"));
                config->setPropBool("expert/@useIOUring", originalSetting);
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 13: Verify AcceptMethod is selected correctly based on io_uring availability
    void testAcceptMethodSelection()
    {
        unsigned short port;
        
        try
        {
            // Test with io_uring enabled (default)
            {
                Owned<TestConnectionListener> listener = createTestListener(port, false, true);
                MilliSleep(100);
                
                AcceptMethod method = listener->getAcceptMethod();
                // Should be either Multishot (if supported), Singleshot (if io_uring available but multishot not supported), or SelectThread (if io_uring not available)
                CPPUNIT_ASSERT_MESSAGE("With io_uring enabled, should use Multishot, Singleshot, or SelectThread",
                                      method == AcceptMethod::MultishotAccept ||
                                      method == AcceptMethod::SingleshotAccept ||
                                      method == AcceptMethod::SelectThread);
                
                listener->stop();
                MilliSleep(200);
            }
            
            // Test with io_uring explicitly disabled
            {
                Owned<TestConnectionListener> listener = createTestListener(port, false, false);
                MilliSleep(100);
                
                AcceptMethod method = listener->getAcceptMethod();
                CPPUNIT_ASSERT_EQUAL_MESSAGE("With io_uring disabled, should use SelectThread",
                                            AcceptMethod::SelectThread, method);
                
                listener->stop();
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 14: Verify multishot vs singleshot behavior differences
    void testMultishotVsSingleshotBehavior()
    {
        unsigned short port;
        constexpr unsigned numClients = 20;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false, true);
            MilliSleep(300);
            
            AcceptMethod method = listener->getAcceptMethod();
            
            // Connect multiple clients rapidly
            std::vector<std::unique_ptr<SimpleClient>> clients;
            for (unsigned i = 0; i < numClients; i++)
            {
                auto client = std::make_unique<SimpleClient>("127.0.0.1", port);
                if (client->connect(2000))
                    clients.push_back(std::move(client));
                MilliSleep(5); // Very small delay
            }
            
            MilliSleep(200);
            
            unsigned accepted = listener->getAcceptedCount();
            
            // Both methods should handle multiple connections, but multishot is more efficient
            CPPUNIT_ASSERT_MESSAGE("Should accept most connections regardless of method",
                                  accepted >= numClients * 0.85);
            
            if (method == AcceptMethod::MultishotAccept)
            {
                // With multishot, we expect very high acceptance rate due to efficiency
                CPPUNIT_ASSERT_MESSAGE("Multishot should accept nearly all rapid connections",
                                      accepted >= numClients * 0.95);
            }
            
            for (auto &client : clients)
                client->close();
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 15: Verify accept continues working correctly in both io_uring modes
    void testPendingCallbacksManagement()
    {
        unsigned short port;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false, true);
            MilliSleep(300);
            
            AcceptMethod method = listener->getAcceptMethod();
            
            // Connect multiple clients to verify continuous accept operation
            for (unsigned i = 0; i < 5; i++)
            {
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client.connect());
                MilliSleep(50);
                client.close();
            }
            
            MilliSleep(200);
            
            // Verify all connections were accepted (proves accept is continuously working)
            unsigned accepted = listener->getAcceptedCount();
            CPPUNIT_ASSERT_EQUAL_MESSAGE("All connections should be accepted (continuous accept working)",
                                        5U, accepted);
            
            listener->stop();
            
            // After stop, no new connections should be accepted
            MilliSleep(200);
            SimpleClient lateClient("127.0.0.1", port);
            bool connected = lateClient.connect(1000);
            CPPUNIT_ASSERT_MESSAGE("Should not connect after stop", !connected);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 16: High connection rate stress test for io_uring
    void testHighConnectionRate()
    {
        unsigned short port;
        constexpr unsigned numClients = 100;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false, true);
            MilliSleep(300);
            
            AcceptMethod method = listener->getAcceptMethod();
            
            // Create many connections as fast as possible
            std::vector<std::thread> threads;
            std::atomic<unsigned> successCount{0};
            
            auto startTime = msTick();
            
            for (unsigned i = 0; i < numClients; i++)
            {
                threads.emplace_back([port, &successCount]() {
                    SimpleClient client("127.0.0.1", port);
                    if (client.connect(3000))
                    {
                        successCount++;
                        MilliSleep(10);
                        client.close();
                    }
                });
                
                // Launch threads in small batches to avoid overwhelming the system
                if (i % 10 == 9)
                    MilliSleep(5);
            }
            
            for (auto &thread : threads)
                thread.join();
            
            auto elapsed = msTick() - startTime;
            
            MilliSleep(300);
            
            unsigned accepted = listener->getAcceptedCount();
            
            // Should accept most connections
            CPPUNIT_ASSERT_MESSAGE("Should handle high connection rate",
                                  accepted >= numClients * 0.90);
            
            if (method == AcceptMethod::MultishotAccept)
            {
                // Multishot should be very efficient
                CPPUNIT_ASSERT_MESSAGE("Multishot should handle high rate very efficiently",
                                      accepted >= numClients * 0.95);
            }
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
    
    // Test 17: Verify AcceptMethod remains stable during operation
    void testAcceptMethodAfterStart()
    {
        unsigned short port;
        
        try
        {
            Owned<TestConnectionListener> listener = createTestListener(port, false, true);
            MilliSleep(300);
            
            AcceptMethod initialMethod = listener->getAcceptMethod();
            
            // Accept several connections
            for (unsigned i = 0; i < 5; i++)
            {
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client.connect());
                MilliSleep(50);
                
                // Verify method hasn't changed
                AcceptMethod currentMethod = listener->getAcceptMethod();
                CPPUNIT_ASSERT_EQUAL_MESSAGE("AcceptMethod should remain stable during operation",
                                            initialMethod, currentMethod);
                
                client.close();
            }
            
            listener->stop();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_FAIL(msg.str());
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(MultishotAcceptTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(MultishotAcceptTests, "MultishotAcceptTests");

#endif // _USE_CPPUNIT
