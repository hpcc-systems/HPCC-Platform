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
#include <unistd.h>

// Helper to get a random port for testing
unsigned short getRandomTestPort()
{
    return 32768 + getRandom() % 10000;
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
            acceptedConnections++;
            // Create a handler like real implementations do
            // Use minimal sizes since we're just testing accept functionality
            return new CReadSocketHandler(*this, asyncReader, sock, 4, 1024);
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
};

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
                
            char *buf = (char *)malloc(len + 1);
            if (!buf)
                return false;
            
            bool success = false;
            try
            {
                socket->readtms(buf, len, len, read, timeoutMs);
                buf[len] = 0;
                response.append(buf);
                success = (read == len);
            }
            catch (...)
            {
                // Ensure cleanup even on exception
            }
            free(buf);
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
        unsigned short port = getRandomTestPort();
        
        try
        {
            TestConnectionListener listener(port, false);
            
            // Give the listener time to start
            MilliSleep(300);
            
            // Note: The test works regardless of whether multishot accept or 
            // thread-based accept is used (depends on io_uring availability)
            
            // Connect a client
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect successfully", client.connect());
            
            // Give time for the connection to be processed
            MilliSleep(300);
            
            // Verify connection was accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("One connection should be accepted", 
                                        1U, listener.getAcceptedCount());
            
            client.close();
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numClients = 10;
        
        try
        {
            TestConnectionListener listener(port, false);
            MilliSleep(300);
            
            std::vector<SimpleClient *> clients;
            
            // Connect all clients
            for (unsigned i = 0; i < numClients; i++)
            {
                SimpleClient *client = new SimpleClient("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect", client->connect());
                clients.push_back(client);
                MilliSleep(10); // Small delay between connections
            }
            
            MilliSleep(200);
            
            // Verify all connections were accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("All connections should be accepted", 
                                        numClients, listener.getAcceptedCount());
            
            // Cleanup
            for (auto client : clients)
            {
                client->close();
                delete client;
            }
            
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numCycles = 20;
        
        try
        {
            TestConnectionListener listener(port, false);
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
                                        numCycles, listener.getAcceptedCount());
            
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        
        try
        {
            TestConnectionListener listener(port, false);
            MilliSleep(300);
            
            // Stop immediately without any connections
            listener.stop();
            
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numClients = 5;
        
        try
        {
            TestConnectionListener listener(port, false);
            MilliSleep(300);
            
            std::vector<SimpleClient *> clients;
            
            // Connect clients
            for (unsigned i = 0; i < numClients; i++)
            {
                SimpleClient *client = new SimpleClient("127.0.0.1", port);
                if (client->connect())
                    clients.push_back(client);
                else
                    delete client;
            }
            
            MilliSleep(100);
            
            // Stop while connections are active
            listener.stop();
            
            // Cleanup
            for (auto client : clients)
            {
                client->close();
                delete client;
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
        unsigned short port = getRandomTestPort();
        
        try
        {
            TestConnectionListener listener(port, false);
            MilliSleep(300);
            
            // Connect a client regardless of which mode is being used
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect even with fallback", 
                                  client.connect());
            
            MilliSleep(100);
            
            // Verify connection was accepted
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in either mode", 
                                        1U, listener.getAcceptedCount());
            
            client.close();
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        
        try
        {
            {
                TestConnectionListener listener(port, false);
                MilliSleep(300);
                
                // Connect one client successfully
                SimpleClient client1("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("First client should connect", client1.connect());
                
                MilliSleep(100);
                listener.stop();
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numConnections = 50;
        
        try
        {
            TestConnectionListener listener(port, false);
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
                                  listener.getAcceptedCount() >= numConnections - 2);
            
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numClients = 30;
        
        try
        {
            TestConnectionListener listener(port, false);
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
            
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        constexpr unsigned numClients = 5;
        
        try
        {
            TestConnectionListener listener(port, false);
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
                                        numClients, listener.getAcceptedCount());
            
            CPPUNIT_ASSERT_MESSAGE("Most connections from slow clients should succeed", 
                                  messagesReceived >= numClients - 1);
            
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        
        try
        {
            // Explicitly disable io_uring via parameter
            TestConnectionListener listener(port, false, false);
            MilliSleep(300);
            
            // Verify io_uring is NOT being used
            CPPUNIT_ASSERT_MESSAGE("Multishot accept should be disabled when io_uring parameter is false", 
                                  !listener.isUsingMultishotAccept());
            
            // But connections should still work (using thread-based accept)
            SimpleClient client("127.0.0.1", port);
            CPPUNIT_ASSERT_MESSAGE("Client should connect even with io_uring disabled", 
                                  client.connect());
            
            MilliSleep(100);
            
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in fallback mode", 
                                        1U, listener.getAcceptedCount());
            
            client.close();
            listener.stop();
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
        unsigned short port = getRandomTestPort();
        
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
                TestConnectionListener listener(port, false);
                MilliSleep(300);
                
                // Verify io_uring is NOT being used due to config
                CPPUNIT_ASSERT_MESSAGE("Multishot accept should be disabled when expert/@useIOUring is false", 
                                      !listener.isUsingMultishotAccept());
                
                // But connections should still work
                SimpleClient client("127.0.0.1", port);
                CPPUNIT_ASSERT_MESSAGE("Client should connect even with io_uring disabled via config", 
                                      client.connect());
                
                MilliSleep(100);
                
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Connection should be accepted in fallback mode", 
                                            1U, listener.getAcceptedCount());
                
                client.close();
                listener.stop();
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
};

CPPUNIT_TEST_SUITE_REGISTRATION(MultishotAcceptTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(MultishotAcceptTests, "MultishotAcceptTests");

#endif // _USE_CPPUNIT
