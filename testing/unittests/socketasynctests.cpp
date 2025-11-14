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
#include <thread>
#include <atomic>

unsigned short randomPort()
{
    return 32768 + getRandom() % 10000;
}

// RAII wrapper for sockaddr to prevent memory leaks
class SockAddrHolder
{
private:
    struct sockaddr *addr = nullptr;
    
public:
    SockAddrHolder() = default;
    
    ~SockAddrHolder()
    {
        if (addr)
            free(addr);
    }
    
    // Non-copyable
    SockAddrHolder(const SockAddrHolder&) = delete;
    SockAddrHolder& operator=(const SockAddrHolder&) = delete;
    
    // Get reference to the pointer for use with createForAsyncConnect
    struct sockaddr *& getRef() { return addr; }
    
    // Get the pointer value
    struct sockaddr * get() const { return addr; }
    
    // Release ownership (caller takes responsibility for freeing)
    struct sockaddr * release()
    {
        struct sockaddr *tmp = addr;
        addr = nullptr;
        return tmp;
    }
};

// Helper class to create a simple echo server for testing
class SimpleEchoServer : public Thread
{
private:
    Owned<ISocket> listenSocket;
    std::atomic<bool> running{true};
    unsigned short port;
    Semaphore started;
    
public:
    SimpleEchoServer() : Thread("SimpleEchoServer"), port(0)
    {
    }
    
    ~SimpleEchoServer()
    {
        stop();
    }
    
    void start()
    {
        Thread::start(false);
        started.wait();
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
    
    unsigned short getPort() const { return port; }
    
    virtual int run() override
    {
        try
        {
            // Create listening socket on any available port
            listenSocket.setown(ISocket::create(randomPort()));
            SocketEndpoint ep;
            listenSocket->getEndpoint(ep);
            port = ep.port;
            
            started.signal();
            
            while (running)
            {
                try
                {
                    Owned<ISocket> client = listenSocket->accept(true);
                    if (!client || !running)
                        break;
                        
                    // Simple echo - read a message and send it back
                    char buffer[1024];
                    size32_t bytesRead = 0;
                    client->read(buffer, 1, sizeof(buffer), bytesRead, 5);
                    if (bytesRead > 0)
                    {
                        client->write(buffer, bytesRead);
                    }
                    client->close();
                }
                catch (IJSOCK_Exception *e)
                {
                    switch (e->errorCode())
                    {
                        case JSOCKERR_cancel_accept:
                        case JSOCKERR_graceful_close:
                            running = false;
                            break;
                    }
                    e->Release();
                }
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("SimpleEchoServer error: %s", msg.str());
            e->Release();
        }
        return 0;
    }
};

// Helper class to simulate a server that disconnects after first connection
class DisconnectingServer : public Thread
{
private:
    Owned<ISocket> listenSocket;
    std::atomic<bool> running{true};
    std::atomic<unsigned> connectCount{0};
    unsigned short port;
    Semaphore started;
    
public:
    DisconnectingServer() : Thread("DisconnectingServer"), port(0)
    {
    }
    
    ~DisconnectingServer()
    {
        stop();
    }
    
    void start()
    {
        Thread::start(false);
        started.wait();
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
    
    unsigned short getPort() const { return port; }
    unsigned getConnectCount() const { return connectCount; }
    
    virtual int run() override
    {
        try
        {
            listenSocket.setown(ISocket::create(randomPort()));
            SocketEndpoint ep;
            listenSocket->getEndpoint(ep);
            port = ep.port;
            
            started.signal();
            
            while (running)
            {
                try
                {
                    Owned<ISocket> client = listenSocket->accept(true);
                    if (!client || !running)
                        break;
                        
                    connectCount++;
                    
                    // Read one byte then immediately close to simulate disconnect
                    char buffer[1];
                    size32_t bytesRead = 0;
                    client->read(buffer, 1, 1, bytesRead, 1);
                    client->close();
                }
                catch (IJSOCK_Exception *e)
                {
                    switch (e->errorCode())
                    {
                        case JSOCKERR_cancel_accept:
                        case JSOCKERR_graceful_close:
                            running = false;
                            break;
                    }
                    e->Release();
                }
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("DisconnectingServer error: %s", msg.str());
            e->Release();
        }
        return 0;
    }
};

// Helper class that periodically disconnects clients for stress testing
class PeriodicDisconnectServer : public Thread
{
private:
    Owned<ISocket> listenSocket;
    std::atomic<bool> running{true};
    std::atomic<unsigned> messageCount{0};
    std::atomic<unsigned> disconnectCount{0};
    unsigned short port;
    Semaphore started;
    unsigned messagesBeforeDisconnect;
    
public:
    PeriodicDisconnectServer(unsigned _messagesBeforeDisconnect = 5) 
        : Thread("PeriodicDisconnectServer"), port(0), messagesBeforeDisconnect(_messagesBeforeDisconnect)
    {
    }
    
    ~PeriodicDisconnectServer()
    {
        stop();
    }
    
    void start()
    {
        Thread::start(false);
        started.wait();
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
    
    unsigned short getPort() const { return port; }
    unsigned getMessageCount() const { return messageCount; }
    unsigned getDisconnectCount() const { return disconnectCount; }
    
    virtual int run() override
    {
        try
        {
            listenSocket.setown(ISocket::create(randomPort()));
            SocketEndpoint ep;
            listenSocket->getEndpoint(ep);
            port = ep.port;
            
            started.signal();
            
            while (running)
            {
                try
                {
                    Owned<ISocket> client = listenSocket->accept(true);
                    if (!client || !running)
                        break;
                    
                    unsigned localMsgCount = 0;
                    while (running)
                    {
                        // Echo messages back
                        char buffer[1024];
                        size32_t bytesRead = 0;
                        
                        try
                        {
                            client->read(buffer, 1, sizeof(buffer), bytesRead, 2);
                            if (bytesRead == 0)
                                break;
                                
                            messageCount++;
                            localMsgCount++;
                            
                            // Echo back
                            client->write(buffer, bytesRead);
                            
                            // Disconnect after N messages to simulate network issues
                            if (localMsgCount >= messagesBeforeDisconnect)
                            {
                                disconnectCount++;
                                break;
                            }
                        }
                        catch (IJSOCK_Exception *e)
                        {
                            e->Release();
                            break;
                        }
                    }
                    client->close();
                }
                catch (IJSOCK_Exception *e)
                {
                    switch (e->errorCode())
                    {
                        case JSOCKERR_cancel_accept:
                        case JSOCKERR_graceful_close:
                            running = false;
                            break;
                    }
                    e->Release();
                }
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            DBGLOG("PeriodicDisconnectServer error: %s", msg.str());
            e->Release();
        }
        return 0;
    }
};

class AsyncSocketConnectionTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AsyncSocketConnectionTests);
        CPPUNIT_TEST(testSyncConnect);
        CPPUNIT_TEST(testAsyncConnectBasic);
        CPPUNIT_TEST(testMultipleAsyncConnections);
        CPPUNIT_TEST(testAsyncConnectFailure);
        CPPUNIT_TEST(testAsyncReconnect);
        CPPUNIT_TEST(testAsyncConnectTimeout);
        CPPUNIT_TEST(testMultipleReconnects);
        CPPUNIT_TEST(testDataIntegrityAcrossReconnects);
        CPPUNIT_TEST(testRapidConnectDisconnect);
        CPPUNIT_TEST(testAsyncConnectCancellation);
    CPPUNIT_TEST_SUITE_END();

protected:
    Owned<IPropertyTree> createIOUringConfig(bool threaded = true)
    {
        Owned<IPropertyTree> config = createPTree("config");
        config->setPropInt("@queueDepth", 64);
        config->setPropBool("@poll", false);
        config->setPropBool("@singleThreaded", !threaded);
        return config.getClear();
    }

public:
    void testSyncConnect()
    {
        SimpleEchoServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        // Test synchronous connection
        Owned<ISocket> socket;
        try
        {
            socket.setown(ISocket::connect_timeout(ep, 5000));
            ASSERT(socket.get() != nullptr);
            
            // Verify we can communicate
            const char *msg = "Hello";
            socket->write(msg, strlen(msg));
            
            char buffer[100];
            size32_t bytesRead = 0;
            socket->read(buffer, strlen(msg), sizeof(buffer), bytesRead, 5);
            ASSERT(bytesRead == strlen(msg));
            ASSERT(memcmp(buffer, msg, strlen(msg)) == 0);
            
            socket->close();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        server.stop();
    }
    
    void testAsyncConnectBasic()
    {
        SimpleEchoServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            server.stop();
            return;
        }
        
        class ConnectCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            SockAddrHolder addrHolder;
            size32_t addrlen = 0;
            
            Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
            ASSERT(socket.get() != nullptr);
            ASSERT(addrHolder.get() != nullptr);
            ASSERT(addrlen > 0);
            
            ConnectCallback callback;
            uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
            
            // Wait for async connection to complete
            bool signaled = callback.completed.wait(5000);
            ASSERT(signaled);
            ASSERT(callback.result >= 0);
            
            // Complete the connection
            ISocket::completeAsyncConnect(socket, callback.result);
            
            // Verify socket is connected
            const char *msg = "AsyncBasic";
            socket->write(msg, strlen(msg));
            
            char buffer[100];
            size32_t bytesRead = 0;
            socket->read(buffer, strlen(msg), sizeof(buffer), bytesRead, 5);
            ASSERT(bytesRead == strlen(msg));
            ASSERT(memcmp(buffer, msg, strlen(msg)) == 0);
            
            socket->close();
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        uring->terminate();
        server.stop();
    }
    
    void testMultipleAsyncConnections()
    {
        SimpleEchoServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            server.stop();
            return;
        }
        
        constexpr unsigned numConnections = 5;
        
        class MultiConnectCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            std::vector<Owned<ISocket>> sockets;
            std::vector<SockAddrHolder> addrHolders(numConnections);
            std::vector<MultiConnectCallback> callbacks;
            
            callbacks.resize(numConnections);
            
            // Create and queue multiple connections
            for (unsigned i = 0; i < numConnections; i++)
            {
                size32_t addrlen = 0;
                
                Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolders[i].getRef(), addrlen);
                uring->enqueueSocketConnect(socket, addrHolders[i].get(), addrlen, callbacks[i]);
                
                sockets.push_back(std::move(socket));
            }
            
            // Wait for all to complete
            for (unsigned i = 0; i < numConnections; i++)
            {
                bool signaled = callbacks[i].completed.wait(5000);
                ASSERT(signaled);
                ASSERT(callbacks[i].result >= 0);
                
                ISocket::completeAsyncConnect(sockets[i], callbacks[i].result);
            }
            
            // Clean up sockets
            for (unsigned i = 0; i < numConnections; i++)
            {
                sockets[i]->close();
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        uring->terminate();
        server.stop();
    }
    
    void testAsyncConnectFailure()
    {
        // Try to connect to a non-existent server
        SocketEndpoint ep("127.0.0.1", 1); // Port 1 should not be listening
        
        try
        {
            SockAddrHolder addrHolder;
            size32_t addrlen = 0;
            
            Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
            
            // Simulate failed async connect
            try
            {
                ISocket::completeAsyncConnect(socket, -1);
                CPPUNIT_FAIL("Expected exception for failed connection");
            }
            catch (IJSOCK_Exception *e)
            {
                // Expected
                ASSERT(e->errorCode() == JSOCKERR_connection_failed);
                e->Release();
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
    }
    
    void testAsyncReconnect()
    {
        DisconnectingServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            server.stop();
            return;
        }
        
        class ReconnectCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };

        try
        {
            // First connection
            {
                SockAddrHolder addrHolder;
                size32_t addrlen = 0;
                
                Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
                ReconnectCallback callback;
                
                uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
                callback.completed.wait(5000);
                
                ASSERT(callback.result >= 0);
                ISocket::completeAsyncConnect(socket, callback.result);
                
                // Send data - server will disconnect
                char data = 'X';
                socket->write(&data, 1);
            }
            
            // Wait a bit for the disconnect to happen
            MilliSleep(1000);
            
            // Second connection (reconnect)
            {
                SockAddrHolder addrHolder;
                size32_t addrlen = 0;
                
                Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
                ReconnectCallback callback;
                
                uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
                callback.completed.wait(5000);
                
                ASSERT(callback.result >= 0);
                ISocket::completeAsyncConnect(socket, callback.result);
                
                // Verify we reconnected
                ASSERT(server.getConnectCount() >= 2);
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        uring->terminate();
        server.stop();
    }
    
    void testAsyncConnectTimeout()
    {
        // Try to connect to a non-routable IP (should timeout)
        // Using TEST-NET-1 from RFC 5737 which is reserved for documentation
        SocketEndpoint ep("192.0.2.1", 9999);
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            return;
        }
        
        class TimeoutCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            SockAddrHolder addrHolder;
            size32_t addrlen = 0;
            
            Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
            TimeoutCallback callback;
            
            uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
            
            // Wait with a short timeout
            bool signaled = callback.completed.wait(2000);
            
            // Connection should either timeout or fail
            if (signaled)
            {
                // If it completed, it should have failed
                ASSERT(callback.result < 0);
            }
        }
        catch (IException *e)
        {
            // Timeout or connection failure is expected
            e->Release();
        }
        
        uring->terminate();
    }
    
    // Test multiple reconnections in succession
    void testMultipleReconnects()
    {
        DisconnectingServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            server.stop();
            return;
        }
        
        class ReconnectCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            const unsigned numReconnects = 10;
            
            for (unsigned i = 0; i < numReconnects; i++)
            {
                SockAddrHolder addrHolder;
                size32_t addrlen = 0;
                
                Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
                ReconnectCallback callback;
                
                uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
                
                bool signaled = callback.completed.wait(5000);
                ASSERT(signaled);
                ASSERT(callback.result >= 0);
                
                ISocket::completeAsyncConnect(socket, callback.result);
                
                // Send data - server will disconnect
                char data = 'X';
                socket->write(&data, 1);
                
                // Brief pause between reconnects
                MilliSleep(100);
            }
            
            // Verify we had the expected number of connections
            ASSERT(server.getConnectCount() >= numReconnects);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        uring->terminate();
        server.stop();
    }
    
    // Test data integrity across multiple reconnections
    void testDataIntegrityAcrossReconnects()
    {
        PeriodicDisconnectServer server(3); // Disconnect after 3 messages
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        try
        {
            const unsigned totalMessages = 20;
            unsigned messagesSent = 0;
            unsigned reconnections = 0;
            
            while (messagesSent < totalMessages)
            {
                Owned<ISocket> socket;
                try
                {
                    socket.setown(ISocket::connect_timeout(ep, 5000));
                    
                    // Send messages until disconnect
                    while (messagesSent < totalMessages)
                    {
                        char sendBuf[64];
                        sprintf(sendBuf, "Message_%u", messagesSent);
                        size_t msgLen = strlen(sendBuf);
                        
                        try
                        {
                            socket->write(sendBuf, msgLen);
                            
                            // Read echo
                            char recvBuf[64];
                            size32_t bytesRead = 0;
                            socket->read(recvBuf, msgLen, sizeof(recvBuf), bytesRead, 2);
                            
                            ASSERT(bytesRead == msgLen);
                            ASSERT(memcmp(sendBuf, recvBuf, msgLen) == 0);
                            
                            messagesSent++;
                        }
                        catch (IJSOCK_Exception *e)
                        {
                            // Expected disconnection
                            e->Release();
                            reconnections++;
                            break;
                        }
                    }
                }
                catch (IException *e)
                {
                    e->Release();
                    MilliSleep(100); // Brief pause before reconnect
                }
            }
            
            ASSERT(messagesSent == totalMessages);
            ASSERT(reconnections > 0); // Should have had at least one reconnection
            DBGLOG("Sent %u messages with %u reconnections", messagesSent, reconnections);
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        server.stop();
    }
    
    // Test rapid connect/disconnect cycles
    void testRapidConnectDisconnect()
    {
        SimpleEchoServer server;
        server.start();
        
        SocketEndpoint ep("127.0.0.1", server.getPort());
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            server.stop();
            return;
        }
        
        class RapidCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            const unsigned numCycles = 20;
            
            for (unsigned i = 0; i < numCycles; i++)
            {
                SockAddrHolder addrHolder;
                size32_t addrlen = 0;
                
                Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
                RapidCallback callback;
                
                uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
                
                bool signaled = callback.completed.wait(5000);
                ASSERT(signaled);
                ASSERT(callback.result >= 0);
                
                ISocket::completeAsyncConnect(socket, callback.result);
                
                // Immediately close - stress test rapid connect/disconnect
                socket->close();
                
                // No delay - connect again immediately
            }
        }
        catch (IException *e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            CPPUNIT_FAIL(msg.str());
            e->Release();
        }
        
        uring->terminate();
        server.stop();
    }
    
    // Test cancelling an async connection
    void testAsyncConnectCancellation()
    {
        // Connect to non-routable address that will hang
        SocketEndpoint ep("192.0.2.1", 9999);
        
        Owned<IPropertyTree> config = createIOUringConfig();
        Owned<IAsyncProcessor> uring = createURingProcessor(config, true);
        
        if (!uring)
        {
            DBGLOG("io_uring not available, skipping test");
            return;
        }
        
        class CancelCallback : public IAsyncCallback
        {
        public:
            Semaphore completed;
            int result = -999;
            
            virtual void onAsyncComplete(int _result) override
            {
                result = _result;
                completed.signal();
            }
        };
        
        try
        {
            SockAddrHolder addrHolder;
            size32_t addrlen = 0;
            
            Owned<ISocket> socket = ISocket::createForAsyncConnect(ep, addrHolder.getRef(), addrlen);
            CancelCallback callback;
            
            uring->enqueueSocketConnect(socket, addrHolder.get(), addrlen, callback);
            
            // Don't wait - immediately close the socket to simulate cancellation
            socket->close();
            
            // Wait briefly to see if callback fires
            callback.completed.wait(1000);
            
            // Socket is closed, connection should not complete successfully
        }
        catch (IException *e)
        {
            // Exception is expected when closing during async connect
            e->Release();
        }
        
        uring->terminate();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(AsyncSocketConnectionTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AsyncSocketConnectionTests, "AsyncSocketConnectionTests");

#endif // _USE_CPPUNIT
