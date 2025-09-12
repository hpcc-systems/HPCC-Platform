/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef SOCKETUTILS_HPP__
#define SOCKETUTILS_HPP__

#ifndef SECURESOCKET_API

#ifndef SECURESOCKET_EXPORTS
    #define SECURESOCKET_API DECL_IMPORT
#else
    #define SECURESOCKET_API DECL_EXPORT
#endif //SECURESOCKET_EXPORTS

#endif

#include "jsocket.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jtime.hpp"
#include <thread>
#include <list>
#include <vector>
#include <atomic>

#include "securesocket.hpp"

//---------------------------------------------------------------------------------------------------------------------

class CReadSocketHandler;

//This is the interface called by the read socket handler when messages are read or the socket is closed
interface ISocketMessageProcessor
{
    virtual bool onlyProcessFirstRead() const = 0;                      // Does this only handle one read request, or are multiple messages processed
    virtual unsigned getMessageSize(const void * header) const = 0;     // For variable length messages, given the header, how big is the rest?
    virtual void processMessage(CReadSocketHandler & socket) = 0;
    virtual void closeConnection(CReadSocketHandler & socket, IJSOCK_Exception * exception) = 0;
};

// This class is used to process reads that are notified from a select/epoll handler
// There is a minimum and maximum message size, and the option to only process a single
// message, or continue processing multiple messages.
class SECURESOCKET_API CReadSocketHandler : public CInterfaceOf<ISocketSelectNotify>
{
public:
    CReadSocketHandler(ISocketMessageProcessor & _processor, ISocket *_sock, size32_t _minSize, size32_t _maxSize);

    ISocket *querySocket() { return sock; }
    MemoryBuffer & queryBuffer() { return buffer; }
    cycle_t queryLastActivityTime() const { return lastActivityCycles; }
    size32_t queryReadSoFar() const { return readSoFar; }
    const char *queryPeerHostText() const { return peerHostText; }
    const char *queryPeerEndpointText() const { return peerEndpointText; }

    bool close();
    bool closeIfTimedout(cycle_t now, cycle_t timeoutCycles);
    // ISocketSelectNotify impl.
    virtual bool notifySelected(ISocket *sock, unsigned selected) override;
    void prepareForNextRead();

protected:
    ISocketMessageProcessor & processor;
    Linked<ISocket> sock;
    StringBuffer peerHostText;
    StringBuffer peerEndpointText;
    MemoryBuffer buffer;
    cycle_t lastActivityCycles = 0;
    size32_t readSoFar = 0;
    size32_t minSize = 0;               // The minimum size to read before the request is valid
    size32_t maxReadSize = 0;           // The maximum that should be read when incoming size is not known
    size32_t requiredSize = 0;          // How much data should be read - set for fixed or variable size
    CriticalSection crit;
    bool closedOrHandled = false;
};

//This class uses a select handler to maintain a list of sockets that are being listened to
//It has the option for closing sockets that have been idle for too long
class SECURESOCKET_API CReadSelectHandler : public ISocketMessageProcessor
{
public:
    CReadSelectHandler(unsigned inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets);
    ~CReadSelectHandler();

    void add(ISocket *sock);

// implementation of ISocketMessageProcessor
    virtual void closeConnection(CReadSocketHandler &socketHandler, IJSOCK_Exception *exception) override;
    virtual void processMessage(CReadSocketHandler & socketHandler) override;

// Must be implemented by a derived class
    virtual CReadSocketHandler *createSocketHandler(ISocket *sock) = 0;
    virtual void processMessageContents(CReadSocketHandler * ownedSocketHandler) = 0;

protected:
    void clearupSocketHandlers();

protected:
    Owned<ISocketSelectHandler> selectHandler;

    // NB: Linked vs Owned, because methods will implicitly construct an object of this type
    // which can be problematic/confusing, for example if Owned and std::list->remove is called
    // with a pointer, it will auto instantiate a OWned<CReadSocketHandler> and cause -ve leak.
    std::list<Linked<CReadSocketHandler>> handlers;
    CriticalSection handlersCS;

    std::thread maintenanceThread;
    Semaphore maintenanceSem;
    cycle_t timeoutCycles;
    unsigned maxListenHandlerSockets;
    std::atomic<bool> aborting{false};
};

// This class starts a thread that listens on a socket.  When a connection is made it adds the socket to a select handler.
// When data is written to the socket it will call the notify handler.
class SECURESOCKET_API CSocketConnectionListener : protected CReadSelectHandler, public Thread
{
public:
    CSocketConnectionListener(unsigned port, unsigned _processPoolSize, bool _useTLS, unsigned _inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets);

    void startPort(unsigned short port);
    void stop();
    bool checkSelfDestruct(const void *p,size32_t sz);

    virtual int run() override;

private:
    Owned<ISocket> listenSocket;
    Owned<IThreadPool> threadPool;
    Owned<ISecureSocketContext> secureContextServer;
    unsigned processPoolSize;
    bool useTLS;
    std::atomic<bool> aborting{false};
};

// Demo only to check all virtuals are well defined
class ConcreteConnectionLister : public CSocketConnectionListener
{
public:
    ConcreteConnectionLister(unsigned port);

    virtual bool onlyProcessFirstRead() const override;
    virtual unsigned getMessageSize(const void * header) const override;
    virtual CReadSocketHandler *createSocketHandler(ISocket *sock) override;
    virtual void processMessageContents(CReadSocketHandler * ownedSocketHandler) override;
};


struct HashSocketEndpoint
{
    unsigned operator()(const SocketEndpoint & ep) const { return ep.hash(0x12345678); }
};


class CTcpSender;
class SECURESOCKET_API CSocketTarget : public CInterface
{
public:
    CSocketTarget(CTcpSender & _sender, const SocketEndpoint & _ep) : sender(_sender), ep(_ep) {}

    size32_t write(const void * data, size32_t len);

protected:
    CTcpSender & sender;
    const SocketEndpoint ep;
    Owned<ISocket> socket;
};

// MORE: This needs extending so that the items in the hash table know their ip address,
// and can automatically reconnect if they are disconnected.
class SECURESOCKET_API CTcpSender
{
public:
    CTcpSender(bool _lowLatency) : lowLatency(_lowLatency) {}

    CSocketTarget * queryWorkerSocket(const SocketEndpoint &ep);

public:
    const bool lowLatency;

protected:
    CriticalSection crit;
    std::unordered_map<SocketEndpoint, Owned<CSocketTarget>, HashSocketEndpoint > workerSockets;
};

#endif
