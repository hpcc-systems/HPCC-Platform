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

#include <string>
#include <unordered_set>
#include <functional>
#include <algorithm>
#include <thread>
#include <limits.h>

#include "platform.h"
#include "socketutils.hpp"

#include "jmutex.hpp"
#include "jsocket.hpp"
#include "jexcept.hpp"
#include "jio.hpp"
#include "jmisc.hpp"
#include "jthread.hpp"
#include "jqueue.tpp"
#include "jtime.hpp"
#include "jprop.hpp"
#include "jregexp.hpp"
#include "jdebug.hpp"
#include "jiouring.hpp"

//---------------------------------------------------------------------------------------------------------------------

CReadSocketHandler::CReadSocketHandler(ISocketMessageProcessor & _processor, ISocket *_sock, size32_t _minSize, size32_t _maxSize)
 : processor(_processor), sock(_sock), minSize(_minSize), maxReadSize(_maxSize)
{
    lastActivityCycles = get_cycles_now();
    SocketEndpoint peerEP;
    sock->getPeerEndpoint(peerEP);
    peerEP.getHostText(peerHostText); // always used by handleAcceptedSocket
    peerEndpointText.append(peerHostText); // only used if tracing an error
    if (peerEP.port)
        peerEndpointText.append(':').append(peerEP.port);
    buffer.ensureCapacity(maxReadSize);
}


bool CReadSocketHandler::close()
{
    // will block any pending notifySelected on this socket
    CriticalBlock b(crit);
    if (!closedOrHandled)
    {
        closedOrHandled = true;
        return true;
    }
    return false;
}

bool CReadSocketHandler::closeIfTimedout(cycle_t now, cycle_t timeoutCycles)
{
    if ((now - lastActivityCycles) >= timeoutCycles)
        return close();

    return false;
}

// ISocketSelectNotify impl.
bool CReadSocketHandler::notifySelected(ISocket *sock, unsigned selected)
{
    Owned<IJSOCK_Exception> exception;
    {
        CLeavableCriticalBlock b(crit);
        if (closedOrHandled)
            return false;

        lastActivityCycles = get_cycles_now();
        try
        {
            // Only read as much data as we know we need - because sends may have been combined into a single packet.
            // If the handler is not one-shot, and it can support multiple messages, then it should be possible
            // to read maxSize, and then process a sequence of messages instead.  That would be more efficient.
            size32_t toRead = requiredSize ? requiredSize : minSize;
            buffer.ensureCapacity(toRead);

            size32_t rd = 0;
            byte * target = (byte *)buffer.bufferBase();
            sock->readtms(target+readSoFar, 0, toRead-readSoFar, rd, 60000); // long enough!
            buffer.reserve(rd); // increment the current length
            readSoFar += rd;
            dbgassertex(target == buffer.bufferBase()); // Check that the reserve has not reallocated the buffer

            if (!requiredSize && (readSoFar >= minSize))
                requiredSize = processor.getMessageSize(target);

            if (requiredSize && (readSoFar >= requiredSize))
            {
                assertex(readSoFar == requiredSize);
                bool oneShort = processor.onlyProcessFirstRead();
                if (oneShort)
                {
                    // process() will remove itself from handler, and need to avoid it doing so while in 'crit'
                    // since the maintenance thread could also be tyring to manipulate handlers and calling closeIfTimedout()
                    closedOrHandled = true;
                    b.leave();
                }
                processor.processMessage(*this);

                if (!oneShort)
                    prepareForNextRead();
            }
        }
        catch (IJSOCK_Exception *e)
        {
            exception.setown(e);
        }
    }

    //Change from mpcomm - must be outside the critical block, otherwise, crit could be freed while still in use
    if (exception)
        processor.closeConnection(*this, exception);

    return false;
}

void CReadSocketHandler::prepareForNextRead()
{
    readSoFar = 0;
    requiredSize = (minSize == maxReadSize) ? minSize : 0;
    buffer.clear();
}


//---------------------------------------------------------------------------------------------------------------------

//This class uses a select handler to maintain a list of sockets that are being listened to
//It has the option for closing sockets that have been idle for too long
CReadSelectHandler::CReadSelectHandler(unsigned inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets)
: maxListenHandlerSockets(_maxListenHandlerSockets ? _maxListenHandlerSockets : ~0U)
{
    constexpr unsigned socketsPerThread = 50;
    selectHandler.setown(createSocketEpollHandler("CSocketConnectionListener", socketsPerThread));
    //selectHandler.setown(createSocketSelectHandler());
    selectHandler->start();

    timeoutCycles = millisec_to_cycle(inactiveCloseTimeoutMs);
    if (timeoutCycles)
    {
        auto maintenanceFunc = [&]
        {
            while (!aborting)
            {
                if (maintenanceSem.wait(10000)) // check every 10s
                    break;
                clearupSocketHandlers();
            }
        };
        maintenanceThread = std::thread(maintenanceFunc);
    }
}

CReadSelectHandler::~CReadSelectHandler()
{
    if (timeoutCycles)
    {
        aborting = true;
        maintenanceSem.signal();
        maintenanceThread.join();
    }
}

void CReadSelectHandler::add(ISocket *sock)
{
    while (true)
    {
        unsigned numHandlers;
        {
            CriticalBlock b(handlersCS);
            numHandlers = handlers.size();
        }
        if (numHandlers < maxListenHandlerSockets)
            break;
        DBGLOG("Too many handlers (%u), waiting for some to be processed (max limit: %u)", numHandlers, maxListenHandlerSockets);
        MilliSleep(1000);
    }

    Owned<CReadSocketHandler> socketHandler = createSocketHandler(sock);

    size_t numHandlers;
    {
        CriticalBlock b(handlersCS);
        constexpr unsigned mode = SELECTMODE_READ;
        selectHandler->add(sock, mode, socketHandler); // NB: sock and handler linked by select handler
        handlers.emplace_back(socketHandler);
        numHandlers = handlers.size();
    }
    if (0 == (numHandlers % 100)) // for info. log at each 100 boundary
        DBGLOG("handlers = %u", (unsigned)numHandlers);
}

void CReadSelectHandler::closeConnection(CReadSocketHandler &socketHandler, IJSOCK_Exception *exception)
{
    unsigned sofar = socketHandler.queryReadSoFar();
    if (sofar) // read something
    {
        VStringBuffer errMsg("Invalid number of connection bytes (%u) serialized from: %s", sofar, socketHandler.queryPeerEndpointText());
        FLLOG(MCoperatorWarning, "%s", errMsg.str());
    }

    Linked<CReadSocketHandler> handler = &socketHandler;
    {
        CriticalBlock b(handlersCS);
        selectHandler->remove(socketHandler.querySocket());
        handlers.remove(&socketHandler);
    }
    handler->querySocket()->close();
}

void CReadSelectHandler::processMessage(CReadSocketHandler & socketHandler)
{
    Linked<CReadSocketHandler> handler = &socketHandler;
    if (onlyProcessFirstRead())
    {
        CriticalBlock b(handlersCS);
        selectHandler->remove(socketHandler.querySocket());
        handlers.remove(&socketHandler);
    }

    processMessageContents(handler.getClear());
}

void CReadSelectHandler::clearupSocketHandlers()
{
    assertex(timeoutCycles);

    std::vector<Owned<CReadSocketHandler>> toClose;
    {
        cycle_t nowCycles = get_cycles_now();
        CriticalBlock b(handlersCS);
        auto it = handlers.begin();
        while (true)
        {
            if (it == handlers.end())
                break;
            CReadSocketHandler *socketHandler = *it;
            if (socketHandler->closeIfTimedout(nowCycles, timeoutCycles))
            {
                toClose.push_back(LINK(socketHandler));
                it = handlers.erase(it);
            }
            else
                ++it;
        }
    }
    for (auto &socketHandler: toClose)
    {
        try
        {
            Owned<IJSOCK_Exception> e = createJSocketException(JSOCKERR_timeout_expired, "Connect timeout expired", __FILE__, __LINE__);
            closeConnection(*socketHandler, e);
        }
        catch (IException *e)
        {
            EXCLOG(e, "CReadSelectHandler::maintenanceFunc");
            e->Release();
        }
    }
}

// This class starts a thread that listens on a socket.  When a connection is made it adds the socket to a select handler.
// When data is written to the socket it will call the notify handler.

//---------------------------------------------------------------------------------------------------------------------


CSocketConnectionListener::CSocketConnectionListener(unsigned port, unsigned _processPoolSize, bool _useTLS, unsigned _inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets)
    : CReadSelectHandler(_inactiveCloseTimeoutMs, _maxListenHandlerSockets), Thread("CSocketConnectionListener"), processPoolSize(_processPoolSize), useTLS(_useTLS)
{
    if (port)
        startPort(port);

    if (useTLS)
        secureContextServer.setown(createSecureSocketContextSecretSrv("local", nullptr, true));

    PROGLOG("CSocketConnectionListener TLS: %s acceptThreadPoolSize: %u", useTLS ? "on" : "off", processPoolSize);
}

bool CSocketConnectionListener::checkSelfDestruct(const void *p,size32_t sz)
{
    const byte *b = (const byte *)p;
    while (sz--)
        if (*(b++)!=0xff)
            return false;

    try {
        if (listenSocket) {
            shutdownAndCloseNoThrow(listenSocket);
            listenSocket.clear();
        }
    }
    catch (...)
    {
        PROGLOG("CSocketConnectionListener::selfDestruct socket close failure");
    }
    return true;
}

void CSocketConnectionListener::startPort(unsigned short port)
{
    if (!listenSocket)
    {
        unsigned listenQueueSize = 600; // default
        listenSocket.setown(ISocket::create(port, listenQueueSize));
    }

    if (processPoolSize)
    {
        class CSocketConnectionListenerFactory : public CInterfaceOf<IThreadFactory>
        {
            CSocketConnectionListener &owner;
        public:
            CSocketConnectionListenerFactory(CSocketConnectionListener &_owner) : owner(_owner)
            {
            }
        // IThreadFactory
            IPooledThread *createNew() override
            {
                class CMPConnectionThread : public CInterfaceOf<IPooledThread>
                {
                    CSocketConnectionListener &owner;
                    Owned<CReadSocketHandler> handler;
                public:
                    CMPConnectionThread(CSocketConnectionListener &_owner) : owner(_owner)
                    {
                    }
                // IPooledThread
                    virtual void init(void *param) override
                    {
                        handler.setown((CReadSocketHandler *)param);
                    }
                    virtual void threadmain() override
                    {
                        owner.processMessageContents(handler.getClear());
                    }
                    virtual bool stop() override
                    {
                        return true;
                    }
                    virtual bool canReuse() const override
                    {
                        return true;
                    }
                };
                return new CMPConnectionThread(owner);
            }
        };
        Owned<IThreadFactory> factory = new CSocketConnectionListenerFactory(*this);
        threadPool.setown(createThreadPool("MPConnectPool", factory, false, nullptr, processPoolSize, INFINITE));
    }
    Thread::start(false);
}

int CSocketConnectionListener::run()
{
#ifdef _TRACE
    LOG(MCdebugInfo, "MP: Connect Thread Starting - accept loop");
#endif
    Owned<IException> exception;

    while (!aborting)
    {
        Owned<ISocket> sock;
        try
        {
            sock.setown(listenSocket->accept(true));
        }
        catch (IException *e)
        {
            exception.setown(e);
        }
        if (sock)
        {
#if defined(_USE_OPENSSL)
            if (useTLS)
            {
                Owned<ISecureSocket> ssock = secureContextServer->createSecureSocket(sock.getClear());
                int tlsTraceLevel = SSLogMin;
//                if (parent->mpTraceLevel >= MPVerboseMsgThreshold)
//                    tlsTraceLevel = SSLogMax;
                int status = ssock->secure_accept(tlsTraceLevel);
                if (status < 0)
                {
                    ssock->close();
                    PROGLOG("MP Connect Thread: failed to accept secure connection");
                    continue;
                }
                sock.setown(ssock.getClear());
            }
#endif // OPENSSL

#ifdef _FULLTRACE
            StringBuffer s;
            SocketEndpoint ep1;
            sock->getPeerEndpoint(ep1);
            PROGLOG("MP: Connect Thread: socket accepted from %s",ep1.getEndpointHostText(s).str());
#endif
            sock->set_keep_alive(true);

            // NB: creates a CSocketHandler that is added to the select handler.
            // it will manage the handling of the incoming ConnectHdr header only.
            // After that, the socket will be removed from the connectSelectHamndler,
            // a CMPChannel will be estalbished, and the socket will be added to the MP CMPPacketReader select handler.
            // See handleAcceptedSocket.
            CReadSelectHandler::add(sock);
        }
        else
        {
            if (!aborting)
            {
                if (exception)
                {
                    constexpr unsigned sleepSecs = 5;
                    // Log and pause for a few seconds, because accept loop may have failed due to handle exhaustion.
                    VStringBuffer msg("MP accept failed. Accept loop will be paused for %u seconds", sleepSecs);
                    EXCLOG(exception, msg.str());
                    exception.clear();
                    MilliSleep(sleepSecs * 1000);
                }
                else // not sure this can ever happen (no exception, still running, and sock==nullptr)
                    LOG(MCdebugInfo, "MP Connect Thread accept returned NULL");
            }
        }
    }
    return 0;
}


void CSocketConnectionListener::stop()
{
    if (!aborting)
    {
        aborting = true;
        listenSocket->cancel_accept();

        // ensure CSocketConnectionListener::run() has exited, and is not accepting more sockets
        if (!join(1000*60*5))   // should be pretty instant
            printf("CSocketConnectionListener::stop timed out\n");

        if (processPoolSize)
        {
            if (!threadPool->joinAll(true, 1000*60*5))
                printf("CSocketConnectionListener::stop threadPool->joinAll timed out\n");
        }
    }
}

// Demo only to check all virtuals are well defined
ConcreteConnectionLister::ConcreteConnectionLister(unsigned port) : CSocketConnectionListener(port, 0, false, 0, 0)
{

}

bool ConcreteConnectionLister::onlyProcessFirstRead() const
{
    return false;
}

unsigned ConcreteConnectionLister::getMessageSize(const void * header) const
{
    return *(const unsigned *)header;
}

CReadSocketHandler *ConcreteConnectionLister::createSocketHandler(ISocket *sock)
{
    //Header size is 64B, max variable to read is 64K
    return new CReadSocketHandler(*this, sock, 64, 0x10000);
}

void ConcreteConnectionLister::processMessageContents(CReadSocketHandler * ownedSocketHandler)
{
    ownedSocketHandler->Release();
}

//---------------------------------------------------------------------------------------------------------------------

CSocketTarget::CSocketTarget(CTcpSender & _sender, const SocketEndpoint & _ep) : sender(_sender), ep(_ep)
{
}

void CSocketTarget::connectAsync(IAsyncProcessor * processor)
{
    CriticalBlock b(crit);

    //Currently synchronous - make it asynchronous in the next version
    socket.clear();
    (void)querySocket();
}

ISocket * CSocketTarget::getSocket()
{
    CriticalBlock b(crit);
    return LINK(querySocket());
}

ISocket * CSocketTarget::querySocket()
{
    if (!socket)
    {
        socket.setown(ISocket::connect_timeout(ep, 5000));
        if (socket && sender.lowLatency)
            socket->set_nagle(false);
    }
    return socket;
}

size32_t CSocketTarget::write(const void * data, size32_t len)
{
    //MORE: Add retry logic.
    //MORE: How do we prevent this blocking other threads though e.g. when sending a request to all nodes in a
    //      channel and one channel is down -.do we use non-blocking and note when something is not sent?
    Owned<ISocket> target = getSocket();
    if (!target)
        return 0;

    try
    {
        return target->write(data, len);
    }
    catch (IException * e)
    {
        //Force a reconnect next time - could add retry loop and logic...
        CriticalBlock b(crit);
        socket.clear();
        throw;
    }
}

void CSocketTarget::writeAsync(IAsyncProcessor * processor, size32_t len, const void * data, IAsyncCallback & callback)
{
    Owned<ISocket> target = getSocket();
    if (target)
        processor->enqueueSocketWrite(target, len, data, callback);
}

CSocketTarget * CTcpSender::queryWorkerSocket(const SocketEndpoint &ep)
{
    CriticalBlock b(crit);
    auto match = workerSockets.find(ep);
    if (match != workerSockets.end())
        return match->second.get();

    Owned<CSocketTarget> workerSocket = new CSocketTarget(*this, ep);
    workerSockets.emplace(ep, workerSocket);
    return workerSocket;
}
