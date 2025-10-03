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

//#define TRACE_MAX_MESSAGES

//---------------------------------------------------------------------------------------------------------------------

CReadSocketHandler::CReadSocketHandler(ISocketMessageProcessor & _processor, IAsyncProcessor * _asyncReader, ISocket *_socket, size32_t _minSize, size32_t _maxSize)
 :  onlyProcessFirstRead(_processor.onlyProcessFirstRead()), processMultipleMessages(!_processor.onlyProcessFirstRead()),
    processor(_processor), asyncReader(_asyncReader), socket(_socket), minSize(_minSize), maxReadSize(_maxSize)
{
    assertex(!(onlyProcessFirstRead && processMultipleMessages)); // If only processing the first read then we can't process multiple messages
    assertex(!(asyncReader && !processMultipleMessages));           // Async code must process multiple messages - because reading size is not limited
    lastActivityCycles = get_cycles_now();
    SocketEndpoint peerEP;
    socket->getPeerEndpoint(peerEP);
    peerEP.getHostText(peerHostText); // always used by handleAcceptedSocket
    peerEndpointText.append(peerHostText); // only used if tracing an error
    if (peerEP.port)
        peerEndpointText.append(':').append(peerEP.port);
    buffer.ensure(maxReadSize);
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

#ifdef TRACE_MAX_MESSAGES
static unsigned maxMessages{1};
#endif

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

            // If we can process multiple messages, then read as much as we can, up to maxReadSize
            if (processMultipleMessages && (toRead < maxReadSize))
                toRead = maxReadSize;

            // Buffer is only used as a block of memory - could switch to a MemoryAttr and use ensure
            byte * target = (byte *)buffer.ensure(toRead);
            size32_t maxToRead = toRead-readSoFar;
            size32_t rd = 0;
            sock->readtms(target+readSoFar, 0, maxToRead, rd, 60000); // long enough!
            readSoFar += rd;

            if (!requiredSize && (readSoFar >= minSize))
                requiredSize = processor.getMessageSize(target);

            if (requiredSize && (readSoFar >= requiredSize))
            {
                // Ensure that this socket handler does not get destroyed when processing the message as a
                // side-effect of removing it from the processor
                Linked<CReadSocketHandler> savedHandler = this;

                assertex(processMultipleMessages || readSoFar == requiredSize);
                if (onlyProcessFirstRead)
                {

                    // process() will remove itself from handler, and need to avoid it doing so while in 'crit'
                    // since the maintenance thread could also be trying to manipulate handlers and calling closeIfTimedout()
                    closedOrHandled = true;
                    b.leave();

                    processor.stopProcessing(*this);
                    processor.processMessage(target, readSoFar);
                }
                else
                {
                    processPendingMessages();
                }
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

void CReadSocketHandler::processPendingMessages()
{
    byte * target = buffer.bytes();
    //Walk the data that has been received, processing all the complete messages
    size32_t offset = 0;
    [[maybe_unused]] unsigned numMessages = 0;
    while (offset <= readSoFar - minSize)
    {
        size32_t msgSize = processor.getMessageSize(target + offset);
        if (msgSize > readSoFar - offset)
            break;
        processor.processMessage(target + offset, msgSize);
        offset += msgSize;
        numMessages++;
    }

#ifdef TRACE_MAX_MESSAGES
    if (numMessages > maxMessages)
    {
        maxMessages = numMessages;
        DBGLOG("Max messages in one read: %u", maxMessages);
    }
#endif

    //Now prepare for the next request - copy any remaining data to the start of the buffer
    size32_t remaining = readSoFar - offset;
    if (remaining)
        memmove(target, target + offset, remaining);

    // And reset the state about the number of bytes read so far
    readSoFar = remaining;
    requiredSize = (minSize == maxReadSize) ? minSize : 0;
    if (readSoFar >= minSize)
        requiredSize = processor.getMessageSize(target);
}

void CReadSocketHandler::startAsyncRead()
{
    // Only read as much data as we know we need - because sends may have been combined into a single packet.
    // If the handler is not one-shot, and it can support multiple messages, then it should be possible
    // to read maxSize, and then process a sequence of messages instead.  That would be more efficient.
    size32_t toRead = requiredSize ? requiredSize : minSize;

    // If we can process multiple messages, then read as much as we can, up to maxReadSize
    if (toRead < maxReadSize)
        toRead = maxReadSize;

    buffer.ensure(toRead);
    byte * target = buffer.bytes();
    size32_t maxToRead = toRead-readSoFar;
    asyncReader->enqueueSocketRead(socket, target+readSoFar, maxToRead, *this);
}

void CReadSocketHandler::onAsyncComplete(int result)
{
    //This is called on the completion of an async read
    if (result < 0)
    {
        Owned<IJSOCK_Exception> exception = createJSocketException(result, "Read error", __FILE__, __LINE__);
        processor.closeConnection(*this, exception);
        return;
    }

    if (result == 0)
    {
        Owned<IJSOCK_Exception> exception = createJSocketException(JSOCKERR_graceful_close, "Connection closed", __FILE__, __LINE__);
        processor.closeConnection(*this, exception);
        return;
    }

    readSoFar += result;

    if (!requiredSize && (readSoFar >= minSize))
        requiredSize = processor.getMessageSize(buffer.bytes());

    if (requiredSize && (readSoFar >= requiredSize))
    {
        processPendingMessages();
    }

    startAsyncRead();
}

//---------------------------------------------------------------------------------------------------------------------

//This class uses a select handler to maintain a list of sockets that are being listened to
//It has the option for closing sockets that have been idle for too long
CReadSelectHandler::CReadSelectHandler(unsigned inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets, bool useIOUring)
: maxListenHandlerSockets(_maxListenHandlerSockets ? _maxListenHandlerSockets : ~0U)
{
    if (useIOUring)
    {
        Owned<IPropertyTree> config = createPTreeFromXMLString("<iouring poll='1'/>");
        // MORE: Update queue depth to allow the max number of connections
        constexpr bool useThreadForCompletion = true;
        asyncReader.setown(createURingProcessor(config, useThreadForCompletion));
    }

    if (!asyncReader)
    {
        constexpr unsigned socketsPerThread = 50;
        selectHandler.setown(createSocketEpollHandler("CSocketConnectionListener", socketsPerThread));
        //selectHandler.setown(createSocketSelectHandler());
        selectHandler->start();
    }

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
        if (selectHandler)
            selectHandler->add(sock, mode, socketHandler); // NB: sock and handler linked by select handler
        handlers.emplace_back(socketHandler);
        numHandlers = handlers.size();
    }
    if (asyncReader)
        socketHandler->startAsyncRead();

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

void CReadSelectHandler::stopProcessing(CReadSocketHandler & socket)
{
    CriticalBlock b(handlersCS);
    selectHandler->remove(socket.querySocket());
    handlers.remove(&socket);
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


static constexpr bool useIOUring = true;
CSocketConnectionListener::CSocketConnectionListener(unsigned port, bool _useTLS, unsigned _inactiveCloseTimeoutMs, unsigned _maxListenHandlerSockets)
    : CReadSelectHandler(_inactiveCloseTimeoutMs, _maxListenHandlerSockets, useIOUring), Thread("CSocketConnectionListener"), useTLS(_useTLS)
{
    if (port)
        startPort(port);

    if (useTLS)
        secureContextServer.setown(createSecureSocketContextSecretSrv("local", nullptr, true));

    PROGLOG("CSocketConnectionListener TLS: %s", useTLS ? "on" : "off");
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
    }
}

//---------------------------------------------------------------------------------------------------------------------

CSocketTarget::CSocketTarget(CTcpSender & _sender, const SocketEndpoint & _ep) : sender(_sender), ep(_ep)
{
}

void CSocketTarget::connect()
{
    // Must be called within a critical section....
    try
    {
        socket.setown(ISocket::connect_timeout(ep, 5000));
        if (socket)
        {
            // Keep track of the number of connections - a useful stat, and to distinguish between initial connection and reconnection.
            numConnects++;
            if (sender.lowLatency)
                socket->set_nagle(false);
        }
    }
    catch (IException * e)
    {
        socket.clear();
        e->Release();
    }
}

unsigned CSocketTarget::dropPendingRequests()
{
    for (unsigned i = 0; i < numRequests; i++)
    {
        unsigned index = wrapIndex(headRequestIndex + i);
        void * buffer = buffers[index];
        sender.releaseBuffer(buffer);
    }
    numRequests = 0;
    unsigned numToSignal = threadsWaiting;
    threadsWaiting = 0;
    return numToSignal;
}

ISocket * CSocketTarget::getSocket()
{
    CriticalBlock b(crit);
    return LINK(querySocket());
}

ISocket * CSocketTarget::querySocket()
{
    if (!socket)
        connect();
    return socket;
}

size32_t CSocketTarget::writeSync(const void * data, size32_t len)
{
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

void CSocketTarget::waitForRequestSpace(CLeavableCriticalBlock & block)
{
    threadsWaiting++;
    block.leave();
    waitSem.wait();
    block.enter();
}

void CSocketTarget::writeAsync(const void * data, size32_t len, void * ownedBuffer)
{
    if (!sender.asyncSender)
    {
        writeSync(data, len);
        sender.releaseBuffer(ownedBuffer);
        return;
    }

    CLeavableCriticalBlock block(crit);
    for(;;)
    {
        //If reconnecting, drop the packets until the connection has succeeded.
        if (unlikely((state == State::Aborting) || (state == State::Reconnecting)))
        {
            sender.releaseBuffer(ownedBuffer);
            return;
        }

        //Ensure there is space to record the next item to send..
        //Maybe this should always expand rather than block???
        if (likely(numRequests != maxQueueDepth))
            break;

        waitForRequestSpace(block);
    }

    //First of all, record the new request in the list of pending items
    unsigned nextRequestIndex = wrapIndex(headRequestIndex + numRequests);

    requests[nextRequestIndex].len = len;
    requests[nextRequestIndex].data = data;
    buffers[nextRequestIndex] = ownedBuffer;
    numRequests++;

    //Check for completions - because it may mean this request can be processed immediately
    sender.asyncSender->checkForCompletions();

    //Depending on the current state, we may need to start a new async operation
    switch (state)
    {
    case State::Unconnected:
        startAsyncConnect();
        break;
    case State::Connected:
        startAsyncWrite();
        break;
    default:
        //Need to wait for the async operation to complete.
        break;
    }
}

void CSocketTarget::startAsyncWrite()
{
    state = State::Writing;
    WriteRequest & request = requests[headRequestIndex];
    sender.asyncSender->enqueueSocketWrite(socket, request.data, request.len, *this);
}

void CSocketTarget::startAsyncConnect()
{
    state = numConnects ? State::Reconnecting : State::Connecting;

    //MORE: Need to code a clean async connect - that would involve
    // * adding an ISocket call to create an unconnected socket
    // * add an async connect call to the ISocket interface
    // * add a callback to the ISocket interface to process the post connect and then call back to this classes' callback
    // for the moment connect synchronously, and revisit in a later PR
    connect();

    //Process as if an async connect had completed
    int result = socket ? 0 : -1;
    onAsyncComplete(result);
}

void CSocketTarget::onAsyncComplete(int result)
{
    unsigned newSpaceToSignal = 0;

    {
        CriticalBlock block(crit); // It is possible that a caller has already locked this critical section
        switch (state)
        {
        case State::Connecting:
        case State::Reconnecting:
            if (result < 0)
            {
                // If connecting for the first time failed there may be requests queued.  At the moment they are not dropped
                // but uncomment the following line if that is the desired behaviour
                // If reconnecting all items will be dropped already.
                // waitSem.signal(dropPendingRequests());

                // What should this do?  Schedule a delay and then try and reconnect?
                // For the moment set the state so that it will try to connect on the next request
                state = State::Unconnected;
            }
            else
            {
                if (numRequests != 0)
                    startAsyncWrite();
                else
                    state = State::Connected;
            }
            break;
        case State::ConnectWaiting:
            startAsyncConnect();
            break;
        case State::Writing:
            if (result >= 0)
            {
                WriteRequest & request = requests[headRequestIndex];
                // How much of the data has been consumed?
                // When writev is supported this may consume multiple buffers...
                if (request.len == result)
                {
                    //All of the data is consumed
                    sender.releaseBuffer(buffers[headRequestIndex]);
                    headRequestIndex = wrapIndex(headRequestIndex + 1);
                    if (threadsWaiting)
                    {
                        newSpaceToSignal = 1;
                        threadsWaiting -= newSpaceToSignal;
                    }
                    numRequests--;
                    if (numRequests != 0)
                        startAsyncWrite();
                    else
                        state = State::Connected;
                }
                else
                {
                    OERRLOG("Short write on socket %u of %zu", result, request.len);
                    //A partial write - need to adjust the size of the request and resubmit
                    request.len -= result;
                    request.data = (const byte *)request.data + result;
                    startAsyncWrite();
                }
            }
            else
            {
                int errcode = -result;
                if (errcode == EAGAIN)
                {
                    startAsyncWrite();
                }
                else
                {
                    //connection lost.  Throw away all pending writes and start connecting again
                    newSpaceToSignal = dropPendingRequests();
                    startAsyncConnect();
                }
            }
            break;
        }
    }
    if (newSpaceToSignal)
        waitSem.signal(newSpaceToSignal);
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

void CTcpSender::releaseBuffer(void * buffer)
{
    throwUnimplementedX("CTcpSender::releaseBuffer must be implemented by derived class to support async writes");
}
