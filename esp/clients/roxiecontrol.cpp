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

#include "roxiecontrol.hpp"
#include "jmisc.hpp"
#include "securesocket.hpp"

const unsigned roxieQueryRoxieTimeOut = 60000;

#define EMPTY_RESULT_FAILURE 1200
#define SECURE_CONNECTION_FAILURE 1201

static void checkRoxieControlExceptions(IPropertyTree *response)
{
    Owned<IMultiException> me = MakeMultiException();
    Owned<IPropertyTreeIterator> endpoints = response->getElements("Endpoint");
    ForEach(*endpoints)
    {
        IPropertyTree &endp = endpoints->query();
        Owned<IPropertyTreeIterator> exceptions = endp.getElements("Exception");
        ForEach (*exceptions)
        {
            IPropertyTree &ex = exceptions->query();
            me->append(*MakeStringException(ex.getPropInt("Code"), "Endpoint %s: %s", endp.queryProp("@ep"), ex.queryProp("Message")));
        }
    }
    if (me->ordinality())
        throw me.getClear();
}

static inline unsigned waitMsToSeconds(unsigned wait)
{
    if (wait==0 || wait==(unsigned)-1)
        return wait;
    return wait/1000;
}

IPropertyTree *sendRoxieControlQuery(ISocket *sock, const char *msg, unsigned wait)
{
    size32_t msglen = strlen(msg);
    size32_t len = msglen;
    _WINREV(len);
    sock->write(&len, sizeof(len));
    sock->write(msg, msglen);

    StringBuffer resp;
    for (;;)
    {
        sock->read(&len, sizeof(len));
        if (!len)
            break;
        _WINREV(len);
        size32_t size_read;
        sock->read(resp.reserveTruncate(len), len, len, size_read, waitMsToSeconds(wait));
        if (size_read<len)
            throw MakeStringException(-1, "Error reading roxie control message response");
    }
    if (resp.isEmpty())
        throw MakeStringException(EMPTY_RESULT_FAILURE, "Empty response string for roxie control request(%s) wait(%d)", msg, wait);
    Owned<IPropertyTree> ret = createPTreeFromXMLString(resp.str());
    if (!ret)
        throw MakeStringException(EMPTY_RESULT_FAILURE, "Empty result tree for roxie control request(%s) wait(%d)", msg, wait);

    checkRoxieControlExceptions(ret);
    return ret.getClear();
}

IPropertyTree *sendRoxieControlQuery(const SocketEndpoint &ep, const char *msg, unsigned wait)
{
    Owned<ISocket> sock = ISocket::connect_timeout(ep, wait);
    return sendRoxieControlQuery(sock, msg, wait);
}

bool sendRoxieControlLock(ISocket *sock, bool allOrNothing, unsigned wait)
{
    Owned<IPropertyTree> resp = sendRoxieControlQuery(sock, "<control:lock/>", wait);
    if (allOrNothing)
    {
        int lockCount = resp->getPropInt("Lock", 0);
        int serverCount = resp->getPropInt("NumServers", 0);
        return (lockCount && (lockCount == serverCount));
    }

    return resp->getPropInt("Lock", 0) != 0;
}

static inline unsigned remainingMsWaitX(unsigned wait, unsigned start)
{
    if (wait==0 || wait==(unsigned)-1)
        return wait;
    unsigned waited = msTick()-start;
    return (wait>waited) ? wait-waited : 0;
}

IPropertyTree *sendRoxieControlAllNodes(ISocket *sock, const char *msg, bool allOrNothing, unsigned wait)
{
    unsigned start = msTick();
    if (!sendRoxieControlLock(sock, allOrNothing, wait))
        throw MakeStringException(-1, "Roxie is too busy (control:lock failed) - please try again later.");
    return sendRoxieControlQuery(sock, msg, remainingMsWaitX(wait, start));
}

IPropertyTree *sendRoxieControlAllNodes(const SocketEndpoint &ep, const char *msg, bool allOrNothing, unsigned wait)
{
    Owned<ISocket> sock = ISocket::connect_timeout(ep, wait);
    return sendRoxieControlAllNodes(sock, msg, allOrNothing, wait);
}

static ISocket *createRoxieControlSocket(ISmartSocketFactory *conn, unsigned wait, unsigned connect_wait)
{
    const SocketEndpoint &ep = conn->nextEndpoint();
    CCycleTimer timer;
    Owned<ISocket> sock = ISocket::connect_timeout(ep, connect_wait);
    if (conn->isTlsService())
    {
#ifndef _USE_OPENSSL
        throw makeStringException(SECURE_CONNECTION_FAILURE, "failed creating secure context for roxie control message OPENSSL not supported");
#else
        Owned<ISecureSocketContext> ownedSC = createSecureSocketContextSSF(conn);
        if (!ownedSC)
            throw makeStringException(SECURE_CONNECTION_FAILURE, "failed creating secure context for roxie control message");

        Owned<ISecureSocket> ssock = ownedSC->createSecureSocket(sock.getClear());
        if (!ssock)
            throw makeStringException(SECURE_CONNECTION_FAILURE, "failed creating secure socket for roxie control message");

        unsigned remainingMs = timer.remainingMs(connect_wait);
        int status = ssock->secure_connect(remainingMs);
        if (status < 0)
        {
            StringBuffer err;
            err.append("Failure to establish secure connection to ");
            ep.getEndpointHostText(err);
            err.append(": returned ").append(status);
            throw makeStringException(SECURE_CONNECTION_FAILURE, err.str());
        }
        return ssock.getClear();
#endif
    }
    return sock.getClear();
}

IPropertyTree *sendRoxieControlQuery(ISmartSocketFactory *conn, const char *msg, unsigned wait, unsigned connect_wait)
{
    Owned<ISocket> sock = createRoxieControlSocket(conn, wait, connect_wait);
    return sendRoxieControlQuery(sock, msg, wait);
}

IPropertyTree *sendRoxieControlAllNodes(ISmartSocketFactory *conn, const char *msg, bool allOrNothing, unsigned wait, unsigned connect_wait)
{
    Owned<ISocket> sock = createRoxieControlSocket(conn, wait, connect_wait);
    return sendRoxieControlAllNodes(sock, msg, allOrNothing, wait);
}
