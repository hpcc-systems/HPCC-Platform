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

const unsigned roxieQueryRoxieTimeOut = 60000;

#define EMPTY_RESULT_FAILURE 1200

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

static inline unsigned remainingMsWait(unsigned wait, unsigned start)
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
    return sendRoxieControlQuery(sock, msg, remainingMsWait(wait, start));
}

IPropertyTree *sendRoxieControlAllNodes(const SocketEndpoint &ep, const char *msg, bool allOrNothing, unsigned wait)
{
    Owned<ISocket> sock = ISocket::connect_timeout(ep, wait);
    return sendRoxieControlAllNodes(sock, msg, allOrNothing, wait);
}
