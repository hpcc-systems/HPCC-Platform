/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#include "roxiecontrol.hpp"
#include "jmisc.hpp"

const unsigned roxieQueryRoxieTimeOut = 60000;

void checkRoxieControlExceptions(IPropertyTree *msg)
{
    Owned<IMultiException> me = MakeMultiException();
    Owned<IPropertyTreeIterator> endpoints = msg->getElements("Endpoint");
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
    loop
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

    Owned<IPropertyTree> ret = createPTreeFromXMLString(resp.str());
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
        throw MakeStringException(-1, "Roxie control:lock failed");
    return sendRoxieControlQuery(sock, msg, remainingMsWait(wait, start));
}

IPropertyTree *sendRoxieControlAllNodes(const SocketEndpoint &ep, const char *msg, bool allOrNothing, unsigned wait)
{
    Owned<ISocket> sock = ISocket::connect_timeout(ep, wait);
    return sendRoxieControlAllNodes(sock, msg, allOrNothing, wait);
}
