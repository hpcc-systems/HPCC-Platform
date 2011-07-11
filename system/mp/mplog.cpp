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

#define mp_decl __declspec(dllexport)

#include "mplog.hpp"
#include "mplog.ipp"
#include "mpcomm.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/system/mp/mplog.cpp $ $Id: mplog.cpp 62376 2011-02-04 21:59:58Z sort $");

LogMsgChildReceiverThread * childReceiver;
LogMsgParentReceiverThread * parentReceiver;
ILogMsgManager * listener;

// PARENT-SIDE CLASSES

// LogMsgLogReceiverThread

int LogMsgLogReceiverThread::run()
{
    while(!done)
    {
        try
        {
            if(queryWorldCommunicator().recv(in, childNode, MPTAG_JLOG_CHILD_TO_PARENT))
            {
                msgBuffer.deserialize(in, true);
                if(isListener)
                    listener->report(msgBuffer);
                else
                    queryLogMsgManager()->report(msgBuffer);
            }
        }
        catch(IException * e)
        {
            done = true;
            CMessageBuffer out;
            out.append('D').append(cid);
            try
            {
                queryWorldCommunicator().send(out, queryMyNode(), MPTAG_JLOG_CONNECT_TO_PARENT, MP_ASYNC_SEND);
            }
            catch(IException * ee)
            {
                ee->Release();
            }
            e->Release();
        }
    }
    return 0;
}

void LogMsgLogReceiverThread::stop()
{
    if(!done)
    {
        done = true;
        queryWorldCommunicator().cancel(childNode, MPTAG_JLOG_CHILD_TO_PARENT);
        join();
    }
}

// CLogMsgLinkToChild

CLogMsgLinkToChild::CLogMsgLinkToChild(MPLogId _cid, MPLogId _pid, INode * _childNode, bool isListener, bool _connected)
    : cid(_cid), pid(_pid), childNode(_childNode), connected(_connected)
{
    receiverThread.setown(new LogMsgLogReceiverThread(cid, childNode, isListener));
    receiverThread->start();
}

CLogMsgLinkToChild::~CLogMsgLinkToChild()
{
    if(connected) disconnect();
    receiverThread->stop();
}

void CLogMsgLinkToChild::sendFilter(ILogMsgFilter * filter) const
{
    CMessageBuffer out;
    filter->serialize(out, false);
    try
    {
        queryWorldCommunicator().send(out, childNode, MPTAG_JLOG_PARENT_TO_CHILD, MP_ASYNC_SEND);
    }
    catch(IException * e)
    {
        e->Release();
    }
}

void CLogMsgLinkToChild::connect()
{
    CMessageBuffer out;
    out.append('A').append(cid);
    try
    {
        queryWorldCommunicator().sendRecv(out, childNode, MPTAG_JLOG_CONNECT_TO_CHILD);
    }
    catch(IException * e)
    {
        e->Release();
    }
    out.read(pid);
    connected = true;
}

void CLogMsgLinkToChild::disconnect()
{
    CMessageBuffer out;
    out.append('D').append(pid);
    try
    {
        queryWorldCommunicator().send(out, childNode, MPTAG_JLOG_CONNECT_TO_CHILD, MP_ASYNC_SEND);
    }
    catch(IException * e)
    {
        e->Release();
    }
    connected = false;
}

// LogMsgChildReceiverThread

int LogMsgChildReceiverThread::run()
{
    INode * sender;
    char ctrl;
    while(!done)
    {
        try
        {
            if(queryWorldCommunicator().recv(in, 0, MPTAG_JLOG_CONNECT_TO_PARENT, &sender))
            {
                in.read(ctrl);
                if(ctrl=='A')
                {
                    MPLogId pid;
                    in.read(pid);
                    MPLogId cid = addChildToManager(pid, sender, false, true);
                    StringBuffer buff;
                    in.clear().append(cid);
                    queryWorldCommunicator().reply(in, MP_ASYNC_SEND);
                }
                else if(ctrl=='D')
                {
                    MPLogId cid;
                    in.read(cid);
                    removeChildFromManager(cid, true);
                }
                else
                    ERRLOG("LogMsgChildReceiverThread::run() : unknown control character on received message");
                if(sender) sender->Release();
            }
        }
        catch(IException * e)
        {
            EXCLOG(e, "LogMsgChildReceiverThread::run()");
            e->Release();
        }
        catch(...)
        {
            ERRLOG("LogMsgChildReceiverThread::run() : unknown exception");
        }
    }
    return 0;
}

void LogMsgChildReceiverThread::stop()
{
    done = true;
    queryWorldCommunicator().cancel(0, MPTAG_JLOG_CONNECT_TO_PARENT);
    join();
}

MPLogId LogMsgChildReceiverThread::addChildToManager(MPLogId pid, INode * childNode, bool isListener, bool connected)
{
    CriticalBlock critBlock(tableOfChildrenCrit);
    aindex_t pos = findChild(childNode);
    if(pos != NotFound)
    {
        if(isListener)
            table.item(pos).queryLink()->sendFilterOwn(listener->getCompoundFilter());
        else
            table.item(pos).queryLink()->sendFilterOwn(queryLogMsgManager()->getCompoundFilter());
        return false;
    }
    MPLogId cid = ++nextId;
    ILogMsgLinkToChild * link = new CLogMsgLinkToChild(cid, pid, childNode, isListener, connected);
    if(!connected) link->connect();
    if(isListener)
    {
        link->sendFilterOwn(listener->getCompoundFilter());
        listener->addChildOwn(link);
    }
    else
    {
        link->sendFilterOwn(queryLogMsgManager()->getCompoundFilter());
        queryLogMsgManager()->addChildOwn(link);
    }
    table.append(*new IdLinkToChildPair(cid, childNode, link, isListener));
    return cid;
}

bool LogMsgChildReceiverThread::removeChildFromManager(MPLogId cid, bool disconnected)
{
    CriticalBlock critBlock(tableOfChildrenCrit);
    aindex_t pos = findChild(cid);
    if(pos == NotFound) return false;
    doRemoveChildFromManager(pos, disconnected);
    return true;
}

bool LogMsgChildReceiverThread::removeChildFromManager(INode const * node, bool disconnected)
{
    StringBuffer buff;
    CriticalBlock critBlock(tableOfChildrenCrit);
    aindex_t pos = findChild(node);
    if(pos == NotFound) return false;
    doRemoveChildFromManager(pos, disconnected);
    return true;
}

void LogMsgChildReceiverThread::doRemoveChildFromManager(aindex_t pos, bool disconnected)
{
    ILogMsgLinkToChild * link = table.item(pos).queryLink();
    bool isListener = table.item(pos).isListener();
    table.remove(pos);
    if(disconnected) link->markDisconnected();
    if(isListener)
        listener->removeChild(link);
    else
        queryLogMsgManager()->removeChild(link);
}

aindex_t LogMsgChildReceiverThread::findChild(MPLogId cid) const
{
    ForEachItemIn(i, table)
        if(table.item(i).queryId() == cid) return i;
    return NotFound;
}

aindex_t LogMsgChildReceiverThread::findChild(INode const * node) const
{
    ForEachItemIn(i, table)
        if(table.item(i).queryNode()->equals(node)) return i;
    return NotFound;
}

// PARENT-SIDE HELPER FUNCTIONS

bool connectLogMsgManagerToChild(INode * childNode)
{
    assertex(childReceiver);
    return (childReceiver->addChildToManager(0, childNode, false, false) != 0);
}

bool connectLogMsgManagerToChildOwn(INode * childNode)
{
    bool ret = connectLogMsgManagerToChild(childNode);
    childNode->Release();
    return ret;
}

bool disconnectLogMsgManagerFromChild(INode * childNode)
{
    return childReceiver->removeChildFromManager(childNode, false);
}

bool disconnectLogMsgManagerFromChildOwn(INode * childNode)
{
    bool ret = disconnectLogMsgManagerFromChild(childNode);
    childNode->Release();
    return ret;
}

void startLogMsgChildReceiver()
{
    childReceiver = new LogMsgChildReceiverThread();
    childReceiver->startRelease();
}

// CHILD-SIDE CLASSES

// LogMsgFilterReceiverThread

int LogMsgFilterReceiverThread::run()
{
    assertex(handler);
    while(!done)
    {
        try
        {
            if(queryWorldCommunicator().recv(in, parentNode, MPTAG_JLOG_PARENT_TO_CHILD))
                queryLogMsgManager()->changeMonitorFilterOwn(handler, getDeserializedLogMsgFilter(in));
        }
        catch(IException * e)
        {
            if (!done) {
                done = true;
                CMessageBuffer out;
                out.append('D').append(pid);
                try
                {
                    queryWorldCommunicator().send(out, queryMyNode(), MPTAG_JLOG_CONNECT_TO_CHILD, MP_ASYNC_SEND);
                }
                catch(IException * ee)
                {
                    ee->Release();
                }
            }
            e->Release();
        }
    }
    return 0;
}

void LogMsgFilterReceiverThread::stop()
{
    if(!done)
    {
        done = true;
        queryWorldCommunicator().cancel(parentNode, MPTAG_JLOG_PARENT_TO_CHILD);
        Sleep(10); // swap 
        if (!join(1000*60*5))   // should be pretty instant
            WARNLOG("LogMsgFilterReceiverThread::stop timed out");
    }
}

// LinkToParentLogMsgHandler

void LinkToParentLogMsgHandler::handleMessage(const LogMsg & msg) const
{
    CMessageBuffer out;
    msg.serialize(out);
    try
    {
        queryWorldCommunicator().send(out, parentNode, MPTAG_JLOG_CHILD_TO_PARENT, MP_ASYNC_SEND);
    }
    catch(IException * e)
    {
        //logging this exception would be a bad idea...
        e->Release();
    }
}

void LinkToParentLogMsgHandler::addToPTree(IPropertyTree * tree) const
{
    IPropertyTree * handlerTree = createPTree(true);
    handlerTree->setProp("@type", "linktoparent");
    StringBuffer buff;
    parentNode->endpoint().getUrlStr(buff);
    handlerTree->setProp("@url", buff.str());
    tree->addPropTree("handler", handlerTree);
}

ILogMsgFilter * LinkToParentLogMsgHandler::receiveFilter() const
{
    CMessageBuffer in;
    if(queryWorldCommunicator().recv(in, parentNode, MPTAG_JLOG_PARENT_TO_CHILD))
        return getDeserializedLogMsgFilter(in);
    else
        return getPassNoneLogMsgFilter(); /* Print some kind of warning here? */
}

void LinkToParentLogMsgHandler::connect()
{
    CMessageBuffer out;
    out.append('A').append(pid);
    try
    {
        queryWorldCommunicator().sendRecv(out, parentNode, MPTAG_JLOG_CONNECT_TO_PARENT);
    }
    catch(IException * e)
    {
        e->Release();
    }
    out.read(cid);
    connected = true;
}

void LinkToParentLogMsgHandler::disconnect()
{
    CMessageBuffer out;
    out.append('D').append(cid);
    try
    {
        queryWorldCommunicator().send(out, parentNode, MPTAG_JLOG_CONNECT_TO_PARENT, MP_ASYNC_SEND);
    }
    catch(IException * e)
    {
        e->Release();
    }
    connected = false;
}

// LogMsgParentReceiverThread

int LogMsgParentReceiverThread::run()
{
    char ctrl;
    INode * sender;
    while(!done)
    {
        try
        {
            if(queryWorldCommunicator().recv(in, 0, MPTAG_JLOG_CONNECT_TO_CHILD, &sender))
            {
                in.read(ctrl);
                if(ctrl=='A')
                {
                    if (in.getReplyTag()!=TAG_NULL) { // protect against old clients crashing Dali
                        MPLogId cid;
                        in.read(cid);
                        MPLogId pid = getNextId();
                        StringBuffer buff;
                        in.clear().append(pid);
                        queryWorldCommunicator().reply(in, MP_ASYNC_SEND);
                        addParentToManager(cid, pid, sender, true);
                    }
                }
                else if(ctrl=='D')
                {
                    MPLogId pid;
                    in.read(pid);
                    removeParentFromManager(pid, true);
                }
                else
                    ERRLOG("LogMsgParentReceiverThread::run() : unknown control character on received message");
                if(sender) sender->Release();
            }
        }
        catch(IException * e)
        {
            EXCLOG(e, "LogMsgParentReceiverThread::run()");
            e->Release();
        }
        catch(...)
        {
            ERRLOG("LogMsgParentReceiverThread::run() : unknown exception");
        }
    }
    return 0;
}

void LogMsgParentReceiverThread::stop()
{
    done = true;
    queryWorldCommunicator().cancel(0, MPTAG_JLOG_CONNECT_TO_CHILD);
    join();
}

MPLogId LogMsgParentReceiverThread::getNextId()
{
    CriticalBlock critBlock(tableOfParentsCrit);
    return ++nextId;
}

bool LogMsgParentReceiverThread::addParentToManager(MPLogId cid, MPLogId pid, INode * parentNode, bool connected)
{
    CriticalBlock critBlock(tableOfParentsCrit);
    if(findParent(parentNode) != NotFound) return false;
    Owned<LinkToParentLogMsgHandler> linkHandler(new LinkToParentLogMsgHandler(cid, pid, parentNode, connected));
    if(!connected) linkHandler->connect();
    Owned<ILogMsgFilter> filter(linkHandler->receiveFilter());
    queryLogMsgManager()->addMonitor(linkHandler, filter);
    table.append(*new IdLinkToParentPair(pid, parentNode, linkHandler));
    linkHandler->startReceiver();
    return true;
}

bool LogMsgParentReceiverThread::removeParentFromManager(MPLogId pid, bool disconnected)
{
    CriticalBlock critBlock(tableOfParentsCrit);
    aindex_t pos = findParent(pid);
    if(pos == NotFound) return false;
    doRemoveParentFromManager(pos, disconnected);
    return true;
}

bool LogMsgParentReceiverThread::removeParentFromManager(const INode * parentNode, bool disconnected)
{
    CriticalBlock critBlock(tableOfParentsCrit);
    aindex_t pos = findParent(parentNode);
    if(pos == NotFound) return false;
    doRemoveParentFromManager(pos, disconnected);
    return true;
}

void LogMsgParentReceiverThread::doRemoveParentFromManager(aindex_t pos, bool disconnected)
{
    LinkToParentLogMsgHandler * linkHandler = table.item(pos).queryLinkHandler();
    table.remove(pos);
    if(disconnected) linkHandler->markDisconnected();
    queryLogMsgManager()->removeMonitor(linkHandler);
}

aindex_t LogMsgParentReceiverThread::findParent(MPLogId pid) const
{
    ForEachItemIn(i, table)
        if(table.item(i).queryId() == pid) return i;
    return NotFound;
}

aindex_t LogMsgParentReceiverThread::findParent(const INode * node) const
{
    ForEachItemIn(i, table)
        if(table.item(i).queryNode()->equals(node)) return i;
    return NotFound;
}

// CHILD-SIDE HELPER FUNCTIONS

bool connectLogMsgManagerToParent(INode * parentNode)
{
    assertex(parentReceiver);
    MPLogId pid = parentReceiver->getNextId();
    return parentReceiver->addParentToManager(0, pid, parentNode, false);
}

bool connectLogMsgManagerToParentOwn(INode * parentNode)
{
    bool ret = connectLogMsgManagerToParent(parentNode);
    parentNode->Release();
    return ret;
}

bool disconnectLogMsgManagerFromParent(INode * parentNode)
{
    return parentReceiver->removeParentFromManager(parentNode, false);
}

bool disconnectLogMsgManagerFromParentOwn(INode * parentNode)
{
    bool ret = disconnectLogMsgManagerFromParent(parentNode);
    parentNode->Release();
    return ret;
}

void startLogMsgParentReceiver()
{
    parentReceiver = new LogMsgParentReceiverThread();
    parentReceiver->startRelease();
}

// MISC. HELPER FUNCTION

bool isMPLogMsgMonitor(ILogMsgHandler * handler)
{
    return (dynamic_cast<LinkToParentLogMsgHandler *>(handler) != NULL);
}

void stopLogMsgReceivers()
{
    if(parentReceiver)
    {
        parentReceiver->Link();
        parentReceiver->stop();
        parentReceiver->Release();
    }
    parentReceiver = 0;
    if(childReceiver)
    {
        childReceiver->Link();
        childReceiver->stop();
        childReceiver->Release();
    }
    childReceiver = 0;
    queryLogMsgManager()->removeMonitorsMatching(isMPLogMsgMonitor);
    queryLogMsgManager()->removeAllChildren();
}

// LISTENER HELPER FUNCTIONS

ILogMsgListener * startLogMsgListener()
{
    if(!listener)
        listener = createLogMsgManager();
    return listener;
}
    
void stopLogMsgListener()
{
    if(listener)
    {
        listener->Release();
        listener = 0;
    }
}

bool connectLogMsgListenerToChild(INode * childNode)
{
    assertex(childReceiver);
    return (childReceiver->addChildToManager(0, childNode, true, false) != 0);
}

bool connectLogMsgListenerToChildOwn(INode * childNode)
{
    bool ret = connectLogMsgListenerToChild(childNode);
    childNode->Release();
    return ret;
}

bool disconnectLogMsgListenerFromChild(INode * childNode)
{
    return childReceiver->removeChildFromManager(childNode, false);
}

bool disconnectLogMsgListenerFromChildOwn(INode * childNode)
{
    bool ret = disconnectLogMsgListenerFromChild(childNode);
    childNode->Release();
    return ret;
}
