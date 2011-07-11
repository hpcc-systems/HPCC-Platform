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

#ifndef MPLOG_IPP
#define MPLOG_IPP

#include "mpbuff.hpp"
#include "mpbase.hpp"
#include "mplog.hpp"

typedef unsigned __int64 MPLogId;

// Base class for the receiver threads used below

class LogMsgReceiverThread : extends Thread
{
public:
    LogMsgReceiverThread(char const * name) : Thread(name), done(false) {}
protected:
    CMessageBuffer            in;
    bool                      done;
};

// PARENT-SIDE CLASSES

// Thread in CLogMsgLinkToChild which receives logs from child

class LogMsgLogReceiverThread : public LogMsgReceiverThread
{
public:
    LogMsgLogReceiverThread(MPLogId _cid, INode * _child, bool _isListener) : LogMsgReceiverThread("LogMsgLogReceiver"), cid(_cid), childNode(_child), isListener(_isListener) {}
    int                       run();
    void                      stop();
private:
    LogMsg                    msgBuffer;
    INode *                   childNode;
    MPLogId                   cid;
    bool                      isListener;
};

// Class on managers's list of children which sends new filters to children, and holds thread which receives log messages

class CLogMsgLinkToChild : public CInterface, implements ILogMsgLinkToChild
{
public:
    CLogMsgLinkToChild(MPLogId _cid, MPLogId _pid, INode * _childNode, bool isListener, bool _connected = false);
    ~CLogMsgLinkToChild();
    IMPLEMENT_IINTERFACE;
    void                      sendFilter(ILogMsgFilter * filter) const;
    void                      sendFilterOwn(ILogMsgFilter * filter) const { sendFilter(filter); filter->Release(); }
    bool                      linksTo(SocketEndpoint const & node) const { return childNode->endpoint().equals(node); }
    void                      connect();
    void                      disconnect();
    bool                      queryConnected() const { return connected; }
    void                      markDisconnected() { connected = false; }
private:
    Linked<INode>             childNode;
    MPLogId                   cid;
    MPLogId                   pid;
    Owned<LogMsgLogReceiverThread> receiverThread;
    bool                      connected;
};

// Pairing of MPLogId and ILogMsgLinkToChild * for LogMsgChildReceiverThread's table

class IdLinkToChildPair : public CInterface
{
public:
    IdLinkToChildPair(MPLogId _cid, INode const * _node, ILogMsgLinkToChild * _link, bool _isListener) : cid(_cid), node(_node), link(_link), isList(_isListener) {}
    MPLogId                   queryId() const { return cid; }
    INode const *             queryNode() const { return node; }
    ILogMsgLinkToChild *      queryLink() const { return link; }
    bool                      isListener() const { return isList; }
private:
    MPLogId                   cid;
    INode const *             node;
    ILogMsgLinkToChild *      link;
    bool                      isList;
};

// Thread in CLogMsgManager which receives adoption requests from children

class LogMsgChildReceiverThread : public LogMsgReceiverThread
{
public:
    LogMsgChildReceiverThread() : LogMsgReceiverThread("LogMsgChildReceiver"), nextId(0) {}
    int                       run();
    void                      stop();
    MPLogId                   addChildToManager(MPLogId pid, INode * childNode, bool isListener, bool connected);
    bool                      removeChildFromManager(MPLogId cid, bool disconnected);
    bool                      removeChildFromManager(INode const * node, bool disconnected);
private:
    aindex_t                  findChild(MPLogId cid) const;
    aindex_t                  findChild(INode const * node) const;
    void                      doRemoveChildFromManager(aindex_t pos, bool disconnected);
private:
    CIArrayOf<IdLinkToChildPair> table;
    CriticalSection           tableOfChildrenCrit;
    MPLogId                   nextId;
};

// CHILD-SIDE CLASSES

// Thread in LinkToParentLogMsgHandler which receives filters from parent

class LogMsgFilterReceiverThread : public LogMsgReceiverThread
{
public:
    LogMsgFilterReceiverThread(MPLogId _pid, INode * _parentNode) : LogMsgReceiverThread("LogMsgFilterReceiver"), handler(0), pid(_pid), parentNode(_parentNode) {}
    void                      setHandler(ILogMsgHandler * _handler) { handler = _handler; }
    int                       run();
    void                      stop();
private:
    ILogMsgHandler *          handler;
    MPLogId                   pid;
    INode *                   parentNode;
};

// Class on manager's list of handlers which sends log messages to a parent, also holds thread which receives filter changes

class LinkToParentLogMsgHandler : public CInterface, implements ILogMsgHandler
{
public:
    LinkToParentLogMsgHandler(MPLogId _cid, MPLogId _pid, INode * _parentNode, bool _connected) : cid(_cid), pid(_pid), parentNode(_parentNode), receiverThread(new LogMsgFilterReceiverThread(_pid, _parentNode)), connected(_connected) { receiverThread->setHandler(this); }
    ~LinkToParentLogMsgHandler() { if(connected) disconnect(); receiverThread->stop(); }
    IMPLEMENT_IINTERFACE;
    void                      handleMessage(LogMsg const & msg) const;
    bool                      needsPrep() const { return false; }
    void                      prep() {}
    void                      addToPTree(IPropertyTree * tree) const;
    unsigned                  queryMessageFields() const { return MSGFIELD_all; }
    void                      setMessageFields(unsigned _fields = MSGFIELD_all) {}
    ILogMsgFilter *           receiveFilter() const;
    void                      startReceiver() { receiverThread->start(); }
    void                      connect();
    void                      disconnect();
    bool                      queryConnected() const { return connected; }
    void                      markDisconnected() { connected = false; }
    bool                      getLogName(StringBuffer &name) const { return false; }
private:
    Linked<INode>             parentNode;
    MPLogId                   cid;
    MPLogId                   pid;
    Owned<LogMsgFilterReceiverThread> receiverThread;
    bool                      connected;
};

// Pairing of INode * and LinkToParentLogMsgHandler * for LogMsgParentReceiverThread's table

class IdLinkToParentPair : public CInterface
{
public:
    IdLinkToParentPair(MPLogId _pid, INode const * _node, LinkToParentLogMsgHandler * _linkHandler) : pid(_pid), node(_node), linkHandler(_linkHandler) {}
    MPLogId                   queryId() const { return pid; }
    INode const *             queryNode() const { return node; }
    LinkToParentLogMsgHandler * queryLinkHandler() const { return linkHandler; }
private:
    MPLogId                   pid;
    INode const *             node;
    LinkToParentLogMsgHandler * linkHandler;
};

// Thread in CLogMsgManager which receives adoption requests from parents

class LogMsgParentReceiverThread : public LogMsgReceiverThread
{
public:
    LogMsgParentReceiverThread() : LogMsgReceiverThread("LogMsgParentReceiver"), nextId(0) {}
    int                       run();
    void                      stop();
    MPLogId                   getNextId();
    bool                      addParentToManager(MPLogId cid, MPLogId pid, INode * parentNode, bool connected);
    bool                      removeParentFromManager(MPLogId pid, bool disconnected);
    bool                      removeParentFromManager(INode const * parentNode, bool disconnected);
private:
    aindex_t                  findParent(MPLogId pid) const;
    aindex_t                  findParent(INode const * node) const;
    void                      doRemoveParentFromManager(aindex_t pos, bool disconnected);
private:
    CIArrayOf<IdLinkToParentPair> table;
    CriticalSection           tableOfParentsCrit;
    MPLogId                   nextId;
};

#endif
