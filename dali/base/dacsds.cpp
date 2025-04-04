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

#include "platform.h"
#include <typeinfo>
#include "jlib.hpp"
#include "jfile.hpp"
#include "javahash.hpp"
#include "javahash.tpp"
#include "jptree.ipp"
#include "jevent.hpp"

#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "dacoven.hpp"
#include "daserver.hpp"
#include "dasess.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"

#include "dasds.ipp" // common header for client/server sds
#include "dacsds.ipp"

static unsigned clientThrottleLimit;
static unsigned clientThrottleDelay;

static CriticalSection SDScrit;

#define CHECK_CONNECTED(XSTR)                                                                                        \
    if (!connected)                                                                                                   \
    {                                                                                                               \
        IERRLOG(XSTR": Closed connection (xpath=%s, sessionId=%" I64F "d)", xpath.get(), sessionId);                 \
        return;                                                                                                     \
    }

////////////////////

class MonitoredChildMap : public ChildMap
{
    CClientRemoteTree &owner;
public:
    MonitoredChildMap(CClientRemoteTree &_owner) : ChildMap(), owner(_owner) { }

    
    virtual bool replace(const char *name, IPropertyTree *tree)
    {
        // suppress notification of old node - old node has been preserved.
        bool changes = owner.queryConnection().queryStateChanges();
        owner.queryConnection().setStateChanges(false);
        bool res = ChildMap::replace(name, tree);
        owner.queryConnection().setStateChanges(changes);
        return res;
    }

    virtual void onRemove(void *e)
    {
        if (owner.queryStateChanges())
        {
            CClientRemoteTree *child = (CClientRemoteTree *)((IPropertyTree *)e);
            assertex(child);
            __int64 sId = child->queryServerId();
            if (sId)
                owner.registerDeleted(child->queryName(), 0, sId);
            else 
            {
                IPTArrayValue *value = child->queryValue();
                if (value)
                {
                    if (value->isArray())
                    {
                        unsigned i = value->elements();
                        while (i--)
                        {
                            CClientRemoteTree &child = *(CClientRemoteTree *)value->queryElement(i);
                            sId = child.queryServerId();
                            if (sId)
                                owner.registerDeleted(child.queryName(), i, sId);
                        }
                    }
                }
            }
        }
        ChildMap::onRemove(e);
    }
};

bool collectChildless(CClientRemoteTree *parent, const char *xpath, CRTArray *childLessList, StringArray *headArr, StringArray *tailArr)
{
    bool res = false;
    StringBuffer head;
    const char *tail;
    if (xpath && '/' == *xpath && '/' == *(xpath+1))
    {
        head.append("*");
        tail = xpath;
    }
    else
    {
        tail = xpath?queryHead(xpath, head):NULL;
        if (!tail && xpath)
            head.append(xpath);
    }
    if (parent->queryChildren())
    {
        if (head.length())
        {
            Owned<IPropertyTreeIterator> iter = parent->getElements(head.str());
            ForEach (*iter)
            {
                res |= collectChildless((CClientRemoteTree *)&iter->query(), tail, childLessList, headArr, tailArr);
                if (NULL == childLessList && res)
                    break;
            }
        }
        else
        {
            Owned<IPropertyTreeIterator> iter = parent->getElements("*");
            ForEach (*iter)
            {
                res |= collectChildless((CClientRemoteTree *)&iter->query(), NULL, childLessList, headArr, tailArr);
                if (NULL == childLessList && res)
                    break;
            }
        }
    }
    else if (parent->hasChildren()) // i.e. no local children, but server flagged as having
    {
        if (childLessList)
        {
            childLessList->append(*parent);
            if (headArr)
                headArr->append(head.length()?head.str():"");
            if (tailArr)
                tailArr->append(tail?tail:"");
        }
        res = true;
    }
    return res;
}

////////////////////

CRemoteConnection::CRemoteConnection(ISDSConnectionManager &manager, ConnectionId connectionId, const char *xpath, SessionId sessionId, unsigned mode, unsigned timeout)
    : CConnectionBase(manager, connectionId, xpath, sessionId, mode, timeout)
{
    INIT_NAMEDCOUNT;
    lazyFetch = true;
    stateChanges = true;
    connected = true;
    serverIterAvailable = querySDS().queryProperties().getPropBool("Client/@serverIterAvailable");
    serverIter =  querySDS().queryProperties().getPropBool("Client/@serverIter");
    useAppendOpt = querySDS().queryProperties().getPropBool("Client/@useAppendOpt");
    serverGetIdsAvailable = querySDS().queryProperties().getPropBool("Client/@serverGetIdsAvailable");
    lockCount = 0;
}

void CRemoteConnection::clearCommitChanges()
{
    CClientRemoteTree *tree = (CClientRemoteTree *) queryRoot();
    bool lazyFetch = setLazyFetch(false);
    tree->clearCommitChanges();
    setLazyFetch(lazyFetch);
}

void CRemoteConnection::getDetails(MemoryBuffer &mb)
{
    mb.append(connectionId);
    mb.append(sessionId);
    mb.append(mode);
    mb.append(timeout);
}

// IRemoteConnection impl.
void CRemoteConnection::changeMode(unsigned mode, unsigned timeout, bool suppressReloads)
{
    CHECK_CONNECTED("changeMode");
    manager.changeMode(*this, mode, timeout, suppressReloads);
}

void CRemoteConnection::rollback()
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("rollback");
    CDisableFetchChangeBlock block(*this);
    if (((CClientRemoteTree *)root.get())->queryState())
        reload(); // all
    else
    {
        class Cop : implements IIteratorOperator
        {
        public:
            virtual bool applyTop(IPropertyTree &_tree) { return true; }
            virtual bool applyChild(IPropertyTree &parent, IPropertyTree &_child, bool &levelBreak)
            {
                CClientRemoteTree &child = (CClientRemoteTree &)_child;
                if (!child.queryServerId() || child.queryState())
                {
                    ((CClientRemoteTree &)parent).clearChildren(); // wipe children - SDS will lazy fetch them again as needed.
                    levelBreak = true;
                    return false;
                }
                return true;
            }
        } op;
        CIterationOperation iop(op);
        iop.iterate(*root);
    }
}

void CRemoteConnection::_rollbackChildren(IPropertyTree *_parent, bool force)
{
    if (force)
    {
        CRemoteTreeBase *parent = QUERYINTERFACE(_parent, CRemoteTreeBase);
        if (parent)
            parent->clearChildren();
    }
    else
    {
        class CRollback
        {
        public:
            void apply(IPropertyTree &_parent)
            {
                CClientRemoteTree *parent = QUERYINTERFACE(&_parent, CClientRemoteTree);
                if (!parent)
                    return;
                if (parent->queryState())
                    parent->clearChildren();
                else
                {
                    Owned<IPropertyTreeIterator> iter = parent->getElements("*");
                    doit(*iter);
                }
            }
            void doit(IPropertyTreeIterator &iter)
            {
                ForEach (iter)
                    apply(iter.query());
            }
        } op;
        op.apply(*_parent);
    }
}

void CRemoteConnection::rollbackChildren(IPropertyTree *parent, bool force)
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("rollbackChildren");
    CDisableFetchChangeBlock block(*this);

    _rollbackChildren(parent, force);
}

void CRemoteConnection::rollbackChildren(const char *_xpath, bool force)
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("rollbackChildren");
    CDisableFetchChangeBlock block(*this);

    Owned<IPropertyTreeIterator> iter = root->getElements(_xpath);
    ForEach (*iter)
        _rollbackChildren(&iter->query(), force);
}

void CRemoteConnection::reload(const char *_xpath)
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("close");
    CDisableFetchChangeBlock block(*this);
    // NB: any linked client trees will still be active.
    if (_xpath == NULL || '\0' == *_xpath)
    {
        clearChanges(*root);
        __int64 serverId = root->queryServerId();
        CRemoteTreeBase *newTree = manager.get(*this, serverId);

        if (NULL == newTree) throw MakeSDSException(SDSExcpt_Reload);
        root.setown(newTree);
    }
    else
    {
        ICopyArrayOf<CClientRemoteTree> parents;
        IArrayOf<CClientRemoteTree> children;
        if ('/' == *_xpath) ++_xpath;
        StringBuffer head;
        const char *tail = splitXPath(_xpath, head);
        Owned<IPropertyTreeIterator> iter = root->getElements(head.str());
        ForEach (*iter)
        {
            CClientRemoteTree &parent = (CClientRemoteTree &)iter->query();
            Owned<IPropertyTreeIterator> childIter = parent.getElements(tail);
            ForEach (*childIter)
            {
                parents.append(parent);
                children.append(*LINK((CClientRemoteTree *)&childIter->query()));
            }
            ForEachItemIn(c, children)
            {
                CClientRemoteTree &child = children.item(c);
                clearChanges(child);
                parent.removeTree(&child);
            }
        }
        ForEachItemIn(e, children)
        {
            CClientRemoteTree &child = (CClientRemoteTree &)children.item(e);
            CClientRemoteTree &parent = (CClientRemoteTree &)parents.item(e);
            if (child.queryServerId())
            {
                IPropertyTree *newChild = manager.get(*this, child.queryServerId());
                if (newChild)
                    parent.addPropTree(child.queryName(), newChild);
            }
        }
    }
}

void CRemoteConnection::commit()
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("commit");
    manager.commit(*this, NULL);
}

void CRemoteConnection::close(bool deleteRoot)
{
    CConnectionLock b(*this);
    CHECK_CONNECTED("close");
    manager.commit(*this, &deleteRoot);
    connected=false;
}

SubscriptionId CRemoteConnection::subscribe(ISDSConnectionSubscription &notify)
{
    CSDSConnectionSubscriberProxy *subscriber = new CSDSConnectionSubscriberProxy(notify, connectionId);
    querySubscriptionManager(SDSCONN_PUBLISHER)->add(subscriber, subscriber->getId());
    return subscriber->getId();
}

void CRemoteConnection::unsubscribe(SubscriptionId id)
{
    querySubscriptionManager(SDSCONN_PUBLISHER)->remove(id);
}

class CClientXPathIterator : public CXPathIterator
{
    CRemoteConnection &connection;
public:
    CClientXPathIterator(CRemoteConnection &_connection, IPropertyTree *root, IPropertyTree *matchTree, IPTIteratorCodes flags) : CXPathIterator(root, matchTree, flags), connection(_connection)
    {
        connection.Link();
    }
    ~CClientXPathIterator()
    {
        connection.Release();
    }

    virtual IPropertyTree *queryChild(IPropertyTree *parent, const char *path)
    {
        // NB: this is going to fetch into local cache what is necessary to satisfy prop[X]
        CSetServerIterBlock b(connection, false);
        return parent->queryPropTree(path);
    }
};


void mergeXPathPTree(IPropertyTree *target, IPropertyTree *toMerge)
{
    Owned<IPropertyTreeIterator> iter = toMerge->getElements("*");
    ForEach (*iter)
    {
        IPropertyTree &e = iter->query();
        StringBuffer path(e.queryName());
        IPropertyTree *existing = target->queryPropTree(path.append("[@pos=\"").append(e.queryProp("@pos")).append("\"]").str());
        if (existing)
            mergeXPathPTree(existing, &e);
        else
            target->addPropTree(e.queryName(), LINK(&e));
    }
}

bool removeLocals(CRemoteConnection &connection, CRemoteTreeBase *tree, IPropertyTree *match)
{
    if (0 == tree->queryServerId())
        return false;
    Owned<IPropertyTreeIterator> iter = match->getElements("*");
    bool res = false;
    StringArray toDelete;
    ForEach (*iter)
    {
        IPropertyTree &child = iter->query();
        StringBuffer childPath(child.queryName());
        CRemoteTreeBase *storeChild;
        {
            CSetServerIterBlock b(connection, false);
            CDisableLazyFetchBlock b2(connection);
            storeChild = (CRemoteTreeBase *)tree->queryPropTree(childPath.append('[').append(child.queryProp("@pos")).append(']').str());
        }
        if (storeChild)
        {
            if (0 != storeChild->queryServerId())
            {
                bool childRes = removeLocals(connection, storeChild, &child);
                if (childRes)
                {
                    unsigned c = 0;
                    Owned<IPropertyTreeIterator> iter = child.getElements("*");
                    ForEach (*iter) c++;
                    if (0 == c)
                    {
                        toDelete.append(childPath.str());
                        res = true;
                    }
                }
            }
            else
            {
                toDelete.append(childPath.str());
                res = true;
            }
        }
    }
    ForEachItemIn(d, toDelete)
        match->removeProp(toDelete.item(d));
    return res;
}

void extractServerIds(IPropertyTree &tree, MemoryBuffer &mb, bool completeTailBranch)
{
    __int64 serverId = tree.getPropInt64("@serverId");
    assertex(serverId);
    mb.append(serverId);
    Owned<IPropertyTreeIterator> iter = tree.getElements("*");
    if (iter->first())
    {
        mb.append((unsigned) 1);
        do
        {
            extractServerIds(iter->query(), mb, completeTailBranch);
        }
        while (iter->next());
    }
    else
        mb.append(completeTailBranch ? (unsigned)0 : (unsigned)1);
}

static void walkAndFill(IPropertyTree &tree, CClientRemoteTree &parent, MemoryBuffer &mb, bool childrenCanBeMissing)
{
    parent.createChildMap();
    bool r;
    if (childrenCanBeMissing)
        mb.read(r);
    else
        r = true;
    if (r)
        parent.deserializeChildrenRT(mb);
    Owned<IPropertyTreeIterator> iter = tree.getElements("*");
    ForEach (*iter)
    {
        IPropertyTree &elem = iter->query();
        StringBuffer path(elem.queryName());
        path.append("[").append(elem.queryProp("@pos")).append("]");
        CClientRemoteTree *child = (CClientRemoteTree *)parent.queryPropTree(path.str());
        assertex(child);
        walkAndFill(elem, *child, mb, childrenCanBeMissing);
    }
}

IPropertyTreeIterator *CRemoteConnection::doGetElements(CClientRemoteTree *tree, const char *xpath, IPTIteratorCodes flags)
{
    CConnectionLock b(*this);
    CSetServerIterBlock b2(*this, false);
    Owned<IPropertyTree> matchTree, serverMatchTree;
    StringAttr path;
    if (xpath)
    {
        unsigned l = strlen(xpath);
        if ('/' == *(xpath+l-1))
            path.set(xpath, l-1);
        else
            path.set(xpath);
    }

    bool remoteGet = queryServerGetIdsAvailable() && iptiter_remoteget == (flags & iptiter_remoteget);
    {
        CDisableLazyFetchBlock b(*this);
        if (collectChildless(tree, path.get(), NULL, NULL, NULL))
        {
            serverMatchTree.setown(queryManager().getXPaths(tree->queryServerId(), xpath, remoteGet));
            if (serverMatchTree && removeLocals(*this, tree, serverMatchTree))
                serverMatchTree.clear();
        }
        // if all nodes had server-side children, then there'd be no point in this
        matchTree.setown(getXPathMatchTree(*tree, xpath));
    }
    if (matchTree && serverMatchTree)
        mergeXPathPTree(matchTree, serverMatchTree);
    else if (serverMatchTree)
        matchTree.setown(LINK(serverMatchTree));
    else if (!matchTree)
        return createNullPTreeIterator();

    if (remoteGet && serverMatchTree)
        queryManager().ensureLocal(*this, *tree, serverMatchTree, flags);

    return new CClientXPathIterator(*this, tree, matchTree, flags & ~iptiter_remote);
}

IPropertyTreeIterator *CRemoteConnection::getElements(const char *xpath, IPTIteratorCodes flags)
{
    if (!serverIterAvailable)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Server-side getElements not supported by server versions prior to " SDS_SVER_MIN_GETXPATHS_CONNECT);
    flags |= iptiter_remote;
    return root->getElements(xpath, flags);
}

/////////////////

CClientRemoteTree::CClientRemoteTree(CRemoteConnection &conn, CPState _state)
    : CRemoteTreeBase(NULL, NULL, NULL), state(_state), serverTreeInfo(0), connection(conn)
{
    INIT_NAMEDCOUNT;
    assertex(!isnocase());
}

CClientRemoteTree::CClientRemoteTree(const char *name, IPTArrayValue *value, ChildMap *children, CRemoteConnection &conn, CPState _state)
    : CRemoteTreeBase(name, value, children), state(_state), serverTreeInfo(0), connection(conn)
{
    INIT_NAMEDCOUNT;
    assertex(!isnocase());
}

void CClientRemoteTree::beforeDispose()
{
    if (queryStateChanges())
        connection.clearChanges(*this);
}

void CClientRemoteTree::Link() const
{
    connection.Link(); // inc ref count on connection
    CRemoteTreeBase::Link();
}

bool CClientRemoteTree::Release() const
{
    //Note: getLinkCount() is not thread safe.
    if (1 < getLinkCount())  //NH -> JCS - you sure this is best way to do this?
    {           
        bool res = CRemoteTreeBase::Release();
        connection.Release(); // if this tree is not being destroyed then decrement usage count on connection
        return res;
    }
    else
        return CRemoteTreeBase::Release();
}

void CClientRemoteTree::deserializeSelfRT(MemoryBuffer &mb)
{
    CRemoteTreeBase::deserializeSelfRT(mb);
    mb.read(serverTreeInfo);
}

void CClientRemoteTree::deserializeChildrenRT(MemoryBuffer &src)
{
    // if and only if there are children, must create monitored map here otherwise a non-monitored map could be create in base
    if (!children)
    {
        StringAttr eName;
        size32_t pos = src.getPos();
        src.read(eName);
        if (eName.length())
            createChildMap();
        src.reset(pos);
    }
    CRemoteTreeBase::deserializeChildrenRT(src);
}

bool CClientRemoteTree::renameTree(IPropertyTree *child, const char *newName)
{
    class DisableStateChanges // supress reset that would result from ownPTree via addPropTree below
    {
        bool changes;
        CRemoteConnection &c;
    public:
        DisableStateChanges(CRemoteConnection &_c) : c(_c) { changes = c.queryStateChanges(); c.setStateChanges(false); }
        ~DisableStateChanges() { reset(); }
        void reset() { c.setStateChanges(changes); }
    } dc(connection);
     
    Linked<IPropertyTree> tmp = child;
    StringAttr oldName = child->queryName();
    if (removeTree(child))
    {
        addPropTree(newName, child);
        tmp.getClear(); // addPropTree has taken ownership.
        dc.reset();
        __int64 id = ((CClientRemoteTree *)child)->queryServerId();
        if (id)
        {
            unsigned pos = findChild(child);
            registerRenamed(((CClientRemoteTree *)child)->queryName(), oldName, pos+1, id); // flag new element as changed.
        }
        return true;
    }
    return false;
}

IPropertyTree *CClientRemoteTree::queryBranch(const char *xpath) const
{
    return const_cast<CClientRemoteTree *>(this)->_queryBranch(xpath);
}

IPropertyTree *CClientRemoteTree::_queryBranch(const char *xpath)
{
    if (queryLazyFetch())
    {
        CRTArray childLessList;
        StringArray headArr, tailArr;
        StringAttr path;
        if (xpath)
        {
            unsigned l = strlen(xpath);
            if ('/' == *(xpath+l-1))
                path.set(xpath, l-1);
            else
                path.set(xpath);
        }
        
        CConnectionLock b(connection);
        bool r;
        { CDisableLazyFetchBlock b2(connection);
            r = collectChildless(this, xpath, &childLessList, &headArr, &tailArr);
        }
        if (r)
        {
            bool getAll = true;
            ForEachItemIn(c, childLessList)
            {
                IPropertyTree &child = childLessList.item(c);
                const char *tail = tailArr.item(c);
                if (!*tail) tail = NULL;
                const char *head = headArr.item(c);
                if (!*head) head = NULL;
                if (head || tail)
                {
                    getAll = false;
                    break;
                }
            }
            if (getAll)
                queryManager().getChildrenFor(childLessList, connection, 0);
            else
            {
                bool useServerIter = connection.queryServerGetIdsAvailable();
                // bug in matching tree creation caused failure if path pointed to self
                if (useServerIter && queryDaliServerVersion().compare("3.7") < 0)
                {
                    CDisableLazyFetchBlock b2(connection);
                    const IPropertyTree *me = queryPropTree(xpath);
                    if (me && me==this)
                        useServerIter = false;
                }
                if (useServerIter)
                {
                    CDisableLazyFetchBlock b2(connection);
                    Owned<IPropertyTree> serverMatchTree = queryManager().getXPaths(queryServerId(), xpath, true);
                    if (serverMatchTree && !removeLocals(connection, this, serverMatchTree))
                        queryManager().ensureLocal(connection, *this, serverMatchTree);
                    return queryPropTree(xpath); // intentionally inside disabled lazy fetching block, as now all local
                }
                else
                {
                    queryManager().getChildrenFor(childLessList, connection, 1); // get 1 level of all parents without children that matched xpath
                    ForEachItemIn(c, childLessList)
                    {
                        IPropertyTree &child = childLessList.item(c);
                        const char *tail = tailArr.item(c);
                        const char *head = headArr.item(c);
                        if (!tail) // no children _below_ the tail or no match for tail portion of xpath
                        {
                            // get all nodes below
                            CRTArray list;
                            Owned<IPropertyTreeIterator> iter = child.getElements(head);
                            ForEach (*iter)
                            {
                                IPropertyTree &c = iter->query();
                                list.append((CRemoteTreeBase &)iter->query());
                            }
                            queryManager().getChildrenFor(list, connection, 0);
                        }
                        else
                        {
                            // couldn't fully match match against partial cache, request more missing children
                            Owned<IPropertyTreeIterator> iter = child.getElements(head);
                            ForEach (*iter)
                            {
                                IPropertyTree &e = iter->query();
                                e.queryBranch(tail);
                            }
                        }
                    }
                }
            }
        }
    }
    
    return queryPropTree(xpath);
}

ChildMap *CClientRemoteTree::checkChildren() const
{
    return const_cast<CClientRemoteTree *>(this)->_checkChildren();
}

ChildMap *CClientRemoteTree::_checkChildren()
{
    CConnectionLock b(connection);
    if (!children && STI_HaveChildren & serverTreeInfo)
    {
        if (queryLazyFetch())
        {
            serverTreeInfo &= ~STI_HaveChildren;
            createChildMap();
            if (serverId)
                queryManager().getChildren(*this, connection);
        }
    }
    return children;
}

IPropertyTree *CClientRemoteTree::ownPTree(IPropertyTree *tree)
{
    // if taking ownership of an orphaned clientremote tree need to reset its attributes.
    if ((connection.queryStateChanges()) && isEquivalent(tree))
    {
        CClientRemoteTree * remoteTree = static_cast<CClientRemoteTree *>(tree);
        if (!remoteTree->IsShared())
        {
            if (remoteTree->queryServerId())
                remoteTree->resetState(CPS_Changed, true);
            return tree;
        }
    }

    return PARENT::ownPTree(tree);
}

IPropertyTree *CClientRemoteTree::create(const char *name, IPTArrayValue *value, ChildMap *children, bool existing)
{
    CClientRemoteTree *newTree = new CClientRemoteTree(name, value, children, connection);
    if (existing)
    {
        newTree->setServerId(queryServerId());
        setServerId(0);
    }
    return newTree;
}

IPropertyTree *CClientRemoteTree::create(MemoryBuffer &mb)
{
    unsigned pos = mb.getPos();
    StringAttr name;
    mb.read(name);
    mb.reset(pos);
    CClientRemoteTree *tree = new CClientRemoteTree(connection);
    tree->deserializeSelfRT(mb);
    return tree;
}

void CClientRemoteTree::createChildMap()
{
    children = new MonitoredChildMap(*this);
}

ChangeInfo *CClientRemoteTree::queryChanges()
{
    return connection.queryChangeInfo(*this);
}

void CClientRemoteTree::setLocal(size32_t size, const void *data, bool _binary)
{
    clearState(CPS_PropAppend);
    mergeState(CPS_Changed);
    PARENT::setLocal(size, data, _binary);
}

void CClientRemoteTree::appendLocal(size32_t size, const void *data, bool binary)
{
    if (0 == size) return;
    if (0 != serverId)
    {
        if (0 != (CPS_PropAppend & state))
        {
            PARENT::appendLocal(size, data, binary);
            return;
        }
        else if (0 == (CPS_Changed & state))
        {
            if (value)
            {
                size32_t sz = value->queryValueSize();
                if (!binary && sz) --sz;
                if (sz)
                {
                    mergeState(CPS_PropAppend);
                    registerPropAppend(sz);
                    PARENT::appendLocal(size, data, binary);
                    return;
                }
            }
            else
            {
                if (STI_External & serverTreeInfo) // if it has, change will only be fetched on a get call
                {
                    mergeState(CPS_PropAppend);
                    registerPropAppend(0); // whole value on commit to be sent for external append.
                    PARENT::appendLocal(size, data, binary);
                    return;
                }
            }
        }
    }
    mergeState(CPS_Changed);
    PARENT::appendLocal(size, data, binary);
}

void CClientRemoteTree::addingNewElement(IPropertyTree &child, int pos)
{
    ((CClientRemoteTree &)child).setState(CPS_New);
#ifdef ENABLE_INSPOS
    if (pos >= 0)
        ((CRemoteTreeBase &)child).mergeState(CPS_InsPos);
#endif
    PARENT::addingNewElement(child, pos);
}

void CClientRemoteTree::removingElement(IPropertyTree *tree, unsigned pos)
{
    CRemoteTreeBase *child = QUERYINTERFACE(tree, CRemoteTreeBase); assertex(child);
    registerDeleted(child->queryName(), pos, child->queryServerId());
    PARENT::removingElement(tree, pos);
}

void CClientRemoteTree::setAttribute(const char *attr, const char *val, bool encoded)
{
    PARENT::setAttribute(attr, val, encoded);
    mergeState(CPS_AttrChanges);
    registerAttrChange(attr);
}

bool CClientRemoteTree::removeAttribute(const char *attr)
{
    if (PARENT::removeAttribute(attr))
    {
        registerDeletedAttr(attr);
        return true;
    }
    else
        return false;
}

void CClientRemoteTree::serializeSelf(MemoryBuffer &tgt)
{
    checkExt();
    PARENT::serializeSelf(tgt);
}

void CClientRemoteTree::registerRenamed(const char *newName, const char *oldName, unsigned pos, __int64 id)
{
    mergeState(CPS_Renames);
    if (queryStateChanges())
        connection.registerRenamed(*this, newName, oldName, pos, id);
}

void CClientRemoteTree::registerDeleted(const char *name, unsigned position, __int64 id)
{
    if (id)
    {
        mergeState(CPS_Deletions);
        if (queryStateChanges())
            connection.registerDeleted(*this, name, position, id);
    }
}

void CClientRemoteTree::registerAttrChange(const char *attr)
{
    mergeState(CPS_AttrChanges);
    if (queryStateChanges())
        connection.registerAttrChange(*this, attr);
}

void CClientRemoteTree::registerDeletedAttr(const char *attr)
{
    mergeState(CPS_AttrDeletions);
    if (queryStateChanges())
        connection.registerDeletedAttr(*this, attr);
}

void CClientRemoteTree::registerPropAppend(size32_t l)
{
    mergeState(CPS_PropAppend);
    if (queryStateChanges())
        connection.registerPropAppend(*this, l);
}

void CClientRemoteTree::clearChanges()
{
    if (0 != (STI_External & serverTreeInfo) && 0 != (CPS_PropAppend & state))
        setProp(NULL, (char *)NULL);
    connection.clearChanges(*this);
}

// block these ops on other threads during a commit, otherwise can lead to internal sds inconsistency.
void CClientRemoteTree::addProp(const char *xpath, const char *val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::addProp(xpath, val);
}

void CClientRemoteTree::setProp(const char *xpath, const char *val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::setProp(xpath, val);
}

void CClientRemoteTree::addPropInt64(const char *xpath, __int64 val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::addPropInt64(xpath, val);
}

void CClientRemoteTree::setPropInt64(const char *xpath, __int64 val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::setPropInt64(xpath, val);
}

void CClientRemoteTree::addPropReal(const char *xpath, double val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::addPropReal(xpath, val);
}

void CClientRemoteTree::setPropReal(const char *xpath, double val)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::setPropReal(xpath, val);
}

void CClientRemoteTree::setPropBin(const char *xpath, size32_t size, const void *data)
{
    CConnectionLock b(connection);
    CRemoteTreeBase::setPropBin(xpath, size, data);
}

IPropertyTree *CClientRemoteTree::setPropTree(const char *xpath, IPropertyTree *val)
{
    CConnectionLock b(connection);
    return CRemoteTreeBase::setPropTree(xpath, val);
}

IPropertyTree *CClientRemoteTree::addPropTree(const char *xpath, IPropertyTree *val)
{
    CConnectionLock b(connection);
    return CRemoteTreeBase::addPropTree(xpath, val);
}

bool CClientRemoteTree::removeProp(const char *xpath)
{
    CConnectionLock b(connection);
    return CRemoteTreeBase::removeProp(xpath);
}

bool CClientRemoteTree::removeTree(IPropertyTree *child)
{
    CConnectionLock b(connection);
    return CRemoteTreeBase::removeTree(child);
}

void CClientRemoteTree::checkExt() const
{
    if (!connection.queryUseAppendOpt()) return;
    if (!value)
    {
        if (STI_External & serverTreeInfo)
        {
            MemoryBuffer mb;
            queryManager().getExternalValueFromServerId(serverId, mb);
            if (mb.length())
            {
                bool binary = IptFlagTst(flags, ipt_binary);
                const_cast<CClientRemoteTree *>(this)->setValue(new CPTValue(mb), binary);
            }
            else
                serverTreeInfo &= ~STI_External;
        }
    }
    else if (0 != (CPS_PropAppend & state))
    {
        if (STI_External & serverTreeInfo)
        {
            MemoryBuffer mb;
            bool binary = IptFlagTst(flags, ipt_binary);
            queryManager().getExternalValueFromServerId(serverId, mb);
            if (mb.length())
            {
                const_cast<CClientRemoteTree *>(this)->setValue(new CPTValue(mb), binary);
                assertex(queryStateChanges());
                connection.registerPropAppend(*const_cast<CClientRemoteTree *>(this), mb.length());
                if (value)
                    value->getValue(mb, binary);
            }
            else
                serverTreeInfo &= ~STI_External;
        }
    }
}

bool CClientRemoteTree::isCompressed(const char *xpath) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::isCompressed(xpath);
}

bool CClientRemoteTree::getProp(const char *xpath, StringBuffer &ret) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::getProp(xpath, ret);
}

const char *CClientRemoteTree::queryProp(const char * xpath) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::queryProp(xpath);
}

bool CClientRemoteTree::getPropBool(const char *xpath, bool dft) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::getPropBool(xpath, dft);
}

__int64 CClientRemoteTree::getPropInt64(const char *xpath, __int64 dft) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::getPropInt64(xpath, dft);
}

bool CClientRemoteTree::getPropBin(const char *xpath, MemoryBuffer &ret) const
{
    if (!xpath) checkExt();
    return CRemoteTreeBase::getPropBin(xpath, ret);
}

IPropertyTreeIterator *CClientRemoteTree::getElements(const char *xpath, IPTIteratorCodes flags) const
{
    if (!serverId || !queryLazyFetch()
        || !xpath || '\0' == *xpath || ('*' == *xpath && '\0' == *(xpath+1)) || !connection.queryServerIterAvailable() || (0 == (flags & iptiter_remote) && !connection.queryServerIter()) )
        return CRemoteTreeBase::getElements(xpath, flags);
    if (!hasChildren()) // not necessarily local yet.
        return createNullPTreeIterator();
    // if it's a single id, then not worth getting matches from server as level either present or needed.
    const char *xxpath = xpath;
    if (isValidXPathStartChr(*xxpath))
    {
        do { ++xxpath; }
        while (isValidXPathChr(*xxpath));
    }
    if ('\0' == *xxpath || ('/' == *xxpath && '/' != *(xxpath+1)))
        return CRemoteTreeBase::getElements(xpath, flags);
    return connection.doGetElements(const_cast<CClientRemoteTree *>(this), xpath, flags);
}

void CClientRemoteTree::localizeElements(const char *xpath, bool allTail)
{
    if (!serverId || !queryLazyFetch() || !connection.queryServerGetIdsAvailable()
        || !xpath || '\0' == *xpath || ('*' == *xpath && '\0' == *(xpath+1)) || !hasChildren())
        return;

    IPTIteratorCodes flags = iptiter_remoteget;
    if (allTail)
        flags = iptiter_remotegetbranch;
    Owned<IPropertyTreeIterator> iter = connection.doGetElements(this, xpath, flags);
    return;
}

void CClientRemoteTree::resetState(unsigned _state, bool sub)
{
    state = _state;
    serverId = 0;
    if (sub)
    {
        Owned<IPropertyTreeIterator> iter = getElements("*");
        ForEach(*iter)
        {
            CClientRemoteTree &child = (CClientRemoteTree &)iter->query();
            child.resetState(state, sub);
        }
    }
}

IPropertyTree *CClientRemoteTree::collateData()
{
    ChangeInfo *changes = queryChanges();
    struct ChangeTree
    {
        ChangeTree(IPropertyTree *donor=NULL) { ptree = LINK(donor); }
        ~ChangeTree() { ::Release(ptree); }
        inline void createTree() { assertex(!ptree); ptree = createPTree(RESERVED_CHANGE_NODE, ipt_fast); }
        inline IPropertyTree *queryTree() { return ptree; }
        inline IPropertyTree *getTree() { return LINK(ptree); }
        inline IPropertyTree *queryCreateTree()
        {
            if (!ptree)
                ptree = createPTree(RESERVED_CHANGE_NODE, ipt_fast);
            return ptree;
        }
    private:
        StringAttr name;
        IPropertyTree *ptree;
    } ct(changes?changes->tree:NULL);
    if (changes) changes->tree.clear();

    if (0 == serverId)
    {
        ct.createTree();
        Owned<IAttributeIterator> iter = getAttributes();
        if (iter->count())
        {
            IPropertyTree *t = ct.queryTree()->addPropTree(ATTRCHANGE_TAG);
            ForEach(*iter)
                t->setProp(iter->queryName(), queryProp(iter->queryName()));

        }
        ct.queryTree()->setPropBool("@new", true);
    }
    else
    {
        if (ct.queryTree())
        {
            Linked<IPropertyTree> ac = ct.queryTree()->queryPropTree(ATTRCHANGE_TAG);
            if (ac)
            {
                ct.queryTree()->removeTree(ac);
                Owned<IAttributeIterator> iter = ac->getAttributes();
                IPropertyTree *t = ct.queryTree()->addPropTree(ATTRCHANGE_TAG);
                ForEach(*iter)
                    t->setProp(iter->queryName(), queryProp(iter->queryName()));
            }
        }
    }
    if ((CPS_Changed & state) || (0 == serverId && queryValue()))
    {
        ct.queryCreateTree()->setPropBool("@localValue", true);
        if (queryValue())
        {
            bool binary=isBinary(NULL);
            ((PTree *)ct.queryTree())->setValue(new CPTValue(queryValue()->queryValueRawSize(), queryValue()->queryValueRaw(), binary, true, isCompressed(NULL)), binary);
        }
        else
            ((PTree *)ct.queryTree())->setValue(new CPTValue(0, NULL, false, true, false), false);
    }
    else if (CPS_PropAppend & state)
    {
        assertex(serverId);
        IPropertyTree *pa = ct.queryTree()->queryPropTree(APPEND_TAG);
        assertex(pa);
        unsigned from = pa->getPropInt(NULL);
        ct.queryTree()->removeTree(pa);
        ct.queryCreateTree()->setPropBool("@appendValue", true);
        MemoryBuffer mb;
        bool binary=isBinary(NULL);
        queryValue()->getValue(mb, true);
        ((PTree *)ct.queryTree())->setValue(new CPTValue(mb.length()-from, mb.toByteArray()+from, binary), binary);
    }

    Owned<IPropertyTree> childTree;
    Owned<IPropertyTreeIterator> _iter = getElements("*");
    IPropertyTreeIterator *iter = _iter;
    if (iter->first())
    {
        while (iter->isValid())
        {
            CClientRemoteTree *child = (CClientRemoteTree *) &iter->query();
            childTree.setown(child->collateData());
            if (childTree)
            {
                if (0 == child->queryServerId())
                {
                    if (CPS_InsPos & child->queryState())
                    {
                        int pos = findChild(child);
                        assertex(NotFound != pos);
                        childTree->setPropInt("@pos", pos+1);
                    }
                }
                else
                {
                    int pos = findChild(child);
                    assertex(NotFound != pos);
                    childTree->setPropInt("@pos", pos+1);
                    childTree->setPropInt64("@id", child->queryServerId());
                }
            }
            if (childTree)
                ct.queryCreateTree()->addPropTree(RESERVED_CHANGE_NODE, childTree.getClear());
            iter->next();
        }
    }
    if (ct.queryTree())
        ct.queryTree()->setProp("@name", queryName());
    return ct.getTree();
}

void CClientRemoteTree::clearCommitChanges(MemoryBuffer *mb)
{
    class Cop : implements IIteratorOperator
    {
    public:
        Cop(MemoryBuffer *_mb=NULL) : mb(_mb) { }
        virtual bool applyTop(IPropertyTree &_tree)
        {
            CClientRemoteTree &tree = (CClientRemoteTree &) _tree;
            tree.clearChanges();
            if (tree.queryState())
                tree.setState(0);
            return true;
        }
        virtual bool applyChild(IPropertyTree &parent, IPropertyTree &child, bool &levelBreak)
        {
            CClientRemoteTree &tree = (CClientRemoteTree &) child;
            if (mb && 0==tree.queryServerId())
            {
                __int64 serverId;
                mb->read(serverId);
                tree.setServerId(serverId);
            }
            return true;
        }
    private:
        MemoryBuffer *mb;
    } op(mb);

    CIterationOperation iop(op);
    iop.iterate(*this);
}

/////////////////////

CClientSDSManager::CClientSDSManager() 
{
    CDaliVersion serverVersionNeeded("2.1"); // to ensure backward compatibility
    childrenCanBeMissing = queryDaliServerVersion().compare(serverVersionNeeded) >= 0;
    CDaliVersion serverVersionNeeded2("3.4"); // to ensure backward compatibility
    lazyExtFlag = queryDaliServerVersion().compare(serverVersionNeeded2) >= 0 ? DAMP_SDSCMD_LAZYEXT : 0;
    properties = NULL;
    IPropertyTree &props = queryProperties();
    CDaliVersion serverVersionNeeded3(SDS_SVER_MIN_GETXPATHS_CONNECT);
    if (queryDaliServerVersion().compare(serverVersionNeeded3) < 0)
        props.removeProp("Client/@serverIter");
    else
        props.setPropBool("Client/@serverIterAvailable", true);
    clientThrottleLimit = props.getPropInt("Client/Throttle/@limit", CLIENT_THROTTLE_LIMIT);
    clientThrottleDelay = props.getPropInt("Client/Throttle/@delay", CLIENT_THROTTLE_DELAY);

    CDaliVersion appendOptVersionNeeded(SDS_SVER_MIN_APPEND_OPT); // min version for append optimization
    props.setPropBool("Client/@useAppendOpt", queryDaliServerVersion().compare(appendOptVersionNeeded) >= 0);
    CDaliVersion serverVersionNeeded4(SDS_SVER_MIN_GETIDS); // min version for get xpath with server ids
    if (queryDaliServerVersion().compare(serverVersionNeeded4) >= 0)
        props.setPropBool("Client/@serverGetIdsAvailable", true);
    concurrentRequests.signal(clientThrottleLimit);
}

CClientSDSManager::~CClientSDSManager()
{
    closedown();
    ::Release(properties);
}

void CClientSDSManager::closedown()
{
    CriticalBlock block(connections.crit);
    SuperHashIteratorOf<CConnectionBase> iter(connections.queryBaseTable());
    ForEach(iter)
    {
        CRemoteConnection &conn = (CRemoteConnection &) iter.query();
        conn.setConnected(false);
    }
}

bool CClientSDSManager::sendRequest(CMessageBuffer &mb, bool throttle)
{
    if (throttle)
    {
        bool avail = concurrentRequests.wait(clientThrottleDelay);
        if (!avail)
            OWARNLOG("Excessive concurrent Dali SDS client transactions. Transaction delayed.");
        bool res;
        try { res = queryCoven().sendRecv(mb, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST); }
        catch (IException *)
        {
            if (avail)
                concurrentRequests.signal();
            throw;
        }
        if (avail)
            concurrentRequests.signal();
        return res;
    }
    else
        return queryCoven().sendRecv(mb, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST);
}

CRemoteTreeBase *CClientSDSManager::get(CRemoteConnection &connection, __int64 serverId)
{
    CCycleTimer elapsedTime(recordingEvents());
    CMessageBuffer mb;

    if (childrenCanBeMissing)
        mb.append((int)DAMP_SDSCMD_GET2 | lazyExtFlag);
    else
        mb.append((int)DAMP_SDSCMD_GET | lazyExtFlag);
    mb.append(connection.queryConnectionId());
    mb.append(serverId);

    size32_t sendSize = mb.length();
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    
    mb.read((int &)replyMsg);
    
    CClientRemoteTree *tree = NULL;
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            CDisableFetchChangeBlock block(connection);
            tree = new CClientRemoteTree(connection);
            tree->deserializeSelfRT(mb);
            break;
        }
        case DAMP_SDSREPLY_EMPTY:
            break;
        default:
            throwMbException("SDS Reply Error ", mb);
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliGet(connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + mb.length());

    return tree;
}

void CClientSDSManager::getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels)
{
    CCycleTimer elapsedTime(recordingEvents());
    CMessageBuffer mb;

    if (childrenCanBeMissing)
        mb.append((int)DAMP_SDSCMD_GETCHILDREN2 | lazyExtFlag);
    else
        mb.append((int)DAMP_SDSCMD_GETCHILDREN | lazyExtFlag);
    mb.append(connection.queryConnectionId());
    mb.append(parent.queryServerId());
    mb.append(levels);
    mb.append((__int64)0); // terminator
    size32_t sendSize = mb.length();
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "fetching SDS branch");

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_EMPTY:
            return;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }

    CDisableFetchChangeBlock block(connection);
    if (childrenCanBeMissing)
    {
        bool r;
        mb.read(r);
        if (!r) return;
    }
    parent.deserializeChildrenRT(mb);

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliGetChildren(connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + mb.length());
}

static void matchServerTree(CClientRemoteTree *local, IPropertyTree &matchTree, ICopyArrayOf<CClientRemoteTree> &matchedLocals, ICopyArrayOf<IPropertyTree> &matched, bool allTail, MemoryBuffer &mb)
{
    Owned<IPropertyTreeIterator> matchIter = matchTree.getElements("*");
    if (matchIter->first())
    {
        if (!local || (local->hasChildren() && NULL == local->queryChildren()))
        {
            if (local)
            {
                matchedLocals.append(*local);
                matched.append(matchTree);
            }
            mb.append(matchTree.getPropInt64("@serverId"));
            mb.append((unsigned)1);
        }

        do
        {
            IPropertyTree &elem = matchIter->query();
            StringBuffer path(elem.queryName());
            path.append('[').append(elem.getPropInt("@pos")).append(']');
            CClientRemoteTree *child = local ? (CClientRemoteTree *)local->queryPropTree(path.str()) : NULL;
            matchServerTree(child, elem, matchedLocals, matched, allTail, mb);
        }
        while (matchIter->next());
    }
    else
    {
        if (!local || (local->hasChildren() && NULL == local->queryChildren()))
        {
            if (local)
            {
                matchedLocals.append(*local);
                matched.append(matchTree);
            }
            mb.append(matchTree.getPropInt64("@serverId"));     
            mb.append(allTail ? (unsigned)0 : (unsigned)1);
        }
    }
}

void CClientSDSManager::ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags)
{
    CCycleTimer elapsedTime(recordingEvents());
    CClientRemoteTree &parent = (CClientRemoteTree &)_parent;

    CMessageBuffer remoteGetMb;
    if (childrenCanBeMissing)
        remoteGetMb.append((int)DAMP_SDSCMD_GETCHILDREN2 | lazyExtFlag);
    else
        remoteGetMb.append((int)DAMP_SDSCMD_GETCHILDREN | lazyExtFlag);
    remoteGetMb.append(connection.queryConnectionId());
    ICopyArrayOf<CClientRemoteTree> matchedLocals;
    ICopyArrayOf<IPropertyTree> matched;
    bool getLeaves = iptiter_remotegetbranch == (flags & iptiter_remotegetbranch);

    CDisableFetchChangeBlock block(connection);
    matchServerTree(&parent, *serverMatchTree, matchedLocals, matched, getLeaves, remoteGetMb);

    if (0 == matched.ordinality())
        return;

    remoteGetMb.append((__int64)0);
    size32_t sendSize = remoteGetMb.length();
    if (!sendRequest(remoteGetMb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "ensureLocal");

    SdsReply replyMsg;  
    remoteGetMb.read((int &)replyMsg);
        
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_EMPTY:
            return;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", remoteGetMb);
        default:
            assertex(false);
    }

    ForEachItemIn(m, matched)
        walkAndFill(matched.item(m), matchedLocals.item(m), remoteGetMb, childrenCanBeMissing);

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliEnsureLocal(connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + remoteGetMb.length());

}

void CClientSDSManager::getChildrenFor(CRTArray &childLessList, CRemoteConnection &connection, unsigned levels)
{
    CCycleTimer elapsedTime(recordingEvents());
    CMessageBuffer mb;

    if (childrenCanBeMissing)
        mb.append((int)DAMP_SDSCMD_GETCHILDREN2 | lazyExtFlag);
    else
        mb.append((int)DAMP_SDSCMD_GETCHILDREN | lazyExtFlag);
    mb.append(connection.queryConnectionId());

    ForEachItemIn(f, childLessList)
    {
        CRemoteTreeBase &parent = childLessList.item(f);
        if (parent.queryServerId())
        {
            mb.append(parent.queryServerId());
            mb.append(levels);
        }
    }
    mb.append((__int64)0); // terminator
    size32_t sendSize = mb.length();
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "getChildrenFor");

    SdsReply replyMsg;  
    mb.read((int &)replyMsg);
        
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_EMPTY:
            return;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }

    CDisableFetchChangeBlock block(connection);
    ForEachItemIn(f2, childLessList)
    {
        CRemoteTreeBase &parent = childLessList.item(f2);
        parent.createChildMap();
        if (parent.queryServerId())
        {
            bool r;
            if (childrenCanBeMissing)
                mb.read(r);
            else
                r = true;
            if (r)
                parent.deserializeChildrenRT(mb);
        }
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliGetChildrenFor(connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + mb.length());
}

IPropertyTreeIterator *CClientSDSManager::getElements(CRemoteConnection &connection, const char *xpath)
{
    CCycleTimer elapsedTime(recordingEvents());
    CMessageBuffer mb;

    mb.append((int)DAMP_SDSCMD_GETELEMENTS | lazyExtFlag);
    mb.append(connection.queryConnectionId());
    mb.append(xpath);

    size32_t sendSize = mb.length();
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    CClientRemoteTree *tree = NULL;
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            unsigned count;
            mb.read(count);
            CDisableFetchChangeBlock block(connection);
            Owned<DaliPTArrayIterator> iter = new DaliPTArrayIterator();
            while (count--)
            {
                CClientRemoteTree *tree = new CClientRemoteTree(connection);
                iter->array.append(*tree);
                tree->deserializeSelfRT(mb);
            }

            if (unlikely(recordingEvents()))
                queryRecorder().recordDaliGetElements(xpath, connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + mb.length());

            return LINK(iter);
        }
        default:
            throwMbException("SDS Reply Error ", mb);
    }
    return NULL;
}

void CClientSDSManager::noteDisconnected(CRemoteConnection &connection)
{
    connection.setConnected(false);
    connections.removeExact(&connection);
}

void CClientSDSManager::commit(CRemoteConnection &connection, bool *disconnectDeleteRoot)
{
    CCycleTimer elapsedTime(recordingEvents());
    CriticalBlock b(crit); // if >1 commit per client concurrently would cause problems with serverId.

    CClientRemoteTree *tree = (CClientRemoteTree *) connection.queryRoot();

    size32_t dataSize = 0;
    try
    {
        CMessageBuffer mb;
        mb.append((int)DAMP_SDSCMD_DATA);
        mb.append(connection.queryConnectionId());
        if (disconnectDeleteRoot)
        {
            mb.append((byte)(0x80 + 1)); // kludge, high bit to indicate new client format. (for backward compat.)
            mb.append(*disconnectDeleteRoot);
        }
        else
            mb.append((byte)0x80); // kludge, high bit to indicate new client format. (for backward compat.)
        bool lazyFetch = connection.setLazyFetch(false);
        Owned<IPropertyTree> changes = tree->collateData();
        connection.setLazyFetch(lazyFetch);

        if (NULL == disconnectDeleteRoot && !changes) return;
        if (changes) changes->serialize(mb);
        try
        {
            dataSize += mb.length();
            if (!sendRequest(mb))
                throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "committing");
        }
        catch (IDaliClient_Exception *e)
        {
            if (DCERR_server_closed == e->errorCode())
            {
                if (changes)
                    IWARNLOG("Dali server disconnect, failed to commit data");
                e->Release();
                if (disconnectDeleteRoot)
                    noteDisconnected(connection);
                return; // JCSMORE does this really help, shouldn't it just throw?
            }
            else
                throw;
        }

        SdsReply replyMsg;
        mb.read((int &)replyMsg);

        switch (replyMsg)
        {
            case DAMP_SDSREPLY_OK:
            {
                bool lazyFetch = connection.setLazyFetch(false);
                // NOTE: this means that send collated data order and the following order have to match!
                // JCSMORE - true but.. hmm.. (could possibly have alternative lookup scheme)
                tree->clearCommitChanges(&mb);
                assertex(mb.getPos() == mb.length()); // must have read it all
                connection.setLazyFetch(lazyFetch);
                break;
            }
            case DAMP_SDSREPLY_EMPTY:
                break;
            case DAMP_SDSREPLY_ERROR:
                throwMbException("SDS Reply Error ", mb);
            default:
                assertex(false);
        }

        dataSize += mb.length();
    }
    catch (IException *)
    {
        if (disconnectDeleteRoot)
            noteDisconnected(connection);
        throw;
    }
    if (disconnectDeleteRoot)
        noteDisconnected(connection);

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliCommit(connection.queryConnectionId(), elapsedTime.elapsedNs(), dataSize);
}

void CClientSDSManager::changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReloads)
{
    CCycleTimer elapsedTime(recordingEvents());
    CConnectionLock b(connection);
    if (mode & RTM_CREATE_MASK)
        throw MakeSDSException(SDSExcpt_BadMode, "calling changeMode");
    unsigned prevMode = connection.queryMode();
    if (RTM_MODE(prevMode, RTM_LOCK_WRITE) && !RTM_MODE(mode, RTM_LOCK_WRITE))
        commit(connection, NULL);

    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_CHANGEMODE);
    mb.append(connection.queryConnectionId());
    mb.append(mode).append(timeout);

    size32_t sendSize = mb.length();
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "changing mode");

    SdsReply replyMsg;
    
    mb.read((int &)replyMsg);
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            connection.setMode(mode);
            if (!suppressReloads)
            {
                if (RTM_MODE(mode, RTM_LOCK_WRITE) && !RTM_MODE(prevMode, RTM_LOCK_WRITE) && !RTM_MODE(prevMode, RTM_LOCK_READ))
                    connection.reload();
            }
            break;
        }
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliChangeMode(connection.queryConnectionId(), elapsedTime.elapsedNs(), sendSize + mb.length());
}

// ISDSManager impl.
#define MIN_MCONNECT_SVER "1.5"
IRemoteConnections *CClientSDSManager::connect(IMultipleConnector *mConnect, SessionId id, unsigned timeout)
{
    CCycleTimer elapsedTime(recordingEvents());
    CDaliVersion serverVersionNeeded(MIN_MCONNECT_SVER);
    if (queryDaliServerVersion().compare(serverVersionNeeded) < 0)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Multiple connect not supported by server versions prior to " MIN_MCONNECT_SVER);

    if (0 == id || id != myProcessSession())
        throw MakeSDSException(SDSExcpt_InvalidSessionId, ", in multi connect, sessionid=%" I64F "x", id);

    CMessageBuffer mb;
    mb.append((unsigned)DAMP_SDSCMD_MCONNECT | lazyExtFlag);
    mb.append(id).append(timeout);
    mConnect->serialize(mb);

    size32_t sendSize = mb.length();
    if (!sendRequest(mb, true))
    {
        StringBuffer s(", making multiple connect to ");
        getMConnectString(mConnect, s);
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "%s", s.str());
    }

    SdsReply replyMsg;
    
    Owned<CRemoteConnections> remoteConnections = new CRemoteConnections();

    unsigned c;
    for (c=0; c<mConnect->queryConnections(); c++)
    {
        mb.read((int &)replyMsg);
        switch (replyMsg)
        {
            case DAMP_SDSREPLY_OK:
            {
                ConnectionId connId;
                mb.read(connId);
                StringAttr xpath;
                unsigned mode;
                mConnect->getConnectionDetails(c, xpath, mode);
                Owned<CRemoteConnection> conn = new CRemoteConnection(*this, connId, xpath, id, mode & ~RTM_CREATE_MASK, timeout);
                assertex(conn.get());
                if (queryProperties().getPropBool("Client/@LogConnection"))
                    DBGLOG("SDSManager::connect() - IMultipleConnector: RemoteConnection ID<%" I64F "x>, timeout<%d>", connId, timeout);

                CClientRemoteTree *tree;
                { CDisableFetchChangeBlock block(*conn);
                    tree = new CClientRemoteTree(*conn);
                    tree->deserializeRT(mb);
                }
                conn->setRoot(tree);
                connections.replace(*conn);
                remoteConnections->add(LINK(conn));
                break;
            }
            case DAMP_SDSREPLY_ERROR:
                throwMbException("SDS Reply Error ", mb);
            default:
                assertex(false);
        }
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliConnect("Multi connect", 0, elapsedTime.elapsedNs(), sendSize + mb.length());

    return LINK(remoteConnections);
}

IRemoteConnection *CClientSDSManager::connect(const char *xpath, SessionId id, unsigned mode, unsigned timeout)
{
    CCycleTimer elapsedTime(recordingEvents());
    if (0 == id || id != myProcessSession())
        throw MakeSDSException(SDSExcpt_InvalidSessionId, ", connecting to %s, sessionid=%" I64F "x", xpath, id);

    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_CONNECT | lazyExtFlag);
    mb.append(id).append(mode).append(timeout);
    mb.append(xpath);

    size32_t sendSize = mb.length();
    if (!sendRequest(mb, true))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, ", connecting to %s", xpath);

    SdsReply replyMsg;
    
    mb.read((int &)replyMsg);
    
    CRemoteConnection *conn = NULL;
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            ConnectionId connId;
            mb.read(connId);
            conn = new CRemoteConnection(*this, connId, xpath, id, mode & ~RTM_CREATE_MASK, timeout);
            assertex(conn);
            if (queryProperties().getPropBool("Client/@LogConnection"))
                DBGLOG("SDSManager::connect(): xpath<%s>, RemoteConnection ID<%" I64F "x>, mode<%x>, timeout<%d>", xpath, connId, mode, timeout);

            CClientRemoteTree *tree;
            { CDisableFetchChangeBlock block(*conn);
                tree = new CClientRemoteTree(*conn);
                tree->deserializeRT(mb);
            }
            conn->setRoot(tree);
            connections.replace(*conn);
            break;
        }
        case DAMP_SDSREPLY_EMPTY:
            break;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliConnect(xpath, conn ? conn->queryConnectionId() : 0, elapsedTime.elapsedNs(), sendSize + mb.length());

    return conn;
}

SubscriptionId CClientSDSManager::subscribe(const char *xpath, ISDSSubscription &notify, bool sub, bool sendValue)
{
    CCycleTimer elapsedTime(recordingEvents());
    assertex(xpath);
    if (sub && sendValue)
        throw MakeSDSException(SDSExcpt_Unsupported, "Subscription to sub elements, with sendValue option unsupported");
    StringBuffer s;
    if ('/' != *xpath)
    {
        s.append('/').append(xpath);
        xpath = s.str();
    }
    CSDSSubscriberProxy *subscriber = new CSDSSubscriberProxy(xpath, sub, sendValue, notify);
    querySubscriptionManager(SDS_PUBLISHER)->add(subscriber, subscriber->getId());

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliSubscribe(xpath, subscriber->getId(), elapsedTime.elapsedNs());

    return subscriber->getId();
}


SubscriptionId CClientSDSManager::subscribeExact(const char *xpath, ISDSNodeSubscription &notify, bool sendValue)
{
    CCycleTimer elapsedTime(recordingEvents());
    if (queryDaliServerVersion().compare(SDS_SVER_MIN_NODESUBSCRIBE) < 0)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Requires dali server version >= " SDS_SVER_MIN_NODESUBSCRIBE " for subscribeExact");
    assertex(xpath);
    StringBuffer s;
    if ('/' != *xpath)
    {
        s.append('/').append(xpath);
        xpath = s.str();
    }
    CSDSNodeSubscriberProxy *subscriber = new CSDSNodeSubscriberProxy(xpath, sendValue, notify);
    querySubscriptionManager(SDSNODE_PUBLISHER)->add(subscriber, subscriber->getId());

    if (unlikely(recordingEvents()))
        queryRecorder().recordDaliSubscribe(xpath, subscriber->getId(), elapsedTime.elapsedNs());

    return subscriber->getId();
}

void CClientSDSManager::unsubscribe(SubscriptionId id)
{
    querySubscriptionManager(SDS_PUBLISHER)->remove(id);
}

void CClientSDSManager::unsubscribeExact(SubscriptionId id)
{
    if (queryDaliServerVersion().compare(SDS_SVER_MIN_NODESUBSCRIBE) < 0)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Requires dali server version >= " SDS_SVER_MIN_NODESUBSCRIBE " for unsubscribeExact");
    querySubscriptionManager(SDSNODE_PUBLISHER)->remove(id);
}

StringBuffer &CClientSDSManager::getInfo(SdsDiagCommand cmd, StringBuffer &out)
{
    CMessageBuffer mb;

    mb.append((int)DAMP_SDSCMD_DIAGNOSTIC);
    mb.append((int)cmd);

    if (!queryCoven().sendRecv(mb, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "querying sds diagnositc info");

    SdsReply replyMsg;  
    mb.read((int &)replyMsg);
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            switch (cmd)
            {
                case DIAG_CMD_STATS:
                    formatUsageStats(mb, out);
                    break;
                case DIAG_CMD_CONNECTIONS:
                    formatConnections(mb, out);
                    break;
                case DIAG_CMD_SUBSCRIBERS:
                    formatSubscribers(mb, out);
                    break;
            }
            break;
        }
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }

    return out;
}

ILockInfoCollection *CClientSDSManager::getLocks(const char *ipPattern, const char *xpathPattern)
{
    CMessageBuffer msg;
    msg.append((int)DAMP_SDSCMD_DIAGNOSTIC);
    msg.append((int)DIAG_CMD_LOCKINFO);
    msg.append(ipPattern?ipPattern:"");
    msg.append(xpathPattern?xpathPattern:"");

    if (!queryCoven().sendRecv(msg, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "getLocks");

    SdsReply replyMsg;
    msg.read((int &)replyMsg);
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", msg);
        default:
            assertex(false);
    }
    return deserializeLockInfoCollection(msg);
}

StringBuffer &CClientSDSManager::getUsageStats(StringBuffer &out)
{
    return getInfo(DIAG_CMD_STATS, out);
}

StringBuffer &CClientSDSManager::getConnections(StringBuffer &out)
{
    return getInfo(DIAG_CMD_CONNECTIONS, out);
}

StringBuffer &CClientSDSManager::getSubscribers(StringBuffer &out)
{
    return getInfo(DIAG_CMD_SUBSCRIBERS, out);
}

// TODO
StringBuffer &CClientSDSManager::getExternalReport(StringBuffer &out)
{
    return out;
}

IPropertyTree &CClientSDSManager::queryProperties() const
{
    if (properties) return *properties;
    CDaliVersion serverVersionNeeded("3.1");
    if (queryDaliServerVersion().compare(serverVersionNeeded) < 0)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Requires dali server version >= 3.1 for getProperties usage");
    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_GETPROPS);
    if (!queryCoven().sendRecv(mb, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "querying sds diagnostic info");
    SdsReply replyMsg;  
    mb.read((int &)replyMsg);
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }
    properties = createPTree(mb, ipt_lowmem);
    if (!properties->hasProp("Client"))
        properties->setPropTree("Client");
    return *properties;
}

IPropertyTree *CClientSDSManager::getXPaths(__int64 serverId, const char *xpath, bool getServerIds)
{
    CMessageBuffer mb;

    mb.append((int)(getServerIds?DAMP_SDSCMD_GETXPATHSPLUSIDS:DAMP_SDSCMD_GETXPATHS));
    mb.append(serverId);
    mb.append(xpath);

    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            return createPTree(mb, ipt_lowmem);
        case DAMP_SDSREPLY_EMPTY:
            return NULL;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
    }
    throwUnexpected();
}

IPropertyTreeIterator *CClientSDSManager::getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortBy, bool caseinsensitive, bool ascending, unsigned from, unsigned limit)
{
    CMessageBuffer mb;

    mb.append((int)DAMP_SDSCMD_GETXPATHSCRITERIA);
    mb.append(baseXPath);
    mb.append(matchXPath);
    mb.append(sortBy);
    mb.append(caseinsensitive);
    mb.append(ascending);
    mb.append(from);
    mb.append(limit);

    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            break;
        case DAMP_SDSREPLY_EMPTY:
            return createNullPTreeIterator();
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            throwUnexpected();
    }
    Owned<IPropertyTree> matchTree = createPTree(mb, ipt_lowmem);
    Owned<CRemoteConnection> conn = (CRemoteConnection *)connect(baseXPath, myProcessSession(), RTM_LOCK_READ, INFINITE);
    if (!conn)
        return createNullPTreeIterator();
    ensureLocal(*conn, (CRemoteTreeBase &)*conn->queryRoot(), matchTree);
    return new CXPathIterator(conn->queryRoot(), matchTree, iptiter_null);
}

void CClientSDSManager::getExternalValueFromServerId(__int64 serverId, MemoryBuffer &resMb)
{
    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_GETEXTVALUE);
    mb.append(serverId);
    if (!sendRequest(mb))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
            resMb.append(mb.length()-mb.getPos(), mb.toByteArray()+mb.getPos());
            return;
        case DAMP_SDSREPLY_EMPTY:
            return;
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
    }
    throwUnexpected();
}

IPropertyTreeIterator *CClientSDSManager::getElementsRaw(const char *xpath, INode *remotedali, unsigned timeout)
{
    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_GETELEMENTSRAW);
    mb.append(xpath);
    bool ok;
    if (remotedali) {
        Owned<IGroup> grp = createIGroup(1,&remotedali);
        Owned<ICommunicator> comm = createCommunicator(grp,false); 
        try {
            ok = comm->sendRecv(mb,RANK_RANDOM, MPTAG_DALI_SDS_REQUEST, timeout);
        }
        catch (IMP_Exception *e)
        {
            if (e->errorCode()!=MPERR_link_closed)
                throw;
            e->Release();
            throw createClientException(DCERR_server_closed);
        }
    }
    else
        ok = sendRequest(mb);
    if (!ok)
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer);

    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            Owned<DaliPTArrayIterator> resultIterator = new DaliPTArrayIterator;
            unsigned count, c;
            mb.read(count);
            for (c=0; c<count; c++)
            {
                Owned<IPropertyTree> item = createPTree(mb, ipt_lowmem);
                resultIterator->array.append(*LINK(item));
            }
            return LINK(resultIterator);
        }
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
    }
    return NULL;
}

void CClientSDSManager::setConfigOpt(const char *opt, const char *value)
{
    IPropertyTree &props = queryProperties();
    if (props.hasProp(opt) && (0 == strcmp(value, props.queryProp(opt))))
        return;
    ensurePTree(&queryProperties(), opt);
    queryProperties().setProp(opt, value);
    if (0 == strcmp("Client/Throttle/@limit", opt))
    {
        unsigned newV = props.getPropInt(opt);
        int diff = clientThrottleLimit-newV;
        if (diff)
        {
            if (diff>0) // new limit is lower than old
            {
                PROGLOG("Reducing concurrentThrottleLimit from %d to %d", clientThrottleLimit, newV);
                unsigned c=0;
                for (;;)
                {
                    // generally won't be waiting, as would expect this option to typically be called just after component startup time.
                    if (!concurrentRequests.wait(clientThrottleDelay))
                        IWARNLOG("Waiting on active requests to lower clientThrottleLimit");
                    else
                    {
                        ++c;
                        if (c == diff)
                            break;
                    }
                }
            }
            else
            {
                PROGLOG("Increasing clientThrottleLimit from %d to %d", clientThrottleLimit, newV);
                concurrentRequests.signal(-diff); // new limit is higher than old
            }
            clientThrottleLimit = newV;
        }
    }
}

#define MIN_QUERYCOUNT_SVER "3.8"
unsigned CClientSDSManager::queryCount(const char *xpath)
{
    CDaliVersion serverVersionNeeded(MIN_QUERYCOUNT_SVER);
    if (queryDaliServerVersion().compare(serverVersionNeeded) < 0)
        throw MakeSDSException(SDSExcpt_VersionMismatch, "Requires dali server version >= " MIN_QUERYCOUNT_SVER " for queryCount(<xpath>)");

    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_GETCOUNT);
    mb.append(xpath);

    if (!sendRequest(mb, true))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, ", queryCount(%s)", xpath);

    unsigned count=0;
    SdsReply replyMsg;    
    mb.read((int &)replyMsg);
    if (DAMP_SDSREPLY_OK == replyMsg)
        mb.read(count);
    else
    {
        assertex(replyMsg == DAMP_SDSREPLY_ERROR);
        throwMbException("SDS Reply Error ", mb);
    }
    return count;
}

#define MIN_UPDTENV_SVER "3.9"
bool CClientSDSManager::updateEnvironment(IPropertyTree *newEnv, bool forceGroupUpdate, StringBuffer &response)
{
    CDaliVersion serverVersionNeeded(MIN_QUERYCOUNT_SVER);
    if (queryDaliServerVersion().compare(serverVersionNeeded) < 0)
    {
        // have to do the old fashioned way, from client
        Owned<IRemoteConnection> conn = querySDS().connect("/",myProcessSession(),0, INFINITE);
        if (conn)
        {
            Owned<IPropertyTree> root = conn->getRoot();
            Owned<IPropertyTree> oldEnvironment = root->getPropTree("Environment");
            if (oldEnvironment.get())
            {
                StringBuffer bakname;
                Owned<IFileIO> io = createUniqueFile(NULL, "environment", "bak", bakname);
                Owned<IFileIOStream> fstream = createBufferedIOStream(io);
                toXML(oldEnvironment, *fstream);         // formatted (default)
                root->removeTree(oldEnvironment);
            }
            root->addPropTree("Environment", LINK(newEnv));
            root.clear();
            conn->commit();
            conn->close();
            StringBuffer messages;
            initClusterGroups(forceGroupUpdate, messages, oldEnvironment);
            if (messages.length())
                PROGLOG("CClientSDSManager::updateEnvironment: %s", messages.str());
            PROGLOG("Environment and node groups updated");
        }
        return true;
    }

    CMessageBuffer mb;
    mb.append((int)DAMP_SDSCMD_UPDTENV);
    newEnv->serialize(mb);
    mb.append(forceGroupUpdate);

    if (!queryCoven().sendRecv(mb, RANK_RANDOM, MPTAG_DALI_SDS_REQUEST))
        throw MakeSDSException(SDSExcpt_FailedToCommunicateWithServer, "querying sds diagnositc info");

    bool result = false;
    StringAttr resultStr;
    SdsReply replyMsg;
    mb.read((int &)replyMsg);
    switch (replyMsg)
    {
        case DAMP_SDSREPLY_OK:
        {
            mb.read(result);
            mb.read(resultStr);
            response.append(resultStr);
            break;
        }
        case DAMP_SDSREPLY_ERROR:
            throwMbException("SDS Reply Error ", mb);
        default:
            assertex(false);
    }
    return result;
}

//////////////

static bool releaseActiveManager = false;
static ISDSManager * activeSDSManager=NULL;
static ISDSManager * savedSDSManager=NULL;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    if (releaseActiveManager)
    {
        delete activeSDSManager;
        activeSDSManager = nullptr;
        delete savedSDSManager;
        savedSDSManager = nullptr;
    }
}

ISDSManager &querySDS()
{
    CriticalBlock block(SDScrit);
    if (activeSDSManager)
        return *activeSDSManager;
    else if (!queryCoven().inCoven())
    {
        releaseActiveManager = true;
        activeSDSManager = new CClientSDSManager();
        return *activeSDSManager;
    }
    else
    {
        activeSDSManager = &querySDSServer();
        return *activeSDSManager;
    }
}

void closeSDS()
{
    CriticalBlock block(SDScrit);

    //In roxie this is called when connection to dali is lost, but other threads can still be processing
    //CRemoteConnections (see HPCC-32410), which uses an ISDSManager member - accessing a stale manager.
    //There can be similar issues at closedown if threads have not been cleaned up properly.
    //Do not delete the active SDS manager immediately - save it so that it is deleted on the next call/closedown.
    ISDSManager * toDelete = savedSDSManager;
    savedSDSManager = activeSDSManager;
    activeSDSManager = nullptr;
    if (savedSDSManager || toDelete)
    {
        assertex(!queryCoven().inCoven()); // only called by client
        try
        {
            if (savedSDSManager)
                savedSDSManager->closedown();
            delete toDelete;
        }
        catch (IMP_Exception *e)
        {
            if (e->errorCode()!=MPERR_link_closed)
                throw;
            EXCLOG(e, "closeSDS");
            e->Release();
        }
        catch (IDaliClient_Exception *e)
        {
            if (e->errorCode()!=DCERR_server_closed)
                throw;
            e->Release();
        }
    }
}
