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

#ifndef DASDS_IPP
#define DASDS_IPP

#include "jarray.hpp"
#include "jhash.hpp"
#include "jiter.ipp"
#include "jstring.hpp"
#include "jptree.ipp"

#include <typeinfo>

#include "dasds.hpp"

#define EXTERNAL_NAME_PREFIX "dalisds_"
#define EXT_ATTR "@sds:ext"
#define EF_LegacyBinaryValue "bin"
#define EF_BinaryValue       "bv2"
#define EF_XML         "xml"

#define TBD UNIMPLEMENTED

#define RESERVED_CHANGE_NODE "T"
#define DELETE_TAG "D"
#define RENAME_TAG "R"
#define ATTRCHANGE_TAG "AC"
#define ATTRDELETE_TAG "AD"
#define APPEND_TAG "PA"

#define CLIENT_THROTTLE_LIMIT 10
#define CLIENT_THROTTLE_DELAY 1000

#if 1
#define DALI_CATCHALL ...
#else
    struct DaliDummyCatchAll{ int i; };
    #define DALI_CATCHALL DaliDummyCatchAll
#endif

#define WRITE_IDS 0
#define READ_IDS 1

typedef __int64 ConnectionId;
enum CPState { CPS_Unchanged=0, CPS_Changed=0x10000, CPS_New=0x10001, CPS_Deletions=0x20000, CPS_AttrDeletions=0x40000, CPS_Renames=0x80000, CPS_InsPos=0x100000, CPS_AttrChanges=0x200000, CPS_PropAppend=0x400000 };
enum PDState { PDS_None=0, PDS_Data=0x01, PDS_Structure=0x02, PDS_Added=(PDS_Structure+0x04), PDS_Deleted=(PDS_Structure+0x08), PDS_Renames=(PDS_Structure+0x10), PDS_New=0x20 };
#define mergePDState(this, other) (this = ((PDState) (((int)this) | ((int)other))))
enum SdsCommand { DAMP_SDSCMD_CONNECT, DAMP_SDSCMD_GET, DAMP_SDSCMD_GETCHILDREN, DAMP_SDSCMD_REVISIONS, DAMP_SDSCMD_DATA, DAMP_SDSCMD_DISCONNECT,
                  DAMP_SDSCMD_CONNECTSERVER, DAMP_SDSCMD_DATASERVER, DAMP_SDSCMD_DISCONNECTSERVER, DAMP_SDSCMD_CHANGEMODE, DAMP_SDSCMD_CHANGEMODESERVER,
                  DAMP_SDSCMD_EDITION, DAMP_SDSCMD_GETSTORE,
                  DAMP_SDSCMD_VERSION, DAMP_SDSCMD_DIAGNOSTIC, DAMP_SDSCMD_GETELEMENTS, DAMP_SDSCMD_MCONNECT, DAMP_SDSCMD_GETCHILDREN2, DAMP_SDSCMD_GET2, DAMP_SDSCMD_GETPROPS,
                  DAMP_SDSCMD_GETXPATHS, DAMP_SDSCMD_GETEXTVALUE, DAMP_SDSCMD_GETXPATHSPLUSIDS, DAMP_SDSCMD_GETXPATHSCRITERIA, DAMP_SDSCMD_GETELEMENTSRAW,
                  DAMP_SDSCMD_GETCOUNT,
                  DAMP_SDSCMD_UPDTENV,
                  DAMP_SDSCMD_MAX,
                  DAMP_SDSCMD_LAZYEXT=0x80000000
                };
enum SdsDiagCommand { DIAG_CMD_LOCKINFO, DIAG_CMD_STATS, DIAG_CMD_CONNECTIONS, DIAG_CMD_SUBSCRIBERS };

enum SdsReply { DAMP_SDSREPLY_OK, DAMP_SDSREPLY_EMPTY, DAMP_SDSREPLY_ERROR };

class CRemoteConnection;
class CRemoteTreeBase;
class CClientRemoteTree;
typedef ICopyArrayOf<CRemoteTreeBase> CRTArray;
interface ISDSConnectionManager
{
    virtual CRemoteTreeBase *get(CRemoteConnection &connection, __int64 serverId) = 0;
    virtual void getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels=1) = 0;
    virtual void getChildrenFor(CRTArray &fetchList, CRemoteConnection &connection, unsigned levels=1) = 0;
    virtual void ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags=iptiter_null) = 0;
    virtual IPropertyTreeIterator *getElements(CRemoteConnection &connection, const char *xpath) = 0;
    virtual void commit(CRemoteConnection &connection, bool *disconnectDeleteRoot) = 0;
    virtual void changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReloads) = 0;
    virtual IPropertyTree *getXPaths(__int64 serverId, const char *xpath, bool getServerIds=false) = 0;
    virtual IPropertyTreeIterator *getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit) = 0;
    virtual void getExternalValueFromServerId(__int64 serverId, MemoryBuffer &mb) = 0;
};

class ChangeInfo;
class CRemoteTreeBase;
interface ITrackChanges
{
    virtual ChangeInfo *queryChanges() = 0;
    virtual void registerRenamed(const char *newName, const char *oldName, unsigned pos, __int64 id) = 0;
    virtual void registerDeleted(const char *name, unsigned pos, __int64 id) = 0;
    virtual void registerDeletedAttr(const char *attr) = 0;
    virtual void clearChanges() = 0;
    virtual void registerAttrChange(const char *attr) = 0;
    virtual void registerPropAppend(size32_t l) = 0;
};

class ChangeInfo : public CInterfaceOf<IInterface>
{
    DECL_NAMEDCOUNT;
public:
    ChangeInfo(IPropertyTree &_owner) : owner(&_owner) { INIT_NAMEDCOUNT; tree.setown(createPTree(RESERVED_CHANGE_NODE)); }
    const IPropertyTree *queryOwner() const { return owner; }
    const void *queryFindParam() const { return &owner; }
public: // data
    Owned<IPropertyTree> tree;

private:
    const IPropertyTree *owner;
};

class ChangeInfoMap : public SuperHashTableOf<ChangeInfo, IPropertyTree *>
{
public:
    ~ChangeInfoMap() { kill(); }
    virtual void onAdd(void *et) { }
    virtual void onRemove(void *et) { ((ChangeInfo *)et)->Release(); }

    virtual unsigned getHashFromElement(const void *et) const
    {
        const ChangeInfo &elem = *(const ChangeInfo *) et;
        return hashc((const unsigned char *) elem.queryFindParam(), sizeof(IPropertyTree *), 0);
    }
    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *) fp, sizeof(IPropertyTree *), 0);
    }
    virtual const void *getFindParam(const void *et) const
    {
        const ChangeInfo &elem=*(const ChangeInfo *)et;
        return elem.queryFindParam();
    }
    virtual bool matchesFindParam(const void *et, const void *key, unsigned) const
    {
        return (*(const ChangeInfo *)et).queryOwner() == *((IPropertyTree **)key);
    }
};

interface IIteratorOperator
{
    virtual bool applyTop(IPropertyTree &tree) = 0;
    virtual bool applyChild(IPropertyTree &parent, IPropertyTree &child, bool &levelBreak) = 0;
};

///////////////////

class CIterationOperation
{
public:
    CIterationOperation(IIteratorOperator &_op) : op(_op)
    {
    }

    bool iterate(IPropertyTree &node)
    {
        bool res = true;
        if (op.applyTop(node))
        {
            IPropertyTreeIterator *iter = node.getElements("*");
            if (iter->first())
            {
                bool levelBreak = false;
                while (iter->isValid())
                {
                    IPropertyTree &child = iter->query();
                    if (op.applyChild(node, child, levelBreak))
                        iterate(child);
                    else if (levelBreak)
                        break;
                    iter->next();
                }
            }
            iter->Release();
        }
        return res;
    }

private: // data
    IIteratorOperator &op;
};

///////////////////

class CPTStack : public IArrayOf<PTree>
{
    bool _fill(IPropertyTree &root, const char *xpath, IPropertyTree &tail);
public:
    CPTStack() { };
    CPTStack(IPropertyTree &root, const char *xpath, IPropertyTree &tail) { fill(root, xpath, tail); }
    CPTStack(CPTStack &other) { ForEachItemIn(o, other) { PTree &t = other.item(o); t.Link(); append(t); } }
    StringBuffer &toString(StringBuffer &str)
    {
        str.append('/');
        if (ordinality()>1)
        {
            unsigned i = 1;
            loop
            {
                str.append(item(i).queryName());
                if (++i >= ordinality())
                    break;
                str.append('/');
            }
        }
        return str;
    }
    StringBuffer &getAbsolutePath(StringBuffer &str);
    bool fill(IPropertyTree &root, const char *xpath, IPropertyTree &tail);
};

enum STIFlags { STI_HaveChildren=1, STI_External=2 };

class CServerConnection;
class CBranchChange;
///////////////////
class CSubscriberContainerList;

class CRemoteTreeBase : public PTree
{
public:
    CRemoteTreeBase(MemoryBuffer &mb);
    CRemoteTreeBase(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL);

    void deserializeRT(MemoryBuffer &src);
    virtual void deserializeSelfRT(MemoryBuffer &src);
    virtual void deserializeChildrenRT(MemoryBuffer &src);
    virtual bool isOrphaned() const { return false; }

    void clearChildren();
    CRemoteTreeBase *createChild(int pos, const char *childName);
    
    inline __int64 queryServerId() { return serverId; }
    virtual void setServerId(__int64 _serverId);
    virtual CSubscriberContainerList *getSubscribers(const char *xpath, CPTStack &stack) { UNIMPLEMENTED; return NULL; } // JCSMORE

// PTree
    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, CRemoteTreeBase)); }
    virtual IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false) = 0;
    virtual IPropertyTree *create(MemoryBuffer &mb) = 0;

// ITrackChanges
    virtual ChangeInfo *queryChanges() { assertex(false); return NULL; }
    virtual void registerRenamed(const char *newName, const char *oldName, unsigned pos, __int64 id) { }
    virtual void registerDeleted(const char *name, unsigned pos, __int64 id) { }
    virtual void registerDeletedAttr(const char *attr) { }
    virtual void clearChanges() { assertex(false); }
    virtual void registerAttrChange(const char *attr) { }
    virtual void registerPropAppend(size32_t l) { }

protected: // data
    __int64 serverId;
};

class CTrackChanges
{
public:
    inline ChangeInfo *queryChangeInfo(IPropertyTree &owner)
    {
        IPropertyTree *_owner = &owner;
        return changeMap.find(&_owner);
    }
    inline ChangeInfo *queryCreateChangeInfo(IPropertyTree &owner)
    {
        IPropertyTree *_owner = &owner;
        ChangeInfo *changes = changeMap.find(&_owner);
        if (!changes)
        {
            changes = new ChangeInfo(owner);
            changeMap.replace(*changes);
        }
        return changes;
    }
    void registerRenamed(IPropertyTree &owner, const char *newName, const char *oldName, unsigned pos, __int64 id)
    {
        ChangeInfo *changes = queryCreateChangeInfo(owner);
        IPropertyTree *t = createPTree();
        t->setProp("@from", oldName);
        t->setProp("@to", newName);
        t->setPropInt64("@id", id);
#ifdef SIBLING_MOVEMENT_CHECK
        t->setProp("@pos", pos);
#endif
        changes->tree->addPropTree(RENAME_TAG, t);
    }

    void registerDeleted(IPropertyTree &owner, const char *name, unsigned pos, __int64 id)
    {
        ChangeInfo *changes = queryCreateChangeInfo(owner);
        IPropertyTree *t = createPTree();
        t->setProp("@name", name);
        t->setPropInt64("@id", id);
#ifdef SIBLING_MOVEMENT_CHECK
        t->setPropInt("@pos", pos+1);
#endif
        changes->tree->addPropTree(DELETE_TAG, t);
    }

    virtual void registerAttrChange(IPropertyTree &owner, const char *attr)
    {
        ChangeInfo *changes = queryCreateChangeInfo(owner);
        IPropertyTree *t = changes->tree->queryPropTree("AD");
        if (t) t->removeProp(attr);
        t = changes->tree->queryPropTree("AC");
        if (!t)
            t = changes->tree->addPropTree("AC", createPTree());
        t->setProp(attr, "");
    }

    void registerDeletedAttr(IPropertyTree &owner, const char *attr)
    {
        ChangeInfo *changes = queryCreateChangeInfo(owner);
        IPropertyTree *t = changes->tree->queryPropTree("AC");
        if (t) t->removeProp(attr);
        t = changes->tree->queryPropTree("AD");
        if (!t)
            t = changes->tree->addPropTree("AD", createPTree());
        t->addProp(attr, "");
    }

    void registerPropAppend(IPropertyTree &owner, size32_t l)
    {
        ChangeInfo *changes = queryCreateChangeInfo(owner);
        IPropertyTree *t = changes->tree->queryPropTree(APPEND_TAG);
        if (!t)
            t = changes->tree->setPropTree(APPEND_TAG, createPTree());
        t->setPropInt(NULL, l);
    }

    void clearChanges()
    {
        changeMap.kill();
    }

    void clearChanges(IPropertyTree &owner)
    {
        IPropertyTree *_owner = &owner;
        changeMap.remove(&_owner);
    }


protected:
    ChangeInfoMap changeMap;
};

class CSDSException : public CInterface, implements ISDSException
{
public:
    IMPLEMENT_IINTERFACE;

    CSDSException(int _errCode) : errCode(_errCode)
    {
    }
    CSDSException(int _errCode, const char *_errMsg, va_list &args) __attribute__((format(printf,3,0))) : errCode(_errCode)
    {
        if (_errMsg)
            errMsg.valist_appendf(_errMsg, args);
    }

    StringBuffer &translateCode(StringBuffer &out) const
    {
        out.append("SDS: ");
        switch (errCode)
        {
            case SDSExcpt_InappropriateXpath:
                return out.append("XPath invalid for this context");
            case SDSExcpt_LockTimeout:
                return out.append("Lock timeout");
            case SDSExcpt_UnknownConnection:
                return out.append("Non existent connection id");
            case SDSExcpt_DistributingTransaction:
                return out.append("Error while distributing transaction");
            case SDSExcpt_Reload:
                return out.append("Failed to reload");
            case SDSExcpt_StoreMismatch:
                return out.append("Initial data stores do not match each other on different coven servers");
            case SDSExcpt_RequestingStore:
                return out.append("Error while requesting data store from other coven servers");
            case SDSExcpt_BadMode:
                return out.append("Invalid lock mode used");
            case SDSExcpt_LoadInconsistency:
                return out.append("Inconsistency detected while loading store");
            case SDSExcpt_RenameFailure:
                return out.append("Rename failure");
            case SDSExcpt_UnknownTreeId:
                return out.append("Unknown tree id (possible if client had unlocked connection and another deleted this tree node)");
            case SDSExcpt_AbortDuringConnection:
                return out.append("Connection aborted during connect ");
            case SDSExcpt_InvalidVersionSyntax:
                return out.append("Invalid versioning syntax sent from client ");
            case SDSExcpt_VersionMismatch:
                return out.append("Client/Server version mismatch ");
            case SDSExcpt_AmbiguousXpath:
                return out.append("Invalid ambiguous xpath detected ");
            case SDSExcpt_OpenStoreFailed:
                return out.append("Failed to open sds xml store file ");
            case SDSExcpt_OrphanedNode:
                return out.append("Transaction to orphaned server node ");
            case SDSExcpt_ServerStoppedLockAborted:
                return out.append("Lock aborted due to server stopping ");
            case SDSExcpt_ConnectionAbsent:
                return out.append("Connection missing (aborted)");
            case SDSExcpt_OpeningExternalFile:
                return out.append("Failed to open external reference file ");
            case SDSExcpt_FailedToCommunicateWithServer:
                return out.append("Failed to communicate to coven server ");
            case SDSExcpt_MissingExternalFile:
                return out.append("Failed to locate external file: ");
            case SDSExcpt_FileCreateFailure:
                return out.append("Failed to create file for new external store: ");
            case SDSExcpt_UnrecognisedCommand:
                return out.append("Unrecognised SDS command: ");
            case SDSExcpt_LoadAborted:
                return out.append("Store load aborted ");
            case SDSExcpt_IPTError:
                return out.append("IPropertyTree exception ");
            case SDSExcpt_StoreInfoMissing:
                return out.append("Store info file not found");
            case SDSExcpt_ClientCacheDirty:
                return out.append("Dirty client cache members used");
            case SDSExcpt_LockHeld:
                return out.append("Lock held");
            case SDSExcpt_SubscriptionParseError:
                return out.append("Subscription parse error");
            default:
                return out.append("INTERNAL ERROR");
        }
    }

// IException
    int errorCode() const { return errCode; }
    StringBuffer &errorMessage(StringBuffer &out) const
    {
        return translateCode(out).append("\n").append(errMsg.str());
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }

private:
    int errCode;
    StringBuffer errMsg;
};

ISDSException *MakeSDSException(int errorCode, const char *errorMsg, ...) __attribute__((format(printf, 2, 3)));
ISDSException *MakeSDSException(int errorCode);

inline void throwMbException(const char *errorMsg, MemoryBuffer &mb)
{
    int code;
    StringAttr exptTxt;
    mb.read(code);
    mb.read(exptTxt);
    throw MakeSDSException(code, "%s : %s", errorMsg, exptTxt.get());
}

//////////////

class CDisableFetchChangeBlock
{
    CRemoteConnection &connection;
    bool lazyFetch, stateChanges;
public:
    CDisableFetchChangeBlock(CRemoteConnection &_connection);
    ~CDisableFetchChangeBlock();
};

class CDisableLazyFetchBlock
{
    CRemoteConnection &connection;
    bool lazyFetch;
public:
    CDisableLazyFetchBlock(CRemoteConnection &_connection);
    ~CDisableLazyFetchBlock();
};

///////////////

class CConnectionBase : public CInterface
{
public:
    CConnectionBase(ISDSConnectionManager &_manager, ConnectionId _connectionId, const char *_xpath, SessionId _sessionId, unsigned _mode, unsigned _timeout)
        : manager(_manager), connectionId(_connectionId), xpath(_xpath), sessionId(_sessionId), mode(_mode), timeout(_timeout)
    {
    }

    inline void setRoot(CRemoteTreeBase *_root) { root.setown(_root); }
    inline ConnectionId queryConnectionId() const { return connectionId; }
    inline const char *queryXPath() const { return xpath; }
    inline unsigned queryTimeout() const { return timeout; }
    inline void setMode(unsigned _mode) { mode = _mode; }
    virtual IPropertyTree *getRoot() { return LINK(root); }
    virtual IPropertyTree *queryRoot() { return root; }
    virtual SessionId querySessionId() const { return sessionId; }
    virtual unsigned queryMode() const { return mode; }

    virtual const void *queryFindParam() const
    {
        return (const void *) &connectionId;
    }

protected:
    ISDSConnectionManager &manager;

    Owned<CRemoteTreeBase> root;
    ConnectionId connectionId;
    StringAttr xpath;
    SessionId sessionId;
    unsigned mode;
    unsigned timeout;
};

//////////////

typedef ThreadSafeOwningSimpleHashTableOf<CConnectionBase, ConnectionId> CConnectionHashTable;

class CSDSManagerBase : public CInterface, implements ISDSConnectionManager
{
public:
    IMPLEMENT_IINTERFACE;

// ISDSConnectionManager
    virtual CRemoteTreeBase *get(CRemoteConnection &connection, __int64 serverId) = 0;
    virtual void getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels) = 0;
    virtual void getChildrenFor(CRTArray &fetchList, CRemoteConnection &connection, unsigned levels) = 0;
    virtual void ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags=iptiter_null) = 0;
    virtual IPropertyTreeIterator *getElements(CRemoteConnection &connection, const char *xpath) = 0;
    virtual void commit(CRemoteConnection &connection, bool *disconnectDeleteRoot) = 0;
    virtual void changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReloads) = 0;
    virtual IPropertyTree *getXPaths(__int64 serverId, const char *xpath, bool getServerIds=false) = 0;
    virtual IPropertyTreeIterator *getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit) = 0;
    virtual void getExternalValueFromServerId(__int64 serverId, MemoryBuffer &mb) = 0;

protected:
    CConnectionHashTable connections;
};

class CPTArrayIterator : public CArrayIteratorOf<IPropertyTree, IPropertyTreeIterator>
{
    DECL_NAMEDCOUNT;
public:
    CPTArrayIterator() : CArrayIteratorOf<IPropertyTree, IPropertyTreeIterator>(array) { INIT_NAMEDCOUNT; }
    IArrayOf<IPropertyTree> array;
};

class CRemoteConnections : public CInterface, implements IRemoteConnections
{
    IArrayOf<IRemoteConnection> connections;
public:
    IMPLEMENT_IINTERFACE;

    void add(IRemoteConnection *connection) { connections.append(*connection); }
    void detachConnections()
    {
        // clear connections, do not release
        connections.popAll(true);
    }

// IRemoteConnections
    virtual IRemoteConnection *queryConnection(unsigned which)
    {
        return &connections.item(which);
    }
    virtual unsigned queryConnections() { return connections.ordinality(); }
};

class CXPathIterator : public CInterfaceOf<IPropertyTreeIterator>
{
    DECL_NAMEDCOUNT;
    IPropertyTree *root;
    IPropertyTree *matchTree;
    IArrayOf<IPropertyTreeIterator> stack;
    ICopyArrayOf<IPropertyTree> iterParents;
    UnsignedArray childPositions;
    IPropertyTree *currentChild;
    IPTIteratorCodes flags;
    bool validateServerIds;
public:
    CXPathIterator(IPropertyTree *_root, IPropertyTree *_matchTree, IPTIteratorCodes _flags) : root(_root), matchTree(_matchTree), flags(_flags)
    {
        INIT_NAMEDCOUNT;
        matchTree->Link();
        validateServerIds = matchTree->hasProp("@serverId");
        currentChild = NULL;
        root->Link();
    }
    ~CXPathIterator()
    {
        root->Release();
        matchTree->Release();
    }
    
    StringBuffer &getCurrentPath(StringBuffer &out)
    {
        if (!currentChild) return out;
        unsigned p=0;
        for (;p<iterParents.ordinality(); p++)
        {
            IPropertyTree &parent = iterParents.item(p);
            out.append(parent.queryName());
            if (p>0)
                out.append('[').append(childPositions.item(p-1)).append(']');
        }
        out.append(currentChild->queryName());
        return out.append('[').append(childPositions.tos()).append(']');
    }

    virtual IPropertyTree *queryChild(IPropertyTree *parent, const char *path)
    {
        return parent->queryPropTree(path);
    }

    IPropertyTree *setNext(IPropertyTree *parent, IPropertyTree *storeParent)
    {
        if (!parent->hasChildren())
            return storeParent;
        Owned<IPropertyTreeIterator> iter = parent->getElements("*", flags);
        ForEach (*iter)
        {
            IPropertyTree &child = iter->query();
            StringBuffer childPath;
            unsigned pos = child.getPropInt("@pos");
            childPath.append(child.queryName()).append('[').append(pos).append(']');
            IPropertyTree *storeChild = queryChild(storeParent, childPath.str());
            if (storeChild)
            {
                stack.append(*LINK(iter));
                iterParents.append(*storeParent);
                childPositions.append(pos);
                IPropertyTree *match = setNext(&child, storeChild);
                if (match)
                    return match;
                childPositions.pop();
                iterParents.pop();
                stack.pop();
            }
            // else - implies tree no longer matches state of server
        }
        return NULL;
    }

    IPropertyTree *getNext()
    {
        if (!currentChild) return NULL;
        while (stack.ordinality())
        {
            IPropertyTreeIterator &iter = stack.tos();
            if (iter.next())
            {
                IPropertyTree &child = iter.query();
                IPropertyTree &storeParent = iterParents.tos();
                StringBuffer childPath;
                unsigned pos = child.getPropInt("@pos");
                childPath.append(child.queryName()).append('[').append(pos).append(']');
                IPropertyTree *storeChild = queryChild(&storeParent, childPath.str());
                if (storeChild)
                {
                    if (validateServerIds && ((CRemoteTreeBase *)storeChild)->queryServerId() != child.getPropInt64("@serverId"))
                        throwUnexpected();

                    IPropertyTree *match = setNext(&child, storeChild);
                    if (match)
                        return match;
                }
                // else - implies tree no longer matches state of server
            }
            childPositions.pop();
            iterParents.pop();
            stack.pop();
        }
        return NULL;
    }

// IPropertyTreeIterator impl.
    virtual bool first()
    {
        stack.kill();
        iterParents.kill();
        childPositions.kill();
        currentChild = setNext(matchTree, root);
        return NULL != currentChild;
    }
    virtual bool next()
    {
        currentChild = getNext();
        return NULL != currentChild;
    }
    virtual bool isValid()
    {
        return NULL != currentChild;
    }
    virtual IPropertyTree & query()
    {
        return *currentChild;
    }
};

IMultipleConnector *deserializeIMultipleConnector(MemoryBuffer &src);
StringBuffer &getMConnectString(IMultipleConnector *mConnect, StringBuffer &s);

extern da_decl StringBuffer &formatUsageStats(MemoryBuffer &src, StringBuffer &out);
extern da_decl StringBuffer &formatConnectionInfo(MemoryBuffer &src, StringBuffer &out);
extern da_decl StringBuffer &formatConnections(MemoryBuffer &src, StringBuffer &out);
extern da_decl StringBuffer &formatSubscriberInfo(MemoryBuffer &src, StringBuffer &out);
extern da_decl StringBuffer &formatSubscribers(MemoryBuffer &src, StringBuffer &out);

#endif
