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

#define da_decl DECL_EXPORT
#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"
#include "jencrypt.hpp"
#include "dacoven.hpp"
#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "daserver.hpp"
#include "dasubs.ipp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "daldap.hpp"
#include "seclib.hpp"
#include "dasess.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

const char *queryRoleName(DaliClientRole role)
{
    switch (role) {
    case DCR_Private: return "Private";
    case DCR_Diagnostic: return "Diagnostic";
    case DCR_ThorSlave: return "ThorSlave";
    case DCR_ThorMaster: return "ThorMaster";
    case DCR_EclCCServer: return "EclCCServer";
    case DCR_EclCC: return "eclcc";
    case DCR_EclServer: return "EclServer";
    case DCR_EclScheduler: return "EclScheduler";
    case DCR_EclAgent: return "EclAgent";
    case DCR_AgentExec: return "AgentExec";
    case DCR_DaliServer:return "DaliServer";
    case DCR_SashaServer: return "SashaServer";
    case DCR_Util: return "Util";
    case DCR_Dfu: return "Dfu";
    case DCR_DfuServer: return "DfuServer";
    case DCR_EspServer: return "EspServer";
    case DCR_WuClient: return "WuClient";
    case DCR_Config: return "Config";
    case DCR_Scheduler: return "Scheduler";
    case DCR_RoxyMaster: return "RoxieMaster";
    case DCR_RoxySlave: return "RoxieSlave";
    case DCR_BackupGen: return "BackupGen";
    case DCR_Other: return "Other";
    }
    return "Unknown";
}



interface ISessionManagerServer: implements IConnectionMonitor
{
    virtual SessionId registerSession(SecurityToken tok,SessionId parentid) = 0;
    virtual SessionId registerClientProcess(INode *node,IGroup *&grp, DaliClientRole role) = 0;
    virtual void addProcessSession(SessionId id, INode *node, DaliClientRole role) = 0;
    virtual void addSession(SessionId id) = 0;
    virtual SessionId lookupProcessSession(INode *node) = 0;
    virtual INode *getProcessSessionNode(SessionId id) =0;
    virtual SecAccessFlags getPermissionsLDAP(const char *key,const char *obj,IUserDescriptor *udesc,unsigned flags, int *err)=0;
    virtual bool clearPermissionsCache(IUserDescriptor *udesc) = 0;
    virtual void stopSession(SessionId sessid,bool failed) = 0;
    virtual void setClientAuth(IDaliClientAuthConnection *authconn) = 0;
    virtual void setLDAPconnection(IDaliLdapConnection *_ldapconn) = 0;
    virtual bool authorizeConnection(int role,bool revoke) = 0;
    virtual void start() = 0;
    virtual void ready() = 0;
    virtual void stop() = 0;
    virtual bool queryScopeScansEnabled(IUserDescriptor *udesc, int * err, StringBuffer &retMsg) = 0;
    virtual bool enableScopeScans(IUserDescriptor *udesc, bool enable, int * err, StringBuffer &retMsg) = 0;
};


static SessionId mySessionId=0;
static ISessionManager *SessionManager=NULL;
static ISessionManagerServer *SessionManagerServer=NULL;
static CriticalSection sessionCrit;

#define SESSIONREPLYTIMEOUT (3*60*1000)


#define CLDAPE_getpermtimeout (-1)
#define CLDAPE_ldapfailure    (-2)

class DECL_EXCEPTION CDaliLDAP_Exception: implements IException, public CInterface
{
    int errcode;
public:


    CDaliLDAP_Exception(int _errcode)
    {
        errcode = _errcode;
    }

    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        if (errcode==0)
            return str;
        str.appendf("LDAP Exception(%d): ",errcode);
        if (errcode==CLDAPE_getpermtimeout)
            return str.append("getPermissionsLDAP - timeout to LDAP server"); 
        if (errcode==CLDAPE_ldapfailure)
            return str.append("getPermissionsLDAP - LDAP server failure");
        return str.append("Unknown Exception"); 
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }

    IMPLEMENT_IINTERFACE;
};



class CdelayedTerminate: public Thread // slightly obfuscated stop code
{
    byte err;

    int run()
    {
        while (getRandom()%711!=0) getRandom(); // look busy
        ERRLOG("Server fault %d",(int)err);
        while (getRandom()%7!=0) Sleep(1);
        exit(0);
    }

public:
    CdelayedTerminate(byte _err) 
    {
        err = _err;
        start();
        Release();
        Sleep(100);
    }
};


class CSessionState: public CInterface
{
protected: friend class CSessionStateTable;
    SessionId id;
public:

    CSessionState(SessionId _id)
    {
        id = _id;
    }

    SessionId getId() const
    {
        return id;
    }

};

class CSessionStateTable: private SuperHashTableOf<CSessionState,SessionId>
{
    CheckedCriticalSection sessstatesect;

    void onAdd(void *) {}

    void onRemove(void *e)
    {
        CSessionState &elem=*(CSessionState *)e;        
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CSessionState &elem=*(const CSessionState *)e;        
        SessionId id=elem.id;
        return low(id)^(unsigned)high(id);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        SessionId id = *(const SessionId *)fp;
        return low(id)^(unsigned)high(id);
    }

    const void * getFindParam(const void *p) const
    {
        const CSessionState &elem=*(const CSessionState *)p;        
        return (void *)&elem.id;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CSessionState *)et)->id==*(SessionId *)fp;
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CSessionState,SessionId);

public: 
    CSessionStateTable() 
    {
    }
    ~CSessionStateTable() { 
        CHECKEDCRITICALBLOCK(sessstatesect,60000);
        _releaseAll();
    }
    bool add(CSessionState *e) // takes ownership
    {
        CHECKEDCRITICALBLOCK(sessstatesect,60000);
        if (SuperHashTableOf<CSessionState,SessionId>::find(&e->id))
            return false;
        SuperHashTableOf<CSessionState,SessionId>::add(*e);
        return true;
    }

    CSessionState *query(SessionId id)
    {
        CHECKEDCRITICALBLOCK(sessstatesect,60000);
        return SuperHashTableOf<CSessionState,SessionId>::find(&id);
    }
    
    void remove(SessionId id)
    {
        CHECKEDCRITICALBLOCK(sessstatesect,60000);
        SuperHashTableOf<CSessionState,SessionId>::remove(&id);
    }

};

class CProcessSessionState: public CSessionState
{
    INode *node;
    DaliClientRole role;
    UInt64Array previousSessionIds;
public:
    CProcessSessionState(SessionId id,INode *_node,DaliClientRole _role)
        : CSessionState(id)
    {
        node = _node;
        node->Link();
        role = _role;
    }
    ~CProcessSessionState()
    {
        node->Release();
    }
    INode &queryNode() const
    {
        return *node;
    }
    DaliClientRole queryRole() const
    {
        return role;
    }
    StringBuffer &getDetails(StringBuffer &buf)
    {
        StringBuffer ep;
        return buf.appendf("%16" I64F "X: %s, role=%s",CSessionState::id,node->endpoint().getUrlStr(ep).str(),queryRoleName(role));
    }
    void addSessionIds(CProcessSessionState &other, bool prevOnly)
    {
        for (;;)
        {
            SessionId id = other.dequeuePreviousSessionId();
            if (!id)
                break;
            previousSessionIds.append(id);
        }
        if (!prevOnly)
            previousSessionIds.append(other.getId());
    }
    SessionId dequeuePreviousSessionId()
    {
        if (!previousSessionIds.ordinality())
            return 0;
        return previousSessionIds.popGet();
    }
    unsigned previousSessionIdCount() const
    {
        return previousSessionIds.ordinality();
    }
    void removeOldSessionId(SessionId id)
    {
        if (previousSessionIds.zap(id))
            PROGLOG("Removed old sessionId (%" I64F "x) from current process state", id);
    }
};

class CMapProcessToSession: private SuperHashTableOf<CProcessSessionState,INode>
{
    CheckedCriticalSection mapprocesssect;


    void onAdd(void *) {}
    void onRemove(void *e) {} // do nothing

    unsigned getHashFromElement(const void *e) const
    {
        const CProcessSessionState &elem=*(const CProcessSessionState *)e;      
        return elem.queryNode().getHash();
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((INode *)fp)->getHash();
    }

    const void * getFindParam(const void *p) const
    {
        const CProcessSessionState &elem=*(const CProcessSessionState *)p;      
        return (void *)&elem.queryNode();
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CProcessSessionState *)et)->queryNode().equals((INode *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CProcessSessionState,INode);

public: 
    CMapProcessToSession() 
    {
    }

    ~CMapProcessToSession()
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        _releaseAll();
    }

    bool add(CProcessSessionState *e) 
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        if (SuperHashTableOf<CProcessSessionState,INode>::find(&e->queryNode()))
            return false;
        SuperHashTableOf<CProcessSessionState,INode>::add(*e);
        return true;
    }

    void replace(CProcessSessionState *e)
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        SuperHashTableOf<CProcessSessionState,INode>::replace(*e);
    }

    CProcessSessionState *query(INode *n) 
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        return SuperHashTableOf<CProcessSessionState,INode>::find(n);
    }
    
    bool remove(const CProcessSessionState *state, ISessionManagerServer *manager)
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        if (SuperHashTableOf<CProcessSessionState,INode>::removeExact((CProcessSessionState *)state))
        {
            if (manager)
                manager->authorizeConnection(state->queryRole(), true);
            return true;
        }
        return false;
    }

    unsigned count()
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        return SuperHashTableOf<CProcessSessionState,INode>::count();
    }

    CProcessSessionState *next(const CProcessSessionState *s)
    {
        CHECKEDCRITICALBLOCK(mapprocesssect,60000);
        return (CProcessSessionState *)SuperHashTableOf<CProcessSessionState,INode>::next(s);
    }
};



enum MSessionRequestKind { 
    MSR_REGISTER_PROCESS_SESSION, 
    MSR_SECONDARY_REGISTER_PROCESS_SESSION, 
    MSR_REGISTER_SESSION, 
    MSR_SECONDARY_REGISTER_SESSION, 
    MSR_LOOKUP_PROCESS_SESSION, 
    MSR_STOP_SESSION,
    MSR_IMPORT_CAPABILITIES,
    MSR_LOOKUP_LDAP_PERMISSIONS,
    MSR_CLEAR_PERMISSIONS_CACHE,
    MSR_EXIT, // TBD
    MSR_QUERY_SCOPE_SCANS_ENABLED,
    MSR_ENABLE_SCOPE_SCANS
};

class CQueryScopeScansEnabledReq : implements IMessageWrapper
{
public:
    bool enabled;
    Linked<IUserDescriptor> udesc;

    CQueryScopeScansEnabledReq(IUserDescriptor *_udesc) : udesc(_udesc) {}
    CQueryScopeScansEnabledReq() {}

    void serializeReq(CMessageBuffer &mb)
    {
        mb.append(MSR_QUERY_SCOPE_SCANS_ENABLED);
        udesc->serialize(mb);
    }

    void deserializeReq(CMessageBuffer &mb)
    {
        udesc.setown(createUserDescriptor(mb));
    }
};

class CEnableScopeScansReq : implements IMessageWrapper
{
public:
    bool doEnable;
    Linked<IUserDescriptor> udesc;

    CEnableScopeScansReq(IUserDescriptor *_udesc, bool _doEnable) : udesc(_udesc), doEnable(_doEnable) {}
    CEnableScopeScansReq() {}

    void serializeReq(CMessageBuffer &mb)
    {
        mb.append(MSR_ENABLE_SCOPE_SCANS);
        udesc->serialize(mb);
        mb.append(doEnable);
    }

    void deserializeReq(CMessageBuffer &mb)
    {
        udesc.setown(createUserDescriptor(mb));
        mb.read(doEnable);
    }
};

class CSessionRequestServer: public Thread
{
    bool stopped;
    ISessionManagerServer &manager;
    Semaphore acceptConnections;

public:
    CSessionRequestServer(ISessionManagerServer &_manager) 
        : Thread("Session Manager, CSessionRequestServer"), manager(_manager)
    {
        stopped = true;
    }

    int run()
    {
        ICoven &coven=queryCoven();

        CMessageHandler<CSessionRequestServer> handler("CSessionRequestServer",this,&CSessionRequestServer::processMessage);
        stopped = false;
        CMessageBuffer mb;
        while (!stopped) {
            try {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_SESSION_REQUEST,NULL))
                    handler.handleMessage(mb);
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CDaliPublisherServer");
                e->Release();
            }
        }
        return 0;
    }

    void processMessage(CMessageBuffer &mb)
    {
        ICoven &coven=queryCoven();
        SessionId id;
        int fn;
        mb.read(fn);
        switch (fn) {
        case MSR_REGISTER_PROCESS_SESSION: {
                acceptConnections.wait();
                acceptConnections.signal();
                Owned<INode> node(deserializeINode(mb));
                Owned<INode> servernode(deserializeINode(mb));  // hopefully me, but not if forwarded
                int role=0;
                if (mb.length()-mb.getPos()>=sizeof(role)) { // a capability block present
                    mb.read(role);
                    if (!manager.authorizeConnection(role,false)) {
                        SocketEndpoint sender = mb.getSender();
                        mb.clear();
                        coven.reply(mb);
                        MilliSleep(100+getRandom()%1000); // Causes client to 'work' for a short time.
                        Owned<INode> node = createINode(sender);
                        coven.disconnect(node);
                        break;
                    }
#ifdef _DEBUG
                    StringBuffer eps;
                    PROGLOG("Connection to %s at %s authorized",queryRoleName((DaliClientRole)role),mb.getSender().getUrlStr(eps).str());
#endif
                }
                
                IGroup *covengrp;
                id = manager.registerClientProcess(node.get(),covengrp,(DaliClientRole)role);
                mb.clear().append(id);
                if (covengrp->rank(servernode)==RANK_NULL) { // must have been redirected
                    covengrp->Release(); // no good, so just use one we know about (may use something more sophisticated later)
                    INode *na = servernode.get();
                    covengrp = createIGroup(1, &na);
                }
                covengrp->serialize(mb);
                covengrp->Release();
                coven.reply(mb);
            }
            break;
        case MSR_SECONDARY_REGISTER_PROCESS_SESSION: {
                mb.read(id);
                Owned<INode> node (deserializeINode(mb));
                int role;
                mb.read(role);
                manager.addProcessSession(id,node.get(),(DaliClientRole)role);
                mb.clear();
                coven.reply(mb);
            }
            break;
        case MSR_REGISTER_SESSION: {
                SecurityToken tok;
                SessionId parentid;
                mb.read(tok).read(parentid);
                SessionId id = manager.registerSession(tok,parentid);
                mb.clear().append(id);
                coven.reply(mb);
            }
            break;
        case MSR_SECONDARY_REGISTER_SESSION: {
                mb.read(id);
                manager.addSession(id);
                mb.clear();
                coven.reply(mb);
            }
            break;
        case MSR_LOOKUP_PROCESS_SESSION: {
                // looks up from node or from id
                Owned<INode> node (deserializeINode(mb));
                if (node->endpoint().isNull()&&(mb.length()-mb.getPos()>=sizeof(id))) {
                    mb.read(id);
                    INode *n = manager.getProcessSessionNode(id);
                    if (n)
                        node.setown(n);
                    node->serialize(mb.clear());
                }
                else {
                    id = manager.lookupProcessSession(node.get());
                    mb.clear().append(id);
                }
                coven.reply(mb);
            }
            break;
        case MSR_STOP_SESSION: {
                SessionId sessid;
                bool failed;
                mb.read(sessid).read(failed);
                manager.stopSession(sessid,failed);
                mb.clear();
                coven.reply(mb);
            }
            break;
        case MSR_LOOKUP_LDAP_PERMISSIONS: {
                StringAttr key;
                StringAttr obj;
                Owned<IUserDescriptor> udesc=createUserDescriptor();
                mb.read(key).read(obj);
                udesc->deserialize(mb);
#ifdef NULL_DALIUSER_STACKTRACE
                //following debug code to be removed
                StringBuffer sb;
                udesc->getUserName(sb);
                if (0==sb.length())
                {
                    DBGLOG("UNEXPECTED USER (NULL) in dasess.cpp CSessionRequestServer::processMessage() line %d", __LINE__);
                }
#endif
                unsigned auditflags = 0;
                if (mb.length()-mb.getPos()>=sizeof(auditflags))
                    mb.read(auditflags);
                udesc->deserializeExtra(mb);//deserialize sessionToken and user signature if present (hpcc ver 7.0.0 and newer)
                int err = 0;
                SecAccessFlags perms = manager.getPermissionsLDAP(key,obj,udesc,auditflags,&err);
                mb.clear().append((int)perms);
                if (err)
                    mb.append(err);
                coven.reply(mb);
            }
            break;
        case MSR_CLEAR_PERMISSIONS_CACHE: {
                Owned<IUserDescriptor> udesc=createUserDescriptor();
                udesc->deserialize(mb);
                bool ok = manager.clearPermissionsCache(udesc);
                mb.append(ok);
                coven.reply(mb);
            }
            break;
        case MSR_QUERY_SCOPE_SCANS_ENABLED:{
                CQueryScopeScansEnabledReq req;
                req.deserializeReq(mb);
                int err;
                StringBuffer retMsg;
                bool enabled = manager.queryScopeScansEnabled(req.udesc, &err, retMsg);
                mb.clear().append(err);
                mb.append(enabled);
                mb.append(retMsg.str());
                if (err != 0 || retMsg.length())
                {
                    StringBuffer user;
                    req.udesc->getUserName(user);
                    DBGLOG("Error %d querying scope scan status for %s : %s", err, user.str(), retMsg.str());
                }
                coven.reply(mb);
            }
            break;
        case MSR_ENABLE_SCOPE_SCANS:{
                CEnableScopeScansReq req;
                req.deserializeReq(mb);
                int err;
                StringBuffer retMsg;
                bool ok = manager.enableScopeScans(req.udesc, req.doEnable, &err, retMsg);
                mb.clear().append(err);
                mb.append(retMsg.str());
                if (err != 0 || retMsg.length())
                {
                    StringBuffer user;
                    req.udesc->getUserName(user);
                    DBGLOG("Error %d %sing Scope Scan Status for %s: %s", err, req.doEnable?"Enabl":"Disabl", user.str(), retMsg.str());
                }
                coven.reply(mb);
            }
            break;
        }
    }

    void ready()
    {
        acceptConnections.signal();
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL, MPTAG_DALI_SESSION_REQUEST);
        }
        join();
    }
};


class CSessionManagerBase: implements ISessionManager, public CInterface
{
protected:
    CheckedCriticalSection sessmanagersect;
public:
    IMPLEMENT_IINTERFACE;


    CSessionManagerBase()
    {
        servernotifys.kill();
    }

    virtual ~CSessionManagerBase()
    {
    }

    class CSessionSubscriptionProxy: implements ISubscription, public CInterface
    {   
        ISessionNotify *sub;
        SubscriptionId id;
        MemoryAttr ma;
        SessionId sessid;
    public:
        IMPLEMENT_IINTERFACE;

        CSessionSubscriptionProxy(ISessionNotify *_sub,SessionId _sessid)
        {
            sub = LINK(_sub);
            sessid = _sessid;
            MemoryBuffer mb;
            mb.append(sessid);
            ma.set(mb.length(),mb.toByteArray());
            id = queryCoven().getUniqueId();
        }

        ~CSessionSubscriptionProxy()
        {
            sub->Release();
        }

        ISessionNotify *queryNotify()
        {
            return sub;
        }

        const MemoryAttr &queryData()
        {
            return ma;
        }


        SubscriptionId getId()
        {
            return id;
        }

        void doNotify(bool aborted)
        {
            if (aborted)
                sub->aborted(sessid);
            else
                sub->closed(sessid);
        }

        void notify(MemoryBuffer &mb)
        {
            bool aborted;
            mb.read(aborted);
            doNotify(aborted);
        }

        void abort()
        {
            //NH: TBD
        }

        bool aborted()
        {
            return false;
        }
    };

    CIArrayOf<CSessionSubscriptionProxy>servernotifys;

    SubscriptionId subscribeSession(SessionId sessid, ISessionNotify *inotify)
    {
        CSessionSubscriptionProxy *proxy;
        SubscriptionId id;
        {
            CHECKEDCRITICALBLOCK(sessmanagersect,60000);
            proxy = new CSessionSubscriptionProxy(inotify,sessid);
            id = proxy->getId();
            if (sessid==SESSID_DALI_SERVER) {
                servernotifys.append(*proxy);
                return id;
            }
        }
        querySubscriptionManager(SESSION_PUBLISHER)->add(proxy,id);
        return id;
    }

    void unsubscribeSession(SubscriptionId id)
    {
        {
            CHECKEDCRITICALBLOCK(sessmanagersect,60000);
            // check not a server subscription
            ForEachItemIn(i,servernotifys) {
                if (servernotifys.item(i).getId()==id) {
                    servernotifys.remove(i);
                    return;
                }
            }
        }
        querySubscriptionManager(SESSION_PUBLISHER)->remove(id);
    }


    class cNotify: implements ISessionNotify, public CInterface
    {
    public:
        IMPLEMENT_IINTERFACE;
        Semaphore sem;
        void closed(SessionId id)
        {
            //PROGLOG("Session closed %" I64F "x",id);
            sem.signal();
        }
        void aborted(SessionId id)
        {
            //PROGLOG("Session aborted %" I64F "x",id);
            sem.signal();
        }
    };

    bool sessionStopped(SessionId id, unsigned timeout)
    {
        Owned<INode> node = getProcessSessionNode(id);
        if (node.get()==NULL)
            return true;
        if (timeout==0)
            return false;
        Owned<cNotify> cnotify = new cNotify;
        querySessionManager().subscribeSession(id,cnotify);
        if (cnotify->sem.wait(timeout))
            return false;
        node.setown(getProcessSessionNode(id));
        return node.get()==NULL;
    }   

    void notifyServerStopped(bool aborted)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        // check not a server subscription
        ForEachItemIn(i,servernotifys) {
            servernotifys.item(i).doNotify(aborted);
        }
    }


};



class CClientSessionManager: public CSessionManagerBase, implements IConnectionMonitor
{
    bool securitydisabled;
public:
    IMPLEMENT_IINTERFACE;

    CClientSessionManager()
    {
        securitydisabled = false;
    }
    virtual ~CClientSessionManager()
    {
        stop();
    }

    SessionId lookupProcessSession(INode *node)
    {
        if (!node)
            return mySessionId;
        CMessageBuffer mb;
        mb.append((int)MSR_LOOKUP_PROCESS_SESSION);
        node->serialize(mb);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
            return 0;
        SessionId ret;
        mb.read(ret);
        return ret;
    }

    virtual INode *getProcessSessionNode(SessionId id)
    {
        if (!id)
            return NULL;
        CMessageBuffer mb;
        mb.append((int)MSR_LOOKUP_PROCESS_SESSION);
        queryNullNode()->serialize(mb);
        mb.append(id);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
            return NULL;
        Owned<INode> node = deserializeINode(mb);
        if (node->endpoint().isNull())
            return NULL;
        return node.getClear();
    }


    SecAccessFlags getPermissionsLDAP(const char *key,const char *obj,IUserDescriptor *udesc,unsigned auditflags,int *err)
    {
        if (err)
            *err = 0;
        if (securitydisabled)
            return SecAccess_Unavailable;
        if (queryDaliServerVersion().compare("1.8") < 0) {
            securitydisabled = true;
            return SecAccess_Unavailable;
        }
        CMessageBuffer mb;
        mb.append((int)MSR_LOOKUP_LDAP_PERMISSIONS);
        mb.append(key).append(obj);
#ifdef NULL_DALIUSER_STACKTRACE
        //following debug code to be removed
        StringBuffer sb;
        if (udesc)
            udesc->getUserName(sb);
        if (0==sb.length())
        {
            DBGLOG("UNEXPECTED USER (NULL) in dasess.cpp getPermissionsLDAP() line %d",__LINE__);
            PrintStackReport();
        }
#endif
        udesc->serialize(mb);
        mb.append(auditflags);
        udesc->serializeExtra(mb);//serialize sessionToken and user signature if Dali version
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
            return SecAccess_None;
        SecAccessFlags perms = SecAccess_Unavailable;
        if (mb.remaining()>=sizeof(perms)) {
            mb.read((int &)perms);
            if (mb.remaining()>=sizeof(int)) {
                int e = 0;
                mb.read(e);
                if (err)
                    *err = e;
                else if (e) 
                    throw new CDaliLDAP_Exception(e);
            }
        }
        if (perms == SecAccess_Unavailable)
            securitydisabled = true;
        return perms;
    }

    bool clearPermissionsCache(IUserDescriptor *udesc)
    {
        if (securitydisabled)
            return true;
        if (queryDaliServerVersion().compare("1.8") < 0) {
            securitydisabled = true;
            return true;
        }
        CMessageBuffer mb;
        mb.append((int)MSR_CLEAR_PERMISSIONS_CACHE);
        udesc->serialize(mb);
        return queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT);
    }

    bool queryScopeScansEnabled(IUserDescriptor *udesc, int * err, StringBuffer &retMsg)
    {
        if (queryDaliServerVersion().compare("3.10") < 0)
        {
            *err = -1;
            StringBuffer ver;
            queryDaliServerVersion().toString(ver);
            retMsg.appendf("Scope Scan status feature requires Dali V3.10 or newer, current Dali version %s",ver.str());
            return false;
        }
        if (securitydisabled)
        {
            *err = -1;
            retMsg.append("Security not enabled");
            return false;
        }
        if (queryDaliServerVersion().compare("1.8") < 0) {
            *err = -1;
            retMsg.append("Security not enabled");
            securitydisabled = true;
            return false;
        }
        CMessageBuffer mb;
        CQueryScopeScansEnabledReq req(udesc);
        req.serializeReq(mb);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
        {
            *err = -1;
            retMsg.append("DALI Send/Recv error");
            return false;
        }
        int rc;
        bool enabled;
        mb.read(rc).read(enabled).read(retMsg);
        *err = rc;
        return enabled;
    }

    bool enableScopeScans(IUserDescriptor *udesc, bool enable, int * err, StringBuffer &retMsg)
    {
        if (queryDaliServerVersion().compare("3.10") < 0)
        {
            *err = -1;
            StringBuffer ver;
            queryDaliServerVersion().toString(ver);
            retMsg.appendf("Scope Scan enable/disable feature requires Dali V3.10 or newer, current Dali version %s",ver.str());
            return false;
        }

        if (securitydisabled)
        {
            *err = -1;
            retMsg.append("Security not enabled");
            return false;
        }
        if (queryDaliServerVersion().compare("1.8") < 0) {
            *err = -1;
            retMsg.append("Security not enabled");
            securitydisabled = true;
            return false;
        }
        CMessageBuffer mb;
        CEnableScopeScansReq req(udesc,enable);
        req.serializeReq(mb);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
        {
            *err = -1;
            retMsg.append("DALI Send/Recv error");
            return false;
        }
        int rc;
        mb.read(rc).read(retMsg);
        *err = rc;
        if (rc == 0)
        {
            StringBuffer user;
            udesc->getUserName(user);
            DBGLOG("Scope Scans %sabled by %s",enable ? "En" : "Dis", user.str());
        }
        return rc == 0;
    }

    bool checkScopeScansLDAP()
    {
        assertex(!"checkScopeScansLDAP called on client");
        return true; // actually only used server size
    }

    unsigned getLDAPflags()
    {
        assertex(!"getLdapFlags called on client");
        return 0;
    }

    void setLDAPflags(unsigned)
    {
        assertex(!"setLdapFlags called on client");
    }

    bool authorizeConnection(DaliClientRole,bool)
    {
        return true;
    }


    SessionId startSession(SecurityToken tok, SessionId parentid)
    {
        CMessageBuffer mb;
        mb.append((int)MSR_REGISTER_SESSION).append(tok).append(parentid);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT))
            return 0;
        SessionId ret;
        mb.read(ret);
        return ret;
    }

    void stopSession(SessionId sessid, bool failed)
    {
        if (sessid==SESSID_DALI_SERVER) {
            notifyServerStopped(failed);
            return;
        }
        CMessageBuffer mb;
        mb.append((int)MSR_STOP_SESSION).append(sessid).append(failed);
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT);
    }

    void onClose(SocketEndpoint &ep)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        Owned<INode> node = createINode(ep);
        if (queryCoven().inCoven(node)) {
            StringBuffer str;
            PROGLOG("Coven Session Stopping (%s)",ep.getUrlStr(str).str());
            if (queryCoven().size()==1)
                notifyServerStopped(true); 
        }
    }

    void start()
    {
        addMPConnectionMonitor(this);
    }

    void stop()
    {
    }

    void ready()
    {
        removeMPConnectionMonitor(this);
    }

    StringBuffer &getClientProcessList(StringBuffer &buf)
    {
        // dummy
        return buf;
    }

    StringBuffer &getClientProcessEndpoint(SessionId,StringBuffer &buf)
    {
        // dummy
        return buf;
    }

    unsigned queryClientCount()
    {
        // dummy
        return 0;
    }

    void importCapabilities(MemoryBuffer &mb)
    {
        CMessageBuffer msg;
        msg.append((int)MSR_IMPORT_CAPABILITIES);
        msg.append(mb.length(), mb.toByteArray());
        queryCoven().sendRecv(msg,RANK_RANDOM,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT);
    }
};

class CLdapWorkItem : public Thread
{
    StringAttr key;
    StringAttr obj;
    Linked<IUserDescriptor> udesc;
    Linked<IDaliLdapConnection> ldapconn;
    unsigned flags;
    bool running;
    Semaphore contsem;
    Semaphore ready;
    Semaphore &threaddone;
    int ret;
public:
    CLdapWorkItem(IDaliLdapConnection *_ldapconn,Semaphore &_threaddone)
        : ldapconn(_ldapconn), threaddone(_threaddone)
    {
        running = false;
    }
    void start(const char *_key,const char *_obj,IUserDescriptor *_udesc,unsigned _flags)
    {
        key.set(_key);
        obj.set(_obj); 
#ifdef NULL_DALIUSER_STACKTRACE
        StringBuffer sb;
        if (_udesc)
            _udesc->getUserName(sb);
        if (sb.length()==0)
        {
            DBGLOG("UNEXPECTED USER (NULL) in dasess.cpp CLdapWorkItem::start() line %d",__LINE__);
            PrintStackReport();
        }
#endif
        udesc.set(_udesc);
        flags = _flags;
        ret = CLDAPE_ldapfailure;
        if (!running) {
            running = true;
            Thread::start();
        }
        contsem.signal();
    }

    int run()
    {
        for (;;) {
            contsem.wait();
            if (!running)
                break;
            try {
                ret = ldapconn->getPermissions(key,obj,udesc,flags);
            }
            catch(IException *e) {
                LOG(MCoperatorError, unknownJob, e, "CLdapWorkItem"); 
                e->Release();
            }
            ready.signal();
        }
        threaddone.signal();
        return 0;
    }

    bool wait(unsigned timeout, int &_ret)
    {
        if (ready.wait(timeout)) {
            _ret = ret;
            return true;
        }
        _ret = 0;
        return false;
    }


    void stop()
    {
        running = false;
        contsem.signal();
    }

    static CLdapWorkItem *get(IDaliLdapConnection *_ldapconn,Semaphore &_threaddone)
    {
        if (!_threaddone.wait(1000*60*5)) {
            ERRLOG("Too many stalled LDAP threads");
            return NULL;
        }
        return new CLdapWorkItem(_ldapconn,_threaddone);
    }

};


class CCovenSessionManager: public CSessionManagerBase, implements ISessionManagerServer, implements ISubscriptionManager
{

    CSessionRequestServer   sessionrequestserver;
    CSessionStateTable      sessionstates;
    CMapProcessToSession    processlookup;
    Owned<IDaliLdapConnection> ldapconn;
    Owned<CLdapWorkItem> ldapworker;
    Semaphore ldapsig;
    atomic_t ldapwaiting;
    Semaphore workthreadsem;
    bool stopping;

    void remoteAddProcessSession(rank_t dst,SessionId id,INode *node, DaliClientRole role)
    {
        CMessageBuffer mb;
        mb.append((int)MSR_SECONDARY_REGISTER_PROCESS_SESSION).append(id);
        node->serialize(mb);
        int r = (int)role;
        mb.append(role);
        queryCoven().sendRecv(mb,dst,MPTAG_DALI_SESSION_REQUEST);
        // no fail currently
    }

    void remoteAddSession(rank_t dst,SessionId id)
    {
        CMessageBuffer mb;
        mb.append((int)MSR_SECONDARY_REGISTER_SESSION).append(id);
        queryCoven().sendRecv(mb,dst,MPTAG_DALI_SESSION_REQUEST);
        // no fail currently
    }

public:
    IMPLEMENT_IINTERFACE;

    CCovenSessionManager()
        : sessionrequestserver(*this)
    {
        mySessionId = queryCoven().getUniqueId(); // tell others in coven TBD
        registerSubscriptionManager(SESSION_PUBLISHER,this);
        atomic_set(&ldapwaiting,0);
        workthreadsem.signal(10);
        stopping = false;
        ldapsig.signal();

    }
    ~CCovenSessionManager()
    {
        stubTable.kill();
    }


    void start()
    {
        sessionrequestserver.start();
    }

    void stop()
    {
        stopping = true;
        if (!ldapsig.wait(60*1000))
            WARNLOG("LDAP stalled(1)");
        if (ldapworker) {
            ldapworker->stop();
            if (!ldapworker->join(1000))
                WARNLOG("LDAP stalled(2)");
            ldapworker.clear();
        }
        ldapconn.clear();
        removeMPConnectionMonitor(this);
        sessionrequestserver.stop();
    }

    void ready()
    {
        addMPConnectionMonitor(this);
        sessionrequestserver.ready();
    }

    void setLDAPconnection(IDaliLdapConnection *_ldapconn)
    {
        if (_ldapconn&&(_ldapconn->getLDAPflags()&DLF_ENABLED))
            ldapconn.setown(_ldapconn);
        else {
            ldapconn.clear();
            ::Release(_ldapconn);
        }
    }

    void setClientAuth(IDaliClientAuthConnection *_authconn)
    {
    }

    void addProcessSession(SessionId id,INode *client,DaliClientRole role)
    {
        StringBuffer str;
        PROGLOG("Session starting %" I64F "x (%s) : role=%s",id,client->endpoint().getUrlStr(str).str(),queryRoleName(role));
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        CProcessSessionState *s = new CProcessSessionState(id,client,role);
        while (!sessionstates.add(s)) // takes ownership
        {
            WARNLOG("Dali session manager: session already registered");
            sessionstates.remove(id);
        }
        while (!processlookup.add(s))
        {
            /* There's existing ip:port match (client) in process table..
             * Old may be in process of closing or about to, but new has beaten the onClose() to it..
             * Track old sessions in new CProcessSessionState instance, so that in-process or upcoming onClose()/stopSession() can find them
             */
            CProcessSessionState *previousState = processlookup.query(client);
            dbgassertex(previousState); // Must be there, it's reason add() failed
            SessionId oldSessionId = previousState->getId();
            s->addSessionIds(*previousState, false); // merges sessions from previous process state into new one that replaces it
            WARNLOG("Dali session manager: registerClient process session already registered, old (%" I64F "x) replaced", oldSessionId);
            processlookup.remove(previousState, this);
        }
    }

    void addSession(SessionId id)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        CSessionState *s = new CSessionState(id);
        while (!sessionstates.add(s)) { // takes ownership
            WARNLOG("Dali session manager: session already registered (2)");
            sessionstates.remove(id);
        }
    }

    SessionId registerSession(SecurityToken, SessionId parent)
    {
        // this is where security token should be checked

        // not in critical block (otherwise possibility of deadlock)
        ICoven &coven=queryCoven();
        SessionId id = coven.getUniqueId();
        rank_t myrank = coven.getServerRank();
        rank_t ownerrank = coven.chooseServer(id);
        // first do local
        if (myrank==ownerrank) 
            addSession(id);
        else 
            remoteAddSession(ownerrank,id);
        ForEachOtherNodeInGroup(r,coven.queryGroup()) {
            if (r!=ownerrank)
                remoteAddSession(r,id);
        }
        return id;
    }


    SessionId registerClientProcess(INode *client, IGroup *& retcoven, DaliClientRole role)
    {
        // not in critical block (otherwise possibility of deadlock)
        retcoven = NULL;
        ICoven &coven=queryCoven();
        SessionId id = coven.getUniqueId();
        rank_t myrank = coven.getServerRank();
        rank_t ownerrank = coven.chooseServer(id);
        // first do local
        if (myrank==ownerrank) 
            addProcessSession(id,client,role);
        else 
            remoteAddProcessSession(ownerrank,id,client,role);
        ForEachOtherNodeInGroup(r,coven.queryGroup()) {
            if (r!=ownerrank)
                remoteAddProcessSession(r,id,client,role);
        }
        retcoven = coven.getGroup();
        return id;
    }

    SessionId lookupProcessSession(INode *node)
    {
        if (!node)
            return mySessionId;
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        CProcessSessionState *s= processlookup.query(node);
        return s?s->getId():0;
    }

    INode *getProcessSessionNode(SessionId id)
    {
        StringBuffer eps;
        if (SessionManager->getClientProcessEndpoint(id,eps).length()!=0) 
            return createINode(eps.str());
        return NULL;
    }

    virtual SecAccessFlags getPermissionsLDAP(const char *key,const char *obj,IUserDescriptor *udesc,unsigned flags, int *err)
    {
        if (err)
            *err = 0;
        if (!ldapconn)
            return SecAccess_Unavailable;
#ifdef _NO_LDAP
        return SecAccess_Unavailable;
#else
#ifdef NULL_DALIUSER_STACKTRACE
        StringBuffer sb;
        if (udesc)
            udesc->getUserName(sb);
        if (sb.length()==0)
        {
            DBGLOG("UNEXPECTED USER (NULL) in dasess.cpp CCovenSessionManager::getPermissionsLDAP() line %d",__LINE__);
            PrintStackReport();
        }
#endif
        if ((ldapconn->getLDAPflags()&(DLF_SAFE|DLF_ENABLED))!=(DLF_SAFE|DLF_ENABLED))
            return ldapconn->getPermissions(key,obj,udesc,flags);
        atomic_inc(&ldapwaiting);
        unsigned retries = 0;
        while (!stopping) {
            if (ldapsig.wait(1000)) {
                atomic_dec(&ldapwaiting);
                if (!ldapworker)
                    ldapworker.setown(CLdapWorkItem::get(ldapconn,workthreadsem));
                if (ldapworker) {
                    ldapworker->start(key,obj,udesc,flags);
                    for (unsigned i=0;i<10;i++) {
                        if (i)
                            WARNLOG("LDAP stalled(%d) - retrying",i);
                        SecAccessFlags ret;
                        if (ldapworker->wait(1000*20,(int&)ret)) {
                            if (ret==CLDAPE_ldapfailure) {
                                LOG(MCoperatorError, unknownJob, "LDAP - failure (returning no access for %s)",obj); 
                                ldapsig.signal();
                                if (err)
                                    *err = CLDAPE_ldapfailure;
                                return SecAccess_None;
                            }
                            else {
                                ldapsig.signal();
                                return ret;
                            }
                        }
                        if (atomic_read(&ldapwaiting)>10)   // give up quicker if piling up
                            break;
                        if (i==5) { // one retry
                            ldapworker->stop(); // abandon thread
                            ldapworker.clear();
                            ldapworker.setown(CLdapWorkItem::get(ldapconn,workthreadsem));
                            if (ldapworker)
                                ldapworker->start(key,obj,udesc,flags);
                        }
                    }
                    if (ldapworker)
                        ldapworker->stop();
                    ldapworker.clear(); // abandon thread
                }
                LOG(MCoperatorError, unknownJob, "LDAP stalled - aborting (returning no access for %s)",obj); 
                ldapsig.signal();
                if (err)
                    *err = CLDAPE_getpermtimeout;
                return SecAccess_None;
            }
            else {
                unsigned waiting = atomic_read(&ldapwaiting);
                static unsigned last=0;
                static unsigned lasttick=0;
                static unsigned first50=0;
                if ((waiting!=last)&&(msTick()-lasttick>1000)) {
                    WARNLOG("%d threads waiting for ldap",waiting);
                    last = waiting;
                    lasttick = msTick();
                }
                if (waiting>50) {
                    if (first50==0)
                        first50 = msTick();
                    else if (msTick()-first50>60*1000) {
                        LOG(MCoperatorError, unknownJob, "LDAP stalled - aborting (returning 0 for %s)", obj); 
                        if (err)
                            *err = CLDAPE_getpermtimeout;
                        break;
                    }
                }
                else
                    first50 = 0;
            }
        }
        atomic_dec(&ldapwaiting);
        return SecAccess_None;
#endif
    }

    virtual bool clearPermissionsCache(IUserDescriptor *udesc)
    {
#ifdef _NO_LDAP
        bool ok = true;
#else
        bool ok = true;
        if (ldapconn && ldapconn->getLDAPflags() & DLF_ENABLED)
            ok = ldapconn->clearPermissionsCache(udesc);
#endif
        return ok;
    }

    virtual bool queryScopeScansEnabled(IUserDescriptor *udesc, int * err, StringBuffer &retMsg)
    {
#ifdef _NO_LDAP
        *err = -1;
        retMsg.append("LDAP not enabled");
        return false;
#else
        *err = 0;
        return checkScopeScansLDAP();
#endif
    }

    virtual bool enableScopeScans(IUserDescriptor *udesc, bool enable, int * err, StringBuffer &retMsg)
    {
#ifdef _NO_LDAP
        *err = -1;
        retMsg.append("LDAP not supporteded");
        return false;
#else
        if (!ldapconn)
        {
            *err = -1;
            retMsg.append("LDAP not connected");
            return false;
        }

        return ldapconn->enableScopeScans(udesc, enable, err);
#endif
    }

    virtual bool checkScopeScansLDAP()
    {
#ifdef _NO_LDAP
        return false;
#else
        return ldapconn?ldapconn->checkScopeScans():false;
#endif
    }
    
    virtual unsigned getLDAPflags()
    {
#ifdef _NO_LDAP
        return 0;
#else
        return ldapconn?ldapconn->getLDAPflags():0;
#endif
    }

    void setLDAPflags(unsigned flags)
    {
#ifndef _NO_LDAP
        if (ldapconn)
            ldapconn->setLDAPflags(flags);
#endif
    }


    bool authorizeConnection(int role,bool revoke)
    {
        return true;
    }


    SessionId startSession(SecurityToken tok, SessionId parentid)
    {
        return registerSession(tok,parentid);
    }

    void setSessionDependancy(SessionId id , SessionId dependsonid )
    {
        UNIMPLEMENTED; // TBD
    }
    void clearSessionDependancy(SessionId id , SessionId dependsonid )
    {
        UNIMPLEMENTED; // TBD
    }

protected:

    class CSessionSubscriptionStub: public CInterface
    {   
        ISubscription *subs;
        SubscriptionId id;
        SessionId sessid;
    public:
        CSessionSubscriptionStub(ISubscription *_subs,SubscriptionId _id) // takes ownership
        {
            subs = _subs;
            id = _id;
            const MemoryAttr &ma = subs->queryData();
            MemoryBuffer mb(ma.length(),ma.get());
            mb.read(sessid);
        }

        ~CSessionSubscriptionStub() 
        {
            subs->Release();
        }

        SubscriptionId getId() { return id; }
        SessionId getSessionId() { return sessid; }
        void notify(bool abort)
        {
            MemoryBuffer mb;
            mb.append(abort);
            subs->notify(mb); 
        }
        const void *queryFindParam() const { return &id; }
    };

    OwningSimpleHashTableOf<CSessionSubscriptionStub, SubscriptionId> stubTable;

    void add(ISubscription *subs,SubscriptionId id)
    {
        CSessionSubscriptionStub *nstub;
        {
            CHECKEDCRITICALBLOCK(sessmanagersect,60000);
            nstub = new CSessionSubscriptionStub(subs,id);
            if (sessionstates.query(nstub->getSessionId())||(nstub->getSessionId()==mySessionId))
            {
                stubTable.replace(*nstub);
                return;
            }
        }
        // see if session known
        MemoryBuffer mb;
        bool abort=true;
        mb.append(abort);
        ERRLOG("Session Manager - adding unknown session ID %" I64F "x", nstub->getSessionId());
        subs->notify(mb); 
        delete nstub;
        return;
    }

    void remove(SubscriptionId id)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        stubTable.remove(&id);
    }

    void stopSession(SessionId id, bool abort)
    {
        PROGLOG("Session stopping %" I64F "x %s",id,abort?"aborted":"ok");
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        // do in multiple stages as may remove one or more sub sessions
        for (;;)
        {
            CIArrayOf<CSessionSubscriptionStub> tonotify;
            SuperHashIteratorOf<CSessionSubscriptionStub> iter(stubTable);
            ForEach(iter)
            {
                CSessionSubscriptionStub &stub = iter.query();
                if (stub.getSessionId()==id)
                    tonotify.append(*LINK(&stub));
            }
            if (tonotify.ordinality()==0)
                break;
            ForEachItemIn(j,tonotify)
            {
                CSessionSubscriptionStub &stub = tonotify.item(j);
                stubTable.removeExact(&stub);
            }
            CHECKEDCRITICALUNBLOCK(sessmanagersect,60000);
            ForEachItemIn(j2,tonotify)
            {
                CSessionSubscriptionStub &stub = tonotify.item(j2);
                try { stub.notify(abort); }
                catch (IException *e) { e->Release(); } // subscriber session may abort during stopSession
            }
            tonotify.kill(); // clear whilst sessmanagersect unblocked, as subs may query session manager.
        }
        const CSessionState *state = sessionstates.query(id);
        if (state)
        {
            const CProcessSessionState *pState = QUERYINTERFACE(state, const CProcessSessionState);
            if (pState)
            {
                CProcessSessionState *cState = processlookup.query(&pState->queryNode()); // get current
                if (pState == cState) // so is current one.
                {
                    /* This is reinstating a previous CProcessSessionState for this node (if there is one),
                     * that has not yet stopped, and adding any other pending process states to the CProcessSessionState
                     * being reinstated.
                     */
                    SessionId prevId = cState->dequeuePreviousSessionId();
                    if (prevId)
                    {
                        PROGLOG("Session (%" I64F "x) in stopSession, detected %d pending previous states, reinstating session (%" I64F "x) as current", id, cState->previousSessionIdCount(), prevId);
                        CSessionState *prevSessionState = sessionstates.query(prevId);
                        dbgassertex(prevSessionState); // must be there
                        CProcessSessionState *prevProcessState = QUERYINTERFACE(prevSessionState, CProcessSessionState);
                        dbgassertex(prevProcessState);
                        /* NB: prevProcessState's have 0 entries in their previousSessionIds, since they were merged at replacement time
                         * in addProcessSession()
                         */

                        /* add in any remaining to-be-stopped process sessions from current that's stopping into this previous state
                         * that's being reinstated, so will be picked up on next onClose()/stopSession()
                         */
                        prevProcessState->addSessionIds(*cState, true);
                        processlookup.replace(prevProcessState);
                    }
                    else
                        processlookup.remove(pState, this);
                }
                else // Here because in stopSession for an previous process state, that has been replaced (in addProcessSession)
                {
                    if (processlookup.remove(pState, this))
                    {
                        // Don't think possible to be here, if not current must have replaced afaics
                        PROGLOG("Session (%" I64F "x) in stopSession, old process session removed", id);
                    }
                    else
                        PROGLOG("Session (%" I64F "x) in stopSession, old process session was already removed", id); // because replaced
                    if (cState)
                    {
                        PROGLOG("Session (%" I64F "x) was replaced, ensuring removed from new process state", id);
                        cState->removeOldSessionId(id); // If already replaced, then must ensure no longer tracked by new
                    }
                }
            }
            sessionstates.remove(id);
        }
    }

    void onClose(SocketEndpoint &ep)
    {
        StringBuffer clientStr;
        PROGLOG("Client closed (%s)", ep.getUrlStr(clientStr).str());

        SessionId idtostop;
        {
            CHECKEDCRITICALBLOCK(sessmanagersect,60000);
            Owned<INode> node = createINode(ep);
            if (queryCoven().inCoven(node))
            {
                PROGLOG("Coven Session Stopping (%s)", clientStr.str());
                // more TBD here
                return;
            }
            CProcessSessionState *s = processlookup.query(node);
            if (!s)
                return;
            idtostop = s->dequeuePreviousSessionId();
            if (idtostop)
            {
                PROGLOG("Previous sessionId (%" I64F "x) for %s was replaced by (%" I64F "x), stopping old session now", idtostop, clientStr.str(), s->getId());
                unsigned c = s->previousSessionIdCount();
                if (c) // very unlikely, but could be >1, trace for info.
                    PROGLOG("%d old sessions pending closure", c);
            }
            else
                idtostop = s->getId();
        }
        stopSession(idtostop,true);
    }

    StringBuffer &getClientProcessList(StringBuffer &buf)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        unsigned n = processlookup.count();
        CProcessSessionState *s=NULL;
        for (unsigned i=0;i<n;i++) {
            s=processlookup.next(s);
            if (!s)
                break;
            s->getDetails(buf).append('\n');
        }
        return buf;
    }

    StringBuffer &getClientProcessEndpoint(SessionId id,StringBuffer &buf)
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        const CSessionState *state = sessionstates.query(id);
        if (state) {
            const CProcessSessionState *pstate = QUERYINTERFACE(state,const CProcessSessionState);
            if (pstate) 
                return pstate->queryNode().endpoint().getUrlStr(buf);
        }
        return buf;
    }

    unsigned queryClientCount()
    {
        CHECKEDCRITICALBLOCK(sessmanagersect,60000);
        return processlookup.count();
    }

};


ISessionManager &querySessionManager()
{
    CriticalBlock block(sessionCrit);
    if (!SessionManager) {
        assertex(!isCovenActive()||!queryCoven().inCoven()); // Check not Coven server (if occurs - not initialized correctly;
                                                   // If !coven someone is checking for dali so allow
        SessionManager = new CClientSessionManager();
    
    }
    return *SessionManager;
}


class CDaliSessionServer: public IDaliServer, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CDaliSessionServer()
    {
    }

    void start()
    {
        CriticalBlock block(sessionCrit);
        assertex(queryCoven().inCoven()); // must be member of coven
        CCovenSessionManager *serv = new CCovenSessionManager();
        SessionManagerServer = serv;
        SessionManager = serv;
        SessionManagerServer->start();
    }

    void suspend()
    {
    }

    void stop()
    {
        CriticalBlock block(sessionCrit);
        SessionManagerServer->stop();
        SessionManagerServer->Release();
        SessionManagerServer = NULL;
        SessionManager = NULL;
    }

    void ready()
    {
        SessionManagerServer->ready();
    }

    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }

};

IDaliServer *createDaliSessionServer()
{
    return new CDaliSessionServer();
}

void setLDAPconnection(IDaliLdapConnection *ldapconn)
{
    if (SessionManagerServer)
        SessionManagerServer->setLDAPconnection(ldapconn);
}


void setClientAuth(IDaliClientAuthConnection *authconn)
{
    if (SessionManagerServer)
        SessionManagerServer->setClientAuth(authconn);
}



#define REG_SLEEP 5000
bool registerClientProcess(ICommunicator *comm, IGroup *& retcoven,unsigned timeout,DaliClientRole role)
{
    // NB doesn't use coven as not yet set up
    if (comm->queryGroup().ordinality()==0)
        return false;
    CTimeMon tm(timeout);
    retcoven = NULL;
    unsigned nextLog = 0, lastNextLog = 0;
    unsigned t=REG_SLEEP;
    unsigned remaining;
    rank_t r;
    while (!tm.timedout(&remaining)) {
        if (remaining>t)
            remaining = t;
        r = getRandom()%comm->queryGroup().ordinality();
        
        bool ok = false;
        if (remaining>10000) // MP protocol has a minimum time of 10s so if remaining < 10s use a detachable thread
            ok = comm->verifyConnection(r,remaining);
        else {
            struct cThread: public Thread
            {
                Semaphore sem;
                Linked<ICommunicator> comm;
                bool ok;
                Owned<IException> exc;
                unsigned remaining;
                rank_t r;
                cThread(ICommunicator *_comm,rank_t _r,unsigned _remaining)
                    : Thread ("dasess.registerClientProcess"), comm(_comm)
                {
                    r = _r;
                    remaining = _remaining;
                    ok = false;
                }
                int run()
                {
                    try {
                        if (comm->verifyConnection(r,remaining))
                            ok = true;
                    }
                    catch (IException *e)
                    {
                        exc.setown(e);
                    }
                    sem.signal();
                    return 0;
                }
            } *t = new cThread(comm,r,remaining);
            t->start();
            t->sem.wait(remaining);
            ok = t->ok;
            if (t->exc.get()) {
                IException *e = t->exc.getClear();
                t->Release();
                throw e;
            }
            t->Release();
        }   
        if (ok) {
            CMessageBuffer mb;
            mb.append((int)MSR_REGISTER_PROCESS_SESSION);
            queryMyNode()->serialize(mb);
            comm->queryGroup().queryNode(r).serialize(mb);
            mb.append((int)role);
            if (comm->sendRecv(mb,r,MPTAG_DALI_SESSION_REQUEST,SESSIONREPLYTIMEOUT)) {
                if (!mb.length())
                {
                    // failed system capability match, 
                    retcoven = comm->getGroup(); // continue as if - will fail later more obscurely.
                    return true;
                }
                mb.read(mySessionId);
                retcoven = deserializeIGroup(mb);
                return true;
            }
        }
        StringBuffer str;
        PROGLOG("Failed to connect to Dali Server %s.", comm->queryGroup().queryNode(r).endpoint().getUrlStr(str).str());
        if (tm.timedout())
        {
            PROGLOG("%s", str.append(" Timed out.").str());
            break;
        }
        else if (0 == nextLog)
        {
            PROGLOG("%s", str.append(" Retrying...").str());
            if ((lastNextLog * REG_SLEEP) >= 60000) // limit to a minute between logging
                nextLog = 60000 / REG_SLEEP;
            else
                nextLog = lastNextLog + 2; // wait +2 REG_SLEEP pauses before next logging
            lastNextLog = nextLog;
        }
        else
            --nextLog;
        Sleep(REG_SLEEP);
    }
    return false;
}


extern void stopClientProcess()
{
    CriticalBlock block(sessionCrit);
    if (mySessionId&&SessionManager) {
        try {
            querySessionManager().stopSession(mySessionId,false);
        }
        catch (IDaliClient_Exception *e) {
            if (e->errorCode()!=DCERR_server_closed)
                throw;
            e->Release();
        }
        mySessionId = 0;
    }
}
    

class CProcessSessionWatchdog
{
};



class CUserDescriptor: implements IUserDescriptor, public CInterface
{
    StringAttr username;
    StringAttr passwordenc;
    MemoryBuffer sessionToken;//ESP session token
    MemoryBuffer signature;//user's digital Signature
public:
    IMPLEMENT_IINTERFACE;
    StringBuffer &getUserName(StringBuffer &buf)
    {
        return buf.append(username);
    }
    StringBuffer &getPassword(StringBuffer &buf)
    {
        decrypt(buf,passwordenc);
        return buf;
    }
    const MemoryBuffer &querySessionToken()
    {
        return sessionToken;
    }
    const MemoryBuffer &querySignature()
    {
        return signature;
    }
    virtual void set(const char *name,const char *password)
    {
        username.set(name);
        StringBuffer buf;
        encrypt(buf,password);
        passwordenc.set(buf.str());
    }
    void set(const char *_name, const char *_password, const MemoryBuffer &_sessionToken, const MemoryBuffer &_signature)
    {
        set(_name, _password);
        sessionToken.clear().append(_sessionToken);
        signature.clear().append(_signature);
    }
    virtual void clear()
    {
        username.clear();
        passwordenc.clear();
        sessionToken.clear();
        signature.clear();
    }
    void serialize(MemoryBuffer &mb)
    {
        mb.append(username).append(passwordenc);
    }
    void serializeExtra(MemoryBuffer &mb)
    {
        if (queryDaliServerVersion().compare("3.14") >= 0)
            mb.append(sessionToken.length()).append(sessionToken).append(signature.length()).append(signature);
    }
    void deserialize(MemoryBuffer &mb)
    {
        mb.read(username).read(passwordenc);
    }
    void deserializeExtra(MemoryBuffer &mb)
    {
        if (mb.remaining() > 0)
        {
            size32_t len = 0;
            mb.read(len);
            if (len)
                sessionToken.append(len, mb.readDirect(len));

            if (mb.remaining() > 0)
            {
                mb.read(len);
                if (len)
                    signature.append(len, mb.readDirect(len));
            }
        }
    }
};

IUserDescriptor *createUserDescriptor()
{
    return new CUserDescriptor;
}

IUserDescriptor *createUserDescriptor(MemoryBuffer &mb)
{
    IUserDescriptor * udesc = createUserDescriptor();
    udesc->deserialize(mb);
    return udesc;
}

MODULE_INIT(INIT_PRIORITY_DALI_DASESS)
{
    return true;
}

MODULE_EXIT()
{
    ::Release(SessionManager);
    SessionManager = NULL;
}


