/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jlzw.hpp"
#include "jmisc.hpp"
#include "jtime.hpp"
#include "jregexp.hpp"
#include "jexcept.hpp"
#include "jsort.hpp"
#include "jptree.hpp"
#include "jbuff.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "dasess.hpp"
#include "daclient.hpp"
#include "daserver.hpp"
#include "dautils.hpp"
#include "danqs.hpp"
#include "mputil.hpp"
#include "rmtfile.hpp"
#include "dadfs.hpp"

#ifdef _DEBUG
//#define EXTRA_LOGGING
//#define TRACE_LOCKS
#endif

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_SUB_LOCK_TIMEOUT (10000)
#define SDS_TRANSACTION_RETRY (60000)
#define SDS_UPDATEFS_TIMEOUT (10000)

#define DEFAULT_NUM_DFS_THREADS 30
#define TIMEOUT_ON_CLOSEDOWN 120000 // On closedown, give up on trying to join a thread in CDaliDFSServer after two minutes

#if _INTERNAL_EDITION == 1
#ifndef _MSC_VER
#warning Disabling Sub-file compatibility checking
#endif
#else
#define SUBFILE_COMPATIBILITY_CHECKING
#endif

//#define PACK_ECL


#define SDS_GROUPSTORE_ROOT "Groups" // followed by name

class CDistributedFile;

enum MDFSRequestKind 
{ 
    MDFS_ITERATE_FILES,
    MDFS_UNUSED1,
    MDFS_GET_FILE_TREE,
    MDFS_GET_GROUP_TREE,
    MDFS_SET_FILE_ACCESSED,
    MDFS_ITERATE_RELATIONSHIPS,
    MDFS_SET_FILE_PROTECT,
    MDFS_ITERATE_FILTEREDFILES,
    MDFS_MAX
};

// Mutex for physical operations (remove/rename)
static CriticalSection physicalChange;

#define MDFS_GET_FILE_TREE_V2 ((unsigned)1)

static int strcompare(const void * left, const void * right)
{
    const char * l = (const char *)left; 
    const char * r = (const char *)right;
    return stricmp(l,r);
}

inline unsigned ipGroupDistance(const IpAddress &ip,const IGroup *grp)
{
    if (!grp)
        return (unsigned)-1;
    return grp->distance(ip);
}

inline unsigned groupDistance(IGroup *grp1,IGroup *grp2)
{
    if (grp1==grp2)
        return 0;
    if (!grp1||!grp2)
        return (unsigned)-1;
    return grp1->distance(grp2);
}
    

static StringBuffer &normalizeFormat(StringBuffer &in)
{
    in.toLowerCase();
    for (unsigned i = 0;i<in.length();)
    {
        switch (in.charAt(i)) {
        case '-': 
        case '_': 
        case ' ': 
            in.remove(i,1);
            break;
        default:
            i++;
            break;
        }
    }
    return in;
}

static StringBuffer &getAttrQueryStr(StringBuffer &str,const char *sub,const char *key,const char *name)
{
    assertex(key[0]=='@');
    str.appendf("%s[%s=\"%s\"]",sub,key,name);
    return str;
}

static IPropertyTree *getNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name,bool preload)
{  // no create
    if (!parent)
        return NULL;
    StringBuffer query;
    getAttrQueryStr(query,sub,key,name);
    if (preload)
        return parent->getBranch(query.str());
    return parent->getPropTree(query.str());
}

static IPropertyTree *addNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name, IPropertyTree* init=NULL)
{  
    IPropertyTree* ret = init?createPTreeFromIPT(init):createPTree(sub);
    assertex(key[0]=='@');
    ret->setProp(key,name);
    ret = parent->addPropTree(sub,ret);
    return LINK(ret);
}

const char *normalizeLFN(const char *s,StringBuffer &tmp)
{ 
    CDfsLogicalFileName dlfn;
    dlfn.set(s);
    return dlfn.get(tmp).str();
}

static IPropertyTree *getEmptyAttr()
{
    return createPTree("Attr");
}

RemoteFilename &constructPartFilename(IGroup *grp,unsigned partno,unsigned partmax,const char *name,const char *partmask,const char *partdir,unsigned copy,ClusterPartDiskMapSpec &mspec,RemoteFilename &rfn)
{
    partno--;
    StringBuffer tmp;
    if (!name||!*name) {
        if (!partmask||!*partmask) {
            partmask = "!ERROR!._$P$_of_$N$"; // could use logical tail name if I had it
            ERRLOG("No partmask for constructPartFilename");
        }
        name = expandMask(tmp,partmask,partno,partmax).str();
    }
    StringBuffer fullname;
    if (findPathSepChar(name)==NULL) 
        addPathSepChar(fullname.append(partdir));
    fullname.append(name);
    unsigned n;
    unsigned d;
    mspec.calcPartLocation(partno,partmax,copy,grp?grp->ordinality():partmax,n,d);
    setReplicateFilename(fullname,d);  
    SocketEndpoint ep;
    if (grp)
        ep=grp->queryNode(n).endpoint();
    rfn.setPath(ep,fullname.toLowerCase().str());
    return rfn;
}

inline void LOGPTREE(const char *title,IPropertyTree *pt)
{
    StringBuffer buf;
    if (pt) {
      toXML(pt,buf);
      PROGLOG("%s:\n%s\n",title,buf.str());
    }
    else
        PROGLOG("%s : NULL",title);
}

inline void LOGFDESC(const char *title,IFileDescriptor *fdesc)
{
    if (fdesc) {
        Owned<IPropertyTree> pt = fdesc->getFileTree();
        LOGPTREE(title,pt);
    }
    else
        PROGLOG("%s : NULL",title);
}


class CDFS_Exception: public CInterface, implements IDFS_Exception
{
    StringAttr errstr;
    int errcode;
public:
    CDFS_Exception(int _errcode, const char *_errstr)
        : errstr(_errstr)
    {
        errcode = _errcode;
    }

    int             errorCode() const { return errcode; }
    StringBuffer &  errorMessage(StringBuffer &str) const
    { 
        if (errcode==DFSERR_ok)
            return str;
        str.append("DFS Exception: ").append(errcode);
        switch(errcode) {
        case DFSERR_LogicalNameAlreadyExists: 
            return str.append(": logical name ").append(errstr).append(" already exists");
        case DFSERR_CannotFindPartFileSize:
            return str.append(": Cannot find physical file size for ").append(errstr);
        case DFSERR_CannotFindPartFileCrc:
            return str.append(": Cannot find physical file crc for ").append(errstr);
        case DFSERR_LookupAccessDenied: 
            return str.append(" Lookup access denied for scope ").append(errstr);
        case DFSERR_CreateAccessDenied: 
            return str.append(" Create access denied for scope ").append(errstr);
        case DFSERR_PhysicalPartAlreadyExists: 
            return str.append(": physical part ").append(errstr).append(" already exists");
        case DFSERR_PhysicalPartDoesntExist: 
            return str.append(": physical part ").append(errstr).append(" doesnt exist");
        case DFSERR_ForeignDaliTimeout: 
            return str.append(": Timeout connecting to Dali Server on ").append(errstr);
        case DFSERR_ClusterNotFound: 
            return str.append(": Cluster not found: ").append(errstr);
        case DFSERR_ClusterAlreadyExists: 
            return str.append(": Cluster already exists: ").append(errstr);
        case DFSERR_LookupConnectionTimout: 
            return str.append(": Lookup connection timeout: ").append(errstr);
        case DFSERR_FailedToDeleteFile:
            return str.append(": Failed to delete file: ").append(errstr);
        }
        return str.append("Unknown DFS Exception"); 
    }
    MessageAudience errorAudience() const { return MSGAUD_user; }

    IMPLEMENT_IINTERFACE;
};


class CConnectLock
{
public:
    Owned<IRemoteConnection> conn;
    CConnectLock(const char *caller, const char *name, bool write, bool preload, bool hold, unsigned timeout)
    {
        unsigned start = msTick();
        bool first = true;
        loop
        {
            try
            {
                unsigned mode = write ? RTM_LOCK_WRITE : RTM_LOCK_READ;
                if (preload) mode |= RTM_SUB;
                if (hold) mode |= RTM_LOCK_HOLD;
                conn.setown(querySDS().connect(name, queryCoven().inCoven() ? 0 : myProcessSession(), mode, (timeout==INFINITE)?1000*60*5:timeout));
#ifdef TRACE_LOCKS
                PROGLOG("%s: LOCKGOT(%x) %s %s",caller,(unsigned)(memsize_t)conn.get(),name,write?"WRITE":"");
                LogRemoteConn(conn);
                PrintStackReport();
#endif
                break;
            }
            catch (ISDSException *e)
            {
                if (SDSExcpt_LockTimeout == e->errorCode())
                {
#ifdef TRACE_LOCKS
                    PROGLOG("%s: LOCKFAIL %s %s",caller,name,write?"WRITE":"");
                    LogRemoteConn(conn);
#endif
                    unsigned tt = msTick()-start;
                    if (timeout!=INFINITE) {
                        StringBuffer tmp;
                        e->errorMessage(tmp);
                        CDFS_Exception *dfse = new CDFS_Exception(DFSERR_LookupConnectionTimout,tmp.str());
                        e->Release();
                        throw dfse;
                    }
                    WARNLOG("CConnectLock on %s waiting for %ds",name,tt/1000);
                    if (first) {
                        PrintStackReport();
                        first = false;
                    }
                    if (tt>SDS_CONNECT_TIMEOUT)
                        throw;
                    e->Release();
                }
                else
                    throw;
            }
            catch (IException *e) {
                StringBuffer tmp("CConnectLock ");
                tmp.append(caller).append(' ').append(name);
                EXCLOG(e, tmp.str());
                throw;
            }
        }
    }
    IRemoteConnection *detach()
    {
#ifdef TRACE_LOCKS
        if (conn.get()) {
            PROGLOG("LOCKDETACH(%x)",(unsigned)(memsize_t)conn.get());
            LogRemoteConn(conn);
        }
#endif
        return conn.getClear();
    }
#ifdef TRACE_LOCKS
    ~CConnectLock()
    {
        if (conn.get()) {
            PROGLOG("LOCKDELETE(%x)",(unsigned)(memsize_t)conn.get());
            LogRemoteConn(conn);
        }
    }
#endif
};

void ensureFileScope(const CDfsLogicalFileName &dlfn,unsigned timeout)
{
    CConnectLock connlock("ensureFileScope",querySdsFilesRoot(),true,false,false,timeout);
    StringBuffer query;
    IPropertyTree *r = connlock.conn->getRoot();
    StringBuffer scopes;
    const char *s=dlfn.getScopes(scopes,true).str();
    loop {
        IPropertyTree *nr;
        const char *e = strstr(s,"::");
        query.clear();
        if (e) 
            query.append(e-s,s);
        else
            query.append(s);
        nr = getNamedPropTree(r,queryDfsXmlBranchName(DXB_Scope),"@name",query.trim().toLowerCase().str(),false);
        if (!nr)
            nr = addNamedPropTree(r,queryDfsXmlBranchName(DXB_Scope),"@name",query.str());
        r->Release();
        if (!e) { 
            ::Release(nr);
            break;
        }
        r = nr;
        s = e+2;
    }
}

void removeFileEmptyScope(const CDfsLogicalFileName &dlfn,unsigned timeout)
{
    CConnectLock connlock("removeFileEmptyScope",querySdsFilesRoot(),true,false,false,timeout); //*1
    IPropertyTree *root = connlock.conn.get()?connlock.conn->queryRoot():NULL;
    if (!root)
        return;
    StringBuffer query;
    dlfn.makeScopeQuery(query.clear(),false);
    StringBuffer head;
    loop {
        if (query.length()) {
            const char *tail = splitXPath(query.str(),head.clear());
            if (!tail||!*tail)
                break;
            IPropertyTree *pt;
            if (head.length()) {
                query.set(head);
                pt = root->queryPropTree(query.str());
            }
            else 
                pt = root;
            IPropertyTree *t = pt?pt->queryPropTree(tail):NULL;
            if (t) {
                if (t->hasChildren()) 
                    break;
                pt->removeTree(t);  
                if (root==pt)
                    break;
            }
            else
                break;
        }
        else 
            break;
    }
}

class CFileLockBase
{
    IRemoteConnection *conn;
protected:
    Owned<IRemoteConnection> lock;

    bool init(const char *lockPath, unsigned mode, IRemoteConnection *_conn, unsigned timeout, const char *msg)
    {
        conn = NULL;
        lock.clear();
        CTimeMon tm(timeout);
        loop
        {
            try
            {
                lock.setown(querySDS().connect(lockPath, myProcessSession(), mode, timeout>60000 ? 60000 : timeout));
                if (lock.get())
                {
                    conn = _conn;
                    return true;
                }
                return false;
            }
            catch (ISDSException *e)
            {
                if (SDSExcpt_LockTimeout != e->errorCode() || tm.timedout())
                    throw;
                WARNLOG("CFileAttrLockBase(%s) blocked for %ds", msg, tm.elapsed()/1000);
                e->Release();
            }
        }
    }
public:
    CFileLockBase()
    {
        conn = NULL;
    }
    ~CFileLockBase()
    {
        // if conn provided, 'lock' was just a surrogate for the owner connection, commit now to conn if write lock
        if (conn && lock)
            conn->commit();
    }
    IRemoteConnection *detach()
    {
        return lock.getClear();
    }
    void clear()
    {
        lock.clear();
        conn = NULL;
    }
    IPropertyTree *queryRoot() const
    {
        return lock.get() ? lock->queryRoot() : NULL;
    }
};

class CFileLock : protected CFileLockBase
{
protected:
    DfsXmlBranchKind kind;
public:
    CFileLock()
    {
        kind = DXB_Internal;
    }
    bool init(const CDfsLogicalFileName &logicalName, DfsXmlBranchKind bkind, unsigned mode, unsigned timeout, const char *msg)
    {
        StringBuffer lockPath;
        logicalName.makeFullnameQuery(lockPath, bkind, true);
        if (CFileLockBase::init(lockPath, mode, NULL, timeout, msg))
        {
            kind = bkind;
            return true;
        }
        kind = DXB_Internal;
        return false;
    }
    bool init(const CDfsLogicalFileName &logicalName, unsigned mode, unsigned timeout, const char *msg)
    {
        StringBuffer lockPath;
        logicalName.makeFullnameQuery(lockPath, DXB_File, true);
        if (CFileLockBase::init(lockPath, mode, NULL, timeout, msg))
        {
            kind = DXB_File;
            return true;
        }
        // try super
        logicalName.makeFullnameQuery(lockPath.clear(), DXB_SuperFile, true);
        if (CFileLockBase::init(lockPath, mode, NULL, timeout, msg))
        {
            kind = DXB_SuperFile;
            return true;
        }
        kind = DXB_Internal;
        return false;
    }
    IRemoteConnection *detach() { return CFileLockBase::detach(); }
    IPropertyTree *queryRoot() const { return CFileLockBase::queryRoot(); }
    IRemoteConnection *queryConnection() const
    {
        return lock;
    }
    void clear()
    {
        CFileLockBase::clear();
        kind = DXB_Internal;
    }
    DfsXmlBranchKind getKind() const { return kind; }
};

class CFileSubLock : protected CFileLockBase
{
public:
    bool init(const CDfsLogicalFileName &logicalName, DfsXmlBranchKind bkind, unsigned mode, const char *subLock, IRemoteConnection *conn, unsigned timeout, const char *msg)
    {
        StringBuffer lockPath;
        logicalName.makeFullnameQuery(lockPath, bkind, true);
        lockPath.appendf("/%s", subLock);
        return CFileLockBase::init(lockPath, mode, conn, timeout, msg);
    }
    bool init(const CDfsLogicalFileName &logicalName, unsigned mode, const char *subLock, IRemoteConnection *conn, unsigned timeout, const char *msg)
    {
        StringBuffer lockPath;
        logicalName.makeFullnameQuery(lockPath, DXB_File, true);
        lockPath.appendf("/%s", subLock);
        if (CFileLockBase::init(lockPath, mode, conn, timeout, msg))
            return true;
        // try super
        logicalName.makeFullnameQuery(lockPath.clear(), DXB_SuperFile, true);
        return CFileLockBase::init(lockPath, mode, conn, timeout, msg);
    }
};


class CFileAttrLock : protected CFileSubLock
{
public:
    bool init(const CDfsLogicalFileName &logicalName, DfsXmlBranchKind bkind, unsigned mode, IRemoteConnection *conn, unsigned timeout, const char *msg)
    {
        return CFileSubLock::init(logicalName, bkind, mode, "Attr", conn, timeout, msg);
    }
    bool init(const CDfsLogicalFileName &logicalName, unsigned mode, IRemoteConnection *conn, unsigned timeout, const char *msg)
    {
        return CFileSubLock::init(logicalName, mode, "Attr", conn, timeout, msg);
    }
    IPropertyTree *queryRoot() const { return CFileSubLock::queryRoot(); }
};

class CFileLockCompound : protected CFileLockBase
{
public:
    bool init(const CDfsLogicalFileName &logicalName, unsigned mode, IRemoteConnection *conn, const char *subLock, unsigned timeout, const char *msg)
    {
        StringBuffer lockPath;
        if (subLock)
            lockPath.appendf("/_Locks/%s/", subLock);
        logicalName.makeXPathLName(lockPath);
        return CFileLockBase::init(lockPath, mode, conn, timeout, msg);
    }
};

class CFileSuperOwnerLock : protected CFileLockCompound
{
public:
    bool init(const CDfsLogicalFileName &logicalName, IRemoteConnection *conn, unsigned timeout, const char *msg)
    {
        return CFileLockCompound::init(logicalName, RTM_CREATE_QUERY | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, conn, "SuperOwnerLock", timeout, msg);
    }
    IRemoteConnection *detach()
    {
        return CFileLockCompound::detach();
    }
};

class CScopeConnectLock
{
    CConnectLock *lock;
public:
    CScopeConnectLock()
    {
        lock = NULL;
    }
    CScopeConnectLock(const char *caller, const CDfsLogicalFileName &lname, bool write, bool preload, bool hold, unsigned timeout)
    {
        lock = NULL;
        init(caller, lname, write, preload, hold, timeout);
    }
    ~CScopeConnectLock()
    {
        delete lock;
    }

    bool init(const char *caller, const CDfsLogicalFileName &lname, bool write, bool preload, bool hold, unsigned timeout)
    {
        delete lock;
        StringBuffer query;
        lname.makeScopeQuery(query,true);
        lock = new CConnectLock(caller, query.str(), write, preload,hold, timeout);
        if (lock->conn.get()==NULL)
        {
            delete lock;
            lock = NULL;
            ensureFileScope(lname);
            lock = new CConnectLock(caller, query.str(), write, preload, hold, timeout);
        }
        return lock->conn.get()!=NULL;
    }
    IRemoteConnection *detach()
    {
        return lock?lock->detach():NULL;
    }

    IRemoteConnection *conn()
    {
        return lock?lock->conn:NULL;
    }

    IPropertyTree *queryRoot()
    {
        return (lock&&lock->conn.get())?lock->conn->queryRoot():NULL;
    }

    void remove()
    {
        if (lock&&lock->conn.get())
            lock->conn->close(true);
    }

    IPropertyTree *queryFileRoot(const CDfsLogicalFileName &dlfn,DfsXmlBranchKind &bkind)
    {
        bool external;
        bool foreign;
        external = dlfn.isExternal();
        foreign = dlfn.isForeign();
        if (external||foreign)
            return NULL;
        IPropertyTree *sroot = queryRoot();
        if (!sroot)
            return NULL;
        StringBuffer tail;
        dlfn.getTail(tail);
        StringBuffer query;
        getAttrQueryStr(query,queryDfsXmlBranchName(DXB_File),"@name",tail.str());
        IPropertyTree *froot = sroot->queryPropTree(query.str());
        bkind = DXB_File;
        if (!froot) {
            // check for super file
            getAttrQueryStr(query.clear(),queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str());
            froot = sroot->queryPropTree(query.str());
            if (froot)
                bkind = DXB_SuperFile;
        }
        return froot;
    }
};

class CClustersLockedSection
{
    Owned<IRemoteConnection> conn;
public:
    CClustersLockedSection(CDfsLogicalFileName &dlfn, bool exclusive)
    {
        StringBuffer xpath;
        dlfn.makeFullnameQuery(xpath,DXB_File,true).append("/ClusterLock");

        /* Avoid RTM_CREATE_QUERY connect() if possible by making 1st call without. This is to avoid write contention caused by RTM_CREATE*
         * NB: RTM_CREATE_QUERY should probably only gain exclusive access in Dali if node is missing.
         */
        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), exclusive ? RTM_LOCK_WRITE : RTM_LOCK_READ, SDS_CONNECT_TIMEOUT));
        if (!conn.get()) // NB: ClusterLock is now created at File create time, so this can only be true for pre-existing File's
        {
            conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_CREATE_QUERY | RTM_LOCK_WRITE, SDS_CONNECT_TIMEOUT));
            assertex(conn.get());
            if (!exclusive)
                conn->changeMode(RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        }
    }
};

static void checkDfsReplyException(MemoryBuffer &mb)
{
    if (mb.length()<=sizeof(int))
        return;
    if ((*(int *)mb.bufferBase()) == -1) { // exception indicator
        int i;
        mb.read(i);
        throw deserializeException(mb);
    }
}

static void foreignDaliSendRecv(const INode *foreigndali,CMessageBuffer &mb, unsigned foreigndalitimeout)
{
    SocketEndpoint ep = foreigndali->endpoint();
    if (ep.port==0)
        ep.port = DALI_SERVER_PORT;
    Owned<IGroup> grp = createIGroup(1,&ep);
    Owned<ICommunicator> comm = createCommunicator(grp,true); 
    if (!comm->verifyConnection(0,foreigndalitimeout)) {
        StringBuffer tmp;
        IDFS_Exception *e = new CDFS_Exception(DFSERR_ForeignDaliTimeout, foreigndali->endpoint().getUrlStr(tmp).str());
        throw e;
    }
    comm->sendRecv(mb,0,MPTAG_DFS_REQUEST);
}

static bool isLocalDali(const INode *foreigndali)
{
    if (!foreigndali)
        return true;
    Owned<INode> node;
    SocketEndpoint ep = foreigndali->endpoint();
    if (ep.port==0) {
        ep.port = DALI_SERVER_PORT;
        node.setown(createINode(ep));
        foreigndali = node.get();
    }
    return queryCoven().inCoven((INode *)foreigndali);
}


class FileClusterInfoArray: public IArrayOf<IClusterInfo>
{
    ClusterPartDiskMapSpec defaultmapping;
    bool singleclusteroverride;
public:
    FileClusterInfoArray()
    {
        singleclusteroverride = false;
    }
    void clear()
    {
        IArrayOf<IClusterInfo>::kill();
    }
    unsigned getNames(StringArray &clusternames)
    {
        StringBuffer name;
        ForEachItem(i) {
            clusternames.append(item(i).getClusterLabel(name.clear()).str());
            if (singleclusteroverride)
                break;
        }
        return clusternames.ordinality();
    }

    unsigned find(const char *_clusterName)
    {
        StringAttr clusterName = _clusterName;
        clusterName.toLowerCase();
        StringBuffer name;
        ForEachItem(i)  {
            if (strcmp(item(i).getClusterLabel(name.clear()).str(),clusterName)==0)
                return i;
            if (singleclusteroverride)
                break;
        }
        return NotFound;
    }

    IGroup *queryGroup(unsigned clusternum)
    {
        if (clusternum>=ordinality())
            return NULL;
        if (singleclusteroverride&&clusternum)
            return NULL;
        return item(clusternum).queryGroup();
    }

    IGroup *getGroup(unsigned clusternum)
    {
        IGroup *ret = queryGroup(clusternum);
        return LINK(ret);
    }

    unsigned copyNum(unsigned part,unsigned copy,unsigned maxparts, unsigned *replicate)
    {
        ForEachItem(i) {
            IGroup *g = queryGroup(i);
            unsigned cw = g?g->ordinality():1;
            unsigned mc = item(i).queryPartDiskMapping().numCopies(part,cw,maxparts);
            if (copy<mc) {
                if (replicate)
                    *replicate = copy;
                return i;
            }
            copy -= mc;
            if (singleclusteroverride)
                break;
        }
        return NotFound;
    }

    ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)
    {
        if (clusternum>=ordinality()||(singleclusteroverride&&clusternum)) 
            return defaultmapping;
        return item(clusternum).queryPartDiskMapping();
    }

    void updatePartDiskMapping(unsigned clusternum,const ClusterPartDiskMapSpec &spec)
    {
        if (clusternum<ordinality())
            item(clusternum).queryPartDiskMapping() = spec;
    }

    StringBuffer &getName(unsigned clusternum,StringBuffer &name)
    {
        if (clusternum<ordinality())
            item(clusternum).getClusterLabel(name);
        return name;
    }

    void setPreferred(const char *clusters,CDfsLogicalFileName &lfname)
    {
        unsigned nc = ordinality();
        if (nc<=1)
            return;
        StringBuffer cname;
        StringArray clustlist;
        if (lfname.getCluster(cname).length())
            clustlist.append(cname.str());
        unsigned i;
        if (clusters) {
            loop {
                const char *s = clusters;
                while (*s&&(*s!=','))
                    s++;
                if (s!=clusters) {
                    cname.clear().append(s-clusters,clusters);
                    for (i=0;i<clustlist.ordinality();i++)
                        if (strcmp(clustlist.item(i),cname.str())==0)
                            break;
                    if (i==clustlist.ordinality())
                        clustlist.append(cname.str());
                }
                if (!*s)
                    break;
                clusters = s+1;
            }
        }
        if (clustlist.ordinality()==0) {
            // sort by closest to this node
            const IpAddress &myip = queryMyNode()->endpoint();
            unsigned *d=new unsigned[nc];
            for (i=0;i<nc;i++) 
                d[i] = ipGroupDistance(myip,item(i).queryGroup());
            // bubble sort it - only a few
            for (i=0;i+1<nc;i++)
                for (unsigned j=0;j+i+1<nc;j++)
                    if (d[j+1]<d[j]) {
                        unsigned bd = d[j+1];
                        d[j+1] = d[j];
                        d[j] = bd;
                        swap(j,j+1);
                    }
            delete [] d;
            return;
        }
        Owned<IGroup> firstgrp;
        unsigned done = 0;
        StringBuffer name;
        StringBuffer name2;
        ForEachItemIn(ci,clustlist) {
            const char *cls = clustlist.item(ci);
            Owned<IGroup> grp = queryNamedGroupStore().lookup(cls); 
            if (!grp) {
                ERRLOG("IDistributedFile::setPreferred - cannot find cluster %s",cls);
                return;
            }
            if (!firstgrp.get())
                firstgrp.set(grp);
            for (i=done;i<nc;i++) {
                IClusterInfo &info=item(i);
                if (stricmp(info.getClusterLabel(name2.clear()).str(),name.str())==0) 
                    break;
                IGroup *grp2 = info.queryGroup();
                if (grp2&&(grp->compare(grp2)!=GRdisjoint)) 
                    break;
            }
            if (i<nc) {
                if (i) {
                    Linked<IClusterInfo> tmp = &item(i);
                    remove(i);
                    add(*tmp.getClear(),done);
                }
                done++;
                if (done+1>=nc)
                    break;
            }
        }
        if (done+1<nc) { // sort remaining by nearest to first group
            unsigned *d=new unsigned[nc]; // only use done to nc
            for (i=done;i<nc;i++)
                d[i] = groupDistance(firstgrp,item(i).queryGroup());
            // bubble sort it - only a few
            for (i=done;i+1<nc;i++)
                for (unsigned j=done;j+i+1<nc;j++)
                    if (d[j+1]<d[j]) {
                        unsigned bd = d[j+1];
                        d[j+1] = d[j];
                        d[j] = bd;
                        swap(j,j+1);
                    }
            delete [] d;
        }       
    }

    void setSingleClusterOnly(bool set=true)
    {
        singleclusteroverride = set;
    }

    unsigned numCopies(unsigned part,unsigned maxparts)
    {
        unsigned ret = 0;
        ForEachItem(i) {
            IGroup *g = queryGroup(i);
            unsigned cw = g?g->ordinality():1;
            ret += item(i).queryPartDiskMapping().numCopies(part,cw,maxparts);
            if (singleclusteroverride)
                break;
        }
        return ret;
    }

};


// Internal extension of transaction interface, used to manipulate and track transaction
interface IDistributedFileTransactionExt : extends IDistributedFileTransaction
{
    virtual IUserDescriptor *queryUser()=0;
    virtual void descend()=0;  // descend into a recursive call (can't autoCommit if depth is not zero)
    virtual void ascend()=0;   // ascend back from the deep, one step at a time
    virtual void autoCommit()=0; // if transaction not active, commit straight away
    virtual void addAction(CDFAction *action)=0;
    virtual void addFile(IDistributedFile *file)=0;
    virtual void ensureFile(IDistributedFile *file)=0;
    virtual void clearFile(IDistributedFile *file)=0;
    virtual void clearFiles()=0;
    virtual void noteAddSubFile(IDistributedSuperFile *super, const char *superName, IDistributedFile *sub) = 0;
    virtual void noteRemoveSubFile(IDistributedSuperFile *super, IDistributedFile *sub) = 0;
    virtual void noteSuperSwap(IDistributedSuperFile *super1, IDistributedSuperFile *super2) = 0;
    virtual void clearSubFiles(IDistributedSuperFile *super) = 0;
    virtual void noteRename(IDistributedFile *file, const char *newName) = 0;
    virtual void validateAddSubFile(IDistributedSuperFile *super, IDistributedFile *sub, const char *subName) = 0;
    virtual bool isSubFile(IDistributedSuperFile *super, const char *subFile, bool sub) = 0;
    virtual bool addDelayedDelete(CDfsLogicalFileName &lfn,unsigned timeoutms=INFINITE)=0; // used internally to delay deletes until commit
    virtual bool prepareActions()=0;
    virtual void retryActions()=0;
    virtual void runActions()=0;
    virtual void commitAndClearup()=0;
};

class CDistributedFileDirectory: public CInterface, implements IDistributedFileDirectory
{
    Owned<IUserDescriptor> defaultudesc;
    Owned<IDFSredirection> redirection;
    
    void resolveForeignFiles(IPropertyTree *tree,const INode *foreigndali);

protected: friend class CDistributedFile;
    StringAttr defprefclusters;
    unsigned defaultTimeout;

public:

    IMPLEMENT_IINTERFACE;

    CDistributedFileDirectory()
    {
        defaultTimeout = INFINITE;
        defaultudesc.setown(createUserDescriptor());
        redirection.setown(createDFSredirection());
    }
    unsigned queryDefaultTimeout() const { return defaultTimeout; }

    IDistributedFile *dolookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout);

    IDistributedFile *lookup(const char *_logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout);
    IDistributedFile *lookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout);

    /* createNew always creates an unnamed unattached distributed file
     * The caller must associated it with a name and credentials when it is attached (attach())
     */
    IDistributedFile *createNew(IFileDescriptor * fdesc, bool includeports=false);
    IDistributedSuperFile *createSuperFile(const char *logicalname,IUserDescriptor *user,bool interleaved,bool ifdoesnotexist,IDistributedFileTransaction *transaction=NULL);
    void removeSuperFile(const char *_logicalname, bool delSubs, IUserDescriptor *user, IDistributedFileTransaction *transaction);

    IDistributedFileIterator *getIterator(const char *wildname, bool includesuper,IUserDescriptor *user);
    IDFAttributesIterator *getDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive, bool includesuper,INode *foreigndali,unsigned foreigndalitimeout);
    IPropertyTreeIterator *getDFAttributesTreeIterator(const char *filters, DFUQResultField* localFilters, const char *localFilterBuf,
        IUserDescriptor *user, bool& allMatchingFilesReceived, INode *foreigndali,unsigned foreigndalitimeout);
    IDFAttributesIterator *getForeignDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive=true, bool includesuper=false, const char *foreigndali="", unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT)
    {
        Owned<INode> foreign;
        if (foreigndali&&*foreigndali) {
            SocketEndpoint ep(foreigndali);
            foreign.setown(createINode(ep));
        }
        return getDFAttributesIterator(wildname, user, recursive, includesuper,foreign,foreigndalitimeout);
    }

    IDFScopeIterator *getScopeIterator(IUserDescriptor *user, const char *subscope,bool recursive,bool includeempty);
    bool loadScopeContents(const char *scopelfn,StringArray *scopes,    StringArray *supers,StringArray *files, bool includeemptyscopes);

    IPropertyTree *getFileTree(const char *lname,IUserDescriptor *user,const INode *foreigndali,unsigned foreigndalitimeout,bool expandnodes=false, bool appendForeign=true);
    void setFileAccessed(CDfsLogicalFileName &dlfn, IUserDescriptor *user,const CDateTime &dt,const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IFileDescriptor *getFileDescriptor(const char *lname,IUserDescriptor *user,const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IDistributedFile *getFile(const char *lname,IUserDescriptor *user,const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

    bool exists(const char *_logicalname,IUserDescriptor *user,bool notsuper=false,bool superonly=false);
    bool existsPhysical(const char *_logicalname,IUserDescriptor *user);

    void addEntry(CDfsLogicalFileName &lfn,IPropertyTree *root,bool superfile, bool ignoreexists);
    bool removeEntry(const char *name, IUserDescriptor *user, IDistributedFileTransaction *transaction=NULL, unsigned timeoutms=INFINITE, bool throwException=false);
    void renamePhysical(const char *oldname,const char *newname,IUserDescriptor *user,IDistributedFileTransaction *transaction);
    void removeEmptyScope(const char *name);

    IDistributedSuperFile *lookupSuperFile(const char *logicalname,IUserDescriptor *user,IDistributedFileTransaction *transaction,unsigned timeout=INFINITE);

    int getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags);
    int getNodePermissions(const IpAddress &ip,IUserDescriptor *user,unsigned auditflags);
    int getFDescPermissions(IFileDescriptor *,IUserDescriptor *user,unsigned auditflags=0);
    void setDefaultUser(IUserDescriptor *user);
    IUserDescriptor* queryDefaultUser();

    DistributedFileCompareResult fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user);
    bool filePhysicalVerify(const char *lfn1,IUserDescriptor *user,bool includecrc,StringBuffer &errstr);
    void setDefaultPreferredClusters(const char *clusters);
    void fixDates(IDistributedFile *fil);

    GetFileClusterNamesType getFileClusterNames(const char *logicalname,StringArray &out); // returns 0 for normal file, 1 for

    bool isSuperFile( const char *logicalname, IUserDescriptor *user, INode *foreigndali, unsigned timeout);

    void promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user, unsigned timeout, StringArray &outunlinked);
    ISimpleSuperFileEnquiry * getSimpleSuperFileEnquiry(const char *logicalname,const char *title,IUserDescriptor *udesc,unsigned timeout);
    bool getFileSuperOwners(const char *logicalname, StringArray &owners);

    IDFSredirection & queryRedirection() { return *redirection; }

    static StringBuffer &getFileRelationshipXPath(StringBuffer &xpath, const char *primary, const char *secondary,const char *primflds,const char *secflds,
                                                const char *kind, const char *cardinality,  const bool *payload
        );
    void doRemoveFileRelationship( IRemoteConnection *conn, const char *primary,const char *secondary,const char *primflds,const char *secflds, const char *kind);
    void removeFileRelationships(const char *primary,const char *secondary, const char *primflds, const char *secflds, const char *kind);
    void addFileRelationship(const char *kind,const char *primary,const char *secondary,const char *primflds, const char *secflds,const char *cardinality,bool payload,IUserDescriptor *user,const char *description);
    IFileRelationshipIterator *lookupFileRelationships(const char *primary,const char *secondary,const char *primflds,const char *secflds,
                                                       const char *kind,const char *cardinality,const bool *payload,
                                                       const char *foreigndali,unsigned foreigndalitimeout);
    void removeAllFileRelationships(const char *filename);
    IFileRelationshipIterator *lookupAllFileRelationships(const char *filenames);

    void renameFileRelationships(const char *oldname,const char *newname,IFileRelationshipIterator *reliter, IUserDescriptor *user);

    bool publishMetaFileXML(const CDfsLogicalFileName &logicalname,IUserDescriptor *user);
    IFileDescriptor *createDescriptorFromMetaFile(const CDfsLogicalFileName &logicalname,IUserDescriptor *user);
    
    bool isProtectedFile(const CDfsLogicalFileName &logicalname, unsigned timeout) ;
    unsigned queryProtectedCount(const CDfsLogicalFileName &logicalname, const char *owner);                    
    bool getProtectedInfo(const CDfsLogicalFileName &logicalname, StringArray &names, UnsignedArray &counts);
    IDFProtectedIterator *lookupProtectedFiles(const char *owner=NULL,bool notsuper=false,bool superonly=false);
    IDFAttributesIterator* getLogicalFilesSorted(IUserDescriptor* udesc, DFUQResultField *sortOrder, const void *filterBuf, DFUQResultField *specialFilters,
            const void *specialFilterBuf, unsigned startOffset, unsigned maxNum, __int64 *cacheHint, unsigned *total, bool *allMatchingFilesReceived);

    void setFileProtect(CDfsLogicalFileName &dlfn,IUserDescriptor *user, const char *owner, bool set, const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
    }
};


// === Transactions
class CDFAction: public CInterface
{
    unsigned locked;
protected:
    IDistributedFileTransactionExt *transaction;
    IArrayOf<IDistributedFile> lockedFiles;
    DFTransactionState state;
    void addFileLock(IDistributedFile *file)
    {
        // derived's prepare must call this before locking
        lockedFiles.append(*LINK(file));
    }
    bool lock()
    {
        // Files most have been acquired already by derived's class prepare
        ForEachItemIn(i,lockedFiles)
        {
            try
            {
                lockedFiles.item(i).lockProperties(0);
            }
            catch (ISDSException *e)
            {
                if (SDSExcpt_LockTimeout != e->errorCode())
                    throw;
                e->Release();
                PROGLOG("CDFAction lock timed out on %s",lockedFiles.item(i).queryLogicalName());
                return false;
            }
            locked++;
        }
        return true;
    }
    void unlock()
    {
        for(unsigned i=0; i<locked; i++)
            lockedFiles.item(i).unlockProperties(state);
        locked = 0;
        lockedFiles.kill();
    }
public:
    CDFAction() : locked(0), state(TAS_NONE)
    {
        transaction = NULL;
    }
    // Clear all locked files (when re-using transaction on auto-commit mode)
    virtual ~CDFAction()
    {
        if (transaction)
            unlock();
    }
    void setTransaction(IDistributedFileTransactionExt *_transaction)
    {
        assertex(_transaction);
        assertex(!transaction);
        transaction = _transaction;
    }
    virtual bool prepare()=0;  // should call lock
    virtual void run()=0; // must override this
    // If some lock fails, call this
    virtual void retry()
    {
        state = TAS_RETRY;
        unlock();
    }
    // MORE: In the rare event of a commit failure, not all actions can be rolled back.
    // Since all actions today occur during "run", and since commit phases does very little,
    // this chance is minimal and will probably be caused by corrupted file descriptors.
    // The problem is that the state of the sub removals and the order in which they occur might not
    // be trivial on such a low level error, and there's no way to atomically do operations in SDS
    // at present time. We need more thought about this.
    virtual void commit()
    {
        state = TAS_SUCCESS;
        unlock();
    }
    virtual void rollback()
    {
        state = TAS_FAILURE;
        unlock();
    }
};

static void setUserDescriptor(Linked<IUserDescriptor> &udesc,IUserDescriptor *user)
{
    if (!user)
    {
#ifdef NULL_DALIUSER_STACKTRACE
        StringBuffer sb;
        if (user)
            user->getUserName(sb);
        if (sb.length()==0)
        {
            DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp setUserDescriptor() %d",__LINE__);
            //following debug code to be removed
            PrintStackReport();
        }
#endif
        user = queryDistributedFileDirectory().queryDefaultUser();
    }
    udesc.set(user);
}

static int getScopePermissions(const char *scopename,IUserDescriptor *user,unsigned auditflags)
{  // scope must be normalized already
    static bool permissionsavail=true;
    if (auditflags==(unsigned)-1) 
        return permissionsavail?1:0;
    int ret = 255;
    if (permissionsavail&&scopename&&*scopename&&((*scopename!='.')||scopename[1])) {
        if (!user)
        {
#ifdef NULL_DALIUSER_STACKTRACE
            DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp getScopePermissions() line %d",__LINE__);
            //following debug code to be removed
            PrintStackReport();
#endif
            user = queryDistributedFileDirectory().queryDefaultUser();
        }
        ret = querySessionManager().getPermissionsLDAP(queryDfsXmlBranchName(DXB_Scope),scopename,user,auditflags);
        if (ret<0) {
            if (ret==-1) {
                permissionsavail=false;
                ret = 255;
            }
            else 
                ret = 0;
        }
    }
    return ret;
}

static void checkLogicalScope(const char *scopename,IUserDescriptor *user,bool readreq,bool createreq)
{
    // scope must be normalized already
    if (!readreq&&!createreq)
        return;
    unsigned auditflags = 0;
    if (readreq)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_READ_WANTED);
    if (createreq)
        auditflags |= (DALI_LDAP_AUDIT_REPORT|DALI_LDAP_WRITE_WANTED);
#ifdef NULL_DALIUSER_STACKTRACE
    if (!user)
    {
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp checkLogicalScope() line %d",__LINE__);
        PrintStackReport();
    }
#endif

    int perm = getScopePermissions(scopename,user,auditflags);
    IDFS_Exception *e = NULL;
    if (readreq&&!HASREADPERMISSION(perm)) 
        e = new CDFS_Exception(DFSERR_LookupAccessDenied,scopename);
    else if (createreq&&!HASWRITEPERMISSION(perm)) 
        e = new CDFS_Exception(DFSERR_CreateAccessDenied,scopename);
    if (e) 
        throw e;
}

static bool checkLogicalName(CDfsLogicalFileName &dlfn,IUserDescriptor *user,bool readreq,bool createreq,bool allowquery,const char *specialnotallowedmsg)
{
    bool ret = true;
    if (dlfn.isMulti()) { //is temporary superFile?
        if (specialnotallowedmsg)
            throw MakeStringException(-1,"cannot %s a multi file name (%s)",specialnotallowedmsg,dlfn.get());
        if (!dlfn.isExpanded())
            dlfn.expand(user);//expand wildcards
        unsigned i = dlfn.multiOrdinality();
        while (--i)//continue looping even when ret is false, in order to check for illegal elements (foreigns/externals), and to check each scope permission
            ret = checkLogicalName((CDfsLogicalFileName &)dlfn.multiItem(i),user,readreq,createreq,allowquery,specialnotallowedmsg)&&ret;
    }
    else {
        if (specialnotallowedmsg) {
            if (dlfn.isExternal()) { 
                if (dlfn.isQuery()&&allowquery)
                    ret = false;
                else
                    throw MakeStringException(-1,"cannot %s an external file name (%s)",specialnotallowedmsg,dlfn.get());
            }
            if (dlfn.isForeign()) { 
                throw MakeStringException(-1,"cannot %s a foreign file name (%s)",specialnotallowedmsg,dlfn.get());
            }
        }
        StringBuffer scopes;
        dlfn.getScopes(scopes);
        checkLogicalScope(scopes.str(),user,readreq,createreq);
    }
    return ret;
}

/*
 * This class removes all files marked for deletion during transactions.
 *
 * TODO: the doDelete method re-acquires the lock to remove the files, and
 * that creates a window (between end of commit and deletion) where other
 * processes can acquire locks and blow things up. To fix this, you'd have
 * to be selective on what files you unlock during commit, so that you
 * can still keep an unified cache AND release the deletions later on.
 */
class CDelayedDelete: public CInterface
{
    CDfsLogicalFileName lfn;
    Linked<IUserDescriptor> user;
    unsigned timeoutms;

public:
    CDelayedDelete(CDfsLogicalFileName &_lfn,IUserDescriptor *_user,unsigned _timeoutms)
        : user(_user), timeoutms(_timeoutms)
    {
        lfn.set(_lfn);
    }

    void doDelete() // Throw on error!
    {
        const char *logicalname = lfn.get();
        if (!lfn.isExternal() && !checkLogicalName(lfn,user,true,true,true,"remove"))
            ThrowStringException(-1, "Logical Name fails for removal on %s", lfn.get());

        loop
        {
            // Transaction files have already been unlocked at this point, delete all remaining files
            Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn, user, true, false, true, NULL, SDS_SUB_LOCK_TIMEOUT);
            if (!file.get())
                return;
            StringBuffer reason;
            if (!file->canRemove(reason, false))
                ThrowStringException(-1, "Can't remove %s: %s", lfn.get(), reason.str());

            // This will do the right thing for either super-files and logical-files.
            try
            {
                file->detach(0); // 0 == timeout immediately if cannot get exclusive lock
                return;
            }
            catch (ISDSException *e)
            {
                switch (e->errorCode())
                {
                    case SDSExcpt_LockTimeout:
                    case SDSExcpt_LockHeld:
                        e->Release();
                        break;
                    default:
                        throw;
                }
            }
            file.clear();
            PROGLOG("CDelayedDelete: pausing");
            Sleep(SDS_TRANSACTION_RETRY/2+(getRandom()%SDS_TRANSACTION_RETRY));
        }
    }
};

class CDistributedFileTransaction: public CInterface, implements IDistributedFileTransactionExt
{
    class CTransactionFile : public CSimpleInterface
    {
        class HTMapping : public CInterface
        {
            IDistributedFile *file;
            StringAttr name;
        public:
            HTMapping(const char *_name, IDistributedFile *_file) : name(_name), file(_file) { }
            IDistributedFile &query() { return *file; }
            const char *queryFindString() const { return name; }
            const void *queryFindParam() const { return &file; }
        };
        class CSubFileIter : protected SuperHashIteratorOf<HTMapping>, implements IDistributedFileIterator
        {
            typedef SuperHashIteratorOf<HTMapping> PARENT;
        public:
            IMPLEMENT_IINTERFACE_USING(PARENT);
            CSubFileIter(OwningStringSuperHashTableOf<HTMapping> &table) : PARENT(table)
            {
            }
        // IDistributedFileIterator impl.
            virtual IDistributedFile &query()
            {
                HTMapping &map = PARENT::query();
                return map.query();
            }
            virtual bool first() { return PARENT::first(); }
            virtual bool isValid() { return PARENT::isValid(); }
            virtual bool next() { return PARENT::next(); }
            virtual StringBuffer &getName(StringBuffer &name)
            {
                HTMapping &map = PARENT::query();
                return name.append(map.queryFindString());
            }
        };
        CDistributedFileTransaction &owner;
        OwningStringSuperHashTableOf<HTMapping> subFilesByName;
        StringAttr name;
        Linked<IDistributedFile> file;
    public:
        CTransactionFile(CDistributedFileTransaction &_owner, const char *_name, IDistributedFile *_file) : owner(_owner), name(_name), file(_file)
        {
        }
        const char *queryName() const { return name; }
        IDistributedFile *queryFile() { return file; }
        IDistributedFileIterator *getSubFiles()
        {
            IDistributedSuperFile *super = file->querySuperFile();
            if (!super)
                return NULL;
            return new CSubFileIter(subFilesByName);
        }
        void clearSubs()
        {
            subFilesByName.kill();
        }
        unsigned numSubFiles() const { return subFilesByName.count(); }
        void noteAddSubFile(IDistributedFile *sub)
        {
            if (NULL == subFilesByName.find(sub->queryLogicalName()))
            {
                Owned<HTMapping> map = new HTMapping(sub->queryLogicalName(), sub);
                subFilesByName.replace(*map.getLink());
            }
        }
        void noteRemoveSubFile(IDistributedFile *sub)
        {
            HTMapping *map = subFilesByName.find(sub->queryLogicalName());
            if (map)
                verifyex(subFilesByName.removeExact(map));
        }
        bool find(const char *subFile, bool sub)
        {
            HTMapping *match = subFilesByName.find(subFile);
            if (match)
                return true;
            else if (sub)
            {
                SuperHashIteratorOf<HTMapping> iter(subFilesByName);
                ForEach(iter)
                {
                    HTMapping &map = iter.query();
                    IDistributedFile &file = map.query();
                    IDistributedSuperFile *super = file.querySuperFile();
                    if (super)
                    {
                        if (owner.isSubFile(super, subFile, sub))
                            return true;
                    }
                }
            }
            return false;
        }
        const void *queryFindParam() const { return &file; }
        const char *queryFindString() const { return name; }
    };
    CIArrayOf<CDFAction> actions;
    OwningSimpleHashTableOf<CTransactionFile, IDistributedFile *> trackedFiles;
    OwningStringSuperHashTableOf<CTransactionFile> trackedFilesByName;
    bool isactive;
    Linked<IUserDescriptor> udesc;
    CIArrayOf<CDelayedDelete> delayeddelete;
    // auto-commit only works at depth zero (for recursive calls)
    // MORE: Maybe this needs a context object (descend on c-tor, ascend on d-tor)
    // But we need all actions within transactions first to find out if there is
    // any exception to the rule used by addSubFile / removeSubFile
    unsigned depth;
    unsigned prepared;

    /* 'owner' is set if, transaction object is implicitly created, because none provided
     * The owner cannot be release or unlocked. The transaction can still retry if other files are locked,
     * so need to ensure 'owner' remains in tracked file cache.
     */
    IDistributedSuperFile *owner;


    void validateAddSubFile(IDistributedSuperFile *super, IDistributedFile *sub, const char *subName);
    CTransactionFile *queryCreate(const char *name, IDistributedFile *file, bool recreate=false)
    {
        Owned<CTransactionFile> trackedFile;
        if (!recreate)
            trackedFile.set(trackedFiles.find(file));
        if (!trackedFile)
        {
            StringBuffer tmp;
            name = normalizeLFN(name, tmp);
            trackedFile.setown(new CTransactionFile(*this, tmp.str(), file));
            trackedFiles.replace(*trackedFile.getLink());
            trackedFilesByName.replace(*trackedFile.getLink());
        }
        return trackedFile;
    }
    CTransactionFile *lookupTrackedFile(IDistributedFile *file)
    {
        return trackedFiles.find(file);
    }

public:
    IMPLEMENT_IINTERFACE;
    CDistributedFileTransaction(IUserDescriptor *user, IDistributedSuperFile *_owner=NULL)
        : isactive(false), depth(0), prepared(0), owner(_owner)
    {
        setUserDescriptor(udesc,user);
    }
    ~CDistributedFileTransaction()
    {
        // New files should be removed automatically if not committed
        // MORE - refactor cCreateSuperFileAction to avoid this
        if (isactive)
            rollback();
        assert(depth == 0);
    }
    void ensureFile(IDistributedFile *file)
    {
        if (!trackedFiles.find(file))
            addFile(file);
    }
    void addFile(IDistributedFile *file)
    {
        CTransactionFile *trackedFile = queryCreate(file->queryLogicalName(), file, false);
        // Also add subfiles to cache
        IDistributedSuperFile *sfile = file->querySuperFile();
        if (sfile)
        {
            Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator();
            ForEach(*iter)
            {
                IDistributedFile *f = &iter->query();
                trackedFile->noteAddSubFile(f);
                addFile(f);
            }
        }
    }
    void noteAddSubFile(IDistributedSuperFile *super, const char *superName, IDistributedFile *sub)
    {
        CTransactionFile *trackedSuper = queryCreate(superName, super);
        trackedSuper->noteAddSubFile(sub);
    }
    void noteRemoveSubFile(IDistributedSuperFile *super, IDistributedFile *sub)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (trackedSuper)
            trackedSuper->noteRemoveSubFile(sub);
    }
    virtual void clearSubFiles(IDistributedSuperFile *super)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (trackedSuper)
            trackedSuper->clearSubs();
    }
    virtual void noteSuperSwap(IDistributedSuperFile *super1, IDistributedSuperFile *super2)
    {
        CTransactionFile *trackedSuper1 = lookupTrackedFile(super1);
        CTransactionFile *trackedSuper2 = lookupTrackedFile(super2);
        assertex(trackedSuper1 && trackedSuper2);
        ICopyArrayOf<IDistributedFile> super1Subs, super2Subs;
        Owned<IDistributedFileIterator> iter = trackedSuper1->getSubFiles();
        ForEach(*iter)
            super1Subs.append(iter->query());
        trackedSuper1->clearSubs();
        iter.setown(trackedSuper2->getSubFiles());
        ForEach(*iter)
            super2Subs.append(iter->query());
        trackedSuper2->clearSubs();
        ForEachItemIn(s, super2Subs)
            trackedSuper1->noteAddSubFile(&super2Subs.item(s));
        ForEachItemIn(s2, super1Subs)
            trackedSuper2->noteAddSubFile(&super1Subs.item(s2));
    }
    void noteRename(IDistributedFile *file, const char *newName)
    {
        CTransactionFile *trackedFile = lookupTrackedFile(file);
        if (trackedFile)
        {
            trackedFiles.removeExact(trackedFile);
            trackedFilesByName.removeExact(trackedFile);
            trackedFile = queryCreate(newName, file);
        }
    }
    void addAction(CDFAction *action)
    {
        actions.append(*action); // takes ownership
        action->setTransaction(this);
    }
    void start()
    {
        if (isactive)
            throw MakeStringException(-1,"Transaction already started");
        clearFiles();
        actions.kill();
        isactive = true;
        prepared = 0;
        assertex(actions.ordinality()==0);
    }
    bool prepareActions()
    {
        prepared = 0;
        unsigned toPrepare = actions.ordinality();
        ForEachItemIn(i0,actions)
        {
            if (actions.item(i0).prepare())
                ++prepared;
            else
                break;
        }
        return prepared == toPrepare;
    }
    void retryActions()
    {
        clearFiles(); // clear all previously tracked pending file changes, e.g. renames, super file additions/removals
        while (prepared) // unlock for retry
            actions.item(--prepared).retry();
    }
    void runActions()
    {
        ForEachItemIn(i,actions)
            actions.item(i).run();
    }
    void commitActions()
    {
        while (actions.ordinality())  // if we get here everything should work!
        {
            Owned<CDFAction> action = &actions.popGet();
            action->commit();
        }
    }
    void commit()
    {
        if (!isactive)
            return;
        IException *rete=NULL;
        // =============== PREPARE AND RETRY UNTIL READY
        try
        {
            loop
            {
                if (prepareActions())
                    break;
                else
                    retryActions();
                PROGLOG("CDistributedFileTransaction: Transaction pausing");
                Sleep(SDS_TRANSACTION_RETRY/2+(getRandom()%SDS_TRANSACTION_RETRY)); 
            }
            runActions();
            commitAndClearup();
            return;
        }
        catch (IException *e)
        {
            rete = e;
        }
        rollback();
        throw rete;
    }
    void commitAndClearup()
    {
        // =============== COMMIT and CLEANUP
        commitActions();
        clearFiles();
        isactive = false;
        actions.kill();
        deleteFiles();
    }

    void rollback()
    {
        // =============== ROLLBACK AND CLEANUP
        while (actions.ordinality())
        {
            try
            {
                // we don't want to unlock what hasn't been locked
                // if an exception was thrown while locking, but we
                // do want to pop them all
                Owned<CDFAction> action = &actions.popGet();
                if (actions.ordinality()<prepared)
                    action->rollback();
            }
            catch (IException *e)
            {
                e->Release();
            }
        }
        actions.kill(); // should be empty
        clearFiles(); // release locks
        if (!isactive)
            return;
        isactive = false;
        // this we only want to do if active
        delayeddelete.kill();
    }

    void autoCommit()
    {
        // Recursive calls to transaction will not commit until
        // all calls have finished (gone back to zero depth)
        if (!depth && !isactive) {
            try {
                isactive = true;
                commit();
            }
            catch (IException *) {
                rollback();
                throw;
            }
        }
    }

    // Call this when you're recurring
    void descend()
    {
        depth++;
    }

    // Call this at the end of the block that started recursion
    void ascend()
    {
        assertex(depth);
        depth--;
    }

    IDistributedFile *findFile(const char *name)
    {
        StringBuffer tmp;
        name = normalizeLFN(name, tmp);
        CTransactionFile *trackedFile = trackedFilesByName.find(tmp.str());
        if (!trackedFile)
            return NULL;
        return trackedFile->queryFile();
    }
    IDistributedFile *lookupFile(const char *name,unsigned timeout)
    {
        IDistributedFile *ret = findFile(name);
        if (ret)
            return LINK(ret);
        else
        {
            ret = queryDistributedFileDirectory().lookup(name, udesc, false, false, false, this, timeout);
            if (ret)
                queryCreate(name, ret, true);
            return ret;
        }
    }

    IDistributedSuperFile *lookupSuperFile(const char *name, unsigned timeout)
    {
        IDistributedFile *f = findFile(name);
        if (f)
            return LINK(f->querySuperFile());
        else
        {
            IDistributedSuperFile *ret = queryDistributedFileDirectory().lookupSuperFile(name,udesc,this,timeout);
            if (ret)
                addFile(ret);
            return ret;
        }
    }

    virtual bool isSubFile(IDistributedSuperFile *super, const char *subFile, bool sub)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (!trackedSuper)
            return false;
        return trackedSuper->find(subFile, sub);
    }

public:
    bool active()
    {
        return isactive;
    }

    void clearFiles()
    {
        trackedFiles.kill();
        trackedFilesByName.kill();
        if (owner)
            addFile(owner); // ensure remains tracked
    }
    void clearFile(IDistributedFile *file)
    {
        CTransactionFile *trackedFile = lookupTrackedFile(file);
        IDistributedSuperFile *sfile = file->querySuperFile();
        if (trackedFile)
        {
            Owned<IDistributedFileIterator> iter = trackedFile->getSubFiles();
            if (iter)
            {
                ForEach(*iter)
                    clearFile(&iter->query());
            }
            trackedFiles.removeExact(trackedFile);
            trackedFilesByName.removeExact(trackedFile);
        }
    }

    IUserDescriptor *queryUser()
    {
        return udesc;
    }

    bool addDelayedDelete(CDfsLogicalFileName &lfn,unsigned timeoutms)
    {
        delayeddelete.append(*new CDelayedDelete(lfn,udesc,timeoutms));
        return true;
    }
    
    void deleteFiles()      // no rollback at this point
    {
        Owned<IMultiException> me = MakeMultiException("Transaction");
        ForEachItemIn(i,delayeddelete) {
            try {
                delayeddelete.item(i).doDelete();
            } catch (IException *e) {
                me->append(*e);
            }
        }
        delayeddelete.kill();
        if (me->ordinality())
            throw me.getClear();
    }
};

static bool recursiveCheckEmptyScope(IPropertyTree &ct)
{
    Owned<IPropertyTreeIterator> iter = ct.getElements("*");
    ForEach(*iter) {
        IPropertyTree &item = iter->query();
        const char *n = item.queryName();
        if (!n||(strcmp(n,queryDfsXmlBranchName(DXB_Scope))!=0))
            return false;
        if (!recursiveCheckEmptyScope(item))
            return false;
    }
    return true;
}


class CDFScopeIterator: public CInterface, implements IDFScopeIterator
{
    PointerArray scopes;
    unsigned index;
    IDistributedFileDirectory *dir;
    bool includeempty;

    void add(IPropertyTree &t, bool recursive, StringBuffer &name)
    {
        name.trim();
        size32_t nl = name.length();
        size32_t l=nl;
        if (nl) {
            name.append("::");
            l+=2;
        }
        Owned<IPropertyTreeIterator> iter = t.getElements(queryDfsXmlBranchName(DXB_Scope));
        ForEach(*iter) {
            IPropertyTree &ct = iter->query();
            if (includeempty||!recursiveCheckEmptyScope(ct)) {
                name.append(ct.queryProp("@name"));
                scopes.append(strdup(name.str()));
                if (recursive)
                    add(ct,recursive,name);
                name.setLength(l);
            }
        }
        name.setLength(nl);
    }

public:
    IMPLEMENT_IINTERFACE;

    CDFScopeIterator(IDistributedFileDirectory *_dir,const char *base,bool recursive, bool _includeempty,unsigned timeout) // attrib not yet implemented 
    {
        includeempty = _includeempty;
        dir = _dir;
        StringBuffer baseq;
        StringBuffer tmp;
        if (base&&*base) {
            CDfsLogicalFileName dlfn;
            dlfn.set(base,".");
            dlfn.makeScopeQuery(baseq,false);
        }
        {
            CConnectLock connlock("CDFScopeIterator",querySdsFilesRoot(),false,false,false,timeout);
            // could use CScopeConnectLock here probably 
            StringBuffer name;
            IPropertyTree *root = connlock.conn->queryRoot();
            if (baseq.length())
                root = root->queryPropTree(baseq.str());
            if (root)
                add(*root,recursive,name);
        }
        if (scopes.ordinality()>1)
            qsortvec(scopes.getArray(),scopes.ordinality(),strcompare);
        index = 0;
    }
    
    ~CDFScopeIterator()
    {
        ForEachItemIn(i,scopes) {
            free(scopes.item(i));
        }
    }
    

    bool first()
    {
        index = 0;
        return isValid();
    }

    bool next()
    {
        index++;
        return isValid();
    }
                            
    bool isValid()
    {
        return (index<scopes.ordinality());
    }

    const char *query()
    {   
        return (const char *)scopes.item(index);
    }
};

struct SerializeFileAttrOptions
{
    bool includeSuperOwner;
    //Add more as needed

    SerializeFileAttrOptions()
    {
        includeSuperOwner = false;
    }
};

class CDFAttributeIterator: public CInterface, implements IDFAttributesIterator
{
    IArrayOf<IPropertyTree> attrs;
    unsigned index;
public:
    IMPLEMENT_IINTERFACE;

    static MemoryBuffer &serializeFileAttributes(MemoryBuffer &mb, IPropertyTree &root, const char *name, bool issuper, SerializeFileAttrOptions& option)
    {
        StringBuffer buf;
        mb.append(name);
        if (issuper) {
            mb.append("!SF");
            mb.append(root.getPropInt("@numsubfiles",0));
            mb.append("");
        }
        else {
            mb.append(root.queryProp("@directory"));
            mb.append(root.getPropInt("@numparts",0));
            mb.append(root.queryProp("@partmask"));
        }
        mb.append(root.queryProp("@modified"));
        Owned<IPropertyTree> attrs = root.getPropTree("Attr");;
        Owned<IAttributeIterator> attriter;
        if (attrs)
            attriter.setown(attrs->getAttributes());
        unsigned count=0;
        size32_t countpos = mb.length();
        mb.append(count);
        if (attriter.get()&&attriter->first()) {
            do {
                mb.append(attriter->queryName());
                mb.append(attriter->queryValue());
                count++;
            } while (attriter->next());
        }
        const char *ps = root.queryProp("@group");
        if (ps&&*ps) {
            count++;
            mb.append("@group");
            mb.append(ps);
        }
        // add protected
        if (attrs) {
            Owned<IPropertyTreeIterator> piter = attrs->getElements("Protect");
            StringBuffer plist;
            ForEach(*piter) {
                const char *name = piter->get().queryProp("@name");
                if (name&&*name) {
                    unsigned count = piter->get().getPropInt("@count");
                    if (count) {
                        if (plist.length())
                            plist.append(',');
                        plist.append(name);
                        if (count>1)
                            plist.append(':').append(count);
                    }
                }
            }
            if (plist.length()) {
                count++;
                mb.append("@protect");
                mb.append(plist.str());
            }
        }
        if (option.includeSuperOwner) {
            //add superowners
            StringBuffer soList;
            Owned<IPropertyTreeIterator> superOwners = root.getElements("SuperOwner");
            ForEach(*superOwners) {
                IPropertyTree &superOwner = superOwners->query();
                const char *name = superOwner.queryProp("@name");
                if (name&&*name) {
                    if (soList.length())
                        soList.append(",");
                    soList.append(name);
                }
            }
            if (soList.length()) {
                count++;
                mb.append("@superowners");
                mb.append(soList.str());
            }
        }
        mb.writeDirect(countpos,sizeof(count),&count);
        return mb;
    }

    CDFAttributeIterator(MemoryBuffer &mb) // attrib not yet implemented
    {
        unsigned numfiles;
        mb.read(numfiles);
        while (numfiles--) {
            IPropertyTree *attr = getEmptyAttr();
            StringAttr val;
            unsigned n;
            mb.read(val);
            attr->setProp("@name",val.get());
            mb.read(val);
            if (stricmp(val,"!SF")==0) {
                mb.read(n);
                attr->setPropInt("@numsubfiles",n);
                mb.read(val);   // not used currently
            }
            else {
                attr->setProp("@directory",val.get());
                mb.read(n);
                attr->setPropInt("@numparts",n);
                mb.read(val);
                attr->setProp("@partmask",val.get());
            }
            mb.read(val);
            attr->setProp("@modified",val.get());
            unsigned count;
            mb.read(count);
            StringAttr at;
            while (count--) {
                mb.read(at);
                mb.read(val);
                attr->setProp(at.get(),val.get());
            }
            attrs.append(*attr);
        }
        index = 0;
    }

    CDFAttributeIterator(IArrayOf<IPropertyTree>& trees)
    {
        ForEachItemIn(t, trees)
            attrs.append(*LINK(&trees.item(t)));
        index = 0;
    }

    ~CDFAttributeIterator()
    {
        attrs.kill();
    }

    bool  first()
    {
        index = 0;
        return (attrs.ordinality()!=0);
    }

    bool  next()
    {
        index++;
        return (index<attrs.ordinality());
    }

    bool  isValid()
    {
        return (index<attrs.ordinality());
    }

    IPropertyTree &  query()
    {
        return attrs.item(index);
    }
};


class CDFProtectedIterator: public CInterface, implements IDFProtectedIterator
{
    StringAttr owner;
    StringAttr fn;
    unsigned count;
    bool issuper;
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTreeIterator> fiter;
    Owned<IPropertyTreeIterator> piter;
    unsigned defaultTimeout;

    bool notsuper;
    bool superonly;

    void fill()
    {
        IPropertyTree &t = fiter->query();
        fn.set(t.queryProp("OrigName"));
        IPropertyTree &pt = piter->query();
        owner.set(pt.queryProp("@name"));
        count = pt.getPropInt("@count");
    }

    void clear()
    {
        piter.clear();
        fiter.clear();
        conn.clear();
        issuper = false;
    }

public:
    IMPLEMENT_IINTERFACE;

    CDFProtectedIterator(const char *_owner,bool _notsuper,bool _superonly,unsigned _defaultTimeout)
        : owner(_owner)
    {
        count = 0;
        issuper = false;
        notsuper=_notsuper;
        superonly=_superonly;
        defaultTimeout = _defaultTimeout;
    }

    ~CDFProtectedIterator()
    {
        clear();
    }

    bool  first()
    {
        clear();
        conn.setown(querySDS().connect("Files",myProcessSession(),0, defaultTimeout));
        if (!conn) 
            return false;
        IPropertyTree *t = conn->queryRoot();
        if (!superonly) {
            fiter.setown(t->getElements("//File[Attr/Protect]", iptiter_remote));
            if (fiter->first()) {
                piter.setown(fiter->query().getElements("Attr/Protect"));
                if (piter->first()) {
                    fill();
                    return true;
                }
            }
        }
        if (!notsuper) {
            issuper = true;
            fiter.clear();
            fiter.setown(t->getElements("//SuperFile[Attr/Protect]", iptiter_remote));
            if (fiter->first()) {
                piter.setown(fiter->query().getElements("Attr/Protect"));
                if (piter->first()) {
                    fill();
                    return true;
                }
            }
        }
        clear();
        return false;
    }

    bool next()
    {
        if (!fiter.get())
            return false;
        if (piter->next()) {
            fill();
            return true;
        }
        loop {
            if (fiter->next()) {
                piter.setown(fiter->query().getElements("Attr/Protect"));
                if (piter->first()) {
                    fill();
                    return true;
                }
            }
            else if (!notsuper&&!issuper) {
                issuper = true;
                fiter.clear();
                fiter.setown(conn->queryRoot()->getElements("//SuperFile[Attr/Protect]", iptiter_remote));
                if (fiter->first()) {
                    piter.setown(fiter->query().getElements("Attr/Protect"));
                    if (piter->first()) {
                        fill();
                        return true;
                    }
                }
                else
                    break;
            }
            else
                break;
        }
        clear();
        return false;
    }
                            
    bool isValid()
    {
        return fiter.get()!=NULL;
    }

    const char *queryFilename()
    {
        return fn;
    }

    const char *queryOwner()
    {
        return owner;
    }

    unsigned getCount()
    {
        return count;
    }

    bool isSuper()
    {
        return issuper;
    }
};


// --------------------------------------------------------

class CDistributedFilePart: public CInterface, implements IDistributedFilePart
{
    unsigned partIndex;
    CDistributedFile &parent;
    Owned<IPropertyTree> attr;
    CriticalSection sect;
    StringAttr overridename;    // may or not be relative to directory
    bool            dirty;      // whether needs updating in tree 

    offset_t getSize(bool checkCompressed);

public:

    virtual void Link(void) const;
    virtual bool Release(void) const;
    void set(IPropertyTree *pt,FileClusterInfoArray &clusters,unsigned maxcluster);
    RemoteFilename &getFilename(RemoteFilename &ret,unsigned copy);
    void renameFile(IFile *file);
    IPropertyTree &queryAttributes();
    bool lockProperties(unsigned timems);
    void unlockProperties(DFTransactionState state);
    bool isHost(unsigned copy);
    offset_t getFileSize(bool allowphysical,bool forcephysical);
    offset_t getDiskSize(bool allowphysical,bool forcephysical);
    bool getModifiedTime(bool allowphysical,bool forcephysical,CDateTime &dt);
    bool getCrc(unsigned &crc); 
    unsigned getPhysicalCrc();  
    IPartDescriptor *getPartDescriptor();
    unsigned numCopies();
    INode *queryNode(unsigned copy);
    unsigned queryDrive(unsigned copy);
    StringBuffer &getPartName(StringBuffer &name);                          
    StringBuffer &getPartDirectory(StringBuffer &name,unsigned copy);
    const char *queryOverrideName() { return overridename; }    
    void clearOverrideName() 
    {
        if (overridename.get()&&overridename.length()) {
            dirty = true; 
            overridename.clear();
        }
    }

    unsigned bestCopyNum(const IpAddress &ip,unsigned rel=0);
    unsigned copyClusterNum(unsigned copy,unsigned *replicate=NULL);

    void childLink(void)        { CInterface::Link(); }                     
    bool childRelease(void)     { return CInterface::Release(); }

    CDistributedFilePart(CDistributedFile &_parent,unsigned _part,IPartDescriptor *pd);

    unsigned getPartIndex() 
    { 
        return partIndex; 
    }

    INode *getNode(unsigned copy)
    {
        INode *ret = queryNode(copy); 
        if (ret)
            return LINK(ret);
        return NULL;
    }

    void setAttr(IPropertyTree &pt)
    {
        attr.setown(createPTreeFromIPT(&pt));      // take a copy
        dirty = false;
    }

    IPropertyTree *queryAttr()
    {
        return attr;
    }

    inline CDistributedFile &queryParent()
    {
        return parent;
    }

    inline bool isDirty()
    {
        return dirty;
    }
    
    inline bool clearDirty()
    {
        bool ret=dirty;
        dirty = false;
        return ret;
    }
};

// --------------------------------------------------------

class CDistributedFilePartArray: public CIArrayOf<CDistributedFilePart>
{
public:
    virtual ~CDistributedFilePartArray()    // this shouldn't be needed - points to problem in CIArrayOf?
    {
        kill();
    }   
    void kill(bool nodel = false)
    {
        if (nodel)
            CIArrayOf<CDistributedFilePart>::kill(true);
        else {
            while (ordinality()) {
                CDistributedFilePart &part = popGet();
                part.Release();
            }
        }
    }   
};

// --------------------------------------------------------

/**
 * Base Iterator class for all iterator types. Implements basic iteration
 * logic and forces all iterators to behave similarly. This will simplify
 * future compatibility with STL containers/algorithms.
 *
 * INTERFACE needs to be extended from IIteratorOf<>
 * ARRAYTY need to be extended from IArrayOf<>
 */
template <class INTERFACE, class ARRAYTY>
class CDistributedFileIteratorBase: public CInterface, implements INTERFACE
{
protected:
    unsigned index;
    ARRAYTY list;

    virtual bool set() { return isValid(); }
public:
    IMPLEMENT_IINTERFACE;

    CDistributedFileIteratorBase()
        : index(0)
    {
    }
    virtual ~CDistributedFileIteratorBase()
    {
        list.kill();
    }

    bool first()
    {
        if (list.ordinality() == 0)
            return false;
        index = 0;
        return set();
    }

    bool next()
    {
        index++;
        set();
        return isValid();
    }

    bool isValid()
    {
        return (index < list.ordinality());
    }
};

/**
 * FilePart Iterator, used by files to manipulate its parts.
 */
class CDistributedFilePartIterator: public CDistributedFileIteratorBase<IDistributedFilePartIterator, CDistributedFilePartArray>
{
public:
    CDistributedFilePartIterator(CDistributedFilePartArray &parts, IDFPartFilter *filter)
    {
        ForEachItemIn(i,parts) {
            if (!filter||filter->includePart(i))
                list.append(*LINK(&parts.item(i)));
        }
    }

    CDistributedFilePartIterator()
    {
    }

    IDistributedFilePart & query()
    {
        return list.item(index);
    }

    CDistributedFilePartArray &queryParts()
    {
        return list;
    }
};

/**
 * File Iterator, used by directory to list file search results.
 */
class CDistributedFileIterator: public CDistributedFileIteratorBase<IDistributedFileIterator, PointerArray>
{
    Owned<IDistributedFile> cur;
    IDistributedFileDirectory *parent;
    Linked<IUserDescriptor> udesc;
    Linked<IDistributedFileTransaction> transaction;

    bool set()
    {
        while (isValid()) {
            cur.setown(parent->lookup(queryName(),udesc));
            if (cur)
                return true;
            index++;
        }
        return false;
    }

public:
    CDistributedFileIterator(IDistributedFileDirectory *_dir,const char *wildname,bool includesuper,IUserDescriptor *user,IDistributedFileTransaction *_transaction=NULL)
        : transaction(_transaction)
    {
        setUserDescriptor(udesc,user);
        if (!wildname||!*wildname)
            wildname = "*";
        parent = _dir;
        bool recursive = (stricmp(wildname,"*")==0);
        Owned<IDFAttributesIterator> attriter = parent->getDFAttributesIterator(wildname,user,recursive,includesuper,NULL);
        ForEach(*attriter) {
            IPropertyTree &pt = attriter->query();
            list.append(strdup(pt.queryProp("@name")));
        }
        index = 0;
        if (list.ordinality()>1)
            qsortvec(list.getArray(),list.ordinality(),strcompare);
    }

    const char *queryName()
    {
        return (const char *)list.item(index);
    }

    StringBuffer & getName(StringBuffer &name)
    {
        return name.append(queryName());
    }

    IDistributedFile & query()
    {
        return *cur;
    }
};

/**
 * SuperFile Iterator, used by CDistributedFile to list all its super-owners by name.
 */
class CDistributedSuperFileIterator: public CDistributedFileIteratorBase<IDistributedSuperFileIterator, StringAttrArray>
{
    CDistributedFileDirectory *parent;
    Linked<IUserDescriptor> udesc;
    Linked<IDistributedFileTransaction> transaction;
    Owned<IDistributedSuperFile> cur;
    Linked<IDistributedFile> owner;

public:
    CDistributedSuperFileIterator(IDistributedFile *_owner, CDistributedFileDirectory *_parent,IPropertyTree *root,IUserDescriptor *user, IDistributedFileTransaction *_transaction)
        : owner(_owner), transaction(_transaction)
    {
        setUserDescriptor(udesc,user);
        parent = _parent;
        if (root)
        {
            Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
            StringBuffer pname;
            ForEach(*iter)
            {
                iter->query().getProp("@name",pname.clear());
                if (pname.length())
                    list.append(* new StringAttrItem(pname.str()));
            }
        }
    }

    IDistributedSuperFile & query()
    {
        // NOTE: This used to include a do/while (!cur.get()&&next()) loop
        // this should never be needed but match previous semantics
        // throwing an exception now, to catch the error early on
        if (transaction.get())
            cur.setown(transaction->lookupSuperFile(queryName()));
        else
            cur.setown(parent->lookupSuperFile(queryName(),udesc,NULL));

        if (!cur.get())
            throw  MakeStringException(-1,"superFileIter: invalid super-file on query at %s", queryName());

        return *cur;
    }

    virtual const char *queryName()
    {
        if (isValid())
            return list.item(index).text.get();
        return NULL;
    }
};

//-----------------------------------------------------------------------------

inline void dfCheckRoot(const char *trc,Owned<IPropertyTree> &root,IRemoteConnection *conn) 
{
    if (root.get()!=conn->queryRoot()) {
        WARNLOG("%s - root changed",trc);
#ifdef _DEBUG
        PrintStackReport();
#endif
        root.setown(conn->getRoot());
    }
}

static bool setFileProtectTree(IPropertyTree &p,const char *owner, bool protect)
{
    bool ret = false;
    CDateTime dt;
    dt.setNow();
    if (owner&&*owner) {
        Owned<IPropertyTree> t = getNamedPropTree(&p,"Protect","@name",owner,false);
        if (t) {
            unsigned c = t->getPropInt("@count");
            if (protect) 
                c++;
            else {
                if (c>=1) {
                    p.removeTree(t);
                    c = 0;
                }
                else
                    c--;
            }
            if (c) {
                t->setPropInt("@count",c);
                StringBuffer str;
                t->setProp("@modified",dt.getString(str).str());
            }
        }
        else if (protect) {
            t.setown(addNamedPropTree(&p,"Protect","@name",owner));
            t->setPropInt("@count",1);
            StringBuffer str;
            t->setProp("@modified",dt.getString(str).str());
        }
        ret = true;
    }
    else if (!protect) {
        unsigned n=0;
        IPropertyTree *pt;
        while ((pt=p.queryPropTree("Protect[1]"))!=NULL) {
            p.removeTree(pt);
            n++;
        }
        if (n)
            ret = true;
    }
    return ret;
}

static bool checkProtectAttr(const char *logicalname,IPropertyTree *froot,StringBuffer &reason)
{
    Owned<IPropertyTreeIterator> wpiter = froot->getElements("Attr/Protect");
    bool prot = false;
    ForEach(*wpiter) {
        IPropertyTree &t = wpiter->query();
        if (t.getPropInt("@count")) {
            const char *wpname = t.queryProp("@name");
            if (!wpname||!*wpname)
                wpname = "<Unknown>";
            if (prot)
                reason.appendf(", %s",wpname);
            else {
                reason.appendf("file %s protected by %s",logicalname,wpname);
                prot = true;
            }
        }
    }
    return prot;
}

/**
 * A template class which implements the common methods of an IDistributedFile interface.
 * The actual interface (extended from IDistributedFile) is provided as a template argument.
 */
template <class INTERFACE>
class CDistributedFileBase : public CInterface, implements INTERFACE
{
protected:
    Owned<IPropertyTree> root;    
    Owned<IRemoteConnection> conn;                  // kept connected during lifetime for attributes
    CDfsLogicalFileName logicalName;
    CriticalSection sect;
    CDistributedFileDirectory *parent;
    unsigned proplockcount;
    unsigned transactionnest;
    Linked<IUserDescriptor> udesc;
    unsigned defaultTimeout;
    bool dirty;
    Owned<IRemoteConnection> superOwnerLock;
public:

    IPropertyTree *queryRoot() { return root; }

    CDistributedFileBase<INTERFACE>()
    {
        parent = NULL;
        proplockcount = 0;
        transactionnest = 0;
        defaultTimeout = INFINITE;
        dirty = false;
    }

    ~CDistributedFileBase<INTERFACE>()
    {
        root.clear();
    }

    void setSuperOwnerLock(IRemoteConnection *_superOwnerLock)
    {
        superOwnerLock.setown(_superOwnerLock);
    }

    unsigned setPropLockCount(unsigned _propLockCount)
    {
        unsigned prevPropLockCount = proplockcount;
        proplockcount = _propLockCount;
        return prevPropLockCount;
    }

    bool isCompressed(bool *blocked)
    {
        return ::isCompressed(queryAttributes(),blocked);
    }

    StringBuffer &getLogicalName(StringBuffer &lname)
    {
        lname.append(logicalName.get());
        return lname;
    }

    void setLogicalName(const char *lname)
    {
        logicalName.set(lname);
    }

    const char *queryLogicalName()
    {
        return logicalName.get();
    }

    IPropertyTree &queryAttributes()
    {
        IPropertyTree *t = root->queryPropTree("Attr");
        if (!t)
            t = root->setPropTree("Attr",createPTree("Attr")); // takes ownership
        return *t;
    }

protected:
    class CFileChangeWriteLock
    {
        IRemoteConnection *conn;
        unsigned timeoutMs, prevMode;
    public:
        CFileChangeWriteLock(IRemoteConnection *_conn, unsigned _timeoutMs)
            : conn(_conn), timeoutMs(_timeoutMs)
        {
            if (conn)
            {
                prevMode = conn->queryMode();
                unsigned newMode = (prevMode & ~RTM_LOCKBASIC_MASK) | RTM_LOCK_WRITE;
                conn->changeMode(RTM_LOCK_WRITE, timeoutMs);
            }
            else
                prevMode = RTM_NONE;
        }
        ~CFileChangeWriteLock()
        {
            if (conn)
                conn->changeMode(prevMode, timeoutMs);
        }
        void clear() { conn = NULL; }
    };
    IPropertyTree *closeConnection(bool removeFile)
    {
        Owned<IPropertyTree> detachedRoot = createPTreeFromIPT(root);
        root.clear();
        if (conn)
        {
            conn->close(removeFile);
            conn.clear();
        }
        return detachedRoot.getClear();
    }
    IPropertyTree *resetFileAttr(IPropertyTree *prop=NULL)
    {
        if (prop)
            return root->setPropTree("Attr", prop);

        root->removeProp("Attr");
        return NULL;
    }
    void updateFS(const CDfsLogicalFileName &lfn, unsigned timeoutMs)
    {
        // Update the file system
        removeFileEmptyScope(lfn, timeoutMs);
        // MORE: We shouldn't have to update all relationships if we had done a better job making sure
        // that all correct relationships were properly cleaned up
        queryDistributedFileDirectory().removeAllFileRelationships(lfn.get());
    }

public:
    bool isAnon()
    {
        return !logicalName.isSet();
    }

    /*
     *  Change connection to write-mode, allowing multiple writers only on the same instance.
     *  Returns true if the lock was lost at least once before succeeding, hinting that some
     *  resources might need reload (like sub-files list, etc).
     *
     *  WARN: This is not thread-safe
     *
     *  @deprecated : use DistributedFilePropertyLock instead, when possible
     */
    bool lockProperties(unsigned timeoutms)
    {
        bool reload = false;
        if (timeoutms==INFINITE)
            timeoutms = defaultTimeout;
        if (proplockcount++==0)
        {
            if (conn)
            {
                conn->rollback(); // changes chouldn't be done outside lock properties
#ifdef TRACE_LOCKS
                PROGLOG("lockProperties: pre safeChangeModeWrite(%x)",(unsigned)(memsize_t)conn.get());
#endif
                try
                {
                    if (0 == timeoutms)
                        conn->changeMode(RTM_LOCK_WRITE, 0, true); // 0 timeout, test and fail immediately if contention
                    else
                        safeChangeModeWrite(conn,queryLogicalName(),reload,timeoutms);
                }
                catch(IException *)
                {
                    proplockcount--;
                    dfCheckRoot("lockProperties",root,conn);
                    if (reload)
                        dirty = true; // safeChangeModeWrite unlocked, and reload will be need if retried
                    throw;
                }
                if (dirty) // a previous attempt unlocked and did not reload
                {
                    dirty = false;
                    if (!reload) // if reload=true, safeChangeModeWrite has just reloaded, so no need to again here
                    {
                        conn->reload();
                        reload = true;
                    }
                }
#ifdef TRACE_LOCKS
                PROGLOG("lockProperties: done safeChangeModeWrite(%x)",(unsigned)(memsize_t)conn.get());
                LogRemoteConn(conn);
#endif
                dfCheckRoot("lockProperties",root,conn);
            }
        }
        return reload;
    }

    /*
     * Change connection back to read mode on the last unlock. There should never be
     * an uneven number of locks/unlocks, since that will leave the connection with
     * the DFS locked until the instance's destruction.
     *
     * WARN: This is not thread-safe
     *
     *  @deprecated : use DistributedFilePropertyLock instead, when possible
     */
    void unlockProperties(DFTransactionState state=TAS_NONE)
    {
        savePartsAttr();
        if (--proplockcount==0) {
            if (conn) {
                // Transactional logic, if any
                switch(state) {
                case TAS_SUCCESS:
                    conn->commit();
                    break;
                case TAS_FAILURE:
                    conn->rollback();
                    break;
                case TAS_RETRY:
                    conn->changeMode(RTM_NONE,defaultTimeout,true);
                    return;
                // TAS_NONE, do nothing
                }
#ifdef TRACE_LOCKS
                PROGLOG("unlockProperties: pre changeMode(%x)",(unsigned)(memsize_t)conn.get());
#endif
                conn->changeMode(RTM_LOCK_READ,defaultTimeout,true);
#ifdef TRACE_LOCKS
                PROGLOG("unlockProperties: post changeMode(%x)",(unsigned)(memsize_t)conn.get());
                LogRemoteConn(conn);
#endif
                dfCheckRoot("unlockProperties",root,conn);
            }
        }
    }

    bool getModificationTime(CDateTime &dt)
    {
        StringBuffer str;
        if (!root->getProp("@modified",str))
            return false;
        dt.setString(str.str());
        return true;
    }

    void setModificationTime(const CDateTime &dt)
    {
        DistributedFilePropertyLock lock(this);
        if (dt.isNull())
            root->removeProp("@modified");
        else {
            StringBuffer str;
            root->setProp("@modified",dt.getString(str).str());
        }
        root->removeProp("@verified");
    }

    void setModified()
    {
        CDateTime dt;
        dt.setNow();
        setModificationTime(dt);
    }

    virtual StringBuffer &getECL(StringBuffer &buf)
    {
        MemoryBuffer mb;
        if (queryAttributes().getPropBin("ECLbin",mb))
            buf.deserialize(mb);
        else
            queryAttributes().getProp("ECL",buf);
        return buf;
    }

    virtual void setECL(const char *ecl)
    {
        DistributedFilePropertyLock lock(this);
        IPropertyTree &p = queryAttributes();
#ifdef PACK_ECL
        p.removeProp("ECL");
        if (!ecl||!*ecl)
            p.removeProp("ECLbin");
        else {
            MemoryBuffer mb;    // could be better
            StringBuffer buf(ecl);
            buf.serialize(mb);
            root->setPropBin("ECLbin",mb.length(),mb.toByteArray());
        }
#else
        p.setProp("ECL",ecl);
#endif
    }

    void setProtect(const char *owner, bool protect, unsigned timems)
    {
        if (logicalName.isForeign()) {
            parent->setFileProtect(logicalName,udesc,owner,protect);
        }
        else {
            bool ret=false;
            if (conn) {
                DistributedFilePropertyLock lock(this);
                IPropertyTree &p = queryAttributes();
                CDateTime dt;
                dt.setNow();
                if (setFileProtectTree(p,owner,protect))
                    conn->commit();
                dfCheckRoot("setProtect.1",root,conn);
            }
            else 
                ERRLOG("setProtect - cannot protect %s (no connection in file)",owner?owner:"");
        }
    }

    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *_transaction)
    {
        CriticalBlock block(sect);
        return new CDistributedSuperFileIterator(this,parent,root,udesc,_transaction);
    }

    virtual void checkFormatAttr(IDistributedFile *sub, const char* exprefix="")
    {
        // check file has same (or similar) format
        IPropertyTree &superProp = queryAttributes();
        IPropertyTree &subProp = sub->queryAttributes();
        if (!exprefix)
            exprefix = "CheckFormatAttr";

        bool superBlocked = false;
        bool superComp = ::isCompressed(superProp,&superBlocked);
        bool subBlocked = false;
        bool subComp = ::isCompressed(subProp,&subBlocked);
        // FIXME: this may fail if an empty superfile added to a compressed superfile
        if (superComp != subComp)
            throw MakeStringException(-1,"%s: %s's compression setting (%s) is different than %s's (%s)",
                    exprefix, sub->queryLogicalName(), (subComp?"compressed":"uncompressed"),
                    queryLogicalName(), (superComp?"compressed":"uncompressed"));
        if (superBlocked != subBlocked)
            throw MakeStringException(-1,"%s: %s's blocked setting (%s) is different than %s's (%s)",
                    exprefix, sub->queryLogicalName(), (subBlocked?"blocked":"unblocked"),
                    queryLogicalName(), (superBlocked?"blocked":"unblocked"));

#ifdef SUBFILE_COMPATIBILITY_CHECKING
        bool subSoft = subProp.hasProp("_record_layout");
        bool superSoft = superProp.hasProp("_record_layout");
        if (superSoft != subSoft)
            throw MakeStringException(-1,"%s: %s's record layout (%s) is different than %s's (%s)",
                    exprefix, sub->queryLogicalName(), (subSoft?"dynamic":"fixed"),
                    queryLogicalName(), (superSoft?"dynamic":"fixed"));
        // If they don't, they must have the same size
        if (!superSoft) {
            unsigned superSize = superProp.getPropInt("@recordSize",0);
            unsigned subSize = subProp.getPropInt("@recordSize",0);
            // Variable length files (CSV, etc) have zero record size
            if (superSize && subSize && (superSize != subSize))
                throw MakeStringException(-1,"%s: %s's record size (%d) is different than %s's (%d)",
                        exprefix, sub->queryLogicalName(), subSize, queryLogicalName(), superSize);
        }
        StringBuffer superFmt;
        bool superHasFmt = superProp.getProp("@format",superFmt);
        StringBuffer subFmt;
        bool subHasFmt = subProp.getProp("@format",subFmt);
        if (subHasFmt && superHasFmt)
            if (strcmp(normalizeFormat(superFmt).str(),normalizeFormat(subFmt).str()) != 0)
                throw MakeStringException(-1,"%s: %s's format (%s) is different than %s's (%s)",
                        exprefix, sub->queryLogicalName(), superFmt.str(),
                        queryLogicalName(), subFmt.str());
#endif
        bool superLocal = superProp.getPropBool("@local",false);
        bool subLocal = subProp.getPropBool("@local",false);
        if (subLocal != superLocal)
            throw MakeStringException(-1,"%s: %s's local setting (%s) is different than %s's (%s)",
                    exprefix, sub->queryLogicalName(), (subLocal?"local":"global"),
                    queryLogicalName(), (superLocal?"local":"global"));

        int superRepO = superProp.getPropInt("@replicateOffset",1);
        int subRepO = subProp.getPropInt("@replicateOffset",1);
        if (subRepO != superRepO)
            throw MakeStringException(-1,"%s: %s's replication offset (%d) is different than %s's (%d)",
                    exprefix, sub->queryLogicalName(), subRepO,
                    queryLogicalName(), superRepO);
    }

    virtual void getSuperOwners(StringArray &owners)
    {
        if (root)
        {
            StringBuffer owner;
            Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
            ForEach (*iter)
            {
                iter->query().getProp("@name", owner.clear());
                if (owner.length())
                {
                    if (NotFound == owners.find(owner))
                        owners.append(owner);
                }
            }
        }
    }

    void linkSuperOwner(const char *superfile,bool link)
    {
        if (!superfile||!*superfile)
            return;
        if (conn)
        {
            CFileSuperOwnerLock attrLock;
            if (0 == proplockcount)
                verifyex(attrLock.init(logicalName, conn, defaultTimeout, "CDistributedFile::linkSuperOwner"));
            Owned<IPropertyTree> t = getNamedPropTree(root,"SuperOwner","@name",superfile,false);
            if (t && !link)
                root->removeTree(t);
            else if (!t && link)
                t.setown(addNamedPropTree(root,"SuperOwner","@name",superfile));
        }
        else 
            ERRLOG("linkSuperOwner - cannot link to %s (no connection in file)",superfile);
    }

    void setAccessed()                              
    {
        CDateTime dt;
        dt.setNow();
        setAccessedTime(dt);
    }

    virtual StringBuffer &getColumnMapping(StringBuffer &mapping)
    {
        queryAttributes().getProp("@columnMapping",mapping);
        return mapping;
    }

    virtual void setColumnMapping(const char *mapping)
    {
        DistributedFilePropertyLock lock(this);
        if (!mapping||!*mapping) 
            queryAttributes().removeProp("@columnMapping");
        else
            queryAttributes().setProp("@columnMapping",mapping);
    }

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
    }

    // MORE - simplify this, after removing CLightWeightSuperFileConn
    bool canModify(StringBuffer &reason)
    {
        return !checkProtectAttr(logicalName.get(),root,reason);
    }

    bool canRemove(StringBuffer &reason,bool ignoresub=false)
    {
        CriticalBlock block(sect);
        if (!canModify(reason))
            return false;
        const char *logicalname = logicalName.get();
        if (!logicalname||!*logicalname) {
            reason.appendf("empty filename");
            return false;
        }
        if (logicalName.isQuery()) {
            reason.appendf("%s is query",logicalname);
            return false;
        }
        if (logicalName.isForeign()) {
            reason.appendf("%s is foreign",logicalname);
            return false;
        }
        if (logicalName.isMulti()) {
            reason.appendf("%s is multi",logicalname);
            return false;
        }
        if (!ignoresub) {
            // And has super owners
            Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
            if (iter->first()) {
                reason.append("Cannot remove file ").append(logicalname).append(" as owned by SuperFile(s): ");
                loop {
                    reason.append(iter->query().queryProp("@name"));
                    if (!iter->next())
                        break;
                    reason.append(", ");
                }
                return false;
            }
        }
        return true;
    }


    virtual const char *queryDefaultDir() = 0;
    virtual unsigned numParts() = 0;
    virtual IDistributedFilePart &queryPart(unsigned idx) = 0;
    virtual IDistributedFilePart* getPart(unsigned idx) = 0;
    virtual void savePartsAttr(bool force=false) = 0;
    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) = 0;
    virtual IDistributedSuperFile *querySuperFile() = 0;
    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)=0;
    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec)=0;
    virtual void enqueueReplicate()=0;
    virtual bool getAccessedTime(CDateTime &dt) = 0;                            // get date and time last accessed (returns false if not set)
    virtual void setAccessedTime(const CDateTime &dt) = 0;                      // set date and time last accessed
};

class CDistributedFile: public CDistributedFileBase<IDistributedFile>
{
protected:
    CDistributedFilePartArray parts;            // use queryParts to access
    CriticalSection sect;
    StringAttr directory;
    StringAttr partmask;
    FileClusterInfoArray clusters;

    void savePartsAttr(bool force)
    {
        CriticalBlock block (sect);
        IPropertyTree *pt;
        if (parts.ordinality()==1) { // single part saved as part
            if (parts.item(0).clearDirty()||force) {
                CDistributedFilePart &part = parts.item(0);
                while ((pt=root->queryPropTree("Part[1]"))!=NULL)
                    root->removeTree(pt);
                pt = createPTreeFromIPT(part.queryAttr());
                pt->setPropInt("@num",1);
                const char *grp = root->queryProp("@group");
                if (!grp||!*grp) {
                    StringBuffer eps;
                    pt->addProp("@node",part.queryNode(0)->endpoint().getUrlStr(eps).str()); // legacy
                }
                const char *override = part.queryOverrideName();
                if (override&&*override)
                    pt->setProp("@name",override);
                else {
                    pt->removeProp("@name");
                    const char *mask=queryPartMask();
                    if (mask&&*mask) {
                        StringBuffer tmp;
                        expandMask(tmp,mask,0,1);
                        pt->setProp("@name",tmp.str());
                    }
                }
                root->setPropTree("Part",pt);
            }
        }
        else {
            unsigned n = parts.ordinality();
            unsigned i1;
            for (i1=0;i1<n;i1++) {
                if (parts.item(i1).clearDirty()||force) {
                    MemoryBuffer mb;
                    CriticalBlock block (sect);
                    ForEachItemIn(i2,parts)
                        serializePartAttr(mb,parts.item(i2).queryAttr());
                    root->setPropBin("Parts",mb.length(),mb.toByteArray());
                    while ((pt=root->queryPropTree("Part[1]"))!=NULL)
                        root->removeTree(pt);
                    break;
                }
            }
            while (i1<n)
                parts.item(i1++).clearDirty();
        }
    }
    void detach(unsigned timeoutMs=INFINITE, bool removePhysicals=true)
    {
        // Removes either a cluster in case of multi cluster file or the whole File entry from DFS

        assert(proplockcount == 0 && "CDistributedFile detach: Some properties are still locked");
        assertex(!isAnon()); // not attached!

        if (removePhysicals)
        {
            // Avoid removing physically when there is no physical representation
            if (logicalName.isMulti())
                removePhysicals = false;
        }

        StringBuffer clusterName;
        Owned<IFileDescriptor> fileDescCopy;
#ifdef EXTRA_LOGGING
        PROGLOG("CDistributedFile::detach(%s)",logicalName.get());
        LOGPTREE("CDistributedFile::detach root.1",root);
#endif
        {
            CriticalBlock block(sect); // JCSMORE - not convinced this is still necessary
            CFileChangeWriteLock writeLock(conn, timeoutMs);

            logicalName.getCluster(clusterName);

            // copy file descriptor before altered, used by physical file removal routines
            if (removePhysicals)
            {
                MemoryBuffer mb;
                Owned<IFileDescriptor> fdesc = getFileDescriptor(clusterName);
                fdesc->serialize(mb);
                fileDescCopy.setown(deserializeFileDescriptor(mb));
            }

            bool removeFile=true;
            if (clusterName.length())
            {
                // Remove just cluster specified, unless it's the last, in which case detach below will remove File entry.
                if (clusters.ordinality()>1)
                {
                    if (removeCluster(clusterName.str()))
                        removeFile=false;
                    else
                        ThrowStringException(-1, "Cluster %s not present in file %s", clusterName.str(), logicalName.get());
                }
            }
            if (removeFile)
            {
                // check can remove, e.g. cannot if this is a subfile of a super
                StringBuffer reason;
                if (!canRemove(reason))
                    throw MakeStringException(-1,"detach: %s", reason.str());
            }
            // detach this IDistributeFile

            /* JCSMORE - In 'removeFile=true' case, this should really delete before release exclusive lock.
             */
            writeLock.clear();
            root.setown(closeConnection(removeFile));
            // NB: The file is now unlocked
            if (removeFile && !logicalName.isExternal())
                updateFS(logicalName, parent->queryDefaultTimeout());

            logicalName.clear();
        }
        // NB: beyond unlock
        if (removePhysicals)
        {
            CriticalBlock block(physicalChange);
            Owned<IMultiException> exceptions = MakeMultiException("CDistributedFile::detach");
            removePhysicalPartFiles(fileDescCopy, exceptions);
            if (exceptions->ordinality())
                throw exceptions.getClear();
        }
    }
    bool removePhysicalPartFiles(IFileDescriptor *fileDesc, IMultiException *mexcept)
    {
        if (logicalName.isExternal())
        {
            if (logicalName.isQuery())
                return false;
        }
        if (logicalName.isForeign())
            throw MakeStringException(-1,"cannot remove a foreign file (%s)",logicalName.get());

        class casyncfor: public CAsyncFor
        {
            IFileDescriptor *fileDesc;
            CriticalSection errcrit;
            IMultiException *mexcept;
        public:
            bool ok;
            bool islazy;
            casyncfor(IFileDescriptor *_fileDesc, IMultiException *_mexcept)
            {
                fileDesc = _fileDesc;
                ok = true;
                islazy = false;
                mexcept = _mexcept;
            }
            void Do(unsigned i)
            {
                IPartDescriptor *part = fileDesc->queryPart(i);
                unsigned nc = part->numCopies();
                for (unsigned copy = 0; copy < nc; copy++)
                {
                    RemoteFilename rfn;
                    part->getFilename(copy, rfn);
                    Owned<IFile> partfile = createIFile(rfn);
                    StringBuffer eps;
                    try
                    {
                        unsigned start = msTick();
                        if (!partfile->remove()&&(copy==0)&&!islazy) // only warn about missing primary files
                            LOG(MCwarning, unknownJob, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                        else
                        {
                            unsigned t = msTick()-start;
                            if (t>5*1000)
                                LOG(MCwarning, unknownJob, "Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getUrlStr(eps).str(), t/1000);
                        }
                    }
                    catch (IException *e)
                    {
                        CriticalBlock block(errcrit);
                        if (mexcept)
                            mexcept->append(*e);
                        else
                        {
                            StringBuffer s("Failed to remove file part ");
                            s.append(partfile->queryFilename()).append(" from ");
                            rfn.queryEndpoint().getUrlStr(s);
                            EXCLOG(e, s.str());
                            e->Release();
                        }
                        ok = false;
                    }
                }
            }
        } afor(fileDesc, mexcept);
        afor.islazy = fileDesc->queryProperties().getPropBool("@lazy");
        afor.For(fileDesc->numParts(),10,false,true);
        return afor.ok;
    }

protected: friend class CDistributedFilePart;
    CDistributedFilePartArray &queryParts()
    {
        return parts;
    }
public:
    IMPLEMENT_IINTERFACE;

    CDistributedFile(CDistributedFileDirectory *_parent, IRemoteConnection *_conn,const CDfsLogicalFileName &lname,IUserDescriptor *user) // takes ownership of conn
    {
        setUserDescriptor(udesc,user);
        logicalName.set(lname);
        parent = _parent;
        conn.setown(_conn);
        CClustersLockedSection sect(logicalName, false);
        root.setown(conn->getRoot());
        root->queryBranch(".");     // load branch
#ifdef EXTRA_LOGGING
        LOGPTREE("CDistributedFile.a root",root);
#endif
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile.a fdesc",fdesc);
#endif
        setFileAttrs(fdesc,false);
        setClusters(fdesc);
        setPreferredClusters(_parent->defprefclusters);
        setParts(fdesc,false);
        //shrinkFileTree(root); // enable when safe!
    }

    CDistributedFile(CDistributedFileDirectory *_parent, IFileDescriptor *fdesc, IUserDescriptor *user, bool includeports)
    {
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile.b fdesc",fdesc);
#endif
        parent = _parent;
        root.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
        root->setPropTree("ClusterLock", createPTree());
//      fdesc->serializeTree(*root,IFDSF_EXCLUDE_NODES);
        setFileAttrs(fdesc,true);
        setClusters(fdesc);
        setPreferredClusters(_parent->defprefclusters);
        saveClusters();
        setParts(fdesc,true);
        udesc.set(user);
#ifdef EXTRA_LOGGING
        LOGPTREE("CDistributedFile.b root.1",root);
#endif
        offset_t totalsize=0;
        unsigned checkSum = ~0;
        bool useableCheckSum = true;
        MemoryBuffer pmb;
        unsigned n = fdesc->numParts();
        for (unsigned i=0;i<n;i++) {
            IPropertyTree *partattr = &fdesc->queryPart(i)->queryProperties();
            if (!partattr)
            {
                totalsize = (unsigned)-1;
                useableCheckSum = false;
            }
            else
            {
                offset_t psz;
                if (totalsize!=(offset_t)-1) {
                    psz = (offset_t)partattr->getPropInt64("@size", -1);
                    if (psz==(offset_t)-1)
                        totalsize = psz;
                    else
                        totalsize += psz;
                }
                if (useableCheckSum) {
                    unsigned crc;
                    if (fdesc->queryPart(i)->getCrc(crc))
                        checkSum ^= crc;
                    else
                        useableCheckSum = false;
                }
            }
        }
        shrinkFileTree(root);
        if (totalsize!=(offset_t)-1)
            queryAttributes().setPropInt64("@size", totalsize);
        if (useableCheckSum)
            queryAttributes().setPropInt64("@checkSum", checkSum);
        setModified();
#ifdef EXTRA_LOGGING
        LOGPTREE("CDistributedFile.b root.2",root);
#endif
    }

    void killParts()
    {
        ForEachItemIn(i,parts)
            parts.item(i).childRelease();
        parts.kill(true);
    }

    ~CDistributedFile()
    {
        assert(proplockcount == 0 && "CDistributedFile destructor: Some properties are still locked");
        if (conn)
            conn->rollback();       // changes should always be done in locked properties
        killParts();
        clusters.kill();
    }

    IFileDescriptor *getFileDescriptor(const char *_clusterName)
    {
        CriticalBlock block (sect);
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
        fdesc->setTraceName(logicalName.get());
        StringArray cnames;
        if (_clusterName&&*_clusterName)
        {
            StringAttr clusterName = _clusterName;
            clusterName.toLowerCase();
            cnames.append(clusterName);
        }
        else
            getClusterNames(cnames);
        fdesc->setClusterOrder(cnames,_clusterName&&*_clusterName);
        return fdesc.getClear();
    }

    void setFileAttrs(IFileDescriptor *fdesc,bool save)
    {
        directory.set(fdesc->queryDefaultDir());
        partmask.set(fdesc->queryPartMask());
        const char *lfn = logicalName.get();
        if (lfn&&*lfn) {
            if (partmask.isEmpty()) {
                StringBuffer mask;
                getPartMask(mask,lfn,0);
                partmask.set(mask);
            }
        }
        if (!save)
            return;
        if (directory.isEmpty())
            root->removeProp("@directory");
        else
            root->setProp("@directory",directory);
        if (partmask.isEmpty())
            root->removeProp("@partmask");
        else
            root->setProp("@partmask",partmask);
        IPropertyTree *t = &fdesc->queryProperties();
        if (isEmptyPTree(t))
            resetFileAttr();
        else
            resetFileAttr(createPTreeFromIPT(t));
    }

    void setClusters(IFileDescriptor *fdesc)
    {
        clusters.clear();
        unsigned nc = fdesc->numClusters();
        if (nc) {
            for (unsigned i=0;i<nc;i++) {
                StringBuffer cname;
                StringBuffer clabel;
                IClusterInfo &cluster = *createClusterInfo(
                                    fdesc->getClusterGroupName(i,cname,NULL).str(),
                                    fdesc->queryClusterGroup(i),
                                    fdesc->queryPartDiskMapping(i),
                                    &queryNamedGroupStore()
                                    );
#ifdef EXTRA_LOGGING
                PROGLOG("setClusters(%d,%s)",i,cname.str());
#endif

                if (!cluster.queryGroup(&queryNamedGroupStore())) {
                    ERRLOG("IDistributedFileDescriptor cannot set cluster for %s",logicalName.get());
                }
                clusters.append(cluster);
            }
        }
        else
            ERRLOG("No cluster specified for %s",logicalName.get());
    }

    unsigned numClusters()
    {
        return clusters.ordinality();
    }

    unsigned findCluster(const char *clustername)
    {
        return clusters.find(clustername);
    }

    unsigned getClusterNames(StringArray &clusternames)
    {
        return clusters.getNames(clusternames);
    }

    void reloadClusters()
    {
        // called from CClustersLockedSection
        if (!CDistributedFileBase<IDistributedFile>::conn)
            return;
        assertex(CDistributedFileBase<IDistributedFile>::proplockcount==0); // cannot reload clusters if properties locked
        CDistributedFileBase<IDistributedFile>::conn->reload(); // should only be cluster changes but a bit dangerous
        IPropertyTree *t = CDistributedFileBase<IDistributedFile>::conn->queryRoot();  // NB not CDistributedFileBase<IDistributedFile>::queryRoot();

        if (!t)
            return;
        clusters.clear();
        getClusterInfo(*t,&queryNamedGroupStore(),0,clusters);
    }
    
    void saveClusters()
    {
        // called from CClustersLockedSection
        IPropertyTree *t;
        if (CDistributedFileBase<IDistributedFile>::conn)
            t = CDistributedFileBase<IDistributedFile>::conn->queryRoot();
        else
            t = CDistributedFileBase<IDistributedFile>::queryRoot(); //cache
        if (!t)
            return;
        IPropertyTree *pt;
        IPropertyTree *tc = CDistributedFileBase<IDistributedFile>::queryRoot(); //cache
        IPropertyTree *t0 = t;
        StringBuffer grplist;
        // the following is complicated by fact there is a cache of the file branch
        loop {
            while ((pt=t->queryPropTree("Cluster[1]"))!=NULL)
                t->removeTree(pt);
            ForEachItemIn(i,clusters) {
                IPropertyTree *pt = createPTree("Cluster");
                clusters.item(i).serializeTree(pt,IFDSF_EXCLUDE_GROUPS);
                if (!isEmptyPTree(pt)) {
                    t->addPropTree("Cluster",pt);
                    if (t==t0) {
                        StringBuffer clabel;
                        clusters.item(i).getClusterLabel(clabel);
                        if (clabel.length()) {
                            if (grplist.length())
                                grplist.append(',');
                            grplist.append(clabel);
                        }
                    }
                }
                else
                    WARNLOG("CFileClusterOwner::saveClusters - empty cluster");
            }
            if (grplist.length()) 
                t->setProp("@group",grplist.str());
            else
                t->removeProp("@group");
            t->setPropInt("@numclusters",clusters.ordinality());
            t->setProp("@directory", directory);
            if (t==tc)
                break;
            t = tc; // now fix cache
        }
        if (CDistributedFileBase<IDistributedFile>::conn)
            CDistributedFileBase<IDistributedFile>::conn->commit(); // should only be cluster changes but a bit dangerous
    }

    void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec)
    {
        if (!clustername&&!*clustername)
            return;
        CClustersLockedSection cls(CDistributedFileBase<IDistributedFile>::logicalName, true);
        reloadClusters();
        if (findCluster(clustername)!=NotFound) {
            IDFS_Exception *e = new CDFS_Exception(DFSERR_ClusterAlreadyExists,clustername);
            throw e;
        }
        Owned<IClusterInfo> cluster = createClusterInfo(clustername,NULL,mspec,&queryNamedGroupStore());
        if (cluster->queryGroup(&queryNamedGroupStore())) {
            clusters.append(*cluster.getClear());
        }
        else {
            IDFS_Exception *e = new CDFS_Exception(DFSERR_ClusterNotFound,clustername);
            throw e;
        }
        saveClusters();
    }

    bool removeCluster(const char *clustername)
    {
        CClustersLockedSection cls(CDistributedFileBase<IDistributedFile>::logicalName, true);
        reloadClusters();
        unsigned i = findCluster(clustername);
        if (i!=NotFound) {
            if (clusters.ordinality()==1)
                throw MakeStringException(-1,"CFileClusterOwner::removeCluster cannot remove sole cluster %s",clustername);
            // If the cluster is the 'default' one we need to update the directory too
            StringBuffer oldBaseDir;
            char pathSepChar = getPathSepChar(directory.get());
            DFD_OS os = SepCharBaseOs(pathSepChar);
            clusters.item(i).getBaseDir(oldBaseDir, os);
            unsigned oldLen = oldBaseDir.length();
            clusters.remove(i);
            if (oldLen && strncmp(directory, oldBaseDir, oldLen)==0 && (directory[oldLen]==pathSepChar || directory[oldLen]=='\0'))
            {
                StringBuffer newBaseDir;
                clusters.item(0).getBaseDir(newBaseDir, os);
                newBaseDir.append(directory.get() + oldBaseDir.length());
                directory.set(newBaseDir);
            }
            saveClusters();
            return true;
        }
        return false;
    }

    void setPreferredClusters(const char *clusterlist)
    {
        clusters.setPreferred(clusterlist,CDistributedFileBase<IDistributedFile>::logicalName);
    }


    INode *queryNode(unsigned idx,unsigned copy)
    {
        unsigned rep;
        unsigned cluster = copyClusterNum(idx,copy,&rep);
        if (cluster==NotFound)
            return queryNullNode();
        unsigned nn;
        unsigned dn;
        IGroup *grp = clusters.queryGroup(cluster);
        if (!grp)
            return queryNullNode();
        if (!clusters.item(cluster).queryPartDiskMapping().calcPartLocation (idx, numParts(),rep, grp->ordinality(), nn, dn))
            return queryNullNode();
        return &grp->queryNode(nn);
    }

    unsigned queryDrive(unsigned idx,unsigned copy,const char *dir)
    {
        // this is odd routine
        unsigned dn = dir?getPathDrive(dir):0;
        if (dn)
            return dn;
        unsigned rep;
        unsigned cluster = copyClusterNum(idx,copy,&rep);
        if (cluster==NotFound)
            return 0;
        unsigned nn;
        IGroup *grp = clusters.queryGroup(cluster);
        if (!grp)
            return 0;
        if (!clusters.item(cluster).queryPartDiskMapping().calcPartLocation (idx, numParts(),rep, grp->ordinality(), nn, dn))
            return 0;
        return dn;
    }


    StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name)
    {
        return clusters.getName(clusternum,name);
    }

    unsigned copyClusterNum(unsigned part, unsigned copy,unsigned *replicate)
    {
        return clusters.copyNum(part,copy, numParts(),replicate);
    }

    ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)
    {
        assertex(clusternum<clusters.ordinality());
        return clusters.queryPartDiskMapping(clusternum);
    }

    void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec)
    {
        CClustersLockedSection cls(CDistributedFileBase<IDistributedFile>::logicalName, true);
        reloadClusters();
        unsigned i = findCluster(clustername);
        if (i!=NotFound) {
            clusters.updatePartDiskMapping(i,spec);
            saveClusters();
        }
    }

    IGroup *queryClusterGroup(unsigned clusternum)
    {
        return clusters.queryGroup(clusternum);
    }

    StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name)
    {
        return clusters.item(clusternum).getGroupName(name, &queryNamedGroupStore());
    }

    virtual unsigned numCopies(unsigned partno)
    {
        return clusters.numCopies(partno,numParts());
    }

    void setSingleClusterOnly()
    {
        clusters.setSingleClusterOnly();
    }

    unsigned numClusterCopies(unsigned cnum,unsigned partnum)
    {
        IClusterInfo &cluster = clusters.item(cnum);
        IGroup *grp = cluster.queryGroup();
        return cluster.queryPartDiskMapping().numCopies(partnum,grp?grp->ordinality():1,numParts());

    }

    void adjustClusterDir(unsigned partno,unsigned copy, StringBuffer &path)
    {
        // this corrects the directory for a copy

        // assumes default dir matches one of clusters
        unsigned rep=0;
        unsigned cluster = NotFound;
        const char *ds = path.str();
        unsigned nc = clusters.ordinality();
        if (nc>1) {
            StringAttr matched;
            StringAttr toadd;
            unsigned i=0;
            bool c = 0;
            int cp = (int)copy;
            while (i<nc) {
                StringBuffer dcmp;
                clusters.item(i).getBaseDir(dcmp,SepCharBaseOs(getPathSepChar(ds)));    // no trailing sep
                const char *t = dcmp.str();
                const char *d = ds;
                while (*d&&(*t==*d)) {
                    d++;
                    t++;
                }
                if (!*t&&(!*d||isPathSepChar(*d))&&(dcmp.length()>matched.length()))
                    matched.set(dcmp);
                unsigned mc = numClusterCopies(i,partno);
                if ((cp>=0)&&(cp<(int)mc)) {
                    toadd.set(dcmp);
                    rep = (unsigned)cp;
                    cluster = i;
                }
                cp -= mc;
                i++;
            }
            if (!matched.isEmpty()&&!toadd.isEmpty()&&(strcmp(matched,toadd)!=0)) {
                StringBuffer tmp(toadd);
                tmp.append(ds+matched.length());
                path.swapWith(tmp);
            }
        }
        else {
            rep = copy;
            cluster = 0;
        }
// now set replicate
        if (cluster!=NotFound) {
            unsigned n;
            unsigned d;
            ClusterPartDiskMapSpec& mspec = clusters.item(cluster).queryPartDiskMapping();
            mspec.calcPartLocation(partno,numParts(),rep,clusters.queryGroup(cluster)?clusters.queryGroup(cluster)->ordinality():numParts(),n,d);
            if ((d==1) && (mspec.flags&CPDMSF_overloadedConfig) && mspec.defaultReplicateDir.length())
                path.set(mspec.defaultReplicateDir.get());
            else
                setReplicateFilename(path,d);
        }
    }

    void setParts(IFileDescriptor *fdesc,bool save)
    {
        unsigned np = fdesc->numParts();
        for (unsigned i = 0;i<np;i++) {
            CDistributedFilePart &part = *new CDistributedFilePart(*this,i,fdesc->queryPart(i));
            parts.append(part);
        }
        if (save) {
            root->setPropInt("@numparts",parts.ordinality());
            savePartsAttr(true);
        }
    }

    unsigned numParts()
    {
        return parts.ordinality();
    }

    IDistributedFilePart &queryPart(unsigned idx)
    {
        if (idx<parts.ordinality())
            return queryParts().item(idx);
        return *(IDistributedFilePart *)NULL;
    }

    IDistributedFilePart* getPart(unsigned idx)
    {
        if (idx>=parts.ordinality())
            return NULL;
        IDistributedFilePart *ret = &queryParts().item(idx);
        return LINK(ret);
    }

    IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL)
    {
        return new CDistributedFilePartIterator(queryParts(),filter);
    }

    void rename(const char *_logicalname,IUserDescriptor *user)
    {
        StringBuffer prevname;
        Owned<IFileRelationshipIterator> reliter;
        // set prevname
        if (!isAnon()) {
            getLogicalName(prevname);
            try {
                IFileRelationshipIterator *iter = parent->lookupAllFileRelationships(prevname.str());
                reliter.setown(iter);
            }
            catch (IException *e) {
                EXCLOG(e,"CDistributedFile::rename");
                e->Release();
            }
            detachLogical();
        }
        attach(_logicalname,user);
        if (prevname.length()) {
            DistributedFilePropertyLock lock(this);
            IPropertyTree &pt = queryAttributes();
            StringBuffer list;
            if (pt.getProp("@renamedFrom",list)&&list.length())
                list.append(',');
            pt.setProp("@renamedFrom",list.append(prevname).str());
        }
        if (reliter.get()) {
            // add back any relationships with new name
            parent->renameFileRelationships(prevname.str(),_logicalname,reliter,user);
        }
    }


    const char *queryDefaultDir()
    {
        CriticalBlock block (sect);
        return directory.get();
    }

    const char *queryPartMask()
    {
        CriticalBlock block (sect);
        if (partmask.isEmpty()) {
            assertex(root);
            partmask.set(root->queryProp("@partmask"));
        }
        return partmask.get();
    }

    bool isAnon() 
    { 
        return (!logicalName.isSet());
    }

    void attach(const char *_logicalname,IUserDescriptor *user)
    {
        CriticalBlock block (sect);
        assertex(isAnon()); // already attached!
        logicalName.set(_logicalname);
        if (!checkLogicalName(logicalName,user,true,true,true,"attach"))
            return; // query
#ifdef EXTRA_LOGGING
        PROGLOG("CDistributedFile::attach(%s)",_logicalname);
        LOGPTREE("CDistributedFile::attach root.1",root);
#endif
        parent->addEntry(logicalName,root.getClear(),false,false);
        killParts();
        clusters.kill();
        CFileLock fcl;
        verifyex(fcl.init(logicalName, DXB_File, RTM_LOCK_READ, defaultTimeout, "CDistributedFile::attach"));
        conn.setown(fcl.detach());
        root.setown(conn->getRoot());
        root->queryBranch(".");     // load branch
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
        setFileAttrs(fdesc,false);
        setClusters(fdesc);
        setParts(fdesc,false);
        setUserDescriptor(udesc, user);
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile::attach fdesc",fdesc);
        LOGPTREE("CDistributedFile::attach root.2",root);
#endif
    }

    /*
     * Internal method (not in IDistributedFile interface) that is used
     * when renaming files (so don't delete the physical representation).
     *
     * This is also used during CPPUINT tests, so we need to make them public
     * only when tests are enabled (ie. non-production mode).
     *
     * See removeLogical()
     */
public:
    void detachLogical(unsigned timeoutms=INFINITE)
    {
        detach(timeoutms, false);
    }

public:
    virtual void detach(unsigned timeoutMs=INFINITE)
    {
        detach(timeoutMs, true);
    }

    bool existsPhysicalPartFiles(unsigned short port)
    {
        unsigned width = numParts();
        CriticalSection errcrit;
        class casyncfor: public CAsyncFor
        {
            IDistributedFile *file;
            unsigned short port;
            CriticalSection &errcrit;
            unsigned width;
        public:
            bool ok;
            casyncfor(IDistributedFile *_file,unsigned _width,unsigned short _port,CriticalSection &_errcrit)
                : errcrit(_errcrit)
            {
                file = _file;
                port = _port;
                ok = true;
                width = _width;
                ok = true;
            }
            void Do(unsigned i)
            {
                {
                    CriticalBlock block(errcrit);
                    if (!ok)
                        return;
                }
                Owned<IDistributedFilePart> part = file->getPart(i);
                unsigned nc = part->numCopies();
                for (unsigned copy = 0; copy < nc; copy++)
                {
                    RemoteFilename rfn;
                    part->getFilename(rfn,copy);
                    if (port)
                        rfn.setPort(port); // if daliservix
                    Owned<IFile> partfile = createIFile(rfn);
                    try
                    {
                        if (partfile->exists())
                            return;
                    }
                    catch (IException *e)
                    {
                        CriticalBlock block(errcrit);
                        StringBuffer s("Failed to find file part ");
                        s.append(partfile->queryFilename()).append(" on ");
                        rfn.queryEndpoint().getUrlStr(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                }
                CriticalBlock block(errcrit);
                ok = false;
            }
        } afor(this,width,port,errcrit);
        afor.For(width,10,false,true);
        return afor.ok;
    }

    // This method takes an existing physical directory path for a logical file
    // and a constructed path to the same logical file created in this context
    // and deduces the original base path e.g. /var/lib/HPCCSystems/hpcc-data/thor
    // This is necessary, because there is no not enough context to directly fetch the
    // original base path to construct new paths for the rename
    bool getBase(const char *oldPath, const char *thisPath, StringBuffer &baseDir)
    {
        const char *oldEnd = oldPath+strlen(oldPath)-1;
        const char *thisEnd = thisPath+strlen(thisPath)-1;
        if (isPathSepChar(*oldEnd))
            oldEnd--;
        if (isPathSepChar(*thisEnd))
            thisEnd--;
        const char *oldP = oldEnd, *thisP = thisEnd;
        loop {
            if (oldP==oldPath || thisP==thisPath)
                break;
            if (*oldP != *thisP) {
                // unless last was separator, continue until find one
                if (isPathSepChar(*(oldP+1)))
                    oldP++;
                else {
                    while (oldP != oldPath && (!isPathSepChar(*oldP)))
                        oldP--;
                }
                baseDir.append(oldP-oldPath, oldPath);
                return true;
            }
            --oldP;
            --thisP;
        }
        return false;
    }

    bool renamePhysicalPartFiles(const char *newname,
                                 const char *cluster,
                                 IMultiException *mexcept,
                                 const char *newbasedir)
    {
        // cluster TBD
        unsigned width = numParts();
        StringBuffer newdir;
        StringBuffer newmask;
        const char *diroverride = NULL;
        char psc = getPathSepChar(directory.get());
        DFD_OS os = SepCharBaseOs(psc);
        StringBuffer basedir;
        if (newbasedir)
            diroverride = newbasedir;

        const char *myBase = queryBaseDirectory(grp_unknown, 0, os);
        StringBuffer baseDir, newPath;
        makePhysicalPartName(logicalName.get(), 0, 0, newPath, false, os, diroverride);
        if (!getBase(directory, newPath, baseDir))
            baseDir.append(myBase); // getBase returns false, if directory==newPath, so have common base
        getPartMask(newmask,newname,width);
        if (newmask.length()==0)
            return false;
        makePhysicalPartName(newname, 0, 0, newPath.clear(), false, os, diroverride);
        if (newPath.length()==0)
            return false;
        if (isPathSepChar(newPath.charAt(newPath.length()-1)))
            newPath.setLength(newPath.length()-1);
        newPath.remove(0, strlen(myBase));
        newdir.append(baseDir).append(newPath);
        StringBuffer fullname;
        CIArrayOf<CIStringArray> newNames;
        unsigned i;
        for (i=0;i<width;i++) {
            newNames.append(*new CIStringArray);
            CDistributedFilePart &part = parts.item(i);
            for (unsigned copy=0; copy<part.numCopies(); copy++) {
                makePhysicalPartName(newname, i+1, width, newPath.clear(), false, os, myBase);
                newPath.remove(0, strlen(myBase));

                StringBuffer copyDir(baseDir);
                adjustClusterDir(i, copy, copyDir);
                fullname.clear().append(copyDir).append(newPath);

                PROGLOG("fullname = %s", fullname.str());
                newNames.item(i).append(fullname);
            }
        }
        // NB: the code below, specifically deals with 1 primary + 1 replicate
        // it will need refactoring if it's to deal with multiple clusters/copies

        // first check file doestn't exist for any new part

        CriticalSection crit;
        class casyncforbase: public CAsyncFor
        {
        protected:
            CriticalSection &crit;
            CIArrayOf<CIStringArray> &newNames;
            IDistributedFile *file;
            unsigned width;
            IMultiException *mexcept;
            bool *ignoreprim;
            bool *ignorerep;
        public:
            bool ok;
            bool * doneprim;
            bool * donerep;
            IException *except;

            casyncforbase(IDistributedFile *_file,CIArrayOf<CIStringArray> &_newNames,unsigned _width,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : newNames(_newNames),crit(_crit)
            {
                width = _width;
                file = _file;
                ok = true;
                mexcept = _mexcept;
                doneprim = (bool *)calloc(sizeof(bool),width);
                donerep = (bool *)calloc(sizeof(bool),width);
                except = NULL;
                ignoreprim = _ignoreprim;
                ignorerep = _ignorerep;
            }
            ~casyncforbase()
            {
                free(doneprim);
                free(donerep);
            }

            virtual bool doPart(IDistributedFilePart *,bool,RemoteFilename &,RemoteFilename &, bool &)
#ifdef _WIN32
            {
                assertex(!"doPart"); // stupid microsoft error
                return false;
            }
#else
             = 0;
#endif          
            void Do(unsigned idx)
            {
                {
                    CriticalBlock block(crit);
                    if (!ok)
                        return;
                }
                Owned<IDistributedFilePart> part = file->getPart(idx);
                unsigned copies = part->numCopies();
                for (int copy = copies-1; copy>=0; copy--)
                {
                    if ((copy==0)&&ignoreprim&&ignoreprim[idx])
                        continue;
                    if ((copy!=0)&&ignorerep&&ignorerep[idx])
                        continue;
                    bool pok=false;
                    IException *ex = NULL;
                    RemoteFilename oldrfn;
                    part->getFilename(oldrfn,(unsigned)copy);
                    const char *newfn = newNames.item(idx).item(copy);
                    if (!newfn||!*newfn)
                        continue;
                    RemoteFilename newrfn;
                    newrfn.setPath(part->queryNode(copy)->endpoint(),newfn);
                    try {
                        pok = doPart(part,copy!=0,oldrfn,newrfn,(copy==0)?doneprim[idx]:donerep[idx]);

                    }
                    catch (IException *e) {
                        EXCLOG(e, NULL);
                        ex = e;
                    }
                    CriticalBlock block(crit);
                    if (!pok||ex) {
                        ok = false;
                        if (ex) {
                            StringBuffer s("renamePhysicalPartFiles ");
                            s.append(file->queryLogicalName()).append(" part ").append(newfn);
                            EXCLOG(ex, s.str());
                            if (mexcept) 
                                mexcept->append(*ex);
                            else {
                                if (except)
                                    ex->Release();
                                else
                                    except = ex;
                            }
                        }
                    }
                }
            }
        };
        class casyncfor1: public casyncforbase
        {
        public:
            casyncfor1(IDistributedFile *_file,CIArrayOf<CIStringArray> &_newNames,unsigned _width,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : casyncforbase(_file,_newNames,_width,_mexcept,_crit,_ignoreprim,_ignorerep)
            {
            }
            bool doPart(IDistributedFilePart *part,bool isrep,RemoteFilename &oldrfn,RemoteFilename &newrfn, bool &done)
            {
                done = false;
                Owned<IFile> src = createIFile(oldrfn);
                if (src->exists()) 
                    done = true;
                else {
                    StringBuffer s;
                    oldrfn.getRemotePath(s);
                    WARNLOG("renamePhysicalPartFiles: %s doesn't exist",s.str());
                    return true;
                }
                Owned<IFile> dest = createIFile(newrfn);
                StringBuffer newname;
                newrfn.getRemotePath(newname);
                if (dest->exists()) {
                    IDFS_Exception *e = new CDFS_Exception(DFSERR_PhysicalPartAlreadyExists,newname.str());
                    throw e;
                }
                // check destination directory exists
                StringBuffer newdir;
                splitDirTail(newname.str(),newdir);
                Owned<IFile> destdir = createIFile(newdir.str());
                destdir->createDirectory();
                return true;
            }

        } afor1 (this,newNames,width,mexcept,crit,NULL,NULL);
        afor1.For(width,10,false,true);
        if (afor1.except)
            throw afor1.except; // no recovery needed
        if (!afor1.ok)
            return false; // no recovery needed
        MemoryAttr ignorebuf;
        bool *ignoreprim = (bool *)ignorebuf.allocate(width*sizeof(bool)*2);
        bool *ignorerep = ignoreprim+width;
        for (i=0;i<width;i++) {
            if (afor1.donerep[i]) {
                ignorerep[i] = false;
                ignoreprim[i] = !afor1.doneprim[i];
            }
            else if (afor1.doneprim[i]) {
                ignorerep[i] = true;
                ignoreprim[i] = false;
            }
            else {
                StringBuffer s(queryLogicalName());
                s.append(" Part ").append(i+1);
                IDFS_Exception *e = new CDFS_Exception(DFSERR_PhysicalPartDoesntExist,s.str());
                throw e;
            }
        }
        // now do the rename
        class casyncfor2: public casyncforbase
        {
        public:
            casyncfor2(IDistributedFile *_file,CIArrayOf<CIStringArray> &_newNames,unsigned _width,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : casyncforbase(_file,_newNames,_width,_mexcept,_crit,_ignoreprim,_ignorerep)
            {
            }
            bool doPart(IDistributedFilePart *part,bool isrep,RemoteFilename &oldrfn,RemoteFilename &newrfn, bool &done)
            {
                done = false;
                StringBuffer oldfn;
                oldrfn.getRemotePath(oldfn);
                StringBuffer newfn;
                newrfn.getRemotePath(newfn);
                Owned<IFile> f = createIFile(oldrfn);
                if (!isrep||f->exists()) { // ignore non-existant replicates
                    f->move(newfn.str());
                    PROGLOG("Succeeded rename %s to %s",oldfn.str(),newfn.str());
                }
                done = true;
                return true;;
            }

        } afor2 (this,newNames,width,mexcept,crit,ignoreprim,ignorerep);
        afor2.For(width,10,false,true);
        if (afor2.ok) {
            // now rename directory and partmask
            DistributedFilePropertyLock lock(this);
            root->setProp("@directory",newdir.str());
            root->setProp("@partmask",newmask.str());
            partmask.set(newmask.str());
            directory.set(newdir.str());
            StringBuffer mask;
            for (unsigned i=0;i<width;i++) {
                mask.appendf("Part[%d]/@name",i+1);
                parts.item(i).clearOverrideName();
            }
            savePartsAttr(false);
        }
        else {
            // attempt recovery
            // do this synchronously to maximize chance of success (I don't expect many to have been done)
            for (i=0;i<width;i++) {
                Owned<IDistributedFilePart> part = getPart(i);
                unsigned copies = part->numCopies();
                for (int copy = copies-1; copy>=0; copy--) {
                    bool done = (copy==0)?afor2.doneprim[i]:afor2.donerep[i];
                    if (done) {
                        RemoteFilename oldrfn;
                        part->getFilename(oldrfn,(unsigned)copy);
                        const char *newfn = newNames.item(i).item(copy);
                        if (!newfn||!*newfn)
                            continue;
                        RemoteFilename newrfn;
                        newrfn.setPath(part->queryNode(copy)->endpoint(),newfn);
                        for (unsigned t=1;t<3;t++) {    // 3 goes
                            try {
                                StringBuffer oldfn;
                                oldrfn.getRemotePath(oldfn);
                                StringBuffer newfn;
                                newrfn.getRemotePath(newfn);
                                Owned<IFile> f = createIFile(newrfn);
                                f->move(oldfn.str());
                                PROGLOG("Succeeded rename %s back to %s",newfn.str(),oldfn.str());
                                break;
                            }
                            catch (IException *e) {
                                if (!afor2.except)
                                    afor2.except = e;
                                else
                                    e->Release();
                            }
                        }
                    }
                }
            }
        }
        if (afor2.except)
            throw afor2.except; 
        return afor2.ok;
    }

    IPropertyTree *queryRoot() { return root; }

    __int64 getFileSize(bool allowphysical,bool forcephysical)
    {
        __int64 ret = (__int64)(forcephysical?-1:queryAttributes().getPropInt64("@size",-1));
        if (ret==-1)
        {
            ret = 0;
            unsigned n = numParts();
            for (unsigned i=0;i<n;i++)
            {
                Owned<IDistributedFilePart> part = getPart(i);
                __int64 ps = part->getFileSize(allowphysical,forcephysical);
                if (ps == -1)
                {
                    ret = ps;
                    break;
                }
                ret += ps;
            }
        }
        return ret;
    }

    __int64 getDiskSize(bool allowphysical,bool forcephysical)
    {
        if (!isCompressed(NULL))
            return getFileSize(allowphysical, forcephysical);

        __int64 ret = (__int64)(forcephysical?-1:queryAttributes().getPropInt64("@compressedSize",-1));
        if (ret==-1)
        {
            ret = 0;
            unsigned n = numParts();
            for (unsigned i=0;i<n;i++)
            {
                Owned<IDistributedFilePart> part = getPart(i);
                __int64 ps = part->getDiskSize(allowphysical,forcephysical);
                if (ps == -1)
                {
                    ret = ps;
                    break;
                }
                ret += ps;
            }
        }
        return ret;
    }

    bool getFileCheckSum(unsigned &checkSum)
    {
        if (queryAttributes().hasProp("@checkSum"))
            checkSum = (unsigned)queryAttributes().getPropInt64("@checkSum");
        else
        {
            checkSum = ~0;
            unsigned n = numParts();
            for (unsigned i=0;i<n;i++) {
                Owned<IDistributedFilePart> part = getPart(i);
                unsigned crc;
                if (!part->getCrc(crc))
                    return false;
                checkSum ^= crc;
            }
        }
        return true;
    }

    virtual bool getFormatCrc(unsigned &crc)
    {
        if (queryAttributes().hasProp("@formatCrc")) {
            // NB pre record_layout CRCs are not valid
            crc = (unsigned)queryAttributes().getPropInt("@formatCrc");
            return true;
        }
        return false;
    }

    virtual bool getRecordLayout(MemoryBuffer &layout) 
    {
        return queryAttributes().getPropBin("_record_layout",layout);
    }

    virtual bool getRecordSize(size32_t &rsz)
    {
        if (queryAttributes().hasProp("@recordSize")) {
            rsz = (size32_t)queryAttributes().getPropInt("@recordSize");
            return true;
        }
        return false;
    }

    virtual unsigned getPositionPart(offset_t pos, offset_t &base)
    {
        unsigned n = numParts();
        base = 0;
        for (unsigned i=0;i<n;i++) {
            Owned<IDistributedFilePart> part = getPart(i);
            offset_t ps = part->getFileSize(true,false);
            if (ps==(offset_t)-1)
                break;
            if (ps>pos)
                return i;
            pos -= ps;
            base += ps;
        }
        return NotFound;
    }

    IDistributedSuperFile *querySuperFile()
    {
        return NULL; // i.e. this isn't super file
    }

    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err)
    {
        unsigned n = numParts();
        if (fdesc.numParts()!=n) {
            err.appendf("Different cluster width (%d/%d)",n,fdesc.numParts());
            return false;
        }
        if (fdesc.numClusters()!=1) {
            err.append("Cannot merge more than one cluster");
            return false;
        }
        StringBuffer cname;
        fdesc.getClusterLabel(0,cname);
        if (cname.length()&&(findCluster(cname.str())!=NotFound)) {
            err.append("File already contains cluster");
            err.append(cname.str());
            return false;
        }
        StringBuffer pname;
        StringBuffer fdtail;
        for (unsigned pn=0;pn<n;pn++) {
            IDistributedFilePart &part = queryPart(pn);
            part.getPartName(pname.clear());
            fdesc.queryPart(pn)->getTail(fdtail.clear());
            if (strcmp(pname.str(),fdtail.str())!=0) {
                err.appendf("Part name mismatch (%s,%s)",pname.str(),fdtail.str());
                return false;
            }
            RemoteFilename fdrfn;
            fdesc.getFilename(pn,0,fdrfn);
            unsigned nc = numCopies(pn);
            for (unsigned c = 0;c<nc;c++) {
                RemoteFilename rfn;
                part.getFilename(rfn,c);
                if (rfn.equals(fdrfn)) {
                    err.appendf("Parts overlap %s and %s",pname.str(),fdtail.str());
                    return false;
                }
            }
        }
        return true;
    }

    void enqueueReplicate()
    {
        MemoryBuffer mb;
        mb.append((byte)DRQ_REPLICATE).append(queryLogicalName());
        udesc->serialize(mb);
        CDateTime filedt;
        getModificationTime(filedt);                    
        filedt.serialize(mb);
        Owned<INamedQueueConnection> qconn = createNamedQueueConnection(0);
        Owned<IQueueChannel> qchannel = qconn->open(DFS_REPLICATE_QUEUE);
        qchannel->put(mb);
    }

    bool getAccessedTime(CDateTime &dt)                     
    {
        StringBuffer str;
        if (!root->getProp("@accessed",str))
            return false;
        dt.setString(str.str());
        return true;
    }

    virtual void setAccessedTime(const CDateTime &dt)       
    {
        if (logicalName.isForeign())
            parent->setFileAccessed(logicalName,udesc,dt);
        else
        {
            CFileAttrLock attrLock;
            if (0 == proplockcount && conn)
                verifyex(attrLock.init(logicalName, DXB_File, RTM_LOCK_WRITE, conn, defaultTimeout, "CDistributedFile::setAccessedTime"));
            if (dt.isNull())
                queryAttributes().removeProp("@accessed");
            else
            {
                StringBuffer str;
                queryAttributes().setProp("@accessed",dt.getString(str).str());
            }
        }
    }

    void setAccessed()
    {
        CDateTime dt;
        dt.setNow();
        setAccessedTime(dt);
    }
};

static unsigned findSubFileOrd(const char *name)
{
    if (*name=='#') {
        const char *n = name+1;
        if (*n) {
            do { n++; } while (*n&&isdigit(*n));
            if (!*n) 
                return atoi(name+1)-1;
        }
    }
    return NotFound;
}

class CDistributedSuperFile: public CDistributedFileBase<IDistributedSuperFile>
{
    void checkNotForeign()
    {
        if (!conn)
            throw MakeStringException(-1,"Operation not allowed on foreign file");
    }

    CDistributedFilePartArray partscache;
    FileClusterInfoArray clusterscache;

    /**
     * Adds a sub-file to a super-file within a transaction.
     */
    class cAddSubFileAction: public CDFAction
    {
        StringAttr parentlname;
        Owned<IDistributedSuperFile> parent;
        Owned<IDistributedFile> sub;
        StringAttr subfile;
        bool before;
        StringAttr other;
    public:
        cAddSubFileAction(const char *_parentlname,const char *_subfile,bool _before,const char *_other)
            : parentlname(_parentlname), subfile(_subfile), before(_before), other(_other)
        {
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname));
            if (!parent)
                throw MakeStringException(-1,"addSubFile: SuperFile %s cannot be found",parentlname.get());
            if (!subfile.isEmpty())
            {
                try
                {
                    sub.setown(transaction->lookupFile(subfile,SDS_SUB_LOCK_TIMEOUT));
                    if (!sub)
                        throw MakeStringException(-1,"cAddSubFileAction: sub file %s not found", subfile.str());
                    // Must validate before locking for update below, to check sub is not already in parent (and therefore locked already)
                    transaction->validateAddSubFile(parent, sub, subfile);
                }
                catch (IDFS_Exception *e)
                {
                    if (e->errorCode()!=DFSERR_LookupConnectionTimout)
                        throw;
                    e->Release();
                    return false;
                }
                if (!sub.get())
                    throw MakeStringException(-1,"addSubFile: File %s cannot be found to add",subfile.get());
            }
            // Try to lock all files
            addFileLock(parent);
            if (lock())
            {
                transaction->noteAddSubFile(parent, parentlname, sub);
                return true;
            }
            unlock();
            parent.clear();
            sub.clear();
            return false;
        }
        virtual void run()
        {
            if (!sub)
                throw MakeStringException(-1,"addSubFile(2): File %s cannot be found to add",subfile.get());
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);                 
            if (sf)
                sf->doAddSubFile(LINK(sub),before,other,transaction);
        }
        virtual void commit()
        {
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);                 
            if (sf)
                sf->updateParentFileAttrs(transaction);
            CDFAction::commit();
        }
        virtual void retry()
        {
            parent.clear();
            sub.clear();
            CDFAction::retry();
        }
    };

    /**
     * Removes a sub-file of a super-file within a transaction.
     */
    class cRemoveSubFileAction: public CDFAction
    {
        StringAttr parentlname;
        Owned<IDistributedSuperFile> parent;
        Owned<IDistributedFile> sub;
        StringAttr subfile;
        bool remsub;
    public:
        cRemoveSubFileAction(const char *_parentlname,const char *_subfile,bool _remsub=false)
            : parentlname(_parentlname), subfile(_subfile), remsub(_remsub)
        {
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname));
            if (!parent)
                throw MakeStringException(-1,"removeSubFile: SuperFile %s cannot be found",parentlname.get());
            if (!subfile.isEmpty())
            {
                try
                {
                    sub.setown(transaction->lookupFile(subfile,SDS_SUB_LOCK_TIMEOUT));
                }
                catch (IDFS_Exception *e)
                {
                    if (e->errorCode()!=DFSERR_LookupConnectionTimout)
                        throw;
                    e->Release();
                    return false;
                }
                if (!transaction->isSubFile(parent, subfile, true))
                {
                    WARNLOG("removeSubFile: File %s is not a subfile of %s", subfile.get(), parent->queryLogicalName());
                    parent.clear();
                    sub.clear();
                    return true; // NB: sub was not a member of super, issue warning and continue without locking
                }
            }
            // Try to lock all files
            addFileLock(parent);
            if (sub && remsub) // NB: I only need to lock (for exclusivity, if going to delete
                addFileLock(sub);
            if (lock())
            {
                if (sub)
                    transaction->noteRemoveSubFile(parent, sub);
                else
                    transaction->clearSubFiles(parent);
                return true;
            }
            unlock();
            parent.clear();
            sub.clear();
            return false;
        }
        virtual void run()
        {
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);
            if (sf) {
                // Delay the deletion of the subs until commit
                if (remsub) {
                    if (subfile) {
                        CDfsLogicalFileName lname;
                        lname.set(subfile.get());
                        transaction->addDelayedDelete(lname, SDS_SUB_LOCK_TIMEOUT);
                    } else { // Remove all subfiles
                        Owned<IDistributedFileIterator> iter = parent->getSubFileIterator(false);
                        ForEach (*iter) {
                            CDfsLogicalFileName lname;
                            IDistributedFile *f = &iter->query();
                            lname.set(f->queryLogicalName());
                            transaction->addDelayedDelete(lname, SDS_SUB_LOCK_TIMEOUT);
                        }
                    }
                }
                // Now we clean the subs
                if (subfile.get())
                    sf->doRemoveSubFile(subfile.get(), transaction);
                else
                    sf->doRemoveSubFiles(transaction);
            }
        }
        virtual void retry()
        {
            parent.clear();
            sub.clear();
            CDFAction::retry();
        }
    };

    /**
     * Removes all subfiles exclusively owned by named superfile within a transaction.
     */
    class cRemoveOwnedSubFilesAction: public CDFAction
    {
        StringAttr parentlname;
        Owned<IDistributedSuperFile> parent;
        bool remsub;
    public:
        cRemoveOwnedSubFilesAction(IDistributedFileTransaction *_transaction, const char *_parentlname,bool _remsub=false)
            : parentlname(_parentlname), remsub(_remsub)
        {
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname));
            if (!parent)
                throw MakeStringException(-1,"removeOwnedSubFiles: SuperFile %s cannot be found", parentlname.get());
            // Try to lock all files
            addFileLock(parent);
            if (lock())
                return true;
            unlock();
            parent.clear();
            return false;
        }
        virtual void run()
        {
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);
            if (sf)
            {
                StringArray toRemove;
                Owned<IDistributedFileIterator> iter = parent->getSubFileIterator(false);
                ForEach (*iter)
                {
                    IDistributedFile *file = &iter->query();
                    CDistributedFile *_file = QUERYINTERFACE(file, CDistributedFile);
                    StringArray owners;
                    _file->getSuperOwners(owners);

                    if (NotFound == owners.find(parentlname))
                        ThrowStringException(-1, "removeOwnedSubFiles: SuperFile %s, subfile %s - subfile not owned by superfile", parentlname.get(), file->queryLogicalName());
                    if (1 == owners.ordinality()) // just me
                    {
                        const char *logicalName = file->queryLogicalName();
                        toRemove.append(logicalName);
                        // Delay the deletion of the subs until commit
                        if (remsub)
                        {
                            CDfsLogicalFileName lname;
                            lname.set(logicalName);
                            transaction->addDelayedDelete(lname, SDS_SUB_LOCK_TIMEOUT);
                        }
                    }
                }
                // Now we clean the subs
                if (sf->numSubFiles(false) == toRemove.ordinality())
                    sf->doRemoveSubFiles(transaction); // remove all
                else
                {
                    ForEachItemIn(r, toRemove)
                        sf->doRemoveSubFile(toRemove.item(r), transaction);
                }
            }
        }
        virtual void retry()
        {
            parent.clear();
            CDFAction::retry();
        }
    };

    /**
     * Swaps sub-files between two super-files within a transaction.
     */
    class cSwapFileAction: public CDFAction
    {
        Linked<IDistributedSuperFile> super1, super2;
        StringAttr super1Name, super2Name;
    public:
        cSwapFileAction(const char *_super1Name, const char *_super2Name)
            : super1Name(_super1Name), super2Name(_super2Name)
        {
        }
        virtual bool prepare()
        {
            super1.setown(transaction->lookupSuperFile(super1Name));
            if (!super1)
                throw MakeStringException(-1,"swapSuperFile: SuperFile %s cannot be found", super1Name.get());
            super2.setown(transaction->lookupSuperFile(super2Name));
            if (!super2)
            {
                super1.clear();
                throw MakeStringException(-1,"swapSuperFile: SuperFile %s cannot be found", super2Name.get());
            }
            // Try to lock all files
            addFileLock(super1);
            for (unsigned i=0; i<super1->numSubFiles(); i++)
                addFileLock(&super1->querySubFile(i));
            addFileLock(super2);
            for (unsigned i=0; i<super2->numSubFiles(); i++)
                addFileLock(&super2->querySubFile(i));
            if (lock())
            {
                transaction->noteSuperSwap(super1, super2);
                return true;
            }
            unlock();
            super1.clear();
            super2.clear();
            return false;
        }
        virtual void run()
        {
            CDistributedSuperFile *sf = QUERYINTERFACE(super1.get(),CDistributedSuperFile);
            if (sf)
                sf->doSwapSuperFile(super2,transaction);
        }
        virtual void retry()
        {
            super1.clear();
            super2.clear();
            CDFAction::retry();
        }
    };

    /**
     * SubFile Iterator, used only to list sub-files of a super-file.
     */
    class cSubFileIterator: public CDistributedFileIteratorBase< IDistributedFileIterator, IArrayOf<IDistributedFile> >
    {
    public:
        cSubFileIterator(IArrayOf<IDistributedFile> &_subfiles, bool supersub)
        {
            ForEachItemIn(i,_subfiles) {
                IDistributedSuperFile* super = supersub?_subfiles.item(i).querySuperFile():NULL;
                if (super) {
                    Owned<IDistributedFileIterator> iter = super->getSubFileIterator(true);
                    ForEach(*iter)
                        list.append(iter->get());
                }
                else
                    list.append(*LINK(&_subfiles.item(i)));
            }
        }

        StringBuffer & getName(StringBuffer &name)
        {
            return list.item(index).getLogicalName(name);
        }

        IDistributedFile & query()
        {
            return list.item(index);
        }
    };

    void checkModify(const char *title) 
    {
        StringBuffer reason;
        if (!canModify(reason)) {
#ifdef EXTRA_LOGGING
            PROGLOG("CDistributedSuperFile::%s(canModify) %s",title,reason.str());
#endif
            if (reason.length())
                throw MakeStringException(-1,"CDistributedSuperFile::%s %s",title,reason.str());
        }
    }

protected:
    int interleaved; // 0 not interleaved, 1 interleaved old, 2 interleaved new
    IArrayOf<IDistributedFile> subfiles;

    void clearSuperOwners(unsigned timeoutMs)
    {
        /* JCSMORE - Why on earth is this doing this way?
         * We are in a super file, we already have [read] locks to sub files (in 'subfiles' array)
         * This should iterate through those and call unlinkSubFile I think.
         */
        Owned<IPropertyTreeIterator> iter = root->getElements("SubFile");
        StringBuffer oquery;
        oquery.append("SuperOwner[@name=\"").append(logicalName.get()).append("\"]");
        Owned<IMultiException> exceptions = MakeMultiException("CDelayedDelete::doRemoveEntry::SuperOwners");
        ForEach(*iter)
        {
            const char *name = iter->query().queryProp("@name");
            if (name&&*name)
            {
                CDfsLogicalFileName subfn;
                subfn.set(name);
                CFileLock fconnlockSub;
                // JCSMORE - this is really not right, but consistent with previous version
                // MORE: Use CDistributedSuperFile::linkSuperOwner(false) - ie. unlink
                if (fconnlockSub.init(subfn, RTM_LOCK_READ, timeoutMs, "CDistributedFile::doRemoveEntry"))
                {
                    IPropertyTree *subfroot = fconnlockSub.queryRoot();
                    if (subfroot)
                    {
                        if (!subfroot->removeProp(oquery.str()))
                            exceptions->append(*MakeStringException(-1, "CDelayedDelete::removeEntry: SubFile %s of %s not found for removal",name?name:"(NULL)", logicalName.get()));
                    }
                }
            }
        }
        if (exceptions->ordinality())
            throw exceptions.getClear();
    }

    virtual void getSuperOwners(StringArray &owners)
    {
        ForEachItemIn(i, subfiles)
        {
            IDistributedFile *file = &subfiles.item(i);
            IDistributedSuperFile *super = file->querySuperFile();
            if (super)
            {
                CDistributedSuperFile *_super = QUERYINTERFACE(super, CDistributedSuperFile);
                if (_super)
                    _super->getSuperOwners(owners);
            }
            else
            {
                CDistributedFile *_file = QUERYINTERFACE(file, CDistributedFile);
                if (_file)
                    _file->getSuperOwners(owners);
            }
        }

    }
    static StringBuffer &getSubPath(StringBuffer &path,unsigned idx)
    {
        return path.append("SubFile[@num=\"").append(idx+1).append("\"]");
    }

    void loadSubFiles(IDistributedFileTransaction *transaction, unsigned timeout)
    {
        partscache.kill();
        StringBuffer path;
        StringBuffer subname;
        subfiles.kill();

        unsigned n = root->getPropInt("@numsubfiles");
        if (n == 0)
            return;
        try
        {
            // Find all reported indexes and bail on bad range (before we lock any file)
            Owned<IPropertyTreeIterator> subit = root->getElements("SubFile");
            // Adding a sub 'before' another get the list out of order (but still valid)
            OwnedMalloc<IPropertyTree *> orderedSubFiles(n, true);
            ForEach (*subit)
            {
                IPropertyTree &sub = subit->query();
                unsigned sn = sub.getPropInt("@num",0);
                if (sn == 0)
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: bad subfile part number %d of %d", logicalName.get(), sn, n);
                if (sn > n)
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: out-of-range subfile part number %d of %d", logicalName.get(), sn, n);
                if (orderedSubFiles[sn-1])
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: duplicated subfile part number %d of %d", logicalName.get(), sn, n);
                orderedSubFiles[sn-1] = &sub;
            }
            for (unsigned i=0; i<n; i++)
            {
                if (!orderedSubFiles[i])
                    ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: missing subfile part number %d of %d", logicalName.get(), i+1, n);
            }
            // Now try to resolve them all (file/superfile)
            for (unsigned f=0; f<n; f++)
            {
                IPropertyTree &sub = *(orderedSubFiles[f]);
                sub.getProp("@name",subname.clear());
                Owned<IDistributedFile> subfile;
                subfile.setown(transaction?transaction->lookupFile(subname.str(),timeout):parent->lookup(subname.str(), udesc, false, false, false, transaction, timeout));
                if (!subfile.get())
                    subfile.setown(transaction?transaction->lookupSuperFile(subname.str(),timeout):parent->lookupSuperFile(subname.str(),udesc,transaction,timeout));

                // Some files are ok not to exist
                if (!subfile.get())
                {
                    CDfsLogicalFileName cdfsl;
                    cdfsl.set(subname);
                    if (cdfsl.isForeign())
                    {
                        // MORE - This foreign treatment seems flaky at best
                        WARNLOG("CDistributedSuperFile: SuperFile %s's sub-file file '%s' is foreign, removing it from super", logicalName.get(), subname.str());
                        root->removeTree(&sub);
                        continue;
                    }
                    else
                        ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: corrupt subfile file '%s' cannot be found", logicalName.get(), subname.str());
                }
                subfiles.append(*subfile.getClear());
            }
            // This is *only* due to foreign files
            if (subfiles.ordinality() != n)
            {
                WARNLOG("CDistributedSuperFile: SuperFile %s's number of sub-files updated to %d", logicalName.get(), subfiles.ordinality());
                root->setPropInt("@numsubfiles", subfiles.ordinality());
            }
        }
        catch (IException *)
        {
            partscache.kill();
            subfiles.kill();    // one out, all out
            throw;
        }
    }

    void addItem(unsigned pos,IDistributedFile *_file)
    {
        Owned<IDistributedFile> file = _file;
        partscache.kill();
        // first renumber all above
        StringBuffer path;
        IPropertyTree *sub;
        for (unsigned i=subfiles.ordinality();i>pos;i--) {
            sub = root->queryPropTree(getSubPath(path.clear(),i-1).str());
            if (!sub)
                throw MakeStringException(-1,"C(2): Corrupt subfile file part %d cannot be found",i);
            sub->setPropInt("@num",i+1);
        }
        sub = createPTree();
        sub->setPropInt("@num",pos+1);
        sub->setProp("@name",file->queryLogicalName());
        if (pos==0) {
            resetFileAttr(createPTreeFromIPT(&file->queryAttributes()));
        }
        root->addPropTree("SubFile",sub);
        subfiles.add(*file.getClear(),pos);
        root->setPropInt("@numsubfiles",subfiles.ordinality());
    }

    void removeItem(unsigned pos)
    {
        partscache.kill();
        StringBuffer path;
        IPropertyTree* sub = root->queryPropTree(getSubPath(path,pos).str());
        if (!sub)
            throw MakeStringException(-1,"CDistributedSuperFile(3): Corrupt subfile file part %d cannot be found",pos+1);
        root->removeTree(sub);
        // now renumber all above
        for (unsigned i=pos+1; i<subfiles.ordinality(); i++) {
            sub = root->queryPropTree(getSubPath(path.clear(),i).str());
            if (!sub)
                throw MakeStringException(-1,"CDistributedSuperFile(2): Corrupt subfile file part %d cannot be found",i+1);
            sub->setPropInt("@num",i);
        }
        subfiles.remove(pos);
        if (pos==0) {
            if (subfiles.ordinality())
                resetFileAttr(createPTreeFromIPT(&subfiles.item(0).queryAttributes()));
            else
                resetFileAttr(getEmptyAttr());
        }
        root->setPropInt("@numsubfiles",subfiles.ordinality());
    }

    void loadParts(CDistributedFilePartArray &partsret, IDFPartFilter *filter)
    {
        unsigned p = 0;
        if (interleaved) { // a bit convoluted but should work
            IArrayOf<IDistributedFile> allsubfiles;
            ForEachItemIn(i,subfiles) {
                // if old format keep original interleaving
                IDistributedSuperFile* super = (interleaved==1)?NULL:subfiles.item(i).querySuperFile();
                if (super) {
                    Owned<IDistributedFileIterator> iter = super->getSubFileIterator(true);
                    ForEach(*iter) 
                        allsubfiles.append(iter->get());
                }
                else
                    allsubfiles.append(*LINK(&subfiles.item(i)));
            }
            unsigned *pn = new unsigned[allsubfiles.ordinality()];
            ForEachItemIn(j,allsubfiles) 
                pn[j] = allsubfiles.item(j).numParts();
            unsigned f=0;
            bool found=false;
            loop {
                if (f==allsubfiles.ordinality()) {
                    if (!found)
                        break; // no more
                    found = false;
                    f = 0;
                }
                if (pn[f]) {
                    found = true;
                    if (!filter||filter->includePart(p)) {
                        IDistributedFile &subfile = allsubfiles.item(f);
                        IDistributedFilePart *part = subfile.getPart(subfile.numParts()-pn[f]);
                        partsret.append(*QUERYINTERFACE(part,CDistributedFilePart)); // bit kludgy
                    }
                    p++;
                    pn[f]--;
                }
                f++;
            }
            delete [] pn;
        }
        else { // sequential
            ForEachItemIn(i,subfiles) { // not wonderfully quick
                IDistributedFile &subfile = subfiles.item(i);
                unsigned n = subfile.numParts();
                unsigned j = 0;
                while (n--) {
                    if (!filter||filter->includePart(p)) {
                        IDistributedFilePart *part = subfile.getPart(j++);
                        partsret.append(*QUERYINTERFACE(part,CDistributedFilePart)); // bit kludgy
                    }
                    p++;
                }
            }
        }
    }

    void linkSubFile(unsigned pos, bool link=true)
    {
        IDistributedFile *subfile = &subfiles.item(pos);
        IDistributedSuperFile *ssub = subfile->querySuperFile();
        if (ssub) {
            CDistributedSuperFile *cdsuper = QUERYINTERFACE(ssub,CDistributedSuperFile);
            cdsuper->linkSuperOwner(queryLogicalName(),link);
        }
        else {
            CDistributedFile *cdfile = QUERYINTERFACE(subfile,CDistributedFile);
            cdfile->linkSuperOwner(queryLogicalName(),link);
        }
    }

    void unlinkSubFile(unsigned pos)
    {
        linkSubFile(pos, false);
    }

    void checkSubFormatAttr(IDistributedFile *sub, const char* exprefix="")
    {
        // empty super files now pass
        ForEachItemIn(i,subfiles) {
            IDistributedSuperFile* super = subfiles.item(i).querySuperFile();
            if (super) {
                CDistributedSuperFile *cdsuper = QUERYINTERFACE(super,CDistributedSuperFile);
                if (cdsuper)
                    cdsuper->checkSubFormatAttr(sub,exprefix);
                return;
            }
            CDistributedFile *cdfile = QUERYINTERFACE(&subfiles.item(0),CDistributedFile);
            if (cdfile)
                cdfile->checkFormatAttr(sub,exprefix);        // any file will do
        }
    }

public:

    void checkFormatAttr(IDistributedFile *sub, const char* exprefix="")
    {
        // only check sub files not siblings, which is excessive (format checking is really only debug aid)
        checkSubFormatAttr(sub,exprefix);
    }

    unsigned findSubFile(const char *name)
    {
        StringBuffer lfn;
        normalizeLFN(name,lfn);
        ForEachItemIn(i,subfiles) 
            if (stricmp(subfiles.item(i).queryLogicalName(),lfn.str())==0)
                return i;
        return NotFound;
    }

    IMPLEMENT_IINTERFACE;

    void init(CDistributedFileDirectory *_parent, IPropertyTree *_root, const CDfsLogicalFileName &_name, IUserDescriptor* user, IDistributedFileTransaction *transaction, unsigned timeout=INFINITE)
    {
        assertex(_name.isSet());
        setUserDescriptor(udesc,user);
        logicalName.set(_name);
        parent = _parent;
        root.set(_root);
        const char *val = root->queryProp("@interleaved");
        if (val&&isdigit(*val))
            interleaved = atoi(val);
        else
            interleaved = strToBool(val)?1:0;
        loadSubFiles(transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IPropertyTree *_root,const CDfsLogicalFileName &_name,IUserDescriptor* user) 
    {
        init(_parent,_root,_name,user,NULL);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IRemoteConnection *_conn,const CDfsLogicalFileName &_name,IUserDescriptor* user, IDistributedFileTransaction *transaction,unsigned timeout)
    {
        conn.setown(_conn);
        init(_parent,conn->queryRoot(),_name,user,transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, CDfsLogicalFileName &_name, IUserDescriptor* user, IDistributedFileTransaction *transaction)
    {
        // temp super file
        assertex(_name.isMulti());
        if (!_name.isExpanded())
            _name.expand(user);//expand wildcards
        Owned<IPropertyTree> tree = _name.createSuperTree();
        init(_parent,tree,_name,user,transaction);
    }

    ~CDistributedSuperFile()
    {
        partscache.kill();
        subfiles.kill();
    }

    StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name)
    {
        // returns the cluster name if all the same
        CriticalBlock block (sect);
        if (subfiles.ordinality()==1)
            return subfiles.item(0).getClusterName(clusternum,name);
        size32_t rl = name.length();
        StringBuffer test;
        ForEachItemIn(i,subfiles) {
            if (i) {
                subfiles.item(i).getClusterName(clusternum,test.clear());
                if (strcmp(name.str(),test.str())!=0) {
                    name.setLength(rl);
                    break;
                }
            }
            else 
                subfiles.item(i).getClusterName(clusternum,name);
        }
        return name; 
    }

    IFileDescriptor *getFileDescriptor(const char *clustername)
    {
        CriticalBlock block (sect);
        if (subfiles.ordinality()==1)
            return subfiles.item(0).getFileDescriptor(clustername);
        // superfiles assume consistant replication & compression
        UnsignedArray subcounts;  
        bool mixedwidth = false;
        unsigned width = 0;
        bool first = true;
        Owned<IPropertyTree> at = getEmptyAttr();
        Owned<IDistributedFileIterator> fiter = getSubFileIterator(true);
        ForEach(*fiter)
        {
            IDistributedFile &file = fiter->query();
            if (first)
            {
                first = false;
                Owned<IAttributeIterator> ait = file.queryAttributes().getAttributes();
                ForEach(*ait)
                {
                    const char *name = ait->queryName();
                    if ((stricmp(name,"@size")!=0)&&(stricmp(name,"@recordCount")!=0))
                    {
                        const char *v = ait->queryValue();
                        if (!v)
                            continue;
                        bool ok = true;
                        // add attributes that are common
                        for (unsigned i=1;i<subfiles.ordinality();i++)
                        {
                            IDistributedFile &file = subfiles.item(i);
                            IDistributedSuperFile *sFile = file.querySuperFile();
                            if (!sFile || sFile->numSubFiles(true)) // skip empty super files
                            {
                                const char *p = file.queryAttributes().queryProp(name);
                                if (!p||(strcmp(p,v)!=0))
                                {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                        if (ok)
                            at->setProp(name,v);
                    }
                }
            }
            unsigned np = file.numParts();
            if (0 == width)
                width = np;
            else if (np!=width)
                mixedwidth = true;
            subcounts.append(np);
        }

        // need common attributes
        Owned<ISuperFileDescriptor> fdesc=createSuperFileDescriptor(at.getClear());
        if (interleaved&&(interleaved!=2))
            WARNLOG("getFileDescriptor: Unsupported interleave value (1)");
        fdesc->setSubMapping(subcounts,interleaved!=0);
        fdesc->setTraceName(logicalName.get());
        Owned<IDistributedFilePartIterator> iter = getIterator(NULL);
        unsigned n = 0;
        SocketEndpointArray reps;
        ForEach(*iter) {
            IDistributedFilePart &part = iter->query();
            CDistributedFilePart *cpart = (clustername&&*clustername)?QUERYINTERFACE(&part,CDistributedFilePart):NULL;
            unsigned copy = 0;
            if (cpart) {
                IDistributedFile &f = cpart->queryParent();
                unsigned cn = f.findCluster(clustername);
                if (cn!=NotFound) {
                    for (unsigned i = 0;i<cpart->numCopies();i++)
                        if (cpart->copyClusterNum(i,NULL)==cn) {
                            copy = i;
                            break;
                        }
                }
            }
            if (mixedwidth) {
                SocketEndpoint rep;
                if (copy+1<part.numCopies())
                    rep = part.queryNode(copy+1)->endpoint();
                reps.append(rep);
            }

            RemoteFilename rfn;
            fdesc->setPart(n,part.getFilename(rfn,copy),&part.queryAttributes());
            n++;
        }
        ClusterPartDiskMapSpec mspec;
        if (subfiles.ordinality()) {
            mspec = subfiles.item(0).queryPartDiskMapping(0);
        }
        mspec.interleave = numSubFiles(true);
        fdesc->endCluster(mspec);
        if (mixedwidth) { // bleah - have to add replicate node numbers
            Owned<IGroup> group = fdesc->getGroup();
            unsigned gw = group->ordinality();
            for (unsigned pn=0;pn<reps.ordinality();pn++) {
                const SocketEndpoint &ep=reps.item(pn);
                if (!ep.isNull()) {
                    unsigned gn = pn;
                    if (gn<gw) {
                        do {
                            gn++;
                            if (gn==gw)
                                gn = 0;
                            if (ep.equals(group->queryNode((rank_t)gn).endpoint())) {
                                IPartDescriptor *part = fdesc->queryPart(pn);
                                if (part)
                                    part->queryProperties().setPropInt("@rn",(unsigned)gn);
                                break;
                            }
                        } while (gn!=pn);
                    }
                }
            }
        }
        return fdesc.getClear();
    }

    unsigned numParts()
    {
        CriticalBlock block(sect);
        unsigned ret=0;
        ForEachItemIn(i,subfiles)
            ret += subfiles.item(i).numParts();
        return ret;
    }

    IDistributedFilePart &queryPart(unsigned idx)
    {
        CriticalBlock block(sect);
        if (subfiles.ordinality()==1)
            return subfiles.item(0).queryPart(idx);
        if (partscache.ordinality()==0)
            loadParts(partscache,NULL);     
        if (idx>=partscache.ordinality())
            return *(IDistributedFilePart *)NULL;
        return partscache.item(idx);
    }

    IDistributedFilePart* getPart(unsigned idx)
    {
        IDistributedFilePart* ret = &queryPart(idx);
        return LINK(ret);
    }

    IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL)
    {
        CriticalBlock block(sect);
        if (subfiles.ordinality()==1)
            return subfiles.item(0).getIterator(filter);
        CDistributedFilePartIterator *ret = new CDistributedFilePartIterator();
        loadParts(ret->queryParts(),filter);
        return ret;
    }

    void rename(const char *_logicalname,IUserDescriptor *user)
    {
        StringBuffer prevname;
        Owned<IFileRelationshipIterator> reliter;
        // set prevname
        if (!isAnon()) {
            getLogicalName(prevname);
            try {
                IFileRelationshipIterator *iter = parent->lookupAllFileRelationships(prevname.str());
                reliter.setown(iter);
            }
            catch (IException *e) {
                EXCLOG(e,"CDistributedFileDirectory::rename");
                e->Release();
            }
            detach();
        }
        attach(_logicalname,user);
        if (reliter.get()) {
            // add back any relationships with new name
            parent->renameFileRelationships(prevname.str(),_logicalname,reliter,user);
        }
    }


    const char *queryDefaultDir()
    {
        // returns the directory if all the same
        const char *ret = NULL;
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            if (subfiles.item(i).numParts())
            {
                const char *s = subfiles.item(i).queryDefaultDir();
                if (!s)
                    return NULL;
                if (!ret)
                    ret = s;
                else if (strcmp(ret,s)!=0)
                    return NULL;
            }
        }
        return ret; 
    }

    const char *queryPartMask()
    {
        // returns the part mask if all the same
        const char *ret = NULL;
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            const char *s = subfiles.item(i).queryPartMask();
            if (!s)
                return NULL;
            if (!ret)
                ret = s;
            else if (stricmp(ret,s)!=0)
                return NULL;
        }
        return ret; 
    }

    void attach(const char *_logicalname,IUserDescriptor *user)
    {
        assertex(!conn.get()); // already attached
        CriticalBlock block (sect);
        StringBuffer tail;
        StringBuffer lfn;
        logicalName.set(_logicalname);
        checkLogicalName(logicalName,user,true,true,false,"attach"); 
        parent->addEntry(logicalName,root.getClear(),true,false);
        conn.clear();
        CFileLock fcl;
        verifyex(fcl.init(logicalName, DXB_SuperFile, RTM_LOCK_READ, defaultTimeout, "CDistributedSuperFile::attach"));
        conn.setown(fcl.detach());
        assertex(conn.get()); // must have been attached
        root.setown(conn->getRoot());
    }

    void detach(unsigned timeoutMs=INFINITE)
    {   
        assertex(conn.get()); // must be attached
        CriticalBlock block(sect);
        checkModify("CDistributedSuperFile::detach");
        subfiles.kill();    

        // Remove from SDS

        /* JCSMORE - this looks very kludgy...
         * We have readlock, this code is doing
         * 1) change to write lock (not using lockProperties or DistributedFilePropertyLock to do so) [using CFileChangeWriteLock]
         *    CFileChangeWriteLock doesn't preserve lock mode quite right.. (see 'newMode')
         * 2) manually deleting SuperOwner from subfiles (in clearSuperOwners)
         * 3) Using the connection to delete the SuperFile from Dali (clones to 'root' in process)
         * 4) ~CFileChangeWriteLock() [writeLock.clear()], restores read lock from write to read
         * 5) updateFS (housekeeping of empty scopes, relationships) - ok
         */
        CFileChangeWriteLock writeLock(conn, timeoutMs);
        clearSuperOwners(timeoutMs);
        writeLock.clear();
        root.setown(closeConnection(true));
        updateFS(logicalName, parent->queryDefaultTimeout());
        logicalName.clear();
    }

    bool existsPhysicalPartFiles(unsigned short port)
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            if (!f.existsPhysicalPartFiles(port))
                return false;
        }
        return true; 
    }

    bool renamePhysicalPartFiles(const char *newlfn,const char *cluster,IMultiException *mexcept,const char *newbasedir)
    {
        throw MakeStringException(-1,"renamePhysicalPartFiles not supported for SuperFiles");
        return false; 
    }

    void serialize(MemoryBuffer &mb)
    {
        UNIMPLEMENTED; // not yet needed
    }

    virtual unsigned numCopies(unsigned partno)
    {
        unsigned ret = (unsigned)-1;
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            unsigned fnc = f.numCopies(partno);
            if (fnc<ret)
                ret = fnc;
        }
        return (ret==(unsigned)-1)?1:ret;
    }

    __int64 getFileSize(bool allowphysical,bool forcephysical)
    {
        __int64 ret = (__int64)(forcephysical?-1:queryAttributes().getPropInt64("@size",-1));
        if (ret==-1)
        {
            ret = 0;
            ForEachItemIn(i,subfiles)
            {
                __int64 ps = subfiles.item(i).getFileSize(allowphysical,forcephysical);
                if (ps == -1)
                    return -1; // i.e. if cannot determine size of any part, total is unknown
                ret += ps;
            }
        }
        return ret;
    }

    __int64 getDiskSize(bool allowphysical,bool forcephysical)
    {
        if (!isCompressed(NULL))
            return getFileSize(allowphysical, forcephysical);

        __int64 ret = (__int64)(forcephysical?-1:queryAttributes().getPropInt64("@compressedSize",-1));
        if (ret==-1)
        {
            ret = 0;
            ForEachItemIn(i,subfiles)
            {
                __int64 ps = subfiles.item(i).getDiskSize(allowphysical,forcephysical);
                if (ps == -1)
                    return -1; // i.e. if cannot determine size of any part, total is unknown
                ret += ps;
            }
        }
        return ret;
    }

    __int64 getRecordCount()
    {
        __int64 ret = queryAttributes().getPropInt64("@recordCount",-1);
        if (ret==-1) {
            ret = 0;
            ForEachItemIn(i,subfiles) {
                __int64 rc = subfiles.item(i).queryAttributes().getPropInt64("@recordCount",-1);
                if (rc == -1) {
                    ret = rc;
                    break;
                }
                ret += rc;
            }
        }
        return ret;
    }

    bool getFileCheckSum(unsigned &checkSum)
    {
        if (queryAttributes().hasProp("@checkSum"))
            checkSum = (unsigned)queryAttributes().getPropInt64("@checkSum");
        else
        {
            checkSum = ~0;
            ForEachItemIn(i,subfiles) {
                unsigned cs;
                if (!subfiles.item(i).getFileCheckSum(cs))
                    return false;
                checkSum ^= cs;
            }
        }
        return true;
    }

    IDistributedSuperFile *querySuperFile()
    {
        return this;
    }

    virtual IDistributedFile &querySubFile(unsigned idx,bool sub)
    {
        CriticalBlock block (sect);
        if (sub) {
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                IDistributedSuperFile *super = f.querySuperFile();
                if (super) {
                    unsigned ns = super->numSubFiles(true);
                    if (ns>idx)
                        return super->querySubFile(idx,true);
                    idx -= ns;
                }
                else if (idx--==0)
                    return f;
            }
            // fall through to error
        }
        return subfiles.item(idx);
    }

    virtual IDistributedFile *querySubFileNamed(const char *name, bool sub)
    {
        CriticalBlock block (sect);
        unsigned idx=findSubFileOrd(name);
        if ((idx!=NotFound)&&(idx<subfiles.ordinality()))
            return &subfiles.item(idx);
        idx=findSubFile(name);
        if (idx!=NotFound) 
            return &subfiles.item(idx);
        if (sub) {
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                IDistributedSuperFile *super = f.querySuperFile();
                if (super) {
                    IDistributedFile *ret = super->querySubFileNamed(name);
                    if (ret)
                        return ret;
                }
            }
        }
        return NULL;
    }

    virtual IDistributedFile *getSubFile(unsigned idx,bool sub)
    {
        CriticalBlock block (sect);
        return LINK(&querySubFile(idx,sub));
    }

    virtual unsigned numSubFiles(bool sub)
    {
        CriticalBlock block (sect);
        unsigned ret = 0;
        if (sub) {
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                IDistributedSuperFile *super = f.querySuperFile();
                if (super) 
                    ret += super->numSubFiles();
                else
                    ret++;
            }
        }
        else
            ret = subfiles.ordinality();
        return ret;
    }

    virtual bool getFormatCrc(unsigned &crc)
    {
        if (queryAttributes().hasProp("@formatCrc")) {
            crc = (unsigned)queryAttributes().getPropInt("@formatCrc");
            return true;
        }
        bool found = false;
        ForEachItemIn(i,subfiles) {
            unsigned c;
            if (subfiles.item(i).getFormatCrc(c)) {
                if (found&&(c!=crc))
                    return false;
                found = true;
                crc = c;
            }
        }
        return found;
    }

    virtual bool getRecordLayout(MemoryBuffer &layout)  
    {
        layout.clear();
        if (queryAttributes().getPropBin("_record_layout",layout))
            return true;
        bool found = false;
        ForEachItemIn(i,subfiles) {
            MemoryBuffer b;
            if (subfiles.item(i).getRecordLayout(found?b:layout)) {
                if (found) {
                    if ((b.length()!=layout.length())||(memcmp(b.bufferBase(),layout.bufferBase(),b.length())!=0))
                        return false;
                }
                else 
                    found = true;
            }
        }
        return found;
    }

    virtual bool getRecordSize(size32_t &rsz)
    {
        if (queryAttributes().hasProp("@recordSize")) {
            rsz = (size32_t)queryAttributes().getPropInt("@recordSize");
            return true;
        }
        bool found = false;
        ForEachItemIn(i,subfiles) {
            size32_t sz;
            if (subfiles.item(i).getRecordSize(sz)) {
                if (found&&(sz!=rsz))
                    return false;
                found = true;
                rsz = sz;
            }
        }
        return found;
    }

    virtual bool isInterleaved()
    {
        return interleaved!=0;
    }

    virtual IDistributedFile *querySubPart(unsigned partidx,unsigned &subfileidx)
    {
        CriticalBlock block (sect);
        subfileidx = 0;
        Owned<IDistributedFilePart> part = getPart(partidx);
        if (!part)
            return NULL;
        CDistributedFilePart *cpart = QUERYINTERFACE(part.get(),CDistributedFilePart);
        if (!cpart)
            return NULL;
        IDistributedFile &ret = cpart->queryParent();
        unsigned n = ret.numParts();
        for (unsigned i=0;i<n;i++) {
            Owned<IDistributedFilePart> spart = ret.getPart(i);
            if (spart.get()==part.get()) {
                subfileidx = i;
                return &ret;
            }
        }
        return NULL;
    }

    virtual unsigned getPositionPart(offset_t pos, offset_t &base)
    {   // not very quick!
        CriticalBlock block (sect);
        unsigned n = numParts();
        base = 0;
        for (unsigned i=0;i<n;i++) {
            Owned<IDistributedFilePart> part = getPart(i);
            offset_t ps = part->getFileSize(true,false);
            if (ps==(offset_t)-1)
                break;
            if (ps>pos)
                return i;
            pos -= ps;
            base += ps;
        }
        return NotFound;
    }

    IDistributedFileIterator *getSubFileIterator(bool supersub=false)
    {
        CriticalBlock block (sect);
        return new cSubFileIterator(subfiles,supersub);
    }

    void updateFileAttrs()
    {
        if (subfiles.ordinality()==0) {
            StringBuffer desc;
            root->getProp("Attr/@description",desc);
            root->removeProp("Attr");       // remove all other attributes if superfile empty
            IPropertyTree *t=resetFileAttr(getEmptyAttr());
            if (desc.length())
                t->setProp("@description",desc.str());
            return;
        }
        root->removeProp("Attr/@size");
        root->removeProp("Attr/@compressedSize");
        root->removeProp("Attr/@checkSum");
        root->removeProp("Attr/@recordCount");  // recordCount not currently supported by superfiles
        root->removeProp("Attr/@formatCrc");    // formatCrc set if all consistant
        root->removeProp("Attr/@recordSize");   // record size set if all consistant
        root->removeProp("Attr/_record_layout");
        __int64 fs = getFileSize(false,false);
        if (fs!=-1)
            root->setPropInt64("Attr/@size",fs);
        if (isCompressed(NULL))
        {
            fs = getDiskSize(false,false);
            if (fs!=-1)
                root->setPropInt64("Attr/@compressedSize",fs);
        }
        unsigned checkSum;
        if (getFileCheckSum(checkSum))
            root->setPropInt64("Attr/@checkSum", checkSum);
        __int64 rc = getRecordCount();
        if (rc!=-1)
            root->setPropInt64("Attr/@recordCount",rc);
        unsigned fcrc;
        if (getFormatCrc(fcrc))
            root->setPropInt("Attr/@formatCrc", fcrc);
        size32_t rsz;
        if (getRecordSize(rsz))
            root->setPropInt("Attr/@recordSize", rsz);
        MemoryBuffer mb;
        if (getRecordLayout(mb)) 
            root->setPropBin("Attr/_record_layout", mb.length(), mb.bufferBase());

    }

    void updateParentFileAttrs(IDistributedFileTransaction *transaction)
    {
        Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
        StringBuffer pname;
        ForEach(*iter) {
            iter->query().getProp("@name",pname.clear());
            Owned<IDistributedSuperFile> psfile = transaction?transaction->lookupSuperFile(pname.str()):
                queryDistributedFileDirectory().lookupSuperFile(pname.str(),udesc,NULL);
            CDistributedSuperFile *file = QUERYINTERFACE(psfile.get(),CDistributedSuperFile);
            if (file) {
                {
                    DistributedFilePropertyLock lock(file);
                    file->setModified();
                    file->updateFileAttrs();
                }
                file->updateParentFileAttrs(transaction);
            }
        }
    }
    void validateAddSubFile(IDistributedFile *sub)
    {
        if (strcmp(sub->queryLogicalName(),queryLogicalName())==0)
            throw MakeStringException(-1,"addSubFile: Cannot add file %s to itself", queryLogicalName());
        if (subfiles.ordinality())
            checkFormatAttr(sub,"addSubFile");
        if (NotFound!=findSubFile(sub->queryLogicalName()))
            throw MakeStringException(-1,"addSubFile: File %s is already a subfile of %s", sub->queryLogicalName(),queryLogicalName());
    }

private:
    void doAddSubFile(IDistributedFile *_sub,bool before,const char *other,IDistributedFileTransactionExt *transaction) // takes ownership of sub
    {
        Owned<IDistributedFile> sub = _sub;
        validateAddSubFile(sub); // shouldn't really be necessary, was validated in transaction before here

        unsigned pos;
        if (other&&*other) {
            pos = findSubFileOrd(other);
            if (pos==NotFound)
                pos = findSubFile(other);
            if (pos==NotFound)
                pos = before?0:subfiles.ordinality();
            else if (!before&&(pos<subfiles.ordinality()))
                pos++;
        }
        else
            pos = before?0:subfiles.ordinality();
        if (pos > subfiles.ordinality())
            throw MakeStringException(-1,"addSubFile: Insert position %d out of range for file %s in superfile %s", pos+1, sub->queryLogicalName(), queryLogicalName());
        addItem(pos,sub.getClear());     // remove if failure TBD?
        setModified();
        updateFileAttrs();
        linkSubFile(pos);
    }

    bool doRemoveSubFiles(IDistributedFileTransactionExt *transaction)
    {
        // have to be quite careful here
        unsigned pos = subfiles.ordinality();
        if (pos)
        {
            DistributedFilePropertyLock lock(this);
            if (lock.needsReload())
                loadSubFiles(transaction,1000*60*10);
            pos = subfiles.ordinality();
            if (pos)
            {
                do
                {
                    pos--;
                    unlinkSubFile(pos);
                    removeItem(pos);
                } while (pos);
                setModified();
                updateFileAttrs();
                lock.unlock();
                updateParentFileAttrs(transaction);
            }
        }
        return true;
    }

    bool doRemoveSubFile(const char *subfile,
                         IDistributedFileTransactionExt *transaction)
    {
        // have to be quite careful here
        unsigned pos=findSubFileOrd(subfile);
        if ((pos==NotFound)||(pos>=subfiles.ordinality()))
            pos = findSubFile(subfile);
        if (pos==NotFound)
            return false;
        {
            DistributedFilePropertyLock lock(this);
            // don't reload subfiles here
            pos=findSubFileOrd(subfile);
            if ((pos==NotFound)||(pos>=subfiles.ordinality()))
                pos = findSubFile(subfile);
            if (pos==NotFound)
                return false;
            unlinkSubFile(pos);
            removeItem(pos);
            setModified();
            updateFileAttrs();
        }
        updateParentFileAttrs(transaction);
        return true;
    }

    bool doSwapSuperFile(IDistributedSuperFile *_file,
            IDistributedFileTransactionExt *transaction)
    {
        assertex(transaction);
        CDistributedSuperFile *file = QUERYINTERFACE(_file,CDistributedSuperFile);
        if (!file)
            return false;
        // Cache names (so we can delete without problems)
        StringArray subnames1;
        StringArray subnames2;
        for (unsigned i=0; i<this->numSubFiles(false); i++)
                subnames1.append(querySubFile(i, false).queryLogicalName());
        for (unsigned i=0; i<file->numSubFiles(false); i++)
                subnames2.append(file->querySubFile(i, false).queryLogicalName());
        // Delete all files
        ForEachItemIn(d1,subnames1) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames1.item(d1));
            if (!doRemoveSubFile(sub->queryLogicalName(), transaction))
                return false;
        }
        ForEachItemIn(d2,subnames2) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames2.item(d2));
            if (!file->doRemoveSubFile(sub->queryLogicalName(), transaction))
                return false;
        }
        // Add files swapped
        ForEachItemIn(a1,subnames1) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames1.item(a1));
            file->doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        ForEachItemIn(a2,subnames2) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames2.item(a2));
            doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        return true;
    }

public:
    void addSubFile(const char * subfile,
                    bool before=false,              // if true before other
                    const char *other=NULL,     // in NULL add at end (before=false) or start(before=true)
                    bool addcontents=false,
                    IDistributedFileTransaction *transaction=NULL
                   )
    {
        CriticalBlock block (sect);
        if (!subfile||!*subfile)
            return;
        checkModify("addSubFile");
        partscache.kill();

        // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
        Linked<IDistributedFileTransactionExt> localtrans;
        if (transaction)
        {
            IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
            localtrans.set(_transaction);
        }
        else
            localtrans.setown(new CDistributedFileTransaction(udesc, this));
        localtrans->ensureFile(this);

        if (addcontents)
        {
            localtrans->descend();
            StringArray subs;
            Owned<IDistributedSuperFile> sfile = localtrans->lookupSuperFile(subfile);
            if (sfile)
            {
                Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator();
                ForEach(*iter)
                    subs.append(iter->query().queryLogicalName());
            }
            sfile.clear();
            ForEachItemIn(i,subs)
                addSubFile(subs.item(i),before,other,false,localtrans);
            localtrans->ascend();
        }
        else
        {
            cAddSubFileAction *action = new cAddSubFileAction(queryLogicalName(),subfile,before,other);
            localtrans->addAction(action); // takes ownership
        }

        localtrans->autoCommit();
    }

    virtual bool removeSubFile(const char *subfile,         // if NULL removes all
                                bool remsub,                // if true removes subfiles from DFS
                                bool remcontents,           // if true, recurse super-files
                                IDistributedFileTransaction *transaction)
    {
        CriticalBlock block (sect);
        if (subfile&&!*subfile)
            return false;
        checkModify("removeSubFile");
        partscache.kill();

        // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
        Linked<IDistributedFileTransactionExt> localtrans;
        if (transaction)
        {
            IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
            localtrans.set(_transaction);
        }
        else
            localtrans.setown(new CDistributedFileTransaction(udesc, this));

        // Make sure this file is in cache (reuse below)
        localtrans->ensureFile(this);

        // If recurring, traverse super-file subs (if super)
        if (remcontents)
        {
            localtrans->descend();
            CDfsLogicalFileName logicalname;
            logicalname.set(subfile);
            IDistributedFile *sub = querySubFileNamed(logicalname.get(),false);
            if (!sub)
                return false;
            IDistributedSuperFile *sfile = sub->querySuperFile();
            if (sfile)
            {
                Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator(true);
                bool ret = true;
                StringArray toremove;
                ForEach(*iter)
                    toremove.append(iter->query().queryLogicalName());
                iter.clear();
                ForEachItemIn(i,toremove)
                {
                    if (!sfile->removeSubFile(toremove.item(i),remsub,false,localtrans))
                        ret = false;
                }
                if (!ret||!remsub)
                    return ret;
            }
            localtrans->ascend();
        }

        cRemoveSubFileAction *action = new cRemoveSubFileAction(queryLogicalName(),subfile,remsub);
        localtrans->addAction(action); // takes ownership
        localtrans->autoCommit();

        // MORE - auto-commit will throw an exception, change this to void
        return true;
    }

    virtual bool removeOwnedSubFiles(bool remsub, // if true removes subfiles from DFS
                                     IDistributedFileTransaction *transaction)
    {
        CriticalBlock block (sect);
        checkModify("removeOwnedSubFiles");
        partscache.kill();

        // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
        Linked<IDistributedFileTransactionExt> localtrans;
        if (transaction)
        {
            IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
            localtrans.set(_transaction);
        }
        else
            localtrans.setown(new CDistributedFileTransaction(udesc, this));

        // Make sure this file is in cache (reuse below)
        localtrans->addFile(this);

        cRemoveOwnedSubFilesAction *action = new cRemoveOwnedSubFilesAction(localtrans, queryLogicalName(), remsub);
        localtrans->addAction(action); // takes ownership
        localtrans->autoCommit();

        // MORE - auto-commit will throw an exception, change this to void
        return true;
    }

    virtual bool swapSuperFile( IDistributedSuperFile *_file,
                                IDistributedFileTransaction *transaction)
    {
        CriticalBlock block (sect);
        if (!_file)
            return false;
        checkModify("swapSuperFile");
        partscache.kill();

        // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
        Linked<IDistributedFileTransactionExt> localtrans;
        if (transaction)
        {
            IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
            localtrans.set(_transaction);
        }
        else
            localtrans.setown(new CDistributedFileTransaction(udesc, this));
        // Make sure this file is in cache
        localtrans->ensureFile(this);

        cSwapFileAction *action = new cSwapFileAction(queryLogicalName(),_file->queryLogicalName());
        localtrans->addAction(action); // takes ownership
        localtrans->autoCommit();

        return true;
    }

    void savePartsAttr(bool force)
    {
    }

    void fillClustersCache()
    {
        if (clusterscache.ordinality()==0) {
            StringBuffer name;
            ForEachItemIn(i,subfiles) {
                StringArray clusters;
                IDistributedFile &f=subfiles.item(i);
                unsigned nc = f.numClusters();
                for(unsigned j=0;j<nc;j++) {
                    f.getClusterName(j,name.clear());
                    if (clusterscache.find(name.str())==NotFound) {
                        IClusterInfo &cluster = *createClusterInfo(name.str(),f.queryClusterGroup(j),f.queryPartDiskMapping(j),&queryNamedGroupStore());
                        clusterscache.append(cluster);
                    }
                }
            }   
        }
    }

    unsigned getClusterNames(StringArray &clusters)
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.getNames(clusters);
    }

    unsigned numClusters()
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.ordinality();
    }
    
    unsigned findCluster(const char *clustername)
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.find(clustername);
    }

    ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum)
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.queryPartDiskMapping(clusternum);
    }

    void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec)
    {
        if (!clustername||!*clustername)
            return;
        CriticalBlock block (sect);
        fillClustersCache();
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.updatePartDiskMapping(clustername,spec);
        }       
    }

    IGroup *queryClusterGroup(unsigned clusternum)
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.queryGroup(clusternum);
    }

    StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name)
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.item(clusternum).getGroupName(name, &queryNamedGroupStore());
    }

    void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec)
    {
        if (!clustername||!*clustername)
            return;
        CriticalBlock block (sect);
        clusterscache.clear();
        subfiles.item(0).addCluster(clustername,mspec);
    }

    virtual bool removeCluster(const char *clustername)
    {
        bool clusterRemoved=false;
        CriticalBlock block (sect);
        clusterscache.clear();
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            clusterRemoved |= f.removeCluster(clustername);
        }       
        return clusterRemoved;
    }

    void setPreferredClusters(const char *clusters)
    {
        CriticalBlock block (sect);
        clusterscache.clear();
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.setPreferredClusters(clusters);
        }       
    }

    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) 
    {
        CriticalBlock block (sect);
        if (subfiles.ordinality()!=1) {
            err.append("only singleton superfiles allowed");
            return false;
        }
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            if (!f.checkClusterCompatible(fdesc,err))
                return false;
        }       
        return true;
    }


    void setSingleClusterOnly()
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.setSingleClusterOnly();
        }       
    }


    void enqueueReplicate()
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.enqueueReplicate();
        }       
    }

    bool getAccessedTime(CDateTime &dt)                     
    {
        bool set=false;
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            if (set)
                set = f.getAccessedTime(dt);
            else {
                CDateTime cmp;
                if (f.getAccessedTime(cmp)) {
                    if (cmp.compare(dt)>0)
                        dt.set(cmp);
                }
            }
        }
        return false;
    }

    void setAccessedTime(const CDateTime &dt)
    {
        {
            CriticalBlock block (sect);
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                f.setAccessedTime(dt);
            }       
        }
    }
};

// --------------------------------------------------------

void CDistributedFileTransaction::validateAddSubFile(IDistributedSuperFile *super, IDistributedFile *sub, const char *subName)
{
    CTransactionFile *trackedSuper = lookupTrackedFile(super);
    if (!trackedSuper)
        return;

    const char *superName = trackedSuper->queryName();
    if (strcmp(subName, superName)==0)
        throw MakeStringException(-1,"addSubFile: Cannot add file %s to itself", superName);
    if (trackedSuper->numSubFiles())
    {
        CDistributedSuperFile *sf = dynamic_cast<CDistributedSuperFile *>(super);
        sf->checkFormatAttr(sub, "addSubFile");
        if (trackedSuper->find(subName, false))
            throw MakeStringException(-1,"addSubFile: File %s is already a subfile of %s", subName, superName);
    }
}

// --------------------------------------------------------


CDistributedFilePart::CDistributedFilePart(CDistributedFile &_parent,unsigned _part,IPartDescriptor *pd)
  : parent(_parent)
{
    partIndex = _part;
    dirty = false;
    if (pd) {
        if (pd->isMulti())
            ERRLOG("Multi filenames not supported in Dali DFS Part %d of %s",_part+1,_parent.queryLogicalName());
        overridename.set(pd->queryOverrideName());
        setAttr(*pd->getProperties());
    }
    else
        ERRLOG("CDistributedFilePart::CDistributedFilePart no IPartDescriptor for part");
}

void CDistributedFilePart::Link(void) const 
{ 
    parent.Link(); 
    CInterface::Link(); 
}                     

bool CDistributedFilePart::Release(void) const
{ 
    parent.Release(); 
    return CInterface::Release(); 
}

offset_t CDistributedFilePart::getSize(bool checkCompressed)
{
    offset_t ret = (offset_t)-1;
    StringBuffer firstname;
    bool compressed = ::isCompressed(parent.queryAttributes());
    unsigned nc=parent.numCopies(partIndex);
    for (unsigned copy=0;copy<nc;copy++)
    {
        RemoteFilename rfn;
        try
        {
            Owned<IFile> partfile = createIFile(getFilename(rfn,copy));
            if (checkCompressed && compressed)
            {
                Owned<ICompressedFileIO> compressedIO = createCompressedFileReader(partfile);
                if (compressedIO)
                    ret = compressedIO->size();
            }
            else
                ret = partfile->size();
            if (ret!=(offset_t)-1)
                return ret;
        }
        catch (IException *e)
        {
            StringBuffer s("CDistributedFilePart::getSize ");
            rfn.getRemotePath(s);
            EXCLOG(e, s.str());
            e->Release();
        }
        if (copy==0)
            rfn.getRemotePath(firstname);
    }
    throw new CDFS_Exception(DFSERR_CannotFindPartFileSize,firstname.str());;
}

StringBuffer & CDistributedFilePart::getPartName(StringBuffer &partname)
{
    if (!overridename.isEmpty()) {
        if (isSpecialPath(overridename)) {
            // bit of a kludge 
            if (isPathSepChar(*overridename)&&partname.length()&&isPathSepChar(partname.charAt(partname.length()-1)))
                partname.setLength(partname.length()-1);
            return partname.append(overridename);
        }
        return partname.append(pathTail(overridename));
    }
    const char *mask=parent.queryPartMask();
    if (!mask||!*mask) {
        const char *err ="CDistributedFilePart::getPartName cannot determine part name (no mask)";
        ERRLOG("%s", err);
        throw MakeStringExceptionDirect(-1, err);
    }
    expandMask(partname,mask,partIndex,parent.numParts());
    return partname;
}

unsigned CDistributedFilePart::bestCopyNum(const IpAddress &ip,unsigned rel)
{
    unsigned n = numCopies();
    unsigned *dist = new unsigned[n];
    unsigned *idx = new unsigned[n];
    for (unsigned c=0;c<n;c++) {
        dist[c] = ip.ipdistance(queryNode(c)->endpoint());
        idx[c] = c;
    }
    if (rel>=n)
        rel = n-1;
    // do bubble sort as not that many!
    for (unsigned i=0; i<n-1; i++) 
        for (unsigned j=0; j<n-1-i; j++) 
            if (dist[idx[j+1]] < dist[idx[j]]) {  
                unsigned t = idx[j];         
                idx[j] = idx[j+1];
                idx[j+1] = t;
            }
    unsigned ret = idx[rel];
    delete [] idx;
    delete [] dist;
    return ret;
}

unsigned CDistributedFilePart::copyClusterNum(unsigned copy,unsigned *replicate)
{
    return parent.copyClusterNum(partIndex,copy,replicate);
}

StringBuffer &CDistributedFilePart::getPartDirectory(StringBuffer &ret,unsigned copy)
{
    const char *defdir = parent.queryDefaultDir();
    StringBuffer dir;
    const char *pn;
    if (overridename.isEmpty())
        pn = parent.queryPartMask();
    else {
        pn = overridename.get();
        if (isSpecialPath(pn))  // its a query
            return ret; // ret.append('/');     // not sure if really need '/' here
    }
    if (pn&&*pn) {
        StringBuffer odir;
        splitDirTail(pn,odir);
        if (odir.length()) {
            if (isAbsolutePath(pn))
                dir.append(odir);
            else if (defdir&&*defdir)
                addPathSepChar(dir.append(defdir)).append(odir);
        }
        else
            dir.append(defdir);
    }
    if (dir.length()==0)
        ERRLOG("IDistributedFilePart::getPartDirectory unable to determine part directory");
    else {
        parent.adjustClusterDir(partIndex,copy,dir);
        ret.append(dir);
    }
    return ret;
}

unsigned CDistributedFilePart::numCopies()
{
    return parent.numCopies(partIndex);
}

INode *CDistributedFilePart::queryNode(unsigned copy)
{
    return parent.queryNode(partIndex,copy);
}

unsigned CDistributedFilePart::queryDrive(unsigned copy)
{
    return parent.queryDrive(partIndex,copy,parent.directory);
}

bool CDistributedFilePart::isHost(unsigned copy)
{
    return (queryNode(copy)->isHost()); 
}


IPropertyTree &CDistributedFilePart::queryAttributes()
{ 
    CriticalBlock block (sect);     // avoid nested blocks
    if (attr) 
        return *attr;
    WARNLOG("CDistributedFilePart::queryAttributes missing part attributes");
    attr.setown(getEmptyAttr());
    return *attr;
}

RemoteFilename &CDistributedFilePart::getFilename(RemoteFilename &ret,unsigned copy)
{
    // this is probably not as efficient as could be
    StringBuffer fullpath;
    getPartDirectory(fullpath,copy);
    addPathSepChar(fullpath);
    getPartName(fullpath);
    SocketEndpoint ep;
    INode *node=queryNode(copy);
    if (node)
        ep = node->endpoint();
    ret.setPath(ep,fullpath.str());
    return ret;
}

bool CDistributedFilePart::getCrc(unsigned &crc)
{
    return getCrcFromPartProps(parent.queryAttributes(),queryAttributes(), crc);
}

unsigned CDistributedFilePart::getPhysicalCrc()
{
    StringBuffer firstname;
    unsigned nc=parent.numCopies(partIndex);
    for (unsigned copy=0;copy<nc;copy++) {
        RemoteFilename rfn;
        try {
            Owned<IFile> partfile = createIFile(getFilename(rfn,copy));
            if (partfile&&partfile->exists()) 
                return partfile->getCRC();
        }
        catch (IException *e)
        {
            StringBuffer s("CDistributedFilePart::getPhysicalCrc ");
            rfn.getRemotePath(s);
            EXCLOG(e, s.str());
            e->Release();
        }
        if (copy==0)
            rfn.getRemotePath(firstname);
    }
    IDFS_Exception *e = new CDFS_Exception(DFSERR_CannotFindPartFileCrc,firstname.str());
    throw e;
}

// TODO: Create DistributedFilePropertyLock for parts
bool CDistributedFilePart::lockProperties(unsigned timeoutms)
{
    dirty = true;
    return parent.lockProperties(timeoutms);
}

// TODO: Create DistributedFilePropertyLock for parts
void CDistributedFilePart::unlockProperties(DFTransactionState state=TAS_NONE)
{
    parent.unlockProperties(state);
}

offset_t CDistributedFilePart::getFileSize(bool allowphysical,bool forcephysical)
{
    offset_t ret = (offset_t)((forcephysical&&allowphysical)?-1:queryAttributes().getPropInt64("@size", -1));
    if (allowphysical&&(ret==(offset_t)-1))
        ret = getSize(true);
    return ret;
}

offset_t CDistributedFilePart::getDiskSize(bool allowphysical,bool forcephysical)
{
    if (!::isCompressed(parent.queryAttributes()))
        return getFileSize(allowphysical, forcephysical);

    if (forcephysical && allowphysical)
        return getSize(false); // i.e. only if force, because all compressed should have @compressedSize attribute

    // NB: compressSize is disk size
    return queryAttributes().getPropInt64("@compressedSize", -1);
}

bool CDistributedFilePart::getModifiedTime(bool allowphysical,bool forcephysical, CDateTime &dt)
{
    StringBuffer s;
    if (!forcephysical&&queryAttributes().getProp("@modified", s)) {
        dt.setString(s.str());
        if (!dt.isNull())
            return true;
    }
    if (allowphysical) {
        unsigned nc=parent.numCopies(partIndex);
        for (unsigned copy=0;copy<nc;copy++) {
            RemoteFilename rfn;
            try {
                Owned<IFile> partfile = createIFile(getFilename(rfn,copy));
                if (partfile->getTime(NULL,&dt,NULL))
                    return true;
            }
            catch (IException *e)
            {
                StringBuffer s("CDistributedFilePart::getFileTime ");
                rfn.getRemotePath(s);
                EXCLOG(e, s.str());
                e->Release();
            }
        }
    }
    return false;
}

unsigned getSuperFileSubs(IDistributedSuperFile *super, IArrayOf<IDistributedFile> &subFiles, bool superSub)
{
    unsigned numSubs = super->numSubFiles(superSub);
    for (unsigned s=0; s<numSubs; s++)
    {
        IDistributedFile &subFile = super->querySubFile(s, superSub);
        subFiles.append(*LINK(&subFile));
    }
    return numSubs;
}


// --------------------------------------------------------

class CNamedGroupIterator: public CInterface, implements INamedGroupIterator
{
    Owned<IPropertyTreeIterator> pe;
    Linked<IRemoteConnection> conn;
    Linked<IGroup> matchgroup;
    bool exactmatch;


    bool match();

public:
    IMPLEMENT_IINTERFACE;
    CNamedGroupIterator(IRemoteConnection *_conn,IGroup *_matchgroup=NULL,bool _exactmatch=false)
        : conn(_conn), matchgroup(_matchgroup)
    {
        exactmatch = _exactmatch;
        if (matchgroup.get()) {
            StringBuffer query;
            query.append("Group[Node/@ip=\"");
            matchgroup->queryNode(0).endpoint().getUrlStr(query);
            query.append("\"]");
            pe.setown(conn->getElements(query.str())); 
        }
        else
            pe.setown(conn->queryRoot()->getElements("Group"));
    }

    bool first()
    {
        if (!pe->first())
            return false;
        if (match())
            return true;
        return next();
    }
    bool next()
    {
        while (pe->next()) 
            if (match())
                return true;
        return false;
    }
    bool isValid()
    {
        return pe->isValid();
    }
    StringBuffer &get(StringBuffer &name)
    {
        pe->query().getProp("@name",name);
        return name;
    }
    StringBuffer &getdir(StringBuffer &dir)
    {
        pe->query().getProp("@dir",dir);
        return dir;
    }
    bool isCluster()
    {
        return pe->query().getPropBool("@cluster");
    }
};

// --------------------------------------------------------

#define GROUP_CACHE_INTERVAL (1000*60)
#define GROUP_EXCEPTION_CACHE_INTERVAL (1000*60*10)

static GroupType translateGroupType(const char *groupType)
{
    if (!groupType)
        return grp_unknown;
    if (strieq(groupType, "Thor"))
        return grp_thor;
    else if (strieq(groupType, "Roxie"))
        return grp_roxie;
    else if (strieq(groupType, "hthor"))
        return grp_hthor;
    else
        return grp_unknown;
}

class CNamedGroupCacheEntry: public CInterface
{
public:
    Linked<IGroup> group;
    StringAttr name;
    StringAttr groupDir;
    GroupType groupType;
    Linked<IException> exception;

    CNamedGroupCacheEntry(IGroup *_group, const char *_name, const char *_dir, GroupType _groupType)
    : group(_group), name(_name), groupDir(_dir), groupType(_groupType)
    {
        cachedtime = msTick();
    }

    CNamedGroupCacheEntry(IException *_exception, const char *_name)
    : exception(_exception), name(_name), groupType(grp_unknown)
    {
        cachedtime = msTick();
    }

    bool expired(unsigned timeNow)
    {
        if (exception)
            return timeNow-cachedtime > GROUP_EXCEPTION_CACHE_INTERVAL;
        else
            return timeNow-cachedtime > GROUP_CACHE_INTERVAL;
    }
protected:
    unsigned cachedtime;
};

class CNamedGroupStore: public CInterface, implements INamedGroupStore
{
    CriticalSection cachesect;
    CIArrayOf<CNamedGroupCacheEntry> cache;
    unsigned defaultTimeout;
    unsigned defaultRemoteTimeout;

public:
    IMPLEMENT_IINTERFACE;

    CNamedGroupStore()
    {
        defaultTimeout = INFINITE;
        defaultRemoteTimeout = FOREIGN_DALI_TIMEOUT;
    }

    IGroup *dolookup(const char *logicalgroupname,IRemoteConnection *conn, StringBuffer *dirret, GroupType &groupType)
    {
        SocketEndpointArray epa;
        StringBuffer gname(logicalgroupname);
        gname.trim();
        if (!gname.length())
            return NULL;
        gname.toLowerCase();
        logicalgroupname = gname.str();
        bool isiprange = (*logicalgroupname!=0);
        for (const char *s1=logicalgroupname;*s1;s1++)
            if (isalpha(*s1)) {
                isiprange = false;
                break;
            }
        if (isiprange) { 
            // allow IP or IP list instead of group name
            // I don't think this is a security problem as groups not checked
            // NB ports not allowed here

            char *buf = strdup(logicalgroupname);
            char *s = buf;
            while (*s) {
                char *next = strchr(s,',');
                if (next) 
                    *next = 0;
                SocketEndpoint ep;
                unsigned n = ep.ipsetrange(s);
                for (unsigned i=0;i<n;i++) {
                    if (ep.isNull()) { // failed
                        epa.kill();
                        break;
                    }
                    epa.append(ep);
                    ep.ipincrement(1);
                }
                if (!next)
                    break;
                s = next+1;
            }
            free(buf);
            if (epa.ordinality())
            {
                groupType = grp_unknown;
                return createIGroup(epa);
            }
        }
        StringBuffer range;
        StringBuffer parent;
        if (decodeChildGroupName(gname.str(),parent,range)) {
            gname.clear().append(parent);
            logicalgroupname = gname.str();
        }
        StringAttr groupdir;
        GroupType type;
        bool cached = false;
        unsigned timeNow = msTick();
        {
            CriticalBlock block(cachesect);
            ForEachItemInRev(idx, cache)
            {
                CNamedGroupCacheEntry &entry = cache.item(idx);
                if (entry.expired(timeNow))
                {
                    cache.remove(idx);
                }
                else if (strcmp(gname.str(),entry.name.get())==0)
                {
                    cached = true;
                    if (entry.exception)
                        throw LINK(entry.exception);
                    if (range.length()==0)
                    {
                        if (dirret)
                            dirret->append(entry.groupDir);
                        groupType = entry.groupType;
                        return entry.group.getLink();
                    }
                    // there is a range so copy to epa
                    entry.group->getSocketEndpoints(epa);
                    groupdir.set(entry.groupDir);
                    type = entry.groupType;
                    break;
                }
            }
        }
        try
        {
            if ((gname.length()>9)&&(memcmp(logicalgroupname,"foreign::",9)==0))
            {
                StringBuffer eps;
                const char *s = logicalgroupname+9;
                while (*s&&((*s!=':')||(s[1]!=':')))
                    eps.append(*(s++));
                if (*s)
                {
                    s+=2;
                    if (*s)
                    {
                        Owned<INode> dali = createINode(eps.str());
                        if (!dali || !getRemoteGroup(dali, s, defaultRemoteTimeout, groupdir, type, epa))
                        {
                            if (!cached)
                            {
                                CriticalBlock block(cachesect);
                                cache.append(*new CNamedGroupCacheEntry(NULL, gname, NULL, grp_unknown));
                            }
                            return NULL;
                        }
                    }
                }
            }
            else if (epa.ordinality()==0) {
                struct sLock
                {
                    sLock()  { lock = NULL; };
                    ~sLock() { delete lock; };
                    CConnectLock *lock;
                } slock;
                if (!conn)
                {
                    slock.lock = new CConnectLock("CNamedGroup::lookup", SDS_GROUPSTORE_ROOT, false, false, false, defaultTimeout);
                    conn = slock.lock->conn;
                    if (!conn)
                    {
                        if (!cached)
                        {
                            CriticalBlock block(cachesect);
                            cache.append(*new CNamedGroupCacheEntry(NULL, gname, NULL, grp_unknown));
                        }
                        return NULL;
                    }
                }
                Owned<IPropertyTree> pt = getNamedPropTree(conn->queryRoot(),"Group","@name",gname.str(),true);
                if (!pt)
                    return NULL;
                type = translateGroupType(pt->queryProp("@kind"));
                groupdir.set(pt->queryProp("@dir"));
                if (groupdir.isEmpty())
                    groupdir.set(queryBaseDirectory(type));
                Owned<IPropertyTreeIterator> pe2 = pt->getElements("Node");
                ForEach(*pe2)
                {
                    SocketEndpoint ep(pe2->query().queryProp("@ip"));
                    epa.append(ep);
                }
            }
        }
        catch (IException *E)
        {
            // cache the exception
            CriticalBlock block(cachesect);
            cache.append(*new CNamedGroupCacheEntry(E, gname));
            throw;
        }
        Owned<IGroup> ret = createIGroup(epa);
        if (!cached)
        {
            CriticalBlock block(cachesect);
            cache.append(*new CNamedGroupCacheEntry(ret, gname, groupdir, type));
        }
        if (range.length())
        {
            SocketEndpointArray epar;
            const char *s = range.str();
            while (*s)
            {
                unsigned start = 0;
                while (isdigit(*s))
                {
                    start = start*10+*s-'0';
                    s++;
                }
                if (!start)
                    break;
                unsigned end;
                if (*s=='-')
                {
                    s++;
                    end = 0;
                    while (isdigit(*s))
                    {
                        end = end*10+*s-'0';
                        s++;
                    }
                    if (!end)
                        end = epa.ordinality();
                }
                else
                    end = start;
                if ((start>epa.ordinality())||(end>epa.ordinality()))
                {
                    s = range.str();
                    break;
                }
                if (*s==',')
                    s++;
                unsigned i=start-1;
                do {                        // allow 400-1 etc
                    i++;
                    if (i>epa.ordinality())
                        i = 1;
                    epar.append(epa.item(i-1));
                } while (i!=end);
            }
            if (*s)
                throw MakeStringException(-1,"Invalid group range %s",range.str());
            ret.setown(createIGroup(epar));
        }
        if (dirret)
            dirret->append(groupdir);
        groupType = type;
        return ret.getClear();
    }

    IGroup *lookup(const char *logicalgroupname)
    {
        GroupType dummy;
        return dolookup(logicalgroupname, NULL, NULL, dummy);
    }

    IGroup *lookup(const char *logicalgroupname, StringBuffer &dir, GroupType &groupType)
    {
        return dolookup(logicalgroupname, NULL, &dir, groupType);
    }

    INamedGroupIterator *getIterator()
    {
        CConnectLock connlock("CNamedGroup::getIterator", SDS_GROUPSTORE_ROOT, false, true, false, defaultTimeout);
        return new CNamedGroupIterator(connlock.conn); // links connection
    }

    INamedGroupIterator *getIterator(IGroup *match,bool exact)
    {
        CConnectLock connlock("CNamedGroup::getIterator", SDS_GROUPSTORE_ROOT, false, false, false, defaultTimeout);
        return new CNamedGroupIterator(connlock.conn,match,exact); // links connection
    }

    void doadd(CConnectLock &connlock,const char *name,IGroup *group,bool cluster,const char *dir)
    {
        if (!group)
            return;
        IPropertyTree *val = createPTree("Group");
        val->setProp("@name",name);
        if (cluster)
            val->setPropBool("@cluster", true);
        if (dir)
            val->setProp("@dir",dir);

        INodeIterator &gi = *group->getIterator();
        StringBuffer str;
        ForEach(gi) {
            IPropertyTree *n = createPTree("Node");
            n = val->addPropTree("Node",n);
            gi.query().endpoint().getIpText(str.clear());
            n->setProp("@ip",str.str());
        }
        gi.Release();
        connlock.conn->queryRoot()->addPropTree("Group",val);
    }
    
    void addUnique(IGroup *group,StringBuffer &lname, const char *dir)
    {
        if (group->ordinality()==1)
        {
            group->getText(lname);
            return;
        }
        CConnectLock connlock("CNamedGroup::addUnique", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        StringBuffer name;
        StringBuffer prop;
        unsigned scale = 16;
        loop {
            name.clear();
            if (lname.length()) { // try suggested name
                name.append(lname);
                name.toLowerCase();
                lname.clear();
            }
            else 
                name.append("__anon").append(getRandom()%scale);
            prop.clear().appendf("Group[@name=\"%s\"]",name.str());
            if (!connlock.conn->queryRoot()->hasProp(prop.str()))
                break;
            scale*=2;
        }
        doadd(connlock,name.str(),group,false,dir);
        lname.append(name);
    }
    
    void add(const char *logicalgroupname, IGroup *group, bool cluster, const char *dir, GroupType groupType)
    {
        StringBuffer name(logicalgroupname);
        name.toLowerCase();
        name.trim();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name.str());
        CConnectLock connlock("CNamedGroup::add", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        connlock.conn->queryRoot()->removeProp(prop.str()); 
        doadd(connlock,name.str(),group,cluster,dir);
        {                                                           
            CriticalBlock block(cachesect);
            cache.kill();
            if (group)
                cache.append(*new CNamedGroupCacheEntry(group, name.str(), dir, groupType));
        }
    }

    void remove(const char *logicalgroupname)
    {
        add(logicalgroupname, NULL, false, NULL, grp_unknown);
    }

    bool find(IGroup *grp, StringBuffer &gname, bool add)
    {
        // gname on entry is suggested name for add if set
        unsigned n = grp->ordinality();
        if (!grp||!n)
            return false;
        Owned<INamedGroupIterator> iter=getIterator(grp,(n==1));     // one node clusters must be exact match
        StringAttr bestname;
        StringBuffer name;
        ForEach(*iter) {
            bool iscluster = iter->isCluster();
            if (iscluster||(bestname.isEmpty())) { 
                iter->get(name.clear());
                if (name.length()) {
                    bestname.set(name);
                    if (iscluster)
                        break;
                }
            }
        }
        iter.clear();
        if (bestname.isEmpty()) {
            if (add||(n==1)) // single-nodes always have implicit group of IP
                addUnique(grp,gname,NULL);
            return false;
        }
        gname.clear().append(bestname);
        return true;
    }

    void swapNode(const IpAddress &from, const IpAddress &to)
    {
        if (from.ipequals(to))
            return;
        CConnectLock connlock("CNamedGroup::swapNode", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        StringBuffer froms;
        from.getIpText(froms);
        StringBuffer tos;
        to.getIpText(tos);
        Owned<IPropertyTreeIterator> pe  = connlock.conn->queryRoot()->getElements("Group");
        ForEach(*pe) {
            IPropertyTree &group = pe->query();
            const char *kind = group.queryProp("@kind");
            if (kind && streq("Spare", kind))
                continue;
            StringBuffer name;
            group.getProp("@name",name);
            StringBuffer xpath("Node[@ip = \"");
            xpath.append(froms).append("\"]");
            for (unsigned guard=0; guard<1000; guard++) {
                Owned<IPropertyTreeIterator> ne = group.getElements(xpath.str());
                if (!ne->first()) 
                    break;
                ne->query().setProp("@ip",tos.str());
                PROGLOG("swapNode swapping %s for %s in group %s",froms.str(),tos.str(),name.str());
            }
        }
        CriticalBlock block(cachesect);
        cache.kill();
    }

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
    }

    unsigned setRemoteTimeout(unsigned timems)
    {
        unsigned ret = defaultRemoteTimeout;
        defaultRemoteTimeout = timems;
        return ret;
    }

    void resetCache()
    {
        CriticalBlock block(cachesect);
        cache.kill();
    }
private:
    bool getRemoteGroup(const INode *foreigndali, const char *gname, unsigned foreigndalitimeout,
                           StringAttr &groupdir, GroupType &type, SocketEndpointArray &epa)
    {
        StringBuffer lcname(gname);
        gname = lcname.trim().toLowerCase().str();
        CMessageBuffer mb;
        mb.append((int)MDFS_GET_GROUP_TREE).append(gname);
        size32_t mbsz = mb.length();
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
        checkDfsReplyException(mb);
        if (mb.length()==0)
            return false;
        byte ok;
        mb.read(ok);
        if (ok!=1) {
            // kludge for prev bug
            if ((ok==(byte)MDFS_GET_GROUP_TREE)&&mb.length()>mbsz) {
                mb.skip(mbsz-1);
                mb.read(ok);
                if (ok!=1) 
                    return false;
            }
            else
                return false;
        }
        Owned<IPropertyTree> pt = createPTree(mb);
        Owned<IPropertyTreeIterator> pe = pt->getElements("Node");
        groupdir.set(pt->queryProp("@dir"));
        type = translateGroupType(pt->queryProp("@kind"));
        ForEach(*pe) {
            SocketEndpoint ep(pe->query().queryProp("@ip"));
            epa.append(ep);
        }
        return epa.ordinality() > 0;
    }

};

static CNamedGroupStore *groupStore = NULL;
static CriticalSection groupsect;

bool CNamedGroupIterator::match()
{
    if (conn.get()) {
        if (matchgroup.get()) {
            if (!groupStore)
                return false;
            const char *name = pe->query().queryProp("@name");
            if (!name||!*name)
                return false;
            GroupType dummy;
            Owned<IGroup> lgrp = groupStore->dolookup(name, conn, NULL, dummy);
            if (lgrp) {
                if (exactmatch)
                    return lgrp->equals(matchgroup);
                GroupRelation gr = matchgroup->compare(lgrp);
                return (gr==GRidentical)||(gr==GRbasesubset)||(gr==GRwrappedsuperset);
            }
        }
        else
            return true;
    }
    return false;
}

INamedGroupStore  &queryNamedGroupStore()
{
    if (!groupStore) {
        CriticalBlock block(groupsect);
        if (!groupStore) 
            groupStore = new CNamedGroupStore();
    }
    return *groupStore;
}

// --------------------------------------------------------

IDistributedFile *CDistributedFileDirectory::lookup(const char *_logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName logicalname;    
    logicalname.set(_logicalname);
    return lookup(logicalname, user, writeattr, hold, lockSuperOwner, transaction, timeout);
}

IDistributedFile *CDistributedFileDirectory::dolookup(CDfsLogicalFileName &_logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName *logicalname = &_logicalname;
    if (logicalname->isMulti()) 
        // don't bother checking because the sub file creation will
        return new CDistributedSuperFile(this,*logicalname,user,transaction); // temp superfile
    if (strchr(logicalname->get(), '*')) // '*' only wildcard supported. NB: This is a temporary fix (See: HPCC-12523)
        throw MakeStringException(-1, "Invalid filename lookup: %s", logicalname->get());
    Owned<IDfsLogicalFileNameIterator> redmatch;
    loop
    {
        checkLogicalName(*logicalname,user,true,writeattr,true,NULL);
        if (logicalname->isExternal()) {
            CDateTime modTime;
            Owned<IFileDescriptor> fDesc = getExternalFileDescriptor(logicalname->get(), &modTime);
            if (!fDesc)
                return NULL;
            CDistributedFile *ret = new CDistributedFile(this, fDesc, NULL, true);
            ret->setLogicalName(logicalname->get());
            ret->setModificationTime(modTime);
            return ret;
        }
        if (logicalname->isForeign()) {
            IDistributedFile * ret = getFile(logicalname->get(),user,NULL);
            if (ret)
                return ret;
        }
        else {
            unsigned start = 0;
            loop {
                CFileLock fcl;
                unsigned mode = RTM_LOCK_READ | RTM_SUB;
                if (hold) mode |= RTM_LOCK_HOLD;
                if (!fcl.init(*logicalname, mode, timeout, "CDistributedFileDirectory::lookup"))
                    break;
                CFileSuperOwnerLock superOwnerLock;
                if (lockSuperOwner)
                    verifyex(superOwnerLock.init(*logicalname, NULL, defaultTimeout, "CDistributedFileDirectory::dolookup(SuperOwnerLock)"));
                if (fcl.getKind() == DXB_File)
                {
                    StringBuffer cname;
                    if (logicalname->getCluster(cname).length())
                    {
                        IPropertyTree *froot=fcl.queryRoot();
                        if (froot)
                        {
                            StringBuffer query;
                            query.appendf("Cluster[@name=\"%s\"]",cname.str());
                            if (!froot->hasProp(query.str())) 
                                break;
                        }
                    }
                    CDistributedFile *ret = new CDistributedFile(this,fcl.detach(),*logicalname,user);  // found
                    ret->setSuperOwnerLock(superOwnerLock.detach());
                    return ret;
                }
                // now super file
                if (fcl.getKind() != DXB_SuperFile)
                    break;
                if (start==0)
                    start = msTick();
                unsigned elapsed;
                try
                {
                    CDistributedSuperFile *ret = new CDistributedSuperFile(this,fcl.detach(),*logicalname,user,transaction,SDS_SUB_LOCK_TIMEOUT);
                    ret->setSuperOwnerLock(superOwnerLock.detach());
                    return ret;
                }
                catch (IDFS_Exception *e)
                {
                    elapsed = msTick()-start;
                    if ((e->errorCode()!=DFSERR_LookupConnectionTimout)||(elapsed>((timeout==INFINITE)?SDS_CONNECT_TIMEOUT:timeout)))
                        throw;
                    EXCLOG(e,"Superfile lookup");
                    e->Release();
                }
                PROGLOG("CDistributedSuperFile connect timeout (%dms) pausing",elapsed);
                Sleep(SDS_TRANSACTION_RETRY/2+(getRandom()%SDS_TRANSACTION_RETRY)); 
            }
        }
        if (redmatch.get()) {
            if (!redmatch->next())
                break;
        }
        else {
            redmatch.setown(queryRedirection().getMatch(logicalname->get()));
            if (!redmatch.get()) 
                break;
            if (!redmatch->first())
                break;
        }
        logicalname = &redmatch->query();

    }
    return NULL;
}

IDistributedFile *CDistributedFileDirectory::lookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, bool writeattr, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout)
{
    return dolookup(logicalname, user, writeattr, hold, lockSuperOwner, transaction, timeout);
}

IDistributedSuperFile *CDistributedFileDirectory::lookupSuperFile(const char *_logicalname,IUserDescriptor *user,IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName logicalname;    
    logicalname.set(_logicalname);
    IDistributedFile *file = dolookup(logicalname, user, false, false, false, transaction, timeout);
    if (file) {
        IDistributedSuperFile *sf = file->querySuperFile();
        if (sf)
            return sf;
        file->Release();
    }
    return NULL;
}

bool CDistributedFileDirectory::isSuperFile(    const char *logicalname,
                                                IUserDescriptor *user,
                                                INode *foreigndali,
                                                unsigned timeout)
{
    Owned<IPropertyTree> tree = getFileTree(logicalname, user, foreigndali,timeout, false);
    return tree.get()&&(strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0);
}


bool CDistributedFileDirectory::exists(const char *_logicalname,IUserDescriptor *user,bool notsuper,bool superonly)
{
    // (currently) no check on scope permissions for exists

    bool external;
    bool foreign;
    CDfsLogicalFileName dlfn;   
    dlfn.set(_logicalname);
    const char *logicalname = dlfn.get();
    external = dlfn.isExternal();
    foreign = dlfn.isForeign();
    if (foreign) {
        Owned<IDistributedFile> file = lookup(_logicalname, user, false, false, false, NULL, defaultTimeout);
        if (file.get()==NULL)
            return false;
        if (file->querySuperFile()) {
            if (notsuper)
                return false;
        }
        else
            if (superonly)
                return false;
    }
    else if (external) {
        if (!existsPhysical(_logicalname,user))
            return false;
    }
    else {
        StringBuffer str;
        if (!superonly) {
            dlfn.makeFullnameQuery(str,DXB_File,true);
            CConnectLock connlockfile("CDistributedFileDirectory::exists",str.str(),false,false,false,defaultTimeout);
            if (connlockfile.conn.get())
                return true;
        }
        if (notsuper)
            return false;
        dlfn.makeFullnameQuery(str.clear(),DXB_SuperFile,true);
        CConnectLock connlocksuper("CDistributedFileDirectory::exists",str.str(),false,false,false,defaultTimeout);
        if (!connlocksuper.conn.get())
            return false;
    }
    return true;
}

bool CDistributedFileDirectory::existsPhysical(const char *_logicalname, IUserDescriptor *user)
{
    Owned<IDistributedFile> file = lookup(_logicalname, user, false, false, false, NULL, defaultTimeout);
    if (!file)
        return false;
    return file->existsPhysicalPartFiles(0);
}

IDistributedFile *CDistributedFileDirectory::createNew(IFileDescriptor *fdesc, bool includeports)
{
    return new CDistributedFile(this, fdesc, NULL, includeports);
}

////////////////////////////////////

class DistributedFilePropertyLockFree
{
    unsigned lockedCount;
    CDistributedFile *file;
    CDistributedSuperFile *sfile;
public:
    DistributedFilePropertyLockFree(IDistributedFile *_file)
    {
        file = dynamic_cast<CDistributedFile *>(_file);
        sfile = NULL;
        if (file)
            lockedCount = file->setPropLockCount(0);
        else
        {
            sfile = dynamic_cast<CDistributedSuperFile *>(_file);
            lockedCount = sfile->setPropLockCount(0);
        }
    }
    ~DistributedFilePropertyLockFree()
    {
        if (file)
            verifyex(0 == file->setPropLockCount(lockedCount));
        else if (sfile)
            verifyex(0 == sfile->setPropLockCount(lockedCount));
    }
};

/**
 * Creates a super-file within a transaction.
 */
class CCreateSuperFileAction: public CDFAction
{
    CDfsLogicalFileName logicalname;
    CDistributedFileDirectory *parent;
    Linked<IDistributedSuperFile> super;
    IUserDescriptor *user;
    bool interleaved, created;

    void clearSuper()
    {
        if (created)
        {
            created = false;
            super->detach();
        }
        super.clear();
    }
public:
    CCreateSuperFileAction(CDistributedFileDirectory *_parent,
                           IUserDescriptor *_user,
                           const char *_flname,
                           bool _interleaved)
        : parent(_parent), user(_user), created(false), interleaved(_interleaved)
    {
        logicalname.set(_flname);
    }
    IDistributedSuperFile *getSuper()
    {
        if (!super)
        {
            Owned<IDistributedSuperFile> _super = transaction->lookupSuperFile(logicalname.get(), SDS_SUB_LOCK_TIMEOUT);
            if (_super)
                super.setown(_super.getClear());
            else
            {
                /* No super, create one if necessary.
                 * This really shouldn't have to work this way, looking up super early, or creating super stub now,
                 * because other super file transactions are based on
                 * TBD: There should be a way to obtain lock independently of actually attaching.
                 */
                Owned<IPropertyTree> root = createPTree();
                root->setPropInt("@interleaved",interleaved?2:0); // this is ill placed
                super.setown(new CDistributedSuperFile(parent, root, logicalname, user));
                created = true;
                transaction->addFile(super);
            }
        }
        return super.getLink();
    }
    bool prepare()
    {
        Owned<IDistributedFile> _super = getSuper();
        // Attach the file to DFS, if wasn't there already
        if (created)
            super->attach(logicalname.get(), user);
        addFileLock(super);
        if (lock())
            return true;
        unlock();
        return false;
    }
    void run()
    {
        // Do nothing, file is already created
    }
    void retry()
    {
        // on retry, we need to remove the file so next lock doesn't fail
        clearSuper();
        CDFAction::retry();
    }
    void rollback()
    {
        state = TAS_FAILURE;
        clearSuper();
        CDFAction::rollback();
    }
};

/**
 * Removes a super-file within a transaction.
 */
class CRemoveSuperFileAction: public CDFAction
{
    CDfsLogicalFileName logicalname;
    Linked<IDistributedSuperFile> super;
    IUserDescriptor *user;
    bool delSub;
    Owned<IDistributedFileTransactionExt> nestedTransaction;

    class CNestedTransaction : public CDistributedFileTransaction
    {
        IDistributedFileTransactionExt *transaction;
        CIArrayOf<CDFAction> actions;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        CNestedTransaction(IDistributedFileTransactionExt *_transaction, IUserDescriptor *user)
            : CDistributedFileTransaction(user), transaction(_transaction)
        {
            if (transaction->active())
                start();
        }
        // wrap rest of calls into parent transaction calls
        virtual IDistributedFile *lookupFile(const char *lfn,unsigned timeout=INFINITE)
        {
            return transaction->lookupFile(lfn, timeout);
        }
        virtual IDistributedSuperFile *lookupSuperFile(const char *slfn,unsigned timeout=INFINITE)
        {
            return transaction->lookupSuperFile(slfn, timeout);
        }
        virtual IUserDescriptor *queryUser() { return transaction->queryUser(); }
        virtual void descend() { transaction->descend(); }
        virtual void ascend() { transaction->ascend(); }
        virtual void addFile(IDistributedFile *file) { transaction->addFile(file); }
        virtual void ensureFile(IDistributedFile *file) { transaction->ensureFile(file); }
        virtual void clearFile(IDistributedFile *file) { transaction->clearFile(file); }
        virtual void noteAddSubFile(IDistributedSuperFile *super, const char *superName, IDistributedFile *sub)
        {
            transaction->noteAddSubFile(super, superName, sub);
        }
        virtual void noteRemoveSubFile(IDistributedSuperFile *super, IDistributedFile *sub)
        {
            transaction->noteRemoveSubFile(super, sub);
        }
        virtual void noteSuperSwap(IDistributedSuperFile *super1, IDistributedSuperFile *super2)
        {
            transaction->noteSuperSwap(super1, super2);
        }
        virtual void clearSubFiles(IDistributedSuperFile *super)
        {
            transaction->clearSubFiles(super);
        }
        virtual void noteRename(IDistributedFile *file, const char *newName)
        {
            transaction->noteRename(file, newName);
        }
        virtual void validateAddSubFile(IDistributedSuperFile *super, IDistributedFile *sub, const char *subName)
        {
            transaction->validateAddSubFile(super, sub, subName);
        }
        virtual bool isSubFile(IDistributedSuperFile *super, const char *subFile, bool sub)
        {
            return transaction->isSubFile(super, subFile, sub);
        }
        virtual bool addDelayedDelete(CDfsLogicalFileName &lfn,unsigned timeoutms=INFINITE)
        {
            return transaction->addDelayedDelete(lfn, timeoutms);
        }
    };

public:
    CRemoveSuperFileAction(IUserDescriptor *_user,
                           const char *_flname,
                           bool _delSub)
        : user(_user), delSub(_delSub)
    {
        logicalname.set(_flname);
    }
    virtual bool prepare()
    {
        // We *have* to make sure the file exists here
        super.setown(transaction->lookupSuperFile(logicalname.get(), SDS_SUB_LOCK_TIMEOUT));
        if (!super)
            ThrowStringException(-1, "Super File %s doesn't exist in the file system", logicalname.get());
        addFileLock(super);
        // Adds actions to transactions before this one and gets executed only on commit
        if (delSub)
        {
            // As 'delSub' means additional actions, handle them in their own transaction context
            // Wrap lookups and record of removed/added subs to parent transaction, for common cache purposes
            nestedTransaction.setown(new CNestedTransaction(transaction, user));
            super->removeSubFile(NULL, true, false, nestedTransaction);
        }
        if (lock())
        {
            if (nestedTransaction)
            {
                if (nestedTransaction->prepareActions())
                    return true;
            }
            else
                return true;
        }
        unlock();
        super.clear();
        return false;
    }
    virtual void retry()
    {
        super.clear();
        if (nestedTransaction)
            nestedTransaction->retryActions();
        CDFAction::retry();
    }
    virtual void run()
    {
        if (nestedTransaction)
            nestedTransaction->runActions();
        super->detach();
    }
    virtual void commit()
    {
        if (nestedTransaction)
            nestedTransaction->commitAndClearup();
        CDFAction::commit();
    }
};

/**
 * Renames a file within a transaction.
 */
class CRenameFileAction: public CDFAction
{
    CDfsLogicalFileName fromName, toName;
    CDistributedFileDirectory *parent;
    Linked<IDistributedFile> file;
    IUserDescriptor *user;
    bool renamed;
    enum RenameAction { ra_regular, ra_splitfrom, ra_mergeinto } ra;

public:
    CRenameFileAction(CDistributedFileDirectory *_parent,
                      IUserDescriptor *_user,
                      const char *_flname,
                      const char *_newname)
        : user(_user), parent(_parent)
    {
        fromName.set(_flname);
        // Basic consistency checking
        toName.set(_newname);
        if (fromName.isExternal() || toName.isExternal())
            throw MakeStringException(-1,"rename: cannot rename external files"); // JCSMORE perhaps you should be able to?
        if (fromName.isForeign() || toName.isForeign())
            throw MakeStringException(-1,"rename: cannot rename foreign files");
        // Make sure files are not the same
        if (0 == strcmp(fromName.get(), toName.get()))
            ThrowStringException(-1, "rename: cannot rename file %s to itself", toName.get());

        ra = ra_regular;
        renamed = false;
    }
    virtual bool prepare()
    {
        // We *have* to make sure the source file exists and can be renamed
        file.setown(transaction->lookupFile(fromName.get(), SDS_SUB_LOCK_TIMEOUT));
        if (!file)
            ThrowStringException(-1, "rename: file %s doesn't exist in the file system", fromName.get());
        if (file->querySuperFile())
            ThrowStringException(-1,"rename: cannot rename file %s as is SuperFile", fromName.get()); // Why not
        StringBuffer reason;
        if (!file->canRemove(reason))
            ThrowStringException(-1,"rename: %s",reason.str());
        addFileLock(file);
        renamed = false;
        if (lock())
        {
            StringBuffer oldcluster, newcluster;
            fromName.getCluster(oldcluster);
            toName.getCluster(newcluster);

            Owned<IDistributedFile> newFile = transaction->lookupFile(toName.get(), SDS_SUB_LOCK_TIMEOUT);
            if (newFile)
            {
                if (newcluster.length())
                {
                    if (oldcluster.length())
                        ThrowStringException(-1,"rename: cannot specify both source and destination clusters on rename");
                    if (newFile->findCluster(newcluster.str())!=NotFound)
                        ThrowStringException(-1,"rename: cluster %s already part of file %s",newcluster.str(),toName.get());
                    if (file->numClusters()!=1)
                        ThrowStringException(-1,"rename: source file %s has more than one cluster",fromName.get());
                    // check compatible here ** TBD
                    ra = ra_mergeinto;
                }
                else
                    ThrowStringException(-1, "rename: file %s already exist in the file system", toName.get());
            }
            else if (oldcluster.length())
            {
                if (newcluster.length())
                    ThrowStringException(-1,"rename: cannot specify both source and destination clusters on rename");
                if (file->numClusters()==1)
                    ThrowStringException(-1,"rename: cannot rename sole cluster %s",oldcluster.str());
                if (file->findCluster(oldcluster.str())==NotFound)
                    ThrowStringException(-1,"rename: cannot find cluster %s",oldcluster.str());
                ra = ra_splitfrom;
            }
            else
            {
                // TODO: something should check that file being renamed is not a subfile of a super where both created in transaction
                transaction->noteRename(file, toName.get());
                ra = ra_regular;
            }
            return true;
        }
        unlock();
        file.clear();
        return false;
    }
    virtual void run()
    {
        doRename(fromName, toName, ra);
        renamed = true;
    }
    virtual void retry()
    {
        file.clear();
        CDFAction::retry();
    }
    virtual void rollback()
    {
        // Only roll back if already renamed
        if (renamed)
        {
            switch (ra)
            {
                case ra_regular:
                    doRename(toName, fromName, ra_regular);
                    break;
                case ra_splitfrom:
                    doRename(toName, fromName, ra_mergeinto);
                    break;
                case ra_mergeinto:
                    doRename(toName, fromName, ra_splitfrom);
                    break;
                default:
                    throwUnexpected();
            }
            renamed = false;
        }
        CDFAction::rollback();
    }

private:
    void doRename(CDfsLogicalFileName &from, CDfsLogicalFileName &to, RenameAction ra)
    {
        CriticalBlock block(physicalChange);

        StringBuffer oldcluster, newcluster;
        fromName.getCluster(oldcluster);
        toName.getCluster(newcluster);

        Owned<IDistributedFile> oldfile;
        if (ra_splitfrom == ra)
        {
            oldfile.setown(file.getClear());
            Owned<IFileDescriptor> newdesc = oldfile->getFileDescriptor(oldcluster.str());
            file.setown(parent->createNew(newdesc));
        }

        // Physical Rename
        Owned<IMultiException> exceptions = MakeMultiException();
        if (!file->renamePhysicalPartFiles(to.get(),newcluster,exceptions))
        {
            unlock();
            StringBuffer errors;
            exceptions->errorMessage(errors);
            ThrowStringException(-1, "rename: could not rename logical file %s to %s: %s", fromName.get(), to.get(), errors.str());
        }

        // Logical rename and cleanup
        switch (ra)
        {
            case ra_splitfrom:
            {
                unlock();
                oldfile->removeCluster(oldcluster.str());
                file->attach(to.get(), user);
                lock();
                break;
            }
            case ra_mergeinto:
            {
                Owned<IDistributedFile> newFile = transaction->lookupFile(to.get(), SDS_SUB_LOCK_TIMEOUT);
                ClusterPartDiskMapSpec mspec = file->queryPartDiskMapping(0);
                // Unlock the old file
                unlock();
                CDistributedFile *_file = dynamic_cast<CDistributedFile *>(file.get());
                _file->detachLogical(INFINITE); // don't delete physicals, now used by newFile
                transaction->clearFile(file); // no long used in transaction
                newFile->addCluster(newcluster.str(),mspec);
                parent->fixDates(newFile);
                // need to clear and re-lookup as changed outside of transaction
                // TBD: Allow 'addCluster' 'fixDates' etc. to be delayed/work inside transaction
                transaction->clearFile(newFile);
                newFile.clear();
                file.setown(transaction->lookupFile(to.get(), SDS_SUB_LOCK_TIMEOUT));
                addFileLock(file);
                lock();
                break;
            }
            case ra_regular:
            {
                /* It is not enough to unlock this actions locks on the file being renamed,
                 * because other actions, before and after may hold locks to the same file.
                 * For now, IDistributeFile::rename, needs to work on a lock free instance.
                 * TBD: Allow IDistributedFile::rename to work properly within transaction.
                 */
                DistributedFilePropertyLockFree unlock(file);
                file->rename(to.get(), user);
                break;
            }
            default:
                throwUnexpected();
        }
        // MORE: If the logical rename fails, we should roll back the physical renaming
        // What if the physical renaming-back fails?!
        // For now, leaving as it was, since physical renaming is more prone to errors than logical
        // And checks were made earlier to make sure it was safe to rename
    }
};

// MORE: This should be implemented in DFSAccess later on
IDistributedSuperFile *CDistributedFileDirectory::createSuperFile(const char *_logicalname,IUserDescriptor *user, bool _interleaved,bool ifdoesnotexist,IDistributedFileTransaction *transaction)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    checkLogicalName(logicalname,user,true,true,false,"have a superfile with");

    // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
    Linked<IDistributedFileTransactionExt> localtrans;
    if (transaction)
    {
        IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
        localtrans.set(_transaction);
    }
    else
        localtrans.setown(new CDistributedFileTransaction(user));

    IDistributedSuperFile *sfile = localtrans->lookupSuperFile(logicalname.get());
    if (sfile)
    {
        if (ifdoesnotexist)
            return sfile;
        else
            throw MakeStringException(-1,"createSuperFile: SuperFile %s already exists",logicalname.get());
    }

    Owned<CCreateSuperFileAction> action = new CCreateSuperFileAction(this,user,_logicalname,_interleaved);
    localtrans->addAction(action.getLink()); // takes ownership
    localtrans->autoCommit();
    return action->getSuper();
}

// MORE: This should be implemented in DFSAccess later on
void CDistributedFileDirectory::removeSuperFile(const char *_logicalname, bool delSubs, IUserDescriptor *user, IDistributedFileTransaction *transaction)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    checkLogicalName(logicalname,user,true,true,false,"have a superfile with");

    // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
    Linked<IDistributedFileTransactionExt> localtrans;
    if (transaction)
    {
        IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
        localtrans.set(_transaction);
    }
    else
        localtrans.setown(new CDistributedFileTransaction(user));

    CRemoveSuperFileAction *action = new CRemoveSuperFileAction(user, _logicalname, delSubs);
    localtrans->addAction(action); // takes ownership
    localtrans->autoCommit();
}

bool CDistributedFileDirectory::removeEntry(const char *name, IUserDescriptor *user, IDistributedFileTransaction *transaction, unsigned timeoutms, bool throwException)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(name);
    if (!logicalname.isExternal())
        checkLogicalName(logicalname,user,true,true,false,"delete");

    // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
    Linked<IDistributedFileTransactionExt> localtrans;
    if (transaction)
    {
        IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
        localtrans.set(_transaction);
    }
    else
        localtrans.setown(new CDistributedFileTransaction(user));

    // Action will be executed at the end of the transaction (commit)
    localtrans->addDelayedDelete(logicalname, timeoutms);

    try
    {
        localtrans->autoCommit();
    }
    catch (IException *e)
    {
        // TODO: Transform removeEntry into void
        StringBuffer msg(logicalname.get());
        msg.append(" - cause: ");
        e->errorMessage(msg);
        ERRLOG("%s", msg.str());
        if (throwException)
            throw new CDFS_Exception(DFSERR_FailedToDeleteFile, msg.str());
        e->Release();
        return false;
    }
    return true;
}

void CDistributedFileDirectory::removeEmptyScope(const char *scope)
{
    if (scope&&*scope) {
        StringBuffer fn(scope);
        fn.append("::x");
        CDfsLogicalFileName dlfn;
        dlfn.set(fn.str());
        removeFileEmptyScope(dlfn,defaultTimeout);
    }
}

void CDistributedFileDirectory::renamePhysical(const char *oldname,const char *newname,IUserDescriptor *user,IDistributedFileTransaction *transaction)
{
    if (!user)
    {
#ifdef NULL_DALIUSER_STACKTRACE
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp CDistributedFileDirectory::renamePhysical %d",__LINE__);
        //following debug code to be removed
        PrintStackReport();
#endif
        user = defaultudesc.get();
    }
    CDfsLogicalFileName oldlogicalname;
    oldlogicalname.set(oldname);
    checkLogicalName(oldlogicalname,user,true,true,false,"rename");

    // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
    Linked<IDistributedFileTransactionExt> localtrans;
    if (transaction)
    {
        IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
        localtrans.set(_transaction);
    }
    else
        localtrans.setown(new CDistributedFileTransaction(user));

    CRenameFileAction *action = new CRenameFileAction(this, user, oldname, newname);
    localtrans->addAction(action); // takes ownership
    localtrans->autoCommit();
}

void CDistributedFileDirectory::fixDates(IDistributedFile *file)
{
    // should do in parallel
    unsigned width = file->numParts();
    CriticalSection crit;
    class casyncfor: public CAsyncFor
    {
        IDistributedFile *file;
        CriticalSection &crit;
        unsigned width;
    public:
        bool ok;
        casyncfor(IDistributedFile *_file,unsigned _width,CriticalSection &_errcrit)
            : crit(_errcrit)
        {
            file = _file;
            ok = true;
            width = _width;
            ok = true;
        }
        void Do(unsigned i)
        {
            CriticalBlock block(crit);
            Owned<IDistributedFilePart> part = file->getPart(i);
            CDateTime dt;
            if (!part->getModifiedTime(false,false,dt))
                return;
            unsigned nc = part->numCopies();
            for (unsigned copy = 0; copy < nc; copy++) {
                RemoteFilename rfn;
                part->getFilename(rfn,copy);
                Owned<IFile> partfile = createIFile(rfn);
                try {
                    CriticalUnblock unblock(crit);
                    CDateTime dt2;
                    if (partfile->getTime(NULL,&dt2,NULL)) {
                        if (!dt.equals(dt2)) {
                            partfile->setTime(NULL,&dt,NULL);
                        }
                    }
                }
                catch (IException *e) {
                    CriticalBlock block(crit);
                    StringBuffer s("Failed to find file part ");
                    s.append(partfile->queryFilename()).append(" on ");
                    rfn.queryEndpoint().getUrlStr(s);
                    EXCLOG(e, s.str());
                    e->Release();
                }
            }
        }
    } afor(file,width,crit);
    afor.For(width,10,false,true);
}

void CDistributedFileDirectory::addEntry(CDfsLogicalFileName &dlfn,IPropertyTree *root,bool superfile, bool ignoreexists)
{
    // add bit awkward
    bool external;
    bool foreign;
    external = dlfn.isExternal();
    foreign = dlfn.isForeign();
    if (external) {
        root->Release();
        return; // ignore attempts to add external
    }
    CScopeConnectLock sconnlock("CDistributedFileDirectory::addEntry", dlfn, true, false, false, defaultTimeout);
    if (!sconnlock.conn()) {// warn?
        root->Release();
        return;
    }
    IPropertyTree* sroot =  sconnlock.conn()->queryRoot();
    StringBuffer tail;
    dlfn.getTail(tail);
    IPropertyTree *prev = getNamedPropTree(sroot,superfile?queryDfsXmlBranchName(DXB_SuperFile):queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
    if (!prev) // check super/file doesn't exist
        prev = getNamedPropTree(sroot,superfile?queryDfsXmlBranchName(DXB_File):queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str(),false);
    if (prev!=NULL) {
        prev->Release();
        root->Release();
        if (ignoreexists)
            return;
        IDFS_Exception *e = new CDFS_Exception(DFSERR_LogicalNameAlreadyExists,dlfn.get());
        throw e;
    }
    root->setProp("@name",tail.str());
    root->setProp("OrigName",dlfn.get());
    if (superfile)
        sroot->addPropTree(queryDfsXmlBranchName(DXB_SuperFile), root); // now owns root
    else
    {
        IPropertyTree *file = sroot->addPropTree(queryDfsXmlBranchName(DXB_File), root); // now owns root
        file->setPropTree("ClusterLock", createPTree());
    }
}

IDistributedFileIterator *CDistributedFileDirectory::getIterator(const char *wildname, bool includesuper, IUserDescriptor *user)
{
    return new CDistributedFileIterator(this,wildname,includesuper,user);
}

GetFileClusterNamesType CDistributedFileDirectory::getFileClusterNames(const char *_logicalname,StringArray &out)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    if (logicalname.isForeign())
        return GFCN_Foreign;
    if (logicalname.isExternal())
        return GFCN_External;
    CScopeConnectLock sconnlock("CDistributedFileDirectory::getFileClusterList", logicalname, false, false, false, defaultTimeout);
    DfsXmlBranchKind bkind;
    IPropertyTree *froot = sconnlock.queryFileRoot(logicalname,bkind);
    if (froot) {
        if (bkind==DXB_File) {
            getFileGroups(froot,out);
            return GFCN_Normal;
        }
        if (bkind==DXB_SuperFile)
            return GFCN_Super;
    }
    return GFCN_NotFound;
}

// --------------------------------------------------------


static CDistributedFileDirectory *DFdir = NULL;
static CriticalSection dfdirCrit;

/**
 * Public method to control DistributedFileDirectory access
 * as a singleton. This is the only way to get directories,
 * files, super-files and logic-files.
 */
IDistributedFileDirectory &queryDistributedFileDirectory()
{
    if (!DFdir) {
        CriticalBlock block(dfdirCrit);
        if (!DFdir)
            DFdir = new CDistributedFileDirectory();
    }
    return *DFdir;
}

/**
 * Shutdown distributed file system (root directory).
 */
void closedownDFS()  // called by dacoven
{
    CriticalBlock block(dfdirCrit);
    try {
        delete DFdir;
    }
    catch (IMP_Exception *e) {
        if (e->errorCode()!=MPERR_link_closed)
            throw;
        PrintExceptionLog(e,"closedownDFS");
        e->Release();
    }
    catch (IDaliClient_Exception *e) {
        if (e->errorCode()!=DCERR_server_closed)
            throw;
        e->Release();
    }
    DFdir = NULL;
    CriticalBlock block2(groupsect);
    ::Release(groupStore);
    groupStore = NULL;
}

class CDFPartFilter : public CInterface, implements IDFPartFilter
{
protected:
    bool *partincluded;
    unsigned max;

public:
    IMPLEMENT_IINTERFACE;

    CDFPartFilter(const char *filter)
    {
        max = 0;
        partincluded = NULL;
        unsigned pn=0;
        const char *s=filter;
        if (!s)
            return;
        while (*s) {
            if (isdigit(*s)) {
                pn = pn*10+(*s-'0');
                if (pn>max)
                    max = pn;
            }
            else
                pn = 0;
            s++;
        }
        if (max==0)
            return;
        partincluded = new bool[max];
        unsigned i;
        for (i=0;i<max;i++)
            partincluded[i] = false;
        pn=0;
        s=filter;
        unsigned start=0;
        loop {
            if ((*s==0)||(*s==',')||isspace(*s)) {
                if (start) {
                    for (i=start-1;i<pn;i++)
                        partincluded[i] = true;
                    start = 0;
                }
                else
                    partincluded[pn-1] = true;
                if (*s==0)
                    break;
                pn = 0;
            }
            else if (isdigit(*s)) {
                pn = pn*10+(*s-'0');
                if (pn>max)
                    max = pn;
                if (s[1]=='-') {
                    s++;
                    start = pn;
                    pn = 0;
                }
            }
            s++;
        }
    }

    ~CDFPartFilter()
    {
        delete [] partincluded;
    }

    bool includePart(unsigned part)
    {
        if (max==0)
            return true;
        if (part>=max)
            return false;
        return partincluded[part];
    };
};

IDFPartFilter *createPartFilter(const char *filter)
{
    return new CDFPartFilter(filter);

}


//=====================================================================================
// Server Side Support

class CFileMatch : public CInterface
{
    StringAttr name;
    Linked<IPropertyTree> tree;
    bool isSuper;
public:
    CFileMatch(const char *_name, IPropertyTree *_tree, bool _isSuper) : name(_name), tree(_tree), isSuper(_isSuper)
    {
    }
    IPropertyTree &queryFileTree() const { return *tree; }
    const char *queryName() const { return name; }
    bool queryIsSuper() const { return isSuper; }
};
typedef CIArrayOf<CFileMatch> CFileMatchArray;

class CScope : public CInterface
{
    StringAttr name;
    CIArrayOf<CFileMatch> files; // matches
    CIArrayOf<CScope> subScopes;
public:
    CScope(const char *_name) : name(_name)
    {
    }
    const char *getName() const { return name; }
    void addMatch(const char *name, IPropertyTree &fileTree, bool isSuper)
    {
        files.append(*new CFileMatch(name, &fileTree, isSuper));
    }
    CScope *addScope(const char *scope)
    {
        CScope *subScope = new CScope(scope);
        subScopes.append(*subScope);
        return subScope;
    }
    void popLastScope()
    {
        subScopes.pop();
    }
    CIArrayOf<CScope> &querySubScopes() { return subScopes; }
    CFileMatchArray &queryFiles() { return files; }
};
typedef CIArrayOf<CScope> CScopeArray;


const char* DFUQFilterFieldNames[] = { "", "@description", "@directory", "@group", "@modified", "@name", "@numclusters", "@numparts",
    "@partmask", "@OrigName", "Attr", "Attr/@job", "Attr/@owner", "Attr/@recordCount", "Attr/@recordSize", "Attr/@size",
    "Attr/@compressedsize", "Attr/@workunit", "Cluster", "Cluster/@defaultBaseDir", "Cluster/@defaultReplDir", "Cluster/@mapFlags",
    "Cluster/@name", "Part", "Part/@name", "Part/@num", "Part/@size", "SuperOwner", "SuperOwner/@name",
    "SubFile", "SubFile/@name", "SubFile/@num", "Attr/@kind" };

extern da_decl const char* getDFUQFilterFieldName(DFUQFilterField feild)
{
    return DFUQFilterFieldNames[feild];
}

class CDFUSFFilter : public CInterface
{
    DFUQFilterType filterType;
    StringAttr attrPath;
    bool hasFilter;
    bool hasFilterHigh;
    StringAttr filterValue;
    StringAttr filterValueHigh;
    int filterValueInt;
    int filterValueHighInt;
    __int64 filterValueInt64;
    __int64 filterValueHighInt64;
    bool filterValueBoolean;
    StringAttr sep;
    StringArray filterArray;

public:
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, const char *_filterValue, const char *_filterValueHigh)
        : filterType(_filterType), attrPath(_attrPath), filterValue(_filterValue), filterValueHigh(_filterValueHigh) {};
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, bool _hasFilter, const int _filterValue, bool _hasFilterHigh, const int _filterValueHigh)
        : filterType(_filterType), attrPath(_attrPath), hasFilter(_hasFilter), filterValueInt(_filterValue), hasFilterHigh(_hasFilterHigh), filterValueHighInt(_filterValueHigh) {};
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, bool _hasFilter, const __int64 _filterValue, bool _hasFilterHigh, const __int64 _filterValueHigh)
        : filterType(_filterType), attrPath(_attrPath), hasFilter(_hasFilter), filterValueInt64(_filterValue), hasFilterHigh(_hasFilterHigh), filterValueHighInt64(_filterValueHigh) {};
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, bool _filterValue)
        : filterType(_filterType), attrPath(_attrPath), filterValueBoolean(_filterValue) {};
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, const char *_filterValue, const char *_sep, StringArray& _filterArray)
        : filterType(_filterType), attrPath(_attrPath), filterValue(_filterValue), sep(_sep)
    {
        ForEachItemIn(i,_filterArray)
        {
            const char* filter = _filterArray.item(i);
            if (filter && *filter)
                filterArray.append(filter);
        }
    };

    DFUQFilterType getFilterType() { return filterType;}
    const char * getAttrPath() { return attrPath.get();}
    const char * getFilterValue() { return filterValue.get();}
    const char * getFilterValueHigh() { return filterValueHigh.get();}
    const int getFilterValueInt() { return filterValueInt;}
    const int getFilterValueHighInt() { return filterValueHighInt;}
    const __int64 getFilterValueInt64() { return filterValueInt64;}
    const __int64 getFilterValueHighInt64() { return filterValueHighInt64;}
    const bool getFilterValueBoolean() { return filterValueBoolean;}
    const char * getSep() { return sep.get();}
    void getFilterArray(StringArray &filters)
    {
        ForEachItemIn(c, filterArray)
            filters.append(filterArray.item(c));
    }

    bool checkFilter(IPropertyTree &file)
    {
        bool match = true;
        switch(filterType)
        {
        case DFUQFTwildcardMatch:
            match = doWildMatch(file);
            break;
        case DFUQFTbooleanMatch:
            match = doBooleanMatch(file);
            break;
        case DFUQFThasProp:
            match = checkHasPropFilter(file);
            break;
        case DFUQFTcontainString:
            match = checkContainStringFilter(file);
            break;
        case DFUQFTstringRange:
            match = checkStringRangeFilter(file);
            break;
        case DFUQFTintegerRange:
            match = checkIntegerRangeFilter(file);
            break;
        case DFUQFTinteger64Range:
            match = checkInteger64RangeFilter(file);
            break;
        }
        return match;
    }
    bool doWildMatch(IPropertyTree &file)
    {
        const char* filter = filterValue.get();
        if (!attrPath.get() || !filter || !*filter || streq(filter, "*"))
            return true;

        const char* prop = file.queryProp(attrPath.get());
        if (prop && WildMatch(prop, filter, true))
            return true;
        return false;
    }
    bool doBooleanMatch(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;

        return filterValueBoolean == file.getPropBool(attrPath.get(), true);
    }
    bool checkHasPropFilter(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;

        return filterValueBoolean == file.hasProp(attrPath.get());
    }
    bool checkContainStringFilter(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;
        const char* prop = file.queryProp(attrPath.get());
        if (!prop || !*prop)
            return false;

        bool found = false;
        if (!sep.get())
        {
            if (filterArray.find(prop) != NotFound) //Match with one of values in the filter
               found = true;
            return found;
        }
        StringArray propArray;
        propArray.appendListUniq(prop, sep.get());
        ForEachItemIn(i,propArray)
        {
            const char* value = propArray.item(i);
            if (!value || !*value)
                continue;
            if (filterArray.find(value) != NotFound) //Match with one of values in the filter
            {
                found = true;
                break;
            }
        }
        return found;
    }
    bool checkStringRangeFilter(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;
        const char* prop = file.queryProp(attrPath.get());
        if (!prop || !*prop)
            return false;
        if (filterValue && (strcmp(filterValue, prop) > 0))
            return false;
        if (filterValueHigh && (strcmp(filterValueHigh, prop) < 0))
            return false;
        return true;
    }
    bool checkIntegerRangeFilter(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;
        int prop = file.getPropInt(attrPath.get());
        if (hasFilter && (prop < filterValueInt))
            return false;
        if (hasFilterHigh && (prop > filterValueHighInt))
            return false;
        return true;
    }
    bool checkInteger64RangeFilter(IPropertyTree &file)
    {
        if (!attrPath.get())
            return true;
        __int64 prop = file.getPropInt64(attrPath.get());
        if (hasFilter && (prop < filterValueInt64))
            return false;
        if (hasFilterHigh && (prop > filterValueHighInt64))
            return false;
        return true;
    }
};
typedef CIArrayOf<CDFUSFFilter> CDFUSFFilterArray;

class CIterateFileFilterContainer : public CInterface
{
    StringAttr filterBuf; //Hold original filter string just in case
    StringAttr wildNameFilter;
    unsigned maxFilesFilter;
    DFUQFileTypeFilter fileTypeFilter;
    CIArrayOf<CDFUSFFilter> filters;
    //The 'filters' contains the file scan filters other than wildNameFilter and fileTypeFilter. Those filters are used for
    //filtering the files using File Attributes tree and CDFUSFFilter::checkFilter(). The wildNameFilter and fileTypeFilter need
    //special code to filter the files.

    SerializeFileAttrOptions options;

    bool isValidInteger(const char *s)
    {
        if (!s)
            return false;
        while (*s)
        {
            if ((*s != '-') && !isdigit(*s))
                return false;
            s++;
        }
        return true;
    }
    void addOption(const char* optionStr)
    {
        if (!optionStr || !*optionStr || !isdigit(*optionStr))
            return;

        DFUQSerializeFileAttrOption option = (DFUQSerializeFileAttrOption) atoi(optionStr);
        switch(option)
        {
        case DFUQSFAOincludeSuperOwner:
            options.includeSuperOwner =  true;
            break;
        //Add more when needed
        }
    }
    void addFilter(DFUQFilterType filterType, const char* attr, const char* value, const char* valueHigh)
    {
        if (!attr || !*attr)
            return;
        if ((DFUQFTwildcardMatch == filterType) || (DFUQFTstringRange == filterType))
        {
            filters.append(*new CDFUSFFilter(filterType, attr, value, valueHigh));
            return;
        }
        if ((DFUQFTbooleanMatch == filterType) || (DFUQFThasProp == filterType))
        {
            bool filter = true;
            if (value && (streq(value, "0") || strieq(value, "false")))
                filter = false;
            filters.append(*new CDFUSFFilter(filterType, attr, filter));
            return;
        }
        if ((DFUQFTintegerRange == filterType) || (DFUQFTinteger64Range == filterType))
        {
            bool hasFilter = false;
            bool hasFilterHigh = false;
            if (value && isValidInteger(value))
                hasFilter = true;
            if (valueHigh && isValidInteger(valueHigh))
                hasFilterHigh = true;
            if (!hasFilter && !hasFilterHigh)
                return;
            if (DFUQFTintegerRange == filterType)
                filters.append(*new CDFUSFFilter(filterType, attr, hasFilter, atoi(value), hasFilterHigh, atoi(valueHigh)));
            else
                filters.append(*new CDFUSFFilter(filterType, attr, hasFilter, (__int64) atol(value), hasFilterHigh, (__int64) atol(valueHigh)));
            return;
        }
    }
    void addFilterArray(DFUQFilterType filterType, const char* attr, const char* value, const char* sep)
    {
        if (!attr || !*attr || !value || !*value)
            return;

        StringArray filterArray;
        filterArray.appendListUniq(value, sep);
        filters.append(*new CDFUSFFilter(filterType, attr, value, sep, filterArray));
    }
    void addSpecialFilter(const char* attr, const char* value)
    {
        if (!attr || !*attr || !value || !*value)
            return;
        if (!isdigit(*attr))
        {
            PROGLOG("Unsupported Special Filter: %s", attr);
            return;
        }
        DFUQSpecialFilter filterName = (DFUQSpecialFilter) atoi(attr);
        switch(filterName)
        {
        case DFUQSFFileNameWithPrefix:
            wildNameFilter.set(value);
            break;
        case DFUQSFFileType:
            if (isdigit(*value))
                fileTypeFilter = (DFUQFileTypeFilter) atoi(value);
            else
                PROGLOG("Unsupported Special Filter: %s, value %s", attr, value);
            break;
        case DFUQSFMaxFiles:
            if (isdigit(*value))
                maxFilesFilter = atoi(value);
            else
                PROGLOG("Unsupported Special Filter: %s, value %s", attr, value);
            break;
        default:
            PROGLOG("Unsupported Special Filter: %d", filterName);
            break;
        }
    }

    bool doWildMatch(const char* filter, const char* value)
    {
        if (!filter || !*filter || streq(filter, "*") || (value && WildMatch(value, filter, true)))
            return true;

        return false;
    }

public:
    CIterateFileFilterContainer()
    {
        maxFilesFilter = ITERATE_FILTEREDFILES_LIMIT;
        fileTypeFilter = DFUQFFTall;
        wildNameFilter.set("*");
        filterBuf.clear();
    };
    void readFilters(const char *filterStr)
    {
        if (!filterStr || !*filterStr)
            return;

        filterBuf.set(filterStr);
        StringArray filterStringArray;
        char sep[] = { DFUQFilterSeparator, '\0' };
        filterStringArray.appendList(filterStr, sep);

        unsigned filterFieldsToRead = filterStringArray.length();
        ForEachItemIn(i,filterStringArray)
        {
            const char* filterTypeStr = filterStringArray.item(i);
            if (!filterTypeStr || !*filterTypeStr)
                continue;
            if (!isdigit(*filterTypeStr))
                continue;
            unsigned filterSize = 4;
            DFUQFilterType filterType = (DFUQFilterType) atoi(filterTypeStr);
            switch(filterType)
            {
            case DFUQFTcontainString:
                if (filterFieldsToRead >= filterSize) //DFUQFilterType | filter name | separator | filter value separated by the separator
                    addFilterArray(DFUQFTcontainString, filterStringArray.item(i+1), (const char*)filterStringArray.item(i+2), (const char*)filterStringArray.item(i+3));
                break;
            case DFUQFThasProp:
            case DFUQFTbooleanMatch:
            case DFUQFTwildcardMatch:
                filterSize = 3;
                if (filterFieldsToRead >= filterSize) //DFUQFilterType | filter name | filter value
                    addFilter(filterType, filterStringArray.item(i+1), (const char*)filterStringArray.item(i+2), NULL);
                break;
            case DFUQFTstringRange:
            case DFUQFTintegerRange:
            case DFUQFTinteger64Range:
                if (filterFieldsToRead >= filterSize) //DFUQFilterType | filter name | from filter | to filter
                    addFilter(filterType, filterStringArray.item(i+1), (const char*)filterStringArray.item(i+2), (const char*)filterStringArray.item(i+3));
                break;
            case DFUQFTincludeFileAttr:
                filterSize = 2;
                if (filterFieldsToRead >= filterSize) //DFUQFilterType | filter
                    addOption(filterStringArray.item(i+1));
                break;
            case DFUQFTspecial:
                filterSize = 3;
                if (filterFieldsToRead >= filterSize) //DFUQFilterType | filter name | filter value
                    addSpecialFilter(filterStringArray.item(i+1), (const char*)filterStringArray.item(i+2));
                break;
            }
            filterFieldsToRead -= filterSize;
            i += (filterSize - 1);
        }
    }
    bool matchFileScanFilter(const char* name, IPropertyTree &file)
    {
        if (!doWildMatch(wildNameFilter.get(), name))
            return false;

        if (!filters.length())
            return true;
        ForEachItemIn(i,filters)
        {
            CDFUSFFilter &filter = filters.item(i);
            bool match = filter.checkFilter(file);
            if (!match)
                return match;
        }
        return true;
    }

    DFUQFileTypeFilter getFileTypeFilter() { return fileTypeFilter; }
    unsigned getMaxFilesFilter() { return maxFilesFilter; }
    void setFileTypeFilter(DFUQFileTypeFilter _fileType)
    {
        fileTypeFilter = _fileType;
    }
    const char* getNameFilter() { return wildNameFilter.get(); }
    void setNameFilter(const char* _wildName)
    {
        if (!_wildName || !*_wildName)
            return;
        wildNameFilter.set(_wildName);
    }
    SerializeFileAttrOptions& getSerializeFileAttrOptions() { return options; }
};

class CFileScanner
{
    bool recursive;
    bool includesuper;
    StringAttr wildname;
    Owned<CScope> topLevelScope;
    CScope *currentScope;
    Owned<CIterateFileFilterContainer> iterateFileFilterContainer;

    bool scopeMatch(const char *name)
    {   // name has trailing '::'
        if (!name || !*name)
            return true;
        const char *s1 = NULL;
        if (!iterateFileFilterContainer)
            s1 = wildname.get();
        else
            s1 = iterateFileFilterContainer->getNameFilter();
        if (!s1 || !*s1)
            return true;
        const char *s2 = name;
        while (*s2) {
            if (*s1=='*') {
                if (recursive)
                    return true;
                if (*s2==':')
                    return false;
                // '*' can only come at end of scope in non-recursive
                while (*s1&&(*s1!=':'))
                    s1++;
                while (*s2&&(*s2!=':'))
                    s2++;
            }
            else if ((*s1==*s2)||(*s1=='?')) {
                s1++;
                s2++;
            }
            else
                return false;
        }
        return true;
    }

    bool processScopes(IPropertyTree &root,StringBuffer &name)
    {
        bool ret = false;
        CScope *parentScope = currentScope;
        if (parentScope)
            currentScope = parentScope->addScope(name);
        else
        { // once only
            topLevelScope.setown(new CScope(""));
            currentScope = topLevelScope;
        }
        size32_t ns = name.length();
        if (ns)
            name.append("::");
        size32_t ns2 = name.length();

        if (scopeMatch(name.str())) {
            Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_Scope));
            if (iter->first()) {
                do {
                    IPropertyTree &scope = iter->query();
                    if (scope.hasChildren()) {
                        name.append(scope.queryProp("@name"));
                        ret |= processScopes(scope, name);
                        name.setLength(ns2);
                    }
                } while (iter->next());
            }
            if (!iterateFileFilterContainer)
                ret |= processFiles(root,name);
            else
                ret |= processFilesWithFilters(root,name);
        }
        if (!ret && parentScope)
            parentScope->popLastScope(); // discard scopes where no matches
        currentScope = parentScope;
        name.setLength(ns);
        return ret;
    }

    bool processFiles(IPropertyTree &root,StringBuffer &name)
    {
        bool ret = false;
        const char *s1 = wildname.get();
        size32_t ns = name.length();
        Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_File));
        if (iter->first()) {
            IPropertyTree &scope = iter->query();
            do {
                IPropertyTree &file = iter->query();
                name.append(file.queryProp("@name"));
                if (!s1||WildMatch(name.str(),s1,true)) {
                    currentScope->addMatch(name,file,false);
                    ret = true;
                }
                name.setLength(ns);
            } while (iter->next());
        }
        if (includesuper) {
            iter.setown(root.getElements(queryDfsXmlBranchName(DXB_SuperFile)));
            if (iter->first()) {
                do {
                    IPropertyTree &file = iter->query();
                    name.append(file.queryProp("@name"));
                    if (!s1||WildMatch(name.str(),s1,true)) {
                        currentScope->addMatch(name,file,true);
                        ret = true;
                    }
                    name.setLength(ns);
                } while (iter->next());
            }
        }
        return ret;
    }

    bool processFilesWithFilters(IPropertyTree &root, StringBuffer &name)
    {
        bool ret = false;
        size32_t ns = name.length();
        DFUQFileTypeFilter fileTypeFilter = iterateFileFilterContainer->getFileTypeFilter();
        if (fileTypeFilter != DFUQFFTsuperfileonly)
            addMatchedFiles(root.getElements(queryDfsXmlBranchName(DXB_File)), false, name, ns, ret);
        if ((fileTypeFilter == DFUQFFTall) || (fileTypeFilter == DFUQFFTsuperfileonly))
            addMatchedFiles(root.getElements(queryDfsXmlBranchName(DXB_SuperFile)), true, name, ns, ret);
        return ret;
    }

    void addMatchedFiles(IPropertyTreeIterator* files, bool isSuper, StringBuffer &name, size32_t ns, bool& ret)
    {
        Owned<IPropertyTreeIterator> iter = files;
        ForEach(*iter)
        {
            IPropertyTree &file = iter->query();
            name.append(file.queryProp("@name"));
            if (iterateFileFilterContainer->matchFileScanFilter(name.str(), file))
            {
                currentScope->addMatch(name,file,isSuper);
                ret = true;
            }
            name.setLength(ns);
        }
    }
public:
    void scan(IPropertyTree *sroot, const char *_wildname,bool _recursive,bool _includesuper)
    {
        if (_wildname)
            wildname.set(_wildname);
        else
            wildname.clear();
        recursive = _recursive;
        includesuper = _includesuper;
        StringBuffer name;
        topLevelScope.clear();
        currentScope = NULL;
        processScopes(*sroot->queryPropTree(querySdsFilesRoot()),name);
    }
    void scan(IPropertyTree *sroot, CIterateFileFilterContainer* _iterateFileFilterContainer, bool _recursive)
    {
        iterateFileFilterContainer.setown(_iterateFileFilterContainer);
        recursive = _recursive;

        StringBuffer name;
        topLevelScope.clear();
        currentScope = NULL;
        processScopes(*sroot->queryPropTree(querySdsFilesRoot()),name);
    }
    void _getResults(bool auth, IUserDescriptor *user, CScope &scope, CFileMatchArray &matchingFiles, StringArray &authScopes,
        unsigned &count, bool checkFileCount)
    {
        if (auth)
        {
            int perm = getScopePermissions(scope.getName(),user,0);     // don't audit
            if (!HASREADPERMISSION(perm))
                return;
            authScopes.append(scope.getName());
        }
        CFileMatchArray &files = scope.queryFiles();
        ForEachItemIn(f, files)
        {
            if (checkFileCount && (count == iterateFileFilterContainer->getMaxFilesFilter()))
                throw MakeStringException(DFSERR_PassIterateFilesLimit, "CFileScanner::_getResults() found >%d files.",
                    iterateFileFilterContainer->getMaxFilesFilter());

            CFileMatch *match = &files.item(f);
            matchingFiles.append(*LINK(match));
            ++count;
        }
        CScopeArray &subScopes = scope.querySubScopes();
        ForEachItemIn(s, subScopes)
        {
            CScope &subScope = subScopes.item(s);
            _getResults(auth, user, subScope, matchingFiles, authScopes, count, checkFileCount);
        }
    }
    unsigned getResults(bool auth, IUserDescriptor *user, CFileMatchArray &matchingFiles, StringArray &authScopes, unsigned &count, bool checkFileCount)
    {
        _getResults(auth, user, *topLevelScope, matchingFiles, authScopes, count, checkFileCount);
        return count;
    }
};

struct CMachineEntry: public CInterface
{
    CMachineEntry(const char *_mname,SocketEndpoint _ep)
        : mname(_mname),ep(_ep)
    {
    }
    StringAttr mname;
    SocketEndpoint ep;
};

typedef CMachineEntry *CMachineEntryPtr;
typedef MapStringTo<CMachineEntryPtr> CMachineEntryMap;

StringBuffer &getClusterGroupName(IPropertyTree &cluster, StringBuffer &groupName)
{
    const char *name = cluster.queryProp("@name");
    const char *nodeGroupName = cluster.queryProp("@nodeGroup");
    if (nodeGroupName && *nodeGroupName)
        name = nodeGroupName;
    groupName.append(name);
    return groupName.trim().toLowerCase();
}

StringBuffer &getClusterSpareGroupName(IPropertyTree &cluster, StringBuffer &groupName)
{
    return getClusterGroupName(cluster, groupName).append("_spares");
}

// JCSMORE - dfs group handling may be clearer if in own module
class CInitGroups
{
    CMachineEntryMap machinemap;
    CIArrayOf<CMachineEntry> machinelist;
    CConnectLock groupsconnlock;
    StringArray clusternames;
    unsigned defaultTimeout;

    bool clusterGroupCompare(IPropertyTree *newClusterGroup, IPropertyTree *oldClusterGroup)
    {
        if (!newClusterGroup && oldClusterGroup)
            return false;
        else if (!oldClusterGroup && newClusterGroup)
            return false;
        if (!newClusterGroup) // both null
            return true;
        // see if identical
        const char *oldKind = oldClusterGroup?oldClusterGroup->queryProp("@kind"):NULL;
        const char *oldDir = oldClusterGroup?oldClusterGroup->queryProp("@dir"):NULL;
        const char *newKind = newClusterGroup?newClusterGroup->queryProp("@kind"):NULL;
        const char *newDir = newClusterGroup?newClusterGroup->queryProp("@dir"):NULL;
        if (oldKind) {
            if (newKind) {
                if (!streq(newKind, newKind))
                    return false;
            }
            else
                return false;
        }
        else if (newKind)
            return false;
        if (oldDir) {
            if (newDir) {
                if (!streq(newDir,oldDir))
                    return false;
            }
            else
                return false;
        }
        else if (NULL!=newDir)
            return false;

        unsigned oldGroupCount = oldClusterGroup->getCount("Node");
        unsigned newGroupCount = newClusterGroup->getCount("Node");
        if (oldGroupCount != newGroupCount)
            return false;
        if (0 == newGroupCount)
            return true;
        Owned<IPropertyTreeIterator> newIter = newClusterGroup->getElements("Node");
        Owned<IPropertyTreeIterator> oldIter = oldClusterGroup->getElements("Node");
        if (newIter->first() && oldIter->first()) {
            loop {
                const char *oldIp = oldIter->query().queryProp("@ip");
                const char *newIp = newIter->query().queryProp("@ip");
                if (!streq(oldIp, newIp))
                    return false;
                if (!oldIter->next() || !newIter->next())
                    break;
            }
        }
        return true;
    }

    void addClusterGroup(const char *name, IPropertyTree *newClusterGroup, bool realCluster)
    {
        VStringBuffer prop("Group[@name=\"%s\"]", name);
        IPropertyTree *root = groupsconnlock.conn->queryRoot();
        IPropertyTree *old = root->queryPropTree(prop.str());
        if (old) {
            // JCSMORE
            // clone
            // iterate through files and point to clone
            //    i) if change is minor, worth swapping to new group anyway?
            //   ii) if old group has machines that are no longer in new environment, mark file bad?
            root->removeTree(old);
        }
        if (!newClusterGroup)
            return;
        if (realCluster)
            clusternames.append(name);
        IPropertyTree *grp = root->addPropTree("Group", newClusterGroup);
        grp->setProp("@name", name);
    }

    IGroup *getGroupFromCluster(GroupType groupType, IPropertyTree &cluster)
    {
        SocketEndpointArray eps;
        const char *processName=NULL;
        switch (groupType) {
            case grp_thor:
                processName = "ThorSlaveProcess";
                break;
            case grp_thorspares:
                processName = "ThorSpareProcess";
                break;
            case grp_roxie:
                processName = "RoxieServerProcess";
                break;
            default:
                throwUnexpected();
        }
        SocketEndpoint nullep;
        Owned<IPropertyTreeIterator> nodes = cluster.getElements(processName);
        ForEach(*nodes) {
            IPropertyTree &node = nodes->query();
            SocketEndpoint ep;
            const char *computer = node.queryProp("@computer");
            const char *netAddress = node.queryProp("@netAddress");
            if (computer && *computer)
            {
                CMachineEntryPtr *m = machinemap.getValue(computer);
                if (!m) {
                    ERRLOG("Cannot construct %s, computer name %s not found\n", cluster.queryProp("@name"), computer);
                    return NULL;
                }
                ep.set((*m)->ep);
            }
            else if (netAddress && *netAddress)
            {
                ep.set(netAddress, 0);
            }
            else
            {
                ERRLOG("Cannot construct %s, missing computer spec on node\n", cluster.queryProp("@name"));
                return NULL;
            }
            switch (groupType) {
                case grp_roxie:
                    // Redundant copies are located via the flags.
                    // Old environments may contain duplicated sever information for multiple ports
                    eps.appendUniq(ep);
                    break;
                case grp_thor:
                case grp_thorspares:
                    eps.append(ep);
                    break;
                default:
                    throwUnexpected();
            }
        }
        if (!eps.ordinality())
            return NULL;
        Owned<IGroup> grp;
        unsigned slavesPerNode = 0;
        if (grp_thor == groupType)
            slavesPerNode = cluster.getPropInt("@slavesPerNode");
        if (slavesPerNode)
        {
            SocketEndpointArray msEps;
            for (unsigned s=0; s<slavesPerNode; s++) {
                ForEachItemIn(e, eps)
                    msEps.append(eps.item(e));
            }
            grp.setown(createIGroup(msEps));
        }
        else
            grp.setown(createIGroup(eps));
        return grp.getClear();
    }

    bool loadMachineMap()
    {
        //GH->JCS This can't be changed to use getEnvironmentFactory() unless that moved inside dalibase;
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Hardware", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn) {
            WARNLOG("Cannot connect to /Environment/Hardware");
            return false;
        }
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> machines= root->getElements("Computer");
        ForEach(*machines) {
            IPropertyTree &machine = machines->query();
            SocketEndpoint ep(machine.queryProp("@netAddress"));
            const char *name = machine.queryProp("@name");
            CMachineEntry *entry = new CMachineEntry(name,ep);
            machinemap.setValue(name, entry);
            machinelist.append(*entry);
        }
        return true;
    }

    IPropertyTree *createClusterGroup(GroupType groupType, IGroup *group, const char *dir, bool realCluster)
    {
        Owned<IPropertyTree> cluster = createPTree("Group");
        if (realCluster)
            cluster->setPropBool("@cluster", true);
        const char *kind=NULL;
        switch (groupType) {
            case grp_thor:
                kind = "Thor";
                break;
            case grp_roxie:
                kind = "Roxie";
                break;
            case grp_hthor:
                kind = "hthor";
                break;
        }
        if (kind)
            cluster->setProp("@kind",kind);
        if (dir)
            cluster->setProp("@dir",dir);
        Owned<INodeIterator> iter = group->getIterator();
        StringBuffer str;
        ForEach(*iter) {
            iter->query().endpoint().getIpText(str.clear());
            IPropertyTree *n = createPTree("Node");
            n->setProp("@ip",str.str());
            cluster->addPropTree("Node", n);
        }
        return cluster.getClear();
    }

    IPropertyTree *createClusterGroupFromEnvCluster(GroupType groupType, IPropertyTree &cluster, const char *dir, bool realCluster)
    {
        Owned<IGroup> group = getGroupFromCluster(groupType, cluster);
        if (!group)
            return NULL;
        return createClusterGroup(groupType, group, dir, realCluster);
    }

    bool constructGroup(IPropertyTree &cluster, const char *altName, IPropertyTree *oldEnvCluster, GroupType groupType, bool force, StringBuffer &messages)
    {
        /* a 'realCluster' is a cluster who's name matches it's nodeGroup
         * if the nodeGroup differs it implies it's sharing the nodeGroup with other thor instance(s).
         */
        bool realCluster = true;
        bool oldRealCluster = true;
        StringBuffer gname, oldGname;
        const char *defDir = NULL;
        switch (groupType)
        {
            case grp_thor:
                getClusterGroupName(cluster, gname);
                if (!streq(cluster.queryProp("@name"), gname.str()))
                    realCluster = false;
                if (oldEnvCluster)
                {
                    getClusterGroupName(*oldEnvCluster, oldGname);
                    if (!streq(oldEnvCluster->queryProp("@name"), oldGname.str()))
                        oldRealCluster = false;
                }
                break;
            case grp_thorspares:
                getClusterSpareGroupName(cluster, gname);
                realCluster = false;
                break;
            case grp_roxie:
                gname.append(cluster.queryProp("@name"));
                break;
            default:
                throwUnexpected();
        }
        if (altName)
            gname.clear().append(altName).toLowerCase();

        VStringBuffer xpath("Group[@name=\"%s\"]", gname.str());
        IPropertyTree *existingClusterGroup = groupsconnlock.conn->queryRoot()->queryPropTree(xpath.str()); // 'live' cluster group

        bool matchOldEnv = false;
        Owned<IPropertyTree> newClusterGroup = createClusterGroupFromEnvCluster(groupType, cluster, defDir, realCluster);
        bool matchExisting = clusterGroupCompare(newClusterGroup, existingClusterGroup);
        if (oldEnvCluster)
        {
            // new matches old, only if neither has changed it's name to mismatch it's nodeGroup name
            if (realCluster == oldRealCluster)
            {
                Owned<IPropertyTree> oldClusterGroup = createClusterGroupFromEnvCluster(groupType, *oldEnvCluster, defDir, oldRealCluster);
                matchOldEnv = clusterGroupCompare(newClusterGroup, oldClusterGroup);
            }
            else
                matchOldEnv = false;
        }
        if (!matchExisting)
        {
            if (force)
            {
                VStringBuffer msg("Forcing new group layout for %s [ matched active = %s, matched old environment = %s ]", gname.str(), matchExisting?"true":"false", matchOldEnv?"true":"false");
                WARNLOG("%s", msg.str());
                messages.append(msg).newline();
                matchExisting = matchOldEnv = false;
            }
            else
            {
                VStringBuffer msg("Active cluster '%s' group layout does not match environment [matched old environment=%s]", gname.str(), matchOldEnv?"true":"false");
                LOG(MCoperatorWarning, unknownJob, "%s", msg.str());                                                                        \
                messages.append(msg).newline();
            }
        }
        if (!existingClusterGroup || (!matchExisting && !matchOldEnv))
        {
            VStringBuffer msg("New cluster layout for cluster %s", gname.str());
            WARNLOG("%s", msg.str());
            messages.append(msg).newline();
            addClusterGroup(gname.str(), newClusterGroup.getClear(), realCluster);
            return true;
        }
        return false;
    }

    void constructHThorGroups(IPropertyTree &cluster)
    {
        const char *groupname = cluster.queryProp("@name");
        if (!groupname || !*groupname)
            return;
        unsigned ins = 0;
        Owned<IPropertyTreeIterator> insts = cluster.getElements("Instance");
        ForEach(*insts) {
            const char *na = insts->query().queryProp("@netAddress");
            if (na&&*na) {
                SocketEndpoint ep(na);
                if (!ep.isNull()) {
                    ins++;
                    VStringBuffer gname("hthor__%s", groupname);
                    if (ins>1)
                        gname.append('_').append(ins);
                    Owned<IGroup> group = createIGroup(1, &ep);
                    Owned<IPropertyTree> clusterGroup = createClusterGroup(grp_hthor, group, NULL, true);
                    addClusterGroup(gname.str(), clusterGroup.getClear(), true);
                }
            }
        }
    }

    enum CgCmd { cg_null, cg_reset, cg_add, cg_remove };
public:

    CInitGroups(unsigned _defaultTimeout)
        : groupsconnlock("constructGroup",SDS_GROUPSTORE_ROOT,true,false,false,_defaultTimeout)
    {
        defaultTimeout = _defaultTimeout;
    }

    bool doClusterGroup(CgCmd cmd, const char *_clusterName, const char *type, bool spares, SocketEndpointArray *eps, StringBuffer &messages)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return false;
        if (!_clusterName || !*_clusterName)
            return false;
        StringAttr clusterName = _clusterName;
        clusterName.toLowerCase();
        if (!type || !*type)
            return false;
        bool ret = true;
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> clusters;
        StringBuffer errMsg;
        const char *clusterType = type;
        if (loadMachineMap()) {
            VStringBuffer xpath("%s[@name=\"%s\"]", type, clusterName.get());
            clusters.setown(root->getElements(xpath.str()));
            if (!clusters || !clusters->first()) {
                VStringBuffer errMsg("Could not find type %s, %s cluster", type, clusterName.get());
                WARNLOG("%s", errMsg.str());
                messages.append(errMsg).newline();
                ret = false;
            }
            else {
                if (!streq("ThorCluster", type))
                    return false; // currently only Thor supported here.
                IPropertyTree &cluster = clusters->query();

                switch (cmd) {
                    case cg_reset:
                    {
                        if (spares) {
                            if (!constructGroup(cluster,NULL,NULL,grp_thorspares,true,messages))
                                ret = false;
                        }
                        else {
                            if (!constructGroup(cluster,NULL,NULL,grp_thor,true,messages))
                                ret = false;
                        }
                        break;
                    }
                    case cg_add:
                    {
                        assertex(eps);
                        StringBuffer groupName;
                        getClusterSpareGroupName(cluster, groupName);
                        IPropertyTree *root = groupsconnlock.conn->queryRoot();
                        VStringBuffer xpath("Group[@name=\"%s\"]",groupName.str());
                        IPropertyTree *existing = root->queryPropTree(xpath.str());
                        if (existing) {
                            Owned<IPropertyTreeIterator> iter = existing->getElements("Node");
                            ForEach(*iter) {
                                SocketEndpoint ep(iter->query().queryProp("@ip"));
                                if (eps->zap(ep)) {
                                    StringBuffer epStr;
                                    VStringBuffer errMsg("addSpares: not adding: %s, already in spares", ep.getUrlStr(epStr).str());
                                    WARNLOG("%s", errMsg.str());
                                    messages.append(errMsg).newline();
                                    while (eps->zap(ep)); // delete any other duplicates
                                }
                            }
                        }
                        else {
                            existing = createPTree();
                            existing->setProp("@name", groupName.str());
                            existing = root->addPropTree("Group", existing);
                        }
                        // add remaining
                        ForEachItemIn(e, *eps) {
                            const SocketEndpoint &ep = eps->item(e);
                            StringBuffer ipStr;
                            ep.getIpText(ipStr);
                            IPropertyTree *node = createPTree();
                            node->setProp("@ip", ipStr.str());
                            existing->addPropTree("Node", node);
                        }
                        break;
                    }
                    case cg_remove:
                    {
                        assertex(eps);
                        StringBuffer groupName;
                        getClusterSpareGroupName(cluster, groupName);
                        IPropertyTree *root = groupsconnlock.conn->queryRoot();
                        VStringBuffer xpath("Group[@name=\"%s\"]", groupName.str());
                        IPropertyTree *existing = root->queryPropTree(xpath.str());
                        if (existing) {
                            ForEachItemIn(e, *eps) {
                                const SocketEndpoint &ep = eps->item(e);
                                StringBuffer ipStr;
                                ep.getIpText(ipStr);
                                VStringBuffer xpath("Node[@ip=\"%s\"]", ipStr.str());
                                if (!existing->removeProp(xpath.str())) {
                                    VStringBuffer errMsg("removeSpares: %s not found in spares", ipStr.str());
                                    WARNLOG("%s", errMsg.str());
                                    messages.append(errMsg).newline();
                                    while (eps->zap(ep)); // delete any other duplicates
                                }
                                else
                                    while (existing->removeProp(xpath.str())); // remove any others, shouldn't be any
                            }
                        }
                        break;
                    }
                }
                if (clusters->next()) {
                    VStringBuffer errMsg("resetThorGroup: more than one cluster named: %s", clusterName.get());
                    WARNLOG("%s", errMsg.str());
                    messages.append(errMsg).newline();
                    ret = false;
                }
            }
        }
        return ret;
    }
    bool resetClusterGroup(const char *clusterName, const char *type, bool spares, StringBuffer &messages)
    {
        return doClusterGroup(cg_reset, clusterName, type, spares, NULL, messages);
    }
    bool addSpares(const char *clusterName, const char *type, SocketEndpointArray &eps, StringBuffer &messages)
    {
        return doClusterGroup(cg_add, clusterName, type, true, &eps, messages);
    }
    bool removeSpares(const char *clusterName, const char *type, SocketEndpointArray &eps, StringBuffer &messages)
    {
        return doClusterGroup(cg_remove, clusterName, type, true, &eps, messages);
    }
    void constructGroups(bool force, StringBuffer &messages, IPropertyTree *oldEnvironment)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return;
        clusternames.kill();
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> clusters;
        if (loadMachineMap()) {
            clusters.setown(root->getElements("ThorCluster"));
            ForEach(*clusters) {
                IPropertyTree &cluster = clusters->query();
                IPropertyTree *oldCluster = NULL;
                if (oldEnvironment) {
                    VStringBuffer xpath("Software/ThorCluster[@name=\"%s\"]", cluster.queryProp("@name"));
                    oldCluster = oldEnvironment->queryPropTree(xpath.str());
                }
                constructGroup(cluster,NULL,oldCluster,grp_thor,force,messages);
                constructGroup(cluster,NULL,oldCluster,grp_thorspares,force,messages);
            }
            clusters.setown(root->getElements("RoxieCluster"));
            ForEach(*clusters) {
                IPropertyTree &cluster = clusters->query();
                IPropertyTree *oldCluster = NULL;
                if (oldEnvironment) {
                    VStringBuffer xpath("Software/RoxieCluster[@name=\"%s\"]", cluster.queryProp("@name"));
                    oldCluster = oldEnvironment->queryPropTree(xpath.str());
                }
                constructGroup(cluster,NULL,oldCluster,grp_roxie,force,messages);
            }
            clusters.setown(root->getElements("EclAgentProcess"));
            ForEach(*clusters) {
                IPropertyTree &cluster = clusters->query();
                constructHThorGroups(cluster);
            }

            // correct cluster flags
            // JCSMORE - why was this necessary, may well be legacy..
            Owned<IPropertyTreeIterator> grps = groupsconnlock.conn->queryRoot()->getElements("Group");
            ForEach(*grps) {
                IPropertyTree &grp = grps->query();
                const char *name = grp.queryProp("@name");
                bool iscluster = NotFound != clusternames.find(name);
                if (iscluster!=grp.getPropBool("@cluster"))
                {
                    if (iscluster)
                        grp.setPropBool("@cluster", true);
                    else
                        grp.removeProp("@cluster");
                }
            }
        }
    }
};

void initClusterGroups(bool force, StringBuffer &response, IPropertyTree *oldEnvironment, unsigned timems)
{
    CInitGroups init(timems);
    init.constructGroups(force, response, oldEnvironment);
}

bool resetClusterGroup(const char *clusterName, const char *type, bool spares, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems);
    return init.resetClusterGroup(clusterName, type, spares, response);
}

bool addClusterSpares(const char *clusterName, const char *type, SocketEndpointArray &eps, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems);
    return init.addSpares(clusterName, type, eps, response);
}

bool removeClusterSpares(const char *clusterName, const char *type, SocketEndpointArray &eps, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems);
    return init.removeSpares(clusterName, type, eps, response);
}

class CDaliDFSServer: public Thread, public CTransactionLogTracker, implements IDaliServer, implements IExceptionHandler
{  // Coven size
    
    bool stopped;
    unsigned defaultTimeout;
    unsigned numThreads;

public:

    IMPLEMENT_IINTERFACE;

    CDaliDFSServer(IPropertyTree *config)
        : Thread("CDaliDFSServer"), CTransactionLogTracker(MDFS_MAX)
    {
        stopped = true;
        defaultTimeout = INFINITE; // server uses default
        numThreads = config->getPropInt("DFS/@numThreads", DEFAULT_NUM_DFS_THREADS);
        PROGLOG("DFS Server: numThreads=%d", numThreads);
    }

    ~CDaliDFSServer()
    {
    }

    void start()
    {
        Thread::start();
    }

    void ready()
    {
    }

    void suspend()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DFS_REQUEST);
        }
        join();
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageHandler<CDaliDFSServer> handler("CDaliDFSServer", this, &CDaliDFSServer::processMessage, this, numThreads, TIMEOUT_ON_CLOSEDOWN, INFINITE);
        CMessageBuffer mb;
        stopped = false;
        while (!stopped)
        {
            try
            {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DFS_REQUEST,NULL))
                {
                    handler.handleMessage(mb);
                    mb.clear(); // ^ has copied mb
                }   
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CDaliDFSServer");
                e->Release();
            }
        }
        return 0;
    }

    void iterateFiles(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_ITERATE_FILES, mb.getSender());

        StringAttr wildname;
        bool recursive;
        bool includesuper = false;
        StringAttr attr;
        mb.read(wildname).read(recursive).read(attr);
        trc.appendf("iterateFiles(%s,%s,%s)",wildname.str(),recursive?"recursive":"",attr.str());
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        Owned<IUserDescriptor> udesc;
        if (mb.getPos()<mb.length()) {
            mb.read(includesuper);
            if (mb.getPos()<mb.length()) {
                udesc.setown(createUserDescriptor());
                udesc->deserialize(mb);
            }
        }

        mb.clear();
        unsigned count=0;
        mb.append(count);

        CFileScanner scanner;
        CSDSServerLockBlock sdsLock; // lock sds while scanning
        unsigned start = msTick();
        scanner.scan(sdsLock, wildname.get(),recursive,includesuper);
        unsigned tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(filescan): %s: took %dms",trc.str(), tookMs);
        sdsLock.unlock(); // unlock to perform authentification

        bool auth = querySessionManager().checkScopeScansLDAP()&&getScopePermissions(NULL,udesc,(unsigned)-1);
        StringArray authScopes;
        CIArrayOf<CFileMatch> matchingFiles;
        start = msTick();
        scanner.getResults(auth, udesc, matchingFiles, authScopes, count, false);
        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(LDAP): %s: took %dms, %d lookups, file matches = %d", trc.str(), tookMs, authScopes.ordinality(), count);

        sdsLock.lock(); // re-lock sds while serializing
        start = msTick();
        SerializeFileAttrOptions options; //The options is needed for the serializeFileAttributes()
        ForEachItemIn(m, matchingFiles)
        {
            CFileMatch &fileMatch = matchingFiles.item(m);
            CDFAttributeIterator::serializeFileAttributes(mb, fileMatch.queryFileTree(), fileMatch.queryName(), fileMatch.queryIsSuper(), options);
        }
        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(filescan-serialization): %s: took %dms, %d files",trc.str(), tookMs, count);

        mb.writeDirect(0,sizeof(count),&count);
    }

    void iterateFilteredFiles(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_ITERATE_FILTEREDFILES, mb.getSender());

        Owned<IUserDescriptor> udesc;
        StringAttr filters;
        bool recursive;
        mb.read(filters).read(recursive);
        trc.appendf("iterateFilteredFiles(%s,%s)",filters.str(),recursive?"recursive":"");
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        if (mb.getPos()<mb.length())
        {
            udesc.setown(createUserDescriptor());
            udesc->deserialize(mb);
        }

        mb.clear();
        unsigned count=0;
        mb.append(count);

        Owned<CIterateFileFilterContainer> iterateFileFilterContainer =  new CIterateFileFilterContainer();
        iterateFileFilterContainer->readFilters(filters);

        CFileScanner scanner;
        CSDSServerLockBlock sdsLock; // lock sds while scanning
        unsigned start = msTick();
        scanner.scan(sdsLock, iterateFileFilterContainer.getLink(), recursive);
        unsigned tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(filescan): %s: took %dms",trc.str(), tookMs);
        sdsLock.unlock(); // unlock to perform authentification

        bool auth = querySessionManager().checkScopeScansLDAP()&&getScopePermissions(NULL,udesc,(unsigned)-1);
        StringArray authScopes;
        CIArrayOf<CFileMatch> matchingFiles;
        start = msTick();
        bool returnAllMatchingFiles = true;
        try
        {
            scanner.getResults(auth, udesc, matchingFiles, authScopes, count, true);
        }
        catch(IException *e)
        {
            if (DFSERR_PassIterateFilesLimit != e->errorCode())
                throw;
            e->Release();
            returnAllMatchingFiles = false;
        }
        mb.append(returnAllMatchingFiles);

        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(LDAP): %s: took %dms, %d lookups, file matches = %d", trc.str(), tookMs, authScopes.ordinality(), count);

        sdsLock.lock(); // re-lock sds while serializing
        start = msTick();
        ForEachItemIn(m, matchingFiles)
        {
            CFileMatch &fileMatch = matchingFiles.item(m);
            CDFAttributeIterator::serializeFileAttributes(mb, fileMatch.queryFileTree(), fileMatch.queryName(), fileMatch.queryIsSuper(), iterateFileFilterContainer->getSerializeFileAttrOptions());
        }
        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(filescan-serialization): %s: took %dms, %d files",trc.str(), tookMs, count);

        mb.writeDirect(0,sizeof(count),&count);
    }

    void iterateRelationships(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_ITERATE_RELATIONSHIPS, mb.getSender());

        StringAttr primary;
        StringAttr secondary;
        StringAttr primflds;
        StringAttr secflds;
        StringAttr kind;
        StringAttr cardinality;
        byte payloadb;
        mb.read(primary).read(secondary).read(primflds).read(secflds).read(kind).read(cardinality).read(payloadb);
        mb.clear();
        bool payload = (payloadb==1);
        trc.appendf("iterateRelationships(%s,%s,%s,%s,%s,%s,%d)",primary.str(),secondary.str(),primflds.str(),secflds.str(),kind.str(),cardinality.str(),(int)payloadb);
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        unsigned start = msTick();
        unsigned count=0;
        CSDSServerLockBlock sdsLock; // lock sds while scanning
        StringBuffer xpath;
        CDistributedFileDirectory::getFileRelationshipXPath(xpath,primary,secondary,primflds,secflds,kind,cardinality,((payloadb==0)||(payloadb==1))?&payload:NULL);
        IPropertyTree *root = sdsLock->queryPropTree(querySdsRelationshipsRoot());
        Owned<IPropertyTreeIterator> iter = root?root->getElements(xpath.str()):NULL;
        mb.append(count);
        // save as sequence of branches
        if (iter) {
            ForEach(*iter.get()) {
                iter->query().serialize(mb);
                count++;
            }
        }
        if (msTick()-start>100) {
            PROGLOG("TIMING(relationshipscan): %s: took %dms, %d relations",trc.str(),msTick()-start,count);
        }
        mb.writeDirect(0,sizeof(count),&count);
    }

    void setFileAccessed(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_SET_FILE_ACCESSED, mb.getSender());
        StringAttr lname;
        mb.read(lname);
        CDateTime dt;
        dt.deserialize(mb);
        trc.appendf("setFileAccessed(%s)",lname.str());
        Owned<IUserDescriptor> udesc;
        if (mb.getPos()<mb.length()) {
            udesc.setown(createUserDescriptor());
            udesc->deserialize(mb);
        }
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        mb.clear();
        StringBuffer tail;
        CDfsLogicalFileName dlfn;   
        dlfn.set(lname);
        if (!checkLogicalName(dlfn,udesc,true,false,true,"setFileAccessed on"))
            return;
        CScopeConnectLock sconnlock("setFileAccessed", dlfn, false, false, false, defaultTimeout);
        IPropertyTree* sroot = sconnlock.conn()?sconnlock.conn()->queryRoot():NULL;
        dlfn.getTail(tail);
        Owned<IPropertyTree> tree = getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
        if (tree) {
            StringBuffer str;
            tree->setProp("@accessed",dt.getString(str).str());
        }
    }

    void setFileProtect(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_SET_FILE_PROTECT, mb.getSender());
        StringAttr lname;
        StringAttr owner;
        bool set;
        mb.read(lname).read(owner).read(set);
        trc.appendf("setFileProtect(%s,%s,%s)",lname.str(),owner.str(),set?"true":"false");
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        Owned<IUserDescriptor> udesc;
        if (mb.getPos()<mb.length()) {
            udesc.setown(createUserDescriptor());
            udesc->deserialize(mb);
        }
        mb.clear();
        StringBuffer tail;
        CDfsLogicalFileName dlfn;   
        dlfn.set(lname);
        if (!checkLogicalName(dlfn,udesc,true,false,true,"setFileProtect"))
            return;
        CScopeConnectLock sconnlock("setFileProtect", dlfn, false, false, false, defaultTimeout);
        IPropertyTree* sroot = sconnlock.conn()?sconnlock.conn()->queryRoot():NULL;
        dlfn.getTail(tail);
        Owned<IPropertyTree> tree = getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
        if (!tree)
            tree.setown(getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str(),false));
        if (tree) {
            IPropertyTree *pt = tree->queryPropTree("Attr");
            if (pt) 
                setFileProtectTree(*pt,*owner?owner:owner,set);
        }
    }

    void getFileTree(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_GET_FILE_TREE, mb.getSender());
        StringAttr lname;
        mb.read(lname);
        unsigned ver;
        if (mb.length()<mb.getPos()+sizeof(unsigned))
            ver = 0;
        else {
            mb.read(ver);
            // this is a bit of a mess - for backward compatibility where user descriptor specified
            if (ver>MDFS_GET_FILE_TREE_V2) {
                mb.reset(mb.getPos()-sizeof(unsigned));
                ver = 0;
            }
        }
        trc.appendf("getFileTree(%s,%d)",lname.str(),ver);
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        Owned<IUserDescriptor> udesc;
        if (mb.getPos()<mb.length()) {
            udesc.setown(createUserDescriptor());
            udesc->deserialize(mb);
        }
        mb.clear();
        CDfsLogicalFileName dlfn;   
        dlfn.set(lname);
        CDfsLogicalFileName *logicalname=&dlfn;
        Owned<IDfsLogicalFileNameIterator> redmatch;
        loop {
            StringBuffer tail;
            checkLogicalName(*logicalname,udesc,true,false,true,"getFileTree on");
            CScopeConnectLock sconnlock("getFileTree", *logicalname, false, false, false, defaultTimeout);
            IPropertyTree* sroot = sconnlock.conn()?sconnlock.conn()->queryRoot():NULL;
            logicalname->getTail(tail);
            Owned<IPropertyTree> tree = getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
            if (tree) {
                if (ver>=MDFS_GET_FILE_TREE_V2) {
                    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),IFDSF_EXCLUDE_CLUSTERNAMES);
                    if (fdesc) {
                        ver = MDFS_GET_FILE_TREE_V2;
                        mb.append((int)-2).append(ver);
                        fdesc->serialize(mb);
                        StringBuffer dts;
                        if (tree->getProp("@modified",dts)) {
                            CDateTime dt;
                            dt.setString(dts.str());
                            dt.serialize(mb);
                        }
                    }
                    else
                        ver = 0;
                }
                if (ver==0) {
                    tree.setown(createPTreeFromIPT(tree));
                    StringBuffer cname;
                    logicalname->getCluster(cname);
                    expandFileTree(tree,true,cname.str()); // resolve @node values that may not be set
                    tree->serialize(mb);
                }
                break;
            }
            else {
                tree.setown(getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str(),false));
                if (tree) {
                    tree->serialize(mb);
                    break;
                }
            }
            if (redmatch.get()) {
                if (!redmatch->next())
                    break;
            }
            else {
                redmatch.setown(queryDistributedFileDirectory().queryRedirection().getMatch(logicalname->get()));
                if (!redmatch.get()) 
                    break;
                if (!redmatch->first())
                    break;
            }
            logicalname = &redmatch->query();
        }
    }

    void getGroupTree(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_GET_GROUP_TREE, mb.getSender());
        StringAttr gname;
        mb.read(gname);
        mb.clear();
        trc.appendf("getGroupTree(%s)",gname.str());
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        byte ok;
        CConnectLock connlock("getGroupTree",SDS_GROUPSTORE_ROOT,false,false,false,defaultTimeout);
        Owned<IPropertyTree> pt = getNamedPropTree(connlock.conn->queryRoot(),"Group","@name",gname.get(),true);
        if (pt) {
            ok = 1;
            mb.append(ok);
            pt->serialize(mb);
        }
        else {
            ok = 0;
            mb.append(ok);
        }
    }

    void processMessage(CMessageBuffer &mb)
    {
        CheckTime block0("CDaliDFSServer::processMessage ");
        ICoven &coven=queryCoven();
        StringBuffer trc;
        int fn;
        mb.read(fn);

        try {
            switch (fn) {
            case MDFS_ITERATE_FILES: {
                    iterateFiles(mb,trc);                    
                }
                break;
            case MDFS_ITERATE_FILTEREDFILES: {
                    iterateFilteredFiles(mb,trc);
                }
                break;
            case MDFS_ITERATE_RELATIONSHIPS: {
                    iterateRelationships(mb,trc);                    
                }
                break;
            case MDFS_GET_FILE_TREE: {
                    getFileTree(mb,trc);
                }
                break;
            case MDFS_GET_GROUP_TREE: {
                    getGroupTree(mb,trc);
                }
                break;
            case MDFS_SET_FILE_ACCESSED: {
                    setFileAccessed(mb,trc);
                }
                break;
            case MDFS_SET_FILE_PROTECT: {
                    setFileProtect(mb,trc);
                }
                break;
            default: {
                    mb.clear();
                }
                break;
            }
        }
        catch (IException *e) {
            int err=-1; // exception marker
            mb.clear().append(err); 
            serializeException(e, mb); 
            e->Release();
        }
        coven.reply(mb);    
        if (block0.slow()) {
            SocketEndpoint ep = mb.getSender();
            ep.getUrlStr(block0.appendMsg(trc).append(" from "));
        }
    }   

    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }

    // CTransactionLogTracker
    virtual StringBuffer &getCmdText(unsigned cmd, StringBuffer &ret) const
    {
        switch (cmd)
        {
        case MDFS_ITERATE_FILES:
            return ret.append("MDFS_ITERATE_FILES");
        case MDFS_ITERATE_FILTEREDFILES:
            return ret.append("MDFS_ITERATE_FILTEREDFILES");
        case MDFS_ITERATE_RELATIONSHIPS:
            return ret.append("MDFS_ITERATE_RELATIONSHIPS");
        case MDFS_GET_FILE_TREE:
            return ret.append("MDFS_GET_FILE_TREE");
        case MDFS_GET_GROUP_TREE:
            return ret.append("MDFS_GET_GROUP_TREE");
        case MDFS_SET_FILE_ACCESSED:
            return ret.append("MDFS_SET_FILE_ACCESSED");
        case MDFS_SET_FILE_PROTECT:
            return ret.append("MDFS_SET_FILE_PROTECT");
        default:
            return ret.append("UNKNOWN");
        }
    }
    // IExceptionHandler impl.
    virtual bool fireException(IException *e)
    {
        EXCLOG(e, "CDaliDFSServer exception");
        return true;
    }
} *daliDFSServer = NULL;


IDFAttributesIterator *CDistributedFileDirectory::getDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive, bool includesuper,INode *foreigndali,unsigned foreigndalitimeout)
{
    if (!wildname||!*wildname||(strcmp(wildname,"*")==0)) {
        recursive = true;
    }
    CMessageBuffer mb;
    mb.append((int)MDFS_ITERATE_FILES).append(wildname).append(recursive).append("").append(includesuper); // "" is legacy
    if (user)
        user->serialize(mb);
#ifdef NULL_DALIUSER_STACKTRACE
    else
    {
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp getDFAttributesIterator() line %d",__LINE__);
        PrintStackReport();
    }
#endif

    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
    return new CDFAttributeIterator(mb);
}

IDFScopeIterator *CDistributedFileDirectory::getScopeIterator(IUserDescriptor *user, const char *basescope, bool recursive,bool includeempty)
{
    return new CDFScopeIterator(this,basescope,recursive,includeempty,defaultTimeout);
}

static bool isValidLFN(const char *lfn)
{ // bit OTT
    if (!lfn||!*lfn||(strcmp(lfn,".")==0))
        return false;
    StringBuffer tmp(".::");
    tmp.append(lfn);
    CDfsLogicalFileName dlfn;
    return dlfn.setValidate(tmp.str());
}

bool CDistributedFileDirectory::loadScopeContents(const char *scopelfn,
                         StringArray *scopes,
                         StringArray *supers,
                         StringArray *files,
                         bool includeemptyscopes
                         )
{
    StringBuffer baseq;
    if (scopelfn&&*scopelfn) {
        if (memcmp(scopelfn,".::",3)==0)        // scopes not in .
            scopelfn += 3;
        StringBuffer tmp(scopelfn);
        if (tmp.trim().length()) {
            tmp.append("::.");
            CDfsLogicalFileName dlfn;
            if (!dlfn.setValidate(tmp.str()))
                return false;
            dlfn.makeScopeQuery(baseq,false);
        }
    }
    CConnectLock connlock("CDistributedFileDirectory::loadScopeContents",querySdsFilesRoot(),false,false,false,defaultTimeout);
    if (!connlock.conn) 
        return false;
    IPropertyTree *root = connlock.conn->queryRoot();
    if (!root) 
        return false;

    if (baseq.length()) {
        root = root->queryPropTree(baseq.str());
        if (!root) 
            return false;
    }
    Owned<IPropertyTreeIterator> iter;
    if (scopes) {
        iter.setown(root->getElements(queryDfsXmlBranchName(DXB_Scope)));
        ForEach(*iter) {
            IPropertyTree &ct = iter->query();
            if (includeemptyscopes||!recursiveCheckEmptyScope(ct)) {
                StringBuffer name;
                if (ct.getProp("@name",name)&&name.trim().length()&&isValidLFN(name.str()))
                    scopes->append(name.str());
            }
        }
    }
    if (!supers&&!files)
        return true;
        
    if (baseq.length()==0) { // bit odd but top level files are in '.'
        CDfsLogicalFileName dlfn;
        dlfn.set(".",".");
        dlfn.makeScopeQuery(baseq,false);
        root = root->queryPropTree(baseq.str());
        if (!root) 
            return true;    
    }
    if (supers) {
        iter.setown(root->getElements(queryDfsXmlBranchName(DXB_SuperFile)));
        ForEach(*iter) {
            IPropertyTree &ct = iter->query();
            StringBuffer name;
            if (ct.getProp("@name",name)&&name.trim().length()&&isValidLFN(name.str()))
                supers->append(name.str());
        }
    }
    if (files) {
        iter.setown(root->getElements(queryDfsXmlBranchName(DXB_File)));
        ForEach(*iter) {
            StringBuffer name;
            IPropertyTree &ct = iter->query();
            if (ct.getProp("@name",name)&&name.trim().length()&&isValidLFN(name.str()))
                files->append(name.str());
        }
    }
    return true;
}

void CDistributedFileDirectory::setFileAccessed(CDfsLogicalFileName &dlfn,IUserDescriptor *user, const CDateTime &dt, const INode *foreigndali,unsigned foreigndalitimeout)
{
    // this accepts either a foreign dali node or a foreign lfn
    Owned<INode> fnode;
    SocketEndpoint ep;
    const char *lname;
    if (dlfn.isForeign()) {
        if (!dlfn.getEp(ep)) 
            throw MakeStringException(-1,"cannot resolve dali ip in foreign file name (%s)",dlfn.get());
        fnode.setown(createINode(ep));
        foreigndali = fnode;
        lname = dlfn.get(true);
    }
    else if (dlfn.isExternal())
        return;
    else
        lname = dlfn.get();
    if (isLocalDali(foreigndali))
        foreigndali = NULL;
    CMessageBuffer mb;
    mb.append((int)MDFS_SET_FILE_ACCESSED).append(lname);
    dt.serialize(mb);
    if (user)
        user->serialize(mb);
#ifdef NULL_DALIUSER_STACKTRACE
    else
    {
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp setFileAccessed() line %d",__LINE__);
        PrintStackReport();
    }
#endif
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

void CDistributedFileDirectory::setFileProtect(CDfsLogicalFileName &dlfn,IUserDescriptor *user, const char *owner, bool set, const INode *foreigndali,unsigned foreigndalitimeout)
{
    // this accepts either a foreign dali node or a foreign lfn
    Owned<INode> fnode;
    SocketEndpoint ep;
    const char *lname;
    if (dlfn.isForeign()) {
        if (!dlfn.getEp(ep)) 
            throw MakeStringException(-1,"cannot resolve dali ip in foreign file name (%s)",dlfn.get());
        fnode.setown(createINode(ep));
        foreigndali = fnode;
        lname = dlfn.get(true);
    }
    else if (dlfn.isExternal())
        return;
    else
        lname = dlfn.get();
    if (isLocalDali(foreigndali))
        foreigndali = NULL;
    CMessageBuffer mb;
    if (!owner)
        owner = "";
    mb.append((int)MDFS_SET_FILE_PROTECT).append(lname).append(owner).append(set);
    if (user)
        user->serialize(mb);
#ifdef NULL_DALIUSER_STACKTRACE
    else
    {
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp setFileProtect() line %d",__LINE__);
        PrintStackReport();
    }
#endif
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

IPropertyTree *CDistributedFileDirectory::getFileTree(const char *lname, IUserDescriptor *user, const INode *foreigndali,unsigned foreigndalitimeout, bool expandnodes, bool appendForeign)
{
    // this accepts either a foreign dali node or a foreign lfn
    Owned<INode> fnode;
    CDfsLogicalFileName dlfn;
    SocketEndpoint ep;
    dlfn.set(lname);
    if (dlfn.isForeign()) {
        if (!dlfn.getEp(ep)) 
            throw MakeStringException(-1,"cannot resolve dali ip in foreign file name (%s)",lname);
        fnode.setown(createINode(ep));
        foreigndali = fnode;
        lname = dlfn.get(true);
    }
    if (isLocalDali(foreigndali))
        foreigndali = NULL;
    CMessageBuffer mb;
    mb.append((int)MDFS_GET_FILE_TREE).append(lname);
    mb.append(MDFS_GET_FILE_TREE_V2);
    if (user)
        user->serialize(mb);
#ifdef NULL_DALIUSER_STACKTRACE
    else
    {
        DBGLOG("UNEXPECTED USER (NULL) in dadfs.cpp getFileTree() line %d",__LINE__);
        PrintStackReport();
    }
#endif
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
    if (mb.length()==0)
        return NULL;
    unsigned ver = 0;
    if ((mb.length()>=sizeof(int))&&(*(int *)mb.bufferBase()) == -2) { // version indicator
        int i;
        mb.read(i);
        mb.read(ver);
    }
    Owned<IPropertyTree> ret;
    if (ver==0)
        ret.setown(createPTree(mb));
    else {
        Owned<IFileDescriptor> fdesc;
        CDateTime modified;
        if (ver==MDFS_GET_FILE_TREE_V2) { // no longer in use but support for back compatibility
            fdesc.setown(deserializeFileDescriptor(mb));
            if (mb.remaining()>0)
                modified.deserialize(mb);
        }
        else
            throw MakeStringException(-1,"Unknown GetFileTree serialization version %d",ver);
        ret.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
        fdesc->serializeTree(*ret,expandnodes?0:CPDMSF_packParts);
        if (!modified.isNull()) {
            StringBuffer dts;
            ret->setProp("@modified",modified.getString(dts).str());
        }
    }
    if (expandnodes) {
        StringBuffer cname;
        dlfn.getCluster(cname);
        expandFileTree(ret,true,cname.str());
        CDfsLogicalFileName dlfn2;
        dlfn2.set(dlfn);
        if (foreigndali) 
            dlfn2.setForeign(foreigndali->endpoint(),false);
        ret->setProp("OrigName",dlfn.get());
    }
    if (foreigndali && appendForeign)
        resolveForeignFiles(ret,foreigndali);
    return ret.getClear();
}

IFileDescriptor *CDistributedFileDirectory::getFileDescriptor(const char *lname,IUserDescriptor *user,const INode *foreigndali,unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname,user,foreigndali,foreigndalitimeout,false);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        Owned<CDistributedSuperFile> sfile = new CDistributedSuperFile(this,tree, dlfn, user);
        return sfile->getFileDescriptor(NULL);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?


    IFileDescriptor * fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),0);
    if (fdesc)
        fdesc->setTraceName(lname);
    return fdesc;
}

IDistributedFile *CDistributedFileDirectory::getFile(const char *lname,IUserDescriptor *user,const INode *foreigndali,unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname,user,foreigndali,foreigndalitimeout,false);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        return new CDistributedSuperFile(this,tree, dlfn, user);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?

    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),IFDSF_FOREIGN_GROUP);
    if (!fdesc)
        return NULL;
    fdesc->setTraceName(lname);
    CDistributedFile *ret = new CDistributedFile(this, fdesc, user, true);
    ret->setLogicalName(lname);
    const char *date = tree->queryProp("@modified");
    if (ret) {
        CDateTime dt;
        if (date&&*date)
            dt.setString(date);
        ret->setModificationTime(dt);   
    }
    return ret;
}

static void addForeignName(IPropertyTree &t,const INode *foreigndali,const char *attr)
{
    StringBuffer sb;
    const char *name = t.queryProp(attr);
    if (!name||!*name)
        return;
    CDfsLogicalFileName logicalname;
    logicalname.set(name);
    if (logicalname.isExternal()||logicalname.isQuery())
        return; // how did that get in here?
    if (logicalname.isForeign()) { 
        SocketEndpoint ep;
        Owned<INode> fd = createINode(ep);
        if (logicalname.getEp(ep)&&isLocalDali(fd)) { // see if pointing back at self
            logicalname.clearForeign();
            t.setProp(attr,logicalname.get());
        }
    }
    else if (foreigndali) {
        logicalname.setForeign(foreigndali->endpoint(),false);
        t.setProp(attr,logicalname.get());
    }
}

void CDistributedFileDirectory::resolveForeignFiles(IPropertyTree *tree,const INode *foreigndali)
{
    if (!tree||!foreigndali)
        return;
    // now add to all sub files
    Owned<IPropertyTreeIterator> pe = tree->getElements("SubFile");
    ForEach(*pe) 
        addForeignName(pe->query(),foreigndali,"@name");
    pe.setown(tree->getElements("SuperOwner"));
    ForEach(*pe) 
        addForeignName(pe->query(),foreigndali,"@name");
    // do origname?
}

int CDistributedFileDirectory::getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags)
{
    CDfsLogicalFileName dlfn;
    dlfn.set(lname);
    StringBuffer scopes;
    dlfn.getScopes(scopes);
    return getScopePermissions(scopes.str(),user,auditflags);
}

int CDistributedFileDirectory::getNodePermissions(const IpAddress &ip,IUserDescriptor *user,unsigned auditflags)
{
    if (ip.isNull())
        return 0;
    CDfsLogicalFileName dlfn;
    SocketEndpoint ep(0,ip);
    dlfn.setExternal(ep,"/x");
    StringBuffer scopes;
    dlfn.getScopes(scopes,true);
    return getScopePermissions(scopes.str(),user,auditflags);
}

int CDistributedFileDirectory::getFDescPermissions(IFileDescriptor *fdesc,IUserDescriptor *user,unsigned auditflags)
{
    // this checks have access to the nodes in the file descriptor
    int ret = 255;
    unsigned np = fdesc->numParts();
    for (unsigned i=0;i<np;i++) {
        INode *node = fdesc->queryNode(i);
        if (node) {
            bool multi = false;
            RemoteMultiFilename mfn;
            unsigned n = 1;
            if (fdesc->isMulti()) {
                fdesc->getMultiFilename(i,0,mfn);
                multi = true;
                n = mfn.ordinality();
            }
            for (unsigned j = 0;j<n;j++) {
                RemoteFilename rfn;
                if (multi) {
                    rfn.set(mfn.item(j));
                }
                else
                fdesc->getFilename(i,0,rfn);
                StringBuffer localpath;
                rfn.getLocalPath(localpath);
                // translate wild cards 
                for (unsigned k=0;k<localpath.length();k++)
                    if ((localpath.charAt(k)=='?')||(localpath.charAt(k)=='*'))
                        localpath.setCharAt(k,'_');
                CDfsLogicalFileName dlfn;
                dlfn.setExternal(rfn.queryEndpoint(),localpath.str());          
                StringBuffer scopes;
                dlfn.getScopes(scopes);
                int p = getScopePermissions(scopes.str(),user,auditflags);
                if (p<ret) {
                    ret = p;
                    if (ret==0)
                        return 0;
                }
            }
        }
    }
    return ret;
}

void CDistributedFileDirectory::setDefaultUser(IUserDescriptor *user)
{
    if (user)
        defaultudesc.set(user);
    else
        defaultudesc.setown(createUserDescriptor());
}

IUserDescriptor* CDistributedFileDirectory::queryDefaultUser()
{
    return defaultudesc.get();
}

void CDistributedFileDirectory::setDefaultPreferredClusters(const char *clusters)
{
    defprefclusters.set(clusters);
}

bool removePhysicalFiles(IGroup *grp,const char *_filemask,unsigned short port,ClusterPartDiskMapSpec &mspec,IMultiException *mexcept)
{
    // TBD this won't remove repeated parts 


    PROGLOG("removePhysicalFiles(%s)",_filemask);
    if (!isAbsolutePath(_filemask))
        throw MakeStringException(-1,"removePhysicalFiles: Filename %s must be complete path",_filemask);

    size32_t l = strlen(_filemask);
    while (l&&isdigit(_filemask[l-1]))
        l--;
    unsigned width=0;
    if (l&&(_filemask[l-1]=='_'))
        width = atoi(_filemask+l);
    if (!width)
        width = grp->ordinality();

    CriticalSection errcrit;
    class casyncfor: public CAsyncFor
    {
        unsigned short port;
        CriticalSection &errcrit;
        IMultiException *mexcept;
        unsigned width;
        StringAttr filemask;
        IGroup *grp;
        ClusterPartDiskMapSpec &mspec;
    public:
        bool ok;
        casyncfor(IGroup *_grp,const char *_filemask,unsigned _width,unsigned short _port,ClusterPartDiskMapSpec &_mspec,IMultiException *_mexcept,CriticalSection &_errcrit)
            : mspec(_mspec),filemask(_filemask),errcrit(_errcrit)
        {
            grp = _grp;
            port = _port;
            ok = true;
            mexcept = _mexcept;
            width = _width;
        }
        void Do(unsigned i)
        {
            for (unsigned copy = 0; copy < 2; copy++)   // ** TBD
            {
                RemoteFilename rfn;
                constructPartFilename(grp,i+1,width,NULL,filemask,"",copy>0,mspec,rfn);
                if (port)
                    rfn.setPort(port); // if daliservix
                Owned<IFile> partfile = createIFile(rfn);
                StringBuffer eps;
                try
                {
                    unsigned start = msTick();
#if 1                   
                    if (partfile->remove()) {
                        PROGLOG("Removed '%s'",partfile->queryFilename());
                        unsigned t = msTick()-start;
                        if (t>5*1000) 
                            LOG(MCwarning, unknownJob, "Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getUrlStr(eps).str(), t/1000);
                    }
                    else
                        LOG(MCwarning, unknownJob, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
#else
                    if (partfile->exists()) 
                        PROGLOG("Would remove '%s'",partfile->queryFilename());
#endif

                }
                catch (IException *e)
                {
                    CriticalBlock block(errcrit);
                    if (mexcept) 
                        mexcept->append(*e);
                    else {
                        StringBuffer s("Failed to remove file part ");
                        s.append(partfile->queryFilename()).append(" from ");
                        rfn.queryEndpoint().getUrlStr(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                    ok = false;
                }
            }
        }
    } afor(grp,_filemask,width,port,mspec,mexcept,errcrit);
    afor.For(width,10,false,true);
    return afor.ok;
}


IDaliServer *createDaliDFSServer(IPropertyTree *config)
{
    assertex(!daliDFSServer); // initialization problem
    daliDFSServer = new CDaliDFSServer(config);
    return daliDFSServer;
}

IDistributedFileTransaction *createDistributedFileTransaction(IUserDescriptor *user)
{
    return new CDistributedFileTransaction(user);
}

static void encodeCompareResult(DistributedFileCompareResult &ret,bool differs,CDateTime &newestdt1,CDateTime &newestdt2)
{
    if (ret!=DFS_COMPARE_RESULT_FAILURE) {
        int cmp = 0;
        if (!newestdt1.isNull()) {
            if (!newestdt2.isNull()) {
                int cmp = newestdt1.compare(newestdt2,false);   
                if (cmp>=0)
                    ret = DFS_COMPARE_RESULT_SAME_NEWER;
                else
                    ret = DFS_COMPARE_RESULT_SAME_OLDER;
            }
            else
                ret = DFS_COMPARE_RESULT_SAME_NEWER;
        }
        else if (!newestdt2.isNull()) 
            ret = DFS_COMPARE_RESULT_SAME_OLDER;
        if (differs) {
            if (ret==DFS_COMPARE_RESULT_SAME_OLDER) // ok they could be same but seems rather unlikely!
                ret = DFS_COMPARE_RESULT_DIFFER_OLDER;
            else
                ret = DFS_COMPARE_RESULT_DIFFER_NEWER;
        }
    }
}

DistributedFileCompareResult CDistributedFileDirectory::fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user)
{
    DistributedFileCompareResult ret = DFS_COMPARE_RESULT_SAME;
    StringBuffer msg;
    try
    {
        Owned<IDistributedFile> file1 = lookup(lfn1, user, false, false, false, NULL, defaultTimeout);
        Owned<IDistributedFile> file2 = lookup(lfn2, user, false, false, false, NULL, defaultTimeout);
        if (!file1)
        {
            errstr.appendf("File %s not found",lfn1);
            ret = DFS_COMPARE_RESULT_FAILURE;
        }
        else if (!file2)
        {
            errstr.appendf("File %s not found",lfn2);
            ret = DFS_COMPARE_RESULT_FAILURE;
        }
        else
        {
            unsigned np = file1->numParts();
            if (np!=file2->numParts())
            {
                errstr.appendf("Files %s and %s have differing number of parts",lfn1,lfn2);
                ret = DFS_COMPARE_RESULT_FAILURE;
            }
            else
            {
                CDateTime newestdt1;
                CDateTime newestdt2;
                bool differs = false;
                class casyncfor: public CAsyncFor
                {
                    CriticalSection crit;
                    DistributedFileCompareResult &ret;
                    IDistributedFile *file1;
                    IDistributedFile *file2;
                    const char *lfn1;
                    const char *lfn2;
                    StringBuffer &errstr;
                    DistributedFileCompareMode mode;
                    bool physdatesize;
                    CDateTime &newestdt1;
                    CDateTime &newestdt2;
                    bool &differs;
                public:
                    casyncfor(const char *_lfn1,const char *_lfn2,IDistributedFile *_file1,IDistributedFile *_file2,DistributedFileCompareMode _mode,DistributedFileCompareResult &_ret,StringBuffer &_errstr,
                        CDateTime &_newestdt1,CDateTime &_newestdt2,bool &_differs)
                        : ret(_ret), errstr(_errstr),newestdt1(_newestdt1),newestdt2(_newestdt2),differs(_differs)
                    {
                        lfn1 = _lfn1;
                        lfn2 = _lfn2;
                        file1 = _file1;
                        file2 = _file2;
                        mode = _mode;
                        physdatesize = (mode==DFS_COMPARE_FILES_PHYSICAL)||(mode==DFS_COMPARE_FILES_PHYSICAL_CRCS);
                    }
                    void Do(unsigned p)
                    {
                        CriticalBlock block (crit);
                        StringBuffer msg;
                        Owned<IDistributedFilePart> part1 = file1->getPart(p);  
                        Owned<IDistributedFilePart> part2 = file2->getPart(p);  
                        CDateTime dt1;
                        RemoteFilename rfn;
                        bool ok;
                        {
                            CriticalUnblock unblock(crit);
                            ok = part1->getModifiedTime(true,physdatesize,dt1);
                        }
                        if (!ok) {
                            if (errstr.length()==0) {
                                errstr.append("Could not find ");
                                part1->getFilename(rfn);
                                rfn.getPath(errstr);
                            }
                            ret = DFS_COMPARE_RESULT_FAILURE;
                        }

                        CDateTime dt2;
                        {
                            CriticalUnblock unblock(crit);
                            ok = part2->getModifiedTime(true,physdatesize,dt2);
                        }
                        if (!ok) {
                            if (errstr.length()==0) {
                                errstr.append("Could not find ");
                                part2->getFilename(rfn);
                                rfn.getPath(errstr);
                            }
                            ret = DFS_COMPARE_RESULT_FAILURE;
                        }
                        if (ret!=DFS_COMPARE_RESULT_FAILURE) {
                            int cmp = dt1.compare(dt2,false);
                            if (cmp>0) {
                                if (newestdt1.isNull()||(dt1.compare(newestdt1,false)>0))
                                    newestdt1.set(dt1);
                            }
                            else if (cmp<0) {
                                if (newestdt2.isNull()||(dt2.compare(newestdt2,false)>0))
                                    newestdt2.set(dt2);
                            }
                        }
                        if ((ret!=DFS_COMPARE_RESULT_FAILURE)&&!differs) {
                            offset_t sz1;
                            offset_t sz2;
                            {
                                CriticalUnblock unblock(crit);
                                sz1 = part1->getFileSize(true,physdatesize);
                                sz2 = part2->getFileSize(true,physdatesize);
                            }
                            if (sz1!=sz2) 
                                differs = true;
                        }
                        if ((ret!=DFS_COMPARE_RESULT_FAILURE)&&!differs) {
                            unsigned crc1;
                            unsigned crc2;
                            if (mode==DFS_COMPARE_FILES_PHYSICAL_CRCS) {
                                {
                                    CriticalUnblock unblock(crit);
                                    crc1 = part1->getPhysicalCrc();
                                    crc2 = part2->getPhysicalCrc();
                                }
                            }
                            else {
                                if (!part1->getCrc(crc1)) 
                                    return;
                                if (!part2->getCrc(crc2)) 
                                    return;
                            }   
                            if (crc1!=crc2) 
                                differs = true;
                        }
                    }
                } afor(lfn1,lfn2,file1,file2,mode,ret,errstr,newestdt1,newestdt2,differs);
                afor.For(np,20,false,false);
                encodeCompareResult(ret,differs,newestdt1,newestdt2);
            }
        }
    }
    catch (IException *e) {
        if (errstr.length()==0)
            e->errorMessage(errstr);
        else
            EXCLOG(e,"CDistributedFileDirectory::fileCompare");
        e->Release();
        ret = DFS_COMPARE_RESULT_FAILURE;
    }
    return ret;
}

bool CDistributedFileDirectory::filePhysicalVerify(const char *lfn, IUserDescriptor *user, bool includecrc, StringBuffer &errstr)
{
    bool differs = false;
    Owned<IDistributedFile> file = lookup(lfn, user, false, false, false, NULL, defaultTimeout);
    if (!file)
    {
        errstr.appendf("Could not find file: %s",lfn);
        return false;
    }
    try
    {
        unsigned np = file->numParts();
        class casyncfor: public CAsyncFor
        {
            CriticalSection crit;
            IDistributedFile *file;
            const char *lfn;
            StringBuffer &errstr;
            bool includecrc;
            bool &differs;
            unsigned defaultTimeout;
        public:
            casyncfor(const char *_lfn,IDistributedFile *_file,StringBuffer &_errstr, bool _includecrc,
                        bool &_differs, unsigned _defaultTimeout)
                : errstr(_errstr), differs(_differs)
            {
                lfn = _lfn;
                file = _file;
                includecrc = _includecrc;
                defaultTimeout = _defaultTimeout;

            }
            void Do(unsigned p)
            {
                CriticalBlock block (crit);
                StringBuffer msg;
                Owned<IDistributedFilePart> part = file->getPart(p);    
                CDateTime dt1; // logical
                CDateTime dt2; // physical
                RemoteFilename rfn;
                bool ok;
                bool nological = !part->getModifiedTime(false,false,dt1);
                {
                    CriticalUnblock unblock(crit);
                    ok = part->getModifiedTime(true,true,dt2);
                }
                if (!ok) {
                    if (errstr.length()==0) {
                        errstr.append("Could not find part file: ");
                        part->getFilename(rfn);
                        rfn.getPath(errstr);
                    }
                    differs = true;
                }
                if (!differs&&!includecrc) {
                    if (nological) {
                        StringBuffer str;
                        // TODO: Create DistributedFilePropertyLock for parts
                        part->lockProperties(defaultTimeout);
                        part->queryAttributes().setProp("@modified",dt2.getString(str).str());
                        part->unlockProperties();
                    }
                    else  {
                        if (dt1.compare(dt2,false)!=0) {
                            if (errstr.length()==0) {
                                errstr.append("Modified time differs for: ");
                                part->getFilename(rfn);
                                rfn.getPath(errstr);
                            }
                            differs = true;
                        }
                    }
                }
                if (!differs) {
                    offset_t sz1;
                    offset_t sz2;
                    {
                        CriticalUnblock unblock(crit);
                        sz1 = part->getFileSize(false,false);
                        sz2 = part->getFileSize(true,true);
                    }
                    if (sz1!=sz2) {
                        if (sz1==(offset_t)-1) {
                            // TODO: Create DistributedFilePropertyLock for parts
                            part->lockProperties(defaultTimeout);
                            part->queryAttributes().setPropInt64("@size",sz2);
                            part->unlockProperties();
                        }
                        else if (sz2!=(offset_t)-1) {
                            if (errstr.length()==0) {
                                errstr.append("File size differs for: ");
                                part->getFilename(rfn);
                                rfn.getPath(errstr);
                            }
                            differs = true;
                        }
                    }
                }
                if (!differs&&includecrc) {
                    unsigned crc1;
                    unsigned crc2;
                    {
                        CriticalUnblock unblock(crit);
                        crc2 = part->getPhysicalCrc();
                    }
                    if (!part->getCrc(crc1)) {
                        // TODO: Create DistributedFilePropertyLock for parts
                        part->lockProperties(defaultTimeout);
                        part->queryAttributes().setPropInt64("@fileCrc",(unsigned)crc2);
                        part->unlockProperties();
                    }
                    else if (crc1!=crc2) {
                        if (errstr.length()==0) {
                            errstr.append("File CRC differs for: ");
                            part->getFilename(rfn);
                            rfn.getPath(errstr);
                        }
                        differs = true;
                    }
                }
            }
        } afor(lfn,file,errstr,includecrc,differs,defaultTimeout);
        afor.For(np,10,false,false);
    }
    catch (IException *e) {
        if (errstr.length()==0) 
            e->errorMessage(errstr);
        else
            EXCLOG(e,"CDistributedFileDirectory::fileCompare");
        e->Release();
        differs = true;
    }
    return !differs;
}

typedef MapStringTo<bool> SubfileSet;
class CFilterAttrIterator: public CInterface, implements IDFAttributesIterator
{
    Owned<IDFAttributesIterator> iter;
    Linked<IUserDescriptor> user;
    SubfileSet sfset;
    bool includesub;
public:
    IMPLEMENT_IINTERFACE;
    CFilterAttrIterator(IDFAttributesIterator *_iter,IUserDescriptor* _user,bool _includesub,unsigned timeoutms)
        : iter(_iter), user(_user)
    {
        includesub = _includesub;
        CDfsLogicalFileName lfn;
        StringBuffer query;
        Owned<IDFScopeIterator> siter = queryDistributedFileDirectory().getScopeIterator(user,NULL,true,false);
        ForEach(*siter) {
            lfn.set(siter->query(),"X");
            lfn.makeScopeQuery(query.clear());
            Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),0, timeoutms);
            if (conn) {
                IPropertyTree *t = conn->queryRoot();
                Owned<IPropertyTreeIterator> iter = t->getElements("SuperFile/SubFile");
                ForEach(*iter) {
                    const char *name =  iter->query().queryProp("@name");
                    if (!sfset.getValue(name)) 
                        sfset.setValue(name, true);
                }
            }
        }
    }
    inline bool match()
    {
        const char *name = iter->query().queryProp("@name");
        return ((sfset.getValue(name)!=NULL)==includesub);
    }
    bool first()
    {
        if (!iter->first())
            return false;
        while (!match())
            if (!iter->next())
                return false;
        return true;
    }
    bool next()
    {
        do {
            if (!iter->next())
                return false;
        } while (!match());
        return true;
    }
    bool isValid() { return iter->isValid(); }
    IPropertyTree  & query() { return iter->query(); }
};

IDFAttributesIterator *createSubFileFilter(IDFAttributesIterator *_iter,IUserDescriptor* _user, bool includesub, unsigned timeoutms)
{
    return new CFilterAttrIterator(_iter,_user,includesub,timeoutms);
}

bool decodeChildGroupName(const char *gname,StringBuffer &parentname, StringBuffer &range)
{
    if (!gname||!*gname)
        return false;
    size32_t l = strlen(gname);
    if (gname[l-1]!=']') 
        return false;
    const char *ss = strchr(gname,'[');
    if (!ss||(ss==gname)) 
        return false;
    range.append(l-(ss-gname)-2,ss+1);
    range.trim();
    if (!range.length())
        return false;
    parentname.append(ss-gname,gname);
    return true;
}

class CLightWeightSuperFileConn: public CInterface, implements ISimpleSuperFileEnquiry
{
    CFileLock lock;
    bool readonly;
    IArrayOf<IRemoteConnection> children;
    unsigned defaultTimeout;
    Owned<IUserDescriptor> udesc;

    static StringBuffer &getSubPath(StringBuffer &path,unsigned idx)
    {
        return path.append("SubFile[@num=\"").append(idx+1).append("\"]");
    }

    void migrateProp(const char *name, unsigned num,IPropertyTree *from,IPropertyTree *to,IPropertyTree *newt, bool allowunchanged)
    {
        StringBuffer aname("Attr/");
        aname.append(name);
        StringBuffer s;
        StringBuffer o;
        if (from->getProp(aname.str(),s)) 
            if ((num==1)||(allowunchanged&&to->getProp(aname.str(),o)&&(strcmp(s.str(),o.str())==0)))
                newt->setProp(name,s.str());
    }

    void migrateAttr(IPropertyTree *from,IPropertyTree *to)
    {
        // this tries hard to set what it knows but avoids sibling traversal
        if (!to)
            return;
        const char *desc = to->queryProp("Attr/@description");
        IPropertyTree* newt = getEmptyAttr();
        if (desc)
            newt->setProp("@description",desc);
        if (from) {
            unsigned num=to->getPropInt("@numsubfiles");
            migrateProp("@size",num,from,to,newt,false);
            migrateProp("@checkSum",num,from,to,newt,true);
            migrateProp("@formatCrc",num,from,to,newt,true);
            migrateProp("@recordSize",num,from,to,newt,true);
            MemoryBuffer mb;
            MemoryBuffer mbo;
            const char *aname = "Attr/_record_layout";
            if (from->getPropBin(aname,mb)) 
                if ((num==1)||(to->getPropBin(aname,mbo)&&
                              (mb.length()==mbo.length())&&
                              (memcmp(mb.bufferBase(),mbo.bufferBase(),mb.length())==0)))
                    newt->setPropBin("_record_layout", mb.length(), mb.bufferBase());
        }
        to->setPropTree("Attr",newt);
    }

    void migrateSuperOwnersAttr(IPropertyTree *from)
    {
        if (!from)
            return;
        Owned<IPropertyTreeIterator> iter = from->getElements("SuperOwner");
        StringBuffer pname;
        StringBuffer query;
        ForEach(*iter) {
            if (iter->query().getProp("@name",pname.clear())) {
                CDfsLogicalFileName lfn;
                lfn.set(pname.str()); 
                lfn.makeFullnameQuery(query.clear(),DXB_SuperFile,true);
                Owned<IRemoteConnection> conn;
                try {
                    conn.setown(querySDS().connect(query.str(),myProcessSession(),RTM_LOCK_WRITE,1000*60*5));
                }
                catch (ISDSException *e) {
                    if (SDSExcpt_LockTimeout != e->errorCode()) 
                        throw;
                    e->Release();
                    WARNLOG("migrateSuperOwnersAttr: Could not lock parent %s",query.str());
                    conn.setown(querySDS().connect(query.str(),myProcessSession(),0,defaultTimeout));
                }
                if (conn) {
                    migrateAttr(from,conn->queryRoot());
                    migrateSuperOwnersAttr(conn->queryRoot());
                }
                else
                    WARNLOG("migrateSuperOwnersAttr could not connect to parent superfile %s",lfn.get());
            }
        }
    }
    
public:

    IMPLEMENT_IINTERFACE;
    
    CLightWeightSuperFileConn(unsigned _defaultTimeout, IUserDescriptor *_udesc)
    {
        defaultTimeout = _defaultTimeout;
        readonly = false;
        udesc.set(_udesc);
    }

    bool connect(CDistributedFileDirectory *parent,const char *title, const char *name, bool _readonly, bool *autocreate, unsigned timeout)
    {
        if (autocreate)
            *autocreate = false;
        readonly = _readonly;
        disconnect(false);
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(name))
            throw MakeStringException(-1,"%s: Invalid superfile name '%s'",title,name);
        if (lfn.isMulti()||lfn.isExternal()||lfn.isForeign()) 
            return false;
        unsigned mode = RTM_SUB | (readonly ? RTM_LOCK_READ : RTM_LOCK_WRITE);
        if (!lock.init(lfn, DXB_SuperFile, mode, timeout, title))
        {
            if (!autocreate)        // NB not !*autocreate here !
                return false;
            IPropertyTree *root = createPTree();
            root->setPropInt("@interleaved",2); 
            root->setPropInt("@numsubfiles",0); 
            root->setPropTree("Attr",getEmptyAttr());   
            parent->addEntry(lfn,root,true,false);
            mode = RTM_SUB | RTM_LOCK_WRITE;
            if (!lock.init(lfn, DXB_SuperFile, mode, timeout, title))
                throw MakeStringException(-1,"%s: Cannot create superfile '%s'",title,name);
            if (autocreate)
                *autocreate = true;
        }
        StringBuffer reason;
        if (!readonly&&checkProtectAttr(name,lock.queryRoot(),reason)) 
            throw MakeStringException(-1,"CDistributedSuperFile::%s %s",title,reason.str());
        return true;
    }

    void disconnect(bool commit)
    {
        if (lock.queryConnection()&&!readonly) {
            if (commit) {
                migrateSuperOwnersAttr(lock.queryRoot());
                CDateTime dt;
                dt.setNow();
                StringBuffer s;
                lock.queryRoot()->setProp("@modified",dt.getString(s).str());
            }
            else {
                ForEachItemIn(i,children)
                    children.item(i).rollback();
                lock.queryConnection()->rollback();
            }
        }
        lock.clear();
        children.kill();
    }

    unsigned numSubFiles() const
    {
        return (unsigned)lock.queryRoot()->getPropInt("@numsubfiles");
    }

    bool getSubFileName(unsigned num,StringBuffer &name) const
    {
        if ((unsigned)lock.queryRoot()->getPropInt("@numsubfiles")<=num)
            return false;
        StringBuffer xpath;
        getSubPath(xpath,num); 
        IPropertyTree *sub = lock.queryRoot()->queryPropTree(xpath.str());
        if (!sub)
            return false;
        name.append(sub->queryProp("@name"));
        return true;
    }

    unsigned findSubName(const char *subname) const
    {
        unsigned n = findSubFileOrd(subname);
        if (n!=NotFound)
            return n;
        StringBuffer lfn;
        normalizeLFN(subname,lfn);
        Owned<IPropertyTreeIterator> iter = lock.queryRoot()->getElements("SubFile");
        ForEach(*iter) {
            if (stricmp(iter->query().queryProp("@name"),lfn.str())==0) {
                unsigned ret=iter->query().getPropInt("@num");
                if (ret&&((unsigned)lock.queryRoot()->getPropInt("@numsubfiles")>=ret))
                    return ret-1;
            }
        }
        return NotFound;
    }

    unsigned getContents(StringArray &contents) const
    {   
        // slightly inefficient
        unsigned n = lock.queryRoot()->getPropInt("@numsubfiles");
        StringBuffer xpath;
        for (unsigned sni=0;sni<n;sni++) {
            getSubPath(xpath.clear(),sni); 
            IPropertyTree *sub = lock.queryRoot()->queryPropTree(xpath.str());
            if (!sub)
                break;
            contents.append(sub->queryProp("@name"));
        }
        return contents.ordinality();
    }
};

// Contention never expected for this function!
#define PROMOTE_CONN_TIMEOUT (60*1000) // how long to wait for a single superfile
#define PROMOTE_DELAY   (30*1000)   

// Check files don't share subfiles (MORE - make this part of swap files action?)
static int hasCommonSubChildren(IDistributedSuperFile *orig, IDistributedSuperFile *dest)
{
    unsigned origSubs = orig->numSubFiles();
    unsigned destSubs = dest->numSubFiles();
    if (origSubs == 0)
        return NotFound;
    for (unsigned j=0; j<origSubs; j++) {
        for (unsigned k=0; k<destSubs; k++) {
            if (strcmp(orig->querySubFile(j).queryLogicalName(), dest->querySubFile(k).queryLogicalName())==0)
                return j;
        }
    }
    return NotFound;
}

// MORE - use string arrays, rather than char* arrays or comma-separated strings
void CDistributedFileDirectory::promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user,unsigned timeout,StringArray &outunlinked)
{
    if (!numsf) 
        return;

    // Create a local transaction that will be destroyed
    Owned<IDistributedFileTransactionExt> transaction = new CDistributedFileTransaction(user);
    transaction->start();

    // Lookup all files (keep them in transaction's cache)
    bool created = false;
    unsigned files = numsf;
    for (unsigned i=0; i<numsf; i++) {
        Owned<IDistributedSuperFile> super = transaction->lookupSuperFile(sfnames[i]);
        if (!super.get()) {
            if (created && createonlyonesuperfile) {
                files = i;
                break;
            }
            Owned<IDistributedSuperFile> sfile = createSuperFile(sfnames[i],user,false,false,transaction);
            created = true;
        }
    }

    // If last file had sub-files, clean and fill outlinked
    Owned<IDistributedSuperFile> last = transaction->lookupSuperFile(sfnames[files-1]);
    assertex(last.get());
    unsigned lastSubs = last->numSubFiles();
    if (files == numsf && lastSubs > 0) {
        for (unsigned i=0; i<lastSubs; i++) {
            outunlinked.append(last->querySubFile(i).queryLogicalName());
        }
        last->removeSubFile(NULL,false,false,transaction);
    }
    last.clear();

    // Move up, starting from last
    for (unsigned i=files-1; i; i--) {
        Owned<IDistributedSuperFile> orig = transaction->lookupSuperFile(sfnames[i-1]);
        Owned<IDistributedSuperFile> dest = transaction->lookupSuperFile(sfnames[i]);
        assertex(orig.get());
        assertex(dest.get());
        int common = hasCommonSubChildren(orig, dest);
        if (common != NotFound) {
            throw MakeStringException(-1,"promoteSuperFiles: superfiles %s and %s share same subfile %s",
                    orig->queryLogicalName(), dest->queryLogicalName(), orig->querySubFile(common).queryLogicalName());
        }
        orig->swapSuperFile(dest, transaction);
    }

    // Move new subs to first super, if any
    Owned<IDistributedSuperFile> first = transaction->lookupSuperFile(sfnames[0]);
    assertex(first.get());
    StringArray toadd;
    toadd.appendListUniq(addsubnames, ",");
    ForEachItemIn(i,toadd) {
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(toadd.item(i)))
            throw MakeStringException(-1,"promoteSuperFiles: invalid logical name to add: %s",toadd.item(i));
        first->addSubFile(toadd.item(i),false,NULL,false,transaction);
    }
    first.clear();

    transaction->commit();

    // MORE - once deletion of logic files are also in transaction we can move this up (and allow promote within transactions)
    if (delsub) {
        ForEachItemIn(j,outunlinked) 
            removeEntry(outunlinked.item(j),user,transaction,timeout);
    }
}


ISimpleSuperFileEnquiry * CDistributedFileDirectory::getSimpleSuperFileEnquiry(const char *logicalname,const char *title,IUserDescriptor *udesc,unsigned timeout)
{
    Owned<CLightWeightSuperFileConn> ret = new CLightWeightSuperFileConn(defaultTimeout,udesc);
    if (ret->connect(this,title,logicalname,true,NULL,timeout))
        return ret.getClear();
    return NULL;
}

bool CDistributedFileDirectory::getFileSuperOwners(const char *logicalname, StringArray &owners)
{
    CFileLock lock;
    CDfsLogicalFileName lfn;
    if (!lfn.setValidate(logicalname))
        throw MakeStringException(-1,"CDistributedFileDirectory::getFileSuperOwners: Invalid file name '%s'",logicalname);
    if (lfn.isMulti()||lfn.isExternal()||lfn.isForeign()) 
        return false;
    if (!lock.init(lfn, RTM_LOCK_READ, defaultTimeout, "CDistributedFileDirectory::getFileSuperOwners"))
        return false;
    CFileSuperOwnerLock superOwnerLock;
    verifyex(superOwnerLock.init(lfn, NULL, defaultTimeout, "CDistributedFileDirectory::getFileSuperOwners(SuperOwnerLock)"));
    Owned<IPropertyTreeIterator> iter = lock.queryRoot()->getElements("SuperOwner");
    StringBuffer pname;
    ForEach(*iter) {
        iter->query().getProp("@name",pname.clear());
        if (pname.length())
            owners.append(pname.str());
    }
    return true;
}

class CFileRelationship: public CInterface, implements IFileRelationship
{
    Linked<IPropertyTree> pt;
    const char *queryProp(const char *name)
    {
        if (pt.get()) {
            const char *ret = pt->queryProp(name);
            if (ret)
                return ret;
        }
        return "";
    }

public:
    IMPLEMENT_IINTERFACE;
    CFileRelationship(IPropertyTree *_pt)
        : pt(_pt)
    {
    }
    virtual const char *queryKind()  {  return queryProp("@kind"); }
    virtual const char *queryPrimaryFilename() { return queryProp("@primary"); }
    virtual const char *querySecondaryFilename() { return queryProp("@secondary"); }
    virtual const char *queryPrimaryFields()  { return queryProp("@primflds"); }
    virtual const char *querySecondaryFields()  { return queryProp("@secflds"); }
    virtual const char *queryCardinality()  { return queryProp("@cardinality"); }
    virtual bool isPayload()  { return pt->getPropBool("@payload"); }
    virtual const char *queryDescription()  { return queryProp("Description"); }
    virtual IPropertyTree *queryTree() { return pt.get(); }
};

class CFileRelationshipIterator: public CInterface, implements IFileRelationshipIterator
{
    unsigned num;
    unsigned idx;
    CMessageBuffer mb;
    Owned<CFileRelationship> r;
    Owned<IPropertyTree> pt;
    Linked<INode> foreigndali;
    unsigned defaultTimeout;

    bool setPT()
    {
        if (idx<num) {
            pt.setown(createPTree(mb));
            addForeignName(*pt,foreigndali,"@primary");
            addForeignName(*pt,foreigndali,"@secondary");
        }
        return pt.get()!=NULL;
    }

public:
    IMPLEMENT_IINTERFACE;
    CFileRelationshipIterator(unsigned timems)
    {
        num = 0;
        idx = 0;
        mb.append(num);
        defaultTimeout = timems;
    }

    void init(
        INode *_foreigndali,
        unsigned foreigndalitimeout,
        const char *primary,
        const char *secondary,
        const char *primflds,
        const char *secflds,
        const char *kind,
        const char *cardinality,
        const bool *payload )
    {
        foreigndali.set(_foreigndali);

        if (isLocalDali(foreigndali)) {
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,false,defaultTimeout);
            StringBuffer xpath;
            CDistributedFileDirectory::getFileRelationshipXPath(xpath,primary,secondary,primflds,secflds,kind,cardinality,payload);
            Owned<IPropertyTreeIterator> iter = connlock.conn?connlock.conn->getElements(xpath.str()):NULL;
            mb.clear();
            unsigned count = 0;
            mb.append(count);
            // save as sequence of branches
            if (iter) {
                ForEach(*iter.get()) {
                    iter->query().serialize(mb);
                    count++;
                }
                mb.writeDirect(0,sizeof(count),&count);
            }
        }
        else {
            byte payloadb = 255;
            if (payload)
                payloadb = *payload?1:0;
            mb.clear().append((int)MDFS_ITERATE_RELATIONSHIPS).append(primary).append(secondary).append(primflds).append(secflds).append(kind).append(cardinality).append(payloadb);
            foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
            checkDfsReplyException(mb);
            if (mb.length()<sizeof(unsigned))
                mb.clear().append((unsigned)0);
        }
    }

    void initall(const char *filename)
    {
        StringBuffer xpath;
        Owned<IPropertyTreeIterator> iter;
        mb.clear();
        unsigned count = 0;
        mb.append(count);
        {
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,false,defaultTimeout);
            CDistributedFileDirectory::getFileRelationshipXPath(xpath,filename,NULL,NULL,NULL,NULL,NULL,NULL);
            // save as sequence of branches
            iter.setown(connlock.conn?connlock.conn->getElements(xpath.str()):NULL);
            if (iter) {
                ForEach(*iter.get()) {
                    iter->query().serialize(mb);
                    count++;
                }
            }
        }
        { // Kludge - seems to be a bug in getElements without second conn lock
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,false,defaultTimeout);
            xpath.clear();
            CDistributedFileDirectory::getFileRelationshipXPath(xpath,NULL,filename,NULL,NULL,NULL,NULL,NULL);
            iter.clear();
            iter.setown(connlock.conn?connlock.conn->getElements(xpath.str()):NULL);
            if (iter) {
                ForEach(*iter.get()) {
                    IPropertyTree &it = iter->query();
                    const char *fn1 = it.queryProp("@primary");
                    if (!fn1||(strcmp(fn1,filename)!=0)) {              // see if already done
                        it.serialize(mb);
                        count++;
                    }
                }
            }
        }
        mb.writeDirect(0,sizeof(count),&count);
    }

    bool first()
    {
        r.clear();
        pt.clear();
        idx = 0;
        mb.reset().read(num);
        return setPT();
    }

    bool next()
    {
        r.clear();
        pt.clear();
        idx++;
        return setPT();
    }

    bool isValid()
    {
        return pt.get()!=NULL;
    }

    IFileRelationship & query()
    {
        if (!r) 
            r.setown(new CFileRelationship(pt));
        return *r;
    }

};

static bool isWild(const char *path,bool emptydefault=false)
{
    if (!path||!*path)
        return emptydefault;
    return ((strchr(path,'?')||strchr(path,'*')));
}

static void addRelationCondition(StringBuffer &xpath,const char *fld,const char *mask)
{
    if (!mask||!*mask||((*mask=='*')&&(!mask[1])))
        return;
    xpath.append('[').append(fld).append('=');
    if (isWild(mask))
        xpath.append('~');
    xpath.append('"').append(mask).append("\"]");
}

static void addRelationBoolCondition(StringBuffer &xpath,const char *fld,const bool *mask)
{
    if (!mask)
        return;
    xpath.append('[').append(fld).append("=\"");
    if (*mask)
        xpath.append("1\"]");
    else
        xpath.append("0\"]");
}

static const char *normLFN(const char *name,CDfsLogicalFileName &logicalname,const char *title)
{
    if (isWild(name,true))
        return name;
    if (!logicalname.setValidate(name))
        throw MakeStringException(-1,"%s: invalid logical file name '%s'",title,name);
    if (logicalname.isForeign()) { 
        SocketEndpoint ep;
        Owned<INode> fd = createINode(ep);
        if (logicalname.getEp(ep)&&isLocalDali(fd))  // see if pointing back at self
            logicalname.clearForeign();
    }
    return logicalname.get();
}


StringBuffer &CDistributedFileDirectory::getFileRelationshipXPath(
  StringBuffer &xpath,
  const char *primary,
  const char *secondary,
  const char *primflds,
  const char *secflds,
  const char *kind,
  const char *cardinality,
  const bool *payload
  )
{
    xpath.append("Relationship");
    CDfsLogicalFileName lfn;
    addRelationCondition(xpath,"@kind",kind);
    addRelationCondition(xpath,"@primary",normLFN(primary,lfn,"findFileRelationship(primary)"));
    addRelationCondition(xpath,"@secondary",normLFN(secondary,lfn,"findFileRelationship(secondary)"));
    addRelationCondition(xpath,"@primflds",primflds);
    addRelationCondition(xpath,"@secflds",secflds);
    addRelationCondition(xpath,"@cardinality",cardinality);
    addRelationBoolCondition(xpath,"@payload",payload);
    return xpath;
}

void CDistributedFileDirectory::doRemoveFileRelationship(
  IRemoteConnection *conn,
  const char *primary,
  const char *secondary,
  const char *primflds,
  const char *secflds,
  const char *kind
  )
{
    if (!conn)
        return;
    StringBuffer xpath;
    CDistributedFileDirectory::getFileRelationshipXPath(xpath,primary,secondary,primflds,secflds,kind,NULL,NULL);
    Owned<IPropertyTreeIterator> iter = conn->getElements(xpath.str());
    IArrayOf<IPropertyTree> toremove;
    ForEach(*iter) {
        IPropertyTree &t = iter->query();
        toremove.append(*LINK(&t));
    }
    ForEachItemIn(i, toremove) {
        conn->queryRoot()->removeTree(&toremove.item(i));
    }
}

void CDistributedFileDirectory::addFileRelationship(
  const char *primary,
  const char *secondary,
  const char *primflds,
  const char *secflds,
  const char *kind,
  const char *cardinality,
  bool payload,
  IUserDescriptor *user,
  const char *description=NULL
  )
{
    if (!kind||!*kind)
        kind = S_LINK_RELATIONSHIP_KIND;
    Owned<IPropertyTree> pt = createPTree("Relationship");
    if (isWild(primary,true)||isWild(secondary,true)||isWild(primflds,false)||isWild(secflds,false)||isWild(cardinality,false))
        throw MakeStringException(-1,"Wildcard not allowed in addFileRelation");
    CDfsLogicalFileName pfn;
    if (!pfn.setValidate(primary))  
        throw MakeStringException(-1,"addFileRelationship invalid primary name '%s'",primary);
    if (pfn.isExternal()||pfn.isForeign()||pfn.isQuery())
        throw MakeStringException(-1,"addFileRelationship primary %s not allowed",pfn.get());
    primary = pfn.get();
    if (!exists(primary,user))
        throw MakeStringException(-1,"addFileRelationship primary %s does not exist",primary);
    CDfsLogicalFileName sfn;
    if (!sfn.setValidate(secondary))  
        throw MakeStringException(-1,"addFileRelationship invalid secondary name '%s'",secondary);
    if (sfn.isExternal()||sfn.isForeign()||sfn.isQuery())
        throw MakeStringException(-1,"addFileRelationship secondary %s not allowed",sfn.get());
    secondary = sfn.get();
    if (!exists(secondary,user))
        throw MakeStringException(-1,"addFileRelationship secondary %s does not exist",secondary);
    if (cardinality&&*cardinality&&!strchr(cardinality,':'))
        throw MakeStringException(-1,"addFileRelationship cardinality %s invalid",cardinality);

    pt->setProp("@kind",kind);
    pt->setProp("@primary",primary);
    pt->setProp("@secondary",secondary);
    pt->setProp("@cardinality",cardinality);
    pt->setProp("@primflds",primflds);
    pt->setProp("@secflds",secflds);
    pt->setPropBool("@payload",payload);
    if (description&&*description)
        pt->setProp("Description",description);

    StringBuffer xpath(querySdsFilesRoot());


    for (unsigned i=0;i<2;i++) {
        CConnectLock connlock("addFileRelation",querySdsRelationshipsRoot(),true,false,false,defaultTimeout);
        if (!connlock.conn) {
            CConnectLock connlock2("addFileRelation.2",querySdsFilesRoot(),true,false,false,defaultTimeout);
            if (!connlock2.conn)
                return;
            Owned<IPropertyTree> ptr = createPTree("Relationships");
            connlock2.conn->queryRoot()->addPropTree("Relationships",ptr.getClear());
            continue;
        }   
        StringBuffer query;
        doRemoveFileRelationship(connlock.conn,primary,secondary,primflds,secflds,kind);
        connlock.conn->queryRoot()->addPropTree("Relationship",pt.getClear());
        break;
    }
}

void CDistributedFileDirectory::removeFileRelationships(
  const char *primary,
  const char *secondary,
  const char *primflds,
  const char *secflds,
  const char *kind
  )
{
    if ((!primary||!*primary||(strcmp(primary,"*")==0))&&
        (!secondary||!*secondary||(strcmp(secondary,"*")==0)))
        throw MakeStringException(-1,"removeFileRelationships primary and secondary cannot both be wild");

    CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,false,defaultTimeout);
    doRemoveFileRelationship(connlock.conn,primary,secondary,primflds,secflds,kind);
}

IFileRelationshipIterator *CDistributedFileDirectory::lookupFileRelationships(
    const char *primary,
    const char *secondary,
    const char *primflds,
    const char *secflds,
    const char *kind,
    const char *cardinality,
    const bool *payload,
    const char *foreigndali,
    unsigned foreigndalitimeout
)
{
    Owned<INode> foreign;
    if (foreigndali&&*foreigndali) {
        SocketEndpoint ep(foreigndali);
        if (ep.isNull())
            throw MakeStringException(-1,"lookupFileRelationships::Cannot resolve foreign dali %s",foreigndali);
        foreign.setown(createINode(ep));
    }
    Owned<CFileRelationshipIterator> ret = new CFileRelationshipIterator(defaultTimeout);
    ret->init(foreign,foreigndalitimeout,primary,secondary,primflds,secflds,kind,cardinality,payload);
    return ret.getClear();
}

void CDistributedFileDirectory::removeAllFileRelationships(const char *filename)
{
    if (!filename||!*filename||(strcmp(filename,"*")==0))
        throw MakeStringException(-1,"removeAllFileRelationships filename cannot be wild");
    {
        CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,false,defaultTimeout);
        doRemoveFileRelationship(connlock.conn,filename,NULL,NULL,NULL,NULL);
    }
    {   // kludge bug in getElements if connection used twice
        CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,false,defaultTimeout);
        doRemoveFileRelationship(connlock.conn,NULL,filename,NULL,NULL,NULL);
    }
}

IFileRelationshipIterator *CDistributedFileDirectory::lookupAllFileRelationships(
    const char *filename)
{
    if (isWild(filename,true))
        throw MakeStringException(-1,"Wildcard filename not allowed in lookupAllFileRelationships");
    CDfsLogicalFileName lfn;
    normLFN(filename,lfn,"lookupAllFileRelationships");
    Owned<CFileRelationshipIterator> ret = new CFileRelationshipIterator(defaultTimeout);
    ret->initall(lfn.get());
    return ret.getClear();
}

void CDistributedFileDirectory::renameFileRelationships(const char *oldname,const char *newname,IFileRelationshipIterator *reliter,IUserDescriptor*user)
{
    CDfsLogicalFileName oldlfn;
    normLFN(oldname,oldlfn,"renameFileRelationships(old name)");
    CDfsLogicalFileName newlfn;
    normLFN(newname,newlfn,"renameFileRelationships(new name)");
    ForEach(*reliter) {
        try {
            IFileRelationship &r = reliter->query();
            bool adj = false;
            const char *pf = r.queryPrimaryFilename();
            if (!pf)
                continue;
            if (strcmp(pf,oldlfn.get())==0) {
                adj = true;
                pf = newlfn.get();
            }
            const char *sf = r.querySecondaryFilename();
            if (!sf)
                continue;
            if (strcmp(sf,oldlfn.get())==0) {
                adj = true;
                sf = newlfn.get();
            }
            if (adj)
                addFileRelationship(pf,sf,r.queryPrimaryFields(),r.querySecondaryFields(),r.queryKind(),r.queryCardinality(),r.isPayload(),user,r.queryDescription());
        }
        catch (IException *e)
        {
            EXCLOG(e,"renameFileRelationships");
            e->Release();
        }
    }
}


// JCSMORE what was this for, not called by anything afaics
bool CDistributedFileDirectory::publishMetaFileXML(const CDfsLogicalFileName &logicalname,IUserDescriptor *user)
{
    if (logicalname.isExternal()||logicalname.isForeign()||logicalname.isQuery()) 
        return false;
    Owned<IPropertyTree> file = getFileTree(logicalname.get(),user,NULL,FOREIGN_DALI_TIMEOUT,true);
    if (!file.get())
        return false;
    if (strcmp(file->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0)
        return false;

    unsigned max = file->getPropInt("@numparts");
    SocketEndpointArray ips;
    StringBuffer xpath;
    StringBuffer ipstr;
    for (unsigned i=0;i<max;i++) {  // probably could be done better
        xpath.clear().append("Part[@num=\"").append(i+1).append("\"]");
        Owned<IPropertyTree> child = file->getPropTree(xpath.str());
        SocketEndpoint ep;
        if (child.get()&&child->getProp("@node",ipstr.clear()))
            ep.ipset(ipstr.str());
        ips.append(ep);
    }


    Owned<IException> exc;
    CriticalSection errcrit;
    class casyncfor: public CAsyncFor
    {
        IPropertyTree* file;
        CriticalSection &errcrit;
        Owned<IException> &exc;
        SocketEndpointArray &ips;
    public:
        casyncfor(IPropertyTree* _file,SocketEndpointArray &_ips,Owned<IException> &_exc,CriticalSection &_errcrit)
            : ips(_ips), exc(_exc), errcrit(_errcrit)
        {
            file = _file;
        }
        void Do(unsigned i)
        {
            UnsignedArray parts;
            const SocketEndpoint &ep = ips.item(i);
            if (ep.isNull())
                return;
            ForEachItemIn(j,ips) {
                if (j==i)
                    parts.append(i);
                else if (ep.ipequals(ips.item(j))) {
                    if (j<i)
                        return; // already done
                    parts.append(j);
                }
            }
            try {
                StringBuffer path;
                StringBuffer mask;
                if (file->getProp("@directory",path)&&file->getProp("@partmask",mask)) {
                    addPathSepChar(path).append(mask);
                    StringBuffer outpath;
                    StringBuffer tail("META__");
                    splitFilename(path.str(), &outpath, &outpath, &tail, NULL);
                    outpath.append(tail).append(".xml");
                    Owned<IPropertyTree> pt = createPTreeFromIPT(file);
                    filterParts(pt,parts);
                    StringBuffer str;
                    toXML(pt, str);
                    RemoteFilename rfn;
                    rfn.setPath(ep,outpath.str());
                    Owned<IFile> out = createIFile(rfn);
                    Owned<IFileIO> outio = out->open(IFOcreate);
                    if (outio)
                        outio->write(0,str.length(),str.str());
                    
                }
            }
            catch(IException *e)
            {
                CriticalBlock block(errcrit);
                EXCLOG(e,"publishMetaFileXML");
                if (!exc.get())
                    exc.setown(e);
                else
                    e->Release();
            }

        }
    } afor(file,ips,exc,errcrit);
    afor.For(max,20);
    if (exc)
        throw exc.getClear();
    return true;
    

}

IFileDescriptor *CDistributedFileDirectory::createDescriptorFromMetaFile(const CDfsLogicalFileName &logicalname,IUserDescriptor *user)
{
    return NULL; // TBD
}

// Overwrite protection

bool CDistributedFileDirectory::isProtectedFile(const CDfsLogicalFileName &logicalName, unsigned timeout)
{
    CFileAttrLock attrLock;
    if (!attrLock.init(logicalName, RTM_LOCK_READ, NULL, timeout?timeout:INFINITE, "CDistributedFileDirectory::isProtectedFile"))
        return false; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = attrLock.queryRoot()->getElements("Protect");
    bool prot = false;
    ForEach(*wpiter) {
        IPropertyTree &t = wpiter->query();
        if (t.getPropInt("@count")) {
            prot = true;
            break;
        }
    }
    // timeout retry TBD
    return prot; 
}

unsigned CDistributedFileDirectory::queryProtectedCount(const CDfsLogicalFileName &logicalName, const char *owner)
{
    CFileAttrLock attrLock;
    if (!attrLock.init(logicalName, RTM_LOCK_READ, NULL, defaultTimeout, "CDistributedFileDirectory::queryProtectedCount"))
        return 0; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = attrLock.queryRoot()->getElements("Protect");
    unsigned count = 0;
    ForEach(*wpiter) {
        IPropertyTree &t = wpiter->query();
        const char *name = t.queryProp("@name");
        if (!owner||!*owner||(name&&(strcmp(owner,name)==0))) 
            count += t.getPropInt("@count");
    }
    return count;
}

bool CDistributedFileDirectory::getProtectedInfo(const CDfsLogicalFileName &logicalName, StringArray &names, UnsignedArray &counts)
{
    CFileAttrLock attrLock;
    if (!attrLock.init(logicalName, RTM_LOCK_READ, NULL, defaultTimeout, "CDistributedFileDirectory::getProtectedInfo"))
        return false; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = attrLock.queryRoot()->getElements("Protect");
    bool prot = false;
    ForEach(*wpiter) {
        IPropertyTree &t = wpiter->query();
        const char *name = t.queryProp("@name");
        names.append(name?name:"<Unknown>");
        unsigned c = t.getPropInt("@count");
        if (c)
            prot = true;
        counts.append(c);
    }
    return prot;
}

IDFProtectedIterator *CDistributedFileDirectory::lookupProtectedFiles(const char *owner,bool notsuper,bool superonly)
{
    return new CDFProtectedIterator(owner,notsuper,superonly,defaultTimeout);
}

const char* DFUQResultFieldNames[] = { "@name", "@description", "@group", "@kind", "@modified", "@job", "@owner",
    "@DFUSFrecordCount", "@recordCount", "@recordSize", "@DFUSFsize", "@size", "@workunit", "@DFUSFcluster", "@numsubfiles",
    "@accessed", "@numparts", "@compressedSize", "@directory", "@partmask", "@superowners", "@persistent" };

extern da_decl const char* getDFUQResultFieldName(DFUQResultField feild)
{
    return DFUQResultFieldNames[feild];
}

IPropertyTreeIterator *deserializeFileAttrIterator(MemoryBuffer& mb, DFUQResultField* localFilters, const char* localFilterBuf)
{
    class CFileAttrIterator: public CInterface, implements IPropertyTreeIterator
    {
        Owned<IPropertyTree> cur;
        StringArray fileNodeGroups;

        void setFileNodeGroup(IPropertyTree *attr, const char* group, StringArray& nodeGroupFilter)
        {
            if (!group || !*group)
                return;

            //The group may contain multiple clusters and some of them may match with the clusterFilter.
            if (nodeGroupFilter.length() == 1)
                attr->setProp(getDFUQResultFieldName(DFUQRFnodegroup), nodeGroupFilter.item(0));//Filter has been handled on server side.
            else
            {
                StringArray groups;
                groups.appendListUniq(group, ",");
                ForEachItemIn(i,groups)
                {
                    //Add a group if no group filter or the group matches with group filter
                    const char* node = groups.item(i);
                    if (node && *node && ((!nodeGroupFilter.length()) || (nodeGroupFilter.find(node) != NotFound)))
                        fileNodeGroups.append(node);
                }
                if (fileNodeGroups.length())
                {
                    //if this file exists on multiple groups, set one of the groups as the "@DFUSFnodegroup" prop for
                    //this attr, leaving the rest inside the fileNodeGroups array. Those groups will be used by the
                    //duplicateFileAttrOnOtherNodeGroup() to duplicate this file attr on other groups.
                    attr->setProp(getDFUQResultFieldName(DFUQRFnodegroup), fileNodeGroups.item(fileNodeGroups.length() -1));
                    fileNodeGroups.pop();
                }
            }
        }

        void setRecordCount(IPropertyTree* file)
        {
            __int64 recordCount = 0;
            if (file->hasProp(getDFUQResultFieldName(DFUQRForigrecordcount)))
                recordCount = file->getPropInt64(getDFUQResultFieldName(DFUQRForigrecordcount));
            else
            {
                __int64 recordSize=file->getPropInt64(getDFUQResultFieldName(DFUQRFrecordsize),0);
                if(recordSize)
                {
                    __int64 size=file->getPropInt64(getDFUQResultFieldName(DFUQRForigsize),-1);
                    recordCount = size/recordSize;
                }
            }
            file->setPropInt64(getDFUQResultFieldName(DFUQRFrecordcount),recordCount);
            return;
        }

        IPropertyTree *deserializeFileAttr(MemoryBuffer &mb, StringArray& nodeGroupFilter)
        {
            IPropertyTree *attr = getEmptyAttr();
            StringAttr val;
            unsigned n;
            mb.read(val);
            attr->setProp(getDFUQResultFieldName(DFUQRFname),val.get());
            mb.read(val);
            if (strieq(val,"!SF"))
            {
                mb.read(n);
                attr->setPropInt(getDFUQResultFieldName(DFUQRFnumsubfiles),n);
                mb.read(val);   // not used currently
            }
            else
            {
                attr->setProp(getDFUQResultFieldName(DFUQRFdirectory),val.get());
                mb.read(n);
                attr->setPropInt(getDFUQResultFieldName(DFUQRFnumparts),n);
                mb.read(val);
                attr->setProp(getDFUQResultFieldName(DFUQRFpartmask),val.get());
            }
            mb.read(val);
            attr->setProp(getDFUQResultFieldName(DFUQRFtimemodified),val.get());
            unsigned count;
            mb.read(count);
            StringAttr at;
            while (count--)
            {
                mb.read(at);
                mb.read(val);
                attr->setProp(at.get(),val.get());
                if (strieq(at.get(), getDFUQResultFieldName(DFUQRFnodegroups)))
                    setFileNodeGroup(attr, val.get(), nodeGroupFilter);
            }
            attr->setPropInt64(getDFUQResultFieldName(DFUQRFsize), attr->getPropInt64(getDFUQResultFieldName(DFUQRForigsize), -1));//Sort the files with empty size to front
            setRecordCount(attr);
            return attr;
        }

        IPropertyTree *duplicateFileAttrOnOtherNodeGroup(IPropertyTree *previousAttr)
        {
            IPropertyTree *attr = getEmptyAttr();
            Owned<IAttributeIterator> ai = previousAttr->getAttributes();
            ForEach(*ai)
                attr->setProp(ai->queryName(),ai->queryValue());
            attr->setProp(getDFUQResultFieldName(DFUQRFnodegroup), fileNodeGroups.item(fileNodeGroups.length()-1));
            fileNodeGroups.pop();
            return attr;
        }

    public:
        IMPLEMENT_IINTERFACE;
        MemoryBuffer mb;
        unsigned numfiles;
        bool allMatchingFilesReceived;
        StringArray nodeGroupFilter;

        bool first()
        {
            mb.reset();
            mb.read(numfiles);
            mb.read(allMatchingFilesReceived);

            return next();
        }

        bool next()
        {
            if (fileNodeGroups.length())
            {
                IPropertyTree *attr = duplicateFileAttrOnOtherNodeGroup(cur);
                cur.clear();
                cur.setown(attr);
                return true;
            }
            cur.clear();
            if (mb.getPos()>=mb.length())
                return false;
            cur.setown(deserializeFileAttr(mb, nodeGroupFilter));
            return true;
        }

        bool isValid()
        {
            return cur.get()!=NULL;
        }

        IPropertyTree  & query()
        {
            return *cur;
        }

        void setLocalFilters(DFUQResultField* localFilters, const char* localFilterBuf)
        {
            if (!localFilters || !localFilterBuf || !*localFilterBuf)
                return;

            const char *fv = localFilterBuf;
            for (unsigned i=0;localFilters[i]!=DFUQRFterm;i++)
            {
                int fmt = localFilters[i];
                int subfmt = (fmt&0xff);
                if ((subfmt==DFUQRFnodegroup) && fv && *fv)
                    nodeGroupFilter.appendListUniq(fv, ",");
                //Add more if needed
                fv = fv + strlen(fv)+1;
            }
        }

    } *fai = new CFileAttrIterator;
    mb.swapWith(fai->mb);
    fai->setLocalFilters(localFilters, localFilterBuf);
    return fai;
}

IPropertyTreeIterator *CDistributedFileDirectory::getDFAttributesTreeIterator(const char* filters, DFUQResultField* localFilters,
    const char* localFilterBuf, IUserDescriptor* user, bool& allMatchingFilesReceived, INode* foreigndali, unsigned foreigndalitimeout)
{
    CMessageBuffer mb;
    mb.append((int)MDFS_ITERATE_FILTEREDFILES).append(filters).append(true);
    if (user)
        user->serialize(mb);

    if (foreigndali)
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);

    unsigned numfiles;
    mb.read(numfiles);
    mb.read(allMatchingFilesReceived);
    return deserializeFileAttrIterator(mb, localFilters, localFilterBuf);
}

IDFAttributesIterator* CDistributedFileDirectory::getLogicalFilesSorted(
    IUserDescriptor* udesc,
    DFUQResultField *sortOrder, // list of fields to sort by (terminated by DFUSFterm)
    const void *filters,  // (appended) string values for filters used by dali server
    DFUQResultField *localFilters, //used for filtering query result received from dali server.
    const void *localFilterBuf,
    unsigned startOffset,
    unsigned maxNum,
    __int64 *cacheHint,
    unsigned *total,
    bool *allMatchingFiles)
{
    class CDFUPager : public CSimpleInterface, implements IElementsPager
    {
        IUserDescriptor* udesc;
        //StringAttr clusterFilter;
        StringAttr filters;
        DFUQResultField *localFilters;
        StringAttr localFilterBuf;
        StringAttr sortOrder;
        bool allMatchingFilesReceived;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDFUPager(IUserDescriptor* _udesc, const char*_filters, DFUQResultField*_localFilters, const char*_localFilterBuf,
            const char*_sortOrder) : udesc(_udesc), filters(_filters), localFilters(_localFilters), localFilterBuf(_localFilterBuf),
            sortOrder(_sortOrder)
        {
            allMatchingFilesReceived = true;
        }
        virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
        {
            Owned<IPropertyTreeIterator> fi = queryDistributedFileDirectory().getDFAttributesTreeIterator(filters.get(),
                localFilters, localFilterBuf.get(), udesc, allMatchingFilesReceived);
            StringArray unknownAttributes;
            sortElements(fi, sortOrder.get(), NULL, NULL, unknownAttributes, elements);
            return NULL;
        }
        virtual bool allMatchingElementsReceived() { return allMatchingFilesReceived; }
    };

    StringBuffer so;
    if (sortOrder)
    {
        for (unsigned i=0;sortOrder[i]!=DFUQRFterm;i++)
        {
            if (so.length())
                so.append(',');
            int fmt = sortOrder[i];
            if (fmt&DFUQRFreverse)
                so.append('-');
            if (fmt&DFUQRFnocase)
                so.append('?');
            if (fmt&DFUQRFnumeric)
                so.append('#');
            so.append(getDFUQResultFieldName((DFUQResultField) (fmt&0xff)));
        }
    }
    IArrayOf<IPropertyTree> results;
    Owned<IElementsPager> elementsPager = new CDFUPager(udesc, (const char*) filters, localFilters, (const char*) localFilterBuf,
        so.length()?so.str():NULL );
    Owned<IRemoteConnection> conn = getElementsPaged(elementsPager,startOffset,maxNum,NULL,"",cacheHint,results,total,allMatchingFiles,false);
    return new CDFAttributeIterator(results);
}
#ifdef _USE_CPPUNIT
/*
 * This method removes files only logically. removeEntry() used to do that, but the only
 * external use for logical-files only is this test-suite, so I'd rather hack the test
 * suite than expose the behaviour to more viewers.
 */
extern da_decl void removeLogical(const char *fname, IUserDescriptor *user) {
    if (queryDistributedFileDirectory().exists(fname, user)) {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(fname, user, true);
        CDistributedFile *f = QUERYINTERFACE(file.get(),CDistributedFile);
        assert(f);
        f->detachLogical();
    }
}
#endif // _USE_CPPUNIT
