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
#include "dadfs.hpp"
#include "eclhelper.hpp"
#include "seclib.hpp"
#include "dameta.hpp"

#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <time.h>

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
#define MAX_PHYSICAL_DELETE_THREADS 1000

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
    MDFS_ITERATE_FILTEREDFILES2,
    MDFS_GET_FILE_TREE2,
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

inline StringBuffer &appendEnsurePathSepChar(StringBuffer &dest, StringBuffer &newPart, char psc)
{
    addPathSepChar(dest, psc);
    if (newPart.length() > 0)
    {
        if (isPathSepChar(newPart.charAt(0)))
            dest.append(newPart.str()+1);
        else
            dest.append(newPart);
    }
    return dest;
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

static IPropertyTree *getNamedPropTree(const IPropertyTree *parent, const char *sub, const char *key, const char *name, bool preload)
{  // no create
    if (!parent)
        return NULL;
    StringBuffer query;
    getAttrQueryStr(query,sub,key,name);
    if (preload)
        return parent->getBranch(query.str());
    return parent->getPropTree(query.str());
}

static IPropertyTree *addNamedPropTree(IPropertyTree *parent, const char *sub, const char *key, const char *name, const IPropertyTree *init=nullptr)
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

static IPropertyTree *getCostPropTree(const char *cluster)
{
    Owned<IPropertyTree> plane = getStoragePlane(cluster);

    if (plane && plane->hasProp("cost/@storageAtRest"))
    {
        return plane->getPropTree("cost");
    }
    else
    {
        return getGlobalConfigSP()->getPropTree("cost");
    }
}

extern da_decl cost_type calcFileAtRestCost(const char * cluster, double sizeGB, double fileAgeDays)
{
    Owned<const IPropertyTree> costPT = getCostPropTree(cluster);

    if (costPT==nullptr)
        return 0;
    double atRestPrice = costPT->getPropReal("@storageAtRest", 0.0);
    double storageCostDaily = atRestPrice * 12 / 365;
    return money2cost_type(storageCostDaily * sizeGB * fileAgeDays);
}

extern da_decl cost_type calcFileAccessCost(const char * cluster, __int64 numDiskWrites, __int64 numDiskReads)
{
    Owned<const IPropertyTree> costPT = getCostPropTree(cluster);

    if (costPT==nullptr)
        return 0;
    constexpr int accessPriceScalingFactor = 10000; // read/write pricing based on 10,000 operations
    double readPrice =  costPT->getPropReal("@storageReads", 0.0);
    double writePrice =  costPT->getPropReal("@storageWrites", 0.0);
    return money2cost_type((readPrice * numDiskReads / accessPriceScalingFactor) + (writePrice * numDiskWrites / accessPriceScalingFactor));
}

extern da_decl cost_type calcFileAccessCost(IDistributedFile *f, __int64 numDiskWrites, __int64 numDiskReads)
{
    StringBuffer clusterName;
    // Should really specify the cluster number too, but this is the best we can do for now
    f->getClusterName(0, clusterName);
    return calcFileAccessCost(clusterName, numDiskWrites, numDiskReads);
}

extern da_decl cost_type calcDiskWriteCost(const StringArray & clusters, stat_type numDiskWrites)
{
    if (!numDiskWrites)
        return 0;
    cost_type writeCost = 0;
    ForEachItemIn(idx, clusters)
        writeCost += calcFileAccessCost(clusters.item(idx), numDiskWrites, 0);
    return writeCost;
}


extern da_decl cost_type updateCostAndNumReads(IDistributedFile *file, stat_type numDiskReads, cost_type curReadCost)
{
    const IPropertyTree & fileAttr = file->queryAttributes();

    if (!curReadCost)
        curReadCost = calcFileAccessCost(file, 0, numDiskReads);
    cost_type legacyReadCost = 0;
    if (!fileAttr.hasProp(getDFUQResultFieldName(DFUQRFreadCost)))
    {
        if (!isFileKey(fileAttr))
        {
            stat_type prevDiskReads = fileAttr.getPropInt64(getDFUQResultFieldName(DFUQRFnumDiskReads), 0);
            legacyReadCost = calcFileAccessCost(file, 0, prevDiskReads);
        }
    }
    file->addAttrValue(getDFUQResultFieldName(DFUQRFreadCost), legacyReadCost + curReadCost);
    file->addAttrValue(getDFUQResultFieldName(DFUQRFnumDiskReads), numDiskReads);
    return curReadCost;
}

// JCSMORE - I suspect this function should be removed/deprecated. It does not deal with dirPerPart or striping.
// makePhysicalPartName supports both, but does not deal with groups/endpoints)
RemoteFilename &constructPartFilename(IGroup *grp,unsigned partno,unsigned partmax,const char *name,const char *partmask,const char *partdir,unsigned copy,ClusterPartDiskMapSpec &mspec,RemoteFilename &rfn)
{
    partno--;
    StringBuffer tmp;
    if (!name||!*name) {
        if (!partmask||!*partmask) {
            partmask = "!ERROR!._$P$_of_$N$"; // could use logical tail name if I had it
            IERRLOG("No partmask for constructPartFilename");
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


class DECL_EXCEPTION CDFS_Exception: implements IDFS_Exception, public CInterface
{
    int errcode;
    StringAttr errstr;
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
        {
            StringBuffer ip;
            queryCoven().queryGroup().queryNode(0).endpoint().getHostText(ip);
            return str.appendf(" Lookup access denied for scope %s at Dali %s", errstr.str(), ip.str());
        }
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
        case DFSERR_RestrictedFileAccessDenied:
            return str.append(": Access to restricted file denied: ").append(errstr);
        case DFSERR_EmptyStoragePlane:
            return str.append(": Cluster does not have storage plane: ").append(errstr);
        case DFSERR_MissingStoragePlane:
            return str.append(": Storage plane missing: ").append(errstr);
        case DFSERR_PhysicalCompressedPartInvalid:
            return str.append(": Compressed part is not in the valid format: ").append(errstr);
        case DFSERR_InvalidRemoteFileContext:
            return str.append(": Lookup of remote files must use wsdfs::lookup - file: ").append(errstr);
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
        for (;;)
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
                    if (timeout!=INFINITE)
                        throw;
                    IWARNLOG("CConnectLock on %s waiting for %ds",name,tt/1000);
                    if (first)
                    {
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
            catch (IException *e)
            {
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
    for (;;) {
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
    for (;;) {
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
        for (;;)
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
                IWARNLOG("CFileAttrLockBase(%s) blocked for %ds", msg, tm.elapsed()/1000);
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
    void commit() { if (conn) conn->commit(); }
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
    void commit() { CFileSubLock::commit(); }
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
    bool initWithFileLock(const CDfsLogicalFileName &logicalName, unsigned timeout, const char *msg, CFileLock &fcl, unsigned fclmode)
    {
        // SuperOwnerLock while holding fcl
        IRemoteConnection *fclConn = fcl.queryConnection();
        if (!fclConn)
            return false; // throw ?
        CTimeMon tm(timeout);
        unsigned remaining = timeout;
        for (;;)
        {
            try
            {
                if (init(logicalName, NULL, 0, msg))
                    return true;
                else
                    return false; // throw ?
            }
            catch (ISDSException *e)
            {
                if (SDSExcpt_LockTimeout != e->errorCode() || tm.timedout(&remaining))
                    throw;
                e->Release();
            }
            // release lock
            {
                fclConn->changeMode(RTM_NONE, remaining);
            }
            tm.timedout(&remaining);
            unsigned stime = 1000 * (2+getRandom()%15); // 2-15 sec
            if (stime > remaining)
                stime = remaining;
            // let another get excl lock
            Sleep(stime);
            tm.timedout(&remaining);
            // get lock again (waiting for other to release excl)
            {
                fclConn->changeMode(fclmode, remaining);
                fclConn->reload();
            }
        }
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
        IDFS_Exception *e = new CDFS_Exception(DFSERR_ForeignDaliTimeout, foreigndali->endpoint().getEndpointHostText(tmp).str());
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
        StringArray preferedClusterList;
        if (lfname.getCluster(cname).length())
            preferedClusterList.append(cname.str());
        unsigned i;
        if (clusters) {
            // if any clusters listed in the 'clusters'(csv list) are not already present in LFN cluster list (from lfname.getCluster), add them.
            // NB: lfname.getCluster will only contain any groups if the lfn has been specified as lfn@<cluster>
            for (;;) {
                const char *s = clusters;
                while (*s&&(*s!=','))
                    s++;
                if (s!=clusters) {
                    cname.clear().append(s-clusters,clusters);
                    for (i=0;i<preferedClusterList.ordinality();i++)
                        if (strcmp(preferedClusterList.item(i),cname.str())==0)
                            break;
                    if (i==preferedClusterList.ordinality())
                        preferedClusterList.append(cname.str());
                }
                if (!*s)
                    break;
                clusters = s+1;
            }
        }

        unsigned done = 0;
        StringBuffer clusterLabel;
        if (isContainerized()) {
            if (0 != preferedClusterList.ordinality()) {
                // In containerized mode, only the group (aka plane) names are considered.
                ForEachItemIn(ci,preferedClusterList) {
                    const char *cls = preferedClusterList.item(ci);
                    for (i=done;i<nc;i++) {
                        IClusterInfo &info=item(i);
                        if (strisame(info.getClusterLabel(clusterLabel.clear()).str(),cls))
                            break;
                    }
                    if (i<nc) {
                        // move found IClusterInfo up ('done' is either top or position of last moved item)
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
            }
            return;
        }
        // else !isContainerized() ..

        // NB: The below (preferedClusterList empty) will be the common case, because:
        //  a) LFN's are not routinely specified with @<cluster>
        //  b) setPreferred is not by default passed a custom perferred 'clusters' list (in fact setDefaultPreferredClusters is not called anywhere)
        // NB2: In Thor, this ip distance calculation is going to happen on the Thor master in relation to it's IP.
        if (preferedClusterList.ordinality()==0) {
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
        StringBuffer name;

        // this manipulates the IClusterInfo order, to promote them, if their group
        // matches (or are a super or subset or intersection) of their namesake in the group store
        // Assuming the supplied 'clusters' exist and are not 'GRdisjoint' with the file's cluster group(s),
        // those IClusterInfo's will be promoted.
        // e.g. setDefaultPreferredClusters is set to CLUSTERXX, CLUSTERYY, and the file has some of them,
        // those clusters will be preferred.
        // NB: If the LFN has cluster(s) (e.g. lfn@mycluster), those will take precedence the preferred clusters
        // because they will have been added to 'preferedClusterList' 1st (see above)
        ForEachItemIn(ci,preferedClusterList) {
            const char *cls = preferedClusterList.item(ci);
            Owned<IGroup> grp = queryNamedGroupStore().lookup(cls);
            if (!grp) {
                IERRLOG("IDistributedFile::setPreferred - cannot find cluster %s",cls);
                return;
            }
            if (!firstgrp.get())
                firstgrp.set(grp);
            for (i=done;i<nc;i++) {
                IClusterInfo &info=item(i);
                if (stricmp(info.getClusterLabel(clusterLabel.clear()).str(),name.str())==0) // JCS - name never set?? should probably be 'cls'
                    break;
                IGroup *grp2 = info.queryGroup();
                if (grp2&&(grp->compare(grp2)!=GRdisjoint))
                    break;
            }
            if (i<nc) {
                // true if found a non-disjoint group above
                // move it above disjoint groups ('done' is either top or position of last non-disjoint group moved)
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
    virtual ICodeContext *queryCodeContext()=0;
};

static IDistributedFileTransactionExt *queryTransactionExt(IDistributedFileTransaction *transaction)
{
    IDistributedFileTransactionExt *_transaction = dynamic_cast<IDistributedFileTransactionExt *>(transaction);
    verifyex(_transaction); // _transaction cannot be null as all IDistributedFileTransaction instances
                            //  are IDistributedFileTransactionExtinstances.
    return _transaction;
}

class CDistributedFileDirectory: implements IDistributedFileDirectory, public CInterface
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

    IDistributedFile *dolookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout);

    IDistributedFile *lookup(const char *_logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool privilegedUser, unsigned timeout) override;
    IDistributedFile *lookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool privilegedUser, unsigned timeout) override;

    /* createNew always creates an unnamed unattached distributed file
     * The caller must associated it with a name and credentials when it is attached (attach())
     */
    IDistributedFile *createNew(IFileDescriptor * fdesc, const char *optionalName=nullptr);
    IDistributedFile *createExternal(IFileDescriptor *desc, const char *name);
    IDistributedSuperFile *createSuperFile(const char *logicalname,IUserDescriptor *user,bool interleaved,bool ifdoesnotexist,IDistributedFileTransaction *transaction=NULL);
    IDistributedSuperFile *createNewSuperFile(IPropertyTree *tree, const char *optionalName=nullptr, IArrayOf<IDistributedFile> *subFiles=nullptr);
    void removeSuperFile(const char *_logicalname, bool delSubs, IUserDescriptor *user, IDistributedFileTransaction *transaction);

    IDistributedFileIterator *getIterator(const char *wildname, bool includesuper,IUserDescriptor *user,bool isPrivilegedUser);
    IDFAttributesIterator *getDFAttributesIterator(const char *wildname, IUserDescriptor *user, bool recursive, bool includesuper,INode *foreigndali,unsigned foreigndalitimeout);
    IPropertyTreeIterator *getDFAttributesTreeIterator(const char *filters, DFUQResultField* localFilters, const char *localFilterBuf,
        IUserDescriptor *user, bool recursive, bool& allMatchingFilesReceived, INode *foreigndali,unsigned foreigndalitimeout);
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

    IPropertyTree *getFileTree(const char *lname,IUserDescriptor *user,const INode *foreigndali,unsigned foreigndalitimeout,GetFileTreeOpts opts = GetFileTreeOpts::expandNodes|GetFileTreeOpts::appendForeign);
    void setFileAccessed(CDfsLogicalFileName &dlfn, IUserDescriptor *user,const CDateTime &dt,const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IFileDescriptor *getFileDescriptor(const char *lname, AccessMode accessMode, IUserDescriptor *user, const INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IDistributedFile *getFile(const char *lname, AccessMode accessMode, IUserDescriptor *user, const INode *foreigndali=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

    bool exists(const char *_logicalname,IUserDescriptor *user,bool notsuper=false,bool superonly=false);
    bool existsPhysical(const char *_logicalname,IUserDescriptor *user);

    void addEntry(CDfsLogicalFileName &lfn,IPropertyTree *root,bool superfile, bool ignoreexists);
    bool removeEntry(const char *name, IUserDescriptor *user, IDistributedFileTransaction *transaction=NULL, unsigned timeoutms=INFINITE, bool throwException=false);
    void renamePhysical(const char *oldname,const char *newname,IUserDescriptor *user,IDistributedFileTransaction *transaction);
    void removeEmptyScope(const char *name);

    IDistributedSuperFile *lookupSuperFile(const char *logicalname, IUserDescriptor *user, AccessMode accessMode, IDistributedFileTransaction *transaction, unsigned timeout=INFINITE);

    SecAccessFlags getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags);
    SecAccessFlags getFScopePermissions(const char *scope,IUserDescriptor *user,unsigned auditflags);
    SecAccessFlags getFDescPermissions(IFileDescriptor *,IUserDescriptor *user,unsigned auditflags=0);
    SecAccessFlags getDLFNPermissions(CDfsLogicalFileName &dlfn,IUserDescriptor *user,unsigned auditflags=0);
    SecAccessFlags getDropZoneScopePermissions(const char *dropZoneName,const char *dropZonePath,IUserDescriptor *user,unsigned auditflags=0);
    void setDefaultUser(IUserDescriptor *user);
    IUserDescriptor* queryDefaultUser();

    DistributedFileCompareResult fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user);
    bool filePhysicalVerify(const char *lfn1,IUserDescriptor *user,bool includecrc,StringBuffer &errstr);
    void setDefaultPreferredClusters(const char *clusters);
    void fixDates(IDistributedFile *fil);

    GetFileClusterNamesType getFileClusterNames(const char *logicalname,StringArray &out); // returns 0 for normal file, 1 for

    bool isSuperFile( const char *logicalname, IUserDescriptor *user=NULL, INode *foreigndali=NULL, unsigned timeout=0);

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
    IDFProtectedIterator *lookupProtectedFiles(const char *owner=NULL,bool notsuper=false,bool superonly=false);
    IDFAttributesIterator* getLogicalFilesSorted(IUserDescriptor* udesc, DFUQResultField *sortOrder, const void *filterBuf, DFUQResultField *specialFilters,
            const void *specialFilterBuf, unsigned startOffset, unsigned maxNum, __int64 *cacheHint, unsigned *total, bool *allMatchingFilesReceived);
    IDFAttributesIterator* getLogicalFiles(IUserDescriptor* udesc, DFUQResultField *sortOrder, const void *filterBuf, DFUQResultField *specialFilters,
            const void *specialFilterBuf, unsigned startOffset, unsigned maxNum, __int64 *cacheHint, unsigned *total, bool *allMatchingFilesReceived, bool recursive, bool sorted);

    void setFileProtect(CDfsLogicalFileName &dlfn,IUserDescriptor *user, const char *owner, bool set, const INode *foreigndali=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
    }
    virtual bool removePhysicalPartFiles(const char *logicalName, IFileDescriptor *fileDesc, IMultiException *mexcept, unsigned numParallelDeletes=0) override;
    virtual void setFileAccessed(IUserDescriptor* udesc, const char *logicalName, const CDateTime &dt, const INode *foreigndali, unsigned foreigndalitimeout);
};


// === Transactions
class CDFAction: public CInterface
{
    unsigned locked = 0;
    unsigned timeoutCount = 0;
protected:
    IDistributedFileTransactionExt *transaction = nullptr;
    IArrayOf<IDistributedFile> lockedFiles;
    DFTransactionState state = TAS_NONE;
    StringBuffer tracing;
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
                PROGLOG("CDFAction[%s] lock timed out on %s", tracing.str(), lockedFiles.item(i).queryLogicalName());

                /* Can be v. useful to know what call stack is if stuck..
                 * Trace after 30 timeouts (each timeout period ~60s)
                 */
                if (0 == (++timeoutCount % 30))
                    PrintStackReport();
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
    CDFAction()
    {
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
    logNullUser(user);//stack trace if NULL user
    if (!user)
    {
        user = queryDistributedFileDirectory().queryDefaultUser();
    }
    udesc.set(user);
}

static bool scopePermissionsAvail = true;
static SecAccessFlags getScopePermissions(const char *scopename,IUserDescriptor *user,unsigned auditflags)
{  // scope must be normalized already
    SecAccessFlags perms = SecAccess_Full;
    if (scopePermissionsAvail && scopename && *scopename) {
        if (!user)
        {
            logNullUser(user);//stack trace if NULL user
            user = queryDistributedFileDirectory().queryDefaultUser();
        }

        perms = querySessionManager().getPermissionsLDAP(queryDfsXmlBranchName(DXB_Scope),scopename,user,auditflags);
        if (perms<0) {
            if (perms == SecAccess_Unavailable) {
                scopePermissionsAvail=false;
                perms = SecAccess_Full;
            }
            else
                perms = SecAccess_None;
        }
    }
    return perms;
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
    logNullUser(user);//stack trace if NULL user
    SecAccessFlags perm = getScopePermissions(scopename,user,auditflags);
    IDFS_Exception *e = NULL;
    if (readreq&&!HASREADPERMISSION(perm))
    {
        StringBuffer scopeDescription;
        StringBuffer username("");
        if (user)
            user->getUserName(username);
        scopeDescription.appendf("%s user '%s', assigned access %s (%d)", scopename, username.str(), getSecAccessFlagName(perm), perm);
        e = new CDFS_Exception(DFSERR_LookupAccessDenied,scopeDescription);
    }
    else if (createreq&&!HASWRITEPERMISSION(perm))
    {
        StringBuffer scopeDescription;
        StringBuffer username("");
        if (user)
            user->getUserName(username);
        scopeDescription.appendf("%s user '%s', assigned access %s (%d)", scopename, username.str(), getSecAccessFlagName(perm), perm);
        e = new CDFS_Exception(DFSERR_CreateAccessDenied,scopeDescription);
    }
    if (e)
        throw e;
}

bool checkLogicalName(CDfsLogicalFileName &dlfn,IUserDescriptor *user,bool readreq,bool createreq,bool allowquery,const char *specialnotallowedmsg)
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

bool checkLogicalName(const char *lfn,IUserDescriptor *user,bool readreq,bool createreq,bool allowquery,const char *specialnotallowedmsg)
{
    CDfsLogicalFileName dlfn;
    dlfn.set(lfn);
    return checkLogicalName(dlfn, user, readreq, createreq, allowquery, specialnotallowedmsg);
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

        CTimeMon timer(timeoutms);
        for (;;)
        {
            // Transaction files have already been unlocked at this point, delete all remaining files
            Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn, user, AccessMode::tbdWrite, false, true, nullptr, defaultPrivilegedUser, SDS_SUB_LOCK_TIMEOUT);
            if (!file.get())
                return;
            StringBuffer reason;
            if (!file->canRemove(reason, false))
                ThrowStringException(-1, "Can't remove %s: %s", lfn.get(), reason.str());

            Owned<IException> timeoutException;
            // This will do the right thing for either super-files and logical-files.
            try
            {
                file->detach(0, NULL); // 0 == timeout immediately if cannot get exclusive lock
                return;
            }
            catch (ISDSException *e)
            {
                switch (e->errorCode())
                {
                    case SDSExcpt_LockTimeout:
                    case SDSExcpt_LockHeld:
                        timeoutException.setown(e);
                        break;
                    default:
                        throw;
                }
            }
            file.clear();
            unsigned sleepTime = SDS_TRANSACTION_RETRY/2+(getRandom()%SDS_TRANSACTION_RETRY);
            if (INFINITE != timeoutms)
            {
                unsigned remaining;
                if (timer.timedout(&remaining))
                {
                    StringBuffer timeoutText;
                    throwStringExceptionV(-1, "Failed to remove %s: %s", logicalname, timeoutException->errorMessage(timeoutText).str());
                }
                if (sleepTime>remaining)
                    sleepTime = remaining;
            }
            PROGLOG("CDelayedDelete: pausing due to locked file = %s", logicalname);
            Sleep(sleepTime);
        }
    }
};

class CDistributedFileTransaction: implements IDistributedFileTransactionExt, public CInterface
{
    class CTransactionFile : public CSimpleInterface
    {
        class HTMapping : public CInterface
        {
            IDistributedFile *file;
            StringAttr name;
        public:
            HTMapping(const char *_name, IDistributedFile *_file) : file(_file), name(_name) { }
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
            StringBuffer tmp;
            subFile = normalizeLFN(subFile, tmp);
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
    ICodeContext *codeCtx;

    /* 'owner' is set if, transaction object is implicitly created, because none provided
     * The owner cannot be release or unlocked. The transaction can still retry if other files are locked,
     * so need to ensure 'owner' remains in tracked file cache.
     */
    IDistributedSuperFile *owner;

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
    void commitActions()
    {
        while (actions.ordinality())  // if we get here everything should work!
        {
            Owned<CDFAction> action = &actions.popGet();
            action->commit();
        }
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
public:
    IMPLEMENT_IINTERFACE;

    CDistributedFileTransaction(IUserDescriptor *user, IDistributedSuperFile *_owner=NULL, ICodeContext *_codeCtx=NULL)
        : isactive(false), depth(0), prepared(0), codeCtx(_codeCtx), owner(_owner)
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
// IDistributedFileTransaction impl.
    virtual void start()
    {
        if (isactive)
            throw MakeStringException(-1,"Transaction already started");
        clearFiles();
        actions.kill();
        isactive = true;
        prepared = 0;
        assertex(actions.ordinality()==0);
    }
    virtual void commit()
    {
        if (!isactive)
            return;
        IException *rete=NULL;
        // =============== PREPARE AND RETRY UNTIL READY
        try
        {
            for (;;)
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
    virtual void rollback()
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
    virtual bool active()
    {
        return isactive;
    }
    virtual IDistributedFile *lookupFile(const char *name, AccessMode accessMode, unsigned timeout)
    {
        IDistributedFile *ret = findFile(name);
        if (ret)
            return LINK(ret);
        else
        {
            ret = queryDistributedFileDirectory().lookup(name, udesc, accessMode, false, false, this, defaultPrivilegedUser, timeout);
            if (ret)
                queryCreate(name, ret, true);
            return ret;
        }
    }
    virtual IDistributedSuperFile *lookupSuperFile(const char *name, AccessMode accessMode, unsigned timeout)
    {
        IDistributedFile *f = findFile(name);
        if (f)
            return LINK(f->querySuperFile());
        else
        {
            IDistributedSuperFile *ret = queryDistributedFileDirectory().lookupSuperFile(name, udesc, accessMode, this, timeout);
            if (ret)
                addFile(ret);
            return ret;
        }
    }
// IDistributedFileTransactionExt impl.
    virtual IUserDescriptor *queryUser()
    {
        return udesc;
    }
    virtual void descend() // Call this when you're recurring
    {
        depth++;
    }
    virtual void ascend() // Call this at the end of the block that started recursion
    {
        assertex(depth);
        depth--;
    }
    virtual void autoCommit()
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
    virtual void addAction(CDFAction *action)
    {
        actions.append(*action); // takes ownership
        action->setTransaction(this);
    }
    virtual void addFile(IDistributedFile *file)
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
    virtual void ensureFile(IDistributedFile *file)
    {
        if (!trackedFiles.find(file))
            addFile(file);
    }
    virtual void clearFile(IDistributedFile *file)
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
    virtual void clearFiles()
    {
        trackedFiles.kill();
        trackedFilesByName.kill();
        if (owner)
            addFile(owner); // ensure remains tracked
    }
    virtual void noteAddSubFile(IDistributedSuperFile *super, const char *superName, IDistributedFile *sub)
    {
        CTransactionFile *trackedSuper = queryCreate(superName, super);
        trackedSuper->noteAddSubFile(sub);
    }
    virtual void noteRemoveSubFile(IDistributedSuperFile *super, IDistributedFile *sub)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (trackedSuper)
            trackedSuper->noteRemoveSubFile(sub);
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
    virtual void clearSubFiles(IDistributedSuperFile *super)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (trackedSuper)
            trackedSuper->clearSubs();
    }
    virtual void noteRename(IDistributedFile *file, const char *newName)
    {
        CTransactionFile *trackedFile = lookupTrackedFile(file);
        if (trackedFile)
        {
            trackedFiles.removeExact(trackedFile);
            trackedFilesByName.removeExact(trackedFile);
            trackedFile = queryCreate(newName, file);
        }
    }
    virtual void validateAddSubFile(IDistributedSuperFile *super, IDistributedFile *sub, const char *subName);
    virtual bool isSubFile(IDistributedSuperFile *super, const char *subFile, bool sub)
    {
        CTransactionFile *trackedSuper = lookupTrackedFile(super);
        if (!trackedSuper)
            return false;
        return trackedSuper->find(subFile, sub);
    }
    virtual bool addDelayedDelete(CDfsLogicalFileName &lfn,unsigned timeoutms)
    {
        delayeddelete.append(*new CDelayedDelete(lfn,udesc,timeoutms));
        return true;
    }
    virtual bool prepareActions()
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
    virtual void retryActions()
    {
        clearFiles(); // clear all previously tracked pending file changes, e.g. renames, super file additions/removals
        while (prepared) // unlock for retry
            actions.item(--prepared).retry();
    }
    virtual void runActions()
    {
        ForEachItemIn(i,actions)
            actions.item(i).run();
    }
    virtual void commitAndClearup()
    {
        // =============== COMMIT and CLEANUP
        commitActions();
        clearFiles();
        isactive = false;
        actions.kill();
        deleteFiles();
    }
    virtual ICodeContext *queryCodeContext()
    {
        return codeCtx;
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


class CDFScopeIterator: implements IDFScopeIterator, public CInterface
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
        StringBuffer lockPath;
        if (!isEmptyString(base))
        {
            CDfsLogicalFileName dlfn;
            dlfn.set(base, "dummyfilename"); // makeScopeQuery expects a lfn to a file, 'dummyfilename' will not be used
            dlfn.makeScopeQuery(lockPath, true);
        }
        else
            lockPath.append(querySdsFilesRoot());

        {
            CConnectLock connlock("CDFScopeIterator", lockPath, false, false, false, timeout);
            if (connlock.conn)
            {
                StringBuffer name;
                add(*connlock.conn->queryRoot(),recursive,name);
            }
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

class CDFAttributeIterator: implements IDFAttributesIterator, public CInterface
{
    unsigned index;
    IArrayOf<IPropertyTree> attrs;
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
                    if (plist.length())
                        plist.append(',');
                    plist.append(name);
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


class CDFProtectedIterator: implements IDFProtectedIterator, public CInterface
{
    StringAttr owner;
    StringAttr fn;
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
        for (;;) {
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
    std::vector<unsigned> stripeNumber;
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
    virtual StringBuffer &getStorageFilePath(StringBuffer & path, unsigned copy) override;
    virtual unsigned getStripeNum(unsigned copy) override;
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
class CDistributedFileIteratorBase: implements INTERFACE, public CInterface
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
    bool isPrivilegedUser = false;

    bool set()
    {
        while (isValid()) {
            cur.setown(parent->lookup(queryName(),udesc, AccessMode::tbdRead, false, false, nullptr, isPrivilegedUser));
            if (cur)
                return true;
            index++;
        }
        return false;
    }

public:
    CDistributedFileIterator(IDistributedFileDirectory *_dir,const char *wildname,bool includesuper,IUserDescriptor *user,bool _isPrivilegedUser, IDistributedFileTransaction *_transaction=NULL)
        : transaction(_transaction), isPrivilegedUser(_isPrivilegedUser)
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
        : transaction(_transaction), owner(_owner)
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
            cur.setown(transaction->lookupSuperFile(queryName(), AccessMode::tbdRead));
        else
            cur.setown(parent->lookupSuperFile(queryName(), udesc, AccessMode::tbdRead, NULL));

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
        DBGLOG("%s - root changed",trc);
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
    if (owner&&*owner)
    {
        Owned<IPropertyTree> t = getNamedPropTree(&p, "Protect", "@name", owner, false);
        if (t)
        {
            if (protect)
            {
                StringBuffer str;
                t->setProp("@modified", dt.getString(str).str());
            }
            else
                p.removeTree(t);
        }
        else if (protect)
        {
            t.setown(addNamedPropTree(&p, "Protect", "@name", owner));
            StringBuffer str;
            t->setProp("@modified",dt.getString(str).str());
        }
        ret = true;
    }
    else if (!protect)
    {
        unsigned n=0;
        IPropertyTree *pt;
        while ((pt=p.queryPropTree("Protect[1]"))!=NULL)
        {
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
    ForEach(*wpiter)
    {
        IPropertyTree &t = wpiter->query();
        const char *wpname = t.queryProp("@name");
        if (!wpname||!*wpname)
            wpname = "<Unknown>";
        if (prot)
            reason.appendf(", %s", wpname);
        else
        {
            reason.appendf("file %s protected by %s", logicalname, wpname);
            prot = true;
        }
    }
    return prot;
}

bool hasTLK(IDistributedFile *f)
{
    if (!isFileKey(f))
        return false;

    unsigned np = f->numParts();
    if (np <= 1)
        return false;

    return isPartTLK(&f->queryPart(np-1));
}


/**
 * A template class which implements the common methods of an IDistributedFile interface.
 * The actual interface (extended from IDistributedFile) is provided as a template argument.
 */
template <class INTERFACE>
class CDistributedFileBase : implements INTERFACE, public CInterface
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
    bool external = false;
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

    IPropertyTree *queryHistory() const
    {
        IPropertyTree *attr =  root->queryPropTree("Attr");
        if (attr)
            return attr->queryPropTree("History");
        return nullptr;
    }

    void resetHistory()
    {
        DistributedFilePropertyLock lock(this);
        IPropertyTree *history = queryHistory();
        if (history)
            queryAttributes().removeTree(history);
    }
    virtual void lockFileAttrLock(CFileAttrLock & attrLock)
    {
        if (!attrLock.init(logicalName, DXB_File, RTM_LOCK_WRITE, conn, defaultTimeout, "CDistributedFile::lockFileAttrLock"))
        {
            // In unlikely event File/Attr doesn't exist, must ensure created, commited and root connection is reloaded.
            verifyex(attrLock.init(logicalName, DXB_File, RTM_LOCK_WRITE|RTM_CREATE_QUERY, conn, defaultTimeout, "CDistributedFile::lockFileAttrLock"));
            attrLock.commit();
            conn->commit();
            conn->reload();
            root.setown(conn->getRoot());
        }
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
                IERRLOG("setProtect - cannot protect %s (no connection in file)",owner?owner:"");
        }
    }
    virtual bool isRestrictedAccess() override
    {
        return queryAttributes().getPropBool("restricted");
    }
    virtual void setRestrictedAccess(bool restricted) override
    {
        DistributedFilePropertyLock lock(this);
        queryAttributes().setPropBool("restricted", restricted);
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
        // MORE - this first check looks completely useless to me
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
        if (subLocal != superLocal && sub->numParts()>1) // ignore if checking 1 part file, which can be flagged as local or non-local
        {
            throw MakeStringException(-1,"%s: %s's local setting (%s) is different than %s's (%s)",
                    exprefix, sub->queryLogicalName(), (subLocal?"local":"global"),
                    queryLogicalName(), (superLocal?"local":"global"));
        }

        int superRepO = superProp.getPropInt("@replicateOffset",1);
        int subRepO = subProp.getPropInt("@replicateOffset",1);
        if (subRepO != superRepO)
            throw MakeStringException(-1,"%s: %s's replication offset (%d) is different than %s's (%d)",
                    exprefix, sub->queryLogicalName(), subRepO,
                    queryLogicalName(), superRepO);
    }

    void getSuperOwners(StringArray &owners)
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
            IERRLOG("linkSuperOwner - cannot link to %s (no connection in file)",superfile);
    }

    void setAccessed()
    {
        CDateTime dt;
        dt.setNow();
        setAccessedTime(dt);
    }

    virtual void addAttrValue(const char *attr, unsigned __int64 value) override
    {
        if (0==value)
            return;
        if (logicalName.isForeign())
        {
            // Note: it is not possible to update foreign attributes at the moment, so ignoring
        }
        else
        {
            CFileAttrLock attrLock;
            if (conn)
                lockFileAttrLock(attrLock);
            unsigned __int64 currentVal = queryAttributes().getPropInt64(attr);
            queryAttributes().setPropInt64(attr, currentVal+value);
        }
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
    bool checkOwned(StringBuffer &error)
    {
        Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
        if (iter->first())
        {
            error.append("Cannot remove file ").append(logicalName.get()).append(" as owned by SuperFile(s): ");
            for (;;)
            {
                error.append(iter->query().queryProp("@name"));
                if (!iter->next())
                    break;
                error.append(", ");
            }
            return true;
        }
        return false;
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
        if (logicalName.isQuery())
        {
            reason.appendf("%s is query",logicalname);
            return false;
        }
        if (logicalName.isForeign())
        {
            reason.appendf("%s is foreign",logicalname);
            return false;
        }
        if (logicalName.isMulti())
        {
            reason.appendf("%s is multi",logicalname);
            return false;
        }
        if (!ignoresub)
        {
            if (checkOwned(reason))
                return false;
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
    virtual bool isExternal() const { return external; }

    virtual int getExpire(StringBuffer *expirationDate)
    {
        int expireDays = queryAttributes().getPropInt("@expireDays", -1);
        if (!expirationDate || (expireDays == -1))
            return expireDays;

        const char *lastAccessed = queryAttributes().queryProp("@accessed");
        if (isEmptyString(lastAccessed))
            return expireDays;

        CDateTime expires;
        expires.setString(lastAccessed);
        expires.adjustTime(60*24*expireDays);
        expires.getString(*expirationDate);
        return expireDays;
    }

    virtual void setExpire(int expireDays)
    {
        DistributedFilePropertyLock lock(this);
        if (expireDays == -1)
            queryAttributes().removeProp("@expireDays"); // Never expire
        else
            queryAttributes().setPropInt("@expireDays", expireDays);
    }
};

class CDistributedFile: public CDistributedFileBase<IDistributedFile>
{
protected:
    CDistributedFilePartArray parts;            // use queryParts to access
    CriticalSection sect;
    StringAttr directory;
    StringAttr partmask;
    FileClusterInfoArray clusters;
    FileDescriptorFlags fileFlags = FileDescriptorFlags::none;
    unsigned lfnHash = 0;
    AccessMode accessMode = AccessMode::none;

    void savePartsAttr(bool force) override
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
                    pt->addProp("@node",part.queryNode(0)->endpoint().getEndpointHostText(eps).str()); // legacy
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
    void detach(unsigned timeoutMs, bool removePhysicals, ICodeContext *ctx)
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
    bool removePhysicalPartFiles(IFileDescriptor *fileDesc, IMultiException *mexcept, unsigned numParallelDeletes=0)
    {
        if (logicalName.isExternal())
        {
            if (logicalName.isQuery())
                return false;
        }
        if (logicalName.isForeign())
            throw MakeStringException(-1,"cannot remove a foreign file (%s)",logicalName.get());

        return parent->removePhysicalPartFiles(logicalName.get(), fileDesc, mexcept, numParallelDeletes);
    }

    bool calculateSkew(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart)
    {
        unsigned np = numParts();
        if (0 == np)
            return false;

        // Do not include the TLK in the skew calculation
        if (hasTLK(this))
            np--;

        offset_t maxPartSz = 0, minPartSz = (offset_t)-1, totalPartSz = 0;

        try
        {
            maxSkewPart = 0;
            minSkewPart = 0;
            for (unsigned p=0; p<np; p++)
            {
                IDistributedFilePart &part = queryPart(p);
                offset_t size = part.getFileSize(true, false);
                if (size > maxPartSz)
                {
                    maxPartSz = size;
                    maxSkewPart = p;
                }
                if (size < minPartSz)
                {
                    minPartSz = size;
                    minSkewPart = p;
                }
                totalPartSz += size;
            }
        }
        catch (IException *e)
        {
            // guard against getFileSize throwing an exception (if parts missing)
            EXCLOG(e);
            e->Release();
            return false;
        }
        offset_t avgPartSz = totalPartSz / np;
        if (0 == avgPartSz)
            minSkew = maxSkew = 0;
        else
        {
            maxSkew = (unsigned)(10000.0 * (((double)maxPartSz-avgPartSz)/avgPartSz));
            minSkew = (unsigned)(10000.0 * ((avgPartSz-(double)minPartSz)/avgPartSz));
        }

        // +1 because published part number references are 1 based.
        maxSkewPart++;
        minSkewPart++;

        return true;
    }

    void calculateSkew() // called when a logical file is attached
    {
        IPropertyTree &attrs = queryAttributes();
        unsigned maxSkew, minSkew, maxSkewPart, minSkewPart;
        if (!calculateSkew(maxSkew, minSkew, maxSkewPart, minSkewPart))
        {
            attrs.removeProp("@maxSkew");
            attrs.removeProp("@minSkew");
            attrs.removeProp("@maxSkewPart");
            attrs.removeProp("@minSkewPart");
            return;
        }
        attrs.setPropInt("@maxSkew", maxSkew);
        attrs.setPropInt("@maxSkewPart", maxSkewPart);
        attrs.setPropInt("@minSkew", minSkew);
        attrs.setPropInt("@minSkewPart", minSkewPart);
    }

protected: friend class CDistributedFilePart;
    CDistributedFilePartArray &queryParts()
    {
        return parts;
    }
public:
    IMPLEMENT_IINTERFACE_O;

    // NB: this form is used for pre-existing file, by dolookup
    CDistributedFile(CDistributedFileDirectory *_parent, IRemoteConnection *_conn,const CDfsLogicalFileName &lname,AccessMode _accessMode,IUserDescriptor *user) // takes ownership of conn
    {
        setUserDescriptor(udesc,user);
        accessMode = _accessMode;
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
        setLFNHash(fdesc);
        setClusters(fdesc);
        setPreferredClusters(_parent->defprefclusters);
        setParts(fdesc,false);
        //shrinkFileTree(root); // enable when safe!
    }

    // NB: this form is used for a new/unattached file
    CDistributedFile(CDistributedFileDirectory *_parent, IFileDescriptor *fdesc, IUserDescriptor *user, bool _external)
    {
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile.b fdesc",fdesc);
#endif
        parent = _parent;
        accessMode = static_cast<AccessMode>(fdesc->queryProperties().getPropInt("@accessMode"));
        root.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
        root->setPropTree("ClusterLock", createPTree());
//      fdesc->serializeTree(*root,IFDSF_EXCLUDE_NODES);
        setFileAttrs(fdesc,true);
        if (!external)
            setLFNHash(fdesc);
        setClusters(fdesc);
        setPreferredClusters(_parent->defprefclusters);
        saveClusters();
        setParts(fdesc,true);
        udesc.set(user);
        external = _external;
#ifdef EXTRA_LOGGING
        LOGPTREE("CDistributedFile.b root.1",root);
#endif
        offset_t totalsize = 0;
        offset_t totalCompressedSize = 0;
        offset_t totalUncompressedSize = 0;
        unsigned checkSum = ~0;
        bool useableCheckSum = true;
        MemoryBuffer pmb;
        unsigned n = fdesc->numParts();
        for (unsigned i=0;i<n;i++)
        {
            IPropertyTree *partattr = &fdesc->queryPart(i)->queryProperties();
            if (!partattr)
            {
                totalsize = (offset_t)-1;
                totalCompressedSize = (offset_t)-1;
                useableCheckSum = false;
            }
            else
            {
                if (totalsize!=(offset_t)-1)
                {
                    offset_t psz = (offset_t)partattr->getPropInt64("@size", -1);
                    if (psz==(offset_t)-1)
                        totalsize = psz;
                    else
                        totalsize += psz;

                    psz = (offset_t)partattr->getPropInt64("@compressedSize", -1);
                    if (psz==(offset_t)-1)
                        totalCompressedSize = psz;
                    else
                        totalCompressedSize += psz;

                    psz = (offset_t)partattr->getPropInt64("@uncompressedSize", -1);
                    if (psz==(offset_t)-1)
                        totalUncompressedSize = psz;
                    else
                        totalUncompressedSize += psz;
                }
                if (useableCheckSum)
                {
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
        if ((totalCompressedSize!=(offset_t)-1) && (totalCompressedSize != 0))
            queryAttributes().setPropInt64("@compressedSize", totalCompressedSize);
        if ((totalUncompressedSize!=(offset_t)-1) && (totalUncompressedSize != 0))
            queryAttributes().setPropInt64("@uncompressedSize", totalUncompressedSize);
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

    //Ensure that enough time has passed from when the file was last modified for reads to be consistent
    //Important for blob storage or remote, geographically synchronized storage
    void checkWriteSync()
    {
        time_t modifiedTime = 0;
        time_t now = 0;

        Owned<IPropertyTreeIterator> iter = root->getElements("Cluster");
        ForEach(*iter)
        {
            const char * name = iter->query().queryProp("@name");
            unsigned marginMs = getWriteSyncMarginMs(name);
            if (marginMs)
            {
                if (0 == modifiedTime)
                {
                    CDateTime modified;
                    if (!getModificationTime(modified))
                        return;
                    modifiedTime = modified.getSimple();
                }

                if (0 == now)
                    now = time(&now);

                //Round the elapsed time down - so that a change on the last ms of one time period does not count as a whole second of elapsed time
                //This could be avoided if the modified time was more granular
                unsigned __int64 elapsedMs = (now - modifiedTime) * 1000;
                if (elapsedMs >= 1000)
                    elapsedMs -= 999;

                if (unlikely(elapsedMs < marginMs))
                {
                    LOG(MCuserProgress, "Delaying access to %s on %s for %ums to ensure write sync", queryLogicalName(), name, (unsigned)(marginMs - elapsedMs));
                    MilliSleep(marginMs - elapsedMs);
                    now = 0; // re-evaluate now - unlikely to actually happen
                }
            }
        }
    }

    bool hasDirPerPart() const
    {
        return FileDescriptorFlags::none != (fileFlags & FileDescriptorFlags::dirperpart);
    }

    IFileDescriptor *getFileDescriptor(const char *_clusterName) override
    {
        CriticalBlock block (sect);
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
        fdesc->setTraceName(logicalName.get());
        fdesc->queryProperties().setPropInt("@accessMode", static_cast<int>(accessMode));
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

    void setLFNHash(IFileDescriptor *fdesc)
    {
        if (fdesc->queryProperties().hasProp("@lfnHash"))
            lfnHash = fdesc->queryProperties().getPropInt("@lfnHash");
        else
        {
            // this is a guard, just in case the file descriptor has the lfnHash missing
            lfnHash = getFilenameHash(logicalName.get());
        }
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
        if (save)
        {
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
        // this change was not required, but it is more consistent to get the property from the deserialized passed in file descriptor
        fileFlags = static_cast<FileDescriptorFlags>(fdesc->queryProperties().getPropInt("@flags"));
    }

    void setClusters(IFileDescriptor *fdesc)
    {
        clusters.clear();
        unsigned nc = fdesc->numClusters();
        if (nc)
        {
            unsigned flags = 0;
            if (FileDescriptorFlags::none != (FileDescriptorFlags::foreign & fdesc->getFlags()))
                flags = IFDSF_FOREIGN_GROUP;
            for (unsigned i=0;i<nc;i++)
            {
                StringBuffer cname;
                fdesc->getClusterGroupName(i, cname, nullptr);
                IClusterInfo *cluster;
                if (cname.length())
                    cluster = LINK(fdesc->queryClusterNum(i));
                else
                {
                    // NB: this is non-standard, for situations where the cluster name is not known,
                    // which happens where none has been provided/set to the file descriptor,
                    // and the file descriptor has been built up of parts with ips. 
                    // createClusterInfo will perform a reverse lookup to Dali to try to discover
                    // a group name.
                    cluster = createClusterInfo(
                                  nullptr,
                                  fdesc->queryClusterGroup(i),
                                  fdesc->queryPartDiskMapping(i),
                                  &queryNamedGroupStore(),
                                  flags
                               );

                    if (!cluster->queryGroup(&queryNamedGroupStore()))
                        IERRLOG("IDistributedFileDescriptor cannot set cluster for %s", logicalName.get());
                }
#ifdef EXTRA_LOGGING
                PROGLOG("setClusters(%d,%s)", i, cluster->queryGroupName());
#endif
                clusters.append(*cluster);
            }
        }
        else
            IERRLOG("No cluster specified for %s",logicalName.get());
    }

    virtual unsigned numClusters() override
    {
        return clusters.ordinality();
    }

    virtual unsigned findCluster(const char *clustername) override
    {
        return clusters.find(clustername);
    }

    virtual unsigned getClusterNames(StringArray &clusternames) override
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
        for (;;) {
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
                    DBGLOG("CFileClusterOwner::saveClusters - empty cluster");
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

    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) override
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

    virtual bool removeCluster(const char *clustername) override
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

    virtual void setPreferredClusters(const char *clusterlist) override
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


    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) override
    {
        return clusters.getName(clusternum,name);
    }

    unsigned copyClusterNum(unsigned part, unsigned copy,unsigned *replicate)
    {
        return clusters.copyNum(part,copy, numParts(),replicate);
    }

    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) override
    {
        assertex(clusternum<clusters.ordinality());
        return clusters.queryPartDiskMapping(clusternum);
    }

    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec) override
    {
        CClustersLockedSection cls(CDistributedFileBase<IDistributedFile>::logicalName, true);
        reloadClusters();
        unsigned i = findCluster(clustername);
        if (i!=NotFound) {
            clusters.updatePartDiskMapping(i,spec);
            saveClusters();
        }
    }

    virtual IGroup *queryClusterGroup(unsigned clusternum) override
    {
        return clusters.queryGroup(clusternum);
    }

    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name) override
    {
        return clusters.item(clusternum).getGroupName(name, &queryNamedGroupStore());
    }

    virtual unsigned numCopies(unsigned partno) override
    {
        return clusters.numCopies(partno,numParts());
    }

    virtual void setSingleClusterOnly() override
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

    virtual unsigned numParts() override
    {
        return parts.ordinality();
    }

    virtual IDistributedFilePart &queryPart(unsigned idx) override
    {
        if (idx>=parts.ordinality())
            throwUnexpectedX("CDistributedFileBase::queryPart out of range");
        return queryParts().item(idx);
    }

    virtual IDistributedFilePart* getPart(unsigned idx) override
    {
        if (idx>=parts.ordinality())
            return NULL;
        IDistributedFilePart *ret = &queryParts().item(idx);
        return LINK(ret);
    }

    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) override
    {
        return new CDistributedFilePartIterator(queryParts(),filter);
    }

    virtual void rename(const char *_logicalname,IUserDescriptor *user) override
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


    virtual const char *queryDefaultDir() override
    {
        CriticalBlock block (sect);
        return directory.get();
    }

    virtual const char *queryPartMask() override
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

    virtual void attach(const char *_logicalname,IUserDescriptor *user) override
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
        try
        {
            calculateSkew();
            // ensure lfnHash is present and published
            if (queryAttributes().hasProp("@lfnHash"))
                lfnHash = queryAttributes().getPropInt("@lfnHash");
            else
            {
                lfnHash = getFilenameHash(logicalName.get());
                queryAttributes().setPropInt("@lfnHash", lfnHash);
            }
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
            setAccessed();
#ifdef EXTRA_LOGGING
            LOGFDESC("CDistributedFile::attach fdesc",fdesc);
            LOGPTREE("CDistributedFile::attach root.2",root);
#endif
        }
        catch (IException *e)
        {
            EXCLOG(e, "CDistributedFile::attach");
            logicalName.clear();
            throw;
        }
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
        detach(timeoutms, false, NULL);
    }

public:
    virtual void detach(unsigned timeoutMs=INFINITE, ICodeContext *ctx=NULL) override
    {
        detach(timeoutMs, true, ctx);
    }

    virtual bool existsPhysicalPartFiles(unsigned short port) override
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
                        rfn.queryEndpoint().getEndpointHostText(s);
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
        for (;;) {
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

    virtual bool renamePhysicalPartFiles(const char *newname,
                                         const char *cluster,
                                         IMultiException *mexcept,
                                         const char *newbasedir) override
    {
        // cluster TBD
        unsigned width = numParts();
        StringBuffer newdir;
        StringBuffer newmask;
        const char *diroverride = NULL;
        char psc = getPathSepChar(directory.get());
        DFD_OS os = SepCharBaseOs(psc);
        StringBuffer basedir;

        StringBuffer myBase;
        if (newbasedir)
        {
            diroverride = newbasedir;
            myBase.set(newbasedir);
        }
        else
        {
#ifdef _CONTAINERIZED
            IClusterInfo &iClusterInfo = clusters.item(0);
            const char *planeName = iClusterInfo.queryGroupName();
            if (!isEmptyString(planeName))
            {
                Owned<IStoragePlane> plane = getDataStoragePlane(planeName, false);
                if (plane)
                {
                    if (clusters.ordinality() > 1)
                    {
                        // NB: this may need revisiting if rename needs to support renaming of files on >1 cluster
                        throwUnexpectedX("renamePhysicalPartFiles - not supported on files on multiple planes");
                    }
                    myBase.set(plane->queryPrefix());
                }
            }
#else
            myBase.set(queryBaseDirectory(grp_unknown, 0, os));
#endif
            diroverride = myBase;
        }

        StringBuffer baseDir, newPath;
        getLFNDirectoryUsingBaseDir(newPath, logicalName.get(), diroverride);
        if (!getBase(directory, newPath, baseDir))
            baseDir.append(myBase); // getBase returns false, if directory==newPath, so have common base
        getPartMask(newmask,newname,width);
        if (newmask.length()==0)
            return false;
        getLFNDirectoryUsingBaseDir(newPath.clear(), newname, diroverride);
        if (newPath.length()==0)
            return false;
        if (isPathSepChar(newPath.charAt(newPath.length()-1)))
            newPath.setLength(newPath.length()-1);
        newPath.remove(0, myBase.length());
        newdir.append(baseDir);
        appendEnsurePathSepChar(newdir, newPath, psc);
        StringBuffer fullname;
        CIArrayOf<CIStringArray> newNames;
        unsigned i;
        for (i=0;i<width;i++)
        {
            newNames.append(*new CIStringArray);
            CDistributedFilePart &part = parts.item(i);
            for (unsigned copy=0; copy<part.numCopies(); copy++)
            {
                unsigned cn = copyClusterNum(i, copy, nullptr);
                unsigned numStripedDevices = queryPartDiskMapping(cn).numStripedDevices;
                unsigned stripeNum = calcStripeNumber(i, lfnHash, numStripedDevices);

                makePhysicalPartName(newname, i+1, width, newPath.clear(), 0, os, myBase, hasDirPerPart(), stripeNum);
                newPath.remove(0, myBase.length());

                StringBuffer copyDir(baseDir);
                adjustClusterDir(i, copy, copyDir);
                fullname.clear().append(copyDir);
                appendEnsurePathSepChar(fullname, newPath, psc);
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
                : crit(_crit), newNames(_newNames)
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
                    DBGLOG("renamePhysicalPartFiles: %s doesn't exist",s.str());
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
            for (unsigned i=0;i<width;i++)
                parts.item(i).clearOverrideName();
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

    virtual __int64 getFileSize(bool allowphysical,bool forcephysical) override
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

    virtual __int64 getDiskSize(bool allowphysical,bool forcephysical) override
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

    virtual bool getFileCheckSum(unsigned &checkSum) override
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

    virtual bool getFormatCrc(unsigned &crc) override
    {
        if (queryAttributes().hasProp("@formatCrc")) {
            // NB pre record_layout CRCs are not valid
            crc = (unsigned)queryAttributes().getPropInt("@formatCrc");
            return true;
        }
        return false;
    }

    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) override
    {
        return queryAttributes().getPropBin(attrname, layout);
    }

    virtual bool getRecordSize(size32_t &rsz) override
    {
        if (queryAttributes().hasProp("@recordSize")) {
            rsz = (size32_t)queryAttributes().getPropInt("@recordSize");
            return true;
        }
        return false;
    }

    virtual unsigned getPositionPart(offset_t pos, offset_t &base) override
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

    IDistributedSuperFile *querySuperFile() override
    {
        return NULL; // i.e. this isn't super file
    }

    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) override
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

    virtual void enqueueReplicate() override
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

    virtual bool getAccessedTime(CDateTime &dt) override
    {
        StringBuffer str;
        if (!root->getProp("@accessed",str))
            return false;
        dt.setString(str.str());
        return true;
    }

    virtual void setAccessedTime(const CDateTime &dt) override
    {
        if (logicalName.isForeign())
            parent->setFileAccessed(logicalName,udesc,dt);
        else
        {
            CFileAttrLock attrLock;
            if (conn)
                lockFileAttrLock(attrLock);

            if (dt.isNull())
                queryAttributes().removeProp("@accessed");
            else
            {
                StringBuffer str;
                queryAttributes().setProp("@accessed",dt.getString(str).str());
            }
        }
    }

    virtual void setAccessed() override
    {
        CDateTime dt;
        dt.setNow();
        setAccessedTime(dt);
    }

    virtual void validate() override
    {
        if (!existsPhysicalPartFiles(0))
        {
            const char * logicalName = queryLogicalName();
            throw MakeStringException(-1, "Some physical parts do not exists, for logical file : %s",(isEmptyString(logicalName) ? "[unattached]" : logicalName));
        }
    }
    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) override
    {
        const IPropertyTree *attrs = root->queryPropTree("Attr");
        if (attrs && attrs->hasProp("@maxSkew"))
        {
            maxSkew = attrs->getPropInt("@maxSkew");
            minSkew = attrs->getPropInt("@minSkew");
            maxSkewPart = attrs->getPropInt("@maxSkewPart");
            minSkewPart = attrs->getPropInt("@minSkewPart");
            return true;
        }
        else if (calculateIfMissing)
            return calculateSkew(maxSkew, minSkew, maxSkewPart, minSkewPart);
        else
            return false;
    }
    virtual void getCost(const char * cluster, cost_type & atRestCost, cost_type & accessCost) override
    {
        atRestCost = 0;
        accessCost = 0;
        CDateTime dt;
        getModificationTime(dt);
        double fileAgeDays = difftime(time(nullptr), dt.getSimple())/(24*60*60);
        double sizeGB = getDiskSize(true, false) / ((double)1024 * 1024 * 1024);
        const IPropertyTree *attrs = root->queryPropTree("Attr");

        if (isEmptyString(cluster))
        {
            StringArray clusterNames;
            unsigned countClusters = getClusterNames(clusterNames);
            for (unsigned i = 0; i < countClusters; i++)
                atRestCost += calcFileAtRestCost(clusterNames[i], sizeGB, fileAgeDays);
            if (countClusters)
                cluster = clusterNames[0];
        }
        else
        {
            atRestCost = calcFileAtRestCost(cluster, sizeGB, fileAgeDays);
        }
        if (attrs)
            accessCost = getReadCost(*attrs, cluster) + getWriteCost(*attrs, cluster);
    }
};

StringBuffer &CDistributedFilePart::getStorageFilePath(StringBuffer & path, unsigned copy)
{
    unsigned nc = copyClusterNum(copy, nullptr);
    IClusterInfo &cluster = parent.clusters.item(nc);
    const char *planeName = cluster.queryGroupName();
    if (isEmptyString(planeName))
    {
        StringBuffer lname;
        parent.getLogicalName(lname);
        throw new CDFS_Exception(DFSERR_EmptyStoragePlane, lname.str());
    }
    // Need storage path(prefix) to work out path on storage plane
    // (After removing prefix, the remaining path is the path on storage plane)
    Owned<IStoragePlane> storagePlane = getDataStoragePlane(planeName, false);
    if (!storagePlane)
        throw new CDFS_Exception(DFSERR_MissingStoragePlane, planeName);

    path.append(parent.directory);
    if (parent.hasDirPerPart())
        addPathSepChar(path).append(partIndex+1); // part subdir 1 based
    addPathSepChar(path);
    getPartName(path);

    unsigned prefixLength = strlen(storagePlane->queryPrefix());
    path.remove(0, prefixLength);
    return path;
}

unsigned CDistributedFilePart::getStripeNum(unsigned copy)
{
    if (copy >= stripeNumber.size() || stripeNumber[copy]==UINT_MAX)
    {
        unsigned nc = copyClusterNum(copy, nullptr);
        IClusterInfo &cluster = parent.clusters.item(nc);
        const char *planeName = cluster.queryGroupName();
        if (isEmptyString(planeName))
        {
            StringBuffer lname;
            parent.getLogicalName(lname);
            throw new CDFS_Exception(DFSERR_EmptyStoragePlane, lname.str());
        }
        Owned<IStoragePlane> storagePlane = getDataStoragePlane(planeName, false);
        if (!storagePlane)
            throw new CDFS_Exception(DFSERR_MissingStoragePlane, planeName);
        if (copy >= stripeNumber.size())
            stripeNumber.insert(stripeNumber.end(), copy - stripeNumber.size() + 1 , UINT_MAX); // create empty place holders
        stripeNumber[copy] = calcStripeNumber(partIndex, parent.lfnHash, storagePlane->numDevices());
    }
    return stripeNumber[copy];
}

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
    bool containsRestrictedSubfile = false;
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
            tracing.appendf("AddSubFile: %s, to super: %s", _subfile, _parentlname);
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname, AccessMode::tbdWrite));
            if (!parent)
                throw MakeStringException(-1,"addSubFile: SuperFile %s cannot be found",parentlname.get());
            if (!subfile.isEmpty())
            {
                try
                {
                    sub.setown(transaction->lookupFile(subfile, AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT));
                    if (!sub)
                        throw MakeStringException(-1,"cAddSubFileAction: sub file %s not found", subfile.str());
                    // Must validate before locking for update below, to check sub is not already in parent (and therefore locked already)
                    transaction->validateAddSubFile(parent, sub, subfile);
                }
                catch (ISDSException *e)
                {
                    if (e->errorCode()!=SDSExcpt_LockTimeout)
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
            tracing.appendf("RemoveSubFile: %s, from super: %s", _subfile, _parentlname);
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname, AccessMode::tbdWrite));
            if (!parent)
                throw MakeStringException(-1,"removeSubFile: SuperFile %s cannot be found",parentlname.get());
            if (!subfile.isEmpty())
            {
                try
                {
                    sub.setown(transaction->lookupFile(subfile, AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT));
                }
                catch (ISDSException *e)
                {
                    if (e->errorCode()!=SDSExcpt_LockTimeout)
                        throw;
                    e->Release();
                    return false;
                }
                if (!transaction->isSubFile(parent, subfile, true))
                {
                    IWARNLOG("removeSubFile: File %s is not a subfile of %s", subfile.get(), parent->queryLogicalName());
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
                        transaction->addDelayedDelete(lname, INFINITE);
                    } else { // Remove all subfiles
                        Owned<IDistributedFileIterator> iter = parent->getSubFileIterator(false);
                        ForEach (*iter) {
                            CDfsLogicalFileName lname;
                            IDistributedFile *f = &iter->query();
                            lname.set(f->queryLogicalName());
                            transaction->addDelayedDelete(lname, INFINITE);
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
            tracing.appendf("RemoveOwnedSubFiles: super: %s", _parentlname);
        }
        virtual bool prepare()
        {
            parent.setown(transaction->lookupSuperFile(parentlname, AccessMode::tbdWrite));
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
                    IDistributedSuperFile *super = file->querySuperFile();
                    StringArray owners;
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
                            transaction->addDelayedDelete(lname, INFINITE);
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
            tracing.appendf("SwapFile: super1: %s, super2: %s", _super1Name, _super2Name);
        }
        virtual bool prepare()
        {
            super1.setown(transaction->lookupSuperFile(super1Name, AccessMode::writeMeta));
            if (!super1)
                throw MakeStringException(-1,"swapSuperFile: SuperFile %s cannot be found", super1Name.get());
            super2.setown(transaction->lookupSuperFile(super2Name, AccessMode::writeMeta));
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
    AccessMode accessMode = AccessMode::none;

    void clearSuperOwners(unsigned timeoutMs, ICodeContext *ctx)
    {
        /* JCSMORE - Why on earth is this doing this way?
         * We are in a super file, we already have [read] locks to sub files (in 'subfiles' array)
         * This should iterate through those and call unlinkSubFile I think.
         */
        Owned<IPropertyTreeIterator> iter = root->getElements("SubFile");
        StringBuffer oquery;
        oquery.append("SuperOwner[@name=\"").append(logicalName.get()).append("\"]");
        ForEach(*iter)
        {
            const char *name = iter->query().queryProp("@name");
            if (name&&*name)
            {
                CDfsLogicalFileName subfn;
                subfn.set(name);
                if (subfn.isForeign() || subfn.isExternal())
                    continue; // can't be owned by a super in this environment, no locking
                CFileLock fconnlockSub;
                // JCSMORE - this is really not right, but consistent with previous version
                // MORE: Use CDistributedSuperFile::linkSuperOwner(false) - ie. unlink
                if (fconnlockSub.init(subfn, RTM_LOCK_READ, timeoutMs, "CDistributedFile::doRemoveEntry"))
                {
                    IPropertyTree *subfroot = fconnlockSub.queryRoot();
                    if (subfroot)
                    {
                        if (!subfroot->removeProp(oquery.str()))
                        {
                            VStringBuffer s("SubFile %s is not owned by SuperFile %s", name, logicalName.get());
                            if (ctx)
                                ctx->addWuExceptionEx(s.str(), 0, SeverityWarning, MSGAUD_user, "DFS[clearSuperOwner]");
                            else
                            {
                                Owned<IException> e = makeStringException(-1, s.str());
                                EXCLOG(e, "DFS[clearSuperOwner]");
                            }
                        }
                    }
                }
            }
        }
    }

    static StringBuffer &getSubPath(StringBuffer &path,unsigned idx)
    {
        return path.append("SubFile[@num=\"").append(idx+1).append("\"]");
    }

    void loadSubFiles(IDistributedFileTransaction *transaction, unsigned timeout, bool link=false)
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
            containsRestrictedSubfile = false;
            // Now try to resolve them all (file/superfile)
            for (unsigned f=0; f<n; f++)
            {
                IPropertyTree &sub = *(orderedSubFiles[f]);
                sub.getProp("@name",subname.clear());
                Owned<IDistributedFile> subfile;
                subfile.setown(transaction?transaction->lookupFile(subname.str(), accessMode, timeout):parent->lookup(subname.str(), udesc, accessMode, false, false, transaction, defaultPrivilegedUser, timeout));
                if (!subfile.get())
                    subfile.setown(transaction?transaction->lookupSuperFile(subname.str(), accessMode, timeout):parent->lookupSuperFile(subname.str(), udesc, accessMode, transaction, timeout));
                // Some files are ok not to exist
                if (!subfile.get())
                {
                    CDfsLogicalFileName cdfsl;
                    cdfsl.set(subname);
                    if (cdfsl.isForeign())
                    {
                        IWARNLOG("CDistributedSuperFile: SuperFile %s's sub-file file '%s' is foreign, but missing", logicalName.get(), subname.str());
                        // Create a dummy empty superfile as a placeholder for the missing foreign file
                        Owned<IPropertyTree> dummySuperRoot = createPTree();
                        dummySuperRoot->setPropInt("@interleaved", 0);
                        subfile.setown(queryDistributedFileDirectory().createNewSuperFile(dummySuperRoot, subname));
                        if (transaction)
                        {
                            IDistributedFileTransactionExt *_transaction = queryTransactionExt(transaction);
                            _transaction->ensureFile(subfile);
                        }
                    }
                    else if (logicalName.isMulti())
                    {
                        /*
                         * implicit superfiles, can't validate subfile presence at this point,
                         * but will be caught if empty and not OPT later.
                         */
                        continue;
                    }
                    else
                        ThrowStringException(-1, "CDistributedSuperFile: SuperFile %s: corrupt subfile file '%s' cannot be found", logicalName.get(), subname.str());
                }
                containsRestrictedSubfile = containsRestrictedSubfile || subfile->isRestrictedAccess();
                subfiles.append(*subfile.getClear());
                if (link)
                    linkSubFile(f);
            }
            // This is can happen due to missing referenced foreign files, or missing files referenced via an implicit inline superfile definition
            if (subfiles.ordinality() != n)
            {
                IWARNLOG("CDistributedSuperFile: SuperFile %s's number of sub-files updated to %d", logicalName.get(), subfiles.ordinality());
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
        for (unsigned i=subfiles.ordinality();i>pos;i--)
        {
            sub = root->queryPropTree(getSubPath(path.clear(),i-1).str());
            if (!sub)
                throw MakeStringException(-1,"C(2): Corrupt subfile file part %d cannot be found",i);
            sub->setPropInt("@num",i+1);
        }
        sub = createPTree();
        sub->setPropInt("@num",pos+1);
        sub->setProp("@name",file->queryLogicalName());
        if (pos==0)
        {
            Owned<IPropertyTree> superAttrs = createPTreeFromIPT(&file->queryAttributes());
            while (superAttrs->removeProp("Protect")); // do not automatically inherit protected status
            resetFileAttr(superAttrs.getClear());
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
        for (unsigned i=pos+1; i<subfiles.ordinality(); i++)
        {
            sub = root->queryPropTree(getSubPath(path.clear(),i).str());
            if (!sub)
                throw MakeStringException(-1,"CDistributedSuperFile(2): Corrupt subfile file part %d cannot be found",i+1);
            sub->setPropInt("@num",i);
        }
        subfiles.remove(pos);
        if (pos==0)
        {
            if (subfiles.ordinality())
            {
                Owned<IPropertyTree> superAttrs = createPTreeFromIPT(&subfiles.item(0).queryAttributes());
                while (superAttrs->removeProp("Protect")); // do not automatically inherit protected status
                resetFileAttr(superAttrs.getClear());
            }
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
            for (;;) {
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

    void addPropIfCommon(IPropertyTree &target, const char *prop, const char *value)
    {
        bool ok = true;
        // add attributes that are common
        for (unsigned i=1; i<subfiles.ordinality(); i++)
        {
            IDistributedFile &file = subfiles.item(i);
            IDistributedSuperFile *sFile = file.querySuperFile();
            if (!sFile || sFile->numSubFiles(true)) // skip empty super files
            {
                const char *otherValue = file.queryAttributes().queryProp(prop);
                if (!otherValue || !streq(otherValue, value))
                {
                    ok = false;
                    break;
                }
            }
        }
        if (ok)
            target.setProp(prop, value);
    }

    IDistributedFilePart *unprotectedQueryPart(unsigned idx)
    {
        if (0 == subfiles.ordinality())
            return nullptr;
        else if ((1 == subfiles.ordinality()))
        {
            if (idx>=subfiles.item(0).numParts())
                return nullptr;
            else
                return &subfiles.item(0).queryPart(idx);
        }
        if (partscache.ordinality()==0)
            loadParts(partscache,NULL);
        if (idx>=partscache.ordinality())
            return nullptr;
        else
            return &partscache.item(idx);
    }
public:

    virtual void checkFormatAttr(IDistributedFile *sub, const char* exprefix="") override
    {
        IDistributedSuperFile *superSub = sub->querySuperFile();
        if (superSub && (0 == superSub->numSubFiles(true)))
            return;

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

    IMPLEMENT_IINTERFACE_O;

    void commonInit(CDistributedFileDirectory *_parent, IPropertyTree *_root, AccessMode _accessMode)
    {
        parent = _parent;
        root.set(_root);
        accessMode = _accessMode;
        const char *val = root->queryProp("@interleaved");
        if (val&&isdigit(*val))
            interleaved = atoi(val);
        else
            interleaved = strToBool(val)?1:0;
    }

    void init(CDistributedFileDirectory *_parent, IPropertyTree *_root, const CDfsLogicalFileName &_name, AccessMode accessMode, IUserDescriptor* user, IDistributedFileTransaction *transaction, unsigned timeout=INFINITE)
    {
        assertex(_name.isSet());
        setUserDescriptor(udesc,user);
        logicalName.set(_name);
        commonInit(_parent, _root, accessMode);
        loadSubFiles(transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IPropertyTree *_root, const CDfsLogicalFileName &_name, AccessMode accessMode, IUserDescriptor* user)
    {
        init(_parent, _root, _name, accessMode, user, NULL);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IRemoteConnection *_conn, const CDfsLogicalFileName &_name, AccessMode accessMode, IUserDescriptor* user, IDistributedFileTransaction *transaction, unsigned timeout)
    {
        conn.setown(_conn);
        init(_parent, conn->queryRoot(), _name, accessMode, user,transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, CDfsLogicalFileName &_name, AccessMode accessMode, IUserDescriptor* user, IDistributedFileTransaction *transaction)
    {
        // temp super file
        assertex(_name.isMulti());
        if (!_name.isExpanded())
            _name.expand(user);//expand wildcards
        Owned<IPropertyTree> tree = _name.createSuperTree();
        init(_parent, tree, _name, accessMode, user, transaction);
        updateFileAttrs();
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IPropertyTree *_root, AccessMode accessMode, const char *optionalName, IArrayOf<IDistributedFile> *subFiles)
    {
        commonInit(_parent, _root, accessMode);
        if (optionalName)
            logicalName.set(optionalName);
        if (subFiles)
        {
            ForEachItemIn(i,*subFiles)
                subfiles.append(OLINK(subFiles->item(i)));
        }
    }

    ~CDistributedSuperFile()
    {
        partscache.kill();
        subfiles.kill();
    }

    virtual StringBuffer &getClusterName(unsigned clusternum,StringBuffer &name) override
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

    virtual IFileDescriptor *getFileDescriptor(const char *clustername) override
    {
        CriticalBlock block (sect);
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
                        addPropIfCommon(*at, name, v);
                    }
                }
                MemoryBuffer mb;
                if (getRecordLayout(mb, "_record_layout"))
                    at->setPropBin("_record_layout", mb.length(), mb.bufferBase());
                if (getRecordLayout(mb, "_rtlType"))
                    at->setPropBin("_rtlType", mb.length(), mb.bufferBase());
                const char *ecl = file.queryAttributes().queryProp("ECL");
                if (!isEmptyString(ecl))
                    addPropIfCommon(*at, "ECL", ecl);
                IPropertyTree *_remoteStoragePlane = file.queryAttributes().queryPropTree("_remoteStoragePlane");
                if (_remoteStoragePlane)
                {
                    // NB: CDistributedSuperFile sub-files in different environments are not permitted
                    if (!at->hasProp("_remoteStoragePlane"))
                       at->setPropTree("_remoteStoragePlane", LINK(_remoteStoragePlane));
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
            IWARNLOG("getFileDescriptor: Unsupported interleave value (1)");
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
        // turn off dirperpart (if present) because the super descriptor has already encoded the common parent dir + tails above.
        FileDescriptorFlags flags = static_cast<FileDescriptorFlags>(fdesc->queryProperties().getPropInt("@flags"));
        flags &= ~FileDescriptorFlags::dirperpart;
        fdesc->queryProperties().setPropInt("@flags", static_cast<int>(flags));
        fdesc->queryProperties().setPropInt("@accessMode", static_cast<int>(accessMode));
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

    virtual unsigned numParts() override
    {
        CriticalBlock block(sect);
        unsigned ret=0;
        ForEachItemIn(i,subfiles)
            ret += subfiles.item(i).numParts();
        return ret;
    }

    virtual IDistributedFilePart &queryPart(unsigned idx) override
    {
        CriticalBlock block(sect);
        IDistributedFilePart *part = unprotectedQueryPart(idx);
        if (nullptr == part)
            throwUnexpectedX("CDistributedSuperFile::queryPart out of range");
        return *part;
    }

    virtual IDistributedFilePart* getPart(unsigned idx) override
    {
        CriticalBlock block(sect);
        return LINK(unprotectedQueryPart(idx));
    }

    virtual IDistributedFilePartIterator *getIterator(IDFPartFilter *filter=NULL) override
    {
        CriticalBlock block(sect);
        if (subfiles.ordinality()==1)
            return subfiles.item(0).getIterator(filter);
        CDistributedFilePartIterator *ret = new CDistributedFilePartIterator();
        loadParts(ret->queryParts(),filter);
        return ret;
    }

    virtual void rename(const char *_logicalname,IUserDescriptor *user) override
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


    virtual const char *queryDefaultDir() override
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

    virtual const char *queryPartMask() override
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

    virtual void attach(const char *_logicalname,IUserDescriptor *user) override
    {
        assertex(!conn.get()); // already attached
        CriticalBlock block (sect);
        StringBuffer tail;
        StringBuffer lfn;
        logicalName.set(_logicalname);
        try
        {
            checkLogicalName(logicalName,user,true,true,false,"attach");
            parent->addEntry(logicalName,root.getClear(),true,false);
            conn.clear();
            CFileLock fcl;
            verifyex(fcl.init(logicalName, DXB_SuperFile, RTM_LOCK_READ, defaultTimeout, "CDistributedSuperFile::attach"));
            conn.setown(fcl.detach());
            assertex(conn.get()); // must have been attached
            root.setown(conn->getRoot());
            loadSubFiles(NULL, 0, true);
        }
        catch (IException *e)
        {
            EXCLOG(e, "CDistributedSuperFile::attach");
            logicalName.clear();
            throw;
        }
    }

    virtual void detach(unsigned timeoutMs=INFINITE, ICodeContext *ctx=NULL) override
    {
        assertex(conn.get()); // must be attached
        CriticalBlock block(sect);
        checkModify("CDistributedSuperFile::detach");
        StringBuffer reason;
        if (checkOwned(reason))
            throw MakeStringException(-1, "detach: %s", reason.str());
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
        clearSuperOwners(timeoutMs, ctx);
        writeLock.clear();
        root.setown(closeConnection(true));
        updateFS(logicalName, parent->queryDefaultTimeout());
        logicalName.clear();
    }

    virtual bool existsPhysicalPartFiles(unsigned short port) override
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            if (!f.existsPhysicalPartFiles(port))
                return false;
        }
        return true;
    }

    virtual bool renamePhysicalPartFiles(const char *newlfn,const char *cluster,IMultiException *mexcept,const char *newbasedir) override
    {
        throw MakeStringException(-1,"renamePhysicalPartFiles not supported for SuperFiles");
        return false;
    }

    void serialize(MemoryBuffer &mb)
    {
        UNIMPLEMENTED; // not yet needed
    }

    virtual unsigned numCopies(unsigned partno) override
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

    virtual __int64 getFileSize(bool allowphysical,bool forcephysical) override
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

    virtual __int64 getDiskSize(bool allowphysical,bool forcephysical) override
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
        if (ret==-1)
        {
            ret = 0;
            ForEachItemIn(i,subfiles)
            {
                IDistributedFile &subFile = subfiles.item(i);
                __int64 rc = subFile.queryAttributes().getPropInt64("@recordCount", -1);
                if (rc == -1)
                {
                    IDistributedSuperFile *super = subFile.querySuperFile();

                    // if regular file or non-empty super, must have a @recordCount to aggregate a total
                    if ((nullptr == super) || (super->numSubFiles()>0))
                        return -1;
                }
                else
                    ret += rc;
            }
        }
        return ret;
    }

    virtual bool getFileCheckSum(unsigned &checkSum) override
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

    virtual IDistributedSuperFile *querySuperFile() override
    {
        return this;
    }

    virtual IDistributedFile &querySubFile(unsigned idx,bool sub) override
    {
        CriticalBlock block (sect);
        if (sub)
        {
            unsigned subfilen = idx;
            ForEachItemIn(i,subfiles)
            {
                IDistributedFile &f=subfiles.item(i);
                IDistributedSuperFile *super = f.querySuperFile();
                if (super)
                {
                    unsigned ns = super->numSubFiles(true);
                    if (ns>subfilen)
                        return super->querySubFile(subfilen,true);
                    subfilen -= ns;
                }
                else if (subfilen--==0)
                    return f;
            }
            throw makeStringExceptionV(-1,"CDistributedSuperFile::querySubFile(%u) for superfile %s - subfile doesn't exist ", idx, logicalName.get());
        }
        else
            return subfiles.item(idx);
    }

    virtual IDistributedFile *querySubFileNamed(const char *name, bool sub) override
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

    virtual IDistributedFile *getSubFile(unsigned idx,bool sub) override
    {
        CriticalBlock block (sect);
        return LINK(&querySubFile(idx,sub));
    }

    virtual unsigned numSubFiles(bool sub) override
    {
        CriticalBlock block (sect);
        unsigned ret = 0;
        if (sub) {
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                IDistributedSuperFile *super = f.querySuperFile();
                if (super)
                    ret += super->numSubFiles(sub);
                else
                    ret++;
            }
        }
        else
            ret = subfiles.ordinality();
        return ret;
    }

    virtual bool getFormatCrc(unsigned &crc) override
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

    virtual bool getRecordLayout(MemoryBuffer &layout, const char *attrname) override
    {
        layout.clear();
        if (queryAttributes().getPropBin(attrname, layout))
            return true;
        bool found = false;
        ForEachItemIn(i,subfiles) {
            MemoryBuffer b;
            if (subfiles.item(i).getRecordLayout(found?b:layout, attrname)) {
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

    virtual bool getRecordSize(size32_t &rsz) override
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

    virtual bool isInterleaved() override
    {
        return interleaved!=0;
    }

    virtual IDistributedFile *querySubPart(unsigned partidx,unsigned &subfileidx) override
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

    virtual unsigned getPositionPart(offset_t pos, offset_t &base) override
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

    virtual IDistributedFileIterator *getSubFileIterator(bool supersub=false) override
    {
        CriticalBlock block (sect);
        return new cSubFileIterator(subfiles,supersub);
    }

    virtual void lockFileAttrLock(CFileAttrLock & attrLock) override
    {
        if (!attrLock.init(logicalName, DXB_SuperFile, RTM_LOCK_WRITE, conn, defaultTimeout, "CDistributedFile::lockFileAttrLock"))
        {
            // In unlikely event File/Attr doesn't exist, must ensure created, commited and root connection is reloaded.
            verifyex(attrLock.init(logicalName, DXB_SuperFile, RTM_LOCK_WRITE|RTM_CREATE_QUERY, conn, defaultTimeout, "CDistributedFile::lockFileAttrLock"));
            attrLock.commit();
            conn->commit();
            conn->reload();
            root.setown(conn->getRoot());
        }
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
        IPropertyTree &attrs = queryAttributes();
        attrs.removeProp("@size");
        attrs.removeProp("@compressedSize");
        attrs.removeProp("@uncompressedSize");
        attrs.removeProp("@checkSum");
        attrs.removeProp("@recordCount");   // recordCount not currently supported by superfiles
        attrs.removeProp("@formatCrc");     // formatCrc set if all consistant
        attrs.removeProp("@recordSize");    // record size set if all consistant
        attrs.removeProp("_record_layout"); // legacy info - set if all consistent
        attrs.removeProp("_rtlType");       // new info - set if all consistent
        attrs.removeProp("@maxSkew");
        attrs.removeProp("@minSkew");
        attrs.removeProp("@maxSkewPart");
        attrs.removeProp("@minSkewPart");

        __int64 fs = getFileSize(false,false);
        if (fs!=-1)
            attrs.setPropInt64("@size",fs);
        if (isCompressed(NULL))
        {
            fs = getDiskSize(false,false);
            if (fs!=-1)
                attrs.setPropInt64("@compressedSize",fs);
        }
        unsigned checkSum;
        if (getFileCheckSum(checkSum))
            attrs.setPropInt64("@checkSum", checkSum);
        __int64 rc = getRecordCount();
        if (rc!=-1)
            attrs.setPropInt64("@recordCount",rc);
        unsigned fcrc;
        if (getFormatCrc(fcrc))
            attrs.setPropInt("@formatCrc", fcrc);
        size32_t rsz;
        if (getRecordSize(rsz))
            attrs.setPropInt("@recordSize", rsz);
        MemoryBuffer mb;
        if (getRecordLayout(mb, "_record_layout"))
            attrs.setPropBin("_record_layout", mb.length(), mb.bufferBase());
        if (getRecordLayout(mb, "_rtlType"))
            attrs.setPropBin("_rtlType", mb.length(), mb.bufferBase());
        const char *kind = nullptr;
        Owned<IDistributedFileIterator> subIter = getSubFileIterator(true);
        ForEach(*subIter)
        {
            IDistributedFile &file = subIter->query();
            const char *curKind = file.queryAttributes().queryProp("@kind");
            if (!kind)
                kind = curKind;
            else if (!strsame(kind, curKind))
            {
                kind = nullptr;
                break;
            }
        }
        if (kind)
            attrs.setProp("@kind", kind);
    }

    void updateParentFileAttrs(IDistributedFileTransaction *transaction)
    {
        Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
        StringBuffer pname;
        ForEach(*iter) {
            iter->query().getProp("@name",pname.clear());
            Owned<IDistributedSuperFile> psfile = transaction?transaction->lookupSuperFile(pname.str(), AccessMode::writeMeta):
                queryDistributedFileDirectory().lookupSuperFile(pname.str(), udesc, AccessMode::writeMeta, NULL);
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

    virtual void validate() override
    {
        unsigned numSubfiles = root->getPropInt("@numsubfiles",0);
        if (numSubfiles)
        {
            Owned<IPropertyTreeIterator> treeIter = root->getElements("SubFile");
            unsigned subFileCount = 0;
            ForEach(*treeIter)
            {
                IPropertyTree & st = treeIter->query();
                StringBuffer subfilename;
                st.getProp("@name", subfilename);
                if (!parent->exists(subfilename.str(), NULL))
                    throw MakeStringException(-1, "Logical subfile '%s' doesn't exists!", subfilename.str());

                if (!parent->isSuperFile(subfilename.str()))
                    if (!parent->existsPhysical(subfilename.str(), NULL))
                    {
                        const char * logicalName = queryLogicalName();
                        throw MakeStringException(-1, "Some physical parts do not exists, for logical file : %s",(isEmptyString(logicalName) ? "[unattached]" : logicalName));
                    }
                subFileCount++;
            }
            if (numSubfiles != subFileCount)
                throw MakeStringException(-1, "The value of @numsubfiles (%d) is not equal to the number of SubFile items (%d)!",numSubfiles, subFileCount);
        }
    }
    virtual bool isRestrictedAccess() override
    {
        // This ensures restriction applies even if superfile is unrestricted but subfiles are.
        // However, this also means that the superfile will show as "Restricted" in ECL Watch and whenever the user tries to unset the flag
        // it will appear to reset to Restricted.
        return containsRestrictedSubfile;
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
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames1.item(d1), AccessMode::writeMeta);
            if (!doRemoveSubFile(sub->queryLogicalName(), transaction))
                return false;
        }
        ForEachItemIn(d2,subnames2) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames2.item(d2), AccessMode::writeMeta);
            if (!file->doRemoveSubFile(sub->queryLogicalName(), transaction))
                return false;
        }
        // Add files swapped
        ForEachItemIn(a1,subnames1) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames1.item(a1), AccessMode::writeMeta);
            file->doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        ForEachItemIn(a2,subnames2) {
            Owned<IDistributedFile> sub = transaction->lookupFile(subnames2.item(a2), AccessMode::writeMeta);
            doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        return true;
    }

public:
    virtual void addSubFile(const char * subfile,
                            bool before=false,              // if true before other
                            const char *other=NULL,     // in NULL add at end (before=false) or start(before=true)
                            bool addcontents=false,
                            IDistributedFileTransaction *transaction=NULL
                   ) override
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
            localtrans.set(queryTransactionExt(transaction));
        }
        else
            localtrans.setown(new CDistributedFileTransaction(udesc, this));
        localtrans->ensureFile(this);

        if (addcontents)
        {
            localtrans->descend();
            StringArray subs;
            Owned<IDistributedSuperFile> sfile = localtrans->lookupSuperFile(subfile, AccessMode::writeMeta);
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
                               IDistributedFileTransaction *transaction) override
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
            localtrans.set(queryTransactionExt(transaction));
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
                                     IDistributedFileTransaction *transaction) override
    {
        CriticalBlock block (sect);
        checkModify("removeOwnedSubFiles");
        partscache.kill();

        // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
        Linked<IDistributedFileTransactionExt> localtrans;
        if (transaction)
        {
            localtrans.set(queryTransactionExt(transaction));
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
                                IDistributedFileTransaction *transaction) override
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
            localtrans.set(queryTransactionExt(transaction));
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

    virtual void savePartsAttr(bool force) override
    {
    }

    void fillClustersCache()
    {
        if (clusterscache.ordinality()==0)
        {
            ForEachItemIn(i,subfiles)
            {
                StringArray clusters;
                IDistributedFile &f=subfiles.item(i);
                Owned<IFileDescriptor> fdesc = f.getFileDescriptor();
                StringArray clusterNames;
                f.getClusterNames(clusterNames);
                unsigned nc = f.numClusters();
                for(unsigned j=0;j<nc;j++)
                {
                    const char *name = clusterNames.item(j);
                    if (clusterscache.find(name)==NotFound)
                    {
                        IClusterInfo *cluster;
                        if (f.querySuperFile()) // because super descriptor has a combined single cluster
                            cluster = createClusterInfo(name, f.queryClusterGroup(j), f.queryPartDiskMapping(j));
                        else
                            cluster = LINK(fdesc->queryClusterNum(j));
                        clusterscache.append(*cluster);
                    }
                }
            }
        }
    }

    virtual unsigned getClusterNames(StringArray &clusters) override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.getNames(clusters);
    }

    virtual unsigned numClusters() override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.ordinality();
    }

    virtual unsigned findCluster(const char *clustername) override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.find(clustername);
    }

    virtual ClusterPartDiskMapSpec &queryPartDiskMapping(unsigned clusternum) override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.queryPartDiskMapping(clusternum);
    }

    virtual void updatePartDiskMapping(const char *clustername,const ClusterPartDiskMapSpec &spec) override
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

    virtual IGroup *queryClusterGroup(unsigned clusternum) override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.queryGroup(clusternum);
    }

    virtual StringBuffer &getClusterGroupName(unsigned clusternum, StringBuffer &name) override
    {
        CriticalBlock block (sect);
        fillClustersCache();
        return clusterscache.item(clusternum).getGroupName(name, &queryNamedGroupStore());
    }

    virtual void addCluster(const char *clustername,const ClusterPartDiskMapSpec &mspec) override
    {
        if (!clustername||!*clustername)
            return;
        CriticalBlock block (sect);
        clusterscache.clear();
        subfiles.item(0).addCluster(clustername,mspec);
    }

    virtual bool removeCluster(const char *clustername) override
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

    virtual void setPreferredClusters(const char *clusters) override
    {
        CriticalBlock block (sect);
        clusterscache.clear();
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.setPreferredClusters(clusters);
        }
    }

    virtual bool checkClusterCompatible(IFileDescriptor &fdesc, StringBuffer &err) override
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


    virtual void setSingleClusterOnly() override
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.setSingleClusterOnly();
        }
    }


    virtual void enqueueReplicate() override
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.enqueueReplicate();
        }
    }

    virtual bool getAccessedTime(CDateTime &dt) override
    {
        bool set=false;
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            if (!set)
                set = f.getAccessedTime(dt);
            else {
                CDateTime cmp;
                if (f.getAccessedTime(cmp)) {
                    if (cmp.compare(dt)>0)
                        dt.set(cmp);
                }
            }
        }
        return set;
    }

    virtual void setAccessedTime(const CDateTime &dt) override
    {
        {
            CriticalBlock block (sect);
            ForEachItemIn(i,subfiles) {
                IDistributedFile &f=subfiles.item(i);
                f.setAccessedTime(dt);
            }
        }
    }

    virtual bool getSkewInfo(unsigned &maxSkew, unsigned &minSkew, unsigned &maxSkewPart, unsigned &minSkewPart, bool calculateIfMissing) override
    {
        return false;
    }

    virtual void getCost(const char * cluster, cost_type & atRestCost, cost_type & accessCost) override
    {
        CriticalBlock block (sect);
        ForEachItemIn(i,subfiles)
        {
            cost_type tmpAtRestcost, tmpAccessCost;
            IDistributedFile &f = subfiles.item(i);
            f.getCost(cluster, tmpAtRestcost, tmpAccessCost);
            atRestCost += tmpAtRestcost;
            accessCost += tmpAccessCost;
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
            IERRLOG("Multi filenames not supported in Dali DFS Part %d of %s",_part+1,_parent.queryLogicalName());
        overridename.set(pd->queryOverrideName());
        setAttr(*pd->getProperties());
    }
    else
        IERRLOG("CDistributedFilePart::CDistributedFilePart no IPartDescriptor for part");
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
                Owned<IFileIO> partFileIO = partfile->open(IFOread);
                if (partFileIO)
                {
                    Owned<ICompressedFileIO> compressedIO = createCompressedFileReader(partFileIO);
                    if (compressedIO)
                        ret = compressedIO->size();
                    else
                        throw new CDFS_Exception(DFSERR_PhysicalCompressedPartInvalid, partfile->queryFilename());
                }
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
        IERRLOG("%s", err);
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
            else if (!isEmptyString(defdir))
                addPathSepChar(dir.append(defdir)).append(odir);
        }
        else
            dir.append(defdir);
    }
    if (dir.length()==0)
        IERRLOG("IDistributedFilePart::getPartDirectory unable to determine part directory");
    else
    {
        parent.adjustClusterDir(partIndex,copy,dir);

// NB: could be compiled in bare-metal,
// but bare-metal components without a config would complain as this calls getGlobalConfig()
        unsigned cn = copyClusterNum(copy, nullptr);
        IClusterInfo &cluster = parent.clusters.item(cn);
        const char *planeName = cluster.queryGroupName();
        if (!isEmptyString(planeName))
        {
            Owned<IStoragePlane> plane;
#ifdef _CONTAINERIZED
            plane.setown(getDataStoragePlane(planeName, false));
#else
            // this is for the remote useDafilesrv case,
            // where the remote storage plane may be needed to remap/stripe, see below.
            IPropertyTree *remoteStoragePlane = parent.queryAttributes().queryPropTree("_remoteStoragePlane");
            if (remoteStoragePlane)
                plane.setown(createStoragePlane(remoteStoragePlane));
#endif
            if (plane)
            {
                StringBuffer planePrefix(plane->queryPrefix());
                Owned<IStoragePlaneAlias> alias = plane->getAliasMatch(parent.accessMode);
                if (alias)
                {
                    StringBuffer tmp;
                    StringBuffer newPlanePrefix(alias->queryPrefix());
                    if (setReplicateDir(dir, tmp, false, planePrefix, newPlanePrefix))
                    {
                        planePrefix.swapWith(newPlanePrefix);
                        dir.swapWith(tmp);
                    }
                }

                StringBuffer stripeDir;
                unsigned numStripedDevices = parent.queryPartDiskMapping(cn).numStripedDevices;
                addStripeDirectory(stripeDir, dir, planePrefix, partIndex, parent.lfnHash, numStripedDevices);
                if (!stripeDir.isEmpty())
                    dir.swapWith(stripeDir);
            }
        }
        if (parent.hasDirPerPart())
            addPathSepChar(dir).append(partIndex+1); // part subdir 1 based
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
    DBGLOG("CDistributedFilePart::queryAttributes missing part attributes");
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

class CNamedGroupIterator: implements INamedGroupIterator, public CInterface
{
    Owned<IPropertyTreeIterator> pe;
    Linked<IRemoteConnection> conn;
    Linked<IGroup> matchgroup;
    bool exactmatch;


    bool match();

public:
    IMPLEMENT_IINTERFACE;
    CNamedGroupIterator(IRemoteConnection *_conn,IGroup *_matchgroup=NULL,bool _exactmatch=false)
        : conn(_conn)
    {
        if (_matchgroup)
        {
            // the matchgroup may contain ports, but they are never part of published groups and are not to be used for matching
            SocketEndpointArray epa;
            for (unsigned i=0; i<_matchgroup->ordinality(); i++)
            {
                SocketEndpoint ep = _matchgroup->queryNode(i).endpoint();
                ep.port = 0;
                epa.append(ep);
            }
            matchgroup.setown(createIGroup(epa));
        }
        exactmatch = _exactmatch;
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

GroupType translateGroupType(const char *groupType)
{
    if (!groupType)
        return grp_unknown;
    if (strieq(groupType, "Thor"))
        return grp_thor;
    else if (strieq(groupType, "Roxie"))
        return grp_roxie;
    else if (strieq(groupType, "hthor"))
        return grp_hthor;
    else if (strieq(groupType, "dropzone"))
        return grp_dropzone;
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
    : name(_name), groupType(grp_unknown), exception(_exception)
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

static unsigned loadGroup(const IPropertyTree *groupTree, SocketEndpointArray &epa, GroupType *type, StringAttr *groupDir)
{
    if (type)
        *type = translateGroupType(groupTree->queryProp("@kind"));
    if (groupDir)
    {
        groupDir->set(groupTree->queryProp("@dir"));
        if (groupDir->isEmpty())
            groupDir->set(queryBaseDirectory(*type));
    }
    Owned<IPropertyTreeIterator> pe = groupTree->getElements("Node");
    ForEach(*pe)
    {
        const char *host = pe->query().queryProp("@ip");
        SocketEndpoint ep(host);
        if (ep.isNull())
        {
            IWARNLOG("loadGroup: failed to resolve host '%s'", host);
            return 0;
        }
        epa.append(ep);
    }
    return epa.ordinality();
}

class CNamedGroupStore: implements INamedGroupStore, public CInterface
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
        groupType = grp_unknown;
        if (!gname.length())
            return nullptr;
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
                return createIGroup(epa);
        }
        StringBuffer range;
        StringBuffer parent;
        if (decodeChildGroupName(gname.str(),parent,range)) {
            gname.clear().append(parent);
            logicalgroupname = gname.str();
        }
        StringAttr groupdir;
        GroupType type = grp_unknown;
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
                    if (!entry.group)  //cache entry of a deleted groupname
                        return nullptr;
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
                            return nullptr;
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
                        return nullptr;
                    }
                }
                Owned<IPropertyTree> groupTree = getNamedPropTree(conn->queryRoot(), "Group", "@name", gname.str(), true);
                if (!groupTree || !loadGroup(groupTree, epa, &type, &groupdir))
                    return nullptr;
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

    IPropertyTree *doAddHosts(CConnectLock &connlock, const char *name, const std::vector<std::string> &hosts, bool cluster, const char *dir)
    {
        IPropertyTree *val = createPTree("Group");
        val->setProp("@name",name);
        if (cluster)
            val->setPropBool("@cluster", true);
        if (dir)
            val->setProp("@dir",dir);
        for (auto &hostOrIpRange: hosts)
        {
            SocketEndpointArray epa;
            epa.fromText(hostOrIpRange.c_str(), 0);
            if (epa.ordinality()>1)
            {
                ForEachItemIn(e, epa)
                {
                    StringBuffer ipStr;
                    epa.item(e).getHostText(ipStr);
                    IPropertyTree *n = val->addPropTree("Node");
                    n->setProp("@ip", ipStr);
                }
            }
            else
            {
                IPropertyTree *n = val->addPropTree("Node");
                n->setProp("@ip", hostOrIpRange.c_str());
            }
        }
        val = connlock.conn->queryRoot()->addPropTree("Group",val);
        return LINK(val);
    }

    void doadd(CConnectLock &connlock,const char *name,IGroup *group,bool cluster,const char *dir)
    {
        if (!group)
            return;
        Owned<INodeIterator> gi = group->getIterator();
        if (gi->first())
        {
            StringBuffer ipStr;
            std::vector<std::string> ips;
            while (true)
            {
                gi->query().endpoint().getHostText(ipStr.clear());
                ips.push_back(ipStr.str());
                if (!gi->next())
                    break;
            }
            ::Release(doAddHosts(connlock, name, ips, cluster, dir));
        }
    }

    void addGroup(const char *logicalgroupname, const std::vector<std::string> &hosts, bool cluster, const char *dir, GroupType groupType, bool overwrite)
    {
        dbgassertex(hosts.size());
        StringBuffer name(logicalgroupname);
        name.toLowerCase();
        name.trim();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name.str());
        CConnectLock connlock("CNamedGroup::add", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        if (!overwrite && connlock.conn->queryRoot()->hasProp(prop.str()))
            return;
        connlock.conn->queryRoot()->removeProp(prop.str());
        if (0 == hosts.size())
            return;
        Owned<IPropertyTree> groupTree = doAddHosts(connlock, name.str(), hosts, cluster, dir);
        SocketEndpointArray eps;
        if (!loadGroup(groupTree, eps, nullptr, nullptr))
        {
            IWARNLOG("CNamedGroupStore.add: failed to add group '%s', due to unresolved hosts", name.str());
            return;
        }
        Owned<IGroup> group = createIGroup(eps);
        {
            CriticalBlock block(cachesect);
            cache.kill();
            cache.append(*new CNamedGroupCacheEntry(group, name, dir, groupType));
        }
    }

    virtual void addUnique(IGroup *group,StringBuffer &lname, const char *dir) override
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
        for (;;) {
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

    IGroup *lookup(const char *logicalgroupname, StringBuffer &dir, GroupType &groupType)
    {
        return dolookup(logicalgroupname, NULL, &dir, groupType);
    }

    virtual IGroup *lookup(const char *logicalgroupname) override
    {
        GroupType dummy;
        return dolookup(logicalgroupname, NULL, NULL, dummy);
    }

    virtual INamedGroupIterator *getIterator() override
    {
        CConnectLock connlock("CNamedGroup::getIterator", SDS_GROUPSTORE_ROOT, false, true, false, defaultTimeout);
        return new CNamedGroupIterator(connlock.conn); // links connection
    }

    virtual INamedGroupIterator *getIterator(IGroup *match,bool exact) override
    {
        CConnectLock connlock("CNamedGroup::getIterator", SDS_GROUPSTORE_ROOT, false, false, false, defaultTimeout);
        return new CNamedGroupIterator(connlock.conn,match,exact); // links connection
    }

    virtual void add(const char *logicalgroupname, const std::vector<std::string> &hosts, bool cluster, const char *dir, GroupType groupType) override
    {
        addGroup(logicalgroupname, hosts, cluster, dir, groupType, true);
    }

    virtual void ensure(const char *logicalgroupname, const std::vector<std::string> &hosts, bool cluster, const char *dir, GroupType groupType) override
    {
        addGroup(logicalgroupname, hosts, cluster, dir, groupType, false);
    }

    virtual void ensureNasGroup(size32_t size) override
    {
        std::vector<std::string> hosts;
        for (unsigned n=0; n<size; n++)
            hosts.push_back("localhost");
        VStringBuffer nasGroupName("__nas__%u", size);
        ensure(nasGroupName, hosts, false, nullptr, grp_unknown);
    }

    virtual StringBuffer &getNasGroupName(StringBuffer &groupName, size32_t size) const override
    {
        return groupName.append("__nas__").append(size);
    }

    virtual unsigned removeNode(const char *logicalgroupname, const char *nodeToRemove) override
    {
        StringBuffer name(logicalgroupname);
        name.toLowerCase();
        name.trim();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name.str());
        CConnectLock connlock("CNamedGroup::add", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);

        Owned<IPropertyTree> groupTree = getNamedPropTree(connlock.conn->queryRoot(), "Group", "@name", name, true);
        if (!groupTree)
            return 0;
        SocketEndpointArray epa;
        if (!loadGroup(groupTree, epa, nullptr, nullptr))
            return 0;

        unsigned numNodes = epa.ordinality();
        Owned<IGroup> group = createIGroup(epa);
        SocketEndpoint removeEp(nodeToRemove);

        unsigned removeCount = 0;
        while (removeCount != numNodes)
        {
            rank_t r = group->rank(removeEp);
            if (RANK_NULL == r)
                break;
            group.setown(group->remove(r));
            VStringBuffer xpath("Node[%u]", r+1); // 1 based
            verifyex(groupTree->removeProp(xpath)); // remove corresponding position in IPT (in Dali)

            ++removeCount;
        }
        if (0 == removeCount)
            return 0;

        const char *groupDir = groupTree->queryProp("@dir");
        GroupType groupType = translateGroupType(groupTree->queryProp("@kind"));

        CriticalBlock block(cachesect);
        cache.kill();
        cache.append(*new CNamedGroupCacheEntry(group, name, groupDir, groupType));
        return removeCount;
    }

    virtual void remove(const char *logicalgroupname) override
    {
        StringBuffer name(logicalgroupname);
        name.toLowerCase();
        name.trim();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name.str());
        CConnectLock connlock("CNamedGroup::add", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        connlock.conn->queryRoot()->removeProp(prop.str());
        {
            CriticalBlock block(cachesect);
            cache.kill();
        }
    }

    virtual bool find(IGroup *grp, StringBuffer &gname, bool add) override
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

    virtual void swapNode(const char *fromHost, const char *toHost) override
    {
        SocketEndpoint from(fromHost);
        SocketEndpoint to(toHost);
        if (from.ipequals(to))
            return;
        CConnectLock connlock("CNamedGroup::swapNode", SDS_GROUPSTORE_ROOT, true, false, false, defaultTimeout);
        Owned<IPropertyTreeIterator> pe  = connlock.conn->queryRoot()->getElements("Group");
        ForEach(*pe)
        {
            IPropertyTree &group = pe->query();
            const char *kind = group.queryProp("@kind");
            if (kind && streq("Spare", kind))
                continue;
            StringBuffer name;
            group.getProp("@name", name);

            SocketEndpointArray eps;
            if (!loadGroup(&group, eps, nullptr, nullptr))
            {
                IWARNLOG("swapNode: failed to load group: '%s'", name.str());
                return;
            }

            unsigned epsPos = 0;
            while (true)
            {
                if (epsPos == eps.ordinality())
                    break;
                if (from == eps.item(epsPos))
                {
                    VStringBuffer xpath("Node[%u]/@ip", epsPos+1);
                    group.setProp(xpath, toHost);
                    PROGLOG("swapNode swapping %s for %s in group %s", fromHost, toHost, name.str());
                    unsigned nodesSwapped = group.getPropInt("@nodesSwapped");
                    group.setPropInt("@nodesSwapped", nodesSwapped+1);
                }
                ++epsPos;
            }
        }
        CriticalBlock block(cachesect);
        cache.kill();
    }

    virtual unsigned setDefaultTimeout(unsigned timems) override
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
    }

    virtual unsigned setRemoteTimeout(unsigned timems) override
    {
        unsigned ret = defaultRemoteTimeout;
        defaultRemoteTimeout = timems;
        return ret;
    }

    virtual void resetCache() override
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

static std::atomic<CNamedGroupStore *> groupStore{nullptr};
static CriticalSection groupsect;


bool CNamedGroupIterator::match()
{
    if (conn.get()) {
        if (matchgroup.get()) {
            if (!groupStore.load())
                return false;
            const char *name = pe->query().queryProp("@name");
            if (!name||!*name)
                return false;
            GroupType dummy;
            Owned<IGroup> lgrp = groupStore.load()->dolookup(name, conn, NULL, dummy);
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

INamedGroupStore &queryNamedGroupStore()
{
    if (!groupStore.load())
    {
        CriticalBlock block(groupsect);
        if (!groupStore.load())
            groupStore.store(new CNamedGroupStore());
    }
    return *(groupStore.load());
}

// --------------------------------------------------------

IDistributedFile *CDistributedFileDirectory::lookup(const char *_logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool privilegedUser, unsigned timeout)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    return lookup(logicalname, user, accessMode, hold, lockSuperOwner, transaction, privilegedUser, timeout);
}

IDistributedFile *CDistributedFileDirectory::dolookup(CDfsLogicalFileName &_logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName *logicalname = &_logicalname;
    if (logicalname->isMulti())
        // don't bother checking because the sub file creation will
        return new CDistributedSuperFile(this, *logicalname, accessMode, user, transaction); // temp superfile
    if (strchr(logicalname->get(), '*')) // '*' only wildcard supported. NB: This is a temporary fix (See: HPCC-12523)
        throw MakeStringException(-1, "Invalid filename lookup: %s", logicalname->get());
    Owned<IDfsLogicalFileNameIterator> redmatch;
    for (;;)
    {
        checkLogicalName(*logicalname,user,true,isWrite(accessMode),true,NULL);
        if (logicalname->isExternal()) {
            Owned<IFileDescriptor> fDesc = getExternalFileDescriptor(logicalname->get());
            if (!fDesc)
                return NULL;
            return queryDistributedFileDirectory().createExternal(fDesc, logicalname->get());
        }
        if (logicalname->isForeign()) {
            IDistributedFile * ret = getFile(logicalname->get(), accessMode, user, NULL);
            if (ret)
                return ret;
        }
        else {
            unsigned start = 0;
            for (;;) {
                CFileLock fcl;
                unsigned mode = RTM_LOCK_READ | RTM_SUB;
                if (hold) mode |= RTM_LOCK_HOLD;
                CTimeMon tm(timeout);
                if (!fcl.init(*logicalname, mode, timeout, "CDistributedFileDirectory::lookup"))
                    break;
                CFileSuperOwnerLock superOwnerLock;
                if (lockSuperOwner)
                {
                    unsigned remaining;
                    tm.timedout(&remaining);
                    verifyex(superOwnerLock.initWithFileLock(*logicalname, remaining, "CDistributedFileDirectory::dolookup(SuperOwnerLock)", fcl, mode));
                }
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
                    CDistributedFile *ret = new CDistributedFile(this,fcl.detach(),*logicalname,accessMode,user);  // found
                    ret->setSuperOwnerLock(superOwnerLock.detach());
                    ret->checkWriteSync();
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
                    CDistributedSuperFile *ret = new CDistributedSuperFile(this, fcl.detach(), *logicalname, accessMode, user, transaction, SDS_SUB_LOCK_TIMEOUT);
                    ret->setSuperOwnerLock(superOwnerLock.detach());
                    return ret;
                }
                catch (ISDSException *e)
                {
                    elapsed = msTick()-start;
                    if ((e->errorCode()!=SDSExcpt_LockTimeout)||(elapsed>((timeout==INFINITE)?SDS_CONNECT_TIMEOUT:timeout)))
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

IDistributedFile *CDistributedFileDirectory::lookup(CDfsLogicalFileName &logicalname, IUserDescriptor *user, AccessMode accessMode, bool hold, bool lockSuperOwner, IDistributedFileTransaction *transaction, bool privilegedUser, unsigned timeout)
{
    if (logicalname.isRemote())
    {
        PrintStackReport(); // to help locate contexts it was called in
        throw new CDFS_Exception(DFSERR_InvalidRemoteFileContext, logicalname.get());
    }
    Owned <IDistributedFile>distributedFile = dolookup(logicalname, user, accessMode, hold, lockSuperOwner, transaction, timeout);
    // Restricted access is currently designed to stop users viewing sensitive information. It is not designed to stop users deleting or overwriting existing restricted files
    if (!isWrite(accessMode) && distributedFile && distributedFile->isRestrictedAccess() && !privilegedUser)
        throw new CDFS_Exception(DFSERR_RestrictedFileAccessDenied,logicalname.get());
    return distributedFile.getClear();
}

IDistributedSuperFile *CDistributedFileDirectory::lookupSuperFile(const char *_logicalname, IUserDescriptor *user, AccessMode accessMode, IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    IDistributedFile *file = dolookup(logicalname, user, accessMode, false, false, transaction, timeout);
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
    Owned<IPropertyTree> tree = getFileTree(logicalname, user, foreigndali,timeout, GetFileTreeOpts::appendForeign);
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
        // Restricted access is currently designed to stop users viewing sensitive information. Assuming privileged user rights to allow
        // exists() operation to succeed regardless of user rights
        Owned<IDistributedFile> file = lookup(_logicalname, user, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, defaultTimeout);
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
    // Restricted access is currently designed to stop users viewing sensitive information. Assuming privileged user rights to allow
    // existsPhysical() operation to succeed regardless of user rights
    Owned<IDistributedFile> file = lookup(_logicalname, user, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, defaultTimeout);
    if (!file)
        return false;
    return file->existsPhysicalPartFiles(0);
}

IDistributedFile *CDistributedFileDirectory::createNew(IFileDescriptor *fdesc, const char *optName)
{
    CDistributedFile *ret = new CDistributedFile(this, fdesc, NULL, false);
    if (optName)
        ret->setLogicalName(optName);
    return ret;
}

IDistributedFile *CDistributedFileDirectory::createExternal(IFileDescriptor *fdesc, const char *name)
{
    CDistributedFile *dFile = new CDistributedFile(this, fdesc, NULL, true);
    dFile->setLogicalName(name);
    return dFile;
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
        : parent(_parent), user(_user), interleaved(_interleaved), created(false)
    {
        tracing.appendf("CreateSuperFile: super: %s", _flname);
        logicalname.set(_flname);
    }
    IDistributedSuperFile *getSuper()
    {
        if (!super)
        {
            Owned<IDistributedSuperFile> _super = transaction->lookupSuperFile(logicalname.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT);
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
                super.setown(new CDistributedSuperFile(parent, root, logicalname, AccessMode::writeMeta, user));
                created = true;
                super->setModified();
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
        CNestedTransaction(IDistributedFileTransactionExt *_transaction, IUserDescriptor *user)
            : CDistributedFileTransaction(user), transaction(_transaction)
        {
            if (transaction->active())
                start();
        }
        // wrap rest of calls into parent transaction calls
        virtual IDistributedFile *lookupFile(const char *lfn, AccessMode accessMode, unsigned timeout=INFINITE)
        {
            return transaction->lookupFile(lfn, accessMode, timeout);
        }
        virtual IDistributedSuperFile *lookupSuperFile(const char *slfn, AccessMode accessMode, unsigned timeout=INFINITE)
        {
            return transaction->lookupSuperFile(slfn, accessMode, timeout);
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
        tracing.appendf("RemoveSuperFile: super: %s", _flname);
        logicalname.set(_flname);
    }
    virtual bool prepare()
    {
        // We *have* to make sure the file exists here
        super.setown(transaction->lookupSuperFile(logicalname.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT));
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
        super->detach(INFINITE, transaction->queryCodeContext());
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
        : parent(_parent), user(_user)
    {
        tracing.appendf("RenameFile: name: %s, newname: %s", _flname, _newname);
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
        file.setown(transaction->lookupFile(fromName.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT));
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

            Owned<IDistributedFile> newFile = transaction->lookupFile(toName.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT);
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
                Owned<IDistributedFile> newFile = transaction->lookupFile(to.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT);
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
                file.setown(transaction->lookupFile(to.get(), AccessMode::tbdWrite, SDS_SUB_LOCK_TIMEOUT));
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
        localtrans.set(queryTransactionExt(transaction));
    }
    else
        localtrans.setown(new CDistributedFileTransaction(user));

    Owned<IDistributedSuperFile> sfile = localtrans->lookupSuperFile(logicalname.get(), AccessMode::tbdWrite);
    if (sfile)
    {
        if (ifdoesnotexist)
            return sfile.getClear();
        else
            throw MakeStringException(-1,"createSuperFile: SuperFile %s already exists",logicalname.get());
    }

    Owned<CCreateSuperFileAction> action = new CCreateSuperFileAction(this,user,_logicalname,_interleaved);
    localtrans->addAction(action.getLink()); // takes ownership
    localtrans->autoCommit();
    return action->getSuper();
}

// MORE: This should be implemented in DFSAccess later on
IDistributedSuperFile *CDistributedFileDirectory::createNewSuperFile(IPropertyTree *tree, const char *optionalName, IArrayOf<IDistributedFile> *subFiles)
{
    return new CDistributedSuperFile(this, tree, AccessMode::writeMeta, optionalName, subFiles);
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
        localtrans.set(queryTransactionExt(transaction));
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
        localtrans.set(queryTransactionExt(transaction));
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
        IERRLOG("%s", msg.str());
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
    logNullUser(user);//stack trace if NULL user
    if (!user)
    {
        user = defaultudesc.get();
    }
    CDfsLogicalFileName oldlogicalname;
    oldlogicalname.set(oldname);
    checkLogicalName(oldlogicalname,user,true,true,false,"rename");

    // Create a local transaction that will be destroyed (MORE: make transaction compulsory)
    Linked<IDistributedFileTransactionExt> localtrans;
    if (transaction)
    {
        localtrans.set(queryTransactionExt(transaction));
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
                    rfn.queryEndpoint().getEndpointHostText(s);
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
    if (prev!=nullptr)
    {
        prev->Release();
        root->Release();
        if (ignoreexists)
            return;
        throw new CDFS_Exception(DFSERR_LogicalNameAlreadyExists,dlfn.get());
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

IDistributedFileIterator *CDistributedFileDirectory::getIterator(const char *wildname, bool includesuper, IUserDescriptor *user,bool isPrivilegedUser)
{
    return new CDistributedFileIterator(this,wildname,includesuper,user,isPrivilegedUser);
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


static std::atomic<CDistributedFileDirectory *> DFdir{nullptr};
static CriticalSection dfdirCrit;

/**
 * Public method to control DistributedFileDirectory access
 * as a singleton. This is the only way to get directories,
 * files, super-files and logic-files.
 */
IDistributedFileDirectory &queryDistributedFileDirectory()
{
    if (!DFdir.load())
    {
        CriticalBlock block(dfdirCrit);
        if (!DFdir.load())
            DFdir.store(new CDistributedFileDirectory());
    }
    return *DFdir.load();
}

/**
 * Shutdown distributed file system (root directory).
 */
void closedownDFS() // called by dacoven
{
    CriticalBlock bDFdir(dfdirCrit);
    try
    {
        delete DFdir.load();
    }
    catch (IMP_Exception *e)
    {
        if (e->errorCode() != MPERR_link_closed)
            throw;
        PrintExceptionLog(e, "closedownDFS");
        e->Release();
    }
    catch (IDaliClient_Exception *e)
    {
        if (e->errorCode() != DCERR_server_closed)
            throw;
        e->Release();
    }
    DFdir.store(nullptr);

    CriticalBlock bGroupStore(groupsect);
    ::Release(groupStore.load());
    groupStore.store(nullptr);
}

class CDFPartFilter : implements IDFPartFilter, public CInterface
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
        for (;;) {
            if ((*s==0)||(*s==',')||isspace(*s)) {
                if (start) {
                    for (i=start-1;i<pn;i++)
                        partincluded[i] = true;
                    start = 0;
                }
                else if (pn == 0)
                    throw makeStringExceptionV(0, "Invalid part filter: %s", filter);
                else
                    partincluded[pn - 1] = true;
                if (*s == 0)
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
    "SubFile", "SubFile/@name", "SubFile/@num", "Attr/@kind", "Attr/@accessed", "Attr/@maxSkew", "Attr/@minSkew" };

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
        : filterType(_filterType), attrPath(_attrPath), hasFilter(_hasFilter), hasFilterHigh(_hasFilterHigh), filterValueInt(_filterValue), filterValueHighInt(_filterValueHigh) {};
    CDFUSFFilter(DFUQFilterType _filterType, const char *_attrPath, bool _hasFilter, const __int64 _filterValue, bool _hasFilterHigh, const __int64 _filterValueHigh)
        : filterType(_filterType), attrPath(_attrPath), hasFilter(_hasFilter), hasFilterHigh(_hasFilterHigh), filterValueInt64(_filterValue), filterValueHighInt64(_filterValueHigh) {};
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
        case DFUQFTinverseWildcardMatch:
            match = doInverseWildMatch(file);
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
    bool doInverseWildMatch(IPropertyTree &file)
    {
        const char* filter = filterValue.get();
        if (!attrPath.get() || !filter || !*filter || streq(filter, "*"))
            return true;

        const char* prop = file.queryProp(attrPath.get());
        if (prop && WildMatch(prop, filter, true))
            return false;
        return true;
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
        if (!filterValue.isEmpty() && (strcmp(filterValue, prop) > 0))
            return false;
        if (!filterValueHigh.isEmpty() && (strcmp(filterValueHigh, prop) < 0))
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
        if (!s || !*s)
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
        if ((DFUQFTwildcardMatch == filterType) || (DFUQFTstringRange == filterType) || (DFUQFTinverseWildcardMatch == filterType))
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
            case DFUQFTinverseWildcardMatch:
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
            const char* attrPath = filter.getAttrPath();
            try
            {
                if (!filter.checkFilter(file))
                    return false;
            }
            catch (IException *e)
            {
                VStringBuffer msg("Failed to check filter %s for %s: ", attrPath, name);
                int code = e->errorCode();
                e->errorMessage(msg);
                e->Release();
                throw MakeStringException(code, "%s", msg.str());
            }
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
            SecAccessFlags perm = getScopePermissions(scope.getName(),user,0);     // don't audit
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

StringBuffer &getClusterGroupName(const IPropertyTree &cluster, StringBuffer &groupName)
{
    const char *name = cluster.queryProp("@name");
    const char *nodeGroupName = cluster.queryProp("@nodeGroup");
    if (nodeGroupName && *nodeGroupName)
        name = nodeGroupName;
    groupName.append(name);
    return groupName.trim().toLowerCase();
}

StringBuffer &getClusterSpareGroupName(const IPropertyTree &cluster, StringBuffer &groupName)
{
    return getClusterGroupName(cluster, groupName).append("_spares");
}

// JCSMORE - dfs group handling may be clearer if in own module
class CInitGroups
{
    std::unordered_map<std::string, std::string> machineMap;
    CConnectLock groupsconnlock;
    StringArray clusternames;
    unsigned defaultTimeout;
    bool machinesLoaded;
    bool writeLock;

    GroupType getGroupType(const char *type)
    {
        if (0 == strcmp("ThorCluster", type))
            return grp_thor;
        else if (0 == strcmp("RoxieCluster", type))
            return grp_roxie;
        else
            throwUnexpected();
    }
    bool clusterGroupCompare(IPropertyTree *newClusterGroup, IPropertyTree *oldClusterGroup)
    {
        if (!newClusterGroup && !oldClusterGroup)
            return true; // i.e. both missing, so match
        else if (!newClusterGroup || !oldClusterGroup)
            return false; // i.e. one of them (not both) missing, so mismatch
        // else // neither missing

        // see if identical
        const char *oldKind = oldClusterGroup->queryProp("@kind");
        const char *oldDir = oldClusterGroup->queryProp("@dir");
        const char *newKind = newClusterGroup->queryProp("@kind");
        const char *newDir = newClusterGroup->queryProp("@dir");
        if (oldKind)
        {
            if (newKind)
            {
                if (!streq(newKind, newKind))
                    return false;
            }
            else
                return false;
        }
        else if (newKind)
            return false;
        if (oldDir)
        {
            if (newDir)
            {
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
        if (newIter->first() && oldIter->first())
        {
            for (;;)
            {
                // NB: for legacy reason these are called @ip in Dali, but they should typically be hostnames
                SocketEndpoint oldEp, newEp;
                oldEp.set(oldIter->query().queryProp("@ip"));
                newEp.set(newIter->query().queryProp("@ip"));
                if (oldEp != newEp)
                    return false;
                if (!oldIter->next() || !newIter->next())
                    break;
            }
        }
        return true;
    }

    void addClusterGroup(const char *name, IPropertyTree *newClusterGroup, bool realCluster)
    {
        if (!writeLock)
            throw makeStringException(0, "CInitGroups::addClusterGroup called in read-only mode");
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

    IGroup *getGroupFromCluster(GroupType groupType, const IPropertyTree &cluster, bool expand)
    {
        Owned<IPropertyTree> groupTree = createClusterGroupFromEnvCluster(groupType, cluster, nullptr, false, expand);
        if (!groupTree)
            return nullptr;
        Owned<IPropertyTreeIterator> nodeIter = groupTree->getElements("Node");
        if (!nodeIter->first())
            return nullptr;
        SocketEndpointArray eps;
        do
        {
            SocketEndpoint ep(nodeIter->query().queryProp("@ip"));
            eps.append(ep);
        }
        while (nodeIter->next());
        return createIGroup(eps);
    }

    bool loadMachineMap(const IPropertyTree *env)
    {
        if (machinesLoaded)
            return true;
        Owned<IPropertyTreeIterator> machines = env->getElements("Hardware/Computer");
        if (!machines->first())
        {
            IWARNLOG("No Hardware/Computer's found");
            return false;
        }
        do
        {
            const IPropertyTree &machine = machines->query();
            const char *host = machine.queryProp("@netAddress");
            const char *name = machine.queryProp("@name");
            machineMap.insert({ name, host });
        }
        while (machines->next());
        machinesLoaded = true;
        return true;
    }

    bool loadMachineMap()
    {
        if (machinesLoaded)
            return true;
        //GH->JCS This can't be changed to use getEnvironmentFactory() unless that moved inside dalibase;
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
        {
            IWARNLOG("Cannot connect to /Environment");
            return false;
        }
        return loadMachineMap(conn->queryRoot());
    }

    IPropertyTree *createClusterGroup(GroupType groupType, const std::vector<std::string> &hosts, const char *dir, const IPropertyTree * envCluster, bool realCluster, bool _expand)
    {
        bool expand = _expand;
        if (grp_thor != groupType)
            expand = false;
        Owned<IPropertyTree> cluster = createPTree("Group");
        if (realCluster)
            cluster->setPropBool("@cluster", true);
        const char *kind=nullptr;
        switch (groupType)
        {
            case grp_thor:
                kind = "Thor";
                break;
            case grp_roxie:
                kind = "Roxie";
                break;
            case grp_hthor:
                kind = "hthor";
                break;
            case grp_dropzone:
                kind = "dropzone";
                break;
        }
        if (kind)
            cluster->setProp("@kind",kind);
        if (dir)
            cluster->setProp("@dir",dir);

        auto addHostsToIPTFunc = [cluster, &hosts]()
        {
            for (auto &host: hosts)
            {
                IPropertyTree *node = cluster->addPropTree("Node");
                node->setProp("@ip", host.c_str());
            }
        };
        if (expand)
        {
            assertex(envCluster);
            unsigned slavesPerNode = envCluster->getPropInt("@slavesPerNode", 1);
            unsigned channelsPerSlave = envCluster->getPropInt("@channelsPerSlave", 1);
            for (unsigned s=0; s<(slavesPerNode*channelsPerSlave); s++)
                addHostsToIPTFunc();
        }
        else
            addHostsToIPTFunc();
        return cluster.getClear();
    }
    const char *getHostFromClusterEntry(const IPropertyTree &node, const char *clusterName)
    {
        const char *computer = node.queryProp("@computer");
        if (!isEmptyString(computer))
        {
            auto it = machineMap.find(computer);
            if (it == machineMap.end())
            {
                OERRLOG("Cannot construct %s, computer name %s not found\n", clusterName, computer);
                return nullptr;
            }
            return it->second.c_str();
        }
        else
        {
            const char *host = node.queryProp("@netAddress");
            if (isEmptyString(host))
            {
                OERRLOG("Cannot construct %s, missing computer spec on node\n", clusterName);
                return nullptr;
            }
            else
                return host;
        }
    }
    IPropertyTree *createClusterGroupFromEnvCluster(GroupType groupType, const IPropertyTree &cluster, const char *dir, bool realCluster, bool expand)
    {
        const char *processName=nullptr;
        switch (groupType)
        {
            case grp_thor:
                processName = "ThorSlaveProcess";
                break;
            case grp_thorspares:
                processName = "ThorSpareProcess";
                break;
            case grp_roxie:
                processName = "RoxieServerProcess";
                break;
            case grp_dropzone:
                processName = "ServerList";
                break;
            default:
                throwUnexpected();
        }
        std::vector<std::string> hosts;
        Owned<IPropertyTreeIterator> nodes = cluster.getElements(processName);
        if (nodes->first())
        {
            do
            {
                IPropertyTree &node = nodes->query();
                const char *host = nullptr;
                if (grp_dropzone == groupType)
                    host = node.queryProp("@server");
                else
                    host = getHostFromClusterEntry(node, cluster.queryProp("@name"));
                switch (groupType)
                {
                    case grp_roxie:
                        // Redundant copies are located via the flags.
                        // Old environments may contain duplicated sever information for multiple ports
                        if (hosts.end() == std::find(hosts.begin(), hosts.end(), host)) // only add if not already there
                            hosts.push_back(host);
                        break;
                    case grp_thor:
                    case grp_thorspares:
                    case grp_dropzone:
                        hosts.push_back(host);
                        break;
                    default:
                        throwUnexpected();
                }
            }
            while (nodes->next());
        }
        else if (grp_dropzone == groupType)
        {
            // legacy support for DropZone's without ServerList
            if (cluster.hasProp("@computer") || cluster.hasProp("@netAddress"))
            {
                const char *host = getHostFromClusterEntry(cluster, cluster.queryProp("@name"));
                if (!isEmptyString(host))
                    hosts.push_back(host);
            }
        }
        if (!hosts.size())
            return nullptr;

        return createClusterGroup(groupType, hosts, dir, &cluster, realCluster, expand);
    }

    bool constructGroup(const IPropertyTree &cluster, const char *altName, IPropertyTree *oldEnvCluster, GroupType groupType, bool force, StringBuffer &messages)
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
                getClusterGroupName(cluster, gname); // NB: ensures lowercases
                if (!strieq(cluster.queryProp("@name"), gname.str()))
                    realCluster = false;
                if (oldEnvCluster)
                {
                    getClusterGroupName(*oldEnvCluster, oldGname); // NB: ensures lowercases
                    if (!strieq(oldEnvCluster->queryProp("@name"), oldGname.str()))
                        oldRealCluster = false;
                }
                break;
            case grp_thorspares:
                getClusterSpareGroupName(cluster, gname); // ensures lowercase
                oldRealCluster = realCluster = false;
                break;
            case grp_roxie:
                gname.append(cluster.queryProp("@name"));
                gname.toLowerCase();
                break;
            case grp_dropzone:
                gname.append(cluster.queryProp("@name"));
                gname.toLowerCase();
                oldRealCluster = realCluster = false;
                defDir = cluster.queryProp("@directory");
                break;
            default:
                throwUnexpected();
        }
        if (altName)
            gname.clear().append(altName).toLowerCase();

        IPropertyTree *existingClusterGroup = queryExistingGroup(gname);
        bool matchOldEnv = false;
        Owned<IPropertyTree> newClusterGroup = createClusterGroupFromEnvCluster(groupType, cluster, defDir, realCluster, true);
        bool matchExisting = !force && clusterGroupCompare(newClusterGroup, existingClusterGroup);
        if (oldEnvCluster)
        {
            // new matches old, only if neither has changed it's name to mismatch it's nodeGroup name
            if (realCluster == oldRealCluster)
            {
                Owned<IPropertyTree> oldClusterGroup = createClusterGroupFromEnvCluster(groupType, *oldEnvCluster, defDir, oldRealCluster, true);
                matchOldEnv = clusterGroupCompare(newClusterGroup, oldClusterGroup);
            }
            else
                matchOldEnv = false;
        }
        if (!matchExisting)
        {
            if (force)
            {
                VStringBuffer msg("Forcing new group layout for %s [ matched active = false, matched old environment = %s ]", gname.str(), matchOldEnv?"true":"false");
                UWARNLOG("%s", msg.str());
                messages.append(msg).newline();
                matchOldEnv = false;
            }
            else
            {
                VStringBuffer msg("Active cluster '%s' group layout does not match environment [matched old environment=%s]", gname.str(), matchOldEnv?"true":"false");
                UWARNLOG("%s", msg.str());                                                                        \
                messages.append(msg).newline();
                if (existingClusterGroup)
                {
                    // NB: not used at moment, but may help spot clusters that have swapped nodes
                    existingClusterGroup->setPropBool("@mismatched", true);
                }
            }
        }
        if ((!existingClusterGroup && (grp_thorspares != groupType)) || (!matchExisting && !matchOldEnv))
        {
            VStringBuffer msg("New cluster layout for cluster %s", gname.str());
            UWARNLOG("%s", msg.str());
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
        ForEach(*insts)
        {
            const char *na = insts->query().queryProp("@netAddress");
            if (!isEmptyString(na))
            {
                SocketEndpoint ep(na);
                if (!ep.isNull())
                {
                    ins++;
                    VStringBuffer gname("hthor__%s", groupname);
                    if (ins>1)
                        gname.append('_').append(ins);
                    Owned<IPropertyTree> clusterGroup = createClusterGroup(grp_hthor, { na }, nullptr, &cluster, true, false);
                    addClusterGroup(gname.str(), clusterGroup.getClear(), true);
                }
            }
        }
    }

    struct BoundHost
    {
        BoundHost(const std::string &_host) : host(_host), ep(_host.c_str()) { }
        std::string host;
        SocketEndpoint ep;
        bool operator == (const SocketEndpoint &other) const
        {
            return ep == other;
        }
    };
    unsigned bind(const std::vector<std::string> &hosts, std::vector<BoundHost> &boundHosts) const
    {
        for (const auto &host: hosts)
        {
            SocketEndpoint boundHost(host.c_str());
            if (boundHosts.end() == std::find(boundHosts.begin(), boundHosts.end(), boundHost))
                boundHosts.push_back(BoundHost(host));
        }
        return boundHosts.size();
    }
    IPropertyTree *queryExistingSpareGroup(const IPropertyTree *cluster, StringBuffer &groupName)
    {
        getClusterSpareGroupName(*cluster, groupName);
        IPropertyTree *root = groupsconnlock.conn->queryRoot();
        VStringBuffer xpath("Group[@name=\"%s\"]", groupName.str());
        return root->queryPropTree(xpath.str());
    }
public:
    CInitGroups(unsigned _defaultTimeout, bool _writeLock)
        : groupsconnlock("constructGroup",SDS_GROUPSTORE_ROOT,_writeLock,false,false,_defaultTimeout)
    {
        defaultTimeout = _defaultTimeout;
        machinesLoaded = false;
        writeLock = _writeLock;
    }

    IPropertyTree *queryCluster(const IPropertyTree *env, const char *_clusterName, const char *type, const char *msg, StringBuffer &messages)
    {
        if (isEmptyString(_clusterName) || isEmptyString(type))
            return nullptr;
        if (!streq("ThorCluster", type)) // currently only Thor supported here.
            return nullptr;
        StringAttr clusterName = _clusterName;
        clusterName.toLowerCase();
        if (loadMachineMap())
        {
            VStringBuffer xpath("Software/%s[@name=\"%s\"]", type, clusterName.get());
            Owned<IPropertyTreeIterator> clusterIter = env->getElements(xpath);

            if (!clusterIter->first())
            {
                VStringBuffer errMsg("%s: Could not find type %s, %s cluster", msg, type, clusterName.get());
                UWARNLOG("%s", errMsg.str());
                messages.append(errMsg).newline();
            }
            else
            {
                IPropertyTree *cluster = &clusterIter->query();
                if (!clusterIter->next())
                    return cluster;
                VStringBuffer errMsg("%s: more than one cluster named: %s", msg, clusterName.get());
                UWARNLOG("%s", errMsg.str());
                messages.append(errMsg).newline();
            }
        }
        return nullptr;
    }
    bool resetClusterGroup(const char *clusterName, const char *type, bool spares, StringBuffer &messages)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return false;
        const IPropertyTree *cluster = queryCluster(conn->queryRoot(), clusterName, type, "resetClusterGroup", messages);
        if (!cluster)
            return false;
        if (spares)
        {
            if (constructGroup(*cluster,NULL,NULL,grp_thorspares,true,messages))
                return true;
        }
        else
        {
            if (constructGroup(*cluster,NULL,NULL,grp_thor,true,messages))
                return true;
        }
        return false;
    }
    bool addSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &messages)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return false;
        const IPropertyTree *cluster = queryCluster(conn->queryRoot(), clusterName, type, "addSpares", messages);
        if (!cluster)
            return false;

        std::vector<BoundHost> boundHostsToAdd;
        bind(hosts, boundHostsToAdd);

        StringBuffer groupName;
        IPropertyTree *existing = queryExistingSpareGroup(cluster, groupName);
        if (existing)
        {
            Owned<IPropertyTreeIterator> iter = existing->getElements("Node");
            ForEach(*iter)
            {
                const char *host = iter->query().queryProp("@ip");
                SocketEndpoint ep(host); // NB: for legacy reason it's called @ip, but should typically be a hostname

                // delete any entries that are already in Group
                auto it = std::remove(boundHostsToAdd.begin(), boundHostsToAdd.end(), ep);
                if (it != boundHostsToAdd.end())
                {
                    boundHostsToAdd.erase(it, boundHostsToAdd.end());
                    VStringBuffer errMsg("addSpares: not adding: %s, already in spares", host);
                    UWARNLOG("%s", errMsg.str());
                    messages.append(errMsg).newline();
                }
            }
        }
        else
        {
            existing = groupsconnlock.conn->queryRoot()->addPropTree("Group");
            existing->setProp("@name", groupName.str());
        }
        // add remaining
        for (const auto &boundHost: boundHostsToAdd)
        {
            IPropertyTree *node = existing->addPropTree("Node");
            node->setProp("@ip", boundHost.host.c_str());
        }
        return true;
    }
    bool removeSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &messages)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return false;
        const IPropertyTree *cluster = queryCluster(conn->queryRoot(), clusterName, type, "removeSpares", messages);
        if (!cluster)
            return false;

        std::vector<BoundHost> boundHostsToRemove;
        bind(hosts, boundHostsToRemove);

        StringBuffer groupName;
        IPropertyTree *existing = queryExistingSpareGroup(cluster, groupName);
        if (existing)
        {
            SocketEndpointArray existingGroupBoundEps;
            StringAttr groupDir;
            if (!loadGroup(existing, existingGroupBoundEps, nullptr, nullptr))
            {
                IWARNLOG("removeSpares: failed to load group: '%s'", groupName.str());
                return false;
            }
            for (const auto &boundHostToRemove: boundHostsToRemove)
            {
                bool matched = true;
                ForEachItemIn(e, existingGroupBoundEps)
                {
                    if (existingGroupBoundEps.item(e) == boundHostToRemove.ep)
                    {
                        VStringBuffer xpath("Node[%u]", e+1);
                        verifyex(existing->removeProp(xpath));
                        matched = true;
                    }
                    // there shouldn't be any others, but in keeping with legacy code, continue matching
                }
                if (!matched)
                {
                    VStringBuffer errMsg("removeSpares: %s not found in spares", boundHostToRemove.host.c_str());
                    UWARNLOG("%s", errMsg.str());
                    messages.append(errMsg).newline();
                }
            }
        }
        return true;
    }
    void clearLZGroups()
    {
        if (!writeLock)
            throw makeStringException(0, "CInitGroups::clearLZGroups called in read-only mode");
        IPropertyTree *root = groupsconnlock.conn->queryRoot();
        std::vector<IPropertyTree *> toDelete;
        Owned<IPropertyTreeIterator> groups = root->getElements("Group[@kind='dropzone']");
        ForEach(*groups)
            toDelete.push_back(&groups->query());
        for (auto &group: toDelete)
            root->removeTree(group);
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

        //Walk the drop zones, and add them as storage groups if they have no servers configured, or "."
        Owned<IPropertyTreeIterator> dropzones = conn->queryRoot()->getElements("DropZone");
        ForEach(*dropzones)
        {
            IPropertyTree & dropZone = dropzones->query();
            unsigned numServers = dropZone.getCount("ServerList");

            //Allow url style drop zones, and drop zones with a single node.  Not sure what >1 would mean in legacy.
            if (numServers <= 1)
            {
                IPropertyTree *oldDropZone = NULL;
                if (oldEnvironment)
                {
                    VStringBuffer xpath("Software/DropZone[@name=\"%s\"]", dropZone.queryProp("@name"));
                    oldDropZone = oldEnvironment->queryPropTree(xpath.str());
                }
                constructGroup(dropZone,NULL,oldDropZone,grp_dropzone,force,messages);
            }
        }
    }

    IPropertyTree * createStorageGroup(const char * name, size32_t size, const char * path)
    {
        std::vector<std::string> hosts(size, "localhost");
        return createClusterGroup(grp_unknown, hosts, path, nullptr, false, false);
    }

    void ensureConsistentStorageGroup(bool force, const char * name, IPropertyTree * newClusterGroup, StringBuffer & messages)
    {
        IPropertyTree *existingClusterGroup = queryExistingGroup(name);
        bool matchExisting = clusterGroupCompare(newClusterGroup, existingClusterGroup);
        if (!existingClusterGroup || !matchExisting)
        {
            if (!existingClusterGroup)
            {
                VStringBuffer msg("New cluster layout for cluster %s", name);
                UWARNLOG("%s", msg.str());
                messages.append(msg).newline();
                addClusterGroup(name, LINK(newClusterGroup), false);
            }
            else if (force)
            {
                VStringBuffer msg("Forcing new group layout for storageplane %s", name);
                UWARNLOG("%s", msg.str());
                messages.append(msg).newline();
                addClusterGroup(name, LINK(newClusterGroup), false);
            }
            else
            {
                VStringBuffer msg("Active cluster '%s' group layout does not match storageplane definition", name);
                UWARNLOG("%s", msg.str());                                                                        \
                messages.append(msg).newline();
            }
        }
    }

    void ensureStorageGroup(bool force, const char * name, unsigned numDevices, const char * path, StringBuffer & messages)
    {
        //Lower case the group name - see CNamedGroupStore::dolookup which lower cases before resolving.
        StringBuffer gname;
        gname.append(name).toLowerCase();

        Owned<IPropertyTree> newClusterGroup = createStorageGroup(gname, numDevices, path);
        ensureConsistentStorageGroup(force, gname, newClusterGroup, messages);
    }

    void constructStorageGroups(bool force, StringBuffer &messages)
    {
        Owned<IPropertyTree> globalConfig = getGlobalConfig();
        IPropertyTree * storage = globalConfig->queryPropTree("storage");
        if (storage)
        {
            normalizeHostGroups(globalConfig);

            Owned<IPropertyTreeIterator> planes = storage->getElements("planes");
            ForEach(*planes)
            {
                IPropertyTree & plane = planes->query();
                const char * name = plane.queryProp("@name");
                if (isEmptyString(name))
                    continue;

                //Lower case the group name - see CNamedGroupStore::dolookup which lower cases before resolving.
                StringBuffer gname;
                gname.append(name).toLowerCase();

                //Two main type of storage plane - with a host group (bare metal) and without.
                const char * hostGroup = plane.queryProp("@hostGroup");
                const char * prefix = plane.queryProp("@prefix");
                Owned<IPropertyTree> newClusterGroup;
                if (hostGroup)
                {
                    Owned<IPropertyTree> match = getHostGroup(hostGroup, true);
                    std::vector<std::string> hosts;
                    Owned<IPropertyTreeIterator> hostIter = match->getElements("hosts");
                    ForEach (*hostIter)
                        hosts.push_back(hostIter->query().queryProp(nullptr));

                    //A bare-metal storage plane defined in terms of a hostGroup
                    newClusterGroup.setown(createClusterGroup(grp_unknown, hosts, prefix, nullptr, false, false));
                }
                else if (plane.hasProp("hostGroup"))
                {
                    throw makeStringExceptionV(-1, "Use 'hosts' rather than 'hostGroup' for inline list of hosts for plane %s", gname.str());
                }
                else if (plane.hasProp("hosts"))
                {
                    //A bare-metal storage plane defined by an explicit list of ips (useful for landing zones)
                    std::vector<std::string> hosts;
                    Owned<IPropertyTreeIterator> iter = plane.getElements("hosts");
                    ForEach(*iter)
                        hosts.push_back(iter->query().queryProp(nullptr));
                    newClusterGroup.setown(createClusterGroup(grp_unknown, hosts, prefix, nullptr, false, false));
                }
                else
                {
                    //Locally mounted, or url accessed storage plane - no associated hosts, localhost used as a placeholder
                    unsigned numDevices = plane.getPropInt("@numDevices", 1);
                    newClusterGroup.setown(createStorageGroup(gname, numDevices, prefix));
                }
                ensureConsistentStorageGroup(force, gname, newClusterGroup, messages);
            }
        }
    }
    IGroup *getGroupFromCluster(const char *type, const IPropertyTree &cluster, bool expand)
    {
        loadMachineMap();
        GroupType gt = getGroupType(type);
        return getGroupFromCluster(gt, cluster, expand);
    }
    IPropertyTree *queryExistingGroup(const char *name)
    {
        VStringBuffer xpath("Group[@name=\"%s\"]", name);
        return groupsconnlock.conn->queryRoot()->queryPropTree(xpath.str());
    }
};

void initClusterGroups(bool force, StringBuffer &response, IPropertyTree *oldEnvironment, unsigned timems)
{
    CInitGroups init(timems, true);
    init.clearLZGroups(); // clear existing LZ groups, current ones will be recreated
    init.constructGroups(force, response, oldEnvironment);
}

void initClusterAndStoragePlaneGroups(bool force, IPropertyTree *oldEnvironment, unsigned timems)
{
    CInitGroups init(timems, true);
    init.clearLZGroups(); // clear existing LZ groups, current ones will be recreated

    StringBuffer response;
    init.constructGroups(force, response, oldEnvironment);
    if (response.length())
        MLOG("DFS group initialization : %s", response.str()); // should this be a syslog?

    response.clear();
    init.constructStorageGroups(false, response);
    if (response.length())
        MLOG("StoragePlane group initialization : %s", response.str()); // should this be a syslog?
}

bool resetClusterGroup(const char *clusterName, const char *type, bool spares, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems, true);
    return init.resetClusterGroup(clusterName, type, spares, response);
}

bool addClusterSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems, true);
    return init.addSpares(clusterName, type, hosts, response);
}

bool removeClusterSpares(const char *clusterName, const char *type, const std::vector<std::string> &hosts, StringBuffer &response, unsigned timems)
{
    CInitGroups init(timems, true);
    return init.removeSpares(clusterName, type, hosts, response);
}

static IGroup *getClusterNodeGroup(const char *clusterName, const char *type, bool processGroup, unsigned timems)
{
    VStringBuffer clusterPath("/Environment/Software/%s[@name=\"%s\"]", type, clusterName);
    Owned<IRemoteConnection> conn = querySDS().connect(clusterPath.str(), myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
    if (!conn)
        return NULL;
    IPropertyTree &cluster = *conn->queryRoot();
    StringBuffer nodeGroupName;
    getClusterGroupName(cluster, nodeGroupName);
    if (0 == nodeGroupName.length())
        throwUnexpected();

    /* NB: Due to the way node groups and swapNode work, we need to return the IP's from the node group corresponding to the cluster
     * which may no longer match the cluster IP's due to node swapping.
     * As the node group is an expanded form of the cluster group (with a IP per partition/slave), with the cluster group repeated
     * N times, where N is slavesPerNode*channelsPerSlave, return the first M (cluster group width) IP's of the node group.
     * Ideally the node group representation would change to match the cluster group definition, but that require a lot of changes
     * to DFS and elsewhere.
     */
    Owned<IGroup> nodeGroup = queryNamedGroupStore().lookup(nodeGroupName);
    CInitGroups init(timems, false);
    Owned<IGroup> expandedClusterGroup = init.getGroupFromCluster(type, cluster, true);
    if (!expandedClusterGroup)
        throwStringExceptionV(0, "Failed to get group for '%s' cluster '%s'", type, clusterName);
    if (!expandedClusterGroup->equals(nodeGroup))
    {
        IPropertyTree *rawGroup = init.queryExistingGroup(nodeGroupName);
        if (!rawGroup)
            throwUnexpectedX("missing node group");
        unsigned nodesSwapped = rawGroup->getPropInt("@nodesSwapped");
        if (nodesSwapped)
        {
            unsigned rawGroupSize = rawGroup->getCount("Node");
            if (rawGroupSize != expandedClusterGroup->ordinality())
                throwStringExceptionV(0, "DFS cluster topology for '%s', does not match existing DFS group size for group '%s' [Environment cluster group size = %u, Dali group size = %u]",
                        clusterName, nodeGroupName.str(), expandedClusterGroup->ordinality(), rawGroupSize);
            VStringBuffer msg("DFS cluster topology for '%s' using group '%s', does not match environment due to previously swapped nodes", clusterName, nodeGroupName.str());
            WARNLOG("%s", msg.str());
        }
        else
            throwStringExceptionV(0, "DFS cluster topology for '%s', does not match existing DFS group layout for group '%s'", clusterName, nodeGroupName.str());
    }
    Owned<IGroup> clusterGroup = init.getGroupFromCluster(type, cluster, false);
    ICopyArrayOf<INode> nodes;
    unsigned l=processGroup?cluster.getPropInt("@slavesPerNode", 1):1; // if process group requested, repeat clusterGroup slavesPerNode times.
    for (unsigned t=0; t<l; t++)
    {
        for (unsigned n=0; n<clusterGroup->ordinality(); n++)
            nodes.append(nodeGroup->queryNode(n));
    }
    return createIGroup(nodes.ordinality(), nodes.getArray());
}

IGroup *getClusterNodeGroup(const char *clusterName, const char *type, unsigned timems)
{
    return getClusterNodeGroup(clusterName, type, false, timems);
}

IGroup *getClusterProcessNodeGroup(const char *clusterName, const char *type, unsigned timems)
{
    return getClusterNodeGroup(clusterName, type, true, timems);
}


class CDaliDFSServer: public Thread, public CTransactionLogTracker, implements IDaliServer, implements IExceptionHandler
{  // Coven size

    bool stopped;
    unsigned defaultTimeout;
    unsigned numThreads;
    Owned<INode> dafileSrvNode;
    CriticalSection dafileSrvNodeCS;

public:

    IMPLEMENT_IINTERFACE_USING(Thread);

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
        Thread::start(false);
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

        bool auth = scopePermissionsAvail && querySessionManager().checkScopeScansLDAP();
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

    void iterateFilteredFiles(TransactionLog &transactionLog, CMessageBuffer &mb,StringBuffer &trc, bool returnAllFilesFlag)
    {
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

        bool auth = scopePermissionsAvail && querySessionManager().checkScopeScansLDAP();
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
        if (returnAllFilesFlag)
            mb.append(returnAllMatchingFiles);

        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(LDAP): %s: took %dms, %d lookups, file matches = %d", trc.str(), tookMs, authScopes.ordinality(), count);

        sdsLock.lock(); // re-lock sds while serializing
        start = msTick();
        ForEachItemIn(m, matchingFiles)
        {
            CFileMatch &fileMatch = matchingFiles.item(m);
            unsigned pos = mb.length();
            try
            {
                CDFAttributeIterator::serializeFileAttributes(mb, fileMatch.queryFileTree(), fileMatch.queryName(), fileMatch.queryIsSuper(), iterateFileFilterContainer->getSerializeFileAttrOptions());
            }
            catch (IException *e)
            {
                StringBuffer errMsg("Failed to serialize properties for file: ");
                LOG(MCuserWarning, e, errMsg.append(fileMatch.queryName()));
                e->Release();
                mb.setLength(pos);
                --count;
            }
        }
        tookMs = msTick()-start;
        if (tookMs>100)
            PROGLOG("TIMING(filescan-serialization): %s: took %dms, %d files",trc.str(), tookMs, count);

        mb.writeDirect(0,sizeof(count),&count);
    }

    void iterateFilteredFiles(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_ITERATE_FILTEREDFILES, mb.getSender());
        iterateFilteredFiles(transactionLog, mb, trc, false);
    }

    void iterateFilteredFiles2(CMessageBuffer &mb,StringBuffer &trc)
    {
        TransactionLog transactionLog(*this, MDFS_ITERATE_FILTEREDFILES2, mb.getSender());
        iterateFilteredFiles(transactionLog, mb, trc, true);
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

    void getFileTree(CMessageBuffer &mb, StringBuffer &trc, unsigned version)
    {
        TransactionLog transactionLog(*this, MDFS_GET_FILE_TREE, mb.getSender());

        GetFileTreeOpts opts = GetFileTreeOpts::none;
        Owned<IUserDescriptor> udesc;

        StringAttr lname;
        mb.read(lname);
        if (version >= 2)
        {
            unsigned _opts;        
            mb.read(_opts);
            opts = static_cast<GetFileTreeOpts>(_opts);
            bool hasUser;
            mb.read(hasUser);
            if (hasUser)
            {
                udesc.setown(createUserDescriptor());
                udesc->deserialize(mb);
            }
        }
        else // pre gft versioning/tidyup
        {
            // for ancient backward version compatibility
            if (mb.length()<mb.getPos()+sizeof(unsigned))
                version = 0;
            else
            {
                mb.read(version);
                // this is a bit of a mess - for backward compatibility where user descriptor specified
                if (version>MDFS_GET_FILE_TREE_V2)
                {
                    mb.reset(mb.getPos()-sizeof(unsigned));
                    version = 0;
                }
                else
                {
                    // NB: just for clarity - MDFS_GET_FILE_TREE_V2 is hardwired to 1
                    // version >= 2 is cleaned up version.
                    version = 1;
                }
            }
            if (mb.getPos()<mb.length())
            {
                udesc.setown(createUserDescriptor());
                udesc->deserialize(mb);
            }
        }
        trc.appendf("getFileTree(%s, client gft version=%u)",lname.str(), version);
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());

        // This is Dali, and for foreign access (see below), if in use, this is likely to be false
        bool secureService = getComponentConfigSP()->getPropBool("@tls");

        mb.clear();
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        CDfsLogicalFileName *logicalname=&dlfn;
        Owned<IDfsLogicalFileNameIterator> redmatch;
        for (;;)
        {
            StringBuffer tail;
            checkLogicalName(*logicalname,udesc,true,false,true,"getFileTree on");
            CScopeConnectLock sconnlock("getFileTree", *logicalname, false, false, false, defaultTimeout);
            IPropertyTree* sroot = sconnlock.conn()?sconnlock.conn()->queryRoot():NULL;
            logicalname->getTail(tail);
            INamedGroupStore *groupResolver = &queryNamedGroupStore();
            if (version >= 2)
            {
                Owned<IPropertyTree> tree = getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
                if (tree)
                {
                    // This is for bare-metal clients using ~foreign pointing at a containerized/k8s setup,
                    // asking for the returned meta data to be remapped to point to the dafilesrv service.
                    if (isContainerized() && hasMask(opts, GetFileTreeOpts::remapToService))
                    {
                        tree.setown(createPTreeFromIPT(tree)); // copy live Dali tree, because it is about to be altered by remapGroupsToDafilesrv
                        remapGroupsToDafilesrv(tree, true, secureService);
                        groupResolver = nullptr; // do not attempt to resolve remapped group (it will not exist and cause addUnique to create a new anon one)

                        const char *remotePlaneName = tree->queryProp("@group");
                        Owned<IPropertyTree> filePlane = getStoragePlane(remotePlaneName);
                        assertex(filePlane);
                        // Used by DFS clients to determine if stripe and/or alias translation needed
                        tree->setPropTree("Attr/_remoteStoragePlane", createPTreeFromIPT(filePlane));
                    }
                    else
                        tree->removeProp("Attr/_remoteStoragePlane");

                    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,groupResolver,IFDSF_EXCLUDE_CLUSTERNAMES);
                    mb.append((int)1); // 1 == standard file
                    fdesc->serialize(mb);

                    /* not sure why this attribute is special/here at top level
                        and not part of the IFileDescriptor serialization itself */
                    StringBuffer dts;
                    tree->getProp("@modified", dts);
                    unsigned l = dts.length();
                    mb.append(l).append(l, dts.str());

                    break;
                }
                else
                {
                    tree.setown(getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str(),false));
                    if (tree)
                    {
                        mb.append((int)2); // 2 == super
                        tree->serialize(mb);
                        break;
                    }
                }
            }
            else
            {
                Owned<IPropertyTree> tree = getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_File),"@name",tail.str(),false);
                if (tree)
                {
                    if (version == MDFS_GET_FILE_TREE_V2)
                    {
                        if (isContainerized())
                        {
                            // NB: to be here, the client is by definition legacy, and should only be via ~foreign.
                            // NB: foreignAccess is a auto-generated template setting, that is set to true if Dali and directio,
                            // have been exposed in the helm chart for foreign access.
                            if (getComponentConfigSP()->getPropBool("@foreignAccess"))
                            {
                                tree.setown(createPTreeFromIPT(tree)); // copy live Dali tree, because it is about to be altered by remapGroupsToDafilesrv
                                remapGroupsToDafilesrv(tree, true, secureService);
                                groupResolver = nullptr; // do not attempt to resolve remapped group (it will not exist and cause addUnique to create a new anon one)
                            }
                        }

                        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,groupResolver,IFDSF_EXCLUDE_CLUSTERNAMES);

                        mb.append((int)-2).append(version);

                        fdesc->serialize(mb);
                        StringBuffer dts;
                        if (tree->getProp("@modified",dts))
                        {
                            CDateTime dt;
                            dt.setString(dts.str());
                            dt.serialize(mb);
                        }
                    }
                    else if (version==0) // ancient version backward compatibility
                    {
                        tree.setown(createPTreeFromIPT(tree));
                        StringBuffer cname;
                        logicalname->getCluster(cname);
                        expandFileTree(tree,true,cname.str()); // resolve @node values that may not be set
                        tree->serialize(mb);
                    }
                    break;
                }
                else
                {
                    tree.setown(getNamedPropTree(sroot,queryDfsXmlBranchName(DXB_SuperFile),"@name",tail.str(),false));
                    if (tree)
                    {
                        tree->serialize(mb);
                        break;
                    }
                }
            }
            if (redmatch.get())
            {
                if (!redmatch->next())
                    break;
            }
            else
            {
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

        try
        {
            switch (fn)
            {
                case MDFS_ITERATE_FILES:
                {
                    iterateFiles(mb, trc);
                    break;
                }
                case MDFS_ITERATE_FILTEREDFILES: // legacy, newer clients will send MDFS_ITERATE_FILTEREDFILES2
                {
                    iterateFilteredFiles(mb, trc);
                    break;
                }
                case MDFS_ITERATE_FILTEREDFILES2:
                {
                    iterateFilteredFiles2(mb, trc);
                    break;
                }
                case MDFS_ITERATE_RELATIONSHIPS:
                {
                    iterateRelationships(mb, trc);
                    break;
                }
                case MDFS_GET_FILE_TREE:
                {
                    getFileTree(mb, trc, 0);
                    break;
                }
                case MDFS_GET_FILE_TREE2:
                {
                    unsigned clientGFTVersion;
                    mb.read(clientGFTVersion);
                    getFileTree(mb, trc, clientGFTVersion);
                    break;
                }
                case MDFS_GET_GROUP_TREE:
                {
                    getGroupTree(mb, trc);
                    break;
                }
                case MDFS_SET_FILE_ACCESSED:
                {
                    setFileAccessed(mb, trc);
                    break;
                }
                case MDFS_SET_FILE_PROTECT:
                {
                    setFileProtect(mb, trc);
                    break;
                }
                default:
                {
                    mb.clear();
                    break;
                }
            }
        }
        catch (IException *e)
        {
            int err=-1; // exception marker
            mb.clear().append(err);
            serializeException(e, mb);
            e->Release();
        }
        coven.reply(mb);
        if (block0.slow())
        {
            SocketEndpoint ep = mb.getSender();
            ep.getEndpointHostText(block0.appendMsg(trc).append(" from "));
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
        case MDFS_ITERATE_FILTEREDFILES2:
            return ret.append("MDFS_ITERATE_FILTEREDFILES2");
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
    logNullUser(user);//stack trace if NULL user
    if (user)
    {
        user->serializeWithoutPassword(mb);
    }

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
    logNullUser(user);//stack trace if NULL user
    if (user)
    {
        user->serializeWithoutPassword(mb);
    }

    if (foreigndali)
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

void CDistributedFileDirectory::setFileAccessed(IUserDescriptor* udesc, const char *logicalName, const CDateTime &dt, const INode *foreigndali, unsigned foreigndalitimeout)
{
    CDfsLogicalFileName dlfn;
    dlfn.set(logicalName);
    setFileAccessed(dlfn, udesc, dt, foreigndali, foreigndalitimeout);
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
    logNullUser(user);//stack trace if NULL user
    if (user)
    {
        user->serializeWithoutPassword(mb);
    }
    if (foreigndali)
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

IPropertyTree *CDistributedFileDirectory::getFileTree(const char *lname, IUserDescriptor *user, const INode *foreigndali,unsigned foreigndalitimeout, GetFileTreeOpts opts)
{
    constexpr unsigned gftVersion = 2; // for future use (0 and 1 are reserved for legacy versions)
    bool expandnodes = hasMask(opts, GetFileTreeOpts::expandNodes);
    bool appendForeign = hasMask(opts, GetFileTreeOpts::appendForeign);

    // this accepts either a foreign dali node or a foreign lfn
    Owned<INode> fnode;
    CDfsLogicalFileName dlfn;
    SocketEndpoint ep;
    dlfn.set(lname);
    if (dlfn.isForeign())
    {
        if (!dlfn.getEp(ep))
            throw MakeStringException(-1,"cannot resolve dali ip in foreign file name (%s)",lname);
        fnode.setown(createINode(ep));
        foreigndali = fnode;
        lname = dlfn.get(true);
    }
    if (isLocalDali(foreigndali))
        foreigndali = NULL;

    bool getFileTree2Support;
    if (!foreigndali)
        getFileTree2Support = queryDaliServerVersion().compare("3.17") >= 0;
    else
    {
        CDaliVersion serverVersion, minClientVersion;
        checkForeignDaliVersionInfo(foreigndali, serverVersion, minClientVersion);
        getFileTree2Support = serverVersion.compare("3.17") >= 0;
    }

    CMessageBuffer mb;

    if (getFileTree2Support)
    {
        mb.append((int)MDFS_GET_FILE_TREE2);
        mb.append(gftVersion);
        mb.append(lname);
        // if it's a foreign dali, and unless explicitly requested to remap or explicitly requested to suppress foreign remapping
        // ensure the remap flag is sent.
        if (foreigndali && !hasMask(opts, GetFileTreeOpts::remapToService | GetFileTreeOpts::suppressForeignRemapToService))
            opts |= GetFileTreeOpts::remapToService;

        mb.append(static_cast<unsigned>(opts));
        logNullUser(user);//stack trace if NULL user
        if (user)
        {
            mb.append(true);
            user->serializeWithoutPassword(mb);
        }
        else
        {
            mb.append(false);
        }
    }
    else
    {
        mb.append((int)MDFS_GET_FILE_TREE).append(lname);
        mb.append(MDFS_GET_FILE_TREE_V2);
        if (user)
            user->serializeWithoutPassword(mb);
    }
    if (foreigndali)
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
    if (mb.length()==0)
        return nullptr;

    Owned<IPropertyTree> ret;
    Owned<IFileDescriptor> fdesc;
    if (getFileTree2Support)
    {
        unsigned type; // 1 = regular file, 2 = super
        mb.read(type);
        if (1 == type)
        {
            fdesc.setown(deserializeFileDescriptor(mb));
            ret.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
            fdesc->serializeTree(*ret,expandnodes?0:CPDMSF_packParts);
            /* See server-side code, not sure why this attribute is special/here at top level
            and not part of the IFileDescriptor serialization itself */
            unsigned l;
            mb.read(l);
            if (l)
            {
                StringAttr v((const char *)mb.readDirect(l), l);
                ret->setProp("@modified", v);
            }
        }
        else
        {
            verifyex(2 == type); // no other valid possibility
            ret.setown(createPTree(mb));
        }
    }
    else
    {
        unsigned ver = 0;
        if ((mb.length()>=sizeof(int))&&(*(int *)mb.bufferBase()) == -2) // version indicator
        {
            int i;
            mb.read(i);
            mb.read(ver);
        }
        if (ver==0)
            ret.setown(createPTree(mb));
        else
        {
            CDateTime modified;
            if (ver==MDFS_GET_FILE_TREE_V2) // no longer in use but support for back compatibility
            {
                fdesc.setown(deserializeFileDescriptor(mb));
                if (mb.remaining()>0)
                    modified.deserialize(mb);
            }
            else
                throw MakeStringException(-1,"Unknown GetFileTree serialization version %d",ver);
            ret.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
            fdesc->serializeTree(*ret,expandnodes?0:CPDMSF_packParts);
            if (!modified.isNull())
            {
                StringBuffer dts;
                ret->setProp("@modified",modified.getString(dts).str());
            }
        }
    }
    if (expandnodes)
    {
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

IFileDescriptor *CDistributedFileDirectory::getFileDescriptor(const char *lname, AccessMode accessMode, IUserDescriptor *user, const INode *foreigndali, unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname, user, foreigndali, foreigndalitimeout, GetFileTreeOpts::appendForeign);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        Owned<CDistributedSuperFile> sfile = new CDistributedSuperFile(this, tree, dlfn, accessMode, user);
        return sfile->getFileDescriptor(NULL);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?


    IFileDescriptor * fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),0);
    fdesc->queryProperties().setPropInt("@accessMode", static_cast<int>(accessMode));
    fdesc->setTraceName(lname);
    return fdesc;
}

IDistributedFile *CDistributedFileDirectory::getFile(const char *lname, AccessMode accessMode, IUserDescriptor *user, const INode *foreigndali, unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname, user, foreigndali, foreigndalitimeout, GetFileTreeOpts::appendForeign);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        return new CDistributedSuperFile(this, tree, dlfn, accessMode, user);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?

    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),IFDSF_FOREIGN_GROUP);
    if (!fdesc)
        return NULL;
    fdesc->setTraceName(lname);
    fdesc->queryProperties().setPropInt("@accessMode", static_cast<int>(accessMode));
    CDistributedFile *ret = new CDistributedFile(this, fdesc, user, false);
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

SecAccessFlags CDistributedFileDirectory::getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags)
{
    CDfsLogicalFileName dlfn;
    dlfn.set(lname);
    return getDLFNPermissions(dlfn,user,auditflags);
}

SecAccessFlags CDistributedFileDirectory::getFDescPermissions(IFileDescriptor *fdesc,IUserDescriptor *user,unsigned auditflags)
{
    // this checks have access to the nodes in the file descriptor
    SecAccessFlags retPerms = SecAccess_Full;
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
                SecAccessFlags perm = getDLFNPermissions(dlfn,user,auditflags);
                if (perm < retPerms) {
                    retPerms = perm;
                    if (retPerms == SecAccess_None)
                        return SecAccess_None;
                }
            }
        }
    }
    return retPerms;
}

SecAccessFlags CDistributedFileDirectory::getDLFNPermissions(CDfsLogicalFileName &dlfn,IUserDescriptor *user,unsigned auditflags)
{
    StringBuffer scopes;
    dlfn.getScopes(scopes);
    return getScopePermissions(scopes,user,auditflags);
}

SecAccessFlags CDistributedFileDirectory::getDropZoneScopePermissions(const char *dropZoneName,const char *dropZonePath,IUserDescriptor *user,unsigned auditflags)
{
    CDfsLogicalFileName dlfn;
    dlfn.setPlaneExternal(dropZoneName,dropZonePath);
    return getScopePermissions(dlfn.get(),user,auditflags);
}

SecAccessFlags CDistributedFileDirectory::getFScopePermissions(const char *scope,IUserDescriptor *user,unsigned auditflags)
{
    return getScopePermissions(scope,user,auditflags);
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

IDaliServer *createDaliDFSServer(IPropertyTree *config)
{
    assertex(!daliDFSServer); // initialization problem
    daliDFSServer = new CDaliDFSServer(config);
    return daliDFSServer;
}

IDistributedFileTransaction *createDistributedFileTransaction(IUserDescriptor *user, ICodeContext *ctx)
{
    return new CDistributedFileTransaction(user, NULL, ctx);
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
        Owned<IDistributedFile> file1 = lookup(lfn1, user, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, defaultTimeout);
        Owned<IDistributedFile> file2 = lookup(lfn2, user, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, defaultTimeout);
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
    Owned<IDistributedFile> file = lookup(lfn, user, AccessMode::tbdRead, false, false, NULL, defaultPrivilegedUser, defaultTimeout);
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
class CFilterAttrIterator: implements IDFAttributesIterator, public CInterface
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

/* given a list of group offsets (positions), create a compact representation of the range
 * compatible with the group range syntax, e.g. mygroup[1-5,8-10] or mygroup[1,5,10]
 */
StringBuffer &encodeChildGroupRange(UnsignedArray &positions, StringBuffer &rangeText)
{
    unsigned items = positions.ordinality();
    if (0 == items)
        return rangeText;
    unsigned start = positions.item(0);
    unsigned last = start;
    rangeText.append('[');
    unsigned p=1;
    while (true)
    {
        unsigned pos = p==items ? NotFound : positions.item(p++);
        if ((pos != last+1))
        {
            if (last-start>0)
                rangeText.append(start).append('-').append(last);
            else
                rangeText.append(last);
            if (NotFound == pos)
                break;
            rangeText.append(',');
            start = pos;
        }
        last = pos;
    }
    return rangeText.append(']');
}

class CLightWeightSuperFileConn: implements ISimpleSuperFileEnquiry, public CInterface
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
                    IWARNLOG("migrateSuperOwnersAttr: Could not lock parent %s",query.str());
                    conn.setown(querySDS().connect(query.str(),myProcessSession(),0,defaultTimeout));
                }
                if (conn) {
                    migrateAttr(from,conn->queryRoot());
                    migrateSuperOwnersAttr(conn->queryRoot());
                }
                else
                    IWARNLOG("migrateSuperOwnersAttr could not connect to parent superfile %s",lfn.get());
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
        Owned<IDistributedSuperFile> super = transaction->lookupSuperFile(sfnames[i], AccessMode::tbdWrite);
        if (!super.get()) {
            if (created && createonlyonesuperfile) {
                files = i;
                break;
            }
            Owned<IDistributedSuperFile> sfile = createSuperFile(sfnames[i],user,true,false,transaction);
            created = true;
        }
    }

    // If last file had sub-files, clean and fill outlinked
    Owned<IDistributedSuperFile> last = transaction->lookupSuperFile(sfnames[files-1], AccessMode::tbdWrite);
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
        Owned<IDistributedSuperFile> orig = transaction->lookupSuperFile(sfnames[i-1], AccessMode::tbdWrite);
        Owned<IDistributedSuperFile> dest = transaction->lookupSuperFile(sfnames[i], AccessMode::tbdWrite);
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
    Owned<IDistributedSuperFile> first = transaction->lookupSuperFile(sfnames[0], AccessMode::tbdWrite);
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
    CTimeMon tm(defaultTimeout);
    if (!lock.init(lfn, RTM_LOCK_READ, defaultTimeout, "CDistributedFileDirectory::getFileSuperOwners"))
        return false;
    CFileSuperOwnerLock superOwnerLock;
    unsigned remaining;
    tm.timedout(&remaining);
    verifyex(superOwnerLock.initWithFileLock(lfn, remaining, "CDistributedFileDirectory::getFileSuperOwners(SuperOwnerLock)", lock, RTM_LOCK_READ));
    Owned<IPropertyTreeIterator> iter = lock.queryRoot()->getElements("SuperOwner");
    StringBuffer pname;
    ForEach(*iter) {
        iter->query().getProp("@name",pname.clear());
        if (pname.length())
            owners.append(pname.str());
    }
    return true;
}

class CFileRelationship: implements IFileRelationship, public CInterface
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

class CFileRelationshipIterator: implements IFileRelationshipIterator, public CInterface
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
    Owned<IPropertyTree> file = getFileTree(logicalname.get(), user, NULL, FOREIGN_DALI_TIMEOUT, GetFileTreeOpts::expandNodes|GetFileTreeOpts::appendForeign);
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
            : errcrit(_errcrit), exc(_exc), ips(_ips)
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
    return attrLock.queryRoot()->hasProp("Protect");
}

IDFProtectedIterator *CDistributedFileDirectory::lookupProtectedFiles(const char *owner,bool notsuper,bool superonly)
{
    return new CDFProtectedIterator(owner,notsuper,superonly,defaultTimeout);
}

const char* DFUQResultFieldNames[] = { "@name", "@description", "@group", "@kind", "@modified", "@job", "@owner",
    "@DFUSFrecordCount", "@recordCount", "@recordSize", "@DFUSFsize", "@size", "@workunit", "@DFUSFcluster", "@numsubfiles",
    "@accessed", "@numparts", "@compressedSize", "@directory", "@partmask", "@superowners", "@persistent", "@protect", "@compressed",
    "@cost", "@numDiskReads", "@numDiskWrites", "@atRestCost", "@accessCost", "@maxSkew", "@minSkew", "@maxSkewPart", "@minSkewPart",
    "@readCost", "@writeCost" };

extern da_decl const char* getDFUQResultFieldName(DFUQResultField field)
{
    return DFUQResultFieldNames[field];
}

IPropertyTreeIterator *deserializeFileAttrIterator(MemoryBuffer& mb, unsigned numFiles, DFUQResultField* localFilters, const char* localFilterBuf)
{
    class CFileAttrIterator: implements IPropertyTreeIterator, public CInterface
    {
        size32_t fileDataStart;
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

        void setIsCompressed(IPropertyTree* file)
        {
            if (isCompressed(*file) || isFileKey(*file))
                file->setPropBool(getDFUQResultFieldName(DFUQRFiscompressed), true);
        }

        void setCost(IPropertyTree* file, const char *nodeGroup)
        {
            // Set the following dynamic fields: atRestCost, accessCost, cost and for legacy files: readCost, writeCost
            StringBuffer str;
            double fileAgeDays = 0.0;
            if (file->getProp(getDFUQResultFieldName(DFUQRFtimemodified), str))
            {
                CDateTime dt;
                dt.setString(str.str());
                fileAgeDays = difftime(time(nullptr), dt.getSimple())/(24*60*60);
            }
            __int64 sizeDiskSize = 0;
            if (isCompressed(*file))
                sizeDiskSize = file->getPropInt64(getDFUQResultFieldName(DFUQRFcompressedsize), 0);
            else
                sizeDiskSize = file->getPropInt64(getDFUQResultFieldName(DFUQRForigsize), 0);
            double sizeGB = sizeDiskSize / ((double)1024 * 1024 * 1024);
            cost_type atRestCost = calcFileAtRestCost(nodeGroup, sizeGB, fileAgeDays);
            file->setPropInt64(getDFUQResultFieldName(DFUQRFatRestCost), atRestCost);

            cost_type readCost = getReadCost(*file, nodeGroup, true);
            cost_type writeCost = getWriteCost(*file, nodeGroup, true);
            cost_type accessCost = readCost + writeCost;
            file->setPropInt64(getDFUQResultFieldName(DFUQRFaccessCost), accessCost);
            file->setPropInt64(getDFUQResultFieldName(DFUQRFcost), atRestCost + accessCost);
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
            setIsCompressed(attr);
            const char *firstNodeGroup = attr->queryProp(getDFUQResultFieldName(DFUQRFnodegroup));
            if (!isEmptyString(firstNodeGroup))
                setCost(attr, firstNodeGroup);
            return attr;
        }

        IPropertyTree *duplicateFileAttrOnOtherNodeGroup(IPropertyTree *previousAttr)
        {
            IPropertyTree *attr = getEmptyAttr();
            Owned<IAttributeIterator> ai = previousAttr->getAttributes();
            ForEach(*ai)
                attr->setProp(ai->queryName(),ai->queryValue());
            const char * nodeGroup = fileNodeGroups.item(fileNodeGroups.length()-1);
            attr->setProp(getDFUQResultFieldName(DFUQRFnodegroup), nodeGroup);
            setCost(attr, nodeGroup);
            fileNodeGroups.pop();
            return attr;
        }

    public:
        IMPLEMENT_IINTERFACE;
        MemoryBuffer mb;
        unsigned numfiles;
        StringArray nodeGroupFilter;

        CFileAttrIterator(MemoryBuffer &_mb, unsigned _numfiles) : numfiles(_numfiles)
        {
            /* not particuarly nice, but buffer contains extra meta info ahead of serialized file info
             * record position to rewind to, if iterator reused.
             */
            fileDataStart = _mb.getPos();
            mb.swapWith(_mb);
        }
        bool first()
        {
            mb.reset(fileDataStart);
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

    } *fai = new CFileAttrIterator(mb, numFiles);
    fai->setLocalFilters(localFilters, localFilterBuf);
    return fai;
}

IPropertyTreeIterator *CDistributedFileDirectory::getDFAttributesTreeIterator(const char* filters, DFUQResultField* localFilters,
    const char* localFilterBuf, IUserDescriptor* user, bool recursive, bool& allMatchingFilesReceived, INode* foreigndali, unsigned foreigndalitimeout)
{
    CMessageBuffer mb;
    CDaliVersion serverVersionNeeded("3.13");
    bool legacy = (queryDaliServerVersion().compare(serverVersionNeeded) < 0);
    if (legacy)
        mb.append((int)MDFS_ITERATE_FILTEREDFILES);
    else
        mb.append((int)MDFS_ITERATE_FILTEREDFILES2);
    mb.append(filters).append(recursive);
    if (user)
    {
        user->serializeWithoutPassword(mb);
    }

    if (foreigndali)
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);

    unsigned numfiles;
    mb.read(numfiles);
    if (legacy)
        allMatchingFilesReceived = true; // don't know any better
    else
        mb.read(allMatchingFilesReceived);
    return deserializeFileAttrIterator(mb, numfiles, localFilters, localFilterBuf);
}

IDFAttributesIterator* CDistributedFileDirectory::getLogicalFiles(
    IUserDescriptor* udesc,
    DFUQResultField *sortOrder, // list of fields to sort by (terminated by DFUSFterm)
    const void *filters,  // (appended) string values for filters used by dali server
    DFUQResultField *localFilters, //used for filtering query result received from dali server.
    const void *localFilterBuf,
    unsigned startOffset,
    unsigned maxNum,
    __int64 *cacheHint,
    unsigned *total,
    bool *allMatchingFiles,
    bool recursive,
    bool sorted)
{
    class CDFUPager : implements IElementsPager, public CSimpleInterface
    {
        IUserDescriptor* udesc;
        //StringAttr clusterFilter;
        StringAttr filters;
        DFUQResultField *localFilters;
        StringAttr localFilterBuf;
        StringAttr sortOrder;
        bool recursive, sorted;
        bool allMatchingFilesReceived;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CDFUPager(IUserDescriptor* _udesc, const char*_filters, DFUQResultField*_localFilters, const char*_localFilterBuf,
            const char*_sortOrder, bool _recursive, bool _sorted) : udesc(_udesc), filters(_filters), localFilters(_localFilters), localFilterBuf(_localFilterBuf),
            sortOrder(_sortOrder), recursive(_recursive), sorted(_sorted)
        {
            allMatchingFilesReceived = true;
        }
        virtual IRemoteConnection* getElements(IArrayOf<IPropertyTree> &elements)
        {
            Owned<IPropertyTreeIterator> fi = queryDistributedFileDirectory().getDFAttributesTreeIterator(filters.get(),
                localFilters, localFilterBuf.get(), udesc, recursive, allMatchingFilesReceived);
            StringArray unknownAttributes;
            sortElements(fi, sorted ? sortOrder.get() : NULL, NULL, NULL, unknownAttributes, elements);
            return NULL;
        }
        virtual bool allMatchingElementsReceived() { return allMatchingFilesReceived; }
    };

    StringBuffer so;
    if (sorted && sortOrder)
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
            if (fmt&DFUQRFfloat)
                so.append('~');
            so.append(getDFUQResultFieldName((DFUQResultField) (fmt&0xff)));
        }
    }
    IArrayOf<IPropertyTree> results;
    Owned<IElementsPager> elementsPager = new CDFUPager(udesc, (const char*) filters, localFilters, (const char*) localFilterBuf,
        so.length()?so.str():NULL, recursive, sorted);
    Owned<IRemoteConnection> conn = getElementsPaged(elementsPager,startOffset,maxNum,NULL,"",cacheHint,results,total,allMatchingFiles,false);
    return new CDFAttributeIterator(results);
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
    return getLogicalFiles(udesc, sortOrder, filters, localFilters, localFilterBuf, startOffset, maxNum,
        cacheHint, total, allMatchingFiles, true, true);
}

bool CDistributedFileDirectory::removePhysicalPartFiles(const char *logicalName, IFileDescriptor *fileDesc, IMultiException *mexcept, unsigned numParallelDeletes)
{
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
                        LOG(MCwarning, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getEndpointHostText(eps).str());
                    else
                    {
                        unsigned t = msTick()-start;
                        if (t>5*1000)
                            LOG(MCwarning, "Removing %s from %s took %ds", partfile->queryFilename(), rfn.queryEndpoint().getEndpointHostText(eps).str(), t/1000);
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
                        rfn.queryEndpoint().getEndpointHostText(s);
                        EXCLOG(e, s.str());
                        e->Release();
                    }
                    ok = false;
                }
            }
        }
    } afor(fileDesc, mexcept);
    afor.islazy = fileDesc->queryProperties().getPropBool("@lazy");
    if (0 == numParallelDeletes)
        numParallelDeletes = fileDesc->numParts();
    if (numParallelDeletes > MAX_PHYSICAL_DELETE_THREADS)
    {
        WARNLOG("Limiting parallel physical delete threads to %d", MAX_PHYSICAL_DELETE_THREADS);
        numParallelDeletes = MAX_PHYSICAL_DELETE_THREADS;
    }
    afor.For(fileDesc->numParts(),numParallelDeletes,false,true);
    return afor.ok;
}

void configurePreferredPlanes()
{
    StringBuffer preferredPlanes;
    Owned<IPropertyTreeIterator> iter = getComponentConfigSP()->getElements("preferredDataReadPlanes");
    if (iter->first())
    {
        preferredPlanes.append(iter->query().queryProp(nullptr));
        while (iter->next())
        {
            preferredPlanes.append(',');
            preferredPlanes.append(iter->query().queryProp(nullptr));
        }
        queryDistributedFileDirectory().setDefaultPreferredClusters(preferredPlanes);
        PROGLOG("Preferred read planes: %s", preferredPlanes.str());
    }
}

static bool doesPhysicalMatchMeta(IPropertyTree &partProps, IFile &iFile, offset_t expectedSize, offset_t &actualSize)
{
    constexpr unsigned delaySecs = 5;
    // NB: temporary workaround for 'narrow' files publishing extra empty parts with the wrong @compressedSize(0)
    // causing a new check introduced in HPCC-33064 to be hit (fixed in HPCC-33113, but will continue to affect exiting files)
    unsigned __int64 size = partProps.getPropInt64("@size", unknownFileSize);
    unsigned __int64 compressedSize = partProps.getPropInt64("@compressedSize", unknownFileSize);
    if ((0 == size) && (0 == compressedSize))
    {
        // either this is a file from 9.10 where empty compressed files can be zero length (no header)
        // or it's a pre 9.10 (and pre HPCC-33133 fix) dummy part created with incorrect a @compressedSize of 0
        // (also in future empty physical files may legitimately not exist)
        // If file exists check that the size is either 0, or the compressed header size (the size of an empty compressed file pre 9.10)
        actualSize = iFile.size();
        if (unknownFileSize != actualSize) // file exists. NB: ok not to exist for future compatibility where 0-length files not written
        {
            if (0 != actualSize) // could be zero if file from >= 9.10
            {
                constexpr size32_t nonEmptyCompressedFileSize = 56; // min size of a non-empty compressed file (with header) - 56 bytes
                if (nonEmptyCompressedFileSize != actualSize)
                {
                    // in >= 9.8 - this could 1st check getWriteSyncMarginMs() and only check if not set.
                    WARNLOG("Empty compressed file %s's size (%" I64F "u) is not expected size of 0 or %u - retry after %u second delay", iFile.queryFilename(), actualSize, nonEmptyCompressedFileSize, delaySecs);
                    MilliSleep(delaySecs * 1000);
                    actualSize = iFile.size();
                    if ((0 != actualSize) && (nonEmptyCompressedFileSize != actualSize))
                        return false; // including if unknownFileSize - no longer exists!
                }
            }
        }
        return true;
    }
    else if (expectedSize != unknownFileSize)
    {
        actualSize = iFile.size();
        if (actualSize != expectedSize)
        {
            // in >= 9.8 - this could 1st check getWriteSyncMarginMs() and only check if not set.
            WARNLOG("File %s's size (%" I64F "u) does not match meta size (%" I64F "u) - retry after %u second delay", iFile.queryFilename(), actualSize, expectedSize, delaySecs);
            MilliSleep(delaySecs * 1000);
            actualSize = iFile.size();
            if (actualSize != expectedSize)
                return false;
        }
    }
    else
        actualSize = unknownFileSize;

    return true;
}

bool doesPhysicalMatchMeta(IPartDescriptor &partDesc, IFile &iFile, offset_t &expectedSize, offset_t &actualSize)
{
    IPropertyTree &partProps = partDesc.queryProperties();
    expectedSize = partDesc.getDiskSize(false, false);
    return doesPhysicalMatchMeta(partProps, iFile, expectedSize, actualSize);
}

bool doesPhysicalMatchMeta(IDistributedFilePart &part, IFile &iFile, offset_t &expectedSize, offset_t &actualSize)
{
    IPropertyTree &partProps = part.queryAttributes();
    expectedSize = part.getDiskSize(false, false);
    return doesPhysicalMatchMeta(partProps, iFile, expectedSize, actualSize);
}


#ifdef _USE_CPPUNIT
/*
 * This method removes files only logically. removeEntry() used to do that, but the only
 * external use for logical-files only is this test-suite, so I'd rather hack the test
 * suite than expose the behaviour to more viewers.
 */
extern da_decl void removeLogical(const char *fname, IUserDescriptor *user) {
    if (queryDistributedFileDirectory().exists(fname, user)) {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(fname, user, AccessMode::tbdWrite, false, false, nullptr, defaultPrivilegedUser);
        CDistributedFile *f = QUERYINTERFACE(file.get(),CDistributedFile);
        assert(f);
        f->detachLogical();
    }
}
#endif // _USE_CPPUNIT
