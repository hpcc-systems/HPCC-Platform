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

#define DFSSERVER_THROTTLE_COUNT 20
#define DFSSERVER_THROTTLE_TIME 1000

#define SUBFILE_COMPATIBILITY_CHECKING

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
    MDFS_MAX
};

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
        }
    }
    return in;
}



static IPropertyTree *getCreatePropTree(IPropertyTree *parent,const char * name)
{  // creates if not existing
    assertex(strchr(name,'/')==NULL);
    IPropertyTree *t = parent->queryPropTree(name);
    if (!t) {
        t = parent->setPropTree(name,createPTree(name)); // takes ownership
    }
    return LINK(t);
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

static IPropertyTreeIterator *getNamedPropIter(IPropertyTree *parent,const char *sub,const char *key,const char *name)
{  // no create
    if (!parent)
        return NULL;
    StringBuffer query;
    getAttrQueryStr(query,sub,key,name);
    return parent->getElements(query.str());
}

static IPropertyTree *addNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name, IPropertyTree* init=NULL)
{  
    IPropertyTree* ret = init?createPTreeFromIPT(init):createPTree(sub);
    assertex(key[0]=='@');
    ret->setProp(key,name);
    ret = parent->addPropTree(sub,ret);
    return LINK(ret);
}

static void removeNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name)
{
    StringBuffer query;
    getAttrQueryStr(query,sub,key,name);
    while (parent->removeProp(query.str()));
}

static IPropertyTree *createNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name)
{  
    removeNamedPropTree(parent,sub,key,name);
    return addNamedPropTree(parent,sub,key,name);
}

static IPropertyTree *getCreateNamedPropTree(IPropertyTree *parent,const char *sub,const char *key,const char *name)
{  // create if not present
    StringBuffer query;
    IPropertyTree *t = parent->getPropTree(getAttrQueryStr(query,sub,key,name).str());
    if (t)
        return t;
    return addNamedPropTree(parent,sub,key,name);
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

class CPauseTransaction
{
    IDistributedFileTransaction *transaction;
    bool active;
public:
    CPauseTransaction(IDistributedFileTransaction *_transaction)
    {
        transaction = _transaction;
        active = transaction?transaction->setActive(false):false;
    }
    ~CPauseTransaction()
    {
        if (active)
            transaction->setActive(true);
    }
};


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
    CConnectLock(const char *caller,const char *name,bool write,bool preload,unsigned timeout)
    {
        unsigned start = msTick();
        bool first = true;
        loop {
            try {
                conn.setown(querySDS().connect(name,queryCoven().inCoven()?0:myProcessSession(),(write?RTM_LOCK_WRITE:RTM_LOCK_READ)|(preload?RTM_SUB:0),(timeout==INFINITE)?1000*60*5:timeout));
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
    CConnectLock connlock("ensureFileScope",querySdsFilesRoot(),true,false,timeout); 
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
    CConnectLock connlock("removeFileEmptyScope",querySdsFilesRoot(),true,false,timeout); //*1
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


class CFileConnectLock
{
    CConnectLock *lock;
    bool attronly;
public:
    CFileConnectLock(bool _attronly=false)
    {
        lock = NULL;
        attronly = _attronly;
    }
    CFileConnectLock(const char *caller,const CDfsLogicalFileName &lname,DfsXmlBranchKind bkind,bool write,bool preload,unsigned timeout,bool _attronly=false)
    {
        lock = NULL;
        attronly = _attronly;
        init(caller,lname,bkind,write,preload,timeout);
    }
    ~CFileConnectLock()
    {
        delete lock;
    }

    bool init(const char *caller,const CDfsLogicalFileName &lname,DfsXmlBranchKind bkind,bool write,bool preload,unsigned timeout)
    {
        kill();
        StringBuffer query;
        lname.makeFullnameQuery(query,bkind,true);
        if (attronly)
            query.append("/Attr");
        lock = new CConnectLock(caller,query.str(),write,preload,timeout);
        return lock->conn.get()!=NULL;
    }

    bool initany(const char *caller,const CDfsLogicalFileName &lname,DfsXmlBranchKind &bkind,bool write,bool preload,unsigned timeout)
    {
        if (init(caller,lname,DXB_File,write,preload,timeout)) {
            bkind = DXB_File;
            return true;
        }
        if (init(caller,lname,DXB_SuperFile,write,preload,timeout)) {
            bkind = DXB_SuperFile;
            return true;
        }
        return false;
    }

    IRemoteConnection *detach()
    {
        return lock?lock->detach():NULL;
    }

    IRemoteConnection *conn()
    {
        return lock?lock->conn:NULL;
    }

    IPropertyTree *queryRoot() const
    {
        return (lock&&lock->conn.get())?lock->conn->queryRoot():NULL;
    }

    void remove()
    {
        if (lock&&lock->conn.get()) {
#ifdef TRACE_LOCKS
            PROGLOG("CFileConnectLock:remove%s",attronly?" Attr":"");
            LogRemoteConn(lock->conn);
#endif
            lock->conn->close(true);
        }
    }

    void kill()
    {
        delete lock;
        lock = NULL;
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
    CScopeConnectLock(const char *caller,const CDfsLogicalFileName &lname,bool write,bool preload,unsigned timeout)
    {
        lock = NULL;
        init(caller,lname,write,preload,timeout);
    }
    ~CScopeConnectLock()
    {
        delete lock;
    }

    bool init(const char *caller,const CDfsLogicalFileName &lname,bool write,bool preload,unsigned timeout)
    {
        delete lock;
        StringBuffer query;
        lname.makeScopeQuery(query,true);
        lock = new CConnectLock(caller,query.str(),write,preload,timeout);
        if (lock->conn.get()==NULL) {
            delete lock;
            lock = NULL;
            ensureFileScope(lname);
            lock = new CConnectLock(caller,query.str(),write,preload,timeout);
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
    CClustersLockedSection(CDfsLogicalFileName &dlfn)
    {
        StringBuffer xpath;
        dlfn.makeFullnameQuery(xpath,DXB_File,true).append("/ClusterLock");
        conn.setown(querySDS().connect(xpath.str(), myProcessSession(), RTM_CREATE | RTM_LOCK_WRITE | RTM_DELETE_ON_DISCONNECT, SDS_CONNECT_TIMEOUT));
    }

    ~CClustersLockedSection()
    {
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

    unsigned find(const char *clustername)
    {
        StringBuffer name;
        ForEachItem(i)  {
            if (strcmp(item(i).getClusterLabel(name.clear()).str(),clustername)==0) 
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


class CDistributedFileDirectory: public CInterface, implements IDistributedFileDirectory
{
    CriticalSection removesect;
    Owned<IUserDescriptor> defaultudesc;
    Owned<IDFSredirection> redirection;
    
    IDistributedFile *createExternal(const CDfsLogicalFileName &logicalname);
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

    IDistributedFile *dolookup(const CDfsLogicalFileName &logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction,bool fixmissing, unsigned timeout);

    IDistributedFile *lookup(const char *_logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction,unsigned timeout);
    IDistributedFile *lookup(const CDfsLogicalFileName &logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction,unsigned timeout);
    
    IDistributedFile *createNew(IFileDescriptor * fdesc, const char *lname,bool includeports=false);
    IDistributedFile *createNew(IFileDescriptor * fdesc, bool includeports=false)
    {
        return createNew(fdesc,NULL,includeports);
    }
    IDistributedFile *createNew(IPropertyTree *tree,bool ignoregroup);
    IDistributedSuperFile *createSuperFile(const char *logicalname,bool interleaved,bool ifdoesnotexist,IUserDescriptor *user);

    IDistributedFileIterator *getIterator(const char *wildname, bool includesuper,IUserDescriptor *user);
    IDFAttributesIterator *getDFAttributesIterator(const char *wildname, bool recursive, bool includesuper,INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout);
    IDFAttributesIterator *getForeignDFAttributesIterator(const char *wildname, bool recursive=true, bool includesuper=false, const char *foreigndali="",IUserDescriptor *user=NULL, unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT)
    {
        Owned<INode> foreign;
        if (foreigndali&&*foreigndali) {
            SocketEndpoint ep(foreigndali);
            foreign.setown(createINode(ep));
        }
        return getDFAttributesIterator(wildname, recursive, includesuper,foreign,user,foreigndalitimeout);
    }

    IDFScopeIterator *getScopeIterator(const char *subscope,bool recursive,bool includeempty,IUserDescriptor *user);
    bool loadScopeContents(const char *scopelfn,StringArray *scopes,    StringArray *supers,StringArray *files, bool includeemptyscopes);

    IPropertyTree *getFileTree(const char *lname,const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout,bool expandnodes=false); 
    void setFileAccessed(CDfsLogicalFileName &dlfn, const CDateTime &dt,const INode *foreigndali=NULL,IUserDescriptor *user=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IFileDescriptor *getFileDescriptor(const char *lname,const INode *foreigndali=NULL,IUserDescriptor *user=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);
    IDistributedFile *getFile(const char *lname,const INode *foreigndali=NULL,IUserDescriptor *user=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

    bool exists(const char *_logicalname,bool notsuper=false,bool superonly=false,IUserDescriptor *user=NULL);
    bool existsPhysical(const char *_logicalname,IUserDescriptor *user);

    void addEntry(CDfsLogicalFileName &lfn,IPropertyTree *root,bool superfile, bool ignoreexists);
    bool removeEntry(const char *_logicalname,IUserDescriptor *user);
    bool removePhysical(const char *_logicalname,unsigned short port,const char *cluster,IMultiException *mexcept,IUserDescriptor *user);
    bool renamePhysical(const char *oldname,const char *newname,unsigned short port,IMultiException *exceptions,IUserDescriptor *user);
    void removeEmptyScope(const char *name);

    IDistributedSuperFile *lookupSuperFile(const char *logicalname,IUserDescriptor *user,IDistributedFileTransaction *transaction,bool fixmissing=false,unsigned timeout=INFINITE);

    void linkSuperOwner(IDistributedFile &subfile,const char *superfile,bool link,IDistributedFileTransaction *transaction);

    int getFilePermissions(const char *lname,IUserDescriptor *user,unsigned auditflags);
    int getNodePermissions(const IpAddress &ip,IUserDescriptor *user,unsigned auditflags);
    int getFDescPermissions(IFileDescriptor *,IUserDescriptor *user=NULL,unsigned auditflags=0);
    void setDefaultUser(IUserDescriptor *user);
    IUserDescriptor* queryDefaultUser();

    bool doRemovePhysical(CDfsLogicalFileName &dlfn,unsigned short port,const char *cluster,IMultiException *mexcept,IUserDescriptor *user,bool ignoresub);
    bool doRemoveEntry(CDfsLogicalFileName &dlfn,IUserDescriptor *user,bool ignoresub);
    DistributedFileCompareResult fileCompare(const char *lfn1,const char *lfn2,DistributedFileCompareMode mode,StringBuffer &errstr,IUserDescriptor *user);
    bool filePhysicalVerify(const char *lfn1,bool includecrc,StringBuffer &errstr,IUserDescriptor *user);
    void setDefaultPreferredClusters(const char *clusters);
    void fixDates(IDistributedFile *fil,unsigned short port);

    GetFileClusterNamesType getFileClusterNames(const char *logicalname,StringArray &out); // returns 0 for normal file, 1 for

    bool isSuperFile( const char *logicalname, INode *foreigndali, IUserDescriptor *user, unsigned timeout);

    void promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user, unsigned timeout, StringArray &outunlinked);
    ISimpleSuperFileEnquiry * getSimpleSuperFileEnquiry(const char *logicalname,const char *title,unsigned timeout);
    bool getFileSuperOwners(const char *logicalname, StringArray &owners);

    IDFSredirection & queryRedirection() { return *redirection; }

    static StringBuffer &getFileRelationshipXPath(StringBuffer &xpath, const char *primary, const char *secondary,const char *primflds,const char *secflds,
                                                const char *kind, const char *cardinality,  const bool *payload
        );
    void doRemoveFileRelationship( IRemoteConnection *conn, const char *primary,const char *secondary,const char *primflds,const char *secflds, const char *kind);
    void removeFileRelationships(const char *primary,const char *secondary, const char *primflds, const char *secflds, const char *kind);
    void addFileRelationship(const char *kind,const char *primary,const char *secondary,const char *primflds, const char *secflds,const char *cardinality,bool payload,const char *description);
    IFileRelationshipIterator *lookupFileRelationships(const char *primary,const char *secondary,const char *primflds,const char *secflds,
                                                       const char *kind,const char *cardinality,const bool *payload,
                                                       const char *foreigndali,unsigned foreigndalitimeout);
    void removeAllFileRelationships(const char *filename);
    IFileRelationshipIterator *lookupAllFileRelationships(const char *filenames);

    void renameFileRelationships(const char *oldname,const char *newname,IFileRelationshipIterator *reliter);

    bool publishMetaFileXML(const CDfsLogicalFileName &logicalname,IUserDescriptor *user);
    IFileDescriptor *createDescriptorFromMetaFile(const CDfsLogicalFileName &logicalname,IUserDescriptor *user);
    
    bool isProtectedFile(const CDfsLogicalFileName &logicalname, unsigned timeout) ;
    unsigned queryProtectedCount(const CDfsLogicalFileName &logicalname, const char *owner);                    
    bool getProtectedInfo(const CDfsLogicalFileName &logicalname, StringArray &names, UnsignedArray &counts);
    IDFProtectedIterator *lookupProtectedFiles(const char *owner=NULL,bool notsuper=false,bool superonly=false);

    static bool cannotRemove(CDfsLogicalFileName &name,IPropertyTree *froot,StringBuffer &reason,bool ignoresub, bool checkprotonly, unsigned timeoutms);
    void setFileProtect(CDfsLogicalFileName &dlfn, const char *owner, bool set, const INode *foreigndali=NULL,IUserDescriptor *user=NULL,unsigned foreigndalitimeout=FOREIGN_DALI_TIMEOUT);

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
protected:
    Linked<IDistributedFileTransaction> transaction;
public:
    CDFAction(IDistributedFileTransaction *transaction);
    virtual ~CDFAction() {}
    virtual bool lock()=0;
    virtual void run()=0;
    virtual void unlock(bool commit,bool rollback)=0;
};

static void setUserDescriptor(Linked<IUserDescriptor> &udesc,IUserDescriptor *user)
{
    if (!user)
        user = queryDistributedFileDirectory().queryDefaultUser();
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
            user = queryDistributedFileDirectory().queryDefaultUser();
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
    int perm = getScopePermissions(scopename,user,auditflags);
    IDFS_Exception *e = NULL;
    if (readreq&&!HASREADPERMISSION(perm)) 
        e = new CDFS_Exception(DFSERR_LookupAccessDenied,scopename);
    else if (createreq&&!HASWRITEPERMISSION(perm)) 
        e = new CDFS_Exception(DFSERR_CreateAccessDenied,scopename);
    if (e) 
        throw e;
}

static bool checkLogicalName(const CDfsLogicalFileName &dlfn,IUserDescriptor *user,bool readreq,bool createreq,bool allowquery,const char *specialnotallowedmsg)
{
    bool ret = true;
    if (dlfn.isMulti()) {
        if (specialnotallowedmsg)
            throw MakeStringException(-1,"cannot %s a multi file name (%s)",specialnotallowedmsg,dlfn.get());
        unsigned i = dlfn.multiOrdinality();
        while (--i)
            ret = checkLogicalName(dlfn.multiItem(i),user,readreq,createreq,allowquery,specialnotallowedmsg)&&ret;
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

static bool checkNeedFQ(const char *filename, const char *dir, const char *partHead, StringBuffer &out)
{
    if (!dir || !*dir || (!containsPathSepChar(filename) && partHead && 0 != strcmp(partHead, dir)))
    {
        out.append(partHead).append(getPathSepChar(dir)).append(filename);
        return true;
    }
    out.append(filename);
    return false;
}

static StringBuffer &checkNeedFQ(const char *filename, const char *dir, RemoteFilename &rfn, StringBuffer &out)
{
    if (!dir || !*dir)
        return rfn.getLocalPath(out);

    if (!containsPathSepChar(filename))
    {
        rfn.getLocalPath(out);
        StringBuffer head;
        splitDirTail(out.str(), head);
        if (head.length() && 0 != strcmp(head.str(), dir))
            return out;
        out.clear();
    }
    return out.append(filename);
}

class CDelayedDelete: public CInterface
{
    StringAttr lfn;
    bool remphys;
    Linked<IUserDescriptor> user;
public:
    CDelayedDelete(const char *_lfn,bool _remphys,IUserDescriptor *_user)
        : lfn(_lfn),user(_user)
    {
        remphys = _remphys;
    }
    void doDelete()
    {
        try {
            if (remphys) 
                queryDistributedFileDirectory().removePhysical(lfn.get(),0,NULL,NULL,user);
            else 
                queryDistributedFileDirectory().removeEntry(lfn.get(),user);
        }
        catch (IException *e) {

            StringBuffer s;
            s.appendf("Transaction commit deleting %s: ",lfn.get());
            e->errorMessage(s);
            WARNLOG("%s",s.str());
            e->Release();
        }
    }
};

class CDistributedFileTransaction: public CInterface, implements IDistributedFileTransaction
{
    CIArrayOf<CDFAction> actions;
    IArrayOf<IDistributedFile> dflist; // owning list of files
    bool isactive;
    Linked<IUserDescriptor> udesc;
    CIArrayOf<CDelayedDelete> delayeddelete;
public:
    IMPLEMENT_IINTERFACE;
    CDistributedFileTransaction(IUserDescriptor *user)
    {
        setUserDescriptor(udesc,user);
        isactive = false;
    }
    void addAction(CDFAction *action)
    {
        actions.append(*action);
    }
    void start()
    {
        if (isactive)
            throw MakeStringException(-1,"Transaction already started");
        isactive = true;
        assertex(actions.ordinality()==0);
    }
    void commit()
    {
        if (!isactive)
            return;
        IException *rete=NULL;
        unsigned nlocked=0;
        try {
            loop {
                ForEachItemIn(i0,actions) 
                    if (actions.item(i0).lock())
                        nlocked++;
                    else
                        break;
                if (nlocked==actions.ordinality())
                    break;
                while (nlocked) // unlock for retry
                    actions.item(--nlocked).unlock(false,false);
                dflist.kill();
                PROGLOG("CDistributedFileTransaction: Transaction pausing");
                Sleep(SDS_TRANSACTION_RETRY/2+(getRandom()%SDS_TRANSACTION_RETRY)); 
            }
        }
        catch (IException *e) {
            rete = e;
        }
        if (!rete) { // all locked so run
            try {
                ForEachItemIn(i,actions)
                    actions.item(i).run();
                while (actions.ordinality()) {  // if we get here everything should work!
                    Owned<CDFAction> action = &actions.popGet();
                    action->unlock(true,false);
                }
                dflist.kill();
                isactive = false;
                deleteFiles();
                return;
            }
            catch (IException *e) {
                rete = e;
            }
        }
        try {
            while (actions.ordinality()) {  
                try {
                    // slightly strange code
                    Owned<CDFAction> action = &actions.popGet();
                    if (actions.ordinality()<nlocked)
                        action->unlock(false,true);
                }
                catch (IException *e) {
                    if (rete)
                        e->Release();
                    else
                        rete = e;
                }
            }
        }
        catch (IException *e) {
            e->Release();
        }
        rollback();
        throw rete;
    }
    void rollback()
    {
        if (!isactive)
            return;
        isactive = false;
        delayeddelete.kill();
        actions.kill();
        dflist.kill();
    }

    IDistributedFile *findFile(const char *name)
    {
        // dont expect *that* many so do a linear search for now
        StringBuffer tmp;
        name = normalizeLFN(name,tmp);
        ForEachItemIn(i,dflist) {
            if (stricmp(name,dflist.item(i).queryLogicalName())==0)
                return &dflist.item(i);
        }
        return NULL;
    }
    
    IDistributedFile *lookupFile(const char *name,unsigned timeout)
    {
        // dont expect *that* many so do a linear search for now
        IDistributedFile * ret = findFile(name);
        if (ret) 
            return LINK(ret);
        ret = queryDistributedFileDirectory().lookup(name,udesc,false,this,timeout);
        if (!ret)
            return NULL;
        if (isactive) {
            ret->Link();
            dflist.append(*ret);
        }
        return ret;
    }

    IDistributedSuperFile *lookupSuperFile(const char *name, bool fixmissing,unsigned timeout)
    {
        IDistributedSuperFile *ret;
        IDistributedFile * f = findFile(name);
        if (f) {
            ret = f->querySuperFile();
            if (ret)
                return LINK(ret);
        }
        ret = queryDistributedFileDirectory().lookupSuperFile(name,udesc,this,fixmissing,timeout);
        if (!ret)
            return NULL;
        if (isactive) {
            ret->Link();
            dflist.append(*ret);
        }
        return ret;
    }

    bool active()
    {
        return isactive;
    }

    bool setActive(bool on)
    {
        bool old = isactive;
        isactive = on;
        return old;
    }

    void clearFiles()
    {
        dflist.kill();
    }

    IUserDescriptor *queryUser()
    {
        return udesc;
    }

    bool addDelayedDelete(const char *lfn,bool remphys,IUserDescriptor *user)
    {
        delayeddelete.append(*new CDelayedDelete(lfn,remphys,user));
        return true;
    }
    
    void deleteFiles()      // no rollback, no exceptions thrown, no regrets
    {
        ForEachItemIn(i,delayeddelete) {
            delayeddelete.item(i).doDelete();
        }
        delayeddelete.kill();
    }

    IDistributedFileTransaction *baseTransaction() { return this; }
};

class CWrappedTransaction: public CInterface, implements IDistributedFileTransaction
{
    Linked<IDistributedFileTransaction> chain;
    IDistributedFile *owner;
    Linked<IUserDescriptor> udesc;
    bool isactive;
public:
    IMPLEMENT_IINTERFACE;

    CWrappedTransaction (IDistributedFileTransaction *_chain,IDistributedFile *_owner, IUserDescriptor *_udesc)
        : chain(_chain), udesc(_udesc)
    {
        owner = _owner;
        isactive = false;
    }
    void start()  { if (chain) chain->start(); }
    void commit() { if (chain) chain->commit(); }
    void rollback() { if (chain) chain->rollback(); }
    bool active()  { if (chain) return chain->active(); return isactive; }
    bool setActive(bool on) { if (chain) return chain->setActive(on); bool ret = isactive; isactive = on; return ret; }
    IDistributedFile *lookupFile(const char *lfn,unsigned timeout=INFINITE)
    {
        if (owner&&(strcmp(lfn,owner->queryLogicalName())==0))
            return LINK(owner);
        if (chain)
            chain-> lookupFile(lfn,timeout);
        return queryDistributedFileDirectory().lookup(lfn,udesc,false,NULL,timeout);
    }
    IDistributedSuperFile *lookupSuperFile(const char *slfn,bool fixmissing=false,unsigned timeout=INFINITE)
    {
        if (owner) {
            IDistributedSuperFile * super = owner->querySuperFile();
            if (super&&(strcmp(slfn,super->queryLogicalName())==0))
                return LINK(super);
        }
        if (chain)
            return chain->lookupSuperFile(slfn,fixmissing,timeout);
        return queryDistributedFileDirectory().lookupSuperFile(slfn,udesc,NULL,fixmissing,timeout);
    }
    IUserDescriptor *queryUser() { if (chain) return chain->queryUser(); return udesc; }
    bool addDelayedDelete(const char *lfn,bool remphys,IUserDescriptor *user)
    {
        if (chain)
            return chain->addDelayedDelete(lfn,remphys,user);
        return false;
    }
    void addAction(CDFAction *action)
    {
        if (chain)
            chain->addAction(action);
        else
            WARNLOG("addAction called on CWrappedTransaction without chain");
    }
    void clearFiles()
    {
        if (chain)
            chain->clearFiles();
        else
            WARNLOG("clearFiles called on CWrappedTransaction without chain");
    }

    IDistributedFileTransaction *baseTransaction() { return chain?chain->baseTransaction():NULL; }

    void clearOwner()
    {
        owner = NULL;
    }
};


class CDistributedFileIterator: public CInterface, implements IDistributedFileIterator
{
    unsigned index;
    Owned<IDistributedFile> cur;
    IDistributedFileDirectory *dir;
    PointerArray files;
    Linked<IUserDescriptor> udesc;

public:
    IMPLEMENT_IINTERFACE;

    CDistributedFileIterator(IDistributedFileDirectory *_dir,const char *wildname,bool includesuper,IUserDescriptor *user) 
    {
        setUserDescriptor(udesc,user);
        if (!wildname||!*wildname)
            wildname = "*";
        dir = _dir;
        bool recursive = (stricmp(wildname,"*")==0);
        Owned<IDFAttributesIterator> attriter = dir->getDFAttributesIterator(wildname,recursive,includesuper,NULL,user);
        ForEach(*attriter) {
            IPropertyTree &pt = attriter->query();
            files.append(strdup(pt.queryProp("@name")));
        }
        index = 0;
        if (files.ordinality()>1)
            qsortvec(files.getArray(),files.ordinality(),strcompare);
    }

    ~CDistributedFileIterator()
    {
        ForEachItemIn(i,files) {
            free(files.item(i));
        }
    }
    
    bool first()
    {
        index = 0;
        return set();
    }

    bool set()
    {
        cur.clear();
        while (isValid()) {
            cur.setown(dir->lookup(queryName(),udesc,false,NULL));
            if (cur)
                return true;
            index++;
        }
        return false;
    }

    bool next()
    {   
        index++;
        return set();
    }

    bool  isValid()
    {
        return index<files.ordinality();
    }

    const char *queryName()
    {
        return (const char *)files.item(index);
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
            CConnectLock connlock("CDFScopeIterator",querySdsFilesRoot(),false,false,timeout); 
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


class CDFAttributeIterator: public CInterface, implements IDFAttributesIterator
{
    IArrayOf<IPropertyTree> attrs; 
    unsigned index;
public:
    IMPLEMENT_IINTERFACE;

    static MemoryBuffer &serializeFileAttributes(MemoryBuffer &mb, IPropertyTree &root, StringBuffer &name, bool issuper)
    {
        StringBuffer buf;
        mb.append(name.str());
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
public:

    virtual void Link(void) const;
    virtual bool Release(void) const;
    void set(IPropertyTree *pt,FileClusterInfoArray &clusters,unsigned maxcluster);
    RemoteFilename &getFilename(RemoteFilename &ret,unsigned copy);
    void renameFile(IFile *file);
    unsigned getCRC();
    IPropertyTree &queryProperties();
    IPropertyTree &lockProperties(unsigned timems);
    void unlockProperties();
    bool isHost(unsigned copy);
    offset_t getFileSize(bool allowphysical,bool forcephysical);
    offset_t getDiskSize();
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

class CDistributedFilePartIterator: public CInterface, implements IDistributedFilePartIterator
{
    unsigned idx;
    CDistributedFilePartArray partsa;
public:
    IMPLEMENT_IINTERFACE;
    CDistributedFilePartIterator(CDistributedFilePartArray &parts,IDFPartFilter *filter)
    {
        idx = 0;
        ForEachItemIn(i,parts) {
            if (!filter||filter->includePart(i))
                appendPart(LINK(&parts.item(i)));
        }
    }
    CDistributedFilePartIterator()
    {
        idx = 0;
    }
    ~CDistributedFilePartIterator()
    {
    }

    void appendPart(CDistributedFilePart *part) // takes ownership
    {
        if (part)
            partsa.append(*part);
    }

    bool first()
    {
        if (partsa.ordinality()==0)
            return false;
        idx = 0;
        return isValid();
    }
    bool next()
    {
        idx++;
        return isValid();
    }
    bool isValid()
    {
        return idx<partsa.ordinality();
    }
    IDistributedFilePart & query()
    {
        return partsa.item(idx);
    }
    IDistributedFilePart & get()
    {
        IDistributedFilePart &ret = partsa.item(idx);
        ret.Link();
        return ret;
    }

    CDistributedFilePartArray &queryParts()
    {
        return partsa;
    }
};


class CDistributedSuperFileIterator: public CInterface, implements IDistributedSuperFileIterator
{
    StringAttrArray names;
    unsigned idx;
    Owned<IDistributedSuperFile> cur;
    CDistributedFileDirectory *parent;
    Linked<IUserDescriptor> udesc;
    Linked<IDistributedFileTransaction> transaction;
public:
    IMPLEMENT_IINTERFACE;
    CDistributedSuperFileIterator(CDistributedFileDirectory *_parent,IPropertyTree *root,IUserDescriptor *user, IDistributedFileTransaction *_transaction)
        : transaction(_transaction)
    {
        setUserDescriptor(udesc,user);
        parent = _parent;
        idx = 0;
        if (root) {
            Owned<IPropertyTreeIterator> iter = root->getElements("SuperOwner");
            StringBuffer pname;
            ForEach(*iter) {
                iter->query().getProp("@name",pname.clear());
                if (pname.length())
                    names.append(* new StringAttrItem(pname.str()));
            }
        }
    }
    ~CDistributedSuperFileIterator()
    {
        cur.clear();
    }
    bool first()
    {
        if (names.ordinality()==0)
            return false;
        idx = 0;
        return true;
    }
    bool next()
    {
        idx++;
        if (idx>=names.ordinality()) 
            return false;
        return true;
    }
    bool isValid()
    {
        return idx<names.ordinality();
    }
    IDistributedSuperFile & query()
    {
        do {
            if (transaction.get())
                cur.setown(transaction->lookupSuperFile(queryName()));
            else
                cur.setown(parent->lookupSuperFile(queryName(),udesc,NULL));
        } while (!cur.get()&&next()); // this should never be needed but match previous semantics   
        return *cur;
    }
    IDistributedSuperFile & get()
    {
        cur->Link();
        return *cur;
    }

    virtual const char *queryName()
    {
        if (isValid())
            return names.item(idx).text.get();
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


extern bool isMulti(const char *str);

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
    Owned<IPropertyTree> attr;    
    unsigned proplockcount;
    unsigned transactionnest;
    Linked<IUserDescriptor> udesc;
    unsigned defaultTimeout;
public:

    IPropertyTree *queryRoot() { return root; }

    CDistributedFileBase<INTERFACE>()
    {
        proplockcount = 0;
        transactionnest = 0;
        defaultTimeout = INFINITE;
    }

    ~CDistributedFileBase<INTERFACE>()
    {
        attr.clear();
        root.clear();
    }

    bool isCompressed(bool *blocked)
    {
        return ::isCompressed(queryProperties(),blocked);
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

    void loadAttr()
    {
        if (proplockcount||!parent)
            attr.setown(getCreatePropTree(root,"Attr"));
        else {
            IPropertyTree *t = root->queryPropTree("Attr");
            if (t)
                t->Link();
            else
                t = getEmptyAttr();
            attr.setown(t);
        }
    }

    IPropertyTree &queryProperties()
    {
        CriticalBlock block (sect);
        if (attr) 
            return *attr;
        loadAttr();
        return *attr;
    }

    bool isAnon() 
    { 
        return !logicalName.isSet(); 
    }

    IPropertyTree &lockProperties(unsigned timeoutms)
    {
        bool lockreleased;
        return lockProperties(lockreleased,timeoutms);
    }

    IPropertyTree & lockProperties(bool &lockreleased,unsigned timeoutms)
    {
        if (timeoutms==INFINITE)
            timeoutms = defaultTimeout;
        lockreleased = false;
        // this is a bit of a kludge for non-transactional superfile operations and other dining philosopher problems
        if (proplockcount++==0) {
            attr.clear();
            if (conn) {
                conn->rollback(); // changes chouldn't be done outside lock properties
#ifdef TRACE_LOCKS
                PROGLOG("lockProperties: pre safeChangeModeWrite(%x)",(unsigned)(memsize_t)conn.get());
#endif
                try {
                    safeChangeModeWrite(conn,queryLogicalName(),lockreleased,timeoutms);
                }
                catch(IException *)
                {
                    proplockcount--;
                    dfCheckRoot("lockProperties",root,conn);
                    throw;
                }
#ifdef TRACE_LOCKS
                PROGLOG("lockProperties: done safeChangeModeWrite(%x)",(unsigned)(memsize_t)conn.get());
                LogRemoteConn(conn);
#endif
                dfCheckRoot("lockProperties",root,conn);
            }
        }
        CriticalBlock block (sect);
        if (attr) 
            return *attr;
        loadAttr();
        return *attr;
    }

    void unlockProperties()
    {
        savePartsAttr();
        if (--proplockcount==0) {
            attr.clear();
            if (conn) {
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

    void clearLockedProperties()
    {
        // assumes committed
        if (proplockcount) {
            proplockcount = 1;
            unlockProperties();
        }
    }

    bool lockTransaction(unsigned timeout)
    {
        if (timeout==INFINITE)
            WARNLOG("Infinite timeout on lockTransaction!");
        bool ret=true;
        if (conn&&(transactionnest++==0)) {
            if (proplockcount) {
                savePartsAttr();            
                conn->commit();
            }
            else {
                try {
                    attr.clear();
#ifdef TRACE_LOCKS
                    PROGLOG("lockTransaction: pre changeMode(%x)",(unsigned)(memsize_t)conn.get());
#endif
                    conn->changeMode(RTM_LOCK_WRITE,timeout,true);
#ifdef TRACE_LOCKS
                    PROGLOG("lockTransaction: done changeMode(%x)",(unsigned)(memsize_t)conn.get());
                    LogRemoteConn(conn);
#endif
                    proplockcount = 1;
                    dfCheckRoot("lockTransaction",root,conn);
                }
                catch(ISDSException *e)
                {
                    if (SDSExcpt_LockTimeout != e->errorCode())
                        throw;
                    e->Release();
                    PROGLOG("IDistributedFileBase %s lockTransaction, lock timed out",queryLogicalName());
                    transactionnest = 0;
                    ret = false;
                }
            }
            queryProperties(); // reloads attr
        }
        return ret;
    }

    virtual void unlockTransaction(bool _commit,bool _rollback)
    {
        if (transactionnest==0) {
            if (!_commit&&!_rollback&&conn)
                conn->changeMode(0,defaultTimeout,true);
            return; 
        }
        if (_rollback) {
            transactionnest = 1;
            conn->rollback();
        }
        if (--transactionnest==0) {
            savePartsAttr();            
            if (_commit) 
                conn->commit();
            if (conn&&(--proplockcount==0)) {
                attr.clear();
#ifdef TRACE_LOCKS
                PROGLOG("unlockTransaction: pre changeMode(%x)",(unsigned)(memsize_t)conn.get());
#endif
                conn->changeMode(RTM_LOCK_READ,defaultTimeout,true);
#ifdef TRACE_LOCKS
                PROGLOG("unlockTransaction: post changeMode(%x)",(unsigned)(memsize_t)conn.get());
                LogRemoteConn(conn);
#endif
                dfCheckRoot("unlockTransaction",root,conn);
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
        lockProperties(defaultTimeout);
        if (dt.isNull())
            root->removeProp("@modified");
        else {
            StringBuffer str;
            root->setProp("@modified",dt.getString(str).str());
        }
        root->removeProp("@verified");
        unlockProperties();
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
        if (queryProperties().getPropBin("ECLbin",mb)) 
            buf.deserialize(mb);
        else
            queryProperties().getProp("ECL",buf);
        return buf;
    }

    virtual void setECL(const char *ecl)
    {
        IPropertyTree &p = lockProperties(defaultTimeout);
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
        unlockProperties();
    }


    virtual bool isSubFile()
    {
        CriticalBlock block(sect);
        return root&&root->hasProp("SuperOwner[1]");
    }

    virtual bool cannotRemove(StringBuffer &reason)
    {
        CriticalBlock block(sect);
        return CDistributedFileDirectory::cannotRemove(logicalName,root,reason,false,false,defaultTimeout);
    }
    
    void setProtect(const char *owner, bool protect, unsigned timems)
    {
        if (logicalName.isForeign()) {
            parent->setFileProtect(logicalName,owner,protect);
        }
        else {
            bool ret=false;
            if (conn) {
                IPropertyTree &p = lockProperties(timems);
                CDateTime dt;
                dt.setNow();
                try {
                    if (setFileProtectTree(p,owner,protect))
                        conn->commit();
                }
                catch (IException *) {
                    unlockProperties();
                    throw;
                }
                unlockProperties();
                dfCheckRoot("setProtect.1",root,conn);
            }
            else 
                ERRLOG("setProtect - cannot protect %s (no connection in file)",owner?owner:"");
        }
    }


    virtual IDistributedSuperFileIterator *getOwningSuperFiles(IDistributedFileTransaction *_transaction)
    {
        CriticalBlock block(sect);
        return new CDistributedSuperFileIterator(parent,root,udesc,_transaction);
    }

    virtual void checkFormatAttr(IDistributedFile *sub, IDistributedFileTransaction *, const char* exprefix="")
    {
        // check file has same (or similar) format
        IPropertyTree &superProp = queryProperties();
        IPropertyTree &subProp = sub->queryProperties();
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

    void linkSuperOwner(const char *superfile,bool link,IDistributedFileTransaction *transaction)
    {
        if (!superfile||!*superfile)
            return;
        if (conn) {
            Owned<IPropertyTree> t = getNamedPropTree(root,"SuperOwner","@name",superfile,false);
            if (t) {
                if (!link) {
                    root->removeTree(t);
                    if (!transaction)
                    {
                        conn->commit();
                        dfCheckRoot("linkSuperOwner",root,conn);
                    }
                }
            }
            else if (link) {
                t.setown(addNamedPropTree(root,"SuperOwner","@name",superfile));
                if (!transaction)
                {
                    conn->commit();
                    dfCheckRoot("linkSuperOwner.2",root,conn);
                }
            }
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
        queryProperties().getProp("@columnMapping",mapping);
        return mapping;
    }

    virtual void setColumnMapping(const char *mapping)
    {
        IPropertyTree &prop = lockProperties(defaultTimeout);
        if (!mapping||!*mapping) 
            prop.removeProp("@columnMapping");
        else
            prop.setProp("@columnMapping",mapping);
        unlockProperties();
    }

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
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

template <class INTERFACE>
class CFileClusterOwner: public INTERFACE
{
protected:
    FileClusterInfoArray clusters;                      
public:

    CFileClusterOwner<INTERFACE>()
    {
    }

    virtual ~CFileClusterOwner<INTERFACE>()
    {
        clusters.kill();
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
        if (!INTERFACE::conn)
            return;
        assertex(INTERFACE::proplockcount==0); // cannot reload clusters if properties locked
        INTERFACE::conn->reload(); // should only be cluster changes but a bit dangerous
        IPropertyTree *t = INTERFACE::conn->queryRoot();  // NB not INTERFACE::queryRoot();

        if (!t)
            return;
        clusters.clear();
        getClusterInfo(*t,&queryNamedGroupStore(),0,clusters);
    }
    
    void saveClusters()
    {
        // called from CClustersLockedSection
        IPropertyTree *t;
        if (INTERFACE::conn)
            t = INTERFACE::conn->queryRoot();  
        else
            t = INTERFACE::queryRoot(); //cache
        if (!t)
            return;
        IPropertyTree *pt;
        IPropertyTree *tc = INTERFACE::queryRoot(); //cache
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
            if (t==tc)
                break;
            t = tc; // now fix cache
        }
        if (INTERFACE::conn) 
            INTERFACE::conn->commit(); // should only be cluster changes but a bit dangerous
    }

    void addCluster(const char *clustername,ClusterPartDiskMapSpec &mspec)
    {
        if (!clustername&&!*clustername)
            return;
        CClustersLockedSection cls(INTERFACE::logicalName);
        reloadClusters();
        if (findCluster(clustername)!=NotFound) {
            if (findCluster(clustername)!=NotFound) {
                IDFS_Exception *e = new CDFS_Exception(DFSERR_ClusterAlreadyExists,clustername);
                throw e;
            }
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

    void removeCluster(const char *clustername)
    {
        CClustersLockedSection cls(INTERFACE::logicalName);
        reloadClusters();
        unsigned i = findCluster(clustername);
        if (i!=NotFound) {
            if (clusters.ordinality()==1) 
                throw MakeStringException(-1,"CFileClusterOwner::removeCluster cannot remove sole cluster %s",clustername);
            clusters.remove(i);
            saveClusters();
        }
    }

    void setPreferredClusters(const char *clusterlist)
    {
        clusters.setPreferred(clusterlist,INTERFACE::logicalName);
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
        CClustersLockedSection cls(INTERFACE::logicalName);
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

    virtual unsigned numCopies(unsigned partno)
    {
        return clusters.numCopies(partno,numParts());
    }

    void setSingleClusterOnly()
    {
        clusters.setSingleClusterOnly();
    }

    virtual unsigned numParts() = 0;

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
            clusters.item(cluster).queryPartDiskMapping().calcPartLocation(partno,numParts(),rep,clusters.queryGroup(cluster)?clusters.queryGroup(cluster)->ordinality():numParts(),n,d);
            setReplicateFilename(path,d);  
        }
    }
};

class CDistributedFile: public CFileClusterOwner< CDistributedFileBase<IDistributedFile> >
{
protected:
    Owned<IFileDescriptor> fdesc;
    CDistributedFilePartArray parts;            // use queryParts to access
    CriticalSection sect;
    StringAttr directory;
    StringAttr partmask;


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
        CClustersLockedSection sect(logicalName);
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

    CDistributedFile(CDistributedFileDirectory *_parent, IFileDescriptor *fdesc, bool includeports)
    {
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile.b fdesc",fdesc);
#endif
        parent = _parent;
        root.setown(createPTree(queryDfsXmlBranchName(DXB_File)));
//      fdesc->serializeTree(*root,IFDSF_EXCLUDE_NODES); 
        setFileAttrs(fdesc,true);
        setClusters(fdesc);
        setPreferredClusters(_parent->defprefclusters);     
        saveClusters();
        setParts(fdesc,true);
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
        lockProperties(defaultTimeout);         // only needed to load attr (no lock)
        shrinkFileTree(root);
        if (totalsize!=(offset_t)-1) 
            attr->setPropInt64("@size", totalsize);
        if (useableCheckSum)
            attr->setPropInt64("@checkSum", checkSum);
        setModified();
        unlockProperties();
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
        clearLockedProperties();    // there actually shouldn't be any!
        if (conn)
            conn->rollback();       // changes should always be done in locked properties
        killParts();
    }

    IFileDescriptor *getFileDescriptor(const char *clustername)
    {
        CriticalBlock block (sect);
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
        fdesc->setTraceName(logicalName.get());
        StringArray cnames;
        if (clustername&&*clustername)
            cnames.append(clustername);
        else
            getClusterNames(cnames);
        fdesc->setClusterOrder(cnames,clustername&&*clustername);
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
        attr.clear();
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
            root->removeProp("Attr");
        else
            root->setPropTree("Attr",createPTreeFromIPT(t));
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
                                    &queryNamedGroupStore(),
                                    fdesc->queryClusterRoxieLabel(i)
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

    void rename(const char *_logicalname,IDistributedFileTransaction *transaction,IUserDescriptor *user)
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
            detach(transaction);
        }
        attach(_logicalname,transaction,user);
        if (prevname.length()) {
            IPropertyTree &pt = lockProperties(defaultTimeout);
            StringBuffer list;
            if (pt.getProp("@renamedFrom",list)&&list.length())
                list.append(',');
            pt.setProp("@renamedFrom",list.append(prevname).str());
            unlockProperties();
        }
        if (reliter.get()) {
            // add back any relationships with new name
            parent->renameFileRelationships(prevname.str(),_logicalname,reliter);
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

    void attach(const char *_logicalname,IDistributedFileTransaction *transaction,IUserDescriptor *user)
    {
        assertex(!transaction); 
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
        attr.clear();
        killParts();
        clusters.kill();
        CFileConnectLock fcl("CDistributedFile::attach",logicalName,DXB_File,false,false,defaultTimeout);
        conn.setown(fcl.detach());
        root.setown(conn->getRoot());
        root->queryBranch(".");     // load branch
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(root,&queryNamedGroupStore(),0);
        setFileAttrs(fdesc,false);
        setClusters(fdesc);
        setParts(fdesc,false);
#ifdef EXTRA_LOGGING
        LOGFDESC("CDistributedFile::attach fdesc",fdesc);
        LOGPTREE("CDistributedFile::attach root.2",root);
#endif
    }

    void detach(IDistributedFileTransaction *transaction)
    {
        assertex(!transaction); 
        clearLockedProperties();    // there actually shouldn't be any!
        CriticalBlock block (sect);
        assertex(!isAnon()); // not attached!
        MemoryBuffer mb;
#ifdef EXTRA_LOGGING
        PROGLOG("CDistributedFile::detach(%s)",logicalName.get());
        LOGPTREE("CDistributedFile::detach root.1",root);
#endif
        root->serialize(mb);
        attr.clear();
        conn.clear();
        root.setown(createPTree(mb));
        StringAttr lname(logicalName.get());
        logicalName.clear();
#ifdef EXTRA_LOGGING
        LOGPTREE("CDistributedFile::detach root.2",root);
#endif
        parent->removeEntry(lname.get(),udesc);
    }

    bool removePhysicalPartFiles(unsigned short port,const char *cluster,IMultiException *mexcept)
    {
        Owned<IGroup> grpfilter;
        if (cluster&&*cluster) {
            unsigned cn = findCluster(cluster);
            if (cn==NotFound)
                return false;
            if (clusters.ordinality()==0)
                cluster = NULL; // cannot delete last cluster
            else
                grpfilter.setown(clusters.getGroup(cn));
        }
        if (logicalName.isExternal()) {
            if (logicalName.isQuery())
                return false;
            throw MakeStringException(-1,"cannot remove an external file (%s)",logicalName.get());
        }
        if (logicalName.isForeign())
            throw MakeStringException(-1,"cannot remove a foreign file (%s)",logicalName.get());

        unsigned width = numParts();
        CriticalSection errcrit;
        class casyncfor: public CAsyncFor
        {
            IDistributedFile *file;
            unsigned short port;
            CriticalSection &errcrit;
            IMultiException *mexcept;
            unsigned width;
            IGroup *grpfilter;
        public:
            bool ok;
            bool islazy;
            casyncfor(IDistributedFile *_file,unsigned _width,unsigned short _port,IGroup *_grpfilter,IMultiException *_mexcept,CriticalSection &_errcrit)
                : errcrit(_errcrit)
            {
                file = _file;
                port = _port;
                ok = true;
                mexcept = _mexcept;
                width = _width;
                grpfilter = _grpfilter;
            }
            void Do(unsigned i)
            {
                Owned<IDistributedFilePart> part = file->getPart(i);
                unsigned nc = part->numCopies();
                for (unsigned copy = 0; copy < nc; copy++)
                {
                    RemoteFilename rfn;
                    part->getFilename(rfn,copy);
                    if (grpfilter&&(grpfilter->rank(rfn.queryEndpoint())==RANK_NULL))
                        continue;
                    if (port)
                        rfn.setPort(port); // if daliservix
                    Owned<IFile> partfile = createIFile(rfn);
                    StringBuffer eps;
                    try
                    {
                        unsigned start = msTick();
                        if (!partfile->remove()&&(copy==0)&&!islazy) // only warn about missing primary files
                            LOG(MCwarning, unknownJob, "Failed to remove file part %s from %s", partfile->queryFilename(),rfn.queryEndpoint().getUrlStr(eps).str());
                        else {
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
        } afor(this,width,port,grpfilter,mexcept,errcrit);
        afor.islazy = queryProperties().getPropInt("@lazy")!=0;
        afor.For(width,10,false,true);
        if (cluster&&*cluster) 
            removeCluster(cluster);
        return afor.ok;
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

    bool renamePhysicalPartFiles(const char *newname,
                                 const char *cluster,
                                 unsigned short port,
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
        else
        {
            const char * base = queryBaseDirectory(false,os);
            size32_t l = strlen(base);
            if ((memcmp(base,directory.get(),l)==0)&&((l==directory.length())||isPathSepChar(directory.get()[l]))) {
                basedir.append(base);
                diroverride = basedir.str();
            }
            else {  // shouldn't get here normally
                // now assume 'Base' dir is same as old
                basedir.append(directory);
                const char *e = strchr(basedir.str()+((psc=='/')?1:0),psc);
                if (e) {
                    const char *e2=strchr(e+1,psc);
                    if (e2) 
                        basedir.setLength(e2-basedir.str());
                    diroverride = basedir.str();
                }
            }
        }
        makePhysicalPartName(newname, 0, 0, newdir, false, os, diroverride);
        if (newdir.length()==0)
            return false;
        if (isPathSepChar(newdir.charAt(newdir.length()-1)))
            newdir.setLength(newdir.length()-1);
        getPartMask(newmask,newname,width);
        if (newmask.length()==0)
            return false;
        StringAttrArray newnamesprim;
        StringAttrArray newnamesrep;
        StringBuffer tmp;
        StringBuffer tmp2;
        StringBuffer fullname;
        unsigned i;
        for (i=0;i<width;i++) {
            CDistributedFilePart &part = parts.item(i);
            const char *fn = expandMask(tmp.clear(),newmask,i,width).str();
            fullname.clear();
            if (findPathSepChar(fn)) 
                fullname.append(fn);
            else
                addPathSepChar(fullname.append(newdir)).append(fn);
            // ensure on same drive
            setPathDrive(fullname,part.queryDrive(0));

            newnamesprim.append(*new StringAttrItem(fullname.str()));
            if (part.numCopies()>1) {                                            // NH *** TBD
                setReplicateDir(fullname,tmp2.clear());
                setPathDrive(tmp2,part.queryDrive(1));
                newnamesrep.append(*new StringAttrItem(tmp2.str()));
            }
        }

        // first check file doestn't exist for any new part

        CriticalSection crit;
        class casyncforbase: public CAsyncFor
        {
        protected:
            CriticalSection &crit;
            StringAttrArray &newnamesprim;
            StringAttrArray &newnamesrep;
            IDistributedFile *file;
            unsigned width;
            unsigned short port;
            IMultiException *mexcept;
            bool *ignoreprim;
            bool *ignorerep;
        public:
            bool ok;
            bool * doneprim;
            bool * donerep;
            IException *except;

            casyncforbase(IDistributedFile *_file,StringAttrArray &_newnamesprim,StringAttrArray &_newnamesrep,unsigned _width,unsigned short _port,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : newnamesprim(_newnamesprim),newnamesrep(_newnamesrep),crit(_crit)
            {
                width = _width;
                file = _file;
                port = _port;
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
                    const char *newfn = (copy==0)?newnamesprim.item(idx).text.get():newnamesrep.item(idx).text.get();
                    if (!newfn||!*newfn)
                        continue;
                    RemoteFilename newrfn;
                    newrfn.setPath(part->queryNode(copy)->endpoint(),newfn);
                    if (port) {
                        newrfn.setPort(port); // if daliservix
                        oldrfn.setPort(port);
                    }
                    try {
                        pok = doPart(part,copy!=0,oldrfn,newrfn,(copy==0)?doneprim[idx]:donerep[idx]);

                    }
                    catch (IException *e) {
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
            casyncfor1(IDistributedFile *_file,StringAttrArray &_newnamesprim,StringAttrArray &_newnamesrep,unsigned _width,unsigned short _port,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : casyncforbase(_file,_newnamesprim,_newnamesrep,_width,_port,_mexcept,_crit,_ignoreprim,_ignorerep)
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

        } afor1 (this,newnamesprim,newnamesrep,width,port,mexcept,crit,NULL,NULL);
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
            casyncfor2(IDistributedFile *_file,StringAttrArray &_newnamesprim,StringAttrArray &_newnamesrep,unsigned _width,unsigned short _port,IMultiException *_mexcept,CriticalSection &_crit,bool *_ignoreprim,bool *_ignorerep)
                : casyncforbase(_file,_newnamesprim,_newnamesrep,_width,_port,_mexcept,_crit,_ignoreprim,_ignorerep)
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

        } afor2 (this,newnamesprim,newnamesrep,width,port,mexcept,crit,ignoreprim,ignorerep);
        afor2.For(width,10,false,true);
        if (afor2.ok) {
            // now rename directory and partmask
                IPropertyTree &pt = lockProperties(defaultTimeout);
                root->setProp("@directory",newdir.str());
                root->setProp("@partmask",newmask.str());
                partmask.set(newmask.str());
                directory.set(newdir.str());
                StringBuffer mask;
                for (unsigned i=0;i<width;i++) {
                    mask.appendf("Part[%d]/@name",i+1);
                    //StringBuffer x;
                    //pt.getProp(mask.str(),x);
                    //pt.removeProp(mask.str());
                    parts.item(i).clearOverrideName();  
                }
                savePartsAttr(false);
                unlockProperties();

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
                        const char *newfn = (copy==0)?newnamesprim.item(i).text.get():newnamesrep.item(i).text.get();
                        if (!newfn||!*newfn)
                            continue;
                        RemoteFilename newrfn;
                        newrfn.setPath(part->queryNode(copy)->endpoint(),newfn);
                        if (port) {
                            newrfn.setPort(port); // if daliservix
                            oldrfn.setPort(port);
                        }
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
        __int64 ret = (__int64)(forcephysical?-1:queryProperties().getPropInt64("@size",-1));
        if (ret==-1) {
            ret = 0;
            unsigned n = numParts();
            for (unsigned i=0;i<n;i++) {
                Owned<IDistributedFilePart> part = getPart(i);
                __int64 ps = part->getFileSize(allowphysical,forcephysical);
                if (ps == -1) {
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
        if (queryProperties().hasProp("@checkSum"))
            checkSum = (unsigned)queryProperties().getPropInt64("@checkSum");
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
        if (queryProperties().hasProp("@formatCrc")) {
            // NB pre record_layout CRCs are not valid
            crc = (unsigned)queryProperties().getPropInt("@formatCrc");
            return true;
        }
        return false;
    }

    virtual bool getRecordLayout(MemoryBuffer &layout) 
    {
        return queryProperties().getPropBin("_record_layout",layout);
    }

    virtual bool getRecordSize(size32_t &rsz)
    {
        if (queryProperties().hasProp("@recordSize")) {
            rsz = (size32_t)queryProperties().getPropInt("@recordSize");
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
        if (logicalName.isForeign()) {
            parent->setFileAccessed(logicalName,dt);
        }
        else {
            IPropertyTree &prop = lockProperties(defaultTimeout);
            if (dt.isNull())
                prop.removeProp("@accessed");
            else {
                StringBuffer str;
                prop.setProp("@accessed",dt.getString(str).str());
            }
            unlockProperties();
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

struct SuperFileSubTreeCache
{
    unsigned n;
    IPropertyTree **subs;
    SuperFileSubTreeCache(IPropertyTree *root,bool fixerr)
    {
        IArrayOf<IPropertyTree> todelete;
        n=root->getPropInt("@numsubfiles");
        subs = (IPropertyTree **)calloc(sizeof(IPropertyTree *),n);
        Owned<IPropertyTreeIterator> subit = root->getElements("SubFile");
        ForEach (*subit) {
            IPropertyTree &sub = subit->query();
            unsigned sn = sub.getPropInt("@num",0);
            if ((sn>0)&&(sn<=n)) 
                subs[sn-1] = &sub;
            else  {
                const char *name = root->queryProp("OrigName");
                if (!name)
                    name = "UNKNOWN";
                WARNLOG("CDistributedSuperFile: SuperFile %s: corrupt subfile part number %d of %d",name,sn,n);
                if (fixerr) 
                    todelete.append(sub);
            }
        }
        ForEachItemIn(i,todelete) {
            root->removeTree(&todelete.item(i));
        }
    }
    ~SuperFileSubTreeCache()
    {
        free(subs);
    }
};


class CDistributedSuperFile: public CDistributedFileBase<IDistributedSuperFile>
{
    void checkNotForeign()
    {
        if (!conn)
            throw MakeStringException(-1,"Operation not allowed on foreign file");
    }

    CDistributedFilePartArray partscache;
    FileClusterInfoArray clusterscache; 

    class cAddSubFileAction: public CDFAction
    {
        StringAttr parentlname;
        Owned<IDistributedSuperFile> parent;
        Owned<IDistributedFile> sub;
        StringAttr subfile;
        bool before;
        StringAttr other;
        bool addcontents;
    public:
        cAddSubFileAction(IDistributedFileTransaction *_transaction,const char *_parentlname,const char *_subfile,bool _before,const char *_other,bool _addcontents)
            : CDFAction(_transaction), parentlname(_parentlname), subfile(_subfile), other(_other)
        {
            before = _before;
            addcontents = _addcontents;
        }
        virtual ~cAddSubFileAction() {}
        bool lock()
        {
            parent.setown(transaction->lookupSuperFile(parentlname));   
            if (!parent)
                throw MakeStringException(-1,"addSubFile: SuperFile %s cannot be found",parentlname.get());
            if (!subfile.isEmpty()) {
                try {
                    sub.setown(transaction->lookupFile(subfile,SDS_SUB_LOCK_TIMEOUT));
                }
                catch (IDFS_Exception *e) {
                    if (e->errorCode()!=DFSERR_LookupConnectionTimout)
                        throw;
                    return false;
                }
                if (!sub.get())
                    throw MakeStringException(-1,"addSubFile: File %s cannot be found to add",subfile.get());
                if (parent->querySubFileNamed(subfile)) 
                    WARNLOG("addSubFile: File %s is already a subfile of %s", subfile.get(),parent->queryLogicalName());
                // cannot abort here as may be clearsuperfile that hasn't been done
                // will pick up as error later if it is
//                  throw MakeStringException(-1,"addSubFile: File %s is already a subfile of %s", subfile.get(),parent->queryLogicalName());
            }
            if (parent->lockTransaction(SDS_SUB_LOCK_TIMEOUT))
                if (sub->lockTransaction(SDS_SUB_LOCK_TIMEOUT))
                    return true;
            sub.clear();
            parent.clear();
            return false;
        }
        void run()
        {
            if (!sub)
                throw MakeStringException(-1,"addSubFile(2): File %s cannot be found to add",subfile.get());
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);                 
            if (sf) {
                CPauseTransaction pausetrans(transaction);
                sf->doAddSubFile(LINK(sub),before,other,transaction);
            }
        }
        void unlock(bool commit,bool rollback)
        {
            sub->unlockTransaction(commit,rollback);
            parent->unlockTransaction(commit,rollback);
            CDistributedSuperFile *sf = QUERYINTERFACE(parent.get(),CDistributedSuperFile);                 
            if (sf&&commit)
                sf->updateParentFileAttrs(transaction);
            sub.clear();
            parent.clear();
        }
        
    };

    void checkModify(const char *title) 
    {
        StringBuffer reason;
        if (cannotModify(reason)) {
#ifdef EXTRA_LOGGING
            PROGLOG("CDistributedSuperFile::%s(cannotRemove) %s",title,reason.str());
#endif
            if (reason.length())
                throw MakeStringException(-1,"CDistributedSuperFile::%s %s",title,reason.str());
        }
    }


protected:
    int interleaved; // 0 not interleaved, 1 interleaved old, 2 interleaved new

    static StringBuffer &getSubPath(StringBuffer &path,unsigned idx)
    {
        return path.append("SubFile[@num=\"").append(idx+1).append("\"]");
    }

    void loadSubFiles(bool fixerr,IDistributedFileTransaction *transaction, unsigned timeout)
    {
        partscache.kill();
        StringBuffer path;
        StringBuffer subname;
        subfiles.kill();
        try {
            SuperFileSubTreeCache subtrees(root,fixerr);
            for (unsigned i=0;i<subtrees.n;i++) {
                IPropertyTree *sub = subtrees.subs[i];
                if (!sub) {
                    StringBuffer s;
                    s.appendf("CDistributedSuperFile: SuperFile %s: corrupt subfile file part %d cannot be found",logicalName.get(),i+1);
                    if (fixerr) {
                        WARNLOG("%s",s.str());
                        break;
                    }
                    throw MakeStringException(-1,"%s",s.str());
                }
                sub->getProp("@name",subname.clear());
                Owned<IDistributedFile> subfile;
                if (!fixerr) 
                    subfile.setown(transaction?transaction->lookupFile(subname.str(),timeout):parent->lookup(subname.str(),udesc,false,transaction,timeout));
                else {
                    try {
                        subfile.setown(transaction?transaction->lookupFile(subname.str(),timeout):parent->lookup(subname.str(),udesc,false,transaction,timeout));
                    }
                    catch (IException *e) {
                        // bit of a kludge to handle subfiles missing 
                        subfile.setown(transaction?transaction->lookupSuperFile(subname.str(),fixerr,timeout):parent->lookupSuperFile(subname.str(),udesc,transaction,fixerr,timeout));
                        if (!subfile.get())
                            throw;
                        e->Release();

                    }
                }
                if (!subfile.get()) {
                    StringBuffer s;
                    s.appendf("CDistributedSuperFile: SuperFile %s is missing sub-file file %s",logicalName.get(),subname.str());
                    CDfsLogicalFileName tmpsub;
                    tmpsub.set(subname);
                    if (fixerr||tmpsub.isForeign()) {
                        WARNLOG("%s",s.str());
                        root->removeTree(sub);
                        for (unsigned j=i+1;j<subtrees.n; j++) {
                            sub = subtrees.subs[j];
                            if (sub)
                                sub->setPropInt("@num",j);
                            if (j==1) {
                                root->setPropTree("Attr",createPTreeFromIPT(sub->queryPropTree("Attr")));
                                attr.clear();
                            }
                            subtrees.subs[j-1] = sub;
                            subtrees.subs[j] = NULL;
                        }
                        subtrees.n--;
                        root->setPropInt("@numsubfiles",subtrees.n);
                        if ((i==0)&&(subtrees.n==0)) {
                            attr.clear();
                            root->setPropTree("Attr",getEmptyAttr());
                        }
                        i--; // will get incremented by for
                        continue;
                    }
                    throw MakeStringException(-1,"%s",s.str());
                }
                subfiles.append(*subfile.getClear());
            }
            if (subfiles.ordinality()<subtrees.n) 
                root->setPropInt("@numsubfiles",subfiles.ordinality());
        }
        catch (IException *) {
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
            attr.clear();
            root->setPropTree("Attr",createPTreeFromIPT(&file->queryProperties()));
        }
        root->addPropTree("SubFile",sub);
        subfiles.add(*file.getClear(),pos);
        root->setPropInt("@numsubfiles",subfiles.ordinality());
    }

    void removeItem(unsigned pos, StringBuffer &subname)
    {
        partscache.kill();
        StringBuffer path;
        IPropertyTree* sub = root->queryPropTree(getSubPath(path,pos).str());
        if (!sub)
            throw MakeStringException(-1,"CDistributedSuperFile(3): Corrupt subfile file part %d cannot be found",pos+1);
        sub->getProp("@name",subname);
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
            attr.clear();
            if (subfiles.ordinality())
                root->setPropTree("Attr",createPTreeFromIPT(&subfiles.item(0).queryProperties()));
            else
                root->setPropTree("Attr",getEmptyAttr());
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

    class cSubFileIterator: public CInterface, implements IDistributedFileIterator
    {
        unsigned cur;
        IArrayOf<IDistributedFile> subfiles;
    public:
        IMPLEMENT_IINTERFACE;

        cSubFileIterator(IArrayOf<IDistributedFile> &_subfiles, bool supersub) 
        {
            ForEachItemIn(i,_subfiles) {
                IDistributedSuperFile* super = supersub?_subfiles.item(i).querySuperFile():NULL;
                if (super) {
                    Owned<IDistributedFileIterator> iter = super->getSubFileIterator(true);
                    ForEach(*iter) 
                        subfiles.append(iter->get());
                }
                else
                    subfiles.append(*LINK(&_subfiles.item(i)));
            }
            cur = 0;
        }
        

        bool first()
        {
            cur = 0;
            return (subfiles.ordinality()!=0);
        }

        bool next()
        {
            cur++;
            return (cur<subfiles.ordinality());
        }
                                
        bool  isValid()
        {
            return (cur<subfiles.ordinality());
        }

        StringBuffer & getName(StringBuffer &name)
        {
            return subfiles.item(cur).getLogicalName(name);
        }
        
        IDistributedFile & query()
        {
            return subfiles.item(cur);
        }
    };

    void linkSubFile(unsigned pos,IDistributedFileTransaction *transaction)
    {
        parent->linkSuperOwner(subfiles.item(pos),queryLogicalName(),true,transaction);
    }

    void unlinkSubFile(unsigned pos,IDistributedFileTransaction *transaction)
    {
        parent->linkSuperOwner(subfiles.item(pos),queryLogicalName(),false,transaction);
    }

    void checkSubFormatAttr(IDistributedFile *sub, IDistributedFileTransaction *_transaction, const char* exprefix="")
    {
        // empty super files now pass
        ForEachItemIn(i,subfiles) {
            IDistributedSuperFile* super = subfiles.item(i).querySuperFile();
            if (super) {
                CDistributedSuperFile *cdsuper = QUERYINTERFACE(super,CDistributedSuperFile);
                if (cdsuper)
                    cdsuper->checkSubFormatAttr(sub,_transaction,exprefix);
                return;
            }
            CDistributedFile *cdfile = QUERYINTERFACE(&subfiles.item(0),CDistributedFile);
            if (cdfile)
                cdfile->checkFormatAttr(sub,_transaction,exprefix);        // any file will do
        }
    }

    void checkFormatAttr(IDistributedFile *sub, IDistributedFileTransaction *_transaction, const char* exprefix="")
    {
        // only check sub files not siblings, which is excessive (format checking is really only debug aid)
        checkSubFormatAttr(sub,_transaction,exprefix);
    }


public:
    IArrayOf<IDistributedFile> subfiles;


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

    void init(CDistributedFileDirectory *_parent, IPropertyTree *_root, const CDfsLogicalFileName &_name, IUserDescriptor* user, bool fixerr, IDistributedFileTransaction *transaction, unsigned timeout=INFINITE) 
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
        loadSubFiles(fixerr,transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IPropertyTree *_root,const CDfsLogicalFileName &_name,IUserDescriptor* user) 
    {
        init(_parent,_root,_name,user,false,NULL);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent, IRemoteConnection *_conn,const CDfsLogicalFileName &_name,IUserDescriptor* user, bool fixerr, IDistributedFileTransaction *transaction,bool fixmissing,unsigned timeout) 
    {
        conn.setown(_conn);
        init(_parent,conn->queryRoot(),_name,user,fixmissing,transaction,timeout);
    }

    CDistributedSuperFile(CDistributedFileDirectory *_parent,const CDfsLogicalFileName &_name, IUserDescriptor* user, bool fixerr, IDistributedFileTransaction *transaction)
    {
        // temp super file
        assertex(_name.isMulti());
        Owned<IPropertyTree> tree = _name.createSuperTree();
        init(_parent,tree,_name,user,false,transaction);
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
        // superfiles assume consistant replication
        UnsignedArray subcounts;  
        bool mixedwidth = false;
        Owned<IPropertyTree> at = getEmptyAttr();
        if (subfiles.ordinality()!=0) {
            Owned<IAttributeIterator> ait = subfiles.item(0).queryProperties().getAttributes();
            ForEach(*ait) {
                const char *name = ait->queryName();
                if ((stricmp(name,"@size")!=0)&&(stricmp(name,"@recordCount")!=0)) {
                    const char *v = ait->queryValue();
                    if (!v||!*v)
                        continue;
                    bool ok = true;
                    for (unsigned i=1;i<subfiles.ordinality();i++) {
                        const char *p = subfiles.item(i).queryProperties().queryProp(name);
                        if (!p||(strcmp(p,v)!=0)) {
                            ok = false;
                            break;
                        }
                    }
                    if (ok)
                        at->setProp(name,v);
                }
            }
            unsigned width = 0;
            Owned<IDistributedFileIterator> fiter = getSubFileIterator(true);
            ForEach(*fiter) {
                unsigned np = fiter->query().numParts();
                if (width) 
                    width = np;
                else if (np!=width) 
                    mixedwidth = true;
                subcounts.append(np);
            }
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
            fdesc->setPart(n,part.getFilename(rfn,copy),&part.queryProperties());
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

    void rename(const char *_logicalname,IDistributedFileTransaction *transaction,IUserDescriptor *user)
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
            detach(transaction);
        }
        attach(_logicalname,transaction,user);
        if (reliter.get()) {
            // add back any relationships with new name
            parent->renameFileRelationships(prevname.str(),_logicalname,reliter);
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



    void attach(const char *_logicalname,IDistributedFileTransaction *transaction,IUserDescriptor *user)
    {
        // I don't think this ever gets called on superfiles
        WARNLOG("attach called on superfile! (%s)",_logicalname);
        assertex(!transaction); 
        CriticalBlock block (sect);
        assertex(isAnon()); // already attached!
        StringBuffer tail;
        StringBuffer lfn;
        logicalName.set(_logicalname);
        checkLogicalName(logicalName,user,true,true,false,"attach"); 
        parent->addEntry(logicalName,root.getClear(),true,false);
        attr.clear();
        conn.clear();
        CFileConnectLock fcl("CDistributedSuperFile::attach",logicalName,DXB_SuperFile,false,false,defaultTimeout);
        conn.setown(fcl.detach());
        root.setown(conn->getRoot());
    }

    void detach(IDistributedFileTransaction *transaction)
    {   
        // will need more thought but this gives limited support for anon
        CriticalBlock block (sect);
        if (isAnon())
            return;
        checkModify("CDistributedSuperFile::detach");
        subfiles.kill();    
        MemoryBuffer mb;
        root->serialize(mb);
        root.clear();
        attr.clear();
        conn.clear();
        root.setown(createPTree(mb));
        StringAttr lname(logicalName.get());
        logicalName.clear();
        parent->removeEntry(lname.get(),udesc);
    }

    bool removePhysicalPartFiles(unsigned short port,const char *clustername,IMultiException *mexcept)
    {
        throw MakeStringException(-1,"removePhysicalPartFiles not supported for SuperFiles");
        return false; 
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

    bool renamePhysicalPartFiles(const char *newlfn,const char *cluster, unsigned short port,IMultiException *mexcept,const char *newbasedir)
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
        __int64 ret = (__int64)(forcephysical?-1:queryProperties().getPropInt64("@size",-1));
        if (ret==-1) {
            ret = 0;
            ForEachItemIn(i,subfiles) {
                __int64 ps = subfiles.item(i).getFileSize(allowphysical,forcephysical);
                if (ps == -1) {
                    ret = ps;
                    break;
                }
                ret += ps;
            }
        }
        return ret;
    }

    __int64 getRecordCount()
    {
        __int64 ret = queryProperties().getPropInt64("@recordCount",-1);
        if (ret==-1) {
            ret = 0;
            ForEachItemIn(i,subfiles) {
                __int64 rc = subfiles.item(i).queryProperties().getPropInt64("@recordCount",-1);
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
        if (queryProperties().hasProp("@checkSum"))
            checkSum = (unsigned)queryProperties().getPropInt64("@checkSum");
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
        if (queryProperties().hasProp("@formatCrc")) {
            crc = (unsigned)queryProperties().getPropInt("@formatCrc");
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
        if (queryProperties().getPropBin("_record_layout",layout)) 
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
        if (queryProperties().hasProp("@recordSize")) {
            rsz = (size32_t)queryProperties().getPropInt("@recordSize");
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

    IDistributedFileIterator *getSubFileIterator(bool supersub)
    {
        CriticalBlock block (sect);
        return new cSubFileIterator(subfiles,supersub);
    }

    void doAddSubFile(IDistributedFile *_sub,bool before,const char *other,IDistributedFileTransaction *transaction) // takes ownership of sub
    {
        Owned<IDistributedFile> sub = _sub;
        if (strcmp(sub->queryLogicalName(),queryLogicalName())==0)
            throw MakeStringException(-1,"addSubFile: Cannot add file %s to itself", queryLogicalName());
        if (subfiles.ordinality())
            checkFormatAttr(sub,transaction,"addSubFile");
        if (findSubFile(sub->queryLogicalName())!=NotFound) 
            throw MakeStringException(-1,"addSubFile: File %s is already a subfile of %s", sub->queryLogicalName(),queryLogicalName());

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
        unsigned cmppos = (pos==0)?1:0;
        addItem(pos,sub.getClear());     // remove if failure TBD?
        setModified();
        updateFileAttrs();
        linkSubFile(pos, transaction);
    }

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
        if (addcontents) {
            StringArray subs;
            Owned<IDistributedSuperFile> sfile = parent->lookupSuperFile(subfile,udesc,transaction);  // Timeout TBD?    
            if (sfile) {
                Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator(true);
                ForEach(*iter)
                    subs.append(iter->query().queryLogicalName());
            }
            sfile.clear();
            ForEachItemIn(i,subs) {
                addSubFile(subs.item(i),before,other,false,transaction);
            }
            return;
            
        }
        if (transaction&&transaction->active()) {
            cAddSubFileAction *action = new cAddSubFileAction(transaction,queryLogicalName(),subfile,before,other,addcontents);
            // action is now owned by transaction so don't unlink or delete!
            return;
        }
        Owned<IDistributedFile> sub = transaction ? transaction->lookupFile(subfile) : parent->lookup(subfile,udesc,false,NULL,defaultTimeout);
        if (!sub)
            throw MakeStringException(-1,"addSubFile(3): File %s cannot be found to add",subfile);
        bool lockreleased;
        lockProperties(lockreleased,defaultTimeout); 
        // need to reload subfiles if changed
        try {
            if (lockreleased)
                loadSubFiles(true,transaction,1000*60*10); 
            doAddSubFile(sub.getClear(),before,other,transaction);
        }
        catch (IException *) {
            unlockProperties();
            throw;
        }
        unlockProperties();
        updateParentFileAttrs(transaction);
    }

    void updateFileAttrs()
    {
        if (subfiles.ordinality()==0) {
            StringBuffer desc;
            root->getProp("Attr/@description",desc);
            root->removeProp("Attr");       // remove all other attributes if superfile empty
            IPropertyTree *t=root->setPropTree("Attr",getEmptyAttr());
            if (desc.length())
                t->setProp("@description",desc.str());
            return;
        }
        root->removeProp("Attr/@size");
        root->removeProp("Attr/@checkSum");
        root->removeProp("Attr/@recordCount");  // recordCount not currently supported by superfiles
        root->removeProp("Attr/@formatCrc");    // formatCrc set if all consistant
        root->removeProp("Attr/@recordSize");   // record size set if all consistant
        root->removeProp("Attr/_record_layout");
        __int64 fs = getFileSize(false,false);
        if (fs!=-1)
            root->setPropInt64("Attr/@size",fs);
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
                file->lockProperties(defaultTimeout);
                file->setModified();
                file->updateFileAttrs();
                file->unlockProperties();
                file->updateParentFileAttrs(transaction);
            }
        }
    }

    virtual bool removeSubFile(const char *subfile,         // if NULL removes all
                                bool remsub,                // if true removes subfiles from DFS 
                                bool remphys,               // if true removes physical parts of sub file
                                bool remcontents,
                                IDistributedFileTransaction *transaction,
                                bool delayed)
    {
        CriticalBlock block (sect);
        if (subfile&&!*subfile)
            return false;
        checkModify("removeSubFile");
        partscache.kill();
        if (remcontents) {
            CDfsLogicalFileName logicalname;    
            logicalname.set(subfile);
            IDistributedFile *sub = querySubFileNamed(logicalname.get(),false);
            if (!sub)
                return false;
            IDistributedSuperFile *sfile = sub->querySuperFile();
            if (sfile) {
                Owned<IDistributedFileIterator> iter = sfile->getSubFileIterator(true);
                bool ret = true;
                StringArray toremove;
                ForEach(*iter) 
                    toremove.append(iter->query().queryLogicalName());
                iter.clear();
                Owned<CWrappedTransaction> wrappedtrans = new CWrappedTransaction(transaction,this,udesc);
                ForEachItemIn(i,toremove)
                    if (!sfile->removeSubFile(toremove.item(i),remsub,remphys,false,wrappedtrans,delayed))
                        ret = false;
                wrappedtrans->clearOwner();
                if (!ret||!remsub)
                    return ret;
            }
        }
        if (transaction&&transaction->active()) {
            class cRemoveSubFileAction: public CDFAction
            {
                StringAttr parentlname;
                Owned<IDistributedSuperFile> parent;
                Owned<IDistributedFile> sub;
                StringAttr subfile;
                bool remsub;
                bool remphys;
                bool remcontents;
            public:
                cRemoveSubFileAction(IDistributedFileTransaction *_transaction,const char *_parentlname,const char *_subfile,bool _remsub, bool _remphys, bool _remcontents)
                    : CDFAction(_transaction), parentlname(_parentlname), subfile(_subfile)
                {
                    remsub = _remsub;
                    remphys = _remphys;
                    remcontents = _remcontents;
                }
                virtual ~cRemoveSubFileAction() {}
                bool lock()
                {
                    parent.setown(transaction->lookupSuperFile(parentlname,true));
                    if (!parent)
                        throw MakeStringException(-1,"removeSubFile: SuperFile %s cannot be found",parentlname.get());
                    if (!subfile.isEmpty()) {
                        try {
                            sub.setown(transaction->lookupFile(subfile,SDS_SUB_LOCK_TIMEOUT));
                        }
                        catch (IDFS_Exception *e) {
                            if (e->errorCode()!=DFSERR_LookupConnectionTimout)
                                throw;
                            return false;
                        }
                        if (!parent->querySubFileNamed(subfile))
                            WARNLOG("addSubFile: File %s is not a subfile of %s", subfile.get(),parent->queryLogicalName());
                    }
                    if (parent->lockTransaction(SDS_SUB_LOCK_TIMEOUT))
                        if (!sub || sub->lockTransaction(SDS_SUB_LOCK_TIMEOUT))
                            return true;
                    parent.clear();
                    sub.clear();
                    return false;
                }
                void run()
                {
                    CPauseTransaction pausetrans(transaction);
                    parent->removeSubFile(subfile,remsub,remphys,remcontents,transaction,true); 
                }
                void unlock(bool commit,bool rollback)
                {
                    if (sub)
                        sub->unlockTransaction(commit,rollback);
                    parent->unlockTransaction(commit,rollback); 
                    parent.clear();
                    sub.clear();
                }
            } *action = new cRemoveSubFileAction(transaction,queryLogicalName(),subfile,remsub,remphys,remcontents);
            // action is now owned by transaction so don't unlink or delete!
            return true; 
        }
        // have to be quite careful here
        StringAttrArray subnames;
        unsigned pos;
        StringBuffer subname;
        if (subfile) {
            unsigned pos=findSubFileOrd(subfile);
            if ((pos==NotFound)||(pos>=subfiles.ordinality()))
                pos = findSubFile(subfile);
            if (pos==NotFound)
                return false;
            lockProperties(defaultTimeout); 
            // don't reload subfiles here
            pos=findSubFileOrd(subfile);
            if ((pos==NotFound)||(pos>=subfiles.ordinality()))
                pos = findSubFile(subfile);
            if (pos==NotFound) {
                unlockProperties();
                return false;
            }
            unlinkSubFile(pos,transaction);
            removeItem(pos,subname.clear());
            subnames.append(* new StringAttrItem(subname.str()));
            setModified();
            updateFileAttrs();
            unlockProperties();
            updateParentFileAttrs(transaction);
        }
        else {
            pos = subfiles.ordinality();
            if (pos) {
                bool lockreleased;
                lockProperties(lockreleased,defaultTimeout); 
                if (lockreleased)
                    loadSubFiles(true,transaction,1000*60*10); 
                pos = subfiles.ordinality();
                if (pos) {
                    do {
                        pos--;
                        unlinkSubFile(pos,transaction);
                        removeItem(pos,subname.clear());
                        subnames.append(* new StringAttrItem(subname.str()));
                    } while (pos);
                    setModified();
                    updateFileAttrs();
                    unlockProperties();
                    updateParentFileAttrs(transaction);
                }
                else
                    unlockProperties();

            }
        }
        if (remsub||remphys) {
            try {
                ForEachItemIn(i,subnames) {
                    bool done;
                    CDfsLogicalFileName dlfn;
                    dlfn.set(subnames.item(i).text.get());
                    if (!transaction||!delayed||!transaction->addDelayedDelete(dlfn.get(),remphys,udesc)) {
                        if (remphys) 
                            done = parent->doRemovePhysical(dlfn,0,NULL,NULL,udesc,true);
                        else {
                            done = parent->doRemoveEntry(dlfn,udesc,true);
                        }
                        if (!done)
                            WARNLOG("removeSubFile(%d) %s not removed, perhaps sub-file of different superfile",(int)remphys,subnames.item(i).text.get());
                    }
                }
            }
            catch (IException *e) {
                // should use multiexception here
                EXCLOG(e,"CDistributedSuperFile::removeSubFile");
                e->Release();
            }
        }
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
        if (transaction&&transaction->active()) {
            class cSwapFileAction: public CDFAction
            {
                Linked<IDistributedSuperFile> parent;
                Linked<IDistributedSuperFile> file;
                StringAttr parentlname;
                StringAttr filelname;
            public:
                cSwapFileAction(IDistributedFileTransaction *_transaction,const char *_parentlname,const char *_filelname)
                    : CDFAction(_transaction), parentlname(_parentlname), filelname(_filelname)
                {
                }
                virtual ~cSwapFileAction() {}
                bool lock()
                {
                    parent.setown(transaction->lookupSuperFile(parentlname));
                    if (!parent)
                        throw MakeStringException(-1,"swapSuperFile: SuperFile %s cannot be found",parentlname.get());
                    file.setown(transaction->lookupSuperFile(filelname));
                    if (!file) {
                        parent.clear();
                        throw MakeStringException(-1,"swapSuperFile: SuperFile %s cannot be found",filelname.get());
                    }
                    if (parent->lockTransaction(SDS_SUB_LOCK_TIMEOUT)) {
                        if (file->lockTransaction(SDS_SUB_LOCK_TIMEOUT)) 
                            return true;
                        parent->unlockTransaction(false,true);
                    }
                    parent.clear();
                    file.clear();
                    return false;
                }
                void run()
                {
                    CPauseTransaction pausetrans(transaction);
                    parent->swapSuperFile(file,transaction);
                }
                void unlock(bool commit,bool rollback)
                {
                    file->unlockTransaction(commit,rollback);
                    parent->unlockTransaction(commit,rollback); 
                    parent.clear();
                    file.clear();
                }
            } *action = new cSwapFileAction(transaction,queryLogicalName(),_file->queryLogicalName());
            // action is now owned by transaction so don't unlink or delete!
            return true; 
        }
        CDistributedSuperFile *file = QUERYINTERFACE(_file,CDistributedSuperFile);
        if (!file)
            return false;
        StringBuffer subname;
        StringArray subnames1; // will be built reversed
        StringArray subnames2; // will be built reversed
        unsigned pos = subfiles.ordinality();
        lockProperties(defaultTimeout);
        if (pos) {
            do {
                pos--;
                subfiles.item(pos).lockProperties(defaultTimeout);
                unlinkSubFile(pos,transaction);
                removeItem(pos,subname.clear());
                subnames1.append(subname.str());
            } while (pos);
        }
        file->lockProperties(defaultTimeout);
        pos = file->subfiles.ordinality();
        if (pos) {
            do {
                pos--;
                file->subfiles.item(pos).lockProperties(defaultTimeout);
                file->unlinkSubFile(pos,transaction);
                file->removeItem(pos,subname.clear());
                subnames2.append(subname.str());
            } while (pos);
        }
        ForEachItemInRev(i1,subnames1) {
            Owned<IDistributedFile> sub = transaction ? transaction->lookupFile(subnames1.item(i1)) : parent->lookup(subnames1.item(i1),udesc,false,NULL,defaultTimeout);
            file->doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        ForEachItemInRev(i2,subnames2) {
            Owned<IDistributedFile> sub = transaction ? transaction->lookupFile(subnames2.item(i2)) : parent->lookup(subnames2.item(i2),udesc,false,NULL,defaultTimeout);
            doAddSubFile(LINK(sub), false, NULL, transaction);
        }
        pos = subfiles.ordinality();
        if (pos) {
            do {
                pos--;
                subfiles.item(pos).unlockProperties();
            } while(pos);
        }
        pos = file->subfiles.ordinality();
        if (pos) {
            do {
                pos--;
                file->subfiles.item(pos).unlockProperties();
            } while(pos);
        }
        file->setModified();
        file->updateFileAttrs();
        file->unlockProperties();
        file->updateParentFileAttrs(transaction);
        setModified();
        updateFileAttrs();
        unlockProperties();
        updateParentFileAttrs(transaction);
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

    void addCluster(const char *clustername,ClusterPartDiskMapSpec &mspec)  
    {
        if (!clustername||!*clustername)
            return;
        CriticalBlock block (sect);
        clusterscache.clear();
        subfiles.item(0).addCluster(clustername,mspec);
    }

    virtual void removeCluster(const char *clustername)
    {
        CriticalBlock block (sect);
        clusterscache.clear();
        ForEachItemIn(i,subfiles) {
            IDistributedFile &f=subfiles.item(i);
            f.removeCluster(clustername);
        }       
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

    bool cannotModify(StringBuffer &reason)
    {
        CriticalBlock block(sect);
        return CDistributedFileDirectory::cannotRemove(logicalName,root,reason,false,true,defaultTimeout);  
    }



};


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
        throw MakeStringException(-1, "%s", err);
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


IPropertyTree &CDistributedFilePart::queryProperties()
{ 
    CriticalBlock block (sect);     // avoid nested blocks
    if (attr) 
        return *attr;
    WARNLOG("CDistributedFilePart::queryProperties missing part attributes");
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
    return getCrcFromPartProps(parent.queryProperties(),queryProperties(), crc);
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

IPropertyTree &CDistributedFilePart::lockProperties(unsigned timeoutms)
{
    parent.lockProperties(timeoutms);
    dirty = true;
    CriticalBlock block (sect);     // avoid nested blocks
    if (attr) 
        return *attr;
    WARNLOG("CDistributedFilePart::queryProperties missing part attributes");
    attr.setown(getEmptyAttr());
    return *attr;
}

void CDistributedFilePart::unlockProperties()
{
    parent.unlockProperties();
}

offset_t CDistributedFilePart::getFileSize(bool allowphysical,bool forcephysical)
{
    offset_t ret = (offset_t)((forcephysical&&allowphysical)?-1:queryProperties().getPropInt64("@size", -1));
    if (allowphysical&&(ret==(offset_t)-1)) {
        StringBuffer firstname;
        bool compressed = ::isCompressed(parent.queryProperties());
        unsigned nc=parent.numCopies(partIndex);
        for (unsigned copy=0;copy<nc;copy++) {
            RemoteFilename rfn;
            try {
                Owned<IFile> partfile = createIFile(getFilename(rfn,copy));
                if (compressed)
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
                StringBuffer s("CDistributedFilePart::getFileSize ");
                rfn.getRemotePath(s);
                EXCLOG(e, s.str());
                e->Release();
            }
            if (copy==0)
                rfn.getRemotePath(firstname);
        }
        IDFS_Exception *e = new CDFS_Exception(DFSERR_CannotFindPartFileSize,firstname.str());;
        throw e;
    }
    return ret;
}

offset_t CDistributedFilePart::getDiskSize()
{
    // gets size on disk
    if (!::isCompressed(parent.queryProperties()))
        return getFileSize(true,false);
    StringBuffer firstname;
    unsigned nc=parent.numCopies(partIndex);
    for (unsigned copy=0;copy<nc;copy++) {
        RemoteFilename rfn;
        try {
            Owned<IFile> partfile = createIFile(getFilename(rfn,copy));
                offset_t ret = partfile->size();
            if (ret!=(offset_t)-1)
                return ret;
        }
        catch (IException *e)
        {
            StringBuffer s("CDistributedFilePart::getFileSize ");
            rfn.getRemotePath(s);
            EXCLOG(e, s.str());
            e->Release();
        }
        if (copy==0)
            rfn.getRemotePath(firstname);
    }
    IDFS_Exception *e = new CDFS_Exception(DFSERR_CannotFindPartFileSize,firstname.str());;
    throw e;
    return 0;
}

bool CDistributedFilePart::getModifiedTime(bool allowphysical,bool forcephysical, CDateTime &dt)
{
    StringBuffer s;
    if (!forcephysical&&queryProperties().getProp("@modified", s)) {
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
        return pe->query().getPropInt("@cluster",0)!=0;
    }
};

// --------------------------------------------------------

#define GROUP_CACHE_INTERVAL (1000*60)

class CNamedGroupStore: public CInterface, implements INamedGroupStore
{
    CriticalSection cachesect;
    Owned<IGroup> cachedgroup;
    StringAttr cachedname;
    StringAttr cachedgroupdir;
    unsigned cachedtime;
    unsigned defaultTimeout;

public:
    IMPLEMENT_IINTERFACE;

    CNamedGroupStore()
    {
        defaultTimeout = INFINITE;
        cachedtime = 0;
    }

    IGroup *dolookup(const char *logicalgroupname,IRemoteConnection *conn, StringBuffer *dirret)
    {
        SocketEndpointArray epa;
        StringBuffer gname(logicalgroupname);
        gname.trim();
        if (!gname.length())
            return NULL;
        gname.toLowerCase();
        logicalgroupname = gname.str();
        if ((gname.length()>9)&&(memcmp(logicalgroupname,"foreign::",9)==0)) {
            StringBuffer eps;
            const char *s = logicalgroupname+9;
            while (*s&&((*s!=':')||(s[1]!=':')))
                eps.append(*(s++));
            if (*s) {
                s+=2;
                if (*s) {
                    Owned<INode> dali = createINode(eps.str());
                    if (dali) 
                        return getRemoteGroup(dali,s,FOREIGN_DALI_TIMEOUT,dirret);
                }
            }
        }
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
        {
            CriticalBlock block(cachesect);
            if (cachedgroup.get()) {
                if (msTick()-cachedtime>GROUP_CACHE_INTERVAL) {
                    cachedgroup.clear();
                    cachedname.clear();
                    cachedgroupdir.clear();
                }
                else if (strcmp(gname.str(),cachedname.get())==0) {
                    cachedtime = msTick();
                    if (range.length()==0) {
                        if (dirret)
                            dirret->append(cachedgroupdir);
                        return cachedgroup.getLink();
                    }
                    // there is a range so copy to epa
                    cachedgroup->getSocketEndpoints(epa);
                    groupdir.set(cachedgroupdir);
                }
            }
        }
        if (epa.ordinality()==0) {
            struct sLock
            {
                sLock()  { lock = NULL; };
                ~sLock() { delete lock; };
                CConnectLock *lock;
            } slock;
            if (!conn) {
                slock.lock = new CConnectLock("CNamedGroup::lookup",SDS_GROUPSTORE_ROOT,false,false,defaultTimeout);
                conn = slock.lock->conn;
                if (!conn)
                    return NULL;
            }
            Owned<IPropertyTree> pt = getNamedPropTree(conn->queryRoot(),"Group","@name",gname.str(),true);
            if (!pt)
                return NULL;
            groupdir.set(pt->queryProp("@dir"));
            Owned<IPropertyTreeIterator> pe2 = pt->getElements("Node");
            ForEach(*pe2) {
                SocketEndpoint ep(pe2->query().queryProp("@ip"));
                epa.append(ep);
            }
        }
        IGroup *ret = createIGroup(epa);
        {
            CriticalBlock block(cachesect);
            cachedgroup.set(ret);
            cachedname.set(gname);
            cachedgroupdir.set(groupdir);
            cachedtime = msTick();
        }
        if (range.length()) {
            SocketEndpointArray epar;
            const char *s = range.str();
            while (*s) {
                unsigned start = 0;
                while (isdigit(*s)) {
                    start = start*10+*s-'0';
                    s++;
                }
                if (!start)
                    break;
                unsigned end;
                if (*s=='-') {
                    s++;
                    end = 0;
                    while (isdigit(*s)) {
                        end = end*10+*s-'0';
                        s++;
                    }
                    if (!end)
                        end = epa.ordinality();
                }
                else 
                    end = start;
                if ((start>epa.ordinality())||(end>epa.ordinality())) {
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
            ::Release(ret);
            ret = createIGroup(epar);
        }
        if (dirret)
            dirret->append(groupdir);
        return ret;
    }

    IGroup *lookup(const char *logicalgroupname)
    {
        return dolookup(logicalgroupname,NULL,NULL);
    }

    IGroup *lookup(const char *logicalgroupname, StringBuffer &dir)
    {
        return dolookup(logicalgroupname,NULL,&dir);
    }

    INamedGroupIterator *getIterator()
    {
        CConnectLock connlock("CNamedGroup::getIterator",SDS_GROUPSTORE_ROOT,false,true,defaultTimeout);
        return new CNamedGroupIterator(connlock.conn); // links connection
    }

    INamedGroupIterator *getIterator(IGroup *match,bool exact)
    {
        CConnectLock connlock("CNamedGroup::getIterator",SDS_GROUPSTORE_ROOT,false,false,defaultTimeout);
        return new CNamedGroupIterator(connlock.conn,match,exact); // links connection
    }

    void doadd(CConnectLock &connlock,const char *name,IGroup *group,bool cluster,const char *dir)
    {
        if (!group)
            return;
        IPropertyTree *val = createPTree("Group");
        val->setProp("@name",name);
        if (cluster)
            val->setPropInt("@cluster",1);
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
    
    void addUnique(IGroup *group,StringBuffer &lname,const char *dir)
    {
        if (group->ordinality()==1) {
            group->getText(lname);
            return;
        }
        CConnectLock connlock("CNamedGroup::addUnique",SDS_GROUPSTORE_ROOT,true,false,defaultTimeout);
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
    
    void add(const char *logicalgroupname,IGroup *group,bool cluster,const char *dir)
    {
        StringBuffer name(logicalgroupname);
        name.toLowerCase();
        name.trim();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name.str());
        CConnectLock connlock("CNamedGroup::add",SDS_GROUPSTORE_ROOT,true,false,defaultTimeout);
        connlock.conn->queryRoot()->removeProp(prop.str()); 
        doadd(connlock,name.str(),group,cluster,dir);
        {                                                           
            CriticalBlock block(cachesect);                     
            cachedgroup.set(group); // may be NULL
            cachedname.set(name.str());
            cachedgroupdir.set(dir);
            cachedtime = msTick();
        }
    }

    void remove(const char *logicalgroupname)
    {
        add(logicalgroupname,NULL,false,NULL);
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

    void swapNode(IpAddress &from, IpAddress &to)
    {
        if (from.ipequals(to))
            return;
        CConnectLock connlock("CNamedGroup::swapNode",SDS_GROUPSTORE_ROOT,true,false,defaultTimeout);
        StringBuffer froms;
        from.getIpText(froms);
        StringBuffer tos;
        to.getIpText(tos);
        Owned<IPropertyTreeIterator> pe  = connlock.conn->queryRoot()->getElements("Group");
        ForEach(*pe) {
            StringBuffer name;
            pe->query().getProp("@name",name);
            StringBuffer xpath("Node[@ip = \"");
            xpath.append(froms).append("\"]");
            for (unsigned guard=0; guard<1000; guard++) {
                Owned<IPropertyTreeIterator> ne = pe->query().getElements(xpath.str());
                if (!ne->first()) 
                    break;
                ne->query().setProp("@ip",tos.str());
                PROGLOG("swapNode swapping %s for %s in group %s",froms.str(),tos.str(),name.str());
            }
        }
        CriticalBlock block(cachesect);
        cachedgroup.clear();
        cachedname.clear();
        cachedgroupdir.clear();
    }

    IGroup *getRemoteGroup(const INode *foreigndali, const char *gname, unsigned foreigndalitimeout, StringBuffer *dirret)
    {
        StringBuffer lcname(gname);
        gname = lcname.trim().toLowerCase().str();
        CMessageBuffer mb;
        mb.append((int)MDFS_GET_GROUP_TREE).append(gname);
        size32_t mbsz = mb.length();
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
        checkDfsReplyException(mb);
        if (mb.length()==0)
            return NULL;
        byte ok;
        mb.read(ok);
        if (ok!=1) {
            // kludge for prev bug
            if ((ok==(byte)MDFS_GET_GROUP_TREE)&&mb.length()>mbsz) {
                mb.skip(mbsz-1);
                mb.read(ok);
                if (ok!=1) 
                    return NULL;
            }
            else
                return NULL;
        }
        Owned<IPropertyTree> pt = createPTree(mb);
        Owned<IPropertyTreeIterator> pe = pt->getElements("Node");
        SocketEndpointArray epa;
        ForEach(*pe) {
            SocketEndpoint ep(pe->query().queryProp("@ip"));
            epa.append(ep);
        }
        IGroup *ret = createIGroup(epa);
        {
            CriticalBlock block(cachesect);
            cachedgroup.set(ret);
            cachedname.set(gname);
            cachedgroupdir.set(pt->queryProp("@dir"));
            if (dirret)
                dirret->append(cachedgroupdir);
            cachedtime = msTick();
        }
        return ret;
    }

    unsigned setDefaultTimeout(unsigned timems)
    {
        unsigned ret = defaultTimeout;
        defaultTimeout = timems;
        return ret;
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
            Owned<IGroup> lgrp = groupStore->dolookup(name,conn,NULL);
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


IDistributedFile *CDistributedFileDirectory::createExternal(const CDfsLogicalFileName &logicalname)
{
    //authentication already done
    SocketEndpoint ep;
    Owned<IGroup> group;
    if (!logicalname.getEp(ep)) {
        StringBuffer grp;
        if (logicalname.getGroupName(grp).length()==0) 
            throw MakeStringException(-1,"missing node in external file name (%s)",logicalname.get());
        group.setown(queryNamedGroupStore().lookup(grp.str()));
        if (!group)
            throw MakeStringException(-1,"cannot resolve node %s in external file name (%s)",grp.str(),logicalname.get());
        ep = group->queryNode(0).endpoint();
    }

    bool iswin=false;
    bool usedafs;
    switch (getDaliServixOs(ep)) { 
      case DAFS_OSwindows: 
          iswin = true;
          // fall through
      case DAFS_OSlinux:   
      case DAFS_OSsolaris: 
          usedafs = ep.port||!ep.isLocal();
          break;
      default:
#ifdef _WIN32
        iswin = true;
#else
        iswin = false;
#endif
        usedafs = false;
    }

    //rest is local path
    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    StringBuffer dir;
    StringBuffer tail;
    IException *e=NULL;
    if (!logicalname.getExternalPath(dir,tail,iswin,&e)) {
        if (e)
            throw e;
        return NULL;
    }
    fileDesc->setDefaultDir(dir.str());
    unsigned n = group.get()?group->ordinality():1;
    StringBuffer partname;
    CDateTime moddt;
    bool moddtset = false;
    for (unsigned i=0;i<n;i++) {
        if (group.get())
            ep = group->queryNode(i).endpoint();
        partname.clear();
        partname.append(dir);
        const char *s = tail.str();
        bool isspecial = (*s=='>');
        if (isspecial)
            partname.append(s);
        else {
            while (*s) {
                if (memicmp(s,"$P$",3)==0) {
                    partname.append(i+1);
                    s += 3;
                }
                else if (memicmp(s,"$N$",3)==0) {
                    partname.append(n);
                    s += 3;
                }
                else
                    partname.append(*(s++));
            }
        }
        if (!ep.port&&usedafs)
            ep.port = getDaliServixPort();
        RemoteFilename rfn;
        rfn.setPath(ep,partname.str());
        if (!isspecial&&(memcmp(partname.str(),"/$/",3)!=0)&&(memcmp(partname.str(),"\\$\\",3)!=0)) { // don't get date on external data
            try {
                Owned<IFile> file = createIFile(rfn);
                CDateTime dt;
                if (file&&file->getTime(NULL,&dt,NULL)) {
                    if (!moddtset||(dt.compareDate(moddt)>0)) {
                        moddt.set(dt);
                        moddtset = true;
                    }
                }
            }
            catch (IException *e) {
                EXCLOG(e,"CDistributedFileDirectory::createExternal");
                e->Release();
            }
        }
        fileDesc->setPart(i,rfn);
    }
    fileDesc->queryPartDiskMapping(0).defaultCopies = 1;
    IDistributedFile * ret = createNew(fileDesc,logicalname.get(),true);   // set modified
    if (ret&&moddtset) {
        ret->setModificationTime(moddt);    
    }
    return ret;
}




IDistributedFile *CDistributedFileDirectory::lookup(const char *_logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction, unsigned timeout)
{
    CDfsLogicalFileName logicalname;    
    logicalname.set(_logicalname);
    return lookup(logicalname,user,writeattr,transaction,timeout);
}

IDistributedFile *CDistributedFileDirectory::dolookup(const CDfsLogicalFileName &_logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction,bool fixmissing,unsigned timeout)
{
    const CDfsLogicalFileName *logicalname = &_logicalname;
    if (logicalname->isMulti()) 
        // don't bother checking because the sub file creation will
        return new CDistributedSuperFile(this,*logicalname,user,true,transaction); // temp superfile
    Owned<IDfsLogicalFileNameIterator> redmatch;
    loop {
        checkLogicalName(*logicalname,user,true,writeattr,true,NULL);
        if (logicalname->isExternal()) 
            return createExternal(*logicalname);    // external always works?
        if (logicalname->isForeign()) {
            IDistributedFile * ret = getFile(logicalname->get(),NULL,user);
            if (ret)
                return ret;
        }
        else {
            unsigned start = 0;
            loop {
                CFileConnectLock fcl;
                DfsXmlBranchKind bkind;
                if (!fcl.initany("CDistributedFileDirectory::lookup",*logicalname,bkind,false,true,timeout))
                    break;
                if (bkind == DXB_File) {
                    StringBuffer cname;
                    if (logicalname->getCluster(cname).length()) {
                        IPropertyTree *froot=fcl.queryRoot();
                        if (froot) {
                            StringBuffer query;
                            query.appendf("Cluster[@name=\"%s\"]",cname.str());
                            if (!froot->hasProp(query.str())) 
                                break;
                        }
                    }
                    return new CDistributedFile(this,fcl.detach(),*logicalname,user);  // found
                }
                // now super file
                if (bkind != DXB_SuperFile) 
                    break;
                if (start==0)
                    start = msTick();
                unsigned elapsed;
                try {
                    return new CDistributedSuperFile(this,fcl.detach(),*logicalname,user,true,transaction,fixmissing,SDS_SUB_LOCK_TIMEOUT);
                }
                catch (IDFS_Exception *e) {
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

IDistributedFile *CDistributedFileDirectory::lookup(const CDfsLogicalFileName &logicalname,IUserDescriptor *user,bool writeattr,IDistributedFileTransaction *transaction, unsigned timeout)
{
    return dolookup(logicalname,user,writeattr,transaction,false,timeout);
}

IDistributedSuperFile *CDistributedFileDirectory::lookupSuperFile(const char *_logicalname,IUserDescriptor *user,IDistributedFileTransaction *transaction, bool fixmissing, unsigned timeout)
{
    CDfsLogicalFileName logicalname;    
    logicalname.set(_logicalname);
    IDistributedFile *file = dolookup(logicalname,user,false,transaction,fixmissing,timeout);
    if (file) {
        IDistributedSuperFile *sf = file->querySuperFile();
        if (sf)
            return sf;
        file->Release();
    }
    return NULL;
}

bool CDistributedFileDirectory::isSuperFile(    const char *logicalname,
                                                INode *foreigndali,
                                                IUserDescriptor *user,
                                                unsigned timeout)
{
    Owned<IPropertyTree> tree = getFileTree(logicalname, foreigndali,user,timeout, false);
    return tree.get()&&(strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0);
}


bool CDistributedFileDirectory::exists(const char *_logicalname,bool notsuper,bool superonly,IUserDescriptor *user)
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
        Owned<IDistributedFile> file = lookup(_logicalname,user,false,NULL,defaultTimeout); 
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
            CConnectLock connlockfile("CDistributedFileDirectory::exists",str.str(),false,false,defaultTimeout);
            if (connlockfile.conn.get())
                return true;
        }
        if (notsuper)
            return false;
        dlfn.makeFullnameQuery(str.clear(),DXB_SuperFile,true);
        CConnectLock connlocksuper("CDistributedFileDirectory::exists",str.str(),false,false,defaultTimeout);
        if (!connlocksuper.conn.get())
            return false;
    }
    return true;
}

bool CDistributedFileDirectory::existsPhysical(const char *_logicalname,IUserDescriptor *user)
{
    Owned<IDistributedFile> file = lookup(_logicalname,user,false,NULL, defaultTimeout); 
    if (!file)
        return false;
    return file->existsPhysicalPartFiles(0);
}

IDistributedFile *CDistributedFileDirectory::createNew(IFileDescriptor *fdesc,const char *lname, bool includeports)
{
    CDistributedFile *file = new CDistributedFile(this, fdesc, includeports);
    if (file&&lname&&*lname&&file->isAnon())
        file->setLogicalName(lname);
    return file;
}

IDistributedSuperFile *CDistributedFileDirectory::createSuperFile(const char *_logicalname, bool _interleaved,bool ifdoesnotexist,IUserDescriptor *user)
{
    CDfsLogicalFileName logicalname;
    logicalname.set(_logicalname);
    checkLogicalName(logicalname,user,true,true,false,"have a superfile with");
    //assertex(!transaction); // transaction TBD
    StringBuffer tail;
    IPropertyTree *root = createPTree();
    root->setPropInt("@interleaved",_interleaved?2:0);
    addEntry(logicalname,root,true,ifdoesnotexist);
    StringBuffer query;
    CFileConnectLock fcl("CDistributedFileDirectory::createSuperFile",logicalname,DXB_SuperFile,false,false,defaultTimeout);
    return new CDistributedSuperFile(this, fcl.detach(), logicalname, user, false, NULL, false, defaultTimeout);
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

bool CDistributedFileDirectory::cannotRemove(CDfsLogicalFileName &dlfn,IPropertyTree *froot,StringBuffer &reason,bool ignoresub, bool checkprotonly, unsigned timeoutms)
{
    const char *logicalname = dlfn.get();
    if (!checkprotonly) {
        if (!logicalname||!*logicalname) {
            reason.appendf("empty filename");
            return true;
        }
        if (dlfn.isQuery()) {
            reason.appendf("%s is query",logicalname);
            return true;
        }
        if (dlfn.isExternal()) {
            reason.appendf("%s is external",logicalname);
            return true;
        }
        if (dlfn.isForeign()) {
            reason.appendf("%s is foreign",logicalname);
            return true;
        }
        if (dlfn.isMulti()) {
            reason.appendf("%s is multi",logicalname);
            return true;
        }
    }
    if (!froot) {
        DfsXmlBranchKind bkind;
        CFileConnectLock fconnlock;
        if (fconnlock.initany("CDistributedFileDirectory::cannotRemove",dlfn,bkind,true,false,timeoutms)) 
            froot = fconnlock.queryRoot();
        if (!froot) {
#ifdef EXTRA_LOGGING
            PROGLOG("CDistributedFileDirectory::cannotRemove(%s) NOT FOUND",logicalname);
#endif
            reason.appendf("Cannot lock %s",logicalname);
            return true;
        }
    }
    if (checkProtectAttr(logicalname,froot,reason))
        return true;
    if (!checkprotonly) {
        Owned<IPropertyTreeIterator> soiter = froot->getElements("SuperOwner");
        ForEach(*soiter) {
            const char *supername = soiter->query().queryProp("@name");
            if (supername&&*supername) {
                CDfsLogicalFileName lsfnp;
                lsfnp.set(supername);
                CFileConnectLock fcl("CDistributedFileDirectory::cannotRemove",lsfnp,DXB_SuperFile,false,false,timeoutms); 
                StringBuffer fquery;
                fquery.append("SubFile[@name=\"").append(logicalname).append("\"]");
                if (fcl.conn()&&fcl.conn()->queryRoot()->hasProp(fquery.str())) {
                    if (!ignoresub)
                        reason.appendf("Cannot remove file %s as owned by SuperFile %s",logicalname,supername);
                    return true;
                }
            }
        }
    }
    return false;
}

bool CDistributedFileDirectory::doRemoveEntry(CDfsLogicalFileName &dlfn,IUserDescriptor *user,bool ignoresub)
{
    const char *logicalname = dlfn.get();
#ifdef EXTRA_LOGGING
    PROGLOG("CDistributedFileDirectory::doRemoveEntry(%s)",logicalname);
#endif
    if (!checkLogicalName(dlfn,user,true,true,true,"remove"))
        return false;
    StringBuffer cname;
    dlfn.getCluster(cname);
    DfsXmlBranchKind bkind;
    CFileConnectLock fconnlock;
    {
        IPropertyTree *froot=NULL;
        if (fconnlock.initany("CDistributedFileDirectory::doRemoveEntry",dlfn,bkind,true,false,defaultTimeout)) 
            froot = fconnlock.queryRoot();
        if (!froot) {
#ifdef EXTRA_LOGGING
            PROGLOG("CDistributedFileDirectory::doRemoveEntry(%s) NOT FOUND",logicalname);
#endif
            return false;
        }
        if (cname.length()) {
            if (bkind==DXB_SuperFile) {
                ERRLOG("Trying to remove cluster %s from superfile %s",logicalname,cname.str());
                return false;
            }
            const char *group = froot->queryProp("@group"); 
            if (group&&(strcmp(group,cname.str())!=0)) {    // see if only cluster (if it is remove entire)
                StringBuffer query;
                query.appendf("Cluster[@name=\"%s\"]",cname.str());
                IPropertyTree *t = froot->queryPropTree(query.str());
                if (t) {
                    return froot->removeTree(t);    
                }
                else {
                    ERRLOG("Cluster %s not present in file %s",logicalname,cname.str());
                    return false;
                }
            }           
        }
        StringBuffer reason;
        if (cannotRemove(dlfn,froot,reason,ignoresub,false,defaultTimeout)) {
#ifdef EXTRA_LOGGING
            PROGLOG("CDistributedFileDirectory::doRemoveEntry(cannotRemove) %s",reason.str());
#endif
            if (reason.length())
                throw MakeStringException(-1,"CDistributedFileDirectory::removeEntry %s",reason.str());
            return false;
        }
        if (bkind==DXB_SuperFile) {
            Owned<IPropertyTreeIterator> iter = froot->getElements("SubFile");
            StringBuffer oquery;
            oquery.append("SuperOwner[@name=\"").append(logicalname).append("\"]");
            ForEach(*iter) {
                const char *name = iter->query().queryProp("@name");
                if (name&&*name) {
                    CDfsLogicalFileName subfn;
                    subfn.set(name);
                    CFileConnectLock fconnlock;
                    DfsXmlBranchKind subbkind;
                    if (fconnlock.initany("CDistributedFileDirectory::doRemoveEntry",subfn,subbkind,false,false,defaultTimeout)) { 
                        IPropertyTree *subfroot = fconnlock.queryRoot();
                        if (subfroot) {
                            if (!subfroot->removeProp(oquery.str()))
                                WARNLOG("CDistributedFileDirectory::removeEntry: SubFile %s of %s not found for removal",name?name:"(NULL)",logicalname);
                        }
                    }
                }
            }
        }
    }
    fconnlock.remove();
    fconnlock.kill();
    try {
        removeFileEmptyScope(dlfn,defaultTimeout);
        removeAllFileRelationships(logicalname);
    }
    catch (IException *e) {
        EXCLOG(e,"CDistributedFileDirectory::doRemoveEntry");
        e->Release();
    }
    return true;
}


bool CDistributedFileDirectory::removeEntry(const char *name,IUserDescriptor *user)
{
    CDfsLogicalFileName dlfn;   
    dlfn.set(name);
    return doRemoveEntry(dlfn,user,false);
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

bool CDistributedFileDirectory::doRemovePhysical(CDfsLogicalFileName &dlfn,unsigned short port,const char *cluster,IMultiException *exceptions,IUserDescriptor *user,bool ignoresub)
{
    CriticalBlock block(removesect);
    const char *logicalname = dlfn.get();
    if (dlfn.isForeign()) {
        WARNLOG("Attempt to delete foreign file %s",logicalname);
        return false;
    }
    if (dlfn.isExternal()) {
        WARNLOG("Attempt to delete external file %s",logicalname);
        return false;
    }
    Owned<IDistributedFile> file = lookup(logicalname,user,true,NULL, defaultTimeout); 
    if (!file)
        return false;
    if (file->isSubFile()&&ignoresub)
        return false;
    if (file->querySuperFile()) {
        ERRLOG("SuperFile remove physical not supported currently");
        file.clear();
        return doRemoveEntry(dlfn,user,ignoresub);  
    }
    StringBuffer clustername(cluster); 
    if (clustername.length()==0)
        dlfn.getCluster(clustername); // override
    if ((clustername.length()==0)||((file->findCluster(clustername.str())==0)&&(file->numClusters()==1))) {
        clustername.clear();
        file->detach(); 
    }
    try {
        file->removePhysicalPartFiles(port,clustername.str(),exceptions); 
    }
    catch (IException *e)
    {
        StringBuffer msg("Removing ");
        msg.append(logicalname);
        EXCLOG(e,msg.str());
        e->Release();
        return false;
    }
    return true;
}

bool CDistributedFileDirectory::removePhysical(const char *_logicalname,unsigned short port,const char *cluster,IMultiException *exceptions,IUserDescriptor *user)
{
    CDfsLogicalFileName dlfn;
    dlfn.set(_logicalname);
    return doRemovePhysical(dlfn,port,cluster,exceptions,user,false);
}

    
bool CDistributedFileDirectory::renamePhysical(const char *oldname,const char *newname,unsigned short port,IMultiException *exceptions,IUserDescriptor *user)
{
    CriticalBlock block(removesect);
    if (!user)
        user = defaultudesc.get();
    CDfsLogicalFileName oldlogicalname; 
    oldlogicalname.set(oldname);
    checkLogicalName(oldlogicalname,user,true,true,false,"rename");
    Owned<IDistributedFile> file = lookup(oldlogicalname,user,true,NULL,defaultTimeout); 
    if (!file) {
        ERRLOG("renamePhysical: %s does not exist",oldname);
        return false;
    }   
    if (file->querySuperFile()) 
        throw MakeStringException(-1,"CDistributedFileDirectory::renamePhysical Cannot rename file %s as is SuperFile",oldname);
    StringBuffer reason;
    if (file->cannotRemove(reason)) 
        throw MakeStringException(-1,"CDistributedFileDirectory::renamePhysical %s",reason.str());
    CDfsLogicalFileName newlogicalname; 
    newlogicalname.set(newname);
    if (newlogicalname.isExternal()) 
        throw MakeStringException(-1,"renamePhysical cannot rename to external file");
    if (newlogicalname.isForeign()) 
        throw MakeStringException(-1,"renamePhysical cannot rename to foreign file");
    StringBuffer oldcluster;
    oldlogicalname.getCluster(oldcluster);
    StringBuffer newcluster;
    newlogicalname.getCluster(newcluster);
    Owned<IDistributedFile> newfile = lookup(newlogicalname.get(),user,true,NULL, defaultTimeout); 
    Owned<IDistributedFile> oldfile;
    bool mergeinto = false;
    bool splitfrom = false;
    if (newfile) {
        if (newcluster.length()) {
            if (oldcluster.length()) 
                throw MakeStringException(-1,"cannot specify both source and destination clusters on rename");
            if (newfile->findCluster(newcluster.str())!=NotFound) 
                throw MakeStringException(-1,"renamePhysical cluster %s already part of file %s",newcluster.str(),newname);
            if (file->numClusters()!=1) 
                throw MakeStringException(-1,"renamePhysical source file %s has more than one cluster",oldname);
            // check compatible here ** TBD
            mergeinto = true;
        }
        else {
            ERRLOG("renamePhysical %s already exists",newname);
            return false;
        }

    }
    else if (oldcluster.length()) {
        if (newcluster.length()) 
            throw MakeStringException(-1,"cannot specify both source and destination clusters on rename");
        if (file->numClusters()==1) 
            throw MakeStringException(-1,"cannot rename sole cluster %s",oldcluster.str());
        if (file->findCluster(oldcluster.str())==NotFound) 
            throw MakeStringException(-1,"renamePhysical cannot find cluster %s",oldcluster.str());
        oldfile.setown(file.getClear());
        Owned<IFileDescriptor> newdesc = oldfile->getFileDescriptor(oldcluster.str());
        file.setown(createNew(newdesc));
        splitfrom = true;
    }
    
    if (port!=(unsigned short)-1) {
        try {
            if (!file->renamePhysicalPartFiles(newlogicalname.get(),splitfrom?oldcluster.str():NULL,port,exceptions))   
                return false;
        }
        catch (IException *e)
        {
            StringBuffer msg("Renaming ");
            msg.append(oldname).append(" to ").append(newname);
            EXCLOG(e,msg.str());
            e->Release();
            return false;
        }
    }
    if (splitfrom) {
        oldfile->removeCluster(oldcluster.str());
        file->attach(newlogicalname.get());
    }
    else if (mergeinto) {
        ClusterPartDiskMapSpec mspec = file->queryPartDiskMapping(0);
        file->detach();
        newfile->addCluster(newcluster.str(),mspec);
        fixDates(newfile,port);
    }
    else
        file->rename(newname,NULL,user);
    return true;
}

void CDistributedFileDirectory::fixDates(IDistributedFile *file,unsigned short port)
{
    // should do in parallel 
    unsigned width = file->numParts();
    CriticalSection crit;
    class casyncfor: public CAsyncFor
    {
        IDistributedFile *file;
        unsigned short port;
        CriticalSection &crit;
        unsigned width;
    public:
        bool ok;
        casyncfor(IDistributedFile *_file,unsigned _width,unsigned short _port,CriticalSection &_errcrit)
            : crit(_errcrit)
        {
            file = _file;
            port = _port;
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
                if (port)
                    rfn.setPort(port); // if daliservix
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
    } afor(file,width,port,crit);
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
    CScopeConnectLock sconnlock("CDistributedFileDirectory::addEntry",dlfn,true,false,defaultTimeout);
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
    sroot->addPropTree(superfile?queryDfsXmlBranchName(DXB_SuperFile):queryDfsXmlBranchName(DXB_File),root); // now owns root  
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
    CScopeConnectLock sconnlock("CDistributedFileDirectory::getFileClusterList",logicalname,false,false,defaultTimeout);
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


class CFileScanner
{
    bool recursive;
    bool includesuper;
    StringAttr wildname;
    IUserDescriptor *user;
    StringArray *scopesallowed;
    unsigned lastscope;

    bool scopeMatch(const char *name)
    {   // name has trailing '::'
        if (!*name)
            return true;
        if (wildname.isEmpty())
            return true;
        const char *s1 = wildname.get();
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

    void processScopes(IPropertyTree &root,StringBuffer &name)
    {
        size32_t ns = name.length();
        if (ns) {
            if (scopesallowed) {
                // scopes *should* be in order so look from last+1
                unsigned i=0;
                for (;i<scopesallowed->ordinality();i++) {
                    unsigned s = (lastscope+i+1)%scopesallowed->ordinality();
                    if (stricmp(scopesallowed->item(s),name.str())==0) {
                        lastscope = s;
                        break;
                    }
                }
                if (i==scopesallowed->ordinality())
                    return;
            }
            else if (user) {
                int perm = getScopePermissions(name.str(),user,0);      // don't audit
                if (!HASREADPERMISSION(perm)) {
                    return;
                }
            }
            name.append("::");
        }
        size32_t ns2 = name.length();

        if (scopeMatch(name.str())) {
            Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_Scope));
            if (iter->first()) {
                do {
                    IPropertyTree &scope = iter->query();
                    name.append(scope.queryProp("@name"));
                    processScopes(scope,name);
                    name.setLength(ns2);
                } while (iter->next());
            }
            processFiles(root,name);
        }
        name.setLength(ns);
    }

    void processFiles(IPropertyTree &root,StringBuffer &name)
    {
        const char *s1 = wildname.get();
        size32_t ns = name.length();
        Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_File));
        if (iter->first()) {
            do {
                IPropertyTree &file = iter->query();
                name.append(file.queryProp("@name"));
                if (!s1||WildMatch(name.str(),s1,true)) 
                    processFile(file,name,false);
                name.setLength(ns);
            } while (iter->next());
        }
        if (includesuper) {
            iter.setown(root.getElements(queryDfsXmlBranchName(DXB_SuperFile)));
            if (iter->first()) {
                do {
                    IPropertyTree &file = iter->query();
                    name.append(file.queryProp("@name"));
                    if (!s1||WildMatch(name.str(),s1,true)) 
                        processFile(file,name,true);
                    name.setLength(ns);
                } while (iter->next());
            }
        }
    }

public:

    virtual void processFile(IPropertyTree &file,StringBuffer &name,bool issuper) = 0;


    void scan(const char *_wildname,bool _recursive,bool _includesuper,IUserDescriptor *_user, StringArray *_scopesallowed=NULL)
    {
        lastscope = 0;
        scopesallowed = _scopesallowed;
        if (_wildname)
            wildname.set(_wildname);
        else
            wildname.clear();
        user = _user;
        recursive = _recursive;
        includesuper = _includesuper;
        StringBuffer name;
        Linked<IPropertyTree> sroot = querySDSServer().lockStoreRead();
        try { 
            processScopes(*sroot->queryPropTree(querySdsFilesRoot()),name); 
        }
        catch (...) { 
            querySDSServer().unlockStoreRead(); 
            throw; 
        }
        querySDSServer().unlockStoreRead();
    }
};

class CScopeScanner
{
    bool recursive;
    StringAttr wildname;
    IUserDescriptor *user;
    StringArray *scopesallowed;
    unsigned lastscope;

    bool scopeMatch(const char *name)
    {   // name has trailing '::'
        if (!*name)
            return true;
        if (wildname.isEmpty())
            return true;
        const char *s1 = wildname.get();
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

    void processScopes(IPropertyTree &root,StringBuffer &name)
    {
        size32_t ns = name.length();
        if (root.hasChildren()) // only process non-empty
            processScope(name.str());
        if (ns) 
            name.append("::");
        size32_t ns2 = name.length();
        bool empty = true;
        if (scopeMatch(name.str())) {
            Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_Scope));
            if (iter->first()) {
                do {
                    IPropertyTree &scope = iter->query();
                    name.append(scope.queryProp("@name"));
                    processScopes(scope,name);
                    name.setLength(ns2);
                } while (iter->next());
            }
        }
        name.setLength(ns);
    }

public:

    virtual void processScope(const char *name) = 0;


    void scan(const char *_wildname,bool _recursive)
    {
        if (_wildname)
            wildname.set(_wildname);
        else
            wildname.clear();
        recursive = _recursive;
        StringBuffer name;
        Linked<IPropertyTree> sroot = querySDSServer().lockStoreRead();
        try { 
            processScopes(*sroot->queryPropTree(querySdsFilesRoot()),name); 
        }
        catch (...) { 
            querySDSServer().unlockStoreRead(); 
            throw; 
        }
        querySDSServer().unlockStoreRead();
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

class CInitGroups
{
    CMachineEntryMap machinemap;
    CIArrayOf<CMachineEntry> machinelist;
    CConnectLock groupsconnlock;
    StringArray clusternames;
    unsigned defaultTimeout;


    void addClusterGroup(const char *name,IGroup *group,const char *kind, bool realcluster, const char *dir)
    {
        StringBuffer lcname(name);
        name = lcname.trim().toLowerCase().str();
        IPropertyTree *root = groupsconnlock.conn->queryRoot();
        StringBuffer prop;
        prop.appendf("Group[@name=\"%s\"]",name);
        IPropertyTree *old = root->queryPropTree(prop.str());
        if (group)
            clusternames.append(name);
        if (old) {
            // see if identical
            bool differs=false;
            const char *oldk = old->queryProp("@kind");
            if (oldk) {
                if (kind)
                    differs = strcmp(kind,oldk)!=0;
                else
                    differs = true;
            }
            else if (kind)
                differs = true;
            const char *oldd = old->queryProp("@dir");
            if (oldd&&!differs) {
                if (dir)
                    differs = strcmp(dir,oldd)!=0;
                else
                    differs = true;
            }
            else
                differs = (dir&&*dir);
            if (group&&!differs) {
                Owned<IPropertyTreeIterator> pe = old->getElements("Node");
                unsigned i=0;
                ForEach(*pe) {
                    if (i>=group->ordinality()) {
                        differs = true;
                        break;
                    }
                    SocketEndpoint ep(pe->query().queryProp("@ip"));
                    if (!ep.equals(group->queryNode(i).endpoint())) {
                        differs = true;
                    }
                    i++;
                }
                if (!differs&&(i==group->ordinality())) {
                    if (old->getPropBool("@cluster")!=realcluster)
                        if (realcluster)
                            old->setPropInt("@cluster",1);
                        else
                            old->removeProp("@cluster");
                    return;
                }
            }
            root->removeProp(prop.str()); 
        }
        if (!group)
            return;
        IPropertyTree *val = createPTree("Group");
        val->setProp("@name",name);
        if (realcluster)
            val->setPropInt("@cluster",1);
        else
            val->removeProp("@cluster");
        if (kind)
            val->setProp("@kind",kind);
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
        root->addPropTree("Group",val);
    }


    void loadEndpoints(IPropertyTree& cluster,SocketEndpointArray &eps,bool roxie,bool roxiefarm,const char *processname)
    {
        SocketEndpoint nullep;
        unsigned n=roxie?0:cluster.getPropInt("@slaves");
        Owned<IPropertyTreeIterator> nodes;
        nodes.setown(cluster.getElements(processname));
        StringArray roxiedirs; // kludge - use roxie dir name to order group
        unsigned i = 0;
        ForEach(*nodes) {
            IPropertyTree &node = nodes->query();
            const char *computer = node.queryProp("@computer");
            CMachineEntryPtr *m = machinemap.getValue(computer);
            if (!m) {
                ERRLOG("Cannot construct %s, computer name %s not found\n",cluster.queryProp("@name"),computer);
                return;
            }
            SocketEndpoint ep = (*m)->ep;
            if (roxiefarm) {
                unsigned k;
                for (k=0;k<eps.ordinality();k++)
                    if (eps.item(k).equals(ep))
                        break;
                if (k==eps.ordinality())
                    eps.append(ep); // just add (don't care about order and no duplicates)
            }
            else if (roxie) {
                Owned<IPropertyTreeIterator> channels;
                channels.setown(node.getElements("RoxieChannel"));
                unsigned j = 0;
                unsigned mindrive = (unsigned)-1;
                ForEach(*channels) {
                    unsigned k = channels->query().getPropInt("@number");
                    const char * dir = channels->query().queryProp("@dataDirectory");
                    unsigned d = dir?getPathDrive(dir):0;
                    if (d<mindrive) {
                        j = k;
                        mindrive = d;
                    }
                }
                if (j==0) {
                    ERRLOG("Cannot construct roxie cluster %s, no channel for node",cluster.queryProp("@name"));
                    return;
                }
                while (eps.ordinality()<j)
                    eps.append(nullep);
                eps.item(j-1) = ep;
            }
            else if (i<n) 
                eps.append(ep);
            else {
                ERRLOG("Cannot construct %s, Too many slaves defined [@slaves = %d, found slave %d]",cluster.queryProp("@name"),n,i+1);
                return;
            }
            i++;
        }
    }

    bool loadMachineMap()
    {
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


    void constructGroup(IPropertyTree& cluster,bool roxie,const char *processname, const char* defdir)
    {
        const char *groupname = cluster.queryProp("@name");
        const char *nodegroupname = cluster.queryProp("@nodeGroup");
        bool realcluster = !nodegroupname||!*nodegroupname||(strcmp(nodegroupname,groupname)==0);
        SocketEndpointArray eps;
        loadEndpoints(cluster,eps,roxie,false,processname);
        if (eps.ordinality()) {
            Owned<IGroup> grp = createIGroup(eps);
            addClusterGroup(groupname,grp,roxie?"Roxie":"Thor",realcluster,defdir);
//          DBGLOG("GROUP: %s updated\n",groupname);
        }
    }

    void constructFarmGroup(IPropertyTree& cluster)
    {
        Owned<IPropertyTreeIterator> farms = cluster.getElements("RoxieFarmProcess");  // probably only one but...
        ForEach(*farms) {
            IPropertyTree& farm = farms->query();
            StringBuffer groupname(cluster.queryProp("@name"));
            groupname.append("__");
            groupname.append(farm.queryProp("@name"));
            SocketEndpointArray eps;
            loadEndpoints(farm,eps,true,true,"RoxieServerProcess");
            if (eps.ordinality()) {
                Owned<IGroup> grp = createIGroup(eps);
                addClusterGroup(groupname.str(),grp,"RoxieFarm",true,farm.queryProp("@dataDirectory"));
    //          DBGLOG("GROUP: %s updated\n",groupname);
            }
        }           
    }


public:

    CInitGroups(unsigned _defaultTimeout)
        : groupsconnlock("constructGroup",SDS_GROUPSTORE_ROOT,true,false,_defaultTimeout)
    {
        defaultTimeout = _defaultTimeout;
    }

    void constructGroups()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
        if (!conn)
            return;
        IPropertyTree* root = conn->queryRoot();
        Owned<IPropertyTreeIterator> clusters;
        if (loadMachineMap()) { 
            clusters.setown(root->getElements("ThorCluster"));
            ForEach(*clusters) 
                constructGroup(clusters->query(),false,"ThorSlaveProcess",NULL);
            clusters.setown(root->getElements("RoxieCluster"));
            ForEach(*clusters) {
                const char *dir = clusters->query().queryProp("@slaveDataDir");
                if (!dir||!*dir)
                    dir = clusters->query().queryProp("@baseDataDir");
                constructGroup(clusters->query(),true,"RoxieSlave",dir);
                constructFarmGroup(clusters->query());
            }
            clusters.setown(root->getElements("EclAgentProcess"));
            ForEach(*clusters) {
                unsigned ins = 0;
                IPropertyTree &pt = clusters->query();
                const char *groupname = pt.queryProp("@name");
                if (groupname&&*groupname) {
                    Owned<IPropertyTreeIterator> insts = pt.getElements("Instance");
                    ForEach(*insts) {
                        const char *na = insts->query().queryProp("@netAddress");
                        if (na&&*na) {
                            SocketEndpoint ep(na);
                            if (!ep.isNull()) {
                                ins++;
                                StringBuffer gname("hthor__");
                                gname.append(groupname);
                                if (ins>1)
                                    gname.append('_').append(ins);
                                Owned<IGroup> grp = createIGroup(1,&ep);
                                addClusterGroup(gname.str(),grp,"hthor",true,NULL);
                            }
                        }
                    }
                }
            }
            IPropertyTree* groot = groupsconnlock.conn->queryRoot();
            Owned<IPropertyTreeIterator> grps = groot->getElements("Group");
            // correct cluster flags
            ForEach(*grps) {
                IPropertyTree &grp = grps->query();
                const char *name = grp.queryProp("@name");
                bool iscluster=false;
                ForEachItemIn(i,clusternames) 
                    if (strcmp(name,clusternames.item(i))==0) {
                        iscluster = true;
                        break;
                    }
                if (iscluster!=grp.getPropBool("@cluster"))
                    if (iscluster)
                        grp.setPropBool("@cluster",true);
                    else
                        grp.removeProp("@cluster");

            }
        }
//      DBGLOG("Initialized cluster groups");
    }
};

void initClusterGroups(unsigned timems)
{
    CInitGroups init(timems);
    init.constructGroups();
}


class CDaliDFSServer: public Thread, public CTransactionLogTracker, implements IDaliServer
{  // Coven size
    
    bool stopped;
    unsigned defaultTimeout;
public:

    IMPLEMENT_IINTERFACE;

    CDaliDFSServer()
        : Thread("CDaliDFSServer"), CTransactionLogTracker(MDFS_MAX)
    {
        stopped = true;
        defaultTimeout = INFINITE; // server uses default
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
        initClusterGroups();
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
        CMessageBuffer mb;
        stopped = false;
        unsigned throttlecount = 0;
        unsigned last;
        while (!stopped) {
            try {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DFS_REQUEST,NULL)) {
                    if (throttlecount&&(last-msTick()<10))
                        throttlecount--;
                    else
                        throttlecount = DFSSERVER_THROTTLE_COUNT;
                    processMessage(mb);
                    if (throttlecount==0) {
                        WARNLOG("Throttling CDaliDFSServer");
                        Sleep(DFSSERVER_THROTTLE_TIME);
                    }
                    last = msTick();
                }   
                else
                    stopped = true;
            }
            catch (IException *e) {
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
        trc.appendf("iterateFiles(%s,%s,%s)",wildname.sget(),recursive?"recursive":"",attr.sget());
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
        StringArray *allowedscopes=NULL;
        StringArray scopes;
        unsigned start = msTick();
        if (querySessionManager().checkScopeScansLDAP()&&getScopePermissions(NULL,NULL,(unsigned)-1)) {

            class cListScopeScanner: public CScopeScanner
            {
                StringArray &scopes;
            public:
                cListScopeScanner(StringArray &_scopes) 
                    : scopes(_scopes)
                {
                }
                void processScope(const char *name)
                {
                    scopes.append(name);
                }
            } sscanner(scopes);
            sscanner.scan(wildname.get(),recursive);
            if (msTick()-start>100)
                PROGLOG("TIMING(scopescan): %s: took %dms for %d scopes",trc.str(),msTick()-start,scopes.ordinality());
            start = msTick();
            ForEachItemInRev(i,scopes) {
                int perm = getScopePermissions(scopes.item(i),udesc,0);     // don't audit
                if (!HASREADPERMISSION(perm)) 
                    scopes.remove(i);
            }
            if (msTick()-start>100)
                PROGLOG("TIMING(LDAP): %s: took %dms, %d lookups",trc.str(),msTick()-start,scopes.ordinality());
            start = msTick();
            allowedscopes = &scopes;
        }

        if (!allowedscopes||(allowedscopes->ordinality())) {
            class CAttributeScanner: public CFileScanner
            {
                MemoryBuffer &mb;
                unsigned &count;
            public:
                CAttributeScanner(MemoryBuffer &_mb,unsigned &_count) 
                    : mb(_mb), count(_count)
                {
                }
                virtual void processFile(IPropertyTree &file,StringBuffer &name,bool issuper)
                {
                    CDFAttributeIterator::serializeFileAttributes(mb, file, name,issuper);
                    count++;
                }
            } scanner(mb,count);
            scanner.scan(wildname.get(),recursive,includesuper,NULL,allowedscopes);
            if (msTick()-start>100)
                PROGLOG("TIMING(filescan): %s: took %dms, %d files",trc.str(),msTick()-start,count);
        }
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
        trc.appendf("iterateRelationships(%s,%s,%s,%s,%s,%s,%d)",primary.sget(),secondary.sget(),primflds.sget(),secflds.sget(),kind.sget(),cardinality.sget(),(int)payloadb);
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        unsigned start = msTick();
        unsigned count=0;
        Linked<IPropertyTree> sroot = querySDSServer().lockStoreRead();
        try { 
            StringBuffer xpath;
            CDistributedFileDirectory::getFileRelationshipXPath(xpath,primary,secondary,primflds,secflds,kind,cardinality,((payloadb==0)||(payloadb==1))?&payload:NULL);
            IPropertyTree *root = sroot->queryPropTree(querySdsRelationshipsRoot());
            Owned<IPropertyTreeIterator> iter = root?root->getElements(xpath.str()):NULL;
            mb.append(count);
            // save as sequence of branches
            if (iter) {
                ForEach(*iter.get()) {
                    iter->query().serialize(mb);
                    count++;
                }
            }
        }
        catch (...) { 
            querySDSServer().unlockStoreRead(); 
            throw; 
        }
        querySDSServer().unlockStoreRead();
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
        trc.appendf("setFileAccessed(%s)",lname.sget());
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
        CScopeConnectLock sconnlock("setFileAccessed",dlfn,false,false,defaultTimeout);
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
        trc.appendf("setFileProtect(%s,%s,%s)",lname.sget(),owner.sget(),set?"true":"false");
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
        CScopeConnectLock sconnlock("setFileProtect",dlfn,false,false,defaultTimeout);
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
        trc.appendf("getFileTree(%s,%d)",lname.sget(),ver);
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
        const CDfsLogicalFileName *logicalname=&dlfn;   
        Owned<IDfsLogicalFileNameIterator> redmatch;
        loop {
            StringBuffer tail;
            checkLogicalName(*logicalname,udesc,true,false,true,"getFileTree on");
            CScopeConnectLock sconnlock("getFileTree",*logicalname,false,false,defaultTimeout);
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
        trc.appendf("getGroupTree(%s)",gname.sget());
        if (queryTransactionLogging())
            transactionLog.log("%s", trc.str());
        byte ok;
        CConnectLock connlock("getGroupTree",SDS_GROUPSTORE_ROOT,false,false,defaultTimeout);
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
} *daliDFSServer = NULL;


IDFAttributesIterator *CDistributedFileDirectory::getDFAttributesIterator(const char *wildname, bool recursive, bool includesuper,INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout)
{
    if (!wildname||!*wildname||(strcmp(wildname,"*")==0)) {
        recursive = true;
    }
    CMessageBuffer mb;
    mb.append((int)MDFS_ITERATE_FILES).append(wildname).append(recursive).append("").append(includesuper); // "" is legacy
    if (user)
        user->serialize(mb);
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
    return new CDFAttributeIterator(mb);
}

IDFScopeIterator *CDistributedFileDirectory::getScopeIterator(const char *basescope, bool recursive,bool includeempty,IUserDescriptor *user)
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
    CConnectLock connlock("CDistributedFileDirectory::loadScopeContents",querySdsFilesRoot(),false,false,defaultTimeout); 
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

void CDistributedFileDirectory::setFileAccessed(CDfsLogicalFileName &dlfn, const CDateTime &dt, const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout)
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
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

void CDistributedFileDirectory::setFileProtect(CDfsLogicalFileName &dlfn, const char *owner, bool set, const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout)
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
    if (foreigndali) 
        foreignDaliSendRecv(foreigndali,mb,foreigndalitimeout);
    else
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DFS_REQUEST);
    checkDfsReplyException(mb);
}

IPropertyTree *CDistributedFileDirectory::getFileTree(const char *lname, const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout, bool expandnodes)
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
    if (foreigndali) 
        resolveForeignFiles(ret,foreigndali);
    return ret.getClear();
}

IFileDescriptor *CDistributedFileDirectory::getFileDescriptor(const char *lname,const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname,foreigndali,user,foreigndalitimeout,false);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        Owned<CDistributedSuperFile> sfile = new CDistributedSuperFile(this,tree, dlfn, NULL);
        return sfile->getFileDescriptor(NULL);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?


    IFileDescriptor * fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),0);
    if (fdesc)
        fdesc->setTraceName(lname);
    return fdesc;
}

IDistributedFile *CDistributedFileDirectory::getFile(const char *lname,const INode *foreigndali,IUserDescriptor *user,unsigned foreigndalitimeout)
{
    Owned<IPropertyTree> tree = getFileTree(lname,foreigndali,user,foreigndalitimeout,false);
    if (!tree)
        return NULL;
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
        CDfsLogicalFileName dlfn;
        dlfn.set(lname);
        return new CDistributedSuperFile(this,tree, dlfn, NULL);
    }
    if (strcmp(tree->queryName(),queryDfsXmlBranchName(DXB_File))!=0)
        return NULL; // what is it?

    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(tree,&queryNamedGroupStore(),IFDSF_FOREIGN_GROUP);
    if (!fdesc)
        return NULL;
    fdesc->setTraceName(lname);
    IDistributedFile *ret = createNew(fdesc,lname,true);
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

void CDistributedFileDirectory::linkSuperOwner(IDistributedFile &subfile,const char *superfile,bool link,IDistributedFileTransaction *transaction)
{
    subfile.lockProperties();
    IDistributedSuperFile *ssub = subfile.querySuperFile();
    if (ssub) {
        CDistributedSuperFile *cdsuper = QUERYINTERFACE(ssub,CDistributedSuperFile);
        cdsuper->linkSuperOwner(superfile,link,transaction);
    }
    else {
        CDistributedFile *cdfile = QUERYINTERFACE(&subfile,CDistributedFile);
        cdfile->linkSuperOwner(superfile,link,transaction);
    }
    subfile.unlockProperties();
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


IDaliServer *createDaliDFSServer()
{
    assertex(!daliDFSServer); // initialization problem
    daliDFSServer = new CDaliDFSServer();
    return daliDFSServer;
}

CDFAction::CDFAction(IDistributedFileTransaction *_transaction)
{
    assertex(_transaction);
    transaction.set(_transaction->baseTransaction()); // smacks of overkill
    transaction->addAction(this);
    transaction->clearFiles();  // clean slate on open files (this is a bit arcane)

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
    try {
        Owned<IDistributedFile> file1 = lookup(lfn1,user,false,NULL,defaultTimeout);    
        Owned<IDistributedFile> file2 = lookup(lfn2,user,false,NULL,defaultTimeout);            
        if (!file1) {
            errstr.appendf("File %s not found",lfn1);
            ret = DFS_COMPARE_RESULT_FAILURE;
        }
        else if (!file2) {
            errstr.appendf("File %s not found",lfn2);
            ret = DFS_COMPARE_RESULT_FAILURE;
        }
        else {
            unsigned np = file1->numParts();
            if (np!=file2->numParts()) {
                errstr.appendf("Files %s and %s have differing number of parts",lfn1,lfn2);
                ret = DFS_COMPARE_RESULT_FAILURE;
            }
            else {
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

bool CDistributedFileDirectory::filePhysicalVerify(const char *lfn,bool includecrc,StringBuffer &errstr,IUserDescriptor *user=NULL)
{
    bool differs = false;
    Owned<IDistributedFile> file = lookup(lfn,user,false,NULL,defaultTimeout);  
    if (!file) {
        errstr.appendf("Could not find file: %s",lfn);
        return false;
    }
    try {
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
                        part->lockProperties(defaultTimeout).setProp("@modified",dt2.getString(str).str());
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
                            part->lockProperties(defaultTimeout).setPropInt64("@size",sz2);
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
                        part->lockProperties(defaultTimeout).setPropInt64("@fileCrc",(unsigned)crc2);
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
        Owned<IDFScopeIterator> siter = queryDistributedFileDirectory().getScopeIterator(NULL,true,false,user);
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
    CFileConnectLock lock;
    bool readonly;
    IArrayOf<IRemoteConnection> children;
    unsigned defaultTimeout;

    static StringBuffer &getSubPath(StringBuffer &path,unsigned idx)
    {
        return path.append("SubFile[@num=\"").append(idx+1).append("\"]");
    }


    IRemoteConnection *connectChild(const char *cname)
    {
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(cname))  
            throw MakeStringException(-1,"CLightWeightSuperFileConn::connectChild invalid subfile name '%s'",cname);
        StringBuffer query;
        lfn.makeFullnameQuery(query,DXB_File,true);
        // no lock here should be ok as parent locked
        IRemoteConnection *conn = conn = querySDS().connect(query.str(),myProcessSession(),0,defaultTimeout);
        if (!conn) {
            lfn.makeFullnameQuery(query.clear(),DXB_SuperFile,true);
            conn = conn = querySDS().connect(query.str(),myProcessSession(),0,defaultTimeout);
            if (!conn) 
                throw MakeStringException(-1,"CLightWeightSuperFileConn::connectChild could not find subfile '%s'",cname);
        }
        children.append(*conn);
        return conn;

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
    
    CLightWeightSuperFileConn(unsigned _defaultTimeout)
    {
        defaultTimeout = _defaultTimeout;
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
        if (!lock.init(title,lfn,DXB_SuperFile,!readonly,true,timeout)) {
            if (!autocreate)        // NB not !*autocreate here !
                return false;
            IPropertyTree *root = createPTree();
            root->setPropInt("@interleaved",2); 
            root->setPropInt("@numsubfiles",0); 
            root->setPropTree("Attr",getEmptyAttr());   
            parent->addEntry(lfn,root,true,false);
            if (!lock.init(title,lfn,DXB_SuperFile,true,true,timeout)) 
                throw MakeStringException(-1,"%s: Cannot create superfile '%s'",title,name);
            if (autocreate)
                *autocreate = true;
        }
        StringBuffer reason;
        if (!readonly&&checkProtectAttr(name,lock.queryRoot(),reason)) 
            throw MakeStringException(-1,"CDistributedSuperFile::%s %s",title,reason.str());
        return true;
    }

    bool commonChildren(CLightWeightSuperFileConn &other)
    {
        StringBuffer supername1;
        StringBuffer supername2;
        lock.queryRoot()->getProp("@name",supername1);
        other.lock.queryRoot()->getProp("@name",supername2);
        Owned<IPropertyTreeIterator> iter1 = lock.queryRoot()->getElements("SubFile");
        Owned<IPropertyTreeIterator> iter2 = other.lock.queryRoot()->getElements("SubFile");
        StringBuffer name1;
        StringBuffer name2;
        ForEach(*iter1) {
            if (iter1->query().getProp("@name",name1.clear())) {
                if (strcmp(name1.str(),supername2.str())==0)        // super is child of other
                    return true;
                ForEach(*iter2) {
                    if (iter2->query().getProp("@name",name2.clear())) {
                        if (strcmp(name1.str(),name2.str())==0)
                            return true;
                        if (strcmp(name2.str(),supername1.str())==0)        // super is child of other
                            return true;
                    }
                }
            }
        }
        return false;
    }

    void unlinkChildren(StringArray &toremove,bool checksingleparent)
    {
        IPropertyTree * root = lock.queryRoot();
        loop {
            IPropertyTree *tree = root->queryPropTree("SubFile[1]");
            if (!tree)
                break;
            toremove.append(tree->queryProp("@name"));
            root->removeTree(tree);
        }
        root->setPropInt("@numsubfiles",0);
        root->setPropTree("Attr",getEmptyAttr());
        const char *supername = root->queryProp("OrigName");
        StringBuffer xpath;
        ForEachItemIn(i,toremove) {
            IRemoteConnection *cc = connectChild(toremove.item(i));
            xpath.clear().appendf("SuperOwner[@name=\"%s\"]",supername);
            cc->queryRoot()->removeProp(xpath.str());
        }
    }

    void linkChildren(StringArray &toadd)
    {
        IPropertyTree * root = lock.queryRoot();
        if (root->getPropInt("@numsubfiles")!=0)
            throw MakeStringException(-1,"CLightWeightSuperFileConn::linkChildren: Destination superfile add (%s) not empty!",root->queryProp("OrigName"));
        root->setPropInt("@numsubfiles",toadd.ordinality());
        const char *supername = root->queryProp("OrigName");
        ForEachItemIn(i,toadd) {
            IPropertyTree *t = root->addPropTree("SubFile",createPTree("SubFile"));
            const char *sname = toadd.item(i);
            t->setProp("@name",sname);
            t->setPropInt("@num",i+1);
            IRemoteConnection *cc = connectChild(sname);
            t = cc->queryRoot()->addPropTree("SuperOwner",createPTree("SuperOwner"));
            t->setProp("@name",supername);
        }
        migrateAttr((children.ordinality()==1)?children.item(0).queryRoot():NULL,root);
    }

    void moveChildren(CLightWeightSuperFileConn &dst)
    {
        // first move subfiles
        IPropertyTree * srcroot = lock.queryRoot();
        IPropertyTree * dstroot = dst.lock.queryRoot();
        if (dstroot->getPropInt("@numsubfiles")!=0)
            throw MakeStringException(-1,"CLightWeightSuperFileConn::moveChildren: Destination of superfile move (%s) not empty!",dstroot->queryProp("OrigName"));
        const char *srcsupername = srcroot->queryProp("OrigName");
        const char *dstsupername = dstroot->queryProp("OrigName");
        StringBuffer xpath;
        IPropertyTree *newt;
        StringBuffer subpath;
        unsigned n=0;
        for (unsigned sni=0;;sni++) {
            getSubPath(subpath.clear(),sni); // correct order
            IPropertyTree *tree = srcroot->queryPropTree(subpath.str());
            if (!tree)
                break;
            newt = createPTreeFromIPT(tree);
            srcroot->removeTree(tree);
            newt = dstroot->addPropTree("SubFile",newt);
            IRemoteConnection *cc = dst.connectChild(newt->queryProp("@name"));
            xpath.clear().appendf("SuperOwner[@name=\"%s\"]",srcsupername);
            tree = cc->queryRoot()->queryPropTree(xpath.str());
            if (tree)
                tree->setProp("@name",dstsupername);
            n++;
        }
        srcroot->setPropInt("@numsubfiles",0);
        dstroot->setPropInt("@numsubfiles",n);
        // replace Attr
        IPropertyTree *attr = srcroot->queryPropTree("Attr[1]");  
        if (attr) {
            newt = createPTreeFromIPT(attr);
            srcroot->removeTree(attr);
        }
        else
            newt = getEmptyAttr();
        srcroot->setPropTree("Attr",getEmptyAttr());
        dstroot->setPropTree("Attr",newt);
    }

    void disconnect(bool commit)
    {
        if (lock.conn()&&!readonly) {
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
                lock.conn()->rollback();
            }
        }
        lock.kill();
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

void CDistributedFileDirectory::promoteSuperFiles(unsigned numsf,const char **sfnames,const char *addsubnames,bool delsub,bool createonlyonesuperfile,IUserDescriptor *user,unsigned timeout,StringArray &outunlinked)
{
    if (!numsf) 
        return;
    // first lock all files
    struct cSF
    {
        CIArrayOf <CLightWeightSuperFileConn> conn;
        bool rollback;
        cSF(unsigned n,unsigned _defaultTimeout) 
        { 
            rollback = true; // if any faults rollback
            while (n--)
                conn.append(* new CLightWeightSuperFileConn(_defaultTimeout));
        }
        ~cSF() 
        { 
            if (rollback) {
                ForEachItemInRev(i,conn) {
                    conn.item(i).disconnect(false);
                }
            }
        }
    } superfiles(numsf,defaultTimeout);
    unsigned start = msTick();
    loop {
        unsigned i;
        for (i=0;i<numsf;i++) {
            try {
                bool autocreate;
                if (!superfiles.conn.item(i).connect(this,"promoteSuperFile",sfnames[i],false,&autocreate,PROMOTE_CONN_TIMEOUT)) {
                    PROGLOG("promoteSuperFile: Cannot connect to %s",sfnames[i]);
                    throw MakeStringException(-1,"promoteSuperFile: Cannot connect to %s",sfnames[i]);
                }
                if (autocreate&&createonlyonesuperfile) {
                    while (numsf>i+1) {
                        numsf--;
                        superfiles.conn.remove(numsf);
                    }
                }
            }
            catch (IDFS_Exception *e) {
                // catch timeout here
                if ((e->errorCode()!=DFSERR_LookupConnectionTimout)||(msTick()-start>((timeout==INFINITE)?SDS_CONNECT_TIMEOUT:timeout)))
                    throw;
                EXCLOG(e,"promoteSuperFiles");
                e->Release();
            }
        }
        if (i==numsf) 
            break;
        while (i--)
            superfiles.conn.item(i).disconnect(false);
        PROGLOG("promoteSuperFiles contention, connection pausing");
        Sleep(getRandom()%PROMOTE_DELAY); // let one get in
    }
    // all locked now
    // check all children different
    ForEachItemIn(i1,superfiles.conn) {
        for (unsigned j=0;j<i1;j++) {
            if (superfiles.conn.item(j).commonChildren(superfiles.conn.item(i1)))
                throw MakeStringException(-1,"Superfiles %s and %s have common subfiles or are related",sfnames[i1],sfnames[j]);
        }
    }
    superfiles.conn.item(numsf-1).unlinkChildren(outunlinked,delsub);
    for (unsigned i=numsf-1;i;i--)
        superfiles.conn.item(i-1).moveChildren(superfiles.conn.item(i)); // NB moveChildren assumes target empty
    StringArray toadd;
    CslToStringArray(addsubnames, toadd, true); 
    ForEachItemIn(i2,toadd) {  // NB names are not normalized so do so
        CDfsLogicalFileName lfn;
        if (!lfn.setValidate(toadd.item(i2)))
            throw MakeStringException(-1,"promoteSuperFiles: invalid logical name to add: %s",toadd.item(i2));
        toadd.replace(lfn.get(),i2);
    }
    superfiles.conn.item(0).linkChildren(toadd);
    superfiles.rollback = false; 
    // commit changes
    ForEachItemIn(i3,superfiles.conn) {
        superfiles.conn.item(i3).disconnect(true);
    }
    if (delsub) {
        ForEachItemIn(j,outunlinked) 
            removePhysical(outunlinked.item(j),0,NULL,NULL,user);
    }


}


ISimpleSuperFileEnquiry * CDistributedFileDirectory::getSimpleSuperFileEnquiry(const char *logicalname,const char *title,unsigned timeout)
{
    Owned<CLightWeightSuperFileConn> ret = new CLightWeightSuperFileConn(defaultTimeout);
    if (ret->connect(this,title,logicalname,true,NULL,timeout))
        return ret.getClear();
    return NULL;
}

bool CDistributedFileDirectory::getFileSuperOwners(const char *logicalname, StringArray &owners)
{
    CFileConnectLock lock;
    CDfsLogicalFileName lfn;
    if (!lfn.setValidate(logicalname))
        throw MakeStringException(-1,"CDistributedFileDirectory::getFileSuperOwners: Invalid file name '%s'",logicalname);
    if (lfn.isMulti()||lfn.isExternal()||lfn.isForeign()) 
        return false;
    DfsXmlBranchKind bkind;
    if (!lock.initany("CDistributedFileDirectory::getFileSuperOwners",lfn,bkind,false,false,defaultTimeout))
        return false;
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
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,defaultTimeout);
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
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,defaultTimeout);
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
            CConnectLock connlock("lookupFileRelationships",querySdsRelationshipsRoot(),false,false,defaultTimeout);
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
    if (!exists(primary))
        throw MakeStringException(-1,"addFileRelationship primary %s does not exist",primary);
    CDfsLogicalFileName sfn;
    if (!sfn.setValidate(secondary))  
        throw MakeStringException(-1,"addFileRelationship invalid secondary name '%s'",secondary);
    if (sfn.isExternal()||sfn.isForeign()||sfn.isQuery())
        throw MakeStringException(-1,"addFileRelationship secondary %s not allowed",sfn.get());
    secondary = sfn.get();
    if (!exists(secondary))
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
        CConnectLock connlock("addFileRelation",querySdsRelationshipsRoot(),true,false,defaultTimeout);
        if (!connlock.conn) {
            CConnectLock connlock2("addFileRelation.2",querySdsFilesRoot(),true,false,defaultTimeout);
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

    CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,defaultTimeout);
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
        CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,defaultTimeout);
        doRemoveFileRelationship(connlock.conn,filename,NULL,NULL,NULL,NULL);
    }
    {   // kludge bug in getElements if connection used twice
        CConnectLock connlock("removeFileRelation",querySdsRelationshipsRoot(),true,false,defaultTimeout);
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

void CDistributedFileDirectory::renameFileRelationships(const char *oldname,const char *newname,IFileRelationshipIterator *reliter)
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
                addFileRelationship(pf,sf,r.queryPrimaryFields(),r.querySecondaryFields(),r.queryKind(),r.queryCardinality(),r.isPayload(),r.queryDescription());
        }
        catch (IException *e)
        {
            EXCLOG(e,"renameFileRelationships");
            e->Release();
        }
    }
}


bool CDistributedFileDirectory::publishMetaFileXML(const CDfsLogicalFileName &logicalname,IUserDescriptor *user=NULL)
{
    if (logicalname.isExternal()||logicalname.isForeign()||logicalname.isQuery()) 
        return false;
    Owned<IPropertyTree> file = getFileTree(logicalname.get(),NULL,user,FOREIGN_DALI_TIMEOUT,true);
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
            SocketEndpoint &ep = ips.item(i);
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

bool CDistributedFileDirectory::isProtectedFile(const CDfsLogicalFileName &logicalname, unsigned timeout)
{
    DfsXmlBranchKind bkind;
    CFileConnectLock fconnattrlock(true);
    if (!fconnattrlock.initany("CDistributedFileDirectory::isProtectedFile",logicalname,bkind,true,false,timeout?timeout:INFINITE))
        return false; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = fconnattrlock.queryRoot()->getElements("Protect");
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

unsigned CDistributedFileDirectory::queryProtectedCount(const CDfsLogicalFileName &logicalname, const char *owner)
{
    DfsXmlBranchKind bkind;
    CFileConnectLock fconnattrlock(true);
    if (!fconnattrlock.initany("CDistributedFileDirectory::isProtectedFile",logicalname,bkind,true,false,defaultTimeout))
        return 0; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = fconnattrlock.queryRoot()->getElements("Protect");
    unsigned count = 0;
    ForEach(*wpiter) {
        IPropertyTree &t = wpiter->query();
        const char *name = t.queryProp("@name");
        if (!owner||!*owner||(name&&(strcmp(owner,name)==0))) 
            count += t.getPropInt("@count");
    }
    return count;
}

bool CDistributedFileDirectory::getProtectedInfo(const CDfsLogicalFileName &logicalname, StringArray &names, UnsignedArray &counts)
{
    DfsXmlBranchKind bkind;
    CFileConnectLock fconnattrlock(true);
    if (!fconnattrlock.initany("CDistributedFileDirectory::isProtectedFile",logicalname,bkind,true,false,defaultTimeout))
        return false; // timeout will raise exception
    Owned<IPropertyTreeIterator> wpiter = fconnattrlock.queryRoot()->getElements("Protect");
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
