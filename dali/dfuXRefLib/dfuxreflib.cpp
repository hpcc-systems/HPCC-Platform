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

// DFU XREF Program
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jptree.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dadiags.hpp"
#include "danqs.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "daft.hpp"
#include "rmtfile.hpp"
#include "dautils.hpp"
#include "jptree.hpp"

#include "XRefNodeManager.hpp"

//#define PARTS_SIZE_NEEDED
//#define CROSSLINK_CHECK_NEEDED

static bool fixSizes = false;

#define TESTXML

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_DFS_ROOT        "Files" // followed by scope/name


//#define CONNECT_EACH_PATH

#include "dfuxreflib.hpp"

extern IPropertyTree *getDirectory(const char * directory, INode * node, unsigned short port);


void testGetDir()
{
    SocketEndpoint ep("10.173.72.1");
    Owned<INode> node = createINode(ep);
    Owned<IPropertyTree> results = getDirectory("/c$/thordata;/d$/thordata",node,getDaliServixPort());
    if (results) {
        PROGLOG("--------------------------------------------------------");
        PROGLOG("DIR");
        StringBuffer dirs;
        toXML(results, dirs, 2);
        PROGLOG("\n%s\n----------------------------------------------------------",dirs.str());
    }
}


#define CFBcluster          (0x00000001)
#define CFBname             (0x00000002)
#define CFBnumparts         (0x00000004)
#define CFBpartslost        (0x00000008)
#define CFBprimarylost      (0x00000010)
#define CFBreplicatedlost   (0x00000040)
#define CFBmodified         (0x00000080)
#define CFBsize             (0x00000100)
#define CFBmismatchedsizes  (0x00000400)
#define CFBpartnode         (0x00010000)
#define CFBpartnum          (0x00020000)
#define CFBpartreplicate    (0x00040000)
#define CFBpartprimary      (0x00080000)
#define CFBpartmask (CFBpartnode|CFBpartnum|CFBpartreplicate|CFBpartprimary)

static IPropertyTree *addBranch(IPropertyTree *dst,const char *name)
{
    return dst->addPropTree(name,createPTree());
}


struct CFileEntry;
struct CDfuDirEntry;
class COrphanEntry;
struct CLogicalNameEntry;
typedef CFileEntry *CFileEntryPtr;
typedef CDfuDirEntry *CDfuDirEntryPtr;
typedef COrphanEntry *COrphanEntryPtr;
typedef CLogicalNameEntry *CLogicalNameEntryPtr;

typedef MapStringTo<CLogicalNameEntryPtr> CLogicalNameMap;
//typedef MapStringTo<CFileEntryPtr> CFileEntryMap;
typedef MapStringTo<CDfuDirEntryPtr> CDfuDirEntryMap;
typedef MapStringTo<COrphanEntryPtr> COrphanEntryMap;

Owned <IFileIOStream> outfileio;

void outf(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void outf(const char *fmt, ...) 
{
    va_list args;
    va_start(args, fmt);
    StringBuffer buf;
    buf.valist_appendf(fmt,args);
    va_end(args);
    if (outfileio)
        outfileio->write(buf.length(),buf.str());
    else
        printf("%s",buf.str());
}

#define FEF_RESOLVED  0x01
#define FEF_REPLICATE 0x02

struct CFileEntry: public CInterface
{
    CFileEntry(const char *_fname,CLogicalNameEntry *_owner,unsigned _part,bool _replicate,__int64 _size, bool compresskludge, __int64 _compsize);
    StringBuffer &getLogicalName(StringBuffer &buf);

    unsigned queryHash()
    {
        return keyhash;
    }

    bool comparePath(const char *toname)
    {
        return strcmp(toname,fname)==0;
    }

    inline bool resolved() { return (flags&FEF_RESOLVED)!=0; }
    inline bool replicate() { return (flags&FEF_REPLICATE)!=0; }

    //IpAddress ip;
    //StringAttr dir;                   // TBD 
    //StringAttr tail;
    StringAttr fname;
    CLogicalNameEntry *owner;
#ifdef PARTS_SIZE_NEEDED
    __int64 size;
    __int64 expsize;
#endif
#ifdef CROSSLINK_CHECK_NEEDED
    Owned<CFileEntry> crosslink;
#endif
    unsigned keyhash;
    unsigned short part;
    byte flags;
    //bool replicate;
    //bool resolved;
};


class CFileEntryMap : public SuperHashTableOf<CFileEntry, const char> 
{
public:
    ~CFileEntryMap()
    {
        releaseAll();
    }
    virtual void onAdd(void *e) 
    { 
    }

    virtual void onRemove(void *e) 
    { 
        CFileEntry &elem=*(CFileEntry *)e;      
        elem.Release();
        
    }

    virtual unsigned getHashFromElement(const void *e) const
    {
        return ((CFileEntry *) e)->queryHash();
    }

    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *)fp, strlen((const char *)fp), 0);
    }

    virtual const void *getFindParam(const void *e) const
    {
        return ((CFileEntry *) e)->fname;
    }

    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const
    {
        return (0 == strcmp(((CFileEntry *) e)->fname, (const char *)fp));
    }
};



StringBuffer &substnum(StringBuffer &str,const char *sub,unsigned n)
{
    StringBuffer out;
    const char *s=str.str();
    const char *p=sub;
    while (*s) {
        if (*s==*p) {
            p++;
            if (!*p) {
                out.append(n);
                p = sub;
            }
        }
        else {
            if (p!=sub) {
                out.append(p-sub,sub);
                p = sub;
            }
            out.append(*s);
        }
        s++;
    }
    str.swapWith(out);
    return str;
}

static StringBuffer &makeScopeQuery(const char *scope,StringBuffer &query)
{
    const char *s=scope;
    loop {
        const char *e=strstr(s,"::");
        if (s!=scope)
            query.append('/');
        StringBuffer tail;
        if (e)
            tail.append(e-s,s);
        else 
            tail.append(s);
        query.append("Scope[@name=\"").append(tail.toLowerCase().str()).append("\"]"); 
        if (!e)
            break;
        s = e+2;
    }
    return query;
}

static const char *splitScope(const char *name,StringBuffer &scope)
{
    const char *s=strstr(name,"::");
    if (s) {
        loop {
            const char *ns = strstr(s+2,"::");
            if (!ns)
                break;
            s = ns;
        }
        StringBuffer str;
        str.append(s-name,name);
        scope.append(str.toLowerCase().str());
        return s+2;
    }
    scope.append(".");
    return name;
}

static StringBuffer &makeFullnameQuery(const char *lname,StringBuffer &query)
{
    StringBuffer scope;
    StringBuffer tail(splitScope(lname,scope));
    return makeScopeQuery(scope.str(),query).append("/File[@name=\"").append(tail.toLowerCase().str()).append("\"]");
}

static StringBuffer &makeAbsoluteFullnameQuery(const char *lname,StringBuffer &query)
{
    query.append(SDS_DFS_ROOT "/");
    return makeFullnameQuery(lname,query);
}


struct CLogicalNameEntry;

class CEndpointItem: public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    SocketEndpoint ep;
    rank_t rank;
};

struct CDfuDirEntry: public CInterface
{
    CDfuDirEntry(const char *_name,unsigned _clustsize)
        : name(_name)
    {
        size = 0;
        num = 0;
        numdir = 0;
        maxsize = 0;
        minsize = -1;
        clustersize = _clustsize;
    }
    StringAttr name;
    __int64 size;
    __int64 minsize;
    IpAddress minip;
    __int64 maxsize;
    IpAddress maxip;
    unsigned num;
    unsigned numdir;
    unsigned clustersize;
    StringBuffer &getskew(StringBuffer &skew)
    {
        if (clustersize&&size&&(minsize<maxsize)) {
            __int64 av = size/(__int64)clustersize;
            if (av) {
                unsigned pcp = (unsigned)(maxsize*100/av);
                unsigned pcn = (unsigned)(minsize*100/av);
                if ((pcp>100)||(pcn<100))
                    skew.appendf("+%d%%/-%d%%",pcp-100,100-pcn);
            }
        }
        return skew;
    }
};


static unsigned short getDafsPort(const SocketEndpoint &ep,unsigned &numfails,CriticalSection *sect)
{
    if (sect) {
        CriticalBlock block(*sect);
        if (numfails>5)
            return 0;
    }
    else if (numfails>5)
        return 0;
    if (testDaliServixPresent(ep)) 
        return ep.port?ep.port:getDaliServixPort();
    StringBuffer err("Failed to connect to DaFileSrv on ");
    ep.getUrlStr(err);
#ifdef _WIN32
    ERRLOG("%s",err.str());
    if (sect) {
        CriticalBlock block(*sect);
        numfails++;
    }
    else 
        numfails++;
#else
    throw MakeStringExceptionDirect(-1, err.str());
#endif
    return 0;
}

class CEndpointTable: public SuperHashTableOf<CEndpointItem,SocketEndpoint>
{
public:
    ~CEndpointTable()
    {
        releaseAll();
    }

    void onAdd(void *)
    {
        // not used
    }

    void onRemove(void *e)
    {
        CEndpointItem &elem=*(CEndpointItem *)e;        
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CEndpointItem &elem=*(const CEndpointItem *)e;        
        return elem.ep.hash(0);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        return ((const SocketEndpoint *)fp)->hash(0);
    }

    const void * getFindParam(const void *p) const
    {
        const CEndpointItem &elem=*(const CEndpointItem *)p;        
        return &elem.ep;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CEndpointItem *)et)->ep.equals(*(SocketEndpoint *)fp);
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CEndpointItem,SocketEndpoint);


};



class CXRefManagerBase
{
protected:
    struct cMessage: public CInterface
    {
        StringAttr lname;
        StringAttr msg;
        cMessage(const char *_lname,const char *_msg)
            : lname(_lname), msg(_msg)
        {
        }
    };

    
public:
    IArrayOf<IGroup> knowngroups;
    StringAttrArray knowngroupnames;
    Linked<IXRefProgressCallback> msgcallback;
    CriticalSection logsect;
    unsigned totalCompressed;
    __int64 totalUncompressedSize;
    __int64 totalCompressedSize;
    __int64 totalSizeOrphans;
    unsigned totalNumOrphans;
    CIArrayOf<CLogicalNameEntry> logicalnamelist;
    CLogicalNameMap logicalnamemap;
    CLogicalNameMap logicaldirmap;
    CFileEntryMap filemap;
    CDfuDirEntryMap dirmap;
    CIArrayOf<CDfuDirEntry> dirlist;
    COrphanEntryMap orphanmap;
    CIArrayOf<COrphanEntry> orphanlist;
    CEndpointTable EndpointTable;
    CIArrayOf<cMessage> errors;
    CIArrayOf<cMessage> warnings;
    CriticalSection inprogresssect;
    SocketEndpointArray inprogress;


    CXRefManagerBase()
    {
        totalCompressed=0;
        totalUncompressedSize=0;
        totalCompressedSize=0;
    }

    virtual ~CXRefManagerBase();

    IGroup *resolveGroup(IGroup *_grp,StringBuffer &gname)  // takes ownership of _grp
    {
        StringBuffer name;
        IGroup *grp = _grp;
        ForEachItemIn(i,knowngroups) {
            GroupRelation gr = _grp->compare(&knowngroups.item(i));
            const char *gn = knowngroupnames.item(i).text.get();
            if ((gr==GRidentical)||(gn&&*gn&&((gr==GRbasesubset)||(gr==GRwrappedsuperset)))) {
                _grp->Release();
                _grp=NULL;
                if (gn)
                    gname.append(gn);
                grp = &knowngroups.item(i);
                grp->Link();
                break;
            }
        }
        

        if (_grp) {
            if (queryNamedGroupStore().find(_grp,gname)) {
                if (gname.length()) {
                    _grp->Release();
                    grp=queryNamedGroupStore().lookup(gname.str());
                }
            }   
            else {
                name.clear();
            }
            knowngroupnames.append(* new StringAttrItem(gname.str()));
            knowngroups.append(*LINK(grp));
        }
        return grp;
    }

    void log(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (msgcallback) {
            msgcallback->progress(line.str());
        }
        else {
            PROGLOG("%s",line.str());
        }
    }
    void error(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (errors.ordinality()<1000) {
            errors.append(*new cMessage(lname,line.str()));
            if (errors.ordinality()==1000) 
                errors.append(*new cMessage("","error limit exceeded (1000), truncating"));
        }

        if (msgcallback) {
            StringBuffer cbline("ERROR: ");
            cbline.append(lname).append(": ").append(line);
            msgcallback->progress(cbline.str());
        }
        else {
            ERRLOG("%s: %s",lname,line.str());
        }
    }

    void warn(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (warnings.ordinality()<1000) {
            warnings.append(*new cMessage(lname,line.str()));
            if (warnings.ordinality()==1000) 
                warnings.append(*new cMessage("","warning limit (1000) exceeded, truncating"));
        }
        if (msgcallback) {
            StringBuffer cbline("WARNING: ");
            cbline.append(lname).append(": ").append(line);
            msgcallback->progress(cbline.str());
        }
        else {
            WARNLOG("%s: %s",lname,line.str());
        }
    }

    void addNodeInProgress(INode &node)
    {
        CriticalBlock block(inprogresssect);
        SocketEndpoint ep = node.endpoint();
        inprogress.append(ep);
    }

    void removeNodeInProgress(INode &node)
    {
        CriticalBlock block(inprogresssect);
        SocketEndpoint ep = node.endpoint();
        inprogress.zap(ep);
        if (inprogress.ordinality()==0)
            return;
        StringBuffer msg("Waiting for ");
        ForEachItemIn(i,inprogress) {
            if (i)
                msg.append(", ");
            inprogress.item(i).getIpText(msg);
        }
        if (msgcallback) 
            msgcallback->progress(msg.str());
        else 
            PROGLOG("%s",msg.str());
    }

    void incDirSize(const IpAddress &ip,const char *dir,__int64 sz,unsigned num,unsigned numdir,unsigned clustsize)
    {
        CDfuDirEntryPtr *entryp= dirmap.getValue(dir);
        CDfuDirEntryPtr entry;
        if (entryp) 
            entry = *entryp;
        else {
            entry = new CDfuDirEntry(dir,clustsize);
            dirmap.setValue(dir,entry);
            dirlist.append(*entry);
        }
        entry->num+=num;
        entry->numdir+=numdir;
        entry->size+=sz;
        if ((entry->minsize==-1)||(entry->minsize>sz)) {
            entry->minsize=sz;
            entry->minip.ipset(ip);
        }
        if (entry->maxsize<sz) {
            entry->maxsize=sz;
            entry->maxip.ipset(ip);
        }
    }

};





struct CLogicalNameEntry: public CInterface
{
    CLogicalNameEntry(CXRefManagerBase &_manager, const char *_lname,IPropertyTree &file) // takes ownership of grp
        : manager(_manager),lname(_lname) 
    {
        replicated = false;
        outsidedir = 0;
        primarynum = 0;
        replicatenum = 0;
        max = file.getPropInt("@numparts");
        missinggrp = false;
        unknowngrp = false;
        const char *s=file.queryProp("@group");  // TBD - Handling for multiple clusters?
        if (s&&*s) {
            grpname.append(s);
            if (isAnonCluster(s))
                manager.warn(_lname,"File has anonymous cluster");
            if (strchr(s,','))
                manager.error(_lname,"XREF can't handle multi-file clusters yet!");
        }
        s = file.queryProp("@partmask");
        if (s&&*s)
            pmask.set(s);
        s = file.queryProp("@modified");
        if (s&&*s)
        {
            modified.set(s);
        }

        done = false;
        wrongwidth = false;
        primaryresolved = NULL;
        replicateresolved = NULL;
        nummismatchedsizes=0;
        mismatchedsizeinfo = NULL;
        totalsize=0;
        recordsize = file.getPropInt("Attr/@recordSize",-1);
        compressed = (file.getPropInt("Attr/@rowCompressed", 0)!=0)||(file.getPropInt("Attr/@blockCompressed", 0)!=0);
        grouped = file.getPropInt("Attr/@grouped", 0)!=0;
        const char *partmask = file.queryProp("@partmask");
        StringBuffer tmp;
        if (partmask&&*partmask) {
            if (!containsPathSepChar(partmask)) {
                const char *dir = file.queryProp("@directory");
                if (dir&&*dir) 
                    tmp.append(dir).append(getPathSepChar(dir));
            }
            tmp.append(partmask);
        }
        tmp.toLowerCase();
        substnum(tmp,"$n$",max);
        dirpartmask.set(tmp.str());
    }
    ~CLogicalNameEntry()
    {
        free(primaryresolved);
        free(replicateresolved);
    }
    void setGroup(IGroup *_grp,IGroup *lgrp)    // note incoming group does *not* override cluster 
                                                // takes ownerhip of lgrp if set
    {
        StringBuffer name;
        unsigned n = 0;
        if (_grp) {
            n = _grp->ordinality();
            grp.setown(manager.resolveGroup(_grp,name));
            if (!grp) {
                SocketListCreator cr;
                SocketEndpointArray epa;
                ForEachNodeInGroup(i,*grp) {
                    SocketEndpoint ep = grp->queryNode(i).endpoint();
                    epa.append(ep);
                }
                cr.addSockets(epa);
                manager.warn(lname.get(),"Cluster group %s did not resolve",cr.getText());
            }
        }
        if (grpname.length()) {
            if(!_grp||(stricmp(grpname.str(),name.str())!=0)) {
                if (!lgrp)
                    lgrp = queryNamedGroupStore().lookup(grpname.str());
                if (lgrp) {
                    if (name.length())
                        mismatchgrp.set(name.str());
                    grp.setown(lgrp);
                }
                else {
                    unknowngrp = true;
                    grpname.swapWith(name);
                    manager.warn(lname.get(),"Cluster group %s not found",grpname.str());
                }
            }
        }
        else {
            grpname.swapWith(name);
            if (n>1)
                missinggrp = true;
        }
        primaryresolved=(CFileEntry * *)calloc(max,sizeof(CFileEntry *));
        replicateresolved=(CFileEntry * *)calloc(max,sizeof(CFileEntry *));
        if (!grp) 
            manager.warn(lname.get(),"No cluster group set");
    }
    bool remove(IUserDescriptor *user)
    {
        IDistributedFileDirectory &fdir = queryDistributedFileDirectory();
        Owned<IDistributedFile> file = fdir.lookup(lname.get(),user);
        if (!file)
            return false;
        file->detach();
        outf("Removed %s from DDFS\n",lname.get());
        return true;
    }

    IGroup *queryGroup()
    {
        return grp.get();
    }

    unsigned queryNumParts()
    {
        return max;
    }

    void addCrossLink(CLogicalNameEntry *entry)
    {
        if (entry) {
            ForEachItemIn(i,crosslinked) {
                if (&crosslinked.item(i)==entry)
                    return;
            }
            crosslinked.append(*LINK(entry));
        }
    }


    void addMismatchedSize(__int64 expected, __int64 actual, __int64 expanded,  CFileEntry *entry)
    {
        unsigned i = nummismatchedsizes++;
        mismatchedsizeinfo = (MMSinfo *)(mismatchedsizeinfo?realloc(mismatchedsizeinfo,nummismatchedsizes*sizeof(MMSinfo)):malloc(sizeof(MMSinfo)));
        mismatchedsizeinfo[i].expected = expected;
        mismatchedsizeinfo[i].actual = actual;
        mismatchedsizeinfo[i].expanded = expanded;
        mismatchedsizeinfo[i].entry = entry;
    }

    bool incomplete()
    {
        for (unsigned j=0;j<max;j++) 
            if (!primaryresolved[j]&&!replicateresolved[j]) 
                return true;
        return false;
    }

    void resolve(CFileEntry *entry);
    IPropertyTree *addFileBranch(IPropertyTree *dst,unsigned flags);

    StringAttr lname;
    StringAttr pmask;
    StringAttr modified;
    unsigned primarynum;
    unsigned replicatenum;
    unsigned max;
    unsigned outsidedir;
    StringBuffer errdir;
    Owned<IGroup> grp;
    SocketEndpointArray outsidenodes;
    CIArrayOf<CLogicalNameEntry> crosslinked;   
    bool unknowngrp;
    StringAttr mismatchgrp;
    bool missinggrp;
    bool wrongwidth;
    struct MMSinfo {
        __int64 expected;
        __int64 actual;
        __int64 expanded;
        CFileEntry *entry;
    } *mismatchedsizeinfo;
    unsigned nummismatchedsizes;
    StringBuffer grpname;
    bool replicated;
    bool done;
    CFileEntry  **primaryresolved;
    CFileEntry  **replicateresolved;
    __int64 totalsize;
    size32_t recordsize;
    bool compressed;
    bool grouped;
    StringAttr dirpartmask;
    CXRefManagerBase &manager;
};


CFileEntry::CFileEntry(const char *_fname,CLogicalNameEntry *_owner,unsigned _part,bool _replicate,__int64 _size, bool compresskludge, __int64 _compsize)
{
    fname.set(_fname);
    keyhash = hashc((const unsigned char *)fname.get(), fname.length(), 0);
    flags = 0;
    if (_replicate)
        flags |= FEF_REPLICATE;
    part = _part;

#ifdef PARTS_SIZE_NEEDED
    size = (_compsize>0)?_compsize:_size;
    expsize = (_compsize>0)?_size:-1;
#endif
    owner = _owner;
    if (compresskludge)
        owner->compressed = true;
}

StringBuffer &CFileEntry::getLogicalName(StringBuffer &buf)
{
    buf.append(owner->lname.get()).append('[').append(part);
    if (replicate())
        buf.append('R');
    return buf.append(']');
}


CXRefManagerBase::~CXRefManagerBase() 
{
    ForEachItemIn(i,logicalnamelist) {
        CLogicalNameEntry &item = logicalnamelist.item(i);
        item.crosslinked.kill();
    }
}



static bool constructLogicalName(const char *fullname,const char *basedir,StringBuffer &logicalname,unsigned &num,unsigned &max)
{
    max = 0;
    num = 0;
    // mask filename with $P$ extension or normal filename to logical name and num (0 for $P$) and max
    while (*fullname&&(toupper(*fullname)==toupper(*basedir))) {
        fullname++;
        basedir++;
    }
    if (isPathSepChar(*fullname))
        fullname++;
    if (!*fullname)
        return false;
    const char *s=fullname;
    loop {
        const char *e=s;
        while (*e&&!isPathSepChar(*e)) 
            e++;
        if (!*e)
            break;
        if (logicalname.length())
            logicalname.append("::");
        logicalname.append(e-s,s);
        s = e+1;
    }
    const char *ext = strchr(s,'.');
    if (!ext)
        return false;
    loop {
        const char *ne = strchr(ext+1,'.');
        if (!ne)
            break;
        ext = ne;
    }
    const char *es=ext;
    if (memicmp(es,"._$P$_of_",9)==0) 
        es += 9;
    else {
        if (memcmp(es,"._",2)!=0)
            return false;
        es+=2;
        StringBuffer n;
        while (*es!='_') {
            if (!*es)
                return false;
            n.append(*(es++));
        }
        num = atoi(n.str());
        if (memicmp(es,"_of_",4)!=0) 
            return false;
        es += 4;
    }
    max = atoi(es);
    if (logicalname.length())
        logicalname.append("::");
    logicalname.append(ext-s,s);
    logicalname.toLowerCase();
    return true;
}

static void constructPartname(const char *filename,unsigned n, StringBuffer &pn,bool replicate)
{

    StringBuffer repdir;
    // mask filename with $P$ extension to paticular part filename
    if (replicate) {
        setReplicateDir(filename,repdir);
        filename = repdir.str();
    }
    while (*filename) {
        if (memicmp(filename,"_$P$_",5)==0) {
            filename = filename+5;
            pn.append('_').append(n).append('_');
        }
        else {
            pn.append(*filename);
            filename++;
        }
    }
}

static bool parseFileName(const char *name,StringBuffer &mname,unsigned &num,unsigned &max,bool &replicate)
{
    // takes filename and creates mask filename with $P$ extension
    StringBuffer nonrepdir;
    replicate = setReplicateDir(name,nonrepdir,false);
    if (replicate) 
        name = nonrepdir.str();
    num = 0;
    max = 0;
    loop {
        char c=*name;
        if (!c)
            break;
        if ((c=='.')&&(name[1]=='_')) {
            unsigned pn = 0;
            const char *s = name+2;
            while (*s&&isdigit(*s)) {
                pn = pn*10+(*s-'0');
                s++;
            }
            if (pn&&(memicmp(s,"_of_",4)==0)) {
                unsigned mn = 0;
                s += 4;
                while (*s&&isdigit(*s)) {
                    mn = mn*10+(*s-'0');
                    s++;
                }
                if ((mn!=0)&&((*s==0)||(*s=='.'))&&(mn>=pn)) {          // NB allow trailing extension
                    mname.append("._$P$_of_").append(mn);
                    if (*s)
                        mname.append(s);
                    num = pn;
                    max = mn;
                    return true;
                }
            }
        }
        mname.append(c);
        name++;
    }
    return false;
}           

        



class COrphanEntry: public CInterface
{
public:
    StringAttr basedir;
    StringAttr fname;
    StringAttr modified;
    unsigned max;
    __int64 size;
    SocketEndpointArray primaryepa;
    UnsignedArray primarypna;
    SocketEndpointArray replicateepa;
    UnsignedArray replicatepna;
    bool dirfailed;
    CLogicalNameEntry *misplaced;
    CXRefManagerBase &manager;
    byte incompletestate; // 0 unknown, 1 ignore, 2 incomplete
    COrphanEntry(CXRefManagerBase &_manager, const char *_fname,const char *_basedir,unsigned _max,const char *_modified,CLogicalNameEntry *_misplaced)
        : manager(_manager),fname(_fname), basedir(_basedir), modified(_modified)
    {
        dirfailed = false;
        max = _max;
        size = 0;
        misplaced = _misplaced;
        incompletestate = 0;
    }
    void add(SocketEndpoint &ep,unsigned pn,bool replicate,__int64 sz)
    {
        if (max&&(pn>=max)) {
            return;
        }
        size += sz;
        if (replicate) {
            replicateepa.append(ep);
            replicatepna.append(pn);
        }
        else {
            primaryepa.append(ep);
            primarypna.append(pn);
        }
    }

    bool complete()
    {
        if (incompletestate)
            return (incompletestate>1);
        if (!max) { 
            // check matches mask
            return true;
        }
        if (max==num()) {
            // check for a group here
            incompletestate = 2;
            return true;
        }
        incompletestate = 1;
        return false;
            
    }

    bool isSingleton()
    {
        if (primaryepa.ordinality()==1) {
            if (replicateepa.ordinality()>1)
                return false;
            if (primarypna.item(0)>0)
                return false;
        }
        if (replicateepa.ordinality()==1) {
            if (primaryepa.ordinality()>1)
                return false;
            if (replicatepna.item(0)>0)
                return false;
        }
        return true;
    }
    bool singletonName(StringBuffer &name,bool replicate)
    {
        SocketEndpoint ep;
        if (replicate) {
            if (replicateepa.ordinality()==0)
                return false;
            ep = replicateepa.item(0);
        }
        else {
            if (primaryepa.ordinality()==0) 
                return false;
            ep = primaryepa.item(0);
        }
        StringBuffer partname;
        constructPartname(fname.get(),1,partname,replicate);
        RemoteFilename rfn;
        rfn.setPath(ep,partname.str());
        rfn.getRemotePath(name);
        return true;
    }

    unsigned num()
    {
        if (!max)
            return 1;
        bool *done = (bool *)calloc(sizeof(bool),max);
        ForEachItemIn(i1,primarypna) 
            done[primarypna.item(i1)] = true;
        ForEachItemIn(i2,replicatepna) 
            done[replicatepna.item(i2)] = true;
        unsigned ret = 0;
        for (unsigned i=0;i<max;i++)
            if (done[i])
                ret++;
        free(done);
        return ret;
    }

    void getEps(StringBuffer &out,bool replicate)
    {
        SocketEndpointArray &epa=(replicate?replicateepa:primaryepa);
        UnsignedArray &pna=(replicate?replicatepna:primarypna);

        unsigned *sorted = new unsigned[epa.ordinality()];
        // its expected to be in order so just do insertion sort
        ForEachItemIn(i1,pna) {
            unsigned i = i1;
            unsigned pn = pna.item(i1);
            while (i&&(pna.item(sorted[i-1])>pn))
                i--;
            if (i<i1)
                memmove(sorted+i+1,sorted+i,sizeof(unsigned)*(i1-i));
            sorted[i] = i1;
        }
        StringBuffer prefix;
        ForEachItemIn(i2,epa) {
            if (i2)
                out.append(", ");
            SocketEndpoint &item = epa.item(sorted[i2]);
            StringBuffer cur;
            item.getUrlStr(cur);
            const char *s1 = prefix.str();
            const char *s2 = cur.str();
            if (prefix.length()&&(memcmp(s1,s2,prefix.length())==0))
                out.append(s2+prefix.length());
            else {
                unsigned n = 2;
                out.append(s2);
                prefix.clear();
                while (*s2&&n) {
                    prefix.append(*s2);
                    if (*s2=='.')
                        n--;
                    s2++;
                }
            }
            out.append('[').append(pna.item(sorted[i2])+1).append(']');
        }
    }

    IPropertyTree *addFileBranch(IPropertyTree *dst,unsigned flags)
    {
        unsigned i;
        unsigned numparts = max?max:1;
        StringBuffer buf;
        IPropertyTree *out;
        if (flags&CFBpartmask) {
    
            unsigned np = (flags&CFBpartprimary)?primarypna.ordinality():0;
            unsigned nr = (flags&CFBpartreplicate)?replicatepna.ordinality():0;
            if (np+nr==0)
                return NULL;
            out = addBranch(dst,"File");
            if (fname.get())
            {
                StringBuffer expfn;
                StringBuffer repfn;
                const char *pm = fname.get();
                if (np+nr==1) {
                    expandMask(expfn, fname.get(), (np?primarypna.item(0):replicatepna.item(0)), numparts);
                    pm = expfn.str();
                    if ((np==0)&&setReplicateDir(pm,repfn))
                        pm = repfn.str();
                }
                else if (((flags&CFBpartprimary)==0) && setReplicateDir(pm,repfn)) // not sure if this used
                    pm = repfn.str();
                out->setProp("Partmask",pm);
            }
        }
        else
            out = addBranch(dst,"File");
        if (flags&CFBnumparts) {
            out->setPropInt("Numparts",numparts);
        }
        if (flags&CFBmodified) {
            if (modified.get()) 
                out->setProp("Modified",modified.get());
        }
        if (flags&CFBcluster) {
            // don't support cluster resolution on orphans
            flags |= (CFBpartnum|CFBpartnode);
        }
        if (flags&CFBsize) {
            out->setPropInt64("Size",size);
        }
        if (flags&CFBpartmask) { 
            for (int copy=0;copy<=1;copy++) {
                if ((!copy&&(flags&CFBpartprimary))||
                    (copy&&(flags&CFBpartreplicate))) {
                    UnsignedArray *parts = new UnsignedArray[numparts];
                    if (copy) {
                        ForEachItemIn(i2,replicatepna) 
                            parts[replicatepna.item(i2)].append(i2);
                    }
                    else {
                        ForEachItemIn(i1,primarypna) 
                            parts[primarypna.item(i1)].append(i1);
                    }
                    for (i=0;i<numparts;i++) {
                        if (parts[i].ordinality()) {
                            StringBuffer xpath;
                            xpath.appendf("Part[Num=\"%d\"]",i+1);
                            IPropertyTree *b = out->queryPropTree(xpath.str());
                            if (!b)
                                b = addBranch(out,"Part");
                            if (flags&CFBpartnode) {
                                ForEachItemIn(i3,parts[i]) {
                                    unsigned p = parts[i].item(i3);
                                    if (copy) {
                                        replicateepa.item(p).getUrlStr(buf.clear());    
                                        b->addProp("RNode",buf.str());
                                    }
                                    else {
                                        primaryepa.item(p).getUrlStr(buf.clear());  
                                        b->addProp("Node",buf.str());
                                    }
                                }
                            }
                            if (flags&CFBpartnum) 
                                b->setPropInt("Num",i+1);
                        }
                    }
                    delete [] parts;
                }
            }
        }
        return out;
    }
};






void CLogicalNameEntry::resolve(CFileEntry *entry)
{
    unsigned part=entry->part-1;
    if (part>=max) {                                                        // MORE
        manager.error(lname.get(),"Part %d: Invalid entry (greater than max %d)",part+1,max);
        return;
    }
    if (entry->replicate()) {
        if (replicateresolved[part]) {
            manager.error(lname.get(),"Part %d: Multiple replicated part entry",part+1);
        }
        else {
            replicatenum++;
            replicateresolved[part] = entry;
        }
    }
    else {
        if (primaryresolved[part]) {
            manager.error(lname.get(),"Part %d: Multiple primary part entry",part+1);
        }
        else {
            primarynum++;
            primaryresolved[part] = entry;
#ifdef PARTS_SIZE_NEEDED
            if (entry->size>0) {
                totalsize += entry->size;
                if (((int)recordsize>0)&&
                    ((entry->size%recordsize)!=0)&&
                    !compressed&&
                    (!grouped||(entry->size%(recordsize+1)!=0))) {
                    manager.error(lname.get(),"Part %d: Record size %d not multiple of file size %"I64F"d\n",part+1,recordsize,entry->size);
                }
            }
#endif
        }

    }
#ifdef PARTS_SIZE_NEEDED
    if (primaryresolved[part]&&replicateresolved[part]&&(primaryresolved[part]->size!=replicateresolved[part]->size)) {
        manager.error(lname.get(),"Part %d: primary size %"I64F"d is different to replicate size %"I64F"d",part+1,primaryresolved[part]->size,replicateresolved[part]->size);
    }
#endif
}


IPropertyTree *CLogicalNameEntry::addFileBranch(IPropertyTree *dst,unsigned flags)
{
    unsigned i;
    StringBuffer buf;
    IPropertyTree *out = addBranch(dst,"File");
    if (flags&CFBname) {
        out->setProp("Name",lname.get());
    }
    if (flags&CFBpartmask) {
        if (pmask.get()) {
            StringBuffer tmp(pmask.get());
            tmp.toLowerCase();
            out->setProp("Partmask",tmp.str());
        }
    }
    if (flags&CFBnumparts) {
        out->setPropInt("Numparts",max);
    }
    if (flags&CFBpartslost) {
        unsigned n=0;
        for (i=0;i<max;i++)
            if ((primaryresolved[i]==NULL)&&(replicateresolved[i]==NULL))
                n++;
        out->setPropInt("Partslost",n);
    }
    if (flags&CFBprimarylost) {
        if (primarynum!=max)
            out->setPropInt("Primarylost",max-primarynum);
    }
    if (flags&CFBreplicatedlost) {
        if (replicatenum!=max)
            out->setPropInt("Replicatedlost",max-replicatenum);
    }
    if (flags&CFBmodified) {
        if (modified.get()) 
            out->setProp("Modified",modified.get());
    }
    if (flags&CFBcluster) {
        if (grpname.length()) 
            out->setProp("Cluster",grpname.str());
    }
#ifdef PARTS_SIZE_NEEDED
    if (flags&CFBsize) {
        out->setPropInt64("Size",totalsize);
    }
#endif
    if ((flags&CFBpartmask)&&grp&&grp->ordinality()) {
        unsigned n=0;
        unsigned nc = (flags&CFBpartreplicate)?2:1; 
        unsigned rep;
        for (i=0;i<max;i++) {
            for (rep=0;rep<nc;rep++) {
                CFileEntry *e=rep?replicateresolved[i]:primaryresolved[i];
                if ((flags&CFBpartslost)&&e)
                    continue;
                IPropertyTree *part = addBranch(out,"Part");
                if (flags&CFBpartnode) {
                    grp->queryNode((i+rep)%grp->ordinality()).endpoint().getUrlStr(buf.clear());    // TBD check grp==cluster TBD
                    part->setProp("Node",buf.str());
                }
                if (flags&CFBpartnum) {
                    part->setPropInt("Num",i+1);
                }
                if (flags&CFBpartreplicate) {
                    if (rep)
                        part->setPropInt("Replicate",1);
                }
                if (flags&CFBmismatchedsizes) {
                    unsigned k = nummismatchedsizes;
                    while (k&&(mismatchedsizeinfo[k-1].entry!=e))
                        k--;
                    if (k) {
                        part->setPropInt64("Recordedsize",mismatchedsizeinfo[k-1].expected);
                        part->setPropInt64("Actualsize",mismatchedsizeinfo[k-1].actual);
                    }
                }
            }
        }
    }
    return out;
}






struct TimedBlock
{
    char *msg;
    unsigned start;
    unsigned limit;
    unsigned ln;
    TimedBlock(const char *_msg,unsigned _limit,unsigned _ln)
    {
        msg = strdup(_msg); 
        ln = _ln;
        start = msTick();
        limit = _limit;
    }
    ~TimedBlock()
    {
        unsigned elapsed=msTick()-start;
        if (elapsed>limit)
            PrintLog("TIME: %s took %dms - line(%d)",msg,elapsed,ln);
        free(msg);
    }
};

#define TIMEDBLOCK(name,msg,lim) TimedBlock name(msg,lim,__LINE__);
      



void loadFromDFS(CXRefManagerBase &manager,IGroup *grp,unsigned numdirs,const char **dirbaselist,const char* Cluster)
{
    rank_t r=grp->ordinality();
    while (r--) {
        SocketEndpoint ep=grp->queryNode(r).endpoint();
        CEndpointItem *item=manager.EndpointTable.find(ep);
        if (!item) {
            item = new CEndpointItem;
            item->ep = ep;
            item->rank = r;
            manager.EndpointTable.add(*item);
        }
    }

    class Cscanner: public CSDSFileScanner
    {
        SocketEndpointArray epa;
        CLogicalNameEntry* lnentry; // set during processFile
        StringBuffer localname;
        StringBuffer remotename;
        unsigned pass;
        unsigned numdirs;
        const char **dirbaselist;
        CXRefManagerBase &manager;
        IGroup *testgroup;
        Owned<IGroup> cachedgroup;

    public:
        Cscanner(CXRefManagerBase &_manager, IGroup *_testgroup, unsigned _numdirs,const char **_dirbaselist)
            : manager(_manager)
        {
            numdirs = _numdirs;
            dirbaselist = _dirbaselist;
            testgroup = _testgroup;
        }

        bool checkGroupOk(const char *grpname)
        {
            if (!grpname)
                return true;
            cachedgroup.setown(queryNamedGroupStore().lookup(grpname));
            if (!cachedgroup) 
                return true;
            ForEachNodeInGroup(i,*cachedgroup)
                if (testgroup->isMember(&cachedgroup->queryNode(i))) // would be better to have hash here TBD
                    return true;
            return false;
        }


        virtual bool checkFileOk(IPropertyTree &file,const char *filename)
        {
            StringArray groups;
            getFileGroups(&file,groups);
            ForEachItemIn(i,groups) {
                if (checkGroupOk(groups.item(i)))
                    return true;
            }
            return false;
        }

        void processParts(IPropertyTree &root)
        {
            Owned<IPropertyTreeIterator> iter;
            unsigned numparts = root.getPropInt("@numparts");
            MemoryBuffer mb;
            if (root.getPropBin("Parts",mb)) {
                iter.setown(deserializePartAttrIterator(mb));   // this itterator is in order
                ForEach(*iter) {
                    IPropertyTree &part = iter->query();
                    unsigned partno = part.getPropInt("@num",0);
                    SocketEndpoint ep;
                    const char *eps = part.queryProp("@node");
                    if (eps&&*eps)
                        ep.set(eps);
                    processPart(root,part,partno,numparts,ep);
                }
            }
            else { // slow but should be going anyway
                iter.setown(root.getElements("Part")); // use parts
                IArrayOf<IPropertyTree> parts;
                Owned<IPropertyTree> empty = createPTree("Attr");
                unsigned i;
                for (i=0;i<numparts;i++)
                    parts.append(*empty.getLink());
                unsigned lastpartno=0;
                ForEach(*iter) {
                    IPropertyTree &part = iter->query();
                    unsigned partno = part.getPropInt("@num",0);
                    if (partno&&(partno!=lastpartno)&&(partno<=numparts)) {
                        parts.replace(*createPTreeFromIPT(&part),partno-1);
                        lastpartno = partno;
                    }
                }
                for (i=0;i<numparts;i++) {
                    IPropertyTree &part = parts.item(i);
                    SocketEndpoint ep;
                    const char *eps = part.queryProp("@node");
                    if (eps&&*eps)
                        ep.set(eps);
                    processPart(root,part,i+1,numparts,ep);
                }
            }
            
        }
        
        
        void processFile(IPropertyTree &file,StringBuffer &name)
        {
            if (!manager.msgcallback.get())
                DBGLOG("processFile %s",name.str());
            Owned<CLogicalNameEntry> lnentrybase = new CLogicalNameEntry(manager,name.str(),file);
            lnentry = lnentrybase;
            epa.kill();
            pass=0;
            processParts(file);         // could improve
            manager.logicalnamemap.setValue(name.str(), lnentry);
            lnentry->Link();
            manager.logicalnamelist.append(*lnentry);
            manager.logicaldirmap.setValue(lnentry->dirpartmask.get(), lnentry);
            lnentry->setGroup(epa.ordinality()?createIGroup(epa):NULL,cachedgroup.getLink());
            pass = 2;
            processParts(file);
        }

        void processFile(StringBuffer &name,StringBuffer &fullpath)
        {
            Owned<IRemoteConnection> conn = querySDS().connect(fullpath.str(),myProcessSession(),RTM_LOCK_READ|RTM_SUB, SDS_CONNECT_TIMEOUT);
            if (conn) // there is at least one duff entry that this is needed for
                processFile(*conn->queryRoot(),name);
        }


        void processPart(IPropertyTree &file,IPropertyTree &part,unsigned partno,unsigned numparts,SocketEndpoint &ep)
        {
            if (pass==0) {
                partno--;
                if (!ep.isNull()) {
                    SocketEndpoint nullep;
                    while (partno>=epa.ordinality())
                        epa.append(nullep);
                    epa.item(partno) = ep;
                }
            }
            else {
                bool replicate=false;
                const char *partname = part.queryProp("@name");
                const char *partmask = file.queryProp("@partmask");
                const char *partdir = file.queryProp("@directory");
                int replicateoffset = file.getPropInt("@replicateOffset",1);
                loop {
                    RemoteFilename rfn; 
                    IGroup *grp = lnentry->queryGroup();
                    if (!grp) {
                        manager.warn(lnentry->lname.get(),"No group found, ignoring logical file");
                        return;
                    }
                    constructPartFilename(grp,partno,numparts,partname,partmask,partdir,replicate,replicateoffset,rfn);
                    SocketEndpoint rep=rfn.queryEndpoint();
                    if (manager.EndpointTable.find(rep)!=NULL) {
                        rfn.getLocalPath(localname.clear());
                        bool dirmatch=false;
                        unsigned k;
                        for (k=0;k<numdirs;k++)
                        {
                            if (memicmp(localname.str(),dirbaselist[k],strlen(dirbaselist[k]))==0) {
                                dirmatch = true;
                                break;
                            }
                        }
                        if (dirmatch)                       {
                            rfn.getRemotePath(remotename.clear());
                            remotename.toLowerCase();
                            
                            CFileEntry *entry= new CFileEntry(remotename.str(),lnentry,partno,replicate,part.getPropInt64("@size", -1),part.getPropInt("@rowCompression", 0)!=0,part.getPropInt("@compressedSize", -1));
                                                        
                            CFileEntry *oldentry= manager.filemap.find(remotename.str());
                            if (oldentry)
                            {
                                if (oldentry->owner!=lnentry) {
#ifdef CROSSLINK_CHECK_NEEDED
                                    StringBuffer s1;
                                    StringBuffer s2;
                                    entry->getLogicalName(s1);
                                    oldentry->getLogicalName(s2);
                                    //outf("CROSSLINK: %s contains same file as %s\n",s1.str(),s2.str());

                                    entry->owner->addCrossLink(oldentry->owner);
                                    oldentry->owner->addCrossLink(entry->owner);
                                    oldentry->crosslink.setown(entry);
#endif
                                }
                            }
                            else {
                                manager.filemap.add(*entry);
                            }
                            
                        }
                        else {
                            lnentry->outsidedir++;
                            if (lnentry->errdir.length()==0)
                                lnentry->errdir.append(localname.str());
                        }
                    }
                    else {
                        if (lnentry->outsidenodes.find(rep)==NotFound)
                            lnentry->outsidenodes.append(rep);
                    }
                    if (replicate)
                        break;
                    replicate = true;
                }               
            }
        }

    } scanner(manager,grp,numdirs,dirbaselist);
    
    manager.log("Loading Files branch from SDS");

    Owned<IRemoteConnection> conn = querySDS().connect(SDS_DFS_ROOT,myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!conn) {
        throw MakeStringException(-1,"Could not connect to Files");
        
    }
    conn->changeMode(RTM_NONE);
    manager.log("Files loaded, scanning");
    scanner.scan(conn);
    manager.log("Scanning done");

}



    

class CPhysicalXREF
{
    unsigned numdirs;
    const char **dirbaselist;
    CriticalSection xrefsect;
    CXRefManagerBase &manager;

public:
    CPhysicalXREF(CXRefManagerBase &_manager, unsigned _numdirs,const char **_dirbaselist)
        : manager(_manager)
    {
        numdirs = _numdirs;
        dirbaselist = _dirbaselist;
        setDaliServixSocketCaching(true);               
    }
        

    void getBaseDir(const char *name,StringBuffer &basedir)
    {
        unsigned i;
        for (i=0;i<numdirs;i++) {
            size32_t l = strlen(dirbaselist[i]);
            if ((memicmp(dirbaselist[i],name,l)==0)&&isPathSepChar(name[l])) {
                basedir.append(dirbaselist[i]).toLowerCase();
                return;
            }
        }
    }

    void xreffile(SocketEndpoint &ep,const char *fullname,IPropertyTree *file)
    {
        RemoteFilename rname;
        StringBuffer name;
        rname.setPath(ep,fullname);
        rname.getRemotePath(name);
        name.toLowerCase();
        CFileEntry *entry= manager.filemap.find(name.str());
        __int64 sz = file->getPropInt64("@size", -1);
        if (entry) {
            do {
                entry->flags |= FEF_RESOLVED;
#ifdef PARTS_SIZE_NEEDED
                if (entry->owner->compressed) {
                    if ((entry->expsize>=0)&&((sz!=entry->size)||((entry->size==0)&&(entry->expsize>0)))) {
                        manager.error(entry->owner->lname.get(),"Part %d: %s size mismatch: recorded size %"I64F"d, actual size %"I64F"d, expanded size %"I64F"d",entry->part,entry->replicate?"replicate":"primary",entry->size,sz,entry->expsize); 
                        entry->owner->addMismatchedSize(entry->size,sz,entry->expsize,entry);
                    }
                    if (entry->size>=0) {
                        manager.totalCompressed++;
                        manager.totalCompressedSize+=sz;
                        manager.totalUncompressedSize+=entry->size;
                    }
                }
                else if ((entry->size>=0)&&(entry->size!=sz)) {
                    if (!entry->replicate) {
                        entry->owner->addMismatchedSize(entry->size,sz,-1,entry);
                    }
                    manager.error(entry->owner->lname.get(),"Part %d: %s size mismatch: recorded size %"I64F"d, actual size %"I64F"d",entry->part,entry->replicate?"replicate":"primary",entry->size,sz); 
                    entry->size=sz; // set to actual size for fix
                }
#endif              
                entry->owner->resolve(entry);
#ifdef CROSSLINK_CHECK_NEEDED
                entry = entry->crosslink;
#else
                entry = NULL;
#endif

            } while (entry);

        }
        else {
            manager.totalSizeOrphans += sz;
            manager.totalNumOrphans++;
            StringBuffer orphanname;
            unsigned m;
            unsigned n;
            bool replicate;
            parseFileName(fullname,orphanname,n,m,replicate);
            if (n>m) 
                manager.error(fullname, "Part %d: number greater than max %d",n+1,m);
            else {
                orphanname.toLowerCase();


                COrphanEntryPtr *entryp = manager.orphanmap.getValue(orphanname.str());
                COrphanEntryPtr entry;
                if (entryp) 
                    entry = *entryp;
                else {
                    StringBuffer basedir;
                    getBaseDir(orphanname.str(),basedir);
                    CLogicalNameEntryPtr *parentp = manager.logicaldirmap.getValue(orphanname.str());
                    entry = new COrphanEntry(manager,orphanname.str(),basedir.str(),m,file->queryProp("@modified"),parentp?*parentp:NULL);
                    manager.orphanmap.setValue(orphanname.str(),entry);
                    manager.orphanlist.append(*entry);
                }
                if (n)
                    n--;
                entry->add(ep,n,replicate,sz);
            }
        }
    }



    void xrefdir(SocketEndpoint &ep,StringBuffer &path,IPropertyTree *dirtree,unsigned clustsize)
    {
        unsigned fnum=0;
        unsigned dnum=0;
        size32_t pathlen = path.length();
        Owned<IPropertyTreeIterator> dirs= dirtree->getElements("directory");
        if (dirs->first()) {
            do {
                IPropertyTree &dir = dirs->query();
                if (path.length()&&!isPathSepChar(path.charAt(path.length()-1)))
                    path.append(getPathSepChar(path.str()));
                path.append(dir.queryProp("@name"));
                xrefdir(ep,path,&dir,clustsize);
                path.setLength(pathlen);
            } while (dirs->next());
            dnum++;
        }
        Owned<IPropertyTreeIterator> files= dirtree->getElements("file");
        if (files->first()) {
            do {
                IPropertyTree &file = files->query();
                const char *fname=file.queryProp("@name");
                size32_t l = strlen(fname);
                if ((l>4)&&(stricmp(fname+l-4,".crc")==0))
                    continue;
                if (path.length()&&!isPathSepChar(path.charAt(path.length()-1)))
                    path.append(getPathSepChar(path.str()));
                path.append(fname);
                xreffile(ep,path.str(),&file);
                path.setLength(pathlen);
                fnum++;
            } while (files->next());
        }
        manager.incDirSize(ep,path.str(),dirtree->getPropInt64("@size", 0),fnum,dnum,clustsize);

    }

    void xref(IPropertyTree *machine,unsigned clustsize)
    {
        CriticalBlock block(xrefsect);
        StringBuffer path;
        SocketEndpoint ep(machine->queryProp("@ip"));
        ep.port = 0;
        xrefdir(ep,path,machine,clustsize);
    }

};


class CXRefManager: public CXRefManagerBase
{

#ifdef PARTS_SIZE_NEEDED
    void fixSizeInDFS()
    {
        StringBuffer name;
        __int64 sz;
        ForEachItemIn(i,logicalnamelist) {
            CLogicalNameEntry &item = logicalnamelist.item(i);
            if (!item.done&&item.nummismatchedsizes) {
                Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(item.lname.get());
                if (file) {
                    outf("checking %s\n",item.lname.get());
                    Owned<IDistributedFilePartIterator> partiter = file->getIterator();
                    unsigned partno=0;
                    __int64 total = 0;
                    ForEach(*partiter) {
                        partno++;
                        Owned<IDistributedFilePart> part = &partiter->get();
                        RemoteFilename rname;
                        part->getFilename(rname);
                        rname.getRemotePath(name.clear());
                        name.toLowerCase();
                        CFileEntry *entry= filemap.find(name.str());
                        if (entry) {
                            __int64 sz = part->queryAttributes().getPropInt64("@size", -1);
                            if (sz!=entry->size) {
                                StringBuffer s1;
                                entry->getLogicalName(s1);
                                outf("SIZEFIX: Changing size for %s from %"I64F"d to %"I64F"d\n",s1.str(),sz,entry->size);
                                part->lockProperties().setPropInt64("@size", entry->size);
                                part->unlockProperties();
                            }
                            if (total!=-1)
                                total += entry->size;
                        }
                        else 
                            total = -1;
                    }
                    sz = file->queryAttributes().getPropInt64("@size", -1);
                    if (sz!=total) {
                        outf("SIZEFIX: Changing total size for %s from %"I64F"d to %"I64F"d\n",item.lname.get(),sz,total);
                        if (total!=-1)
                            file->lockProperties().setPropInt64("@size", total);
                        else
                            file->lockProperties().removeProp("@size");
                        file->unlockProperties();
                    }                   
                }
            }
        }
        outf("Size fix completed\n");
    }
#endif

    void xrefRemoteDirectories(IGroup *g,unsigned numdirs,const char **dirbaselist,unsigned numthreads)
    {
        if (numthreads<=1)
            numthreads = 10;    // should be safe now
        StringBuffer dirlist;
        unsigned j;
        for (j=0;j<numdirs;j++) {
            const char *basedir = dirbaselist[j];
            if (!basedir||!*basedir)
                continue;
            if (dirlist.length())
                dirlist.append(';');
            dirlist.append(basedir);
        }
        CPhysicalXREF cxref(*this,numdirs,dirbaselist);
        unsigned numfails=0;
        class casyncfor: public CAsyncFor
        {
            CPhysicalXREF &cxref;
            IGroup *g;
            const char *dirlist;
            bool abort;
            CXRefManagerBase &manager;
            CriticalSection crit;
            unsigned numfails;
        public:
            casyncfor(IGroup *_g, const char *_dirlist, CPhysicalXREF &_cxref, CXRefManagerBase *_manager)
                : cxref(_cxref), manager(*_manager)
            {
                g = _g;
                dirlist = _dirlist;
                abort = false;
                numfails = 0;
            }
            void Do(unsigned idx)
            {
                if (abort)
                    return;
                CriticalBlock block(manager.logsect); // only the get directory is async
                try {
                    StringBuffer msg;
                    INode &node = g->queryNode(idx);
                    node.endpoint().getUrlStr(msg);
                    manager.log("Getting directories for %s",msg.str());
                    manager.addNodeInProgress(node);
                    Owned<IPropertyTree> results;
                    {
                        CriticalUnblock unblock(manager.logsect);
                        unsigned short port = getDafsPort(node.endpoint(),numfails,&crit);
                        results.setown(getDirectory(dirlist,&node,port));
                    }
                    manager.log("Crossreferencing %s",msg.str());
                    cxref.xref(results,g->ordinality());
                    manager.removeNodeInProgress(node);
                }
                catch (IException *)
                {
                    abort = true;
                    throw;
                }
            }
        } afor(g,dirlist.str(),cxref,this);
        afor.For(g->ordinality(), numthreads,false,true);
        ForEachItemIn(i1,logicalnamelist) {
            CLogicalNameEntry &item = logicalnamelist.item(i1);
            if (item.nummismatchedsizes) {
                error(item.lname.get(),"%d part%s physical size differ from recorded size\n",item.nummismatchedsizes,item.nummismatchedsizes>1?"s":"");
            }
            if (item.crosslinked.ordinality()) {
                StringBuffer to;
                ForEachItemIn(cli1,item.crosslinked) {
                    if (cli1)
                        to.append(", ");
                    to.append(item.crosslinked.item(cli1).lname.get());
                }
                error(item.lname.get(),"Crosslinked with %s",to.str());
            }
        }
    }

    void listDir(IGroup *g,unsigned numdirs,const char **dirbaselist)
    {
        class cDirScan
        {
        public:
            char pathsepchar;

            void scanfile(SocketEndpoint &ep,const char *fullname,IPropertyTree *file)
            {
                const char * tail = fullname;
                loop {
                    const char *s = strchr(tail,pathsepchar);
                    if (!s)
                        break;
                    tail = s+1;
                }
                if (memicmp(tail+3,".HISTORY.",9)!=0)
                    return;
                RemoteFilename rname;
                StringBuffer name;
                rname.setPath(ep,fullname);
                rname.getRemotePath(name);
                Owned<IFile> f= createIFile(name.str());
                outf("%12"I64F"d %s\n",f->size(),name.str());
            }



            void scandir(SocketEndpoint &ep,StringBuffer &path,IPropertyTree *dirtree)
            {
                size32_t pathlen = path.length();
                Owned<IPropertyTreeIterator> dirs= dirtree->getElements("directory");
                if (dirs->first()) {
                    do {
                        IPropertyTree &dir = dirs->query();
                        if (path.length()&&(path.charAt(path.length()-1)!=pathsepchar))
                            path.append(pathsepchar);
                        path.append(dir.queryProp("@name"));
                        scandir(ep,path,&dir);
                        path.setLength(pathlen);
                    } while (dirs->next());
                }
                Owned<IPropertyTreeIterator> files= dirtree->getElements("file");
                if (files->first()) {
                    do {
                        IPropertyTree &file = files->query();
                        const char *fname=file.queryProp("@name");
                        size32_t l = strlen(fname);
    //                  if ((l>4)&&(stricmp(fname+l-4,".crc")==0))
    //                      continue;
                        if (path.length()&&(path.charAt(path.length()-1)!=pathsepchar))
                            path.append(pathsepchar);
                        path.append(fname);
                        scanfile(ep,path.str(),&file);
                        path.setLength(pathlen);
                    } while (files->next());
                }

            }

            void scan(IPropertyTree *machine)
            {
                StringBuffer path;
                SocketEndpoint ep(machine->queryProp("@ip"));
                ep.port = 0;
                scandir(ep,path,machine);
            }

        } scandir;

        StringBuffer dirlist;
        unsigned j;
        for (j=0;j<numdirs;j++) {
            const char *basedir = dirbaselist[j];
            if (!basedir||!*basedir)
                continue;
            if (dirlist.length())
                dirlist.append(';');
            dirlist.append(basedir);
        }
        unsigned numfails = 0;
        for (unsigned i=0;i<g->ordinality();i++) {
            INode &node = g->queryNode(i);
            scandir.pathsepchar = getPathSepChar(dirlist.str());
            unsigned short port = getDafsPort(node.endpoint(),numfails,NULL);
            Owned<IPropertyTree> results = getDirectory(dirlist.str(),&node,port);
            scandir.scan(results);

        }
    }


    CriticalSection xrefsect;


    void addn(StringBuffer &b,unsigned p,unsigned &firstp,unsigned &lastp)
    {
        if (p) {
            if (firstp==0) {
                firstp = p;
                lastp = p;
            }
            else if (lastp+1==p) 
                lastp = p;
            else {
                addn(b,0,firstp,lastp);
                firstp = p;
                lastp = p;
            }
        }
        else if (firstp!=0) {
            if (b.length())
                b.append(',');
            b.append(firstp);
            if (lastp!=firstp) {
                b.append('-');
                b.append(lastp);
            }
            firstp = 0;
        }
    }

    const char *plural(const char *s)
    {
        if ((strchr(s,'-')!=NULL)||(strchr(s,',')!=NULL))
            return "s";
        else return "";
    }



    void outputTextReport(const char *filename)
    {
        Owned<IFile> file = createIFile(filename);
        Owned<IFileIO> fileio = file->open(IFOcreate);
        if (!fileio) {
            printf("ERROR cannot create %s\n",filename);
            return;
        }
        outfileio.setown(createIOStream(fileio));       SuperHashIteratorOf<CFileEntry> fileiter(filemap);
        if (errors.ordinality()) {
            outf("\n--------------------------------------------------------\nERRORS\n");
            ForEachItemIn(i,errors) {
                cMessage &item = errors.item(i);
                outf("%s: %s\n",item.lname.get(),item.msg.get());
            }
        }
        if (warnings.ordinality()) {
            outf("\n--------------------------------------------------------\nWARNINGS:\n");
            ForEachItemIn(i,warnings) {
                cMessage &item = warnings.item(i);
                outf("%s: %s\n",item.lname.get(),item.msg.get());
            }
        }

        {
            outf("\n--------------------------------------------------------\nUNRESOLVED PRIMARY PARTS:\n");
            ForEach(fileiter) {
                CFileEntry &item = fileiter.query();
                if (!item.resolved()&&!item.replicate()) {
                    StringBuffer s1;
                    item.getLogicalName(s1);
                    outf("%s (%s)\n",s1.str(),item.fname.get());
                }
            }
        }
        {
            outf("\n--------------------------------------------------------\nUNRESOLVED REPLICATE PARTS:\n");
            ForEach(fileiter) {
                CFileEntry &item = fileiter.query();
                if (!item.resolved()&&item.replicate()) {
                    StringBuffer s1;
                    item.getLogicalName(s1);
                    outf("%s (%s)\n",s1.str(),item.fname.get());
                }
            }
            
        }
        outf("\n--------------------------------------------------------\nMISPLACED PARTS:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (!item.complete()&&item.misplaced) {
                    if (item.max>0)
                        outf("%s %s (found %d, size %"I64F"d, modified %s)\n",item.fname.get(),item.misplaced->lname.get(),item.num(),item.size,item.modified.get());
                    else
                        outf("%s (size %"I64F"d, modified %s)\n",item.fname.get(),item.size,item.modified.get());
                    StringBuffer s1;
                    item.getEps(s1,false);
                    outf("        primary: %s\n",s1.str());
                    item.getEps(s1.clear(),true);
                    outf("        replicate: %s\n",s1.str());
                }
            }
        }
        outf("\n--------------------------------------------------------\nORPHANS PARTIAL:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (!item.complete()&&!item.misplaced) {
                    if (item.max>0)
                        outf("%s (found %d, size %"I64F"d, modified %s)\n",item.fname.get(),item.num(),item.size,item.modified.get());
                    else
                        outf("%s (size %"I64F"d, modified %s)\n",item.fname.get(),item.size,item.modified.get());
                    StringBuffer s1;
                    item.getEps(s1,false);
                    outf("        primary: %s\n",s1.str());
                    item.getEps(s1.clear(),true);
                    outf("        replicate: %s\n",s1.str());
                }
            }
        }
        outf("\n--------------------------------------------------------\nSINGLETON ORPHANS:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (item.complete()) {
                    StringBuffer sname;
                    if (item.isSingleton()) {
                        if (item.singletonName(sname,false))
                            outf("%s (size %"I64F"d)\n",sname.str(),item.size);
                        if (item.singletonName(sname.clear(),true))
                            outf("%s (size %"I64F"d)\n",sname.str(),item.size);
                    }
                }
            }
        }
        outf("\n--------------------------------------------------------\nORPHANS COMPLETE:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (item.complete()) {
                    StringBuffer sname;
                    if (!item.isSingleton()) {
                        outf("%s %s(size %"I64F"d)\n",item.fname.get(),item.misplaced?item.misplaced->lname.get():"",item.size);
                        StringBuffer s1;
                        item.getEps(s1,false);
                        outf("        primary: %s\n",s1.str());
                        item.getEps(s1.clear(),true);
                        outf("        replicate: %s\n",s1.str());
                    }
                }
            }
        }
        outf("\n--------------------------------------------------------\nOUTSIDE DIRECTORIES:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&item.outsidedir) {
                    outf("%s (directory %s, %d of %d)\n",item.lname.get(),item.errdir.str(),item.outsidedir,item.max);
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nOUTSIDE CLUSTERS:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&item.outsidenodes.ordinality()) {
                    outf("%s (%d of %d)\n",item.lname.get(),item.outsidenodes.ordinality(),item.max);
                    SocketListCreator cr;
                    cr.addSockets(item.outsidenodes);
                    outf("      %s\n",cr.getText());
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nINCOMPLETE FILES:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&item.incomplete()) {
                    outf("%s (found %d of %d",item.lname.get(),item.primarynum,item.max);
                    unsigned m=item.max-item.primarynum;
                    if (m<=10) {
                        StringBuffer unres;
                        for (unsigned j=0;j<item.max;j++) {
                            if (!item.primaryresolved[j]&&!item.replicateresolved[j]) {
                                if (unres.length()!=0)
                                    unres.append(',');
                                unres.append(j+1);
                            }
                        }
                        outf(" missing parts: %s",unres.str());
                    }
                    outf(")\n");
                    item.done = true;
                }
            }
        }
#ifdef PARTS_SIZE_NEEDED
        outf("\n--------------------------------------------------------\nMISMATCHED SIZES:\n");
        {
            if (fixSizes)
                fixSizeInDFS();

            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&item.nummismatchedsizes) {
                    outf("%s (%d of %d)\n",item.lname.get(),item.nummismatchedsizes,item.max);
                }
            }
        }
#endif
        outf("\n--------------------------------------------------------\nPRIMARY MISSING:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&(item.primarynum!=item.max)) {
                    outf("%s (found %d of %d",item.lname.get(),item.primarynum,item.max);
                    unsigned m=item.max-item.primarynum;
                    if (m<=10) {
                        StringBuffer unres;
                        unsigned j0a;
                        for (j0a=0;j0a<item.max;j0a++) {
                            if (!item.primaryresolved[j0a]&&item.replicateresolved[j0a]) {
                                if (unres.length()!=0)
                                    unres.append(',');
                                unres.append(j0a+1);
                            }
                        }
                        outf(" missing parts: %s",unres.str());
                    }
                    outf(")\n");
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nSECONDARY MISSING:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done&&(item.replicatenum!=item.max)) {
                    outf("%s (found %d of %d",item.lname.get(),item.replicatenum,item.max);
                    unsigned m=item.max-item.primarynum;
                    if (m<=10) {
                        StringBuffer unres;
                        unsigned j0a;
                        for (j0a=0;j0a<item.max;j0a++) {
                            if (item.primaryresolved[j0a]&&!item.replicateresolved[j0a]) {
                                if (unres.length()!=0)
                                    unres.append(',');
                                unres.append(j0a+1);
                            }
                        }
                        outf(" missing parts: %s",unres.str());
                    }
                    outf(")\n");
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nCROSSLINKED:\n");
        {
            ForEachItemIn(i1,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i1);
                if (!item.done&&item.crosslinked.ordinality()) {
                    StringBuffer to;
                    ForEachItemIn(cli1,item.crosslinked) {
                        if (cli1)
                            to.append(", ");
                        to.append(item.crosslinked.item(cli1).lname.get());
                    }
                    outf("%s to %s\n",item.lname.get(),to.str());
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nGROUPS:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (item.unknowngrp||item.mismatchgrp.get()||item.missinggrp) {
                    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(item.lname.get(),UNKNOWN_USER);
                    if (file) {
                        if (item.missinggrp)
                            outf("WARNING: Missing group for %s\n",item.lname.get());
                        else if (item.mismatchgrp.get()) {
                            // ** TBD check queryClusterName correct in following
                            StringBuffer tmp;
                            outf("ERROR: Group mismatch for %s, Group says %s but nodes match %s\n",item.lname.get(),file->getClusterName(0,tmp).str(),item.mismatchgrp.get());
                        }
                        else if (item.unknowngrp) {
                            // ** TBD check queryClusterName correct in following
                            StringBuffer tmp;
                            outf("WARNING: Unknown group %s for %s\n",file->getClusterName(0,tmp).str(),item.lname.get());
                        }
                    }
                }
            }
        }
        {
            outf("\n--------------------------------------------------------\nDIRECTORIES:\n");
            ForEachItemIn(i,dirlist) {
                CDfuDirEntry &item = dirlist.item(i);
                if (item.minsize<0)
                    item.minsize = 0;
                StringBuffer s1;
                if (!item.minip.isNull())
                    item.minip.getIpText(s1);
                StringBuffer s2;
                if (!item.maxip.isNull())
                    item.maxip.getIpText(s2);
                StringBuffer skew;
                item.getskew(skew);
                outf("%s numfiles=%u totalsize=%"CF"d minsize=%"CF"d(%s) maxsize=%"CF"d(%s), skew=%s\n",item.name.get(),item.num,item.size,
                    item.minsize,s1.str(),item.maxsize,s2.str(),skew.str());
            }
        }
        outf("\n--------------------------------------------------------\nOK:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.done) {
                    outf("%s\n",item.lname.get());
                }
            }
        }
        outf("TOTAL ORPHANS: %d files %"I64F"d bytes\n",totalNumOrphans,totalSizeOrphans);
        outf("Row Compression: %d files %"I64F"d compressed %"I64F"d uncompressed\n",totalCompressed,totalCompressedSize, totalUncompressedSize);
        outfileio.clear();
    }


    void outputCsvReport(const char *filename)
    {
        // more needs doing to make 'proper' csv
        Owned<IFile> file = createIFile(filename);
        Owned<IFileIO> fileio = file->open(IFOcreate);
        if (!fileio) {
            printf("ERROR cannot create %s\n",filename);
            return;
        }
        outfileio.setown(createIOStream(fileio));
        outf("===============================================================\n");
        outf("\n--------------------------------------------------------\nORPHANS PARTIAL:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if ((!item.complete())&&!item.misplaced) {
                    outf("%s,%d,%"I64F"d,%s\n",item.fname.get(),item.num(),item.size,item.modified.get());
                }
            }
        }
        outf("\n--------------------------------------------------------\nORPHANS PARTIAL FILES:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (!item.complete()) {
                    SocketEndpoint nullep;
                    ForEachItemIn(j1,item.primaryepa) {
                        SocketEndpoint ep=item.primaryepa.item(j1);
                        unsigned pn=item.primarypna.item(j1);
                        if (!ep.equals(nullep)) {
                            StringBuffer partname;
                            constructPartname(item.fname.get(),pn+1,partname,false);
                            RemoteFilename rfn;
                            rfn.setPath(ep,partname.str());
                            rfn.getRemotePath(partname.clear());
                            outf("%s,%s\n",partname.str(),item.misplaced?item.misplaced->lname.get():"");
                        }
                    }
                    ForEachItemIn(j2,item.replicateepa) {
                        SocketEndpoint ep=item.replicateepa.item(j2);
                        unsigned pn=item.replicatepna.item(j2);
                        if (!ep.equals(nullep)) {
                            StringBuffer partname;
                            constructPartname(item.fname.get(),pn+1,partname,true);
                            RemoteFilename rfn;
                            rfn.setPath(ep,partname.str());
                            rfn.getRemotePath(partname.clear());
                            outf("%s,%s\n",partname.str(),item.misplaced?item.misplaced->lname.get():"");
                        }
                    }
                }
            }
        }
        outf("\n--------------------------------------------------------\nORPHANS COMPLETE:\n");
        {
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (item.complete()&&!item.isSingleton()) {
                    outf("%s,%d,%"I64F"d,%s %s\n",item.fname.get(),item.num(),item.size,item.modified.get(),item.misplaced?item.misplaced->lname.get():"");
                }
            }
        }
        outf("\n--------------------------------------------------------\nINCOMPLETE FILES:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                item.done = false;
                bool incomplete = false;
                unsigned j0a;
                for (j0a=0;j0a<item.max;j0a++) 
                    if (!item.primaryresolved[j0a]&&!item.replicateresolved[j0a]) 
                        incomplete = true;
                if (incomplete&&(!item.outsidedir)&&(item.outsidenodes.ordinality()==0)&&(item.primarynum!=item.max)) {
                    outf("%s,%d,%d,%d,%"I64F"d\n",item.lname.get(),item.primarynum,item.replicatenum,item.max,item.totalsize);
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nPRIMARY MISSING:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.outsidedir&&(item.outsidenodes.ordinality()==0)&&(item.primarynum!=item.max)) {
                    outf("%s,%d,%d,%"I64F"d\n",item.lname.get(),item.primarynum,item.max,item.totalsize);
                    item.done = true;
                }
            }
        }
        outf("\n--------------------------------------------------------\nSECONDARY MISSING:\n");
        {
            ForEachItemIn(i,logicalnamelist) {
                CLogicalNameEntry &item = logicalnamelist.item(i);
                if (!item.outsidedir&&(item.outsidenodes.ordinality()==0)&&(item.replicatenum!=item.max)) {
                    outf("%s,%d,%d,%"I64F"d\n",item.lname.get(),item.replicatenum,item.max,item.totalsize);
                    item.done = true;
                }
            }
        }
        outfileio.clear();
    }

    void outputBackupReport()
    {
        ForEachItemIn(i,logicalnamelist) {
            CLogicalNameEntry &item = logicalnamelist.item(i);
            if ((!item.outsidedir)&&(item.outsidenodes.ordinality()==0)) {
                if ((item.primarynum!=item.max)||(item.replicatenum!=item.max)) {
                    StringBuffer cdrv;
                    StringBuffer ddrv;
                    StringBuffer missing;
                    unsigned firstc=0;
                    unsigned firstd=0;
                    unsigned firstm=0;
                    unsigned lastc=0;
                    unsigned lastd=0;
                    unsigned lastm=0;
                    unsigned j;
                    for (j=0;j<item.max;j++) {
                        if (!item.primaryresolved[j]) {
                            if (!item.replicateresolved[j]) {
                                addn(missing,j+1,firstm,lastm);
                            }
                            else {
                                addn(cdrv,j+1,firstc,lastc);
                            }
                        }
                        else if (!item.replicateresolved[j]) {
                            addn(ddrv,j+1,firstd,lastd);
                        }
                    }
                    addn(cdrv,0,firstc,lastc);
                    addn(ddrv,0,firstd,lastd);
                    addn(missing,0,firstm,lastm);
                    printf("%s\n",item.lname.get());
                    if (cdrv.length()) 
                        printf("        C: missing part%s %s\n",plural(cdrv.str()),cdrv.str());
                    if (ddrv.length()) 
                        printf("        D: missing part%s %s\n",plural(ddrv.str()),ddrv.str());
                    if (missing.length()) 
                        printf("        C: and D: missing part%s %s\n",plural(missing.str()),missing.str());
                    printf("\n");
                }
            }
        }
    }

    static int compareDirectory(CInterface **le, CInterface **re)
    {
        const CDfuDirEntry *l = (const CDfuDirEntry *)*le;
        const CDfuDirEntry *r = (const CDfuDirEntry *)*re;
        __int64 dif = l->size-r->size;
        if (dif<0)
            return 1;
        if (dif>0)
            return -1;
        return stricmp(l->name.get(),r->name.get());
    }

    IPropertyTree * outputTree()
    {
        log("Collating output");
        IPropertyTree *out = createPTree("XREF");
        IPropertyTree *orphans = addBranch(out,"Orphans");
        IPropertyTree *found = addBranch(out,"Found");
        IPropertyTree *lost = addBranch(out,"Lost");
        IPropertyTree *message = addBranch(out,"Messages");
        IPropertyTree *directories = addBranch(out,"Directories");
        
        // Lost
        {
            DBGLOG("// Lost");
            ForEachItemIn(i,logicalnamelist)
            {
                CLogicalNameEntry &item = logicalnamelist.item(i);
            
                if ((!item.outsidedir)&&(item.outsidenodes.ordinality()==0)) {

                    if (item.incomplete()) { // need check for if replicated here
                        item.addFileBranch(lost,CFBname|CFBcluster|CFBsize|CFBnumparts|CFBpartslost|CFBprimarylost|CFBreplicatedlost|CFBmodified|CFBpartnode|CFBpartnum|CFBpartprimary|CFBpartreplicate);
                        item.done = true;
                    }
                }
            }
        }
        // Found
        {
            DBGLOG("// Found");
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (item.complete()) {
                    item.addFileBranch(found,CFBpartnode|CFBpartnum|CFBpartreplicate|CFBpartprimary|CFBnumparts|CFBcluster|CFBmodified|CFBsize);
                }
            }
        }
        // Orphans
        {
            DBGLOG("// Orphans");
            ForEachItemIn(i,orphanlist) {
                COrphanEntry &item = orphanlist.item(i);
                if (!item.complete()) {
                    item.addFileBranch(orphans,CFBpartnode|CFBpartnum|CFBpartprimary|CFBnumparts|CFBmodified|CFBsize|CFBpartnode|CFBpartnum);
                    item.addFileBranch(orphans,CFBpartnode|CFBpartnum|CFBpartreplicate|CFBnumparts|CFBmodified|CFBsize|CFBpartnode|CFBpartnum);
                }
            }
        }
        // Messages
        {
            DBGLOG("// Messages");
            {
                ForEachItemIn(i,errors) {
                    cMessage &item = errors.item(i);
                    IPropertyTree *t = addBranch(message,"Error");
                    t->addProp("File",item.lname.get());
                    t->addProp("Text",item.msg.get());
                }
            }
            {
                ForEachItemIn(i,warnings) {
                    cMessage &item = warnings.item(i);
                    IPropertyTree *t = addBranch(message,"Warning");
                    t->addProp("File",item.lname.get());
                    t->addProp("Text",item.msg.get());
                }
            }
        }
        // Directories
        {
            dirlist.sort(compareDirectory);
            DBGLOG("// Directories");
            {
                ForEachItemIn(i,dirlist) {
                    CDfuDirEntry &item = dirlist.item(i);
                    if (item.minsize<0)
                        item.minsize = 0;
                    if ((item.num==0)&&(item.size==0)&&(item.numdir>0)) // exclude intermediate empty dirs
                        continue;
                    IPropertyTree *t = addBranch(directories,"Directory");
                    t->addProp("Name",item.name.get());
                    t->addPropInt("Num",item.num);
                    t->addPropInt64("Size",item.size);
                    if (item.size) {
                        t->addPropInt64("MaxSize",item.maxsize);
                        StringBuffer s1;
                        if (!item.maxip.isNull())
                            item.maxip.getIpText(s1);
                        t->addProp("MaxIP",s1.str());
                        t->addPropInt64("MinSize",item.minsize);
                        s1.clear();
                        if (!item.minip.isNull())
                            item.minip.getIpText(s1);
                        t->addProp("MinIP",s1.str());
                        item.getskew(s1.clear());
                        if (s1.length())
                            t->addProp("Skew",s1.str());
                    }
                }
            }
        }


        return out;
    }



public:
    IPropertyTree *process(unsigned nclusters,const char **clusters,unsigned numdirs,const char **dirbaselist,unsigned flags,IXRefProgressCallback *_msgcallback,unsigned numthreads)
    {

        CriticalBlock block(xrefsect);
        msgcallback.set(_msgcallback);

        IPropertyTree *out=NULL;
        
        Owned<IGroup> g;
        unsigned j;
        if (!nclusters) {
            error("XREF","No clusters specified\n");
            return NULL;
        }
        if (!numdirs) {
            error("XREF","No directories specified\n");
            return NULL;
        }
        for (j=0;j<nclusters;j++) {
            Owned<IGroup> gsub = queryNamedGroupStore().lookup(clusters[j]);
            if (!gsub) {
                error(clusters[j],"Could not find cluster group");
                return NULL;
            }
            if (!g)
                g.set(gsub.get());
            else
                g.setown(g->combine(gsub.get()));
        }
        totalSizeOrphans =0;
        totalNumOrphans = 0;


        logicalnamelist.kill();
        dirlist.kill();
        orphanlist.kill();
        
        const char* cluster = clusters[0];
        loadFromDFS(*this,g,numdirs,dirbaselist,cluster);

        xrefRemoteDirectories(g,numdirs,dirbaselist,numthreads);
        StringBuffer filename;
        filename.clear().append("xrefrpt");
        addFileTimestamp(filename, true);
        filename.append(".txt");
        
        if (flags&PMtextoutput) 
            outputTextReport(filename.str());
        filename.clear().append("xrefrpt");
        addFileTimestamp(filename, true);
        filename.append(".txt");

        if (flags&PMcsvoutput) 
            outputCsvReport(filename.str());

        if (flags&PMbackupoutput) 
            outputBackupReport();

        if (flags&PMtreeoutput) 
            out = outputTree(); 

        logicalnamemap.kill();
        filemap.kill();
        orphanmap.kill();
        dirmap.kill();
        logicaldirmap.kill();

        log("Complete");
        DBGLOG("Finished...");
        return out;
    }

};


IPropertyTree *  runXRef(unsigned nclusters,const char **clusters,IXRefProgressCallback *callback,unsigned numthreads)
{
    if (nclusters==0)
        return NULL;
    CXRefManager xrefmanager;
    const char *dirs[2];
    unsigned numdirs;
#ifdef _WIN32
    bool islinux = false;
#else
    bool islinux = true;
#endif
    // assume all nodes same OS
    Owned<IGroup> group = queryNamedGroupStore().lookup(clusters[0]);
    if (group)
        islinux = queryOS(group->queryNode(0).endpoint())==MachineOsLinux;
    dirs[0] = queryBaseDirectory(grp_unknown, 0,islinux?DFD_OSunix:DFD_OSwindows);  // MORE - should use the info from the group store
    dirs[1] = queryBaseDirectory(grp_unknown, 1,islinux?DFD_OSunix:DFD_OSwindows);
    numdirs = 2;
    IPropertyTree *ret=NULL;
    try {
        ret = xrefmanager.process(nclusters,clusters,numdirs,dirs,PMtreeoutput,callback,numthreads);
    }
    catch (IException *e) {
        StringBuffer s;
        e->errorMessage(s);
        if (callback)
            callback->error(s.str());
        else
            ERRLOG("%s",s.str());
    }
    return ret;
}


IPropertyTree * runXRefCluster(const char *cluster,IXRefNode *nodeToUpdate)
{
    DBGLOG("runXRefCluster starting for cluster %s",cluster);
        


    CXRefManager xrefmanager;
    IPropertyTree *ret=NULL;
    IXRefProgressCallback* callback = dynamic_cast<IXRefProgressCallback*>(nodeToUpdate);
    try {
        const char *clusters[1];
        clusters[0] = cluster;
        ret = runXRef(1, clusters,callback,4);  // only single thread for time being
        //    xrefmanager.process(1,clusters,4,dirs,PMtreeoutput,callback);
    }
    catch (IException *e) {
        StringBuffer s;
        e->errorMessage(s);
        if (callback)
            callback->error(s.str());
        else
            ERRLOG("%s",s.str());
    }
    if(ret)
    {
        DBGLOG("runXRefCluster building DFU node for cluster %s",cluster);
        nodeToUpdate->BuildXRefData(*ret,cluster);
        nodeToUpdate->commit();
    }
    return ret;
}


IPropertyTree * RunProcess(unsigned nclusters,const char **clusters,unsigned numdirs,const char **dirbaselist,unsigned flags,IXRefProgressCallback *_msgcallback,unsigned numthreads)
{
    //Provide a wrapper for the command line
    if (flags & PMupdateeclwatch) {
        if (nclusters==1) {
            const char *cluster = *clusters;
            CXRefNodeManager nodemanager;
            Owned<IPropertyTree> tree = runXRef(nclusters,clusters,NULL,numthreads);
            if (tree) {
                Owned<IXRefNode> xRefNode = nodemanager.getXRefNode(cluster);
                if (!xRefNode.get())
                    xRefNode.setown( nodemanager.CreateXRefNode(cluster));
                xRefNode->setCluster(cluster);
                xRefNode->BuildXRefData(*tree.get(),cluster);
                xRefNode->commit();
            }
        }
        else {
            // do clusters 1 at time
            for (unsigned i = 0; i<nclusters; i++)
                RunProcess(1,clusters+i,numdirs,dirbaselist,flags,_msgcallback,numthreads);
        }
        return NULL;
    }
    CXRefManager xrefmanager;
    return xrefmanager.process(nclusters,clusters,numdirs,dirbaselist,flags,_msgcallback,numthreads);
}
