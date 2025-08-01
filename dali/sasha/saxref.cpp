//TBD check min time from when *finished*

#include "platform.h"

#include "jlib.hpp"
#include "jiface.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"
#include "jset.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"

#include "saserver.hpp"
#include "sautil.hpp"
#include "sacoalescer.hpp"
#include "sacmd.hpp"

#define DEFAULT_MAXDIRTHREADS 500
#define DEFAULT_MAXMEMORY 4096

#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define SDS_LOCK_TIMEOUT 300000


#define DEFAULT_XREF_INTERVAL       48 // hours
#define DEFAULT_EXPIRY_INTERVAL     24 // hours

#define DEFAULT_EXPIRYDAYS 14
#define DEFAULT_PERSISTEXPIRYDAYS 7


#define LOGPFX "XREF: "
#define LOGPFX2 "FILEEXPIRY: "

#define DEFAULT_RECENT_CUTOFF_DAYS 1

inline bool nextCsvToken(const char *&s,StringBuffer &tok) 
{
    if (!s)
        return false;
    for (;;) {
        if (!*s)
            return false;
        if (*s!=',')
            break;
        s++;
    }
    const char *e = s;
    for (;;) {
        if (!*e||(*e==','))
            break;
        e++;
    }
    if (e==s)
        return false;
    tok.append(e-s,s);
    s = e;  // leave comma for next time
    return true;
}



struct cMisplacedRec
{
    cMisplacedRec *next;
    unsigned short nn;  // node on (+N*drv)
    unsigned short pn;  // part number 
    bool marked;

    void init(unsigned drv,
              unsigned pf,      // part
              unsigned xn,      // node located on
              unsigned tn)      // total nodes
    {
        nn = (unsigned short)(xn+tn*drv);
        pn = (unsigned short)pf;
        marked = false;
        next = NULL;
    }

    bool eq(unsigned drv,
              unsigned pf,      // part
              unsigned xn,      // node located on
              unsigned tn)      // total nodes
    {
        if (pn != (unsigned short)pf)
            return false;
        return (nn == (unsigned short)(xn+tn*drv));
    }

    unsigned getDrv(unsigned tn)
    {
        return nn/tn;
    }

    unsigned getNode(unsigned tn)
    {
        return nn%tn;
    }
};



struct cFileDesc // no virtuals
{
    unsigned hash;
    unsigned short N;           // num parts
    bool isDirPerPart;          // directory-per-part number present in physical path
    byte filenameLen;           // length of file name excluding extension i.e. ._$P$_of_$N$
    const char *owningfile;     // for crosslinked
    cMisplacedRec *misplaced;   // for files on the wrong node
    byte name[1];               // first byte length
    // char namestr[name[1]]
    // bitset presentc[N];
    // bitset presentd[N];
    // bitset markedc[N];
    // bitset markedd[N];

    static cFileDesc * create(CLargeMemoryAllocator &mem,const char *_name,unsigned n,bool d,unsigned fnLen)
    {
        size32_t sl = strlen(_name); 
        if (sl>255) {
            OWARNLOG(LOGPFX "File name %s longer than 255 chars, truncating",_name);
            sl = 255;
        }
        size32_t ml = (n*4+7)/8;
        size32_t sz = sizeof(cFileDesc)+sl+ml;
        cFileDesc * ret = (cFileDesc *)mem.alloc(sz);
        ret->N = (unsigned short)n;
        ret->name[0] = (byte)sl;
        ret->isDirPerPart = d;
        ret->filenameLen = (byte)fnLen;
        ret->owningfile = NULL;
        ret->misplaced = NULL;

        memcpy(&ret->name[1],_name,sl);
        memset(ret->map(),0,ml);
        ret->hash = hashc((const byte *)_name,sl,17);
        return ret;
    }


    inline byte *map() const
    {
        return (byte *)&name+1+name[0];
    }

    bool setpresent(unsigned d,unsigned i) // returns old value
    {
        if (d)
            i += N*d;
        byte v = 1<<(i%8);
        byte &m = map()[i/8];
        bool ret = (m&v)!=0;
        m |= v;
        return ret;
    }

    bool testpresent(unsigned d,unsigned i)
    {
        if (d)
            i += N*d;
        return (map()[i/8]&(1<<(i%8)))!=0;
    }

    bool setmarked(unsigned d,unsigned i) // returns old value
    {
        return setpresent(d+2,i);
    }

    bool testmarked(unsigned d,unsigned i)
    {
        return testpresent(d+2,i);
    }

    bool eq(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255) 
            sl = 255;
        if (sl!=(byte)name[0])
            return false;
        return memcmp(key,name+1,sl)==0;
    }

    bool isHPCCFile() const
    {
        return filenameLen > 0;
    }

    bool getName(StringBuffer &buf) const
    {
        // Check mask exists
        if (filenameLen == 0)
            return false;
        buf.append((size32_t)filenameLen, (const char *)(name+1));
        return true;
    }

    StringBuffer &getNameMask(StringBuffer &buf) const
    {
        return buf.append((size32_t)name[0],(const char *)(name+1));
    }

    StringBuffer &getPartName(StringBuffer &buf,unsigned p) const
    {
        StringBuffer mask;
        getNameMask(mask);
        return expandMask(buf, mask, p, N);
    }

    static cFileDesc * create(const char *)
    {
        assertex(false);
        return NULL;
        // not used
    }
    
    static unsigned getHash(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255) 
            sl = 255;
        return  hashc((const byte *)key,sl,17);
    }
    

    static void destroy(cFileDesc *)
    {
        // not owning
    }



};


struct cDirDesc
{
    unsigned hash;
    CMinHashTable<cDirDesc> dirs;       
    CMinHashTable<cFileDesc> files; 
    offset_t totalsize[2];              //  across all nodes
    offset_t minsize[2];                //  smallest node size
    offset_t maxsize[2];                //  largest node size
    unsigned short minnode[2];          //  smallest node (1..)
    unsigned short maxnode[2];          //  largest node (1..)




    byte *name;                     // first byte length  NB this is the tail name
    // char namestr[*name]

    cDirDesc(CLargeMemoryAllocator &mem,const char *_name)
    {
        size32_t sl = strlen(_name);
        if (sl>255) {
            OWARNLOG(LOGPFX "Directory name %s longer than 255 chars, truncating",_name);
            sl = 255;
        }
        name = (byte *)mem.alloc(sl+1);
        name[0] = (byte)sl;
        memcpy(name+1,_name,sl);
        hash = hashc((const byte *)_name,sl,17);
        for (unsigned drv=0;drv<2;drv++) {
            totalsize[drv] = 0;
            minnode[drv] = 0;
            minsize[drv] = 0;
            maxnode[drv] = 0;
            maxsize[drv] = 0;
        }
    }
    ~cDirDesc()
    {
        unsigned i;
        cDirDesc *d = dirs.first(i);
        while (d) {
            delete d;
            d = dirs.next(i);
        }
        // don't delete the files (they are from mem)
    }

    bool eq(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255) 
            sl = 255;
        if (sl!=(byte)name[0])
            return false;
        return memcmp(key,name+1,sl)==0;
    }

    static cDirDesc * create(const char *)
    {
        assertex(false);
        return NULL;
        // not used
    }
    
    static unsigned getHash(const char *key)
    {
        size32_t sl = strlen(key);
        if (sl>255) 
            sl = 255;
        return  hashc((const byte *)key,sl,17);
    }
    

    static void destroy(cDirDesc *)
    {
        // not owning
    }

    StringBuffer &getName(StringBuffer &buf)
    {
        return buf.append((size32_t)name[0],(const char *)name+1);
    }

    cDirDesc *lookupDir(const char *name,CLargeMemoryAllocator *mem)
    { 
        cDirDesc *ret = dirs.find(name,false);
        if (!ret&&mem) {
            ret = new cDirDesc(*mem,name);
            dirs.add(ret);
        }
        return ret;
    }

    const char *decodeName(unsigned drv,const char *name,unsigned node, unsigned numnodes,
                    StringAttr &mask,       // decoded mask
                    unsigned &pf,           // part node
                    unsigned &nf,           // num parts
                    unsigned &filenameLen)  // length of file name excluding extension i.e. ._$P$_of_$N$
    {
        const char *fn = name;
        // first see if tail fits a mask
        if (deduceMask(fn, true, mask, pf, nf, filenameLen))
            fn = mask.get();
        else {  // didn't match mask so use straight name
            //PROGLOG("**unmatched(%d,%d,%d) %s",drv,node,numnodes,name);
            pf = (node+numnodes-drv)%numnodes;
            nf = NotFound;
        }
        return fn;
    }

    bool isMisplaced(unsigned partNum, unsigned numParts, const SocketEndpoint &ep, IGroup &grp)
    {
        // MORE: Add better check for misplaced in Containerized
        // If plane being scanned is host based (i.e. not locally mounted), misplaced could still make sense
        if (isContainerized())
            return false;

        return numParts!=grp.ordinality() || partNum>=grp.ordinality() || !grp.queryNode(partNum).endpoint().equals(ep);
    }

    cFileDesc *addFile(unsigned drv,const char *name,__int64 sz,CDateTime &dt,unsigned node, const SocketEndpoint &ep, IGroup &grp, unsigned numnodes, CLargeMemoryAllocator *mem)
    {

        unsigned nf;          // num parts
        unsigned pf;          // part num
        unsigned filenameLen; // length of file name excluding extension i.e. ._$P$_of_$N$
        StringAttr mask;
        const char *fn = decodeName(drv,name,node,numnodes,mask,pf,nf,filenameLen);
        bool misplaced = isMisplaced(pf, nf, ep, grp);
        cFileDesc *file = files.find(fn,false);
        if (!file) {
            if (!mem)
                return NULL;
            // dirPerPart is set to false during scanDirectories, and later updated in listOrphans by mergeDirPerPartDirs
            file = cFileDesc::create(*mem,fn,nf,false,filenameLen);
            files.add(file);
        }
        if (misplaced) {
            cMisplacedRec *mp = file->misplaced;
            while (mp) {
                if (mp->eq(drv,pf,node,numnodes)) {
                    OERRLOG(LOGPFX "Duplicate file with mismatched tail (%d,%d) %s",pf,node,name);
                    return NULL;
                }
                mp = mp->next;
            }
            if (!mem)
                return NULL;
            mp = (cMisplacedRec *)mem->alloc(sizeof(cMisplacedRec));
            mp->init(drv,pf,node,numnodes);
            mp->next = file->misplaced;
            file->misplaced = mp;
            // NB: still perform setpresent() below, so that later 'orphan' and 'found' scanning can spot the part as orphaned or part of a found file.
        }
        if (file->setpresent(drv,pf)) {
            OERRLOG(LOGPFX "Duplicate file with mismatched tail (%d) %s",pf,name);
            file = NULL;
        }
        return file;
    }

    bool markFile(unsigned drv,const char *name, unsigned node, const SocketEndpoint &ep, IGroup &grp, unsigned numnodes)
    {
        unsigned nf;
        unsigned pf;
        unsigned filenameLen;
        StringAttr mask;
        const char *fn = decodeName(drv,name,node,numnodes,mask,pf,nf,filenameLen);
        // TODO: Add better check for misplaced in isContainerized
        // If plane being scanned is host based (i.e. not locally mounted), misplaced could still make sense
        bool misplaced = !isContainerized() && (nf!=grp.ordinality() || pf>=grp.ordinality() || !grp.queryNode(pf).endpoint().equals(ep));
        cFileDesc *file = files.find(fn,false);
        if (file) {
            if (misplaced) {
                cMisplacedRec *mp = file->misplaced;
                while (mp) {
                    if (mp->eq(drv,pf,node,numnodes)) {
                        mp->marked = true;
                        return true;
                    }
                    mp = mp->next;
                }
            }
            else if (file->testpresent(drv,pf)) {
                file->setmarked(drv,pf);
                return true;
            }
        }
        return false;
    }

    void addNodeStats(unsigned node,unsigned drv,offset_t sz)
    {
        if (drv>1)
            drv = 1;
        totalsize[drv] += sz;
        if (!minnode[drv]||(minsize[drv]>sz)) {
            minnode[drv] = node+1;
            minsize[drv] = sz;
        }
        if (!maxnode[drv]||(maxsize[drv]<sz)) {
            maxnode[drv] = node+1;
            maxsize[drv] = sz;
        }
    }

    bool empty(unsigned drv)
    {
        // empty if no files, and all subdirs are empty
        if ((files.ordinality()!=0)||(totalsize[drv]!=0))
            return false;
        if (dirs.ordinality()==0)
            return true;
        unsigned i;
        cDirDesc *sd = dirs.first(i);
        while (sd) {
            if (sd->empty(drv))
                return false;
            sd = dirs.next(i);
        }
        return true;
    }

};


struct cMessage: public CInterface
{
    StringAttr lname;
    StringAttr msg;
    cMessage(const char *_lname,const char *_msg)
        : lname(_lname), msg(_msg)
    {
    }
};


// Parses cDirDesc name to determine if it is a dir-per-part directory
// Returns the name converted to a number if possible, otherwise 0
static unsigned getDirPerPartNum(cDirDesc *dir)
{
    StringBuffer dirName;
    dir->getName(dirName);
    const char *name = dirName.str();
    unsigned num = 0;
    while (*name)
    {
        if (isdigit(*name))
            num = num * 10 + (*name - '0');
        else
            return 0;
        name++;
    }
    return num;
}

// A found file that has a dir-per-part directory will have multiple cFileDesc entries in each of the dir-per-part
// cDirDescs. For found files, we do not know if it is a dir-per-part file since there is no metadata. We only merge
// cFileDescs where only a single file was marked present, and we find matching files in the dir-per-part directories.
static void mergeDirPerPartDirs(cDirDesc *parent, cDirDesc *dir, const char *currentPath, CLargeMemoryAllocator *mem)
{
    if (!isContainerized())
        return;
    if (dir->files.ordinality() == 0 || dir->dirs.ordinality() != 0)
        return;

    // Check if dir name is a number
    unsigned dirPerPartNum = getDirPerPartNum(dir);
    if (dirPerPartNum == 0)
        return;

    unsigned i = 0;
    cFileDesc *file = dir->files.first(i);
    while (file)
    {
        // If this is a dir-per-part directory, the dirPerPartNum cannot be larger than the number of file parts,
        // and there should be enough subdirectories under the parent directory for each file part
        if (dirPerPartNum <= file->N && file->N <= parent->dirs.ordinality())
        {
            // A dir-per-part file will have only the part matching the dir name marked present
            // If more than one file is marked present, it is not a dir-per-part file
            unsigned present = 0;
            for (unsigned j=0;j<file->N;j++)
            {
                if (file->testpresent(0, j))
                    present++;
            }

            // Avoid merging if multiple parts are marked present in a single directory
            if (present == 1)
            {
                StringBuffer fname;
                file->getNameMask(fname);

                cFileDesc *movedFile = nullptr;
                for (unsigned k=0;k<file->N;k++)
                {
                    cDirDesc *dirPerPartDir = parent->dirs.find(std::to_string(k+1).c_str(), false);
                    if (dirPerPartDir)
                    {
                        cFileDesc *dirPerPartFile = dirPerPartDir->files.find(fname,false);
                        if (dirPerPartFile)
                        {
                            if (movedFile == nullptr)
                            {
                                parent->files.add(dirPerPartFile);
                                movedFile = dirPerPartFile;
                                movedFile->isDirPerPart = true;
                            }
                            else
                            {
                                movedFile->setpresent(0, k);
                                if (dirPerPartFile->testmarked(0, k))
                                    movedFile->setmarked(0, k);
                            }
                            dirPerPartDir->files.remove(dirPerPartFile);
                        }
                    }
                }
            }
        }

        file = dir->files.next(i);
    }
}


class CNewXRefManagerBase
{
public:
    CriticalSection logsect;
    Owned<IRemoteConnection> logconn;
    StringAttr logcache;
    StringAttr clustname;
    CIArrayOf<cMessage> errors;
    CIArrayOf<cMessage> warnings;
    StringAttr rootdir;
    unsigned lastlog;
    unsigned sfnum;
    unsigned fnum;
    Owned<IPropertyTree> foundbranch;
    Owned<IPropertyTree> lostbranch;
    Owned<IPropertyTree> orphansbranch;
    Owned<IPropertyTree> dirbranch;

    void log(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (clustname.get())
            PROGLOG(LOGPFX "[%s] %s",clustname.get(),line.str());
        else
            PROGLOG(LOGPFX "%s",line.str());
        if (logconn) {
            logcache.set(line.str());
            updateStatus(false);
        }
    }

    void statlog(const char * format, ...) __attribute__((format(printf, 2, 3)))
    {
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        if (logconn) {
            logcache.set(line.str());
            updateStatus(false);
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

        OERRLOG("%s: %s",lname,line.str());
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
        OWARNLOG("%s: %s",lname,line.str());
    }


    void updateStatus(bool uncond)
    {
        CriticalBlock block(logsect);

        if (logcache.length()&&logconn) {
            if (uncond||(msTick()-lastlog>10000))  {
                logconn->queryRoot()->setProp("@status",logcache.get());
                logconn->commit();
                lastlog = msTick();
            }
        }

    }

    void addBranch(IPropertyTree *root,const char *name,IPropertyTree *branch)
    {
        if (!branch)
            return;
        branch->setProp("Cluster",clustname);
        StringBuffer datastr;
        toXML(branch,datastr);
        root->addPropTree(name,createPTree(name))->setPropBin("data",datastr.length(),datastr.str());
    }

    CNewXRefManagerBase()
    {
        lastlog = 0;
        sfnum = 0;
        fnum = 0;
    }

    void start(bool updateeclwatch,const char *clname)
    {
        StringBuffer xpath;
        {   // remove old tree
            Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
            if (!conn)
                return;
            IPropertyTree *xrefroot = conn->queryRoot();
            xpath.appendf("Cluster[@name=\"%s\"]", clname);

        }
        if (updateeclwatch) {
            xpath.insert(0,"/DFU/XREF/");
            logconn.setown(querySDS().connect(xpath.str(),myProcessSession(),0 ,INFINITE));
        }
        log("Starting");
    }

    void finish(bool aborted)
    {
        if (aborted)
            log("Aborted");
        logconn.clear(); // final message done by save to eclwatch
    }


    void addErrorsWarnings(IPropertyTree *croot)
    {
        Owned<IPropertyTree> message = createPTree("Messages");
        ForEachItemIn(i1,errors) {
            cMessage &item = errors.item(i1);
            IPropertyTree *t = message->addPropTree("Error",createPTree("Error"));
            t->addProp("File",item.lname.get());
            t->addProp("Text",item.msg.get());
        }
        ForEachItemIn(i2,warnings) {
            cMessage &item = warnings.item(i2);
            IPropertyTree *t = message->addPropTree("Warning",createPTree("Warning"));
            t->addProp("File",item.lname.get());
            t->addProp("Text",item.msg.get());
        }
        addBranch(croot,"Messages",message);
    }



    void saveToEclWatch(bool &abort,bool byscheduler) 
    {
        if (abort)
            return;
        log("Saving information");
        Owned<IPropertyTree> croot = createPTree("Cluster");
        croot->setProp("@name",clustname);
        if (!rootdir.isEmpty()) 
            croot->setProp("@rootdir",rootdir);
        CDateTime dt;
        dt.setNow();
        StringBuffer dts;
        dt.getString(dts);
        croot->setProp("@modified",dts.str());
        StringBuffer ss("Generated");
        if (byscheduler)
            ss.append(" by sasha scheduler");
        if (sfnum)
            ss.appendf("  [%d superfiles, %d subfiles]",sfnum,fnum);
        else if (fnum)
            ss.appendf("  [%d files]",fnum);
        croot->setProp("@status",ss.str());
        addBranch(croot,"Orphans",orphansbranch);
        addBranch(croot,"Lost",lostbranch);
        addBranch(croot,"Found",foundbranch);
        addBranch(croot,"Directories",dirbranch);
        addErrorsWarnings(croot);
        if (abort)
            return;
        logconn.clear();
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
        if (abort)
            return;
        IPropertyTree *xrefroot = conn->queryRoot();
        StringBuffer xpath;
        xpath.appendf("Cluster[@name=\"%s\"]", clustname.get());
        xrefroot->removeProp(xpath.str());
        xrefroot->addPropTree("Cluster",croot.getClear());
    }

};


class CNewXRefManager: public CNewXRefManagerBase
{
    cDirDesc *root;     
    CriticalSection crit;
    bool iswin;                     // set by scanDirectories
    IpAddress *iphash;
    unsigned *ipnum;
    unsigned iphashsz;
    IArrayOf<IPropertyTree> sorteddirs;

public:
    Owned<IGroup> grp, rawgrp;
    StringArray clusters;           // list of matching cluster (used in xref)
    StringBuffer clusterscsl;       // comma separated list of cluster (used in xref)
    unsigned numnodes;
    StringArray lostfiles;
    CLargeMemoryAllocator mem;
    bool verbose;
    unsigned numuniqnodes = 0;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> storagePlane;
    bool isPlaneStriped = false;
    unsigned numStripedDevices = 1;

    CNewXRefManager(IPropertyTree *plane,unsigned maxMb=DEFAULT_MAXMEMORY)
        : mem(0x100000*((memsize_t)maxMb),0x10000,true)
    {
        iswin = false; // set later
        root = new cDirDesc(mem,"");
        verbose = true;
        iphash = NULL;
        ipnum = NULL;
        foundbranch.setown(createPTree("Found"));
        lostbranch.setown(createPTree("Lost"));
        orphansbranch.setown(createPTree("Orphans"));
        dirbranch.setown(createPTree("Directories"));
        log("Max memory = %d MB", maxMb);

        StringBuffer userName;
        serverConfig->getProp("@sashaUser", userName);
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);

        if (plane)
        {
            storagePlane.set(plane);
            unsigned numDevices = storagePlane->getPropInt("@numDevices", 1);
            isPlaneStriped = !storagePlane->hasProp("@hostGroup") && (numDevices>1);
            numStripedDevices = isPlaneStriped ? numDevices : 1;
        }
    }

    ~CNewXRefManager()
    {
        delete root;
        if (iphash) 
            delete [] iphash;
        delete [] ipnum;
    }


    void start(bool updateeclwatch)
    {
        CNewXRefManagerBase::start(updateeclwatch,clustname);
    }
    


    void addIpHash(const IpAddress &ip,unsigned n)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull()) 
            if (++h==iphashsz)
                h = 0;
        iphash[h] = ip;
        ipnum[h] = n;
    }

    unsigned checkIpHash(const IpAddress &ip)
    {
        unsigned r;
        _cpyrev4(&r,&ip);
        unsigned h = hashc((const byte *)&r,sizeof(r),0)%iphashsz;
        while (!iphash[h].isNull()) {
            if (iphash[h].ipequals(ip))
                return ipnum[h];
            if (++h==iphashsz)
                h = 0;
        }
        return NotFound;
    }


    bool setGroup(const char *_clustname,const char *_grpname, IArrayOf<IGroup> &done, StringArray &donedir)
    {
        StringBuffer cluststr(_clustname);
        //cluststr.toLowerCase(); do not lower case
        clustname.set(cluststr);
        StringBuffer grpstr;
        StringBuffer range;
        if (!decodeChildGroupName(_grpname,grpstr, range))
            grpstr.append(_grpname);
        grpstr.toLowerCase();
        StringAttr grpname(grpstr.str());
        StringBuffer basedir;
        GroupType groupType;
        grp.setown(queryNamedGroupStore().lookup(grpstr.str(), basedir, groupType));
        if (!grp) {
            OERRLOG(LOGPFX "Cluster %s node group %s not found",clustname.get(),grpstr.str());
            return false;
        }
        // Group overlap doesn't apply to containerized. Should be cleaned up once planes are fully integrated
        if (!isContainerized()) {
            ForEachItemIn(i1,done) {
                GroupRelation gr = done.item(i1).compare(grp);
                if ((gr==GRidentical)||(gr==GRsubset)) {
                    if (strcmp(basedir.str(),donedir.item(i1))==0) {
                        OWARNLOG(LOGPFX "Node group %s already done",grpstr.str());
                        return false;
                    }
                }
            }
        }
        done.append(*LINK(grp));
        donedir.append(basedir.str());
        numnodes = grp->ordinality();
        // lets add HT for grp
        delete [] iphash;
        iphash = NULL;
        delete [] ipnum;
        iphashsz = numnodes*2;
        iphash = new IpAddress[iphashsz];
        ipnum = new unsigned[iphashsz];
        SocketEndpointArray deduppedEps;
        ForEachNodeInGroup(i,*grp) {
            const SocketEndpoint &ep = grp->queryNode(i).endpoint();
            if (ep.port!=0) 
                OWARNLOG(LOGPFX "Group has ports!");
            // check port 0 TBD
            if (NotFound == checkIpHash(ep)) {
                addIpHash(ep,i);
                deduppedEps.append(ep);
            }
        }   
        rawgrp.setown(createIGroup(deduppedEps));
        numuniqnodes = rawgrp->ordinality();
        clusters.kill();
        clusterscsl.clear().append(grpstr);
        clusters.append(grpstr.str());
        if (!isContainerized()) {
            // This code is locating all other groups that are a subset of the group being scanned.
            // and builds this list up into 'clustercsl', which is used when XREF identifies a lost
            // file, tagging them with this list, as possible candidate clusters it may be part of.
            Owned<INamedGroupIterator> giter = queryNamedGroupStore().getIterator(rawgrp,false);
            StringBuffer gname;
            ForEach(*giter) {
                giter->get(gname.clear());
                if (strcmp(grpname,gname.str())!=0) {
                    clusters.append(gname.str());
                    clusterscsl.append(',').append(gname.str());
                }
            }
            // add the first IP also
            rawgrp->queryNode(0).endpoint().getHostText(gname.clear());
            clusters.append(gname.str());
            clusterscsl.append(',').append(gname.str());
        }
        if (isContainerized()) {
            Owned<const IPropertyTree> plane = getStoragePlaneConfig(_clustname, true);
            rootdir.set(plane->queryProp("@prefix"));
        }
        else if (basedir.length()==0) {
            const char *ddir = "thor";
            const char *rdir = "thor";
            StringBuffer datadir;
            StringBuffer repdir;
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"data","thor",_clustname,datadir))
                ddir = datadir.str();
            if (getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"mirror","thor",_clustname,repdir))
                rdir = repdir.str();
            iswin = grp->ordinality()?(getDaliServixOs(grp->queryNode(0).endpoint())==DAFS_OSwindows):false;
            setBaseDirectory(ddir,0,iswin?DFD_OSwindows:DFD_OSunix);
            setBaseDirectory(rdir,1,iswin?DFD_OSwindows:DFD_OSunix);
            rootdir.set(queryBaseDirectory(grp_unknown, 0, iswin?DFD_OSwindows:DFD_OSunix));
        }
        else {
            rootdir.set(basedir);
            iswin = getPathSepChar(rootdir.get())=='\\';
        }
        assertex(!rootdir.isEmpty());
        return true;
    }


    void clear()
    {
        mem.reset();
        delete root;
        root = new cDirDesc(mem,"");
    }

    cDirDesc *findDirectory(const char *name)
    { 
        if (stricmp(name,rootdir)==0) 
            return root;
        if (!*name) 
            return NULL;
        StringBuffer pdir;
        const char *tail = splitDirTail(name,pdir);
        size32_t dl = pdir.length();
        if (dl&&isPathSepChar(pdir.charAt(dl-1)))
            pdir.setLength(--dl);
        cDirDesc *p = findDirectory(pdir.str());
        if (!p)
            return NULL;
        return p->lookupDir(tail,NULL);
    }

    bool dirFiltered(const char *filename)
    {
        // TBD (e.g. collections)
        return false;
    }

    bool fileFiltered(const char *filename,const CDateTime &dt)
    {
        if (!filename||!*filename)
            return true;
        const char *tail=pathTail(filename);
        if (tail&&(memicmp(tail,"backup",6)==0)) {
            size32_t sz = strlen(tail);
            if (sz>10) {
                if (strcmp(tail+sz-4,".lst")==0)
                    return true;
                if (strcmp(tail+sz-4,".log")==0)
                    return true;
            }
        }
        return false;
    }


    bool scanDirectory(unsigned node,const SocketEndpoint &ep,StringBuffer &path, unsigned drv, cDirDesc *pdir, IFile *cachefile, unsigned level)
    {
        size32_t dsz = path.length();
        if (pdir==NULL) 
            pdir = root;
        RemoteFilename rfn;
        rfn.setPath(ep,path.str());
        Owned<IFile> file;
        if (cachefile)
            file.set(cachefile);
        else
            file.setown(createIFile(rfn));
        Owned<IDirectoryIterator> iter;
        Owned<IException> e;
        {
            CriticalUnblock unblock(crit); // not strictly necessary if numThreads==1, but no harm
            try {
                iter.setown(file->directoryFiles(NULL,false,true));
            }
            catch (IException *_e) {
                e.setown(_e);
            }
        }
        if (e) {
            StringBuffer tmp(LOGPFX "scanDirectory ");
            rfn.getRemotePath(tmp);
            EXCLOG(e,tmp.str());
            return false;
        }
        StringBuffer fname;
        offset_t nsz = 0;
        StringArray dirs;
        ForEach(*iter) {
            iter->getName(fname.clear());
            if (iswin)
                fname.toLowerCase();
            addPathSepChar(path).append(fname);
            if (iter->isDir())  {
                // NB: Check if a subdirectory is a stripe directory under certain conditions
                // Stripe directories must be under root. The level is 0 if root
                // Only look for stripe directories if the plane details say it is striped
                if ((level == 0) && isPlaneStriped) {
                    const char *dir = fname.str();
                    bool isDirStriped = dir[0] == 'd' && dir[1] != '\0'; // Directory may be striped if it starts with 'd' and longer than one character
                    if (isDirStriped) {
                        dir++;
                        while (*dir) {
                            if (!isdigit(*(dir++))) {
                                isDirStriped = false;
                                break;
                            }
                        }
                        if (isDirStriped) {
                            // To properly match all file parts, we need to remove the stripe directory from the path
                            // so that the cDirDesc hierarchy matches the logical scope hierarchy
                            // /var/lib/HPCCSystems/hpcc-data/d1/somescope/otherscope/afile.1_of_2
                            // /var/lib/HPCCSystems/hpcc-data/d2/somescope/otherscope/afile.2_of_2
                            // These files would never be matched if we didn't build up the cDirDesc structure without the stripe directory
                            if (!scanDirectory(node,ep,path,drv,pdir,NULL,level+1))
                                return false;

                            path.setLength(dsz);
                            continue;
                        }
                    }
                    // Top-level directory is not striped, but isPlaneStriped is true. Throw an error, but continue processing directory as normal
                    OERRLOG(LOGPFX "Top-level directory striping mismatch for %s: isPlaneStriped=%d", path.str(), isPlaneStriped);
                }
                dirs.append(fname.str());
            }
            else {
                CDateTime dt;
                offset_t fsz = iter->getFileSize();
                nsz += fsz;
                iter->getModifiedTime(dt);
                if (!fileFiltered(path.str(),dt)) {
                    try {
                        pdir->addFile(drv,fname.str(),fsz,dt,node,ep,*grp,numnodes,&mem);
                    }
                    catch (IException *e) {
                        StringBuffer filepath, errMsg;
                        addPathSepChar(rfn.getRemotePath(filepath)).append(fname);
                        error(filepath.str(), "scanDirectory Error adding file : %s", e->errorMessage(errMsg).str());
                        e->Release();
                    }
                }
            }
            path.setLength(dsz);
        }
        iter.clear();
        ForEachItemIn(i,dirs) {
            addPathSepChar(path).append(dirs.item(i));
            if (file.get()&&!resetRemoteFilename(file,path.str())) // sneaky way of avoiding cache
                file.clear();
            if (!scanDirectory(node,ep,path,drv,pdir->lookupDir(dirs.item(i),&mem),file,level+1))
                return false;
            path.setLength(dsz);
        }
        pdir->addNodeStats(node,drv,nsz);
        return true;

    }

    bool scanDirectories(bool &abort, unsigned numThreads)
    {
        class casyncfor: public CAsyncFor
        {
            CNewXRefManager &parent;
            const char *rootdir;
            unsigned n;
            unsigned r;
            CriticalSection &crit;
            bool &abort;
        public:
            bool ok;
            casyncfor(CNewXRefManager &_parent,const char *_rootdir,CriticalSection &_crit,bool &_abort)
                : parent(_parent), crit(_crit), abort(_abort)
            {
                rootdir = _rootdir;
                n = parent.numuniqnodes;
                r = (n+1)/2;
                ok = true;
            }
            void Do(unsigned i)
            {
                if (abort)
                    return;
                CriticalBlock block(crit);
                if (!ok||abort)
                    return;

                StringBuffer path(rootdir);
                StringBuffer tmp;
                // A hosted plane will never be striped, so for striped planes, use local host
                if (parent.isPlaneStriped)
                {
                    assertex(!parent.storagePlane->hasProp("@hostGroup"));
                    SocketEndpoint localEP;
                    localEP.setLocalHost(0);
                    addPathSepChar(path).append('d').append(i+1);
                    parent.log("Scanning %s directory %s",parent.storagePlane->queryProp("@name"),path.str());
                    if (!parent.scanDirectory(0,localEP,path,0,parent.root,NULL,1))
                    {
                        ok = false;
                        return;
                    }
                }
                else
                {
                    SocketEndpoint ep = parent.rawgrp->queryNode(i).endpoint();
                    parent.log("Scanning %s directory %s",ep.getEndpointHostText(tmp).str(),path.str());
                    if (!parent.scanDirectory(i,ep,path,0,NULL,NULL,0)) {
                        ok = false;
                        return;
                    }
                    if (!isContainerized()) {
                        i = (i+r)%n;
                        setReplicateFilename(path,1);
                        ep = parent.rawgrp->queryNode(i).endpoint();
                        parent.log("Scanning %s directory %s",ep.getEndpointHostText(tmp.clear()).str(),path.str());
                        if (!parent.scanDirectory(i,ep,path,1,NULL,NULL,0)) {
                            ok = false;
                        }
                    }
                }
    //             PROGLOG("Done %i - %d used",i,parent.mem.maxallocated());
            }
        } afor(*this,rootdir,crit,abort);
        unsigned numMaxThreads = 0;
        if (isPlaneStriped)
            numMaxThreads = numStripedDevices;
        else
            numMaxThreads = numuniqnodes;
        if (numThreads > numMaxThreads)
            numThreads = numMaxThreads;
        afor.For(numMaxThreads,numThreads,true,numThreads>1);
        if (afor.ok)
            log("Directory scan complete");
        else
            log("Errors occurred during scan");
        return afor.ok;
    }

    void scanLogicalFiles(bool &abort) 
    {
        if (!grp||abort)
            return;
        class cfilescan1 : public CSDSFileScanner
        {
            Owned<IRemoteConnection> conn;
            CNewXRefManager &parent;
            bool &abort;

            bool checkFileOk(IPropertyTree &file,const char *filename)
            {
                if (abort)
                    return false;
                StringArray groups;
                getFileGroups(&file,groups);
                if (groups.ordinality()==0) { 
                    parent.error(filename,"File has no group defined");
                    return false;
                }
                ForEachItemIn(i,groups) {
                    ForEachItemIn(j,parent.clusters) {
                        if (strcmp(parent.clusters.item(j),groups.item(i))==0) {
//                          if (j!=0)
//                              OWARNLOG("DANXREF(scanFiles):  %s has alt group %s",filename,parent.clusters.item(i));
                            return true;
                        }
                    }
                }
                return false;
            }

            bool checkScopeOk(const char *scopename)
            {
                return !abort;
            }
            

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                if (abort)
                    return;
                parent.log("Process file %s",name.str());
                parent.fnum++;

                Owned<IFileDescriptor> fdesc;
                try {
                    fdesc.setown(deserializeFileDescriptorTree(&file,&queryNamedGroupStore()));
                }
                catch (IException *e) {
                    EXCLOG(e,"processFile");
                    e->Release();
                }
                if (fdesc) {
                    unsigned np = fdesc->numParts();
                    if (np==0) {
                        parent.error(name.str(),"File has no parts");
                        return;
                    }
                    bool checkzport = true;
                    StringBuffer fn;
                    StringBuffer dir;
                    StringBuffer lastdir;
                    cDirDesc *pdir = NULL;
                    bool islost = false;
                    bool incluster = true;          
                    for (unsigned p=0;p<np;p++) {
                        if (abort)
                            return;
                        unsigned matched = 0;
                        unsigned nc = fdesc->numCopies(p);
                        if (nc==0) 
                            continue;   // ignore if no parts
                        for (unsigned c=0;c<nc;c++) {
                            RemoteFilename rfn;
                            fdesc->getFilename(p,c,rfn);
                            const SocketEndpoint &ep = rfn.queryEndpoint();
                            if (checkzport&&ep.port) {
                                parent.error(name.str(),"File has non-zero port");
                                checkzport = false;
                            }
                            unsigned nn = parent.checkIpHash(ep);
                            if (nn!=NotFound) {
                                rfn.getLocalPath(fn.clear());
                                const char *tail = splitDirTail(fn.str(),dir.clear());
                                if (dir.length()&&isPathSepChar(dir.charAt(dir.length()-1)))
                                    dir.setLength(dir.length()-1);
                                unsigned drv = isContainerized() ? 0 : getPathDrive(dir.str()); // should match c
                                if (drv)
                                    setReplicateFilename(dir,0);
                                if ((lastdir.length()==0)||(strcmp(lastdir.str(),dir.str())!=0)) {
                                    pdir = parent.findDirectory(dir.str());
                                    lastdir.clear().append(dir);
                                }
                                if (pdir&&pdir->markFile(drv,tail,nn,ep,*parent.grp,parent.numnodes)) {
                                    matched++;
                                }
                            }
                            else if (p==0) { // skip file
                                if (parent.verbose)
                                    PROGLOG(LOGPFX "ignoring file %s",name.str());
                                p = np;
                                incluster = false;
                                break;
                            }
                        }
                        if (!matched&&incluster)
                            islost = true;
                    }
                    if (islost) {
                        if (parent.verbose)
                            PROGLOG(LOGPFX "Potential lost file: %s",name.str());
                        parent.lostfiles.append(name.str());
                    }
                }
                else {
                    parent.error(name.str(),"cannot create file descriptor");
                }
            }
        public:

            cfilescan1(CNewXRefManager &_parent,bool &_abort)
                : parent(_parent), abort(_abort)
            {
            }

            ~cfilescan1()
            {
            }

            void scan()
            {
                if (abort)
                    return;
                conn.setown(querySDS().connect("/Files", myProcessSession(), 0, 100000));
                if (!conn||abort)
                    return;
                CSDSFileScanner::scan(conn);
            }

        } filescan(*this,abort);

        filescan.scan();
        log("File scan complete");

    }

    bool checkOrphanPhysicalFile(RemoteFilename &rfn,offset_t &sz,CDateTime &dt)
    {
        try {
            Owned<IFile> file = createIFile(rfn);
            bool isdir;
            bool ret = false;
            if (file->getInfo(isdir,sz,dt)&&!isdir) 
                ret = true;
#ifdef _DEBUG
            StringBuffer dbgname;
            rfn.getPath(dbgname);
            PROGLOG("checkOrphanPhysicalFile(%s) = %s",dbgname.str(),ret?"true":"false");
#endif
            return ret;
        }
        catch (IException *e) {
            StringBuffer tmp(LOGPFX "listOrphans reading ");
            rfn.getRemotePath(tmp);
            EXCLOG(e,tmp.str());
            e->Release();
        }
        return false;
    }

    void addOrphanPartNode(Owned<IPropertyTree> &branch,const SocketEndpoint &ep,unsigned i,bool rep)
    {
        if (!branch)
            branch.setown(createPTree("File"));
        i++;
        StringBuffer tmp;
        tmp.appendf("Part[Num=\"%d\"]",i);
        IPropertyTree* pb = branch->queryPropTree(tmp.str());
        if (!pb) {
            pb = createPTree("Part");
            pb->setPropInt("Num",i);
            pb = branch->addPropTree("Part",pb);
        }
        pb->setProp(rep?"RNode":"Node",ep.getEndpointHostText(tmp.clear()).str());
    }

    void addExternalFoundFile(cFileDesc *file, const char *currentPath, unsigned int recentCutoffDays)
    {
        // Treat external files as a single file because without a partmask there is no way
        // to determine the number of parts or the part number
        StringBuffer filePath(currentPath);
        file->getNameMask(addPathSepChar(filePath));

        // Files that are non-conforming will be marked as misplaced in the directory scan
        unsigned node = 0;
        assertex(file->misplaced);
        node = file->misplaced->nn;

        SocketEndpoint ep = grp->queryNode(node).endpoint();
        RemoteFilename rfn;
        rfn.setPath(ep, filePath.str());
        offset_t sz;
        CDateTime dt;
        bool found;
        {
            CheckTime ct("checkOrphanPhysicalFile ");
            found = checkOrphanPhysicalFile(rfn,sz,dt);
            if (ct.slow())
                ct.appendMsg(filePath.str());
        }
        if (found)
        {
            CDateTime now;
            now.setNow();
            CDateTime co(dt);
            co.adjustTime(recentCutoffDays*60*24);
            if (co.compare(now)>=0) {
                warn(filePath.str(),"Recent external file ignored");
                return;
            }

            StringBuffer tmp;
            Owned<IPropertyTree> branch;
            addOrphanPartNode(branch,rfn.queryEndpoint(),0,false);
            branch->setPropInt64("Size",sz);
            branch->setProp("Partmask",filePath.str());
            branch->setProp("Modified",dt.getString(tmp.clear()));
            branch->setPropInt("Numparts",1);
            branch->setPropInt("Partsfound",1);
            foundbranch->addPropTree("File",branch.getClear());
        }
    }

    void listOrphans(cFileDesc *f,const char *currentPath,const char *currentScope,bool &abort,unsigned int recentCutoffDays)
    {
        if (abort)
            return;
        if (!f)
            return;
        // first check if any orhans at all (maybe could do this faster)
#ifdef _DEBUG
        StringBuffer dbgname;
        f->getNameMask(dbgname);
        PROGLOG("listOrphans TEST FILE(%s)",dbgname.str());
#endif

        // Non-conforming files won't have a part mask and should be treated as single files
        if (!f->isHPCCFile()) {
            addExternalFoundFile(f,currentPath,recentCutoffDays);
            return;
        }

        unsigned drv;
        unsigned drvs = isContainerized() ? 1 : 2;
        for (drv=0;drv<drvs;drv++) {
            unsigned i0;
            for (i0=0;i0<f->N;i0++) 
                if (f->testpresent(drv,i0)&&!f->testmarked(drv,i0)) 
                    break;
            if (i0<f->N)
                break;
        }
        if (drv==drvs)
            return; // no orphans
        StringBuffer mask;
        StringBuffer scopeBuf(currentScope);
        scopeBuf.append("::");
        f->getNameMask(mask);
        assertex(f->getName(scopeBuf)); // Should always return true for HPCC files
        // orphans are only orphans if there doesn't exist a valid file
        try {
            if (queryDistributedFileDirectory().exists(scopeBuf.str(),udesc,true,false)) {
                warn(mask.str(),"Orphans ignored as %s exists",scopeBuf.str());
                return;
            }
        }
        catch (IException *e) {
            EXCLOG(e,"CNewXRefManager::listOrphans");
            e->Release();
            return;
        }
        // treat drive differently for orphans (bit silly but bward compatible
        MemoryAttr buf;
        bool *completed = (bool *)buf.allocate(f->N);
        memset(completed,0,f->N);
        Owned<IPropertyTree> branch[2];
        CDateTime mostrecent[2];
        offset_t totsize[2];
        totsize[0] = 0;
        totsize[1] = 0;
        unsigned ndone[2];
        ndone[0] = 0;
        ndone[1] = 0;
        unsigned fnameHash = getFilenameHash(scopeBuf.str());
        const char * prefix = storagePlane->queryProp("@prefix");
        for (drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            bool warnnotexists = true;
            StringBuffer path;
            if (drv)
                setReplicateFilename(addPathSepChar(path.append(currentPath)),drv);
            size32_t psz = path.length();
            StringBuffer tmp;
            for (unsigned pn=0;pn<f->N;pn++) {
                if (f->testpresent(drv,pn)&&!f->testmarked(drv,pn)) {
                    RemoteFilename rfn;
                    constructPartFilename(grp, pn+1, drv, f->N, fnameHash, drv, f->isDirPerPart, scopeBuf.str(), prefix, mask.str(), numStripedDevices, rfn);
                    offset_t sz;
                    CDateTime dt;
                    bool found;
                    {
                        CheckTime ct("checkOrphanPhysicalFile ");
                        found = checkOrphanPhysicalFile(rfn,sz,dt);
                        if (ct.slow()) {
                            rfn.getPath(tmp.clear());
                            ct.appendMsg(tmp.str());
                        }
                    }
                    if (found) {
                        if (mostrecent[drv].isNull()||(dt.compare(mostrecent[drv],false)>0)) 
                            mostrecent[drv].set(dt);
                        completed[pn] = true;
                        totsize[drv] += sz;
                        ndone[drv]++;
                        addOrphanPartNode(branch[drv],rfn.queryEndpoint(),pn,drv!=0);
                    }
                    else if (warnnotexists) {
                        rfn.getRemotePath(tmp.clear());
                        tmp.append(" no longer exists");
                        warn(tmp.str(),"Orphan file no longer exists");
                        warnnotexists = false;
                    }
                }
                path.setLength(psz);
            }
        }
        if (abort)
            return;
        // check if complete here
        unsigned ncomplete = 0;
        for (unsigned i=0;i<f->N;i++) 
            if (completed[i]) 
                ncomplete++;
        if (ncomplete!=f->N) {  // if a found file ignore misplaces
            cMisplacedRec *mp = f->misplaced;
            bool warnnotexists = true;
            while (mp) {
                if (abort)
                    return;
                if (!mp->marked) {
                    unsigned drv = mp->getDrv(numnodes);
                    RemoteFilename rfn;
                    constructPartFilename(grp, mp->pn, drv, f->N, fnameHash, drv, f->isDirPerPart, scopeBuf.str(), prefix, mask.str(), numStripedDevices, rfn);
                    offset_t sz;
                    CDateTime dt;
                    if (checkOrphanPhysicalFile(rfn,sz,dt)) {
                        if (mostrecent[drv].isNull()||(dt.compare(mostrecent[drv],false)>0)) 
                            mostrecent[drv].set(dt);
                        totsize[drv] += sz;
                        ndone[drv]++;
                        addOrphanPartNode(branch[drv],rfn.queryEndpoint(),mp->pn,drv!=0);
                    }
                    else if (verbose&&warnnotexists) {
                        StringBuffer tmp;
                        rfn.getRemotePath(tmp);
                        warn(tmp.str(),"Orphan file no longer exists");
                        warnnotexists = false;
                    }
                }
                mp = mp->next;
            }
        }
        CDateTime now;
        now.setNow();
        StringBuffer tmp;
        for (drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            if (branch[drv]) {
                addPathSepChar(tmp.clear().append(currentPath)).append(mask.str());
                if (drv) 
                    setReplicateFilename(tmp,1);
                CDateTime co(mostrecent[drv]);
                co.adjustTime(recentCutoffDays*60*24);
                if (co.compare(now)>=0) {
                    warn(tmp.str(),"Recent orphans ignored");
                    branch[drv].clear();
                    continue;
                }
                branch[drv]->setProp("Partmask",tmp.str());
                branch[drv]->setPropInt64("Size",totsize[drv]);
                branch[drv]->setProp("Modified",mostrecent[drv].getString(tmp.clear()));
                branch[drv]->setPropInt("Numparts",f->N);
                branch[drv]->setPropBool("IsDirPerPart", f->isDirPerPart);
            }
            if (ncomplete!=f->N) {
                if (branch[drv]) 
                    branch[drv]->setPropInt("Partsfound",ndone[drv]);
            }
        }
        if (ncomplete!=f->N) {
            if (branch[0])
                orphansbranch->addPropTree("File",branch[0].getClear());
            if (branch[1])
                orphansbranch->addPropTree("File",branch[1].getClear());
        }
        else {
            if (branch[0]) {
                if (branch[1]) {
                    StringBuffer xpath;
                    StringBuffer ips;
                    for (unsigned i=0;i<f->N;i++) {
                        xpath.clear().appendf("Part[Num=\"%d\"]/RNode[1]",i+1);
                        if (branch[1]->getProp(xpath.str(),tmp.clear())) {
                            SocketEndpoint ep(tmp.str());
                            addOrphanPartNode(branch[0],ep,i,true);
                        }
                    }
                }
                foundbranch->addPropTree("File",branch[0].getClear());
            }
            else if (branch[1])
                foundbranch->addPropTree("File",branch[1].getClear());
        }
    }

    void listDirectory(cDirDesc *d,const char *name,bool &abort)
    {
        unsigned drvs = isContainerized() ? 1 : 2;
        for (unsigned drv=0;drv<drvs;drv++) {
            if (abort)
                return;
            if ((d->files.ordinality()!=0)||(d->totalsize[drv]!=0)||d->empty(drv)) { // final empty() is to make sure only truly empty dirs get added
                                                                                  // but not empty parents
                Owned<IPropertyTree> dt = createPTree("Directory");
                if (drv) {
                    StringBuffer tmp(name);
                    setReplicateFilename(tmp,drv);
                    dt->addProp("Name",tmp.str());
                }
                else
                    dt->addProp("Name",name);
                dt->addPropInt("Num",d->files.ordinality());
                dt->addPropInt64("Size",d->totalsize[drv]);
                if (d->totalsize[drv]) {
                    StringBuffer s1;
                    if (d->maxnode[drv]) {
                        dt->addPropInt64("MaxSize",d->maxsize[drv]);
                        grp->queryNode(d->maxnode[drv]-1).endpoint().getHostText(s1);
                        dt->addProp("MaxIP",s1.str());
                    }
                    if (d->minnode[drv]) {
                        dt->addPropInt64("MinSize",d->minsize[drv]);
                        grp->queryNode(d->minnode[drv]-1).endpoint().getHostText(s1.clear());
                        dt->addProp("MinIP",s1.str());
                    }
                    if (d->minsize[drv]<d->maxsize[drv]) {
                        __int64 av = d->totalsize[drv]/(__int64)rawgrp->ordinality();
                        if (av) {
                            unsigned pcp = (unsigned)(d->maxsize[drv]*100/av);
                            unsigned pcn = (unsigned)(d->minsize[drv]*100/av);
                            if ((pcp>100)||(pcn<100)) {
                                s1.clear().appendf("+%d%%/-%d%%",pcp-100,100-pcn);
                                dt->addProp("Skew",s1.str());
                            }
                        }
                    }
                }
                sorteddirs.append(*dt.getClear());
            }
        }
    }

    void listOrphans(cDirDesc *d,StringBuffer &basedir,StringBuffer &scope,bool &abort,unsigned int recentCutoffDays)
    {
        if (abort)
            return;
        if (!d) {
            d = root;
            if (!d)
                return;
            basedir.append(rootdir);
        }
#ifdef _DEBUG
        StringBuffer dbgname;
        d->getName(dbgname);
        PROGLOG("listOrphans TEST DIR(%s)",dbgname.str());
#endif
        size32_t bds = basedir.length();
        size32_t scopeLen = scope.length();
        if (bds!=0) 
            addPathSepChar(basedir);
        if (scopeLen!=0)
            scope.append("::");
        d->getName(basedir);
        d->getName(scope);
        listDirectory(d,basedir.str(),abort);
        unsigned i = 0;
        cDirDesc *dir = d->dirs.first(i);
        while (dir) {
            mergeDirPerPartDirs(d,dir,basedir,&mem);
            listOrphans(dir,basedir,scope,abort,recentCutoffDays);
            if (abort)
                return;
            dir = d->dirs.next(i);
        }
        i = 0;
        cFileDesc *file = d->files.first(i);
        while (file) {
            try {
                listOrphans(file,basedir,scope,abort,recentCutoffDays);
            }
            catch (IException *e) {
                StringBuffer filepath, errMsg;
                file->getNameMask(addPathSepChar(filepath.append(basedir)));
                error(filepath.str(),"listOrphans Error processing file : %s",e->errorMessage(errMsg).str());
                e->Release();
            }
            if (abort)
                return;
            file = d->files.next(i);
        }
        basedir.setLength(bds);
        scope.setLength(scopeLen);
    }

    static int compareDirs(IInterface * const *t1,IInterface * const *t2)
    {
        IPropertyTree *pt1 = *(IPropertyTree **)t1;
        IPropertyTree *pt2 = *(IPropertyTree **)t2;
        offset_t sz1 = pt1->getPropInt64("Size");
        offset_t sz2 = pt2->getPropInt64("Size");
        if (sz1<sz2)
            return -1;
        if (sz1>sz2)
            return 1;
        return stricmp(pt2->queryProp("Name"),pt1->queryProp("Name")); // rev
    }

    void listOrphans(bool &abort,unsigned int recentCutoffDays)
    {   
        // also does directories
        log("Scanning for orphans");
        StringBuffer basedir;
        StringBuffer scope;
        listOrphans(NULL,basedir,scope,abort,recentCutoffDays);
        if (abort)
            return;
        log("Orphan scan complete");
        sorteddirs.sort(compareDirs);   // NB sort reverse
        while (!abort&&sorteddirs.ordinality())
            dirbranch->addPropTree("Directory",&sorteddirs.popGet());
        log("Directories sorted");
    }


    void listLost(bool &abort,bool ignorelazylost,unsigned int recentCutoffDays)
    {
        log("Scanning for lost files");
        StringBuffer tmp;
        ForEachItemIn(i0,lostfiles) {
            if (abort)
                return;
            CDfsLogicalFileName lfn;
            if (!lfn.setValidate(lostfiles.item(i0))) {
                error(lostfiles.item(i0), "Invalid filename detected");
                continue;
            }
            Owned<IDistributedFile> file;
            try {
                file.setown(queryDistributedFileDirectory().lookup(lfn,udesc,AccessMode::tbdRead,false,false,nullptr,defaultPrivilegedUser));
            }
            catch (IException *e) {
                EXCLOG(e,"CNewXRefManager::listLost");
                e->Release();
            }
            if (!file) {
                error(lfn.get(),"could not lookup possible lost file");
                continue;
            }
            file->setPreferredClusters(clusterscsl);
            StringBuffer tmpname;
            file->setSingleClusterOnly();
            file->getClusterName(0,tmpname);
            if ((tmpname.length()==0)||(clusters.find(tmpname.str())==NotFound)) {
                StringBuffer tmp;
                error(lfn.get(),"could not set preferred cluster (set to %s)",tmpname.str());
                continue;
            }
            if (ignorelazylost&&(file->queryAttributes().getPropInt("@lazy")!=0)) {
                warn(lfn.get(),"Lazy file ignored");
                continue;
            }
            CDateTime dt;
            Owned<IPropertyTree> ft = createPTree("File");
            if (file->getModificationTime(dt)) {
                CDateTime now;
                now.setNow();
                CDateTime co(dt);
                co.adjustTime(recentCutoffDays*60*24);
                if (co.compare(now)>=0) {
                    warn(lfn.get(),"Recent file ignored");
                    continue;
                }
                dt.getString(tmp.clear());
                ft->setProp("Modified",tmp.str());
            }
            unsigned np = file->numParts();
            unsigned cn = 0;                    
            ft->setProp("Name",lfn.get());
            tmp.clear().append(file->queryPartMask()).toLowerCase();
            ft->setProp("Partmask",tmp.str());
            ft->setPropInt("Numparts",np);
            file->getClusterName(cn,tmp.clear());
            ft->setProp("Cluster",tmp.str());
            bool *primlost = new bool[np];
            bool *replost = new bool[np];
            for (unsigned i0=0;i0<np;i0++) {
                primlost[i0] = true;
                replost[i0] = true;
            }
            bool ok = true;
            if (abort) {
                delete [] primlost;
                delete [] replost;
                return;
            }
            Owned<IDistributedFilePartIterator> piter = file->getIterator();
            ForEach(*piter) {
                if (abort) {
                    delete [] primlost;
                    delete [] replost;
                    return;
                }
                IDistributedFilePart &part = piter->query();
                unsigned pn = part.getPartIndex();
                unsigned nc = part.numCopies();
                for (unsigned copy = 0; copy < nc; copy++) {
                    RemoteFilename rfn;
                    part.getFilename(rfn,copy);
                    Owned<IFile> partfile = createIFile(rfn);
                    StringBuffer eps;
                    bool lost = true;
                    try {
                        if (partfile->exists()) {
                            if (copy>0)
                                replost[pn] = false;
                            else
                                primlost[pn] = false;
                            lost = false;
                        }
                    }
                    catch (IException *e)
                    {
                        StringBuffer tmp(LOGPFX "Checking file ");
                        rfn.getRemotePath(tmp);
                        EXCLOG(e, tmp.str());
                        e->Release();
                        ok = false;
                    }
                    if (!ok)
                        break;
                    if (lost) {
                        Owned<IPropertyTree> pt = createPTree("Part");
                        StringBuffer tmp;
                        rfn.queryEndpoint().getHostText(tmp);
                        pt->setProp("Node",tmp.str());
                        pt->setPropInt("Num",pn+1);
                        if (copy>0)
                            pt->setPropInt("Replicate",copy);
                        ft->addPropTree("Part",pt.getClear());
                    }
                }
            }
            unsigned pc = 0;
            unsigned rc = 0;
            unsigned c = 0;
            for (unsigned i1=0;i1<np;i1++) {
                if (primlost[i1]&&replost[i1]) {
                    pc++;
                    rc++;
                    c++;
                }
                else if (primlost[i1]) 
                    pc++;
                else if (replost[i1])
                    rc++;
            }
            delete [] primlost;
            delete [] replost;
            if (c) {
                PROGLOG(LOGPFX "Adding %s to lost files",lfn.get());
                ft->addPropInt("Partslost",c);
                ft->addPropInt("Primarylost",pc);
                ft->addPropInt("Replicatedlost",rc);
                lostbranch->addPropTree("File",ft.getClear());
            }
        }
        log("Lost scan complete");
    }


    void save()
    {
        if (!rootdir.isEmpty()) {
            orphansbranch->setProp("@rootdir",rootdir);
            lostbranch->setProp("@rootdir",rootdir);
            foundbranch->setProp("@rootdir",rootdir);
        }
        saveXML("orphans.xml",orphansbranch);
        saveXML("found.xml",foundbranch);
        saveXML("lost.xml",lostbranch);
        saveXML("directories.xml",dirbranch);
    }






};

class CSuperfileCheckManager: public CNewXRefManagerBase
{
public:

    CSuperfileCheckManager()
    {
        clustname.set("SuperFiles");
    }

    void start(bool updateeclwatch)
    {
        CNewXRefManagerBase::start(updateeclwatch,"SuperFiles");
    }

    void errornotrecent(const char *lname,const char * format, ...) __attribute__((format(printf, 3, 4)))
    {
        // checks can lock LFN and not recently changed
        CriticalBlock block(logsect);
        va_list args;
        va_start(args, format);
        StringBuffer line;
        line.valist_appendf(format, args);
        va_end(args);
        CDfsLogicalFileName lfn;
        if (lfn.setValidate(lname)&&!lfn.isExternal()&&!lfn.isForeign()) {
            StringBuffer xpath;
            lfn.makeFullnameQuery(xpath,DXB_SuperFile,true);
            bool ignore = true;
            try {
                Owned<IRemoteConnection> conn = querySDS().connect(xpath.str(),myProcessSession(),RTM_LOCK_READ ,1000*10);
                if (!conn) {
                    lfn.makeFullnameQuery(xpath.clear(),DXB_File,true);
                    conn.setown(querySDS().connect(xpath.str(),myProcessSession(),RTM_LOCK_READ ,1000*10));
                }
                StringBuffer str;
                if (conn->queryRoot()->getProp("@modified",str)) {
                    CDateTime dt;
                    dt.setString(str.str());
                    dt.adjustTime(60*24);
                    CDateTime now;
                    now.setNow();
                    if (now.compareDate(dt)>0)
                        ignore = false;
                }
            }
            catch (IException *e) {
                e->Release();
            }
            if (ignore) {
                PROGLOG(LOGPFX "Ignoring %s: %s",lname,line.str());
                return;
            }
        }
        if (errors.ordinality()<1000) {
            errors.append(*new cMessage(lname,line.str()));
            if (errors.ordinality()==1000) 
                errors.append(*new cMessage("","error limit exceeded (1000), truncating"));
        }

        OERRLOG("%s: %s",lname,line.str());
    }

    void checkSuperFileLinkage()
    {
        StringArray superowner;
        StringArray superowned;
        StringArray fileowned;
        StringArray fileowner;
        sfnum = 0;
        fnum = 0;
        class cfilescan1: public CSDSFileScanner
        {
            CSuperfileCheckManager &parent;
            StringArray &superowner;
            StringArray &superowned;
            StringArray &fileowned;
            StringArray &fileowner;

            void processFile(IPropertyTree &file,StringBuffer &name)
            {
                //parent.statlog("Scanning File %s",name.str());
                Owned<IPropertyTreeIterator> iter = file.getElements("SuperOwner");
                StringArray superowner;
                bool owned = false;
                ForEach(*iter) {
                    IPropertyTree &sfile = iter->query();
                    const char *owner = sfile.queryProp("@name");
                    if (!owner||!*owner) {
                        parent.errornotrecent(name.str(),"FAILED nullsuperownername");
                    }
                    else {
                        bool ok = true;
                        ForEachItemIn(i,superowner) {
                            if (strcmp(owner,superowner.item(i))==0) {
                                parent.errornotrecent(name.str(),"FAILED dupsuperownername(%s)",owner);
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            superowner.append(owner);
                            fileowned.append(name.str());
                            fileowner.append(owner);
                            owned = true;
                        }
                    }   
                }
                if (owned)
                    parent.fnum++;
            }
            void processSuperFile(IPropertyTree &file,StringBuffer &name)
            {
                parent.sfnum++;
                parent.log("Scanning SuperFile %s",name.str());
                unsigned numsub = file.getPropInt("@numsubfiles");
                unsigned n = 0;
                Owned<IPropertyTreeIterator> iter = file.getElements("SubFile");
                Owned<IBitSet> parts = createThreadSafeBitSet();
                StringArray subname;
                ForEach(*iter) {
                    IPropertyTree &sfile = iter->query();
                    const char *owned = sfile.queryProp("@name");
                    if (!owned||!*owned) {
                        parent.errornotrecent(name.str(),"FAILED nullsubfilename");
                    }
                    else {
                        bool ok = true;
                        ForEachItemIn(i,subname) {
                            if (strcmp(owned,subname.item(i))==0) {
                                parent.errornotrecent(name.str(),"FAILED dupsubfilename(%s)",owned);
                                ok = false;
                                break;
                            }
                        }
                        if (ok) {
                            unsigned num = sfile.getPropInt("@num");
                            if (!num)
                                parent.errornotrecent(name.str(),"FAILED missingsubnum(%s)",sfile.queryProp("@name"));
                            else if (num>numsub)
                                parent.errornotrecent(name.str(),"FAILED toobigsubnum(%s,%d,%d)",sfile.queryProp("@name"),num,numsub);
                            else if (parts->testSet(num-1))
                                parent.errornotrecent(name.str(),"FAILED dupsubnum(%s,%d)",sfile.queryProp("@name"),num);
                            else {
                                subname.append(owned);
                                superowner.append(name.str());
                                superowned.append(owned);
                            }
                        }
                    }
                    n++;
                }
                if (n!=numsub)
                    parent.errornotrecent(name.str(),"FAILED mismatchednumsubs(%d,%d)",numsub,n);
                processFile(file,name); // superfile is a file too!
            }
        public:

            cfilescan1(CSuperfileCheckManager &_parent, StringArray &_superowner,StringArray &_superowned,StringArray &_fileowned,StringArray &_fileowner)
                : parent(_parent), superowner(_superowner),superowned(_superowned),fileowned(_fileowned),fileowner(_fileowner)
            {
            }

        } filescan(*this, superowner,superowned,fileowned,fileowner);

        Owned<IRemoteConnection> conn = querySDS().connect("/Files", myProcessSession(),0, 100000);
        filescan.scan(conn,true,true);

        bool fix = false;

        log("Crossreferencing %d SuperFiles",superowner.ordinality());
        ForEachItemIn(i1,superowner) {
            const char *owner = superowner.item(i1);
            const char *owned = superowned.item(i1);
            bool ok = false;
            if (*owned=='{') 
                ok = true;
            else {
                ForEachItemIn(i2,fileowned) {
                    if ((stricmp(owner,fileowner.item(i2))==0)&&(stricmp(owned,fileowned.item(i2))==0)) {
                        ok = true;
                        break;
                    }
                }
            }
            if (!ok) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(owned)) {
                    errornotrecent(owner,"FAILED badsubpath %s\n",owned);
                }
                else {
                    StringBuffer lfnpath;
                    lfn.makeFullnameQuery(lfnpath,DXB_File);
                    if (!lfn.isExternal()&&!lfn.isForeign()) {
                        Owned<IRemoteConnection> conn = querySDS().connect(lfnpath.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
                        if (conn) {
                            StringBuffer  path;
                            path.appendf("SuperOwner[@name=\"%s\"]",owner);
                            if (!conn->queryRoot()->hasProp(path.str())) {
                                if (fix) {
                                    Owned<IPropertyTree> t = createPTree("SuperOwner");
                                    t->setProp("@name",owner);
                                    conn->queryRoot()->addPropTree("SuperOwner",t.getClear());
                                }
                                errornotrecent(owned,"%s nosuperlink to %s",fix?"FIXED":"FAILED",owner);
                            }

                        }
                        else
                            errornotrecent(owner,"FAILED subnotexist %s",owned);
                    }
                }
            }
        }       
        log("Crossreferencing %d Files",fileowned.ordinality());
        ForEachItemIn(i3,fileowned) {
            const char *fowner = fileowner.item(i3);
            const char *fowned = fileowned.item(i3);
            bool ok = false;
            ForEachItemIn(i4,superowner) {
                if ((stricmp(superowner.item(i4),fowner)==0)&&(stricmp(superowned.item(i4),fowned)==0)) {
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                CDfsLogicalFileName lfn;
                if (!lfn.setValidate(fowner)) {
                    errornotrecent(fowned,"FAILED badsuperpath %s",fowner);
                }
                else {
                    StringBuffer lfnpath;
                    lfn.makeFullnameQuery(lfnpath,DXB_SuperFile);
                    Owned<IRemoteConnection> conn = querySDS().connect(lfnpath.str(),myProcessSession(),0, INFINITE);
                    if (conn) 
                        errornotrecent(fowner,"FAILED nosublink to %s",fowned);
                    else {
                        bool fixed = false;
                        if (fix) {
                            CDfsLogicalFileName lfn2;
                            if (lfn2.setValidate(fowned)) {
                                lfn2.makeFullnameQuery(lfnpath.clear(),DXB_File);
                                Owned<IRemoteConnection> conn2 = querySDS().connect(lfnpath.str(),myProcessSession(),fix?RTM_LOCK_WRITE:0, INFINITE);
                                if (conn2) {
                                    StringBuffer xpath;
                                    xpath.appendf("SuperOwner[@name=\"%s\"]",fowner);
                                    if (conn2->queryRoot()->hasProp(xpath.str())) {
                                        conn2->queryRoot()->removeProp(xpath.str());
                                        fixed = true;
                                    }
                                }

                            }
                        }
                        warn(fowned,"%s supernotexist %s",fixed?"FIXED":"FAILED",fowner);
                    }
                }
            }
        }       

    }



};

#if 0


void usage(const char *pname)
{
    // TBD
}

void testScan(const char *grpname)
{
    CNewXRefManager manager;
    if (!manager.setGroup(grpname)) {
        return;
    }
    if (manager.scanDirectories()) {
        manager.scanLogicalFiles();
        manager.listLost();
        manager.listOrphans();
        manager.save();
//      manager.saveToEclWatch();
    }
}


int main(int argc, char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    setNodeCaching(true);
    StringBuffer logName("danxref");
    StringBuffer aliasLogName(logName);
    aliasLogName.append(".log");

    ILogMsgHandler *fileMsgHandler = getRollingFileLogMsgHandler(logName.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str());
    queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
    queryStderrLogMsgHandler()->setMessageFields(0);
    StringBuffer cmdline;
    unsigned i;
    for (i=0;i<argc;i++)
      cmdline.append(' ').append(argv[i]);


    if (argc<3) {
        usage(argv[0]);
        return 0;
    }

    SocketEndpoint ep;
    SocketEndpointArray epa;
    ep.set(argv[1],DALI_SERVER_PORT);
    epa.append(ep);
    Owned<IGroup> group = createIGroup(epa); 
    try {
        initClientProcess(group,DCR_Dfu);
        setPasswordsFromSDS();
        testScan(argv[2]);


    }
    catch (IException *e) {
        pexception("Exception",e);
        e->Release();
    }

    closeEnvironment();
    closedownClientProcess();

    releaseAtoms();
    return 0;
}

#endif


static constexpr float maxMemPercentage = 0.9; // In containerized, leave some headroom for the pod
class CSashaXRefServer: public ISashaServer, public Thread
{  
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    bool ignorelazylost, suspendCoalescer;
    Owned<IPropertyTree> props;
    std::unordered_map<std::string, Linked<IPropertyTree>> storagePlanes;

    class cRunThread: public Thread
    {
        CSashaXRefServer &parent;
        StringAttr servers;
    public:
        cRunThread(CSashaXRefServer &_parent,const char *_servers)
            : parent(_parent), servers(_servers)
        {
        }
        int run()
        {
            parent.runXRef(servers,false,false);
            return 0;
        }
    }; 


public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaXRefServer()
        : Thread("CSashaXRefServer")
    {
        if (!isContainerized())
            suspendCoalescer = true; // can be overridden by configuration setting
        else
            suspendCoalescer = false;
        stopped = false;
    }

    ~CSashaXRefServer()
    {
    }

    void start()
    {
        Thread::start(false);
    }

    void ready()
    {
    }
    
    void stop()
    {
        if (!stopped) {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaXRefServer aborted");
    }

    void runXRef(const char *clustcsl,bool updateeclwatch,bool byscheduler)
    {
        if (stopped||!clustcsl||!*clustcsl)
            return;
        class CSuspendResume : public CSimpleInterface
        {
        public:
            CSuspendResume()
            {
                PROGLOG(LOGPFX "suspending coalesce");
                suspendCoalescingServer();
            }
            ~CSuspendResume()
            {
                PROGLOG(LOGPFX "resuming coalesce");
                resumeCoalescingServer();
            }
        };
        if (!isContainerized()) {
            Owned<CSimpleInterface> suspendresume;
            if (suspendCoalescer)
                suspendresume.setown(new CSuspendResume());
        }
        synchronized block(runmutex);
        if (stopped)
            return;
        CSuspendAutoStop suspendstop;
        PROGLOG(LOGPFX "Started %s",clustcsl);
        StringArray list;
        getFileGroups(clustcsl, list);
        bool checksuperfiles=false;
        ForEachItemInRev(i0,list) {
            if (strcmp(list.item(i0),"SuperFiles")==0) {
                checksuperfiles = true;
                list.remove(i0);
            }
        }
        // Revisit: XREF should really be plane centric only
        StringArray groups;
        StringArray cnames;
        // NB: must be a list of planes only
        ForEachItemIn(i1, list) {
            const char *planeName = list.item(i1);
            Owned<IPropertyTreeIterator> planesIter = getPlanesIterator("data", planeName);
            ForEach(*planesIter) {
                IPropertyTree &plane = planesIter->query();
                bool isNotCopy = !plane.getPropBool("@copy", false);
                bool isNotHthorPlane = !plane.getPropBool("@hthorplane", false);
                if (isNotCopy && isNotHthorPlane) {
                    planeName = plane.queryProp("@name");
                    if (isContainerized()) {
                        groups.append(planeName);
                        cnames.append(planeName);
                    }
                    storagePlanes[planeName].set(&plane);
                }
            }
        }
        if (!isContainerized()) {
            Owned<IRemoteConnection> conn = querySDS().connect("/Environment/Software", myProcessSession(), RTM_LOCK_READ, SDS_CONNECT_TIMEOUT);
            if (!conn) {
                OERRLOG("Could not connect to /Environment/Software");
                return;
            }
            clustersToGroups(conn->queryRoot(),list,cnames,groups,NULL);
        }
        IArrayOf<IGroup> groupsdone;
        StringArray dirsdone;
        ForEachItemIn(i,groups) {
#ifdef TESTINGSUPERFILELINKAGE
            continue;
#endif
            const char *gname = groups.item(i);
            unsigned maxMb;
            if (isContainerized()) {
                const char *resourcedMemory = props->queryProp("resources/@memory");
                if (!isEmptyString(resourcedMemory)) {
                    offset_t sizeBytes = friendlyStringToSize(resourcedMemory);
                    maxMb = (unsigned)(sizeBytes / 0x100000);
                }
                else
                    maxMb = DEFAULT_MAXMEMORY;
                maxMb *= maxMemPercentage;
            }
            else
                maxMb = props->getPropInt("@memoryLimit", DEFAULT_MAXMEMORY);
            CNewXRefManager manager(storagePlanes[gname],maxMb);
            if (!manager.setGroup(cnames.item(i),gname,groupsdone,dirsdone)) 
                continue;
            manager.start(updateeclwatch);
            manager.updateStatus(true);
            if (stopped)
                break;
            unsigned numThreads = props->getPropInt("@numThreads", DEFAULT_MAXDIRTHREADS);
            unsigned int recentCutoffDays = props->getPropInt("@cutoff", DEFAULT_RECENT_CUTOFF_DAYS);
            if (manager.scanDirectories(stopped,numThreads)) {
                manager.updateStatus(true);
                manager.scanLogicalFiles(stopped);
                manager.updateStatus(true);
                manager.listLost(stopped,ignorelazylost,recentCutoffDays);
                manager.updateStatus(true);
                manager.listOrphans(stopped,recentCutoffDays);
                manager.updateStatus(true);
                manager.saveToEclWatch(stopped,byscheduler);
                manager.updateStatus(true);
            }
            manager.finish(stopped);
            manager.updateStatus(true);
            if (stopped)
                break;
        }
        if (checksuperfiles&&!stopped) {
            CSuperfileCheckManager scmanager;
            scmanager.start(updateeclwatch);
            scmanager.updateStatus(true);
            if (stopped)
                return;
            scmanager.checkSuperFileLinkage();
            scmanager.updateStatus(true);
            scmanager.saveToEclWatch(stopped,byscheduler);
            scmanager.updateStatus(true);
        }
        PROGLOG(LOGPFX "%s %s",clustcsl,stopped?"Stopped":"Done");
    }

    void xrefRequest(const char *servers)
    {
        //MORE: This could still be running when the server terminates which will likely cause the thread to core
        cRunThread *thread = new cRunThread(*this,servers);
        thread->startRelease();
    }

    bool checkClusterSubmitted(StringBuffer &cname)
    {
        cname.clear();
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_LOCK_WRITE ,INFINITE);
        Owned<IPropertyTreeIterator> clusters= conn->queryRoot()->getElements("Cluster");
        ForEach(*clusters) {
            IPropertyTree &cluster = clusters->query();
            const char *status = cluster.queryProp("@status");
            if (status&&(stricmp(status,"Submitted")==0)) {
                cluster.setProp("@status","Not Found"); // prevent cycling
                const char *name = cluster.queryProp("@name");
                if (name) {
                    if (cname.length())
                        cname.append(',');
                    cname.append(name);
                }
            }
        }
        return cname.length()!=0;
    }

    void setSubmittedOk(bool on)
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/DFU/XREF",myProcessSession(),RTM_CREATE_QUERY|RTM_LOCK_WRITE ,INFINITE);
        if (conn->queryRoot()->getPropBool("@useSasha")!=on)
            conn->queryRoot()->setPropBool("@useSasha",on);
    }

    int run()
    {
        if (isContainerized())
            props.setown(getComponentConfig());
        else
        {
            props.setown(serverConfig->getPropTree("DfuXRef"));
            if (!props)
                props.setown(createPTree("DfuXRef"));
        }

        bool eclwatchprovider = true;
        if (!isContainerized()) // NB: containerized does not support xref any other way.
        {
            // eclwatchProvider sets useSasha in call to setSubmittedOk
            eclwatchprovider = props->getPropBool("@eclwatchProvider");
        }

        unsigned interval = props->getPropInt("@interval",DEFAULT_XREF_INTERVAL);
        const char *clusters = props->queryProp(isContainerized() ? "@planes" : "@clusterlist");
        StringBuffer clusttmp;
        // TODO: xref should support checking superfiles in containerized
        if (props->getPropBool("@checkSuperFiles",isContainerized()?false:true))
        {
            if (!clusters||!*clusters)
                clusters = "SuperFiles";
            else
                clusters = clusttmp.append(clusters).append(',').append("SuperFiles").str();
        }
        if (!interval)
            stopped = !eclwatchprovider;
        setSubmittedOk(eclwatchprovider);
        if (!isContainerized())
            suspendCoalescer = props->getPropBool("@suspendCoalescerDuringXref", true);
        ignorelazylost = props->getPropBool("@ignoreLazyLost",true);
        PROGLOG(LOGPFX "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2+1;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval)
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            StringBuffer cname;
            bool byscheduler=false;
            if (!eclwatchprovider||!checkClusterSubmitted(cname.clear()))
            {
                if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                    continue;
                started = (unsigned)-1;
                if (!schedule.ready())
                    continue;
                byscheduler = true;
            }
            try
            {
                runXRef(cname.length()?cname.str():clusters,true,byscheduler);
                cname.clear();
            }
            catch (IException *e)
            {
                StringBuffer s;
                EXCLOG(e, LOGPFX);
                e->Release();
            }
        }
        PROGLOG(LOGPFX "Exit");
        return 0;
    }


} *sashaXRefServer = NULL;


ISashaServer *createSashaXrefServer()
{
    assertex(!sashaXRefServer); // initialization problem
    sashaXRefServer = new CSashaXRefServer();
    return sashaXRefServer;
}

void processXRefRequest(ISashaCommand *cmd)
{
    if (sashaXRefServer) {
        StringBuffer clusterlist(cmd->queryCluster());
        // only support single cluster for the moment
        if (clusterlist.length())
            sashaXRefServer->xrefRequest(clusterlist);
    }
}



// File Expiry monitor

class CSashaExpiryServer: public ISashaServer, public Thread
{  
    bool stopped;
    Semaphore stopsem;
    Mutex runmutex;
    Owned<IUserDescriptor> udesc;
    Linked<IPropertyTree> props;

public:
    IMPLEMENT_IINTERFACE_USING(Thread);

    CSashaExpiryServer(IPropertyTree *_config)
        : Thread("CSashaExpiryServer"), props(_config)
    {
        stopped = false;

        StringBuffer userName;
#ifdef _CONTAINERIZED
        props->getProp("@user", userName);
#else
        serverConfig->getProp("@sashaUser", userName);
#endif
        udesc.setown(createUserDescriptor());
        udesc->set(userName.str(), nullptr);
    }

    ~CSashaExpiryServer()
    {
    }

    void start()
    {
        Thread::start(false);
    }

    void ready()
    {
    }
    
    void stop()
    {
        if (!stopped) {
            stopped = true;
            stopsem.signal();
        }
        synchronized block(runmutex);   // hopefully stopped should stop
        if (!join(1000*60*3))
            OERRLOG("CSashaExpiryServer aborted");
    }

    void runExpiry()
    {
        synchronized block(runmutex);
        if (stopped)
            return;
        PROGLOG(LOGPFX2 "Started");
        unsigned defaultExpireDays = props->getPropInt("@expiryDefault", DEFAULT_EXPIRYDAYS);
        unsigned defaultPersistExpireDays = props->getPropInt("@persistExpiryDefault", DEFAULT_PERSISTEXPIRYDAYS);
        StringArray expirylist;

        StringBuffer filterBuf;
        // all non-superfiles
        filterBuf.append(DFUQFTspecial).append(DFUQFilterSeparator).append(DFUQSFFileType).append(DFUQFilterSeparator).append(DFUQFFTnonsuperfileonly).append(DFUQFilterSeparator);
        // hasProp,SuperOwner,"false" - meaning not owned by a superfile
        filterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFsuperowner)).append(DFUQFilterSeparator).append("false").append(DFUQFilterSeparator);
        // hasProp,Attr/@expireDays,"true" - meaning file has @expireDays attribute
        filterBuf.append(DFUQFThasProp).append(DFUQFilterSeparator).append(getDFUQFilterFieldName(DFUQFFexpiredays)).append(DFUQFilterSeparator).append("true").append(DFUQFilterSeparator);

        std::vector<DFUQResultField> selectiveFields = {DFUQResultField::expireDays, DFUQResultField::accessed, DFUQResultField::persistent, DFUQResultField::term};

        bool allMatchingFilesReceived;
        Owned<IPropertyTreeIterator> iter = queryDistributedFileDirectory().getDFAttributesFilteredIterator(filterBuf,
            nullptr, selectiveFields.data(), udesc, true, allMatchingFilesReceived);
        ForEach(*iter)
        {
            IPropertyTree &attr=iter->query();
            if (attr.hasProp("@expireDays"))
            {
                unsigned expireDays = attr.getPropInt("@expireDays");
                const char * name = attr.queryProp("@name");
                const char *lastAccessed = attr.queryProp("@accessed");
                if (lastAccessed && name&&*name)
                {
                    if (0 == expireDays)
                    {
                        bool isPersist = attr.getPropBool("@persistent");
                        expireDays = isPersist ? defaultPersistExpireDays : defaultExpireDays;
                    }
                    CDateTime now;
                    now.setNow();
                    CDateTime expires;
                    try
                    {
                        expires.setString(lastAccessed);
                        expires.adjustTime(60*24*expireDays);
                        if (now.compare(expires,false)>0)
                        {
                            expirylist.append(name);
                            StringBuffer expiresStr;
                            expires.getString(expiresStr);
                            PROGLOG(LOGPFX2 "%s expired on %s", name, expiresStr.str());
                        }
                    }
                    catch (IException *e)
                    {
                        StringBuffer s;
                        EXCLOG(e, LOGPFX2 "setdate");
                        e->Release();
                    }
                }
            }
        }
        iter.clear();
        ForEachItemIn(i,expirylist)
        {
            if (stopped)
                break;
            const char *lfn = expirylist.item(i);
            try
            {
                /* NB: 0 timeout, meaning fail and skip, if there is any locking contention.
                 * If the file is locked, it implies it is being accessed.
                 */
                queryDistributedFileDirectory().removeEntry(lfn, udesc, NULL, 0, true);
                PROGLOG(LOGPFX2 "Deleted %s",lfn);
            }
            catch (IException *e) // may want to just detach if fails
            {
                OWARNLOG(e, LOGPFX2 "remove");
                e->Release();
            }
        }
        PROGLOG(LOGPFX2 "%s",stopped?"Stopped":"Done");
    }

    int run()
    {
        unsigned interval = props->getPropInt("@interval",DEFAULT_EXPIRY_INTERVAL);
        if (!interval)
            stopped = true;
        PROGLOG(LOGPFX2 "min interval = %d hr", interval);
        unsigned initinterval = (interval-1)/2;  // wait a bit til dali has started
        CSashaSchedule schedule;
        if (interval) 
            schedule.init(props,interval,initinterval);
        initinterval *= 60*60*1000; // ms
        unsigned started = msTick();
        while (!stopped)
        {
            stopsem.wait(1000*60);
            if (stopped)
                break;
            if (!interval||((started!=(unsigned)-1)&&(msTick()-started<initinterval)))
                continue;
            started = (unsigned)-1;
            if (!schedule.ready())
                continue;
            try {
                runExpiry();
            }
            catch (IException *e) {
                StringBuffer s;
                EXCLOG(e, LOGPFX2);
                e->Release();
            }
        }
        PROGLOG(LOGPFX2 "Exit");
        return 0;
    }


} *sashaExpiryServer = NULL;


ISashaServer *createSashaFileExpiryServer()
{
    assertex(!sashaExpiryServer); // initialization problem
#ifdef _CONTAINERIZED
    Linked<IPropertyTree> config = serverConfig;
#else
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
#endif
    sashaExpiryServer = new CSashaExpiryServer(config);
    return sashaExpiryServer;
}

void runExpiryCLI()
{
    Owned<IPropertyTree> config = serverConfig->getPropTree("DfuExpiry");
    if (!config)
        config.setown(createPTree("DfuExpiry"));
    Owned<CSashaExpiryServer> sashaExpiryServer = new CSashaExpiryServer(config);
    sashaExpiryServer->runExpiry();
}
