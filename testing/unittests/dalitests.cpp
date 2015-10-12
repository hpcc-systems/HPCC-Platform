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

/*
 * Dali Quick Regression Suite: Tests Dali functionality on a programmatic way.
 *
 * Add as much as possible here to avoid having to run the Hthor/Thor regressions
 * all the time for Dali tests, since most of it can be tested quickly from here.
 */

#ifdef _USE_CPPUNIT
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dautils.hpp"

#include "unittests.hpp"

//#define COMPAT

// ======================================================================= Support Functions / Classes

static __int64 subchangetotal;
static unsigned subchangenum;
static CriticalSection subchangesect;
static IRemoteConnection *Rconn;
static IDistributedFileDirectory & dir = queryDistributedFileDirectory();
static IUserDescriptor *user = createUserDescriptor();
static unsigned initCounter = 0; // counter for initialiser

// Declared in dadfs.cpp *only* when CPPUNIT is active
extern void removeLogical(const char *fname, IUserDescriptor *user);

void init() {
    // Only initialise on first pass
    if (initCounter != 0)
        return;
    InitModuleObjects();
    user->set("user", "passwd");
    // Connect to local Dali
    SocketEndpoint ep;
    ep.set(".", 7070);
    SocketEndpointArray epa;
    epa.append(ep);
    Owned<IGroup> group = createIGroup(epa);
    initClientProcess(group, DCR_Other);

    initCounter++;
}

void destroy() {
    // Only destroy on last pass
    if (initCounter != 0)
        return;
    // Cleanup
    releaseAtoms();
    closedownClientProcess();
    setNodeCaching(false);

    initCounter--;
}

class CCSub : public CInterface, implements ISDSConnectionSubscription, implements ISDSSubscription
{
    unsigned n;
    unsigned &count;
public:
    IMPLEMENT_IINTERFACE;

    CCSub(unsigned _n,unsigned &_count)
        : count(_count)
    {
        n = _n;
    }
    virtual void notify()
    {
        CriticalBlock block(subchangesect);
        subchangetotal += n;
        subchangenum++;
        count++;
    }
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        CriticalBlock block(subchangesect);
        subchangetotal += n;
        subchangenum++;
        subchangetotal += (unsigned)flags;
        subchangetotal += crc32(xpath,strlen(xpath),0);
        if (valueLen) {
            subchangetotal += crc32((const char *)valueData,valueLen,0);
        }
        count++;

    }

};

class CChange : public Thread
{
    Owned<IRemoteConnection> conn;
    Owned<CCSub> sub;
    StringAttr path;
    SubscriptionId id[10];
    unsigned n;
    unsigned count;

public:
    Semaphore stopsem;
    CChange(unsigned _n)
    {
        n = _n;
        StringBuffer s("/DAREGRESS/CONSUB");
        s.append(n+1);
        path.set(s.str());
        conn.setown(querySDS().connect(path, myProcessSession(), RTM_CREATE|RTM_DELETE_ON_DISCONNECT, 1000000));
        unsigned i;
        for (i=0;i<5;i++)
            id[i] = conn->subscribe(*new CCSub(n*1000+i,count));
        s.append("/testprop");
        for (;i<10;i++)
            id[i] = querySDS().subscribe(s.str(),*new CCSub(n*1000+i,count),false,true);
        count = 0;
        start();
    }

    virtual int run()
    {
        unsigned i;
        for (i = 0;i<10; i++) {
            conn->queryRoot()->setPropInt("testprop", (i*17+n*21)%100);
            conn->commit();
            for (unsigned j=0;j<1000;j++) {
                {
                    CriticalBlock block(subchangesect);
                    if (count>=(i+1)*10)
                        break;
                }
                Sleep(10);
            }
        }
        stopsem.wait();
        for (i=0;i<10;i++)
            conn->unsubscribe(id[i]);
        return 0;
    }
};

interface IChecker
{
    virtual void title(unsigned n,const char *s)=0;
    virtual void add(const char *s,__int64 v)=0;
    virtual void add(const char *s,const char* v)=0;
    virtual void add(unsigned n,const char *s,__int64 v)=0;
    virtual void add(unsigned n,const char *s,const char* v)=0;
    virtual void error(const char *txt)=0;
};

void checkFilePart(IChecker *checker,IDistributedFilePart *part,bool blocked)
{
    StringBuffer tmp;
    checker->add("getPartIndex",part->getPartIndex());
    unsigned n = part->numCopies();
    checker->add("numCopies",part->numCopies());
    checker->add("maxCopies",n);
    RemoteFilename rfn;
    for (unsigned copy=0;copy<n;copy++) {
        INode *node = part->queryNode(copy);
        if (node)
            checker->add(copy,"queryNode",node->endpoint().getUrlStr(tmp.clear()).str());
        else
            checker->error("missing node");
        checker->add(copy,"getFilename",part->getFilename(rfn,copy).getRemotePath(tmp.clear()).str());
    }
    checker->add("getPartName",part->getPartName(tmp.clear()).str());
#ifndef COMPAT
    checker->add("getPartDirectory",part->getPartDirectory(tmp.clear()).str());
#endif
    checker->add("queryProperties()",toXML(&part->queryAttributes(),tmp.clear()).str());
    checker->add("isHost",part->isHost()?1:0);
    checker->add("getFileSize",part->getFileSize(false,false));
    CDateTime dt;
    if (part->getModifiedTime(false,false,dt))
        dt.getString(tmp.clear());
    else
        tmp.clear().append("nodatetime");
    checker->add("getModifiedTime",tmp.str());
    unsigned crc;
    if (part->getCrc(crc)&&!blocked)
        checker->add("getCrc",crc);
    else
        checker->add("getCrc","nocrc");
}


void checkFile(IChecker *checker,IDistributedFile *file)
{
    StringBuffer tmp;
    checker->add("queryLogicalName",file->queryLogicalName());
    unsigned np = file->numParts();
    checker->add("numParts",np);
    checker->add("queryDefaultDir",file->queryDefaultDir());
    if (np>1)
        checker->add("queryPartMask",file->queryPartMask());
    checker->add("queryProperties()",toXML(&file->queryAttributes(),tmp.clear()).str());
    CDateTime dt;
    if (file->getModificationTime(dt))
        dt.getString(tmp.clear());
    else
        tmp.clear().append("nodatetime");

    // Owned<IFileDescriptor> desc = getFileDescriptor();
    // checkFileDescriptor(checker,desc);

    //virtual bool existsPhysicalPartFiles(unsigned short port) = 0;                // returns true if physical patrs all exist (on primary OR secondary)

    //Owned<IPropertyTree> tree = getTreeCopy();
    //checker->add("queryProperties()",toXML(tree,tmp.clear()).str());

    checker->add("getFileSize",file->getFileSize(false,false));
    bool blocked;
    checker->add("isCompressed",file->isCompressed(&blocked)?1:0);
    checker->add("blocked",blocked?1:0);
    unsigned csum;
    if (file->getFileCheckSum(csum)&&!blocked)
        checker->add("getFileCheckSum",csum);
    else
        checker->add("getFileCheckSum","nochecksum");
    StringBuffer clustname;
    checker->add("queryClusterName(0)",file->getClusterName(0,clustname).str());
    for (unsigned i=0;i<np;i++) {
        Owned<IDistributedFilePart> part = file->getPart(i);
        if (part)
            checkFilePart(checker,part,blocked);
    }
}

void checkFiles(const char *fn)
{
    class cChecker: implements IChecker
    {
    public:
        virtual void title(unsigned n,const char *s)
        {
            printf("Title[%d]='%s'\n",n,s);
        }
        virtual void add(const char *s,__int64 v)
        {
            printf("%s=%" I64F "d\n",s,v);
        }
        virtual void add(const char *s,const char* v)
        {
            printf("%s='%s'\n",s,v);
        }
        virtual void add(unsigned n,const char *s,__int64 v)
        {
            printf("%s[%d]=%" I64F "d\n",s,n,v);
        }
        virtual void add(unsigned n,const char *s,const char* v)
        {
            printf("%s[%d]='%s'\n",s,n,v);
        }
        virtual void error(const char *txt)
        {
            printf("ERROR '%s'\n",txt);
        }
    } checker;
    unsigned start = msTick();
    unsigned slowest = 0;
    StringAttr slowname;
    if (fn) {
        checker.title(1,fn);
        try {
            Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(fn,user);
            if (!file)
                printf("file '%s' not found\n",fn);
            else
                checkFile(&checker,file);
        }
        catch (IException *e) {
            StringBuffer str;
            e->errorMessage(str);
            e->Release();
            checker.error(str.str());
        }
    }
    else {
        Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false,user);
        unsigned i=0;
        unsigned ss = msTick();
        ForEach(*iter) {
            i++;
            StringBuffer lfname;
            iter->getName(lfname);
            checker.title(i,lfname.str());
            try {
                IDistributedFile &file=iter->query();
                checkFile(&checker,&file);
                unsigned t = (msTick()-ss);
                if (t>slowest) {
                    slowest = t;
                    slowname.set(lfname.str());
                }
            }
            catch (IException *e) {
                StringBuffer str;
                e->errorMessage(str);
                e->Release();
                checker.error(str.str());
            }
            ss = msTick();
        }
    }
    unsigned t = msTick()-start;
    printf("Complete in %ds\n",t/1000);
    if (!slowname.isEmpty())
        printf("Slowest %s = %dms\n",slowname.get(),slowest);
};

const char *filelist=
    "thor_data400::gong_delete_plus,"
    "thor_data400::in::npanxx,"
    "thor_data400::tpm_deduped,"
    "thor_data400::base::movers_ingest_ll,"
    "thor_hank::cemtemp::fldl,"
    "thor_data400::patch,"
    "thor_data400::in::flvehreg_01_prethor_upd200204_v3,"
    "thor_data400::in::flvehreg_01_prethor_upd20020625_v3_flag,"
    "thor_data400::in::flvehreg_01_prethor_upd20020715_v3,"
    "thor_data400::in::flvehreg_01_prethor_upd20020715_v3_v3_flag,"
    "thor_data400::in::flvehreg_01_prethor_upd20020816_v3,"
    "thor_data400::in::flvehreg_01_prethor_upd20020816_v3_flag,"
    "thor_data400::in::flvehreg_01_prethor_upd20020625_v3,"
    "thor_data400::in::fl_lic_prethor_200208v2,"
    "thor_data400::in::fl_lic_prethor_200209,"
    "thor_data400::in::fl_lic_prethor_200210,"
    "thor_data400::in::fl_lic_prethor_200210_reclean,"
    "thor_data400::in::fl_lic_upd_200301,"
    "thor_data400::in::fl_lic_upd_200302,"
    "thor_data400::in::oh_lic_200209,"
    "thor_data400::in::ohio_lic_upd_200210,"
    "thor_data400::prepped_for_keys,"
    "a0e65__w20060224-155748,"
    "common_did,"
    "test::ftest1,"
    "thor_data50::BASE::chunk,"
    "hthor::key::codes_v320040901,"
    "thor_data400::in::ucc_direct_ok_99999999_event_20060124,"
    "thor400::ks_work::distancedetails";

#ifndef COMPAT

void dispFDesc(IFileDescriptor *fdesc)
{
    printf("======================================\n");
    Owned<IPropertyTree> pt = createPTree("File");
    fdesc->serializeTree(*pt);
    StringBuffer out;
    toXML(pt,out);
    printf("%s\n",out.str());
    Owned<IFileDescriptor> fdesc2 = deserializeFileDescriptorTree(pt);
    toXML(pt,out.clear());
    printf("%s\n",out.str());
    unsigned np = fdesc->numParts();
    unsigned ncl = fdesc->numClusters();
    printf("numclusters = %d, numparts=%d\n",ncl,np);
    for (unsigned pass=0;pass<1;pass++) {
        for (unsigned ip=0;ip<np;ip++) {
            IPartDescriptor *part = fdesc->queryPart(ip);
            unsigned nc = part->numCopies();
            for (unsigned ic=0;ic<nc;ic++) {
                StringBuffer tmp1;
                StringBuffer tmp2;
                StringBuffer tmp3;
                StringBuffer tmp4;
                RemoteFilename rfn;
                bool blocked;
                out.clear().appendf("%d,%d: '%s' '%s' '%s' '%s' '%s' '%s' %s%s%s",ip,ic,
                    part->getDirectory(tmp1,ic).str(),
                    part->getTail(tmp2).str(),
                    part->getPath(tmp3,ic).str(),
                    fdesc->getFilename(ip,ic,rfn).getRemotePath(tmp4).str(),  // multi TBD
                    fdesc->queryPartMask()?fdesc->queryPartMask():"",
                    fdesc->queryDefaultDir()?fdesc->queryDefaultDir():"",
                    fdesc->isGrouped()?"GROUPED ":"",
                    fdesc->queryKind()?fdesc->queryKind():"",
                    fdesc->isCompressed(&blocked)?(blocked?" BLOCKCOMPRESSED":" COMPRESSED"):""
                );
                printf("%s\n",out.str());
                if (1) {
                    MemoryBuffer mb;
                    part->serialize(mb);
                    Owned<IPartDescriptor> copypart;
                    copypart.setown(deserializePartFileDescriptor(mb));
                    StringBuffer out2;
                    out2.appendf("%d,%d: '%s' '%s' '%s' '%s' '%s' '%s' %s%s%s",ip,ic,
                        copypart->getDirectory(tmp1.clear(),ic).str(),
                        copypart->getTail(tmp2.clear()).str(),
                        copypart->getPath(tmp3.clear(),ic).str(),
                        copypart->getFilename(ic,rfn).getRemotePath(tmp4.clear()).str(),  // multi TBD
                        copypart->queryOwner().queryPartMask()?copypart->queryOwner().queryPartMask():"",
                        copypart->queryOwner().queryDefaultDir()?copypart->queryOwner().queryDefaultDir():"",
                        copypart->queryOwner().isGrouped()?"GROUPED ":"",
                        copypart->queryOwner().queryKind()?copypart->queryOwner().queryKind():"",
                        copypart->queryOwner().isCompressed(&blocked)?(blocked?" BLOCKCOMPRESSED":" COMPRESSED"):""
                    );
                    if (strcmp(out.str(),out2.str())!=0)
                        printf("FAILED!\n%s\n%s\n",out.str(),out2.str());
                    pt.setown(createPTree("File"));
                    copypart->queryOwner().serializeTree(*pt);
                    StringBuffer out;
                    toXML(pt,out);
                //  printf("%d,%d: \n%s\n",ip,ic,out.str());

                }
            }
        }
    }
}

#endif

// ================================================================================== UNIT TESTS

class CDaliTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( CDaliTests );
        CPPUNIT_TEST(testDFS);
//        CPPUNIT_TEST(testReadAllSDS); // Ignoring this test; See comments below
        CPPUNIT_TEST(testSDSRW);
        CPPUNIT_TEST(testSDSSubs);
        CPPUNIT_TEST(testSDSSubs2);
        CPPUNIT_TEST(testFiles);
        CPPUNIT_TEST(testGroups);
        CPPUNIT_TEST(testMultiCluster);
#ifndef COMPAT
        CPPUNIT_TEST(testDF1);
        CPPUNIT_TEST(testDF2);
        CPPUNIT_TEST(testMisc);
        CPPUNIT_TEST(testDFile);
#endif
        CPPUNIT_TEST(testDFSTrans);
        CPPUNIT_TEST(testDFSPromote);
        CPPUNIT_TEST(testDFSDel);
        CPPUNIT_TEST(testDFSRename);
        CPPUNIT_TEST(testDFSClearAdd);
        CPPUNIT_TEST(testDFSRename2);
        CPPUNIT_TEST(testDFSRenameThenDelete);
        CPPUNIT_TEST(testDFSRemoveSuperSub);
// This test requires access to an external IP with dafilesrv running
//        CPPUNIT_TEST(testDFSRename3);
        CPPUNIT_TEST(testDFSAddFailReAdd);
        CPPUNIT_TEST(testDFSRetrySuperLock);
        CPPUNIT_TEST(testDFSHammer);
        CPPUNIT_TEST(testSDSNodeSubs);
    CPPUNIT_TEST_SUITE_END();

#ifndef COMPAT


#endif

    void testGrp(SocketEndpointArray &epa)
    {
        Owned<IGroup> grp = createIGroup(epa);
        StringBuffer s;
        grp->getText(s);
        printf("'%s'\n",s.str());
        Owned<IGroup> grp2 = createIGroup(s.str());
        if (grp->compare(grp2)!=GRidentical) {
            grp->getText(s.clear());
            printf("^FAILED! %s\n",s.str());
        }
    }

    unsigned fn(unsigned n, unsigned m, unsigned seed, unsigned depth, IPropertyTree *parent)
    {
        __int64 val = parent->getPropInt64("val",0);
        parent->setPropInt64("val",n+val);
        val = parent->getPropInt64("@val",0);
        parent->setPropInt64("@val",m+val);
        val = parent->getPropInt64(NULL,0);
        parent->setPropInt64(NULL,seed+val);
        if (Rconn&&((n+m+seed)%100==0))
            Rconn->commit();
        if (!seed)
            return m+n;
        if (n==m)
            return seed;
        if (depth>10)
            return seed+n+m;
        if (seed%7==n%7)
            return n;
        if (seed%7==m%7)
            return m;
        char name[64];
        unsigned v = seed;
        name[0] = 's';
        name[1] = 'u';
        name[2] = 'b';
        unsigned i = 3;
        while (v) {
            name[i++] = ('A'+v%26 );
            v /= 26;
        }
        name[i] = 0;
        IPropertyTree *child = parent->queryPropTree(name);
        if (!child)
            child = parent->addPropTree(name, createPTree(name));
        return fn(fn(n,seed,seed*17+11,depth+1,child),fn(seed,m,seed*11+17,depth+1,child),seed*19+7,depth+1,child);
    }

    unsigned fn2(unsigned n, unsigned m, unsigned seed, unsigned depth, StringBuffer &parentname)
    {
        if (!Rconn)
            return 0;
        if ((n+m+seed)%25==0) {
            Rconn->commit();
            Rconn->Release();
            Rconn = querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000);
            ASSERT(Rconn && "Failed to connect to /DAREGRESS");
        }
        IPropertyTree *parent = parentname.length()?Rconn->queryRoot()->queryPropTree(parentname.str()):Rconn->queryRoot();
        ASSERT(parent && "Failed to connect to parent");
        __int64 val = parent->getPropInt64("val",0);
        parent->setPropInt64("val",n+val);
        val = parent->getPropInt64("@val",0);
        parent->setPropInt64("@val",m+val);
        val = parent->getPropInt64(NULL,0);
        parent->setPropInt64(NULL,seed+val);
        if (!seed)
            return m+n;
        if (n==m)
            return seed;
        if (depth>10)
            return seed+n+m;
        if (seed%7==n%7)
            return n;
        if (seed%7==m%7)
            return m;
        char name[64];
        unsigned v = seed;
        name[0] = 's';
        name[1] = 'u';
        name[2] = 'b';
        unsigned i = 3;
        while (v) {
            name[i++] = ('A'+v%26 );
            v /= 26;
        }
        name[i] = 0;
        unsigned l = parentname.length();
        if (parentname.length())
            parentname.append('/');
        parentname.append(name);
        IPropertyTree *child = parent->queryPropTree(name);
        if (!child)
            child = parent->addPropTree(name, createPTree(name));
        unsigned ret = fn2(fn2(n,seed,seed*17+11,depth+1,parentname),fn2(seed,m,seed*11+17,depth+1,parentname),seed*19+7,depth+1,parentname);
        parentname.setLength(l);
        return ret;
    }

    IFileDescriptor *createDescriptor(const char* dir, const char* name, unsigned parts, unsigned recSize, unsigned index=0)
    {
        Owned<IPropertyTree> pp = createPTree("Part");
        Owned<IFileDescriptor>fdesc = createFileDescriptor();
        fdesc->setDefaultDir(dir);
        StringBuffer s;
        SocketEndpoint ep;
        ep.setLocalHost(0);
        StringBuffer ip;
        ep.getIpText(ip);
        for (unsigned k=0;k<parts;k++) {
            s.clear().append(ip);
            Owned<INode> node = createINode(s.str());
            pp->setPropInt64("@size",recSize);
            s.clear().append(name);
            if (index)
                s.append(index);
            s.append("._").append(k+1).append("_of_").append(parts);
            fdesc->setPart(k,node,s.str(),pp);
        }
        fdesc->queryProperties().setPropInt("@recordSize",recSize);
        fdesc->setDefaultDir(dir);
        return fdesc.getClear();
    }

    void setupDFS(const char *scope, unsigned supersToDel=3, unsigned subsToCreate=4)
    {
        StringBuffer bufScope;
        bufScope.append("regress::").append(scope);
        StringBuffer bufDir;
        bufDir.append("regress/").append(scope);

        logctx.CTXLOG("Cleaning up '%s' scope", bufScope.str());
        for (unsigned i=1; i<=supersToDel; i++) {
            StringBuffer super = bufScope;
            super.append("::super").append(i);
            if (dir.exists(super.str(),user,false,true))
                ASSERT(dir.removeEntry(super.str(), user) && "Can't remove super-file");
        }

        logctx.CTXLOG("Creating 'regress::trans' subfiles(1,%d)", subsToCreate);
        for (unsigned i=1; i<=subsToCreate; i++) {
            StringBuffer name;
            name.append("sub").append(i);
            StringBuffer sub = bufScope;
            sub.append("::").append(name);

            // Remove first
            if (dir.exists(sub.str(),user,true,false))
                ASSERT(dir.removeEntry(sub.str(), user) && "Can't remove sub-file");

            try {
                // Create the sub file with an arbitrary format
                Owned<IFileDescriptor> subd = createDescriptor(bufDir.str(), name.str(), 1, 17);
                Owned<IPartDescriptor> partd = subd->getPart(0);
                RemoteFilename rfn;
                partd->getFilename(0, rfn);
                StringBuffer fname;
                rfn.getPath(fname);
                recursiveCreateDirectoryForFile(fname.str());
                OwnedIFile ifile = createIFile(fname.str());
                Owned<IFileIO> io;
                io.setown(ifile->open(IFOcreate));
                io->write(0, 17, "12345678901234567");
                io->close();
                Owned<IDistributedFile> dsub = dir.createNew(subd, sub.str());  // GH->JCS second parameter is wrong
                dsub->attach(sub.str(),user);
            } catch (IException *e) {
                StringBuffer msg;
                e->errorMessage(msg);
                logctx.CTXLOG("Caught exception while creating file in DFS: %s", msg.str());
                e->Release();
                ASSERT(0 && "Exception Caught in setupDFS - is the directory writeable by this user?");
            }

            // Make sure it got created
            ASSERT(dir.exists(sub.str(),user,true,false) && "Can't add physical files");
        }
    }

    void testReadBranch(const char *path)
    {
        PROGLOG("Connecting to %s",path);
        Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_READ, 10000);
        ASSERT(conn && "Could not connect");
        IPropertyTree *root = conn->queryRoot();
        Owned<IAttributeIterator> aiter = root->getAttributes();
        StringBuffer s;
        ForEach(*aiter)
            aiter->getValue(s.clear());
        aiter.clear();
        root->getProp(NULL,s.clear());
        Owned<IPropertyTreeIterator> iter = root->getElements("*");
        StringAttrArray children;
        UnsignedArray childidx;
        ForEach(*iter) {
            children.append(*new StringAttrItem(iter->query().queryName()));
            childidx.append(root->queryChildIndex(&iter->query()));
        }
        iter.clear();
        conn.clear();
        ForEachItemIn(i,children) {
            s.clear().append(path);
            if (path[strlen(path)-1]!='/')
                s.append('/');
            s.append(children.item(i).text).append('[').append(childidx.item(i)+1).append(']');
            testReadBranch(s.str());
        }
    }

    const IContextLogger &logctx;

public:
    CDaliTests() : logctx(queryDummyContextLogger()) {
        init();
    }

    ~CDaliTests() {
        destroy();
    }

    void testSDSRW()
    {
        Owned<IPropertyTree> ref = createPTree("DAREGRESS");
        fn(1,2,3,0,ref);
        StringBuffer refstr;
        toXML(ref,refstr,0,XML_SortTags|XML_Format);
        logctx.CTXLOG("Created reference size %d",refstr.length());
        Owned<IRemoteConnection> conn = querySDS().connect("/DAREGRESS",myProcessSession(), RTM_CREATE, 1000000);
        Rconn = conn;
        IPropertyTree *root = conn->queryRoot();
        fn(1,2,3,0,root);
        conn.clear();
        logctx.CTXLOG("Created test branch 1");
        conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), RTM_DELETE_ON_DISCONNECT, 1000000));
        root = conn->queryRoot();
        StringBuffer s;
        toXML(root,s,0,XML_SortTags|XML_Format);
        ASSERT(strcmp(s.str(),refstr.str())==0 && "Branch 1 does not match");
        conn.clear();
        conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000));
        ASSERT(!conn && "RTM_DELETE_ON_DISCONNECT failed");
        Rconn = querySDS().connect("/DAREGRESS",myProcessSession(), RTM_CREATE, 1000000);
        StringBuffer pn;
        fn2(1,2,3,0,pn);
        ::Release(Rconn);
        logctx.CTXLOG("Created test branch 2");
        Rconn = NULL;
        conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), RTM_DELETE_ON_DISCONNECT, 1000000));
        root = conn->queryRoot();
        toXML(root,s.clear(),0,XML_SortTags|XML_Format);
        ASSERT(strcmp(s.str(),refstr.str())==0 && "Branch 2 does not match");
        conn.clear();
        conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000));
        ASSERT(!conn && "RTM_DELETE_ON_DISCONNECT failed");
    }

    /*
     * This test is invasive, obsolete and the main source of
     * errors in the DFS code. It was created on a time where
     * the DFS API was spread open and methods could openly
     * fiddle with its internals without injury. Times have changed.
     *
     * TODO: Convert this test into a proper test of the DFS as
     * it currently stands, not work around its deficiencies.
     *
     * Unfortunately, to do that, some functionality has to be
     * re-worked (like creating groups, adding files to it,
     * creating physical temporary files, etc).
     */
    void testDFS()
    {
        const size32_t recsize = 17;
        StringBuffer s;
        unsigned i;
        unsigned n;
        unsigned t;
        queryNamedGroupStore().remove("daregress_group");
        dir.removeEntry("daregress::superfile1", user);
        SocketEndpointArray epa;
        for (n=0;n<400;n++) {
            s.clear().append("192.168.").append(n/256).append('.').append(n%256);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        Owned<IGroup> group = createIGroup(epa);
        queryNamedGroupStore().add("daregress_group",group,true);
        ASSERT(queryNamedGroupStore().find(group,s.clear()) && "Created logical group not found");
        ASSERT(stricmp(s.str(),"daregress_group")==0 && "Created logical group found with wrong name");
        group.setown(queryNamedGroupStore().lookup("daregress_group"));
        ASSERT(group && "named group lookup failed");
        logctx.CTXLOG("Named group created    - 400 nodes");
        for (i=0;i<100;i++) {
            Owned<IPropertyTree> pp = createPTree("Part");
            Owned<IFileDescriptor>fdesc = createFileDescriptor();
            fdesc->setDefaultDir("thordata/regress");
            n = 9;
            for (unsigned k=0;k<400;k++) {
                s.clear().append("192.168.").append(n/256).append('.').append(n%256);
                Owned<INode> node = createINode(s.str());
                pp->setPropInt64("@size",(n*777+i)*recsize);
                s.clear().append("daregress_test").append(i).append("._").append(n+1).append("_of_400");
                fdesc->setPart(n,node,s.str(),pp);
                n = (n+9)%400;
            }
            fdesc->queryProperties().setPropInt("@recordSize",17);
            s.clear().append("daregress::test").append(i);
            removeLogical(s.str(), user);
            StringBuffer cname;
            Owned<IDistributedFile> dfile = dir.createNew(fdesc);
            ASSERT(stricmp(dfile->getClusterName(0,cname),"daregress_group")==0 && "Cluster name wrong");
            s.clear().append("daregress::test").append(i);
            dfile->attach(s.str(),user);
        }
        logctx.CTXLOG("DFile create done      - 100 files");
        unsigned samples = 5;
        t = 33;
        for (i=0;i<100;i++) {
            s.clear().append("daregress::test").append(t);
            ASSERT(dir.exists(s.str(),user) && "Could not find sub-file");
            Owned<IDistributedFile> dfile = dir.lookup(s.str(), user);
            ASSERT(dfile && "Could not find sub-file");
            offset_t totsz = 0;
            n = 11;
            for (unsigned k=0;k<400;k++) {
                Owned<IDistributedFilePart> part = dfile->getPart(n);
                ASSERT(part && "part not found");
                s.clear().append("192.168.").append(n/256).append('.').append(n%256);
                Owned<INode> node = createINode(s.str());
                ASSERT(node->equals(part->queryNode()) && "part node mismatch");
                ASSERT(part->getFileSize(false,false)==(n*777+t)*recsize && "size node mismatch");
                s.clear().append("daregress_test").append(t).append("._").append(n+1).append("_of_400");
    /* ** TBD
                if (stricmp(s.str(),part->queryPartName())!=0)
                    ERROR4("part name mismatch %d, %d '%s' '%s'",t,n,s.str(),part->queryPartName());
    */
                totsz += (n*777+t)*recsize;
                if ((samples>0)&&(i+n+t==k)) {
                    samples--;
                    RemoteFilename rfn;
                    part->getFilename(rfn,samples%2);
                    StringBuffer fn;
                    rfn.getRemotePath(fn);
                    logctx.CTXLOG("SAMPLE: %d,%d %s",t,n,fn.str());
                }
                n = (n+11)%400;
            }
            ASSERT(totsz==dfile->getFileSize(false,false) && "total size mismatch");
            t = (t+33)%100;
        }
        logctx.CTXLOG("DFile lookup done      - 100 files");

        // check iteration
        __int64 crctot = 0;
        unsigned np = 0;
        unsigned totrows = 0;
        Owned<IDistributedFileIterator> fiter = dir.getIterator("daregress::*",false, user);
        Owned<IDistributedFilePartIterator> piter;
        ForEach(*fiter) {
            piter.setown(fiter->query().getIterator());
            ForEach(*piter) {
                RemoteFilename rfn;
                StringBuffer s;
                piter->query().getFilename(rfn,0);
                rfn.getRemotePath(s);
                piter->query().getFilename(rfn,1);
                rfn.getRemotePath(s);
                crctot += crc32(s.str(),s.length(),0);
                np++;
                totrows += (unsigned)(piter->query().getFileSize(false,false)/fiter->query().queryAttributes().getPropInt("@recordSize",-1));
            }
        }
        piter.clear();
        fiter.clear();
        logctx.CTXLOG("DFile iterate done     - %d parts, %d rows, CRC sum %" I64F "d",np,totrows,crctot);
        Owned<IDistributedSuperFile> sfile;
        sfile.setown(dir.createSuperFile("daregress::superfile1",user,true,false));
        for (i = 0;i<100;i++) {
            s.clear().append("daregress::test").append(i);
            sfile->addSubFile(s.str());
        }
        sfile.clear();
        sfile.setown(dir.lookupSuperFile("daregress::superfile1", user));
        ASSERT(sfile && "Could not find added superfile");
        __int64 savcrc = crctot;
        crctot = 0;
        np = 0;
        totrows = 0;
        size32_t srs = (size32_t)sfile->queryAttributes().getPropInt("@recordSize",-1);
        ASSERT(srs==17 && "Superfile does not match subfile row size");
        piter.setown(sfile->getIterator());
        ForEach(*piter) {
            RemoteFilename rfn;
            StringBuffer s;
            piter->query().getFilename(rfn,0);
            rfn.getRemotePath(s);
            piter->query().getFilename(rfn,1);
            rfn.getRemotePath(s);
            crctot += crc32(s.str(),s.length(),0);
            np++;
            totrows += (unsigned)(piter->query().getFileSize(false,false)/srs);
        }
        piter.clear();
        logctx.CTXLOG("Superfile iterate done - %d parts, %d rows, CRC sum %" I64F "d",np,totrows,crctot);
        ASSERT(crctot==savcrc && "SuperFile does not match sub files");
        unsigned tr = (unsigned)(sfile->getFileSize(false,false)/srs);
        ASSERT(totrows==tr && "Superfile size does not match part sum");
        sfile->detach();
        sfile.clear();
        sfile.setown(dir.lookupSuperFile("daregress::superfile1",user));
        ASSERT(!sfile && "Superfile deletion failed");
        t = 37;
        for (i=0;i<100;i++) {
            s.clear().append("daregress::test").append(t);
            removeLogical(s.str(), user);
            t = (t+37)%100;
        }
        logctx.CTXLOG("DFile removal complete");
        t = 39;
        for (i=0;i<100;i++) {
            ASSERT(!dir.exists(s.str(),user) && "Found dir after deletion");
            Owned<IDistributedFile> dfile = dir.lookup(s.str(), user);
            ASSERT(!dfile && "Found file after deletion");
            t = (t+39)%100;
        }
        logctx.CTXLOG("DFile removal check complete");
        queryNamedGroupStore().remove("daregress_group");
        ASSERT(!queryNamedGroupStore().lookup("daregress_group") && "Named group not removed");
    }

    void testDFSTrans()
    {
        setupDFS("trans");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);

        // Auto-commit
        logctx.CTXLOG("Auto-commit test (inactive transaction)");
        Owned<IDistributedSuperFile> sfile1 = dir.createSuperFile("regress::trans::super1",user , false, false, transaction);
        sfile1->addSubFile("regress::trans::sub1", false, NULL, false, transaction);
        sfile1->addSubFile("regress::trans::sub2", false, NULL, false, transaction);
        sfile1.clear();
        sfile1.setown(dir.lookupSuperFile("regress::trans::super1", user, transaction));
        ASSERT(sfile1.get() && "non-transactional add super1 failed");
        ASSERT(sfile1->numSubFiles() == 2 && "auto-commit add sub failed, not all subs were added");
        ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub1") == 0 && "auto-commit add sub failed, wrong name for sub1");
        ASSERT(strcmp(sfile1->querySubFile(1).queryLogicalName(), "regress::trans::sub2") == 0 && "auto-commit add sub failed, wrong name for sub2");
        sfile1.clear();

        // Rollback
        logctx.CTXLOG("Rollback test (active transaction)");
        transaction->start();
        Owned<IDistributedSuperFile> sfile2 = dir.createSuperFile("regress::trans::super2", user, false, false, transaction);
        sfile2->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
        sfile2->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
        transaction->rollback();
        ASSERT(sfile2->numSubFiles() == 0 && "transactional rollback failed, some subs were added");
        sfile2.clear();
        sfile2.setown(dir.lookupSuperFile("regress::trans::super2", user, transaction));
        ASSERT(!sfile2.get() && "transactional rollback super2 failed, it exists!");

        // Commit
        logctx.CTXLOG("Commit test (active transaction)");
        transaction->start();
        Owned<IDistributedSuperFile> sfile3 = dir.createSuperFile("regress::trans::super3", user, false, false, transaction);
        sfile3->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
        sfile3->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
        transaction->commit();
        sfile3.clear();
        sfile3.setown(dir.lookupSuperFile("regress::trans::super3", user, transaction));
        ASSERT(sfile3.get() && "transactional add super3 failed");
        ASSERT(sfile3->numSubFiles() == 2 && "transactional add sub failed, not all subs were added");
        ASSERT(strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub3") == 0 && "transactional add sub failed, wrong name for sub3");
        ASSERT(strcmp(sfile3->querySubFile(1).queryLogicalName(), "regress::trans::sub4") == 0 && "transactional add sub failed, wrong name for sub4");
        sfile3.clear();
    }

    void testDFSPromote()
    {
        setupDFS("trans");

        unsigned timeout = 1000; // 1s

        /* Make the meta info of one of the subfiles mismatch the rest, as subfiles are promoted through
         * the super files, this should _not_ cause an issue, as no single super file will contain
         * mismatched subfiles.
        */
        Owned<IDistributedFile> sub1 = dir.lookup("regress::trans::sub1", user, false, false, false, NULL, timeout);
        assertex(sub1);
        sub1->lockProperties();
        sub1->queryAttributes().setPropBool("@local", true);
        sub1->unlockProperties();
        sub1.clear();

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);

        // ===============================================================================
        // Don't change these parameters, or you'll have to change all ERROR tests below
        const char *sfnames[3] = {
            "regress::trans::super1", "regress::trans::super2", "regress::trans::super3"
        };
        bool delsub = false;
        bool createonlyone = true;
        // ===============================================================================
        StringArray outlinked;

        logctx.CTXLOG("Promote (1, -, -) - first iteration");
        dir.promoteSuperFiles(3, sfnames, "regress::trans::sub1", delsub, createonlyone, user, timeout, outlinked);
        {
            Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
            ASSERT(sfile1.get() && "promote failed, super1 doesn't exist");
            ASSERT(sfile1->numSubFiles() == 1 && "promote failed, super1 should have one subfile");
            ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub1") == 0 && "promote failed, wrong name for sub1");
            Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
            ASSERT(!sfile2.get() && "promote failed, super2 does exist");
            ASSERT(outlinked.length() == 0 && "promote failed, outlinked expected empty");
        }

        logctx.CTXLOG("Promote (2, 1, -) - second iteration");
        dir.promoteSuperFiles(3, sfnames, "regress::trans::sub2", delsub, createonlyone, user, timeout, outlinked);
        {
            Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
            ASSERT(sfile1.get() && "promote failed, super1 doesn't exist");
            ASSERT(sfile1->numSubFiles() == 1 && "promote failed, super1 should have one subfile");
            ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub2") == 0 && "promote failed, wrong name for sub2");
            Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
            ASSERT(sfile2.get() && "promote failed, super2 doesn't exist");
            ASSERT(sfile2->numSubFiles() == 1 && "promote failed, super2 should have one subfile");
            ASSERT(strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub1") == 0 && "promote failed, wrong name for sub1");
            Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
            ASSERT(!sfile3.get() && "promote failed, super3 does exist");
            ASSERT(outlinked.length() == 0 && "promote failed, outlinked expected empty");
        }

        logctx.CTXLOG("Promote (3, 2, 1) - third iteration");
        dir.promoteSuperFiles(3, sfnames, "regress::trans::sub3", delsub, createonlyone, user, timeout, outlinked);
        {
            Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
            ASSERT(sfile1.get() &&* "promote failed, super1 doesn't exist");
            ASSERT(sfile1->numSubFiles() == 1 && "promote failed, super1 should have one subfile");
            ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub3") == 0 && "promote failed, wrong name for sub3");
            Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
            ASSERT(sfile2.get() && "promote failed, super2 doesn't exist");
            ASSERT(sfile2->numSubFiles() == 1 && "promote failed, super2 should have one subfile");
            ASSERT(strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub2") == 0 && "promote failed, wrong name for sub2");
            Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
            ASSERT(sfile3.get() && "promote failed, super3 doesn't exist");
            ASSERT(sfile3->numSubFiles() == 1 && "promote failed, super3 should have one subfile");
            ASSERT(strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub1") == 0 && "promote failed, wrong name for sub1");
            ASSERT(outlinked.length() == 0 && "promote failed, outlinked expected empty");
        }

        logctx.CTXLOG("Promote (4, 3, 2) - fourth iteration, expect outlinked");
        dir.promoteSuperFiles(3, sfnames, "regress::trans::sub4", delsub, createonlyone, user, timeout, outlinked);
        {
            Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
            ASSERT(sfile1.get() && "promote failed, super1 doesn't exist");
            ASSERT(sfile1->numSubFiles() == 1 && "promote failed, super1 should have one subfile");
            ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub4") == 0 && "promote failed, wrong name for sub4");
            Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
            ASSERT(sfile2.get() && "promote failed, super2 doesn't exist");
            ASSERT(sfile2->numSubFiles() == 1 && "promote failed, super2 should have one subfile");
            ASSERT(strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub3") == 0 && "promote failed, wrong name for sub3");
            Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
            ASSERT(sfile3.get() && "promote failed, super3 doesn't exist");
            ASSERT(sfile3->numSubFiles() == 1 && "promote failed, super3 should have one subfile");
            ASSERT(strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub2") == 0 && "promote failed, wrong name for sub2");
            ASSERT(outlinked.length() == 1 && "promote failed, outlinked expected only one item");
            ASSERT(strcmp(outlinked.popGet(), "regress::trans::sub1") == 0 && "promote failed, outlinked expected to be sub1");
            Owned<IDistributedFile> sub1 = dir.lookup("regress::trans::sub1", user, false, false, false, NULL, timeout);
            ASSERT(sub1.get() && "promote failed, sub1 was physically deleted");
        }

        logctx.CTXLOG("Promote ([2,3], 4, 3) - fifth iteration, two in-files");
        dir.promoteSuperFiles(3, sfnames, "regress::trans::sub2,regress::trans::sub3", delsub, createonlyone, user, timeout, outlinked);
        {
            Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
            ASSERT(sfile1.get() && "promote failed, super1 doesn't exist");
            ASSERT(sfile1->numSubFiles() == 2 && "promote failed, super1 should have two subfiles");
            ASSERT(strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub2") == 0 && "promote failed, wrong name for sub1");
            ASSERT(strcmp(sfile1->querySubFile(1).queryLogicalName(), "regress::trans::sub3") == 0 && "promote failed, wrong name for sub2");
            Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
            ASSERT(sfile2.get() && "promote failed, super2 doesn't exist");
            ASSERT(sfile2->numSubFiles() == 1 && "promote failed, super2 should have one subfile");
            ASSERT(strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub4") == 0 && "promote failed, wrong name for sub4");
            Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
            ASSERT(sfile3.get() && "promote failed, super3 doesn't exist");
            ASSERT(sfile3->numSubFiles() == 1 && "promote failed, super3 should have one subfile");
            ASSERT(strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub3") == 0 && "promote failed, wrong name for sub3");
            ASSERT(outlinked.length() == 1 && "promote failed, outlinked expected only one item");
            ASSERT(strcmp(outlinked.popGet(), "regress::trans::sub2") == 0 && "promote failed, outlinked expected to be sub2");
            Owned<IDistributedFile> sub1 = dir.lookup("regress::trans::sub1", user, false, false, false, NULL, timeout);
            ASSERT(sub1.get() && "promote failed, sub1 was physically deleted");
            Owned<IDistributedFile> sub2 = dir.lookup("regress::trans::sub2", user, false, false, false, NULL, timeout);
            ASSERT(sub2.get() && "promote failed, sub2 was physically deleted");
        }
    }

    void testSDSSubs()
    {
        subchangenum = 0;
        subchangetotal = 0;
        IArrayOf<CChange> a;
        for (unsigned i=0; i<10 ; i++)
            a.append(*new CChange(i));
        unsigned last = 0;
        loop {
            Sleep(1000);
            {
                CriticalBlock block(subchangesect);
                if (subchangenum==last)
                    break;
                last = subchangenum;
            }
        }
        ForEachItemIn(i1, a)
            a.item(i1).stopsem.signal();
        ForEachItemIn(i2, a)
            a.item(i2).join();
        ASSERT(subchangenum==1000 && "Not all notifications received");
        logctx.CTXLOG("%d subscription notifications, check sum = %" I64F "d",subchangenum,subchangetotal);
    }

    class CNodeSubCommitThread : public CInterface, implements IThreaded
    {
        StringAttr xpath;
        bool finalDelete;
        CThreaded threaded;
    public:
        IMPLEMENT_IINTERFACE;

        CNodeSubCommitThread(const char *_xpath, bool _finalDelete) : threaded("CNodeSubCommitThread"), xpath(_xpath), finalDelete(_finalDelete)
        {
        }
        virtual void main()
        {
            unsigned mode = RTM_LOCK_WRITE;
            if (finalDelete)
                mode |= RTM_DELETE_ON_DISCONNECT;
            Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), mode, 1000000);
            assertex(conn);
            for (unsigned i=0; i<5; i++)
            {
                VStringBuffer val("newval%d", i+1);
                conn->queryRoot()->setProp(NULL, val.str());
                conn->commit();
            }
            conn->queryRoot()->setProp("subnode", "newval");
            conn->commit();
            conn.clear(); // if finalDelete=true, deletes subscribed node in process, should get notificaiton

        }
        void start()
        {
            threaded.init(this);
        }
        void join()
        {
            threaded.join();
        }
    };


    class CResults
    {
        StringArray results;
        CRC32 crc;
    public:
        void add(const char *out)
        {
            PROGLOG("%s", out);
            results.append(out);
        }
        unsigned getCRC()
        {
            results.sortAscii();
            ForEachItemIn(r, results)
            {
                const char *result = results.item(r);
                crc.tally(strlen(result), result);
            }
            PROGLOG("CRC = %x", crc.get());
            results.kill();
            return crc.get();
        }
    };

    class CSubscriber : CSimpleInterface, implements ISDSNodeSubscription
    {
        StringAttr path;
        CResults &results;
        unsigned notifications, expectedNotifications;
        Semaphore joinSem;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSubscriber(const char *_path, CResults &_results, unsigned _expectedNotifications)
            : path(_path), results(_results), expectedNotifications(_expectedNotifications)
        {
            notifications = 0;
            if (0 == expectedNotifications)
                joinSem.signal();
        }
        virtual void notify(SubscriptionId id, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            StringAttr value;
            if (valueLen)
                value.set((const char *)valueData, valueLen);
            VStringBuffer res("Subscriber(%s): flags=%d, value=%s", path.get(), flags, 0==valueLen ? "(none)" : value.get());
            results.add(res);
            ++notifications;
            if (notifications == expectedNotifications)
                joinSem.signal();
        }
        void join()
        {
            if (joinSem.wait(5000))
            {
                MilliSleep(100); // wait a bit, see if get more than expected
                if (notifications == expectedNotifications)
                {
                    VStringBuffer out("Subscriber(%s): %d notifications received", path.get(), notifications);
                    results.add(out);
                    return;
                }
            }
            VStringBuffer out("Expected %d notifications, received %d", expectedNotifications, notifications);
            results.add(out);
        }
    };

    void sdsNodeCommit(const char *test, unsigned from, unsigned to, bool finalDelete)
    {
        CIArrayOf<CNodeSubCommitThread> commitThreads;
        for (unsigned i=from; i<=to; i++)
        {
            VStringBuffer path("/DAREGRESS/NodeSubTest/node%d", i);
            CNodeSubCommitThread *commitThread = new CNodeSubCommitThread(path, finalDelete);
            commitThreads.append(* commitThread);
        }
        ForEachItemIn(t, commitThreads)
            commitThreads.item(t).start();
        ForEachItemIn(t2, commitThreads)
            commitThreads.item(t2).join();
    }

    void testSDSNodeSubs()
    {
        // setup
        Owned<IRemoteConnection> conn = querySDS().connect("/DAREGRESS/NodeSubTest", myProcessSession(), RTM_CREATE, 1000000);
        IPropertyTree *root = conn->queryRoot();
        unsigned i, ai;
        for (i=0; i<10; i++)
        {
            VStringBuffer name("node%d", i+1);
            IPropertyTree *sub = root->setPropTree(name, createPTree());
            for (ai=0; ai<2; ai++)
            {
                VStringBuffer name("@attr%d", i+1);
                VStringBuffer val("val%d", i+1);
                sub->setProp(name, val);
            }
        }
        conn.clear();

        CResults results;

        {
            const char *testPath = "/DAREGRESS/NodeSubTest/doesnotexist";
            Owned<CSubscriber> subscriber = new CSubscriber(testPath, results, 0);
            try
            {
                querySDS().subscribeExact(testPath, *subscriber, true);
                throwUnexpected();
            }
            catch(IException *e)
            {
                if (SDSExcpt_SubscriptionNoMatch != e->errorCode())
                    throw;
                results.add("Correctly failed to add subscriber to non-existent node.");
            }
            subscriber.clear();
        }

        {
            const char *testPath = "/DAREGRESS/NodeSubTest/node1";
            Owned<CSubscriber> subscriber = new CSubscriber(testPath, results, 2*5+1+1);
            SubscriptionId id = querySDS().subscribeExact(testPath, *subscriber, false);

            sdsNodeCommit(testPath, 1, 1, false);
            sdsNodeCommit(testPath, 1, 1, true); // will delete 'node1'

            subscriber->join();
            querySDS().unsubscribeExact(id); // will actually be a NOP, as will be already unsubscribed when 'node1' deleted.
        }

        {
            const char *testPath = "/DAREGRESS/NodeSubTest/node*";
            Owned<CSubscriber> subscriber = new CSubscriber(testPath, results, 9*6);
            SubscriptionId id = querySDS().subscribeExact(testPath, *subscriber, false);

            sdsNodeCommit(testPath, 2, 10, false);

            subscriber->join();
            querySDS().unsubscribeExact(id);
        }

        {
            UInt64Array subscriberIds;
            IArrayOf<CSubscriber> subscribers;
            for (i=2; i<=10; i++) // NB: from 2, as 'node1' deleted in previous tests
            {
                for (ai=0; ai<2; ai++)
                {
                    VStringBuffer path("/DAREGRESS/NodeSubTest/node%d[@attr%d=\"val%d\"]", i, i, i);
                    Owned<CSubscriber> subscriber = new CSubscriber(path, results, 11);
                    SubscriptionId id = querySDS().subscribeExact(path, *subscriber, 0==ai);
                    subscribers.append(* subscriber.getClear());
                    subscriberIds.append(id);
                }
            }
            const char *testPath = "/DAREGRESS/NodeSubTest/node*";
            Owned<CSubscriber> subscriber = new CSubscriber(testPath, results, 9*5+9*(5+1));
            SubscriptionId id = querySDS().subscribeExact(testPath, *subscriber, false);

            sdsNodeCommit(testPath, 2, 10, false);
            sdsNodeCommit(testPath, 2, 10, true);

            subscriber->join();
            querySDS().unsubscribeExact(id);
            ForEachItemIn(s, subscriberIds)
            {
                subscribers.item(s).join();
                querySDS().unsubscribeExact(subscriberIds.item(s));
            }
        }

        ASSERT(0xa68e2324 == results.getCRC() && "SDS Node notifcation differences");
    }

    /*
     * This test is silly and can take a very long time on clusters with
     * a large file-system. But keeping it here for further reference.
     * MORE: Maybe, this could be added to daliadmin or a thorough check
     * on the filesystem, together with super-file checks et al.
    void testReadAllSDS()
    {
        logctx.CTXLOG("Test SDS connecting to every branch");
        testReadBranch("/");
        logctx.CTXLOG("Connected to every branch");
    }
    */

    void testMultiCluster()
    {
        Owned<IGroup> grp1 = createIGroup("192.168.51.1-5");
        Owned<IGroup> grp2 = createIGroup("192.168.16.1-5");
        Owned<IGroup> grp3 = createIGroup("192.168.53.1-5");
        queryNamedGroupStore().add("testgrp1",grp1);
        queryNamedGroupStore().add("testgrp2",grp2);
        queryNamedGroupStore().add("testgrp3",grp3);

        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        fdesc->setDefaultDir("/c$/thordata/test");
        fdesc->setPartMask("testfile1._$P$_of_$N$");
        fdesc->setNumParts(5);
        ClusterPartDiskMapSpec mapping;
        fdesc->addCluster(grp1,mapping);
        fdesc->addCluster(grp2,mapping);
        fdesc->addCluster(grp3,mapping);
        removeLogical("test::testfile1", user);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        removeLogical("test::testfile1", user);
        file->attach("test::testfile1",user);
        StringBuffer name;
        unsigned i;
        for (i=0;i<file->numClusters();i++)
            PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
        file.clear();
        file.setown(queryDistributedFileDirectory().lookup("test::testfile1",user));
        for (i=0;i<file->numClusters();i++)
            PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
        file.clear();
        file.setown(queryDistributedFileDirectory().lookup("test::testfile1@testgrp1",user));
        for (i=0;i<file->numClusters();i++)
            PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
        file.clear();
        removeLogical("test::testfile1@testgrp2", user);
        file.setown(queryDistributedFileDirectory().lookup("test::testfile1",user));
        for (i=0;i<file->numClusters();i++)
            PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
    }

    void testFiles()
    {
        StringBuffer fn;
        const char *s = filelist;
        unsigned slowest = 0;
        StringAttr slowname;
        unsigned tot = 0;
        unsigned n = 0;
        while (*s) {
            fn.clear();
            while (*s==',')
                s++;
            while (*s&&(*s!=','))
                fn.append(*(s++));
            if (fn.length()) {
                n++;
                unsigned ss = msTick();
                checkFiles(fn);
                unsigned t = (msTick()-ss);
                if (t>slowest) {
                    slowest = t;
                    slowname.set(fn);
                }
                tot += t;
            }
        }
        printf("Complete in %ds avg %dms\n",tot/1000,tot/(n?n:1));
        if (!slowname.isEmpty())
            printf("Slowest %s = %dms\n",slowname.get(),slowest);
    }

    void testGroups()
    {
        SocketEndpointArray epa;
        SocketEndpoint ep;
        Owned<IGroup> grp;
        testGrp(epa);
        ep.set("10.150.10.80");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.81");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.82");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.83:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.84:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.84:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.84:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.84:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.85:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.86:111");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.87");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.87");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.10.88");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.150.11.88");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.173.10.88");
        epa.append(ep);
        testGrp(epa);
        ep.set("10.173.10.88:22222");
        epa.append(ep);
        testGrp(epa);
        ep.set("192.168.10.88");
        epa.append(ep);
        testGrp(epa);
    }

#ifndef COMPAT

    void testDF1()
    {
        const char * fname = "testing::propfile2";
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        Owned<IPropertyTree> pt = createPTree("Attr");
        RemoteFilename rfn;
        rfn.setRemotePath("//10.150.10.80/c$/thordata/test/part._1_of_3");
        pt->setPropInt("@size",123);
        fdesc->setPart(0,rfn,pt);
        rfn.setRemotePath("//10.150.10.81/c$/thordata/test/part._2_of_3");
        pt->setPropInt("@size",456);
        fdesc->setPart(1,rfn,pt);
        rfn.setRemotePath("//10.150.10.82/c$/thordata/test/part._3_of_3");
        pt->setPropInt("@size",789);
        fdesc->setPart(2,rfn,pt);
        dispFDesc(fdesc);
        try {
            removeLogical(fname, user);
            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
            {
                DistributedFilePropertyLock lock(file);
                lock.queryAttributes().setProp("@testing","1");
            }
            file->attach(fname,user);
        } catch (IException *e) {
            StringBuffer msg;
            e->errorMessage(msg);
            logctx.CTXLOG("Caught exception while setting property: %s", msg.str());
            e->Release();
        }
    }

    void testDF2() // 4*3 superfile
    {
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        Owned<IPropertyTree> pt = createPTree("Attr");
        RemoteFilename rfn;
        rfn.setRemotePath("//10.150.10.80/c$/thordata/test/partone._1_of_3");
        pt->setPropInt("@size",1231);
        fdesc->setPart(0,rfn,pt);
        rfn.setRemotePath("//10.150.10.80/c$/thordata/test/parttwo._1_of_3");
        pt->setPropInt("@size",1232);
        fdesc->setPart(1,rfn,pt);
        rfn.setRemotePath("//10.150.10.80/c$/thordata/test/partthree._1_of_3");
        pt->setPropInt("@size",1233);
        fdesc->setPart(2,rfn,pt);
        rfn.setRemotePath("//10.150.10.80/c$/thordata/test2/partfour._1_of_3");
        pt->setPropInt("@size",1234);
        fdesc->setPart(3,rfn,pt);
        rfn.setRemotePath("//10.150.10.81/c$/thordata/test/partone._2_of_3");
        pt->setPropInt("@size",4565);
        fdesc->setPart(4,rfn,pt);
        rfn.setRemotePath("//10.150.10.81/c$/thordata/test/parttwo._2_of_3");
        pt->setPropInt("@size",4566);
        fdesc->setPart(5,rfn,pt);
        rfn.setRemotePath("//10.150.10.81/c$/thordata/test/partthree._2_of_3");
        pt->setPropInt("@size",4567);
        fdesc->setPart(6,rfn,pt);
        rfn.setRemotePath("//10.150.10.81/c$/thordata/test2/partfour._2_of_3");
        pt->setPropInt("@size",4568);
        fdesc->setPart(7,rfn,pt);
        rfn.setRemotePath("//10.150.10.82/c$/thordata/test/partone._3_of_3");
        pt->setPropInt("@size",7899);
        fdesc->setPart(8,rfn,pt);
        rfn.setRemotePath("//10.150.10.82/c$/thordata/test/parttwo._3_of_3");
        pt->setPropInt("@size",78910);
        fdesc->setPart(9,rfn,pt);
        rfn.setRemotePath("//10.150.10.82/c$/thordata/test/partthree._3_of_3");
        pt->setPropInt("@size",78911);
        fdesc->setPart(10,rfn,pt);
        rfn.setRemotePath("//10.150.10.82/c$/thordata/test2/partfour._3_of_3");
        pt->setPropInt("@size",78912);
        fdesc->setPart(11,rfn,pt);
        ClusterPartDiskMapSpec mspec;
        mspec.interleave = 4;
        fdesc->endCluster(mspec);
        dispFDesc(fdesc);
    }

    void testMisc()
    {
        ClusterPartDiskMapSpec mspec;
        Owned<IGroup> grp = createIGroup("10.150.10.1-3");
        RemoteFilename rfn;
        for (unsigned i=0;i<3;i++)
            for (unsigned ic=0;ic<mspec.defaultCopies;ic++) {
                constructPartFilename(grp,i+1,3,(i==1)?"test.txt":NULL,"test._$P$_of_$N$","/c$/thordata/test",ic,mspec,rfn);
                StringBuffer tmp;
                printf("%d,%d: %s\n",i,ic,rfn.getRemotePath(tmp).str());
            }
    }

    void testDFile()
    {
        ClusterPartDiskMapSpec map;
        {   // 1: single part file old method
#define TN "1"
            removeLogical("test::ftest" TN, user);
            Owned<IFileDescriptor> fdesc = createFileDescriptor();
            RemoteFilename rfn;
            rfn.setRemotePath("//10.150.10.1/c$/thordata/test/ftest" TN "._1_of_1");
            fdesc->setPart(0,rfn);
            fdesc->endCluster(map);
            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
            file->attach("test::ftest" TN,user);
#undef TN
        }
        {   // 2: single part file new method
#define TN "2"
            removeLogical("test::ftest" TN, user);
            Owned<IFileDescriptor> fdesc = createFileDescriptor();
            fdesc->setPartMask("ftest" TN "._$P$_of_$N$");
            fdesc->setNumParts(1);
            Owned<IGroup> grp = createIGroup("10.150.10.1");
            fdesc->addCluster(grp,map);
            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
            file->attach("test::ftest" TN,user);
#undef TN
        }
        Owned<IGroup> grp3 = createIGroup("10.150.10.1,10.150.10.2,10.150.10.3");
        queryNamedGroupStore().add("__testgroup3__",grp3,true);
        {   // 3: three parts file old method
#define TN "3"
            removeLogical("test::ftest" TN, user);
            Owned<IFileDescriptor> fdesc = createFileDescriptor();
            RemoteFilename rfn;
            rfn.setRemotePath("//10.150.10.1/c$/thordata/test/ftest" TN "._1_of_3");
            fdesc->setPart(0,rfn);
            rfn.setRemotePath("//10.150.10.2/c$/thordata/test/ftest" TN "._2_of_3");
            fdesc->setPart(1,rfn);
            rfn.setRemotePath("//10.150.10.3/c$/thordata/test/ftest" TN "._3_of_3");
            fdesc->setPart(2,rfn);
            fdesc->endCluster(map);
            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
            file->attach("test::ftest" TN,user);
#undef TN
        }
        {   // 4: three part file new method
#define TN "4"
            removeLogical("test::ftest" TN, user);
            Owned<IFileDescriptor> fdesc = createFileDescriptor();
            fdesc->setPartMask("ftest" TN "._$P$_of_$N$");
            fdesc->setNumParts(3);
            fdesc->addCluster(grp3,map);
            Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
            file->attach("test::ftest" TN,user);
#undef TN
        }
    }

#endif

    void testDFSDel()
    {
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        setupDFS("del");

        // Sub-file deletion
        logctx.CTXLOG("Creating regress::del::super1 and attaching sub");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::del::super1", user, false, false, transaction);
        sfile->addSubFile("regress::del::sub1", false, NULL, false, transaction);
        sfile->addSubFile("regress::del::sub4", false, NULL, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Deleting regress::del::sub1, should fail");
        try {
            if (dir.removeEntry("regress::del::sub1", user, transaction)) {
                ASSERT(0 && "Could remove sub, this will make the DFS inconsistent!");
                return;
            }
        } catch (IException *e) {
            // expecting an exception
            e->Release();
        }


        logctx.CTXLOG("Removing regress::del::sub1 from super1, no del");
        sfile.setown(transaction->lookupSuperFile("regress::del::super1"));
        sfile->removeSubFile("regress::del::sub1", false, false, transaction);
        ASSERT(sfile->numSubFiles() == 1 && "File sub1 was not removed from super1");
        sfile.clear();
        ASSERT(dir.exists("regress::del::sub1", user) && "File sub1 was removed from the file system");

        logctx.CTXLOG("Removing regress::del::sub4 from super1, del");
        sfile.setown(transaction->lookupSuperFile("regress::del::super1"));
        sfile->removeSubFile("regress::del::sub4", true, false, transaction);
        ASSERT(!sfile->numSubFiles() && "File sub4 was not removed from super1");
        sfile.clear();
        ASSERT(!dir.exists("regress::del::sub4", user) && "File sub4 was NOT removed from the file system");

        // Logical Remove
        logctx.CTXLOG("Deleting 'regress::del::super1, should work");
        ASSERT(dir.removeEntry("regress::del::super1", user) && "Can't remove super1");
        logctx.CTXLOG("Deleting 'regress::del::sub1 autoCommit, should work");
        ASSERT(dir.removeEntry("regress::del::sub1", user) && "Can't remove sub1");

        logctx.CTXLOG("Removing 'regress::del::sub2 - rollback");
        transaction->start();
        dir.removeEntry("regress::del::sub2", user, transaction);
        transaction->rollback();

        ASSERT(dir.exists("regress::del::sub2", user, true, false) && "Shouldn't have removed sub2 on rollback");

        logctx.CTXLOG("Removing 'regress::del::sub2 - commit");
        transaction->start();
        dir.removeEntry("regress::del::sub2", user, transaction);
        transaction->commit();

        ASSERT(!dir.exists("regress::del::sub2", user, true, false) && "Should have removed sub2 on commit");

        // Physical Remove
        logctx.CTXLOG("Physically removing 'regress::del::sub3 - rollback");
        transaction->start();
        dir.removeEntry("regress::del::sub3", user, transaction);
        transaction->rollback();

        ASSERT(dir.exists("regress::del::sub3", user, true, false) && "Shouldn't have removed sub3 on rollback");

        logctx.CTXLOG("Physically removing 'regress::del::sub3 - commit");
        transaction->start();
        dir.removeEntry("regress::del::sub3", user, transaction);
        transaction->commit();

        ASSERT(!dir.exists("regress::del::sub3", user, true, false) && "Should have removed sub3 on commit");
    }

    void testDFSRename()
    {
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        if (dir.exists("regress::rename::other1",user,false,false))
            ASSERT(dir.removeEntry("regress::rename::other1", user) && "Can't remove 'regress::rename::other1'");
        if (dir.exists("regress::rename::other2",user,false,false))
            ASSERT(dir.removeEntry("regress::rename::other2", user) && "Can't remove 'regress::rename::other2'");

        setupDFS("rename");

        try {
            logctx.CTXLOG("Renaming 'regress::rename::sub1 to 'sub2' with auto-commit, should fail");
            dir.renamePhysical("regress::rename::sub1", "regress::rename::sub2", user, transaction);
            ASSERT(0 && "Renamed to existing file should have failed!");
            return;
        } catch (IException *e) {
            // Expecting exception
            e->Release();
        }

        logctx.CTXLOG("Renaming 'regress::rename::sub1 to 'other1' with auto-commit");
        dir.renamePhysical("regress::rename::sub1", "regress::rename::other1", user, transaction);
        ASSERT(dir.exists("regress::rename::other1", user, true, false) && "Renamed to other failed");

        logctx.CTXLOG("Renaming 'regress::rename::sub2 to 'other2' and rollback");
        transaction->start();
        dir.renamePhysical("regress::rename::sub2", "regress::rename::other2", user, transaction);
        transaction->rollback();
        ASSERT(!dir.exists("regress::rename::other2", user, true, false) && "Renamed to other2 when it shouldn't");

        logctx.CTXLOG("Renaming 'regress::rename::sub2 to 'other2' and commit");
        transaction->start();
        dir.renamePhysical("regress::rename::sub2", "regress::rename::other2", user, transaction);
        transaction->commit();
        ASSERT(dir.exists("regress::rename::other2", user, true, false) && "Renamed to other failed");

        try {
            logctx.CTXLOG("Renaming 'regress::rename::sub3 to 'sub3' with auto-commit, should fail");
            dir.renamePhysical("regress::rename::sub3", "regress::rename::sub3", user, transaction);
            ASSERT(0 && "Renamed to same file should have failed!");
            return;
        } catch (IException *e) {
            // Expecting exception
            e->Release();
        }

        // To make sure renamed files are cleaned properly
        printf("Renaming 'regress::rename::other2 to 'sub2' on auto-commit\n");
        dir.renamePhysical("regress::rename::other2", "regress::rename::sub2", user, transaction);
        ASSERT(dir.exists("regress::rename::sub2", user, true, false) && "Renamed from other2 failed");
    }

    void testDFSClearAdd()
    {
        setupDFS("clearadd");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        logctx.CTXLOG("Creating regress::clearadd::super1 and attaching sub1 & sub4");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::clearadd::super1", user, false, false, transaction);
        sfile->addSubFile("regress::clearadd::sub1", false, NULL, false, transaction);
        sfile->addSubFile("regress::clearadd::sub4", false, NULL, false, transaction);
        sfile.clear();

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit
        transaction->start();

        logctx.CTXLOG("Removing sub1 from super1, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::clearadd::super1"));
        sfile->removeSubFile("regress::clearadd::sub1", false, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Adding sub1 back into to super1, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::clearadd::super1"));
        sfile->addSubFile("regress::clearadd::sub1", false, NULL, false, transaction);
        sfile.clear();
        try
        {
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        sfile.setown(dir.lookupSuperFile("regress::clearadd::super1", user));
        ASSERT(NULL != sfile->querySubFileNamed("regress::clearadd::sub1") && "regress::clearadd::sub1, should be a subfile of super1");

        // same but remove all (clear)
        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit
        transaction->start();

        logctx.CTXLOG("Adding sub2 into to super1, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::clearadd::super1"));
        sfile->addSubFile("regress::clearadd::sub2", false, NULL, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Removing all sub files from super1, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::clearadd::super1"));
        sfile->removeSubFile(NULL, false, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Adding sub2 back into to super1, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::clearadd::super1"));
        sfile->addSubFile("regress::clearadd::sub2", false, NULL, false, transaction);
        sfile.clear();
        try
        {
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        sfile.setown(dir.lookupSuperFile("regress::clearadd::super1", user));
        ASSERT(NULL != sfile->querySubFileNamed("regress::clearadd::sub2") && "regress::clearadd::sub2, should be a subfile of super1");
        ASSERT(NULL == sfile->querySubFileNamed("regress::clearadd::sub1") && "regress::clearadd::sub1, should NOT be a subfile of super1");
        ASSERT(NULL == sfile->querySubFileNamed("regress::clearadd::sub4") && "regress::clearadd::sub4, should NOT be a subfile of super1");
        ASSERT(1 == sfile->numSubFiles() && "regress::clearadd::super1 should contain 1 subfile");
    }

    void testDFSAddFailReAdd()
    {
        setupDFS("addreadd");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        logctx.CTXLOG("Creating super1 and supet2, adding sub1 and sub2 to super1 and sub3 to super2");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::addreadd::super1", user, false, false, transaction);
        sfile->addSubFile("regress::addreadd::sub1", false, NULL, false, transaction);
        sfile->addSubFile("regress::addreadd::sub2", false, NULL, false, transaction);
        sfile.clear();
        Owned<IDistributedSuperFile> sfile2 = dir.createSuperFile("regress::addreadd::super2", user, false, false, transaction);
        sfile2->addSubFile("regress::addreadd::sub3", false, NULL, false, transaction);
        sfile2.clear();

        class CShortLock : implements IThreaded
        {
            StringAttr fileName;
            unsigned secDelay;
            CThreaded threaded;
        public:
            CShortLock(const char *_fileName, unsigned _secDelay) : fileName(_fileName), secDelay(_secDelay), threaded("CShortLock", this) { }
            ~CShortLock()
            {
                threaded.join();
            }
            virtual void main()
            {
                Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(fileName, NULL);

                if (!file)
                {
                    PROGLOG("File %s not found", fileName.get());
                    return;
                }
                PROGLOG("Locked file: %s, sleeping (before unlock) for %d secs", fileName.get(), secDelay);

                MilliSleep(secDelay * 1000);

                PROGLOG("Unlocking file: %s", fileName.get());
            }
            void start() { threaded.start(); }
        };

        /* Tests transaction failing, due to lock and retrying after having partial success */

        CShortLock sL("regress::addreadd::sub2", 30); // the 2nd subfile of super1
        sL.start();

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit
        logctx.CTXLOG("Starting transaction");
        transaction->start();

        logctx.CTXLOG("Adding contents of regress::addreadd::super1 to regress::addreadd::super2, within transaction");
        sfile.setown(transaction->lookupSuperFile("regress::addreadd::super2"));
        sfile->addSubFile("regress::addreadd::super1", false, NULL, true, transaction); // add contents of super1 to super2
        sfile.setown(transaction->lookupSuperFile("regress::addreadd::super1"));
        sfile->removeSubFile(NULL, false, false, transaction); // clears super1
        sfile.clear();

        try
        {
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        transaction.clear();
        sfile.setown(dir.lookupSuperFile("regress::addreadd::super2", user));
        ASSERT(3 == sfile->numSubFiles() && "regress::addreadd::super2 should contain 3 subfiles");
        ASSERT(NULL != sfile->querySubFileNamed("regress::addreadd::sub1") && "regress::addreadd::sub1, should be a subfile of super2");
        ASSERT(NULL != sfile->querySubFileNamed("regress::addreadd::sub2") && "regress::addreadd::sub2, should be a subfile of super2");
        ASSERT(NULL != sfile->querySubFileNamed("regress::addreadd::sub3") && "regress::addreadd::sub3, should be a subfile of super2");
        sfile.setown(dir.lookupSuperFile("regress::addreadd::super1", user));
        ASSERT(0 == sfile->numSubFiles() && "regress::addreadd::super1 should contain 0 subfiles");
    }

    void testDFSRetrySuperLock()
    {
        setupDFS("retrysuperlock");

        logctx.CTXLOG("Creating regress::retrysuperlock::super1 and regress::retrysuperlock::sub1");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::retrysuperlock::super1", user, false, false);
        sfile->addSubFile("regress::retrysuperlock::sub1", false, NULL, false);
        sfile.clear();

        class CShortLock : implements IThreaded
        {
            StringAttr fileName;
            unsigned secDelay;
            CThreaded threaded;
        public:
            CShortLock(const char *_fileName, unsigned _secDelay) : fileName(_fileName), secDelay(_secDelay), threaded("CShortLock", this) { }
            ~CShortLock()
            {
                threaded.join();
            }
            virtual void main()
            {
                Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(fileName, NULL);

                if (!file)
                {
                    PROGLOG("File %s not found", fileName.get());
                    return;
                }
                PROGLOG("Locked file: %s, sleeping (before unlock) for %d secs", fileName.get(), secDelay);

                MilliSleep(secDelay * 1000);

                PROGLOG("Unlocking file: %s", fileName.get());
            }
            void start() { threaded.start(); }
        };

        /* Tests transaction failing, due to lock and retrying after having partial success */

        CShortLock sL("regress::retrysuperlock::super1", 15);
        sL.start();

        sfile.setown(dir.lookupSuperFile("regress::retrysuperlock::super1", user));
        if (sfile)
        {
            logctx.CTXLOG("Removing subfiles from regress::retrysuperlock::super1");
            sfile->removeSubFile(NULL, false, false);
            logctx.CTXLOG("SUCCEEDED");
        }
        // put it back, for next test
        sfile->addSubFile("regress::retrysuperlock::sub1", false, NULL, false);
        sfile.clear();

        // try again, this time in transaction
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit
        logctx.CTXLOG("Starting transaction");
        transaction->start();

        sfile.setown(transaction->lookupSuperFile("regress::retrysuperlock::super1"));
        if (sfile)
        {
            logctx.CTXLOG("Removing subfiles from regress::retrysuperlock::super1 with transaction");
            sfile->removeSubFile(NULL, false, false, transaction);
            logctx.CTXLOG("SUCCEEDED");
        }
        sfile.clear();
        logctx.CTXLOG("Committing transaction");
        transaction->commit();
    }

    void testDFSRename2()
    {
        setupDFS("rename2");

        /* Create a super and sub1 and sub4 in a auto-commit transaction
         * Inside a transaction, do:
         * a) rename sub2 to renamedsub2
         * b) remove sub1
         * c) add sub1
         * d) add renamedsub2
         * e) commit transaction
         * f) renamed files existing and superfile contents
         */
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        logctx.CTXLOG("Creating regress::rename2::super1 and attaching sub1 & sub4");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::rename2::super1", user, false, false, transaction);
        sfile->addSubFile("regress::rename2::sub1", false, NULL, false, transaction);
        sfile->addSubFile("regress::rename2::sub4", false, NULL, false, transaction);
        sfile.clear();

        if (dir.exists("regress::rename2::renamedsub2",user,false,false))
            ASSERT(dir.removeEntry("regress::rename2::renamedsub2", user) && "Can't remove 'regress::rename2::renamedsub2'");

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit

        logctx.CTXLOG("Starting transaction");
        transaction->start();

        logctx.CTXLOG("Renaming regress::rename2::sub2 TO regress::rename2::renamedsub2");
        dir.renamePhysical("regress::rename2::sub2", "regress::rename2::renamedsub2", user, transaction);

        logctx.CTXLOG("Removing regress::rename2::sub1 from regress::rename2::super1");
        sfile.setown(transaction->lookupSuperFile("regress::rename2::super1"));
        sfile->removeSubFile("regress::rename2::sub1", false, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Adding renamedsub2 to super1");
        sfile.setown(transaction->lookupSuperFile("regress::rename2::super1"));
        sfile->addSubFile("regress::rename2::renamedsub2", false, NULL, false, transaction);
        sfile.clear();

        logctx.CTXLOG("Adding back sub1 to super1");
        sfile.setown(transaction->lookupSuperFile("regress::rename2::super1"));
        sfile->addSubFile("regress::rename2::sub1", false, NULL, false, transaction);
        sfile.clear();

        try
        {
            logctx.CTXLOG("Committing transaction");
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        transaction.clear();

        // validate..
        ASSERT(dir.exists("regress::rename2::renamedsub2", user, true, false) && "regress::rename2::renamedsub2 should exist now transaction committed");
        sfile.setown(dir.lookupSuperFile("regress::rename2::super1", user));

        ASSERT(NULL != sfile->querySubFileNamed("regress::rename2::renamedsub2") && "regress::rename2::renamedsub2, should be a subfile of super1");
        ASSERT(NULL != sfile->querySubFileNamed("regress::rename2::sub1") && "regress::rename2::sub1, should be a subfile of super1");
        ASSERT(NULL == sfile->querySubFileNamed("regress::rename2::sub2") && "regress::rename2::sub2, should NOT be a subfile of super1");
        ASSERT(NULL != sfile->querySubFileNamed("regress::rename2::sub4") && "regress::rename2::sub4, should be a subfile of super1");
        ASSERT(3 == sfile->numSubFiles() && "regress::rename2::super1 should contain 4 subfiles");
    }

    void testDFSRenameThenDelete()
    {
        setupDFS("renamedelete");
        if (dir.exists("regress::renamedelete::renamedsub2",user,false,false))
            ASSERT(dir.removeEntry("regress::renamedelete::renamedsub2", user) && "Can't remove 'regress::renamedelete::renamedsub2'");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        logctx.CTXLOG("Starting transaction");
        transaction->start();

        logctx.CTXLOG("Renaming regress::renamedelete::sub2 TO regress::renamedelete::renamedsub2");
        dir.renamePhysical("regress::renamedelete::sub2", "regress::renamedelete::renamedsub2", user, transaction);

        logctx.CTXLOG("Removing regress::renamedelete::renamedsub2");
        ASSERT(dir.removeEntry("regress::renamedelete::renamedsub2", user, transaction) && "Can't remove 'regress::rename2::renamedsub2'");


        try
        {
            logctx.CTXLOG("Committing transaction");
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        transaction.clear();

        // validate..
        ASSERT(!dir.exists("regress::renamedelete::sub2", user, true, false) && "regress::renamedelete::sub2 should NOT exist now transaction has been committed");
        ASSERT(!dir.exists("regress::renamedelete::renamedsub2", user, true, false) && "regress::renamedelete::renamedsub2 should NOT exist now transaction has been committed");
    }

// NB: This test requires access (via dafilesrv) to an external IP (10.239.222.21 used below, but could be any)
    void testDFSRename3()
    {
        setupDFS("rename3");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

        if (dir.exists("regress::tenwayfile",user))
            ASSERT(dir.removeEntry("regress::tenwayfile", user) && "Can't remove");

        Owned<IFileDescriptor> fdesc = createDescriptor("regress", "tenwayfile", 1, 17);
        Owned<IGroup> grp1 = createIGroup("10.239.222.1");
        ClusterPartDiskMapSpec mapping;
        fdesc->setClusterGroup(0, grp1);

        Linked<IPartDescriptor> part = fdesc->getPart(0);
        RemoteFilename rfn;
        part->getFilename(0, rfn);
        StringBuffer path;
        rfn.getPath(path);
        recursiveCreateDirectoryForFile(path.str());
        OwnedIFile ifile = createIFile(path.str());
        Owned<IFileIO> io;
        io.setown(ifile->open(IFOcreate));
        io->write(0, 17, "12345678901234567");
        io->close();

        Owned<IDistributedFile> dsub = dir.createNew(fdesc);
        dsub->attach("regress::tenwayfile", user);
        dsub.clear();
        fdesc.clear();

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit
        transaction->start();

        logctx.CTXLOG("Renaming regress::rename3::sub2 TO regress::tenwayfile@mythor");
        dir.renamePhysical("regress::rename3::sub2", "regress::tenwayfile@mythor", user, transaction);

        try
        {
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }

        transaction.setown(createDistributedFileTransaction(user));

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit
        transaction->start();

        logctx.CTXLOG("Renaming regress::tenwayfile TO regress::rename3::sub2");
        dir.renamePhysical("regress::tenwayfile@mythor", "regress::rename3::sub2", user, transaction);

        try
        {
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
    }

    void testDFSRemoveSuperSub()
    {
        setupDFS("removesupersub");

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);

        logctx.CTXLOG("Creating regress::removesupersub::super1 and attaching sub1 & sub4");
        Owned<IDistributedSuperFile> sfile = dir.createSuperFile("regress::removesupersub::super1", user, false, false, transaction);
        sfile->addSubFile("regress::removesupersub::sub1", false, NULL, false, transaction);
        sfile->addSubFile("regress::removesupersub::sub4", false, NULL, false, transaction);
        sfile.clear();

        transaction.setown(createDistributedFileTransaction(user)); // disabled, auto-commit

        logctx.CTXLOG("Starting transaction");
        transaction->start();

        logctx.CTXLOG("Removing super removesupersub::super1 along with it's subfiles sub1 and sub4");
        dir.removeSuperFile("regress::removesupersub::super1", true, user, transaction);

        try
        {
            logctx.CTXLOG("Committing transaction");
            transaction->commit();
        }
        catch (IException *e)
        {
            StringBuffer eStr;
            e->errorMessage(eStr);
            CPPUNIT_ASSERT_MESSAGE(eStr.str(), 0);
            e->Release();
        }
        transaction.clear();

        // validate..
        ASSERT(!dir.exists("regress::removesupersub::super1", user, true, false) && "regress::removesupersub::super1 should NOT exist");
        ASSERT(!dir.exists("regress::removesupersub::sub1", user, true, false) && "regress::removesupersub::sub1 should NOT exist");
        ASSERT(!dir.exists("regress::removesupersub::sub4", user, true, false) && "regress::removesupersub::sub4 should NOT exist");
    }

    void testDFSHammer()
    {
        unsigned numFiles = 100;
        unsigned numReads = 40000;
        unsigned hammerThreads = 10;
        setupDFS("hammer", 0, numFiles);

        StringBuffer msg("Reading ");
        msg.append(numFiles).append(" files").append(numReads).append(" times, on ").append(hammerThreads).append(" threads");
        logctx.CTXLOG("%s", msg.str());

        class CHammerFactory : public CSimpleInterface, implements IThreadFactory
        {
        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            virtual IPooledThread *createNew()
            {
                class CHammerThread : public CSimpleInterface, implements IPooledThread
                {
                    StringAttr filename;
                public:
                    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

                    virtual void init(void *param)
                    {
                        filename.set((const char *)param);
                    }
                    virtual void main()
                    {
                        try
                        {
                            Owned<IPropertyTree> tree = queryDistributedFileDirectory().getFileTree(filename,user);
                        }
                        catch (IException *e)
                        {
                            PrintExceptionLog(e, NULL);
                        }
                    }
                    virtual bool stop() { return true; }
                    virtual bool canReuse() { return true; }
                };
                return new CHammerThread();
            }
        } poolFactory;

        CTimeMon tm;
        Owned<IThreadPool> pool = createThreadPool("TSDSTest", &poolFactory, NULL, hammerThreads, 2000);
        while (numReads--)
        {
            StringBuffer filename("regress::hammer::sub");
            unsigned fn = 1+(getRandom()%numFiles);
            filename.append(fn);
            PROGLOG("Hammer file: %s", filename.str());
            pool->start((void *)filename.str());
        }
        pool->joinAll();
        PROGLOG("Hammer test took: %d ms", tm.elapsed());
    }

    void testSDSSubs2()
    {
        class CSubscriber : public CSimpleInterfaceOf<ISDSSubscription>
        {
            StringAttr xpath;
            bool sub;
            StringBuffer &result;
            SubscriptionId id;
        public:
            CSubscriber(StringBuffer &_result, const char *_xpath, bool _sub) : result(_result), xpath(_xpath), sub(_sub)
            {
                id = querySDS().subscribe(xpath, *this, sub, !sub);
                PROGLOG("Subscribed to %s", xpath.get());
            }
            ~CSubscriber()
            {
                querySDS().unsubscribe(id);
            }
            virtual void notify(SubscriptionId id, const char *_xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
            {
                PROGLOG("CSubscriber notified path=%s for subscriber=%s, sub=%s", _xpath, xpath.get(), sub?"true":"false");
                if (result.length())
                    result.append("|");
                result.append(xpath);
                if (!sub && valueLen)
                    result.append(",").append(valueLen, (const char *)valueData);
            }
        };
        Owned<IRemoteConnection> conn = querySDS().connect("/DAREGRESS/TestSub2", myProcessSession(), RTM_CREATE, INFINITE);
        Owned<IPropertyTree> tree = createPTreeFromXMLString("<a><b1><c/></b1><b2/><b3><d><e/></d></b3></a>");
        IPropertyTree *root = conn->queryRoot();
        root->setPropTree("a", tree.getClear());
        conn->commit();

        StringBuffer result;
        Owned<ISDSSubscription> sub1 = new CSubscriber(result, "/DAREGRESS/TestSub2/a", true);
        Owned<ISDSSubscription> sub2 = new CSubscriber(result, "/DAREGRESS/TestSub2/a/b1", false);
        Owned<ISDSSubscription> sub3 = new CSubscriber(result, "/DAREGRESS/TestSub2/a/b2", false);
        Owned<ISDSSubscription> sub4 = new CSubscriber(result, "/DAREGRESS/TestSub2/a/b1/c", false);
        Owned<ISDSSubscription> sub5 = new CSubscriber(result, "/DAREGRESS/TestSub2/a/b3", true);

        MilliSleep(1000);

        StringArray expectedResults;
        expectedResults.append("/DAREGRESS/TestSub2/a");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b1,testv");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b2,testv");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b1/c,testv");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b1,testv");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b2,testv");
        expectedResults.append("/DAREGRESS/TestSub2/a");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b3");
        expectedResults.append("/DAREGRESS/TestSub2/a|/DAREGRESS/TestSub2/a/b3");

        StringArray props;
        props.appendList("S:a,S:a/b1,S:a/b2,S:a/b1/c,S:a/b1/d,S:a/b2/e,S:a/b2/e/f,D:a/b3/d/e,D:a/b3/d", ",");

        assertex(expectedResults.ordinality() == props.ordinality());

        ForEachItemIn(p, props)
        {
            result.clear(); // filled by subscriber notifications
            const char *cmd = props.item(p);
            const char *propPath=cmd+2;
            switch (*cmd)
            {
                case 'S':
                {
                    PROGLOG("Changing %s", propPath);
                    root->setProp(propPath, "testv");
                    break;
                }
                case 'D':
                {
                    PROGLOG("Deleting %s", propPath);
                    root->removeProp(propPath);
                    break;
                }
                default:
                    throwUnexpected();
            }
            conn->commit();

            MilliSleep(100); // time for notifications to come through

            PROGLOG("Checking results");
            StringArray resultArray;
            resultArray.appendList(result, "|");
            result.clear();
            resultArray.sortAscii();
            ForEachItemIn(r, resultArray)
            {
                if (result.length())
                    result.append("|");
                result.append(resultArray.item(r));
            }
            const char *expectedResult = expectedResults.item(p);
            if (0 == strcmp(expectedResult, result))
                PROGLOG("testSDSSubs2 [ %s ]: MATCH", cmd);
            else
            {
                VStringBuffer errMsg("testSDSSubs2 [ %s ]: MISMATCH", cmd);
                errMsg.newline().append("Expected: ").append(expectedResult);
                errMsg.newline().append("Got: ").append(result);
                PROGLOG("%s", errMsg.str());
                CPPUNIT_ASSERT_MESSAGE(errMsg.str(), 0);
            }
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( CDaliTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CDaliTests, "Dali" );

class CDaliUtils : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CDaliUtils);
      CPPUNIT_TEST(testDFSLfn);
    CPPUNIT_TEST_SUITE_END();
public:
    void testDFSLfn()
    {
        const char *lfns[] = { "~foreign::192.168.16.1::scope1::file1",
                               "~file::192.168.16.1::dir1::file1",
                               "~file::192.168.16.1::>some query or another",
                               "~file::192.168.16.1::wild?card1",
                               "~file::192.168.16.1::wild*card2",
                               "~file::192.168.16.1::^C^a^S^e^d",
                               "~file::192.168.16.1::file@cluster1",
                               "~prefix::{multi1*,multi2*}",
                               "{~foreign::192.168.16.1::multi1, ~foreign::192.168.16.2::multi2}",

                               // NB: CDfsLogicalFileName allows these with strict=false (the default)
                               ". :: scope1 :: file",
                               ":: scope1 :: file",
                               "~ scope1 :: scope2 :: file  ",
                               ". :: scope1 :: file nine",
                               ". :: scope1 :: file ten  ",
                               ". :: scope1 :: file",
                               NULL                                             // terminator
                             };
        const char *invalidLfns[] = {
                               ". :: sc~ope1::file",
                               ". ::  ::file",
                               "~~scope1::file",
                               "~sc~ope1::file2",
                               ".:: scope1::file*",
                               NULL                                             // terminator
                             };
        PROGLOG("Checking valid logical filenames");
        unsigned nlfn=0;
        loop
        {
            const char *lfn = lfns[nlfn++];
            if (NULL == lfn)
                break;
            PROGLOG("lfn = %s", lfn);
            CDfsLogicalFileName dlfn;
            try
            {
                dlfn.set(lfn);
            }
            catch (IException *e)
            {
                VStringBuffer err("Logical filename '%s' failed.", lfn);
                EXCLOG(e, err.str());
                CPPUNIT_FAIL(err.str());
                e->Release();
            }
        }
        PROGLOG("Checking invalid logical filenames");
        nlfn = 0;
        loop
        {
            const char *lfn = invalidLfns[nlfn++];
            if (NULL == lfn)
                break;
            PROGLOG("lfn = %s", lfn);
            CDfsLogicalFileName dlfn;
            try
            {
                dlfn.set(lfn);
                VStringBuffer err("Logical filename '%s' passed and should have failed.", lfn);
                ERRLOG("%s", err.str());
                CPPUNIT_FAIL(err.str());
            }
            catch (IException *e)
            {
                EXCLOG(e, "Expected error:");
                e->Release();
            }
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( CDaliUtils );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( CDaliUtils, "DaliUtils" );


#endif // _USE_CPPUNIT
