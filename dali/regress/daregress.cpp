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

/*
 * Dali Quick Regression Suite: Tests Dali functionality on a programmatic way.
 *
 * Add as much as possible here to avoid having to run the Hthor/Thor regressions
 * all the time for Dali tests, since most of it can be tested quickly from here.
 */

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"

static int errorcount;

#undef ERROR
#define ERROR(s) printf("ERROR(%d): %s[%d]: %s\n", ++errorcount, __FILE__, __LINE__, s)
#define ERROR1(s,p1) printf("ERROR(%d): %s[%d]: " s "\n", ++errorcount, __FILE__, __LINE__, p1)
#define ERROR2(s,p1,p2) printf("ERROR(%d): %s[%d]: " s "\n", ++errorcount, __FILE__, __LINE__, p1,p2)
#define ERROR3(s,p1,p2,p3) printf("ERROR(%d): %s[%d]: " s "\n", ++errorcount, __FILE__, __LINE__, p1,p2,p3)
#define ERROR4(s,p1,p2,p3,p4) printf("ERROR(%d): %s[%d]: " s "\n", ++errorcount, __FILE__, __LINE__, p1,p2,p3,p4)

// ======================================================================= Support Functions / Classes

static IRemoteConnection *Rconn;
static IDistributedFileDirectory &dir = queryDistributedFileDirectory();
static IUserDescriptor *user = createUserDescriptor();

static unsigned fn(unsigned n, unsigned m, unsigned seed, unsigned depth, IPropertyTree *parent)
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

static unsigned fn2(unsigned n, unsigned m, unsigned seed, unsigned depth, StringBuffer &parentname)
{
    if (!Rconn)
        return 0;
    if ((n+m+seed)%25==0) {
        Rconn->commit();
        Rconn->Release();
        Rconn = querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000);
        if (!Rconn) {
            ERROR("Failed to connect to /DAREGRESS");
            return 0;
        }
    }
    IPropertyTree *parent = parentname.length()?Rconn->queryRoot()->queryPropTree(parentname.str()):Rconn->queryRoot();
    if (!parent) {
        ERROR1("Failed to connect to %s",parentname.str());
        Rconn->Release();
        Rconn = NULL;
        return 0;
    }
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

static __int64 subchangetotal;
static unsigned subchangenum;
static CriticalSection subchangesect;

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

extern void dispFDesc(IFileDescriptor *fdesc);

extern void runDafsTest();

struct RunOptions {
    char *match;
    char *dali;
    bool full;
};

static void usage(const char *error=NULL)
{
    if (error)
        printf("ERROR: %s\n", error);
    printf("usage: DAREGRESS <dali-host> [ test-name-match ] [ full ]\n");
    exit(-1);
}

static RunOptions read_cmdline(int argc, char **argv)
{
    StringBuffer cmd;
    splitFilename(argv[0], NULL, NULL, &cmd, NULL);
    StringBuffer lf;
    openLogFile(lf, cmd.toLowerCase().append(".log").str());

    RunOptions opt;
    opt.dali = argv[1];
    opt.full = false;
    opt.match = 0;
    for (unsigned i=2;i<argc;i++) {
        if (stricmp(argv[i],"full")==0)
            opt.full = 1;
        else
            if (!opt.match)
                opt.match = argv[i];
            else
                usage("Argument not recognised, too many matches");
    }
    return opt;
}

struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };

static IFileDescriptor *createFileDescriptor(const char* dir, const char* name, unsigned parts, unsigned recSize, unsigned index=0)
{
    Owned<IPropertyTree> pp = createPTree("Part");
    Owned<IFileDescriptor>fdesc = createFileDescriptor();
    fdesc->setDefaultDir(dir);
    StringBuffer s;
    for (unsigned k=0;k<parts;k++) {
        s.clear().append("192.168.1.10");
        Owned<INode> node = createINode(s.str());
        pp->setPropInt64("@size",recSize);
        s.clear().append(name);
        if (index)
            s.append(index);
        s.append("._").append(k+1).append("_of_").append(parts);
        fdesc->setPart(k,node,s.str(),pp);
    }
    fdesc->queryProperties().setPropInt("@recordSize",recSize);
    return fdesc.getClear();
}

static bool setupDFS(const char *scope, unsigned supersToDel=3, unsigned subsToCreate=4)
{
    StringBuffer buf;
    buf.append("regress::").append(scope);

    printf("Cleaning up '%s' scope\n", buf.str());
    for (unsigned i=1; i<=supersToDel; i++) {
        StringBuffer super = buf;
        super.append("::super").append(i);
        if (dir.exists(super.str(),false,true) && !dir.removeEntry(super.str(), user)) {
            ERROR1("Can't remove %s", super.str());
            return false;
        }
    }

    printf("Creating 'regress::trans' subfiles(1,4)\n");
    for (unsigned i=1; i<=subsToCreate; i++) {
        StringBuffer name;
        name.append("sub").append(i);
        StringBuffer sub = buf;
        sub.append("::").append(name);

        // Remove first
        if (dir.exists(sub.str(),true,false) && !dir.removeEntry(sub.str(), user)) {
            ERROR1("Can't remove %s", sub.str());
            return false;
        }

        // Create the sub file with an arbitrary format
        Owned<IFileDescriptor> subd = createFileDescriptor(scope, name, 3, 17);
        Owned<IDistributedFile> dsub = dir.createNew(subd);
        dsub->attach(sub.str());
        subd.clear();
        dsub.clear();

        // Make sure it got created
        if (!dir.exists(sub.str(),true,false)) {
            ERROR1("Can't add %s", sub.str());
            return false;
        }
    }

    return true;
}

// ======================================================================= Test cases

static void testSDSRW()
{
    Owned<IPropertyTree> ref = createPTree("DAREGRESS");
    fn(1,2,3,0,ref);
    StringBuffer refstr;
    toXML(ref,refstr,0,XML_SortTags|XML_Format);
    printf("Created reference size %d\n",refstr.length());
    Owned<IRemoteConnection> conn = querySDS().connect("/DAREGRESS",myProcessSession(), RTM_CREATE, 1000000);
    Rconn = conn;
    IPropertyTree *root = conn->queryRoot();
    fn(1,2,3,0,root);
    conn.clear();
    printf("Created test branch 1\n");
    conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), RTM_DELETE_ON_DISCONNECT, 1000000));
    root = conn->queryRoot();
    StringBuffer s;
    toXML(root,s,0,XML_SortTags|XML_Format);
    if (strcmp(s.str(),refstr.str())!=0) {
        ERROR("Branch 1 does not match");
    }
    else
        printf("Branch 1 matches\n");
    conn.clear();
    conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000));
    if (conn)
        ERROR("RTM_DELETE_ON_DISCONNECT failed");
    Rconn = querySDS().connect("/DAREGRESS",myProcessSession(), RTM_CREATE, 1000000);
    StringBuffer pn;
    fn2(1,2,3,0,pn);
    ::Release(Rconn);
    printf("Created test branch 2\n");
    Rconn = NULL;
    conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), RTM_DELETE_ON_DISCONNECT, 1000000));
    root = conn->queryRoot();
    toXML(root,s.clear(),0,XML_SortTags|XML_Format);
    if (strcmp(s.str(),refstr.str())!=0) {
        ERROR("Branch 2 does not match");
    }
    else
        printf("Branch 2 matches\n");
    conn.clear();
    conn.setown(querySDS().connect("/DAREGRESS",myProcessSession(), 0, 1000000));
    if (conn)
        ERROR("RTM_DELETE_ON_DISCONNECT failed");
}

static void testDFS()
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
    if (!queryNamedGroupStore().find(group,s.clear()))
        ERROR("Created logical group not found");
    if (stricmp(s.str(),"daregress_group")!=0)
        ERROR("Created logical group found with wrong name");
    group.setown(queryNamedGroupStore().lookup("daregress_group"));
    if (!group)
        ERROR("named group lookup failed");
    printf("Named group created    - 400 nodes\n");
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
        dir.removeEntry(s.str(), user);
        StringBuffer cname;
        Owned<IDistributedFile> dfile = dir.createNew(fdesc);
        if (stricmp(dfile->getClusterName(0,cname),"daregress_group")!=0)
            ERROR1("Cluster name wrong %d",i);
        s.clear().append("daregress::test").append(i);
        dfile->attach(s.str());
    }
    printf("DFile create done      - 100 files\n");
    unsigned samples = 5;
    t = 33;
    for (i=0;i<100;i++) {
        s.clear().append("daregress::test").append(t);
        if (!dir.exists(s.str()))
            ERROR1("Could not find %s",s.str());
        Owned<IDistributedFile> dfile = dir.lookup(s.str(), user);
        if (!dfile) {
            ERROR1("Could not find %s",s.str());
            continue;
        }
        offset_t totsz = 0;
        n = 11;
        for (unsigned k=0;k<400;k++) {
            Owned<IDistributedFilePart> part = dfile->getPart(n);
            if (!part) {
                ERROR2("part not found %d %d",t,n);
                continue;
            }
            s.clear().append("192.168.").append(n/256).append('.').append(n%256);
            Owned<INode> node = createINode(s.str());
            if (!node->equals(part->queryNode()))
                ERROR2("part node mismatch %d, %d",t,n);
            if (part->getFileSize(false,false)!=(n*777+t)*recsize)
                ERROR4("size node mismatch %d, %d, %d, %d",t,n,(unsigned)part->getFileSize(false,false),(n*777+t)*recsize);
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
                printf("SAMPLE: %d,%d %s\n",t,n,fn.str());
            }
            n = (n+11)%400;
        }
        if (totsz!=dfile->getFileSize(false,false))
            ERROR1("total size mismatch %d",t);
        t = (t+33)%100; 
    }
    printf("DFile lookup done      - 100 files\n");

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
    printf("DFile iterate done     - %d parts, %d rows, CRC sum %"I64F"d\n",np,totrows,crctot);
    Owned<IDistributedSuperFile> sfile;
    sfile.setown(dir.createSuperFile("daregress::superfile1",true,false,user));
    for (i = 0;i<100;i++) {
        s.clear().append("daregress::test").append(i);
        sfile->addSubFile(s.str());
    }
    sfile.clear();
    sfile.setown(dir.lookupSuperFile("daregress::superfile1", user));
    if (!sfile) {
        ERROR("Could not find added superfile");
        return;
    }
    __int64 savcrc = crctot;
    crctot = 0;
    np = 0;
    totrows = 0;
    size32_t srs = (size32_t)sfile->queryAttributes().getPropInt("@recordSize",-1);
    if (srs!=17)
        ERROR1("Superfile does not match subfile row size %d",srs);
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
    printf("Superfile iterate done - %d parts, %d rows, CRC sum %"I64F"d\n",np,totrows,crctot);
    if (crctot!=savcrc)
        ERROR("SuperFile does not match sub files");
    unsigned tr = (unsigned)(sfile->getFileSize(false,false)/srs); 
    if (totrows!=tr)
        ERROR1("Superfile size does not match part sum %d",tr);
    sfile->detach();
    sfile.clear();
    sfile.setown(dir.lookupSuperFile("daregress::superfile1"));
    if (sfile)
        ERROR("Superfile deletion failed");
    t = 37;
    for (i=0;i<100;i++) {
        s.clear().append("daregress::test").append(t);
        if (i%1) {
            Owned<IDistributedFile> dfile = dir.lookup(s.str());
            if (!dfile)
                ERROR1("Could not find %s",s.str());
            dfile->detach();
        }
        else 
            dir.removeEntry(s.str(), user);
        t = (t+37)%100; 
    }
    printf("DFile removal complete\n");
    t = 39;
    for (i=0;i<100;i++) {
        if (dir.exists(s.str()))
            ERROR1("Found %s after deletion",s.str());
        Owned<IDistributedFile> dfile = dir.lookup(s.str(), user);
        if (dfile)
            ERROR1("Found %s after deletion",s.str());
        t = (t+39)%100; 
    }
    printf("DFile removal check complete\n");
    queryNamedGroupStore().remove("daregress_group");
    if (queryNamedGroupStore().lookup("daregress_group"))
        ERROR("Named group not removed");
}

static void testDFSTrans()
{
    if (!setupDFS("trans"))
        return;

    Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);

    // Auto-commit
    printf("Auto-commit test (inactive transaction)\n");
    Owned<IDistributedSuperFile> sfile1 = dir.createSuperFile("regress::trans::super1", false, false, user, transaction);
    sfile1->addSubFile("regress::trans::sub1", false, NULL, false, transaction);
    sfile1->addSubFile("regress::trans::sub2", false, NULL, false, transaction);
    sfile1.clear();
    sfile1.setown(dir.lookupSuperFile("regress::trans::super1", user, transaction));
    if (!sfile1.get())
        ERROR("non-transactional add super1 failed");
    if (sfile1->numSubFiles() != 2)
        ERROR("auto-commit add sub failed, not all subs were added");
    else {
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub1") != 0)
            ERROR("auto-commit add sub failed, wrong name for sub1");
        if (strcmp(sfile1->querySubFile(1).queryLogicalName(), "regress::trans::sub2") != 0)
            ERROR("auto-commit add sub failed, wrong name for sub2");
    }
    sfile1.clear();

    // Rollback
    printf("Rollback test (active transaction)\n");
    transaction->start();
    Owned<IDistributedSuperFile> sfile2 = dir.createSuperFile("regress::trans::super2", false, false, user, transaction);
    sfile2->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
    sfile2->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
    transaction->rollback();
    if (sfile2->numSubFiles() != 0)
        ERROR("transactional rollback failed, some subs were added");
    sfile2.clear();
    sfile2.setown(dir.lookupSuperFile("regress::trans::super2", user, transaction));
    if (sfile2.get())
        ERROR("transactional rollback super2 failed, it exists!");

    // Commit
    printf("Commit test (active transaction)\n");
    transaction->start();
    Owned<IDistributedSuperFile> sfile3 = dir.createSuperFile("regress::trans::super3", false, false, user, transaction);
    sfile3->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
    sfile3->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
    transaction->commit();
    sfile3.clear();
    sfile3.setown(dir.lookupSuperFile("regress::trans::super3", user, transaction));
    if (!sfile3.get())
        ERROR("transactional add super3 failed");
    if (sfile3->numSubFiles() != 2)
        ERROR("transactional add sub failed, not all subs were added");
    else {
        if (strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub3") != 0)
            ERROR("transactional add sub failed, wrong name for sub3");
        if (strcmp(sfile3->querySubFile(1).queryLogicalName(), "regress::trans::sub4") != 0)
            ERROR("transactional add sub failed, wrong name for sub4");
    }
    sfile3.clear();
}

static void testDFSPromote()
{
    if (!setupDFS("trans"))
        return;

    Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);

    // ===============================================================================
    // Don't change these parameters, or you'll have to change all ERROR tests below
    const char *sfnames[3] = {
        "regress::trans::super1", "regress::trans::super2", "regress::trans::super3"
    };
    bool delsub = false;
    bool createonlyone = true;
    unsigned timeout = 1000; // 1s
    // ===============================================================================
    StringArray outlinked;

    printf("Promote (1, -, -) - first iteration\n");
    dir.promoteSuperFiles(3, sfnames, "regress::trans::sub1", delsub, createonlyone, user, timeout, outlinked);
    {
        Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
        if (!sfile1.get())
            ERROR("promote failed, super1 doesn't exist");
        if (sfile1->numSubFiles() != 1)
            ERROR("promote failed, super1 should have one subfile");
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub1") != 0)
            ERROR("promote failed, wrong name for sub1");
        Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
        if (sfile2.get())
            ERROR("promote failed, super2 does exist");
        if (outlinked.length() != 0)
            ERROR("promote failed, outlinked expected empty");
    }

    printf("Promote (2, 1, -) - second iteration\n");
    dir.promoteSuperFiles(3, sfnames, "regress::trans::sub2", delsub, createonlyone, user, timeout, outlinked);
    {
        Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
        if (!sfile1.get())
            ERROR("promote failed, super1 doesn't exist");
        if (sfile1->numSubFiles() != 1)
            ERROR("promote failed, super1 should have one subfile");
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub2") != 0)
            ERROR("promote failed, wrong name for sub2");
        Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
        if (!sfile2.get())
            ERROR("promote failed, super2 doesn't exist");
        if (sfile2->numSubFiles() != 1)
            ERROR("promote failed, super2 should have one subfile");
        if (strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub1") != 0)
            ERROR("promote failed, wrong name for sub1");
        Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
        if (sfile3.get())
            ERROR("promote failed, super3 does exist");
        if (outlinked.length() != 0)
            ERROR("promote failed, outlinked expected empty");
    }

    printf("Promote (3, 2, 1) - third iteration\n");
    dir.promoteSuperFiles(3, sfnames, "regress::trans::sub3", delsub, createonlyone, user, timeout, outlinked);
    {
        Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
        if (!sfile1.get())
            ERROR("promote failed, super1 doesn't exist");
        if (sfile1->numSubFiles() != 1)
            ERROR("promote failed, super1 should have one subfile");
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub3") != 0)
            ERROR("promote failed, wrong name for sub3");
        Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
        if (!sfile2.get())
            ERROR("promote failed, super2 doesn't exist");
        if (sfile2->numSubFiles() != 1)
            ERROR("promote failed, super2 should have one subfile");
        if (strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub2") != 0)
            ERROR("promote failed, wrong name for sub2");
        Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
        if (!sfile3.get())
            ERROR("promote failed, super3 doesn't exist");
        if (sfile3->numSubFiles() != 1)
            ERROR("promote failed, super3 should have one subfile");
        if (strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub1") != 0)
            ERROR("promote failed, wrong name for sub1");
        if (outlinked.length() != 0)
            ERROR("promote failed, outlinked expected empty");
    }

    printf("Promote (4, 3, 2) - fourth iteration, expect outlinked\n");
    dir.promoteSuperFiles(3, sfnames, "regress::trans::sub4", delsub, createonlyone, user, timeout, outlinked);
    {
        Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
        if (!sfile1.get())
            ERROR("promote failed, super1 doesn't exist");
        if (sfile1->numSubFiles() != 1)
            ERROR("promote failed, super1 should have one subfile");
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub4") != 0)
            ERROR("promote failed, wrong name for sub4");
        Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
        if (!sfile2.get())
            ERROR("promote failed, super2 doesn't exist");
        if (sfile2->numSubFiles() != 1)
            ERROR("promote failed, super2 should have one subfile");
        if (strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub3") != 0)
            ERROR("promote failed, wrong name for sub3");
        Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
        if (!sfile3.get())
            ERROR("promote failed, super3 doesn't exist");
        if (sfile3->numSubFiles() != 1)
            ERROR("promote failed, super3 should have one subfile");
        if (strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub2") != 0)
            ERROR("promote failed, wrong name for sub2");
        if (outlinked.length() != 1)
            ERROR("promote failed, outlinked expected only one item");
        if (strcmp(outlinked.popGet(), "regress::trans::sub1") != 0)
            ERROR("promote failed, outlinked expected to be sub1");
        Owned<IDistributedFile> sub1 = dir.lookup("regress::trans::sub1", user, false, NULL, timeout);
        if (!sub1.get())
            ERROR("promote failed, sub1 was physically deleted");
    }

    printf("Promote ([1,2], 4, 3) - fifth iteration, two in-files\n");
    dir.promoteSuperFiles(3, sfnames, "regress::trans::sub1,regress::trans::sub2", delsub, createonlyone, user, timeout, outlinked);
    {
        Owned<IDistributedSuperFile> sfile1 = dir.lookupSuperFile("regress::trans::super1", user, NULL, timeout);
        if (!sfile1.get())
            ERROR("promote failed, super1 doesn't exist");
        if (sfile1->numSubFiles() != 2)
            ERROR("promote failed, super1 should have two subfiles");
        if (strcmp(sfile1->querySubFile(0).queryLogicalName(), "regress::trans::sub1") != 0)
            ERROR("promote failed, wrong name for sub1");
        if (strcmp(sfile1->querySubFile(1).queryLogicalName(), "regress::trans::sub2") != 0)
            ERROR("promote failed, wrong name for sub2");
        Owned<IDistributedSuperFile> sfile2 = dir.lookupSuperFile("regress::trans::super2", user, NULL, timeout);
        if (!sfile2.get())
            ERROR("promote failed, super2 doesn't exist");
        if (sfile2->numSubFiles() != 1)
            ERROR("promote failed, super2 should have one subfile");
        if (strcmp(sfile2->querySubFile(0).queryLogicalName(), "regress::trans::sub4") != 0)
            ERROR("promote failed, wrong name for sub4");
        Owned<IDistributedSuperFile> sfile3 = dir.lookupSuperFile("regress::trans::super3", user, NULL, timeout);
        if (!sfile3.get())
            ERROR("promote failed, super3 doesn't exist");
        if (sfile3->numSubFiles() != 1)
            ERROR("promote failed, super3 should have one subfile");
        if (strcmp(sfile3->querySubFile(0).queryLogicalName(), "regress::trans::sub3") != 0)
            ERROR("promote failed, wrong name for sub3");
        if (outlinked.length() != 1)
            ERROR("promote failed, outlinked expected only one item");
        if (strcmp(outlinked.popGet(), "regress::trans::sub2") != 0)
            ERROR("promote failed, outlinked expected to be sub2");
        Owned<IDistributedFile> sub1 = dir.lookup("regress::trans::sub1", user, false, NULL, timeout);
        if (!sub1.get())
            ERROR("promote failed, sub1 was physically deleted");
        Owned<IDistributedFile> sub2 = dir.lookup("regress::trans::sub2", user, false, NULL, timeout);
        if (!sub2.get())
            ERROR("promote failed, sub2 was physically deleted");
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
    if (subchangenum!=1000)
        ERROR1("Not all notifications received %d",subchangenum);
    printf("%d subscription notifications, check sum = %"I64F"d\n",subchangenum,subchangetotal);
}

void testReadBranch(const char *path)
{
    PROGLOG("Connecting to %s",path);
    Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_READ, 10000);
    if (!conn) {
        ERROR1("Could not connect to %s",path);
        return;
    }
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

void testReadAllSDS()
{
    printf("Test SDS connecting to every branch\n");
    testReadBranch("/");
    printf("Connected to every branch\n");
}

static void testDFSDel()
{
    Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user); // disabled, auto-commit

    if (!setupDFS("del"))
        return;

    printf("Creating 'regress::del::super1 and attaching sub\n");
    Owned<IDistributedSuperFile> sfile1 = dir.createSuperFile("regress::del::super1", false, false, user, transaction);
    sfile1->addSubFile("regress::del::sub1", false, NULL, false, transaction);
    sfile1.clear();

    printf("Deleting 'regress::del::sub1, should fail\n");
    try {
        if (dir.removeEntry("regress::del::sub1", user)) {
            ERROR("Could remove sub, this will make the DFS inconsistent!");
            return;
        }
    } catch (IException *) {
        // expecting an exception
    }

    printf("Deleting 'regress::del::super1, should work\n");
    if (!dir.removeEntry("regress::del::super1", user)) {
        ERROR("Can't remove super1");
        return;
    }
    printf("Deleting 'regress::del::sub1, should work\n");
    if (!dir.removeEntry("regress::del::sub1", user)) {
        ERROR("Can't remove sub1");
        return;
    }
}

// ======================================================================= Test Engine

struct TestArray {
    char *name;
    void(*func)();
    TestArray *next;
} Tests = { NULL, NULL, NULL };
TestArray *currentTest = &Tests;

static void registerTest(const char* name, void(*fp)())
{
    assert(name && fp && "Test must have names and a function to run");
    if (currentTest->func) {
        TestArray *t = new TestArray();
        t->name = (char*)name;
        t->func = fp;
        t->next = 0;
        currentTest->next = t;
        currentTest = t;
    } else {
        currentTest->name = (char*)name;
        currentTest->func = fp;
        currentTest->next = 0;
    }
}

static int runTest(const char* match)
{
    int n=0;
    currentTest = &Tests;
    while (currentTest) {
        if (match && strstr(currentTest->name, match) == 0) {
            currentTest = currentTest->next;
            continue;
        }
        printf("\n ++ Running test '%s'\n",currentTest->name);
        try {
            currentTest->func();
        } catch (IException *e) {
            StringBuffer s;
            s.append(" ++ Test Exception");
            pexception(s.str(),e);
            errorcount++;
        }
        n++;
        printf(" ++ Test '%s' complete\n",currentTest->name);
        currentTest = currentTest->next;
    }
    return n;
}

void initTests() {
    registerTest("SDS read/write", testSDSRW);
    registerTest("DFS basics", testDFS);
    registerTest("DFS transaction", testDFSTrans);
    registerTest("DFS promote super file", testDFSPromote);
    registerTest("DFS subdel", testDFSDel);
    registerTest("SDS subscriptions", testSDSSubs);
}

int main(int argc, char* argv[])
{   
    if (argc<2) {
        usage();
        return 1;
    }
    ReleaseAtomBlock rABlock;
    InitModuleObjects();
    EnableSEHtoExceptionMapping();
    user->set("user", "passwd");
    RunOptions opt = read_cmdline(argc, argv);
    initTests();
    unsigned nt = 0;
    printf("Dali Regression Tests\n");
    printf("=====================\n");

    try {
        printf(" ++ Connecting to Dali server at %s\n", opt.dali);
        SocketEndpoint ep;
        ep.set(opt.dali,DALI_SERVER_PORT);
        if (ep.isNull()) {
            usage("could not resolve dali server IP");
            return 1;
        }
        SocketEndpointArray epa;
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 
        initClientProcess(group, DCR_Other);

        // Test
        errorcount = 0;
        if (opt.full) {
            testReadAllSDS();
            nt++;
        }
        nt += runTest(opt.match);

        // Cleanup
        closedownClientProcess();
        setNodeCaching(false);
    }
    catch (IException *e) {
        pexception(" ++ DAREGRESS Exception",e);
        errorcount++;
    }
    printf("\nDali Regression Tests Report\n");
    printf(" ++ Tests completed: %3d\n", nt);
    printf(" ++ Errors reported: %3d\n", errorcount);
    return errorcount;
}

