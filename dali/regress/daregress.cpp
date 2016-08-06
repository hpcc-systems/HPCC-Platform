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
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    const size32_t recsize = 17;
    StringBuffer s;
    unsigned i;
    unsigned n;
    unsigned t;
    queryNamedGroupStore().remove("daregress_group");
    dir.removeEntry("daregress::superfile1");
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
        dir.removeEntry(s.str());
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
        Owned<IDistributedFile> dfile = dir.lookup(s.str());
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
    Owned<IDistributedFileIterator> fiter = dir.getIterator("daregress::*",false);
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
    sfile.setown(dir.createSuperFile("daregress::superfile1",true));
    for (i = 0;i<100;i++) {
        s.clear().append("daregress::test").append(i);
        sfile->addSubFile(s.str());
    }
    sfile.clear();
    sfile.setown(dir.lookupSuperFile("daregress::superfile1"));
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
            dir.removeEntry(s.str());
        t = (t+37)%100; 
    }
    printf("DFile removal complete\n");
    t = 39;
    for (i=0;i<100;i++) {
        if (dir.exists(s.str()))
            ERROR1("Found %s after deletion",s.str());
        Owned<IDistributedFile> dfile = dir.lookup(s.str());
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
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction();

    // Prepare - MORE - Change this when create/remove file is part of transactions
    printf("Cleaning up 'regress::trans' scope\n");
    if (dir.exists("regress::trans::super1",false,true) && !dir.removeEntry("regress::trans::super1")) {
        ERROR("Can't remove super1");
        return;
    }
    if (dir.exists("regress::trans::super2",false,true) && !dir.removeEntry("regress::trans::super2")) {
        ERROR("Can't remove super2");
        return;
    }
    if (dir.exists("regress::trans::super3",false,true) && !dir.removeEntry("regress::trans::super3")) {
        ERROR("Can't remove super3");
        return;
    }

    if (dir.exists("regress::trans::sub1",true,false) && !dir.removeEntry("regress::trans::sub1")) {
        ERROR("Can't remove sub1");
        return;
    }
    printf("Creating 'regress::trans' subfiles(1,4)\n");
    Owned<IFileDescriptor> sub1 = createFileDescriptor("regress::trans", "sub1", 3, 17);
    Owned<IDistributedFile> dsub1 = dir.createNew(sub1);
    dsub1->attach("regress::trans::sub1");
    dsub1.clear();

    if (dir.exists("regress::trans::sub1",true,false) && !dir.removeEntry("regress::trans::sub2")) {
        ERROR("Can't remove sub2");
        return;
    }
    Owned<IFileDescriptor> sub2 = createFileDescriptor("regress::trans", "sub2", 3, 17);
    Owned<IDistributedFile> dsub2 = dir.createNew(sub2);
    dsub2->attach("regress::trans::sub2");
    dsub2.clear();

    if (dir.exists("regress::trans::sub1",true,false) && !dir.removeEntry("regress::trans::sub3")) {
        ERROR("Can't remove sub3");
        return;
    }
    Owned<IFileDescriptor> sub3 = createFileDescriptor("regress::trans", "sub3", 3, 17);
    Owned<IDistributedFile> dsub3 = dir.createNew(sub3);
    dsub3->attach("regress::trans::sub3");
    dsub3.clear();

    if (dir.exists("regress::trans::sub1",true,false) && !dir.removeEntry("regress::trans::sub4")) {
        ERROR("Can't remove sub4");
        return;
    }
    Owned<IFileDescriptor> sub4 = createFileDescriptor("regress::trans", "sub4", 3, 17);
    Owned<IDistributedFile> dsub4 = dir.createNew(sub4);
    dsub4->attach("regress::trans::sub4");
    dsub4.clear();

    // Auto-commit
    printf("Auto-commit test (inactive transaction)\n");
    Owned<IDistributedSuperFile> sfile1 = dir.createSuperFile("regress::trans::super1", false, false, NULL, transaction);
    sfile1->addSubFile("regress::trans::sub1", false, NULL, false, transaction);
    sfile1->addSubFile("regress::trans::sub2", false, NULL, false, transaction);
    sfile1.clear();
    sfile1.setown(dir.lookupSuperFile("regress::trans::super1", NULL, transaction));
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
    Owned<IDistributedSuperFile> sfile2 = dir.createSuperFile("regress::trans::super2", false, false, NULL, transaction);
    sfile2->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
    sfile2->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
    transaction->rollback();
    if (sfile2->numSubFiles() != 0)
        ERROR("transactional rollback failed, some subs were added");
    sfile2.clear();
    sfile2.setown(dir.lookupSuperFile("regress::trans::super2", NULL, transaction));
    if (sfile2.get())
        ERROR("transactional rollback super2 failed, it exists!");

    // Commit - FIXME - adding the superfile inside the transaction exposed a flaw in the DFS that only happens in this case
    printf("Commit test (active transaction)\n");
    Owned<IDistributedSuperFile> sfile3 = dir.createSuperFile("regress::trans::super3", false, false, NULL, transaction);
    transaction->start();
    sfile3->addSubFile("regress::trans::sub3", false, NULL, false, transaction);
    sfile3->addSubFile("regress::trans::sub4", false, NULL, false, transaction);
    transaction->commit();
    sfile3.clear();
    sfile3.setown(dir.lookupSuperFile("regress::trans::super3", NULL, transaction));
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

