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
        child = parent->addPropTree(name, createPTree(name,false));
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
        child = parent->addPropTree(name, createPTree(name,false));
    unsigned ret = fn2(fn2(n,seed,seed*17+11,depth+1,parentname),fn2(seed,m,seed*11+17,depth+1,parentname),seed*19+7,depth+1,parentname);
    parentname.setLength(l);
    return ret;
}


static void test1()
{
    printf("Test SDS read/write\n");
    Owned<IPropertyTree> ref = createPTree("DAREGRESS",false);
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


static void test2() 
{
    const size32_t recsize = 17;
    printf("Test DFS\n");
    StringBuffer s;
    unsigned i;
    unsigned n;
    unsigned t;
    queryNamedGroupStore().remove("daregress_group");
    queryDistributedFileDirectory().removeEntry("daregress::superfile1");
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
        Owned<IPropertyTree> pp = createPTree("Part",false);
        Owned<IFileDescriptor>fdesc = createFileDescriptor();
        fdesc->setDefaultDir("c:\\thordata\\regress");
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
        queryDistributedFileDirectory().removeEntry(s.str());
        StringBuffer cname;
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().createNew(fdesc);
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
        if (!queryDistributedFileDirectory().exists(s.str())) 
            ERROR1("Could not find %s",s.str());
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(s.str());
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
    Owned<IDistributedFileIterator> fiter = queryDistributedFileDirectory().getIterator("daregress::*",false); 
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
            totrows += (unsigned)(piter->query().getFileSize(false,false)/fiter->query().queryProperties().getPropInt("@recordSize",-1));
        }
    }
    piter.clear();
    fiter.clear();
    printf("DFile iterate done     - %d parts, %d rows, CRC sum %"I64F"d\n",np,totrows,crctot);
    Owned<IDistributedSuperFile> sfile;
    sfile.setown(queryDistributedFileDirectory().createSuperFile("daregress::superfile1",true));
    for (i = 0;i<100;i++) {
        s.clear().append("daregress::test").append(i);
        sfile->addSubFile(s.str());
    }
    sfile.clear();
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile("daregress::superfile1"));
    if (!sfile) {
        ERROR("Could not find added superfile");
        return;
    }
    __int64 savcrc = crctot;
    crctot = 0;
    np = 0;
    totrows = 0;
    size32_t srs = (size32_t)sfile->queryProperties().getPropInt("@recordSize",-1);
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
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile("daregress::superfile1"));
    if (sfile)
        ERROR("Superfile deletion failed");
    t = 37;
    for (i=0;i<100;i++) {
        s.clear().append("daregress::test").append(t);
        if (i%1) {
            Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(s.str());
            if (!dfile)
                ERROR1("Could not find %s",s.str());
            dfile->detach();
        }
        else 
            queryDistributedFileDirectory().removeEntry(s.str());
        t = (t+37)%100; 
    }
    printf("DFile removal complete\n");
    t = 39;
    for (i=0;i<100;i++) {
        if (queryDistributedFileDirectory().exists(s.str()))
            ERROR1("Found %s after deletion",s.str());
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(s.str());
        if (dfile)
            ERROR1("Found %s after deletion",s.str());
        t = (t+39)%100; 
    }
    printf("DFile removal check complete\n");
    queryNamedGroupStore().remove("daregress_group");
    if (queryNamedGroupStore().lookup("daregress_group"))
        ERROR("Named group not removed");
}

void testSubscription(bool subscriber, int subs, int comms)
{
    class TestSubscription : public CInterface, implements ISDSSubscription
    {
    public:
        IMPLEMENT_IINTERFACE;
        virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            PrintLog("Notification(%"I64F"x) of %s - flags = %d",(__int64) id, xpath, flags);
            if (valueData)
            {
                StringBuffer data;
                appendURL(&data, (const char *)valueData, valueLen, 0);
                PrintLog("ValueData = %s", data.str());
            }
        }
    };


    unsigned subscriptions = subs==-1?400:subs;
    unsigned commits = comms==-1?400:comms;
    unsigned i;
    if (subscriber) {
            TestSubscription **subs = (TestSubscription **) alloca(sizeof(TestSubscription *)*subscriptions);
            SubscriptionId *ids = (SubscriptionId *) alloca(sizeof(SubscriptionId)*subscriptions);
            for (i=0; i<subscriptions; i++){
                    subs[i] = new TestSubscription;
                    PrintLog("Subscribe %d",i);
                    StringBuffer key;
                    key.append("/TESTS/TEST").append(i);
                    ids[i] = querySDS().subscribe(key.str(), *subs[i], true);
            }
            PrintLog("paused 1");
            getchar();
            for (i=0; i<subscriptions; i++)  {
                    querySDS().unsubscribe(ids[i]);
                    subs[i]->Release();
            }
            PrintLog("paused");
            getchar();
    }
    else {
            Owned<IRemoteConnection> conn = querySDS().connect("/DAREGRESS",myProcessSession(), RTM_CREATE_QUERY, 1000000);
            IPropertyTree *root = conn->queryRoot();
            unsigned i, _i;

            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i);
                    root->setPropTree(key.str(), createPTree(false));

            }
            conn->commit();

            PrintLog("paused 1");

            getchar();
            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i).append("/index");
                    root->setPropInt(key.str(), i);
                    PrintLog("Commit %d", i);
                    conn->commit();
            }
            PrintLog("paused 2");
            getchar();
            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i).append("/index");
                    root->setPropInt(key.str(), subscriptions-i);            
                    conn->commit();
            }
            PrintLog("paused 3");
            getchar();
            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i).append("/index");
                    root->setPropInt(key.str(), i);         
                    conn->commit();
            }
    }
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


void test3()
{
    printf("Test SDS subscriptions\n");
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


static bool runTest(unsigned i)
{
    try {
        switch (i) {
        case 1: test1(); break;
        case 2: test2(); break;
        case 3: test3(); break;
        default:
            return false;
        }
        printf("Test #%d complete\n",i);
    }
    catch (IException *e) {
        StringBuffer s;
        s.append("DAREGRESS Test #").append(i).append(" Exception");
        pexception(s.str(),e);
    }
    return true; 
}

extern void runDafsTest();


static void usage(const char *error=NULL)
{
    if (error) 
        printf("ERROR: %s\n", error);
    printf("usage: DAREGRESS <dali-ip>"); 
    printf("or:    DAREGRESS <dali-ip> { test-number> } [ full ]"); 
}

struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };


int main(int argc, char* argv[])
{   
    ReleaseAtomBlock rABlock;
    InitModuleObjects();

    EnableSEHtoExceptionMapping();

    try {
        StringBuffer cmd;
        splitFilename(argv[0], NULL, NULL, &cmd, NULL);
        openLogFile(cmd.toLowerCase().append(".log").str());
        if (argc<2) {
            usage();
            return 1;
        }
        printf("Dali Regression Tests\n");
        printf("=====================\n");
        SocketEndpoint ep;
        ep.set(argv[1],DALI_SERVER_PORT);
        if (ep.isNull()) {
            usage("could not resolve dali server IP");
            return 1;
        }
        SocketEndpointArray epa;
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 
        initClientProcess(group, DCR_Other);
        errorcount = 0;
        unsigned nt = 0;
        bool done=false;
        bool full=false;
        loop {
            for (unsigned i=2;i<argc;i++) {
                if (stricmp(argv[i],"test")==0) {
                    runDafsTest();
                    done = true;
                    break;
                }
                else if (stricmp(argv[i],"full")==0)
                    full = true;
                else {
                    unsigned n = atoi(argv[i]);
                    if (n) {
                        done = true;
                        runTest(n);
                    }
                }
            }
            if (!done) {
                for (unsigned i=1;;i++) {
                    if (!runTest(i)) 
                        break;
                    nt++;
                }
            }
            if (full) {
                testReadAllSDS(); 
            }
            printf("%d Test%s completed, %d error%s reported\n",nt,(nt!=1)?"s":"",errorcount,(errorcount!=1)?"s":"");
            break;
        }
        closedownClientProcess();
        setNodeCaching(false);
    }
    catch (IException *e) {
        pexception("DAREGRESS Exception",e);
    }
    
    return 0;
}

