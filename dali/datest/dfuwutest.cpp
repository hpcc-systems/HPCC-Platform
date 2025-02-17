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
#include "jmisc.hpp"
#include "dfuwu.hpp"
#include "jsuperhash.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "rmtfile.hpp"
#include "daclient.hpp"
#include "dafdesc.hpp"
#include "dadfs.hpp"
#include "dautils.hpp"

#include "dfuutil.hpp"



void testProgressUpdate()
{
    // TBD
}

void testAbort(const char *wuid)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(wuid,false);
    if (wu) {
        wu->requestAbort();
    }
    else
        UERRLOG("WUID %s not found", wuid);
}

static StringBuffer& constructFileMask(const char* filename, StringBuffer& filemask)
{
    filemask.clear().append(filename).toLowerCase().append("._$P$_of_$N$");
    return filemask;
}

void testProgressMonitor(const char *wuid)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit(wuid,false);
    class cProgressMon: public CInterface, implements IDFUprogressSubscriber
    {
    public:
        IMPLEMENT_IINTERFACE;

        void notify(IConstDFUprogress *progress)
        {
            StringBuffer s;
            progress->formatProgressMessage(s);
            PROGLOG(" %s",s.str());
        }

    } progressmon;
    wu->subscribeProgress(&progressmon);
    wu->waitForCompletion(1000*10*60);
    wu->subscribeProgress(NULL);
    IConstDFUprogress* progress = wu->queryProgress();
    StringBuffer s;
    PROGLOG(" state    = %s",encodeDFUstate(progress->getState(),s.clear()).str());
    progress->formatSummaryMessage(s.clear());
    PROGLOG(" %s",s.str());
}

void copyTest()
{
    for (unsigned i=0;i<1000;i++) {
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        wu->setClusterName("thor18way");
        StringBuffer teststr;
        teststr.append("Test").append(i+1);
        wu->setJobName(teststr.str());
        wu->setQueue("dfuserver_queue");
        wu->setUser("Nigel");
        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();
        IDFUprogress *progress = wu->queryUpdateProgress();
        StringBuffer srcname;
        srcname.append("thor_dev::testfilem").append(i%8+1);
        StringBuffer dstname;
        dstname.append("thor_dev::testfilem").append(i%8+2);
        wu->setCommand(DFUcmd_copy);
        source->setLogicalName(srcname.str());
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        Owned<IGroup> grp = queryNamedGroupStore().lookup("thor18way");
        for (unsigned j=0;j<18;j++) {
            StringBuffer filename;
            filename.append("/c$/thordata/testfile").append(i%8+2).append("M._");
            filename.append(j+1);
            filename.append("_of_18");
            fdesc->setPart(j,&grp->queryNode(j),filename.str());
        }
        destination->setFromFileDescriptor(*fdesc);
        destination->setLogicalName(dstname.str());
        options->setReplicate(true);
        options->setOverwrite(true);
        submitDFUWorkUnit(wu.getClear());
    }
}

void importTest()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setClusterName("thor18way");
    StringBuffer teststr;
    teststr.append("Import Test");
    wu->setJobName(teststr.str());
    wu->setQueue("dfuserver_queue");
    wu->setUser("Nigel");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    StringBuffer dstname;
    dstname.append("thor_dev::testinput");
    wu->setCommand(DFUcmd_import);
    SocketEndpoint destep("rigel",7071);
    RemoteFilename destfn;
    destfn.setPath(destep,"/export/home/nhicks/daliservix/testfile.d00");
    source->setSingleFilename(destfn);
    source->setRecordSize(64);  // needed cause going to split file
    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    Owned<IGroup> grp = queryNamedGroupStore().lookup("thor18way");
    for (unsigned j=0;j<18;j++) {
        StringBuffer filename;
        filename.append("/c$/thordata/importtest._");
        filename.append(j+1);
        filename.append("_of_18");
        fdesc->setPart(j, &grp->queryNode(j),filename.str());
    }
    destination->setFromFileDescriptor(*fdesc);
    destination->setLogicalName(dstname.str());
    options->setReplicate(true);
    options->setOverwrite(true);
    submitDFUWorkUnit(wu.getClear());
}

void SprayTest(unsigned num)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    StringBuffer tmp;
    for (unsigned n = 0; n<num; n++) {
        for (int spray=1;spray>=0;spray--) {
            Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
            wu->setClusterName("thor_fi_01");
            if (spray)
                tmp.clear().append("Test spray #").append(n+1);
            else
                tmp.clear().append("Test despray #").append(n+1);
            wu->setJobName(tmp.str());
            wu->setQueue("dfuserver_queue");
            wu->setUser("Nigel");
            IDFUfileSpec *source = wu->queryUpdateSource();
            IDFUfileSpec *destination = wu->queryUpdateDestination();
            IDFUoptions *options = wu->queryUpdateOptions();
            IDFUprogress *progress = wu->queryUpdateProgress();
            if (spray) {
                wu->setCommand(DFUcmd_import);
                RemoteFilename rfn;
                SocketEndpoint ep("192.168.168.254");
                rfn.setPath(ep,"/data/testfile1.d00");
                source->setSingleFilename(rfn);
                source->setTitle("testfile1.d00");
                source->setRecordSize(32);  // needed cause going to split file
                destination->setLogicalName("testing::nigel::testfile1");
                destination->setDirectory("/c$/thordata/testing/nigel");
                destination->setFileMask("testfile1._$P$_of_$N$");                  
                destination->setGroupName("thor_fi_01");    
                options->setReplicate(true);
                options->setOverwrite(true);
            }
            else {
                wu->setCommand(DFUcmd_export);
                source->setLogicalName("testing::nigel::testfile1");
                RemoteFilename rfn;
                SocketEndpoint ep("192.168.168.254");
                rfn.setPath(ep,"/data/testfile1.d01");
                destination->setSingleFilename(rfn);
                destination->setTitle("testfile1.d02");
                options->setOverwrite(true);
            }
            PROGLOG("submitting %s",wu->queryId()); 
            submitDFUWorkUnit(wu.getClear());
        }
    }
}


static void printDesc(IFileDescriptor *desc)
{
    if (desc) {
        Owned<IPropertyTree> pt = desc->getFileTree();
        StringBuffer out;
        toXML(pt,out);
        PROGLOG("\n%s",out.str());
        unsigned n = desc->numParts();
        PROGLOG("  numParts = %d",n);
        PROGLOG("  numCopies(0) = %d",desc->numCopies(0));
        StringBuffer tmp1;
        unsigned i;
        for (i=0;i<n;i++) {
            unsigned copy;
            for (copy = 0;copy<desc->numCopies(i);copy++) {
                RemoteFilename rfn;
                desc->getFilename(i,copy,rfn);
                PROGLOG("  file (%d,%d) = %s",copy,i,rfn.getRemotePath(tmp1.clear()).str());
            }
        }
    }
}


void testWUcreate(int kind,StringBuffer &wuidout)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setClusterName("thor_data400");
    wu->setJobName("test job");
    wu->setQueue("nigel_dfuserver_queue");
    wu->setUser("Nigel");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    switch (kind) {
    case 1:  // 1->N spray
        {
            wu->setCommand(DFUcmd_import);
            RemoteFilename rfn;
            SocketEndpoint ep("172.16.18.175");
            rfn.setPath(ep,"/export/home/nhicks/test/testfile1.d00");
            source->setSingleFilename(rfn);
            source->setTitle("testfile1.d00");
            source->setRecordSize(32);  // needed cause going to split file
            destination->setLogicalName("thor_dev::nigel::testfile1");
            destination->setDirectory("c:\\thordata\\thor_dev\\nigel");
            destination->setFileMask("testfile1._$P$_of_$N$");                  
            destination->setGroupName("thor_dev");  
            options->setReplicate(true);
            options->setOverwrite(true);
        }
        break;
    case 2:  // N->1 despray
        {
            wu->setCommand(DFUcmd_export);
            source->setLogicalName("thor_dev::nigel::testfile1");
            RemoteFilename rfn;
            SocketEndpoint ep("172.16.18.175");
            rfn.setPath(ep,"/export/home/nhicks/test/testfile2.d00");
            destination->setSingleFilename(rfn);
            destination->setTitle("testfile2.d00");
        }
        break;
    case 3:  // N->N copy
        {   // simple copy using group as destination
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName("thor_dev::nigel::testfile1");
            destination->setLogicalName("thor_dev::nigel::testfile2");
            destination->setDirectory("c:\\thordata\\thor_dev\\nigel");
            destination->setFileMask("testfile2._$P$_of_$N$");                  
            destination->setGroupName("thor_dev");  
            options->setReplicate(true);
        }
        break;
    case 4:  // N->M copy
        {   // copy specifying nodes
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName("thor_dev::nigel::testfile2");
            destination->setLogicalName("nigeltest::testfile3");
            Owned<IFileDescriptor> fdesc = createFileDescriptor();
            Owned<INode> node;
            node.setown(createINode("10.173.4.3"));
            fdesc->setPart(0,node,"/c$/roxiedata/part._1_of_10");
            node.setown(createINode("10.173.4.4"));
            fdesc->setPart(1,node,"/c$/roxiedata/part._2_of_10");
            node.setown(createINode("10.173.4.5"));
            fdesc->setPart(2,node,"/c$/roxiedata/part._3_of_10");
            node.setown(createINode("10.173.4.6"));
            fdesc->setPart(3,node,"/c$/roxiedata/part._4_of_10");
            node.setown(createINode("10.173.4.7"));
            fdesc->setPart(4,node,"/c$/roxiedata/part._5_of_10");
            node.setown(createINode("10.173.4.8"));
            fdesc->setPart(5,node,"/c$/roxiedata/part._6_of_10");
            node.setown(createINode("10.173.4.9"));
            fdesc->setPart(6,node,"/c$/roxiedata/part._7_of_10");
            node.setown(createINode("10.173.4.10"));
            fdesc->setPart(7,node,"/c$/roxiedata/part._8_of_10");
            node.setown(createINode("10.173.4.11"));
            fdesc->setPart(8,node,"/c$/roxiedata/part._9_of_10");
            node.setown(createINode("10.173.4.12"));
            fdesc->setPart(9,node,"/c$/roxiedata/part._10_of_10");
            destination->setFromFileDescriptor(*fdesc);
            options->setReplicate(true);
            destination->setWrap(true);
            destination->setNumPartsOverride(50);
            Owned<IFileDescriptor> fdesc2 = destination->getFileDescriptor(false);
            printDesc(fdesc2);


        }
        break;
// tests
    case 5:  // N->N copy
        {   // simple copy using group as destination
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName("nigel18::dist1.out");
            destination->setLogicalName("nigel18::dist1.cpy");
            destination->setDirectory("c:\\thordata\\nigel18");
            destination->setFileMask("dist1.cpy._$P$_of_$N$");                  
            destination->setGroupName("nigel18");   
            options->setReplicate(true);
        }
        break;
    case 6:  // 1->N spray
        {   // simple copy using group as destination
            wu->setCommand(DFUcmd_import);
            RemoteFilename rfn;
            SocketEndpoint ep("10.150.10.16");
            rfn.setPath(ep,"c:\\thordata\\out1.d00._1_of_1");
            source->setSingleFilename(rfn);
            source->setTitle("testfile1.d00");
            source->setRecordSize(16);  // needed cause going to split file
            destination->setLogicalName("nigel18::testout1");
            destination->setDirectory("c:\\thordata\\nigel18");
            destination->setFileMask("testout1._$P$_of_$N$");                   
            destination->setGroupName("nigel18");   
            options->setReplicate(true);
            options->setOverwrite(true);
        }
        break;
    case 7:  // copy from an external Dali
        {   
            wu->setCommand(DFUcmd_copy);
            source->setLogicalName("nigel18::dist1.out");
            SocketEndpoint ep("10.150.10.75");
            source->setForeignDali(ep);
            destination->setLogicalName("nigel18::dist2.cpy");
            destination->setDirectory("c:\\thordata\\nigel18");
            destination->setFileMask("dist2.cpy._$P$_of_$N$");                  
            destination->setGroupName("nigel18");   
            options->setReplicate(true);
            options->setOverwrite(true);
        }
        break;
    case 8: // monitor test
        {
            wu->setCommand(DFUcmd_monitor);
            wu->setQueue("dfumonitor_queue");
            IDFUmonitor *monitor = wu->queryUpdateMonitor();
            source->setLogicalName("nigel18::dist1.out");
            monitor->setShotLimit(1);
        }
        break;
    case 9: // copy test
        {
            wu->setCommand(DFUcmd_copy);
            SocketEndpoint ep("10.150.29.161");
            source->setForeignDali(ep);
            source->setForeignUser("nhicks","h1ck5");
            source->setLogicalName("thor_data400::nhtest::testspray1");
            destination->setLogicalName("thor_data400::nhtest::testspray1");
            destination->setDirectory("c:\\thordata\\thor_data400\\nhtest");
            destination->setFileMask("testspray1._$P$_of_$N$");                 
            destination->setGroupName("thor_data400");  
            options->setReplicate(true);
            options->setOverwrite(true);
        }
        break;
    case 10: // copy test
        {
            wu->setCommand(DFUcmd_copy);
            SocketEndpoint ep("10.150.29.161");
            source->setForeignDali(ep);
            source->setForeignUser("nhicks","h1ck5");
            source->setLogicalName("thor_data400::base::hss_name_phonew20050107-143927");
            destination->setLogicalName("thor_data400::nhtest::testcopy1");
            destination->setDirectory("c:\\thordata\\thor_data400\\nhtest");
            destination->setFileMask("testcopy1._$P$_of_$N$");                  
            destination->setGroupName("thor_data400");  
            options->setReplicate(true);
            options->setOverwrite(true);
        }
        break;
    }
    wuidout.append(wu->queryId());  
    submitDFUWorkUnit(wu.getClear());
}

IFileDescriptor *createRoxieFileDescriptor(const char *cluster, const char *lfn, bool servers)
{
    Owned<IRemoteConnection> envconn = querySDS().connect("/Environment",myProcessSession(),RTM_LOCK_READ, INFINITE);
    if (!envconn) {
        DBGLOG("Could not connect to %s",lfn);
        return NULL;
    }
    Owned<IPropertyTree> envroot = envconn->getRoot();
    StringBuffer grpname(cluster);
    if (servers)
        grpname.append("__servers");
    Owned<IGroup> grp = queryNamedGroupStore().lookup(grpname.str());
    if (!grp) {
        UERRLOG("Logical group %s not found",grpname.str());
        return NULL;
    }
    Owned<IFileDescriptor> ret = createFileDescriptor();
    unsigned width = grp->ordinality();
    for (unsigned i=0;i<width;i++) {
        StringBuffer filename;
        StringBuffer dirxpath;
        dirxpath.appendf("Software/RoxieCluster[@name=\"%s\"]/Roxie%sProcess[@channel=\"%d\"]/@dataDirectory",cluster,servers?"Server":"Slave",i+1);
        const char * dir = envroot->queryProp(dirxpath.str());
        if (!dir) {
            UERRLOG("dataDirectory not specified");
            return NULL;
        }
        makePhysicalPartName(lfn,i+1,width,filename,0,DFD_OSdefault,dir,false,0);
        RemoteFilename rfn;
        rfn.setPath(grp->queryNode(i).endpoint(),filename.str());
        ret->setPart(i,rfn,NULL);
    }
    return ret.getClear();
}


void testRoxieDest()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setClusterName("thor_data400");
    wu->setJobName("test job");
    wu->setQueue("nigel_dfuserver_queue");
    wu->setUser("Nigel");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    destination->setLogicalName("thor_data50::key::stuminiquery_name_password");
    destination->setDirectory("/e$/test");
    Owned<IFileDescriptor> desc = createRoxieFileDescriptor("roxie","thor_data50::key::stuminiquery_name_password",false);
    destination->setFromFileDescriptor(*desc);
    StringBuffer fileMask;
    constructFileMask("thor_data50::key::stuminiquery_name_password", fileMask);
    destination->setFileMask(fileMask.str());
    destination->setGroupName("roxie");

    PROGLOG("%s",wu->queryId());
}



void testRoxieCopies()
{
    queryNamedGroupStore().add("__test_cluster_10", { "10.173.10.81-90" }, true);
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    for (unsigned i= 0; i<18;i++) {
        Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
        wu->setClusterName("thor_data400");
        wu->setJobName("test job");
        wu->setQueue("nigel_dfuserver_queue");
        wu->setUser("Nigel");
        IDFUfileSpec *source = wu->queryUpdateSource();
        IDFUfileSpec *destination = wu->queryUpdateDestination();
        IDFUoptions *options = wu->queryUpdateOptions();
        IDFUprogress *progress = wu->queryUpdateProgress();
#if 1
        destination->setLogicalName("thor_data50::key::testfile");
#else
        destination->setDirectory("/c$/roxiedata/thor_data50/key");
        destination->setFileMask("testfile._$P$_of_$N$");
#endif

        destination->setGroupName("__test_cluster_10");
        unsigned np = (i>=9)?51:10;
        ClusterPartDiskMapSpec mspec;
        switch(i%9) {
        case 0:
            if (np==10)
                np = 5;
            PROGLOG("%d parts, full_redundancy",np);
            mspec.setRoxie(1,1,0);
            destination->setClusterPartDiskMapSpec("__test_cluster_10",mspec);
            destination->setClusterPartDefaultBaseDir("__test_cluster_10","/c$/roxiedata");
            options->setReplicate();
            break;
        case 1:
            PROGLOG("%d parts, cyclic_redundancy",np);
            mspec.setRoxie(1,2,1);
            destination->setClusterPartDiskMapSpec("__test_cluster_10",mspec);
            destination->setClusterPartDefaultBaseDir("__test_cluster_10","/c$/roxiedata");
            options->setReplicate();
            break;
        case 2:
            PROGLOG("%d parts, no_redundancy",np);
            mspec.setRoxie(0,1);
            destination->setClusterPartDiskMapSpec("__test_cluster_10",mspec);
            destination->setClusterPartDefaultBaseDir("__test_cluster_10","/c$/roxiedata");
            break;
        case 3:
            PROGLOG("%d parts, overloaded",np);
            if (np==10)
                np = 20;
            mspec.setRoxie(0,2);
            destination->setClusterPartDiskMapSpec("__test_cluster_10",mspec);
            destination->setClusterPartDefaultBaseDir("__test_cluster_10","/c$/roxiedata");
            break;
        case 4:
            PROGLOG("%d parts, thor_cyclic_redundancy",np);
            mspec.setRoxie(1,2,1);
            destination->setClusterPartDiskMapSpec("__test_cluster_10",mspec);
            destination->setClusterPartDefaultBaseDir("__test_cluster_10","/c$/roxiedata");
            options->setReplicate();
            break;
        case 5:
            options->setReplicate();
            // fall through
        default:
            if (i==8)
                np = 20;
            PROGLOG("%d parts, mapping mode %d",np,(i%9-5));
            destination->setClusterPartDiskMapping((DFUclusterPartDiskMapping)(i%9-5),"/c$/roxiedata", "__test_cluster_10");
        }
#if 1
        destination->setNumPartsOverride(np);
        destination->setWrap(true); //??
#endif
        PROGLOG("A======================");
        StringBuffer buf;
        wu->toXML(buf);
        PROGLOG("\n%s",buf.str());

        Owned<IFileDescriptor> fdesc2 = destination->getFileDescriptor(false);
        PROGLOG("B--------------------------");
        printDesc(fdesc2);
        PROGLOG("C--------------------------");

        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc2);
        Owned<IFileDescriptor> fdesc3 = file->getFileDescriptor();
        printDesc(fdesc3);
        PROGLOG("E--------------------------");
        StringBuffer fn("testing::file");
        fn.append(i);
        queryDistributedFileDirectory().removeEntry(fn.str(),UNKNOWN_USER);
        file->attach(fn.str(),UNKNOWN_USER);
        file.clear();
        file.setown(queryDistributedFileDirectory().lookup(fn.str(),UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser));
        Owned<IFileDescriptor> fdesc4 = file->getFileDescriptor();
        printDesc(fdesc4);
    }
}


void test2()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->updateWorkUnit("D20060303-110019",false);
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    if (destination->getWrap())
        destination->setNumPartsOverride(51);
    Owned<IFileDescriptor> desc =  destination->getFileDescriptor();
    printDesc(desc);
}


void test3()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IConstDFUWorkUnit> wu = factory->openWorkUnit("D20060815-145448",false);
    IConstDFUfileSpec *src = wu->querySource();
    Owned<IFileDescriptor> desc =  src->getFileDescriptor();
    printDesc(desc);
    Owned<IFileDescriptor> desc2 =  src->getFileDescriptor();
    printDesc(desc2);
}


void testMultiFilename()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setClusterName("thor_data400");
    RemoteMultiFilename rmfn;
    rmfn.append("\"c:\\import\\tmp1\\x,y\",*._?_of_3");
    wu->queryUpdateSource()->setMultiFilename(rmfn);
}

void testIterate()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();   
    Owned<IConstDFUWorkUnitIterator> iter = //factory->getWorkUnitsByOwner("Nigel");
                                            factory->getWorkUnitsByState(DFUstate_started);
    StringBuffer wuid;
    StringBuffer s;
    ForEach(*iter) {
        if (iter->getId(wuid.clear()).length()) {
            Owned<IConstDFUWorkUnit> wu = iter->get();
            PROGLOG("%s:",wuid.str());
            PROGLOG("  cluster  = %s",wu->getClusterName(s.clear()).str());
            PROGLOG("  job name = %s",wu->getJobName(s.clear()).str());
            PROGLOG("  queue    = %s",wu->getQueue(s.clear()).str());
            PROGLOG("  state    = %s",encodeDFUstate(wu->queryProgress()->getState(),s.clear()).str());
    
            IConstDFUfileSpec * file = wu->queryDestination(); 
            StringBuffer tmp;
            PROGLOG("  dest    = %s",file->getTitle(tmp).str());

        }
    }
}

void testPagedIterate()
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();   
    __int64 cachehint=0;
    unsigned n=0;
    for (unsigned page=0;page<3;page++) {
        DFUsortfield sortorder[] = {DFUsf_user,DFUsf_state,DFUsf_term};
        Owned<IConstDFUWorkUnitIterator> iter = factory->getWorkUnitsSorted(sortorder, NULL, NULL, page*10, 10, "nigel", &cachehint, NULL, nullptr);
        StringBuffer s;
        ForEach(*iter) {

            Owned<IConstDFUWorkUnit> wu = iter->get();
            PROGLOG("%s:",wu->queryId());
            PROGLOG("  cluster  = %s",wu->getClusterName(s.clear()).str());
            PROGLOG("  job name = %s",wu->getJobName(s.clear()).str());
            PROGLOG("  queue    = %s",wu->getQueue(s.clear()).str());
            PROGLOG("  user     = %s",wu->getUser(s.clear()).str());
            PROGLOG("  state    = %s",encodeDFUstate(wu->queryProgress()->getState(),s.clear()).str());
    
            IConstDFUfileSpec * file = wu->queryDestination(); 
            StringBuffer tmp;
            PROGLOG("  dest    = %s",file->getTitle(tmp).str());

        }
    }
}


#if 0
void testDFUwuqueue()
{
    StringAttrArray wulist;
    unsigned njobs = queuedJobs("nigel_dfuserver_queue",wulist);
    PROGLOG("njobs = %d",njobs);
    for (unsigned i=0; i<wulist.ordinality(); i++) {
        PROGLOG("job[%d] = %s",i,wulist.item(i).text.get());
    }
}
#else
void testDFUwuqueue(const char *name)
{
    StringAttrArray wulist;
    unsigned running = queuedJobs(name,wulist); 
    StringBuffer cmd;
    StringBuffer cname;
    StringBuffer jname;
    StringBuffer uname;
    ForEachItemIn(i,wulist) {
        const char *wuid = wulist.item(i).text.get();
        Owned<IConstDFUWorkUnit> wu = getDFUWorkUnitFactory()->openWorkUnit(wuid,false);
        if (wu) 
            PROGLOG("%s: %s,%s,%s,%s,%s",wuid,i<running?"Running":"Queued",
              encodeDFUcommand(wu->getCommand(),cmd.clear()).str(),
              wu->getClusterName(cname.clear()).str(),
              wu->getUser(uname.clear()).str(),
              wu->getJobName(jname.clear()).str()
            );
    }
}
    
#endif

#define WAIT_SECONDS 30

void testSuperRemoteCopy(const char *remoteip,const char *file)
{
    Owned<IDfuFileCopier> copier = createRemoteFileCopier("dfuserver_queue","thor","Test1",true);
    Owned<IDFUhelper> helper = createIDFUhelper();
    SocketEndpoint ep(remoteip);
    helper->superForeignCopy(file,ep,file,UNKNOWN_USER,UNKNOWN_USER,false,copier);
}


void testRepeatedFiles1(StringBuffer &wuid)
{
    // IMPORT single cluster, part repeated
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test job 1");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_import);
    RemoteFilename rfn;
    SocketEndpoint ep("10.173.34.80");
    rfn.setPath(ep,"d:\\thordata\\test1._1_of_1");
    source->setSingleFilename(rfn);
    source->setTitle("test1");
    source->setRecordSize(10);  // needed cause going to split file


    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    Owned<IGroup> grp = queryNamedGroupStore().lookup("thor");
    fdesc->setDefaultDir("/c$/thordata/thor_dev/nigel");
    fdesc->setPartMask("testspray1._$P$_of_$N$");
    fdesc->setNumParts(19);
    ClusterPartDiskMapSpec mapping;
    mapping.setRepeatedCopies(18,false);
    fdesc->addCluster("thor",grp,mapping);

    destination->setFromFileDescriptor(*fdesc);
    destination->setLogicalName("thor_dev::nigel::testspray1");
    options->setReplicate(true);
    options->setOverwrite(true);
    submitDFUWorkUnit(wu.getClear());
}

void testRepeatedFiles2(StringBuffer &wuid)
{
    // COPY single cluster, part repeated  (uses file created by 1)
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test job 2");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName("thor_dev::nigel::testspray1");
    source->setTitle("test2");
    source->setRecordSize(10);  // needed cause going to split file


    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    Owned<IGroup> grp = queryNamedGroupStore().lookup("thor");
    fdesc->setDefaultDir("/c$/thordata/thor_dev/nigel");
    fdesc->setPartMask("testcopy1._$P$_of_$N$");
    fdesc->setNumParts(19);
    ClusterPartDiskMapSpec mapping;
    mapping.setRepeatedCopies(18,false);
    fdesc->addCluster("thor",grp,mapping);

    destination->setFromFileDescriptor(*fdesc);
    destination->setLogicalName("thor_dev::nigel::testcopy1");
    options->setReplicate(true);
    options->setOverwrite(true);
    submitDFUWorkUnit(wu.getClear());
}

static IGroup *getAuxGroup()
{
    IGroup *auxgrp = queryNamedGroupStore().lookup("test_dummy_group");
    if (!auxgrp)
    {
        queryNamedGroupStore().add("test_dummy_group", { "10.173.34.70-77" }, true, "/c$/dummydata");
        auxgrp = queryNamedGroupStore().lookup("test_dummy_group");
    }
    return auxgrp;
}

void testRepeatedFiles3(StringBuffer &wuid)
{
    // lets make a dummy cluster if doesn't exist already
    Owned<IGroup> auxgrp =  getAuxGroup();
    // IMPORT double cluster, part repeated
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test job 3");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_import);
    RemoteFilename rfn;
    SocketEndpoint ep("10.173.34.80");
    rfn.setPath(ep,"d:\\thordata\\test1._1_of_1");
    source->setSingleFilename(rfn);
    source->setTitle("test3");
    source->setRecordSize(10);  // needed cause going to split file


    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    Owned<IGroup> grp = queryNamedGroupStore().lookup("thor");
    fdesc->setDefaultDir("/c$/thordata/thor_dev/nigel");
    fdesc->setPartMask("testspray3._$P$_of_$N$");
    fdesc->setNumParts(19);
    ClusterPartDiskMapSpec mapping;
    mapping.setRepeatedCopies(18,false);
    fdesc->addCluster("thor",grp,mapping);
    ClusterPartDiskMapSpec auxmapping;
    auxmapping.setRepeatedCopies(18,true);
    auxmapping.setDefaultBaseDir("/c$/dummydata");
    fdesc->addCluster("test_dummy_group",grp,auxmapping);

    destination->setFromFileDescriptor(*fdesc);
    destination->setLogicalName("thor_dev::nigel::testspray2");
    options->setReplicate(true);
    options->setOverwrite(true);
    submitDFUWorkUnit(wu.getClear());
}

void testRepeatedFiles4(StringBuffer &wuid)
{
    // COPY dual cluster, part repeated on cluster 1 only repeated cluster2 (uses file created by 1)
    Owned<IGroup> auxgrp =  getAuxGroup();
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test job 2");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName("thor_dev::nigel::testspray1");
    source->setTitle("test4");
    source->setRecordSize(10);  // needed cause going to split file
    destination->setLogicalName("thor_dev::nigel::testcopy2");
//  destination->setDirectory("/c$/thordata/thor_dev/nigel");
    destination->setFileMask("testcopy2._$P$_of_$N$");
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/thordata","thor",true,false);
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/dummydata","test_dummy_group",true,true);
    destination->setWrap(true);
    options->setReplicate(true);
    options->setOverwrite(true);
    submitDFUWorkUnit(wu.getClear());
}

void testRepeatedFiles5(StringBuffer &wuid)
{
    // COPY dual cluster, part repeated on cluster 1 only repeated cluster2 (uses file created by 1)
    Owned<IGroup> auxgrp =  getAuxGroup();
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test job 2");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName("thor_data400::in::uccv2::nyc::party::init");
    SocketEndpoint ep("10.173.28.12");
    source->setForeignDali(ep);
    source->setTitle("test4");
    source->setRecordSize(10);  // needed cause going to split file
    destination->setLogicalName("thor_dev::nigel::testcopy3");
//  destination->setDirectory("/c$/thordata/thor_dev/nigel");
    destination->setFileMask("testcopy2._$P$_of_$N$");
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/thordata","thor",true,false);
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/dummydata","test_dummy_group",true,true);
    destination->setWrap(true);
    options->setReplicate(true);
    options->setOverwrite(true);
    options->setSuppressNonKeyRepeats(true);                            // **** only repeat last part when src kind = key
    submitDFUWorkUnit(wu.getClear());
}

void testSuperCopy1(StringBuffer &wuid)
{
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wuid.append(wu->queryId()); 
    wu->setClusterName("thor");
    wu->setJobName("test super copy 1");
    wu->setQueue("dfuserver_queue");
    wu->setUser("nhicks");
    IDFUfileSpec *source = wu->queryUpdateSource();
    IDFUfileSpec *destination = wu->queryUpdateDestination();
    IDFUoptions *options = wu->queryUpdateOptions();
    IDFUprogress *progress = wu->queryUpdateProgress();
    wu->setCommand(DFUcmd_supercopy);
    //source->setLogicalName("thor_data400::in::vehreg_nv_ttl_update");
    source->setLogicalName("thor_data400::in::uccv2::nyc::party");
    SocketEndpoint ep("10.173.28.12");
    source->setForeignDali(ep);
    destination->setLogicalName("nigel::testsupercopy1");
//  destination->setDirectory("/c$/thordata/thor_dev/nigel");
//  destination->setFileMask("testcopy2._$P$_of_$N$");
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/thordata","thor",true,false);
    destination->setClusterPartDiskMapping(DFUcpdm_c_replicated_by_d,"/c$/dummydata","test_dummy_group",true,true);
    destination->setWrap(true);
    destination->setRoxiePrefix("testprefix");
    options->setReplicate(true);
    options->setOverwrite(true);
    options->setSuppressNonKeyRepeats(true);
    submitDFUWorkUnit(wu.getClear());
}






struct ReleaseAtomBlock { ~ReleaseAtomBlock() { releaseAtoms(); } };
int main(int argc, char* argv[])
{   
    ReleaseAtomBlock rABlock;
    InitModuleObjects();

//  EnableSEHtoExceptionMapping();
    try {

        CDfsLogicalFileName dlfn;
        verifyex(dlfn.setValidate("foreign::10.173.28.12:7070::thor_data400::in::uccv2::20061115::nyc::party",true));


        StringBuffer cmd;
        splitFilename(argv[0], NULL, NULL, &cmd, NULL);
        StringBuffer lf;
        openLogFile(lf, cmd.toLowerCase().append(".log").str());
        PROGLOG("DFUWUTEST Starting");
        SocketEndpoint ep;
        SocketEndpointArray epa;
        ep.set(argv[1],DALI_SERVER_PORT);
        epa.append(ep);
        Owned<IGroup> group = createIGroup(epa); 
        initClientProcess(group,DCR_Testing);
        if (0) { 
            //test2();
            //testMultiFilename();
            //testPagedIterate();
            //test3();
            //testSuperRemoteCopy(argv[2],argv[3]);
        }
        else if ((argc>3)&&(stricmp(argv[2],"abort")==0)) {
            testAbort(argv[3]);
        }
        else {
//          testRoxieDest();

//          SprayTest(atoi(argv[2]));
//          importTest();

//          testRoxieCopies();
            StringBuffer wuid;

//          testRepeatedFiles1(wuid);
//          testRepeatedFiles2(wuid);
//          testRepeatedFiles3(wuid);
//          testRepeatedFiles4(wuid);
            testSuperCopy1(wuid);
//          testRepeatedFiles5(wuid);

            
//          testWUcreate(1,wuid.clear());
//          testWUcreate(2,wuid.clear());
//          testWUcreate(3,wuid.clear());
//          testWUcreate(4,wuid.clear());
//          testWUcreate(5,wuid.clear());
//          testWUcreate(6,wuid.clear());
//          testWUcreate(7,wuid.clear());
//          testIterate();
//          testProgressMonitor(wuid.str());
//          testWUcreate(8,wuid.clear());
//          testWUcreate(9,wuid.clear());
//          testWUcreate(10,wuid.clear());
            PROGLOG("WUID = %s",wuid.str());
//          testDFUwuqueue("dfuserver_queue");
        }
        closedownClientProcess();
    }
    catch (IException *e) {
        EXCLOG(e,"Exception");
        e->Release();
    }
    return 0;
}



//=========================================================================

#if 0 //  EXAMPLES for ROXIE

void simpleRoxieCopy()
{
    const char * queueName;
    const char * srcName;
    const char * dstName;
    const char * destCluster;
    const char * user;
    const char * password;
    const char * srcDali = NULL;
    const char * srcUser;
    const char * srcPassword;
    const char * fileMask;
    bool compressed = false;
    bool overwrite;
    DFUclusterPartDiskMapping val;                          // roxie
    const char * baseDir;                                   // roxie
    
    
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setJobName(dstName);
    wu->setQueue(queueName);
    wu->setUser(user);
    wu->setPassword(password);
    wu->setClusterName(destCluster);


    IDFUfileSpec *source = wu->queryUpdateSource();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName(srcName);
    if (srcDali)                                    // remote copy
    {
        SocketEndpoint ep(srcDali);
        source->setForeignDali(ep);
        source->setForeignUser(srcUser, srcPassword);
    }

    IDFUfileSpec *destination = wu->queryUpdateDestination();
    destination->setLogicalName(dstName);
    destination->setFileMask(fileMask);

    destination->setClusterPartDiskMapping(val, baseDir, destCluster);  // roxie

    if(compressed)
        destination->setCompressed(true);

    destination->setWrap(true);                                         // *** roxie always wraps


    IDFUoptions *options = wu->queryUpdateOptions();
    options->setOverwrite(overwrite);
    options->setReplicate(val==DFUcpdm_c_replicated_by_d);              // roxie
    // other options
    wu->submit();                            
}


void repeatedLastPartRoxieCopy()
{
    const char * queueName;
    const char * srcName;
    const char * dstName;
    const char * destCluster;
    const char * user;
    const char * password;
    const char * srcDali = NULL;
    const char * srcUser;
    const char * srcPassword;
    const char * fileMask;
    bool compressed = false;
    bool overwrite;

    DFUclusterPartDiskMapping val;                          // roxie
    const char * baseDir;                                   // roxie
    
    
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setJobName(dstName);
    wu->setQueue(queueName);
    wu->setUser(user);
    wu->setPassword(password);
    wu->setClusterName(destCluster);


    IDFUfileSpec *source = wu->queryUpdateSource();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName(srcName);
    if (srcDali)                                    // remote copy
    {
        SocketEndpoint ep(srcDali);
        source->setForeignDali(ep);
        source->setForeignUser(srcUser, srcPassword);
    }

    IDFUfileSpec *destination = wu->queryUpdateDestination();
    destination->setLogicalName(dstName);
    destination->setFileMask(fileMask);

    destination->setClusterPartDiskMapping(val, baseDir, destCluster, true);  // **** repeat last part

    if(compressed)
        destination->setCompressed(true);

    destination->setWrap(true);                                         // roxie always wraps

    IDFUoptions *options = wu->queryUpdateOptions();
    options->setOverwrite(overwrite);
    options->setReplicate(val==DFUcpdm_c_replicated_by_d);              // roxie

    options->setSuppressNonKeyRepeats(true);                            // **** only repeat last part when src kind = key
    
    // other options
    wu->submit();                            
}

void repeatedLastPartWithServersRoxieCopy()
{
    const char * queueName;
    const char * srcName;
    const char * dstName;
    const char * destCluster;
    const char * user;
    const char * password;
    const char * srcDali = NULL;
    const char * srcUser;
    const char * srcPassword;
    const char * fileMask;
    bool compressed = false;
    bool overwrite;
    DFUclusterPartDiskMapping val;                          // roxie
    const char * baseDir;                                   // roxie
    const char *farmname;                           // **** RoxieFarmProcess/@name in Environment
    const char *farmBaseDir;                        // **** base directory for farm
                                                    
    
    
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setJobName(dstName);
    wu->setQueue(queueName);
    wu->setUser(user);
    wu->setPassword(password);
    wu->setClusterName(destCluster);


    IDFUfileSpec *source = wu->queryUpdateSource();
    wu->setCommand(DFUcmd_copy);
    source->setLogicalName(srcName);
    if (srcDali)                                    // remote copy
    {
        SocketEndpoint ep(srcDali);
        source->setForeignDali(ep);
        source->setForeignUser(srcUser, srcPassword);
    }

    IDFUfileSpec *destination = wu->queryUpdateDestination();
    destination->setLogicalName(dstName);
    destination->setFileMask(fileMask);

    destination->setClusterPartDiskMapping(val, baseDir, destCluster, true);  // **** last part repeated
    StringBuffer farmCluster(destCluster);
    farmCluster.append("__").append(farmname);  // dali stores server cluster as combination of roxie cluster name and farm name
    destination->setClusterPartDiskMapping(val,farmBaseDir,farmCluster.str(),true,true);  // **** only last part

    if(compressed)
        destination->setCompressed(true);

    destination->setWrap(true);                                         // roxie always wraps

    IDFUoptions *options = wu->queryUpdateOptions();
    options->setOverwrite(overwrite);
    options->setReplicate(val==DFUcpdm_c_replicated_by_d);              // roxie

    options->setSuppressNonKeyRepeats(true);                            // **** only repeat last part when src kind = key

    // other options
    wu->submit();                            
}


void simpleRoxieSuperCopy()
{
    const char * queueName;
    const char * srcName;
    const char * dstName;               // ***  name of target superfile
                                        // **** must already contain extra (i.e. roxie) leading prefix
    const char * destCluster;
    const char * user;
    const char * password;
    const char * srcDali = NULL;
    const char * srcUser;
    const char * srcPassword;
    const char * fileMask;
    bool compressed = false;
    bool overwrite;
    DFUclusterPartDiskMapping val;                          // roxie
    const char * baseDir;                                   // roxie

    const char * extraPrefix;           // **** extra leading prefix for sub-file names (e.g. "roxie1")
                                        // should *not* contain trailing "::"
    
    
    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createWorkUnit();
    wu->setJobName(dstName);
    wu->setQueue(queueName);
    wu->setUser(user);
    wu->setPassword(password);
    wu->setClusterName(destCluster);


    IDFUfileSpec *source = wu->queryUpdateSource();
    wu->setCommand(DFUcmd_supercopy);                                   // **** super copy
    source->setLogicalName(srcName);
    if (srcDali)                                    // remote copy
    {
        SocketEndpoint ep(srcDali);
        source->setForeignDali(ep);
        source->setForeignUser(srcUser, srcPassword);
    }

    IDFUfileSpec *destination = wu->queryUpdateDestination();
    destination->setLogicalName(dstName);
    destination->setFileMask(fileMask);

    destination->setClusterPartDiskMapping(val, baseDir, destCluster);  // roxie

    destination->setRoxiePrefix(extraPrefix);                       // added to start of each sub file and main name

    if(compressed)
        destination->setCompressed(true);

    destination->setWrap(true);                                         // roxie always wraps

    IDFUoptions *options = wu->queryUpdateOptions();
    options->setOverwrite(overwrite);
    options->setReplicate(val==DFUcpdm_c_replicated_by_d);              // roxie
    // other options
    wu->submit();                            
}

#endif
