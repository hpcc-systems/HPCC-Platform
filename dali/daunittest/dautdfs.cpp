
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"

#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dautils.hpp"


#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>

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


#define DFSUTGROUP  "dfsutgroup"
#define DFSUTSCOPE  "test::dfsut"
#define DFSUTDIR    "test/dfsut"


class TTestDFS : public CPPUNIT_NS::TestFixture
{
    CPPUNIT_TEST_SUITE(TTestDFS);
      CPPUNIT_TEST(testDFS);
    CPPUNIT_TEST_SUITE_END();
    
    INamedGroupStore *dfsgroup;
    IDistributedFileDirectory *dfsdir;

    void removeGroups()
    {
        dfsgroup->remove(DFSUTGROUP "1");
        dfsgroup->remove(DFSUTGROUP "7");
        dfsgroup->remove(DFSUTGROUP "400");
        dfsgroup->remove(DFSUTGROUP "400b");
    }

    void removeFiles()
    {
        StringArray superfiles;
        StringArray files;
        Owned<IDistributedFileIterator> iter = dfsdir->getIterator(DFSUTSCOPE "::*", true, UNKNOWN_USER);
        ForEach(*iter) {
            IDistributedFile &file = iter->query();
            if (file.querySuperFile())
                superfiles.appendUniq(file.queryLogicalName());  // should recurse owning but maybe later
            else
                files.appendUniq(file.queryLogicalName());
        }
        iter.clear();
        ForEachItemIn(i,superfiles) {
            dfsdir->removeEntry(superfiles.item(i), UNKNOWN_USER);
        }
        ForEachItemIn(j,files) {
            dfsdir->removeEntry(files.item(j), UNKNOWN_USER);
        }
    }



    
public:
    void setUp() 
    {
        dfsgroup = &queryNamedGroupStore();
        dfsdir = &queryDistributedFileDirectory();
        try {
            removeFiles();
            removeGroups();
        }
        catch (IException *e) {
            EXCLOG(e,"TTestDFS::setUp");
        }
    }
    void tearDown() 
    {
        try {
            //removeFiles();
            //removeGroups();
        }
        catch (IException *e) {
            EXCLOG(e,"TTestDFS::tearDown");
        }

    } 
    
protected:
    void testDFS() 
    { 
        Owned<IGroup> grp = createIGroup("192.168.1.1");
        dfsgroup->add(DFSUTGROUP "1", grp, true);
        SocketEndpointArray epa;
        unsigned n;
        StringBuffer s;
        for (n=0;n<7;n++) {
            s.clear().append("192.168.2.").append(n+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        grp.setown(createIGroup(epa)); 
        dfsgroup->add(DFSUTGROUP "7", grp, true);
        epa.kill();
        for (n=0;n<400;n++) {
            s.clear().append("192.168.").append(n/256+3).append('.').append(n%256+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        grp.setown(createIGroup(epa)); 
        dfsgroup->add(DFSUTGROUP "400", grp, true);
        epa.kill();
        for (n=0;n<400;n++) {
            s.clear().append("192.168.").append(n/256+5).append('.').append(n%256+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        grp.setown(createIGroup(epa)); 
        dfsgroup->add(DFSUTGROUP "400b", grp, true);
        grp.setown(dfsgroup->lookup(DFSUTGROUP "400"));
        CPPUNIT_ASSERT(grp.get()!=NULL);
        CPPUNIT_ASSERT(grp->ordinality()==400);
        epa.kill();
        for (n=0;n<grp->ordinality();n++) {
            s.clear().append("192.168.").append(n/256+3).append('.').append(n%256+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
            CPPUNIT_ASSERT(epa.item(n).equals(grp->queryNode(n).endpoint()));
        }
        epa.kill();
        for (n=0;n<grp->ordinality();n++) {
            s.clear().append("192.168.").append(n/256+5).append('.').append(n%256+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        grp.setown(createIGroup(epa)); 
        CPPUNIT_ASSERT(dfsgroup->find(grp, s.clear()));     
        CPPUNIT_ASSERT(strcmp(DFSUTGROUP "400b",s.str())==0);
        epa.kill();
        for (n=0;n<7;n++) {
            s.clear().append("192.168.7.").append(n+1);
            SocketEndpoint ep(s.str());
            epa.append(ep);
        }
        grp.setown(createIGroup(epa)); 
        dfsgroup->add(DFSUTGROUP "7b", grp, true, "/c$/altdir");
        epa.kill();
        grp.clear();

        ClusterPartDiskMapSpec mapping;


        // now create a simple one part file
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        grp.setown(dfsgroup->lookup(DFSUTGROUP "1"));
        fdesc->setDefaultDir("/c$/thordata/" DFSUTDIR);
        fdesc->setPartMask("testfile1._$P$_of_$N$");
        fdesc->setNumParts(1);
        fdesc->addCluster(grp,mapping);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        file->attach(DFSUTSCOPE "::testfile1", UNKNOWN_USER);
        file.clear();
        file.setown(dfsdir->lookup(DFSUTSCOPE "::testfile1", UNKNOWN_USER));
        CPPUNIT_ASSERT(file.get()!=NULL);
        CPPUNIT_ASSERT(file->numParts()==1);
        CPPUNIT_ASSERT(file->numCopies(0)==2);
        Owned<IDistributedFilePart> part=file->getPart(0);
        CPPUNIT_ASSERT(part.get()!=NULL);
        part->getPartName(s.clear());
        CPPUNIT_ASSERT(stricmp(s.str(),"testfile1._1_of_1")==0);
        SocketEndpoint ep;
        ep.set("192.168.1.1");
        Owned<INode> node;
        RemoteFilename rfn;
        unsigned cpi;
        StringBuffer t;
        for (cpi=0;cpi<2;cpi++) {
            node.setown(part->getNode(cpi));
            CPPUNIT_ASSERT(node.get()!=NULL);
            CPPUNIT_ASSERT(node->endpoint().equals(ep));
            part->getPartDirectory(s.clear(),cpi);
            t.clear().appendf("/%c$/thordata/test/dfsut",cpi?'d':'c');
            CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            t.insert(0,"//192.168.1.1");
            t.append("/testfile1._1_of_1");
            part->getFilename(rfn,cpi);
            rfn.getRemotePath(s.clear());
            CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
        }
        
        // now create a simple multi part file with one extra part (e.g. tlk) 
        fdesc.setown(createFileDescriptor());
        grp.setown(dfsgroup->lookup(DFSUTGROUP "7"));
        fdesc->setDefaultDir("/c$/thordata/" DFSUTDIR);
        fdesc->setPartMask("testfile2._$P$_of_$N$");
        fdesc->setNumParts(8);
        fdesc->addCluster(grp,mapping);
        file.setown(queryDistributedFileDirectory().createNew(fdesc));
        file->attach(DFSUTSCOPE "::testfile2", UNKNOWN_USER);
        file.clear();
        file.setown(dfsdir->lookup(DFSUTSCOPE "::testfile2", UNKNOWN_USER));
        CPPUNIT_ASSERT(file.get()!=NULL);
        CPPUNIT_ASSERT(file->numParts()==8);
        unsigned pi;
        StringBuffer tmp;
        for (pi=0;pi<8;pi++) {
            tmp.clear().appendf("192.168.2.%d",pi%7+1);
            SocketEndpoint ep(tmp.str());
            tmp.clear().appendf("192.168.2.%d",((pi+1)%7)+1);
            SocketEndpoint rep(tmp.str());
            CPPUNIT_ASSERT(file->numCopies(pi)==2);
            part.setown(file->getPart(pi));
            CPPUNIT_ASSERT(part.get()!=NULL);
            part->getPartName(s.clear());
            t.clear().appendf("testfile2._%d_of_8",pi+1);
            CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            for (cpi=0;cpi<2;cpi++) {
                node.setown(part->getNode(cpi));
                CPPUNIT_ASSERT(node.get()!=NULL);
                CPPUNIT_ASSERT(node->endpoint().equals((cpi&1)?rep:ep));
                part->getPartDirectory(s.clear(),cpi);
                StringBuffer t;
                t.clear().appendf("/%c$/thordata/test/dfsut",cpi?'d':'c');
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
                t.insert(0,(pi+(cpi&1))%7+1);
                t.insert(0,"//192.168.2.");
                t.appendf("/testfile2._%d_of_8",pi+1);
                part->getFilename(rfn,cpi);
                rfn.getRemotePath(s.clear());
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            }
        }

        // now create a multi part file with last part repeated on every node
        fdesc.setown(createFileDescriptor());
        grp.setown(dfsgroup->lookup(DFSUTGROUP "7"));
        fdesc->setDefaultDir("/c$/thordata/" DFSUTDIR);
        fdesc->setPartMask("testfile3._$P$_of_$N$");
        fdesc->setNumParts(8);
        mapping.setRepeatedCopies(7,false);
        fdesc->addCluster(grp,mapping);
        file.setown(queryDistributedFileDirectory().createNew(fdesc));
        file->attach(DFSUTSCOPE "::testfile3", UNKNOWN_USER);
        file.clear();
        file.setown(dfsdir->lookup(DFSUTSCOPE "::testfile3", UNKNOWN_USER));
        CPPUNIT_ASSERT(file.get()!=NULL);
        CPPUNIT_ASSERT(file->numParts()==8);
        for (pi=0;pi<8;pi++) {
            unsigned nc = (pi==7)?14:2;
            CPPUNIT_ASSERT(file->numCopies(pi)==nc);
            part.setown(file->getPart(pi));
            CPPUNIT_ASSERT(part.get()!=NULL);
            part->getPartName(s.clear());
            t.clear().appendf("testfile3._%d_of_8",pi+1);
            CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            for (cpi=0;cpi<nc;cpi++) {
                bool isrep = cpi&1;
                tmp.clear().appendf("192.168.2.%d",(pi+(cpi/2))%7+1);
                SocketEndpoint ep(tmp.str());
                tmp.clear().appendf("192.168.2.%d",(pi+(cpi/2)+1)%7+1);
                SocketEndpoint rep(tmp.str());
                node.setown(part->getNode(cpi));
                CPPUNIT_ASSERT(node.get()!=NULL);
                CPPUNIT_ASSERT(node->endpoint().equals(isrep?rep:ep));
                part->getPartDirectory(s.clear(),cpi);
                StringBuffer t;
                t.clear().appendf("/%c$/thordata/test/dfsut",isrep?'d':'c');
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
                StringBuffer eps;
                if (isrep)
                    rep.getUrlStr(eps);
                else
                    ep.getUrlStr(eps);
                t.insert(0,eps.str());
                t.insert(0,"//");
                t.appendf("/testfile3._%d_of_8",pi+1);
                part->getFilename(rfn,cpi);
                rfn.getRemotePath(s.clear());
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            }
        }

        // now create a multi part file with last part copied on every node
        // and also with that part repeated on a different cluster (e.g. like a roxie cluster)
        // with a different directory
        fdesc.setown(createFileDescriptor());
        grp.setown(dfsgroup->lookup(DFSUTGROUP "7"));
        fdesc->setDefaultDir("/c$/thordata/" DFSUTDIR);
        fdesc->setPartMask("testfile4._$P$_of_$N$");
        fdesc->setNumParts(8);
        mapping.setRepeatedCopies(7,false);
        fdesc->addCluster(grp,mapping);
        StringBuffer dir2;
        GroupType groupType;
        grp.setown(dfsgroup->lookup(DFSUTGROUP "7b", dir2, groupType));
        ClusterPartDiskMapSpec mapping2;
        mapping2.setDefaultBaseDir(dir2);
        mapping2.setRepeatedCopies(7,true);
        fdesc->addCluster(grp,mapping2);
        file.setown(queryDistributedFileDirectory().createNew(fdesc));
        file->attach(DFSUTSCOPE "::testfile4", UNKNOWN_USER);
        file.clear();
        file.setown(dfsdir->lookup(DFSUTSCOPE "::testfile4", UNKNOWN_USER));
        CPPUNIT_ASSERT(file.get()!=NULL);
        CPPUNIT_ASSERT(file->numParts()==8);
        for (pi=0;pi<8;pi++) {
            unsigned nc = (pi==7)?28:2;
            CPPUNIT_ASSERT(file->numCopies(pi)==nc);
            part.setown(file->getPart(pi));
            CPPUNIT_ASSERT(part.get()!=NULL);
            part->getPartName(s.clear());
            t.clear().appendf("testfile4._%d_of_8",pi+1);
            CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            for (unsigned cpi=0;cpi<nc;cpi++) {
                unsigned cpin = cpi%14;                         // normalize for each cluster
                bool isrep = cpi&1;
                node.setown(part->getNode(cpi));
                CPPUNIT_ASSERT(node.get()!=NULL);
                SocketEndpoint cmp("192.168.2.0");
                bool isclustb = (node->endpoint().ipcompare(cmp)>0);
                tmp.clear().appendf("192.168.%d.%d",isclustb?7:2,(pi+(cpin/2))%7+1);
                SocketEndpoint ep(tmp.str());
                tmp.clear().appendf("192.168.%d.%d",isclustb?7:2,(pi+(cpin/2)+1)%7+1);
                SocketEndpoint rep(tmp.str());
                CPPUNIT_ASSERT(node->endpoint().equals(isrep?rep:ep));
                part->getPartDirectory(s.clear(),cpi);
                StringBuffer t;
                t.clear().appendf("/%c$/%s/test/dfsut",isrep?'d':'c',isclustb?"altdir":"thordata");
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
                StringBuffer eps;
                if (isrep)
                    rep.getUrlStr(eps);
                else
                    ep.getUrlStr(eps);
                t.insert(0,eps.str());
                t.insert(0,"//");
                t.appendf("/testfile4._%d_of_8",pi+1);
                part->getFilename(rfn,cpi);
                rfn.getRemotePath(s.clear());
                CPPUNIT_ASSERT(stricmp(s.str(),t.str())==0);
            }
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TTestDFS);



