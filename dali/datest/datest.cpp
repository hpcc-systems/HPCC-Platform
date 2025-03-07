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

#include <string>
#include <array>

#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "danqs.hpp"
#include "dautils.hpp"
#include "dasess.hpp"
#include "mplog.hpp"
#include "rmtclient.hpp"

#include "rtlformat.hpp"

#include "jptree.hpp"
#include "wsdfuaccess.hpp"

using namespace wsdfuaccess;
using namespace dafsstream;


#define DEFAULT_TEST "RANDTEST"
static const char *whichTest = DEFAULT_TEST;
static StringArray testParams;
static unsigned nIter = 1;

#define TEST(X) (0==stricmp(whichTest, X))


//#define TEST_REMOTEFILE
//#define TEST_REMOTEFILE2
//#define TEST_REMOTEFILE3
//#define TEST_COPYFILE
//#define TEST_DEADLOCK
//#define TEST_THREADS
#define TEST_MEMTHREADS
#define MDELAY 100

static void addTestFile(const char *name,unsigned n)
{
    queryDistributedFileDirectory().removeEntry(name,UNKNOWN_USER);
    SocketEndpointArray epa;
    for (unsigned i=0;i<n;i++) {
        StringBuffer ips("192.168.0.");
        ips.append((byte)i+1);
        SocketEndpoint ep(ips.str());
        epa.append(ep);
    }
    Owned<IGroup> group = createIGroup(epa); 
    Owned<IPropertyTree> fileInfo = createPTree();
    Owned<IFileDescriptor> fileDesc = createFileDescriptor();
    StringBuffer dir;
    getLFNDirectoryUsingDefaultBaseDir(dir, name, DFD_OSdefault);
    StringBuffer partmask;
    getPartMask(partmask,name,n);
    StringBuffer path;
    for (unsigned m=0; m<n; m++) {
        RemoteFilename rfn;
        constructPartFilename(group,m+1,n,NULL,partmask.str(),dir.str(),false,1,rfn);
        rfn.getLocalPath(path.clear());
        Owned<IPropertyTree> pp = createPTree("Part");
        pp->setPropInt64("@size",1234*(m+1));
        fileDesc->setPart(m,&group->queryNode(m), path.str(), pp);
    }
    Owned<IDistributedFile> dfile =  queryDistributedFileDirectory().createNew(fileDesc);
    {
        DistributedFilePropertyLock lock(dfile);
        IPropertyTree &t = lock.queryAttributes();
        t.setProp("@owned","nigel");
        t.setPropInt("@recordSize",1);
        t.setProp("ECL","TESTECL();");
    }
    dfile->attach(name,UNKNOWN_USER);
}

#define TEST_SUPER_FILE "nhtest::super"
#define TEST_SUB_FILE "nhtest::sub"



void Test_SuperFile()
{
    // create subfile
    // first remove if exists
    unsigned i;
    unsigned n;
    Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(UNKNOWN_USER);
    Owned<IDistributedSuperFile> sfile;
    Owned<IDistributedFilePartIterator> piter;
    Owned<IDistributedFileIterator> iter;
    queryDistributedFileDirectory().removeEntry(TEST_SUPER_FILE"4",UNKNOWN_USER);
    queryDistributedFileDirectory().removeEntry(TEST_SUPER_FILE"3",UNKNOWN_USER);
    queryDistributedFileDirectory().removeEntry(TEST_SUPER_FILE"2",UNKNOWN_USER);
#if 1
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile(TEST_SUPER_FILE"1", UNKNOWN_USER, AccessMode::tbdWrite));
    if (sfile) {
        sfile->removeSubFile(NULL,true);
        sfile.clear();
        queryDistributedFileDirectory().removeEntry(TEST_SUPER_FILE"1",UNKNOWN_USER);
    }
    sfile.setown(queryDistributedFileDirectory().createSuperFile(TEST_SUPER_FILE"1",UNKNOWN_USER,true));
    for (i = 0;i<3;i++) {
        StringBuffer name(TEST_SUB_FILE);
        name.append(i+1);
        addTestFile(name.str(),i+2);
        sfile->addSubFile(name);
    }
    sfile.clear();
#endif
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile(TEST_SUPER_FILE"1", UNKNOWN_USER, AccessMode::tbdRead));
    printf("NumSubFiles = %d\n",sfile->numSubFiles());
#if 1
    i=0;
    iter.setown(sfile->getSubFileIterator());
    ForEach(*iter) {
        StringBuffer name;
        iter->getName(name);
        printf("  %d: %s\n",i+1,name.str());
        IDistributedFile *f = &iter->query();
        assertex(stricmp(f->queryLogicalName(),name.str())==0);
        assertex(&sfile->querySubFile(i)==f);
        assertex(sfile->querySubFileNamed(name.str())==f);
        i++;
    }
    iter.clear();
#endif
    piter.setown(sfile->getIterator());
    n = sfile->numParts();
    printf("NumSubParts = %d\n",n);
    i = 0;
    ForEach(*piter) {
        IDistributedFilePart *part = &piter->query();
        unsigned subp;
        IDistributedFile *subf = sfile->querySubPart(i,subp);
        assertex(subf);
        const char* lname = subf->queryLogicalName();
        StringBuffer pname;
        part->getPartName(pname);
        for (unsigned c=0;c<part->numCopies();c++) {
            RemoteFilename rfn;
            StringBuffer tmp;
            printf("  %d: %s[%d,%d] %s %s\n",i,lname,subp,c,pname.str(),part->getFilename(rfn,c).getRemotePath(tmp).str());
        }
        i++;
    }
    piter.clear();
    sfile.setown(queryDistributedFileDirectory().createSuperFile(TEST_SUPER_FILE"2",UNKNOWN_USER,true));
    transaction->start();
    for (i = 0;i<3;i++) {
        StringBuffer name(TEST_SUB_FILE);
        name.append(i+1);
        sfile->addSubFile(name,false,NULL,false,transaction);
    }
    sfile.clear(); // mustn't have owner open when commit transaction
    transaction->commit();
    sfile.setown(queryDistributedFileDirectory().createSuperFile(TEST_SUPER_FILE"3",UNKNOWN_USER,true));
    for (i = 0;i<3;i++) {
        StringBuffer name(TEST_SUB_FILE);
        name.append(i+1);
        sfile->addSubFile(name,false,NULL,false,transaction);
    }
    sfile.clear();  // mustn't have owner open when commit transaction
    transaction->rollback();
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile(TEST_SUPER_FILE"2", UNKNOWN_USER, AccessMode::tbdWrite));
    transaction->start();
    sfile->removeSubFile(TEST_SUB_FILE"1",false,false,transaction);
    StringBuffer name(TEST_SUB_FILE"4");
    addTestFile(name.str(),3);
    sfile->addSubFile(name,false,NULL,false,transaction);
    sfile.clear(); // mustn't have owner open when commit transaction
    transaction->commit();
    sfile.setown(queryDistributedFileDirectory().createSuperFile(TEST_SUPER_FILE"4",UNKNOWN_USER,true));
    transaction->start();
    sfile->addSubFile(TEST_SUPER_FILE"1",false,NULL,false,transaction);
    sfile->addSubFile(TEST_SUPER_FILE"2",false,NULL,false,transaction);
    sfile->addSubFile(TEST_SUPER_FILE"3",false,NULL,false,transaction);
    sfile.clear();  // mustn't have owner open when commit transaction
    transaction->commit();
    sfile.setown(queryDistributedFileDirectory().lookupSuperFile(TEST_SUPER_FILE"4", UNKNOWN_USER, AccessMode::tbdRead));
    i=0;
    iter.setown(sfile->getSubFileIterator());
    ForEach(*iter) {
        StringBuffer name;
        iter->getName(name);
        printf("  %d: %s\n",i+1,name.str());
        IDistributedFile *f = &iter->query();
        assertex(stricmp(f->queryLogicalName(),name.str())==0);
        assertex(&sfile->querySubFile(i)==f);
        assertex(sfile->querySubFileNamed(name.str())==f);
        i++;
    }
    i = 0;
    iter.setown(sfile->getSubFileIterator(true));
    ForEach(*iter) {
        StringBuffer name;
        iter->getName(name);
        printf("  %d: %s\n",i+1,name.str());
        IDistributedFile *f = &iter->query();
        assertex(stricmp(f->queryLogicalName(),name.str())==0);
        //assertex(&sfile->querySubFile(i)==f);
        //assertex(sfile->querySubFileNamed(name.str())==f);
        i++;
    }
    iter.clear();
    piter.setown(sfile->getIterator());
    n = sfile->numParts();
    printf("NumSubParts = %d\n",n);
    i = 0;
    ForEach(*piter) {
        IDistributedFilePart *part = &piter->query();
        unsigned subp;
        IDistributedFile *subf = sfile->querySubPart(i,subp);
        assertex(subf);
        const char* lname = subf->queryLogicalName();
        StringBuffer pname;
        part->getPartName(pname);
        for (unsigned c=0;c<part->numCopies();c++) {
            RemoteFilename rfn;
            StringBuffer tmp;
            printf("  %d: %s[%d,%d] %s %s\n",i,lname,subp,c,pname.str(),part->getFilename(rfn,c).getRemotePath(tmp).str());
        }
        i++;
    }
}

void Test_SuperFile2()
{
    // create subfile
    // first remove if exists
    unsigned i;
    Owned<IDistributedSuperFile> sfile;
    queryDistributedFileDirectory().removeEntry(TEST_SUPER_FILE"B1",UNKNOWN_USER);
    sfile.setown(queryDistributedFileDirectory().createSuperFile(TEST_SUPER_FILE"B1",UNKNOWN_USER,true));
    for (unsigned tst=0;tst<2;tst++) {
        printf("sfile size = %" I64F "d\n",sfile->getFileSize(false,false));
        for (i = 0;i<3;i++) {
            StringBuffer name(TEST_SUB_FILE);
            name.append(i+1);
            addTestFile(name.str(),i+2);
            Owned<IDistributedFile> sbfile = queryDistributedFileDirectory().lookup(name,UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
            printf("adding size = %" I64F "d\n",sbfile->getFileSize(false,false));
            sfile->addSubFile(name);
            printf("sfile size = %" I64F "d\n",sfile->getFileSize(false,false));
        }
        sfile.clear();
        sfile.setown(queryDistributedFileDirectory().lookupSuperFile(TEST_SUPER_FILE"B1", UNKNOWN_USER, AccessMode::tbdWrite));
        printf("NumSubFiles = %d\n",sfile->numSubFiles());
        if (tst==1) {
            sfile->removeSubFile(NULL,false);
            printf("sfile size = %" I64F "d\n",sfile->getFileSize(false,false));
        }
        else {
            for (i = 0;i<3;i++) {
                StringBuffer name(TEST_SUB_FILE);
                name.append(i+1);
                Owned<IDistributedFile> sbfile = queryDistributedFileDirectory().lookup(name,UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
                printf("removing size = %" I64F "d\n",sbfile->getFileSize(false,false));
                sfile->removeSubFile(name,false);
                printf("sfile size = %" I64F "d\n",sfile->getFileSize(false,false));
            }
        }
        printf("NumSubFiles = %d\n",sfile->numSubFiles());
    }
}


void Test_PartIter()
{
    unsigned start = msTick();
    Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup("nhtest::file_name_ssn20030805",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    Owned<IDistributedFilePartIterator> parts = file->getIterator();
    ForEach(*parts) {
        IDistributedFilePart & thisPart = parts->query(); 
        IPropertyTree &partProps = thisPart.queryAttributes();
    }
    printf("time taken = %d\n",msTick()-start);
}

void testCDfsLogicalFileName()
{
    CDfsLogicalFileName cdlfn;
    const char *lfn;
    SocketEndpoint ep;
    StringBuffer eps;
    StringBuffer path;
    StringBuffer dir;
    StringBuffer scopes;
    StringBuffer tail;
    assertex(!cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strlen(lfn)==0);
    lfn=cdlfn.get(true); assertex(strlen(lfn)==0);
    cdlfn.set("xYz");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,".::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,".::xyz")==0);
    cdlfn.set("X");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,".::x")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,".::x")==0);
    cdlfn.set(" xYz ");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,".::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,".::xyz")==0);
    cdlfn.getScopes(scopes.clear()); assertex(strcmp(scopes.str(),".")==0);
    cdlfn.getTail(tail.clear()); assertex(strcmp(tail.str(),"xyz")==0);
    cdlfn.set("aBc::xYz");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"abc::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"abc::xyz")==0);
    cdlfn.set("A::X");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"a::x")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"a::x")==0);
    cdlfn.set(" AbC :: xYz ");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    cdlfn.getScopes(scopes.clear()); assertex(strcmp(scopes.str(),"abc")==0);
    cdlfn.getTail(tail.clear()); assertex(strcmp(tail.str(),"xyz")==0);
    lfn=cdlfn.get(); assertex(strcmp(lfn,"abc::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"abc::xyz")==0);
    cdlfn.set("123::aBc::xYz");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"123::abc::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"123::abc::xyz")==0);
    cdlfn.set("1:: A ::X");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"1::a::x")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"1::a::x")==0);
    cdlfn.set(" 123 :: AbC :: xYz ");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"123::abc::xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"123::abc::xyz")==0);
    cdlfn.getScopes(scopes.clear()); assertex(strcmp(scopes.str(),"123::abc")==0);
    cdlfn.getTail(tail.clear()); assertex(strcmp(tail.str(),"xyz")==0);
    cdlfn.set("file::10.150.10.75::c$::test::file.xyz");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"file::10.150.10.75::c$::test::file.xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"file::10.150.10.75::c$::test::file.xyz")==0);
    verifyex(cdlfn.getEp(ep)); 
    ep.getEndpointHostText(eps.clear()); assertex(strcmp(eps.str(),"10.150.10.75")==0);
    verifyex(cdlfn.getExternalPath(path.clear(),path,true)); assertex(strcmp(path.str(),"c:\\test\\file.xyz")==0);
    verifyex(cdlfn.getExternalPath(dir.clear(),path.clear(),true)); assertex(strcmp(path.str(),"file.xyz")==0);
    verifyex(cdlfn.getExternalPath(path.clear(),path,false)); assertex(strcmp(path.str(),"/c$/test/file.xyz")==0);
    verifyex(cdlfn.getExternalPath(dir.clear(),path.clear(),false)); assertex(strcmp(path.str(),"file.xyz")==0);
    cdlfn.set("file::10.150.10.75:7100::c$::test::file.xyz");
    assertex(cdlfn.isSet());
    assertex(!cdlfn.isForeign());
    assertex(cdlfn.isExternal());
    lfn=cdlfn.get(); assertex(strcmp(lfn,"file::10.150.10.75:7100::c$::test::file.xyz")==0);
    verifyex(cdlfn.getEp(ep)); 
    ep.getEndpointHostText(eps.clear()); assertex(strcmp(eps.str(),"10.150.10.75:7100")==0);
    verifyex(cdlfn.getExternalPath(path.clear(),path,true)); assertex(strcmp(path.str(),"c:\\test\\file.xyz")==0);
    verifyex(cdlfn.getExternalPath(dir.clear(),path.clear(),true)); assertex(strcmp(path.str(),"file.xyz")==0);
    verifyex(cdlfn.getExternalPath(path.clear(),path,false)); assertex(strcmp(path.str(),"/c$/test/file.xyz")==0);
    verifyex(cdlfn.getExternalPath(dir.clear(),path.clear(),false)); assertex(strcmp(path.str(),"file.xyz")==0);
    cdlfn.set("foreign::10.150.10.75::test::file.xyz");
    assertex(cdlfn.isSet());
    assertex(cdlfn.isForeign());
    assertex(!cdlfn.isExternal());
    verifyex(cdlfn.getEp(ep)); 
    ep.getEndpointHostText(eps.clear()); assertex(strcmp(eps.str(),"10.150.10.75")==0);
    lfn=cdlfn.get(); assertex(strcmp(lfn,"foreign::10.150.10.75::test::file.xyz")==0);
    lfn=cdlfn.get(true); assertex(strcmp(lfn,"test::file.xyz")==0);
    cdlfn.getScopes(scopes.clear()); assertex(strcmp(scopes.str(),"foreign::10.150.10.75::test")==0);
    cdlfn.getScopes(scopes.clear(),true); assertex(strcmp(scopes.str(),"test")==0);
    cdlfn.getTail(tail.clear()); assertex(strcmp(tail.str(),"file.xyz")==0);
    StringBuffer baseq;
    CDfsLogicalFileName dlfn;
/*
#define T(s,a,q) \
    dlfn.set(s,"x"); \
    dlfn.makeScopeQuery(baseq.clear(),a); \
    assertex(strcmp(q,baseq.str())==0)
*/
    
    #define T(s,a,q) assertex(strcmp(q,s)==0)
    T("abcd::efgh",true,"abcd");
/*      
//      "Files/Scope[@name=\"abcd\"]/Scope[@name=\"efgh\"]");
    T("efgh",true,"Files/Scope[@name=\"efgh\"]");
    T(".",true,"Files/Scope[@name=\".\"]");
    T("",true,"Files/Scope[@name=\".\"]");
    T(NULL,true,"Files/Scope[@name=\".\"]");
    T("abcd::efgh",false,"Scope[@name=\"abcd\"]/Scope[@name=\"efgh\"]");
    T("efgh",false,"Scope[@name=\"efgh\"]");
    T(".",false,"Scope[@name=\".\"]");
    T("",false,"Scope[@name=\".\"]");
    T(NULL,false,"Scope[@name=\".\"]");
*/
}


void Test_DFS()
{
    testCDfsLogicalFileName();
#if 0
    IDistributedFileIterator& iter = *queryDistributedFileDirectory().getIterator("thor_data400::*");
    StringBuffer name;
    ForEach(iter) {
        iter.getName(name.clear());
        if (strchr(name.str(),'\001')) {
            printf("name = %s\n",name.str());
            break;
        }
    }
    iter.Release();
    IDistributedFile *dfiler = queryDistributedFileDirectory().lookup(name.str());
    if (!dfiler) {
        printf("not found");
        return;
    }
    dfiler->detach();
    dfiler->attach("thor_data400::unknown_corrupt");
    dfiler->Release();
    return;
#endif
    Owned<IPropertyTree> pp = createPTree("Part");
    IFileDescriptor *fdesc = createFileDescriptor();
    fdesc->setDefaultDir("c:\\thordata\\test");
    INode *node = createINode("192.168.0.1");
    pp->setPropInt64("@size",1234);
    fdesc->setPart(0,node,"testfile1.d00._1_of_3",pp);
    node->Release();
    node = createINode("192.168.0.2");
    pp->setPropInt64("@size",2345);
    fdesc->setPart(1,node,"testfile1.d00._2_of_3",pp);
    node->Release();
    node = createINode("192.168.0.3");
    pp->setPropInt64("@size",3456);
    fdesc->setPart(2,node,"testfile1.d00._3_of_3",pp);
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile1",UNKNOWN_USER);
    IDistributedFile *dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile1",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    fdesc = createFileDescriptor();
    fdesc->setDefaultDir("c:\\thordata");
    node = createINode("192.168.0.1");
    pp->setPropInt64("@size",23456);
    fdesc->setPart(0,node,"testfile2.d00._1_of_3");
    node->Release();
    node = createINode("192.168.0.2");
    pp->setPropInt64("@size",33456);
    fdesc->setPart(1,node,"testfile2.d00._2_of_3");
    node->Release();
    node = createINode("192.168.0.3");
    pp->setPropInt64("@size",43456);
    fdesc->setPart(2,node,"testfile2.d00._3_of_3");
    node->Release();
    node = createINode("192.168.0.4");
    fdesc->setPart(1,node,"testfile2.d00._2_of_3");
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile2",UNKNOWN_USER);
    dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile2",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    fdesc = createFileDescriptor();
    fdesc->setDefaultDir("c:\\thordata");
    node = createINode("192.168.0.1");
    fdesc->setPart(0,node,"testfile3.d00._1_of_3");
    node->Release();
    node = createINode("192.168.0.2");
    fdesc->setPart(1,node,"testfile3.d00._2_of_3");
    node->Release();
    node = createINode("192.168.0.3");
    fdesc->setPart(2,node,"testfile3.d00._3_of_3");
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile3",UNKNOWN_USER);
    dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile3",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    IDistributedFile *f = queryDistributedFileDirectory().lookup("nigel::test::testfile2",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    if (!f)
        printf("failed 1");
    ::Release(f);
    f = queryDistributedFileDirectory().lookup("nigel::zest::testfile1",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    assertex(!f);
    f = queryDistributedFileDirectory().lookup("nigel::test::zestfile1",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    assertex(!f);
    f = queryDistributedFileDirectory().lookup("nigel::test::testfile1",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    if (!f)
        printf("failed 2 ");
    ::Release(f);
    f = queryDistributedFileDirectory().lookup("nigel::test::testfile3",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    if (!f)
        printf("failed 3");
    StringBuffer str;
    for (unsigned p=0;p<f->numParts();p++) {
        Owned<IDistributedFilePart> part = f->getPart(p);
        RemoteFilename rfn;
        part->getFilename(rfn).getRemotePath(str.clear());
        printf("part[%d,0]  = %s\n",p+1,str.str());
        part->getFilename(rfn,1).getRemotePath(str.clear());
        printf("part[%d,1]  = %s\n",p+1,str.str());
    }
    ::Release(f);
#if 0
    IDistributedFileIterator& iter = *queryDistributedFileDirectory().getIterator("*::*::*");
    ForEach(iter) {
        StringBuffer name;
        printf("name = %s\n",iter.getName(name).str());
    }
    ForEach(iter) {
        StringBuffer name;
        printf("name = %s\n",iter.getName(name).str());
    }
    iter.Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile4");
    f = queryDistributedFileDirectory().lookup("nigel::test::testfile2");
    fdesc = f->getFileDescriptor();
    dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile4");
    dfile->Release();
    fdesc->Release();
    f->Release();
#endif
}

void Test_DFSU()
{
    Owned<IPropertyTree> pp = createPTree("Part");
    IFileDescriptor *fdesc = createFileDescriptor();
    fdesc->setDefaultDir("/c$/thordata/test");
    INode *node = createINode("192.168.0.1");
    pp->setPropInt64("@size",1234);
    fdesc->setPart(0,node,"testfile1.d00._1_of_3",pp);
    node->Release();
    node = createINode("192.168.0.2");
    pp->setPropInt64("@size",2345);
    fdesc->setPart(1,node,"testfile1.d00._2_of_3",pp);
    node->Release();
    node = createINode("192.168.0.3");
    pp->setPropInt64("@size",3456);
    fdesc->setPart(2,node,"testfile1.d00._3_of_3",pp);
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile1u",UNKNOWN_USER);
    IDistributedFile *dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile1u",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    fdesc = createFileDescriptor();
    fdesc->setDefaultDir("/c$/thordata");
    node = createINode("192.168.0.1");
    pp->setPropInt64("@size",23456);
    fdesc->setPart(0,node,"testfile2.d00._1_of_3");
    node->Release();
    node = createINode("192.168.0.2");
    pp->setPropInt64("@size",33456);
    fdesc->setPart(1,node,"testfile2.d00._2_of_3");
    node->Release();
    node = createINode("192.168.0.3");
    pp->setPropInt64("@size",43456);
    fdesc->setPart(2,node,"testfile2.d00._3_of_3");
    node->Release();
    node = createINode("192.168.0.4");
    fdesc->setPart(1,node,"testfile2.d00._2_of_3");
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile2u",UNKNOWN_USER);
    dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile2u",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    fdesc = createFileDescriptor();
    fdesc->setDefaultDir("/c$/thordata");
    node = createINode("192.168.0.1");
    fdesc->setPart(0,node,"testfile3.d00._1_of_3");
    node->Release();
    node = createINode("192.168.0.2");
    fdesc->setPart(1,node,"testfile3.d00._2_of_3");
    node->Release();
    node = createINode("192.168.0.3");
    fdesc->setPart(2,node,"testfile3.d00._3_of_3");
    node->Release();
    queryDistributedFileDirectory().removeEntry("nigel::test::testfile3u",UNKNOWN_USER);
    dfile = queryDistributedFileDirectory().createNew(fdesc);
    dfile->attach("nigel::test::testfile3u",UNKNOWN_USER);
    dfile->Release();
    fdesc->Release();
    IDistributedFile *f = queryDistributedFileDirectory().lookup("nigel::test::testfile2u",UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    if (!f)
        printf("failed 1");
    StringBuffer str;
    for (unsigned p=0;p<f->numParts();p++) {
        Owned<IDistributedFilePart> part = f->getPart(p);
        RemoteFilename rfn;
        part->getFilename(rfn).getRemotePath(str.clear());
        printf("upart[%d,0]  = %s\n",p+1,str.str());
        part->getFilename(rfn,1).getRemotePath(str.clear());
        printf("upart[%d,1]  = %s\n",p+1,str.str());
    }

    ::Release(f);
}

void testcode()
{


}


void Test_MultiFile()
{
    Owned<IPropertyTree> pp = createPTree("Part");
    Owned<IFileDescriptor> fdesc = createFileDescriptor();
    fdesc->setDefaultDir("c:\\thordata\\test");
    INode *node = createINode("10.150.10.16");
    pp->setPropInt64("@size",1234);
    fdesc->setPart(0,node,"firstpart,testfile1.*._1_of_3,c:\\test\\lastpart",pp);
    node->Release();
    node = createINode("10.150.10.17");
    pp->setPropInt64("@size",2345);
    fdesc->setPart(1,node,"testfile1.d00._2_of_3",pp);
    node->Release();
    node = createINode("10.150.10.18");
    pp->setPropInt64("@size",3456);
    fdesc->setPart(2,node,"testfile1.*._3_of_3",pp);
    node->Release();

    assertex(fdesc->isMulti());
    unsigned n = fdesc->numParts();
    for (unsigned p=0;p<n;p++) {
        for (unsigned cpy=0;cpy<2;cpy++) {
            RemoteMultiFilename rmfn;
            fdesc->getMultiFilename(p,cpy,rmfn);
            bool iswild = rmfn.isWild();
            printf("Part %d[%d]%s%s\n",p+1,cpy,fdesc->isMulti(p)?", MULTI":"",iswild?", WILD":"");
            unsigned nc = rmfn.ordinality();
            StringBuffer rfns;
            for (unsigned j=0;j<nc;j++) {
                const RemoteFilename &rfn = rmfn.item(j);
                printf("  Component %d %s%s\n",j,rfn.getRemotePath(rfns.clear()).str(),
                                              rmfn.isWild(j)?", WILD":"");
            }
            if (iswild) {
                try {
                    rmfn.expandWild();
                    nc = rmfn.ordinality();
                    for (unsigned k=0;k<nc;k++) {
                        const RemoteFilename &rfn = rmfn.item(k);
                        printf("  Resolved %d %s\n",k,rfn.getRemotePath(rfns.clear()).str());
                        assertex(!rmfn.isWild(k));
                    }
                }
                catch (IException *e) {
                    EXCLOG(e,"expandWild");
                    e->Release();
                }

            }
        }
    }

}


#define BUFFSIZE 0x8000
#define COUNT 10

struct RecordStruct
{
    char fill[16];
    unsigned idx;
    unsigned key;
    unsigned check;
    unsigned sum;
};

#define NRECS ((__int64)1024*1024*1024*20/sizeof(RecordStruct)) // i.e. ~20GB
// #define NRECS ((__int64)1024*1024*1024*2/sizeof(RecordStruct)) // i.e. ~2GB
// #define NRECS ((__int64)1024*1024*500/sizeof(RecordStruct)) // i.e. ~500MB
// #define NRECS ((__int64)1024*500/sizeof(RecordStruct)) // i.e. ~500KB

void TestCopyFile(char *srcfname, char *dstfname) // TEST_COPYFILE
{
    // example cmdline: datest srcfile //1.2.3.4:7100/home/username/dstfile
    Owned<IFile> srcfile = createIFile(srcfname);
    Owned<IFileIO> srcio = srcfile->open(IFOcreate);
    char buf[100] = { "TestCopyFile" };
    srcio->write(0, 18, buf);
    srcio->close();
    Owned<IFile> dstfile = createIFile(dstfname);
    srcfile->copyTo(dstfile,0x100000,NULL,false);
}

void TestRemoteFile3(int nfiles, int fsizemb)
{
    //SocketEndpoint ep("10.150.10.8:7100");
    SocketEndpoint ep("127.0.1.1:7100");
    //ISocket *sock = ISocket::connect(7100,"10.150.10.8");

    // ------------------------------

    printf("TestRemoteFile3: nfiles = %d fsizemb = %d\n", nfiles, fsizemb);

    IFile *fileA[32769];
    IFileIO *ioA[32769];
    char filen[256] = { "" };

    size_t nrecs = (fsizemb*1024*1024) / sizeof(RecordStruct);

    unsigned t=msTick();

    if(nfiles > 0){

    for (int oi=0;oi<=nfiles;oi++) {
        sprintf(filen, "testfile.%d", oi);
        fileA[oi] = createRemoteFile(ep, filen);
        ioA[oi] = fileA[oi]->open(IFOcreate);
    }

    for (int oi=0;oi<=nfiles;oi++) {
        ioA[oi]->Release();
        fileA[oi]->Release();
    }

    unsigned r = msTick()-t;
    printf("elapsed time createRemoteFile (%d) = %lf (sec)\n", nfiles, (double)r/1000.0);

    }

    // ------------------------------

    if(nrecs > 0){

#if 1
    for (int oi=0;oi<=0;oi++) {

        IFile *file = createRemoteFile(ep, "testfile20.d00");
        IFileIO *io = file->open(IFOcreate);
        byte *buffer = (byte *)malloc(0x8000);
        unsigned br = 0x8000/sizeof(RecordStruct);
        size32_t buffsize = br*sizeof(RecordStruct);
        unsigned curidx = 0;
        unsigned nr = nrecs;
        __int64 pos = 0;
        t=msTick();
#if 1
        unsigned j;
        RecordStruct *rs;
        while (nr) {
            if (nr<br)
                br = nr;
            rs = (RecordStruct *)buffer;
            for (j=0;j<br;j++) {
                rs->idx = curidx++;
                itoa(rs->idx,rs->fill,16);
                unsigned k;
                for (k=strlen(rs->fill);k<sizeof(rs->fill);k++)
                    rs->fill[k] = ' ';
                rs->key = getRandom()%1000+1;
                rs->check = rs->idx*rs->key;
                rs->sum = 0;
                rs++;
            }
            size32_t wr = io->write(pos, br*sizeof(RecordStruct),buffer);
            assertex(wr==br*sizeof(RecordStruct));
            pos += br*sizeof(RecordStruct);
            nr -= br;
        }
        io->Release();
        unsigned r = msTick()-t;
        printf("Write (buffsize = %dk): elapsed time write = %lf (sec)\n",(buffsize+1023)/1024,(double)r/1000.0);
        Sleep(10);
#endif
        br = 0x2000/sizeof(RecordStruct);
        for (unsigned iter=1;iter<10;iter++) {
            t=msTick();
            buffsize = br*sizeof(RecordStruct);
            buffer = (byte *)realloc(buffer,buffsize);
            curidx = 0;
            nr = nrecs;
            pos = 0;
            unsigned r = msTick();
            IFileIO *io = file->open(IFOread);
            while (nr) {
                if (nr<br)
                    br = nr;
                size32_t rd = io->read(pos, br*sizeof(RecordStruct),buffer);
                // fprintf(stderr, "nr = %u rd = %u br = %u sizeof(RecordStruct) = %lu\n", nr, rd, br, sizeof(RecordStruct));
                assertex(rd==br*sizeof(RecordStruct));
                pos += br*sizeof(RecordStruct);
                nr -= br;
            }
            io->Release();
            r = msTick()-t;
            printf("Read (buffsize = %dk): elapsed time read = %lf (sec)\n",(buffsize+1023)/1024,(double)r/1000.0);
            Sleep(10);
            br += br;
        }

        file->Release();

    }
#endif

    }

    return;
}

void TestRemoteFile2()
{
    unsigned t=msTick();
    SocketEndpoint ep("10.150.10.8:7100");
    //ISocket *sock = ISocket::connect(7100,"10.150.10.8");
    IFile *file = createRemoteFile(ep, "testfile20.d00");
    IFileIO *io = file->open(IFOcreate);
    byte *buffer = (byte *)malloc(0x8000);
    unsigned br = 0x8000/sizeof(RecordStruct);
    size32_t buffsize = br*sizeof(RecordStruct);
    unsigned curidx = 0;
    unsigned nr = NRECS;
    __int64 pos = 0;
#if 0
    unsigned j;
    RecordStruct *rs;
    while (nr) {
        if (nr<br)
            br = nr;
        rs = (RecordStruct *)buffer;
        for (j=0;j<br;j++) {
            rs->idx = curidx++;
            itoa(rs->idx,rs->fill,16);
            unsigned k;
            for (k=strlen(rs->fill);k<sizeof(rs->fill);k++)
                rs->fill[k] = ' ';
            rs->key = getRandom()%1000+1;
            rs->check = rs->idx*rs->key;
            rs->sum = 0;
            rs++;
        }
        size32_t wr = io->write(pos, br*sizeof(RecordStruct),buffer);
        assertex(wr==br*sizeof(RecordStruct));
        pos += br*sizeof(RecordStruct);
        nr -= br;
    }
    io->Release();
    r = msTick()-t;
    printf("Write (buffsize = %dk): elapsed time write = %d\n",(buffsize+1023)/1024,t/1000); 
    Sleep(10);
#endif
    br = 0x2000/sizeof(RecordStruct);
    for (unsigned iter=1;iter<10;iter++) {     
        buffsize = br*sizeof(RecordStruct);
        buffer = (byte *)realloc(buffer,buffsize);
        curidx = 0;
        nr = NRECS;
        pos = 0;
        unsigned r = msTick();
        IFileIO *io = file->open(IFOread);
        while (nr) {
            if (nr<br)
                br = nr;
            size32_t rd = io->read(pos, br*sizeof(RecordStruct),buffer);
            assertex(rd==br*sizeof(RecordStruct));
            pos += br*sizeof(RecordStruct);
            nr -= br;
        }
        io->Release();
        r = msTick()-t;
        printf("Read (buffsize = %dk): elapsed time write = %d\n",(buffsize+1023)/1024,r/1000);
        Sleep(10);
        br += br;
    }

    file->Release();
}


void TestRemoteFile(unsigned part,unsigned of)
{
    byte *buffer = (byte *)malloc(0x10000);
    unsigned br = 0x8000/sizeof(RecordStruct);
    size32_t buffsize = br*sizeof(RecordStruct);
    unsigned t=msTick();
    unsigned nr = ((20*1024*1024)/(sizeof(RecordStruct)*of))*1024;
    
    //ISocket *sock = ISocket::connect(7100,"10.150.10.8");
    SocketEndpoint ep("10.150.10.8:7100");
    IFile *infile = createRemoteFile(ep, "testfile20.d00");
    IFileIO *inio = infile->open(IFOread);
    StringBuffer str("c:\\dali\\test1.");
    str.append(part);
    IFile *outfile = createIFile(str.str());
    IFileIO *outio = outfile->open(IFOcreate);
    offset_t inpos = (offset_t)(part-1)*nr*sizeof(RecordStruct);
    offset_t outpos = 0;
    while (nr) {
        if (nr<br)
            br = nr;
        size32_t rd = inio->read(inpos, br*sizeof(RecordStruct),buffer);
        assertex(rd==br*sizeof(RecordStruct));
        outio->write(outpos, br*sizeof(RecordStruct),buffer);
        inpos += br*sizeof(RecordStruct);
        outpos += br*sizeof(RecordStruct);
        nr -= br;
    }
    inio->Release();
    outio->Release();
    t = msTick()-t;
    printf("Transfer: elapsed time write = %d\n",t/1000); 

    infile->Release();
    outfile->Release();
    free(buffer);
}



void QTest(bool testput)
{
    PROGLOG("starting QTest %s",testput?"put":"get");
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0);
    Owned<IQueueChannel> channel = conn->open("testq");
    unsigned i;
    unsigned t1 = msTick();
    byte *buf=(byte *)malloc(1024*128);
    memset(buf,77,1024*128);
    if (!testput) {
        while (channel->probe()) {
            MemoryBuffer mb;
            channel->get(mb);
        }
    }
    unsigned qn = 0;
    unsigned n;
    for (n=1;n<=128;n++) {
        PROGLOG("start %d",n);
        unsigned t1 = msTick();
        for (i=0;i<1000;i++) {
            qn++;
            Sleep(getRandom()%1000);
            MemoryBuffer mb;
            if (testput) {
                mb.append("Hello").append(i);
                queryMyNode()->serialize(mb);
                buf[0] = qn%256;
                buf[1024*n-1] = 255-buf[0];
                mb.append(1024*n,buf);
                channel->put(mb);
#if 1
                if (i%100==99) {
                    PROGLOG("Put %i - %d on queue",i,channel->probe());
                    PROGLOG("time taken = %d",msTick()-t1);
                    t1 = msTick();
                }
#endif
            }
            else {
#if 1
                for (;;) {
                    if (channel->get(mb,0,100))
                        break;
                    printf(".");
                }
                printf("\n");
#else
                channel->get(mb);
#endif
                StringAttr str;
                unsigned p;
                mb.read(str).read(p);
                Owned<INode> node = deserializeINode(mb);
                size32_t sz = mb.length()-mb.getPos();
                if (sz) {
                    mb.read(sz,buf);
                    if ((buf[0]!=255-buf[sz-1])||(buf[0]!=qn%256)) {
                        PROGLOG("%d: sz=%d, buf[0]=%d, buf[sz-1]=%d  %d %d",qn,sz,(int)buf[0],(int)buf[sz-1],mb.length(),mb.getPos());
                        return;
                    }
                    assertex(buf[0]==255-buf[sz-1]);
#if 1
                    StringBuffer eps;
                    if (i%100==99) {
                        PROGLOG("Got %s - %d from %s",str.get(),n,node->endpoint().getEndpointHostText(eps).str());
                        PROGLOG("time taken = %d",msTick()-t1);
                        t1 = msTick();
                    }
#endif
                }
            }
        }
        PROGLOG("average message of %dK took %dms  ",n,msTick()-t1);
    }
    free(buf);
}

class cNotify: public CInterface, implements ISessionNotify
{
public:
    IMPLEMENT_IINTERFACE;
    Semaphore sem;
    void closed(SessionId id)
    {
        PROGLOG("Session closed %" I64F "d",id);
        sem.signal();
    }
    void aborted(SessionId id)
    {
        PROGLOG("Session aborted %" I64F "d",id);
        sem.signal();
    }
};

void Test_Session(const char *eps) // test for sessions
{
    if (!eps||!*eps) {
        for (unsigned i=0;i<100;i++) {
            PROGLOG("Tick %d",i);
            Sleep(1000);
        }
        return;
    }
    Owned<cNotify> cnotify = new cNotify;
    querySessionManager().subscribeSession(12345678,cnotify);
    cnotify->sem.wait();
    INode *node = createINode(eps,7777);
    SessionId id;
    for (;;) {
        id = querySessionManager().lookupProcessSession(node);
        if (id) {
            PROGLOG("Session looked up %" I64F "d",id);
            break;
        }
        Sleep(1000);
    }
    querySessionManager().subscribeSession(id,cnotify);
    cnotify->sem.wait();
    Sleep(1000);
}

void QTest2(bool testput)
{
    Owned<INamedQueueConnection> conn = createNamedQueueConnection(0);
    Owned<IQueueChannel> channel = conn->open("testq");
    CMessageBuffer mb;
    if (testput) {
        SessionId session = querySessionManager().startSession(0);
        PROGLOG("session started = %" I64F "d",session);
        mb.append(session);
        channel->put(mb);
        while (!querySessionManager().sessionStopped(session,1000*5))
            PROGLOG("Still going!");
    }
    else {
        channel->get(mb);
        SessionId session;
        mb.read(session);
        PROGLOG("Started");
        Sleep(1000*6);
        PROGLOG("stopping session %" I64F "d",session);
        querySessionManager().stopSession(session,false);
        PROGLOG("Stopped");
    }
}

#define TSUB
#ifdef TSUB
class TestSubscription : public CInterface, implements ISDSSubscription
{
public:
    IMPLEMENT_IINTERFACE;

// ISDSSubscription impl.
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        static int nno = 0;
        PROGLOG("%d: Notification(%" I64F "d) of %s - flags = %d", nno++, (__int64) id, xpath, flags);
        if (valueData)
        {
            StringBuffer data;
            appendURL(&data, (const char *)valueData, valueLen, 0);
            PROGLOG("ValueData = %s", data.str());
        }
    }
};
#endif

class TSDSThread : public CInterface, implements IPooledThread
{
    StringAttr path;
public:
    IMPLEMENT_IINTERFACE;

    virtual void init(void *param) override
    {
        path.set((char *) param);
    }

    virtual void threadmain() override
    {
        try
        {
            Owned<IRemoteConnection> conn = querySDS().connect(path, myProcessSession(), RTM_LOCK_WRITE|RTM_LOCK_SUB, 1000000);
            PROGLOG("connecting to %s", path.get());
            if (!conn)
                throw MakeStringException(-1, "Failed to connect to path %s", path.get());
            IPropertyTree *root = conn->queryRoot();

            root->setPropInt("TTestProp1", fastRand());
            root->setPropInt("TTestProp2", fastRand());
        }
        catch (IException *e)
        {
            PrintExceptionLog(e, NULL);
        }
    }

    virtual bool stop() override
    {
        return true;
    }

    virtual bool canReuse() const override { return true; }
};

class TSDSTestPool : public CInterface, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;

    virtual IPooledThread *createNew()
    {
        return new TSDSThread();
    }
};


class CQPutTest : public Thread
{
public:
    CQPutTest() : Thread("CQPutTest") { start(false); }
    virtual int run()
    {
        try {
            QTest(true);
        }
        catch (IException *e) {
            pexception("QTest(true): Exception",e);
        }
        return 0;
    }
};
class CQGetTest : public Thread
{
public:
    CQGetTest() : Thread("CQPutTest") { start(false); }
    virtual int run()
    {
        try {
            QTest(false);
        }
        catch (IException *e) {
            pexception("QTest(false): Exception",e);
        }
        return 0;
    }
};

void testSubscription(bool subscriber, int subs, int comms)
{
    class TestSubscription : public CInterface, implements ISDSSubscription
    {
    public:
        IMPLEMENT_IINTERFACE;
        virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            PROGLOG("Notification(%" I64F "x) of %s - flags = %d",(__int64) id, xpath, flags);
            if (valueData)
            {
                StringBuffer data;
                appendURL(&data, (const char *)valueData, valueLen, 0);
                PROGLOG("ValueData = %s", data.str());
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
                    PROGLOG("Subscribe %d",i);
                    StringBuffer key;
                    key.append("/TESTS/TEST").append(i);
                    ids[i] = querySDS().subscribe(key.str(), *subs[i], true);
            }
            PROGLOG("paused 1");
            getchar();
            for (i=0; i<subscriptions; i++)  {
                    querySDS().unsubscribe(ids[i]);
                    subs[i]->Release();
            }
            PROGLOG("paused");
            getchar();
    }
    else {
            Owned<IRemoteConnection> conn = querySDS().connect("/TESTS",
myProcessSession(), RTM_CREATE_QUERY, 1000000);
            IPropertyTree *root = conn->queryRoot();
            unsigned i, _i;

            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i);
                    root->setPropTree(key.str(), createPTree());

            }
            conn->commit();

            PROGLOG("paused 1");

            getchar();
            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i).append("/index");
                    root->setPropInt(key.str(), i);
                    PROGLOG("Commit %d", i);
                    conn->commit();
            }
            PROGLOG("paused 2");
            getchar();
            for (_i=0; _i<commits; _i++) {
                i = _i%subscriptions;
                    StringBuffer key;
                    key.append("TEST").append(i).append("/index");
                    root->setPropInt(key.str(), subscriptions-i);            
                    conn->commit();
            }
            PROGLOG("paused 3");
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

class CCSub : public CInterface, implements ISDSConnectionSubscription
{
    StringAttr conn;
public:
    IMPLEMENT_IINTERFACE;

    CCSub(const char *_conn) : conn(_conn) { }
    virtual void notify()
    {
        PROGLOG("Connection %s changed", conn.get());
    }
};

class CChange : public Thread
{
    Owned<IRemoteConnection> conn;
    Owned<CCSub> sub;
    StringAttr path;

public:
    CChange(const char *_path) : path(_path)
    {
        conn.setown(querySDS().connect(_path, myProcessSession(), RTM_CREATE_QUERY, 1000000));

        id1 = id2 = 0;
        start(false);
    }
    virtual int run()
    {
        for (;;)
        {
            conn->queryRoot()->setPropInt("testprop", fastRand()*100);
            conn->commit();
            if (id1)
            {
                conn->unsubscribe(id1);
                id1 = 0;
            }
            else
            {
                StringBuffer s("sub1 ");
                s.append(path);
                id1 = conn->subscribe(*new CCSub(s.str()));
            }
            if (id2)
            {
                conn->unsubscribe(id2);
                id2 = 0;
            }
            else
            {
                StringBuffer s("sub2 ");
                s.append(path);
                id2 = conn->subscribe(*new CCSub(s.str()));
            }
        }
        throwUnexpected(); // loop never terminates, but some compilers complain about missing return without this line
    }

    SubscriptionId id1, id2;
};

void testConnectionSubscription()
{
    IArrayOf<CChange> a;

    unsigned c;
    for (c=0; c<10; c++)
    {
        StringBuffer s("/TESTS/CONSUB");
        s.append(c+1);
        a.append(*new CChange(s.str()));
    }

    ForEachItemIn(cc, a)
        a.item(cc).join();
}

void TestStress()
{
// config.
    unsigned maxRunning = 40;
    unsigned count = 1000;
    unsigned branches = 4;
    unsigned pauseWhenBusyDelay = 200;
    const char *branchPrefix = "branch";
    bool queues = false;

//
    Owned<IRemoteConnection> conn = querySDS().connect("/Stress", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, 1000000);
    PROGLOG("connected to /Stress");

    IPropertyTree *root = conn->queryRoot();
    unsigned b;
    for (b=0; b<branches; b++)
    {
        StringBuffer branch(branchPrefix);
        branch.append(b+1);
        root->setPropTree(branch.str(), createPTree());
    }
    conn->commit();

//
    Owned<CQPutTest> putTest;
    Owned<CQGetTest> getTest;
//

    if (queues)
    {
        putTest.setown(new CQPutTest());
        getTest.setown(new CQGetTest());
    }
    TSDSTestPool poolFactory;
    Owned<IThreadPool> pool = createThreadPool("TSDSTest", &poolFactory, false, nullptr);

    unsigned path = 0;
    while (count)
    {
        Owned<IPooledThreadIterator> rIter = pool->running();
        unsigned c=0;
        ForEach (*rIter) c++;
        if (c>=maxRunning)
        {
            PROGLOG("Pause");
            Sleep(pauseWhenBusyDelay);
        }
        else
        {
            StringBuffer branch("Stress/");
            branch.append(branchPrefix).append(++path);
            pool->start((void *)branch.str());
            if (path >= branches)
                path = 0;
            count--;
        }
    }

    PROGLOG("Joining all TSDSThread running threads");
    pool->joinAll();
    pool.clear();

    if (queues)
    {
        PROGLOG("Joining putTest");
        putTest->join();
        PROGLOG("Joining gettTest");
        getTest->join();
    }

    PROGLOG("Finished");

    return;
}

#define SDS_LOCK_TIMEOUT (unsigned)-1
class CStressTestBase : public CInterface, implements IInterface
{
    StringAttr name;
    unsigned occurence;
public:
    IMPLEMENT_IINTERFACE;
    CStressTestBase(const char *_name, unsigned _occurence) : name(_name), occurence(_occurence)
    {
    }
    void threadmain()
    {
        PROGLOG("test; %s - start", name.get());
        test();
        PROGLOG("test; %s - end", name.get());
    }
    virtual void test() = 0;

    const char *queryName() { return name; }
    unsigned queryOccurence() { return occurence; }
};

class CStressTest1 : public CStressTestBase
{
public:
    CStressTest1(unsigned o) : CStressTestBase("StressTest1", o) { }
    virtual void test()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/common", myProcessSession(), RTM_CREATE|RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->addProp(queryName(), queryName());
        MilliSleep(getRandom()%100);
        conn->close(true);
    }
};

class CStressTest2 : public CStressTestBase
{
public:
    CStressTest2(unsigned o) : CStressTestBase("StressTest2", o) { }
    virtual void test()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/common", myProcessSession(), RTM_LOCK_READ|RTM_CREATE_ADD|RTM_DELETE_ON_DISCONNECT, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->addProp(queryName(), queryName());
        MilliSleep(getRandom()%100);
    }
};

class CStressTest3 : public CStressTestBase
{
public:
    CStressTest3(unsigned o) : CStressTestBase("StressTest3", o) { }
    virtual void test()
    {
        // semantics a little odd, lock ensures n
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/common/", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        conn->queryRoot()->addProp(queryName(), queryName());
        MilliSleep(getRandom()%100);
        conn->close(true);
    }
};

class CStressTest4 : public CStressTestBase
{
public:
    CStressTest4(unsigned o) : CStressTestBase("StressTest4", o) { }
    virtual void test()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/common", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        // possible legitimate exception : ambiguous
        if (conn) // may not exist
        {
            conn->changeMode(RTM_LOCK_WRITE); // non-exclusive->exclusive lock, something else can grab and orphan, possible exception: orphaned node
            conn->queryRoot()->addProp(queryName(), queryName());
            MilliSleep(getRandom()%100);
        }
    }
};

class CStressTest5 : public CStressTestBase
{
public:
    CStressTest5(unsigned o) : CStressTestBase("StressTest5", o) { }
    virtual void test()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/common", myProcessSession(), RTM_LOCK_WRITE|RTM_DELETE_ON_DISCONNECT, SDS_LOCK_TIMEOUT);
        // possible legitimate exception : ambiguous
        if (conn) // may not exist
        {
            conn->changeMode(RTM_LOCK_READ);
            conn->queryRoot()->addProp(queryName(), queryName());
            MilliSleep(getRandom()%100);
        }
    }
};

class CStressTest6 : public CStressTestBase
{
public:
    CStressTest6(unsigned o) : CStressTestBase("StressTest6", o) { }
    virtual void test()
    {
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2/ext/", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT);
        MemoryBuffer mb;
        void *mem = mb.reserveTruncate(100*1024);
        memset(mem, 1, mb.length());
        conn->queryRoot()->setPropBin("bin", mb.length(), mb.toByteArray());
        conn.clear();
        MilliSleep(getRandom()%500);
        conn.setown(querySDS().connect("/Tests/Stress2/ext/", myProcessSession(), RTM_LOCK_WRITE, SDS_LOCK_TIMEOUT));
        conn->queryRoot()->removeProp("bin");
        conn.clear();
    }
};

class CStressPoolFactory : public CInterface, public IThreadFactory
{
    class CStressPoolHandler : public CInterface, implements IPooledThread
    {
        Linked<CStressTestBase> stressTest;
        unsigned delay, test;
    public:
        IMPLEMENT_IINTERFACE;
        virtual void init(void *startInfo) override
        {
            stressTest.set((CStressTestBase *)startInfo);
        }

        virtual void threadmain() override
        {
            try
            {
                stressTest->threadmain();
            }
            catch (IException *e)
            {
                StringBuffer s(stressTest->queryName());
                EXCLOG(e, s.append(" failure").str());
            }
        }
        virtual bool canReuse() const override
        {
            return true;
        }
        virtual bool stop() override
        {
            return true;
        }
    };
public:
    IMPLEMENT_IINTERFACE;
    IPooledThread *createNew()
    {
        return new CStressPoolHandler();
    }
};

struct Range
{
    unsigned which;
    unsigned lower;
    unsigned upper;
};
static int rangeFind(const void *_key, const void *e)
{
    unsigned key = *(unsigned *)_key;
    Range &range = *(Range *)e;
    if (key < range.lower)
        return -1;
    else if (key >= range.upper)
        return 1;
    else
        return 0;
}

void TestStress2()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/Tests/Stress2", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
    conn->changeMode(RTM_LOCK_READ);
    
    Owned<CStressPoolFactory> factory = new CStressPoolFactory();
    Owned<IThreadPool> threadPool = createThreadPool("Stress2 Thread Pool", factory, false, nullptr, 60);

    unsigned totalCount = 0;
    unsigned subCount = 1;
    unsigned whichTest = (unsigned)-1;
    unsigned o = testParams.ordinality();
    if (o)
    {
        totalCount = atoi(testParams.item(0));
        if (o>1)
            whichTest = atoi(testParams.item(1));
    }

    CIArrayOf<CStressTestBase> tests;
    tests.append(*new CStressTest1(20));
    tests.append(*new CStressTest2(20));
    tests.append(*new CStressTest3(15));
    tests.append(*new CStressTest4(15));
    tests.append(*new CStressTest5(15));
    tests.append(*new CStressTest6(15));
    
    MemoryBuffer rangeMb;
    Range *ranges = (Range *) rangeMb.reserveTruncate(sizeof(Range) * tests.ordinality());
    unsigned r = 0;
    ForEachItemIn(p, tests)
    {
        Range &range = ranges[p];
        range.which = p;
        unsigned perc = tests.item(p).queryOccurence();
        range.lower = r;
        r += perc;
        assertex(r <= 100);
        range.upper = r;
    }
    
    // count # of each test

    seedRandom((unsigned)get_cycles_now());
    bool stop = false;
    while (!stop)
    {
        unsigned test;
        if ((unsigned)-1 == whichTest)
        {
            test = getRandom() % 100;
            const void *result = bsearch(&test, ranges, tests.ordinality(), sizeof(Range), rangeFind);
            if (result)
            {
                Range &range = *(Range *)result;
                test = range.which;
            }
            else
                test = NotFound;
        }
        else
        {
            test = whichTest-1; // (input = 1  based)
            if (test >= tests.ordinality())
                throw MakeStringException(0, "Test out of range, there are only %d tests", tests.ordinality());
        }

        if (NotFound == test)
        {
            PROGLOG("No test run this cycle");
            unsigned delay = getRandom()%200;
            MilliSleep(delay);
        }
        else
        {
            unsigned _subCount = subCount;
            while (_subCount--)
            {
                unsigned delay = getRandom()%200+150;
                MilliSleep(delay);
                try
                {
                    threadPool->startNoBlock(&tests.item(test));
                }
                catch (IException *e)
                {
                    EXCLOG(e, NULL);
                    MilliSleep(1000);
                }
                if (totalCount && --totalCount == 0)
                {
                    stop = true;
                    break;
                }
            }
        }
    }

    threadPool->joinAll();
}

#define UNDERTHRESHOLD 0x10
#define OVERTHRESHOLD  0x100000
#define CREATEINITIAL
//#define REMOVEEXT
void TestExternal()
{
    Owned<IRemoteConnection> conn;
    IPropertyTree *root;
    size32_t sz;
    unsigned l;

    void *test = malloc(OVERTHRESHOLD);
    MemoryBuffer mb;
    mb.setBuffer(OVERTHRESHOLD, test, true);
    memset(test, 'A', OVERTHRESHOLD);

    StringBuffer extPropName("testExternal");
    extPropName.append(getRandom()%3);
    for (l=0; l<2; l++)
    {
        conn.setown(querySDS().connect("/Tests", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, 2000*MDELAY));
        root = conn->queryRoot();

#ifdef CREATEINITIAL
        {
            Owned<IFile> ifile = createIFile("c:\\utils\\pkzipc.exe");
            Owned<IFileIO> fileIO = ifile->open(IFOread);
            assertex(fileIO);

            sz = (size32_t)ifile->size();
            void *mem = malloc(sz);

            fileIO->read(0, sz, mem);

            root->setPropBin(extPropName.str(), sz, mem);
            free(mem);
        }

        PROGLOG("Writing binary to SDS, size=%d", sz);
        conn->commit();
        PROGLOG("Written binary to SDS, size=%d", sz);
        conn.clear();
#endif
        conn.setown(querySDS().connect("/Tests", myProcessSession(), RTM_LOCK_READ, 2000*MDELAY));
        root = conn->queryRoot();

        MemoryBuffer mb;
        PROGLOG("Reading binary to SDS");
        verifyex(root->getPropBin(extPropName.str(), mb));

        {
            Owned<IFile> ifile = createIFile("testExt.exe");
            Owned<IFileIO> fileIO = ifile->open(IFOcreate);
            assertex(fileIO);

            sz = mb.length();
            fileIO->write(0, mb.length(), mb.toByteArray());
        }
        PROGLOG("Read back binary, size=%d", sz);

        PROGLOG("Writing large string");
        char *str = (char *)test;
        str[32000] = '\0';
        root->setProp("largeString", str);

        const char *b = root->queryProp("@sds:ext");
        conn->commit();
        PROGLOG("Written large string");

        conn.clear();
    }
    return;
    conn.setown(querySDS().connect("/Tests", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, 2000*MDELAY));
    root = conn->queryRoot();

    root->setPropBin(extPropName.str(), UNDERTHRESHOLD, test);
    PROGLOG("setting binary to small, size=%d", UNDERTHRESHOLD);
    conn->commit();
    PROGLOG("set binary to small, size=%d", UNDERTHRESHOLD);
    root->setPropBin(extPropName.str(), 1024, test);
    PROGLOG("setting binary to big, size=%d", OVERTHRESHOLD);
    conn->commit();
    PROGLOG("set binary to big, size=%d", OVERTHRESHOLD);

#ifdef REMOVEEXT
    root->removeProp(extPropName.str());
    PROGLOG("removing binary to small, size=%d", UNDERTHRESHOLD);
    conn->commit();
    PROGLOG("removed prop");
#endif
}

class CSubTest : public Thread
{
public:
    CSubTest(const char *_path) : path(_path) { start(false); }

    virtual int run()
    {
        srand( (unsigned)time( NULL ) );
        try
        {
            unsigned extra = fastRand()%2 ? RTM_LOCK_SUB : 0;
            Owned<IRemoteConnection> conn2 = querySDS().connect(path, myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|extra, 1000);
        }
        catch (IException *e)
        {
            StringBuffer s("Timed out connecting to");
            s.append(path);
            PrintExceptionLog(e, s.str());
        }
        return 1;
    }
private:
    StringAttr path;
};

void TestSubLocks()
{
    IPropertyTree *root;

    { Owned<IRemoteConnection> conn = querySDS().connect("/Tests", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE, 2000*MDELAY);
    }
    Owned<IRemoteConnection> conn1 = querySDS().connect("/Tests/SubLocks", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|RTM_LOCK_SUB, WAIT_FOREVER);
    root = conn1->queryRoot();
    root->setProp("b1", "branch 1");
    root->setProp("b2", "branch 2");
    conn1->commit();

    try
    {
        // expect to fail.
        Owned<IRemoteConnection> conn2 = querySDS().connect("/Tests/SubLocks/b1", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|RTM_LOCK_SUB, 1000);
    }
    catch (IException *e)
    {
        PrintExceptionLog(e, "Timed out connecting to /Tests/SubLocks/b1");
    }

    conn1.clear();
    conn1.setown(querySDS().connect("/Tests/SubLocks/b1", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|RTM_LOCK_SUB, 1000));

    try
    {
        // expect to succeed.
        Owned<IRemoteConnection> conn = querySDS().connect("/Tests/SubLocks/b2", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|RTM_LOCK_SUB, 1000);
    }
    catch (IException *e)
    {
        PrintExceptionLog(e, "Timed out connecting to /Tests/SubLocks/b2");
    }

    try
    {
        // expect to fail.
        Owned<IRemoteConnection> conn2 = querySDS().connect("/Tests/SubLocks", myProcessSession(), RTM_CREATE_QUERY|RTM_LOCK_WRITE|RTM_LOCK_SUB, 1000);
    }
    catch (IException *e)
    {
        PrintExceptionLog(e, "Timed out connecting to /Tests/SubLocks");
    }

    // random testing
    unsigned num = 100;
    IArrayOf<CSubTest> threads;
    StringArray paths;
    paths.append("/Tests/SubLocks");
    paths.append("/Tests/SubLocks/b1");
    paths.append("/Tests/SubLocks/b2");
    unsigned i;
    for (i=0; i<num; i++)
    {
        CSubTest * t = new CSubTest(paths.item(fastRand()%paths.ordinality()));
        threads.append(* t);
    }
    PROGLOG("joining");
    for (i=0; i<num; i++)
    {
        threads.item(i).join();
    }

    PROGLOG("SubLocks test done");
}

void TestSDS1()
{
    StringBuffer xml;
    ISDSManager &sdsManager = querySDS();
    IRemoteConnection *conn;
    IPropertyTree *root;

#ifdef TSUB
    Owned<TestSubscription> ts = new TestSubscription();
    SubscriptionId id = querySDS().subscribe("/subtest", *ts, false, true);

    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();

    unsigned i;
    for (i=0; i<10; i++)
    {
        while (root->removeProp("subtest"));
        conn->commit();
        root->setPropInt("subtest", i+1);
        root->setProp("subtest[1]/aaa", "2");
        root->setProp("subtest[1]/aaa/aaasub", "2");
        root->setProp("subtest[1]/bbb", "2");
        root->setProp("blah", "3");
        conn->commit();
    }

    conn->Release();

    querySDS().unsubscribe(id);

    return;
#endif
#if 1
    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE | RTM_LOCK_SUB, 2000*MDELAY);
    root = conn->queryRoot();

    IPropertyTree *t1 = createPTree();
    IPropertyTree *t2 = root->setPropTree("t2", createPTree());
    IPropertyTree *_t1 = t2->setPropTree("t1", t1);
    _t1->setPropTree("under_t1", createPTree());

    IPropertyTree *t3 = root->setPropTree("t3", createPTree());

    conn->commit();

    _t1 = t2->getPropTree("t1");
    t2->removeTree(_t1);

    conn->commit();

    t3->addPropTree("newName", _t1);

    conn->commit();
    conn->Release();

    return;


    Owned<IPropertyTreeIterator> diter = root->getElements("*");
    while (diter->first())
    {
        IPropertyTree &child = diter->query();
        PROGLOG("child = %s", child.queryName());
        root->removeTree(&child);
    }

    conn->commit();

    conn->Release();
#endif

#if 1 // test attribute deletion etc.
    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();

    root->removeProp("attrtree");
    IPropertyTree *attrTree = createPTree();
    attrTree->addProp("@rattr1", "a");
    attrTree->addProp("@rattr2", "b");
    attrTree->addProp("@rattr3", "c");
    attrTree->addProp("@rattr4", "d");
    attrTree->addProp("adt", "e");
    attrTree->addProp("adt/@attr1", "f");
    attrTree->addProp("adt/@attr2", "g");
    attrTree->addProp("adt/@attr3", "h");

    root->setPropTree("attrtree", attrTree);

    conn->Release();

    conn = sdsManager.connect("/attrtree", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();

    verifyex(root->removeProp("@rattr3"));
    verifyex(root->removeProp("adt/@attr3"));

    conn->Release();

    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();
    verifyex(root->removeProp("attrtree"));
    conn->Release();
#endif


#if 1 // test1 qualified add/set
    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();

    root->removeProp("blah");
    root->addProp("blah", "blahv1");

    conn->commit();
    root->addProp("blah", "blahv2");
    root->setProp("blah[2]/subb", "ggg");
    conn->commit();
    root->addProp("blah", "blahv3");
    conn->commit();
    conn->Release();

    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();
    root->removeProp("blah");
    conn->Release();

#endif

#if 1 // test2 qualified add/set
    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();

    root->setProp("Software", "test");
    root->addProp("Software", "jaketest1");
    root->addProp("Software[1]/hello", "jaketest2-hello1");
    root->addProp("Software[1]/hello[1]", "jaketest2-hello2");
    root->addProp("Software", "jaketest3");
    root->addProp("Software[2]", "jaketest4");
    root->addProp("Software[1]", "jaketest5");
    root->addProp("Software[1]/hello[1]", "jaketest5-hello1");
    root->addProp("Software[2]/hello[1]", "jaketestX2-hello0");

    conn->commit();

    IPropertyTreeIterator *iter = root->getElements("Software");
    iter->first();
    int x=0;
    while (iter->isValid())
    {
        IPropertyTree &t = iter->query();
        printf("x=%d, val=%s\n", x, t.queryProp(NULL));
        iter->next();
        x++;
    }
    iter->Release();
    StringBuffer s;
    toXML(root, s);
    printf("XML : %s\n", s.str());

    conn->Release();

    conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->queryRoot();
    root->removeProp("Software");
    conn->Release();

#endif

#if 1 // test similar to DFS file release.
    // create f (local)
    IPropertyTree *f = createPTree("file", ipt_caseInsensitive);
    IPropertyTree *p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","1");
    p->setProp("filename","testfile1.d00._1_of_3");
    p->setProp("node","192.168.0.3");
    f->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","2");
    p->setProp("filename","testfile1.d00._2_of_3");
    p->setProp("node","192.168.0.3");
    f->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","3");
    p->setProp("filename","testfile1.d00._3_of_3");
    p->setProp("node","192.168.0.3");
    f->addPropTree("part",p);
    f->setProp("directory","c:\\thordata");
    f->setProp("@name","testfile1");
    
    IPropertyTree *f2 = createPTree("file", ipt_caseInsensitive);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","1");
    p->setProp("filename","testfile2.d00._1_of_3");
    p->setProp("node","192.168.0.3");
    f2->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","2");
    p->setProp("filename","testfile2.d00._2_of_3");
    p->setProp("node","192.168.0.3");
    f2->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","3");
    p->setProp("f2ilename","testfile2.d00._3_of_3");
    p->setProp("node","192.168.0.3");
    f2->addPropTree("part",p);
    f2->setProp("directory","c:\\thordata");
    f2->setProp("@name","testfile2");
    
    IPropertyTree *f3 = createPTree("file", ipt_caseInsensitive);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","1");
    p->setProp("filename","testfile3.d00._1_of_3");
    p->setProp("node","192.168.0.3");
    f3->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","2");
    p->setProp("filename","testfile3.d00._2_of_3");
    p->setProp("node","192.168.0.3");
    f3->addPropTree("part",p);
    p = createPTree("part", ipt_caseInsensitive);
    p->setProp("@num","3");
    p->setProp("filename","testfile3.d00._3_of_3");
    p->setProp("node","192.168.0.3");
    f3->addPropTree("part",p);
    f3->setProp("directory","c:\\thordata");
    f3->setProp("@name","testfile3");

    conn = sdsManager.connect("/Files", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
    root = conn->getRoot();
    const char *name = root->queryName();


    IPropertyTree *sroot = createPTree();
    sroot->setProp("@name","nigel");
    sroot = root->addPropTree("scope",sroot);
    IPropertyTree *sroot2 = createPTree();
    sroot2->setProp("@name","test");
    sroot2 = sroot->addPropTree("scope",sroot2);


    bool b1 = root->removeProp("scope[@name=\"nigel\"]/scope[@name=\"test\"]/file[@name=\"testfile1\"]");

    sroot2->addPropTree("file",f);


    conn->commit();

    bool b2 = root->removeProp("scope[@name=\"nigel\"]/scope[@name=\"test\"]/file[@name=\"testfile2\"]");
    sroot2->addPropTree("file",f2);
    conn->commit();

    bool b3 = root->removeProp("scope[@name=\"nigel\"]/scope[@name=\"test\"]/file[@name=\"testfile3\"]");
    sroot2->addPropTree("file",f3);
    conn->commit();

    root->Release();
    conn->Release();

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    conn = sdsManager.connect("Files/scope[@name=\"nigel\"]/scope[@name=\"test\"]/file[@name=\"testfile2\"]", myProcessSession(), RTM_LOCK_WRITE, 5000*MDELAY);
    root = conn->queryRoot();

    toXML(root, xml.clear());

    PROGLOG("previously committed file : %s", xml.str());

    conn->Release();
#endif


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//  conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 5000*MDELAY);
//  conn = sdsManager.connect("/", myProcessSession(), 0, 5000*MDELAY);
//  conn = sdsManager.connect("/newbranch", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE, 5000*MDELAY);
//  conn = querySDS().connect("/Files/scope[@name=\"test\"]/file[@name=\"testfile1\"]", myProcessSession(), RTM_LOCK_READ, 10000*MDELAY);
//  conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_UNIQUE, 5000*MDELAY);
//  conn = sdsManager.connect("/nonexist", myProcessSession(), RTM_LOCK_WRITE, 5000*MDELAY);
//  conn = querySDS().connect("/Files", myProcessSession(), RTM_LOCK_READ, 10000*MDELAY);
//  conn = querySDS().connect("/Files/WorkUnit", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, 10000*MDELAY);
//  conn = querySDS().connect("Files/scope[@name=\"nigel\"]/scope[@name=\"test\"]/file[@name=\"testfile3\"]", myProcessSession(), RTM_LOCK_READ, 10000*MDELAY);
//  conn = querySDS().connect("Files/scope[@name=\"nigel\"]/scope[@name=\"test\"]", myProcessSession(), RTM_LOCK_READ, 10000*MDELAY);

#if 1 // CREATE_ADD test
    conn = sdsManager.connect("/newbranch", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, 5000*MDELAY);
    root = conn->getRoot();

    root->setPropInt("@id", 5);

    IPropertyTree *a = createPTree(ipt_caseInsensitive);
    a->setProp("@attr1", "123");

    root->setPropTree("file", LINK(a));

    root->Release();
    conn->Release();

//////////////////

    conn = sdsManager.connect("/newbranch[@id=\"5\"]", myProcessSession(), RTM_LOCK_WRITE, 5000*MDELAY);
    root = conn->queryRoot();
    bool b = root->removeProp("file[@attr1=\"123\"]");
    conn->Release();
#endif

#if 1  // CREATE test
    conn = sdsManager.connect("/newbranch", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE, 5000*MDELAY);
    root = conn->getRoot();
    root->setPropInt("@id", 10);
    root->setPropInt("sub1", 5);
    IPropertyTree *sub = root->queryPropTree("sub1");

    IPropertyTree *subsub = createPTree();
    subsub->setProp("hello", "there");
    sub->setPropTree("hellosubsub", subsub);
    root->Release();
    conn->Release();


    conn = sdsManager.connect("/newbranch[@id=\"10\"]", myProcessSession(), RTM_LOCK_WRITE, 5000*MDELAY);
    root = conn->queryRoot();
    sub = root->queryPropTree("sub1");

    toXML(sub, xml.clear());
    PROGLOG("hello = %s", xml.str());

    conn->Release();
#endif
}

void testDfuStreamRead(StringArray &params)
{
    // reads a DFS file
    try
    {
        const char *fname = params.item(0);
        const char *filter = nullptr;
        const char *outputECLFormat = nullptr;
        if (params.ordinality()>1)
        {
            filter = params.item(1);
            if (isEmptyString(filter))
                filter = nullptr;
            if (params.ordinality()>2)
            {
                outputECLFormat = params.item(2);
                if (isEmptyString(outputECLFormat))
                    outputECLFormat = nullptr;
            }
        }
        Owned<IUserDescriptor> userDesc = createUserDescriptor();
        userDesc->set("jsmith","password");

        Owned<IDFUFileAccess> srcFile = lookupDFUFile(fname, "testDfuStreamRead", 300, userDesc);
        if (!srcFile)
        {
            WARNLOG("File '%s' not found!", fname);
            return;
        }

        IOutputMetaData *meta = srcFile->queryEngineInterface()->queryMeta();
        CommonXmlWriter xmlWriter(XWFnoindent);

        unsigned sourceN = srcFile->queryNumParts();
        for (unsigned p=0; p<sourceN; p++)
        {
            Owned<IDFUFilePartReader> reader = srcFile->createFilePartReader(p, 0, nullptr, true);

            if (outputECLFormat)
            {
                reader->setOutputRecordFormat(outputECLFormat);
                meta = reader->queryMeta();
            }

            if (filter)
                reader->addFieldFilter(filter);

            reader->start();

            while (true)
            {
                size32_t sz;
                const void *row = reader->nextRow(sz);
                if (!row)
                {
                    if (!srcFile->queryIsGrouped())
                        break;
                    row = reader->nextRow(sz);
                    if (!row)
                        break;
                }
                meta->toXML((const byte *)row, xmlWriter.clear());
                PROGLOG("Row: %s", xmlWriter.str());
            }
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
}

void testDfuStreamWrite(const char *fname)
{
    // reads a DFS file and writes it to <filename>_copy
    try
    {
        Owned<IUserDescriptor> userDesc = createUserDescriptor();
        userDesc->set("jsmith","password");

        const char *newFileName = "dfsstream::newfile1";
        if (!isEmptyString(fname))
            newFileName = fname;
        const char *newEclRecDef = "{ string10 fname; string10 sname; unsigned4 age; };";
        Owned<IDFUFileAccess> tgtFile = createDFUFile(newFileName, "mythor", dft_flat, newEclRecDef, "datest-write-newfile1", 300, true, userDesc); // NB: compressed file

// NB: must match record definition
        struct Row
        {
            std::string fname;
            std::string sname;
            unsigned age;
        };
        const std::array<Row, 6> rows = { { { "John      ", "Smith     ", 59 },
                                            { "Samuel    ", "Peeps     ", 39 },
                                            { "Bob       ", "Marks     ", 12 },
                                            { "Jake      ", "Smith     ", 12 },
                                            { "Paul      ", "Smith     ", 12 },
                                            { "Sarah     ", "Potters   ", 28 }
                                          }
                                        };

        offset_t fileSize = 0;
        unsigned numRecs = 0;
        unsigned targetN = tgtFile->queryNumParts();
        for (unsigned p=0; p<targetN; p++)
        {
            Owned<IDFUFilePartWriter> writer = tgtFile->createFilePartWriter(p);
            writer->start();
            unsigned numPartRecs = 0;
            offset_t partSize = 0;
            for (auto &row: rows)
            {
                char rowMem[24];
                memcpy(rowMem, row.fname.c_str(), 10);
                memcpy(rowMem+10, row.sname.c_str(), 10);
                memcpy(rowMem+20, &row.age, sizeof(row.age));
                writer->write(sizeof(rowMem), rowMem);
                partSize += sizeof(rowMem);
                ++numPartRecs;
            }
            tgtFile->setPartPropertyInt(p, "@recordCount", numPartRecs); // JCSMORE
            tgtFile->setPartPropertyInt(p, "@size", partSize);
            numRecs += numPartRecs;
            fileSize += partSize;
        }
        tgtFile->setFilePropertyInt("@recordCount", numRecs);
        tgtFile->setFilePropertyInt("@size", fileSize);
        publishDFUFile(tgtFile, true, userDesc);
        tgtFile.clear();


        // Read the file back


        Owned<IDFUFileAccess> srcFile = lookupDFUFile(newFileName, "datest-read-newfile1", 300, userDesc);
        if (!srcFile)
        {
            WARNLOG("File '%s' not found!", newFileName);
            return;
        }

        unsigned sourceN = srcFile->queryNumParts();
        for (unsigned p=0; p<sourceN; p++)
        {
            Owned<IDFUFilePartReader> reader = srcFile->createFilePartReader(p);

            // filter by Smith and project to new format
            reader->addFieldFilter("sname=['Smith']");
            reader->setOutputRecordFormat("{ string5 age; string20 fname; };");

            reader->start();

            while (true)
            {
                size32_t sz;
                const byte *row = (const byte *)reader->nextRow(sz);
                if (!row)
                {
                    if (!srcFile->queryIsGrouped())
                        break;
                    row = (const byte *)reader->nextRow(sz);
                    if (!row)
                        break;
                }
                PROGLOG("Row: age=%.*s, fname=%.*s", 5, row, 20, row+5);
            }
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
}

void testDfuStreamCopy(const char *srcFileName)
{
    // reads a DFS file and writes it to <filename>_copy
    try
    {
        if (isEmptyString(srcFileName))
            throw makeStringException(0, "no source logical filename supplied");

        Owned<IUserDescriptor> userDesc = createUserDescriptor();
        userDesc->set("jsmith","password");

        Owned<IDFUFileAccess> srcFile = lookupDFUFile(srcFileName, "datest", 60, userDesc);
        if (!srcFile)
        {
            WARNLOG("File '%s' not found", srcFileName);
            return;
        }
        IDFUFileAccessExt *srcFileEx = srcFile->queryEngineInterface();

        const char *eclRecDef = srcFileEx->queryProperties().queryProp("ECL");
        if (!eclRecDef)
            throw makeStringExceptionV(0, "File '%s' has no record definition", srcFileName);
        IOutputMetaData *srcMeta = srcFileEx->queryMeta();

        const char *srcGroup = srcFile->queryClusterGroupName();
        const char *clusterName = startsWith(srcGroup, "hthor__") ? "myeclagent" : "mythor";
        StringBuffer tgtFileName(srcFileName);
        tgtFileName.append("_copy");
        Owned<IDFUFileAccess> tgtFile = createDFUFile(tgtFileName, clusterName, dft_flat, eclRecDef, "myRequestId", 300, false, userDesc);
        IDFUFileAccessExt *tgtFileEx = srcFile->queryEngineInterface();


        unsigned tgtFileParts = tgtFile->queryNumParts();
        unsigned currentWriterPart = 0;
        Owned<IDFUFilePartWriter> writer;

        unsigned numRecs = 0;
        unsigned srcFileParts = srcFile->queryNumParts();
        unsigned tally = srcFileParts;
        for (unsigned p=0; p<srcFileParts; p++)
        {
            Owned<IDFUFilePartReader> reader = srcFile->createFilePartReader(p);
            reader->start();

            if (tally >= srcFileParts)
            {
                tally -= srcFileParts;
                writer.setown(tgtFile->createFilePartWriter(currentWriterPart++));
                writer->start();
            }
            tally += tgtFileParts;

            while (true)
            {
                size32_t sz;
                const void *row = reader->nextRow(sz);
                if (!row)
                {
                    if (!srcFile->queryIsGrouped())
                        break;
                    row = reader->nextRow(sz);
                    if (!row)
                        break;
                }
                ++numRecs;
                CommonXmlWriter xmlwrite(0);
                srcMeta->toXML((const byte *)row, xmlwrite);
                PROGLOG("row: %s", xmlwrite.str());

                writer->write(sz, row);
            }

        }
        writer.clear();

        // write some blank parts, if src # parts less than target # parts
        while (currentWriterPart<tgtFileParts)
        {
            writer.setown(tgtFile->createFilePartWriter(currentWriterPart++));
            writer->start();
            writer.clear();
        }
        PROGLOG("numRecs writtern = %u", numRecs);
        tgtFileEx->queryProperties().setPropInt64("@recordCount", numRecs);
        //tgtFileEx->queryProperties().setPropInt64("@size", fileSize);
        publishDFUFile(tgtFile, true, userDesc);

        // read it back for good measure
        Owned<IDFUFileAccess> newSrcFile = lookupDFUFile(tgtFileName, "datest", 60, userDesc);
        if (!newSrcFile)
        {
            WARNLOG("File '%s' not found", tgtFileName.str());
            return;
        }
        IOutputMetaData *tgtMeta = tgtFileEx->queryMeta();
        CommonXmlWriter xmlWriter(0);
        for (unsigned p=0; p<tgtFileParts; p++)
        {
            Owned<IDFUFilePartReader> reader = newSrcFile->createFilePartReader(p);
            reader->start();

            while (true)
            {
                size32_t sz;
                const void *row = reader->nextRow(sz);
                if (!row)
                    break;
                ++numRecs;
                tgtMeta->toXML((const byte *)row, xmlWriter);
                PROGLOG("new file row: %s", xmlWriter.str());
            }
        }
    }
    catch (IException *e)
    {
        EXCLOG(e, nullptr);
        e->Release();
    }
}


class CClientTestSDS : public Thread
{
public:
    CClientTestSDS() : Thread("ClientTestSDS"){ }

    virtual int run()
    {
        try
        {
            TestSDS1();
        }
        catch (IException *e)
        {
            pexception("CClientTestSDS", e);
            e->Release();
        }
        return 0;
    }

};

void TestSDS2()
{
    CClientTestSDS *t1 = new CClientTestSDS();
    CClientTestSDS *t2 = new CClientTestSDS();

    t1->start(false);
    t2->start(false);

    t1->join();
    t2->join();
    t1->Release();
    t2->Release();
}


struct SDS3Params
{
    ReadWriteLock *reinitLock;
    IGroup *group;
};
class TestSDS3TestThread : public CInterface, implements IPooledThread
{
    CThreaded threaded;
    Owned<IRemoteConnection> conn;
    StringAttr xpath;
    unsigned mode, action;
    ReadWriteLock *reinitLock;
    IGroup *group;

public:
    IMPLEMENT_IINTERFACE;

    TestSDS3TestThread() : threaded("TestSDS3TestThread") { }
    
    virtual void init(void *param) override
    {
        SDS3Params *params = (SDS3Params *)param;
        reinitLock = params->reinitLock;
        group = params->group;
    }
    virtual bool stop() override { return true; }
    virtual bool canReuse() const override { return true; }
    virtual void threadmain() override
    {
        action = getRandom() % 8;
        mode = getRandom() % 2;

        if (4 == action || 5 == action)
            xpath.set("/aparent");
        else
            xpath.set("/aparent/achild");
        if (3 == action || 1 == action)
            mode = 0;

        if (7 == action)
        {
            Sleep(1000);
            WriteLockBlock b(*reinitLock);
            PROGLOG("shutdown / reinit test");
            reinitClientProcess(group, DCR_Testing);
        }
        else
        {
            unsigned times = getRandom() % 20 + 10;
            while (times--)
            {
                MilliSleep(getRandom() %100 + 100);
                ReadLockBlock b(*reinitLock);
                conn.setown(querySDS().connect(xpath.get(), myProcessSession(), mode, 10000*MDELAY));
                if (!conn)
                {
                    for (;;)
                    {
                        PROGLOG("creating initial branch");
                        conn.setown(querySDS().connect(xpath.get(), myProcessSession(), RTM_CREATE| RTM_LOCK_WRITE, 10000*MDELAY));
                        if (6 == action || 5==action)
                        {
                            Owned<IRemoteConnection> conn2 = querySDS().connect(xpath.get(), myProcessSession(), RTM_CREATE_ADD| RTM_LOCK_WRITE, 10000*MDELAY); // add some ambiguity
                        }
                        conn.clear();
                        conn.setown(querySDS().connect(xpath.get(), myProcessSession(), mode, 10000*MDELAY));
                        if (conn)
                            break;
                    }
                }
                try
                {
                    PROGLOG("xpath=%s, mode=%d, action=%d", xpath.get(), mode, action);
                    switch (action)
                    {
                        case 0:
                        {
                            conn->changeMode(0);
                            break;
                        }
                        case 2:
                        {
                            conn->close();
                            break;
                        }
                        case 1:
                        case 3:
                        {
                            conn->close(true);
                            break;
                        }
                        case 4:
                        {
                            conn->queryRoot()->removeProp("achild");
                            break;
                        }
                        default:
                            break;
                    }
                }
                catch (IException *e)
                {
                    EXCLOG(e, NULL);
                    e->Release();
                }
                conn.clear();
            }
        }
    }
};

void TestSDS3(IGroup *group)
{
    class TSDS1 : public CInterface, implements IThreadFactory
    {
    public:
        IMPLEMENT_IINTERFACE;
        virtual IPooledThread *createNew() { return new TestSDS3TestThread(); }
    } poolFactory;

    unsigned nthreads = testParams.ordinality()?atoi(testParams.item(0)):10;
    ReadWriteLock reinitLock;
    Owned<IThreadPool> pool = createThreadPool("TSDS1", &poolFactory, false, nullptr, nthreads);

    SDS3Params params;
    params.reinitLock = &reinitLock;
    params.group = group;
    for (;;)
    {
        pool->start(&params, NULL, 50000); // keep starting them as they become available
    }
    PROGLOG("Joining all TSDSThread running threads");
    pool->joinAll();
    pool.clear();
}

void TestNodeSubs()
{
    class CNodeSubPool : public CSimpleInterfaceOf<IThreadFactory>
    {
        class CNodeSubscriber : public CSimpleInterfaceOf<ISDSNodeSubscription>
        {
        public:
            virtual void notify(SubscriptionId id, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
            {
                PROGLOG("CNodeSubscriber notified");
            }
        };
        SubscriptionId sid;
        CriticalSection sidCrit;
        Owned<ISDSNodeSubscription> subscriber;

        void test()
        {
            try
            {
                unsigned t = getRandom()%5;
                switch (t)
                {
                    case 0:
                    {
                        // connect thread
                        PROGLOG("Creating SDS node");
                        Owned<IRemoteConnection> conn = querySDS().connect("/nodesubtest", myProcessSession(), RTM_CREATE|RTM_LOCK_WRITE, INFINITE);
                        MilliSleep(5+getRandom()%50);
                        conn.clear();
                        break;
                    }
                    case 1:
                    {
                        // node sub test
                        CriticalBlock b(sidCrit);
                        if (!sid)
                        {
                            PROGLOG("Subscribing to node");
                            sid = querySDS().subscribeExact("/nodesubtest", *subscriber, false);
                        }
                        break;
                    }
                    case 2:
                    {
                        // node sub test
                        CriticalBlock b(sidCrit);
                        if (sid)
                        {
                            PROGLOG("Unsubscribing to node");
                            querySDS().unsubscribeExact(sid);
                            sid = 0;
                        }
                        break;
                    }
                    case 3:
                    {
                        PROGLOG("Deleting node");
                        Owned<IRemoteConnection> conn = querySDS().connect("/nodesubtest", myProcessSession(), RTM_LOCK_WRITE, INFINITE);
                        if (conn)
                            conn->close(true);
                        break;
                    }
                    case 4:
                    {
                        PROGLOG("Gathering subscriber info");
                        StringBuffer info;
                        querySDS().getSubscribers(info);
                        if (info.length())
                            PROGLOG("Subscribers: \n%s", info.str());
                        break;
                    }
                }
            }
            catch (IException *e)
            {
                PrintExceptionLog(e, NULL);
                e->Release();
            }
        }
        class CNodeSubThread : public CInterface, implements IPooledThread
        {
            CNodeSubPool &owner;
        public:
            IMPLEMENT_IINTERFACE;

            CNodeSubThread(CNodeSubPool &_owner) : owner(_owner) { }
            virtual void init(void *param) override
            {
            }
            virtual void threadmain() override
            {
                owner.test();
            }
            virtual bool stop() override { return true; }
            virtual bool canReuse() const override { return true; }
        };
    public:
        CNodeSubPool()
        {
            sid = 0;
            subscriber.setown(new CNodeSubscriber());
        }
        virtual IPooledThread *createNew()
        {
            return new CNodeSubThread(*this);
        }
    } poolFactory;

    Owned<IThreadPool> pool = createThreadPool("TSDSTest", &poolFactory, false, nullptr, 100, 100000);

    unsigned tests = testParams.ordinality() ? atoi(testParams.item(0)) : 10;
    for (unsigned t=0; t<tests; t++)
    {
        pool->start(NULL);
    }

    PROGLOG("Joining all TSDSThread running threads");
    pool->joinAll();
    pool.clear();

}

void TestSDSXPaths()
{
    const char *testXML =

"<ROOT attrRoot=\"9\">"
" <A attrA=\"a1\" numA=\"1\">"
"  <B attrB=\"a1b1\" numB=\"1\">bval_a1b1</B>"
" </A>"
" <A attrA=\"a2\" numA=\"2\">"
"  <B attrB=\"a2b1\" numB=\"2\">bval_a2_b1"
"   <C attrC=\"a2b1c1\" numC=\"1\"></C>"
"  </B>"
"  <B attrB=\"a2b2\" numB=\"3\">bval_a2_b2"
"   <C attrC=\"a2b2c1\" numC=\"2\"></C>"
"  </B>"
"  <B attrB=\"a2b1\" numB=\"4\">bval_a2_b3</B>"
" </A>"
" <A numA=\"3\">"
" </A>"
" <A2 numA=\"1\">"
"  <B attrB=\"a2b1\">bval_a21_b1"
"   <C>"
"    <B>bval_a21_b1_c1_b1"
"     <C></C>"
"    </B>"
"   </C>"
"  </B>"
" </A2>"
" <A3 numA=\"1\"></A3>"
"</ROOT>";


    const char *xpathTests[] = {

"A",
"A[B]",
"A[@attrA]",
"*[@numA]",
"A/B[@attrB]/C",
"A[@attrA = \"a1\"]",
"A[@attrA = \"a1*\"]",
"A/B[@attrB=\"a2*\"]",
"A[@attrA = ~\"a1*\"]",
"A[B=\"bval2\"]",

"//B[C]",
"A//B[C]",

"A[@attrA][B=\"bval2\"]",
"A/B[@numB < \"2\"]",
"A/B[@numB <= \"2\"]",
"A/B[@numB = \"2\"]",
"A/B[@numB > \"2\"]",
"A/B[@numB >= \"2\"]",

"A/B[@attrB >> \"a1b1\"]",
NULL
    };


    class CSplitIFileIO : public CInterface, implements IFileIO
    {
        IArrayOf<IFileIO> iFileIOs;
    public:
        IMPLEMENT_IINTERFACE;

        CSplitIFileIO() { }
        void addIFileIO(IFileIO *iFileIO) { iFileIOs.append(*iFileIO); }
    // IFileIO
        virtual size32_t read(offset_t pos, size32_t len, void * data) { UNIMPLEMENTED; return 0; }
        virtual offset_t size() { UNIMPLEMENTED; return 0; }
        virtual size32_t write(offset_t pos, size32_t len, const void * data)
        {
            size32_t sz = iFileIOs.item(0).write(pos, len, data);
            unsigned i=1;
            for (i=1; i<iFileIOs.ordinality(); i++)
                verifyex(sz == iFileIOs.item(i).write(pos, len, data));
            return sz;
        }
        virtual unsigned __int64 getStatistic(StatisticKind kind) { return 0; }
        virtual offset_t appendFile(IFile *file,offset_t pos=0,offset_t len=-1) { UNIMPLEMENTED; return 0; }
        virtual void setSize(offset_t size) { UNIMPLEMENTED; }
        virtual void flush() { }
        virtual void close() { }
    };

    const char *newFileName = "xpathTests.out";
    OwnedIFile newFile;
    OwnedIFileIO newFileIO;

    HANDLE out;
#ifdef WIN32
    out = GetStdHandle(STD_OUTPUT_HANDLE);
#else
    out = fileno(stdout);
#endif
    Owned<IFileIO> stdOutFileIO = createIFileIO(nullptr,out,IFOwrite);
    if (testParams.ordinality())
    {
        newFileName = testParams.item(0);
        newFile.setown(createIFile(newFileName));
        newFileIO.setown(newFile->open(IFOcreate));
    }
    else
    {
        newFile.setown(createIFile(newFileName));
        CSplitIFileIO *split= new CSplitIFileIO();
        newFileIO.setown(split);
        split->addIFileIO(newFile->open(IFOcreate));
        split->addIFileIO(LINK(stdOutFileIO));
    }

    OwnedIFile newFileSecondary = createIFile("xpathTestsSecondary.out");
    Owned<IIOStream> newFileIOStream;

    Owned<IPropertyTree> originalTree = createPTreeFromXMLString(testXML);
    Owned<IPropertyTree> tree;
    unsigned l;
    MemoryBuffer newOutput, secondary;
    for (l=0; l<2; l++)
    {
        newFileIOStream.clear();
        if (0 == l)
        {
            newFileIOStream.setown(createIOStream(newFileIO));
            newFileIO.clear();
            tree.set(originalTree);
        }
        else
        {
            OwnedIFileIO newFileSecondaryIO = newFileSecondary->open(IFOcreate);
            newFileIOStream.setown(createBufferedIOStream(newFileSecondaryIO));
            newFileSecondaryIO.clear();

            Owned<IRemoteConnection> conn = querySDS().connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
            conn->queryRoot()->setPropTree("ROOT", LINK(originalTree));
            tree.setown(createPTreeFromIPT(conn->queryRoot()->queryPropTree("ROOT")));
        }
        unsigned test = 0;
        while (xpathTests[test] != NULL)
        {
            try
            {
                Owned<IPropertyTreeIterator> iter;
                Owned<IRemoteConnection> conn;
                iter.setown(tree->getElements(xpathTests[test]));
                if (1 == l)
                {
                    unsigned count = 0;
                    ForEach (*iter)
                        ++count;
                    if (count > 1)
                    {
                        // PROGLOG("SDS connection made to root to avoid connection ambiguity for test: %s", xpathTests[test]);
                        conn.setown(querySDS().connect("/ROOT", myProcessSession(), RTM_LOCK_READ, 2000*MDELAY));
                        iter.setown(conn->getElements(xpathTests[test]));
                    }
                    else
                    {
                        StringBuffer path("/ROOT/");
                        path.append(xpathTests[test]);
                        conn.setown(querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_READ, 2000*MDELAY));
                        if (conn)
                            iter.setown(conn->queryRoot()->getElements(NULL));
                        else
                            iter.setown(createNullPTreeIterator());
                    }
                }
                unsigned count = 0;
                StringBuffer outMsg("Test = ");
                writeStringToStream(*newFileIOStream, outMsg.append(xpathTests[test]).newline().str());
                ForEach (*iter)
                {
                    IPropertyTree &match = iter->query();
                    StringBuffer out("Matched node = ");
                    out.append(match.queryName());
                    const char *value = match.queryProp(NULL);
                    if (value)
                        out.append(", value = ").append(value);
                    writeStringToStream(*newFileIOStream, out.newline().str());
                    count++;
                }
                
                writeStringToStream(*newFileIOStream, outMsg.clear().append("Match count = ").append(count).newline().str());
            }
            catch (IException *e)
            {
                StringBuffer errMsg("Test ");
                errMsg.append(test).append(" \"").append(xpathTests[test]).append("\" failed");
                EXCLOG(e, errMsg.str());
                e->Release();
            }
            test++;
        }

        newFileIOStream.clear();
        if (0 == l)
        {
            OwnedIFileIO newFileIO = newFile->open(IFOread);
            read(newFileIO, 0, (size32_t)newFile->size(), newOutput);
            newFileIO.clear();
        }
        else
        {
            OwnedIFileIO newFileSecondaryIO = newFileSecondary->open(IFOread);
            read(newFileSecondaryIO, 0, (size32_t)newFileSecondary->size(), secondary);
            newFileSecondaryIO.clear();

            if (newOutput.length() != secondary.length() || 0 != memcmp(newOutput.toByteArray(), secondary.toByteArray(), newOutput.length()))
                throw MakeStringException(0, "Local and SDS outputs mismatch");

            newFileSecondary->remove();
        }
    }
}

void TestLocks()
{
#if 0
    ICommunicator *comm = createCommunicator(mygroup);
    CMessageBuffer mb;
    DistributedLockId lockid;
    if (mygroup->rank()==0) {
        lockid = createDistributedLockId();
        mb.append(lockid);
        comm->send(mb,RANK_ALL,1);
    } 
    else {
        comm->recv(mb,RANK_ALL,1);
        mb.read(lockid);
    }
    IDistributedLock *dl = createDistributedLock(lockid);
    for (unsigned i=0;i<100;i++) {
        Sleep(getRandom()%3000);
        bool excl = (getRandom()%3)==0;
        PROGLOG("getting %s lock",excl?"exclusive":"non-exclusive");
        dl->lock(excl);
        PROGLOG("got %s lock",excl?"exclusive":"non-exclusive");
        Sleep(getRandom()%5000);
        dl->unlock();
        PROGLOG("release %s lock",excl?"exclusive":"non-exclusive");
    }
    dl->Release();
    comm->Release();
#endif
}


void TestServerShutdown(IGroup *group)
{
    myProcessSession();
    unsigned i=0;
    while (i<10) {
        i++;
        try {
            printf("Test 1\n"); Sleep(5000); //_getch(); 
            ISDSManager &sdsManager = querySDS();
            printf("Test 2\n"); Sleep(1000); //_getch(); 
            IRemoteConnection *conn;
            conn = sdsManager.connect("/", myProcessSession(), RTM_LOCK_WRITE, 2000*MDELAY);
            IPropertyTree *root = conn->queryRoot();
            printf("Test 3\n"); Sleep(1000); //_getch(); 
            root->setPropInt("subtest", i);
            printf("Test 4\n"); Sleep(1000); //_getch(); 
            conn->commit();
            printf("Test 5\n"); Sleep(1000); //_getch(); 
            conn->changeMode( RTM_LOCK_READ);
            printf("Test 6\n"); Sleep(1000); //_getch(); 
            root->setPropInt("subtest/test", i);
            printf("Test 7\n"); Sleep(1000); //_getch(); 
            conn->changeMode( 0);
            printf("Test 9\n"); Sleep(1000); //_getch(); 
            root->setPropInt("subtest/test", i+100);
            printf("Test 10\n"); Sleep(1000); //_getch(); 
            root->setPropInt("subtest/test", i+200);
            conn->changeMode( RTM_LOCK_WRITE);
            conn->Release();
            printf("Test 11\n"); Sleep(5000); //_getch(); 
        }
        catch (IException *e) {
            pexception("Exception",e);
        }
        reinitClientProcess(group, DCR_Testing);
    }
}

#define NCCS        40
#define NCCSTHREAD 20

static CriticalSection *CCS[NCCS];

class TestCCSThread : public Thread
{
    const char *name;
public:
    TestCCSThread(const char *_name) : Thread(_name) { name = strdup(_name); } 
    ~TestCCSThread() { free((void *)name); }
    int run()
    {
        try {
            for (;;) {
                Sleep(getRandom()%1000);
                unsigned i = getRandom()%NCCS;
                PROGLOG("%s locking %d",name,i);
                CCS[i]->enter();
                Sleep(getRandom()%1000);
                unsigned j = getRandom()%NCCS;
                PROGLOG("%s locking %d",name,j);
                CCS[j]->enter();
                if (getRandom()%2==0) {
                    unsigned t = i;
                    i = j;
                    j = t;
                }
                Sleep(getRandom()%1000);
                PROGLOG("%s unlocking %d",name,j);
                CCS[j]->leave();
                Sleep(getRandom()%1000);
                PROGLOG("%s unlocking %d",name,i);
                CCS[i]->leave();
            }
        }
        catch (IException *e) {
            pexception("Exception",e);
        }
        return 0;
    }
};

static void TestCriticalSection()
{
    char id[16];
    char num[8];
    TestCCSThread *threads[NCCSTHREAD];
    for (unsigned i=0;i<NCCSTHREAD; i++) {
        itoa(i,num,10);
        strcpy(id,"Thread");
        strcat(id,num);
        threads[i] = new TestCCSThread(id);
    }
    for (unsigned j=0;j<NCCS; j++) {
        itoa(j,num,10);
        strcpy(id,"CCS");
        strcat(id,num);
        CCS[j] = new CriticalSection();
    }
    unsigned k;
    for (k=0;k<NCCSTHREAD; k++) 
        threads[k]->start(false);
    for (k=0;k<NCCSTHREAD; k++) 
        threads[k]->join();
}

#define NMEMPTRS 523

class TestMemThread : public Thread
{
    const char *name;
    void *ptrs[NMEMPTRS];

public:
    TestMemThread(const char *_name) : Thread(_name) { name = strdup(_name); memset(ptrs, 0, NMEMPTRS * sizeof(void*));} 
    ~TestMemThread() { free((void *)name); }
    int run()
    {
        try {
            for (;;) {
                unsigned i = getRandom()%NMEMPTRS;
                if (ptrs[i])
                    free(ptrs[i]);
                if (getRandom() & 1)
                    ptrs[i] = malloc(getRandom());
            }
        }
        catch (IException *e) {
            pexception("Exception",e);
        }
        return 0;
    }
};

static void TestMemThreads()
{
    char id[16];
    char num[8];
    TestMemThread *threads[NCCSTHREAD];
    for (unsigned i=0;i<NCCSTHREAD; i++) {
        itoa(i,num,10);
        strcpy(id,"Thread");
        strcat(id,num);
        threads[i] = new TestMemThread(id);
    }
    unsigned k;
    for (k=0;k<NCCSTHREAD; k++) 
        threads[k]->start(false);
    for (k=0;k<NCCSTHREAD; k++) 
        threads[k]->join();
}

#define NUMPTRS2 0x10000
class TestMemThread2 : public Thread
{
    const char *name;
    static void *ptrs[NUMPTRS2];

public:
    TestMemThread2(const char *_name) : Thread(_name) 
    { 
        memset(ptrs, 0, NUMPTRS2 * sizeof(void*));
    } 
    int run()
    {
        try {
            unsigned i;         
            unsigned res=0;
            for (i=0;i<NUMPTRS2;i++) {
                ptrs[i] = malloc(0x10000);
                if (!ptrs[i])
                    break;
                if (i%100==0)
                    printf("%d\n",i);
                memset(ptrs[i],i,0x10000);
                res+=0x10000;
            }
            while (i) 
                free(ptrs[--i]);
            printf("allocated %x\n",res);
        }
        catch (IException *e) {
            pexception("Exception",e);
        }
        catch (...) {
            printf("unknown exception!\n");
        }
        return 0;
    }
};

void *TestMemThread2::ptrs[NUMPTRS2];


void testMultiConnect()
{
    const char *grpname = "dummy" ; // "thor18way"; // "thor_data400" ; 
    Owned<IGroup> grp = queryNamedGroupStore().lookup(grpname);
    if (!grp) {
        printf("Group %s not found\n",grpname);
        return;
    }
    SocketEndpointArray eps;
    unsigned j=0;
    for (j=0;j<grp->ordinality();j++) {
        SocketEndpoint ep = grp->queryNode(j).endpoint();
        ep.port = 5051;
        eps.append(ep);
    }
    class cNotify: implements ISocketConnectNotify
    {
        void connected(unsigned idx,const SocketEndpoint &ep,ISocket *socket)
        {
            StringBuffer epstr;
            ep.getEndpointHostText(epstr);
            printf("%s suceeded\n",epstr.str());
        }
        void failed(unsigned idx,const SocketEndpoint &ep,int err)
        {
            StringBuffer epstr;
            ep.getEndpointHostText(epstr);
            printf("%s failed (%d)\n",epstr.str(),err);
        }
    } notify;
    unsigned t = msTick();
    IPointerArrayOf<ISocket> out;
    multiConnect(eps,notify,5000);
    printf("connect took %d\n",msTick()-t);
}

void testlockprop(const char *lfn)
{
    Owned<IDistributedFile> f1 = queryDistributedFileDirectory().lookup(lfn,UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    Owned<IDistributedFile> f2 = queryDistributedFileDirectory().lookup(lfn,UNKNOWN_USER,AccessMode::tbdRead,false,false,nullptr,defaultNonPrivilegedUser);
    f1->lockProperties();
    f1->unlockProperties();
    printf("done\n");
}

void usage(const char *error=NULL)
{
    if (error) printf("%s\n", error);
    printf("usage: DATEST <server_ip:port>* [/test <name> [<test params...>] [/NITER <iterations>]\n");
    printf("where name = RANDTEST | DFS | QTEST | QTEST2 | SESSION | LOCKS | SDS1 | SDS2 | XPATHS| STRESS | STRESS2 | SHUTDOWN | EXTERNAL | SUBLOCKS | SUBSCRIPTION | CONNECTIONSUBS | MULTIFILE | NODESUBS | DFUSTREAMREAD | DFUSTREAMWRITE | DFUSTREAMCOPY\n");
    printf("eg:  datest . /test QTEST put          -- one coven server running locally, running qtest with param \"put\"\n");
    printf("     datest eq0001016 eq0001017        -- two coven servers, use default test %s\n", DEFAULT_TEST);
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
        StringBuffer lf;
        openLogFile(lf, cmd.toLowerCase().append(".log").str());

#if defined(TEST_MEMTHREADS)
        printf("start...\n");
        TestMemThread2 t("test");
        t.start(false);
        t.join();
        printf("end...\n");
        return 0;
#endif
        

// Non dali tests
#if defined(TEST_THREADS)
        TestMemThreads(); // doesn't terminate
#endif
#if defined(TEST_DEADLOCK)
        TestCriticalSection(); // doesn't terminate
#endif
#if defined(TEST_REMOTEFILE)
        TestRemoteFile(atoi(argv[1]),18);
        return 0;
#endif
#if defined(TEST_REMOTEFILE2)
        TestRemoteFile2();
        return 0;
#endif
#if defined(TEST_REMOTEFILE3)
        int nfiles = 1000;
        int fsizemb = 512;
        if(argc >= 2)
            nfiles = atoi(argv[1]);
        if(argc >= 3)
            fsizemb = atoi(argv[2]);
        TestRemoteFile3(nfiles, fsizemb);
        return 0;
#endif
#if defined(TEST_COPYFILE)
        if(argc >= 3)
            TestCopyFile(argv[1], argv[2]);
        else
            PROGLOG("TestCopyFile(src-file, dst-file) missing arguments");
        return 0;
#endif
        if (argc<2) {
            usage();
            return 1;
        }

        printf("DATEST Starting\n");
        SocketEndpoint ep;
        SocketEndpointArray epa;

        enum { unspecified, daservers, testparams } state = daservers;

        unsigned i=0;
        while (i<(unsigned)argc-1)
        {
            if (argv[++i][0]=='/')
            {
                if (0 == strcmp("?", argv[i]+1) || 0 == stricmp("HELP", argv[i]))
                {
                    usage();
                    return 1;
                }
                else if (0 == stricmp("TEST", argv[i]+1))
                {
                    state = testparams;
                    if (i==argc-1) { usage("missing test name"); return 1; }
                    whichTest = argv[++i];
                }
                else if (0 == stricmp("NITER", argv[i]+1))
                {
                    state = unspecified;
                    if (i==argc-1) { usage("missing /NITER #"); return 1; }
                    nIter = atoi(argv[++i]);
                }
                else
                {
                    usage("unrecognised option switch");
                    return 1;
                }
            }
            else
            {
                switch (state)
                {
                    case daservers:
                        ep.set(argv[i],DALI_SERVER_PORT);
                        epa.append(ep);
                        break;
                    case testparams:
                        testParams.append(argv[i]);
                        break;
                    default:
                        assertex(false);
                }
            }
        }
        if (!epa.ordinality())
        {
            usage("No dali servers specified");
            return 1;
        }

        IGroup *group = createIGroup(epa); 

        if (TEST("SESSION"))
            initClientProcess(group,DCR_Testing, testParams.ordinality() ? 0 : 7777);
        else
            initClientProcess(group, DCR_Testing);

        
        //testlockprop("test::propagated_matchrecs");



        for(unsigned iter=0;iter<nIter;iter++)
        {

            if (TEST("RANDTEST"))
            {
                switch (getRandom()%12) {
                case 0: Test_DFS(); break;
                case 1: QTest(true); break;
                case 2: QTest(false); break;
                case 3: QTest2(true); break;
                case 4: QTest2(false); break;
                case 5: TestSDS1(); break;
                case 6: TestStress(); break;
                case 7: TestSDS2(); break;
                case 8: TestServerShutdown(group); break;
                case 9: TestExternal(); break;
                case 10: TestSubLocks(); break;
                case 11: TestSDS3(group); break;
                case 12: TestNodeSubs(); break;
                }
            }
            else if (TEST("DFS"))
                Test_DFS();
            else if (TEST("SUPERFILE"))
                Test_SuperFile();
            else if (TEST("SUPERFILE2"))
                Test_SuperFile2();
            else if (TEST("MULTIFILE"))
                Test_MultiFile();
            else if (TEST("DFSU"))
                Test_DFSU();
            else if (TEST("SESSION"))
                Test_Session(testParams.ordinality()?testParams.item(0):NULL);
            else if (TEST("QTEST"))
                QTest(testParams.ordinality()&&0==stricmp(testParams.item(0),"PUT"));
            else if (TEST("QTEST2"))
                QTest2(testParams.ordinality()&&0==stricmp(testParams.item(0),"PUT"));
            else if (TEST("LOCKS"))
                TestLocks();
            else if (TEST("SDS1"))
                TestSDS1();
            else if (TEST("SDS2"))
                TestSDS2();
            else if (TEST("SDS3"))
                TestSDS3(group);
            else if (TEST("NODESUBS"))
                TestNodeSubs();
            else if (TEST("XPATHS"))
                TestSDSXPaths();
            else if (TEST("STRESS"))
                TestStress();
            else if (TEST("STRESS2"))
                TestStress2();
            else if (TEST("EXTERNAL"))
                TestExternal();
            else if (TEST("SUBLOCKS"))
                TestSubLocks();
            else if (TEST("SUBSCRIPTION"))
                testSubscription(testParams.ordinality()&&0!=atoi(testParams.item(0)), testParams.isItem(1)?atoi(testParams.item(1)):-1, testParams.isItem(2)?atoi(testParams.item(2)):-1);
            else if (TEST("CONNECTIONSUBS"))
                testConnectionSubscription();
            else if (TEST("SHUTDOWN"))
                TestServerShutdown(group);
            else if (TEST("FILEPARTS"))
                Test_PartIter();
            else if (TEST("MULTICONNECT"))
                testMultiConnect();
            else if (TEST("DFUSTREAMREAD"))
                testDfuStreamRead(testParams);
            else if (TEST("DFUSTREAMWRITE"))
                testDfuStreamWrite(testParams.ordinality() ? testParams.item(0) : nullptr);
            else if (TEST("DFUSTREAMCOPY"))
                testDfuStreamCopy(testParams.ordinality() ? testParams.item(0) : nullptr);
//          else if (TEST("DALILOG"))
//              testDaliLog(testParams.ordinality()&&0!=atoi(testParams.item(0)));
            else
            {
                usage("Unknown test");
                return 1;
            }
        }
        group->Release();
        closedownClientProcess();
    }
    catch (IException *e) {
        pexception("Exception",e);
    }
    catch (...) { if (!TEST("RANDTEST")) throw; }
    
    return 0;
}
