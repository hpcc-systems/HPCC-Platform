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

//TBD: add + test interleave (==number of sub-files)


#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "mpbase.hpp"
#include "jptree.hpp"

#include "dadfs.hpp"
#include "dafdesc.hpp"

//#define COMPAT



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
    queryDistributedFileDirectory().removeEntry("test::testfile1");
    Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
    queryDistributedFileDirectory().removeEntry("test::testfile1");
    file->attach("test::testfile1");
    StringBuffer name;
    unsigned i;
    for (i=0;i<file->numClusters();i++)
        PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
    file.clear();
    file.setown(queryDistributedFileDirectory().lookup("test::testfile1"));
    for (i=0;i<file->numClusters();i++)
        PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
    file.clear();
    file.setown(queryDistributedFileDirectory().lookup("test::testfile1@testgrp1"));
    for (i=0;i<file->numClusters();i++)
        PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
    file.clear();
    queryDistributedFileDirectory().removePhysical("test::testfile1@testgrp2");
    file.setown(queryDistributedFileDirectory().lookup("test::testfile1"));
    for (i=0;i<file->numClusters();i++)
        PROGLOG("cluster[%d] = %s",i,file->getClusterName(i,name.clear()).str());
}




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
    checker->add("isSubFile",file->isSubFile()?1:0);
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
            printf("%s=%"I64F"d\n",s,v);
        }
        virtual void add(const char *s,const char* v)
        {
            printf("%s='%s'\n",s,v);
        }
        virtual void add(unsigned n,const char *s,__int64 v)
        {
            printf("%s[%d]=%"I64F"d\n",s,n,v);
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
            Owned<IDistributedFile> file=queryDistributedFileDirectory().lookup(fn);
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
        Owned<IDistributedFileIterator> iter = queryDistributedFileDirectory().getIterator("*",false);
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

void testSingleFile()
{
//  const char *fn = "thor_data400::prepped_for_keys";
//  const char *fn = "a0e65__w20060224-155748";
//  const char *fn = "common_did";
//  const char *fn = "test::ftest1";
//  const char *fn = "thor_data50::BASE::chunk";
//  const char *fn = "hthor::key::codes_v320040901";
//  const char *fn = "thor_data400::in::ucc_direct_ok_99999999_event_20060124";
    const char *fn = "thor400::ks_work::distancedetails";
    checkFiles(fn);
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

#ifndef COMPAT

void testDFile(ClusterPartDiskMapSpec &map)
{
    {   // 1: single part file old method
#define TN "1"
        queryDistributedFileDirectory().removeEntry("test::ftest"TN);
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        RemoteFilename rfn;
        rfn.setRemotePath("//10.150.10.1/c$/thordata/test/ftest"TN"._1_of_1");
        fdesc->setPart(0,rfn);
        fdesc->endCluster(map);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        file->attach("test::ftest"TN);
#undef TN
    }
    {   // 2: single part file new method
#define TN "2"
        queryDistributedFileDirectory().removeEntry("test::ftest"TN);
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        fdesc->setPartMask("ftest"TN"._$P$_of_$N$");
        fdesc->setNumParts(1);
        Owned<IGroup> grp = createIGroup("10.150.10.1");
        fdesc->addCluster(grp,map);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        file->attach("test::ftest"TN);
#undef TN
    }
    Owned<IGroup> grp3 = createIGroup("10.150.10.1,10.150.10.2,10.150.10.3");
    queryNamedGroupStore().add("__testgroup3__",grp3,true);
    {   // 3: three parts file old method
#define TN "3"
        queryDistributedFileDirectory().removeEntry("test::ftest"TN);
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        RemoteFilename rfn;
        rfn.setRemotePath("//10.150.10.1/c$/thordata/test/ftest"TN"._1_of_3");
        fdesc->setPart(0,rfn);
        rfn.setRemotePath("//10.150.10.2/c$/thordata/test/ftest"TN"._2_of_3");
        fdesc->setPart(1,rfn);
        rfn.setRemotePath("//10.150.10.3/c$/thordata/test/ftest"TN"._3_of_3");
        fdesc->setPart(2,rfn);
        fdesc->endCluster(map);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        file->attach("test::ftest"TN);
#undef TN
    }
    {   // 4: three part file new method
#define TN "4"
        queryDistributedFileDirectory().removeEntry("test::ftest"TN);
        Owned<IFileDescriptor> fdesc = createFileDescriptor();
        fdesc->setPartMask("ftest"TN"._$P$_of_$N$");
        fdesc->setNumParts(3);
        fdesc->addCluster(grp3,map);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        file->attach("test::ftest"TN);
#undef TN
    }
}

void testDFile() 
{
    ClusterPartDiskMapSpec map;
    testDFile(map);
}

#endif

void mt(const char *s)
{
    printf("======================================\n");
    printf("'%s'\n",s);
    StringArray array;
    RemoteMultiFilename::expand(s,array);
    StringBuffer s1;
    RemoteMultiFilename::tostr(array,s1);
    printf("'%s'\n",s1.str());
    ForEachItemIn(i1,array)
        printf("%d:'%s'\n",i1,array.item(i1));
    RemoteMultiFilename rmfn;
    rmfn.append(s);
    SocketEndpoint ep("10.150.10.80");
    rmfn.setEp(ep);
    ForEachItemIn(i2,rmfn) {
        StringBuffer tmp;
        rmfn.item(i2).getLocalPath(tmp);
        printf("%d:'%s'\n",i2,tmp.str());
    }   
    StringBuffer sd;
    StringBuffer sdt;
    StringBuffer sdr;
    StringBuffer sm;
    const char *sds = splitDirMultiTail(s,sd,sdt);
    printf("splitDirMultiTail('%s','%s')\n",sd.str(),sds);
    mergeDirMultiTail(sd.str(),sdt.str(),sm);
    printf("mergeDirMultiTail('%s')\n",sm.str());
    removeRelativeMultiPath(sm.str(),sd.str(),sdt.clear());
    printf("removeRelativeMultiPath('%s')\n",sdt.str());
}

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

void testGrp()
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
    Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
    {
        DistributedFilePropertyLock lock(file);
        lock.queryAttributes().setProp("@testing","1");
    }
    file->attach("testing::propfile2");
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

#endif



void runDafsTest()
{
#if 1
//  checkFiles(NULL);
//  testSingleFile();
//  testFiles();
//  testMultiCluster();
    testDF1();
    return; 
#endif

#if 0
    StringArray test;
    test.append("a1");
    test.append("b2");
    test.append("c3");
    test.replace("xxx",1);
#endif
#if 0
    mt("c:\\test\\x,y,z\\abc,e:\\test\\fgh,ijk,\"a, b\",eee\\fff,c:\\test\\eee\\ggg");
    mt("/c$/test/x,y,z/abc,/e$/test/fgh,ijk,\"a, b\",eee/fff,/c$/test/eee/ggg");
    mt("/c$/test/uvw/x,y,z/abc,/e$/test/fgh,ijk,\"a, b\",eee/fff,/c$/test/eee/ggg");
#endif
#if 0
    testDF1();
    testDF2();
    testMisc();
//  testGrp();
#endif
#ifndef COMPAT
    testDFile();
#endif
}

