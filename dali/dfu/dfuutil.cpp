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

#include <platform.h>
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jstring.hpp"
#include "jmisc.hpp"
#include "jregexp.hpp"
#include "jprop.hpp"

#include "daclient.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "dfuerror.hpp"
#include "dautils.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"
#include "dfuutil.hpp"

// savemap
// superkey functions
// (logical) directory functions

#define PHYSICAL_COPY_POLL_TIME 10000 // for aborting

#ifdef _DEBUG
//#define LOG_PART_COPY
#endif

static bool physicalPartCopy(IFile *from,const char *tofile, Owned<IException> &exc, StringBuffer *tmpname)
{
    StringBuffer tmpnamestr;
    if (!tmpname)
        tmpname = &tmpnamestr;
    tmpname->append(tofile).append("__");
    size32_t l = tmpname->length();
    genUUID(*tmpname,true); // true for windows
    StringAttr uuid(tmpname->str()+l);
    tmpname->append(".tmp");
    RemoteFilename tmpfn;
    tmpfn.setRemotePath(tmpname->str());
    //unsigned lastpc;
#ifdef LOG_PART_COPY
    PROGLOG("start physicalPartCopy(%s,%s)",from->queryFilename(),tmpname->str());
#endif
    try {
        recursiveCreateDirectoryForFile(tmpname->str());
        while(!asyncCopyFileSection(
                uuid,
                from,
                tmpfn,
                (offset_t)-1, // creates file
                0,
                (offset_t)-1, // all file
                NULL,
                PHYSICAL_COPY_POLL_TIME)) {
            // Abort check TBD
        }
    }
    catch (IException *e) {
        EXCLOG(e,"SingleFileCopy: File copy error");
        if (exc)
            exc.setown(e);
        else
            e->Release();
    }
    Owned<IFile> f = createIFile(tmpfn);
    if (!exc.get()&&(tmpnamestr.length()!=0)) {
        try {
#ifdef LOG_PART_COPY
            PROGLOG("physicalPartCopy rename(%s,%s)",tmpname->str(),pathTail(tofile));
#endif
            f->rename(pathTail(tofile));
        }
        catch (IException *e) {
            EXCLOG(e,"SingleFileCopy: File rename error");
            if (exc)
                exc.setown(e);
            else
                e->Release();
        }
    }
    if (exc.get()) {
        try {
            f->remove();
        }
        catch (IException *e) {
            // ignore
            e->Release();
        }
    }
#ifdef LOG_PART_COPY
    PROGLOG("done physicalPartCopy %s",(exc.get()==NULL)?"OK":"Failed");
#endif
    return exc.get()==NULL;
}

static bool getFileInfo(RemoteFilename &fn, Owned<IFile> &f, offset_t &size,CDateTime &modtime)
{
    f.setown(createIFile(fn));
    bool isdir = false;
    bool ret = f->getInfo(isdir,size,modtime);
    if (ret&&isdir) {
        StringBuffer fs;
        fn.getRemotePath(fs);
        throw MakeStringException(-1,"%s is a directory",fs.str());
    }
    return ret;
}

class CFileCloner
{
public:
    StringAttr nameprefix;
    Owned<INode> foreigndalinode;
    Linked<IUserDescriptor> userdesc;
    Linked<IUserDescriptor> foreignuserdesc;
    StringAttr srcCluster;
    StringAttr cluster1;
    StringAttr prefix;
    Owned<IGroup> grp1;
    ClusterPartDiskMapSpec spec1;
    StringAttr cluster2;
    Owned<IGroup> grp2;
    ClusterPartDiskMapSpec spec2;
    bool overwrite;
    IDistributedFileDirectory *fdir;
    bool repeattlk;
    bool copyphysical;
    unsigned level;



    void physicalCopyFile(IFileDescriptor *srcdesc,IFileDescriptor *dstdesc)
    {

        CriticalSection crit;
        Semaphore sem(10); // parallel local
        StringArray tmpnames;
        StringArray dstnames;
        Owned<IException> exc;


        class casyncfor: public CAsyncFor
        {
            CriticalSection &crit;
            Semaphore &sem;
            IFileDescriptor *srcdesc;
            IFileDescriptor *dstdesc;
            StringArray &tmpnames;
            StringArray &dstnames;
        public:
            Owned<IException> &exc;
            casyncfor(CriticalSection &_crit,Semaphore &_sem,IFileDescriptor *_srcdesc,IFileDescriptor *_dstdesc,Owned<IException> &_exc, StringArray &_tmpnames, StringArray &_dstnames)
                : crit(_crit), sem(_sem), exc(_exc), tmpnames(_tmpnames), dstnames(_dstnames)
            {
                srcdesc = _srcdesc;
                dstdesc = _dstdesc;
            }
            void Do(unsigned pn)
            {
                CriticalBlock block(crit);
                RemoteFilename srcfn;
                Owned<IFile> srcf;
                IPartDescriptor &srcpart = *srcdesc->queryPart(pn);
                bool got = false;
                for (unsigned cpy = 0; cpy<srcpart.numCopies(); cpy++) {
                    srcpart.getFilename(cpy,srcfn);
                    try {
                        CriticalUnblock unblock(crit);
                        sem.wait();
                        offset_t srcsize;
                        CDateTime srcmodtime;
                        got = getFileInfo(srcfn,srcf,srcsize,srcmodtime);
                        sem.signal();
                    }
                    catch (IException *e) {
                        EXCLOG(e,"copyLogicalFile open source part");
                        if (exc.get())
                            e->Release();
                        else
                            exc.setown(e);
                        sem.signal();
                        return;
                    }
                    if (got)
                        break;
                }
                if (!got) {
                    ERRLOG("copyLogicalFile: part %d missing any copies",pn+1);
                    if (!exc.get())
                        exc.setown(MakeStringException(-1,"copyLogicalFile: part %d missing any copies",pn+1));
                    return;
                }

                RemoteFilename dstfn;
                dstdesc->queryPart(pn)->getFilename(0,dstfn);
                StringBuffer dstfs;
                dstfn.getRemotePath(dstfs);
                Owned<IException> ce;
                int ret;
                StringBuffer tmpname;
                {
                    CriticalUnblock unblock(crit);
                    ret = physicalPartCopy(srcf,dstfs.str(), ce, &tmpname);
                }
                if (!ret) {
                    if (ce.get()&&!exc.get())
                        exc.setown(ce.getClear());
                    return;
                }
                dstnames.append(dstfs.str());
                tmpnames.append(tmpname.str());

            }
        } afor(crit,sem,srcdesc,dstdesc,exc,tmpnames,dstnames);
        afor.For(srcdesc->numParts(),100,false,true);
        if (!exc.get()) {
            class casyncfor2: public CAsyncFor
            {
                CriticalSection &crit;
                Semaphore &sem;
                StringArray &tmpnames;
                StringArray &dstnames;
            public:
                Owned<IException> &exc;
                casyncfor2(CriticalSection &_crit,Semaphore &_sem,StringArray &_tmpnames,StringArray &_dstnames,Owned<IException> &_exc)
                    : crit(_crit), sem(_sem), tmpnames(_tmpnames), dstnames(_dstnames), exc(_exc)
                {
                }
                void Do(unsigned pn)
                {
                    CriticalBlock block(crit);
                    if (exc.get())
                        return;
                    RemoteFilename rfn;
                    rfn.setRemotePath(tmpnames.item(pn));
                    Owned<IFile> f = createIFile(rfn);
                    try {
                        CriticalUnblock unblock(crit);
                        sem.wait();
#ifdef LOG_PART_COPY
                        PROGLOG("physicalCopyFile rename(%s,%s)",tmpnames.item(pn),dstnames.item(pn));
#endif
                        f->rename(pathTail(dstnames.item(pn)));
                        sem.signal();
                    }
                    catch (IException *e) {
                        EXCLOG(e,"physicalCopyFile: File rename error");
                        if (!exc.get())
                            exc.setown(e);
                        else
                            e->Release();
                        sem.signal();
                        return;
                    }
                    tmpnames.replace(dstnames.item(pn),pn); // so as will get cleared up
                }
            } afor2(crit,sem,tmpnames,dstnames,exc);
            afor2.For(srcdesc->numParts(),10,false,true);
        }
        if (exc.get()) {
            ForEachItemIn(i2,tmpnames) {
                RemoteFilename rfn;
                rfn.setRemotePath(tmpnames.item(i2));
                Owned<IFile> f = createIFile(rfn);
                try {
                    f->remove();
                }
                catch (IException *e) {
                    // ignore
                    e->Release();
                }
            }
            throw exc.getClear();
        }
    }

    void physicalReplicateFile(IFileDescriptor *desc,const char *filename)  // name already has prefix added
    {

        // now replicate
        CriticalSection crit;
        Semaphore sem(10); // parallel local
        class casyncfor2: public CAsyncFor
        {
            CriticalSection &crit;
            Semaphore &sem;
            IFileDescriptor *desc;
            const char * filename;
        public:
            Owned<IException> exc;
            casyncfor2(CriticalSection &_crit,Semaphore &_sem,IFileDescriptor *_desc, const char *_filename)
                : crit(_crit), sem(_sem)
            {
                desc = _desc;
                filename = _filename;

            }
            void Do(unsigned pn)
            {
                CriticalBlock block(crit);
                if (exc)
                    return;
                // first find part that exists
                RemoteFilename srcfn;
                Owned<IFile> srcf;
                IPartDescriptor &part = *desc->queryPart(pn);
                unsigned nc = part.numCopies();
                unsigned cpy = 0;
                offset_t srcsize;
                CDateTime srcmodtime;
                for (; cpy<nc; cpy++) {
                    part.getFilename(cpy,srcfn);
                    bool got= false;
                    try {
                        CriticalUnblock unblock(crit);
                        sem.wait();
                        got = getFileInfo(srcfn,srcf,srcsize,srcmodtime);
                        sem.signal();
                    }

                    catch (IException *e) {
                        EXCLOG(e,"replicateLogicalFile open source");
                        if (exc.get())
                            e->Release();
                        else
                            exc.setown(e);
                        sem.signal();
                    }
                    if (got)
                        break;
                }
                if (cpy>=nc) {
                    ERRLOG("replicateLogicalFile: %s part %d missing any copies",filename,pn+1);
                    if (!exc.get())
                        exc.setown(MakeStringException(-1,"replicateLogicalFile: %s part %d missing any copies",filename,pn+1));
                    return;
                }
                for (unsigned dstcpy = 0; dstcpy<part.numCopies(); dstcpy++)
                    if (dstcpy!=cpy) {
                        RemoteFilename dstfn;
                        part.getFilename(dstcpy,dstfn);
                        Owned<IFile> dstf;
                        bool got = false;
                        try {
                            CriticalUnblock unblock(crit);
                            sem.wait();
                            offset_t dstsize;
                            CDateTime dstmodtime;
                            got = getFileInfo(dstfn,dstf,dstsize,dstmodtime);
                            // check size/date TBD
                            sem.signal();
                            if (got&&(srcsize==dstsize)&&srcmodtime.equals(dstmodtime)) 
                                continue;
                        }

                        catch (IException *e) {
                            EXCLOG(e,"cloneLogicalFile open dest");
                            if (exc.get())
                                e->Release();
                            else
                                exc.setown(e);
                            sem.signal();
                            return;
                        }
                        StringBuffer dstfs;
                        dstfn.getRemotePath(dstfs);
                        Owned<IException> ce;
                        bool ok;
                        {
                            CriticalUnblock unblock(crit);
                            ok = physicalPartCopy(srcf,dstfs.str(), ce, NULL);
                        }
                        if (!ok) {
                            if (ce.get()&&!exc.get())
                                exc.setown(ce.getClear());
                            return;
                        }
                    }



            }

        } afor2(crit,sem,desc,filename);
        afor2.For(desc->numParts(),100,false,true);
        if (afor2.exc.get())
            throw afor2.exc.getClear();
    }

    void cloneSubFile(IPropertyTree *ftree,const char *destfilename, INode *srcdali, const char *srcCluster)   // name already has prefix added
    {
        Owned<IFileDescriptor> srcfdesc = deserializeFileDescriptorTree(ftree, NULL, 0);
        const char * kind = srcfdesc->queryProperties().queryProp("@kind");
        bool iskey = kind&&(strcmp(kind,"key")==0);

        Owned<IFileDescriptor> dstfdesc = createFileDescriptor(srcfdesc->getProperties());
        if (!nameprefix.isEmpty())
            dstfdesc->queryProperties().setProp("@roxiePrefix", nameprefix.get());
        if (!copyphysical)
            dstfdesc->queryProperties().setPropInt("@lazy", 1);
        // calculate dest dir

        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"Logical name %s invalid",destfilename);

        ClusterPartDiskMapSpec spec = spec1;
        if (iskey&&repeattlk)
            spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
        StringBuffer dstpartmask;
        getPartMask(dstpartmask,destfilename,srcfdesc->numParts());
        dstfdesc->setPartMask(dstpartmask.str());
        unsigned np = srcfdesc->numParts();
        dstfdesc->setNumParts(srcfdesc->numParts());
        DFD_OS os = (getPathSepChar(srcfdesc->queryDefaultDir())=='\\')?DFD_OSwindows:DFD_OSunix;
        StringBuffer dir;
        StringBuffer dstdir;
        makePhysicalPartName(dstlfn.get(),0,0,dstdir,false,os,spec.defaultBaseDir.get());
        dstfdesc->setDefaultDir(dstdir.str());
        dstfdesc->addCluster(cluster1,grp1,spec);
        if (iskey&&!cluster2.isEmpty())
            dstfdesc->addCluster(cluster2,grp2,spec2);

        for (unsigned pn=0;pn<srcfdesc->numParts();pn++) {
            offset_t sz = srcfdesc->queryPart(pn)->queryProperties().getPropInt64("@size",-1);
            if (sz!=(offset_t)-1)
                dstfdesc->queryPart(pn)->queryProperties().setPropInt64("@size",sz);
            StringBuffer dates;
            if (srcfdesc->queryPart(pn)->queryProperties().getProp("@modified",dates))
                dstfdesc->queryPart(pn)->queryProperties().setProp("@modified",dates.str());
        }

        if (copyphysical) {
            physicalCopyFile(srcfdesc,dstfdesc);
            physicalReplicateFile(dstfdesc,destfilename);
        }

        if (!srcdali || srcdali->endpoint().isNull())
            dstfdesc->queryProperties().setProp("@cloneFromPeerCluster", srcCluster);
        else
        {
            StringBuffer s;
            dstfdesc->queryProperties().setProp("@cloneFrom", srcdali->endpoint().getUrlStr(s).str());
            dstfdesc->queryProperties().setProp("@cloneFromDir", srcfdesc->queryDefaultDir());
            if (srcCluster && *srcCluster) //where to copy from has been explicity set to a remote location, don't copy from local sources
                dstfdesc->queryProperties().setProp("@cloneFromPeerCluster", "-");
            if (prefix.length())
                dstfdesc->queryProperties().setProp("@cloneFromPrefix", prefix.get());
            unsigned numClusters = srcfdesc->numClusters();
            for (unsigned clusterNum = 0; clusterNum < numClusters; clusterNum++)
            {
                StringBuffer sourceGroup;
                srcfdesc->getClusterGroupName(clusterNum, sourceGroup, NULL);
                if (srcCluster && *srcCluster && !streq(sourceGroup, srcCluster))
                    continue;
                Owned<IPropertyTree> groupInfo = createPTree("cloneFromGroup");
                groupInfo->setProp("@groupName", sourceGroup);
                ClusterPartDiskMapSpec &spec = srcfdesc->queryPartDiskMapping(clusterNum);
                spec.toProp(groupInfo);
                dstfdesc->queryProperties().addPropTree("cloneFromGroup", groupInfo.getClear());
            }
        }

        Owned<IDistributedFile> dstfile = fdir->createNew(dstfdesc);
        dstfile->attach(destfilename,userdesc);
    }

    void extendSubFile(IPropertyTree *ftree,const char *destfilename)
    {
        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"Logical name %s invalid",destfilename);
        Owned<IDistributedFile> dfile = fdir->lookup(dstlfn,userdesc,true);
        if (dfile) {
            ClusterPartDiskMapSpec spec = spec1;
            const char * kind = ftree->queryProp("Attr/@kind");
            bool iskey = kind&&(strcmp(kind,"key")==0);
            if (iskey&&repeattlk)
                spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
            dfile->addCluster(cluster1,spec);
            if (iskey&&!cluster2.isEmpty())
                dfile->addCluster(cluster2,spec2);
            if (copyphysical) {
                Owned<IFileDescriptor> fdesc=dfile->getFileDescriptor();
                physicalReplicateFile(fdesc,destfilename);
            }
        }
    }

public:
    void init(  const char *_cluster1,
                DFUclusterPartDiskMapping _clustmap,
                bool _repeattlk,
                const char *_cluster2,
                IUserDescriptor *_userdesc,
                const char *_foreigndali,
                IUserDescriptor *_foreignuserdesc,
                const char *_nameprefix,
                bool _overwrite,
                bool _copyphysical
            )
    {
        if (_userdesc)
            userdesc.set(_userdesc);
        if (_foreignuserdesc)
            foreignuserdesc.set(_foreignuserdesc);
        else if (_userdesc)
            foreignuserdesc.set(_userdesc);
        overwrite = _overwrite;
        copyphysical = _copyphysical;
        nameprefix.set(_nameprefix);
        repeattlk = _repeattlk;
        cluster1.set(_cluster1);
        level = 0;
        if (_foreigndali&&*_foreigndali)
            foreigndalinode.setown(createINode(_foreigndali,DALI_SERVER_PORT));
        fdir = &queryDistributedFileDirectory();
        switch(_clustmap) {
        case DFUcpdm_c_replicated_by_d:
            spec1.defaultCopies = DFD_DefaultCopies;
            break;
        case DFUcpdm_c_only:
            spec1.defaultCopies = DFD_NoCopies;
            break;
        case DFUcpdm_d_only:
            spec1.defaultCopies = DFD_NoCopies;
            spec1.startDrv = 1;
            break;
        case DFUcpdm_c_then_d:
            spec1.defaultCopies = DFD_NoCopies;
            spec1.flags = CPDMSF_wrapToNextDrv;
            break;
        }
        StringBuffer defdir1;
        GroupType groupType;
        grp1.setown(queryNamedGroupStore().lookup(_cluster1, defdir1, groupType));
        if (!grp1)
            throw MakeStringException(-1,"Cannot find cluster %s",_cluster1);
        if (defdir1.length())
            spec1.setDefaultBaseDir(defdir1.str());
        if (_cluster2&&*_cluster2) {
            spec2 = spec1;
            cluster2.set(_cluster2);
            StringBuffer defdir2;
            grp2.setown(queryNamedGroupStore().lookup(_cluster2, defdir2, groupType));
            if (!grp2)
                throw MakeStringException(-1,"Cannot find cluster %s",_cluster2);
            spec2.setRepeatedCopies(CPDMSRP_lastRepeated,true); // only TLK on cluster2
            if (defdir2.length())
                spec2.setDefaultBaseDir(defdir2.str());
        }
    }

    void cloneSuperFile(const char *filename, CDfsLogicalFileName &dlfn)
    {
        level++;
        Linked<INode> srcdali = foreigndalinode;
        CDfsLogicalFileName slfn;
        slfn.set(filename);
        if (slfn.isForeign()) { // trying to confuse me
            SocketEndpoint ep;
            slfn.getEp(ep);
            slfn.clearForeign();
            srcdali.setown(createINode(ep));
        }
        Owned<IPropertyTree> ftree = fdir->getFileTree(slfn.get(),foreignuserdesc,srcdali, FOREIGN_DALI_TIMEOUT, false);
        if (!ftree.get()) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }

        const char *dstlfn = slfn.get();
        StringBuffer dstname;
        if (!nameprefix.isEmpty()) {
            dstname.append(nameprefix).append("::");
            const char *oldprefix = ftree->queryProp("Attr/@roxiePrefix");
            if (oldprefix&&*oldprefix) {
                size32_t l = strlen(oldprefix);
                if ((l+2<strlen(dstlfn))&&
                    (memicmp(oldprefix,dstlfn,l)==0)&&
                    (dstlfn[l]==':') && (dstlfn[l+1]==':'))
                    dstlfn += l+2;
            }
        }
        dstlfn = dstname.append(dstlfn).str();
        dlfn.set(dstname.str());
        if (!srcdali.get()||queryCoven().inCoven(srcdali)) {
            // if dali is local and filenames same
            if (strcmp(slfn.get(),dlfn.get())==0) {
                if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))==0) {
                    extendSubFile(ftree,dlfn.get());
                }
                else if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
                    Owned<IPropertyTreeIterator> piter = ftree->getElements("SubFile");
                    ForEach(*piter) {
                        CDfsLogicalFileName dlfnres;
                        cloneSuperFile(piter->query().queryProp("@name"),dlfnres);
                    }
                }
                else
                    throw MakeStringException(-1,"Cannot clone %s to itself",dlfn.get());
            }
            level--;
            return;
        }

        // first see if target exists (and remove if does and overwrite specified)
        Owned<IDistributedFile> dfile = fdir->lookup(dlfn,userdesc,true);
        if (dfile) {
            if (!overwrite)
                throw MakeStringException(-1,"Destination file %s already exists",dlfn.get());
            dfile->detach();
            dfile.clear();
        }
        if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))==0) {
            cloneSubFile(ftree,dlfn.get(), srcdali, srcCluster);
        }
        else if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0) {
            StringArray subfiles;
            Owned<IPropertyTreeIterator> piter = ftree->getElements("SubFile");
            ForEach(*piter) {
                CDfsLogicalFileName dlfnres;
                cloneSuperFile(piter->query().queryProp("@name"),dlfnres);
                subfiles.append(dlfnres.get());
            }
            // now construct the superfile
            Owned<IDistributedSuperFile> sfile = fdir->createSuperFile(dlfn.get(),userdesc,true,false);
            if (!sfile)
                throw MakeStringException(-1,"SuperFile %s could not be created",dlfn.get());
            ForEachItemIn(i,subfiles) {
                sfile->addSubFile(subfiles.item(i));
            }
            if (!nameprefix.isEmpty()) {
                DistributedFilePropertyLock lock(sfile);
                lock.queryAttributes().setProp("@roxiePrefix",nameprefix.get());
            }
        }
        else {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s in Dali %s is not a file or superfile",filename,srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }
        level--;
    }


    void cloneFile(const char *filename, const char *destfilename)
    {
        level++;
        Linked<INode> srcdali = foreigndalinode;
        CDfsLogicalFileName slfn;
        slfn.set(filename);
        if (slfn.isForeign()) { // trying to confuse me
            SocketEndpoint ep;
            slfn.getEp(ep);
            slfn.clearForeign();
            srcdali.setown(createINode(ep));
        }
        Owned<IPropertyTree> ftree = fdir->getFileTree(slfn.get(), foreignuserdesc, srcdali, FOREIGN_DALI_TIMEOUT, false);
        if (!ftree.get()) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }
        CDfsLogicalFileName dlfn;
        dlfn.set(destfilename);
        if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))!=0) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s in Dali %s is not a simple file",filename,srcdali?srcdali->endpoint().getUrlStr(s).str():"(local)");
        }
        if (!srcdali.get()||queryCoven().inCoven(srcdali)) {
            // if dali is local and filenames same
            if (strcmp(slfn.get(),dlfn.get())==0) {
                extendSubFile(ftree,dlfn.get());
                level--;
                return;
            }
        }

        // first see if target exists (and remove if does and overwrite specified)
        Owned<IDistributedFile> dfile = fdir->lookup(dlfn,userdesc,true);
        if (dfile) {
            if (!overwrite)
                throw MakeStringException(-1,"Destination file %s already exists",dlfn.get());
            dfile->detach();
            dfile.clear();
        }
        cloneSubFile(ftree,dlfn.get(),srcdali, srcCluster);
        level--;
    }


};


class CDFUhelper: public CInterface, implements IDFUhelper
{
public:
    IMPLEMENT_IINTERFACE;

    void addSuper(const char *superfname, IUserDescriptor *user, unsigned numtoadd, const char **subfiles, const char *before, bool autocreatesuper)
    {
        if (!numtoadd)
            throwError(DFUERR_DNoSubfileToAddToSuperFile);

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);
        transaction->start();

        Owned<IDistributedSuperFile> superfile = transaction->lookupSuperFile(superfname);
        if (!superfile)
        {
            if (!autocreatesuper)
                throwError1(DFUERR_DSuperFileNotFound, superfname);
            superfile.setown(queryDistributedFileDirectory().createSuperFile(superfname,user,true,false,transaction));
        }

        for (unsigned i=0;i<numtoadd;i++)
        {
            if (superfile->querySubFileNamed(subfiles[i]))
                throwError1(DFUERR_DSuperFileContainsSub, subfiles[i]);

            if (before&&*before)
                superfile->addSubFile(subfiles[i],true,(stricmp(before,"*")==0)?NULL:before,false,transaction);
            else
                superfile->addSubFile(subfiles[i],false,NULL,false,transaction);
        }
        transaction->commit();
    }


    void removeSuper(const char *superfname, IUserDescriptor *user, unsigned numtodelete, const char **subfiles, bool delsub, bool removesuperfile)
    {
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);
        // We need this here, since caching only happens with active transactions
        // MORE - abstract this with DFSAccess, or at least enable caching with a flag
        Owned<IDistributedSuperFile> superfile = transaction->lookupSuperFile(superfname);

        if (!superfile)
            throwError1(DFUERR_DSuperFileNotFound, superfname);
        StringAttrArray toremove;
        // Delete All
        if (numtodelete == 0) {
            if (delsub) {
                Owned<IDistributedFileIterator> iter=superfile->getSubFileIterator();
                ForEach(*iter) {
                    StringBuffer name;
                    iter->getName(name);
                    toremove.append(*new StringAttrItem(name.str()));
                }
            }
        }
        // Delete Some / Wildcard
        // MORE - shouldn't we allow wildcard to delete all matching, without the need to specify numtodelete?
        else {
            for (unsigned i1=0;i1<numtodelete;i1++)
                if (strchr(subfiles[i1],'?')||strchr(subfiles[i1],'*')) {
                    Owned<IDistributedFileIterator> iter=superfile->getSubFileIterator();
                    ForEach(*iter) {
                        StringBuffer name;
                        iter->getName(name);
                        if (WildMatch(name.str(),subfiles[i1]))
                            toremove.append(*new StringAttrItem(name.str()));
                    }
                }
                else
                    if (superfile->querySubFileNamed(subfiles[i1]))
                        toremove.append(*new StringAttrItem(subfiles[i1]));
                    else
                        throwError1(DFUERR_DSuperFileDoesntContainSub, subfiles[i1]);
        }
        // Do we have something to delete?
        if (toremove.ordinality()) {
            transaction->start();
            ForEachItemIn(i2,toremove)
                superfile->removeSubFile(toremove.item(i2).text.get(),delsub,false,transaction);
            transaction->commit();
        }
        // Delete superfile if empty
        if (removesuperfile && (superfile->numSubFiles() == 0)) {
            superfile.clear();
            // MORE - add file deletion to transaction
            queryDistributedFileDirectory().removeEntry(superfname,user);
        }
    }

    void listSubFiles(const char *superfname,StringAttrArray &out,IUserDescriptor *user)
    {
        Owned<IDistributedSuperFile> superfile = queryDistributedFileDirectory().lookupSuperFile(superfname,user);
        if (!superfile)
            throwError1(DFUERR_DSuperFileNotFound, superfname);
        Owned<IDistributedFileIterator> iter=superfile->getSubFileIterator();
        ForEach(*iter) {
            StringBuffer name;
            iter->getName(name);
            out.append(*new StringAttrItem(name.str()));
        }
    }

    StringBuffer &getFileXML(const char *lfn,StringBuffer &out, IUserDescriptor *user)
    {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn,user);
        if (!file) {
            INamedGroupStore  &grpstore= queryNamedGroupStore();
            Owned<IGroup> grp = grpstore.lookup(lfn);
            if (grp) {
                out.append("<Group name=\"").append(lfn).append("\">\n");
                ForEachNodeInGroup(i,*grp) {
                    StringBuffer ip;
                    grp->getNode(i)->endpoint().getIpText(ip);
                    out.append("  <Node ip=\"").append(ip).append("\">\n");
                }
                out.append("</Group>\n");
                return out;
            }
            else
                throwError1(DFUERR_DFileNotFound, lfn);
        }
        else {
            Owned<IFileDescriptor> fdesc = file->getFileDescriptor();
            Owned<IPropertyTree> t = fdesc->getFileTree();
            toXML(t, out, true);
        }
        return out;
    }

    void addFileXML(const char *lfn,const StringBuffer &xml,IUserDescriptor *user)
    {
        Owned<IPropertyTree> t = createPTreeFromXMLString(xml);
        Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(t,&queryNamedGroupStore(),0);
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc,true);
        if (file)
            file->attach(lfn,user);
    }

    void addFileRemote(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *user,IUserDescriptor *srcuser=NULL)
    {
        SocketEndpoint daliep = srcdali;
        if (daliep.port==0)
            daliep.port= DALI_SERVER_PORT;
        Owned<INode> node = createINode(daliep);
        Owned<IFileDescriptor> fdesc = queryDistributedFileDirectory().getFileDescriptor(srclfn,srcuser,node);
        if (!fdesc) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",srclfn,daliep.getUrlStr(s).str());
        }
        Owned<IDistributedFile> file = queryDistributedFileDirectory().createNew(fdesc);
        if (file)
            file->attach(lfn,user);
    }


    void superForeignCopy(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *srcuser,IUserDescriptor *user, bool overwrite, IDfuFileCopier *copier)
    {
        SocketEndpoint daliep = srcdali;
        CDfsLogicalFileName slfn;
        slfn.set(srclfn);
        if (slfn.isForeign()) // trying to confuse me
        {
            slfn.getEp(daliep);
            slfn.clearForeign();
        }
        if (daliep.port==0)
            daliep.port= DALI_SERVER_PORT;
        Owned<INode> srcnode = createINode(daliep);
        if (queryCoven().inCoven(srcnode))
        {
            // if dali is local and filenames same
            CDfsLogicalFileName dlfn;
            dlfn.set(lfn);
            if (strcmp(slfn.get(),dlfn.get())==0)
            {
                PROGLOG("File copy of %s not done as file local",srclfn);
                return;
            }
        }
        Owned<IPropertyTree> ftree = queryDistributedFileDirectory().getFileTree(srclfn,srcuser,srcnode, FOREIGN_DALI_TIMEOUT, false);
        if (!ftree.get())
        {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",srclfn,daliep.getUrlStr(s).str());
        }
        // first see if target exists (and remove if does and overwrite specified)
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(lfn,user,true);
        if (dfile)
        {
            if (!overwrite)
                throw MakeStringException(-1,"Destination file %s already exists",lfn);
            if (!dfile->querySuperFile())
            {
                if (ftree->hasProp("Attr/@fileCrc")&&ftree->getPropInt64("Attr/@size")&&
                    ((unsigned)ftree->getPropInt64("Attr/@fileCrc")==(unsigned)dfile->queryAttributes().getPropInt64("@fileCrc"))&&
                    (ftree->getPropInt64("Attr/@size")==dfile->getFileSize(false,false)))
                {
                    PROGLOG("File copy of %s not done as file unchanged",srclfn);
                    return;
                }
            }
            dfile->detach();
            dfile.clear();
        }
        if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))==0)
        {
            assertex(copier);
            if (!copier->copyFile(lfn,daliep,srclfn,srcuser,user))
                throw MakeStringException(-1,"File %s could not be copied",lfn);

        }
        else if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_SuperFile))==0)
        {
            StringArray subfiles;
            Owned<IPropertyTreeIterator> piter = ftree->getElements("SubFile");
            ForEach(*piter)
            {
                const char *name = piter->query().queryProp("@name");
                CDfsLogicalFileName dlfn;
                dlfn.set(name);
                superForeignCopy(dlfn.get(true),daliep,name,srcuser,user,overwrite,copier);
                subfiles.append(dlfn.get(true));
            }
            if (!copier->wait())    // should throw exeption if failure
                return;

            // now construct the superfile
            Owned<IDistributedSuperFile> sfile = queryDistributedFileDirectory().createSuperFile(lfn,user,true,false);
            if (!sfile)
                throw MakeStringException(-1,"SuperFile %s could not be created",lfn);
            ForEachItemIn(i,subfiles)
                sfile->addSubFile(subfiles.item(i));
        }
        else
        {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s in Dali %s is not a file or superfile",srclfn,daliep.getUrlStr(s).str());
        }
    }


    void createFileClone(const char *srcname,               // src LFN (can be foreign)
                         const char *cluster1,              // group name of roxie cluster
                         DFUclusterPartDiskMapping clustmap, // how the nodes are mapped
                         bool repeattlk,                    // repeat last part on all nodes if key
                         const char *cluster2,              // servers cluster (for just tlk)
                         IUserDescriptor *userdesc,         // user desc for local dali
                         const char *foreigndali,           // can be omitted if srcname foreign or local
                         IUserDescriptor *foreignuserdesc,  // user desc for foreign dali if needed
                         const char *nameprefix,            // prefix for new name
                         bool overwrite,                    // delete destination if exists
                         bool dophysicalcopy                // NB *not* using DFU server
                         )
    {
        CFileCloner cloner;
        cloner.init(cluster1,clustmap,repeattlk,cluster2,userdesc,foreigndali,foreignuserdesc,nameprefix,overwrite,dophysicalcopy);
        CDfsLogicalFileName dlfn;
        cloner.cloneSuperFile(srcname,dlfn);
    }

    void createSingleFileClone(const char *srcname,         // src LFN (can't be super)
                         const char *srcCluster,            // optional specific cluster to copy data from
                         const char *dstname,               // dst LFN
                         const char *cluster1,              // group name of roxie cluster
                         const char *prefix,
                         DFUclusterPartDiskMapping clustmap, // how the nodes are mapped
                         bool repeattlk,                    // repeat last part on all nodes if key
                         const char *cluster2,              // servers cluster (for just tlk)
                         IUserDescriptor *userdesc,         // user desc for local dali
                         const char *foreigndali,           // can be omitted if srcname foreign or local
                         IUserDescriptor *foreignuserdesc,  // user desc for foreign dali if needed
                         bool overwrite,                    // delete destination if exists
                         bool dophysicalcopy                // NB *not* using DFU server
                         )
    {
        CFileCloner cloner;
        cloner.init(cluster1,clustmap,repeattlk,cluster2,userdesc,foreigndali,foreignuserdesc,NULL,overwrite,dophysicalcopy);
        cloner.srcCluster.set(srcCluster);
        cloner.prefix.set(prefix);
        cloner.cloneFile(srcname,dstname);
    }

    void cloneFileRelationships(
        const char *foreigndali,     // where src relationships are retrieved from (can be NULL for local)
        StringArray &srcfns,             // file names on source
        StringArray &dstfns,             // corresponding filenames on dest (must exist)
        IPropertyTree *relationships,    // if not NULL, tree will have all relationships filled in
        IUserDescriptor *user
    )
    {
        // not a quick routine! (n^2)
        unsigned n = srcfns.ordinality();
        if (n!=dstfns.ordinality())
            throw MakeStringException(-1,"cloneFileRelationships - src and destination arrays not same size");
        // first find which of dstfns exist
        MemoryAttr ma;
        bool *ex = (bool *)ma.allocate(dstfns.ordinality()*sizeof(bool));
        unsigned i;
        for (i=0;i<n;i++)
            ex[i] = queryDistributedFileDirectory().exists(dstfns.item(i),user);
        for (i=0;i<n;i++) {
            if (!ex[i])
                continue;
            for (unsigned j=0;j<n;j++) {
                if (!ex[j])
                    continue;
                Owned<IFileRelationshipIterator> iter = queryDistributedFileDirectory().lookupFileRelationships(srcfns.item(i),srcfns.item(j),NULL,NULL,NULL,NULL,NULL,foreigndali);
                ForEach(*iter) {
                    try {
                        IFileRelationship &r = iter->query();
                        queryDistributedFileDirectory().addFileRelationship(dstfns.item(i),dstfns.item(j),r.queryPrimaryFields(),r.querySecondaryFields(),r.queryKind(),r.queryCardinality(),r.isPayload(),user,r.queryDescription());
                        if (relationships)
                        {
                            IPropertyTree *tree = iter->query().queryTree(); // relies on this being modifiable
                            if (tree) {
                                tree->Link();
                                tree = relationships->addPropTree("Relationship", tree);
                                tree->setProp("@primary", dstfns.item(i));
                                tree->setProp("@secondary", dstfns.item(j));
                            }
                        }
                    }
                    catch (IException *e)
                    {
                        EXCLOG(e,"cloneFileRelationships");
                        e->Release();
                    }
                }
            }
        }
    }

};


IDFUhelper *createIDFUhelper()
{
    return new CDFUhelper;
}




