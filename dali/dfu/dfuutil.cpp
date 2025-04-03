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

#include "ws_dfsclient.hpp"

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
                PHYSICAL_COPY_POLL_TIME,
                CFflush_rdwr)) {
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
    StringAttr remoteStorage;
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
    unsigned overwriteFlags;
    IDistributedFileDirectory *fdir;
    bool repeattlk;
    bool copyphysical;
    unsigned level;

    CFileCloner() : overwriteFlags(0) {}

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
                : crit(_crit), sem(_sem), tmpnames(_tmpnames), dstnames(_dstnames), exc(_exc)
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
                    OERRLOG("copyLogicalFile: part %d missing any copies",pn+1);
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
                    OERRLOG("replicateLogicalFile: %s part %d missing any copies",filename,pn+1);
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

    void updateCloneFrom(const char *lfn, IPropertyTree &attrs, IFileDescriptor *srcfdesc, const IPropertyTree *srcTree, INode *srcdali, const char *srcCluster)
    {
        DBGLOG("updateCloneFrom %s", lfn);
        if (remoteStorage.isEmpty() && (!srcdali || srcdali->endpoint().isNull()))
            attrs.setProp("@cloneFromPeerCluster", srcCluster);
        else
        {
            // for now, only use source file descriptor as cloned source if it's from
            // wsdfs file backed by remote storage using dafilesrv (NB: if it is '_remoteStoragePlane' will be set)
            // JCSMORE: it may be this can replace the need for the other 'clone*' attributes altogether.
            if (srcfdesc->queryProperties().hasProp("_remoteStoragePlane") && srcdali && !srcdali->endpoint().isNull())
            {
                attrs.setPropTree("cloneFromFDesc", createPTreeFromIPT(srcTree));
                StringBuffer host;
                attrs.setProp("@cloneFrom", srcdali->endpoint().getEndpointHostText(host).str());
                if (prefix.length())
                    attrs.setProp("@cloneFromPrefix", prefix.get());
                return;
            }
            else
            {
                attrs.removeProp("cloneFromFDesc");
                attrs.removeProp("@cloneFrom");
                attrs.removeProp("@cloneFromPrefix");
            }

            while(attrs.removeProp("cloneFromGroup"));
            StringBuffer s;
            if (!remoteStorage.isEmpty())
            {
                attrs.setProp("@cloneRemote", remoteStorage.str());
                if (!isEmptyString(srcCluster))
                    attrs.setProp("@cloneRemoteCluster", srcCluster);
            }
            else
            {
                attrs.setProp("@cloneFrom", srcdali->endpoint().getEndpointHostText(s).str());
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
                    attrs.addPropTree("cloneFromGroup", groupInfo.getClear());
                }
            }
            attrs.setProp("@cloneFromDir", srcfdesc->queryDefaultDir());
            if (srcCluster && *srcCluster) //where to copy from has been explicity set to a remote location, don't copy from local sources
                attrs.setProp("@cloneFromPeerCluster", "-");
            if (prefix.length())
                attrs.setProp("@cloneFromPrefix", prefix.get());
        }
    }
    void updateCloneFrom(IDistributedFile *dfile, IFileDescriptor *srcfdesc, const IPropertyTree *srcTree, INode *srcdali, const char *srcCluster)
    {
        DistributedFilePropertyLock lock(dfile);
        IPropertyTree &attrs = lock.queryAttributes();
        updateCloneFrom(dfile->queryLogicalName(), attrs, srcfdesc, srcTree, srcdali, srcCluster);
    }
    void updateCloneFrom(const char *lfn, IFileDescriptor *dstfdesc, IFileDescriptor *srcfdesc, const IPropertyTree *srcTree, INode *srcdali, const char *srcCluster)
    {
        updateCloneFrom(lfn, dstfdesc->queryProperties(), srcfdesc, srcTree, srcdali, srcCluster);
    }

    void cloneSubFile(IPropertyTree *ftree,const char *destfilename, INode *srcdali, const char *srcCluster)   // name already has prefix added
    {
        Owned<IFileDescriptor> srcfdesc = deserializeFileDescriptorTree(ftree, NULL, 0);
        const char * kind = srcfdesc->queryProperties().queryProp("@kind");
        bool iskey = kind&&(strcmp(kind,"key")==0);

        Owned<IPropertyTree> dstProps = createPTreeFromIPT(&srcfdesc->queryProperties());
        // If present, we do not want this as part of the cloned properties of this new local roxie file
        dstProps->removeProp("_remoteStoragePlane");
        Owned<IFileDescriptor> dstfdesc = createFileDescriptor(dstProps.getClear());

        if (!nameprefix.isEmpty())
            dstfdesc->queryProperties().setProp("@roxiePrefix", nameprefix.get());
        if (!copyphysical)
            dstfdesc->queryProperties().setPropInt("@lazy", 1);
        // calculate dest dir

        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"cloneSubFile: Logical name %s invalid",destfilename);

        ClusterPartDiskMapSpec spec = spec1;
        if (iskey&&repeattlk)
            spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
        StringBuffer dstpartmask;
        unsigned numParts = srcfdesc->numParts();
        getPartMask(dstpartmask, destfilename, numParts);
        dstfdesc->setPartMask(dstpartmask.str());
        dstfdesc->setNumParts(numParts);
        StringBuffer dir;
        StringBuffer dstdir;
        getLFNDirectoryUsingBaseDir(dstdir, dstlfn.get(), spec.defaultBaseDir.get());
        dstfdesc->setDefaultDir(dstdir.str());

        Owned<IStoragePlane> plane = getDataStoragePlane(cluster1, false);
        if (plane) // I think it should always exist, even in bare-metal.., but guard against it not for now (assumes initializeStoragePlanes has been called)
        {
            DBGLOG("cloneSubFile: destfilename='%s', plane='%s', dirPerPart=%s", destfilename, cluster1.get(), boolToStr(plane->queryDirPerPart()));

            FileDescriptorFlags newFlags = srcfdesc->getFlags();
            if (plane->queryDirPerPart() && (numParts > 1))
                newFlags |= FileDescriptorFlags::dirperpart;
            else
                newFlags &= ~FileDescriptorFlags::dirperpart;
            dstfdesc->setFlags(newFlags);
        }
        else
            WARNLOG("cloneSubFile: destfilename='%s', plane='%s' not found", destfilename, cluster1.get());
        dstfdesc->addCluster(cluster1,grp1,spec);
        if (iskey&&!cluster2.isEmpty())
            dstfdesc->addCluster(cluster2,grp2,spec2);

        for (unsigned pn=0; pn<numParts; pn++)
        {
            IPropertyTree &srcProps = srcfdesc->queryPart(pn)->queryProperties();
            IPropertyTree &dstProps = dstfdesc->queryPart(pn)->queryProperties();
            offset_t sz = srcProps.getPropInt64("@size",-1);
            if (sz!=(offset_t)-1)
                dstProps.setPropInt64("@size",sz);
            StringBuffer dates;
            if (srcProps.getProp("@modified",dates))
                dstProps.setProp("@modified",dates.str());
            if (srcProps.hasProp("@kind"))
                dstProps.setProp("@kind", srcProps.queryProp("@kind"));
            copyProp(dstProps, srcProps, "@uncompressedSize");
            copyProp(dstProps, srcProps, "@recordCount");
            copyProp(dstProps, srcProps, "@offsetBranches");
        }

        if (!copyphysical) //cloneFrom tells roxie where to copy from.. it's unnecessary if we already did the copy
            updateCloneFrom(destfilename, dstfdesc, srcfdesc, ftree, srcdali, srcCluster);
        else
        {
            DBGLOG("copyphysical dst=%s", destfilename);
            physicalCopyFile(srcfdesc,dstfdesc);
            physicalReplicateFile(dstfdesc,destfilename);
        }

        Owned<IDistributedFile> dstfile = fdir->createNew(dstfdesc);
        dstfile->attach(destfilename,userdesc);
    }

    void addCluster(IPropertyTree *ftree,const char *destfilename)
    {
        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"Logical name %s invalid",destfilename);
        Owned<IDistributedFile> dfile = fdir->lookup(dstlfn,userdesc,AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
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

    void extendSubFile(IPropertyTree *ftree,const char *destfilename)
    {
        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"Logical name %s invalid",destfilename);
        Owned<IDistributedFile> dfile = fdir->lookup(dstlfn,userdesc,AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
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
                const char *_remoteStorage,
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
        overwriteFlags = _overwrite ? DALI_UPDATEF_REPLACE_FILE : 0;
        copyphysical = _copyphysical;
        nameprefix.set(_nameprefix);
        repeattlk = _repeattlk;
        cluster1.set(_cluster1);
        level = 0;
        if (_foreigndali&&*_foreigndali)
            foreigndalinode.setown(createINode(_foreigndali,DALI_SERVER_PORT));
        if (_remoteStorage && *_remoteStorage)
            remoteStorage.set(_remoteStorage);
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
    inline bool checkOverwrite(unsigned flag) { return (overwriteFlags & flag)!=0; }
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
        Owned<IPropertyTree> ftree = fdir->getFileTree(slfn.get(),foreignuserdesc,srcdali, FOREIGN_DALI_TIMEOUT, GetFileTreeOpts::appendForeign);
        if (!ftree.get()) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getEndpointHostText(s).str():"(local)");
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
        Owned<IDistributedFile> dfile = fdir->lookup(dlfn,userdesc,AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
        if (dfile) {
            if (!checkOverwrite(DALI_UPDATEF_REPLACE_FILE))
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
            throw MakeStringException(-1,"Source file %s in Dali %s is not a file or superfile",filename,srcdali?srcdali->endpoint().getEndpointHostText(s).str():"(local)");
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
        Owned<IPropertyTree> ftree = fdir->getFileTree(slfn.get(), foreignuserdesc, srcdali, FOREIGN_DALI_TIMEOUT, GetFileTreeOpts::appendForeign);
        if (!ftree.get()) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getEndpointHostText(s).str():"(local)");
        }
        IPropertyTree *attsrc = ftree->queryPropTree("Attr");
        if (!attsrc) {
            StringBuffer s;
            throw MakeStringException(-1,"Attributes for source file %s could not be found in Dali %s",slfn.get(),srcdali?srcdali->endpoint().getEndpointHostText(s).str():"(local)");
        }
        CDfsLogicalFileName dlfn;
        dlfn.set(destfilename);
        if (strcmp(ftree->queryName(),queryDfsXmlBranchName(DXB_File))!=0) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s in Dali %s is not a simple file",filename,srcdali?srcdali->endpoint().getEndpointHostText(s).str():"(local)");
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
        Owned<IDistributedFile> dfile = fdir->lookup(dlfn,userdesc,AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
        if (dfile) {
            if (!checkOverwrite(DALI_UPDATEF_REPLACE_FILE))
                throw MakeStringException(-1,"Destination file %s already exists",dlfn.get());

            IPropertyTree &attloc = dfile->queryAttributes();
            if (dfile->numParts() == (unsigned)ftree->getPropInt("@numparts") &&
                attsrc->getPropInt("@eclCRC") == attloc.getPropInt("@eclCRC") &&
                attsrc->getPropInt("@totalCRC") == attloc.getPropInt64("@totalCRC"))
            {
                if (!copyphysical)
                {
                    Owned<IFileDescriptor> dstfdesc=dfile->getFileDescriptor();
                    Owned<IFileDescriptor> srcfdesc = deserializeFileDescriptorTree(ftree, NULL, 0);
                    updateCloneFrom(filename, dstfdesc, srcfdesc, ftree, srcdali, srcCluster);
                }
                return;
            }

            dfile->detach();
            dfile.clear();
        }
        cloneSubFile(ftree,dlfn.get(),srcdali, srcCluster);
        level--;
    }

    inline const char *getDaliEndPointStr(INode *daliNode, StringBuffer &s)
    {
        if (!daliNode)
            return "(local)";
        return daliNode->endpoint().getEndpointHostText(s).str();
    }
    inline bool checkHasCluster(IDistributedFile *dfile)
    {
        StringArray clusterNames;
        dfile->getClusterNames(clusterNames);
        return clusterNames.find(cluster1)!=NotFound;
    }
    inline bool checkIsKey(IPropertyTree *ftree)
    {
        const char * kind = ftree->queryProp("Attr/@kind");
        return kind && streq(kind, "key");
    }
    void addCluster(IDistributedFile *dfile, IPropertyTree *srcFtree)
    {
        if (dfile)
        {
            ClusterPartDiskMapSpec spec = spec1;
            if (checkIsKey(srcFtree) && repeattlk)
                spec.setRepeatedCopies(CPDMSRP_lastRepeated, false);
            DBGLOG("addCluster %s to file %s", cluster1.str(), dfile->queryLogicalName());
            dfile->addCluster(cluster1, spec);
        }
    }
    bool checkFileChanged(IDistributedFile *dfile, IPropertyTree *srcftree, IPropertyTree *srcAttrs)
    {
        IPropertyTree &dstAttrs = dfile->queryAttributes();
        return (dfile->numParts() != (unsigned) srcftree->getPropInt("@numparts") ||
                srcAttrs->getPropInt("@eclCRC") != dstAttrs.getPropInt("@eclCRC") ||
                srcAttrs->getPropInt("@totalCRC") != dstAttrs.getPropInt64("@totalCRC"));
    }
    inline bool checkValueChanged(const char *s1, const char *s2)
    {
        if (s1 && s2)
            return !streq(s1, s2);
        return ((s1 && *s1) || (s2 && *s2));
    }
    bool checkCloneFromChanged(IDistributedFile *dfile, IFileDescriptor *srcfdesc, INode *srcdali, const char *srcCluster)
    {
        if (!srcdali || srcdali->endpoint().isNull())
        {
            return checkValueChanged(dfile->queryAttributes().queryProp("@cloneFromPeerCluster"), srcCluster);
        }
        else
        {
            IPropertyTree *dstClonedFDesc = dfile->queryAttributes().queryPropTree("cloneFromFDesc");
            IPropertyTree *srcClonedFDesc = srcfdesc->queryProperties().queryPropTree("cloneFromFDesc");
            if (dstClonedFDesc && srcClonedFDesc)
            {
                // if both based on cloneFromFDesc, no need to check other varieties.
                return !areMatchingPTrees(dstClonedFDesc, srcClonedFDesc);
            }
            else if (dstClonedFDesc || srcClonedFDesc) // one has cloneFromFDesc, the other doesn't
                return true;
            // else - neither based on cloneFromFDesc
            StringBuffer s;
            if (checkValueChanged(dfile->queryAttributes().queryProp("@cloneRemote"), remoteStorage.str()))
                return true;
            if (checkValueChanged(dfile->queryAttributes().queryProp("@cloneFrom"), srcdali->endpoint().getEndpointHostText(s).str()))
                return true;
            if (checkValueChanged(dfile->queryAttributes().queryProp("@cloneFromDir"), srcfdesc->queryDefaultDir()))
                return true;
            if (checkValueChanged(dfile->queryAttributes().queryProp("@cloneFromPeerCluster"), (srcCluster && *srcCluster) ? "-" : NULL))
                return true;
            if (checkValueChanged(dfile->queryAttributes().queryProp("@cloneFromPrefix"), prefix.get()))
                return true;

            unsigned groupCount = dfile->queryAttributes().getCount("cloneFromGroup");
            if (srcCluster && *srcCluster && groupCount!=1)
                return true;

            unsigned numSrcClusters = srcfdesc->numClusters();
            if (numSrcClusters != groupCount)
                return true;
            StringArray clusterNames;
            dfile->getClusterNames(clusterNames);
            for (unsigned clusterNum = 0; clusterNum < numSrcClusters; clusterNum++)
            {
                StringBuffer sourceGroup;
                srcfdesc->getClusterGroupName(clusterNum, sourceGroup, NULL);
                if (!clusterNames.contains(sourceGroup.str()))
                    return true;
            }
        }
        return false;
    }

    void cloneRoxieFile(const char *filename, const char *destfilename)
    {
        DBGLOG("cloneRoxieFile: filename='%s', destfilename='%s'", filename, destfilename);

        Linked<INode> srcdali = foreigndalinode;
        CDfsLogicalFileName srcLFN;
        srcLFN.set(filename);
        if (srcLFN.isForeign())
        {
            SocketEndpoint ep;
            srcLFN.getEp(ep);
            srcLFN.clearForeign();
            srcdali.setown(createINode(ep));
        }

        StringBuffer s;
        Owned<IPropertyTree> ftree;
        IPropertyTree *attsrc = nullptr;
        if (remoteStorage || srcLFN.isRemote())
        {
            StringBuffer remoteLFN;
            if (!srcLFN.isRemote())
                remoteLFN.append("remote::").append(remoteStorage).append("::");
            srcLFN.get(remoteLFN);

            Owned<wsdfs::IDFSFile> dfsFile = wsdfs::lookupDFSFile(remoteLFN.str(), AccessMode::readSequential, INFINITE, wsdfs::keepAliveExpiryFrequency, foreignuserdesc);
            if (!dfsFile)
                throw makeStringExceptionV(-1,"Source file %s could not be found in Remote Storage", remoteLFN.str()); //remote scope already included in remoteLFN
            ftree.setown(dfsFile->queryFileMeta()->getPropTree("File"));
        }
        else
        {
            ftree.setown(fdir->getFileTree(srcLFN.get(), foreignuserdesc, srcdali, FOREIGN_DALI_TIMEOUT, GetFileTreeOpts::appendForeign));
            if (!ftree.get())
                throw MakeStringException(-1,"Source file %s could not be found in Dali %s",srcLFN.get(), getDaliEndPointStr(srcdali, s));
            attsrc = ftree->queryPropTree("Attr");
            if (!attsrc)
                throw MakeStringException(-1,"Attributes for source file %s could not be found in Dali %s",srcLFN.get(), getDaliEndPointStr(srcdali, s));
        }

        CDfsLogicalFileName dlfn;
        dlfn.set(destfilename);
        if (!streq(ftree->queryName(),queryDfsXmlBranchName(DXB_File)))
            throw MakeStringException(-1,"Source file %s in Dali %s is not a simple file",filename, getDaliEndPointStr(srcdali, s));

        if (!remoteStorage.length() && (!srcdali.get() || queryCoven().inCoven(srcdali)))
        {
            // if dali is local and filenames same
            if (streq(srcLFN.get(), dlfn.get()))
            {
                extendSubFile(ftree,dlfn.get());
                return;
            }
        }

        //see if target already exists
        Owned<IDistributedFile> dfile = fdir->lookup(dlfn, userdesc, AccessMode::tbdWrite, false, false, nullptr, defaultPrivilegedUser);
        if (dfile)
        {
            if (!checkOverwrite(DALI_UPDATEF_SUBFILE_MASK))
                throw MakeStringException(-1, "Destination file %s already exists", dlfn.get());

            if (checkOverwrite(DALI_UPDATEF_REPLACE_FILE) && checkFileChanged(dfile, ftree, attsrc)) //complete overwrite
            {
                DBGLOG("replacing file %s", destfilename);
                dfile->detach();
                dfile.clear();
            }
            else
            {
                if (checkOverwrite(DALI_UPDATEF_APPEND_CLUSTER) && !checkHasCluster(dfile))
                    addCluster(dfile, ftree);
                //cloneFrom is to tell roxie where to copy the physical file from, it's unecessary if we do the copy here
                if (!copyphysical && checkOverwrite(DALI_UPDATEF_CLONE_FROM))
                {
                    Owned<IFileDescriptor> srcfdesc = deserializeFileDescriptorTree(ftree, NULL, 0);
                    if (checkCloneFromChanged(dfile, srcfdesc, srcdali, srcCluster))
                        updateCloneFrom(dfile, srcfdesc, ftree, srcdali, srcCluster);
                }
                return;
            }
        }
        cloneSubFile(ftree,dlfn.get(),srcdali, srcCluster);
    }

};


class CDFUhelper: implements IDFUhelper, public CInterface
{
public:
    IMPLEMENT_IINTERFACE

    void addSuper(const char *superfname, IUserDescriptor *user, unsigned numtoadd, const char **subfiles, const char *before, bool autocreatesuper)
    {
        if (!numtoadd)
            throwError(DFUERR_DNoSubfileToAddToSuperFile);

        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);
        transaction->start();

        Owned<IDistributedSuperFile> superfile = transaction->lookupSuperFile(superfname, AccessMode::writeMeta);
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
        superfile.clear();
        transaction->commit();
    }


    void removeSuper(const char *superfname, IUserDescriptor *user, unsigned numtodelete, const char **subfiles, bool delsub, bool removesuperfile)
    {
        Owned<IDistributedFileTransaction> transaction = createDistributedFileTransaction(user);
        // We need this here, since caching only happens with active transactions
        // MORE - abstract this with DFSAccess, or at least enable caching with a flag
        Owned<IDistributedSuperFile> superfile = transaction->lookupSuperFile(superfname, AccessMode::writeMeta);

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
        if (removesuperfile && toremove.ordinality()!=superfile->numSubFiles())
            removesuperfile = false;
        // Do we have something to delete?
        if (toremove.ordinality() || removesuperfile) {
            transaction->start();
            ForEachItemIn(i2,toremove)
                superfile->removeSubFile(toremove.item(i2).text.get(),delsub,false,transaction);
            // Delete superfile if empty
            if (removesuperfile)
                queryDistributedFileDirectory().removeEntry(superfname, user, transaction);
            superfile.clear();
            transaction->commit();
        }
    }

    void listSubFiles(const char *superfname,StringAttrArray &out,IUserDescriptor *user)
    {
        Owned<IDistributedSuperFile> superfile = queryDistributedFileDirectory().lookupSuperFile(superfname, user, AccessMode::readMeta);
        if (!superfile)
            throwError1(DFUERR_DSuperFileNotFound, superfname);
        Owned<IDistributedFileIterator> iter=superfile->getSubFileIterator();
        ForEach(*iter) {
            StringBuffer name;
            iter->getName(name);
            out.append(*new StringAttrItem(name.str()));
        }
    }

    StringBuffer &getFileXML(const char *lfn, StringBuffer &out, IUserDescriptor *user)
    {
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(lfn, user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
        if (!file) {
            INamedGroupStore  &grpstore= queryNamedGroupStore();
            Owned<IGroup> grp = grpstore.lookup(lfn);
            if (grp) {
                out.append("<Group name=\"").append(lfn).append("\">\n");
                ForEachNodeInGroup(i, *grp) {
                    StringBuffer ip;
                    grp->getNode(i)->endpoint().getHostText(ip);
                    out.append("  <Node ip=\"").append(ip).append("\">\n");
                }
                out.append("</Group>\n");
                return out;
            }
            else
                throwError1(DFUERR_DFileNotFound, lfn);
        }
        else
        {
            Owned<IPropertyTree> t = queryDistributedFileDirectory().getFileTree(lfn, user);
            toXML(t, out, true);
        }
        return out;
    }

    virtual void addFileXML(const char *lfn, const StringBuffer &xml, const char * cluster, IUserDescriptor *user) override
    {
        Owned<IPropertyTree> t = createPTreeFromXMLString(xml);
        IDistributedFileDirectory &dfd = queryDistributedFileDirectory();

        if (dfd.exists(lfn, user))
            throw MakeStringException(-1, "Destination file '%s' already exists!", lfn);

        // Check if this XML is a superfile map
        Owned<IDistributedFile> file;
        const char * nodeName = t->queryName();
        if (0 == strcmp(nodeName, queryDfsXmlBranchName(DXB_SuperFile)))
        {
            // It seems XML is a super file
            file.setown(dfd.createNewSuperFile(t));
        }
        else if (0 == strcmp(nodeName, queryDfsXmlBranchName(DXB_File)))
        {
            // Logical file map
            //Patch the cluster before the file is created
            if (!isEmptyString(cluster))
            {
                //In containerized mode the directory is configured somewhere else...
                //replace @directory with a directory calculated from the storage plane
                //prefix and the scope of the logical filename
                //MORE: This could should be commoned up with similar code elsewhere
                Owned<IStoragePlane> plane = getDataStoragePlane(cluster, true);
                StringBuffer location(plane->queryPrefix());
                const char * temp = lfn;
                for (;;)
                {
                    const char * sep = strstr(temp, "::");
                    if (!sep)
                        break;
                    addPathSepChar(location);
                    location.appendLower(sep - temp, temp);
                    temp = sep + 2;
                }
                t->setProp("@directory", location);

                //Remove any clusters in the incoming definition, and replace it with a single entry
                while (t->removeProp("Cluster"))
                {
                    //Loop again incase there are any other matches - currently removeProp() appears to remove the whole
                    //list, but when qualified it would only remove the 1st...
                }
                IPropertyTree * entry = t->addPropTree("Cluster", createPTree());
                entry->setProp("@name", cluster);
            }

            Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(t, &queryNamedGroupStore(), 0);
            file.setown(dfd.createNew(fdesc));
        }
        else
            throw MakeStringException(-1, "Unrecognised file XML root tag detected: '%s'", nodeName);

        file->validate();
        PROGLOG("Adding %s file %s.", file->querySuperFile()?"super":"logical", lfn);
        file->attach(lfn,user);
    }

    void addFileRemote(const char *lfn,SocketEndpoint &srcdali,const char *srclfn,IUserDescriptor *user,IUserDescriptor *srcuser=NULL)
    {
        SocketEndpoint daliep = srcdali;
        if (daliep.port==0)
            daliep.port= DALI_SERVER_PORT;
        Owned<INode> node = createINode(daliep);
        Owned<IFileDescriptor> fdesc = queryDistributedFileDirectory().getFileDescriptor(srclfn, AccessMode::tbdRead, srcuser, node);
        if (!fdesc) {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",srclfn,daliep.getEndpointHostText(s).str());
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
        Owned<IPropertyTree> ftree = queryDistributedFileDirectory().getFileTree(srclfn,srcuser,srcnode, FOREIGN_DALI_TIMEOUT, GetFileTreeOpts::appendForeign);
        if (!ftree.get())
        {
            StringBuffer s;
            throw MakeStringException(-1,"Source file %s could not be found in Dali %s",srclfn,daliep.getEndpointHostText(s).str());
        }
        // first see if target exists (and remove if does and overwrite specified)
        Owned<IDistributedFile> dfile = queryDistributedFileDirectory().lookup(lfn,user,AccessMode::tbdWrite,false,false,nullptr,defaultPrivilegedUser);
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
            throw MakeStringException(-1,"Source file %s in Dali %s is not a file or superfile",srclfn,daliep.getEndpointHostText(s).str());
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
        cloner.init(cluster1,clustmap,repeattlk,cluster2,userdesc,foreigndali,nullptr,foreignuserdesc,nameprefix,overwrite,dophysicalcopy);
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
        DBGLOG("createSingleFileClone src=%s@%s, dst=%s@%s, prefix=%s, ow=%d, docopy=%d", srcname, srcCluster, dstname, cluster1, prefix, overwrite, dophysicalcopy);
        CFileCloner cloner;
        cloner.init(cluster1,clustmap,repeattlk,cluster2,userdesc,foreigndali,nullptr,foreignuserdesc,NULL,overwrite,dophysicalcopy);
        cloner.srcCluster.set(srcCluster);
        cloner.prefix.set(prefix);
        cloner.cloneFile(srcname,dstname);
    }

    virtual void cloneRoxieSubFile(const char *srcLFN,             // src LFN (can't be super)
                         const char *srcCluster,
                         const char *dstLFN,                       // dst LFN
                         const char *dstCluster,                   // group name of roxie cluster
                         const char *prefix,
                         unsigned redundancy,                      // Number of "spare" copies of the data
                         unsigned channelsPerNode,                 // Overloaded and cyclic modes
                         int replicateOffset,                      // Used In cyclic mode only
                         const char *defReplicateFolder,
                         IUserDescriptor *userdesc,                // user desc for local dali
                         const char *foreigndali,                  // can be omitted if srcname foreign or local
                         const char *remoteStorage,                 // can be omitted if srcname remote or local
                         unsigned overwriteFlags,                  // overwrite destination if exists
                         bool dophysicalcopy
                         )
    {
        DBGLOG("cloneRoxieSubFile src=%s@%s, dst=%s@%s, prefix=%s, ow=%d, docopy=%d", srcLFN, srcCluster, dstLFN, dstCluster, prefix, overwriteFlags, dophysicalcopy);
        CFileCloner cloner;
        // MORE: Would the following be better to ensure files are copied when queries are deployed?
        // bool copyPhysical = isContainerized() && (foreigndali != nullptr);
        cloner.init(dstCluster, DFUcpdm_c_replicated_by_d, true, NULL, userdesc, foreigndali, remoteStorage, NULL, NULL, false, dophysicalcopy);
        cloner.overwriteFlags = overwriteFlags;
#ifndef _CONTAINERIZED
        //In containerized mode there is no need to replicate files to the local disks of the roxie cluster - so don't set the special flag
        cloner.spec1.setRoxie(redundancy, channelsPerNode, replicateOffset);
        if (defReplicateFolder)
            cloner.spec1.setDefaultReplicateDir(defReplicateFolder);
#endif
        cloner.srcCluster.set(srcCluster);
        cloner.prefix.set(prefix);
        cloner.cloneRoxieFile(srcLFN, dstLFN);
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




