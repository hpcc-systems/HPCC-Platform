/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#include "referencedfilelist.hpp"

#include "jptree.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "dasess.hpp"

#define WF_LOOKUP_TIMEOUT (1000*15)  // 15 seconds

bool getIsOpt(const IPropertyTree &graphNode)
{
    if (graphNode.hasProp("att[@name='_isOpt']"))
        return graphNode.getPropBool("att[@name='_isOpt']/@value", false);
    else
        return graphNode.getPropBool("att[@name='_isIndexOpt']/@value", false);
}

const char *skipForeign(const char *name, StringBuffer *ip)
{
    if (*name=='~')
        name++;
    const char *d1 = strstr(name, "::");
     if (d1)
     {
        StringBuffer cmp;
        if (strieq("foreign", cmp.append(d1-name, name).trim().str()))
        {
            // foreign scope - need to strip off the ip and port
            d1 += 2;  // skip ::

            const char *d2 = strstr(d1,"::");
            if (d2)
            {
                if (ip)
                    ip->append(d2-d1, d1).trim();
                d2 += 2;
                while (*d2 == ' ')
                    d2++;

                name = d2;
            }
        }
    }
    return name;
}

void splitDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster)
{
    if (!address || !*address)
    {
        cluster.append(defaultCluster);
        return;
    }

    const char *s=strchr(address, '@');
    if (s)
    {
        cluster.append(s - address, address);
        address = s + 1;
    }
    else if (defaultCluster && *defaultCluster)
        cluster.append(defaultCluster);
    s=strchr(address, '/');
    if (!s)
        ip.append(address);
    else
    {
        ip.append(s - address, address);
        prefix.append(s+1);
    }
}

void splitDerivedDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster, const char *baseCluster, const char *baseIP, const char *basePrefix)
{
    if (address && *address)
    {
        splitDfsLocation(address, cluster, ip, prefix, defaultCluster);
        return;
    }
    ip.append(baseIP);
    cluster.append(baseCluster);
    prefix.append(basePrefix);
}

class ReferencedFileList;

class ReferencedFile : public CInterface, implements IReferencedFile
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFile(const char *lfn, const char *sourceIP, const char *srcCluster, const char *prefix, bool isSubFile, unsigned _flags, const char *_pkgid) : flags(_flags), pkgid(_pkgid)
    {
        logicalName.set(skipForeign(lfn, &daliip)).toLowerCase();
        if (daliip.length())
            flags |= RefFileForeign;
        else
            daliip.set(sourceIP);
        fileSrcCluster.set(srcCluster);
        filePrefix.set(prefix);
        if (isSubFile)
            flags |= RefSubFile;
    }

    void reset()
    {
        flags &= RefSubFile;
    }

    IPropertyTree *getRemoteFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);
    IPropertyTree *getSpecifiedOrRemoteFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);

    void processLocalFileInfo(IDistributedFile *df, const char *dstCluster, const char *srcCluster, StringArray *subfiles);
    void processRemoteFileTree(IPropertyTree *tree, const char *srcCluster, StringArray *subfiles);

    void resolveLocal(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles);
    void resolveRemote(IUserDescriptor *user, INode *remote, const char *remotePrefix, const char *dstCluster, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveForeign=false);
    void resolve(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, INode *remote, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool resolveForeign=false);

    virtual const char *getLogicalName() const {return logicalName.str();}
    virtual unsigned getFlags() const {return flags;}
    virtual const SocketEndpoint &getForeignIP(SocketEndpoint &ep) const
    {
        if (flags & RefFileForeign && daliip.length())
            ep.set(daliip.str());
        else
            ep.set(NULL);
        return ep;
    }
    virtual void cloneInfo(IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool overwrite=false, bool cloneForeign=false);
    void cloneSuperInfo(ReferencedFileList *list, IUserDescriptor *user, INode *remote, bool overwrite);
    virtual const char *queryPackageId() const {return pkgid.get();}

public:
    StringBuffer logicalName;
    StringAttr pkgid;
    StringBuffer daliip;
    StringBuffer filePrefix;
    StringAttr fileSrcCluster;
    unsigned flags;
};

class ReferencedFileList : public CInterface, implements IReferencedFileList
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileList(const char *username, const char *pw)
    {
        if (username && pw)
        {
            user.setown(createUserDescriptor());
            user->set(username, pw);
        }
    }

    void ensureFile(const char *ln, unsigned flags, const char *pkgid, const char *daliip=NULL, const char *srcCluster=NULL, const char *remotePrefix=NULL);

    virtual void addFile(const char *ln, const char *daliip=NULL, const char *srcCluster=NULL, const char *remotePrefix=NULL);
    virtual void addFiles(StringArray &files);
    virtual void addFilesFromWorkUnit(IConstWorkUnit *cw);
    virtual void addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid);
    virtual void addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg);
    virtual void addFilesFromPackageMap(IPropertyTree *pm);

    void addFileFromSubFile(IPropertyTree &subFile, const char *_daliip, const char *srcCluster, const char *_remotePrefix);
    void addFilesFromSuperFile(IPropertyTree &superFile, const char *_daliip, const char *srcCluster, const char *_remotePrefix);
    void addFilesFromPackage(IPropertyTree &package, const char *_daliip, const char *srcCluster, const char *_remotePrefix);

    virtual IReferencedFileIterator *getFiles();
    virtual void cloneFileInfo(IDFUhelper *helper, bool overwrite, bool cloneSuperInfo, bool cloneForeign=false);
    virtual void cloneRelationships();
    virtual void cloneAllInfo(IDFUhelper *helper, bool overwrite, bool cloneSuperInfo, bool cloneForeign=false)
    {
        cloneFileInfo(helper, overwrite, cloneSuperInfo, cloneForeign);
        cloneRelationships();
    }
    virtual void resolveFiles(const char *process, const char *remoteIP, const char *_remotePrefix, const char *srcCluster, bool checkLocalFirst, bool addSubFiles, bool resolveForeign=false);
    void resolveSubFiles(StringArray &subfiles, bool checkLocalFirst, bool resolveForeign);

public:
    Owned<IUserDescriptor> user;
    Owned<INode> remote;
    MapStringToMyClass<ReferencedFile> map;
    StringAttr process;
    StringAttr srcCluster;
    StringAttr remotePrefix;
};

void ReferencedFile::processLocalFileInfo(IDistributedFile *df, const char *dstCluster, const char *srcCluster, StringArray *subfiles)
{
    IDistributedSuperFile *super = df->querySuperFile();
    if (super)
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IDistributedFileIterator> it = super->getSubFileIterator(true); //supersub = true, no need to deal with LOCAL supersubs
            ForEach(*it)
            {
                IDistributedFile &sub = it->query();
                StringBuffer name;
                sub.getLogicalName(name);
                subfiles->append(name.str());
            }
        }
    }
    else
    {
        if (!dstCluster || !*dstCluster)
            return;
        if (df->findCluster(dstCluster)==NotFound)
            flags |= RefFileNotOnCluster;
        if (fileSrcCluster.length())
            srcCluster=fileSrcCluster;
        if (srcCluster && *srcCluster)
            if (NotFound == df->findCluster(srcCluster))
                flags |= RefFileNotOnSource;
    }
}

void ReferencedFile::processRemoteFileTree(IPropertyTree *tree, const char *srcCluster, StringArray *subfiles)
{
    flags |= RefFileRemote;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;
    if (streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile)))
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IPropertyTreeIterator> it = tree->getElements("SubFile");
            ForEach(*it)
                subfiles->append(it->query().queryProp("@name"));
        }
    }
    else if (srcCluster && *srcCluster)
    {

        VStringBuffer xpath("Cluster[@name='%s']", srcCluster);
        if (!tree->hasProp(xpath))
            flags |= RefFileNotOnSource;
    }

}

void ReferencedFile::resolveLocal(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles)
{
    if (flags & RefFileInPackage)
        return;
    reset();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
    if(df)
        processLocalFileInfo(df, dstCluster, srcCluster, subfiles);
    else
        flags |= RefFileNotFound;
}

IPropertyTree *ReferencedFile::getRemoteFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix)
{
    if (!remote)
        return NULL;
    StringBuffer remoteLFN;
    if (remotePrefix && *remotePrefix)
        remoteLFN.append(remotePrefix).append("::").append(logicalName);
    return queryDistributedFileDirectory().getFileTree(remoteLFN.length() ? remoteLFN : logicalName, user, remote, WF_LOOKUP_TIMEOUT, false, false);
}

IPropertyTree *ReferencedFile::getSpecifiedOrRemoteFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix)
{
    if (daliip.length())
    {
        Owned<INode> daliNode;
        daliNode.setown(createINode(daliip));
        return getRemoteFileTree(user, daliNode, filePrefix);
    }
    if (!remote)
        return NULL;
    StringBuffer remoteLFN;
    Owned<IPropertyTree> fileTree = getRemoteFileTree(user, remote, remotePrefix);
    if (!fileTree)
        return NULL;
    remote->endpoint().getUrlStr(daliip);
    filePrefix.set(remotePrefix);
    return fileTree.getClear();
}

void ReferencedFile::resolveRemote(IUserDescriptor *user, INode *remote, const char *remotePrefix, const char *dstCluster, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveForeign)
{
    if ((flags & RefFileForeign) && !resolveForeign)
        return;
    if (flags & RefFileInPackage)
        return;
    reset();
    if (checkLocalFirst)
    {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user);
        if(df)
        {
            processLocalFileInfo(df, dstCluster, NULL, subfiles);
            return;
        }
    }
    Owned<IPropertyTree> tree = getSpecifiedOrRemoteFileTree(user, remote, remotePrefix);
    if (tree)
        processRemoteFileTree(tree, srcCluster, subfiles);
    else
        flags |= RefFileNotFound;
}

void ReferencedFile::resolve(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, INode *remote, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool resolveForeign)
{
    if (daliip.length() || remote)
        resolveRemote(user, remote, remotePrefix, dstCluster, srcCluster, checkLocalFirst, subfiles, resolveForeign);
    else
        resolveLocal(dstCluster, srcCluster, user, subfiles);
}

void ReferencedFile::cloneInfo(IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool overwrite, bool cloneForeign)
{
    if ((flags & RefFileCloned) || (flags & RefFileSuper) || (flags & RefFileInPackage))
        return;
    if ((flags & RefFileForeign) && !cloneForeign)
        return;
    if (!(flags & (RefFileRemote | RefFileForeign | RefFileNotOnCluster)))
        return;
    if (!daliip.length())
        return;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;

    try
    {
        StringBuffer srcLFN;
        if (filePrefix.length())
            srcLFN.append(filePrefix.str()).append("::");
        srcLFN.append(logicalName.str());

        helper->createSingleFileClone(srcLFN, srcCluster, logicalName, dstCluster, filePrefix,
            DFUcpdm_c_replicated_by_d, true, NULL, user, daliip, NULL, overwrite, false);
        flags |= RefFileCloned;
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("Unknown error copying file info for [%s::] %s, from %s on dfs-dali %s", filePrefix.str(), logicalName.str(), fileSrcCluster.length() ? fileSrcCluster.get() : "*", daliip.str());
    }
}

void ReferencedFile::cloneSuperInfo(ReferencedFileList *list, IUserDescriptor *user, INode *remote, bool overwrite)
{
    if ((flags & RefFileCloned) || (flags & RefFileInPackage) || !(flags & RefFileSuper) || !(flags & RefFileRemote))
        return;

    try
    {
        Owned<IPropertyTree> tree = getSpecifiedOrRemoteFileTree(user, remote, NULL);
        if (!tree)
            return;

        IDistributedFileDirectory &dir = queryDistributedFileDirectory();
        Owned<IDistributedFile> df = dir.lookup(logicalName.str(), user);
        if(df)
        {
            if (!overwrite)
                return;
            df->detach();
            df.clear();
        }

        Owned<IDistributedSuperFile> superfile = dir.createSuperFile(logicalName.str(),user, true, false);
        flags |= RefFileCloned;
        Owned<IPropertyTreeIterator> subfiles = tree->getElements("SubFile");
        ForEach(*subfiles)
        {
            const char *name = subfiles->query().queryProp("@name");
            if (list)
            {
                //ensure superfile in superfile is cloned, before add
                ReferencedFile *subref = list->map.getValue(name);
                if (subref)
                    subref->cloneSuperInfo(list, user, remote, overwrite);
            }
            if (name && *name)
                superfile->addSubFile(name, false, NULL, false);
        }
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e);
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("Unknown error copying superfile info for %s", logicalName.str());
    }
}

class ReferencedFileIterator : public CInterface, implements IReferencedFileIterator
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileIterator(ReferencedFileList *_list)
    {
        list.set(_list);
        iter.setown(new HashIterator(list->map));
    }

    virtual bool first()
    {
        return iter->first();
    }

    virtual bool next()
    {
        return iter->next();
    }
    virtual bool isValid()
    {
        return iter->isValid();
    }
    virtual IReferencedFile  & query()
    {
        return *list->map.mapToValue(&iter->query());
    }

    virtual ReferencedFile  & queryObject()
    {
        return *(list->map.mapToValue(&iter->query()));
    }

public:
    Owned<ReferencedFileList> list;
    Owned<HashIterator> iter;
};

void ReferencedFileList::ensureFile(const char *ln, unsigned flags, const char *pkgid, const char *daliip, const char *srcCluster, const char *prefix)
{
    Owned<ReferencedFile> file = new ReferencedFile(ln, daliip, srcCluster, prefix, false, flags, pkgid);
    if (!file->logicalName.length())
        return;
    ReferencedFile *existing = map.getValue(file->getLogicalName());
    if (existing)
        existing->flags |= flags;
    else
    {
        const char *refln = file->getLogicalName();
        map.setValue(refln, file.getClear());
    }
}

void ReferencedFileList::addFile(const char *ln, const char *daliip, const char *srcCluster, const char *prefix)
{
    ensureFile(ln, 0, NULL, daliip, srcCluster, prefix);
}

void ReferencedFileList::addFiles(StringArray &files)
{
    ForEachItemIn(i, files)
        addFile(files.item(i));
}

void ReferencedFileList::addFileFromSubFile(IPropertyTree &subFile, const char *ip, const char *cluster, const char *prefix)
{
    addFile(subFile.queryProp("@value"), ip, cluster, prefix);
}

void ReferencedFileList::addFilesFromSuperFile(IPropertyTree &superFile, const char *_ip, const char *_cluster, const char *_prefix)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    splitDerivedDfsLocation(superFile.queryProp("@daliip"), cluster, ip, prefix, NULL, _cluster, _ip, _prefix);
    if (superFile.hasProp("@sourceCluster"))
        cluster.set(superFile.queryProp("@sourceCluster"));

    Owned<IPropertyTreeIterator> subFiles = superFile.getElements("SubFile[@value]");
    ForEach(*subFiles)
        addFileFromSubFile(subFiles->query(), ip, cluster, prefix);
}

void ReferencedFileList::addFilesFromPackage(IPropertyTree &package, const char *_ip, const char *_cluster, const char *_prefix)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    splitDerivedDfsLocation(package.queryProp("@daliip"), cluster, ip, prefix, NULL, _cluster, _ip, _prefix);
    if (package.hasProp("@sourceCluster"))
        cluster.set(package.queryProp("@sourceCluster"));

    Owned<IPropertyTreeIterator> supers = package.getElements("SuperFile");
    ForEach(*supers)
        addFilesFromSuperFile(supers->query(), ip, cluster, prefix);
}

void ReferencedFileList::addFilesFromPackageMap(IPropertyTree *pm)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    const char *srcCluster = pm->queryProp("@sourceCluster");
    splitDerivedDfsLocation(pm->queryProp("@daliip"), cluster, ip, prefix, srcCluster, srcCluster, NULL, NULL);
    Owned<IPropertyTreeIterator> packages = pm->getElements("Package");
    ForEach(*packages)
        addFilesFromPackage(packages->query(), ip, cluster, prefix);
}

void ReferencedFileList::addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg)
{
    Owned<IConstWUGraphIterator> graphs = &cw->getGraphs(GraphTypeActivities);
    ForEach(*graphs)
    {
        Owned <IPropertyTree> xgmml = graphs->query().getXGMMLTree(false);
        Owned<IPropertyTreeIterator> iter = xgmml->getElements("//node[att/@name='_*ileName']");
        ForEach(*iter)
        {
            IPropertyTree &node = iter->query();
            const char *logicalName = node.queryProp("att[@name='_fileName']/@value");
            if (!logicalName)
                logicalName = node.queryProp("att[@name='_indexFileName']/@value");
            if (!logicalName)
                continue;
            ThorActivityKind kind = (ThorActivityKind) node.getPropInt("att[@name='_kind']/@value", TAKnone);
            //not likely to be part of roxie queries, but for forward compatibility:
            if(kind==TAKdiskwrite || kind==TAKindexwrite || kind==TAKcsvwrite || kind==TAKxmlwrite)
                continue;
            if (node.getPropBool("att[@name='_isSpill']/@value") ||
                node.getPropBool("att[@name='_isTransformSpill']/@value"))
                continue;
            unsigned flags = 0;
            if (pkg)
            {
                const char *pkgid = pkg->locateSuperFile(logicalName);
                if (pkgid)
                {
                    flags |= (RefFileSuper | RefFileInPackage);
                    Owned<ISimpleSuperFileEnquiry> ssfe = pkg->resolveSuperFile(logicalName);
                    if (ssfe && ssfe->numSubFiles()>0)
                    {
                        unsigned count = ssfe->numSubFiles();
                        while (count--)
                        {
                            StringBuffer subfile;
                            ssfe->getSubFileName(count, subfile);
                            ensureFile(subfile, RefSubFile | RefFileInPackage, NULL);
                        }
                    }
                }
                ensureFile(logicalName, flags, pkgid);
            }
            else
                ensureFile(logicalName, flags, NULL);
        }
    }
}

void ReferencedFileList::addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid)
{
    const IHpccPackage *pkg = NULL;
    if (pm && queryid && *queryid)
        pkg = pm->matchPackage(queryid);
    addFilesFromQuery(cw, pkg);
}

void ReferencedFileList::addFilesFromWorkUnit(IConstWorkUnit *cw)
{
    addFilesFromQuery(cw, NULL, NULL);
}

void ReferencedFileList::resolveSubFiles(StringArray &subfiles, bool checkLocalFirst, bool resolveForeign)
{
    StringArray childSubFiles;
    ForEachItemIn(i, subfiles)
    {
        Owned<ReferencedFile> file = new ReferencedFile(subfiles.item(i), NULL, NULL, NULL, true, 0, NULL);
        if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
        {
            file->resolve(process.get(), srcCluster, user, remote, remotePrefix, checkLocalFirst, &childSubFiles, resolveForeign);
            const char *ln = file->getLogicalName();
            map.setValue(ln, file.getClear());
        }
    }
    if (childSubFiles.length())
        resolveSubFiles(childSubFiles, checkLocalFirst, resolveForeign);
}

void ReferencedFileList::resolveFiles(const char *_process, const char *remoteIP, const char *_remotePrefix, const char *_srcCluster, bool checkLocalFirst, bool expandSuperFiles, bool resolveForeign)
{
    process.set(_process);
    remote.setown((remoteIP && *remoteIP) ? createINode(remoteIP, 7070) : NULL);
    srcCluster.set(_srcCluster);
    remotePrefix.set(_remotePrefix);

    StringArray subfiles;
    {
        ReferencedFileIterator files(this);
        ForEach(files)
            files.queryObject().resolve(process, srcCluster, user, remote, remotePrefix, checkLocalFirst, expandSuperFiles ? &subfiles : NULL, resolveForeign);
    }
    if (expandSuperFiles)
        resolveSubFiles(subfiles, checkLocalFirst, resolveForeign);
}

void ReferencedFileList::cloneFileInfo(IDFUhelper *helper, bool overwrite, bool cloneSuperInfo, bool cloneForeign)
{
    ReferencedFileIterator files(this);
    ForEach(files)
        files.queryObject().cloneInfo(helper, user, process, srcCluster, overwrite, cloneForeign);
    if (cloneSuperInfo)
        ForEach(files)
            files.queryObject().cloneSuperInfo(this, user, remote, overwrite);
}

void ReferencedFileList::cloneRelationships()
{
    if (!remote || remote->endpoint().isNull())
        return;

    StringBuffer addr;
    remote->endpoint().getUrlStr(addr);
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    ReferencedFileIterator files(this);
    ForEach(files)
    {
        ReferencedFile &file = files.queryObject();
        if (!(file.getFlags() & RefFileRemote))
            continue;
        Owned<IFileRelationshipIterator> iter = dir.lookupFileRelationships(file.getLogicalName(), NULL,
            NULL, NULL, NULL, NULL, NULL, addr.str(), WF_LOOKUP_TIMEOUT);

        ForEach(*iter)
        {
            IFileRelationship &r=iter->query();
            const char* assoc = r.querySecondaryFilename();
            if (!assoc)
                continue;
            if (*assoc == '~')
                assoc++;
            IReferencedFile *refAssoc = map.getValue(assoc);
            if (refAssoc && !(refAssoc->getFlags() & RefFileCopyInfoFailed))
            {
                dir.addFileRelationship(file.getLogicalName(), assoc, r.queryPrimaryFields(), r.querySecondaryFields(),
                    r.queryKind(), r.queryCardinality(), r.isPayload(), user, r.queryDescription());
            }
        }
    }
}

IReferencedFileIterator *ReferencedFileList::getFiles()
{
    return new ReferencedFileIterator(this);
}

IReferencedFileList *createReferencedFileList(const char *user, const char *pw)
{
    return new ReferencedFileList(user, pw);
}
