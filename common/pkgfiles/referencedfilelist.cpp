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

#include "referencedfilelist.hpp"

#include "jptree.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"
#include "dautils.hpp"
#include "dasds.hpp"
#include "dadfs.hpp"
#include "daqueue.hpp"
#include "dasess.hpp"
#include "dfuwu.hpp"
#ifndef _CONTAINERIZED
#include "environment.hpp"
#endif

#include "ws_dfsclient.hpp"

#define WF_LOOKUP_TIMEOUT (1000*15)  // 15 seconds

bool getIsOpt(const IPropertyTree &graphNode)
{
    if (graphNode.hasProp("att[@name='_isOpt']"))
        return graphNode.getPropBool("att[@name='_isOpt']/@value", false);
    else
        return graphNode.getPropBool("att[@name='_isIndexOpt']/@value", false);
}

bool checkForeign(const char *lfn)
{
    if (*lfn=='~')
        lfn++;
    static size_t l = strlen("foreign");
    if (strnicmp("foreign", lfn, l)==0)
    {
        lfn+=l;
        while (isspace(*lfn))
            lfn++;
        if (lfn[0]==':' && lfn[1]==':')
            return true;
    }
    return false;
}

static const char *skipNextLfnScope(const char *lfn, StringBuffer *s)
{
    const char *sep = strstr(lfn, "::");
    if (sep)
    {
        if (s)
            s->append(sep-lfn, lfn).trim();
        sep += 2;
        while (*sep == ' ')
            sep++;
        return sep;
    }
    return lfn;
}
const char *skipForeignOrRemote(const char *name, StringBuffer *ip, StringBuffer *remote)
{
    unsigned maxTildas = 2;
    while (maxTildas-- && *name=='~')
        name++;
    const char *sep = strstr(name, "::");
     if (sep)
     {
        if (strnicmp("remote", name, sep-name)==0)
            return skipNextLfnScope(sep+2, remote);
        else if (strnicmp("foreign", name, sep-name)==0)
            return skipNextLfnScope(sep+2, ip);
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

void splitDerivedDfsLocationOrRemote(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix,
                                    const char *defaultCluster, const char *baseCluster, const char *baseIP, const char *basePrefix,
                                    const char *currentRemoteStorage, StringBuffer& effectiveRemoteStorage, const char *baseRemoteStorage)
{
    if (!isEmptyString(address) && !isEmptyString(currentRemoteStorage))
        throw makeStringExceptionV(-1, "Cannot specify both a dfs location (%s) and a remote storage location (%s)", address, currentRemoteStorage);
    // Choose either a daliip (split from address) or a remote-storage location to
    // propagate to the next level of parsing as the effective method of resolving a file
    if (!isEmptyString(address))
    {
        splitDfsLocation(address, cluster, ip, prefix, defaultCluster);
        if (!ip.isEmpty())
            effectiveRemoteStorage.clear();
        return;
    }

    if (!isEmptyString(currentRemoteStorage))
    {
        effectiveRemoteStorage.append(currentRemoteStorage);
        // Cluster and prefix aren't used if ip is empty, so they don't need to be cleared
        ip.clear();
        return;
    }

    ip.append(baseIP);
    cluster.append(baseCluster);
    prefix.append(basePrefix);
    effectiveRemoteStorage.append(baseRemoteStorage);
}

class ReferencedFileList;

class ReferencedFile : implements IReferencedFile, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFile(const char *lfn, const char *sourceIP, const char *srcCluster, const char *prefix, bool isSubFile, unsigned _flags, const char *_pkgid, bool noDfs, bool calcSize, const char *remoteStorageName)
    : pkgid(_pkgid), fileSize(0), numParts(0), flags(_flags), noDfsResolution(noDfs), calcFileSize(calcSize), trackSubFiles(false)
    {
        {
            //Scope ensures strings are assigned
            StringAttrBuilder logicalNameText(logicalName), daliipText(daliip), remoteStorageText(remoteStorage);
            logicalNameText.set(skipForeignOrRemote(lfn, &daliipText, &remoteStorageText)).toLowerCase();
        }
        if (remoteStorage.length())
            flags |= RefFileLFNRemote;
        else if (!isEmptyString(remoteStorageName))
        {
            // can be declared in packagemap at different scopes
            remoteStorage.set(remoteStorageName);
            flags |= RefFileLFNRemote;
        }
        else if (daliip.length())
            flags |= RefFileLFNForeign;
        else
            daliip.set(sourceIP); // can be declared in packagemap at different scopes
        fileSrcCluster.set(srcCluster);
        filePrefix.set(prefix);
        if (isSubFile)
            flags |= RefSubFile;
    }

    ReferencedFile(const char *lfn, const char *sourceIP, const char *srcCluster, const char *prefix, bool isSubFile, unsigned _flags, const char *_pkgid, bool noDfs, bool calcSize)
    : ReferencedFile(lfn, sourceIP, srcCluster, prefix, isSubFile, _flags, _pkgid, noDfs, calcSize, nullptr) {    }

    void reset()
    {
        flags &= ~(RefFileNotOnCluster | RefFileNotFound | RefFileResolvedForeign | RefFileResolvedRemote | RefFileCopyInfoFailed | RefFileCloned | RefFileNotOnSource); //these flags are calculated during resolve
    }

    IPropertyTree *getRemoteStorageFileTree(IUserDescriptor *user, const char *remoteStorageName, const char *remotePrefix);
    IPropertyTree *getForeignFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);
    IPropertyTree *getFileOrProvidedForeignFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);

    void processLocalFileInfo(IDistributedFile *df, const StringArray &locations, const char *srcCluster, StringArray *subfiles);
    void processLocalFileInfo(IDistributedFile *df, const char *dstCluster, const char *srcCluster, StringArray *subfiles);
    void processForeignFileTree(const IPropertyTree *tree, const char *srcCluster, StringArray *subfiles);
    void processRemoteFileTree(const IPropertyTree *tree, const char *srcCluster, StringArray *subfiles);

    void resolveLocal(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles);
    void resolveLocal(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles);

    void resolveRemote(IUserDescriptor *user, const char *remoteStorage, const char *remotePrefix, const StringArray &locations, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign=false);
    void resolveForeign(IUserDescriptor *user, INode *remote, const char *remotePrefix, const StringArray &locations, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign=false);
    void resolveForeign(IUserDescriptor *user, INode *remote, const char *remotePrefix, const char *dstCluster, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign=false);

    void resolveLocalOrRemote(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, const char *remoteStorage, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool trackSubFiles, bool resolveLFNForeign=false);
    void resolveLocalOrForeign(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, INode *remote, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool trackSubFiles, bool resolveLFNForeign=false);

    virtual bool needsCopying(bool cloneForeign) const override;

    virtual const char *getLogicalName() const {return logicalName.str();}
    virtual unsigned getFlags() const {return flags;}
    virtual const SocketEndpoint &getForeignIP(SocketEndpoint &ep) const
    {
        if ((flags & RefFileLFNForeign) && daliip.length())
            ep.set(daliip.str());
        else
            ep.set(NULL);
        return ep;
    }
    virtual void cloneInfo(const IPropertyTree *directories, IDFUWorkUnit *publisherWu, unsigned updateFlags, IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, const char *dfu_queue);
    void cloneSuperInfo(IDFUWorkUnit *publisherWu, unsigned updateFlags, ReferencedFileList *list, IUserDescriptor *user, INode *remote);
    virtual const char *queryPackageId() const {return pkgid.get();}
    virtual __int64 getFileSize()
    {
        return fileSize;
    }
    virtual unsigned getNumParts()
    {
        return numParts;
    }
    virtual const StringArray &getSubFileNames() const { return subFileNames; };
    virtual void appendSubFileNames(const StringArray &names)
    {
        ForEachItemIn(i, names)
            subFileNames.append(names.item(i));
    };
public:
    StringArray subFileNames;
    StringAttr logicalName;
    StringAttr pkgid;
    StringAttr daliip;
    StringAttr remoteStorage;
    StringAttr filePrefix;
    StringAttr fileSrcCluster;
    __int64 fileSize;
    unsigned numParts;
    unsigned flags;
    bool noDfsResolution;
    bool calcFileSize;
    bool trackSubFiles;
};

class ReferencedFileList : implements IReferencedFileList, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileList(const char *username, const char *pw, bool allowForeignFiles, bool allowFileSizeCalc, const char *_jobname)
        : jobName(_jobname), allowForeign(allowForeignFiles), allowSizeCalc(allowFileSizeCalc)
    {
        if (username)
        {
            user.setown(createUserDescriptor());
            user->set(username, pw);
        }
    }

    ReferencedFileList(IUserDescriptor *userDesc, bool allowForeignFiles, bool allowFileSizeCalc, const char *_jobname)
        : jobName(_jobname), allowForeign(allowForeignFiles), allowSizeCalc(allowFileSizeCalc)
    {
        if (userDesc)
            user.set(userDesc);
    }

    void ensureFile(const char *ln, unsigned flags, const char *pkgid, bool noDfsResolution, const StringArray *subfileNames, const char *daliip=nullptr, const char *srcCluster=nullptr, const char *remotePrefix=nullptr, const char* remoteStorage=nullptr);

    virtual void addFile(const char *ln, const char *daliip=nullptr, const char *srcCluster=nullptr, const char *remotePrefix=nullptr, const char *remoteStorageName=nullptr);
    virtual void addFiles(StringArray &files);
    virtual void addFilesFromWorkUnit(IConstWorkUnit *cw);
    virtual bool addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid);
    virtual bool addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg);
    virtual void addFilesFromPackageMap(IPropertyTree *pm);

    void addFileFromSubFile(IPropertyTree &subFile, const char *_daliip, const char *srcCluster, const char *_remotePrefix, const char *_remoteStorageName);
    void addFilesFromSuperFile(IPropertyTree &superFile, const char *_daliip, const char *srcCluster, const char *_remotePrefix, const char *_remoteStorageName);
    void addFilesFromPackage(IPropertyTree &package, const char *_daliip, const char *srcCluster, const char *_remotePrefix, const char *_remoteStorageName);

    virtual IReferencedFileIterator *getFiles();
    virtual void cloneFileInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder);

    virtual void cloneRelationships();
    virtual void cloneAllInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder)
    {
        cloneFileInfo(publisherWuid, dstCluster, updateFlags, helper, cloneSuperInfo, cloneForeign, redundancy, channelsPerNode, replicateOffset, defReplicateFolder);
        cloneRelationships();
    }
    virtual void resolveFiles(const StringArray &locations, const char *remoteIP, const char *_remotePrefix, const char *srcCluster, bool checkLocalFirst, bool addSubFiles, bool trackSubFiles, bool resolveLFNForeign, bool useRemoteStorage) override;

    void resolveSubFiles(StringArray &subfiles, const StringArray &locations, bool checkLocalFirst, bool trackSubFiles, bool resolveLFNForeign);
    virtual bool filesNeedCopying(bool cloneForeign);
    virtual void setDfuQueue(const char *queue) override
    {
        dfu_queue.set(queue);
    }
    virtual void setKeyCompression(const char * _keyCompression) override
    {
        keyCompression.set(_keyCompression);
    }

public:
    Owned<IUserDescriptor> user;
    Owned<INode> remote;
    MapStringToMyClass<ReferencedFile> map;
    StringAttr remoteStorage;
    StringAttr srcCluster;
    StringAttr remotePrefix;
    StringAttr jobName; //used to populate DFU job name, but could be used elsewhere
    StringAttr dfu_queue;
    StringAttr keyCompression;
    bool allowForeign;
    bool allowSizeCalc;
};

bool fileExistsWithinLocations(IDistributedFile *df, const StringArray &locations)
{
    if (!df)
        return false;
    ForEachItemIn(i, locations)
    {
        const char *name = locations.item(i);
        if (df->findCluster(name)!=NotFound)
            return true;
    }
    return false;
}

void ReferencedFile::processLocalFileInfo(IDistributedFile *df, const StringArray &locations, const char *srcCluster, StringArray *subfiles)
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
                if (trackSubFiles)
                    subFileNames.append(name.str());
            }
        }
    }
    else
    {
        flags |= RefSubFile;
        if (!locations.length())
            return;
        if (!fileExistsWithinLocations(df, locations))
            flags |= RefFileNotOnCluster;
        if (fileSrcCluster.length())
            srcCluster=fileSrcCluster;
        if (srcCluster && *srcCluster)
            if (NotFound == df->findCluster(srcCluster))
                flags |= RefFileNotOnSource;
        fileSize = df->getFileSize(calcFileSize, false);
        numParts = df->numParts();
    }
}

void ReferencedFile::processLocalFileInfo(IDistributedFile *df, const char *dstCluster, const char *srcCluster, StringArray *subfiles)
{
    StringArray locations;
    locations.append(dstCluster);
    processLocalFileInfo(df, locations, srcCluster, subfiles);
}

void ReferencedFile::processForeignFileTree(const IPropertyTree *tree, const char *srcCluster, StringArray *subfiles)
{
    flags |= RefFileResolvedForeign;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;
    if (streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile)))
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IPropertyTreeIterator> it = tree->getElements("SubFile");
            ForEach(*it)
            {
                const char *lfn = it->query().queryProp("@name");
                StringBuffer foreignLfn;
                if (flags & RefFileLFNForeign)
                    lfn = foreignLfn.append("foreign::").append(this->daliip).append("::").append(lfn).str();
                subfiles->append(lfn);
                if (trackSubFiles)
                    subFileNames.append(lfn);
            }
        }
    }
    else if (srcCluster && *srcCluster)
    {

        VStringBuffer xpath("Cluster[@name='%s']", srcCluster);
        if (!tree->hasProp(xpath))
            flags |= RefFileNotOnSource;
        numParts = tree->getPropInt("@numparts", 0);
        fileSize = tree->getPropInt64("Attr/@size", 0);
    }
}

static StringBuffer &makeRemoteLFN(StringBuffer &remoteLfn, const char *remoteStorageName, const char *remotePrefix, const char *lfn)
{
    if (isEmptyString(remoteStorageName))
        return remoteLfn;
    remoteLfn.append("remote::").append(remoteStorageName).append("::");
    if (!isEmptyString(remotePrefix))
        remoteLfn.append(remotePrefix).append("::");
    return remoteLfn.append(lfn);
}

void ReferencedFile::processRemoteFileTree(const IPropertyTree *tree, const char *srcCluster, StringArray *subfiles)
{
    flags |= RefFileResolvedRemote;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;
    if (streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile)))
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            Owned<IPropertyTreeIterator> it = tree->getElements("SubFile");
            ForEach(*it)
            {
                const char *lfn = it->query().queryProp("@name");
                StringBuffer remoteLfn;
                if (flags & RefFileLFNForeign)
                    lfn = makeRemoteLFN(remoteLfn, this->remoteStorage, nullptr, lfn).str();
                subfiles->append(lfn);
                if (trackSubFiles)
                    subFileNames.append(lfn);
            }
        }
    }
    else if (srcCluster && *srcCluster)
    {
        VStringBuffer xpath("Cluster[@name='%s']", srcCluster);
        if (!tree->hasProp(xpath))
            flags |= RefFileNotOnSource;
        numParts = tree->getPropInt("@numparts", 0);
        fileSize = tree->getPropInt64("Attr/@size", 0);
    }
}

void ReferencedFile::resolveLocal(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles)
{
    if (flags & RefFileInPackage)
        return;
    if (noDfsResolution)
    {
        flags |= RefFileNotFound;
        return;
    }
    reset();
    Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
    if(df)
        processLocalFileInfo(df, locations, srcCluster, subfiles);
    else
    {
        flags |= RefFileNotFound;
        DBGLOG("ReferencedFile not found (local) %s", logicalName.str());
    }
}

void ReferencedFile::resolveLocal(const char *dstCluster, const char *srcCluster, IUserDescriptor *user, StringArray *subfiles)
{
    StringArray locations;
    if (!isEmptyString(dstCluster))
        locations.append(dstCluster);
    resolveLocal(locations, srcCluster, user, subfiles);
}

IPropertyTree *ReferencedFile::getForeignFileTree(IUserDescriptor *user, INode *daliNode, const char *remotePrefix)
{
    if (!daliNode)
        return NULL;
    StringBuffer remoteLFN;
    if (remotePrefix && *remotePrefix)
        remoteLFN.append(remotePrefix).append("::").append(logicalName);
    return queryDistributedFileDirectory().getFileTree(remoteLFN.length() ? remoteLFN.str() : logicalName.str(), user, daliNode, WF_LOOKUP_TIMEOUT, GetFileTreeOpts::none);
}

IPropertyTree *ReferencedFile::getFileOrProvidedForeignFileTree(IUserDescriptor *user, INode *providedDaliNode, const char *remotePrefix)
{
    if (daliip.length())
    {
        Owned<INode> fileDaliNode;
        fileDaliNode.setown(createINode(daliip));
        return getForeignFileTree(user, fileDaliNode, filePrefix);
    }
    if (!providedDaliNode)
        return NULL;
    StringBuffer remoteLFN;
    Owned<IPropertyTree> fileTree = getForeignFileTree(user, providedDaliNode, remotePrefix);
    if (!fileTree)
        return NULL;
    StringAttrBuilder daliipText(daliip);
    providedDaliNode->endpoint().getEndpointHostText(daliipText);
    filePrefix.set(remotePrefix);
    return fileTree.getClear();
}

void ReferencedFile::resolveForeign(IUserDescriptor *user, INode *remote, const char *remotePrefix, const char *dstCluster, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign)
{
    StringArray locations;
    if (!isEmptyString(dstCluster))
        locations.append(dstCluster);
    resolveForeign(user, remote, remotePrefix, locations, srcCluster, checkLocalFirst, subfiles, resolveLFNForeign);
}
void ReferencedFile::resolveForeign(IUserDescriptor *user, INode *remote, const char *remotePrefix, const StringArray &locations, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign)
{
    if ((flags & RefFileLFNForeign) && !resolveLFNForeign && !trackSubFiles)
        return;
    if (flags & RefFileInPackage)
        return;
    if (noDfsResolution)
    {
        flags |= RefFileNotFound;
        return;
    }
    reset();
    if (checkLocalFirst) //usually means we don't want to overwrite existing file info
    {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
        if(df)
        {
            processLocalFileInfo(df, locations, NULL, subfiles);
            return;
        }
    }
    Owned<IPropertyTree> tree = getFileOrProvidedForeignFileTree(user, remote, remotePrefix);
    if (tree)
    {
        processForeignFileTree(tree, srcCluster, subfiles);
        return;
    }
    else if (!checkLocalFirst && (!srcCluster || !*srcCluster)) //haven't already checked and not told to use a specific copy
    {
        resolveLocal(locations, srcCluster, user, subfiles);
        return;
    }

    flags |= RefFileNotFound;

    StringBuffer dest;
    DBGLOG("Remote ReferencedFile not found %s [dali=%s, remote=%s, prefix=%s]", logicalName.str(), daliip.get(), remote ? remote->endpoint().getEndpointHostText(dest).str() : nullptr, remotePrefix);
}


IPropertyTree *ReferencedFile::getRemoteStorageFileTree(IUserDescriptor *user, const char *remoteStorageName, const char *remotePrefix)
{
    StringBuffer remoteLFN;
    makeRemoteLFN(remoteLFN, remoteStorageName, remotePrefix, logicalName);

    Owned<wsdfs::IDFSFile> dfsFile = wsdfs::lookupDFSFile(remoteLFN.str(), AccessMode::readSequential, INFINITE, wsdfs::keepAliveExpiryFrequency, user);
    IPropertyTree *tree = (dfsFile) ? dfsFile->queryFileMeta() : nullptr;
    if (!tree)
    {
        DBGLOG("RemoteStorage FileMetaTree not found %s [remoteStorage=%s, prefix=%s]", remoteLFN.str(), nullText(remoteStorageName), nullText(remotePrefix));
        return nullptr;
    }
    tree = tree->getPropTree("File");
    if (!tree)
        DBGLOG("RemoteStorage FileTree not found %s [remoteStorage=%s, prefix=%s]", remoteLFN.str(), nullText(remoteStorageName), nullText(remotePrefix));
    return tree;
}

void ReferencedFile::resolveRemote(IUserDescriptor *user, const char *remoteStorageName, const char *remotePrefix, const StringArray &locations, const char *srcCluster, bool checkLocalFirst, StringArray *subfiles, bool resolveLFNForeign)
{
    if (isEmptyString(remoteStorageName))
        return;
    if ((flags & RefFileLFNForeign) && !resolveLFNForeign && !trackSubFiles)
        return;
    if (flags & RefFileInPackage)
        return;
    if (noDfsResolution)
    {
        flags |= RefFileNotFound;
        return;
    }
    reset();
    if (checkLocalFirst) //usually means we don't want to overwrite existing file info
    {
        Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
        if(df)
        {
            processLocalFileInfo(df, locations, NULL, subfiles);
            return;
        }
    }
    Owned<IPropertyTree> tree = getRemoteStorageFileTree(user, remoteStorageName, remotePrefix);
    if (tree)
    {
        remoteStorage.set(remoteStorageName);
        processRemoteFileTree(tree, srcCluster, subfiles);
        return;
    }
    else if (!checkLocalFirst && (!srcCluster || !*srcCluster)) //haven't already checked and not told to use a specific copy
    {
        resolveLocal(locations, srcCluster, user, subfiles);
        return;
    }

    flags |= RefFileNotFound;

    DBGLOG("RemoteStorage ReferencedFile not found %s [remoteStorage=%s, prefix=%s]", logicalName.str(), remoteStorageName, remotePrefix);
}

void ReferencedFile::resolveLocalOrRemote(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, const char *remoteStorageName, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool _trackSubFiles, bool resolveLFNForeign)
{
    trackSubFiles = _trackSubFiles;
    if (!isEmptyString(remoteStorageName))
        resolveRemote(user, remoteStorageName, remotePrefix, locations, srcCluster, checkLocalFirst, subfiles, resolveLFNForeign);
    else
        resolveLocal(locations, srcCluster, user, subfiles);
}

void ReferencedFile::resolveLocalOrForeign(const StringArray &locations, const char *srcCluster, IUserDescriptor *user, INode *remote, const char *remotePrefix, bool checkLocalFirst, StringArray *subfiles, bool _trackSubFiles, bool resolveLFNForeign)
{
    trackSubFiles = _trackSubFiles;
    if (daliip.length() || remote)
        resolveForeign(user, remote, remotePrefix, locations, srcCluster, checkLocalFirst, subfiles, resolveLFNForeign);
    else
        resolveLocal(locations, srcCluster, user, subfiles);
}

static void setRoxieClusterPartDiskMapping(const char *clusterName, const char *defaultFolder, const char *defaultReplicateFolder, bool supercopy, IDFUfileSpec *wuFSpecDest, IDFUoptions *wuOptions)
{
    ClusterPartDiskMapSpec spec;
    spec.setDefaultBaseDir(defaultFolder);

    if (!supercopy)
        spec.setRepeatedCopies(CPDMSRP_lastRepeated,false);
    wuFSpecDest->setClusterPartDiskMapSpec(clusterName,spec);
}

static void getDefaultDFUName(StringBuffer &dfuQueueName)
{
// Using the first queue for now.
#ifdef _CONTAINERIZED
    Owned<IPropertyTreeIterator> dfuQueues = getComponentConfigSP()->getElements("dfuQueues");
    ForEach(*dfuQueues)
    {
        const char *dfuName = dfuQueues->query().queryProp("@name");
        if (!isEmptyString(dfuName))
        {
            getDfuQueueName(dfuQueueName, dfuName);
            break;
        }
    }
#else
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    Owned<IConstEnvironment> env = factory->openEnvironment();

    StringBuffer xpath ("Software/DfuServerProcess");
    Owned<IPropertyTree> root = &env->getPTree();
    Owned<IPropertyTreeIterator> targets = root->getElements(xpath.str());
    ForEach(*targets)
    {
        IPropertyTree &target = targets->query();
        if (target.hasProp("@queue"))
            dfuQueueName.set(target.queryProp("@queue"));
    }
#endif
}

static void dfuCopy(const IPropertyTree *directories, IDFUWorkUnit *publisherWu, IUserDescriptor *user, const char *sourceLogicalName, const char *destLogicalName, const char *destPlane, const char *srcLocation, bool supercopy, bool overwrite, bool preserveCompression, bool nosplit, bool useRemoteStorage)
{
    if(!publisherWu)
        throw makeStringException(-1, "Failed to create Publisher DFU Workunit.");
    if(isEmptyString(sourceLogicalName))
        throw makeStringException(-1, "Source logical file not specified.");
    if(isEmptyString(destLogicalName))
        throw makeStringException(-1, "Destination logical file not specified.");
    if(isEmptyString(destPlane))
        throw makeStringException(-1, "Destination node group not specified.");

    PROGLOG("Copy from %s[%s]  %s to %s", (!isEmptyString(srcLocation) && useRemoteStorage) ? "remote" : "", isEmptyString(srcLocation) ? "local" : srcLocation, sourceLogicalName, destLogicalName);

    StringBuffer destFolder, destTitle, defaultFolder, defaultReplicateFolder;
    DfuParseLogicalPath(directories, destLogicalName, destPlane, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

    CDfsLogicalFileName logicalName;
    logicalName.set(sourceLogicalName);
    if (!isEmptyString(srcLocation))
    {
        if (useRemoteStorage)
        {
            StringBuffer remoteSpec;
            VStringBuffer lfn("remote::%s::", srcLocation);
            if (logicalName.isRemote())
                logicalName.getRemoteSpec(remoteSpec, lfn);
            else
            {
                logicalName.clearForeign();
                logicalName.get(lfn);
            }
            logicalName.set(lfn);
            sourceLogicalName = logicalName.get();
        }
        else
        {
            SocketEndpoint ep(srcLocation);
            if (ep.isNull())
                throw MakeStringException(-1, "ReferencedFile Copy %s: cannot resolve SourceDali network IP from %s.", sourceLogicalName, srcLocation);

            logicalName.setForeign(ep,false);
        }
    }
    Owned<IDistributedFile> file = wsdfs::lookup(logicalName, user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser, INFINITE);
    if (!file)
        throw MakeStringException(-1, "ReferencedFile failed to find file: %s", logicalName.get());

    if (supercopy)
    {
        if (!file->querySuperFile())
            supercopy = false;
    }
    else if (file->querySuperFile() && (file->querySuperFile()->numSubFiles() > 1) && isFileKey(file))
        supercopy = true;

    Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
    Owned<IDFUWorkUnit> wu = factory->createPublisherSubTask(publisherWu);
    wu->setJobName(destLogicalName);

    if (user)
    {
        StringBuffer username;
        wu->setUser(user->getUserName(username).str());
    }

    wu->setClusterName(destPlane);
    if (supercopy)
        wu->setCommand(DFUcmd_supercopy);
    else
        wu->setCommand(DFUcmd_copy);

    IDFUfileSpec *wuFSpecSource = wu->queryUpdateSource();
    IDFUfileSpec *wuFSpecDest = wu->queryUpdateDestination();
    IDFUoptions *wuOptions = wu->queryUpdateOptions();
    wuFSpecSource->setLogicalName(sourceLogicalName);
    if (!isEmptyString(srcLocation))
    {
        if (!useRemoteStorage)
        {
            SocketEndpoint ep(srcLocation);
            wuFSpecSource->setForeignDali(ep);
        }

        if (user)
        {
            StringBuffer srcUserName;
            user->getUserName(srcUserName);
            if(!srcUserName.isEmpty())
                wuFSpecSource->setForeignUser(srcUserName, "");
        }
    }
    wuFSpecDest->setLogicalName(destLogicalName);
    wuOptions->setOverwrite(overwrite);
    wuOptions->setPreserveCompression(preserveCompression);
    wuOptions->setNoSplit(nosplit);

    setRoxieClusterPartDiskMapping(destPlane, defaultFolder.str(), defaultReplicateFolder.str(), supercopy, wuFSpecDest, wuOptions);
    wuFSpecDest->setWrap(true); // roxie always wraps
    if (!supercopy)
        wuOptions->setSuppressNonKeyRepeats(true); // **** only repeat last part when src kind = key

    submitDFUWorkUnit(wu.getClear());
}

bool ReferencedFile::needsCopying(bool cloneForeign) const
{
    if ((flags & RefFileCloned) || (flags & RefFileSuper) || (flags & RefFileInPackage))
        return false;
    if ((flags & RefFileLFNForeign) && !cloneForeign)
        return false;
    if (!(flags & (RefFileResolvedForeign | RefFileResolvedRemote | RefFileLFNForeign | RefFileLFNRemote | RefFileNotOnCluster)))
        return false;
    return true;
}

void ReferencedFile::cloneInfo(const IPropertyTree *directories, IDFUWorkUnit *publisherWu, unsigned updateFlags, IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, const char *dfu_queue)
{
    if (!needsCopying(cloneForeign))
        return;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;

    try
    {
        StringBuffer srcLFN;
        if (filePrefix.length())
            srcLFN.append(filePrefix.str()).append("::");
        srcLFN.append(logicalName.str());

        bool dfucopy = (updateFlags & DFU_UPDATEF_COPY)!=0;
        if (!dfucopy)
            //Whether remote or on a local plane if we get here the file is not on a plane that roxie considers an direct access plane, so if we're in copy data mode the the file will be copied
            helper->cloneRoxieSubFile(srcLFN, srcCluster, logicalName, dstCluster, filePrefix, redundancy, channelsPerNode, replicateOffset, defReplicateFolder, user, daliip, remoteStorage, updateFlags, false);
        else
            dfuCopy(directories, publisherWu, user, srcLFN, logicalName, dstCluster, remoteStorage.isEmpty() ? daliip : remoteStorage, false, (updateFlags & DFU_UPDATEF_OVERWRITE)!=0, true, false, !remoteStorage.isEmpty());

        flags |= RefFileCloned;
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e, "ReferencedFile ");
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("ReferencedFile Unknown error copying file info for [%s::] %s, from %s on dfs-dali %s", filePrefix.str(), logicalName.str(), fileSrcCluster.length() ? fileSrcCluster.get() : "*", daliip.str());
    }
}

void ReferencedFile::cloneSuperInfo(IDFUWorkUnit *publisherWu, unsigned updateFlags, ReferencedFileList *list, IUserDescriptor *user, INode *remote)
{
    if ((flags & RefFileCloned) || (flags & RefFileInPackage) || !(flags & RefFileSuper) || !(flags & (RefFileResolvedForeign | RefFileResolvedRemote)))
        return;

    try
    {
        Owned<IPropertyTree> tree = getFileOrProvidedForeignFileTree(user, remote, NULL);
        if (!tree)
            return;

        IDistributedFileDirectory &dir = queryDistributedFileDirectory();
        Owned<IDistributedFile> df = dir.lookup(logicalName.str(), user, AccessMode::tbdRead, false, false, nullptr, defaultPrivilegedUser);
        if(df)
        {
            if (!(updateFlags & DALI_UPDATEF_SUPERFILES))
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
                    subref->cloneSuperInfo(publisherWu, updateFlags, list, user, remote);
            }
            if (name && *name)
                superfile->addSubFile(name, false, NULL, false);
        }
    }
    catch (IException *e)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG(e, "ReferencedFile ");
        e->Release();
    }
    catch (...)
    {
        flags |= RefFileCopyInfoFailed;
        DBGLOG("ReferencedFile Unknown error copying superfile info for %s", logicalName.str());
    }
}

class ReferencedFileIterator : implements IReferencedFileIterator, public CInterface
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

void ReferencedFileList::ensureFile(const char *ln, unsigned flags, const char *pkgid, bool noDfsResolution, const StringArray *subfileNames, const char *daliip, const char *srcCluster, const char *prefix, const char *remoteStorageName)
{
    if (!allowForeign && checkForeign(ln))
        throw MakeStringException(-1, "Foreign file not allowed%s: %s", (flags & RefFileInPackage) ? " (declared in package)" : "", ln);

    Owned<ReferencedFile> file = new ReferencedFile(ln, daliip, srcCluster, prefix, false, flags, pkgid, noDfsResolution, allowSizeCalc, remoteStorageName);
    if (!file->logicalName.length())
        return;
    if (subfileNames)
        file->appendSubFileNames(*subfileNames);
    ReferencedFile *existing = map.getValue(file->getLogicalName());
    if (existing)
        existing->flags |= flags;
    else
    {
        const char *refln = file->getLogicalName();
        // NOTE: setValue links its parameter
        map.setValue(refln, file);
    }
}

void ReferencedFileList::addFile(const char *ln, const char *daliip, const char *srcCluster, const char *prefix, const char *remoteStorageName)
{
    ensureFile(ln, 0, NULL, false, nullptr, daliip, srcCluster, prefix, remoteStorageName);
}

void ReferencedFileList::addFiles(StringArray &files)
{
    ForEachItemIn(i, files)
        addFile(files.item(i));
}

void ReferencedFileList::addFileFromSubFile(IPropertyTree &subFile, const char *ip, const char *cluster, const char *prefix, const char *remoteStorageName)
{
    addFile(subFile.queryProp("@value"), ip, cluster, prefix, remoteStorageName);
}

void ReferencedFileList::addFilesFromSuperFile(IPropertyTree &superFile, const char *_ip, const char *_cluster, const char *_prefix, const char *ancestorRemoteStorage)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    StringBuffer effectiveRemoteStorage;
    splitDerivedDfsLocationOrRemote(superFile.queryProp("@daliip"), cluster, ip, prefix, nullptr, _cluster, _ip, _prefix,
                                    superFile.queryProp("@remoteStorage"), effectiveRemoteStorage, ancestorRemoteStorage);
    if (superFile.hasProp("@sourceCluster"))
        cluster.set(superFile.queryProp("@sourceCluster"));

    Owned<IPropertyTreeIterator> subFiles = superFile.getElements("SubFile[@value]");
    ForEach(*subFiles)
        addFileFromSubFile(subFiles->query(), ip, cluster, prefix, effectiveRemoteStorage);
}

void ReferencedFileList::addFilesFromPackage(IPropertyTree &package, const char *_ip, const char *_cluster, const char *_prefix, const char *ancestorRemoteStorage)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    StringBuffer effectiveRemoteStorage;
    splitDerivedDfsLocationOrRemote(package.queryProp("@daliip"), cluster, ip, prefix, nullptr, _cluster, _ip, _prefix,
                                    package.queryProp("@remoteStorage"), effectiveRemoteStorage, ancestorRemoteStorage);
    if (package.hasProp("@sourceCluster"))
        cluster.set(package.queryProp("@sourceCluster"));

    Owned<IPropertyTreeIterator> supers = package.getElements("SuperFile");
    ForEach(*supers)
        addFilesFromSuperFile(supers->query(), ip, cluster, prefix, effectiveRemoteStorage);
}

void ReferencedFileList::addFilesFromPackageMap(IPropertyTree *pm)
{
    StringBuffer ip;
    StringBuffer cluster;
    StringBuffer prefix;
    StringBuffer remoteStorageName;
    const char *srcCluster = pm->queryProp("@sourceCluster");
    splitDerivedDfsLocationOrRemote(pm->queryProp("@daliip"), cluster, ip, prefix, srcCluster, srcCluster, nullptr, nullptr,
                                    pm->queryProp("@remoteStorage"), remoteStorageName, nullptr);
    Owned<IPropertyTreeIterator> packages = pm->getElements("Package");
    ForEach(*packages)
        addFilesFromPackage(packages->query(), ip, cluster, prefix, remoteStorageName);
    packages.setown(pm->getElements("Part/Package"));
    ForEach(*packages)
        addFilesFromPackage(packages->query(), ip, cluster, prefix, remoteStorageName);
}

bool ReferencedFileList::addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg)
{
    SummaryMap files;
    if (cw->getSummary(SummaryType::ReadFile, files) &&
        cw->getSummary(SummaryType::ReadIndex, files))
    {
        for (const auto& [lName, summaryFlags] : files)
        {
            const char *logicalName = lName.c_str();
            StringArray subfileNames;
            unsigned flags = (summaryFlags & SummaryFlags::IsOpt) ? RefFileOptional : RefFileNotOptional;
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
                            ensureFile(subfile, RefSubFile | RefFileInPackage, pkgid, false, nullptr);
                            subfileNames.append(subfile);
                        }
                    }
                }
                ensureFile(logicalName, flags, pkgid, pkg->isCompulsory(), &subfileNames);
            }
            else
                ensureFile(logicalName, flags, NULL, false, &subfileNames);
        }
    }
    else
    {
        Owned<IConstWUGraphIterator> graphs = &cw->getGraphs(GraphTypeActivities);
        ForEach(*graphs)
        {
            Owned <IPropertyTree> xgmml = graphs->query().getXGMMLTree(false, false);
            Owned<IPropertyTreeIterator> iter = xgmml->getElements("//node[att/@name='_*ileName']");
            ForEach(*iter)
            {
                IPropertyTree &node = iter->query();
                bool isOpt = false;
                const char *logicalName = node.queryProp("att[@name='_fileName']/@value");
                if (!logicalName)
                    logicalName = node.queryProp("att[@name='_indexFileName']/@value");
                if (!logicalName)
                    continue;

                isOpt = node.getPropBool("att[@name='_isIndexOpt']/@value");
                if (!isOpt)
                    isOpt = node.getPropBool("att[@name='_isOpt']/@value");

                ThorActivityKind kind = (ThorActivityKind) node.getPropInt("att[@name='_kind']/@value", TAKnone);
                //not likely to be part of roxie queries, but for forward compatibility:
                if(kind==TAKdiskwrite || kind==TAKspillwrite || kind==TAKindexwrite || kind==TAKcsvwrite || kind==TAKxmlwrite || kind==TAKjsonwrite)
                    continue;
                if (node.getPropBool("att[@name='_isSpill']/@value") ||
                    node.getPropBool("att[@name='_isTransformSpill']/@value"))
                    continue;
                StringArray subfileNames;
                unsigned flags = isOpt ? RefFileOptional : RefFileNotOptional;
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
                                ensureFile(subfile, RefSubFile | RefFileInPackage, pkgid, false, nullptr);
                                subfileNames.append(subfile);
                            }
                        }
                    }
                    ensureFile(logicalName, flags, pkgid, pkg->isCompulsory(), &subfileNames);
                }
                else
                    ensureFile(logicalName, flags, NULL, false, &subfileNames);
            }
        }
    }
    return pkg ? pkg->isCompulsory() : false;
}

bool ReferencedFileList::addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid)
{
    const IHpccPackage *pkg = NULL;
    if (pm && queryid && *queryid)
    {
        pkg = pm->matchPackage(queryid);
    }
    return addFilesFromQuery(cw, pkg);
}

void ReferencedFileList::addFilesFromWorkUnit(IConstWorkUnit *cw)
{
    addFilesFromQuery(cw, NULL, NULL);
}

void ReferencedFileList::resolveSubFiles(StringArray &subfiles, const StringArray &locations, bool checkLocalFirst, bool trackSubFiles, bool resolveLFNForeign)
{
    StringArray childSubFiles;
    ForEachItemIn(i, subfiles)
    {
        const char *lfn = subfiles.item(i);
        if (!allowForeign && checkForeign(lfn))
            throw MakeStringException(-1, "Foreign sub file not allowed: %s", lfn);

        Owned<ReferencedFile> file = new ReferencedFile(lfn, NULL, NULL, NULL, true, 0, NULL, false, allowSizeCalc);
        if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
        {
            if (remoteStorage.isEmpty())
                file->resolveLocalOrForeign(locations, srcCluster, user, remote, remotePrefix, checkLocalFirst, &childSubFiles, trackSubFiles, resolveLFNForeign);
            else
                file->resolveLocalOrRemote(locations, srcCluster, user, remoteStorage, remotePrefix, checkLocalFirst, &childSubFiles, trackSubFiles, resolveLFNForeign);
            const char *ln = file->getLogicalName();
            // NOTE: setValue links its parameter
            map.setValue(ln, file);
        }
    }
    if (childSubFiles.length())
        resolveSubFiles(childSubFiles, locations, checkLocalFirst, trackSubFiles, resolveLFNForeign);
}

void ReferencedFileList::resolveFiles(const StringArray &locations, const char *remoteLocation, const char *_remotePrefix, const char *_srcCluster, bool checkLocalFirst, bool expandSuperFiles, bool trackSubFiles, bool resolveLFNForeign, bool useRemoteStorage)
{
    StringArray subfiles;
    srcCluster.set(_srcCluster);
    remotePrefix.set(_remotePrefix);

    ReferencedFileIterator files(this);

    // For use when expandSuperFiles=true
    if (useRemoteStorage)
        remoteStorage.set(remoteLocation);

    ForEach(files)
    {
        ReferencedFile &file = files.queryObject();
        if (file.daliip.isEmpty() && (!file.remoteStorage.isEmpty() || useRemoteStorage))
        {
            if (!user)
                user.setown(createUserDescriptor());

            if (file.remoteStorage.isEmpty()) // Can be set at multiple levels in a packagemap
                file.remoteStorage.set(remoteLocation); // Top-level remoteLocation has lowest precedence, used if nothing set in packagemap

            DBGLOG("ReferencedFileList resolving remote storage file at %s", nullText(file.remoteStorage));
            file.resolveLocalOrRemote(locations, srcCluster, user, file.remoteStorage, remotePrefix, checkLocalFirst, expandSuperFiles ? &subfiles : NULL, trackSubFiles, resolveLFNForeign);
        }
        else
        {
            // The remoteLocation is a daliip when useRemoteStorage is false
            const char *passedDaliip = !useRemoteStorage ? remoteLocation : nullptr;
            if (!isEmptyString(passedDaliip) || !file.daliip.isEmpty())
                DBGLOG("ReferencedFileList resolving remote dali file at %s", isEmptyString(passedDaliip) ? nullText(file.daliip) : passedDaliip);
            else
                DBGLOG("ReferencedFileList resolving local file (no daliip or remote storage)");
            // Otherwise, passing nullptr for remote allows resolveLocalOrForeign to use ReferencedFile.daliip with
            // the matching ReferencedFile.remotePrefix instead of the ReferencedFileList.remotePrefix passed in here.
            remote.setown(!isEmptyString(passedDaliip) ? createINode(passedDaliip, 7070) : nullptr);
            file.resolveLocalOrForeign(locations, srcCluster, user, remote, remotePrefix, checkLocalFirst, expandSuperFiles ? &subfiles : NULL, trackSubFiles, resolveLFNForeign);
        }

        if (expandSuperFiles)
            resolveSubFiles(subfiles, locations, checkLocalFirst, trackSubFiles, resolveLFNForeign);
    }
}

bool ReferencedFileList::filesNeedCopying(bool cloneForeign)
{
    ReferencedFileIterator files(this);
    ForEach(files)
    {
        if (files.queryObject().needsCopying(cloneForeign))
            return true;
    }
    return false;
}

void ReferencedFileList::cloneFileInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder)
{
    Owned<IDFUWorkUnit> publisher;
    bool dfucopy = 0 != (updateFlags & DFU_UPDATEF_COPY);
    bool needToCopyPhysicalFiles = filesNeedCopying(cloneForeign);

    if (dfucopy)
    {
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        if (!needToCopyPhysicalFiles)
        {
            if (!publisherWuid.isEmpty())
            {
                publisher.setown(factory->updateWorkUnit(publisherWuid, true));
                if (publisher)
                {
                    publisher->setJobName(jobName.isEmpty() ? "copy published files" : jobName.get());
                    IDFUprogress *progress = publisher->queryUpdateProgress();
                    if (progress)
                        progress->setState(DFUstate_finished); //don't just delete because empty, automated systems might be tracking
                    publisher->commit();
                }
                publisher.clear();
            }
        }
        else
        {
            StringBuffer dfuQueueName(dfu_queue);
            if (!dfuQueueName)
                getDefaultDFUName(dfuQueueName);

            if (publisherWuid.isEmpty())
                publisher.setown(factory->createPublisherWorkUnit());
            else
            {
                //Publisher WUIDs can be preallocated in order to provide them to the user early in the process, but only newly created publisher workunits can be used
                publisher.setown(factory->updateWorkUnit(publisherWuid, true));
                if(!publisher)
                    throw makeStringException(-1, "Failed to open preallocated Publisher DFU Workunit.");
                if (publisher->queryProgress()->getState()!=DFUstate_unknown)
                    throw makeStringException(-1, "Cannot clone files by reusing a previously used publisher workunit.");
            }

            publisher->setQueue(dfuQueueName);
            publisher->setJobName(jobName.isEmpty() ? "copy published files" : jobName.get());
            publisherWuid.set(publisher->queryId());

            if (keyCompression)
                publisher->queryUpdateOptions()->setKeyCompression(keyCompression);
        }
    }

    ReferencedFileIterator files(this);
    if (needToCopyPhysicalFiles)
    {
        IPropertyTree *directories = nullptr;
#ifndef _CONTAINERIZED
        Owned<IPropertyTree> envtree = getHPCCEnvironment();
        if (envtree)
            directories = envtree->queryPropTree("Software/Directories");
#endif
        ForEach(files)
            files.queryObject().cloneInfo(directories, publisher, updateFlags, helper, user, dstCluster, srcCluster, cloneForeign, redundancy, channelsPerNode, replicateOffset, defReplicateFolder, dfu_queue);
    }
    if (cloneSuperInfo)
        ForEach(files)
            files.queryObject().cloneSuperInfo(publisher, updateFlags, this, user, remote);
}

void ReferencedFileList::cloneRelationships()
{
    if (!remote || remote->endpoint().isNull())
        return;

    StringBuffer addr;
    remote->endpoint().getEndpointHostText(addr);
    IDistributedFileDirectory &dir = queryDistributedFileDirectory();
    ReferencedFileIterator files(this);
    ForEach(files)
    {
        ReferencedFile &file = files.queryObject();
        if (!(file.getFlags() & (RefFileResolvedForeign | RefFileResolvedRemote)))
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

IReferencedFileList *createReferencedFileList(const char *user, const char *pw, bool allowForeignFiles, bool allowFileSizeCalc, const char *jobname)
{
    return new ReferencedFileList(user, pw, allowForeignFiles, allowFileSizeCalc, jobname);
}

IReferencedFileList *createReferencedFileList(IUserDescriptor *user, bool allowForeignFiles, bool allowFileSizeCalc, const char *jobname)
{
    return new ReferencedFileList(user, allowForeignFiles, allowFileSizeCalc, jobname);
}
