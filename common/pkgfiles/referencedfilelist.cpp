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

#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include <vector>
#endif

#define WF_LOOKUP_TIMEOUT (1000*15)  // 15 seconds

// Build reference file list:
//   for each workunit file:
//     add the logical name and its package flags to the list
//   for each package-map Package or Part/Package:
//     inherit the map scope (source cluster, foreign Dali, remote storage, prefix)
//     override inherited scope with explicit package values
//     add package files and declared superfiles using that effective scope
//   for each listed file:
//     apply any file-level scope or embedded remote/foreign qualifier
//     if checkLocalFirst and a local copy exists, use it
//     else resolve through foreign Dali, remote storage, or local DFS
//     if superfile expansion is enabled, qualify children with their scope,
//       add missing children to the list, and recursively resolve each new child
//       (the list acts as the visited set, so duplicates and cycles stop)
//   if cloning is requested:
//     copy file metadata and recursively rebuild nested superfile relationships
//     (mark each superfile cloned before visiting its children)
//
// NOTE: Packages are not traversed recursively.  The daliip/remoteStorage options only apply to
//       that package, not to any Base packages it may reference.

static bool checkForeign(const char *lfn)
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
        cluster.append(baseCluster);
        ip.clear();
        return;
    }

    ip.append(baseIP);
    cluster.append(baseCluster);
    prefix.append(basePrefix);
    effectiveRemoteStorage.append(baseRemoteStorage);
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

static StringBuffer &makeForeignLFN(StringBuffer &foreignLfn, const char *daliip, const char *prefix, const char *lfn)
{
    foreignLfn.append("foreign::").append(daliip).append("::");
    if (!isEmptyString(prefix))
        foreignLfn.append(prefix).append("::");
    return foreignLfn.append(lfn);
}

//---------------------------------------------------------------------------------------------------------------------

class ReferencedFileList;

// Exposes only the local metadata used here, avoiding a mock of the broad Dali file interface.
interface IRefLocalFile : extends IInterface
{
    virtual bool isSuperFile() const = 0;
    virtual void appendSubFileNames(StringArray &names) const = 0;
    virtual unsigned findCluster(const char *name) const = 0;
    virtual __int64 getFileSize(bool calcSize) const = 0;
    virtual unsigned numParts() const = 0;
};

// Centralizes external file lookups so resolution can be tested without contacting Dali or remote storage.
interface IRefFileResolver : extends IInterface
{
    // Lookup results are returned with one owned reference.
    virtual IRefLocalFile *lookupLocal(const char *logicalName, IUserDescriptor *user, bool calcSize) = 0;
    virtual IPropertyTree *lookupForeign(const char *logicalName, IUserDescriptor *user, INode *daliNode, const char *prefix) = 0;
    virtual IPropertyTree *lookupRemoteStorage(const char *remoteLfn, IUserDescriptor *user) = 0;
};

class CDistributedRefLocalFile : implements IRefLocalFile, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CDistributedRefLocalFile(IDistributedFile *_file)
    {
        file.setown(_file);
    }

    virtual bool isSuperFile() const override
    {
        return file->querySuperFile() != nullptr;
    }

    virtual void appendSubFileNames(StringArray &names) const override
    {
        IDistributedSuperFile *super = file->querySuperFile();
        if (!super)
            return;
        Owned<IDistributedFileIterator> it = super->getSubFileIterator(true);
        ForEach(*it)
        {
            StringBuffer name;
            it->query().getLogicalName(name);
            names.append(name);
        }
    }

    virtual unsigned findCluster(const char *name) const override
    {
        return file->findCluster(name);
    }

    virtual __int64 getFileSize(bool calcSize) const override
    {
        return file->getFileSize(calcSize, false);
    }

    virtual unsigned numParts() const override
    {
        return file->numParts();
    }

private:
    Owned<IDistributedFile> file;
};

class CDistributedRefFileResolver : implements IRefFileResolver, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    virtual IRefLocalFile *lookupLocal(const char *logicalName, IUserDescriptor *user, bool calcSize) override
    {
        AccessMode accessMode = AccessMode::meta;
        if (calcSize)
            accessMode |= AccessMode::readPhysicalMeta;
        Owned<IDistributedFile> file = queryDistributedFileDirectory().lookup(logicalName, user, accessMode, false, false, nullptr, defaultPrivilegedUser);
        if (!file)
            return nullptr;
        return new CDistributedRefLocalFile(file.getClear());
    }

    virtual IPropertyTree *lookupForeign(const char *logicalName, IUserDescriptor *user, INode *daliNode, const char *prefix) override
    {
        if (!daliNode)
            return nullptr;
        StringBuffer remoteLfn;
        if (!isEmptyString(prefix))
            remoteLfn.append(prefix).append("::");
        remoteLfn.append(logicalName);
        return queryDistributedFileDirectory().getFileTree(remoteLfn.str(), user, AccessMode::readLogicalMeta, daliNode, WF_LOOKUP_TIMEOUT, GetFileTreeOpts::none);
    }

    virtual IPropertyTree *lookupRemoteStorage(const char *remoteLfn, IUserDescriptor *user) override
    {
        Owned<wsdfs::IDFSFile> dfsFile = wsdfs::lookupDFSFile(remoteLfn, AccessMode::readLogicalMeta, INFINITE, wsdfs::keepAliveExpiryFrequency, user);
        IPropertyTree *meta = dfsFile ? dfsFile->queryFileMeta() : nullptr;
        return meta ? LINK(meta) : nullptr;
    }
};

static IRefFileResolver *createRefFileResolver()
{
    return new CDistributedRefFileResolver;
}

class ReferencedFile : implements IReferencedFile, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFile(const char *lfn, const char *sourceIP, const char *srcCluster, const char *prefix, bool isSubFile, unsigned _flags, const char *_pkgid, bool noDfs, bool calcSize, const char *remoteStorageName, IRefFileResolver *_fileResolver)
    : pkgid(_pkgid), fileSize(0), numParts(0), flags(_flags), noDfsResolution(noDfs), calcFileSize(calcSize)
    {
        fileResolver.set(_fileResolver);
        {
            //Scope ensures strings are assigned
            StringAttrBuilder logicalNameText(logicalName), daliipText(daliip), remoteStorageText(remoteStorage);
            logicalNameText.set(skipForeignOrRemote(lfn, &daliipText, &remoteStorageText)).toLowerCase();
        }
        if (remoteStorage.length())
        {
            flags |= RefFileLFNRemote;
            hasExplicitRemoteLfn = true;
        }
        else if (daliip.length())
            flags |= RefFileLFNForeign;
        else if (!isEmptyString(remoteStorageName))
        {
            // can be declared in packagemap at different scopes
            remoteStorage.set(remoteStorageName);
            flags |= RefFileLFNRemote;
        }
        else
            daliip.set(sourceIP); // can be declared in packagemap at different scopes
        fileSrcCluster.set(srcCluster);
        filePrefix.set(prefix);
        if (isSubFile)
            flags |= RefSubFile;
    }

    void reset()
    {
        flags &= ~(RefFileNotOnCluster | RefFileNotFound | RefFileResolvedForeign | RefFileResolvedRemote | RefFileCopyInfoFailed | RefFileCloned | RefFileNotOnSource); //these flags are calculated during resolve
        // Resolution scope belongs to one resolveFiles() pass and must not leak into the next one.
        resolvedRemoteStorage.clear();
        resolvedRemotePrefix.clear();
        hasResolvedRemoteStorage = false;
        hasResolvedRemotePrefix = false;
    }

    IPropertyTree *getRemoteStorageFileTree(IUserDescriptor *user, const char *remoteStorageName, const char *remotePrefix);
    IPropertyTree *getForeignFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);
    IPropertyTree *getFileOrProvidedForeignFileTree(IUserDescriptor *user, INode *remote, const char *remotePrefix);

    void processLocalFileInfo(IRefLocalFile *file, const StringArray &locations, const char *srcCluster, StringArray *subfiles);
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
    virtual void cloneInfo(IDFUWorkUnit *publisherWu, StringArray &subtaskWuids, unsigned updateFlags, IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, const char *dfu_queue);
    void cloneSuperInfo(unsigned updateFlags, ReferencedFileList *list, IUserDescriptor *user, INode *remote, IPropertyTree *deferredSuperFiles);
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
    const char *queryEffectiveRemoteStorage() const
    {
        return hasResolvedRemoteStorage ? resolvedRemoteStorage.str() : remoteStorage.str();
    }
    const char *queryEffectiveRemoteStorage(const char *fallback) const
    {
        if (hasResolvedRemoteStorage)
            return resolvedRemoteStorage.str();
        return hasRemoteStorageScope() ? remoteStorage.str() : fallback;
    }
    bool hasExplicitForeignLfn() const
    {
        return (flags & RefFileLFNForeign) != 0;
    }
    bool hasRemoteStorageScope() const
    {
        return (flags & RefFileLFNRemote) != 0;
    }
    const char *queryEffectivePrefix() const
    {
        if (hasResolvedRemotePrefix)
            return resolvedRemotePrefix.str();
        if (hasExplicitRemoteLfn || hasExplicitForeignLfn())
            return nullptr;
        return filePrefix.str();
    }
    const char *queryEffectivePrefix(const char *fallback) const
    {
        if (hasResolvedRemotePrefix)
            return resolvedRemotePrefix.str();
        if (hasExplicitRemoteLfn || hasExplicitForeignLfn())
            return nullptr;
        return filePrefix.isEmpty() ? fallback : filePrefix.str();
    }
public:
    StringArray subFileNames;
    Linked<IRefFileResolver> fileResolver;
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
    bool trackSubFiles = false;
    bool skipDfuCopy = false;
    bool hasExplicitRemoteLfn = false;
    bool hasResolvedRemoteStorage = false;
    bool hasResolvedRemotePrefix = false;
    StringAttr resolvedRemoteStorage;
    StringAttr resolvedRemotePrefix;
};

class ReferencedFileList : implements IReferencedFileList, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    ReferencedFileList(const char *username, const char *pw, bool allowForeignFiles, bool allowFileSizeCalc, const char *_jobname)
        : jobName(_jobname), allowForeign(allowForeignFiles), allowSizeCalc(allowFileSizeCalc)
    {
        fileResolver.setown(createRefFileResolver());
        if (username)
        {
            user.setown(createUserDescriptor());
            user->set(username, pw);
        }
    }

    ReferencedFileList(IUserDescriptor *userDesc, bool allowForeignFiles, bool allowFileSizeCalc, const char *_jobname)
        : jobName(_jobname), allowForeign(allowForeignFiles), allowSizeCalc(allowFileSizeCalc)
    {
        fileResolver.setown(createRefFileResolver());
        if (userDesc)
            user.set(userDesc);
    }

    ReferencedFileList(IRefFileResolver *resolver, bool allowForeignFiles, bool allowFileSizeCalc, const char *_jobname)
        : jobName(_jobname), allowForeign(allowForeignFiles), allowSizeCalc(allowFileSizeCalc)
    {
        fileResolver.set(resolver);
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
    virtual void cloneFileInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, bool targetPlaneSpecified) override;

    virtual void cloneRelationships();
    virtual void cloneAllInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, bool targetPlaneSpecified) override
    {
        cloneFileInfo(publisherWuid, dstCluster, updateFlags, helper, cloneSuperInfo, cloneForeign, redundancy, channelsPerNode, replicateOffset, defReplicateFolder, targetPlaneSpecified);
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
    Owned<IRefFileResolver> fileResolver;
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

// Should probably move to dadfs as a global helper function
static bool fileExistsWithinLocations(IRefLocalFile *file, const StringArray &locations)
{
    if (!file)
        return false;
    ForEachItemIn(i, locations)
    {
        const char *name = locations.item(i);
        if (file->findCluster(name)!=NotFound)
            return true;
    }
    return false;
}

void ReferencedFile::processLocalFileInfo(IRefLocalFile *file, const StringArray &locations, const char *srcCluster, StringArray *subfiles)
{
    if (file->isSuperFile())
    {
        flags |= RefFileSuper;
        if (subfiles)
        {
            StringArray names;
            file->appendSubFileNames(names);
            ForEachItemIn(i, names)
            {
                subfiles->append(names.item(i));
                if (trackSubFiles)
                    subFileNames.append(names.item(i));
            }
        }
    }
    else
    {
        flags |= RefSubFile;
        if (!locations.length())
            return;
        if (!fileExistsWithinLocations(file, locations))
            flags |= RefFileNotOnCluster;
        if (fileSrcCluster.length())
            srcCluster=fileSrcCluster;
        if (srcCluster && *srcCluster)
            if (NotFound == file->findCluster(srcCluster))
                flags |= RefFileNotOnSource;
        fileSize = file->getFileSize(calcFileSize);
        numParts = file->numParts();
    }
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
                // SubFile/@name is already the full logical name on the source dali, including any prefix
                if (daliip.length())
                    lfn = makeForeignLFN(foreignLfn, daliip, nullptr, lfn).str();
                subfiles->append(lfn);
                if (trackSubFiles)
                    subFileNames.append(skipForeignOrRemote(lfn, nullptr, nullptr));
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
                // SubFile/@name is already the full logical name on the source dali, including any prefix
                if (!isEmptyString(queryEffectiveRemoteStorage()))
                    lfn = makeRemoteLFN(remoteLfn, queryEffectiveRemoteStorage(), nullptr, lfn).str();
                // Recursive resolution needs the remote-qualified name, while subFileNames is
                // exposed to callers that look up children by their logical name.
                subfiles->append(lfn);
                if (trackSubFiles)
                    subFileNames.append(skipForeignOrRemote(lfn, nullptr, nullptr));
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
    Owned<IRefLocalFile> file = fileResolver->lookupLocal(logicalName, user, calcFileSize);
    if(file)
        processLocalFileInfo(file, locations, srcCluster, subfiles);
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
    return fileResolver->lookupForeign(logicalName, user, daliNode, remotePrefix);
}

IPropertyTree *ReferencedFile::getFileOrProvidedForeignFileTree(IUserDescriptor *user, INode *providedDaliNode, const char *remotePrefix)
{
    if (daliip.length())
    {
        Owned<INode> fileDaliNode;
        fileDaliNode.setown(createINode(daliip));
        return getForeignFileTree(user, fileDaliNode, queryEffectivePrefix());
    }
    if (!providedDaliNode)
        return NULL;
    Owned<IPropertyTree> fileTree = getForeignFileTree(user, providedDaliNode, remotePrefix);
    if (!fileTree)
        return NULL;
    StringAttrBuilder daliipText(daliip);
    providedDaliNode->endpoint().getEndpointHostText(daliipText);
    resolvedRemotePrefix.set(remotePrefix);
    hasResolvedRemotePrefix = true;
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
        Owned<IRefLocalFile> file = fileResolver->lookupLocal(logicalName, user, calcFileSize);
        if(file)
        {
            processLocalFileInfo(file, locations, NULL, subfiles);
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

    Owned<IPropertyTree> metadata = fileResolver->lookupRemoteStorage(remoteLFN, user);
    if (!metadata)
    {
        DBGLOG("RemoteStorage FileMetaTree not found %s [remoteStorage=%s, prefix=%s]", remoteLFN.str(), nullText(remoteStorageName), nullText(remotePrefix));
        return nullptr;
    }
    Owned<IPropertyTree> fileTree = metadata->getPropTree(queryDfsXmlBranchName(DXB_File));
    if (!fileTree)
        fileTree.setown(metadata->getPropTree(queryDfsXmlBranchName(DXB_SuperFile)));
    if (!fileTree)
        DBGLOG("RemoteStorage FileTree not found %s [remoteStorage=%s, prefix=%s]", remoteLFN.str(), nullText(remoteStorageName), nullText(remotePrefix));
    return fileTree.getClear();
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
        Owned<IRefLocalFile> file = fileResolver->lookupLocal(logicalName, user, calcFileSize);
        if(file)
        {
            processLocalFileInfo(file, locations, NULL, subfiles);
            return;
        }
    }
    Owned<IPropertyTree> tree = getRemoteStorageFileTree(user, remoteStorageName, remotePrefix);
    if (tree)
    {
        // Keep request-level scope separate so a later resolveFiles() call can replace it.
        resolvedRemoteStorage.set(remoteStorageName);
        hasResolvedRemoteStorage = true;
        resolvedRemotePrefix.set(remotePrefix);
        hasResolvedRemotePrefix = true;
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

static void dfuCopy(IDFUWorkUnit *publisherWu, StringArray &subtaskWuids, IUserDescriptor *user, const char *sourceLogicalName, const char *destLogicalName, const char *destPlane, const char *srcLocation, bool supercopy, bool overwrite, bool preserveCompression, bool nosplit, bool useRemoteStorage)
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
    DfuParseLogicalPath(destLogicalName, destPlane, destFolder, destTitle, defaultFolder, defaultReplicateFolder);

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
    Owned<IDistributedFile> file = wsdfs::lookup(logicalName, user, AccessMode::readLogicalMeta, false, false, nullptr, defaultPrivilegedUser, INFINITE);
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

    IDFUoptions *parentOptions = publisherWu->queryUpdateOptions();

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

    const char * keyCompression = parentOptions->queryKeyCompression();
    if (!isEmptyString(keyCompression))
        wuOptions->setKeyCompression(keyCompression);

    setRoxieClusterPartDiskMapping(destPlane, defaultFolder.str(), defaultReplicateFolder.str(), supercopy, wuFSpecDest, wuOptions);
    wuFSpecDest->setWrap(true); // roxie always wraps
    if (!supercopy)
        wuOptions->setSuppressNonKeyRepeats(true); // **** only repeat last part when src kind = key

    wu->commit();
    subtaskWuids.append(wu->queryId());
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

void ReferencedFile::cloneInfo(IDFUWorkUnit *publisherWu, StringArray &subtaskWuids, unsigned updateFlags, IDFUhelper *helper, IUserDescriptor *user, const char *dstCluster, const char *srcCluster, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, const char *dfu_queue)
{
    if (!needsCopying(cloneForeign) || skipDfuCopy)
        return;
    if (fileSrcCluster.length())
        srcCluster = fileSrcCluster;

    try
    {
        const char *effectivePrefix = queryEffectivePrefix();
        const char *effectiveRemoteStorage = queryEffectiveRemoteStorage();
        StringBuffer srcLFN;
        if (!isEmptyString(effectivePrefix))
            srcLFN.append(effectivePrefix).append("::");
        srcLFN.append(logicalName.str());

        bool dfucopy = (updateFlags & DFU_UPDATEF_COPY)!=0;
        if (!dfucopy)
            //Whether remote or on a local plane if we get here the file is not on a plane that roxie considers an direct access plane, so if we're in copy data mode the the file will be copied
            helper->cloneRoxieSubFile(srcLFN, srcCluster, logicalName, dstCluster, effectivePrefix, redundancy, channelsPerNode, replicateOffset, defReplicateFolder, user, daliip, effectiveRemoteStorage, updateFlags, false);
        else
            dfuCopy(publisherWu, subtaskWuids, user, srcLFN, logicalName, dstCluster, isEmptyString(effectiveRemoteStorage) ? daliip.str() : effectiveRemoteStorage, false, (updateFlags & DFU_UPDATEF_OVERWRITE)!=0, true, false, !isEmptyString(effectiveRemoteStorage));

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

void ReferencedFile::cloneSuperInfo(unsigned updateFlags, ReferencedFileList *list, IUserDescriptor *user, INode *remote, IPropertyTree *deferredSuperFiles)
{
    if ((flags & RefFileCloned) || (flags & RefFileInPackage) || !(flags & RefFileSuper) || !(flags & (RefFileResolvedForeign | RefFileResolvedRemote)))
        return;

    try
    {
        Owned<IPropertyTree> tree;
        if (flags & RefFileResolvedRemote)
            tree.setown(getRemoteStorageFileTree(user, queryEffectiveRemoteStorage(), queryEffectivePrefix()));
        else
            tree.setown(getFileOrProvidedForeignFileTree(user, remote, nullptr));
        if (!tree)
            return;

        if (!(updateFlags & DALI_UPDATEF_SUPERFILES))
        {
            Owned<IDistributedFile> df = queryDistributedFileDirectory().lookup(logicalName.str(), user, AccessMode::readLogicalMeta, false, false, nullptr, defaultPrivilegedUser);
            if (df)
                return;
        }

        flags |= RefFileCloned;
        Owned<IPropertyTree> superFile = createPTree("SuperFile");
        superFile->setProp("@name", logicalName.str());
        Owned<IPropertyTreeIterator> subfiles = tree->getElements("SubFile");
        ForEach(*subfiles)
        {
            const char *name = subfiles->query().queryProp("@name");
            const char *childName = name;
            if (list)
            {
                //ensure superfile in superfile is cloned, before add
                // SubFile/@name is already the full logical name, matching the key used when the child was expanded
                ReferencedFile *subref = nullptr;
                if (!isEmptyString(name))
                    subref = list->map.getValue(name);
                if (subref)
                {
                    subref->cloneSuperInfo(updateFlags, list, user, remote, deferredSuperFiles);
                    childName = subref->getLogicalName();
                }
            }
            if (!isEmptyString(childName))
                superFile->addPropTree("SubFile")->setProp("@name", childName);
        }
        // Added after the recursion above so nested superfiles are created before this one
        deferredSuperFiles->addPropTree("SuperFile", superFile.getClear());
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

static void markDFUSubmissionFailed(const char *wuid)
{
    try
    {
        Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
        Owned<IDFUWorkUnit> wu = factory->updateWorkUnit(wuid, true);
        if (!wu)
        {
            PROGLOG("ReferencedFile failed to reopen child DFU workunit %s after submission failure", wuid);
            return;
        }
        wu->queryUpdateProgress()->setState(DFUstate_failed, "Failed to submit child DFU workunit");
    }
    catch (IException *e)
    {
        DBGLOG(e, "ReferencedFile failed to record child DFU submission failure");
        e->Release();
    }
    catch (...)
    {
        DBGLOG("ReferencedFile failed to record child DFU submission failure for %s", wuid);
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
    if (isEmptyString(ln))
        return;

    if (!allowForeign && checkForeign(ln))
        throw MakeStringException(-1, "Foreign file not allowed%s: %s", (flags & RefFileInPackage) ? " (declared in package)" : "", ln);

    Owned<ReferencedFile> file = new ReferencedFile(ln, daliip, srcCluster, prefix, false, flags, pkgid, noDfsResolution, allowSizeCalc, remoteStorageName, fileResolver);
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

    Owned<IPropertyTreeIterator> files = package.getElements("File");
    ForEach(*files)
        addFile(files->query().queryProp("@value"), ip, cluster, prefix, effectiveRemoteStorage);
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

        Owned<ReferencedFile> file = new ReferencedFile(lfn, NULL, NULL, NULL, true, 0, NULL, false, allowSizeCalc, nullptr, fileResolver);
        if (file->logicalName.length() && !map.getValue(file->getLogicalName()))
        {
            // Prefer per-file remoteStorage (extracted from embedded remote:: LFN); when used, the prefix is already baked into logicalName so suppress the list-level remotePrefix to avoid doubling it.
            const char *effectiveRemoteStorage = file->remoteStorage.isEmpty() ? remoteStorage.str() : file->remoteStorage.str();
            const char *effectiveRemotePrefix = file->remoteStorage.isEmpty() ? remotePrefix.str() : nullptr;
            if (isEmptyString(effectiveRemoteStorage))
                file->resolveLocalOrForeign(locations, srcCluster, user, remote, effectiveRemotePrefix, checkLocalFirst, &childSubFiles, trackSubFiles, resolveLFNForeign);
            else
                file->resolveLocalOrRemote(locations, srcCluster, user, effectiveRemoteStorage, effectiveRemotePrefix, checkLocalFirst, &childSubFiles, trackSubFiles, resolveLFNForeign);
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
    StringArray unresolved;
    // Avoid flooding the log with a line per file (and per resolveFiles call, which can be for
    // thousands of files each time a packagemap is added). Only the first few unresolved names are
    // kept for the summary line below; the rest are just counted.
    constexpr unsigned maxNamedFiles = 3;
    unsigned numResolved = 0;
    unsigned numUnresolved = 0;
    unsigned numRemoteStorage = 0;
    unsigned numRemoteDali = 0;
    unsigned numLocal = 0;
    srcCluster.set(_srcCluster);
    remotePrefix.set(_remotePrefix);

    // Superfile expansion inserts into the map, so snapshot files to avoid invalidating its iterator or resolving children twice.
    CIArrayOf<ReferencedFile> filesToResolve;
    {
        ReferencedFileIterator files(this);
        ForEach(files)
            filesToResolve.append(*LINK(&files.queryObject()));
    }

    // remoteStorage is also used while expanding subfiles, so do not carry a remote scope into a later pass.
    remoteStorage.clear();
    if (useRemoteStorage)
        remoteStorage.set(remoteLocation);

    ForEachItemIn(fileIndex, filesToResolve)
    {
        ReferencedFile &file = filesToResolve.item(fileIndex);
        // Clear cached per-resolution scope before queryEffective...() selects the scope for this pass.
        // Resetting each file here is sufficient; a separate pass over all files is unnecessary.
        // The resolver resets again, but that is too late here; reset only clears derived state that this pass recomputes.
        file.reset();
        if (file.daliip.isEmpty() && (file.hasRemoteStorageScope() || useRemoteStorage))
        {
            if (!user)
                user.setown(createUserDescriptor());

            const char *effectiveRemoteStorage = file.queryEffectiveRemoteStorage(remoteLocation);
            const char *effectivePrefix = file.queryEffectivePrefix(remotePrefix);
            file.resolveLocalOrRemote(locations, srcCluster, user, effectiveRemoteStorage, effectivePrefix, checkLocalFirst, expandSuperFiles ? &subfiles : NULL, trackSubFiles, resolveLFNForeign);
        }
        else
        {
            // The remoteLocation is a daliip when useRemoteStorage is false
            const char *passedDaliip = !useRemoteStorage ? remoteLocation : nullptr;
            // Otherwise, passing nullptr for remote allows resolveLocalOrForeign to use ReferencedFile.daliip with
            // the matching ReferencedFile.remotePrefix instead of the ReferencedFileList.remotePrefix passed in here.
            remote.setown(!isEmptyString(passedDaliip) ? createINode(passedDaliip, 7070) : nullptr);
            file.resolveLocalOrForeign(locations, srcCluster, user, remote, remotePrefix, checkLocalFirst, expandSuperFiles ? &subfiles : NULL, trackSubFiles, resolveLFNForeign);
        }

        unsigned fileFlags = file.getFlags();
        if (fileFlags & RefFileNotFound)
        {
            numUnresolved++;
            if (unresolved.ordinality() < maxNamedFiles)
                unresolved.append(file.getLogicalName());
        }
        else
        {
            numResolved++;
            if (fileFlags & RefFileResolvedRemote)
                numRemoteStorage++;
            else if (fileFlags & RefFileResolvedForeign)
                numRemoteDali++;
            else
                numLocal++;
        }

        if (expandSuperFiles)
            resolveSubFiles(subfiles, locations, checkLocalFirst, trackSubFiles, resolveLFNForeign);
    }

    // If there were any files, report a single summary line instead of one per file, naming a
    // manageable number of the unresolved files and counts of the rest.
    if (numUnresolved + numResolved > 0)
    {
        StringBuffer msg;
        msg.appendf("ReferencedFileList%s%s: resolved %u file(s) (%u remote storage, %u remote dali, %u local)",
            jobName.isEmpty() ? "" : " for ", jobName.isEmpty() ? "" : jobName.get(), numResolved, numRemoteStorage, numRemoteDali, numLocal);
        if (numUnresolved)
        {
            msg.appendf(", failed to resolve %u", numUnresolved);
            for (unsigned i = 0; i < unresolved.length(); i++)
                msg.appendf("%s%s", i ? ", " : ": ", unresolved.item(i));
            if (numUnresolved > unresolved.length())
                msg.appendf(" and %u more", numUnresolved - unresolved.length());
        }
        DBGLOG("%s", msg.str());
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

void ReferencedFileList::cloneFileInfo(StringBuffer &publisherWuid, const char *dstCluster, unsigned updateFlags, IDFUhelper *helper, bool cloneSuperInfo, bool cloneForeign, unsigned redundancy, unsigned channelsPerNode, int replicateOffset, const char *defReplicateFolder, bool targetPlaneSpecified)
{
    Owned<IDFUWorkUnit> publisher;
    bool dfucopy = 0 != (updateFlags & DFU_UPDATEF_COPY);
    bool overwrite = (updateFlags & DFU_UPDATEF_OVERWRITE) != 0;
    bool needToCopyPhysicalFiles = false;

    ReferencedFileIterator files(this);
    ForEach(files)
    {
        ReferencedFile &file = files.queryObject();
        file.skipDfuCopy = false;
        if (!file.needsCopying(cloneForeign))
            continue;

        if (dfucopy && !overwrite)
        {
            Owned<IDistributedFile> destination = queryDistributedFileDirectory().lookup(file.logicalName, user, AccessMode::meta, false, false, nullptr, defaultPrivilegedUser);
            // A specified target (including a bare-metal Roxie cluster) must already host the file; otherwise any existing plane is acceptable.
            if (destination)
                file.skipDfuCopy = !targetPlaneSpecified || destination->findCluster(dstCluster) != NotFound;
        }
        if (!file.skipDfuCopy)
            needToCopyPhysicalFiles = true;
    }

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
                    publisher->setJobName(jobName.isEmpty() ? "copy published files" : jobName);
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
            publisher->setJobName(jobName.isEmpty() ? "copy published files" : jobName);
            if (user)
            {
                StringBuffer username;
                publisher->setUser(user->getUserName(username).str());
            }
            publisherWuid.set(publisher->queryId());

            if (keyCompression)
                publisher->queryUpdateOptions()->setKeyCompression(keyCompression);
        }
    }

    StringArray subtaskWuids;
    if (needToCopyPhysicalFiles)
    {
        ForEach(files)
            files.queryObject().cloneInfo(publisher, subtaskWuids, updateFlags, helper, user, dstCluster, srcCluster, cloneForeign, redundancy, channelsPerNode, replicateOffset, defReplicateFolder, dfu_queue);
    }
    Owned<IPropertyTree> deferredSuperFiles = createPTree("DeferredSuperFiles");
    deferredSuperFiles->setPropBool("@allowReplace", (updateFlags & DALI_UPDATEF_SUPERFILES) != 0);
    if (cloneSuperInfo)
        ForEach(files)
            files.queryObject().cloneSuperInfo(updateFlags, this, user, remote, deferredSuperFiles);
    if (deferredSuperFiles->hasProp("SuperFile"))
    {
        // Subfiles being copied do not exist yet, so the publisher creates the superfiles once all copies have finished
        if (publisher && subtaskWuids.ordinality())
            publisher->setDeferredSuperFiles(deferredSuperFiles);
        else if (!createDeferredSuperFiles(*deferredSuperFiles, user))
        {
            Owned<IPropertyTreeIterator> failed = deferredSuperFiles->getElements("SuperFile[@state=\"failed\"]");
            ForEach(*failed)
            {
                ReferencedFile *file = map.getValue(failed->query().queryProp("@name"));
                if (file)
                    file->flags |= RefFileCopyInfoFailed;
            }
        }
    }
    if (publisher)
    {
        publisher->commit();
        // Released first: a failed submission can finalize the publisher on this thread, which needs its lock
        publisher.clear();
        // Wait until the full task count is committed before a fast child can mark the publisher complete.
        ForEachItemIn(i, subtaskWuids)
        {
            const char *subtaskWuid = subtaskWuids.item(i);
            try
            {
                submitDFUWorkUnit(subtaskWuid);
            }
            catch (IException *e)
            {
                PROGLOG("ReferencedFile failed to submit child DFU workunit %s", subtaskWuid);
                DBGLOG(e, "ReferencedFile ");
                e->Release();
                markDFUSubmissionFailed(subtaskWuid);
            }
            catch (...)
            {
                DBGLOG("ReferencedFile unknown error submitting child DFU workunit %s", subtaskWuid);
                markDFUSubmissionFailed(subtaskWuid);
            }
        }
    }
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

#ifdef _USE_CPPUNIT
class CXmlRefLocalFile : implements IRefLocalFile, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CXmlRefLocalFile(IPropertyTree *_tree)
    {
        tree.setown(_tree);
    }

    virtual bool isSuperFile() const override
    {
        return streq(tree->queryName(), queryDfsXmlBranchName(DXB_SuperFile));
    }

    virtual void appendSubFileNames(StringArray &names) const override
    {
        Owned<IPropertyTreeIterator> subfiles = tree->getElements("SubFile");
        ForEach(*subfiles)
            names.append(subfiles->query().queryProp("@name"));
    }

    virtual unsigned findCluster(const char *name) const override
    {
        VStringBuffer xpath("Cluster[@name='%s']", name);
        return tree->hasProp(xpath) ? 0 : NotFound;
    }

    virtual __int64 getFileSize(bool) const override
    {
        return tree->getPropInt64("Attr/@size", 0);
    }

    virtual unsigned numParts() const override
    {
        return tree->getPropInt("@numparts", 0);
    }

private:
    Owned<IPropertyTree> tree;
};

class CXmlRefFileResolver : implements IRefFileResolver, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CXmlRefFileResolver(const char *xml)
    {
        files.setown(createPTreeFromXMLString(xml));
    }

    virtual IRefLocalFile *lookupLocal(const char *logicalName, IUserDescriptor *, bool) override
    {
        localLookups++;
        VStringBuffer filePath("Local/File[@lfn='%s']", logicalName);
        Owned<IPropertyTree> file = files->getPropTree(filePath);
        if (!file)
        {
            VStringBuffer superPath("Local/SuperFile[@lfn='%s']", logicalName);
            file.setown(files->getPropTree(superPath));
        }
        return file ? new CXmlRefLocalFile(file.getClear()) : nullptr;
    }

    virtual IPropertyTree *lookupForeign(const char *logicalName, IUserDescriptor *, INode *daliNode, const char *prefix) override
    {
        foreignLookups++;
        StringBuffer host;
        if (daliNode)
            daliNode->endpoint().getHostText(host);
        VStringBuffer lfn("%s%s%s", isEmptyString(prefix) ? "" : prefix, isEmptyString(prefix) ? "" : "::", logicalName);
        VStringBuffer xpath("Foreign[@dali='%s']/File[@lfn='%s']", host.str(), lfn.str());
        Owned<IPropertyTree> file = files->getPropTree(xpath);
        if (!file)
        {
            VStringBuffer superPath("Foreign[@dali='%s']/SuperFile[@lfn='%s']", host.str(), lfn.str());
            file.setown(files->getPropTree(superPath));
        }
        return file.getClear();
    }

    virtual IPropertyTree *lookupRemoteStorage(const char *remoteLfn, IUserDescriptor *) override
    {
        remoteLookups++;
        VStringBuffer xpath("Remote[@lfn='%s']", remoteLfn);
        Owned<IPropertyTree> metadata = files->getPropTree(xpath);
        return metadata.getClear();
    }

    unsigned localLookups = 0;
    unsigned foreignLookups = 0;
    unsigned remoteLookups = 0;

private:
    Owned<IPropertyTree> files;
};

class ReferencedFileListTests : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(ReferencedFileListTests);
        CPPUNIT_TEST(testResolveCases);
        CPPUNIT_TEST(testConflictingScopesRejected);
        CPPUNIT_TEST(testPackageAndSuperfileScopeConflictsRejected);
        CPPUNIT_TEST(testForeignQualifierRejectedWhenDisabled);
        CPPUNIT_TEST(testForeignSuperfileChildRejectedWhenDisabled);
        CPPUNIT_TEST(testResolutionEarlyReturns);
    CPPUNIT_TEST_SUITE_END();

protected:
    struct ExpectedFile
    {
        const char *name;
        unsigned flags;
        __int64 size;
        unsigned flagsToExclude = 0;
    };

    struct ResolveCase
    {
        const char *name;
        const char *packageMapXml;
        const char *logicalName;
        const char *daliip;
        const char *remotePrefix;
        const char *remoteStorage;
        const char *srcCluster;
        bool expandSuperFiles;
        bool trackSubFiles;
        bool resolveLFNForeign;
        bool useRemoteStorage;
        unsigned minimumLocalLookups;
        unsigned expectedForeignLookups;
        unsigned minimumRemoteLookups;
        std::vector<ExpectedFile> expectedFiles;
        bool allowForeignFiles = true;
        bool checkLocalFirst = false;
        const char *remoteLocation = nullptr;
        unsigned maximumRemoteLookups = 0xffffffffu;
        unsigned maximumLocalLookups = 0xffffffffu;
    };

    static const char *queryFixtureXml()
    {
        return "<Files>"
            "<Local>"
                "<SuperFile lfn='local::super'><SubFile name='local::leaf'/></SuperFile>"
                "<File lfn='local::leaf' numparts='2'><Cluster name='thor'/><Attr size='34'/></File>"
                "<File lfn='precedence' numparts='1'><Cluster name='thor'/><Attr size='81'/></File>"
                "<File lfn='fallback' numparts='1'><Cluster name='thor'/><Attr size='82'/></File>"
                "<File lfn='remotefallback' numparts='1'><Cluster name='thor'/><Attr size='83'/></File>"
                "<File lfn='localofftarget' numparts='1'><Cluster name='archive'/><Attr size='87'/></File>"
            "</Local>"
            "<Foreign dali='127.0.0.1'>"
                "<File lfn='item' numparts='1'><Cluster name='thor'/><Attr size='19'/></File>"
                "<File lfn='mapfile'><Cluster name='mapPlane'/><Attr size='21'/></File>"
                "<File lfn='partfile'><Cluster name='mapPlane'/><Attr size='22'/></File>"
                "<File lfn='map::prefixchild'><Cluster name='mapPlane'/><Attr size='23'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.2'>"
                "<File lfn='pkgfile'><Cluster name='pkgPlane'/><Attr size='31'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.3'>"
                "<File lfn='superchild'><Cluster name='superPlane'/><Attr size='41'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.4'>"
                "<File lfn='scope::embedded'><Cluster name='foreignPlane'/><Attr size='71'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.5'>"
                "<SuperFile lfn='foreignsuper'><SubFile name='scope::child'/></SuperFile>"
            "</Foreign>"
            "<Foreign dali='127.0.0.6'>"
                "<File lfn='precedence'><Cluster name='foreignPlane'/><Attr size='80'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.7'>"
                "<File lfn='scope::inheritedforeign'><Cluster name='foreignPlane'/><Attr size='75'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.8'>"
                "<File lfn='foreignlocalmiss'><Cluster name='foreignPlane'/><Attr size='85'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.9'>"
                "<File lfn='requestprefix::requestdali'><Cluster name='requestPlane'/><Attr size='88'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.10'>"
                "<File lfn='foreignoffsource'><Cluster name='archive'/><Attr size='89'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.11'>"
                "<SuperFile lfn='scope::foreignroot'><SubFile name='scope::foreignleaf'/></SuperFile>"
                "<File lfn='scope::foreignleaf'><Cluster name='foreignPlane'/><Attr size='95'/></File>"
            "</Foreign>"
            "<Foreign dali='127.0.0.12'>"
                "<File lfn='mapPrefix::addressfile'><Cluster name='mapPlane'/><Attr size='96'/></File>"
            "</Foreign>"
            "<Remote lfn='remote::mapStore::mapremote'><File><Cluster name='mapPlane'/><Attr size='61'/></File></Remote>"
            "<Remote lfn='remote::pkgStore::pkgremote'><File><Cluster name='pkgPlane'/><Attr size='62'/></File></Remote>"
            "<Remote lfn='remote::superStore::superremote'><File><Cluster name='superPlane'/><Attr size='63'/></File></Remote>"
            "<Remote lfn='remote::qualStore::scope::upperremote'><File><Cluster name='remotePlane'/><Attr size='72'/></File></Remote>"
            "<Remote lfn='remote::store::precedence'><File><Cluster name='remotePlane'/><Attr size='80'/></File></Remote>"
            "<Remote lfn='remote::requestStore::requestfile'><File><Cluster name='remotePlane'/><Attr size='84'/></File></Remote>"
            "<Remote lfn='remote::inheritedstore::scope::inheritedforeign'><File><Cluster name='remotePlane'/><Attr size='76'/></File></Remote>"
            "<Remote lfn='remote::localmissstore::remotelocalmiss'><File><Cluster name='remotePlane'/><Attr size='86'/></File></Remote>"
            "<Remote lfn='remote::mismatchstore::remoteoffsource'><File><Cluster name='archive'/><Attr size='90'/></File></Remote>"
            "<Remote lfn='remote::store::scope::super'>"
                "<SuperFile><SubFile name='scope::child'/></SuperFile>"
            "</Remote>"
            "<Remote lfn='remote::store::scope::child'>"
                "<File numparts='3'><Cluster name='thor'/><Attr size='51'/></File>"
            "</Remote>"
            "<Remote lfn='remote::nestedstore::scope::tree'>"
                "<SuperFile><SubFile name='scope::branch'/></SuperFile>"
            "</Remote>"
            "<Remote lfn='remote::nestedstore::scope::branch'>"
                "<SuperFile><SubFile name='scope::leaf'/></SuperFile>"
            "</Remote>"
            "<Remote lfn='remote::nestedstore::scope::leaf'>"
                "<File><Cluster name='remotePlane'/><Attr size='91'/></File>"
            "</Remote>"
            "<Remote lfn='remote::sharedstore::scope::root'>"
                "<SuperFile><SubFile name='scope::shared'/><SubFile name='scope::shared'/></SuperFile>"
            "</Remote>"
            "<Remote lfn='remote::sharedstore::scope::shared'>"
                "<File><Cluster name='remotePlane'/><Attr size='92'/></File>"
            "</Remote>"
            "<Remote lfn='remote::missingstore::scope::root'>"
                "<SuperFile><SubFile name='scope::absent'/></SuperFile>"
            "</Remote>"
            "<Remote lfn='remote::disabledstore::scope::root'>"
                "<SuperFile><SubFile name='scope::child'/></SuperFile>"
            "</Remote>"
            "</Files>";
    }

    static void assertFiles(ReferencedFileList &list, const ResolveCase &testCase)
    {
        unsigned actualCount = 0;
        Owned<IReferencedFileIterator> actualFiles = list.getFiles();
        ForEach(*actualFiles)
            actualCount++;
        CPPUNIT_ASSERT_MESSAGE(testCase.name, actualCount == testCase.expectedFiles.size());

        for (const ExpectedFile &expected : testCase.expectedFiles)
        {
            bool found = false;
            Owned<IReferencedFileIterator> files = list.getFiles();
            ForEach(*files)
            {
                IReferencedFile &file = files->query();
                if (!streq(file.getLogicalName(), expected.name))
                    continue;
                CPPUNIT_ASSERT_EQUAL_MESSAGE(testCase.name, expected.flags, file.getFlags() & expected.flags);
                CPPUNIT_ASSERT_MESSAGE(testCase.name, (file.getFlags() & expected.flagsToExclude) == 0);
                if (expected.size)
                    CPPUNIT_ASSERT_MESSAGE(testCase.name, file.getFileSize() == expected.size);
                found = true;
                break;
            }
            CPPUNIT_ASSERT_MESSAGE(testCase.name, found);
        }
    }

    void testResolveCases()
    {
        static const ResolveCase cases[] =
        {
            {
                "package map local superfile expansion",
                "<PackageMap><Package><File value='local::super'/></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                true, true, false, false,
                2, 0, 0,
                {
                    { "local::super", RefFileSuper, 0 },
                    { "local::leaf", RefSubFile, 34 }
                }
            },
            {
                "foreign Dali lookup",
                nullptr, "item", "127.0.0.1", nullptr, nullptr, "thor",
                false, false, true, false,
                0, 1, 0,
                {
                    { "item", RefFileResolvedForeign, 19 }
                }
            },
            {
                "foreign superfile recursively resolves its child",
                nullptr, "~FOREIGN::127.0.0.11::scope::foreignroot", nullptr, nullptr, nullptr, "foreignPlane",
                true, true, true, false,
                0, 2, 0,
                {
                    { "scope::foreignroot", RefFileLFNForeign | RefFileResolvedForeign | RefFileSuper, 0 },
                    { "scope::foreignleaf", RefFileLFNForeign | RefFileResolvedForeign | RefSubFile, 95 }
                }
            },
            {
                "remote storage superfile expansion",
                nullptr, "super", nullptr, "scope", "store", "thor",
                true, true, false, false,
                0, 0, 2,
                {
                    { "super", RefFileResolvedRemote | RefFileSuper, 0 },
                    { "scope::child", RefFileResolvedRemote | RefSubFile, 51 }
                }
            },
            {
                "package map scope inherited through package and superfile",
                "<PackageMap daliip='127.0.0.1' sourceCluster='mapPlane'><Package><SuperFile><SubFile value='mapfile'/></SuperFile></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "mapfile", RefFileResolvedForeign, 21, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "package Dali and source cluster override map scope",
                "<PackageMap daliip='127.0.0.1' sourceCluster='mapPlane'><Package daliip='127.0.0.2' sourceCluster='pkgPlane'><File value='pkgfile'/></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "pkgfile", RefFileResolvedForeign, 31, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "superfile Dali and source cluster override package scope",
                "<PackageMap daliip='127.0.0.1' sourceCluster='mapPlane'><Package daliip='127.0.0.2' sourceCluster='pkgPlane'><SuperFile daliip='127.0.0.3' sourceCluster='superPlane'><SubFile value='superchild'/></SuperFile></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "superchild", RefFileResolvedForeign, 41, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "Part Package inherits package map scope",
                "<PackageMap daliip='127.0.0.1' sourceCluster='mapPlane'><Part><Package><File value='partfile'/></Package></Part></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "partfile", RefFileResolvedForeign, 22, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "package map remote storage inherited through package and superfile",
                "<PackageMap remoteStorage='mapStore' sourceCluster='mapPlane'><Package><SuperFile><SubFile value='mapremote'/></SuperFile></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 0, 1,
                {
                    { "mapremote", RefFileResolvedRemote, 61, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "package remote storage overrides map scope",
                "<PackageMap remoteStorage='mapStore' sourceCluster='mapPlane'><Package remoteStorage='pkgStore' sourceCluster='pkgPlane'><File value='pkgremote'/></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 0, 1,
                {
                    { "pkgremote", RefFileResolvedRemote, 62, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "superfile remote storage overrides package scope",
                "<PackageMap remoteStorage='mapStore' sourceCluster='mapPlane'><Package remoteStorage='pkgStore' sourceCluster='pkgPlane'><SuperFile remoteStorage='superStore' sourceCluster='superPlane'><SubFile value='superremote'/></SuperFile></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 0, 1,
                {
                    { "superremote", RefFileResolvedRemote, 63, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "Dali prefix inherited from package map",
                "<PackageMap daliip='127.0.0.1/map' sourceCluster='mapPlane'><Package><SuperFile><SubFile value='prefixchild'/></SuperFile></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "prefixchild", RefFileResolvedForeign, 23, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "cluster at IP and prefix Dali location is split correctly",
                "<PackageMap daliip='mapPlane@127.0.0.12/mapPrefix'><Package><File value='addressfile'/></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 1, 0,
                {
                    { "addressfile", RefFileResolvedForeign, 96, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "foreign LFN remains unresolved when resolution is disabled",
                nullptr, "~FOREIGN::127.0.0.4::Scope::Embedded", nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                0, 0, 0,
                {
                    { "scope::embedded", RefFileLFNForeign, 0, RefFileResolvedForeign | RefFileNotFound }
                }
            },
            {
                "foreign LFN resolves case-insensitively with embedded prefix",
                nullptr, "~FOREIGN::127.0.0.4::Scope::Embedded", nullptr, nullptr, nullptr, "foreignPlane",
                false, false, true, false,
                0, 1, 0,
                {
                    { "scope::embedded", RefFileLFNForeign | RefFileResolvedForeign, 71, RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "remote LFN skips two tildes, lowercases name, and is allowed without foreign access",
                nullptr, "~~REMOTE::qualStore::Scope::UpperRemote", nullptr, nullptr, nullptr, "remotePlane",
                false, false, false, false,
                0, 0, 1,
                {
                    { "scope::upperremote", RefFileLFNRemote | RefFileResolvedRemote, 72, RefFileNotFound | RefFileNotOnSource }
                },
                false
            },
            {
                "embedded foreign LFN overrides inherited remote storage",
                "<PackageMap remoteStorage='inheritedstore' sourceCluster='foreignPlane'><Package><File value='foreign::127.0.0.7::scope::inheritedforeign'/></Package></PackageMap>",
                nullptr, nullptr, nullptr, nullptr, nullptr,
                false, false, true, false,
                0, 1, 0,
                {
                    { "scope::inheritedforeign", RefFileLFNForeign | RefFileResolvedForeign, 75, RefFileLFNRemote | RefFileNotFound | RefFileNotOnSource }
                }
            },
            {
                "local copy takes precedence over foreign Dali",
                nullptr, "precedence", "127.0.0.6", nullptr, nullptr, nullptr,
                false, false, false, false,
                1, 0, 0,
                {
                    { "precedence", RefSubFile, 81, RefFileResolvedForeign | RefFileResolvedRemote | RefFileNotFound }
                },
                true, true
            },
            {
                "local copy takes precedence over remote storage",
                nullptr, "precedence", nullptr, nullptr, "store", nullptr,
                false, false, false, false,
                1, 0, 0,
                {
                    { "precedence", RefSubFile, 81, RefFileResolvedForeign | RefFileResolvedRemote | RefFileNotFound }
                },
                true, true, nullptr, 0
            },
            {
                "local-first foreign lookup proceeds after local miss",
                nullptr, "foreignlocalmiss", "127.0.0.8", nullptr, nullptr, "foreignPlane",
                false, false, false, false,
                1, 1, 0,
                {
                    { "foreignlocalmiss", RefFileResolvedForeign, 85, RefFileNotFound }
                },
                true, true, nullptr, 0, 1
            },
            {
                "local-first foreign miss does not retry local",
                nullptr, "foreignlocalmissblocked", "127.0.0.99", nullptr, nullptr, nullptr,
                false, false, false, false,
                1, 1, 0,
                {
                    { "foreignlocalmissblocked", RefFileNotFound, 0, RefFileResolvedForeign | RefFileResolvedRemote }
                },
                true, true, nullptr, 0, 1
            },
            {
                "local-first remote lookup proceeds after local miss",
                nullptr, "remotelocalmiss", nullptr, nullptr, "localmissstore", "remotePlane",
                false, false, false, false,
                1, 0, 1,
                {
                    { "remotelocalmiss", RefFileResolvedRemote, 86, RefFileNotFound }
                },
                true, true, nullptr, 1, 1
            },
            {
                "local-first remote miss does not retry local",
                nullptr, "remotelocalmissblocked", nullptr, nullptr, "missingStore", nullptr,
                false, false, false, false,
                1, 0, 1,
                {
                    { "remotelocalmissblocked", RefFileNotFound, 0, RefFileResolvedForeign | RefFileResolvedRemote }
                },
                true, true, nullptr, 1, 1
            },
            {
                "foreign miss falls back to local when source cluster is unspecified",
                nullptr, "fallback", "127.0.0.99", nullptr, nullptr, nullptr,
                false, false, false, false,
                1, 1, 0,
                {
                    { "fallback", RefSubFile, 82, RefFileNotFound | RefFileResolvedForeign | RefFileResolvedRemote }
                }
            },
            {
                "foreign miss does not fall back when source cluster is specified",
                nullptr, "foreignblocked", "127.0.0.99", nullptr, nullptr, "thor",
                false, false, false, false,
                0, 1, 0,
                {
                    { "foreignblocked", RefFileNotFound, 0, RefFileResolvedForeign }
                }
            },
            {
                "remote miss falls back to local when source cluster is unspecified",
                nullptr, "remotefallback", nullptr, nullptr, "missingStore", nullptr,
                false, false, false, false,
                1, 0, 1,
                {
                    { "remotefallback", RefSubFile, 83, RefFileNotFound | RefFileResolvedRemote }
                }
            },
            {
                "remote miss does not fall back when source cluster is specified",
                nullptr, "remoteblocked", nullptr, nullptr, "missingStore", "thor",
                false, false, false, false,
                0, 0, 1,
                {
                    { "remoteblocked", RefFileNotFound, 0, RefFileResolvedRemote }
                }
            },
            {
                "request-level remote storage resolves an unqualified file",
                nullptr, "requestfile", nullptr, nullptr, nullptr, "remotePlane",
                false, false, false, true,
                0, 0, 1,
                {
                    { "requestfile", RefFileResolvedRemote, 84, RefFileNotFound | RefFileNotOnSource }
                },
                true, false, "requestStore"
            },
            {
                "request-level Dali and prefix resolve an unqualified file",
                nullptr, "requestdali", nullptr, "requestprefix", nullptr, "requestPlane",
                false, false, false, false,
                0, 1, 0,
                {
                    { "requestdali", RefFileResolvedForeign, 88, RefFileNotFound | RefFileNotOnSource }
                },
                true, false, "127.0.0.9"
            },
            {
                "local file outside target locations is marked not on cluster",
                nullptr, "localofftarget", nullptr, nullptr, nullptr, nullptr,
                false, false, false, false,
                1, 0, 0,
                {
                    { "localofftarget", RefSubFile | RefFileNotOnCluster, 87, RefFileNotFound }
                }
            },
            {
                "foreign file outside source cluster is marked not on source",
                nullptr, "foreignoffsource", "127.0.0.10", nullptr, nullptr, "requestedPlane",
                false, false, false, false,
                0, 1, 0,
                {
                    { "foreignoffsource", RefFileResolvedForeign | RefFileNotOnSource, 89, RefFileNotFound }
                }
            },
            {
                "remote file outside source cluster is marked not on source",
                nullptr, "remoteoffsource", nullptr, nullptr, "mismatchstore", "requestedPlane",
                false, false, false, false,
                0, 0, 1,
                {
                    { "remoteoffsource", RefFileResolvedRemote | RefFileNotOnSource, 90, RefFileNotFound }
                }
            },
            {
                "nested remote superfiles expand recursively",
                nullptr, "tree", nullptr, "scope", "nestedstore", "remotePlane",
                true, true, false, false,
                0, 0, 3,
                {
                    { "tree", RefFileResolvedRemote | RefFileSuper, 0 },
                    { "scope::branch", RefFileResolvedRemote | RefFileSuper | RefSubFile, 0 },
                    { "scope::leaf", RefFileResolvedRemote | RefSubFile, 91 }
                }
            },
            {
                "shared remote child is resolved and listed once",
                nullptr, "root", nullptr, "scope", "sharedstore", "remotePlane",
                true, true, false, false,
                0, 0, 2,
                {
                    { "root", RefFileResolvedRemote | RefFileSuper, 0 },
                    { "scope::shared", RefFileResolvedRemote | RefSubFile, 92 }
                },
                true, false, nullptr, 2
            },
            {
                "missing remote superfile child is retained as not found",
                nullptr, "root", nullptr, "scope", "missingstore", "remotePlane",
                true, true, false, false,
                0, 0, 2,
                {
                    { "root", RefFileResolvedRemote | RefFileSuper, 0 },
                    { "scope::absent", RefFileNotFound | RefSubFile, 0 }
                }
            },
            {
                "disabled remote superfile expansion does not add children",
                nullptr, "root", nullptr, "scope", "disabledstore", "remotePlane",
                false, false, false, false,
                0, 0, 1,
                {
                    { "root", RefFileResolvedRemote | RefFileSuper, 0 }
                }
            }
        };

        StringArray locations;
        locations.append("thor");
        constexpr unsigned numCases = sizeof(cases) / sizeof(cases[0]);
        for (unsigned caseIndex = 0; caseIndex < numCases; caseIndex++)
        {
            const ResolveCase &testCase = cases[caseIndex];
            Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
            ReferencedFileList list(resolver, testCase.allowForeignFiles, false, testCase.name);
            if (testCase.packageMapXml)
            {
                Owned<IPropertyTree> packageMap = createPTreeFromXMLString(testCase.packageMapXml);
                list.addFilesFromPackageMap(packageMap);
            }
            else
                list.addFile(testCase.logicalName, testCase.daliip, nullptr, testCase.remotePrefix, testCase.remoteStorage);

            list.resolveFiles(locations, testCase.remoteLocation, testCase.remotePrefix, testCase.srcCluster, testCase.checkLocalFirst,
                testCase.expandSuperFiles, testCase.trackSubFiles, testCase.resolveLFNForeign, testCase.useRemoteStorage);

            assertFiles(list, testCase);
            CPPUNIT_ASSERT_MESSAGE(testCase.name, resolver->localLookups >= testCase.minimumLocalLookups);
            CPPUNIT_ASSERT_MESSAGE(testCase.name, resolver->localLookups <= testCase.maximumLocalLookups);
            CPPUNIT_ASSERT_EQUAL_MESSAGE(testCase.name, testCase.expectedForeignLookups, resolver->foreignLookups);
            CPPUNIT_ASSERT_MESSAGE(testCase.name, resolver->remoteLookups >= testCase.minimumRemoteLookups);
            CPPUNIT_ASSERT_MESSAGE(testCase.name, resolver->remoteLookups <= testCase.maximumRemoteLookups);
        }
    }

    void testConflictingScopesRejected()
    {
        Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
        ReferencedFileList list(resolver, true, false, "conflicting scope test");
        Owned<IPropertyTree> packageMap = createPTreeFromXMLString(
            "<PackageMap daliip='127.0.0.1' remoteStorage='store'><Package><File value='file'/></Package></PackageMap>");
        bool rejected = false;
        try
        {
            list.addFilesFromPackageMap(packageMap);
        }
        catch (IException *e)
        {
            StringBuffer message;
            e->errorMessage(message);
            rejected = strstr(message.str(), "Cannot specify both") != nullptr;
            e->Release();
        }
        CPPUNIT_ASSERT(rejected);
    }

    void testPackageAndSuperfileScopeConflictsRejected()
    {
        static const char *invalidPackageMaps[] =
        {
            "<PackageMap><Package daliip='127.0.0.1' remoteStorage='store'><File value='file'/></Package></PackageMap>",
            "<PackageMap><Package><SuperFile daliip='127.0.0.1' remoteStorage='store'><SubFile value='file'/></SuperFile></Package></PackageMap>"
        };
        constexpr unsigned numCases = sizeof(invalidPackageMaps) / sizeof(invalidPackageMaps[0]);
        for (unsigned caseIndex = 0; caseIndex < numCases; caseIndex++)
        {
            Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
            ReferencedFileList list(resolver, true, false, "conflicting nested scope test");
            Owned<IPropertyTree> packageMap = createPTreeFromXMLString(invalidPackageMaps[caseIndex]);
            bool rejected = false;
            try
            {
                list.addFilesFromPackageMap(packageMap);
            }
            catch (IException *e)
            {
                StringBuffer message;
                e->errorMessage(message);
                rejected = strstr(message.str(), "Cannot specify both") != nullptr;
                e->Release();
            }
            CPPUNIT_ASSERT(rejected);
        }
    }

    void testForeignQualifierRejectedWhenDisabled()
    {
        Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
        ReferencedFileList list(resolver, false, false, "foreign LFN policy test");
        bool rejected = false;
        try
        {
            list.addFile("~FOREIGN::127.0.0.4::scope::embedded");
        }
        catch (IException *e)
        {
            StringBuffer message;
            e->errorMessage(message);
            rejected = strstr(message.str(), "Foreign file not allowed") != nullptr;
            e->Release();
        }
        CPPUNIT_ASSERT(rejected);
    }

    void testForeignSuperfileChildRejectedWhenDisabled()
    {
        Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
        ReferencedFileList list(resolver, false, false, "foreign subfile policy test");
        list.addFile("foreignsuper", "127.0.0.5");

        StringArray locations;
        locations.append("thor");
        bool rejected = false;
        try
        {
            list.resolveFiles(locations, nullptr, nullptr, nullptr, false, true, false, true, false);
        }
        catch (IException *e)
        {
            StringBuffer message;
            e->errorMessage(message);
            rejected = strstr(message.str(), "Foreign sub file not allowed") != nullptr;
            e->Release();
        }
        CPPUNIT_ASSERT(rejected);
    }

    void testResolutionEarlyReturns()
    {
        Owned<CXmlRefFileResolver> resolver = new CXmlRefFileResolver(queryFixtureXml());
        StringArray locations;
        locations.append("thor");

        ReferencedFile noDfsLocal("noDfsLocal", nullptr, nullptr, nullptr, false, RefFileNone, nullptr, true, false, nullptr, resolver);
        noDfsLocal.resolveLocal(locations, nullptr, nullptr, nullptr);
        CPPUNIT_ASSERT(noDfsLocal.getFlags() & RefFileNotFound);

        ReferencedFile noDfsForeign("noDfsForeign", nullptr, nullptr, nullptr, false, RefFileNone, nullptr, true, false, nullptr, resolver);
        noDfsForeign.resolveForeign(nullptr, nullptr, nullptr, locations, nullptr, false, nullptr, true);
        CPPUNIT_ASSERT(noDfsForeign.getFlags() & RefFileNotFound);

        ReferencedFile noDfsRemote("noDfsRemote", nullptr, nullptr, nullptr, false, RefFileNone, nullptr, true, false, nullptr, resolver);
        noDfsRemote.resolveRemote(nullptr, "store", nullptr, locations, nullptr, false, nullptr, true);
        CPPUNIT_ASSERT(noDfsRemote.getFlags() & RefFileNotFound);

        ReferencedFile packageLocal("packageLocal", nullptr, nullptr, nullptr, false, RefFileInPackage, nullptr, false, false, nullptr, resolver);
        packageLocal.resolveLocal(locations, nullptr, nullptr, nullptr);
        CPPUNIT_ASSERT(packageLocal.getFlags() & RefFileInPackage);
        CPPUNIT_ASSERT(!(packageLocal.getFlags() & RefFileNotFound));

        ReferencedFile packageForeign("packageForeign", nullptr, nullptr, nullptr, false, RefFileInPackage, nullptr, false, false, nullptr, resolver);
        packageForeign.resolveForeign(nullptr, nullptr, nullptr, locations, nullptr, false, nullptr, true);
        CPPUNIT_ASSERT(packageForeign.getFlags() & RefFileInPackage);
        CPPUNIT_ASSERT(!(packageForeign.getFlags() & RefFileNotFound));

        ReferencedFile packageRemote("packageRemote", nullptr, nullptr, nullptr, false, RefFileInPackage, nullptr, false, false, nullptr, resolver);
        packageRemote.resolveRemote(nullptr, "store", nullptr, locations, nullptr, false, nullptr, true);
        CPPUNIT_ASSERT(packageRemote.getFlags() & RefFileInPackage);
        CPPUNIT_ASSERT(!(packageRemote.getFlags() & RefFileNotFound));

        CPPUNIT_ASSERT_EQUAL(0U, resolver->localLookups);
        CPPUNIT_ASSERT_EQUAL(0U, resolver->foreignLookups);
        CPPUNIT_ASSERT_EQUAL(0U, resolver->remoteLookups);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ReferencedFileListTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ReferencedFileListTests, "ReferencedFileListTests");
#endif
