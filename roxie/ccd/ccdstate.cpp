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
#include <jlib.hpp>

#include "jisem.hpp"
#include "jhash.hpp"
#include "jsort.hpp"
#include "jregexp.hpp"
#include "jevent.hpp"

#include "udptopo.hpp"
#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccddali.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdlistener.hpp"
#include "ccdfile.hpp"
#include "ccdsnmp.hpp"

#include "hqlplugins.hpp"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "dafdesc.hpp"
#include "dautils.hpp"
#include "rmtfile.hpp"

#include "pkgimpl.hpp"
#include "roxiehelper.hpp"

#include "ws_dfsclient.hpp"


//-------------------------------------------------------------------------------------------
// class CRoxiePluginCtx - provide the environments for plugins loaded by roxie. 
// Base class handles making sure memory allocation comes from the right heap. 
// implement get/set properties to allow plugin configuration information to be retrieved from Roxie topology file
//-------------------------------------------------------------------------------------------

class CRoxiePluginCtx : public SimplePluginCtx
{
public:
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const
    {
        return topology->getPropInt(propName, defaultValue);
    }
    virtual const char *ctxQueryProp(const char *propName) const
    {
        return topology->queryProp(propName);
    }
} PluginCtx;

SafePluginMap *plugins;

//================================================================================================

// In legacy state files, the original file names passed in _fileName or _indexFileName may have been translated into _superFileName or _superKeyName,
// and then 0 or more (max 1 for subfiles, no limit for subkeys) _fileName or _indexFileName will have been added. This translation will not take place
// if the files resolve to single file/key, or if we are using new embedded wu system

// Basic mode of operation therefore is to get the original name, see if it can be resolved by package into a list of subfiles, and if not, use 
// iterator on the xgmml node to get the list. 

// These two helper functions will return the original filenames placed in the XGMML by the codegen, regardless of how/if roxieconfig resolved them

static const char *_queryNodeFileName(const IPropertyTree &graphNode)
{
    if (graphNode.hasProp("att[@name='_file_dynamic']"))
        return NULL;
    else
        return graphNode.queryProp("att[@name='_fileName']/@value");
}

static const char *_queryNodeIndexName(const IPropertyTree &graphNode)
{
    if (graphNode.hasProp("att[@name='_indexFile_dynamic']"))
        return NULL;
    else
        return graphNode.queryProp("att[@name='_indexFileName']/@value");
}

static bool isSimpleIndexActivity(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKindexaggregate:
    case TAKindexcount:
    case TAKindexexists:
    case TAKindexgroupaggregate:
    case TAKindexgroupcount:
    case TAKindexgroupexists:
    case TAKindexnormalize:
    case TAKindexread:
        return true;
    default:
        return false;
    }
}

const char *queryNodeFileName(const IPropertyTree &graphNode, ThorActivityKind kind)
{
    if (isSimpleIndexActivity(kind))
        return NULL;
    else
        return _queryNodeFileName(graphNode);
}

const char *queryNodeIndexName(const IPropertyTree &graphNode, ThorActivityKind kind)
{
    if (isSimpleIndexActivity(kind))
        return _queryNodeFileName(graphNode);
    else
        return _queryNodeIndexName(graphNode);
}

// DelayedReleaser mechanism hangs on to a link to an object for a while...

class DelayedReleaseQueueItem : public CInterfaceOf<IInterface>
{
    Owned<IInterface> goer;
    time_t goTime;
public:
    DelayedReleaseQueueItem(IInterface *_goer, unsigned delaySeconds)
    : goer(_goer)
    {
        time(&goTime);
        goTime += delaySeconds;
    }
    unsigned remaining()
    {
        time_t now;
        time(&now);
        if (now > goTime)
            return 0;
        else
            return (unsigned)(goTime - now);
    }
};

class DelayedReleaserThread : public Thread
{
private:
    std::atomic<bool> closing;
    bool started;
    CriticalSection lock;
    IArrayOf<DelayedReleaseQueueItem> queue;
    Semaphore sem;
public:
    DelayedReleaserThread() : Thread("DelayedReleaserThread")
    {
        closing = false;
        started = false;
    }

    ~DelayedReleaserThread()
    {
        stop();
    }

    virtual int run()
    {
        if (traceLevel)
            DBGLOG("DelayedReleaserThread %p starting", this);
        unsigned nextTimeout = INFINITE;
        while (!closing || queue.length())
        {
            sem.wait(nextTimeout);
            CriticalBlock b(lock);
            nextTimeout = INFINITE;
            ForEachItemInRev(idx, queue)
            {
                DelayedReleaseQueueItem &goer = queue.item(idx);
                unsigned timeRemaining = goer.remaining();
                if (!timeRemaining)
                    queue.remove(idx);
                else if (timeRemaining < nextTimeout)
                    nextTimeout = timeRemaining;
            }
            if (nextTimeout != INFINITE)
                nextTimeout = nextTimeout * 1000;
            clearKeyStoreCache(false);   // Allows us to fully release files we no longer need because of unloaded queries
        }
        if (traceLevel)
            DBGLOG("DelayedReleaserThread %p exiting", this);
        return 0;
    }

    void stop()
    {
        if (started)
        {
            closing = true;
            sem.signal();
            join();
        }
    }

    void delayedRelease(IInterface *goer, unsigned delaySeconds)
    {
        if (goer)
        {
            CriticalBlock b(lock);
            if (!started)
            {
                start(false);
                started = true;
            }
            queue.append(*new DelayedReleaseQueueItem(goer, delaySeconds));
            sem.signal();
        }
    }
};

Owned<DelayedReleaserThread> delayedReleaser;

void createDelayedReleaser()
{
    delayedReleaser.setown(new DelayedReleaserThread);
}

void stopDelayedReleaser()
{
    if (delayedReleaser)
        delayedReleaser->stop();
    delayedReleaser.clear();
}


//-------------------------------------------------------------------------

bool startRoxieEventRecording(const char * options, const char * filename)
{
    if (isEmptyString(options))
        options = "threadid";

    StringBuffer outputFilename;
    const char * path = filename;
    if (!isAbsolutePath(filename))
    {
        getTempFilePath(outputFilename, "eventrecorder", nullptr);
        outputFilename.append(PATHSEPCHAR);
        if (!isEmptyString(filename))
        {
            outputFilename.append(filename);
        }
        else
        {
            //MORE: Revisit this at a later date
            unsigned seq = (unsigned)(get_cycles_now() % 100000);
            outputFilename.append("roxieevents.").append((unsigned)GetCurrentProcessId()).append(".").append(seq).append(".evt");
        }

        path = outputFilename.str();
        //MORE: The caller will need to know the full pathname
    }

    recursiveCreateDirectoryForFile(path);
    if (!queryRecorder().startRecording(options, path, true))
        return false;

    //Generate information about all files that are already registered - others will be added as they are opened
    recordEventIndexInformation();

    queryRecorder().pauseRecording(false, false);
    return true;
}

bool stopRoxieEventRecording(EventRecordingSummary * optSummary)
{
    return queryRecorder().stopRecording(optSummary);
}

//-------------------------------------------------------------------------

class CSimpleSuperFileArray : implements ISimpleSuperFileEnquiry, public CInterface
{
    IArrayOf<IPropertyTree> subFiles;
public:
    IMPLEMENT_IINTERFACE;
    CSimpleSuperFileArray(IPropertyTreeIterator &_subs)
    {
        ForEach(_subs)
        {
            IPropertyTree &sub = _subs.query();
            sub.Link();
            subFiles.append(sub);
        }
    }
    virtual unsigned numSubFiles() const 
    {
        return subFiles.length();
    }
    virtual bool getSubFileName(unsigned num, StringBuffer &name) const
    {
        if (subFiles.isItem(num))
        {
            name.append(subFiles.item(num).queryProp("@value"));
            return true;
        }
        else
            return false;
    }
    virtual unsigned findSubName(const char *subname) const
    {
        ForEachItemIn(idx, subFiles)
        {
            if (strieq(subFiles.item(idx).queryProp("@value"), subname))
                return idx;
        }
        return NotFound;
    }
    virtual unsigned getContents(StringArray &contents) const
    {
        ForEachItemIn(idx, subFiles)
        {
            contents.append(subFiles.item(idx).queryProp("@value"));
        }
        return subFiles.length();
    }
};

//-------------------------------------------------------------------------------------------
// class CRoxiePackage - provide the environment in which file names and query options are interpreted
// by a roxie query. 
// File names are resolved into IResolvedFile objects. A cache is used to ensure that the IResolvedFile is 
// shared wherever possible.
// Effective environment is precomputed in mergedEnvironment for efficient recall by queries
// Packages are described using XML files - see documentation for details. 
//-------------------------------------------------------------------------------------------

/**
 * Packages are hierarchical - they are searched recursively to get the info you want
 * A PackageMap defines the entire environment - potentially each query that uses that PackageMap will pick a different package within it
 * A particular instantiation of a roxie query (i.e. a IQueryFactory) will have a pointer to the specific IRoxiePackage within the active PackageMap
 * that is providing its environment.
 *
 * A PackageMap can also indicate the name of the QuerySet it applies to. If not specified, at will apply to all QuerySets on the Roxie.
 *
 * A PackageSet is a list of PackageMap id's, and is used to tell Roxie what PackageMaps to load.
 * A Roxie can have multiple PackageMap's active. When updating the data, you might:
 *  - create a new PackageMap to refer to the new data
 *  - once it has loaded, mark it active, and mark the previous one as inactive
 *  - Once sure no queries in flight, unload the previous one
 *
 * Each Roxie will load all PackageMaps that are in any PackageSet whose @process attribute matches the cluster name.
 *
 * All package information is stored in Dali (and cached locally)
 *
 * <PackageSets>
 *  <PackageSet id = 'ps1' process='*'>                                # use this packageset for all roxies (same as omitting process)
 *   <PackageMap id='pm1b' querySet='qs1' active='true'/>  # Use the PackageMap pm1b for QuerySet qs1 and make it active
 *   <PackageMap id='pm1a' querySet='qs1' active='false'/> # Use the PackageMap pm1a for QuerySet qs1 but don't make it active
 *   <PackageMap id='pm2' querySet='dev*' active='true'/>  # Use the PackageMap pm1a for all QuerySets with names starting dev and make it active
 *  </PackageMapSet>
 * </PackageSets>
 *
 * <PackageMaps>
 *  <PackageMap id='pm1a'>
 *   <Package id='package1'>
 *    ...
 *   </Package>
 *   <Package id='package2'>
 *   </Package>
 *  </PackageMap>
 *  <PackageMap id='pm2'>
 *  </PackageMap>
 *  <PackageMap id='pm3'>
 *  </PackageMap>
 * </PackageMaps>
 */

class CResolvedFileCache : implements IResolvedFileCache
{
    CriticalSection cacheLock;
    CopyMapStringToMyClass<IResolvedFile> files;

public:
    // Retrieve number of files in cache
    inline unsigned count() const
    {
        return files.count();
    }

    // Add a filename and the corresponding IResolvedFile to the cache
    virtual void addCache(const char *filename, const IResolvedFile *file)
    {
        CriticalBlock b(cacheLock);
        IResolvedFile *add = const_cast<IResolvedFile *>(file);
        add->setCache(this);
        files.setValue(filename, add);
    }
    // Remove an IResolvedFile from the cache
    virtual void removeCache(const IResolvedFile *file)
    {
        CriticalBlock b(cacheLock);
        if (doTrace(traceRoxieFiles, TraceFlags::Detailed))
            DBGLOG("removeCache %s", file->queryFileName());
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table. 
        // So only remove from hash table if what we find there matches the item that is being deleted.
        IResolvedFile *goer = files.getValue(file->queryFileName());
        if (goer == file)
            files.remove(file->queryFileName());
        // You might want to remove files from the daliServer cache too, but it's not safe to do so here as there may be multiple package caches
    }
    // Lookup a filename in the cache
    virtual IResolvedFile *lookupCache(const char *filename)
    {
        CriticalBlock b(cacheLock);
        IResolvedFile *cache = files.getValue(filename);
        if (cache)
        {
            if (cache->isAliveAndLink())
                return cache;
            if (traceLevel)
                DBGLOG("Not returning %s from cache as isAlive() returned false", filename);
        }
        return NULL;
    }
};

// Note - we use a separate cache for the misses rather than any clever attempts to overload
// the one cache with a "special" value, since (among other reasons) the misses are cleared
// prior to a package reload, but the hits need not be (as the file will be locked as long as it
// is in the cache)

static CriticalSection daliMissesCrit;
static Owned<KeptLowerCaseAtomTable> daliMisses;

static void noteDaliMiss(const char *filename)
{
    CriticalBlock b(daliMissesCrit);
    if (doTrace(traceRoxieFiles, TraceFlags::Max))
        DBGLOG("noteDaliMiss %s", filename);
    daliMisses->addAtom(filename);
}

static bool checkCachedDaliMiss(const char *filename)
{
    CriticalBlock b(daliMissesCrit);
    bool ret = daliMisses->find(filename) != NULL;
    if (doTrace(traceRoxieFiles, TraceFlags::Max))
        DBGLOG("checkCachedDaliMiss %s returns %d", filename, ret);
    return ret;
}

static void clearDaliMisses()
{
    CriticalBlock b(daliMissesCrit);
    if (traceLevel)
        DBGLOG("Clearing dali misses cache");
    daliMisses.setown(new KeptLowerCaseAtomTable);
}


class CRoxiePackageNode : extends CPackageNode, implements IRoxiePackage
{
protected:
    static CResolvedFileCache daliFiles;
    static CriticalSection daliLookupCrits[NUM_DALI_CRITS];
    mutable CResolvedFileCache fileCache;
    IArrayOf<IResolvedFile> files;  // Used when preload set
    IArrayOf<IKeyArray> keyArrays;  // Used when preload set
    IArrayOf<IFileIOArray> fileArrays;  // Used when preload set

    virtual aindex_t getBaseCount() const = 0;
    virtual const CRoxiePackageNode *getBaseNode(aindex_t pos) const = 0;

    virtual RecordTranslationMode getSysFieldTranslationEnabled() const override { return fieldTranslationEnabled; } //roxie configured value

    // Use local package file only to resolve subfile into physical file info
    IResolvedFile *resolveLFNusingPackage(const char *fileName) const
    {
        if (node)
        {
            StringBuffer xpath;
            IPropertyTree *fileInfo = node->queryPropTree(xpath.appendf("File[@id='%s']", fileName).str());
            if (fileInfo)
            {
                Owned <IResolvedFileCreator> result = createResolvedFile(fileName, NULL, false);
                result->addSubFile(createFileDescriptorFromRoxieXML(fileInfo), NULL);
                return result.getClear();
            }
        }
        return NULL;
    }

    // Use dali to resolve subfile into physical file info
    static IResolvedFile *resolveLFNusingDaliOrLocal(const char *fileName, bool useCache, bool cacheResult, AccessMode accessMode, bool alwaysCreate, bool resolveLocal, bool isPrivilegedUser)
    {
        unsigned hash = hashcz((const unsigned char *) fileName, 0x811C9DC5);
        CriticalBlock b(daliLookupCrits[hash % NUM_DALI_CRITS]);
        // MORE - look at alwaysCreate... This may be useful to implement earlier locking semantics.
        if (doTrace(traceRoxieFiles, TraceFlags::Max))
            DBGLOG("resolveLFNusingDaliOrLocal %s %d %d %x %d", fileName, useCache, cacheResult, (unsigned)accessMode, alwaysCreate);
        IResolvedFile* result = NULL;
        if (useCache)
        {
            result = daliFiles.lookupCache(fileName);
            if (result)
            {
                if (doTrace(traceRoxieFiles, TraceFlags::Max))
                    DBGLOG("resolveLFNusingDaliOrLocal %s - cache hit", fileName);
                return result;
            }
        }
        if (alwaysCreate || !useCache || !checkCachedDaliMiss(fileName))
        {
            Owned<IRoxieDaliHelper> daliHelper = connectToDali();
            if (daliHelper)
            {
                if (daliHelper->connected())
                {
                    Owned<IDistributedFile> dFile = daliHelper->resolveLFN(fileName, cacheResult, accessMode, isPrivilegedUser);
                    if (dFile)
                        result = createResolvedFile(fileName, NULL, dFile.getClear(), daliHelper, !useCache, cacheResult, accessMode);
                }
                else if (!isWrite(accessMode))  // If we need write access and expect a dali, but don't have one, we should probably fail
                {
                    // we have no dali, we can't lock..
                    Owned<IFileDescriptor> fd = daliHelper->resolveCachedLFN(fileName);
                    if (fd)
                    {
                        Owned <IResolvedFileCreator> creator = createResolvedFile(fileName, NULL, false);
                        Owned<IFileDescriptor> remoteFDesc = daliHelper->checkClonedFromRemote(fileName, fd, cacheResult, isPrivilegedUser);
                        creator->addSubFile(fd.getClear(), remoteFDesc.getClear());
                        result = creator.getClear();
                    }
                }
            }
            if (!result && (resolveLocal || alwaysCreate))
            {
                StringBuffer useName;
                bool wasDFS = false;
                if (!resolveLocal || strstr(fileName,"::") != NULL)
                {
                    makeSinglePhysicalPartName(fileName, useName, true, wasDFS);
                }
                else
                    useName.append(fileName);
                bool exists = checkFileExists(useName);
                if (exists || alwaysCreate)
                {
                    Owned <IResolvedFileCreator> creator = createResolvedFile(fileName, wasDFS ? NULL : useName.str(), false);
                    if (exists)
                        creator->addSubFile(useName);
                    result = creator.getClear();
                }
            }
        }
        if (cacheResult)
        {
            if (doTrace(traceRoxieFiles, TraceFlags::Max))
                DBGLOG("resolveLFNusingDaliOrLocal %s - cache add %d", fileName, result != NULL);
            if (result)
                daliFiles.addCache(fileName, result);
            else
                noteDaliMiss(fileName);
        }
        return result;
    }

    // Use local package and its bases to resolve existing file into physical file info via all supported resolvers
    IResolvedFile *lookupExpandedFileName(const char *fileName, bool useCache, bool cacheResult, AccessMode accessMode, bool alwaysCreate, bool checkCompulsory, bool isPrivilegedUser) const
    {
        IResolvedFile *result = lookupFile(fileName, useCache, cacheResult, accessMode, alwaysCreate, isPrivilegedUser);
        if (!result && (!checkCompulsory || !isCompulsory()))
            result = resolveLFNusingDaliOrLocal(fileName, useCache, cacheResult, accessMode, alwaysCreate, resolveLocally(), isPrivilegedUser);
        return result;
    }

    IResolvedFile *lookupFile(const char *fileName, bool useCache, bool cacheResult, AccessMode accessMode, bool alwaysCreate, bool isPrivilegedUser) const
    {
        // Order of resolution: 
        // 1. Files named in package
        // 2. Files named in bases

        IResolvedFile* result = useCache ? fileCache.lookupCache(fileName) : NULL;
        if (result)
            return result;

        Owned<const ISimpleSuperFileEnquiry> subFileInfo = resolveSuperFile(fileName);
        if (subFileInfo)
        {
            unsigned numSubFiles = subFileInfo->numSubFiles();
            // Note: do not try to optimize the common case of a single subfile
            // as we still want to report the superfile info from the resolvedFile
            Owned<IResolvedFileCreator> super;
            for (unsigned idx = 0; idx < numSubFiles; idx++)
            {
                StringBuffer subFileName;
                subFileInfo->getSubFileName(idx, subFileName);
                if (subFileName.length())  // Empty subfile names can come from package file - just ignore
                {
                    if (subFileName.charAt(0)=='~')
                    {
                        // implies that a package file had ~ in subfile names - shouldn't really, but we allow it (and just strip the ~)
                        subFileName.remove(0,1);
                    }
                    if (doTrace(traceRoxieFiles, TraceFlags::Detailed))
                        DBGLOG("Looking up subfile %s", subFileName.str());
                    AccessMode subAccessMode = AccessMode::readRandom;   // NOTE - overwriting a superfile does NOT require write access to subfiles
                    Owned<const IResolvedFile> subFileInfo = lookupExpandedFileName(subFileName, useCache, cacheResult, subAccessMode, false, false, isPrivilegedUser);
                    if (subFileInfo)
                    {
                        if (!super)
                            super.setown(createResolvedFile(fileName, NULL, true));
                        super->addSubFile(subFileInfo);
                    }
                }
            }
            if (super && cacheResult)
                fileCache.addCache(fileName, super);
            return super.getClear();
        }
        result = resolveLFNusingPackage(fileName);
        if (result)
        {
            if (cacheResult)
                fileCache.addCache(fileName, result);
            return result;
        }
        aindex_t count = getBaseCount();
        for (aindex_t i = 0; i < count; i++)
        {
            const CRoxiePackageNode *basePackage = getBaseNode(i);
            if (!basePackage)
                continue;
            IResolvedFile *result = basePackage->lookupFile(fileName, useCache, cacheResult, accessMode, alwaysCreate, isPrivilegedUser);
            if (result)
                return result;
        }
        return NULL;
    }

    void doPreload(unsigned channel, const IResolvedFile *resolved)
    {
        if (resolved->isKey())
            keyArrays.append(*resolved->getKeyArray(false, channel));
        else
            fileArrays.append(*resolved->getIFileIOArray(false, channel));
    }

    void checkPreload()
    {
        if (isPreload())
        {
            // Look through all files and resolve them now
            Owned<IPropertyTreeIterator> supers = node->getElements("SuperFile");
            const bool isCodeSigned = isActivityCodeSigned(*node);
            ForEach(*supers)
            {
                IPropertyTree &super = supers->query();
                const char *name = super.queryProp("@id");
                if (name)
                {
                    try
                    {
                        const IResolvedFile *resolved = lookupFileName(name, false, true, true, NULL, true, isCodeSigned);
                        if (resolved)
                        {
                            files.append(*const_cast<IResolvedFile *>(resolved));
                            Owned<const ITopologyServer> topology = getTopology();
                            for (unsigned channel : topology->queryChannels())
                            {
                                assertex(channel);
                                doPreload(channel, resolved);
                            }
                        }
                    }
                    catch (IException *E)
                    {
                        VStringBuffer msg("Failed to preload file %s for package node %s", name, queryId());
                        EXCLOG(E, msg.str());
                        E->Release();
                    }
                }
            }
        }
    }

    // default constructor for derived class use
    CRoxiePackageNode()
    {
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxiePackageNode(IPropertyTree *p) : CPackageNode(p)
    {
    }

    ~CRoxiePackageNode()
    {
        keyArrays.kill();
        fileArrays.kill();
        files.kill();
        assertex(fileCache.count()==0);
        // If it's possible for cached objects to outlive the cache I think there is a problem...
        // we could set the cache field to null here for any objects still in cache but there would be a race condition
    }

    virtual void setHash(hash64_t newhash)
    {
        hash = newhash;
    }

    virtual IPropertyTreeIterator *getInMemoryIndexInfo(const IPropertyTree &graphNode) const 
    {
        StringBuffer xpath;
        xpath.append("SuperFile[@id='").append(queryNodeFileName(graphNode, getActivityKind(graphNode))).append("']");
        return lookupElements(xpath.str(), "MemIndex");
    }

    virtual const IResolvedFile *lookupFileName(const char *_fileName, bool opt, bool useCache, bool cacheResult, IConstWorkUnit *wu, bool ignoreForeignPrefix, bool isPrivilegedUser) const
    {
        StringBuffer fileName;
        expandLogicalFilename(fileName, _fileName, wu, false, ignoreForeignPrefix);
        if (doTrace(traceRoxieFiles, TraceFlags::Max))
            DBGLOG("lookupFileName %s", fileName.str());

        const IResolvedFile *result = lookupExpandedFileName(fileName, useCache, cacheResult, AccessMode::readRandom, false, true, isPrivilegedUser);
        if (!result)
        {
            StringBuffer compulsoryMsg;
            if (isCompulsory())
                    compulsoryMsg.append(" (Package is compulsory)");
            if (!opt && !pretendAllOpt)
                throw MakeStringException(ROXIE_FILE_ERROR, "Could not resolve filename %s%s", fileName.str(), compulsoryMsg.str());
        if (doTrace(traceRoxieFiles))
                DBGLOG("Could not resolve OPT filename %s%s", fileName.str(), compulsoryMsg.str());
        }
        return result;
    }

    virtual IRoxieWriteHandler *createWriteHandler(const char *_fileName, bool overwrite, bool extend, const StringArray &clusters, IConstWorkUnit *wu, bool isPrivilegedUser) const
    {
        StringBuffer fileName;
        expandLogicalFilename(fileName, _fileName, wu, false, false);
        Owned<IResolvedFile> resolved = lookupFile(fileName, false, false, AccessMode::writeSequential, true, isPrivilegedUser);
        if (!resolved)
            resolved.setown(resolveLFNusingDaliOrLocal(fileName, false, false, AccessMode::writeSequential, true, resolveLocally(), isPrivilegedUser));
        if (resolved)
        {
            if (resolved->exists())
            {
                if (!overwrite)
                    throw MakeStringException(99, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", resolved->queryFileName());
                if (extend)
                    UNIMPLEMENTED; // How does extend fit in with the clusterwritemanager stuff? They can't specify cluster and extend together...
                resolved->setCache(NULL);
                resolved->remove();
            }
            if (resolved->queryPhysicalName())
                fileName.clear().append(resolved->queryPhysicalName());  // if it turned out to be a local file
            resolved.clear();
        }
        else
            throw MakeStringException(ROXIE_FILE_ERROR, "Cannot write %s", fileName.str());
        // filename by now may be a local filename, or a dali one
        Owned<IRoxieDaliHelper> daliHelper = connectToDali();
        bool onlyLocal = fileNameServiceDali.isEmpty();
        bool onlyDFS = !resolveLocally() && !onlyLocal;

        IUserDescriptor *user = NULL;
        if (wu)
            user = wu->queryUserDescriptor();//ad-hoc mode
        else if (daliHelper)
            user = daliHelper->queryUserDescriptor();//predeployed query mode

        Owned<ILocalOrDistributedFile> ldFile = createLocalOrDistributedFile(fileName, user, onlyLocal, onlyDFS, AccessMode::writeSequential, isPrivilegedUser, &clusters);
        if (!ldFile)
            throw MakeStringException(ROXIE_FILE_ERROR, "Cannot write %s", fileName.str());
        return createRoxieWriteHandler(daliHelper, ldFile.getClear(), clusters);
    }

    //map ambiguous IHpccPackage
    virtual ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const
    {
        return CPackageNode::resolveSuperFile(superFileName);
    }
    virtual const char *queryEnv(const char *varname) const
    {
        return CPackageNode::queryEnv(varname);
    }
    virtual RecordTranslationMode getEnableFieldTranslation() const override
    {
        return CPackageNode::getEnableFieldTranslation();
    }
    virtual bool isCompulsory() const
    {
        return CPackageNode::isCompulsory();
    }
    virtual bool isPreload() const
    {
        return CPackageNode::isPreload();
    }
    virtual const IPropertyTree *queryTree() const
    {
        return CPackageNode::queryTree();
    }
    virtual hash64_t queryHash() const
    {
        return CPackageNode::queryHash();
    }
    virtual const char *queryId() const
    {
        return CPackageNode::queryId();
    }
    virtual bool resolveLocally() const
    {
        return CPackageNode::resolveLocally();
    }
};

CResolvedFileCache CRoxiePackageNode::daliFiles;
CriticalSection CRoxiePackageNode::daliLookupCrits[NUM_DALI_CRITS];

typedef CResolvedPackage<CRoxiePackageNode> CRoxiePackage;

IRoxiePackage *createRoxiePackage(IPropertyTree *p, IRoxiePackageMap *packages)
{
    Owned<CRoxiePackage> pkg = new CRoxiePackage(p);
    pkg->resolveBases(packages);
    return pkg.getClear();
}

//================================================================================================
// CPackageMap - an implementation of IPackageMap using a string map
//================================================================================================

class CRoxiePackageMap : public CPackageMapOf<CRoxiePackageNode, IRoxiePackage>, implements IRoxiePackageMap
{
public:
    IMPLEMENT_IINTERFACE;

    typedef CPackageMapOf<CRoxiePackageNode, IRoxiePackage> BASE;

    CRoxiePackageMap(const char *_packageId, const char *_querySet, bool _active)
        : BASE(_packageId, _querySet, _active)
    {
    }

    //map ambiguous IHpccPackageMap interface
    virtual const IHpccPackage *queryPackage(const char *name) const
    {
        return BASE::queryPackage(name);
    }
    virtual const IHpccPackage *matchPackage(const char *name) const
    {
        return BASE::matchPackage(name);
    }
    virtual const char *queryPackageId() const
    {
        return BASE::queryPackageId();
    }
    virtual bool isActive() const
    {
        return BASE::isActive();
    }
    virtual const StringArray &getPartIds() const
    {
        return BASE::getPartIds();
    }
    virtual bool validate(const StringArray &queryids, const StringArray &queriesToIgnore, StringArray &wrn, StringArray &err, StringArray &unmatchedQueries, StringArray &unusedPackages, StringArray &unmatchedFiles, bool ignoreOptionalFiles) const
    {
        return BASE::validate(queryids, queriesToIgnore, wrn, err, unmatchedQueries, unusedPackages, unmatchedFiles, ignoreOptionalFiles);
    }
    virtual void gatherFileMappingForQuery(const char *queryname, IPropertyTree *fileInfo) const
    {
        BASE::gatherFileMappingForQuery(queryname, fileInfo);
    }
    virtual const IRoxiePackage *queryRoxiePackage(const char *name) const
    {
        return queryResolvedPackage(name);
    }
    virtual const IRoxiePackage *matchRoxiePackage(const char *name) const
    {
        return matchResolvedPackage(name);
    }
};

static CRoxiePackageMap *emptyPackageMap;
static CRoxiePackage *rootPackage;
static SpinLock emptyPackageMapCrit;
static IRoxieDebugSessionManager *debugSessionManager;

extern const IRoxiePackage &queryRootRoxiePackage()
{
    SpinBlock b(emptyPackageMapCrit);
    if (!rootPackage)
    {
        // Set up the root package. This contains global settings from topology file
        rootPackage = new CRoxiePackage(topology); // attributes become control: environment settings. Rest of topology ignored.
        rootPackage->setHash(0);  // we don't include the topology in the package hashes...
        rootPackage->resolveBases(NULL);
    }
    return *rootPackage;
}

extern const IRoxiePackageMap &queryEmptyRoxiePackageMap()
{
    SpinBlock b(emptyPackageMapCrit);
    if (!emptyPackageMap)
        emptyPackageMap = new CRoxiePackageMap("<none>", NULL, true);
    return *emptyPackageMap;
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    emptyPackageMap = NULL;
    debugSessionManager = NULL;
    return true;
}

MODULE_EXIT()
{
    ::Release(emptyPackageMap); // You can't use static Owned to release anything that may own a IPropertyTree
    ::Release(rootPackage);
    ::Release(debugSessionManager);
}

// IRoxieQuerySetManager
//  - CRoxieQuerySetManager -
//    - CRoxieServerQuerySetManager
//    - CRoxieAgentQuerySetManager
//
// Manages a set of instantiated queries and allows us to look them up by queryname or alias
//
// IRoxieQuerySetManagerSet
// - CRoxieAgentQuerySetManagerSet
//
// Manages the IRoxieQuerySetManager for multiple channels
//
// CRoxieQueryPackageManager
// - CRoxieDaliQueryPackageManager
// - CStandaloneQueryPackageManager
//
// Groups a server resource manager and a set of agent resource managers (one per channel) together.
// There is one per PackageMap
//
// CQueryPackageSetManager at outer level
// There will be exactly one of these. It will reload the CQueryPackageManager's if dali Package info changes

//================================================================================================
// CRoxieQuerySetManager - shared base class for agent and server query set manager classes
// Manages a set of instantiated queries and allows us to look them up by queryname or alias,
// as well as controlling their lifespan
//================================================================================================

class CRoxieQuerySetManager : public CInterface, implements IRoxieQuerySetManager 
{
protected:
    MapStringToMyClass<IQueryFactory> queries;
    MapStringToMyClass<IQueryFactory> aliases;   // Do we gain anything by having two tables?
    unsigned channelNo;
    bool active;
    StringAttr querySetName;
    CriticalSection crit;   // For parallel load

    void addQuery(const char *id, IQueryFactory *n)
    {
        {
            CriticalBlock b(crit);
            queries.setValue(id, n);
        }
        n->Release();  // setValue links
    }

    void addAlias(const char *alias, const char *original)
    {
        if (original && alias)
        {
            IQueryFactory *orig = queries.getValue(original);
            if (orig)
            {
                CriticalBlock b(crit);
                aliases.setValue(alias, orig);
            }
            else
                throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", original);
        }
        else
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Invalid parameters to addAlias");
    }

    virtual IQueryFactory *loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, bool forceRetry) = 0;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieQuerySetManager(unsigned _channelNo, const char *_querySetName)
        : queries(true), aliases(true), active(false), querySetName(_querySetName)
    {
        channelNo = _channelNo;
    }

    virtual const char *queryId() const
    {
        return querySetName;
    }

    virtual bool isActive() const
    {
        return active;
    }

    virtual void load(const IPropertyTree *querySet, const IRoxiePackageMap &packages, hash64_t &hash, bool forceRetry)
    {
        unsigned numQueries = const_cast<IPropertyTree *>(querySet)->getCount("Query");
        if (numQueries)
        {
            std::vector<hash64_t> queryHashes(numQueries);
            asyncFor(numQueries, parallelQueryLoadThreads, [this, querySet, &packages, &queryHashes, forceRetry](unsigned i)
            {
                queryHashes[i] = 0;
                VStringBuffer xpath("Query[%u]", i+1);
                const IPropertyTree *query = querySet->queryPropTree(xpath);
                assertex(query);
                const char *id = query->queryProp("@id");
                const char *dllName = query->queryProp("@dll");
                try
                {
                    if (!id || !*id || !dllName || !*dllName)
                        throw MakeStringException(ROXIE_QUERY_MODIFICATION, "dll and id must be specified");
                    Owned<const IQueryDll> queryDll = createQueryDll(dllName);
                    const IHpccPackage *package = NULL;
                    const char *packageName = query->queryProp("@package");
                    if (packageName && *packageName)
                    {
                        package = packages.queryPackage(packageName); // if a package is specified, require exact match
                        if (!package)
                            throw MakeStringException(ROXIE_QUERY_MODIFICATION, "Package %s specified by query %s not found", packageName, id);
                    }
                    else
                    {
                        package = packages.queryPackage(id);  // Look for an exact match, then a fuzzy match, using query name as the package id
                        if(!package) package = packages.matchPackage(id);
                        if (!package) package = &queryRootRoxiePackage();
                    }
                    assertex(package && QUERYINTERFACE(package, const IRoxiePackage));
                    IQueryFactory *qf = loadQueryFromDll(id, queryDll.getClear(), *QUERYINTERFACE(package, const IRoxiePackage), query, forceRetry);
                    queryHashes[i] = qf->queryHash();
                    addQuery(id, qf);
                }
                catch (IException *E)
                {
                    // we don't want a single bad query in the set to stop us loading all the others
                    StringBuffer msg;
                    msg.appendf("Failed to load query %s from %s", id ? id : "(null)", dllName ? dllName : "(null)");
                    EXCLOG(E, msg.str());
                    if (id)
                    {
                        StringBuffer emsg;
                        E->errorMessage(emsg);
                        Owned<IPropertyTree> stateInfo = createPTree();
                        stateInfo->setPropBool("@suspended", true);
                        stateInfo->setProp("@loadFailedReason", emsg);
                        Owned<IQueryFactory> dummyQuery = loadQueryFromDll(id, NULL, queryRootRoxiePackage(), stateInfo, false);
                        queryHashes[i] = dummyQuery->queryHash();
                        addQuery(id, dummyQuery.getClear());
                    }
                    E->Release();
                }
            });
            for (auto h : queryHashes)
                hash = rtlHash64Data(sizeof(h), &h, hash);
        }
        unsigned numAliases = const_cast<IPropertyTree *>(querySet)->getCount("Alias");
        if (numAliases)
        {
            std::vector<hash64_t> aliasHashes(numAliases);
            asyncFor(numAliases, parallelQueryLoadThreads, [this, querySet, &aliasHashes](unsigned i)
            {
                aliasHashes[i] = 0;
                VStringBuffer xpath("Alias[%u]", i+1);
                IPropertyTree *item = querySet->queryPropTree(xpath);
                assertex(item);
                const char *alias = item->queryProp("@name");
                const char *original = item->queryProp("@id");
                try
                {
                    addAlias(alias, original);
                    hash64_t hash = rtlHash64VStr(alias, 0);
                    aliasHashes[i] = rtlHash64VStr(original, hash);
                }
                catch (IException *E)
                {
                    // we don't want a single bad alias in the set to stop us loading all the others
                    VStringBuffer msg("Failed to set alias %s on %s", alias, original);
                    EXCLOG(E, msg.str());
                    E->Release();
                    aliasHashes[i] = 0;
                }
            });
            for (auto h : aliasHashes)
                hash = rtlHash64Data(sizeof(h), &h, hash);
        }
        active = packages.isActive();
        if (active)
            hash = rtlHash64VStr("active", hash);
    }

    virtual void getStats(const char *queryName, const char *graphName, IConstWorkUnit *statsWu, unsigned channel, bool reset, const IRoxieContextLogger &logctx) const override
    {
        Owned<IQueryFactory> f = getQuery(queryName, NULL, logctx);
        if (f)
        {
            f->gatherStats(statsWu, graphName, channel, reset);
        }
        else
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName);
    }

    virtual void resetQueryTimings(const char *queryName, const IRoxieContextLogger &logctx)
    {
        Owned<IQueryFactory> f = getQuery(queryName, NULL, logctx);
        if (f)
            f->resetQueryTimings();
        else
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName);
    }

    virtual void resetAllQueryTimings()
    {
        HashIterator elems(queries);
        for (elems.first(); elems.isValid(); elems.next())
        {
            IMapping &cur = elems.query();
            queries.mapToValue(&cur)->resetQueryTimings();
        }
    }

    virtual void getActivityMetrics(StringBuffer &reply) const
    {
        HashIterator elems(queries);
        for (elems.first(); elems.isValid(); elems.next())
        {
            IMapping &cur = elems.query();
            queries.mapToValue(&cur)->getActivityMetrics(reply);
        }
    }

    virtual void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieQuerySetManagerSet *agents, const IRoxieContextLogger &logctx) const
    {
        HashIterator elems(queries);
        for (elems.first(); elems.isValid(); elems.next())
        {
            IMapping &cur = elems.query();
            IQueryFactory *query = queries.mapToValue(&cur);
            IArrayOf<IQueryFactory> agentQueries;
            agents->getQueries(query->queryQueryName(), agentQueries, logctx);
            query->getQueryInfo(reply, full, &agentQueries, logctx);
        }
        HashIterator aliasIterator(aliases);
        for (aliasIterator.first(); aliasIterator.isValid(); aliasIterator.next())
        {
            IMapping &cur = aliasIterator.query();
            reply.appendf(" <Alias id='%s' query='%s'/>\n", (const char *) cur.getKey(), aliases.mapToValue(&cur)->queryQueryName());
        }
    }

    virtual void preloadOnce() const
    {
        HashIterator elems(queries);
        for (elems.first(); elems.isValid(); elems.next())
        {
            IMapping &cur = elems.query();
            IQueryFactory *query = queries.mapToValue(&cur);
            query->preloadOnce();
        }
    }

    virtual IQueryFactory *getQuery(const char *id, StringBuffer *querySet, const IRoxieContextLogger &logctx) const
    {
        if (querySet && querySet->length() && !streq(querySet->str(), querySetName))
            return NULL;
        IQueryFactory *ret;
        ret = aliases.getValue(id);
        if (!ret)
            ret = queries.getValue(id);
        if (ret && querySet)
            querySet->set(querySetName);
        return LINK(ret);
    }

};

//===============================================================================================================

class CRoxieServerQuerySetManager : public CRoxieQuerySetManager
{
public:
    CRoxieServerQuerySetManager(const char *_querySetName)
        : CRoxieQuerySetManager(0, _querySetName)
    {
    }

    virtual IQueryFactory * loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, bool forceRetry)
    {
        return createServerQueryFactory(id, dll, package, stateInfo, false, forceRetry);
    }

};

extern IRoxieQuerySetManager *createServerManager(const char *querySet)
{
    return new CRoxieServerQuerySetManager(querySet);
}

//===============================================================================================================

class CRoxieAgentQuerySetManager : public CRoxieQuerySetManager
{
public:
    CRoxieAgentQuerySetManager(unsigned _channelNo, const char *_querySetName)
        : CRoxieQuerySetManager(_channelNo, _querySetName)
    {
        channelNo = _channelNo;
    }

    virtual IQueryFactory *loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, bool forceRetry)
    {
        return createAgentQueryFactory(id, dll, package, channelNo, stateInfo, false, forceRetry);
    }

};

class CRoxieAgentQuerySetManagerSet : public CInterface, implements IRoxieQuerySetManagerSet
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieAgentQuerySetManagerSet(unsigned _numChannels, const char *querySetName)
        : numChannels(_numChannels)
    {
        managers = new CRoxieAgentQuerySetManager *[numChannels];
        memset(managers, 0, sizeof(CRoxieAgentQuerySetManager *) * numChannels);
        Owned<const ITopologyServer> topology = getTopology();
        for (unsigned channelNo : topology->queryChannels())
        {
            assertex(channelNo>0 && channelNo<=numChannels);
            if (managers[channelNo-1] == NULL)
                managers[channelNo-1] = new CRoxieAgentQuerySetManager(channelNo, querySetName);
            else
                throw MakeStringException(ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channel %d repeated for this agent", channelNo);
        }
    }

    ~CRoxieAgentQuerySetManagerSet()
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            ::Release(managers[channel]);
        delete [] managers;
    }

    inline CRoxieAgentQuerySetManager *item(int idx)
    {
        return managers[idx];
    }

    virtual void load(const IPropertyTree *querySets, const IRoxiePackageMap &packages, hash64_t &hash, bool forceRetry)
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            if (managers[channel])
                managers[channel]->load(querySets, packages, hash, forceRetry); // MORE - this means the hash depends on the number of channels. Is that desirable?
    }

    virtual void preloadOnce() override
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            if (managers[channel])
                managers[channel]->preloadOnce();
    }

    virtual void getQueries(const char *id, IArrayOf<IQueryFactory> &queries, const IRoxieContextLogger &logctx) const
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            if (managers[channel])
            {
                IQueryFactory *query = managers[channel]->getQuery(id, NULL, logctx);
                if (query)
                    queries.append(*query);
            }
    }

private:
    unsigned numChannels;
    CRoxieAgentQuerySetManager **managers;
};

//===============================================================================================================

class CRoxieDebugSessionManager : implements IRoxieDebugSessionManager, public CInterface
{
protected:
    ReadWriteLock debugLock; 
    MapStringToMyClass<IDebuggerContext> debuggerContexts;

public:
    IMPLEMENT_IINTERFACE;
    void getActiveQueries(StringBuffer &reply)
    {
        HashIterator q(debuggerContexts);
        for (q.first(); q.isValid(); q.next())
        {
            IDebuggerContext *ctx = debuggerContexts.mapToValue(&q.query());
            reply.appendf(" <Query id='%s' uid='%s' debug='1'/>\n", ctx->queryQueryName(), ctx->queryDebugId());
        }
    }

    virtual void registerDebugId(const char *id, IDebuggerContext *ctx)
    {
        WriteLockBlock block(debugLock);
        debuggerContexts.setValue(id, ctx);
    }

    virtual void deregisterDebugId(const char *id)
    {
        WriteLockBlock block(debugLock);
        debuggerContexts.remove(id);
    }

    virtual IDebuggerContext *lookupDebuggerContext(const char *id)
    {
        ReadLockBlock block(debugLock);
        IDebuggerContext *ctx = debuggerContexts.getValue(id);
        if (ctx)
            return LINK(ctx);
        else
        {
#ifdef _DEBUG
            // In a debug environment, it is convenient to be able to use '*' to mean 'the only active debug session'...
            if (strcmp(id, "*")==0 && debuggerContexts.count()==1)
            {
                HashIterator q(debuggerContexts);
                for (q.first(); q.isValid(); q.next())
                {
                    IDebuggerContext *ctx = debuggerContexts.mapToValue(&q.query());
                    return LINK(ctx);
                }
            }
#endif
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Debug context %s not found", id);
        }
    }
};

//===============================================================================================

/*----------------------------------------------------------------------------------------------
* A CRoxieQueryPackageManager object manages all the queries that are currently runnable via XML.
* There may be more than one in existence, but only one will be active and therefore used to
* look up queries that are received - this corresponds to the currently active package.
*-----------------------------------------------------------------------------------------------*/

static hash64_t hashXML(const IPropertyTree *tree)
{
    StringBuffer xml;
    toXML(tree, xml, 0, XML_SortTags);
    return rtlHash64Data(xml.length(), xml.str(), 877029);
}

class CRoxieQueryPackageManagerBase : public CInterface
{
public:
    virtual hash64_t getHash()
    {
        CriticalBlock b2(updateCrit);
        return queryHash;
    }

    IRoxieQuerySetManager* getRoxieServerManager() const
    {
        CriticalBlock b2(updateCrit);
        return serverManager.getLink();
    }

    IRoxieQuerySetManagerSet* getRoxieAgentManagers() const
    {
        CriticalBlock b2(updateCrit);
        return agentManagers.getLink();
    }

protected:
    ~CRoxieQueryPackageManagerBase()
    {
        if (agentQueryReleaseDelaySeconds)
            delayedReleaser->delayedRelease(agentManagers.getClear(), agentQueryReleaseDelaySeconds);
    }

    // Derived classes wanting to read serverManager or agentManagers must call this function to safely obtain their current values

    void getQueryManagers(Owned<IRoxieQuerySetManager> &_serverManager, Owned<CRoxieAgentQuerySetManagerSet> &_agentManagers) const
    {
        CriticalBlock b2(updateCrit);
        _serverManager.set(serverManager);
        _agentManagers.set(agentManagers);
    }

    void reloadQueryManagers(CRoxieAgentQuerySetManagerSet *newAgentManagers, IRoxieQuerySetManager *newServerManager, hash64_t newHash)
    {
        Owned<CRoxieAgentQuerySetManagerSet> oldAgentManagers;
        Owned<IRoxieQuerySetManager> oldServerManager;
        {
            // Atomically, replace the existing query managers with the new ones
            CriticalBlock b2(updateCrit);
            oldAgentManagers.setown(agentManagers.getClear()); // so that the release happens outside the critblock
            oldServerManager.setown(serverManager.getClear()); // so that the release happens outside the critblock
            agentManagers.setown(newAgentManagers);
            serverManager.setown(newServerManager);
            queryHash = newHash;
        }
        if (agentQueryReleaseDelaySeconds)
            delayedReleaser->delayedRelease(oldAgentManagers.getClear(), agentQueryReleaseDelaySeconds);
    }

private:
    mutable CriticalSection updateCrit;  // protects updates of agentManagers and serverManager, and queryHash. Must be held ONLY to link, release, or overwrite these values.
    Owned<CRoxieAgentQuerySetManagerSet> agentManagers;
    Owned<IRoxieQuerySetManager> serverManager;
    hash64_t queryHash = 0;
};

class CRoxieQueryPackageManager : public CRoxieQueryPackageManagerBase
{
public:
    CRoxieQueryPackageManager(unsigned _numChannels, const char *_querySet, const IRoxiePackageMap *_packages, hash64_t _xmlHash)
        : packages(_packages), numChannels(_numChannels), xmlHash(_xmlHash), querySet(_querySet)
    {
    }

    ~CRoxieQueryPackageManager()
    {
    }

    inline const char *queryPackageId() const
    {
        return packages->queryPackageId();
    }

    virtual void reloadIncremental()
    {
    }

    virtual void load(bool forceReload) = 0;

    bool matches(hash64_t _xmlHash, bool _active) const
    {
        return _xmlHash == xmlHash && _active==packages->isActive();
    }

    void getInfo(StringBuffer &reply, const IRoxieContextLogger &logctx) const
    {
        reply.appendf(" <PackageSet id=\"%s\" querySet=\"%s\"", queryPackageId(), querySet.get());
        if (!packages || !packages->getPartIds().ordinality())
        {
            reply.append("/>\n");
            return;
        }
        reply.append(">\n");
        const StringArray &parts = packages->getPartIds();
        ForEachItemIn(i, parts)
            reply.appendf("  <Part id='%s'/>\n", parts.item(i));
        reply.append(" </PackageSet>\n");
    }

    bool resetStats(const char *queryId, const IRoxieContextLogger &logctx)
    {
        Owned<IRoxieQuerySetManager> serverManager;
        Owned<CRoxieAgentQuerySetManagerSet> agentManagers;
        getQueryManagers(serverManager, agentManagers);
        if (queryId)
        {
            Owned<IQueryFactory> query = serverManager->getQuery(queryId, NULL, logctx);
            if (!query)
                return false;
            const char *id = query->queryQueryName();
            serverManager->resetQueryTimings(id, logctx);
            for (unsigned channel = 0; channel < numChannels; channel++)
                if (agentManagers->item(channel))
                {
                    agentManagers->item(channel)->resetQueryTimings(id, logctx);
                }
        }
        else
        {
            serverManager->resetAllQueryTimings();
            for (unsigned channel = 0; channel < numChannels; channel++)
                if (agentManagers->item(channel))
                    agentManagers->item(channel)->resetAllQueryTimings();
        }
        return true;
    }

    bool getStats(const char *queryId, const char *graphName, StringBuffer &reply, const char *wuid, const IRoxieContextLogger &logctx) const
    {
        Owned<IRoxieQuerySetManager> serverManager;
        Owned<CRoxieAgentQuerySetManagerSet> agentManagers;
        getQueryManagers(serverManager, agentManagers);
        if (serverManager->isActive())
        {
            Owned<IQueryFactory> query = serverManager->getQuery(queryId, NULL, logctx);
            if (query)
            {
                bool reset = false;  // MORE - tidy up around here.
                Owned<IConstWorkUnit> statsWu;
                if (wuid)
                {
                    Owned<IRoxieDaliHelper> daliHelper = ::connectToDali();
                    if (!daliHelper->connected())
                        throw makeStringException(ROXIE_CONTROL_MSG_ERROR, "Can't create stats WU - dali not connected");
                    statsWu.setown(daliHelper->createStatsWorkUnit(wuid, query->queryDll()->queryName()));
                }
                else
                {
                    statsWu.setown(createLocalWorkUnitFromPTree(createPTreeFromIPT(queryExtendedWU(query->queryWorkUnit())->queryPTree())));
                }
                query->gatherStats(statsWu, graphName, -1, reset);
                for (unsigned channel = 0; channel < numChannels; channel++)
                    if (agentManagers->item(channel))
                        agentManagers->item(channel)->getStats(queryId, graphName, statsWu, channel+1, reset, logctx);
                if (!wuid || *wuid=='*')
                {
                    WorkunitUpdate wu(&statsWu->lock());
                    wu->setState(WUStateCompleted);   // We don't set the state when updating existing workunits
                }
                reply.appendf("<Query id='%s'>\n", queryId);
                if (wuid)
                    reply.appendf(" <wuid>%s</wuid>\n", statsWu->queryWuid());
                else
                {
                    reply.appendf("<Graphs>\n");
                    Owned<IConstWUGraphIterator> graphs = &statsWu->getGraphs(GraphTypeActivities);
                    ForEach(*graphs)
                    {
                        IConstWUGraph &graph = graphs->query();
                        Owned<IPropertyTree> xgmml = graph.getXGMMLTree(true, false);  // We can't merge between nodes if we format the values
                        unsigned wfid = xgmml->getPropInt("@wfid");
                        SCMStringBuffer s;
                        reply.appendf("<Graph name='%s' wfid='%u'>\n <xgmml>\n", graph.getName(s).str(), wfid);
                        toXML(xgmml, reply, 2);
                        reply.append(" </xgmml>\n</Graph>\n");
                    }
                    reply.appendf("</Graphs>\n");
                }
                reply.append("</Query>\n");
                return true;
            }
        }
        return false;
    }
    void getActivityMetrics(StringBuffer &reply) const
    {
        Owned<IRoxieQuerySetManager> serverManager;
        Owned<CRoxieAgentQuerySetManagerSet> agentManagers;
        getQueryManagers(serverManager, agentManagers);
        serverManager->getActivityMetrics(reply);
        for (unsigned channel = 0; channel < numChannels; channel++)
        {
            if (agentManagers->item(channel))
            {
                agentManagers->item(channel)->getActivityMetrics(reply);
            }
        }
    }
    void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieContextLogger &logctx) const
    {
        Owned<IRoxieQuerySetManager> serverManager;
        Owned<CRoxieAgentQuerySetManagerSet> agentManagers;
        getQueryManagers(serverManager, agentManagers);
        serverManager->getAllQueryInfo(reply, full, agentManagers, logctx);
    }
    const char *queryQuerySetName()
    {
        return querySet;
    }

    // These are set at construction and not changed for the lifetime of the object
    const Owned<const IRoxiePackageMap> packages;
    const unsigned numChannels;
    const hash64_t xmlHash;
    const StringAttr querySet;
};

/**
 * class CRoxieDaliQueryPackageManager - manages queries specified in QuerySets, for a given package set.
 *
 * If the QuerySet is modified, it will be reloaded.
 * There is one CRoxieDaliQueryPackageManager for every PackageSet - only one will be active for query lookup
 * at a given time (the one associated with the active PackageSet).
 *
 * To deploy new data, typically we will load a new PackageSet, make it active, then release the old one
 * A packageSet is not modified while loaded, to avoid timing issues between agents and server.
 *
 * We need to be able to spot a change (in dali) to the active package indicator (and switch the active CRoxieDaliQueryPackageManager)
 * We need to be able to spot a change (in dali) that adds a new PackageSet
 * We need to decide what to do about a change (in dali) to an existing PackageSet. Maybe we allow it (leave it up to the gui to
 * encourage changing in the right sequence). In which case a change to the package info in dali means reload all global package
 * managers (and then discard the old ones). Hash-based queries means everything should work ok.
 * -> If the active ptr changes, just change what is active
 *    If any change to any package set, reload all globalResourceManagers and discard prior
 *    The query caching code should ensure that it is quick enough to do so
 *
 **/
class CRoxieDaliQueryPackageManager : public CRoxieQueryPackageManager, implements ISafeSDSSubscription
{
    Owned<IRoxieDaliHelper> daliHelper;
    Owned<IDaliPackageWatcher> notifier;
    std::atomic<bool> dirty{false};

public:
    IMPLEMENT_IINTERFACE;
    CRoxieDaliQueryPackageManager(unsigned _numChannels, const IRoxiePackageMap *_packages, const char *_querySet, hash64_t _xmlHash)
        : CRoxieQueryPackageManager(_numChannels, _querySet, _packages, _xmlHash)
    {
        daliHelper.setown(connectToDali());
    }

    ~CRoxieDaliQueryPackageManager()
    {
        if (notifier)
            daliHelper->releaseSubscription(notifier);
    }

    virtual ISafeSDSSubscription *linkIfAlive() override { return isAliveAndLink() ? this : nullptr; }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData) override
    {
        //Mark this queryset as potentially modified - and then request an incremental reload
        dirty = true;
        globalPackageSetManager->requestReload(false, false, true);
    }

    virtual void load(bool forceReload) override
    {
        notifier.setown(daliHelper->getQuerySetSubscription(querySet, this));
        reload(forceReload);
    }

    virtual void reloadIncremental() override
    {
        if (dirty.exchange(false))
            reload(false);
    }

private:
    void reload(bool forceRetry)
    {
        hash64_t newHash = numChannels;
        Owned<IPropertyTree> newQuerySet = daliHelper->getQuerySet(querySet);
        Owned<CRoxieAgentQuerySetManagerSet> newAgentManagers = new CRoxieAgentQuerySetManagerSet(numChannels, querySet);
        Owned<IRoxieQuerySetManager> newServerManager = createServerManager(querySet);
        newServerManager->load(newQuerySet, *packages, newHash, forceRetry);
        newAgentManagers->load(newQuerySet, *packages, newHash, forceRetry);
        reloadQueryManagers(newAgentManagers.getClear(), newServerManager.getClear(), newHash);
    }
};

class CStandaloneQueryPackageManager : public CRoxieQueryPackageManager
{
    Owned<IPropertyTree> standaloneDll;

public:
    CStandaloneQueryPackageManager(unsigned _numChannels, const char *_querySet, const IRoxiePackageMap *_packages, IPropertyTree *_standaloneDll)
        : CRoxieQueryPackageManager(_numChannels, _querySet, _packages, 0), standaloneDll(_standaloneDll)
    {
        assertex(standaloneDll);
    }

    ~CStandaloneQueryPackageManager()
    {
    }

    virtual void load(bool forceReload) override
    {
        hash64_t newHash = numChannels;
        Owned<IPropertyTree> newQuerySet = createPTree("QuerySet", ipt_lowmem);
        newQuerySet->setProp("@name", "_standalone");
        newQuerySet->addPropTree("Query", standaloneDll.getLink());
        Owned<CRoxieAgentQuerySetManagerSet> newAgentManagers = new CRoxieAgentQuerySetManagerSet(numChannels, querySet);
        Owned<IRoxieQuerySetManager> newServerManager = createServerManager(querySet);
        newServerManager->load(newQuerySet, *packages, newHash, forceReload);
        newAgentManagers->load(newQuerySet, *packages, newHash, forceReload);
        reloadQueryManagers(newAgentManagers.getClear(), newServerManager.getClear(), newHash);
    }
};

static SpinLock roxieDebugSessionManagerLock;
extern IRoxieDebugSessionManager &queryRoxieDebugSessionManager()
{
    SpinBlock b(roxieDebugSessionManagerLock);
    if (!debugSessionManager)
        debugSessionManager = new CRoxieDebugSessionManager();
    return *debugSessionManager;
}

class CRoxiePackageSetWatcher : public CInterface
{
public:
    CRoxiePackageSetWatcher(IRoxieDaliHelper *_daliHelper, unsigned numChannels, CRoxiePackageSetWatcher *oldPackages, bool forceReload)
    : daliHelper(_daliHelper), stateHash(0)
    {
        ForEachItemIn(idx, allQuerySetNames)
        {
            createQueryPackageManagers(numChannels, allQuerySetNames.item(idx), oldPackages, forceReload);
        }
    }

    CRoxiePackageSetWatcher(IRoxieDaliHelper *_daliHelper, const IQueryDll *standAloneDll, unsigned numChannels, const char *querySet, bool forceReload)
    : daliHelper(_daliHelper), stateHash(0)
    {
        Owned<IPropertyTree> standAloneDllTree;
        standAloneDllTree.setown(createPTree("Query", ipt_lowmem));
        standAloneDllTree->setProp("@id", "roxie");
        standAloneDllTree->setProp("@dll", standAloneDll->queryDll()->queryName());
        Owned<CRoxieQueryPackageManager> qpm = new CStandaloneQueryPackageManager(numChannels, querySet, LINK(&queryEmptyRoxiePackageMap()), standAloneDllTree.getClear());
        qpm->load(forceReload);
        stateHash = rtlHash64Data(sizeof(stateHash), &stateHash, qpm->getHash());
        allQueryPackages.append(*qpm.getClear());
    }

    IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
            if (sm->isActive())
            {
                Owned<IQueryFactory> library = sm->getQuery(libraryName, NULL, logctx);
                if (library)
                {
                    if (library->isQueryLibrary())
                    {
                        unsigned foundInterfaceHash = library->getQueryLibraryInterfaceHash();
                        if (!foundInterfaceHash || (foundInterfaceHash == expectedInterfaceHash))
                            return library.getClear();
                        else
                            throw MakeStringException(ROXIE_LIBRARY_ERROR, "The library interface found in %s is not compatible (found %d, expected %d)", libraryName, foundInterfaceHash, expectedInterfaceHash);
                    }
                    else
                        throw MakeStringException(ROXIE_LIBRARY_ERROR, "The query resolved by %s is not a library", libraryName);
                }
            }
        }
        throw MakeStringException(ROXIE_LIBRARY_ERROR, "No library available for %s", libraryName);
    }

    IQueryFactory *getQuery(const char *id, StringBuffer *querySet, IArrayOf<IQueryFactory> *agentQueries, const IRoxieContextLogger &logctx) const
    {
        if (querySet && querySet->length() && !allQuerySetNames.contains(querySet->str()))
            throw MakeStringException(ROXIE_INVALID_TARGET, "Target %s not found", querySet->str());
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
            if (sm->isActive())
            {
                IQueryFactory *query = sm->getQuery(id, querySet, logctx);
                if (query)
                {
                    if (agentQueries)
                    {
                        Owned<IRoxieQuerySetManagerSet> agentManagers = allQueryPackages.item(idx).getRoxieAgentManagers();
                        agentManagers->getQueries(id, *agentQueries, logctx);
                    }
                    return query;
                }
            }
        }
        return NULL;
    }

    int getActivePackageCount() const
    {
        int count = 0;
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
            if (sm->isActive())
                count++;
        }
        return count;
    }


    inline hash64_t queryHash() const
    {
        return stateHash;
    }

    void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieContextLogger &logctx) const
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> serverManager = allQueryPackages.item(idx).getRoxieServerManager();
            if (serverManager->isActive())
            {
                Owned<IRoxieQuerySetManagerSet> agentManagers = allQueryPackages.item(idx).getRoxieAgentManagers();
                serverManager->getAllQueryInfo(reply, full, agentManagers, logctx);
            }
        }
    }

    void preloadOnce() const
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> serverManager = allQueryPackages.item(idx).getRoxieServerManager();
            if (serverManager->isActive())
            {
                serverManager->preloadOnce();
                Owned<IRoxieQuerySetManagerSet> agentManagers = allQueryPackages.item(idx).getRoxieAgentManagers();
                agentManagers->preloadOnce();
            }
        }
    }

    void getActivityMetrics(StringBuffer &reply) const
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            CRoxieQueryPackageManager &qpm = allQueryPackages.item(idx);
            qpm.getActivityMetrics(reply);
        }
    }

    void getInfo(StringBuffer &reply, const IRoxieContextLogger &logctx) const
    {
        reply.append("<PackageSets>\n");
        ForEachItemIn(idx, allQueryPackages)
        {
            allQueryPackages.item(idx).getInfo(reply, logctx);
        }
        reply.append("</PackageSets>\n");
    }

    void getStats(StringBuffer &reply, const char *id, const char *graphName, const char *wuid, const IRoxieContextLogger &logctx) const
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            if (allQueryPackages.item(idx).getStats(id, graphName, reply, wuid, logctx))
               return;
        }
    }

    void resetStats(const char *target, const char *id, const IRoxieContextLogger &logctx) const
    {
        bool matched = false;
        ForEachItemIn(idx, allQueryPackages)
        {
            CRoxieQueryPackageManager &queryPackage = allQueryPackages.item(idx);
            if (target && *target && !strieq(queryPackage.queryQuerySetName(), target))
                continue;
            if (allQueryPackages.item(idx).resetStats(id, logctx))
            {
                if (target && *target)
                    return;
                matched = true;
            }
        }
        if (!matched && id && *id)
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", id);
    }

    void reloadIncremental()
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            allQueryPackages.item(idx).reloadIncremental();
        }
    }
private:
    CIArrayOf<CRoxieQueryPackageManager> allQueryPackages;
    Linked<IRoxieDaliHelper> daliHelper;
    hash64_t stateHash;

    CRoxieQueryPackageManager *getPackageManager(const char *id)
    {
        ForEachItemIn(idx, allQueryPackages)
        {
            CRoxieQueryPackageManager &pm = allQueryPackages.item(idx);
            if (strcmp(pm.queryPackageId(), id)==0)
                return LINK(&pm);
        }
        return NULL;
    }

    void createQueryPackageManager(unsigned numChannels, const IRoxiePackageMap *packageMap, const char *querySet, hash64_t xmlHash, bool forceReload)
    {
        Owned<CRoxieQueryPackageManager> qpm = new CRoxieDaliQueryPackageManager(numChannels, packageMap, querySet, xmlHash);
        qpm->load(forceReload);
        stateHash = rtlHash64Data(sizeof(stateHash), &stateHash, qpm->getHash());
        allQueryPackages.append(*qpm.getClear());
    }

    void createQueryPackageManagers(unsigned numChannels, const char *querySet, CRoxiePackageSetWatcher *oldPackages, bool forceReload)
    {
        int loadedPackages = 0;
        int activePackages = 0;
        Owned<IPropertyTree> packageTree = daliHelper->getPackageSets();
        Owned<IPropertyTreeIterator> packageSets = packageTree->getElements("PackageSet");
        ForEach(*packageSets)
        {
            IPropertyTree &ps = packageSets->query();
            const char *packageSetSpec = ps.queryProp("@process");
            if (!packageSetSpec || WildMatch(roxieName, packageSetSpec, false))
            {
                if (traceLevel)
                {
                    DBGLOG("Loading package set %s, process spec %s", ps.queryProp("@id") ?  ps.queryProp("@id") : "<no-id>",
                                                                      packageSetSpec ? packageSetSpec : "<*>");
                }
                Owned<IPropertyTreeIterator> packageMaps = ps.getElements("PackageMap");
                ForEach(*packageMaps)
                {
                    IPropertyTree &pm = packageMaps->query();
                    const char *packageMapId = pm.queryProp("@id");
                    const char *packageMapFilter = pm.queryProp("@querySet");
                    if (packageMapId && *packageMapId && (!packageMapFilter || WildMatch(querySet, packageMapFilter, false)))
                    {
                        try
                        {
                            bool isActive = pm.getPropBool("@active", true);
                            Owned<IPropertyTree> xml = daliHelper->getPackageMap(packageMapId);
                            hash64_t xmlHash = hashXML(xml);
                            Owned<CRoxieQueryPackageManager> oldPackageManager;
                            if (oldPackages)
                            {
                                oldPackageManager.setown(oldPackages->getPackageManager(packageMapId));
                            }
                            if (oldPackageManager && oldPackageManager->matches(xmlHash, isActive))
                            {
                                //Check for any changes to the queryset
                                oldPackageManager->reloadIncremental();
                                if (traceLevel)
                                    DBGLOG("Package map %s, active %s already loaded", packageMapId, isActive ? "true" : "false");
                                stateHash = rtlHash64Data(sizeof(stateHash), &stateHash, oldPackageManager->getHash());
                                allQueryPackages.append(*oldPackageManager.getClear());
                            }
                            else
                            {
                                if (traceLevel)
                                    DBGLOG("Loading package map %s, active %s", packageMapId, isActive ? "true" : "false");
                                Owned<CRoxiePackageMap> packageMap = new CRoxiePackageMap(packageMapId, packageMapFilter, isActive);
                                packageMap->load(xml);
                                createQueryPackageManager(numChannels, packageMap.getLink(), querySet, xmlHash, forceReload);
                            }
                            loadedPackages++;
                            if (isActive)
                                activePackages++;
                        }
                        catch (IException *E)
                        {
                            StringBuffer msg;
                            msg.appendf("Failed to load package map %s", packageMapId);
                            EXCLOG(E, msg.str());
                            E->Release();
                        }
                    }
                }
            }
        }
        if (!loadedPackages)
        {
            if (traceLevel)
                DBGLOG("Loading empty package for QuerySet %s", querySet);
            createQueryPackageManager(numChannels, LINK(&queryEmptyRoxiePackageMap()), querySet, 0, forceReload);
        }
        else if (traceLevel)
            DBGLOG("Loaded %d packages (%d active)", loadedPackages, activePackages);
    }

};

class CRoxiePackageSetManager : implements IRoxieQueryPackageManagerSet, implements ISafeSDSSubscription, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxiePackageSetManager(const IQueryDll *_standAloneDll) :
        standAloneDll(_standAloneDll), autoReloadThread(*this)
    {
        if (topology && topology->getPropBool("@lockDali", false))
            daliHelper.setown(connectToDali());
        else
            daliHelper.setown(connectToDali(ROXIE_DALI_CONNECT_TIMEOUT));
        pSetsNotifier.setown(daliHelper->getPackageSetsSubscription(this));
        pMapsNotifier.setown(daliHelper->getPackageMapsSubscription(this));
    }

    ~CRoxiePackageSetManager()
    {
        autoReloadThread.stop();
        autoReloadThread.join();
        if (pSetsNotifier)
            daliHelper->releaseSubscription(pSetsNotifier);
        if (pMapsNotifier)
            daliHelper->releaseSubscription(pMapsNotifier);
    }

    virtual ISafeSDSSubscription *linkIfAlive() override { return isAliveAndLink() ? this : nullptr; }

    void requestReload(bool waitUntilComplete, bool forceRetry, bool incremental)
    {
        assertex(!(incremental && forceRetry));
        if (!incremental)
            autoAllIncremental = false;
        if (forceRetry)
            autoForceRetry = true;
        if (waitUntilComplete)
            ++autoSignalsPending;
        ++autoPending;
        autoReloadTrigger.signal();
        if (waitUntilComplete)
            autoReloadComplete.wait();
    }

    //Return if there is no active reload, or when a reload has been completed
    void waitForReload()
    {
        if (autoPending == 0)
            return;

        ++autoSignalsPending;
        autoReloadTrigger.signal();
        autoReloadComplete.wait();
    }


    virtual void load()
    {
        try
        {
            reload(false);
            daliHelper->commitCache();
            controlSem.signal();
            autoReloadThread.start(false);   // Don't want to overlap auto-reloads with the initial load
        }
        catch(IException *E)
        {
            EXCLOG(E, "No configuration could be loaded");
            controlSem.interrupt();
            throw; // Roxie will refuse to start up if configuration invalid
        }
    }

    virtual void doControlMessage(IPropertyTree *xml, StringBuffer &reply, const IRoxieContextLogger &logctx)
    {
        if (!controlSem.wait(20000))
            throw MakeStringException(ROXIE_TIMEOUT, "Timed out waiting for current control query to complete");
        try
        {
            _doControlMessage(xml, reply, logctx);
            reply.append(" <Status>ok</Status>\n");
        }
        catch(IException *E)
        {
            controlSem.signal();
            EXCLOG(E);
            throw;
        }
        catch(...)
        {
            controlSem.signal();
            throw;
        }
        controlSem.signal();
    }

    virtual IQueryFactory *lookupLibrary(const char *libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const
    {
        ReadLockBlock b(packageCrit);
        return allQueryPackages->lookupLibrary(libraryName, expectedInterfaceHash, logctx);
    }

    virtual IQueryFactory *getQuery(const char *id, StringBuffer *querySet, IArrayOf<IQueryFactory> *agentQueries, const IRoxieContextLogger &logctx) const
    {
        ReadLockBlock b(packageCrit);
        return allQueryPackages->getQuery(id, querySet, agentQueries, logctx);
    }

    virtual int getActivePackageCount() const
    {
        ReadLockBlock b(packageCrit);
        return allQueryPackages->getActivePackageCount();
    }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        requestReload(false, false, false);
    }

private:
    Owned<const IQueryDll> standAloneDll;
    Owned<CRoxieDebugSessionManager> debugSessionManager;
    Owned<IRoxieDaliHelper> daliHelper;
    Owned<IDaliPackageWatcher> pSetsNotifier;
    Owned<IDaliPackageWatcher> pMapsNotifier;
    mutable ReadWriteLock packageCrit;
    InterruptableSemaphore controlSem;
    Owned<CRoxiePackageSetWatcher> allQueryPackages;

    Semaphore autoReloadTrigger;
    Semaphore autoReloadComplete;
    std::atomic<unsigned> autoSignalsPending{0};
    std::atomic<unsigned> autoPending{0};
    std::atomic<bool> autoAllIncremental{true};
    std::atomic<bool> autoForceRetry{false};

    class AutoReloadThread : public Thread
    {
        static constexpr unsigned waitForReloadDelayMs = 500; // How long to wait for an explicit <control:reload>
        static constexpr unsigned NotifyMergeDelayMs = 50; // How long to wait for other notifications before reloading the querySet
        std::atomic<bool> closing{false};
        CRoxiePackageSetManager &owner;
    public:
        AutoReloadThread(CRoxiePackageSetManager &_owner)
        : Thread("AutoReloadThread"), owner(_owner)
        {
        }

        virtual int run()
        {
            if (traceLevel)
                DBGLOG("AutoReloadThread %p starting", this);

            while (!closing)
            {
                owner.autoReloadTrigger.wait();
                if (closing)
                    break;

                //If there has been an update to a packagemap or queryset, there may also be a control:reload in quick succession, so wait for it.
                //NOTE: control:reload generally locks the roxie connection and waits for a response, so it is unlikely that
                //multiple control:reloads will be received at the same time.
                CCycleTimer mergeTimer;
                unsigned elapsedMs = 0;
                //Keep waiting until we receive an explicit control:reload or the timeout expires
                while (!owner.autoSignalsPending)
                {
                    owner.autoReloadTrigger.wait(waitForReloadDelayMs - elapsedMs);
                    elapsedMs = mergeTimer.elapsedMs();
                    if (elapsedMs >= waitForReloadDelayMs)
                        break;
                }

                if (closing)
                    break;

                unsigned prevPending = owner.autoPending.load();
                unsigned waits = 0;
                if (prevPending)
                {
                    //Often there are many requests to reload query sets, and perform a global reload.
                    //Iterate in a loop to combine all the requests within a time period to avoid repeated reloads.
                    for (;;)
                    {
                        MilliSleep(NotifyMergeDelayMs);
                        waits++;

                        unsigned nextPending = owner.autoPending.load();
                        //Check to see if any other requests came in during the small delay.
                        if (prevPending == nextPending)
                        {
                            //If no more process all the notifications
                            break;
                        }
                        prevPending = nextPending;
                    }
                }

                // How many threads are waiting for a completed signal in response to control:reload?
                // They can all be signalled when the reload is complete, and subsequent iterations will do nothing (but may signal).
                unsigned signalsPending = owner.autoSignalsPending.exchange(0);

                //Check if there are any requests from last time that have not been processed yet.
                unsigned requestsPending = owner.autoPending.exchange(0);
                if (requestsPending)
                {
                    //NOTE: Following are read in reverse order from the order they are set to avoid race conditions
                    bool forceRetry = owner.autoForceRetry.exchange(false);
                    bool incremental = owner.autoAllIncremental.exchange(true);

                    if (traceLevel)
                        DBGLOG("AutoReload: [%s] %u changes (%u waits) delayed %ums ", (forceRetry ? "force" : (incremental ? "incremental" : "reload")), requestsPending, waits, mergeTimer.elapsedMs());

                    CCycleTimer timer;
                    try
                    {
                        //If all the changes are incremental, then only update the querysets that have changed, otherwise reload everything
                        if (incremental)
                            owner.reloadIncremental();
                        else
                            owner.reload(forceRetry);

                        if (traceLevel)
                            DBGLOG("AutoReload: took %ums (signal %u threads)", timer.elapsedMs(), signalsPending);
                    }
                    catch (IException *E)
                    {
                        if (!closing)
                            EXCLOG(MCoperatorError, E, "AutoReloadThread: ");
                        E->Release();
                    }
                    catch (...)
                    {
                        IERRLOG("Unknown exception in AutoReloadThread");
                    }
                }
                else
                {
                    if (traceLevel)
                       DBGLOG("AutoReload - nothing to do %u requests waiting", signalsPending);
                }

                if (signalsPending)
                    owner.autoReloadComplete.signal(signalsPending);
            }
            if (traceLevel)
                DBGLOG("AutoReloadThread %p exiting", this);
            return 0;
        }

        void stop()
        {
            closing = true;
            owner.autoReloadTrigger.signal();
        }
    } autoReloadThread;

    void reload(bool forceRetry)
    {
        clearDaliMisses();
        // We want to kill the old packages, but not until we have created the new ones
        // So that the query/dll caching will work for anything that is not affected by the changes
        Owned<CRoxiePackageSetWatcher> newPackages;
        if (standAloneDll)
            newPackages.setown(new CRoxiePackageSetWatcher(daliHelper, standAloneDll, numChannels, "roxie", forceRetry));
        else
        {
            Owned<CRoxiePackageSetWatcher> currentPackages;
            {
                ReadLockBlock b(packageCrit);
                currentPackages.setown(allQueryPackages.getLink());
            }
            newPackages.setown(new CRoxiePackageSetWatcher(daliHelper, numChannels, currentPackages, forceRetry));
        }
        // Hold the lock for as little time as we can
        // Note that we must NOT hold the lock during the delete of the old object - or we deadlock.
        // Hence the slightly convoluted code below
        Owned<CRoxiePackageSetWatcher> oldPackages;  // NB Destroyed outside the WriteLockBlock
        {
            WriteLockBlock b(packageCrit);
            oldPackages.setown(allQueryPackages.getLink());  // Ensure we don't delete the old packages until after we have loaded the new
            allQueryPackages.setown(newPackages.getClear());
        }

        //Release the old packages before clearing any unused entries.
        oldPackages.clear();
        completeReload();
    }

    void reloadIncremental()
    {
        {
            ReadLockBlock b(packageCrit);
            allQueryPackages->reloadIncremental();
        }
        completeReload();
    }

    void completeReload()
    {
        if (preloadOnceData)
        {
            ReadLockBlock readBlock(packageCrit);
            allQueryPackages->preloadOnce();
        }
        // Avoid clearing keys and updating the cache file if we are just about to reload something
        if (autoPending.load() == 0)
        {
            clearKeyStoreCache(false);   // Allows us to fully release files we no longer need because of unloaded queries
            daliHelper->commitCache();
        }
    }

    // Common code used by control:queries and control:getQueryXrefInfo

    void getQueryInfo(IPropertyTree *control, StringBuffer &reply, bool full, const IRoxieContextLogger &logctx) const
    {
        Owned<IPropertyTreeIterator> ids = control->getElements("Query");
        reply.append("<Queries reporting='1'>\n");
        if (ids->first())
        {
            ForEach(*ids)
            {
                const char *id = ids->query().queryProp("@id");
                if (id)
                {
                    IArrayOf<IQueryFactory> agentQueries;
                    Owned<IQueryFactory> query = getQuery(id, NULL, &agentQueries, logctx);
                    if (query)
                        query->getQueryInfo(reply, full, &agentQueries, logctx);
                    else
                        reply.appendf(" <Query id=\"%s\" error=\"Query not found\"/>\n", id);
                }
            }
        }
        else
        {
            ReadLockBlock readBlock(packageCrit);
            allQueryPackages->getAllQueryInfo(reply, full, logctx);
        }
        reply.append("</Queries>\n");
    }

    void _doControlMessage(IPropertyTree *control, StringBuffer &reply, const IRoxieContextLogger &logctx)
    {
        const char *queryName = control->queryName();
        logctx.CTXLOG("doControlMessage - %s", queryName);
        assertex(memicmp(queryName, "control:", 8) == 0);
        bool unknown = false;
        switch (_toupper(queryName[8]))
        {
        case 'A':
            if (stricmp(queryName, "control:aclupdate") == 0)
            {
                // MORE - do nothing for now - possibly needed in the future - leave this so no exception is thrown
            }
            else if (stricmp(queryName, "control:activeQueries")==0)
            {
                if (debugSessionManager)
                    debugSessionManager->getActiveQueries(reply);
            }
            else if (stricmp(queryName, "control:activitymetrics")==0)
            {
                ReadLockBlock readBlock(packageCrit);
                allQueryPackages->getActivityMetrics(reply);
            }
            else if (stricmp(queryName, "control:alive")==0)
            {
                reply.appendf("<Alive restarts='%d'/>", restarts.load());
            }
            else
                unknown = true;
            break;

        case 'B':
            if (stricmp(queryName, "control:blobCacheMem")==0)
            {
                blobCacheMB = control->getPropInt("@val", 0);
                topology->setPropInt("@blobCacheMem", blobCacheMB);
                setBlobCacheMem(blobCacheMB * 0x100000ULL);
            }
            else
                unknown = true;
            break;

        case 'C':
            if (stricmp(queryName, "control:cacheInfo")==0)
            {
                bool clear = control->getPropBool("@clear", false);
                unsigned channel = control->getPropInt("@channel", -1);
                if (clear)
                    queryFileCache().clearOsCache();
                else
                {
                    reply.append(" <CacheInfo");
                    if (channel != (unsigned) -1)
                        reply.appendf(" channel='%u'", channel);
                    reply.append(">\n");
                    queryFileCache().reportOsCache(reply, channel);
                    reply.appendf(" </CacheInfo>\n");
                }
            }
            else if (stricmp(queryName, "control:checkCompleted")==0)
            {
                checkCompleted = control->getPropBool("@val", true);
                topology->setPropBool("@checkCompleted", checkCompleted );
            }
            else if (stricmp(queryName, "control:checkingHeap")==0)
            {
                defaultCheckingHeap = control->getPropBool("@val", true);
                topology->setPropInt("@checkingHeap", defaultCheckingHeap);
            }
            else if (stricmp(queryName, "control:clearIndexCache")==0)
            {
                bool clearAll = control->getPropBool("@clearAll", true);
                clearKeyStoreCache(clearAll);
            }
            else if (stricmp(queryName, "control:closedown")==0)
            {
                closedown();
            }
            else if (stricmp(queryName, "control:closeExpired")==0)
            {
                queryFileCache().closeExpired(false);
                queryFileCache().closeExpired(true);
            }
            else if (stricmp(queryName, "control:closeLocalExpired")==0)
            {
                queryFileCache().closeExpired(false);
            }
            else if (stricmp(queryName, "control:closeRemoteExpired")==0)
            {
                queryFileCache().closeExpired(true);
            }
            else
                unknown = true;
            break;

        case 'D':
            if (stricmp(queryName, "control:dafilesrvLookupTimeout")==0)
            {
                dafilesrvLookupTimeout = control->getPropInt("@val", 10000);
                topology->setPropInt("@dafilesrvLookupTimeout", dafilesrvLookupTimeout);
                setRemoteFileTimeouts(dafilesrvLookupTimeout, 0);
            }
            else if (stricmp(queryName, "control:defaultConcatPreload")==0)
            {
                defaultConcatPreload = control->getPropInt("@val", 0);
                topology->setPropInt("@defaultConcatPreload", defaultConcatPreload);
            }
            else if (stricmp(queryName, "control:defaultFetchPreload")==0)
            {
                defaultFetchPreload = control->getPropInt("@val", 0);
                topology->setPropInt("@defaultFetchPreload", defaultFetchPreload);
            }
            else if (stricmp(queryName, "control:defaultFullKeyedJoinPreload")==0)
            {
                defaultFullKeyedJoinPreload = control->getPropInt("@val", 0);
                topology->setPropInt("@defaultFullKeyedJoinPreload", defaultFullKeyedJoinPreload);
            }
            else if (stricmp(queryName, "control:defaultHighPriorityTimeLimit")==0)
            {
                defaultTimeLimit[1] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultHighPriorityTimeLimit", defaultTimeLimit[1]);
            }
            else if (stricmp(queryName, "control:defaultHighPriorityTimeWarning")==0)
            {
                defaultWarnTimeLimit[1] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultHighPriorityTimeWarning", defaultWarnTimeLimit[1]);
            }
            else if (stricmp(queryName, "control:defaultKeyedJoinPreload")==0)
            {
                defaultKeyedJoinPreload = control->getPropInt("@val", 0);
                topology->setPropInt("@defaultKeyedJoinPreload", defaultKeyedJoinPreload);
            }
            else if (stricmp(queryName, "control:defaultLowPriorityTimeLimit")==0)
            {
                defaultTimeLimit[0] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultLowPriorityTimeLimit", defaultTimeLimit[0]);
            }
            else if (stricmp(queryName, "control:defaultLowPriorityTimeWarning")==0)
            {
                defaultWarnTimeLimit[0] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultLowPriorityTimeWarning", defaultWarnTimeLimit[0]);
            }
            else if (stricmp(queryName, "control:defaultParallelJoinPreload")==0)
            {
                defaultParallelJoinPreload = control->getPropInt("@val", 0);
                topology->setPropInt("@defaultParallelJoinPreload", defaultParallelJoinPreload);
            }
            else if (stricmp(queryName, "control:defaultSLAPriorityTimeLimit")==0)
            {
                defaultTimeLimit[2] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultSLAPriorityTimeLimit", defaultTimeLimit[2]);
            }
            else if (stricmp(queryName, "control:defaultSLAPriorityTimeWarning")==0)
            {
                defaultWarnTimeLimit[2] = control->getPropInt("@limit", 0);
                topology->setPropInt("@defaultSLAPriorityTimeWarning", defaultWarnTimeLimit[2]);
            }
            else if (stricmp(queryName, "control:deleteUnneededPhysicalFiles")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:deleteUnneededQueryCacheFiles")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:doIbytiDelay")==0)
            {   // WARNING: use with extra care only during inactivity on system
                doIbytiDelay = control->getPropBool("@val", true);
                topology->setPropBool("@doIbytiDelay", doIbytiDelay);
            }
            else
                unknown = true;
            break;

        case 'F':
            if (stricmp(queryName, "control:fieldTranslationEnabled")==0)
            {
                const char *val = control->queryProp("@val");
                if (val)
                    fieldTranslationEnabled = getTranslationMode(val, false);
                else
                    fieldTranslationEnabled = RecordTranslationMode::PayloadRemoveOnly;
                val = getTranslationModeText(fieldTranslationEnabled);
                topology->setProp("@fieldTranslationEnabled", val);
            }
            else if (stricmp(queryName, "control:flushJHtreeCacheOnOOM")==0)
            {
                flushJHtreeCacheOnOOM = control->getPropBool("@val", true);
                topology->setPropInt("@flushJHtreeCacheOnOOM", flushJHtreeCacheOnOOM);
            }
            else
                unknown = true;
            break;

        case 'G':
            if (stricmp(queryName, "control:getACLinfo") == 0)
            {
                // MORE - do nothing for now - possibly needed in the future - leave this so no exception is thrown
            }
            else if (stricmp(queryName, "control:getClusterName")==0)
            {
                reply.appendf("<clusterName id='%s'/>", roxieName.str());
            }
            else if (stricmp(queryName, "control:getQueryXrefInfo")==0)
            {
                getQueryInfo(control, reply, true, logctx);
            }
            else if (stricmp(queryName, "control:getQuery")==0)
            {
                const char* id = control->queryProp("@id");
                if (!id)
                    throw MakeStringException(ROXIE_MISSING_PARAMS, "No query name specified");

                Owned<IQueryFactory> q = getQuery(id, NULL, NULL, logctx);
                if (q)
                {
                    Owned<IPropertyTree> tempTree = q->cloneQueryXGMML();
                    tempTree->setProp("@roxieName", roxieName.str());
                    toXML(tempTree, reply);
                }
                else
                    throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", id);
            }
            else if (stricmp(queryName, "control:getQueryWarningTime")==0)
            {
                const char *id = control->queryProp("Query/@id");
                if (!id)
                    badFormat();
                Owned<IQueryFactory> f = getQuery(id, NULL, NULL, logctx);
                if (f)
                {
                    unsigned warnLimit = f->queryOptions().warnTimeLimit;
                    reply.appendf("<QueryTimeWarning val='%d'/>", warnLimit);
                }
            }
            else if (stricmp(queryName, "control:getBuildVersion")==0)
            {
                reply.appendf("<version id='%s'/>", hpccBuildInfo.buildTag);
            }
            else if (strieq(queryName, "control:getMemLocked"))
            {
                reply.appendf(" <heapLockMemory locked='%d'/>\n", roxiemem::getRoxieMemLocked());
            }
            else
                unknown = true;
            break;

        case 'I':
            if (stricmp(queryName, "control:indexmetrics")==0)
            {
                getIndexMetrics(reply);
            }
            else if (stricmp(queryName, "control:inMemoryKeysEnabled")==0)
            {
                inMemoryKeysEnabled = control->getPropBool("@val", true);
                topology->setPropBool("@inMemoryKeysEnabled", inMemoryKeysEnabled);
            }
            else
                unknown = true;
            break;


        case 'L':
            if (stricmp(queryName, "control:leafCacheMem")==0)
            {
                leafCacheMB = control->getPropInt("@val", 50);
                topology->setPropInt("@leafCacheMem", leafCacheMB);
                setLeafCacheMem(leafCacheMB * 0x100000ULL);
            }
            else if (stricmp(queryName, "control:listFileOpenErrors")==0)
            {
                // this just creates a delta state file to remove references to Keys / Files we now longer have interest in
                StringAttrMapping *mapping = queryFileCache().queryFileErrorList();

                HashIterator iter(*mapping);
                StringBuffer err;
                for (iter.first(); iter.isValid(); iter.next())
                {
                    IMapping &cur = iter.query();
                    StringAttr *item = mapping->mapToValue(&cur);

                    const char *filename = (const char*)cur.getKey();
                    const char *filetype = item->get();

                    reply.appendf("<file><name>%s</name><type>%s</type></file>", filename, filetype);
                }
            }
            else if (stricmp(queryName, "control:listUnusedFiles")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:lockDali")==0)
            {
                if (adhocRoxie)
                    throw MakeStringException(ROXIE_CONTROL_MSG_ERROR, "Cannot lock dali when listening for workunits");
                topology->setPropBool("@lockDali", true);
                if (daliHelper)
                    daliHelper->disconnect();
                saveTopology(true);
            }
            else if (stricmp(queryName, "control:logfullqueries")==0)
            {
                logFullQueries = control->getPropBool("@val", true);
                topology->setPropBool("@logFullQueries", logFullQueries);
            }
            else
                unknown = true;
            break;

        case 'M':
            if (stricmp(queryName, "control:memoryStatsInterval")==0)
            {
                memoryStatsInterval = (unsigned) control->getPropInt64("@val", 0);
                roxiemem::setMemoryStatsInterval(memoryStatsInterval);
                topology->setPropInt64("@memoryStatsInterval", memoryStatsInterval);
            }
            else if (stricmp(queryName, "control:memtrace")==0)
            {
                unsigned memTraceLevel = control->getPropInt("@level", 0);
                roxiemem::setMemTraceLevel(memTraceLevel);
                topology->setPropInt("@memTraceLevel", memTraceLevel);
            }
            else if (stricmp(queryName, "control:memtracesizelimit")==0)
            {
                memsize_t memTraceSizeLimit = (memsize_t) control->getPropInt64("@val", control->getPropInt64("@value", 0)); // used to accept @value so coded like this for backward compatibility
                roxiemem::setMemTraceSizeLimit(memTraceSizeLimit);
                topology->setPropInt64("@memTraceSizeLimit", memTraceSizeLimit);
            }
            else if (stricmp(queryName, "control:metrics")==0)
            {
                roxieMetrics->getMetrics(reply);
            }
            else if (stricmp(queryName, "control:minFreeDiskSpace")==0)
            {
                minFreeDiskSpace = (unsigned) control->getPropInt64("@val", 1048576);
                topology->setPropInt64("@minFreeDiskSpace", minFreeDiskSpace);
            }
            else if (stricmp(queryName, "control:misctrace")==0)
            {
                miscDebugTraceLevel = control->getPropInt("@level", 0);
                topology->setPropInt("@miscDebugTraceLevel", miscDebugTraceLevel);
            }
            else if (strieq(queryName, "control:memlock"))
            {
                int srtn = roxiemem::lockRoxieMem(true);
                if (!srtn)
                {
                    topology->setPropBool("@heapLockMemory", true);
                    reply.append(" <heapLockMemory locked='1'/>\n");
                }
                else
                    reply.appendf(" <heapLockMemory locked='%d' error='%d' descr='unable to lock roxie memory'/>\n", roxiemem::getRoxieMemLocked(), srtn);
            }
            else if (strieq(queryName, "control:memunlock"))
            {
                // ought to have some acl restriction / key for this ...
                int srtn = roxiemem::lockRoxieMem(false);
                if (!srtn)
                {
                    topology->setPropBool("@heapLockMemory", false);
                    reply.append(" <heapLockMemory locked='0'/>\n");
                }
                else
                    reply.appendf(" <heapLockMemory locked='%d' error='%d' descr='unable to unlock roxie memory'/>\n", roxiemem::getRoxieMemLocked(), srtn);
            }
            else
                unknown = true;
            break;

        case 'N':
            if (stricmp(queryName, "control:nodeCacheMem")==0)
            {
                nodeCacheMB = control->getPropInt("@val", 100);
                topology->setPropInt("@nodeCacheMem", nodeCacheMB);
                setNodeCacheMem(nodeCacheMB * 0x100000ULL);
            }
            else if (stricmp(queryName, "control:numFilesToProcess")==0)
            { 
                int numFiles = queryFileCache().numFilesToCopy();
                reply.appendf("<FilesToProcess value='%d'/>", numFiles);
            }
            else
                unknown = true;
            break;


        case 'P':
            if (stricmp(queryName, "control:parallelAggregate")==0)
            {
                parallelAggregate = control->getPropInt("@val", 0);
                if (!parallelAggregate)
                    parallelAggregate = hdwInfo.numCPUs;
                if (!parallelAggregate)
                    parallelAggregate = 1;
                topology->setPropInt("@parallelAggregate", parallelAggregate);
            }
            else if (stricmp(queryName, "control:pauseEventRecording")==0)
            {
                bool addEventForChange = control->getPropBool("@audit", false);
                bool success = queryRecorder().pauseRecording(true, addEventForChange);
                reply.appendf("<EventRecording success='%s'/>", boolToStr(success));
            }
            else if (stricmp(queryName, "control:perf")==0)
            {
                unsigned perfTime = (unsigned) control->getPropInt64("@time", 60);
                PerfTracer perf;
                double interval = control->getPropReal("@interval", 0.2);
                perf.setInterval(interval);
                perf.traceFor(perfTime);
                reply.append(perf.queryResult().str());
            }
            else if (stricmp(queryName, "control:preabortIndexReadsThreshold")==0)
            {
                preabortIndexReadsThreshold = control->getPropInt("@val", 100);
                topology->setPropInt("@preabortIndexReadsThreshold", preabortIndexReadsThreshold);
            }
            else if (stricmp(queryName, "control:preabortKeyedJoinsThreshold")==0)
            {
                preabortKeyedJoinsThreshold = control->getPropInt("@val", 100);
                topology->setPropInt("@preabortKeyedJoinsThreshold", preabortKeyedJoinsThreshold);
            }
            else
                unknown = true;
            break;

        case 'Q':
            if (stricmp(queryName, "control:queries")==0)
            {
                getQueryInfo(control, reply, false, logctx);
            }
            else if (stricmp(queryName, "control:queryAggregates")==0)
            {
                time_t from;
                const char *fromTime = control->queryProp("@from");
                if (fromTime)
                {
                    CDateTime f;
                    f.setString(fromTime, NULL, true);
                    from = f.getSimple();
                }
                else
                    from = startupTime;
                time_t to;
                const char *toTime = control->queryProp("@to");
                if (toTime)
                {
                    CDateTime t;
                    t.setString(toTime, NULL, true);
                    to = t.getSimple();
                }
                else
                    time(&to);
                const char *id = control->queryProp("Query/@id");
                bool rawStats = control->getPropBool("@rawStats", false);
                if (id)
                {
                    if (!rawStats)
                    {
                        Owned<IQueryFactory> f = getQuery(id, NULL, NULL, logctx);
                        if (f)
                        {
                            Owned<const IPropertyTree> stats = f->getQueryStats(from, to);
                            toXML(stats, reply);
                        }
                        else
                            throw MakeStringException(ROXIE_CONTROL_MSG_ERROR, "Unknown query %s", id);
                    }
                    else
                    {
                        Owned<const IPropertyTree> stats = getQueryRawStats(id, from, to);
                        toXML(stats, reply);
                    }
                }
                else
                {
                    bool includeAllQueries = control->getPropBool("@all", true);
                    Owned<const IPropertyTree> stats = getAllQueryStats(includeAllQueries, rawStats, from, to);
                    toXML(stats, reply);
                }
            }
            else if (stricmp(queryName, "control:queryPackageInfo")==0)
            {
                ReadLockBlock readBlock(packageCrit);
                allQueryPackages->getInfo(reply, logctx);
            }
            else if (stricmp(queryName, "control:queryStats")==0)
            {
                const char *id = control->queryProp("Query/@id");
                if (!id)
                    badFormat();
                const char *action = control->queryProp("Query/@action");
                const char *wuid = control->queryProp("Query/@wuid");
                const char *graphName = 0;
                if (action)
                {
                    if (stricmp(action, "listGraphNames") == 0)
                    {
                        Owned<IQueryFactory> query = getQuery(id, NULL, NULL, logctx);
                        if (query)
                        {
                            reply.appendf("<Query id='%s'>\n", id);
                            StringArray graphNames;
                            query->getGraphNames(graphNames);
                            ForEachItemIn(idx, graphNames)
                            {
                                const char *graphName = graphNames.item(idx);
                                reply.appendf("<Graph name='%s'/>", graphName);
                            }
                            reply.appendf("</Query>\n");
                        }
                        return;  // done
                    }
                    else if (stricmp(action, "selectGraph") == 0)
                        graphName = control->queryProp("Query/@name");
                    else if (stricmp(action, "allGraphs") != 0)  // if we get here and its NOT allgraphs - then error
                        throw MakeStringException(ROXIE_CONTROL_MSG_ERROR, "invalid action in control:queryStats %s", action);
                }
                ReadLockBlock readBlock(packageCrit);
                allQueryPackages->getStats(reply, id, graphName, wuid, logctx);
            }
            else if (stricmp(queryName, "control:queryWuid")==0)
            {
                UNIMPLEMENTED;
            }
            else
                unknown = true;
            break;

        case 'R':
            if (stricmp(queryName, "control:reload")==0)
            {
                requestReload(true, control->getPropBool("@forceRetry", false), false);
                if (daliHelper && daliHelper->connected())
                    reply.appendf("<Dali connected='1'/>");
                else
                    reply.appendf("<Dali connected='0'/>");
                unsigned __int64 shash;
                {
                    ReadLockBlock readBlock(packageCrit);
                    shash = allQueryPackages->queryHash();
                }
                reply.appendf("<State hash='%" I64F "u' topologyHash='%" I64F "u' originalTopologyHash='%" I64F "u'/>", 
                  shash, currentTopologyHash, originalTopologyHash);
            }
            else if (stricmp(queryName, "control:resetcache")==0)
            {
                releaseAgentDynamicFileCache();
            }
            else if (stricmp(queryName, "control:resetindexmetrics")==0)
            {
                resetIndexMetrics();
            }
            else if (stricmp(queryName, "control:resetmetrics")==0)
            {
                roxieMetrics->resetMetrics();
            }
            else if (stricmp(queryName, "control:resetquerystats")==0)
            {
                ReadLockBlock readBlock(packageCrit);
                Owned<IPropertyTreeIterator> queries = control->getElements("Query");
                if (queries->first())
                {
                    while (queries->isValid())
                    {
                        IPropertyTree &query = queries->query();
                        const char *id = query.queryProp("@id");
                        const char *target = query.queryProp("@target");
                        if (!id)
                            badFormat();
                        allQueryPackages->resetStats(target, id, logctx);
                        queries->next();
                    }
                }
                else
                    allQueryPackages->resetStats(NULL, NULL, logctx);
            }
            else if (stricmp(queryName, "control:resetremotedalicache")==0)
            {
                queryNamedGroupStore().resetCache();
            }
            else if (stricmp(queryName, "control:restart")==0)
            {
                FatalError("Roxie process restarted by operator request");
            }
            else if (stricmp(queryName, "control:resumeEventRecording")==0)
            {
                bool addEventForChange = control->getPropBool("@audit", false);
                bool success = queryRecorder().pauseRecording(false, addEventForChange);
                reply.appendf("<EventRecording success='%s'/>", boolToStr(success));
            }
            else if (stricmp(queryName, "control:retrieveActivityDetails")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:retrieveFileInfo")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:roxiememstats") == 0)
            {
                StringBuffer memStats;
                queryMemoryPoolStats(memStats);
                reply.append("<MemoryStats>").append(memStats.str()).append("</MemoryStats>\n");
            }
            else
                unknown = true;
            break;

        case 'S':
            if (stricmp(queryName, "control:setAffinity")==0)
            {
                __uint64 affinity = control->getPropInt64("@val", 0); // by default just refresh cached settings
                topology->setPropInt64("@affinity", affinity);
                updateAffinity(affinity);
            }
            else if (stricmp(queryName, "control:setCacheInfo")==0)
            {
                Owned<IPTreeIterator> infos = control->getElements(".//CacheInfo");
                ForEach(*infos)
                {
                    IPropertyTree &info = infos->query();
                    queryFileCache().warmOsCache(info.queryProp(""));
                }
            }
            else if (stricmp(queryName, "control:setCopyResources")==0)
            {
                copyResources = control->getPropBool("@val", true);
                topology->setPropBool("@copyResources", copyResources);
            }
            else if (stricmp(queryName, "control:simpleLocalKeyedJoins")==0)
            {
                simpleLocalKeyedJoins = control->getPropBool("@val", true);
            }
            else if (stricmp(queryName, "control:soapInfo")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:soapTrace")==0)
            {
                soapTraceLevel = control->getPropInt("@level", 0);
                topology->setPropInt("@soapTraceLevel", soapTraceLevel);
            }
            else if (stricmp(queryName, "control:socketCheckInterval")==0)
            {
                socketCheckInterval = (unsigned) control->getPropInt64("@val", 0);
                topology->setPropInt64("@socketCheckInterval", socketCheckInterval);
            }
            else if (stricmp(queryName, "control:startEventRecording")==0)
            {
                const char * options = control->queryProp("@options");
                const char * filename = control->queryProp("@filename");
                bool success = startRoxieEventRecording(options, filename);
                reply.appendf("<EventRecording success='%s'/>", boolToStr(success));
            }
            else if (stricmp(queryName, "control:stopEventRecording")==0)
            {
                EventRecordingSummary summary;
                bool success = stopRoxieEventRecording(&summary);
                if (success)
                {
                    reply.appendf("<EventRecording success='true' numEvents='%u' filename='%s' size='%llu'/>", summary.numEvents, summary.filename.str(), summary.totalSize);
                }
                else
                    reply.appendf("<EventRecording success='false'/>");
            }
            else if (stricmp(queryName, "control:state")==0)
            {
                if (daliHelper && daliHelper->connected())
                    reply.appendf("<Dali connected='1'/>");
                else
                    reply.appendf("<Dali connected='0'/>");
                unsigned __int64 shash;
                {
                    ReadLockBlock readBlock(packageCrit);
                    shash = allQueryPackages->queryHash();
                }
                reply.appendf("<State hash='%" I64F "u' topologyHash='%" I64F "u' originalTopologyHash='%" I64F "u'/>", 
                  shash, currentTopologyHash, originalTopologyHash);
            }
            else if (stricmp(queryName, "control:steppingEnabled")==0)
            {
                steppingEnabled = control->getPropBool("@val", true);
            }
            else if (stricmp(queryName, "control:systemMonitor")==0)
            {
#ifndef _CONTAINERIZED
                unsigned interval = control->getPropInt("@interval", 60000);
                bool enable = control->getPropBool("@enable", true);
                if (enable)
                    startPerformanceMonitor(interval, PerfMonStandard, perfMonHook);
                else
                    stopPerformanceMonitor();
#else
                UNIMPLEMENTED; //better than ignoring 'control:systemMonitor' in containerized mode
#endif
            }
            //MORE: control:stats??
            else
                unknown = true;
            break;

        case 'T':
            if (stricmp(queryName, "control:testAgentFailure")==0)
            {
                testAgentFailure = control->getPropInt("@val", (unsigned) -1);
            }
            else if (stricmp(queryName, "control:timeActivities")==0)
            {
                defaultTimeActivities = control->getPropBool("@val", true);
                topology->setPropInt("@timeActivities", defaultTimeActivities);
            }
            else if (stricmp(queryName, "control:timings")==0)
            {
                reply.append("<Timings>");
                queryActiveTimer()->getTimings(reply);
                reply.append("</Timings>");
                if (control->getPropBool("@reset", false))
                {
                    queryActiveTimer()->reset();
                }
            }
            else if (stricmp(queryName, "control:topology")==0)
            {
                toXML(topology, reply);
            }
            else if (stricmp(queryName, "control:toposerver")==0)
            {
                if (control->hasProp("@freeze"))
                {
                    freezeTopology(control->getPropBool("@freeze"));
                }
                else
                {
                    reply.append("<Toposerver>");
                    getTopology()->report(reply);
                    reply.append("</Toposerver>");
                }
            }
            else if (stricmp(queryName, "control:trace")==0)
            {
                traceLevel = control->getPropInt("@level", 0);
                if (traceLevel > MAXTRACELEVEL)
                    traceLevel = MAXTRACELEVEL;
                topology->setPropInt("@traceLevel", traceLevel);
            }
            else if (stricmp(queryName, "control:traceStartStop")==0)
            {
                traceStartStop = control->getPropBool("@val", true);
                topology->setPropInt("@traceStartStop", traceStartStop);
            }
            else
                unknown = true;
            break;

        case 'U':
            if (stricmp(queryName, "control:udptrace")==0)
            {
                udpTraceLevel = control->getPropInt("@level", 0);
                topology->setPropInt("@udpTraceLevel", udpTraceLevel);
            }
            else if (stricmp(queryName, "control:unlockDali")==0)
            {
                topology->setPropBool("@lockDali", false);
                // Dali will reattach via the timer that checks every so often if can reattach...
                saveTopology(false);
            }
            else if (stricmp(queryName, "control:unsuspend")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:userMetric")==0)
            {
                const char *name = control->queryProp("@name");
                const char *regex= control->queryProp("@regex");
                if (name && regex)
                {
                    roxieMetrics->addUserMetric(name, regex);
                    // MORE - we could add to topology, we could check for dups, and we could support removing them.
                }
                else
                    throw MakeStringException(ROXIE_MISSING_PARAMS, "Metric name or regex missing");
            }
            else
                unknown = true;
            break;

        case 'W':
            if (stricmp(queryName, "control:watchActivityId")==0)
            {
                watchActivityId = control->getPropInt("@id", true);
            }
            else if (stricmp(queryName, "control:waitForReload")==0)
            {
                waitForReload();
            }
            else
                unknown = true;
            break;

        default:
            unknown = true;
            break;
        }
        currentTopologyHash = getTopologyHash();
        if (unknown)
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName);
    }

    void badFormat()
    {
        throw MakeStringException(ROXIE_INVALID_INPUT, "Badly formated control query");
    }
};

extern IRoxieQueryPackageManagerSet *createRoxiePackageSetManager(const IQueryDll *standAloneDll)
{
    return new CRoxiePackageSetManager(standAloneDll);
}

IRoxieQueryPackageManagerSet *globalPackageSetManager = NULL;

extern void loadPlugins()
{
    DBGLOG("Preloading plugins from %s", pluginDirectory.str());
    if (pluginDirectory.length())
    {
        plugins = new SafePluginMap(&PluginCtx, traceLevel);
        if (topology->hasProp("preload"))
        {
            Owned<IPropertyTreeIterator> preloads = topology->getElements("preload");
            ForEach(*preloads)
            {
                const char *preload = preloads->query().queryProp(".");
                if (!streq(preload, "none"))
                {
                    VStringBuffer soname(SharedObjectPrefix "%s" SharedObjectExtension, preload);
                    if (!plugins->loadNamed(pluginDirectory, soname))
                        DBGLOG("Could not preload plugin %s at any of the following locations: %s", soname.str(), pluginDirectory.str());
                }
            }
        }
        else
            plugins->loadFromList(pluginDirectory);
    }
}

extern void cleanupPlugins()
{
    delete plugins;
    plugins = NULL;
}

/*=======================================================================================================
* mergeStats and associated code is used to combine the graph stats from multiple nodes in a cluster into
* a single aggregate structure
* It should be moved into ccdquery.cpp really
*========================================================================================================*/

typedef void (*mergefunc)(IPropertyTree *s1, IPropertyTree *s2, unsigned level);

struct MergeInfo
{
    const char *element;
    const char *attribute;
    mergefunc f;
};

void mergeSubGraphs(IPropertyTree *s1, IPropertyTree *s2, unsigned);

void mergeNodes(IPropertyTree *s1, IPropertyTree *s2)
{
    Owned<IPropertyTreeIterator> elems = s1->getElements("att");
    ForEach(*elems)
    {
        IPropertyTree &e1 = elems->query();
        unsigned __int64 v1 = e1.getPropInt64("@value", 0);
        const char *name = e1.queryProp("@name");
        if (stricmp(name, "_kind")==0 && v1 == TAKsubgraph)
        {
            IPropertyTree *s1child = s1->queryPropTree("att/graph");
            IPropertyTree *s2child = s2->queryPropTree("att[@name='_kind']/graph");
            if (s1child && s2child)
            {
                mergeSubGraphs(s1child, s2child, 0);
                s2->removeProp("att[@name='_kind']");
            }
        }
        else
        {
            StringBuffer xpath;
            xpath.appendf("att[@name='%s']", name);
            if (startsWith(name, "SizeMax"))
            {
                IPropertyTree *e2 = s2->queryPropTree(xpath.str());
                if (e2)
                {
                    unsigned __int64 v2 = e2->getPropInt64("@value", 0);
                    if (v2 > v1)
                        e1.setPropInt64("@value", v2);
                    s2->removeTree(e2);
                }
            }
            else if (startsWith(name, "Size") || startsWith(name, "Time") || startsWith(name, "Num"))
            {
                IPropertyTree *e2 = s2->queryPropTree(xpath.str());
                if (e2)
                {
                    unsigned __int64 v2 = e2->getPropInt64("@value", 0);
                    e1.setPropInt64("@value", v1+v2);
                    s2->removeTree(e2);
                }
            }
            else
            {
                // remove from s2 any complete dups
                const char *s1val = e1.queryProp("@value");
                Owned<IPropertyTreeIterator> s2elems = s2->getElements(xpath.str());
                IArrayOf<IPropertyTree> goers;
                ForEach(*s2elems)
                {
                    IPropertyTree &e2 = s2elems->query();
                    const char *s2val = e2.queryProp("@value");
                    if ((!s1val && !s2val) || (s1val && s2val && strcmp(s1val, s2val)==0))
                        goers.append(*LINK(&e2));
                }
                ForEachItemIn(idx, goers)
                {
                    s2->removeTree(&goers.item(idx));
                }
            }
        }
    }
    elems.setown(s2->getElements("*"));
    ForEach(*elems)
    {
        IPropertyTree &e2 = elems->query();
        s1->addPropTree(e2.queryName(), LINK(&e2));
    }
}

void mergeSubGraphs(IPropertyTree *s1, IPropertyTree *s2, unsigned)
{
    Owned<IPropertyTreeIterator> elems = s1->getElements("*");
    ForEach(*elems)
    {
        IPropertyTree &e1 = elems->query();
        const char *elemName = e1.queryName();
        StringBuffer xpath;
        if (strcmp(elemName, "att")==0)
        {
            xpath.appendf("att[@name='%s']", e1.queryProp("@name"));
            IPropertyTree *e2 = s2->queryPropTree(xpath.str());
            if (e2)
                s2->removeTree(e2);
        }
        else
        {
            xpath.appendf("%s[@id='%s']", elemName, e1.queryProp("@id"));
            IPropertyTree *e2 = s2->queryPropTree(xpath.str());
            if (e2)
            {
                mergeNodes(&e1, e2);
                s2->removeTree(e2);
            }
        }
    }
    elems.setown(s2->getElements("*"));
    ForEach(*elems)
    {
        IPropertyTree &e2 = elems->query();
        s1->addPropTree(e2.queryName(), LINK(&e2));
    }
}

void mergeNode(IPropertyTree *s1, IPropertyTree *s2, unsigned level);

MergeInfo mergeTable[] =
{
    {"Query", "@id", mergeStats},
    {"Graphs", NULL, mergeStats},
    {"Graph", "@name", mergeStats},
    {"xgmml", NULL, mergeStats},
    {"graph", NULL, mergeStats},
    {"node",  "@id", mergeNode},
    {"att",   NULL, mergeStats},
    {"graph", NULL, mergeSubGraphs},
};

void mergeNode(IPropertyTree *s1, IPropertyTree *s2, unsigned level)
{
    if (s1->hasProp("att/@name"))
        mergeNodes(s1, s2);
    else
        mergeStats(s1, s2, level);
}

void mergeStats(IPropertyTree *s1, IPropertyTree *s2, unsigned level)
{
    MergeInfo & mi = mergeTable[level];
    Owned<IPropertyTreeIterator> elems = s2->getElements(mi.element);
    ForEach(*elems)
    {
        IPropertyTree &e2 = elems->query();
        StringBuffer xpath;
        if (mi.attribute)
            xpath.appendf("%s[%s='%s']", mi.element, mi.attribute, e2.queryProp(mi.attribute));
        else
            xpath.append(mi.element);
        IPropertyTree *e1 = s1->queryPropTree(xpath.str());
        if (e1)
        {
            mi.f(e1, &e2, level+1);
        }
        else
            s1->addPropTree(mi.element, LINK(&e2));
    }
}

void mergeStats(IPropertyTree *s1, IPropertyTree *s2)
{
    Owned<IPropertyTreeIterator> elems = s2->getElements("Exception");
    ForEach(*elems)
    {
        s1->addPropTree("Exception", LINK(&elems->query()));
    }
    mergeStats(s1, s2, 0);
}

void mergeQueries(IPropertyTree *dest, IPropertyTree *src)
{
    Owned<IPropertyTreeIterator> elems = src->getElements("Exception");
    ForEach(*elems)
    {
        dest->addPropTree("Exception", LINK(&elems->query()));
    }
    IPropertyTree *destQueries = ensurePTree(dest, "Queries");
    IPropertyTree *srcQueries = src->queryPropTree("Queries");
    if (!srcQueries)
        return;
    destQueries->setPropInt("@reporting", destQueries->getPropInt("@reporting") + srcQueries->getPropInt("@reporting"));

    Owned<IPropertyTreeIterator> queries = srcQueries->getElements("Query");
    ForEach(*queries)
    {
        IPropertyTree *srcQuery = &queries->query();
        const char *id = srcQuery->queryProp("@id");
        if (!id || !*id)
            continue;
        VStringBuffer xpath("Query[@id='%s']", id);
        IPropertyTree *destQuery = destQueries->queryPropTree(xpath);
        if (!destQuery)
        {
            destQueries->addPropTree("Query", LINK(srcQuery));
            continue;
        }
        int suspended = destQuery->getPropInt("@suspended") + srcQuery->getPropInt("@suspended"); //keep count to recognize "partially suspended" queries
        mergePTree(destQuery, srcQuery);
        if (suspended)
            destQuery->setPropInt("@suspended", suspended);
    }
    Owned<IPropertyTreeIterator> aliases = srcQueries->getElements("Alias");
    ForEach(*aliases)
    {
        IPropertyTree *srcQuery = &aliases->query();
        const char *id = srcQuery->queryProp("@id");
        if (!id || !*id)
            continue;
        VStringBuffer xpath("Alias[@id='%s']", id);
        IPropertyTree *destQuery = destQueries->queryPropTree(xpath);
        if (!destQuery)
        {
            destQueries->addPropTree("Alias", LINK(srcQuery));
            continue;
        }
        mergePTree(destQuery, srcQuery);
    }
}

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

static const char *g1 =
        "<Stats>"
        "<Query id='stats'>"
        "<Graphs>"
        "<Graph name='graph1'>"
         "<xgmml>"
          "<graph>"
           "<node id='1'>"
            "<att>"
             "<graph>"
              "<node id='2' label='Temp Table'>"
               "<att name='name' value='d'/>"
               "<att name='_kind' value='25'/>"
               "<att name='helper' value='f2'/>"
              "</node>"
              "<node id='2a'>"
              " <att name='_kind' value='1'>"   // TAKsubgraph
              "  <graph>"
              "   <node id='7696' label='Nested'>"
              "    <att name='NumSeeks' value='15'/>"
              "   </node>"
              "  </graph>"
              " </att>"
              "</node>"
              "<node id='3' label='Filter'>"
               "<att name='name' value='ds'/>"
               "<att name='_kind' value='5'/>"
               "<att name='helper' value='f3'/>"
              "</node>"
              "<att name='rootGraph' value='1'/>"
              "<edge id='2_0' source='2' target='3'>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='NumRows' value='3'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_1' source='5' target='7'>"
               "<att name='_sourceIndex' value='1'/>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Graphs>"
        "</Query>"
        "</Stats>";
static const char *g2 =
        "<Stats>"
        "<Query id='stats'>"
        "<Graphs>"
        "<Graph name='graph1'>"
         "<xgmml>"
          "<graph>"
           "<node id='1'>"
            "<att>"
             "<graph>"
              "<node id='2' label='Temp Table'>"
               "<att name='name' value='d'/>"
               "<att name='_kind' value='25'/>"
               "<att name='helper' value='f2'/>"
              "</node>"
              "<node id='2a'>"
              " <att name='_kind' value='1'>"   // TAKsubgraph
              "  <graph>"
              "   <node id='7696' label='Nested'>"
              "    <att name='NumSeeks' value='25'/>"
              "   </node>"
              "  </graph>"
              " </att>"
              "</node>"
              "<node id='4' label='Filter2'>"
               "<att name='name' value='ds2'/>"
               "<att name='_kind' value='53'/>"
               "<att name='helper' value='f23'/>"
              "</node>"
              "<att name='rootGraph' value='1'/>"
              "<edge id='2_0' source='2' target='3'>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='NumRows' value='3'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Graphs>"
        "</Query>"
        "</Stats>";
static const char *expected =
        "<Stats>"
        "<Query id='stats'>"
        "<Graphs>"
        "<Graph name='graph1'>"
         "<xgmml>"
          "<graph>"
           "<node id='1'>"
            "<att>"
             "<graph>"
              "<node id='2' label='Temp Table'>"
               "<att name='name' value='d'/>"
               "<att name='_kind' value='25'/>"
               "<att name='helper' value='f2'/>"
              "</node>"
              "<node id='2a'>"
              " <att name='_kind' value='1'>"   // TAKsubgraph
              "  <graph>"
              "   <node id='7696' label='Nested'>"
              "    <att name='NumSeeks' value='40'/>"
              "   </node>"
              "  </graph>"
              " </att>"
              "</node>"
              "<node id='3' label='Filter'>"
               "<att name='name' value='ds'/>"
               "<att name='_kind' value='5'/>"
               "<att name='helper' value='f3'/>"
              "</node>"
              "<node id='4' label='Filter2'>"
               "<att name='name' value='ds2'/>"
               "<att name='_kind' value='53'/>"
               "<att name='helper' value='f23'/>"
              "</node>"
              "<att name='rootGraph' value='1'/>"
              "<edge id='2_0' source='2' target='3'>"
               "<att name='NumRows' value='30'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='NumRows' value='30'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='NumRows' value='6'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_1' source='5' target='7'>"
               "<att name='_sourceIndex' value='1'/>"
               "<att name='NumRows' value='15'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Graphs>"
        "</Query>"
        "</Stats>"
        ;

class MergeStatsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( MergeStatsTest );
    CPPUNIT_TEST(test1);
    // CPPUNIT_TEST(test2);  Handy for debugging problem cases...
    CPPUNIT_TEST_SUITE_END();

protected:
    void test1()
    {
        Owned<IPropertyTree> p1 = createPTreeFromXMLString(g1);
        Owned<IPropertyTree> p2 = createPTreeFromXMLString(g2);
        Owned<IPropertyTree> e = createPTreeFromXMLString(expected);
        mergeStats(p1, p2);
        StringBuffer s1, s2;
        toXML(p1, s1);
        toXML(e, s2);
        CPPUNIT_ASSERT(strcmp(s1, s2)==0);
    }
    void test2()
    {
        Owned<IPropertyTree> mergedReply = createPTree("Merged");
        Owned<IPropertyTree> p1 = createPTreeFromXMLFile("stats1.xml");
        Owned<IPropertyTreeIterator> meat = p1->getElements("Endpoint");
        ForEach(*meat)
        {
            if (mergedReply)
            {
                mergeStats(mergedReply, &meat->query());
            }
        }
        Owned<IPropertyTree> p2 = createPTreeFromXMLFile("stats2.xml");
        meat.setown(p2->getElements("Endpoint"));
        ForEach(*meat)
        {
            if (mergedReply)
            {
                mergeStats(mergedReply, &meat->query());
            }
        }
        StringBuffer s1;
        toXML(mergedReply, s1);
        //toXML(e, s2);
        //CPPUNIT_ASSERT(strcmp(s1, s2)==0);
        printf("%s", s1.str());
    }

};


CPPUNIT_TEST_SUITE_REGISTRATION( MergeStatsTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( MergeStatsTest, "MergeStatsTest" );

#endif

