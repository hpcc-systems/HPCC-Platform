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
#include <jlib.hpp>
#include "build-config.h"

#include "jisem.hpp"
#include "jsort.hpp"
#include "jregexp.hpp"

#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdfile.hpp"
#include "ccdsnmp.hpp"

#include "hqlplugins.hpp"
#include "thorplugin.hpp"
#include "eclrtl.hpp"
#include "dafdesc.hpp"
#include "dautils.hpp"

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

const char *queryNodeFileName(const IPropertyTree &graphNode)
{
    const char *id = graphNode.queryProp("att[@name='_fileName']/@value");
    return id;
}

const char *queryNodeIndexName(const IPropertyTree &graphNode)
{
    const char * id = graphNode.queryProp("att[@name='_indexFileName']/@value");
    if (!id && !graphNode.hasProp("att[@name='_indexFileName_dynamic']"))   // can remove soon
        id = graphNode.queryProp("att[@name='_fileName']/@value");
    return id;
}

class CSimpleSuperFileArray : public CInterface, implements ISimpleSuperFileEnquiry
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
            if (stricmp(subFiles.item(idx).queryProp("@value"), subname))
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

class CRoxiePackage : extends CInterface, implements IRoxiePackage
{
protected:
    Owned<IPropertyTree> node;
    IArrayOf<CRoxiePackage> bases;
    Owned<IRoxieDaliHelper> daliHelper;
    Owned<IProperties> mergedEnvironment;
    hash64_t hash;
    bool compulsory;  // This concept may well disappear...

    mutable CriticalSection cacheLock;
    mutable CopyMapStringToMyClass<IResolvedFile> fileCache;

    // Add a filename and the corresponding IResolvedFile to the cache
    void addCache(const char *filename, const IResolvedFile *file) const
    {
        CriticalBlock b(cacheLock);
        IResolvedFile *add = const_cast<IResolvedFile *>(file);
        add->setCache(this);
        fileCache.setValue(filename, add);
    }
    // Remove an IResolvedFile from the cache
    void removeCache(const IResolvedFile *file) const
    {
        CriticalBlock b(cacheLock);
        // NOTE: it's theoretically possible for the final release to happen after a replacement has been inserted into hash table. 
        // So only remove from hash table if what we find there matches the item that is being deleted.
        IResolvedFile *goer = fileCache.getValue(file->queryFileName());
        if (goer == file)
            fileCache.remove(file->queryFileName());
        // You might want to remove files from the daliServer cache too, but it's not safe to do so here as there may be multiple package caches
    }
    // Lookup a filename in the cache
    IResolvedFile *lookupCache(const char *filename) const
    {
        CriticalBlock b(cacheLock);
        IResolvedFile *cache = fileCache.getValue(filename);
        if (cache)
        {
            LINK(cache);
            if (cache->isAlive())
                return cache;
        }
        return NULL;
    }
    // Load mergedEnvironment from local XML node
    void loadEnvironment()
    {
        mergedEnvironment.setown(createProperties(true));
        Owned<IPropertyTreeIterator> envIterator = node->getElements("Environment");
        ForEach(*envIterator)
        {
            IPropertyTree &env = envIterator->query();
            const char *id = env.queryProp("@id");
            const char *val = env.queryProp("@value");
            if (!val)
                val = env.queryProp("@val"); // Historically we used val here - not sure why... other parts of package file used value
            if (id && val)
                mergedEnvironment->setProp(id, val);
            else
            {
                StringBuffer s;
                toXML(&env, s);
                throw MakeStringException(0, "PACKAGE_ERROR: Environment element missing id or value: %s", s.str());
            }
        }
        Owned<IAttributeIterator> attrs = node->getAttributes();
        for(attrs->first(); attrs->isValid(); attrs->next())
        {
            StringBuffer s("control:");
            s.append(attrs->queryName()+1);  // queryName() has a leading @, hence the +1
            mergedEnvironment->setProp(s.str(), attrs->queryValue());
        }
    }
    // Merge base package environment into mergedEnvironment
    void mergeEnvironment(const CRoxiePackage *base)
    {
        Owned<IPropertyIterator> envIterator = base->mergedEnvironment->getIterator();
        ForEach(*envIterator)
        {
            const char *id = envIterator->getPropKey();
            const char *val = base->mergedEnvironment->queryProp(id);
            if (id && val && !mergedEnvironment->hasProp(id))
                mergedEnvironment->setProp(id, val);
        }
    }
    // Search this package and any bases for an element matching xpath1, then return iterator for its children that match xpath2
    IPropertyTreeIterator *lookupElements(const char *xpath1, const char *xpath2) const
    {
        IPropertyTree *parentNode = node->queryPropTree(xpath1);
        if (parentNode)
            return parentNode->getElements(xpath2);
        ForEachItemIn(idx, bases)
        {
            const CRoxiePackage &basePackage = bases.item(idx);
            IPropertyTreeIterator *it = basePackage.lookupElements(xpath1, xpath2);
            if (it)
                return it;
        }
        return NULL;
    }
    // Use local package and its bases to resolve superfile name list of subfiles via all supported resolvers
    const ISimpleSuperFileEnquiry *resolveSuperFile(const char *superFileName) const
    {
        // Order of resolution: 
        // 1. SuperFiles named in local package
        // 2. SuperFiles named in bases
        // There is no dali or local case - a superfile that is resolved in dali must also resolve the subfiles there (and is all done in the resolveLFNusingDali method)
        if (node)
        {
            StringBuffer superFileXPath;
            superFileXPath.append("SuperFile[@id='").append(superFileName).append("']");
            Owned<IPropertyTreeIterator> subFiles = lookupElements(superFileXPath, "SubFile");
            if (subFiles)
            {
                Owned<CSimpleSuperFileArray> result = new CSimpleSuperFileArray(*subFiles);
                return result.getClear();
            }
        }
        return NULL;
    }
    // Use local package file only to resolve subfile into physical file info
    IResolvedFile *resolveLFNusingPackage(const char *fileName) const
    {
        if (node)
        {
            StringBuffer xpath;
            IPropertyTree *fileInfo = node->queryPropTree(xpath.appendf("File[@id='%s']", fileName).str());
            if (fileInfo)
            {
                Owned <IResolvedFileCreator> result = createResolvedFile(fileName);
                result->addSubFile(createFileDescriptorFromRoxieXML(fileInfo));
                return result.getClear();
            }
        }
        return NULL;
    }
    // Use dali to resolve subfile into physical file info
    IResolvedFile *resolveLFNusingDali(const char *fileName, bool cacheIt, bool writeAccess) const
    {
        if (daliHelper)
        {
            if (daliHelper->connected())
            {
                Owned<IDistributedFile> dFile = daliHelper->resolveLFN(fileName, cacheIt, writeAccess);
                if (dFile)
                    return createResolvedFile(fileName, dFile.getClear());
            }
            else if (!writeAccess)  // If we need write access and expect a dali, but don't have one, we should probably fail
            {
                // we have no dali, we can't lock..
                Owned<IFileDescriptor> fd = daliHelper->resolveCachedLFN(fileName);
                if (fd)
                {
                    Owned <IResolvedFileCreator> result = createResolvedFile(fileName);
                    result->addSubFile(fd.getClear());
                    return result.getClear();
                }
            }
        }
        return NULL;
    }
    // Use local package file's localFile info to resolve subfile into physical file info
    IResolvedFile *resolveLFNusingLocal(const char *fileName) const
    {
        if (node && node->getPropBool("@localFiles"))
        {
            Owned <IResolvedFileCreator> result = createResolvedFile(fileName);
            return result.getClear();
        }
        return NULL;
    }
    // Use local package and its bases to resolve existing file into physical file info via all supported resolvers
    IResolvedFile *lookupFile(const char *fileName, bool cache, bool writeAccess) const
    {
        // Order of resolution: 
        // 1. Files named in package
        // 2. If dali lookup enabled, dali
        // 3. If local file system lookup enabled, local file system?
        // 4. Files named in bases

        IResolvedFile* result = lookupCache(fileName);
        if (result)
            return result;

        Owned<const ISimpleSuperFileEnquiry> subFileInfo = resolveSuperFile(fileName);
        if (subFileInfo)
        {
            unsigned numSubFiles = subFileInfo->numSubFiles();
            if (numSubFiles==1)
            {
                // Optimize the common case of a single subfile
                StringBuffer subFileName;
                subFileInfo->getSubFileName(0, subFileName);
                return lookupFile(subFileName, cache, writeAccess);
            }
            else
            {
                // Have to do some merging...
                Owned<IResolvedFileCreator> super;
                for (unsigned idx = 0; idx < numSubFiles; idx++)
                {
                    StringBuffer subFileName;
                    subFileInfo->getSubFileName(idx, subFileName);
                    Owned<const IResolvedFile> subFileInfo = lookupFile(subFileName, cache, writeAccess);
                    if (subFileInfo)
                    {
                        if (!super) 
                            super.setown(createResolvedFile(fileName));
                        super->addSubFile(subFileInfo);
                    }
                }
                if (super && cache)
                    addCache(fileName, super);
                return super.getClear();
            }
        }
        result = resolveLFNusingPackage(fileName);
        if (!result)
            result = resolveLFNusingDali(fileName, cache, writeAccess);
        if (!result)
            result = resolveLFNusingLocal(fileName);
        if (result)
        {
            if (cache)
                addCache(fileName, result);
            return result;
        }
        ForEachItemIn(idx, bases)
        {
            const CRoxiePackage &basePackage = bases.item(idx);
            IResolvedFile *result = basePackage.lookupFile(fileName, cache, writeAccess);
            if (result)
                return result;
        }
        return NULL;
    }

    // default constructor for derived class use
    CRoxiePackage()
    {
        hash = 0;
        compulsory = false;
    }

public:
    IMPLEMENT_IINTERFACE;

    CRoxiePackage(IPropertyTree *p)
    {
        if (p)
            node.set(p);
        else
            node.setown(createPTree("RoxiePackages"));
        StringBuffer xml;
        toXML(node, xml);
        hash = rtlHash64Data(xml.length(), xml.str(), 9994410);
        compulsory = false;
        daliHelper.setown(connectToDali()); // MORE - should make this conditional
        if (!daliHelper.get() || !daliHelper->connected())
            node->setPropBool("@localFiles", true);
    }

    ~CRoxiePackage()
    {
        assertex(fileCache.count()==0);
        // If it's possible for cached objects to outlive the cache I think there is a problem...
        // we could set the cache field to null here for any objects still in cache but there would be a race condition
    }

    virtual void resolveBases(IPackageMap *packages)
    {
        loadEnvironment();
        if (packages)
        {
            Owned<IPropertyTreeIterator> baseIterator = node->getElements("Base");
            if (baseIterator->first())
            {
                do
                {
                    IPropertyTree &baseElem = baseIterator->query();
                    const char *baseId = baseElem.queryProp("@id");
                    if (!baseId)
                        throw MakeStringException(0, "PACKAGE_ERROR: base element missing id attribute");
                    const IRoxiePackage *_base = packages->queryPackage(baseId);
                    if (_base)
                    {
                        const CRoxiePackage *base = static_cast<const CRoxiePackage *>(_base);
                        bases.append(const_cast<CRoxiePackage &>(*LINK(base)));   // should really be an arrayof<const base> but that would require some fixing in jlib
                        hash = rtlHash64Data(sizeof(base->hash), &base->hash, hash);
                        mergeEnvironment(base);
                    }
                    else
                        throw MakeStringException(0, "PACKAGE_ERROR: base package %s not found", baseId);
                }
                while(baseIterator->next());
            }
            else 
            {
                const IRoxiePackage &rootPackage = queryRootPackage();
                const CRoxiePackage &base = static_cast<const CRoxiePackage &>(rootPackage);
                base.Link();
                bases.append(const_cast<CRoxiePackage &>(base));   // should really be an arryof<const base> but that would require some fixing in jlib
                hash = rtlHash64Data(sizeof(base.hash), &base.hash, hash);
                mergeEnvironment(&base);
            }
        }
        const char * val = queryEnv("control:compulsory");
        if (val)
            compulsory=strToBool(val);
    }

    virtual bool getEnableFieldTranslation() const
    {
        const char *val = queryEnv("control:enableFieldTranslation");
        if (val)
            return strToBool(val);
        else
            return fieldTranslationEnabled;
    }

    virtual const char *queryEnv(const char *varname) const
    {
        return mergedEnvironment->queryProp(varname);
    }

    virtual hash64_t queryHash() const 
    {
        return hash;
    }

    virtual IPropertyTreeIterator *getInMemoryIndexInfo(const IPropertyTree &graphNode) const 
    {
        StringBuffer xpath;
        xpath.append("SuperFile[@id='").append(queryNodeFileName(graphNode)).append("']");
        return lookupElements(xpath.str(), "MemIndex");
    }

    virtual const IResolvedFile *lookupFileName(const char *_fileName, bool opt, bool cache) const
    {
        StringBuffer fileName;
        expandLogicalFilename(fileName, _fileName, NULL, false);   // MORE - if we have a wu, and we have not yet got rid of the concept of scope, we should use it here

        const IResolvedFile *result = lookupFile(fileName, cache, false);
        if (!result)
        {
            if (!opt)
                throw MakeStringException(ROXIE_FILE_ERROR, "Could not resolve filename %s", fileName.str());
            if (traceLevel > 4)
                DBGLOG("Could not resolve OPT filename %s", fileName.str());
        }
        return result;
    }

    virtual IRoxieWriteHandler *createFileName(const char *_fileName, bool overwrite, bool extend, const StringArray &clusters) const
    {
        StringBuffer fileName;
        expandLogicalFilename(fileName, _fileName, NULL, false);   // MORE - if we have a wu, and we have not yet got rid of the concept of scope, we should use it here
        // Dali filenames used locally
        bool disconnected = !daliHelper->connected();
        if (disconnected && strstr(fileName,"::"))
        {
            StringBuffer name;
            bool wasDFS;
            makeSinglePhysicalPartName(fileName, name, true, wasDFS, baseDataDirectory.str());
            fileName.clear().append(name);
        }
        Owned<IResolvedFile> resolved = lookupFile(fileName, false, true);
        if (resolved)
        {
            if (!overwrite)
                throw MakeStringException(99, "Cannot write %s, file already exists (missing OVERWRITE attribute?)", resolved->queryFileName());
            if (extend)
                UNIMPLEMENTED; // How does extend fit in with the clusterwritemanager stuff? They can't specify cluster and extend together...
            removeCache(resolved);
            resolved->remove();
            resolved.clear();
        }
        Owned<ILocalOrDistributedFile> ldFile = createLocalOrDistributedFile(fileName, NULL, disconnected, false, true); // MORE - is onlyDFS right?
        if (!ldFile)
            throw MakeStringException(ROXIE_FILE_ERROR, "Cannot write %s, %s file not found", fileName.str(), (disconnected?"local":"DFS"));

        return createRoxieWriteHandler(daliHelper, ldFile.getClear(), clusters);
    }

    virtual const IPropertyTree *queryTree() const
    {
        return node;
    }

    virtual IPropertyTree *getQuerySets() const
    {
        if (node)
            return node->getPropTree("QuerySets");
        else
            return NULL;
    }
};

IRoxiePackage *createPackage(IPropertyTree *p)
{
    return new CRoxiePackage(p);
}

//================================================================================================
// CPackageMap - an implementation of IPackageMap using a string map
//================================================================================================

class CPackageMap : public CInterface, implements IPackageMap
{
    MapStringToMyClass<IRoxiePackage> packages;
    StringAttr packageId;
    bool active;
    StringArray wildMatches, wildIds;
public:
    IMPLEMENT_IINTERFACE;
    CPackageMap(const char *_packageId, bool _active)
        : packageId(_packageId), active(_active), packages(true)
    {
    }

    // IPackageMap interface
    virtual bool isActive() const
    {
        return active;
    }
    virtual const IRoxiePackage *queryPackage(const char *name) const
    {
        return name ? packages.getValue(name) : NULL;
    }
    virtual const IRoxiePackage *matchPackage(const char *name) const
    {
        if (name)
        {
            ForEachItemIn(idx, wildMatches)
            {
                if (WildMatch(name, wildMatches.item(idx), true))
                    return queryPackage(wildIds.item(idx));
            }
        }
        return NULL;
    }
    virtual const char *queryPackageId() const
    {
        return packageId;
    }
    void load(IPropertyTree *xml)
    {
        Owned<IPropertyTreeIterator> allpackages = xml->getElements("Package");
        ForEach(*allpackages)
        {
            IPropertyTree &packageTree = allpackages->query();
            const char *id = packageTree.queryProp("@id");
            if (id && *id)
            {
                Owned<IRoxiePackage> package = createPackage(&packageTree);
                package->resolveBases(this);
                packages.setValue(id, package);
                const char *queries = packageTree.queryProp("@queries");
                if (queries && *queries)
                {
                    wildMatches.append(queries);
                    wildIds.append(id);
                }
            }
            else
                throw MakeStringException(ROXIE_UNKNOWN_PACKAGE, "Invalid package map - Package element missing id attribute");
        }
    }
};

static CPackageMap *emptyPackageMap;
static CRoxiePackage *rootPackage;
static SpinLock emptyPackageMapCrit;
static IRoxieDebugSessionManager *debugSessionManager;

extern const IRoxiePackage &queryRootPackage()
{
    SpinBlock b(emptyPackageMapCrit);
    if (!rootPackage)
    {
        // Set up the root package. This contains global settings from topology file
        rootPackage = new CRoxiePackage(topology); // attributes become control: environment settings. Rest of topology ignored.
        rootPackage->resolveBases(NULL);
    }
    return *rootPackage;
}

extern const IPackageMap &queryEmptyPackageMap()
{
    SpinBlock b(emptyPackageMapCrit);
    if (!emptyPackageMap)
        emptyPackageMap = new CPackageMap("<none>", true);
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
//    - CRoxieSlaveQuerySetManager
//
// Manages a set of instantiated queries and allows us to look them up by queryname or alias
//
// IRoxieQuerySetManagerSet
// - CRoxieSlaveQuerySetManagerSet
//
// Manages the IRoxieQuerySetManager for multiple channels
//
// CRoxieQueryPackageManager
// - CRoxieDaliQueryPackageManager
// - CStandaloneQueryPackageManager
//
// Groups a server resource manager and a set of slave resource managers (one per channel) together.
// There is one per PackageMap
//
// CQueryPackageSetManager at outer level
// There will be exactly one of these. It will reload the CQueryPackageManager's if dali Package info changes

//================================================================================================
// CRoxieQuerySetManager - shared base class for slave and server query set manager classes
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

    void addQuery(const char *id, IQueryFactory *n, hash64_t &hash)
    {
        hash = rtlHash64Data(sizeof(hash), &hash, n->queryHash());
        queries.setValue(id, n);
        n->Release();  // setValue links
    }

    void addAlias(const char *alias, const char *original, hash64_t &hash)
    {
        if (original && alias)
        {
            IQueryFactory *orig = queries.getValue(original);
            if (orig)
            {
                hash = rtlHash64VStr(alias, hash);
                hash = rtlHash64Data(sizeof(hash), &hash, orig->queryHash());
                aliases.setValue(alias, orig);
            }
            else
                throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", original);
        }
        else
            throw MakeStringException(ROXIE_INTERNAL_ERROR, "Invalid parameters to addAlias");
    }

    virtual IQueryFactory *loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, IRoxieLibraryLookupContext *libraryContext) = 0;

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

    virtual void load(const IPropertyTree *querySet, const IPackageMap &packages, hash64_t &hash, IRoxieLibraryLookupContext *libraryContext)
    {
        Owned<IPropertyTreeIterator> queryNames = querySet->getElements("Query");
        ForEach (*queryNames)
        {
            const IPropertyTree &query = queryNames->query();
            try
            {
                const char *id = query.queryProp("@id");
                const char *dllName = query.queryProp("@dll");
                if (!id || !*id || !dllName || !*dllName)
                    throw MakeStringException(ROXIE_QUERY_MODIFICATION, "dll and id must be specified");
                Owned<const IQueryDll> queryDll = createQueryDll(dllName);
                const IRoxiePackage *package = NULL;
                const char *packageName = query.queryProp("@package");
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
                    if (!package) package = &queryRootPackage();
                }
                assertex(package);
                addQuery(id, loadQueryFromDll(id, queryDll.getClear(), *package, &query, libraryContext), hash);
            }
            catch (IException *E)
            {
                // we don't want a single bad query in the set to stop us loading all the others
                StringBuffer msg, qxml;
                toXML(&query, qxml);
                msg.appendf("Failed to load query: %s", qxml.str());
                EXCLOG(E, msg.str());
                E->Release();
            }
        }

        Owned<IPropertyTreeIterator> a = querySet->getElements("Alias");
        ForEach(*a)
        {
            IPropertyTree &item = a->query();
            const char *alias = item.queryProp("@name");
            const char *original = item.queryProp("@id");
            try
            {
                addAlias(alias, original, hash);
            }
            catch (IException *E)
            {
                // we don't want a single bad alias in the set to stop us loading all the others
                VStringBuffer msg("Failed to set alias %s on %s", alias, original);
                EXCLOG(E, msg.str());
                E->Release();
            }
        }
        active = packages.isActive();
        if (active)
            hash = rtlHash64VStr("active", hash);
    }

    virtual void getStats(const char *queryName, const char *graphName, StringBuffer &reply, const IRoxieContextLogger &logctx) const
    {
        Owned<IQueryFactory> f = getQuery(queryName, logctx);
        if (f)
        {
            reply.appendf("<Query id='%s'>\n", queryName);
            f->getStats(reply, graphName);
            reply.append("</Query>\n");
        }
        else
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName);
    }

    virtual void resetQueryTimings(const char *queryName, const IRoxieContextLogger &logctx)
    {
        Owned<IQueryFactory> f = getQuery(queryName, logctx);
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

    virtual void getAllQueryInfo(StringBuffer &reply, bool full, const IRoxieContextLogger &logctx) const
    {
        HashIterator elems(queries);
        for (elems.first(); elems.isValid(); elems.next())
        {
            IMapping &cur = elems.query();
            IQueryFactory *query = queries.mapToValue(&cur);
            query->getQueryInfo(reply, full, logctx);
        }
        HashIterator aliasIterator(aliases);
        for (aliasIterator.first(); aliasIterator.isValid(); aliasIterator.next())
        {
            IMapping &cur = aliasIterator.query();
            reply.appendf(" <Alias id='%s' query='%s'/>\n", (const char *) cur.getKey(), aliases.mapToValue(&cur)->queryQueryName());
        }
    }

    virtual IQueryFactory * lookupLibrary(const char * libraryName, unsigned expectedInterfaceHash, const IRoxieContextLogger &logctx) const
    {
#ifdef _DEBUG
        DBGLOG("Lookup library %s (hash %d)", libraryName, expectedInterfaceHash);
#endif
        Owned<IQueryFactory> query = getQuery(libraryName, logctx);
        if (query)
        {
            if (query->isQueryLibrary())
            {
                unsigned foundInterfaceHash = query->getQueryLibraryInterfaceHash();
                if (!foundInterfaceHash  || (foundInterfaceHash == expectedInterfaceHash))
                    return query.getClear();
                else
                    throw MakeStringException(ROXIE_LIBRARY_ERROR, "The library interface found in %s is not compatible (found %d, expected %d)", libraryName, foundInterfaceHash, expectedInterfaceHash);
            }
            else
                throw MakeStringException(ROXIE_LIBRARY_ERROR, "The query resolved by %s is not a library", libraryName);
        }
        else
            throw MakeStringException(ROXIE_LIBRARY_ERROR, "No compatible library available for %s", libraryName);
    }

    virtual IQueryFactory *getQuery(const char *id, const IRoxieContextLogger &logctx) const
    {
        IQueryFactory *ret;
        ret = aliases.getValue(id);
        if (ret && logctx.queryTraceLevel() > 5)
            logctx.CTXLOG("Found query alias %s => %s", id, ret->queryQueryName());
        if (!ret)
            ret = queries.getValue(id);
        return LINK(ret);
    }

};

//===============================================================================================================

class CRoxieServerQuerySetManager : public CRoxieQuerySetManager
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieServerQuerySetManager(const char *_querySetName)
        : CRoxieQuerySetManager(0, _querySetName)
    {
    }

    virtual IQueryFactory * loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, IRoxieLibraryLookupContext *libraryContext)
    {
        return createServerQueryFactory(id, dll, package, stateInfo, libraryContext);
    }

};

extern IRoxieQuerySetManager *createServerManager(const char *querySet)
{
    return new CRoxieServerQuerySetManager(querySet);
}

//===============================================================================================================

class CRoxieSlaveQuerySetManager : public CRoxieQuerySetManager
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieSlaveQuerySetManager(unsigned _channelNo, const char *_querySetName)
        : CRoxieQuerySetManager(_channelNo, _querySetName)
    {
        channelNo = _channelNo;
    }

    virtual IQueryFactory *loadQueryFromDll(const char *id, const IQueryDll *dll, const IRoxiePackage &package, const IPropertyTree *stateInfo, IRoxieLibraryLookupContext *libraryContext)
    {
        return createSlaveQueryFactory(id, dll, package, channelNo, stateInfo, libraryContext);
    }

};

class CRoxieSlaveQuerySetManagerSet : public CInterface, implements IRoxieQuerySetManagerSet
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxieSlaveQuerySetManagerSet(unsigned _numChannels, const char *querySetName)
        : numChannels(_numChannels)
    {
        CriticalBlock b(ccdChannelsCrit);
        managers = new CRoxieSlaveQuerySetManager *[numChannels];
        memset(managers, 0, sizeof(CRoxieSlaveQuerySetManager *) * numChannels);
        Owned<IPropertyTreeIterator> it = ccdChannels->getElements("RoxieSlaveProcess");
        ForEach(*it)
        {
            unsigned channelNo = it->query().getPropInt("@channel", 0);
            assertex(channelNo>0 && channelNo<=numChannels);
            if (managers[channelNo-1] == NULL)
                managers[channelNo-1] = new CRoxieSlaveQuerySetManager(channelNo, querySetName);
            else
                throw MakeStringException(ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channel %d repeated for this slave", channelNo);
        }
    }

    ~CRoxieSlaveQuerySetManagerSet()
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            ::Release(managers[channel]);
        delete [] managers;
    }

    inline CRoxieSlaveQuerySetManager *item(int idx)
    {
        return managers[idx];
    }

    virtual void load(const IPropertyTree *querySets, const IPackageMap &packages, hash64_t &hash, IRoxieLibraryLookupContext *libraryContext)
    {
        for (unsigned channel = 0; channel < numChannels; channel++)
            if (managers[channel])
                managers[channel]->load(querySets, packages, hash, libraryContext); // MORE - this means the hash depends on the number of channels. Is that desirable?
    }

private:
    unsigned numChannels;
    CRoxieSlaveQuerySetManager **managers;
};

//===============================================================================================================

class CRoxieDebugSessionManager : public CInterface, implements IRoxieDebugSessionManager
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

class CRoxieQueryPackageManager : public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CRoxieQueryPackageManager(unsigned _numChannels, const char *_querySet, const IPackageMap *_packages)
        : numChannels(_numChannels), packages(_packages), querySet(_querySet)
    {
        queryHash = 0;
    }

    ~CRoxieQueryPackageManager()
    {
    }

    inline const char *queryPackageId() const
    {
        return packages->queryPackageId();
    }

    virtual void reload()
    {
        // Default is to do nothing...
    }

    virtual void load() = 0;

    virtual hash64_t getHash()
    {
        CriticalBlock b2(updateCrit);
        return queryHash;
    }

    IRoxieQuerySetManager* getRoxieServerManager()
    {
        CriticalBlock b2(updateCrit);
        return serverManager.getLink();
    }

    void resetStats(const char *queryId, const IRoxieContextLogger &logctx)
    {
        CriticalBlock b(updateCrit);
        if (queryId)
        {
            Owned<IQueryFactory> query = serverManager->getQuery(queryId, logctx);
            if (query)
            {
                const char *id = query->queryQueryName();
                serverManager->resetQueryTimings(id, logctx);
                for (unsigned channel = 0; channel < numChannels; channel++)
                    if (slaveManagers->item(channel))
                    {
                        slaveManagers->item(channel)->resetQueryTimings(id, logctx);
                    }
            }
        }
        else
        {
            serverManager->resetAllQueryTimings();
            for (unsigned channel = 0; channel < numChannels; channel++)
                if (slaveManagers->item(channel))
                    slaveManagers->item(channel)->resetAllQueryTimings();
        }
    }

    void getStats(const char *queryId, const char *action, const char *graphName, StringBuffer &reply, const IRoxieContextLogger &logctx) const
    {
        CriticalBlock b2(updateCrit);
        Owned<IQueryFactory> query = serverManager->getQuery(queryId, logctx);
        if (query)
        {
            StringBuffer freply;
            serverManager->getStats(queryId, graphName, freply, logctx);
            Owned<IPropertyTree> stats = createPTreeFromXMLString(freply.str());
            for (unsigned channel = 0; channel < numChannels; channel++)
                if (slaveManagers->item(channel))
                {
                    StringBuffer sreply;
                    slaveManagers->item(channel)->getStats(queryId, graphName, sreply, logctx);
                    Owned<IPropertyTree> cstats = createPTreeFromXMLString(sreply.str());
                    mergeStats(stats, cstats, 1);
                }
            toXML(stats, reply);
        }
    }
    void getActivityMetrics(StringBuffer &reply) const
    {
        CriticalBlock b2(updateCrit);
        serverManager->getActivityMetrics(reply);
        for (unsigned channel = 0; channel < numChannels; channel++)
        {
            if (slaveManagers->item(channel))
            {
                slaveManagers->item(channel)->getActivityMetrics(reply);
            }
        }
    }
    void getAllQueryInfo(StringBuffer &reply, bool full,  const IRoxieContextLogger &logctx) const
    {
        CriticalBlock b2(updateCrit);
        serverManager->getAllQueryInfo(reply, full, logctx);
    }
protected:
    void reloadQueryManagers(CRoxieSlaveQuerySetManagerSet *newSlaveManagers, IRoxieQuerySetManager *newServerManager, hash64_t newHash)
    {
        Owned<CRoxieSlaveQuerySetManagerSet> oldSlaveManagers;
        Owned<IRoxieQuerySetManager> oldServerManager;
        {
            // Atomically, replace the existing query managers with the new ones
            CriticalBlock b2(updateCrit);
            oldSlaveManagers.setown(slaveManagers.getClear()); // so that the release happens outside the critblock
            oldServerManager.setown(serverManager.getClear()); // so that the release happens outside the critblock
            slaveManagers.setown(newSlaveManagers);
            serverManager.setown(newServerManager);
            queryHash = newHash;
        }
    }

    mutable CriticalSection updateCrit;  // protects updates of slaveManagers and serverManager
    Owned<CRoxieSlaveQuerySetManagerSet> slaveManagers;
    Owned<IRoxieQuerySetManager> serverManager;

    Owned<const IPackageMap> packages;
    unsigned numChannels;
    hash64_t queryHash;
    StringAttr querySet;
};

/**
 * class CRoxieDaliQueryPackageManager - manages queries specified in QuerySets, for a given package set.
 *
 * If the QuerySet is modified, it will be reloaded.
 * There is one CRoxieDaliQueryPackageManager for every PackageSet - only one will be active for query lookup
 * at a given time (the one associated with the active PackageSet).
 *
 * To deploy new data, typically we will load a new PackageSet, make it active, then release the old one
 * A packageSet is not modified while loaded, to avoid timing issues between slaves and server.
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
class CRoxieDaliQueryPackageManager : public CRoxieQueryPackageManager, implements ISDSSubscription
{
    Owned<IRoxieDaliHelper> daliHelper;
    Owned<IDaliPackageWatcher> notifier;

public:
    IMPLEMENT_IINTERFACE;
    CRoxieDaliQueryPackageManager(unsigned _numChannels, const IPackageMap *_packages, const char *_querySet)
        : CRoxieQueryPackageManager(_numChannels, _querySet, _packages)
    {
        daliHelper.setown(connectToDali());
    }

    ~CRoxieDaliQueryPackageManager()
    {
        if (notifier)
            daliHelper->releaseSubscription(notifier);
    }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        reload();
        daliHelper->commitCache();
    }

    virtual void load()
    {
        notifier.setown(daliHelper->getQuerySetSubscription(querySet, this));
        reload();
    }

    virtual void reload()
    {
        hash64_t newHash = numChannels;
        Owned<IPropertyTree> newQuerySet = daliHelper->getQuerySet(querySet);
        Owned<CRoxieSlaveQuerySetManagerSet> newSlaveManagers = new CRoxieSlaveQuerySetManagerSet(numChannels, querySet);
        Owned<IRoxieQuerySetManager> newServerManager = createServerManager(querySet);
        newServerManager->load(newQuerySet, *packages, newHash, newServerManager);
        newSlaveManagers->load(newQuerySet, *packages, newHash, newServerManager);
        reloadQueryManagers(newSlaveManagers.getClear(), newServerManager.getClear(), newHash);
        clearKeyStoreCache(false);   // Allows us to fully release files we no longer need because of unloaded queries
    }

};

class CStandaloneQueryPackageManager : public CRoxieQueryPackageManager
{
    Owned<IPropertyTree> standaloneDll;

public:
    IMPLEMENT_IINTERFACE;

    CStandaloneQueryPackageManager(unsigned _numChannels, const char *_querySet, const IPackageMap *_packages, IPropertyTree *_standaloneDll)
        : CRoxieQueryPackageManager(_numChannels, _querySet, _packages), standaloneDll(_standaloneDll)
    {
        assertex(standaloneDll);
    }

    ~CStandaloneQueryPackageManager()
    {
    }

    virtual void load()
    {
        hash64_t newHash = numChannels;
        Owned<IPropertyTree> newQuerySet = createPTree("QuerySet");
        newQuerySet->setProp("@name", "_standalone");
        newQuerySet->addPropTree("Query", standaloneDll.getLink());
        Owned<CRoxieSlaveQuerySetManagerSet> newSlaveManagers = new CRoxieSlaveQuerySetManagerSet(numChannels, querySet);
        Owned<IRoxieQuerySetManager> newServerManager = createServerManager(querySet);
        newServerManager->load(newQuerySet, *packages, newHash, newServerManager);
        newSlaveManagers->load(newQuerySet, *packages, newHash, newServerManager);
        reloadQueryManagers(newSlaveManagers.getClear(), newServerManager.getClear(), newHash);
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

class CRoxiePackageSetManager : public CInterface, implements IRoxieQueryPackageManagerSet, implements ISDSSubscription
{
public:
    IMPLEMENT_IINTERFACE;
    CRoxiePackageSetManager(const IQueryDll *_standAloneDll) :
        standAloneDll(_standAloneDll)
    {
        daliHelper.setown(connectToDali(ROXIE_DALI_CONNECT_TIMEOUT));
    }

    ~CRoxiePackageSetManager()
    {
        unsubscribe();
    }

    virtual void load()
    {
        try
        {
            reload();
            daliHelper->commitCache();
            controlSem.signal();
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

    virtual IRoxieLibraryLookupContext *getLibraryLookupContext(const char *querySet) const
    {
        ReadLockBlock b(packageCrit);
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
            if (sm->isActive() && strcmp(sm->queryId(), querySet)==0)
                return sm.getClear();
        }
        return NULL;
    }

    virtual IQueryFactory *getQuery(const char *id, const IRoxieContextLogger &logctx) const
    {
        ReadLockBlock b(packageCrit);
        ForEachItemIn(idx, allQueryPackages)
        {
            Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
            if (sm->isActive())
            {
                IQueryFactory *query = sm->getQuery(id, logctx);
                if (query)
                    return query;
            }
        }
        return NULL;
    }

    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        reload();
        daliHelper->commitCache();
    }

private:
    Owned<const IQueryDll> standAloneDll;
    Owned<CRoxieDebugSessionManager> debugSessionManager;
    CIArrayOf<CRoxieQueryPackageManager> allQueryPackages;
    mutable ReadWriteLock packageCrit;
    InterruptableSemaphore controlSem;
    IArrayOf<IDaliPackageWatcher> notifiers;
    Owned<IRoxieDaliHelper> daliHelper;
    hash64_t stateHash;

    void reload()
    {
        WriteLockBlock b(packageCrit);
        if (standAloneDll)
            loadStandaloneQuery(standAloneDll, numChannels, "roxie");
        else
        {
            ForEachItemIn(idx, allQuerySetNames)
            {
                createQueryPackageManagers(numChannels, allQuerySetNames.item(idx));
            }
        }
    }

    // Common code used by control:queries and control:getQueryXrefInfo

    void getQueryInfo(IPropertyTree *control, StringBuffer &reply, bool full, const IRoxieContextLogger &logctx)
    {
        Owned<IPropertyTreeIterator> ids = control->getElements("Query");
        reply.append("<Queries>\n");
        if (ids->first())
        {
            ForEach(*ids)
            {
                const char *id = ids->query().queryProp("@id");
                if (!id)
                    throw MakeStringException(ROXIE_MISSING_PARAMS, "Query name missing");
                Owned<IQueryFactory> query = getQuery(id, logctx);
                if (!query)
                    throw MakeStringException(ROXIE_MISSING_PARAMS, "Query %s not found", id);
                query->getQueryInfo(reply, full, logctx);
            }
        }
        else
        {
            ForEachItemIn(idx, allQueryPackages)
            {
                Owned<IRoxieQuerySetManager> sm = allQueryPackages.item(idx).getRoxieServerManager();
                sm->getAllQueryInfo(reply, full, logctx);
            }
        }
        reply.append("</Queries>\n");
    }

    void _doControlMessage(IPropertyTree *control, StringBuffer &reply, const IRoxieContextLogger &logctx)
    {
        ReadLockBlock readBlock(packageCrit); // Some of the control activities below need a lock
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
                debugSessionManager->getActiveQueries(reply);
            }
            else if (stricmp(queryName, "control:activitymetrics")==0)
            {
                ForEachItemIn(idx, allQueryPackages)
                {
                    CRoxieQueryPackageManager &qpm = allQueryPackages.item(idx);
                    qpm.getActivityMetrics(reply);
                }
            }
            else if (stricmp(queryName, "control:alive")==0)
            {
                reply.appendf("<Alive restarts='%d'/>", restarts);
            }
            else if (stricmp(queryName, "control:allowRoxieOnDemand")==0)
            {
                allowRoxieOnDemand= control->getPropBool("@val", true);
                topology->setPropBool("@allowRoxieOnDemand", allowRoxieOnDemand);
            }
            else
                unknown = true;
            break;

        case 'B':
            if (stricmp(queryName, "control:blobCacheMem")==0)
            {
                blobCacheMB = control->getPropInt("@val", 0);
                topology->setPropInt("@blobCacheMem", blobCacheMB);
                setBlobCacheMem(blobCacheMB * 0x100000);
            }
            else
                unknown = true;
            break;

        case 'C':
            if (stricmp(queryName, "control:checkCompleted")==0)
            {
                checkCompleted = control->getPropBool("@val", true);
                topology->setPropBool("@checkCompleted", checkCompleted );
            }
            else if (stricmp(queryName, "control:clearIndexCache")==0)
            {
                bool clearAll = control->getPropBool("@clearAll", true);
                clearKeyStoreCache(clearAll);
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
            }
            else if (stricmp(queryName, "control:defaultCheckingHeap")==0)
            {
                defaultCheckingHeap = control->getPropInt("@val", false);
                topology->setPropInt("@checkingHeap", defaultCheckingHeap);
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
            else if (stricmp(queryName, "control:deleteKeyDiffFiles")==0)
            {
                UNIMPLEMENTED;
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

        case 'E':
            if (stricmp(queryName, "control:enableKeyDiff")==0)
            {
                enableKeyDiff = control->getPropBool("@val", true);
                topology->setPropBool("@enableKeyDiff", enableKeyDiff);
            }
            else if (stricmp(queryName, "control:enableForceKeyDiffCopy")==0)
            {
                enableForceKeyDiffCopy = control->getPropBool("@val", false);
                topology->setPropBool("@enableForceKeyDiffCopy", enableForceKeyDiffCopy);
            }
            else
                unknown = true;
            break;
            
        case 'F':
            if (stricmp(queryName, "control:fieldTranslationEnabled")==0)
            {
                fieldTranslationEnabled = control->getPropBool("@val", true);
                topology->setPropInt("@fieldTranslationEnabled", fieldTranslationEnabled);
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
            else if (stricmp(queryName, "control:getKeyInfo")==0)
            {
                reportInMemoryIndexStatistics(reply, control->queryProp("@id"), control->getPropInt("@count", 10));
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

                Owned<IQueryFactory> q = getQuery(id, logctx);
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
                Owned<IQueryFactory> f = getQuery(id, logctx);
                if (f)
                {
                    unsigned warnLimit = f->getWarnTimeLimit();
                    reply.appendf("<QueryTimeWarning val='%d'/>", warnLimit);
                }
            }
            else if (stricmp(queryName, "control:getBuildVersion")==0)
            {
                reply.appendf("<version id='%s'/>", BUILD_TAG);
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
                setLeafCacheMem(leafCacheMB * 0x100000);
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
                topology->setPropBool("@lockDali", true);
                if (daliHelper)
                    daliHelper->disconnect();
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
                roxiemem::memTraceLevel = control->getPropInt("@level", 0);
                topology->setPropInt("@memTraceLevel", roxiemem::memTraceLevel);
            }
            else if (stricmp(queryName, "control:memtracesizelimit")==0)
            {
                roxiemem::memTraceSizeLimit = (memsize_t) control->getPropInt64("@val", control->getPropInt64("@value", 0)); // used to accept @value so coded like this for backward compatibility
                topology->setPropInt64("@memTraceSizeLimit", roxiemem::memTraceSizeLimit);
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
            else
                unknown = true;
            break;

        case 'N':
            if (stricmp(queryName, "control:nodeCachePreload")==0)
            {
                nodeCachePreload = control->getPropBool("@val", true);
                topology->setPropBool("@nodeCachePreload", nodeCachePreload);
                setNodeCachePreload(nodeCachePreload);
            }
            else if (stricmp(queryName, "control:nodeCacheMem")==0)
            {
                nodeCacheMB = control->getPropInt("@val", 100);
                topology->setPropInt("@nodeCacheMem", nodeCacheMB);
                setNodeCacheMem(nodeCacheMB * 0x100000);
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
            else if (stricmp(queryName, "control:pingInterval")==0)
            {
                unsigned newInterval = (unsigned) control->getPropInt64("@val", 0);
                if (newInterval && !pingInterval)
                {
                    pingInterval = newInterval; // best to set before the start...
                    startPingTimer();
                }
                else if (pingInterval && !newInterval)
                    stopPingTimer();  // but after the stop
                pingInterval = newInterval;
                topology->setPropInt64("@pingInterval", pingInterval);
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
            else if (stricmp(queryName, "control:probeAllRows")==0)
            {
                probeAllRows = control->getPropBool("@val", true);
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
                const char *id = control->queryProp("Query/@id");
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
                if (fromTime)
                {
                    CDateTime t;
                    t.setString(toTime, NULL, true);
                    to = t.getSimple();
                }
                else
                    time(&to);
                if (id)
                {
                    Owned<IQueryFactory> f = getQuery(id, logctx);
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
                    Owned<const IPropertyTree> stats = getAllQueryStats(from, to);
                    toXML(stats, reply);
                }
            }
            else if (stricmp(queryName, "control:queryDiffFileInfoCache")==0)
            {
                queryDiffFileInfoCache()->queryDiffFileNames(reply);
            }
            else if (stricmp(queryName, "control:queryPackageInfo")==0)
            {
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:queryStats")==0)
            {
                const char *id = control->queryProp("Query/@id");
                if (!id)
                    badFormat();
                const char *action = control->queryProp("Query/@action");
                const char *graphName = 0;
                if (action)
                {
                    if (stricmp(action, "listGraphNames") == 0)
                    {
                        Owned<IQueryFactory> query = getQuery(id, logctx);
                        if (query)
                        {
                            reply.appendf("<Query id='%s'>\n", id);
                            StringArray graphNames;
                            query->getGraphNames(graphNames);
                            ForEachItemIn(idx, graphNames)
                            {
                                const char *graphName = graphNames.item(idx);
                                reply.appendf("<Graph id='%s'/>", graphName);
                            }
                            reply.appendf("</Query>\n");
                        }
                        return;  // done
                    }
                    else if (stricmp(action, "selectGraph") == 0)
                        graphName = control->queryProp("Query/@name");
                    else if (stricmp(action, "allGraphs") != 0)  // if we get here and its NOT allgraphs - then error
                        throw MakeStringException(ROXIE_CONTROL_MSG_ERROR, "invalid action in control:queryState %s", action);
                }
                ForEachItemIn(idx, allQueryPackages)
                {
                    allQueryPackages.item(idx).getStats(id, action, graphName, reply, logctx);
                }
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
                readBlock.clear();
                reload();
                if (daliHelper && daliHelper->connected())
                    reply.appendf("<Dali connected='1'/>");
                else
                    reply.appendf("<Dali connected='0'/>");
                reply.appendf("<State hash='%"I64F"u'/>", (unsigned __int64) stateHash);
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
                Owned<IPropertyTreeIterator> queries = control->getElements("Query");
                if (queries->first())
                {
                    while (queries->isValid())
                    {
                        IPropertyTree &query = queries->query();
                        const char *id = query.queryProp("@id");
                        if (!id)
                            badFormat();
                        ForEachItemIn(idx, allQueryPackages)
                        {
                            allQueryPackages.item(idx).resetStats(id, logctx);
                        }
                        queries->next();
                    }
                }
                else
                {
                    ForEachItemIn(idx, allQueryPackages)
                    {
                        allQueryPackages.item(idx).resetStats(NULL, logctx);
                    }
                }
            }
            else if (stricmp(queryName, "control:restart")==0)
            {
                FatalError("Roxie process restarted by operator request");
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
            if (stricmp(queryName, "control:setCopyResources")==0)
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
            else if (stricmp(queryName, "control:state")==0)
            {
                if (daliHelper && daliHelper->connected())
                    reply.appendf("<Dali connected='1'/>");
                else
                    reply.appendf("<Dali connected='0'/>");
                reply.appendf("<State hash='%"I64F"u'/>", (unsigned __int64) stateHash);
            }
            else if (stricmp(queryName, "control:status")==0)
            {
                CriticalBlock b(ccdChannelsCrit);
                toXML(ccdChannels, reply);
            }
            else if (stricmp(queryName, "control:steppingEnabled")==0)
            {
                steppingEnabled = control->getPropBool("@val", true);
            }
            else if (stricmp(queryName, "control:suspend")==0)
            {
                StringBuffer id(control->queryProp("Query/@id"));
                if (!id.length())
                    badFormat();
                {
                    Owned<IQueryFactory> f = getQuery(id, logctx);
                    if (f)
                        id.clear().append(f->queryQueryName());  // use the spelling of the query stored with the query factory
                }
                UNIMPLEMENTED;
            }
            else if (stricmp(queryName, "control:suspendChannel")==0)
            {
                if (control->hasProp("@channel") && control->hasProp("@suspend"))
                {
                    unsigned channel = control->getPropInt("@channel", 0);
                    bool suspend = control->getPropBool("@suspend", true);
                    CriticalBlock b(ccdChannelsCrit);
                    if (channel)
                    {
                        StringBuffer xpath;
                        IPropertyTree *slaveNode = ccdChannels->queryPropTree(xpath.appendf("RoxieSlaveProcess[@channel='%u']", channel).str());
                        if (slaveNode)
                        {
                            ROQ->suspendChannel(channel, suspend, logctx);
                            slaveNode->setPropBool("@suspended", suspend);
                        }
                        else
                            throw MakeStringException(ROXIE_INVALID_INPUT, "Unknown channel %u", channel);
                    }
                    else
                    {
                        Owned<IPropertyTreeIterator> slaves = ccdChannels->getElements("RoxieSlaveProcess");
                        ForEach(*slaves)
                        {
                            IPropertyTree &slaveNode = slaves->query();
                            channel = slaveNode.getPropInt("@channel", 0);
                            ROQ->suspendChannel(channel, suspend, logctx);
                            slaveNode.setPropBool("@suspended", suspend);
                        }
                    }
                    toXML(ccdChannels, reply);
                }
                else
                    badFormat();
            }
            else if (stricmp(queryName, "control:suspendServer")==0)
            {
                if (control->hasProp("@port") && control->hasProp("@suspend"))
                {
                    unsigned port = control->getPropInt("@port", 0);
                    bool suspend = control->getPropBool("@suspend", true);
                    CriticalBlock b(ccdChannelsCrit);
                    if (port)
                    {
                        StringBuffer xpath;
                        IPropertyTree *serverNode = ccdChannels->queryPropTree(xpath.appendf("RoxieServerProcess[@port='%u']", port).str());
                        if (serverNode)
                        {
                            suspendRoxieListener(port, suspend);
                            serverNode->setPropBool("@suspended", suspend);
                        }
                        else
                            throw MakeStringException(ROXIE_INVALID_INPUT, "Unknown Roxie server port %u", port);
                    }
                    else
                    {
                        Owned<IPropertyTreeIterator> servers = ccdChannels->getElements("RoxieServerProcess");
                        ForEach(*servers)
                        {
                            IPropertyTree &serverNode = servers->query();
                            port = serverNode.getPropInt("@port", 0);
                            suspendRoxieListener(port, suspend);
                            serverNode.setPropBool("@suspended", suspend);
                        }
                    }
                    toXML(ccdChannels, reply);
                }
                else
                    badFormat();
            }
            else if (stricmp(queryName, "control:systemMonitor")==0)
            {
                unsigned interval = control->getPropInt("@interval", 60000);
                bool enable = control->getPropBool("@enable", true);
                if (enable)
                    startPerformanceMonitor(interval);
                else
                    stopPerformanceMonitor();
            }
            else
                unknown = true;
            break;

        case 'T':
            if (stricmp(queryName, "control:testSlaveFailure")==0)
            {
                testSlaveFailure = control->getPropInt("@val", 20);
            }
            else if (stricmp(queryName, "control:timeActivities")==0)
            {
                timeActivities = control->getPropBool("@val", true);
                topology->setPropInt("@timeActivities", timeActivities);
            }
            else if (stricmp(queryName, "control:timings")==0)
            {
                reply.append("<Timings>");
                timer->getTimings(reply);
                reply.append("</Timings>");
                if (control->getPropBool("@reset", false))
                {
                    timer->reset();
                }
            }
            else if (stricmp(queryName, "control:topology")==0)
            {
                toXML(topology, reply);
            }
            else if (stricmp(queryName, "control:trace")==0)
            {
                traceLevel = control->getPropInt("@level", 0);
                if (traceLevel > MAXTRACELEVEL)
                    traceLevel = MAXTRACELEVEL;
                topology->setPropInt("@traceLevel", traceLevel);
            }
            else if (stricmp(queryName, "control:traceServerSideCache")==0)
            {
                traceServerSideCache = control->getPropBool("@val", true);
                topology->setPropInt("@traceServerSideCache", traceServerSideCache);
            }
            else if (stricmp(queryName, "control:traceJHtreeAllocations")==0)
            {
                traceJHtreeAllocations = control->getPropBool("@val", true);
                topology->setPropInt("@traceJHtreeAllocations", traceJHtreeAllocations);
            }
            else if (stricmp(queryName, "control:traceSmartStepping")==0)
            {
                traceSmartStepping = control->getPropBool("@val", true);
                topology->setPropInt("@traceSmartStepping", traceSmartStepping);
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
            else if (stricmp(queryName, "control:useTreeCopy")==0)
            {
                useTreeCopy = control->getPropBool("@val", true);
                topology->setPropInt("@useTreeCopy", useTreeCopy);
            }
            else
                unknown = true;
            break;

        case 'W':
            if (stricmp(queryName, "control:watchActivityId")==0)
            {
                watchActivityId = control->getPropInt("@id", true);
            }
            else
                unknown = true;
            break;

        default:
            unknown = true;
            break;
        }
        if (unknown)
            throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown query %s", queryName);
    }

    void badFormat()
    {
        throw MakeStringException(ROXIE_INVALID_INPUT, "Badly formated control query");
    }

    void createQueryPackageManager(unsigned numChannels, const IPackageMap *packageMap, const char *querySet)
    {
        // Called from reload inside write lock block
        Owned<CRoxieQueryPackageManager> qpm = new CRoxieDaliQueryPackageManager(numChannels, packageMap, querySet);
        qpm->load();
        stateHash = rtlHash64Data(sizeof(stateHash), &stateHash, qpm->getHash());
        allQueryPackages.append(*qpm.getClear());
    }

    void createQueryPackageManagers(unsigned numChannels, const char *querySet)
    {
        // Called from reload inside write lock block
        unsubscribe();
        // We want to kill the old packages, but not until we have created the new ones (i.e. at the end of this function)
        // So that the query/dll caching will work for anything that is not affected by the changes
        CIArrayOf<CRoxieQueryPackageManager> oldQueryPackages;
        appendArray(oldQueryPackages, allQueryPackages);
        allQueryPackages.kill();
        stateHash = 0;

        Owned<IDaliPackageWatcher> notifier = daliHelper->getPackageSetsSubscription(this);
        if (notifier)
            notifiers.append(*notifier.getClear());
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
                        bool isActive = pm.getPropBool("@active", true);
                        if (traceLevel)
                            DBGLOG("Loading package map %s, active %s", packageMapId, isActive ? "true" : "false");
                        try
                        {
                            Owned<CPackageMap> packageMap = new CPackageMap(packageMapId, isActive);
                            Owned<IPropertyTree> xml = daliHelper->getPackageMap(packageMapId);
                            packageMap->load(xml);
                            createQueryPackageManager(numChannels, packageMap.getLink(), querySet);
                            notifiers.append(*daliHelper->getPackageMapSubscription(packageMapId, this));
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
        if (!allQueryPackages.length())
        {
            if (traceLevel)
                DBGLOG("Loading empty package for QuerySet %s", querySet);
            createQueryPackageManager(numChannels, LINK(&queryEmptyPackageMap()), querySet);
        }
        if (traceLevel)
            DBGLOG("Loaded packages");
    }

    void loadStandaloneQuery(const IQueryDll *standAloneDll, unsigned numChannels, const char *querySet)
    {
        // Called from reload inside write lock block
        Owned<IPropertyTree> standAloneDllTree;
        standAloneDllTree.setown(createPTree("Query"));
        standAloneDllTree->setProp("@id", "roxie");
        standAloneDllTree->setProp("@dll", standAloneDll->queryDll()->queryName());
        Owned<CRoxieQueryPackageManager> qpm = new CStandaloneQueryPackageManager(numChannels, querySet, LINK(&queryEmptyPackageMap()), standAloneDllTree.getClear());
        qpm->load();
        stateHash = rtlHash64Data(sizeof(stateHash), &stateHash, qpm->getHash());
        allQueryPackages.append(*qpm.getClear());
    }

    void unsubscribe()
    {
        ForEachItemIn(idx, notifiers)
        {
            daliHelper->releaseSubscription(&notifiers.item(idx));
        }
        notifiers.kill();
    }
};

extern IRoxieQueryPackageManagerSet *createRoxiePackageSetManager(const IQueryDll *standAloneDll)
{
    return new CRoxiePackageSetManager(standAloneDll);
}

IRoxieQueryPackageManagerSet *globalPackageSetManager = NULL;

extern void loadPlugins()
{
    if (pluginDirectory.length() && isDirectory(pluginDirectory.str()))
    {
        plugins = new SafePluginMap(&PluginCtx, traceLevel >= 1);
        plugins->loadFromDirectory(pluginDirectory);
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
            const char *type = e1.queryProp("@type");
            if (type)
            {
                IPropertyTree *e2 = s2->queryPropTree(xpath.str());
                if (e2)
                {
                    unsigned __int64 v2 = e2->getPropInt64("@value", 0);
                    if (strcmp(name, "max")==0)
                    {
                        if (v2 > v1)
                            e1.setPropInt64("@value", v2);
                    }
                    else if (strcmp(type, "min")==0)
                    {
                        if (v2 < v1)
                            e1.setPropInt64("@value", v2);
                    }
                    else if (strcmp(type, "sum")==0)
                        e1.setPropInt64("@value", v1+v2);
                    else
                        throw MakeStringException(ROXIE_UNKNOWN_QUERY, "Unknown type %s in graph statistics", type);
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
    {"Graph", "@id", mergeStats},
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
    Owned<IPropertyTreeIterator> elems = s1->getElements(mi.element);
    ForEach(*elems)
    {
        IPropertyTree &e1 = elems->query();
        StringBuffer xpath;
        if (mi.attribute)
            xpath.appendf("%s[%s='%s']", mi.element, mi.attribute, e1.queryProp(mi.attribute));
        else
            xpath.append(mi.element);
        IPropertyTree *e2 = s2->queryPropTree(xpath.str());
        if (e2)
        {
            mi.f(&e1, e2, level+1);
            s2->removeTree(e2);
        }
    }
    elems.setown(s2->getElements(mi.element));
    ForEach(*elems)
    {
        IPropertyTree &e2 = elems->query();
        s1->addPropTree(mi.element, LINK(&e2));
    }
}

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

static const char *g1 =
        "<Stats>"
        "<Query id='stats'>"
        "<Graph id='graph1'>"
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
              "    <att name='seeks' value='15' type='sum'/>"
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
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='count' value='3' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_1' source='5' target='7'>"
               "<att name='_sourceIndex' value='1'/>"
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Query>"
        "</Stats>";
static const char *g2 =
        "<Stats>"
        "<Query id='stats'>"
        "<Graph id='graph1'>"
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
              "    <att name='seeks' value='25' type='sum'/>"
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
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='count' value='3' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Query>"
        "</Stats>";
static const char *expected =
        "<Stats>"
        "<Query id='stats'>"
        "<Graph id='graph1'>"
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
              "    <att name='seeks' type='sum' value='40'/>"
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
               "<att name='count' value='30' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='3_0' source='3' target='5'>"
               "<att name='count' value='30' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_0' source='5' target='6'>"
               "<att name='count' value='6' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
              "<edge id='5_1' source='5' target='7'>"
               "<att name='_sourceIndex' value='1'/>"
               "<att name='count' value='15' type='sum'/>"
               "<att name='started' value='1'/>"
               "<att name='stopped' value='1'/>"
              "</edge>"
             "</graph>"
            "</att>"
           "</node>"
          "</graph>"
         "</xgmml>"
        "</Graph>"
        "</Query>"
        "</Stats>"
        ;

class MergeStatsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( MergeStatsTest );
        CPPUNIT_TEST(test1);
    CPPUNIT_TEST_SUITE_END();

protected:
    void test1()
    {
        Owned<IPropertyTree> p1 = createPTreeFromXMLString(g1);
        Owned<IPropertyTree> p2 = createPTreeFromXMLString(g2);
        Owned<IPropertyTree> e = createPTreeFromXMLString(expected);
        mergeStats(p1, p2, 0);
        StringBuffer s1, s2;
        toXML(p1, s1);
        toXML(e, s2);
        CPPUNIT_ASSERT(strcmp(s1, s2)==0);
    }

};


CPPUNIT_TEST_SUITE_REGISTRATION( MergeStatsTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( MergeStatsTest, "MergeStatsTest" );

#endif

