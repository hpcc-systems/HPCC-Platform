/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "platform.h"
#include <array>
#include <limits>
#include <string>
#include <unordered_map>

#include "jplane.hpp"
#include "jmutex.hpp"
#include "jptree.hpp"
#include "jfile.hpp"
#include "jsecrets.hpp"
#include "jlog.hpp"
#include "jerror.hpp"

//------------------------------------------------------------------------------------------------------------

// Default blocked IO sizes for containerized reads (in KB)
constexpr size32_t defaultIndexSequentialBlockedIOSizeK = 4 * 1024;  // 4MB
constexpr size32_t defaultIndexRandomBlockedIOSizeK = 64;            // 64KB

// Cache/update plane attributes settings
static unsigned jPlaneHookId = 0;

// Declare the array with an anonymous struct
enum PlaneAttrType { boolean, integer };
struct PlaneAttributeInfo
{
    PlaneAttrType type;
    size32_t scale;
    bool isExpert;
    const char *name;
};
static const std::array<PlaneAttributeInfo, PlaneAttributeCount> planeAttributeInfo = {{
    { PlaneAttrType::integer, 1024, false, "blockedFileIOKB" },   // enum PlaneAttributeType::BlockedSequentialIO    {0}
    { PlaneAttrType::integer, 1024, false, "blockedRandomIOKB" }, // enum PlaneAttributeType::BlockedRandomIOKB      {1}
    { PlaneAttrType::boolean, 0, true, "fileSyncWriteClose" },    // enum PlaneAttributeType::FileSyncWriteClose     {2}
    { PlaneAttrType::boolean, 0, true, "concurrentWriteSupport" },// enum PlaneAttributeType::ConcurrentWriteSupport {3}
    { PlaneAttrType::integer, 1, false, "writeSyncMarginMs" },    // enum PlaneAttributeType::WriteSyncMarginMs      {4}
    { PlaneAttrType::boolean, 0, true, "renameSupported" },       // enum PlaneAttributeType::RenameSupported        {5}
}};

static constexpr unsigned __int64 unsetPlaneAttrValue = 0xFFFFFFFF00000000;

const char *getPlaneAttributeString(PlaneAttributeType attr)
{
    assertex(attr < PlaneAttributeCount);
    return planeAttributeInfo[attr].name;
}

//---------------------------------------------------------------------------------------------------------------------

static bool isAccessible(const IPropertyTree * xml)
{
    //Unusual to have components specified, so short-cicuit the common case
    if (!xml->hasProp("components"))
        return true;

    const char * thisComponentName = queryComponentName();
    if (!thisComponentName)
        return false;

    Owned<IPropertyTreeIterator> component = xml->getElements("components");
    ForEach(*component)
    {
        if (strsame(component->query().queryProp(nullptr), thisComponentName))
            return true;
    }
    return false;
}

class CStoragePlaneAlias : public CInterfaceOf<IStoragePlaneAlias>
{
public:
    CStoragePlaneAlias(IPropertyTree *_xml) : xml(_xml)
    {
        Owned<IPropertyTreeIterator> modeIter = xml->getElements("mode");
        ForEach(*modeIter)
        {
            const char *modeStr = modeIter->query().queryProp(nullptr);
            modes |= getAccessModeFromString(modeStr);
        }
        accessible = ::isAccessible(xml);
    }
    virtual AccessMode queryModes() const override { return modes; }
    virtual const char *queryPrefix() const override { return xml->queryProp("@prefix"); }
    virtual bool isAccessible() const override { return accessible; }

private:
    Linked<IPropertyTree> xml;
    AccessMode modes = AccessMode::none;
    bool accessible = false;
};

class CStorageApiInfo : public CInterfaceOf<IStorageApiInfo>
{
public:
    CStorageApiInfo(const IPropertyTree * _xml) : xml(_xml)
    {
        if (!xml) // shouldn't happen
            throw makeStringException(MSGAUD_programmer, -1, "Invalid call: CStorageApiInfo(nullptr)");
    }
    virtual const char * getStorageType() const override
    {
        return xml->queryProp("@type");
    }
    virtual const char * queryStorageApiAccount(unsigned stripeNumber) const override
    {
        const char *account = queryContainer(stripeNumber)->queryProp("@account");
        if (isEmptyString(account))
            account = xml->queryProp("@account");
        return account;
    }
    virtual const char * queryStorageContainerName(unsigned stripeNumber) const override
    {
        return queryContainer(stripeNumber)->queryProp("@name");
    }
    virtual StringBuffer & getSASToken(unsigned stripeNumber, StringBuffer & token) const override
    {
        const char * secretName = queryContainer(stripeNumber)->queryProp("@secret");
        if (isEmptyString(secretName))
        {
            secretName = xml->queryProp("@secret");
            if (isEmptyString(secretName))
                return token.clear();  // return empty string if no secret name is specified
        }
        getSecretValue(token, "storage", secretName, "token", false);
        return token.trimRight();
    }
    virtual bool useManagedIdentity() const override
    {
        return xml->getPropBool("@managed", false);
    }

private:
    IPropertyTree * queryContainer(unsigned stripeNumber) const
    {
        if (stripeNumber==0) // stripeNumber==0 when not striped -> use first item in 'containers' list
            stripeNumber++;
        VStringBuffer path("containers[%u]", stripeNumber);
        IPropertyTree *container = xml->queryPropTree(path.str());
        if (!container)
            throw makeStringExceptionV(JLIBERR_UtilNoContainerProvidedPathS, "No container provided: path %s", path.str());
        return container;
    }
    Owned<const IPropertyTree> xml;
};

//------------------------------------------------------------------------------------------------------------

//Unlikely to be contended - so have a single shared static
static CriticalSection isLocalCrit;
class CStoragePlane final : public CInterfaceOf<IStoragePlane>
{
public:
    CStoragePlane(const IPropertyTree & plane, const IPropertyTree & _defaults) : config(&plane), defaults(&_defaults)
    {
        name = plane.queryProp("@name");
        prefix = plane.queryProp("@prefix", "");
        mirrorPrefix = plane.queryProp("@mirrorPrefix", nullptr);
        category = plane.queryProp("@category", "");
        devices = plane.getPropInt("@numDevices", 1);

        for (unsigned propNum=0; propNum<PlaneAttributeType::PlaneAttributeCount; ++propNum)
        {
            const PlaneAttributeInfo &attrInfo = planeAttributeInfo[propNum];
            std::string prop;
            if (attrInfo.isExpert)
                prop += "expert/";
            prop += "@" + std::string(attrInfo.name);
            switch (attrInfo.type)
            {
                case PlaneAttrType::integer:
                {
                    unsigned __int64 value = plane.getPropInt64(prop.c_str(), unsetPlaneAttrValue);
                    if (unsetPlaneAttrValue == value)
                        value = defaults->getPropInt64(prop.c_str(), unsetPlaneAttrValue);

                    if (unsetPlaneAttrValue != value)
                    {
                        if (attrInfo.scale)
                        {
                            dbgassertex(PlaneAttrType::integer == attrInfo.type);
                            value *= attrInfo.scale;
                        }
                    }
                    attributeValues[propNum] = value;
                    break;
                }
                case PlaneAttrType::boolean:
                {
                    unsigned __int64 value;
                    if (plane.hasProp(prop.c_str()))
                        value = plane.getPropBool(prop.c_str()) ? 1 : 0;
                    else if (defaults->hasProp(prop.c_str()))
                        value = defaults->getPropBool(prop.c_str()) ? 1 : 0;
                    else if (FileSyncWriteClose == propNum) // temporary (if FileSyncWriteClose and unset, check legacy fileSyncMaxRetrySecs), purely for short term backward compatibility (see HPCC-32757)
                    {
                        unsigned __int64 v = plane.getPropInt64("expert/@fileSyncMaxRetrySecs", unsetPlaneAttrValue);
                        // NB: fileSyncMaxRetrySecs==0 is treated as set/enabled
                        if (unsetPlaneAttrValue != v)
                            value = 1;
                        else
                            value = unsetPlaneAttrValue;
                    }
                    else
                        value = unsetPlaneAttrValue;
                    attributeValues[propNum] = value;
                    break;
                }
                default:
                    throwUnexpected();
            }
        }

        Owned<IPropertyTreeIterator> srcAliases = plane.getElements("aliases");
        ForEach(*srcAliases)
            aliases.push_back(new CStoragePlaneAlias(&srcAliases->query()));

        StringArray planeHosts;
        getPlaneHosts(planeHosts, config);
        ForEachItemIn(h, planeHosts)
            hosts.emplace_back(planeHosts.item(h));

        compression.set(config->queryProp("@compression", defaults->queryProp("@compression")));

        bool defaultCompressed = defaults->getPropBool("@compressLogicalFiles");
        compressed = compression || config->getPropBool("@compressLogicalFiles", defaultCompressed);
    }

   virtual const char * queryName() const override { return name.c_str(); }

   virtual const char * queryPrefix() const override { return prefix.c_str(); }

    virtual const char * queryMirrorPrefix() const override
    {
        if (mirrorPrefix)
            return mirrorPrefix.str();
        return prefix.c_str();
    }

    virtual unsigned numDevices() const override { return devices; }

    virtual const std::vector<std::string> &queryHosts() const override
    {
        return hosts;
    }

    virtual bool isAnyDeviceLocal() const override
    {
        CriticalBlock b(isLocalCrit);
        if (cachedLocalPlane)
            return isLocal;

        isLocal = false;
        if (hosts.size()== 0)
        {
            isLocal = true;
        }
        else
        {
            const char * myHostName = GetCachedHostName();
            for (auto &host: hosts)
            {
                if (strsame(host.c_str(), ".") || strsame(host.c_str(), "localhost"))
                {
                    isLocal = true;
                    break;
                }

                if (strisame(myHostName, host.c_str()))
                {
                    isLocal = true;
                    break;
                }
            }
        }
        cachedLocalPlane = true;
        return isLocal;
    }

    virtual unsigned numDefaultSprayParts() const override { return config->getPropInt("@defaultSprayParts", 1); }

    virtual bool queryDirPerPart() const override { return config->getPropBool("@subDirPerFilePart", isContainerized()); } // default to dir. per part in containerized mode

    virtual unsigned queryNumStripes() const override
    {
        if (!isStriped())
            return 1;

        return numDevices();
    }

    virtual IStoragePlaneAlias *getAliasMatch(AccessMode desiredModes) const override
    {
        if (AccessMode::none == desiredModes)
            return nullptr;
        // go through and return one with most mode matches (should there be any other weighting?)
        unsigned bestScore = 0;
        IStoragePlaneAlias *bestMatch = nullptr;
        for (const auto & alias : aliases)
        {
            // Some aliases are only mounted in a restricted set of components (to avoid limits on the number of connections)
            if (!alias->isAccessible())
                continue;

            AccessMode aliasModes = alias->queryModes();
            unsigned match = static_cast<unsigned>(aliasModes & desiredModes);
            unsigned score = 0;
            while (match)
            {
                score += match & 1;
                match >>= 1;
            }
            if (score > bestScore)
            {
                bestScore = score;
                bestMatch = alias;
            }
        }
        return LINK(bestMatch);
    }
    virtual IStorageApiInfo *getStorageApiInfo() const
    {
        IPropertyTree *apiInfo = config->getPropTree("storageapi");
        if (apiInfo)
            return new CStorageApiInfo(apiInfo);
        return nullptr;
    }

    virtual bool isAccessible() const override
    {
        return ::isAccessible(config);
    }

    virtual bool isStriped() const
    {
        //For bare metal systems, striped containers need to be explicitly configured.
        if (config->hasProp("@hostGroup") || !isContainerized())
        {
            if (!config->getPropBool("@striped"))
               return false;
        }

        return devices > 1;
    }

    virtual unsigned __int64 getAttribute(PlaneAttributeType attr, unsigned __int64 defaultValue) const override
    {
        assertex(attr < PlaneAttributeCount);
        unsigned __int64  value = attributeValues[attr];
        if (value == unsetPlaneAttrValue)
            return defaultValue;
        return value;
    }

    virtual const IPropertyTree * queryConfig() const { return config; }

    const char * queryCategory() const { return category.c_str(); }

    virtual bool compressOnWrite() const override
    {
        return compressed;
    }

    virtual const char * queryCompression() const
    {
        return compression;
    }

    virtual unsigned queryDefaultCopies() const override
    {
        return config->getPropInt("@redundancy", 0) + 1;
    }

private:
    std::string name;
    std::string prefix;
    StringAttr mirrorPrefix; // can be null
    std::string category;
    StringAttr compression;
    unsigned devices{1};
    std::array<unsigned __int64, PlaneAttributeCount> attributeValues;
    Linked<const IPropertyTree> config;
    Linked<const IPropertyTree> defaults;
    std::vector<Owned<IStoragePlaneAlias>> aliases;
    std::vector<std::string> hosts;
    mutable bool cachedLocalPlane{false};
    mutable bool isLocal{false};
    bool compressed{false};
};

// {prefix, {key1: value1, key2: value2, ...}}
static std::unordered_map<std::string, Owned<CStoragePlane>> storagePlaneMap;
static CriticalSection storagePlaneMapCrit;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    auto updateFunc = [&](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
    {
        CriticalBlock b(storagePlaneMapCrit);
        storagePlaneMap.clear();

        Owned<IPropertyTree> storage = getGlobalConfigSP()->getPropTree("storage");
        // This may be null if running standalone (e.g. eclcc), or in a component that does not have access to the configuration.
        if (!storage)
            return;

        Owned<IPropertyTree> defaults = storage->getPropTree("defaults");
        if (!defaults)
            defaults.setown(createPTree("defaults"));
        Owned<IPropertyTreeIterator> planesIter = storage->getElements("planes");
        ForEach(*planesIter)
        {
            const IPropertyTree &plane = planesIter->query();
            const char * name = plane.queryProp("@name");
            storagePlaneMap[name].setown(new CStoragePlane(plane, *defaults));
        }
    };

    jPlaneHookId = installConfigUpdateHook(updateFunc, true);
    return true;
}

MODULE_EXIT()
{
    removeConfigUpdateHook(jPlaneHookId);
}


static bool isPathInPrefix(const char * prefix, const char *path)
{
    if (isEmptyString(prefix))
        return false; //prefix is empty, path is not - can't match.

    while (*prefix && *prefix == *path)
    {
        path++;
        prefix++;
    }

    if (0 == *prefix)
    {
        if (0 == *path || isPathSepChar(*path))
            return true;
        if (isPathSepChar(*(path - 1))) //implies both last characters of prefix and path were '/'
            return true;
    }
    else if (0 == *path && isPathSepChar(*prefix) && (0 == *(prefix + 1)))
        return true;
    return false;
}

// The following static functions must be called with the storagePlaneMapCrit held

// Find the storage plane that best matches the path - it is possible on some pathological configurations
// that a drop zone is configured with a prefix of '/'.  To avoid this, check all paths and return the
// longest match.
static const CStoragePlane * doFindStoragePlaneFromPath(const char * path, bool required)
{
    size_t bestLength = 0;
    const CStoragePlane *bestMatch = nullptr;
    for (auto &e: storagePlaneMap)
    {
        CStoragePlane *plane = e.second;
        const char *prefix = plane->queryPrefix();
        if (isPathInPrefix(prefix, path))
        {
            //Both bare metal and containerized systems can have planes with host names.
            auto & hosts = plane->queryHosts();
            if (hosts.size() != 0)
            {
                bool hasMatchingHost = plane->isAnyDeviceLocal();
                if (!hasMatchingHost)
                {
                    // MORE: Check if the filename is //<host>/path
                    // HPCC-34519
                }

                if (!hasMatchingHost)
                    continue;
            }

            //Some pathological configurations have '/' and '/x/y' as mount paths - pick the most restrictive
            size_t len = strlen(prefix);
            if (!bestMatch || len > bestLength)
            {
                bestMatch = plane;
                bestLength = len;
            }
        }
    }

    if (bestMatch)
        return bestMatch;

    if (required)
        throw makeStringExceptionV(JLIBERR_UtilCouldNotMapFilenameToStoragePlane, "Could not map filename to storage plane %s", path);

    return nullptr;
}

static const CStoragePlane * doFindStoragePlaneByName(const char * name, bool required)
{
    if (!isEmptyString(name))
    {
        auto it = storagePlaneMap.find(name);
        if (it != storagePlaneMap.end())
            return it->second;
    }

    if (required)
        throw makeStringExceptionV(JLIBERR_UtilUnknownStoragePlaneS, "Unknown storage plane '%s'", name);

    return nullptr;
}

//------------------------------------------------------------------------------------------------------------

IPropertyTree * getHostGroup(const char * name, bool required)
{
    if (!isEmptyString(name))
    {
        VStringBuffer xpath("storage/hostGroups[@name='%s']", name);
        Owned<IPropertyTree> global = getGlobalConfig();
        IPropertyTree * match = global->getPropTree(xpath);
        if (match)
            return match;
    }
    if (required)
        throw makeStringExceptionV(JLIBERR_UtilNoEntryFoundForHostgroupS, "No entry found for hostGroup: '%s'", name ? name : "<null>");
    return nullptr;
}

IPropertyTree * getRemoteStorage(const char * name)
{
    VStringBuffer xpath("storage/remote[@name='%s']", name);
    Owned<IPropertyTree> global = getGlobalConfig();
    return global->getPropTree(xpath);
}

IPropertyTreeIterator * getRemoteStoragesIterator()
{
    return getGlobalConfigSP()->getElements("storage/remote");
}

//------------------------------------------------------------------------------------------------------------

const IStoragePlane * getStoragePlaneByName(const char * name, bool required)
{
    if (isEmptyString(name))
        return nullptr;

    CriticalBlock b(storagePlaneMapCrit);
    const CStoragePlane *e = doFindStoragePlaneByName(name, required);
    if (!e)
        return nullptr;

    LINK(e);
    return e;
}

const IStoragePlane * getStoragePlaneFromPath(const char *filePath, bool required)
{
    CriticalBlock b(storagePlaneMapCrit);
    const CStoragePlane *e = doFindStoragePlaneFromPath(filePath, required);
    if (!e)
        return nullptr;

    LINK(e);
    return e;
}

//MORE: Revisit every call to this function to see if it can be replaced with a more specialized call
//This will not support inheriting values from the defaults.
const IPropertyTree * getStoragePlaneConfig(const char * name, bool required)
{
    if (isEmptyString(name))
        return nullptr;

    CriticalBlock b(storagePlaneMapCrit);
    auto it = storagePlaneMap.find(name);
    if (it != storagePlaneMap.end())
        return LINK(it->second->queryConfig());

    if (required)
        throw makeStringExceptionV(JLIBERR_UtilUnknownStoragePlaneS_1, "Unknown storage plane %s", name);
    return nullptr;
}

//MORE: Revisit every call to this function to see if it can be replaced
IPropertyTreeIterator * getPlanesIterator(const char * category, const char *name)
{
    StringBuffer xpath("storage/planes");
    if (!isEmptyString(category))
        xpath.appendf("[@category='%s']", category);
    if (!isEmptyString(name))
        xpath.appendf("[@name='%s']", name);
    return getGlobalConfigSP()->getElements(xpath);
}

unsigned __int64 getPlaneAttributeValue(const char *planeName, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue)
{
    if (!planeName)
        return defaultValue;
    assertex(planeAttrType < PlaneAttributeCount);
    CriticalBlock b(storagePlaneMapCrit);
    auto it = storagePlaneMap.find(planeName);
    if (it != storagePlaneMap.end())
        return it->second->getAttribute(planeAttrType, defaultValue);

    return defaultValue;
}

const char *findPlaneFromPath(const char *filePath, StringBuffer &result)
{
    CriticalBlock b(storagePlaneMapCrit);
    const CStoragePlane *e = doFindStoragePlaneFromPath(filePath, false);
    if (!e)
        return nullptr;

    result.append(e->queryName());
    return result;
}


bool findPlaneAttrFromPath(const char *filePath, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue, unsigned __int64 &resultValue)
{
    CriticalBlock b(storagePlaneMapCrit);
    const CStoragePlane *e = doFindStoragePlaneFromPath(filePath, false);
    if (e)
    {
        resultValue = e->getAttribute(planeAttrType, defaultValue);
        return true;
    }
    return false;
}

size32_t getBlockedFileIOSize(const char *planeName, size32_t defaultSize)
{
    return (size32_t)getPlaneAttributeValue(planeName, BlockedSequentialIO, defaultSize);
}

size32_t getBlockedRandomIOSize(const char *planeName, size32_t defaultSize)
{
    return (size32_t)getPlaneAttributeValue(planeName, BlockedRandomIO, defaultSize);
}

size32_t getForeignBlockedIOSize(bool isFiltered)
{
    unsigned foreignBlockedIOSizeK = (unsigned)getExpertOptInt64("foreignSequentialBlockedIOSizeK", (unsigned)-1);
    if (isFiltered)
    {
        foreignBlockedIOSizeK = (unsigned)getExpertOptInt64("foreignRandomBlockedIOSizeK", foreignBlockedIOSizeK);
        if ((unsigned)-1 == foreignBlockedIOSizeK)
            foreignBlockedIOSizeK = isContainerized() ? defaultIndexRandomBlockedIOSizeK : 0; // default to 64k random block size for filtered foreign reads on containerized environment
    }
    else
    {
        if ((unsigned)-1 == foreignBlockedIOSizeK)
            foreignBlockedIOSizeK = isContainerized() ? defaultIndexSequentialBlockedIOSizeK : 0; // default to 4MB sequential block size for unfiltered foreign reads on containerized environment
    }
    verifyex(foreignBlockedIOSizeK <= ((size32_t)-1) / 1024);
    return foreignBlockedIOSizeK * 1024;
}

// This is the block size to use for index reads, conditional on whether reading with/without filtered. If filtered, use the random IO size, otherwise the sequential IO size.
size32_t getIndexBlockedIOSize(const char *planeName, bool isFiltered)
{
    static CriticalSection missingBlockedIOWarnCrit;
    static std::unordered_map<std::string, unsigned> warnedMissingBlockedIOByPlane;

    // If unfiltered, use the sequential block size if defined in the plane, or component config.
    size32_t blockedIOSize = getBlockedFileIOSize(planeName, (size32_t)-1);
    if (isFiltered)
        blockedIOSize = getBlockedRandomIOSize(planeName, blockedIOSize);

    if ((size32_t)-1 == blockedIOSize)
    {
        if (isContainerized())
        {
            size32_t blockedIOSizeK = isFiltered ? defaultIndexRandomBlockedIOSizeK : defaultIndexSequentialBlockedIOSizeK;

            if (!isEmptyString(planeName))
            {
                // Warn once per plane and mode (random/sequential) to avoid flooding logs.
                bool shouldWarn = false;
                unsigned warnMask = isFiltered ? 0x01 : 0x02;
                {
                    CriticalBlock b(missingBlockedIOWarnCrit);
                    unsigned &warnedMask = warnedMissingBlockedIOByPlane[planeName];
                    if (0 == (warnedMask & warnMask))
                    {
                        warnedMask |= warnMask;
                        shouldWarn = true;
                    }
                }
                if (shouldWarn)
                    OWARNLOG("No %s blocked IO size configured for storage plane '%s' in containerized mode; using default %u KB. Configure blockedFileIOKB/blockedRandomIOKB to avoid potential performance issues", isFiltered ? "random" : "sequential", planeName, blockedIOSizeK);
            }
            blockedIOSize = blockedIOSizeK * 1024;
        }
        else
            blockedIOSize = 0; // caller should interpret as use default
    }
    return blockedIOSize;
}

//------------------------------------------------------------------------------------------------------------
// Write-sync visibility cache (see jplane.hpp)
//
// The write-sync visibility cache tracks recently published file parts that may not yet be visible
// to all nodes in the cluster due to eventual consistency in the underlying storage plane.
// Keyed by the part's full physical path, it records a visibility deadline (publish @modified + margin),
// published on-disk size, and compression state. This lets jfile transparently retry physical access
// to a part that is not yet visible (e.g., cross-AZ blobnfs propagation lag), and also wait for its
// physical size to reach the published size (cross-AZ propagation can briefly expose a partial file),
// without delaying access to parts that are not being tracked.
//
// Reader concurrency design:
// This cache is on the hot path of physical file access (every safeStat / open of a part). The vast
// majority of these are for paths that are not tracked here at all, so it is designed to stay as
// close to lock-free as possible:
//  - The common case (cache empty) returns immediately on a single relaxed-acquire atomic load, taking no
//    lock whatsoever - so unrelated, non-plane file accesses pay almost nothing.
//  - When entries do exist, the only lock taken is the briefest possible: a single Link() of the current
//    immutable snapshot. All actual work (hash probe and checks) then runs lock-free on this thread's
//    private snapshot reference, never contending with other readers or with a writer.
//
// The snapshot (copy-on-publish) design enables this: readers never mutate shared state during a lookup
// (with one exception: an opportunistic, guarded clear of an entirely-expired snapshot to restore the
// lock-free fast path). Rebuilding the map on each write is deliberately pushed entirely onto the (rare) writer.
//
// Ordering guarantee:
// When a part IS tracked here, the writer (noteWriteSyncFiles) will have fully committed and published
// the snapshot well before any reader looks the part up. The write happens when the IFileDescriptor is
// created (typically on Dali lookup or worker deserialization), and reads happen later when something
// opens the part. There is no race where a just-written part is missed by a reader; it is already
// in the published snapshot.
//
// Cache pruning / eviction:
// Entries are given a visibility deadline. To prevent unbounded growth, the writer (noteWriteSyncFiles)
// performs a sweep on every write, purging any entries from the canonical map that have passed their
// deadline. This bounds the cache to only those parts recently published and still within their margin.
// Additionally, if the cache becomes quiet, the first reader to notice that the entirely published snapshot
// has expired will opportunistically clear it, aggressively returning the system to the lock-free fast path.


// The set/get methods below provide a global tuning and debugging aide to adjust
// the plane's write-sync margin, allowing operators to extend delays or force tracing
// without changing the plane configuration.
static constexpr int noWriteSyncMarginDeltaMs = std::numeric_limits<int>::min();
static std::atomic<int> writeSyncMarginDeltaMs{noWriteSyncMarginDeltaMs};

void setWriteSyncMarginDeltaMs(int deltaMs)
{
    writeSyncMarginDeltaMs = deltaMs;
}

// Returns true if a write-sync margin delta has been explicitly configured,
// even if that value is zero. If configured, it triggers tracing regardless
// of the margin delta value.
bool getWriteSyncMarginDeltaMs(int &deltaMs)
{
    int value = writeSyncMarginDeltaMs;
    if (noWriteSyncMarginDeltaMs == value)
    {
        deltaMs = 0;
        return false;
    }
    deltaMs = value;
    return true;
}

// {part physical path -> visibility deadline + published on-disk size for a recently published file part}.
// The full path the reader will open is stored, so lookups are a direct hash match with no key derivation.
struct WriteSyncEntry
{
    time_t deadline = 0;                 // epoch secs by which the part should be visible to all readers
    offset_t expectedSize = (offset_t)-1; // published on-disk size to validate against ((offset_t)-1 => unknown / do not validate)
    bool compressed = false;             // part is compressed - affects how an empty (size 0) part is validated
};

class CWriteSyncSnapshot : public CInterfaceOf<IInterface>
{
public:
    CWriteSyncSnapshot(const std::unordered_map<std::string, WriteSyncEntry> &sourceMap)
    {
        entries.reserve(sourceMap.size());
        for (const auto &entry : sourceMap)
        {
            entries.emplace(entry.first, entry.second);
            if (entry.second.deadline > latestDeadline)
                latestDeadline = entry.second.deadline;
        }
    }

    inline bool isExpired(time_t now) const
    {
        return now >= latestDeadline;
    }

    unsigned getDelayRemainingMs(const char *physicalPath, time_t now, offset_t &expectedSize, bool &compressed) const
    {
        auto it = entries.find(physicalPath);
        if (it == entries.end())
            return 0;
        if (now >= it->second.deadline) // expired - the part should now be visible
            return 0;
        expectedSize = it->second.expectedSize;
        compressed = it->second.compressed;
        unsigned __int64 remainingMs = (unsigned __int64)(it->second.deadline - now) * 1000;
        if (remainingMs > 0xFFFFFFFFU)
            remainingMs = 0xFFFFFFFFU;
        return (unsigned)remainingMs;
    }

    inline size_t size() const
    {
        return entries.size();
    }

    std::unordered_map<std::string, WriteSyncEntry> entries;
    time_t latestDeadline = 0; // latest deadline across the snapshot; if expired, every entry is expired
};

// Canonical mutable map. Only ever accessed by writers while holding writeSyncWriteCrit; readers never touch it.
static std::unordered_map<std::string, WriteSyncEntry> writeSyncDeadlineMap;

static CriticalSection writeSyncWriteCrit; // serializes writers and guards writeSyncDeadlineMap + the snapshot build

// Published immutable snapshot. Swapping this pointer is the only thing that contends with readers, so
// writeSyncDeadlineCrit is held for the minimum possible time - just the swap, never the rebuild.
static Owned<const CWriteSyncSnapshot> writeSyncDeadlineSnapshot;
static CriticalSection writeSyncDeadlineCrit;

// Lock-free fast path: the cache is almost always empty, so readers can skip taking the lock entirely.
static std::atomic<size_t> writeSyncDeadlineCount{0};

// Called while holding writeSyncWriteCrit. Builds the new snapshot from the canonical map outside the reader
// lock, then takes writeSyncDeadlineCrit only to swap in the new pointer - readers are never blocked while the
// (potentially large) snapshot is copied, only for the single pointer swap.
static void publishWriteSyncSnapshot()
{
    Owned<CWriteSyncSnapshot> nextSnapshot;
    size_t count = 0;
    if (!writeSyncDeadlineMap.empty())
    {
        nextSnapshot.setown(new CWriteSyncSnapshot(writeSyncDeadlineMap));
        count = nextSnapshot->size();
    }

    // Minimal critical section: swap the published pointer and update the lock-free count together.
    CriticalBlock b(writeSyncDeadlineCrit);
    writeSyncDeadlineSnapshot.setown(nextSnapshot.getClear());
    writeSyncDeadlineCount.store(count, std::memory_order_release);
}

#ifdef _USE_CPPUNIT
void resetWriteSyncStateForTest()
{
    CriticalBlock b(writeSyncWriteCrit);
    writeSyncDeadlineMap.clear();
    publishWriteSyncSnapshot();
    writeSyncMarginDeltaMs = noWriteSyncMarginDeltaMs;
}
#endif

void noteWriteSyncFiles(const std::vector<WriteSyncFileInfo> &files, time_t deadline)
{
    time_t now = time(nullptr);
    if (now >= deadline) // already past the visibility margin - nothing to track
        return;

    int marginDeltaMs = 0;
    bool forceTrace = getWriteSyncMarginDeltaMs(marginDeltaMs);

    CriticalBlock b(writeSyncWriteCrit); // serialize writers; readers are unaffected by this lock

    // prune entries that have passed their deadline once for the whole batch, keeping the map bounded to
    // recently published parts
    for (auto it = writeSyncDeadlineMap.begin(); it != writeSyncDeadlineMap.end(); )
    {
        if (it->second.deadline <= now)
            it = writeSyncDeadlineMap.erase(it);
        else
            ++it;
    }

    for (const WriteSyncFileInfo &file : files)
    {
        auto [it, isNewEntry] = writeSyncDeadlineMap.try_emplace(file.path);
        WriteSyncEntry &existing = it->second;
        existing.deadline = deadline;
        // (offset_t)-1 means the published on-disk size is unknown (no meta size) and is carried through as-is
        // so the reader skips size validation; any other value (including 0) is a real size to validate against.
        existing.expectedSize = file.expectedSize;
        existing.compressed = file.compressed;
        if (forceTrace)
        {
            WARNLOG("noteWriteSyncFiles: %s write-sync entry (marginDeltaMs=%d): path=%s, deadline=%lld, expectedSize=%lld, compressed=%s",
                isNewEntry ? "added" : "updated", marginDeltaMs, file.path.c_str(), (long long)deadline, (long long)file.expectedSize, file.compressed ? "true" : "false");
        }
    }
    publishWriteSyncSnapshot();
}

unsigned getPathWriteSyncDelayRemainingMs(const char *physicalPath, offset_t &expectedSize, bool &compressed)
{
    expectedSize = (offset_t)-1; // unknown / do not validate unless a tracked entry says otherwise
    compressed = false;
    if (0 == writeSyncDeadlineCount.load(std::memory_order_acquire)) // common case - nothing being tracked (no lock)
        return 0;
    if (isEmptyString(physicalPath))
        return 0;

    // Take a linked reference to the current snapshot under the lock - this is only held for the
    // duration of a single link (refcount bump), the map probe below runs lock-free on our private copy.
    Owned<const CWriteSyncSnapshot> snapshot;
    time_t now = time(nullptr);
    {
        CriticalBlock b(writeSyncDeadlineCrit);
        snapshot.set(writeSyncDeadlineSnapshot);
        if (!snapshot)
            return 0;
        if (snapshot->isExpired(now)) // every entry in the published snapshot has expired
        {
            // All tracked parts have expired. Opportunistically clear the published snapshot so subsequent
            // readers return on the lock-free fast path again, rather than continuing to take writeSyncDeadlineCrit
            // until the next writer prunes and republishes. Guarded so we never discard a newer snapshot a writer
            // may have just swapped in: only clear if the currently published snapshot is still this expired one.
            if (writeSyncDeadlineSnapshot.get() == snapshot.get())
            {
                writeSyncDeadlineSnapshot.clear();
                writeSyncDeadlineCount.store(0, std::memory_order_release);
            }
            return 0;
        }
    }
    return snapshot->getDelayRemainingMs(physicalPath, now, expectedSize, compressed);
}

static std::atomic<int> avoidRename{-1};
static CriticalSection avoidRenameCS;
// returns true if configured and should use 'result'
static bool checkComponentAndGlobalAvoidRename(bool &result)
{
    if (-1 == avoidRename) // 1st time
    {
        // NB: wouldn't update if config changed, but not sure I care (could add hook if really wanted to)
        CriticalBlock b(avoidRenameCS);
        if (-1 == avoidRename)
        {
            int v = getConfigInt("expert/@avoidRename", -1);
            if (-1 != v) // i.e. is there a setting at all
                avoidRename = v>0;
            else
                avoidRename = -2; // suppress further checks
        }
    }
    if (-2 == avoidRename)
        return false;
    result = avoidRename > 0;
    return true;
}

bool getRenameSupportedFromPath(const char *filePath) // NB: no default, let the plane type determine the default
{
    if (isUrl(filePath))
        return false;

    // check plane property first
    Linked<const CStoragePlane> plane;
    {
        CriticalBlock b(storagePlaneMapCrit);
        plane.set(doFindStoragePlaneFromPath(filePath, false));
    }

    return getRenameSupportedFromPlane(plane);
}


bool getRenameSupportedFromPlane(const IStoragePlane * plane) // NB: no default, let the plane type determine the default
{
    if (plane)
    {
        // return if configured
        unsigned __int64 value = plane->getAttribute(RenameSupported, unsetPlaneAttrValue);
        if (unsetPlaneAttrValue != value)
            return value > 0;
    }

    // handle legacy component or global expert property
    bool result;
    if (checkComponentAndGlobalAvoidRename(result)) // returns true if expert setting configured
        return result;

    if (plane)
    {
        // In the absence of plane configuration (or component/global expert setting)
        // we assume that any plane backed by a pvc or storageapi does not support rename
        if (plane->queryConfig()->hasProp("@pvc") || plane->queryConfig()->hasProp("storageapi"))
            return false;
    }

    // if none of the above, we assume rename is supported
    return true;
}

//------------------------------------------------------------------------------------------------------------

bool getDefaultStoragePlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getDefaultPlane(ret, "@dataPlane", "data"))
        return true;

    throwUnexpectedX("Default data plane not specified"); // The default should always have been configured by the helm charts
}

bool getDefaultSpillPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@spillPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@spillPlane", ret))
        return true;
    else if (getDefaultPlane(ret, nullptr, "spill"))
        return true;

    throwUnexpectedX("Default spill plane not specified"); // The default should always have been configured by the helm charts
}

bool getDefaultIndexBuildStoragePlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@indexBuildPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@indexBuildPlane", ret))
        return true;
    else
        return getDefaultStoragePlane(ret);
}

bool getDefaultPersistPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@persistPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@persistPlane", ret))
        return true;
    else
        return getDefaultStoragePlane(ret);
}

bool getDefaultJobTempPlane(StringBuffer &ret)
{
    if (!isContainerized())
        return false;
    if (getComponentConfigSP()->getProp("@jobTempPlane", ret))
        return true;
    else if (getGlobalConfigSP()->getProp("storage/@jobTempPlane", ret))
        return true;
    else
    {
        // NB: In hthor jobtemps are written to the spill plane and hence ephemeral storage by default
        // In Thor they are written to the default data storage plane by default.
        // This is because HThor doesn't need them persisted beyond the lifetime of the process, but Thor does.
        return getDefaultStoragePlane(ret);
    }
}

//---------------------------------------------------------------------------------------------------------------------

bool isPathInPlane(IPropertyTree *plane, const char *path)
{
    if (isEmptyString(path))
        return true;

    const char *prefix = plane->queryProp("@prefix");
    return isPathInPrefix(prefix, path);
}

//---------------------------------------------------------------------------------------------------------------------


static const IPropertyTree *getPlaneHostGroup(const IPropertyTree *plane)
{
    if (plane->hasProp("@hostGroup"))
        return getHostGroup(plane->queryProp("@hostGroup"), true);
    else if (plane->hasProp("hosts"))
        return LINK(plane); // plane itself holds 'hosts'
    return nullptr;
}

unsigned getNumPlaneStripes(const char *clusterName)
{
    Owned<const IStoragePlane> storagePlane = getStoragePlaneByName(clusterName, false);
    if (!storagePlane)
    {
        OWARNLOG("getNumPlaneStripes: Storage plane %s not found", clusterName);
        return 1;
    }

    return storagePlane->queryNumStripes();
}

bool isHostInPlane(IPropertyTree *plane, const char *host, bool ipMatch)
{
    Owned<const IPropertyTree> planeGroup = getPlaneHostGroup(plane);
    if (!planeGroup)
        return false;
    Owned<IPropertyTreeIterator> hostsIter = planeGroup->getElements("hosts");
    SocketEndpoint hostEp;
    if (ipMatch)
        hostEp.set(host);
    ForEach (*hostsIter)
    {
        const char *planeHost = hostsIter->query().queryProp(nullptr);
        if (ipMatch)
        {
            SocketEndpoint planeHostEp(planeHost);
            if (planeHostEp.ipequals(hostEp))
                return true;
        }
        else if (streq(planeHost, host))
            return true;
    }
    return false;
}

bool getPlaneHost(StringBuffer &host, const IPropertyTree *plane, unsigned which)
{
    Owned<const IPropertyTree> hostGroup = getPlaneHostGroup(plane);
    if (!hostGroup)
        return false;

    unsigned maxHosts = hostGroup->getCount("hosts");
    if (which >= maxHosts)
        throw makeStringExceptionV(JLIBERR_UtilGetplanehostIndexUOutOfRange1U, "getPlaneHost: index %u out of range 1..%u", which, maxHosts);
    VStringBuffer xpath("hosts[%u]", which+1); // which is 0 based
    host.append(hostGroup->queryProp(xpath));
    return true;
}

void getPlaneHosts(StringArray &hosts, const IPropertyTree *plane)
{
    Owned<const IPropertyTree> hostGroup = getPlaneHostGroup(plane);
    if (hostGroup)
    {
        Owned<IPropertyTreeIterator> hostsIter = hostGroup->getElements("hosts");
        ForEach (*hostsIter)
            hosts.append(hostsIter->query().queryProp(nullptr));
    }
}

//MORE: This could be cached
static const IStoragePlane * getStoragePlane(const char * name, const std::vector<std::string> &categories, bool required)
{
    CriticalBlock b(storagePlaneMapCrit);
    const CStoragePlane * match = doFindStoragePlaneByName(name, required);
    if (!match)
        return nullptr;

    const char * category = match->queryCategory();
    auto r = std::find(categories.begin(), categories.end(), category);
    if (r == categories.end())
    {
        if (required)
            throw makeStringExceptionV(JLIBERR_UtilStoragePlaneSDoesNotMatchRequest, "storage plane '%s' does not match request categories (plane category=%s)", name, category);
        return nullptr;
    }

    const IStoragePlane * result = match;
    return LINK(result);
}

static void getStoragePlanes(StoragePlaneArray & planes, const std::vector<std::string> &categories)
{
    CriticalBlock b(storagePlaneMapCrit);
    for (auto &e: storagePlaneMap)
    {
        const CStoragePlane * plane = e.second;
        const char * category = plane->queryCategory();
        auto r = std::find(categories.begin(), categories.end(), category);
        if (r != categories.end())
        {
            plane->Link();
            planes.append(*plane);
        }
    }
}

const std::vector<std::string> dataPlaneCategories = { "data", "lz", "remote" };
const IStoragePlane * getDataStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();

    // NB: need to include "remote" planes too, because std. file access will encounter
    // files on the "remote" planes, when they have been remapped to them via ~remote access
    return getStoragePlane(group, dataPlaneCategories, required);
}

void getDataStoragePlanes(StoragePlaneArray &planes)
{
    return getStoragePlanes(planes, dataPlaneCategories);
}

const std::vector<std::string> remotePlaneCategories = { "remote" };
const IStoragePlane * getRemoteStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();
    return getStoragePlane(group, remotePlaneCategories, required);
}

IStoragePlane * createStoragePlane(IPropertyTree *meta)
{
    Owned<IPropertyTree> defaults = getGlobalConfigSP()->getPropTree("storage/defaults");
    if (!defaults)
        defaults.setown(createPTree("defaults"));
    return new CStoragePlane(*meta, *defaults);
}


AccessMode getAccessModeFromString(const char *access)
{
    // use a HT?
    if (streq(access, "read"))
        return AccessMode::read;
    else if (streq(access, "write"))
        return AccessMode::write;
    else if (streq(access, "random"))
        return AccessMode::random;
    else if (streq(access, "sequential"))
        return AccessMode::sequential;
    else if (streq(access, "noMount"))
        return AccessMode::noMount;
    else if (isEmptyString(access))
        return AccessMode::none;
    throwUnexpectedX("getAccessModeFromString : unrecognized access mode string");
}
