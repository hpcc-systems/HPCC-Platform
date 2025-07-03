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
#include <string>
#include <unordered_map>

#include "jplane.hpp"
#include "jmutex.hpp"
#include "jptree.hpp"
#include "jfile.hpp"
#include "jsecrets.hpp"
#include "jlog.hpp"

//------------------------------------------------------------------------------------------------------------

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
    { PlaneAttrType::integer, 1024, false, "blockedRandomIOKB" }, // enum PlaneAttributeType::blockedRandomIOKB      {1}
    { PlaneAttrType::boolean, 0, true, "fileSyncWriteClose" },    // enum PlaneAttributeType::fileSyncWriteClose     {2}
    { PlaneAttrType::boolean, 0, true, "concurrentWriteSupport" },// enum PlaneAttributeType::concurrentWriteSupport {3}
    { PlaneAttrType::integer, 1, false, "writeSyncMarginMs" },    // enum PlaneAttributeType::WriteSyncMarginMs      {4}
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

private:
    IPropertyTree * queryContainer(unsigned stripeNumber) const
    {
        if (stripeNumber==0) // stripeNumber==0 when not striped -> use first item in 'containers' list
            stripeNumber++;
        VStringBuffer path("containers[%u]", stripeNumber);
        IPropertyTree *container = xml->queryPropTree(path.str());
        if (!container)
            throw makeStringExceptionV(-1, "No container provided: path %s", path.str());
        return container;
    }
    Owned<const IPropertyTree> xml;
};

//------------------------------------------------------------------------------------------------------------

class CStoragePlane final : public CInterfaceOf<IStoragePlane>
{
public:
    CStoragePlane(const IPropertyTree & plane)
    {
        config.set(&plane);

        name = plane.queryProp("@name");
        prefix = plane.queryProp("@prefix", "");
        category = plane.queryProp("@category", "");
        devices = config->getPropInt("@numDevices", 1);

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
    }

    virtual const char * queryPrefix() const override { return prefix.c_str(); }

    virtual unsigned numDevices() const override { return devices; }

    virtual const std::vector<std::string> &queryHosts() const override
    {
        return hosts;
    }

    virtual unsigned numDefaultSprayParts() const override { return config->getPropInt("@defaultSprayParts", 1); }

    virtual bool queryDirPerPart() const override { return config->getPropBool("@subDirPerFilePart", isContainerized()); } // default to dir. per part in containerized mode

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

    const char * queryName() const { return name.c_str(); }

    const IPropertyTree * queryConfig() const { return config; }

    const char * queryCategory() const { return category.c_str(); }

    unsigned __int64 getAttribute(PlaneAttributeType attr) const
    {
        assertex(attr < PlaneAttributeCount);
        return attributeValues[attr];
    }

private:
    std::string name;
    std::string prefix;
    std::string category;
    unsigned devices{1};
    std::array<unsigned __int64, PlaneAttributeCount> attributeValues;
    Linked<const IPropertyTree> config;
    std::vector<Owned<IStoragePlaneAlias>> aliases;
    std::vector<std::string> hosts;
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

        Owned<IPropertyTreeIterator> planesIter = getGlobalConfigSP()->getElements("storage/planes");
        ForEach(*planesIter)
        {
            const IPropertyTree &plane = planesIter->query();
            const char * name = plane.queryProp("@name");
            storagePlaneMap[name].setown(new CStoragePlane(plane));
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

//The following static functions must be called with the storagePlaneMapCrit held
static const CStoragePlane * doFindStoragePlaneFromPath(const char * path, bool required)
{
    for (auto &e: storagePlaneMap)
    {
        const char *prefix = e.second->queryPrefix();
        if (isPathInPrefix(prefix, path))
            return e.second;
    }

    if (required)
        throw makeStringExceptionV(99, "Could not map filename to storage plane %s", path);

    return nullptr;
}

static const CStoragePlane * doFindStoragePlaneByName(const char * name, bool required)
{
    auto it = storagePlaneMap.find(name);
    if (it != storagePlaneMap.end())
        return it->second;

    if (required)
        throw makeStringExceptionV(99, "Unknown storage plane %s", name);

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
        throw makeStringExceptionV(-1, "No entry found for hostGroup: '%s'", name ? name : "<null>");
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
const IPropertyTree * getStoragePlaneConfig(const char * name, bool required)
{
    CriticalBlock b(storagePlaneMapCrit);
    auto it = storagePlaneMap.find(name);
    if (it != storagePlaneMap.end())
        return LINK(it->second->queryConfig());

    if (required)
        throw makeStringExceptionV(99, "Unknown storage plane %s", name);
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
    {
        unsigned __int64 v = it->second->getAttribute(planeAttrType);
        if (v != unsetPlaneAttrValue)
            return v;
    }
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
        unsigned __int64 value = e->getAttribute(planeAttrType);
        if (unsetPlaneAttrValue != value)
            resultValue = value;
        else
            resultValue = defaultValue;
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
    Owned<const IPropertyTree> storagePlane = getStoragePlaneConfig(clusterName, false);
    if (!storagePlane)
    {
        OWARNLOG("lookupNumStripedDevices: Storage plane %s not found", clusterName);
        return 1;
    }

    unsigned numDevices = storagePlane->getPropInt("@numDevices");
    bool isPlaneStriped = !storagePlane->hasProp("@hostGroup") && (numDevices>1);

    return isPlaneStriped ? numDevices : 1;
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
        throw makeStringExceptionV(0, "getPlaneHost: index %u out of range 1..%u", which, maxHosts);
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
            throw makeStringExceptionV(-1, "storage plane '%s' does not match request categories (plane category=%s)", name, category);
        return nullptr;
    }

    const IStoragePlane * result = match;
    return LINK(result);
}

const IStoragePlane * getDataStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();

    // NB: need to include "remote" planes too, because std. file access will encounter
    // files on the "remote" planes, when they have been remapped to them via ~remote access
    return getStoragePlane(group, { "data", "lz", "remote" }, required);
}

const IStoragePlane * getRemoteStoragePlane(const char * name, bool required)
{
    StringBuffer group;
    group.append(name).toLowerCase();
    return getStoragePlane(group, { "remote" }, required);
}

IStoragePlane * createStoragePlane(IPropertyTree *meta)
{
    return new CStoragePlane(*meta);
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
