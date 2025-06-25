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

#include "platform.h"
#include <array>
#include <string>
#include <unordered_map>

#include "jplane.hpp"
#include "jmutex.hpp"
#include "jptree.hpp"
#include "jfile.hpp"

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

// {prefix, {key1: value1, key2: value2, ...}}
typedef std::pair<std::string, std::array<unsigned __int64, PlaneAttributeCount>> PlaneAttributesMapElement;

static std::unordered_map<std::string, PlaneAttributesMapElement> planeAttributesMap;
static CriticalSection planeAttributeMapCrit;
static constexpr unsigned __int64 unsetPlaneAttrValue = 0xFFFFFFFF00000000;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    auto updateFunc = [&](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
    {
        CriticalBlock b(planeAttributeMapCrit);
        planeAttributesMap.clear();
        Owned<IPropertyTreeIterator> planesIter = getPlanesIterator(nullptr, nullptr);
        ForEach(*planesIter)
        {
            const IPropertyTree &plane = planesIter->query();
            PlaneAttributesMapElement &element = planeAttributesMap[plane.queryProp("@name")];
            const char * prefix = plane.queryProp("@prefix");
            element.first = prefix ? prefix : ""; // If prefix is empty, avoid segfault by setting it to an empty string
            auto &values = element.second;
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
                        values[propNum] = value;
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
                        values[propNum] = value;
                        break;
                    }
                    default:
                        throwUnexpected();
                }
            }
        }
    };

    jPlaneHookId = installConfigUpdateHook(updateFunc, true);
    return true;
}

MODULE_EXIT()
{
    removeConfigUpdateHook(jPlaneHookId);
}


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

IPropertyTree * getStoragePlane(const char * name)
{
    VStringBuffer xpath("storage/planes[@name='%s']", name);
    Owned<IPropertyTree> global = getGlobalConfig();
    return global->getPropTree(xpath);
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

IPropertyTreeIterator * getPlanesIterator(const char * category, const char *name)
{
    StringBuffer xpath("storage/planes");
    if (!isEmptyString(category))
        xpath.appendf("[@category='%s']", category);
    if (!isEmptyString(name))
        xpath.appendf("[@name='%s']", name);
    return getGlobalConfigSP()->getElements(xpath);
}

const char *getPlaneAttributeString(PlaneAttributeType attr)
{
    assertex(attr < PlaneAttributeCount);
    return planeAttributeInfo[attr].name;
}

unsigned __int64 getPlaneAttributeValue(const char *planeName, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue)
{
    if (!planeName)
        return defaultValue;
    assertex(planeAttrType < PlaneAttributeCount);
    CriticalBlock b(planeAttributeMapCrit);
    auto it = planeAttributesMap.find(planeName);
    if (it != planeAttributesMap.end())
    {
        unsigned __int64 v = it->second.second[planeAttrType];
        if (v != unsetPlaneAttrValue)
            return v;
    }
    return defaultValue;
}

static PlaneAttributesMapElement *findPlaneElementFromPath(const char *filePath)
{
    for (auto &e: planeAttributesMap)
    {
        const char *prefix = e.second.first.c_str();
        if (!isEmptyString(prefix)) // sanity check, std::string cannot be null, so check if empty
        {
            if (startsWith(filePath, prefix))
                return &e.second;
        }
    }
    return nullptr;
}

const char *findPlaneFromPath(const char *filePath, StringBuffer &result)
{
    CriticalBlock b(planeAttributeMapCrit);
    PlaneAttributesMapElement *e = findPlaneElementFromPath(filePath);
    if (!e)
        return nullptr;

    result.append(e->first.c_str());
    return result;
}

bool findPlaneAttrFromPath(const char *filePath, PlaneAttributeType planeAttrType, unsigned __int64 defaultValue, unsigned __int64 &resultValue)
{
    CriticalBlock b(planeAttributeMapCrit);
    PlaneAttributesMapElement *e = findPlaneElementFromPath(filePath);
    if (e)
    {
        unsigned __int64 value = e->second[planeAttrType];
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
