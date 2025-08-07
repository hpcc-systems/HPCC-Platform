/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include <string>
#include <unordered_map>

#include "jlog.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "hpccconfig.hpp"

bool getService(StringBuffer &serviceAddress, const char *serviceName, bool failIfNotFound)
{
    if (!isEmptyString(serviceName))
    {
        VStringBuffer serviceQualifier("services[@type='%s']", serviceName);
        Owned<IPropertyTree> serviceTree = getGlobalConfigSP()->getPropTree(serviceQualifier);
        if (serviceTree)
        {
            serviceAddress.append(serviceTree->queryProp("@name")).append(':').append(serviceTree->queryProp("@port"));
            return true;
        }
    }
    if (failIfNotFound)
        throw makeStringExceptionV(-1, "Service '%s' not found", serviceName);
    return false;
}

IPropertyTree* getMemorySpecifications(const IPropertyTree *config, const char *context, unsigned maxMB, GetJobValueFunction getJobValueFunction, GetJobValueBoolFunction getJobValueBoolFunction)
{
    /* Creates a property tree with memory settings retreived from either
     * the helper function (typically a workunit value fetch function) or the config.
     * Helper function values take priority.
     * "total" is computed and filled with the aggregate sum of all memory settings.
     * "recommendedMaxMemory" is computed and filled in based on max and the "maxMemPercentage" setting.
     */
    Owned<IPropertyTree> memorySpecifications = createPTree(context);

    auto getSetting = [&](const char *setting, StringBuffer &result) -> bool
    {
        VStringBuffer workunitSettingName("%s.%s", context, setting); // NB: workunit options are case insensitive
        if (getJobValueFunction(workunitSettingName, result))
            return true;
        VStringBuffer configSettingName("%s/@%s", context, setting);
        return config->getProp(configSettingName, result);
    };
    std::initializer_list<const char *> memorySettings = { "query", "thirdParty" };
    offset_t totalRequirements = 0;
    for (auto setting : memorySettings)
    {
        StringBuffer value;
        if (getSetting(setting, value))
        {
            offset_t memBytes = friendlyStringToSize(value);
            VStringBuffer attrName("@%s", setting);
            memorySpecifications->setPropInt64(attrName.str(), memBytes);
            totalRequirements += memBytes;
        }
    }
    offset_t maxBytes = ((offset_t)maxMB) * 0x100000;
    if (totalRequirements > maxBytes)
        throw makeStringExceptionV(0, "The total memory requirements of the query (%u MB) in '%s' exceed the memory limit (%u MB)", (unsigned)(totalRequirements / 0x100000), context, maxMB);
    memorySpecifications->setPropInt64("@total", totalRequirements);

    float maxPercentage = 100.0;
    offset_t recommendedMaxMemory = maxBytes;
    StringBuffer value;
    if (getSetting("maxMemPercentage", value))
    {
        maxPercentage = atof(value);
        verifyex((maxPercentage > 0.0) && (maxPercentage <= 100.0));
        if (maxPercentage < 100.0)
        {
            recommendedMaxMemory = maxBytes / 100.0 * maxPercentage;
            if (totalRequirements > recommendedMaxMemory)
                WARNLOG("The total memory requirements of the query (%u MB) exceed the recommended limits for '%s' (total memory: %u MB, recommended max percentage : %.2f%%)", (unsigned)(totalRequirements / 0x100000), context, maxMB, maxPercentage);
        }
    }
    memorySpecifications->setPropInt64("@recommendedMaxMemory", recommendedMaxMemory);
    memorySpecifications->setPropInt64("@totalMemory", maxBytes);

    // a simple helper used below, to fetch bool from workunit, or the memory settings (either managerMemory or workerMemory) or legacy location
    auto getBoolSetting = [&](const char *setting, bool defaultValue)
    {
        VStringBuffer attrSetting("@%s", setting);
        return getJobValueBoolFunction(setting,
            config->getPropBool(VStringBuffer("%s/%s", context, attrSetting.str()),
            config->getPropBool(attrSetting, defaultValue)));
    };
    // heapMasterUseHugePages is only used by thor, so it should really not be used if if eclagent calls this function.
    // But, to keeps things simple, we'll just use the same setting for eclagent and thor (eclagent won't have heapMasterUseHugePages)
    bool gmemAllowHugePages = getBoolSetting("heapMasterUseHugePages", getBoolSetting("heapUseHugePages", false));
    memorySpecifications->setPropBool("@heapUseHugePages", gmemAllowHugePages);
    memorySpecifications->setPropBool("@heapUseTransparentHugePages", getBoolSetting("heapUseTransparentHugePages", true));
    memorySpecifications->setPropBool("@heapRetainMemory", getBoolSetting("heapRetainMemory", false));
    memorySpecifications->setPropBool("@heapLockMemory", getBoolSetting("heapLockMemory", false));
    memorySpecifications->setPropBool("@traceRoxiePeakMemory", getBoolSetting("traceRoxiePeakMemory", false));
    return memorySpecifications.getClear();
}

static unsigned pipeProgramUpdateHookCBId = 0;
static const char *builtInPrograms = "roxiepipe"; // csv list
static StringBuffer allowedPipePrograms, allowedPipeProgramsWithBuiltIns;
static CriticalSection allowedPipeCS;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    auto updateFunc = [&](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
    {
        StringArray builtInList, configuredList, combinedList;
        builtInList.appendListUniq(builtInPrograms, ",");

        Owned<IPropertyTree> config = getComponentConfig();
        // NB: containerized config supports a different format
        if (isContainerized())
        {
            Owned<IPropertyTreeIterator> iter = config->getElements("allowedPipePrograms");
            ForEach(*iter)
            {
                const char *prog = iter->query().queryProp(nullptr);
                if (!isEmptyString(prog))
                    configuredList.appendUniq(prog);
            }
        }
        else
            configuredList.appendListUniq(config->queryProp("@allowedPipePrograms"), ",");
        
        ForEachItemIn(b, builtInList)
            combinedList.appendUniq(builtInList.item(b));
        ForEachItemIn(c, configuredList)
            combinedList.appendUniq(configuredList.item(c));

        ForEachItemIn(i, combinedList)
        {
            if (streq("*", combinedList.item(i)))
            {
                // disregard all others
                CriticalBlock block(allowedPipeCS);
                allowedPipePrograms.set("*");
                allowedPipeProgramsWithBuiltIns.set(allowedPipePrograms);
                return;
            }
        }
        CriticalBlock block(allowedPipeCS);
        builtInList.getString(allowedPipePrograms.clear(), ",");
        combinedList.getString(allowedPipeProgramsWithBuiltIns.clear(), ",");
    };
    pipeProgramUpdateHookCBId = installConfigUpdateHook(updateFunc, true);
    return true;
}

MODULE_EXIT()
{
    removeConfigUpdateHook(pipeProgramUpdateHookCBId);
}

void getAllowedPipePrograms(StringBuffer &allowedPrograms, bool addBuiltInPrograms)
{
    CriticalBlock block(allowedPipeCS);
    if (addBuiltInPrograms)
        allowedPrograms.append(allowedPipeProgramsWithBuiltIns);
    else
        allowedPrograms.append(allowedPipePrograms);
}
