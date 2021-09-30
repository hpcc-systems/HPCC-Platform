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

void getMemorySpecifications(std::unordered_map<std::string, __uint64> &memorySpecifications, const IPropertyTree *config, const char *context, unsigned maxMB, GetJobValueFunction getJobValueFunction)
{
    /* fills the memorySpecifications map with memory settings retreived from either
     * the helper function (typically a workunit value fetch function) or the config.
     * Helper function values take priority.
     * "total" is computed and filled with the aggregate sum of all memory settings.
     * "recommendedMaxMemory" is computed and filled in based on max and the "maxMemPercentage" setting.
     */

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
            memorySpecifications[setting] = memBytes;
            totalRequirements += memBytes;
        }
    }
    offset_t maxBytes = ((offset_t)maxMB) * 0x100000;
    if (totalRequirements > maxBytes)
        throw makeStringExceptionV(0, "The total memory requirements of the query (%u MB) in '%s' exceed the memory limit (%u MB)", (unsigned)(totalRequirements / 0x100000), context, maxMB);
    memorySpecifications["total"] = totalRequirements;

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
    memorySpecifications["recommendedMaxMemory"] = recommendedMaxMemory;
}
