/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
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

#include "jconfig.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jstring.hpp"


namespace config
{

IStringIterator *getContainerTargets(const char *processType, const char *processName)
{
    Owned<CStringArrayIterator> ret = new CStringArrayIterator;
    Owned<IPropertyTreeIterator> queues = getComponentConfigSP()->getElements("queues");
    ForEach(*queues)
    {
        IPropertyTree& queue = queues->query();
        if (!isEmptyString(processType))
        {
            const char* type = queue.queryProp("@type");
            if (isEmptyString(type) || !strieq(type, processType))
                continue;
        }
        const char* qName = queue.queryProp("@name");
        if (isEmptyString(qName))
            continue;

        if (!isEmptyString(processName) && !strieq(qName, processName))
            continue;

        ret->append_unique(qName);
    }
    if (!isEmptyString(processType) && !strieq("roxie", processType))
        return ret.getClear();

    Owned<IPropertyTreeIterator> services = getGlobalConfigSP()->getElements("services[@type='roxie']");
    ForEach(*services)
    {
        IPropertyTree& service = services->query();
        const char* targetName = service.queryProp("@target");
        if (isEmptyString(targetName))
            continue;

        if (!isEmptyString(processName) && !strieq(targetName, processName))
            continue;

        ret->append_unique(targetName);
    }
    return ret.getClear();
}


} // namespace config