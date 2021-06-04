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


#include "jptree.hpp"
#include "jstring.hpp"

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
