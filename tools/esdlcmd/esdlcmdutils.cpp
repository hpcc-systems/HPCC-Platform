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

#include "esdlcmdutils.hpp"

bool extractEsdlCmdOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix)
{
    if (option.length())        // check if already specified via a command line option
        return true;
    if (propertyName && globals && globals->getProp(propertyName, option))
        return true;
    if (envName && *envName)
    {
        const char * env = getenv(envName);
        if (env)
        {
            option.append(env);
            return true;
        }
    }
    if (defaultPrefix)
        option.append(defaultPrefix);
    if (defaultSuffix)
        option.append(defaultSuffix);
    return false;
}
