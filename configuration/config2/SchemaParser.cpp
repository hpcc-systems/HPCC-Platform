/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "SchemaParser.hpp"
#include "Exceptions.hpp"


bool SchemaParser::parse(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms)
{
    bool rc = true;
    try
    {
        doParse(configPath, masterConfigFile, cfgParms);
    }
    catch (const ParseException &pe)
    {
        m_message = "The following error was detected while parsing the configuration: " + static_cast<std::string>(pe.what());
        rc = false;
    }
    return rc;
}
