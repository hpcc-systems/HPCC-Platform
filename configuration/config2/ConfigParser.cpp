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

#include "ConfigParser.hpp"
#include "ConfigExceptions.hpp"


bool ConfigParser::parseEnvironmentConfig(const std::vector<std::string> &cfgParms, Status &status)
{
    try
    {
        doParse(cfgParms);
    }
    catch (const ParseException &pe)
    {
        std::string msg = "The following error was detected while parsing the configuration: " + static_cast<std::string>(pe.what());
        status.addStatusMsg(statusMsg::fatal, "", "", "", msg);
    }
    catch (...)
    {
        int i = 8;
    }
    return status.isOk();
}


std::vector<std::string> ConfigParser::split(const std::string  &input, const std::string  &delim)
{
    size_t  start = 0, end = 0, delimLen = delim.length();
    std::vector<std::string> list;

    while (end != std::string::npos)
    {
        end = input.find(delim, start);
        list.push_back(input.substr(start, (end == std::string::npos) ? std::string::npos : end - start));
        start = ((end > (std::string::npos - delimLen)) ? std::string::npos : end + delimLen);
    }
    return list;
}