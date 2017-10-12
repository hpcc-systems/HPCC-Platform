/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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


#ifndef _CONFIG2_CONFIGPARSER_HPP_
#define _CONFIG2_CONFIGPARSER_HPP_

#include <string>
#include <memory>
#include <vector>


#include "ConfigItem.hpp"


class ConfigParser 
{
    public:

        ConfigParser(const std::string &basePath, std::shared_ptr<ConfigItem> &pConfig) : m_basePath(basePath), m_pConfig(pConfig) { };
        virtual ~ConfigParser() { };
        virtual bool parseEnvironmentConfig(const std::vector<std::string> &cfgParms);
    

    protected:

        virtual bool doParse(const std::vector<std::string> &cfgParms) = 0;
        ConfigParser() { };
        std::vector<std::string> split(const std::string  &input, const std::string  &delim);
        

    protected:

        std::string m_basePath;
        std::shared_ptr<ConfigItem> m_pConfig;
};


#endif // _CONFIG2_CONFIGPARSER_HPP_
