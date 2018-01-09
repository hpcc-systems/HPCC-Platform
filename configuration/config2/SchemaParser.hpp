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


#ifndef _CONFIG2_CONFIGPARSER_HPP_
#define _CONFIG2_CONFIGPARSER_HPP_

#include <string>
#include <memory>
#include <vector>


#include "SchemaItem.hpp"
#include "Status.hpp"
#include "platform.h"


class DECL_EXPORT SchemaParser
{
    public:

        SchemaParser(std::shared_ptr<SchemaItem> &pSchema) : m_pSchemaItem(pSchema) { };
        virtual ~SchemaParser() { };
        virtual bool parse(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms);
        const std::string getLastMessage() const { return m_message;  }


    protected:

        virtual bool doParse(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms) = 0;
        SchemaParser() { };
        std::vector<std::string> split(const std::string  &input, const std::string  &delim);


    protected:

        std::shared_ptr<SchemaItem> m_pSchemaItem;
        std::string m_message;                       // a place where a message can be stored and retrieved, such as for a parse error
};


#endif // _CONFIG2_CONFIGPARSER_HPP_
