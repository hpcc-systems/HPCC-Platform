/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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


#ifndef _CONFIG2_XMLENVIRONMENTLOADER_HPP_
#define _CONFIG2_XMLENVIRONMENTLOADER_HPP_

#include <string>
#include <fstream>
#include "SchemaItem.hpp"
#include "EnvironmentNode.hpp"
#include "EnvironmentLoader.hpp"

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

namespace pt = boost::property_tree;

class DECL_EXPORT XMLEnvironmentLoader : public EnvironmentLoader
{
    public:

        XMLEnvironmentLoader() { }
        virtual ~XMLEnvironmentLoader() { }
        virtual std::vector<std::shared_ptr<EnvironmentNode>> load(std::istream &in, const std::shared_ptr<SchemaItem> &pSchemaItem) const override;
        void parse(const pt::ptree &envTree, const std::shared_ptr<SchemaItem> &pConfigItem, std::shared_ptr<EnvironmentNode> &pEnvNode) const;

};

#endif
