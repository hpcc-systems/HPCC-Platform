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


#ifndef _CONFIG2_XSDCOMPONENTPARSER_HPP_
#define _CONFIG2_XSDCOMPONENTPARSER_HPP_

#include <string>
#include <memory>
#include <map>
#include <boost/property_tree/ptree.hpp>

#include "XSDSchemaParser.hpp"

namespace pt = boost::property_tree;

class XSDComponentParser : public XSDSchemaParser
{
    public:

        XSDComponentParser(std::shared_ptr<SchemaItem> pConfig) : XSDSchemaParser(pConfig) { }
        virtual ~XSDComponentParser() { }
        virtual void parseXSD(const pt::ptree &tree);


    protected:

        XSDComponentParser() { };
};


#endif // _CONFIG2_XSDCOMPONENTPARSER_HPP_
