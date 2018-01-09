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

#include "XSDValueSetParser.hpp"
#include "Exceptions.hpp"

namespace pt = boost::property_tree;

void XSDValueSetParser::parseXSD(const pt::ptree &valueSetTree)
{
    for (auto it = valueSetTree.begin(); it != valueSetTree.end(); ++it)
    {
        //
        // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
        std::string elemType = it->first;
        if (it->first == "xs:attributeGroup")
        {
            parseAttributeGroup(it->second);
        }
        else if (it->first == "xs:attribute")
        {
            parseAttribute(it->second);
        }
    }
}


void XSDValueSetParser::parseAttributeGroup(const pt::ptree &attributeTree)
{
    std::string groupRefName = getXSDAttributeValue(attributeTree, "<xmlattr>.ref");
    std::shared_ptr<SchemaItem> pValueSet = m_pSchemaItem->getSchemaType(groupRefName, true);
    if (pValueSet)
    {
        m_pSchemaItem->addAttribute(pValueSet->getAttributes());
    }
}