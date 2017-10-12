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

#include "XSDValueSetParser.hpp"
#include "ConfigExceptions.hpp"

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
        // this is where elements in element would be hangled,probably with allocting new valueSet with this one as the parent.
    }
}


void XSDValueSetParser::parseAttributeGroup(const pt::ptree &attributeTree)
{
    //
    // Only support an attribute reference. The ref value is a type.
    std::string groupRefName = getXSDAttributeValue(attributeTree, "<xmlattr>.ref");
    std::shared_ptr<ConfigItemValueSet> pValueSet = std::dynamic_pointer_cast<ConfigItemValueSet>(m_pConfig->getConfigType(groupRefName));
    if (pValueSet)
    {
        m_pValueSet->addCfgValue(pValueSet);
    }
}


void XSDValueSetParser::parseAttribute(const pt::ptree &attr)
{
    std::shared_ptr<CfgValue> pCfgValue = getCfgValue(attr);
    m_pValueSet->addCfgValue(pCfgValue);
}
