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


#include "XSDComponentParser.hpp"
#include "XSDValueSetParser.hpp"
#include "ConfigExceptions.hpp"

namespace pt = boost::property_tree;


void XSDComponentParser::parseXSD(const pt::ptree &compTree)
{
    bool foundComponentDef = false;

    pt::ptree tree = compTree.get_child("", pt::ptree());

    //
    // First time through look for attributeGroups that can be defined and for existence of a sequenc element that actually defines the component
    for (auto it = tree.begin(); it != tree.end(); ++it)
    {
        //
        // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
        std::string elemType = it->first;
        if (elemType == "xs:attributeGroup")
        {
            parseAttributeGroup(it->second);
        }
        else if (elemType == "xs:sequence")
        {
            foundComponentDef = true;
        }
    }


    if (foundComponentDef)
    {
        pt::ptree elemTree = tree.get_child("xs:sequence.xs:element", pt::ptree());
        if (!elemTree.empty())
        {
            std::string elementName = getXSDAttributeValue(elemTree, "<xmlattr>.name");
            m_pConfig->setName(elementName);
            int minOccurs = elemTree.get("<xmlattr>.minOccurs", 1);
            std::string maxOccursStr = elemTree.get("<xmlattr>.maxOccurs", "1");
            int maxOccurs = (maxOccursStr != "unbounded") ? stoi(maxOccursStr) : -1;
            m_pConfig->setMinInstances(minOccurs);
            m_pConfig->setMaxInstances(maxOccurs);

            //
            // Parse any attributes, these are located in the xs:complexType section
            pt::ptree attributeTree = elemTree.get_child("xs:complexType", pt::ptree());
            for (auto attrIt = attributeTree.begin(); attrIt != attributeTree.end(); ++attrIt)
            {
                //
                // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
                std::string elemType = attrIt->first;
                if (elemType == "xs:attributeGroup")
                {
                    parseAttributeGroup(attrIt->second);
                }
                else if (elemType == "xs:attribute")
                {
                    parseAttribute(attrIt->second);
                }
            }

            //
            // Now parse the sequence section (these are sub keys for the component)
            XSDConfigParser::parseXSD(elemTree.get_child("xs:complexType.xs:sequence", pt::ptree()));

            //
            // See if any post config stuff like unique, key, or keyref
            for (auto it = elemTree.begin(); it != elemTree.end(); ++it)
            {
                //
                // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
                std::string elemType = it->first;
                if (elemType == "xs:key")
                {
                    parseKey(it->second);
                }
                else if (elemType == "xs:keyref")
                {
                    parseKeyRef(it->second);
                }
            }
        
        }
    }
}


void XSDComponentParser::parseKey(const pt::ptree &keyTree)
{
    std::string keyName = getXSDAttributeValue(keyTree, "<xmlattr>.name");
    std::string elementName = getXSDAttributeValue(keyTree, "xs:selector.<xmlattr>.xpath", false, "");
    std::string attrName = getXSDAttributeValue(keyTree, "xs:field.<xmlattr>.xpath", false, "");
    std::string attributeName;

    if (attrName.find_first_of('@') != std::string::npos)
    {
        attributeName = attrName.substr(attrName.find_first_of('@') + 1);
    }
    else
    {
        attributeName = attrName;
    }

    m_pConfig->addKey(keyName, elementName, attributeName);
}


void XSDComponentParser::parseKeyRef(const pt::ptree &keyTree)
{
    std::string keyRefName = getXSDAttributeValue(keyTree, "<xmlattr>.refer");
    std::string elementName = getXSDAttributeValue(keyTree, "xs:selector.<xmlattr>.xpath", false, "");
    std::string attrName = getXSDAttributeValue(keyTree, "xs:field.<xmlattr>.xpath", false, "");
    std::string attributeName;

    if (attrName.find_first_of('@') != std::string::npos)
    {
        attributeName = attrName.substr(attrName.find_first_of('@') + 1);
    }
    else
    {
        attributeName = attrName;
    }

    m_pConfig->addKeyRef(keyRefName, elementName, attributeName);
}

