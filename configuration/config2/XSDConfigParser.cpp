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

#include <exception>
#include "XSDConfigParser.hpp"
#include "ConfigExceptions.hpp"
#include "CfgValue.hpp"

#include "XSDComponentParser.hpp"
#include "XSDValueSetParser.hpp"
#include "ConfigItemComponent.hpp"
#include "CfgStringLimits.hpp"
#include "CfgIntegerLimits.hpp"

namespace pt = boost::property_tree;

bool XSDConfigParser::doParse(const std::vector<std::string> &cfgParms)
{
    bool rc = true;
    //try
    {
		//
		// Add some default types to the config. Note changing values for limits 
		std::shared_ptr<CfgLimits> pStringLimits;
		std::shared_ptr<CfgLimits> pIntLimits;

		std::shared_ptr<CfgType> pType = std::make_shared<CfgType>("xs:string");
        pStringLimits = std::make_shared<CfgStringLimits>();
		pType->setLimits(pStringLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:token");
        pStringLimits = std::make_shared<CfgStringLimits>();
		pType->setLimits(pStringLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:boolean");
        std::shared_ptr<CfgLimits> pBoolLimits = std::make_shared<CfgStringLimits>();
        pBoolLimits->addAllowedValue("true");
        pBoolLimits->addAllowedValue("false");
		pType->setLimits(pBoolLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:integer");
        pIntLimits = std::make_shared<CfgIntegerLimits>();
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:int");
        pIntLimits = std::make_shared<CfgIntegerLimits>();
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:nonNegativeInteger");
        pIntLimits = std::make_shared<CfgIntegerLimits>();
		pIntLimits->setMinInclusive(0);
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:positiveInteger");
        pIntLimits = std::make_shared<CfgIntegerLimits>();
		pIntLimits->setMinInclusive(1);
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

        pType = std::make_shared<CfgType>("xs:unsignedInt");
        pIntLimits = std::make_shared<CfgIntegerLimits>();
        pIntLimits->setMinInclusive(0);
        pType->setLimits(pIntLimits);
        m_pConfig->addType(pType);

        m_buildsetFilename = cfgParms[1];
        parseXSD(cfgParms[0]);
    }
    
    return rc;
}


void XSDConfigParser::parseXSD(const std::string &filename)
{
    pt::ptree xsdTree;
    std::string fpath = m_basePath + filename;
    try
    {
        pt::read_xml(fpath, xsdTree);
    }
    catch (const std::exception &e)
    {
        std::string xmlError = e.what();
        throw(ParseException("Input configuration file is not valid: " + xmlError));
    }

    try
    {
        auto schemaIt = xsdTree.find("xs:schema");
        const pt::ptree &keys = schemaIt->second.get_child("", pt::ptree());
        parseXSD(keys);
    }
    catch (ParseException &pe)
    {
        pe.addFilename(filename);
        throw(pe);
    }
}


void XSDConfigParser::parseXSD(const pt::ptree &keys)
{
    for (auto it = keys.begin(); it != keys.end(); ++it)
    {
        //
        // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
        std::string elemType = it->first;
        if (elemType == "xs:include")
        {
            std::string schemaFile = getXSDAttributeValue(it->second, "<xmlattr>.schemaLocation");
            if (m_pConfig->addUniqueName(schemaFile))
            {
                parseXSD(schemaFile);
            }
        }
        else if (elemType == "xs:simpleType")
        {
            parseSimpleType(it->second);
        }
        else if (elemType == "xs:complexType")
        {
            parseComplexType(it->second);
        }
        else if (elemType == "xs:attributeGroup")
        {
            parseAttributeGroup(it->second);
        }
        else if (elemType == "xs:attribute")
        {
            parseAttribute(it->second);
        }
        else if (elemType == "xs:sequence")
        {
            parseXSD(it->second.get_child("", pt::ptree()));
        }
        else if (elemType == "xs:element")
        {
            parseElement(it->second);
        }
        else if (elemType == "xs:key")
        {
            parseKey(it->second);
        }
        else if (elemType == "xs:keyref")
        {
            parseKeyRef(it->second);
        }
    }
}


std::string XSDConfigParser::getXSDAttributeValue(const pt::ptree &tree, const std::string &attrName, bool throwIfNotPresent, const std::string &defaultVal) const
{
    std::string value = defaultVal;
    try
    {
        value = tree.get<std::string>(attrName);
    }
    catch (std::exception &e)
    {
        if (throwIfNotPresent)
            throw(ParseException("Missing attribute " + attrName + "."));
    }
    return value;
}


void XSDConfigParser::parseSimpleType(const pt::ptree &typeTree)
{
    std::shared_ptr<CfgType> pCfgType = getCfgType(typeTree, true);
    m_pConfig->addType(pCfgType);
}


void XSDConfigParser::parseAttribute(const pt::ptree &attr)
{
    std::shared_ptr<CfgValue> pCfgValue = getCfgValue(attr);
	m_pConfig->addAttribute(pCfgValue);
}


void XSDConfigParser::parseAttributeGroup(const pt::ptree &attributeTree)
{
    std::string groupName = getXSDAttributeValue(attributeTree, "<xmlattr>.name", false, "");  // only a named attributeGroup is supported
	if (groupName != "")
	{
		std::shared_ptr<ConfigItemValueSet> pValueSet = std::make_shared<ConfigItemValueSet>(groupName, m_pConfig);
		std::shared_ptr<XSDValueSetParser> pXSDValueSetParaser = std::make_shared<XSDValueSetParser>(m_basePath, std::dynamic_pointer_cast<ConfigItem>(pValueSet));
		pXSDValueSetParaser->parseXSD(attributeTree.get_child("", pt::ptree()));
		m_pConfig->addConfigType(pValueSet, groupName);
	}
	else
	{
		std::string refName = getXSDAttributeValue(attributeTree, "<xmlattr>.ref", false, "");  // only a named attributeGroup is supported
		if (refName != "")
		{
			std::shared_ptr<ConfigItemValueSet> pValueSet = std::dynamic_pointer_cast<ConfigItemValueSet>(m_pConfig->getConfigType(refName, true));
			if (pValueSet)
			{
				m_pConfig->addAttribute(pValueSet->getCfgValues());
			}
		}
	}
}


void XSDConfigParser::parseComplexType(const pt::ptree &typeTree)
{
    std::string complexTypeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", false, "");
    std::string className = typeTree.get("<xmlattr>.hpcc:class", "");
    std::string catName = typeTree.get("<xmlattr>.hpcc:category", "");
    std::string componentName = typeTree.get("<xmlattr>.hpcc:componentName", "");
    std::string displayName = typeTree.get("<xmlattr>.hpcc:displayName", "");

    if (complexTypeName != "")
    {
        if (className != "")
        {
            if (className == "component")
            {
                std::shared_ptr<ConfigItemComponent> pComponent = std::make_shared<ConfigItemComponent>(complexTypeName, m_pConfig);
                pComponent->setCategory(catName);
                pComponent->setComponentName(componentName);
                pComponent->setDisplayName(displayName);
                pt::ptree componentTree = typeTree.get_child("", pt::ptree());
                if (!componentTree.empty())
                {
                    std::shared_ptr<XSDComponentParser> pComponentXSDParaser = std::make_shared<XSDComponentParser>(m_basePath, std::dynamic_pointer_cast<ConfigItem>(pComponent));
                    pComponentXSDParaser->parseXSD(typeTree);
                    m_pConfig->addConfigType(pComponent, complexTypeName);
                }
                else
                {
                    throw(ParseException("Component definition empty: " + displayName));
                }
            }
            else
            {
                throw(ParseException("Unrecognized class name for complex type: " + className));
            }
        }

        //
        // This is a complex type definition of just regular XSD statements, no special format. Create a parser and parse it
        // and add it to the 
        else
        {
            std::shared_ptr<ConfigItem> pTypeItem = std::make_shared<ConfigItem>(complexTypeName, "", m_pConfig);
            //pTypeItem->setDisplayName(displayName);
            pt::ptree childTree = typeTree.get_child("", pt::ptree());
            if (!childTree.empty())
            {
                std::shared_ptr<XSDConfigParser> pXSDParaser = std::make_shared<XSDConfigParser>(m_basePath, pTypeItem);
                pXSDParaser->parseXSD(childTree);
                m_pConfig->addConfigType(pTypeItem, complexTypeName);
            }
            else
            {
                throw(ParseException("Complex type definition empty: " + displayName));
            }
        }
        // todo: else throw, not recognized complexType class
    }

    //
    // Just a complexType delimiter, ignore and parse the children
    else
    {
        parseXSD(typeTree.get_child("", pt::ptree()));
    }
}


void XSDConfigParser::parseElement(const pt::ptree &elemTree)
{
    std::string elementName = elemTree.get("<xmlattr>.name", "");
    std::string className = elemTree.get("<xmlattr>.hpcc:class", "");
    std::string category = elemTree.get("<xmlattr>.hpcc:category", "");
    std::string displayName = elemTree.get("<xmlattr>.hpcc:displayName", "");
    //std::string refName = elemTree.get("<xmlattr>.ref", "");
    std::string typeName = elemTree.get("<xmlattr>.type", "");
    int minOccurs = elemTree.get("<xmlattr>.minOccurs", 1);
    std::string maxOccursStr = elemTree.get("<xmlattr>.maxOccurs", "1");
    int maxOccurs = (maxOccursStr != "unbounded") ? stoi(maxOccursStr) : -1;

    std::shared_ptr<ConfigItem> pConfigElement = std::make_shared<ConfigItem>(elementName, className, m_pConfig);
    pConfigElement->setDisplayName(displayName);
    pConfigElement->setMinInstances(minOccurs);
    pConfigElement->setMaxInstances(maxOccurs);
    pConfigElement->setCategory(category);

    pt::ptree childTree = elemTree.get_child("", pt::ptree());
    
    // special case to set the root since the top level schema can't specify it
    if (category == "root")  // special case to set the root since the top level schema can't specify it
    {
        m_pConfig->setName(elementName);
        parseXSD(childTree);
    }
    else
    {
        //
        // If a type is specified, then either it's a simple value type (which could be previously defined) for this element, or a named complex type. 
        if (typeName != "")
        {
            const std::shared_ptr<CfgType> pSimpleType = m_pConfig->getType(typeName, false);
            if (pSimpleType != nullptr)
            {
                std::shared_ptr<CfgValue> pCfgValue = std::make_shared<CfgValue>("");  // no name value since it's the element's value 
                pCfgValue->setType(pSimpleType);                      // will throw if type is not defined
                pConfigElement->setItemCfgValue(pCfgValue);
            }
            else
            {
                std::shared_ptr<ConfigItem> pConfigType = m_pConfig->getConfigType(typeName, false);
                if (pConfigType != nullptr)
                {
                    pConfigElement->insertConfigType(pConfigType);
 
                    if (pConfigType->getClassName() == "component")
                    {
                        pConfigElement->setName(pConfigType->getName());
                        pConfigElement->setClassName(pConfigType->getClassName());
                        pConfigElement->setCategory(pConfigType->getCategory());
                        pConfigElement->setComponentName(pConfigType->getComponentName());  // for components, the config type name is used as the component name
                    }
                }
                else
                {
                    std::string msg = "Unable to find type " + typeName + " when parsing element " + elementName;
                    throw(ParseException(msg));
                }
            }
        }

        //
        // Now, if there are children, create a parser and have at it
        if (!childTree.empty())
        {
            std::shared_ptr<XSDConfigParser> pXSDParaser = std::make_shared<XSDConfigParser>(m_basePath, pConfigElement);
            pXSDParaser->parseXSD(childTree);
        }

        //
        // Add the element
        m_pConfig->addChild(pConfigElement);
      
    }
}


std::shared_ptr<CfgType> XSDConfigParser::getCfgType(const pt::ptree &typeTree, bool nameRequired)
{
    std::string typeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", nameRequired, "");

    std::shared_ptr<CfgType> pCfgType = std::make_shared<CfgType>(typeName);
    std::shared_ptr<CfgLimits> pLimits; // = std::make_shared<CfgLimits>();
    auto restriction = typeTree.find("xs:restriction");
    if (restriction != typeTree.not_found())
    {
        std::string baseType = getXSDAttributeValue(restriction->second, "<xmlattr>.base");
		std::shared_ptr<CfgType> pType = std::make_shared<CfgType>(*(m_pConfig->getType(baseType)));
		pLimits = std::make_shared<CfgLimits>(*(pType->getLimits()));
        if (!pLimits)
        {
            std::string msg = "Unsupported base type(" + baseType + ")";
            throw(ParseException(msg));
        }

		pCfgType->setAutoValueType(getXSDAttributeValue(restriction->second, "<xmlattr>.hpcc:autoType", false, ""));

        if (!restriction->second.empty())
        {
            pt::ptree restrictTree = restriction->second.get_child("", pt::ptree());
            for (auto it = restrictTree.begin(); it != restrictTree.end(); ++it)
            {
                try
                {
                    std::string restrictionType = it->first;
                    if (restrictionType == "xs:minLength")
                        pLimits->setMinLength(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:maxLength")
                        pLimits->setMaxLength(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:length")
                        pLimits->setLength(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:minInclusive")
                        pLimits->setMinInclusive(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:maxInclusive")
                        pLimits->setMaxInclusive(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:minExclusive")
                        pLimits->setMinExclusive(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:maxExclusive")
                        pLimits->setMaxExclusive(it->second.get<int>("<xmlattr>.value"));
                    else if (restrictionType == "xs:pattern")
                        pLimits->addPattern(it->second.get("<xmlattr>.value", "0"));
                    else if (restrictionType == "xs:enumeration")
                    {
                        pLimits->addAllowedValue(it->second.get("<xmlattr>.value", "badbadbad"), it->second.get("<xmlattr>.hpcc:description", ""));
                    }
                    else if (restrictionType.find("xs:") != std::string::npos)
                    {
                        std::string msg = "Unsupported restriction(" + it->first + ") found while parsing type(" + typeName + ")";
                        throw(ParseException(msg));
                    }
                }
                catch (std::exception &e)
                {
                    std::string msg = "Invalid value found for restriction(" + it->first + ")";
                    throw(ParseException(msg));
                }
            }
        }
    }

    pCfgType->setLimits(pLimits);
    return pCfgType;
}


std::shared_ptr<CfgValue> XSDConfigParser::getCfgValue(const pt::ptree &attr)
{
    std::string attrName = getXSDAttributeValue(attr, "<xmlattr>.name");
    std::shared_ptr<CfgValue> pCfgValue = std::make_shared<CfgValue>(attrName);
    pCfgValue->setDisplayName(attr.get("<xmlattr>.hpcc:displayName", attrName));
    pCfgValue->setRequired(attr.get("<xmlattr>.use", "optional") == "required");
    pCfgValue->setTooltip(attr.get("<xmlattr>.hpcc:tooltip", ""));
    pCfgValue->setReadOnly(attr.get("<xmlattr>.hpcc:readOnly", "false") == "true");
    pCfgValue->setHidden(attr.get("<xmlattr>.hpcc:hidden", "false") == "true");
    pCfgValue->setDeprecated(attr.get("<xmlattr>.hpcc:deprecated", "false") == "true");
    pCfgValue->setMirrorFromPath(attr.get("<xmlattr>.hpcc:mirrorFrom", ""));

    std::string defaultValue = attr.get("<xmlattr>.default", "notsetnotsetAAAnotsetnotset");
    if (defaultValue != "notsetnotsetAAAnotsetnotset")
    {
        pCfgValue->setDefault(defaultValue);
    }

    std::string modList = attr.get("<xmlattr>.hpcc:modifiers", "");
    if (modList.length())
    {
        pCfgValue->setModifiers(split(modList, ","));
    }

    std::string typeName = attr.get("<xmlattr>.type", "");
    if (typeName != "")
    {
        pCfgValue->setType(m_pConfig->getType(typeName));
    }
    else
    {
        std::shared_ptr<CfgType> pCfgType = getCfgType(attr.get_child("xs:simpleType", pt::ptree()), false);
        if (!pCfgType->isComplete())
        {
            throw(ParseException("Attribute " + attrName + " does not have a valid type"));
        }
        pCfgValue->setType(pCfgType);
    }
    return pCfgValue;
}


void XSDConfigParser::parseKey(const pt::ptree &keyTree)
{
    std::string keyName = getXSDAttributeValue(keyTree, "<xmlattr>.name");
    bool duplicateOk = keyTree.get("<xmlattr>.hpcc:allowDuplicate", "false") == "true";
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

    m_pConfig->addUniqueAttributeValueSetDefinition(keyName, elementName, attributeName, duplicateOk);
}


void XSDConfigParser::parseKeyRef(const pt::ptree &keyTree)
{
    std::string keyName = getXSDAttributeValue(keyTree, "<xmlattr>.refer");
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

    m_pConfig->addReferenceToUniqueAttributeValueSet(keyName, elementName, attributeName);
}