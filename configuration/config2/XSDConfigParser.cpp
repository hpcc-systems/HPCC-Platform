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
    try
    {
		//
		// Add some default types to the config. Note changing values for limits 
		std::shared_ptr<CfgLimits> pStringLimits = std::make_shared<CfgStringLimits>();;
		std::shared_ptr<CfgLimits> pIntLimits = std::make_shared<CfgIntegerLimits>();

		std::shared_ptr<CfgType> pType = std::make_shared<CfgType>("xs:string");
		pType->setLimits(pStringLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:token");
		pType->setLimits(pStringLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:boolean");
		pStringLimits->addAllowedValue("true");
		pStringLimits->addAllowedValue("false");
		pType->setLimits(pStringLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:integer");
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:int");
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:nonNegativeInteger");
		pIntLimits->setMinInclusive(0);
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

		pType = std::make_shared<CfgType>("xs:positiveInteger");
		pIntLimits->setMinInclusive(1);
		pType->setLimits(pIntLimits);
		m_pConfig->addType(pType);

        m_buildsetFilename = cfgParms[1];
        parseXSD(cfgParms[0]);
    }
    catch (...)
    {
		// need to return the message somehow here
        rc = false;
    }
    
    return rc;
}


void XSDConfigParser::parseXSD(const std::string &filename)
{
    try
    {
        pt::ptree xsdTree;
        std::string fpath = m_basePath + filename;
        pt::read_xml(fpath, xsdTree);
        auto schemaIt = xsdTree.find("xs:schema");
        const pt::ptree &keys = schemaIt->second.get_child("", pt::ptree());
        parseXSD(keys);
    }
	catch (ParseException &e)
	{
		throw(e);
	}
    catch (std::exception &e)
    {
        throw(ParseException("Input configuration file is not valid: " + *e.what()));
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
			std::shared_ptr<ConfigItemValueSet> pValueSet = std::dynamic_pointer_cast<ConfigItemValueSet>(m_pConfig->getConfigType(refName));
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
    std::string displayName = typeTree.get("<xmlattr>.hpcc:displayName", "");

    if (complexTypeName != "" || className != "")
    {
        if (className == "component")
        {
            std::shared_ptr<ConfigItemComponent> pComponent = std::make_shared<ConfigItemComponent>(complexTypeName, m_pConfig); 
            pComponent->setCategory(catName);
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
                throw(new ParseException("Component definition empty: " + displayName));
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
    std::string refName = elemTree.get("<xmlattr>.ref", "");
    std::string typeName = elemTree.get("<xmlattr>.type", "");
    int minOccurs = elemTree.get("<xmlattr>.minOccurs", 1);
    std::string maxOccursStr = elemTree.get("<xmlattr>.maxOccurs", "1");
    int maxOccurs = (maxOccursStr != "unbounded") ? stoi(maxOccursStr) : -1;
        
    std::shared_ptr<ConfigItemComponent> pTypeConfigItem;

    //
    // If we have a type name specififed, see of it has a config defined for it
    if (typeName != "")
    {
        std::shared_ptr<ConfigItem> pConfigItem = m_pConfig->getConfigType(typeName);
        if (pConfigItem)
        {
            std::shared_ptr<ConfigItemComponent> pComponent = std::dynamic_pointer_cast<ConfigItemComponent>(pConfigItem);
            if (pComponent)
            {
                pTypeConfigItem = std::make_shared<ConfigItemComponent>(*pComponent);
                pTypeConfigItem->setCategory(category);
                pTypeConfigItem->setDisplayName(displayName);
                pTypeConfigItem->setMinInstances(minOccurs);
                pTypeConfigItem->setMaxInstances(maxOccurs);
                pTypeConfigItem->setVersion(elemTree.get("<xmlattr>.hpcc:version", -1));
            }
            else
            {
                throw(new ParseException("Element reference is not a compoenent: " + refName));
            }
        }
    }

    //
    // A class name of component is used to start the definition of a component type that can be referenced as a type elsewhere
    if (className == "component")
    {
        std::shared_ptr<ConfigItemComponent> pComponent = std::make_shared<ConfigItemComponent>(elementName, m_pConfig);  
        pComponent->setCategory(category);
        pComponent->setDisplayName(displayName);
        pComponent->setMinInstances(minOccurs);
        pComponent->setMaxInstances(maxOccurs);
		pComponent->setVersion(elemTree.get("<xmlattr>.hpcc:version", -1));
        std::shared_ptr<ConfigItem> pConfigItem = std::dynamic_pointer_cast<ConfigItem>(pComponent);
        std::shared_ptr<XSDComponentParser> pComponentXSDParaser = std::make_shared<XSDComponentParser>(m_basePath, pConfigItem);
        pComponentXSDParaser->parseXSD(elemTree.get_child("xs:complexType", pt::ptree())); 
        m_pConfig->addChild(pComponent);
    }

    //
    // A category?
    else if (className == "category")
    {
        //
        // If we have a type config we found before, add it
        if (pTypeConfigItem)
        {   
            m_pConfig->addChild(pTypeConfigItem);
        }
        else
        {
            pt::ptree childTree = elemTree.get_child("", pt::ptree());
            if (category == "root")
            {
                m_pConfig->setName(elementName);
                parseXSD(childTree);
            }
            else
            {
                std::shared_ptr<ConfigItem> pConfigItem = std::make_shared<ConfigItem>(elementName, "category", m_pConfig);
                std::shared_ptr<XSDConfigParser> pXSDParaser = std::make_shared<XSDConfigParser>(m_basePath, pConfigItem);
                pXSDParaser->parseXSD(childTree);
                m_pConfig->addChild(pConfigItem);
            }
        }
    }
    else if (elementName != "")
    {
		//
		// We have an element. Create a new config Item for it
		std::shared_ptr<ConfigItem> pConfigElement = std::make_shared<ConfigItem>(elementName, "element", m_pConfig);   // class name of element 
		
		//
		// Does the element have a type? If so, process it and move on
		if (typeName != "")
		{
			std::shared_ptr<CfgValue> pCfgValue = std::make_shared<CfgValue>("");  // no name value since it's the element's value 
			pCfgValue->setType(m_pConfig->getType(typeName));                      // will throw if type is not defined
			pConfigElement->setItemCfgValue(pCfgValue);
		}
		else
		{
			//
			// Go ahead and parse the contents of the element. This will pick up any attributes, or if a simple type is defined for it
			std::shared_ptr<XSDConfigParser> pXSDParaser = std::make_shared<XSDConfigParser>(m_basePath, pConfigElement);    
			pXSDParaser->parseXSD(elemTree.get_child("", pt::ptree()));
		}

		pConfigElement->setDisplayName(displayName);
		pConfigElement->setMinInstances(minOccurs);
		pConfigElement->setMaxInstances(maxOccurs);
		m_pConfig->addChild(pConfigElement);  
    }
    else
    {
        if (pTypeConfigItem)
        {
            m_pConfig->addChild(pTypeConfigItem);
        }
        else
        {
            throw(new ParseException("Only component elements are supported at the top level"));
        }
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
		std::shared_ptr<CfgType> pType = m_pConfig->getType(baseType);
		pLimits = pType->getLimits();
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
                        pLimits->addAllowedValue(it->second.get("<xmlattr>.value", "badbadbad"), it->second.get("<xmlattr>.hpcc:description", ""));
                    else if (restrictionType.find("xs:") != std::string::npos)
                    {
                        std::string msg = "Unsupported restriction(" + it->first + ") found while parsing type(" + typeName + ")";
                        throw(new ParseException(msg));
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
    pCfgValue->setForceOutput(attr.get("<xmlattr>.forceOutput", true));
    pCfgValue->setTooltip(attr.get("<xmlattr>.hpcc:tooltip", ""));
    pCfgValue->setReadOnly(attr.get("<xmlattr>.hpcc:readOnly", "false") == "true");
    pCfgValue->setHidden(attr.get("<xmlattr>.hpcc:hidden", "false") == "true");
    pCfgValue->setDefault(attr.get("<xmlattr>.default", ""));
    pCfgValue->setDeprecated(attr.get("<xmlattr>.hpcc:deprecated", "false") == "true");
    pCfgValue->setMirrorFromPath(attr.get("<xmlattr>.hpcc:mirrorFrom", ""));

    std::string modList = attr.get("<xmlattr>.modifiers", "");
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
        if (!pCfgType->isValid())
        {
            throw(new ParseException("Attribute " + attrName + " does not have a valid type"));
        }
        pCfgValue->setType(pCfgType);
    }
    return pCfgValue;
}

