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
#include "XSDSchemaParser.hpp"
#include "Exceptions.hpp"
#include "SchemaValue.hpp"

#include "XSDComponentParser.hpp"
#include "XSDValueSetParser.hpp"
#include "SchemaTypeStringLimits.hpp"
#include "SchemaTypeIntegerLimits.hpp"

namespace pt = boost::property_tree;

bool XSDSchemaParser::doParse(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms)
{
    bool rc = true;

    //
    // Add some default types to the config. Note changing values for limits
    std::shared_ptr<SchemaTypeStringLimits> pStringLimits;
    std::shared_ptr<SchemaTypeIntegerLimits> pIntLimits;

    std::shared_ptr<SchemaType> pType = std::make_shared<SchemaType>("xs:string");
    pStringLimits = std::make_shared<SchemaTypeStringLimits>();
    pType->setLimits(pStringLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:token");
    pStringLimits = std::make_shared<SchemaTypeStringLimits>();
    pType->setLimits(pStringLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:boolean");
    std::shared_ptr<SchemaTypeLimits> pBoolLimits = std::make_shared<SchemaTypeStringLimits>();
    pBoolLimits->addAllowedValue("true");
    pBoolLimits->addAllowedValue("false");
    pType->setLimits(pBoolLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:integer");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:int");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:nonNegativeInteger");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pIntLimits->setMinInclusive(0);
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:positiveInteger");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pIntLimits->setMinInclusive(1);
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:unsignedInt");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pIntLimits->setMinInclusive(0);
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    //
    // Get our specific XSD parameters from the input
    m_basePath = configPath;
    m_masterXSDFilename = masterConfigFile;
    //m_buildsetFilename = cfgParms[2];
    parseXSD(m_masterXSDFilename);

    return rc;
}


void XSDSchemaParser::parseXSD(const std::string &filename)
{
    pt::ptree xsdTree;
    std::string fpath = m_basePath + filename;
    try
    {
        pt::read_xml(fpath, xsdTree, pt::xml_parser::trim_whitespace | pt::xml_parser::no_comments);
    }
    catch (const std::exception &e)
    {
        std::string xmlError = e.what();
        ParseException pe("Unable to read/parse file. Check that file is formatted correctly. Error = " + xmlError);
        pe.addFilename(filename);
        throw(pe);
    }

    try
    {
        auto schemaIt = xsdTree.find("xs:schema");
        pt::ptree emptyTree;
        const pt::ptree &keys = schemaIt->second.get_child("", emptyTree);
        parseXSD(keys);
    }
    catch (ParseException &pe)
    {
        pe.addFilename(filename);
        throw(pe);
    }
}


void XSDSchemaParser::parseXSD(const pt::ptree &keys)
{
    for (auto it = keys.begin(); it != keys.end(); ++it)
    {
        //
        // Element parent (a type in realilty) and the element name help figure out how to process the XSD schema element
        std::string elemType = it->first;
        if (elemType == "xs:include")
        {
            std::string schemaFile = getXSDAttributeValue(it->second, "<xmlattr>.schemaLocation");
            if (m_pSchemaItem->addUniqueName(schemaFile))
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


std::string XSDSchemaParser::getXSDAttributeValue(const pt::ptree &tree, const std::string &attrName, bool throwIfNotPresent, const std::string &defaultVal) const
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


void XSDSchemaParser::parseSimpleType(const pt::ptree &typeTree)
{
    std::shared_ptr<SchemaType> pCfgType = getSchemaType(typeTree, true);
    m_pSchemaItem->addSchemaValueType(pCfgType);
}


void XSDSchemaParser::parseAttribute(const pt::ptree &attr)
{
    std::shared_ptr<SchemaValue> pCfgValue = getSchemaValue(attr);
    m_pSchemaItem->addAttribute(pCfgValue);
}


void XSDSchemaParser::parseAttributeGroup(const pt::ptree &attributeTree)
{
    std::string groupName = getXSDAttributeValue(attributeTree, "<xmlattr>.name", false, "");  // only a named attributeGroup is supported

    //
    // If there is a name (for the attribute group) then a group of attributes is being defined. Create the group, parese it, and add it as a
    // schema type that can be reused (usually with a ref= reference in another attribute group schema item)
    if (!groupName.empty())
    {
        std::shared_ptr<SchemaItem> pValueSet = std::make_shared<SchemaItem>(groupName, "valueset", m_pSchemaItem);
        std::shared_ptr<XSDValueSetParser> pXSDValueSetParaser = std::make_shared<XSDValueSetParser>(pValueSet);
        pXSDValueSetParaser->parseXSD(attributeTree.get_child("", pt::ptree()));
        m_pSchemaItem->addSchemaType(pValueSet, groupName);
    }

    //
    // Is it a reference to a named attribute group previously saved? If so, grab the defined attributes and add them.
    else
    {
        std::string refName = getXSDAttributeValue(attributeTree, "<xmlattr>.ref", false, "");  // only a named attributeGroup is supported
        if (!refName.empty())
        {
            std::shared_ptr<SchemaItem> pValueSet = m_pSchemaItem->getSchemaType(refName, true);
            if (pValueSet)
            {
                std::vector<std::shared_ptr<SchemaValue>> attributes;
                pValueSet->getAttributes(attributes);
                m_pSchemaItem->addAttribute(attributes);
            }
        }
    }
}


void XSDSchemaParser::parseComplexType(const pt::ptree &typeTree)
{
    std::string complexTypeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", false, "");
    std::string className = typeTree.get("<xmlattr>.hpcc:class", "");
    std::string catName = typeTree.get("<xmlattr>.hpcc:category", "");
    std::string componentName = typeTree.get("<xmlattr>.hpcc:componentName", "");
    std::string displayName = typeTree.get("<xmlattr>.hpcc:displayName", "");
    bool hidden = typeTree.get("<xmlattr>.hpcc:hidden", "false") == "true";

    if (!complexTypeName.empty())
    {
        if (!className.empty())
        {
            if (className == "component")
            {
                std::shared_ptr<SchemaItem> pComponent = std::make_shared<SchemaItem>(complexTypeName, "component", m_pSchemaItem);
                pComponent->setProperty("category", catName);
                pComponent->setProperty("componentName", componentName);
                pComponent->setProperty("displayName", displayName);
                pComponent->setHidden(hidden);
                pt::ptree componentTree = typeTree.get_child("", pt::ptree());
                if (!componentTree.empty())
                {
                    std::shared_ptr<XSDComponentParser> pComponentXSDParaser = std::make_shared<XSDComponentParser>(std::dynamic_pointer_cast<SchemaItem>(pComponent));
                    pComponentXSDParaser->parseXSD(typeTree);
                    m_pSchemaItem->addSchemaType(pComponent, complexTypeName);
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
            std::shared_ptr<SchemaItem> pTypeItem = std::make_shared<SchemaItem>(complexTypeName, "", m_pSchemaItem);
            pt::ptree childTree = typeTree.get_child("", pt::ptree());
            if (!childTree.empty())
            {
                std::shared_ptr<XSDSchemaParser> pXSDParaser = std::make_shared<XSDSchemaParser>(pTypeItem);
                pXSDParaser->parseXSD(childTree);
                m_pSchemaItem->addSchemaType(pTypeItem, complexTypeName);
            }
            else
            {
                throw(ParseException("Complex type definition empty: " + displayName));
            }
        }
    }

    //
    // Just a complexType delimiter, ignore and parse the children
    else
    {
        parseXSD(typeTree.get_child("", pt::ptree()));
    }
}


void XSDSchemaParser::parseElement(const pt::ptree &elemTree)
{
    std::string elementName = elemTree.get("<xmlattr>.name", "");
    std::string className = elemTree.get("<xmlattr>.hpcc:class", "");
    std::string category = elemTree.get("<xmlattr>.hpcc:category", "");
    std::string displayName = elemTree.get("<xmlattr>.hpcc:displayName", "");
    std::string tooltip = elemTree.get("<xmlattr>.hpcc:tooltip", "");
    std::string typeName = elemTree.get("<xmlattr>.type", "");
    unsigned minOccurs = elemTree.get("<xmlattr>.minOccurs", 1);
    std::string maxOccursStr = elemTree.get("<xmlattr>.maxOccurs", "1");
    unsigned maxOccurs = (maxOccursStr != "unbounded") ? stoi(maxOccursStr) : UINTMAX_MAX;

    std::shared_ptr<SchemaItem> pNewSchemaItem = std::make_shared<SchemaItem>(elementName, className, m_pSchemaItem);
    pNewSchemaItem->setProperty("displayName", displayName);
    pNewSchemaItem->setMinInstances(minOccurs);
    pNewSchemaItem->setMaxInstances(maxOccurs);
    pNewSchemaItem->setProperty("category", category);
    pNewSchemaItem->setProperty("tooltip", tooltip);
    pNewSchemaItem->setHidden(elemTree.get("<xmlattr>.hpcc:hidden", "false") == "true");

    pt::ptree childTree = elemTree.get_child("", pt::ptree());

    // special case to set the root since the top level schema can't specify it
    if (category == "root")  // special case to set the root since the top level schema can't specify it
    {
        m_pSchemaItem->setProperty("name", elementName);
        parseXSD(childTree);
    }
    else
    {
        //
        // If a type is specified, then either it's a simple value type (which could be previously defined) for this element, or a named complex type.
        if (!typeName.empty())
        {
            const std::shared_ptr<SchemaType> pSimpleType = m_pSchemaItem->getSchemaValueType(typeName, false);
            if (pSimpleType != nullptr)
            {
                std::shared_ptr<SchemaValue> pCfgValue = std::make_shared<SchemaValue>("");  // no name value since it's the element's value
                pCfgValue->setType(pSimpleType);                      // will throw if type is not defined
                pNewSchemaItem->setItemSchemaValue(pCfgValue);
            }
            else
            {
                std::shared_ptr<SchemaItem> pConfigType = m_pSchemaItem->getSchemaType(typeName, false);
                if (pConfigType != nullptr)
                {
                    //
                    // Insert into this config element the component defined data (attributes, references, etc.)
                    pNewSchemaItem->insertSchemaType(pConfigType);

                    //
                    // Set element min/max instances to that defined by the component type def (ignore values parsed above)
                    pNewSchemaItem->setMinInstances(pConfigType->getMinInstances());
                    pNewSchemaItem->setMaxInstances(pConfigType->getMaxInstances());

                    //
                    // If a component, then set element data (allow overriding with locally parsed values)
                    if (pConfigType->getProperty("className") == "component")
                    {
                        pNewSchemaItem->setProperty("name", (!elementName.empty()) ? elementName : pConfigType->getProperty("name"));
                        pNewSchemaItem->setProperty("className", (!className.empty()) ? className : pConfigType->getProperty("className"));
                        pNewSchemaItem->setProperty("category", (!category.empty()) ? category : pConfigType->getProperty("category"));
                        pNewSchemaItem->setProperty("displayName", (!displayName.empty()) ? displayName : pConfigType->getProperty("displayName"));
                        pNewSchemaItem->setProperty("componentName", pConfigType->getProperty("componentName"));
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
            std::shared_ptr<XSDSchemaParser> pXSDParaser = std::make_shared<XSDSchemaParser>(pNewSchemaItem);
            pXSDParaser->parseXSD(childTree);
        }

        //
        // Add the element
        m_pSchemaItem->addChild(pNewSchemaItem);

    }
}


std::shared_ptr<SchemaType> XSDSchemaParser::getSchemaType(const pt::ptree &typeTree, bool nameRequired)
{
    std::string typeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", nameRequired, "");

    std::shared_ptr<SchemaType> pCfgType = std::make_shared<SchemaType>(typeName);
    std::shared_ptr<SchemaTypeLimits> pLimits;
    auto restriction = typeTree.find("xs:restriction");
    if (restriction != typeTree.not_found())
    {
        std::string baseType = getXSDAttributeValue(restriction->second, "<xmlattr>.base");
        std::shared_ptr<SchemaType> pType = std::make_shared<SchemaType>(*(m_pSchemaItem->getSchemaValueType(baseType)));

        pLimits = pType->getLimits();

        if (!restriction->second.empty())
        {
            pt::ptree restrictTree = restriction->second.get_child("", pt::ptree());
            if (std::dynamic_pointer_cast<SchemaTypeIntegerLimits>(pLimits) != nullptr)
            {
                std::shared_ptr<SchemaTypeIntegerLimits> pBaseIntLimits = std::dynamic_pointer_cast<SchemaTypeIntegerLimits>(pLimits);
                std::shared_ptr<SchemaTypeIntegerLimits> pIntLimits = std::make_shared<SchemaTypeIntegerLimits>(*pBaseIntLimits);
                parseIntegerTypeLimits(restrictTree, pIntLimits);
                pLimits = pIntLimits;
            }
            else if (std::dynamic_pointer_cast<SchemaTypeStringLimits>(pLimits) != nullptr)
            {
                std::shared_ptr<SchemaTypeStringLimits> pBaseStringimits = std::dynamic_pointer_cast<SchemaTypeStringLimits>(pLimits);
                std::shared_ptr<SchemaTypeStringLimits> pStringimits = std::make_shared<SchemaTypeStringLimits>(*pBaseStringimits);
                parseStringTypeLimits(restrictTree, pStringimits);
                pLimits = pStringimits;
            }
            else
            {
                std::string msg = "Unsupported base type(" + baseType + ")";
                throw(ParseException(msg));
            }
        }
    }

    pCfgType->setLimits(pLimits);
    return pCfgType;
}



void XSDSchemaParser::parseIntegerTypeLimits(const pt::ptree &restrictTree, std::shared_ptr<SchemaTypeIntegerLimits> &pIntegerLimits)
{
    for (auto it = restrictTree.begin(); it != restrictTree.end(); ++it)
    {
        std::string restrictionType = it->first;

        if (restrictionType == "xs:minInclusive")
            pIntegerLimits->setMinInclusive(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:maxInclusive")
            pIntegerLimits->setMaxInclusive(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:minExclusive")
            pIntegerLimits->setMinExclusive(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:maxExclusive")
            pIntegerLimits->setMaxExclusive(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:enumeration")
        {
            pIntegerLimits->addAllowedValue(it->second.get("<xmlattr>.value", "badbadbad"), it->second.get("<xmlattr>.hpcc:description", ""));
        }
        else if (restrictionType != "<xmlattr>")
        {
            std::string msg = "Invalid restriction(" + it->first + ") found while parsing type";
            throw(ParseException(msg));
        }
    }
}


void XSDSchemaParser::parseStringTypeLimits(const pt::ptree &restrictTree, std::shared_ptr<SchemaTypeStringLimits> &pStringLimits)
{
    for (auto it = restrictTree.begin(); it != restrictTree.end(); ++it)
    {
        std::string restrictionType = it->first;

        if (restrictionType == "xs:minLength")
            pStringLimits->setMinLength(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:maxLength")
            pStringLimits->setMaxLength(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:length")
            pStringLimits->setLength(it->second.get<int>("<xmlattr>.value"));
        else if (restrictionType == "xs:pattern")
            pStringLimits->addPattern(it->second.get("<xmlattr>.value", "0"));
        else if (restrictionType == "xs:enumeration")
        {
            pStringLimits->addAllowedValue(it->second.get("<xmlattr>.value", "badbadbad"), it->second.get("<xmlattr>.hpcc:description", ""));
        }
        else if (restrictionType != "<xmlattr>")
        {
            std::string msg = "Invalid restriction(" + it->first + ") found while parsing type";
            throw(ParseException(msg));
        }
    }
}


std::shared_ptr<SchemaValue> XSDSchemaParser::getSchemaValue(const pt::ptree &attr)
{
    std::string attrName = getXSDAttributeValue(attr, "<xmlattr>.name");
    std::shared_ptr<SchemaValue> pCfgValue = std::make_shared<SchemaValue>(attrName);
    pCfgValue->setDisplayName(attr.get("<xmlattr>.hpcc:displayName", attrName));
    pCfgValue->setRequired(attr.get("<xmlattr>.use", "optional") == "required");
    pCfgValue->setTooltip(attr.get("<xmlattr>.hpcc:tooltip", ""));
    pCfgValue->setReadOnly(attr.get("<xmlattr>.hpcc:readOnly", "false") == "true");
    pCfgValue->setHidden(attr.get("<xmlattr>.hpcc:hidden", "false") == "true");
    pCfgValue->setDeprecated(attr.get("<xmlattr>.hpcc:deprecated", "false") == "true");
    pCfgValue->setMirrorFromPath(attr.get("<xmlattr>.hpcc:mirrorFrom", ""));
    pCfgValue->setAutoGenerateType(attr.get("<xmlattr>.hpcc:autoGenerateType", ""));
    pCfgValue->setAutoGenerateValue(attr.get("<xmlattr>.hpcc:autoGenerateValue", ""));
    pCfgValue->setDefaultValue(attr.get("<xmlattr>.default", ""));

    std::string modList = attr.get("<xmlattr>.hpcc:modifiers", "");
    if (modList.length())
    {
        pCfgValue->setModifiers(split(modList, ","));
    }

    std::string typeName = attr.get("<xmlattr>.type", "");
    if (!typeName.empty())
    {
        pCfgValue->setType(m_pSchemaItem->getSchemaValueType(typeName));
    }
    else
    {
        std::shared_ptr<SchemaType> pType = getSchemaType(attr.get_child("xs:simpleType", pt::ptree()), false);
        if (!pType->isValid())
        {
            throw(ParseException("Attribute " + attrName + " does not have a valid type"));
        }
        pCfgValue->setType(pType);
    }
    return pCfgValue;
}


void XSDSchemaParser::parseKey(const pt::ptree &keyTree)
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

    m_pSchemaItem->addUniqueAttributeValueSetDefinition(keyName, elementName, attributeName, duplicateOk);
}


void XSDSchemaParser::parseKeyRef(const pt::ptree &keyTree)
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

    m_pSchemaItem->addReferenceToUniqueAttributeValueSet(keyName, elementName, attributeName);
}