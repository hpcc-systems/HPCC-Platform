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
#include "EnvironmentEventHandlers.hpp"
#include "Utils.hpp"

namespace pt = boost::property_tree;

bool XSDSchemaParser::doParse(const std::string &configPath, const std::string &masterConfigFile,  const std::vector<std::string> &cfgParms)
{
    bool rc = true;

    //
    // Add some default types to the config. Note changing values for limits
    std::shared_ptr<SchemaTypeStringLimits> pStringLimits;
    std::shared_ptr<SchemaTypeIntegerLimits> pIntLimits;

    std::shared_ptr<SchemaType> pType = std::make_shared<SchemaType>("xs:string");
    pType->setBaseType("string");
    pStringLimits = std::make_shared<SchemaTypeStringLimits>();
    pType->setLimits(pStringLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:token");
    pType->setBaseType("string");
    pStringLimits = std::make_shared<SchemaTypeStringLimits>();
    pType->setLimits(pStringLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:boolean");
    pType->setBaseType("boolean");
    std::shared_ptr<SchemaTypeLimits> pBoolLimits = std::make_shared<SchemaTypeStringLimits>();
    pBoolLimits->addAllowedValue("true");
    pBoolLimits->addAllowedValue("false");
    pType->setLimits(pBoolLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:integer");
    pType->setBaseType("integer");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:nonNegativeInteger");
    pType->setBaseType("integer");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pIntLimits->setMinInclusive(0);
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:positiveInteger");
    pType->setBaseType("integer");
    pIntLimits = std::make_shared<SchemaTypeIntegerLimits>();
    pIntLimits->setMinInclusive(1);
    pType->setLimits(pIntLimits);
    m_pSchemaItem->addSchemaValueType(pType);

    pType = std::make_shared<SchemaType>("xs:unsignedInt");
    pType->setBaseType("integer");
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
        else if (elemType == "xs:annotation")
        {
            parseAnnotation(it->second);
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
    std::shared_ptr<SchemaType> pCfgType = getType(typeTree, true);
    m_pSchemaItem->addSchemaValueType(pCfgType);
}


std::shared_ptr<SchemaValue> XSDSchemaParser::parseAttribute(const pt::ptree &attr)
{
    std::shared_ptr<SchemaValue> pCfgValue = getSchemaValue(attr);
    m_pSchemaItem->addAttribute(pCfgValue);
    return pCfgValue;
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
        std::string groupByName = getXSDAttributeValue(attributeTree, "<xmlattr>.hpcc:groupByName", false, "");
        pXSDValueSetParaser->setGroupByName(groupByName);
        pXSDValueSetParaser->parseXSD(attributeTree.get_child("", pt::ptree()));
        m_pSchemaItem->addSchemaType(pValueSet, groupName);
        m_pSchemaItem->setProperty("attribute_group_default_overrides", getXSDAttributeValue(attributeTree, "<xmlattr>.hpcc:defaultInCodeOverrides", false, ""));
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
                std::string groupByName = getXSDAttributeValue(attributeTree, "<xmlattr>.groupByName", false, "");
                if (!groupByName.empty())
                {
                    for (auto &attr: attributes)
                    {
                        attr->setGroupByName(groupByName);
                    }
                }
                m_pSchemaItem->addAttribute(attributes);

                //
                // Add any unique valueset references
                m_pSchemaItem->addUniqueAttrValueSetDefsAndRefs(pValueSet);

                //
                // See if there are any overrides for default in code values
                std::string dfltOverrides = pValueSet->getProperty("attribute_group_default_overrides");
                if (!dfltOverrides.empty())
                {
                    std::vector<std::string> overrides = splitString(dfltOverrides, ",");
                    for (auto &info: overrides)
                    {
                        std::vector<std::string> vals = splitString(info, "=");
                        if (vals.size() != 2)
                        {
                            throw (ParseException("Invalid default value override in attribute group (" + refName + ")"));
                        }

                        std::shared_ptr<SchemaValue> pAttr = m_pSchemaItem->getAttribute(vals[0], false);
                        if (pAttr)
                        {
                            pAttr->setCodeDefault(vals[1]);
                        }
                    }
                }
            }
        }
    }
}


void XSDSchemaParser::parseComplexType(const pt::ptree &typeTree)
{
    std::string complexTypeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", false, "");

    if (!complexTypeName.empty())
    {
        std::shared_ptr<SchemaItem> pComplexType = std::make_shared<SchemaItem>(complexTypeName, "component", m_pSchemaItem);
        pComplexType->setProperty("itemType", complexTypeName);

        pt::ptree childTree = typeTree.get_child("", pt::ptree());
        if (!childTree.empty())
        {
            std::shared_ptr<XSDSchemaParser> pXSDParaser = std::make_shared<XSDSchemaParser>(pComplexType);
            pXSDParaser->parseXSD(childTree);
            m_pSchemaItem->addSchemaType(pComplexType, complexTypeName);
        }
        else
        {
            throw(ParseException("Complex type definition empty: " + complexTypeName));
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
    //
    // Get a couple attributes to help figure out how to handle the element
    std::string elementName = elemTree.get("<xmlattr>.name", "");
    std::string category = elemTree.get("<xmlattr>.hpcc:category", "");

    pt::ptree emptyTree;
    pt::ptree childTree = elemTree.get_child("", emptyTree);

    if (category == "root")
    {
        m_pSchemaItem->setProperty("name", elementName);
        parseXSD(childTree);
    }
    else
    {
        std::string className = elemTree.get("<xmlattr>.hpcc:class", "");
        std::string displayName = elemTree.get("<xmlattr>.hpcc:displayName", elementName);
        std::string tooltip = elemTree.get("<xmlattr>.hpcc:tooltip", "");
        std::string typeName = elemTree.get("<xmlattr>.type", "");
        std::string itemType = elemTree.get("<xmlattr>.hpcc:itemType", "");
        std::string insertLimitType = elemTree.get("<xmlattr>.hpcc:insertLimitType", "");
        std::string insertLimitData = elemTree.get("<xmlattr>.hpcc:insertLimitData", "");
        unsigned minOccurs = elemTree.get("<xmlattr>.minOccurs", 1);
        std::string maxOccursStr = elemTree.get("<xmlattr>.maxOccurs", "1");
        unsigned maxOccurs = (maxOccursStr != "unbounded") ? stoi(maxOccursStr) : UINT_MAX;
        std::shared_ptr<SchemaItem> pNewSchemaItem = std::make_shared<SchemaItem>(elementName, className, m_pSchemaItem);

        if (!className.empty()) pNewSchemaItem->setProperty("className", className);
        if (!displayName.empty()) pNewSchemaItem->setProperty("displayName", displayName);
        if (!tooltip.empty()) pNewSchemaItem->setProperty("tooltip", tooltip);
        if (!insertLimitType.empty()) pNewSchemaItem->setProperty("insertLimitType", insertLimitType);
        if (!insertLimitData.empty()) pNewSchemaItem->setProperty("insertLimitData", insertLimitData);
        pNewSchemaItem->setProperty("itemType", itemType);
        pNewSchemaItem->setProperty("category", category.empty() ? displayName : category );
        pNewSchemaItem->setMinInstances(minOccurs);
        pNewSchemaItem->setMaxInstances(maxOccurs);
        pNewSchemaItem->setHidden(elemTree.get("<xmlattr>.hpcc:hidden", "false") == "true");

        //
        // Type specified?
        if (!typeName.empty())
        {
            //
            // If a simple type, then it represents the element value type, so allocate a value object, set its type and add
            // it to the item.
            const std::shared_ptr<SchemaType> pSimpleType = m_pSchemaItem->getSchemaValueType(typeName, false);
            if (pSimpleType != nullptr)
            {
                std::shared_ptr<SchemaValue> pCfgValue = std::make_shared<SchemaValue>("");  // no name value since it's the element's value
                pCfgValue->setType(pSimpleType);                      // will throw if type is not defined
                pNewSchemaItem->setItemSchemaValue(pCfgValue);
                std::shared_ptr<XSDSchemaParser> pXSDParaser = std::make_shared<XSDSchemaParser>(pNewSchemaItem);
                pXSDParaser->parseXSD(childTree);
                m_pSchemaItem->addChild(pNewSchemaItem);
            }
            else
            {
                //
                // Wasn't a simple type, complex?
                std::shared_ptr<SchemaItem> pConfigType = m_pSchemaItem->getSchemaType(typeName, false);
                if (pConfigType != nullptr)
                {
                    //
                    // A complex type was found. Insert it
                    std::vector<std::shared_ptr<SchemaItem>> typeChildren;
                    pConfigType->getChildren(typeChildren);
                    for (auto childIt = typeChildren.begin(); childIt != typeChildren.end(); ++childIt)
                    {
                        std::shared_ptr<SchemaItem> pNewItem = std::make_shared<SchemaItem>(*(*childIt));
                        pNewItem->setParent(m_pSchemaItem);
                        m_pSchemaItem->addChild(pNewItem);
                    }
                }
                else
                {
                    std::string msg = "Unable to find type " + typeName + " when parsing element " + elementName;
                    throw(ParseException(msg));
                }
            }
        }
        else
        {
            //
            // No type, just continue parsing a new element
            //pNewSchemaItem = std::make_shared<SchemaItem>(elementName, className, m_pSchemaItem);
            std::shared_ptr<XSDSchemaParser> pXSDParaser = std::make_shared<XSDSchemaParser>(pNewSchemaItem);
            pXSDParaser->parseXSD(childTree);
            m_pSchemaItem->addChild(pNewSchemaItem);
        }
    }
}


void XSDSchemaParser::parseAnnotation(const pt::ptree &elemTree)
{
    pt::ptree emptyTree;
    const pt::ptree &keys = elemTree.get_child("", emptyTree);

    //
    // Parse app info sections
    for (auto it = keys.begin(); it != keys.end(); ++it)
    {
        if (it->first == "xs:appinfo")
        {
            parseAppInfo(it->second);
        }
    }
}



void XSDSchemaParser::parseAppInfo(const pt::ptree &elemTree)
{
    std::string appInfoType = elemTree.get("<xmlattr>.hpcc:infoType", "");
    pt::ptree emptyTree, childTree;

    //
    // Process the app info based on its type
    if (appInfoType == "event")
    {
        childTree = elemTree.get_child("", emptyTree);
        std::string eventType = getXSDAttributeValue(childTree, "eventType");

        //
        // Fir a create event type, get the eventAction attrbute to decide what to do
        if (eventType == "create")
        {
            std::string eventAction = getXSDAttributeValue(childTree, "eventAction");

            //
            // addAttributeDependencies is used to set dependent values for an attribute based on the value of another attribute.
            if (eventAction == "addAttributeDependencies")
            {
                std::shared_ptr<AttributeDependencyCreateEventHandler> pDep = std::make_shared<AttributeDependencyCreateEventHandler>();
                pt::ptree dataTree = childTree.get_child("eventData", emptyTree);
                for (auto it = dataTree.begin(); it != dataTree.end(); ++it)
                {
                    if (it->first == "itemType")
                    {
                        pDep->setItemType(it->second.data());
                    }
                    else if (it->first == "attribute")
                    {
                        std::string attrName = getXSDAttributeValue(it->second, "<xmlattr>.attributeName");
                        std::string attrVal = getXSDAttributeValue(it->second, "<xmlattr>.attributeValue");
                        std::string depAttr = getXSDAttributeValue(it->second, "<xmlattr>.dependentAttribute");
                        std::string depVal = getXSDAttributeValue(it->second, "<xmlattr>.dependentValue");
                        pDep->addDependency(attrName, attrVal, depAttr, depVal);
                    }
                    else if (it->first == "match")
                    {
                        std::string attrName = it->second.get("eventNodeAttribute", "").data();
                        pDep->setEventNodeAttributeName(attrName);
                        std::string matchAttrName = it->second.get("targetAttribute", "");
                        if (!matchAttrName.empty())
                        {
                            pDep->setTargetAttributeName(matchAttrName);
                        }
                        std::string path = it->second.get("targetPath", "");
                        pDep->setTargetPath(path);
                    }
                }
                m_pSchemaItem->addEventHandler(pDep);
            }

            //
            // Insert XML is ued to insert XML ino the environment based on what's in the eventData section
            else if (eventAction == "insertXML")
            {
                std::shared_ptr<InsertEnvironmentDataCreateEventHandler> pInsert = std::make_shared<InsertEnvironmentDataCreateEventHandler>();

                pt::ptree dataTree = childTree.get_child("eventData", emptyTree);
                for (auto it = dataTree.begin(); it != dataTree.end(); ++it)
                {
                    //
                    // itemTye is the type of the item that was created for which the create event shall be sent.
                    if (it->first == "itemType")
                    {
                        pInsert->setItemType(it->second.data());
                    }

                    //
                    // The match section is optional. It is used to further qualify the conditions when XML is inserted. If missing,
                    // the XML is inserted whenever a new node of "itemType" is inserted. When present, the following fields further
                    // qualify when the XML is inserted.
                    //
                    //    matchItemAttribute  - name of attribute from created node whose value is compared with the value of an attribute in
                    //                          another node (defined by matchLocalAttribte and matchPath)
                    //    matchPath           - XPath to select the node for comparing attribute values
                    //    matchLocalAttribute - name of attribute from node selected by matchPath for value comparison. This option is
                    //                          optional. If not present, the name of the attribute for comparison in the selected node is
                    //                          matchItemAttribute
                    else if (it->first == "match")
                    {
                        std::string attrName = it->second.get("eventNodeAttribute", "").data();
                        pInsert->setEventNodeAttributeName(attrName);
                        std::string matchAttrName = it->second.get("targetAttribute", "");
                        if (!matchAttrName.empty())
                        {
                            pInsert->setTargetAttributeName(matchAttrName);
                        }
                        std::string path = it->second.get("targetPath", "");
                        pInsert->setTargetPath(path);
                    }

                    //
                    // the XML to be inserted. It is inserted based on the match section
                    else if (it->first == "xml")
                    {
                        pt::ptree emptyTree;
                        const pt::ptree &insertXMLTree = it->second.get_child("", emptyTree);

                        std::ostringstream out;
                        pt::write_xml(out, insertXMLTree);
                        pInsert->setEnvironmentInsertData(out.str());
                    }
                }
                m_pSchemaItem->addEventHandler(pInsert);
            }

            //
            // addAttributeDependencies is used to set dependent values for an attribute based on the value of another attribute.
            else if (eventAction == "setAttributeValue")
            {
                std::shared_ptr<AttributeSetValueCreateEventHandler> pSetAttrValue = std::make_shared<AttributeSetValueCreateEventHandler>();
                pt::ptree dataTree = childTree.get_child("eventData", emptyTree);
                for (auto it = dataTree.begin(); it != dataTree.end(); ++it)
                {
                    if (it->first == "itemType")
                    {
                        pSetAttrValue->setItemType(it->second.data());
                    }
                    else if (it->first == "attribute")
                    {
                        std::string attrName = getXSDAttributeValue(it->second, "<xmlattr>.attributeName");
                        std::string attrVal = getXSDAttributeValue(it->second, "<xmlattr>.attributeValue");
                        pSetAttrValue->addAttributeValue(attrName, attrVal);
                    }
                    else if (it->first == "match")
                    {
                        std::string attrName = it->second.get("eventNodeAttribute", "").data();
                        pSetAttrValue->setEventNodeAttributeName(attrName);
                        std::string matchAttrName = it->second.get("targetAttribute", "");
                        if (!matchAttrName.empty())
                        {
                            pSetAttrValue->setTargetAttributeName(matchAttrName);
                        }
                        std::string path = it->second.get("targetPath", "");
                        pSetAttrValue->setTargetPath(path);
                    }
                }
                m_pSchemaItem->addEventHandler(pSetAttrValue);
            }
        }
    }
}


std::shared_ptr<SchemaType> XSDSchemaParser::getType(const pt::ptree &typeTree, bool nameRequired)
{
    std::string typeName = getXSDAttributeValue(typeTree, "<xmlattr>.name", nameRequired, "");

    if (!nameRequired && !typeName.empty())
    {
        std::string msg = "Name (" + typeName + ") not allowed in local xs:simpleType definition";
        throw(ParseException(msg));
    }

    std::shared_ptr<SchemaType> pCfgType = std::make_shared<SchemaType>(typeName);
    std::shared_ptr<SchemaTypeLimits> pLimits;
    auto restriction = typeTree.find("xs:restriction");
    if (restriction != typeTree.not_found())
    {
        std::string xsdBaseType = getXSDAttributeValue(restriction->second, "<xmlattr>.base");
        std::shared_ptr<SchemaType> pBaseType = m_pSchemaItem->getSchemaValueType(xsdBaseType);
        pCfgType->setBaseType(pBaseType->getBaseType());
        if (typeName != pBaseType->getBaseType())
        {
            pCfgType->setSubType(typeName);
        }

        pLimits = pBaseType->getLimits();

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
                std::string msg = "Unsupported base type(" + xsdBaseType + ")";
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
            parseAllowedValue(it->second, &(*pIntegerLimits));
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
            parseAllowedValue(it->second, &(*pStringLimits));
        }
        else if (restrictionType != "<xmlattr>")
        {
            std::string msg = "Invalid restriction(" + it->first + ") found while parsing type";
            throw(ParseException(msg));
        }
    }
}


void XSDSchemaParser::parseAllowedValue(const pt::ptree &allowedValueTree, SchemaTypeLimits *pTypeLimits)
{
    AllowedValue allowedValue;

    //
    // Parse the value for the enumeration, the add to the allowed values for the limits for this type. Note that enumerations
    // are enhanced with additional information for the UI.
    allowedValue.m_value = allowedValueTree.get("<xmlattr>.value", "XXXmissingYYY");
    allowedValue.m_displayName = allowedValueTree.get("<xmlattr>.hpcc:displayName", allowedValue.m_value);
    allowedValue.m_description = allowedValueTree.get("<xmlattr>.hpcc:description", "");
    allowedValue.m_userMessage = allowedValueTree.get("<xmlattr>.hpcc:userMessage", "");
    allowedValue.m_userMessageType = allowedValueTree.get("<xmlattr>.hpcc:userMessageType", allowedValue.m_userMessage.empty() ? "" : "info");

    //
    // Parse any attribute lists
    std::string attrList = allowedValueTree.get("<xmlattr>.hpcc:optionalAttributes", "");
    if (attrList.length())
    {
        allowedValue.m_optionalAttributes = splitString(attrList, ",");
    }

    attrList = allowedValueTree.get("<xmlattr>.hpcc:requiredAttributes", "");
    if (attrList.length())
    {
        allowedValue.m_requiredAttributes = splitString(attrList, ",");
    }

    //
    // Value is required. Throw an exception if not found
    if (allowedValue.m_value == "XXXmissingYYY")
    {
        std::string msg = "Missing value attribute for enumeration";
        throw(ParseException(msg));
    }

    pTypeLimits->addAllowedValue(allowedValue);
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
    pCfgValue->setCodeDefault(attr.get("<xmlattr>.hpcc:defaultInCode", ""));
    pCfgValue->setValueLimitRuleType(attr.get("<xmlattr>.hpcc:valueLimitRuleType", ""));
    pCfgValue->setValueLimitRuleData(attr.get("<xmlattr>.hpcc:valueLimitRuleData", ""));
    pCfgValue->setRequiredIf(attr.get("<xmlattr>.hpcc:requiredIf", ""));

    std::string modList = attr.get("<xmlattr>.hpcc:modifiers", "");
    if (modList.length())
    {
        pCfgValue->setModifiers(splitString(modList, ","));
    }

    std::string typeName = attr.get("<xmlattr>.type", "");
    if (!typeName.empty())
    {
        pCfgValue->setType(m_pSchemaItem->getSchemaValueType(typeName));
    }
    else
    {
        std::shared_ptr<SchemaType> pType = getType(attr.get_child("xs:simpleType", pt::ptree()), false);
        if (!pType->isValid())
        {
            throw(ParseException("Attribute " + m_pSchemaItem->getProperty("name") + "[@" + attrName + "] does not have a valid type"));
        }
        pCfgValue->setType(pType);
    }

    //
    // Keyed value or from a keyed set?
    std::string uniqueKey = attr.get("<xmlattr>.hpcc:uniqueKey", "");
    std::string sourceKey = attr.get("<xmlattr>.hpcc:sourceKey", "");

    //
    // Make sure both aren't specified
    if (!uniqueKey.empty() && !sourceKey.empty())
    {
        throw(ParseException("Attribute " + m_pSchemaItem->getProperty("name") + "[@" + attrName + "] cannot be both unique and from a source path"));
    }

    //
    // If value must be unique, add a unique value set definition
    if (!uniqueKey.empty())
    {
        std::string elementPath = "./";
        m_pSchemaItem->addUniqueAttributeValueSetDefinition(uniqueKey, elementPath, attrName, true);
    }

    //
    // If the value must be from an existing attribute, add reference to such
    else if (!sourceKey.empty())
    {
        std::string elementPath = "./";
        m_pSchemaItem->addReferenceToUniqueAttributeValueSet(sourceKey, elementPath, attrName);
    }

    return pCfgValue;
}
