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

#include "XMLEnvironmentMgr.hpp"
#include "XSDSchemaParser.hpp"


bool XMLEnvironmentMgr::createParser()
{
    m_pSchemaParser = std::make_shared<XSDSchemaParser>(m_pSchema);
    return true;
}


bool XMLEnvironmentMgr::doLoadEnvironment(std::istream &in)
{

    bool rc = true;
    pt::ptree envTree;
    try
    {
        pt::read_xml(in, envTree, pt::xml_parser::trim_whitespace | pt::xml_parser::no_comments);
        auto rootIt = envTree.begin();

        //
        // Start at root, these better match!
        std::string rootName = rootIt->first;
        if (rootName == m_pSchema->getProperty("name"))
        {
            m_pRootNode = std::make_shared<EnvironmentNode>(m_pSchema, rootName);
            m_pRootNode->setId("0");
            addPath(m_pRootNode);
            parse(rootIt->second, m_pSchema, m_pRootNode);
        }
    }
    catch (const std::exception &e)
    {
        std::string xmlError = e.what();
        m_message = "Unable to read/parse Environment file. Error = " + xmlError;
        rc = false;
    }
    return rc;
}


bool XMLEnvironmentMgr::save(std::ostream &out)
{
    bool rc = true;
    try
    {
        pt::ptree envTree, topTree;
        serialize(envTree, m_pRootNode);
        topTree.add_child("Environment", envTree);
        pt::write_xml(out, topTree);
    }
    catch (const std::exception &e)
    {
        std::string xmlError = e.what();
        m_message = "Unable to save Environment file. Error = " + xmlError;
        rc = false;
    }
    return rc;
}


void XMLEnvironmentMgr::parse(const pt::ptree &envTree, const std::shared_ptr<SchemaItem> &pConfigItem, std::shared_ptr<EnvironmentNode> &pEnvNode)
{

    //
    // First see if the node has a value
    std::string value;
    try
    {
        value = envTree.get<std::string>("");
        if (!value.empty())
        {
            std::shared_ptr<SchemaValue> pCfgValue = pConfigItem->getItemSchemaValue();
            std::shared_ptr<EnvironmentValue> pEnvValue = std::make_shared<EnvironmentValue>(pEnvNode, pCfgValue, "");  // node's value has no name
            pEnvValue->setValue(value, nullptr);
            pEnvNode->setLocalEnvValue(pEnvValue);
        }
    }
    catch (...)
    {
        // do nothing
    }

    //
    // Find elements in environment tree cooresponding to this config item, then parse each
    for (auto it = envTree.begin(); it != envTree.end(); ++it)
    {
        std::string elemName = it->first;

        //
        // First see if there are attributes for this element (<xmlattr> === <element attr1="xx" attr2="yy" ...></element>  The attr1 and attr2 are in this)
        if (elemName == "<xmlattr>")
        {
            for (auto attrIt = it->second.begin(); attrIt != it->second.end(); ++attrIt)
            {
                std::shared_ptr<SchemaValue> pSchemaValue = pConfigItem->getAttribute(attrIt->first);  // note, undefined attributes in schema will return a generic schema value
                std::string curValue = attrIt->second.get_value<std::string>();
                std::shared_ptr<EnvironmentValue> pEnvValue = std::make_shared<EnvironmentValue>(pEnvNode, pSchemaValue, attrIt->first, curValue);   // this is where we would use a variant
                pSchemaValue->addEnvironmentValue(pEnvValue);
                pEnvNode->addAttribute(attrIt->first, pEnvValue);
            }
        }
        else
        {
            std::string typeName = it->second.get("<xmlattr>.buildSet", "");
            std::shared_ptr<SchemaItem> pSchemaItem;
            if (!typeName.empty())
            {
                pSchemaItem = pConfigItem->getChildByComponent(elemName, typeName);
            }
            else
            {
                pSchemaItem = pConfigItem->getChild(elemName);
            }
            std::shared_ptr<EnvironmentNode> pElementNode = std::make_shared<EnvironmentNode>(pSchemaItem, elemName, pEnvNode);
            pElementNode->setId(getUniqueKey());
            addPath(pElementNode);
            parse(it->second, pSchemaItem, pElementNode);
            pEnvNode->addChild(pElementNode);
        }
    }
}


void XMLEnvironmentMgr::serialize(pt::ptree &envTree, std::shared_ptr<EnvironmentNode> &pEnvNode) const
{
    std::vector<std::shared_ptr<EnvironmentValue>> attributes;
    pEnvNode->getAttributes(attributes);
    for (auto attrIt = attributes.begin(); attrIt != attributes.end(); ++attrIt)
    {
        envTree.put("<xmlattr>." + (*attrIt)->getName(), (*attrIt)->getValue());
    }
    std::shared_ptr<EnvironmentValue> pNodeValue = pEnvNode->getLocalEnvValue();
    if (pNodeValue)
    {
        envTree.put_value(pNodeValue->getValue());
    }
    std::vector<std::shared_ptr<EnvironmentNode>> children;
    pEnvNode->getChildren(children);
    for (auto childIt = children.begin(); childIt != children.end(); ++childIt)
    {
        pt::ptree nodeTree;
        serialize(nodeTree, *childIt);
        envTree.add_child((*childIt)->getName(), nodeTree);
    }
}