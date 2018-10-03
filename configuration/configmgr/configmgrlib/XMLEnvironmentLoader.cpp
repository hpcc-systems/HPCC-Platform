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

#include "XMLEnvironmentLoader.hpp"
#include "XSDSchemaParser.hpp"
#include "Exceptions.hpp"


std::vector<std::shared_ptr<EnvironmentNode>> XMLEnvironmentLoader::load(std::istream &in, const std::shared_ptr<SchemaItem> &pSchemaItem) const
{
    std::vector<std::shared_ptr<EnvironmentNode>> envNodes;
    std::shared_ptr<EnvironmentNode> pEnvNode;

    pt::ptree envTree;
    try
    {
        pt::read_xml(in, envTree, pt::xml_parser::trim_whitespace | pt::xml_parser::no_comments);
        //auto rootIt = envTree.begin();

        // if root, want to start with rootIt, but auto match rootIt first with schemaItem, then parse on down, but want to return
        // envNode for the root

        // if not root, to through rootIt with schema item as parent for each elemement, return envNode of each item iterated

        std::shared_ptr<SchemaItem> pParseRootSchemaItem;
        for (auto envIt = envTree.begin(); envIt != envTree.end(); ++envIt)
        {
            if (envIt->first == pSchemaItem->getProperty("name"))
            {
                pParseRootSchemaItem = pSchemaItem;
            }
            else
            {
                std::vector<std::shared_ptr<SchemaItem>> children;
                pSchemaItem->getChildren(children, envIt->first);
                if (children.empty())
                {
                    throw (ParseException("Unable to start parsing environment, root node element " + envIt->first + " not found"));
                }
                pParseRootSchemaItem = children[0];
            }

            pEnvNode = std::make_shared<EnvironmentNode>(pParseRootSchemaItem, envIt->first);  // caller may need to set the parent
            parse(envIt->second, pParseRootSchemaItem, pEnvNode);
            envNodes.push_back(pEnvNode);
        }
    }
    catch (const std::exception &e)
    {
        std::string xmlError = e.what();
        std::string msg = "Unable to read/parse Environment file. Error = " + xmlError;
        throw (ParseException(msg));
    }
    return envNodes;
}


void XMLEnvironmentLoader::parse(const pt::ptree &envTree, const std::shared_ptr<SchemaItem> &pConfigItem, std::shared_ptr<EnvironmentNode> &pEnvNode) const
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
            if (!pCfgValue)
            {
                pCfgValue = std::make_shared<SchemaValue>("", false);
                pCfgValue->setType(pConfigItem->getSchemaValueType("default"));
                pConfigItem->setItemSchemaValue(pCfgValue);
            }

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
            std::vector<std::shared_ptr<SchemaItem>> children;
            std::shared_ptr<SchemaItem> pSchemaItem;
            pConfigItem->getChildren(children, elemName, typeName);
            if (children.empty())
            {
                pSchemaItem = std::make_shared<SchemaItem>(elemName, "default", pConfigItem);  // default item if none found
            }
            else if (children.size() > 1)
            {
                throw (ParseException("Ambiguous element found during parsing, unable to find schem item, element name = " + elemName + ", itemType = " + typeName));
            }
            else
            {
                pSchemaItem = children[0];
            }

            // If no schema item is found, that's ok, the node just has no defined configuration
            std::shared_ptr<EnvironmentNode> pElementNode = std::make_shared<EnvironmentNode>(pSchemaItem, elemName, pEnvNode);
            parse(it->second, pSchemaItem, pElementNode);
            pEnvNode->addChild(pElementNode);
        }
    }
}
