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

#include "EnvironmentEventHandlers.hpp"
#include "EnvironmentNode.hpp"
#include "EnvironmentValue.hpp"


void MatchEnvironmentEventHandler::processEvent(const std::string &eventType, std::shared_ptr<EnvironmentNode> pEventNode)
{
    if (m_eventType == eventType && pEventNode->getSchemaItem()->getItemType() == m_itemType)
    {
        //
        // If an event node attribute was defined, go check it
        if (!m_eventNodeAttribute.empty())
        {
            //
            // We need to check aginst an attribute in the event node. Build a list of comparison nodes using the
            // target path
            std::vector<std::shared_ptr<EnvironmentNode>> matchNodes;
            pEventNode->fetchNodes(m_targetPath, matchNodes);
            for (auto &nodeIt: matchNodes)
            {
                std::shared_ptr<EnvironmentValue> pItemAttr = pEventNode->getAttribute(m_targetAttribute);
                if (pItemAttr)
                {
                    std::shared_ptr<EnvironmentValue> pMatchAttr =  nodeIt->getAttribute(m_targetAttribute);
                    if (pMatchAttr)
                    {
                        if (pMatchAttr->getValue() == pItemAttr->getValue())
                        {
                            doHandleEvent(pEventNode);
                        }
                    }
                }
            }
        }
        else
        {
            doHandleEvent(pEventNode);
        }
    }
}


void MatchEnvironmentEventHandler::setEventNodeAttributeName(const std::string &name)
{
    m_eventNodeAttribute = name;
    if (m_targetAttribute.empty())
    {
        m_targetAttribute = name;
    }
}


void AttributeDependencyCreateEventHandler::addDependency(const std::string &attrName, const std::string &attrVal, const std::string &depAttr, const std::string &depVal)
{
    auto valSetMapIt = m_depAttrVals.find(attrName);
    if (valSetMapIt == m_depAttrVals.end())
    {
        std::map<std::string, std::pair<std::string, std::string>> keyValMap;
        m_depAttrVals[attrName] = keyValMap;
    }

    m_depAttrVals[attrName][attrVal] = std::pair<std::string, std::string>(depAttr, depVal);
}


void AttributeDependencyCreateEventHandler::doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode)
{
    for (auto attrIt = m_depAttrVals.begin(); attrIt != m_depAttrVals.end(); ++attrIt)
    {
        std::shared_ptr<EnvironmentValue> pAttr = pEventNode->getAttribute(attrIt->first);
        if (pAttr && pAttr->getSchemaValue()->getType()->isEnumerated())
        {
            for (auto valueIt = attrIt->second.begin(); valueIt != attrIt->second.end(); ++valueIt)
            {
                pAttr->getSchemaValue()->getType()->getLimits()->addDependentAttributeValue(valueIt->first, valueIt->second.first, valueIt->second.second);
            }
        }
    }
}


void InsertEnvironmentDataCreateEventHandler::doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode)
{
    pEventNode->addEnvironmentInsertData(m_envData);
}


void AttributeSetValueCreateEventHandler::addAttributeValue(const std::string &attrName, const std::string &attrVal)
{
    m_attrVals.push_back(NameValue(attrName, attrVal));
}


void AttributeSetValueCreateEventHandler::doHandleEvent(std::shared_ptr<EnvironmentNode> pEventNode)
{

    for (auto &attrValPair : m_attrVals)
    {
        std::shared_ptr<EnvironmentValue> pAttr = pEventNode->getAttribute(attrValPair.name);
        if (pAttr)
        {
            pAttr->setValue(attrValPair.value, nullptr);
        }
    }
}
