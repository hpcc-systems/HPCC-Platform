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

#include "EnvironmentNode.hpp"
#include "Exceptions.hpp"
#include "Utils.hpp"

void EnvironmentNode::addChild(std::shared_ptr<EnvironmentNode> pNode)
{
    m_children.insert(std::make_pair(pNode->getName(), pNode));
}


bool EnvironmentNode::removeChild(const std::shared_ptr<EnvironmentNode> pNode)
{
    bool removed = false;
    for (auto it=m_children.begin(); it!= m_children.end() && !removed; ++it)
    {
        if (pNode == it->second)
        {
            m_children.erase(it);
            removed = true;
        }
    }
    return removed;
}


bool EnvironmentNode::addAttribute(const std::string &name, std::shared_ptr<EnvironmentValue> pValue)
{
    auto retValue = m_attributes.insert(std::make_pair(name, pValue));
    return retValue.second;
}


void EnvironmentNode::getChildren(std::vector<std::shared_ptr<EnvironmentNode>> &childNodes, const std::string &name) const
{
    if (name.empty())
    {
        for (auto nodeIt = m_children.begin(); nodeIt != m_children.end(); ++nodeIt)
        {
            childNodes.push_back(nodeIt->second);
        }
    }
    else
    {
        auto rangeIt = m_children.equal_range(name);
        for (auto it = rangeIt.first; it != rangeIt.second; ++it)
        {
            childNodes.push_back(it->second);
        }
    }
}


std::shared_ptr<EnvironmentNode> EnvironmentNode::getParent() const
{
    std::shared_ptr<EnvironmentNode> pParent;
    if (!m_pParent.expired())
    {
        pParent = m_pParent.lock();
    }
    return pParent;
}


void EnvironmentNode::getAttributes(std::vector<std::shared_ptr<EnvironmentValue>> &attrs) const
{
    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attrs.push_back(attrIt->second);
    }
}


void EnvironmentNode::addMissingAttributesFromConfig()
{
    std::vector<std::shared_ptr<SchemaValue>> configuredAttributes;
    m_pSchemaItem->getAttributes(configuredAttributes);

    //
    // go through all the configured attrubutes and for each that is not present in our list, add it
    for (auto it = configuredAttributes.begin(); it != configuredAttributes.end(); ++it)
    {
        auto attrIt = m_attributes.find((*it)->getName());
        if (attrIt == m_attributes.end())
        {
            std::shared_ptr<SchemaValue> pCfgValue = *it;
            std::shared_ptr<EnvironmentValue> pEnvValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, pCfgValue->getName());
            pCfgValue->addEnvironmentValue(pEnvValue);
            addAttribute(pCfgValue->getName(), pEnvValue);
        }
    }

}


void EnvironmentNode::setAttributeValues(const std::vector<NameValue> &values, Status &status, bool allowInvalid, bool forceCreate)
{
    for (auto it = values.begin(); it != values.end(); ++it)
    {
        setAttributeValue((*it).name, (*it).value, status, allowInvalid, forceCreate);
    }
}


void EnvironmentNode::setAttributeValue(const std::string &attrName, const std::string &value, Status &status, bool allowInvalid, bool forceCreate)
{
    std::shared_ptr<EnvironmentValue> pEnvValue;

    auto it = m_attributes.find(attrName);
    if (it != m_attributes.end())
    {
        pEnvValue = it->second;
    }

    //
    // Not found on this node. See if the configuration defines the attribute. If so, set the value and move on.
    // If not and the forceCreate flag is set, create it.
    else if (forceCreate)
    {
        std::shared_ptr<SchemaValue> pCfgValue = m_pSchemaItem->getAttribute(attrName);
        pEnvValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, attrName);
        addAttribute(attrName, pEnvValue);
        if (!pCfgValue->isDefined())
        {
            status.addMsg(statusMsg::warning, getId(), attrName, "Undefined attribute did not exist in configuration, was created");
        }
    }

    if (pEnvValue)
    {
        pEnvValue->setValue(value, &status, allowInvalid);
    }
    else
    {
        status.addMsg(statusMsg::error, getId(), attrName, "The attribute does not exist and was not created");
    }

}


std::string EnvironmentNode::getAttributeValue(const std::string &name) const
{
    std::string value;
    std::shared_ptr<EnvironmentValue> pAttribute = getAttribute(name);
    if (pAttribute)
        value = pAttribute->getValue();
    return value;
}


bool EnvironmentNode::setLocalValue(const std::string &newValue, Status &status, bool force)
{
    bool rc = false;

    //
    // If no environment value is present, create one first
    if (!m_pLocalValue)
    {
        std::shared_ptr<SchemaValue> pCfgValue = m_pSchemaItem->getItemSchemaValue();
        m_pLocalValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, "");  // node's value has no name
    }

    rc = m_pLocalValue->setValue(newValue, &status, force);
    return rc;
}


std::string EnvironmentNode::getLocalValue() const
{
    std::string value;
    if (m_pLocalValue)
    {
        value = m_pLocalValue->getValue();
    }
    return value;
}


void EnvironmentNode::validate(Status &status, bool includeChildren, bool includeHiddenNodes) const
{
    if (!m_pSchemaItem->isHidden() || includeHiddenNodes)
    {
        //
        // Check node value
        if (m_pLocalValue)
        {
            m_pLocalValue->validate(status, m_id);
        }

        //
        // Check any attributes
        for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
        {
            attrIt->second->validate(status, m_id);

            //
            // If this value must be unique, make sure it is
            if (attrIt->second->getSchemaValue()->isUniqueValue())
            {
                bool found = false;
                std::vector<std::string> allValues;
                attrIt->second->getAllValuesForSiblings(allValues);
                std::set<std::string> unquieValues;
                for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
                {
                    auto ret = unquieValues.insert(*it);
                    found = !ret.second;
                }

                if (found)
                {
                    status.addUniqueMsg(statusMsg::error, m_id, attrIt->second->getName(), "Attribute value must be unique");
                }
            }

            //
            // Does this value need to be from another set of values?
            if (attrIt->second->getSchemaValue()->isFromUniqueValueSet())
            {
                bool found = false;
                std::vector<std::string> allValues;
                attrIt->second->getSchemaValue()->getAllKeyRefValues(allValues);
                for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
                    found = *it == attrIt->second->getValue();
                if (!found)
                {
                    status.addMsg(statusMsg::error, m_id, attrIt->second->getName(), "Attribute value must be from a unique set");
                }
            }
        }

        //
        // Now check all children
        if (includeChildren)
        {
            for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
            {
                childIt->second->validate(status, includeChildren, includeHiddenNodes);
            }
        }
    }
}


void EnvironmentNode::getAttributeValueForAllSiblings(const std::string &attrName, std::vector<std::string> &result) const
{
    std::shared_ptr<EnvironmentNode> pParentNode = m_pParent.lock();
    if (pParentNode)
    {
        std::vector<std::shared_ptr<EnvironmentNode>> nodes;
        pParentNode->getChildren(nodes, m_name);
        for (auto it = nodes.begin(); it != nodes.end(); ++it)
        {
            result.push_back((*it)->getAttributeValue(attrName));
        }
    }
}


const std::shared_ptr<EnvironmentValue> EnvironmentNode::getAttribute(const std::string &name) const
{
    std::shared_ptr<EnvironmentValue> pValue;
    auto it = m_attributes.find(name);
    if (it != m_attributes.end())
    {
        pValue = it->second;
    }
    return pValue;
}


void EnvironmentNode::getInsertableItems(std::vector<InsertableItem> &insertableItems) const
{
    std::map<std::string, unsigned> childCounts;

    //
    // Iterate over the children and for each, create a childCount entry based on the
    // child node's configuration type
    for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
    {
        std::string itemType = childIt->second->getSchemaItem()->getItemType();
        auto findIt = childCounts.find(itemType);
        if (findIt != childCounts.end())
        {
            ++findIt->second;  // increment the number of instances of this item type.
        }
        else
        {
            childCounts.insert({ itemType, 1 });
        }
    }

    //
    // Now get the full list of configurable items, then resolve it against the child counts from
    // above to build a vector of insertable items
    std::vector<std::shared_ptr<SchemaItem>> configChildren;
    m_pSchemaItem->getChildren(configChildren);
    for (auto cfgIt = configChildren.begin(); cfgIt != configChildren.end(); ++cfgIt)
    {
        auto findIt = childCounts.find((*cfgIt)->getItemType());
        if (findIt != childCounts.end())
        {
            if (findIt->second < (*cfgIt)->getMaxInstances())
            {
                insertableItems.push_back(InsertableItem(shared_from_this(), *cfgIt));
            }
        }
        else
        {
            insertableItems.push_back(InsertableItem(shared_from_this(), *cfgIt));
        }
    }
}


//
// Called to initialize a newly added node to the environment (not just read from the environment)
void EnvironmentNode::initialize()
{
    //
    // Add missing attributes
    addMissingAttributesFromConfig();

    //
    // If we are a component and there is a buildSet attribute, set the value to the configItem's type
    if (!(m_pSchemaItem->getProperty("componentName").empty())  && hasAttribute("buildSet"))
    {
        Status status;
        setAttributeValue("buildSet", m_pSchemaItem->getProperty("componentName"), status);
    }

    //
    // Initilize each attribute
    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attrIt->second->initialize();
    }
}


void EnvironmentNode::fetchNodes(const std::string &path, std::vector<std::shared_ptr<EnvironmentNode>> &nodes) const
{
    //
    // If path starts with / and we are not the root, get the root and do the find
    if (path[0] == '/')
    {
        std::string remainingPath = path.substr(1);
        std::string rootName = remainingPath;
        std::shared_ptr<const EnvironmentNode> pRoot = getRoot();
        size_t slashPos = path.find_first_of('/', 1);
        if (slashPos != std::string::npos)
        {
            rootName = path.substr(1, slashPos-1);
            remainingPath = path.substr(slashPos + 1);
            size_t atPos = rootName.find_first_of('@');
            if (atPos != std::string::npos)
            {
                rootName.erase(atPos, std::string::npos);
            }
        }

        if (pRoot->getName() == rootName)
        {
            pRoot->fetchNodes(remainingPath, nodes);
        }
    }
    else if (path[0] == '.')
    {
        //
        // Parent ?
        if (path[1] == '.')
        {
            //
            // Path must be at least 4 characters in length to support the leading ../<remaining path>
            if (!m_pParent.expired() && path.length() >= 4)
            {
                m_pParent.lock()->fetchNodes(path.substr(3), nodes);  // note skipping over '..'
            }
            else
            {
                throw new ParseException("Attempt to navigate to parent with no parent or path is incomplete");
            }
        }
        else
        {
            fetchNodes(path.substr(1), nodes); // do the find from here stripping the '.' indicator
        }
    }

    //
    // Otherwise, start searching
    else
    {
        std::string nodeName = path;
        std::string remainingPath, searchAttrName, searchAttrValue;

        //
        // Get our portion of the path which is up to the next / or the remaining string and
        // set the remaining portion of the path to search
        size_t slashPos = nodeName.find_first_of('/');
        if (slashPos != std::string::npos)
        {
            remainingPath = nodeName.substr(slashPos + 1);
            nodeName.erase(slashPos, std::string::npos);  // truncate
        }

        //
        // Any attributes we need to look for?
        size_t atPos = nodeName.find_first_of('@');
        if (atPos != std::string::npos)
        {
            searchAttrName = nodeName.substr(atPos + 1);
            nodeName.erase(atPos, std::string::npos);
            size_t equalPos = searchAttrName.find_first_of('=');
            if (equalPos != std::string::npos)
            {
                searchAttrValue = searchAttrName.substr(equalPos + 1);
                searchAttrName.erase(equalPos, std::string::npos);
            }
        }

        //
        // Now search the children for nodes matching the node name
        std::vector<std::shared_ptr<EnvironmentNode>> childNodes;
        getChildren(childNodes, nodeName);

        //
        // If there is an attribute specified, dig deeper
        if (!searchAttrName.empty())
        {
            auto childNodeIt = childNodes.begin();
            while (childNodeIt != childNodes.end())
            {
                std::shared_ptr<EnvironmentValue> pValue = (*childNodeIt)->getAttribute(searchAttrName);
                if (pValue)
                {
                    //
                    // The attribute value must be present and, if necessary, must match the search value
                    std::string curAttrValue = pValue->getValue();
                    if (!curAttrValue.empty() && (searchAttrValue.empty() || (searchAttrValue == curAttrValue)))
                    {
                        ++childNodeIt;  // keep it
                    }
                    else
                    {
                        childNodeIt = childNodes.erase(childNodeIt);
                    }
                }
                else
                {
                    childNodeIt = childNodes.erase(childNodeIt);
                }
            }
        }

        //
        // If there is path remaining, call the children in childNodes with the remaining path, otherwise we've reached
        // the end. Whatever nodes are in childNodes are appended to the input nodes vector for return
        if (!remainingPath.empty())
        {
            for (auto childNodeIt = childNodes.begin(); childNodeIt != childNodes.end(); ++childNodeIt)
            {
                (*childNodeIt)->fetchNodes(remainingPath, nodes);
            }
        }
        else
        {
            nodes.insert(nodes.end(), childNodes.begin(), childNodes.end());
        }
    }
}


std::shared_ptr<const EnvironmentNode> EnvironmentNode::getRoot() const
{
    if (!m_pParent.expired())
    {
        return m_pParent.lock()->getRoot();
    }
    std::shared_ptr <const EnvironmentNode> ptr = shared_from_this();
    return ptr;
}
