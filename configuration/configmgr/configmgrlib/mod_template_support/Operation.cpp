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

#include "Operation.hpp"
#include "TemplateExecutionException.hpp"
#include "EnvironmentNode.hpp"
#include "EnvironmentValue.hpp"


void Operation::addAttribute(modAttribute &newAttribute)
{
    m_attributes.emplace_back(newAttribute);
}


bool Operation::execute(EnvironmentMgr *pEnvMgr, Variables *pInputs)
{
    bool rc = true;
    size_t count, startIndex;

    pInputs->setInputIndex(0);

    //
    // Determine the count of iterations to do
    std::string countStr = pInputs->doValueSubstitution(m_count);
    try {
        count = std::stoul(countStr);
    } catch (...) {
        throw TemplateExecutionException("Non-numeric count found for count");
    }

    //
    // Determine the starting index
    std::string startIdxStr = pInputs->doValueSubstitution(m_startIndex);
    try {
        startIndex = std::stoul(startIdxStr);
    } catch (...) {
        throw TemplateExecutionException("Non-numeric count found for start index");
    }

    //
    // Now execute the operation count times
    for (size_t idx=0; idx < count; ++idx)
    {
        pInputs->setInputIndex(startIndex + idx);
        assignAttributeCookedValues(pInputs);
        doExecute(pEnvMgr, pInputs);
    }
    return rc;
}


void Operation::assignAttributeCookedValues(Variables *pInputs)
{
    //
    // go through the ones defined by the operation and set each (by name)
    for (auto &attr: m_attributes)
    {
        attr.cookedValue = pInputs->doValueSubstitution(attr.value);
    }
}


void Operation::getParentNodeIds(EnvironmentMgr *pEnvMgr, Variables *pInputs)
{
    //
    // Find the parent node(s). Either was input, or based on a path, which may match more than one node
    if (!m_parentNodeId.empty())
    {
        std::shared_ptr<Variable> pInput = pInputs->getVariable(m_parentNodeId);
        if (pInput)
        {
            std::size_t numIds = pInput->getNumValues();
            for (std::size_t idx = 0; idx < numIds; ++idx)
            {
                m_parentNodeIds.emplace_back(pInputs->doValueSubstitution(pInput->getValue(idx)));
            }
        }
        else
        {
            m_parentNodeIds.emplace_back(pInputs->doValueSubstitution(m_parentNodeId));
        }
    }
    else
    {
        std::vector<std::shared_ptr<EnvironmentNode>> envNodes;
        std::string path = pInputs->doValueSubstitution(m_path);
        pEnvMgr->fetchNodes(path, envNodes);
        for (auto &envNode: envNodes)
            m_parentNodeIds.emplace_back(envNode->getId());
    }
}


std::shared_ptr<Variable> Operation::createInput(std::string inputName, const std::string &inputType, Variables *pInputs, bool existingOk)
{
    std::shared_ptr<Variable> pInput;

    pInput = pInputs->getVariable(inputName, false);

    if (!pInput)
    {
        pInput = variableFactory(inputType, inputName);
        pInputs->add(pInput);
    }
    else if (!existingOk)
    {
        std::string msg = "Attempt to create input value that already exists: " + inputName;
        throw TemplateExecutionException(msg);
    }
    return pInput;
}


bool Operation::createAttributeSaveInputs(Variables *pInputs)
{
    bool rc = false;
    for (auto &attr: m_attributes)
    {
        //
        // If this is a saved attribute value, make sure the input exists
        if (!attr.saveValue.empty())
        {
            createInput(attr.saveValue, "string", pInputs, attr.duplicateSaveValueOk);
            rc = true;
        }
    }
    return rc;
}


void Operation::saveAttributeValues(Variables *pInputs, const std::shared_ptr<EnvironmentNode> &pEnvNode)
{
    for (auto &attr: m_attributes)
    {
        if (!attr.saveValue.empty())
        {
            bool found=false, set=false;
            std::shared_ptr<Variable> pInput = pInputs->getVariable(attr.saveValue);
            std::size_t numNames = attr.getNumNames();
            for (std::size_t idx=0; idx<numNames; ++idx)
            {
                auto pAttr = pEnvNode->getAttribute(attr.getName(idx));
                if (pAttr)
                {
                    found = true;
                    std::string attrValue = pAttr->getValue();
                    if (!attrValue.empty())
                    {
                        pInput->addValue(pAttr->getValue());
                        set = true;
                        break;
                    }
                }
            }

            if (attr.errorIfNotFound && !found)
            {
                throw TemplateExecutionException("Unable to find attribute starting with name=" + attr.getName());
            }

            if (attr.errorIfEmpty && !set)
            {
                throw TemplateExecutionException("No value found for attribute starting with name=" + attr.getName());
            }
        }
    }
}
