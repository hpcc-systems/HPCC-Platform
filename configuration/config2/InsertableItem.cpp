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

#include "InsertableItem.hpp"
#include "EnvironmentNode.hpp"


InsertableItem::InsertableItem(std::shared_ptr<const EnvironmentNode> pParentEnvNode, const std::shared_ptr<SchemaItem> &pSchemaItem) :
    m_pParentEnvNode(pParentEnvNode), m_pSchemaItem(pSchemaItem), m_limitChoices(false)
{
    std::string insertLimitType = m_pSchemaItem->getProperty("insertLimitType");
    if (!insertLimitType.empty())
    {
        m_limitChoices = true;
        if (insertLimitType == "attribute")
        {
            std::string attributeName = m_pSchemaItem->getProperty("insertLimitData");
            std::shared_ptr<SchemaValue> pSchemaValue = m_pSchemaItem->getAttribute(attributeName);
            std::vector<AllowedValue> allowedValues;
            pSchemaValue->getAllowedValues(allowedValues, m_pParentEnvNode);
            for (auto &av : allowedValues)
            {
                m_itemLimits.push_back(InsertItemLimitChoice(av.m_value, attributeName, av.m_value));
            }
        }
    }
}
