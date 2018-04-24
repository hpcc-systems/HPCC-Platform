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

InsertableItem::InsertableItem(const std::shared_ptr<SchemaItem> &pSchemaItem) :
    m_pSchemaItem(pSchemaItem)
{

    std::string insertLimitType = pSchemaItem->getProperty("insertLimitType");
    if (!insertLimitType.empty())
    {
        if (insertLimitType == "attribute")
        {
            std::string attributeName = pSchemaItem->getProperty("insertLimitData");
            std::shared_ptr<SchemaValue> pSchemaValue = pSchemaItem->getAttribute(attributeName);
            std::vector<AllowedValue> allowedValues;
            pSchemaValue->getAllowedValues(allowedValues);
            for (auto &&av : allowedValues)
            {
                m_itemLimits.push_back(InsertItemLimitChoice(av.m_value, attributeName, av.m_value));
            }
        }
    }
}
