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


#ifndef _CONFIG2_CONFIGINSERTABLE_ITEMS_HPP_
#define _CONFIG2_CONFIGINSERTABLE_ITEMS_HPP_

#include "SchemaItem.hpp"
#include <vector>

struct InsertItemLimitChoice
{
    InsertItemLimitChoice(const std::string &item, const std::string &attrName, const std::string &attrVal) :
        itemName(item), attributeName(attrName), attributeValue(attrVal) { }
    std::string itemName;
    std::string attributeName;
    std::string attributeValue;
};


struct InsertableItem
{
    InsertableItem(const std::shared_ptr<SchemaItem> &pSchemaItem);
    std::shared_ptr<SchemaItem> m_pSchemaItem;
    std::vector<InsertItemLimitChoice> m_itemLimits;
};

#endif
