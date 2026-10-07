/*##############################################################################

    Copyright (C) 2026 HPCC Systems®.

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

#include "eventconsumption.h"
#include "eventindex.hpp"
#include "jhconst.hpp"
#include <limits>

namespace
{
static_assert(NodeSearchFlagsMask <= std::numeric_limits<byte>::max(),
    "Traceable SearchFlags must fit in the event SearchFlags byte");

constexpr IndexSearchFlagInfo searchFlagInfos[] = {
    { EvExtAttrSearchAllKeyed, NodeSearchAllKeyed, "SearchFlags.AllKeyed" },
    { EvExtAttrSearchSingleValue, NodeSearchSingleValue, "SearchFlags.SingleValue" },
    { EvExtAttrSearchUnfiltered, NodeSearchUnfiltered, "SearchFlags.Unfiltered" },
    { EvExtAttrSearchCount, NodeSearchCount, "SearchFlags.Count" },
};
}

unsigned queryIndexSearchFlagCount()
{
    return sizeof(searchFlagInfos) / sizeof(searchFlagInfos[0]);
}

const IndexSearchFlagInfo* queryIndexSearchFlagInfoByIndex(unsigned index)
{
    return index < queryIndexSearchFlagCount() ? &searchFlagInfos[index] : nullptr;
}

const IndexSearchFlagInfo* queryIndexSearchFlagInfo(unsigned attrId)
{
    for (const auto& info : searchFlagInfos)
    {
        if (info.attrId == attrId)
            return &info;
    }
    return nullptr;
}

const IndexSearchFlagInfo* queryIndexSearchFlagInfo(const char* name)
{
    for (const auto& info : searchFlagInfos)
    {
        if (strieq(info.name, name))
            return &info;
    }
    return nullptr;
}
