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

#pragma once

#include "eventconsumption.h"
#include "eventmetaparser.hpp"
#include "jevent.hpp"
#include <algorithm>
#include <vector>
#include <string>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>

/**
 * @interface IGroupFormatter
 * @brief Interface for formatting and outputting hierarchical grouping reports.
 *
 * Provides callbacks for the reporting lifecycle (begin/end report, begin/end group)
 * as well as for outputting the specific leaf-level accumulator and group-level
 * subtotal accumulator states.
 *
 * @tparam TLeafAccumulator Type tracking accumulated metrics at the leaf level.
 * @tparam TSubtotalAccumulator Type tracking accumulated metrics for intermediate group subtotals.
 */
template <typename TLeafAccumulator, typename TSubtotalAccumulator = TLeafAccumulator>
class IGroupFormatter
{
public:
    virtual ~IGroupFormatter() = default;
    virtual void beginReport(const std::vector<std::vector<std::string>>& groupColumns) = 0;
    virtual void beginGroup(size_t level, const std::vector<std::string>& groupValues) = 0;
    virtual void outputLeafSummary(const std::vector<std::string>& groupValues, const TLeafAccumulator& metrics) = 0;
    virtual void outputSubtotal(size_t level, const std::vector<std::string>& groupValues, const TSubtotalAccumulator& metrics) = 0;
    virtual void endGroup(size_t level, const std::vector<std::string>& groupValues) = 0;
    virtual void endReport() = 0;
};

enum EventGroupExtAttr : unsigned
{
    EvExtAttrLogicalFileName = EvAttrMax + 1
};

/**
 * @struct GroupAttribute
 * @brief Describes a single grouping attribute, including any scaling or bucketing modifier.
 */
struct GroupAttribute
{
    unsigned attrId;            // e.g., EvAttrFileOffset
    EventAttrUnit unit;         // e.g., EAUsize
    __uint64 bucketScale = 1;   // The divisor for bucketing, defaults to 1 (raw value)
    bool isBucket = false;      // True if a user-supplied bucket modifier was parsed
};

/**
 * @class GroupAttributeExtractor
 * @brief Utility for resolving the string value of grouping attributes.
 *
 * Extracts values from events or looks them up from the current metadata state
 * (e.g., resolving a logical filename or service name that corresponds to the event).
 */
class event_decl GroupAttributeExtractor
{
public:
    static GroupAttribute parseAttribute(const char* attrDesc);
    static bool isApplicable(const GroupAttribute& attr, const CEvent& event);
    static std::string getValue(const GroupAttribute& attr, const CEvent& event, const CMetaInfoState* metaState);
    static std::string formatValue(const GroupAttribute& attr, const std::string& rawValue);
    static __uint64 getHash(const std::vector<GroupAttribute>& attrs, const CEvent& event, const CMetaInfoState* metaState);
    static bool isEqual(const std::vector<GroupAttribute>& attrs, const CEvent& event, const CMetaInfoState* metaState, const std::vector<std::string>& groupValues);
private:
    static const char* resolveStringAttribute(EventAttr attr, const CEvent& event, const CMetaInfoState* metaState);
};

/**
 * @class CGroupNode
 * @brief Represents a single group level in the hierarchical grouping tree.
 *
 * Recursively accumulates event data based on a defined grouping hierarchy.
 * Events are accumulated both into the current level's subtotal and passed
 * down to the appropriate child node based on extracted attribute values.
 * When the bottom of the hierarchy is reached, events are accumulated into
 * the leaf summary.
 *
 * @tparam TLeafAccumulator Type tracking accumulated metrics at the leaf level.
 * @tparam TSubtotalAccumulator Type tracking accumulated metrics for intermediate group subtotals.
 */
template <typename TLeafAccumulator, typename TSubtotalAccumulator = TLeafAccumulator>
class CGroupNode
{
public:
    TSubtotalAccumulator subTotal;
    std::unordered_multimap<__uint64, std::unique_ptr<CGroupNode<TLeafAccumulator, TSubtotalAccumulator>>> children;
    std::vector<std::string> groupValues;
    std::optional<TLeafAccumulator> leafSummary;

    void process(const CEvent& event, const CMetaInfoState* metaState,
                 const std::vector<std::vector<GroupAttribute>>& hierarchy, size_t currentLevel, size_t maxApplicableDepth)
    {
        subTotal.accumulate(event, metaState);

        if (currentLevel >= hierarchy.size())
        {
            if (!leafSummary)
                leafSummary.emplace();
            leafSummary->accumulate(event, metaState);
            return;
        }

        if (currentLevel >= maxApplicableDepth)
            return;

        __uint64 hash = GroupAttributeExtractor::getHash(hierarchy[currentLevel], event, metaState);
        auto range = children.equal_range(hash);
        auto it = range.first;
        for (; it != range.second; ++it)
        {
            if (GroupAttributeExtractor::isEqual(hierarchy[currentLevel], event, metaState, it->second->groupValues))
                break;
        }

        if (it == range.second)
        {
            auto newNode = std::make_unique<CGroupNode>();
            for (const GroupAttribute& attr : hierarchy[currentLevel])
                newNode->groupValues.push_back(GroupAttributeExtractor::getValue(attr, event, metaState));
            it = children.emplace(hash, std::move(newNode));
        }

        it->second->process(event, metaState, hierarchy, currentLevel + 1, maxApplicableDepth);
    }

    void render(IGroupFormatter<TLeafAccumulator, TSubtotalAccumulator>& formatter, const std::vector<std::string>& parentGroupValues, const std::vector<std::vector<GroupAttribute>>& hierarchy, size_t currentLevel, bool isRoot = false) const
    {
        std::vector<std::string> formattedGroupValues;
        const std::vector<std::string>& myGroupValues = isRoot ? parentGroupValues : this->groupValues;

        if (isRoot)
        {
            formattedGroupValues = myGroupValues;
        }
        else
        {
            for (size_t i = 0; i < myGroupValues.size() && i < hierarchy[currentLevel - 1].size(); ++i)
                formattedGroupValues.push_back(GroupAttributeExtractor::formatValue(hierarchy[currentLevel - 1][i], myGroupValues[i]));
        }

        if (!isRoot)
            formatter.beginGroup(currentLevel, formattedGroupValues);

        std::vector<const CGroupNode<TLeafAccumulator, TSubtotalAccumulator>*> orderedChildren;
        orderedChildren.reserve(children.size());
        for (const auto& [hash, childNode] : children)
            orderedChildren.push_back(childNode.get());

        std::sort(orderedChildren.begin(), orderedChildren.end(),
            [&hierarchy, currentLevel](const auto* a, const auto* b)
            {
                for (size_t i = 0; i < a->groupValues.size() && i < hierarchy[currentLevel].size(); ++i)
                {
                    if (a->groupValues[i] != b->groupValues[i])
                    {
                        EventAttr attr = (EventAttr)hierarchy[currentLevel][i].attrId;
                        if (attr < EvAttrMax)
                        {
                            switch (queryEventAttributeType(attr))
                            {
                            case EATu1:
                            case EATu2:
                            case EATu4:
                            case EATu8:
                            {
                                char* endA = nullptr;
                                char* endB = nullptr;
                                __uint64 valA = strtoull(a->groupValues[i].c_str(), &endA, 10);
                                __uint64 valB = strtoull(b->groupValues[i].c_str(), &endB, 10);
                                if (endA != a->groupValues[i].c_str() && endB != b->groupValues[i].c_str())
                                    return valA < valB;
                                break;
                            }
                            default:
                                break;
                            }
                        }
                        return a->groupValues[i] < b->groupValues[i];
                    }
                }
                return false;
            });

        for (const auto* childNode : orderedChildren)
            childNode->render(formatter, formattedGroupValues, hierarchy, currentLevel + 1, false);

        if (leafSummary)
            formatter.outputLeafSummary(formattedGroupValues, leafSummary.value());

        if (!children.empty() || (isRoot && !leafSummary))
        {
            if (isRoot)
                formatter.outputSubtotal(currentLevel, {"TOTAL"}, subTotal);
            else
                formatter.outputSubtotal(currentLevel, formattedGroupValues, subTotal);
        }

        if (!isRoot)
            formatter.endGroup(currentLevel, formattedGroupValues);
    }
};
