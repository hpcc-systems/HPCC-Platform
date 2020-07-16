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

#include <set>
#include "ws_workunitsService.hpp"
#include "ws_wudetails.hpp"
#include "jlib.hpp"
#include "workunit.hpp"
#include "jset.hpp"
#include "jstatcodes.h"

typedef std::pair<WuAttr, StringBuffer> AttribValuePair;
bool operator==(const AttribValuePair & p1, const AttribValuePair & p2)
{
    return p1.first==p2.first;
}

class WUDetailsVisitor : public IWuScopeVisitor
{
public:
    WUDetailsVisitor(IConstWUPropertyOptions & _propertyOptions, IConstWUPropertiesToReturn & _propertiesToReturn);
    virtual ~WUDetailsVisitor(){};
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override;
    virtual void noteAttribute(WuAttr attr, const char * value) override;
    virtual void noteHint(const char * kind, const char * value) override;
    virtual void noteException(IConstWUException & exception) override;

    void resetScope();
    void noteScopeType(const StatisticScopeType _sst);
    IArrayOf<IEspWUResponseProperty> & getResponseProperties() { endListAttr(); return EspWUResponseProperties;}
    IArrayOf<IEspWUResponseNote> &getResponseNotes() { endListAttr(); return EspWUResponseNotes;}
    unsigned __int64 getMaxTimestamp() const { return maxTimestamp;}

private:
    static const unsigned StatisticFilterMaskSize = StatisticKind::StMax;
    static const unsigned AttributeFilterMaskSize = WuAttr::WaMax-WaNone;

    bool includeName = false;
    bool includeRawValue = false;
    bool includeFormatted = false;
    bool includeMeasure = false;
    bool includeCreator = false;
    bool includeCreatorType = false;

    bool statisticsPostFilterRequired = false;        // Does statistics have be filtered in the visited method (noteStatistic)
                                                      // Only when true, does the following 3 variables have valid values
    std::set<StatisticKind> extraStatistics[SSTmax];  // Extra statistics returned for a given scope type
    const std::set<StatisticKind> * extraStatisticsForCurScope = nullptr; // Tracks statistics required for current scope type
    Owned<IBitSet> propertiesToReturnStats;           // Statistic to be returned for every scope/scope type


    bool attributesPostFilterRequired = false;         // Does attributes have be filtered in the visited method (noteAttribute)
                                                      // Only when true, does the following 3 variables have valid values
    std::set<WuAttr> extraAttributes[SSTmax];         // Extra attributes returned for a given scope type
    const std::set<WuAttr> * extraAttributesForCurScope = nullptr; // Tracks attributes required for current scope type
    Owned<IBitSet> propertiesToReturnAttribs;         // Attributes to be returned for every scope/scope type

    unsigned __int64 maxTimestamp = 0;
    IArrayOf<IEspWUResponseProperty> EspWUResponseProperties;
    IArrayOf<IEspWUResponseNote> EspWUResponseNotes;
    StatisticScopeType currentStatisticScopeType = SSTnone;

    class MultiListValues
    {
    public:
        void add(WuAttr attr, const char *value)
        {
            AttribValuePair tmp;
            tmp.first = attr;

            auto cur = find(attribValuePairs.begin(), attribValuePairs.end(), tmp);
            if (cur!=attribValuePairs.end())
            {
                cur->second.appendf(", \"%s\"",  value);
            }
            else
            {
                tmp.second.appendf("[\"%s\"",  value);
                attribValuePairs.emplace_back(tmp);
            }
        }

        void flushListAttributes(WUDetailsVisitor & parent)
        {
            for(AttribValuePair & ap: attribValuePairs)
            {
                parent.addAttribToResp(ap.first, ap.second.append("]").str());
            }
            attribValuePairs.clear();
        }
    private:
        std::vector<AttribValuePair> attribValuePairs;
    } multiListValues;

    void addAttribToResp(WuAttr attr, const char * value)
    {
        Owned<IEspWUResponseProperty> EspWUResponseProperty = createWUResponseProperty("","");
        EspWUResponseProperty->setName(queryWuAttributeName(attr));
        if (includeFormatted)
            EspWUResponseProperty->setFormatted(value);
        if (includeRawValue)
            EspWUResponseProperty->setRawValue(value);

        EspWUResponseProperties.append(*EspWUResponseProperty.getClear());
    }

    void endListAttr()
    {
        multiListValues.flushListAttributes(*this);
    }
    bool includeAttribute(WuAttr w);

    void buildAttribListToReturn(IConstWUPropertiesToReturn & propertiesToReturn);
};

WUDetailsVisitor::WUDetailsVisitor(IConstWUPropertyOptions & propertyOptions, IConstWUPropertiesToReturn & propertiesToReturn)
{
    includeName = propertyOptions.getIncludeName();
    includeRawValue = propertyOptions.getIncludeRawValue();
    includeFormatted = propertyOptions.getIncludeFormatted();
    includeMeasure = propertyOptions.getIncludeMeasure();
    includeCreator = propertyOptions.getIncludeCreator();
    includeCreatorType = propertyOptions.getIncludeCreatorType();

    buildAttribListToReturn(propertiesToReturn);
}

void WUDetailsVisitor::noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra)
{
    endListAttr();

    // This section handles the special case where there are Statistics in ExtraProperties.
    // When ExtraProperties are provided, this filter handles both PropertiesToReturn
    // and ExtraProperties. (In all other cases, the scope iterator will filter before visiting
    if (statisticsPostFilterRequired)
    {
        // Check first if the StatisticKind is listed in the main list (propertiesToReturnStats)
        // If there's no match there, check if the StatisticKind is in the scope type specific list
        if (!propertiesToReturnStats->test(kind))
        {
            // If it's not in propertiesToReturnStats, then check the extraStatisticsForCurScope
            if (extraStatisticsForCurScope==nullptr ||
                extraStatisticsForCurScope->find(kind)==extraStatisticsForCurScope->end())
            {
                return; // It is in neither list so filter out
            }
        }
    }

    Owned<IEspWUResponseProperty> EspWUResponseProperty = createWUResponseProperty("","");

    if (includeName)
        EspWUResponseProperty->setName(queryStatisticName(kind));
    if (includeRawValue)
    {
        StringBuffer rawValue;
        rawValue.append(value);
        EspWUResponseProperty->setRawValue(rawValue);
    }
    SCMStringBuffer tmpStr;
    if (includeFormatted)
        EspWUResponseProperty->setFormatted(extra.getFormattedValue(tmpStr).str());
    if (includeMeasure && extra.getMeasure()!=SMeasureNone)
        EspWUResponseProperty->setMeasure(queryMeasureName(extra.getMeasure()));
    if (includeCreator)
        EspWUResponseProperty->setCreator(extra.getCreator(tmpStr).str());
    if (includeCreatorType && extra.getCreatorType()!=SCTnone)
        EspWUResponseProperty->setCreatorType(queryCreatorTypeName(extra.getCreatorType()));

    EspWUResponseProperties.append(*EspWUResponseProperty.getClear());
    if (extra.getTimestamp()>maxTimestamp)
        maxTimestamp = extra.getTimestamp();
}

bool WUDetailsVisitor::includeAttribute(WuAttr attr)
{
    if (propertiesToReturnAttribs->test(attr-WaNone))
        return true;
    if (extraAttributesForCurScope!=nullptr &&
        extraAttributesForCurScope->find(attr)!=extraAttributesForCurScope->end())
        return true;
    return false;
}

void WUDetailsVisitor::noteAttribute(WuAttr attr, const char * value)
{
    // Note: For attributes that may have its values as MultiList, return it as
    // a MultiList only. That is unless the Single kind is specifically requested
    // as there is no reason to provide both.

    // Try to get  the MultiList equivalent.  If it can't, listAttr==WaNone
    // e.g. if attr==WaDefinition, then listAttr==WaDefinitionList
    WuAttr listAttr = getListAttribute(attr);
    // If values may be returned as MultiList (listAttr!=WaNone), set flag to
    // assuming that values will be returned as MultiList
    bool returnAttrAsMultiList = (listAttr!=WaNone);
    bool returnAttrAsSingleValue = true;

    if (attributesPostFilterRequired)
    {
        if (includeAttribute(attr))             // Single item specifically requested
        {
            if (isListAttribute(attr))          // Check to see if attr is multilist type
            {
                returnAttrAsMultiList = true;
                returnAttrAsSingleValue = false;
                listAttr=attr;
            }
            else
                returnAttrAsMultiList = false;  // so don't return as MultiList value
        }
        else
        {
            returnAttrAsSingleValue = false;    // Attribute as single value not selected
            if (returnAttrAsMultiList && !includeAttribute(listAttr))
                return;
        }
    }
    if (!returnAttrAsMultiList && !returnAttrAsSingleValue)
        return;

    StringBuffer encoded;
    encodeXML(value, encoded);

    if (returnAttrAsMultiList)
        multiListValues.add(listAttr,encoded.str());
    else
        addAttribToResp(attr, encoded.str());
}

void WUDetailsVisitor::noteHint(const char * kind, const char * value)
{
    endListAttr();
    Owned<IEspWUResponseProperty> EspWUResponseProperty = createWUResponseProperty("","");

    StringBuffer hint("hint:");
    hint.append(kind);
    EspWUResponseProperty->setName(hint);

    StringBuffer encoded;
    if (includeFormatted || includeRawValue)
    {
        encodeXML(value, encoded);
        value = encoded.str();
    }

    if (includeFormatted)
        EspWUResponseProperty->setFormatted(value);
    if (includeRawValue)
        EspWUResponseProperty->setRawValue(value);

    EspWUResponseProperties.append(*EspWUResponseProperty.getClear());
}
void WUDetailsVisitor::noteException(IConstWUException & exception)
{
    Owned<IEspWUResponseNote> EspWUResponseNote = createWUResponseNote("","");
    SCMStringBuffer source, message;
    exception.getExceptionSource(source);
    exception.getExceptionMessage(message);

    EspWUResponseNote->setSource(source.str());
    EspWUResponseNote->setMessage(message.str());
    if (exception.getExceptionCode())
        EspWUResponseNote->setErrorCode(exception.getExceptionCode());
    else
        EspWUResponseNote->setErrorCode_null();

    EspWUResponseNote->setSeverity(querySeverityString(exception.getSeverity()));

    EspWUResponseNote->setCost(exception.getPriority());
    EspWUResponseNotes.append(*EspWUResponseNote.getClear());
}

// Get StatisticKind or WuAttr from property name and return true
// When neither possible, return false.
static bool getPropertyIdFromName(const char *propName, StatisticKind & sk, WuAttr & wa )
{
    wa = WaNone;
    sk = queryStatisticKind(propName, StKindNone);
    if (sk!=StKindNone)
        return true;

    wa = queryWuAttribute(propName, WaNone);
    if (wa!=WaNone)
        return true;

    return false;
}

void WUDetailsVisitor::buildAttribListToReturn(IConstWUPropertiesToReturn & propertiesToReturn)
{
    bool returnAllStatistic = propertiesToReturn.getAllStatistics();
    bool returnAllAttributes = propertiesToReturn.getAllAttributes();

    if ( (returnAllStatistic && returnAllAttributes) || propertiesToReturn.getAllProperties())
        return;

    // ScopeType specific properties (extra properties specific to given scope types)
    IArrayOf<IConstWUExtraProperties> & extraProperties = propertiesToReturn.getExtraProperties();
    ForEachItemIn(idx, extraProperties)
    {
        IConstWUExtraProperties & cur = extraProperties.item(idx);
        const char * scopeTypeWithAdditionalProps = cur.getScopeType();
        if (!scopeTypeWithAdditionalProps || !*scopeTypeWithAdditionalProps)
            continue;

        StatisticScopeType sst = queryScopeType(scopeTypeWithAdditionalProps, SSTnone);
        if (sst==SSTnone)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid ScopeType (%s) in ExtraProperties",scopeTypeWithAdditionalProps);

        // Generate list of properties for this Scope Type
        const StringArray & props = cur.getProperties();
        ForEachItemIn(idx2, props)
        {
            StatisticKind sk;
            WuAttr wa;
            const char *propName = props[idx2];

            if (!getPropertyIdFromName(propName, sk, wa))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid property name (%s) in ExtraProperties",propName);

            if (sk!=StKindNone)
            {
                if (!returnAllStatistic)
                {
                    extraStatistics[sst].insert(sk);
                    statisticsPostFilterRequired = true;
                }
            }
            else
            {
                if (!returnAllAttributes)
                {
                    extraAttributes[sst].insert(wa);
                    attributesPostFilterRequired = true;
                }
            }
        }
    }

    // Examine propertiesToReturn to see if any thing there will require a post filter
    if (!attributesPostFilterRequired)
    {
        // If single kind list is requested, it must be handled by post filter as the default behaviour
        // is to only return multi list kind. (Scope iterator's filtering is not able to handle this
        // and like with any other filtering that cannot be handled by the scope iterator it must be
        // handled by the post filter. By adding the single kind item to the post filter, noteAttribute
        // is forced to return the single kind.)
        //
        // Note: if attributesPostFilterRequired is set to true, then the second propertiesToReturnList
        // loop is processed to add the single kind list items as well as all other attibutes required.
        StringArray & propertiesToReturnList = propertiesToReturn.getProperties();
        ForEachItemIn(idx1,propertiesToReturnList)
        {
            const char * propName = propertiesToReturnList[idx1];
            WuAttr wa = queryWuAttribute(propName, WaNone);
            if (wa!=WaNone)
            {
                if (getListAttribute(wa)!=WaNone)
                {
                    attributesPostFilterRequired = true;
                    break;
                }
            }
        }
    }
    // Post Filtering (filtering within in visited method) required if statistics or attributes requested in ExtraProperties
    if (!statisticsPostFilterRequired && !attributesPostFilterRequired)
        return;

    if (statisticsPostFilterRequired)
        propertiesToReturnStats.set(createBitSet(StatisticFilterMaskSize));
    if (attributesPostFilterRequired)
        propertiesToReturnAttribs.set(createBitSet(AttributeFilterMaskSize));

    StringArray & propertiesToReturnList = propertiesToReturn.getProperties();
    ForEachItemIn(idx1,propertiesToReturnList)
    {
        const char * propName = propertiesToReturnList[idx1];
        if (!propName || *propName==0) continue;

        StatisticKind sk;
        WuAttr wa;
        if (!getPropertyIdFromName(propName, sk, wa))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid property name (%s) in PropertiesToReturn",propName);
        if (sk!=StKindNone)
        {
            if (statisticsPostFilterRequired)
                propertiesToReturnStats->set(sk, true);
        }
        else
        {
            if (attributesPostFilterRequired)
                propertiesToReturnAttribs->set(wa-WaNone,true);
        }
    }
}

void WUDetailsVisitor::resetScope()
{
    currentStatisticScopeType = SSTnone;
    extraStatisticsForCurScope = nullptr;
    extraAttributesForCurScope = nullptr;
    EspWUResponseProperties.clear();
    EspWUResponseNotes.clear();
    endListAttr();
}

void WUDetailsVisitor::noteScopeType(const StatisticScopeType _sst)
{
    currentStatisticScopeType = _sst;
    extraStatisticsForCurScope = & extraStatistics[currentStatisticScopeType];
    extraAttributesForCurScope = & extraAttributes[currentStatisticScopeType];
}

WUDetails::WUDetails(IConstWorkUnit *_workunit, const char *_wuid)
: workunit(_workunit), wuid(_wuid)
{
}

void WUDetails::processRequest(IEspWUDetailsRequest &req, IEspWUDetailsResponse &resp)
{
    IConstWUScopeOptions & scopeOptions =  req.getScopeOptions();
    const bool includeScope = scopeOptions.getIncludeScope();
    const bool includeScopeType = scopeOptions.getIncludeScopeType();
    const bool includeId = scopeOptions.getIncludeId();

    buildWuScopeFilter(req.getScopeFilter(), req.getNestedFilter(), req.getPropertiesToReturn(),
                       req.getFilter(), scopeOptions);

    StringBuffer filter;
    PROGLOG("WUDetails: %s", wuScopeFilter.describe(filter).str());

    IArrayOf<IEspWUResponseScope> respScopes;
    WUDetailsVisitor wuDetailsVisitor(req.getPropertyOptions(), req.getPropertiesToReturn());
    Owned<IConstWUScopeIterator> iter = &workunit->getScopeIterator(wuScopeFilter);
    ForEach(*iter)
    {
        StatisticScopeType scopeType = iter->getScopeType();
        const char * scope = iter->queryScope();
        assertex(scope);

        wuDetailsVisitor.noteScopeType(scopeType);
        iter->playProperties(wuDetailsVisitor);

        Owned<IEspWUResponseScope> respScope = createWUResponseScope("","");
        if (includeScope)
            respScope->setScopeName(scope);
        if (includeScopeType)
            respScope->setScopeType(queryScopeTypeName(scopeType));
        if (includeId)
            respScope->setId(queryScopeTail(scope));

        IArrayOf<IEspWUResponseProperty> & properties = wuDetailsVisitor.getResponseProperties();
        if (!properties.empty())
            respScope->setProperties(properties);

        IArrayOf<IEspWUResponseNote> & notes = wuDetailsVisitor.getResponseNotes();
        if (!notes.empty())
            respScope->setNotes(notes);

        respScopes.append(*respScope.getClear());

        wuDetailsVisitor.resetScope();
    }
    StringBuffer maxVersion;
    maxVersion.append(wuDetailsVisitor.getMaxTimestamp());

    resp.setWUID(wuid.str());
    resp.setMaxVersion(maxVersion.str());
    resp.setScopes(respScopes);
}

void WUDetails::buildWuScopeFilter(IConstWUScopeFilter & requestScopeFilter, IConstWUNestedFilter & nestedFilter,
                                   IConstWUPropertiesToReturn & propertiesToReturn, const char * filter,
                                   IConstWUScopeOptions & scopeOptions)
{
    wuScopeFilter.addFilter(filter);
    wuScopeFilter.setDepth(0, requestScopeFilter.getMaxDepth());
    const char * measure = propertiesToReturn.getMeasure();
    if (measure && *measure)
        wuScopeFilter.setMeasure(measure);
    wuScopeFilter.setIncludeNesting(nestedFilter.getDepth());
    wuScopeFilter.setIncludeMatch(scopeOptions.getIncludeMatchedScopesInResults());

    StringArray & scopes = requestScopeFilter.getScopes();
    ForEachItemIn(idx1,scopes)
        if (*scopes.item(idx1))
            wuScopeFilter.addScope(scopes.item(idx1));

    StringArray & ids = requestScopeFilter.getIds();
    ForEachItemIn(idx2,ids)
        if (*ids.item(idx2))
            wuScopeFilter.addId(ids.item(idx2));

    StringArray & scopeTypes = requestScopeFilter.getScopeTypes();
    ForEachItemIn(idx3,scopeTypes)
        if (*scopeTypes.item(idx3))
            wuScopeFilter.addScopeType(scopeTypes.item(idx3));

    buildPropertyFilter(requestScopeFilter.getPropertyFilters());

    StringArray & nestedScopeTypes = nestedFilter.getScopeTypes();
    ForEachItemIn(idx4,nestedScopeTypes)
        if (*nestedScopeTypes.item(idx4))
            wuScopeFilter.setIncludeScopeType(nestedScopeTypes.item(idx4));

    const char * minVersion = propertiesToReturn.getMinVersion();
    if (minVersion && *minVersion)
    {
        StringBuffer sMinVersion("version[");
        sMinVersion.append(propertiesToReturn.getMinVersion()).append("]");
        wuScopeFilter.addFilter(sMinVersion);
    }

    StringArray & properties = propertiesToReturn.getProperties();
    ForEachItemIn(idx5,properties)
    {
        const char * propName = properties.item(idx5);
        if (propName && *propName)
            wuScopeFilter.addOutput(propName);
    }

    IArrayOf<IConstWUExtraProperties> & extraProperties= propertiesToReturn.getExtraProperties();
    ForEachItemIn(idx6, extraProperties)
    {
        IConstWUExtraProperties & cur = extraProperties.item(idx6);
        const char * scopeTypeWithExtraProps = cur.getScopeType();
        if (!scopeTypeWithExtraProps || !*scopeTypeWithExtraProps) continue;

        StringArray & properties = cur.getProperties();
        ForEachItemIn(idx7, properties)
        {
            wuScopeFilter.addOutput(properties.item(idx7));
        }
    }

    WuPropertyTypes wuPropertyTypeMask = PTnone;
    if (propertiesToReturn.getAllProperties())
    {
        wuPropertyTypeMask = PTall;
    }
    else
    {
        if (propertiesToReturn.getAllStatistics())
            wuPropertyTypeMask |= PTstatistics;
        if (propertiesToReturn.getAllAttributes())
            wuPropertyTypeMask |= PTattributes;
        if (propertiesToReturn.getAllHints())
            wuPropertyTypeMask |= PThints;
        if (propertiesToReturn.getAllScopes())
            wuPropertyTypeMask |= PTscope;
        if (propertiesToReturn.getAllNotes())
            wuPropertyTypeMask |= PTnotes;
    }
    wuScopeFilter.addOutputProperties(wuPropertyTypeMask);

    wuScopeFilter.finishedFilter();
}

void WUDetails::buildPropertyFilter(IArrayOf<IConstWUPropertyFilter> & reqPropertyFilter)
{
    ForEachItemIn(idx,reqPropertyFilter)
    {
        IConstWUPropertyFilter & attribFilterItem = reqPropertyFilter.item(idx);
        const char * propertyName = attribFilterItem.getName();
        if ( !propertyName || *propertyName==0 ) continue;

        const char *exactValue = attribFilterItem.getExactValue();
        const char *minValue = attribFilterItem.getMinValue();
        const char *maxValue = attribFilterItem.getMaxValue();
        const bool hasExactValue = exactValue && *exactValue!=0;
        const bool hasMinValue = minValue && *minValue!=0;
        const bool hasMaxValue = maxValue && *maxValue!=0;

        if (hasExactValue && (hasMinValue||hasMaxValue))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,
                                      "Invalid Property Filter ('%s') - ExactValue may not be used with MinValue or MaxValue",
                                      propertyName);

        const StatisticKind sk = queryStatisticKind(propertyName, StKindNone);
        if (sk==StKindAll)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Property Name ('%s') in Property Filter", propertyName);

        if (sk==StKindNone)
        {
            WuAttr attr = queryWuAttribute(propertyName, WaNone);
            if (attr == WaNone || attr == WaAll)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Property Name ('%s') in Property Filter", propertyName);

            if (hasMinValue || hasMaxValue)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Range comparisons not supported for attribute '%s' in Property Filter", propertyName);

            if (hasExactValue)
                wuScopeFilter.addRequiredAttr(attr, exactValue);
            else
                wuScopeFilter.addRequiredAttr(attr, nullptr);
        }
        else
        {
            if (hasExactValue)
            {
                stat_type exactVal = atoi64(exactValue);
                wuScopeFilter.addRequiredStat(sk,exactVal,exactVal);
            }
            else if (hasMinValue||hasMaxValue)
            {
                stat_type minVal = atoi64(minValue);
                stat_type maxVal = atoi64(maxValue);
                wuScopeFilter.addRequiredStat(sk,minVal,maxVal);
            }
            else
                wuScopeFilter.addRequiredStat(sk);
        }
    }
}
