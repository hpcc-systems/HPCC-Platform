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

class WUDetailsVisitor : public IWuScopeVisitor
{
public:
    WUDetailsVisitor(IConstWUPropertyOptions & _propertyOptions, IConstWUPropertiesToReturn & _propertiesToReturn);
    virtual ~WUDetailsVisitor(){};
    virtual void noteStatistic(StatisticKind kind, unsigned __int64 value, IConstWUStatistic & extra) override;
    virtual void noteAttribute(WuAttr attr, const char * value) override;
    virtual void noteHint(const char * kind, const char * value) override;

    void resetScope();
    void noteScopeType(const StatisticScopeType _sst);
    IArrayOf<IEspWUResponseProperty> & getResponseProperties() { return EspWUResponseProperties;}
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

    bool extraStatisticsRequested = false;
    std::set<StatisticKind> extraStatistics[SSTmax];
    bool extraAttributesRequested = false;
    std::set<WuAttr> extraAttributes[SSTmax];

    bool returnStatisticListSpecified = false;
    Owned<IBitSet> returnStatisticList;
    bool returnAttributeSpecified = false;
    Owned<IBitSet> returnAttributeList;

    const std::set<StatisticKind> * additionalStatsForCurScope = nullptr;
    const std::set<WuAttr> * additionalAttribsForCurScope = nullptr;
    unsigned __int64 maxTimestamp = 0;
    IArrayOf<IEspWUResponseProperty> EspWUResponseProperties;
    StatisticScopeType currentStatisticScopeType = SSTnone;

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
    if (extraStatisticsRequested)
    {
        // If the statistic is not in the standard return statistic list,
        // then check if it's in the additional property list
        if (returnStatisticListSpecified && !returnStatisticList->test(kind))
        {
            if (additionalStatsForCurScope==nullptr ||
                additionalStatsForCurScope->find(kind)==additionalStatsForCurScope->end())
            {
                return;
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

void WUDetailsVisitor::noteAttribute(WuAttr attr, const char * value)
{
    if (extraAttributesRequested)
    {
        // If the Attribute is not in the standard return statistic list,
        // then check if it's in the additional attribute list
        if ((returnAttributeSpecified && !returnAttributeList->test(attr-WaNone)))
        {
            if (additionalAttribsForCurScope==nullptr ||
                additionalAttribsForCurScope->find(attr)==additionalAttribsForCurScope->end())
            {
                return;
            }
        }
    }

    Owned<IEspWUResponseProperty> EspWUResponseProperty = createWUResponseProperty("","");
    EspWUResponseProperty->setName(queryWuAttributeName(attr));
    if (includeFormatted)
        EspWUResponseProperty->setFormatted(value);
    if (includeRawValue)
        EspWUResponseProperty->setRawValue(value);

    EspWUResponseProperties.append(*EspWUResponseProperty.getClear());
}

void WUDetailsVisitor::noteHint(const char * kind, const char * value)
{
    Owned<IEspWUResponseProperty> EspWUResponseProperty = createWUResponseProperty("","");

    StringBuffer hint("hint:");
    hint.append(kind);
    EspWUResponseProperty->setName(hint);
    if (includeFormatted)
        EspWUResponseProperty->setFormatted(value);
    if (includeRawValue)
        EspWUResponseProperty->setRawValue(value);

    EspWUResponseProperties.append(*EspWUResponseProperty.getClear());
}

void WUDetailsVisitor::buildAttribListToReturn(IConstWUPropertiesToReturn & propertiesToReturn)
{
    const bool allStatistics = propertiesToReturn.getAllStatistics();
    const bool allAttributes = propertiesToReturn.getAllAttributes();

    if (propertiesToReturn.getAllProperties() || (allStatistics && allAttributes) )
        return;

    IArrayOf<IConstWUExtraProperties> & extraProperties = propertiesToReturn.getExtraProperties();
    ForEachItemIn(idx, extraProperties)
    {
        IConstWUExtraProperties & cur = extraProperties.item(idx);
        const char * scopeTypeWithAdditionalProps = cur.getScopeType();
        if (!scopeTypeWithAdditionalProps || !*scopeTypeWithAdditionalProps)
            continue;

        StatisticScopeType sst = queryScopeType(scopeTypeWithAdditionalProps,SSTnone);
        if(sst==SSTnone)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid ScopeType (%s) in ExtraProperties",scopeTypeWithAdditionalProps);

        const StringArray & props = cur.getProperties();
        ForEachItemIn(idx2, props)
        {
            StatisticKind sk = queryStatisticKind(props[idx2], StKindNone);
            if (sk==StKindAll)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid property name (%s) in ExtraProperties",props[idx2]);
            if (sk!=StKindNone)
            {
                if (!allStatistics)
                {
                    extraStatistics[sst].insert(sk);
                    extraStatisticsRequested = true;
                }
            }
            else
            {
                const WuAttr wa = queryWuAttribute(props[idx2], WaMax);
                if (wa==WaMax)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid property name (%s) in ExtraProperties",props[idx2]);
                if (!allAttributes)
                {
                    extraAttributes[sst].insert(wa);
                    extraAttributesRequested = true;
                }
            }
        }
    }

    // If additional stats or attributes specified for scope type,
    // then noteStatistic & noteAttribute will need to work out what to return
    if (extraStatisticsRequested || extraAttributesRequested)
    {
        StringArray & propertiesToReturnList = propertiesToReturn.getProperties();
        ForEachItemIn(idx,propertiesToReturnList)
        {
            const char * attributeName = propertiesToReturnList[idx];
            if (!attributeName || *attributeName==0) continue;

            const StatisticKind sk = queryStatisticKind(attributeName, StKindNone);
            if (sk==StKindAll)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Attribute name in AttributeToReturn(%s)", attributeName);
            if (sk!=StKindNone)
            {
                if (!returnStatisticListSpecified)
                {
                    returnStatisticList.set(createBitSet(StatisticFilterMaskSize));
                    returnStatisticListSpecified=true;
                }
                returnStatisticList->set(sk, true);
            }
            else
            {
              const WuAttr wa = queryWuAttribute(attributeName, WaMax);
              if (wa!=WaMax)
              {
                  if (!returnAttributeSpecified)
                  {
                      returnAttributeList.set(createBitSet(AttributeFilterMaskSize));
                      returnAttributeSpecified=true;
                  }
                  returnAttributeList->set(wa-WaNone,true);
              }
              else
                  throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Attribute name in AttributeToReturn(%s)", attributeName);
            }
        }
    }
}

void WUDetailsVisitor::resetScope()
{
    currentStatisticScopeType = SSTnone;
    additionalStatsForCurScope = nullptr;
    additionalAttribsForCurScope = nullptr;
    EspWUResponseProperties.clear();
}

void WUDetailsVisitor::noteScopeType(const StatisticScopeType _sst)
{
    currentStatisticScopeType = _sst;
    additionalStatsForCurScope = & extraStatistics[currentStatisticScopeType];
    additionalAttribsForCurScope = & extraAttributes[currentStatisticScopeType];
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
        if (properties.item(idx5) && *properties.item(idx5))
        {
            wuScopeFilter.addOutput(properties.item(idx5));
        }
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
        wuPropertyTypeMask = PTstatistics|PTattributes|PThints|PTscope;
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
        const bool hasExactValue = *exactValue!=0;
        const bool hasMinValue = *minValue!=0;
        const bool hasMaxValue = *maxValue!=0;

        if (hasExactValue && (hasMinValue||hasMaxValue))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,
                                      "Invalid Property Filter ('%s') - ExactValue may not be used with MinValue or MaxValue",
                                      propertyName);
        const StatisticKind sk = queryStatisticKind(propertyName, StKindNone);
        if (sk==StKindAll || sk==StKindNone)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Property Name ('%s') in Property Filter", propertyName);
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
