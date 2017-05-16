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

#include "ws_workunitsService.hpp"
#include "ws_wudetails.hpp"

static int funcCompareString(char const * const *l, char const * const *r)
{
    return strcmp(*l, *r);
}

// Return Match if 'key' is a child scope of 'r' or identical match
// Otherwise. return -1 or +1 for continuing binary search
static int funcNestedScopeSearch(char const * const *key, char const * const *tablevalue)
{
    const char * l = * key;
    const char * r = * tablevalue;

    while (*l && *l==*r)
    {
        ++l;
        ++r;
    }
    if ( *l==':' && *r==0)
      return 0;
    return *l - *r;
}

inline unsigned getScopeDepth(const char * scope)
{
    unsigned depth = 1;

    for (const char * p = scope; *p; ++p)
        if (*p==':') ++depth;
    return depth;
}

static const char * getScopeIdFromScopeName(const char * scopeName)
{
    const char * p = strrchr(scopeName, ':');
    return p?p+1:scopeName;
}

static unsigned buildScopeTypeMaskFromStringArray(const StringArray & arrayScopeTypeList)
{

    unsigned mask = 0;
    ForEachItemIn(idx, arrayScopeTypeList)
    {
        const char * scopeType = arrayScopeTypeList[idx];
        if (*scopeType==0) continue;

        StatisticScopeType sst = queryScopeType(scopeType);
        if (sst==SSTnone ||sst==SSTall)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid ScopeType(%s) in filterNestedScopeTypes",scopeType);
        mask |= (1<<sst);
    }
    if (mask==0)
      mask=-1; // If no scope type filter, then filtering on scope type disabled

    return mask;
}

StatisticsFilter * CWUDetails::initStatisticsFilter()
{
    Owned<StatisticsFilter> wustatsfilter = new StatisticsFilter;

    StatisticMeasure measure = queryMeasure(attribsToReturn.getMeasure());
    if (measure != SMeasureNone && measure != SMeasureAll)
        wustatsfilter->setMeasure(measure);

    return wustatsfilter.getClear();
}

void CWUDetails::process(IEspWUDetailsResponse &resp)
{
    Owned<StatisticsFilter>  wustatsfilter = initStatisticsFilter();
    buildAttribFilter();
    buildAttribListToReturn();
    nestedScopeTypeFilterMask = buildScopeTypeMaskFromStringArray(nfilter.getScopeTypes());
    ids.pruneEmpty();
    ids.sortAscii();      // Will need to lookup up 'id' in ids (for bSearch)
    scopes.pruneEmpty();
    scopeTypes.pruneEmpty();
    scopeTypeFilterMask = -1;  // assume filtering disabled for scope types

    // Potential issue: minVersion > 2^63 will cause error message to be returned
    // (Practically, this should not be an issue with as timestamps are <2^63)
    __int64 reqMinVersion = atoi64(attribsToReturn.getMinVersion());
    if (reqMinVersion<0)
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "MinVersion number must be zero or positive");
    // minVersion==0 => keep minVersion unchanged so that scopes returned regardless of version number
    // otherise => increment so that the previously return scopes are not returned
    minVersion = reqMinVersion ? reqMinVersion+1 : 0;

    unsigned __int64 maxTimestamp = 0;
    IArrayOf<IEspWUScope> respScopes;
    if (scopeTypes.ordinality()>0)
    {
        if (scopes.ordinality()>0)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ScopeType filter may not be used with scope filter");
        if (ids.ordinality()>0)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "ScopeType filter may not be used with id filter");

        DBGLOG("onWUDetails: Filter by ScopeType");
        if (nestedDepth)
        {
            scopeTypeFilterMask = buildScopeTypeMaskFromStringArray(scopeTypes);
            wustatsfilter->setScopeDepth(0, scopeFilter.getMaxDepth()+nfilter.getDepth());
            Owned<IConstWUStatisticIterator> stats = & workunit->getStatistics(wustatsfilter);
            processStatsIntoResponse(respScopes, *stats, maxTimestamp);
        }
        else
        {
            wustatsfilter->setScopeDepth(0, scopeFilter.getMaxDepth());
            ForEachItemIn(idx, scopeTypes)
            {
                StatisticScopeType scopeType = queryScopeType(scopeTypes[idx]);
                if (scopeType == SSTnone)
                    throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid ScopeType: %s", scopeTypes[idx]);
                wustatsfilter->setScopeType(scopeType);

                Owned<IConstWUStatisticIterator> stats = & workunit->getStatistics(wustatsfilter);
                processStatsIntoResponse(respScopes, *stats, maxTimestamp);
            }
        }
    }
    else if (scopes.ordinality()>0)
    {
        DBGLOG("onWUDetails: Filter by Scope");
        wustatsfilter->setScopeDepth(0, scopeFilter.getMaxDepth()+nfilter.getDepth());
        if(nestedDepth)
            scopes.sortAscii();  // needed when searching nested scope
        ForEachItemIn(idx, scopes)
        {
            if(nestedDepth)
            {
                StringBuffer scopeMatch(scopes[idx]);
                scopeMatch.append("*");
                wustatsfilter->setScope(scopeMatch);
            }
            else
            {
                wustatsfilter->setScope(scopes[idx]);
            }

            Owned<IConstWUStatisticIterator> stats = & workunit->getStatistics(wustatsfilter);
            processStatsIntoResponse(respScopes, *stats, maxTimestamp);
        }
    }
    else
    {
        wustatsfilter->setScopeDepth(0, scopeFilter.getMaxDepth()+nfilter.getDepth());
        DBGLOG("onWUDetails: Filter by id");
        Owned<IConstWUStatisticIterator> stats = & workunit->getStatistics(wustatsfilter);
        processStatsIntoResponse(respScopes, *stats, maxTimestamp);
    }
    StringBuffer maxVersion;
    maxVersion.append(maxTimestamp);

    resp.setWUID(wuid.str());
    resp.setMaxVersion(maxVersion.str());
    resp.setScopes(respScopes);
}

void CWUDetails::buildAttribFilter()
{
    IArrayOf<IConstWUAttributeFilter> & reqAttribFilter = scopeFilter.getAttributeFilters();
    ForEachItemIn(idx,reqAttribFilter)
    {
          IConstWUAttributeFilter & attr = reqAttribFilter.item(idx);
          const char * sStatisticKind = attr.getName();
          if ( *sStatisticKind==0 ) continue; // skip nulls

          StatisticKind sk = queryStatisticKind(sStatisticKind);
          if (sk == StKindNone || sk== StKindAll)
              throw MakeStringException(ECLWATCH_INVALID_INPUT,
                                        "Invalid Attribute Name ('%s') in Attribute Filter",
                                        reqAttribFilter.item(idx).getName());

          const char * exactValue = attr.getExactValue();
          const char * minValue = attr.getMinValue();
          const char * maxValue = attr.getMaxValue();
          bool hasExactValue = *exactValue!=0;
          bool hasMinValue = *minValue!=0;
          bool hasMaxValue = *maxValue!=0;

          if (hasExactValue && (hasMinValue||hasMaxValue))
              throw MakeStringException(ECLWATCH_INVALID_INPUT,
                                        "Invalid Attribute Filter ('%s') - ExactValue may not be used with MinValue or MaxValue",
                                        reqAttribFilter.item(idx).getName());

          attribFilterMask->set(sk,true);
          Owned<CAttributeFilter> caf = new CAttributeFilter(sk, atoi64(exactValue), atoi64(minValue),
                                                             atoi64(maxValue), hasExactValue, hasMinValue, hasMaxValue);
          arrayAttributeFilter.append(*caf.getClear());
    }
    arrayAttributeFilter.sort(funcCompareAttribFilter);
}

void CWUDetails::buildAttribListToReturn()
{
    StringArray & attribsToReturnList = attribsToReturn.getAttributes();
    returnAttribListSpecified=false;
    ForEachItemIn(idx,attribsToReturnList)
    {
        const char * attribName = attribsToReturnList[idx];
        if (*attribName==0) continue;

        StatisticKind sk = queryStatisticKind(attribName);
        if (sk==StKindNone || sk==StKindAll)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Attribute name in AttributeToReturn(%s)",attribName);

        returnAttribList->set(sk, true);
        returnAttribListSpecified=true;
    }
}

void CWUDetails::processStatsIntoResponse(IArrayOf<IEspWUScope> & respScopes,
                                          IConstWUStatisticIterator & statsIter,
                                          unsigned __int64 & maxTimestamp)
{
    unsigned maxDepth = scopeFilter.getMaxDepth();
    bool limitToIdList = ids.ordinality()>0;
    bool filterByScope = scopes.ordinality()>0;
    bool filterByAttributeValue = arrayAttributeFilter.ordinality();

    DBGLOG("Id list count: %d", ids.ordinality());
    DBGLOG("AttributeFilter count: %d", arrayAttributeFilter.ordinality());

    IArrayOf<IEspWUResponseAttribute> attribs;
    SCMStringBuffer previousScope, previousScopeType, previousId;
    StringBuffer skipScope;
    unsigned statsCount = 0, statsAdded = 0;
    StringArray nestedScopeList;
    CAttributeFilter tmpForAttribSearch;
    Owned<IBitSet> attribFilterProcessed(createBitSet(AttributeFilterMaskSize,true));
    ForEach(statsIter)
    {
        ++statsCount;
        IConstWUStatistic & stat = statsIter.query();

        bool foundInNestedScopeList;// True if a nested match
        SCMStringBuffer scope;
        stat.getScope(scope);
        const char * id;
        id = getScopeIdFromScopeName(scope.str());
        StatisticScopeType scopeType = stat.getScopeType();

        // Shortcut to avoid processing a scope that has been filtered out previously
        if (skipScope.length()>0 && strcmp(skipScope.str(),scope.str())!=0)
            continue;
        // Check if scope name has changed. If so, need to return the list attributes for previous scope
        if (strcmp(previousScope.str(),scope.str())!=0)
        {
            if (previousScope.length()>0)
            {
                if(*attribFilterProcessed==*attribFilterMask)
                {
                    Owned<IEspWUScope> respScope = buildRespScope(previousScope.str(), previousScopeType.str(), previousId.str(), attribs);
                    respScopes.append(*respScope.getClear());
                }

                attribs.clear();
                previousScope.clear();
            }

            foundInNestedScopeList = false;
            attribFilterProcessed->reset();
            unsigned scopeDepth = getScopeDepth(scope.str());
            if (maxDepth && scopeDepth > maxDepth)
            {
                skipScope.set(scope.str());
                if (!nestedDepth || scopeDepth > maxDepth + nestedDepth)
                  continue;

                DBGLOG("Nested Filter: scope %s %u", scope.str(), (unsigned) scopeType);
                // Nested scope
                unsigned matchedScopeDepth;
                const char *matchedScope = 0;
                if (filterByScope)
                {

                    aindex_t pos = scopes.bSearch(scope.str(), funcNestedScopeSearch);
                    if (pos==NotFound)
                        continue;
                    matchedScopeDepth = getScopeDepth(scopes[pos]);
                }
                else if (limitToIdList || scopeTypeFilterMask!=-1)
                {
                    aindex_t pos = nestedScopeList.bSearch(scope.str(),funcNestedScopeSearch);
                    if (pos==NotFound)
                        continue;
                    matchedScopeDepth = getScopeDepth(nestedScopeList[pos]);
                } else
                    matchedScopeDepth = maxDepth;

                if (scopeDepth>nestedDepth+matchedScopeDepth)
                    continue;

                if ((nestedScopeTypeFilterMask & (1<<scopeType))==0)
                    continue;
                foundInNestedScopeList = true;
            }
            if (!foundInNestedScopeList)
            {
                // Following to filters only matched when not processing nested scope
                if ((scopeTypeFilterMask & (1<<scopeType))==0)
                    continue;
                if (limitToIdList && ids.bSearch(id,funcCompareString)==NotFound)
                    continue;
            }
            skipScope.clear();
            previousScope.set(scope.str());
            previousScopeType.set(queryScopeTypeName(scopeType));
            previousId.set(id);
        }

        if (filterByAttributeValue)
        {
            StatisticKind sk = stat.getKind();
            if (attribFilterMask->test(sk))
            {
                tmpForAttribSearch.setStatisticKind(sk);
                aindex_t pos = arrayAttributeFilter.bSearch(&tmpForAttribSearch,funcCompareAttribFilter);
                if (pos!=NotFound)
                {
                    unsigned __int64 graphRawValue = stat.getValue();
                    if (!arrayAttributeFilter.item(pos).matchesValue(graphRawValue))
                    {
                        skipScope.set(scope.str());     // Attribute condition failed, so skip all remain for this scope
                        attribs.clear();
                        previousScope.clear();
                        continue;
                    }
                }
                attribFilterProcessed->set(sk,true);
            }
        }
        // Add to nestedScopeList, if required (nestedDepth && !foundInNestedScopeList)
        // nestScopeList not required for scope restricted match as "scopes" array used to query nested match
        if (nestedDepth && !foundInNestedScopeList && !filterByScope)
        {
            bool isNew;
            const char * tscope = strdup(scope.str());
            nestedScopeList.bAdd(tscope, funcCompareString, isNew); // binary adds only if it is new
            if (!isNew)
                free(const_cast<char *>(tscope));
        }
        if (returnAttribListSpecified)
        {
            if (returnAttribList->test(stat.getKind()))
               continue;
        }

        unsigned __int64 timeStamp = stat.getTimestamp();
        if (timeStamp<minVersion)
            continue;
        if (timeStamp>maxTimestamp)
            maxTimestamp = timeStamp;

        Owned<IEspWUResponseAttribute> attrib = copyAttribs(stat, attribOptions);
        attribs.append(*attrib.getClear());
        ++statsAdded;
    }
    DBGLOG("Statistics processed %u, stats added %u", statsCount, statsAdded);
    if (previousScope.length()>0 && *attribFilterProcessed==*attribFilterMask)
    {
        Owned<IEspWUScope> respScope = buildRespScope(previousScope.str(), previousScopeType.str(), previousId.str(), attribs);
        respScopes.append(*respScope.getClear());
        attribs.clear();
    }
}

IEspWUScope * CWUDetails::buildRespScope(const char * scope,
                                         const char * scopeType,
                                         const char * id,
                                         IArrayOf<IEspWUResponseAttribute> & attribs)
{
    Owned<IEspWUScope> wuscope = createWUScope("","");

    if (scopeOptions.getIncludeScope())     wuscope->setScope(scope);
    if (scopeOptions.getIncludeScopeType()) wuscope->setScopeType(scopeType);
    if (scopeOptions.getIncludeId())        wuscope->setId(id);
    //if (scopeOptions.getIncludeMatchedScopesInResults()

    wuscope->setAttributes(attribs);

    return wuscope.getClear();
}

IEspWUResponseAttribute * CWUDetails::copyAttribs(IConstWUStatistic & cur,
                                                  IConstWUAttributeOptions & attribOptions)
{
    Owned<IEspWUResponseAttribute> attrib = createWUResponseAttribute("","");
    SCMStringBuffer tmpstr;

    if (attribOptions.getIncludeName())
        attrib->setName(queryStatisticName(cur.getKind()));
    if (attribOptions.getIncludeRawValue())
    {
        StringBuffer rawValue;
        rawValue.append(cur.getValue());
        attrib->setRawValue(rawValue);
    }
    if (attribOptions.getIncludeFormatted())
        attrib->setFormatted(cur.getFormattedValue(tmpstr).str());
    if (attribOptions.getIncludeMeasure())
        attrib->setMeasure(queryMeasureName(cur.getMeasure()));
    if (attribOptions.getIncludeCreator())
        attrib->setCreator(cur.getCreator(tmpstr).str());
    if (attribOptions.getIncludeCreatorType())
        attrib->setCreatorType(queryCreatorTypeName(cur.getCreatorType()));

    return attrib.getClear();
}

