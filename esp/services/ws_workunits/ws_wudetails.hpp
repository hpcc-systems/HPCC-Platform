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

#ifndef _ESPWIZ_ws_wudetails_HPP__
#define _ESPWIZ_ws_wudetails_HPP__

#include "jlib.hpp"
#include "jset.hpp"
#include "jstatcodes.h"

class CWUDetails
{
public:
    CWUDetails(IConstWorkUnit *_workunit, const char *_wuid, IEspWUDetailsRequest &req)
    : workunit(_workunit), wuid(_wuid),scopeFilter(req.getFilter()),
      attribsToReturn(req.getAttributeToReturn()), scopeOptions(req.getScopeOptions()),
      attribOptions(req.getAttributeOptions()), nfilter(req.getNested()), nestedDepth(nfilter.getDepth())
    {
        ids.appendList(scopeFilter.getIds());
        scopes.appendList(scopeFilter.getScopes());
        scopeTypes.appendList(scopeFilter.getScopeTypes());

        attribFilterMask.set(createBitSet(AttributeFilterMaskSize,true));
        returnAttribList.set(createBitSet(AttributeFilterMaskSize,true));

        // If a compile time error is reported here then the number of enums in StatisticScopeType has grown so that
        // an 'unsigned' is too small to be used as a mask for all the values in StatisticScopeType
        static_assert(StatisticScopeType::SSTmax<sizeof(unsigned)*8, "scopeTypeFilterMask and nestedScopeTypeFilterMask mask too small hold all values");
    };

    void process(IEspWUDetailsResponse & resp);

private:
    class CAttributeFilter: public CInterface
    {
        StatisticKind statisticKind;
        unsigned __int64 exactValue;
        unsigned __int64 minValue;
        unsigned __int64 maxValue;
        bool hasExactValue;
        bool hasMinValue;
        bool hasMaxValue;
    public:
        IMPLEMENT_IINTERFACE;

        CAttributeFilter()
                        : statisticKind(StKindNone), exactValue(0), minValue(0), maxValue(0),
                        hasExactValue(false), hasMinValue(false), hasMaxValue(false) {};
        CAttributeFilter(StatisticKind sk, unsigned __int64 exact, unsigned __int64 min,
                        unsigned __int64 max, bool _hasExactValue, bool _hasMinValue, bool _hasMaxValue)
                        : statisticKind(sk), exactValue(exact), minValue(min), maxValue(max),
                        hasExactValue(_hasExactValue), hasMinValue(_hasMinValue), hasMaxValue(_hasMaxValue) {};
        StatisticKind getStatisticKind() { return statisticKind; }
        void setStatisticKind(StatisticKind sk) { statisticKind = sk; }
        bool matchesValue(unsigned __int64 graphRawValue)
        {
            bool matches = true;
            if (hasExactValue)
            {
                if (graphRawValue != exactValue) matches = false;
            }
            else
            {
                if (hasMinValue && graphRawValue < minValue) matches = false;
                if (hasMaxValue && graphRawValue > maxValue) matches = false;
            }
            return matches;
        }
    };

    static const unsigned AttributeFilterMaskSize = StatisticKind::StMax;
    // Requested Parameters
    Owned<IConstWorkUnit> workunit;
    StringBuffer wuid;
    IConstWUScopeFilter & scopeFilter;
    IConstWUAttributeToReturn & attribsToReturn;
    IConstWUScopeOptions & scopeOptions;
    IConstWUAttributeOptions & attribOptions;
    IConstWUNestedFilter & nfilter;
    StringArray ids, scopes, scopeTypes;
    CIArrayOf<CAttributeFilter> arrayAttributeFilter;
    Owned<IBitSet> attribFilterMask;
    bool returnAttribListSpecified;
    Owned<IBitSet> returnAttribList;
    unsigned scopeTypeFilterMask;
    unsigned nestedScopeTypeFilterMask;
    unsigned __int64 minVersion;
    unsigned nestedDepth;

    StatisticsFilter * initStatisticsFilter();

    void buildAttribFilter();
    void buildAttribListToReturn();
    void buildScopeTypeListFromStringArray(const StringArray & arrayScopeTypeList, UnsignedArray & resultArray);

    void processStatsIntoResponse(IArrayOf<IEspWUScope> & respScopes,
                                  IConstWUStatisticIterator & statsIter,
                                  unsigned __int64 & maxTimestamp);

    IEspWUScope *buildRespScope(const char * scope,
                                const char * scopeType,
                                const char * id,
                                IArrayOf<IEspWUResponseAttribute> & attribs);

    IEspWUResponseAttribute *copyAttribs(IConstWUStatistic & cur,
                                         IConstWUAttributeOptions & attribOptions);

    static int funcCompareAttribFilter(CInterface* const * l, CInterface* const * r)
    {
        CAttributeFilter * ll = static_cast<CAttributeFilter *>(*l);
        CAttributeFilter * rr = static_cast<CAttributeFilter *>(*r);

        return (int) ll->getStatisticKind() - (int) rr->getStatisticKind();
    }
};
#endif
