/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#ifndef __HQLCFILTER_HPP_
#define __HQLCFILTER_HPP_

#include "hqlfilter.hpp"

struct BuildFilterState
{
    BuildFilterState(BuildCtx & _funcctx, const char * _listName) : funcctx(_funcctx)
    {
        listName = _listName;
        curFieldIdx = 0;
        curOffset = 0;
        wildOffset = (unsigned) -1;
        numActiveSets = 0;
        warnedAllConditionsWild = false;
        doneImplicitWarning = true;
        wildWasKeyed = false;
    }

    inline bool wildPending() { return wildOffset != (unsigned)-1; }
    inline void clearWild() { wildOffset = (unsigned) -1; }

    const char * getSetName(bool createValueSets);
    void popSetName();

//Constant while building monitors
    BuildCtx & funcctx;
    const char * listName;

//State variables used when generating
    OwnedHqlExpr implicitWildField;
    unsigned numActiveSets;
    CIArrayOf<StringAttrItem> setNames;
    bool doneImplicitWarning;
    bool warnedAllConditionsWild;
    bool wildWasKeyed;
    unsigned curFieldIdx;
    unsigned curOffset;
    unsigned wildOffset;
};


class CppFilterExtractor final : public FilterExtractor
{
public:
    CppFilterExtractor(IHqlExpression * _tableExpr, HqlCppTranslator & _translator, int _numKeyableFields, bool isDiskRead, bool _createValueSets);

    void buildSegments(BuildCtx & ctx, const char * listName, bool _ignoreUnkeyed);
    bool createGroupingMonitor(BuildCtx ctx, const char * listName, IHqlExpression * select, unsigned & maxField);
    bool useValueSets() const { return createValueSets; }
    void noteKeyedFieldUsage(SourceFieldUsage * fieldUsage);

protected:
    void buildEmptyKeySegment(BuildFilterState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo);
    void buildKeySegment(BuildFilterState & buildState, BuildCtx & ctx, unsigned whichField, unsigned curSize);
    void buildKeySegmentExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    void buildKeySegmentCompareExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * requiredSet, IHqlExpression & thisKey);
    void buildKeySegmentInExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    bool buildSingleKeyMonitor(StringBuffer & createMonitorText, KeySelectorInfo & selectorInfo, BuildCtx & ctx, IHqlExpression & thisKey);
    bool buildSubRange(BuildCtx & ctx, IHqlExpression * range, CHqlBoundExpr & bound);
    void callAddAll(BuildCtx & ctx, IHqlExpression * targetVar);
    void createStringSet(BuildCtx & ctx, const char * target, unsigned size, IHqlExpression * selector);
    KeyCondition * createTranslatedCondition(IHqlExpression * cond, KeyedKind keyedKind);
    IHqlExpression * getMonitorValueAddress(BuildCtx & ctx, IHqlExpression * expandedSelector, IHqlExpression * value);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * expr, SharedHqlExpr & subrange, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * value, SharedHqlExpr & subrange, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector);

    virtual IHqlExpression * getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary) override;

protected:
    void spotSegmentCSE(BuildCtx & ctx);

    class SelectSpotter : public NewHqlTransformer
    {
    public:
        SelectSpotter(const HqlExprArray & _selects);

        void analyseExpr(IHqlExpression * expr);

    public:
        bool hasSelects;
        const HqlExprArray & selects;
    };

protected:
    HqlCppTranslator & translator;
    IIdAtom * addRangeFunc;
    IIdAtom * killRangeFunc;
};

#endif
