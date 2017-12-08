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
#ifndef __HQLSOURCE_IPP_
#define __HQLSOURCE_IPP_

IHqlExpression * convertToPhysicalTable(IHqlExpression * tableExpr, bool ensureSerialized);

enum KeyedKind { KeyedYes, KeyedNo, KeyedExtend };
struct KeyCondition : public CInterface
{
public:
    KeyCondition()          { keyedKind = KeyedNo; isWild = false; generated = false; wasKeyed = false; }
    KeyCondition(IHqlExpression * _selector, IHqlExpression * _expr, KeyedKind _keyedKind)          
                            { selector.set(_selector); expr.set(_expr); keyedKind = _keyedKind; isWild = false; generated = false; wasKeyed = isKeyed(); }

    bool isKeyed()          { return (keyedKind != KeyedNo); }

    HqlExprAttr     selector;
    HqlExprAttr     expr;
    KeyedKind       keyedKind;
    bool            isWild;
    bool            generated;
    bool            wasKeyed;

};

typedef CIArrayOf<KeyCondition> KeyConditionArray;

class KeyConditionInfo : public CInterface
{
public:
    void appendPreFilter(IHqlExpression * expr) { extendAndCondition(preFilter, expr); }
    void appendPostFilter(IHqlExpression * expr) { extendAndCondition(postFilter, expr); }
    void appendCondition(KeyCondition & next) { conditions.append(next); }

    IHqlExpression * createConjunction();

public:
    HqlExprAttr preFilter;          // before activity executed
    HqlExprAttr postFilter;         // after candidate record returned
    KeyConditionArray conditions;
};

//---------------------------------------------------------------------------

enum KeyFailureReason { KFRunknown, KFRnokey, KFRor, KFRtoocomplex, KFRcast };      // ordered
class KeyFailureInfo
{
public:
    KeyFailureInfo()  { code = KFRunknown; }

    void clear()                                                    { code = KFRunknown; }
    void merge(const KeyFailureInfo & other);
    void reportError(HqlCppTranslator & translator, IHqlExpression * condition);
    void set(KeyFailureReason _code)                                { code = _code; }
    void set(KeyFailureReason _code, IHqlExpression * _field)       { code = _code; field.set(_field); }

protected:
    KeyFailureReason code;
    OwnedHqlExpr field;
};
    

struct BuildMonitorState
{
    BuildMonitorState(BuildCtx & _funcctx, const char * _listName) : funcctx(_funcctx) 
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

enum MonitorFilterKind { NoMonitorFilter, MonitorFilterSkipEmpty, MonitorFilterSkipAll };

struct KeySelectorInfo
{
public:
    KeySelectorInfo(KeyedKind _keyedKind, IHqlExpression * _selector, IHqlExpression * _expandedSelector, unsigned _fieldIdx, size32_t _offset, size32_t _size)
    {
        keyedKind = _keyedKind; 
        selector = _selector; 
        expandedSelector = _expandedSelector;
        fieldIdx = _fieldIdx;
        offset = _offset; 
        size = _size;
    }

    const char * getFFOptions();

    IHqlExpression * selector;
    IHqlExpression * expandedSelector;
    unsigned fieldIdx;
    size32_t offset;
    size32_t size;
    KeyedKind keyedKind;
};

class MonitorExtractor
{
public:
    MonitorExtractor(IHqlExpression * _tableExpr, HqlCppTranslator & _translator, int _numKeyableFields, bool isDiskRead);

    void appendFilter(IHqlExpression * expr)                { keyed.appendPostFilter(expr); }
    void buildSegments(BuildCtx & ctx, const char * listName, bool _ignoreUnkeyed);
    void extractFilters(IHqlExpression * filter, SharedHqlExpr & extraFilter);
    void extractFilters(HqlExprArray & exprs, SharedHqlExpr & extraFilter);
    void extractFiltersFromFilterDs(IHqlExpression * expr);
    void extractAllFilters(IHqlExpression * filter);
    IHqlExpression * queryExtraFilter()                     { return keyed.postFilter; }
    IHqlExpression * getClearExtraFilter()                  { return keyed.postFilter.getClear(); }
    bool isCleanlyKeyedExplicitly()                         { return cleanlyKeyedExplicitly; }
    bool isKeyedExplicitly()                                { return keyedExplicitly; }
    bool isFiltered()                                       { return keyed.postFilter || isKeyed(); }
    bool isKeyed();
    IHqlExpression * queryGlobalGuard()                     { return keyed.preFilter; }
    void reportFailureReason(IHqlExpression * cond)         { failReason.reportError(translator, cond); }
    const char * queryKeyName(StringBuffer & s);
    void preventMerge(IHqlExpression * select)              { if (select) noMergeSelects.append(*select); }

    bool isEqualityFilterBefore(IHqlExpression * select);
    unsigned queryKeySelectIndex(IHqlExpression * select)   { return keyableSelects.find(*select); }
    bool createGroupingMonitor(BuildCtx ctx, const char * listName, IHqlExpression * select, unsigned & maxField);

protected:
    void buildEmptyKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo);
    void buildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned whichField, unsigned curSize);
    void buildKeySegmentExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    void buildKeySegmentCompareExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * requiredSet, IHqlExpression & thisKey);
    void buildKeySegmentInExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    bool buildSingleKeyMonitor(StringBuffer & createMonitorText, KeySelectorInfo & selectorInfo, BuildCtx & ctx, IHqlExpression & thisKey);
    void callAddAll(BuildCtx & ctx, IHqlExpression * targetVar);
    IHqlExpression * castToFieldAndBack(IHqlExpression * left, IHqlExpression * right);
    bool containsTableSelects(IHqlExpression * expr);
    IHqlExpression * createRangeCompare(IHqlExpression * selector, IHqlExpression * value, IHqlExpression * lengthExpr, bool compareEqual);
    void createStringSet(BuildCtx & ctx, const char * target, unsigned size, IHqlExpression * selector);
    KeyCondition * createTranslatedCondition(IHqlExpression * cond, KeyedKind keyedKind);
    bool extractBoolFieldFilter(KeyConditionInfo & matches, IHqlExpression * selector, KeyedKind keyedKind, bool compareValue);
    bool extractFilters(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
    void extractFoldedWildFields(IHqlExpression * expr);
    bool extractIfFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind);
    bool extractSimpleCompareFilter(KeyConditionInfo & state, IHqlExpression * expr, KeyedKind keyedKind);
    void expandKeyableFields();
    void expandSelects(IHqlExpression * expr, IHqlSimpleScope * expandedScope, IHqlExpression * keySelector, IHqlExpression * expandedSelector);;
    bool extractOrFilter(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
    IHqlExpression * getMonitorValueAddress(BuildCtx & ctx, IHqlExpression * expandedSelector, IHqlExpression * value);
    IHqlExpression * getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary);
    IHqlExpression * invertTransforms(IHqlExpression * left, IHqlExpression * right);
    bool isEqualityFilter(IHqlExpression * select);
    bool isKeySelect(IHqlExpression * select);
    bool isIndexInvariant(IHqlExpression * expr, bool includeRoot);
    bool isPrevSelectKeyed(IHqlExpression * select);
    bool matchSubstringFilter(KeyConditionInfo & matches, node_operator op, IHqlExpression * left, IHqlExpression * right, KeyedKind keyedKind, bool & duplicate);
    IHqlExpression * isKeyableFilter(IHqlExpression * left, IHqlExpression * right, bool & duplicate, node_operator compareOp, KeyFailureInfo & reason, KeyedKind keyedKind);
    bool okToKey(IHqlExpression * select, KeyedKind keyedKind);
    IHqlExpression * queryKeyableSelector(IHqlExpression * expr);
    IHqlExpression * querySimpleJoinValue(IHqlExpression * field);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * expr, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * value, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector);
    IHqlExpression * unwindConjunction(HqlExprArray & matches, IHqlExpression * expr);

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
    IHqlExpression * tableExpr;
    HqlCppTranslator & translator;
//  LinkedHqlExpr filter;
//  LinkedHqlExpr globalGuard;
    KeyConditionInfo keyed;
    unsigned numKeyableFields;
    KeyFailureInfo failReason;

    HqlExprAttr keyableRecord;
    HqlExprArray keyableSelects;
    // expanded record + selects have bitfields/alien/varstrings expanded to a fixed size basic type.
    HqlExprAttr expandedRecord;
    HqlExprArray expandedSelects;

    HqlExprCopyArray noMergeSelects;    // don't merge these fields (even for wildcards) because they are separate stepping fields.
    unsigned firstOffsetField;          // first field where the keyed offset is adjusted
    bool onlyHozedCompares;
    bool ignoreUnkeyed;
    bool cleanlyKeyedExplicitly;
    bool keyedExplicitly;
    bool allowDynamicFormatChange;
    const bool createValueSets;
    IIdAtom * addRangeFunc;
    IIdAtom * killRangeFunc;
};

//---------------------------------------------------------------------------

struct VirtualFieldsInfo
{
public:
    VirtualFieldsInfo()
    { 
        simpleVirtualsAtEnd = true;
        requiresDeserialize = false;
    }

    IHqlExpression * createPhysicalRecord();
    void gatherVirtualFields(IHqlExpression * record, bool ignoreVirtuals, bool ensureSerialized);
    bool hasVirtuals()      { return virtuals.ordinality() != 0; }
    bool hasVirtualsOrDeserialize() { return requiresDeserialize || virtuals.ordinality() != 0; }
    bool needFilePosition() { return virtuals.ordinality() > 0; }
    bool needFilePosition(bool local);

public:
    HqlExprArray    physicalFields;
    HqlExprArray    selects;
    HqlExprArray    virtuals;
    bool            simpleVirtualsAtEnd;
    bool            requiresDeserialize;
};

//---------------------------------------------------------------------------

unsigned getProjectCount(IHqlExpression * expr);
IHqlExpression * createMetadataIndexRecord(IHqlExpression * record, bool hasInternalFilePosition);

#endif
