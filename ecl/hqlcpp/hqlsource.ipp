/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
        curOffset = 0;
        wildOffset = (unsigned) -1;
        numActiveSets = 0;
        warnedAllConditionsWild = false;
        doneImplicitWarning = true;
        wildWasKeyed = false;
    }

    inline bool wildPending() { return wildOffset != (unsigned)-1; }
    inline void clearWild() { wildOffset = (unsigned) -1; }

    const char * getSetName();
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
    unsigned curOffset;
    unsigned wildOffset;
};

enum MonitorFilterKind { NoMonitorFilter, MonitorFilterSkipEmpty, MonitorFilterSkipAll };

struct KeySelectorInfo
{
public:
    KeySelectorInfo(KeyedKind _keyedKind, IHqlExpression * _selector, IHqlExpression * _expandedSelector, size32_t _offset, size32_t _size, bool _mapOffset, bool _isComputed)
    {
        keyedKind = _keyedKind; 
        selector = _selector; 
        expandedSelector = _expandedSelector; 
        offset = _offset; 
        size = _size;
        expandNeeded = (selector->queryType() != expandedSelector->queryType());
        mapOffset = _mapOffset;
        isComputed = _isComputed;
    }

    IHqlExpression * selector;
    IHqlExpression * expandedSelector;
    size32_t offset;
    size32_t size;
    KeyedKind keyedKind;
    bool expandNeeded;
    bool mapOffset;
    bool isComputed;
};

class MonitorExtractor
{
public:
    MonitorExtractor(IHqlExpression * _tableExpr, HqlCppTranslator & _translator, int _numKeyableFields, bool _allowTranslatedConds);

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
    void optimizeSegments(IHqlExpression * leftRecord);
    IHqlExpression * queryGlobalGuard()                     { return keyed.preFilter; }
    void reportFailureReason(IHqlExpression * cond)         { failReason.reportError(translator, cond); }
    const char * queryKeyName(StringBuffer & s);
    void preventMerge(IHqlExpression * select)              { if (select) noMergeSelects.append(*select); }

    bool isEqualityFilterBefore(IHqlExpression * select);
    unsigned queryKeySelectIndex(IHqlExpression * select)   { return keyableSelects.find(*select); }
    bool createGroupingMonitor(BuildCtx ctx, const char * listName, IHqlExpression * select, unsigned & maxOffset);

protected:
    void buildArbitaryKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned curSize, IHqlExpression * condition);
    void buildEmptyKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo);
    void buildWildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo);
    void buildWildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned offset, unsigned size);
    void buildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned whichField, unsigned curSize);
    void buildKeySegmentExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    void buildKeySegmentCompareExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * requiredSet, IHqlExpression & thisKey);
    void buildKeySegmentInExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind);
    bool buildSingleKeyMonitor(StringBuffer & createMonitorText, KeySelectorInfo & selectorInfo, BuildCtx & ctx, IHqlExpression & thisKey);
    void callAddAll(BuildCtx & ctx, IHqlExpression * targetVar);
    IHqlExpression * castToFieldAndBack(IHqlExpression * left, IHqlExpression * right);
    bool containsTableSelects(IHqlExpression * expr);
    IHqlExpression * createRangeCompare(IHqlExpression * selector, IHqlExpression * value, IHqlExpression * lengthExpr, bool compareEqual);
    void createStringSet(BuildCtx & ctx, const char * target, unsigned size, ITypeInfo * type);
    KeyCondition * createTranslatedCondition(IHqlExpression * cond, KeyedKind keyedKind);
    bool extractBoolFieldFilter(KeyConditionInfo & matches, IHqlExpression * selector, KeyedKind keyedKind, bool compareValue);
    bool extractFilters(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
    void extractFoldedWildFields(IHqlExpression * expr);
    bool extractIfFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind);
    bool extractSimpleCompareFilter(KeyConditionInfo & state, IHqlExpression * expr, KeyedKind keyedKind);
    void expandKeyableFields();
    void expandSelects(IHqlExpression * expr, IHqlSimpleScope * expandedScope, IHqlExpression * keySelector, IHqlExpression * expandedSelector);;
    bool extractOrFilter(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
    void generateFormatWrapping(StringBuffer & createMonitorText, IHqlExpression * selector, IHqlExpression * expandedSelector, unsigned curOffset);
    void generateOffsetWrapping(StringBuffer & createMonitorText, IHqlExpression * selector, unsigned curOffset);
    IHqlExpression * getMonitorValueAddress(BuildCtx & ctx, IHqlExpression * value);
    IHqlExpression * getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary);
    IHqlExpression * invertTransforms(IHqlExpression * left, IHqlExpression * right);
    bool isEqualityFilter(IHqlExpression * select);
    bool isKeySelect(IHqlExpression * select);
    bool isIndexInvariant(IHqlExpression * expr);
    bool isPrevSelectKeyed(IHqlExpression * select);
    bool matchSubstringFilter(KeyConditionInfo & matches, node_operator op, IHqlExpression * left, IHqlExpression * right, KeyedKind keyedKind, bool & duplicate);
    IHqlExpression * isKeyableFilter(IHqlExpression * left, IHqlExpression * right, bool & duplicate, node_operator compareOp, KeyFailureInfo & reason, KeyedKind keyedKind);
    bool okToKey(IHqlExpression * select, KeyedKind keyedKind);
    IHqlExpression * queryKeyableSelector(IHqlExpression * expr);
    IHqlExpression * querySimpleJoinValue(IHqlExpression * field);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * expr, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector, bool isTranslated);
    void extractCompareInformation(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * value, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector, bool isTranslated);
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
    UnsignedArray mergedSizes;
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
    bool allowTranslatedConds;
    bool ignoreUnkeyed;
    bool cleanlyKeyedExplicitly;
    bool keyedExplicitly;
    bool allowDynamicFormatChange;
};

//---------------------------------------------------------------------------

struct VirtualFieldsInfo
{
public:
    VirtualFieldsInfo()
    { 
        virtualsAtEnd = true;
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
    bool            virtualsAtEnd;
    bool            requiresDeserialize;
};

//---------------------------------------------------------------------------

unsigned getProjectCount(IHqlExpression * expr);

#endif
