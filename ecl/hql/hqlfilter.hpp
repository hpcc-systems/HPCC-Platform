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
#ifndef __HQLFILTER_HPP_
#define __HQLFILTER_HPP_

enum KeyedKind { KeyedYes, KeyedNo, KeyedExtend };
struct HQL_API KeyCondition : public CInterface
{
public:
    KeyCondition()          { keyedKind = KeyedNo; isWild = false; generated = false; wasKeyed = false; }
    KeyCondition(IHqlExpression * _selector, IHqlExpression * _expr, KeyedKind _keyedKind, IHqlExpression * _subrange)
                            { selector.set(_selector); subrange.set(_subrange); expr.set(_expr); keyedKind = _keyedKind; isWild = false; generated = false; wasKeyed = isKeyed(); }

    bool isKeyed()          { return (keyedKind != KeyedNo); }

    HqlExprAttr     selector;
    HqlExprAttr     subrange;
    HqlExprAttr     expr;
    KeyedKind       keyedKind;
    bool            isWild;
    bool            generated;
    bool            wasKeyed;

};

typedef CIArrayOf<KeyCondition> KeyConditionArray;

class HQL_API KeyConditionInfo : public CInterface
{
public:
    void appendPreFilter(IHqlExpression * expr) { extendAndCondition(preFilter, expr); }
    void appendPostFilter(IHqlExpression * expr) { extendAndCondition(postFilter, expr); }
    void appendCondition(KeyCondition & next) { conditions.append(next); }

    IHqlExpression * createConjunction();
    bool isSingleMatchCondition() const;

public:
    HqlExprAttr preFilter;          // before activity executed
    HqlExprAttr postFilter;         // after candidate record returned
    KeyConditionArray conditions;
};

//---------------------------------------------------------------------------

enum KeyFailureReason { KFRunknown, KFRnokey, KFRor, KFRtoocomplex, KFRcast };      // ordered
class HQL_API KeyFailureInfo
{
public:
    KeyFailureInfo()  { code = KFRunknown; }

    void clear()                                                    { code = KFRunknown; }
    void merge(const KeyFailureInfo & other);
    void reportError(IErrorReceiver & errorReceiver, IHqlExpression * condition);
    void set(KeyFailureReason _code)                                { code = _code; }
    void set(KeyFailureReason _code, IHqlExpression * _field)       { code = _code; field.set(_field); }

protected:
    KeyFailureReason code;
    OwnedHqlExpr field;
};


enum MonitorFilterKind { NoMonitorFilter, MonitorFilterSkipEmpty, MonitorFilterSkipAll };

struct HQL_API KeySelectorInfo
{
public:
    KeySelectorInfo(KeyedKind _keyedKind, IHqlExpression * _selector, IHqlExpression * _subrange, IHqlExpression * _expandedSelector, unsigned _fieldIdx, size32_t _offset, size32_t _size)
    {
        keyedKind = _keyedKind;
        selector = _selector;
        subrange = _subrange;
        expandedSelector = _expandedSelector;
        fieldIdx = _fieldIdx;
        offset = _offset;
        size = _size;
    }

    const char * getFFOptions();

    IHqlExpression * selector;
    IHqlExpression * subrange;
    IHqlExpression * expandedSelector;
    unsigned fieldIdx;
    size32_t offset;
    size32_t size;
    KeyedKind keyedKind;
};

class HQL_API FilterExtractor
{
public:
    FilterExtractor(IErrorReceiver & _errorReceiver, IHqlExpression * _tableExpr, int _numKeyableFields, bool isDiskRead, bool _createValueSets);

    void appendFilter(IHqlExpression * expr)                { keyed.appendPostFilter(expr); }
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
    void reportFailureReason(IHqlExpression * cond);
    const char * queryKeyName(StringBuffer & s);
    void preventMerge(IHqlExpression * select)              { if (select) noMergeSelects.append(*select); }

    bool isEqualityFilterBefore(IHqlExpression * select);
    unsigned queryKeySelectIndex(IHqlExpression * select)   { return keyableSelects.find(*select); }
    bool isSingleMatchCondition() const;

    IFieldFilter * createSingleFieldFilter(IRtlFieldTypeDeserializer &deserializer) const;
    IFieldFilter * createFieldFilter(IRtlFieldTypeDeserializer &deserializer, IHqlExpression * selector) const;

protected:
    bool containsTableSelects(IHqlExpression * expr);
    IHqlExpression * createRangeCompare(IHqlExpression * selector, IHqlExpression * value, IHqlExpression * lengthExpr, bool compareEqual);
    KeyCondition * createTranslatedCondition(IHqlExpression * cond, KeyedKind keyedKind);
    bool extractBoolFieldFilter(KeyConditionInfo & matches, IHqlExpression * selector, KeyedKind keyedKind, bool compareValue);
    bool extractFilters(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
    void extractFoldedWildFields(IHqlExpression * expr);
    bool extractIfFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind);
    bool extractSimpleCompareFilter(KeyConditionInfo & state, IHqlExpression * expr, KeyedKind keyedKind);
    void expandKeyableFields();
    void expandSelects(IHqlExpression * expr, IHqlSimpleScope * expandedScope, IHqlExpression * keySelector, IHqlExpression * expandedSelector);;
    bool extractOrFilter(KeyConditionInfo & matches, IHqlExpression * filter, KeyedKind keyedKind);
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
    IHqlExpression * unwindConjunction(HqlExprArray & matches, IHqlExpression * expr);

    virtual IHqlExpression * getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary);

    IValueSet * createValueSetExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const;
    IValueSet * createValueSetInExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const;
    IValueSet * createValueSetCompareExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const;

protected:
    IErrorReceiver & errorReceiver;
    IHqlExpression * tableExpr;
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
    bool excludeVirtuals;
    bool cleanlyKeyedExplicitly;
    bool keyedExplicitly;
    bool allowDynamicFormatChange;
    const bool createValueSets;
};

extern HQL_API IHqlExpression * getExplicitlyPromotedCompare(IHqlExpression * filter);
extern HQL_API IHqlExpression * castToFieldAndBack(IHqlExpression * left, IHqlExpression * right);

#endif
