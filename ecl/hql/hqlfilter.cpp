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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"
#include "eclrtl_imp.hpp"
#include "rtlkey.hpp"

#include "hql.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlthql.hpp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"

#include "hqlfold.hpp"
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqlfilter.hpp"
#include "hqlerrors.hpp"


//-----------------------------------------------------------------------------------------------
//-- Index processing
//-----------------------------------------------------------------------------------------------

/*

  Note on generating segment monitors for conditions (ty)x = Y

  1) casting from Tx to Ty loses information.  E.g., (int1)string2field = int1value.
     In this case it is almost impossible to generate a segment monitor because we would need to work out all
     the possibly values for x which could generate the value y.
     The only exception is an inequality (and no_notin), which we can use to remove some of the candidates.  The test will always need
     duplicating since we cannot remove all the expected values.

  2) Casting from Ty to Tx loses information.  E.g., (string)string2field = stringvalue
     In this case we can process the filter without prefiltering by testing whether isExact: (Ty)(Tx)Y == Y, and following the following rules:
     a) no_eq.  If isExact, add value else nothing.
     b) no_ne.  If isExtact, remove value else nothing.
     c) no_gt.  Always add > value
     d) no_ge.  If isExact add >= value else add > value
     e) no_lt.  If isExact add < value else add <= value
     f) no_le.  Always add <= value

  3) Note casts must be present on both sides to indicate exactly what type the comparison will be done as.  This includes (string)which would
     normally be missing if the field was of type string<n>.

*/

static node_operator getModifiedOp(node_operator op, bool duplicate)
{
    if (!duplicate)
        return op;

    switch (op)
    {
    case no_eq:
    case no_le:
    case no_ge:
    case no_in:
        return op;
        //err on the side of caution for the segment monitors -
        //the test later which check it more accurately.
    case no_gt:
        return no_ge;
    case no_lt:
        return no_le;
    case no_ne:
    case no_notin:
        return no_none;
    default:
        UNIMPLEMENTED;
    }
}

static IHqlExpression * querySubStringRange(IHqlExpression * expr)
{
    for(;;)
    {
        switch (expr->getOperator())
        {
        case no_substring:
            return expr->queryChild(1);
        case no_cast:
        case no_implicitcast:
            break;
        default:
            return nullptr;
        }
        expr = expr->queryChild(0);
    }
}

void KeyFailureInfo::merge(const KeyFailureInfo & other)
{
    if (code < other.code)
        set(other.code, other.field);
}

void KeyFailureInfo::reportError(IErrorReceiver & errorReceiver, IHqlExpression * condition)
{
    StringBuffer ecl;
    getExprECL(condition, ecl);
    switch (code)
    {
    case KFRunknown:
        errorReceiver.throwError1(HQLERR_KeyedJoinTooComplex, ecl.str());
    case KFRnokey:
        errorReceiver.throwError1(HQLERR_KeyAccessNoKeyField, ecl.str());
    case KFRtoocomplex:
        errorReceiver.throwError1(HQLERR_KeyedJoinTooComplex, ecl.str());
    case KFRcast:
        errorReceiver.throwError2(HQLERR_KeyAccessNeedCast, ecl.str(), str(field->queryName()));
    case KFRor:
        errorReceiver.throwError1(HQLERR_OrMultipleKeyfields, ecl.str());
    }
}


IHqlExpression * KeyConditionInfo::createConjunction()
{
    LinkedHqlExpr result = preFilter;
    ForEachItemIn(i, conditions)
        extendAndCondition(result, conditions.item(i).expr);
    extendAndCondition(result, postFilter);
    return result.getClear();
}

bool KeyConditionInfo::isSingleMatchCondition() const
{
    if (preFilter || postFilter)
        return false;

    IHqlExpression * prevSelector = nullptr;
    ForEachItemIn(i, conditions)
    {
        KeyCondition & condition = conditions.item(i);
        IHqlExpression * selector = condition.selector;
        if (!prevSelector)
            prevSelector = selector;
        else if (prevSelector != selector)
            return false;
    }
    return (prevSelector != nullptr);
}

//---------------------------------------------------------------------------------------------------------------------

const char * KeySelectorInfo::getFFOptions()
{
    switch (keyedKind)
    {
    case KeyedExtend:
        return "FFopt";
    default:
        return "FFkeyed";
    }
}

//---------------------------------------------------------------------------------------------------------------------

IHqlExpression * getExplicitlyPromotedCompare(IHqlExpression * filter)
{
    switch (filter->getOperator())
    {
    case no_in:
    case no_notin:
        return LINK(filter);
    }
    IHqlExpression * l = filter->queryChild(0);
    IHqlExpression * r = filter->queryChild(1);
    ITypeInfo * lType = queryUnqualifiedType(l->queryType());
    ITypeInfo * rType = queryUnqualifiedType(r->queryType());
    if (lType == rType)
        return LINK(filter);

    //Add explicit casts to the type.  ensureExprType won't add a (string) to a string2 field.
    Owned<ITypeInfo> promotedType = getPromotedECLType(lType, rType);
    HqlExprArray args;
    if (lType == promotedType)
        args.append(*LINK(l));
    else
        args.append(*createValue(no_implicitcast, LINK(promotedType), LINK(l)));
    args.append(*ensureExprType(r, promotedType));
    return filter->clone(args);
}

//---------------------------------------------------------------------------------------------------------------------

static IHqlExpression * createExpandedRecord(IHqlExpression * expr);

static ITypeInfo * getExpandedFieldType(ITypeInfo * type, IHqlExpression * expr)
{
    Linked<ITypeInfo> expandedType = type;
    if (type->getSize() == UNKNOWN_LENGTH)
        expandedType.clear();
    switch (type->getTypeCode())
    {
    case type_packedint:
        expandedType.setown(makeIntType(type->queryPromotedType()->getSize(), type->isSigned()));
        break;
    case type_bitfield:
        expandedType.set(type->queryPromotedType());
        break;
    case type_varstring:
    case type_varunicode:
#if 0
        if (type->getSize() != UNKNOWN_LENGTH)
        {
            unsigned len = type->getStringLen();
            switch (type->getTypeCode())
            {
            case type_varstring:
                expandedType.setown(makeStringType(len, LINK(type->queryCharset()), LINK(type->queryCollation())));
                break;
            case type_varunicode:
                expandedType.setown(makeUnicodeType(len, type->queryLocale()));
                break;
            }
            break;
        }
#endif              //fall through
    case type_data:
    case type_qstring:
    case type_string:
    case type_unicode:
    case type_utf8:
        if (type->getSize() == UNKNOWN_LENGTH)
        {
            unsigned maxLength = UNKNOWN_LENGTH;
            IHqlExpression * maxSizeExpr = expr ? queryAttributeChild(expr, maxSizeAtom, 0) : NULL;
            if (maxSizeExpr)
            {
                unsigned maxSize = (unsigned)maxSizeExpr->queryValue()->getIntValue();
                switch (type->getTypeCode())
                {
                case type_data:
                case type_string:
                    maxLength = maxSize - sizeof(size32_t);
                    break;
                case type_qstring:
                    maxLength = rtlQStrLength(maxSize - sizeof(size32_t));
                    break;
                case type_unicode:
                    maxLength = (maxSize-sizeof(size32_t))/sizeof(UChar);
                    break;
                case type_utf8:
                    maxLength = (maxSize-sizeof(size32_t))/4;
                    break;
                case type_varstring:
                    maxLength = maxSize - 1;
                    break;
                case type_varunicode:
                    maxLength = (maxSize/sizeof(UChar)) - 1;
                    break;
                }
            }
            else
            {
                IHqlExpression * maxLengthExpr = expr ? queryAttributeChild(expr, maxLengthAtom, 0) : NULL;
                if (maxLengthExpr)
                    maxLength = (unsigned)maxLengthExpr->queryValue()->getIntValue();
            }

            if (maxLength != UNKNOWN_LENGTH)
            {
                switch (type->getTypeCode())
                {
                case type_data:
                    expandedType.setown(makeDataType(maxLength));
                    break;
                case type_qstring:
                    expandedType.setown(makeQStringType(maxLength));
                    break;
                case type_string:
                    expandedType.setown(makeStringType(maxLength, LINK(type->queryCharset()), LINK(type->queryCollation())));
                    break;
                case type_unicode:
                    expandedType.setown(makeUnicodeType(maxLength, type->queryLocale()));
                    break;
                case type_utf8:
                    expandedType.setown(makeUtf8Type(maxLength, type->queryLocale()));
                    break;
                case type_varstring:
                    expandedType.setown(makeVarStringType(maxLength, LINK(type->queryCharset()), LINK(type->queryCollation())));
                    break;
                case type_varunicode:
                    expandedType.setown(makeVarUnicodeType(maxLength, type->queryLocale()));
                    break;
                }
            }
        }
        else
        {
            //This could ensure the strings are ascii, but the ebcdic strings are still comparable, and the order will be more logical
            //if they remain as ebcdic.
        }
        break;
    case type_table:
    case type_groupedtable:
    case type_set:
        expandedType.clear();
        break;
    case type_row:
        {
            OwnedHqlExpr newRecord = createExpandedRecord(queryRecord(type));
            if (isEmptyRecord(newRecord))
                expandedType.clear();
            else
                expandedType.setown(makeRowType(LINK(newRecord->queryRecordType())));
            break;
        }
    case type_alien:
        {
            IHqlAlienTypeInfo * alien = queryAlienType(type);
            expandedType.set(alien->queryLogicalType());
            break;
        }
    }
    return expandedType.getClear();
}

static void createExpanded(HqlExprArray & fields, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        //if blocks need to generate translated segment monitors to be keyed, so don't expand them
        break;
    case no_record:
        {
            ForEachChild(i, expr)
                createExpanded(fields, expr->queryChild(i));
            break;
        }
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            Owned<ITypeInfo> expandedType = getExpandedFieldType(type, expr);
            if (expandedType)
            {
                if (expandedType == type)
                    fields.append(*LINK(expr));
                else
                {
                    HqlExprArray attrs;
                    unwindChildren(attrs, expr);
                    //MORE: Any default will now have the wrong type => remove it for the moment (ideally it would be projected)
                    removeAttribute(attrs, defaultAtom);
                    fields.append(*createField(expr->queryId(), LINK(expandedType), attrs));
                }
            }
            break;
        }
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        fields.append(*LINK(expr));
        break;
    }
}

static IHqlExpression * createExpandedRecord(IHqlExpression * expr)
{
    HqlExprArray fields;
    createExpanded(fields, expr);
    return cloneOrLink(expr, fields);
}


IHqlExpression * castToFieldAndBack(IHqlExpression * left, IHqlExpression * right)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * castType = right->queryType();
            ITypeInfo * uncastType = uncast->queryType();

            OwnedHqlExpr castRight = ensureExprType(right, uncastType);
            OwnedHqlExpr base = castToFieldAndBack(uncast, castRight);
            //If this cast doesn't lose any information and child didn't change then don't bother
            //casting back and forwards.
            if ((base == castRight) && !castLosesInformation(uncastType, castType))
                return LINK(right);
            return ensureExprType(base, castType);
        }

    case no_select:
        {
            ITypeInfo * leftType = left->queryType();
            ITypeInfo * rightType = right->queryType();
            if (leftType == rightType || !castLosesInformation(leftType, rightType))
                return LINK(right);
            OwnedHqlExpr castToField = ensureExprType(right, leftType);
            return ensureExprType(castToField, rightType);
        }
    case no_substring:
    {
        OwnedHqlExpr cast = castToFieldAndBack(left->queryChild(0), right);
        //Theoretically needed for all types.  In practice this only makes a difference for data since strings
        //ignore trailing spaces
        if (left->queryType()->getTypeCode() == type_data)
            return replaceChild(left, 0, cast.getClear());
        return cast.getClear();
    }
    case no_add:
    case no_sub:
        return castToFieldAndBack(left->queryChild(0), right);
    default:
        throwUnexpected();
    }
}



//---------------------------------------------------------------------------------------------------------------------

FilterExtractor::FilterExtractor(IErrorReceiver & _errorReceiver, IHqlExpression * _tableExpr, int _numKeyableFields, bool _isDiskRead, bool _createValueSets)
    : errorReceiver(_errorReceiver), createValueSets(_createValueSets)
{
    tableExpr = _tableExpr;

    if (_numKeyableFields <= 0)
    {
        //-ve number means remove a certain number of fields from the record
        IHqlExpression * record = tableExpr->queryRecord();
        numKeyableFields = 0;
        ForEachChild(i, record)
            if (!record->queryChild(i)->isAttribute())
                numKeyableFields++;
        numKeyableFields += _numKeyableFields;          // remove payload fields.
    }
    else
        numKeyableFields = (unsigned)_numKeyableFields;

    onlyHozedCompares = !_isDiskRead;
    excludeVirtuals = _isDiskRead;

    expandKeyableFields();
    cleanlyKeyedExplicitly = false;
    keyedExplicitly = false;
    allowDynamicFormatChange = !tableExpr->hasAttribute(fixedAtom);
}

bool FilterExtractor::isSingleMatchCondition() const
{
    return keyed.isSingleMatchCondition();
}

void FilterExtractor::expandKeyableFields()
{
    HqlExprArray fields;
    IHqlExpression * tableRecord = tableExpr->queryRecord();
    unsigned cnt = 0;
    ForEachChild(i, tableRecord)
    {
        if (cnt == numKeyableFields)
            break;
        IHqlExpression * cur = tableRecord->queryChild(i);
        if (!cur->isAttribute())
        {
            if (!(excludeVirtuals && cur->hasAttribute(virtualAtom)))
            {
                fields.append(*LINK(cur));
                cnt++;
            }
        }
    }
    keyableRecord.setown(createRecord(fields));

    if (createValueSets)
        expandedRecord.set(keyableRecord);
    else
        expandedRecord.setown(createExpandedRecord(keyableRecord));
    IHqlExpression * selector = tableExpr->queryNormalizedSelector();

    OwnedHqlExpr expandedSelector = createDataset(no_anon, LINK(expandedRecord), createUniqueId());
    firstOffsetField = NotFound;
    expandSelects(keyableRecord, expandedRecord->querySimpleScope(), selector, expandedSelector);
    if (firstOffsetField == NotFound)
        firstOffsetField = keyableSelects.ordinality();
}

void FilterExtractor::expandSelects(IHqlExpression * expr, IHqlSimpleScope * expandedScope, IHqlExpression * keySelector, IHqlExpression * expandedSelector)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                expandSelects(expr->queryChild(i), expandedScope, keySelector, expandedSelector);
            break;
        }
    case no_ifblock:
        expandSelects(expr->queryChild(1), expandedScope, keySelector, expandedSelector);
        break;
    case no_field:
        {
            OwnedHqlExpr match = expandedScope->lookupSymbol(expr->queryId());
            if (match)
            {
                OwnedHqlExpr keySelected = createSelectExpr(LINK(keySelector), LINK(expr));
                OwnedHqlExpr expandedSelected = createSelectExpr(LINK(expandedSelector), LINK(match));
                IHqlExpression * record = expr->queryRecord();
                if (expr->isDatarow())
                    expandSelects(record, match->queryRecord()->querySimpleScope(), keySelected, expandedSelected);
                else
                {
                    if ((expr != match) && (firstOffsetField == NotFound))
                    {
                        ITypeInfo * exprType = expr->queryType();
                        ITypeInfo * matchType = match->queryType();
                        if ((exprType->getSize() != matchType->getSize()) ||
                            (exprType->getTypeCode() == type_bitfield || matchType->getTypeCode() == type_bitfield))
                            firstOffsetField = keyableSelects.ordinality();
                    }

                    keyableSelects.append(*LINK(keySelected));
                    expandedSelects.append(*LINK(expandedSelected));
                }
            }
            else
            {
                if (firstOffsetField == NotFound)
                    firstOffsetField = keyableSelects.ordinality();
            }
            break;
        }
    }
}

IHqlExpression * FilterExtractor::unwindConjunction(HqlExprArray & matches, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    expr->unwindList(matches, op);
    OwnedHqlExpr invariant;
    ForEachItemInRev(i, matches)
    {
        IHqlExpression & cur = matches.item(i);
        if (isIndexInvariant(&cur, false))
        {
            invariant.setown(extendConditionOwn(op, LINK(&cur), invariant.getClear()));
            matches.remove(i);
        }
    }
    return invariant.getClear();
}


bool FilterExtractor::isKeySelect(IHqlExpression * select)
{
    return (keyableSelects.find(*select) != NotFound);
}

bool FilterExtractor::isEqualityFilter(IHqlExpression * search)
{
    bool matched = false;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == search)
        {
            if (!cur.isWild)
            {
                if (matched)
                    return false;
                matched = true;
                IHqlExpression * matchExpr = cur.expr;
                if (matchExpr->getOperator() != no_eq)
                    return false;
            }
        }
    }
    return matched;
}

bool FilterExtractor::isEqualityFilterBefore(IHqlExpression * select)
{
    ForEachItemIn(i, keyableSelects)
    {
        IHqlExpression & cur = keyableSelects.item(i);
        if (select == &cur)
            return true;
        if (!isEqualityFilter(&cur))
            return false;
    }
    throwUnexpected();
}

bool FilterExtractor::isPrevSelectKeyed(IHqlExpression * select)
{
    unsigned match = keyableSelects.find(*select);
    assertex(match != NotFound);
    if (match == 0)
        return true;
    IHqlExpression * prev = &keyableSelects.item(match-1);
    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector == prev)
        {
            if (!cur.isWild && cur.isKeyed())
                return true;
            if (cur.wasKeyed)
                return true;
        }
    }
    return false;
}


bool FilterExtractor::okToKey(IHqlExpression * select, KeyedKind keyedKind)
{
    if (keyedKind == KeyedYes)
        return true;

    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector == select && cur.isWild)
            return false;
    }

    return true;
}

bool FilterExtractor::isIndexInvariant(IHqlExpression * expr, bool includeRoot)
{
    if (containsAssertKeyed(expr))
        return false;

    HqlExprCopyArray scopeUsed;
    expr->gatherTablesUsed(scopeUsed);

    IHqlExpression * search = tableExpr->queryNormalizedSelector();
    ForEachItemIn(i, scopeUsed)
    {
        IHqlExpression * cur = &scopeUsed.item(i);
        for (;;)
        {
            if (cur == search)
                return false;

            if (includeRoot && (queryRoot(cur) == search))
                return false;

            IHqlExpression * parent = queryNextMultiLevelDataset(cur, true);
            if (!parent)
                break;
            cur = parent;
        }
    }
    return true;
}




IHqlExpression * FilterExtractor::invertTransforms(IHqlExpression * left, IHqlExpression * right)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            assertex(right->queryType()->getTypeCode() != type_set);

            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * uncastType = uncast->queryType();


            OwnedHqlExpr castRight = ensureExprType(right, uncastType);
            return invertTransforms(uncast, castRight);
        }

    case no_select:
        {
            assertex(isKeySelect(left));
            ITypeInfo * leftType = left->queryType();
            ITypeInfo * rightType = right->queryType();
            if (!createValueSets)
            {
                if (leftType == rightType || !castLosesInformation(leftType, rightType))
                    return LINK(right);
            }
            return ensureExprType(right, leftType);
        }

    case no_add:
    case no_sub:
        {
            assertex(right->getOperator() != no_list);

            OwnedHqlExpr adjusted = createValue(op == no_sub ? no_add : no_sub, right->getType(), LINK(right), LINK(left->queryChild(1)));
            return invertTransforms(left->queryChild(0), adjusted);
        }
     case no_substring:
         {
             assertex(right->getOperator() != no_list);

             return invertTransforms(left->queryChild(0), right);
         }
    default:
        UNIMPLEMENTED;
    }
}



IHqlExpression * FilterExtractor::queryKeyableSelector(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
    case no_add:
    case no_sub:
        return queryKeyableSelector(expr->queryChild(0));
    case no_select:
        if (isKeySelect(expr))
            return expr;
        return NULL;
    }
    return NULL;
}

IHqlExpression * FilterExtractor::isKeyableFilter(IHqlExpression * left, IHqlExpression * right, bool & duplicate, node_operator compareOp, KeyFailureInfo & reason, KeyedKind keyedKind)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * castType = left->queryType();
            ITypeInfo * uncastType = uncast->queryType();

            //Keyed filters on alien datatypes do not work, and can trigger an internal error in ensureExprType()
            if (uncastType->getTypeCode() == type_alien)
            {
                reason.set(KFRtoocomplex, left);
                return nullptr;
            }

            //(ty)x = y.            E.g., (int1)string2field = int1value
            //if more than one value of x[uncastType] corresponds to a single value in y[castType] then we can't sensibly create
            //the key segment monitor.  Because we will get false negatives.  If it is an inverse then duplicate (see below)
            bool canRemoveCast = true;
            if (castLosesInformation(castType, uncastType))
            {
                if ((compareOp != no_ne) && (compareOp != no_notin))
                    canRemoveCast = false;
                duplicate = true;
            }

            //if more than one value of y corresponds to a single value of x then need to duplicate the test condition.
            //or pretest whether (ty)(tx)y == y.
            //if (castLosesInformation(uncastType, castType))
            //Now taken care of when the segment monitors are created

            //If the comparison is non equality and the cast changes the collation sequence then you can't remove it.
            switch (compareOp)
            {
            case no_eq:
            case no_ne:
            case no_in:
            case no_notin:
                break;
            default:
                if (!preservesOrder(castType, uncastType))
                    canRemoveCast = false;
                break;
            }

            Linked<ITypeInfo> newType = uncastType;
            if (right->queryType()->getTypeCode() == type_set)
                newType.setown(makeSetType(newType.getLink()));
            OwnedHqlExpr castRight = ensureExprType(right, newType);
            IHqlExpression * ret = isKeyableFilter(uncast, castRight, duplicate, compareOp, reason, keyedKind);
            if (canRemoveCast || !ret)
                return ret;
            reason.set(KFRcast, ret->queryChild(1));
            return NULL;
        }

    case no_select:
        if (isKeySelect(left) && okToKey(left, keyedKind))
        {
            if (isIndexInvariant(right, false))
                return left;
            reason.set(KFRtoocomplex, left);
        }
        else
            reason.set(KFRnokey);
        return NULL;

    case no_substring:
        {
            IHqlExpression * range = left->queryChild(1);
            if (range->getOperator() == no_rangeto)
            {
                IValue *end = range->queryChild(0)->queryValue();
                if (!createValueSets && !end)
                    break;
                return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
            }
            else if (range->getOperator() == no_range)
            {
                if (!matchesConstantValue(range->queryChild(0), 1))
                    break;
                if (!createValueSets && !range->queryChild(1)->queryValue())
                    break;
                return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
            }
            reason.set(KFRtoocomplex, right);
            return NULL;
        }
    case no_add:
    case no_sub:
        if (isIndexInvariant(left->queryChild(1), false))
            return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
        reason.set(KFRtoocomplex, left);
        return NULL;
    }
    reason.set(KFRnokey);
    return NULL;
}



static IHqlExpression * getCompareValue(ITypeInfo * fieldType, unsigned subStringLen, IValue * value, int whichBoundary)
{
    type_t ftc = fieldType->getTypeCode();
    unsigned fieldLen = fieldType->getStringLen();
    unsigned lenValue = value->queryType()->getStringLen();
    const void * rawValue = value->queryValue();
    size32_t resultLen;
    rtlDataAttr result;

    if (whichBoundary < 0)
    {
        switch (ftc)
        {
        case type_qstring:
            rtlCreateQStrRangeLow(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_string:
            rtlCreateStrRangeLow(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_data:
            rtlCreateDataRangeLow(resultLen, result.refdata(), fieldLen, subStringLen, lenValue, rawValue);
            break;
        case type_unicode:
            rtlCreateUnicodeRangeLow(resultLen, result.refustr(), fieldLen, subStringLen, lenValue, static_cast<const UChar *>(rawValue));
            break;
        default:
            //should this generate a warning/error instead?
            rtlCreateRange(resultLen, result.refstr(), fieldLen, subStringLen, fieldType->getSize(), static_cast<const char *>(rawValue), 0, 0);
            break;
        }
    }
    else
    {
        switch (ftc)
        {
        case type_qstring:
            rtlCreateQStrRangeHigh(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_string:
            rtlCreateStrRangeHigh(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_data:
            rtlCreateDataRangeHigh(resultLen, result.refdata(), fieldLen, subStringLen, lenValue, rawValue);
            break;
        case type_unicode:
            rtlCreateUnicodeRangeHigh(resultLen, result.refustr(), fieldLen, subStringLen, lenValue, static_cast<const UChar *>(rawValue));
            break;
        default:
            rtlCreateRange(resultLen, result.refstr(), fieldLen, subStringLen, fieldType->getSize(), static_cast<const char *>(rawValue), 255, 0);
            break;
        }
    }
    assertex(resultLen == fieldLen);
    return createConstant(createValueFromMem(LINK(fieldType), result.getdata()));
}


static IHqlExpression * removeCastTrim(IHqlExpression * expr)
{
    for (;;)
    {
        if ((expr->getOperator() == no_trim) && !expr->queryChild(1))
            expr = expr->queryChild(0);
        else if (isLengthPreservingCast(expr))
            expr = expr->queryChild(0);
        else
            return expr;

        expr = queryNonAliased(expr);
    }
}

static IHqlExpression * queryLengthFromRange(IHqlExpression * range)
{
    switch (range->getOperator())
    {
    case no_rangefrom:
    case no_rangecommon:
        return NULL;
    case no_rangeto:
        return range->queryChild(0);
    case no_range:
        if (getIntValue(range->queryChild(0), 0) == 1)
            return range->queryChild(1);
        return NULL;
    default:
        if (getIntValue(range, 0) == 1)
            return range;
        return NULL;
    }
}

static void extendRangeCheck(SharedHqlExpr & globalGuard, SharedHqlExpr & localCond, IHqlExpression * selector, IHqlExpression * lengthExpr, bool compareEqual)
{
#if 0
    //This might be a good idea, but probably doesn't make a great deal of difference at runtime
    //Optimize the case where you check zero length to use a wild carded range instead
    //x[1..len=0] = y from x in range 0000000..FFFFFF to  len==0 || x in range....
    if (compareEqual)
    {
        if (!lengthExpr->queryValue())
        {
            OwnedHqlExpr testLength = createBoolExpr(no_eq, LINK(lengthExpr), ensureExprType(queryZero(), lengthExpr->queryType()));
            localCond.setown(createBoolExpr(no_or, testLength.getClear(), LINK(localCond)));
        }
    }
#endif
    //For a range check x[1..m] = y we generate
    //x in range(sizeof(x), m, y, 0) and range(maxlength(x), m, y, 255)
    //need to guard with condition len(trim(y)) <= m

    //If x[1..length(trim(y))] == y then don't add a condition
    IHqlExpression * cur = queryNonAliased(lengthExpr);
    IHqlExpression * compare = removeCastTrim(queryNonAliased(selector));
    if (cur->getOperator() == no_charlen)
    {
        cur = queryNonAliased(cur->queryChild(0));
        if (cur->getOperator() == no_trim)
        {
            cur = queryNonAliased(cur->queryChild(0));
            while (isLengthPreservingCast(cur))
                cur = queryNonAliased(cur->queryChild(0));

            IHqlExpression * compare = selector;
            for (;;)
            {
                compare = removeCastTrim(queryNonAliased(compare));
                if (cur->queryBody() == compare->queryBody())
                    return;
                //Casts between strings that reduce/increase the number of characters don't matter as long as
                //they eventually match the search string
                if (!isCast(compare) || !isStringType(compare->queryType()))
                    break;
                compare = compare->queryChild(0);
                if (!isStringType(compare->queryType()))
                    break;
            }
        }
    }

    // if x[1..n] = z[1..n] then no need for a condition
    if (compare->getOperator() == no_substring)
    {
        IHqlExpression * range = queryLengthFromRange(compare->queryChild(1));
        if (range == lengthExpr)
            return;
    }

    // if x[1..n] = (string<m>y) where m<n then no need for condition
    unsigned selectorLength = selector->queryType()->getStringLen();
    if (selectorLength <= getIntValue(lengthExpr, 0))
        return;

    //otherwise, if x[1..y] == z then add check length(trim(z)) <= y
    OwnedHqlExpr trim = createTrimExpr(selector, NULL);
    OwnedITypeInfo unsignedType = makeIntType(sizeof(unsigned), false);
    OwnedHqlExpr len = createValue(no_charlen, LINK(unsignedType), LINK(trim));
    ITypeInfo * lengthType = lengthExpr->queryType();
    Owned<ITypeInfo> compareType = getPromotedECLCompareType(unsignedType, lengthType);
    OwnedHqlExpr positiveLen = createValue(no_maxlist, lengthExpr->getType(), createValue(no_list, makeSetType(LINK(lengthType)), LINK(lengthExpr), createConstant(lengthType->castFrom(false, I64C(0)))));
    OwnedHqlExpr test = createValue(no_le, makeBoolType(), ensureExprType(len, compareType), ensureExprType(positiveLen, compareType));
    test.setown(foldHqlExpression(test));
    if (compareEqual)
        extendConditionOwn(globalGuard, no_and, test.getClear());
    else
        extendConditionOwn(localCond, no_or, getInverse(test));
}


IHqlExpression * FilterExtractor::getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary)
{
    unsigned fieldLength = fieldType->getStringLen();
    IValue * constValue = value->queryValue();

    if (constValue && lengthExpr->queryValue())
    {
        unsigned subStringLen = (unsigned)lengthExpr->queryValue()->getIntValue();
        if ((int)subStringLen < 0) subStringLen = 0;
        if (subStringLen > fieldLength)
            errorReceiver.throwError1(HQLERR_SubstringOutOfRange, subStringLen);

        return getCompareValue(fieldType, subStringLen, constValue, whichBoundary);
    }
    return nullptr;
}


IHqlExpression * FilterExtractor::createRangeCompare(IHqlExpression * selector, IHqlExpression * value, IHqlExpression * lengthExpr, bool compareEqual)
{
    OwnedHqlExpr foldedValue = foldHqlExpression(value);
    if (createValueSets)
    {
        OwnedHqlExpr rangeExpr = createValue(no_rangeto, makeNullType(), LINK(lengthExpr));
        OwnedHqlExpr substr = createValue(no_substring, getStretchedType(UNKNOWN_LENGTH, selector->queryType()), LINK(selector), rangeExpr.getClear());
        return createValue(compareEqual ? no_eq : no_ne, makeBoolType(), LINK(substr), foldedValue.getClear());
    }

    ITypeInfo * fieldType = selector->queryType();
    OwnedHqlExpr lowExpr = getRangeLimit(fieldType, lengthExpr, foldedValue, -1);
    OwnedHqlExpr highExpr = getRangeLimit(fieldType, lengthExpr, foldedValue, +1);
    if (!lowExpr || !highExpr)
        errorReceiver.throwError(HQLERR_NonConstantRange);

    //Could convert to two separate tests, but code is worse, and boundary conditions aren't going to happen.
    return createValue(compareEqual ? no_between : no_notbetween, makeBoolType(), LINK(selector), lowExpr.getClear(), highExpr.getClear());
}



bool FilterExtractor::matchSubstringFilter(KeyConditionInfo & matches, node_operator op, IHqlExpression * left, IHqlExpression * right, KeyedKind keyedKind, bool & duplicate)
{
    LinkedHqlExpr value = right;
    duplicate = false;
    OwnedHqlExpr guard;
    ITypeInfo * guardCastType = NULL;

    if ((left->getOperator() == no_cast) || (left->getOperator() == no_implicitcast))
    {
        //code is extracted and simplified from isKeyableFilter() above - should be commoned up.
        IHqlExpression * uncast = left->queryChild(0);
        ITypeInfo * castType = left->queryType();
        ITypeInfo * uncastType = uncast->queryType();

        //(ty)x = y.
        //if more than one value of x[uncastType] corresponds to a single value in y[castType] then we can't sensibly create
        //the key segment monitor.  Because we will get false negatives.  If it is an inverse then duplicate (see below)
        bool canRemoveCast = true;
        if (castLosesInformation(castType, uncastType))
            canRemoveCast = false;

        //if more than one value of y corresponds to a single value of x then need to duplicate the test condition.
        if (!preservesOrder(castType, uncastType))
            canRemoveCast = false;

        if (!canRemoveCast)
        {
//          reason.set(KFRcast, ret->queryChild(1));
            return false;
        }

        if ((op != no_in) && (op != no_notin))
        {
            value.setown(ensureExprType(right, uncastType));

            //If a simple equality test then create a global guard to check that we aren't matching a false positive
            if (castLosesInformation(uncastType, castType))
                guard.setown(createBoolExpr(no_eq, ensureExprType(value, castType), LINK(right)));
        }
        else
        {
            //if an IN then add guards to each comparison - generated later...
            if (castLosesInformation(uncastType, castType))
                guardCastType = uncastType;
            else
            {
                Owned<ITypeInfo> targetType = makeSetType(LINK(uncastType));
                value.setown(ensureExprType(right, targetType));
            }
        }

        left = uncast;
    }

    if (left->getOperator() != no_substring)
        return false;

    if ((op == no_in) || (op == no_notin))
    {
        value.setown(normalizeListCasts(value));
        if (value->getOperator() != no_list)
            return false;
    }

    IHqlExpression * selector = left->queryChild(0);
    if (!isKeySelect(selector) || !okToKey(selector, keyedKind))
        return false;
    if (!isIndexInvariant(right, false))
        return false;
    ITypeInfo * fieldType = selector->queryType();
    unsigned fieldLength = fieldType->getStringLen();
    if (!createValueSets && (fieldLength == UNKNOWN_LENGTH))
        return false;

    OwnedHqlExpr range = foldHqlExpression(left->queryChild(1));
    IHqlExpression * lengthExpr = queryLengthFromRange(range);
    if (!lengthExpr)
        return false;

    OwnedHqlExpr newTest;
    if ((op == no_eq) || (op == no_ne))
    {
        newTest.setown(createRangeCompare(selector, value, lengthExpr, op == no_eq));
        extendRangeCheck(guard, newTest, right, lengthExpr, op == no_eq);
    }
    else //no_in, no_notin
    {
        HqlExprArray compares;
        ForEachChild(i, value)
        {
            IHqlExpression * cur = value->queryChild(i);
            LinkedHqlExpr castValue = cur;
            OwnedHqlExpr valueGuard;

            if (guardCastType)
            {
                castValue.setown(ensureExprType(castValue, guardCastType));
                valueGuard.setown(createBoolExpr(no_eq, ensureExprType(castValue, cur->queryType()), LINK(cur)));
                extendRangeCheck(valueGuard, valueGuard, cur, lengthExpr, (op == no_in));
            }
            OwnedHqlExpr cond = createRangeCompare(selector, castValue, lengthExpr, (op == no_in));
            if (valueGuard)
                cond.setown(createValue(no_and, makeBoolType(), valueGuard.getClear(), cond.getClear()));
            compares.append(*cond.getClear());
        }

        node_operator combineOp = (op == no_in) ? no_or : no_and;
        OwnedITypeInfo boolType = makeBoolType();
        newTest.setown(createBalanced(combineOp, boolType, compares));
    }

    KeyCondition * entry = new KeyCondition(selector, newTest, keyedKind, left->queryChild(1));
    matches.appendCondition(*entry);
    if (guard)
        matches.appendPreFilter(guard);
    return true;
}


bool FilterExtractor::extractSimpleCompareFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    OwnedHqlExpr promoted = getExplicitlyPromotedCompare(expr);
    IHqlExpression * l = promoted->queryChild(0);
    IHqlExpression * r = promoted->queryChild(1);
    bool duplicate = false;

    KeyFailureInfo reasonl, reasonr;
    node_operator op = expr->getOperator();
    IHqlExpression * matchedSelector = isKeyableFilter(l, r, duplicate, op, reasonl, keyedKind);
    Owned<KeyCondition> result;
    if (matchedSelector)
    {
        node_operator newOp = getModifiedOp(op, duplicate);

        if (newOp != no_none)
        {
            OwnedHqlExpr newFilter = createValue(newOp, expr->getType(), LINK(l), LINK(r));
            result.setown(new KeyCondition(matchedSelector, newFilter, keyedKind, querySubStringRange(l)));
        }
    }
    else
    {
        duplicate = false;
        matchedSelector = isKeyableFilter(r, l, duplicate, op, reasonr, keyedKind);
        if (matchedSelector)
        {
            node_operator newOp = getModifiedOp(getReverseOp(op), duplicate);
            if (newOp != no_none)
            {
                OwnedHqlExpr newFilter = createValue(newOp, expr->getType(), LINK(r), LINK(l));
                result.setown(new KeyCondition(matchedSelector, newFilter, keyedKind, querySubStringRange(r)));
            }
        }
    }

    bool extracted = (result != NULL);
    if (extracted)
    {
        matches.appendCondition(*result.getClear());
    }
    else
    {
        failReason.merge(reasonl);
        failReason.merge(reasonr);
    }
    if (duplicate || !extracted)
        matches.appendPostFilter(expr);
    return extracted;
}


bool FilterExtractor::extractOrFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    HqlExprArray conds;
    expr->unwindList(conds, no_or);

    bool validOrFilter = true;
    HqlExprAttr invariant;
    CIArrayOf<KeyConditionInfo> branches;
    ForEachItemIn(idx, conds)
    {
        IHqlExpression & cur = conds.item(idx);
        if (isIndexInvariant(&cur, false))
            extendOrCondition(invariant, &cur);
        else
        {
            KeyConditionInfo & branch = * new KeyConditionInfo;
            branches.append(branch);
            //Can't generate an OR with a pure post-filter
            if (!extractFilters(branch, &cur, keyedKind))
                validOrFilter = false;
        }
    }

    //check all the conditions that are ORd together don't contain references to multiple fields.
    KeyCondition * firstBranch = NULL;
    bool multipleBranches = branches.ordinality() > 1;
    bool multipleSelectors = false;
    bool multipleConditions = false;
    bool hasPostFilter = false;
    ForEachItemIn(i1, branches)
    {
        KeyConditionInfo & branch = branches.item(i1);
        if (branch.postFilter)
            hasPostFilter = true;
        ForEachItemIn(i2, branch.conditions)
        {
            KeyCondition & cur = branch.conditions.item(i2);
            if (!firstBranch)
                firstBranch = &cur;
            else
            {
                multipleConditions = true;
                if (i1 == 0)
                {
                    //Check for ((i.x = 3) AND (i.y = 4))
                    //Which can only create a keyed filter if it is ORd with an invariant expression
                    if (firstBranch->selector != cur.selector)
                        multipleSelectors = true;
                }
                else
                {
                    if ((firstBranch->selector != cur.selector) || multipleSelectors)
                        validOrFilter = false;
                }
            }
        }
    }

    if (multipleBranches && hasPostFilter)
        validOrFilter = false;

    if (validOrFilter && firstBranch)
    {
        bool optimizeSingleBranch = true;
        if (multipleSelectors || hasPostFilter || (optimizeSingleBranch && !multipleConditions))
        {
            //Invariant ored with a conjunction
            //X or (A and B) -> (X or A) AND (X or B)
            assertex(branches.ordinality() == 1);
            KeyConditionInfo & branch = branches.item(0);
            OwnedHqlExpr preFilter = branch.preFilter ? extendCondition(no_or, invariant, branch.preFilter) : NULL;
            OwnedHqlExpr postFilter = branch.postFilter ? extendCondition(no_or, invariant, branch.postFilter) : NULL;

            matches.appendPreFilter(preFilter);
            matches.appendPostFilter(postFilter);
            ForEachItemIn(i2, branch.conditions)
            {
                KeyCondition & cur = branch.conditions.item(i2);
                OwnedHqlExpr filter = extendCondition(no_or, invariant, cur.expr);
                matches.conditions.append(*new KeyCondition(cur.selector, filter, keyedKind, cur.subrange));
            }
        }
        else
        {
            LinkedHqlExpr combinedCondition = invariant;

            ForEachItemIn(i1, branches)
            {
                KeyConditionInfo & branch = branches.item(i1);
                OwnedHqlExpr conjunction = branch.createConjunction();
                extendOrCondition(combinedCondition, conjunction);
            }

            matches.conditions.append(*new KeyCondition(firstBranch->selector, combinedCondition, keyedKind, nullptr));
        }
        return true;
    }
    else
    {
        matches.appendPostFilter(expr);
        KeyFailureInfo reason;
        reason.set(KFRor);
        failReason.merge(reason);
        return false;
    }
}


bool FilterExtractor::extractIfFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    //MORE: This could generate better code, but I don't think it is worth the effort at the moment.
    //Really, I should analyse left and right.  Iterate each selector referenced.  If there are no post conditions then
    //generate IF(a, X, Y) compound expression, otherwise generate the default below.
    IHqlExpression * cond = expr->queryChild(0);
    if ((keyedKind != KeyedNo) && isIndexInvariant(cond, false))
    {
        //Convert IF(a, X, Y) to... IF (a, X, true) AND IF (a, true, Y) to... (NOT a OR X) AND (a OR Y)
        OwnedHqlExpr inverseCond = getInverse(cond);
        OwnedHqlExpr trueValue = createBoolExpr(no_or, LINK(inverseCond), LINK(expr->queryChild(1)));
        OwnedHqlExpr falseValue = createBoolExpr(no_or, LINK(cond), LINK(expr->queryChild(2)));
        OwnedHqlExpr combined = createBoolExpr(no_and, LINK(trueValue), LINK(falseValue));
        return extractFilters(matches, combined, keyedKind);
    }
    matches.appendPostFilter(expr);
    return false;
}


bool FilterExtractor::containsTableSelects(IHqlExpression * expr)
{
    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(inScope);

    //Check that cursors for all inScope tables are already bound in the start context
    return inScope.find(*tableExpr->queryNormalizedSelector()) != NotFound;
}


void FilterExtractor::extractFilters(IHqlExpression * expr, SharedHqlExpr & extraFilter)
{
    HqlExprArray conds;
    expr->unwindList(conds, no_and);
    extractFilters(conds, extraFilter);
}

void FilterExtractor::extractFilters(HqlExprArray & exprs, SharedHqlExpr & extraFilter)
{
    OwnedHqlExpr savedFilter = keyed.postFilter.getClear();
    ForEachItemIn(i1, exprs)
    {
        IHqlExpression & cur = exprs.item(i1);
        switch (cur.getOperator())
        {
        case no_assertkeyed:
        case no_assertwild:
            extractFilters(keyed, &cur, KeyedNo);
            break;
        }
    }

    keyedExplicitly = (keyed.conditions.ordinality() != 0);
    cleanlyKeyedExplicitly = keyedExplicitly && !keyed.postFilter;
    ForEachItemIn(i2, exprs)
    {
        IHqlExpression & cur = exprs.item(i2);
        switch (cur.getOperator())
        {
        case no_assertkeyed:
        case no_assertwild:
            break;
        default:
            if (!keyedExplicitly)
                extractFilters(keyed, &cur, KeyedNo);
            else if (!cur.isAttribute() && isIndexInvariant(&cur, true))
                keyed.appendPreFilter(&cur);
            else
                keyed.appendPostFilter(&cur);
            break;
        }
    }

    extraFilter.set(keyed.postFilter);
    keyed.postFilter.setown(extendConditionOwn(no_and, savedFilter.getClear(), LINK(extraFilter)));
}


void FilterExtractor::extractFiltersFromFilterDs(IHqlExpression * expr)
{
    HqlExprArray conds;
    HqlExprAttr dummy;
    unwindFilterConditions(conds, expr);
    extractFilters(conds, dummy);
}



void FilterExtractor::extractFoldedWildFields(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
    case no_add:
    case no_sub:
        //fields may have been transformed since folding...
        extractFoldedWildFields(expr->queryChild(0));
        break;
    case no_select:
        if (isKeySelect(expr))
        {
            KeyCondition * condition = new KeyCondition;
            condition->selector.set(expr);
            condition->isWild = true;
            condition->wasKeyed = true;
            keyed.conditions.append(*condition);
        }
        break;
    }
}

bool FilterExtractor::extractBoolFieldFilter(KeyConditionInfo & matches, IHqlExpression * selector, KeyedKind keyedKind, bool compareValue)
{
    if (selector->isBoolean())
    {
        if (isKeySelect(selector) && okToKey(selector, keyedKind))
        {
            OwnedHqlExpr newFilter = createValue(no_eq, makeBoolType(), LINK(selector), createConstant(compareValue));
            matches.appendCondition(*new KeyCondition(selector, newFilter, keyedKind, nullptr));
            return true;
        }
    }
    return false;
}

bool FilterExtractor::extractFilters(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    if (!expr->isAttribute() && isIndexInvariant(expr, true))
    {
        extendAndCondition(matches.preFilter, expr);
        return true;
    }

    IHqlExpression *l = expr->queryChild(0);
    IHqlExpression *r = expr->queryChild(1);
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_and:
        {
            bool extracted = extractFilters(matches, l, keyedKind);
            if (!extractFilters(matches, r, keyedKind)) extracted = false;
            return extracted;
        }
    case no_or:
        return extractOrFilter(matches, expr, keyedKind);
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return true;
    case no_not:
        {
            IHqlExpression * arg = expr->queryChild(0);
            OwnedHqlExpr inverse = getInverse(arg);
            if (inverse->queryBody() != expr->queryBody())
                return extractFilters(matches, inverse, keyedKind);

            if ((arg->getOperator() == no_select) && arg->isBoolean() &&
                extractBoolFieldFilter(matches, arg, keyedKind, false))
                return true;

            matches.appendPostFilter(expr);
            return false;
        }
    case no_between:
    case no_notbetween:
        {
            //Convert this into two comparisons because that will handle weird boundary conditions much better.
            OwnedHqlExpr normalized = expandBetween(expr);
            return extractFilters(matches, normalized, keyedKind);
        }
    case no_eq:
    case no_ne:
        {
            bool duplicate = false;
            if (matchSubstringFilter(matches, op, l, r, keyedKind, duplicate) || matchSubstringFilter(matches, op, r, l, keyedKind, duplicate))
            {
                if (duplicate)
                    matches.appendPostFilter(expr);
                return true;
            }
            return extractSimpleCompareFilter(matches, expr, keyedKind);
        }
    case no_in:
    case no_notin:
        {
            bool duplicate = false;
            if (matchSubstringFilter(matches, op, l, r, keyedKind, duplicate))
            {
                if (duplicate)
                    matches.appendPostFilter(expr);
                return true;
            }
            return extractSimpleCompareFilter(matches, expr, keyedKind);
        }
    case no_gt:
    case no_lt:
    case no_ge:
    case no_le:
        return extractSimpleCompareFilter(matches, expr, keyedKind);
    case no_assertkeyed:
        {
            KeyFailureInfo reason;
            reason.merge(failReason);
            failReason.clear();
            bool extend = expr->hasAttribute(extendAtom);
            if (!extractFilters(matches, l, extend ? KeyedExtend : KeyedYes))
            {
                if (!extend)
                    failReason.reportError(errorReceiver, expr);
            }

            IHqlExpression * original = expr->queryAttribute(_selectors_Atom);
            if (original)
            {
                ForEachChild(i, original)
                    extractFoldedWildFields(original->queryChild(i));
            }
            failReason.merge(reason);
            return true;
        }
    case no_assertwild:
        {
            if (l->getOperator() == no_all)
            {
                IHqlExpression * original = expr->queryAttribute(_selectors_Atom);
                assertex(original);
                ForEachChild(i, original)
                    extractFoldedWildFields(original->queryChild(i));
            }
            else
            {
                IHqlExpression * selector = queryKeyableSelector(l);
                if (!selector)
                {
                    StringBuffer keyname;
                    errorReceiver.throwError1(HQLERR_WildNotReferenceIndex, queryKeyName(keyname));
                }
                KeyCondition * condition = new KeyCondition;
                condition->selector.set(selector);
                condition->isWild = true;
                matches.appendCondition(*condition);
            }
            return true;
        }
    case no_if:
        return extractIfFilter(matches, expr, keyedKind);
    case no_select:
        {
            if (expr->isBoolean() && extractBoolFieldFilter(matches, expr, keyedKind, true))
                return true;

            matches.appendPostFilter(expr);
            return false;
        }
    default:
        // Add this condition to the catchall expr
        matches.appendPostFilter(expr);
        return false;
    }
}


void FilterExtractor::extractAllFilters(IHqlExpression * dataset)
{
    for (;;)
    {
        switch (dataset->getOperator())
        {
        case no_newkeyindex:
            return;
        case no_filter:
            extractAllFilters(dataset->queryChild(0));
            extractFiltersFromFilterDs(dataset);
            return;
        case no_compound_indexread:
        case no_newusertable:
        case no_hqlproject:
        case no_distributed:
        case no_preservemeta:
        case no_unordered:
        case no_sorted:
        case no_stepped:
        case no_grouped:
        case no_alias_scope:
        case no_dataset_alias:
            break;
        default:
            UNIMPLEMENTED;
        }
        dataset = dataset->queryChild(0);
    }
}


bool FilterExtractor::isKeyed()
{
    ForEachItemIn(i, keyed.conditions)
    {
        if (!keyed.conditions.item(i).isWild)
            return true;
    }
    return false;
}


bool expandFilename(StringBuffer & s, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_constant:
        expr->toString(s);
        return true;
    case no_getresult:
        //more
        break;
    case no_concat:
        {
            bool hadString = expandFilename(s, expr->queryChild(0));
            unsigned oldLength = s.length();
            s.append("+");
            if (expandFilename(s, expr->queryChild(1)) || hadString)
                return true;
            s.setLength(oldLength);
            return false;
        }
    case no_alias:
    case no_cast:
    case no_implicitcast:
        return expandFilename(s, expr->queryChild(0));
    }
    if (hasNamedSymbol(expr))
    {
        s.append(expr->queryName());
        return true;
    }
    s.append("...");
    return false;
}

void FilterExtractor::reportFailureReason(IHqlExpression * cond)
{
    failReason.reportError(errorReceiver, cond);
}

const char * FilterExtractor::queryKeyName(StringBuffer & s)
{
    IAtom * name = tableExpr->queryName();
    if (name)
        s.append(" \'").append(name).append("'");
    else
    {
        IHqlExpression * filename = queryTableFilename(tableExpr);
        if (filename)
        {
            if (!expandFilename(s.append(' '), filename))
                s.clear();
        }
    }
    return s.str();
}


IHqlExpression * FilterExtractor::querySimpleJoinValue(IHqlExpression * selector)
{
    IHqlExpression * matched = NULL;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == selector)
        {
            if (!cur.isWild)
            {
                if (matched)
                    return NULL;
                IHqlExpression * matchExpr = cur.expr;
                if (matchExpr->getOperator() != no_eq)
                    return NULL;
                if (matchExpr->queryChild(0) != selector)
                    return NULL;
                matched = matchExpr->queryChild(1);
            }
        }
    }

    return matched;
}

//-- Runtime filter generation
static __declspec(noreturn) void throwTooComplex(IHqlExpression * expr) __attribute__((noreturn));
static void throwTooComplex(IHqlExpression * expr)
{
    StringBuffer ecl;
    getExprECL(expr, ecl);
    throwError1(HQLERR_ExprTooComplexForValueSet, ecl.str());
}

static IHqlExpression * getNormalizedCompareValue(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
    {
        OwnedHqlExpr arg = getNormalizedCompareValue(expr->queryChild(0));
        return ensureExprType(arg, expr->queryType());
    }
    case no_nofold:
        return getNormalizedCompareValue(expr->queryChild(0));
    default:
        return LINK(expr);
    }
}

static bool normalizeValueCompare(OwnedHqlExpr & normalized, bool & truncated, IHqlExpression * lhs, IHqlExpression * value)
{
    //Primarily to aid creating test cases....
    OwnedHqlExpr rhs = getNormalizedCompareValue(value);
    if (!rhs->queryValue())
        return false;

    truncated = false;
    LinkedHqlExpr compareValue = rhs->queryBody();
    OwnedHqlExpr recastValue;
    if ((lhs->getOperator() != no_select) || (lhs->queryType() != compareValue->queryType()))
    {
        OwnedHqlExpr temp  = castToFieldAndBack(lhs, compareValue);
        if (temp != compareValue)
        {
            truncated = true;
        }
    }

    normalized.set(rhs);
    return true;
}


IValueSet * FilterExtractor::createValueSetInExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const
{
    if (!exprReferencesDataset(expr, tableExpr))
        throwTooComplex(expr);        //MORE: Possibly report another error

    IHqlExpression * rhs = expr->queryChild(1);
    Owned<IValueSet> values = createValueSet(type);
    switch (rhs->getOperator())
    {
    case no_null:
        return values.getClear();
    case no_all:
        values->addAll();
        return values.getClear();
    case no_list:
        break;
    default:
        throwTooComplex(expr);
    }

    ForEachChild(i, rhs)
    {
        OwnedHqlExpr normalized;
        bool truncated = false;
        if (!normalizeValueCompare(normalized, truncated, selector, rhs->queryChild(i)))
            throwTooComplex(expr);        //MORE: Possibly report another error

        if (!normalized->queryValue())
            throwTooComplex(expr);

        if (!truncated)
        {
            MemoryBuffer compareValue;
            if (!createConstantField(compareValue, selector, normalized))
                throwTooComplex(expr);

            const char * compareRaw = compareValue.toByteArray();
            values->addRawRange(compareRaw, compareRaw);
        }
    }

    if (expr->getOperator() == no_notin)
        values->invertSet();
    return values.getClear();
}


IValueSet * FilterExtractor::createValueSetCompareExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const
{
    if (!exprReferencesDataset(expr, tableExpr))
        throwTooComplex(expr);        //MORE: Possibly report another error

    OwnedHqlExpr normalized;
    bool truncated = false;
    if (!normalizeValueCompare(normalized, truncated, selector, expr->queryChild(1)))
        throwTooComplex(expr);        //MORE: Possibly report another error

    Owned<IValueSet> values = createValueSet(type);

    MemoryBuffer compareValue;
    if (!normalized->queryValue())
        throwTooComplex(expr);

    if (!createConstantField(compareValue, selector, normalized))
        throwTooComplex(expr);

    //TBD: Support substring matches
    size32_t subLength = MatchFullString;
    node_operator op = expr->getOperator();
    const char * compareRaw = compareValue.toByteArray();
    switch (op)
    {
    case no_eq:
        if (!truncated)
            values->addRawRangeEx(compareRaw, compareRaw, subLength);
        break;
    case no_ne:
        values->addAll();
        if (!truncated)
            values->killRawRangeEx(compareRaw, compareRaw, subLength);
        break;
    case no_le:
    {
        Owned<IValueTransition> upper = values->createRawTransitionEx(CMPle, compareRaw, subLength);
        values->addRange(nullptr, upper);
        break;
    }
    case no_lt:
    {
        Owned<IValueTransition> upper = values->createRawTransitionEx(truncated ? CMPle: CMPlt, compareRaw, subLength);
        values->addRange(nullptr, upper);
        break;
    }
    case no_ge:
    {
        Owned<IValueTransition> lower = values->createRawTransitionEx(truncated ? CMPgt : CMPge, compareRaw, subLength);
        values->addRange(lower, nullptr);
        break;
    }
    case no_gt:
    {
        Owned<IValueTransition> lower = values->createRawTransitionEx(CMPgt, compareRaw, subLength);
        values->addRange(lower, nullptr);
        break;
    }
    case no_between:
    case no_notbetween:
        {
            //NB: This should only be generated for substring queries.  User betweens are converted
            //to two separate comparisons to cope with range issues.
            throwUnexpectedOp(op);
        }
    default:
        throwUnexpectedOp(op);
    }
    return values.getClear();
}


IValueSet * FilterExtractor::createValueSetExpr(IHqlExpression * selector, const RtlTypeInfo & type, IHqlExpression * expr) const
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_in:
    case no_notin:
        return createValueSetInExpr(selector, type, expr);
    case no_if:
        break; // Report an error
    case no_and:
    {
        Owned<IValueSet> left = createValueSetExpr(selector, type, expr->queryChild(0));
        Owned<IValueSet> right = createValueSetExpr(selector, type, expr->queryChild(1));
        left->intersectSet(right);
        return left.getClear();
    }
    case no_or:
    {
        Owned<IValueSet> left = createValueSetExpr(selector, type, expr->queryChild(0));
        Owned<IValueSet> right = createValueSetExpr(selector, type, expr->queryChild(1));
        left->unionSet(right);
        return left.getClear();
    }
    case no_eq:
    case no_ne:
    case no_gt:
    case no_ge:
    case no_lt:
    case no_le:
        return createValueSetCompareExpr(selector, type, expr);
    }

    throwTooComplex(expr);
}

IFieldFilter * FilterExtractor::createSingleFieldFilter(IRtlFieldTypeDeserializer &deserializer) const
{
    IHqlExpression * selector = keyed.conditions.item(0).selector;
    return createFieldFilter(deserializer, selector);
}

IFieldFilter * FilterExtractor::createFieldFilter(IRtlFieldTypeDeserializer &deserializer, IHqlExpression * selector) const
{
    const RtlTypeInfo * fieldType = buildRtlType(deserializer, selector->queryType());
    Owned<IValueSet> values;
    HqlExprArray conditions;
    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector == selector)
            conditions.append(*LINK(cur.expr));
    }

    if (conditions.ordinality())
    {
        OwnedITypeInfo boolType = makeBoolType();
        OwnedHqlExpr fullExpr = createBalanced(no_and, boolType, conditions);
        values.setown(createValueSetExpr(selector, *fieldType, fullExpr));
    }

    unsigned fieldIndex = keyableSelects.find(*selector);
    if (values)
        return ::createFieldFilter(fieldIndex, values);

    return createWildFieldFilter(fieldIndex, *fieldType);
}
