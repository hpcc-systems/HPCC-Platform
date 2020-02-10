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
#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "workunit.hpp"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlfold.hpp"
#include "hqlstmt.hpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"
#include "hqlattr.hpp"
#include "hqlcatom.hpp"

#include "hqlfunc.hpp"

#include "hqlcpp.ipp"
#include "hqlcpputil.hpp"

//===========================================================================

static ITypeInfo * cachedVoidType;
static IHqlExpression * cachedZero;
static IHqlExpression * cachedNullChar;
static IHqlExpression * defaultAttrExpr;
static IHqlExpression * selfAttrExpr;

ITypeInfo * boolType;
ITypeInfo * sizetType;
ITypeInfo * signedType;
ITypeInfo * unsignedType;
ITypeInfo * defaultIntegralType;
ITypeInfo * counterType;
ITypeInfo * unknownDataType;
ITypeInfo * unknownStringType;
ITypeInfo * unknownVarStringType;
ITypeInfo * unknownUtf8Type;
ITypeInfo * constUnknownVarStringType;
ITypeInfo * unknownUnicodeType;
ITypeInfo * fposType;
ITypeInfo * doubleType;
IHqlExpression * skipActionMarker;
IHqlExpression * skipReturnMarker;
IHqlExpression * subGraphMarker;
IHqlExpression * removedAssignTag;
IHqlExpression * internalAttrExpr;
IHqlExpression * activityIdMarkerExpr;
IHqlExpression * conditionalRowMarkerExpr;

//===========================================================================

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    boolType = makeBoolType();
    signedType = makeIntType(sizeof(signed), true);
    unsignedType = makeIntType(sizeof(unsigned), false);
    sizetType = makeIntType(sizeof(size32_t), false);
    defaultIntegralType = makeIntType(8, true);
    counterType = makeIntType(8, false);
    unknownDataType = makeDataType(UNKNOWN_LENGTH);
    unknownStringType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    unknownVarStringType = makeVarStringType(UNKNOWN_LENGTH);
    unknownUtf8Type = makeUtf8Type(UNKNOWN_LENGTH, 0);
    constUnknownVarStringType = makeConstantModifier(LINK(unknownVarStringType));
    unknownUnicodeType = makeUnicodeType(UNKNOWN_LENGTH, 0);
    fposType = makeIntType(8, false);
    doubleType = makeRealType(8);

    cachedVoidType = makeVoidType();
    cachedZero = createIntConstant(0);
    cachedNullChar = createConstant(createCharValue(0, makeCharType()));
    defaultAttrExpr = createAttribute(defaultAtom);
    selfAttrExpr = createAttribute(selfAtom);

    skipActionMarker = createAttribute(skipActionMarkerAtom);
    skipReturnMarker = createAttribute(skipReturnMarkerAtom);
    subGraphMarker = createAttribute(subgraphAtom);
    removedAssignTag = createAttribute(_internal_Atom);
    internalAttrExpr = createAttribute(internalAtom);
    activityIdMarkerExpr = createAttribute(activityIdMarkerAtom);
    conditionalRowMarkerExpr = createAttribute(_conditionalRowMarker_Atom);
    return true;
}
MODULE_EXIT()
{
    conditionalRowMarkerExpr->Release();
    activityIdMarkerExpr->Release();
    internalAttrExpr->Release();
    removedAssignTag->Release();
    subGraphMarker->Release();
    skipReturnMarker->Release();
    skipActionMarker->Release();
    selfAttrExpr->Release();
    defaultAttrExpr->Release();
    boolType->Release();
    cachedVoidType->Release();
    cachedZero->Release();
    cachedNullChar->Release();
    unsignedType->Release();
    signedType->Release();
    defaultIntegralType->Release();
    sizetType->Release();
    counterType->Release();
    unknownDataType->Release();
    unknownStringType->Release();
    unknownVarStringType->Release();
    unknownUtf8Type->Release();
    constUnknownVarStringType->Release();
    unknownUnicodeType->Release();
    fposType->Release();
    doubleType->Release();
}


//===========================================================================

IHqlExpression * getZero()                              { return LINK(cachedZero); }
ITypeInfo *     queryBoolType()                     { return boolType; }
ITypeInfo *     queryVoidType()                     { return cachedVoidType; }

IHqlExpression * queryNullChar()                    { return cachedNullChar; }
IHqlExpression * queryZero()                            { return cachedZero; }
IHqlExpression * getDefaultAttr() { return LINK(defaultAttrExpr); }
IHqlExpression * getSelfAttr() { return LINK(selfAttrExpr); }
IHqlExpression * queryActivityIdMarker() { return activityIdMarkerExpr; }
IHqlExpression * queryConditionalRowMarker() { return conditionalRowMarkerExpr; }

//===========================================================================

ITypeInfo * getArrayElementType(ITypeInfo * itemType)
{
    // use a var string type to get better C++ generated...
    if (storePointerInArray(itemType))
        return makeVarStringType(UNKNOWN_LENGTH);
    return LINK(itemType);
}


ITypeInfo * getConcatResultType(IHqlExpression * expr)
{
    assertex(!"not sure if this is unicode safe, but appears not to be used");
    //first work out the maximum size of the target
    unsigned max = expr->numChildren();
    unsigned idx;
    unsigned totalSize = 0;
    bool unknown = false;
    type_t resultType = type_string;

    for (idx = 0; idx < max; idx++)
    {
        ITypeInfo * type = expr->queryChild(idx)->queryType();
        unsigned size = type->getStringLen();
        if (size == UNKNOWN_LENGTH)
            unknown = true;
        else
            totalSize += size;
        if (type->getTypeCode() == type_varstring)
            resultType = type_varstring;
    }

    if (unknown)
        totalSize = 1023;
    if (resultType == type_string)
        return makeStringType(totalSize, NULL, NULL);
    return makeVarStringType(totalSize);
}


bool isCompare3Valued(ITypeInfo * type)
{
    type = type->queryPromotedType();
    switch (type->getTypeCode())
    {
        case type_string: case type_data:
            if (type->getSize() != 1)
                return true;
            break;
        case type_qstring:
        case type_varstring:
        case type_unicode:
        case type_varunicode:
        case type_decimal:
        case type_utf8:
            return true;
    }
    return false;
}

bool storePointerInArray(ITypeInfo * type) 
{ 
    return type->isReference() && isTypePassedByAddress(type); 
}

//---------------------------------------------------------------------------

bool isSelectSortedTop(IHqlExpression * selectExpr)
{
    IHqlExpression * index = selectExpr->queryChild(1);
    if (matchesConstantValue(index, 1))
    {
        IHqlExpression * ds = selectExpr->queryChild(0);
        return ((ds->getOperator() == no_sort) || (ds->getOperator() == no_topn));
    }
    return false;
}

//---------------------------------------------------------------------------

ITypeInfo * makeRowReferenceType(IHqlExpression * ds)
{
    ITypeInfo * recordType = ds ? LINK(ds->queryRecordType()) : NULL;
    ITypeInfo * rowType = makeReferenceModifier(makeRowType(recordType));
    if (ds)
    {
        ITypeInfo * dsType = ds->queryType();
        if (hasLinkedRow(dsType))
            rowType = makeAttributeModifier(rowType, getLinkCountedAttr());
        if (hasOutOfLineModifier(dsType))
            rowType = makeOutOfLineModifier(rowType);
    }

    return rowType;
}

ITypeInfo * makeRowReferenceType(const CHqlBoundExpr & bound) 
{ 
    return makeRowReferenceType(bound.expr); 
}

IHqlExpression * addMemberSelector(IHqlExpression * expr, IHqlExpression * selector)
{
    if (!expr)
        return NULL;
    if (expr->getOperator() == no_variable)
        return createValue(no_pselect, expr->getType(), LINK(selector), LINK(expr));
    if (expr->numChildren() == 0)
        return LINK(expr);
    HqlExprArray args;
    ForEachChild(i, expr)
        args.append(*addMemberSelector(expr->queryChild(i), selector));
    return expr->clone(args);
}


//Only called on translated expressions
IHqlExpression * addExpressionModifier(IHqlExpression * expr, typemod_t modifier, IInterface * extra)
{
    //Not sure which is best implementation...
#if 1
    return createValue(no_typetransfer, makeModifier(expr->getType(), modifier, LINK(extra)), LINK(expr));
#else
    HqlExprArray args;
    unwindChildren(args, expr);
    return createValue(expr->getOperator(), makeModifier(expr->getType(), modifier, LINK(extra)), args);
#endif
}



static void expandFieldNames(IErrorReceiver & errorProcessor, StringBuffer & out, IHqlExpression * record, StringBuffer & prefix, const char * sep, IHqlExpression * formatFunc)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            expandFieldNames(errorProcessor, out, cur, prefix, sep, formatFunc);
            break;
        case no_ifblock:
            expandFieldNames(errorProcessor, out, cur->queryChild(1), prefix, sep, formatFunc);
            break;
        case no_field:
            {
                StringBuffer lowerName;
                lowerName.append(cur->queryName()).toLowerCase();
                if (formatFunc)
                {
                    HqlExprArray args;
                    args.append(*createConstant(lowerName.str()));
                    OwnedHqlExpr bound = createBoundFunction(NULL, formatFunc, args, NULL, true);
                    OwnedHqlExpr folded = foldHqlExpression(errorProcessor, bound, HFOthrowerror|HFOfoldimpure|HFOforcefold);
                    assertex(folded->queryValue());
                    lowerName.clear();
                    getStringValue(lowerName, folded);
                }

                switch (cur->queryType()->getTypeCode())
                {
                case type_record:
                case type_row:
                    {
                        unsigned len = prefix.length();
                        prefix.append(lowerName).append(".");
                        expandFieldNames(errorProcessor, out, cur->queryRecord(), prefix, sep, formatFunc);
                        prefix.setLength(len);
                        break;
                    }
                default:
                    {
                        if (out.length())
                            out.append(sep);
                        out.append(prefix).append(lowerName);
                        break;
                    }
                }
                break;
            }
        }
    }
}

void expandFieldNames(IErrorReceiver & errorProcessor, StringBuffer & out, IHqlExpression * record, const char * sep, IHqlExpression * formatFunc)
{
    StringBuffer prefix;
    expandFieldNames(errorProcessor, out, record, prefix, sep, formatFunc);
}


IHqlExpression * ensurePositiveOrZeroInt64(IHqlExpression * expr)
{
    if (!expr->queryType()->isSigned())
        return LINK(expr);

    Owned<ITypeInfo> type = makeIntType(8, true);
    if (isCast(expr) && expr->queryType() == type)
    {
        ITypeInfo * uncastType = expr->queryChild(0)->queryType();
        if (!uncastType->isSigned() && uncastType->isInteger() && uncastType->getSize() < 8)
            return LINK(expr);
    }


    OwnedHqlExpr cast = ensureExprType(expr, type);
    IValue * value = cast->queryValue();
    Owned<IValue> zeroValue = type->castFrom(true, I64C(0));
    OwnedHqlExpr zero = createConstant(LINK(zeroValue));
    if (value)
    {
        if (value->compare(zeroValue) < 0)
            return LINK(zero);
        return LINK(cast);
    }

    //A bit convoluted, but we only want to evaluate impure expressions (e.g., random()!) once.
    //So force them to appear pure (so get commoned up), wrap in an alias, and then create the conditional assignment
    if (!cast->isPure())
    {
        OwnedHqlExpr localAttr = createLocalAttribute();
        OwnedHqlExpr pure = createValue(no_pure, cast->getType(), LINK(cast));
        cast.setown(createAlias(pure, localAttr));
    }

    return createValue(no_if, LINK(type), createBoolExpr(no_lt, LINK(cast), LINK(zero)), LINK(zero), LINK(cast));
}


void getOutputLibraryName(SCMStringBuffer & libraryName, IConstWorkUnit * wu)
{
    wu->getApplicationValue("LibraryModule", "name", libraryName);
}

IHqlExpression * projectCreateSetDataset(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * select = expr->queryChild(1);
    IHqlExpression * record = ds->queryRecord();

    //Project down to a single field if necessary. Not needed if selecting the only field in the dataset.
    if (queryRealChild(record, 1) || (select->getOperator() != no_select) || (record->queryChild(0) != select->queryChild(1)) || ds->queryNormalizedSelector() != select->queryChild(0))
    {
        HqlExprArray assigns;
        OwnedHqlExpr targetField;
        if (select->getOperator() == no_select)
            targetField.set(select->queryChild(1));
        else
            targetField.setown(createFieldFromValue(valueId, select));
        IHqlExpression * newRecord = createRecord(targetField);
        assigns.append(*createAssign(createSelectExpr(getSelf(newRecord), LINK(targetField)), LINK(select)));
        IHqlExpression * newTransform = createValue(no_newtransform, makeTransformType(LINK(newRecord->queryRecordType())), assigns);

        HqlExprArray args;
        args.append(*LINK(ds));
        args.append(*newRecord);
        args.append(*newTransform);
        OwnedHqlExpr projectedDs = createDataset(no_newusertable, args);

        return createValue(no_createset, expr->getType(), LINK(projectedDs), createSelectExpr(LINK(projectedDs), LINK(targetField)));
    }
    return LINK(expr);
}

bool mustInitializeField(IHqlExpression * field)
{
    if (hasLinkCountedModifier(field))
        return true;
    return false;
}

bool worthGeneratingRowAsSingleActivity(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_left:
        case no_right:
        case no_activerow:
        case no_createrow:
        case no_getresult:
            return true;
        case no_select:
            if (!isNewSelector(expr))
                return true;
            break;
        case no_alias:
        case no_projectrow:
            break;
        case no_if:
            return worthGeneratingRowAsSingleActivity(expr->queryChild(1)) && worthGeneratingRowAsSingleActivity(expr->queryChild(2));
        default:
            //Do not generate no_getgraph result - better as separate activities
            return false;
        }
        expr = expr->queryChild(0);
    }
}

bool isNonConstantAndQueryInvariant(IHqlExpression * expr)
{
    if (expr->isConstant())
        return false;
    if (!expr->isIndependentOfScope())
        return false;
    if (isContextDependent(expr, true, false))
        return false;
    return true;
}
