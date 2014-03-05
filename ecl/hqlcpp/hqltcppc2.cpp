/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jexcept.hpp"
#include "jmisc.hpp"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlfunc.hpp"
#include "hqlcpputil.hpp"
#include "hqlfold.hpp"
#include "hqlutil.hpp"

#include "hqlstmt.hpp"
#include "hqlwcpp.hpp"
#include "hqlcpp.ipp"
#include "hqltcppc.ipp"
#include "hqlhtcpp.ipp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlpmap.hpp"
#include "hqlthql.hpp"
#include "hqlattr.hpp"
#include "hqlusage.hpp"

//#define TraceExprPrintLog(x, expr)                PrintLog(x ": %s", expr->toString(StringBuffer()).str());

//---------------------------------------------------------------------------
CChildSetColumnInfo::CChildSetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CChildSetColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, boolType, 0);
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, sizeof(bool));
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(bool)+sizeof(size32_t));

    bound.isAll.setown(convertAddressToValue(address, boolType));
    bound.length.setown(convertAddressToValue(addressSize, sizetType));
    bound.expr.setown(convertAddressToValue(addressData, queryType()));
}

void CChildSetColumnInfo::gatherSize(SizeStruct & target)
{
    addVariableSize(sizeof(bool) + sizeof(size32_t), target);
}


IHqlExpression * CChildSetColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, sizeof(bool));

    OwnedHqlExpr length = convertAddressToValue(addressSize, sizetType);
    OwnedHqlExpr boundSize = translator.getBoundSize(column->queryType(), length, NULL);
    return createValue(no_translated, LINK(sizetType), adjustValue(boundSize, sizeof(bool)+sizeof(size32_t)));
}

void CChildSetColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, boolType, 0);
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, sizeof(bool));
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(bool)+sizeof(size32_t));

    size32_t sizeExtra = sizeof(bool)+sizeof(size32_t);

    //Read the all flag and the size
    OwnedHqlExpr sizeAllSizet = getSizetConstant(sizeExtra);
    callDeserializeGetN(translator, ctx, helper, sizeAllSizet, address);

    OwnedHqlExpr targetSize = convertAddressToValue(addressSize, sizetType);
    OwnedHqlExpr unboundSize = createTranslated(targetSize);
    checkAssignOk(translator, ctx, selector, unboundSize, sizeExtra);

    callDeserializeGetN(translator, ctx, helper, targetSize, addressData);

    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    OwnedHqlExpr srcSize = adjustValue(targetSize, sizeExtra);
    ctx.associateExpr(sizeOfExpr, srcSize);
}

bool CChildSetColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    OwnedHqlExpr sizeAllFlag = getSizetConstant(1);
    callDeserializerSkipInputTranslatedSize(translator, ctx, state.helper, sizeAllFlag);
    OwnedHqlExpr sizeOfItems = callDeserializerGetSize(translator, ctx, state.helper);
    callDeserializerSkipInputTranslatedSize(translator, ctx, state.helper, sizeOfItems);
    return true;
}



void CChildSetColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, boolType, 0);
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, sizeof(bool));
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(bool)+sizeof(size32_t));

    OwnedHqlExpr isAllTarget = convertAddressToValue(address, boolType);
    OwnedHqlExpr lengthTarget = convertAddressToValue(addressSize, sizetType);

    ITypeInfo * columnType = column->queryType();
    ITypeInfo * elementType = columnType->queryChildType();
    OwnedHqlExpr value = ensureExprType(_value, columnType);

    OwnedHqlExpr inlineSize;
    switch (value->getOperator())
    {
    case no_list:
        if ((value->numChildren() != 0) && ::isFixedSize(elementType))
            inlineSize.setown(getSizetConstant(value->numChildren() * elementType->getSize())); 
        break;
    }

    if (inlineSize)
    {
        checkAssignOk(translator, ctx, selector, inlineSize, sizeof(size32_t)+sizeof(bool));

        Owned<IHqlCppSetBuilder> builder = translator.createInlineSetBuilder(elementType, isAllTarget, inlineSize, addressData);
        builder->buildDeclare(ctx);

        translator.buildSetAssign(ctx, builder, value);

        CHqlBoundTarget boundTarget;
        boundTarget.length.set(lengthTarget);
        builder->buildFinish(ctx, boundTarget);
    }
    else
    {
        CHqlBoundExpr bound;
        if ((value->getOperator() == no_list) && value->numChildren())
        {
            CHqlBoundTarget tempTarget;
            translator.createTempFor(ctx, columnType, tempTarget, typemod_none, FormatNatural);
            translator.buildExprAssign(ctx, tempTarget, value);
            bound.setFromTarget(tempTarget);
        }
        else
            translator.buildExpr(ctx, value, bound);
        ensureSimpleLength(translator, ctx, bound);

        OwnedHqlExpr isAll = bound.getIsAll();
        OwnedHqlExpr length = translator.getBoundLength(bound);
        OwnedHqlExpr size = createValue(no_translated, LINK(sizetType), translator.getBoundSize(bound));
        checkAssignOk(translator, ctx, selector, size, sizeof(size32_t)+sizeof(bool));

        translator.assignBoundToTemp(ctx, isAllTarget, isAll);
        translator.assignBoundToTemp(ctx, lengthTarget, length);
        translator.buildBlockCopy(ctx, addressData, bound);

        ensureSimpleLength(translator, ctx, bound);
        OwnedHqlExpr boundSize = translator.getBoundSize(bound);
        associateSizeOf(ctx, selector, boundSize, sizeof(size32_t)+sizeof(bool));
    }
}

//---------------------------------------------------------------------------

IHqlExpression * CMemberInfo::addDatasetLimits(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    LinkedHqlExpr value = _value;
    IHqlExpression * choosen = column->queryAttribute(choosenAtom);
    if (choosen)
    {
        LinkedHqlExpr choosenValue = choosen->queryChild(0);
        if (!choosenValue->queryValue())
        {
            OwnedHqlExpr self = container->getRelativeSelf();
            OwnedHqlExpr absoluteExpr = replaceSelector(choosenValue, querySelfReference(), self);
            choosenValue.setown(selector->queryRootRow()->bindToRow(absoluteExpr, querySelfReference()));
        }
        else
        {
            if (hasNoMoreRowsThan(value, getIntValue(choosenValue)))
                choosenValue.clear();
        }

        if (choosenValue)
            value.setown(createDataset(no_choosen, LINK(value), LINK(choosenValue)));
    }

    IHqlExpression * maxCount = queryAttributeChild(column, maxCountAtom, 0);
    if (maxCount && !hasNoMoreRowsThan(value, getIntValue(maxCount)))
    {
        //Generate a limit test if there isn't a limit that ensures it is small enough
        StringBuffer failText, columnText;
        expandSelectPathText(columnText, true).toLowerCase();
        failText.appendf("Too many rows assigned to field %s", columnText.str());
        OwnedHqlExpr fail = translator.createFailAction(failText.str(), maxCount, NULL, translator.queryCurrentActivityId(ctx));
        value.setown(createDataset(no_limit, LINK(value), createComma(LINK(maxCount), LINK(fail))));
    }
    return value.getClear();
}


bool CMemberInfo::hasDatasetLimits() const
{
    if (column->queryAttribute(choosenAtom))
        return true;

    if (queryAttributeChild(column, maxCountAtom, 0))
        return true;

    return false;
}

CChildDatasetColumnInfo::CChildDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize) : CColumnInfo(_container, _prior, _column)
{
    ColumnToOffsetMap * offsetMap = map.queryMapping(column->queryRecord(), defaultMaxRecordSize);
    maxChildSize = offsetMap->getMaxSize();
#ifdef _DEBUG
    assertex(!recordRequiresSerialization(column->queryRecord(), internalAtom));
#endif
}

void CChildDatasetColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    bound.length.setown(convertAddressToValue(addressSize, sizetType));
    bound.expr.setown(convertAddressToValue(addressData, queryType()));
}

void CChildDatasetColumnInfo::gatherSize(SizeStruct & target)
{
    addVariableSize(sizeof(size32_t), target);
}

void CColumnInfo::buildDeserializeChildLoop(HqlCppTranslator & translator, BuildCtx & loopctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr endMarker = loopctx.getTempDeclare(sizetType, NULL);
    HqlExprArray args;
    args.append(*LINK(helper));
    OwnedHqlExpr beginCall = translator.bindTranslatedFunctionCall(deserializerBeginNestedId, args);
    loopctx.addAssign(endMarker, beginCall);

    args.append(*LINK(helper));
    args.append(*LINK(endMarker));
    OwnedHqlExpr loopCall = createBoolExpr(no_not, translator.bindTranslatedFunctionCall(deserializerFinishedNestedId, args));
    loopctx.addLoop(loopCall, NULL, false);
}

void CColumnInfo::buildDeserializeToBuilder(HqlCppTranslator & translator, BuildCtx & ctx, IHqlCppDatasetBuilder * builder, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    BuildCtx loopctx(ctx);
    buildDeserializeChildLoop(translator, loopctx, selector, helper, serializeForm);

    BoundRow * selfRow = builder->buildDeserializeRow(loopctx, helper, serializeForm);
    builder->finishRow(loopctx, selfRow);
}


void CChildDatasetColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    IHqlExpression * record = column->queryRecord();
    assertex(!recordRequiresLinkCount(record)); // Why would it?
    if (column->isDictionary())
    {
        if (serializeForm == diskAtom)
        {
            //If we ever generate the meta definition for an internal serialization format then the following needs to be implemented
            UNIMPLEMENTED_X("deserialize serialized dictionary from disk");
            return;
        }
    }


    if (isConditional())
        checkAssignOk(translator, ctx, selector, queryZero(), sizeof(size32_t));

    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    //Read the all flag and the size
    OwnedHqlExpr sizeSizet = getSizetConstant(sizeof(size32_t));
    callDeserializeGetN(translator, ctx, helper, sizeSizet, addressSize);

    OwnedHqlExpr targetSize = convertAddressToValue(addressSize, sizetType);
    OwnedHqlExpr simpleSize = translator.ensureSimpleTranslatedExpr(ctx, targetSize);
    OwnedHqlExpr unboundSize = createTranslated(simpleSize);
    checkAssignOk(translator, ctx, selector, unboundSize, sizeof(size32_t));

    callDeserializeGetN(translator, ctx, helper, simpleSize, addressData);

    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    OwnedHqlExpr srcSize = adjustValue(simpleSize, sizeof(size32_t));
    ctx.associateExpr(sizeOfExpr, srcSize);
}


void CChildDatasetColumnInfo::buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    if (column->isDictionary())
    {
        if (serializeForm == diskAtom)
        {
            //If we ever generate the meta definition for an internal serialization format then the following needs to be implemented
            UNIMPLEMENTED_X("deserialize serialized dictionary from disk");
        }
    }

    CColumnInfo::buildSerialize(translator, ctx, selector, helper, serializeForm);
}



bool CChildDatasetColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    OwnedHqlExpr sizeOfDataset = callDeserializerGetSize(translator, ctx, state.helper);
    callDeserializerSkipInputTranslatedSize(translator, ctx, state.helper, sizeOfDataset);
    return true;
}


IHqlExpression * CChildDatasetColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr length = convertAddressToValue(addressSize, sizetType);
    OwnedHqlExpr boundSize = translator.getBoundSize(column->queryType(), length, NULL);
    return createValue(no_translated, LINK(sizetType), adjustValue(boundSize, sizeof(size32_t)));
}

void CChildDatasetColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    OwnedHqlExpr lengthTarget = convertAddressToValue(addressSize, sizetType);

    ITypeInfo * columnType = column->queryType();
    IHqlExpression * record = column->queryRecord();
    OwnedHqlExpr value = addDatasetLimits(translator, ctx, selector, _value);
    ITypeInfo * valueType = value->queryType();
    assertRecordTypesMatch(valueType, columnType);

    CHqlBoundExpr bound;
    translator.buildDataset(ctx, value, bound, FormatBlockedDataset);
    translator.normalizeBoundExpr(ctx, bound);
    ensureSimpleLength(translator, ctx, bound);

    OwnedHqlExpr length = translator.getBoundLength(bound);
    OwnedHqlExpr size = createValue(no_translated, LINK(sizetType), translator.getBoundSize(bound));
    checkAssignOk(translator, ctx, selector, size, sizeof(size32_t));

    translator.assignBoundToTemp(ctx, lengthTarget, length);
    translator.buildBlockCopy(ctx, addressData, bound);

    //Use the size just calculated for the field
    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    OwnedHqlExpr boundSize = translator.getBoundSize(bound);
    OwnedHqlExpr srcSize = adjustValue(boundSize, sizeof(size32_t));
    ctx.associateExpr(sizeOfExpr, srcSize);
}

AColumnInfo * CChildDatasetColumnInfo::lookupColumn(IHqlExpression * search)
{
    throwError1(HQLERR_LookupNotActiveDataset, search->queryName()->str());
    return NULL;
}


//---------------------------------------------------------------------------

CChildLimitedDatasetColumnInfo::CChildLimitedDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize) : CColumnInfo(_container, _prior, _column)
{
    IHqlExpression * count = column->queryAttribute(countAtom);
    if (count)
        countField.setown(foldHqlExpression(column->queryAttribute(countAtom)->queryChild(0)));
    else
    {
        IHqlExpression * size = column->queryAttribute(sizeofAtom);
        if (size)
            sizeField.setown(foldHqlExpression(size->queryChild(0)));
        else
            countField.setown(createConstantOne());
    }
    if (countField)
        countField.setown(ensureExprType(countField, sizetType));
    if (sizeField)
        sizeField.setown(ensureExprType(sizeField, sizetType));
    ColumnToOffsetMap * offsetMap = map.queryMapping(column->queryRecord(), defaultMaxRecordSize);
    maxChildSize = offsetMap->getMaxSize();
    fixedChildSize = offsetMap->isFixedWidth() ? maxChildSize : UNKNOWN_LENGTH;
}

void CChildLimitedDatasetColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), 0);

    if (countField)
    {
        OwnedHqlExpr mappedCount = replaceSelector(countField, querySelfReference(), selector->queryExpr()->queryChild(0));
        CHqlBoundExpr boundCount;
        translator.buildExpr(ctx, mappedCount, boundCount);
        bound.count.set(boundCount.expr);
    }
    else
    {
        OwnedHqlExpr mappedSize = replaceSelector(sizeField, querySelfReference(), selector->queryExpr()->queryChild(0));
        CHqlBoundExpr boundSize;
        translator.buildExpr(ctx, mappedSize, boundSize);
        bound.length.set(boundSize.expr);
    }

    bound.expr.setown(convertAddressToValue(addressData, queryType()));
}

void CChildLimitedDatasetColumnInfo::gatherSize(SizeStruct & target)
{
    if (isFixedSize())
    {
        unsigned fixedSize;
        if (sizeField && sizeField->queryValue())
            fixedSize = (unsigned)getIntValue(sizeField);
        else
        {
            fixedSize = (unsigned)getIntValue(countField) * fixedChildSize;
        }

        if (isConditional())
            addVariableSize(fixedSize, target);
        else
            target.addFixed(fixedSize);
    }
    else
    {
        addVariableSize(0, target);
    }
}


IHqlExpression * CChildLimitedDatasetColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    CHqlBoundExpr bound;
    buildColumnExpr(translator, ctx, selector, bound);
    OwnedHqlExpr length = translator.getBoundLength(bound);
    return createTranslated(length);
}

bool CChildLimitedDatasetColumnInfo::isFixedSize()
{
    if (sizeField && sizeField->queryValue())
        return true;
    if (countField && countField->queryValue() && (fixedChildSize != UNKNOWN_LENGTH))
        return true;
    return false;   //MORE:
}

void CChildLimitedDatasetColumnInfo::buildDeserializeChildLoop(HqlCppTranslator & translator, BuildCtx & loopctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr mappedCount = replaceSelector(countField, querySelfReference(), selector->queryExpr()->queryChild(0));
    CHqlBoundExpr bound;
    translator.buildTempExpr(loopctx, mappedCount, bound);

    OwnedHqlExpr test = createValue(no_postdec, LINK(bound.expr));
    loopctx.addLoop(test, NULL, false);
}

bool CChildLimitedDatasetColumnInfo::prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state)
{
    OwnedHqlExpr self = container->getRelativeSelf();
    if (sizeField)
    {
        OwnedHqlExpr mappedSize = replaceSelector(sizeField, querySelfReference(), self);
        gatherSelectExprs(state.requiredValues, mappedSize);
    }
    else if (countField)
    {
        OwnedHqlExpr mappedCount = replaceSelector(countField, querySelfReference(), self);
        gatherSelectExprs(state.requiredValues, mappedCount);
    }
    return true;
}


bool CChildLimitedDatasetColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    try
    {
        OwnedHqlExpr self = container->getRelativeSelf();
        if (sizeField)
        {
            OwnedHqlExpr mappedSize = replaceSelector(sizeField, querySelfReference(), self);
            OwnedHqlExpr replacedSize = quickFullReplaceExpressions(mappedSize, state.requiredValues, state.mappedValues);
            if (containsSelector(replacedSize, queryRootSelf()))
                return false;
            callDeserializerSkipInputSize(translator, ctx, state. helper, replacedSize);
            return true;
        }
        else
        {
            OwnedHqlExpr mappedCount = replaceSelector(countField, querySelfReference(), self);
            OwnedHqlExpr replacedCount = quickFullReplaceExpressions(mappedCount, state.requiredValues, state.mappedValues);
            if (containsSelector(replacedCount, queryRootSelf()))
                return false;
            if (fixedChildSize != UNKNOWN_LENGTH)
            {
                OwnedHqlExpr scaledSize = multiplyValue(replacedCount, fixedChildSize);
                callDeserializerSkipInputSize(translator, ctx, state. helper, scaledSize);
                return true;
            }

            BuildCtx loopctx(ctx);
            CHqlBoundExpr bound;
            translator.buildTempExpr(loopctx, replacedCount, bound);

            OwnedHqlExpr test = createValue(no_postdec, LINK(bound.expr));
            loopctx.addLoop(test, NULL, false);

            StringBuffer prefetcherInstanceName;
            translator.ensureRowPrefetcher(prefetcherInstanceName, ctx, column->queryRecord());
            
            StringBuffer s;
            s.append(prefetcherInstanceName).append("->readAhead(");
            translator.generateExprCpp(s, state.helper).append(");");
            loopctx.addQuoted(s);
            return true;
        }
    }
    catch (IException * e)
    {
        //yuk yuk yuk!!  Couldn't resolve the dataset count/size for some strange reason
        e->Release();
    }
    return false;
}


void CChildLimitedDatasetColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    assertex(!column->isDictionary());
    if (sizeField || !countField)
    {
        ctx.addQuoted("rtlFailUnexpected();");
        return;
    }

    //NB: The serialized form of a dataset with an external count is not the same as a normal dataset
    IHqlExpression * record = column->queryRecord();
    if (recordRequiresSerialization(record, serializeForm) || !translator.isFixedRecordSize(record))
    {
        Owned<IHqlCppDatasetBuilder> builder = translator.createBlockedDatasetBuilder(column->queryRecord());
        builder->buildDeclare(ctx);

        buildDeserializeToBuilder(translator, ctx, builder, selector, helper, serializeForm);

        CHqlBoundExpr bound;
        builder->buildFinish(ctx, bound);

        setColumnFromBuilder(translator, ctx, selector, builder);
    }
    else
        CColumnInfo::buildDeserialize(translator, ctx, selector, helper, serializeForm);
}



void CChildLimitedDatasetColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    if (sizeField)
        translator.throwError(HQLERR_CannotCreateSizedChildDataset);

    if (countField)
    {
        IHqlExpression * record = column->queryRecord();

        OwnedHqlExpr mappedCount = replaceSelector(countField, querySelfReference(), selector->queryExpr()->queryChild(0));
        Owned<IHqlCppDatasetBuilder> builder = translator.createLimitedDatasetBuilder(record, mappedCount);
        builder->buildDeclare(ctx);
        translator.buildDatasetAssign(ctx, builder, value);

        setColumnFromBuilder(translator, ctx, selector, builder);
    }
}

void CChildLimitedDatasetColumnInfo::setColumnFromBuilder(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlCppDatasetBuilder * builder)
{
    CHqlBoundExpr bound;
    builder->buildFinish(ctx, bound);
    if (bound.length)
        bound.length.setown(translator.ensureSimpleTranslatedExpr(ctx, bound.length));

    OwnedHqlExpr size = createValue(no_translated, LINK(sizetType), translator.getBoundSize(bound));
    checkAssignOk(translator, ctx, selector, size, 0);

    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), 0);
    translator.buildBlockCopy(ctx, addressData, bound);

    //Use the size just calculated for the field
    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    OwnedHqlExpr srcSize = translator.getBoundSize(bound);
    ctx.associateExpr(sizeOfExpr, srcSize);
}

AColumnInfo * CChildLimitedDatasetColumnInfo::lookupColumn(IHqlExpression * search)
{
    throwError1(HQLERR_LookupNotActiveDataset, search->queryName()->str());
    return NULL;
}

//--------------------------------------------------------------------------------------------

CChildLinkedDatasetColumnInfo::CChildLinkedDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize) : CColumnInfo(_container, _prior, _column)
{
    ColumnToOffsetMap * offsetMap = map.queryMapping(column->queryRecord(), defaultMaxRecordSize);
    maxChildSize = offsetMap->getMaxSize();
}

void CChildLinkedDatasetColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    bound.count.setown(convertAddressToValue(addressSize, sizetType));
    bound.expr.setown(convertAddressToValue(addressData, queryType()));
}

void CChildLinkedDatasetColumnInfo::gatherSize(SizeStruct & target)
{
    unsigned thisSize = sizeof(size32_t) + sizeof(byte * *);
    if (isConditional())
        addVariableSize(thisSize, target);      // the size is used for ensure if condition is true
    else
        target.addFixed(thisSize);
}


IHqlExpression * CChildLinkedDatasetColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    return getSizetConstant(sizeof(size32_t) + sizeof(byte * *));
}

void CChildLinkedDatasetColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeFormat)
{
    if (isConditional())
        checkAssignOk(translator, ctx, selector, queryZero(), sizeof(size32_t) + sizeof(byte * *));

    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    IHqlExpression * record = column->queryRecord();
    CHqlBoundTarget boundTarget;
    boundTarget.count.setown(convertAddressToValue(addressSize, sizetType));
    boundTarget.expr.setown(convertAddressToValue(addressData, queryType()));

    IIdAtom * func = NULL;
    HqlExprArray args;
    args.append(*translator.createSerializer(ctx, record, serializeFormat, deserializerAtom));
    if (column->isDictionary())
    {
        if (serializeFormat == diskAtom)
        {
            func = deserializeChildDictionaryFromDatasetFromStreamId;
            StringBuffer lookupHelperName;
            translator.buildDictionaryHashClass(record, lookupHelperName);
            args.append(*createQuoted(lookupHelperName.str(), makeBoolType()));
        }
        else
            func = deserializeChildDictionaryFromStreamId;
    }
    else
        func = deserializeChildRowsetFromStreamId;

    args.append(*LINK(helper));
    OwnedHqlExpr call = translator.bindFunctionCall(func, args, queryType());
    translator.buildExprAssign(ctx, boundTarget, call);
}


bool CChildLinkedDatasetColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    OwnedHqlExpr sizeOfDataset = callDeserializerGetSize(translator, ctx, state.helper);
    callDeserializerSkipInputTranslatedSize(translator, ctx, state.helper, sizeOfDataset);
    return true;
}



void CChildLinkedDatasetColumnInfo::buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeFormat)
{
    IHqlExpression * record = column->queryRecord();

    IIdAtom * func = NULL;
    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*translator.createSerializer(ctx, record, serializeFormat, serializerAtom));
    args.append(*LINK(selector->queryExpr()));
    if (column->isDictionary())
    {
        if (serializeFormat == diskAtom)
            func = serializeChildDictionaryToDatasetToStreamId;
        else
            func = serializeChildDictionaryToStreamId;
    }
    else
        func = serializeChildRowsetToStreamId;
    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
    translator.buildStmt(ctx, call);
}



bool CChildLinkedDatasetColumnInfo::modifyColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value, node_operator op)
{
    if (hasDatasetLimits() || (op != no_assign_addfiles))
        return false;

    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    ITypeInfo * resultType = queryType();
    ITypeInfo * valueType = value->queryType();
    assertex(recordTypesMatch(valueType, resultType));

    CHqlBoundTarget boundTarget;
    boundTarget.count.setown(convertAddressToValue(addressSize, sizetType));
    boundTarget.expr.setown(convertAddressToValue(addressData, queryType()));

    HqlExprArray args;
    args.append(*LINK(value));
    OwnedHqlExpr call = translator.bindFunctionCall(appendRowsToRowsetId, args, resultType);
    translator.buildDatasetAssign(ctx, boundTarget, call);
    return true;
}


void CChildLinkedDatasetColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    OwnedHqlExpr addressSize = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressData = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    ITypeInfo * resultType = queryType();
    LinkedHqlExpr value = _value;
    ITypeInfo * valueType = value->queryType();

    assertRecordTypesMatch(resultType, valueType);

    value.setown(addDatasetLimits(translator, ctx, selector, value));

    CHqlBoundTarget boundTarget;
    boundTarget.count.setown(convertAddressToValue(addressSize, sizetType));
    boundTarget.expr.setown(convertAddressToValue(addressData, queryType()));

    if (value->getOperator() == no_null)
        value.setown(createNullExpr(column));
    
    translator.buildDatasetAssign(ctx, boundTarget, value);
}

AColumnInfo * CChildLinkedDatasetColumnInfo::lookupColumn(IHqlExpression * search)
{
    throwError1(HQLERR_LookupNotActiveDataset, search->queryName()->str());
    return NULL;
}

