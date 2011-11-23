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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

#include "hql.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"
#include "hqlpmap.hpp"

#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlresource.hpp"
#include "hqlregex.ipp"
#include "hqlsource.ipp"
#include "hqlcse.ipp"

#include "eclhelper.hpp"

//--------------------------------------------------------------------------------------------------

IHqlExpression * getHozedBias(ITypeInfo * type)
{
    unsigned __int64 bias = ((unsigned __int64)1 << (type->getSize()*8-1));
    return createConstant(type->castFrom(false, bias));
}

bool requiresHozedTransform(ITypeInfo * type)
{
    type = type->queryPromotedType();
    switch (type->getTypeCode())
    {
    case type_boolean:
    case type_data:
    case type_qstring:
        return false;
    case type_littleendianint:
        return type->isSigned() || type->getSize() != 1;
    case type_bigendianint:
        return type->isSigned();
    case type_string:
    case type_varstring:
        return (type->queryCharset()->queryName() != asciiAtom);
    case type_decimal:
        return type->isSigned();
    default:
        //anything else is a payload field, don't do any transformations...
        return false;
    }
}

bool requiresHozedTransform(IHqlExpression * value, ITypeInfo * keyFieldType)
{
    ITypeInfo * type = value->queryType();
    if (type != keyFieldType)
        return true;

    return requiresHozedTransform(type);
}

bool isKeyableType(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_boolean:
    case type_swapint:
    case type_int:
    case type_decimal:
        return true;
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_data:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return (type->getSize() != UNKNOWN_LENGTH);
    default:
        return false;
    }
}


IHqlExpression * getHozedKeyValue(IHqlExpression * _value)
{
    HqlExprAttr value = _value;
    Linked<ITypeInfo> type = _value->queryType()->queryPromotedType();

    type_t tc = type->getTypeCode();
    switch (tc)
    {
    case type_boolean:
    case type_data:
    case type_qstring:
        break;
    case type_int:
    case type_swapint:
        if (type->isSigned())
        {
            type.setown(makeIntType(type->getSize(), false));
            value.setown(ensureExprType(value, type));
            value.setown(createValue(no_add, LINK(type), LINK(value), getHozedBias(type)));
        }
        if ((type->getTypeCode() == type_littleendianint) && (type->getSize() != 1))
            type.setown(makeSwapIntType(type->getSize(), false));
        break;
    case type_string:
        if (type->queryCharset()->queryName() != asciiAtom)
            type.setown(makeStringType(type->getSize(), NULL, NULL));
        break;
    case type_varstring:
        if (type->queryCharset()->queryName() != asciiAtom)
            type.setown(makeVarStringType(type->getStringLen(), NULL, NULL));
        break;
    case type_decimal:
        if (!type->isSigned())
            break;
        //fallthrough
    default:
        //anything else is a payload field, don't do any transformations...
        break;
    }

    return ensureExprType(value, type);
}


IHqlExpression * convertIndexPhysical2LogicalValue(IHqlExpression * cur, IHqlExpression * physicalSelect, bool allowTranslate)
{
    if (cur->hasProperty(blobAtom))
    {
        if (cur->isDataset())
            return createDataset(no_id2blob, LINK(physicalSelect), LINK(cur->queryRecord()));
        else if (cur->isDatarow())
            return createRow(no_id2blob, LINK(physicalSelect), LINK(cur->queryRecord()));
        else
            return createValue(no_id2blob, cur->getType(), LINK(physicalSelect));
    }
    else if (allowTranslate)
    {
        LinkedHqlExpr newValue = physicalSelect;

        OwnedHqlExpr target = createSelectExpr(getActiveTableSelector(), LINK(cur));            // select not used, just created to get correct types.
        ITypeInfo * type = target->queryType();
        type_t tc = type->getTypeCode();
        if (tc == type_int || tc == type_swapint)
        {
            if (type->isSigned())
            {
                Owned<ITypeInfo> tempType = makeIntType(type->getSize(), false);
                newValue.setown(ensureExprType(newValue, tempType));
                newValue.setown(createValue(no_sub, newValue->getType(), LINK(newValue), getHozedBias(newValue->queryType())));
            }
        }

        return ensureExprType(newValue, type);
    }
    else
        return LINK(physicalSelect);
}


//--------------------------------------------------------------------------------------------------

void HqlCppTranslator::buildJoinMatchFunction(BuildCtx & ctx, const char * name, IHqlExpression * left, IHqlExpression * right, IHqlExpression * match, IHqlExpression * selSeq)
{
    if (match)
    {
        StringBuffer s;
        BuildCtx matchctx(ctx);
        matchctx.addQuotedCompound(s.append("virtual bool ").append(name).append("(const void * _left, const void * _right)"));

        matchctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
        matchctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");

        bindTableCursor(matchctx, left, "left", no_left, selSeq);
        bindTableCursor(matchctx, right, "right", no_right, selSeq);

        OwnedHqlExpr cseMatch = options.spotCSE ? spotScalarCSE(match) : LINK(match);
        buildReturn(matchctx, cseMatch);
    }
}

//--------------------------------------------------------------------------------------------------

IHqlExpression * createKeyFromComplexKey(IHqlExpression * expr)
{
    IHqlExpression * base = queryPhysicalRootTable(expr);
    if (base->getOperator() == no_newkeyindex)
        return LINK(base);
    UNIMPLEMENTED_XY("Key", getOpString(base->getOperator()));
    return NULL;
}

class KeyedJoinInfo : public CInterface
{
public:
    KeyedJoinInfo(HqlCppTranslator & _translator, IHqlExpression * _expr, bool _canOptimizeTransfer);
    ~KeyedJoinInfo();

    void buildClearRightFunction(BuildCtx & classctx);
    void buildExtractFetchFields(BuildCtx & ctx);
    void buildExtractIndexReadFields(BuildCtx & ctx);
    void buildExtractJoinFields(ActivityInstance & instance);
    void buildFailureTransform(BuildCtx & ctx, IHqlExpression * transform);
    void buildFetchMatch(BuildCtx & ctx);
    void buildIndexReadMatch(BuildCtx & ctx);
    void buildLeftOnly(BuildCtx & ctx);
    void buildMonitors(BuildCtx & ctx);
    void buildTransform(BuildCtx & ctx);

    IHqlExpression * getMatchExpr(bool isKeyFilter)         { return isKeyFilter ? LINK(monitors->queryExtraFilter()) : LINK(fileFilter); }
    bool isFetchFiltered()                                  { return fileFilter != NULL; }
    bool isFullJoin()                                       { return file != NULL; }
    bool isHalfJoin()                                       { return !file; }
    bool processFilter();
    IHqlExpression * queryKey()                             { return key; }
    IHqlExpression * queryOriginalKey()                     { return originalKey; }
    IHqlExpression * queryKeyFilename()                     { return hasComplexIndex ? NULL : key->queryChild(3); }
    IHqlExpression * queryFile()                            { return file; }
    IHqlExpression * queryFileFilename()                    { return file->queryChild(0); }
    IHqlExpression * queryRawKey()                          { return rawKey; }
    IHqlExpression * queryRawRhs()                          { return rawRhs; }
    bool isKeyOpt()                                         { return key->hasProperty(optAtom); }
    bool isFileOpt()                                        { return file && file->hasProperty(optAtom); }
    bool needToExtractJoinFields() const                    { return extractJoinFieldsTransform != NULL; }
    bool hasPostFilter() const                              { return monitors->queryExtraFilter() || fileFilter; }
    bool requireActivityForKey() const                      { return hasComplexIndex; }

    void reportFailureReason(IHqlExpression * cond)         { monitors->reportFailureReason(cond); }

protected:
    void buildClearRecord(BuildCtx & ctx, RecordSelectIterator & rawIter, RecordSelectIterator & keyIter);
    void buildTransformBody(BuildCtx & ctx, IHqlExpression * transform);
    IHqlExpression * expandDatasetReferences(IHqlExpression * expr, IHqlExpression * ds);
    IHqlExpression * optimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * expr, IHqlExpression * leftSelector);
    void optimizeExtractJoinFields();
    void optimizeTransfer(SharedHqlExpr & targetDataset, SharedHqlExpr & targetTransform, SharedHqlExpr & keyedFilter, OwnedHqlExpr * extraFilter);
    void splitFilter(IHqlExpression * filter, SharedHqlExpr & keyTarget);

protected:
    HqlCppTranslator & translator;
    HqlExprAttr     expr;
    HqlExprAttr     originalKey;            // even if computed/parameter
    HqlExprAttr     key;
    HqlExprAttr     rawKey;
    HqlExprAttr     expandedKey;
    HqlExprAttr     file;
    HqlExprAttr     expandedFile;
    HqlExprAttr     keyAccessDataset;
    HqlExprAttr     keyAccessTransform;
    HqlExprAttr     fileAccessDataset;
    HqlExprAttr     fileAccessTransform;
    HqlExprAttr     joinSeq;
    MonitorExtractor * monitors;
    HqlExprAttr     fileFilter;
    HqlExprAttr     leftOnlyMatch;
    HqlExprAttr     rawRhs;
    TableProjectMapper keyedMapper;
    OwnedHqlExpr    counter;
    OwnedHqlExpr    extractJoinFieldsRecord;
    OwnedHqlExpr    extractJoinFieldsTransform;
    bool            canOptimizeTransfer;
    bool            hasComplexIndex;
};

KeyedJoinInfo::KeyedJoinInfo(HqlCppTranslator & _translator, IHqlExpression * _expr, bool _canOptimizeTransfer) : translator(_translator)
{ 
    expr.set(_expr);
    joinSeq.set(querySelSeq(expr));
    hasComplexIndex = false;

    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * keyed = expr->queryProperty(keyedAtom);
    if (keyed && keyed->queryChild(0))
    {
        key.set(keyed->queryChild(0));
        if (right->getOperator() == no_keyed)
            right = right->queryChild(0);
        file.set(right);
        IHqlExpression * rightTable = queryPhysicalRootTable(right);
        if (!rightTable || rightTable->queryNormalizedSelector() != right->queryNormalizedSelector())
            translator.throwError(HQLERR_FullKeyedNeedsFile);
        expandedFile.setown(convertToPhysicalTable(rightTable, true));
        keyedMapper.setDataset(key);
    }
    else if (right->getOperator() == no_newkeyindex)
    {
        key.set(right);
    }
    else
    {
        hasComplexIndex = true;
        originalKey.set(right);
        key.setown(createKeyFromComplexKey(right));
    }

    if (!originalKey)
        originalKey.set(key);
    expandedKey.setown(translator.convertToPhysicalIndex(key));
    rawKey.set(queryPhysicalRootTable(expandedKey));
    canOptimizeTransfer = _canOptimizeTransfer; 
    monitors = NULL;
    counter.set(queryPropertyChild(expr, _countProject_Atom, 0));

    if (isFullJoin())
        rawRhs.set(queryPhysicalRootTable(expandedFile));
    else
        rawRhs.set(rawKey);
}

KeyedJoinInfo::~KeyedJoinInfo()
{
    delete monitors;
}


void KeyedJoinInfo::buildClearRecord(BuildCtx & ctx, RecordSelectIterator & rawIter, RecordSelectIterator & keyIter)
{
    keyIter.first();
    ForEach(rawIter)
    {
        assert(keyIter.isValid());
        OwnedHqlExpr rawSelect = rawIter.get();
        OwnedHqlExpr keySelect = keyIter.get();

        ITypeInfo * keyFieldType = keySelect->queryType();
        OwnedHqlExpr null = createNullExpr(keyFieldType);
        OwnedHqlExpr keyNull = (rawIter.isInsideIfBlock() || (rawIter.isInsideNested() && isInPayload())) ? LINK(null) : getHozedKeyValue(null);
        OwnedHqlExpr folded = foldHqlExpression(keyNull);
        translator.buildAssign(ctx, rawSelect, folded);
        keyIter.next();
    }
}


void KeyedJoinInfo::buildClearRightFunction(BuildCtx & classctx)
{
    if (extractJoinFieldsTransform || isFullJoin())
    {
        OwnedHqlExpr ds = createDataset(no_anon, LINK(extractJoinFieldsRecord));
        translator.buildClearRecordMember(classctx, "Right", ds);
    }
    else
    {
        //Need to initialize the record with the zero logical values, not zero key values
        //which differs for biased integers etc.
        BuildCtx funcctx(classctx);
        funcctx.addQuotedCompound("virtual size32_t createDefaultRight(ARowBuilder & crSelf)");
        translator.ensureRowAllocated(funcctx, "crSelf");
        
        BoundRow * selfCursor = translator.bindSelf(funcctx, rawKey, "crSelf");
        IHqlExpression * rawSelf = selfCursor->querySelector();
        RecordSelectIterator rawIter(rawKey->queryRecord(), rawSelf);

        RecordSelectIterator keyIter(key->queryRecord(), key);
        buildClearRecord(funcctx, rawIter, keyIter);
        translator.buildReturnRecordSize(funcctx, selfCursor);
    }
}


void KeyedJoinInfo::buildExtractFetchFields(BuildCtx & ctx)
{
    // For the data going to the fetch remote activity:
    //virtual size32_t extractFetchFields(ARowBuilder & crSelf, const void * _left) = 0;
    if (fileAccessDataset)
    {
        BuildCtx ctx1(ctx);
        ctx1.addQuotedCompound("virtual size32_t extractFetchFields(ARowBuilder & crSelf, const void * _left)");
        translator.ensureRowAllocated(ctx1, "crSelf");
        if (fileAccessTransform)
        {
            translator.buildTransformBody(ctx1, fileAccessTransform, expr->queryChild(0), NULL, fileAccessDataset, joinSeq);
        }
        else
        {
            translator.buildRecordSerializeExtract(ctx1, fileAccessDataset->queryRecord());
        }
    }

    //virtual IOutputMetaData * queryFetchInputRecordSize() = 0;
    OwnedHqlExpr null = createValue(no_null, makeVoidType());
    translator.buildMetaMember(ctx, fileAccessDataset.get() ? fileAccessDataset.get() : null.get(), "queryFetchInputRecordSize");
}


void KeyedJoinInfo::buildExtractIndexReadFields(BuildCtx & ctx)
{
    //virtual size32_t extractIndexReadFields(ARowBuilder & crSelf, const void * _left) = 0;
    BuildCtx ctx1(ctx);
    ctx1.addQuotedCompound("virtual size32_t extractIndexReadFields(ARowBuilder & crSelf, const void * _left)");
    translator.ensureRowAllocated(ctx1, "crSelf");
    if (keyAccessTransform)
    {
        translator.buildTransformBody(ctx1, keyAccessTransform, expr->queryChild(0), NULL, keyAccessDataset, joinSeq);
    }
    else
    {
        translator.buildRecordSerializeExtract(ctx1, keyAccessDataset->queryRecord());
    }

    //virtual IOutputMetaData * queryIndexReadInputRecordSize() = 0;
    translator.buildMetaMember(ctx, keyAccessDataset, "queryIndexReadInputRecordSize");
}


void KeyedJoinInfo::buildExtractJoinFields(ActivityInstance & instance)
{
    //virtual size32_t extractJoinFields(void *dest, const void *diskRow, IBlobProvider * blobs) = 0;
    BuildCtx extractctx(instance.startctx);
    extractctx.addQuotedCompound("virtual size32_t extractJoinFields(ARowBuilder & crSelf, const void *_left, unsigned __int64 _filepos, IBlobProvider * blobs)");
    translator.ensureRowAllocated(extractctx, "crSelf");
    if (needToExtractJoinFields())
    {
        OwnedHqlExpr extracted = createDataset(no_anon, LINK(extractJoinFieldsRecord));
        OwnedHqlExpr raw = createDataset(no_anon, LINK(rawRhs->queryRecord()));


        BoundRow * selfCursor = translator.buildTransformCursors(extractctx, extractJoinFieldsTransform, raw, NULL, extracted, joinSeq);
        if (isHalfJoin())
        {
            OwnedHqlExpr left = createSelector(no_left, raw, joinSeq);
            translator.associateBlobHelper(extractctx, left, "blobs");

            OwnedHqlExpr fileposExpr = getFilepos(left, false);
            OwnedHqlExpr fileposVar = createVariable("_filepos", fileposExpr->getType());
            extractctx.associateExpr(fileposExpr, fileposVar);
        }

        translator.doBuildTransformBody(extractctx, extractJoinFieldsTransform, selfCursor);
    }
    else
    {
        translator.buildRecordSerializeExtract(extractctx, extractJoinFieldsRecord);
    }

    //virtual IOutputMetaData * queryJoinFieldsRecordSize() = 0;
    translator.buildMetaMember(instance.classctx, extractJoinFieldsRecord, "queryJoinFieldsRecordSize");
}

void KeyedJoinInfo::buildFetchMatch(BuildCtx & ctx)
{
    translator.buildJoinMatchFunction(ctx, "fetchMatch", fileAccessDataset, expr->queryChild(1), fileFilter, joinSeq);
}


void KeyedJoinInfo::buildIndexReadMatch(BuildCtx & ctx)
{
    LinkedHqlExpr matchExpr = monitors->queryExtraFilter();

    if (matchExpr)
    {
        BuildCtx matchctx(ctx);
        matchctx.addQuotedCompound("virtual bool indexReadMatch(const void * _left, const void * _right, unsigned __int64 _filepos, IBlobProvider * blobs)");

        matchctx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
        matchctx.addQuoted("const unsigned char * right = (const unsigned char *) _right;");

        OwnedHqlExpr fileposExpr = getFilepos(rawKey, false);
        OwnedHqlExpr fileposVar = createVariable("_filepos", fileposExpr->getType());

        if (translator.queryOptions().spotCSE)
            matchExpr.setown(spotScalarCSE(matchExpr));

        translator.associateBlobHelper(matchctx, rawKey, "blobs");

        translator.bindTableCursor(matchctx, keyAccessDataset, "left", no_left, joinSeq);
        translator.bindTableCursor(matchctx, rawKey, "right");
        matchctx.associateExpr(fileposExpr, fileposVar);

        translator.buildReturn(matchctx, matchExpr);
    }
}


void KeyedJoinInfo::buildLeftOnly(BuildCtx & ctx)
{
    if (leftOnlyMatch)
    {
        BuildCtx funcctx(ctx);
        funcctx.addQuotedCompound("virtual bool leftCanMatch(const void * _left)");
        funcctx.addQuoted("const unsigned char * left = (const unsigned char *)_left;");
        translator.bindTableCursor(funcctx, expr->queryChild(0), "left", no_left, joinSeq);
        translator.buildReturn(funcctx, leftOnlyMatch);
    }
}


void KeyedJoinInfo::buildMonitors(BuildCtx & ctx)
{
    monitors->optimizeSegments(keyAccessDataset->queryRecord());

    //---- virtual void createSegmentMonitors(struct IIndexReadContext *) { ... } ----
    BuildCtx createSegmentCtx(ctx);
    createSegmentCtx.addQuotedCompound("virtual void createSegmentMonitors(IIndexReadContext *irc, const void * _left)");
    createSegmentCtx.addQuoted("const unsigned char * left = (const unsigned char *) _left;");
    translator.bindTableCursor(createSegmentCtx, keyAccessDataset, "left", no_left, joinSeq);
    monitors->buildSegments(createSegmentCtx, "irc", false);
}


void KeyedJoinInfo::buildTransform(BuildCtx & ctx)
{
    BuildCtx funcctx(ctx);
    switch (expr->getOperator())
    {
    case no_join:
        {
            funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos)");
            break;
        }
    case no_denormalize:
        {
            funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, unsigned counter)");

            translator.associateCounter(funcctx, counter, "counter");
            break;
        }
    case no_denormalizegroup:
        {
            funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned numRows, const void * * _rows)");
            funcctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");
            break;
        }
    }   

    translator.ensureRowAllocated(funcctx, "crSelf");
    buildTransformBody(funcctx, expr->queryChild(3));
}

//expand references to oldDataset using ds.  
//First expand references like a.b using the table definition
//Then need to expand references to the complete table as a usertable projection.
//e.g., left.x := l.x + right;
static IHqlExpression * expandDatasetReferences(IHqlExpression * expr, IHqlExpression * ds, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    TableProjectMapper mapper(ds);
    OwnedHqlExpr expanded = mapper.expandFields(expr, oldDataset, newDataset);

    OwnedHqlExpr mapParent;
    IHqlExpression * dsParent = ds->queryChild(0);
    OwnedHqlExpr seq = createSelectorSequence();
    switch (getChildDatasetType(ds))
    {
    case childdataset_dataset:
        mapParent.set(dsParent->queryNormalizedSelector());
        break;
    case childdataset_left: 
        UNIMPLEMENTED;
        mapParent.setown(createSelector(no_left, dsParent, querySelSeq(ds)));
        break;
    default:
        UNIMPLEMENTED;
        break;
    }

    OwnedHqlExpr newLeft = createSelector(no_left, dsParent, seq);
    OwnedHqlExpr newKeyTransform = replaceSelector(queryNewColumnProvider(ds), mapParent, newLeft);
    HqlExprArray args;
    unwindChildren(args, newKeyTransform);
    newKeyTransform.setown(createValue(no_transform, makeTransformType(LINK(ds->queryRecordType())), args));
    OwnedHqlExpr rightProject = createRow(no_projectrow, LINK(newDataset), createComma(LINK(newKeyTransform), LINK(seq)));
    OwnedHqlExpr wrappedProject = createRow(no_newrow, LINK(rightProject));
    return replaceSelector(expanded, oldDataset, wrappedProject);
}

IHqlExpression * KeyedJoinInfo::expandDatasetReferences(IHqlExpression * transform, IHqlExpression * ds)
{
    OwnedHqlExpr oldRight = createSelector(no_right, ds, joinSeq);
    OwnedHqlExpr newRight = createSelector(no_right, ds->queryChild(0), joinSeq);

    return ::expandDatasetReferences(transform, ds, oldRight, newRight);
}

void KeyedJoinInfo::buildTransformBody(BuildCtx & ctx, IHqlExpression * transform)
{
    IHqlExpression * rhs = expr->queryChild(1);
    IHqlExpression * rhsRecord = rhs->queryRecord();
    OwnedHqlExpr serializedRhsRecord = getSerializedForm(rhsRecord);

    //Map the file position field in the file to the incoming parameter
    //MORE: This doesn't cope with local/global file position distinctions.
    OwnedHqlExpr fileposVar = createVariable("_filepos", makeIntType(8, false));

    OwnedHqlExpr joinDataset = createDataset(no_anon, LINK(extractJoinFieldsRecord));
    OwnedHqlExpr newTransform = replaceMemorySelectorWithSerializedSelector(transform, rhsRecord, no_right, joinSeq);
    OwnedHqlExpr oldRight = createSelector(no_right, serializedRhsRecord, joinSeq);
    OwnedHqlExpr newRight = createSelector(no_right, joinDataset, joinSeq);
    OwnedHqlExpr fileposExpr = getFilepos(newRight, false);

    if (extractJoinFieldsTransform)
    {
        IHqlExpression * fileposField = isFullJoin() ? queryVirtualFileposField(file->queryRecord()) : queryLastField(key->queryRecord());
        if (fileposField && (expr->getOperator() != no_denormalizegroup))
        {
            HqlMapTransformer fileposMapper;
            OwnedHqlExpr select = createSelectExpr(LINK(oldRight), LINK(fileposField));
            OwnedHqlExpr castFilepos = ensureExprType(fileposExpr, fileposField->queryType());
            fileposMapper.setMapping(select, castFilepos);
            newTransform.setown(fileposMapper.transformRoot(newTransform));
        }

        newTransform.setown(replaceSelector(newTransform, oldRight, newRight));
    }
    else
    {
        if (isFullJoin())
        {
            if (expandedFile != queryPhysicalRootTable(file))
                newTransform.setown(expandDatasetReferences(newTransform, expandedFile));
        }
        else
            newTransform.setown(expandDatasetReferences(newTransform, expandedKey));
    }

    newTransform.setown(optimizeHqlExpression(newTransform, HOOfold|HOOcompoundproject));
    newTransform.setown(foldHqlExpression(newTransform));

    BoundRow * selfCursor = translator.buildTransformCursors(ctx, newTransform, expr->queryChild(0), joinDataset, expr, joinSeq);

    if (expr->getOperator() == no_denormalizegroup)
    {
        //Last parameter is false since implementation of group keyed denormalize in roxie passes in pointers to the serialized slave data
        bool rowsAreLinkCounted = false;
        translator.bindRows(ctx, no_right, joinSeq, expr->queryProperty(_rowsid_Atom), joinDataset, "numRows", "rows", rowsAreLinkCounted);
    }

    ctx.associateExpr(fileposExpr, fileposVar);
    translator.doBuildTransformBody(ctx, newTransform, selfCursor);
}

void KeyedJoinInfo::buildFailureTransform(BuildCtx & ctx, IHqlExpression * onFailTransform)
{
    BuildCtx funcctx(ctx);
    funcctx.addQuotedCompound("virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, IException * except)");
    translator.associateLocalFailure(funcctx, "except");
    translator.ensureRowAllocated(funcctx, "crSelf");

    buildTransformBody(funcctx, onFailTransform);
}

IHqlExpression * KeyedJoinInfo::optimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * filter, IHqlExpression * leftSelector)
{
    switch (filter->getOperator())
    {
    case no_join:
        return NULL;
    case no_left:
        if (filter->queryBody() == leftSelector)
            return NULL;        // Something nasty e.g., evaluate()...
        break;
    case no_right:
        return LINK(filter);
    case no_select:
        {
            //Check for an expression of the form LEFT.x.y.z.a.b.c, but if any of x,y,z, are datasets then process later.
            IHqlExpression * cur = filter;
            IHqlExpression * ds;
            loop
            {
                ds = cur->queryChild(0);
                //if a select from a dataset, then wait until we recurse to here
                if ((ds->getOperator() != no_select) || ds->isDataset())
                    break;
                cur = ds;
            }
            //check it was the correct left..
            if (ds->queryBody() == leftSelector)
            {
                unsigned match = values.find(*filter);
                if (match == NotFound)
                {
                    match = fields.ordinality();
                    LinkedHqlExpr field = filter->queryChild(1);
                    if (fields.find(*field) != NotFound)
                    {
                        //Check same field isn't used in two different nested records.
                        StringBuffer name;
                        name.append("__unnamed__").append(fields.ordinality());
                        field.setown(createField(createIdentifierAtom(name), field->getType(), NULL, NULL));
                    }

                    fields.append(*LINK(field));
                    values.append(*LINK(filter));
                }

                IHqlExpression * matchField = &fields.item(match);
                OwnedHqlExpr serializedField = getSerializedForm(matchField);
                OwnedHqlExpr result = createSelectExpr(getActiveTableSelector(), LINK(serializedField));
                return ensureDeserialized(result, matchField->queryType());
            }
            break;
        }
    case no_attr_expr:
        if (filter->queryName() == _selectors_Atom)
            return LINK(filter);
        break;
    }

    HqlExprArray children;
    ForEachChild(i, filter)
    {
        IHqlExpression * next = optimizeTransfer(fields, values, filter->queryChild(i), leftSelector);
        if (!next) return NULL;
        children.append(*next);
    }
    return cloneOrLink(filter, children);
}


IHqlExpression * reverseOptimizeTransfer(IHqlExpression * left, IHqlExpression * transform, IHqlExpression * filter)
{
    if (!transform)
        return LINK(filter);

    HqlMapTransformer mapper;
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        IHqlExpression * tgt = cur->queryChild(0);
        IHqlExpression * src = cur->queryChild(1);
        OwnedHqlExpr selector = createSelectExpr(LINK(left), LINK(tgt->queryChild(1)));
        mapper.setMapping(selector, src);
    }
    return mapper.transformRoot(filter);
}



void KeyedJoinInfo::optimizeTransfer(SharedHqlExpr & targetDataset, SharedHqlExpr & targetTransform, SharedHqlExpr & filter, OwnedHqlExpr * extraFilter)
{
    IHqlExpression * dataset = expr->queryChild(0);
    if (canOptimizeTransfer)
    {
        if (filter)
        {
            IHqlExpression * record = dataset->queryRecord();
            HqlExprArray fields;
            HqlExprArray values;
            bool hasExtra = (extraFilter && extraFilter->get());
            OwnedHqlExpr oldLeft = createSelector(no_left, dataset, joinSeq);
            OwnedHqlExpr newFilter = optimizeTransfer(fields, values, filter, oldLeft);
            OwnedHqlExpr newExtraFilter = hasExtra ? optimizeTransfer(fields, values, *extraFilter, oldLeft) : NULL;
            if (newFilter && (newExtraFilter || !hasExtra) && fields.ordinality() < getFieldCount(dataset->queryRecord()))
            {
                OwnedHqlExpr extractedRecord = translator.createRecordInheritMaxLength(fields, dataset);
                OwnedHqlExpr serializedRecord = getSerializedForm(extractedRecord);
                targetDataset.setown(createDataset(no_anon, LINK(serializedRecord), NULL));

                HqlExprArray assigns;
                OwnedHqlExpr self = getSelf(serializedRecord);
                ForEachItemIn(i, fields)
                {
                    IHqlExpression * curField = &fields.item(i);
                    OwnedHqlExpr serializedField = getSerializedForm(curField);
                    OwnedHqlExpr value = ensureSerialized(&values.item(i));
                    assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(serializedField)), LINK(value)));
                }
                targetTransform.setown(createValue(no_newtransform, makeTransformType(serializedRecord->getType()), assigns));
                
                OwnedHqlExpr leftSelect = createSelector(no_left, serializedRecord, joinSeq);
                filter.setown(replaceSelector(newFilter, queryActiveTableSelector(), leftSelect));
                if (hasExtra)
                    extraFilter->setown(replaceSelector(newExtraFilter, queryActiveTableSelector(), leftSelect));

            }
            else if (recordRequiresSerialization(record))
            {
                OwnedHqlExpr serializedRecord = getSerializedForm(record);
                targetDataset.setown(createDataset(no_anon, LINK(serializedRecord)));
                targetTransform.setown(createRecordMappingTransform(no_transform, serializedRecord, oldLeft));

                filter.setown(replaceMemorySelectorWithSerializedSelector(filter, record, no_left, joinSeq));
                if (hasExtra)
                    extraFilter->setown(replaceMemorySelectorWithSerializedSelector(*extraFilter, record, no_left, joinSeq));
            }
            else
                targetDataset.set(dataset);
        }
        else
        {
            //any fields will be serialized automatically, and no filter
            targetDataset.set(dataset);
        }
    }
    else
    {
        targetDataset.set(dataset);
    }
}


static void expandAllFields(HqlExprArray & fieldsAccessed, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_field:
        if (fieldsAccessed.find(*expr) == NotFound)
            fieldsAccessed.append(*LINK(expr));
        break;
    case no_ifblock:
        expandAllFields(fieldsAccessed, expr->queryChild(1));
        break;
    case no_record:
        ForEachChild(i, expr)
            expandAllFields(fieldsAccessed, expr->queryChild(i));
        break;
    }
}


static void doGatherFieldsAccessed(RecursionChecker & checker, HqlExprArray & fieldsAccessed, IHqlExpression * expr, IHqlExpression * ds)
{
    if (checker.alreadyVisited(expr))
        return;
    checker.setVisited(expr);
    switch (expr->getOperator())
    {
    case no_select:
        if (expr->queryChild(0) == ds)
        {
            IHqlExpression * field = expr->queryChild(1);
            if (fieldsAccessed.find(*field) == NotFound)
                fieldsAccessed.append(*LINK(field));
            return;
        }
        break;
    case no_record:
    case no_attr:
        return;
    case no_left:
    case no_right:
        //Never walk children
        if (expr == ds)
            expandAllFields(fieldsAccessed, expr->queryRecord());
        return;
    default:
        if (expr == ds)
        {
            expandAllFields(fieldsAccessed, expr->queryRecord());
            return;
        }
        break;
    }
    ForEachChild(i, expr)
        doGatherFieldsAccessed(checker, fieldsAccessed, expr->queryChild(i), ds);
}


static void gatherFieldsAccessed(HqlExprArray & fieldsAccessed, IHqlExpression * cond, IHqlExpression * ds)
{
    RecursionChecker checker;

    doGatherFieldsAccessed(checker, fieldsAccessed, cond, ds);
}

void KeyedJoinInfo::optimizeExtractJoinFields()
{
    HqlExprArray fieldsAccessed;
    bool doExtract = false;
    IHqlExpression * fileposField = NULL;
    OwnedHqlExpr right = createSelector(no_right, expr->queryChild(1), joinSeq);
    IHqlExpression * rightRecord = right->queryRecord();
    OwnedHqlExpr extractedRecord;

    if (expr->getOperator() == no_denormalizegroup)
    {
        //Version1: Don't remove any fields
        doExtract = true;
        OwnedHqlExpr rows = createDataset(no_rows, LINK(right), LINK(expr->queryProperty(_rowsid_Atom)));
        if (isFullJoin())
        {
            //unwindFields(fieldsAccessed, file->queryRecord());
            extractedRecord.set(file->queryRecord());
        }
        else
        {
//          unwindFields(fieldsAccessed, key->queryRecord());
            extractedRecord.set(key->queryRecord());
                fileposField = queryLastField(key->queryRecord());
        }
    }
    else
    {
        gatherFieldsAccessed(fieldsAccessed, expr->queryChild(3), right);

        if (isFullJoin())
        {
            IHqlExpression * filepos = queryVirtualFileposField(file->queryRecord());
            if (filepos)
                fieldsAccessed.zap(*filepos);
            if (translator.getTargetClusterType() != HThorCluster)
                doExtract = (fieldsAccessed.ordinality() < getFlatFieldCount(rawRhs->queryRecord()));
        }
        else
        {
            IHqlExpression * keyRecord = key->queryRecord();
            IHqlExpression * filepos = queryLastField(keyRecord);
            if (filepos)
                fieldsAccessed.zap(*filepos);

            if (translator.getTargetClusterType() != HThorCluster)
                doExtract = (fieldsAccessed.ordinality() < getFlatFieldCount(keyRecord)-1);

            if (!doExtract && recordContainsBlobs(keyRecord))
                doExtract = true;
        }

        if (recordRequiresSerialization(rightRecord))
            doExtract = true;
    }

    if (doExtract)
    {
        HqlExprArray assigns;
        OwnedHqlExpr left = createSelector(no_left, rawRhs, joinSeq);

        if (extractedRecord || (fieldsAccessed.ordinality() != 0))
        {
            if (!extractedRecord)
                extractedRecord.setown(translator.createRecordInheritMaxLength(fieldsAccessed, rawRhs));
            extractJoinFieldsRecord.setown(getSerializedForm(extractedRecord));
            OwnedHqlExpr self = getSelf(extractJoinFieldsRecord);
            OwnedHqlExpr memorySelf = getSelf(extractedRecord);

            if (isFullJoin() && (rawRhs->queryBody() == expandedFile->queryBody()))
            {
                assertex(extractedRecord == extractJoinFieldsRecord);
                ForEachChild(i, extractedRecord)
                {
                    IHqlExpression * curMemoryField = extractedRecord->queryChild(i);
                    IHqlExpression * curSerializedField = extractJoinFieldsRecord->queryChild(i);
                    if (curMemoryField == fileposField)
                        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(curSerializedField)), getFilepos(left, false)));
                    else if (!curMemoryField->isAttribute())
                        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(curSerializedField)), createSelectExpr(LINK(left), LINK(curMemoryField))));      // no
                }
            }
            else
            {
                TableProjectMapper fieldMapper;
                if (isFullJoin())
                    fieldMapper.setDataset(expandedFile);
                else
                    fieldMapper.setDataset(expandedKey);

                ForEachChild(i, extractJoinFieldsRecord)
                {
                    IHqlExpression * curMemoryField = extractedRecord->queryChild(i);
                    IHqlExpression * curSerializedField = extractJoinFieldsRecord->queryChild(i);
                    if (curMemoryField == fileposField)
                        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(curSerializedField)), getFilepos(left, false)));
                    else if (!curMemoryField->isAttribute())
                    {
                        OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(curSerializedField));
                        OwnedHqlExpr src = createSelectExpr(LINK(memorySelf), LINK(curMemoryField));
                        OwnedHqlExpr mappedSrc = fieldMapper.expandFields(src, memorySelf, left, rawRhs);
                        assigns.append(*createAssign(tgt.getClear(), ensureSerialized(mappedSrc)));
                    }
                }
            }
        }
        else
        {
            //A bit of a hack - Richard can't cope with zero length values being returned, so allocate
            //a single byte to keep him happy.
            OwnedHqlExpr dummyField = createField(unnamedAtom, makeIntType(1, false), NULL, NULL);
            extractJoinFieldsRecord.setown(createRecord(dummyField));
            OwnedHqlExpr self = getSelf(extractJoinFieldsRecord);

            assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(dummyField)), getZero()));
        }

        extractJoinFieldsTransform.setown(createValue(no_transform, makeTransformType(extractJoinFieldsRecord->getType()), assigns));
    }
    else
    {
        if (isFullJoin())
            extractJoinFieldsRecord.set(rawRhs->queryRecord());
        else
            extractJoinFieldsRecord.set(rawKey->queryRecord());
    }
}



bool KeyedJoinInfo::processFilter()
{
    OwnedHqlExpr atmostCond, atmostLimit;
    IHqlExpression * atmost = expr->queryProperty(atmostAtom);
    extractAtmostArgs(atmost, atmostCond, atmostLimit);

    IHqlExpression * cond = expr->queryChild(2);
    OwnedHqlExpr fuzzy, hard;
    translator.splitFuzzyCondition(cond, atmostCond, fuzzy, hard);

    OwnedHqlExpr keyedKeyFilter, fuzzyKeyFilter;
    splitFilter(hard, keyedKeyFilter);
    if (!keyedKeyFilter)
    {
        if (!cond->queryValue() || cond->queryValue()->getBoolValue())
        {
            StringBuffer s;
            getExprECL(cond, s);
            if (isFullJoin())
                translator.throwError1(HQLERR_KeyAccessNoKeyField, s.str());
            else
                translator.throwError1(HQLERR_KeyedJoinTooComplex, s.str());
        }
        else
            leftOnlyMatch.set(cond);
    }
    if (atmost && fileFilter)
    {
        StringBuffer s;
        translator.throwError1(HQLERR_BadKeyedJoinConditionAtMost,getExprECL(fileFilter, s.append(" (")).append(")").str());
    }
    splitFilter(fuzzy, fuzzyKeyFilter);

    //Now work out what fields need to be serialized to perform the match
    optimizeTransfer(keyAccessDataset, keyAccessTransform, keyedKeyFilter, &fuzzyKeyFilter);
    if (file && fileFilter)
        optimizeTransfer(fileAccessDataset, fileAccessTransform, fileFilter, NULL);

    //Now need to transform the index into its real representation so
    //the hozed transforms take place.
    unsigned payload = numPayloadFields(key);
    assertex(payload);          // don't use rawindex once payload can be 0
    TableProjectMapper mapper(expandedKey);
    OwnedHqlExpr rightSelect = createSelector(no_right, key, joinSeq);
    OwnedHqlExpr newFilter = mapper.expandFields(keyedKeyFilter, rightSelect, rawKey, rawKey);

    //Now extract the filters from it.
    OwnedHqlExpr extra;
    monitors = new MonitorExtractor(rawKey, translator, -(int)numPayloadFields(rawKey), false);
    if (newFilter)
        monitors->extractFilters(newFilter, extra);

    if (atmost && extra && (atmostCond || !monitors->isCleanlyKeyedExplicitly()))
    {
        StringBuffer s;
        //map the key references back so the error message refers to RIGHT instead of a weird key expression.
        bool collapsedAll = false;
        OwnedHqlExpr collapsed = mapper.collapseFields(extra, rawKey, rightSelect, rawKey, &collapsedAll);
        translator.throwError1(HQLERR_BadKeyedJoinConditionAtMost,getExprECL(collapsed, s.append(" (")).append(")").str());
    }

//  OwnedHqlExpr oldLeft = createSelector(no_left, expr->queryChild(0), joinSeq);
    OwnedHqlExpr newLeft = createSelector(no_left, keyAccessDataset, joinSeq);

    //Finally extend the non-keyed filter with the non-keyed portion.
    OwnedHqlExpr newFuzzyKeyFilter = mapper.expandFields(fuzzyKeyFilter, rightSelect, rawKey, rawKey);
//  newFuzzyKeyFilter.setown(replaceSelector(newFuzzyKeyFilter, oldLeft, newLeft));
    monitors->appendFilter(newFuzzyKeyFilter);

    //add any key-invariant condition to the leftOnly match
    IHqlExpression * keyedLeftOnly = monitors->queryGlobalGuard();
    if (keyedLeftOnly)
        extendConditionOwn(leftOnlyMatch, no_and, reverseOptimizeTransfer(newLeft, keyAccessTransform, keyedLeftOnly));

    //optimize the fields returned from the rhs to perform the transform
    if (expr->getOperator() != no_keyeddistribute)
        optimizeExtractJoinFields();

    return monitors->isKeyed();
}

void KeyedJoinInfo::splitFilter(IHqlExpression * filter, SharedHqlExpr & keyTarget)
{
    if (!filter) return;
    if (filter->getOperator() == no_and)
    {
        splitFilter(filter->queryChild(0), keyTarget);
        splitFilter(filter->queryChild(1), keyTarget);
    }
    else if (containsOnlyLeft(filter))
        extendAndCondition(leftOnlyMatch, filter);
    else if (filter->queryValue())
    {
        //remove silly "and true" conditions
        if (!filter->queryValue()->getBoolValue())
            extendAndCondition(keyTarget, filter);
    }
    else
    {
        if (file)
        {
            bool doneAll = false;
            OwnedHqlExpr fileRight = createSelector(no_right, file, joinSeq);
            OwnedHqlExpr keyRight = createSelector(no_right, key, joinSeq);
            OwnedHqlExpr mapped = keyedMapper.collapseFields(filter, fileRight, keyRight, &doneAll);
            if (doneAll)
                extendAndCondition(keyTarget, mapped);
            else
                extendAndCondition(fileFilter, filter);
        }
        else
            extendAndCondition(keyTarget, filter);
    }
}




void HqlCppTranslator::buildKeyedJoinExtra(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * info)
{
    //virtual IOutputMetaData * queryDiskRecordSize() = 0;  // Excluding fpos and sequence
    if (info->isFullJoin())
        buildMetaMember(instance.classctx, info->queryRawRhs(), "queryDiskRecordSize");

    //virtual unsigned __int64 extractPosition(const void * _right) = 0;  // Gets file position value from rhs row
    if (info->isFullJoin())
    {
        IHqlExpression * index = info->queryKey();
        IHqlExpression * indexRecord = index->queryRecord();
        BuildCtx ctx4(instance.startctx);
        ctx4.addQuotedCompound("virtual unsigned __int64 extractPosition(const void * _right)");
        ctx4.addQuoted("const unsigned char * right = (const unsigned char *) _right;");
        bindTableCursor(ctx4, index, "right");
        OwnedHqlExpr fileposExpr = createSelectExpr(LINK(index), LINK(indexRecord->queryChild(indexRecord->numChildren()-1)));
        buildReturn(ctx4, fileposExpr);
    }

    //virtual const char * getFileName() = 0;                   // Returns filename of raw file fpos'es refer into
    if (info->isFullJoin())
        buildFilenameFunction(instance, instance.createctx, "getFileName", info->queryFileFilename(), hasDynamicFilename(info->queryFile()));

    //virtual bool diskAccessRequired() = 0;
    if (info->isFullJoin())
        doBuildBoolFunction(instance.startctx, "diskAccessRequired", true);

    //virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right) = 0;
    info->buildTransform(instance.startctx);
    IHqlExpression * onFail = expr->queryProperty(onFailAtom);
    if (onFail)
    {
        //virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, IException * except)
        info->buildFailureTransform(instance.startctx, onFail->queryChild(0));
    }

    //limit helpers...
    IHqlExpression * limit = expr->queryProperty(limitAtom);
    if (limit)
    {
        if (limit->hasProperty(skipAtom))
        {
            BuildCtx ctx1(instance.startctx);
            ctx1.addQuotedCompound("virtual unsigned __int64 getSkipLimit()");
            buildReturn(ctx1, limit->queryChild(0));
        }
        else
            buildLimitHelpers(instance.startctx, limit->queryChild(0), limit->queryChild(1), false, info->queryKeyFilename(), instance.activityId);
    }
}


void HqlCppTranslator::buildKeyJoinIndexReadHelper(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * info)
{
    //virtual size32_t extractIndexReadFields(ARowBuilder & crSelf, const void * _input) = 0;
    //virtual IOutputMetaData * queryIndexReadInputRecordSize() = 0;
    info->buildExtractIndexReadFields(instance.startctx);

    //virtual const char * getIndexFileName() = 0;
    buildFilenameFunction(instance, instance.startctx, "getIndexFileName", info->queryKeyFilename(), hasDynamicFilename(info->queryKey()));

    //virtual IOutputMetaData * queryIndexRecordSize() = 0; //Excluding fpos and sequence
    buildMetaMember(instance.classctx, info->queryRawKey(), "queryIndexRecordSize");

    //virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *lhs) = 0;
    info->buildMonitors(instance.startctx);

    //virtual bool indexReadMatch(const void * indexRow, const void * inputRow) = 0;
    info->buildIndexReadMatch(instance.startctx);
}

void HqlCppTranslator::buildKeyJoinFetchHelper(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * info)
{
    //virtual size32_t extractFetchFields(ARowBuilder & crSelf, const void * _input) = 0;
    //virtual IOutputMetaData * queryFetchInputRecordSize() = 0;
    info->buildExtractFetchFields(instance.startctx);

    // Inside the fetch remote activity
    //virtual bool fetchMatch(const void * diskRow, const void * inputRow) = 0;
    info->buildFetchMatch(instance.startctx);

    //virtual size32_t extractJoinFields(void *dest, const void *diskRow, IBlobProvider * blobs) = 0;
    info->buildExtractJoinFields(instance);
}

ABoundActivity * HqlCppTranslator::doBuildActivityKeyedJoinOrDenormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    KeyedJoinInfo info(*this, expr, !targetHThor());
    IHqlExpression * cond = expr->queryChild(2);
    if (!info.processFilter() && !cond->isConstant())
        info.reportFailureReason(cond);
    if (info.isFullJoin())
    {
        IHqlExpression * table = info.queryFile();
        if (table->getOperator() != no_table)
            throwError(HQLERR_FullJoinNeedDataset);
    }

    Owned<ABoundActivity> boundDataset1 = buildCachedActivity(ctx, expr->queryChild(0));
    Owned<ABoundActivity> boundIndexActivity;
    if (options.forceActivityForKeyedJoin || info.requireActivityForKey())
        boundIndexActivity.setown(buildCachedActivity(ctx, info.queryOriginalKey()));

    node_operator op = expr->getOperator();
    ThorActivityKind kind;
    switch (op)
    {
    case no_join:
        kind = TAKkeyedjoin;
        break;
    case no_denormalize:
        kind = TAKkeyeddenormalize;
        break;
    case no_denormalizegroup:
        kind = TAKkeyeddenormalizegroup;
        break;
    }
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, (op == no_join) ? "KeyedJoin" : "KeyedDenormalize");

    IHqlExpression * indexName = info.queryKeyFilename();
    if (indexName)
    {
        OwnedHqlExpr folded = foldHqlExpression(indexName);
        if (folded->queryValue())
        {
            StringBuffer graphLabel;
            if (instance->isGrouped)
                graphLabel.append("Grouped ");
            else if (instance->isLocal)
                graphLabel.append("Local ");
            graphLabel.append(getActivityText(instance->kind));
            getStringValue(graphLabel.append("\n'"), folded).append("'");
            instance->graphLabel.set(graphLabel.str());
        }
    }
    buildActivityFramework(instance);
    IHqlExpression * rowlimit = expr->queryProperty(rowLimitAtom);

    StringBuffer s;
    buildInstancePrefix(instance);

    OwnedHqlExpr atmostCond, atmostLimit;
    IHqlExpression * atmost = expr->queryProperty(atmostAtom);
    extractAtmostArgs(atmost, atmostCond, atmostLimit);

    //virtual unsigned getJoinFlags()
    StringBuffer flags;
    bool isLeftOuter = (expr->hasProperty(leftonlyAtom) || expr->hasProperty(leftouterAtom));
    if (expr->hasProperty(leftonlyAtom)) flags.append("|JFexclude");
    if (isLeftOuter) flags.append("|JFleftouter");
    if (expr->hasProperty(firstAtom)) flags.append("|JFfirst");
    if (expr->hasProperty(firstLeftAtom)) flags.append("|JFfirstleft");
    if (transformContainsSkip(expr->queryChild(3)))
        flags.append("|JFtransformMaySkip");
    if (info.isFetchFiltered())
        flags.append("|JFfetchMayFilter");
    if (rowlimit && rowlimit->hasProperty(skipAtom))
        flags.append("|JFmatchAbortLimitSkips");
    if (rowlimit && rowlimit->hasProperty(countAtom))
        flags.append("|JFcountmatchabortlimit");
    if (expr->hasProperty(onFailAtom))
        flags.append("|JFonfail");
    if (info.isKeyOpt())
        flags.append("|JFindexoptional");
    if (info.needToExtractJoinFields())
        flags.append("|JFextractjoinfields");
    if (expr->hasProperty(unorderedAtom))
        flags.append("|JFreorderable");
    if (transformReturnsSide(expr, no_left, 0))
        flags.append("|JFtransformmatchesleft");
    if (info.queryKeyFilename() && !info.queryKeyFilename()->isConstant())
        flags.append("|JFvarindexfilename");
    if (hasDynamicFilename(info.queryKey()))      
        flags.append("|JFdynamicindexfilename");
    if (boundIndexActivity)
        flags.append("|JFindexfromactivity");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getJoinFlags", flags.str()+1);

    //Fetch flags
    flags.clear();
    if (info.isFullJoin())
    {
        if (info.isFileOpt())
            flags.append("|FFdatafileoptional");
        if (!info.queryFileFilename()->isConstant())
            flags.append("|FFvarfilename");
        if (hasDynamicFilename(info.queryFile()))     
            flags.append("|FFdynamicfilename");
    }

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFetchFlags", flags.str()+1);

    //virtual unsigned getJoinLimit()
    if (!isZero(atmostLimit))
        doBuildUnsignedFunction(instance->startctx, "getJoinLimit", atmostLimit);

    //virtual unsigned getKeepLimit()
    LinkedHqlExpr keepLimit = queryPropertyChild(expr, keepAtom, 0);
    if (keepLimit)
        doBuildUnsignedFunction(instance->startctx, "getKeepLimit", keepLimit);

    bool implicitLimit = !rowlimit && !atmost &&
                        (!keepLimit || info.hasPostFilter()) &&
                        !expr->hasProperty(leftonlyAtom);

    //virtual unsigned getKeepLimit()
    doBuildJoinRowLimitHelper(*instance, rowlimit, info.queryKeyFilename(), implicitLimit);

    buildFormatCrcFunction(instance->classctx, "getIndexFormatCrc", info.queryRawKey(), info.queryRawKey(), 1);
    if (info.isFullJoin())
    {
        buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", info.queryRawRhs(), NULL, 0);
        buildEncryptHelper(instance->startctx, info.queryFile()->queryProperty(encryptAtom), "getFileEncryptKey");
    }

    IHqlExpression * key = info.queryKey();
    buildSerializedLayoutMember(instance->classctx, key->queryRecord(), "getIndexLayout", numKeyedFields(key));

    //--function to clear right, used for left outer join
    if (isLeftOuter || expr->hasProperty(onFailAtom))
        info.buildClearRightFunction(instance->createctx);

    buildKeyedJoinExtra(*instance, expr, &info);

    buildKeyJoinIndexReadHelper(*instance, expr, &info);
    buildKeyJoinFetchHelper(*instance, expr, &info);

    info.buildLeftOnly(instance->startctx);

    if (targetRoxie())
    {
        instance->addAttributeBool("_diskAccessRequired", info.isFullJoin());
        instance->addAttributeBool("_isIndexOpt", info.isKeyOpt());
        instance->addAttributeBool("_isOpt", info.isFileOpt());
    }

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset1, 0, 0);
    if (boundIndexActivity)
        buildConnectInputOutput(ctx, instance, boundIndexActivity, 0, 1);
    addFileDependency(info.queryKeyFilename(), instance->queryBoundActivity());
    if (info.isFullJoin())
        addFileDependency(info.queryFileFilename(), instance->queryBoundActivity());

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityKeyedDistribute(BuildCtx & ctx, IHqlExpression * expr)
{
    if (!targetThor())
        return buildCachedActivity(ctx, expr->queryChild(0));

    HqlExprArray leftSorts, rightSorts;
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * indexRecord = right->queryRecord();
    IHqlExpression * seq = querySelSeq(expr);
    bool isLimitedSubstringJoin;
    OwnedHqlExpr match = findJoinSortOrders(expr, leftSorts, rightSorts, isLimitedSubstringJoin, NULL);
    assertex(leftSorts.ordinality() == rightSorts.ordinality());
    if (isLimitedSubstringJoin)
        throwError(HQLERR_KeyedDistributeNoSubstringJoin);

    unsigned numUnsortedFields = numPayloadFields(right);
    unsigned numKeyedFields = getFlatFieldCount(indexRecord)-numUnsortedFields;
    if (match || (!expr->hasProperty(firstAtom) && (leftSorts.ordinality() != numKeyedFields)))
        throwError(HQLERR_MustMatchExactly);    //Should already be caught in parser

    KeyedJoinInfo info(*this, expr, false);
    info.processFilter();

    Owned<ABoundActivity> boundDataset1 = buildCachedActivity(ctx, left);
    
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKkeyeddistribute, expr, "KeyedDistribute");
    buildActivityFramework(instance);

    StringBuffer s;
    buildInstancePrefix(instance);

    IHqlExpression * keyFilename = info.queryKeyFilename();
    bool dynamic = hasDynamicFilename(info.queryKey());

    //virtual unsigned getFlags()
    StringBuffer flags;
    if (!keyFilename->isConstant())
        flags.append("|KDFvarindexfilename");
    if (dynamic)
        flags.append("|KDFdynamicindexfilename");


    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * getIndexFileName() = 0;
    buildFilenameFunction(*instance, instance->startctx, "getIndexFileName", keyFilename, dynamic);

    //virtual IOutputMetaData * queryIndexRecordSize() = 0; //Excluding fpos and sequence
    buildMetaMember(instance->classctx, info.queryRawKey(), "queryIndexRecordSize");

    //virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *lhs) = 0;
    info.buildMonitors(instance->startctx);

    //The comparison is against the raw key entries, so expand out the logical to the physical
    //virtual ICompare * queryCompareRowKey() = 0;
    OwnedHqlExpr expandedIndex = convertToPhysicalIndex(right);
    assertex(expandedIndex->getOperator() == no_newusertable);
    IHqlExpression * rawIndex = expandedIndex->queryChild(0);
    OwnedHqlExpr oldSelector = createSelector(no_activetable, right, seq);
    OwnedHqlExpr newSelector = createSelector(no_activetable, rawIndex, seq);
    TableProjectMapper mapper(expandedIndex);

    HqlExprArray normalizedRight;
    ForEachItemIn(i, rightSorts)
    {
        IHqlExpression & curRight = rightSorts.item(i);
        normalizedRight.append(*mapper.expandFields(&curRight, oldSelector, newSelector));
    }
    DatasetReference    leftDs(left, no_activetable, seq);
    DatasetReference    rightDs(rawIndex, no_activetable, seq);

    doCompareLeftRight(instance->nestedctx, "CompareRowKey", leftDs, rightDs, leftSorts, normalizedRight);

    buildFormatCrcFunction(instance->classctx, "getFormatCrc", info.queryRawKey(), info.queryRawKey(), 1);
    buildSerializedLayoutMember(instance->classctx, indexRecord, "getIndexLayout", numKeyedFields);

    OwnedHqlExpr matchExpr = info.getMatchExpr(true);
    assertex(!matchExpr);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset1, 0, 0);
    Owned<ABoundActivity> whoAmI = instance->getBoundActivity();
    addFileDependency(keyFilename, whoAmI);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityKeyDiff(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    IHqlExpression * original = expr->queryChild(0);
    IHqlExpression * updated = expr->queryChild(1);
    IHqlExpression * output = expr->queryChild(2);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKkeydiff, expr, "KeyDiff");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual unsigned getFlags() = 0;
    StringBuffer flags;
    if (expr->hasProperty(overwriteAtom))
        flags.append("|KDPoverwrite");
    else if (expr->hasProperty(noOverwriteAtom))
        flags.append("|KDPnooverwrite");
    if (!output->isConstant())
        flags.append("|KDPvaroutputname");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * queryOriginalName() = 0;         // may be null
    buildRefFilenameFunction(*instance, instance->startctx, "queryOriginalName", original);

    //virtual const char * queryPatchName() = 0;
    buildRefFilenameFunction(*instance, instance->startctx, "queryUpdatedName", updated);

    //virtual const char * queryOutputName() = 0;
    buildFilenameFunction(*instance, instance->startctx, "queryOutputName", output, hasDynamicFilename(expr));

    //virtual int getSequence() = 0;
    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);

    buildExpiryHelper(instance->createctx, expr->queryProperty(expireAtom));

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityKeyPatch(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    StringBuffer s;
    IHqlExpression * original = expr->queryChild(0);
    IHqlExpression * patch = expr->queryChild(1);
    IHqlExpression * output = expr->queryChild(2);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKkeypatch, expr, "KeyPatch");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual unsigned getFlags() = 0;
    StringBuffer flags;
    if (expr->hasProperty(overwriteAtom))
        flags.append("|KDPoverwrite");
    else if (expr->hasProperty(noOverwriteAtom))
        flags.append("|KDPnooverwrite");
    if (!output->isConstant())
        flags.append("|KDPvaroutputname");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * queryOriginalName() = 0;
    buildRefFilenameFunction(*instance, instance->startctx, "queryOriginalName", original);

    //virtual const char * queryPatchName() = 0;
    buildFilenameFunction(*instance, instance->startctx, "queryPatchName", patch, true);

    //virtual const char * queryOutputName() = 0;
    buildFilenameFunction(*instance, instance->startctx, "queryOutputName", output, hasDynamicFilename(expr));

    //virtual int getSequence() = 0;
    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);

    buildExpiryHelper(instance->createctx, expr->queryProperty(expireAtom));

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

IHqlExpression * querySelectorTable(IHqlExpression * expr)
{
    loop
    {
        IHqlExpression * selector = expr->queryChild(0);
        if (selector->getOperator() != no_select)
            return selector;
        switch (selector->queryType()->getTypeCode())
        {
        case type_row:
        case type_record:
            break;
        default:
            return selector;
        }
    }
}

static IHqlExpression * getBlobAttribute(IHqlExpression * ds)
{
    return createAttribute(blobHelperAtom, LINK(ds->queryNormalizedSelector()));
}

IHqlExpression * queryBlobHelper(BuildCtx & ctx, IHqlExpression * select)
{
    OwnedHqlExpr search = getBlobAttribute(querySelectorTable(select));
    HqlExprAssociation * match = ctx.queryAssociation(search, AssocExpr, NULL);
    if (!match)
        return NULL;
    return match->queryExpr();
}

void HqlCppTranslator::associateBlobHelper(BuildCtx & ctx, IHqlExpression * ds, const char * name)
{
    OwnedHqlExpr search = getBlobAttribute(ds);
    OwnedHqlExpr matched = createVariable(name, ds->getType());
    ctx.associateExpr(search, matched);
}


IHqlExpression * HqlCppTranslator::getBlobRowSelector(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * id = expr->queryChild(0);
    IHqlExpression * helper = queryBlobHelper(ctx, id);

    //MORE: Need to clone the dataset attributes. Really they should be included in the type somehow: via modifiers?
    //or give an error if blob used on alien with ref/
    OwnedHqlExpr field = createField(unnamedAtom, expr->getType(), NULL, NULL); 
    HqlExprArray fields;
    fields.append(*LINK(field));
    OwnedHqlExpr record = createRecord(fields);
    OwnedHqlExpr row = createRow(no_anon, LINK(record), createAttribute(_internal_Atom, LINK(id)));
    if (!ctx.queryAssociation(row, AssocRow, NULL))
    {
        Owned<ITypeInfo> rowType = makeReferenceModifier(row->getType());
        OwnedHqlExpr boundRow = ctx.getTempDeclare(rowType, NULL);

        CHqlBoundExpr boundId;
        buildExpr(ctx, id, boundId);
        HqlExprArray args;
        args.append(*LINK(helper));
        args.append(*LINK(boundId.expr));
        OwnedHqlExpr call = bindTranslatedFunctionCall(lookupBlobAtom, args);
        ctx.addAssign(boundRow, call);

        bindRow(ctx, row, boundRow);
    }
    return createSelectExpr(LINK(row), LINK(field));
}


void HqlCppTranslator::doBuildAssignIdToBlob(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    if (!queryBlobHelper(ctx, expr->queryChild(0)))
    {
        //Do the assignment by building an expression then assigning
        doBuildExprAssign(ctx, target, expr);
        return;
    }

    OwnedHqlExpr select = getBlobRowSelector(ctx, expr);
    buildExprAssign(ctx, target, select);
}

void HqlCppTranslator::doBuildExprIdToBlob(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (!queryBlobHelper(ctx, expr->queryChild(0)))
    {
        if (!buildExprInCorrectContext(ctx, expr, tgt, false))
            throwError(HQLERR_BlobTranslationContextNotFound);
        return;
    }
    OwnedHqlExpr select = getBlobRowSelector(ctx, expr);
    buildExpr(ctx, select, tgt);
}

IReferenceSelector * HqlCppTranslator::doBuildRowIdToBlob(BuildCtx & ctx, IHqlExpression * expr, bool isNew)
{
    if (!queryBlobHelper(ctx, expr->queryChild(0)))
        throwError(HQLERR_AccessRowBlobInsideChildQuery);
    OwnedHqlExpr select = getBlobRowSelector(ctx, expr);
    return buildNewOrActiveRow(ctx, select, isNew);
}

void HqlCppTranslator::doBuildExprBlobToId(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    IHqlExpression * value = expr->queryChild(0);
    IHqlExpression * helper = queryBlobHelper(ctx, value);
    assertex(helper);

    Owned<IReferenceSelector> selector = buildReference(ctx, value);
    CHqlBoundExpr boundSize, boundAddress;
    selector->getSize(ctx, boundSize);
    selector->buildAddress(ctx, boundAddress);

    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(boundSize.expr));
    args.append(*LINK(boundAddress.expr));
    tgt.expr.setown(bindTranslatedFunctionCall(createBlobAtom, args));
}
