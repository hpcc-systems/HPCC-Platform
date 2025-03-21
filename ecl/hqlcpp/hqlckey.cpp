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

#include "platform.h"

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

#include "hql.hpp"
#include "hqlmeta.hpp"
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

//--------------------------------------------------------------------------------------------------

void HqlCppTranslator::buildJoinMatchFunction(BuildCtx & ctx, const char * name, IHqlExpression * left, IHqlExpression * right, IHqlExpression * match, IHqlExpression * selSeq)
{
    if (match)
    {
        StringBuffer proto;
        proto.append("virtual bool ").append(name).append("(const void * _left, const void * _right) override");

        MemberFunction matchFunc(*this, ctx, proto, MFdynamicproto|MFoptimize);

        matchFunc.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        matchFunc.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");

        bindTableCursor(matchFunc.ctx, left, "left", no_left, selSeq);
        bindTableCursor(matchFunc.ctx, right, "right", no_right, selSeq);

        OwnedHqlExpr cseMatch = options.spotCSE ? spotScalarCSE(match, NULL, queryOptions().spotCseInIfDatasetConditions) : LINK(match);
        traceExpression("joinMatch", cseMatch);
        buildReturn(matchFunc.ctx, cseMatch);
    }
}

//--------------------------------------------------------------------------------------------------

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
    bool isKeyOpt()                                         { return key->hasAttribute(optAtom); }
    bool isFileOpt()                                        { return file && file->hasAttribute(optAtom); }
    bool needToExtractJoinFields() const                    { return extractJoinFieldsTransform != NULL; }
    bool hasPostFilter() const                              { return monitors->queryExtraFilter() || fileFilter; }
    bool requireActivityForKey() const                      { return hasComplexIndex; }
    bool isKeySigned()                                      { return key->hasAttribute(_signed_Atom); }
    bool isFileSigned()                                     { return file && file->hasAttribute(_signed_Atom); }

    void reportFailureReason(IHqlExpression * cond)         { monitors->reportFailureReason(cond); }
    bool useValueSets() const { return createValueSets; }

protected:
    void buildClearRecord(BuildCtx & ctx, RecordSelectIterator & rawIter, RecordSelectIterator & keyIter);
    void buildTransformBody(BuildCtx & ctx, IHqlExpression * transform);
    IHqlExpression * createKeyFromComplexKey(IHqlExpression * expr);
    IHqlExpression * expandDatasetReferences(IHqlExpression * expr, IHqlExpression * ds);
    IHqlExpression * optimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * expr, IHqlExpression * leftSelector);
    IHqlExpression * doOptimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * expr, IHqlExpression * leftSelector);
    void optimizeExtractJoinFields();
    void optimizeTransfer(SharedHqlExpr & targetDataset, SharedHqlExpr & targetTransform, SharedHqlExpr & keyedFilter, OwnedHqlExpr * extraFilter);
    IHqlExpression * querySimplifiedKey(IHqlExpression * expr);
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
    HqlExprAttr     rawFile;
    HqlExprAttr     keyAccessDataset;
    HqlExprAttr     keyAccessTransform;
    HqlExprAttr     fileAccessDataset;
    HqlExprAttr     fileAccessTransform;
    HqlExprAttr     joinSeq;
    CppFilterExtractor * monitors;
    HqlExprAttr     fileFilter;
    HqlExprAttr     leftOnlyMatch;
    HqlExprAttr     rawRhs;
    TableProjectMapper keyedMapper;
    OwnedHqlExpr    counter;
    OwnedHqlExpr    extractJoinFieldsRecord;
    OwnedHqlExpr    extractJoinFieldsTransform;
    bool            canOptimizeTransfer;
    bool            hasComplexIndex;
    bool            keyHasFileposition;
    bool            createValueSets;
};

KeyedJoinInfo::KeyedJoinInfo(HqlCppTranslator & _translator, IHqlExpression * _expr, bool _canOptimizeTransfer) : translator(_translator)
{ 
    expr.set(_expr);
    joinSeq.set(querySelSeq(expr));
    hasComplexIndex = false;

    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * keyed = expr->queryAttribute(keyedAtom);
    if (keyed && keyed->queryChild(0))
    {
        key.set(keyed->queryChild(0));
        if (right->getOperator() == no_keyed)
            right = right->queryChild(0);
        assertex(getBoolAttribute(right, filepositionAtom, true));
        file.set(right);
        IHqlExpression * rightTable = queryPhysicalRootTable(right);
        if (!rightTable || rightTable->queryNormalizedSelector() != right->queryNormalizedSelector())
            translator.throwError(HQLERR_FullKeyedNeedsFile);
        translator.ensureDiskAccessAllowed(rightTable);
        expandedFile.setown(convertToPhysicalTable(rightTable, true));
        rawFile.set(queryPhysicalRootTable(expandedFile));
        translator.ensureDiskAccessAllowed(rawFile);
        keyedMapper.setDataset(key);
    }
    else if (right->getOperator() == no_newkeyindex)
    {
        key.set(right);
    }
    else
    {
        originalKey.set(right);
        //We could call key.set(querySimplifiedKey(right)) to succeed in some cases instead of generating an error.
        if (translator.getTargetClusterType() == RoxieCluster)
        {
            hasComplexIndex = true;
            key.setown(createKeyFromComplexKey(right));
        }
        else
            translator.throwError1(HQLERR_KeyedJoinNoRightIndex_X, getOpString(right->getOperator()));
    }

    keyHasFileposition = getBoolAttribute(key, filepositionAtom, true);

    //Allow a hint on the join to override an option on the key,
    createValueSets = getHintBool(key, createValueSetsAtom, translator.queryOptions().createValueSets);
    createValueSets = getHintBool(expr, createValueSetsAtom, createValueSets);

    if (!originalKey)
        originalKey.set(key);
    expandedKey.setown(translator.convertToPhysicalIndex(key));
    rawKey.set(queryPhysicalRootTable(expandedKey));
    translator.ensureDiskAccessAllowed(rawKey);
    canOptimizeTransfer = _canOptimizeTransfer; 
    monitors = NULL;
    counter.set(queryAttributeChild(expr, _countProject_Atom, 0));

    if (isFullJoin())
        rawRhs.set(rawFile);
    else
        rawRhs.set(rawKey);
}

KeyedJoinInfo::~KeyedJoinInfo()
{
    delete monitors;
}


IHqlExpression * KeyedJoinInfo::querySimplifiedKey(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_sorted:
        case no_distributed:
        case no_sort:
        case no_distribute:
        case no_preservemeta:
        case no_assertsorted:
        case no_assertgrouped:
        case no_assertdistributed:
        case no_nofold:
        case no_forcegraph:
        case no_nocombine:
        case no_unordered:
            break;
        case no_newkeyindex:
            return LINK(expr);
        default:
            return NULL;
        }
        expr = expr->queryChild(0);
    }
}

IHqlExpression * queryBaseIndexForKeyedJoin(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if (op == no_if)
    {
        IHqlExpression * left = queryBaseIndexForKeyedJoin(expr->queryChild(1));
        IHqlExpression * right = queryBaseIndexForKeyedJoin(expr->queryChild(2));
        if (left && right)
        {
            //IF (cond, index) and IF(cond, null, index) should be allowed, and will return the index
            if (left->getOperator() != no_null)
                return left;
            return right;
        }
        return nullptr;
    }
    else if (op == no_chooseds)
    {
        IHqlExpression * result = nullptr;
        ForEachChildFrom(i, expr, 1)
        {
            IHqlExpression * match = queryBaseIndexForKeyedJoin(expr->queryChild(i));
            if (!match)
                return nullptr;
            if (!result || result->getOperator() == no_null)
                result = match;
        }
        return result;
    }
    else if (op == no_null)
        return expr;
    else if (op == no_split)
        return queryBaseIndexForKeyedJoin(expr->queryChild(0));

    return queryPhysicalRootTable(expr);
}

IHqlExpression * KeyedJoinInfo::createKeyFromComplexKey(IHqlExpression * expr)
{
    IHqlExpression * base = queryBaseIndexForKeyedJoin(expr);
    if (!base)
    {
        translator.throwError1(HQLERR_KeyedJoinNoRightIndex_X, getOpString(expr->getOperator()));
        return NULL;
    }

    if (base->getOperator() == no_newkeyindex)
        return LINK(base);

    translator.throwError1(HQLERR_KeyedJoinNoRightIndex_X, getOpString(base->getOperator()));
    return NULL;
}

void KeyedJoinInfo::buildClearRecord(BuildCtx & ctx, RecordSelectIterator & rawIter, RecordSelectIterator & keyIter)
{
    keyIter.first();
    ForEach(rawIter)
    {
        assert(keyIter.isValid());
        OwnedHqlExpr rawSelect = rawIter.get();
        OwnedHqlExpr keySelect = keyIter.get();

        OwnedHqlExpr null = createNullExpr(keySelect);
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
        MemberFunction func(translator, classctx, "virtual size32_t createDefaultRight(ARowBuilder & crSelf) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        
        BoundRow * selfCursor = translator.bindSelf(func.ctx, rawKey, "crSelf");
        IHqlExpression * rawSelf = selfCursor->querySelector();
        RecordSelectIterator rawIter(rawKey->queryRecord(), rawSelf, false);

        RecordSelectIterator keyIter(key->queryRecord(), key, false);
        buildClearRecord(func.ctx, rawIter, keyIter);
        translator.buildReturnRecordSize(func.ctx, selfCursor);
    }
}


void KeyedJoinInfo::buildExtractFetchFields(BuildCtx & ctx)
{
    // For the data going to the fetch remote activity:
    //virtual size32_t extractFetchFields(ARowBuilder & crSelf, const void * _left) = 0;
    if (fileAccessDataset)
    {
        MemberFunction func(translator, ctx, "virtual size32_t extractFetchFields(ARowBuilder & crSelf, const void * _left) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        if (fileAccessTransform)
        {
            translator.buildTransformBody(func.ctx, fileAccessTransform, expr->queryChild(0), NULL, fileAccessDataset, joinSeq);
        }
        else
        {
            translator.buildRecordSerializeExtract(func.ctx, fileAccessDataset->queryRecord());
        }
    }

    //virtual IOutputMetaData * queryFetchInputRecordSize() = 0;
    translator.buildMetaMember(ctx, fileAccessDataset, false, "queryFetchInputRecordSize");
}


void KeyedJoinInfo::buildExtractIndexReadFields(BuildCtx & ctx)
{
    //virtual size32_t extractIndexReadFields(ARowBuilder & crSelf, const void * _left) = 0;
    MemberFunction func(translator, ctx, "virtual size32_t extractIndexReadFields(ARowBuilder & crSelf, const void * _left) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    if (keyAccessTransform)
    {
        translator.buildTransformBody(func.ctx, keyAccessTransform, expr->queryChild(0), NULL, keyAccessDataset, joinSeq);
    }
    else
    {
        translator.buildRecordSerializeExtract(func.ctx, keyAccessDataset->queryRecord());
    }

    //virtual IOutputMetaData * queryIndexReadInputRecordSize() = 0;
    translator.buildMetaMember(ctx, keyAccessDataset, isGrouped(keyAccessDataset), "queryIndexReadInputRecordSize");    //->false
}


void KeyedJoinInfo::buildExtractJoinFields(ActivityInstance & instance)
{
    //virtual size32_t extractJoinFields(void *dest, const void *diskRow, IBlobProvider * blobs) = 0;
    MemberFunction func(translator, instance.startctx, "virtual size32_t extractJoinFields(ARowBuilder & crSelf, const void *_left, IBlobProvider * blobs) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    if (needToExtractJoinFields())
    {
        OwnedHqlExpr extracted = createDataset(no_anon, LINK(extractJoinFieldsRecord));
        OwnedHqlExpr raw = createDataset(no_anon, LINK(rawRhs->queryRecord()));


        BoundRow * selfCursor = translator.buildTransformCursors(func.ctx, extractJoinFieldsTransform, raw, NULL, extracted, joinSeq);
        if (isHalfJoin())
        {
            OwnedHqlExpr left = createSelector(no_left, raw, joinSeq);
            translator.associateBlobHelper(func.ctx, left, "blobs");
        }

        translator.doBuildTransformBody(func.ctx, extractJoinFieldsTransform, selfCursor);
    }
    else
    {
        translator.buildRecordSerializeExtract(func.ctx, extractJoinFieldsRecord);
    }

    //virtual IOutputMetaData * queryJoinFieldsRecordSize() = 0;
    translator.buildMetaMember(instance.classctx, extractJoinFieldsRecord, false, "queryJoinFieldsRecordSize");
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
        MemberFunction func(translator, ctx, "virtual bool indexReadMatch(const void * _left, const void * _right, IBlobProvider * blobs) override");

        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");

        if (translator.queryOptions().spotCSE)
            matchExpr.setown(spotScalarCSE(matchExpr, NULL, translator.queryOptions().spotCseInIfDatasetConditions));

        translator.associateBlobHelper(func.ctx, rawKey, "blobs");

        translator.bindTableCursor(func.ctx, keyAccessDataset, "left", no_left, joinSeq);
        translator.bindTableCursor(func.ctx, rawKey, "right");

        translator.buildReturn(func.ctx, matchExpr);
    }
}


void KeyedJoinInfo::buildLeftOnly(BuildCtx & ctx)
{
    if (leftOnlyMatch)
    {
        MemberFunction func(translator, ctx, "virtual bool leftCanMatch(const void * _left) override");
        func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *)_left;");
        translator.bindTableCursor(func.ctx, expr->queryChild(0), "left", no_left, joinSeq);
        translator.buildReturn(func.ctx, leftOnlyMatch);
    }
}


void KeyedJoinInfo::buildMonitors(BuildCtx & ctx)
{
    //---- virtual void createSegmentMonitors(struct IIndexReadContext *) { ... } ----
    MemberFunction func(translator, ctx, "virtual void createSegmentMonitors(IIndexReadContext *irc, const void * _left) override");
    func.ctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    translator.bindTableCursor(func.ctx, keyAccessDataset, "left", no_left, joinSeq);
    monitors->buildSegments(func.ctx, "irc", false);
}


void KeyedJoinInfo::buildTransform(BuildCtx & ctx)
{
    MemberFunction func(translator, ctx);
    switch (expr->getOperator())
    {
    case no_join:
    case no_denormalize:
        {
            func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, unsigned counter) override");
            translator.associateCounter(func.ctx, counter, "counter");
            break;
        }
    case no_denormalizegroup:
        {
            func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned numRows, const void * * _rows) override");
            func.ctx.addQuotedLiteral("const byte * * rows = (const byte * *) _rows;");
            break;
        }
    }   

    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr->queryChild(3));
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
        //mapParent.setown(createSelector(no_left, dsParent, querySelSeq(ds)));
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
    switch (ds->getOperator())
    {
    case no_newusertable:
    case no_hqlproject:
        break;
    default:
        return LINK(transform);
    }

    OwnedHqlExpr oldRight = createSelector(no_right, ds, joinSeq);
    OwnedHqlExpr newRight = createSelector(no_right, ds->queryChild(0), joinSeq);
    return ::expandDatasetReferences(transform, ds, oldRight, newRight);
}

void KeyedJoinInfo::buildTransformBody(BuildCtx & ctx, IHqlExpression * transform)
{
    IHqlExpression * rhs = expr->queryChild(1);
    IHqlExpression * rhsRecord = rhs->queryRecord();
    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);

    OwnedHqlExpr originalRight = createSelector(no_right, rhsRecord, joinSeq);
    OwnedHqlExpr serializedRhsRecord = getSerializedForm(rhsRecord, diskAtom);
    OwnedHqlExpr serializedRight = createSelector(no_right, serializedRhsRecord, joinSeq);

    OwnedHqlExpr joinDataset = createDataset(no_anon, LINK(extractJoinFieldsRecord));
    OwnedHqlExpr extractedRight = createSelector(no_right, extractJoinFieldsRecord, joinSeq);

    OwnedHqlExpr newTransform = LINK(transform);

    //The RIGHT passed into the transform does not match the format of the key.
    //In particular all fields will be serialized, and unused fields are likely to be removed.
    //So any references in the transform to RIGHT and fields from RIGHT need to be mapped accordingly.
    //If ROWS(RIGHT) is used, and RIGHT needs to be deserialized, then it needs to be mapped first.
    if (expr->getOperator() == no_denormalizegroup)
    {
        assertex(extractedRight == serializedRight);    // no fields are currently projected out.  Needs to change if they ever are.
        if (extractedRight != originalRight)
        {
            //References to ROWS(originalRight) need to be replaced with DESERIALIZE(ROWS(serializedRight))
            OwnedHqlExpr originalRows = createDataset(no_rows, LINK(originalRight), LINK(rowsid));
            OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(extractedRight), LINK(rowsid));
            OwnedHqlExpr deserializedRows = ensureDeserialized(rowsExpr, rhs->queryType(), diskAtom);
            newTransform.setown(replaceExpression(newTransform, originalRows, deserializedRows));
        }
    }

    newTransform.setown(replaceMemorySelectorWithSerializedSelector(newTransform, rhsRecord, no_right, joinSeq, diskAtom));

    OwnedHqlExpr fileposExpr;
    IHqlExpression * fileposField = isFullJoin() ? queryVirtualFileposField(file->queryRecord()) : nullptr;
    fileposExpr.setown(getFilepos(extractedRight, false));
    if (extractJoinFieldsTransform)
    {
        if (fileposField && (expr->getOperator() != no_denormalizegroup))
        {
            HqlMapTransformer fileposMapper;
            OwnedHqlExpr select = createSelectExpr(LINK(serializedRight), LINK(fileposField));
            OwnedHqlExpr castFilepos = ensureExprType(fileposExpr, fileposField->queryType());
            fileposMapper.setMapping(select, castFilepos);
            newTransform.setown(fileposMapper.transformRoot(newTransform));
        }

        newTransform.setown(replaceSelector(newTransform, serializedRight, extractedRight));
    }
    else
    {
        if (isFullJoin())
            newTransform.setown(expandDatasetReferences(newTransform, expandedFile));
        else
            newTransform.setown(expandDatasetReferences(newTransform, expandedKey));
    }

    newTransform.setown(optimizeHqlExpression(translator.queryErrorProcessor(), newTransform, HOOfold|HOOcompoundproject));
    newTransform.setown(foldHqlExpression(newTransform));

    BoundRow * selfCursor = translator.buildTransformCursors(ctx, newTransform, expr->queryChild(0), joinDataset, expr, joinSeq);

    if (expr->getOperator() == no_denormalizegroup)
    {
        //Last parameter is false since implementation of group keyed denormalize in roxie passes in pointers to the serialized slave data
        bool rowsAreLinkCounted = false;
        translator.bindRows(ctx, no_right, joinSeq, rowsid, joinDataset, "numRows", "rows", rowsAreLinkCounted);
    }

    //Map the file position field in the file to the incoming parameter
    //MORE: This doesn't cope with local/global file position distinctions.
    if (fileposField && (expr->getOperator() != no_denormalizegroup))
    {
        OwnedHqlExpr fileposVar = createVariable("_filepos", makeIntType(8, false));
        ctx.associateExpr(fileposExpr, fileposVar);
    }
    translator.doBuildTransformBody(ctx, newTransform, selfCursor);

    if (isFullJoin())
    {
        SourceFieldUsage * fileUsage = translator.querySourceFieldUsage(rawFile);
        if (fileUsage)
        {
            OwnedHqlExpr rawTransform = expandDatasetReferences(transform, expandedFile);
            OwnedHqlExpr right = createSelector(no_right, rawFile, joinSeq);
            ::gatherFieldUsage(fileUsage, rawTransform, right, false);
        }
    }
    else
    {
        SourceFieldUsage * keyUsage = translator.querySourceFieldUsage(rawKey);
        if (keyUsage)
        {
            OwnedHqlExpr rawTransform = expandDatasetReferences(transform, expandedKey);
            OwnedHqlExpr right = createSelector(no_right, rawKey, joinSeq);
            ::gatherFieldUsage(keyUsage, rawTransform, right, false);
        }
    }
}

void KeyedJoinInfo::buildFailureTransform(BuildCtx & ctx, IHqlExpression * onFailTransform)
{
    MemberFunction func(translator, ctx, "virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, IException * except) override");
    translator.associateLocalFailure(func.ctx, "except");
    translator.ensureRowAllocated(func.ctx, "crSelf");

    buildTransformBody(func.ctx, onFailTransform);
}


static bool fieldNameAlreadyExists(const HqlExprArray & fields, IAtom * name)
{
    ForEachItemIn(i, fields)
    {
        if (fields.item(i).queryName() == name)
            return true;
    }
    return false;
}

IHqlExpression * KeyedJoinInfo::optimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * filter, IHqlExpression * leftSelector)
{
    TransformMutexBlock block;
    return doOptimizeTransfer(fields, values, filter, leftSelector);
}

IHqlExpression * KeyedJoinInfo::doOptimizeTransfer(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * filter, IHqlExpression * leftSelector)
{
    IHqlExpression * prev = static_cast<IHqlExpression *>(filter->queryTransformExtra());
    if (prev)
        return LINK(prev);

    //MORE: We could also check if already transformed..., but that imposes an additional cost on simple filters, so on balance it is better without it
    //if (!filter->usesSelector(leftSelector))
    //    return LINK(filter);

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
            for (;;)
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
                    if (fieldNameAlreadyExists(fields, field->queryName()))
                    {
                        //Check same field isn't used in two different nested records.
                        StringBuffer name;
                        name.append("__unnamed__").append(fields.ordinality());
                        field.setown(createFieldFromValue(createIdAtom(name), field));
                    }

                    fields.append(*LINK(field));
                    values.append(*LINK(filter));
                }

                IHqlExpression * matchField = &fields.item(match);
                OwnedHqlExpr serializedField = getSerializedForm(matchField, diskAtom);
                OwnedHqlExpr result = createSelectExpr(getActiveTableSelector(), LINK(serializedField));
                OwnedHqlExpr deserialized = ensureDeserialized(result, matchField->queryType(), diskAtom);
                filter->setTransformExtra(deserialized);
                return deserialized.getClear();
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
        IHqlExpression * next = doOptimizeTransfer(fields, values, filter->queryChild(i), leftSelector);
        if (!next) return NULL;
        children.append(*next);
    }
    OwnedHqlExpr ret = cloneOrLink(filter, children);
    filter->setTransformExtra(ret);
    return ret.getClear();
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
                OwnedHqlExpr extractedRecord = createRecord(fields);
                OwnedHqlExpr serializedRecord = getSerializedForm(extractedRecord, diskAtom);
                targetDataset.setown(createDataset(no_anon, LINK(serializedRecord), NULL));

                HqlExprArray assigns;
                OwnedHqlExpr self = getSelf(serializedRecord);
                ForEachItemIn(i, fields)
                {
                    IHqlExpression * curField = &fields.item(i);
                    OwnedHqlExpr serializedField = getSerializedForm(curField, diskAtom);
                    OwnedHqlExpr value = ensureSerialized(&values.item(i), diskAtom);
                    assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(serializedField)), LINK(value)));
                }
                targetTransform.setown(createValue(no_newtransform, makeTransformType(serializedRecord->getType()), assigns));
                
                OwnedHqlExpr leftSelect = createSelector(no_left, serializedRecord, joinSeq);
                filter.setown(replaceSelector(newFilter, queryActiveTableSelector(), leftSelect));
                if (hasExtra)
                    extraFilter->setown(replaceSelector(newExtraFilter, queryActiveTableSelector(), leftSelect));

            }
            else if (recordRequiresSerialization(record, diskAtom))
            {
                OwnedHqlExpr serializedRecord = getSerializedForm(record, diskAtom);
                targetDataset.setown(createDataset(no_anon, LINK(serializedRecord)));
                targetTransform.setown(createRecordMappingTransform(no_transform, serializedRecord, oldLeft));

                filter.setown(replaceMemorySelectorWithSerializedSelector(filter, record, no_left, joinSeq, diskAtom));
                if (hasExtra)
                    extraFilter->setown(replaceMemorySelectorWithSerializedSelector(*extraFilter, record, no_left, joinSeq, diskAtom));
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
    if (!cond)
        return;

    RecursionChecker checker;

    doGatherFieldsAccessed(checker, fieldsAccessed, cond, ds);
}

void KeyedJoinInfo::optimizeExtractJoinFields()
{
    HqlExprArray fieldsAccessed;
    bool doExtract = false;
    OwnedHqlExpr right = createSelector(no_right, expr->queryChild(1), joinSeq);
    IHqlExpression * rightRecord = right->queryRecord();
    OwnedHqlExpr extractedRecord;

    if (expr->getOperator() == no_denormalizegroup)
    {
        //Version1: Don't remove any fields
        doExtract = true;
        OwnedHqlExpr rows = createDataset(no_rows, LINK(right), LINK(expr->queryAttribute(_rowsid_Atom)));
        if (isFullJoin())
        {
            extractedRecord.set(file->queryRecord());
        }
        else
        {
            extractedRecord.set(key->queryRecord());
        }
    }
    else
    {
        gatherFieldsAccessed(fieldsAccessed, expr->queryChild(3), right);
        gatherFieldsAccessed(fieldsAccessed, queryAttributeChild(expr, onFailAtom, 0), right);

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

            if (translator.getTargetClusterType() != HThorCluster)
                doExtract = (fieldsAccessed.ordinality() < getFlatFieldCount(keyRecord));

            if (!doExtract && recordContainsBlobs(keyRecord))
                doExtract = true;
        }

        if (recordRequiresSerialization(rightRecord, diskAtom))
            doExtract = true;
    }

    if (doExtract)
    {
        HqlExprArray assigns;
        OwnedHqlExpr left = createSelector(no_left, rawRhs, joinSeq);

        if (extractedRecord || (fieldsAccessed.ordinality() != 0))
        {
            if (!extractedRecord)
                extractedRecord.setown(createRecord(fieldsAccessed));
            extractJoinFieldsRecord.setown(getSerializedForm(extractedRecord, diskAtom));
            OwnedHqlExpr self = getSelf(extractJoinFieldsRecord);
            OwnedHqlExpr memorySelf = getSelf(extractedRecord);

            if (isFullJoin() && (rawRhs->queryBody() == expandedFile->queryBody()))
            {
                assertex(extractedRecord == extractJoinFieldsRecord);
                ForEachChild(i, extractedRecord)
                {
                    IHqlExpression * curMemoryField = extractedRecord->queryChild(i);
                    IHqlExpression * curSerializedField = extractJoinFieldsRecord->queryChild(i);
                    if (!curMemoryField->isAttribute())
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
                    if (!curMemoryField->isAttribute())
                    {
                        OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(curSerializedField));
                        OwnedHqlExpr src = createSelectExpr(LINK(memorySelf), LINK(curMemoryField));
                        OwnedHqlExpr mappedSrc = fieldMapper.expandFields(src, memorySelf, left, rawRhs);
                        assigns.append(*createAssign(tgt.getClear(), ensureSerialized(mappedSrc, diskAtom)));
                    }
                }
            }
        }
        else
        {
            //A bit of a hack - Richard can't cope with zero length values being returned, so allocate
            //a single byte to keep him happy.
            OwnedHqlExpr nonEmptyAttr = createAttribute(_nonEmpty_Atom);
            extractJoinFieldsRecord.setown(createRecord(nonEmptyAttr));
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
    IHqlExpression * atmostAttr = expr->queryAttribute(atmostAtom);
    AtmostLimit atmost(atmostAttr);

    IHqlExpression * cond = expr->queryChild(2);
    OwnedHqlExpr fuzzy, hard;
    splitFuzzyCondition(cond, atmost.required, fuzzy, hard);

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
    if (atmostAttr && fileFilter)
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
    TableProjectMapper mapper(expandedKey);
    OwnedHqlExpr rightSelect = createSelector(no_right, key, joinSeq);
    OwnedHqlExpr newFilter = mapper.expandFields(keyedKeyFilter, rightSelect, rawKey, rawKey);

    //Now extract the filters from it.
    OwnedHqlExpr extra;
    monitors = new CppFilterExtractor(rawKey, translator, -(int)numPayloadFields(rawKey), false, createValueSets);
    if (newFilter)
    {
        monitors->extractFilters(newFilter, extra);
        SourceFieldUsage * fieldUsage = translator.querySourceFieldUsage(rawKey);
        if (fieldUsage)
            monitors->noteKeyedFieldUsage(fieldUsage);
    }

    if (atmostAttr && extra && (atmost.required || !monitors->isCleanlyKeyedExplicitly()))
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

    SourceFieldUsage * keyUsage = translator.querySourceFieldUsage(rawKey);
    if (keyUsage)
    {
        gatherFieldUsage(keyUsage, newFilter, rawKey->queryNormalizedSelector(), false);
        if (isFullJoin())
            keyUsage->noteFilepos();
    }

    if (file && fileFilter)
    {
        SourceFieldUsage * fileUsage = translator.querySourceFieldUsage(rawFile);
        if (fileUsage)
        {
            OwnedHqlExpr rawFilter = expandDatasetReferences(fileFilter, expandedFile);
            OwnedHqlExpr fileRight = createSelector(no_right, rawFile, joinSeq);
            gatherFieldUsage(fileUsage, rawFilter, fileRight, false);
        }
    }

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
    //virtual IOutputMetaData * queryDiskRecordSize() = 0;
    //virtual IOutputMetaData * queryProjectedDiskRecordSize() = 0;
    if (info->isFullJoin())
    {
        buildMetaMember(instance.classctx, info->queryRawRhs(), false, "queryDiskRecordSize");
        buildMetaMember(instance.classctx, info->queryRawRhs(), false, "queryProjectedDiskRecordSize");
    }
    //virtual unsigned __int64 extractPosition(const void * _right) = 0;  // Gets file position value from rhs row
    if (info->isFullJoin())
    {
        IHqlExpression * index = info->queryKey();
        IHqlExpression * indexRecord = index->queryRecord();
        MemberFunction func(*this, instance.startctx, "virtual unsigned __int64 extractPosition(const void * _right) override");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
        bindTableCursor(func.ctx, index, "right");
        OwnedHqlExpr fileposExpr = createSelectExpr(LINK(index), LINK(indexRecord->queryChild(indexRecord->numChildren()-1)));
        buildReturn(func.ctx, fileposExpr);
    }

    //virtual const char * getFileName() = 0;                   // Returns filename of raw file fpos'es refer into
    if (info->isFullJoin())
        buildFilenameFunction(instance, instance.createctx, WaFilename, "getFileName", info->queryFileFilename(), hasDynamicFilename(info->queryFile()), SummaryType::ReadFile, info->isKeyOpt(), info->isFileSigned());

    //virtual bool diskAccessRequired() = 0;
    if (info->isFullJoin())
        doBuildBoolFunction(instance.startctx, "diskAccessRequired", true);

    //virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right) = 0;
    info->buildTransform(instance.startctx);
    IHqlExpression * onFail = expr->queryAttribute(onFailAtom);
    if (onFail)
    {
        //virtual size32_t onFailTransform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _filepos, IException * except)
        info->buildFailureTransform(instance.startctx, onFail->queryChild(0));
    }

    //limit helpers...
    IHqlExpression * limit = expr->queryAttribute(limitAtom);
    if (limit)
    {
        if (limit->hasAttribute(skipAtom))
        {
            MemberFunction func(*this, instance.startctx, "virtual unsigned __int64 getSkipLimit() override");
            buildReturn(func.ctx, limit->queryChild(0));
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
    buildFilenameFunction(instance, instance.startctx, WaIndexname, "getIndexFileName", info->queryKeyFilename(), hasDynamicFilename(info->queryKey()), SummaryType::ReadIndex, info->isKeyOpt(), info->isKeySigned());

    //virtual IOutputMetaData * queryIndexRecordSize() = 0;
    LinkedHqlExpr indexExpr = info->queryKey();
    OwnedHqlExpr serializedRecord;
    unsigned numPayload = numPayloadFields(indexExpr);
    if (numPayload)
        serializedRecord.setown(notePayloadFields(indexExpr->queryRecord(), numPayload));
    else
        serializedRecord.set(indexExpr->queryRecord());
    serializedRecord.setown(getSerializedForm(serializedRecord, diskAtom));

    bool hasFilePosition = getBoolAttribute(indexExpr, filepositionAtom, true);
    serializedRecord.setown(createMetadataIndexRecord(serializedRecord, hasFilePosition));
    buildMetaMember(instance.classctx, serializedRecord, false, "queryIndexRecordSize");
    buildMetaMember(instance.classctx, serializedRecord, false, "queryProjectedIndexRecordSize");

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
    default:
        throwUnexpected();
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
    IHqlExpression * rowlimit = expr->queryAttribute(rowLimitAtom);

    StringBuffer s;
    buildInstancePrefix(instance);

    IHqlExpression * atmostAttr = expr->queryAttribute(atmostAtom);
    AtmostLimit atmost(atmostAttr);

    //virtual unsigned getJoinFlags()
    StringBuffer flags;
    bool isLeftOuter = (expr->hasAttribute(leftonlyAtom) || expr->hasAttribute(leftouterAtom));
    if (expr->hasAttribute(leftonlyAtom)) flags.append("|JFexclude");
    if (isLeftOuter) flags.append("|JFleftouter");
    if (expr->hasAttribute(firstAtom)) flags.append("|JFfirst");
    if (expr->hasAttribute(firstLeftAtom)) flags.append("|JFfirstleft");
    if (transformContainsSkip(expr->queryChild(3)))
        flags.append("|JFtransformMaySkip");
    if (info.isFetchFiltered())
        flags.append("|JFfetchMayFilter");
    if (rowlimit && rowlimit->hasAttribute(skipAtom))
        flags.append("|JFmatchAbortLimitSkips");
    if (rowlimit && rowlimit->hasAttribute(countAtom))
        flags.append("|JFcountmatchabortlimit");
    if (expr->hasAttribute(onFailAtom))
        flags.append("|JFonfail");
    if (info.isKeyOpt())
        flags.append("|JFindexoptional");
    if (info.needToExtractJoinFields())
        flags.append("|JFextractjoinfields");
    if (!isOrdered(expr))
        flags.append("|JFreorderable");
    if (transformReturnsSide(expr, no_left, 0))
        flags.append("|JFtransformmatchesleft");
    if (info.queryKeyFilename() && !info.queryKeyFilename()->isConstant())
        flags.append("|JFvarindexfilename");
    if (hasDynamicFilename(info.queryKey()))      
        flags.append("|JFdynamicindexfilename");
    if (boundIndexActivity)
        flags.append("|JFindexfromactivity");
    if (info.useValueSets())
        flags.append("|JFnewfilters");
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
        if (isNonConstantAndQueryInvariant(info.queryFileFilename()))
            flags.append("|FFinvariantfilename");
    }

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFetchFlags", flags.str()+1);

    //virtual unsigned getJoinLimit()
    if (!isZero(atmost.limit))
        doBuildUnsignedFunction(instance->startctx, "getJoinLimit", atmost.limit);

    //virtual unsigned getKeepLimit()
    LinkedHqlExpr keepLimit = queryAttributeChild(expr, keepAtom, 0);
    if (keepLimit)
        doBuildUnsignedFunction(instance->startctx, "getKeepLimit", keepLimit);

    bool implicitLimit = !rowlimit && !atmostAttr &&
                        (!keepLimit || info.hasPostFilter()) &&
                        !expr->hasAttribute(leftonlyAtom);

    //virtual unsigned getKeepLimit()
    doBuildJoinRowLimitHelper(*instance, rowlimit, info.queryKeyFilename(), implicitLimit);

    buildFormatCrcFunction(instance->classctx, "getIndexFormatCrc", true, info.queryRawKey(), info.queryRawKey(), 1);
    if (info.isFullJoin())
    {
        //Remove virtual attributes from the record, so the crc will be compatible with the disk read record
        //can occur with a (highly unusual) full keyed join to a persist file... (see indexread14.ecl)
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(info.queryRawRhs()->queryRecord());
        buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", noVirtualRecord);
        buildEncryptHelper(instance->startctx, info.queryFile()->queryAttribute(encryptAtom), "getFileEncryptKey");
    }

    IHqlExpression * key = info.queryKey();
    buildSerializedLayoutMember(instance->classctx, key->queryRecord(), "getIndexLayout", numKeyedFields(key));

    //--function to clear right, used for left outer join
    if (isLeftOuter || expr->hasAttribute(onFailAtom))
        info.buildClearRightFunction(instance->createctx);

    buildKeyedJoinExtra(*instance, expr, &info);

    buildKeyJoinIndexReadHelper(*instance, expr, &info);
    buildKeyJoinFetchHelper(*instance, expr, &info);

    info.buildLeftOnly(instance->startctx);

    if (targetRoxie())
    {
        instance->addAttributeBool(WaIsDiskAccessRequired, info.isFullJoin());
        instance->addAttributeBool(WaIsIndexOpt, info.isKeyOpt());
        instance->addAttributeBool(WaIsFileOpt, info.isFileOpt());
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
    if (!targetThor() || insideChildQuery(ctx))
        return buildCachedActivity(ctx, expr->queryChild(0));

    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * indexRecord = right->queryRecord();
    IHqlExpression * seq = querySelSeq(expr);

    JoinSortInfo joinInfo(expr);
    joinInfo.findJoinSortOrders(false);
    if (joinInfo.hasOptionalEqualities())
        throwError(HQLERR_KeyedDistributeNoSubstringJoin);

    unsigned numUnsortedFields = numPayloadFields(right);
    unsigned numKeyedFields = getFlatFieldCount(indexRecord)-numUnsortedFields;
    if (joinInfo.extraMatch || (!expr->hasAttribute(firstAtom) && (joinInfo.queryLeftReq().ordinality() != numKeyedFields)))
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
    if (info.useValueSets())
        flags.append("|KDFnewfilters");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * getIndexFileName() = 0;
    buildFilenameFunction(*instance, instance->startctx, WaIndexname, "getIndexFileName", keyFilename, dynamic, SummaryType::ReadIndex, info.isKeyOpt(), info.isKeySigned());

    //virtual IOutputMetaData * queryIndexRecordSize() = 0;
    LinkedHqlExpr indexExpr = info.queryRawKey();
    OwnedHqlExpr serializedRecord;
    unsigned numPayload = numPayloadFields(indexExpr);
    if (numPayload)
        serializedRecord.setown(notePayloadFields(indexExpr->queryRecord(), numPayload));
    else
        serializedRecord.set(indexExpr->queryRecord());
    serializedRecord.setown(getSerializedForm(serializedRecord, diskAtom));

    bool hasFilePosition = getBoolAttribute(indexExpr, filepositionAtom, true);
    serializedRecord.setown(createMetadataIndexRecord(serializedRecord, hasFilePosition));
    buildMetaMember(instance->classctx, serializedRecord, false, "queryIndexRecordSize");

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
    ForEachItemIn(i, joinInfo.queryRightReq())
    {
        IHqlExpression & curRight = joinInfo.queryRightReq().item(i);
        normalizedRight.append(*mapper.expandFields(&curRight, oldSelector, newSelector));
    }
    DatasetReference    leftDs(left, no_activetable, seq);
    DatasetReference    rightDs(rawIndex, no_activetable, seq);

    doCompareLeftRight(instance->nestedctx, "CompareRowKey", leftDs, rightDs, joinInfo.queryLeftReq(), normalizedRight);

    buildFormatCrcFunction(instance->classctx, "getFormatCrc", true, info.queryRawKey(), info.queryRawKey(), 1);
    buildSerializedLayoutMember(instance->classctx, indexRecord, "getIndexLayout", numKeyedFields);

    OwnedHqlExpr matchExpr = info.getMatchExpr(true);
    if (matchExpr)
        reportError(expr, ERR_MATCH_KEY_EXACTLY,"Condition on DISTRIBUTE must match the key exactly");

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset1, 0, 0);
    Owned<ABoundActivity> whoAmI = instance->getBoundActivity();
    addFileDependency(keyFilename, whoAmI);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityKeyDiff(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    ensureDiskAccessAllowed(expr);

    StringBuffer s;
    IHqlExpression * original = expr->queryChild(0);
    IHqlExpression * updated = expr->queryChild(1);
    IHqlExpression * output = expr->queryChild(2);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKkeydiff, expr, "KeyDiff");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual unsigned getFlags() = 0;
    StringBuffer flags;
    if (expr->hasAttribute(overwriteAtom))
        flags.append("|KDPoverwrite");
    else if (expr->hasAttribute(noOverwriteAtom))
        flags.append("|KDPnooverwrite");
    if (!output->isConstant())
        flags.append("|KDPvaroutputname");
    if (expr->hasAttribute(expireAtom))
        flags.append("|KDPexpires");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * getOriginalName() = 0;         // may be null
    buildRefFilenameFunction(*instance, instance->startctx, WaOriginalFilename, "getOriginalName", original);
    noteAllFieldsUsed(original);

    //virtual const char * getUpdatedName() = 0;
    buildRefFilenameFunction(*instance, instance->startctx, WaUpdatedFilename, "getUpdatedName", updated);
    noteAllFieldsUsed(updated);

    //virtual const char * getOutputName() = 0;
    buildFilenameFunction(*instance, instance->startctx, WaOutputFilename, "getOutputName", output, hasDynamicFilename(expr), SummaryType::WriteFile, false, expr->hasAttribute(_signed_Atom));

    //virtual int getSequence() = 0;
    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);

    buildExpiryHelper(instance->createctx, expr->queryAttribute(expireAtom));

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityKeyPatch(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    ensureDiskAccessAllowed(expr);
    StringBuffer s;
    IHqlExpression * original = expr->queryChild(0);
    IHqlExpression * patch = expr->queryChild(1);
    IHqlExpression * output = expr->queryChild(2);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKkeypatch, expr, "KeyPatch");
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);

    //virtual unsigned getFlags() = 0;
    StringBuffer flags;
    if (expr->hasAttribute(overwriteAtom))
        flags.append("|KDPoverwrite");
    else if (expr->hasAttribute(noOverwriteAtom))
        flags.append("|KDPnooverwrite");
    if (!output->isConstant())
        flags.append("|KDPvaroutputname");
    if (expr->hasAttribute(expireAtom))
        flags.append("|KDPexpires");
    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //virtual const char * getOriginalName() = 0;
    buildRefFilenameFunction(*instance, instance->startctx, WaOriginalFilename, "getOriginalName", original);
    noteAllFieldsUsed(original);

    //virtual const char * getPatchName() = 0;
    buildFilenameFunction(*instance, instance->startctx, WaPatchFilename, "getPatchName", patch, true, SummaryType::ReadFile, false, false);

    //virtual const char * getOutputName() = 0;
    buildFilenameFunction(*instance, instance->startctx, WaOutputFilename, "getOutputName", output, hasDynamicFilename(expr), SummaryType::WriteIndex, false, false);

    //virtual int getSequence() = 0;
    doBuildSequenceFunc(instance->classctx, querySequence(expr), false);
    HqlExprArray xmlnsAttrs;
    Owned<IWUResult> result = createDatasetResultSchema(querySequence(expr), NULL, original->queryRecord(), xmlnsAttrs, false, true, numKeyedFields(original));

    buildExpiryHelper(instance->createctx, expr->queryAttribute(expireAtom));

    buildInstanceSuffix(instance);
    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

IHqlExpression * querySelectorTable(IHqlExpression * expr)
{
    for (;;)
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
    OwnedHqlExpr field = createField(unnamedId, expr->getType(), NULL, NULL);
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
        OwnedHqlExpr call = bindTranslatedFunctionCall(lookupBlobId, args);
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
    tgt.expr.setown(bindTranslatedFunctionCall(createBlobId, args));
}
