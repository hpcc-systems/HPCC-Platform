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

#include "hql.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"
#include "hqlpmap.hpp"
#include "hqlattr.hpp"

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
#include "hqlgraph.ipp"
#include "hqlccommon.hpp"
#include "hqliter.ipp"
#include "hqlinline.hpp"
#include "hqlusage.hpp"
#include "hqlcppds.hpp"

#define MAX_FIXED_SIZE_RAW 1024
#define INLINE_TABLE_EXPAND_LIMIT 4

void addGraphIdAttribute(ActivityInstance * instance, BuildCtx & ctx, IHqlExpression * graphId)
{
    SubGraphInfo * match = matchActiveGraph(ctx, graphId);
    if (!match)
    {
        StringBuffer graphname;
        graphname.append(graphId->queryChild(0)->querySequenceExtra());
        throwError1(HQLERR_AccessUnavailableGraph, graphname.str());
    }
    instance->addAttributeInt("_graphId", match->graphId);
}

//===========================================================================


void HqlCppTranslator::doBuildRowIfBranch(BuildCtx & initctx, BuildCtx & ctx, BoundRow * targetRow, IHqlExpression * branchExpr)
{
    IHqlExpression * targetRowExpr = targetRow->queryBound();

    Owned<IReferenceSelector> rowSelector = buildNewRow(ctx, branchExpr);
    Owned<BoundRow> boundRow = rowSelector->getRow(ctx);
    OwnedHqlExpr rowExpr = getPointer(boundRow->queryBound());
    OwnedHqlExpr castRow = createValue(no_implicitcast, targetRowExpr->getType(), LINK(rowExpr));
    ctx.addAssign(targetRowExpr, castRow);

    if (rowSelector->isConditional())
        targetRow->setConditional(true);
}


IReferenceSelector * HqlCppTranslator::doBuildRowIf(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr foldedCond = foldHqlExpression(expr->queryChild(0));
    if (foldedCond->queryValue())
    {
        unsigned branch = (foldedCond->queryValue()->getBoolValue()) ? 1 : 2;
        return buildNewRow(ctx, expr->queryChild(branch));
    }

    IHqlExpression * trueBranch = expr->queryChild(1);
    IHqlExpression * falseBranch = expr->queryChild(2);

    //Ideally should have a constant modifier on the following row...
    Owned<ITypeInfo> rowType = makeReferenceModifier(expr->getType());
    OwnedHqlExpr rowExpr = ctx.getTempDeclare(rowType, NULL);
    Owned<BoundRow> row = createBoundRow(expr->queryBody(), rowExpr);

    //MORE: Need casts because cursor may be (probably are) constant, but temporary isn't
    //should find out by looking at the a const modifier on the cursor.
    BuildCtx condctx(ctx);

    IHqlStmt * cond = buildFilterViaExpr(condctx, foldedCond);
    //Mark the context as conditional after the filter test, so any temporaries from the filter aren't affected.
    condctx.associateExpr(queryConditionalRowMarker(), rowExpr);

    doBuildRowIfBranch(ctx, condctx, row, trueBranch);

    condctx.selectElse(cond);

    condctx.associateExpr(queryConditionalRowMarker(), rowExpr);
    doBuildRowIfBranch(ctx, condctx, row, falseBranch);

    ctx.associate(*row);
    return createReferenceSelector(row);
}


IReferenceSelector * HqlCppTranslator::doBuildRowDeserializeRow(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * srcRow = expr->queryChild(0);
    IHqlExpression * record = expr->queryRecord();
    IAtom * serializeForm = expr->queryChild(2)->queryName();

    Owned<BoundRow> tempRow = declareLinkedRow(ctx, expr, false);

    CHqlBoundTarget target;
    target.expr.set(tempRow->queryBound());

    HqlExprArray args;  
    args.append(*createSerializer(ctx, record, serializeForm, deserializerAtom));
    args.append(*LINK(srcRow));
    Owned<ITypeInfo> resultType = makeReferenceModifier(makeAttributeModifier(makeRowType(record->getType()), getLinkCountedAttr()));
    OwnedHqlExpr call = bindFunctionCall(rtlDeserializeRowId, args, resultType);
    buildExprAssign(ctx, target, call);

    ctx.associate(*tempRow);
    return createReferenceSelector(tempRow);
}


void HqlCppTranslator::buildConstRow(IHqlExpression * record, IHqlExpression * rowData, CHqlBoundExpr & bound)
{
    OwnedHqlExpr marker = createAttribute(rowAtom, LINK(record), LINK(rowData));
    BuildCtx declareCtx(*code, literalAtom);
    if (declareCtx.getMatchExpr(marker, bound))
        return;

    //MORE: This probably needs to go in the header as well...
    Owned<ITypeInfo> rowType = makeConstantModifier(makeRowType(record->getType()));
    if (options.canLinkConstantRows)
        rowType.setown(setLinkCountedAttr(rowType, true));

    StringBuffer rowName;
    getUniqueId(rowName.append("r"));

    //Generate two variables to cope with the different ways the data is interpreted.
    //Would prefer it to be cleaner... row value would need an associated size
    unsigned dataSize = rowData->queryType()->getSize();
    OwnedITypeInfo declareType;
    OwnedHqlExpr initializer;
    if (options.staticRowsUseStringInitializer)
    {
        //Generates smaller code (and probably more efficient representation in the c++ compiler
        //const byte[5+1] = "Hello";         need an extra byte for the implicit \0
        declareType.setown(makeDataType(dataSize+1));
        initializer.set(rowData);
    }
    else
    {
        //Following is strictly correct, but much larger.
        //const byte[5] = { 'H','e','l','l','o' };
        declareType.set(rowData->queryType());
        initializer.setown(createValue(no_create_initializer, rowData->getType(), LINK(rowData)));
    }

    //MORE: Currently these are marked as const rows, but not generated as such 
    OwnedHqlExpr boundDeclare = createVariable(rowName, makeConstantModifier(LINK(declareType)));
    OwnedHqlExpr boundRow = createVariable(rowName, LINK(rowType));
    declareCtx.addDeclare(boundDeclare, initializer);

    if (options.spanMultipleCpp)
    {
        BuildCtx protoctx(*code, mainprototypesAtom);
        protoctx.addDeclareExternal(boundDeclare);
    }

    bound.length.setown(getSizetConstant(dataSize));
    bound.expr.set(boundRow);
    declareCtx.associateExpr(marker, bound);
}


bool HqlCppTranslator::doBuildRowConstantTransform(IHqlExpression * transform, CHqlBoundExpr & bound)
{
    if (!transform->isConstant() || !options.generateStaticInlineTables)
        return false;
    OwnedHqlExpr constRow = createConstantRowExpr(transform);
    if (!constRow || !canGenerateStringInline(constRow->queryType()->getSize()))
        return false;
    buildConstRow(transform->queryRecord(), constRow, bound);
    return true;
}


IReferenceSelector * HqlCppTranslator::doBuildRowCreateRow(BuildCtx & ctx, IHqlExpression * expr)
{
    CHqlBoundExpr bound;
    if (!doBuildRowConstantTransform(expr->queryChild(0), bound))
        return doBuildRowViaTemp(ctx, expr);
    BoundRow * row = bindConstantRow(ctx, expr, bound);
    return createReferenceSelector(row);
}

BoundRow * HqlCppTranslator::bindConstantRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound)
{
    BoundRow * row = bindRow(ctx, expr, bound.expr);
    //MORE: This should be done more cleanly
    OwnedHqlExpr sizeOfRow = createSizeof(row->querySelector());
    ctx.associateExpr(sizeOfRow, bound.length);
    return row;
}

bool HqlCppTranslator::doBuildRowConstantNull(IHqlExpression * expr, CHqlBoundExpr & bound)
{
    if (!options.generateStaticInlineTables)
        return false;

    IHqlExpression * record = expr->queryRecord();
    OwnedHqlExpr constRow = createConstantNullRowExpr(record);
    if (!constRow)
        return false;

    buildConstRow(record, constRow, bound);
    return true;
}


IReferenceSelector * HqlCppTranslator::doBuildRowNull(BuildCtx & ctx, IHqlExpression * expr)
{
    CHqlBoundExpr bound;
    if (!doBuildRowConstantNull(expr, bound))
        return doBuildRowViaTemp(ctx, expr);

    BoundRow * row = bindRow(ctx, expr, bound.expr);
    return createReferenceSelector(row);
}


IReferenceSelector * HqlCppTranslator::doBuildRowViaTemp(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprAssociation * match;
    if (expr->isDataset())
        match = ctx.queryAssociation(expr->queryNormalizedSelector(), AssocCursor, NULL);
    else
        match = ctx.queryAssociation(expr, AssocRow, NULL);

    if (match)
    {
        BoundRow * row = (BoundRow *)match;
        return createReferenceSelector(row, expr);
    }

    Owned<BoundRow> tempRow = declareTempRow(ctx, ctx, expr);
    buildRowAssign(ctx, tempRow, expr);

    ctx.associate(*tempRow);
    return createReferenceSelector(tempRow);
}


void HqlCppTranslator::buildDefaultRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound)
{
    OwnedHqlExpr clearExpr = createRow(no_null, LINK(expr->queryRecord()), createAttribute(clearAtom));
    BoundRow * matchedRow = (BoundRow *)ctx.queryAssociation(clearExpr, AssocRow, NULL);
    if (!matchedRow)
    {
        if (doBuildRowConstantNull(expr, bound))
        {
            bindRow(ctx, clearExpr, bound.expr);
        }
        else
        {
            BuildCtx * declarectx = &ctx;
            BuildCtx * callCtx = &ctx;
            getInvariantMemberContext(ctx, &declarectx, &callCtx, true, false);

            Owned<BoundRow> tempRow = declareTempRow(*declarectx, *callCtx, clearExpr);

            Owned<BoundRow> rowBuilder = createRowBuilder(*callCtx, tempRow);
            OwnedHqlExpr size = createVariable(LINK(sizetType));
            OwnedHqlExpr clearCall = createClearRowCall(*callCtx, rowBuilder);
            callCtx->addDeclare(size, clearCall);

            OwnedHqlExpr sizeOfRow = createSizeof(rowBuilder->querySelector());
            callCtx->associateExpr(sizeOfRow, size);
            finalizeTempRow(*callCtx, tempRow, rowBuilder);
            declarectx->associate(*tempRow);

            bound.expr.set(tempRow->queryBound());
        }
    }
    else
        bound.expr.set(matchedRow->queryBound());

    //yuk yuk, hack.  If called from a const context then need to make the reference unconst.
    //The real fix is to implement real const tracking throughout the code generator, but that is far from trivial.
    //rkc39.hql is an example...
    if (ctx.queryMatchExpr(constantMemberMarkerExpr))
        bound.expr.setown(createValue(no_cast, makeReferenceModifier(bound.expr->getType()), getPointer(bound.expr)));
}

void HqlCppTranslator::buildNullRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound)
{
    bound.expr.setown(createValue(no_nullptr, makeRowReferenceType(expr)));
}

IReferenceSelector * HqlCppTranslator::doBuildRowFromXMLorJSON(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * onFail = expr->queryAttribute(onFailAtom);

//  assertex(supportsLinkCountedRows);
    Owned<ITypeInfo> overrideType = setLinkCountedAttr(expr->queryType(), true);
    Owned<ITypeInfo> utf8Type = makeUtf8Type(UNKNOWN_LENGTH, NULL);
    IHqlExpression * record = expr->queryRecord();
    OwnedHqlExpr ds = createDataset(no_anon, LINK(record));

    StringBuffer instanceName, factoryName, s;
    bool usesContents = false;
    node_operator op = expr->getOperator();
    getUniqueId(instanceName.append(op==no_fromjson ? "json" : "xml"));
    buildXmlReadTransform(ds, factoryName, usesContents);

    OwnedHqlExpr curActivityId = getCurrentActivityId(ctx);

    //MORE: This should be generalised so that any invariant class creation can be handled by the same code.
    BuildCtx * declareCtx = &ctx;
    BuildCtx * initCtx = &ctx;
    if (!insideOnCreate(ctx) && getInvariantMemberContext(ctx, &declareCtx, &initCtx, false, false))
    {
        declareCtx->addQuoted(s.clear().append("Owned<IXmlToRowTransformer> ").append(instanceName).append(";"));
        s.clear().append(instanceName).append(".setown(").append(factoryName).append("(ctx,");
        generateExprCpp(s, curActivityId).append("));");
        initCtx->addQuoted(s);
    }
    else
    {
        s.append("Owned<IXmlToRowTransformer> ").append(instanceName).append(" = ").append(factoryName).append("(ctx,");
        generateExprCpp(s, curActivityId).append(");");
        ctx.addQuoted(s);
    }

    HqlExprArray args;
    args.append(*ensureExprType(expr->queryChild(1), utf8Type));
    args.append(*createQuoted(instanceName, makeBoolType()));
    args.append(*createConstant(expr->hasAttribute(trimAtom)));
    OwnedHqlExpr function;
    if (op==no_fromjson)
        function.setown(bindFunctionCall(createRowFromJsonId, args, overrideType));
    else
        function.setown(bindFunctionCall(createRowFromXmlId, args, overrideType));

    Owned<BoundRow> result;
    if (onFail)
    {
        result.setown(declareTempRow(ctx, ctx, expr));
        BuildCtx tryctx(ctx);
        tryctx.addTry();
        buildRowAssign(tryctx, result, function);

        HqlExprArray dummyArgs;
        OwnedHqlExpr exception = createParameter(createIdAtom("e"), 0, makePointerType(makeClassType("IException")), dummyArgs);
        BuildCtx catchctx(ctx);
        catchctx.addCatch(exception);
        catchctx.addQuoted("Owned<IException> _e = e;");    // ensure that the exception is released

        associateLocalFailure(catchctx, "e");
        OwnedHqlExpr row = createRow(no_createrow, LINK(onFail->queryChild(0)));
        buildRowAssign(catchctx, result, row);
    }
    else
    {
        CHqlBoundExpr bound;
        buildExpr(ctx, function, bound);

        Owned<ITypeInfo> rowType = makeReferenceModifier(LINK(overrideType));
        OwnedHqlExpr rowExpr = ctx.getTempDeclare(rowType, NULL);
        result.setown(createBoundRow(expr, rowExpr));

        OwnedHqlExpr defaultRowPtr = getPointer(bound.expr);
        ctx.addAssign(rowExpr, defaultRowPtr);
    }

    ctx.associate(*result);
    return createReferenceSelector(result);
}




//NB: If this is a dataset operation, this function assumes that any parent datasets are already in scope 
//    i.e. processed in buildDatasetAssign()
//    the one exception is aggregate because it needs to treat its input differently.

IReferenceSelector * HqlCppTranslator::buildNewOrActiveRow(BuildCtx & ctx, IHqlExpression * expr, bool isNew)
{
    if (isNew)
        return buildNewRow(ctx, expr);
    else
        return buildActiveRow(ctx, expr);
}


IReferenceSelector * HqlCppTranslator::buildNewRow(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(!expr->isDataset());
    BoundRow * match = static_cast<BoundRow *>(ctx.queryAssociation(expr, AssocRow, NULL));
    if (match)
        return createReferenceSelector(match, expr);

    BoundRow * row = NULL;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_activerow:
        return buildActiveRow(ctx, expr->queryChild(0));
    case no_if:
        return doBuildRowIf(ctx, expr);
    case no_id2blob:
        return doBuildRowIdToBlob(ctx, expr, true);
    case no_index:
    case no_selectnth:
        return buildDatasetIndex(ctx, expr);
    case no_selectmap:
        return buildDatasetSelectMap(ctx, expr);
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_activetable:
        return buildActiveRow(ctx, expr);
    case no_fromxml:
    case no_fromjson:
        return doBuildRowFromXMLorJSON(ctx, expr);
    case no_serialize:
        {
            IHqlExpression * deserialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(1)->queryName();
            if (isDummySerializeDeserialize(expr))
                return buildNewRow(ctx, deserialized->queryChild(0));
            else if (!typeRequiresDeserialization(deserialized->queryType(), serializeForm))
                return buildNewRow(ctx, deserialized);
            else
                return doBuildRowViaTemp(ctx, expr);
        }
    case no_deserialize:
        {
            IHqlExpression * serialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(2)->queryName();
            if (isDummySerializeDeserialize(expr))
                return buildNewRow(ctx, serialized->queryChild(0));
            else if (!typeRequiresDeserialization(expr->queryType(), serializeForm))
                return buildNewRow(ctx, serialized);
            else
                return doBuildRowDeserializeRow(ctx, expr);
        }
    case no_deref:
        {
            //Untested
            CHqlBoundExpr bound;
            buildExpr(ctx, expr, bound);
            row = bindRow(ctx, expr, bound.expr);
            break;
        }
    case no_createrow:
        return doBuildRowCreateRow(ctx, expr);
    case no_newusertable:
    case no_hqlproject:
    case no_temprow:
    case no_projectrow:
        return doBuildRowViaTemp(ctx, expr);
    case no_null:
        return doBuildRowNull(ctx, expr);
    case no_typetransfer:
        {
            CHqlBoundExpr bound;
            IHqlExpression * value = expr->queryChild(1);
            if (value->isDatarow())
                buildAddress(ctx, value, bound);
            else
                buildExpr(ctx, value, bound);

            OwnedHqlExpr cursorExpr = createValue(no_implicitcast, makeReferenceModifier(expr->getType()), LINK(bound.expr));
            row = bindRow(ctx, expr, cursorExpr);
            break;
        }
    case no_getresult:
        {
            IAtom * serializeForm = diskAtom;  // What if we start using internal in the engines?
            IHqlExpression * seqAttr = expr->queryAttribute(sequenceAtom);
            IHqlExpression * nameAttr = expr->queryAttribute(namedAtom);
            IHqlExpression * record = expr->queryRecord();
            OwnedHqlExpr serializedRecord = getSerializedForm(record, serializeForm);

            OwnedHqlExpr temp = createDatasetF(no_getresult, LINK(serializedRecord), LINK(seqAttr), LINK(nameAttr), NULL);
            OwnedHqlExpr row = createRow(no_selectnth, LINK(temp), createComma(getSizetConstant(1), createAttribute(noBoundCheckAtom)));
            row.setown(ensureDeserialized(row, expr->queryType(), serializeForm));
            return buildNewRow(ctx, row);
        }
    case no_matchattr:
        return doBuildRowMatchAttr(ctx, expr);
    case no_matchrow:
        return doBuildRowMatchRow(ctx, expr, true);
    case no_getgraphresult:
    case no_call:
    case no_externalcall:
    case no_alias:
    case no_translated:
    case no_libraryinput:
        {
            CHqlBoundExpr bound;
            buildExpr(ctx, expr, bound);

            Owned<ITypeInfo> rawType = removeModifier(expr->queryType(), typemod_ref);
            OwnedHqlExpr cursorExpr = createValue(no_implicitcast, makeReferenceModifier(LINK(rawType)), LINK(bound.expr));
            row = bindRow(ctx, expr, cursorExpr);
            if (bound.length)
            {
                OwnedHqlExpr sizeOfRow = createSizeof(row->querySelector());
                ctx.associateExpr(sizeOfRow, bound.length);
            }

            //We could associate the original expression to allow better cse for child datasets in transforms, but it doesn't actually improve any examples
            //IHqlExpression * original = queryAttributeChild(expr, _original_Atom, 0);
            //if (original)
            //  bindRow(ctx, original, cursorExpr)->setResultAlias();
            break;//return createReferenceSelector(cursor);
        }
    case no_comma:
    case no_compound:
        buildStmt(ctx, expr->queryChild(0));
        return buildNewRow(ctx, expr->queryChild(1));
    case no_select:
        {
#ifdef _DEBUG
            IHqlExpression * field = expr->queryChild(1);
#endif
            Owned<IReferenceSelector> selector;
            if (isNewSelector(expr))
                selector.setown(buildNewRow(ctx, expr->queryChild(0)));
            else
                selector.setown(buildActiveRow(ctx, expr->queryChild(0)));
            return selector->select(ctx, expr);
        }
        //If called because known to be a single row.
    case no_datasetfromrow:
    case no_nofold:
    case no_nohoist:
    case no_forcegraph:
    case no_nocombine:
    case no_section:
    case no_sectioninput:
        return buildNewRow(ctx, expr->queryChild(0));
    case no_skip:
        {
            buildStmt(ctx, expr);
            OwnedHqlExpr null = createNullExpr(expr);
            return buildNewRow(ctx, null);
        }
    case no_alias_scope:
        {
            expandAliasScope(ctx, expr);
            return buildNewRow(ctx, expr->queryChild(0));
        }
    case no_split:
        throwUnexpected();
        //not at all sure about this.....
        return buildNewRow(ctx, expr->queryChild(0));
    default:
        {
            HqlExprAssociation * match;
            if (expr->isDataset())
                match = ctx.queryAssociation(expr->queryNormalizedSelector(), AssocCursor, NULL);
            else
                match = ctx.queryAssociation(expr, AssocRow, NULL);

            if (match)
            {
                BoundRow * row = (BoundRow *)match;
                IReferenceSelector * alias = row->queryAlias();
                if (alias)
                    return LINK(alias);
                return createReferenceSelector(row, expr);
            }
            UNIMPLEMENTED_XY("row", getOpString(expr->getOperator()));
        }
    }

    assertex(row);
    return createReferenceSelector(row);
}


IReferenceSelector * HqlCppTranslator::buildActiveRow(BuildCtx & ctx, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_activetable:
    case no_selfref:            // shouldn't ever occur...
        //All selectors should be listed here...
        break;
    case no_activerow:
        return buildActiveRow(ctx, expr->queryChild(0));
    default:
        if (!expr->isDataset() && !expr->isDictionary())
            return buildNewRow(ctx, expr);
        break;
    }

    HqlExprAssociation * match = ctx.queryAssociation(expr->queryNormalizedSelector(), AssocCursor, NULL);
    if (match)
    {
        BoundRow * row = (BoundRow *)match;
        IReferenceSelector * alias = row->queryAlias();
        if (alias)
            return LINK(alias);
        return createReferenceSelector(row, expr);
    }

    switch (op)
    {
    case no_select:
        {
#ifdef _DEBUG
            IHqlExpression * field = expr->queryChild(1);
#endif
            Owned<IReferenceSelector> selector = buildNewOrActiveRow(ctx, expr->queryChild(0), isNewSelector(expr));
            return selector->select(ctx, expr);
        }
    case no_id2blob:
        return doBuildRowIdToBlob(ctx, expr, false);
    }

    StringBuffer tablename;
    getExprIdentifier(tablename, expr);

    traceExpression("Dataset not found", expr);

    RowAssociationIterator iter(ctx);
    ForEach(iter)
    {
        BoundRow & cur = iter.get();
        traceExpression("BoundCursor:", cur.querySelector());
    }
    throwError1(HQLERR_DatasetNotActive, tablename.str());
    return NULL; //remove warning about control paths
}

BoundRow * HqlCppTranslator::ensureLinkCountedRow(BuildCtx & ctx, BoundRow * row)
{
    if (row->isLinkCounted())
        return row;

    OwnedHqlExpr srcRow = createTranslated(row->queryBound());
    OwnedHqlExpr tempRowExpr = declareLinkedRowExpr(ctx, row->queryRecord(), false);
    Owned<BoundRow> tempRow = row->clone(tempRowExpr);

    OwnedHqlExpr source = getPointer(row->queryBound());
    BuildCtx subctx(ctx);
    if (row->isConditional())
        subctx.addFilter(source);

    IHqlExpression * sourceExpr = row->querySelector();
    OwnedHqlExpr rowExpr = sourceExpr->isDataset() ? ensureActiveRow(sourceExpr) : LINK(sourceExpr);
    OwnedHqlExpr size = createSizeof(rowExpr);
    CHqlBoundExpr boundSize;
    buildExpr(subctx, size, boundSize);

    StringBuffer allocatorName;
    ensureRowAllocator(allocatorName, ctx, row->queryRecord(), getCurrentActivityId(subctx));

    StringBuffer s;
    s.append("rtlCloneRow(").append(allocatorName).append(",");
    generateExprCpp(s, boundSize.expr).append(",");
    generateExprCpp(s, source);
    s.append(")");
    OwnedHqlExpr call = createQuoted(s, tempRow->queryBound()->getType());

    subctx.addAssign(tempRow->queryBound(), call);

    ctx.associate(*tempRow);
    return tempRow;
}

IReferenceSelector * HqlCppTranslator::ensureLinkCountedRow(BuildCtx & ctx, IReferenceSelector * source)
{
    if (!source->isRoot() || !source->queryRootRow()->isLinkCounted())
    {
        Owned<BoundRow> row = source->getRow(ctx);
        BoundRow * lcrRow = ensureLinkCountedRow(ctx, row);
        assertex(row != lcrRow);
        return createReferenceSelector(lcrRow, source->queryExpr());
    }
    return LINK(source);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildExprAggregate(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    OwnedHqlExpr normalized = normalizeAnyDatasetAliases(expr);
    if (expr != normalized)
    {
        buildExpr(ctx, normalized, tgt);
        return;
    }

    node_operator op = expr->getOperator();
    ITypeInfo * type = expr->queryType();
    ITypeInfo * tempType = op == no_count ? unsignedType : type;

    LoopInvariantHelper helper;
    BuildCtx aggctx(ctx);
    if (options.optimizeLoopInvariant)
        helper.getBestContext(aggctx, expr);

    CHqlBoundTarget result;
    createTempFor(aggctx, tempType, result, typemod_none, FormatNatural);
    doBuildAssignAggregate(aggctx, result, expr);

    tgt.setFromTarget(result);
    if (!isSameBasicType(type, tempType))
        tgt.expr.setown(createValue(no_implicitcast, LINK(type), tgt.expr.getClear()));
    if (expr->isPure())
        aggctx.associateExpr(expr, tgt);
}


void HqlCppTranslator::doBuildAssignAggregateLoop(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IHqlExpression * dataset, IHqlExpression * doneFirstVar, bool multiPath)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_exists:
        {
            OwnedHqlExpr optimized = queryOptimizedExists(ctx, expr, dataset);
            if (optimized)
            {
                if (matchesBoolean(optimized, false))
                    return;

                if (multiPath)
                {
                    BuildCtx condctx(ctx);
                    condctx.addFilter(optimized);
                    assignBound(condctx, target, queryBoolExpr(true));
                }
                else
                    assignBound(ctx, target, optimized);
                return;
            }
            break;
        }
    case no_count:
        {
            CHqlBoundExpr temp;
            if (canBuildOptimizedCount(ctx, dataset, temp))
            {
                OwnedHqlExpr thisCount = temp.getTranslatedExpr();
                buildIncrementAssign(ctx, target, thisCount);
                return;
            }
            break;
        }
    }

    switch (dataset->getOperator())
    {
    case no_if:
        {
            BuildCtx subctx(ctx);
            IHqlStmt * stmt = buildFilterViaExpr(subctx, dataset->queryChild(0));
            doBuildAssignAggregateLoop(subctx, target, expr, dataset->queryChild(1), doneFirstVar, multiPath);
            subctx.selectElse(stmt);
            doBuildAssignAggregateLoop(subctx, target, expr, dataset->queryChild(2), doneFirstVar, multiPath);
            return;
        }
    case no_addfiles:
        {
            doBuildAssignAggregateLoop(ctx, target, expr, dataset->queryChild(0), doneFirstVar, true);
            doBuildAssignAggregateLoop(ctx, target, expr, dataset->queryChild(1), doneFirstVar, true);
            return;
        }
    case no_chooseds:
        {
            CHqlBoundExpr cond;
            buildExpr(ctx, dataset->queryChild(0), cond);

            IHqlExpression * last = queryLastNonAttribute(dataset);
            BuildCtx subctx(ctx);
            IHqlStmt * switchstmt = subctx.addSwitch(cond.expr);
            ForEachChildFrom(i, dataset, 1)
            {
                IHqlExpression * cur = dataset->queryChild(i);
                if (cur != last)
                {
                    OwnedHqlExpr label = getSizetConstant(i);
                    subctx.addCase(switchstmt, label);
                }
                else
                    subctx.addDefault(switchstmt);

                doBuildAssignAggregateLoop(subctx, target, expr, cur, doneFirstVar, multiPath);
            }
            return;
        }
    case no_null:
        return;
    }

    LinkedHqlExpr arg = expr->queryChild(1);
    IHqlExpression * oldDataset = expr->queryChild(0);
    //If no_if or no_addfiles has been optimized above then the selector for the argument will have changed => map it.
    if (arg && (dataset != oldDataset))
        arg.setown(replaceSelector(arg, oldDataset, dataset));

    bool needToBreak = (op == no_exists);
    if (needToBreak)
    {
        //if it can have at most one row (fairly strange code!) then don't add a break
        //unless it was deliberately a choosen to restrict the number of iterations.
        if (hasNoMoreRowsThan(dataset, 1) && (dataset->getOperator() != no_choosen))
            needToBreak = false;
    }

    BuildCtx loopctx(ctx);
    buildDatasetIterate(loopctx, dataset, needToBreak);

    switch (op)
    {
    case no_exists:
        buildExprAssign(loopctx, target, queryBoolExpr(true));
        if (needToBreak)
            loopctx.addBreak();
        break;
    case no_count:
        {
            OwnedHqlExpr inc = createValue(no_postinc, makeVoidType(), LINK(target.expr));
            loopctx.addExpr(inc);
            break;
        }
    case no_sum:
        {
            OwnedHqlExpr cseArg = options.spotCSE ? spotScalarCSE(arg, NULL, queryOptions().spotCseInIfDatasetConditions) : LINK(arg);
            buildIncrementAssign(loopctx, target, cseArg);
            break;
        }
    case no_min:
    case no_max:
        {
            BuildCtx maxctx(loopctx);
            OwnedHqlExpr resultExpr = target.getTranslatedExpr();

            OwnedHqlExpr cseArg = options.spotCSE ? spotScalarCSE(arg, NULL, queryOptions().spotCseInIfDatasetConditions) : LINK(arg);
            OwnedHqlExpr simpleArg = buildSimplifyExpr(loopctx, cseArg);
            OwnedHqlExpr test = createBoolExpr((op == no_min) ? no_lt : no_gt, LINK(simpleArg), LINK(resultExpr));
            if (doneFirstVar)
            {
                IHqlExpression * firstTest = createValue(no_not, makeBoolType(), LINK(doneFirstVar));
                test.setown(createBoolExpr(no_or, firstTest, test.getClear()));
            }
            buildFilter(maxctx, test);

            buildExprAssign(maxctx, target, simpleArg);
            if (doneFirstVar)
                buildAssignToTemp(maxctx, doneFirstVar, queryBoolExpr(true));
            break;
        }
    default:
        assertex(!"unknown aggregate on child datasets");
        break;
    }
}

bool assignAggregateDirect(const CHqlBoundTarget & target, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    ITypeInfo * type = expr->queryType();
    ITypeInfo * tempType = op == no_count ? unsignedType : type;

    if (!isSameUnqualifiedType(target.queryType(), tempType))
        return false;

    //For exists/count/sum use a temporary variable, and then assign rather than accumulating directly in the target
    switch (op)
    {
    case no_sum:
        if (type->getTypeCode() != type_int)
            break;
        //fall through
    case no_exists:
    case no_count:
        if (target.expr->getOperator() != no_variable)
            return false;
        break;
    }
    return true;
}

static bool isNullValueMinimumValue(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_int:
    case type_swapint:
    case type_decimal:
        return !type->isSigned();
    case type_data:
    case type_qstring:
        return true;
    }
    return false;
}

void HqlCppTranslator::doBuildAssignAggregate(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * _expr)
{
    OwnedHqlExpr expr = normalizeAnyDatasetAliases(_expr);
    if (assignAggregateDirect(target, expr))
    {
        IHqlExpression * dataset = expr->queryChild(0);
        OwnedHqlExpr resultExpr = target.getTranslatedExpr();

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_exists:
            buildExprAssign(ctx, target, queryBoolExpr(false));
            break;
        default:
            {
                OwnedHqlExpr null = createNullExpr(target.queryType());
                buildExprAssign(ctx, target, null);
                break;
            }
        }

        OwnedHqlExpr doneFirstVar;
        if ((op == no_min) || ((op == no_max) && !isNullValueMinimumValue(target.queryType())))
        {
            doneFirstVar.setown(ctx.getTempDeclare(queryBoolType(), queryBoolExpr(false)));
        }

        doBuildAssignAggregateLoop(ctx, target, expr, dataset, doneFirstVar, false);
    }
    else
    {
        doBuildExprAssign(ctx, target, expr);
    }
}


//---------------------------------------------------------------------------

bool HqlCppTranslator::canBuildOptimizedCount(BuildCtx & ctx, IHqlExpression * dataset, CHqlBoundExpr & tgt)
{
    switch (dataset->getOperator())
    {
    case no_select:
        {
            if (isMultiLevelDatasetSelector(dataset, false))
                return false;

            Owned<IReferenceSelector> selector = buildReference(ctx, dataset);
            CHqlBoundExpr temp;
            selector->get(ctx, temp);
            tgt.expr.setown(getBoundCount(temp));
            return true;
        }
        break;
    default:
        if (!alwaysEvaluatesToBound(dataset))
            break;
        //fall through
    case no_rows:
    case no_null:
        {
            CHqlBoundExpr temp;
            buildDataset(ctx, dataset, temp, FormatNatural);
            tgt.expr.setown(getBoundCount(temp));
            return true;
        }
    }

#if 0
    //This is improves a few obscure cases (normally in the global context).  I'm not convinced it is worth the extra cycles.
    //Could also remove the bound.count test.
    CHqlBoundExpr bound;
    if (ctx.getMatchExpr(dataset, bound) && bound.count)
    {
        tgt.expr.setown(getBoundCount(bound));
        return true;
    }
#endif

    return false;
}

void HqlCppTranslator::doBuildExprCount(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (expr->hasAttribute(keyedAtom))
        throwError(HQLERR_KeyedCountNonKeyable);

    IHqlExpression * dataset = expr->queryChild(0);
    CHqlBoundExpr temp;
    if (canBuildOptimizedCount(ctx, dataset, temp))
    {
        OwnedHqlExpr translated = temp.getTranslatedExpr();
        OwnedHqlExpr cast = ensureExprType(translated, expr->queryType());
        buildExpr(ctx, cast, tgt);
    }
    else
        doBuildExprAggregate(ctx, expr, tgt);
}

//---------------------------------------------------------------------------

IHqlExpression * HqlCppTranslator::queryOptimizedExists(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * dataset)
{
    node_operator dsOp = dataset->getOperator();
    //really this is isSimple()

    CHqlBoundExpr optimized;
    bool canOptimizeCount = canBuildOptimizedCount(ctx, dataset, optimized);
    node_operator op = (expr->getOperator() == no_exists) ? no_ne : no_eq;
    bool specialCase = false;
    switch (dsOp)
    {
    case no_select:
        specialCase = canOptimizeCount;
        break;
    default:
        specialCase = !canOptimizeCount && alwaysEvaluatesToBound(dataset);
        break;
    }

    OwnedHqlExpr test;
    if (specialCase)
    {
        CHqlBoundExpr temp;
        buildDataset(ctx, dataset, temp, FormatNatural);
        if (temp.count)
            test.set(temp.count);
        else
            test.setown(getBoundSize(temp));
    }
    else if (canOptimizeCount)
    {
        test.set(optimized.expr);
    }

    if (test)
    {
        OwnedHqlExpr cond = createBoolExpr(op, LINK(test), createConstant(test->queryType()->castFrom(false, 0)));
        if (cond->isConstant())
            return foldHqlExpression(cond);
        return cond.getClear();
    }

    return NULL;
}

void HqlCppTranslator::doBuildExprExists(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    OwnedHqlExpr optimized = queryOptimizedExists(ctx, expr, expr->queryChild(0));
    if (optimized)
        tgt.expr.setown(optimized.getClear());
    else
        doBuildExprAggregate(ctx, expr, tgt);
}

//---------------------------------------------------------------------------

static IHqlExpression * createMinMax(node_operator compareOp, ITypeInfo * type, IHqlExpression * left, IHqlExpression * right)
{
    return createValue(no_if, LINK(type),
                            createBoolExpr(compareOp, LINK(left), LINK(right)), 
                            LINK(left), LINK(right));
}

bool HqlCppTranslator::doBuildAggregateMinMaxList(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, IHqlExpression * list, CHqlBoundExpr * tgt, node_operator compareOp)
{
    OwnedHqlExpr value;

    unsigned max = list->numChildren();
    switch(max)
    {
    case 0:
        value.setown(createNullExpr(expr));
        break;
    case 1:
        value.set(list->queryChild(0));
        break;
    case 2:
    case 3:
        {
            OwnedHqlExpr simple[3];
            for (unsigned i=0; i < max; i++)
            {
                CHqlBoundExpr bound;
                buildSimpleExpr(ctx, list->queryChild(i), bound);
                simple[i].setown(bound.getTranslatedExpr());
            }

            ITypeInfo * type = expr->queryType();
            if (max == 2)
                value.setown(createMinMax(compareOp, type, simple[0], simple[1]));
            else
            {
                OwnedHqlExpr cmp02 = createMinMax(compareOp, type, simple[0], simple[2]);
                OwnedHqlExpr cmp12 = createMinMax(compareOp, type, simple[1], simple[2]);
                value.setown(createValue(no_if, expr->getType(),
                            createBoolExpr(compareOp, LINK(simple[0]), LINK(simple[1])), 
                            LINK(cmp02), LINK(cmp12)));
            }
        }
    }

    if (value)
    {
        buildExprOrAssign(ctx, target, value, tgt);
        return true;
    }

    {
        CHqlBoundTarget temp;
        createTempFor(ctx, expr, temp);
        buildExprAssign(ctx, temp, list->queryChild(0));
        OwnedHqlExpr best = temp.getTranslatedExpr();
        for (unsigned i=1; i < list->numChildren(); i++)
        {
            CHqlBoundExpr bound;
            buildSimpleExpr(ctx, list->queryChild(i), bound);
            OwnedHqlExpr simple = bound.getTranslatedExpr();
            BuildCtx subctx(ctx);
            OwnedHqlExpr cond = createBoolExpr(compareOp, LINK(simple), LINK(best));
            buildFilter(subctx, cond);
            buildExprAssign(subctx, temp, simple);
        }
        buildExprOrAssign(ctx, target, best, tgt);
        return true;
    }
}

void HqlCppTranslator::doBuildAggregateList(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * tgt)
{
    OwnedHqlExpr list = normalizeListCasts(expr->queryChild(0));

    if (list->getOperator() == no_alias_scope)
    {
        expandAliasScope(ctx, list);
        list.set(list->queryChild(0));
    }

    node_operator aggOp;
    switch (expr->getOperator())
    {
    case no_existslist:
        {
            //Fixed length lists should have been optimized away
            CHqlBoundExpr bound;
            buildExpr(ctx, list, bound);
            OwnedHqlExpr test;
            if (bound.count)
                test.set(bound.count);
            else
                test.setown(getBoundLength(bound));
            OwnedHqlExpr value = createValue(no_ne, makeBoolType(), LINK(test), ensureExprType(queryZero(), test->queryType()));
            OwnedHqlExpr translated = createTranslated(value);
            buildExprOrAssign(ctx, target, translated, tgt);
            return;
        }
    case no_countlist:
        {
            //Fixed length lists should have been optimized away
            CHqlBoundExpr bound;
            buildExpr(ctx, list, bound);
            OwnedHqlExpr test = getBoundCount(bound);
            OwnedHqlExpr value = ensureExprType(test, expr->queryType());
            OwnedHqlExpr translated = createTranslated(value);
            buildExprOrAssign(ctx, target, translated, tgt);
            return;
        }
    case no_sumlist:
        aggOp = no_sumgroup;
        if (list->getOperator() == no_list)
        {
            HqlExprArray args;
            ITypeInfo * exprType = expr->queryType();
            ForEachChild(i, list)
                args.append(*ensureExprType(list->queryChild(i), exprType));
            OwnedHqlExpr balanced = createBalanced(no_add, exprType, args);
            if (!balanced)
                balanced.setown(ensureExprType(queryZero(), exprType));
            buildExprOrAssign(ctx, target, balanced, tgt);
            return;
        }
        //special case fixed length lists
        break;
    case no_minlist:
        aggOp = no_mingroup;
        if (list->getOperator() == no_list)
        {
            if (doBuildAggregateMinMaxList(ctx, target, expr, list, tgt, no_lt))
                return;
        }
        break;
    case no_maxlist:
        aggOp = no_maxgroup;
        if (list->getOperator() == no_list)
        {
            if (doBuildAggregateMinMaxList(ctx, target, expr, list, tgt, no_gt))
                return;
        }
        break;
    default:
        throwUnexpectedOp(expr->getOperator());
    }

    ITypeInfo * elemType = list->queryType()->queryChildType();
    if (!elemType)
        elemType = defaultIntegralType;

    //Default implementation in terms of a dataset
    OwnedHqlExpr field = createField(valueId, LINK(elemType), NULL);
    OwnedHqlExpr record = createRecord(field);
    OwnedHqlExpr ds = createDataset(no_temptable, LINK(list), LINK(record));

    OwnedHqlExpr aggField = createField(valueId, expr->getType(), NULL);
    OwnedHqlExpr aggRecord = createRecord(aggField);
    OwnedHqlExpr self = createSelector(no_self, aggRecord, NULL);
    OwnedHqlExpr aggExpr = createValue(aggOp, expr->getType(), createSelectExpr(LINK(ds), LINK(field)));
    OwnedHqlExpr aggAssign = createAssign(createSelectExpr(LINK(self), LINK(aggField)), LINK(aggExpr));
    OwnedHqlExpr aggTransform = createValue(no_newtransform, makeTransformType(aggRecord->getType()), LINK(aggAssign));
    OwnedHqlExpr agg = createDataset(no_newaggregate, LINK(ds), createComma(LINK(aggRecord), LINK(aggTransform)));
    OwnedHqlExpr result = createNewSelectExpr(createRow(no_selectnth, LINK(agg), createConstantOne()), LINK(aggField));
    buildExprOrAssign(ctx, target, result, tgt);
}

//---------------------------------------------------------------------------

static HqlTransformerInfo graphIndependanceCheckerInfo("GraphIndependanceChecker");
class GraphIndependanceChecker : public NewHqlTransformer
{
public:
    GraphIndependanceChecker(IHqlExpression * _graph) : NewHqlTransformer(graphIndependanceCheckerInfo), graph(_graph) { independent = true; }

    void analyseExpr(IHqlExpression * expr)
    {
        if (!independent || alreadyVisited(expr))
            return;

        switch (expr->getOperator())
        {
        case no_getgraphresult:
        case no_getgraphloopresultset:
        case no_getgraphloopresult:
            if (expr->queryChild(1) == graph)
            {
                independent = false;
                return;
            }
            break;
        }
        NewHqlTransformer::analyseExpr(expr);
    }

    inline bool isIndependent() const { return independent; }

protected:
    LinkedHqlExpr graph;
    bool independent;
};


bool isGraphIndependent(IHqlExpression * expr, IHqlExpression * graph)
{
    switch (expr->getOperator())
    {
    case no_constant:
        return true;
    }
    GraphIndependanceChecker checker(graph);
    checker.analyse(expr, 0);
    return checker.isIndependent();
}

///--------------------------------------------------------------------------------------------------------------------

IHqlExpression * createCounterAsGraphResult(IHqlExpression * counter, IHqlExpression * represents, unsigned seq)
{
    OwnedHqlExpr value = createScalarFromGraphResult(counter->queryType(), unsignedType, represents, seq);
    OwnedHqlExpr internalAttr = createAttribute(internalAtom);
    return createAlias(value, internalAttr);
}

ChildGraphExprBuilder::ChildGraphExprBuilder(unsigned _numInputs)
: numInputs(_numInputs)
{
    numOutputs=0;
    represents.setown(createAttribute(graphAtom, createUniqueId()));
    resultsExpr.setown(createAttribute(resultsAtom, LINK(represents)));
}

IHqlExpression * ChildGraphExprBuilder::addDataset(IHqlExpression * expr)
{
    OwnedHqlExpr resultNumExpr;
    ForEachItemIn(i, results)
    {
        IHqlExpression & curSetResult = results.item(i);
        if (expr->queryBody() == curSetResult.queryChild(0)->queryBody())
        {
            resultNumExpr.set(curSetResult.queryChild(2));
            break;
        }
    }

    if (!resultNumExpr)
    {
        resultNumExpr.setown(getSizetConstant(numResults()));
        results.append(*createValue(no_setgraphresult, makeVoidType(), LINK(expr), LINK(represents), LINK(resultNumExpr)));
        numOutputs++;
    }

    HqlExprArray args;
    args.append(*LINK(expr->queryRecord()));
    args.append(*LINK(represents));
    args.append(*LINK(resultNumExpr));
    if (isGrouped(expr))
        args.append(*createAttribute(groupedAtom));
    if (!expr->isDataset())
        args.append(*createAttribute(rowAtom));
    args.append(*createAttribute(externalAtom, LINK(resultsExpr)));
    args.append(*createAttribute(_original_Atom, LINK(expr)));
    IHqlExpression * recordCountAttr = queryRecordCountInfo(expr);
    if (recordCountAttr)
        args.append(*LINK(recordCountAttr));
    OwnedHqlExpr ret = expr->isDictionary() ? createDictionary(no_getgraphresult, args) : createDataset(no_getgraphresult, args);
    if (expr->isDatarow())
        ret.setown(createRow(no_selectnth, LINK(ret), createComma(getSizetConstant(1), createAttribute(noBoundCheckAtom))));
    return ret.getClear();
}

void ChildGraphExprBuilder::addAction(IHqlExpression * expr)
{
    results.append(*LINK(expr));
}

unsigned ChildGraphExprBuilder::addInput()
{
    unsigned id = numResults();
    numInputs++;
    return id;
}

IHqlExpression * ChildGraphExprBuilder::getGraph(node_operator listOp)
{
    HqlExprArray args;
    args.append(*LINK(represents));
    args.append(*getSizetConstant(numResults()));
    args.append(*createActionList(listOp, results));
    return createValue(no_childquery, makeVoidType(), args);
}

//---------------------------------------------------------------------------
// Child dataset processing

ChildGraphBuilder::ChildGraphBuilder(HqlCppTranslator & _translator, IHqlExpression * subgraph)
: translator(_translator), childQuery(subgraph)
{
    represents.set(subgraph->queryChild(0));
    id = translator.nextActivityId();

    appendUniqueId(instanceName.append("child"), id);
    instanceExpr.setown(createQuoted(instanceName, makeBoolType()));
    resultsExpr.setown(createAttribute(resultsAtom, LINK(represents)));

    StringBuffer s;
    resultInstanceExpr.setown(createQuoted(appendUniqueId(s.append("res"), id), makeBoolType()));
    numResults = (unsigned)getIntValue(subgraph->queryChild(1));
}

void ChildGraphBuilder::generateGraph(BuildCtx & ctx)
{
    if (translator.queryOptions().showChildCountInGraph)
    {
        ActivityInstance * activeActivity = translator.queryCurrentActivity(ctx);
        if (activeActivity)
            activeActivity->noteChildQuery();
    }
    BuildCtx graphctx(ctx);

    //Make sure at least one results - because currently that's how we determine if new resourcing is being used
    //Remove this line once all engines use the new child queries exclusively
    if (numResults == 0) numResults++;

    OwnedHqlExpr resourced = translator.getResourcedChildGraph(graphctx, childQuery, numResults, no_none);

    Owned<ParentExtract> extractBuilder = translator.createExtractBuilder(graphctx, PETchild, represents, resourced, true);
    if (!translator.queryOptions().serializeRowsetInExtract)
        extractBuilder->setAllowDestructor();
    translator.beginExtract(graphctx, extractBuilder);

    translator.doBuildThorSubGraph(graphctx, resourced, SubGraphChild, id, represents);

    EvalContext * instance = translator.queryEvalContext(graphctx);
    OwnedHqlExpr retInstanceExpr;
    if (instance && !translator.insideOnCreate(graphctx))
        retInstanceExpr.setown(instance->createGraphLookup(id, true));
    else
        retInstanceExpr.setown(translator.doCreateGraphLookup(graphctx, graphctx, id, "this", true));
    assertex(retInstanceExpr == instanceExpr);

    CHqlBoundExpr boundExtract;
    extractBuilder->endCreateExtract(boundExtract);

    HqlExprArray args;
    args.append(*LINK(instanceExpr));
    args.append(*createTranslated(boundExtract.length));
    args.append(*boundExtract.getTranslatedExpr());

    OwnedHqlExpr call = translator.bindFunctionCall(evaluateChildQueryInstanceId, args);
    CHqlBoundExpr bound;
    translator.buildExpr(graphctx, call, bound);
    StringBuffer s;
    s.append("Owned<IEclGraphResults> ");
    translator.generateExprCpp(s, resultInstanceExpr);
    s.append(" = ");
    translator.generateExprCpp(s, bound.expr);
    s.append(";");
    graphctx.addQuoted(s);

    translator.endExtract(graphctx, extractBuilder);
    ctx.associateExpr(resultsExpr, resultInstanceExpr);
}

void ChildGraphBuilder::generatePrefetchGraph(BuildCtx & _ctx, OwnedHqlExpr * retGraphExpr)
{
    BuildCtx ctx(_ctx);
    ctx.addGroup();

    BuildCtx aliasctx(ctx);
    aliasctx.addGroup();

    OwnedHqlExpr resourced = translator.getResourcedChildGraph(ctx, childQuery, numResults, no_none);

    Owned<ParentExtract> extractBuilder = translator.createExtractBuilder(ctx, PETchild, represents, resourced, false);
    createBuilderAlias(aliasctx, extractBuilder);
    translator.beginExtract(ctx, extractBuilder);

    translator.doBuildThorSubGraph(ctx, resourced, SubGraphChild, id, represents);

    EvalContext * instance = translator.queryEvalContext(ctx);
    OwnedHqlExpr retInstanceExpr;
    assertex(instance && !translator.insideOnCreate(ctx));
    retInstanceExpr.setown(instance->createGraphLookup(id, true));
    assertex(retInstanceExpr == instanceExpr);

    retGraphExpr->setown(retInstanceExpr.getClear());
}

void ChildGraphBuilder::createBuilderAlias(BuildCtx & ctx, ParentExtract * extractBuilder)
{
    StringBuffer s;
    s.append("rtlRowBuilder & ");
    translator.generateExprCpp(s, extractBuilder->queryExtractName());
    s.append(" = builder;");
    ctx.addQuoted(s);
}

unique_id_t ChildGraphBuilder::buildLoopBody(BuildCtx & ctx, bool multiInstance)
{
    BuildCtx subctx(ctx);
    subctx.addGroup();

    OwnedHqlExpr resourced = translator.getResourcedChildGraph(ctx, childQuery, numResults, no_loop);
    //Add a flag to indicate multi instance
    if (multiInstance)
        resourced.setown(appendOwnedOperand(resourced, createAttribute(multiInstanceAtom)));

    bool isGlobalThorLoop = translator.targetThor() && !translator.insideChildQuery(ctx);
    Owned<ParentExtract> extractBuilder = isGlobalThorLoop ? translator.createExtractBuilder(ctx, PETloop, represents, GraphRemote, false)
                                                           : translator.createExtractBuilder(ctx, PETloop, represents, resourced, false);

    createBuilderAlias(subctx, extractBuilder);

    translator.beginExtract(ctx, extractBuilder);
    translator.doBuildThorSubGraph(ctx, resourced, SubGraphLoop, id, represents);
    translator.endExtract(ctx, extractBuilder);

    return id;
}



static HqlTransformerInfo graphLoopReplacerInfo("GraphLoopReplacer");
class GraphLoopReplacer : public NewHqlTransformer
{
public:
    GraphLoopReplacer(IHqlExpression * _rowsid, IHqlExpression * _represents, IHqlExpression * _counter, bool _isParallel) :
        NewHqlTransformer(graphLoopReplacerInfo), rowsid(_rowsid), represents(_represents), counter(_counter), isParallel(_isParallel)
    {
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_counter:
            if (expr->queryBody() == counter)
            {
                if (isParallel)
                {
                    HqlExprArray args;
                    args.append(*LINK(represents));
//                  unwindChildren(args, expr);
                    OwnedHqlExpr ret = createValue(no_loopcounter, expr->getType(), args);
                    //Yuk:  Wrap this in an alias to ensure it is evaluated at the correct place.
                    //there has to be a better way.....  We could...
                    //a) strictly defined when it can be evaluated - e.g., ctx->defines(graph) && (!parentctx || !parentctx->definesGraph)
                    //b) set a flag in the expression to indicate forced evaluation (even worse than the alias)
                    //c) add the code to evaluate no_loopcounter inside evaluateInContext
                    return createAlias(ret, internalAttrExpr);
                }
                else
                {
                    counterResult.setown(createCounterAsGraphResult(counter, represents, 0));
                    return LINK(counterResult);
                }
            }
            break;
        case no_rowsetindex:
            {
                IHqlExpression * rowset = expr->queryChild(0);
                if (rowset->getOperator() != no_rowset)
                    break;
                IHqlExpression * rows = rowset->queryChild(0);
                if (rows->queryChild(1) != rowsid)
                    break;
                HqlExprArray args;
                args.append(*LINK(rows->queryChild(0)->queryRecord()));
                args.append(*LINK(represents));
                args.append(*transform(expr->queryChild(1)));
                return createDataset(no_getgraphloopresult, args);
            }
        case no_rowset:
            {
                IHqlExpression * rows = expr->queryChild(0);
                if (rows->queryChild(1) != rowsid)
                    break;
                HqlExprArray args;
                args.append(*LINK(rows->queryChild(0)->queryRecord()));
                args.append(*LINK(represents));
                return createValue(no_getgraphloopresultset, expr->getType(), args);
            }
        }
        return NewHqlTransformer::createTransformed(expr);
    }

    inline IHqlExpression * queryCounterResult() { return counterResult; }

protected:
    IHqlExpression * rowsid;
    IHqlExpression * represents;
    IHqlExpression * counter;
    OwnedHqlExpr counterResult;
    bool isParallel;
};



unique_id_t ChildGraphBuilder::buildGraphLoopBody(BuildCtx & ctx, bool isParallel)
{
    BuildCtx subctx(ctx);
    subctx.addGroup();

    IHqlExpression * query = childQuery->queryChild(2);
    translator.traceExpression("Before Loop resource", query);
    OwnedHqlExpr resourced = translator.getResourcedChildGraph(ctx, childQuery, numResults, no_loop);
    translator.traceExpression("After Loop resource", resourced);

    //Add a flag to indicate multi instance
    if (isParallel)
    {
        HqlExprArray args;
        unwindChildren(args, resourced);
        args.append(*createAttribute(multiInstanceAtom));
        args.append(*createAttribute(delayedAtom));
        resourced.setown(resourced->clone(args));
    }

    bool isGlobalThorLoop = translator.targetThor() && !translator.insideChildQuery(ctx);
    Owned<ParentExtract> extractBuilder = isGlobalThorLoop ? translator.createExtractBuilder(ctx, PETloop, represents, GraphRemote, false)
                                                           : translator.createExtractBuilder(ctx, PETloop, represents, resourced, false);

    createBuilderAlias(subctx, extractBuilder);

    translator.beginExtract(ctx, extractBuilder);
    translator.doBuildThorSubGraph(ctx, resourced, SubGraphLoop, id, represents);
    translator.endExtract(ctx, extractBuilder);

    return id;
}

unique_id_t ChildGraphBuilder::buildRemoteGraph(BuildCtx & ctx)
{
    BuildCtx subctx(ctx);
    subctx.addGroup();

    OwnedHqlExpr resourced = translator.getResourcedChildGraph(ctx, childQuery, numResults, no_allnodes);

    Owned<ParentExtract> extractBuilder = translator.createExtractBuilder(ctx, PETremote, represents, GraphRemote, false);

    createBuilderAlias(subctx, extractBuilder);

    translator.beginExtract(ctx, extractBuilder);
    translator.doBuildThorSubGraph(ctx, resourced, SubGraphChild, id, represents);
    translator.endExtract(ctx, extractBuilder);

    return id;
}


void HqlCppTranslator::buildChildGraph(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * represents= expr->queryChild(0);
    OwnedHqlExpr resultsExpr = createAttribute(resultsAtom, LINK(represents));
    //Shouldn't really happen, but if this graph has already benn called just use the results
    if (ctx.queryMatchExpr(resultsExpr))
        return;

    ChildGraphBuilder graphBuilder(*this, expr);
    graphBuilder.generateGraph(ctx);
}

void HqlCppTranslator::beginExtract(BuildCtx & ctx, ParentExtract * extractBuilder)
{
    ctx.associate(*extractBuilder);
}

void HqlCppTranslator::endExtract(BuildCtx & ctx, ParentExtract * extractBuilder)
{ 
    extractBuilder->endUseExtract(ctx);
    ctx.removeAssociation(extractBuilder);
}

void HqlCppTranslator::buildAssignChildDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_call:
    case no_externalcall:
    case no_libraryinput:
        buildDatasetAssign(ctx, target, expr);
        return;
    }

    OwnedHqlExpr call;
    {
        ChildGraphExprBuilder builder(0);
        call.setown(builder.addDataset(expr));

        OwnedHqlExpr subquery = builder.getGraph();
        buildStmt(ctx, subquery);
    }

    buildExprAssign(ctx, target, call);
}


IHqlExpression * HqlCppTranslator::getResourcedChildGraph(BuildCtx & ctx, IHqlExpression * childQuery, unsigned numResults, node_operator graphKind)
{
    if (options.paranoidCheckNormalized || options.paranoidCheckDependencies)
        DBGLOG("Before resourcing a child graph");

    IHqlExpression * graphIdExpr = childQuery->queryChild(0);
    IHqlExpression * originalQuery = childQuery->queryChild(2);
    LinkedHqlExpr resourced = originalQuery;
    checkNormalized(ctx, resourced);

    unsigned csfFlags = CSFindex|options.optimizeDiskFlag;
    switch (targetClusterType)
    {
    case HThorCluster:
        csfFlags |= CSFcompoundSpill;
        break;
    case ThorLCRCluster:
        //Don't compound spills inside a child query because it can cause non remote projects to become remote
        //And we'll also probably be using the roxie code to implement
        break;
    case RoxieCluster:
        break;
    }

    {
        cycle_t startCycles = get_cycles_now();
        CompoundSourceTransformer transformer(*this, CSFpreload|csfFlags);
        resourced.setown(transformer.process(resourced));
        checkNormalized(ctx, resourced);
        noteFinishedTiming("workunit;tree transform: optimize disk read", startCycles);
    }

    if (options.optimizeGraph)
    {
        cycle_t startCycles = get_cycles_now();
        traceExpression("BeforeOptimizeSub", resourced);
        resourced.setown(optimizeHqlExpression(queryErrorProcessor(), resourced, getOptimizeFlags()|HOOcompoundproject));
        traceExpression("AfterOptimizeSub", resourced);
        noteFinishedTiming("workunit;optimize graph", startCycles);
    }

    traceExpression("BeforeResourcing Child", resourced);

    cycle_t startCycles = get_cycles_now();
    HqlExprCopyArray activeRows;
    gatherActiveCursors(ctx, activeRows);
    if (graphKind == no_loop)
    {
        bool insideChild = insideChildQuery(ctx);
        resourced.setown(resourceLoopGraph(*this, activeRows, resourced, targetClusterType, graphIdExpr, numResults, insideChild));
    }
    else
        resourced.setown(resourceNewChildGraph(*this, activeRows, resourced, targetClusterType, graphIdExpr, numResults));

    noteFinishedTiming("workunit;resource graph", startCycles);
    checkNormalized(ctx, resourced);
    traceExpression("AfterResourcing Child", resourced);
    
    resourced.setown(optimizeGraphPostResource(resourced, csfFlags, false));
    if (options.optimizeSpillProject)
    {
        resourced.setown(convertSpillsToActivities(resourced, true));
        resourced.setown(optimizeGraphPostResource(resourced, csfFlags, false));
    }

    if (options.paranoidCheckNormalized || options.paranoidCheckDependencies)
        DBGLOG("After resourcing a child graph");

    return resourced.getClear();
}


void HqlCppTranslator::buildChildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (expr->isPure() && ctx.getMatchExpr(expr, tgt))
        return;

    LoopInvariantHelper helper;
    BuildCtx bestctx(ctx);
    if (options.optimizeLoopInvariant)
        helper.getBestContext(bestctx, expr);

    CHqlBoundTarget temp;
    //MORE: Should have similar code to buildTempExpr()
    createTempFor(bestctx, expr, temp);
    buildAssignChildDataset(bestctx, temp, expr);
    tgt.setFromTarget(temp);

    if (expr->isPure())
        bestctx.associateExpr(expr, tgt);
}


unique_id_t HqlCppTranslator::buildGraphLoopSubgraph(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * body, IHqlExpression * counter, bool multiInstance)
{
    ChildGraphExprBuilder graphBuilder(0);

    OwnedHqlExpr transformedBody;
    OwnedHqlExpr counterResult;
    IHqlExpression * graphid = graphBuilder.queryRepresents();
    {
        const bool isParallel = multiInstance;
        GraphLoopReplacer replacer(rowsid, graphid, counter, isParallel);
        transformedBody.setown(replacer.transformRoot(body));
        counterResult.set(replacer.queryCounterResult());
    }

    if (counterResult)
        graphBuilder.addInput();

    OwnedHqlExpr result = createValue(no_setgraphloopresult, makeVoidType(), LINK(transformedBody), LINK(graphid));
    graphBuilder.addAction(result);
    OwnedHqlExpr subquery = graphBuilder.getGraph();

    ChildGraphBuilder builder(*this, subquery);
    return builder.buildGraphLoopBody(ctx, multiInstance);
}

unique_id_t HqlCppTranslator::buildRemoteSubgraph(BuildCtx & ctx, IHqlExpression * dataset)
{
    ChildGraphExprBuilder graphBuilder(0);
    if (dataset->isAction())
    {
        graphBuilder.addAction(dataset);
    }
    else
    {
        OwnedHqlExpr ignoredResult = graphBuilder.addDataset(dataset);
    }

    OwnedHqlExpr subquery = graphBuilder.getGraph();
    ChildGraphBuilder builder(*this, subquery);
    return builder.buildRemoteGraph(ctx);
}

//---------------------------------------------------------------------------
// Functions to check whether a dataset can be evaluated inline or not.

//MORE: These should probably be split out into an hqlinline.cpp

bool HqlCppTranslator::canIterateInline(BuildCtx * ctx, IHqlExpression * expr)
{
    return (isInlineOk() && ::canIterateInline(ctx, expr));
}

bool HqlCppTranslator::canAssignInline(BuildCtx * ctx, IHqlExpression * expr)
{
    if (!isInlineOk())
        return false;
    return options.allowInlineSpill ? ::canProcessInline(ctx, expr) : ::canAssignInline(ctx, expr);
}

bool HqlCppTranslator::canEvaluateInline(BuildCtx * ctx, IHqlExpression * expr)
{
    if (!isInlineOk())
        return false;
    return options.allowInlineSpill ? ::canProcessInline(ctx, expr) : ::canEvaluateInline(ctx, expr);
}

bool HqlCppTranslator::canProcessInline(BuildCtx * ctx, IHqlExpression * expr)
{
    if (!isInlineOk())
        return false;
    return ::canProcessInline(ctx, expr);
}

bool HqlCppTranslator::isInlineOk()
{
    if (!activeGraphCtx)
        return true;
    return true;
}

IHqlExpression * HqlCppTranslator::buildSpillChildDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    CHqlBoundExpr bound;
    buildChildDataset(ctx, expr, bound);
    return bound.getTranslatedExpr();
}

IHqlExpression * HqlCppTranslator::forceInlineAssignDataset(BuildCtx & ctx, IHqlExpression * expr)
{
    loop
    {
        CHqlBoundExpr bound;
        if (expr->isPure() && ctx.getMatchExpr(expr, bound))
            return bound.getTranslatedExpr();

        if (canProcessInline(&ctx, expr) || (expr->getOperator() == no_translated))
            return LINK(expr);

        switch (expr->getOperator())
        {
        case no_compound:
            buildStmt(ctx, expr->queryChild(0));
            expr = expr->queryChild(1);
            break;
        default:
            return buildSpillChildDataset(ctx, expr);
        }
    }
}

//---------------------------------------------------------------------------
// Dataset temp creation

IHqlExpression * createGetResultFromWorkunitDataset(IHqlExpression * expr)
{
    IHqlExpression * name = queryAttributeChild(expr, nameAtom, 0);
    if (name)
        name = createExprAttribute(namedAtom, LINK(name));
    assertex(expr->isDataset());
    return createDataset(no_getresult, LINK(expr->queryRecord()), createComma(LINK(expr->queryAttribute(sequenceAtom)), name));
}

void HqlCppTranslator::buildAssignSerializedDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IAtom * serializeForm)
{
    OwnedITypeInfo serializedType = getSerializedForm(expr->queryType(), serializeForm);
    assertex(recordTypesMatch(target.queryType(), serializedType));

    HqlExprArray args;
    args.append(*createSerializer(ctx, expr->queryRecord(), serializeForm, serializerAtom));
    args.append(*LINK(expr));

    IIdAtom * func;
    if (target.expr->isDictionary())
    {
        assertex(serializeForm == internalAtom);
        func = rtlSerializeDictionaryId;
    }
    else if (expr->isDictionary())
    {
        assertex(serializeForm == diskAtom);
        func = rtlSerializeDictionaryToDatasetId;
    }
    else
    {
        if (isGrouped(expr))
            func = groupedRowset2DatasetXId;
        else
            func = rowset2DatasetXId;
    }


    OwnedHqlExpr call = bindFunctionCall(func, args, serializedType);
    buildExprAssign(ctx, target, call);
}

void HqlCppTranslator::buildSerializedDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, IAtom * serializeForm)
{
    CHqlBoundTarget target;
    OwnedITypeInfo serializedType = getSerializedForm(expr->queryType(), serializeForm);
    createTempFor(ctx, serializedType, target, typemod_none, FormatBlockedDataset);
    buildAssignSerializedDataset(ctx, target, expr, serializeForm);
    tgt.setFromTarget(target);
}


void HqlCppTranslator::buildAssignDeserializedDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IAtom * serializeForm)
{
    OwnedITypeInfo serializedType = getSerializedForm(target.queryType(), serializeForm);
    assertex(recordTypesMatch(serializedType, expr->queryType()));

    IIdAtom * func;
    IHqlExpression * record = ::queryRecord(target.queryType());
    HqlExprArray args;
    args.append(*createSerializer(ctx, record, serializeForm, deserializerAtom));
    if (target.expr->isDictionary())
    {
        if (serializeForm == internalAtom)
        {
            assertex(expr->isDictionary());
            func = rtlDeserializeDictionaryId;
        }
        else if (serializeForm == diskAtom)
        {
            assertex(expr->isDataset());
            func = rtlDeserializeDictionaryFromDatasetId;
            StringBuffer lookupHelperName;
            buildDictionaryHashClass(record, lookupHelperName);
            args.append(*createQuoted(lookupHelperName.str(), makeBoolType()));
        }
        else
            throwUnexpected();
    }
    else
    {
        if (isGrouped(expr))
            func = groupedDataset2RowsetXId;
        else
            func = dataset2RowsetXId;
    }

    args.append(*LINK(expr));
    OwnedHqlExpr call = bindFunctionCall(func, args, target.queryType());
    buildExprAssign(ctx, target, call);
}

void HqlCppTranslator::buildDeserializedDataset(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * expr, CHqlBoundExpr & tgt, IAtom * serializeForm)
{
#ifdef _DEBUG
    OwnedITypeInfo serializedType = getSerializedForm(type, serializeForm);
    assertex(recordTypesMatch(expr->queryType(), serializedType));
#endif

    ITypeInfo * const exprType = expr->queryType();
    assertex(!hasLinkedRow(exprType));

    CHqlBoundTarget target;
    createTempFor(ctx, type, target, typemod_none, FormatLinkedDataset);

    buildAssignDeserializedDataset(ctx, target, expr, serializeForm);

    tgt.setFromTarget(target);
}


void HqlCppTranslator::ensureDatasetFormat(BuildCtx & ctx, ITypeInfo * type, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    IAtom * serializeForm = internalAtom; // The format of serialized expressions in memory must match the internal serialization format
    ITypeInfo * tgtType = tgt.queryType();
    switch (format)
    {
    case FormatStreamedDataset:
        if (!hasStreamedModifier(tgtType))
        {
            ensureDatasetFormat(ctx, type, tgt, FormatLinkedDataset);
            HqlExprArray args;
            args.append(*tgt.getTranslatedExpr());
            OwnedITypeInfo streamedType = setStreamedAttr(type, true);
            OwnedHqlExpr call = bindFunctionCall(createRowStreamId, args, streamedType);
            buildTempExpr(ctx, call, tgt);
            return;
        }
        break;
    case FormatBlockedDataset:
        if (isArrayRowset(tgtType))
        {
            OwnedHqlExpr deserializedExpr = tgt.getTranslatedExpr();
            LinkedHqlExpr savedCount = tgt.count;
            assertex(!deserializedExpr->isDictionary());
            buildSerializedDataset(ctx, deserializedExpr, tgt, serializeForm);
            if (savedCount && !isFixedWidthDataset(deserializedExpr))
                tgt.count.set(savedCount);
            return;
        }
        break;
    case FormatLinkedDataset:
        if (!hasLinkCountedModifier(tgtType))
        {
            OwnedHqlExpr serializedExpr = tgt.getTranslatedExpr();
            if (recordTypesMatch(type, tgtType))
            {
                //source is an array of rows, or a simple dataset that doesn't need any transformation
                buildTempExpr(ctx, serializedExpr, tgt, FormatLinkedDataset);
            }
            else
                buildDeserializedDataset(ctx, type, serializedExpr, tgt, serializeForm);
            return;
        }
        break;
    case FormatArrayDataset:
        if (!isArrayRowset(tgtType))
        {
            OwnedHqlExpr serializedExpr = tgt.getTranslatedExpr();
            buildDeserializedDataset(ctx, type, serializedExpr, tgt, serializeForm);
            return;
        }
        break;
    }
}

void HqlCppTranslator::buildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    doBuildDataset(ctx, expr, tgt, format);
    ensureDatasetFormat(ctx, expr->queryType(), tgt, format);
}


void HqlCppTranslator::doBuildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    if (expr->isPure() && ctx.getMatchExpr(expr, tgt))
        return;

/*
    OwnedHqlExpr transformed = normalizeAnyDatasetAliases(expr);
    if (transformed && (transformed != expr))
    {
        doBuildDataset(ctx, transformed, tgt, format);
        ctx.associateExpr(expr, tgt);
        return;
    }
*/

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_dataset_alias:
        if (!expr->hasAttribute(_normalized_Atom))
        {
            OwnedHqlExpr uniqueChild = normalizeDatasetAlias(expr);
            doBuildDataset(ctx, uniqueChild, tgt, format);
        }
        else
            doBuildDataset(ctx, expr->queryChild(0), tgt, format);
        return;
    case no_alias:
        doBuildExprAlias(ctx, expr, &tgt, NULL);
        return;
    case no_owned_ds:
        buildTempExpr(ctx, expr, tgt);
        return;
    case no_fail:
        doBuildStmtFail(ctx, expr->queryChild(1));
        //fallthrough
    case no_null:
        doBuildDatasetNull(expr, tgt, format);
        return;
    case no_translated:
        expandTranslated(expr, tgt);
        return;
    case no_select:
        {
            if (isMultiLevelDatasetSelector(expr, false))
                break;

            Owned<IReferenceSelector> selected = buildReference(ctx, expr);
            selected->get(ctx, tgt);
            return;
        }
    case no_libraryinput:
        if (!buildExprInCorrectContext(ctx, expr, tgt, false))
            throwUnexpected();
        return;
    case no_call:
    case no_externalcall:
        buildTempExpr(ctx, expr, tgt);
        return;
    case no_newaggregate:
        if (canAssignInline(&ctx, expr))
        {
            Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);

            Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);
            Owned<IReferenceSelector> createdRef = createReferenceSelector(rowBuilder);

            BuildCtx subctx(ctx);
            subctx.addGroup();
            doBuildRowAssignAggregate(subctx, createdRef, expr);
            finalizeTempRow(ctx, tempRow, rowBuilder);

            convertBoundRowToDataset(ctx, tgt, tempRow, format);
            return;
        }
        break;
    case no_id2blob:
        doBuildExprIdToBlob(ctx, expr, tgt);
        return;
    case no_rows:
        {
            if (!buildExprInCorrectContext(ctx, expr, tgt, false))
                throwError(HQLERR_RowsUsedOutsideContext);
            return;
        }
    case no_limit:
        if (expr->hasAttribute(skipAtom) || expr->hasAttribute(onFailAtom))
            break;
        doBuildDatasetLimit(ctx, expr, tgt, format);
        return;
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_distributed:
    case no_preservemeta:
    case no_sorted:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_section:
    case no_sectioninput:
        buildDataset(ctx, expr->queryChild(0), tgt, format);
        return;
    case no_forcegraph:
#ifdef _DEBUG
        throwUnexpected();
#endif
        buildDataset(ctx, expr->queryChild(0), tgt, format);
        return;
    case no_getgraphresult:
        doBuildExprGetGraphResult(ctx, expr, tgt, format);
        return;
    case no_getresult:
    case no_workunit_dataset:
        doBuildExprGetResult(ctx, expr, tgt);
        return;
    case no_skip:
        {
            buildStmt(ctx, expr);
            OwnedHqlExpr null = createNullExpr(expr);
            buildDataset(ctx, null, tgt, format);
            return;
        }
    case no_serialize:
        {
            IHqlExpression * deserialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(1)->queryName();
            if (isDummySerializeDeserialize(expr))
                doBuildDataset(ctx, deserialized->queryChild(0), tgt, format);
            else if (!typeRequiresDeserialization(deserialized->queryType(), serializeForm))
                //Optimize creating a serialized version of a dataset if the record is the same serialized and unserialized
                buildDataset(ctx, deserialized, tgt, FormatNatural);
            else
                buildSerializedDataset(ctx, deserialized, tgt, serializeForm);
            return;
        }
    case no_deserialize:
        {
            IHqlExpression * serialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(2)->queryName();
            if (isDummySerializeDeserialize(expr))
                doBuildDataset(ctx, serialized->queryChild(0), tgt, format);
            else if (!typeRequiresDeserialization(expr->queryType(), serializeForm))
                //Optimize creating a deserialized version of a dataset if the record is the same serialized and unserialized
                buildDataset(ctx, serialized, tgt, FormatNatural);
            else
                buildDeserializedDataset(ctx, expr->queryType(), serialized, tgt, serializeForm);
            return;
        }
    case no_datasetfromrow:
        {
            IHqlExpression * row = expr->queryChild(0);
            if (isAlwaysActiveRow(row) && (format == FormatNatural))
            {
                Owned<IReferenceSelector> selector = buildActiveRow(ctx, row);
                BuildCtx groupctx(ctx);
                groupctx.addGroup();
                BoundRow * bound = bindSelectorAsRootRow(groupctx, selector, row);
                convertBoundRowToDataset(groupctx, tgt, bound, format);
                tgt.count.setown(getSizetConstant(1));
                ctx.associateExpr(expr, tgt);
                return;
            }
            break;
        }
    case no_inlinetable:
        if (doBuildConstantDatasetInlineTable(expr, tgt, format))
            return;
        break;
    case no_compound:
        {
            buildStmt(ctx, expr->queryChild(0));
            buildDataset(ctx, expr->queryChild(1), tgt, format);
            return;
        }
    case no_createdictionary:
        {
            if (isConstantDictionary(expr))
            {
                if (doBuildDictionaryInlineTable(ctx, expr, tgt, format))
                    return;
            }

            IHqlExpression * record = expr->queryRecord();
            IHqlExpression * dataset = expr->queryChild(0);
            Owned<IHqlCppDatasetBuilder> builder = createLinkedDictionaryBuilder(record);

            builder->buildDeclare(ctx);

            buildDatasetAssign(ctx, builder, dataset);

            builder->buildFinish(ctx, tgt);
            ctx.associateExpr(expr, tgt);
            return;
        }
    case no_if:
        if (::canEvaluateInline(&ctx, expr->queryChild(1)) && ::canEvaluateInline(&ctx, expr->queryChild(2)))
        {
            buildTempExpr(ctx, expr, tgt, format);
            return;
        }
    }

    if (expr->isDictionary())
    {
        buildTempExpr(ctx, expr, tgt, format);
        return;
    }

    bool singleRow = hasSingleRow(expr);
    bool useTempRow = singleRow && canAssignInline(&ctx, expr) && (format != FormatLinkedDataset) && (format != FormatArrayDataset);
    //Conditional row assignment if variable length causes offset to be recalculated outside of the if()
    //if (useTempRow && (op == no_if) && isVariableSizeRecord(expr->queryRecord()))
    //  useTempRow = false;

    if (useTempRow)
    {
        Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);

        Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);
        Owned<IHqlCppDatasetBuilder> builder = createSingleRowTempDatasetBuilder(expr->queryRecord(), rowBuilder);
        builder->buildDeclare(ctx);

        buildDatasetAssign(ctx, builder, expr);

        //builder->buildFinish(ctx, tempTarget);
        finalizeTempRow(ctx, tempRow, rowBuilder);

        convertBoundRowToDataset(ctx, tgt, tempRow, format);
    }
    else
    {
        if (!canAssignInline(&ctx, expr))
        {
            CHqlBoundTarget tempTarget;
            createTempFor(ctx, expr->queryType(), tempTarget, typemod_none, format);
            buildDatasetAssign(ctx, tempTarget, expr);
            tgt.setFromTarget(tempTarget);
            //buildTempExpr(ctx, expr, tgt);            // can't use this because it causes recursion on no_selectnth
        }
        else
        {
            Owned<IHqlCppDatasetBuilder> builder;

            IHqlExpression * record = expr->queryRecord();
            IAtom * serializeForm = internalAtom; // The format of serialized expressions in memory must match the internal serialization format
            OwnedHqlExpr serializedRecord = getSerializedForm(record, serializeForm);
            if (format == FormatNatural)
            {
                if (record != serializedRecord)
                    ensureContextAvailable(ctx);
                if (!ctx.queryMatchExpr(codeContextMarkerExpr))
                {
                    if (record != serializedRecord)
                        throwError(HQLERR_LinkedDatasetNoContext);
                    format = FormatBlockedDataset;
                }
                else
                {
                    format = FormatLinkedDataset;
                }
            }
            else if (record != serializedRecord)
                format = FormatLinkedDataset;   // Have to serialize it later - otherwise it won't be compatible

            if (format == FormatLinkedDataset || format == FormatArrayDataset)
            {
                IHqlExpression * choosenLimit = NULL;
                if ((op == no_choosen) && !isChooseNAllLimit(expr->queryChild(1)) && !queryRealChild(expr, 2))
                {
                    choosenLimit = expr->queryChild(1);
                    expr = expr->queryChild(0);
                }

                //MORE: Extract limit and choosen and pass as parameters
                builder.setown(createLinkedDatasetBuilder(record, choosenLimit));
            }
            else if ((op == no_choosen) && !isChooseNAllLimit(expr->queryChild(1)) && !queryRealChild(expr, 2))
            {
                //Build a limited builder - it is likely to be just as efficient, and often much more e.g., choosen(a+b, n)
                builder.setown(createChoosenDatasetBuilder(serializedRecord, expr->queryChild(1)));
                expr = expr->queryChild(0);
            }
            else
                builder.setown(createBlockedDatasetBuilder(serializedRecord));

            builder->buildDeclare(ctx);

            buildDatasetAssign(ctx, builder, expr);

            builder->buildFinish(ctx, tgt);
        }
    }
    if (singleRow)
        tgt.count.setown(getSizetConstant(1));
    else if (op == no_inlinetable)
    {
        IHqlExpression * transforms = expr->queryChild(0);
        if (!transformListContainsSkip(transforms))
            tgt.count.setown(getSizetConstant(transforms->numChildren()));
    }

    ctx.associateExpr(expr, tgt);
}

//---------------------------------------------------------------------------
// Dataset assignment - to temp

static bool isWorthAssigningDirectly(BuildCtx & ctx, const CHqlBoundTarget & /*target*/, IHqlExpression * expr)
{
    //target parameter is currently unused - it should be used to check that linkcounted attributes match etc.
    if (expr->getOperator() == no_null)
        return false;
    //A poor approximation.  Could also include function calls if the is-link-counted matches.
    return ::canEvaluateInline(&ctx, expr);
}

void HqlCppTranslator::buildDatasetAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_fail:
        doBuildStmtFail(ctx, expr->queryChild(1));
        return;
    case no_call:
    case no_externalcall:
        doBuildCall(ctx, &target, expr, NULL);
        return;
    case no_getgraphresult:
        doBuildAssignGetGraphResult(ctx, target, expr);
        return;
    case no_workunit_dataset:
    case no_getresult:
        buildExprAssign(ctx, target, expr);
        return;
    case no_null:
        {
            CHqlBoundExpr bound;
            buildDataset(ctx, expr, bound, isArrayRowset(target.queryType()) ? FormatLinkedDataset : FormatBlockedDataset);
            if (hasWrapperModifier(target.queryType()) && hasLinkCountedModifier(target.queryType()))
            {
                OwnedHqlExpr complex = bound.getComplexExpr();
                ctx.addAssign(target.expr, complex);
            }
            else
            {
                if (target.count) ctx.addAssign(target.count, bound.count);
                if (target.length) ctx.addAssign(target.length, bound.length);
                ctx.addAssign(target.expr, bound.expr);
            }
            return;
        }
    case no_inlinetable:
        {
            //This will typically generate a loop.  If few items then it is more efficient to expand the assigns/clones out.
            if (options.canLinkConstantRows || (expr->queryChild(0)->numChildren() > INLINE_TABLE_EXPAND_LIMIT))
            {
                CHqlBoundExpr bound;
                if (doBuildConstantDatasetInlineTable(expr, bound, FormatNatural))
                {
                    OwnedHqlExpr translated = bound.getTranslatedExpr();
                    buildDatasetAssign(ctx, target, translated);
                    return;
                }
            }
            break;
        }
    case no_alias:
        {
            CHqlBoundExpr bound;
            buildDataset(ctx, expr, bound, FormatNatural);
            OwnedHqlExpr translated = bound.getTranslatedExpr();
            buildDatasetAssign(ctx, target, translated);
            return;
        }
    case no_owned_ds:
        {
            ITypeInfo * targetType = target.queryType();
            if (hasLinkCountedModifier(targetType) && hasWrapperModifier(targetType))
            {
                CHqlBoundExpr bound;
                buildDataset(ctx, expr->queryChild(0), bound, FormatLinkedDataset);
                OwnedHqlExpr compound = createValue(no_complex, bound.expr->getType(), LINK(bound.count), LINK(bound.expr));
                ctx.addAssign(target.expr, compound);
                return;
            }
            break;
        }
    case no_compound:
        {
            buildStmt(ctx, expr->queryChild(0));
            buildDatasetAssign(ctx, target, expr->queryChild(1));
            return;
        }
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_distributed:
    case no_preservemeta:
    case no_sorted:
    case no_nofold:
    case no_nohoist:
    case no_forcegraph:
    case no_nocombine:
    case no_section:
    case no_sectioninput:
        buildDatasetAssign(ctx, target, expr->queryChild(0));
        return;
    case no_if:
        //Only generate conditional assignments to a target if both source and target require no temporary.
        if (expr->isDictionary() || (::canEvaluateInline(&ctx, expr->queryChild(1)) && ::canEvaluateInline(&ctx, expr->queryChild(2))))
        //The following line would be better, but it needs improvements to the cse generation first, otherwise some examples get worse.
        //if (expr->isDictionary() || (isWorthAssigningDirectly(ctx, target, expr->queryChild(1)) || isWorthAssigningDirectly(ctx, target, expr->queryChild(2))))
        {
            buildDatasetAssignIf(ctx, target, expr);
            return;
        }
        break;
    case no_chooseds:
        if (expr->isDictionary())
        {
            buildDatasetAssignChoose(ctx, target, expr);
            return;
        }
        break;
    case no_serialize:
        {
            IHqlExpression * deserialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(1)->queryName();
            if (isDummySerializeDeserialize(expr))
                buildDatasetAssign(ctx, target, deserialized->queryChild(0));
            else if (!typeRequiresDeserialization(deserialized->queryType(), serializeForm))
                buildDatasetAssign(ctx, target, deserialized);
            else
                buildAssignSerializedDataset(ctx, target, deserialized, serializeForm);
            return;
        }
    case no_deserialize:
        {
            IHqlExpression * serialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(2)->queryName();
            if (isDummySerializeDeserialize(expr))
                buildDatasetAssign(ctx, target, serialized->queryChild(0));
            else if (!typeRequiresDeserialization(expr->queryType(), serializeForm))
                buildDatasetAssign(ctx, target, serialized);
            else
                buildAssignDeserializedDataset(ctx, target, serialized, serializeForm);
            return;
        }
    case no_select:
        {
            bool isNew;
            IHqlExpression * ds = querySelectorDataset(expr, isNew);
            if (!isNew || ds->isDatarow())
            {
                Owned<IReferenceSelector> selected = buildReference(ctx, expr);
                selected->assignTo(ctx, target);
                return;
            }
            break;
        }
    case no_typetransfer:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (expr->isDataset() && child->isDataset())
            {
                //Special case no-op type transfers and assignment from a call returning unknown-type dataset
                if (!recordTypesMatch(expr, child) &&
                    !(child->getOperator() == no_externalcall && recordTypesMatch(child, queryNullRecord())))
                {
                    CHqlBoundExpr bound;
                    buildDataset(ctx, child, bound, FormatNatural);
                    ITypeInfo * newType = cloneModifiers(bound.expr->queryType(), expr->queryType());
                    bound.expr.setown(createValue(no_typetransfer, newType, LINK(bound.expr)));
                    OwnedHqlExpr translated = bound.getTranslatedExpr();
                    buildDatasetAssign(ctx, target, translated);
                }
                else
                    buildDatasetAssign(ctx, target, child);
                return;
            }
            break;
        }
    case no_createdictionary:
        {
            if (isConstantDictionary(expr))
            {
                CHqlBoundExpr temp;
                if (doBuildDictionaryInlineTable(ctx, expr, temp, FormatNatural))
                {
                    OwnedHqlExpr translated = temp.getTranslatedExpr();
                    buildDatasetAssign(ctx, target, translated);
                    return;
                }
            }

            IHqlExpression * record = expr->queryRecord();
            Owned<IHqlCppDatasetBuilder> builder = createLinkedDictionaryBuilder(record);
            builder->buildDeclare(ctx);

            buildDatasetAssign(ctx, builder, expr->queryChild(0));

            builder->buildFinish(ctx, target);
            return;
        }
    }

    if (!canAssignInline(&ctx, expr) && (op != no_translated))
    {
        buildAssignChildDataset(ctx, target, expr);
        return;
    }

    ITypeInfo * to = target.queryType();
    ITypeInfo * exprType = expr->queryType();
    bool targetOutOfLine = isArrayRowset(to);
    switch (op)
    {
    case no_limit:
        assertex(!expr->hasAttribute(skipAtom) && !expr->hasAttribute(onFailAtom));
        //Do the limit check as a post test.  
        //It means we may read more records than we need to, but the code is inline, and the code is generally much better.
        if (target.count)
        {
            buildDatasetAssign(ctx, target, expr->queryChild(0));
            CHqlBoundExpr bound;
            bound.setFromTarget(target);
            doBuildCheckDatasetLimit(ctx, expr, bound);
            return;
        }
        break;
    case no_translated:
        {
            bool sourceOutOfLine = isArrayRowset(exprType);
            if (sourceOutOfLine != targetOutOfLine && !hasStreamedModifier(exprType))
            {
                IAtom * serializeFormat = internalAtom; // The format of serialized expressions in memory must match the internal serialization format
                OwnedITypeInfo serializedSourceType = getSerializedForm(exprType, serializeFormat);
                OwnedITypeInfo serializedTargetType = getSerializedForm(to, serializeFormat);
                if (queryUnqualifiedType(serializedSourceType) == queryUnqualifiedType(serializedTargetType))
                {
                    if (targetOutOfLine)
                    {
                        buildAssignDeserializedDataset(ctx, target, expr, serializeFormat);
                    }
                    else
                    {
                        buildAssignSerializedDataset(ctx, target, expr, serializeFormat);
                    }
                    return;
                }
            }
            break;
        }
    }

    if (recordTypesMatch(to, exprType))
    {
        switch (op)
        {
        case no_rows:
            {
                CHqlBoundExpr bound;
                buildDataset(ctx, expr, bound, FormatLinkedDataset);
                OwnedHqlExpr translated = bound.getTranslatedExpr();
                buildDatasetAssign(ctx, target, translated);
                return;
            }
        case no_select:
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(expr, isNew);
                if (isNew && !ds->isDatarow())
                    break;
            }
            //fall through
        case no_translated:
        case no_null:
        case no_id2blob:
            {
                IIdAtom * func = NULL;
                if (!isArrayRowset(to))
                {
                    if (!isArrayRowset(exprType))
                        func = dataset2DatasetXId;
                }
                else if (hasLinkCountedModifier(to))
                {
                    if (hasLinkCountedModifier(exprType))
                    {
                        CHqlBoundExpr bound;
                        buildDataset(ctx, expr, bound, FormatLinkedDataset);
                        assertex(bound.count && bound.expr);

                        if (hasWrapperModifier(to))
                        {
                            //assigns to count and rows members
                            StringBuffer s;
                            generateExprCpp(s, target.expr);
                            s.append(".set(");
                            generateExprCpp(s, bound.count);
                            s.append(",");
                            generateExprCpp(s, bound.expr);
                            s.append(");");
                            ctx.addQuoted(s);
                        }
                        else
                        {
                            ctx.addAssign(target.count, bound.count);
                            HqlExprArray args;
                            args.append(*LINK(bound.expr));
                            OwnedHqlExpr call = bindTranslatedFunctionCall(linkRowsetId, args);
                            ctx.addAssign(target.expr, call);
                        }
                        return;
                    }
                }

                if (func)
                {
                    HqlExprArray args;
                    args.append(*LINK(expr));
                    OwnedHqlExpr call = bindFunctionCall(func, args);
                    buildExprAssign(ctx, target, call);
                    return;
                }
            }
        }
    }

    IHqlExpression * record = ::queryRecord(to);
    Owned<IHqlCppDatasetBuilder> builder;
    if (targetOutOfLine)
    {
        if (isDictionaryType(target.queryType()))
        {
            builder.setown(createLinkedDictionaryBuilder(record));
        }
        else
        {
            IHqlExpression * choosenLimit = NULL;
            if ((op == no_choosen) && !isChooseNAllLimit(expr->queryChild(1)) && !queryRealChild(expr, 2))
            {
                choosenLimit = expr->queryChild(1);
                expr = expr->queryChild(0);
            }
            builder.setown(createLinkedDatasetBuilder(record, choosenLimit));
        }
    }
    else
        builder.setown(createBlockedDatasetBuilder(record));
    builder->buildDeclare(ctx);

    buildDatasetAssign(ctx, builder, expr);

    builder->buildFinish(ctx, target);
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildCheckDatasetLimit(BuildCtx & ctx, IHqlExpression * expr, const CHqlBoundExpr & bound)
{
    IHqlExpression * record = expr->queryRecord();
    IHqlExpression * limit = expr->queryChild(1);

    OwnedHqlExpr test;
    if (!bound.count && bound.length && isFixedRecordSize(record))
    {
        OwnedHqlExpr size = bound.length->queryValue() ? LINK(bound.length) : createTranslated(bound.length);
        OwnedHqlExpr maxSize = createValue(no_mul, LINK(sizetType), ensureExprType(limit, sizetType), getSizetConstant(getFixedRecordSize(record)));
        test.setown(createBoolExpr(no_gt, ensureExprType(size, sizetType), LINK(maxSize)));
    }
    else
    {
        OwnedHqlExpr count = getBoundCount(bound);
        OwnedHqlExpr translatedCount = count->queryValue() ? LINK(count) : createTranslated(count);
        test.setown(createBoolExpr(no_gt, ensureExprType(translatedCount, sizetType), ensureExprType(limit, sizetType)));
    }
    OwnedHqlExpr folded = foldHqlExpression(test);
    LinkedHqlExpr fail = queryRealChild(expr, 2);

    if (folded->queryValue())
    {
        if (!folded->queryValue()->getBoolValue())
            return;

        StringBuffer failMessageText;
        if (fail)
        {
            OwnedHqlExpr failMessage = getFailMessage(fail, true);
            if (failMessage && failMessage->queryValue())
                failMessage->queryValue()->getStringValue(failMessageText);
        }
        if (failMessageText.length())
            WARNING1(CategoryUnexpected, HQLWRN_LimitAlwaysExceededX, failMessageText.str());
        else
            WARNING(CategoryUnexpected, HQLWRN_LimitAlwaysExceeded);
    }

    if (!fail)
        fail.setown(createFailAction("Limit exceeded", limit, NULL, queryCurrentActivityId(ctx)));

    BuildCtx subctx(ctx);
    buildFilter(subctx, folded);
    buildStmt(subctx, fail);
}

void HqlCppTranslator::doBuildDatasetLimit(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    buildDataset(ctx, expr->queryChild(0), tgt, format);

    doBuildCheckDatasetLimit(ctx, expr, tgt);
}

void HqlCppTranslator::doBuildDatasetNull(IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
     tgt.count.setown(getSizetConstant(0));
     tgt.length.setown(getSizetConstant(0));
     IHqlExpression * record = expr->queryRecord();
     Owned<ITypeInfo> type;
     if (expr->isDictionary())
         type.setown(makeDictionaryType(makeRowType(record->getType())));
     else
         type.setown(makeTableType(makeRowType(record->getType())));
     if ((format == FormatLinkedDataset) || (format == FormatArrayDataset) || expr->isDictionary())
         type.setown(setLinkCountedAttr(type, true));
     tgt.expr.setown(createValue(no_nullptr, makeReferenceModifier(type.getClear())));
}


class ConstantRow : public CInterface
{
public:
    ConstantRow(IHqlExpression * _transform, IHqlExpression * _boundRow) : transform(_transform), boundRow(_boundRow)
    {
    }

public:
    IHqlExpression * transform;
    LinkedHqlExpr boundRow;
};

class ConstantRowArray : public CIArrayOf<ConstantRow> {};

bool HqlCppTranslator::buildConstantRows(ConstantRowArray & boundRows, IHqlExpression * transforms)
{
    HqlExprArray rows;

    ForEachChild(row, transforms)
    {
        OwnedHqlExpr constRow = createConstantRowExpr(transforms->queryChild(row));
        if (!constRow || !canGenerateStringInline(constRow->queryType()->getSize()))
            return false;
        rows.append(*constRow.getClear());
    }

    ForEachItemIn(i, rows)
    {
        IHqlExpression * transform = transforms->queryChild(i);
        CHqlBoundExpr bound;
        buildConstRow(transform->queryRecord(), &rows.item(i), bound);
        boundRows.append(*new ConstantRow(transform, bound.expr));
    }
    return true;
}

bool HqlCppTranslator::doBuildConstantDatasetInlineTable(IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    if (!options.generateStaticInlineTables)
        return false;

    BuildCtx declareCtx(*code, literalAtom);
    IHqlExpression * transforms = expr->queryChild(0);
    IHqlExpression * record = expr->queryRecord();
    if (transforms->numChildren() == 0)
    {
        OwnedHqlExpr null = createDataset(no_null, LINK(record));
        buildDataset(declareCtx, null, tgt, format);
        return true;
    }

    //Remove unique id when checking for constant datasets already generated
    OwnedHqlExpr exprKey = removeAttribute(expr, _uid_Atom);
    if (declareCtx.getMatchExpr(exprKey, tgt))
        return true;

    ConstantRowArray boundRows;
    if (!buildConstantRows(boundRows, transforms))
        return false;

    Owned<ITypeInfo> rowType = makeConstantModifier(makeReferenceModifier(makeRowType(LINK(queryRecordType(expr->queryType())))));

    HqlExprArray args;
    ForEachItemIn(i, boundRows)
        args.append(*LINK(boundRows.item(i).boundRow));
    OwnedHqlExpr values = createValue(no_list, makeSetType(LINK(rowType)), args);

    unsigned maxRows = values->numChildren();
    Owned<ITypeInfo> declareType = makeConstantModifier(makeArrayType(LINK(rowType), maxRows));
    OwnedITypeInfo rowsType = makeOutOfLineModifier(makeTableType(LINK(rowType)));
    if (options.canLinkConstantRows)
        rowsType.setown(setLinkCountedAttr(rowsType, true));

    OwnedHqlExpr table = declareCtx.getTempDeclare(declareType, values);
    if (options.spanMultipleCpp)
    {
        BuildCtx protoctx(*code, mainprototypesAtom);
        protoctx.addDeclareExternal(table);
    }

    tgt.count.setown(getSizetConstant(maxRows));
    tgt.expr.setown(createValue(no_typetransfer, LINK(rowsType), LINK(table)));

    declareCtx.associateExpr(exprKey, tgt);
    return true;
}

//---------------------------------------------------------------------------------------------------------------------
//The code generator uses the the run time library code used to build dictionaries to try and ensure they stay compatible.
//
//The following classes allow IHqlExpressions to be used with those external classes.  A ConstantRow * is used where a
//row would normally be used at runtime.

//This class provides the minimal functionality for the interface required to call the dictionary builder class
class EclccEngineRowAllocator : public CInterfaceOf<IEngineRowAllocator>
{
public:
    virtual byte * * createRowset(unsigned _numItems) { return (byte * *)malloc(_numItems * sizeof(byte *)); }
    virtual byte * * linkRowset(byte * * rowset) { throwUnexpected(); }
    virtual void releaseRowset(unsigned count, byte * * rowset) { free(rowset); }
    virtual byte * * appendRowOwn(byte * * rowset, unsigned newRowCount, void * row)
    {
        byte * * expanded = reallocRows(rowset, newRowCount-1, newRowCount);
        expanded[newRowCount-1] = (byte *)row;
        return expanded;
    }
    virtual byte * * reallocRows(byte * * rowset, unsigned oldRowCount, unsigned newRowCount)
    {
        return (byte * *)realloc(rowset, newRowCount * sizeof(byte *));
    }

    virtual void * createRow() { throwUnexpected(); }
    virtual void releaseRow(const void * row) {  } // can occur if a row is removed from a dictionary.
    virtual void * linkRow(const void * row) { return const_cast<void *>(row); }  // can occur if a dictionary is resized.

//Used for dynamically sizing rows.
    virtual void * createRow(size32_t & allocatedSize) { throwUnexpected(); }
    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size) { throwUnexpected(); }
    virtual void * finalizeRow(size32_t newSize, void * row, size32_t oldSize) { throwUnexpected(); }

    virtual IOutputMetaData * queryOutputMeta() { return NULL; }
    virtual unsigned queryActivityId() const { return 0; }
    virtual StringBuffer &getId(StringBuffer & out) { return out; }
    virtual IOutputRowSerializer *createDiskSerializer(ICodeContext *ctx = NULL) { throwUnexpected(); }
    virtual IOutputRowDeserializer *createDiskDeserializer(ICodeContext *ctx) { throwUnexpected(); }
    virtual IOutputRowSerializer *createInternalSerializer(ICodeContext *ctx = NULL) { throwUnexpected(); }
    virtual IOutputRowDeserializer *createInternalDeserializer(ICodeContext *ctx) { throwUnexpected(); }
    virtual IEngineRowAllocator *createChildRowAllocator(const RtlTypeInfo *type) { throwUnexpected(); }
};

//Use a (constant) transform to map selectors of the form queryActiveTableSelector().field
static IHqlExpression * mapExprViaTransform(IHqlExpression * transform, IHqlExpression * expr)
{
    NewProjectMapper2 mapper;
    mapper.setMapping(transform);
    return mapper.expandFields(expr, queryActiveTableSelector(), queryActiveTableSelector(), queryActiveTableSelector());
}

//Implement hash - constructor parameter is the hash expression
class EclccCHash : implements IHash
{
public:
    EclccCHash(IHqlExpression * _hashExpr) : hashExpr(_hashExpr) {}

    virtual unsigned hash(const void *data)
    {
        const ConstantRow * row = reinterpret_cast<const ConstantRow *>(data);
        OwnedHqlExpr expanded = mapExprViaTransform(row->transform, hashExpr);
        OwnedHqlExpr folded = foldHqlExpression(expanded);
        assertex(folded->queryValue());
        return (unsigned)getIntValue(folded, 0);
    }

protected:
    LinkedHqlExpr hashExpr;
};

// implement compare -the constructor parameter is the list of fields to compare
class EclccCCompare : implements ICompare
{
public:
    EclccCCompare(IHqlExpression * _sortorder) : sortorder(_sortorder) {}

    virtual int docompare(const void * _left,const void * _right) const
    {
        const ConstantRow * left = reinterpret_cast<const ConstantRow *>(_left);
        const ConstantRow * right = reinterpret_cast<const ConstantRow *>(_right);

        OwnedHqlExpr expandedLeft = mapExprViaTransform(left->transform, sortorder);
        OwnedHqlExpr expandedRight = mapExprViaTransform(right->transform, sortorder);
        OwnedHqlExpr order = createValue(no_order, LINK(signedType), expandedLeft.getClear(), expandedRight.getClear());
        OwnedHqlExpr folded = foldHqlExpression(order);
        assertex(folded->queryValue());
        return (int)getIntValue(folded, 0);
    }

protected:
    LinkedHqlExpr sortorder;
};

//The dictionary information class - the hash lookup versions are not implemented
class EclccHashLookupInfo : implements IHThorHashLookupInfo
{
public:
    EclccHashLookupInfo(IHqlExpression * hashExpr, IHqlExpression * sortorder)
        : hasher(hashExpr), comparer(sortorder)
    {
    }

    virtual IHash * queryHash() { return &hasher; }
    virtual ICompare * queryCompare() { return &comparer; }
    virtual IHash * queryHashLookup() { throwUnexpected(); }
    virtual ICompare * queryCompareLookup() { throwUnexpected(); }

protected:
    EclccCHash hasher;
    EclccCCompare comparer;
};

void HqlCppTranslator::createInlineDictionaryRows(HqlExprArray & args, ConstantRowArray & boundRows, IHqlExpression * keyRecord, IHqlExpression * nullRow)
{
    //The code generator uses the the run time library code used to build dictionaries to try and ensure they stay compatible.
    HqlExprArray keyedDictFields;
    expandRecord(keyedDictFields, queryActiveTableSelector(), keyRecord);
    OwnedHqlExpr keyedlist = createSortList(keyedDictFields);
    OwnedHqlExpr hash = createValue(no_hash32, LINK(unsignedType), LINK(keyedlist));

    //Estimate a good hash table size from the number of rows - otherwise the size can be more than double the number of rows
    size32_t hashSize = (boundRows.ordinality() * 4 / 3) + 1;
    EclccEngineRowAllocator rowsetAllocator;
    EclccHashLookupInfo hasher(hash, keyedlist);
    RtlLinkedDictionaryBuilder builder(&rowsetAllocator, &hasher, hashSize);
    ForEachItemIn(i, boundRows)
        builder.appendOwn(&boundRows.item(i));

    unsigned size = builder.getcount();
    ConstantRow * * rows = reinterpret_cast<ConstantRow * *>(builder.queryrows());
    for (unsigned i=0; i < size; i++)
    {
        if (rows[i])
            args.append(*LINK(rows[i]->boundRow));
        else
            args.append(*LINK(nullRow)); 
    }
}

bool HqlCppTranslator::doBuildDictionaryInlineTable(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    if (!options.generateStaticInlineTables || !options.canLinkConstantRows)
        return false;

    IHqlExpression * dataset = expr->queryChild(0);
    assertex(dataset->getOperator() == no_inlinetable);

    IHqlExpression * transforms = dataset->queryChild(0);
    IHqlExpression * record = dataset->queryRecord();
    if (transforms->numChildren() == 0)
    {
        OwnedHqlExpr null = createDictionary(no_null, LINK(record));
        buildDataset(ctx, null, tgt, format);
        return true;
    }

    BuildCtx declareCtx(*code, literalAtom);
    //Remove unique id when checking for constant datasets already generated
    OwnedHqlExpr exprNoUnique = removeAttribute(dataset, _uid_Atom);
    OwnedHqlExpr exprKey = createAttribute(dictionaryAtom, exprNoUnique.getClear());
    if (declareCtx.getMatchExpr(exprKey, tgt))
        return true;

    ConstantRowArray boundRows;
    if (!buildConstantRows(boundRows, transforms))
        return false;

    OwnedHqlExpr keyRecord = getDictionaryKeyRecord(record);
    Owned<ITypeInfo> rowType = makeConstantModifier(makeReferenceModifier(makeRowType(LINK(queryRecordType(expr->queryType())))));
    OwnedHqlExpr nullExpr = createValue(no_nullptr, LINK(rowType));

    HqlExprArray args;
    try
    {
        createInlineDictionaryRows(args, boundRows, keyRecord, nullExpr);
    }
    catch (IException * e)
    {
        //If the hash or compare couldn't be done (e.g., some strange field type that isn't constant folded)
        //then generate a warning and fall back to the default inline dictionary code
        EXCLOG(e, "Generating an inline dictionary");
        e->Release();
        return false;
    }
    OwnedHqlExpr values = createValue(no_list, makeSetType(LINK(rowType)), args);

    unsigned maxRows = values->numChildren();
    Owned<ITypeInfo> declareType = makeConstantModifier(makeArrayType(LINK(rowType), maxRows));
    OwnedITypeInfo rowsType = makeOutOfLineModifier(makeDictionaryType(LINK(rowType)));
    rowsType.setown(setLinkCountedAttr(rowsType, true));

    OwnedHqlExpr table = declareCtx.getTempDeclare(declareType, values);
    if (options.spanMultipleCpp)
    {
        BuildCtx protoctx(*code, mainprototypesAtom);
        protoctx.addDeclareExternal(table);
    }

    tgt.count.setown(getSizetConstant(maxRows));
    tgt.expr.setown(createValue(no_typetransfer, LINK(rowsType), LINK(table)));

    declareCtx.associateExpr(exprKey, tgt);
    return true;
}


//---------------------------------------------------------------------------
// Dataset creation via builder

void HqlCppTranslator::buildDatasetAssignTempTable(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    OwnedHqlExpr values = normalizeListCasts(expr->queryChild(0));
    if (values->getOperator() == no_null)
        return;

    IHqlExpression * record = expr->queryChild(1);
    OwnedHqlExpr rowsExpr;
    if (values->queryType()->getTypeCode() == type_set)
    {
        if ((values->getOperator() == no_list) && !values->isConstant())
        {
            ForEachChild(i, values)
            {
                BuildCtx loopctx(ctx);
                BoundRow * targetRow = target->buildCreateRow(loopctx);
                OwnedHqlExpr targetField = createSelectExpr(LINK(targetRow->querySelector()), LINK(record->queryChild(0)));
                buildAssign(loopctx, targetField, values->queryChild(i));
                target->finishRow(loopctx, targetRow);
            }
        }
        else
        {
            Owned<IHqlCppSetCursor> cursor = createSetSelector(ctx, values);
            BuildCtx loopctx(ctx);
            CHqlBoundExpr boundCurElement;
            cursor->buildIterateLoop(loopctx, boundCurElement, false);
            BoundRow * targetRow = target->buildCreateRow(loopctx);

            OwnedHqlExpr targetField = createSelectExpr(LINK(targetRow->querySelector()), LINK(record->queryChild(0)));
            OwnedHqlExpr value = boundCurElement.getTranslatedExpr();
            buildAssign(loopctx, targetField, value);
            target->finishRow(loopctx, targetRow);
        }
    }
    else
    {
        BuildCtx subctx(ctx);
        BoundRow * targetRow = target->buildCreateRow(subctx);
        Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
        buildRowAssign(subctx, targetRef, values);
        target->finishRow(subctx, targetRow);
    }
}

void HqlCppTranslator::buildDatasetAssignInlineTable(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    IHqlExpression * transforms = expr->queryChild(0);
    if (transforms->numChildren() == 0)
        return;

    unsigned maxRows = transforms->numChildren();
    unsigned row;
    const bool copyConstantRows = true;//getFieldCount(expr->queryRecord()) > 2;
    for (row = 0; row < maxRows; row++)
    {
        IHqlExpression * transform = transforms->queryChild(row);
        OwnedHqlExpr rowValue = createRow(no_createrow, LINK(transform));

        BuildCtx subctx(ctx);
        CHqlBoundExpr bound;
        //Work in progress.  Check if there are several fields - otherwise not worth it.s
        if (doBuildRowConstantTransform(transform, bound))
        {
            BoundRow * row = bindConstantRow(subctx, rowValue, bound);
            if (target->buildLinkRow(subctx, row))
                continue;
        }

        BoundRow * targetRow = target->buildCreateRow(subctx);
        Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
        buildRowAssign(subctx, targetRef, rowValue);
        target->finishRow(subctx, targetRow);
    }
}

void HqlCppTranslator::buildDatasetAssignDatasetFromTransform(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_dataset_from_transform);
    IHqlExpression * count = expr->queryChild(0);
    if (isZero(count) || isNegative(count))
        return;

    IHqlExpression * transform = expr->queryChild(1);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);

    // If it is at all possible that it could be negative, we must test before producing rows
    CHqlBoundExpr boundCount;
    buildSimpleExpr(ctx, count, boundCount);
    BuildCtx subctx(ctx);
    if (couldBeNegative(count))
    {
        OwnedHqlExpr zero = createConstant(0, count->getType());
        OwnedHqlExpr ifTest = createValue(no_gt, makeBoolType(), boundCount.getTranslatedExpr(), LINK(zero));
        buildFilter(subctx, ifTest);
    }

    // loopVar = 1;
    OwnedHqlExpr loopVar = subctx.getTempDeclare(counterType, NULL);
    OwnedHqlExpr one = getSizetConstant(1);
    buildAssignToTemp(subctx, loopVar, one);

    // for(; loopVar <= maxRows; loopVar++)
    OwnedHqlExpr loopTest = createValue(no_le, makeBoolType(), LINK(loopVar), LINK(boundCount.expr));
    OwnedHqlExpr inc = createValue(no_postinc, loopVar->getType(), LINK(loopVar));
    subctx.addLoop(loopTest, inc, false);
    if (counter)
        subctx.associateExpr(counter, loopVar);

    OwnedHqlExpr rowValue = createRow(no_createrow, LINK(transform));
    BoundRow * targetRow = target->buildCreateRow(subctx);
    Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
    buildRowAssign(subctx, targetRef, rowValue);
    target->finishRow(subctx, targetRow);
}

class InlineDatasetSkipCallback : public CInterface, implements IHqlCodeCallback
{
public:
    IMPLEMENT_IINTERFACE

    virtual void buildCode(HqlCppTranslator & translator, BuildCtx & ctx)
    {
        ctx.addContinue();
    }
};

void HqlCppTranslator::buildDatasetAssignProject(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    BuildCtx iterctx(ctx);
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);

    OwnedHqlExpr counterVar;
    if (counter)
    {
        counterVar.setown(iterctx.getTempDeclare(unsignedType, queryZero()));
    }

    bool containsSkip = transformContainsSkip(expr->queryChild(1));
    BoundRow * sourceCursor = buildDatasetIterate(iterctx, ds, containsSkip);
    if (counter)
    {
        iterctx.associateExpr(counter, counterVar);
        OwnedHqlExpr inc = createValue(no_postinc, LINK(unsignedType), LINK(counterVar));
        iterctx.addExpr(inc);
    }

    if (sourceCursor)
    {
        if (isNullProject(expr, true, false))
        {
            if (target->buildLinkRow(iterctx, sourceCursor))
                return;
        }
        BoundRow * targetRow = target->buildCreateRow(iterctx);
        HqlExprAssociation * skipAssociation = NULL;
        if (containsSkip)
        {
            OwnedHqlExpr callback = createUnknown(no_unknown, makeVoidType(), NULL, new InlineDatasetSkipCallback);
            skipAssociation = ctx.associateExpr(skipActionMarker, callback);
        }

        Owned<IReferenceSelector> targetRef = buildActiveRow(iterctx, targetRow->querySelector());
        switch (expr->getOperator())
        {
        case no_hqlproject:
            doBuildRowAssignProject(iterctx, targetRef, expr);
            break;
        case no_newusertable:
            doBuildRowAssignUserTable(iterctx, targetRef, expr);
            break;
        }

        ctx.removeAssociation(skipAssociation);
        target->finishRow(iterctx, targetRow);
    }
}


void HqlCppTranslator::buildDatasetAssignJoin(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    IHqlExpression * left = expr->queryChild(0);
    IHqlExpression * right = expr->queryChild(1);
    IHqlExpression * cond = expr->queryChild(2);

    IHqlExpression * selSeq = querySelSeq(expr);
    bool leftOuter = expr->hasAttribute(leftonlyAtom) || expr->hasAttribute(leftouterAtom);

    CHqlBoundExpr nullRhs;
    if (leftOuter)
        buildDefaultRow(ctx, right, nullRhs);

    BuildCtx leftIterCtx(ctx);
    BoundRow * leftCursor = buildDatasetIterate(leftIterCtx, left, false);
    bindTableCursor(leftIterCtx, left, leftCursor->queryBound(), no_left, selSeq);

    OwnedHqlExpr matchedAnyVar;
    if (leftOuter)
        matchedAnyVar.setown(leftIterCtx.getTempDeclare(queryBoolType(), queryBoolExpr(false)));

    BuildCtx rightIterCtx(leftIterCtx);
    BoundRow * rightCursor = buildDatasetIterate(rightIterCtx, right, false);
    bindTableCursor(rightIterCtx, right, rightCursor->queryBound(), no_right, selSeq);

    OwnedHqlExpr cseCond = options.spotCSE ? spotScalarCSE(cond, NULL, queryOptions().spotCseInIfDatasetConditions) : LINK(cond);
    buildFilter(rightIterCtx, cseCond);
    if (!expr->hasAttribute(leftonlyAtom))
    {
        BoundRow * targetRow = target->buildCreateRow(rightIterCtx);
        Owned<IReferenceSelector> targetRef = buildActiveRow(rightIterCtx, targetRow->querySelector());
        OwnedHqlExpr rowValue = createRow(no_createrow, LINK(expr->queryChild(3)));
        buildRowAssign(rightIterCtx, targetRef, rowValue);
        target->finishRow(rightIterCtx, targetRow);
    }
    if (matchedAnyVar)
    {
        buildAssignToTemp(rightIterCtx, matchedAnyVar, queryBoolExpr(true));

        OwnedHqlExpr test = getInverse(matchedAnyVar);
        leftIterCtx.addFilter(test);

        OwnedHqlExpr defaultRowPtr = getPointer(nullRhs.expr);
        bindTableCursor(leftIterCtx, right, defaultRowPtr, no_right, selSeq);

        BoundRow * targetRow = target->buildCreateRow(leftIterCtx);
        Owned<IReferenceSelector> targetRef = buildActiveRow(leftIterCtx, targetRow->querySelector());
        OwnedHqlExpr rowValue = createRow(no_createrow, LINK(expr->queryChild(3)));
        buildRowAssign(leftIterCtx, targetRef, rowValue);
        target->finishRow(leftIterCtx, targetRow);
    }
}


void HqlCppTranslator::buildDatasetAssignAggregate(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    BuildCtx subctx(ctx);
    subctx.addGroup();
    BoundRow * targetRow = target->buildCreateRow(subctx);

    Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
    doBuildRowAssignAggregate(subctx, targetRef, expr);
    target->finishRow(subctx, targetRow);
}

void HqlCppTranslator::buildDatasetAssignChoose(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr)
{
    CHqlBoundExpr cond;
    buildExpr(ctx, expr->queryChild(0), cond);

    IHqlExpression * last = queryLastNonAttribute(expr);
    BuildCtx subctx(ctx);
    IHqlStmt * switchstmt = subctx.addSwitch(cond.expr);
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur != last)
        {
            OwnedHqlExpr label = getSizetConstant(i);
            subctx.addCase(switchstmt, label);
        }
        else
            subctx.addDefault(switchstmt);

        buildDatasetAssign(subctx, target, cur);
    }
}


void HqlCppTranslator::buildDatasetAssignChoose(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    CHqlBoundExpr cond;
    buildExpr(ctx, expr->queryChild(0), cond);

    IHqlExpression * last = queryLastNonAttribute(expr);
    BuildCtx subctx(ctx);
    IHqlStmt * switchstmt = subctx.addSwitch(cond.expr);
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur != last)
        {
            OwnedHqlExpr label = getSizetConstant(i);
            subctx.addCase(switchstmt, label);
        }
        else
            subctx.addDefault(switchstmt);

        buildDatasetAssign(subctx, target, cur);
    }
}


void HqlCppTranslator::buildDatasetAssignIf(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    BuildCtx subctx(ctx);
    IHqlStmt * filter = buildFilterViaExpr(subctx, expr->queryChild(0));
    buildDatasetAssign(subctx, target, expr->queryChild(1));

    IHqlExpression * elseExpr = expr->queryChild(2);
    assertex(elseExpr);
    subctx.selectElse(filter);
    buildDatasetAssign(subctx, target, elseExpr);
}


void HqlCppTranslator::buildDatasetAssign(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * _expr)
{
    OwnedHqlExpr expr = forceInlineAssignDataset(ctx, _expr);

    bool isRowAssign = false;
    BuildCtx subctx(ctx);
    switch (expr->getOperator())
    {
    case no_fail:
        doBuildStmtFail(ctx, expr->queryChild(1));
        return;
    case no_addfiles:
        buildDatasetAssign(subctx, target, expr->queryChild(0));
        buildDatasetAssign(subctx, target, expr->queryChild(1));
        return;
    case no_temptable:
        buildDatasetAssignTempTable(subctx, target, expr);
        //MORE: Create rows and assign each one in turn.  Could possibly be done with a different dataset selector
        return;
    case no_inlinetable:
        buildDatasetAssignInlineTable(subctx, target, expr);
        return;
    case no_dataset_from_transform:
        buildDatasetAssignDatasetFromTransform(subctx, target, expr);
        return;
    case no_xmlproject:
        buildDatasetAssignXmlProject(subctx, target, expr);
        return;
    case no_datasetfromrow:
        {
            isRowAssign = true;
            expr.set(expr->queryChild(0));
            break;
        }
    case no_if:
        {
            CHqlBoundExpr bound;
            buildExpr(subctx, expr->queryChild(0), bound);
            IHqlStmt * filter = subctx.addFilter(bound.expr);
            buildDatasetAssign(subctx, target, expr->queryChild(1));

            IHqlExpression * elseExpr = expr->queryChild(2);
            if (elseExpr && elseExpr->getOperator() != no_null)
            {
                subctx.selectElse(filter);
                buildDatasetAssign(subctx, target, elseExpr);
            }
        }
        return;
    case no_chooseds:
        buildDatasetAssignChoose(subctx, target, expr);
        return;
    case no_null:
        return;
    case no_activetable:
    case no_temprow:
    case no_createrow:
    case no_projectrow:
    case no_typetransfer:
        isRowAssign = true;
        break;
    case no_newaggregate:
        buildDatasetAssignAggregate(subctx, target, expr);
        return;
    case no_hqlproject:
    case no_newusertable:
        buildDatasetAssignProject(subctx, target, expr);
        return;
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_distributed:
    case no_preservemeta:
    case no_sorted:
    case no_nofold:
    case no_nohoist:
    case no_forcegraph:
    case no_nocombine:
    case no_section:
    case no_sectioninput:
        buildDatasetAssign(subctx, target, expr->queryChild(0));
        return;
    case no_alias_scope:
//      expandAliasScope(subctx, expr);
        buildDatasetAssign(subctx, target, expr->queryChild(0));
        return;
    case no_filter:
        {
            //We want to evaluate invariant conditions outside of the loop, rather than inside the dataset assignment
            //Currently the test is whether the expression is independent of any tables.  Better would be to
            //see if the test was dependent on any of the datasets introduced by expr->queryChild(0).
            HqlExprArray conds;
            unwindFilterConditions(conds, expr);

            IHqlExpression * ds = expr->queryChild(0);
#if 0
            HqlExprCopyArray selectors;
            loop
            {
                selectors.append(*ds->queryNormalizedSelector());
                IHqlExpression * root = queryRoot(expr);
                if (!root || root->getOperator() != no_select)
                    break;
                bool isNew;
                ds = querySelectorDataset(root, isNew);
                if (!isNew)
                    break;
            }
#endif

            unsigned max = conds.ordinality();
            unsigned i = 0;
            bool optimized = false;
            while (i < max)
            {
                IHqlExpression & cur = conds.item(i);
#if 0
                bool overlap = false;
                ForEachItemIn(j, selectors)
                {
                    if (containsSelector(&cur, &selectors.item(j)))
                    {
                        overlap = true;
                        break;
                    }
                }
#else
                bool overlap = containsSelector(&cur, ds->queryNormalizedSelector());
#endif
                if (!overlap)
                {
                    buildFilter(subctx, &cur);
                    conds.remove(i);
                    optimized = true;
                    max--;
                }
                else
                    i++;
            }

            if (max == 0)
            {
                buildDatasetAssign(subctx, target, ds);
                return;
            }

            if (optimized)
            {
                conds.add(*LINK(ds), 0);
                expr.setown(expr->clone(conds));
            }
            break;
        }
    case no_join:
        buildDatasetAssignJoin(subctx, target, expr);
        return;
    }

    ITypeInfo * type = expr->queryType();
    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_record:       // e.g. dataset.recordfield
        case type_row:
            isRowAssign = true;
            break;
        }
    }

    if (isRowAssign)
    {
        bool done = false;
        subctx.addGroup();

        //Some code primarily here to improve the generated code for productions inside parse statements for text parsing.
        //see if we can replace a memcpy of the child record with a link...
        if (!target->isRestricted())
        {
            switch (expr->getOperator())
            {
            case no_matchattr:
            case no_left:
            case no_right:
                {
                    //Only try for really simple cases...
                    Owned<IReferenceSelector> sourceRef = buildNewRow(subctx, expr);
                    Owned<BoundRow> sourceRow = sourceRef->getRow(subctx);
                    if (!target->buildLinkRow(subctx, sourceRow))
                    {
                        BoundRow * targetRow = target->buildCreateRow(subctx);
                        Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
                        buildRowAssign(subctx, targetRef, sourceRef);
                        target->finishRow(subctx, targetRow);
                    }
                    done = true;
                    break;
                }
            }
        }

        if (!done)
        {
            BoundRow * match = static_cast<BoundRow *>(ctx.queryAssociation(expr, AssocRow, NULL));
            if (match && target->buildLinkRow(subctx, match))
                done = true;
        }

        if (!done)
        {
            BoundRow * targetRow = target->buildCreateRow(subctx);
            Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
            buildRowAssign(subctx, targetRef, expr);
            target->finishRow(subctx, targetRow);
        }
    }
    else
    {
        if (!target->buildAppendRows(subctx, expr))
        {
            BoundRow * sourceRow = buildDatasetIterate(subctx, expr, false);
            if (sourceRow)
            {
                if (!target->buildLinkRow(subctx, sourceRow))
                {
                    BoundRow * targetRow = target->buildCreateRow(subctx);

                    Owned<IReferenceSelector> targetRef = buildActiveRow(subctx, targetRow->querySelector());
                    Owned<IReferenceSelector> sourceRef = buildActiveRow(subctx, sourceRow->querySelector());
                    buildRowAssign(subctx, targetRef, sourceRef);
                    target->finishRow(subctx, targetRow);
                }
            }
        }
    }
}

//---------------------------------------------------------------------------
// Dataset iteration

BoundRow * HqlCppTranslator::buildDatasetIterateSelectN(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    OwnedHqlExpr counter = ctx.getTempDeclare(sizetType, NULL);
    buildAssignToTemp(ctx, counter, expr->queryChild(1));
    BoundRow * cursor = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
    if (cursor)
    {
        OwnedHqlExpr dec = createValue(no_predec, counter->getType(), LINK(counter));
        OwnedHqlExpr test = createBoolExpr(no_eq, LINK(dec), getSizetConstant(0));
        ctx.addFilter(test);
    }
    return cursor;
}


BoundRow * HqlCppTranslator::buildDatasetIterateChoosen(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    OwnedHqlExpr counter = ctx.getTempDeclare(sizetType, queryZero());

    CHqlBoundExpr boundLow, boundHigh;
    OwnedHqlExpr foldedHigh = foldHqlExpression(expr->queryChild(1));
    if (!isChooseNAllLimit(foldedHigh))
        buildSimpleExpr(ctx, foldedHigh, boundHigh);
    if (queryRealChild(expr, 2))
    {
        OwnedHqlExpr foldedLow = foldHqlExpression(expr->queryChild(2));
        OwnedHqlExpr low = adjustValue(foldedLow, -1);
        buildSimpleExpr(ctx, low, boundLow);
        if (boundHigh.expr)
            boundHigh.expr.setown(createValue(no_add, LINK(boundHigh.queryType()), LINK(boundHigh.expr), LINK(boundLow.expr)));
    }

    BoundRow * cursor = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);

    if (cursor)
    {
        OwnedHqlExpr inc = createValue(no_postinc, counter->getType(), LINK(counter));
        ctx.addExpr(inc);

        OwnedHqlExpr cond;
        if (boundLow.expr)
            extendConditionOwn(cond, no_and, createBoolExpr(no_gt, LINK(counter), LINK(boundLow.expr)));
        if (boundHigh.expr)
            extendConditionOwn(cond, no_and, createBoolExpr(no_le, LINK(counter), LINK(boundHigh.expr)));
        if (cond)
            ctx.addFilter(cond);
    }
    return cursor;
}


BoundRow * HqlCppTranslator::buildDatasetIterateLimit(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    OwnedHqlExpr counter = ctx.getTempDeclare(sizetType, queryZero());

    CHqlBoundExpr boundHigh;
    OwnedHqlExpr foldedHigh = foldHqlExpression(expr->queryChild(1));
    buildSimpleExpr(ctx, foldedHigh, boundHigh);

    BoundRow * cursor = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
    if (cursor)
    {
        OwnedHqlExpr inc = createValue(no_preinc, counter->getType(), LINK(counter));
        OwnedHqlExpr cond = createBoolExpr(no_gt, LINK(inc), LINK(boundHigh.expr));
        BuildCtx subctx(ctx);
        subctx.addFilter(cond);

        LinkedHqlExpr fail = expr->queryChild(2);
        if (!fail || fail->isAttribute())
            fail.setown(createFailAction("Limit exceeded", foldedHigh, NULL, queryCurrentActivityId(ctx)));
        buildStmt(subctx, fail);
    }
    return cursor;
}


BoundRow * HqlCppTranslator::buildDatasetIterateProject(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    IHqlExpression * dataset = expr->queryChild(0);
    OwnedHqlExpr counterVar;
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    if (counter)
    {
        counterVar.setown(ctx.getTempDeclare(unsignedType, queryZero()));
    }

    bool containsSkip = transformContainsSkip(expr->queryChild(1));
    if (containsSkip)
        needToBreak = true;

    buildDatasetIterate(ctx, dataset, needToBreak);
    Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);
    if (counter)
    {
        ctx.associateExpr(counter, counterVar);
        OwnedHqlExpr inc = createValue(no_postinc, LINK(unsignedType), LINK(counterVar));
        ctx.addExpr(inc);
    }

    Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);

    OwnedHqlExpr leftSelect = createSelector(no_left, dataset, querySelSeq(expr));
    OwnedHqlExpr transform = replaceSelector(expr->queryChild(1), leftSelect, dataset->queryNormalizedSelector());

    HqlExprAssociation * skipAssociation = NULL;
    if (containsSkip)
    {
        OwnedHqlExpr callback = createUnknown(no_unknown, makeVoidType(), NULL, new InlineDatasetSkipCallback);
        skipAssociation = ctx.associateExpr(skipActionMarker, callback);
    }
    doTransform(ctx, transform, rowBuilder);

    ctx.removeAssociation(skipAssociation);     //remove it in case keeping hold of it causes issues.
    finalizeTempRow(ctx, tempRow, rowBuilder);

    return bindTableCursor(ctx, expr, tempRow->queryBound());
}


BoundRow * HqlCppTranslator::buildDatasetIterateUserTable(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    IHqlExpression * dataset = expr->queryChild(0);

    buildDatasetIterate(ctx, dataset, needToBreak);
    Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);
    Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);

    doTransform(ctx, expr->queryChild(2), rowBuilder);

    finalizeTempRow(ctx, tempRow, rowBuilder);

    return bindTableCursor(ctx, expr, tempRow->queryBound());
}


BoundRow * HqlCppTranslator::buildDatasetIterateSpecialTempTable(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    IHqlExpression * values = expr->queryChild(0);
    bool requiresTempRow = false;
    ITypeInfo * setType = values->queryType();
    ITypeInfo * type = setType->queryChildType();
    switch (type->getTypeCode())
    {
    case type_int:
        requiresTempRow = isComplexSet(setType, false);
        break;
    case type_swapint:
    case type_packedint:
    case type_alien:
    case type_bitfield:
        requiresTempRow = true;
        break;
    default:
        if (type->getSize() == UNKNOWN_LENGTH)
            requiresTempRow = true;
        break;
    }

    Owned<IHqlCppSetCursor> cursor = createSetSelector(ctx, values);
    CHqlBoundExpr boundCurElement;
    cursor->buildIterateLoop(ctx, boundCurElement, false);

    if (requiresTempRow)
    {
        //MORE: This could probably be improved by having a variety of buildIterateLoop which returned the
        //underlying bound row.  However it occurs fairly infrequently, so not a priority.
        Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);
        Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);

        IHqlExpression * record = expr->queryRecord();
        OwnedHqlExpr target = createSelectExpr(LINK(rowBuilder->querySelector()), LINK(record->queryChild(0)));
        OwnedHqlExpr curValue = boundCurElement.getTranslatedExpr();
        buildAssign(ctx, target, curValue);
        finalizeTempRow(ctx, tempRow, rowBuilder);

        return bindTableCursor(ctx, expr, tempRow->queryBound());
    }
    else
    {
        assertex(!boundCurElement.length && !boundCurElement.count);
        OwnedHqlExpr address = getPointer(boundCurElement.expr);
        address.setown(createValue(no_implicitcast, makeRowReferenceType(expr), LINK(address)));
        return bindTableCursor(ctx, expr, address);
    }
}


BoundRow * HqlCppTranslator::buildDatasetIterateFromDictionary(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    BoundRow * dictionaryRow = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
    assertex(dictionaryRow->isConditional());
    ctx.addFilter(dictionaryRow->queryBound());
    return rebindTableCursor(ctx, expr, dictionaryRow, no_none, NULL);
}


BoundRow * HqlCppTranslator::buildDatasetIterateStreamedCall(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    CHqlBoundExpr bound;
    doBuildExprCall(ctx, expr, bound);

    ITypeInfo * exprType = expr->queryType();
    Owned<ITypeInfo> wrappedType = makeWrapperModifier(LINK(exprType));

    ctx.addLoop(NULL, NULL, false);

    Owned<ITypeInfo> wrappedRowType = makeWrapperModifier(LINK(queryRowType(exprType)));
    OwnedHqlExpr tempRow = ctx.getTempDeclare(wrappedRowType, NULL);

    StringBuffer s;
    generateExprCpp(s, tempRow).append(".setown(");
    generateExprCpp(s, bound.expr).append("->nextRow());");
    ctx.addQuoted(s);

    s.clear().append("if (!");generateExprCpp(s, tempRow).append(".getbytes()) break;");
    ctx.addQuoted(s);
    return bindTableCursor(ctx, expr, tempRow);
}


BoundRow * HqlCppTranslator::buildDatasetIterate(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak)
{
    if (!canProcessInline(&ctx, expr))
    {
        Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, expr);
        return cursor->buildIterateLoop(ctx, needToBreak);
    }

    switch (expr->getOperator())
    {
    case no_dataset_alias:
        if (!expr->hasAttribute(_normalized_Atom))
        {
            OwnedHqlExpr uniqueChild = normalizeDatasetAlias(expr);
            BoundRow * childCursor = buildDatasetIterate(ctx, uniqueChild, needToBreak);
            return rebindTableCursor(ctx, expr, childCursor, no_none, NULL);
        }
        else
        {
            throwUnexpected();
            //The following would only be triggered for a splitter (not yet generated), and that would require
            //disambiguation when that was built.
            BoundRow * childCursor = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
            return rebindTableCursor(ctx, expr, childCursor, no_none, NULL);
        }
    case no_null:
        buildFilter(ctx, queryBoolExpr(false));
        return NULL;
    case no_filter:
        {
            IHqlExpression * dataset = expr->queryChild(0);
#ifdef _OPTIMZE_INLINE_FILTERS_
            //Good code, but messes up accidental cse in some createSegmentMonitor calls.
            HqlExprAttr invariant;
            OwnedHqlExpr cond = extractFilterConditions(invariant, expr, dataset);
            if (invariant)
                buildFilter(ctx, invariant);

            //MORE: if (canAssignInline(ctx, ds) && !canIterateInline(ctx, ds)) break;
            BoundRow * cursor = buildDatasetIterate(ctx, dataset, needToBreak);
            if (cond)
                buildFilter(ctx, cond);
#else
            //MORE: if (canAssignInline(ctx, ds) && !canIterateInline(ctx, ds)) break;
            BoundRow * cursor = buildDatasetIterate(ctx, dataset, needToBreak);
            unsigned max = expr->numChildren();
            for (unsigned i=1; i < max; i++)
                buildFilter(ctx, expr->queryChild(i));
#endif
            return cursor;
        }
    case no_id2blob:
    case no_select:
    case no_translated:
        {
            Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, expr);
            return cursor->buildIterateLoop(ctx, needToBreak);
        }
    case no_choosen:
        return buildDatasetIterateChoosen(ctx, expr, needToBreak);
    case no_limit:
        return buildDatasetIterateLimit(ctx, expr, needToBreak);
    case no_index:
    case no_selectnth:
        return buildDatasetIterateSelectN(ctx, expr, needToBreak);
    case no_hqlproject:
        return buildDatasetIterateProject(ctx, expr, needToBreak);
    case no_newusertable:
        return buildDatasetIterateUserTable(ctx, expr, needToBreak);
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_distributed:
    case no_preservemeta:
    case no_sorted:
    case no_nofold:
    case no_nohoist:
    case no_forcegraph:
    case no_nocombine:
        return buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
    case no_sectioninput:
    case no_section:
        {
            BoundRow * row = buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
#ifdef _DEBUG
            StringBuffer s;
            if (expr->getOperator() == no_section)
                s.append("//---- section ");
            else
                s.append("//---- end section ");
            getStringValue(s, expr->queryChild(1), "<?>").append(" ");
            getStringValue(s, expr->queryChild(2)).append("----");
            ctx.addQuoted(s);
#endif
            return row;
        }
    case no_alias_scope:
//      expandAliasScope(ctx, expr);
        return buildDatasetIterate(ctx, expr->queryChild(0), needToBreak);
    case no_temptable:
        {
            IHqlExpression * values = expr->queryChild(0);
            if (values->queryType()->getTypeCode() == type_set)
            {
                if (values->getOperator() == no_alias)
                    values = values->queryChild(0);

                bool special = false;
                switch (values->getOperator())
                {
                case no_getresult:
                case no_null:
                    special = true;
                    break;
                }
                if (special)
                    return buildDatasetIterateSpecialTempTable(ctx, expr, needToBreak);
            }
            break;
        }
    case no_datasetfromdictionary:
        return buildDatasetIterateFromDictionary(ctx, expr, needToBreak);
    case no_call:
    case no_externalcall:
        if (hasStreamedModifier(expr->queryType()))
            return buildDatasetIterateStreamedCall(ctx, expr, needToBreak);
        break;

#if 0
    //Following should improve the code, but I'm not sure how to correctly convert a referenceSelector to a boundExpr (since it may be with an existing row)
    case no_datasetfromrow:
        if (!needToBreak)
        {
            BoundRow * row = buildNewRow(ctx, expr->queryChild(0));
            bindTableCursor(ctx, expr, tempRow->queryBound(), no_none, NULL);
            return row;
        }
        break;
#endif

    }

    Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, expr);
    return cursor->buildIterateLoop(ctx, needToBreak);
}

//---------------------------------------------------------------------------
// Row Assignment

void HqlCppTranslator::buildCompoundAssign(BuildCtx & ctx, IHqlExpression * left, IReferenceSelector * leftSelector, IHqlExpression * rightScope, IHqlExpression * rightSelector)
{
    switch (left->getOperator())
    {
    case no_ifblock:
        {
            BuildCtx subctx(ctx);
            OwnedHqlExpr test = replaceSelector(left->queryChild(0), querySelfReference(), leftSelector->queryExpr());
            buildFilter(subctx, test);
            buildCompoundAssign(subctx, left->queryChild(1), leftSelector, rightScope, rightSelector);

            //This calculates the size of the previous block.  It means that subsequent uses of the 
            //offsets are cached - even if they are inside another ifblock().
            CHqlBoundExpr bound;
            IHqlExpression * mapexpr = createSelectExpr(LINK(leftSelector->queryExpr()), LINK(left));
            OwnedHqlExpr size = createValue(no_sizeof, makeIntType(4,false), mapexpr);
            buildCachedExpr(ctx, size, bound);
        }
        break;
    case no_field:
        {
            Owned<IReferenceSelector> selectedLeft = leftSelector->select(ctx, left);
            OwnedHqlExpr selectedRight;
            IHqlExpression * leftRecord = ::queryRecord(leftSelector->queryType());
            if (!rightScope || (leftRecord==rightScope))
            {
                selectedRight.setown(createSelectExpr(LINK(rightSelector), LINK(left)));
            }
            else
            {
                IHqlSimpleScope * scope = rightScope->querySimpleScope();
                IHqlExpression * resolved = scope->lookupSymbol(left->queryId());
                assertex(resolved);
                selectedRight.setown(createSelectExpr(LINK(rightSelector), resolved));
            }

            if (left->queryType()->getTypeCode() == type_row)
                buildCompoundAssign(ctx, left->queryRecord(), selectedLeft, selectedRight->queryRecord(), selectedRight);
            else
                selectedLeft->set(ctx, selectedRight);
            break;
        }
        break;
    case no_record:
        {
            ForEachChild(i, left)
                buildCompoundAssign(ctx, left->queryChild(i), leftSelector, rightScope, rightSelector);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}


void HqlCppTranslator::buildCompoundAssign(BuildCtx & ctx, IHqlExpression * left, IHqlExpression * right)
{
    //MORE: May need to resolve lhs and rhs in scope to find out the record type.
//  buildCompoundAssign(ctx, column->queryRecord(), selector->queryExpr(), value->queryRecord(), value);
    UNIMPLEMENTED;
}


void HqlCppTranslator::doBuildRowAssignAggregateClear(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * transform = expr->queryChild(2);

    unsigned numAggregates = transform->numChildren();
    unsigned idx;
    OwnedHqlExpr self = getSelf(expr);
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;
        Owned<IReferenceSelector> curTarget = createSelfSelect(ctx, target, cur->queryChild(0), self);
        IHqlExpression * src = cur->queryChild(1);

        switch (src->getOperator())
        {
        case no_countgroup:
        case no_maxgroup:
        case no_mingroup:
        case no_sumgroup:
        case no_existsgroup:
            curTarget->buildClear(ctx, 0);
            break;
        default:
            if (src->isConstant())
                curTarget->set(ctx, src);
            else
                curTarget->buildClear(ctx, 0);
            break;
        }
    }
}


void HqlCppTranslator::doBuildRowAssignAggregateNext(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr, bool isSingleExists, IHqlExpression * alreadyDoneExpr)
{
    IHqlExpression * transform = expr->queryChild(2);

    unsigned numAggregates = transform->numChildren();
    OwnedHqlExpr self = getSelf(expr);
    OwnedHqlExpr notAlreadyDone = alreadyDoneExpr ? getInverse(alreadyDoneExpr) : NULL;
    bool isVariableOffset = false;
    for (unsigned idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;

        IHqlExpression * targetSelect = cur->queryChild(0);
        Owned<IReferenceSelector> curTarget = createSelfSelect(ctx, target, targetSelect, self);
        IHqlExpression * src = cur->queryChild(1);
        IHqlExpression * arg = src->queryChild(0);
        IHqlExpression * cond = src->queryChild(1);

        BuildCtx condctx(ctx);
        node_operator srcOp = src->getOperator();
        switch (srcOp)
        {
        case no_countgroup:
            {
                assertex(!(arg && isVariableOffset));
                if (arg)
                    buildFilter(condctx, arg);
                OwnedHqlExpr one = getSizetConstant(1);
                if (isVariableOffset)
                {
                    IHqlStmt * ifStmt = condctx.addFilter(notAlreadyDone);
                    curTarget->set(condctx, one);
                    condctx.selectElse(ifStmt);
                }
                buildIncrementAssign(condctx, curTarget, one);
            }
            break;
        case no_sumgroup:
            {
                assertex(!(cond && isVariableOffset));
                if (cond)
                    buildFilter(condctx, cond);
                if (isVariableOffset)
                {
                    IHqlStmt * ifStmt = condctx.addFilter(notAlreadyDone);
                    curTarget->set(condctx, arg);
                    condctx.selectElse(ifStmt);
                }
                buildIncrementAssign(condctx, curTarget, arg);
            }
            break;
        case no_maxgroup:
        case no_mingroup:
            {
                node_operator op = (srcOp == no_maxgroup) ? no_gt : no_lt;
                assertex(!cond);
                OwnedHqlExpr castArg = ensureExprType(arg, targetSelect->queryType());      // cast to correct type, assume it can fit in the target type.
                OwnedHqlExpr temp = buildSimplifyExpr(condctx, castArg);
                OwnedHqlExpr compare = createBoolExpr (op, LINK(temp), LINK(curTarget->queryExpr()));
                if (notAlreadyDone)
                    compare.setown(createBoolExpr(no_or, LINK(notAlreadyDone), compare.getClear()));

                buildFilter(condctx, compare);
                curTarget->set(condctx, temp);
            }
            break;
        case no_existsgroup:
            assertex(!(arg && isVariableOffset));
            if (arg)
                buildFilter(condctx, arg);
            curTarget->set(condctx, queryBoolExpr(true));
            if (isSingleExists)
                condctx.addBreak();
            break;
        default:
            if (!src->isConstant() || isVariableOffset)
            {
                condctx.addFilter(notAlreadyDone);
                curTarget->set(condctx, src);
            }
            break;
        }
        if (targetSelect->queryType()->getSize() == UNKNOWN_LENGTH)
            isVariableOffset = true;
    }
    if (alreadyDoneExpr)
        buildAssignToTemp(ctx, alreadyDoneExpr, queryBoolExpr(true));
}


void HqlCppTranslator::doBuildRowAssignAggregate(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(2);
    unsigned numAggregates = transform->numChildren();

    if (isKeyedCountAggregate(expr))
        throwError(HQLERR_KeyedCountNonKeyable);

    bool needGuard = false;
    bool isSingleExists = true;
    unsigned idx;
    for (idx = 0; idx < numAggregates; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        if (cur->isAttribute())
            continue;

        IHqlExpression * tgt = cur->queryChild(0);
        IHqlExpression * src = cur->queryChild(1);
        switch (src->getOperator())
        {
        case no_countgroup:
        case no_sumgroup:
            isSingleExists = false;
            break;
        case no_existsgroup:
            break;
        case no_mingroup:
            isSingleExists = false;
            needGuard = true;
            break;
        case no_maxgroup:
            isSingleExists = false;
            if (!isNullValueMinimumValue(src->queryType()))
                needGuard = true;
            break;
        default:
            isSingleExists = false;
            if (!src->isConstant())
                needGuard = true;
            break;
        }
        if ((tgt->queryType()->getSize() == UNKNOWN_LENGTH) && (idx+1 != numAggregates))
            needGuard = true;
    }

    OwnedHqlExpr guard;
    if (needGuard)
    {
        Owned<ITypeInfo> boolType = makeBoolType(); 
        guard.setown(ctx.getTempDeclare(boolType, queryBoolExpr(false)));
    }

    doBuildRowAssignAggregateClear(ctx, target, expr);
    BuildCtx condctx(ctx);
    buildDatasetIterate(condctx, dataset, isSingleExists);
    doBuildRowAssignAggregateNext(condctx, target, expr, isSingleExists, guard);
}

void HqlCppTranslator::doBuildRowAssignProject(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
    if (counter && !ctx.queryMatchExpr(counter))
        throwError(HQLERR_CounterNotFound);

    BuildCtx subctx(ctx);

    assertex(target->isRoot());
    IHqlExpression * selSeq = querySelSeq(expr);
    OwnedHqlExpr leftSelect = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr activeDataset = ensureActiveRow(dataset->queryNormalizedSelector());
    OwnedHqlExpr transform = queryNewReplaceSelector(expr->queryChild(1), leftSelect, activeDataset);
    Owned<BoundRow> selfCursor;
    if (!transform)
    {
        subctx.addGroup();
        selfCursor.set(bindSelectorAsSelf(subctx, target, expr));
        //Mapping may potentially be ambiguous, so do things correctly (see hqlsource for details)
        BoundRow * prevCursor = resolveSelectorDataset(subctx, dataset->queryNormalizedSelector());
        transform.set(expr->queryChild(1));

        bindTableCursor(subctx, dataset, prevCursor->queryBound(), no_left, selSeq);
    }
    else
    {
        //Not introducing any new left rows => no problem assigning to target selector
        selfCursor.setown(target->getRow(subctx));
    }

    doTransform(subctx, transform, selfCursor);
}

void HqlCppTranslator::doBuildRowAssignCreateRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * transform = expr->queryChild(0);
    if (transform->isConstant())
    {
#ifdef USE_CONSTANT_ROW_FOR_ASSIGN
        //Generally not worthwhile - unless maybe it has a large number of fields....
        CHqlBoundExpr bound;
        if (doBuildRowConstantTransform(transform, bound))
        {
            OwnedHqlExpr raw = bound.getTranslatedExpr();
            buildRowAssign(ctx, target, raw);
            return;
        }
#endif
    }

    Owned<BoundRow> selfCursor = target->getRow(ctx);
    doTransform(ctx, transform, selfCursor);
}
        
void HqlCppTranslator::doBuildRowAssignNullRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    target->buildClear(ctx, 0);
}
        
void HqlCppTranslator::doBuildRowAssignProjectRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * srcRow = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(1);

    Owned<IReferenceSelector> source = buildNewRow(ctx, srcRow);
    BuildCtx subctx(ctx);

    OwnedHqlExpr leftSelect = createSelector(no_left, srcRow, querySelSeq(expr));
    OwnedHqlExpr newTransform = replaceSelector(transform, leftSelect, srcRow);

    Owned<BoundRow> selfCursor = target->getRow(subctx);
    doTransform(subctx, newTransform, selfCursor);
}
        
void HqlCppTranslator::doBuildRowAssignSerializeRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    IHqlExpression * srcRow = expr->queryChild(0);
    IAtom * serializeForm = expr->queryChild(1)->queryName();

    Owned<IReferenceSelector> source = buildNewRow(ctx, srcRow);

    BuildCtx subctx(ctx);

    Owned<BoundRow> leftCursor = source->getRow(subctx);
    BoundRow * selfCursor = bindSelectorAsSelf(subctx, target, expr);
    IHqlExpression * unserializedRecord = srcRow->queryRecord();

    //If builder isn't provided then the target must be a fixed size record that doesn't require serialization
    //Therefore this should never be called.
    assertex(selfCursor->queryBuilder());
    {
        HqlExprArray args;
        args.append(*createSerializer(ctx, unserializedRecord, serializeForm, serializerAtom));
        args.append(*LINK(srcRow));

        Owned<ITypeInfo> type = makeTransformType(expr->queryRecord()->getType());
        OwnedHqlExpr call = bindFunctionCall(rtlSerializeToBuilderId, args, type);
        doTransform(subctx, call, selfCursor);
        //MORE: This doesn't associated the returned size with the target if assigned to a child field.
        //very unusual code, so not too concerned.
    }

    subctx.removeAssociation(selfCursor);
}
        
void HqlCppTranslator::doBuildRowAssignUserTable(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    Owned<BoundRow> selfCursor = target->getRow(ctx);
    doTransform(ctx, expr->queryChild(2), selfCursor);
}


void HqlCppTranslator::buildRowAssign(BuildCtx & ctx, BoundRow * targetRow, IHqlExpression * expr)
{
    //MORE: We should improve assigning a link counted row to a dataset as well.
    //The problem is that currently the dataset constructor is responsible for finializing the rows.
    //which is more compact if the row can't just be appended.  Possibly needs an alwaysCreatesTemp()
    //to help decide.
    IHqlExpression * targetExpr = targetRow->queryBound();
    if (targetRow->isLinkCounted() && hasWrapperModifier(targetExpr->queryType()))
    {
        CHqlBoundTarget target;
        target.expr.set(targetRow->queryBound());

        switch (expr->getOperator())
        {
        //MORE could support no_null, no_if, no_translated, constant no_createrow etc.
        case no_call:
        case no_externalcall:
        case no_getgraphresult:
            buildExprAssign(ctx, target, expr);
            return;
        case no_comma:
        case no_compound:
            buildStmt(ctx, expr->queryChild(0));
            buildRowAssign(ctx, targetRow, expr->queryChild(1));
            return;
        case no_selectnth:
            {
                Owned<IReferenceSelector> src = buildNewRow(ctx, expr);
                Owned<BoundRow> srcRow = src->getRow(ctx);
                BoundRow * linkedRow = ensureLinkCountedRow(ctx, srcRow);
                ctx.addAssignLink(targetRow->queryExpr(), linkedRow->queryExpr());
                return;
            }
        }
    }

    BuildCtx subctx(ctx);
    IHqlStmt * stmt = subctx.addGroup();
    stmt->setIncomplete(true);

    Owned<BoundRow> rowBuilder = createRowBuilder(subctx, targetRow);
    Owned<IReferenceSelector> createdRef = createReferenceSelector(rowBuilder);
    buildRowAssign(subctx, createdRef, expr);
    finalizeTempRow(subctx, targetRow, rowBuilder);

    stmt->setIncomplete(false);
    stmt->mergeScopeWithContainer();
}


void HqlCppTranslator::buildRowAssign(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_temprow:
        throwUnexpected();
    case no_projectrow:
        doBuildRowAssignProjectRow(ctx, target, expr);
        return;
    case no_createrow:
        doBuildRowAssignCreateRow(ctx, target, expr);
        return;
    case no_null:
        doBuildRowAssignNullRow(ctx, target, expr);
        return;
    case no_nofold:
    case no_forcegraph:
    case no_nocombine:
        buildRowAssign(ctx, target, expr->queryChild(0));
        return;
    case no_serialize:
        {
            IHqlExpression * deserialized = expr->queryChild(0);
            IAtom * serializeForm = expr->queryChild(1)->queryName();
            if (isDummySerializeDeserialize(expr))
                buildRowAssign(ctx, target, deserialized->queryChild(0));
            else if (!typeRequiresDeserialization(deserialized->queryType(), serializeForm))
                buildRowAssign(ctx, target, deserialized);
            else
                doBuildRowAssignSerializeRow(ctx, target, expr);
            return;
        }
    case no_if:
        {
            //Assigning a variable size record can mean that references to self need recalculating outside of the condition,
            //producing poor code.
            if (!isVariableSizeRecord(expr->queryRecord()))
            {
                OwnedHqlExpr foldedCond = foldHqlExpression(expr->queryChild(0));
                BuildCtx condctx(ctx);
                IHqlStmt * cond = buildFilterViaExpr(condctx, foldedCond);

                buildRowAssign(condctx, target, expr->queryChild(1));
                condctx.selectElse(cond);
                buildRowAssign(condctx, target, expr->queryChild(2));
                return;
            }
        }
        break;
        /*
    case no_externalcall:
        //MORE: Should assign directly to the target, but may not be very easy....
        if (target->isBinary() && queryRecord(source->queryType()) == expr->queryRecord())
        {
            CHqlBoundExpr address;
            target->buildAddress(ctx, address);
            row.setown(createValue(no_typetransfer, makeReferenceModifier(LINK(selector->queryType())), LINK(address.expr)));
            doBuildCall(ctx, &target, expr, NULL);
        }
        break;
        */
    case no_comma:
    case no_compound:
        buildStmt(ctx, expr->queryChild(0));
        buildRowAssign(ctx, target, expr->queryChild(1));
        return;
    }

    Owned<IReferenceSelector> src = buildNewRow(ctx, expr);
    buildRowAssign(ctx, target, src);
}

void HqlCppTranslator::buildRowAssign(BuildCtx & ctx, IReferenceSelector * target, IReferenceSelector * source)
{
    BuildCtx subctx(ctx);
    IHqlExpression * sourceRecord = ::queryRecord(source->queryType());
    IHqlExpression * targetRecord = ::queryRecord(target->queryType());

    //if record structures are identical, then we must just be able to block copy the information across.
    bool useMemcpy = (sourceRecord == targetRecord) && source->isBinary() && !source->isConditional();

    if (useMemcpy)
    {
        if (source->queryRootRow()->isConditional())
        {
            IHqlStmt * ifStmt = subctx.addFilter(source->queryRootRow()->queryBound());
            target->setRow(subctx, source);
            subctx.selectElse(ifStmt);
            target->buildClear(subctx, 0);
        }
        else
            target->setRow(subctx, source);
    }
    else
        buildCompoundAssign(subctx, targetRecord, target, sourceRecord, source->queryExpr());
}

//---------------------------------------------------------------------------
// Dataset selection

IReferenceSelector * HqlCppTranslator::doBuildRowSelectTop(BuildCtx & ctx, IHqlExpression * expr)
{
    //create a temporary
    Owned<ITypeInfo> rowType = makeReferenceModifier(expr->getType());
    OwnedHqlExpr rowExpr = ctx.getTempDeclare(rowType, NULL);
    Owned<BoundRow> row = createBoundRow(expr, rowExpr);
    ctx.associate(*row);                                    // associate here because it is compared inside the loop

    CHqlBoundExpr boundCleared;
#ifdef CREATE_DEAULT_ROW_IF_NULL
    buildDefaultRow(ctx, expr, boundCleared);
#else
    buildNullRow(ctx, expr, boundCleared);
#endif

    OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);

    //Declare row for final level, iterate the appropriate number of times, and then assign and break.
    BuildCtx initctx(ctx);
    initctx.addGroup();         // add a group to allow a temporary to be declared later.
    initctx.addAssign(rowExpr, defaultRowPtr);

    HqlExprAssociation * savedMarker = ctx.associateExpr(queryConditionalRowMarker(), rowExpr);
    BuildCtx iterctx(ctx);

    IHqlExpression * sort = expr->queryChild(0);
    IHqlExpression * dataset = sort->queryChild(0);
    IHqlExpression * sortList = sort->queryChild(1);
    BoundRow * chooseCursor = buildDatasetIterate(iterctx, dataset, false);

    //if (!best) { best=row; } else { if (next < best) best = row; }  Must short-circuit the test of best
    OwnedHqlExpr testBest = createBoolExpr(no_eq, LINK(rowExpr), LINK(defaultRowPtr));
    IHqlStmt * ifStmt = iterctx.addFilter(testBest);
    {
        OwnedHqlExpr source = ensureIteratedRowIsLive(initctx, ctx, iterctx, chooseCursor, dataset, expr);
        OwnedHqlExpr castLeftRow = createValue(no_implicitcast, LINK(rowType), LINK(source));//chooseCursor->queryBound()));
        iterctx.addAssign(rowExpr, castLeftRow);
    }
    iterctx.selectElse(ifStmt);

    CHqlBoundExpr bound;
    buildOrderedCompare(iterctx, dataset, sortList, bound, dataset, expr);
    OwnedHqlExpr compare = createBoolExpr(no_lt, LINK(bound.expr), getZero());
    iterctx.addFilter(compare);
    {
        OwnedHqlExpr source = ensureIteratedRowIsLive(initctx, ctx, iterctx, chooseCursor, dataset, expr);
        OwnedHqlExpr castLeftRow = createValue(no_implicitcast, LINK(rowType), LINK(source));//chooseCursor->queryBound()));
        iterctx.addAssign(rowExpr, castLeftRow);
    }

    ctx.removeAssociation(savedMarker);
    //Set conditional later on, so test in main loop is explicit
#ifndef CREATE_DEAULT_ROW_IF_NULL
    row->setConditional(true);
#endif
    return createReferenceSelector(row);
}


BoundRow * HqlCppTranslator::buildOptimizeSelectFirstRow(BuildCtx & ctx, IHqlExpression * expr)
{
    BoundRow * parentRow = NULL;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_compound_childaggregate:
        return buildOptimizeSelectFirstRow(ctx, expr->queryChild(0));
    case no_hqlproject:
    case no_newusertable:
        {
            parentRow = buildOptimizeSelectFirstRow(ctx, expr->queryChild(0));
            if (!parentRow)
                return NULL;
        }
        //fall through
    case no_newaggregate:
        {
            Owned<BoundRow> tempRow = declareTempAnonRow(ctx, ctx, expr);
            Owned<BoundRow> rowBuilder = createRowBuilder(ctx, tempRow);

            Owned<IReferenceSelector> createdRef = createReferenceSelector(rowBuilder);

            BuildCtx subctx(ctx);
            subctx.addGroup();
            if (parentRow)
            {
                if (op == no_hqlproject)
                    bindTableCursor(ctx, expr->queryChild(0), tempRow->queryBound(), no_left, querySelSeq(expr));
                else
                    bindTableCursor(ctx, expr->queryChild(0), tempRow->queryBound(), no_none, NULL);
            }
                
            doBuildRowAssignAggregate(subctx, createdRef, expr);
            finalizeTempRow(ctx, tempRow, rowBuilder);

            return tempRow;
        }
        //
    default:
        return NULL;
    }
}


void HqlCppTranslator::convertBoundDatasetToFirstRow(IHqlExpression * expr, CHqlBoundExpr & bound)
{
    Owned<ITypeInfo> type = makeReferenceModifier(expr->getType());
    ITypeInfo * boundType = bound.queryType();
    if (isArrayRowset(boundType))
    {
        Linked<ITypeInfo> rowType = queryUnqualifiedType(queryRowType(boundType));
        rowType.setown(makeReferenceModifier(LINK(rowType)));
        if (hasLinkedRow(boundType))
            type.setown(setLinkCountedAttr(type, true));
        bound.expr.setown(createValue(no_deref, LINK(rowType), LINK(bound.expr)));
    }
    else if (bound.queryType()->isReference())
        bound.expr.setown(createValue(no_typetransfer, LINK(type), bound.expr.getClear()));
    else
        bound.expr.setown(createValue(no_implicitcast, LINK(type), bound.expr.getClear()));
}

void HqlCppTranslator::convertBoundRowToDataset(BuildCtx & ctx, CHqlBoundExpr & bound, const BoundRow * row, ExpressionFormat preferredFormat)
{
    IHqlExpression * boundRow = row->queryBound();
    IHqlExpression * record = row->queryDataset()->queryRecord();
    Owned<ITypeInfo> type = makeTableType(makeRowType(LINK(record->queryType())));
    Owned<ITypeInfo> refType = makeReferenceModifier(LINK(type));
    if (hasLinkCountedModifier(boundRow->queryType()) && (preferredFormat != FormatBlockedDataset))
    {
        OwnedHqlExpr curActivityId = getCurrentActivityId(ctx);
        StringBuffer allocatorName;
        ensureRowAllocator(allocatorName, ctx, record, curActivityId);

        OwnedHqlExpr src = getPointer(boundRow);
        //We can't just take the address of the link counted row, have to create a temporary dataset
        //could be fixed once the link counting is tracked on rows and datasets
        CHqlBoundTarget target;
        createTempFor(ctx, type, target, typemod_none, FormatLinkedDataset);
        StringBuffer s;
        generateExprCpp(s, target.expr).append(".setRow(").append(allocatorName).append(",");
            generateExprCpp(s, src).append(");");
        ctx.addQuoted(s);
        bound.setFromTarget(target);
    }
    else if (boundRow->queryType()->isReference())
        bound.expr.setown(createValue(no_typetransfer, LINK(refType), LINK(boundRow)));
    else
        bound.expr.setown(createValue(no_implicitcast, LINK(refType), LINK(boundRow)));
    bound.count.setown(getSizetConstant(1));
}

IHqlExpression * HqlCppTranslator::ensureIteratedRowIsLive(BuildCtx & initctx, BuildCtx & searchctx, BuildCtx & iterctx, BoundRow * row, IHqlExpression * dataset, IHqlExpression * rowExpr)
{
    //The problem is that we are iterating through the rows in a dataset, and we need to access the "best" row outside of the loop
    //However subsequent iterations of the loop might invalidate the current row, so we need to ensure the best row is retained.
    //There should really be a better way, but it isn't too bad with link counted rows.
    bool needToPreserve = false;
    IHqlExpression * ds = dataset;
    while (ds && !needToPreserve)
    {
        if (ds->isDatarow())
        {
            if (initctx.queryAssociation(ds, AssocRow, NULL))
                break;
        }
        else
        {
            if (initctx.queryMatchExpr(ds))
                break;
        }

        switch (ds->getOperator())
        {
        case no_filter:
        case no_grouped:
        case no_stepped:
        case no_sorted:
        case no_distributed:
        case no_preservemeta:
        case no_choosen:
        case no_selectnth:                      // can occur as the lhs of no_select
        case no_compound_childnormalize:
        case no_compound_selectnew:
        case no_compound_childread:
        case no_dataset_alias:
            ds = ds->queryChild(0);
            break;
        case no_select:
            if (ds->isDataset())
            {
                if (ds->hasAttribute(newAtom))
                {
                    ds = ds->queryChild(0);
                    //don't walk complexds[1].childDataset<new> since the [1] will be generated as a temporary
//                  if (!ds->isDataset() && (ds->getOperator() != no_select))
//                      ds = NULL;
                }
                else
                    ds = NULL;
            }
            else
            {
                //ds.x.y, always walk to to ds.x
                ds = ds->queryChild(0);
            }
            break;
        case no_rows:
        case no_call:
        case no_externalcall:
        case no_getresult:
        case no_getgraphresult:
        case no_alias:
            ds = NULL;
            break;
        default:
            needToPreserve = true;
            break;
        }
    }

    OwnedHqlExpr source = getPointer(row->queryBound());
    if (!needToPreserve || initctx.hasAssociation(*row, false))
        return source.getClear();

    //If link counted, then declare a member that is a link counted row to ensure this row remains linked.
    ITypeInfo * rowType = rowExpr->queryType();
    if (hasLinkCountedModifier(row->queryBound()))
    {
        CHqlBoundTarget saveTarget;
        createTempFor(initctx, rowType, saveTarget, typemod_wrapper, FormatLinkedDataset);
        StringBuffer s;
        generateExprCpp(s, saveTarget.expr);
        s.append(".set(");
        generateExprCpp(s, source).append(");");
        iterctx.addQuoted(s);
        return getPointer(saveTarget.expr);
    }

    BuildCtx childctx(iterctx);
    childctx.addGroup();

    Owned<BoundRow> tempRow = declareTempRow(childctx, childctx, rowExpr);
    Owned<BoundRow> rowBuilder = createRowBuilder(childctx, tempRow);

    OwnedHqlExpr sourceSelector = ensureActiveRow(row->querySelector());
    buildAssign(childctx, rowBuilder->querySelector(), sourceSelector);
    finalizeTempRow(childctx, tempRow, rowBuilder);
    return getPointer(tempRow->queryBound());
}

IReferenceSelector * HqlCppTranslator::buildDatasetIndexViaIterator(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr dataset = normalizeAnyDatasetAliases(querySkipDatasetMeta(expr->queryChild(0)));
    IHqlExpression * index = expr->queryChild(1);
    IHqlExpression * childDataset = dataset;
    switch (dataset->getOperator())
    {
    case no_hqlproject:
        //optimize selectnth(project(rows, t), n) to projectrow(selectnth(rows, n), t)
        IHqlExpression * transform = dataset->queryChild(1);
        if (!containsSkip(transform) && !expr->hasAttribute(_countProject_Atom))
            childDataset = dataset->queryChild(0);
        break;
    }

    //create a temporary
    //Following works because rows are created as temporaries in the class, so still in scope outside the iterate loop.
    //Not a strictly correct assumption - e.g., if ever done in the main process() code.
    Owned<ITypeInfo> rowType = makeReferenceModifier(expr->getType());
    OwnedHqlExpr rowExpr = ctx.getTempDeclare(rowType, NULL);
    Owned<BoundRow> row = createBoundRow(expr, rowExpr);

    CHqlBoundExpr boundCleared;
#ifdef CREATE_DEAULT_ROW_IF_NULL
    buildDefaultRow(ctx, expr, boundCleared);
#else
    buildNullRow(ctx, expr, boundCleared);
    row->setConditional(true);
#endif
    OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);

    //Declare row for final level, iterate the appropriate number of times, and then assign and break.
    BuildCtx initctx(ctx);
    initctx.addGroup();         // add a group to allow a temporary to be declared later.
    initctx.addAssign(rowExpr, defaultRowPtr);
    HqlExprAssociation * savedMarker = ctx.associateExpr(queryConditionalRowMarker(), rowExpr);

    BuildCtx iterctx(ctx);
    bool done = false;
    if (childDataset->getOperator() == no_rows) //hasOutOfLineModifier(dataset->queryType()))
    {
        CHqlBoundExpr boundDs;
        buildDataset(ctx, childDataset, boundDs, FormatNatural);
        if (boundDs.count && isArrayRowset(boundDs.expr->queryType()))
        {
            OwnedHqlExpr castIndex = ensureExprType(index, unsignedType);
            OwnedHqlExpr adjustedIndex = adjustValue(castIndex, -1);
            CHqlBoundExpr boundIndex;
            buildExpr(ctx, adjustedIndex, boundIndex);
            OwnedHqlExpr count = getBoundCount(boundDs);                // could be serialized, so can't assume bound.count is set

            OwnedHqlExpr test = createValue(no_gt, makeBoolType(), LINK(count), LINK(boundIndex.expr));
            iterctx.addFilter(test);
            if (dataset != childDataset)
            {
                Owned<ITypeInfo> datasetRowType = makeRowReferenceType(boundDs);
                OwnedHqlExpr selectedRow = createValue(no_index, LINK(datasetRowType), LINK(boundDs.expr), LINK(boundIndex.expr));
                OwnedHqlExpr projected = createRow(no_projectrow, createTranslated(selectedRow), createComma(LINK(dataset->queryChild(1)), LINK(querySelSeq(dataset))));
                Owned<IReferenceSelector> newRow = buildNewRow(iterctx, projected);
                OwnedHqlExpr newPtr = getPointer(newRow->queryRootRow()->queryBound());
                iterctx.addAssign(rowExpr, newPtr);
            }
            else
            {
                OwnedHqlExpr selectedRow = createValue(no_index, LINK(rowType), LINK(boundDs.expr), LINK(boundIndex.expr));
                iterctx.addAssign(rowExpr, selectedRow);
            }
            done = true;
        }
    }

    if (!done)
    {
        BoundRow * chooseCursor;
        //If choosing the first element, then no need to maintain a counter...
        IValue * indexValue = index->queryValue();
        if (indexValue && (indexValue->getIntValue() == 1))
            chooseCursor = buildDatasetIterate(iterctx, dataset, true);
        else
            chooseCursor = buildDatasetIterate(iterctx, expr, true);
        if (chooseCursor)
        {
            OwnedHqlExpr source = getPointer(chooseCursor->queryBound());

            //MORE: Need casts because cursor may be (probably are) constant, but temporary isn't
            //should somehow fnd out by looking at the cursors.
            OwnedHqlExpr castLeftRow = createValue(no_implicitcast, LINK(rowType), LINK(source));
            iterctx.addAssign(rowExpr, castLeftRow);
            iterctx.addBreak();
        }
    }
    ctx.removeAssociation(savedMarker);
    ctx.associate(*row);
    return createReferenceSelector(row);
}

IReferenceSelector * HqlCppTranslator::buildDatasetIndex(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprAssociation * match = ctx.queryAssociation(expr, AssocRow, NULL);
    if (match)
        return createReferenceSelector(static_cast<BoundRow *>(match));

#if 0
    //Causes some queries (ncf10) to run out of memory, so disable for the moment.
    OwnedHqlExpr optimized = optimizeHqlExpression(expr, getOptimizeFlags()|HOOcompoundproject);
    if (optimized != expr)
        return buildNewRow(ctx, optimized);
#endif

    OwnedHqlExpr dataset = normalizeAnyDatasetAliases(expr->queryChild(0));

    //Special cases:
    //i) selecting row [1] from something that only has a single row
    //ii) selecting row [n] from something that can be iterated.
    //iii) row[1] from something sorted that can be iterated.
    BoundRow * row = NULL;
    if (isTrivialSelectN(expr))
    {
        BoundRow * row = NULL;
#if 0
        //This could be a good idea - but again it can mess up cse since dataset never gets bound.
        //Could enable if I implement cse on datasets within transforms.
//          if (canIterateInline(&ctx, dataset))
//              row = buildOptimizeSelectFirstRow(ctx, dataset);
#endif
        if (!row)
        {
            CHqlBoundExpr bound;
            buildDataset(ctx, dataset, bound, FormatNatural);
            convertBoundDatasetToFirstRow(expr, bound);
            row = bindRow(ctx, expr, bound.expr);
        }
        return createReferenceSelector(row);
    }
    else if (canIterateInline(&ctx, dataset))
    {
        //MORE? Following doesn't work for implicit normalize which iterates multiple levels
        bool specialCase = false;
        dataset.set(querySkipDatasetMeta(dataset));
        
        switch (dataset->getOperator())
        {
        case no_select:
            specialCase = !isMultiLevelDatasetSelector(expr, false);
            break;
        case no_if:
        case no_createdictionary:
        case no_inlinetable:
        case no_join:
            //Always creates a temporary, so don't use an iterator
            specialCase = true;
            break;
        default:
            specialCase = alwaysEvaluatesToBound(dataset) || hasSingleRow(dataset) || !canIterateInline(&ctx, dataset);
            break;
        }

        if (!specialCase)
            return buildDatasetIndexViaIterator(ctx, expr);
    }
    else if (isSelectSortedTop(expr) && canIterateInline(&ctx, dataset->queryChild(0)))
    {
        return doBuildRowSelectTop(ctx, expr);
    }
    //MORE: Is this a good idea???
    else if (!canProcessInline(&ctx, expr))
    {
        CHqlBoundExpr bound;
        OwnedHqlExpr dsExpr = expr->isDatarow() ? createDatasetFromRow(LINK(expr)) : LINK(expr);
        buildDataset(ctx, dsExpr, bound, FormatNatural);
        convertBoundDatasetToFirstRow(expr, bound);
        row = bindRow(ctx, expr, bound.expr);
    }

    if (!row)
    {
        Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, dataset);
        row = cursor->buildSelectNth(ctx, expr);

        if (!row)
        {
            CHqlBoundExpr boundCleared;
            buildDefaultRow(ctx, dataset, boundCleared);
            OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
            row = bindRow(ctx, expr, defaultRowPtr);
        }
    }
    return createReferenceSelector(row);
}

IReferenceSelector * HqlCppTranslator::buildDatasetSelectMap(BuildCtx & ctx, IHqlExpression * expr)
{
    HqlExprAssociation * match = ctx.queryAssociation(expr, AssocRow, NULL);
    if (match)
        return createReferenceSelector(static_cast<BoundRow *>(match));

    OwnedHqlExpr dictionary = normalizeAnyDatasetAliases(expr->queryChild(0));

    //MORE: This should really be a createDictionarySelector call.
    Owned<IHqlCppDatasetCursor> cursor = createDatasetSelector(ctx, dictionary);
    Owned<BoundRow> row = cursor->buildSelectMap(ctx, expr);

    if (!row)
    {
        CHqlBoundExpr boundCleared;
        buildDefaultRow(ctx, dictionary, boundCleared);
        OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
        row.setown(bindRow(ctx, expr, defaultRowPtr));
    }

    return createReferenceSelector(row);
}

//---------------------------------------------------------------------------

IHqlExpression * HqlCppTranslator::buildGetLocalResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * graphId = expr->queryChild(1);
    IHqlExpression * resultNum = expr->queryChild(2);
    Linked<ITypeInfo> exprType = queryUnqualifiedType(expr->queryType());
    if (!hasLinkCountedModifier(exprType))
        exprType.setown(makeAttributeModifier(LINK(exprType), getLinkCountedAttr()));

    if (expr->hasAttribute(externalAtom))
    {
        IHqlExpression * resultInstance = queryAttributeChild(expr, externalAtom, 0);
        HqlExprAssociation * matchedResults = ctx.queryMatchExpr(resultInstance);
        if (!matchedResults)
        {
            //Very unusual - a result is required from a child query, but that child query is actually in
            //the parent/grandparent.  We need to evaluate in the parent instead.
            CHqlBoundExpr match;
            if (!buildExprInCorrectContext(ctx, expr, match, false))
                throwUnexpected();
            return match.getTranslatedExpr();
        }

        HqlExprArray args;
        args.append(*LINK(matchedResults->queryExpr()));
        args.append(*LINK(resultNum));
        if (expr->isDictionary())
            return bindFunctionCall(getChildQueryDictionaryResultId, args, exprType);
        if (expr->isDatarow())
            return bindFunctionCall(getChildQueryLinkedRowResultId, args, exprType);
        return bindFunctionCall(getChildQueryLinkedResultId, args, exprType);
    }

    assertex(activeActivities.ordinality());
    queryAddResultDependancy(activeActivities.tos(), graphId, resultNum);

    SubGraphInfo * activeSubgraph = queryActiveSubGraph(ctx);
    assertex(activeSubgraph && graphId == activeSubgraph->graphTag);
    unique_id_t id = activeSubgraph->graphId;

    EvalContext * instance = queryEvalContext(ctx);
    OwnedHqlExpr retInstanceExpr;
    if (instance && !insideOnCreate(ctx))
        retInstanceExpr.setown(instance->createGraphLookup(id, false));
    else
        retInstanceExpr.setown(doCreateGraphLookup(ctx, ctx, id, "this", true));

    HqlExprArray args;
    args.append(*LINK(retInstanceExpr));
    args.append(*LINK(resultNum));
    if (expr->isDictionary())
        return bindFunctionCall(getLocalDictionaryResultId, args, exprType);
    if (expr->isDatarow())
        return bindFunctionCall(getLocalLinkedRowResultId, args, exprType);
    return bindFunctionCall(getLocalLinkedResultId, args, exprType);
}

void HqlCppTranslator::doBuildAssignGetGraphResult(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    if (expr->hasAttribute(_streaming_Atom))
    {
        if (insideLibrary())
            throwError(HQLERR_StreamInputUsedDirectly);
        else
            throwError(HQLERR_LoopTooComplexForParallel);
    }

    if (expr->hasAttribute(externalAtom))
    {
        OwnedHqlExpr call = buildGetLocalResult(ctx, expr);
        buildExprAssign(ctx, target, call);
        return;
    }

    if (!isCurrentActiveGraph(ctx, expr->queryChild(1)))
    {
        CHqlBoundExpr match;
        if (!buildExprInCorrectContext(ctx, expr, match, false))
            throwError(HQLERR_GraphContextNotFound);

        assign(ctx, target, match);
        return;
    }

    OwnedHqlExpr call = buildGetLocalResult(ctx, expr);
    buildExprAssign(ctx, target, call);
}


void HqlCppTranslator::doBuildExprGetGraphResult(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format)
{
    if (!expr->hasAttribute(externalAtom) && (!isCurrentActiveGraph(ctx, expr->queryChild(1)) || !insideOnStart(ctx)))
    {
        doBuildAliasValue(ctx, expr, tgt, NULL);
        return;

        if (!isCurrentActiveGraph(ctx, expr->queryChild(1)))
        {
            if (!buildExprInCorrectContext(ctx, expr, tgt, false))
                throwError(HQLERR_GraphContextNotFound);
            return;
        }
    }

    OwnedHqlExpr call = buildGetLocalResult(ctx, expr);
    switch (expr->queryType()->getTypeCode())
    {
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        buildTempExpr(ctx, call, tgt);
        break;
    default:
        buildExpr(ctx, call, tgt);
        break;
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityGetGraphResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * graphId = expr->queryChild(1);
    IHqlExpression * resultNum = expr->queryChild(2);
    ThorActivityKind activityKind = (expr->hasAttribute(_streaming_Atom) ? TAKlocalstreamread : TAKlocalresultread);

    bool useImplementationClass = options.minimizeActivityClasses && (resultNum->getOperator() == no_constant);
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, activityKind, expr, "LocalResultRead");
    if (useImplementationClass)
        instance->setImplementationClass(newLocalResultReadArgId);
    if (expr->hasAttribute(_loop_Atom))
    {
        if (isCurrentActiveGraph(ctx, graphId))
            instance->graphLabel.set("Begin Loop");
        else
            instance->graphLabel.set("Outer Loop Input");
    }
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    if (!useImplementationClass)
        doBuildUnsignedFunction(instance->classctx, "querySequence", resultNum);
    else
        instance->addConstructorParameter(resultNum);

    addGraphIdAttribute(instance, ctx, graphId);
    buildInstanceSuffix(instance);

    queryAddResultDependancy(*instance->queryBoundActivity(), graphId, resultNum);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivitySetGraphDictionaryResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dictionary = expr->queryChild(0);
    IHqlExpression * dataset = dictionary->queryChild(0);
    IHqlExpression * graphId = expr->queryChild(1);
    IHqlExpression * resultNum = expr->queryChild(2);
    bool isSpill = expr->hasAttribute(_spill_Atom);

    ABoundActivity * parentActivity = activeActivities.ordinality() ? &activeActivities.tos() : NULL;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKdictionaryresultwrite, expr, "DictionaryResultWrite");

    buildActivityFramework(instance, isRoot && !isSpill);

    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->classctx, "querySequence", resultNum);
    doBuildBoolFunction(instance->classctx, "usedOutsideGraph", !isSpill);

    if (parentActivity && !insideRemoteGraph(ctx) && !isSpill)
    {
        addDependency(ctx, instance->queryBoundActivity(), parentActivity, childAtom, "Child");
    }

    buildDictionaryHashMember(instance->createctx, dictionary, "queryHashLookupInfo");

    instance->addAttributeBool("_isSpill", isSpill);
    if (targetRoxie())
        addGraphIdAttribute(instance, ctx, graphId);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    associateRemoteResult(*instance, graphId, resultNum);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivitySetGraphResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset = expr->queryChild(0);
    if (dataset->isDictionary())
        return doBuildActivitySetGraphDictionaryResult(ctx, expr, isRoot);

    IHqlExpression * graphId = expr->queryChild(1);
    IHqlExpression * resultNum = expr->queryChild(2);
    bool isSpill = expr->hasAttribute(_spill_Atom);

    ABoundActivity * parentActivity = activeActivities.ordinality() ? &activeActivities.tos() : NULL;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    bool useImplementationClass = options.minimizeActivityClasses;
    Owned<ActivityInstance> instance;
    if (expr->getOperator() == no_spillgraphresult)
    {
        instance.setown(new ActivityInstance(*this, ctx, TAKlocalresultspill, expr, "LocalResultSpill"));
    }
    else
    {
        instance.setown(new ActivityInstance(*this, ctx, TAKlocalresultwrite, expr, "LocalResultWrite"));
    }

    if (useImplementationClass)
        instance->setImplementationClass(newLocalResultSpillArgId);

    if (expr->hasAttribute(_loop_Atom))
        instance->graphLabel.set("End Loop");
    buildActivityFramework(instance, isRoot && !isSpill);

    buildInstancePrefix(instance);

    if (!useImplementationClass)
    {
        doBuildUnsignedFunction(instance->classctx, "querySequence", resultNum);
        doBuildBoolFunction(instance->classctx, "usedOutsideGraph", !isSpill);
    }
    else
    {
        instance->addConstructorParameter(resultNum);
        instance->addConstructorParameter(queryBoolExpr(!isSpill));
    }

    if (parentActivity && !insideRemoteGraph(ctx) && !isSpill)
    {
        const char * relationship;
        if (expr->hasAttribute(_loop_Atom))
            relationship = "Body";
        else if (insideRemoteGraph(ctx))
            relationship = "Remote";
        else
            relationship = "Child";

        addDependency(ctx, instance->queryBoundActivity(), parentActivity, childAtom, relationship);
    }

    instance->addAttributeBool("_isSpill", isSpill);
    instance->addAttributeBool("_fromChild", expr->hasAttribute(_accessedFromChild_Atom));
    if (targetRoxie())
        addGraphIdAttribute(instance, ctx, graphId);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    associateRemoteResult(*instance, graphId, resultNum);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivityReturnResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot)
{
    IHqlExpression * dataset = expr->queryChild(0);

    ABoundActivity * parentActivity = activeActivities.ordinality() ? &activeActivities.tos() : NULL;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    ThorActivityKind kind;
    const char * helper;
    if (dataset->isDataset())
    {
        kind = TAKdatasetresult;
        helper = "DatasetResult";
    }
    else
    {
        kind = TAKrowresult;
        helper = "RowResult";
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, helper);
    buildActivityFramework(instance, isRoot);

    buildInstancePrefix(instance);
    if (parentActivity && !insideRemoteGraph(ctx))
        addDependency(ctx, instance->queryBoundActivity(), parentActivity, childAtom, "Child");
    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


void HqlCppTranslator::doBuildAssignLoopCounter(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr)
{
    if (!isCurrentActiveGraph(ctx, expr->queryChild(0)))
    {
        CHqlBoundExpr match;
        if (!buildExprInCorrectContext(ctx, expr, match, false))
            throwError(HQLERR_GraphContextNotFound);

        assign(ctx, target, match);
        return;
    }

    HqlExprArray args;
    OwnedHqlExpr call = bindFunctionCall(getGraphLoopCounterId, args);
    buildExprAssign(ctx, target, call);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityGetGraphLoopResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * graphId = expr->queryChild(1);
    IHqlExpression * resultNum = expr->queryChild(2);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKgraphloopresultread, expr, "GraphLoopResultRead");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    doBuildUnsignedFunction(instance->startctx, "querySequence", resultNum);
    addGraphIdAttribute(instance, ctx, graphId);
    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}


ABoundActivity * HqlCppTranslator::doBuildActivitySetGraphLoopResult(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * graphId = expr->queryChild(1);
    bool isSpill = expr->hasAttribute(_spill_Atom);

    ABoundActivity * parentActivity = activeActivities.ordinality() ? &activeActivities.tos() : NULL;
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, dataset);

    bool useImplementationClass = options.minimizeActivityClasses;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKgraphloopresultwrite, expr, "GraphLoopResultWrite");
    if (useImplementationClass)
        instance->setImplementationClass(newGraphLoopResultWriteArgId);

    buildActivityFramework(instance, true);

    buildInstancePrefix(instance);
    if (parentActivity && !insideRemoteGraph(ctx) && !isSpill)
        addDependency(ctx, instance->queryBoundActivity(), parentActivity, childAtom, "Body");
    if (targetRoxie())
        addGraphIdAttribute(instance, ctx, graphId);

    buildInstanceSuffix(instance);

    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

static IHqlExpression * queryResultExpr(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_compound:
            expr = expr->queryChild(1);
            break;
        case no_subgraph:
            expr = expr->queryChild(0);
            break;
        case no_returnresult:
            return expr;
        default:
            throwUnexpectedOp(expr->getOperator());
        }
    }
}


ABoundActivity * HqlCppTranslator::doBuildActivityForceLocal(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    if (targetHThor() || (targetThor() && !insideChildQuery(ctx)))
    {
        WARNING(CategoryIgnored, HQLWRN_LocalHasNoEffect);
        return buildCachedActivity(ctx, child);
    }

    OwnedHqlExpr result = createValue(no_returnresult, makeVoidType(), LINK(child));
    OwnedHqlExpr remote = resourceThorGraph(*this, result, RoxieCluster, 1, NULL);
    unique_id_t localId = doBuildThorSubGraph(ctx, remote, SubGraphRemote);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKlocalgraph, expr, "Null");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);
    instance->addAttributeInt("_subgraph", localId);

    ActivityAssociation * match = static_cast<ActivityAssociation *>(ctx.queryAssociation(queryResultExpr(remote), AssocActivity, NULL));
    assertex(match);
    addDependency(ctx, match->activity, instance->queryBoundActivity(), childAtom);

    buildInstanceSuffix(instance);

    return instance->getBoundActivity();
}


void HqlCppTranslator::doBuildStmtApply(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * start = expr->queryAttribute(beforeAtom);
    IHqlExpression * end = expr->queryAttribute(afterAtom);

    if (start)
        buildStmt(ctx, start->queryChild(0));
    BuildCtx condctx(ctx);
    buildDatasetIterate(condctx, dataset, false);
    unsigned max = expr->numChildren();
    for (unsigned i=1; i < max; i++)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isAttribute())
            buildStmt(condctx, cur);
    }

    if (end)
        buildStmt(ctx, end->queryChild(0));
}
