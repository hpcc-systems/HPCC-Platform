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
#include "jhash.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

#include "hql.hpp"
#include "hqlattr.hpp"
#include "hqlfold.hpp"
#include "hqlthql.hpp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"
#include "hqlcpp.ipp"
#include "hqlcatom.hpp"
#include "hqlcpputil.hpp"
#include "hqlgraph.ipp"
#include "hqllib.ipp"
#include "hqlwcpp.hpp"
#include "hqlttcpp.ipp"
#include "hqlinline.hpp"

static IHqlExpression * queryLibraryInputSequence(IHqlExpression * expr)
{
    IHqlExpression * arg = expr->queryChild(0);
    if (arg->isRecord())
        arg = expr->queryChild(1);
    return arg;
}

HqlCppLibrary::HqlCppLibrary(HqlCppTranslator & _translator, IHqlExpression * libraryInterface, ClusterType _clusterType) : translator(_translator), clusterType(_clusterType), inputMapper(libraryInterface)
{
    assertex(libraryInterface->getOperator() == no_funcdef);
    scopeExpr = libraryInterface->queryChild(0);
    assertex(scopeExpr->getOperator() == no_virtualscope);

    streamedCount = 0;
    if (clusterType != HThorCluster)
        streamedCount = inputMapper.numStreamedInputs();

    extractOutputs();
}


void HqlCppLibrary::extractOutputs()
{
    HqlExprArray symbols;
    scopeExpr->queryScope()->getSymbols(symbols);

    IHqlScope * scope = scopeExpr->queryScope();
    HqlDummyLookupContext dummyctx(NULL);
    ForEachItemIn(i, symbols)
    {
        IHqlExpression & cur = symbols.item(i);
        if (isExported(&cur))
        {
            IIdAtom * id = cur.queryId();
            OwnedHqlExpr value = scope->lookupSymbol(id, LSFpublic, dummyctx);

            if (value && !value->isFunction())
            {
                if (value->isDataset() || value->isDatarow() || value->queryType()->isScalar())
                {
                    OwnedHqlExpr null = createNullExpr(value);
                    outputs.append(*cur.cloneAllAnnotations(null));
                }
            }
        }
    }
    outputs.sort(compareSymbolsByName);
}

unsigned HqlCppLibrary::getInterfaceHash() const
{
    unsigned crc = getHash(outputs, 0x12345678);            // only hashes type and name, not implementation
    crc = getHash(inputMapper.queryRealParameters(), crc);
    if (translator.queryOptions().implicitLinkedChildRows)
        crc ^= 0x456271;
    if (crc == 0) crc = 0x87654321;                         // ensure it is non zero.
    return crc;
}

unsigned HqlCppLibrary::getHash(const HqlExprArray & values, unsigned crc) const
{
    unsigned num = values.ordinality();
    crc = hashc((const byte *)&num, sizeof(num), crc);
    ForEachItemIn(i, values)
    {
        IHqlExpression & cur = values.item(i);

        //names are significant because inputs/outputs are ordered by name
        const char * name = str(cur.queryName());
        crc = hashnc((const byte *)name, strlen(name), crc);

        ITypeInfo * type = cur.queryType();
        byte tc = type->getTypeCode();
        switch (tc)
        {
        case type_record:
        case type_row:
        case type_table:
        case type_groupedtable:
        case type_dictionary:
            {
                OwnedHqlExpr normalizedRecord = normalizeExpression(translator, cur.queryRecord());
                OwnedHqlExpr serialized = getSerializedForm(normalizedRecord, diskAtom);
                unsigned recordCrc = getExpressionCRC(serialized);
                crc = hashc((const byte *)&tc, sizeof(tc), crc);
                crc = hashc((const byte *)&recordCrc, sizeof(recordCrc), crc);
                break;
            }
        default:
            {
                size32_t size = type->getSize();
                crc = hashc((const byte *)&tc, sizeof(tc), crc);
                crc = hashc((const byte *)&size, sizeof(size), crc);
                break;
            }
        }
    }
    return crc;
}


unsigned HqlCppLibrary::queryOutputIndex(IAtom * name) const
{
    ForEachItemIn(i, outputs)
    {
        if (outputs.item(i).queryName() == name)
            return i;
    }
    return NotFound;
}


HqlCppLibraryImplementation::HqlCppLibraryImplementation(HqlCppTranslator & _translator, IHqlExpression * libraryInterface, IHqlExpression * _libraryId, ClusterType _clusterType)
: HqlCppLibrary(_translator, libraryInterface, _clusterType), libraryId(_libraryId)
{
    inputMapper.mapRealToLogical(inputExprs, logicalParams, libraryId, (clusterType != HThorCluster), translator.targetThor());
}


HqlCppLibraryInstance::HqlCppLibraryInstance(HqlCppTranslator & translator, IHqlExpression * _instanceExpr, ClusterType clusterType) : instanceExpr(_instanceExpr)
{
    assertex(instanceExpr->getOperator() == no_libraryscopeinstance);
    libraryFuncdef = instanceExpr->queryDefinition();
    assertex(libraryFuncdef->getOperator() == no_funcdef);
    IHqlExpression * libraryScope = libraryFuncdef->queryChild(0);
    assertex(libraryScope->getOperator() == no_libraryscope);
    IHqlExpression * libraryInterface = queryAttributeChild(libraryScope, implementsAtom, 0);
    assertex(libraryInterface);

    library.setown(new HqlCppLibrary(translator, libraryInterface, clusterType));

    assertex(library->totalInputs() == (libraryFuncdef->queryChild(1)->numChildren()));
}



//---------------------------------------------------------------------------

static HqlTransformerInfo hqlLibraryTransformerInfo("HqlLibraryTransformer");
class HqlLibraryTransformer: public QuickHqlTransformer
{
public:
    HqlLibraryTransformer(IConstWorkUnit * _wu, bool _isLibrary) 
        : QuickHqlTransformer(hqlLibraryTransformerInfo, NULL), wu(_wu)
    {
        ignoreFirstScope = _isLibrary;
    }

    IHqlExpression * doTransformLibrarySelect(IHqlExpression * expr)
    {
        //Map the new attribute
        IHqlExpression * oldModule = expr->queryChild(1);
        OwnedHqlExpr newModule = transform(oldModule);
        if (oldModule == newModule)
            return LINK(expr);

        IIdAtom * id = expr->queryChild(3)->queryId();
        HqlDummyLookupContext dummyctx(NULL);
        OwnedHqlExpr value = newModule->queryScope()->lookupSymbol(id, makeLookupFlags(true, expr->hasAttribute(ignoreBaseAtom), false), dummyctx);
        assertex(value != NULL);
        IHqlExpression * oldAttr = expr->queryChild(2);
        if (oldAttr->isDataset() || oldAttr->isDatarow())
            return value.getClear();

        assertex(value->isDataset());
        IHqlExpression * field = value->queryRecord()->queryChild(0);
        OwnedHqlExpr select = createRow(no_selectnth, value.getClear(), createConstantOne());
        return createSelectExpr(select.getClear(), LINK(field));        // no newAtom because not normalised yet
    }

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_libraryselect:
            return doTransformLibrarySelect(expr);
        }

        return QuickHqlTransformer::createTransformedBody(expr);
    }

protected:
    IHqlExpression * createSimplifiedLibrary(IHqlExpression * libraryExpr)
    {
        IHqlScope * oldScope = libraryExpr->queryScope();
        IHqlScope * lookupScope = oldScope;
        IHqlExpression * interfaceExpr = queryAttributeChild(libraryExpr, implementsAtom, 0);
        if (interfaceExpr)
            lookupScope = interfaceExpr->queryScope();

        HqlExprArray symbols, newSymbols;
        lookupScope->getSymbols(symbols);

        HqlDummyLookupContext dummyctx(NULL);
        ForEachItemIn(i, symbols)
        {
            IHqlExpression & cur = symbols.item(i);
            if (isExported(&cur))
            {
                IIdAtom * id = cur.queryId();
                OwnedHqlExpr oldSymbol = oldScope->lookupSymbol(id, LSFpublic, dummyctx);
                OwnedHqlExpr newValue;
                ITypeInfo * type = oldSymbol->queryType();
                if (oldSymbol->isDataset() || oldSymbol->isDatarow())
                {
                    newValue.setown(createNullExpr(oldSymbol));
                }
                else
                {
                    //Convert a scalar to a select from a dataset
                    OwnedHqlExpr field = createField(unknownId, LINK(type), NULL, NULL);
                    OwnedHqlExpr newRecord = createRecord(field);
                    OwnedHqlExpr ds = createDataset(no_null, LINK(newRecord));
                    newValue.setown(createNullExpr(ds));
                }

                if (oldSymbol->isFunction())
                {
                    throwUnexpected();
#if 0
                    // Should have been caught in the parser..  Following code should be correct if we ever work out how to implement
                    HqlExprArray parms;
                    unwindChildren(parms, oldSymbol, 1);
                    IHqlExpression * formals = createSortList(parms);
                    newValue.setown(createFunctionDefinition(id, newValue.getClear(), formals, NULL, NULL));
#endif
                }

                IHqlExpression * newSym = oldSymbol->cloneAllAnnotations(newValue);
                newSymbols.append(*newSym);
            }
        }

        HqlExprArray children;
        unwindChildren(children, libraryExpr);
        return queryExpression(oldScope->clone(children, newSymbols));
    }

protected:
    IConstWorkUnit * wu;
    bool ignoreFirstScope;
};


class HqlEmbeddedLibraryTransformer: public HqlLibraryTransformer
{
public:
    HqlEmbeddedLibraryTransformer(IConstWorkUnit * _wu, HqlExprArray & _internalLibraries, bool _isLibrary) 
        : HqlLibraryTransformer(_wu, _isLibrary), internalLibraries(_internalLibraries)
    {
    }

    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_libraryscopeinstance:
            {
                IHqlExpression * scopeFunc = expr->queryDefinition();
                assertex(scopeFunc->getOperator() == no_funcdef);
                IHqlExpression * moduleExpr = scopeFunc->queryChild(0);
                assertex(moduleExpr->getOperator() == no_libraryscope);
                IHqlExpression * internalExpr = moduleExpr->queryAttribute(internalAtom);
                if (internalExpr)
                {
                    IHqlExpression * nameExpr = moduleExpr->queryAttribute(nameAtom);
                    unsigned sequence = matchedInternalLibraries.find(*nameExpr);
                    if (sequence == NotFound)
                    {
                        sequence = internalLibraries.ordinality();
                        internalLibraries.append(*transformEmbeddedLibrary(scopeFunc));
                        matchedInternalLibraries.append(*LINK(nameExpr));
                    }
                    //remove the parameter from the internal attribute from the module that is being called.
                    //so save in subsequent translation
                    VStringBuffer graphName("graph%u", EMBEDDED_GRAPH_DELTA+(sequence+1));
                    OwnedHqlExpr noInternalExpr = removeAttribute(moduleExpr, internalAtom);
                    OwnedHqlExpr newModuleExpr = appendOwnedOperand(noInternalExpr, createAttribute(embeddedAtom, createConstant(graphName)));
                    OwnedHqlExpr newScopeFunc = replaceChild(scopeFunc, 0, newModuleExpr);
                    HqlExprArray instanceArgs;
                    unwindChildren(instanceArgs, expr);
                    return createLibraryInstance(LINK(newScopeFunc), instanceArgs);
                }
                else
                    return LINK(expr);
                //otherwise already transformed....
                break;
            }
        case no_libraryscope:
            if (!ignoreFirstScope)
            {
                //Now create a simplified no_library scope with null values for each of the values of the exported symbols
                //Don't walk contents because that is an implementation detail, which could change
                return createSimplifiedLibrary(expr);
            }
            ignoreFirstScope = false;
            break;
        }

        return HqlLibraryTransformer::createTransformedBody(expr);
    }

    IHqlExpression * transformEmbeddedLibrary(IHqlExpression * expr)
    {
        //avoid special casing above
        assertex(expr->getOperator() == no_funcdef);
        IHqlExpression * library = expr->queryChild(0);
        HqlExprArray args;
        args.append(*QuickHqlTransformer::createTransformedBody(library));
        unwindChildren(args, expr, 1);
        return expr->clone(args);
    }

protected:
    HqlExprArray & internalLibraries;
    HqlExprArray matchedInternalLibraries;
};

//---------------------------------------------------------------------------


void HqlCppTranslator::processEmbeddedLibraries(HqlExprArray & exprs, HqlExprArray & internalLibraries, bool isLibrary)
{
    HqlExprArray transformed;

    HqlEmbeddedLibraryTransformer transformer(wu(), internalLibraries, isLibrary);
    ForEachItemIn(i, exprs)
        transformed.append(*transformer.transform(&exprs.item(i)));

    replaceArray(exprs, transformed);
}

//---------------------------------------------------------------------------

static IHqlExpression * splitSelect(IHqlExpression * & rootDataset, IHqlExpression * expr, IHqlExpression * selSeq)
{
    assertex(expr->getOperator() == no_select);
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * field = expr->queryChild(1);

    IHqlExpression * attrSelector;
    if (ds->isDatarow() && (ds->getOperator() == no_select))
    {
        attrSelector = splitSelect(rootDataset, ds, selSeq);
    }
    else
    {
        rootDataset = ds;
        attrSelector = createSelector(no_left, ds, selSeq);
    }
    return createSelectExpr(attrSelector, LINK(field));
}

static IHqlExpression * convertScalarToDataset(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case NO_AGGREGATE:
        return convertScalarAggregateToDataset(expr);
    case no_select:
        {
            //Project dataset down to a single field...
            IHqlExpression * ds;
            OwnedHqlExpr selSeq = createSelectorSequence();
            OwnedHqlExpr newExpr = splitSelect(ds, expr, selSeq);
            IHqlExpression * field = expr->queryChild(1);
            OwnedHqlExpr record = createRecord(field);
            OwnedHqlExpr assign = createAssign(createSelectExpr(createSelector(no_self, record, NULL), LINK(field)), LINK(newExpr));
            OwnedHqlExpr row = createRow(no_projectrow, { LINK(ds), createValue(no_transform, makeTransformType(record->getType()), LINK(assign)), LINK(selSeq) });
            return LINK(row);
            //Following is more strictly correct, but messes up the resourcing.
            //return createDatasetFromRow(LINK(row));
        }
        break;
    }

    OwnedHqlExpr field = createField(valueId, expr->getType(), NULL, NULL);
    OwnedHqlExpr record = createRecord(field);
    OwnedHqlExpr assign = createAssign(createSelectExpr(createSelector(no_self, record, NULL), LINK(field)), LINK(expr));
    OwnedHqlExpr row = createRow(no_createrow, createValue(no_transform, makeTransformType(record->getType()), LINK(assign)));
    return createDatasetFromRow(LINK(row));
}

void HqlCppLibraryImplementation::mapLogicalToImplementation(HqlExprArray & exprs, IHqlExpression * libraryExpr)
{
    //First replace parameters with streamed inputs, and no_libraryinputs, by creating some psuedo-arguments,
    //and then binding them.
    HqlExprArray actuals;
    appendArray(actuals, logicalParams);
    OwnedHqlExpr bound = createBoundFunction(NULL, libraryExpr, actuals, NULL, true);
    IHqlScope * scope = bound->queryScope();
    assertex(scope);

    //Now resolve each of the outputs in the transformed module
    HqlDummyLookupContext dummyctx(NULL);
    unsigned numInputs = numStreamedInputs();
    ForEachItemIn(i, outputs)
    {
        IHqlExpression & curOutput = outputs.item(i);
        OwnedHqlExpr output = scope->lookupSymbol(curOutput.queryId(), LSFpublic, dummyctx);

        // Do a global replace of input(n) with no_getgraphresult(n), and no_param with no_
        if (!output->isDatarow() && !output->isDataset())
            output.setown(convertScalarToDataset(output));
        
        HqlExprArray args;
        args.append(*LINK(output));
        args.append(*LINK(libraryId));
        args.append(*getSizetConstant(i+numInputs));
        exprs.append(*createValue(no_setgraphresult, makeVoidType(), args));
    }

}

//---------------------------------------------------------------------------

void ThorBoundLibraryActivity::noteOutputUsed(IAtom * name)
{
    unsigned matchIndex = libraryInstance->library->queryOutputIndex(name);
    assertex(matchIndex != NotFound);
    addGraphAttributeInt(graphNode, "_outputUsed", matchIndex);
}

ABoundActivity * HqlCppTranslator::doBuildActivityLibrarySelect(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * module = expr->queryChild(1);

    ThorBoundLibraryActivity * callInstance = static_cast<ThorBoundLibraryActivity *>(buildCachedActivity(ctx, module));

    callInstance->noteOutputUsed(expr->queryChild(3)->queryName());

    return callInstance;
}


//---------------------------------------------------------------------------

void HqlCppTranslator::buildLibraryInstanceExtract(BuildCtx & ctx, HqlCppLibraryInstance * libraryInstance)
{
    MemberFunction func(*this, ctx, "virtual void createParentExtract(rtlRowBuilder & builder) override");

    BuildCtx beforeBuilderCtx(func.ctx);
    beforeBuilderCtx.addGroup();
    Owned<ParentExtract> extractBuilder = createExtractBuilder(func.ctx, PETlibrary, NULL, GraphNonLocal, false);

    StringBuffer s;
    s.append("rtlRowBuilder & ");
    generateExprCpp(s, extractBuilder->queryExtractName());
    s.append(" = builder;");
    beforeBuilderCtx.addQuoted(s);

    beginExtract(func.ctx, extractBuilder);

    //Ensure all the values are added to the serialization in the correct order 
    CHqlBoundExpr dummyTarget;
    unsigned numParams = libraryInstance->numParameters();
    for (unsigned i2 = libraryInstance->numStreamedInputs(); i2 < numParams; i2++)
    {
        IHqlExpression * parameter = libraryInstance->queryParameter(i2);
        extractBuilder->addSerializedExpression(libraryInstance->queryActual(i2), parameter->queryType());
    }

    endExtract(func.ctx, extractBuilder);
}

ABoundActivity * HqlCppTranslator::doBuildActivityLibraryInstance(BuildCtx & ctx, IHqlExpression * expr)
{
    Owned<HqlCppLibraryInstance> libraryInstance = new HqlCppLibraryInstance(*this, expr, targetClusterType);

    CIArray boundInputs;

    unsigned numStreamed = libraryInstance->numStreamedInputs();
    for (unsigned i1=0; i1 < numStreamed; i1++)
        boundInputs.append(*buildCachedActivity(ctx, libraryInstance->queryActual(i1)));

    IHqlExpression * moduleFunction = expr->queryDefinition();      // no_funcdef
    IHqlExpression * module = moduleFunction->queryChild(0);
    assertex(module->getOperator() == no_libraryscope);
    IHqlExpression * nameAttr = module->queryAttribute(nameAtom);
    IHqlExpression * embeddedAttr = module->queryAttribute(embeddedAtom);
    OwnedHqlExpr name = foldHqlExpression(nameAttr->queryChild(0));
    IValue * nameValue = name->queryValue();
    IHqlExpression * originalName = queryAttributeChild(module, _original_Atom, 0);

    StringBuffer libraryName;
    if (nameValue)
        nameValue->getStringValue(libraryName);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKlibrarycall, expr, "LibraryCall");
    StringBuffer graphLabel;
    if (originalName)
    {
        StringBuffer temp;
        temp.append(originalName->queryName()).toLowerCase();
        graphLabel.append("Library").newline().append(temp);
    }
    else if (nameValue)
        graphLabel.append("Library").newline().append(libraryName);

    if (graphLabel.length())
        instance->graphLabel.set(graphLabel.str());

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    buildLibraryInstanceExtract(instance->startctx, libraryInstance);

    //MORE: Need to call functions to add the extract
    const HqlCppLibrary * library = libraryInstance->library;

    if (nameValue)
    {
        instance->addAttribute(WaLibraryName, libraryName.str());
        Owned<IWULibrary> wulib = wu()->updateLibraryByName(libraryName.str());
    }


    instance->addAttributeInt(WaInterfaceHash, library->getInterfaceHash());
    instance->addAttributeBool(WaIsEmbedded, (embeddedAttr != NULL));
    instance->addAttributeInt(WaNumMaxOutputs, library->outputs.ordinality());
    if (embeddedAttr)
        instance->addAttribute(WaIdGraph, embeddedAttr->queryChild(0));
    if (!targetHThor())
        instance->addAttributeInt(WaIdLibraryGraph, nextActivityId());            // reserve an id...

    // A debugging option to make it clearer how a library is being called.
    if (options.addLibraryInputsToGraph)
    {
        unsigned numParams = libraryInstance->numParameters();
        for (unsigned iIn = 0; iIn < numParams; iIn++)
        {
            IHqlExpression * parameter = libraryInstance->queryParameter(iIn);
            StringBuffer paramName;
            StringBuffer paramEcl;
            paramName.append("debuggingInput").append(iIn).append("__").append(parameter->queryName());
            toECL(libraryInstance->queryActual(iIn), paramEcl, false, true);
            instance->addAttribute(paramName, paramEcl);
        }
    }

    StringBuffer s;
    BuildCtx metactx(instance->classctx);
    metactx.addQuotedFunction("virtual IOutputMetaData * queryOutputMeta(unsigned whichOutput) override");
    BuildCtx switchctx(metactx);
    switchctx.addQuotedCompoundLiteral("switch (whichOutput)");

    HqlDummyLookupContext dummyCtx(NULL);
    IHqlScope * moduleScope = module->queryScope();
    ForEachItemIn(iout, library->outputs)
    {
        IHqlExpression & cur = library->outputs.item(iout);
        OwnedHqlExpr dataset = moduleScope->lookupSymbol(cur.queryId(), LSFpublic, dummyCtx);
        assertex(dataset && dataset->queryRecord());
        MetaInstance meta(*this, dataset->queryRecord(), isGrouped(dataset));
        buildMetaInfo(meta);
        switchctx.addQuoted(s.clear().append("case ").append(iout).append(": return &").append(meta.queryInstanceObject()).append(";"));
    }
    metactx.addReturn(queryQuotedNullExpr());

    {
        //Library Name must be onCreate invariant
        MemberFunction func(*this, instance->createctx, "virtual char * getLibraryName() override");
        buildReturn(func.ctx, name, unknownVarStringType);
    }

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, boundInputs)
        buildConnectInputOutput(ctx, instance, (ABoundActivity *)&boundInputs.item(idx2), 0, idx2);

    Owned<ABoundActivity> boundInstance = instance->getBoundActivity();
    return new ThorBoundLibraryActivity(boundInstance, instance->graphNode, libraryInstance);
}



void HqlCppTranslator::buildLibraryGraph(BuildCtx & ctx, IHqlExpression * expr, const char * graphName)
{
    OwnedHqlExpr resourced = getResourcedGraph(expr->queryChild(0), NULL);

    beginGraph(graphName);

    traceExpression("beforeGenerate", resourced);
    BuildCtx initctx(ctx);
    initctx.addGroup();
    initctx.addFilter(queryBoolExpr(false));

    Owned<LibraryEvalContext> libraryContext = new LibraryEvalContext(*this);
    initctx.associate(*libraryContext);

    unsigned numParams = outputLibrary->totalInputs();
    for (unsigned i1 = outputLibrary->numStreamedInputs(); i1 < numParams; i1++)
    {
        IHqlExpression & parameter = *outputLibrary->queryInputExpr(i1);
        libraryContext->associateExpression(initctx, &parameter);
    }

    Owned<ParentExtract> extractBuilder = createExtractBuilder(initctx, PETlibrary, outputLibraryId, GraphRemote, false);
    beginExtract(initctx, extractBuilder);

    BuildCtx evalctx(ctx);
    evalctx.addGroup();
    evalctx.addFilter(queryBoolExpr(false));

    //Ensure all the values are added to the serialization in the correct order by creating a dummy context and then
    //evaluating each parameter in term.  Slightly ugly - it would be better using different calls.
    extractBuilder->associateCursors(evalctx, evalctx, GraphNonLocal);

    CHqlBoundExpr dummyTarget;
    for (unsigned i2 = outputLibrary->numStreamedInputs(); i2 < numParams; i2++)
    {
        IHqlExpression * parameter = outputLibrary->queryInputExpr(i2);
        OwnedHqlExpr normalParameter = normalizeExpression(*this, parameter);
        extractBuilder->evaluateExpression(evalctx, normalParameter, dummyTarget, NULL, false);
    }

    BuildCtx graphctx(initctx);
    activeGraphCtx = &ctx;
    doBuildThorSubGraph(graphctx, resourced, SubGraphRoot, (unsigned)getIntValue(outputLibraryId->queryChild(0)), outputLibraryId);
    activeGraphCtx = NULL;

    endExtract(initctx, extractBuilder);
    endGraph();
}


//---------------------------------------------------------------------------


LibraryEvalContext::LibraryEvalContext(HqlCppTranslator & _translator) : EvalContext(_translator, NULL, NULL)
{
}

AliasKind LibraryEvalContext::evaluateExpression(BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt, bool evaluateLocally)
{
    if (value->getOperator() == no_libraryinput)
    {
        IHqlExpression * seq = queryLibraryInputSequence(value);
        unsigned match = values.find(*seq);
        assertex(match != NotFound);
        tgt.setFromTranslated(&bound.item(match));
    }
    else
        translator.buildTempExpr(ctx, value, tgt);
    return RuntimeAlias;
}

void LibraryEvalContext::associateExpression(BuildCtx & ctx, IHqlExpression * value)
{
    //Add the association by sequence number, because this is pre record normalization.
    assertex(value->getOperator() == no_libraryinput);
    IHqlExpression * seq = queryLibraryInputSequence(value);
    assertex(!values.contains(*seq));

    CHqlBoundTarget tempTarget;
    translator.createTempFor(ctx, value, tempTarget);
    values.append(*LINK(seq));

    CHqlBoundExpr tgt;
    tgt.setFromTarget(tempTarget);
    bound.append(*tgt.getTranslatedExpr());
}

void LibraryEvalContext::tempCompatiablityEnsureSerialized(const CHqlBoundTarget & tgt)
{
    throwUnexpected();
}

