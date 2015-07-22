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
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "eclrtl.hpp"
#include "hql.hpp"
#include "hqlttcpp.ipp"
#include "hqlmeta.hpp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqltcppc.ipp"
#include "hqlcatom.hpp"
#include "hqlfold.hpp"
#include "hqlgraph.ipp"
#include "hqllib.ipp"
#include "hqlpmap.hpp"
#include "hqlopt.hpp"
#include "hqlcerrors.hpp"
#include "hqlscope.hpp"
#include "hqlsource.ipp"
#include "hqlvalid.hpp"
#include "hqlerror.hpp"
#include "hqlalias.hpp"
#include "hqlir.hpp"

#define TraceExprPrintLog(x, expr) TOSTRLOG(MCdebugInfo(300), unknownJob, x, (expr)->toString);
//Following are for code that currently cause problems, but are probably a good idea
//#define MAP_PROJECT_TO_USERTABLE
//#define REMOVE_NAMED_SCALARS
//#define OPTIMIZE_IMPLICIT_CAST

#define PERSIST_VERSION                     1           // Increment when implementation is incompatible.
#define REMOVE_GLOBAL_ANNOTATION                    // This should improve cse.  It currently does for some, but not others...

#define DEFAULT_FOLD_OPTIONS    HFOfoldfilterproject

//#define PICK_ENGINE_EARLY

//===========================================================================

static bool isWorthHoisting(IHqlExpression * expr, bool asSubQuery)
{
    bool isFiltered = false;
    loop
    {
        switch (expr->getOperator())
        {
        case no_newkeyindex:
        case no_table:
        case no_temptable:
        case no_inlinetable:
        case no_globalscope:
        case no_global:
        case no_independent:
        case no_field:
        case no_datasetfromrow:
        case no_datasetfromdictionary:
        case no_null:
        case no_workunit_dataset:
        case no_colon:
            //compound activities not in list because not created at this point.
            return (isFiltered && asSubQuery);
        case no_select:
            return !isTargetSelector(expr);
        case no_filter:
            expr = expr->queryChild(0);
            isFiltered = true;
            break;
        case no_compound_diskread:
        case no_compound_indexread:
        case no_hqlproject:
        case no_newusertable:
        case no_limit:
        case no_catchds:
        case no_keyedlimit:
        case no_sorted:
        case no_grouped:
        case no_stepped:
        case no_distributed:
        case no_preservemeta:
        case no_nofold:
        case no_nohoist:
        case no_section:
        case no_sectioninput:
        case no_dataset_alias:
        case no_forcegraph:
            expr = expr->queryChild(0);
            break;
        case no_fail:
            return false;
        default:
            return true;
        }
    }
}


IHqlExpression * getDebugValueExpr(IConstWorkUnit * wu, IHqlExpression * expr)
{
    StringBuffer name;
    getStringValue(name, expr->queryChild(0));
    SCMStringBuffer value;
    wu->getDebugValue(name, value);

    ITypeInfo * exprType = expr->queryType();
    if (exprType->getTypeCode() == type_boolean)
        return createConstant(clipStrToBool(value.length(), value.str()));
    return createConstant(exprType->castFrom(value.length(), value.str()));
}

//===========================================================================

struct GlobalAttributeInfo
{
public:
    GlobalAttributeInfo(const char * _filePrefix, const char * _storedPrefix, IHqlExpression * _value) : value(_value)
    {
        setOp = no_none;
        persistOp = no_none;
        few = false;
        filePrefix = _filePrefix;
        storedPrefix = _storedPrefix;
        numPersistInstances = 0;
    }

    void extractGlobal(IHqlExpression * global, ClusterType platform);
    void extractStoredInfo(IHqlExpression * expr, IHqlExpression * codehash, bool isRoxie, int multiplePersistInstances);
    void checkFew(HqlCppTranslator & translator);
    void splitGlobalDefinition(ITypeInfo * type, IHqlExpression * value, IConstWorkUnit * wu, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput, bool isRoxie);
    IHqlExpression * getStoredKey();
    void preventDiskSpill() { few = true; }
    IHqlExpression * queryCluster() const { return cluster; }
    int queryMaxPersistCopies() const { return numPersistInstances; }

protected:
    void doSplitGlobalDefinition(ITypeInfo * type, IHqlExpression * value, IConstWorkUnit * wu, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput, bool isRoxie);
    IHqlExpression * createSetValue(IHqlExpression * value, IHqlExpression * aliasName);
    void createSmallOutput(IHqlExpression * value, SharedHqlExpr & setOutput);
    IHqlExpression * queryAlias(IHqlExpression * value);
    IHqlExpression * queryFilename(IHqlExpression * value, IConstWorkUnit * wu, bool isRoxie);
    void splitSmallDataset(IHqlExpression * value, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput);
    void setCluster(IHqlExpression * expr);

public:
    LinkedHqlExpr value;
    OwnedHqlExpr storedName;
    OwnedHqlExpr originalLabel;
    OwnedHqlExpr sequence;
    OwnedHqlExpr fieldFormat;
    node_operator setOp;
    node_operator persistOp;
protected:
    OwnedHqlExpr aliasName;
    OwnedHqlExpr cachedFilename;
    OwnedHqlExpr cluster;
    OwnedHqlExpr extraSetAttr;
    OwnedHqlExpr extraOutputAttr;
    OwnedHqlExpr codehash;
    const char * filePrefix;
    const char * storedPrefix;
    int numPersistInstances;
    bool few;
};


static bool isTrivialInlineOutput(IHqlExpression * expr)
{
    if (queryRealChild(expr, 1))
        return false;
    IHqlExpression * ds = expr->queryChild(0);
    if ((ds->getOperator() != no_null) || !ds->isDataset())
        return false;
    IHqlExpression * seq = querySequence(expr);
    if (getIntValue(seq, -1) >= 0)
        return false;
    return true;
}

//---------------------------------------------------------------------------

IHqlExpression * createNextStringValue(IHqlExpression * value, const char * prefix = NULL)
{
    StringBuffer valueText;
    valueText.append(prefix ? prefix : "a");
    getUniqueId(valueText);

#if 0
    if (value)
    {
        const char * nameText = value->queryName()->str();
        if (nameText)
            valueText.append("__").appendLower(strlen(nameText), nameText);
        //otherwise append the operator?
    }
#endif

#if 0
#ifdef _DEBUG
    //Following lines are here to add a break point when debugging
    if (stricmp(valueText.str(), "a1") == 0)
        valueText.length();
    if (stricmp(valueText.str(), "a2") == 0)
        valueText.length();
#endif
#endif

    return createConstant(valueText.str());
}

IHqlExpression * createSetResult(IHqlExpression * value)
{
    HqlExprArray args;
    args.append(*LINK(value));
    args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
    args.append(*createAttribute(namedAtom, createNextStringValue(value)));
    return createSetResult(args);
}

static IHqlExpression * addAttrOwnToDataset(IHqlExpression * dataset, IHqlExpression * attr)
{
    HqlExprArray args;
    unwindChildren(args, dataset);
    args.append(*attr);
    return dataset->clone(args);
}

static IHqlExpression * mergeLimitIntoDataset(IHqlExpression * dataset, IHqlExpression * limit)
{
    return addAttrOwnToDataset(dataset, createAttribute(limitAtom, LINK(limit->queryChild(1)), LINK(limit->queryChild(2))));
}

//---------------------------------------------------------------------------

static bool isOptionTooLate(const char * name)
{
    if (stricmp(name, "gatherDependencies") == 0) return true;
    if (stricmp(name, "gatherDependenciesSelection") == 0) return true;
    if (stricmp(name, "archiveToCpp") == 0) return true;
    if (stricmp(name, "importAllModules") == 0) return true;
    if (stricmp(name, "importImplicitModules") == 0) return true;
    if (stricmp(name, "noCache") == 0) return true;
    return false;
}

static HqlTransformerInfo newThorStoredReplacerInfo("NewThorStoredReplacer");
NewThorStoredReplacer::NewThorStoredReplacer(HqlCppTranslator & _translator, IWorkUnit * _wu, ICodegenContextCallback * _ctxCallback)
: QuickHqlTransformer(newThorStoredReplacerInfo, NULL), translator(_translator)
{
    wu = _wu;
    ctxCallback = _ctxCallback;
    foldStored = false;
    seenMeta = false;
}


void NewThorStoredReplacer::doAnalyse(IHqlExpression * expr)
{
    //NOTE: This is called very early before no_assertconstant has been processed, so we need to explicitly
    //constant fold, and check it is constant (bug 26963)

    node_operator op = expr->getOperator();
    if (op == no_setmeta)
    {
        StringBuffer errorTemp;
        seenMeta = true;
        IAtom * kind = expr->queryChild(0)->queryName();
        if (kind == webserviceAtom)
        {
            Owned<IWUWebServicesInfo> wsi = wu->updateWebServicesInfo(false);
            if (wsi)
                translator.ERRORAT(expr, HQLERR_MultipleHashWebserviceCalls);
            wsi.setown(wu->updateWebServicesInfo(true));

            IHqlExpression *wsExpr = expr->queryChild(0);
            ForEachChild(i, wsExpr)
            {
                StringBuffer name, value;
                OwnedHqlExpr folded = foldHqlExpression(wsExpr->queryChild(i));
                getHintNameValue(folded, name, value);
                wsi->setText(name, value);
            }
        }
        else if (kind == debugAtom)
        {
            OwnedHqlExpr foldedName = foldHqlExpression(expr->queryChild(1));
            OwnedHqlExpr foldedValue = foldHqlExpression(expr->queryChild(2));
            IValue * name = foldedName->queryValue();
            IValue * value = foldedValue->queryValue();

            if (!name)
                translator.reportError(expr, ECODETEXT(HQLERR_ExpectedConstantDebug), getExprECL(foldedName, errorTemp).str());
            if (!value)
                translator.reportError(expr, ECODETEXT(HQLERR_ExpectedConstantDebug), getExprECL(foldedValue, errorTemp).str());

            StringBuffer nameText,valueText;
            name->getStringValue(nameText);
            if (isOptionTooLate(nameText.str()))
                translator.reportWarning(CategoryIgnored, HQLWRN_OptionSetToLate, HQLWRN_OptionSetToLate_Text, nameText.str());

            if (value->queryType()->getTypeCode() == type_boolean)
                valueText.append(value->getBoolValue() ? 1 : 0);
            else
                value->getStringValue(valueText);
            wu->setDebugValue(nameText.str(), valueText, true);
        }
        else if (kind == workunitAtom)
        {
            OwnedHqlExpr foldedName = foldHqlExpression(expr->queryChild(1));
            OwnedHqlExpr foldedValue = foldHqlExpression(expr->queryChild(2));
            IValue * name = foldedName->queryValue();
            IValue * value = foldedValue->queryValue();

            if (!name)
                translator.reportError(expr, ECODETEXT(HQLERR_ExpectedConstantWorkunit), getExprECL(foldedName, errorTemp).str());
            if (!value)
                translator.reportError(expr, ECODETEXT(HQLERR_ExpectedConstantWorkunit), getExprECL(foldedValue, errorTemp).str());

            StringBuffer nameText,valueText;
            name->getStringValue(nameText);
            value->getStringValue(valueText);
            if (stricmp(nameText.str(), "name") == 0)
                wu->setJobName(valueText.str());
            else if (stricmp(nameText.str(), "priority") == 0)
            {
                if (isStringType(value->queryType()))
                {
                    WUPriorityClass prio = PriorityClassUnknown;
                    if (stricmp(valueText.str(), "low") == 0)
                        prio = PriorityClassLow;
                    else if (stricmp(valueText.str(), "normal") == 0)
                        prio = PriorityClassNormal;
                    else if (stricmp(valueText.str(), "high") == 0)
                        prio = PriorityClassHigh;
                    wu->setPriority(prio);
                }
                else
                    wu->setPriorityLevel((int)value->getIntValue());
            }
            else if (stricmp(nameText.str(), "cluster") == 0)
            {
                ctxCallback->noteCluster(valueText.str());
                wu->setClusterName(valueText.str());
            }
            else if (stricmp(nameText.str(), "protect") == 0)
            {
                wu->protect(value->getBoolValue());
            }
            else if (stricmp(nameText.str(), "scope") == 0)
            {
                wu->setWuScope(valueText.str());
            }
            else
                translator.reportError(expr, ECODETEXT(HQLERR_UnsupportedHashWorkunit), nameText.str());
        }
        else if (kind == linkAtom)
        {
            OwnedHqlExpr foldedName = foldHqlExpression(expr->queryChild(1));
            StringBuffer libraryText;
            if (getStringValue(libraryText, foldedName).length())
                translator.useLibrary(libraryText);
        }
        else if ((kind == constAtom) || (kind == storedAtom))
        {
            //assume there won't be many of these... otherwise we should use a hash table
            OwnedHqlExpr lowerName = lowerCaseHqlExpr(expr->queryChild(1));
            //Use lowerName->queryBody() to remove named symbols/location annotations etc.
            storedNames.append(*LINK(lowerName->queryBody()));
            storedValues.append(*LINK(expr->queryChild(2)));
            storedIsConstant.append(kind == constAtom);
        }
        else if (kind == onWarningAtom)
            translator.addGlobalOnWarning(expr);
    }
    else if (op == no_colon)
    {
        if (queryAttributeInList(labeledAtom, expr->queryChild(1)))
            seenMeta = true;
    }

    //Walk the body rather than expr so that the no_setmeta isn't processed again.
    //If the code above was in doAnalyseBody() the error location information would not be available.
    QuickHqlTransformer::doAnalyseBody(expr->queryBody());
}

bool NewThorStoredReplacer::needToTransform()
{
    //foldStored = translator.queryOptions().foldStored;    // NB: options isn't initialised correctly at this point.
    foldStored = wu->getDebugValueBool("foldStored", false);
    return (foldStored || seenMeta);
}


// This works on unnormalized trees, so based on QuickHqlTransformer
IHqlExpression * NewThorStoredReplacer::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_colon:
        {
            HqlExprArray actions;
            expr->queryChild(1)->unwindList(actions, no_comma);

            OwnedHqlExpr replacement;
            OwnedHqlExpr matchedName;
            bool onlyStored = true;
            bool forceConstant = foldStored;
            ForEachItemIn(idx, actions)
            {
                IHqlExpression & cur = actions.item(idx);
                switch (cur.getOperator())
                {
                case no_stored:
                    {
                        OwnedHqlExpr storedName = lowerCaseHqlExpr(cur.queryChild(0));
                        IHqlExpression * searchName = storedName->queryBody();
                        unsigned match = storedNames.find(*searchName);
                        if (match != NotFound)
                        {
                            if (storedIsConstant.item(match))
                                forceConstant = true;

                            matchedName.set(searchName);
                            replacement.set(&storedValues.item(match));
                        }
                        break;
                    }
                case no_attr:
                case no_attr_expr:
                    if (cur.queryName() == labeledAtom)
                    {
                        OwnedHqlExpr storedName = lowerCaseHqlExpr(cur.queryChild(0));
                        IHqlExpression * searchName = storedName->queryBody();
                        unsigned match = storedNames.find(*searchName);
                        if (match != NotFound)
                        {
                            matchedName.set(searchName);
                            replacement.set(&storedValues.item(match));
                        }
                        else
                            replacement.set(expr->queryChild(0));

                        forceConstant = true;
                        break;
                    }
                default:
                    onlyStored = false;
                    break;
                }
            }

            if (matchedName)
            {
                unsigned activeMatch = activeReplacements.find(*matchedName);
                if (activeMatch != NotFound)
                {
                    StringBuffer nameText;
                    getExprECL(matchedName, nameText);
                    if (activeMatch+1 != activeReplacements.ordinality())
                    {
                        StringBuffer othersText;
                        for (unsigned i=activeMatch+1; i < activeReplacements.ordinality(); i++)
                        {
                            othersText.append(",");
                            getExprECL(&activeReplacements.item(i), othersText);
                        }
                        translator.reportError(expr, ECODETEXT(HQLERR_RecursiveStoredOther),  forceConstant ? "CONSTANT" : "STORED", nameText.str(), othersText.str()+1);
                    }
                    else
                        translator.reportError(expr, ECODETEXT(HQLERR_RecursiveStored),  forceConstant ? "CONSTANT" : "STORED", nameText.str());
                }

                ITypeInfo * exprType = expr->queryType();
                ITypeInfo * replacementType = replacement->queryType();
                type_t etc = exprType->getTypeCode();
                type_t rtc = replacementType->getTypeCode();

                StringBuffer nameText, exprTypeText, replacementTypeText;
                switch (etc)
                {
                case type_groupedtable:
                case type_table:
                case type_row:
                case type_record:
                case type_transform:
                case type_void:
                    {
                        if (etc != rtc)
                        {
                            getExprECL(matchedName, nameText);
                            getFriendlyTypeStr(exprType, exprTypeText);
                            getFriendlyTypeStr(replacementType, replacementTypeText);
                            translator.reportError(expr, ECODETEXT(HQLERR_HashStoredTypeMismatch), nameText.str(), exprTypeText.str(), replacementTypeText.str());
                        }
                        else if (expr->queryRecord() != replacement->queryRecord())
                        {
                            StringBuffer s;
                            translator.reportError(expr, ECODETEXT(HQLERR_HashStoredRecordMismatch), getExprECL(matchedName, s).str());
                        }
                    }
                    break;
                case type_set:
                case type_array:
                    {
                        if ((rtc != type_set) && (rtc != type_array))
                        {
                            getExprECL(matchedName, nameText);
                            getFriendlyTypeStr(exprType, exprTypeText);
                            getFriendlyTypeStr(replacementType, replacementTypeText);
                            translator.reportError(expr, ECODETEXT(HQLERR_HashStoredTypeMismatch), nameText.str(), exprTypeText.str(), replacementTypeText.str());
                        }
                        replacement.setown(ensureExprType(replacement, exprType));
                        break;
                    }
                default:
                    {
                        switch (rtc)
                        {
                        case type_groupedtable:
                        case type_table:
                        case type_row:
                        case type_record:
                        case type_transform:
                        case type_void:
                            {
                                getExprECL(matchedName, nameText);
                                getFriendlyTypeStr(exprType, exprTypeText);
                                getFriendlyTypeStr(replacementType, replacementTypeText);
                                translator.reportError(expr, ECODETEXT(HQLERR_HashStoredTypeMismatch), nameText.str(), exprTypeText.str(), replacementTypeText.str());
                            }
                        default:
                            replacement.setown(ensureExprType(replacement, exprType));
                        }
                    }
                    break;
                }
            }

            LinkedHqlExpr result;
            if (matchedName)
                activeReplacements.append(*matchedName);

            if (onlyStored)
            {
                if (forceConstant && replacement)
                    result.setown(transform(replacement));
                else if (foldStored)
                    result.setown(transform(expr->queryChild(0)));
            }
            if (replacement && !result)
            {
                HqlExprArray args;
                args.append(*transform(replacement));
                result.setown(completeTransform(expr, args));
            }

            if (matchedName)
                activeReplacements.pop();

            if (result)
                return result.getClear();
            break;
        }
    case no_comma:
    case no_compound:
        if (expr->queryChild(0)->getOperator() == no_setmeta)
            return transform(expr->queryChild(1));
        if (expr->queryChild(1)->getOperator() == no_setmeta)
            return transform(expr->queryChild(0));
        break;
    case no_actionlist:
    case no_orderedactionlist:
        {
            HqlExprArray actions;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (cur->getOperator() != no_setmeta)
                    actions.append(*transform(cur));
            }
            if (actions.ordinality() != 0)
                return expr->clone(actions);
            return transform(expr->queryChild(0));
        }
    }
    return QuickHqlTransformer::createTransformed(expr);
}

//---------------------------------------------------------------------------
// NB: This is called after no_setresults are added, but before any normalization.

static HqlTransformerInfo hqlThorBoundaryTransformerInfo("HqlThorBoundaryTransformer");
HqlThorBoundaryTransformer::HqlThorBoundaryTransformer(IConstWorkUnit * _wu, bool _isRoxie, unsigned _maxRootMaybes, bool _resourceConditionalActions, bool _resourceSequential)
: NewHqlTransformer(hqlThorBoundaryTransformerInfo)
{
    wu = _wu;
    isRoxie = _isRoxie;
    maxRootMaybes = _maxRootMaybes;
    resourceConditionalActions = _resourceConditionalActions;
    resourceSequential = _resourceSequential;
}


void HqlThorBoundaryTransformer::transformCompound(HqlExprArray & result, node_operator compoundOp, const HqlExprArray & args, unsigned MaxMaybes)
{
    UnsignedArray normalizeOptions;
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        normalizeOptions.append(normalizeThor(&cur));
    }

    //If any "yes" or "some" values are separated by maybes then convert the maybes (and "somes") into yes
    unsigned lastYes = NotFound;
    unsigned numMaybes = 0;
    ForEachItemIn(i2, args)
    {
        switch (normalizeOptions.item(i2))
        {
        case OptionYes:
        case OptionSome:
            if (lastYes != NotFound)
            {
                if (numMaybes <= MaxMaybes)
                {
                    for (unsigned j = lastYes; j <= i2; j++)
                        normalizeOptions.replace(OptionYes, j);
                }
            }
            numMaybes = 0;
            lastYes = i2;
            break;
        case OptionMaybe:
            numMaybes++;
            break;
        case OptionNo:
            lastYes = NotFound;
            break;
        }
    }

    //Do thor and non-thor parallel actions independently
    HqlExprArray thor;
    ForEachItemIn(idx, args)
    {
        IHqlExpression * cur = &args.item(idx);
        if (normalizeOptions.item(idx) == OptionYes)
            thor.append(*LINK(cur));
        else
        {
            if (thor.ordinality())
            {
                result.append(*createWrapper(no_thor, createCompound(compoundOp, thor)));
                thor.kill();
            }
            result.append(*createTransformed(cur));
        }
    }
    if (thor.ordinality())
        result.append(*createWrapper(no_thor, createCompound(compoundOp, thor)));
}

IHqlExpression * HqlThorBoundaryTransformer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_field:
    case no_constant:
    case no_attr:
    case no_attr_link:
    case no_getresult:
    case no_left:
    case no_right:
        return LINK(expr);
    case no_sizeof:
    case no_offsetof:
        return getTransformedChildren(expr);
    }

    //Unusually, wrap the expression in a thor node before processing annotations.
    //This ensures that the location/named symbol stays with the action.
    //MORE: If this is a dataset then it needs to turn it into a setResult()/getResult() pair.
    if (normalizeThor(expr) == OptionYes)
    {
        if (!expr->isTransform())
            return createWrapper(no_thor, LINK(expr));
    }

    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    switch (op)
    {
    case no_actionlist:
    case no_orderedactionlist:
        {
            HqlExprArray nonThor, args;
            expr->unwindList(args, op);
            transformCompound(nonThor, op, args, (unsigned)-1);
            return createCompound(op, nonThor);
        }
    case no_parallel:
        {
            HqlExprArray expanded;
            expr->unwindList(expanded, no_parallel);

            //Similar to compound, but possible to reorder branches...
            unsigned numThor = 0;
            UnsignedArray normalizeOptions;
            ForEachItemIn(idx, expanded)
            {
                YesNoOption option = normalizeThor(&expanded.item(idx));
                normalizeOptions.append(option);
                if ((option == OptionYes) || (option == OptionSome))
                    numThor++;
            }

            if (numThor > 1)
            {
                HqlExprArray thor, nonThor;
                ForEachItemIn(idx, expanded)
                {
                    IHqlExpression * cur = &expanded.item(idx);
                    switch (normalizeOptions.item(idx))
                    {
                    case OptionYes:
                    case OptionSome:
                        thor.append(*LINK(cur));
                        break;
                    default:
                        nonThor.append(*createTransformed(cur));
                        break;
                    }
                }
                if (nonThor.ordinality() == 0)
                {
                    //can happen if inputs are a mixture of yes and some.
                    return createWrapper(no_thor, LINK(expr));
                }
                nonThor.append(*createWrapper(no_thor, createValue(no_parallel, makeVoidType(), thor)));
                return expr->clone(nonThor);
            }
            break;
        }
    case no_nothor:
        return LINK(expr);
    }

    return NewHqlTransformer::createTransformed(expr);
}

static YesNoOption combine(YesNoOption left, YesNoOption right, bool isUnion)
{
    if ((left == OptionNo) || (right == OptionNo))
        return OptionNo;
    if (left == OptionUnknown)
        return right;
    if (right == OptionUnknown)
        return left;

    //Yes,Some,Maybe
    if (isUnion)
    {
        //return definite if both branches may benefit.
        switch (left)
        {
        case OptionYes:
            return (right != OptionMaybe) ? OptionYes : OptionSome;
        case OptionSome:
            return (right != OptionMaybe) ? OptionYes : OptionSome;
        case OptionMaybe:
            return (right != OptionMaybe) ? OptionSome : OptionMaybe;
        }
    }
    else
    {
        //Intersection, return definite
        switch (left)
        {
        case OptionYes:
            return (right == OptionYes) ? OptionYes : OptionSome;
        case OptionSome:
            return OptionSome;
        case OptionMaybe:
            return (right == OptionMaybe) ? OptionMaybe : OptionSome;
        }
    }
    throwUnexpected();
}


//MORE: Needs to be yes, no, possibly.
YesNoOption HqlThorBoundaryTransformer::normalizeThor(IHqlExpression * expr)
{
    HqlThorBoundaryInfo * extra = queryBodyExtra(expr);
    if (extra->normalize == OptionUnknown)
        extra->normalize = calcNormalizeThor(expr);
    return extra->normalize;
}

YesNoOption HqlThorBoundaryTransformer::calcNormalizeThor(IHqlExpression * expr)
{
    //MORE: This should probably be cached in the extra info & recursed more correctly
    node_operator op = expr->getOperator();
    ITypeInfo * type = expr->queryType();

    switch (op)
    {
    case no_constant:
    case no_field:
    case no_record:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_getresult:
    case no_left:
    case no_right:
    case no_sizeof:
    case no_all:
    case no_self:
    case no_activerow:
        return OptionMaybe;
    case no_evaluate:
        throwUnexpected();
    case no_select:
        {
            bool isNew;
            IHqlExpression * ds = querySelectorDataset(expr, isNew);
            switch (ds->getOperator())
            {
            case no_getresult:
            case no_call:
            case no_externalcall:
                return normalizeThor(ds);
            case no_self:
                return OptionMaybe;
            }
            return isNew ? OptionYes : OptionMaybe;
        }
    case NO_AGGREGATE:
    case no_executewhen:
        return OptionYes;
    case NO_ACTION_REQUIRES_GRAPH:
        {
            if ((op == no_output) && isTrivialInlineOutput(expr))
                return OptionMaybe;
            return OptionYes;
        }
    case no_sequential: // do not do inside thor - stops graphs being merged.
        if (!resourceSequential)
            return OptionNo;
        // fallthrough
    case no_actionlist:
    case no_orderedactionlist:
    case no_parallel:
        {
            YesNoOption option = OptionUnknown;
            ForEachChild(idx, expr)
            {
                YesNoOption childOption = normalizeThor(expr->queryChild(idx));
                if (childOption == OptionNo)
                    return OptionNo;
                bool fixedOrder = (op == no_sequential) || (op == no_orderedactionlist);
                option = combine(option, childOption, fixedOrder);       // can reorder parallel - so intersection is better
            }
            return option;
        }
    case no_cluster:
    case no_nothor:
        return OptionNo;
    case no_if:
        if (type && (type->getTypeCode() == type_void))
        {
            if (resourceConditionalActions)
            {
                IHqlExpression * falseExpr = expr->queryChild(2);
                YesNoOption leftOption = normalizeThor(expr->queryChild(1));
                YesNoOption rightOption = falseExpr ? normalizeThor(falseExpr) : OptionMaybe;
                YesNoOption branchOption = combine(leftOption, rightOption, true);
                YesNoOption condOption = normalizeThor(expr->queryChild(0));
                if ((branchOption == OptionYes) && (condOption != OptionNo))
                    return OptionYes;
//              if ((condOption == OptionYes) && (branchOption != OptionNo))
//                  return OptionYes;
                return combine(condOption, branchOption, true);
            }
            return OptionNo;    // not supported
        }
        // default action.  Not completely convinced it is correct....
        break;
    case no_choose:
        if (type && (type->getTypeCode() == type_void))
        {
            if (resourceConditionalActions)
            {
                UNIMPLEMENTED;
            }
        }
        break;
    case no_setresult:
        {
            IHqlExpression * value = expr->queryChild(0);
            YesNoOption valueOption = normalizeThor(value);
            //Probably worth doing the whole thing in thor if some part if it needs to be - will improve commoning up if nothing else.
            if (valueOption == OptionSome)
                return OptionYes;
            return valueOption;
        }
    case no_extractresult:
        {
            IHqlExpression * ds = expr->queryChild(0);
            OwnedHqlExpr transformedDs = transform(ds);
            if (transformedDs != ds)
                return OptionYes;
            //Probably worth doing the whole thing in thor if some part if it needs to be - will improve commoning up if nothing else.
            return normalizeThor(expr->queryChild(1));
        }
    case no_compound:
        {
            YesNoOption leftOption = normalizeThor(expr->queryChild(0));
            YesNoOption rightOption = normalizeThor(expr->queryChild(1));
            return combine(leftOption, rightOption, true);
        }

    case no_call:
        {
            YesNoOption bodyOption = normalizeThor(expr->queryBody()->queryFunctionDefinition());
            //do Something with it
            break;
        }
    case no_externalcall:
        {
            IHqlExpression * func = expr->queryExternalDefinition();
            IHqlExpression * funcDef = func->queryChild(0);
            if (funcDef->hasAttribute(gctxmethodAtom) || funcDef->hasAttribute(globalContextAtom))
                return OptionNo;
//          if (funcDef->hasAttribute(graphAtom))
//              return OptionYes;
            if (!resourceConditionalActions && expr->isAction())
                return OptionNo;
            //depends on the results of the arguments..
            type = NULL;        // don't check the return type
            break;
        }
    case no_setworkflow_cond:
    case no_ensureresult:
        return OptionNo;
    case no_null:
        return OptionMaybe;
    }

    //NB: things like NOT EXISTS we want evaluated as NOT THOR(EXISTS()), or as part of a larger context
    //otherwise things like klogermann14.xhql don't get EXISTS() csed between graphs.
    YesNoOption option = OptionMaybe;
    ForEachChild(idx, expr)
    {
        YesNoOption childOption = normalizeThor(expr->queryChild(idx));
        if (childOption == OptionNo)
            return OptionNo;
        option = combine(option, childOption, true);
    }

    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_row:
            switch (op)
            {
            case no_fromxml:
            case no_fromjson:
                return option;
            case no_createrow:
                //MORE: There are more cases that could be evaluated outside of thor, but playing safe.
                if (expr->queryChild(0)->isConstant())
                    return option;
                break;
            }
            return OptionYes;
        case type_groupedtable:
        case type_table:
            //must be a dataset parameter to a call, or an argument to a comparison
            //Need to know whether it can be evaluate inline or not.
            //if it does require thor, then we will need to generate a setresult/get result pair to do it.
            return !canProcessInline(NULL, expr) ? OptionYes : OptionMaybe;
        }
    }

    return option;
}

void HqlThorBoundaryTransformer::transformRoot(const HqlExprArray & in, HqlExprArray & out)
{
    //NewHqlTransformer::transformArray(in, out);
    //following theoretically might improve things, but generally just causes code to become worse,
    //because all the global set results are done in activities, and there isn't cse between them
    transformCompound(out, no_actionlist, in, maxRootMaybes);
}


void HqlCppTranslator::markThorBoundaries(WorkflowItem & curWorkflow)
{
    HqlExprArray & exprs = curWorkflow.queryExprs();
    HqlExprArray bounded;

    HqlThorBoundaryTransformer thorTransformer(wu(), targetRoxie(), options.maxRootMaybeThorActions, options.resourceConditionalActions, options.resourceSequential);
    thorTransformer.transformRoot(exprs, bounded);
    replaceArray(exprs, bounded);
}

//---------------------------------------------------------------------------
// NB: This is called after no_setresults are added, but before any normalization.

IHqlExpression * ThorScalarTransformer::queryAlreadyTransformed(IHqlExpression * expr)
{
    bool conditional = isConditional();
    if (conditional)
    {
        IHqlExpression * ret = queryExtra(expr)->transformed[false];
        if (ret)
            return ret;
    }
    return queryExtra(expr)->transformed[isConditional()];
}

IHqlExpression * ThorScalarTransformer::queryAlreadyTransformedSelector(IHqlExpression * expr)
{
    return queryExtra(expr)->transformedSelector[isConditional()];
}

void ThorScalarTransformer::setTransformed(IHqlExpression * expr, IHqlExpression * transformed)
{
    queryExtra(expr)->transformed[isConditional()].set(transformed);
}


void ThorScalarTransformer::setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed)
{
    queryExtra(expr)->transformedSelector[isConditional()].set(transformed);
}



static HqlTransformerInfo ThorScalarTransformerInfo("ThorScalarTransformer");
ThorScalarTransformer::ThorScalarTransformer(const HqlCppOptions & _options) : HoistingHqlTransformer(ThorScalarTransformerInfo, CTFnoteifactions), options(_options)
{
    isConditionalDepth = 0;
    seenCandidate = false;
}


void ThorScalarTransformer::doAnalyseExpr(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_thor:
        {
            ITypeInfo * type = expr->queryType();
            if (type && (type->isScalar() || type->getTypeCode() == type_row))
                seenCandidate = true;
            //No point looking further than a no_thor they don't (currently) get nested within each other.
            return;
        }
    }

    HoistingHqlTransformer::doAnalyseExpr(expr);
}


void ThorScalarTransformer::createHoisted(IHqlExpression * expr, SharedHqlExpr & setResultStmt, SharedHqlExpr & getResult, bool addWrapper)
{
    IHqlExpression * value = expr;
    HqlExprArray actions;
    while (value->getOperator() == no_compound)
    {
        unwindCommaCompound(actions, value->queryChild(0));
        value = value->queryChild(1);
    }

    IHqlExpression * setResult = createSetResult(value);
    getResult.setown(createGetResultFromSetResult(setResult));
    actions.append(*setResult);
    setResultStmt.setown(createActionList(actions));

    if (addWrapper)
        setResultStmt.setown(createValue(no_thor, makeVoidType(), setResultStmt.getClear()));
}


IHqlExpression * ThorScalarTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
    {
        //THOR(NULL) is marked as constant (probably incorrectly), but still needs hoisting.
//      if ((expr->getOperator() != no_thor) || (expr->numChildren() == 0))
            return LINK(expr);
    }

    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_if:
    case no_chooseds:
        {
            bool isGuard = isFailureGuard(expr);
            HqlExprArray children;
            children.append(*transform(expr->queryChild(0)));
            if (!isGuard)
                isConditionalDepth++;
            transformChildren(expr, children);
            if (!isGuard)
                isConditionalDepth--;
            return cloneOrLink(expr, children);
        }
    case no_thor:
        {
            ITypeInfo * type = expr->queryType();
//          if (isUsedUnconditionally(expr) && type && (type->isScalar() || type->getTypeCode() == type_row))
            if ((type->isScalar() || type->getTypeCode() == type_row))
            {
                //only other solution is to have some kind of graph result which is returned.
                assertex(options.workunitTemporaries);
                OwnedHqlExpr getResult, setResult;
                OwnedHqlExpr transformedChild = transform(expr->queryChild(0));
                createHoisted(transformedChild, setResult, getResult, true);
                //Note sure if this condition is needed any more - I suspect better without.
                if (isConditional())
                    return createCompound(setResult.getClear(), getResult.getClear());
                else
                {
                    appendToTarget(*setResult.getClear());
                    return getResult.getClear();
                }
            }
            return LINK(expr);
        }
        break;
    case no_mapto:
        if (isConditional())
        {
            HqlExprArray children;
            isConditionalDepth--;
            children.append(*transform(expr->queryChild(0)));
            isConditionalDepth++;
            children.append(*transform(expr->queryChild(1)));
            return cloneOrLink(expr, children);
        }
        break;
    case no_case:
    case no_map:
        {
            HqlExprArray children;
            unsigned firstArg = 0;
            unsigned numChildren = expr->numChildren();
            children.ensure(numChildren);
            if (op != no_map)
            {
                firstArg = 1;
                children.append(*transform(expr->queryChild(0)));
            }
            isConditionalDepth++;
            for (unsigned idx=firstArg; idx < numChildren; idx++)
                children.append(*transform(expr->queryChild(idx)));
            isConditionalDepth--;
            return cloneOrLink(expr, children);
        }
    case no_table:
        //Don't look at the default values for fields in the table's record
        return LINK(expr);
    }

    return HoistingHqlTransformer::createTransformed(expr);
}



//---------------------------------------------------------------------------


/*
  Look for expressions like count(x) and abc[1].x inside a condition and convert to
  setresult(<complicated>, 'x')
  getresult('x')

  Problems:

  o it tries to keep get/set results within the conditional branches that need them,
    but that means if it occurs in more than one then it will only get added once.

  o It (and all transformers) should possibly always generate a setresult,getresult locally,
    and then have another pass that moves all the setresults to the optimal place. E.g. to the highest shared location.
    [Alternatively can a global, but conditional, root graph be generated for it?]

  Also converts
  setresult(thor(x)) to thor(setresult(x))
  thor(scalar) to setresult(scalar,'x'),getresult('x')

*/

static void normalizeResultFormat(WorkflowItem & curWorkflow, const HqlCppOptions & options)
{
    //Until thor has a way of calling a graph and returning a result we need to call this transformer, so that
    //scalars that need to be evaluated in thor are correctly hoisted.
    ThorScalarTransformer transformer(options);
    HqlExprArray & exprs = curWorkflow.queryExprs();

    transformer.analyseArray(exprs, 0);

    if (transformer.needToTransform())
    {
        HqlExprArray transformed;
        transformer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }
}

//---------------------------------------------------------------------------

//Try and get the HOLe queries at the start and the Thor queries at the end.
//Keep track of the dependencies of the statements, so that they don't get reordered
//too aggressively.
//---------------------------------------------------------------------------

static HqlTransformerInfo sequenceNumberAllocatorInfo("SequenceNumberAllocator");
SequenceNumberAllocator::SequenceNumberAllocator(HqlCppTranslator & _translator) : NewHqlTransformer(sequenceNumberAllocatorInfo), translator(_translator)
{
    applyDepth = 0;
    sequence = 0;
}

void SequenceNumberAllocator::nextSequence(HqlExprArray & args, IHqlExpression * name, IAtom * overwriteAction, IHqlExpression * value, bool needAttr, bool * duplicate)
{
    IHqlExpression * seq = NULL;
    if (duplicate)
        *duplicate = false;
    if (name)
    {
        SharedHqlExpr * matched = namedMap.getValue(name);
        if (matched)
        {
            StringBuffer nameText;
            name->toString(nameText);

            IHqlExpression * prev = matched->get();
            if (prev->isAttribute())
            {
                IAtom * prevName = prev->queryName();
                if (!overwriteAction)
                {
                    if (prevName == extendAtom)
                        throwError1(HQLERR_ExtendMismatch, nameText.str());
                    else
                        throwError1(HQLERR_OverwriteMismatch, nameText.str());
                }
                else if (prevName != overwriteAction)
                    throwError1(HQLERR_ExtendOverwriteMismatch, nameText.str());

                IHqlExpression * prevValue = prev->queryChild(1);
                if (!recordTypesMatch(prevValue->queryType(), value->queryType()))
                    throwError1(HQLERR_ExtendTypeMismatch, nameText.str());
                seq = LINK(prev->queryChild(0));
                assertex(duplicate);
                *duplicate = true;
            }
            else
            {
                if (overwriteAction)
                {
                    if (overwriteAction == extendAtom)
                        throwError1(HQLERR_ExtendMismatch, nameText.str());
                    else
                        throwError1(HQLERR_OverwriteMismatch, nameText.str());
                }
                else
                    throwError1(HQLERR_DuplicateNameOutput, nameText.str());
            }
        }

        if (!seq)
        {
            seq = createConstant(signedType->castFrom(true, (__int64)sequence++));
            OwnedHqlExpr saveValue = overwriteAction ? createAttribute(overwriteAction, LINK(seq), LINK(value)) : LINK(seq);
            namedMap.setValue(name, saveValue);
        }
    }
    else
        seq = createConstant(signedType->castFrom(true, (__int64)sequence++));

    if (needAttr)
        args.append(*createAttribute(sequenceAtom, seq));
    else
        args.append(*seq);
}

IHqlExpression * SequenceNumberAllocator::doTransformRootExpr(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch(op)
    {
    case no_compound:
    case no_comma:
    case no_parallel:
    case no_sequential:
    case no_actionlist:
    case no_orderedactionlist:
        {
            HqlExprArray args;
            ForEachChild(idx, expr)
                args.append(*doTransformRootExpr(expr->queryChild(idx)));
            return cloneOrLink(expr, args);
        }
    case no_buildindex:
    case no_output:
    case no_apply:
    case no_distribution:
    case no_keydiff:
    case no_keypatch:
    case no_outputscalar:
        return createTransformed(expr);         //NB: Do not common up!!!
    case no_setmeta:
        return LINK(expr);
    default:
        {
            OwnedHqlExpr transformed = transform(expr);
            ITypeInfo * type = transformed->queryType();
            if (type && type->getTypeCode() != type_void)
            {
                HqlExprArray args;
                bool isOuterWorkflow = (op == no_colon) && workflowContainsSchedule(transformed);
                assertex(!isOuterWorkflow || !workflowContainsNonSchedule(transformed));
                if (isOuterWorkflow)
                    args.append(*LINK(transformed->queryChild(0)));
                else
                    args.append(*LINK(transformed));
                nextSequence(args, NULL, NULL, NULL, true, NULL);
                IHqlExpression * ret = createSetResult(args);
                if (isOuterWorkflow)
                {
                    args.kill();
                    args.append(*ret);
                    unwindChildren(args, transformed, 1);
                    ret = transformed->clone(args);
                }
                return ret;
            }
            return LINK(transformed);
        }
    }
}


IHqlExpression * SequenceNumberAllocator::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_actionlist:
        return doTransformRootExpr(expr);
    case no_apply:
        {
            HqlExprArray args;
            args.append(*transform(expr->queryChild(0)));
            applyDepth++;
            args.append(*transform(expr->queryChild(1)));
            applyDepth--;
            OwnedHqlExpr ret = completeTransform(expr, args);
            return attachSequenceNumber(ret);
        }
    case no_outputscalar:
        if (applyDepth)
        {
            translator.WARNINGAT(CategoryUnexpected, expr, HQLERR_ScalarOutputWithinApply);
        }
        break;
    case no_parallel:
    case no_sequential:
    case no_orderedactionlist:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                OwnedHqlExpr next = transform(expr->queryChild(i));
                if (!next->isAction())
                {
                    if (!next->isConstant())
                        next.setown(createValue(no_evaluate_stmt, makeVoidType(), next.getClear()));
                    else
                        next.clear();
                }

                if (next)
                    args.append(*next.getClear());
            }
            return expr->clone(args);
        }
    }
    Owned<IHqlExpression> transformed = NewHqlTransformer::createTransformed(expr);
    return attachSequenceNumber(transformed.get());
}

static IAtom * queryOverwriteAction(IHqlExpression * expr)
{
    if (expr->hasAttribute(extendAtom))
        return extendAtom;
    if (expr->hasAttribute(overwriteAtom))
        return overwriteAtom;
    if (expr->hasAttribute(noOverwriteAtom))
        return noOverwriteAtom;
    return NULL;
}

IHqlExpression * SequenceNumberAllocator::attachSequenceNumber(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_buildindex:
    case no_output:
    case no_apply:
    case no_distribution:
    case no_keydiff:
    case no_keypatch:
        {
            queryExtra(expr)->setGetsSequence();
            HqlExprArray args;
            unwindChildren(args, expr);
            bool duplicate = false;
            nextSequence(args, queryResultName(expr), queryOverwriteAction(expr), expr->queryChild(0), true, &duplicate);
            args.append(*createUniqueId());
            return expr->clone(args);
        }
        break;
    case no_outputscalar:
        {
            IHqlExpression * name = queryResultName(expr);
            queryExtra(expr)->setGetsSequence();
            HqlExprArray args;
            args.append(*LINK(expr->queryChild(0)));
            bool duplicate = false;
            nextSequence(args, name, queryOverwriteAction(expr), expr->queryChild(0), true, &duplicate);
            if (name)
                args.append(*createAttribute(namedAtom, LINK(name)));
            args.append(*createAttribute(outputAtom));
            args.append(*createUniqueId());
            IHqlExpression *noXpath = expr->queryAttribute(noXpathAtom);
            if (noXpath)
                args.append(*LINK(noXpath));
            gatherAttributes(args, xmlnsAtom, expr);
            return createSetResult(args);
        }
    default:
        return LINK(expr);
    }
}

void HqlCppTranslator::allocateSequenceNumbers(HqlExprArray & exprs)
{
    HqlExprArray sequenced;
    SequenceNumberAllocator transformer(*this);
    transformer.transformRoot(exprs, sequenced);
    replaceArray(exprs, sequenced);
    maxSequence = transformer.getMaxSequence();
}

//---------------------------------------------------------------------------

static void replaceAssignSelector(HqlExprArray & assigns, IHqlExpression * newSelector)
{
    ForEachItemIn(idx, assigns)
    {
        IHqlExpression & cur = assigns.item(idx);
        IHqlExpression * lhs = cur.queryChild(0);
        IHqlExpression * rhs = cur.queryChild(1);
        assigns.replace(*createAssign(replaceSelector(lhs, queryActiveTableSelector(), newSelector), replaceSelector(rhs, queryActiveTableSelector(), newSelector)), idx);
    }
}




static IHqlExpression * createArith(node_operator op, ITypeInfo * type, IHqlExpression * numerator, IHqlExpression * denominator)
{
    return createValue(op, LINK(type), ensureExprType(numerator, type), ensureExprType(denominator, type));
}



//MORE: This might be better as a class, it would reduce the number of parameters
IHqlExpression * doNormalizeAggregateExpr(IHqlExpression * selector, IHqlExpression * expr, HqlExprArray & fields, HqlExprArray & assigns, bool & extraSelectNeeded, bool canOptimizeCasts);
IHqlExpression * evalNormalizeAggregateExpr(IHqlExpression * selector, IHqlExpression * expr, HqlExprArray & fields, HqlExprArray & assigns, bool & extraSelectNeeded, bool canOptimizeCasts)
{
    switch (expr->getOperator())
    {
    case no_avegroup:
        //Map this to sum(x)/count(x)
        {
            IHqlExpression * arg = expr->queryChild(0);
            IHqlExpression * cond = expr->queryChild(1);

            Owned<ITypeInfo> sumType = getSumAggType(arg);
            ITypeInfo * exprType = expr->queryType();
            OwnedHqlExpr sum = createValue(no_sumgroup, LINK(sumType), LINK(arg), LINK(cond));
            OwnedHqlExpr count = createValue(no_countgroup, LINK(defaultIntegralType), LINK(cond));

            //average should be done as a real operation I think, possibly decimal, if argument is decimal
            OwnedHqlExpr avg = createArith(no_div, exprType, sum, count);
            return doNormalizeAggregateExpr(selector, avg, fields, assigns, extraSelectNeeded, false);
        }
    case no_vargroup:
        //Map this to (sum(x^2)-sum(x)^2/count())/count()
        {
            IHqlExpression * arg = expr->queryChild(0);
            IHqlExpression * cond = expr->queryChild(1);

            ITypeInfo * exprType = expr->queryType();
            OwnedHqlExpr xx = createArith(no_mul, exprType, arg, arg);
            OwnedHqlExpr sumxx = createValue(no_sumgroup, LINK(exprType), LINK(xx), LINK(cond));
            OwnedHqlExpr sumx = createValue(no_sumgroup, LINK(exprType), LINK(arg), LINK(cond));
            OwnedHqlExpr count = createValue(no_countgroup, LINK(defaultIntegralType), LINK(cond));

            //average should be done as a real operation I think, possibly decimal, if argument is decimal
            OwnedHqlExpr n1 = createArith(no_mul, exprType, sumx, sumx);
            OwnedHqlExpr n2 = createArith(no_div, exprType, n1, count);
            OwnedHqlExpr n3 = createArith(no_sub, exprType, sumxx, n2);
            OwnedHqlExpr n4 = createArith(no_div, exprType, n3, count);
            return doNormalizeAggregateExpr(selector, n4, fields, assigns, extraSelectNeeded, false);
        }
    case no_covargroup:
        //Map this to (sum(x.y)-sum(x).sum(y)/count())/count()
        {
            IHqlExpression * argX = expr->queryChild(0);
            IHqlExpression * argY = expr->queryChild(1);
            IHqlExpression * cond = expr->queryChild(2);

            ITypeInfo * exprType = expr->queryType();
            OwnedHqlExpr xy = createArith(no_mul, exprType, argX, argY);
            OwnedHqlExpr sumxy = createValue(no_sumgroup, LINK(exprType), LINK(xy), LINK(cond));
            OwnedHqlExpr sumx = createValue(no_sumgroup, LINK(exprType), LINK(argX), LINK(cond));
            OwnedHqlExpr sumy = createValue(no_sumgroup, LINK(exprType), LINK(argY), LINK(cond));
            OwnedHqlExpr count = createValue(no_countgroup, LINK(defaultIntegralType), LINK(cond));

            //average should be done as a real operation I think, possibly decimal, if argument is decimal
            OwnedHqlExpr n1 = createArith(no_mul, exprType, sumx, sumy);
            OwnedHqlExpr n2 = createArith(no_div, exprType, n1, count);
            OwnedHqlExpr n3 = createArith(no_sub, exprType, sumxy, n2);
            OwnedHqlExpr n4 = createArith(no_div, exprType, n3, count);
            return doNormalizeAggregateExpr(selector, n4, fields, assigns, extraSelectNeeded, false);
        }
    case no_corrgroup:
        //Map this to (covar(x,y)/(var(x).var(y)))
        //== (sum(x.y)*count() - sum(x).sum(y))/sqrt((sum(x.x)*count()-sum(x)^2) * (sum(y.y)*count()-sum(y)^2))
        {
            IHqlExpression * argX = expr->queryChild(0);
            IHqlExpression * argY = expr->queryChild(1);
            IHqlExpression * cond = expr->queryChild(2);

            ITypeInfo * exprType = expr->queryType();
            OwnedHqlExpr xx = createArith(no_mul, exprType, argX, argX);
            OwnedHqlExpr sumxx = createValue(no_sumgroup, LINK(exprType), LINK(xx), LINK(cond));
            OwnedHqlExpr xy = createArith(no_mul, exprType, argX, argY);
            OwnedHqlExpr sumxy = createValue(no_sumgroup, LINK(exprType), LINK(xy), LINK(cond));
            OwnedHqlExpr yy = createArith(no_mul, exprType, argY, argY);
            OwnedHqlExpr sumyy = createValue(no_sumgroup, LINK(exprType), LINK(yy), LINK(cond));
            OwnedHqlExpr sumx = createValue(no_sumgroup, LINK(exprType), LINK(argX), LINK(cond));
            OwnedHqlExpr sumy = createValue(no_sumgroup, LINK(exprType), LINK(argY), LINK(cond));
            OwnedHqlExpr count = createValue(no_countgroup, LINK(defaultIntegralType), LINK(cond));

            OwnedHqlExpr n1 = createArith(no_mul, exprType, sumxy, count);
            OwnedHqlExpr n2 = createArith(no_mul, exprType, sumx, sumy);
            OwnedHqlExpr n3 = createArith(no_sub, exprType, n1, n2);

            OwnedHqlExpr n4 = createArith(no_mul, exprType, sumxx, count);
            OwnedHqlExpr n5 = createArith(no_mul, exprType, sumx, sumx);
            OwnedHqlExpr n6 = createArith(no_sub, exprType, n4, n5);

            OwnedHqlExpr n7 = createArith(no_mul, exprType, sumyy, count);
            OwnedHqlExpr n8 = createArith(no_mul, exprType, sumy, sumy);
            OwnedHqlExpr n9 = createArith(no_sub, exprType, n7, n8);

            OwnedHqlExpr n10 = createArith(no_mul, exprType, n6, n9);
            OwnedHqlExpr n11 = createValue(no_sqrt, LINK(exprType), LINK(n10));
            OwnedHqlExpr n12 = createArith(no_div, exprType, n3, n11);
            return doNormalizeAggregateExpr(selector, n12, fields, assigns, extraSelectNeeded, false);
        }
        throwUnexpected();

    case no_variance:
    case no_covariance:
    case no_correlation:
        throwUnexpectedOp(expr->getOperator());

    case no_count:
    case no_sum:
    case no_max:
    case no_min:
    case no_ave:
    case no_select:
    case no_exists:
    case no_field:
        // a count on a child dataset or something else - add it as it is...
        //goes wrong for count(group)*
        return LINK(expr);
    case no_countgroup:
    case no_sumgroup:
    case no_maxgroup:
    case no_mingroup:
    case no_existsgroup:
        {
            ForEachItemIn(idx, assigns)
            {
                IHqlExpression & cur = assigns.item(idx);
                if (cur.queryChild(1) == expr)
                {
                    extraSelectNeeded = true;
                    return LINK(cur.queryChild(0)); //replaceSelector(cur.queryChild(0), querySelf(), queryActiveTableSelector());
                }
            }
            IHqlExpression * targetField;
            if (selector)
            {
                targetField = LINK(selector->queryChild(1));
            }
            else
            {
                StringBuffer temp;
                temp.append("_agg_").append(assigns.ordinality());
                targetField = createField(createIdAtom(temp.str()), expr->getType(), NULL);
                extraSelectNeeded = true;
            }
            fields.append(*targetField);
            assigns.append(*createAssign(createSelectExpr(getActiveTableSelector(), LINK(targetField)), LINK(expr)));
            return createSelectExpr(getActiveTableSelector(), LINK(targetField));
        }
    case no_cast:
    case no_implicitcast:
        if (selector && canOptimizeCasts)
        {
            IHqlExpression * child = expr->queryChild(0);
            if (expr->queryType()->getTypeCode() == child->queryType()->getTypeCode())
            {
                IHqlExpression * ret = doNormalizeAggregateExpr(selector, child, fields, assigns, extraSelectNeeded, false);
                //This should be ret==child
                if (ret == selector)
                    return ret;
                HqlExprArray args;
                args.append(*ret);
                return expr->clone(args);
            }
        }
        //fallthrough...
    default:
        {
            HqlExprArray args;
            unsigned max = expr->numChildren();
            unsigned idx;
            bool diff = false;
            args.ensure(max);
            for (idx = 0; idx < max; idx++)
            {
                IHqlExpression * child = expr->queryChild(idx);
                IHqlExpression * changed = doNormalizeAggregateExpr(NULL, child, fields, assigns, extraSelectNeeded, false);
                args.append(*changed);
                if (child != changed)
                    diff = true;
            }
            if (diff)
                return expr->clone(args);
            return LINK(expr);
        }
    }
}

IHqlExpression * doNormalizeAggregateExpr(IHqlExpression * selector, IHqlExpression * expr, HqlExprArray & fields, HqlExprArray & assigns, bool & extraSelectNeeded, bool canOptimizeCasts)
{
    IHqlExpression * match = static_cast<IHqlExpression *>(expr->queryTransformExtra());
    if (match)
        return LINK(match);
    IHqlExpression * ret = evalNormalizeAggregateExpr(selector, expr, fields, assigns, extraSelectNeeded, canOptimizeCasts);
    expr->setTransformExtra(ret);
    return ret;
}


IHqlExpression * normalizeAggregateExpr(IHqlExpression * selector, IHqlExpression * expr, HqlExprArray & fields, HqlExprArray & assigns, bool & extraSelectNeeded, bool canOptimizeCasts)
{
    TransformMutexBlock block;
    return doNormalizeAggregateExpr(selector, expr, fields, assigns, extraSelectNeeded, canOptimizeCasts);
}


//---------------------------------------------------------------------------

static void appendComponent(HqlExprArray & cpts, bool invert, IHqlExpression * expr)
{
    if (invert)
        cpts.append(*createValue(no_negate, expr->getType(), LINK(expr)));
    else
        cpts.append(*LINK(expr));
}

static void expandRowComponents(HqlExprArray & cpts, bool invert, IHqlExpression * select, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            expandRowComponents(cpts, invert, select, cur);
            break;
        case no_ifblock:
            expandRowComponents(cpts, invert, select, cur->queryChild(1));
            break;
        case no_field:
            {
                OwnedHqlExpr childSelect = createSelectExpr(LINK(select), LINK(cur));
                if (!childSelect->isDatarow())
                    appendComponent(cpts, invert, childSelect);
                else
                    expandRowComponents(cpts, invert, childSelect, childSelect->queryRecord());
                break;
            }
        }
    }
}


static IHqlExpression * simplifySortlistComplexity(IHqlExpression * sortlist)
{
    if (!sortlist)
        return NULL;

    //convert concat on fixed width strings to a list of fields.
    bool same = true;
    HqlExprArray cpts;
    ForEachChild(idx, sortlist)
    {
        IHqlExpression * cpt = sortlist->queryChild(idx);
        IHqlExpression * cur = cpt;
        bool expand = false;
        bool invert = false;
        if (cpt->getOperator() == no_negate)
        {
            invert = true;
            cur = cur->queryChild(0);
        }

        if (cur->getOperator() == no_concat)
        {
            HqlExprArray concats;
            cur->unwindList(concats, no_concat);
            expand = true;
            ForEachItemIn(idxc, concats)
            {
                ITypeInfo * type = concats.item(idxc).queryType();
                unsigned tc = type->getTypeCode();
                if (!((tc == type_string || tc == type_data) && (type->getSize() != UNKNOWN_LENGTH)))
                    expand = false;
            }
            if (expand)
            {
                ForEachItemIn(idxc, concats)
                    appendComponent(cpts, invert, &concats.item(idxc));
            }
        }
        else if (cur->getOperator() == no_trim)
        {
            //Strings are always compared as if they are trimmed (or padded with arbitrary spaces)
            //=> sort by TRIM(string) can just sort by the string instead.  (Don't match LEFT/RIGHT versions.)
            if (!cur->queryChild(1))
            {
                IHqlExpression * arg = cur->queryChild(0);
                ITypeInfo * argType = arg->queryType();
                if (cur->queryType()->getTypeCode() == argType->getTypeCode())
                {
                    expand = true;
                    appendComponent(cpts, invert, arg);
                }
            }
        }
        else
        {
#if 0
            if (cur->getOperator() == no_select && cur->isDatarow() && !cur->hasAttribute(newAtom))
            {
                expand = true;
                expandRowComponents(cpts, invert, cur, cur->queryRecord());
            }
#endif
        }

        if (!expand)
            cpts.append(*LINK(cpt));
        else
            same = false;
    }
    if (!same)
        return createSortList(cpts);

    return NULL;
}

static IHqlExpression * normalizeIndexBuild(IHqlExpression * expr, bool sortIndexPayload, bool alwaysLocal, bool allowImplicitSubSort)
{
    LinkedHqlExpr dataset = expr->queryChild(0);
    IHqlExpression * buildRecord = dataset->queryRecord();

    // If any field types collate differently before and after translation to their hozed
    // format, then we need to do the translation here, otherwise this
    // sort may not be in the correct order.  (ebcdic->ascii?  integers are ok; unicode isn't!)
    // First build the sort order we need....
    HqlExprArray sorts;
    gatherIndexBuildSortOrder(sorts, expr, sortIndexPayload);

    OwnedHqlExpr sortOrder = createSortList(sorts);
    OwnedHqlExpr newsort = simplifySortlistComplexity(sortOrder);
    if (!newsort)
        newsort.set(sortOrder);

    ForEachChild(i1, expr)
    {
        IHqlExpression * cur = expr->queryChild(i1);
        if (cur->getOperator() == no_distributer)
        {
            LinkedHqlExpr ds = dataset;
            IHqlExpression * index = cur->queryChild(0);

            if (!expr->hasAttribute(sortedAtom))
            {
                if (!expr->hasAttribute(localAtom))
                {
                    HqlExprArray joinCondition;
                    IHqlExpression * indexRecord = index->queryChild(1);
                    assertex(indexRecord->numChildren() == buildRecord->numChildren());
                    unsigned numFields = firstPayloadField(index);
                    OwnedHqlExpr seq = createSelectorSequence();
                    OwnedHqlExpr left = createSelector(no_left, dataset, seq);
                    OwnedHqlExpr right = createSelector(no_right, index, seq);
                    OwnedHqlExpr cond;
                    unsigned idxLhs = 0;
                    unsigned idxRhs = 0;
                    for (unsigned i2=0; i2 < numFields; i2++)
                    {
                        IHqlExpression * lhs = createSelectExpr(LINK(left), LINK(queryNextRecordField(buildRecord, idxLhs)));
                        IHqlExpression * rhs = createSelectExpr(LINK(right), LINK(queryNextRecordField(indexRecord, idxRhs)));
                        IHqlExpression * test = createBoolExpr(no_eq, lhs, rhs);
                        extendConditionOwn(cond, no_and, test);
                    }

                    HqlExprArray args;
                    args.append(*ds.getClear());
                    args.append(*LINK(index));
                    args.append(*cond.getClear());
                    args.append(*LINK(seq));
                    ds.setown(createDataset(no_keyeddistribute, args));
                    ds.setown(cloneInheritedAnnotations(expr, ds));
                }

                ds.setown(createDataset(no_sort, ds.getClear(), createComma(LINK(newsort), createLocalAttribute())));
                ds.setown(cloneInheritedAnnotations(expr, ds));
            }

            if (expr->hasAttribute(mergeAtom))
            {
                LinkedHqlExpr sortedIndex = index;
                if (!index->hasAttribute(sortedAtom))
                {
                    HqlExprArray args;
                    unwindChildren(args, index);
                    args.append(*createAttribute(sortedAtom));
                    sortedIndex.setown(index->clone(args));
                }
                HqlExprArray sorts;
                unwindChildren(sorts, newsort);
                OwnedHqlExpr sortAttr = createExprAttribute(sortedAtom, sorts);

                HqlExprArray args;
                args.append(*LINK(ds));
                args.append(*sortedIndex.getClear());
                args.append(*createLocalAttribute());
                args.append(*replaceSelector(sortAttr, ds->queryNormalizedSelector(), queryActiveTableSelector()));
                ds.setown(createDataset(no_merge, args));
                ds.setown(cloneInheritedAnnotations(expr, ds));
            }

            HqlExprArray args;
            unwindChildren(args, expr);
            args.replace(*ds.getClear(), 0);
            args.remove(i1);
            args.append(*createAttribute(sortedAtom));
            args.append(*createLocalAttribute());
            args.append(*createAttribute(indexAtom, LINK(index->queryChild(3))));
            return expr->clone(args);
        }
    }

    IHqlExpression * distributed = expr->queryAttribute(distributedAtom);
    if (distributed && distributed->queryChild(0))
    {
        OwnedHqlExpr distribute = createDataset(no_distribute, LINK(dataset), LINK(distributed->queryChild(0)));
        distribute.setown(cloneInheritedAnnotations(expr, distribute));
        HqlExprArray args;
        args.append(*distribute.getClear());
        unwindChildren(args, expr, 1);
        args.zap(*distributed);
        return expr->clone(args);
    }


    if (!expr->hasAttribute(sortedAtom))
    {
        if (dataset->queryType()->getTypeCode() == type_groupedtable)
        {
            while (dataset->getOperator() == no_group)
                dataset.set(dataset->queryChild(0));
            if (dataset->queryType()->getTypeCode() == type_groupedtable)
            {
                dataset.setown(createDataset(no_group, LINK(dataset), NULL));
                dataset.setown(cloneInheritedAnnotations(expr, dataset));
            }
        }

        OwnedHqlExpr sorted = ensureSorted(dataset, newsort, expr, expr->hasAttribute(localAtom), true, alwaysLocal, allowImplicitSubSort, true);
        if (sorted == dataset)
            return NULL;

        sorted.setown(inheritAttribute(sorted, expr, skewAtom));
        sorted.setown(inheritAttribute(sorted, expr, thresholdAtom));

        HqlExprArray args;
        args.append(*LINK(sorted));
        unwindChildren(args, expr, 1);
        args.append(*createAttribute(sortedAtom));
        return expr->clone(args);
    }

    if (expr->hasAttribute(dedupAtom))
    {
        IHqlExpression * ds = expr->queryChild(0);
        OwnedHqlExpr seq = createSelectorSequence();
        OwnedHqlExpr mappedSortList = replaceSelector(newsort, queryActiveTableSelector(), ds);
        HqlExprArray dedupArgs;
        dedupArgs.append(*LINK(expr->queryChild(0)));
        unwindChildren(dedupArgs, mappedSortList);
        dedupArgs.append(*createLocalAttribute());
        dedupArgs.append(*LINK(seq));
        OwnedHqlExpr dedup = createDataset(no_dedup, dedupArgs);

        HqlExprArray  buildArgs;
        buildArgs.append(*cloneInheritedAnnotations(expr, dedup));
        unwindChildren(buildArgs, expr, 1);
        removeAttribute(buildArgs, dedupAtom);
        return expr->clone(buildArgs);
    }

    return NULL;
}


static HqlTransformerInfo thorHqlTransformerInfo("ThorHqlTransformer");
ThorHqlTransformer::ThorHqlTransformer(HqlCppTranslator & _translator, ClusterType _targetClusterType, IConstWorkUnit * wu)
: NewHqlTransformer(thorHqlTransformerInfo), translator(_translator), options(_translator.queryOptions())
{
    targetClusterType = _targetClusterType;
    topNlimit = options.topnLimit;
    groupAllDistribute = isThorCluster(targetClusterType) && options.groupAllDistribute;
}


IHqlExpression * ThorHqlTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = PARENT::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);

    IHqlExpression * normalized = NULL;
    switch (transformed->getOperator())
    {
    case no_group:
        normalized = normalizeGroup(transformed);
        break;
    case no_join:
    case no_selfjoin:
    case no_denormalize:
    case no_denormalizegroup:
        if (transformed->hasAttribute(groupAtom))
            normalized = normalizeJoinAndGroup(transformed);
        else
            normalized = normalizeJoinOrDenormalize(transformed);
        break;
    case no_cosort:
    case no_sort:
    case no_sorted:
    case no_assertsorted:
        normalized = normalizeSort(transformed);
        break;
    case no_subsort:
        normalized = normalizeSubSort(transformed);
        break;
    case no_cogroup:
        normalized = normalizeCoGroup(transformed);
        break;
    case no_choosen:
        normalized = normalizeChooseN(transformed);
        break;
    case no_aggregate:
        normalized = normalizeTableGrouping(transformed);
        break;
    case no_newusertable:
        normalized = normalizeTableGrouping(transformed);
        break;
    case no_newaggregate:
        normalized = normalizeTableGrouping(transformed);
        if (!normalized)
            normalized = normalizeTableToAggregate(transformed, true);
        if (!normalized || (normalized == transformed))
        {
            ::Release(normalized);
            normalized = normalizePrefetchAggregate(transformed);
        }
        break;
    case no_dedup:
        normalized = normalizeDedup(transformed);
        break;
    case no_rollup:
        normalized = normalizeRollup(transformed);
        break;
    case no_select:
        normalized = normalizeSelect(transformed);
        break;
    case no_temptable:
        normalized = normalizeTempTable(transformed);
        break;
        //MORE should do whole aggregate expression e.g., max(x)-min(x)
    case NO_AGGREGATE:
        normalized = normalizeScalarAggregate(transformed);
        break;
    case no_setresult:
        normalized = convertSetResultToExtract(transformed);
        break;
    case no_projectrow:
        {
            IHqlExpression * ds = transformed->queryChild(0);
            if (isAlwaysActiveRow(ds))
            {
                //Transform PROJECT(row, transform) to a ROW(transform') since more efficient
                OwnedHqlExpr myLeft = createSelector(no_left, ds, querySelSeq(transformed));
                OwnedHqlExpr replaced = replaceSelector(transformed->queryChild(1), myLeft, ds);
                normalized = createRow(no_createrow, LINK(replaced));
            }
            break;
        }
    case no_debug_option_value:
        //pick best engine etc. definitely done by now, so substitute any options that haven't been processed already
        return getDebugValueExpr(translator.wu(), expr);
    }

    if (normalized && (normalized != transformed))
    {
        transformed.setown(transform(normalized));
        normalized->Release();
    }

/*
    //Has a minor impact on unnecessary local attributes
    if (!translator.targetThor() && transformed->hasAttribute(localAtom) && localChangesActivityAction(transformed))
        return removeAttribute(transformed, localAtom);
*/
    return transformed.getClear();
}


static IHqlExpression * convertDedupToGroupedDedup(IHqlExpression * expr, IHqlExpression * grouping, bool compareAll)
{
    IHqlExpression * localAttr = expr->queryAttribute(localAtom);
    HqlExprArray groupArgs;
    groupArgs.append(*LINK(expr->queryChild(0)));
    groupArgs.append(*LINK(grouping));
    if (compareAll)
        groupArgs.append(*createAttribute(allAtom));
    if (localAttr)
        groupArgs.append(*LINK(localAttr));

    //Ideally this would remove the equality conditions from the dedup, but ok since they are ignored later when generating
    OwnedHqlExpr group = createDataset(no_group, groupArgs);
    group.setown(cloneInheritedAnnotations(expr, group));

    HqlExprArray dedupArgs;
    dedupArgs.append(*LINK(group));
    unwindChildren(dedupArgs, expr, 1);
    removeAttribute(dedupArgs, localAtom); //(since now a grouped dedup)
    OwnedHqlExpr ungroup = createDataset(no_group, expr->clone(dedupArgs), NULL);
    return cloneInheritedAnnotations(expr, ungroup);
}

IHqlExpression * ThorHqlTransformer::normalizeDedup(IHqlExpression * expr)
{
    if (isGroupedActivity(expr))
    {
        //MORE: It should be possible to remove ,ALL if no conditions and
        //the equalities (ignoring any grouping conditions) match the group sort order
        return NULL;
    }

    // DEDUP, ALL, local: - pre sort the data, group, dedup and ungroup
    // DEDUP, ALL, global - if just had a sort by any of the criteria then do it local
    // DEDUP, not all, not grouped, group by criteria, dedup, degroup
    DedupInfoExtractor info(expr);
    if (info.equalities.ordinality() == 0)
        return NULL;

    IHqlExpression * dataset = expr->queryChild(0);
    bool hasLocal = isLocalActivity(expr);
    bool isLocal = hasLocal || !translator.targetThor();
    bool isHashDedup = expr->hasAttribute(hashAtom);
    if (info.compareAllRows)
    {
        IHqlExpression * manyProp = expr->queryAttribute(manyAtom);
        if (!isLocal && manyProp)
        {
            //If lots of duplicates, then dedup all locally and then dedup all globally.
            HqlExprArray localArgs;
            unwindChildren(localArgs, expr);
            localArgs.zap(*manyProp);
            localArgs.append(*createLocalAttribute());

            OwnedHqlExpr localDedup = expr->clone(localArgs);
            HqlExprArray globalArgs;
            globalArgs.append(*localDedup.getClear());
            unwindChildren(globalArgs, expr, 1);
            globalArgs.zap(*manyProp);
            return expr->clone(globalArgs);
        }
    }

    //If a dedup can be done locally then force it to be local
    if (!isLocal)
    {
        OwnedHqlExpr newSort = createValueSafe(no_sortlist, makeSortListType(NULL), info.equalities);

        if (isPartitionedForGroup(dataset, newSort, info.compareAllRows))
        {
            OwnedHqlExpr ret = appendOwnedOperand(expr, createLocalAttribute());
            //A global all join implies hash (historically) so preserve that semantic
            if (info.compareAllRows && !isHashDedup)
                return appendOwnedOperand(ret, createAttribute(hashAtom));
            return ret.getClear();
        }
    }

    //DEDUP,ALL
    if (info.compareAllRows)
    {
        OwnedHqlExpr groupOrder = createValueSafe(no_sortlist, makeSortListType(NULL), info.equalities);
        bool checkLocal = isLocal || (options.supportsMergeDistribute && !isHashDedup);

        //If the dataset is already sorted for deduping, (and no extra tests) then
        //if local can just remove the ALL attribute, since the records are already adjacent.
        //if global remove the all, but enclose the dedup in a group to avoid serial processing
        //Ignore HASH if specified since this has to be more efficient.
        if (info.conds.ordinality() == 0)
        {
            bool alreadySorted = isSortedForGroup(dataset, groupOrder, checkLocal);
            if (alreadySorted)
            {
                OwnedHqlExpr noHash = removeAttribute(expr, hashAtom);
                OwnedHqlExpr noAll = removeAttribute(noHash, allAtom);
                if (isLocal)
                    return noAll.getClear();
                return convertDedupToGroupedDedup(noAll, groupOrder, checkLocal && !isLocal);
            }
        }

        if (!isHashDedup)
        {
            //If has post non equality condition, change it to a group all->dedup->ungroup
            if (info.conds.ordinality())
                return convertDedupToGroupedDedup(expr, groupOrder, true);

            //If local and thor (since hash dedup may overflow) convert to sort, dedup(not all)
            //Otherwise a hashdedup is likely to be more efficient - since it will be linear cf O(NlnN) for the sort
            if (hasLocal && translator.targetThor())
            {
                HqlExprArray dedupArgs;
                dedupArgs.append(*ensureSortedForGroup(dataset, groupOrder, true, false, options.implicitGroupSubSort));
                unwindChildren(dedupArgs, expr, 1);
                removeAttribute(dedupArgs, allAtom);
                return expr->clone(dedupArgs);
            }
            else
            {
                if (matchesConstantValue(info.numToKeep, 1) && !info.keepLeft)
                    return appendOwnedOperand(expr, createAttribute(hashAtom));
            }
        }
    }
    else
    {
        //Convert dedup(ds, exprs) to group(dedup(group(ds, exprs), exprs))
        //To ensure that the activity isn't executed serially.
        if (!isLocal && !areConstant(info.equalities))
        {
            OwnedHqlExpr groupOrder = createValueSafe(no_sortlist, makeSortListType(NULL), info.equalities);
            return convertDedupToGroupedDedup(expr, groupOrder, false);
        }
    }

    return NULL;
}

IHqlExpression * ThorHqlTransformer::normalizeRollup(IHqlExpression * expr)
{
    if (isGroupedActivity(expr))
        return NULL;

    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * cond = expr->queryChild(1);
    if (isThorCluster(targetClusterType) && !expr->queryAttribute(localAtom) && isIndependentOfScope(expr))
    {
        HqlExprArray equalities;
        OwnedHqlExpr extra;
        if (cond->getOperator() == no_sortlist)
            cond->unwindList(equalities, no_sortlist);
        else if (!cond->isBoolean())
            equalities.append(*LINK(cond));
        else
        {
            HqlExprArray terms;
            cond->unwindList(terms, no_and);
            OwnedHqlExpr left = createSelector(no_left, dataset, querySelSeq(expr));
            OwnedHqlExpr right = createSelector(no_right, dataset, querySelSeq(expr));
            ForEachItemIn(i, terms)
            {
                IHqlExpression & cur = terms.item(i);
                bool matched = false;
                if (cur.getOperator() == no_eq)
                {
                    OwnedHqlExpr mappedLeft = replaceSelector(cur.queryChild(0), left, dataset);
                    OwnedHqlExpr mappedRight = replaceSelector(cur.queryChild(1), right, dataset);
                    if (mappedLeft == mappedRight)
                    {
                        equalities.append(*LINK(mappedLeft));
                        matched = true;
                    }
                }
                if (!matched)
                    extendConditionOwn(extra, no_and, LINK(&cur));
            }
        }

        //Don't create a group by constant - it will kill thor!
        ForEachItemInRev(ie, equalities)
            if (equalities.item(ie).isConstant())
                equalities.remove(ie);

        if (equalities.ordinality())
        {
            OwnedHqlExpr left = createSelector(no_left, dataset, querySelSeq(expr));

            //If anything in the join condition references LEFT then the whole condition is currently passed the modified row
            //so remove any fields that are modified in the transform
            HqlExprArray ambiguousSelects;
            if (cond->usesSelector(left))
                filterAmbiguousRollupCondition(ambiguousSelects, equalities, expr);

            if (equalities.ordinality() == 0)
            {
                translator.reportWarning(CategoryEfficiency, SeverityUnknown, queryLocation(expr), ECODETEXT(HQLWRN_AmbiguousRollupNoGroup));
            }
            else
            {
                OwnedHqlExpr groupOrder = createValueSafe(no_sortlist, makeSortListType(NULL), equalities);

                if (isPartitionedForGroup(dataset, groupOrder, false))
                    return appendOwnedOperand(expr, createLocalAttribute());

                //This list can only contain items if the filter is using left/right => expand to an equality
                IHqlExpression * selector = dataset->queryNormalizedSelector();
                OwnedHqlExpr right = createSelector(no_right, dataset, querySelSeq(expr));
                ForEachItemIn(i, ambiguousSelects)
                {
                    IHqlExpression * select = &ambiguousSelects.item(i);
                    OwnedHqlExpr leftSelect = replaceSelector(select, selector, left);
                    OwnedHqlExpr rightSelect = replaceSelector(select, selector, right);
                    IHqlExpression * eq = createBoolExpr(no_eq, leftSelect.getClear(), rightSelect.getClear());
                    extendConditionOwn(extra, no_and, eq);
                }

                HqlExprArray groupArgs, rollupArgs;
                groupArgs.append(*LINK(dataset));
                groupArgs.append(*LINK(groupOrder));

                OwnedHqlExpr group = createDataset(no_group, groupArgs);
                group.setown(cloneInheritedAnnotations(expr, group));
                rollupArgs.append(*LINK(group));
                if (extra)
                    rollupArgs.append(*extra.getClear());
                else
                    rollupArgs.append(*createConstant(true));
                unwindChildren(rollupArgs, expr, 2);
                OwnedHqlExpr ungroup = createDataset(no_group, expr->clone(rollupArgs), NULL);
                return cloneInheritedAnnotations(expr, ungroup);
            }
        }
    }

    return NULL;
}


IHqlExpression * ThorHqlTransformer::skipOverGroups(IHqlExpression * dataset, bool isLocal)
{
    //if grouping a group, remove the initial group.
    //Not completely sure about this - it may potentially cause extra splitters.
    IHqlExpression * newDataset = dataset;
    while (newDataset->getOperator() == no_group)
    {
        if (newDataset->hasAttribute(allAtom))
            break;

        if (isLocal && queryRealChild(newDataset, 1))
        {
            //NOTE: local groups should not remove preceding non-local groups.
            if (translator.targetThor() && !newDataset->hasAttribute(localAtom))
                break;
        }
        newDataset = newDataset->queryChild(0);
    }
    return newDataset;
}


IHqlExpression * ThorHqlTransformer::skipGroupsWithinGroup(IHqlExpression * expr, bool isLocal)
{
    //if grouping a group, remove the initial group.
    //Not completely sure about this - it may potentially cause extra splitters.
    IHqlExpression * dataset = expr->queryChild(0);
    if (dataset->getOperator() == no_group)
    {
        IHqlExpression * newDataset = skipOverGroups(dataset, isLocal);
        if (newDataset == dataset)
            return NULL;

        //if we end up with the original grouping then probably have ungroup(group(x,y))
        //so no need to do this group either
        if (queryGrouping(newDataset) == queryGrouping(expr))
            return LINK(newDataset);

        return replaceChild(expr, 0, newDataset);
    }

    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeGroup(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_group);
    IHqlExpression * sortlist = queryRealChild(expr, 1);
    IHqlExpression * dataset = expr->queryChild(0);
    if (!sortlist)
        return skipGroupsWithinGroup(expr, false);

    OwnedHqlExpr newsort = simplifySortlistComplexity(sortlist);
    if (newsort)
        return replaceChild(expr, 1, newsort);

    bool hasLocal = expr->hasAttribute(localAtom);
    bool isLocal = hasLocal || !translator.targetThor();
    bool wantSorted = expr->hasAttribute(sortedAtom);
    bool hasAll = expr->hasAttribute(allAtom);

    // First check if a global group can be done locally - applicable to all and non-all versions.
    if (!isLocal)
    {
        if (!wantSorted && isPartitionedForGroup(dataset, sortlist, hasAll))
            return appendLocalAttribute(expr);
    }

    if (!hasAll)
    {
        //if grouping a group, remove the initial group.
        //Not completely sure about this - it may potentially cause extra splitters.
        return skipGroupsWithinGroup(expr, isLocal);
    }

    //First check to see if the dataset is already sorted by the group criteria, or more.
    //The the data could be globally sorted, but not distributed, and this is likely to be more efficient than redistributing...
    OwnedHqlExpr sorted = ensureSortedForGroup(dataset, sortlist, hasLocal, !translator.targetThor(), options.implicitGroupSubSort);
    if (sorted == dataset)
        return removeAttribute(expr, allAtom);
    sorted.setown(cloneInheritedAnnotations(expr, sorted));
    sorted.setown(inheritAttribute(sorted, expr, skewAtom));
    sorted.setown(inheritAttribute(sorted, expr, thresholdAtom));

    if (!isLocal)
    {
        //Options for ensuring distributed and locally sorted (in order)
        // DISTRIBUTE,MERGE - since lightweight and streaming.
        // DISTRIBUTE,LOCAL SORT
        // SORT
        if (!wantSorted)
        {
            //is it best to hash on all the grouping fields, or just some of them?  Do all for the moment.
            OwnedHqlExpr hashed = createValue(no_hash32, LINK(unsignedType), LINK(sortlist), createAttribute(internalAtom));
            if (options.supportsMergeDistribute && isSortedForGroup(dataset, sortlist, true))
            {
                //Dataset is locally sorted, so can use the merge distribute to remove the subsequent local sort.
                //changing a heavyweight global sort into a lightweight distribute,merge
                OwnedHqlExpr sortOrder = getExistingSortOrder(dataset, true, true);
                OwnedHqlExpr mergeAttr = createExprAttribute(mergeAtom, replaceSelector(sortOrder, queryActiveTableSelector(), dataset));
                sorted.setown(createDatasetF(no_distribute, LINK(dataset), LINK(hashed), mergeAttr.getClear(), NULL));
                sorted.setown(cloneInheritedAnnotations(expr, sorted));
            }
            else
            {
                if (groupAllDistribute || expr->hasAttribute(unsortedAtom))
                {
                    OwnedHqlExpr distributed = createDataset(no_distribute, LINK(dataset), LINK(hashed));
                    distributed.setown(cloneInheritedAnnotations(expr, distributed));
                    sorted.setown(createDataset(no_sort, LINK(distributed), createComma(LINK(sortlist), createLocalAttribute())));
                    sorted.setown(cloneInheritedAnnotations(expr, sorted));
                }
            }
        }
#ifdef _DEBUG
        assertex(!sortlist->isPure() || isPartitionedForGroup(sorted, sortlist, true)); // sanity check
#endif
    }

    //Do a local group after the sort because we know they can't overlap...
    OwnedHqlExpr ret = createDatasetF(no_group, sorted.getClear(), LINK(sortlist), createLocalAttribute(), NULL);
    return expr->cloneAllAnnotations(ret);
}

IHqlExpression * ThorHqlTransformer::normalizeCoGroup(IHqlExpression * expr)
{
    IHqlExpression * grouping = queryAttributeChild(expr, groupAtom, 0);
    OwnedHqlExpr newsort = simplifySortlistComplexity(grouping);
    if (newsort)
    {
        OwnedHqlExpr newGroup = createExprAttribute(groupAtom, newsort.getClear());
        return replaceOwnedAttribute(expr, newGroup.getClear());
    }

    HqlExprArray inputs;
    //Gather the inputs and ensure they aren't grouped.
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!cur->isAttribute())
        {
            if (isGrouped(cur))
            {
                OwnedHqlExpr ungroup = createDataset(no_group, LINK(cur));
                inputs.append(*cloneInheritedAnnotations(expr, ungroup));
            }
            else
                inputs.append(*LINK(cur));
        }
    }

    bool hasLocal = expr->hasAttribute(localAtom);
    bool alwaysLocal = !translator.targetThor();
    bool isLocal = hasLocal || alwaysLocal;
    OwnedHqlExpr localFlag = !alwaysLocal ? createLocalAttribute() : NULL;
    OwnedHqlExpr bestSortOrder;
    //Choose the best existing sort order (for the moment assume the shortest - although
    //even better would be to pick the shortest most frequent
    ForEachItemIn(iBest, inputs)
    {
        if (isSortedForGroup(&inputs.item(iBest), grouping, true))
        {
            OwnedHqlExpr localOrder = getExistingSortOrder(&inputs.item(iBest), true, true);
            if (!bestSortOrder || (localOrder->numChildren() < bestSortOrder->numChildren()))
                bestSortOrder.set(localOrder);
        }
    }

    if (!isLocal)
    {
        //Ensure all the inputs are co-distributed (use an existing distribution if possible)
        //Even better would be to pick the most frequent
        OwnedHqlExpr distribution;
        ForEachItemIn(i, inputs)
        {
            IHqlExpression & cur = inputs.item(i);
            if (isPartitionedForGroup(&cur, grouping, true))
            {
                IHqlExpression * curDistribution = queryDistribution(&cur);
                if (!isSortDistribution(curDistribution))
                {
                    distribution.set(curDistribution);
                    break;
                }
            }
        }

        if (!distribution)
            distribution.setown(createValue(no_hash32, LINK(unsignedType), LINK(grouping), createAttribute(internalAtom)));

        ForEachItemIn(iReplace, inputs)
        {
            IHqlExpression & cur = inputs.item(iReplace);
            if (queryDistribution(&cur) != distribution)
            {
                OwnedHqlExpr mappedDistribution = replaceSelector(distribution, queryActiveTableSelector(), &cur);
                OwnedHqlExpr mergeAttr;
                if (bestSortOrder && isAlreadySorted(&cur, bestSortOrder, true, true))
                    mergeAttr.setown(createExprAttribute(mergeAtom, replaceSelector(bestSortOrder, queryActiveTableSelector(), &cur)));
                OwnedHqlExpr distributedInput = createDatasetF(no_distribute, LINK(&cur), LINK(mappedDistribution), mergeAttr.getClear(), NULL);
                distributedInput.setown(cloneInheritedAnnotations(expr, distributedInput));
                inputs.replace(*distributedInput.getClear(), iReplace);
            }
        }
    }

    OwnedHqlExpr merged;
    if (bestSortOrder)
    {
        //If some of the datasets are sorted then sort the remaining inputs by the same order and merge
        HqlExprArray sortedInputs;
        ForEachItemIn(i, inputs)
        {
            IHqlExpression & cur = inputs.item(i);
            OwnedHqlExpr mappedOrder = replaceSelector(bestSortOrder, queryActiveTableSelector(), &cur);
            sortedInputs.append(*ensureSorted(&cur, mappedOrder, expr, true, true, alwaysLocal, options.implicitSubSort, false));
        }
        HqlExprArray sortedArgs;
        unwindChildren(sortedArgs, bestSortOrder);
        sortedInputs.append(*createExprAttribute(sortedAtom, sortedArgs));
        if (localFlag)
            sortedInputs.append(*LINK(localFlag));
        merged.setown(createDataset(no_merge, sortedInputs));
    }
    else
    {
        //otherwise append the datasets and then sort them all
        OwnedHqlExpr appended = createDataset(no_addfiles, inputs);
        appended.setown(cloneInheritedAnnotations(expr, appended));
        OwnedHqlExpr mappedOrder = replaceSelector(grouping, queryActiveTableSelector(), appended);
        merged.setown(createDatasetF(no_sort, LINK(appended), mappedOrder.getClear(), LINK(localFlag), NULL));
    }

    //Now group by the grouping condition
    merged.setown(cloneInheritedAnnotations(expr, merged));
    OwnedHqlExpr mappedGrouping = replaceSelector(grouping, queryActiveTableSelector(), merged);
    OwnedHqlExpr grouped = createDataset(no_group, LINK(merged), mappedGrouping.getClear());
    return expr->cloneAllAnnotations(grouped);
}

static bool canReorderMatchExistingLocalSort(HqlExprArray & newElements1, HqlExprArray & newElements2, IHqlExpression * ds1, Shared<IHqlExpression> & ds2, const HqlExprArray & elements1, const HqlExprArray & elements2, bool canSubSort, bool isLocal, bool alwaysLocal)
{
    newElements1.kill();
    newElements2.kill();
    if (reorderMatchExistingLocalSort(newElements1, newElements2, ds1, elements1, elements2))
    {
        if (isAlreadySorted(ds2, newElements2, isLocal||alwaysLocal, true))
            return true;

        if (canSubSort && isWorthShuffling(ds2, newElements2, isLocal||alwaysLocal, true))
        {
            OwnedHqlExpr subsorted = getSubSort(ds2, newElements2, isLocal, true, alwaysLocal);
            if (subsorted)
            {
                ds2.swap(subsorted);
                return true;
            }
        }
    }
    return false;
}


bool ThorHqlTransformer::isLightweightJoinCandidate(IHqlExpression * expr, bool isLocal, bool isLimitedSubstringJoin)
{
    //This is equally applicable to hthor and roxie.  However non lookup joins currently generate group activities on
    //the inputs which look less efficient.  It may still be better to enable it though.
    if (!translator.targetThor())
        return false;

    if (!options.spotLocalMerge || isLimitedSubstringJoin || !isLocal)
        return false;

    if (expr->hasAttribute(_lightweight_Atom))
        return false;

    switch (expr->getOperator())
    {
    case no_join:
    case no_selfjoin:
    case no_denormalizegroup:
    case no_denormalize:
        return true;
    }
    return false;
}

static IHqlExpression * createDistributedInput(IHqlExpression * ds, IHqlExpression * sortlist, bool internal)
{
    //could use a more optimal hash function since comparing against self, so fields are same type
    IHqlExpression * internalExpr = internal ?  createAttribute(internalAtom) : NULL;
    OwnedHqlExpr activeDist = createValue(no_hash32, LINK(unsignedType), LINK(sortlist), internalExpr);
    OwnedHqlExpr dist = replaceSelector(activeDist, queryActiveTableSelector(), ds);
    return createDataset(no_distribute, LINK(ds), LINK(dist));
}

static IHqlExpression * createDistributedInput(IHqlExpression * ds, const HqlExprArray & sorts, bool internal)
{
    OwnedHqlExpr sortlist = createValueSafe(no_sortlist, makeSortListType(NULL), sorts);
    return createDistributedInput(ds, sortlist, internal);
}

/*

Perform the following transformation:

R := JOIN(l, r, LEFT.key = RIGHT.key AND fuzzy(LEFT,RIGHT), t(LEFT,RIGHT), GROUP(LEFT.id1, LEFT.id2), ATMOST(optional))

DL := DISTRIBUTE(L, HASH(key));
DR := DISTRIBUTE(R, HASH(key));
SL := SORT(DL, id, LOCAL);          // Later replace this with a LEFTSORT() attribute on the join (so can optimize self join)
//If it is a self join, SR == SL
JR := JOIN(SL, DR, LEFT.key = RIGHT.key, t(LEFT,RIGHT), LOOKUP MANY, LOCAL);
DJ := DISTRIBUTE(J, HASH(leftid1, leftid2), MERGE(leftid1, leftid2));
R := GROUP(DJ, leftid1, leftid2, LOCAL);

*/

IHqlExpression * ThorHqlTransformer::normalizeJoinAndGroup(IHqlExpression * expr)
{
    IHqlExpression * oldLeft = expr->queryChild(0);
    IHqlExpression * oldRight = expr->queryChild(1);
    LinkedHqlExpr newLeft = oldLeft;
    LinkedHqlExpr newRight = oldRight;
    IHqlExpression * groupOrder = queryAttributeChild(expr, groupAtom, 0);
    node_operator op = expr->getOperator();

    bool hasLocal = isLocalActivity(expr);
    bool alwaysLocal = !translator.targetThor();
    if (!hasLocal && !alwaysLocal)
    {
        JoinSortInfo joinInfo(expr);
        joinInfo.findJoinSortOrders(false);

        OwnedHqlExpr leftList = createValueSafe(no_sortlist, makeSortListType(NULL), joinInfo.queryLeftReq());
        OwnedHqlExpr mappedLeftList = replaceSelector(leftList, queryActiveTableSelector(), newLeft->queryNormalizedSelector());
        OwnedHqlExpr hashLeft = createValue(no_hash32, makeIntType(4, false), mappedLeftList.getClear());
        newLeft.setown(createDataset(no_distribute, LINK(newLeft), LINK(hashLeft)));

        if (oldRight == oldLeft)
            newRight.set(newLeft);
        else if (op != no_selfjoin)
        {
            OwnedHqlExpr rightList = createValueSafe(no_sortlist, makeSortListType(NULL), joinInfo.queryRightReq());
            OwnedHqlExpr mappedRightList = replaceSelector(rightList, queryActiveTableSelector(), newRight->queryNormalizedSelector());
            OwnedHqlExpr hashRight = createValue(no_hash32, makeIntType(4, false), mappedRightList.getClear());
            newRight.setown(createDataset(no_distribute, LINK(newRight), LINK(hashRight)));
        }
    }

    OwnedHqlExpr newLocalAttr = alwaysLocal ? NULL : createLocalAttribute();

    //Sort the left hand dataset into grouping order.
    assertex(groupOrder);
    OwnedHqlExpr left = createSelector(no_left, expr->queryChild(0), querySelSeq(expr));
    OwnedHqlExpr leftSortOrder = replaceSelector(groupOrder, left, newLeft);
    newLeft.setown(createDatasetF(no_sort, newLeft.getClear(), LINK(leftSortOrder), LINK(newLocalAttr), NULL));

    if (oldRight == oldLeft)
        newRight.set(newLeft);

    //Now create the modified join
    HqlExprArray joinArgs;
    joinArgs.append(*LINK(newLeft));
    joinArgs.append(*LINK(newRight));
    unwindChildren(joinArgs, expr, 2);
    removeAttribute(joinArgs, groupAtom);
    if (!hasLocal && !alwaysLocal)
        joinArgs.append(*createLocalAttribute());
    OwnedHqlExpr newJoin = expr->clone(joinArgs);

    //Now need to map the fields from the input dataset to the join output
    NewProjectMapper2 mapper;
    mapper.setMapping(newJoin->queryChild(3));
    bool matchedAll = true;
    OwnedHqlExpr mappedOrder = mapper.collapseFields(groupOrder, left, newJoin->queryNormalizedSelector(), left, &matchedAll);
    assertex(matchedAll); // This is checked in the parser, so shouldn't be triggered here.

    //Distribute the result
    LinkedHqlExpr distributed = newJoin;
    if (!hasLocal && !alwaysLocal)
    {
        OwnedHqlExpr hashOut = createValue(no_hash32, makeIntType(4, false), LINK(mappedOrder));
        OwnedHqlExpr mergeOut = createExprAttribute(mergeAtom, LINK(mappedOrder));
        distributed.setown(createDatasetF(no_distribute, LINK(newJoin), hashOut.getClear(), mergeOut.getClear(), NULL));
    }

    //And finally group it.
    return createDatasetF(no_group, LINK(distributed), LINK(mappedOrder), LINK(newLocalAttr), NULL);
}

static IHqlExpression * queryDistributionKey(IHqlExpression * rhs)
{
    loop
    {
        switch (rhs->getOperator())
        {
        case no_filter:
        case no_metaactivity:
        case no_sorted:
            rhs = rhs->queryChild(0);
            break;
        case no_newkeyindex:
            return rhs;
        default:
            return NULL;
        }
    }
}



IHqlExpression * ThorHqlTransformer::normalizeJoinOrDenormalize(IHqlExpression * expr)
{
    IHqlExpression * leftDs = expr->queryChild(0);
    IHqlExpression * rightDs = queryJoinRhs(expr);
    IHqlExpression * seq = querySelSeq(expr);
    node_operator op = expr->getOperator();

    if (op == no_join)
    {
        if (isSelfJoin(expr))
        {
            HqlExprArray children;
            unwindChildren(children, expr);
            children.replace(*createAttribute(_selfJoinPlaceholder_Atom), 1);       // replace the 1st dataset with an attribute so parameters are still in the same place.
            OwnedHqlExpr ret = createDataset(no_selfjoin, children);
            return expr->cloneAllAnnotations(ret);
        }
    }

    bool hasLocal = isLocalActivity(expr);
    bool alwaysLocal = !translator.targetThor();
    if (alwaysLocal)
    {
        if (expr->hasAttribute(hashAtom))
            return removeAttribute(expr, hashAtom);
        //Hthor and roxie don't currently support smart joins since the normal join requires the input to be sorted
        if (expr->hasAttribute(smartAtom))
            return removeAttribute(expr, smartAtom);
    }

    bool isLocal = hasLocal || alwaysLocal;
    //hash,local doesn't make sense (hash is only used for distribution) => remove hash
    //but also prevent it being converted to a lookup join??
    if (isLocal && expr->hasAttribute(hashAtom))
        return removeAttribute(expr, hashAtom);

    //Check to see if this join should be done as a keyed join...
    if (!expr->hasAttribute(lookupAtom) && !expr->hasAttribute(smartAtom) &&
        !expr->hasAttribute(allAtom) && !expr->hasAttribute(streamedAtom))
    {
        if (rightDs->getOperator() == no_filter)
        {
            bool moveRhsFilter = false;
            if (expr->hasAttribute(keyedAtom) && queryAttributeChild(expr, keyedAtom, 0))
            {
                //Full keyed join - ensure the filter is moved from the rhs to the condition.
                moveRhsFilter = true;
            }
            else if (options.spotPotentialKeyedJoins && (rightDs != leftDs))
            {
                //This can turn some non keyed joins into keyed joins
                IHqlExpression * cur = rightDs;
                while (cur->getOperator() == no_filter)
                    cur = cur->queryChild(0);
                if (cur->getOperator() == no_newkeyindex)
                    moveRhsFilter = true;
            }

            if (moveRhsFilter)
            {
                //Transform join(a, b(x), c) to join(a, b, c and evaluate(right, x))
                HqlExprAttr extraFilter;
                OwnedHqlExpr right = createSelector(no_right, rightDs, seq);
                IHqlExpression * cur = rightDs;
                while (cur->getOperator() == no_filter)
                {
                    unsigned max = cur->numChildren();
                    for (unsigned i = 1; i < max; i++)
                    {
                        IHqlExpression * filter = cur->queryChild(i);
                        if (!filter->isAttribute())
                        {
                            IHqlExpression * newFilter = replaceSelector(filter, rightDs, right);
                            extendConditionOwn(extraFilter, no_and, newFilter);
                        }
                    }
                    cur = cur->queryChild(0);
                }
                HqlExprArray args;
                unwindChildren(args, expr);
                args.replace(*LINK(cur), 1);
                args.replace(*createValue(no_and, makeBoolType(), LINK(expr->queryChild(2)), extraFilter.getClear()), 2);
                return expr->clone(args);
            }
        }
    }

    //Tag a keyed join as ordered in the platforms that ensure it does remain ordered.  Extend if the others do.
    if (isKeyedJoin(expr))
    {
        if ((translator.targetRoxie() || options.keyedJoinPreservesOrder) && !expr->hasAttribute(_ordered_Atom) && !expr->hasAttribute(unorderedAtom))
            return appendOwnedOperand(expr, createAttribute(_ordered_Atom));
        return NULL;
    }


    JoinSortInfo joinInfo(expr);
    joinInfo.findJoinSortOrders(canBeSlidingJoin(expr));

    //If the data is already distributed so the data is on the correct machines then perform the join locally.
    //Should be equally applicable to lookup, hash, all and normal joins.
    if (!isLocal && !joinInfo.hasOptionalEqualities() && joinInfo.queryLeftReq().ordinality())
    {
        if (isDistributedCoLocally(leftDs, rightDs, joinInfo.queryLeftReq(), joinInfo.queryRightReq()))
            return appendOwnedOperand(expr, createLocalAttribute());

        if (options.createImplicitKeyedDistributeForJoin && !expr->hasAttribute(lookupAtom))
        {
            IHqlExpression * rhsKey = queryDistributionKey(rightDs);
            if (rhsKey)
            {
                IHqlExpression * indexRecord = rightDs->queryRecord();
                unsigned numUnsortedFields = numPayloadFields(rightDs);
                unsigned numKeyedFields = getFlatFieldCount(indexRecord)-numUnsortedFields;

                //To create a keyed distribute, all the keyed fields in the rhs key must have equalities
                //in the join condition.  Need to extract a join condition which just contains that subset.
                if (joinInfo.numRequiredEqualities() >= numKeyedFields)
                {
                    OwnedHqlExpr keyedCondition = joinInfo.getContiguousJoinCondition(numKeyedFields);
                    if (keyedCondition)
                    {
                        OwnedHqlExpr distribute = createDataset(no_keyeddistribute, LINK(leftDs), createComma(LINK(rhsKey), keyedCondition.getClear(), LINK(seq)));
                        HqlExprArray args;
                        args.append(*distribute.getClear());
                        unwindChildren(args, expr, 1);
                        args.append(*createLocalAttribute());
                        return expr->clone(args);
                    }
                }
            }
        }

        if (options.matchExistingDistributionForJoin)
        {
            //Should this exclude lookup joins??
            //On balance it is probably worthwhile since it means that only 1/clustersize data is on each node.

            //If left side (assumed to be the largest) is already distributed, it would be more efficient
            //to redistribute the rhs by a matching hash function (or use cosort), and then join locally.
            //Be careful about the persist scaling factors though.
            //SORT partitions should be supported once they are persisted by the system
            IHqlExpression * leftDistribution = queryDistribution(leftDs);
            if (matchesAnyDistribution(leftDistribution))
                return appendOwnedOperand(expr, createLocalAttribute());

            if (!isPersistDistribution(leftDistribution) && !isSortedDistribution(leftDistribution) && isPartitionedForGroup(leftDs, joinInfo.queryLeftReq(), true))
            {
                //MORE: May need a flag to stop this - to prevent issues with skew.
                OwnedHqlExpr newHash = createMatchingDistribution(leftDistribution, joinInfo.queryLeftReq(), joinInfo.queryRightReq());
                if (newHash)
                {
                    OwnedHqlExpr dist = replaceSelector(newHash, queryActiveTableSelector(), rightDs);
                    OwnedHqlExpr newRhs = createDataset(no_distribute, LINK(rightDs), LINK(dist));
                    OwnedHqlExpr newJoin = replaceChild(expr, 1, newRhs);
                    return appendOwnedOperand(newJoin, createLocalAttribute());
                }
            }
        }
        else
        {
            IHqlExpression * leftDistribution = queryDistribution(leftDs);
            if (!isPersistDistribution(leftDistribution) && !isSortedDistribution(leftDistribution) && isPartitionedForGroup(leftDs, joinInfo.queryLeftReq(), true))
                DBGLOG("MORE: Potential for distributed join optimization");
        }
    }

    if (joinInfo.hasOptionalEqualities() && !isLocal && !expr->hasAttribute(hashAtom) && !expr->hasAttribute(allAtom))
    {
        if (joinInfo.hasRequiredEqualities())
            return appendAttribute(expr, hashAtom);
        throwError(HQLERR_PrefixJoinRequiresEquality);
    }

    if (expr->hasAttribute(allAtom))
        return NULL;

    if (expr->hasAttribute(lookupAtom))
        return NULL;

    //Try and convert local joins to a lightweight join that doesn't require any sorting of the inputs.
    //Improves resourcing for thor, and prevents lookup conversion for hthor/roxie
    //Worthwhile even for lookup joins
    if (isLightweightJoinCandidate(expr, isLocal, joinInfo.hasOptionalEqualities()))
    {
        if (isAlreadySorted(leftDs, joinInfo.queryLeftSort(), true, true) &&
            isAlreadySorted(rightDs, joinInfo.queryRightSort(), true, true))
        {
            //If this is a lookup join without a many then we need to make sure only the first match is retained.
            return appendOwnedOperand(expr, createAttribute(_lightweight_Atom));
        }

        //Check for a local join where we can reorder the condition so both sides match the existing sort orders.
        //could special case self-join to do less work, but probably not worth the effort.
        HqlExprArray sortedLeft, sortedRight;
        if (!joinInfo.hasOptionalEqualities())
        {
            //Since the distribution and order of global joins is not defined this could probably be used for non-local as well.
            LinkedHqlExpr newLeftDs = leftDs;
            LinkedHqlExpr newRightDs = rightDs;
            bool canSubSort = options.subsortLocalJoinConditions;
            bool reordered = canReorderMatchExistingLocalSort(sortedLeft, sortedRight, newLeftDs, newRightDs,
                                                              joinInfo.queryLeftSort(), joinInfo.queryRightSort(), canSubSort, isLocal, alwaysLocal);
            //If allowed to subsort then try the otherway around
            if (!reordered && canSubSort)
                reordered = canReorderMatchExistingLocalSort(sortedRight, sortedLeft, newRightDs, newLeftDs,
                                                             joinInfo.queryRightSort(), joinInfo.queryLeftSort(), canSubSort, isLocal, alwaysLocal);

            if (reordered)
            {
                //Recreate the join condition in the correct order to match the existing sorts...
                HqlExprAttr newcond;
                OwnedHqlExpr leftSelector = createSelector(no_left, newLeftDs, seq);
                OwnedHqlExpr rightSelector = createSelector(no_right, newRightDs, seq);
                ForEachItemIn(i, sortedLeft)
                {
                    OwnedHqlExpr lc = replaceSelector(&sortedLeft.item(i), queryActiveTableSelector(), leftSelector);
                    OwnedHqlExpr rc = replaceSelector(&sortedRight.item(i), queryActiveTableSelector(), rightSelector);
                    extendConditionOwn(newcond, no_and, createValue(no_eq, makeBoolType(), lc.getClear(), rc.getClear()));
                }
                extendConditionOwn(newcond, no_and, LINK(joinInfo.extraMatch));
                HqlExprArray args;
                args.append(*newLeftDs.getClear());
                args.append(*newRightDs.getClear());
                args.append(*newcond.getClear());
                unwindChildren(args, expr, 3);
                args.append(*createAttribute(_lightweight_Atom));
                return expr->clone(args);
            }
        }
    }

    if (!isThorCluster(targetClusterType) && !expr->hasAttribute(_normalized_Atom))
    {
        if (!expr->hasAttribute(streamedAtom) && !expr->hasAttribute(smartAtom))
        {
            //Sort,Sort->join is O(NlnN) lookup join using a hash table is O(N) =>convert for hthor/roxie
            bool createLookup = false;
            if ((op == no_join) && options.convertJoinToLookup)
            {
                if ((targetClusterType == RoxieCluster) || hasFewRows(rightDs))
                    if (!isFullJoin(expr) && !isRightJoin(expr) && !expr->hasAttribute(partitionRightAtom))
                        createLookup = !expr->hasAttribute(_lightweight_Atom);
            }

            if (joinInfo.hasOptionalEqualities())
                createLookup = false;           //doesn't support it yet
            else if (createLookup && joinInfo.queryLeftSort().ordinality())
            {
                //Check this isn't going to generate a between join - if it is that takes precedence.
                if ((joinInfo.slidingMatches.ordinality() != 0) && (joinInfo.queryLeftSort().ordinality() == joinInfo.slidingMatches.ordinality()))
                    createLookup = false;
            }

            if (createLookup)
            {
                IHqlExpression * lhs = expr->queryChild(0);
                HqlExprArray args;
                if (isGrouped(lhs))
                {
                    OwnedHqlExpr ungroup = createDataset(no_group, LINK(lhs));
                    args.append(*cloneInheritedAnnotations(expr, ungroup));
                }
                else
                    args.append(*LINK(lhs));
                unwindChildren(args, expr, 1);
                args.append(*createAttribute(manyAtom));
                args.append(*createAttribute(lookupAtom));
                return expr->clone(args);
            }
        }
    }

    //Convert hash selfjoin to self-join(distribute)
    if ((op == no_selfjoin) && expr->hasAttribute(hashAtom))
    {
        assertex(!isLocal);
        if (joinInfo.hasRequiredEqualities())
        {
            OwnedHqlExpr sortlist = createValueSafe(no_sortlist, makeSortListType(NULL), joinInfo.queryLeftReq());
            OwnedHqlExpr distribute;
            //Only likely to catch this partition test if isLimitedSubstringJoin true, otherwise caught above
            if (!isPartitionedForGroup(leftDs, sortlist, true))
            {
                distribute.setown(createDistributedInput(leftDs, sortlist, true));
                distribute.setown(cloneInheritedAnnotations(expr, distribute));
            }
            else
                distribute.set(leftDs);

            HqlExprArray args;
            args.append(*LINK(distribute));
            unwindChildren(args, expr, 1);
            removeAttribute(args, hashAtom);
            args.append(*createLocalAttribute());
            return expr->clone(args);
        }
    }

    if (options.expandHashJoin && isThorCluster(targetClusterType) && expr->hasAttribute(hashAtom) && !joinInfo.hasOptionalEqualities())
    {
        HqlExprArray args;
        args.append(*createDistributedInput(leftDs, joinInfo.queryLeftReq(), false));
        args.append(*createDistributedInput(rightDs, joinInfo.queryRightReq(), false));
        unwindChildren(args, expr, 2);
        removeAttribute(args, hashAtom);
        args.append(*createLocalAttribute());
        return expr->clone(args);
    }

    if (isThorCluster(targetClusterType) && isLocal && options.implicitJoinSubSort && !expr->hasAttribute(smartAtom))
    {
        IHqlExpression * noSortAttr = expr->queryAttribute(noSortAtom);
        OwnedHqlExpr newLeft;
        OwnedHqlExpr newRight;
        if (!userPreventsSort(noSortAttr, no_left))
            newLeft.setown(getSubSort(leftDs, joinInfo.queryLeftSort(), isLocal, true, alwaysLocal));
        if (!userPreventsSort(noSortAttr, no_right))
            newRight.setown(getSubSort(rightDs, joinInfo.queryRightSort(), isLocal, true, alwaysLocal));
        if (newLeft || newRight)
        {
            HqlExprArray args;
            if (newLeft)
                args.append(*newLeft.getClear());
            else
                args.append(*LINK(leftDs));
            if (newRight)
                args.append(*newRight.getClear());
            else
                args.append(*LINK(rightDs));
            unwindChildren(args, expr, 2);
            return expr->clone(args);
        }
    }


    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeScalarAggregate(IHqlExpression * expr)
{
    OwnedHqlExpr project = convertScalarAggregateToDataset(expr);
    if (!project)
        throwUnexpected();
    IHqlExpression * field = project->queryRecord()->queryChild(0);
    OwnedHqlExpr ret = createNewSelectExpr(project.getClear(), LINK(field));
    return expr->cloneAllAnnotations(ret);
}

IHqlExpression * ThorHqlTransformer::normalizeSelect(IHqlExpression * expr)
{
    return NULL;
    /*
    The idea of this code is to convert a.b.c into normalize(a.b, a.b.c) if a.b is an out-of scope dataset
    However the following isn't good enough since the fields from a.b also need to be accessible.  We would
    need to introduce a field in the result $parent$, and also assign that across.  Subsequent references to
    a.b.xyz would need to be converted to in.parent.xyz.  It will generate very inefficient code, so not going
    to go this way at the moment.
    */
    if (!isNewSelector(expr) || !expr->isDataset())
        return NULL;

    IHqlExpression * ds = expr->queryChild(0);
    if (!ds->isDataset())
        return NULL;

    //If we are a no_select of a no_select that is also new, insert an implicit denormalized
    HqlExprArray args;
    args.append(*LINK(ds));

    OwnedHqlExpr selSeq = createSelectorSequence();
    HqlExprArray selectArgs;
    unwindChildren(selectArgs, expr);
    selectArgs.replace(*createSelector(no_left, ds, selSeq), 0);
    removeAttribute(selectArgs, newAtom);
    args.append(*expr->clone(selectArgs));

    //Create a transform self := right;
    OwnedHqlExpr right = createSelector(no_right, expr, selSeq);
    OwnedHqlExpr assign = createAssign(getSelf(expr), LINK(right));
    OwnedHqlExpr transform = createValue(no_transform, makeTransformType(LINK(expr->queryRecordType())), LINK(assign));
    args.append(*LINK(transform));
    args.append(*LINK(selSeq));

    args.append(*createAttribute(_internal_Atom));

    return createDataset(no_normalize, args);
}


IHqlExpression * ThorHqlTransformer::normalizeSort(IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * sortlist = expr->queryChild(1);
    OwnedHqlExpr newsort = simplifySortlistComplexity(sortlist);
    if (newsort)
    {
        if (newsort == sortlist)
        {
            dbglogExpr(sortlist);
            throwUnexpected();
        }

        HqlExprArray args;
        unwindChildren(args, expr);
        args.replace(*newsort.getClear(), 1);
        return expr->clone(args);
    }

    node_operator op = expr->getOperator();
    if (translator.targetThor())
    {
        if ((op == no_sort) && !isGrouped(expr) && !expr->hasAttribute(localAtom))
        {
            //sort(ds, a,b,c) - check so see if there is a previous sort distribution of sort(ds,a,b,c) if so, this sort can be done locally
            if (queryDistribution(expr) == queryDistribution(dataset))
                return appendLocalAttribute(expr);
        }
    }

    if (op == no_sorted)
    {
        IHqlExpression * normalized = normalizeSortSteppedIndex(expr, sortedAtom);
        if (normalized)
            return normalized;
    }

    bool isLocal = expr->hasAttribute(localAtom);
    bool alwaysLocal = !translator.targetThor();
    if ((op != no_assertsorted) && isAlreadySorted(dataset, sortlist, isLocal||alwaysLocal, false))
        return LINK(dataset);
    if (op == no_sorted)
        return normalizeSortSteppedIndex(expr, sortedAtom);

    //NOTE: We can't convert a global sort to a subsort because that will change the distribution
    if (options.implicitSubSort && (isLocal || alwaysLocal) && (op != no_assertsorted))
    {
        OwnedHqlExpr subsorted = getSubSort(dataset, sortlist, isLocal, false, alwaysLocal);
        if (subsorted)
            return dataset->cloneAllAnnotations(subsorted);
    }
    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeSubSort(IHqlExpression * expr)
{
    IHqlExpression * sortlist = expr->queryChild(1);
    IHqlExpression * grouping = expr->queryChild(2);
    OwnedHqlExpr newsort = simplifySortlistComplexity(sortlist);
    OwnedHqlExpr newgrouping = simplifySortlistComplexity(grouping);
    if (newsort || newgrouping)
    {
        HqlExprArray args;
        unwindChildren(args, expr);
        if (newsort)
            args.replace(*newsort.getClear(), 1);
        if (newgrouping)
            args.replace(*newgrouping.getClear(), 2);
        return expr->clone(args);
    }

    if (translator.targetThor() && !expr->hasAttribute(localAtom))
        return convertSubSortToGroupedSort(expr);

    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeSortSteppedIndex(IHqlExpression * expr, IAtom * attrName)
{
    node_operator op = expr->getOperator();
    if (op == no_assertsorted)
        return NULL;

    IHqlExpression * dataset = expr->queryChild(0);

    node_operator datasetOp = dataset->getOperator();
    if ((datasetOp == no_keyindex) || (datasetOp == no_newkeyindex))
    {
        if (!dataset->hasAttribute(attrName))
        {
            HqlExprArray selects;
            IHqlExpression * sortList = expr->queryChild(1);
            if (sortList)
            {
                OwnedHqlExpr mapped = replaceSelector(sortList, dataset->queryNormalizedSelector(), queryActiveTableSelector());
                unwindChildren(selects, mapped);
            }

            HqlExprArray args;
            unwindChildren(args, dataset);
            args.append(*createExprAttribute(attrName, selects));
            return dataset->clone(args);
        }
    }
    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeTempTable(IHqlExpression * expr)
{
#if 0
    //This would be a great improvement to the generated code, but the xml storage formats are different + it doesn't cope with ALL.
    IHqlExpression * values = expr->queryChild(0);
    ITypeInfo * valuesType = values->queryType();
    if ((values->getOperator() == no_getresult) && (valuesType->getTypeCode() == type_set))
    {
        IHqlExpression * record = expr->queryChild(1);
        if ((record->numChildren() == 1) && (valuesType->queryChildType() == record->queryChild(0)->queryType()))
        {
            HqlExprArray args;
            args.append(*LINK(record));
            args.append(*createAttribute(sequenceAtom, LINK(values->queryChild(0))));
            if (values->queryChild(1))
                args.append(*createAttribute(nameAtom, LINK(values->queryChild(1))));
            return createDataset(no_workunit_dataset, args);
        }
    }
#endif
    return NULL;
}


IHqlExpression * ThorHqlTransformer::normalizeChooseN(IHqlExpression * expr)
{
    OwnedHqlExpr first = foldHqlExpression(queryRealChild(expr, 2));
    if (first)
    {
        if (matchesConstantValue(first, 1))
        {
            HqlExprArray args;
            unwindChildren(args, expr);
            args.remove(2);
            return expr->clone(args);
        }
    }

    if (!options.spotTopN) return NULL;
    return queryConvertChoosenNSort(expr, topNlimit);
}


static IHqlExpression * extractPrefetchFields(HqlExprArray & fields, HqlExprArray & values, IHqlExpression * ds, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_newtransform:
    case no_transform:
    case no_assignall:
    case NO_AGGREGATEGROUP:
    case no_sortlist:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
                args.append(*extractPrefetchFields(fields, values, ds, expr->queryChild(i)));
            return expr->clone(args);
        }
    case no_assign:
        {
            HqlExprArray args;
            args.append(*LINK(expr->queryChild(0)));
            args.append(*extractPrefetchFields(fields, values, ds, expr->queryChild(1)));
            return expr->clone(args);
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_record:
    case no_field:
        return LINK(expr);
    }

    unsigned match = values.find(*expr);
    if (match == NotFound)
    {
        //What about preserving link counting on datasets?
        match = fields.ordinality();
        StringBuffer name;
        name.append("_f").append(match).append("_");
        IHqlExpression * field = createField(createIdAtom(name.str()), expr->getType(), NULL);
        fields.append(*field);
        values.append(*LINK(expr));
    }
    return createSelectExpr(getActiveTableSelector(), LINK(&fields.item(match)));
}


IHqlExpression * ThorHqlTransformer::normalizePrefetchAggregate(IHqlExpression * expr)
{
    //This optimization may be worth doing even if there is no prefetch attribute if the value being summed is very complicated!
    IHqlExpression * prefetch = expr->queryAttribute(prefetchAtom);
    if (!prefetch)
        return NULL;

    //Create a prefetch project for all parameters to count/sum/grouping expressions
    //and then aggregate those values.
    IHqlExpression * ds = expr->queryChild(0);
    HqlExprArray tempArgs, fields, values;
    ForEachChildFrom(i, expr, 2)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur != prefetch)
            tempArgs.append(*extractPrefetchFields(fields, values, ds, cur));
    }

    OwnedHqlExpr newRecord = createRecord(fields);
    OwnedHqlExpr self = createSelector(no_self, newRecord, NULL);
    HqlExprArray assigns;
    ForEachItemIn(iv, fields)
    {
        IHqlExpression * tgt = createSelectExpr(LINK(self), &OLINK(fields.item(iv)));
        assigns.append(*createAssign(tgt, &OLINK(values.item(iv))));
    }

    HqlExprArray args;
    args.append(*LINK(ds));
    args.append(*LINK(newRecord));
    args.append(*createValue(no_newtransform, makeTransformType(newRecord->getType()), assigns));
    args.append(*LINK(prefetch));
    OwnedHqlExpr project = createDataset(no_newusertable, args);
    project.setown(cloneInheritedAnnotations(expr, project));

    args.kill();
    args.append(*LINK(project));
    args.append(*LINK(expr->queryChild(1)));
    ForEachItemIn(i2, tempArgs)
        args.append(*replaceSelector(&tempArgs.item(i2), queryActiveTableSelector(), project->queryNormalizedSelector()));
    return expr->clone(args);
}


static IHqlExpression * convertAggregateGroupingToGroupedAggregate(IHqlExpression * expr, IHqlExpression* groupBy)
{
    IHqlExpression * dataset = expr->queryChild(0);

    HqlExprArray groupArgs;
    groupArgs.append(*LINK(dataset));
    groupArgs.append(*LINK(groupBy));
    groupArgs.append(*createAttribute(allAtom));
    unwindChildren(groupArgs, expr, 4);
    OwnedHqlExpr result = createDataset(no_group, groupArgs);

    result.setown(cloneInheritedAnnotations(expr, result));

    HqlExprArray args;
    unwindChildren(args, expr);
    args.replace(*result.getClear(), 0);
    args.remove(3); // no longer grouped.
    return expr->clone(args);
}



IHqlExpression * ThorHqlTransformer::getMergeTransform(IHqlExpression * dataset, IHqlExpression * transform)
{
    HqlExprArray args;
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            args.append(*getMergeTransform(dataset, cur));
            break;
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                IHqlExpression * lhsField = lhs->queryChild(1);
                IHqlExpression * rhs = cur->queryChild(1);
                OwnedHqlExpr selected = createSelectExpr(LINK(dataset), LINK(lhsField));
                OwnedHqlExpr newRhs;
                node_operator rhsOp = rhs->getOperator();
                switch (rhsOp)
                {
                case no_countgroup:
                case no_sumgroup:
                    newRhs.setown(createValue(no_sumgroup, selected->getType(), LINK(selected)));
                    break;
                case no_maxgroup:
                case no_mingroup:
                    newRhs.setown(createValue(rhsOp, selected->getType(), LINK(selected)));
                    break;
                case no_existsgroup:
                    newRhs.setown(createValue(no_existsgroup, selected->getType(), LINK(selected)));
                    break;
                case no_vargroup:
                case no_covargroup:
                case no_corrgroup:
                case no_avegroup:
                    throwUnexpected();
                default:
                    newRhs.set(selected);
                    break;
                }
                args.append(*createAssign(LINK(lhs), newRhs.getClear()));
                break;
            }
        default:
            args.append(*LINK(cur));
            break;
        }
    }
    return transform->clone(args);
}


//Convert table(x { count(group), sum(group, x) }, gr) to
//sort(x, gr, local) -> group(gr) -> aggregate -> distribute(merge) -> group(local) -> aggregate'
IHqlExpression * ThorHqlTransformer::normalizeMergeAggregate(IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * groupBy = expr->queryChild(3);

    //If locally distributed then don't do anything
    OwnedHqlExpr noMerge = removeAttribute(expr, mergeAtom);
    //This transformation only works for grouped aggregation
    if (!groupBy || groupBy->isAttribute())
        return noMerge.getClear();

    if (!translator.targetThor() || expr->hasAttribute(localAtom) || isPartitionedForGroup(dataset, groupBy, true))
        return noMerge.getClear();

    //Convert the aggregation (so no covariance/ave and other computed fields)
    OwnedHqlExpr normalized = normalizeTableToAggregate(noMerge, true);
    IHqlExpression * aggregate = normalized;
    if (aggregate->getOperator() != no_newaggregate)
        aggregate = aggregate->queryChild(0);
    assertex(aggregate->getOperator() == no_newaggregate);

    HqlExprArray localAggregateArgs;
    unwindChildren(localAggregateArgs, aggregate);
    removeAttribute(localAggregateArgs, hashAtom);
    removeAttribute(localAggregateArgs, mergeAtom);
    localAggregateArgs.append(*createLocalAttribute());
    localAggregateArgs.append(*createAttribute(sortedAtom));

    //Local aggregate and force a local sort order to be used
    OwnedHqlExpr localAggregate = aggregate->clone(localAggregateArgs);
    OwnedHqlExpr localGroupedAggregate = convertAggregateGroupingToGroupedAggregate(localAggregate, groupBy);
    //Ensure the group,all is transformed to a local sort, local group
    OwnedHqlExpr transformedFirstAggregate = transform(localGroupedAggregate);

    //Use distribute(,MERGE) to move rows globally, and remain sorted
    //Note grouping fields need to be mapped using the fields projected by the aggregate
    TableProjectMapper mapper(transformedFirstAggregate);
    bool groupCanBeMapped = false;
    OwnedHqlExpr mappedGrouping = mapper.collapseFields(groupBy, dataset, transformedFirstAggregate, &groupCanBeMapped);
    assertex(groupCanBeMapped);

    OwnedHqlExpr sortOrder = getExistingSortOrder(transformedFirstAggregate, true, true);
    OwnedHqlExpr mergeAttr = createExprAttribute(mergeAtom, replaceSelector(sortOrder, queryActiveTableSelector(), transformedFirstAggregate));
    OwnedHqlExpr hashed = createValue(no_hash32, LINK(unsignedType), LINK(mappedGrouping), createAttribute(internalAtom));
    OwnedHqlExpr redistributed = createDatasetF(no_distribute, LINK(transformedFirstAggregate), LINK(hashed), mergeAttr.getClear(), NULL);
    redistributed.setown(cloneInheritedAnnotations(expr, redistributed));
    OwnedHqlExpr grouped = createDatasetF(no_group, LINK(redistributed), LINK(mappedGrouping), createLocalAttribute(), NULL);
    grouped.setown(cloneInheritedAnnotations(expr, grouped));

    HqlExprArray args;
    args.append(*LINK(grouped));
    args.append(*LINK(localAggregate->queryChild(1)));
    args.append(*getMergeTransform(grouped->queryNormalizedSelector(), localAggregate->queryChild(2)));
    unwindChildren(args, localAggregate, 4);
    OwnedHqlExpr newAggregate = localAggregate->clone(args);
    if (aggregate == normalized)
        return newAggregate.getClear();
    return replaceChildDataset(normalized, newAggregate, 0);
}


IHqlExpression * ThorHqlTransformer::normalizeTableToAggregate(IHqlExpression * expr, bool canOptimizeCasts)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * record = expr->queryChild(1);
    IHqlExpression * transform = expr->queryChild(2);
    IHqlExpression * groupBy = expr->queryChild(3);

    if (!isAggregateDataset(expr))
        return NULL;

    //MORE: Should fail if asked to group by variable length field, or do max/min on variable length field.
    HqlExprArray aggregateFields;
    HqlExprArray aggregateAssigns;
    HqlExprArray extraAssigns;

    bool extraSelectNeeded = false;
    OwnedHqlExpr self = getSelf(expr);
    ForEachChild(idx, transform)
    {
        IHqlExpression * assign=transform->queryChild(idx);
        IHqlExpression * cur = assign->queryChild(0);
        IHqlExpression * src = assign->queryChild(1);
        IHqlExpression * mapped = normalizeAggregateExpr(cur, src, aggregateFields, aggregateAssigns, extraSelectNeeded, canOptimizeCasts);

        if (mapped == src)
        {
            mapped->Release();
            mapped = replaceSelector(cur, self, queryActiveTableSelector());

            // Not an aggregate - must be an expression that is used in the grouping
            aggregateFields.append(*LINK(cur->queryChild(1)));
            aggregateAssigns.append(*createAssign(LINK(mapped), LINK(src)));
        }

        // Add expression to calculate the fields to the second projection
        extraAssigns.append(*createAssign(LINK(cur), mapped));
    }

    //Now add any grouping fields.......
    IHqlExpression * newGroupBy = NULL;
    if (groupBy && !groupBy->isAttribute())
    {
        unsigned numGroupBy = groupBy->numChildren();
        HqlExprArray newGroupElement;
        for (unsigned idx = 0; idx < numGroupBy; idx++)
        {
            IHqlExpression * curGroup = groupBy->queryChild(idx);

            bool matched = false;
            ForEachItemIn(idxa, aggregateAssigns)
            {
                IHqlExpression * rhs = aggregateAssigns.item(idxa).queryChild(1);
                if (rhs->getOperator() == no_activerow)
                    rhs = rhs->queryChild(0);
                if (rhs == curGroup)
                {
                    matched = true;
                    break;
                }
            }
            if (!matched)
            {
                StringBuffer temp;
                temp.append("_agg_").append(aggregateAssigns.ordinality());
                IHqlExpression * targetField = createField(createIdAtom(temp.str()), curGroup->getType(), NULL);
                aggregateFields.append(*targetField);
                aggregateAssigns.append(*createAssign(createSelectExpr(getActiveTableSelector(), LINK(targetField)), LINK(curGroup)));
                extraSelectNeeded = true;
            }
            newGroupElement.append(*LINK(curGroup));
        }
        newGroupBy = createSortList(newGroupElement);
    }

    IHqlExpression * aggregateRecord = extraSelectNeeded ? createRecord(aggregateFields) : LINK(record);
    OwnedHqlExpr aggregateSelf = getSelf(aggregateRecord);
    replaceAssignSelector(aggregateAssigns, aggregateSelf);
    IHqlExpression * aggregateTransform = createValue(no_newtransform, makeTransformType(aggregateRecord->getType()), aggregateAssigns);

    HqlExprArray aggregateAttrs;
    unwindAttributes(aggregateAttrs, expr);
    if (!expr->hasAttribute(localAtom) && newGroupBy && !isGrouped(dataset) && isPartitionedForGroup(dataset, newGroupBy, true))
        aggregateAttrs.append(*createLocalAttribute());

    OwnedHqlExpr ret = createDataset(no_newaggregate, LINK(dataset), createComma(aggregateRecord, aggregateTransform, newGroupBy, createComma(aggregateAttrs)));
    if (extraSelectNeeded)
        ret.setown(cloneInheritedAnnotations(expr, ret));
    else
        ret.setown(expr->cloneAllAnnotations(ret));

    if (expr->hasAttribute(mergeAtom))
        ret.setown(normalizeMergeAggregate(ret));

    if (extraSelectNeeded)
    {
        replaceAssignSelector(extraAssigns, ret);
        IHqlExpression * projectTransform = createValue(no_newtransform, makeTransformType(record->getType()), extraAssigns);
        ret.setown(createDataset(no_newusertable, ret.getClear(), createComma(LINK(record), projectTransform)));
        ret.setown(expr->cloneAllAnnotations(ret));
    }

    return ret.getClear();
}


IHqlExpression * ThorHqlTransformer::normalizeTableGrouping(IHqlExpression * expr)
{
    //Transform table(x,y,z) to table(group(x,z),y)
    IHqlExpression * dataset = expr->queryChild(0);
    LinkedHqlExpr group = queryRealChild(expr, 3);

    if (group)
    {
        if (expr->hasAttribute(mergeAtom))
            return normalizeMergeAggregate(expr);

        bool useHashAggregate = expr->hasAttribute(fewAtom);
        if (expr->getOperator() == no_aggregate)
        {
            OwnedHqlExpr selector = createSelector(no_left, dataset->queryRecord(), querySelSeq(expr));
            group.setown(replaceSelector(group, selector, dataset));
            //Cannot use a hash aggregate if we don't know the mapping from input to output fields...
            if (!isKnownTransform(expr->queryChild(2)))
                useHashAggregate = false;
        }

        if (useHashAggregate && group->isConstant() && !translator.targetThor())
            return removeAttribute(expr, fewAtom);

        if (!expr->hasAttribute(manyAtom) && !expr->hasAttribute(sortedAtom))
        {
            if (isSmallGrouping(group))
            {
                OwnedHqlExpr newsort = simplifySortlistComplexity(group);
                if (!newsort)
                    newsort.set(group);

                LinkedHqlExpr ds = dataset;
                if (isGrouped(ds))
                {
                    ds.setown(createDataset(no_group, ds.getClear(), NULL));
                    ds.setown(cloneInheritedAnnotations(expr, ds));
                }
                OwnedHqlExpr sorted = ensureSortedForGroup(ds, newsort, expr->hasAttribute(localAtom), !translator.targetThor(), options.implicitGroupSubSort);

                //For thor a global grouped aggregate would transfer elements between nodes so it is still likely to
                //be more efficient to do a hash aggregate.  Even better would be to check the distribution
                if ((sorted != ds) ||
                    (translator.targetThor() && !expr->hasAttribute(localAtom) && !isPartitionedForGroup(ds, newsort, true)))
                    useHashAggregate = true;
            }
            //Default to a hash aggregate for child queries/normalized sources
            IHqlExpression * rootDs = queryExpression(dataset->queryDataset()->queryRootTable());
            if (rootDs && rootDs->getOperator() == no_select)
                useHashAggregate = true;
        }

        if (!useHashAggregate && !expr->hasAttribute(groupedAtom))
            return convertAggregateGroupingToGroupedAggregate(expr, group);
    }
    return NULL;
}


void HqlCppTranslator::convertLogicalToActivities(WorkflowItem & curWorkflow)
{
    {
        cycle_t startCycles = get_cycles_now();
        ThorHqlTransformer transformer(*this, targetClusterType, wu());

        HqlExprArray & exprs = curWorkflow.queryExprs();
        HqlExprArray transformed;

        transformer.transformRoot(exprs, transformed);

        replaceArray(exprs, transformed);
        noteFinishedTiming("compile:tree transform: convert logical", startCycles);
    }

    if (queryOptions().normalizeLocations)
        normalizeAnnotations(*this, curWorkflow.queryExprs());
}

//------------------------------------------------------------------------

CompoundSourceInfo::CompoundSourceInfo(IHqlExpression * _original) : NewTransformInfo(_original)
{
    sourceOp = no_none;
    mode = no_none;
    splitCount = 0;
    reset();
}

void CompoundSourceInfo::reset()
{
    forceCompound = false;
    isBoundary = false;
    isPreloaded = false;
    isLimited = false;
    hasChoosen = false;
    hasSkipLimit = false;
    isCloned = false;
    isFiltered = false;
    isPostFiltered = false;
    isCreateRowLimited = false;
    hasOnFail = false;
}


bool CompoundSourceInfo::canMergeLimit(IHqlExpression * expr, ClusterType targetClusterType) const
{
    if (isAggregate() || isChooseNAllLimit(expr->queryChild(1)) || !isBinary())
        return false;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_limit:
        //Can't merge a limit into a choosen() because the limit will be applied first
        if (isLimited || hasChoosen)
            return false;

        //Don't merge skip and onfail limits into activities that can't implement them completely
        if (targetClusterType != RoxieCluster)
        {
            if (expr->hasAttribute(skipAtom) || expr->hasAttribute(onFailAtom))
                return false;
        }
        else
        {
            //Can always limit a count/aggregate with a skip limit - just resets count to 0
            if (expr->hasAttribute(skipAtom))
                return true;
        }
        break;
    case no_choosen:
        if (hasChoosen)
            return false;
        break;
    }

    switch (sourceOp)
    {
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_indexread:
    case no_compound_indexnormalize:
        return true;
    }

    return false;
}


void CompoundSourceInfo::ensureCompound()
{
    if (sourceOp != no_none)
    {
        forceCompound = true;
#if 0
        //MORE: We should really remove the sharing for entries that are going to become compound activities.
        //However, that isn't just for this case - should be iterative
        //e.g. while (spotMoreCompoundActivities())....
        IHqlExpression * search = original;
        loop
        {
            CompoundSourceInfo * extra = queryExtra(search);
            if (extra->sharedCount-- > 1)
                break;
            search = search->queryChild(0);
        }
#endif
    }
}

bool CompoundSourceInfo::inherit(const CompoundSourceInfo & other, node_operator newSourceOp)
{
    isLimited = other.isLimited;
    hasSkipLimit = other.hasSkipLimit;
    hasChoosen = other.hasChoosen;
    isFiltered = other.isFiltered;
    isPostFiltered = other.isPostFiltered;
    isPreloaded = other.isPreloaded;
    isCreateRowLimited = other.isCreateRowLimited;
    hasOnFail = other.hasOnFail;
    mode = other.mode;
    uid.set(other.uid);

    if (other.sourceOp == no_none)
        return false;

    if (newSourceOp == no_none)
    {
        if (other.isCloned)
            return false;
        newSourceOp = other.sourceOp;
    }

    sourceOp = newSourceOp;
    return true;
}

bool CompoundSourceInfo::isAggregate() const
{
    switch (sourceOp)
    {
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
        return true;
    }
    return false;
}


//This doesn't try to restrict creating the compound nodes to the inner level, but will also create them for nested children.
//This shouldn't cause any problems, since compound operators within compounds are ignored, and it means that this transformer
//doesn't have to cope with being scope dependent.
//Calling the transformer again later on child queries should extend the compound activities if appropriate.
static HqlTransformerInfo compoundSourceTransformerInfo("CompoundSourceTransformer");
CompoundSourceTransformer::CompoundSourceTransformer(HqlCppTranslator & _translator, unsigned _flags)
: NewHqlTransformer(compoundSourceTransformerInfo), translator(_translator)
{
    targetClusterType = translator.getTargetClusterType();
    flags = _flags;
    insideCompound = false;
    candidate = false;
}


void CompoundSourceTransformer::analyseGatherInfo(IHqlExpression * expr)
{
    CompoundSourceInfo * extra = queryBodyExtra(expr);
    node_operator op = expr->getOperator();
    bool wasInsideCompound = insideCompound;
    if (!insideCompound)
        extra->noteUsage();

    if (!expr->isDataset())
        insideCompound = false;

    switch (op)
    {
    case no_fetch:
        {
            unsigned max = expr->numChildren();
            for (unsigned i =1; i < max; i++)
                analyseExpr(expr->queryChild(i));
            break;
        }
    case no_keyed:
    case no_record:
    case no_attr:
    case no_attr_expr:
        break;
    case no_keyedlimit:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_preload:
        insideCompound = true;
        NewHqlTransformer::analyseExpr(expr);
        break;
    case no_filter:
        if (filterIsKeyed(expr))
            insideCompound = true;
        NewHqlTransformer::analyseExpr(expr);
        break;
    case no_hqlproject:
    case no_newusertable:
    case no_aggregate:
    case no_newaggregate:
        if (expr->hasAttribute(keyedAtom))
            insideCompound = true;
        NewHqlTransformer::analyseExpr(expr);
        break;
    case no_join:
        if (isKeyedJoin(expr) && !expr->hasAttribute(_complexKeyed_Atom))
        {
            analyseExpr(expr->queryChild(0));
            doAnalyseChildren(expr, 2);
        }
        else
            NewHqlTransformer::analyseExpr(expr);
        break;
    default:
        NewHqlTransformer::analyseExpr(expr);
        break;
    }

    switch (op)
    {
    case no_newkeyindex:
        extra->sourceOp = no_compound_indexread;
        extra->uid.set(expr->queryAttribute(_uid_Atom));
        extra->mode = no_thor;
        break;
    case no_table:
        {
            IHqlExpression * mode = expr->queryChild(2);
            if (!mode)
                break;
            switch (mode->getOperator())
            {
            case no_thor:
            case no_flat:
                if ((flags & CSFcompoundSpill) || !expr->hasAttribute(_spill_Atom))
                {
                    extra->sourceOp = no_compound_diskread;
                    extra->isPreloaded = expr->hasAttribute(preloadAtom);
                    extra->uid.set(expr->queryAttribute(_uid_Atom));
                    extra->mode = no_thor;
                }
                break;
            case no_csv:
                if (translator.queryOptions().enableCompoundCsvRead)
                {
                    extra->sourceOp = no_compound_diskread;
                    extra->isPreloaded = expr->hasAttribute(preloadAtom);
                    extra->uid.set(expr->queryAttribute(_uid_Atom));
                    extra->mode = mode->getOperator();
                }
                break;
            }
            break;
        }
    case no_hqlproject:
        {
            if (!expr->hasAttribute(prefetchAtom))
            {
                IHqlExpression * transform = expr->queryChild(1);
                IHqlExpression * counter = queryAttributeChild(expr, _countProject_Atom, 0);
                if (!counter || !transformContainsCounter(transform, counter))
                {
                    IHqlExpression * dataset = expr->queryChild(0);
                    CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);
                    //Skips in datasets don't work very well at the moment - pure() is a bit strict really.
                    if ((dataset->isPure() || expr->hasAttribute(keyedAtom)) && !parentExtra->isAggregate())
                    {
                        extra->inherit(*parentExtra);
                        if (expr->hasAttribute(keyedAtom))
                            extra->ensureCompound();
                        if (!isPureActivity(expr))
                        {
                            extra->isFiltered = true;
                            extra->isPostFiltered = true;
                        }
                    }
                }
            }
            break;
        }
    case no_keyedlimit:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);
            if (!parentExtra->isAggregate() && parentExtra->isBinary())
            {
                extra->inherit(*parentExtra);
                extra->ensureCompound();
                if (expr->hasAttribute(onFailAtom))
                    extra->hasOnFail = true;
            }
            break;
        }
    case no_inlinetable:
    case no_temptable:
    case no_datasetfromrow:
        extra->sourceOp = no_compound_inline;
        extra->mode = no_inlinetable;
        break;
    case no_workunit_dataset:
//      extra->sourceOp = no_compound_childread;
        break;
    case no_getgraphresult:
    case no_externalcall:
//      if (expr->isDataset())
//          extra->sourceOp = no_compound_childread;
        break;
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);
            extra->inherit(*parentExtra, op);
            extra->uid.set(expr->queryAttribute(_uid_Atom));
            break;
        }
    case no_select:
        if (expr->isDataset())
        {
            if (expr->hasAttribute(newAtom))
            {
                IHqlExpression * dataset = expr->queryChild(0);
                CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);
                if (!parentExtra->isAggregate() && !parentExtra->hasAnyLimit() && parentExtra->isBinary())
                {
                    node_operator newOp = no_none;
                    switch (parentExtra->sourceOp)
                    {
                    case no_compound_diskread:
                    case no_compound_disknormalize:
                        if (flags & CSFnewdisk)
                            newOp = no_compound_disknormalize;
                        break;
                    case no_compound_indexread:
                    case no_compound_indexnormalize:
                        if (flags & CSFnewindex)
                            newOp = no_compound_indexnormalize;
                        break;
                    case no_compound_childread:
                    case no_compound_childnormalize:
                        if (flags & CSFnewchild)
                            newOp = no_compound_childnormalize;
                        break;
                    }
                    if (newOp)
                    {
                        if (extra->inherit(*parentExtra))
                        {
                            extra->sourceOp = newOp;
                            extra->ensureCompound();
                        }
                    }
                }

                if ((flags & CSFnewchild) && (extra->sourceOp == no_none))
                {
                    extra->reset();
                    extra->sourceOp = no_compound_selectnew;
                    extra->ensureCompound();
                }
            }
            else
            {
                if ((flags & CSFnewchild) && !isTargetSelector(expr))           // latter is optimization - still works without this
                {
                    extra->sourceOp = no_compound_childread;
                }
            }
        }
        break;
    case no_choosen:
        {
            IHqlExpression * arg2 = expr->queryChild(2);
            if (arg2 && !arg2->isPure())
                break;
            //fall through
        }
    case no_limit:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);

            bool cloneRequired = needToCloneLimit(expr, parentExtra->sourceOp);
            if (cloneRequired && !expr->queryChild(1)->isPure())
                break;

            if (parentExtra->canMergeLimit(expr, targetClusterType) && !isGrouped(expr) && parentExtra->isBinary())
            {
                if (extra->inherit(*parentExtra))
                {
                    if (op == no_choosen)
                    {
                        extra->hasChoosen = true;
                    }
                    else
                    {
                        extra->isLimited = true;
                        if (expr->hasAttribute(skipAtom))
                            extra->hasSkipLimit = true;
                    }
                    if (expr->hasAttribute(onFailAtom))
                        extra->isCreateRowLimited = true;
                    extra->isCloned = cloneRequired;
                }
            }
            break;
        }
    case no_aggregate:
    case no_newusertable:
    case no_newaggregate:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);

            if (isAggregateDataset(expr))
            {
                //Don't yet have csv/xml variants!
                if (!parentExtra->isBinary())
                    break;

                //ONFAIL isn't supported for compound aggregates at the moment - although it could be....
                if (parentExtra->hasOnFail)
                    break;

                IHqlExpression * root = queryRoot(dataset);
                if (!root || isGrouped(root) || expr->hasAttribute(localAtom))
                    break;

                bool isSimpleCountExists = isSimpleCountExistsAggregate(expr, true, false);
                if (parentExtra->isCreateRowLimited)
                    break;
                if (parentExtra->hasAnyLimit() && !isSimpleCountExists)
                    break;

                //We either have a limit or choosen as the input
                if (parentExtra->isCloned)
                {
                    assertex(isSimpleCountExists);
                    //Too complicated if the limit is local
                    if ((targetClusterType == ThorLCRCluster) && dataset->hasAttribute(localAtom))
                        break;

                    //CHOOSEN(ds, x, <n>)
                    if ((dataset->getOperator() == no_choosen) && queryRealChild(dataset, 2))
                        break;
                }


                node_operator newOp = no_none;
                node_operator parentOp = parentExtra->sourceOp;
                if (queryRealChild(expr, 3))
                {
                    //Grouped aggregate
                    switch (parentOp)
                    {
                    case no_compound_diskread:
                    case no_compound_disknormalize:
                        if (flags & CSFnewdisk)
                            newOp = no_compound_diskgroupaggregate;
                        break;
                    case no_compound_indexread:
                    case no_compound_indexnormalize:
                        if (flags & CSFnewindex)
                            newOp = no_compound_indexgroupaggregate;
                        break;
                    case no_compound_childread:
                    case no_compound_childnormalize:
                        if (flags & CSFnewchild)
                            newOp = no_compound_childgroupaggregate;
                        break;
                    }
                }
                else
                {
                    switch (parentOp)
                    {
                    case no_compound_diskread:
                    case no_compound_disknormalize:
                        if (flags & CSFnewdisk)
                        {
                            newOp = no_compound_diskaggregate;
                            if (isSimpleCountExists && !parentExtra->isFiltered && (parentOp == no_compound_diskread))
                            {
                                IHqlExpression * root = queryRoot(expr);
                                if (root)
                                {
                                    ColumnToOffsetMap * map = translator.queryRecordOffsetMap(root->queryRecord());
                                    if (map->isFixedWidth())
                                        extra->forceCompound = true;
                                }
                            }
                        }
                        break;
                    case no_compound_indexread:
                    case no_compound_indexnormalize:
                        //Don't create counts for (non-keyed) skip limits - little benefit, and could cause problems
                        //correctly returning the counts - e.g. especially for exists()
                        if ((flags & CSFnewindex) && !parentExtra->hasSkipLimit)
                        {
                            newOp = no_compound_indexaggregate;
                            //Force counts on indexes to become a new compound activity
                            //otherwise if(count(x) > n, f(x), g()) will always cause x to be read and spilt.
                            //The commented out test would do a better job, but not all keyed filters have an explicit keyed() so it is insufficient
                            //
                            //Really this should become a count if there are no index reads with the same level of conditionality, or if all accesses
                            //are counts.
                            //That can be logged as a future enhancement.....
//                          if (isSimpleCountExists && !parentExtra->isPostFiltered && (parentOp == no_compound_indexread))
                            if (isSimpleCountExists && (parentOp == no_compound_indexread))
                            {
                                //A skip limit will require everything to be read anyway - so no point splitting in two
                                if (!parentExtra->hasSkipLimit)
                                    extra->forceCompound = true;
                            }
                        }
                        break;
                    case no_compound_childread:
                    case no_compound_childnormalize:
                        if (flags & CSFnewchild)
                            newOp = no_compound_childaggregate;
                        break;
                    case no_compound_inline:
                        if (flags & CSFnewinline)
                            newOp = no_compound_inline;
                        break;
                    }
                }
                if (newOp)
                {
                    //NB: When creating a limited aggregate, it is ok if the input indicates it is cloned
                    //because the new compound count operation will take it into account.
                    extra->inherit(*parentExtra, newOp);
                }
            }
            else
            {
                if (!parentExtra->isAggregate())
                    extra->inherit(*queryBodyExtra(dataset));
            }
            if (expr->hasAttribute(keyedAtom))
                extra->ensureCompound();
        }
        break;
    case no_filter:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            CompoundSourceInfo * parentExtra = queryBodyExtra(dataset);
            if (!parentExtra->hasAnyLimit() && !parentExtra->isAggregate())
            {
                if (extra->inherit(*parentExtra))
                {
                    extra->isFiltered = true;
                    if (filterIsKeyed(expr))
                        extra->ensureCompound();
                    if (filterIsUnkeyed(expr))
                        extra->isPostFiltered = true;
                }
            }
        }
        break;
    case no_preload:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            extra->inherit(*queryBodyExtra(dataset));
            extra->isPreloaded = true;
            break;
        }
    case no_sorted:
    case no_preservemeta:
    case no_distributed:
    case no_grouped:
    case no_stepped:
    case no_section:
    case no_sectioninput:
    case no_dataset_alias:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            extra->inherit(*queryBodyExtra(dataset));
            break;
        }
    case no_usertable:
    case no_selectfields:
        UNIMPLEMENTED;
        break;
    case no_addfiles:
        if (canProcessInline(NULL, expr) && (flags & CSFnewinline))
            extra->sourceOp = no_compound_inline;
        break;
    }
    insideCompound = wasInsideCompound;
}


void CompoundSourceTransformer::analyseMarkBoundaries(IHqlExpression * expr)
{
    //This code means that child-query compounds inside a compound aren't yet spotted, they are spotted later.
    if (createCompoundSource(expr))
    {
        queryBodyExtra(expr)->isBoundary = true;
        candidate = true;
        return;
    }
    else if (isCompoundSource(expr))
        return;

    //Might cause problems if Some items are references (e.g., keyed, fetch(0), keyedjoin(1) and don't want translating.
    NewHqlTransformer::analyseExpr(expr);
}

void CompoundSourceTransformer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr->queryBody()))
    {
        if ((pass == 0) && !insideCompound)
        {
            if (!queryBodyExtra(expr)->isNoteUsageFirst())
                return;
        }
        else
            return;
    }

    if (expr->isConstant())
        return;

    switch (pass)
    {
    case 0:
        analyseGatherInfo(expr);
        break;
    case 1:
        analyseMarkBoundaries(expr);
        break;
    default:
        throwUnexpected();
        break;
    }
}

bool CompoundSourceTransformer::childrenAreShared(IHqlExpression * expr)
{
    if (isCompoundSource(expr))
        return false;
    unsigned numChildren = getNumChildTables(expr);
    for (unsigned i=0; i < numChildren; i++)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (queryBodyExtra(cur)->isShared() || childrenAreShared(cur))
            return true;
    }
    return false;
}


bool CompoundSourceTransformer::createCompoundSource(IHqlExpression * expr)
{
    CompoundSourceInfo * extra = queryBodyExtra(expr);
    if (extra->sourceOp == no_none)
        return false;
    if (extra->forceCompound)
        return true;
    if (isSourceActivity(expr))
        return false;
    if (expr->getOperator() == no_preservemeta)
        return false;
    if (extra->isPreloaded)
        return (flags & CSFpreload) != 0;
    switch (extra->sourceOp)
    {
    case no_compound_diskread:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
        return ((flags & CSFignoreShared) || !childrenAreShared(expr));
    case no_compound_disknormalize:
        return true;
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_indexread:
        //MORE: Should stop at sufficiently shared children - e.g.,
        //* if the children are aggregates, when we get that far.
        //* if child actions don't change the filter significantly (e.g, just projects, or no seg monitors)
        {
            if (!(flags & CSFindex))
                return false;
            CompoundSourceInfo * parentExtra = queryBodyExtra(expr->queryChild(0));
            return ((flags & CSFignoreShared) || !childrenAreShared(expr) || !parentExtra->isFiltered);
        }
    case no_compound_indexnormalize:
        return ((flags & CSFindex) != 0);
    case no_compound_inline:
        if (!(flags & CSFnewinline))
            return false;
        return !childrenAreShared(expr);
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
        return true;
    }
    UNIMPLEMENTED;
    return false;
}

IHqlExpression * CompoundSourceTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
        return LINK(expr);

    OwnedHqlExpr ret = queryTransformAnnotation(expr);
    if (ret)
        return ret.getClear();

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    CompoundSourceInfo * extra = queryBodyExtra(expr);
    if (extra->isBoundary)
    {
        HqlExprAttr def = transformed;
        if (extra->isCloned)
            transformed.setown(appendLocalAttribute(transformed));
        transformed.setown(createDataset(extra->sourceOp, LINK(transformed), LINK(extra->uid)));
        if (extra->isCloned)
        {
            HqlExprArray args;
            unwindChildren(args, def);
            args.replace(*transformed.getClear(), 0);
            transformed.setown(def->clone(args));
        }
    }

    return transformed.getClear();
}


ANewTransformInfo * CompoundSourceTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(CompoundSourceInfo, expr);
}


bool CompoundSourceTransformer::needToCloneLimit(IHqlExpression * expr, node_operator sourceOp)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_choosen:
        if (queryRealChild(expr, 2))
            return true;
        break;
    case no_limit:
        if (expr->hasAttribute(skipAtom) && (targetClusterType != RoxieCluster))
            return true;
        break;
    }

    switch (targetClusterType)
    {
    case RoxieCluster:
        return false;
    case HThorCluster:
        return (sourceOp != no_compound_indexread) || (op != no_limit);
    case ThorLCRCluster:
        return true;
    default:
        UNIMPLEMENTED;
    }
}


IHqlExpression * CompoundSourceTransformer::process(IHqlExpression * expr)
{
    analyse(expr, 0);
    analyse(expr, 1);
    if (candidate)
        return transformRoot(expr);
    return LINK(expr);
}

//---------------------------------------------------------------------------

IHqlExpression * getMergedFetch(IHqlExpression * expr)
{
    IHqlExpression * child = expr->queryChild(0);
    if (isLimitedDataset(child))
        return LINK(expr);

    HqlExprArray args;
    if (child->getOperator() == no_compound_fetch)
        return swapDatasets(expr);
    if (child->getOperator() != no_fetch)
        return LINK(expr);
    args.append(*LINK(expr));
    return createDataset(no_compound_fetch, args);
}

static HqlTransformerInfo compoundActivityTransformerInfo("CompoundActivityTransformer");
CompoundActivityTransformer::CompoundActivityTransformer(ClusterType _targetClusterType) : NewHqlTransformer(compoundActivityTransformerInfo)
{
    targetClusterType = _targetClusterType;
}

IHqlExpression * CompoundActivityTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);

    switch (transformed->getOperator())
    {
    case no_filter:
        return getMergedFetch(transformed);
    case no_limit:
        {
            if (transformed->hasAttribute(onFailAtom))
                break;

            LinkedHqlExpr dataset = transformed->queryChild(0);
            if (dataset->hasAttribute(limitAtom) || transformed->hasAttribute(skipAtom))
                break;

            // A limited KEYED-JOIN should never read more than limit rows from the index for each left row
            // so add a LIMIT attribute onto the keyed join to ensure it doesn't return too many records.
            switch (dataset->getOperator())
            {
            case no_join:
                if (isKeyedJoin(dataset))
                    break;
                return transformed.getClear();
            default:
                return transformed.getClear();
            }

            HqlExprArray args;
            unwindChildren(args, transformed);
            args.replace(*mergeLimitIntoDataset(dataset, transformed), 0);
            return transformed->clone(args);
        }
    }

    return transformed.getClear();
}


//------------------------------------------------------------------------

static HqlTransformerInfo optimizeActivityTransformerInfo("OptimizeActivityTransformer");
OptimizeActivityTransformer::OptimizeActivityTransformer(bool _optimizeCountCompare, bool _optimizeNonEmpty)
: NewHqlTransformer(optimizeActivityTransformerInfo)
{
    optimizeCountCompare = _optimizeCountCompare; optimizeNonEmpty = _optimizeNonEmpty;
}

void OptimizeActivityTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();
    queryBodyExtra(expr)->noteUsed();
    if (alreadyVisited(expr))
        return;
    NewHqlTransformer::analyseExpr(expr);
}

//either a simple count, or isCountAggregate is guaranteed to be true - so structure is well defined
IHqlExpression * OptimizeActivityTransformer::insertChoosen(IHqlExpression * lhs, IHqlExpression * limit, __int64 limitDelta)
{
    if (isShared(lhs))
        return NULL;

    IHqlExpression * ds = lhs->queryChild(0);
    HqlExprArray args;
    switch (lhs->getOperator())
    {
    case no_choosen:
        return NULL;
    case no_count:
    case no_newaggregate:
        {
            //count on a child dataset is better if not limited...
            node_operator dsOp = ds->getOperator();
            if ((dsOp == no_select) || (dsOp == no_choosen) || (dsOp == no_rows))
                return NULL;
            args.append(*createDataset(no_choosen, LINK(ds), adjustValue(limit, limitDelta)));
            break;
        }
    case no_implicitcast:
    case no_cast:
    case no_compound_childaggregate:
    case no_compound_diskaggregate:
    case no_compound_indexaggregate:
    case no_select:
        {
            IHqlExpression * newDs = insertChoosen(ds, limit, limitDelta);
            if (!newDs)
                return NULL;
            args.append(*newDs);
            break;
        }
    default:
        throwUnexpectedOp(lhs->getOperator());
    }
    unwindChildren(args, lhs, 1);
    return lhs->clone(args);
}


static bool looksLikeSimpleCount(IHqlExpression * expr)
{
    if ((expr->getOperator() == no_select) && expr->hasAttribute(newAtom))
    {
        IHqlExpression * ds = expr->queryChild(0);
        return isSimpleCountAggregate(ds, false);
    }
    return (expr->getOperator() == no_count);
}


IHqlExpression * OptimizeActivityTransformer::optimizeCompare(IHqlExpression * lhs, IHqlExpression * rhs, node_operator op)
{
    if (isShared(lhs))
        return NULL;
    if (!isIndependentOfScope(rhs))
        return NULL;

    if (!looksLikeSimpleCount(lhs))
        return NULL;

    // count(x) op count(y) - not clear if a choosen should be added to either, so assume neither for the moment,
    // (we definitely don't want it added to both, which happens without the second test.)
    if (looksLikeSimpleCount(rhs))
        return NULL;

    //Convert count(x) >= 1 to exists(x)  (and other varients)
    node_operator existOp = no_none;
    switch (op)
    {
    case no_ne:
    case no_gt:
        if (matchesConstantValue(rhs, 0))
            existOp = no_exists;
        break;
    case no_eq:
    case no_le:
        if (matchesConstantValue(rhs, 0))
            existOp = no_not;
        break;
    case no_lt:
        if (matchesConstantValue(rhs, 1))
            existOp = no_not;
        break;
    case no_ge:
        if (matchesConstantValue(rhs, 1))
            existOp = no_exists;
        break;
    }

    if (existOp != no_none)
    {
        if (lhs->getOperator() == no_count)
        {
            IHqlExpression * ds = lhs->queryChild(0);
            OwnedHqlExpr ret = createValue(no_exists, makeBoolType(), LINK(ds));
            if (existOp == no_not)
                return createValue(no_not, makeBoolType(), ret.getClear());
            return ret.getClear();
        }
    }

    unsigned choosenDelta =0;
    switch (op)
    {
    case no_eq:
        //count(x) == n -> count(choosen(x,n+1)) == n
        choosenDelta = 1;
        break;
    case no_ne:
        //count(x) != 0 -> count(choosen(x,n+1)) != n
        choosenDelta = 1;
        break;
    case no_lt:
        //count(x) < n -> count(choosen(x,n)) < n
        break;
    case no_le:
        //count(x) <= n -> count(choosen(x,n+1)) <= n
        choosenDelta = 1;
        break;
    case no_gt:
        //count(x) > n -> count(choosen(x,n+1)) > n
        choosenDelta = 1;
        break;
    case no_ge:
        //count(x) >= n -> count(choosen(x,n)) >= n
        break;
    }

    IHqlExpression * newLhs = insertChoosen(lhs, rhs, choosenDelta);
    if (!newLhs)
        return NULL;
    return createValue(op, makeBoolType(), newLhs, LINK(rhs));
}


static IHqlExpression * queryNormalizedAggregateParameter(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_choosen:
            if (queryRealChild(expr, 2))
                return expr;
            break;
        case no_sort:
        case no_subsort:
        case no_distribute:
            break;
        default:
            return expr;
        }
        expr = expr->queryChild(0);
    }
}

static bool aggregateMatchesDataset(IHqlExpression * agg, IHqlExpression * ds)
{
    return queryNormalizedAggregateParameter(agg)->queryBody() == queryNormalizedAggregateParameter(ds)->queryBody();
}

static bool isCheckExistsAtleast(IHqlExpression * cond, IHqlExpression * ds, __int64 minMinElements, __int64 maxMinElements)
{
    if (maxMinElements <= 0)
        return false;
    switch (cond->getOperator())
    {
    case no_exists:
        if (aggregateMatchesDataset(cond->queryChild(0), ds))
            return true;
        break;
    case no_ne:
        {
            IHqlExpression * condLhs = cond->queryChild(0);
            if ((condLhs->getOperator() == no_count) && isZero(cond->queryChild(1)) && (minMinElements == 1))
            {
                if (aggregateMatchesDataset(condLhs->queryChild(0), ds))
                    return true;
            }
            break;
        }
    case no_gt:
        minMinElements--;
        maxMinElements--;
        //fallthrough
    case no_ge:
        {
            IHqlExpression * condLhs = cond->queryChild(0);
            if (condLhs->getOperator() == no_count)
            {
                IHqlExpression * limit = cond->queryChild(1);
                if (limit->queryValue())
                {
                    __int64 limitVal = limit->queryValue()->getIntValue();
                    if ((limitVal <= maxMinElements) && (limitVal >= minMinElements))
                    {
                        if (aggregateMatchesDataset(condLhs->queryChild(0), ds))
                            return true;
                    }
                }
            }
        }
        break;
    }
    return false;
}

//is "value" of the form ds[n].x and other the same as the null expression for that field?
//if so we may be able to remove a condition
IHqlExpression * queryNullDsSelect(__int64 & selectIndex, IHqlExpression * value, IHqlExpression * other)
{
    if (isCast(value))
        value = value->queryChild(0);
    if (value->getOperator() != no_select)
        return NULL;
    bool isNew;
    IHqlExpression * ds = querySelectorDataset(value, isNew);
    if (!isNew || ds->getOperator() != no_selectnth)
        return NULL;
    IValue * index = ds->queryChild(1)->queryValue();
    if (!index)
        return NULL;
    if (!isNullExpr(other, value))
        return NULL;
    selectIndex = index->getIntValue();
    return ds->queryChild(0);
}


IHqlExpression * OptimizeActivityTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = doCreateTransformed(expr);
    if (transformed)
    {
        assertex(transformed != expr);
        queryBodyExtra(transformed)->inherit(queryBodyExtra(expr));
        return transform(transformed);
    }
    transformed.setown(NewHqlTransformer::createTransformed(expr));
    if (transformed != expr)
        queryBodyExtra(transformed)->inherit(queryBodyExtra(expr));
    return transformed.getClear();
}

IHqlExpression * OptimizeActivityTransformer::doCreateTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_if:
        {
            IHqlExpression * cond = expr->queryChild(0);
            IHqlExpression * lhs = expr->queryChild(1);
            //convert if(exists(x)|count(x)>0, x, y) to nonempty(x, y);
            //must happen before the count(x)>n optimization below....
            if (expr->isDataset())
            {
                if (isCheckExistsAtleast(cond, lhs, 1, 1))
                {
                    IHqlExpression * rhs = expr->queryChild(2);

                    //always convert if(exists(x),x) to x regardless of x, or the optimizeNonEmpty option
                    if (rhs->getOperator() == no_null)
                        return transform(lhs);

                    if (optimizeNonEmpty && !canProcessInline(NULL, expr))
                    {
                        HqlExprArray args;
                        args.append(*transform(lhs));
                        args.append(*transform(rhs));
                        OwnedHqlExpr ret = createDataset(no_nonempty, args);
                        return expr->cloneAllAnnotations(ret);
                    }
                }
            }

            __int64 selectIndex = 0;
            //check for if (count(x) >= 10, x[10].value, <null>) and convert to x[10].value
            //also valid for count(x) >= 1, but not count(x) >= 11
            IHqlExpression * ds = queryNullDsSelect(selectIndex, expr->queryChild(1), expr->queryChild(2));
            if (ds)
            {
                if (isCheckExistsAtleast(cond, ds, 1, selectIndex))
                    return LINK(lhs);
            }
            break;
        }

    case no_selectnth:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if ((ds->getOperator() != no_sort) || isShared(ds))
                break;
            IHqlExpression * index = expr->queryChild(1);
            if (getIntValue(index, 99999) > 100)
                break;
            OwnedHqlExpr transformedDs = transform(ds);
            OwnedHqlExpr transformedIndex = transform(index);
            HqlExprArray args;
            unwindChildren(args, transformedDs);
            args.add(*LINK(transformedIndex), 2);
            OwnedHqlExpr topn = createDataset(no_topn, args);
            args.kill();
            args.append(*ds->cloneAllAnnotations(topn));
            args.append(*LINK(transformedIndex));
            unwindChildren(args, expr, 2);
            return expr->clone(args);
        }

    case no_eq:
    case no_ne:
    case no_le:
    case no_lt:
    case no_ge:
    case no_gt:
        //MORE Would still be worth doing for thor i) if a no_select non-new, ii) if the lhs was an aggregate on
        //a compound_disk_aggregate iii) possibly others.
        if (optimizeCountCompare)
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * rhs = expr->queryChild(1);
            OwnedHqlExpr ret = optimizeCompare(lhs, rhs, op);
            if (!ret)
                ret.setown(optimizeCompare(rhs, lhs, getReverseOp(op)));
            if (ret)
                return ret.getClear();
        }
        break;
    }

    return NULL;
}


void optimizeActivities(HqlExprArray & exprs, bool optimizeCountCompare, bool optimizeNonEmpty)
{
    OptimizeActivityTransformer transformer(optimizeCountCompare, optimizeNonEmpty);
    HqlExprArray results;
    transformer.analyseArray(exprs, 0);
    transformer.transformRoot(exprs, results);
    replaceArray(exprs, results);
}

IHqlExpression * optimizeActivities(IHqlExpression * expr, bool optimizeCountCompare, bool optimizeNonEmpty)
{
    OptimizeActivityTransformer transformer(optimizeCountCompare, optimizeNonEmpty);
    HqlExprArray results;
    transformer.analyse(expr, 0);
    return transformer.transformRoot(expr);
}

IHqlExpression * GlobalAttributeInfo::queryAlias(IHqlExpression * value)
{
    if (!aliasName)
    {
        if (storedName)
            aliasName.set(storedName);
        else
            aliasName.setown(createNextStringValue(value, storedPrefix));
    }
    return aliasName;
}

IHqlExpression * GlobalAttributeInfo::queryFilename(IHqlExpression * value, IConstWorkUnit * wu, bool isRoxie)
{
    if (!cachedFilename)
    {
        if (storedName)
            cachedFilename.set(storedName);
        else
            cachedFilename.setown(createNextStringValue(value, filePrefix));
        if (persistOp != no_persist)
        {
            StringBuffer prefix("~");
            if (storedName)
            {
                if (persistOp == no_stored)
                    prefix.append("jobtemp::stored");
                else if (persistOp == no_checkpoint)
                    prefix.append("jobtemp::checkpoint");
            }
            if (persistOp == no_once)
                prefix.append("once::");
            bool wuidIsConstant = isRoxie || !wu->getCloneable();
            if (wuidIsConstant)
            {
                StringBuffer s;
                cachedFilename->queryValue()->getStringValue(s.append(prefix));
                cachedFilename.setown(createConstant(s.str()));
            }
            else
            {
                ITypeInfo * type = makeStringType(UNKNOWN_LENGTH, NULL, NULL);

                OwnedHqlExpr filename = createValue(no_concat, type, createConstant(prefix), cachedFilename.getClear());
                cachedFilename.setown(foldHqlExpression(filename));
            }
        }
    }
    return cachedFilename;
}


IHqlExpression * GlobalAttributeInfo::createSetValue(IHqlExpression * value, IHqlExpression * name)
{
    HqlExprArray args;
    args.append(*LINK(value));
    args.append(*createAttribute(sequenceAtom, LINK(sequence)));
    args.append(*createAttribute(namedAtom, LINK(name)));
    if (extraSetAttr)
        extraSetAttr->unwindList(args, no_comma);
    if (cluster)
        args.append(*createAttribute(clusterAtom, LINK(cluster)));
    if (fieldFormat)
        args.append(*LINK(fieldFormat));
    if (setOp == no_setresult)
        return createSetResult(args);
    return createValue(setOp, makeVoidType(), args);
}

IHqlExpression * GlobalAttributeInfo::getStoredKey()
{
    return createAttribute(nameAtom, LINK(sequence), lowerCaseHqlExpr(originalLabel));
}

void GlobalAttributeInfo::setCluster(IHqlExpression * expr)
{
    if (expr && !isBlankString(expr))
        cluster.set(expr);
}

void GlobalAttributeInfo::extractGlobal(IHqlExpression * global, ClusterType platform)
{
    few = spillToWorkunitNotFile(value, platform) || value->isDictionary();
    if (global)
    {
        if (global->hasAttribute(fewAtom))
            few = true;
        else if (global->hasAttribute(manyAtom) && (platform != RoxieCluster))
            few = false;
    }
    setOp = no_setresult;
    sequence.setown(getLocalSequenceNumber());
    persistOp = no_global;
}

void GlobalAttributeInfo::extractStoredInfo(IHqlExpression * expr, IHqlExpression * _codehash, bool isRoxie, int multiplePersistInstances)
{
    node_operator op = expr->getOperator();
    few = expr->hasAttribute(fewAtom) || (isRoxie) || (value->isDictionary() && !expr->hasAttribute(manyAtom));
    switch (op)
    {
    case no_stored:
        setOp = no_ensureresult;
        storedName.set(expr->queryChild(0));
        originalLabel.set(storedName);
        sequence.setown(getStoredSequenceNumber());
        fieldFormat.set(expr->queryAttribute(storedFieldFormatAtom));
        few = true;
        break;
    case no_checkpoint:
        setOp = no_ensureresult;
        storedName.set(expr->queryChild(0));
        originalLabel.set(storedName);
        sequence.setown(getLocalSequenceNumber());
        extraSetAttr.setown(createAttribute(checkpointAtom));
        break;
    case no_persist:
        assertex(_codehash);
        codehash.set(_codehash);
        setOp = no_ensureresult;
        storedName.set(expr->queryChild(0));
        originalLabel.set(storedName);
        sequence.setown(getGlobalSequenceNumber());
        extraSetAttr.setown(createAttribute(_workflowPersist_Atom, LINK(codehash)));
        setCluster(queryRealChild(expr, 1));
        few = expr->hasAttribute(fewAtom);       // PERSISTs need a consistent format.
        extraOutputAttr.setown(createComma(LINK(expr->queryAttribute(expireAtom)), LINK(expr->queryAttribute(clusterAtom))));
        numPersistInstances = multiplePersistInstances;
        if (expr->hasAttribute(multipleAtom))
            numPersistInstances = (int)getIntValue(queryAttributeChild(expr, multipleAtom, 0), -1);
        else if (expr->hasAttribute(singleAtom))
            numPersistInstances = 0;

        if (numPersistInstances != 0)
        {
            StringBuffer s;
            getStringValue(s, storedName);
            s.append("__p");
            getStringValue(s, codehash);
            storedName.setown(createConstant(s.str()));
        }
        break;
    case no_global:
        throwUnexpected();
    case no_independent:
        setOp = no_setresult;
        storedName.clear();
        sequence.setown(getLocalSequenceNumber());
        extraSetAttr.setown(createAttribute(_workflow_Atom));
        setCluster(queryRealChild(expr, 0));
        op = no_global;
        break;
    case no_once:
        setOp = no_setresult;
        storedName.clear();
        sequence.setown(getOnceSequenceNumber());
        extraSetAttr.setown(createAttribute(_workflow_Atom));
        break;
    case no_success:
    case no_failure:
    case no_recovery:
        if(setOp == no_none)
        {
            storedName.clear();
            setOp = no_setresult;
            sequence.setown(getLocalSequenceNumber());
        }
        break;
    default:
        return;
    }
    persistOp = op;
}


void GlobalAttributeInfo::splitGlobalDefinition(ITypeInfo * type, IHqlExpression * value, IConstWorkUnit * wu, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput, bool isRoxie)
{
    doSplitGlobalDefinition(type, value, wu, setOutput, getOutput, isRoxie);
}

void GlobalAttributeInfo::doSplitGlobalDefinition(ITypeInfo * type, IHqlExpression * value, IConstWorkUnit * wu, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput, bool isRoxie)
{
    OwnedHqlExpr targetName;
    if (storedName)
        targetName.set(storedName);
    else
        targetName.setown(createNextStringValue(value));

    ITypeInfo * valueType = value->queryType();
    if (value->isDataset() || value->isDictionary())
    {
        if (few)
        {
            splitSmallDataset(value, setOutput, getOutput);
            return;
        }
        LinkedHqlExpr filename = queryFilename(value, wu, isRoxie);

        HqlExprArray args;
        if (value->isDictionary())
            args.append(*createDataset(no_datasetfromdictionary, LINK(value)));
        else
            args.append(*LINK(value));
        args.append(*LINK(filename));

        //NB: Also update the dataset node at the end...
        if (valueType->getTypeCode() == type_groupedtable)
            args.append(*createAttribute(groupedAtom));
        else
            assertex(!isGrouped(valueType));

        bool compressFile = true;
        switch (persistOp)
        {
        case no_persist:
            {
                args.append(*createAttribute(_workflowPersist_Atom));
                args.append(*createAttribute(sequenceAtom, getGlobalSequenceNumber()));
                //add a flag to help get the resourcing right - may need to hash distribute on different size thor
                IHqlExpression * distribution = queryDistribution(value);
                if (distribution && !distribution->isAttribute())
                    args.append(*createAttribute(distributedAtom));
                break;
            }
        case no_stored:
            args.append(*createAttribute(ownedAtom));
            args.append(*createAttribute(sequenceAtom, getStoredSequenceNumber()));
            break;
        case no_checkpoint:
            args.append(*createAttribute(ownedAtom));
            args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
            break;
        case no_once:
            args.append(*createAttribute(ownedAtom));
            args.append(*createAttribute(sequenceAtom, getOnceSequenceNumber()));
            break;
        case no_global:
            //May extend over several different graphs
            args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
            args.append(*createAttribute(ownedAtom));
            args.append(*createAttribute(jobTempAtom));
            break;
        default:
            //global, independent, success, failure, etc. etc.
            args.append(*createAttribute(ownedAtom));
            args.append(*createAttribute(jobTempAtom));
            args.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
            break;
        }
        if (compressFile)
            args.append(*createAttribute(__compressed__Atom));
        args.append(*createAttribute(overwriteAtom));
        if (extraOutputAttr)
            extraOutputAttr->unwindList(args, no_comma);

        OwnedHqlExpr output = createValue(no_output, makeVoidType(), args);
//      if (persistOp == no_independent)
        if (setOp == no_setresult)
            setOutput.set(output);
        else
            setOutput.setown(createSetValue(output, queryAlias(value)));

        if(getOutput)
        {
            IHqlExpression * record = value->queryRecord();

            args.kill();
            args.append(*LINK(filename));
            args.append(*LINK(record));
            args.append(*createValue(no_thor));
            args.append(*createAttribute(_noVirtual_Atom));         // don't interpret virtual fields in spilled output

            if (persistOp == no_persist)
                args.append(*createAttribute(_workflowPersist_Atom));
            if (isGrouped(value))
                args.append(*createAttribute(groupedAtom));
            if (compressFile)
                args.append(*createAttribute(__compressed__Atom));
            if (hasSingleRow(value))
                args.append(*createAttribute(rowAtom));
            if (output->hasAttribute(jobTempAtom))
                args.append(*createAttribute(jobTempAtom));
            if (persistOp != no_stored)
            {
                IHqlExpression * recordCountAttr = queryRecordCountInfo(value);
                if (recordCountAttr)
                    args.append(*LINK(recordCountAttr));
            }

            OwnedHqlExpr getValue = createDataset(no_table, args);
            //getValue.setown(cloneInheritedAnnotations(value, getValue));
            if (persistOp != no_stored)
                getValue.setown(preserveTableInfo(getValue, value, false, (persistOp == no_persist) ? filename : NULL));

            //Note: getValue->queryType() != valueType because the dataset used for field resolution has changed...
            if (value->isDictionary())
                getValue.setown(createDictionary(no_createdictionary, getValue.getClear()));
            getOutput->setown(getValue.getClear());
        }
    }
    else if (type->getTypeCode() == type_void)
    {
        switch (persistOp)
        {
        case no_stored:
        case no_checkpoint:
        case no_once:
        case no_persist:
            setOutput.setown(createSetValue(value, queryAlias(value)));
            break;
        default:
            setOutput.set(value);
            break;
        }
        if(getOutput) getOutput->setown(createValue(no_null, makeVoidType(), createAttribute(_internal_Atom, LINK(sequence), LINK(queryAlias(value)))));
    }
    else
    {
        if (type->getTypeCode() == type_set)
            extraSetAttr.setown(createComma(extraSetAttr.getClear(), createAttribute(_original_Atom, createValue(no_implicitcast, LINK(type), LINK(value)))));
        setOutput.setown(createSetValue(value, queryAlias(value)));
        if(getOutput) getOutput->setown(createGetResultFromSetResult(setOutput, type));
    }
}

void GlobalAttributeInfo::createSmallOutput(IHqlExpression * value, SharedHqlExpr & setOutput)
{
    if (value->getOperator() == no_temptable)
    {
        IHqlExpression * values = value->queryChild(0);
        if ((values->getOperator() == no_null) ||
            ((values->getOperator() == no_list) && (values->numChildren() == 0)))
        {
            OwnedHqlExpr newNull = createDataset(no_null, LINK(value->queryRecord()));
            setOutput.setown(createSetValue(newNull, queryAlias(value)));
            return;
        }
        else if (values->getOperator() == no_all)
        {
            OwnedHqlExpr newAll = createDataset(no_all, LINK(value->queryRecord()));
            setOutput.setown(createSetValue(newAll, queryAlias(value)));
            return;
        }
    }
//  else if (value->getOperator() == no_null)
//  {
//      setOutput.setown(createSetValue(value, queryAlias()));
//      return;
//  }

    HqlExprArray args;
    args.append(*LINK(value));
    args.append(*createAttribute(sequenceAtom, LINK(sequence)));
    args.append(*createAttribute(namedAtom, LINK(queryAlias(value))));
    if (isGrouped(value))
        args.append(*createAttribute(groupedAtom));
    setOutput.setown(createValue(no_output, makeVoidType(), args));
    if (setOp != no_setresult)
    {
        extraSetAttr.setown(createComma(LINK(extraSetAttr), createAttribute(noSetAtom)));
        setOutput.setown(createSetValue(setOutput, queryAlias(value)));
    }
}

void GlobalAttributeInfo::checkFew(HqlCppTranslator & translator)
{
//  if (few && isGrouped(value))
//      translator.WARNINGAT(queryLocation(value), HQLWRN_GroupedGlobalFew);
}


void GlobalAttributeInfo::splitSmallDataset(IHqlExpression * value, SharedHqlExpr & setOutput, OwnedHqlExpr * getOutput)
{
    createSmallOutput(value, setOutput);

    if(getOutput)
    {
        IHqlExpression * record = value->queryRecord();

        HqlExprArray args;
        args.append(*LINK(record));
        args.append(*createAttribute(nameAtom, LINK(queryAlias(value))));
        args.append(*createAttribute(sequenceAtom, LINK(sequence)));
        if (isGrouped(value))
            args.append(*createAttribute(groupedAtom));
        if (persistOp != no_stored)
        {
            IHqlExpression * recordCountAttr = queryRecordCountInfo(value);
            if (recordCountAttr)
                args.append(*LINK(recordCountAttr));
        }
        OwnedHqlExpr wuRead = value->isDictionary() ? createDictionary(no_workunit_dataset, args) : createDataset(no_workunit_dataset, args);
        //wuRead.setown(cloneInheritedAnnotations(value, wuRead));
        if (persistOp != no_stored)
            getOutput->setown(preserveTableInfo(wuRead, value, true, NULL));
        else
            getOutput->set(wuRead);
    }
}

//------------------------------------------------------------------------

static bool isStored(IHqlExpression * set)
{
    switch (set->getOperator())
    {
    case no_setresult:
    case no_ensureresult:
    case no_output:
        return matchesConstantValue(queryAttributeChild(set, sequenceAtom, 0), ResultSequenceStored);
    }
    return false;
}

static bool isTrivialStored(IHqlExpression * set)
{
    switch (set->getOperator())
    {
    case no_setresult:
    case no_ensureresult:
        if (matchesConstantValue(queryAttributeChild(set, sequenceAtom, 0), ResultSequenceStored))
        {
            IHqlExpression * value = set->queryChild(0);
            loop
            {
                switch (value->getOperator())
                {
                case no_constant:
                case no_all:
                case no_null:
                    return true;
                case no_list:
                    return (value->numChildren() == 0);
                case no_cast:
                case no_implicitcast:
                    value = value->queryChild(0);
                    break;
                case no_output:
                    return isTrivialInlineOutput(value);
                default:
                    return false;
                }
            }
        }
        break;
    case no_output:
        return isTrivialInlineOutput(set);
    }
    return false;
}

inline bool isWorkflowAction(IHqlExpression * expr)
{
    return expr && (expr->getOperator() == no_workflow_action);
}

void cloneDependencies(UnsignedArray & tgt, const UnsignedArray & src)
{
    ForEachItemIn(i, src)
        tgt.append(src.item(i));
}


inline bool addDependency(UnsignedArray & tgt, unsigned wfid)
{
    if (!tgt.contains(wfid))
    {
        tgt.append(wfid);
        return true;
    }
    return false;
}

void inheritDependencies(UnsignedArray & tgt, const UnsignedArray & src)
{
    ForEachItemIn(i, src)
        addDependency(tgt, src.item(i));
}


bool hasSameDependencies(UnsignedArray const & d1, UnsignedArray const & d2)
{
    if (d1.ordinality() != d2.ordinality())
        return false;
    ForEachItemIn(i, d2)
    {
        if (d1.find(d2.item(i)) == NotFound)
            return false;
    }
    return true;
}

bool hasExtraDependencies(UnsignedArray const & p, UnsignedArray const & n, UnsignedArray const & ignore)
{
    if (n.ordinality() > p.ordinality() + ignore.ordinality())
        return true;
    ForEachItemIn(i, n)
    {
        unsigned cur = n.item(i);
        if (!p.contains(cur) && !ignore.contains(cur))
            return true;
    }
    return false;
}

void diffDependencies(UnsignedArray & target, UnsignedArray const & d1, UnsignedArray const & d2)
{
    ForEachItemIn(i, d1)
    {
        unsigned cur = d1.item(i);
        if (d2.find(cur) == NotFound)
            addDependency(target, cur);
    }
    ForEachItemIn(j, d2)
    {
        unsigned cur = d2.item(j);
        if (d1.find(cur) == NotFound)
            addDependency(target, cur);
    }
}


void intersectDependencies(UnsignedArray & target, UnsignedArray const & d1, UnsignedArray const & d2)
{
    ForEachItemIn(i, d1)
    {
        unsigned cur = d1.item(i);
        if (d2.find(cur) != NotFound)
            addDependency(target, cur);
    }
}


//------------------------------------------------------------------------

static HqlTransformerInfo workflowTransformerInfo("WorkflowTransformer");
WorkflowTransformer::WorkflowTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator)
: NewHqlTransformer(workflowTransformerInfo), wu(_wu), translator(_translator), wfidCount(0)
{
    const HqlCppOptions & options = translator.queryOptions();
    trivialStoredWfid = 0;
    nextInternalFunctionId = 0;
    onceWfid = 0;
    combineAllStored = options.combineAllStored;
    combineTrivialStored = options.combineTrivialStored;
    expandPersistInputDependencies = options.expandPersistInputDependencies;
    multiplePersistInstances = options.multiplePersistInstances ? options.defaultNumPersistInstances : 0;
    isRootAction = true;
    isRoxie = (translator.getTargetClusterType() == RoxieCluster);
    workflowOut = NULL;
    isConditional = false;
    insideStored = false;
    activeWfid = 0;
}

//-- Helper routines --

IWorkflowItem * WorkflowTransformer::addWorkflowToWorkunit(unsigned wfid, WFType type, WFMode mode, UnsignedArray const & dependencies, ContingencyData const & conts, IHqlExpression * cluster)
{
    Owned<IWorkflowItem> wf(wu->addWorkflowItem(wfid, type, mode, conts.success, conts.failure, conts.recovery, conts.retries, conts.contingencyFor));
    if (cluster)
    {
        StringBuffer clusterText;
        getStringValue(clusterText, cluster);
        wf->setCluster(clusterText);
    }
    ForEachItemIn(idx, dependencies)
        wf->addDependency(dependencies.item(idx));
    return wf.getClear();
}

void WorkflowTransformer::setWorkflowSchedule(IWorkflowItem * wf, const ScheduleData & sched)
{
    if(sched.now)
    {
        wf->setScheduledNow();
    }
    else
    {
        wu->incEventScheduledCount();
        wf->setScheduledOn(sched.eventName.str(), sched.eventText.str());
        if(sched.counting)
        {
            wf->setScheduleCount(sched.count);
            if (sched.count == 0)
                wf->setState(WFStateDone);
        }
    }
    int priority = sched.priority;
    if(priority > 100) priority = 100;
    if(priority < 0) priority = 0;
    wf->setSchedulePriority(priority);
}

void WorkflowTransformer::setWorkflowPersist(IWorkflowItem * wf, char const * persistName, unsigned persistWfid, int numPersistInstances)
{
    wf->setPersistInfo(persistName, persistWfid, numPersistInstances);
}

WorkflowItem * WorkflowTransformer::createWorkflowItem(IHqlExpression * expr, unsigned wfid, node_operator workflowOp)
{
    WorkflowItem * item = new WorkflowItem(wfid, workflowOp);
    expr->unwindList(item->queryExprs(), no_comma);
    gatherIndirectDependencies(item->dependencies, expr);
    return item;
}

IWorkflowItem * WorkflowTransformer::lookupWorkflowItem(unsigned wfid)
{
    Owned<IWorkflowItemIterator> iter = wu->updateWorkflowItems();
    ForEach(*iter)
    {
        Owned<IWorkflowItem> cur = iter->get();
        if (cur->queryWfid() == wfid)
            return cur.getClear();
    }
    return NULL;
}

bool WorkflowTransformer::hasStoredDependencies(IHqlExpression * expr)
{
    return false;
}

void WorkflowTransformer::inheritDependencies(IHqlExpression * expr)
{
    ForEachChild(i, expr)
        copyDependencies(queryBodyExtra(expr->queryChild(i)), queryBodyExtra(expr));
}

void WorkflowTransformer::copyDependencies(WorkflowTransformInfo * source, WorkflowTransformInfo * dest)
{
    if(!source) return;
    UnsignedArray const & dependencies = source->queryDependencies();
    ForEachItemIn(idx, dependencies)
        dest->addDependency(dependencies.item(idx));
}


void WorkflowTransformer::copySetValueDependencies(WorkflowTransformInfo * source, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    if (op == no_compound || op==no_actionlist)
    {
        copySetValueDependencies(source, expr->queryChild(expr->numChildren()-1));
        inheritDependencies(expr);
    }
    else
        copyDependencies(source, queryBodyExtra(expr));
}


unsigned WorkflowTransformer::ensureWorkflowAction(IHqlExpression * expr)
{
    if (isWorkflowAction(expr))
        return (unsigned)getIntValue(expr->queryChild(0));
    unsigned wfid = ++wfidCount;
    Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(expr), rootCluster);
    workflowOut->append(*createWorkflowItem(expr, wfid, no_actionlist));
    return wfid;
}


//-- first pass - extracting workflow

unsigned WorkflowTransformer::splitValue(IHqlExpression * value)
{
    GlobalAttributeInfo info("jobtemp::wf", "wf", value);
    info.sequence.setown(getLocalSequenceNumber());
    info.setOp = no_setresult;
    info.persistOp = no_global;
    OwnedHqlExpr setValue;
    info.checkFew(translator);
    info.splitGlobalDefinition(value->queryType(), value, wu, setValue, 0, (translator.getTargetClusterType() == RoxieCluster));
    inheritDependencies(setValue);
    unsigned wfid = ++wfidCount;
    workflowOut->append(*createWorkflowItem(setValue, wfid, no_global));
    return wfid;
}


WorkflowItem * WorkflowTransformer::findWorkflowItem(unsigned wfid)
{
    ForEachItemIn(i, *workflowOut)
    {
        WorkflowItem & cur = workflowOut->item(i);
        if (cur.wfid == wfid)
            return &cur;
    }
    return NULL;
}

void WorkflowTransformer::extractDependentInputs(UnsignedArray & visited, DependenciesUsed & dependencies, const UnsignedArray & wfids)
{
    ForEachItemIn(i, wfids)
    {
        unsigned wfid = wfids.item(i);
        if (wfid == trivialStoredWfid)
            continue;

        if (visited.contains(wfid))
            continue;

        visited.append(wfid);

        const WorkflowItem * match = findWorkflowItem(wfid);
        assertex(match);
        switch (match->workflowOp)
        {
        case no_persist:
            if (expandPersistInputDependencies)
                break;
            continue;
        case no_stored:
            continue;
        }

        extractDependentInputs(visited, dependencies, match->dependencies);
        ForEachItemIn(iExpr, match->exprs)
            gatherDependencies(&match->exprs.item(iExpr), dependencies, GatherAll);
   }
}

IHqlExpression * WorkflowTransformer::extractWorkflow(IHqlExpression * untransformed, IHqlExpression * expr)
{
    IHqlExpression * value = expr->queryChild(0);
    GlobalAttributeInfo info("jobtemp::wf", "wf", value);
    info.sequence.setown(getLocalSequenceNumber());
    OwnedHqlExpr scheduleActions;

    HqlExprArray actions;
    unwindChildren(actions, expr, 1);

    IHqlExpression * originalAttr = queryAttribute(_original_Atom, actions);
    OwnedHqlExpr codehash;
    if (originalAttr)
    {
        unsigned crc = getExpressionCRC(originalAttr->queryChild(0)) + PERSIST_VERSION;
        codehash.setown(getSizetConstant(crc));
    }

    //First check for duplicate expressions, and cope with the weird case where they are identical except for the annotations.
    //Do it before wfid is allocated to make life simpler
    ForEachItemIn(iCheck, actions)
    {
        IHqlExpression & cur = actions.item(iCheck);
        node_operator curOp = cur.getOperator();
        switch (curOp)
        {
        case no_persist:
        case no_checkpoint:
        case no_stored:
            info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);

            OwnedHqlExpr id = info.getStoredKey();
            unsigned match = alreadyProcessed.find(*id);
            if (match == NotFound)
                break;

            //Compare the definitions - not the expressions, otherwise the original attribute can create false negatives
            IHqlExpression * prevValue = alreadyProcessedExpr.item(match).queryChild(0);
            if(prevValue->queryBody() != value->queryBody())
            {
                StringBuffer s;
                getStoredDescription(s, info.sequence, info.originalLabel, true);
                if(prevValue->queryType() != value->queryBody()->queryType())
                {
#ifdef _DEBUG
                    debugFindFirstDifference(alreadyProcessedExpr.item(match).queryBody(), expr->queryBody());
#endif
                    if (curOp == no_stored)
                        throwError1(HQLERR_DuplicateStoredDiffType, s.str());
                    else
                        throwError1(HQLERR_DuplicateDefinitionDiffType, s.str());
                }
                else if (translator.queryOptions().allowStoredDuplicate)            // only here as a temporary workaround
                    translator.reportWarning(CategoryMistake, SeverityUnknown, queryActiveLocation(expr), HQLERR_DuplicateDefinition, HQLERR_DuplicateDefinition_Text, s.str());
                else
                {
                    if (queryLocationIndependent(prevValue) != queryLocationIndependent(value))
                    {
                        EclIR::dbglogIR(2, queryLocationIndependent(prevValue), queryLocationIndependent(value));
                        if (curOp == no_stored)
                            throwError1(HQLERR_DuplicateStoredDefinition, s.str());
                        else
                            throwError1(HQLERR_DuplicateDefinition, s.str());
                    }
                }
            }

            //If the body was essentially the same, call transform on the previous value - so
            return transform(&alreadyProcessedUntransformed.item(match));
        }
    }

    ContingencyData conts;
    ScheduleData sched;
    unsigned wfid = ++wfidCount;
    unsigned schedWfid = 0;
    ForEachItemIn(idx, actions)
    {
        IHqlExpression & cur = actions.item(idx);
        node_operator curOp = cur.getOperator();
        switch (curOp)
        {
        case no_persist:
            if (isRoxie)
            {
                // MORE - Add dynamic attribute to ensure the file is not pre-resolved
            }
            //fall through
        case no_checkpoint:
        case no_stored:
            {
                info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);

                OwnedHqlExpr id = info.getStoredKey();
                alreadyProcessed.append(*id.getClear());
                alreadyProcessedExpr.append(*LINK(expr));
                alreadyProcessedUntransformed.append(*LINK(untransformed));
            }
            break;
        case no_independent:
        case no_once:
            info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);
            break;
        case no_success:
            {
                OwnedHqlExpr successExpr = transformSequentialEtc(cur.queryChild(0));
                conts.success = splitValue(successExpr);
                Owned<IWorkflowItem> wf = addWorkflowContingencyToWorkunit(conts.success, WFTypeSuccess, WFModeNormal, queryDirectDependencies(successExpr), NULL, wfid);
                info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);
                break;
            }
        case no_failure:
            {
                OwnedHqlExpr failureExpr = transformSequentialEtc(cur.queryChild(0));
                conts.failure = splitValue(failureExpr);
                Owned<IWorkflowItem> wf = addWorkflowContingencyToWorkunit(conts.failure, WFTypeFailure, WFModeNormal, queryDirectDependencies(failureExpr), NULL, wfid);
                info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);
                break;
            }
        case no_recovery:
            {
            conts.recovery = splitValue(cur.queryChild(0));
            conts.retries = (unsigned)getIntValue(cur.queryChild(1), 0);
                Owned<IWorkflowItem> wf = addWorkflowContingencyToWorkunit(conts.recovery, WFTypeRecovery, WFModeNormal, queryDirectDependencies(cur.queryChild(0)), NULL, wfid);
            info.extractStoredInfo(&cur, codehash, isRoxie, multiplePersistInstances);
            break;
            }
        case no_attr:
            assertex(cur.queryName() == _original_Atom);
            break;
        case no_when:
            {
                OwnedHqlExpr folded = foldHqlExpression(&cur);
                IHqlExpression * event = folded->queryChild(0);
                IHqlExpression * eventFilter = event->queryChild(1);

                sched.now = false;
                event->queryChild(0)->queryValue()->getStringValue(sched.eventName);
                if (eventFilter)
                    eventFilter->queryValue()->getStringValue(sched.eventText);
                else
                    sched.eventText.append("*");
                if(cur.numChildren()>1)
                {
                    sched.counting = true;
                    sched.count = (unsigned)getIntValue(folded->queryChild(1));
                }
                sched.independent = true;
            }
            break;
        case no_priority:
            {
                sched.priority = (int)getIntValue(cur.queryChild(0));
                sched.independent = true;
                break;
            }
        default:
            throwUnexpectedOp(curOp);
        }
    }

    if (isDependentOnParameter(expr))
    {
        StringBuffer s;
        getStoredDescription(s, info.sequence, info.originalLabel, true);
        translator.reportWarning(CategoryMistake, SeverityUnknown, queryActiveLocation(expr), HQLWRN_WorkflowDependParameter, HQLWRN_WorkflowDependParameter_Text, s.str());
    }

    OwnedHqlExpr setValue;
    OwnedHqlExpr getValue;
    bool done = false;
    if (info.setOp != no_none)
    {
        assertex(!sched.independent);           // should have been enforced by the tree normalization
        ITypeInfo * type = expr->queryType();
        info.checkFew(translator);
        info.splitGlobalDefinition(type, value, wu, setValue, &getValue, isRoxie);
        copySetValueDependencies(queryBodyExtra(value), setValue);
    }
    else
    {
        assertex(sched.independent);
        getValue.set(value);
        done = true;
        schedWfid = wfid;
    }

    if(!sched.independent && !conts.success && !conts.failure && !conts.recovery)
    {
        bool combine = false;
        if (combineAllStored && !hasNonTrivialDependencies(setValue))
        {
            switch (getResultSequenceValue(setValue))
            {
            case ResultSequenceStored:
                combine = true;
                break;
            case ResultSequenceInternal:
                combine = insideStored;
                break;
            }
        }

        if (info.persistOp == no_once)
        {
            //MORE: Error if refers to stored or persist
            if (queryDirectDependencies(setValue).ordinality())
                translator.ERRORAT(queryLocation(untransformed), HQLERR_OnceCannotAccessStored);

            if (onceWfid == 0)
            {
                onceWfid = wfid;
            }
            else
            {
                wfid = onceWfid;
                wfidCount--;
            }
            if (!onceExprs.contains(*setValue))
                onceExprs.append(*LINK(setValue));
            done = true;
        }

        if (combineTrivialStored && isTrivialStored(setValue))
            combine = true;

        if (combine)
        {
            if (trivialStoredWfid == 0)
            {
                trivialStoredWfid = wfid;
                storedWfids.append(wfid);
            }
            else
            {
                wfid = trivialStoredWfid;
                wfidCount--;
            }
            if (trivialStoredExprs.find(*setValue) == NotFound)
                trivialStoredExprs.append(*LINK(setValue));
            done = true;
        }
    }

    if (!done)
    {
        if (info.persistOp == no_stored)
            storedWfids.append(wfid);

        //If you really want side effects within a no_persist to be processed in the correct sequence
        //you need to use persist(failure(independent, f(independent))
        //It generally makes worse code (and incorrect in jbellow.xhql) if they are expanded.
        //because there is no ensure result  in the expected wfid.
        if ((info.persistOp != no_persist) && expr->isAction())
            setValue.setown(transformSequentialEtc(setValue));

        if(info.persistOp == no_persist)
        {
            StringBuffer persistName;
            info.storedName->queryValue()->getStringValue(persistName);
            unsigned persistWfid = ++wfidCount;
            Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModePersist, queryDirectDependencies(setValue), conts, info.queryCluster());
            setWorkflowPersist(wf, persistName.str(), persistWfid, info.queryMaxPersistCopies());

            DependenciesUsed dependencies(false);
            UnsignedArray visited;
            extractDependentInputs(visited, dependencies, queryDirectDependencies(setValue));
            gatherDependencies(setValue, dependencies, GatherAll);
            dependencies.removeInternalReads();

            HqlExprArray checkArgs;
            checkArgs.append(*createExprAttribute(_files_Atom, dependencies.tablesRead));
            inheritDependencies(&checkArgs.item(0));
            if (dependencies.resultsRead.ordinality())
            {
                checkArgs.append(*createExprAttribute(_results_Atom, dependencies.resultsRead));
                inheritDependencies(&checkArgs.item(1));
            }
            checkArgs.append(*createAttribute(_codehash_Atom, LINK(codehash)));
            checkArgs.append(*createAttribute(namedAtom, LINK(info.storedName)));
            if (expr->isDataset())
                checkArgs.append(*createAttribute(fileAtom));
            OwnedHqlExpr check = createValue(no_persist_check, makeVoidType(), checkArgs);
            inheritDependencies(check);

            workflowOut->append(*createWorkflowItem(check, persistWfid, no_actionlist));
            workflowOut->append(*createWorkflowItem(setValue, wfid, no_persist));

            Owned<IWorkflowItem> wfPersist = addWorkflowToWorkunit(persistWfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(check), NULL);
        }
        else
        {
            if (info.queryCluster())
            {
                OwnedHqlExpr cluster = createValue(no_cluster, makeVoidType(), LINK(setValue), LINK(info.queryCluster()));
                inheritDependencies(cluster);
                setValue.set(cluster);
            }
            Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(setValue), conts, info.queryCluster());
            workflowOut->append(*createWorkflowItem(setValue, wfid, info.persistOp));
        }
    }

    if(sched.independent)
    {
        if (schedWfid == 0)
            schedWfid = ++wfidCount;
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(schedWfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(getValue), info.queryCluster());
        setWorkflowSchedule(wf, sched);
        workflowOut->append(*createWorkflowItem(getValue, schedWfid, no_none));
        getValue.setown(createNullExpr(expr->queryType()));
    }
    else
        queryBodyExtra(getValue.get())->addDependency(wfid);

    return getValue.getClear();
}


IHqlExpression * WorkflowTransformer::extractCommonWorkflow(IHqlExpression * expr, IHqlExpression * transformed)
{
    if (!transformed->queryDataset())
        return LINK(transformed);

    WorkflowTransformInfo * extra = queryBodyExtra(expr);
    if (!extra->isCommonUpCandidate() || !isWorthHoisting(transformed, false))
        return LINK(transformed);

    if (isContextDependent(transformed) || !isIndependentOfScope(transformed))
        return LINK(transformed);

    StringBuffer s;
    IHqlExpression * location = activeLocations.ordinality() ? &activeLocations.tos() : NULL;
    if (!translator.queryOptions().performWorkflowCse)
    {
        s.appendf("AutoWorkflow: Try adding ': INDEPENDENT' to %s ", getOpString(expr->getOperator()));
        if (expr->queryName())
            s.append("[").append(expr->queryName()).append("] ");
        s.append(" to common up code between workflow items");
        DBGLOG("%s", s.str());
        translator.addWorkunitException(SeverityInformation, HQLWRN_TryAddingIndependent, s.str(), location);
        if (!translator.queryOptions().performWorkflowCse)
            return LINK(transformed);
    }

    //This code would need a lot more work for it to be enabled by default.
    // e.g., ensure it really is worth commoning up, the expressions aren't to be evaluated on different clusters etc. etc.
    unsigned wfid = ++wfidCount;

    s.appendf("AutoWorkflow: Spotted %s ", getOpString(expr->getOperator()));
    if (expr->queryName())
        s.append("[").append(expr->queryName()).append("] ");
    s.append(" to common up between workflow items [").append(wfid).append("]");
    DBGLOG("%s", s.str());
    translator.addWorkunitException(SeverityInformation, 0, s.str(), location);

    GlobalAttributeInfo info("jobtemp::wfa", "wfa", transformed);
    info.extractGlobal(NULL, translator.getTargetClusterType());       // should really be a slightly different function

    OwnedHqlExpr setValue;
    OwnedHqlExpr getValue;
    ContingencyData conts;
    WorkflowTransformInfo * transformedExtra = queryBodyExtra(transformed);
    info.splitGlobalDefinition(transformed->queryType(), transformed, wu, setValue, &getValue, isRoxie);
    copySetValueDependencies(transformedExtra, setValue);

    Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(setValue), conts, NULL);
    workflowOut->append(*createWorkflowItem(setValue, wfid, no_actionlist));

    queryBodyExtra(getValue.get())->addDependency(wfid);
    return getValue.getClear();
}

IHqlExpression * WorkflowTransformer::transformInternalFunction(IHqlExpression * newFuncDef)
{
    IHqlExpression * body = newFuncDef->queryChild(0);
    if (body->getOperator() != no_outofline)
        return LINK(newFuncDef);

    IHqlExpression * ecl = body->queryChild(0);

    StringBuffer funcname;
    funcname.append("user").append(++nextInternalFunctionId);
    if (translator.queryOptions().debugGeneratedCpp)
        funcname.append("_").append(newFuncDef->queryName()).toLowerCase();
    OwnedHqlExpr funcNameExpr = createConstant(funcname);

    IHqlExpression * formals = newFuncDef->queryChild(1);
    OwnedHqlExpr newFormals = mapInternalFunctionParameters(formals);

    HqlExprArray bodyArgs;
    bodyArgs.append(*replaceParameters(ecl, formals, newFormals));
    unwindChildren(bodyArgs, body, 1);
    bodyArgs.append(*createLocalAttribute());
    bodyArgs.append(*createExprAttribute(entrypointAtom, LINK(funcNameExpr)));
    OwnedHqlExpr newBody = body->clone(bodyArgs);
    inheritDependencies(newBody);
    copyDependencies(queryBodyExtra(ecl), queryBodyExtra(newBody));

    HqlExprArray funcdefArgs;
    funcdefArgs.append(*LINK(newBody));
    funcdefArgs.append(*LINK(newFormals));
    unwindChildren(funcdefArgs, newFuncDef, 2);
    OwnedHqlExpr namedFuncDef = newFuncDef->clone(funcdefArgs);
    inheritDependencies(namedFuncDef);

    if (ecl->getOperator() == no_embedbody)
        return namedFuncDef.getClear();

    WorkflowItem * item = new WorkflowItem(namedFuncDef);
    workflowOut->append(*item);
    OwnedHqlExpr external = createExternalFuncdefFromInternal(namedFuncDef);
    copyDependencies(queryBodyExtra(namedFuncDef), queryBodyExtra(external));
    return external.getClear();
}

IHqlExpression * WorkflowTransformer::transformInternalCall(IHqlExpression * transformed)
{
    IHqlExpression * funcDef = transformed->queryDefinition();
    Owned<IHqlExpression> newFuncDef = transform(funcDef);

    HqlExprArray parameters;
    unwindChildren(parameters, transformed);
    OwnedHqlExpr rebound = createReboundFunction(newFuncDef, parameters);
    inheritDependencies(rebound);
    copyDependencies(queryBodyExtra(newFuncDef), queryBodyExtra(rebound));
    return rebound.getClear();
}


IHqlExpression * WorkflowTransformer::createTransformed(IHqlExpression * expr)
{
    //Could short-circuit if doesn't contain workflow, but it also modifies outputs/buildindex...

    //Force record to be transformed - so any stored values in record (ifblock!!) are hoisted.
    node_operator op = expr->getOperator();
    if (op == no_param)
        return LINK(expr);

    if (op == no_transform || op == no_newtransform)
        ::Release(transform(expr->queryRecord()));

    IHqlExpression * body = expr->queryBody(true);
    if (expr != body)
    {
        switch (expr->getAnnotationKind())
        {
        case annotate_location:
        case annotate_symbol:
            activeLocations.append(*expr);
            break;
        }
        OwnedHqlExpr transformedBody = transform(body);
        switch (expr->getAnnotationKind())
        {
        case annotate_location:
        case annotate_symbol:
            activeLocations.pop();
            break;
        }
        OwnedHqlExpr transformed = (transformedBody == body) ? LINK(expr) : expr->cloneAnnotation(transformedBody);
        //more: this really shouldn't be needed
        inheritDependencies(transformed);
        return transformed.getClear();
    }

    bool wasInsideStored = insideStored;
    if ((op == no_colon) && queryOperatorInList(no_stored, expr->queryChild(1)))
        insideStored = true;
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    insideStored = wasInsideStored;
    inheritDependencies(transformed);

    switch (op)
    {
#if 0
        //MORE: Workflow in user functions doesn't work for roxie at the moment
    case no_call:
        transformed.setown(transformCall(transformed));
        inheritDependencies(transformed);
        copyDependencies(queryBodyExtra(transformed->queryExternalDefinition()), queryBodyExtra(transformed));
        break;
    case no_externalcall:
        transformed.setown(transformExternalCall(transformed));
        inheritDependencies(transformed);
        copyDependencies(queryExtra(transformed->queryExternalDefinition()), queryExtra(transformed));
        break;
#endif
    case no_colon:
        if (translator.insideLibrary())
        {
            SCMStringBuffer libraryName;
            StringBuffer colonText(" (");
            getOutputLibraryName(libraryName, wu);
            getExprECL(expr, colonText);
            colonText.append(")");
            throwError2(HQLERR_LibraryCannotContainWorkflow, libraryName.str(), colonText.str());
        }
        transformed.setown(extractWorkflow(expr, transformed));
        break;
    case no_output:
    case no_buildindex:
        {
            IHqlExpression * updateAttr = transformed->queryAttribute(updateAtom);
            if (updateAttr)
            {
                DependenciesUsed dependencies(false);
                gatherDependencies(transformed->queryChild(0), dependencies, GatherAll);
                dependencies.removeInternalReads();

                bool canEvaluateFilenames = true;
                HqlExprArray updateArgs;
                unwindChildren(updateArgs, updateAttr);
                if (dependencies.tablesRead.ordinality())
                {
                    OwnedHqlExpr attr = createExprAttribute(_files_Atom, dependencies.tablesRead);
                    if (!isIndependentOfScope(attr) || isContextDependent(attr))
                    {
                        if (!updateAttr->hasAttribute(alwaysAtom))
                            throwError(HQLERR_InputsAreTooComplexToUpdate);
                        canEvaluateFilenames = false;
                    }
                    else
                        updateArgs.append(*attr.getClear());
                }
                if (dependencies.resultsRead.ordinality())
                    updateArgs.append(*createExprAttribute(_results_Atom, dependencies.resultsRead));

                HqlExprArray args;
                unwindChildren(args, transformed);
                args.zap(*updateAttr);
                if (canEvaluateFilenames)
                    args.append(*createExprAttribute(updateAtom, updateArgs));
                transformed.setown(transformed->clone(args));
                inheritDependencies(transformed);
            }
            break;
        }
    case no_funcdef:
        transformed.setown(transformInternalFunction(transformed));
        break;
    case no_call:
        transformed.setown(transformInternalCall(transformed));
        break;
    }

    return extractCommonWorkflow(expr, transformed);
}

//-- second pass - sort out sequential etc.

/*
 This is very tricky...  The problem is we only want to create workflow actions for sequential/parallel and conditions if they
 are necessary.  In particular.
 o workflow items are only executed once per invocation
 o create them for sequential if the dependencies haven't already been evaluated
 o create them for conditions if the non-intersection of the dependencies for the branches haven't already been evaluated
 o create if a workflow action has been created for a child action.
 o can't rely on createTransform() updating the dependencies so-far because the transform() may be cached.
 o Need to be careful that dependencies done so far are set up correctly before each call to transform()
*/

UnsignedArray const & WorkflowTransformer::queryDependencies(unsigned wfid)
{
    if (wfid == trivialStoredWfid)
        return emptyDependencies;
    ForEachItemIn(i, *workflowOut)
    {
        WorkflowItem & cur = workflowOut->item(i);
        if (cur.wfid == wfid)
            return cur.dependencies;
    }
    throwUnexpected();
}


void WorkflowTransformer::gatherIndirectDependencies(UnsignedArray & result, IHqlExpression * expr)
{
    if (isWorkflowAction(expr))
    {
        unsigned wfid = (unsigned)getIntValue(expr->queryChild(0));
        ::inheritDependencies(result, queryDependencies(wfid));
    }
    else
    {
        const UnsignedArray & direct = queryBodyExtra(expr)->queryDependencies();
        ForEachItemIn(i, direct)
        {
            unsigned wfid = direct.item(i);
            if (addDependency(result, wfid))
                ::inheritDependencies(result, queryDependencies(wfid));
        }
    }
}

bool WorkflowTransformer::hasNonTrivialDependencies(IHqlExpression * expr)
{
    UnsignedArray const & dependencies = queryDirectDependencies(expr);
    ForEachItemIn(i, dependencies)
    {
        unsigned cur = dependencies.item(i);
        if ((cur != trivialStoredWfid) && (cur != onceWfid))
            return true;
    }
    return false;
}

UnsignedArray const & WorkflowTransformer::queryDirectDependencies(IHqlExpression * expr)
{
    return queryBodyExtra(expr)->queryDependencies();
}


void WorkflowTransformer::cacheWorkflowDependencies(unsigned wfid, UnsignedArray & extra)
{
    WorkflowItem * item = new WorkflowItem(wfid, no_actionlist);
    ForEachItemIn(i, extra)
    {
        unsigned wfid = extra.item(i);
        item->dependencies.append(wfid);
        ::inheritDependencies(item->dependencies, queryDependencies(wfid));
    }
    workflowOut->append(*item);
}


IHqlExpression * WorkflowTransformer::createWorkflowAction(unsigned wfid)
{
    //NB: Needs to include wfid as an argument otherwise inherited dependencies get messed up
    OwnedHqlExpr transformed = createValue(no_workflow_action, makeVoidType(), getSizetConstant(wfid));
    queryBodyExtra(transformed)->addDependency(wfid);
    return transformed.getClear();
}


void WorkflowTransformer::ensureWorkflowAction(UnsignedArray & dependencies, IHqlExpression * expr)
{
    unsigned wfid = ensureWorkflowAction(expr);
    addDependency(dependencies, wfid);
}

//Create a sequential workflow action if any of the branches contains a workflow action
IHqlExpression * WorkflowTransformer::createCompoundWorkflow(IHqlExpression * expr)
{
    HqlExprArray pendingBranches;
    UnsignedArray childWfid;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        unsigned mark = markDependencies();
        OwnedHqlExpr transformed = transformRootAction(cur);
        restoreDependencies(mark);

        if (isWorkflowAction(transformed))
        {
            if (pendingBranches.ordinality())
            {
                OwnedHqlExpr branch = createActionList(pendingBranches);
                inheritDependencies(branch);
                ensureWorkflowAction(childWfid, branch);
                pendingBranches.kill();
            }
            ensureWorkflowAction(childWfid, transformed);
        }
        else
        {
            pendingBranches.append(*LINK(transformed));
        }
        gatherIndirectDependencies(cumulativeDependencies, transformed);
    }

    if (childWfid.ordinality())
    {
        if (pendingBranches.ordinality())
        {
            OwnedHqlExpr branch = createActionList(pendingBranches);
            inheritDependencies(branch);
            ensureWorkflowAction(childWfid, branch);
        }

        unsigned wfid = ++wfidCount;
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeSequential, childWfid, rootCluster);
        cacheWorkflowDependencies(wfid, childWfid);
        return createWorkflowAction(wfid);
    }

    return LINK(expr);
}


//Create a sequential workflow action if any of the branches introduce new dependencies/or creates a workflow item (e.g., wait!)
IHqlExpression * WorkflowTransformer::createSequentialWorkflow(IHqlExpression * expr)
{
    OwnedHqlExpr nextBranch;
    UnsignedArray childWfid;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        unsigned mark = markDependencies();
        OwnedHqlExpr transformed = transformRootAction(cur);
        restoreDependencies(mark);

        UnsignedArray dependencies;
        gatherIndirectDependencies(dependencies, transformed);
        if (hasExtraDependencies(cumulativeDependencies, dependencies, storedWfids) || isWorkflowAction(transformed))
        {
            if (nextBranch)
            {
                ensureWorkflowAction(childWfid, nextBranch);
                nextBranch.clear();
            }
            ::inheritDependencies(cumulativeDependencies, dependencies);
            if (isWorkflowAction(transformed))
                ensureWorkflowAction(childWfid, transformed);
            else
                nextBranch.set(transformed);
        }
        else
        {
            if (nextBranch)
                nextBranch.setown(createValue(expr->getOperator(), nextBranch.getClear(), LINK(transformed)));
            else
                nextBranch.set(transformed);
            inheritDependencies(nextBranch);
        }
    }

    if (childWfid.ordinality())
    {
        if (nextBranch)
            ensureWorkflowAction(childWfid, nextBranch);

        unsigned wfid = ++wfidCount;
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeSequential, childWfid, rootCluster);
        cacheWorkflowDependencies(wfid, childWfid);
        return createWorkflowAction(wfid);
    }

    return LINK(expr);
}


// Create a parallel workflow action if any of the child actions are workflow actions
IHqlExpression * WorkflowTransformer::createParallelWorkflow(IHqlExpression * expr)
{
    HqlExprArray branches;
    UnsignedArray childWfid;
    unsigned mark = markDependencies();
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        OwnedHqlExpr transformed = transformRootAction(cur);
        if (isWorkflowAction(transformed))
            ensureWorkflowAction(childWfid, transformed);
        else
            branches.append(*LINK(transformed));
        restoreDependencies(mark);
    }


    if (childWfid.ordinality())
    {
        if (branches.ordinality())
        {
            OwnedHqlExpr branch = createActionList(branches);
            inheritDependencies(branch);
            ensureWorkflowAction(childWfid, branch);
        }

        unsigned wfid = ++wfidCount;
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeParallel, childWfid, rootCluster);
        cacheWorkflowDependencies(wfid, childWfid);
        return createWorkflowAction(wfid);
    }

    return LINK(expr);
}

IHqlExpression * WorkflowTransformer::createIfWorkflow(IHqlExpression * expr)
{
    IHqlExpression * cond = expr->queryChild(0);
    IHqlExpression * trueExpr = expr->queryChild(1);
    IHqlExpression * falseExpr = expr->queryChild(2);

    OwnedHqlExpr newCond = LINK(cond);
    gatherIndirectDependencies(cumulativeDependencies, cond);

    //more: inherit dependencies?
    UnsignedArray trueDepends, falseDepends;
    unsigned mark = markDependencies();
    OwnedHqlExpr newTrueExpr = transformRootAction(trueExpr);
    restoreDependencies(mark);
    OwnedHqlExpr newFalseExpr = falseExpr ? transformRootAction(falseExpr) : NULL;
    restoreDependencies(mark);

    //Need to turn a conditional action into a conditional workflow item if
    //i) it has a workflow action as a child.
    //ii) the true/false branches are dependent on something that hasn't already been evaluated
    //    (and isn't shared between both branches)
    bool needToCreateWorkflow = false;
    if (hasDependencies(newTrueExpr) || (newFalseExpr && hasDependencies(newFalseExpr)))
    {
        needToCreateWorkflow = isWorkflowAction(newTrueExpr) || isWorkflowAction(newFalseExpr);
        if (!needToCreateWorkflow)
        {
            //Failures are assumed to be exceptional, so don't worry about extra dependencies
            if (!isFailAction(newTrueExpr) && !isFailAction(newFalseExpr))
            {
                UnsignedArray newTrueDepends;
                gatherIndirectDependencies(newTrueDepends, newTrueExpr);
                if (!falseExpr)
                    needToCreateWorkflow = hasExtraDependencies(cumulativeDependencies, newTrueDepends, storedWfids);
                else
                {
                    UnsignedArray newFalseDepends;
                    gatherIndirectDependencies(newFalseDepends, newFalseExpr);
                    UnsignedArray diff;
                    diffDependencies(diff, newTrueDepends, newFalseDepends);
                    needToCreateWorkflow = hasExtraDependencies(cumulativeDependencies, diff, storedWfids);
                }
            }
        }

        if (needToCreateWorkflow)
        {
            //Represent as wfid(cond-wfid, true-wfid, false-wfid)
            UnsignedArray dependencies;
            OwnedHqlExpr setCondExpr = createValue(no_setworkflow_cond, makeVoidType(), LINK(cond));
            inheritDependencies(setCondExpr);

            ensureWorkflowAction(dependencies, setCondExpr);
            ensureWorkflowAction(dependencies, newTrueExpr);

            if (newFalseExpr)
                ensureWorkflowAction(dependencies, newFalseExpr);

            unsigned wfid = ++wfidCount;
            Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeCondition, dependencies, rootCluster);

            WorkflowItem * item = new WorkflowItem(wfid, no_if);
            cloneDependencies(item->dependencies, dependencies);

            if (falseExpr)
            {
                UnsignedArray newTrueDepends;
                UnsignedArray newFalseDepends;
                gatherIndirectDependencies(newTrueDepends, newTrueExpr);
                gatherIndirectDependencies(newFalseDepends, newFalseExpr);
                intersectDependencies(item->dependencies, newTrueDepends, newFalseDepends);
            }

            workflowOut->append(*item);
            return createWorkflowAction(wfid);
        }
    }

    return LINK(expr);
}


IHqlExpression * WorkflowTransformer::createWaitWorkflow(IHqlExpression * expr)
{
    //First create a EndWait workflow item which has a when clause of the wait criteria
    OwnedHqlExpr folded = foldHqlExpression(expr);
    IHqlExpression * event = folded->queryChild(0);
    IHqlExpression * eventFilter = event->queryChild(1);

    ScheduleData sched;
    sched.now = false;
    getStringValue(sched.eventName, event->queryChild(0));
    if (eventFilter)
        getStringValue(sched.eventText, eventFilter);
    else
        sched.eventText.append("*");
    sched.counting = true;
    sched.count = 0;
    sched.independent = true;

    unsigned endWaitWfid = ++wfidCount;
    UnsignedArray noDependencies;
    Owned<IWorkflowItem> wf = addWorkflowToWorkunit(endWaitWfid, WFTypeNormal, WFModeWait, noDependencies, rootCluster);
    setWorkflowSchedule(wf, sched);
    OwnedHqlExpr doNothing = createValue(no_null, makeVoidType());
    workflowOut->append(*createWorkflowItem(doNothing, endWaitWfid, no_wait));

    //Now create a wait entry, with the EndWait as the dependency
    UnsignedArray dependencies;
    dependencies.append(endWaitWfid);

    unsigned beginWaitWfid = ++wfidCount;
    Owned<IWorkflowItem> wfWait = addWorkflowToWorkunit(beginWaitWfid, WFTypeNormal, WFModeBeginWait, dependencies, rootCluster);
    cacheWorkflowDependencies(beginWaitWfid, dependencies);
    return createWorkflowAction(beginWaitWfid);
}


IHqlExpression * WorkflowTransformer::transformRootAction(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_compound:
        if (expr->isAction())
            return createCompoundWorkflow(expr);
        break;
    case no_parallel:
        return createParallelWorkflow(expr);
    case no_sequential:
    case no_orderedactionlist:
        return createSequentialWorkflow(expr);
    case no_actionlist:
        return createCompoundWorkflow(expr);
    case no_if:
        if (expr->isAction())
            return createIfWorkflow(expr);
        break;
    case no_wait:
        return createWaitWorkflow(expr);
    case no_ensureresult:
        {
            IHqlExpression * value = expr->queryChild(0);
            if (!value->isAction())
                break;
            OwnedHqlExpr transformed = transformRootAction(value);
            if (value == transformed)
                break;
            HqlExprArray args;
            args.append(*transformed.getClear());
            unwindChildren(args, expr, 1);
            OwnedHqlExpr ret = expr->clone(args);
            inheritDependencies(ret);
            return ret.getClear();
        }
    }

    return LINK(expr);
}

IHqlExpression * WorkflowTransformer::transformSequentialEtc(IHqlExpression * expr)
{
    unsigned mark = markDependencies();
    //Ignore differences in access to trivial stored variables.
    if (trivialStoredWfid)
        cumulativeDependencies.append(trivialStoredWfid);
    if (onceWfid)
        cumulativeDependencies.append(onceWfid);
    OwnedHqlExpr ret = transformRootAction(expr);
    restoreDependencies(mark);
    return ret.getClear();
}

void WorkflowTransformer::percolateScheduledIds(WorkflowArray & workflow)
{
    ForEachItemIn(i, workflow)
    {
        WorkflowItem & cur = workflow.item(i);
        Owned<IWorkflowItem> wf = lookupWorkflowItem(cur.queryWfid());
        if (wf && wf->isScheduledNow())
        {
            ForEachItemIn(i2, cur.dependencies)
            {
                Owned<IWorkflowItem> child = lookupWorkflowItem(cur.dependencies.item(i2));
                if (child->queryMode() == WFModeWait)
                    child->setScheduledWfid(cur.queryWfid());
            }
        }
    }
}

///- workflow processing

void WorkflowTransformer::analyseExpr(IHqlExpression * expr)
{
    WorkflowTransformInfo * extra = queryBodyExtra(expr);
    if (extra->noteWorkflow(activeWfid, isConditional))
        return;

    switch (expr->getOperator())
    {
    case no_allnodes:
        //MORE: Do I need to recurse and explicitly disable hoisting?
        return;
    case no_if:
        {
            bool wasConditional = isConditional;
            analyseExpr(expr->queryChild(0));
            isConditional = true;
            analyseExpr(expr->queryChild(1));
            if (expr->queryChild(2))
                analyseExpr(expr->queryChild(2));
            isConditional = wasConditional;
            return;
        }
    case no_colon:
        {
            if (!isIndependentOfScope(expr->queryChild(0)))
            {
                StringBuffer s;
                if (expr->queryName())
                    s.appendf(" '%s'", str(expr->queryName()));
                //MORE: Better if we also kept nested track of locations
                translator.WARNINGAT1(CategoryMistake, queryActiveLocation(expr), HQLWRN_WorkflowSeemsToBeDependent, s.str());
            }

            bool wasConditional = isConditional;
            isConditional = false;
            unsigned prevWfid = activeWfid;
            activeWfid = ++wfidCount;
            analyseExpr(expr->queryChild(0));
            activeWfid = prevWfid;
            isConditional = wasConditional;
            return;
        }
    }

    NewHqlTransformer::analyseExpr(expr);
}


void WorkflowTransformer::analyseAll(const HqlExprArray & in)
{
    activeWfid = ++wfidCount;
    analyseArray(in, 0);
    wfidCount = 0;
}


void WorkflowTransformer::transformRoot(const HqlExprArray & in, WorkflowArray & out)
{
    wfidCount = 0;
    workflowOut = &out;
    HqlExprArray transformed;
    WorkflowTransformInfo globalInfo(NULL);
    ForEachItemIn(idx, in)
    {
        OwnedHqlExpr ret = transform(&in.item(idx));
        copyDependencies(queryBodyExtra(ret), &globalInfo);
        //ignore results that do nothing, but still collect the dependencies...
        if (ret->getOperator() != no_null)
            transformed.append(*ret.getClear());
    }

    if (onceExprs.length())
    {
        //By definition they don't have any dependencies, so no need to call inheritDependencies.
        OwnedHqlExpr onceExpr = createActionList(onceExprs);
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(onceWfid, WFTypeNormal, WFModeOnce, queryDirectDependencies(onceExpr), NULL);
        wf->setScheduledNow();
        out.append(*createWorkflowItem(onceExpr, onceWfid, no_once));
    }

    if (trivialStoredExprs.length())
    {
        //By definition they don't have any dependencies, so no need to call inheritDependencies.
        OwnedHqlExpr trivialStoredExpr = createActionList(trivialStoredExprs);
        Owned<IWorkflowItem> wf = addWorkflowToWorkunit(trivialStoredWfid, WFTypeNormal, WFModeNormal, queryDirectDependencies(trivialStoredExpr), NULL);
        out.append(*createWorkflowItem(trivialStoredExpr, trivialStoredWfid, no_stored));
    }

    if (transformed.ordinality())
    {
        //Handle sequential etc.
        OwnedHqlExpr combined = createActionList(transformed);
        OwnedHqlExpr result = transformSequentialEtc(combined);
        transformed.kill();
        transformed.append(*result.getClear());
    }

    UnsignedArray const & dependencies = globalInfo.queryDependencies();
    if(transformed.ordinality() || dependencies.ordinality())
    {
        if ((transformed.ordinality() == 0) && (dependencies.ordinality() == 1))
        {
            Owned<IWorkflowItem> wf = lookupWorkflowItem(dependencies.item(0));
            wf->setScheduledNow();
        }
        else
        {
            Owned<IHqlExpression> combinedItems = createComma(transformed);
            if (!combinedItems)
                combinedItems.setown(createValue(no_null, makeVoidType()));

            unsigned wfid;
            if (!isWorkflowAction(combinedItems))
            {
                wfid = ++wfidCount;
                ScheduleData sched;
                Owned<IWorkflowItem> wf = addWorkflowToWorkunit(wfid, WFTypeNormal, WFModeNormal, dependencies, NULL);
                setWorkflowSchedule(wf, sched);
                out.append(*createWorkflowItem(combinedItems, wfid, no_actionlist));
            }
            else
                wfid = ensureWorkflowAction(combinedItems);

            Owned<IWorkflowItem> wf = lookupWorkflowItem(wfid);
            wf->setScheduledNow();
        }
    }

    workflowOut = NULL;
    percolateScheduledIds(out);
}

void extractWorkflow(HqlCppTranslator & translator, HqlExprArray & exprs, WorkflowArray & out)
{
    WorkflowTransformer transformer(translator.wu(), translator);
    if (translator.queryOptions().performWorkflowCse || translator.queryOptions().notifyWorkflowCse)
        transformer.analyseAll(exprs);
    transformer.transformRoot(exprs, out);
}



//------------------------------------------------------------------------

enum { SIKnone, SIKhole, SIKagent, SIKthor };
class StatementInfo : public CInterface
{
public:
    StatementInfo(IHqlExpression * _expr);

    void calcDependencies();
    bool canSwapOrder(StatementInfo & other)
    {
        return queryDependencies().canSwapOrder(other.queryDependencies());
    }
    inline bool isConditional()     { return expr->getOperator() == no_if; }
    inline bool isThorQuery()       { return category == SIKthor; }
    DependenciesUsed & queryDependencies()
    {
        if (!hasDependencies)
        {
            calcDependencies();
            hasDependencies = true;
        }
        return dependencies;
    }

public:
    HqlExprAttr expr;

protected:
    DependenciesUsed dependencies;
    bool hasDependencies;
    unsigned category;
};


StatementInfo::StatementInfo(IHqlExpression * _expr) : dependencies(true)
{
    expr.set(_expr);
    if (expr->getOperator() == no_thor)
        category = SIKthor;
    else
        category = SIKagent;
    hasDependencies = false;
}

void StatementInfo::calcDependencies()
{
    gatherDependencies(expr, dependencies, GatherAll);
}


void groupThorGraphs(HqlExprArray & in)
{
    //Gather information about the statements...
    bool hadThor = false;
    bool lastWasThor = false;
    bool couldImprove = false;
    CIArrayOf<StatementInfo> stmts;
    ForEachItemIn(idx, in)
    {
        StatementInfo & cur = *new StatementInfo(&in.item(idx));
        stmts.append(cur);
        if (cur.isThorQuery())
        {
            if (hadThor && !lastWasThor)
                couldImprove = true;
            hadThor = true;
            lastWasThor = true;
        }
        else
            lastWasThor = false;
    }

    //If no thor queries are split by other queries, then may as well keep in the same order...
    if (!couldImprove)
        return;

    //Need to work out the best order to generate the statements in.  We want
    //to move non thor queries to the front, so we do a insertion sort on them
    CICopyArrayOf<StatementInfo> sorted;
    ForEachItemIn(idx1, stmts)
    {
        StatementInfo & cur = stmts.item(idx1);
        bool curIsThor = cur.isThorQuery();

        unsigned insertPos = sorted.ordinality();
        ForEachItemInRev(idx2, sorted)
        {
            StatementInfo & compare = sorted.item(idx2);
            if (compare.isThorQuery() == curIsThor)
            {
                insertPos = idx2+1;
                break;
            }
            if (!compare.canSwapOrder(cur))
                break;
        }
        sorted.add(cur, insertPos);
    }

    //Finally see if there is any merit in moving an initial block of thor queries down to
    //merge with a subsequent one.
    StatementInfo & first = sorted.item(0);
    if (first.isThorQuery())
    {
        unsigned max = sorted.ordinality();
        unsigned numToMove;
        for (numToMove = 1; numToMove < max; numToMove++)
        {
            if (!(sorted.item(numToMove)).isThorQuery())
                break;
        }

        for (unsigned i=numToMove; i < max; i++)
        {
            StatementInfo & compare = sorted.item(i);
            if (compare.isThorQuery())
            {
                for (unsigned j=0; j < numToMove; j++)
                    sorted.rotateL(0, i-1);
                break;
            }
            for (unsigned j=0; j < numToMove; j++)
            {
                if (!compare.canSwapOrder(sorted.item(j)))
                {
                    i = max - 1;
                    break;
                }
            }
        }
    }

    in.kill();
    ForEachItemIn(idxSorted, sorted)
    {
        StatementInfo & cur = sorted.item(idxSorted);
        in.append(*cur.expr.getLink());
    }
}


//------------------------------------------------------------------------

//We will generate better code if conditional statements precede unconditional statements because globals can
//be commoned up better.
bool moveUnconditionalEarlier(HqlExprArray & in)
{
    //Gather information about the statements...
    unsigned numConditionals = 0;
    unsigned firstConditional = NotFound;
    bool couldImprove = false;
    CIArrayOf<StatementInfo> stmts;
    ForEachItemIn(idx, in)
    {
        StatementInfo & cur = *new StatementInfo(&in.item(idx));
        stmts.append(cur);
        if (cur.isConditional())
        {
            if (numConditionals == 0)
                firstConditional = idx;
            numConditionals++;
        }
        else if (numConditionals)
            couldImprove = true;
    }

    //If no unconditionals follow a conditional, and no conditionals to be combined, then keep in the same order...
    if (!couldImprove && numConditionals <= 1)
        return false;

    //For each block of unconditional statements which follow a conditional statement, see if they can be moved over the conditional statements.
    //(copies with no overhead if couldImprove is false)
    CICopyArrayOf<StatementInfo> sorted;
    unsigned max = stmts.ordinality();
    for (unsigned idx1 = 0; idx1 < max;)
    {
        StatementInfo & cur = stmts.item(idx1);
        bool isConditional = cur.isConditional();
        unsigned cnt = 1;
        if (isConditional || idx1 < firstConditional)
        {
            sorted.append(cur);
        }
        else
        {
            //calculate the number of contiguous unconditional statements
            for (cnt=1; idx1+cnt < max; cnt++)
            {
                if (stmts.item(idx1+cnt).isConditional())
                    break;
            }

            unsigned movePosition = 0;
            for (unsigned iBlock = 0; iBlock < cnt; iBlock++)
            {
                StatementInfo & curBlock = stmts.item(idx1+iBlock);
                unsigned bestPosition = NotFound;           // best position to add block.
                unsigned prev = idx1;
                while (prev-- > firstConditional)
                {
                    StatementInfo & compare = sorted.item(prev);
                    if (!compare.canSwapOrder(curBlock))
                        break;
                    if (prev == firstConditional)
                        bestPosition = prev;
                    else if (compare.isConditional() && !sorted.item(prev-1).isConditional())
                        bestPosition = prev;
                }
                if (bestPosition == NotFound)
                {
                    //can't move this element in the block => append the items to the list.
                    movePosition = sorted.ordinality();
                    break;
                }
                //Intersection of the best positions to provide earliest we can move the block
                if (movePosition < bestPosition)
                    movePosition = bestPosition;
            }

            for (unsigned iBlock2 = 0; iBlock2 < cnt; iBlock2++)
                sorted.add(stmts.item(idx1+iBlock2), movePosition+iBlock2);
        }
        idx1 += cnt;
    }

    //See if moving conditional statements could make some conditions next to each other

    //Now see if any of the conditional statements can be combined.


    //Finally replace the array
    in.kill();
    ForEachItemIn(idxSorted, sorted)
    {
        StatementInfo & cur = (StatementInfo &)sorted.item(idxSorted);
        in.append(*cur.expr.getLink());
    }
    return true;
}

//------------------------------------------------------------------------

static void mergeThorGraphs(HqlExprArray & exprs, bool resourceConditionalActions, bool resourceSequential);

static IHqlExpression * mergeThorGraphs(IHqlExpression * expr, bool resourceConditionalActions, bool resourceSequential)
{
    HqlExprArray args;
    expr->unwindList(args, no_actionlist);
    mergeThorGraphs(args, resourceConditionalActions, resourceSequential);
    return createActionList(args);
}


static void mergeThorGraphs(HqlExprArray & exprs, bool resourceConditionalActions, bool resourceSequential)
{
    HqlExprArray thorActions;
    HqlExprArray combined;
    ForEachItemIn(idx, exprs)
    {
        IHqlExpression * original = &exprs.item(idx);
        LinkedHqlExpr cur = original;

        const node_operator op = cur->getOperator();
        switch (op)
        {
        case no_compound:
            {
                OwnedHqlExpr replace = mergeThorGraphs(cur->queryChild(0), resourceConditionalActions, resourceSequential);
                cur.setown(replaceChild(cur, 0, replace));
                break;
            }
        case no_if:
            if (cur->isAction())
            {
                IHqlExpression * left = cur->queryChild(1);
                IHqlExpression * right = cur->queryChild(2);
                OwnedHqlExpr newLeft = mergeThorGraphs(left, resourceConditionalActions, resourceSequential);
                OwnedHqlExpr newRight = right ? mergeThorGraphs(right, resourceConditionalActions, resourceSequential) : NULL;
                if (left != newLeft || right != newRight)
                {
                    HqlExprArray args;
                    unwindChildren(args, cur);

                    //Not sure about this - the test condition may not be evaluatable inside thor
                    if (resourceConditionalActions && ((newLeft->getOperator() == no_thor) && (!newRight || newRight->getOperator() == no_thor)))
                    {
                        args.replace(*LINK(newLeft->queryChild(0)), 1);
                        if (newRight)
                            args.replace(*LINK(newRight->queryChild(0)), 2);
                        cur.setown(createValue(no_thor, makeVoidType(), cur->clone(args)));
                    }
                    else
                    {
                        args.replace(*LINK(newLeft), 1);
                        if (newRight)
                            args.replace(*LINK(newRight), 2);
                        cur.setown(cur->clone(args));
                    }
                }
            }
            break;
        case no_parallel:
            if (false)
            {
                HqlExprArray args;
                bool allThor = true;
                ForEachChild(i, cur)
                {
                    IHqlExpression * merged = mergeThorGraphs(cur->queryChild(i), resourceConditionalActions, resourceSequential);
                    args.append(*merged);
                    if (merged->getOperator() != no_thor)
                        allThor = false;
                }

                if (allThor)
                {
                    ForEachItemIn(i, args)
                        args.replace(*LINK(args.item(i).queryChild(0)), i);
                    cur.setown(cur->clone(args));
                    cur.setown(createValue(no_thor, makeVoidType(), cur.getClear()));
                }
                else
                    cur.setown(cur->clone(args));
                break;
            }
            //fall through
        case no_actionlist:
        case no_orderedactionlist:
            {
                HqlExprArray args;
                cur->unwindList(args, op);
                mergeThorGraphs(args, resourceConditionalActions, resourceSequential);
                cur.setown(cur->clone(args));
                break;
            }
        case no_sequential:
            {
                HqlExprArray args;
                bool allThor = true;
                ForEachChild(i, cur)
                {
                    IHqlExpression * merged = mergeThorGraphs(cur->queryChild(i), resourceConditionalActions, resourceSequential);
                    args.append(*merged);
                    if (merged->getOperator() != no_thor)
                        allThor = false;
                }

                if (resourceSequential && allThor)
                {
                    ForEachItemIn(i, args)
                        args.replace(*LINK(args.item(i).queryChild(0)), i);
                    cur.setown(cur->clone(args));
                    cur.setown(createValue(no_thor, makeVoidType(), cur.getClear()));
                }
                else
                    cur.setown(cur->clone(args));
                break;
            }
        case no_ensureresult:
            {
                HqlExprArray args;
                unwindChildren(args, cur);
                args.replace(*mergeThorGraphs(cur->queryChild(0), resourceConditionalActions, resourceSequential), 0);
                cur.setown(cloneOrLink(cur, args));
                break;
            }

        }

        if (cur->getOperator() == no_thor)
        {
            thorActions.append(*LINK(cur->queryChild(0)));
        }
        else
        {
            if (thorActions.ordinality())
            {
                combined.append(*createValue(no_thor, makeVoidType(), createActionList(thorActions)));
                thorActions.kill();
            }
            combined.append(*cur.getClear());
        }
    }
    if (thorActions.ordinality())
        combined.append(*createValue(no_thor, makeVoidType(), createActionList(thorActions)));
    replaceArray(exprs, combined);
}

void mergeThorGraphs(WorkflowItem & workflow, bool resourceConditionalActions, bool resourceSequential)
{
    groupThorGraphs(workflow.queryExprs());

    mergeThorGraphs(workflow.queryExprs(), resourceConditionalActions, resourceSequential);
}

//------------------------------------------------------------------------
inline bool isTypeToHoist(ITypeInfo * type)
{
    return isSingleValuedType(type);// || (type && type->getTypeCode() == type_set);
}

//Currently we do not create globals for common sub expressions between global expressions
//I suspect a better solution is to create them much later in the resourcing, but keep the
//code for the moment for reference.
static const bool createGlobalForGlobalCse = false;

static HqlTransformerInfo scalarGlobalTransformerInfo("ScalarGlobalTransformer");
ScalarGlobalTransformer::ScalarGlobalTransformer(HqlCppTranslator & _translator)
: HoistingHqlTransformer(scalarGlobalTransformerInfo, CTFtraverseallnodes), translator(_translator)
{
    isGlobal = true;
    isOkToHoist = true;
}

bool ScalarGlobalTransformer::isCandidate(IHqlExpression * expr)
{
    //  Commented line has problems with SELF used in HOLE definition, and explosion in thumphrey7 etc.
    //return !expr->isConstant() && !isContextDependent(expr) && expr->isPure() && expr->isIndependentOfScope();
    if (!containsAnyDataset(expr) && !expr->isConstant() && !isContextDependent(expr) && expr->isPure() && expr->isIndependentOfScope())
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_nofold:
            //try to hoist the thing that is nofolded instead
            return false;
        default:
            return true;
        }
    }
    return false;
}

bool ScalarGlobalTransformer::canCreateCandidate(IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    if (isTypeToHoist(type))
    {
        if (canCreateTemporary(expr))
            return true;
    }
    return false;
}

void ScalarGlobalTransformer::analyseExpr(IHqlExpression * expr)
{
    ScalarGlobalExtra * extra = queryBodyExtra(expr);

    bool wasGlobal = isGlobal;
    bool wasOkToHoist = isOkToHoist;
    analyseThis(expr);

    if (++extra->numUses > 1)
    {
        //Already creating an alias => no need to look any further
        if (extra->createGlobal)
            return;

        //If we have already visited this node globally then any elements that are candidates will already be tagged
        if (extra->visitedAllowHoist)
            return;

        if (createGlobalForGlobalCse)
        {
            //Option1: no_globalscope is added for cses between no_globalscopes.
            extra->visitedAllowHoist = true;

            //Otherwise it may be worth creating a new candidate for an expression used from two places
            isOkToHoist = true;
        }
        else
        {
            //Option2: Don't create a common node for cses between no_globalscopes
            if (!isGlobal)
                return;
            extra->visitedAllowHoist = true;
        }
    }
    else
        extra->visitedAllowHoist = isOkToHoist;

    if (isGlobal)
        extra->isLocal = false;

    doAnalyseExpr(expr);
    isGlobal = wasGlobal;
    isOkToHoist = wasOkToHoist;
}

void ScalarGlobalTransformer::doAnalyseExpr(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_attr:
    case no_constant:
    case no_attr_link:
    case no_null:
    case no_all:
    case no_funcdef:
        return;
    case no_persist_check:
        //No point spotting global within this since it will not create a subquery..
        return;
    case no_attr_expr:
        {
            IAtom * name = expr->queryName();
            if ((name == _selectors_Atom) || (name == keyedAtom))
                return;
            analyseChildren(expr);
            return;
        }
    case no_getresult:
    case no_libraryinput:
    case no_setresult:
        queryBodyExtra(expr)->alreadyGlobal = true;
        break;
    case no_globalscope:
        queryBodyExtra(expr)->alreadyGlobal = true;                     // don't tag again - even if opt flag is present
        queryBodyExtra(expr->queryChild(0))->alreadyGlobal = true;
        break;
    case no_ensureresult:
        //When transformed, transform tends to be called on the children only
        //=> Stop the argument being turned unnecessarily to a global temporary
        queryBodyExtra(expr)->alreadyGlobal = true;
        queryBodyExtra(expr->queryChild(0))->alreadyGlobal = true;
        break;
    }

    if (isOkToHoist && isCandidate(expr))
    {
        if (canCreateCandidate(expr))
        {
            queryBodyExtra(expr)->createGlobal = true;
            isGlobal = false;
            isOkToHoist = false;
        }
    }

    HoistingHqlTransformer::doAnalyseExpr(expr);
}

/*
Try and decide what is trivial enough to serialise, and what should remain.  It is more trial an error than particularly logical

o Better to store smaller objects because they will serialize smaller.
o If something is used more than once then probably worth serializing regardless - since calculation will be commoned up.
o Don't really want to serialize 'x' and 'x <> ''' to the same function - but much better to serialize 'x <> ''' rather than 'x' if only one used.

*/
bool ScalarGlobalTransformer::isComplex(IHqlExpression * expr, bool checkGlobal)
{
    ScalarGlobalExtra * extra = queryBodyExtra(expr);
    if (checkGlobal)
    {
        //If something else has turned this into a global then no point.
        if (extra->alreadyGlobal || extra->createGlobal)
            return false;
    }

    switch (expr->getOperator())
    {
    case no_workunit_dataset:
    case no_getresult:
        return expr->hasAttribute(wuidAtom);
    case no_constant:
    case no_globalscope:
    case no_libraryinput:
        return false;
    case no_cast:
    case no_implicitcast:
        //serialize if the cast reduces the size of the item, otherwise check argument.
        if (expr->queryType()->getSize() <= expr->queryChild(0)->queryType()->getSize())
            return true;
        //If used a lot then save lots of duplicated work.
        if (extra->numUses > 2)
            return true;
        break;
    case no_eq:
    case no_ne:
    case no_lt:
    case no_gt:
    case no_le:
    case no_ge:
        //Accessed more than once-> probably worth commoning up
        if (extra->numUses > 1)
            return true;

        //Some possible improvements.  Probably better to implement these and all hoisting in the resourcing phase.
        //MORE: Probably more efficient to calculate a boolean and serialize just that - if it is used in a dataset context
        //MORE: If an argument is a string probably worth commoning up - it depends on whether the string is also used in
        //      the same context.
        break;
        //f[1..length(trim(x))] = x is very common, and if the length(trim)) was serialized separately then
        //the generated code would be worse.
    case no_trim:
    case no_charlen:
    case no_sorted:
    case no_not:
        break;
    case no_substring:
        //single character substring - don't create separate items just for this, since likely to have many of them.
        if (!expr->queryChild(1)->queryValue())
            return true;
        break;
    default:
        if (expr->isConstant())
            return false;
        return true;
    }

    ForEachChild(i, expr)
    {
        if (isComplex(expr->queryChild(i), true))
            return true;
    }
    return false;
}

IHqlExpression * ScalarGlobalTransformer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    OwnedHqlExpr transformed = HoistingHqlTransformer::createTransformed(expr);

    switch (transformed->getOperator())
    {
    case no_setresult:
    case no_globalscope:
    case no_ensureresult:
        {
        //Don't prevent no_globalscope being added in analyse because other expression may require
        //it to be made global.  However if a no_globalscope has been added to the child, remove it.  
        IHqlExpression * child = transformed->queryChild(0);
        if (child->getOperator() == no_globalscope)
        {
            if (expr->queryChild(0)->getOperator() != no_globalscope)
            {
                HqlExprArray args;
                args.append(*LINK(child->queryChild(0)));
                unwindChildren(args, transformed, 1);
                return transformed->clone(args);
            }
        }
        break;
        }
    }

    ScalarGlobalExtra * extra = queryBodyExtra(expr);
    if (extra->createGlobal)
    {
        if (!extra->alreadyGlobal && isComplex(expr, false))
        {
#ifdef _DEBUG
        translator.traceExpression("Mark as global", expr);
#endif
            //mark as global, so isComplex() can take it into account.
            extra->alreadyGlobal = true;
            if (expr->getOperator() == no_createset)
                transformed.setown(projectCreateSetDataset(transformed));
            return createValue(no_globalscope, transformed->getType(), LINK(transformed));
        }
        else
            extra->createGlobal = false;
    }
    return transformed.getClear();
}

//------------------------------------------------------------------------

static HqlTransformerInfo explicitGlobalTransformerInfo("ExplicitGlobalTransformer");
ExplicitGlobalTransformer::ExplicitGlobalTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator)
: HoistingHqlTransformer(explicitGlobalTransformerInfo, CTFnoteifactions|CTFtraverseallnodes), translator(_translator)
{
    wu = _wu;
    isRoxie = (translator.getTargetClusterType() == RoxieCluster);
    seenGlobalScope = false;
    seenLocalGlobalScope = false;
}

void ExplicitGlobalTransformer::doAnalyseExpr(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_nothor:
        if (expr->isAction())
            break;
        //fall through
    case no_globalscope:
        //Try and avoid transforms (especially nested ones) as much as possible.
        seenGlobalScope = true;
        //If local attribute is present on a global, then an independent transform may cause an extra
        //transformation because it may become unconditional, when previously conditional
        if (expr->hasAttribute(localAtom))
            seenLocalGlobalScope = true;
        break;
    }
    HoistingHqlTransformer::doAnalyseExpr(expr);
}

IHqlExpression * ExplicitGlobalTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
        return LINK(expr);

    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    OwnedHqlExpr transformed = HoistingHqlTransformer::createTransformed(expr);
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_nothor:
        if (transformed->isAction())
            break;
        //fall through
    case no_globalscope:
        {
            IHqlExpression * value = transformed->queryChild(0);
            if (expr->hasAttribute(optAtom))
            {
                if (!isIndependentOfScope(value))
                    return LINK(value);
            }

            if (!expr->hasAttribute(localAtom) || isUsedUnconditionally(expr))
            {
                if (!isIndependentOfScope(value))
                {
                    IHqlExpression * symbol = queryActiveSymbol();
                    StringBuffer s;
                    if (symbol && symbol->queryBody() == expr)
                        s.appendf(" '%s'", str(symbol->queryName()));
                    else
                    {
                        s.append(" ").append(getOpString(value->getOperator()));
                        if (symbol)
                            s.append(" in ").append(symbol->queryName());
                    }
                    if (op == no_nothor)
                        translator.reportWarning(CategoryMistake, SeverityUnknown, queryActiveLocation(expr), ECODETEXT(HQLWRN_NoThorContextDependent), s.str());
                    else
                        translator.reportWarning(CategoryMistake, SeverityUnknown, queryActiveLocation(expr), ECODETEXT(HQLWRN_GlobalDoesntSeemToBe), s.str());
                }
                if (value->getOperator() == no_createset)
                {
                    OwnedHqlExpr createset = projectCreateSetDataset(value);
                    IHqlExpression * ds = createset->queryChild(0);

                    HqlExprArray outArgs, setArgs;
                    outArgs.append(*LINK(ds));
                    outArgs.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
                    outArgs.append(*createAttribute(namedAtom, createNextStringValue(value)));

                    IHqlExpression * setResult = createValue(no_output, makeVoidType(), outArgs);
                    appendToTarget(*setResult);
                    transformed.setown(createGetResultFromSetResult(setResult, expr->queryType()));
                }
                else
                {
                    GlobalAttributeInfo info("jobtemp::global","gl", transformed);
                    if (op == no_nothor)
                        info.extractGlobal(NULL, RoxieCluster);
                    else
                        info.extractGlobal(transformed, translator.getTargetClusterType());
                    OwnedHqlExpr getResult, setResult;
                    info.checkFew(translator);
                    info.splitGlobalDefinition(transformed->queryType(), value, wu, setResult, &getResult, isRoxie);
                    if (op == no_nothor)
                        setResult.setown(createValue(no_nothor, makeVoidType(), LINK(setResult)));
                    IHqlExpression * cluster = queryRealChild(transformed, 1);
                    if (cluster && !isBlankString(cluster))
                        setResult.setown(createValue(no_cluster, makeVoidType(), LINK(setResult), LINK(cluster)));
                    appendToTarget(*setResult.getClear());
                    transformed.setown(getResult.getClear());
                }
                break;
            }
        }
    }
    return transformed.getClear();
}

static HqlTransformerInfo optGlobalTransformerInfo("OptGlobalTransformer");
OptGlobalTransformer::OptGlobalTransformer() : NewHqlTransformer(optGlobalTransformerInfo)
{
    seenOptGlobal = false;
}

void OptGlobalTransformer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_globalscope:
        if (expr->hasAttribute(optAtom))
            seenOptGlobal = true;
        break;
    }
    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * OptGlobalTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    node_operator op = transformed->getOperator();
    switch (op)
    {
    case no_globalscope:
        {
            if (transformed->hasAttribute(optAtom))
            {
                IHqlExpression * value = transformed->queryChild(0);
                if (!isIndependentOfScope(value))
                    return LINK(value);
                return removeAttribute(transformed, optAtom);
            }
            break;
        }
    }
    return transformed.getClear();
}

//------------------------------------------------------------------------

static HqlTransformerInfo scopeIndependentActionCheckerInfo("ScopeIndependentActionChecker");
class ScopeIndependentActionChecker : public NewHqlTransformer
{
public:
    ScopeIndependentActionChecker(HqlCppTranslator & _translator) : NewHqlTransformer(scopeIndependentActionCheckerInfo), translator(_translator)
    {
    }

protected:
    void analyseExpr(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_parallel:
        case no_if:
        case no_sequential:
        case no_compound:
        case no_actionlist:
        case no_orderedactionlist:
            NewHqlTransformer::analyseExpr(expr);
            break;
        case no_output:
            if (!isIndependentOfScope(expr))
            {
                IHqlExpression * filename = queryRealChild(expr, 2);
                if (!filename)
                    filename = expr->queryAttribute(namedAtom);

                StringBuffer s;
                if (filename)
                    getExprECL(filename, s);
                translator.WARNINGAT1(CategoryMistake, queryActiveLocation(expr), HQLWRN_OutputDependendOnScope, s.str());

    #if 0
                checkIndependentOfScope(expr);
    #endif
            }
            break;
        default:
            if (!isIndependentOfScope(expr))
            {
                //There appears to be partial support for functions generated as workflow items - which are not
                //scope independent since they depend on their paramaters => don't complain about them
                if (expr->getOperator() != no_return_stmt)
                    translator.WARNINGAT(CategoryMistake, queryActiveLocation(expr), HQLWRN_GlobalActionDependendOnScope);

    #if 0
                checkIndependentOfScope(expr);
    #endif
            }
            break;
        }
    }

private:
    HqlCppTranslator & translator;
};

void checkGlobalActionsIndependentOfScope(HqlCppTranslator & translator, const HqlExprArray & exprs)
{
    ScopeIndependentActionChecker checker(translator);
    checker.analyseArray(exprs, 0);
}


//------------------------------------------------------------------------


IHqlDataset * queryRootDataset(IHqlExpression * dataset)
{
    return dataset->queryDataset()->queryRootTable();
}

//roxie only executes outputs to temporaries if they are required, or if not all references are from within the graph
//therefore, there is no need to special case if actions.  Thor on the other hand will cause it to be executed unnecessarily.
static HqlTransformerInfo newScopeMigrateTransformerInfo("NewScopeMigrateTransformer");
NewScopeMigrateTransformer::NewScopeMigrateTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator)
: HoistingHqlTransformer(newScopeMigrateTransformerInfo, 0), translator(_translator)
{
    wu = _wu;
    isRoxie = translator.targetRoxie();
    if (!isRoxie && !_translator.queryOptions().resourceConditionalActions)
        setFlags(CTFnoteifactions);
    minimizeWorkunitTemporaries = translator.queryOptions().minimizeWorkunitTemporaries;
#ifdef REMOVE_GLOBAL_ANNOTATION
    activityDepth = 0;      // should be 0 to actually have any effect - but causes problems...
#else
    activityDepth = 999;        // should be 0 to actually have any effect - but causes problems...
#endif
}

void NewScopeMigrateTransformer::analyseExpr(IHqlExpression * expr)
{
    ScopeMigrateInfo * extra = queryBodyExtra(expr);
    if (activityDepth > extra->maxActivityDepth)
    {
        if (extra->maxActivityDepth == 0)
            extra->setUnvisited();              // so we walk children again
        extra->maxActivityDepth = activityDepth;
    }

    unsigned savedActivityDepth = activityDepth;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case NO_AGGREGATE:
    case no_createset:
    case NO_ACTION_REQUIRES_GRAPH:
    case no_extractresult:
    case no_distributer:
    case no_within:
    case no_notwithin:
    case no_soapaction_ds:
    case no_returnresult:
        activityDepth++;
        break;
    case no_setresult:
        if (expr->queryChild(0)->isDataset())
            activityDepth++;
        break;
    case no_select:
        if (expr->hasAttribute(newAtom))
            activityDepth++;
        break;
    }

    HoistingHqlTransformer::analyseExpr(expr);
    activityDepth = savedActivityDepth;
}

IHqlExpression * NewScopeMigrateTransformer::hoist(IHqlExpression * expr, IHqlExpression * hoisted)
{
    if (minimizeWorkunitTemporaries)
        return createWrapper(no_globalscope, LINK(hoisted));
    IHqlExpression * setResult = createSetResult(hoisted);
    appendToTarget(*setResult);

    return createGetResultFromSetResult(setResult);
}


IHqlExpression * NewScopeMigrateTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
        return LINK(expr);

    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    OwnedHqlExpr transformed = HoistingHqlTransformer::createTransformed(expr);

    ScopeMigrateInfo * extra = queryBodyExtra(expr);
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_createset:
        {
            if (isUsedUnconditionally(expr))
            {
                if (isIndependentOfScope(transformed) && !isContextDependent(expr))
                {
                    OwnedHqlExpr createset = projectCreateSetDataset(transformed);

                    if (minimizeWorkunitTemporaries)
                        return createWrapper(no_globalscope, LINK(createset));

                    //MORE: This is only temporary until child datasets come into existence, then it will need improving
                    //Save it as a temporary dataset in the wu, and retrieve it as a getresult(set)
                    IHqlExpression * ds = createset->queryChild(0);

                    HqlExprArray outArgs, setArgs;
                    outArgs.append(*LINK(ds));
                    outArgs.append(*createAttribute(sequenceAtom, getLocalSequenceNumber()));
                    outArgs.append(*createAttribute(namedAtom, createNextStringValue(expr)));

                    IHqlExpression * setResult = createValue(no_output, makeVoidType(), outArgs);
                    appendToTarget(*setResult);
                    transformed.setown(createGetResultFromSetResult(setResult, expr->queryType()));
                }
            }
            break;
        }
    case no_select:
        {
            bool isNew;
            IHqlExpression * row = querySelectorDataset(transformed, isNew);
            if (isNew)
            {
                if (isUsedUnconditionally(expr))
                {
                    if (extra->maxActivityDepth != 0)
                    {
                        node_operator rowOp = row->getOperator();
                        if (rowOp == no_selectnth)
                        {
                            node_operator dsOp = row->queryChild(0)->getOperator();
                            if ((dsOp == no_workunit_dataset) || (dsOp == no_inlinetable))
                                break;
                        }
                        if (rowOp == no_createrow || rowOp == no_getresult)
                            break;
                        if (!isInlineTrivialDataset(row) && !isContextDependent(row) && !transformed->isDataset() && !transformed->isDictionary())
                        {
                            if (isIndependentOfScope(row))
                                return hoist(expr, transformed);
                        }
                    }
                }
            }
        }
        break;
    case NO_AGGREGATE:
        {
            if (isUsedUnconditionally(expr))
            {
                if (extra->maxActivityDepth != 0)
                {
                    IHqlExpression * datasetExpr = transformed->queryChild(0);
                    IHqlDataset * rootDataset = queryRootDataset(datasetExpr);
                    if (!rootDataset)
                    {
                        //Something like a+b+c
                        rootDataset = datasetExpr->queryDataset()->queryTable();
                        if (!rootDataset)
                            break;
                    }

                    //Don't do anything with child datasets....
                    IHqlExpression * rootDatasetExpr = queryExpression(rootDataset);
                    node_operator rootOp = rootDatasetExpr->getOperator();
                    if ((rootOp == no_select) || (rootOp == no_field))
                        break;

                    if (isIndependentOfScope(datasetExpr) && !isContextDependent(expr))
                    {
                        return hoist(expr, transformed);
                    }
                }
            }
            break;
        }
    case no_globalscope:
        if (isRedundantGlobalScope(transformed))
            return LINK(transformed->queryChild(0));
        break;
    }
    return transformed.getClear();
}



void migrateExprToNaturalLevel(WorkflowItem & cur, IWorkUnit * wu, HqlCppTranslator & translator)
{
    const HqlCppOptions & options = translator.queryOptions();
    HqlExprArray & exprs = cur.queryExprs();
    if (translator.queryOptions().moveUnconditionalActions)
        moveUnconditionalEarlier(exprs);
    translator.checkNormalized(exprs);

    if (options.hoistSimpleGlobal)
    {
        ScalarGlobalTransformer transformer(translator);
        HqlExprArray results;
        transformer.analyseArray(exprs, 0);
        transformer.transformRoot(exprs, results);
        replaceArray(exprs, results);
        translator.checkNormalized(exprs);
    }

    translator.traceExpressions("m0", exprs);

    checkGlobalActionsIndependentOfScope(translator, exprs);

    if (options.workunitTemporaries)
    {
        ExplicitGlobalTransformer transformer(wu, translator);

        transformer.analyseArray(exprs, 0);
        if (transformer.needToTransform())
        {
            HqlExprArray results;
            transformer.transformRoot(exprs, results);
            replaceArray(exprs, results);
        }
    }
    else
    {
        OptGlobalTransformer transformer;

        transformer.analyseArray(exprs, 0);
        if (transformer.needToTransform())
        {
            HqlExprArray results;
            transformer.transformRoot(exprs, results);
            replaceArray(exprs, results);
        }
    }

    translator.checkNormalized(exprs);

    translator.traceExpressions("m1", exprs);

    if (options.allowScopeMigrate) // && !options.minimizeWorkunitTemporaries)
    {
        NewScopeMigrateTransformer transformer(wu, translator);
        HqlExprArray results;

        transformer.analyseArray(exprs, 0);
        transformer.transformRoot(exprs, results);
        replaceArray(exprs, results);
        translator.checkNormalized(exprs);
    }

    translator.traceExpressions("m2", exprs);
}

void expandGlobalDatasets(WorkflowArray & array, IWorkUnit * wu, HqlCppTranslator & translator)
{
}

//---------------------------------------------------------------------------

bool AutoScopeMigrateInfo::addGraph(unsigned graph)
{
    if (graph == lastGraph)
        return false;
    if (lastGraph)
        manyGraphs = true;
    lastGraph = graph;
    return true;
}

bool AutoScopeMigrateInfo::doAutoHoist(IHqlExpression * transformed, bool minimizeWorkunitTemporaries)
{
    if (useCount == 0)
        return false;

    node_operator op = original->getOperator();
    switch (op)
    {
    case no_fail:
        return false;
    }

    if (firstUseIsConditional && firstUseIsSequential)
        return false;

    if (firstUseIsSequential && !manyGraphs)
        return false;

//  The following *should* generate better code, but there are currently a couple of exceptions (cmaroney29, jholt20) which need investigation
//  if (!manyGraphs)
//      return false;

    if (globalInsideChild && !minimizeWorkunitTemporaries)// && !transformed->isDataset() && !transformed->isDatarow())
        return true;

    if (!manyGraphs)
        return false;

    if (!original->isDataset())
    {
        switch (op)
        {
        case NO_AGGREGATE:
            break;
        default:
            return false;
        }
    }

    if (!isWorthHoisting(transformed, false))
        return false;

    if (isContextDependent(transformed))
        return false;

    return isIndependentOfScope(original);
}

static HqlTransformerInfo autoScopeMigrateTransformerInfo("AutoScopeMigrateTransformer");
AutoScopeMigrateTransformer::AutoScopeMigrateTransformer(IWorkUnit * _wu, HqlCppTranslator & _translator)
: NewHqlTransformer(autoScopeMigrateTransformerInfo), translator(_translator)
{
    wu = _wu;
    isRoxie = (translator.getTargetClusterType() == RoxieCluster);
    isConditional = false;
    isSequential = false;
    hasCandidate = false;
    activityDepth = 0;
    curGraph = 1;
    globalTarget = NULL;
}

void AutoScopeMigrateTransformer::analyseExpr(IHqlExpression * expr)
{
    AutoScopeMigrateInfo * extra = queryBodyExtra(expr);
    if (isConditional)
        extra->condUseCount++;
    else
        extra->useCount++;

    bool newGraph = extra->addGraph(curGraph);
    if (!newGraph)
        return;

    if (extra->doAutoHoist(expr, translator.queryOptions().minimizeWorkunitTemporaries))
    {
        hasCandidate = true;
        return;
    }

    unsigned savedDepth = activityDepth;
    doAnalyseExpr(expr);
    activityDepth = savedDepth;
}

void AutoScopeMigrateTransformer::doAnalyseExpr(IHqlExpression * expr)
{
    AutoScopeMigrateInfo * extra = queryBodyExtra(expr);
    if (activityDepth && expr->isDataset())
    {
        if (isWorthHoisting(expr, true) && isIndependentOfScope(expr) && !isContextDependent(expr))
        {
#ifdef _DEBUG
            isWorthHoisting(expr, true);
#endif
            extra->globalInsideChild = true;
            hasCandidate = true;
            activityDepth = 0;
        }
    }

    extra->firstUseIsConditional = isConditional;
    extra->firstUseIsSequential = isSequential;
    switch (expr->getOperator())
    {
    case no_allnodes:
    case no_keyedlimit:
    case no_nothor:
    case no_sizeof:
        return;
    case no_sequential:
        return;
    case no_if:
    case no_choose:
        {
            if (expr->isAction())
            {
                bool wasConditional = isConditional;
                analyseExpr(expr->queryChild(0));
                isConditional = true;
                ForEachChildFrom(i, expr, 1)
                    analyseExpr(expr->queryChild(i));
                isConditional = wasConditional;
                return;
            }
            break;
        }
    case no_newtransform:
    case no_transform:
        if (curGraph)
        {
            activityDepth++;
            NewHqlTransformer::analyseExpr(expr);
            activityDepth--;
            return;
        }
        break;
    case no_thor:
        //ignore thor attribute on a dataset..
        if (expr->queryType())
        {
            curGraph++;
            NewHqlTransformer::analyseExpr(expr);
            curGraph++;     // don't restore - new pseudo graph to aid cse between global branches separated by graphs
            return;
        }
        break;
    }

    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * AutoScopeMigrateTransformer::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_allnodes:
    case no_keyedlimit:
    case no_libraryscope:
    case no_nothor:
    case no_sequential:
        return LINK(expr);
    case no_thor:
        {
            IHqlExpression * actions = expr->queryChild(0);
            if (actions)
            {
                //MORE: Simplify this???? or remove the special case all together?
                //OwnedHqlExpr newActions = transform(actions);
                HqlExprArray args;
                unwindCommaCompound(args, actions);
                ForEachItemIn(i, args)
                    graphActions.append(*transform(&args.item(i)));
                OwnedHqlExpr newActions = createActionList(graphActions);
                graphActions.kill();
                if (actions == newActions)
                    return LINK(expr);
                return createWrapper(no_thor, newActions.getClear());
            }
            break;
        }
    }

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);

    AutoScopeMigrateInfo * extra = queryBodyExtra(expr);
    if (extra->doAutoHoist(transformed, translator.queryOptions().minimizeWorkunitTemporaries))
    {
        StringBuffer s;
        s.appendf("AutoGlobal: Spotted %s ", getOpString(expr->getOperator()));
        if (expr->queryName())
            s.append("[").append(expr->queryName()).append("] ");
        s.append("as an item to hoist");
        DBGLOG("%s", s.str());

        GlobalAttributeInfo info("jobtemp::auto","auto", transformed);
        info.extractGlobal(NULL, translator.getTargetClusterType());
        if (translator.targetThor() && extra->globalInsideChild)
            info.preventDiskSpill();
        OwnedHqlExpr getResult, setResult;
        info.checkFew(translator);
        info.splitGlobalDefinition(transformed->queryType(), transformed, wu, setResult, &getResult, isRoxie);

        //If the first use is conditional, then hoist the expression globally (it can't have any dependents)
        //else hoist it within the current graph, otherwise it can get hoisted before globals on datasets that
        //it is dependent on.
        if (extra->firstUseIsConditional)
            globalTarget->append(*createWrapper(no_thor, setResult.getClear()));
        else
            graphActions.append(*setResult.getClear());
        transformed.setown(getResult.getClear());
    }

    return transformed.getClear();
}


void AutoScopeMigrateTransformer::transformRoot(const HqlExprArray & in, HqlExprArray & out)
{
    globalTarget = &out;
    NewHqlTransformer::transformRoot(in, out);
    globalTarget = NULL;
}



//---------------------------------------------------------------------------

static HqlTransformerInfo trivialGraphRemoverInfo("TrivialGraphRemover");
TrivialGraphRemover::TrivialGraphRemover() : NewHqlTransformer(trivialGraphRemoverInfo)
{
    hasCandidate = false;
}

void TrivialGraphRemover::analyseExpr(IHqlExpression * expr)
{
    if (hasCandidate || alreadyVisited(expr))
        return;

    if (expr->getOperator() == no_thor)
    {
        if (isTrivialGraph(expr->queryChild(0)))
            hasCandidate = true;
        return;
    }

    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * TrivialGraphRemover::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_thor:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (child && isTrivialGraph(child))
                return LINK(child);
            return LINK(expr);
        }
    }
    return NewHqlTransformer::createTransformed(expr);
}


bool TrivialGraphRemover::isTrivialGraph(IHqlExpression * expr)
{
    if (!expr)
        return false;
    if (expr->getOperator() == no_setresult)
    {
        IHqlExpression * value = expr->queryChild(0);
        if (value->getOperator() != no_getresult)
            return false;
        return true;
    }
    else if (expr->getOperator() == no_output)
        return isTrivialInlineOutput(expr);
    else
        return false;

}

void removeTrivialGraphs(WorkflowItem & curWorkflow)
{
    HqlExprArray & exprs = curWorkflow.queryExprs();
    TrivialGraphRemover transformer;
    transformer.analyseArray(exprs, 0);
    if (transformer.worthTransforming())
    {
        HqlExprArray simplified;
        transformer.transformRoot(exprs, simplified);
        replaceArray(exprs, simplified);
    }
}

//==============================================================================================================

class FilterCloner
{
public:
    FilterCloner(IHqlExpression * _ds) { ds.set(_ds); matched = false; lockTransformMutex(); }
    ~FilterCloner() { unlockTransformMutex(); }

    void addMappings(IHqlExpression * expr, IHqlExpression * addDs);
    IHqlExpression * inheritFilters(IHqlExpression * expr);

    inline bool hasMappings() { return matched; }

protected:
    void doAddMappings(IHqlExpression * expr);
    bool isMatchingSelector(IHqlExpression * expr);
    void setMapping(IHqlExpression * selector, IHqlExpression * value);

protected:
    HqlExprAttr ds;
    bool matched;
};


void FilterCloner::setMapping(IHqlExpression * selector, IHqlExpression * value)
{
    selector->setTransformExtra(value);
    matched = true;
}

void FilterCloner::doAddMappings(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_and:
            doAddMappings(expr->queryChild(0));
            expr = expr->queryChild(1);
            continue;
        case no_in:
        case no_notin:
            {
                IHqlExpression * lhs = expr->queryChild(0);
                IHqlExpression * rhs = expr->queryChild(1);
                if (isMatchingSelector(lhs) && !containsActiveDataset(rhs))
                    setMapping(lhs, expr);
                break;
            }
        case no_between:
        case no_notbetween:
            {
                IHqlExpression * lhs = expr->queryChild(0);
                if (isMatchingSelector(lhs) && !containsActiveDataset(expr->queryChild(1)) && !containsActiveDataset(expr->queryChild(2)))
                    setMapping(lhs, expr);
                break;
            }
        case no_eq:
        case no_ne:
        case no_lt:
        case no_gt:
        case no_ge:
        case no_le:
            {
                IHqlExpression * lhs = expr->queryChild(0);
                IHqlExpression * rhs = expr->queryChild(1);
                if (isMatchingSelector(lhs) && !containsActiveDataset(rhs))
                    setMapping(lhs, expr);
                else if (isMatchingSelector(rhs) && !containsActiveDataset(lhs))
                    setMapping(rhs, expr);
                break;
            }
        }
        return;
    }
}

void FilterCloner::addMappings(IHqlExpression * expr, IHqlExpression * addDs)
{
    if (!expr) return;
    OwnedHqlExpr replaced = replaceSelector(expr, addDs, ds);
    doAddMappings(replaced);
}

bool FilterCloner::isMatchingSelector(IHqlExpression * expr)
{
    return queryDatasetCursor(expr) == ds;
}

IHqlExpression * FilterCloner::inheritFilters(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_and:
    case no_assertkeyed:
    case no_assertstepped:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
                args.append(*inheritFilters(expr->queryChild(i)));
            return cloneOrLink(expr, args);
        }
    case no_eq:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * rhs = expr->queryChild(1);
            IHqlExpression * lhsExtra = (IHqlExpression *)lhs->queryTransformExtra();
            if (lhsExtra)
            {
                DBGLOG("Inheriting filter condition");
                IHqlExpression * cond = replaceExpression(lhsExtra, lhs, rhs);
                return createValue(no_and, LINK(expr), cond);
            }
            IHqlExpression * rhsExtra = (IHqlExpression *)rhs->queryTransformExtra();
            if (rhsExtra)
            {
                DBGLOG("Inheriting filter condition");
                IHqlExpression * cond = replaceExpression(rhsExtra, rhs, lhs);
                return createValue(no_and, LINK(expr), cond);
            }
            break;
        }
    case no_in:
    case no_notin:
    case no_between:
    case no_notbetween:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * lhsExtra = (IHqlExpression *)lhs->queryTransformExtra();
            if (lhsExtra)
            {
                DBGLOG("Inheriting filter condition");
                return createValue(no_and, LINK(expr), LINK(lhsExtra));
            }
            break;
        }
    }
    return LINK(expr);
}

static IHqlExpression * optimizeJoinFilter(IHqlExpression * expr)
{
    //NB: Not a member function because we use a different transform mutex, and don't want to accidentally interfere with caller's use
    IHqlExpression * index = expr->queryChild(1);
    if (!index->hasAttribute(filteredAtom) && !index->hasAttribute(_filtered_Atom) && (index->getOperator() == no_newkeyindex))
        return LINK(expr);

    if (expr->hasAttribute(keyedAtom))
        return LINK(expr);      //MORE!

    OwnedHqlExpr rhs = createSelector(no_right, index, querySelSeq(expr));
    FilterCloner processor(rhs);

    while (index->getOperator() != no_newkeyindex)
    {
        switch (index->getOperator())
        {
        case no_filter:
        //case no_filtered:
            //MORE: This might be useful in the future
            UNIMPLEMENTED;
        }
        index = index->queryChild(0);
    }

    processor.addMappings(queryAttributeChild(index, filteredAtom, 0), queryActiveTableSelector());
    processor.addMappings(queryAttributeChild(index, _filtered_Atom, 0), queryActiveTableSelector());
    if (!processor.hasMappings())
        return LINK(expr);

    HqlExprArray exprs;
    expr->queryChild(2)->unwindList(exprs, no_and);

    bool keyedExplicitly = false;
    ForEachItemIn(i1, exprs)
    {
        IHqlExpression & cur = exprs.item(i1);
        switch (cur.getOperator())
        {
        case no_assertkeyed:
        case no_assertwild:
            keyedExplicitly = true;
            exprs.replace(*processor.inheritFilters(&cur), i1);
            break;
        }
    }

    if (!keyedExplicitly)
    {
        ForEachItemIn(i2, exprs)
        {
            IHqlExpression & cur = exprs.item(i2);
            switch (cur.getOperator())
            {
            case no_assertkeyed:
            case no_assertwild:
            case no_attr:
            case no_attr_link:
            case no_attr_expr:
                break;
            default:
                exprs.replace(*processor.inheritFilters(&cur), i2);
                break;
            }
        }
    }

    HqlExprArray args;
    unwindChildren(args, expr);
    args.replace(*createBalanced(no_and, queryBoolType(), exprs), 2);
    return expr->clone(args);
}


static HqlTransformerInfo filteredIndexOptimizerInfo("FilteredIndexOptimizer");
FilteredIndexOptimizer::FilteredIndexOptimizer(bool _processJoins, bool _processReads)
: NewHqlTransformer(filteredIndexOptimizerInfo)
{
    processJoins = _processJoins;
    processReads = _processReads;
}

IHqlExpression * FilteredIndexOptimizer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    if (processJoins && isKeyedJoin(transformed))
        transformed.setown(optimizeJoinFilter(transformed));

    if (processReads)
    {
        switch (transformed->getOperator())
        {
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
            //MORE:
            break;
        }
    }
    return transformed.getClear();
}


//==============================================================================================================


static HqlTransformerInfo localUploadTransformerInfo("LocalUploadTransformer");
LocalUploadTransformer::LocalUploadTransformer(IWorkUnit * _wu) : NewHqlTransformer(localUploadTransformerInfo)
{
    wu = _wu;
}


IHqlExpression * LocalUploadTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    if (transformed->hasAttribute(localUploadAtom))
    {
        assertex(transformed->getOperator() == no_table);
        IHqlExpression * filename = transformed->queryChild(0);
        IHqlExpression * mode = transformed->queryChild(2);
        assertex(filename->getOperator() == no_constant);

        StringBuffer sourceName,localName;
        filename->queryValue()->getStringValue(sourceName);
        getUniqueId(localName.append("local"));

        LocalFileUploadType uploadType = UploadTypeWUResult;
        switch (mode->getOperator())
        {
        case no_csv:
            uploadType = UploadTypeWUResultCsv;
            break;
        case no_xml:
            uploadType = UploadTypeWUResultXml;
            break;
        }

        wu->addLocalFileUpload(uploadType, sourceName, localName, NULL);

        HqlExprArray args;
        args.append(*LINK(expr->queryRecord()));
        args.append(*createAttribute(nameAtom, createConstant(localName.str())));
        args.append(*createAttribute(sequenceAtom, getStoredSequenceNumber()));
        return createDataset(no_workunit_dataset, args);
    }

    return transformed.getClear();
}


//==============================================================================================================

/*

  The following code converts expressions of the form a.b where a and b are datasets into a normalized form including
  an explicit normalize activity.  It follows the following rules:

  1) An filter/project/table on a.b logically has access to fields in a and a.b.
  2) An operation with a transform (or record defining the output) other than a PROJECT/TABLE, implicitly loses access to fields in a for all
     subsequent operations.
  3) When a dataset is used in an output or outer outer context (e.g., call parameter) then only the fields in the child dataset are passed.

  This is done by converting f(a.b) to normalize(a, f(LEFT.b)) at the appropriate place.

  These rules mean we maintain the HOLe semantics which allow computed fields to be implemented by projecting fields from the parent dataset,
  but also mean that we avoid problems with parent datasets e.g., join(a.b, c.d)

  To make this efficient we also need to implement the following in the code generator:
  1) aggregate-normalize(x).
  2) normalize-source.
  3) aggregate-normalize-source
  4) inline processing of normalize.

  After this transform there should be no existing datasets of the form <a.b>[new].  There is a new function to ensure this is correct.

  For the first version, it only generates normalizes around datasets - and assumes that parent fields don't need to be mapped into the denormalize.
  This makes it simpler, but means that parse statements that take a record which could theoretically access parent dataset
  fields won't work.  The fix would require a new kind of child normalize that took a no_newtransform, and would require
  analysis of which parent fields are used in the child's no_newtransform.
  */

inline void getDatasetRange(IHqlExpression * expr, unsigned & first, unsigned & max)
{
    first = 0;
    switch (getChildDatasetType(expr))
    {
    case childdataset_many_noscope:
    case childdataset_many:
        max = expr->numChildren();
        break;
    case childdataset_if:
        first = 1;
        max = expr->numChildren();
        break;
    default:
        max = getNumChildTables(expr);
        break;
    }
}

static HqlTransformerInfo nestedSelectorNormalizerInfo("NestedSelectorNormalizer");
NestedSelectorNormalizer::NestedSelectorNormalizer() : NewHqlTransformer(nestedSelectorNormalizerInfo)
{
    spottedCandidate = false;
}

void NestedSelectorNormalizer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;

    NewHqlTransformer::analyseExpr(expr);

    if (expr->isDataset())
    {
        bool childrenAreDenormalized = false;
        unsigned first, max;
        getDatasetRange(expr, first, max);

        for (unsigned i=0; i < max; i++)
        {
            if (queryBodyExtra(expr->queryChild(i))->isDenormalized)
                childrenAreDenormalized = true;
        }


        NestedSelectorInfo * extra = queryBodyExtra(expr);
        switch (expr->getOperator())
        {
        case no_select:
            if (isNewSelector(expr))
            {
                childrenAreDenormalized = true;
                spottedCandidate = true;
            }
            break;
        case no_hqlproject:
        case no_usertable:
            break;
        default:
            //Follow test effectively checks whether parent dataset is active beyond this point
            if (expr->queryBody() == expr->queryNormalizedSelector())
            {
                if (childrenAreDenormalized)
                {
                    extra->insertDenormalize = true;
                    childrenAreDenormalized = false;
                }
            }
            break;
        }

        extra->isDenormalized = childrenAreDenormalized;
    }
}

static IHqlExpression * splitSelector(IHqlExpression * expr, SharedHqlExpr & oldDataset)
{
    assertex(expr->getOperator() == no_select);
    IHqlExpression * ds = expr->queryChild(0);
    if (expr->hasAttribute(newAtom))
    {
        oldDataset.set(ds);
        OwnedHqlExpr left = createSelector(no_left, ds, querySelSeq(expr));
        return createSelectExpr(left.getClear(), LINK(expr->queryChild(1)));
    }

    HqlExprArray args;
    args.append(*splitSelector(ds, oldDataset));
    unwindChildren(args, expr, 1);
    return expr->clone(args);
}


IHqlExpression * NestedSelectorNormalizer::createNormalized(IHqlExpression * expr)
{
    IHqlExpression * root = queryRoot(expr);
    assertex(root && root->getOperator() == no_select && isNewSelector(root));
    OwnedHqlExpr selSeq = createSelectorSequence();
    OwnedHqlExpr oldDataset;
    OwnedHqlExpr newSelector = splitSelector(root, oldDataset);
    OwnedHqlExpr right = createSelector(no_right, expr, selSeq);
    HqlExprArray args;
    args.append(*LINK(oldDataset));
    args.append(*replaceExpression(expr, root, newSelector));
    args.append(*createTransformFromRow(right));
    args.append(*LINK(selSeq));
    OwnedHqlExpr ret = createDataset(no_normalize, args);
    return expr->cloneAllAnnotations(ret);
}


IHqlExpression * NestedSelectorNormalizer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    NestedSelectorInfo * extra = queryBodyExtra(expr);

    bool denormalizeInputs = false;
    if (extra->insertDenormalize)
    {
        denormalizeInputs = true;
    }
    else
    {
        switch (expr->getOperator())
        {
        case NO_AGGREGATE:
        case no_joined:
        case no_buildindex:
        case no_apply:
        case no_distribution:
        case no_distributer:
        case no_within:
        case no_notwithin:
        case no_output:
        case no_createset:
        case no_soapaction_ds:
        case no_newsoapaction_ds:
        case no_returnresult:
        case no_setgraphresult:
        case no_setgraphloopresult:
        case no_keydiff:
        case no_rowdiff:

        case no_extractresult:
//      case no_setresult:
        case no_blob2id:
        case no_selectnth:
        case no_keypatch:

        case no_assign:
        case no_lt:
        case no_le:
        case no_gt:
        case no_ge:
        case no_ne:
        case no_eq:
        case no_order:
        case no_keyed:
        case no_loopbody:

        case no_rowvalue:
        case no_setmeta:
        case no_typetransfer:
        case no_subgraph:
            denormalizeInputs = true;
            break;
        }
    }

    if (denormalizeInputs)
    {
        bool same = true;
        HqlExprArray args;
        unwindChildren(args, transformed);

        unsigned first, max;
        getDatasetRange(expr, first, max);
        for (unsigned i = first; i < max; i++)
        {
            if (queryBodyExtra(expr->queryChild(i))->isDenormalized)
            {
                args.replace(*createNormalized(&args.item(i)), i);
                same = false;
            }
        }
        if (!same)
            return transformed->clone(args);
    }
    return transformed.getClear();
}



//==============================================================================================================

/*

  Code to spot ambiguous LEFT dataset references....

  */

static HqlTransformerInfo leftRightSelectorNormalizerInfo("LeftRightSelectorNormalizer");
LeftRightSelectorNormalizer::LeftRightSelectorNormalizer(bool _allowAmbiguity) : NewHqlTransformer(leftRightSelectorNormalizerInfo)
{
    allowAmbiguity = _allowAmbiguity;
    isAmbiguous = false;
}

void LeftRightSelectorNormalizer::checkAmbiguity(const HqlExprCopyArray & inScope, IHqlExpression * selector)
{
    node_operator selectOp = selector->getOperator();
    ForEachItemIn(i, inScope)
    {
        IHqlExpression & cur = inScope.item(i);
        if ((&cur != selector) && (cur.getOperator() == selectOp) && (cur.queryRecord() == selector->queryRecord()))
        {
            isAmbiguous = true;
            if (!allowAmbiguity)
            {
                StringBuffer ecl;
                getExprECL(selector, ecl);
                throwError1(HQLERR_AmbiguousLeftRight, ecl.str());
            }
        }
    }
}


void LeftRightSelectorNormalizer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;

    IHqlExpression * selSeq = querySelSeq(expr);
    if (selSeq)
    {
        HqlExprCopyArray inScope;
        switch (getChildDatasetType(expr))
        {
        case childdataset_none:
        case childdataset_many_noscope:
        case childdataset_many:
        case childdataset_map:
        case childdataset_dataset_noscope:
        case childdataset_if:
        case childdataset_case:
        case childdataset_dataset:
        case childdataset_evaluate:
            break;
        case childdataset_datasetleft:
        case childdataset_left:
            {
                IHqlExpression * dataset = expr->queryChild(0);
                OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
                gatherChildTablesUsed(NULL, &inScope, expr, 1);
                checkAmbiguity(inScope, left);
                break;
            }
        case childdataset_leftright:
            {
                OwnedHqlExpr left = createSelector(no_left, expr->queryChild(0), selSeq);
                OwnedHqlExpr right = createSelector(no_right, expr->queryChild(1), selSeq);
                gatherChildTablesUsed(NULL, &inScope, expr, 2);
                checkAmbiguity(inScope, left);
                checkAmbiguity(inScope, right);
                break;
            }
        case childdataset_same_left_right:
        case childdataset_top_left_right:
        case childdataset_nway_left_right:
            {
                IHqlExpression * dataset = expr->queryChild(0);
                OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
                OwnedHqlExpr right = createSelector(no_right, dataset, selSeq);
                gatherChildTablesUsed(NULL, &inScope, expr, 1);
                checkAmbiguity(inScope, left);
                checkAmbiguity(inScope, right);
                break;
            }
        default:
            UNIMPLEMENTED;
        }

    }

    NewHqlTransformer::analyseExpr(expr);
}

IHqlExpression * LeftRightSelectorNormalizer::createTransformed(IHqlExpression * expr)
{
    if (expr->isAttribute() && expr->queryName() == _selectorSequence_Atom)
        return createDummySelectorSequence();

    return NewHqlTransformer::createTransformed(expr);
}

IHqlExpression * LeftRightSelectorNormalizer::createTransformedSelector(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_left:
    case no_right:
        return transform(expr);
    }
    return NewHqlTransformer::createTransformedSelector(expr);
}




//==============================================================================================================

static HqlTransformerInfo forceLocalTransformerInfo("ForceLocalTransformer");
ForceLocalTransformer::ForceLocalTransformer(ClusterType _targetClusterType) : NewHqlTransformer(forceLocalTransformerInfo)
{
    targetClusterType = _targetClusterType;
    insideForceLocal = false;
    allNodesDepth = 0;
}

IHqlExpression * ForceLocalTransformer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (expr->getOperator())
    {
    case no_forcelocal:
    case no_forcenolocal:
        {
            bool wasLocal = insideForceLocal;
            insideForceLocal = (op == no_forcelocal) && (targetClusterType != HThorCluster);
            IHqlExpression * ret = transform(expr->queryChild(0));
            insideForceLocal = wasLocal;
            return ret;
        }
    case no_thisnode:
        if ((targetClusterType != HThorCluster) && (allNodesDepth == 0))
            throwError(HQLERR_ThisNodeNotInsideAllNodes);
        //fall through
    case no_allnodes:
        {
            if (targetClusterType != HThorCluster)
            {
                unsigned oldDepth = allNodesDepth;
                if (op == no_allnodes)
                    allNodesDepth++;
                else
                    allNodesDepth--;
                IHqlExpression * ret = NewHqlTransformer::createTransformed(expr);
                allNodesDepth = oldDepth;
                return ret;
            }
            else
                return transform(expr->queryChild(0));
        }
    case no_globalscope:
    case no_colon:
        {
            bool wasLocal = insideForceLocal;
            unsigned oldDepth = allNodesDepth;
            insideForceLocal = false;
            allNodesDepth = 0;
            IHqlExpression * ret = NewHqlTransformer::createTransformed(expr);
            insideForceLocal = wasLocal;
            allNodesDepth = oldDepth;
            return ret;
        }
    }

    OwnedHqlExpr ret = NewHqlTransformer::createTransformed(expr);
    if (!insideForceLocal || !localChangesActivity(expr) || expr->hasAttribute(noLocalAtom))
        return ret.getClear();

    return appendLocalAttribute(ret);
}

ANewTransformInfo * ForceLocalTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(ForceLocalTransformInfo, expr);
}

IHqlExpression * ForceLocalTransformer::queryAlreadyTransformed(IHqlExpression * expr)
{
    ForceLocalTransformInfo * extra = queryExtra(expr);
    return extra->localTransformed[boolToInt(insideForceLocal)][boolToInt(insideAllNodes())];
}

void ForceLocalTransformer::setTransformed(IHqlExpression * expr, IHqlExpression * transformed)
{
    ForceLocalTransformInfo * extra = queryExtra(expr);
    extra->localTransformed[boolToInt(insideForceLocal)][boolToInt(insideAllNodes())].set(transformed);
}

IHqlExpression * ForceLocalTransformer::queryAlreadyTransformedSelector(IHqlExpression * expr)
{
    ForceLocalTransformInfo * extra = queryExtra(expr);
    return extra->localTransformedSelector[boolToInt(insideForceLocal)][boolToInt(insideAllNodes())];
}


void ForceLocalTransformer::setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed)
{
    ForceLocalTransformInfo * extra = queryExtra(expr);
    extra->localTransformedSelector[boolToInt(insideForceLocal)][boolToInt(insideAllNodes())].set(transformed);
}

//---------------------------------------------------------------------------

/*
This transform is responsible for ensuring that all datasets get converted to link counted datasets.  Because all records of the same type get converted it is necessary to
transform the index and table definitions - otherwise a single record would need to be transformed in multiple different ways, which would require a much more
complicated transformation.  However there are some exceptions:

a) weird field dataset syntax which dates back to hole days is no transformed.
b) add a project before a build index to ensure the code is more efficient.  (It will be later moved over any intervening sorts).
c) ensure input and out from a pipe is serialized (so that sizeof(inputrow) returns a sensible value)


*/

static HqlTransformerInfo hqlLinkedChildRowTransformerInfo("HqlLinkedChildRowTransformer");
HqlLinkedChildRowTransformer::HqlLinkedChildRowTransformer(bool _implicitLinkedChildRows) : QuickHqlTransformer(hqlLinkedChildRowTransformerInfo, NULL)
{
    implicitLinkedChildRows = _implicitLinkedChildRows;
}



IHqlExpression * HqlLinkedChildRowTransformer::ensureInputSerialized(IHqlExpression * expr)
{
    LinkedHqlExpr dataset = expr->queryChild(0);
    IHqlExpression * record = dataset->queryRecord();
    OwnedHqlExpr serializedRecord = getSerializedForm(record, diskAtom);

    //If the dataset requires serialization, it is much more efficient to serialize before the sort, than to serialize after.
    if (record == serializedRecord)
        return LINK(expr);

    OwnedHqlExpr selSeq = createSelectorSequence();

    //The expression/transform has references to the in-memory selector, but the selector provided to transform will be serialised.
    //so create a mapping <unserialized> := f(serialized)
    //and then use it to expand references to the unserialized format
    IHqlExpression * selector = dataset->queryNormalizedSelector();
    OwnedHqlExpr mapTransform = createRecordMappingTransform(no_transform, serializedRecord, selector);
    OwnedHqlExpr newDataset = createDatasetF(no_newusertable, LINK(dataset), LINK(serializedRecord), LINK(mapTransform), LINK(selSeq), NULL);

    NewProjectMapper2 mapper;
    mapper.setMapping(mapTransform);

    HqlExprArray oldArgs, expandedArgs, newArgs;
    newArgs.append(*LINK(newDataset));
    unwindChildren(oldArgs, expr, 1);
    mapper.expandFields(expandedArgs, oldArgs, dataset, newDataset, selector);

    //Finally replace any remaining selectors - so that sizeof(ds) gets mapped correctly.  Obscure..... but could possibly be used in a pipe,repeat
    ForEachItemIn(i, expandedArgs)
        newArgs.append(*replaceSelector(&expandedArgs.item(i), selector, newDataset->queryNormalizedSelector()));

    return expr->clone(newArgs);
}


IHqlExpression * HqlLinkedChildRowTransformer::transformBuildIndex(IHqlExpression * expr)
{
    return ensureInputSerialized(expr);
}


IHqlExpression * HqlLinkedChildRowTransformer::transformPipeThrough(IHqlExpression * expr)
{
    //serialize input to pipe through, so that if they happen to use sizeof(ds) on the input it will give the
    //non serialized format.  No major need to ensure output is not serialized.
    return ensureInputSerialized(expr);
}


IHqlExpression * HqlLinkedChildRowTransformer::createTransformedBody(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            switch (type->getTypeCode())
            {
            case type_dictionary:
            case type_table:
            case type_groupedtable:
                OwnedHqlExpr transformedRecord = transform(expr->queryRecord());
                if (recordRequiresLinkCount(transformedRecord))
                {
                    if (expr->hasAttribute(embeddedAtom) || queryAttribute(type, embeddedAtom) || expr->hasAttribute(countAtom) || expr->hasAttribute(sizeofAtom))
                        throwError1(HQLERR_InconsistentEmbedded, str(expr->queryId()));
                }
                if (expr->hasAttribute(embeddedAtom))
                {
                    OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
                    return removeAttribute(transformed, embeddedAtom);
                }
                if (implicitLinkedChildRows && !expr->hasAttribute(_linkCounted_Atom) && !queryAttribute(type, embeddedAtom))
                {
                    //Don't use link counted rows for weird HOLe style dataset attributes
                    if (expr->hasAttribute(countAtom) || expr->hasAttribute(sizeofAtom))
                        break;
                    //add the attribute first so a no linked field doesn't contain a record that requires it
                    OwnedHqlExpr modified = appendOwnedOperand(expr, getLinkCountedAttr());
                    return transform(modified);
                }
                break;
            }
            break;
        }
    case no_buildindex:
        {
            OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
            return transformBuildIndex(transformed);
        }
    case no_pipe:
        if (expr->queryRecord())
        {
            return QuickHqlTransformer::createTransformedBody(expr);
        }
    case no_output:
        //would this be a good idea for output to file?  output to pipe?
        if (false)
        {
            IHqlExpression * filename = queryRealChild(expr, 2);
            if (filename)
            {
                OwnedHqlExpr transformed = QuickHqlTransformer::createTransformedBody(expr);
                return ensureInputSerialized(transformed);
            }
        }
        break;
    case no_embedbody:
        //Don't change the type of an embed body - otherwise result it will become link counted when not expected.
        return LINK(expr);
    }
    return QuickHqlTransformer::createTransformedBody(expr);
}

//---------------------------------------------------------------------------

HqlScopeTaggerInfo::HqlScopeTaggerInfo(IHqlExpression * _expr) : MergingTransformInfo(_expr)
{
    if (!onlyTransformOnce() && isIndependentOfScope(_expr))
    {
        //If the node doesn't have any active selectors then it isn't going to be context dependent
        setOnlyTransformOnce(true);
    }
}

ANewTransformInfo * HqlScopeTagger::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(HqlScopeTaggerInfo, expr);
}


/*
Details of the scope tagging.

no_select
    if the lhs dataset is in scope then no flags are attached.  if the lhs is not in scope, then a newAtom attribute is
    attached to select node.  That may contain the following attributes:
        global  - is an outer level activity
        noTable - no other tables are active at this point      e.g., global[1].field;
        relatedTable - I think this could be deprecated in favour of the flag above.

Datasets
    In other situations where a dataset/datarow is ambiguous whether it is new or in scope then a no_activerow
    is inserted to indicate it is in scope.

Note:
    new attributes aren't created for subfield e.g, myDataset.level1.level2.

    This means that if (myDataset.level1) is expanded to some expression then a newAtom will be need to be created on the new select

  */

static HqlTransformerInfo hqlScopeTaggerInfo("HqlScopeTagger");
HqlScopeTagger::HqlScopeTagger(IErrorReceiver & _errors, ErrorSeverityMapper & _errorMapper)
: ScopedDependentTransformer(hqlScopeTaggerInfo), errors(_errors), errorMapper(_errorMapper)
{
}


static const char * getECL(IHqlExpression * expr, StringBuffer & s)
{
    toUserECL(s, expr, false);
    if (s.length() > 2)
        s.setLength(s.length()-2);
    return s.str();
}

void HqlScopeTagger::checkActiveRow(IHqlExpression * expr)
{
    if (!isDatasetActive(expr))
    {
        StringBuffer exprText;
        getECL(expr, exprText);
        elideString(exprText, 20);
        VStringBuffer msg("ROW(%s) - dataset argument is not in scope.  Did you mean dataset[1]?", exprText.str());
        reportError(CategoryError, msg);
    }
}

void HqlScopeTagger::reportSelectorError(IHqlExpression * selector, IHqlExpression * expr)
{
    ScopeInfo * scope = innerScope;
    if (innerScope && innerScope->isEmpty() && scopeStack.ordinality() > 1)
        scope = &scopeStack.item(scopeStack.ordinality()-2);

    StringBuffer exprText, datasetName, scopeName;
    StringBuffer msg;
    if (scope && scope->dataset)
    {
        IHqlExpression * topScope = scope->dataset;
        msg.appendf("%s - Table %s is not related to %s",
            getECL(expr, exprText),
            getExprIdentifier(datasetName, selector).str(), getExprIdentifier(scopeName, topScope).str());
    }
    else if (scope && scope->left)
    {
        msg.appendf("%s - no active row for Table %s inside transform (use LEFT?)",
            getECL(expr, exprText),
            getExprIdentifier(datasetName, selector).str());
    }
    else
    {
        msg.appendf("%s - no specified row for Table %s", getECL(expr, exprText),
            getExprIdentifier(datasetName, selector).str());
    }

    reportError(CategoryError, msg);
}


IHqlExpression * HqlScopeTagger::transformSelect(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    if (isDatasetActive(ds))
    {
        if (!innerScope && scopeStack.ordinality() == 0)
        {
            ds = queryDatasetCursor(ds);
            switch (ds->getOperator())
            {
            case no_left:
            case no_right:
                StringBuffer exprText, datasetName;
                VStringBuffer msg("%s - %s not in scope, possibly passed into a global/workflow definition", getECL(expr, exprText), getExprIdentifier(datasetName, ds).str());
                reportError(CategoryError, msg);
                break;
            }
        }
        return Parent::createTransformed(expr);             // this will call transformSelector() on lhs since new is not present
    }

    IHqlExpression * cursor = queryDatasetCursor(ds);
    if (cursor->isDataset())
    {
        if (expr->isDictionary())
        {
            StringBuffer exprText;
            VStringBuffer msg("dictionary %s must be explicitly NORMALIZED", getECL(expr, exprText));
            reportError(CategoryError, msg);
        }
        else if (!expr->isDataset())
        {
            if (!isDatasetARow(ds))
                reportSelectorError(ds, expr);
        }
    }

    pushScope();
    OwnedHqlExpr newDs = transformNewDataset(ds, false);
    popScope();

    IHqlExpression * field = expr->queryChild(1);
    if (ds->isDataset())
    {
        if (!expr->isDataset() && !expr->isDatarow() && !expr->isDictionary())
        {
            //If the left is a dataset, and the right isn't a dataset or a datarow then this doesn't make sense - it is an illegal
            return createSelectExpr(newDs.getClear(), LINK(field));
        }
    }
    //MORE: What about child datasets - should really be tagged as
    //if (!isNewDataset && field->isDataset() && !containsSelf(ds) && !isDatasetActive(ds)) isNew = true;
    if ((newDs->getOperator() == no_select) && newDs->isDatarow())
        return createSelectExpr(newDs.getClear(), LINK(field));
    return createNewSelectExpr(newDs.getClear(), LINK(field));
}

IHqlExpression * HqlScopeTagger::transformSelectorsAttr(IHqlExpression * expr)
{
    HqlExprArray args;
    HqlExprArray transformedArgs;
    unwindChildren(args, expr);
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        assertex(cur.getOperator() == no_select);
        //Only retain selectors for datasets which are in scope.
        if (isDatasetActive(cur.queryChild(0)))
            transformedArgs.append(*transformSelector(&cur));
    }
    return expr->clone(transformedArgs);
}

IHqlExpression * HqlScopeTagger::transformNewDataset(IHqlExpression * expr, bool isActiveOk)
{
    node_operator op = expr->getOperator();

    switch (op)
    {
    case no_activerow:
        {
            IHqlExpression * arg0 = expr->queryChild(0);
            checkActiveRow(arg0);
            OwnedHqlExpr transformedArg = transformSelector(arg0);
            return ensureActiveRow(transformedArg);
        }
    }

    OwnedHqlExpr transformed = transform(expr);
    switch (op)
    {
        //MORE: I'm still not quite sure the active tagging of rows is right to have these as exceptions...
    case no_left:
    case no_right:
    case no_matchattr:
        return transformed.getClear();
    case no_select:
        if (isDatasetActive(expr))
        {
            IHqlExpression * ds = expr->queryChild(0);
            if (!isAlwaysActiveRow(ds))
            {
                StringBuffer exprText;
                VStringBuffer msg("%s - Need to use active(dataset) to refer to the current row of an active dataset", getECL(expr, exprText));
                reportError(CategoryError, msg);
            }
        }
        return transformed.getClear();
    }

    if (isDatasetActive(expr))
    {
        if (!isActiveOk)
        {
            StringBuffer exprText;
            VStringBuffer msg("%s - Need to use active(dataset) to refer to the current row of an active dataset", getECL(expr, exprText));
            reportError(CategoryError, msg);
        }

        return ensureActiveRow(transformed->queryNormalizedSelector());
    }

    switch (op)
    {
    case no_if:
    case no_chooseds:
        {
            HqlExprArray args;
            args.append(*transform(expr->queryChild(0)));
            args.append(*transformNewDataset(expr->queryChild(1), false));
            ForEachChildFrom(i, expr, 2)
                args.append(*transformNewDataset(expr->queryChild(i), false));
            return expr->clone(args);
        }
    case no_addfiles:
    case no_projectrow:
        return transformAmbiguousChildren(expr);
    case no_case:
    case no_map:
        throwUnexpected();      // should have been converted to no_if by now...
    default:
        return transformed.getClear();
    }
}


IHqlExpression * HqlScopeTagger::transformAmbiguous(IHqlExpression * expr, bool isActiveOk)
{
    ITypeInfo * type = expr->queryType();
    type_t tc = type_void;
    if (type)
        tc = type->getTypeCode();

    switch (tc)
    {
    case type_void:
        return transformAmbiguousChildren(expr);
    case type_table:
    case type_groupedtable:
        {
            pushScope();
            OwnedHqlExpr ret = transformNewDataset(expr, isActiveOk);
            popScope();
            return ret.getClear();
        }
    }
    return transform(expr);
}


IHqlExpression * HqlScopeTagger::transformAmbiguousChildren(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    if (max == 0)
        return LINK(expr);

    bool same = true;
    HqlExprArray args;
    args.ensure(max);
    for(unsigned i=0; i < max; i++)
    {
        IHqlExpression * cur = expr->queryChild(i);
        IHqlExpression * tr = transformAmbiguous(cur, false);
        args.append(*tr);
        if (cur != tr)
            same = false;
    }
    if (same)
        return LINK(expr);
    return expr->clone(args);
}


IHqlExpression * HqlScopeTagger::transformSizeof(IHqlExpression * expr)
{
    IHqlExpression * arg = expr->queryChild(0)->queryNormalizedSelector();

    //Sizeof (dataset.somefield(<new>)) - convert to sizeof(record.somefield), so the argument doesn't get hoisted incorrectly, and don't get a scope error
    OwnedHqlExpr newArg;
    if (arg->getOperator() == no_select)
    {
        IHqlExpression * ds = arg->queryChild(0);
        IHqlExpression * cursor = queryDatasetCursor(ds);
        if (!isDatasetActive(cursor) && cursor->isDataset())
            newArg.setown(createSelectExpr(LINK(ds->queryRecord()), LINK(arg->queryChild(1))));
    }

    if (!newArg)
        newArg.setown(transformAmbiguous(arg, true));

    HqlExprArray args;
    args.append(*newArg.getClear());
    return completeTransform(expr, args);
}

IHqlExpression * HqlScopeTagger::transformWithin(IHqlExpression * dataset, IHqlExpression * scope)
{
    while (dataset->getOperator() == no_related)
        dataset = dataset->queryChild(0);

    if (dataset->getOperator() != no_select)
    {
        StringBuffer exprText;
        VStringBuffer msg("%s - dataset filtered by WITHIN is too complex", getECL(dataset, exprText));
        reportError(CategoryError, msg);
        return transform(dataset);
    }

    IHqlExpression * ds = dataset->queryChild(0);
    IHqlExpression * field = dataset->queryChild(1);
    if (ds->queryNormalizedSelector() == scope)
    {
        OwnedHqlExpr newDs = transform(ds);
        return createSelectExpr(newDs.getClear(), LINK(field));
    }
    OwnedHqlExpr newDs = transformWithin(ds, scope);
    return createNewSelectExpr(newDs.getClear(), LINK(field));
}

IHqlExpression * HqlScopeTagger::transformRelated(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * scope = expr->queryChild(1);

    if (!isDatasetActive(scope))
    {
        StringBuffer exprText;
        VStringBuffer msg("dataset \"%s\" used in WITHIN is not in scope", getECL(scope, exprText));
        reportError(CategoryError, msg);
    }

    //Check the ds is a table
    IHqlDataset * scopeDs = scope->queryDataset();
    if (scopeDs != scopeDs->queryTable())
    {
        StringBuffer exprText;
        VStringBuffer msg("dataset \"%s\" used as parameter to WITHIN is too complex", getECL(expr, exprText));
        reportError(CategoryError, msg);
    }

    return transformWithin(ds, scope->queryNormalizedSelector());
}


IHqlExpression * HqlScopeTagger::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody(true);
    if (expr != body)
    {
        switch (expr->getAnnotationKind())
        {
        case annotate_meta:
            {
                unsigned max = errorMapper.processMetaAnnotation(expr);
                OwnedHqlExpr transformedBody = transform(body);
                errorMapper.restoreLocalOnWarnings(max);
                if (body == transformedBody)
                    return LINK(expr);
                return expr->cloneAnnotation(transformedBody);
            }
            break;
        case annotate_symbol:
            {
                ErrorSeverityMapper::SymbolScope saved(errorMapper, expr);
                OwnedHqlExpr transformedBody = transform(body);
                if (body == transformedBody)
                    return LINK(expr);
                return expr->cloneAnnotation(transformedBody);
            }
        case annotate_location:
            {
                break;
            }
        }
        OwnedHqlExpr transformedBody = transform(body);
        if (body == transformedBody)
            return LINK(expr);
        return expr->cloneAnnotation(transformedBody);
    }

    switch (expr->getOperator())
    {
    case no_left:
    case no_right:
    case no_self:
    case no_top:
        return LINK(expr);
    case no_activerow:
        checkActiveRow(expr->queryChild(0));
        break;
    case no_select:
        return transformSelect(expr);
    case no_call:
    case no_externalcall:
    case no_rowvalue:
//  case no_addfiles:
//  case no_libraryscopeinstance:??
        return transformAmbiguousChildren(expr);
    case no_offsetof:
    case no_sizeof:
        return transformSizeof(expr);
    case no_attr_expr:
        if (expr->queryName() == _selectors_Atom)
            return transformSelectorsAttr(expr);
        return transformAmbiguousChildren(expr);

    case no_datasetfromrow:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if (ds->isDataset() && !isDatasetActive(ds))
            {
                StringBuffer exprText;
                VStringBuffer msg("dataset %s mistakenly interpreted as a datarow, possibly due to missing dataset() in parameter type", getECL(ds, exprText));
                reportError(CategoryError, msg);
            }
            return transformAmbiguousChildren(expr);
        }
    case no_temptable:
        if (expr->queryChild(0)->isDatarow())
            return transformAmbiguousChildren(expr);
        break;
    case no_related:
        return transformRelated(expr);
    case no_eq:
    case no_ne:
    case no_lt:
    case no_le:
    case no_gt:
    case no_ge:
    case no_order:
        //MORE: Should check this doesn't make the comparison invalid.
        return transformAmbiguousChildren(expr);
    case no_assign:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * rhs = expr->queryChild(1);
            OwnedHqlExpr newRhs = transformAmbiguous(rhs, false);
            if (lhs->isDatarow() && newRhs->isDataset())
            {
                StringBuffer exprText;
                VStringBuffer msg("dataset expression (%s) assigned to field '%s' with type row", getECL(rhs, exprText), str(lhs->queryChild(1)->queryName()));
                reportError(CategoryError, msg.str());
            }
            if (rhs == newRhs)
                return LINK(expr);
            HqlExprArray children;
            children.append(*LINK(expr->queryChild(0)));
            children.append(*newRhs.getClear());
            return completeTransform(expr, children);
        }
        break;
    case no_evaluate:
        throwUnexpected();
    case no_projectrow:
        {
            OwnedHqlExpr transformed = Parent::createTransformed(expr);
            if (transformed->queryChild(0)->isDataset())
                reportError(CategoryError, "PROJECT() row argument resolved to a dataset.  Missing DATASET() from parameter type?");
            return transformed.getClear();
        }
    case no_merge:
        {
            HqlExprArray children;
            transformChildren(expr, children);
            IHqlExpression * sorted = queryAttribute(sortedAtom, children);
            if (!sorted || queryAttribute(_implicitSorted_Atom, children))
            {
                IHqlExpression * dataset = &children.item(0);
                removeAttribute(children, _implicitSorted_Atom);

                if (sorted)
                    children.zap(*sorted);

                HqlExprArray sorts;
                OwnedHqlExpr order = getExistingSortOrder(dataset, expr->hasAttribute(localAtom), true);
                if (order)
                    unwindChildren(sorts, order);
                ForEachItemInRev(i, sorts)
                {
                    if (sorts.item(i).isAttribute())
                    {
                        reportError(CategorySyntax, HQLWRN_MergeBadSortOrder_Text);
                        sorts.remove(i);
                    }
                }
                children.append(*createExprAttribute(sortedAtom, sorts));
            }
            return expr->clone(children);
        }
    }

    return Parent::createTransformed(expr);
}


void HqlScopeTagger::reportError(WarnErrorCategory category, const char * msg)
{
    IHqlExpression * location = errorMapper.queryActiveSymbol();
    //Make this an error when we are confident...
    int startLine= location ? location->getStartLine() : 0;
    int startColumn = location ? location->getStartColumn() : 0;
    ISourcePath * sourcePath = location ? location->querySourcePath() : NULL;
    ErrorSeverity severity = queryDefaultSeverity(category);
    Owned<IError> err = createError(category, severity, ERR_ASSERT_WRONGSCOPING, msg, str(sourcePath), startLine, startColumn, 0);
    errors.report(err);        // will throw immediately if it is an error.
}


//---------------------------------------------------------------------------------------------------------------------

SharedTableInfo * ImplicitAliasTransformInfo::uses(IHqlExpression * tableBody) const
{
   ForEachItemIn(i, sharedTables)
   {
        SharedTableInfo & cur = sharedTables.item(i);
        if (cur.dataset == tableBody)
            return &cur;
   }
   return NULL;
}

void ImplicitAliasTransformInfo::add(SharedTableInfo * table)
{
    sharedTables.append(*LINK(table));
}

void ImplicitAliasTransformInfo::addAmbiguity(SharedTableInfo * table)
{
    containsAmbiguity = true;
    merge(table);
}

void ImplicitAliasTransformInfo::merge(SharedTableInfo * table)
{
    ForEachItemIn(i, sharedTables)
    {
        SharedTableInfo & cur = sharedTables.item(i);
        if (cur.dataset == table->dataset)
        {
            if (cur.depth < table->depth)
                sharedTables.replace(*LINK(table), i);
            return;
        }
    }
    add(table);
}

void ImplicitAliasTransformInfo::inherit(const ImplicitAliasTransformInfo * other)
{
    ForEachItemIn(i, other->sharedTables)
        merge(&other->sharedTables.item(i));
}


static HqlTransformerInfo implicitAliasTransformerInfo("ImplicitAliasTransformer");
ImplicitAliasTransformer::ImplicitAliasTransformer() : NewHqlTransformer(implicitAliasTransformerInfo)
{
    seenShared = true;
    seenAmbiguity = false;
}


void ImplicitAliasTransformer::analyseExpr(IHqlExpression * _expr)
{
    IHqlExpression * body = _expr->queryBody();
    if (alreadyVisited(body))
    {
        if ((pass == 0) && body->isDataset())
        {
            switch (body->getOperator())
            {
            case no_rows:
                break;
            default:
                seenShared = true;
                queryExtra(body)->shared.setown(new SharedTableInfo(body, 0));
                break;
            }
        }
        return;
    }

    NewHqlTransformer::analyseExpr(body);
    if (pass == 0)
        return;

    ImplicitAliasTransformInfo * extra = queryExtra(body);
    if (extra->shared)
        extra->add(extra->shared);

    switch (body->getOperator())
    {
    case no_activerow:
    case no_filepos:
    case no_file_logicalname:
    case no_offsetof:
    case no_joined:
    case no_colon:
    case no_globalscope:
    case no_attr:
        return;
    case no_select:
        {
            bool isNew;
            IHqlExpression * ds = querySelectorDataset(body, isNew);
            if (isNew)
            {
                ImplicitAliasTransformInfo * dsExtra = queryExtra(ds->queryBody());
                extra->inherit(dsExtra);
            }
            return;
        }
    }

    IHqlExpression * dataset = NULL;
    switch (getChildDatasetType(body))
    {
    case childdataset_none:
    case childdataset_many_noscope:
    case childdataset_many:
    case childdataset_map:
    case childdataset_dataset_noscope:
    case childdataset_if:
    case childdataset_case:
    case childdataset_evaluate:
    case childdataset_left:
    case childdataset_leftright:
    case childdataset_same_left_right:
    case childdataset_nway_left_right:
        break;
    case childdataset_dataset:
    case childdataset_datasetleft:
    case childdataset_top_left_right:
        dataset = body->queryChild(0)->queryBody();
        break;
    default:
        UNIMPLEMENTED;
    }

    ForEachChild(i, body)
    {
        IHqlExpression * cur = body->queryChild(i);
        ImplicitAliasTransformInfo * childExtra = queryExtra(cur->queryBody());
        //If this is one of the arguments to an operation which has an active top dataset,
        //check to see if any of the contained expressions reference this item
        if (dataset && (i != 0))
        {
            SharedTableInfo * match = childExtra->uses(dataset);
            if (match)
            {
                seenAmbiguity = true;
                SharedTableInfo * nested = createAmbiguityInfo(match->dataset, match->depth+1);
                extra->addAmbiguity(nested);
            }
//            dbglogExpr(_expr);
//            DBGLOG("Implicit nested table ambiguity spotted in expression");
        }
        extra->inherit(childExtra);
    }
}

SharedTableInfo * ImplicitAliasTransformer::createAmbiguityInfo(IHqlExpression * dataset, unsigned depth)
{
    ForEachItemIn(i, ambiguousTables)
    {
        SharedTableInfo & cur = ambiguousTables.item(i);
        if ((cur.dataset == dataset) && (depth == cur.depth))
            return &cur;
    }
    ambiguousTables.append(*new SharedTableInfo(dataset, depth));
    return &ambiguousTables.tos();
}


ANewTransformInfo * ImplicitAliasTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(ImplicitAliasTransformInfo, expr);
}

IHqlExpression * ImplicitAliasTransformer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (expr != body)
    {
        OwnedHqlExpr newBody = transform(body);
        return expr->cloneAllAnnotations(newBody);
    }

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);

    ImplicitAliasTransformInfo * extra = queryExtra(body);
    if (!extra->containsAmbiguity)
        return transformed.getClear();

    SharedTableInfo * match = extra->uses(body->queryChild(0)->queryBody());
    assertex(match && match->depth != 0);
    if (!match->uid)
        match->uid.setown(createUniqueId());

    OwnedHqlExpr aliased;
    IHqlExpression * dataset = transformed->queryChild(0)->queryBody();
    IHqlExpression * uid = match->uid;
    aliased.setown(createDataset(no_dataset_alias, LINK(dataset), LINK(uid)));

    //Replace dataset with an aliased variety, and remap all the selectors
    HqlExprArray args;
    args.append(*LINK(aliased));
    replaceSelectors(args, transformed, 1, dataset->queryNormalizedSelector(), aliased->queryNormalizedSelector());
    return transformed->clone(args);
}

void ImplicitAliasTransformer::process(HqlExprArray & exprs)
{
    analyseArray(exprs, 0);
    if (!seenShared)
        return;
    analyseArray(exprs, 1);
    if (hasAmbiguity())
    {
//        DBGLOG("Implicit nested dataset ambiguity spotted in expression");
        HqlExprArray transformed;
        transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }
}
//---------------------------------------------------------------------------------------------------------------------

SharedTableInfo * LeftRightTransformInfo::uses(IHqlExpression * tableBody) const
{
   ForEachItemIn(i, sharedTables)
   {
        SharedTableInfo & cur = sharedTables.item(i);
        if (cur.dataset == tableBody)
            return &cur;
   }
   return NULL;
}

void LeftRightTransformInfo::add(SharedTableInfo * table)
{
    sharedTables.append(*LINK(table));
}

bool LeftRightTransformInfo::noteUsed(IHqlExpression * seq)
{
    if (!seqs.contains(*seq))
        seqs.append(*seq);

    if (seqs.ordinality() > 1)
    {
        shared.setown(new SharedTableInfo(original, 0));
        return true;
    }
    return false;
}

void LeftRightTransformInfo::addAmbiguity(SharedTableInfo * table)
{
    containsAmbiguity = true;
    merge(table);
}

void LeftRightTransformInfo::merge(SharedTableInfo * table)
{
    ForEachItemIn(i, sharedTables)
    {
        SharedTableInfo & cur = sharedTables.item(i);
        if (cur.dataset == table->dataset)
        {
            if (cur.depth < table->depth)
                sharedTables.replace(*LINK(table), i);
            return;
        }
    }
    add(table);
}

void LeftRightTransformInfo::inherit(const LeftRightTransformInfo * other)
{
    ForEachItemIn(i, other->sharedTables)
        merge(&other->sharedTables.item(i));
}


static HqlTransformerInfo LeftRightTransformerInfo("LeftRightTransformer");
LeftRightTransformer::LeftRightTransformer() : NewHqlTransformer(LeftRightTransformerInfo)
{
    seenShared = true;
}


void LeftRightTransformer::analyseExpr(IHqlExpression * _expr)
{
    IHqlExpression * body = _expr->queryBody();
    if (alreadyVisited(body))
        return;

    NewHqlTransformer::analyseExpr(body);
    if (pass == 0)
    {
        //First pass gathers a list of selectors that are potentially ambiguous if the sequence was removed
        IHqlExpression * left = NULL;
        IHqlExpression * right = NULL;
        switch (getChildDatasetType(body))
        {
        case childdataset_left:
        case childdataset_datasetleft:
            left = body->queryChild(0);
            break;
        case childdataset_same_left_right:
        case childdataset_nway_left_right:
        case childdataset_top_left_right:
            left = body->queryChild(0);
            right = body->queryChild(0);
            break;
        case childdataset_leftright:
            left = body->queryChild(0);
            right = body->queryChild(1);
            break;
        }

        if (left)
        {
            LeftRightTransformInfo * extra = queryExtra(body);
            IHqlExpression * selSeq = querySelSeq(body);
            OwnedHqlExpr seq = createDummySelectorSequence();
            extra->rawLeft.setown(createSelector(no_left, left, seq));
            incUsage(extra->rawLeft, selSeq);
            if (right)
            {
                //no_left is used deliberately in the following to avoid complications where right matches
                //but left doesn't, causing the depths to be messed up.
                extra->rawRight.setown(createSelector(no_left, right, seq));
                if (extra->rawLeft != extra->rawRight)
                    incUsage(extra->rawRight, selSeq);
                else
                    extra->rawRight.clear();
            }
        }
    }
    else
    {
        //Second pass - for each expression, gather a list of selectors that would actually be ambiguous.
        LeftRightTransformInfo * extra = queryExtra(body);
        IHqlExpression * rawLeft = extra->rawLeft;
        IHqlExpression * rawRight = extra->rawRight;

        //If LEFT is potentially ambiguous, then add it to the list of selectors used by this expression
        if (rawLeft)
        {
            LeftRightTransformInfo * leftExtra = queryExtra(rawLeft);
            if (leftExtra->shared)
                extra->add(leftExtra->shared);
        }
        //Ditto for right
        if (rawRight)
        {
            LeftRightTransformInfo * rightExtra = queryExtra(rawRight);
            if (rightExtra->shared)
                extra->add(rightExtra->shared);
        }

        switch (body->getOperator())
        {
        case no_activerow:
        case no_filepos:
        case no_file_logicalname:
        case no_offsetof:
        case no_joined:
        case no_colon:
        case no_globalscope:
        case no_attr:
            return;
        case no_select:
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(body, isNew);
                if (isNew)
                {
                    LeftRightTransformInfo * dsExtra = queryExtra(ds->queryBody());
                    extra->inherit(dsExtra);
                }
                return;
            }
        }

        ForEachChild(i, body)
        {
            IHqlExpression * cur = body->queryChild(i);
            LeftRightTransformInfo * childExtra = queryExtra(cur->queryBody());
            //If this is one of the arguments to an operation which has an active top dataset,
            //check to see if any of the contained expressions reference this item
            if ((i != 0) && rawLeft)
            {
                SharedTableInfo * matchLeft = childExtra->uses(rawLeft);
                SharedTableInfo * matchRight = rawRight ? childExtra->uses(rawRight) : NULL;
                if (matchLeft || matchRight)
                {
                    unsigned leftDepth = matchLeft ? matchLeft->depth : 0;
                    unsigned rightDepth = matchRight ? matchRight->depth : 0;
                    unsigned depth = leftDepth > rightDepth ? leftDepth : rightDepth;
                    SharedTableInfo * nested = createAmbiguityInfo(rawLeft, depth+1);
                    extra->addAmbiguity(nested);
                    if (rawRight)
                    {
                        SharedTableInfo * nested = createAmbiguityInfo(rawRight, depth+1);
                        extra->addAmbiguity(nested);
                    }
                }
            }
            extra->inherit(childExtra);
        }
    }
}

void LeftRightTransformer::incUsage(IHqlExpression * expr, IHqlExpression * seq)
{
    //MORE: Needs to keep track of the sequences that were used with it, so know if needs disambiguating.
    LeftRightTransformInfo * extra = queryExtra(expr);
    if (extra->noteUsed(seq))
        seenShared = true;
}

SharedTableInfo * LeftRightTransformer::createAmbiguityInfo(IHqlExpression * dataset, unsigned depth)
{
    ForEachItemIn(i, ambiguousTables)
    {
        SharedTableInfo & cur = ambiguousTables.item(i);
        if ((cur.dataset == dataset) && (depth == cur.depth))
            return &cur;
    }
    ambiguousTables.append(*new SharedTableInfo(dataset, depth));
    return &ambiguousTables.tos();
}


ANewTransformInfo * LeftRightTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(LeftRightTransformInfo, expr);
}

IHqlExpression * LeftRightTransformer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    if (expr != body)
    {
        OwnedHqlExpr newBody = transform(body);
        return expr->cloneAllAnnotations(newBody);
    }

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);

    LeftRightTransformInfo * extra = queryExtra(body);
    SharedTableInfo * matchLeft = extra->rawLeft ? extra->uses(extra->rawLeft) : NULL;
    if (matchLeft)
    {
        childDatasetType dsType = getChildDatasetType(expr);
        IHqlExpression * left = transformed->queryChild(0);
        IHqlExpression * right = hasRight(dsType) ? (hasSameLeftRight(dsType) ? left : transformed->queryChild(1)) : NULL;

        IHqlExpression * oldSelSeq = querySelSeq(expr);
        OwnedHqlExpr newSelSeq = createSelectorSequence(matchLeft->depth);
        OwnedHqlExpr oldLeft = createSelector(no_left, left, oldSelSeq);
        OwnedHqlExpr newLeft = createSelector(no_left, left, newSelSeq);

        //Replace dataset with an aliased variety, and remap all the selectors
        HqlExprArray mapped;
        replaceSelectors(mapped, transformed, 1, oldLeft, newLeft);

        if (right)
        {
            OwnedHqlExpr oldRight = createSelector(no_right, right, oldSelSeq);
            OwnedHqlExpr newRight = createSelector(no_right, right, newSelSeq);
            unsigned firstArg = (hasSameLeftRight(dsType) ? 0 : 1);
            replaceSelectors(mapped, firstArg, oldRight, newRight);
        }

        HqlExprArray args;
        args.append(*LINK(left));
        appendArray(args, mapped);
        args.zap(*oldSelSeq);
        args.append(*LINK(newSelSeq));
        transformed.setown(transformed->clone(args));
    }
    return transformed.getClear();
}

void LeftRightTransformer::process(HqlExprArray & exprs)
{
    analyseArray(exprs, 0);
    if (!seenShared)
        return;
    analyseArray(exprs, 1);
    HqlExprArray transformed;
    transformRoot(exprs, transformed);
    replaceArray(exprs, transformed);
}
//---------------------------------------------------------------------------------------------------------------------

/*
  Common up expressions so that all references to the same expression have identical symbols, annotations.
  Generally it improves the code a lot - especially when macros are used.  However there are occasional problems....

  a) ut.CleanCompany(ds.x) and ut.cleanCompany(left.x).
     If a component one of them is commoned up with another occurrence, then you can get different named symbols within the expanded function.  When we
     then test to see if something is sorted it can incorrectly mismatch (see busheader.xhql dnb_combined_append)


  */
static void unwindAnnotations(HqlExprCopyArray & unwound, IHqlExpression * expr)
{
    if (expr->getAnnotationKind() == annotate_none)
        return;
    unwindAnnotations(unwound, expr->queryBody(true));
    unwound.append(*expr);
}

IHqlExpression * AnnotationTransformInfo::cloneAnnotations(IHqlExpression * newBody)
{
    if (annotations.ordinality() == 0)
        return LINK(newBody);

    return annotations.item(0).cloneAllAnnotations(newBody);
    LinkedHqlExpr ret = newBody;

#if 1
    ForEachItemIn(i, annotations)
        ret.setown(annotations.item(i).cloneAllAnnotations(ret));

#else
    //Code saved once we start removing duplicate annotations (e.g., locations)
    HqlExprCopyArray toApply;
    ForEachItemIn(i, annotations)
        unwindAnnotations(toApply, &annotations.item(i));

    ForEachItemIn(i2, toApply)
    {
        IHqlExpression & curAnnotate = toApply.item(i2);
        ret.setown(curAnnotate.cloneAnnotation(ret));
    }
#endif

    return ret.getClear();
}

void AnnotationTransformInfo::noteAnnotation(IHqlExpression * annotation)
{
    //MORE: Need more intelligence to see if this is a subset of what we already have..
    annotations.append(*annotation);
}

static HqlTransformerInfo annotationNormalizerInfo("AnnotationNormalizerTransformer");
AnnotationNormalizerTransformer::AnnotationNormalizerTransformer()
: NewHqlTransformer(annotationNormalizerInfo)
{
}

ANewTransformInfo * AnnotationNormalizerTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(AnnotationTransformInfo, expr);
}

void AnnotationNormalizerTransformer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;
    IHqlExpression * body = expr->queryBody();
    if (expr != body)
    {
        queryLocationIndependentExtra(body)->noteAnnotation(expr);
        //Note: expr already tested if expr == body...
        if (alreadyVisited(body))
            return;
    }

    node_operator op = body->getOperator();
    switch (op)
    {
    case no_attr_expr:
        analyseChildren(body);
        return;
    }

    NewHqlTransformer::analyseExpr(body);
}


IHqlExpression * AnnotationNormalizerTransformer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    IHqlExpression * body = expr->queryBody();

    switch (op)
    {
    case no_list:
        {
            if (body->numChildren() == 0)
                return LINK(body);
            break;
        }
    case no_constant:
//  case no_null:
        {
            //AnnotationTransformInfo * extra = queryLocationIndependentExtra(body);

            //Don't common up the location information for this, otherwise it gets silly!  Possibly worth removing altogether if ambiguous?
            //MORE: This should probably depend on whether there is more than one annotation on the constant.
            return LINK(body);
        }
    }

    if (expr != body)
        return transform(body);

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    return queryLocationIndependentExtra(body)->cloneAnnotations(transformed);
}

AnnotationTransformInfo * AnnotationNormalizerTransformer::queryLocationIndependentExtra(IHqlExpression * expr)
{
    return static_cast<AnnotationTransformInfo *>(queryTransformExtra(queryLocationIndependent(expr)));
}

void normalizeAnnotations(HqlCppTranslator & translator, HqlExprArray & exprs)
{
    //First iterate through the expressions and call queryLocationIndependent() to avoid nested transforms (which are less efficient)
    ForEachItemIn(iInit, exprs)
        queryLocationIndependent(&exprs.item(iInit));

    translator.traceExpressions("before annotation normalize", exprs);

    cycle_t startCycles = get_cycles_now();
    AnnotationNormalizerTransformer normalizer;
    HqlExprArray transformed;
    normalizer.analyseArray(exprs, 0);
    normalizer.transformRoot(exprs, transformed);
    replaceArray(exprs, transformed);
    translator.noteFinishedTiming("compile:tree transform: normalize.annotations", startCycles);
}

//---------------------------------------------------------------------------

static HqlTransformerInfo containsCompoundTransformerInfo("ContainsCompoundTransformer");
ContainsCompoundTransformer::ContainsCompoundTransformer()
: QuickHqlTransformer(containsCompoundTransformerInfo, NULL)
{
    containsCompound = false;
}

//NB: This cannot be short circuited, because it is also gathering information about whether or
void ContainsCompoundTransformer::doAnalyseBody(IHqlExpression * expr)
{
    if (containsCompound)
        return;

    switch (expr->getOperator())
    {
    case no_compound:
        if (!expr->isAction())
        {
            containsCompound = true;
            return;
        }
        break;
    case no_colon:
    case no_cluster:
    case no_sequential:
    case no_allnodes:
    case no_thisnode:
        //Need to recursively handle these
        containsCompound = true;
        return;
    case no_record:
    case no_field:
    case no_attr:
    case no_attr_link:
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_workunit_dataset:
    case no_assertwild:
    case no_getresult:
    case no_getgraphresult:
    case no_activerow:
    case no_newkeyindex:
        return;
    case no_list:
        if (expr->isConstant())
            return;
        break;
    case no_select:
//      if (expr->hasAttribute(newAtom))
            analyse(expr->queryChild(0));
        return;
    case no_assign:
        analyse(expr->queryChild(1));
        return;
    }

    QuickHqlTransformer::doAnalyseBody(expr);
}


bool containsCompound(const HqlExprArray & exprs)
{
    ContainsCompoundTransformer spotter;
    spotter.analyseArray(exprs);
    return spotter.containsCompound;
}

bool containsCompound(IHqlExpression * expr)
{
    ContainsCompoundTransformer spotter;
    spotter.analyse(expr);
    return spotter.containsCompound;
}

static HqlTransformerInfo nestedCompoundTransformerInfo("NestedCompoundTransformer");
NestedCompoundTransformer::NestedCompoundTransformer(HqlCppTranslator & _translator)
: HoistingHqlTransformer(nestedCompoundTransformerInfo, CTFnoteifactions), translator(_translator), translatorOptions(_translator.queryOptions())
{
}


//For the moment allow simple external calls in scalar setting
//to make logging much easier...
static bool isSimpleSideeffect(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_externalcall:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return true;
    case no_comma:
    case no_compound:
    case no_parallel:
        {
            ForEachChild(i, expr)
            {
                if (!isSimpleSideeffect(expr->queryChild(i)))
                    return false;
            }
            return true;
        }
    case no_if:
        {
            ForEachChildFrom(i, expr, 1)
            {
                if (!isSimpleSideeffect(expr->queryChild(i)))
                    return false;
            }
            return true;
        }
    }
    return false;
}

static bool isCalloutSideeffect(IHqlExpression * expr)
{
    if (!expr->queryType()->isScalar())
        return false;
    return isSimpleSideeffect(expr->queryChild(0));
}

IHqlExpression * NestedCompoundTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
        return LINK(expr);

    IHqlExpression * ret = queryTransformAnnotation(expr);
    if (ret)
        return ret;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_compound:
        if (isUsedUnconditionally(expr) && !expr->isAction() && !isCalloutSideeffect(expr))
        {
            IHqlExpression * sideEffect = expr->queryChild(0);
            IHqlExpression * value = expr->queryChild(1);
            if (!isIndependentOfScope(sideEffect))
            {
                StringBuffer s;
                if (sideEffect->queryName())
                    s.appendf(" '%s'", str(sideEffect->queryName()));
                else if (value->queryName())
                    s.appendf(" '%s'", str(value->queryName()));
                else
                    s.append(" ").append(getOpString(sideEffect->getOperator()));
                IHqlExpression * location = queryLocation(sideEffect);
                if (!location)
                    location = queryLocation(value);
                if (!location)
                    location = queryActiveLocation();
                if (!isSimpleSideeffect(sideEffect))
                {
                    //MORE: This should possibly be an error, but there are still false positives e.g., OUTPUT(ds1.childds)
                    //so needs to stay a warning.
//                  translator.ERRORAT1(location, HQLERR_GlobalSideEffectDependent, s.str());
                    translator.WARNINGAT1(CategoryUnexpected, location, HQLWRN_GlobalSideEffectDependent, s.str());
                }
                break;
            }

            if (!translatorOptions.workunitTemporaries)
            {
                StringBuffer s;
                if (expr->queryName())
                    s.append(expr->queryName()).append(": ");
                getExprECL(sideEffect, s);
                throwError1(HQLERR_LibrariesCannotContainSideEffects, s.str());
            }

            appendToTarget(*transform(sideEffect));
            return transform(value);
        }
        break;
    }

    return HoistingHqlTransformer::createTransformed(expr);
}

//---------------------------------------------------------------------------

class LocationInfo : public CInterface
{
public:
    HqlExprArray matches;
};

static HqlTransformerInfo duplicateCodeSpotterInfo("DuplicateCodeSpotter");
class DuplicateCodeSpotter : public QuickHqlTransformer
{
public:
    DuplicateCodeSpotter() : QuickHqlTransformer(duplicateCodeSpotterInfo, NULL) {}
    inline bool checkExpr(IHqlExpression * expr)
    {
        if (!expr->isDataset())
            return false;
        switch (expr->getOperator())
        {
        case no_join:
            break;
        default:
            return false;
        }
        return true;
    }
    virtual void doAnalyse(IHqlExpression * expr)
    {
        if (checkExpr(expr))
        {
            IHqlExpression * location = queryLocation(expr);
            if (location)
            {
                OwnedHqlExpr attr = createLocationAttr(location->querySourcePath(), location->getStartLine(), location->getStartColumn(), 0);
                Linked<LocationInfo> info;
                Shared<LocationInfo> * match = map.getValue(attr);
                if (match)
                    info.set(*match);
                else
                {
                    info.setown(new LocationInfo);
                    map.setValue(attr, info);
                }
                IHqlExpression * body = expr->queryBody();
                if (!info->matches.contains(*body))
                {
                    ForEachItemIn(i, info->matches)
                    {
                        debugFindFirstDifference(body, &info->matches.item(i));
                    }
                    info->matches.append(*LINK(body));
                }
            }
        }
        QuickHqlTransformer::doAnalyse(expr);
    }

    MapOwnedToOwned<IHqlExpression, LocationInfo> map;
};


void spotPotentialDuplicateCode(HqlExprArray & exprs)
{
    DuplicateCodeSpotter spotter;
    spotter.analyseArray(exprs);
}

//---------------------------------------------------------------------------

static bool isUniqueAttributeName(IAtom * name)
{
    const char * nameText = str(name);
    unsigned len = strlen(nameText);
    if (len > 3)
    {
        if ((nameText[len-2] == '_') && (nameText[len-1] == '_') && isdigit((unsigned char)nameText[len-3]))
            return true;
    }
    return false;
}

static bool containsUpperCase(const char * s)
{
    loop
    {
        unsigned char next = *s++;
        if (!next)
            return false;
        if (isupper(next))
            return true;
    }
}

static IIdAtom * simplifySymbolName(IIdAtom * name, bool commonUniqueNameAttributes)
{
    if (!commonUniqueNameAttributes)
        return NULL;

    //Rename all attributes __x__1234__ to __x__
    const char * nameText = str(lower(name));
    size_t nameLen = strlen(nameText);
    size_t len = nameLen;
    if (len > 3)
    {
        if ((nameText[len-2] == '_') && (nameText[len-1] == '_') && isdigit((unsigned char)nameText[len-3]))
        {
            len -= 3;
            while (len && isdigit((unsigned char)nameText[len-1]))
                len--;
            //Shouldn't be possible...
            if (len == 0)
                len = nameLen;
        }
    }
    if (nameLen != len)
    {
        StringAttr truncName;
        truncName.set(nameText, len);
        return createIdAtom(truncName);
    }
    return NULL;
}

static IIdAtom * lowerCaseSymbolName(IIdAtom * name)
{
    if (containsUpperCase(str(name)))
        return createIdAtom(str(lower(name)));
    return name;
}

static bool exprIsSelfConstant(IHqlExpression * expr)
{
    if (expr->isConstant())
        return true;
    switch (expr->getOperator())
    {
    case no_select:
        {
            IHqlExpression * selector = expr->queryChild(0);
            while (selector->getOperator() == no_select)
                selector = selector->queryChild(0);
            return (selector->getOperator() == no_selfref);
        }
    }
    if (expr->isDataset() && (getNumChildTables(expr) == 0))
        return false;

    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!exprIsSelfConstant(cur))
            return false;
    }
    return true;
}


static IAtom * queryPatUseModule(IHqlExpression * expr)
{
    IHqlExpression * moduleAttr = expr->queryAttribute(moduleAtom);
    if (moduleAttr)
        return moduleAttr->queryChild(0)->queryBody()->queryName();
    return NULL;
}

static IAtom * queryPatUseName(IHqlExpression * expr)
{
    IHqlExpression * nameAttr = expr->queryAttribute(nameAtom);
    return nameAttr->queryChild(0)->queryBody()->queryName();
}


void HqlTreeNormalizerInfo::noteSymbol(IHqlExpression * _symbol)
{
    if (!symbol || isUniqueAttributeName(symbol->queryName()))
        symbol = _symbol;
}

static HqlTransformerInfo hqlTreeNormalizerInfo("HqlTreeNormalizer");
HqlTreeNormalizer::HqlTreeNormalizer(HqlCppTranslator & _translator) : NewHqlTransformer(hqlTreeNormalizerInfo), translator(_translator)
{
    seenForceLocal = false;
    seenLocalUpload = false;
    const HqlCppOptions & translatorOptions = translator.queryOptions();
    options.assertSortedDistributed = translatorOptions.assertSortedDistributed;
    options.removeAsserts = !translatorOptions.checkAsserts;
    options.commonUniqueNameAttributes = translatorOptions.commonUniqueNameAttributes;
    options.sortIndexPayload = translatorOptions.sortIndexPayload;
    options.allowSections = translatorOptions.allowSections;
    options.normalizeExplicitCasts = translatorOptions.normalizeExplicitCasts;
    options.ensureRecordsHaveSymbols  = translatorOptions.ensureRecordsHaveSymbols;
    options.outputRowsAsDatasets = translator.targetRoxie();
    options.constantFoldNormalize = translatorOptions.constantFoldNormalize;
    options.allowActivityForKeyedJoin = translatorOptions.allowActivityForKeyedJoin;
    options.implicitSubSort = translatorOptions.implicitBuildIndexSubSort;
    errorProcessor = &translator.queryErrorProcessor();
    nextSequenceValue = 1;
}



ANewTransformInfo * HqlTreeNormalizer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(HqlTreeNormalizerInfo, expr);
}

HqlTreeNormalizerInfo * HqlTreeNormalizer::queryLocationIndependentExtra(IHqlExpression * expr)
{
    return static_cast<HqlTreeNormalizerInfo *>(queryTransformExtra(queryLocationIndependent(expr)));
}


void HqlTreeNormalizer::convertRecordToAssigns(HqlExprArray & assigns, IHqlExpression * oldRecord, IHqlExpression * targetSelector, bool canOmit, bool convertTempTable)
{
    ForEachChild(idx, oldRecord)
    {
        IHqlExpression * oldField = oldRecord->queryChild(idx);
        OwnedHqlExpr newField = transform(oldField);

        switch (oldField->getOperator())
        {
        case no_ifblock:
            convertRecordToAssigns(assigns, oldField->queryChild(1), targetSelector, canOmit, convertTempTable);
            break;
        case no_record:
            convertRecordToAssigns(assigns, oldField, targetSelector, canOmit, convertTempTable);
            break;
        case no_field:
            {
                IHqlExpression * oldFieldRecord = oldField->queryRecord();
                IHqlExpression * value = queryRealChild(oldField, 0);
                OwnedHqlExpr newTargetSelector = createSelectExpr(LINK(targetSelector), LINK(newField));
                if (oldFieldRecord && !oldField->isDataset() && !value)
                {
                    if (convertTempTable)
                        convertRecordToAssigns(assigns, oldFieldRecord, newTargetSelector, canOmit, convertTempTable);
                    else
                    {
                        IHqlExpression * newRecord = newField->queryRecord();
                        OwnedHqlExpr newSelf = getSelf(newRecord);
                        HqlExprArray newAssigns;
                        convertRecordToAssigns(newAssigns, oldFieldRecord, newSelf, canOmit, convertTempTable);
                        IHqlExpression * transform = createValue(no_newtransform, makeTransformType(newRecord->getType()), newAssigns);
                        IHqlExpression * newValue = createRow(no_createrow, transform);
                        assigns.append(*createAssign(LINK(newTargetSelector), newValue));
                    }
                }
                else
                {
                    assertex(value || canOmit);
                    if (value && (!convertTempTable || exprIsSelfConstant(value)))
                        assigns.append(*createAssign(LINK(newTargetSelector), transform(value)));

                    if (oldFieldRecord && convertTempTable)
                        assigns.append(*createExprAttribute(defaultAtom, createExprAttribute(defaultAtom, LINK(newTargetSelector)), convertRecordToAssigns(oldFieldRecord, canOmit, convertTempTable)));
                }
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        default:
            UNIMPLEMENTED;
        }
    }
}

IHqlExpression * HqlTreeNormalizer::convertRecordToAssigns(IHqlExpression * oldRecord, bool canOmit, bool convertTempTable)
{
    OwnedHqlExpr newRecord = transform(oldRecord);
    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(newRecord);
    convertRecordToAssigns(assigns, oldRecord, self, canOmit, convertTempTable);
    return createValue(no_transform, makeTransformType(newRecord->getType()), assigns);
}

// Problems occur if a record used in a select fields is also used for some other linked purpose.
// Thankfully the only one so far is BUILDINDEX(index)
IHqlExpression * HqlTreeNormalizer::convertSelectToProject(IHqlExpression * newRecord, IHqlExpression * expr)
{
    OwnedHqlExpr newDataset = transform(expr->queryChild(0));
    IHqlExpression * oldRecord = expr->queryChild(1);
    if (oldRecord->getOperator() == no_null)
        return newDataset.getClear();

    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(newRecord);
    convertRecordToAssigns(assigns, oldRecord, self, false, false);

    OwnedHqlExpr newTransform = createValue(no_newtransform, makeTransformType(LINK(newRecord->queryRecordType())), assigns);
    newTransform.setown(newRecord->cloneAllAnnotations(newTransform));
    HqlExprArray args;
    args.append(*newDataset.getClear());
    args.append(*LINK(newRecord));
    args.append(*newTransform.getClear());
    unsigned numChildren = expr->numChildren();
    for (unsigned idx = 2; idx < numChildren; idx++)
        args.append(*transform(expr->queryChild(idx)));

    node_operator op = isAggregateDataset(expr) ? no_newaggregate : no_newusertable;
    OwnedHqlExpr project = createDataset(op, args);
    return expr->cloneAllAnnotations(project);
}


IHqlExpression * HqlTreeNormalizer::removeDefaultsFromExpr(IHqlExpression * expr, unsigned recordChildIndex, node_operator newOp)
{
    IHqlExpression * oldRecord = expr->queryChild(recordChildIndex);
    OwnedHqlExpr newRecord = transform(oldRecord);

    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(newRecord);
    convertRecordToAssigns(assigns, oldRecord, self, false, false);

    IHqlExpression * newTransform = createValue(no_newtransform, makeTransformType(LINK(newRecord->queryRecordType())), assigns);
    HqlExprArray args;
    args.append(*transform(expr->queryChild(0)));
    for (unsigned i= 1; i < recordChildIndex; i++)
        args.append(*transform(expr->queryChild(i)));
    args.append(*LINK(newRecord));
    args.append(*newTransform);
    unsigned numChildren = expr->numChildren();
    for (unsigned idx = recordChildIndex+1; idx < numChildren; idx++)
        args.append(*transform(expr->queryChild(idx)));

    OwnedHqlExpr project;
    if (expr->isDataset())
        project.setown(createDataset(newOp, args));
    else if (expr->isDatarow())
        project.setown(createRow(newOp, args));
    else
        project.setown(createValue(newOp, makeVoidType(), args));
    return expr->cloneAllAnnotations(project);
}


ITypeInfo * HqlTreeNormalizer::transformType(ITypeInfo * type)
{
    switch (type->queryModifier())
    {
    case typemod_original:
        {
            switch (type->getTypeCode())
            {
            case type_record:
            case type_transform:
            case type_row:
            case type_table:
            case type_groupedtable:
                //Strip all original annotations - they cause branches to not be commoned up
                return transformType(type->queryTypeBase());
            }
            //But keep annotations used for typedef information, they should probably work differently
            break;
        }
    case typemod_none:
        {
            //Ensure all records with the same format get the same original modifier
            Owned<ITypeInfo> transformedType = NewHqlTransformer::transformType(type);
            if (type->getTypeCode() == type_record)
            {
                IHqlExpression * record = queryExpression(type);
                if (record && record->getOperator() == no_record)
                {
                    OwnedHqlExpr transformedRecord = transform(record);
                    if (transformedRecord->queryBody() != transformedRecord)
                        return makeOriginalModifier(LINK(transformedType), LINK(transformedRecord));
                }
            }
            return transformedType.getClear();
        }
        break;
    }

    return NewHqlTransformer::transformType(type);
}

bool isVoidOrDatasetOrList(IHqlExpression * expr)
{
    ITypeInfo * type = expr->queryType();
    switch (type->getTypeCode())
    {
    case type_void:
    case type_table:
    case type_row:
    case type_groupedtable:
    case type_set:
        return true;
    default:
        return false;
    }
}


inline IHqlExpression * createColon(IHqlExpression * l, HqlExprArray & actions)
{
    HqlExprArray args;
    args.append(*l);
    ForEachItemIn(i, actions)
        args.append(OLINK(actions.item(i)));
    return createWrapper(no_colon, args);
}

void HqlTreeNormalizer::analyseExpr(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody();
    node_operator op = body->getOperator();

    if ((op == no_record) && (expr != body))
    {
        IHqlExpression * symbol = queryNamedSymbol(expr);
        if (symbol)
            queryLocationIndependentExtra(body)->noteSymbol(expr);
    }

    if (alreadyVisited(body))
        return;

    //Record a list of all USE(name[,name]) so we know what needs fixing up, and all patterns with explicit defines already
    switch (op)
    {
    case no_pat_use:
        if (body->hasAttribute(nameAtom))
            forwardReferences.append(*LINK(body));
        break;
    case no_pat_instance:
        if (body->queryChild(0)->getOperator() == no_define)
            defines.append(*LINK(body));
        break;
    case no_libraryscopeinstance:
        analyseExpr(body->queryDefinition());
        break;
    case no_transform:
    case no_call:
    case no_externalcall:
        {
            IHqlExpression * record = queryOriginalRecord(body->queryType());
            if (record)
                analyseExpr(record);
            break;
        }
    case no_attr_expr:
    case no_record:
    case no_ifblock:
    case no_select:
        analyseChildren(body);
        return;
    case no_field:
        {
            IHqlExpression * record = queryOriginalRecord(body->queryType());
            if (record)
                analyseExpr(record);
            analyseChildren(body);
            break;
        }
    }

    Parent::analyseExpr(body);
}


IHqlExpression * HqlTreeNormalizer::makeRecursiveName(IAtom * searchModule, IAtom * searchName)
{
    //If this symbol is already has a user define, use that instead of creating our own,
    //because I don't cope very well with multiple defines on the same pattern instance.
    ForEachItemIn(i, defines)
    {
        IHqlExpression & cur = defines.item(i);
        IHqlExpression * moduleExpr = cur.queryChild(2);
        IAtom * module = moduleExpr ? moduleExpr->queryBody()->queryName() : NULL;
        IAtom * name = cur.queryChild(1)->queryBody()->queryName();
        if (name == searchName && module == searchModule)
            return LINK(cur.queryChild(0)->queryChild(1));
    }
    StringBuffer s;
    s.append("$").append(searchModule).append(".").append(searchName);
    return createConstant(s.str());
}

IHqlExpression * HqlTreeNormalizer::queryTransformPatternDefine(IHqlExpression * expr)
{
    if (expr->queryChild(0)->getOperator() == no_define)
        return NULL;

    IHqlExpression * moduleExpr = expr->queryChild(2);
    IAtom * module = moduleExpr ? moduleExpr->queryBody()->queryName() : NULL;
    IAtom * name = expr->queryChild(1)->queryBody()->queryName();
    ForEachItemIn(i, forwardReferences)
    {
        IHqlExpression * cur = &forwardReferences.item(i);
        if ((name == queryPatUseName(cur)) && (module == queryPatUseModule(cur)))
        {
            IHqlExpression * base = transform(expr->queryChild(0));
            HqlExprArray args;
            args.append(*createValue(no_define, base->getType(), base, makeRecursiveName(module, name)));
            unwindChildren(args, expr, 1);
            return createValue(expr->getOperator(), transformType(expr->queryType()), args);
        }
    }
    return NULL;
}


IHqlExpression * HqlTreeNormalizer::transformActionList(IHqlExpression * expr)
{
    HqlExprArray args;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (cur->getOperator() != no_setmeta)
        {
            OwnedHqlExpr transformed = transform(cur);
            if ((transformed->getOperator() != no_null) || !transformed->isAction())
                args.append(*transformed.getClear());
        }
    }
    return expr->clone(args);
}


IHqlExpression * HqlTreeNormalizer::transformCaseToIfs(IHqlExpression * expr)
{
    unsigned max = numRealChildren(expr);
    OwnedHqlExpr testVar = transform(expr->queryChild(0));
    OwnedHqlExpr elseExpr = transform(expr->queryChild(max-1));
    for (unsigned idx = max-2; idx != 0; idx--)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        IHqlExpression * curValue = cur->queryChild(0);
        Owned<ITypeInfo> type = ::getPromotedECLType(testVar->queryType(), curValue->queryType());
        OwnedHqlExpr castCurValue = ensureExprType(curValue, type);

        OwnedHqlExpr test = createBoolExpr(no_eq, ensureExprType(testVar, type), transform(castCurValue));
        if (options.constantFoldNormalize)
            test.setown(foldConstantOperator(test, 0, NULL));
        OwnedHqlExpr trueExpr = transform(cur->queryChild(1));
        elseExpr.setown(createIf(test.getClear(), trueExpr.getClear(), elseExpr.getClear()));
        if (options.constantFoldNormalize)
            elseExpr.setown(foldConstantOperator(elseExpr, 0, NULL));
    }
    return elseExpr.getClear();
}

IHqlExpression * HqlTreeNormalizer::transformCaseToChoose(IHqlExpression * expr)
{
    //For the moment only convert datasets to choose format.  (Partly to test implementation.)
    //Datarows are unlikely to benefit, and will cause additional work.
    //Converting actions has implications for needing new activity kinds, and support in thor.
    if (!expr->isDataset())
        return transformCaseToIfs(expr);

    unsigned max = numRealChildren(expr);
    HqlExprArray branches;
    OwnedHqlExpr testVar = transform(expr->queryChild(0));

    HqlExprArray caseArgs;
    caseArgs.append(*LINK(testVar));
    bool isNullMapping = true;
    for (unsigned i1=1; i1 < max-1; i1++)
    {
        IHqlExpression * mapto = expr->queryChild(i1);
        OwnedHqlExpr key = transform(mapto->queryChild(0));
        OwnedHqlExpr branch = transform(mapto->queryChild(1));
        unsigned matchIndex = branches.find(*branch);
        if (matchIndex == NotFound)
        {
            matchIndex = branches.ordinality();
            branches.append(*branch.getClear());
        }
        OwnedHqlExpr value = getSizetConstant(matchIndex+1);
        //MORE: Could calculate a delta and add/subtract it from testVar
        if (!key->queryValue() || !matchesConstantValue(key, matchIndex+1))
            isNullMapping = false;

        caseArgs.append(*createValue(no_mapto, value->getType(), key.getClear(), LINK(value)));
    }
    caseArgs.append(*getSizetConstant(max-1));

    HqlExprArray args;
    if (isNullMapping)
        args.append(*LINK(testVar));
    else
        args.append(*createValue(no_case, LINK(sizetType), caseArgs));
    appendArray(args, branches);
    args.append(*transform(expr->queryChild(max-1)));

    if (expr->isDataset())
        return createDataset(no_chooseds, args);
    return createAction(no_choose, args);
}

IHqlExpression * HqlTreeNormalizer::transformEvaluate(IHqlExpression * expr)
{
    //Evaluate causes chaos - so translate it to a different form.
    //following cases supported so far:
    //EVALUATE(LEFT/RIGHT, g())  -> g(LEFT)
    //EVAlUATE(x, field)         -> x.field;
    //EVALUATE(t[n], e)          -> table(t,{f1 := e})[n].f1;

    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * attr = expr->queryChild(1);
    OwnedHqlExpr transformed;
    OwnedHqlExpr activeTable = getActiveTableSelector();
    if ((attr->getOperator() == no_select) && (attr->queryChild(0) == activeTable))
    {
        //EVAlUATE(x, field)         -> x.field;
        transformed.setown(createSelectExpr(LINK(ds), LINK(attr->queryChild(1))));
    }
    else if (attr->isConstant())
        transformed.set(attr);
    else
    {
        switch (ds->getOperator())
        {
        case no_left:
        case no_right:
            //EVALUATE(LEFT/RIGHT, g())  -> g(LEFT)
            //May change too many datasets?
            transformed.setown(replaceSelector(attr, activeTable, ds));
            break;
        case no_select:
            //EVALUATE(x.y, g())  -> EVALUATE(x, g(y))
            //May change too many datasets?
            transformed.setown(createValue(no_evaluate, attr->getType(), LINK(ds->queryChild(0)), replaceSelector(attr, activeTable, ds->queryChild(1))));
            break;
        case no_selectnth:
            {
                IHqlExpression * baseDs = ds->queryChild(0);
                if ((attr->getOperator() == no_select) && (attr->queryChild(0)->queryNormalizedSelector() == baseDs->queryNormalizedSelector()))
                {
                    //Special case a select same as field...
                    transformed.setown(createSelectExpr(LINK(ds), LINK(attr->queryChild(1))));
                }
                else
                {
                    //EVALUATE(t[n], e)          -> table(t,{f1 := e})[n].f1;
                    OwnedHqlExpr field = createField(valueId, expr->getType(), NULL);
                    IHqlExpression * aggregateRecord = createRecord(field);

                    IHqlExpression * newAttr = replaceSelector(attr, activeTable, baseDs);
                    IHqlExpression * assign = createAssign(createSelectExpr(getSelf(aggregateRecord), LINK(field)), newAttr);
                    IHqlExpression * transform = createValue(no_newtransform, makeTransformType(aggregateRecord->getType()), assign);

                    IHqlExpression * project = createDataset(no_newusertable, LINK(baseDs), createComma(aggregateRecord, transform));
                    project = createRow(no_selectnth, project, LINK(ds->queryChild(1)));
                    transformed.setown(createSelectExpr(project, LINK(field)));
                }
                break;
            }
        default:
            UNIMPLEMENTED;
        }
    }
    return transform(transformed);
}

IHqlExpression * HqlTreeNormalizer::transformMap(IHqlExpression * expr)
{
    unsigned max = numRealChildren(expr);
    OwnedHqlExpr elseExpr = transform(expr->queryChild(max-1));
    for (unsigned idx = max-1; idx-- != 0; )
    {
        IHqlExpression * cur = expr->queryChild(idx);
        elseExpr.setown(createIf(transform(cur->queryChild(0)), transform(cur->queryChild(1)), elseExpr.getClear()));
        if (options.constantFoldNormalize)
            elseExpr.setown(foldConstantOperator(elseExpr, 0, NULL));
    }
    return elseExpr.getClear();
}

IHqlExpression * HqlTreeNormalizer::transformTempRow(IHqlExpression * expr)
{
    ECLlocation dummyLocation(0, 0, 0, NULL);
    OwnedHqlExpr createRow = convertTempRowToCreateRow(translator.queryErrorProcessor(), dummyLocation, expr);
    return transform(createRow);
}

IHqlExpression * HqlTreeNormalizer::transformTempTable(IHqlExpression * expr)
{
    ECLlocation dummyLocation(0, 0, 0, NULL);
    OwnedHqlExpr inlineTable = convertTempTableToInlineTable(translator.queryErrorProcessor(), dummyLocation, expr);
    if (expr != inlineTable)
        return transform(inlineTable);

    IHqlExpression * oldValues = expr->queryChild(0);
    IHqlExpression * oldRecord = expr->queryChild(1);
    OwnedHqlExpr values = normalizeListCasts(oldValues);
    OwnedHqlExpr newRecord = transform(oldRecord);
    node_operator valueOp = values->getOperator();

    if ((valueOp != no_recordlist) && (valueOp != no_list))
    {
        if (queryRealChild(expr, 2))
            return Parent::createTransformed(expr);

        HqlExprArray children;
        children.append(*transform(oldValues));
        children.append(*LINK(newRecord));
        children.append(*convertRecordToAssigns(oldRecord, true, true));
        return expr->clone(children);
    }

    //should have already been handled by convertTempTableToInlineTable();
    throwUnexpected();
}



IHqlExpression * HqlTreeNormalizer::transformNewKeyIndex(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    //If dataset is already null, then do standard
    if (ds->getOperator() == no_null)
        return completeTransform(expr);

    //Before we do anything replace the dataset with a null dataset.  This ensures we do the minimum transformation on the rest of the tree
    OwnedHqlExpr newDs = createDataset(no_null, LINK(ds->queryRecord()));
    HqlExprArray args;
    args.append(*ds->cloneAllAnnotations(newDs));
    args.append(*LINK(expr->queryChild(1)));
    args.append(*quickFullReplaceExpression(expr->queryChild(2), ds->queryNormalizedSelector(), newDs));
    unwindChildren(args, expr, 3);
    OwnedHqlExpr ret = expr->clone(args);
    return transform(ret);
}


IHqlExpression * HqlTreeNormalizer::transformKeyIndex(IHqlExpression * expr)
{
    //Before we do anything replace the dataset with a null dataset.  This ensures we do the minimum transformation on the rest of the tree
    IHqlExpression * ds = expr->queryChild(0);
    OwnedHqlExpr newDs = createDataset(no_null, LINK(ds->queryRecord()));
    HqlExprArray args;
    args.append(*ds->cloneAllAnnotations(newDs));
    args.append(*quickFullReplaceExpression(expr->queryChild(1), ds->queryNormalizedSelector(), newDs));
    unwindChildren(args, expr, 2);
    OwnedHqlExpr normalized = expr->clone(args);

    //Now convert from the no_keyindex format to the no_newkeyindex format.
    //force the 1st argument to be processed..
    OwnedHqlExpr transformed = completeTransform(normalized);

    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(transformed);
    convertRecordToAssigns(assigns, normalized->queryChild(1), self, true, false);      // fpos may not have a value...

    args.kill();
    unwindChildren(args, transformed);
    args.add(*createValue(no_newtransform, makeTransformType(transformed->queryChild(1)->getType()), assigns), 2);

    OwnedHqlExpr ret = createDataset(no_newkeyindex, args);

    //MORE: This would be the place to add a FILTERED() attribute derived from any filters applied to the dataset
    return expr->cloneAllAnnotations(ret);
}


IHqlExpression * HqlTreeNormalizer::transformMerge(IHqlExpression * expr)
{
    HqlExprArray children;
    transformChildren(expr, children);

    HqlExprArray args;
    reorderAttributesToEnd(args, children);
    return expr->clone(args);
}


IHqlExpression * HqlTreeNormalizer::transformPatNamedUse(IHqlExpression * expr)
{
    OwnedHqlExpr define = makeRecursiveName(queryPatUseModule(expr), queryPatUseName(expr));
    HqlExprArray args;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = queryRealChild(expr, i);
        if (cur)
            args.append(*LINK(cur));
    }
    args.append(*define.getClear());
    ITypeInfo * type = expr->queryType();
    return createValue(no_pat_use, transformType(type), args);
}

IHqlExpression * HqlTreeNormalizer::transformPatCheckIn(IHqlExpression * expr)
{
    OwnedHqlExpr set = transform(expr->queryChild(1));
    //because this is a check pattern, we are free to remove any instance tags - they can't be used for matching.
    while (set->getOperator() == no_pat_instance)
        set.set(set->queryChild(0));

    if (set->getOperator() == no_pat_set)
    {
        IHqlExpression * notAttr = expr->queryAttribute(notAtom);
        if (!notAttr)
            return LINK(set);
        HqlExprArray args;
        unwindChildren(args, set);
        if (args.find(*notAttr) == NotFound)
            args.append(*LINK(notAttr));
        else
            args.zap(*notAttr);
        return set->clone(args);
    }

    HqlExprArray values, newValues;
    set->unwindList(values, no_pat_or);

    ForEachItemIn(idx, values)
    {
        IHqlExpression * cur = &values.item(idx);
        while (cur->getOperator() == no_pat_instance)
            cur = cur->queryChild(0);
        if (cur->getOperator() != no_pat_const)
            return NULL;
        IValue * value = cur->queryChild(0)->queryValue();
        if (!value)
            return NULL;
        ITypeInfo * type = value->queryType();
        if (type->getStringLen() != 1)
            return NULL;
        switch (type->getTypeCode())
        {
        case type_string:
            newValues.append(*createConstant((int)*(const byte *)value->queryValue()));
            break;
        case type_unicode:
            newValues.append(*createConstant((int)*(const UChar *)value->queryValue()));
            break;
        case type_utf8:
            newValues.append(*createConstant((int)rtlUtf8Char((const char *)value->queryValue())));
            break;
        default:
            return NULL;
        }
    }

    if (expr->hasAttribute(notAtom))
        newValues.append(*createAttribute(notAtom));
    return createValue(no_pat_set, makePatternType(), newValues);
}

IHqlExpression * HqlTreeNormalizer::transformTable(IHqlExpression * untransformed)
{
    //Convert DATASET('xx', rec, PIPE('z'))
    //DATASET('xx', rec, THOR) | PIPE('z')
    OwnedHqlExpr transformed = completeTransform(untransformed);

    IHqlExpression * mode = transformed->queryChild(2);
    if (mode->getOperator() != no_pipe)
        return transformed.getClear();

    IHqlExpression * filename = transformed->queryChild(0);
    StringBuffer s;
    if (getStringValue(s, filename, NULL).length() == 0)
        return transformed.getClear();

    OwnedHqlExpr modeThor = createValue(no_thor);
    IHqlExpression * diskRead = replaceChild(transformed, 2, modeThor);
    HqlExprArray args;
    args.append(*diskRead);
    unwindChildren(args, mode);
    return createDataset(no_pipe, args);
}

IHqlExpression * HqlTreeNormalizer::optimizeAssignSkip(HqlExprArray & children, IHqlExpression * expr, IHqlExpression * cond, unsigned depth)
{
    if (!containsSkip(expr))
        return LINK(expr);

    switch (expr->getOperator())
    {
    case no_skip:
        children.append(*createValue(no_skip, makeVoidType(), LINK(cond)));
        return NULL;
    case no_cast:
    case no_implicitcast:
        {
            bool same= true;
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                IHqlExpression * ret = optimizeAssignSkip(children, cur, cond, depth);
                if (!ret)
                    return NULL;
                args.append(*ret);
                if (cur != ret)
                    same = false;
            }
            if (same)
                return LINK(expr);
            return expr->clone(args);
        }
        //could try and handle map/case/choose, but less common, and more complicated.
    case no_if:
        {
            //For the moment only hoist SKIPS within a single level of IF() conditions.
            //Multi level rarely occur, and don't significantly improve the code
            if (depth != 0)
                return LINK(expr);

            IHqlExpression * thisCond = expr->queryChild(0);
            IHqlExpression * left = expr->queryChild(1);
            IHqlExpression * right = expr->queryChild(2);
            if (!right)
                return LINK(expr);

            OwnedHqlExpr leftCond = extendConditionOwn(no_and, LINK(cond), LINK(thisCond));
            OwnedHqlExpr inverseCond = createValue(no_not, makeBoolType(), LINK(thisCond));
            OwnedHqlExpr rightCond = extendConditionOwn(no_and, LINK(cond), LINK(inverseCond));
            OwnedHqlExpr newLeft = optimizeAssignSkip(children, left, leftCond, depth+1);
            OwnedHqlExpr newRight = optimizeAssignSkip(children, right, rightCond, depth+1);
            if (!newLeft && !newRight)
                return NULL;
            //if cond is true, then it will skip => no need to check the condition
            if (!newLeft)
                return LINK(newRight);
            if (!newRight)
                return LINK(newLeft);
            if (left == newLeft && right == newRight)
                return LINK(expr);
            HqlExprArray args;
            unwindChildren(args, expr);
            args.replace(*newLeft.getClear(), 1);
            args.replace(*newRight.getClear(), 2);
            return expr->clone(args);
        }
    default:
        return LINK(expr);
    }
}
bool HqlTreeNormalizer::transformTransform(HqlExprArray & children, IHqlExpression * expr)
{
    bool same = true;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            transformTransform(children, cur);
            same = false;   // assign all is removed and assigns expanded in its place
            break;
        case no_assign:
            {
                OwnedHqlExpr assign = transform(cur);
                if (cur->getInfoFlags() & HEFcontainsSkip)
                {
                    IHqlExpression * rhs = assign->queryChild(1);
                    OwnedHqlExpr newRhs = optimizeAssignSkip(children, rhs, NULL, 0);
                    if (rhs != newRhs)
                    {
                        IHqlExpression * lhs = assign->queryChild(0);
                        if (!newRhs)
                            newRhs.setown(createNullExpr(rhs));
                        assign.setown(createAssign(LINK(lhs), newRhs.getClear()));
                    }
                }
                if (assign != cur)
                    same = false;
                children.append(*assign.getClear());
                break;
            }
        default:
            {
                IHqlExpression * next = transform(cur);
                children.append(*next);
                if (next != cur)
                    same = false;
            }
            break;
        }
    }
    return same;
}

IHqlExpression * HqlTreeNormalizer::transformTransform(IHqlExpression * expr)
{
    HqlExprArray children;
    IHqlExpression * oldRecord = queryOriginalRecord(expr);
    OwnedHqlExpr newRecord = transform(oldRecord);

    bool same = transformTransform(children, expr);
    if ((oldRecord != newRecord) || !same)
    {
        ITypeInfo * newRecordType = createRecordType(newRecord);
        OwnedHqlExpr ret = createValue(expr->getOperator(), makeTransformType(newRecordType), children);
        return expr->cloneAllAnnotations(ret);
    }
    return LINK(expr);
}


IHqlExpression * HqlTreeNormalizer::transformIfAssert(node_operator newOp, IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    HqlExprArray children;
    bool same = transformChildren(expr, children);
    if ((expr->hasAttribute(assertAtom) || (options.assertSortedDistributed && (newOp != no_assertgrouped))) && !options.removeAsserts)
    {
        OwnedHqlExpr ret = createDataset(newOp, children);
        return expr->cloneAllAnnotations(ret);
    }
    if (!same)
        return expr->clone(children);
    return LINK(expr);
}

IHqlExpression * HqlTreeNormalizer::transformExecuteWhen(IHqlExpression * expr)
{
    OwnedHqlExpr transformedAction = transform(expr->queryChild(1));
    if ((transformedAction->getOperator() == no_setmeta) ||
        ((transformedAction->getOperator() == no_null) && transformedAction->isAction()))
        return transform(expr->queryChild(0));

    HqlExprArray children;
    if (translator.queryOptions().convertWhenExecutedToCompound && !expr->queryChild(2))
    {
        //For the moment, for maximal compatibility, convert no_executewhen to a no_compound
        children.append(*transformedAction.getClear());
        children.append(*transform(expr->queryChild(0)));
        OwnedHqlExpr ret = createCompound(children);
        return expr->cloneAllAnnotations(ret);
    }

    //Need to create a unique id to differentiate the different side effects.
    transformChildren(expr, children);
    assertex(!expr->hasAttribute(_uid_Atom));
    children.append(*createUniqueId());
    return expr->clone(children);
}

IHqlExpression * HqlTreeNormalizer::transformWithinFilter(IHqlExpression * expr)
{
    OwnedHqlExpr ds = transform(expr->queryChild(0));

    HqlExprArray children;
    children.append(*LINK(ds));
    ForEachChildFrom(i, expr, 1)
    {
        IHqlExpression * filter = expr->queryChild(i);
        if (filter->getOperator() == no_within)
        {
            IHqlExpression * scope = filter->queryChild(0);
            while (scope->getOperator() == no_filter)
            {
                ForEachChildFrom(i2, scope, 1)
                    children.append(*transform(scope->queryChild(i2)));
                scope = scope->queryChild(0);
            }
            ds.setown(createDataset(no_related, LINK(ds), transform(scope)));
        }
        else
            children.append(*transform(filter));
    }

    if (children.ordinality() == 1)
        return ds.getClear();

    children.replace(*ds.getClear(), 0);
    return expr->clone(children);
}


IHqlExpression * HqlTreeNormalizer::validateKeyedJoin(IHqlExpression * expr)
{
    //Transform join(x, local(key), ....) to join(x, key, ...., local);
    HqlExprArray children;
    transformChildren(expr, children);

    unsigned prevChildren = children.ordinality();
    IHqlExpression * rhs = &children.item(1);
    loop
    {
        node_operator op = rhs->getOperator();
        if (op == no_forcelocal)
            children.append(*createLocalAttribute());
        else if (op == no_forcenolocal)
            children.append(*createAttribute(noLocalAtom));
        else if ((op == no_section) || (op == no_sectioninput))
        {
            //remove the section
        }
        else
            break;
        rhs = rhs->queryChild(0);
    }

    if (prevChildren != children.ordinality())
    {
        if (isKey(rhs))
            children.replace(*LINK(rhs), 1);
        else
            children.trunc(prevChildren);
    }

    //Now check that a join marked as keyed has a key as the rhs.
    IHqlExpression * keyed = expr->queryAttribute(keyedAtom);
    if (!keyed || keyed->queryChild(0) || isKey(rhs))
        return expr->clone(children);

    if (options.allowActivityForKeyedJoin)
    {
        children.append(*createAttribute(_complexKeyed_Atom));
        return expr->clone(children);
    }

    StringBuffer s;
    if (expr->queryName())
        s.append(" (").append(expr->queryName()).append(")");
    throwError1(HQLERR_RhsKeyedNotKey, s.str());
    return NULL;
}


//A bit of a nasty dependency - this should match the capabilities of the code in hqlsource for finding selectors
static void gatherPotentialSelectors(HqlExprArray & args, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_and:
    case no_or:
    case no_eq:
    case no_ne:
    case no_gt:
    case no_lt:
    case no_ge:
    case no_le:
        gatherPotentialSelectors(args, expr->queryChild(0));
        gatherPotentialSelectors(args, expr->queryChild(1));
        break;
    case no_if:
    case no_case:
    case no_map:
    case no_mapto:
        {
            ForEachChild(i, expr)
                gatherPotentialSelectors(args, expr->queryChild(i));
            break;
        }
    case no_assertkeyed:
    case no_assertstepped:
    case no_not:
    case no_between:
    case no_notbetween:
    case no_cast:
    case no_implicitcast:
    case no_notin:
    case no_in:
    case no_add:
    case no_sub:
    case no_substring:
        gatherPotentialSelectors(args, expr->queryChild(0));
        break;
    case no_select:
        {
            IHqlExpression * selector = expr->queryNormalizedSelector();
            if (!args.contains(*selector))
                args.append(*LINK(selector));
            break;
        }
    }
}


IHqlExpression * HqlTreeNormalizer::transformChildrenNoAnnotations(IHqlExpression * expr)
{
    HqlExprArray args;
    ForEachChild(i, expr)
    {
        OwnedHqlExpr newChild = transform(expr->queryChild(i)->queryBody());
        args.append(*LINK(newChild->queryBody()));
    }
    return completeTransform(expr, args);
}

//The following symbol removal code works, but I'm not sure I want to do it at the moment because of the changes to the HOLe queries
//Remove as many named symbols as we can - try and keep for datasets and statements so can go in the tree.
IHqlExpression * HqlTreeNormalizer::createTransformed(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody(true);
    node_operator op = expr->getOperator();
    if (expr != body)
    {
        OwnedHqlExpr transformedBody;
        try
        {
            transformedBody.setown(transform(body));
        }
        catch (IException * e)
        {
            if (dynamic_cast<IError *>(e))
                throw;
            IHqlExpression * location = queryLocation(expr);
            if (location)
            {
                IError * error = annotateExceptionWithLocation(e, location);
                e->Release();
                throw error;
            }
            throw;
        }

        //Don't retain any annotations on records - except for a symbol which maybe added in the createTransform()
        //code.  Otherwise expressions that would otherwise be commoned up are treated as different.
        if (op == no_record)
            return transformedBody.getClear();

        switch (expr->getAnnotationKind())
        {
        case annotate_warning:
        case annotate_parsemeta:
            return transformedBody.getClear();
        case annotate_javadoc:
            return expr->cloneAnnotation(transformedBody);
        case annotate_meta:
            {
                HqlExprArray preservedMeta;
                IHqlExpression * cur;
                bool changed = false;
                for (unsigned i=0; (cur = expr->queryAnnotationParameter(i)) != 0; i++)
                {
                    IAtom * name = cur->queryName();
                    bool keep = true;
                    if (name == deprecatedAtom)
                        keep = false;
                    else if (!options.allowSections && (name == sectionAtom))
                        keep = false;

                    if (keep)
                        preservedMeta.append(*LINK(cur));
                    else
                        changed = true;
                }
                if (changed)
                {
                    if (preservedMeta.ordinality() == 0)
                        return transformedBody.getClear();
                    return createMetaAnnotation(transformedBody.getClear(), preservedMeta);
                }
                break; // default action
            }
        case annotate_symbol:
            {
                if (hasNamedSymbol(transformedBody))
                    return transformedBody.getClear();

                IIdAtom * id = expr->queryId();
                IIdAtom * simpleId = simplifySymbolName(id, options.commonUniqueNameAttributes);
                if (simpleId)
                    return cloneSymbol(expr, simpleId, transformedBody, NULL, NULL);
                break;
            }
        } // switch(kind)
        if (body == transformedBody)
            return LINK(expr);
        return expr->cloneAnnotation(transformedBody);
    }

    //MORE: Types of all pattern attributes should also be normalized.  Currently they aren't which causes discrepancies between types
    //for ghoogle.hql.  It could conceivably cause problems later on.
    if (forwardReferences.ordinality())
    {
        if (op == no_pat_use && expr->hasAttribute(nameAtom))
            return transformPatNamedUse(expr);
        if (op == no_pat_instance)
        {
            OwnedHqlExpr ret = queryTransformPatternDefine (expr);
            if (ret)
                return ret.getClear();
        }
    }

    IHqlExpression * sideEffects = expr->queryAttribute(_sideEffect_Atom);
    if (sideEffects)
    {
        HqlExprArray args;
        unwindChildren(args, expr);
        args.zap(*sideEffects);
        OwnedHqlExpr next = createCompound(LINK(sideEffects->queryChild(0)), expr->clone(args));
        return transform(next);
    }

    if (!options.constantFoldNormalize)
        return createTransformedBody(expr);

    switch (op)
    {
    case no_if:
        {
            OwnedHqlExpr cond = transform(expr->queryChild(0));
            IValue * condValue = cond->queryValue();
            if (condValue)
            {
                unsigned idx = condValue->getBoolValue() ? 1 : 2;
                IHqlExpression * branch = expr->queryChild(idx);
                if (branch)
                    return transform(branch);
                assertex(expr->isAction());
                return createValue(no_null, makeVoidType());
            }
            break;
        }
    case no_choose:
    case no_chooseds:
        {
            OwnedHqlExpr cond = transform(expr->queryChild(0));
            IValue * condValue = cond->queryValue();
            if (condValue)
            {
                unsigned idx = (unsigned)condValue->getIntValue();
                IHqlExpression * branch = queryRealChild(expr, idx);
                if (branch)
                    return transform(branch);
                IHqlExpression * defaultExpr = queryLastNonAttribute(expr);
                return transform(defaultExpr);
            }
            break;
        }
    case no_and:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            OwnedHqlExpr simpleRight = transformSimpleConst(right);
            if (simpleRight->queryValue())
            {
                if (simpleRight->queryValue()->getBoolValue())
                    return transform(left);
                return simpleRight.getClear();
            }
            OwnedHqlExpr newLeft = transform(left);
            IValue * leftValue = newLeft->queryValue();
            if (leftValue)
            {
                if (!leftValue->getBoolValue())
                    return newLeft.getClear();
                return transform(right);
            }
            break;
        }
    case no_or:
        {
            IHqlExpression * left = expr->queryChild(0);
            IHqlExpression * right = expr->queryChild(1);
            OwnedHqlExpr simpleRight = transformSimpleConst(right);
            if (simpleRight->queryValue())
            {
                if (!simpleRight->queryValue()->getBoolValue())
                    return transform(left);
                return simpleRight.getClear();
            }
            OwnedHqlExpr newLeft = transform(left);
            IValue * leftValue = newLeft->queryValue();
            if (leftValue)
            {
                if (leftValue->getBoolValue())
                    return newLeft.getClear();
                return transform(right);
            }
            break;
        }
    case  no_attr:
        if (expr->queryName() == _original_Atom)
            return LINK(expr);
        break;
    }
    OwnedHqlExpr transformed = createTransformedBody(expr);
    return foldConstantOperator(transformed, 0, NULL);
}

IHqlExpression * HqlTreeNormalizer::createTransformedBody(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_constant:
        return LINK(expr);          // avoid creating an array in default code...
    case no_case:
        if (isVoidOrDatasetOrList(expr) || expr->isDictionary())
            return transformCaseToChoose(expr);
        break;
    case no_map:
        if (isVoidOrDatasetOrList(expr) || expr->isDictionary())
            return transformMap(expr);
        break;
    case no_transform:
        //optimize location of skips
        return transformTransform(expr);
    case no_getresult:
    case no_newtransform:
        {
            IHqlExpression * record = queryOriginalRecord(expr);
            if (record)
                ::Release(transform(record));

            LinkedHqlExpr cleaned = expr;
            //remove any no_assignall children...  could really do for no_transform as well... would reduce clarity of graph ecl
            if ((op == no_newtransform) && queryChildOperator(no_assignall, expr))
            {
                HqlExprArray args;
                ForEachChild(i, expr)
                    expr->queryChild(i)->unwindList(args, no_assignall);
                cleaned.setown(expr->clone(args));
            }
            return Parent::createTransformed(cleaned);
        }
    case no_usertable:
    case no_selectfields:
        {
            OwnedHqlExpr newRecord = transform(expr->queryChild(1));
            return convertSelectToProject(newRecord, expr);
        }
    case no_parse:
        return removeDefaultsFromExpr(expr, 3, no_newparse);
    case no_xmlparse:
        return removeDefaultsFromExpr(expr, 2, no_newxmlparse);
    case no_soapcall:
        return removeDefaultsFromExpr(expr, 2, no_newsoapcall);
    case no_soapcall_ds:
        return removeDefaultsFromExpr(expr, 3, no_newsoapcall_ds);
    case no_soapaction_ds:
        return removeDefaultsFromExpr(expr, 3, no_newsoapaction_ds);
#ifdef OPTIMIZE_IMPLICIT_CAST
    //Following is a good idea, but makes some things worse because of the way we currently spot table invariants.
    case no_implicitcast:
        {
            OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
            return ensureExprType(transformed->queryChild(0), transformed->queryType());
        }
#endif
    case no_record:
        {
            OwnedHqlExpr transformed = completeTransform(expr);
            if (transformed->hasAttribute(packedAtom))
                transformed.setown(getPackedRecord(transformed));

            if (options.ensureRecordsHaveSymbols)
            {
                //Ensure all records only have a single unique name, and transform it here so that record types also map to that unique name
                IHqlExpression * recordSymbol = queryLocationIndependentExtra(expr)->symbol;
                if (recordSymbol)
                {
                    IIdAtom * id = recordSymbol->queryId();
                    IIdAtom * simpleId = simplifySymbolName(id, options.commonUniqueNameAttributes);
                    IIdAtom * newid = simpleId ? simpleId : id;
                    IIdAtom * lowerId = lowerCaseSymbolName(newid);
                    return createSymbol(lowerId, transformed.getClear(), ob_private);
                }
            }
            return transformed.getClear();
        }
    case no_left:
    case no_right:
    case no_top:
    case no_self:
        {
            IHqlExpression * record = expr->queryChild(0);
            //If no record argument then make sure the type is transformed
            if (!record)
                break;

            HqlExprArray children;
            //Ensure that the first parameter to one of these nodes is the body of the record, not a named symbol
            OwnedHqlExpr transformedRecord = transform(record);
            children.append(*LINK(transformedRecord->queryBody()));
            return completeTransform(expr, children);
        }
    case no_field:
        {
            //Remove the default values...
            HqlExprArray children;
            bool same = true;
            ForEachChild(idx, expr)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                if (cur->isAttribute())
                {
                    IHqlExpression * transformed = transform(cur);
                    children.append(*transformed);
                    if (cur != transformed)
                        same = false;
                }
                else
                    same = false;
            }

            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            IIdAtom * id = expr->queryId();
            IIdAtom * newId = lowerCaseSymbolName(id);
            if ((type != newType) || (newId != id))
                return createField(newId, newType.getClear(), children);

            if (same)
                return LINK(expr);
            return expr->clone(children);
        }
    case no_trim:
        //TRIM(x,RIGHT) should be represented the same way as TRIM(x) - and it's more efficient
        if ((expr->numChildren() == 2) && (expr->queryChild(1)->queryName() == rightAtom))
        {
            HqlExprArray children;
            children.append(*transform(expr->queryChild(0)));
            return expr->clone(children);
        }
        break;
    case no_pat_pattern:
        return LINK(expr->queryChild(1));
    case no_temptable:
        return transformTempTable(expr);
    case no_temprow:
        return transformTempRow(expr);
    case no_keyindex:
        return transformKeyIndex(expr);
    case no_newkeyindex:
//      seenIndex = true;
        return transformNewKeyIndex(expr);
    case no_table:
        if (expr->hasAttribute(localUploadAtom))
            seenLocalUpload = true;
        return transformTable(expr);
    case no_pat_checkin:
        if (expr->queryChild(0)->getOperator() == no_pat_anychar)
        {
            IHqlExpression * transformed = transformPatCheckIn(expr);
            if (transformed)
                return transformed;
        }
        break;
    case no_denormalize:
    case no_denormalizegroup:
        {
            OwnedHqlExpr transformed = validateKeyedJoin(expr);
            //Explicitly add a left outer flag to a denormalize if no other join type is specified.
            //Do here rather than in parser so crc for persists isn't changed.
            if (!transformed->hasAttribute(innerAtom) &&
                !transformed->hasAttribute(leftonlyAtom) && !transformed->hasAttribute(leftouterAtom) &&
                !transformed->hasAttribute(rightonlyAtom) && !transformed->hasAttribute(rightouterAtom) &&
                !transformed->hasAttribute(fullonlyAtom) && !transformed->hasAttribute(fullouterAtom))
            {
                return appendOwnedOperand(transformed, createAttribute(leftouterAtom));
            }
            return transformed.getClear();
        }
    case no_colon:
        {
            OwnedHqlExpr transformed = Parent::createTransformed(expr);
            LinkedHqlExpr value = transformed->queryChild(0);

            bool same = true;
            bool needToPreserveOriginal = false;
            HqlExprArray actions, scheduleActions;
            unwindChildren(actions, transformed, 1);
            ForEachItemInRev(i, actions)
            {
                IHqlExpression & cur = actions.item(i);
                IHqlExpression * replacement = NULL;
                switch (cur.getOperator())
                {
                case no_global:
                    {
                        HqlExprArray scopeArgs;
                        scopeArgs.append(*LINK(value));
                        unwindChildren(scopeArgs, &cur);
                        replacement = createWrapper(no_globalscope, scopeArgs);
                        break;
                    }
                case no_persist:
                    {
                        needToPreserveOriginal = true;
                        same = false;
                        break;
                    }
                case no_attr:
                case no_attr_expr:
                case no_attr_link:
                    if (cur.queryName() == defineAtom)
                        replacement = createValue(no_define, transformed->getType(), LINK(value), LINK(cur.queryChild(0)));
                    break;
                    //Separate scheduled items into a separate no_colon.
                case no_when:
                case no_priority:
                    scheduleActions.append(OLINK(cur));
                    actions.remove(i);
                    same = false;
                    break;
                }
                if (replacement)
                {
                    value.setown(replacement);
                    actions.remove(i);
                    same = false;
                }
            }

            if (same)
                return transformed.getClear();

            if (needToPreserveOriginal)
                actions.append(*createAttribute(_original_Atom, LINK(expr->queryChild(0))));

            OwnedHqlExpr result;
            if (actions.ordinality() == 0)
                result.set(value);
            else
                result.setown(createColon(LINK(value), actions));

            if (scheduleActions.ordinality())
                result.setown(createColon(result.getClear(), scheduleActions));

            return result.getClear();
        }
    case no_evaluate:
        return transformEvaluate(expr);
    case no_selectnth:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if (isGrouped(ds))
            {
                OwnedHqlExpr newChild = createDataset(no_group, LINK(ds));
                OwnedHqlExpr mapped = replaceChild(expr, 0, newChild);
                return transform(mapped);
            }
            break;
        }
    case no_assert_ds:
        if (options.removeAsserts)
            return transform(expr->queryChild(0));
        break;
    case no_section:
    case no_sectioninput:
        if (!options.allowSections)
            return transform(expr->queryChild(0));
        break;
    case no_type:
        return transformAlienType(expr);
    case no_param:
        {
            //no_param may be retained by library call definitions + they need the type transforming for consistency
            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            if (type != newType)
            {
                //Attributes shouldn't need transforming, but simplest
                HqlExprArray attrs;
                transformChildren(expr, attrs);
                return createParameter(expr->queryId(), (unsigned)expr->querySequenceExtra(), newType.getClear(), attrs);
            }
            break;
        }
    case no_libraryscope:
        {
            OwnedHqlExpr ret = transformScope(expr);
            if (translator.targetHThor())
                return appendOwnedOperand(ret, createAttribute(_noStreaming_Atom));
            return ret.getClear();
        }
    case no_virtualscope:
        return transformScope(expr);
    case no_libraryscopeinstance:
        {
            IHqlExpression * oldFunction = expr->queryDefinition();
            OwnedHqlExpr newFunction = transform(oldFunction);
            HqlExprArray children;
            bool same = true;
            ForEachChild(i, expr)
            {
                LinkedHqlExpr cur = expr->queryChild(i);
                if (cur->getOperator() == no_virtualscope)
                {
                    cur.setown(checkCreateConcreteModule(NULL, cur, cur->queryAttribute(_location_Atom)));
                    assertex(cur->getOperator() != no_virtualscope);
                    same = false;
                }
                else if (cur->getOperator() == no_purevirtual)
                {
                    IAtom * name = cur->queryName();
                    throwError1(HQLERR_LibraryMemberArgNotDefined, name ? str(name) : "");
                }
                IHqlExpression * transformed = transform(cur);
                children.append(*transformed);
                if (cur != transformed)
                    same = false;
            }
            if (same && (oldFunction == newFunction))
                return LINK(expr);
            return createLibraryInstance(newFunction.getClear(), children);
        }
    case no_transformascii:
    case no_transformebcdic:
        {
            HqlExprArray children;
            transformChildren(expr, children);
            OwnedHqlExpr transformed = createDataset(no_hqlproject, children);
            return transform(transformed);
        }
    case no_join:
        {
            OwnedHqlExpr transformed = validateKeyedJoin(expr);
            if (isSelfJoin(expr))
            {
                HqlExprArray children;
                unwindChildren(children, transformed);
                children.replace(*createAttribute(_selfJoinPlaceholder_Atom), 1);       // replace the 1st dataset with an attribute so parameters are still in the same place.
                return createDataset(no_selfjoin, children);
            }
            if (isKeyedJoin(transformed) && translator.targetRoxie() && !expr->hasAttribute(_ordered_Atom))
                return appendOwnedOperand(transformed, createAttribute(_ordered_Atom));
            return transformed.getClear();
        }
    case no_projectrow:
        {
            //Work around a problem where left is ambiguous - either outer LEFT, or left within this ROW
            //Not a full solution since PROJECT(PROJECT(LEFT),t(LEFT)) where project(LEFT) doesn't change types
            //would suffer from the same problem.
            //Remove as many instances of PROJECT(row, transform) as we can since ROW(transform) is handled more efficient.
            HqlExprArray children;
            OwnedHqlExpr ds = transform(expr->queryChild(0));
            node_operator dsOp = ds->getOperator();
            if (dsOp == no_left)
//          if (isAlwaysActiveRow(ds))
            {
                //MORE: The call to replaceExpression below isn't actually correct unless selectors are unique
                //this optimization may have to move elsewhere.
                OwnedHqlExpr newTransform = transform(expr->queryChild(1));
                OwnedHqlExpr newSel = transform(querySelSeq(expr));
                OwnedHqlExpr myLeft = createSelector(no_left, ds, newSel);
                OwnedHqlExpr replaced = quickFullReplaceExpression(newTransform, myLeft, ds);
                return createRow(no_createrow, LINK(replaced));
            }

            children.append(*ds.getClear());
            return completeTransform(expr, children);
        }
    case no_sorted:
        return transformIfAssert(no_assertsorted, expr);
    case no_grouped:
        return transformIfAssert(no_assertgrouped, expr);
    case no_distributed:
        //remove distributed(x)
        if (expr->hasAttribute(unknownAtom))
            return transform(expr->queryChild(0));
        return transformIfAssert(no_assertdistributed, expr);
#if defined(MAP_PROJECT_TO_USERTABLE)
    case no_hqlproject:
        if (!isCountProject(expr))
        {
            HqlExprArray children;
            transformChildren(expr, children);
            IHqlExpression * ds = &children.item(0);
            OwnedHqlExpr left = createSelector(no_left, ds, querySelSeq(expr));
            OwnedHqlExpr mapped = replaceExpression(&children.item(1), left, ds->queryNormalizedSelector());
            children.add(*LINK(mapped->queryRecord()), 1);

            HqlExprArray assigns;
            unwindChildren(assigns, mapped);
            children.replace(*createValue(no_newtransform, mapped->getType(), assigns), 2);
            OwnedHqlExpr transformed = createDataset(no_newusertable, children);
            return transform(transformed);
        }
        break;
#endif
    case no_comma:
    case no_compound:
        if (expr->queryChild(0)->getOperator() == no_setmeta)
            return transform(expr->queryChild(1));
        if ((op == no_compound) && expr->isAction())
        {
            HqlExprArray args;
            expr->unwindList(args, no_compound);
            OwnedHqlExpr compound = createAction(no_actionlist, args);
            return transform(compound);
        }
        break;
    case no_actionlist:
    case no_orderedactionlist:
        return transformActionList(expr);
    case no_forcelocal:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
        seenForceLocal = true;
        break;
    case no_enth:
        {
            HqlExprArray children;
            bool same = transformChildren(expr, children);
            IHqlExpression * denom = queryRealChild(expr, 2);
            if (!denom && !expr->queryAttribute(localAtom))
            {
                children.add(*createValue(no_count, LINK(defaultIntegralType), LINK(&children.item(0))), 2);
                same = false;
            }
            if (!same)
                return expr->clone(children);
            return LINK(expr);
        }
    case no_assertconstant:
        {
            IHqlExpression * child = expr->queryChild(0);
            OwnedHqlExpr ret = transform(child);
            OwnedHqlExpr folded  = foldHqlExpression(translator.queryErrorProcessor(), ret, NULL, HFOforcefold);
            if (!folded->isConstant())
            {
                StringBuffer s;
                getExprECL(child, s);
                translator.ERRORAT1(expr->queryChild(1), HQLERR_ExpectedConstant, s.str());
            }
            return folded.getClear();
        }
    case no_assertconcrete:
        {
            ECLlocation errpos;
            errpos.extractLocationAttr(expr->queryChild(1));
            reportAbstractModule(translator.queryErrorProcessor(), expr->queryChild(0), errpos);
            throw MakeStringException(HQLERR_ErrorAlreadyReported, "%s", "");
        }
    case no_pat_instance:
        {
            OwnedHqlExpr child = transform(expr->queryChild(0));
            if (child->getOperator() == no_pat_instance && child->hasAttribute(tempAtom))
                return createValue(no_pat_instance, child->getType(), LINK(child->queryChild(0)));
            //default action
            break;
        }
    case no_if:
        {
            //Parameters are being used a lot to select between two items in inside a function/module
            //so much better if we trim the tree earlier....
            IValue * value = expr->queryChild(0)->queryValue();
            if (value && !expr->isAction())
            {
                unsigned branch = value->getBoolValue() ? 1 : 2;
                IHqlExpression * arg = expr->queryChild(branch);
                if (arg)
                    return transform(arg);
            }
            break;
        }
    case no_stored:
        {
            HqlExprArray children;
            OwnedHqlExpr name = transform(expr->queryChild(0));
            children.append(*lowerCaseHqlExpr(name));
            return completeTransform(expr, children);
        }
    case no_merge:
        return transformMerge(expr);
        //yuk: Sets of datasets need special casing because their type isn't implicitly calculated from their inputs.
    case no_datasetlist:
    case no_rowset:
        {
            HqlExprArray children;
            transformChildren(expr, children);
            OwnedITypeInfo setType = makeSetType(children.item(0).getType());
            return createValue(op, setType.getClear(), children);
        }
    case no_rowsetrange:
        {
            HqlExprArray children;
            transformChildren(expr, children);
            OwnedITypeInfo setType = children.item(0).getType();
            return createValue(op, setType.getClear(), children);
        }
    case no_buildindex:
        {
            //Normalize the index build by splitting out the sort here, so that constant percolating
            //is also done on these parameters
            OwnedHqlExpr transformed = Parent::createTransformed(expr);
            loop
            {
                IHqlExpression * ret = normalizeIndexBuild(transformed, options.sortIndexPayload, !translator.targetThor(), options.implicitSubSort);
                if (!ret)
                    return LINK(transformed);
                transformed.setown(ret);
            }
        }
    case no_keyed:
        {
            HqlExprArray args;
            bool same = transformChildren(expr, args);
            IHqlExpression * ds = &args.item(0);
            if ((ds->getOperator() == no_section) || (ds->getOperator() == no_sectioninput))
            {
                args.replace(*LINK(ds->queryChild(0)), 0);
                same = false;
            }
            if (!same)
                return expr->clone(args);
            return LINK(expr);
        }
    case no_eclcrc:
        {
            OwnedHqlExpr arg = transform(expr->queryChild(0)->queryChild(0));
            return createConstant(expr->queryType()->castFrom(true, (__int64)getExpressionCRC(arg)));
        }
    case no_cast:
        if (options.normalizeExplicitCasts)
        {
            Owned<ITypeInfo> type = transformType(expr->queryType());
            OwnedHqlExpr arg = transform(expr->queryChild(0));
            return createValue(no_implicitcast, type.getClear(), arg.getClear());
        }
        break;
#if 0
    //This code adds a assertsorted activity after an nary-join, but I don't think it is actually correct, so removed.  I may revisit.
    case no_nwayjoin:
        if (expr->hasAttribute(assertAtom) && !removeAsserts)
        {
            OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);;
            IHqlExpression * ds = transformed->queryChild(0);
            IHqlExpression * selSeq = querySelSeq(transformed);
            OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
            IHqlExpression * sortOrder = transformed->queryChild(3);
            HqlExprArray args;
            args.append(*LINK(transformed));
            //MORE: Need fixing once join can have a different output format
            args.append(*replaceSelector(sortOrder, left, transformed->queryNormalizedSelector()));
            return createDataset(no_assertsorted, args);
        }
        break;
#endif

    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        {
            IAtom * name = expr->queryName();
            if ((name == _uid_Atom) && (expr->numChildren() > 0))
            {
                //Make sure we ignore any line number information on the parameters mangled with the uid - otherwise
                //they may create too many unique ids.
                IHqlExpression * normalForm = queryLocationIndependent(expr);
                if (normalForm != expr)
                    return transform(normalForm);
                return ::createUniqueId();
            }

#ifdef USE_SELSEQ_UID
            if (name == _selectorSequence_Atom)
            {
                //Ensure parameterised sequences generate a unique sequence number...
                //Not sure the following is really necessary, but will reduce in memory tree size....
                //also saves complications from having weird attributes in the tree
                if (expr->numChildren() > 0)
                {
                    //Make sure we ignore any line number information on the parameters mangled with the uid - otherwise
                    //they may create too many unique ids.
                    OwnedHqlExpr transformed = Parent::createTransformed(expr);
                    IHqlExpression * normalForm = queryLocationIndependent(transformed);

                    OwnedHqlExpr ret;
                    if (normalForm != expr)
                    {
                        IHqlExpression * mapped = queryAlreadyTransformed(normalForm);
                        if (!mapped)
                        {
                            ret.setown(createSelectorSequence());
                            setTransformed(normalForm, ret);
                        }
                        else
                            ret.set(mapped);
                    }
                    else
                        ret.setown(createSelectorSequence());
                    return ret.getClear();
                }
            }
#endif

            //If a named symbol is used as an argument to maxlength you can end up with a situation
            //where records are identical except for that named symbol.
            //If f(a + b) is then optimized to b this can lead to incompatible selectors, since
            //a and b are "compatible", but not identical.
            //This then causes chaos, so strip them as a precaution... but it is only a partial solution.
            if (name == maxLengthAtom || name == maxCountAtom)
                return transformChildrenNoAnnotations(expr);
            break;
        }
    case no_call:
        {
            IHqlExpression * oldFuncdef = expr->queryFunctionDefinition();
            if (oldFuncdef->getOperator() == no_delayedselect)
            {
                IHqlExpression * module = oldFuncdef->queryChild(1);
                ECLlocation errpos(module);
                //errpos.extractLocationAttr(expr->queryChild(1));
                reportAbstractModule(translator.queryErrorProcessor(), module, errpos);
                throw MakeStringException(HQLERR_ErrorAlreadyReported, "%s", "");
            }
            if (oldFuncdef->getOperator() == no_param)
                return LINK(expr);
            assertex(oldFuncdef->getOperator() == no_funcdef);
            return transformCall(expr);
        }
    case no_externalcall:
        //Yuk.... Because we ensure that all records have a name, we need to make sure that external functions that return records
        //also have there return value normalized - otherwise (jtolbert2.xhql) you can create an ambiguity
        //We could also want to do this for user functions - but better would be to have a different node type.
        if (options.ensureRecordsHaveSymbols)
        {
            if (expr->queryRecord())
                return transformExternalCall(expr);
        }
        break;
    case no_external:
        {
            ITypeInfo * type = expr->queryType();
            OwnedITypeInfo newType = transformType(type);
            HqlExprArray args;
            bool same = transformChildren(expr, args);
            if (same && (type == newType))
                return LINK(expr);
            return createExternalReference(expr->queryId(), newType.getClear(), args);
        }
    case no_outputscalar:
        if (options.outputRowsAsDatasets && expr->queryChild(0)->isDatarow())
        {
            HqlExprArray args;
            bool same = transformChildren(expr, args);
            args.replace(*createDatasetFromRow(LINK(&args.item(0))), 0);
            return createValue(no_output, makeVoidType(), args);
        }
        break;
    case no_nameof:
        {
            OwnedHqlExpr newChild = transform(expr->queryChild(0));
            switch (newChild->getOperator())
            {
            case no_newkeyindex:
                return LINK(newChild->queryChild(3));
            case no_table:
                return LINK(newChild->queryChild(0));
            default:
                throwError(HQLERR_CannotDeduceNameDataset);
            }
            break;
        }
    case no_typedef:
        {
            HqlExprArray children;
            transformChildren(expr, children);
            OwnedITypeInfo newType = transformType(expr->queryType());
            return createValue(op, newType.getClear(), children);
        }
    case no_assertkeyed:
        {
            //Ensure assertkeyed is tagged with the selectors of each of the fields that are keyed, otherwise
            //when expressions are constant folded, the information about keyed fields is lost.
            HqlExprArray children;
            transformChildren(expr, children);
            HqlExprArray args;
            gatherPotentialSelectors(args, expr);
            OwnedHqlExpr selectors = createExprAttribute(_selectors_Atom, args);
            children.append(*transform(selectors));
            return expr->clone(children);
        }
    case no_sequence:
        return getSizetConstant(nextSequenceValue++);
    case no_filter:
        return transformWithinFilter(expr);
    case no_executewhen:
        return transformExecuteWhen(expr);
    case no_funcdef:
        {
            HqlExprArray children;
            if (transformChildren(expr, children))
                return LINK(expr);
            return createFunctionDefinition(expr->queryId(), children);
        }
    case no_debug_option_value:
        {
            if (!matchesConstantString(expr->queryChild(0), "targetClusterType", true))
                return getDebugValueExpr(translator.wu(), expr);
            break;
        }
    case no_loop:
        {
            OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
            IHqlExpression * loopCond = queryRealChild(transformed, 3);
            if (loopCond)
            {
                //Create a firstCond attribute so that the condition for whether to execute the loop
                //the first time will be efficiently optimized.
                IHqlExpression * dataset = transformed->queryChild(0);
                IHqlExpression * filter = queryRealChild(transformed, 2);
                IHqlExpression * rowsid = transformed->queryAttribute(_rowsid_Atom);
                IHqlExpression * selSeq = querySelSeq(transformed);
                IHqlExpression * counter = queryAttributeChild(transformed, _countProject_Atom, 0);
                OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
                OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(left), LINK(rowsid));
                OwnedHqlExpr initialLoopDataset = LINK(dataset);
                if (filter)
                {
                    //If there is a loop filter then the global condition is applied to dataset filtered by that.
                    OwnedHqlExpr mappedFilter = replaceSelector(filter, left, dataset);
                    initialLoopDataset.setown(createDataset(no_filter, initialLoopDataset.getClear(), LINK(mappedFilter)));
                }
                OwnedHqlExpr firstCond = quickFullReplaceExpression(loopCond, rowsExpr, initialLoopDataset);
                if (counter)
                {
                    //Whether to evaluate the 1st time round the loop requires COUNTER=1
                    OwnedHqlExpr one = createConstant(createIntValue(1, counter->getType()));
                    firstCond.setown(quickFullReplaceExpression(firstCond, counter, one));
                }
                return appendOwnedOperand(transformed, createExprAttribute(_loopFirst_Atom, firstCond.getClear()));
            }
            return transformed.getClear();
        }
        break;
    }

    ITypeInfo * type = expr->queryType();
    bool checkType = false;
    if (type)
    {
        switch (type->getTypeCode())
        {
        case type_pattern:
        case type_rule:
            checkType = true;
            break;
        }
    }

    unsigned max = expr->numChildren();
    if ((max == 0) && !checkType)
        return LINK(expr);

    bool same = true;
    HqlExprArray children;
    children.ensure(max);
    for (unsigned idx=0;idx<max;idx++)
    {
        IHqlExpression * child = expr->queryChild(idx);
        IHqlExpression * tchild = transform(child);
        children.append(*tchild);
        if (child != tchild)
            same = false;
    }

    if (checkType)
    {
        OwnedITypeInfo newType = transformType(type);
        if (type != newType)
            return createWrapper(op, newType, children);
    }

    if (!same)
        return expr->clone(children);
    return LINK(expr);
}

IHqlExpression * HqlTreeNormalizer::createTransformedSelector(IHqlExpression * expr)
{
    throwUnexpected();
}

IHqlExpression * normalizeExpression(HqlCppTranslator & translator, IHqlExpression * expr)
{
    HqlTreeNormalizer normalizer(translator);
    return normalizer.transformRoot(expr);
}

void normalizeHqlTree(HqlCppTranslator & translator, HqlExprArray & exprs)
{
    bool seenForceLocal;
    bool seenLocalUpload;
    {
        //First iterate through the expressions and call queryLocationIndependent() to avoid nested transforms (which are less efficient)
//      ForEachItemIn(iInit, exprs)
//          queryLocationIndependent(&exprs.item(iInit));

        cycle_t startCycles = get_cycles_now();
        HqlTreeNormalizer normalizer(translator);
        HqlExprArray transformed;
        normalizer.analyseArray(exprs, 0);
        normalizer.transformRoot(exprs, transformed);
//      logTreeStats(exprs);
//      logTreeStats(transformed);
//      DBGLOG("Before normalize %u unique expressions, after normalize %u unique expressions", getNumUniqueExpressions(exprs), getNumUniqueExpressions(transformed));
        replaceArray(exprs, transformed);
        seenForceLocal = normalizer.querySeenForceLocal();
        seenLocalUpload = normalizer.querySeenLocalUpload();
        translator.noteFinishedTiming("compile:tree transform: normalize.initial", startCycles);
    }

    if (translator.queryOptions().constantFoldPostNormalize)
    {
        cycle_t startCycles = get_cycles_now();
        HqlExprArray transformed;
        quickFoldExpressions(transformed, exprs, NULL, 0);
        replaceArray(exprs, transformed);
        translator.noteFinishedTiming("compile:tree transform: normalize.fold", startCycles);
    }

    translator.traceExpressions("before scope tag", exprs);

    {
        cycle_t startCycles = get_cycles_now();
        HqlScopeTagger normalizer(translator.queryErrorProcessor(), translator.queryLocalOnWarningMapper());
        HqlExprArray transformed;
        normalizer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
        translator.noteFinishedTiming("compile:tree transform: normalize.scope", startCycles);
    }

    if (translator.queryOptions().normalizeLocations)
        normalizeAnnotations(translator, exprs);

    translator.traceExpressions("after scope tag", exprs);

    {
        cycle_t startCycles = get_cycles_now();
        HqlLinkedChildRowTransformer transformer(translator.queryOptions().implicitLinkedChildRows);
        HqlExprArray transformed;
        transformer.transformArray(exprs, transformed);
        replaceArray(exprs, transformed);
        translator.noteFinishedTiming("compile:tree transform: normalize.linkedChildRows", startCycles);;
    }

    if (seenLocalUpload)
    {
        LocalUploadTransformer transformer(translator.wu());
        HqlExprArray transformed;
        transformer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }

    if (seenForceLocal)
    {
        //Add ,local to all sources, so that count(x) inside local() is differentiated from a global count(x)
        ForceLocalTransformer localizer(translator.getTargetClusterType());
        HqlExprArray transformed;
        localizer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }

#ifdef USE_SELSEQ_UID
    if (translator.queryOptions().detectAmbiguousSelector || translator.queryOptions().allowAmbiguousSelector)
    {
        LeftRightSelectorNormalizer transformer(translator.queryOptions().allowAmbiguousSelector);

        transformer.analyseArray(exprs, 0);

        if (!transformer.containsAmbiguity())
        {
            HqlExprArray transformed;
            transformer.transformRoot(exprs, transformed);
            replaceArray(exprs, transformed);
        }
    }
#endif

    if (false)
    {
        NestedSelectorNormalizer transformer;

        transformer.analyseArray(exprs, 0);
        if (transformer.requiresTransforming())
        {
            HqlExprArray transformed;
            transformer.transformRoot(exprs, transformed);
            replaceArray(exprs, transformed);
        }
    }

#if 0
    if (seenIndex)
    {
        FilteredIndexOptimizer transformer(true, false);
        HqlExprArray transformed;
        transformer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }
#endif

#ifdef _DEBUG
    //spotPotentialDuplicateCode(exprs);
#endif
}

IHqlExpression * normalizeHqlTree(HqlCppTranslator & translator, IHqlExpression * expr)
{
    HqlExprArray exprs;
    expr->unwindList(exprs, no_comma);
    normalizeHqlTree(translator, exprs);
    return createComma(exprs);
}


void hoistNestedCompound(HqlCppTranslator & translator, HqlExprArray & exprs)
{
    if (containsCompound(exprs))
    {
        NestedCompoundTransformer normalizer(translator);
        normalizer.analyseArray(exprs, 0);
        HqlExprArray transformed;
        normalizer.transformRoot(exprs, transformed);
        replaceArray(exprs, transformed);
    }
}


void hoistNestedCompound(HqlCppTranslator & translator, WorkflowArray & workflow)
{
    ForEachItemIn(i, workflow)
        hoistNestedCompound(translator, workflow.item(i).queryExprs());
}


//---------------------------------------------------------------------------

static IHqlExpression * substituteClusterSize(unsigned numNodes, IHqlExpression * expr, ICodegenContextCallback * ctxCallback, IWorkUnit * wu);

static HqlTransformerInfo clusterSubstitueTransformerInfo("ClusterSubstitueTransformer");
class ClusterSubstitueTransformer : public NewHqlTransformer
{
public:
    ClusterSubstitueTransformer(unsigned size, ICodegenContextCallback * _ctxCallback, IWorkUnit * _wu)
        : NewHqlTransformer(clusterSubstitueTransformerInfo)
    {
        ctxCallback = _ctxCallback;
        wu = _wu;
        OwnedHqlExpr clusterSizeExpr = createValue(no_clustersize, makeIntType(4, false));
        if (size)
            clusterSizeValue.setown(getSizetConstant(size));
        setTransformed(clusterSizeExpr, clusterSizeValue);
    }

protected:
    IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        if (expr->isConstant())
            return LINK(expr);
        switch (expr->getOperator())
        {
        case no_clustersize:
            //Cope if CLUSTERSIZE is assigned to a named symbol
            if (clusterSizeValue)
                return LINK(clusterSizeValue);
            break;
        case no_cluster:
            return createSubstitutedChild(expr, expr->queryChild(1));
        case no_colon:
            {
                ForEachChild(i, expr)
                {
                    IHqlExpression * child = expr->queryChild(i);
                    if (child->getOperator() == no_persist)
                    {
                        IHqlExpression * cluster = queryRealChild(child, 1);
                        if (cluster && !isBlankString(cluster))
                            return createSubstitutedChild(expr, cluster);
                    }
                    else if (child->getOperator() == no_global)
                    {
                        IHqlExpression * cluster = queryRealChild(child, 0);
                        if (cluster && !isBlankString(cluster))
                            return createSubstitutedChild(expr, cluster);
                    }
                }
                break;
            }
        }

        return NewHqlTransformer::createTransformed(expr);
    }

    IHqlExpression * createSubstitutedChild(IHqlExpression * expr, IHqlExpression * cluster)
    {
        StringBuffer clusterText;
        getStringValue(clusterText, cluster);
        if (clusterText.length())
            ctxCallback->noteCluster(clusterText.str());
#if 0
        Owned<IConstWUClusterInfo> clusterInfo = wu->getClusterInfo(clusterText.str());
        if (clusterInfo)
        {
            unsigned numNodes = clusterInfo->getSize();
            if (numNodes == 0) numNodes = 1;
            HqlExprArray args;
            unwindChildren(args, expr);
            args.replace(*substituteClusterSize(numNodes, &args.item(0), ctxCallback, wu), 0);
            return expr->clone(args);
        }
#endif
        return LINK(expr);
    }

protected:
    ICodegenContextCallback * ctxCallback;
    IWorkUnit * wu;
    OwnedHqlExpr clusterSizeValue;
};

IHqlExpression * substituteClusterSize(unsigned numNodes, IHqlExpression * expr, ICodegenContextCallback * ctxCallback, IWorkUnit * wu)
{
    ClusterSubstitueTransformer transformer(numNodes, ctxCallback, wu);
    return transformer.transformRoot(expr);
}



void HqlCppTranslator::substituteClusterSize(HqlExprArray & exprs)
{
    unsigned numNodes = options.specifiedClusterSize;

    ClusterSubstitueTransformer transformer(numNodes, ctxCallback, wu());
    HqlExprArray transformed;
    ForEachItemIn(i, exprs)
        transformed.append(*transformer.transformRoot(&exprs.item(i)));
    replaceArray(exprs, transformed);
}

IHqlExpression * HqlCppTranslator::separateLibraries(IHqlExpression * query, HqlExprArray & internalLibraries)
{
    HqlExprArray exprs;
    query->unwindList(exprs, no_comma);

    traceExpressions("before transform graph for generation", exprs);
    //Remove any meta entries from the tree.
    ForEachItemInRev(i, exprs)
        if (exprs.item(i).getOperator() == no_setmeta)
            exprs.remove(i);

    processEmbeddedLibraries(exprs, internalLibraries, isLibraryScope(query));
    return createComma(exprs);
}

void HqlCppTranslator::normalizeGraphForGeneration(HqlExprArray & exprs, HqlQueryContext & query)
{
    if (isLibraryScope(query.expr))
        outputLibrary->mapLogicalToImplementation(exprs, query.expr);
    else
        query.expr->unwindList(exprs, no_comma);

    //Ensure the incoming query will be freed up when no longer used
    query.expr.clear();

    traceExpressions("before transform graph for generation", exprs);
    //Don't change the engine if libraries are involved, otherwise things will get very confused.

    {
        cycle_t startCycles = get_cycles_now();
        expandDelayedFunctionCalls(&queryErrorProcessor(), exprs);
        noteFinishedTiming("compile:tree transform: expand delayed calls", startCycles);
    }

    {
        cycle_t startCycles = get_cycles_now();
        traceExpressions("before normalize", exprs);
        normalizeHqlTree(*this, exprs);
        noteFinishedTiming("compile:tree transform: normalize", startCycles);
    }

    if (wu()->getDebugValueBool("dumpIR", false))
        EclIR::dbglogIR(exprs);

    checkNormalized(exprs);
#ifdef PICK_ENGINE_EARLY
    if (options.pickBestEngine)
        pickBestEngine(exprs);
#endif

    allocateSequenceNumbers(exprs);                                             // Added to all expressions/output statements etc.

    traceExpressions("allocate Sequence", exprs);
    checkNormalized(exprs);
}


void HqlCppTranslator::applyGlobalOptimizations(HqlExprArray & exprs)
{
    traceExpressions("begin transformGraphForGeneration", exprs);
    checkNormalized(exprs);

    {
        cycle_t startCycles = get_cycles_now();
        substituteClusterSize(exprs);
        noteFinishedTiming("compile:tree transform: substituteClusterSize", startCycles);
    }

    {
        cycle_t startCycles = get_cycles_now();
        HqlExprArray folded;
        unsigned foldOptions = DEFAULT_FOLD_OPTIONS;
        if (options.foldConstantDatasets) foldOptions |= HFOconstantdatasets;
        if (options.percolateConstants) foldOptions |= HFOpercolateconstants;
        if (options.percolateFilters) foldOptions |= HFOpercolatefilters;
        if (options.optimizeMax) foldOptions |= HFOx_op_not_x;
        if (options.globalFoldOptions != (unsigned)-1)
            foldOptions = options.globalFoldOptions;

        foldHqlExpression(queryErrorProcessor(), folded, exprs, foldOptions);
        replaceArray(exprs, folded);
        noteFinishedTiming("compile:tree transform: global fold", startCycles);
    }

    traceExpressions("after global fold", exprs);
    checkNormalized(exprs);

    if (options.globalOptimize)
    {
        cycle_t startCycles = get_cycles_now();
        HqlExprArray folded;
        optimizeHqlExpression(queryErrorProcessor(), folded, exprs, HOOfold);
        replaceArray(exprs, folded);
        noteFinishedTiming("compile:tree transform: global optimize", startCycles);
    }

    traceExpressions("alloc", exprs);
    checkNormalized(exprs);
    modifyOutputLocations(exprs);
}

void HqlCppTranslator::transformWorkflowItem(WorkflowItem & curWorkflow)
{
#ifdef USE_SELSEQ_UID
    if (options.normalizeSelectorSequence)
    {
        cycle_t startCycles = get_cycles_now();
        LeftRightTransformer normalizer;
        normalizer.process(curWorkflow.queryExprs());
        noteFinishedTiming("compile:tree transform: left right", startCycles);
        //traceExpressions("after implicit alias", workflow);
    }
#endif

    if (queryOptions().createImplicitAliases)
    {
        cycle_t startCycles = get_cycles_now();
        ImplicitAliasTransformer normalizer;
        normalizer.process(curWorkflow.queryExprs());
        noteFinishedTiming("compile:tree transform: implicit alias", startCycles);
        //traceExpressions("after implicit alias", workflow);
    }

    {
        cycle_t startCycles = get_cycles_now();
        hoistNestedCompound(*this, curWorkflow.queryExprs());
        noteFinishedTiming("compile:tree transform: hoist nested compound", startCycles);
    }

    if (options.optimizeNestedConditional)
    {
        cycle_t startCycles = get_cycles_now();
        optimizeNestedConditional(curWorkflow.queryExprs());
        noteFinishedTiming("compile:optimize nested conditional", startCycles);
        traceExpressions("nested", curWorkflow);
        checkNormalized(curWorkflow);
    }

    checkNormalized(curWorkflow);
    //sort(x)[n] -> topn(x, n)[]n, count(x)>n -> count(choosen(x,n+1)) > n and possibly others
    {
        cycle_t startCycles = get_cycles_now();
        optimizeActivities(curWorkflow.queryExprs(), !targetThor(), options.optimizeNonEmpty);
        noteFinishedTiming("compile:tree transform: optimize activities", startCycles);
    }
    checkNormalized(curWorkflow);

    //----------------------------- Transformations below this mark may have created globals so be very careful with hoisting ---------------------

    {
        cycle_t startCycles = get_cycles_now();
        migrateExprToNaturalLevel(curWorkflow, wu(), *this);       // Ensure expressions are evaluated at the best level - e.g., counts moved to most appropriate level.
        noteFinishedTiming("compile:tree transform: migrate", startCycles);
        //transformToAliases(exprs);
        traceExpressions("migrate", curWorkflow);
        checkNormalized(curWorkflow);
    }

    {
        cycle_t startCycles = get_cycles_now();
        markThorBoundaries(curWorkflow);                                               // work out which engine is going to perform which operation.
        noteFinishedTiming("compile:tree transform: thor hole", startCycles);
        traceExpressions("boundary", curWorkflow);
        checkNormalized(curWorkflow);
    }

    if (options.optimizeGlobalProjects)
    {
        cycle_t startCycles = get_cycles_now();
        insertImplicitProjects(*this, curWorkflow.queryExprs());
        noteFinishedTiming("compile:global implicit projects", startCycles);
        traceExpressions("implicit", curWorkflow);
        checkNormalized(curWorkflow);
    }

    cycle_t startCycles3 = get_cycles_now();
    normalizeResultFormat(curWorkflow, options);
    noteFinishedTiming("compile:tree transform: normalize result", startCycles3);
    traceExpressions("results", curWorkflow);
    checkNormalized(curWorkflow);

    optimizePersists(curWorkflow.queryExprs());

    traceExpressions("per", curWorkflow);
    checkNormalized(curWorkflow);
//  flattenDatasets(workflow);
//  traceExpressions("flatten", workflow);

    {
        cycle_t startCycles = get_cycles_now();
        mergeThorGraphs(curWorkflow, options.resourceConditionalActions, options.resourceSequential);          // reduces number of graphs sent to thor
        noteFinishedTiming("compile:tree transform: merge thor", startCycles);
    }

    traceExpressions("merged", curWorkflow);
    checkNormalized(curWorkflow);

    if (queryOptions().normalizeLocations)
        normalizeAnnotations(*this, curWorkflow.queryExprs());

    spotGlobalCSE(curWorkflow);                                                        // spot CSE within those graphs, and create some more
    checkNormalized(curWorkflow);

    //expandGlobalDatasets(workflow, wu(), *this);

    {
        cycle_t startCycles = get_cycles_now();
        mergeThorGraphs(curWorkflow, options.resourceConditionalActions, options.resourceSequential);
        noteFinishedTiming("compile:tree transform: merge thor", startCycles);
    }
    checkNormalized(curWorkflow);

    removeTrivialGraphs(curWorkflow);
    checkNormalized(curWorkflow);
}

bool HqlCppTranslator::transformGraphForGeneration(HqlQueryContext & query, WorkflowArray & workflow)
{
    HqlExprArray exprs;
    normalizeGraphForGeneration(exprs, query);

    if (options.generateLogicalGraph || options.generateLogicalGraphOnly)
    {
        LogicalGraphCreator creator(wu());
        creator.createLogicalGraph(exprs);
        if (options.generateLogicalGraphOnly)
            return false;
        curActivityId = creator.queryMaxActivityId();
    }

    applyGlobalOptimizations(exprs);
    if (exprs.ordinality() == 0)
        return false;   // No action needed

    cycle_t startCycles = get_cycles_now();
    ::extractWorkflow(*this, exprs, workflow);

    traceExpressions("workflow", workflow);
    checkNormalized(workflow);
    noteFinishedTiming("compile:tree transform: stored results", startCycles);

    if (outputLibrary && workflow.ordinality() > 1)
    {
        unsigned cnt = 0;
        ForEachItemIn(i, workflow)
        {
            if (!workflow.item(i).isFunction())
                cnt++;
        }
        if (cnt > 1)
        {
            SCMStringBuffer libraryName;
            getOutputLibraryName(libraryName, wu());
            throwError2(HQLERR_LibraryCannotContainWorkflow, libraryName.str(), "");
        }
    }

    ForEachItemIn(i, workflow)
    {
        WorkflowItem & curWorkflow = workflow.item(i);
        transformWorkflowItem(curWorkflow);
    }

#ifndef PICK_ENGINE_EARLY
    if (options.pickBestEngine)
        pickBestEngine(workflow);
#endif
    updateClusterType();

    ForEachItemIn(i2, workflow)
    {
        WorkflowItem & curWorkflow = workflow.item(i2);
        traceExpressions("before convert to logical", curWorkflow);

        convertLogicalToActivities(curWorkflow);                                           // e.g., merge disk reads, transform group, all to sort etc.

    #ifndef _DEBUG
        if (options.regressionTest)
    #endif
        {
            cycle_t startCycles = get_cycles_now();
            checkDependencyConsistency(curWorkflow.queryExprs());
            noteFinishedTiming("compile:tree transform: check dependency", startCycles);
        }

        traceExpressions("end transformGraphForGeneration", curWorkflow);
        checkNormalized(curWorkflow);
    }
    return true;
}

//---------------------------------------------------------------------------
/*
Different transformers:
merge: required if a child get removed or merged with a parent of a non-table dataset.
       adding is not a problem (unless tables are inserted) because all refs to ds.x will remain valid.
       but if tables can be deleted/modified then then it will cause problems, since ds.x needs to be translated to ds'.x within the scope
       It is also ok if only scalars are transformed.

Transformer         base   merge    dependants  [should be]
filterExtractor     simple  N
resource            Scoped  (Y)                     complex - could possibly derive from merging...? why scoped?
HqlThorBoundary     New     N
HqlResult           New     N       isConditional,insideThor,insideCondition
                                                    creation of getresult only done on scalars, so I think it is ok.
                                                    could possibly remove the scoping if count() etc. were tagged as outer level or not
ThorHql             Merging Y                       Again scoped because of no_count etc.
CompoundSource      New     add*                    Either added, or non-tables(limits) are cloned, so no merging issues.
CompoundActivity    Merging Y                       When limit merged into a dataset. [ need a new way? ]
Workflow            New             [inTransform]   I think no for the same reason as above, or it adds. layers.
NewScopeMigrate     New     ?                       I think it might be ok, because doesn't modify any tables, only scalars
ThorCount           New     N                       no issues.
Cse                 New     ?                       Probably, but ok, if only done on scalars.
HqlTreeNormalizer   Scoped  *                       I don't think it does any, but need to be careful none are introduced.
*/



/*
NOTES:

  Consider adding a hqlmeta.hpp that defines all the characteristics of an IHqlExpression node - e.g.,
  is it constant, number of child files, text, what filenames does it generate? what does it read,
  what results does it read/write.

  Dependancy code:
  1. In the resourcer
  2. TableDependencies In hqlttcpp to stop reordering when not valid.

  ?Is GetResultHash called on globals that haven't been calculated???

*/
