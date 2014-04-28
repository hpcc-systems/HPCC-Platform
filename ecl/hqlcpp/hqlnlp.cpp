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
#ifndef U_OVERRIDE_CXX_ALLOCATION
#define U_OVERRIDE_CXX_ALLOCATION 0 // Enabling this forces all allocation of ICU objects to ICU's heap, but is incompatible with jmemleak
#endif
#include "jliball.hpp"
#include "hqlexpr.hpp"
#include "hqlcerrors.hpp"
#include "hqlnlp.ipp"
#include "hqlhtcpp.ipp"
#include "hqlcatom.hpp"
#include "thorralgo.ipp"
#include "hqlfold.hpp"
#include "hqlcse.ipp"
#include "hqlccommon.hpp"
#include "hqlutil.hpp"
#include "hqltcppc.hpp"
#include "hqlcpputil.hpp"

#include "unicode/uchar.h"

#define DEFAULT_NLP_DETAIL              1
#define DEFAULT_PATTERN_MAX_LENGTH      4096
#ifdef __64BIT__
#define __DEFINED_64BIT__ true
#else
#define __DEFINED_64BIT__ false
#endif

//---------------------------------------------------------------------------

ValidateKind getValidateKind(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_matchtext: return ValidateIsString;
    case no_matchunicode: return ValidateIsUnicode;
    }
    ForEachChild(idx, expr)
    {
        ValidateKind kind = getValidateKind(expr->queryChild(idx));
        if (kind != ValidateIsEither)
            return kind;
    }
    return ValidateIsEither;
}


//---------------------------------------------------------------------------


IHqlExpression * optimizeParse(IHqlExpression * parseExpr)
{
    return LINK(parseExpr);
}

//===========================================================================

MatchReference::MatchReference(IHqlExpression * expr)
{
    expand(expr, true);
}

bool MatchReference::equals(const MatchReference & other) const
{
    if (names.ordinality() != other.names.ordinality())
        return false;
    ForEachItemIn(idx, names)
    {
        if (&names.item(idx) != &other.names.item(idx))
            return false;
        if (&indices.item(idx) != &other.indices.item(idx))
            return false;
    }
    return true;
}

void MatchReference::expand(IHqlExpression * expr, bool isLast)
{
    switch (expr->getOperator())
    {
    case no_pat_select:
        expand(expr->queryChild(0), false);
        expand(expr->queryChild(1), true);
        break;
    case no_pat_index:
        names.append(*LINK(expr->queryChild(0)));
        indices.append(*LINK(expr->queryChild(1)));
        break;
    default:
        names.append(*LINK(expr));
        indices.append(*createConstant((int)UNKNOWN_INSTANCE));
        break;
    }
}

void MatchReference::getPath(StringBuffer & path)
{
    ForEachItemIn(idx, names)
    {
        if (idx)
            path.append(".");
        path.append(queryPatternName(&names.item(idx))->lower());
    }
}

StringBuffer & MatchReference::getDebugText(StringBuffer & out, RegexIdAllocator & idAllocator)
{
    ForEachItemIn(i1, names)
    {
        IHqlExpression & curName = names.item(i1);
        IAtom * name = queryPatternName(&curName)->lower();
        if (i1)
            out.append("/");
        out.append(name->str());
        out.append("{").append(idAllocator.queryID(curName.queryChild(0), name)).append("}");
        unsigned inst = (unsigned)indices.item(i1).queryValue()->getIntValue();
        if (inst != UNKNOWN_INSTANCE)
            out.append("[").append(inst).append("]");
    }
    return out;
}

void MatchReference::compileMatched(RegexIdAllocator & idAllocator, UnsignedArray & ids, UnsignedArray & indexValues)
{
    ForEachItemIn(idx, names)
    {
        IHqlExpression & curName = names.item(idx);
        ids.append(idAllocator.queryID(curName.queryChild(0), queryPatternName(&curName)->lower()));
        indexValues.append((unsigned)indices.item(idx).queryValue()->getIntValue());
    }
}

//---------------------------------------------------------------------------

NlpParseContext::NlpParseContext(IHqlExpression * _expr, IWorkUnit * _wu, const HqlCppOptions & options, ITimeReporter * _timeReporter) : timeReporter(_timeReporter)
{
    workunit = _wu;
    expr.set(_expr);
    IHqlExpression * search = expr->queryChild(1);

    switch (search->queryType()->getTypeCode())
    {
    case type_unicode:
    case type_varunicode:
        info.type = type_unicode;
        break;
    case type_utf8:
        info.type = type_utf8;
        break;
    default:
        info.type = type_string;
        break;
    }

    info.charSize = (info.type == type_unicode) ? sizeof(UChar) : sizeof(char);
    IHqlExpression * sepAttr = expr->queryAttribute(separatorAtom);
    if (sepAttr)
        info.separator.set(sepAttr->queryChild(0));
    info.caseSensitive = !expr->hasAttribute(noCaseAtom);        // default true.
    info.dfaRepeatMax = options.dfaRepeatMax;
    info.dfaRepeatMaxScore = options.dfaRepeatMaxScore;
    info.uidBase = getUniqueId();
    allMatched = false;
}


void NlpParseContext::addAllMatched()
{
    allMatched = true;
}

unsigned NlpParseContext::addMatchReference(IHqlExpression * matchPathExpr)
{
    if (!matchPathExpr) matchPathExpr = expr->queryChild(2);

    Owned<MatchReference> ref = new MatchReference(matchPathExpr);

    ForEachItemIn(idx, matches)
    {
        if (ref->equals(matches.item(idx)))
            return idx;
    }
    matches.append(*ref.getClear());
    extractMatchedSymbols(matchPathExpr);
    return matches.ordinality()-1;
}


IHqlExpression * NlpParseContext::queryValidateExpr(IHqlExpression * expr) const
{
    IHqlExpression * unicodeTest = expr->queryChild(2);
    switch (info.type)
    {
    case type_unicode:
    case type_utf8:     // I think this is best
        if (unicodeTest && !unicodeTest->isAttribute())
            return unicodeTest;
        break;
    }
    return expr->queryChild(1);
}

void NlpParseContext::buildValidators(HqlCppTranslator & translator, BuildCtx & classctx)
{
    if (validators.ordinality())
    {
        BuildCtx helperctx(classctx);
        translator.beginNestedClass(helperctx, "helper", "INlpHelper");

        BuildCtx funcctx(helperctx);
        funcctx.addQuotedCompound("virtual IValidator * queryValidator(unsigned i)");
        BuildCtx casectx(funcctx);
        casectx.addQuotedCompound("switch (i)");

        ForEachItemIn(idx, validators)
        {
            StringBuffer member;
            translator.getUniqueId(member.append("val"));

            LinkedHqlExpr validateExpr = queryValidateExpr(&validators.item(idx));

            ValidateKind kind = getValidateKind(validateExpr);

            BuildCtx validatorctx(helperctx);
            translator.beginNestedClass(validatorctx, member, (kind != ValidateIsUnicode) ? "IStringValidator" : "IUnicodeValidator");

            BuildCtx validctx(validatorctx);
            CHqlBoundExpr boundMatched;
            if (kind != ValidateIsUnicode)
            {
                validctx.addQuotedCompound("virtual bool isValid(unsigned len, const char * data)");
                boundMatched.length.setown(createVariable("len", LINK(sizetType)));
                boundMatched.expr.setown(createVariable("data", makeReferenceModifier(LINK(unknownStringType))));
                validctx.associateExpr(activeMatchTextExpr, boundMatched);
            }
            else
            {
                validctx.addQuotedCompound("virtual bool isValid(unsigned len, const UChar * data)");
                boundMatched.length.setown(createVariable("len", LINK(sizetType)));
                boundMatched.expr.setown(createVariable("data", makeReferenceModifier(LINK(unknownUnicodeType))));
                validctx.associateExpr(activeMatchUnicodeExpr, boundMatched);
            }
            validctx.associateExpr(activeNlpMarkerExpr, activeNlpMarkerExpr);
            validctx.associateExpr(activeValidateMarkerExpr, activeValidateMarkerExpr);
            translator.bindTableCursor(validctx, queryNlpParsePseudoTable(), queryNlpParsePseudoTable());
            if (translator.queryOptions().spotCSE)
                validateExpr.setown(spotScalarCSE(validateExpr, NULL, translator.queryOptions().spotCseInIfDatasetConditions));
            translator.buildReturn(validctx, validateExpr);
            translator.endNestedClass();

            StringBuffer s;
            s.append("case ").append(idx).append(": return &").append(member).append(";");
            casectx.addQuoted(s);
        }
        funcctx.addReturn(queryQuotedNullExpr());

        translator.endNestedClass();
        classctx.addQuotedLiteral("virtual INlpHelper * queryHelper() { return &helper; }");
    }
}


void NlpParseContext::buildProductions(HqlCppTranslator & translator, BuildCtx & classctx, BuildCtx & startctx)
{
    if (!productions.ordinality())
        return;

    {
        BuildCtx metactx(classctx);
        metactx.addQuotedCompound("virtual IOutputMetaData * queryProductionMeta(unsigned id)");

        BuildCtx metacasectx(metactx);
        metacasectx.addQuotedCompound("switch (id)");

        StringBuffer s;
        ForEachItemIn(i, productions)
        {
            IHqlExpression & cur = productions.item(i);
            MetaInstance meta(translator, cur.queryChild(1)->queryRecord(), false);
            translator.buildMetaInfo(meta);

            s.clear().append("case ").append(getIntValue(cur.queryChild(0)));
            s.append(": return &").append(meta.queryInstanceObject()).append(";");
            metacasectx.addQuoted(s);
        }
        metactx.addQuotedLiteral("return 0;");
    }

    {
        OwnedHqlExpr callback = createVariable("input", makeBoolType());
        BuildCtx prodctx(startctx);
        prodctx.addQuotedCompound("virtual size32_t executeProduction(ARowBuilder & crSelf, unsigned id, IProductionCallback * input)");
        prodctx.associateExpr(activeProductionMarkerExpr, callback);

        BuildCtx prodcasectx(prodctx);
        prodcasectx.addQuotedCompound("switch (id)");

        StringBuffer s, subname;
        ForEachItemIn(i, productions)
        {
            IHqlExpression & cur = productions.item(i);
            IHqlExpression * transform = cur.queryChild(1);
            if (transform->getOperator() == no_record)
                continue;

            subname.clear().append("executeProduction").append(i+1);
            
            s.clear().append("case ").append(getIntValue(cur.queryChild(0)));
            s.append(": return ").append(subname).append("(crSelf, input);");
            prodcasectx.addQuoted(s);

            {
                BuildCtx childctx(startctx);
                childctx.addQuotedCompound(s.clear().append("size32_t ").append(subname).append("(ARowBuilder & crSelf, IProductionCallback * input)"));
                translator.ensureRowAllocated(childctx, "crSelf");
                childctx.associateExpr(activeProductionMarkerExpr, callback);


                OwnedHqlExpr newTransform = LINK(transform);
                OwnedHqlExpr dataset = createDataset(no_anon, LINK(transform->queryRecord()));

                translator.buildTransformBody(childctx, newTransform, NULL, NULL, dataset, NULL);
            }
        }
        prodctx.addQuotedLiteral("return (size32_t)-1;");
    }
}

void NlpParseContext::extractMatchedSymbols(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_pat_select:
        extractMatchedSymbols(expr->queryChild(0));
        extractMatchedSymbols(expr->queryChild(1));
        break;
    case no_pat_index:
        expr = expr->queryChild(0);
        //fall through
    default:
        {
            assertex(expr->getOperator() == no_pat_instance);
            if (!matchedSymbols.contains(*expr))
                matchedSymbols.append(*LINK(expr));
            break;
        }
    }
}

bool NlpParseContext::isMatched(IHqlExpression * expr, IAtom * name)
{
    if (allMatched)
        return true;
    ForEachItemIn(i, matchedSymbols)
    {
        IHqlExpression & cur = matchedSymbols.item(i);
        if ((cur.queryChild(0)->queryBody() == expr->queryBody()) && 
            (cur.queryChild(1)->queryName() == name))
            return true;
    }
    return false;
}


void NlpParseContext::doExtractValidates(IHqlExpression * expr)
{
    if (expr->queryTransformExtra())
        return;
    expr->setTransformExtraUnlinked(expr);
    switch (expr->getOperator())
    {
    case no_pat_validate:
        validators.append(*LINK(expr));
        doExtractValidates(expr->queryChild(0));
        break;
    default:
        ForEachChild(idx, expr)
            doExtractValidates(expr->queryChild(idx));
        break;
    }
}


void NlpParseContext::extractValidates(IHqlExpression * expr)
{
    lockTransformMutex();
    doExtractValidates(expr);
    unlockTransformMutex();
}

bool NlpParseContext::isValidMatch(MatchReference & match, unsigned depth, IHqlExpression * pattern)
{
    //MORE: This should check whether already visited, especially once we allow recursive patterns!
    if (&match.names.item(depth) == pattern)
    {
        if (depth+1 == match.names.ordinality())
            return true;
        if (isValidMatch(match, depth+1, pattern))
            return true;
    }
    ForEachChild(idx, pattern)
        if (isValidMatch(match, depth, pattern->queryChild(idx)))
            return true;
    return false;
}

void NlpParseContext::checkValidMatches()
{
    ForEachItemIn(idx, matches)
    {
        if (!isValidMatch(matches.item(idx), 0, expr->queryChild(2)))
        {
            StringBuffer path;
            matches.item(idx).getPath(path);
            throwError1(HQLERR_BadMatchedPath, path.str());
        }
    }
}

void NlpParseContext::compileMatched(NlpAlgorithm & parser)
{
    parser.matchInfo->setFormat(info.inputFormat());
    ForEachItemIn(idx, matches)
    {
        UnsignedArray ids;
        UnsignedArray indexValues;
        matches.item(idx).compileMatched(idAllocator, ids, indexValues);
        parser.matchInfo->addResult(ids, indexValues);
    }
}


static void getOptions(IHqlExpression * expr, INlpParseAlgorithm::MatchAction & matchAction, INlpParseAlgorithm::ScanAction & scanAction)
{
    matchAction = INlpParseAlgorithm::NlpMatchAll;
    scanAction = INlpParseAlgorithm::NlpScanNext;
    if (expr->hasAttribute(firstAtom))       matchAction = INlpParseAlgorithm::NlpMatchFirst;
    if (expr->hasAttribute(allAtom))         matchAction = INlpParseAlgorithm::NlpMatchAll;

    if (expr->hasAttribute(noScanAtom))      scanAction = INlpParseAlgorithm::NlpScanNone;
    if (expr->hasAttribute(scanAtom))        scanAction = INlpParseAlgorithm::NlpScanNext;
    if (expr->hasAttribute(scanAllAtom))     scanAction = INlpParseAlgorithm::NlpScanAll;
    if (expr->hasAttribute(wholeAtom))       scanAction = INlpParseAlgorithm::NlpScanWhole;
}

void NlpParseContext::getDebugText(StringBuffer & s, unsigned detail)
{
    INlpParseAlgorithm::MatchAction matchAction;
    INlpParseAlgorithm::ScanAction scanAction;
    getOptions(expr, matchAction, scanAction);

    s.newline().append("Options:  ").append("Match(");
    switch (matchAction)
    {
    case INlpParseAlgorithm::NlpMatchFirst:     s.append("First"); break;
    case INlpParseAlgorithm::NlpMatchAll:       s.append("All"); break;
    }
    s.append(")  Scan(");
    switch (scanAction)
    {
    case INlpParseAlgorithm::NlpScanWhole:      s.append("Whole"); break;
    case INlpParseAlgorithm::NlpScanNone:       s.append("None"); break;
    case INlpParseAlgorithm::NlpScanNext:       s.append("Next"); break;
    case INlpParseAlgorithm::NlpScanAll:        s.append("All"); break;
    }
    s.append(")").newline();
    s.append("Matches:").newline();
    ForEachItemIn(idx, matches)
        matches.item(idx).getDebugText(s.append("\t"), idAllocator).newline();
}


void NlpParseContext::setParserOptions(INlpParseAlgorithm & parser)
{
    INlpParseAlgorithm::MatchAction matchAction;
    INlpParseAlgorithm::ScanAction scanAction;
    getOptions(expr, matchAction, scanAction);

    IHqlExpression * keep = expr->queryAttribute(keepAtom);
    unsigned keepLimit = keep ? (unsigned)keep->queryChild(0)->queryValue()->getIntValue() : 0;
    IHqlExpression * atmost = expr->queryAttribute(atmostAtom);
    unsigned atmostLimit = atmost ? (unsigned)atmost->queryChild(0)->queryValue()->getIntValue() : 0;
    IHqlExpression * maxLength = expr->queryAttribute(maxLengthAtom);
    size32_t maxLengthValue = maxLength ? (unsigned)maxLength->queryChild(0)->queryValue()->getIntValue() : DEFAULT_PATTERN_MAX_LENGTH;

    parser.setOptions(matchAction, scanAction, info.inputFormat(), keepLimit, atmostLimit);
    parser.setChoose(expr->hasAttribute(minAtom), expr->hasAttribute(maxAtom), expr->hasAttribute(bestAtom), !expr->hasAttribute(manyAtom));
    parser.setJoin(expr->hasAttribute(notMatchedAtom), expr->hasAttribute(notMatchedOnlyAtom));
    parser.setLimit(maxLengthValue);
}


//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildParseTransform(BuildCtx & classctx, IHqlExpression * expr)
{
    BuildCtx funcctx(classctx);

    funcctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IMatchedResults * matched, IMatchWalker * walker)");
    ensureRowAllocated(funcctx, "crSelf");
    funcctx.addQuotedLiteral("const unsigned char * left = (const unsigned char *) _left;");
    funcctx.associateExpr(activeNlpMarkerExpr, activeNlpMarkerExpr);
    bindTableCursor(funcctx, queryNlpParsePseudoTable(), queryNlpParsePseudoTable());

    // Bind left to "left" and right to RIGHT
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * transform = expr->queryChild(4);
    BoundRow * selfCursor = bindSelf(funcctx, expr, "crSelf");
    if (transform->getOperator() == no_newtransform)
        bindTableCursor(funcctx, dataset, "left");
    else
        bindTableCursor(funcctx, dataset, "left", no_left, querySelSeq(expr));

    associateSkipReturnMarker(funcctx, queryZero(), selfCursor);
    doTransform(funcctx, transform, selfCursor);

    buildReturnRecordSize(funcctx, selfCursor);
}


void HqlCppTranslator::doBuildParseSearchText(BuildCtx & classctx, IHqlExpression * dataset, IHqlExpression * search, type_t searchTypeCode, ITypeInfo * transferType)
{
    BuildCtx funcctx(classctx);

    if (searchTypeCode == type_unicode)
    {
        funcctx.addQuotedCompound("virtual void getSearchText(size32_t & retLen, char * & _retText, const void * _self)");
        funcctx.addQuotedLiteral("UChar * & retText = *(UChar * *)&_retText;");        // don't ask.
    }
    else
        funcctx.addQuotedCompound("virtual void getSearchText(size32_t & retLen, char * & retText, const void * _self)");
    funcctx.addQuotedLiteral("const unsigned char * self = (const unsigned char *) _self;");
    bindTableCursor(funcctx, dataset, "self");

    bool needToFree = true;
    Owned<ITypeInfo> retType;
    switch (searchTypeCode)
    {
    case type_unicode:
        retType.setown(makeUnicodeType(UNKNOWN_LENGTH, 0));
        break;
    case type_utf8:
        retType.setown(makeUtf8Type(UNKNOWN_LENGTH, 0));
        break;
    default:
        retType.setown(makeStringType(UNKNOWN_LENGTH, NULL, NULL));
        break;
    }
    OwnedHqlExpr castSearch = ensureExprType(search, retType);
    castSearch.setown(foldHqlExpression(castSearch));
    retType.setown(makeReferenceModifier(retType.getClear()));

    switch (castSearch->getOperator())
    {
    case no_select:
    case no_constant:
        //Not strictly true - could be conditional
        //also misses lots of cases - but I doubt anyone will ever complain...
        needToFree = false;
        break;
    }


    OwnedHqlExpr retLen = createVariable("retLen", LINK(sizetType));
    OwnedHqlExpr tempLen;
    if (transferType)
        tempLen.setown(funcctx.getTempDeclare(sizetType, NULL));

    CHqlBoundTarget target;
    target.length.set(tempLen ? tempLen : retLen);
    target.expr.setown(createVariable("retText", LINK(retType)));
    if (needToFree)
    {
        buildExprAssign(funcctx, target, castSearch);
    }
    else
    {
        CHqlBoundExpr bound;
        buildExpr(funcctx, castSearch, bound);
        OwnedHqlExpr len = getBoundLength(bound);
        funcctx.addAssign(target.length, len);
        funcctx.addAssign(target.expr, bound.expr);
    }
    if (tempLen)
    {
        OwnedHqlExpr source = target.getTranslatedExpr();
        OwnedHqlExpr transferred = createValue(no_typetransfer, LINK(transferType), LINK(source));
        OwnedHqlExpr length = createValue(no_charlen, LINK(sizetType), LINK(transferred));
        buildAssignToTemp(funcctx, retLen, length);
    }

    doBuildBoolFunction(classctx, "searchTextNeedsFree", needToFree);
}

void HqlCppTranslator::doBuildParseSearchText(BuildCtx & classctx, IHqlExpression * expr)
{
    doBuildParseSearchText(classctx, expr->queryChild(0), expr->queryChild(1), nlpParse->searchType(), NULL);
}


void HqlCppTranslator::doBuildParseExtra(BuildCtx & classctx, IHqlExpression * expr)
{
    StringBuffer flags;
    if (expr->hasAttribute(groupAtom)) flags.append("|PFgroup");
    if (expr->hasAttribute(parallelAtom)) flags.append("|PFparallel");

    if (flags.length())
        doBuildUnsignedFunction(classctx, "getFlags", flags.str()+1);
}


void HqlCppTranslator::doBuildParseValidators(BuildCtx & classctx, IHqlExpression * expr)
{
    nlpParse->extractValidates(expr->queryChild(2));
    nlpParse->buildValidators(*this, classctx);
}


void HqlCppTranslator::doBuildParseCompiled(BuildCtx & classctx, MemoryBuffer & buffer)
{
    if (buffer.length() > 1000000)
        WARNING1(HQLWRN_ParseVeryLargeDefinition, buffer.length());

    BuildCtx funcctx(classctx);

    MemoryBuffer compressed;
    compressToBuffer(compressed, buffer.length(), buffer.toByteArray());
    unsigned buffLen = compressed.length();
    CHqlBoundExpr bound;
    StringBuffer s;
    OwnedHqlExpr srcData = addDataLiteral((const char *)compressed.toByteArray(), buffLen);
    OwnedHqlExpr retData = createVariable("retData", makePointerType(makeVoidType()));

    funcctx.addQuotedCompound("virtual void queryCompiled(IResourceContext *ctx, size32_t & retLen, const void * & retData)");
    funcctx.addQuotedF("//uncompressed size = %d", buffer.length());
    buildExpr(funcctx, srcData, bound);

    funcctx.addQuoted(s.append("retLen = ").append(buffLen).append(";"));
    funcctx.addAssign(retData, srcData);
}


void HqlCppTranslator::gatherExplicitMatched(IHqlExpression * expr)
{
    ForEachChild(idx, expr)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (cur->getOperator() == no_matched)
        {
            IHqlExpression * arg = cur->queryChild(0);
            if (arg->getOperator() ==  no_all)
                nlpParse->addAllMatched();
            else
                nlpParse->addMatchReference(arg);
        }
    }
}

ABoundActivity * HqlCppTranslator::doBuildActivityParse(BuildCtx & ctx, IHqlExpression * _expr)
{
    Owned<ABoundActivity> boundDataset = buildCachedActivity(ctx, _expr->queryChild(0));

    unsigned startTime = msTick();
    OwnedHqlExpr expr = optimizeParse(_expr);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKparse, expr, "Parse");

    buildActivityFramework(instance);
    
    buildInstancePrefix(instance);

    //This will become conditional on the flags....
    unsigned startPrepareTime = msTick();
    ITimeReporter * reporter = options.addTimingToWorkunit ? timeReporter : NULL;
    if (expr->hasAttribute(tomitaAtom))
        nlpParse = createTomitaContext(expr, code->workunit, options, reporter);
    else
    {
        //In 64bit the engines have enough stack space to use the stack-based regex implementation
        byte algorithm = __DEFINED_64BIT__ ? NLPAregexStack : NLPAregexHeap;
        switch (options.regexVersion)
        {
        case 1:
            algorithm = NLPAregexStack;
            break;
        case 2:
            algorithm = NLPAregexHeap;
            break;
        }
        IHqlExpression * algorithmHint = queryHintChild(expr, algorithmAtom, 0);
        if (matchesConstantString(algorithmHint, "stack", true))
            algorithm = NLPAregexStack;
        else if (matchesConstantString(algorithmHint, "heap", true))
            algorithm = NLPAregexHeap;

        nlpParse = createRegexContext(expr, code->workunit, options, reporter, algorithm);
    }

    gatherExplicitMatched(expr);
    doBuildParseTransform(instance->startctx, expr);            // also gathers all the MATCHED() definitions.
    doBuildParseSearchText(instance->startctx, expr);
    doBuildParseValidators(instance->nestedctx, expr);
    doBuildParseExtra(instance->startctx, expr);
    updateTimer("workunit;Generate PARSE: Prepare", msTick()-startPrepareTime);
    
    MemoryBuffer buffer;
    unsigned startCompileTime = msTick();
    nlpParse->compileSearchPattern();
    nlpParse->queryParser()->serialize(buffer);
    if (nlpParse->isGrammarAmbiguous())
        WARNING1(HQLWRN_GrammarIsAmbiguous, instance->activityId);

    doBuildParseCompiled(instance->classctx, buffer);
    updateTimer("workunit;Generate PARSE: Compile", msTick()-startCompileTime);

    nlpParse->buildProductions(*this, instance->classctx, instance->startctx);

#if 0
    StringBuffer text;
    getSystemTraceInfo(text, PerfMonProcMem);
    wu()->setDebugValue("maxMemory", text.str(), true);
#endif

    if (options.debugNlp != 0)
    {
        BuildCtx subctx(instance->classctx);
        subctx.addQuotedLiteral("#if 0\nHuman readable form of the grammar");
        StringBuffer s;
        nlpParse->getDebugText(s, options.debugNlp);
        subctx.addQuoted(s);
        subctx.addQuotedLiteral("#endif");

        if (options.debugNlpAsHint)
        {
            StringBuffer hintText;
            hintText.append("<Hint type=\"activity\" id=\"").append(instance->activityId).append("\">").newline();
            encodeXML(s.str(), hintText, 0, s.length(), false);
            hintText.append("</Hint>");
            code->addHint(hintText.str(), ctxCallback);
        }
    }

    ::Release(nlpParse);
    nlpParse = NULL;
    buildInstanceSuffix(instance);
    buildConnectInputOutput(ctx, instance, boundDataset, 0, 0);
    updateTimer("workunit;Generate PARSE", msTick()-startTime);

    return instance->getBoundActivity();
}

//---------------------------------------------------------------------------

void getCheckRange(IHqlExpression * range, unsigned & minLength, unsigned & maxLength, unsigned charSize)
{
    minLength = 0;
    maxLength = PATTERN_UNLIMITED_LENGTH;
    switch (range->getOperator())
    {
    case no_constant:
        minLength = maxLength = (unsigned)range->queryValue()->getIntValue();
        break;
    case no_rangefrom:
        minLength = (unsigned)range->queryChild(0)->queryValue()->getIntValue();
        break;
    case no_rangeto:
        maxLength = (unsigned)range->queryChild(0)->queryValue()->getIntValue();
        break;
    case no_range:
        minLength = (unsigned)range->queryChild(0)->queryValue()->getIntValue();
        maxLength = (unsigned)range->queryChild(1)->queryValue()->getIntValue();
        break;
    }
    minLength *= charSize;
    if (maxLength < PATTERN_UNLIMITED_LENGTH / charSize)
        maxLength *= charSize;
    else
        maxLength = PATTERN_UNLIMITED_LENGTH;
}

void HqlCppTranslator::doBuildMatched(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound)
{
    if (!nlpParse)
        throwError1(HQLERR_MatchedUsedOutsideParse, getOpString(expr->getOperator()));

    if (!ctx.queryMatchExpr(activeNlpMarkerExpr))
    {
        CHqlBoundExpr match;
        if (!buildExprInCorrectContext(ctx, expr, match, false))
            throwError1(HQLERR_MatchedUsedOutsideParse, getOpString(expr->getOperator()));

        if (target)
            assign(ctx, *target, match);
        else
            bound->set(match);
        return;
    }

    IHqlExpression * patternExpr = queryRealChild(expr, 0);
    if (ctx.queryMatchExpr(activeValidateMarkerExpr))
    {
        CHqlBoundExpr match;
        switch (expr->getOperator())
        {
        case no_matchtext:
            if (!ctx.getMatchExpr(activeMatchTextExpr, match))
                throwError(HQLERR_MatchTextNotUnicode);
            if (patternExpr)
                throwError1(HQLERR_NoArgumentsInValidator, "MATCHTEXT");
            break;
        case no_matchunicode:
            if (!ctx.getMatchExpr(activeMatchUnicodeExpr, match))
                throwError(HQLERR_MatchUnicodeNotText);
            if (patternExpr)
                throwError1(HQLERR_NoArgumentsInValidator, "MATCHUNICODE");
            break;
        case no_matchutf8:
            if (!ctx.getMatchExpr(activeMatchUtf8Expr, match))
                throwError(HQLERR_MatchUtf8NotText);
            if (patternExpr)
                throwError1(HQLERR_NoArgumentsInValidator, "MATCHUTF8");
            break;
        default:
            throwError(HQLERR_MatchTextOrUnicode);
        }
        if (target)
            assign(ctx, *target, match);
        else
            bound->set(match);
        return;
    }

    unsigned matchedIndex = nlpParse->addMatchReference(patternExpr);
    IIdAtom * func;
    switch (expr->getOperator())
    {
    case no_matched:        func = getMatchedId; break;
    case no_matchtext:      func = getMatchTextId; break;
    case no_matchunicode:   func = getMatchUnicodeId; break;
    case no_matchlength:    func = getMatchLengthId; break;
    case no_matchposition:  func = getMatchPositionId; break;
    case no_matchutf8:      func = getMatchUtf8Id; break;
    default: UNIMPLEMENTED;
    }

    HqlExprArray args;
    args.append(*createQuoted("matched", makeVoidType()));
    args.append(*createConstant((__int64)matchedIndex));
    OwnedHqlExpr call = bindFunctionCall(func, args);
    buildExprOrAssign(ctx, target, call, bound);
}


IReferenceSelector * HqlCppTranslator::doBuildRowMatchRow(BuildCtx & ctx, IHqlExpression * expr, bool isNew)
{
    if (!nlpParse)
        throwError1(HQLERR_MatchedUsedOutsideParse, getOpString(expr->getOperator()));

    if (!ctx.queryMatchExpr(activeNlpMarkerExpr))
        throwError(HQLERR_AccessMatchAttrInChildQuery);

    unsigned matchedIndex = nlpParse->addMatchReference(expr->queryChild(1));

    HqlExprArray args;
    args.append(*createQuoted("matched", makeVoidType()));
    args.append(*createConstant((__int64)matchedIndex));
    OwnedHqlExpr call = bindTranslatedFunctionCall(getMatchRowId, args);

    IHqlExpression * record = expr->queryRecord();
    StringBuffer rowName;
    getUniqueId(rowName.append("row"));
    OwnedHqlExpr row = createVariable(rowName, makeConstantModifier(makeRowReferenceType(record)));
    ctx.addDeclare(row);
    ctx.addAssign(row, call);

    BoundRow * cursor = bindRow(ctx, expr, row);
    if (expr->queryChild(1))
        cursor->setConditional(true);
    return createReferenceSelector(cursor);
}



void HqlCppTranslator::doBuildMatchAttr(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound)
{
    if (!nlpParse)
        throwError1(HQLERR_MatchedUsedOutsideParse, getOpString(expr->getOperator()));

    if (!ctx.queryMatchExpr(activeNlpMarkerExpr) && !ctx.queryMatchExpr(activeProductionMarkerExpr))
    {
        CHqlBoundExpr match;
        if (!buildExprInCorrectContext(ctx, expr, match, false))
            throwError1(HQLERR_MatchedUsedOutsideParse, getOpString(expr->getOperator()));

        if (target)
            assign(ctx, *target, match);
        else
            bound->set(match);
        return;
    }

    HqlExprAssociation * marker = ctx.queryMatchExpr(activeProductionMarkerExpr);
    ITypeInfo * exprType = expr->queryType();
    if (marker)
    {
        HqlExprArray args;
        args.append(*LINK(marker->queryExpr()));
        args.append(*LINK(expr->queryChild(0)));

        IIdAtom * name;
        switch (exprType->getTypeCode())
        {
        case type_string:
            name = getProductionTextId;
            break;
        case type_unicode:
            name = getProductionUnicodeId;
            break;
        case type_utf8:
            name = getProductionUtf8Id;
            break;
        default:
            throwUnexpectedType(exprType);
        }
        OwnedHqlExpr call = bindFunctionCall(name, args);
        doBuildCall(ctx, target, call, bound);
    }
    else
    {
        node_operator op;
        switch (exprType->getTypeCode())
        {
        case type_string:
            op = no_matchtext;
            break;
        case type_unicode:
            op = no_matchunicode;
            break;
        case type_utf8:
            op = no_matchutf8;
            break;
        default:
            throwUnexpectedType(exprType);
        }
        OwnedHqlExpr newExpr = createValue(op, LINK(exprType));
        doBuildMatched(ctx, target, expr, bound);
    }
}

IReferenceSelector * HqlCppTranslator::doBuildRowMatchAttr(BuildCtx & ctx, IHqlExpression * expr)
{
    if (!ctx.queryMatchExpr(activeNlpMarkerExpr) && !ctx.queryMatchExpr(activeProductionMarkerExpr))
        throwError(HQLERR_AccessMatchAttrInChildQuery);

    HqlExprArray args;
    IIdAtom * name;
    HqlExprAssociation * marker = ctx.queryMatchExpr(activeProductionMarkerExpr);
    if (marker)
    {
        name = getProductionResultId;
        args.append(*LINK(marker->queryExpr()));
        args.append(*LINK(expr->queryChild(1)));
    }
    else
    {
        name = getRootResultId;
        args.append(*createQuoted("matched", makeVoidType()));
    }

    OwnedHqlExpr call = bindTranslatedFunctionCall(name, args);

    IHqlExpression * record = expr->queryRecord();
    StringBuffer rowName;
    getUniqueId(rowName.append("row"));

    OwnedITypeInfo rowType = makeConstantModifier(makeRowReferenceType(record));
    rowType.setown(makeAttributeModifier(LINK(rowType), getLinkCountedAttr()));

    OwnedHqlExpr row = createVariable(rowName, rowType.getClear());
    ctx.addDeclare(row);
    ctx.addAssign(row, call);

    BoundRow * cursor = bindRow(ctx, expr, row);
    return createReferenceSelector(cursor);
}

/*

Some special nodes are processed as follows:

  x (a before b) y                  : x -> a -> checkNext(b) -> y
  x (a after b) y                   : x -> checkPrev(b) -> a -> y
  x (a in b) y                      : x -> check(a, b) -> y
  x (a{2,3}) y                      : x -> repeat(a) -> y
  x (a+) y                          : x -> a +-> y
                                           ^-/

Optimization issues:
  o Need information about all named elements that are referenced by MATCHED
  o The match ids are based on the logical named expressions, so if IHqlExpression tree rebuilt, matched need patching.
  o Don't want to re-apply same thing twice - so delay expanding named symbols.
*/
