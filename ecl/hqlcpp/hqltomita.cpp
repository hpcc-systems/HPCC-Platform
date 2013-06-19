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
#include <algorithm>
#include "jliball.hpp"
#include "eclrtl.hpp"
#include "hqlexpr.hpp"
#include "hqlthql.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlutil.hpp"
#include "hqltomita.ipp"
#include "hqlhtcpp.ipp"
#include "thortparse.ipp"
#include "hqlregex.ipp"

//#define DEFAULT_DFA_COMPLEXITY    0
#define DEFAULT_UNICODE_DFA_COMPLEXITY  0
#define DEFAULT_DFA_COMPLEXITY  2000

//===========================================================================

template <class T> 
void cloneLinkedArray(T & target, const T & source)
{
    ForEachItemIn(i, source)
        target.append(OLINK(source.item(i)));
}


//---------------------------------------------------------------------------

TomFeature::TomFeature(IHqlExpression * expr, TomFeatureKind _kind)
{
    def.set(expr);
    kind = _kind;
    mask = 0;
}

StringBuffer & TomFeature::getDebugText(StringBuffer & out)
{
    out.append("  FEATURE ");
    getName(out);
    switch (kind)
    {
    case TomMaskFeature:
        {
            if (values.ordinality())
            {
                out.append(" := ");
                getValue(out);
            }
            out.append(";");
            break;
        }
    case TomStrFeature:
        out.append(" := STRING;");
        break;
    case TomIntFeature:
        out.append(" := INTEGER;");
        break;
    }
    return out.appendf(" // mask(%x)", mask).newline();
}

StringBuffer & TomFeature::getName(StringBuffer & out)
{
    if (def && def->queryName())
        return out.append(def->queryName());
    else
        return out.appendf("anon:%p", this);
}

StringBuffer & TomFeature::getValue(StringBuffer & out)
{
    ForEachItemIn(idx, values)
    {
        if (idx) out.append("|");
        values.item(idx).getName(out);
    }
    return out;
}

StringBuffer & TomFeature::getGuardValue(StringBuffer & out)
{
    if (def)
        return getName(out);
    return getValue(out);
}

//---------------------------------------------------------------------------

StringBuffer & TomGuard::getDebugText(StringBuffer & out)
{
    if (feature)
        feature->getName(out).append("=");
    return out;
}
    
StringBuffer & TomMaskGuard::getDebugText(StringBuffer & out)
{
    TomGuard::getDebugText(out);
    value->getGuardValue(out);
    return out;
}

StringBuffer & TomStrGuard::getDebugText(StringBuffer & out)
{
    return TomGuard::getDebugText(out).append('\'').append(value).append('\'');
}

StringBuffer & TomIntGuard::getDebugText(StringBuffer & out)
{
    return TomGuard::getDebugText(out).append(value);
}

//---------------------------------------------------------------------------

TomToken::TomToken(IHqlExpression * expr, const TomGuardArray & _guards)
{
    pattern.set(expr);
    cloneLinkedArray(guards, _guards);
    id = NotFound;
}


StringBuffer & TomToken::getDebugText(StringBuffer & out)
{
    out.append("  TOKEN<").append(id).append("> ");
    getName(out).append(" ");
    if (pattern)
    {
        IHqlExpression * expr = pattern;
        if (expr->getOperator() == no_pat_instance)
            expr = expr->queryChild(0);
        getExprECL(expr->queryBody(), out.append(" := "));
    }
    return out.append(";").newline();
}

StringBuffer & TomToken::getName(StringBuffer & out)
{
    IHqlExpression * expr = pattern;
    if (!expr)
        return out.append("EOF");
    IAtom * name = expr->queryName();
    if (expr->getOperator() == no_pat_instance)
    {
        name = expr->queryChild(1)->queryName();
        expr = expr->queryChild(0);
    }
    if (name)
        out.append("tok").append(name);
    else if (id != NotFound)
        out.appendf("tok%d", id);
    else
        out.appendf("tok%p", this);
    return out;
}


bool TomTokenSet::add(TomToken & other)
{
    if (tokens.find(other) == NotFound)
    {
        tokens.append(other);
        return true;
    }
    other.Release();
    return false;
}

bool TomTokenSet::add(TomTokenSet & other)
{
    bool changed = false;
    ForEachItemIn(idx, other)
    {
        TomToken & cur = other.item(idx);
        if (tokens.find(cur) == NotFound)
        {
            tokens.append(OLINK(cur));
            changed = true;
        }
    }
    return changed;
}

//---------------------------------------------------------------------------

StringBuffer & outputGuards(StringBuffer & out, const TomGuardArray & guards, const char * prefix, const char * suffix)
{
    if (guards.ordinality())
    {
        out.append(prefix);
        ForEachItemIn(idx, guards)
        {
            if (idx) out.append(",");
            guards.item(idx).getDebugText(out);
        }
        out.append(suffix);
    }
    return out;
}


bool TomStep::calcCanBeNull(bool isRoot, bool & result)
{
    return true;
}

bool TomStep::calcFirst(bool isRoot, TomTokenSet & tokens)
{
    return true;
}

StringBuffer & TomStep::getDebugText(StringBuffer & out)
{
    return out;
}


StringBuffer & TomConditionStep::getDebugText(StringBuffer & out)
{
    out.append("(?");
    if (rule)
        rule->getName(out).append(":");
    getExprECL(condition, out);
    return out.append("?)");
}


TomGuardStep::TomGuardStep(const TomGuardArray & _guards)
{
    cloneLinkedArray(guards, _guards);
}


StringBuffer & TomGuardStep::getDebugText(StringBuffer & out)
{
    return outputGuards(out, guards, "[", "]");
}


TomNonTerminalStep::TomNonTerminalStep(TomRule * _rule, const TomGuardArray & _guards)
{
    rule = _rule;
    cloneLinkedArray(guards, _guards);
}


bool TomNonTerminalStep::calcCanBeNull(bool isRoot, bool & result)
{
    return rule->calcCanBeNull(isRoot, result);
}

bool TomNonTerminalStep::calcFirst(bool isRoot, TomTokenSet & tokens)
{
    return rule->calcFirst(isRoot, tokens);
}

//-- Non Terminal

bool TomNonTerminalStep::canBeNull() 
{ 
    return rule->canBeNull();
}


StringBuffer & TomNonTerminalStep::getDebugText(StringBuffer & out)
{
    rule->getName(out);
    return outputGuards(out, guards, "{", "}");
}


unsigned TomNonTerminalStep::getId() 
{ 
    return rule->getId(); 
}

TomProduction * TomNonTerminalStep::queryExpandRules()
{
    return rule->queryExpand();
}

//-- Penalty

StringBuffer & TomPenaltyStep::getDebugText(StringBuffer & out)
{
    return out.append("PENALTY(").append(penalty).append(")");
}

//-- Terminal

bool TomTerminalStep::calcFirst(bool isRoot, TomTokenSet & tokens)
{
    tokens.add(*LINK(token));
    return true;
}

StringBuffer & TomTerminalStep::getDebugText(StringBuffer & out)
{
    return token->getName(out);
}

//-- Use --

TomUseStep::TomUseStep(IHqlExpression * _expr, const TomGuardArray & _guards)
{
    expr.set(_expr);
    cloneLinkedArray(guards, _guards);
}


TomStep * TomUseStep::expandRecursion(IResolveContext & ctx)
{
    TomRule * replacement;
    assertex(expr->queryChild(0)->queryValue());
    replacement = ctx.queryDefine(expr->queryChild(0));
    replacement->addUse();
    return new TomNonTerminalStep(replacement, guards);
}

//---------------------------------------------------------------------------

void TomProduction::buildProduction(LRTableBuilder & builder, IHqlExpression * test, GenerateTomitaCtx & ctx)
{
    builder.addProduction(id, rule->getId(), rule->queryName(), steps.ordinality(), penalty, transformClonesFirstSymbol());
    if (test)
    {
        switch (test->getOperator())
        {
        case no_pat_first:
            builder.addValidator(id, LRVfirst, 0, 0, NULL);
            break;
        case no_pat_last:
            builder.addValidator(id, LRVlast, 0, 0, NULL);
            break;
        case no_pat_validate:
            {
                ValidateKind validatorKind = getValidateKind(ctx.regex.queryValidateExpr(test));
                byte op;
                switch (ctx.info.type)
                {
                case type_unicode:
                case type_utf8:
                    op = (validatorKind != ValidateIsString) ? LRVvalidateuni : LRVvalidateasc;
                    break;
                default:
                    op = (validatorKind != ValidateIsUnicode) ? LRVvalidateasc : LRVvalidateuni;
                }
                builder.addValidator(id, op, ctx.regex.getValidatorIndex(test), 0, NULL);
                break;
            }
        case no_pat_checklength:
            {
                unsigned minLength = 0;
                unsigned maxLength = PATTERN_UNLIMITED_LENGTH;
                getCheckRange(test->queryChild(1), minLength, maxLength, ctx.info.charSize);
                builder.addValidator(id, LRVchecklength, minLength, maxLength, NULL);
                break;
            }
        case no_pat_checkin:
            {
                UNIMPLEMENTED;
                AsciiDfa * dfa = NULL;
                builder.addValidator(id, LRVcheckin, 0, 0, dfa);
                break;
            }
        case no_pat_x_before_y:
        case no_pat_before_y:
            {
                UNIMPLEMENTED;
                AsciiDfa * dfa = NULL;
                builder.addValidator(id, LRVbefore, 0, 0, dfa);
                break;
            }
        case no_pat_x_after_y:
        case no_pat_after_y:
            {
                UNIMPLEMENTED;
                unsigned minLength = 0;
                unsigned maxLength = 0;
                AsciiDfa * dfa = NULL;
                builder.addValidator(id, LRVafter, minLength, maxLength, dfa);
                break;
            }
        default:
            UNIMPLEMENTED;
        }
    }
}


void TomProduction::buildReduce(LRTableBuilder & builder)
{
    rule->buildReduce(builder, this);
}

bool TomProduction::calcCanBeNull(bool isRoot, bool & result)
{
    ForEachItemIn(idx, steps)
    {
        if (!steps.item(idx).calcCanBeNull(isRoot, result))
            return false;
        if (!result)
            return false;
    }
    result = true;
    return true;
}


bool TomProduction::canBeNull()
{
    bool result = false;
    calcCanBeNull(true, result);
    return result;
}


bool TomProduction::calcFirst(bool isRoot, TomTokenSet & result)
{
    bool known = true;
    ForEachItemIn(idx, steps)
    {
        TomStep & cur = steps.item(idx);
        if (!steps.item(idx).calcFirst(isRoot, result))
            known = false;
        if (!cur.canBeNull())
            break;
    }
    return known;
}


bool TomProduction::calcFollow()
{
    bool changed = false;
    unsigned max = steps.ordinality();
    ForEachItemIn(idx, steps)
    {
        TomStep & cur = steps.item(idx);
        TomRule * curRule = cur.queryRule();
        if (curRule)
        {
            unsigned nextIdx = idx+1;
            for (;nextIdx < max; nextIdx++)
            {
                TomStep & next = steps.item(nextIdx);
                if (curRule->gatherFollow(next))
                    changed = true;
                if (!next.canBeNull())
                    break;
            }
            if (nextIdx == max)
                if (curRule->gatherFollow(*rule))
                    changed = true;
        }
    }
    return changed;
}


bool TomProduction::transformClonesFirstSymbol()
{
    if (transform)
        return false;

    if (!rule->queryRecord())
        return false;

    return queryStep(0) != NULL;
}


void TomProduction::expandRecursion(IResolveContext & ctx)
{
    ForEachItemIn(idx, steps)
    {
        TomStep * replacement = steps.item(idx).expandRecursion(ctx);
        if (replacement)
            steps.replace(*replacement, idx);
    }
}


StringBuffer & TomProduction::getDebugText(StringBuffer & out)
{
    out.appendf("    Production<%d>: [", id);

    ForEachItemIn(idx, localFeatures)
    {
        if (idx) out.append(",");
        localFeatures.item(idx).getName(out);
    }
    out.append("]");
    if (penalty)
        out.append("PENALTY(").append(penalty).append(")");
    if (transformClonesFirstSymbol())
        out.append(" CloningTransform");
    out.append(" := ");
    ForEachItemIn(i, steps)
    {
        if (i) out.append(" ");
        steps.item(i).getDebugText(out);
    }
    return out.newline();
}


bool TomProduction::isSimple(bool singleUse)
{
    if (localFeatures.ordinality() || transform)
        return false;
    if (singleUse)
        return true;
    return steps.ordinality()<=1;
}

void TomProduction::optimizeRules()
{
    ForEachItemInRev(idx, steps)
    {
        Linked<TomProduction> other = steps.item(idx).queryExpandRules();
        if (other)
        {
            steps.remove(idx);
            penalty += other->penalty;
            ForEachItemIn(i,other->steps)
                steps.add(OLINK(other->steps.item(i)), idx+i);
            //MORE: Should inc usage count on rules that are cloned when expanding simple.
            //MORE: Should probably do in two passes i) expand if used once.  ii) expand simple
        }
    }
}


//---------------------------------------------------------------------------

TomRule::TomRule(IHqlExpression * expr, IAtom * _name, const TomFeatureArray & _features, bool _implicit, bool _isMatched)
{
    def.set(expr);
    cloneLinkedArray(features, _features);
    implicit = _implicit;
    id = 0;
    isNullState = ValueUnknown;
    firstState = ValueUnknown;
    curExpandState = (unsigned)-1;
    numUses = 0;

    IHqlExpression * body = expr;
    cachedName = _name;
    if (body)
    {
        //skip attribute used to mark an implicit rule
        if (body->isAttribute())
            body = body->queryChild(0);
        if (body->getOperator() == no_pat_featureparam)
            body = body->queryChild(0);
        if (body->getOperator() == no_define)
            defineName.set(body->queryChild(1));
    }
    isMatched = _isMatched;
}


void TomRule::buildProductions(LRTableBuilder & builder, GenerateTomitaCtx & ctx)
{
    ForEachItemIn(idx, productions)
        productions.item(idx).buildProduction(builder, test, ctx);
}

void TomRule::buildReduce(LRTableBuilder & builder, TomProduction * production)
{
    ForEachItemIn(idx, follow)
        builder.addReduce(follow.item(idx).getId(), production->getId());
}

bool TomRule::canBeNull()
{
    bool ret = false;
    calcCanBeNull(true, ret);
    return ret;
}


bool TomRule::calcCanBeNull(bool isRoot, bool & result)
{
    if (isNullState == ValueKnown)
    {
        result = isNull;
        return true;
    }
    if (isNullState == ValueRecursive)
        return false;

    isNullState = ValueRecursive;
    isNull = false;
    bool known = true;
    ForEachItemIn(idx, productions)
    {
        if (productions.item(idx).calcCanBeNull(false, isNull))
        {
            // Short circuit....
            if (isNull)
            {
                result = isNull;
                isNullState = ValueKnown;
                return true;
            }
        }
        else
            known = false;
    }

    if (isNull)
        result = true;
    if (!known && !isRoot)
    {
        isNullState = ValueUnknown;
        return false;
    }

    isNullState = ValueKnown;
    return true;
}


bool TomRule::calcFirst(bool isRoot, TomTokenSet & result)
{
    if (firstState == ValueKnown)
    {
        result.add(first);
        return true;
    }
    if (firstState == ValueRecursive)
        return false;

    firstState = ValueRecursive;
    first.kill();
    bool known = true;
    ForEachItemIn(idx, productions)
    {
        if (!productions.item(idx).calcFirst(false, first))
            known = false;
    }
    result.add(first);
    if (!known && !isRoot)
    {
        firstState = ValueUnknown;
        return false;
    }

    firstState = ValueKnown;
    return true;
}


bool TomRule::calcFollow()
{
    bool changed = false;
    ForEachItemIn(idx, productions)
        if (productions.item(idx).calcFollow())
            changed = true;
    return changed;
}


void TomRule::expandRecursion(IResolveContext & ctx)
{
    ForEachItemIn(idx, productions)
        productions.item(idx).expandRecursion(ctx);
}


bool TomRule::gatherFollow(TomRule & next)
{
    unsigned num = follow.ordinality();
    follow.add(next.follow);
    return (num != follow.ordinality());
}

bool TomRule::gatherFollow(TomStep & next)
{
    unsigned num = follow.ordinality();
    next.calcFirst(true, follow);
    return (num != follow.ordinality());
}

StringBuffer & TomRule::getDebugText(StringBuffer & out)
{
    out.append("  Rule<").append(id).append("> ");
    getName(out);
    if (features.ordinality())
    {
        out.append("{");
        ForEachItemIn(idx, features)
        {
            if (idx) out.append(",");
            features.item(idx).getName(out);
        }
        out.append("}");
    }
    if (test)
    {
        out.append(" Test: (");
        getExprECL(test, out);
        out.append(")");
    }
    out.newline();
    out.appendf("    CanBeNull(%d) First[", canBeNull());
    if (firstState == ValueUnknown)
        out.append("?");

    ForEachItemIn(i1, first)
    {
        if (i1) out.append(" ");
        out.append(first.item(i1).getId());
    }
    out.append("] Follow[");
    ForEachItemIn(i2, follow)
    {
        if (i2) out.append(" ");
        out.append(follow.item(i2).getId());
    }
    out.append("]").newline();
    ForEachItemIn(idx, productions)
        productions.item(idx).getDebugText(out);
    return out;
}


void TomRule::getFeatures(TomFeatureArray & target)
{
    cloneLinkedArray(target, features);
}

StringBuffer & TomRule::getName(StringBuffer & out)
{
    IHqlExpression * expr = def;
    if (expr && expr->isAttribute())
        expr = expr->queryChild(0);
    if (cachedName)
        out.append(cachedName);
    else if (id)
        out.appendf("rule%d", id);
    else
        out.appendf("rule%p", this);
    return out;
}

bool TomRule::matchesDefine(IHqlExpression * name)
{
    return (name == defineName);
}

void TomRule::optimizeRules()
{
    ForEachItemIn(idx, productions)
        productions.item(idx).optimizeRules();
}


TomProduction * TomRule::queryExpand()
{
    if (productions.ordinality() != 1)
        return NULL;
    if (cachedName && isMatched)
        return NULL;
    if (test)
        return NULL;
    TomProduction * first = &productions.item(0);
    if (first->isSimple(numUses == 1))
        return first;
    return NULL;
}

IHqlExpression * TomRule::queryRecord()
{
    if (def)
    {
        if (def->isAttribute() && def->queryName() == tomitaAtom)
            return def->queryChild(0)->queryRecord();
        return def->queryRecord();
    }
    return NULL;
}

void TomRule::setProductionIds(HqlExprArray & productionMappings, unsigned & id)
{
    ForEachItemIn(idx, productions)
    {
        TomProduction & cur = productions.item(idx);
        cur.setId(id++);

        LinkedHqlExpr mapTo = cur.queryTransform();
        if (!mapTo && def)
        {
            IHqlExpression * record = queryRecord();

            if (record)
            {
                if (!cur.queryStep(0))
                {
                    //create a default transform to clear the record.
                    mapTo.setown(createNullTransform(record));
                }
                else
                {
                    //Check there is only a single input symbol which has an associated record
                    TomRule * defaultRule = NULL;
                    for (unsigned i=0;;i++)
                    {
                        TomStep * curStep = cur.queryStep(i);
                        if (!curStep)
                            break;

                        TomRule * curRule = curStep->queryRule();
                        if (curRule && curRule->queryRecord())
                        {
                            if (defaultRule)
                                throwError(HQLERR_ExpectedTransformManyInputs);
                            defaultRule = curRule;
                        }
                    }
                    if (!defaultRule || !recordTypesMatch(defaultRule->queryRecord(), record))
                        throwError(HQLERR_ExpectedTransformManyInputs);

                    //If so, then don't generate a transform since the input record can be cloned.
                    mapTo.set(record);
                }
            }
        }

        if (mapTo)
            productionMappings.append(*createValue(no_mapto, makeVoidType(), createConstant((__int64)cur.getId()), LINK(mapTo)));
    }
}

//---------------------------------------------------------------------------


TomitaContext::TomitaContext(IHqlExpression * _expr, IWorkUnit * _wu, const HqlCppOptions & _options, ITimeReporter * _timeReporter) 
: NlpParseContext(_expr, _wu, _options, _timeReporter), parser(NULL), translatorOptions(_options)
{
    numTerminals = 0;
    numSymbols = 0;
    numProductions = 0;
    done = NULL;
    gotos = NULL;
    maximizeLexer = _options.maximizeLexer;
    grammarAmbiguous = false;
}


TomitaContext::~TomitaContext()
{
}


void TomitaContext::addLexerTerminator(IHqlExpression * expr)
{
    //NB: expr is know to be valid...
    switch (expr->getOperator())
    {
    case no_list:
        {
            ForEachChild(idx, expr)
                addLexerTerminator(expr->queryChild(idx));
            break;
        }
    case no_constant:
        {
            ITypeInfo * type = expr->queryType();
            const void * value = expr->queryValue()->queryValue();
            switch (type->getTypeCode())
            {
            case type_data:
            case type_string:
                parser.endTokenChars.append(*(const byte *)value);
                break;
            case type_utf8:
                assertex(rtlUtf8Size(1, value) == 1);
                parser.endTokenChars.append(*(const byte *)value);
                break;
            case type_unicode:
                UNIMPLEMENTED;
            }
            break;
        }
    }
}


unsigned TomitaContext::addMatchReference(IHqlExpression * expr)
{
    if (expr && expr->queryType()->getTypeCode() == type_pattern)
    {
        StringBuffer s;
        s.append(expr->queryName());
        if (!s.length())
            getExprECL(expr, s);
        throwError1(HQLWRN_TomitaMatchPattern, s.str());
    }
    return NlpParseContext::addMatchReference(expr);
}

void TomitaContext::associateIds()
{
    unsigned id=0;
    ForEachItemIn(idx1, tokens)
        tokens.item(idx1).setId(id++);
    numTerminals = id;

    ForEachItemIn(idx2, rules)
        rules.item(idx2).setId(id++);
    numSymbols = id;
}


void TomitaContext::calculateFeatureMasks()
{
}


TomFeature * TomitaContext::createFeature(IHqlExpression * expr)
{
    TomFeature * feature = queryFeature(expr);
    if (feature)
        return feature;

    assertex(expr->getOperator() == no_pat_featuredef);
    IHqlExpression * def = expr->queryChild(0);
    TomFeatureKind kind = TomMaskFeature;
    if (def->getOperator() == no_null)
    {
        switch (def->queryType()->getTypeCode())
        {
        case type_string:
            kind = TomStrFeature;
            break;
        case type_int:
            kind = TomIntFeature;
            break;
        }
    }
    feature = new TomFeature(expr, kind);
    if (def->getOperator() != no_null)
    {
        HqlExprArray args;
        def->unwindList(args, no_pat_or);
        ForEachItemIn(idx, args)
            feature->addValue(createFeature(&args.item(idx)));
    }
    features.append(*feature);
    return feature;
}

TomFeature * TomitaContext::createFeatureValue(IHqlExpression * expr)
{
    TomFeature * feature = queryFeature(expr);
    if (feature)
        return feature;

    feature = new TomFeature(expr, TomMaskFeature);
    HqlExprArray args;
    expr->unwindList(args, no_pat_or);
    ForEachItemIn(idx, args)
        feature->addValue(createFeature(&args.item(idx)));
    features.append(*feature);
    return feature;
}

void TomitaContext::createFeatures(TomFeatureArray & target, IHqlExpression * expr)
{
    if (!expr)
        return;
    HqlExprArray args;
    expr->unwindList(args, no_comma);
    ForEachItemIn(idx, args)
        target.append(OLINK(*createFeature(&args.item(idx))));
}

TomGuard * TomitaContext::createGuard(IHqlExpression * featureExpr, IHqlExpression * valueExpr)
{
    TomFeature * feature = NULL;
    if (featureExpr)
        feature = createFeature(featureExpr);

    switch (valueExpr->queryType()->getTypeCode())
    {
    case type_feature:
        {
            TomFeature * value = createFeatureValue(valueExpr);
            return new TomMaskGuard(feature, value);
        }
    case type_string:
        {
            StringBuffer text;
            valueExpr->queryValue()->getStringValue(text);
            //MORE: case sensitivity/unicode
            IAtom * value = createAtom(text);
            return new TomStrGuard(feature, value);
        }
    case type_int:
        {
            unsigned value = (unsigned)getIntValue(valueExpr);
            return new TomIntGuard(feature, value);
        }
    default:
        UNIMPLEMENTED;
    }
}

void TomitaContext::createGuards(TomGuardArray & guards, IHqlExpression * expr)
{
    if (!expr)
        return;
    HqlExprArray args;
    expr->unwindList(args, no_comma);

    ForEachItemIn(idx, args)
    {
        IHqlExpression & cur = args.item(idx);
        assertex(cur.getOperator() == no_eq);
        TomGuard * guard = createGuard(cur.queryChild(0), cur.queryChild(1));
        guards.append(*guard);
    }
}

void TomitaContext::createImplicitProduction(TomRule * self, TomRule * rule, IHqlExpression * expr, bool expandTokens)
{
    switch (expr->getOperator())
    {
    case no_pat_repeat:
        if (!expandTokens)
        {
            IHqlExpression * action = expr->queryChild(0);
            unsigned low = getRepeatMin(expr);
            unsigned high = getRepeatMax(expr);
            TomProduction * first = new TomProduction(rule);
            TomProduction * last = NULL;
            if ((low == 0) && (high == 1))
            {
                last = new TomProduction(rule);
                createSteps(self, last, action, expandTokens);
            }
            else
            {
                TomRule * actionRule = createImplicitRule(self, action, expandTokens);
                TomGuardArray guards;
                for (unsigned i = 0; i < low; i++)
                    first->addStepOwn(new TomNonTerminalStep(actionRule, guards));
                if (high != low)
                {
                    if (high == (unsigned)-1)
                    {
                        last = new TomProduction(rule);
                        last->addStepOwn(new TomNonTerminalStep(rule, guards));
                        last->addStepOwn(new TomNonTerminalStep(actionRule, guards));
                    }
                    else
                    {
                        if (high - low > 5)
                            throwError(HQLERR_ArbitaryRepeatUnimplemented);
                        for (unsigned steps = low+1; steps <= high; steps++)
                        {
                            TomProduction * next = new TomProduction(rule);
                            for (unsigned i = 0; i < steps; i++)
                                next->addStepOwn(new TomNonTerminalStep(actionRule, guards));
                            rule->addProductionOwn(next);
                        }
                    }
                }
            }
            rule->addProductionOwn(first);
            if (last) rule->addProductionOwn(last);
        }
        else
            createProduction(self, rule, expr, expandTokens);
        break;

    default:
        createProduction(self, rule, expr, expandTokens);
        break;
    }
}

TomRule * TomitaContext::createImplicitRule(TomRule * self, IHqlExpression * expr, bool expandTokens)
{
    OwnedHqlExpr wrapped = createAttribute(tomitaAtom, LINK(expr));
    TomRule * rule = queryRule(wrapped, NULL);
    if (!rule)
    {
        TomFeatureArray features;
        activeRules.tos().getFeatures(features);
        rule = new TomRule(wrapped, NULL, features, true, false);
        rules.append(*rule);
        activeRules.append(*rule);

        LinkedHqlExpr body = expr;
        switch (expr->getOperator())
        {
        case no_pat_first:
        case no_pat_last:
        case no_pat_validate:
        case no_pat_checklength:
        case no_pat_checkin:
        case no_pat_x_before_y:
        case no_pat_before_y:
        case no_pat_x_after_y:
        case no_pat_after_y:
            {
                rule->setTest(expr);
                body.set(body->queryChild(0));
                if (!body)
                    body.setown(createValue(no_null, makePatternType()));
                break;
            }
        }

        HqlExprArray args;
        body->unwindList(args, no_pat_or);
        ForEachItemIn(idx, args)
            createImplicitProduction(self, rule, &args.item(idx), expandTokens);
        activeRules.pop();
    }
    return rule;
}


void TomitaContext::createProduction(TomRule * self, TomRule * rule, IHqlExpression * expr, bool expandTokens)
{
    TomProduction * production = new TomProduction(rule);
    createSteps(self, production, expr, expandTokens);
    rule->addProductionOwn(production);
}

void TomitaContext::createProductions(TomRule * rule, IHqlExpression * expr, bool expandTokens)
{
    HqlExprArray args;
    expr->unwindList(args, no_pat_or);
    ForEachItemIn(idx, args)
        createProduction(rule, rule, &args.item(idx), expandTokens);
}


TomRule * TomitaContext::createRule(IHqlExpression * expr, IAtom * name, bool expandTokens)
{
    TomRule * rule = queryRule(expr, name);
    if (!rule)
    {
        TomFeatureArray features;
        IHqlExpression * def = expr;
        if (def->getOperator() == no_pat_featureparam)
        {
            createFeatures(features, def->queryChild(1));
            def = def->queryChild(0);
        }
        if (def->getOperator() == no_define)
            def = def->queryChild(0);

        rule = new TomRule(expr, name, features, false, isMatched(expr, name));
        rules.append(*rule);
        activeRules.append(*rule);
        createProductions(rule, def, expandTokens);
        activeRules.pop();
    }
    rule->addUse();
    return rule;
}


void TomitaContext::createStepAsImplicitRule(TomRule * self, TomProduction * production, IHqlExpression * expr, bool expandTokens)
{
    TomRule * rule = createImplicitRule(self, expr, expandTokens);
    TomGuardArray guards;
    production->addStepOwn(new TomNonTerminalStep(rule, guards));
}


void TomitaContext::createSteps(TomRule * self, TomProduction * production, IHqlExpression * expr, bool expandTokens)
{
    switch (expr->getOperator())
    {
    case no_pat_instance:
        {
            IHqlExpression * def = expr->queryChild(0);
            bool canExpand;
            if (maximizeLexer)
                canExpand = canCreateLexerToken(def, false);
            else
                canExpand = isToken(def, expandTokens);
            if (!canExpand)
            {
                if (expr->hasAttribute(_function_Atom))
                    createSteps(self, production, def, expandTokens);
                else
                {
                    TomGuardArray guards;
                    if (def->getOperator() == no_pat_featureactual)
                    {
                        HqlExprArray actuals;
                        def->queryChild(1)->unwindList(actuals, no_comma);
                        ForEachItemIn(idx, actuals)
                            guards.append(*createGuard(NULL, &actuals.item(idx)));
                        def = def->queryChild(0);
                    }
                    TomRule * rule = createRule(def, expr->queryChild(1)->queryName(), expandTokens);
                    production->addStepOwn(new TomNonTerminalStep(rule, guards));
                    //MORE: Need to add guards to localFeatures
                }
                return;
            }
            break;
        }
    case no_pat_or:
        //MORE: This changes when we want to extract guard information...
        if (!expandTokens)
        {
            if (expr->numChildren() == 1)
                createSteps(self, production, expr->queryChild(0), expandTokens);
            else
                createStepAsImplicitRule(self, production, expr, expandTokens);
            return;
        }
        break;
    case no_pat_guard:
        {
            TomGuardArray guards;
            createGuards(guards, expr->queryChild(0));
            production->addStepOwn(new TomGuardStep(guards));
            return;
        }
    case no_penalty:
        production->addPenalty((int)getIntValue(expr->queryChild(0)));
        return;
    case no_null:
        //production->addStepOwn(new TomNullStep);
        return;
    case no_pat_case:
    case no_pat_nocase:
        expandTokens = true;
        break;
    case no_pat_first:
    case no_pat_last:
        createStepAsImplicitRule(self, production, expr, expandTokens);
        return;
    }

    if (expandTokens)
    {
        TomToken * token = createToken(expr);
        production->addStepOwn(new TomTerminalStep(token));
        return;
    }

    switch (expr->getOperator())
    {
    case no_pat_imptoken:
    case no_pat_token:
        {
            createSteps(self, production, expr->queryChild(0), true);
            break;
        }
    case no_pat_follow:
        {
            ForEachChild(i, expr)
                createSteps(self, production, expr->queryChild(i), expandTokens);
            break;
        }
    case no_pat_repeat:
        {
            createStepAsImplicitRule(self, production, expr, expandTokens);
            break;
        }
    case no_pat_use:
        {
            //MORE: Should guards be allowed?
            TomGuardArray guards;
            production->addStepOwn(new TomUseStep(expr, guards));
        }
        break;
    case no_self:
        {
            //MORE: Should guards be allowed?
            TomGuardArray guards;
            production->addStepOwn(new TomNonTerminalStep(self, guards));
            break;
        }
    case no_pat_instance:
        createSteps(self, production, expr->queryChild(0), expandTokens);
        break;
    case no_pat_validate:
    case no_pat_checklength:
    case no_pat_checkin:
    case no_pat_x_before_y:
    case no_pat_before_y:
    case no_pat_x_after_y:
    case no_pat_after_y:
        createStepAsImplicitRule(self, production, expr, expandTokens);
        break;
    case no_implicitcast:
        //can sometimes occur when parameters are bound
        createSteps(self, production, expr->queryChild(0), expandTokens);
        break;
    case no_pat_production:
        createSteps(self, production, expr->queryChild(0), expandTokens);
        assertex(!production->transform);
        production->transform.set(expr->queryChild(1));
        break;
    case no_penalty:
        //already handled
        break;
    default:
        UNIMPLEMENTED;
    }

}

TomToken * TomitaContext::createToken(IHqlExpression * expr)
{
    ForEachItemIn(idx, tokens)
    {
        TomToken & cur = tokens.item(idx);
        if (cur.matches(expr))
            return &cur;
    }

    TomGuardArray guards;
    if (expr->getOperator() == no_pat_follow)
    {
        ForEachChild(idx, expr)
        {
            IHqlExpression * cur = expr->queryChild(idx);
            if (cur->getOperator() == no_pat_guard)
                createGuards(guards, cur->queryChild(0));
        }
    }
    TomToken * token = new TomToken(expr, guards);
    tokens.append(*token);
    return token;
}



void TomitaContext::compileSearchPattern()
{
    if (searchType() == type_unicode)
        throwError(HQLERR_TomitaNoUnicode);

    checkValidMatches();
    IHqlExpression * grammar = expr->queryChild(2);
    IHqlExpression * rootSymbol = grammar->queryChild(0);
    assertex(grammar->queryType()->getTypeCode() != type_pattern);
    grammarRule.set(createRule(rootSymbol, grammar->queryChild(1)->queryName(), false));

    //Create an augmented grammar G' with rule S' := S;
    TomFeatureArray noFeatures;
    TomGuardArray noGuards;
    rootRule.setown(new TomRule(NULL, NULL, noFeatures, true, false));
    rootProduction.setown(new TomProduction(rootRule));
    rootProduction->addStepOwn(new TomNonTerminalStep(grammarRule, noGuards));
    rootProduction->setRoot();
    rootRule->addProductionOwn(LINK(rootProduction));
    rules.append(*LINK(rootRule));

    eofToken.setown(new TomToken(NULL, noGuards));
    tokens.append(*LINK(eofToken));
    OwnedHqlExpr nullExpr = createValue(no_null);

    //MORE, create rules for used list...
    expandRecursion();
    optimizeRules();
    associateIds();
    calculateFeatureMasks();
    generateLexer();
    generateParser();
    setParserOptions(parser);
    compileMatched(parser);
}


void TomitaContext::generateLexer()
{
    //nested scope
    try
    {
        RegexContext regex(expr, wu(), translatorOptions, timeReporter, NLPAregexStack);

        regex.beginLexer();
        ForEachItemIn(idx, tokens)
        {
            TomToken & cur = tokens.item(idx);
            if (&cur != eofToken)
                regex.addLexerToken(cur.id, cur.pattern);
        }
        AsciiDfaBuilder builder(parser.tokenDfa);
        regex.generateLexer(&builder);
    }
    catch (IException * e)
    {
        StringBuffer s;
        e->errorMessage(s);
        e->Release();
        throwError1(HQLERR_TomitaPatternTooComplex, s.str());
    }

    IHqlExpression * separator = expr->queryAttribute(separatorAtom);
    if (separator)
    {
        RegexContext regex2(expr, wu(), translatorOptions, timeReporter, NLPAregexStack);

        regex2.beginLexer();
        regex2.addLexerToken(1, separator->queryChild(0));
        AsciiDfaBuilder builder(parser.skipDfa);
        regex2.generateLexer(&builder);
    }
    else
        parser.skipDfa.setEmpty();

    IHqlExpression * terminator = expr->queryAttribute(terminatorAtom);
    if (terminator)
        addLexerTerminator(terminator->queryChild(0));

    parser.eofId = eofToken->getId();
}


//-- Parser generation -------------------------------------------------

//---------------------------------------------------------------------------

inline int compareLRItem(HqlLRItem * left, HqlLRItem * right) 
{
    if (left->production->seq < right->production->seq)
        return -1;
    if (left->production->seq > right->production->seq)
        return +1;
    if (left->index < right->index)
        return -1;
    if (left->index > right->index)
        return +1;
    return 0;
}

int compareLRItem(CInterface * * left, CInterface * * right) 
{ 
    return compareLRItem((HqlLRItem *)*left, (HqlLRItem *)*right);
}


void HqlLRItemSet::add(HqlLRItem * value)
{
    bool isNew;
    CInterface * castValue = value;
    values.bAdd(castValue, compareLRItem, isNew);
    if (!isNew)
        value->Release();
}


int HqlLRItemSet::compare(const HqlLRItemSet & other) const
{
    unsigned numThis = values.ordinality();
    unsigned numOther = other.values.ordinality();
    unsigned numCommon = std::min(numThis, numOther);
    for (unsigned i = 0; i < numCommon; i++)
    {
        HqlLRItem & left = values.item(i);
        HqlLRItem & right = other.values.item(i);
        int c = compareLRItem(&left, &right);
        if (c)
            return c;
    }
    if (numThis > numOther)
        return +1;
    else if (numThis < numOther)
        return -1;
    else
        return 0;
}

bool HqlLRItemSet::equals(const HqlLRItemSet & other) const
{
    unsigned numThis = values.ordinality();
    unsigned numOther = other.values.ordinality();
    if (numThis != numOther)
        return false;
    for (unsigned i = 0; i < numThis; i++)
    {
        if (compareLRItem(&values.item(i), &other.values.item(i)) == 0)
            return false;
    }
    return true;
}

StringBuffer & HqlLRGoto::getDebugText(StringBuffer & out)          
{ 
    return out.append(symbol).append("->").append(state->id); 
}

void HqlLRState::addItem(TomProduction * production, unsigned idx)
{
    HqlLRItem * next = new HqlLRItem(production, idx);
    items.add(next);
}

void HqlLRState::addGoto(unsigned id, HqlLRState * state)
{
    gotos.append(*new HqlLRGoto(id, state));
}


void HqlLRState::buildNullReductions(LRTableBuilder & builder, TomRule * rule)
{
    ForEachItemIn(i2, rule->productions)
    {
        TomProduction & curProd = rule->productions.item(i2);
        if (curProd.canBeNull())
            curProd.buildReduce(builder);
        TomStep * first = curProd.queryStep(0);
        if (first)
        {
            TomRule * stepRule = first->queryRule();
            if (stepRule)
                buildNullReductions(builder, stepRule);
        }
    }
}

void HqlLRState::buildStateItem(LRTableBuilder & builder, TomProduction * production, unsigned index)
{
    TomStep * step = production->queryStep(index);
    if (step)
    {
        if (step->isTerminal())
        {
            TomTokenSet tokens;
            step->calcFirst(true, tokens);
            ForEachItemIn(idx, tokens)
            {
                TomToken & cur = tokens.item(idx); 
                HqlLRState * gotoState = queryGoto(cur.getId());
                assertex(gotoState);
                builder.addShift(cur.getId(), gotoState->id);
            }
        }
        else
        {
            //Need to be careful.  If the a.Xb is in kernal, and X-> is a production then need to include reduce X
            TomRule * rule = step->queryRule();
            if (rule)
            {
                if (!rule->alreadyExpanded(id))
                {
                    ForEachItemIn(i2, rule->productions)
                        buildStateItem(builder, &rule->productions.item(i2), 0);
                }
            }
        }
    }
    else
        production->buildReduce(builder);
}

void HqlLRState::buildState(LRTableBuilder & builder, TomToken * eofToken, unsigned numTerminals)
{
    builder.beginState(id);
    ForEachItemIn(idx, items)
    {
        HqlLRItem & cur = items.item(idx);
        TomProduction * production = cur.production;
        buildStateItem(builder, production, cur.index);
        if (production->isRootReduction() && (cur.index == 1))
            builder.addAccept(eofToken->getId());
    }
    ForEachItemIn(idx2, gotos)
    {
        HqlLRGoto & cur = gotos.item(idx2);
        if (cur.symbol >= numTerminals)
            builder.addGoto(cur.symbol, cur.state->id);
    }

    builder.endState();
}

StringBuffer & HqlLRState::getDebugText(StringBuffer & out)
{
    out.appendf("\t[%d] I={", id);
    ForEachItemIn(idx, items)
    {
        if (idx) out.append(",");
        items.item(idx).getDebugText(out);
    }
    out.append("} [");
    ForEachItemIn(idx2, gotos)
    {
        if (idx2) out.append(",");
        gotos.item(idx2).getDebugText(out);
    }
    return out.append("]").newline();
}


HqlLRState * HqlLRState::queryGoto(token_id id)
{
    ForEachItemIn(idx, gotos)
    {
        HqlLRGoto & cur = gotos.item(idx);
        if (cur.symbol == id)
            return cur.state;
    }
    return NULL;
}


static inline int compareState(HqlLRState * left, HqlLRState * right) 
{ 
    return left->items.compare(right->items);
}

static int compareState(CInterface * * left, CInterface * * right) 
{ 
    return compareState((HqlLRState *)*left, (HqlLRState *)*right);
}


void TomitaContext::addGotos(TomProduction * production, unsigned idx)
{
    //This could be done via virtuals if the context was passed around.
    //but the code is a bit messy.
    unsigned numSteps = production->steps.ordinality();
    if (numSteps == idx)
        return;

    TomStep & step = production->steps.item(idx);
    unsigned id = step.getId();
    if (!gotos[id]) gotos[id] = new HqlLRState;
    gotos[id]->addItem(production, idx+1);
    TomRule * rule = step.queryRule();
    if (rule && !done[id])
    {
        done[id] = true;
        ForEachItemIn(idx, rule->productions)
        {
            TomProduction & cur = rule->productions.item(idx);
            addGotos(&cur, 0);
        }
    }
}

HqlLRState * TomitaContext::addState(HqlLRState * state)
{
    bool isNew = false;
    CInterface * castState = state;
    unsigned matchIndex = orderedStates.bAdd(castState, compareState, isNew);
    if (isNew)
        states.append(*LINK(state));
    else
        state->Release();

    return &orderedStates.item(matchIndex);
}

void TomitaContext::calculateStates()
{
    gotos = new HqlLRState * [numSymbols];
    memset(gotos, 0, sizeof(HqlLRState *) * numSymbols);
    done = new bool[numSymbols];

    try
    {
        HqlLRState * root = new HqlLRState;
        root->addItem(rootProduction, 0);
        rootState.set(addState(root));

        for (unsigned idxState = 0; idxState < states.ordinality(); idxState++)
        {
            memset(done, 0, sizeof(bool) * numSymbols);
            HqlLRState & curState = states.item(idxState);
            ForEachItemIn(idx, curState.items)
            {
                HqlLRItem & curItem = curState.items.item(idx);
                addGotos(curItem.production, curItem.index);
            }
            for (unsigned i= 0; i < numSymbols; i++)
            {
                if (gotos[i])
                {
                    HqlLRState * state = addState(gotos[i]);
                    curState.addGoto(i, state);
                    gotos[i] = NULL;
                }
            }
        }
    }
    catch (...)
    {
        for (unsigned i= 0; i < numSymbols; i++)
            delete gotos[i];

        delete [] gotos;
        delete [] done;
        throw;
    }

    delete [] gotos;
    delete [] done;

    ForEachItemIn(i1, states)
        states.item(i1).id = i1;

    unsigned id = 0;
    ForEachItemIn(i2, rules)
        rules.item(i2).setProductionIds(productions, id);
    numProductions = id;
}


void TomitaContext::calculateFollowing()
{
    //first(x) and canBeNull(x) are calculated on request
    
    //Need to be careful about recursion:  keep repeating until no more items added.
    grammarRule->addFollowOwn(*LINK(eofToken));
    loop
    {
        bool changed = false;
        ForEachItemIn(idx, rules)
        {
            if (rules.item(idx).calcFollow())
                changed = true;
        }
        if (!changed)
            break;
    }
}

void TomitaContext::expandRecursion()
{
    ForEachChild(i1, expr)
    {
        IHqlExpression * cur = expr->queryChild(i1);
        if (cur->getOperator() == no_pat_use)
            createRule(cur->queryChild(0), NULL, false);
    }

    ForEachItemIn(idx, rules)
        rules.item(idx).expandRecursion(*this);
}


void TomitaContext::optimizeRules()
{
    ForEachItemIn(idx, rules)
    {
        TomRule & cur = rules.item(idx);
        if (&cur != rootRule)
            cur.optimizeRules();
    }
}


void TomitaContext::generateParser()
{
    //Following the method described in the dragon book. pp....
    calculateStates();
    calculateFollowing();
    generateTables();
    ForEachItemIn(idx, rules)
    {
        TomRule & cur = rules.item(idx);
        idAllocator.setID(cur.def, cur.queryName(), cur.id);
    }
}

void TomitaContext::generateTables()
{
    GenerateTomitaCtx ctx(*this, info, idAllocator);

    LRTableBuilder builder(parser.table);

    //MORE: Walk and call addGoto etc.
    builder.init(states.ordinality(), numTerminals, numSymbols, numProductions);
    ForEachItemIn(idx, states)
        states.item(idx).buildState(builder, eofToken, numTerminals);
    ForEachItemIn(i1, rules)
        rules.item(i1).buildProductions(builder, ctx);
    builder.finished(rootState->id);

    grammarAmbiguous = builder.isAmbiguous();
}

void TomitaContext::getDebugText(StringBuffer & s, unsigned detail)
{
    NlpParseContext::getDebugText(s, detail);
    s.append("\nFeatures:\n");
    ForEachItemIn(i1, features)
        features.item(i1).getDebugText(s);
    s.append("Tokens:\n");
    ForEachItemIn(i2, tokens)
        tokens.item(i2).getDebugText(s);
    s.append("Rules:\n");
    ForEachItemIn(i3, rules)
        rules.item(i3).getDebugText(s);
    s.append("Lexer:\n");
    s.append("\tEndOfToken: [");
    ForEachItemIn(i, parser.endTokenChars)
    {
        char c = parser.endTokenChars.item(i);
        encodeXML(&c, s, ENCODE_WHITESPACE, 1);
    }
    s.append("]").newline();

    s.append("\tToken DFA");
    parser.tokenDfa.toXML(s, detail);
    s.append("\tSkip DFA");
    parser.skipDfa.toXML(s, detail);
    s.append("\nStates:\n");
    ForEachItemIn(i4, states)
        states.item(i4).getDebugText(s);
    s.append("Parser:\n");
    parser.table.trace(s);
}


bool TomitaContext::canCreateLexerToken(IHqlExpression * expr, bool insideOr)
{
    switch (expr->getOperator())
    {
    case no_null:
    case no_pat_const:
    case no_pat_anychar:
    case no_pat_set:
        return true;
    case no_pat_repeat:
        return isStandardRepeat(expr);
    case no_pat_instance:
        return canCreateLexerToken(expr->queryChild(0), insideOr);
        //depends if used for match...
        return false;
    case no_pat_or:
        {
            ForEachChild(idx, expr)
            {
                if (!canCreateLexerToken(expr->queryChild(idx), true))
                    return false;
            }
            return true;
        }
    case no_pat_follow:
        {
            ForEachChild(idx, expr)
            {
                if (!canCreateLexerToken(expr->queryChild(idx), insideOr))
                    return false;
            }
            return true;
        }
    default:
        return false;
    }
}

bool TomitaContext::isToken(IHqlExpression * expr, bool expandTokens)
{
    if (!expandTokens)
    {
/*
        switch (expr->getOperator())
        {
        case no_pat_token:
        case no_pat_imptoken:
            return isToken(expr->queryChild(0), true);
        }
        */
        return false;
    }

    switch (expr->getOperator())
    {
    case no_pat_instance:
    case no_pat_or:
    case no_pat_guard:
    case no_penalty:
    case no_null:
    case no_pat_first:
    case no_pat_last:
        return false;
    default:
        return true;
    }
}
        

TomRule * TomitaContext::queryDefine(IHqlExpression * defineName)
{
    ForEachItemIn(idx, rules)
    {
        TomRule & cur = rules.item(idx);
        if (cur.matchesDefine(defineName))
            return &cur;
    }
    StringBuffer s;
    defineName->toString(s);
    throwError1(HQLERR_DefineUseStrNotFound, s.str());
    return NULL;
}


TomFeature * TomitaContext::queryFeature(IHqlExpression * expr)
{
    //MORE: May need a hash table!
    ForEachItemIn(idx, features)
    {
        TomFeature & cur = features.item(idx);
        if (cur.matches(expr))
            return &cur;
    }
    return NULL;
}


TomRule * TomitaContext::queryRule(IHqlExpression * expr, IAtom * name)
{
    //MORE: May need a hash table!
    ForEachItemIn(idx, rules)
    {
        TomRule & cur = rules.item(idx);
        if (cur.matches(expr, name))
            return &cur;
    }
    return NULL;
}



//---------------------------------------------------------------------------

NlpParseContext * createTomitaContext(IHqlExpression * expr, IWorkUnit * wu, const HqlCppOptions & options, ITimeReporter * timeReporter)
{
    return new TomitaContext(expr, wu, options, timeReporter);
}


/*
ToDo:
* Features
  o Need to process implicit guards and other conditions.
  o Design how feature actuals are represented and bound.
  o May need guards based on other guards - need to review grammars to see, so can guard a production and also match with others.  
    It can be worked around with an intermediate production.

* Give an error if unicode is used with ,parse
* Give an error if a token cannot create a DFA
* Tokens: [+see notes below]
  o Allow before to be included in a token, and use it as the default mechanism for generating tokens.
  o UNKNOWN - pattern matching at different priorities.
* Validation
  o BEFORE, AFTER [+inverse]
  o pattern IN pattern [+inverse]
  o Non-DFA patterns?
  o Inverse of check length
* Revisit node packing, and test with the "boy saw the girl" example
* Reduce number of empty productions e.g., ConnectWord := rule42.  Use a flag to indicate top 
* Productions: Allow the order of the symbols to be changed in the graph that is generated.
* Optimizations:
  o Make position a rolling window so no need to copy
  o best could be processed when packed nodes being built
* LEFT,RIGHT,NONE to specify precedence of operators + remove s/r conflicts

Some notes on the algorithms:

o S->S' in augmented grammar G'

closure(I) :=
1. All items in I.
2. If A->a.Bb in closure(I) and B->c is a production then add B->.c

closure(I)
{
   J := I
   added = [];
   loop
     for each item A->a.Bb in J
        if !added[B]
           added[B] = true;
           foreach production B->c
              if (B->.c) not in J
                 add (B->.c) to J.
   until no mode items added;
}


Kernal: S'->.S and all items without dots on lhs.
Non: items with dots on lhs

No need to store non-kernal items since adding them can never add any kernals + can always regenerate.

goto(I, X) := closure of all items [A->aX.b] such that [A->a.Xb] is in I.

items(G')
    C := closure({S'->.S})
    loop
        for each item I in C
           for each grammar symbol X
              q := goto(I, X);
              if (q is non empty) and q not in C, add q to C.
    until no more items added to C.


Generating the parsing tables:

1. Calculate C := { I... } the items of G'

2. If [A->x.ay] is in Ii and goto(Ii,a) = Ij action[i, a] = shift j;

3. If [A->x.] is in Ii then action[i, a] := reduce by production for all a in FOLLOW(A)  [A may not be S']

4. If [S'->S.] in Ii then action[i, $] = accept;

5. goto transitions: if gotot(Ii, A) = Ij   goto[i, A} := j

6. The initial entry is one containing S'->S


FIRST(x) := set of terminals that begin the strings derived from x
            if (x=>e) then e is also in FIRST(x)

first(x)
{
    if (x is a terminal) result = {x}
    if (x->e) is a production, add e to first(x)
    if (x non terminal) and x->y1y2y3, then first(x) = first(y1) + if(first(y1) includes e, first(y2).....)
       - only add e if ALL y1y2y3 include e.
}

follow(X) := set of terminals x that can occur to the right of non terminal X in some sentinel form.
{
    follow(S) = {$}
    repeat
        if a production A->aBb then all in FIRST(b) (except e) is added to follow B.
        if a production A->aB or A->aBb where FIRST(b) includes e. then add everything in follow(A) to follow(B)
    until nothing added to any follow set.
}


Tokens:
  * Each token should have an optional set of characters that can or can not follow it.
  * When a token is potentially accepted it should check the set
    i) If specified, and matches then insert a token.
    ii) at end if not-specified then insert a token.
  * repeats aren't specified.

Reducing the tables:

  i) if rows of the action table are identical, then they can be commoned up

  ii) errors can be converted to reductions without problem.

  iii) can have a list of (symbol, action) pairs instead of an array.


Ambiguous grammars:
  i) on shift/reduce.  Compare precedence of operator being shifted with right most operator in the reduction (or user defined)

  ii) associativity.  If the same precedence then if left associative reduce else shift  [ should check associativity is the same]

  iii) special rules.  Possibly add a commit syntax to imply if this matches don't try and reduce on others?

  */
