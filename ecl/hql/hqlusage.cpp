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

#include "jlib.hpp"
#include "hqltrans.ipp"
#include "hqlusage.hpp"

#include "hqlutil.hpp"
#include "hqlpmap.hpp"
#include "hqlthql.hpp"

//#define SEARCH_FILENAME "xxx"
//#define SEARCH_FIELD "field1"

#ifdef SEARCH_FIELD
static void breakOnMatchField(bool isKeyed)
{
    printf("%u\n", isKeyed);
}
#endif

class ExpressionStatsInfo
{
public:
    enum { MaxOperands = 17 };  //An arbitrary upper limit to the distinct number of operands being counted
public:
    ExpressionStatsInfo() { count = 0; _clear(numOperands); countMax = 0; sumMax = 0; }

    void trace()
    {
        DBGLOG("numUnique %u", count);
        for (unsigned i=0; i < MaxOperands; i++)
            DBGLOG("  %u operands: %u", i, numOperands[i]);
        DBGLOG("  %u expressions total %u operands", countMax, sumMax);
    }

    unsigned count;
    unsigned numOperands[MaxOperands];
    unsigned countMax;
    unsigned sumMax;
};

static void calcNumUniqueExpressions(IHqlExpression * expr, ExpressionStatsInfo & info)
{
    if (expr->queryTransformExtra())
        return;
    expr->setTransformExtraUnlinked(expr);

    //use head recursion
    for (;;)
    {
        info.count++;
        unsigned max = expr->numChildren();
        if (max >= ExpressionStatsInfo::MaxOperands)
        {
            info.countMax++;
            info.sumMax += max;
        }
        else
            info.numOperands[max]++;

        if (max == 0)
            return;

        for (unsigned idx=1; idx < max; idx++)
            calcNumUniqueExpressions(expr->queryChild(idx), info);
        expr = expr->queryChild(0);
    }
}

unsigned getNumUniqueExpressions(IHqlExpression * expr)
{
    TransformMutexBlock block;
    ExpressionStatsInfo info;
    calcNumUniqueExpressions(expr, info);
    return info.count;
}


unsigned getNumUniqueExpressions(const HqlExprArray & exprs)
{
    TransformMutexBlock block;
    ExpressionStatsInfo info;
    ForEachItemIn(i, exprs)
        calcNumUniqueExpressions(&exprs.item(i),info);
    return info.count;
}

//------------------------------------------------------------------------------------------------

static HqlTransformerInfo quickExpressionCounterInfo("QuickExpressionCounter");
class HQL_API QuickExpressionCounter : public QuickHqlTransformer
{
public:
    QuickExpressionCounter(IHqlExpression * _search, unsigned _limit)
    : QuickHqlTransformer(quickExpressionCounterInfo, NULL), search(_search), limit(_limit)
    {
        matches = 0;
    }

    void analyse(IHqlExpression * expr)
    {
        if (expr == search)
            matches++;
        if (matches >= limit)
            return;
        QuickHqlTransformer::analyse(expr);
    }

    bool limitReached() const { return matches >= limit; }
    unsigned numMatches() const { return matches; }

protected:
    HqlExprAttr search;
    unsigned matches;
    unsigned limit;
};



extern HQL_API unsigned getNumOccurences(HqlExprArray & exprs, IHqlExpression * search, unsigned limit)
{
    QuickExpressionCounter counter(search, limit);
    ForEachItemIn(i, exprs)
        counter.analyse(&exprs.item(i));
    return counter.numMatches();
}


extern HQL_API void logTreeStats(IHqlExpression * expr)
{
    TransformMutexBlock block;
    ExpressionStatsInfo info;
    calcNumUniqueExpressions(expr,info);
    info.trace();
}

extern HQL_API void logTreeStats(const HqlExprArray & exprs)
{
    TransformMutexBlock block;
    ExpressionStatsInfo info;
    ForEachItemIn(i, exprs)
        calcNumUniqueExpressions(&exprs.item(i),info);
    info.trace();
}

//---------------------------------------------------------------------------

HQL_API bool containsSelector(IHqlExpression * expr, IHqlExpression * selector)
{
    return exprReferencesDataset(expr, selector);
}

//---------------------------------------------------------------------------

static HqlTransformerInfo hqlSelectorAnywhereLocatorInfo("HqlSelectorAnywhereLocator");
HqlSelectorAnywhereLocator::HqlSelectorAnywhereLocator(IHqlExpression * _selector) : NewHqlTransformer(hqlSelectorAnywhereLocatorInfo)
{
    selector.set(_selector);
    foundSelector = false;
}


void HqlSelectorAnywhereLocator::analyseExpr(IHqlExpression * expr)
{
    if (foundSelector || alreadyVisited(expr))
        return;
    NewHqlTransformer::analyseExpr(expr);
}

void HqlSelectorAnywhereLocator::analyseSelector(IHqlExpression * expr)
{
    if (expr == selector)
    {
        foundSelector = true;
        return;
    }
    NewHqlTransformer::analyseSelector(expr);
}

bool HqlSelectorAnywhereLocator::containsSelector(IHqlExpression * expr)
{
    foundSelector = false;
    analyse(expr, 0);
    return foundSelector;
}

HQL_API bool containsSelectorAnywhere(IHqlExpression * expr, IHqlExpression * selector)
{
    HqlSelectorAnywhereLocator locator(selector);
    return locator.containsSelector(expr);
}

//------------------------------------------------------------------------------------------------

static HqlTransformerInfo selectCollectingTransformerInfo("SelectCollectingTransformer");
class SelectCollectingTransformer : public NewHqlTransformer
{
public:
    SelectCollectingTransformer(HqlExprArray & _found)
    : NewHqlTransformer(selectCollectingTransformerInfo), found(_found)
    {
    }

    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (alreadyVisited(expr))
            return;
        if (expr->getOperator() == no_select)
        {
            if (!found.contains(*expr))
                found.append(*LINK(expr));
            return;
        }
        NewHqlTransformer::analyseExpr(expr);
    }

protected:
    HqlExprArray & found;
};


void gatherSelectExprs(HqlExprArray & target, IHqlExpression * expr)
{
    SelectCollectingTransformer collector(target);
    collector.analyse(expr, 0);
}

//------------------------------------------------------------------------------------------------

static HqlTransformerInfo fieldAccessAnalyserInfo("FieldAccessAnalyser");
FieldAccessAnalyser::FieldAccessAnalyser(IHqlExpression * _selector) : NewHqlTransformer(fieldAccessAnalyserInfo), selector(_selector)
{
    unwindFields(fields, selector->queryRecord());
    numAccessed = 0;
    accessed.setown(createThreadSafeBitSet());
}

IHqlExpression * FieldAccessAnalyser::queryLastFieldAccessed() const
{
    if (numAccessed == 0)
        return NULL;
    if (accessedAll())
        return &fields.tos();
    ForEachItemInRev(i, fields)
    {
        if (accessed->test(i))
            return &fields.item(i);
    }
    throwUnexpected();
}

void FieldAccessAnalyser::analyseExpr(IHqlExpression * expr)
{
    if (accessedAll() || alreadyVisited(expr))
        return;
    if (expr == selector)
    {
        setAccessedAll();
        return;
    }
    if (expr->getOperator() == no_select)
    {
        if (expr->queryChild(0) == selector)
        {
            unsigned match = fields.find(*expr->queryChild(1));
            assertex(match != NotFound);
            if (!accessed->test(match))
            {
                accessed->set(match);
                numAccessed++;
            }
            return;
        }
    }
    NewHqlTransformer::analyseExpr(expr);
}


void FieldAccessAnalyser::analyseSelector(IHqlExpression * expr)
{
    if (expr == selector)
    {
        setAccessedAll();
        return;
    }
    if (expr->getOperator() == no_select)
    {
        if (expr->queryChild(0) == selector)
        {
            unsigned match = fields.find(*expr->queryChild(1));
            assertex(match != NotFound);
            if (!accessed->test(match))
            {
                accessed->set(match);
                numAccessed++;
            }
            return;
        }
    }
    NewHqlTransformer::analyseSelector(expr);
}


//------------------------------------------------------------------------------------------------

static void expandSelectText(StringBuffer & s, IHqlExpression * expr)
{
    if (expr->getOperator() != no_field)
    {
        assertex(expr->getOperator() == no_select);
        IHqlExpression * ds = expr->queryChild(0);
        IHqlExpression * field = expr->queryChild(1);
        if (ds != queryActiveTableSelector())
        {
            expandSelectText(s, ds);
            s.append(".");
        }
        s.append(field->queryName());
    }
    else
        s.append(expr->queryName());
}


static IPropertyTree * addSelect(IPropertyTree * xml, IHqlExpression * expr, bool isUsed, bool isKeyed, bool isPayload)
{
    //Sanity check to help catch logic problems elsewhere.  It should only be possible to use KEYED() on key fields!
    //But if it fails we do not want to fail the compile => only check in debug.
    dbgassertex(!(isKeyed && isPayload));
    StringBuffer text;
    expandSelectText(text, expr);
    Owned<IPropertyTree> field = createPTree("field");
    field->setProp("@name", text.str());
    if (isKeyed)
        field->setPropBool("@hasKeyedUse", isKeyed);
    if (!isUsed)
        field->setPropBool("@unused", true);
    if (isPayload)
        field->setPropBool("@payload", true);
    const char * tag = field->queryName();
    xml = ensurePTree(xml, "fields");
    return xml->addPropTree(tag, field.getClear());
}

SourceFieldUsage::SourceFieldUsage(IHqlExpression * _source, bool _includeFieldDetail, bool _includeUnusedFields)
: source(_source), includeFieldDetail(_includeFieldDetail), includeUnusedFields(_includeUnusedFields)
{
    usedAll = false;
    usedFilepos = false;
}

void SourceFieldUsage::noteKeyedSelect(IHqlExpression * select, IHqlExpression * selector)
{
    noteSelect(select, selector, true);
}

void SourceFieldUsage::noteSelect(IHqlExpression * select, IHqlExpression * selector, bool isKeyed)
{
#ifdef SEARCH_FIELD
    if (select->queryChild(1)->queryName() == createAtom(SEARCH_FIELD))
    {
#ifdef SEARCH_FILENAME
        if (matchesConstantString(queryFilename(), SEARCH_FILENAME, true))
#endif
        {
            breakOnMatchField(isKeyed);
        }
    }
#endif

    //MORE: For simple selectors may be more efficient to search before replacing the selector.
    OwnedHqlExpr mapped = replaceSelector(select, selector, queryActiveTableSelector());
    //MORE: May need to use a hash table.

    unsigned match = selects.find(*mapped);
    if (match == NotFound)
    {
        selects.append(*mapped.getClear());
        areKeyed.append(isKeyed);
    }
    else if (isKeyed)
        areKeyed.replace(true, match);
}

IHqlExpression * SourceFieldUsage::queryFilename() const
{
    switch (source->getOperator())
    {
    case no_newkeyindex:
        return source->queryChild(3);
    case no_table:
        return source->queryChild(0);
    }
    throwUnexpected();
    return NULL;
}

const char * SourceFieldUsage::queryFilenameText() const
{
    if (!cachedFilenameEcl.get())
    {
        IHqlExpression * filename = queryFilename();
        assertex(filename);

        StringBuffer nameText;
        getExprECL(filename, nameText);
        cachedFilenameEcl.set(nameText.str(), nameText.length());
    }
    return cachedFilenameEcl.get();
}

IPropertyTree * SourceFieldUsage::createReport(const IPropertyTree * exclude) const
{
    bool sourceIsKey = isKey(source);
    const char * type = sourceIsKey ? "index" : "dataset";
    const char * nameText = queryFilenameText();

    if (exclude)
    {
        StringBuffer xpath;
        xpath.append(type).append("[@name=\"");
        xpath.append(nameText);
        xpath.append("\"]");
        if (exclude->hasProp(xpath))
            return NULL;
    }

    Owned<IPropertyTree> entry = createPTree("datasource");
    entry->setProp("@name", nameText);
    entry->setProp("@type", type);

    bool reportPayload = isKey(source);
    unsigned firstPayload = NotFound;
    IHqlExpression * record = source->queryRecord();
    if (reportPayload)
    {
        unsigned numPayloadFields = 0;
        if (getBoolAttribute(source, filepositionAtom, true))
            numPayloadFields = 1;
        IHqlExpression * payloadAttr = source->queryAttribute(_payload_Atom);
        if (!payloadAttr)
            payloadAttr = record->queryAttribute(_payload_Atom);
        if (payloadAttr)
            numPayloadFields = (unsigned)getIntValue(payloadAttr->queryChild(0));
        if (numPayloadFields)
            firstPayload = firstPayloadField(record, numPayloadFields);
    }
    unsigned numFields = 0;
    unsigned numFieldsUsed = 0;
    unsigned numPayloadCandidates = 0;
    expandSelects(entry, record, queryActiveTableSelector(), usedAll, firstPayload, numFields, numFieldsUsed, numPayloadCandidates);

    entry->setPropInt("@numFields", numFields);
    entry->setPropInt("@numFieldsUsed", numFieldsUsed);
    if (sourceIsKey && numPayloadCandidates)
        entry->setPropInt("@numUnkeyedKeyed", numPayloadCandidates);
    return entry.getClear();
}


void SourceFieldUsage::expandSelects(IPropertyTree * xml, IHqlExpression * record, IHqlExpression * selector, bool allUsed, unsigned firstPayload, unsigned & numFields, unsigned & numFieldsUsed, unsigned & numPayloadCandidates) const
{
    bool reportKeyed = trackKeyed();
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        bool inPayload = (i >= firstPayload);
        unsigned childFirstPayload = inPayload ? 0 : NotFound;
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                unsigned match = selects.find(*selected);
                bool isKeyed = reportKeyed && (match != NotFound) ? areKeyed.item(match) : false;
                bool thisUsed = allUsed || (match != NotFound);
                if (cur->isDatarow())
                {
                    expandSelects(xml, cur->queryRecord(), selected, thisUsed, childFirstPayload, numFields, numFieldsUsed, numPayloadCandidates);
                }
                else
                {
                    numFields++;
                    if (!inPayload && !isKeyed)
                        numPayloadCandidates++;
                    if (thisUsed)
                    {
                        if (includeFieldDetail)
                            addSelect(xml, selected, true, isKeyed, inPayload);
                        numFieldsUsed++;
                    }
                    else
                    {
                        if (includeUnusedFields)
                            addSelect(xml, selected, false, false, inPayload);
                    }
                }
                break;
            }
        case no_record:
            expandSelects(xml, cur, selector, allUsed, childFirstPayload, numFields, numFieldsUsed, numPayloadCandidates);
            break;
        case no_ifblock:
            //MORE: Theoretically if any of the fields within the ifblock are used, then the fields
            //used in the ifblock condition are also used.  Needs to be handled by a preprocessing step.
            expandSelects(xml, cur->queryChild(1), selector, allUsed, childFirstPayload, numFields, numFieldsUsed, numPayloadCandidates);
            break;
        }
    }
}


//------------------------------------------------------------------------------------------------

static HqlTransformerInfo sourceFieldTrackerInfo("SourceFieldTracker");
class SourceFieldTracker : public NewHqlTransformer
{
public:
    SourceFieldTracker(SourceFieldUsage * _fieldUsage, IHqlExpression * _selector, bool isKeyed)
        : NewHqlTransformer(sourceFieldTrackerInfo), fieldUsage(_fieldUsage), selector(_selector), insideKeyed(isKeyed)
    {
    }

    virtual void analyseExpr(IHqlExpression * expr);

protected:
    bool isSelected(IHqlExpression * expr) const;

protected:
    SourceFieldUsage * fieldUsage;
    IHqlExpression * selector;
    bool insideKeyed = false;
    bool suppressKeyed = false;
};

void SourceFieldTracker::analyseExpr(IHqlExpression * expr)
{
    ANewTransformInfo * extra = queryTransformExtra(expr);
    if (alreadyVisited(extra))
    {
        //Use spareByte1 to track whether this expression has been visited inside KEYED()
        if (!insideKeyed || extra->spareByte1)
            return;
        if (!fieldUsage->trackKeyed())
            return;
    }

    if (insideKeyed)
        extra->spareByte1 = true;
    else if (fieldUsage->seenAll())
        return;

    if (expr == selector)
    {
        fieldUsage->noteAll();
        return;
    }

    if (isSelected(expr))
    {
        fieldUsage->noteSelect(expr->queryNormalizedSelector(), selector, insideKeyed);
        return;
    }

    bool savedInsideKeyed = insideKeyed;
    bool savedSupressKeyed = suppressKeyed;
    switch (expr->getOperator())
    {
    case no_filepos:
        if (expr->queryChild(0) == selector)
        {
            fieldUsage->noteFilepos();
            return;
        }
        break;
    case no_assertkeyed:
    case no_assertwild:
        if (!suppressKeyed)
            insideKeyed = true;
        break;
    case no_select:
        //If an expression contains a child query on an index, e.g. exists(ds(field=RIGHT.x)) then do not
        //tag RIGHT.x as keyed.  Representation is no_select(no_select_nth(no_aggregate,...)))
        if (isNewSelector(expr))
            suppressKeyed = true;
        break;
    }

    NewHqlTransformer::analyseExpr(expr);
    insideKeyed = savedInsideKeyed;
    suppressKeyed = savedSupressKeyed;
}


bool SourceFieldTracker::isSelected(IHqlExpression * expr) const
{
    for (;;)
    {
        if (expr->getOperator() != no_select)
            return false;
        IHqlExpression * ds = expr->queryChild(0);
        if (ds->queryNormalizedSelector() == selector)
            return true;
        expr = ds;
    }
}

//------------------------------------------------------------------------------------------------

void gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr, IHqlExpression * selector, bool isKeyed)
{
    unsigned first = getNumChildTables(expr);
    SourceFieldTracker tracker(fieldUsage, selector, isKeyed);
    ForEachChildFrom(i, expr, first)
        tracker.analyse(expr->queryChild(i), 0);
}

void gatherParentFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr)
{
    bool hasDs = false;
    bool hasLeft = false;
    bool hasRight = false;
    switch (getChildDatasetType(expr))
    {
    case childdataset_dataset:
        hasDs = true;
        break;
    case childdataset_datasetleft:
        hasDs = true;
        hasLeft = true;
        break;
    case childdataset_left:
        hasLeft = true;
        break;
    case childdataset_same_left_right:
        hasLeft = true;
        hasRight = true;
        break;
    case childdataset_top_left_right:
        hasDs = true;
        hasLeft = true;
        hasRight = true;
        break;
    case childdataset_leftright: // e.g., no_aggregate
        hasLeft = true;
        break;
    }

    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * selSeq = querySelSeq(expr);
    //MORE: Do all this in a single pass
    if (hasDs)
        gatherFieldUsage(fieldUsage, expr, ds->queryNormalizedSelector(), false);
    if (hasLeft)
    {
        OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
        gatherFieldUsage(fieldUsage, expr, left, false);
    }
    if (hasRight)
    {
        OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
        gatherFieldUsage(fieldUsage, expr, right, false);
    }
}
