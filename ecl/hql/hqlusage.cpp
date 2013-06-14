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

#include "jlib.hpp"
#include "hqltrans.ipp"
#include "hqlusage.hpp"

#include "hqlutil.hpp"
#include "hqlpmap.hpp"
#include "hqlthql.hpp"

//#define SEARCH_FILENAME "xxx"
//#define SEARCH_FIELD "field1"

#ifdef SEARCH_FIELD
static void breakOnMatchField()
{
    strlen("");
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
    loop
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
    accessed.setown(createBitSet());
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


static IPropertyTree * addSelect(IPropertyTree * xml, IHqlExpression * expr, bool isUsed)
{
    StringBuffer text;
    expandSelectText(text, expr);
    Owned<IPropertyTree> field = createPTree(isUsed ? "field" : "unused");
    field->setProp("@name", text.str());
    const char * tag = field->queryName();
    return xml->addPropTree(tag, field.getClear());
}

SourceFieldUsage::SourceFieldUsage(IHqlExpression * _source)
: source(_source)
{
    usedAll = false;
    usedFilepos = false;
}

void SourceFieldUsage::noteSelect(IHqlExpression * select, IHqlExpression * selector)
{
#ifdef SEARCH_FIELD
    if (select->queryChild(1)->queryName() == createAtom(SEARCH_FIELD))
    {
        if (matchesConstantString(queryFilename(), SEARCH_FILENAME, true))
        {
            breakOnMatchField();
        }
    }
#endif

    //MORE: For simple selectors may be more efficient to search before replacing the selector.
    OwnedHqlExpr mapped = replaceSelector(select, selector, queryActiveTableSelector());
    //MORE: May need to use a hash table.
    if (!selects.contains(*mapped))
        selects.append(*mapped.getClear());
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

IPropertyTree * SourceFieldUsage::createReport(bool includeFieldDetail, const IPropertyTree * exclude) const
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

    Owned<IPropertyTree> entry = createPTree(type);
    entry->setProp("@name", nameText);

    unsigned numFields = 0;
    unsigned numFieldsUsed = 0;
    expandSelects(entry, source->queryRecord(), queryActiveTableSelector(), usedAll, includeFieldDetail, numFields, numFieldsUsed);
    if (isKey(source))
    {
        IHqlExpression * original = queryAttributeChild(source, _original_Atom, 0);
        if (!original)
            original = source;
        IHqlExpression * lastField = queryLastField(original->queryRecord());
        if (usedFilepos || !lastField->hasAttribute(_implicitFpos_Atom))
        {
            numFields++;
            if (usedFilepos || usedAll)
            {
                if (includeFieldDetail)
                    addSelect(entry, lastField, true);
                numFieldsUsed++;
            }
        }
    }

    entry->setPropInt("@numFields", numFields);
    entry->setPropInt("@numFieldsUsed", numFieldsUsed);
    return entry.getClear();
}


void SourceFieldUsage::expandSelects(IPropertyTree * xml, IHqlExpression * record, IHqlExpression * selector, bool allUsed, bool includeFieldDetail, unsigned & numFields, unsigned & numFieldsUsed) const
{
    bool seenAll = true;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                bool thisUsed = allUsed || selects.contains(*selected);
                if (cur->isDatarow())
                {
                    expandSelects(xml, cur->queryRecord(), selected, thisUsed, includeFieldDetail, numFields, numFieldsUsed);
                }
                else
                {
                    numFields++;
                    if (thisUsed)
                    {
                        if (includeFieldDetail)
                            addSelect(xml, selected, thisUsed);
                        numFieldsUsed++;
                    }
                    else
                    {
                        //could have an option to output unused fields, with code like the following:
                        //addSelect(xml, selected, thisUsed);
                        seenAll = false;
                    }
                }
                break;
            }
        case no_record:
            expandSelects(xml, cur, selector, allUsed, includeFieldDetail, numFields, numFieldsUsed);
            break;
        case no_ifblock:
            //MORE: Theoretically if any of the fields within the ifblock are used, then the fields
            //used in the ifblock condition are also used.  Needs to be handled by a preprocessing step.
            expandSelects(xml, cur->queryChild(1), selector, allUsed, includeFieldDetail, numFields, numFieldsUsed);
            break;
        }
    }
}


//------------------------------------------------------------------------------------------------

static HqlTransformerInfo sourceFieldTrackerInfo("SourceFieldTracker");
class SourceFieldTracker : public NewHqlTransformer
{
public:
    SourceFieldTracker(SourceFieldUsage * _fieldUsage, IHqlExpression * _selector)
        : NewHqlTransformer(sourceFieldTrackerInfo), fieldUsage(_fieldUsage), selector(_selector)
    {
    }

    virtual void analyseExpr(IHqlExpression * expr);

protected:
    bool isSelected(IHqlExpression * expr) const;

protected:
    SourceFieldUsage * fieldUsage;
    IHqlExpression * selector;
};

void SourceFieldTracker::analyseExpr(IHqlExpression * expr)
{
    if (fieldUsage->seenAll() || alreadyVisited(expr))
        return;
    if (expr == selector)
    {
        fieldUsage->noteAll();
        return;
    }

    if (isSelected(expr))
    {
        fieldUsage->noteSelect(expr->queryNormalizedSelector(), selector);
        return;
    }

    switch (expr->getOperator())
    {
    case no_filepos:
        if (expr->queryChild(0) == selector)
        {
            fieldUsage->noteFilepos();
            return;
        }
        break;
    }

    NewHqlTransformer::analyseExpr(expr);
}


bool SourceFieldTracker::isSelected(IHqlExpression * expr) const
{
    loop
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

void gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr, IHqlExpression * selector)
{
    unsigned first = getNumChildTables(expr);
    SourceFieldTracker tracker(fieldUsage, selector);
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
        gatherFieldUsage(fieldUsage, expr, ds->queryNormalizedSelector());
    if (hasLeft)
    {
        OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
        gatherFieldUsage(fieldUsage, expr, left);
    }
    if (hasRight)
    {
        OwnedHqlExpr right = createSelector(no_right, ds, selSeq);
        gatherFieldUsage(fieldUsage, expr, right);
    }
}
