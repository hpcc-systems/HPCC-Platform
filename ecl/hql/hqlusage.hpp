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

#ifndef __HQLUSAGE_HPP_
#define __HQLUSAGE_HPP_

class HQL_API HqlSelectorAnywhereLocator : public NewHqlTransformer
{
public:
    HqlSelectorAnywhereLocator(IHqlExpression * _selector);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);

    bool containsSelector(IHqlExpression * expr);

protected:
    bool foundSelector;
    OwnedHqlExpr selector;
};

class HQL_API FieldAccessAnalyser : public NewHqlTransformer
{
public:
    FieldAccessAnalyser(IHqlExpression * selector);

    inline bool accessedAll() const { return numAccessed == fields.ordinality(); }
    IHqlExpression * queryLastFieldAccessed() const;

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);

    inline void setAccessedAll() { numAccessed = fields.ordinality(); }

protected:
    LinkedHqlExpr selector;
    HqlExprCopyArray fields;
    Owned<IBitSet> accessed;
    unsigned numAccessed;
};

class HQL_API SourceFieldUsage : public CInterface
{
public:
    SourceFieldUsage(IHqlExpression * _source);

    void noteSelect(IHqlExpression * select, IHqlExpression * selector);
    inline void noteAll() { usedAll = true; }
    inline void noteFilepos() { usedFilepos = true; }

    IHqlExpression * queryFilename() const;
    inline bool matches(IHqlExpression * search) const { return source == search; }
    inline bool seenAll() const { return usedAll; }

    IPropertyTree * createReport() const;

protected:
    void expandSelects(IPropertyTree * xml, IHqlExpression * record, IHqlExpression * selector, bool allUsed, unsigned & numFields, unsigned & numFieldsUsed) const;

protected:
    LinkedHqlExpr source;
    HqlExprArray selects;
    bool usedAll;
    bool usedFilepos;
};


extern HQL_API unsigned getNumUniqueExpressions(IHqlExpression * expr);
extern HQL_API unsigned getNumUniqueExpressions(const HqlExprArray & exprs);
extern HQL_API unsigned getNumOccurences(HqlExprArray & exprs, IHqlExpression * search, unsigned limit);
extern HQL_API void logTreeStats(IHqlExpression * expr);
extern HQL_API void logTreeStats(const HqlExprArray & exprs);
extern HQL_API void gatherSelectExprs(HqlExprArray & target, IHqlExpression * expr);
extern HQL_API bool containsSelector(IHqlExpression * expr, IHqlExpression * selector);
extern HQL_API bool containsSelectorAnywhere(IHqlExpression * expr, IHqlExpression * selector);         // searches through nested "hidden" definitions
extern HQL_API void gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr, IHqlExpression * selector);
extern HQL_API void gatherParentFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr);

#endif
