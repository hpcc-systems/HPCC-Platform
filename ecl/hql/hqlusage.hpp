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
    SourceFieldUsage(IHqlExpression * _source, bool _includeFieldDetail, bool _includeUnusedFields);

    void noteKeyedSelect(IHqlExpression * select, IHqlExpression * selector);
    void noteSelect(IHqlExpression * select, IHqlExpression * selector, bool isKeyed);
    inline void noteAll() { usedAll = true; }
    inline void noteFilepos() { usedFilepos = true; }

    const char * queryFilenameText() const;
    inline bool matches(IHqlExpression * search) const { return source == search; }
    inline bool isComplete() const { return seenAll() && !trackKeyed(); }
    inline bool seenAll() const { return usedAll; }
    inline bool trackKeyed() const { return (source->getOperator() == no_newkeyindex); }

    IPropertyTree * createReport(const IPropertyTree * exclude) const;

protected:
    void expandSelects(IPropertyTree * xml, IHqlExpression * record, IHqlExpression * selector, bool allUsed, unsigned firstPayload, unsigned & numFields, unsigned & numFieldsUsed, unsigned & numPayloadCandidates) const;
    IHqlExpression * queryFilename() const;

protected:
    LinkedHqlExpr source;
    HqlExprArray selects;
    BoolArray areKeyed;
    bool usedAll;
    bool usedFilepos;
    bool includeFieldDetail;
    bool includeUnusedFields;

private:
    mutable StringAttr cachedFilenameEcl;
};


extern HQL_API unsigned getNumUniqueExpressions(IHqlExpression * expr);
extern HQL_API unsigned getNumUniqueExpressions(const HqlExprArray & exprs);
extern HQL_API unsigned getNumOccurences(HqlExprArray & exprs, IHqlExpression * search, unsigned limit);
extern HQL_API void logTreeStats(IHqlExpression * expr);
extern HQL_API void logTreeStats(const HqlExprArray & exprs);
extern HQL_API void gatherSelectExprs(HqlExprArray & target, IHqlExpression * expr);
extern HQL_API bool containsSelector(IHqlExpression * expr, IHqlExpression * selector);
extern HQL_API bool containsSelectorAnywhere(IHqlExpression * expr, IHqlExpression * selector);         // searches through nested "hidden" definitions
extern HQL_API void gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr, IHqlExpression * selector, bool isKeyed);
extern HQL_API void gatherParentFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr);

#endif
