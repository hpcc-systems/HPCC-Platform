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
#ifndef __HQLPMAP_HPP_
#define __HQLPMAP_HPP_

#include "jlib.hpp"
#include "hqlexpr.hpp"

interface IExpandCallback
{
    virtual void onExpand(IHqlExpression * select, IHqlExpression * newValue) = 0;
    virtual IHqlExpression * onExpandSelector() { return NULL; }
    virtual void onDatasetChanged(IHqlExpression * newValue, IHqlExpression * oldValue) {}
    virtual void onUnknown() = 0;
};

class TableProjectMapper;
class HQL_API NewProjectMapper2 : public CInterface
{
public:
    NewProjectMapper2() { mapping = NULL; matchedAll = true; expandCallback = NULL; }

    //if newDataset is NULL, the mapping stored in the transform is returned unchanged (useful for seeing if filters can move over joins)
    IHqlExpression * expandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * mapperParent, IExpandCallback * _expandCallback = NULL);
    void expandFields(HqlExprArray & target, const HqlExprArray & src, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent, IExpandCallback * _expandCallback = NULL);
    IHqlExpression * collapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * mapperParent, bool * collapsedAll);

    void addTransformMapping(IHqlExpression * tgt, IHqlExpression * src);
    bool empty() { return targets.ordinality() == 0 && children.ordinality() == 0; }
    void initSelf(IHqlExpression * dataset);
    bool isMappingKnown();
    inline IHqlExpression * querySelf() { return self; }
    void setMapping(IHqlExpression * mapping);
    void setUnknownMapping();

private:
    void addMapping(IHqlExpression * field, IHqlExpression * expr);

    bool ensureMapping();

    void setRecord(IHqlExpression * record, IHqlExpression * selector);
    void setRecord(IHqlExpression * record);
    void setTransform(IHqlExpression * transform);
    void setTransformRowAssignment(IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * record);
    void setTransformRowAssignment(IHqlExpression * nestedSelf, IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * record, TableProjectMapper & mapper);
        
    IHqlExpression * doExpandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent);
    IHqlExpression * doCollapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset);
    inline IHqlExpression * cacheCollapseFields(IHqlExpression * expr, IHqlExpression * oldParent, IHqlExpression * newDataset)
    {
        IInterface * extra = expr->queryTransformExtra();
        if (extra)
            return (IHqlExpression *)LINK(extra);

        IHqlExpression * ret = doCollapseFields(expr, oldParent, newDataset);
        expr->setTransformExtra(ret);
        return ret;
    }
    IHqlExpression * collapseChildFields(IHqlExpression * expr, IHqlExpression * newDataset);

    IHqlExpression * recursiveExpandSelect(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent);
    IHqlExpression * doNewExpandSelect(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent);

private:
    HqlExprArray targets;
    HqlExprArray sources;
    UnsignedArray childIndex;
    CIArrayOf<NewProjectMapper2> children;
    OwnedHqlExpr self;
    IHqlExpression * mapping;
    bool insideSelectorReference = false;
    //Following are only set when expanding/contracting
    bool matchedAll;
    IExpandCallback * expandCallback;
};

class HQL_API TableProjectMapper : public NewProjectMapper2
{
public:
    TableProjectMapper(IHqlExpression * ds = NULL);

    bool setDataset(IHqlExpression * ds);
    void setMapping(IHqlExpression * mapping, IHqlExpression * ds);

    IHqlExpression * expandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IExpandCallback * _expandCallback = NULL);
    IHqlExpression * collapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, bool * collapsedAll);

    inline IHqlExpression * expandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * mapperParent, IExpandCallback * _expandCallback = NULL)
    {
        return NewProjectMapper2::expandFields(expr, oldDataset, newDataset, mapperParent, _expandCallback);
    }
    inline IHqlExpression * collapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * mapperParent, bool * collapsedAll)
    {
        return NewProjectMapper2::collapseFields(expr, oldDataset, newDataset, mapperParent, collapsedAll);
    }

    inline IHqlExpression * queryTransformSelector() { return mapParent; }

protected:
    LinkedHqlExpr mapParent;
};


class HQL_API RecordTransformCreator
{
public:
    void createAssignments(HqlExprArray & assigns, IHqlExpression * expr, IHqlExpression * targetSelector, IHqlExpression * sourceSelector);
    IHqlExpression * createMappingTransform(node_operator op, IHqlExpression * targetRecord, IHqlExpression * sourceSelector);

    virtual IHqlExpression * getMissingAssignValue(IHqlExpression * expr)       { throwUnexpected(); }
};

class HQL_API SimpleExpandMonitor : public IExpandCallback
{
public:
    SimpleExpandMonitor() { complex = false; }

    virtual IHqlExpression * onExpandSelector() { complex = true; return NULL; }
    virtual void onDatasetChanged(IHqlExpression * newValue, IHqlExpression * oldValue) {}
    virtual void onExpand(IHqlExpression * select, IHqlExpression * newValue) {}
    virtual void onUnknown() { complex = true; }

    inline bool isComplex()                                     { return complex; }
    inline void setComplex() { complex = true; }

protected:
    bool complex;
};

class HQL_API FullExpandMonitor : public IExpandCallback
{
public:
    FullExpandMonitor(IHqlExpression * _createRow) : createRow(_createRow) { }

    virtual IHqlExpression * onExpandSelector();
    virtual void onDatasetChanged(IHqlExpression * newValue, IHqlExpression * oldValue) {}
    virtual void onExpand(IHqlExpression * select, IHqlExpression * newValue) {}
    virtual void onUnknown() { throwUnexpected(); }

protected:
    LinkedHqlExpr createRow;
};



extern HQL_API IHqlExpression * createRecordMappingTransform(node_operator op, IHqlExpression * targetRecord, IHqlExpression * sourceSelector);
extern HQL_API IHqlExpression * replaceMemorySelectorWithSerializedSelector(IHqlExpression * expr, IHqlExpression * memoryRecord, node_operator side, IHqlExpression * selSeq, IAtom * serializeVariety);

extern HQL_API TableProjectMapper * createProjectMapper(IHqlExpression * expr);
extern HQL_API TableProjectMapper * createProjectMapper(IHqlExpression * mapping, IHqlExpression * parent);

//Replace references to the inscope dataset oldDataset with references to newDataset.
//The tree must be scope tagged before it works correctly though...
extern HQL_API IHqlExpression * replaceSelector(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset);
extern HQL_API void replaceSelectors(HqlExprArray & out, IHqlExpression * expr, unsigned first, IHqlExpression * oldDataset, IHqlExpression * newDataset);
extern HQL_API IHqlExpression * updateChildSelectors(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector, unsigned firstChild);
extern HQL_API IHqlExpression * updateMappedFields(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector, unsigned firstChild);
extern HQL_API void replaceSelectors(HqlExprArray & out, unsigned first, IHqlExpression * oldDataset, IHqlExpression * newDataset);
extern HQL_API IHqlExpression * scopedReplaceSelector(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset);
extern HQL_API IHqlExpression * replaceSelfRefSelector(IHqlExpression * expr, IHqlExpression * newDataset);
extern HQL_API IHqlExpression * updateActiveSelectorFields(IHqlExpression * expr, IHqlExpression * oldRecord, IHqlExpression * newRecord, unsigned firstChild);

extern HQL_API bool isNullProject(IHqlExpression * expr, bool canIgnorePayload, bool canLoseFieldsFromEnd);
extern HQL_API bool isSimpleProject(IHqlExpression * expr);                             // Restriction or rearrangement only
extern HQL_API bool leftRecordIsSubsetOfRight(IHqlExpression * left, IHqlExpression * right);
extern HQL_API bool transformReturnsSide(IHqlExpression * expr, node_operator side, unsigned inputIndex);

extern HQL_API bool sortDistributionMatches(IHqlExpression * dataset, bool isLocal);

extern HQL_API IHqlExpression * getExtractSelect(IHqlExpression * transform, IHqlExpression * field, bool okToSkipRow);
//Find the assignment that corresponds to select (with selector "selector") and return the assigned value
extern HQL_API IHqlExpression * getExtractSelect(IHqlExpression * transform, IHqlExpression * selector, IHqlExpression * select, bool okToSkipRow);
extern HQL_API IHqlExpression * getParentDatasetSelector(IHqlExpression * ds);
//return all selects from the expression that refer to a particular selector
extern HQL_API void gatherSelects(HqlExprCopyArray & selectors, IHqlExpression * expr, IHqlExpression * selector);

#endif
