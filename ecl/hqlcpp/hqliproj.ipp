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
#ifndef __HQLIPROJ_IPP_
#define __HQLIPROJ_IPP_

#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqlhtcpp.ipp"
#include "hqltrans.ipp"
#include "hqlutil.hpp"

//#define USE_IPROJECT_HASH

enum ProjectExprKind
{
    NonActivity,
    CreateRecordActivity,               // creates a record, can remove fields from output at will
    CreateRecordLRActivity,             // creates a record, can remove fields from output at will, has left and right input
    CompoundActivity,                   // a compound source, so inserting a project before it is assumed to have no cost
    CompoundableActivity,               // a source that could become a compound activity, so worth adding projects after
    TransformRecordActivity,            // contains a transform, but input must match output
    FixedInputActivity,                 // can't change input to this activity.  E.g., pipe, output
                                        // or input/output record contains ifblocks.
    SourceActivity,                     // No inputs, so no need to do any calculations.
    SimpleActivity,                     // Filter etc - may be worth inserting a project before it.
    PassThroughActivity,                // input always equals output.
    ScalarSelectActivity,               // <someDataset>[n].field
    DenormalizeActivity,                // contains a transform, but left must match output
    ActionSinkActivity,                 // a sink, but that doesn't necessarily use all input fields.
    CreateRecordSourceActivity,         // a source activity containing a transform i.e., inline table
    ComplexNonActivity,                 
    AnyTypeActivity,                    // can be created any type.
};


class UsedFieldSet
{
public:
    UsedFieldSet() { all = false; }

    void addUnique(IHqlExpression * field);
    void append(IHqlExpression & field);
    void clone(const UsedFieldSet & source);
    void cloneFields(const UsedFieldSet & source);
    int compareOrder(IHqlExpression * left, IHqlExpression * right) const;
    bool contains(IHqlExpression & field) const;
    void getFields(HqlExprArray & target) const;
    void getText(StringBuffer & s) const;
    void intersectFields(const UsedFieldSet & source);
    void set(const UsedFieldSet & source);
    void setAll() { all = true; }
    void setAll(IHqlExpression * record);
    void sort(ICompare & compare);

    inline unsigned ordinality() const { return fields.ordinality(); }
    inline bool includeAll() const { return all; }
    inline IHqlExpression & item(unsigned i) const { return fields.item(i); }

protected:
    void kill();

protected:
#ifdef USE_IPROJECT_HASH
    HqlExprHashTable hash;
#endif
    HqlExprArray fields;
    bool all;
};

//Save memory allocation if only a single item in the list.  Could conceiv
class OptimizeSingleExprCopyArray : private HqlExprCopyArray
{
public:
    OptimizeSingleExprCopyArray() { singleValue = NULL; }

    unsigned ordinality() const 
    { 
        return singleValue ? 1 : HqlExprCopyArray::ordinality(); 
    }
    IHqlExpression & item(unsigned i) const 
    {
        if (singleValue && i == 0)
            return *singleValue;
        return HqlExprCopyArray::item(i); 
    }
    void ensure(unsigned max)
    {
        if (max > 1)
            HqlExprCopyArray::ensure(max); 
    }
    unsigned find(IHqlExpression & cur) const 
    { 
        if (singleValue)
            return &cur == singleValue ? 0 : NotFound;
        return HqlExprCopyArray::find(cur); 
    }
    void remove(unsigned i) 
    { 
        if (singleValue && i == 0)
            singleValue = NULL;
        else
            HqlExprCopyArray::remove(i); 
    }
    void append(IHqlExpression & cur) 
    { 
        if (singleValue)
        {
            HqlExprCopyArray::append(*singleValue); 
            HqlExprCopyArray::append(cur); 
            singleValue = NULL;
        }
        else if (HqlExprCopyArray::ordinality() == 0)
            singleValue = &cur;
        else
            HqlExprCopyArray::append(cur); 
    }

protected:
    IHqlExpression * singleValue;
};

typedef OptimizeSingleExprCopyArray SelectUsedArray;

struct ImplicitProjectOptions
{
    unsigned insertProjectCostLevel;
    byte notifyOptimizedProjects;
    bool isRoxie;
    bool optimizeProjectsPreservePersists;
    bool autoPackRecords;
    bool optimizeSpills;
    bool enableCompoundCsvRead;
};

class ImplicitProjectInfo;

class ComplexImplicitProjectInfo;
class ImplicitProjectInfo : public MergingTransformInfo
{
public:
    ImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind);

    void addActiveSelect(IHqlExpression * selectOrDataset);
    void addActiveSelects(const SelectUsedArray & src);
    void removeProductionSelects();
    void removeScopedFields(IHqlExpression * selector);
    void removeRowsFields(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right);

    virtual ComplexImplicitProjectInfo * queryComplexInfo() { return NULL; }

    inline const SelectUsedArray & querySelectsUsed() { return selectsUsed; }
    inline bool checkAlreadyVisited()
    {
        if (visited)
            return true;
        visited = true;
        return false;
    }
    inline bool checkGatheredSelects()
    {
        if (gatheredSelectsUsed) return true;
        gatheredSelectsUsed = true;
        return false;
    }
    inline ProjectExprKind activityKind() const { return visited ? (ProjectExprKind)kind : NonActivity; }

    inline void preventOptimization() 
    { 
        canOptimize = false; 
    }

    inline bool okToOptimize() const { return canOptimize && visited; }

protected:
    SelectUsedArray selectsUsed;                    // all fields used by this item and its children

public:
#ifdef _DEBUG
    ProjectExprKind kind;
#else
    byte kind;
#endif

private:
    bool visited:1;
    bool gatheredSelectsUsed:1;

//These fields really belong in ComplexImplicitProjectInfo, but are included here to reduce the memory consumption, and improve alignment o the derived record
public:
    bool canOptimize:1;
    bool insertProject:1;
    bool alreadyInScope:1;
    bool canReorderOutput:1;
    bool calcedReorderOutput:1;
    bool visitedAllowingActivity:1;

    byte childDatasetType;
};

typedef ICopyArrayOf<ComplexImplicitProjectInfo> ProjectInfoArray;
class ComplexImplicitProjectInfo : public ImplicitProjectInfo, public ICompare
{
public:
    ComplexImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind);
    IMPLEMENT_IINTERFACE

    virtual ComplexImplicitProjectInfo * queryComplexInfo() { return this; }
    virtual int docompare(const void *,const void *) const;             // compare within output record

    void addAllOutputs();
    bool addOutputField(IHqlExpression * field);
    IHqlExpression * createOutputProject(IHqlExpression * ds);
    void ensureOutputNotEmpty();
    void finalizeOutputRecord();
    void inheritRequiredFields(UsedFieldSet * requiredList);
    bool safeToReorderInput();
    bool safeToReorderOutput();
    void setMatchingOutput(ComplexImplicitProjectInfo * other);
    void setReorderOutput(bool ok)              { canReorderOutput = ok; calcedReorderOutput = true; }

    void stopOptimizeCompound(bool cascade);
    void trace();

    inline bool outputChanged() const                   { return newOutputRecord != original->queryRecord() && okToOptimize(); }

    virtual void notifyRequiredFields(ComplexImplicitProjectInfo * whichInput);

    HqlExprArray * queryOutputFields();

    unsigned queryCostFactor(ClusterType clusterType);
    

protected:
    void trace(const char * label, const UsedFieldSet & fields);

public:
    //later: create a derived class - if is activity or has child dataset

    ComplexImplicitProjectInfo * outputInfo;
    HqlExprAttr newOutputRecord;                // Once set it indicates it won't be changed again

    ProjectInfoArray inputs;
    ProjectInfoArray outputs;
    UsedFieldSet outputFields;
    UsedFieldSet leftFieldsRequired;
    UsedFieldSet rightFieldsRequired;
    UsedFieldSet fieldsToBlank;                 // used as an exception list for iterate and rollup and normalize

public:
};

//MORE: Could remove dependency on insideCompound if it was ok to have compound operators scattered through the
//      contents of a compound item.  Probably would cause few problems, and would make life simpler
class ImplicitProjectTransformer : public MergingHqlTransformer
{
    typedef MergingHqlTransformer Parent;

public:
    ImplicitProjectTransformer(HqlCppTranslator & _translator, bool _optimizeSpills);

    IHqlExpression * process(IHqlExpression * expr);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

protected:
    inline ImplicitProjectInfo * queryBodyExtra(IHqlExpression * expr)  { return (ImplicitProjectInfo *)queryTransformExtra(expr->queryBody()); }
    inline ComplexImplicitProjectInfo * queryBodyComplexExtra(IHqlExpression * expr)    
    { 
        ComplexImplicitProjectInfo * extra = queryBodyExtra(expr)->queryComplexInfo(); 
        assertex(extra);
        return extra;
    }

    void calculateFieldsUsed(IHqlExpression * expr);
    void connect(IHqlExpression * source, IHqlExpression * sink);
    void createFilteredAssigns(HqlExprArray & assigns, IHqlExpression * transform, const UsedFieldSet & fields, IHqlExpression * newSelf, const UsedFieldSet * exceptions);
    IHqlExpression * createFilteredTransform(IHqlExpression * transform, const UsedFieldSet & fields, IHqlExpression * record, const UsedFieldSet * exceptions = NULL);
    void finalizeFields();
    void finalizeFields(IHqlExpression * expr);
    void gatherFieldsUsed(IHqlExpression * expr, ImplicitProjectInfo * extra);
    ProjectExprKind getProjectExprKind(IHqlExpression * expr);
    ProjectExprKind getRawProjectExprKind(IHqlExpression * expr);
    void getTransformedChildren(IHqlExpression * expr, HqlExprArray & children);
    void inheritActiveFields(ImplicitProjectInfo * target, IHqlExpression * source);
    void inheritActiveFields(IHqlExpression * expr, ImplicitProjectInfo * extra, unsigned min, unsigned max);
    void insertProjects();
    void insertProjects(IHqlExpression * expr);
    void logChange(const char * message, IHqlExpression * expr, const UsedFieldSet & fields);
    void percolateFields();
    const SelectUsedArray & querySelectsUsedForField(IHqlExpression * transform, IHqlExpression * field);
    void traceActivities();
    IHqlExpression * updateSelectors(IHqlExpression * newExpr, IHqlExpression * oldExpr);
    IHqlExpression * updateChildSelectors(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector, unsigned firstChild);

    void processSelectsUsedForCreateRecord(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);
    void processSelectsUsedForDenormalize(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * leftSelect, IHqlExpression * rightSelect);
    void processSelectsUsedForTransformRecord(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);

protected:
    const SelectUsedArray & querySelectsUsed(IHqlExpression * expr);

protected:
    HqlCppTranslator &  translator;
    HqlExprArray activities;
    ClusterType targetClusterType;
    unsigned activityDepth;
    ImplicitProjectOptions options;
    bool allowActivity;
};



#endif
