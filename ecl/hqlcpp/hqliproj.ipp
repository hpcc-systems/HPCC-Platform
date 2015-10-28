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
    FixedInputActivity,                 // can't change input to this activity.  E.g., pipe, output
                                        // or input/output record contains ifblocks.
    SourceActivity,                     // No inputs, so no need to do any calculations.
    SimpleActivity,                     // Filter etc - may be worth inserting a project before it.
    PassThroughActivity,                // input always equals output.
    ScalarSelectActivity,               // <someDataset>[n].field
    DenormalizeActivity,                // contains a transform, but left must match output, fields in LEFT must be in output
    RollupTransformActivity,            // contains a transform, input must match output, fields in LEFT must have
                                        // values in input and output
    IterateTransformActivity,           // contains a transform, input must match output, fields in LEFT must have values
                                        //in output (not nesc evaluated in the input)
    SinkActivity,                       // a sink, but that doesn't necessarily use all input fields.
    CreateRecordSourceActivity,         // a source activity containing a transform i.e., inline table
    ComplexNonActivity,                 
    AnyTypeActivity,                    // can be created any type.
};

//---------------------------------------------------------------------------------------------------------------------

//Save memory allocation if only a single item in the list.
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

//---------------------------------------------------------------------------------------------------------------------

class NestedField;

//NB: Once all is set this structure should not be modified.  That allows the un-modified definitions to be shared
//by other expressions.
class UsedFieldSet
{
public:
    UsedFieldSet() { all = false; maxGathered = 0; originalFields = NULL; }

    void addUnique(IHqlExpression * field);
    NestedField * addNested(IHqlExpression * field);
    bool allGathered() const;
    void appendField(IHqlExpression & ownedField);
    void appendNested(IHqlExpression & ownedField, NestedField * ownedNested);
    void clone(const UsedFieldSet & source);
    bool checkAllFieldsUsed();

    int compareOrder(IHqlExpression * left, IHqlExpression * right) const;
    void createDifference(const UsedFieldSet & left, const UsedFieldSet & right);
    IHqlExpression * createFilteredTransform(IHqlExpression * transform, const UsedFieldSet * exceptions) const;
    void calcFinalRecord(bool canPack, bool ignoreIfEmpty);
    NestedField * findNested(IHqlExpression * field) const;
    NestedField * findNestedByName(IHqlExpression * field) const;
    void gatherTransformValuesUsed(HqlExprArray * selfSelects, HqlExprArray * parentSelects, HqlExprArray * values, IHqlExpression * selector, IHqlExpression * transform);
    void getText(StringBuffer & s) const;
    void intersectFields(const UsedFieldSet & source);
    bool isEmpty() const;
    void noteGatheredAll() { maxGathered = (unsigned)-1; }
    void optimizeFieldsToBlank(const UsedFieldSet & allAssigned, IHqlExpression * transform);
    bool requiresFewerFields(const UsedFieldSet & other) const;
    void set(const UsedFieldSet & source);
    void setAll();
    void setRecord(IHqlExpression * record);
    void unionFields(const UsedFieldSet & source);

    inline unsigned numFields() const { return fields.ordinality(); }
    inline void clear() { kill(); }
    inline bool includeAll() const { return all; }
    inline IHqlExpression * queryOriginalRecord() const {
        assertex(originalFields);
        return originalFields->queryFinalRecord();
    }
    inline const UsedFieldSet * queryOriginal() const { return originalFields; }
    inline IHqlExpression * queryFinalRecord() const { return finalRecord; }
    inline void setAllIfAny() { if (originalFields) setAll(); }
    inline void setOriginal(const UsedFieldSet * _originalFields) { originalFields = _originalFields; }

protected:
    bool contains(IHqlExpression & field) const;
    bool contains(IAtom * name) const; // debugging only
    IHqlExpression * findByName(IAtom * name) const;
    IHqlExpression * createFilteredAssign(IHqlExpression * field, IHqlExpression * value, IHqlExpression * newSelf, const UsedFieldSet * exceptions) const;
    void createFilteredAssigns(HqlExprArray & assigns, IHqlExpression * transform, IHqlExpression * newSelf, const UsedFieldSet * exceptions) const;
    IHqlExpression * createRowTransform(IHqlExpression * row, const UsedFieldSet * exceptions) const;
    void kill();
    void gatherExpandSelectsUsed(HqlExprArray * selfSelects, HqlExprArray * parentSelects, IHqlExpression * selector, IHqlExpression * source);
    unsigned getOriginalPosition(IHqlExpression * field) const;

protected:
    OwnedHqlExpr finalRecord;
    const UsedFieldSet * originalFields;
#ifdef USE_IPROJECT_HASH
    HqlExprHashTable hash;
#endif
    HqlExprArray fields;
    CIArrayOf<NestedField> nested;
    unsigned maxGathered;
    bool all;
};

class RecordOrderComparer : public ICompare
{
public:
    RecordOrderComparer(const UsedFieldSet & _fields) : fields(_fields) {}

    virtual int docompare(const void * l,const void * r) const;

protected:
    const UsedFieldSet & fields;
};


class NestedField : public CInterface
{
public:
    NestedField(IHqlExpression * _field, const UsedFieldSet * _original) : field(_field) { used.setOriginal(_original); }

    NestedField * clone()
    {
        //MORE: The following needs testing - for correctness and speed improvements.
        //if (used.includeAll())
        //    return LINK(this);
        NestedField * ret = new NestedField(field, used.queryOriginal());
        ret->used.clone(used);
        return ret;
    }
    void clear() { used.clear(); }

    inline bool isEmpty() const { return used.isEmpty(); }
    inline bool includeAll() const { return used.includeAll(); }

public:
    IHqlExpression * field;
    UsedFieldSet used;
};

struct ImplicitProjectOptions
{
    unsigned insertProjectCostLevel;
    byte notifyOptimizedProjects;
    bool isRoxie;
    bool optimizeProjectsPreservePersists;
    bool autoPackRecords;
    bool optimizeSpills;
    bool enableCompoundCsvRead;
    bool projectNestedTables;
};

class ImplicitProjectInfo;

class ComplexImplicitProjectInfo;
class ImplicitProjectInfo : public NewTransformInfo
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
};

typedef ICopyArrayOf<ComplexImplicitProjectInfo> ProjectInfoArray;
class ComplexImplicitProjectInfo : public ImplicitProjectInfo
{
public:
    ComplexImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind);

    virtual ComplexImplicitProjectInfo * queryComplexInfo() { return this; }

    void addAllOutputs();
    IHqlExpression * createOutputProject(IHqlExpression * ds);
    void finalizeOutputRecord();
    void inheritRequiredFields(const UsedFieldSet & requiredList);
    bool safeToReorderInput();
    bool safeToReorderOutput();
    void setMatchingOutput(ComplexImplicitProjectInfo * other);
    void setReorderOutput(bool ok)              { canReorderOutput = ok; calcedReorderOutput = true; }
    void setOriginalRecord(ComplexImplicitProjectInfo * outputInfo) { outputFields.setOriginal(&outputInfo->outputFields); }

    void stopOptimizeCompound(bool cascade);
    void trace();

    inline bool outputChanged() const
    {
        return (queryOutputRecord() != original->queryRecord()) && okToOptimize();
    }
    inline IHqlExpression * queryOutputRecord() const { return outputFields.queryFinalRecord(); }

    virtual void notifyRequiredFields(ComplexImplicitProjectInfo * whichInput);

    HqlExprArray * queryOutputFields();

    unsigned queryCostFactor(ClusterType clusterType);
    

protected:
    void trace(const char * label, const UsedFieldSet & fields);

public:
    //later: create a derived class - if is activity or has child dataset

    ProjectInfoArray inputs;
    ProjectInfoArray outputs;
    UsedFieldSet outputFields;
    UsedFieldSet leftFieldsRequired;
    UsedFieldSet rightFieldsRequired;
    UsedFieldSet fieldsToBlank;                 // used as an exception list for iterate and rollup and normalize

public:
};

class ImplicitProjectTransformer : public NewHqlTransformer
{
    typedef NewHqlTransformer Parent;

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
    IHqlExpression * createParentTransformed(IHqlExpression * expr);
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

    void processSelect(ComplexImplicitProjectInfo * extra, IHqlExpression * curSelect, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);
    void processSelects(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);
    void processSelects(ComplexImplicitProjectInfo * extra, HqlExprArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);
    void processTransform(ComplexImplicitProjectInfo * extra, IHqlExpression * transform, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect);

protected:
    const SelectUsedArray & querySelectsUsed(IHqlExpression * expr);
    void setOriginal(UsedFieldSet & fields, IHqlExpression * ds) { fields.setOriginal(&queryBodyComplexExtra(ds->queryRecord())->outputFields); }

protected:
    HqlCppTranslator &  translator;
    HqlExprArray activities;
    ClusterType targetClusterType;
    ImplicitProjectOptions options;
    bool allowActivity;
};



#endif
