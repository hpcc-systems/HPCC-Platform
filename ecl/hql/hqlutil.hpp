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
#ifndef __HQLUTIL_HPP_
#define __HQLUTIL_HPP_

#include "hqlexpr.hpp"
#include "hqlir.hpp"

extern HQL_API bool containsAggregate(IHqlExpression * expr);
extern HQL_API bool containsComplexAggregate(IHqlExpression * expr);
extern HQL_API node_operator queryTransformSingleAggregate(IHqlExpression * transform);
extern HQL_API bool containsOnlyLeft(IHqlExpression * expr,bool ignoreSelfOrFilepos = false);
extern HQL_API IHqlExpression * queryPhysicalRootTable(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryTableFilename(IHqlExpression * expr);

extern HQL_API void splitFuzzyCondition(IHqlExpression * condition, IHqlExpression * atmostCond, SharedHqlExpr & fuzzy, SharedHqlExpr & hard);

extern HQL_API IHqlExpression * createRawIndex(IHqlExpression * index);
extern HQL_API IHqlExpression * createImpureOwn(IHqlExpression * expr);
extern HQL_API IHqlExpression * getNormalizedFilename(IHqlExpression * filename);
extern HQL_API IHqlExpression * replaceChild(IHqlExpression * expr, unsigned childIndex, IHqlExpression * newChild);
extern HQL_API IHqlExpression * getExpandSelectExpr(IHqlExpression * expr);
extern HQL_API bool canBeSlidingJoin(IHqlExpression * expr);
extern HQL_API bool isActiveRow(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryFieldFromSelect(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryFieldFromExpr(IHqlExpression * expr);
extern HQL_API bool isValidFieldReference(IHqlExpression * expr);
extern HQL_API bool isFieldSelectedFromRecord(IHqlExpression * expr);

extern HQL_API void gatherHints(HqlExprCopyArray & target, IHqlExpression * expr);
extern HQL_API IHqlExpression * queryHint(IHqlExpression * expr, IAtom * name);
extern HQL_API IHqlExpression * queryHintChild(IHqlExpression * expr, IAtom * name, unsigned idx);
extern HQL_API void unwindHintAttrs(HqlExprArray & args, IHqlExpression * expr);

extern HQL_API IHqlExpression * replaceChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild);
extern HQL_API IHqlExpression * insertChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild);
extern HQL_API IHqlExpression * swapDatasets(IHqlExpression * parent);
extern HQL_API IHqlExpression * createCompare(node_operator op, IHqlExpression * l, IHqlExpression * r);    // handles cast insertion...
extern HQL_API IHqlExpression * createRecord(IHqlExpression * field);
extern HQL_API IHqlExpression * queryFirstField(IHqlExpression * record);
extern HQL_API IHqlExpression * queryLastField(IHqlExpression * record);
extern HQL_API IHqlExpression * queryLastNonAttribute(IHqlExpression * expr);
extern HQL_API unsigned numNonAttributes(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryNextRecordField(IHqlExpression * recorhqlutid, unsigned & idx);
extern HQL_API void expandRecord(HqlExprArray & selects, IHqlExpression * selector, IHqlExpression * expr);

extern HQL_API int compareSymbolsByName(IInterface * const * pleft, IInterface * const * pright);
extern HQL_API int compareScopesByName(IInterface * const * pleft, IInterface * const * pright);
extern HQL_API int compareAtoms(IInterface * const * pleft, IInterface * const * pright);
extern HQL_API IHqlExpression * getSizetConstant(unsigned size);
extern HQL_API IHqlExpression * createIntConstant(__int64 val);
extern HQL_API IHqlExpression * createUIntConstant(unsigned __int64 val);
extern HQL_API bool isBlankString(IHqlExpression * expr);
extern HQL_API bool isNullString(IHqlExpression * expr);
extern HQL_API IHqlExpression * createIf(IHqlExpression * cond, IHqlExpression * left, IHqlExpression * right);

extern HQL_API void gatherIndexBuildSortOrder(HqlExprArray & sorts, IHqlExpression * expr, bool sortIndexPayload);
extern HQL_API bool recordContainsBlobs(IHqlExpression * record);
inline bool recordIsEmpty(IHqlExpression * record) { return queryLastField(record) == NULL; }
extern HQL_API IHqlExpression * queryVirtualFileposField(IHqlExpression * record);
extern HQL_API IHqlExpression * removeAttributeFromFields(IHqlExpression * expr, IAtom * name);

extern HQL_API IHqlExpression * flattenListOwn(IHqlExpression * list);
extern HQL_API void flattenListOwn(HqlExprArray & out, IHqlExpression * list);
extern HQL_API void releaseList(IHqlExpression * list);

extern HQL_API bool isDistributedSourceActivity(IHqlExpression * expr);
extern HQL_API bool isSourceActivity(IHqlExpression * expr, bool ignoreCompound = false);
extern HQL_API bool isDistributedActivity(IHqlExpression * expr);

extern HQL_API unsigned getFirstActivityArgument(IHqlExpression * expr);
extern HQL_API unsigned getNumActivityArguments(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryChildActivity(IHqlExpression * expr, unsigned index);
extern HQL_API bool isSinkActivity(IHqlExpression * expr);
extern HQL_API bool hasActiveTopDataset(IHqlExpression * expr);

extern HQL_API unsigned getFieldCount(IHqlExpression * expr);
extern HQL_API unsigned getFlatFieldCount(IHqlExpression * expr);
extern HQL_API unsigned getVarSizeFieldCount(IHqlExpression * expr, bool expandRows);
extern HQL_API unsigned isEmptyRecord(IHqlExpression * record);
extern HQL_API unsigned isSimpleRecord(IHqlExpression * record);
extern HQL_API void getSimpleFields(HqlExprArray &out, IHqlExpression *record);

struct HqlRecordStats
{
    unsigned fields = 0;
    unsigned unknownSizeFields = 0;
};
extern HQL_API void gatherRecordStats(HqlRecordStats & stats, IHqlExpression * expr);

extern HQL_API bool isTrivialSelectN(IHqlExpression * expr);

extern HQL_API IHqlExpression * queryConvertChoosenNSort(IHqlExpression * expr, unsigned __int64 topNlimit);

extern HQL_API IHqlExpression * queryAttributeChild(IHqlExpression * expr, IAtom * name, unsigned idx);
extern HQL_API IHqlExpression * querySequence(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryResultName(IHqlExpression * expr);
extern HQL_API int getResultSequenceValue(IHqlExpression * expr);
extern HQL_API unsigned countTotalFields(IHqlExpression * record, bool includeVirtual);
extern HQL_API unsigned getFieldNumber(IHqlExpression * ds, IHqlExpression * selector);
extern HQL_API bool transformContainsSkip(IHqlExpression * transform);
extern HQL_API bool transformListContainsSkip(IHqlExpression * transforms);
extern HQL_API bool recordContainsNestedRow(IHqlExpression * record);
extern HQL_API IHqlExpression * queryStripCasts(IHqlExpression * expr);
extern HQL_API bool remainingChildrenMatch(IHqlExpression * left, IHqlExpression * right, unsigned first);

extern HQL_API IHqlExpression * queryInvalidCsvRecordField(IHqlExpression * expr);
extern HQL_API bool isValidCsvRecord(IHqlExpression * expr);
extern HQL_API bool isValidXmlRecord(IHqlExpression * expr);

extern HQL_API bool matchesConstantValue(IHqlExpression * expr, __int64 test);
extern HQL_API bool matchesBoolean(IHqlExpression * expr, bool test);
extern HQL_API bool matchesConstantString(IHqlExpression * expr, const char * text, bool ignoreCase);
extern HQL_API void getHintNameValue(IHqlExpression * attr, StringBuffer &name, StringBuffer &value);
extern HQL_API bool getBoolValue(IHqlExpression * expr, bool dft);
extern HQL_API __int64 getIntValue(IHqlExpression * expr, __int64 dft = 0);
extern HQL_API StringBuffer & getStringValue(StringBuffer & out, IHqlExpression * expr, const char * dft = NULL);
extern HQL_API StringBuffer & getUTF8Value(StringBuffer & out, IHqlExpression * expr, const char * dft = NULL);
extern HQL_API bool isEmptyList(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryNextMultiLevelDataset(IHqlExpression * expr, bool followActiveSelectors);
extern HQL_API bool isMultiLevelDatasetSelector(IHqlExpression * expr, bool followActiveSelectors);
extern HQL_API bool isCompoundSource(IHqlExpression * expr);
extern HQL_API bool canSetBeAll(IHqlExpression * expr);

extern HQL_API bool workflowContainsSchedule(IHqlExpression * colonExpr);
extern HQL_API bool workflowContainsNonSchedule(IHqlExpression * colonExpr);

extern HQL_API IHqlExpression * getNormalizedCondition(IHqlExpression * expr);
extern HQL_API IHqlExpression * getInverse(IHqlExpression * op);
extern HQL_API IHqlExpression * getNegative(IHqlExpression * value);
extern HQL_API bool areInverseExprs(IHqlExpression * left, IHqlExpression* right);

extern HQL_API IHqlExpression * convertRecordToTransform(IHqlExpression * record, bool canOmit);
extern HQL_API void unwindFilterConditions(HqlExprArray & conds, IHqlExpression * expr);
extern HQL_API unsigned getBestLengthEstimate(IHqlExpression * expr);
extern HQL_API void unwindFields(HqlExprArray & fields, IHqlExpression * record);
extern HQL_API void unwindFields(HqlExprCopyArray & fields, IHqlExpression * record);
extern HQL_API unsigned numAttributes(const HqlExprArray & args);
extern HQL_API unsigned numAttributes(const IHqlExpression * expr);
extern HQL_API IHqlExpression * createGetResultFromSetResult(IHqlExpression * setExpr, ITypeInfo * type=NULL);

extern HQL_API IHqlExpression * convertScalarToGraphResult(IHqlExpression * value, ITypeInfo * fieldType, IHqlExpression * represents, unsigned seq);
extern HQL_API IHqlExpression * createScalarFromGraphResult(ITypeInfo * scalarType, ITypeInfo * fieldType, IHqlExpression * represents, unsigned seq);

extern HQL_API IHqlExpression * createTrimExpr(IHqlExpression * value, IHqlExpression * flags);
extern HQL_API bool isRightTrim(IHqlExpression * expr);
extern HQL_API bool isOpRedundantForCompare(IHqlExpression * expr);

extern HQL_API bool isLengthPreservingCast(IHqlExpression * expr);

/**
 * Report fields that are present in one record but not in another
 *
 * @param newRecord     The record in which to search
 * @param oldRecord     The record providing the fields to search for
 * @param errs          Where to report errors
 * @param location      Location to use when reporting errors
 */
extern HQL_API void reportDroppedFields(IHqlExpression * newRecord, IHqlExpression * oldRecord, IErrorReceiver &err, ECLlocation &location);

extern HQL_API IHqlExpression * createTransformFromRow(IHqlExpression * expr);
extern HQL_API IHqlExpression * createNullTransform(IHqlExpression * record);

/**
 * Create a transform that maps from a row inSelector to a target selfSelector. If replaceMissingWithDefault is true,
 * the source can be a subset of the target, and the default values for the fields are used to initialize the target fields.
 * If false, all target fields must be present in source.
 *
 * @param selfSelector  The target for the transform
 * @param inSelector    The source row for the transform
 * @param replaceMissingWithDefault  If true, use default value for target fields not present in source
 * @param err           Where to report errors
 * @param location      Location to use when reporting errors
 */
extern HQL_API IHqlExpression * createMappingTransform(IHqlExpression * selfSelector, IHqlExpression * inSelector, bool replaceMissingWithDefault, IErrorReceiver &err, ECLlocation &location);

extern HQL_API IHqlExpression * getFailCode(IHqlExpression * failExpr);
extern HQL_API IHqlExpression * getFailMessage(IHqlExpression * failExpr, bool nullIfOmitted);

extern HQL_API IAtom * queryCsvTableEncoding(IHqlExpression * tableExpr);
extern HQL_API IAtom * queryCsvEncoding(IHqlExpression * csvAttr);
extern HQL_API IHqlExpression * combineIfsToMap(IHqlExpression * expr);
extern HQL_API IHqlExpression * appendLocalAttribute(IHqlExpression * expr);
extern HQL_API IHqlExpression * removeLocalAttribute(IHqlExpression * expr);
extern HQL_API IHqlExpression * removeAttribute(IHqlExpression * expr, IAtom * attr);
extern HQL_API IHqlExpression * removeOperand(IHqlExpression * expr, IHqlExpression * operand);
extern HQL_API IHqlExpression * removeChildOp(IHqlExpression * expr, node_operator op);
extern HQL_API IHqlExpression * appendAttribute(IHqlExpression * expr, IAtom * attr);
extern HQL_API IHqlExpression * appendOwnedOperand(IHqlExpression * expr, IHqlExpression * ownedOperand);
extern HQL_API IHqlExpression * replaceOwnedAttribute(IHqlExpression * expr, IHqlExpression * ownedAttribute);
extern HQL_API IHqlExpression * appendOwnedOperandsF(IHqlExpression * expr, ...);
extern HQL_API IHqlExpression * inheritAttribute(IHqlExpression * expr, IHqlExpression * donor, IAtom * name);
extern HQL_API void inheritAttribute(HqlExprArray & attrs, IHqlExpression * donor, IAtom * name);
extern HQL_API bool hasOperand(IHqlExpression * expr, IHqlExpression * child);

extern HQL_API unsigned numRealChildren(IHqlExpression * expr);

extern HQL_API IHqlExpression * createEvaluateOutputModule(HqlLookupContext & ctx, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, bool expandCallsWhenBound, node_operator outputOp, IIdAtom *matchId);
extern HQL_API IHqlExpression * createStoredModule(IHqlExpression * scopeExpr);
extern HQL_API IHqlExpression * convertScalarAggregateToDataset(IHqlExpression * expr);

extern HQL_API void getStoredDescription(StringBuffer & text, IHqlExpression * sequence, IHqlExpression * name, bool includeInternalName);
extern HQL_API void reorderAttributesToEnd(HqlExprArray & target, const HqlExprArray & source);
extern HQL_API IHqlExpression * createSetResult(HqlExprArray & args);
extern HQL_API IHqlExpression * convertSetResultToExtract(IHqlExpression * setResult);
extern HQL_API IHqlExpression * removeDatasetWrapper(IHqlExpression * ds);
extern HQL_API void gatherGraphReferences(HqlExprCopyArray & graphs, IHqlExpression * value, bool externalIds);

/**
 * Get a list of all virtual fields from a record
 *
 * @param virtuals      The receiving array
 * @param record        The record in which to search
 */
extern HQL_API void getVirtualFields(HqlExprArray & virtuals, IHqlExpression * record);
extern HQL_API bool containsVirtualFields(IHqlExpression * record);
extern HQL_API IHqlExpression * removeVirtualFields(IHqlExpression * record);
extern HQL_API void unwindTransform(HqlExprCopyArray & exprs, IHqlExpression * transform);
extern HQL_API bool isConstantTransform(IHqlExpression * transform);
extern HQL_API bool isConstantDataset(IHqlExpression * expr);
extern HQL_API bool isConstantDictionary(IHqlExpression * expr);
extern HQL_API bool isSimpleTransformToMergeWith(IHqlExpression * expr);
extern HQL_API bool isSimpleTransform(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryUncastExpr(IHqlExpression * expr);
extern HQL_API bool areConstant(const HqlExprArray & args);
extern HQL_API bool getFoldedConstantText(StringBuffer& ret, IHqlExpression * expr);

extern HQL_API bool isProjectableCall(IHqlExpression *expr);

extern HQL_API IHqlExpression * createTransformForField(IHqlExpression * field, IHqlExpression * value);
extern HQL_API IHqlExpression * convertScalarToRow(IHqlExpression * value, ITypeInfo * fieldType);
extern HQL_API bool splitResultValue(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * value);

//Is 'expr' really dependent on a parameter - expr->isFullyBound() can give false negatives.
extern HQL_API bool isDependentOnParameter(IHqlExpression * expr);
extern HQL_API bool isTimed(IHqlExpression * expr);

inline bool isInternalEmbedAttr(IAtom *name)
{
    return name == languageAtom || name == projectedAtom || name == streamedAtom || name == _linkCounted_Atom || name == importAtom || name==foldAtom || name==timeAtom || name==prebindAtom;
}


inline void extendConditionOwn(SharedHqlExpr & cond, node_operator op, IHqlExpression * r)
{
    cond.setown(extendConditionOwn(op, cond.getClear(), r));
}

inline void extendAndCondition(SharedHqlExpr & cond, IHqlExpression * r)
{
    cond.setown(extendConditionOwn(no_and, cond.getClear(), LINK(r)));
}

inline void extendOrCondition(SharedHqlExpr & cond, IHqlExpression * r)
{
    cond.setown(extendConditionOwn(no_or, cond.getClear(), LINK(r)));
}

inline IHqlExpression * queryNonAliased(IHqlExpression * expr)
{
    if (expr->getOperator() == no_alias)
        return expr->queryChild(0);
    return expr;
}


inline bool appendUniqueExpr(HqlExprArray & list, IHqlExpression * expr)
{
    if (list.find(*expr) == NotFound)
    {
        list.append(*expr);
        return true;
    }
    expr->Release();
    return false;
}

class HQL_API DedupInfoExtractor
{
public:
    enum DedupKeyCompareKind { DedupKeyIsSuperset, DedupKeyIsSubset, DedupKeyIsSame, DedupKeyIsDifferent };
    enum DedupCompareKind { DedupDoesNothing, DedupDoesAll, DedupIsDifferent };

    DedupInfoExtractor(IHqlExpression * expr);

    DedupCompareKind compareWith(const DedupInfoExtractor & other);
    unsigned getConstantKeep() const    { if (numToKeep->queryValue()) return (unsigned)numToKeep->queryValue()->getIntValue(); return 0; }

protected:
    DedupKeyCompareKind compareKeys(const DedupInfoExtractor & other);

public:
    bool keepLeft;
    bool compareAllRows;
    bool compareAllFields;
    bool isLocal;
    bool keepBest;
    OwnedHqlExpr numToKeep;
    HqlExprArray conds;
    HqlExprArray equalities;
};

extern HQL_API bool dedupMatchesWholeRecord(IHqlExpression * expr);

enum
{
    GatherFileRead          = 0x0001,
    GatherFileWrite         = 0x0002,
    GatherFiles             = (GatherFileRead|GatherFileWrite),

    GatherResultRead        = 0x0010,
    GatherResultWrite       = 0x0020,
    GatherResults           = (GatherResultRead|GatherResultWrite),

    GatherGraphResultRead   = 0x0100,
    GatherGraphResultWrite  = 0x0200,
    GatherGraphResults      = (GatherGraphResultRead|GatherGraphResultWrite),

    GatherAllResultRead     = (GatherResultRead|GatherGraphResultRead),
    GatherAllResultWrite    = (GatherResultWrite|GatherGraphResultWrite),
    GatherAllResults        = (GatherResults|GatherGraphResults),

    GatherAllRead           = (GatherFileRead|GatherResultRead|GatherGraphResultRead),
    GatherAllWrite          = (GatherFileWrite|GatherResultWrite|GatherGraphResultWrite),
    GatherAll               = (GatherAllRead|GatherAllWrite),
};
struct HQL_API DependenciesUsed
{
public:
    DependenciesUsed(bool _normalize) { clear(); normalize = _normalize; }

    bool canSwapOrder(const DependenciesUsed & other) const;
    void clear();
    void extractDependencies(IHqlExpression * expr, unsigned flags);
    bool isDependantOn(const DependenciesUsed & other) const;
    bool isExplicitlyDependantOn(const DependenciesUsed & other) const;
    bool isSubsetOf(const DependenciesUsed & other) const;
    void mergeIn(const DependenciesUsed & other);
    void noteInconsistency(IHqlExpression * expr);
    void removeInternalReads();

protected:
    void addFilenameRead(IHqlExpression * expr);
    void addFilenameWrite(IHqlExpression * expr);
    void addRefDependency(IHqlExpression * expr);
    void addResultRead(IHqlExpression * wuid, IHqlExpression * seq, IHqlExpression * name, bool isGraphResult);
    void addResultWrite(IHqlExpression * seq, IHqlExpression * name, bool isGraphResult);
    IHqlExpression * getNormalizedFilename(IHqlExpression * filename);

public:
    HqlExprArray tablesRead;
    HqlExprArray tablesWritten;
    HqlExprArray resultsRead;
    HqlExprArray resultsWritten;
    bool allRead;
    bool allWritten;
    bool normalize;
    LinkedHqlExpr inconsistent;
};

extern HQL_API void checkDependencyConsistency(IHqlExpression * expr);
extern HQL_API void checkDependencyConsistency(const HqlExprArray & exprs);
extern HQL_API void checkSelectConsistency(IHqlExpression * expr);
extern HQL_API bool isUngroup(IHqlExpression * expr);
extern HQL_API bool containsExpression(IHqlExpression * expr, IHqlExpression * search);
extern HQL_API bool containsOperator(IHqlExpression * expr, node_operator search);
extern HQL_API bool containsIfBlock(IHqlExpression * record);
extern HQL_API bool canCreateRtlTypeInfo(IHqlExpression * record); // Can we generate completely valid rtltypeinfo?
extern HQL_API IHqlExpression * removeAnnotations(IHqlExpression * expr, IHqlExpression * search);

class HQL_API DependencyGatherer
{
public:
    DependencyGatherer(DependenciesUsed & _used, unsigned _flags) : used(_used), flags(_flags) {}
    void gatherDependencies(IHqlExpression * expr);

protected:
    void doGatherDependencies(IHqlExpression * expr);

protected:
    DependenciesUsed & used;
    unsigned flags;
};
extern HQL_API void gatherDependencies(IHqlExpression * expr, DependenciesUsed & used, unsigned flags);
extern HQL_API bool introducesNewDependencies(IHqlExpression * oldexpr, IHqlExpression * newexpr);


class TransformMutexBlock
{
public:
    inline TransformMutexBlock() { lockTransformMutex(); }
    inline ~TransformMutexBlock() { unlockTransformMutex(); }
};

class HQL_API RecordSelectIterator
{
public:
    RecordSelectIterator(IHqlExpression * record, IHqlExpression * selector);

    bool first();
    bool next();
    bool isInsideIfBlock();
    bool isInsideNested()                   { return nestingDepth > 0; }
    bool isValid();
    IHqlExpression * get();
    IHqlExpression * query();

protected:
    void beginRecord(IHqlExpression * record);
    bool doNext();

protected:
    HqlExprAttr selector;
    HqlExprAttr curSelector;
    HqlExprAttr rootRecord;
    HqlExprAttr rootSelector;
    HqlExprArray savedSelector;
    HqlExprCopyArray records;
    UnsignedArray indices;
    unsigned ifblockDepth;
    unsigned nestingDepth;
};

//===========================================================================

class HQL_API SubStringHelper
{
public:
    SubStringHelper(IHqlExpression * expr);
    SubStringHelper(IHqlExpression * src, IHqlExpression * range);

    bool canGenerateInline()    { return special || infiniteString; }
    bool knownStart()           { return fixedStart != UNKNOWN_LENGTH; }
    bool knownEnd()             { return fixedEnd != UNKNOWN_LENGTH; }

    void init(IHqlExpression * _src, IHqlExpression * range);

public:
    IHqlExpression * from;
//  IHqlExpression * expr;
    IHqlExpression * to;
    IHqlExpression * src;
    ITypeInfo * srcType;
    unsigned fixedStart;
    unsigned fixedEnd;
    bool special;
    bool infiniteString;
};


class HQL_API HqlExprHashTable : public SuperHashTable
{
public:
    void add(IHqlExpression * e) { SuperHashTable::add(e); }
    IHqlExpression * find(IHqlExpression * e) const { return (IHqlExpression *)SuperHashTable::find(e); }
    void remove(IHqlExpression * e) { SuperHashTable::removeExact(e); }

    ~HqlExprHashTable() { _releaseAll(); }

private:
    virtual void     onAdd(void *et);
    virtual void     onRemove(void *et);
    virtual unsigned getHashFromElement(const void *et) const;
    virtual unsigned getHashFromFindParam(const void *fp) const;
    virtual const void * getFindParam(const void *et) const;
    virtual bool     matchesFindParam(const void *et, const void *key, unsigned fphash) const;
};


class HQL_API BitfieldPacker
{
public:
    BitfieldPacker()
    {
        reset();
    }

    bool checkSpaceAvailable(unsigned & thisBitOffset, unsigned & thisBits, ITypeInfo * type);      // return true if fitted in current container

    void reset()
    {
        activeType = nullptr;
        bitsRemaining = 0;
        nextBitOffset = 0;
    }

public:
    ITypeInfo * activeType = nullptr;
    unsigned bitsRemaining;
    unsigned nextBitOffset;
};


#define ForEachChildActivity(idx, expr)  unsigned numOfChildren##idx = getNumActivityArguments(expr); \
        for (unsigned idx = 0; idx < numOfChildren##idx; idx++) 

interface IMaxSizeCallback
{
    virtual size32_t getMaxSize(IHqlExpression * record) = 0;
};

interface IDefRecordElement;
extern HQL_API IDefRecordElement * createMetaRecord(IHqlExpression * record, IMaxSizeCallback * callback);

extern HQL_API bool castPreservesValueAndOrder(IHqlExpression * expr);
extern HQL_API void expandRowSelectors(HqlExprArray & target, HqlExprArray const & source);
extern HQL_API IHqlExpression * extractCppBodyAttrs(unsigned len, const char * value);
extern HQL_API unsigned cleanupEmbeddedCpp(unsigned len, char * buffer);
extern HQL_API bool isNullList(IHqlExpression * expr);

extern HQL_API IHqlExpression *notePayloadFields(IHqlExpression *record, unsigned payloadCount);
extern HQL_API IHqlExpression *getDictionaryKeyRecord(IHqlExpression *record);
extern HQL_API IHqlExpression *getDictionarySearchRecord(IHqlExpression *record);
extern HQL_API IHqlExpression * createSelectMapRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression * dict, IHqlExpression *values);
extern HQL_API IHqlExpression * createRowForDictExpr(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *expr, IHqlExpression *dict);
extern HQL_API IHqlExpression * createINDictExpr(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *expr, IHqlExpression *dict);
extern HQL_API IHqlExpression *createINDictRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *row, IHqlExpression *dict);
extern HQL_API IHqlExpression * convertTempRowToCreateRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression * expr);
extern HQL_API IHqlExpression * convertTempTableToInlineTable(IErrorReceiver & errors, ECLlocation & location, IHqlExpression * expr);
extern HQL_API void setPayloadAttribute(HqlExprArray &args);

extern HQL_API bool areTypesComparable(ITypeInfo * leftType, ITypeInfo * rightType);
extern HQL_API bool arraysMatch(const HqlExprArray & left, const HqlExprArray & right);
extern HQL_API IHqlExpression * ensureTransformType(IHqlExpression * transform, node_operator op);

extern HQL_API const char * queryChildNodeTraceText(StringBuffer & s, IHqlExpression * expr);

extern HQL_API void appendArray(HqlExprCopyArray & tgt, const HqlExprCopyArray & src);
extern HQL_API void appendArray(HqlExprCopyArray & tgt, const HqlExprArray & src);
extern HQL_API void replaceArray(HqlExprArray & tgt, const HqlExprArray & src);


class HQL_API LibraryInputMapper
{
public:
    LibraryInputMapper(IHqlExpression * _libraryInterface);

    void expandParameters();
    unsigned findParameter(IIdAtom * search);

    void mapLogicalToReal(HqlExprArray & mapped, HqlExprArray & params);
    void mapRealToLogical(HqlExprArray & inputExprs, HqlExprArray & logicalParams, IHqlExpression * libraryId, bool canStream, bool distributed);
    inline unsigned numParameters() const { return realParameters.ordinality(); }
    inline unsigned numStreamedInputs() const { return streamingAllowed ? numDatasets : 0; }

    IHqlExpression * resolveParameter(IIdAtom * search);
    inline const HqlExprArray & queryRealParameters() const { return realParameters; }

protected:
    void expandParameter(IHqlExpression * expr, unsigned & nextParameter);
    IHqlExpression * mapRealToLogical(const HqlExprArray & inputExprs, IHqlExpression * expr, IHqlExpression * libraryId);
    void mapLogicalToReal(HqlExprArray & mapped, IHqlExpression * expr, IHqlExpression * value);

protected:
    LinkedHqlExpr libraryInterface;
    LinkedHqlExpr scopeExpr;
    unsigned numDatasets;
    HqlExprArray realParameters;
    bool streamingAllowed;
};

/*
 *
 *This class is used for processing the onWarningMappings.
 *
 * Global onWarnings are simple, and are just added to a class instance
 * local onWarnings are
 * - applied in a stack-wise manner, so should be removed when no longer processing inside them
 * - only apply until the next defined symbol - to prevent them having an effect too far down the tree
 *
 * For this reason SymbolBlocks and Blocks are used to ensure those rules are followed
 *
 */
class HQL_API ErrorSeverityMapper : public IndirectErrorReceiver
{
public:
    struct ErrorSeverityMapperState
    {
        IHqlExpression * symbol;
        unsigned firstActiveMapping;
        unsigned maxMappings;
    };

    struct SymbolScope
    {
        inline SymbolScope(ErrorSeverityMapper & _processor, IHqlExpression * _expr) : processor(_processor) { processor.pushSymbol(state, _expr); }
        inline ~SymbolScope() { processor.popSymbol(state); }

    private:
        ErrorSeverityMapper & processor;
        ErrorSeverityMapperState state;
    };

    struct Scope
    {
        inline Scope(ErrorSeverityMapper & _processor) : processor(_processor) { processor.saveState(state); }
        inline ~Scope() { processor.restoreState(state); }

    private:
        ErrorSeverityMapper & processor;
        ErrorSeverityMapperState state;
    };

public:
    ErrorSeverityMapper(IErrorReceiver & errorProcessor);

    virtual IError * mapError(IError * error);
    virtual void exportMappings(IWorkUnit * wu) const;

    bool addCommandLineMapping(const char * mapping);
    bool addMapping(const char * category, const char * value);
    void addOnWarning(unsigned code, IAtom * action);
    void addOnWarning(IHqlExpression * setMetaExpr);
    unsigned processMetaAnnotation(IHqlExpression * expr);
    void pushSymbol(ErrorSeverityMapperState & saved, IHqlExpression * _symbol);
    void popSymbol(const ErrorSeverityMapperState & saved) { restoreState(saved); }
    void restoreLocalOnWarnings(unsigned prevMax);
    void saveState(ErrorSeverityMapperState & saved) const;
    void setSymbol(IHqlExpression * _symbol);
    void restoreState(const ErrorSeverityMapperState & saved);

    inline IHqlExpression * queryActiveSymbol() { return activeSymbol; }

private:
    ErrorSeverityMapper(const ErrorSeverityMapper &); // prevent this being called

protected:
    IHqlExpression * activeSymbol;
    HqlExprArray severityMappings;
    unsigned firstActiveMapping;
    ErrorSeverity categoryAction[CategoryMax];
};

extern HQL_API bool isGlobalOnWarning(IHqlExpression * expr);

extern HQL_API IHqlExpression * queryDefaultMaxRecordLengthExpr();
extern HQL_API IHqlExpression * getFixedSizeAttr(unsigned size);
extern HQL_API IHqlExpression * queryAlignedAttr();
extern HQL_API IHqlExpression * queryLinkCountedAttr();
extern HQL_API IHqlExpression * queryProjectedAttr();
extern HQL_API IHqlExpression * queryUnadornedAttr();
extern HQL_API IHqlExpression * queryNlpParsePseudoTable();
extern HQL_API IHqlExpression * queryXmlParsePseudoTable();
extern HQL_API IHqlExpression * queryQuotedNullExpr();

extern HQL_API IHqlExpression * getEmbeddedAttr();
extern HQL_API IHqlExpression * getInlineAttr();
extern HQL_API IHqlExpression * getReferenceAttr();

extern HQL_API IHqlExpression * getLinkCountedAttr();
extern HQL_API IHqlExpression * getProjectedAttr();
extern HQL_API IHqlExpression * getStreamedAttr();

extern HQL_API IHqlExpression * getGlobalSequenceNumber();
extern HQL_API IHqlExpression * getLocalSequenceNumber();
extern HQL_API IHqlExpression * getStoredSequenceNumber();
extern HQL_API IHqlExpression * getOnceSequenceNumber();

extern HQL_API IHqlExpression * createOmittedValue();
inline IHqlExpression * ensureNormalizedDefaultValue(IHqlExpression * expr) { return expr ? expr : createOmittedValue(); }
inline IHqlExpression * queryDefaultValue(IHqlExpression * expr, unsigned idx) 
{ 
    if (!expr)
        return NULL;
    IHqlExpression * ret = expr->queryChild(idx);
    return (ret && (ret->getOperator() != no_omitted)) ? ret : NULL;
}

extern HQL_API bool hasNonNullRecord(ITypeInfo * type);

//Mangle the names to make it slightly trickier for someone disassembling the system, 
//hardly worth it though... since the jlib aes function call gives it away...
#define encryptEclAttribute  normalizeEclText
#define decryptEclAttribute  isAttributeValid
extern HQL_API void encryptEclAttribute(IStringVal & out, size32_t len, const void * in);
extern void decryptEclAttribute(MemoryBuffer & out, const char * in);

extern HQL_API bool debugFindFirstDifference(IHqlExpression * left, IHqlExpression * right);
extern HQL_API void debugTrackDifference(IHqlExpression * expr);

extern HQL_API StringBuffer & convertToValidLabel(StringBuffer &out, const char * in, unsigned inlen);

extern HQL_API bool arraysSame(HqlExprArray & left, HqlExprArray & right);
extern HQL_API bool arraysSame(HqlExprCopyArray & left, HqlExprCopyArray & right);
extern HQL_API bool isFailAction(IHqlExpression * expr);
extern HQL_API bool isFailureGuard(IHqlExpression * expr);

extern HQL_API bool isKeyedDataset(IHqlExpression * expr);
extern HQL_API bool isSteppedDataset(IHqlExpression * expr);
extern HQL_API void createClearAssigns(HqlExprArray & assigns, IHqlExpression * record, IHqlExpression * targetSelector);
extern HQL_API IHqlExpression * createClearTransform(IHqlExpression * record);

extern HQL_API IHqlExpression * createDefaultAssertMessage(IHqlExpression * expr);

extern HQL_API bool createMangledFunctionName(StringBuffer & name, IHqlExpression * funcdef);
extern HQL_API bool createMangledFunctionName(StringBuffer & mangled, IHqlExpression * funcdef, CompilerType compiler);

extern HQL_API void extractXmlName(StringBuffer & name, StringBuffer * itemName, StringBuffer * valueName, IHqlExpression * field, const char * defaultItemName, bool reading);
extern HQL_API void extractXmlName(SharedHqlExpr & name, OwnedHqlExpr * itemName, OwnedHqlExpr * valueName, IHqlExpression * field, const char * defaultItemName, bool reading);
extern HQL_API void getRecordXmlSchema(StringBuffer & result, IHqlExpression * record, bool useXPath, unsigned keyedCount);

extern HQL_API IHqlExpression * querySimplifyInExpr(IHqlExpression * expr);
extern HQL_API IHqlExpression * createSizeof(IHqlExpression * expr);
extern HQL_API bool allParametersHaveDefaults(IHqlExpression * function);
extern HQL_API bool expandMissingDefaultsAsStoreds(HqlExprArray & args, IHqlExpression * function);

extern HQL_API bool createConstantField(MemoryBuffer & target, IHqlExpression * field, IHqlExpression * value);
extern HQL_API bool createConstantRow(MemoryBuffer & target, IHqlExpression * transform);
extern HQL_API bool createConstantNullRow(MemoryBuffer & target, IHqlExpression * record);
extern HQL_API IHqlExpression * createConstantRowExpr(IHqlExpression * transform);
extern HQL_API IHqlExpression * createConstantNullRowExpr(IHqlExpression * record);
extern HQL_API IHqlExpression * ensureOwned(IHqlExpression * expr);
extern HQL_API bool isSetWithUnknownElementSize(ITypeInfo * type);
extern HQL_API IHqlExpression * replaceParameters(IHqlExpression * body, IHqlExpression * oldParams, IHqlExpression * newParams);

extern HQL_API IHqlExpression * normalizeDatasetAlias(IHqlExpression * expr);
extern HQL_API IHqlExpression * normalizeAnyDatasetAliases(IHqlExpression * expr);

//In hqlgram2.cpp
extern HQL_API IPropertyTree * queryEnsureArchiveModule(IPropertyTree * archive, const char * name, IHqlScope * rScope);
extern HQL_API IPropertyTree * queryArchiveAttribute(IPropertyTree * module, const char * name);
extern HQL_API IPropertyTree * createArchiveAttribute(IPropertyTree * module, const char * name);

extern HQL_API IError * annotateExceptionWithLocation(IException * e, IHqlExpression * location);
extern HQL_API IHqlExpression * expandMacroDefinition(IHqlExpression * expr, HqlLookupContext & ctx, bool reportError);
extern HQL_API IHqlExpression * convertAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx, bool syntaxCheck);
extern HQL_API StringBuffer & appendLocation(StringBuffer & s, IHqlExpression * location, const char * suffix = NULL);
extern HQL_API bool userPreventsSort(IHqlExpression * noSortAttr, node_operator side);
extern HQL_API IHqlExpression * queryTransformAssign(IHqlExpression * transform, IHqlExpression * searchField);
extern HQL_API IHqlExpression * queryTransformAssignValue(IHqlExpression * transform, IHqlExpression * searchField);
extern HQL_API IHqlExpression * convertSetToExpression(bool isAll, size32_t len, const void * ptr, ITypeInfo * setType);

struct FieldTypeInfoStruct;
interface IRtlFieldTypeDeserializer;
class RtlTypeInfo;

/*
 * Check whether an xpath contains any non-scalar elements (and is this unsuitable for use when generating ECL)
 *
 * @param  xpath The xpath to check
 * @return True if non-scalar elements are present.
 */
extern HQL_API bool checkXpathIsNonScalar(const char *xpath);

/*
 * Fill in field type information for specified type, for creation of runtime type information from compiler structures
 *
 * @param out  Filled in with resulting information
 * @param type Compiler type
 */
extern HQL_API void getFieldTypeInfo(FieldTypeInfoStruct &out, ITypeInfo *type);

/*
 * Build a runtime type information structure from a compile-time type descriptor
 *
 * @param  deserializer Deserializer object used to create and own the resulting types
 * @param  type         Compiler type information structure
 * @return              Run-time type information structure, owned by supplied deserializer
 */
extern HQL_API const RtlTypeInfo *buildRtlType(IRtlFieldTypeDeserializer &deserializer, ITypeInfo *type);

extern HQL_API bool isCommonSubstringRange(IHqlExpression * expr);
extern HQL_API bool isFileOutput(IHqlExpression * expr);
extern HQL_API bool isWorkunitOutput(IHqlExpression * expr);

class HQL_API AtmostLimit
{
public:
    AtmostLimit(IHqlExpression * expr = NULL)
    {
        extractAtmostArgs(expr);
    }
    void extractAtmostArgs(IHqlExpression * atmost);

public:
    OwnedHqlExpr required;
    HqlExprArray optional;
    OwnedHqlExpr limit;
};

class HQL_API JoinSortInfo
{
    friend class JoinOrderSpotter;
public:
    JoinSortInfo(IHqlExpression * expr);
    JoinSortInfo(IHqlExpression * condition, IHqlExpression * leftDs, IHqlExpression * rightDs, IHqlExpression * seq, IHqlExpression * atmost);

    void findJoinSortOrders(bool allowSlidingMatch);
    IHqlExpression * getContiguousJoinCondition(unsigned numRhsFields);

    inline bool hasRequiredEqualities() const { return leftReq.ordinality() != 0; }
    inline bool hasOptionalEqualities() const { return leftOpt.ordinality() != 0; }
    inline bool hasHardRightNonEquality() const { return hasRightNonEquality; }
    inline const HqlExprArray & queryLeftReq() { return leftReq; }
    inline const HqlExprArray & queryRightReq() { return rightReq; }
    inline const HqlExprArray & queryLeftOpt() { return leftOpt; }
    inline const HqlExprArray & queryRightOpt() { return rightOpt; }
    inline const HqlExprArray & queryLeftSort() { initSorts(); return leftSorts; }
    inline const HqlExprArray & queryRightSort() { initSorts(); return rightSorts; }
    inline bool isSlidingJoin() const { return (slidingMatches.ordinality() != 0); }
    inline unsigned numRequiredEqualities() const { return leftReq.ordinality(); }

    bool neverMatchSelf() const;

protected:
    void init();
    void initSorts();
    void doFindJoinSortOrders(IHqlExpression * condition, bool allowSlidingMatch);

public:
    AtmostLimit atmost;
    OwnedHqlExpr extraMatch;
    HqlExprArray slidingMatches;
    bool conditionAllEqualities;
protected:
    //The expression is assumed to outlast this class instance => doesn't used linked
    IHqlExpression * cond;
    IHqlExpression * lhs;
    IHqlExpression * rhs;
    IHqlExpression * seq;
    IHqlExpression * atmostAttr;
    LinkedHqlExpr left;
    LinkedHqlExpr right;
    bool hasRightNonEquality;
    HqlExprArray leftReq;
    HqlExprArray leftOpt;
    HqlExprArray leftSorts;
    HqlExprArray rightReq;
    HqlExprArray rightOpt;
    HqlExprArray rightSorts;
};

extern HQL_API bool joinHasRightOnlyHardMatch(IHqlExpression * expr, bool allowSlidingMatch);
extern HQL_API void gatherParseWarnings(IErrorReceiver * errs, IHqlExpression * expr, IErrorArray & warnings);

#endif
