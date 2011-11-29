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
#ifndef __HQLUTIL_HPP_
#define __HQLUTIL_HPP_

#include "hqlexpr.hpp"

extern HQL_API bool containsAggregate(IHqlExpression * expr);
extern HQL_API bool containsComplexAggregate(IHqlExpression * expr);
extern HQL_API node_operator queryTransformSingleAggregate(IHqlExpression * transform);
extern HQL_API bool containsOnlyLeft(IHqlExpression * expr,bool ignoreSelfOrFilepos = false);
extern HQL_API IHqlExpression * queryPhysicalRootTable(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryTableFilename(IHqlExpression * expr);
extern HQL_API IHqlExpression * findJoinSortOrders(IHqlExpression * condition, IHqlExpression * leftDs, IHqlExpression * rightDs, IHqlExpression * seq, HqlExprArray &leftSorts, HqlExprArray &rightSorts, bool & isLimitedSubstringJoin, HqlExprArray * slidingMatches);
extern HQL_API IHqlExpression * findJoinSortOrders(IHqlExpression * expr, HqlExprArray &leftSorts, HqlExprArray &rightSorts, bool & isLimitedSubstringJoin, HqlExprArray * slidingMatches);
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
extern HQL_API IHqlExpression * queryHint(IHqlExpression * expr, _ATOM name);
extern HQL_API IHqlExpression * queryHintChild(IHqlExpression * expr, _ATOM name, unsigned idx);

extern HQL_API IHqlExpression * replaceChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild);
extern HQL_API IHqlExpression * insertChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild);
extern HQL_API IHqlExpression * swapDatasets(IHqlExpression * parent);
extern HQL_API IHqlExpression * createCompare(node_operator op, IHqlExpression * l, IHqlExpression * r);    // handles cast insertion...
extern HQL_API IHqlExpression * createRecord(IHqlExpression * field);
extern HQL_API IHqlExpression * queryLastField(IHqlExpression * record);
extern HQL_API IHqlExpression * queryNextRecordField(IHqlExpression * record, unsigned & idx);
extern HQL_API int compareSymbolsByName(IInterface * * pleft, IInterface * * pright);
extern HQL_API int compareAtoms(IInterface * * pleft, IInterface * * pright);
extern HQL_API IHqlExpression * getSizetConstant(unsigned size);
extern HQL_API IHqlExpression * createIntConstant(__int64 val);
extern HQL_API IHqlExpression * createUIntConstant(unsigned __int64 val);
extern HQL_API bool isBlankString(IHqlExpression * expr);
extern HQL_API bool isNullString(IHqlExpression * expr);
extern HQL_API IHqlExpression * createIf(IHqlExpression * cond, IHqlExpression * left, IHqlExpression * right);

extern HQL_API void gatherIndexBuildSortOrder(HqlExprArray & sorts, IHqlExpression * expr, bool sortIndexPayload);
extern HQL_API bool recordContainsBlobs(IHqlExpression * record);
extern HQL_API IHqlExpression * queryVirtualFileposField(IHqlExpression * record);

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
extern HQL_API unsigned isEmptyRecord(IHqlExpression * record);
extern HQL_API bool isTrivialSelectN(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryConvertChoosenNSort(IHqlExpression * expr, unsigned __int64 topNlimit);

extern HQL_API IHqlExpression * queryPropertyChild(IHqlExpression * expr, _ATOM name, unsigned idx);
extern HQL_API IHqlExpression * querySequence(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryResultName(IHqlExpression * expr);
extern HQL_API int getResultSequenceValue(IHqlExpression * expr);
extern HQL_API unsigned countTotalFields(IHqlExpression * record, bool includeVirtual);
extern HQL_API bool transformContainsSkip(IHqlExpression * transform);
extern HQL_API bool transformListContainsSkip(IHqlExpression * transforms);
extern HQL_API bool recordContainsNestedRecord(IHqlExpression * record);
extern HQL_API bool hasMaxLength(IHqlExpression * record);

extern HQL_API IHqlExpression * queryInvalidCsvRecordField(IHqlExpression * expr);
extern HQL_API bool isValidCsvRecord(IHqlExpression * expr);
extern HQL_API bool isValidXmlRecord(IHqlExpression * expr);

extern HQL_API bool matchesConstantValue(IHqlExpression * expr, __int64 test);
extern HQL_API bool matchesBoolean(IHqlExpression * expr, bool test);
extern HQL_API bool matchesConstantString(IHqlExpression * expr, const char * text, bool ignoreCase);
extern HQL_API bool getBoolValue(IHqlExpression * expr, bool dft);
extern HQL_API __int64 getIntValue(IHqlExpression * expr, __int64 dft = 0);
extern HQL_API StringBuffer & getStringValue(StringBuffer & out, IHqlExpression * expr, const char * dft = NULL);
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
extern HQL_API IHqlExpression * createTrimExpr(IHqlExpression * value, IHqlExpression * flags);
extern HQL_API bool isLengthPreservingCast(IHqlExpression * expr);

extern HQL_API IHqlExpression * createTransformFromRow(IHqlExpression * expr);
extern HQL_API IHqlExpression * createNullTransform(IHqlExpression * record);

extern HQL_API IHqlExpression * getFailCode(IHqlExpression * failExpr);
extern HQL_API IHqlExpression * getFailMessage(IHqlExpression * failExpr, bool nullIfOmitted);

extern HQL_API _ATOM queryCsvTableEncoding(IHqlExpression * tableExpr);
extern HQL_API _ATOM queryCsvEncoding(IHqlExpression * csvAttr);
extern HQL_API IHqlExpression * combineIfsToMap(IHqlExpression * expr);
extern HQL_API IHqlExpression * appendLocalAttribute(IHqlExpression * expr);
extern HQL_API IHqlExpression * removeLocalAttribute(IHqlExpression * expr);
extern HQL_API IHqlExpression * removeProperty(IHqlExpression * expr, _ATOM attr);
extern HQL_API IHqlExpression * removeOperand(IHqlExpression * expr, IHqlExpression * operand);
extern HQL_API IHqlExpression * removeChildOp(IHqlExpression * expr, node_operator op);
extern HQL_API IHqlExpression * appendOwnedOperand(IHqlExpression * expr, IHqlExpression * ownedOperand);
extern HQL_API IHqlExpression * replaceOwnedProperty(IHqlExpression * expr, IHqlExpression * ownedProeprty);
extern HQL_API IHqlExpression * appendOwnedOperandsF(IHqlExpression * expr, ...);
extern HQL_API IHqlExpression * inheritAttribute(IHqlExpression * expr, IHqlExpression * donor, _ATOM name);
extern HQL_API unsigned numRealChildren(IHqlExpression * expr);

extern HQL_API IHqlExpression * createEvaluateOutputModule(HqlLookupContext & ctx, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, bool expandCallsWhenBound, node_operator outputOp);
extern HQL_API IHqlExpression * createStoredModule(IHqlExpression * scopeExpr);
extern HQL_API IHqlExpression * convertScalarAggregateToDataset(IHqlExpression * expr);

extern HQL_API void getStoredDescription(StringBuffer & text, IHqlExpression * sequence, IHqlExpression * name, bool includeInternalName);
extern HQL_API void reorderAttributesToEnd(HqlExprArray & target, const HqlExprArray & source);
extern HQL_API IHqlExpression * createSetResult(HqlExprArray & args);
extern HQL_API IHqlExpression * convertSetResultToExtract(IHqlExpression * setResult);
extern HQL_API IHqlExpression * removeDatasetWrapper(IHqlExpression * ds);
extern HQL_API void gatherGraphReferences(HqlExprCopyArray & graphs, IHqlExpression * value, bool externalIds);

extern HQL_API bool containsVirtualFields(IHqlExpression * record);
extern HQL_API IHqlExpression * removeVirtualFields(IHqlExpression * record);
extern HQL_API void unwindTransform(HqlExprCopyArray & exprs, IHqlExpression * transform);
extern HQL_API bool isConstantTransform(IHqlExpression * transform);
extern HQL_API bool isConstantDataset(IHqlExpression * expr);
extern HQL_API bool isSimpleTransformToMergeWith(IHqlExpression * expr);
extern HQL_API IHqlExpression * queryUncastExpr(IHqlExpression * expr);
extern HQL_API bool areConstant(const HqlExprArray & args);


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
    void addResultRead(IHqlExpression * seq, IHqlExpression * name, bool isGraphResult);
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
extern HQL_API bool isUngroup(IHqlExpression * expr);
extern HQL_API bool containsExpression(IHqlExpression * expr, IHqlExpression * search);
extern HQL_API bool containsOperator(IHqlExpression * expr, node_operator search);
extern HQL_API bool containsIfBlock(IHqlExpression * record);
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

    ~HqlExprHashTable() { releaseAll(); }

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
        bitsRemaining = 0;
        nextBitOffset = 0;
    }

public:
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

extern HQL_API IHqlExpression * convertTempRowToCreateRow(IErrorReceiver * errors, ECLlocation & location, IHqlExpression * expr);
extern HQL_API IHqlExpression * convertTempTableToInlineTable(IErrorReceiver * errors, ECLlocation & location, IHqlExpression * expr);
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
    unsigned findParameter(_ATOM search);

    void mapLogicalToReal(HqlExprArray & mapped, HqlExprArray & params);
    void mapRealToLogical(HqlExprArray & inputExprs, HqlExprArray & logicalParams, IHqlExpression * libraryId, bool canStream, bool distributed);
    inline unsigned numParameters() const { return realParameters.ordinality(); }
    inline unsigned numStreamedInputs() const { return streamingAllowed ? numDatasets : 0; }

    IHqlExpression * resolveParameter(_ATOM search);
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

extern HQL_API _ATOM getWarningAction(unsigned errorCode, const HqlExprArray & overrides, unsigned first);
class HQL_API WarningProcessor
{
public:
    struct OnWarningState
    {
        IHqlExpression * symbol;
        unsigned firstOnWarning;
        unsigned onWarningMax;
    };

    struct OnWarningStateSymbolBlock
    {
        inline OnWarningStateSymbolBlock(WarningProcessor & _processor, IHqlExpression * _expr) : processor(_processor) { processor.pushSymbol(state, _expr); }
        inline ~OnWarningStateSymbolBlock() { processor.popSymbol(state); }

    private:
        WarningProcessor & processor;
        OnWarningState state;
    };

    struct OnWarningStateBlock
    {
        inline OnWarningStateBlock(WarningProcessor & _processor) : processor(_processor) { processor.saveState(state); }
        inline ~OnWarningStateBlock() { processor.restoreState(state); }

    private:
        WarningProcessor & processor;
        OnWarningState state;
    };

public:
    WarningProcessor();

    void addGlobalOnWarning(unsigned code, _ATOM action);
    void addGlobalOnWarning(IHqlExpression * setMetaExpr);
    void addWarning(IECLError * warning);
    inline void checkForGlobalOnWarning(IHqlExpression * expr)
    {
        if ((expr->getOperator() == no_setmeta) && (expr->queryChild(0)->queryName() == onWarningAtom))
            addGlobalOnWarning(expr);
    }
    void processMetaAnnotation(IHqlExpression * expr);
    void processWarningAnnotation(IHqlExpression * expr);
    void pushSymbol(OnWarningState & saved, IHqlExpression * _symbol);
    void popSymbol(const OnWarningState & saved) { restoreState(saved); }
    void saveState(OnWarningState & saved);
    void setSymbol(IHqlExpression * _symbol);
    void restoreState(const OnWarningState & saved);
    void report(IErrorReceiver & errors);
    void report(IErrorReceiver * errors, IErrorReceiver * warnings, IECLError * warning);   // process warning now.

    inline void appendUnique(IECLErrorArray & list, IECLError * search)
    {
        if (!list.contains(*search))
            list.append(*LINK(search));
    }
    inline unsigned numErrors() const { return allErrors.ordinality(); }
    inline IHqlExpression * queryActiveSymbol() { return activeSymbol; }


protected:
    void combineSandboxWarnings();
    void applyGlobalOnWarning();

protected:
    IHqlExpression * activeSymbol;
    IECLErrorArray possibleWarnings;
    IECLErrorArray warnings;
    IECLErrorArray allErrors;
    HqlExprArray globalOnWarnings;
    HqlExprArray localOnWarnings;
    unsigned firstLocalOnWarning;
};


extern HQL_API IHqlExpression * queryDefaultMaxRecordLengthExpr();
extern HQL_API IHqlExpression * getFixedSizeAttr(unsigned size);
extern HQL_API IHqlExpression * querySerializedFormAttr();
extern HQL_API IHqlExpression * queryAlignedAttr();
extern HQL_API IHqlExpression * queryLinkCountedAttr();
extern HQL_API IHqlExpression * queryUnadornedAttr();
extern HQL_API IHqlExpression * queryMatchxxxPseudoFile();
extern HQL_API IHqlExpression * queryQuotedNullExpr();

extern HQL_API IHqlExpression * getEmbeddedAttr();
extern HQL_API IHqlExpression * getInlineAttr();
extern HQL_API IHqlExpression * getReferenceAttr();

extern HQL_API IHqlExpression * getLinkCountedAttr();
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

extern HQL_API StringBuffer & convertToValidLabel(StringBuffer &out, const char * in, unsigned inlen);
extern HQL_API bool arraysSame(CIArray & left, CIArray & right);
extern HQL_API bool arraysSame(Array & left, Array & right);
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
extern HQL_API void getRecordXmlSchema(StringBuffer & result, IHqlExpression * record, bool useXPath);

extern HQL_API IHqlExpression * querySimplifyInExpr(IHqlExpression * expr);
extern HQL_API IHqlExpression * createSizeof(IHqlExpression * expr);
extern HQL_API bool allParametersHaveDefaults(IHqlExpression * function);
extern HQL_API bool expandMissingDefaultsAsStoreds(HqlExprArray & args, IHqlExpression * function);

extern HQL_API bool createConstantRow(MemoryBuffer & target, IHqlExpression * transform);
extern HQL_API bool createConstantNullRow(MemoryBuffer & target, IHqlExpression * record);
extern HQL_API IHqlExpression * createConstantRowExpr(IHqlExpression * transform);
extern HQL_API IHqlExpression * createConstantNullRowExpr(IHqlExpression * record);
extern HQL_API IHqlExpression * ensureOwned(IHqlExpression * expr);
extern HQL_API bool isSetWithUnknownElementSize(ITypeInfo * type);
extern HQL_API IHqlExpression * replaceParameters(IHqlExpression * body, IHqlExpression * oldParams, IHqlExpression * newParams);

//In hqlgram2.cpp
extern HQL_API IPropertyTree * queryEnsureArchiveModule(IPropertyTree * archive, const char * name, IHqlScope * rScope);
extern HQL_API IPropertyTree * queryArchiveAttribute(IPropertyTree * module, const char * name);
extern HQL_API IPropertyTree * createArchiveAttribute(IPropertyTree * module, const char * name);

extern HQL_API IECLError * annotateExceptionWithLocation(IException * e, IHqlExpression * location);
extern HQL_API IHqlExpression * convertAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx);
extern HQL_API StringBuffer & appendLocation(StringBuffer & s, IHqlExpression * location, const char * suffix = NULL);

#endif
