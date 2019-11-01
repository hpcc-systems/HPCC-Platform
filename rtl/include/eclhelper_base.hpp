/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#ifndef ECLHELPER_BASE_HPP
#define ECLHELPER_BASE_HPP

// Only the base classes for Activity helpers - CHThorArg and derived classes - should be included in this file.

/*
This file contains base class definitions for the different helper classes.  Any common methods are implemented here.

Doesn't include jlib.hpp yet, so different implementation of CInterface (RtlCInterface) in eclrtl.hpp

*/

//---------------------------------------------------------------------------
template <class INTERFACE>
class ECLRTL_API CThorArgOf : public INTERFACE, public RtlCInterface
{
public:
    ICodeContext * ctx;
    virtual void Link() const override { RtlCInterface::Link(); }
    virtual bool Release() const override { return RtlCInterface::Release(); }
    virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer *) override { ctx = _ctx; }

    virtual void serializeCreateContext(MemoryBuffer &) override {}
    virtual void onStart(const byte *, MemoryBuffer *) override {}
    virtual void serializeStartContext(MemoryBuffer &) override {}
};

template <class INTERFACE>
class ECLRTL_API CThorSinkArgOf : public CThorArgOf<INTERFACE>
{
public:
    virtual IOutputMetaData * queryOutputMeta() override { return NULL; }
};

class ECLRTL_API CThorSinkLibraryArg : public CThorArgOf<IHThorLibraryCallArg>
{
public:
    virtual IOutputMetaData * queryOutputMeta() override { return NULL; }
    virtual IOutputMetaData * queryOutputMeta(unsigned whichOutput) override { return NULL; }
};

class ECLRTL_API CThorIndexWriteArg : public CThorSinkArgOf<IHThorIndexWriteArg>
{
public:
    virtual unsigned getFlags() override;
    virtual const char * getDistributeIndexName() override;
    virtual unsigned getExpiryDays() override;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) override;
    virtual unsigned getFormatCrc() override = 0;
    virtual const char * getCluster(unsigned idx) override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual bool getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx) override;
    virtual unsigned getWidth() override;
    virtual ICompare * queryCompare() override;
    virtual const IBloomBuilderInfo * const *queryBloomInfo() const override;
    virtual __uint64 getPartitionFieldMask() const override;
};

class ECLRTL_API CBloomBuilderInfo : public IBloomBuilderInfo
{
    virtual bool getBloomEnabled() const override;
    virtual __uint64 getBloomFields() const override;
    virtual unsigned getBloomLimit() const override;
    virtual double getBloomProbability() const override;
};

class ECLRTL_API CThorFirstNArg : public CThorArgOf<IHThorFirstNArg>
{
    virtual __int64 numToSkip() override;
    virtual bool preserveGrouping() override;
};

class ECLRTL_API CThorChooseSetsArg : public CThorArgOf<IHThorChooseSetsArg>
{
};

class ECLRTL_API CThorChooseSetsExArg : public CThorArgOf<IHThorChooseSetsExArg>
{
};

class ECLRTL_API CThorDiskWriteArg : public CThorSinkArgOf<IHThorDiskWriteArg>
{
    virtual int getSequence() override;
    virtual unsigned getFlags() override;
    virtual unsigned getTempUsageCount() override;
    virtual unsigned getExpiryDays() override;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual const char * getCluster(unsigned idx) override;
};

class ECLRTL_API CThorPipeReadArg : public CThorArgOf<IHThorPipeReadArg>
{
public:
    virtual unsigned getPipeFlags() override;
    virtual ICsvToRowTransformer * queryCsvTransformer() override;
    virtual IXmlToRowTransformer * queryXmlTransformer() override;
    virtual const char * getXmlIteratorPath() override;
};

//A class which only implements default values for the interface
class ECLRTL_API CHThorXmlWriteExtra : public IHThorXmlWriteExtra
{
    virtual const char * getXmlIteratorPath() override;
    virtual const char * getHeader() override;
    virtual const char * getFooter() override;
    virtual unsigned getXmlFlags() override;
};

class ECLRTL_API CThorPipeWriteArg : public CThorSinkArgOf<IHThorPipeWriteArg>
{
public:
    virtual char * getNameFromRow(const void * _self) override;
    virtual bool recreateEachRow() override;
    virtual unsigned getPipeFlags() override;
    virtual IHThorCsvWriteExtra * queryCsvOutput() override;
    virtual IHThorXmlWriteExtra * queryXmlOutput() override;
};

class ECLRTL_API CThorPipeThroughArg : public CThorArgOf<IHThorPipeThroughArg>
{
public:
    virtual char * getNameFromRow(const void * _self) override;
    virtual bool recreateEachRow() override;
    virtual unsigned getPipeFlags() override;
    virtual IHThorCsvWriteExtra * queryCsvOutput() override;
    virtual IHThorXmlWriteExtra * queryXmlOutput() override;
    virtual ICsvToRowTransformer * queryCsvTransformer() override;
    virtual IXmlToRowTransformer * queryXmlTransformer() override;
    virtual const char * getXmlIteratorPath() override;
};


class ECLRTL_API CThorFilterArg : public CThorArgOf<IHThorFilterArg>
{
    virtual bool canMatchAny() override;
    virtual bool isValid(const void * _left) override;
};

class ECLRTL_API CThorFilterGroupArg : public CThorArgOf<IHThorFilterGroupArg>
{
    virtual bool canMatchAny() override;
    virtual bool isValid(unsigned _num, const void * * _rows) override;
};

class ECLRTL_API CThorGroupArg : public CThorArgOf<IHThorGroupArg>
{
};

class ECLRTL_API CThorDegroupArg : public CThorArgOf<IHThorDegroupArg>
{
};

class ECLRTL_API CThorIterateArg : public CThorArgOf<IHThorIterateArg>
{
    virtual bool canFilter() override;
};

typedef CThorIterateArg CThorGroupIterateArg;

class ECLRTL_API CThorProcessArg : public CThorArgOf<IHThorProcessArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorProjectArg : public CThorArgOf<IHThorProjectArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorQuantileArg : public CThorArgOf<IHThorQuantileArg>
{
    virtual unsigned getFlags() override;
    virtual unsigned __int64 getNumDivisions() override;
    virtual double getSkew() override;
    virtual unsigned __int64 getScore(const void * _left) override;
    virtual void getRange(bool & isAll, size32_t & tlen, void * & tgt) override;
};

class ECLRTL_API CThorPrefetchProjectArg : public CThorArgOf<IHThorPrefetchProjectArg>
{
    virtual bool canFilter() override;
    virtual bool canMatchAny() override;
    virtual unsigned getFlags() override;
    virtual unsigned getLookahead() override;
    virtual IThorChildGraph *queryChild() override;
    virtual bool preTransform(rtlRowBuilder & extract, const void * _left, unsigned __int64 _counter) override;
};

class ECLRTL_API CThorFilterProjectArg : public CThorArgOf<IHThorFilterProjectArg>
{
    virtual bool canFilter() override;
    virtual bool canMatchAny() override;
};

class ECLRTL_API CThorCountProjectArg : public CThorArgOf<IHThorCountProjectArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorNormalizeArg : public CThorArgOf<IHThorNormalizeArg>
{
};

class ECLRTL_API CThorSelectNArg : public CThorArgOf<IHThorSelectNArg>
{
};

class ECLRTL_API CThorCombineArg : public CThorArgOf<IHThorCombineArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorCombineGroupArg : public CThorArgOf<IHThorCombineGroupArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorRollupGroupArg : public CThorArgOf<IHThorRollupGroupArg>
{
    virtual bool canFilter() override;
};

class ECLRTL_API CThorRegroupArg : public CThorArgOf<IHThorRegroupArg>
{
};

class ECLRTL_API CThorNullArg : public CThorSinkArgOf<IHThorNullArg>
{
};

class ECLRTL_API CThorActionArg : public CThorSinkArgOf<IHThorActionArg>
{
    virtual void action() override;;
};

typedef CThorActionArg CThorSideEffectArg;

class ECLRTL_API CThorLimitArg : public CThorArgOf<IHThorLimitArg>
{
    virtual void onLimitExceeded() override;           // nothing generated for skip.
    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
};

class ECLRTL_API CThorCatchArg : public CThorArgOf<IHThorCatchArg>
{
    virtual unsigned getFlags() override;
    virtual bool isHandler(IException * e) override;
    virtual void onExceptionCaught() override;
    virtual size32_t transformOnExceptionCaught(ARowBuilder & rowBuilder, IException * e) override;
};

class ECLRTL_API CThorSplitArg : public CThorArgOf<IHThorSplitArg>
{
    virtual unsigned numBranches() override;
    virtual bool isBalanced() override;
};

class ECLRTL_API CThorSpillArg : public CThorArgOf<IHThorSpillArg>
{
    virtual IOutputMetaData * queryDiskRecordSize() override;
    virtual int getSequence() override;
    virtual unsigned getFlags() override;
    virtual unsigned getTempUsageCount() override;
    virtual unsigned getExpiryDays() override;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual const char * getCluster(unsigned idx) override;
};

class ECLRTL_API CThorNormalizeChildArg : public CThorArgOf<IHThorNormalizeChildArg>
{
};

class ECLRTL_API CThorNormalizeLinkedChildArg : public CThorArgOf<IHThorNormalizeLinkedChildArg>
{
};

class ECLRTL_API CThorChildIteratorArg : public CThorArgOf<IHThorChildIteratorArg>
{
};

class ECLRTL_API CThorRawIteratorArg : public CThorArgOf<IHThorRawIteratorArg>
{
};

class ECLRTL_API CThorLinkedRawIteratorArg : public CThorArgOf<IHThorLinkedRawIteratorArg>
{
};

class ECLRTL_API CThorRollupArg : public CThorArgOf<IHThorRollupArg>
{
    virtual unsigned getFlags() override;
    virtual bool matches(const void * _left, const void * _right) override;
};

class ECLRTL_API CThorDedupArg : public CThorArgOf<IHThorDedupArg>
{
    virtual bool matches(const void * _left, const void * _right) override;
    virtual unsigned numToKeep() override;
    virtual ICompare * queryComparePrimary() override;
    virtual unsigned getFlags() override;
    virtual ICompare * queryCompareBest() override;
};

class ECLRTL_API CThorAggregateArg : public CThorArgOf<IHThorAggregateArg>
{
    virtual unsigned getAggregateFlags() override;
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) override = 0;
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) override = 0;
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) override = 0;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorCountAggregateArg : public CThorAggregateArg
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) override;
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorExistsAggregateArg : public CThorAggregateArg
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) override;
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorThroughAggregateArg : public CThorArgOf<IHThorThroughAggregateArg>
{
    virtual unsigned getAggregateFlags() override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorDistributionArg : public CThorSinkArgOf<IHThorDistributionArg>
{
};

class ECLRTL_API CThorGroupAggregateArg : public CThorArgOf<IHThorGroupAggregateArg>
{
    virtual unsigned getAggregateFlags() override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorHashAggregateArg : public CThorArgOf<IHThorHashAggregateArg>
{
    virtual unsigned getAggregateFlags() override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

class ECLRTL_API CThorInlineTableArg : public CThorArgOf<IHThorInlineTableArg>
{
public:
    virtual unsigned getFlags() override;
};

class ECLRTL_API CThorInlineRowArg : public CThorInlineTableArg
{
    virtual __uint64 numRows() override;
};

class ECLRTL_API CThorSampleArg : public CThorArgOf<IHThorSampleArg>
{
};

class ECLRTL_API CThorEnthArg : public CThorArgOf<IHThorEnthArg>
{
};

class ECLRTL_API CThorFunnelArg : public CThorArgOf<IHThorFunnelArg>
{
    virtual bool isOrdered() override;
    virtual bool pullSequentially() override;
};

class ECLRTL_API CThorNonEmptyArg : public CThorArgOf<IHThorNonEmptyArg>
{
};

class ECLRTL_API CThorMergeArg : public CThorArgOf<IHThorMergeArg>
{
    virtual ISortKeySerializer * querySerialize() override;        // only if global
    virtual ICompare * queryCompareKey() override;
    virtual ICompare * queryCompareRowKey() override;        // row is lhs, key is rhs
    virtual bool dedup() override;
};

class ECLRTL_API CThorRemoteResultArg : public CThorSinkArgOf<IHThorRemoteResultArg>
{
    virtual int getSequence() override;
};

class ECLRTL_API CThorApplyArg : public CThorArgOf<IHThorApplyArg>
{
    virtual void start() override;
    virtual void end() override;
};

class ECLRTL_API CThorSortArg : public CThorArgOf<IHThorSortArg>
{
    virtual double getSkew() override;           // 0=default
    virtual bool hasManyRecords() override;
    virtual double getTargetSkew() override;
    virtual ISortKeySerializer * querySerialize() override;
    virtual unsigned __int64 getThreshold() override;
    virtual IOutputMetaData * querySortedRecordSize() override;
    virtual const char * getSortedFilename() override;
    virtual ICompare * queryCompareLeftRight() override;
    virtual ICompare * queryCompareSerializedRow() override;
    virtual unsigned getAlgorithmFlags() override;
    virtual const char * getAlgorithm() override;
};

class ECLRTL_API CThorTopNArg : public CThorArgOf<IHThorTopNArg>
{
    virtual double getSkew() override;           // 0=default
    virtual bool hasManyRecords() override;
    virtual double getTargetSkew() override;
    virtual ISortKeySerializer * querySerialize() override;
    virtual unsigned __int64 getThreshold() override;
    virtual IOutputMetaData * querySortedRecordSize() override;
    virtual const char * getSortedFilename() override;
    virtual ICompare * queryCompareLeftRight() override;
    virtual ICompare * queryCompareSerializedRow() override;
    virtual unsigned getAlgorithmFlags() override;
    virtual const char * getAlgorithm() override;

    virtual bool hasBest() override;
    virtual int compareBest(const void * _left) override;
};

class ECLRTL_API CThorSubSortArg : public CThorArgOf<IHThorSubSortArg>
{
    virtual double getSkew() override;           // 0=default
    virtual bool hasManyRecords() override;
    virtual double getTargetSkew() override;
    virtual ISortKeySerializer * querySerialize() override;
    virtual unsigned __int64 getThreshold() override;
    virtual IOutputMetaData * querySortedRecordSize() override;
    virtual const char * getSortedFilename() override;
    virtual ICompare * queryCompareLeftRight() override;
    virtual ICompare * queryCompareSerializedRow() override;
};

class ECLRTL_API CThorKeyedJoinArg : public CThorArgOf<IHThorKeyedJoinArg>
{
    virtual bool diskAccessRequired() override;
    virtual const char * getFileName() override;
    virtual IOutputMetaData * queryDiskRecordSize() override;
    virtual IOutputMetaData * queryProjectedDiskRecordSize() override;
    virtual unsigned __int64 extractPosition(const void * _right) override;
    
    // For the data going to the indexRead remote activity:
    virtual bool leftCanMatch(const void * inputRow) override;
    virtual bool indexReadMatch(const void * indexRow, const void * inputRow, IBlobProvider * blobs) override;

    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getSkipLimit() override;
    virtual unsigned getMatchAbortLimit() override;
    virtual void onMatchAbortLimitExceeded() override;

    virtual unsigned getFetchFlags() override;
    virtual unsigned getDiskFormatCrc() override;
    virtual unsigned getProjectedFormatCrc() override;
    virtual void getFileEncryptKey(size32_t & keyLen, void * & key) override;

    virtual unsigned getJoinLimit() override;
    virtual unsigned getKeepLimit() override;
    virtual unsigned getJoinFlags() override;
    virtual unsigned getProjectedIndexFormatCrc() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;

    virtual size32_t extractFetchFields(ARowBuilder & rowBuilder, const void * _input) override;
    virtual bool fetchMatch(const void * diskRow, const void * inputRow) override;

    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) override;

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _dummyRight, const void * _origRow, unsigned __int64 keyedFpos, IException * e) override;
//Join:
//Denormalize:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned __int64 keyedFpos, unsigned counter) override;
//Denormalize group:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned _numRows, const void * * _rows) override;
};

typedef CThorKeyedJoinArg CThorKeyedDenormalizeArg;
typedef CThorKeyedJoinArg CThorKeyedDenormalizeGroupArg;

class ECLRTL_API CThorJoinArg : public CThorArgOf<IHThorJoinArg>
{
    virtual bool isLeftAlreadySorted() override;
    virtual bool isRightAlreadySorted() override;
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder) override;
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) override;
    virtual bool match(const void * _left, const void * _right) override;
    virtual ISortKeySerializer * querySerializeLeft() override;
    virtual ISortKeySerializer * querySerializeRight() override;
    virtual unsigned __int64 getThreshold() override;
    virtual double getSkew() override;           // 0=default
    virtual double getTargetSkew() override;           // 0=default
    virtual unsigned getJoinLimit() override;
    virtual unsigned getKeepLimit() override;
    virtual unsigned getJoinFlags() override;
    virtual unsigned getMatchAbortLimit() override;
    virtual void onMatchAbortLimitExceeded() override;
    virtual ICompare * queryCompareLeftRightLower() override;
    virtual ICompare * queryCompareLeftRightUpper() override;
    virtual ICompare * queryPrefixCompare() override;
    virtual ICompare * queryCompareLeftKeyRightRow() override;
    virtual ICompare * queryCompareRightKeyLeftRow() override;

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e, unsigned flags) override;

//Join:
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) override;
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) override;
};

typedef CThorJoinArg CThorDenormalizeArg;
typedef CThorJoinArg CThorDenormalizeGroupArg;

class ECLRTL_API CThorAllJoinArg : public CThorArgOf<IHThorAllJoinArg>
{
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder) override;
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) override;
    virtual bool match(const void * _left, const void * _right) override;
    virtual unsigned getKeepLimit() override;
    virtual unsigned getJoinFlags() override;
    virtual unsigned getMatchAbortLimit() override;
    virtual void onMatchAbortLimitExceeded() override;

//Join:
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) override;
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) override;
};

typedef CThorAllJoinArg CThorAllDenormalizeArg;
typedef CThorAllJoinArg CThorAllDenormalizeGroupArg;

// Used for hash and lookup joins.
class ECLRTL_API CThorHashJoinArg : public CThorArgOf<IHThorHashJoinArg>
{
    virtual bool isLeftAlreadySorted() override;
    virtual bool isRightAlreadySorted() override;
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder) override;
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) override;
    virtual bool match(const void * _left, const void * _right) override;
    virtual ISortKeySerializer * querySerializeLeft() override;
    virtual ISortKeySerializer * querySerializeRight() override;
    virtual unsigned __int64 getThreshold() override;
    virtual double getSkew() override;           // 0=default
    virtual double getTargetSkew() override;           // 0=default
    virtual unsigned getJoinLimit() override;
    virtual unsigned getKeepLimit() override;
    virtual unsigned getJoinFlags() override;
    virtual unsigned getMatchAbortLimit() override;
    virtual void onMatchAbortLimitExceeded() override;
    virtual ICompare * queryCompareLeftRightLower() override;
    virtual ICompare * queryCompareLeftRightUpper() override;
    virtual ICompare * queryCompareLeft() override;        // not needed for lookup
    virtual ICompare * queryCompareRight() override;        // not needed for many lookup
    virtual ICompare * queryPrefixCompare() override;
    virtual ICompare * queryCompareLeftKeyRightRow() override;
    virtual ICompare * queryCompareRightKeyLeftRow() override;

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e, unsigned flags) override;

//Join:
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) override;
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) override;
};


typedef CThorHashJoinArg CThorHashDenormalizeArg;
typedef CThorHashJoinArg CThorHashDenormalizeGroupArg;

class ECLRTL_API CThorKeyedDistributeArg : public CThorArgOf<IHThorKeyedDistributeArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
};

class ECLRTL_API CThorFetchArg : public CThorArgOf<IHThorFetchArg>
{
    virtual unsigned getFetchFlags() override;
    virtual unsigned getDiskFormatCrc() override;
    virtual unsigned getProjectedFormatCrc() override;
    virtual void getFileEncryptKey(size32_t & keyLen, void * & key) override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual size32_t extractJoinFields(ARowBuilder & rowBuilder, const void * _right) override;
    virtual bool extractAllJoinFields() override;
    virtual IOutputMetaData * queryExtractedSize() override;
};

class ECLRTL_API CThorWorkUnitWriteArg : public CThorSinkArgOf<IHThorWorkUnitWriteArg>
{
    virtual int getSequence() override;
    virtual const char * queryName() override;
    virtual unsigned getFlags() override;
    virtual void serializeXml(const byte * self, IXmlWriter & out) override;
    virtual unsigned getMaxSize() override;
};

class ECLRTL_API CThorXmlWorkunitWriteArg : public CThorSinkArgOf<IHThorXmlWorkunitWriteArg>
{
    virtual int getSequence() override;
    virtual const char * queryName() override;
    virtual unsigned getFlags() override;
};

class ECLRTL_API CThorDictionaryWorkUnitWriteArg : public CThorSinkArgOf<IHThorDictionaryWorkUnitWriteArg>
{
    virtual int getSequence() override;
    virtual const char * queryName() override;
    virtual unsigned getFlags() override;
};

class ECLRTL_API CThorDictionaryResultWriteArg : public CThorSinkArgOf<IHThorDictionaryResultWriteArg>
{
    virtual bool usedOutsideGraph() override;
};

class ECLRTL_API CThorHashDistributeArg : public CThorArgOf<IHThorHashDistributeArg>
{
    virtual IHash    * queryHash() override;
    virtual double getSkew() override;           // 0=default
    virtual double getTargetSkew() override;           // 0=default
    virtual ICompare * queryMergeCompare() override;
};

class ECLRTL_API CThorNWayDistributeArg : public CThorArgOf<IHThorNWayDistributeArg>
{
    virtual unsigned getFlags() override;
    virtual bool     include(const byte * left, unsigned targetNode) override;
};

class ECLRTL_API CThorHashDedupArg : public CThorArgOf<IHThorHashDedupArg>
{
    virtual unsigned getFlags() override;
    virtual ICompare * queryCompareBest() override;
    virtual IOutputMetaData * queryKeySize() override;
    virtual size32_t recordToKey(ARowBuilder & rowBuilder, const void * _record) override;
};

class ECLRTL_API CThorHashMinusArg : public CThorArgOf<IHThorHashMinusArg>
{
};

class ECLRTL_API CThorIfArg : public CThorSinkArgOf<IHThorIfArg>
{
};

class ECLRTL_API CThorCaseArg : public CThorSinkArgOf<IHThorCaseArg>
{
};

class ECLRTL_API CThorSequentialArg : public CThorSinkArgOf<IHThorSequentialArg>
{
};

class ECLRTL_API CThorParallelArg : public CThorSinkArgOf<IHThorParallelArg>
{
};

class ECLRTL_API CThorKeyDiffArg : public CThorSinkArgOf<IHThorKeyDiffArg>
{
    virtual unsigned getFlags() override;
    virtual unsigned getExpiryDays() override;
};

class ECLRTL_API CThorKeyPatchArg : public CThorSinkArgOf<IHThorKeyPatchArg>
{
    virtual unsigned getFlags() override;
    virtual unsigned getExpiryDays() override;
};

class ECLRTL_API CThorWorkunitReadArg : public CThorArgOf<IHThorWorkunitReadArg>
{
    virtual int querySequence() override;
    virtual const char * getWUID() override;
    virtual ICsvToRowTransformer * queryCsvTransformer() override;
    virtual IXmlToRowTransformer * queryXmlTransformer() override;
};

class ECLRTL_API CThorLocalResultReadArg : public CThorArgOf<IHThorLocalResultReadArg>
{
};

class ECLRTL_API CThorLocalResultWriteArg : public CThorSinkArgOf<IHThorLocalResultWriteArg>
{
    virtual bool usedOutsideGraph() override;
};

typedef CThorLocalResultWriteArg CThorLocalResultSpillArg;

class ECLRTL_API CThorGraphLoopResultReadArg : public CThorArgOf<IHThorGraphLoopResultReadArg>
{
};

class ECLRTL_API CThorGraphLoopResultWriteArg : public CThorSinkArgOf<IHThorGraphLoopResultWriteArg>
{
};

//-- Csv --

class ECLRTL_API CThorCsvWriteArg : public CThorSinkArgOf<IHThorCsvWriteArg>
{
    virtual int getSequence() override;
    virtual unsigned getFlags() override;
    virtual unsigned getTempUsageCount() override;
    virtual unsigned getExpiryDays() override;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual const char * getCluster(unsigned idx) override;
};

class ECLRTL_API CThorCsvFetchArg: public CThorArgOf<IHThorCsvFetchArg>
{
    virtual unsigned getFetchFlags() override;
    virtual unsigned getDiskFormatCrc() override;
    virtual unsigned getProjectedFormatCrc() override;
    virtual void getFileEncryptKey(size32_t & keyLen, void * & key) override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual size32_t extractJoinFields(ARowBuilder & rowBuilder, const void * _right) override;
    virtual bool extractAllJoinFields() override;
    virtual IOutputMetaData * queryExtractedSize() override;
};

//-- Xml --

class ECLRTL_API CThorXmlParseArg : public CThorArgOf<IHThorXmlParseArg>
{
    virtual bool requiresContents() override;
};

class ECLRTL_API CThorXmlFetchArg : public CThorArgOf<IHThorXmlFetchArg>
{
    virtual unsigned getFetchFlags() override;
    virtual unsigned getDiskFormatCrc() override;
    virtual unsigned getProjectedFormatCrc() override;
    virtual void getFileEncryptKey(size32_t & keyLen, void * & key) override;
    virtual bool requiresContents() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual size32_t extractJoinFields(ARowBuilder & rowBuilder, const void * _right) override;
    virtual bool extractAllJoinFields() override;
    virtual IOutputMetaData * queryExtractedSize() override;
};

//Simple xml generation...
class ECLRTL_API CThorXmlWriteArg : public CThorSinkArgOf<IHThorXmlWriteArg>
{
    virtual const char * getXmlIteratorPath() override;             // supplies the prefix and suffix for a row
    virtual const char * getHeader() override;
    virtual const char * getFooter() override;
    virtual unsigned getXmlFlags() override;

    virtual int getSequence() override;
    virtual unsigned getFlags() override;
    virtual unsigned getTempUsageCount() override;
    virtual unsigned getExpiryDays() override;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual const char * getCluster(unsigned idx) override;
};

//-- SOAP --

class ECLRTL_API CThorSoapActionArg : public CThorSinkArgOf<IHThorSoapActionArg>
{
    virtual void toXML(const byte * self, IXmlWriter & out) override;
    virtual const char * getHeader() override;
    virtual const char * getFooter() override;
    virtual unsigned getFlags() override;
    virtual unsigned numParallelThreads() override;
    virtual unsigned numRecordsPerBatch() override;
    virtual int numRetries() override;
    virtual double getTimeout() override;
    virtual double getTimeLimit() override;
    virtual const char * getSoapAction() override;
    virtual const char * getNamespaceName() override;
    virtual const char * getNamespaceVar() override;
    virtual const char * getHttpHeaderName() override;
    virtual const char * getHttpHeaderValue() override;
    virtual const char * getHttpHeaders() override;
    virtual const char * getProxyAddress() override;
    virtual const char * getAcceptType() override;
    virtual IXmlToRowTransformer * queryInputTransformer() override;
    virtual const char * getInputIteratorPath() override;
    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) override;
    virtual void getLogText(size32_t & lenText, char * & text, const void * left) override;
    virtual const char * getXpathHintsXml() override;
    virtual const char * getRequestHeader() override;
    virtual const char * getRequestFooter() override;
};

class ECLRTL_API CThorSoapCallArg : public CThorArgOf<IHThorSoapCallArg>
{
//writing to the soap service.
    virtual void toXML(const byte * self, IXmlWriter & out) override;
    virtual const char * getHeader() override;
    virtual const char * getFooter() override;
    virtual unsigned getFlags() override;
    virtual unsigned numParallelThreads() override;
    virtual unsigned numRecordsPerBatch() override;
    virtual int numRetries() override;
    virtual double getTimeout() override;
    virtual double getTimeLimit() override;
    virtual const char * getSoapAction() override;
    virtual const char * getNamespaceName() override;
    virtual const char * getNamespaceVar() override;
    virtual const char * getHttpHeaderName() override;
    virtual const char * getHttpHeaderValue() override;
    virtual const char * getHttpHeaders() override;
    virtual const char * getProxyAddress() override;
    virtual const char * getAcceptType() override;
    virtual IXmlToRowTransformer * queryInputTransformer() override;
    virtual const char * getInputIteratorPath() override;
    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) override;
    virtual void getLogText(size32_t & lenText, char * & text, const void * left) override;
    virtual const char * getXpathHintsXml() override;
    virtual const char * getRequestHeader() override;
    virtual const char * getRequestFooter() override;
};

typedef CThorSoapCallArg CThorHttpCallArg;

typedef CThorNullArg CThorDatasetResultArg;
typedef CThorNullArg CThorRowResultArg;
typedef CThorNullArg CThorPullArg;

class ECLRTL_API CThorParseArg : public CThorArgOf<IHThorParseArg>
{
    virtual INlpHelper * queryHelper() override;
    virtual unsigned getFlags() override;
    virtual IOutputMetaData * queryProductionMeta(unsigned id) override;
    virtual size32_t executeProduction(ARowBuilder & rowBuilder, unsigned id, IProductionCallback * input) override;
};

class ECLRTL_API CThorIndexReadArg : public CThorArgOf<IHThorIndexReadArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual void setCallback(IThorIndexCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;

    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;

    virtual bool needTransform() override;
    virtual bool transformMayFilter() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;
    virtual ISteppingMeta * queryRawSteppingMeta() override;
    virtual ISteppingMeta * queryProjectedSteppingMeta() override;
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) override;
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) override;

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) override;

public:
    IThorIndexCallback * fpp;
};

class ECLRTL_API CThorSteppedIndexReadArg : public CThorIndexReadArg, implements IHThorSteppedSourceExtra
{
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;
    virtual unsigned getSteppedFlags() override;
    virtual double getPriority() override;
    virtual unsigned getPrefetchSize() override;
};

class ECLRTL_API CThorIndexNormalizeArg : public CThorArgOf<IHThorIndexNormalizeArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual void setCallback(IThorIndexCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;

    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) override;

public:
    IThorIndexCallback * fpp;
};

class ECLRTL_API CThorIndexAggregateArg : public CThorArgOf<IHThorIndexAggregateArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual void setCallback(IThorIndexCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
    virtual void processRows(ARowBuilder & rowBuilder, size32_t srcLen, const void * src) override;

public:
    IThorIndexCallback * fpp;
};

class ECLRTL_API CThorIndexCountArg : public CThorArgOf<IHThorIndexCountArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual void setCallback(IThorIndexCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;

    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;

    virtual bool hasFilter() override;
    virtual size32_t numValid(const void * src) override;
    virtual size32_t numValid(size32_t srcLen, const void * _src) override;
    virtual unsigned __int64 getChooseNLimit() override;

public:
    IThorIndexCallback * fpp;
};

class ECLRTL_API CThorIndexGroupAggregateArg : public CThorArgOf<IHThorIndexGroupAggregateArg>
{
    virtual unsigned getFlags() override;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) override;
    virtual void setCallback(IThorIndexCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual IHThorSteppedSourceExtra *querySteppingExtra() override;
    virtual bool createGroupSegmentMonitors(IIndexReadContext *ctx) override;
    virtual unsigned getGroupingMaxField() override;
    virtual size32_t initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
    virtual void processRows(size32_t srcLen, const void * src, IHThorGroupAggregateCallback * callback) override;

public:
    IThorIndexCallback * fpp;
};


class ECLRTL_API CThorDiskReadArg : public CThorArgOf<IHThorDiskReadArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;

    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;

    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;

    virtual bool needTransform() override;
    virtual bool transformMayFilter() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;
    virtual ISteppingMeta * queryRawSteppingMeta() override;
    virtual ISteppingMeta * queryProjectedSteppingMeta() override;
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) override;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) override;

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) override;

public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorDiskNormalizeArg : public CThorArgOf<IHThorDiskNormalizeArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;

    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;

    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) override;

public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorDiskAggregateArg : public CThorArgOf<IHThorDiskAggregateArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;

public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorDiskCountArg : public CThorArgOf<IHThorDiskCountArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;

    virtual bool hasFilter() override;
    virtual size32_t numValid(const void * src) override;
    virtual unsigned __int64 getChooseNLimit() override;

public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorDiskGroupAggregateArg : public CThorArgOf<IHThorDiskGroupAggregateArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
    virtual bool createGroupSegmentMonitors(IIndexReadContext *ctx) override;
    virtual unsigned getGroupingMaxField() override;
    virtual size32_t initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) override;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;

public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorCsvReadArg: public CThorArgOf<IHThorCsvReadArg>
{
    virtual unsigned getFlags() override;
    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned getDiskFormatCrc() override;   // no meaning
    virtual unsigned getProjectedFormatCrc() override;   // no meaning
    virtual void setCallback(IThorDiskCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorXmlReadArg: public CThorArgOf<IHThorXmlReadArg>
{
    virtual unsigned getFlags() override;
    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned getDiskFormatCrc() override;   // no meaning
    virtual unsigned getProjectedFormatCrc() override;   // no meaning
    virtual void setCallback(IThorDiskCallback * _tc) override;
    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;
public:
    IThorDiskCallback * fpp;
};

class ECLRTL_API CThorNewDiskReadArg : public CThorArgOf<IHThorNewDiskReadArg>
{
    virtual unsigned getFlags() override;
    virtual void setCallback(IThorDiskCallback * _tc) override;

    virtual bool canMatchAny() override;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) override;
    virtual bool canMatch(const void * row) override;
    virtual bool hasMatchFilter() override;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) override;

    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;

    virtual bool needTransform() override;
    virtual bool transformMayFilter() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;
    virtual ISteppingMeta * queryRawSteppingMeta() override;
    virtual ISteppingMeta * queryProjectedSteppingMeta() override;
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) override;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) override;
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) override;

    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) override;
    virtual const char * queryFormat() override;
    virtual void getFormatOptions(IXmlWriter & options) override;
    virtual void getFormatDynOptions(IXmlWriter & options) override;

public:
    IThorDiskCallback * fpp;
};


//Normalize
class ECLRTL_API CThorChildNormalizeArg : public CThorArgOf<IHThorChildNormalizeArg>
{
};

//Aggregate
class ECLRTL_API CThorChildAggregateArg : public CThorArgOf<IHThorChildAggregateArg>
{
};

//NormalizedAggregate
//NB: The child may actually be a grandchild/great-grand child, so need to store some sort of current state in the hash table
class ECLRTL_API CThorChildGroupAggregateArg : public CThorArgOf<IHThorChildGroupAggregateArg>
{
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) override;
};

//Normalize
class ECLRTL_API CThorChildThroughNormalizeArg : public CThorArgOf<IHThorChildThroughNormalizeArg>
{
    virtual unsigned __int64 getChooseNLimit() override;
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
    virtual unsigned __int64 getKeyedLimit() override;
    virtual void onKeyedLimitExceeded() override;
};

class ECLRTL_API CThorLoopArg : public CThorArgOf<IHThorLoopArg>
{
    virtual unsigned getFlags() override;
    virtual bool sendToLoop(unsigned counter, const void * in) override;
    virtual unsigned numIterations() override;
    virtual bool loopAgain(unsigned counter, unsigned num, const void * * _rows) override;
    virtual unsigned defaultParallelIterations() override;
    virtual bool loopFirstTime() override;
    virtual unsigned loopAgainResult() override;
};

class ECLRTL_API CThorGraphLoopArg : public CThorArgOf<IHThorGraphLoopArg>
{
    virtual unsigned getFlags() override;
};

class ECLRTL_API CThorRemoteArg : public CThorArgOf<IHThorRemoteArg>
{
    virtual IOutputMetaData * queryOutputMeta() override;        // for action variety
    virtual unsigned __int64 getRowLimit() override;
    virtual void onLimitExceeded() override;
};

class ECLRTL_API CThorLibraryCallArg : public CThorSinkLibraryArg
{
};

class ECLRTL_API CThorNWayInputArg : public CThorArgOf<IHThorNWayInputArg>
{
};

class ECLRTL_API CThorNWayGraphLoopResultReadArg : public CThorArgOf<IHThorNWayGraphLoopResultReadArg>
{
    virtual bool isGrouped() const override;
};

class ECLRTL_API CThorNWayMergeArg : public CThorArgOf<IHThorNWayMergeArg>
{
    virtual ISortKeySerializer * querySerialize() override;        // only if global
    virtual ICompare * queryCompareKey() override;
    virtual ICompare * queryCompareRowKey() override;        // row is lhs, key is rhs
    virtual bool dedup() override;
    virtual ISteppingMeta * querySteppingMeta() override;
};

class ECLRTL_API CThorNWayMergeJoinArg : public CThorArgOf<IHThorNWayMergeJoinArg>
{
    virtual unsigned getJoinFlags() override;
    virtual ICompareEq * queryNonSteppedCompare() override;

//For range comparison
    virtual void adjustRangeValue(ARowBuilder & rowBuilder, const void * input, __int64 delta) override;
    virtual unsigned __int64 extractRangeValue(const void * input) override;
    virtual __int64 maxRightBeforeLeft() override;
    virtual __int64 maxLeftBeforeRight() override;

//MJFtransform
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) override;

//MJFleftonly helper
    virtual bool createNextJoinValue(ARowBuilder & rowBuilder, const void * _value) override;

//MJFmofn helper
    virtual unsigned getMinMatches() override;
    virtual unsigned getMaxMatches() override;

    virtual INaryCompareEq * queryGlobalCompare() override;
    virtual size32_t createLowInputRow(ARowBuilder & rowBuilder) override;
    virtual ICompareEq * queryPartitionCompareEq() override;
};

class ECLRTL_API CThorNWaySelectArg : public CThorArgOf<IHThorNWaySelectArg>
{
};

class ECLRTL_API CThorSectionArg : public CThorArgOf<IHThorSectionArg>
{
    virtual unsigned getFlags() override;
    virtual void getDescription(size32_t & _retLen, char * & _retData) override;
};

class ECLRTL_API CThorSectionInputArg : public CThorArgOf<IHThorSectionInputArg>
{
    virtual unsigned getFlags() override;
};

class ECLRTL_API CThorTraceArg : public CThorArgOf<IHThorTraceArg>
{
    virtual bool isValid(const void * _left) override;
    virtual bool canMatchAny() override;
    virtual unsigned getKeepLimit() override;
    virtual unsigned getSample() override;
    virtual unsigned getSkip() override;
    virtual const char *getName() override;
};


class ECLRTL_API CThorWhenActionArg : public CThorArgOf<IHThorWhenActionArg>
{
};

class ECLRTL_API CThorStreamedIteratorArg : public CThorArgOf<IHThorStreamedIteratorArg>
{
};

class ECLRTL_API CThorExternalArg : public CThorArgOf<IHThorExternalArg>
{
public:
    CThorExternalArg(unsigned _numInputs);
    virtual ~CThorExternalArg();
    virtual IRowStream * createOutput(IThorActivityContext * activityContext) override;
    virtual void execute(IThorActivityContext * activityContext) override;
    virtual void setInput(unsigned whichInput, IRowStream * input) override;

protected:
    IRowStream * * inputs;
};


//-- Full implementations of selective activities that don't ever require any access to the context.

class ECLRTL_API CLibraryNullArg : public CThorNullArg
{
public:
    inline CLibraryNullArg(IOutputMetaData * _meta = NULL) : meta(_meta) {}
    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class ECLRTL_API CLibrarySplitArg : public CThorSplitArg
{
public:
    CLibrarySplitArg(unsigned _tempUsageCount, bool _balanced, IOutputMetaData * _meta) :
        tempUsageCount(_tempUsageCount), meta(_meta), balanced(_balanced) {}

    virtual unsigned numBranches()                          { return tempUsageCount; }
    virtual bool isBalanced()                               { return balanced; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    unsigned tempUsageCount;
    IOutputMetaData * meta;
    bool balanced;
};

class ECLRTL_API CLibraryFunnelArg : public CThorFunnelArg
{
public:
    CLibraryFunnelArg(bool _ordered, bool _sequential, IOutputMetaData * _meta) :
        meta(_meta), ordered(_ordered), sequential(_sequential) {}

    virtual bool isOrdered()                            { return ordered; }
    virtual bool pullSequentially()                     { return sequential; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    IOutputMetaData * meta;
    bool ordered;
    bool sequential;
};


class ECLRTL_API CLibraryLocalResultSpillArg : public CThorLocalResultSpillArg
{
public:
    CLibraryLocalResultSpillArg(unsigned _sequence, bool _usedOutside, IOutputMetaData * _meta) :
        sequence(_sequence), meta(_meta), usedOutside(_usedOutside) {}

    virtual unsigned querySequence() { return sequence; }
    virtual bool usedOutsideGraph() { return usedOutside; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    unsigned sequence;
    IOutputMetaData * meta;
    bool usedOutside;
};


class ECLRTL_API CLibraryWorkUnitReadArg : public CThorWorkunitReadArg
{
public:
    CLibraryWorkUnitReadArg(const char * _name, IOutputMetaData * _meta) :
        name(_name), meta(_meta) {}

    virtual const char * queryName() { return name; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    const char * name;
    IOutputMetaData * meta;
};


class ECLRTL_API CLibraryWorkUnitWriteArg : public CThorWorkUnitWriteArg
{
public:
    CLibraryWorkUnitWriteArg(const char * _name, unsigned _flags, IOutputMetaData * _meta) :
        name(_name), meta(_meta), flags(_flags)  {}

    virtual const char * queryName() { return name; }
    virtual unsigned getFlags() { return flags; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }

protected:
    const char * name;
    IOutputMetaData * meta;
    unsigned flags;
};


class ECLRTL_API CLibraryMemorySpillSplitArg : public CThorSpillArg
{
public:
    CLibraryMemorySpillSplitArg(unsigned _tempUsageCount, const char * _filename, IOutputMetaData * _meta) :
        meta(_meta), filename(_filename), tempUsageCount(_tempUsageCount) {}

    virtual unsigned getFlags()                             { return TDXtemporary|TDXcompress|TDWnoreplicate; }
    virtual unsigned getTempUsageCount()                    { return tempUsageCount; }
    virtual unsigned getFormatCrc()                         { rtlFailUnexpected(); return 0; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }
    virtual const char * queryRecordECL()                   { rtlFailUnexpected(); return NULL; }
    virtual const char * getFileName()                      { return filename; }

protected:
    IOutputMetaData * meta;
    const char * filename;
    unsigned tempUsageCount;
};


class ECLRTL_API CLibraryMemorySpillReadArg : public CThorDiskReadArg
{
public:
    CLibraryMemorySpillReadArg(const char * _filename, IOutputMetaData * _meta) :
        filename(_filename), meta(_meta) {}

    virtual unsigned getFlags()                             { return TDXtemporary|TDXcompress; }
    virtual unsigned getDiskFormatCrc()                     { rtlFailUnexpected(); return 0; }
    virtual unsigned getProjectedFormatCrc()                { rtlFailUnexpected(); return 0; }
    virtual IOutputMetaData * queryDiskRecordSize()         { return meta; }
    virtual IOutputMetaData * queryProjectedDiskRecordSize() { return meta; }
    virtual IOutputMetaData * queryOutputMeta()             { return meta; }
    virtual const char * getFileName()                      { return filename; }
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left) { rtlFailUnexpected(); return 0; }

protected:
    const char * filename;
    IOutputMetaData * meta;
};


class ECLRTL_API CLibraryWhenActionArg : public CThorWhenActionArg
{
public:
    inline CLibraryWhenActionArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};


class ECLRTL_API CLibraryDegroupArg : public CThorDegroupArg
{
public:
    inline CLibraryDegroupArg(IOutputMetaData * _meta = NULL) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};


typedef size32_t (*rowClearFunction)(ARowBuilder & crSelf, IResourceContext * ctx);
class ECLRTL_API CLibrarySelectNArg : public CThorSelectNArg
{
public:
    inline CLibrarySelectNArg(unsigned __int64 _row, rowClearFunction _rowClear, IOutputMetaData * _meta = NULL)
        : meta(_meta), row(_row), rowClear(_rowClear) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }
    virtual unsigned __int64 getRowToSelect() { return row; }
    virtual size32_t createDefault(ARowBuilder & rowBuilder) { return rowClear(rowBuilder, ctx); }

protected:
    IOutputMetaData * meta;
    unsigned __int64 row;
    rowClearFunction rowClear;
};


class ECLRTL_API CLibraryLocalResultReadArg : public CThorLocalResultReadArg
{
public:
    inline CLibraryLocalResultReadArg(unsigned _sequence, IOutputMetaData * _meta = NULL)
        : meta(_meta), sequence(_sequence) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }
    virtual unsigned querySequence() { return sequence; }

protected:
    IOutputMetaData * meta;
    unsigned sequence;
};


class ECLRTL_API CLibraryGraphLoopResultWriteArg : public CThorGraphLoopResultWriteArg
{
public:
    inline CLibraryGraphLoopResultWriteArg(IOutputMetaData * _meta)
        : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class ECLRTL_API CLibraryCountAggregateArg : public CThorCountAggregateArg
{
public:
    inline CLibraryCountAggregateArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class ECLRTL_API CLibraryExistsAggregateArg : public CThorExistsAggregateArg
{
public:
    inline CLibraryExistsAggregateArg(IOutputMetaData * _meta) : meta(_meta) {}

    virtual IOutputMetaData * queryOutputMeta() { return meta; }

protected:
    IOutputMetaData * meta;
};

class ECLRTL_API CLibraryConstantRawIteratorArg : public CThorLinkedRawIteratorArg
{
public:
    inline CLibraryConstantRawIteratorArg(unsigned _numRows, const byte * * _rows, IOutputMetaData * _meta)
        : numRows(_numRows), meta(_meta), rows(_rows)
    {
        cur = 0;
    }

    virtual void onStart(const byte *, MemoryBuffer * ) override { cur = 0U; }

    virtual const byte * next() override
    {
        if (cur < numRows)
            return rows[cur++];
        return NULL;
    }

    virtual IOutputMetaData * queryOutputMeta() override { return meta; }

protected:
    unsigned numRows;
    IOutputMetaData * meta;
    const byte * * rows;
    unsigned cur;
};

class ECLRTL_API EclProcess : implements IEclProcess, public RtlCInterface
{
public:
    RTLIMPLEMENT_IINTERFACE
};



#endif
