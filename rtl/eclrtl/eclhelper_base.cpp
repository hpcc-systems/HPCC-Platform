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

#include "platform.h"

#include "eclrtl.hpp"
#include "eclhelper.hpp"
#include "rtlkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlfield.hpp"
#include "rtlds_imp.hpp"
#include "eclhelper_base.hpp"
#include "rtlbcd.hpp"
#include "rtlrecord.hpp"

//---------------------------------------------------------------------------

//CThorIndexWriteArg

unsigned CThorIndexWriteArg::getFlags() { return 0; }
const char * CThorIndexWriteArg::getDistributeIndexName() { return NULL; }
unsigned CThorIndexWriteArg::getExpiryDays() { return 0; }
void CThorIndexWriteArg::getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) { }
const char * CThorIndexWriteArg::getCluster(unsigned idx) { return NULL; }
bool CThorIndexWriteArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
bool CThorIndexWriteArg::getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx) { return false; }
unsigned CThorIndexWriteArg::getWidth() { return 0; }
ICompare * CThorIndexWriteArg::queryCompare() { return NULL; }
const IBloomBuilderInfo * const *CThorIndexWriteArg::queryBloomInfo() const { return nullptr; }
__uint64 CThorIndexWriteArg::getPartitionFieldMask() const { return 0; }

//CBloomBuilderInfo
__uint64 CBloomBuilderInfo::getBloomFields() const
{
    return 0;
}
unsigned CBloomBuilderInfo::getBloomLimit() const
{
    return 1000000;
}
double CBloomBuilderInfo::getBloomProbability() const
{
    return 0.01;
}
bool CBloomBuilderInfo::getBloomEnabled() const
{
    return true;
}

//CThorFirstNArg

__int64 CThorFirstNArg::numToSkip() { return 0; }
bool CThorFirstNArg::preserveGrouping() { return false; }

//CThorDiskWriteArg
int CThorDiskWriteArg::getSequence() { return -3; }
unsigned CThorDiskWriteArg::getFlags() { return 0; }
unsigned CThorDiskWriteArg::getTempUsageCount() { return 0; }
unsigned CThorDiskWriteArg::getExpiryDays() { return 0; }
void CThorDiskWriteArg::getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) { }
void CThorDiskWriteArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
const char * CThorDiskWriteArg::getCluster(unsigned idx) { return NULL; }

//CThorPipeReadArg
unsigned CThorPipeReadArg::getPipeFlags() { return 0; }
ICsvToRowTransformer * CThorPipeReadArg::queryCsvTransformer() { return NULL; }
IXmlToRowTransformer * CThorPipeReadArg::queryXmlTransformer() { return NULL; }
const char * CThorPipeReadArg::getXmlIteratorPath() { return NULL; }

//CThorPipeWriteArg
char * CThorPipeWriteArg::getNameFromRow(const void * _self) { return NULL; }
bool CThorPipeWriteArg::recreateEachRow() { return (getPipeFlags() & TPFrecreateeachrow) != 0; }
unsigned CThorPipeWriteArg::getPipeFlags() { return 0; }
IHThorCsvWriteExtra * CThorPipeWriteArg::queryCsvOutput() { return NULL; }
IHThorXmlWriteExtra * CThorPipeWriteArg::queryXmlOutput() { return NULL; }

//CThorPipeThroughArg
char * CThorPipeThroughArg::getNameFromRow(const void * _self) { return NULL; }
bool CThorPipeThroughArg::recreateEachRow() { return (getPipeFlags() & TPFrecreateeachrow) != 0; }
unsigned CThorPipeThroughArg::getPipeFlags() { return 0; }
IHThorCsvWriteExtra * CThorPipeThroughArg::queryCsvOutput() { return NULL; }
IHThorXmlWriteExtra * CThorPipeThroughArg::queryXmlOutput() { return NULL; }
ICsvToRowTransformer * CThorPipeThroughArg::queryCsvTransformer() { return NULL; }
IXmlToRowTransformer * CThorPipeThroughArg::queryXmlTransformer() { return NULL; }
const char * CThorPipeThroughArg::getXmlIteratorPath() { return NULL; }

//CThorFilterArg
bool CThorFilterArg::canMatchAny() { return true; }
bool CThorFilterArg::isValid(const void * _left) { return true; }

//CThorFilterGroupArg

bool CThorFilterGroupArg::canMatchAny() { return true; }
bool CThorFilterGroupArg::isValid(unsigned _num, const void * * _rows) { return true; }

//CThorIterateArg

bool CThorIterateArg::canFilter() { return false; }

//CThorProcessArg

bool CThorProcessArg::canFilter() { return false; }

//CThorProjectArg

bool CThorProjectArg::canFilter() { return false; }

//CThorQuantileArg

unsigned CThorQuantileArg::getFlags() { return 0; }
unsigned __int64 CThorQuantileArg::getNumDivisions() { return 2; }
double CThorQuantileArg::getSkew() { return 0; }
unsigned __int64 CThorQuantileArg::getScore(const void * _left) { return 1; }
void CThorQuantileArg::getRange(bool & isAll, size32_t & tlen, void * & tgt) { isAll = true; tlen = 0; tgt = NULL; }

//CThorPrefetchProjectArg

bool CThorPrefetchProjectArg::canFilter() { return false; }
bool CThorPrefetchProjectArg::canMatchAny() { return true; }
unsigned CThorPrefetchProjectArg::getFlags() { return 0; }
unsigned CThorPrefetchProjectArg::getLookahead() { return 0; }
IThorChildGraph *CThorPrefetchProjectArg::queryChild() { return NULL; }
bool CThorPrefetchProjectArg::preTransform(rtlRowBuilder & extract, const void * _left, unsigned __int64 _counter) { return true; }

//CThorFilterProjectArg

bool CThorFilterProjectArg::canFilter() { return false; }
bool CThorFilterProjectArg::canMatchAny() { return true; }

//CThorCountProjectArg

bool CThorCountProjectArg::canFilter() { return false; }

//CThorCombineArg

bool CThorCombineArg::canFilter() { return false; }

//CThorCombineGroupArg

bool CThorCombineGroupArg::canFilter() { return false; }

// CThorRollupGroupArg
bool CThorRollupGroupArg::canFilter() { return false; }

//CThorActionArg

void CThorActionArg::action() {}

//CThorLimitArg

void CThorLimitArg::onLimitExceeded() {}
size32_t CThorLimitArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorCatchArg

unsigned CThorCatchArg::getFlags() { return 0; }
bool CThorCatchArg::isHandler(IException * e) { return true; }
void CThorCatchArg::onExceptionCaught() { }
size32_t CThorCatchArg::transformOnExceptionCaught(ARowBuilder & rowBuilder, IException * e) { return 0; }

//CThorSplitArg

unsigned CThorSplitArg::numBranches() { return 2; }
bool CThorSplitArg::isBalanced() { return false; }

//CThorSpillArg

IOutputMetaData * CThorSpillArg::queryDiskRecordSize() { return queryOutputMeta(); }
int CThorSpillArg::getSequence() { return -3; }
unsigned CThorSpillArg::getFlags() { return 0; }
unsigned CThorSpillArg::getTempUsageCount() { return 1; }
unsigned CThorSpillArg::getExpiryDays() { return 0; }
void CThorSpillArg::getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) { }
void CThorSpillArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
const char * CThorSpillArg::getCluster(unsigned idx) { return NULL; }

//CThorRollupArg

unsigned CThorRollupArg::getFlags() { return 0; }
bool CThorRollupArg::matches(const void * _left, const void * _right) { return true; }

//CThorDedupArg

bool CThorDedupArg::matches(const void * _left, const void * _right) { return true; }
unsigned CThorDedupArg::numToKeep() { return 1; }
ICompare * CThorDedupArg::queryComparePrimary() { return NULL; }
unsigned CThorDedupArg::getFlags() { return HDFkeepleft; }
ICompare * CThorDedupArg::queryCompareBest() { return NULL; }

//CThorAggregateArg

unsigned CThorAggregateArg::getAggregateFlags() { return 0; }
size32_t CThorAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorCountAggregateArg

size32_t CThorCountAggregateArg::clearAggregate(ARowBuilder & rowBuilder)
{
    void * target = rowBuilder.getSelf();
    *((unsigned __int64 *)target) = 0;
    return sizeof(unsigned __int64);
}
size32_t CThorCountAggregateArg::processFirst(ARowBuilder & rowBuilder, const void * src)
{
    void * target = rowBuilder.getSelf();
    *((unsigned __int64 *)target) = 1;
    return sizeof(unsigned __int64);
}
size32_t CThorCountAggregateArg::processNext(ARowBuilder & rowBuilder, const void * src)
{
    void * target = rowBuilder.getSelf();
    ++*((unsigned __int64 *)target);
    return sizeof(unsigned __int64);
}
size32_t CThorCountAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src)
{
    void * target = rowBuilder.getSelf();
    *((unsigned __int64 *)target) += *((unsigned __int64 *)src);
    return sizeof(unsigned __int64);
}

//CThorExistsAggregateArg

size32_t CThorExistsAggregateArg::clearAggregate(ARowBuilder & rowBuilder)
{
    void * target = rowBuilder.getSelf();
    *((bool *)target) = false;
    return sizeof(bool);
}
size32_t CThorExistsAggregateArg::processFirst(ARowBuilder & rowBuilder, const void * src)
{
    void * target = rowBuilder.getSelf();
    *((bool *)target) = true;
    return sizeof(bool);
}
size32_t CThorExistsAggregateArg::processNext(ARowBuilder & rowBuilder, const void * src)
{
    return sizeof(bool);
}
size32_t CThorExistsAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src)
{
    void * target = rowBuilder.getSelf();
    if (*((bool *)src))
        *((bool *)target) = true;
    return sizeof(bool);
}

//CThorThroughAggregateArg

unsigned CThorThroughAggregateArg::getAggregateFlags() { return 0; }
size32_t CThorThroughAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorGroupAggregateArg

unsigned CThorGroupAggregateArg::getAggregateFlags() { return 0; }
size32_t CThorGroupAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorHashAggregateArg

unsigned CThorHashAggregateArg::getAggregateFlags() { return 0; }
size32_t CThorHashAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }


//CThorInlineTableArg

unsigned CThorInlineTableArg::getFlags() { return 0; }

//CThorInlineRowArg

__uint64 CThorInlineRowArg::numRows() { return 1; }

//CThorFunnelArg

bool CThorFunnelArg::isOrdered() { return false; }
bool CThorFunnelArg::pullSequentially() { return false; }

//CThorMergeArg

ISortKeySerializer * CThorMergeArg::querySerialize() { return NULL; }        // only if global
ICompare * CThorMergeArg::queryCompareKey() { return NULL; }
ICompare * CThorMergeArg::queryCompareRowKey() { return NULL; }        // row is lhs, key is rhs
bool CThorMergeArg::dedup() { return false; }

//CThorRemoteResultArg

int CThorRemoteResultArg::getSequence() { return -3; }

//CThorApplyArg

void CThorApplyArg::start() { }
void CThorApplyArg::end() { }

//CThorSortArg

double CThorSortArg::getSkew() { return 0; }           // 0=default
bool CThorSortArg::hasManyRecords() { return false; }
double CThorSortArg::getTargetSkew() { return 0; }
ISortKeySerializer * CThorSortArg::querySerialize() { return NULL; }
unsigned __int64 CThorSortArg::getThreshold() { return 0; }
IOutputMetaData * CThorSortArg::querySortedRecordSize() { return NULL; }
const char * CThorSortArg::getSortedFilename() { return NULL; }
ICompare * CThorSortArg::queryCompareLeftRight() { return NULL; }
ICompare * CThorSortArg::queryCompareSerializedRow() { return NULL; }
unsigned CThorSortArg::getAlgorithmFlags() { return TAFconstant; }
const char * CThorSortArg::getAlgorithm() { return NULL; }

//CThorTopNArg

double CThorTopNArg::getSkew() { return 0; }           // 0=default
bool CThorTopNArg::hasManyRecords() { return false; }
double CThorTopNArg::getTargetSkew() { return 0; }
ISortKeySerializer * CThorTopNArg::querySerialize() { return NULL; }
unsigned __int64 CThorTopNArg::getThreshold() { return 0; }
IOutputMetaData * CThorTopNArg::querySortedRecordSize() { return NULL; }
const char * CThorTopNArg::getSortedFilename() { return NULL; }
ICompare * CThorTopNArg::queryCompareLeftRight() { return NULL; }
ICompare * CThorTopNArg::queryCompareSerializedRow() { return NULL; }
unsigned CThorTopNArg::getAlgorithmFlags() { return TAFconstant; }
const char * CThorTopNArg::getAlgorithm() { return NULL; }

bool CThorTopNArg::hasBest() { return false; }
int CThorTopNArg::compareBest(const void * _left) { return +1; }

//CThorSubSortArg

double CThorSubSortArg::getSkew() { return 0; }           // 0=default
bool CThorSubSortArg::hasManyRecords() { return false; }
double CThorSubSortArg::getTargetSkew() { return 0; }
ISortKeySerializer * CThorSubSortArg::querySerialize() { return NULL; }
unsigned __int64 CThorSubSortArg::getThreshold() { return 0; }
IOutputMetaData * CThorSubSortArg::querySortedRecordSize() { return NULL; }
const char * CThorSubSortArg::getSortedFilename() { return NULL; }
ICompare * CThorSubSortArg::queryCompareLeftRight() { return NULL; }
ICompare * CThorSubSortArg::queryCompareSerializedRow() { return NULL; }

//CThorKeyedJoinArg

bool CThorKeyedJoinArg::diskAccessRequired() { return false; }
const char * CThorKeyedJoinArg::getFileName() { return NULL; }
IOutputMetaData * CThorKeyedJoinArg::queryDiskRecordSize() { return NULL; }
IOutputMetaData * CThorKeyedJoinArg::queryProjectedDiskRecordSize() { return NULL; }
unsigned __int64 CThorKeyedJoinArg::extractPosition(const void * _right) { return 0; }

unsigned CThorKeyedJoinArg::getFetchFlags() { return 0; }
unsigned CThorKeyedJoinArg::getDiskFormatCrc() { return 0; }
unsigned CThorKeyedJoinArg::getProjectedFormatCrc() { return getDiskFormatCrc(); }  // Should really be crc of queryProjectedDiskRecordSize layout
void CThorKeyedJoinArg::getFileEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

bool CThorKeyedJoinArg::leftCanMatch(const void * inputRow) { return true; }
bool CThorKeyedJoinArg::indexReadMatch(const void * indexRow, const void * inputRow, IBlobProvider * blobs) { return true; }

unsigned __int64 CThorKeyedJoinArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorKeyedJoinArg::onLimitExceeded() { }
unsigned __int64 CThorKeyedJoinArg::getSkipLimit() { return 0; }
unsigned CThorKeyedJoinArg::getMatchAbortLimit() { return 0; }
void CThorKeyedJoinArg::onMatchAbortLimitExceeded() { }
unsigned CThorKeyedJoinArg::getProjectedIndexFormatCrc() { return getIndexFormatCrc(); }  // Should really be crc of queryIndexReadInputRecordSize layout (?)


unsigned CThorKeyedJoinArg::getJoinLimit() { return 0; }
unsigned CThorKeyedJoinArg::getKeepLimit() { return 0; }
unsigned CThorKeyedJoinArg::getJoinFlags() { return 0; }
bool CThorKeyedJoinArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }

size32_t CThorKeyedJoinArg::extractFetchFields(ARowBuilder & rowBuilder, const void * _input) { return 0; }
bool CThorKeyedJoinArg::fetchMatch(const void * diskRow, const void * inputRow) { return true; }
size32_t CThorKeyedJoinArg::createDefaultRight(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorKeyedJoinArg::onFailTransform(ARowBuilder & rowBuilder, const void * _dummyRight, const void * _origRow, unsigned __int64 keyedFpos, IException * e) { return 0; }
//Join:
//Denormalize:
size32_t CThorKeyedJoinArg::transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned __int64 keyedFpos, unsigned counter) { return 0; }
//Denormalize group:
size32_t CThorKeyedJoinArg::transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned _numRows, const void * * _rows) { return 0; }


//CThorJoinArg

bool CThorJoinArg::isLeftAlreadySorted() { return false; }
bool CThorJoinArg::isRightAlreadySorted() { return false; }
size32_t CThorJoinArg::createDefaultLeft(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorJoinArg::createDefaultRight(ARowBuilder & rowBuilder) { return 0; }
bool CThorJoinArg::match(const void * _left, const void * _right) { return true; }
ISortKeySerializer * CThorJoinArg::querySerializeLeft() { return NULL; }
ISortKeySerializer * CThorJoinArg::querySerializeRight() { return NULL; }
unsigned __int64 CThorJoinArg::getThreshold() { return 0; }
double CThorJoinArg::getSkew() { return 0; }           // 0=default
double CThorJoinArg::getTargetSkew() { return 0; }           // 0=default
unsigned CThorJoinArg::getJoinLimit() { return 0; }
unsigned CThorJoinArg::getKeepLimit() { return 0; }
unsigned CThorJoinArg::getJoinFlags() { return 0; }
unsigned CThorJoinArg::getMatchAbortLimit() { return 0; }
void CThorJoinArg::onMatchAbortLimitExceeded() { }
ICompare * CThorJoinArg::queryCompareLeftRightLower() { return NULL; }
ICompare * CThorJoinArg::queryCompareLeftRightUpper() { return NULL; }
ICompare * CThorJoinArg::queryPrefixCompare() { return NULL; }
ICompare * CThorJoinArg::queryCompareLeftKeyRightRow() { return NULL; }
ICompare * CThorJoinArg::queryCompareRightKeyLeftRow() { return NULL; }
size32_t CThorJoinArg::onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e, unsigned flggs) { return 0; }
size32_t CThorJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) { return 0; }
size32_t CThorJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) { return 0; }

//CThorAllJoinArg

size32_t CThorAllJoinArg::createDefaultLeft(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorAllJoinArg::createDefaultRight(ARowBuilder & rowBuilder) { return 0; }
bool CThorAllJoinArg::match(const void * _left, const void * _right) { return true; }
unsigned CThorAllJoinArg::getKeepLimit() { return 0; }
unsigned CThorAllJoinArg::getJoinFlags() { return 0; }
size32_t CThorAllJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) { return 0; }
size32_t CThorAllJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) { return 0; }
unsigned CThorAllJoinArg::getMatchAbortLimit() { return 0; }
void CThorAllJoinArg::onMatchAbortLimitExceeded() { }

//CThorHashJoinArg

bool CThorHashJoinArg::isLeftAlreadySorted() { return false; }
bool CThorHashJoinArg::isRightAlreadySorted() { return false; }
size32_t CThorHashJoinArg::createDefaultLeft(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorHashJoinArg::createDefaultRight(ARowBuilder & rowBuilder) { return 0; }
bool CThorHashJoinArg::match(const void * _left, const void * _right) { return true; }
ISortKeySerializer * CThorHashJoinArg::querySerializeLeft() { return NULL; }
ISortKeySerializer * CThorHashJoinArg::querySerializeRight() { return NULL; }
unsigned __int64 CThorHashJoinArg::getThreshold() { return 0; }
double CThorHashJoinArg::getSkew() { return 0; }           // 0=default
double CThorHashJoinArg::getTargetSkew() { return 0; }           // 0=default
unsigned CThorHashJoinArg::getJoinLimit() { return 0; }
unsigned CThorHashJoinArg::getKeepLimit() { return 0; }
unsigned CThorHashJoinArg::getJoinFlags() { return 0; }
unsigned CThorHashJoinArg::getMatchAbortLimit() { return 0; }
void CThorHashJoinArg::onMatchAbortLimitExceeded() { }
ICompare * CThorHashJoinArg::queryCompareLeftRightLower() { return NULL; }
ICompare * CThorHashJoinArg::queryCompareLeftRightUpper() { return NULL; }
ICompare * CThorHashJoinArg::queryCompareLeft() { return NULL; }        // not needed for lookup
ICompare * CThorHashJoinArg::queryCompareRight() { return NULL; }        // not needed for many lookup
ICompare * CThorHashJoinArg::queryPrefixCompare() { return NULL; }
ICompare * CThorHashJoinArg::queryCompareLeftKeyRightRow() { return NULL; }
ICompare * CThorHashJoinArg::queryCompareRightKeyLeftRow() { return NULL; }
size32_t CThorHashJoinArg::onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e, unsigned flags) { return 0; }
size32_t CThorHashJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned flags) { return 0; }
size32_t CThorHashJoinArg::transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned flags) { return 0; }

//CThorKeyedDistributeArg

unsigned CThorKeyedDistributeArg::getFlags() { return 0; }
bool CThorKeyedDistributeArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }

//CThorWorkUnitWriteArg

int CThorWorkUnitWriteArg::getSequence() { return -3; }
const char * CThorWorkUnitWriteArg::queryName() { return NULL; }
unsigned CThorWorkUnitWriteArg::getFlags() { return 0; }
void CThorWorkUnitWriteArg::serializeXml(const byte * self, IXmlWriter & out) { queryOutputMeta()->toXML(self, out); }
unsigned CThorWorkUnitWriteArg::getMaxSize() { return 0; }

//CThorXmlWorkunitWriteArg

int CThorXmlWorkunitWriteArg::getSequence() { return -3; }
const char * CThorXmlWorkunitWriteArg::queryName() { return NULL; }
unsigned CThorXmlWorkunitWriteArg::getFlags() { return 0; }

//CThorDictionaryWorkUnitWriteArg

int CThorDictionaryWorkUnitWriteArg::getSequence() { return -3; }
const char * CThorDictionaryWorkUnitWriteArg::queryName() { return NULL; }
unsigned CThorDictionaryWorkUnitWriteArg::getFlags() { return 0; }

//CThorDictionaryResultWriteArg

bool CThorDictionaryResultWriteArg::usedOutsideGraph() { return true; }

//CThorHashDistributeArg

IHash    * CThorHashDistributeArg::queryHash() { return NULL; }
double CThorHashDistributeArg::getSkew() { return 0; }           // 0=default
double CThorHashDistributeArg::getTargetSkew() { return 0; }           // 0=default
ICompare * CThorHashDistributeArg::queryMergeCompare() { return NULL; }

//CThorNWayDistributeArg

unsigned CThorNWayDistributeArg::getFlags() { return 0; }
bool     CThorNWayDistributeArg::include(const byte * left, unsigned targetNode) { return true; }    // default to include all

//CThorHashDedupArg

unsigned CThorHashDedupArg::getFlags() { return HDFkeepleft; }
ICompare * CThorHashDedupArg::queryCompareBest() { return NULL; }
IOutputMetaData * CThorHashDedupArg::queryKeySize() { return NULL; }
size32_t CThorHashDedupArg::recordToKey(ARowBuilder & rowBuilder, const void * _record) { return 0; }

//CThorKeyDiffArg

unsigned CThorKeyDiffArg::getFlags() { return 0; }
unsigned CThorKeyDiffArg::getExpiryDays() { return 0; }

//CThorKeyPatchArg

unsigned CThorKeyPatchArg::getFlags() { return 0; }
unsigned CThorKeyPatchArg::getExpiryDays() { return 0; }

//CThorWorkunitReadArg

int CThorWorkunitReadArg::querySequence() { return -3; }
const char * CThorWorkunitReadArg::getWUID() { return NULL; }
ICsvToRowTransformer * CThorWorkunitReadArg::queryCsvTransformer() { return NULL; }
IXmlToRowTransformer * CThorWorkunitReadArg::queryXmlTransformer() { return NULL; }

//CThorLocalResultWriteArg

bool CThorLocalResultWriteArg::usedOutsideGraph() { return true; }

//CThorCsvWriteArg

int CThorCsvWriteArg::getSequence() { return -3; }
unsigned CThorCsvWriteArg::getFlags() { return 0; }
unsigned CThorCsvWriteArg::getTempUsageCount() { return 0; }
unsigned CThorCsvWriteArg::getExpiryDays() { return 0; }
void CThorCsvWriteArg::getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) { }
void CThorCsvWriteArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
const char * CThorCsvWriteArg::getCluster(unsigned idx) { return NULL; }

//CThorXmlParseArg

bool CThorXmlParseArg::requiresContents() { return false; }


unsigned CThorFetchArg::getFetchFlags() { return 0; }
unsigned CThorFetchArg::getDiskFormatCrc() { return 0; }
unsigned CThorFetchArg::getProjectedFormatCrc() { return getDiskFormatCrc(); }  // Should really be crc of queryProjectedDiskRecordSize layout
void CThorFetchArg::getFileEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
unsigned __int64 CThorFetchArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorFetchArg::onLimitExceeded()         { }
size32_t CThorFetchArg::extractJoinFields(ARowBuilder & rowBuilder, const void * _right) { return 0; }
bool CThorFetchArg::extractAllJoinFields()    { return false; }
IOutputMetaData * CThorFetchArg::queryExtractedSize() { return NULL; }

unsigned CThorCsvFetchArg::getFetchFlags() { return 0; }
unsigned CThorCsvFetchArg::getDiskFormatCrc() { return 0; }
unsigned CThorCsvFetchArg::getProjectedFormatCrc() { return getDiskFormatCrc(); }  // Should really be crc of queryProjectedDiskRecordSize layout
void CThorCsvFetchArg::getFileEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
unsigned __int64 CThorCsvFetchArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorCsvFetchArg::onLimitExceeded()         { }
size32_t CThorCsvFetchArg::extractJoinFields(ARowBuilder & rowBuilder, const void * _right) { return 0; }
bool CThorCsvFetchArg::extractAllJoinFields()    { return false; }
IOutputMetaData * CThorCsvFetchArg::queryExtractedSize() { return NULL; }

//CThorXmlFetchArg

unsigned CThorXmlFetchArg::getFetchFlags() { return 0; }
unsigned CThorXmlFetchArg::getDiskFormatCrc() { return 0; }
unsigned CThorXmlFetchArg::getProjectedFormatCrc() { return getDiskFormatCrc(); }  // Should really be crc of queryProjectedDiskRecordSize layout
void CThorXmlFetchArg::getFileEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
bool CThorXmlFetchArg::requiresContents() { return false; }
unsigned __int64 CThorXmlFetchArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorXmlFetchArg::onLimitExceeded()         { }
size32_t CThorXmlFetchArg::extractJoinFields(ARowBuilder & rowBuilder, const void * _right) { return 0; }
bool CThorXmlFetchArg::extractAllJoinFields()    { return false; }
IOutputMetaData * CThorXmlFetchArg::queryExtractedSize() { return NULL; }

//CThorXmlWriteArg

const char * CThorXmlWriteArg::getXmlIteratorPath() { return NULL; }             // supplies the prefix and suffix for a row
const char * CThorXmlWriteArg::getHeader() { return NULL; }
const char * CThorXmlWriteArg::getFooter() { return NULL; }
unsigned CThorXmlWriteArg::getXmlFlags() { return 0; }

int CThorXmlWriteArg::getSequence() { return -3; }
unsigned CThorXmlWriteArg::getFlags() { return 0; }
unsigned CThorXmlWriteArg::getTempUsageCount() { return 0; }
unsigned CThorXmlWriteArg::getExpiryDays() { return 0; }
void CThorXmlWriteArg::getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) { }
void CThorXmlWriteArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
const char * CThorXmlWriteArg::getCluster(unsigned idx) { return NULL; }

//CHThorXmlWriteExtra
const char * CHThorXmlWriteExtra::getXmlIteratorPath() { return NULL; }             // supplies the prefix and suffix for a row
const char * CHThorXmlWriteExtra::getHeader() { return NULL; }
const char * CHThorXmlWriteExtra::getFooter() { return NULL; }
unsigned CHThorXmlWriteExtra::getXmlFlags() { return 0; }

//CThorSoapActionArg

void CThorSoapActionArg::toXML(const byte * self, IXmlWriter & out) { return; }
const char * CThorSoapActionArg::getHeader() { return NULL; }
const char * CThorSoapActionArg::getFooter() { return NULL; }
unsigned CThorSoapActionArg::getFlags() { return 0; }
unsigned CThorSoapActionArg::numParallelThreads() { return 0; }
unsigned CThorSoapActionArg::numRecordsPerBatch() { return 0; }
int CThorSoapActionArg::numRetries() { return -1; }
double CThorSoapActionArg::getTimeout() { return -1.0; }
double CThorSoapActionArg::getTimeLimit() { return 0.0; }
const char * CThorSoapActionArg::getSoapAction() { return NULL; }
const char * CThorSoapActionArg::getNamespaceName() { return NULL; }
const char * CThorSoapActionArg::getNamespaceVar() { return NULL; }
const char * CThorSoapActionArg::getHttpHeaderName() { return NULL; }
const char * CThorSoapActionArg::getHttpHeaderValue() { return NULL; }
const char * CThorSoapActionArg::getHttpHeaders() { return NULL; }
const char * CThorSoapActionArg::getProxyAddress() { return NULL; }
const char * CThorSoapActionArg::getAcceptType() { return NULL; }
IXmlToRowTransformer * CThorSoapActionArg::queryInputTransformer() { return NULL; }
const char * CThorSoapActionArg::getInputIteratorPath() { return NULL; }
size32_t CThorSoapActionArg::onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) { return 0; }
void CThorSoapActionArg::getLogText(size32_t & lenText, char * & text, const void * left) { lenText =0; text = NULL; }
const char * CThorSoapActionArg::getXpathHintsXml() { return nullptr;}
const char * CThorSoapActionArg::getRequestHeader() { return nullptr; }
const char * CThorSoapActionArg::getRequestFooter() { return nullptr; }

//CThorSoapCallArg

void CThorSoapCallArg::toXML(const byte * self, IXmlWriter & out) { return; }
const char * CThorSoapCallArg::getHeader() { return NULL; }
const char * CThorSoapCallArg::getFooter() { return NULL; }
unsigned CThorSoapCallArg::getFlags() { return 0; }
unsigned CThorSoapCallArg::numParallelThreads() { return 0; }
unsigned CThorSoapCallArg::numRecordsPerBatch() { return 0; }
int CThorSoapCallArg::numRetries() { return -1; }
double CThorSoapCallArg::getTimeout() { return -1.0; }
double CThorSoapCallArg::getTimeLimit() { return 0.0; }
const char * CThorSoapCallArg::getSoapAction() { return NULL; }
const char * CThorSoapCallArg::getNamespaceName() { return NULL; }
const char * CThorSoapCallArg::getNamespaceVar() { return NULL; }
const char * CThorSoapCallArg::getHttpHeaderName() { return NULL; }
const char * CThorSoapCallArg::getHttpHeaderValue() { return NULL; }
const char * CThorSoapCallArg::getHttpHeaders() { return NULL; }
const char * CThorSoapCallArg::getProxyAddress() { return NULL; }
const char * CThorSoapCallArg::getAcceptType() { return NULL; }
IXmlToRowTransformer * CThorSoapCallArg::queryInputTransformer() { return NULL; }
const char * CThorSoapCallArg::getInputIteratorPath() { return NULL; }
const char * CThorSoapCallArg::getXpathHintsXml() { return nullptr; }
const char * CThorSoapCallArg::getRequestHeader() { return nullptr; }
const char * CThorSoapCallArg::getRequestFooter() { return nullptr; }

size32_t CThorSoapCallArg::onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) { return 0; }
void CThorSoapCallArg::getLogText(size32_t & lenText, char * & text, const void * left) { lenText =0; text = NULL; }

//CThorParseArg

INlpHelper * CThorParseArg::queryHelper() { return NULL; }
unsigned CThorParseArg::getFlags() { return 0; }
IOutputMetaData * CThorParseArg::queryProductionMeta(unsigned id) { return NULL; }
size32_t CThorParseArg::executeProduction(ARowBuilder & rowBuilder, unsigned id, IProductionCallback * input) { return 0; }

//CThorIndexReadArg

unsigned CThorIndexReadArg::getFlags() { return 0; }
bool CThorIndexReadArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
void CThorIndexReadArg::setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
bool CThorIndexReadArg::canMatchAny()                              { return true; }
void CThorIndexReadArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorIndexReadArg::canMatch(const void * row)                 { return true; }
bool CThorIndexReadArg::hasMatchFilter()                           { return false; }
IHThorSteppedSourceExtra * CThorIndexReadArg::querySteppingExtra()  { return nullptr; }

unsigned __int64 CThorIndexReadArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorIndexReadArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorIndexReadArg::onLimitExceeded()                          { }

bool CThorIndexReadArg::needTransform() { return false; }
bool CThorIndexReadArg::transformMayFilter() { return false; }
unsigned __int64 CThorIndexReadArg::getKeyedLimit() { return (unsigned __int64) -1; }
void CThorIndexReadArg::onKeyedLimitExceeded() { }
ISteppingMeta * CThorIndexReadArg::queryRawSteppingMeta() { return NULL; }
ISteppingMeta * CThorIndexReadArg::queryProjectedSteppingMeta() { return NULL; }
void CThorIndexReadArg::mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) {}
size32_t CThorIndexReadArg::unfilteredTransform(ARowBuilder & rowBuilder, const void * src) { return 0; }

size32_t CThorIndexReadArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorIndexReadArg::transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorSteppedIndexReadArg

IHThorSteppedSourceExtra *CThorSteppedIndexReadArg::querySteppingExtra() { return this; }
unsigned CThorSteppedIndexReadArg::getSteppedFlags() { return 0; }
double CThorSteppedIndexReadArg::getPriority() { return 0; }
unsigned CThorSteppedIndexReadArg::getPrefetchSize() { return 0; }


//CThorIndexNormalizeArg

unsigned CThorIndexNormalizeArg::getFlags() { return 0; }
bool CThorIndexNormalizeArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
void CThorIndexNormalizeArg::setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
bool CThorIndexNormalizeArg::canMatchAny()                              { return true; }
void CThorIndexNormalizeArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorIndexNormalizeArg::canMatch(const void * row)                 { return true; }
bool CThorIndexNormalizeArg::hasMatchFilter()                           { return false; }
IHThorSteppedSourceExtra * CThorIndexNormalizeArg::querySteppingExtra()  { return nullptr; }

unsigned __int64 CThorIndexNormalizeArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorIndexNormalizeArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorIndexNormalizeArg::onLimitExceeded()                          { }
unsigned __int64 CThorIndexNormalizeArg::getKeyedLimit()                { return (unsigned __int64) -1; }
void CThorIndexNormalizeArg::onKeyedLimitExceeded()                     { }

size32_t CThorIndexNormalizeArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorIndexNormalizeArg::transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorIndexAggregateArg

unsigned CThorIndexAggregateArg::getFlags() { return 0; }
bool CThorIndexAggregateArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
void CThorIndexAggregateArg::setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
bool CThorIndexAggregateArg::canMatchAny()                              { return true; }
void CThorIndexAggregateArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorIndexAggregateArg::canMatch(const void * row)                 { return true; }
bool CThorIndexAggregateArg::hasMatchFilter()                           { return false; }
IHThorSteppedSourceExtra * CThorIndexAggregateArg::querySteppingExtra()  { return nullptr; }
size32_t CThorIndexAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
void CThorIndexAggregateArg::processRows(ARowBuilder & rowBuilder, size32_t srcLen, const void * src) { rtlFailUnexpected(); }

//CThorIndexCountArg

unsigned CThorIndexCountArg::getFlags() { return 0; }
bool CThorIndexCountArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
void CThorIndexCountArg::setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
bool CThorIndexCountArg::canMatchAny()                              { return true; }
void CThorIndexCountArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorIndexCountArg::canMatch(const void * row)                 { return true; }
bool CThorIndexCountArg::hasMatchFilter()                           { return false; }
IHThorSteppedSourceExtra * CThorIndexCountArg::querySteppingExtra()  { return nullptr; }
unsigned __int64 CThorIndexCountArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorIndexCountArg::onLimitExceeded() { }
unsigned __int64 CThorIndexCountArg::getKeyedLimit() { return (unsigned __int64) -1; }
void CThorIndexCountArg::onKeyedLimitExceeded() { }
size32_t CThorIndexCountArg::numValid(size32_t srcLen, const void * _src)
{
    rtlFailUnexpected();
    return 0;
}
bool CThorIndexCountArg::hasFilter()                                                { return false; }       // also true if denormalized(!)
size32_t CThorIndexCountArg::numValid(const void * src)                             { return 1; }           //NB: Can be > 1 if source is normlized
unsigned __int64 CThorIndexCountArg::getChooseNLimit()                              { return (unsigned __int64) -1; }

//CThorIndexGroupAggregateArg

unsigned CThorIndexGroupAggregateArg::getFlags() { return 0; }
bool CThorIndexGroupAggregateArg::getIndexLayout(size32_t & _retLen, void * & _retData) { return false; }
void CThorIndexGroupAggregateArg::setCallback(IThorIndexCallback * _tc) { fpp = _tc; }
bool CThorIndexGroupAggregateArg::canMatchAny()                              { return true; }
void CThorIndexGroupAggregateArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorIndexGroupAggregateArg::canMatch(const void * row)                 { return true; }
bool CThorIndexGroupAggregateArg::hasMatchFilter()                           { return false; }
IHThorSteppedSourceExtra * CThorIndexGroupAggregateArg::querySteppingExtra()  { return nullptr; }
bool CThorIndexGroupAggregateArg::createGroupSegmentMonitors(IIndexReadContext *ctx) { return false; }
unsigned CThorIndexGroupAggregateArg::getGroupingMaxField() { return 0; }
size32_t CThorIndexGroupAggregateArg::initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
size32_t CThorIndexGroupAggregateArg::processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) { rtlFailUnexpected(); return 0; }
size32_t CThorIndexGroupAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
void CThorIndexGroupAggregateArg::processRows(size32_t srcLen, const void * src, IHThorGroupAggregateCallback * callback) { rtlFailUnexpected(); }


//CThorDiskReadArg

unsigned CThorDiskReadArg::getFlags() { return 0; }
void CThorDiskReadArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorDiskReadArg::canMatchAny()                              { return true; }
void CThorDiskReadArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorDiskReadArg::canMatch(const void * row)                 { return true; }
bool CThorDiskReadArg::hasMatchFilter()                           { return false; }
void CThorDiskReadArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

unsigned __int64 CThorDiskReadArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorDiskReadArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorDiskReadArg::onLimitExceeded()                          { }

bool CThorDiskReadArg::needTransform() { return false; }
bool CThorDiskReadArg::transformMayFilter() { return false; }
unsigned __int64 CThorDiskReadArg::getKeyedLimit() { return (unsigned __int64) -1; }
void CThorDiskReadArg::onKeyedLimitExceeded() { }
ISteppingMeta * CThorDiskReadArg::queryRawSteppingMeta() { return NULL; }
ISteppingMeta * CThorDiskReadArg::queryProjectedSteppingMeta() { return NULL; }
void CThorDiskReadArg::mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) { }
size32_t CThorDiskReadArg::transform(ARowBuilder & rowBuilder, const void * src)
{
    rtlFail(800, "transform() should not be called, input is deserialized");
}

size32_t CThorDiskReadArg::unfilteredTransform(ARowBuilder & rowBuilder, const void * src) { return 0; }
size32_t CThorDiskReadArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorDiskReadArg::transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorDiskNormalizeArg

unsigned CThorDiskNormalizeArg::getFlags() { return 0; }
void CThorDiskNormalizeArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorDiskNormalizeArg::canMatchAny()                              { return true; }
void CThorDiskNormalizeArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorDiskNormalizeArg::canMatch(const void * row)                 { return true; }
bool CThorDiskNormalizeArg::hasMatchFilter()                           { return false; }
void CThorDiskNormalizeArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
unsigned __int64 CThorDiskNormalizeArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorDiskNormalizeArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorDiskNormalizeArg::onLimitExceeded()                          { }
unsigned __int64 CThorDiskNormalizeArg::getKeyedLimit()                { return (unsigned __int64) -1; }
void CThorDiskNormalizeArg::onKeyedLimitExceeded()                     { }
size32_t CThorDiskNormalizeArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorDiskNormalizeArg::transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorDiskAggregateArg

unsigned CThorDiskAggregateArg::getFlags() { return 0; }
void CThorDiskAggregateArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorDiskAggregateArg::canMatchAny()                              { return true; }
void CThorDiskAggregateArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorDiskAggregateArg::canMatch(const void * row)                 { return true; }
bool CThorDiskAggregateArg::hasMatchFilter()                           { return false; }
void CThorDiskAggregateArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
size32_t CThorDiskAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorDiskCountArg

unsigned CThorDiskCountArg::getFlags() { return 0; }
void CThorDiskCountArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorDiskCountArg::canMatchAny()                              { return true; }
void CThorDiskCountArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorDiskCountArg::canMatch(const void * row)                 { return true; }
bool CThorDiskCountArg::hasMatchFilter()                           { return false; }
void CThorDiskCountArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

unsigned __int64 CThorDiskCountArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorDiskCountArg::onLimitExceeded() { }
unsigned __int64 CThorDiskCountArg::getKeyedLimit() { return (unsigned __int64) -1; }
void CThorDiskCountArg::onKeyedLimitExceeded() { }
bool CThorDiskCountArg::hasFilter()                                                { return false; }       // also true if denormalized(!)
size32_t CThorDiskCountArg::numValid(const void * src)                             { return 1; }           //NB: Can be > 1 if source is normlized
unsigned __int64 CThorDiskCountArg::getChooseNLimit()                              { return (unsigned __int64) -1; }

//CThorDiskGroupAggregateArg

unsigned CThorDiskGroupAggregateArg::getFlags() { return 0; }
void CThorDiskGroupAggregateArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorDiskGroupAggregateArg::canMatchAny()                              { return true; }
void CThorDiskGroupAggregateArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorDiskGroupAggregateArg::canMatch(const void * row)                 { return true; }
bool CThorDiskGroupAggregateArg::hasMatchFilter()                           { return false; }
void CThorDiskGroupAggregateArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }
bool CThorDiskGroupAggregateArg::createGroupSegmentMonitors(IIndexReadContext *ctx) { return false; }
unsigned CThorDiskGroupAggregateArg::getGroupingMaxField() { return 0; }
size32_t CThorDiskGroupAggregateArg::initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }
size32_t CThorDiskGroupAggregateArg::processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) { rtlFailUnexpected(); return 0; }
size32_t CThorDiskGroupAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorCsvReadArg

unsigned CThorCsvReadArg::getFlags() { return 0; }
unsigned __int64 CThorCsvReadArg::getChooseNLimit() { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorCsvReadArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorCsvReadArg::onLimitExceeded() { }
unsigned CThorCsvReadArg::getDiskFormatCrc() { return 0; }   // no meaning
unsigned CThorCsvReadArg::getProjectedFormatCrc() { return 0; }   // no meaning
void CThorCsvReadArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorCsvReadArg::canMatchAny()                              { return true; }
void CThorCsvReadArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorCsvReadArg::canMatch(const void * row)                 { return true; }
bool CThorCsvReadArg::hasMatchFilter()                           { return false; }
void CThorCsvReadArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

//CThorXmlReadArg

unsigned CThorXmlReadArg::getFlags() { return 0; }
unsigned __int64 CThorXmlReadArg::getChooseNLimit() { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorXmlReadArg::getRowLimit() { return (unsigned __int64) -1; }
void CThorXmlReadArg::onLimitExceeded() { }
unsigned CThorXmlReadArg::getDiskFormatCrc() { return 0; }   // no meaning
unsigned CThorXmlReadArg::getProjectedFormatCrc() { return 0; }   // no meaning
void CThorXmlReadArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorXmlReadArg::canMatchAny()                              { return true; }
void CThorXmlReadArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorXmlReadArg::canMatch(const void * row)                 { return true; }
bool CThorXmlReadArg::hasMatchFilter()                           { return false; }
void CThorXmlReadArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

//CThorNewDiskReadArg

unsigned CThorNewDiskReadArg::getFlags() { return 0; }
void CThorNewDiskReadArg::setCallback(IThorDiskCallback * _tc) { fpp = _tc; }
bool CThorNewDiskReadArg::canMatchAny()                              { return true; }
void CThorNewDiskReadArg::createSegmentMonitors(IIndexReadContext *ctx) {}
bool CThorNewDiskReadArg::canMatch(const void * row)                 { return true; }
bool CThorNewDiskReadArg::hasMatchFilter()                           { return false; }
void CThorNewDiskReadArg::getEncryptKey(size32_t & keyLen, void * & key) { keyLen = 0; key = 0; }

unsigned __int64 CThorNewDiskReadArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorNewDiskReadArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorNewDiskReadArg::onLimitExceeded()                          { }
const char * CThorNewDiskReadArg::queryFormat()                      { return "flat"; }
void CThorNewDiskReadArg::getFormatOptions(IXmlWriter & options)     { }
void CThorNewDiskReadArg::getFormatDynOptions(IXmlWriter & options)  { }

bool CThorNewDiskReadArg::needTransform() { return false; }
bool CThorNewDiskReadArg::transformMayFilter() { return false; }
unsigned __int64 CThorNewDiskReadArg::getKeyedLimit() { return (unsigned __int64) -1; }
void CThorNewDiskReadArg::onKeyedLimitExceeded() { }
ISteppingMeta * CThorNewDiskReadArg::queryRawSteppingMeta() { return NULL; }
ISteppingMeta * CThorNewDiskReadArg::queryProjectedSteppingMeta() { return NULL; }
void CThorNewDiskReadArg::mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) { }
size32_t CThorNewDiskReadArg::transform(ARowBuilder & rowBuilder, const void * src)
{
    rtlFail(800, "transform() should not be called, input is deserialized");
}

size32_t CThorNewDiskReadArg::unfilteredTransform(ARowBuilder & rowBuilder, const void * src) { return 0; }
size32_t CThorNewDiskReadArg::transformOnLimitExceeded(ARowBuilder & rowBuilder) { return 0; }
size32_t CThorNewDiskReadArg::transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) { return 0; }

//CThorChildGroupAggregateArg

size32_t CThorChildGroupAggregateArg::mergeAggregate(ARowBuilder & rowBuilder, const void * src) { rtlFailUnexpected(); return 0; }

//CThorChildThroughNormalizeArg
unsigned __int64 CThorChildThroughNormalizeArg::getChooseNLimit()              { return I64C(0x7fffffffffffffff); }
unsigned __int64 CThorChildThroughNormalizeArg::getRowLimit()                  { return (unsigned __int64) -1; }
void CThorChildThroughNormalizeArg::onLimitExceeded()                          { }
unsigned __int64 CThorChildThroughNormalizeArg::getKeyedLimit()                { return (unsigned __int64) -1; }
void CThorChildThroughNormalizeArg::onKeyedLimitExceeded()                     { }

//CThorLoopArg

unsigned CThorLoopArg::getFlags() { return 0; }
bool CThorLoopArg::sendToLoop(unsigned counter, const void * in) { return true; }
unsigned CThorLoopArg::numIterations() { return 0; }
bool CThorLoopArg::loopAgain(unsigned counter, unsigned num, const void * * _rows) { return num != 0; }
unsigned CThorLoopArg::defaultParallelIterations() { return 0; }
bool CThorLoopArg::loopFirstTime() { return false; }
unsigned CThorLoopArg::loopAgainResult() { return 0; }

//CThorGraphLoopArg

unsigned CThorGraphLoopArg::getFlags() { return 0; }

//CThorRemoteArg

IOutputMetaData * CThorRemoteArg::queryOutputMeta() { return NULL; }        // for action variety
unsigned __int64 CThorRemoteArg::getRowLimit() { return 10000; }
void CThorRemoteArg::onLimitExceeded() { rtlSysFail(1, "Too many records returned from ALLNODES()"); }

//CThorNWayGraphLoopResultReadArg

bool CThorNWayGraphLoopResultReadArg::isGrouped() const { return false; }

//CThorNWayMergeArg

ISortKeySerializer * CThorNWayMergeArg::querySerialize() { return NULL; }        // only if global
ICompare * CThorNWayMergeArg::queryCompareKey() { return NULL; }
ICompare * CThorNWayMergeArg::queryCompareRowKey() { return NULL; }        // row is lhs, key is rhs
bool CThorNWayMergeArg::dedup() { return false; }
ISteppingMeta * CThorNWayMergeArg::querySteppingMeta() { return NULL; }

//CThorNWayMergeJoinArg

unsigned CThorNWayMergeJoinArg::getJoinFlags() { return 0; }
ICompareEq * CThorNWayMergeJoinArg::queryNonSteppedCompare() { return NULL; }
void CThorNWayMergeJoinArg::adjustRangeValue(ARowBuilder & rowBuilder, const void * input, __int64 delta) {}
unsigned __int64 CThorNWayMergeJoinArg::extractRangeValue(const void * input) { return 0; }
__int64 CThorNWayMergeJoinArg::maxRightBeforeLeft() { return 0; }
__int64 CThorNWayMergeJoinArg::maxLeftBeforeRight() { return 0; }
size32_t CThorNWayMergeJoinArg::transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) { return 0; }
bool CThorNWayMergeJoinArg::createNextJoinValue(ARowBuilder & rowBuilder, const void * _value) { return false; }
unsigned CThorNWayMergeJoinArg::getMinMatches() { return 0; }
unsigned CThorNWayMergeJoinArg::getMaxMatches() { return 0x7fffffff; }
INaryCompareEq * CThorNWayMergeJoinArg::queryGlobalCompare() { return NULL; }
size32_t CThorNWayMergeJoinArg::createLowInputRow(ARowBuilder & rowBuilder) { return 0; }
ICompareEq * CThorNWayMergeJoinArg::queryPartitionCompareEq() { return NULL; }

//CThorSectionArg

unsigned CThorSectionArg::getFlags() { return 0; }
void CThorSectionArg::getDescription(size32_t & _retLen, char * & _retData) { _retLen = 0; _retData = NULL; }

//CThorSectionInputArg

unsigned CThorSectionInputArg::getFlags() { return 0; }

//CThorTraceArg

bool CThorTraceArg::isValid(const void * _left) { return true; }
bool CThorTraceArg::canMatchAny() { return true; }
unsigned CThorTraceArg::getKeepLimit() { return (unsigned) -1; }
unsigned CThorTraceArg::getSample() { return 0; }
unsigned CThorTraceArg::getSkip() { return 0; }
const char *CThorTraceArg::getName() { return NULL; }


//CThorExternalArg
CThorExternalArg::CThorExternalArg(unsigned _numInputs)
{
    inputs = new IRowStream *[_numInputs];
    for (unsigned i=0; i < _numInputs; i++)
        inputs[i] = nullptr;
}

CThorExternalArg::~CThorExternalArg() { delete [] inputs; }

IRowStream * CThorExternalArg::createOutput(IThorActivityContext * activityContext) { rtlFailUnexpected(); }
void CThorExternalArg::execute(IThorActivityContext * activityContext) { rtlFailUnexpected(); }
void CThorExternalArg::setInput(unsigned whichInput, IRowStream * input) { inputs[whichInput] = input; }

