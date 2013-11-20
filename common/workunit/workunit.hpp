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

#ifndef WORKUNIT_INCL
#define WORKUNIT_INCL

#ifdef _WIN32
    #ifdef WORKUNIT_EXPORTS
        #define WORKUNIT_API __declspec(dllexport)
    #else
        #define WORKUNIT_API __declspec(dllimport)
    #endif
#else
    #define WORKUNIT_API
#endif

#define MINIMUM_SCHEDULE_PRIORITY 0
#define DEFAULT_SCHEDULE_PRIORITY 50
#define MAXIMUM_SCHEDULE_PRIORITY 100

#include "jiface.hpp"
#include "errorlist.h"
#include "jtime.hpp"
#include "jsocket.hpp"
#include "jstats.h"

#define CHEAP_UCHAR_DEF
#ifdef _WIN32
typedef wchar_t UChar;
#else //__WIN32
typedef unsigned short UChar;
#endif //__WIN32


// error codes
#define QUERRREG_ADD_NAMEDQUERY     QUERYREGISTRY_ERROR_START
#define QUERRREG_REMOVE_NAMEDQUERY  QUERYREGISTRY_ERROR_START+1
#define QUERRREG_WUID               QUERYREGISTRY_ERROR_START+2
#define QUERRREG_DLL                QUERYREGISTRY_ERROR_START+3
#define QUERRREG_SETALIAS           QUERYREGISTRY_ERROR_START+4
#define QUERRREG_RESOLVEALIAS       QUERYREGISTRY_ERROR_START+5
#define QUERRREG_REMOVEALIAS        QUERYREGISTRY_ERROR_START+6
#define QUERRREG_QUERY_REGISTRY     QUERYREGISTRY_ERROR_START+7
#define QUERRREG_SUSPEND            QUERYREGISTRY_ERROR_START+8
#define QUERRREG_UNSUSPEND          QUERYREGISTRY_ERROR_START+9
#define QUERRREG_COMMENT            QUERYREGISTRY_ERROR_START+10

class CDateTime;
interface ISetToXmlTransformer;
interface ISecManager;
interface ISecUser;
class StringArray;
class StringBuffer;

typedef unsigned __int64 __uint64;

interface IScmIterator : extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
};

interface IQueueSwitcher : extends IInterface
{
    virtual void * getQ(const char * qname, const char * wuid) = 0;
    virtual void putQ(const char * qname, const char * wuid, void * qitem) = 0;
    virtual bool isAuto() = 0;
};


//!  PriorityClass
//! Not sure what the real current class values are -- TBD

enum WUPriorityClass
{
    PriorityClassUnknown = 0,
    PriorityClassLow = 1,
    PriorityClassNormal = 2,
    PriorityClassHigh = 3,
    PriorityClassSize = 4
};



enum WUQueryType
{
    QueryTypeUnknown = 0,
    QueryTypeEcl = 1,
    QueryTypeSql = 2,
    QueryTypeXml = 3,
    QueryTypeAttribute = 4,
    QueryTypeSize = 5
};



enum WUState
{
    WUStateUnknown = 0,
    WUStateCompiled = 1,
    WUStateRunning = 2,
    WUStateCompleted = 3,
    WUStateFailed = 4,
    WUStateArchived = 5,
    WUStateAborting = 6,
    WUStateAborted = 7,
    WUStateBlocked = 8,
    WUStateSubmitted = 9,
    WUStateScheduled = 10,
    WUStateCompiling = 11,
    WUStateWait = 12,
    WUStateUploadingFiles = 13,
    WUStateDebugPaused = 14,
    WUStateDebugRunning = 15,
    WUStatePaused = 16,
    WUStateSize = 17
};



enum WUAction
{
    WUActionUnknown = 0,
    WUActionCompile = 1,
    WUActionCheck = 2,
    WUActionRun = 3,
    WUActionExecuteExisting = 4,
    WUActionPause = 5, 
    WUActionPauseNow = 6, 
    WUActionResume = 7, 
    WUActionSize = 8
};



enum WUCompareMode
{
    CompareModeOff = 0,
    CompareModeHole = 1,
    CompareModeThor = 2,
    CompareModeSize = 3
};



enum WUResultStatus
{
    ResultStatusUndefined = 0,
    ResultStatusCalculated = 1,
    ResultStatusSupplied = 2,
    ResultStatusFailed = 3,
    ResultStatusPartial = 4,
    ResultStatusSize = 5
};



enum WUExceptionSeverity
{
    ExceptionSeverityInformation = 0,
    ExceptionSeverityWarning = 1,
    ExceptionSeverityError = 2,
    ExceptionSeveritySize = 3
};



//! IConstWUGraph

enum WUGraphType
{
    GraphTypeAny = 0,
    GraphTypeProgress = 1,
    GraphTypeEcl = 2,
    GraphTypeActivities = 3,
    GraphTypeSubProgress = 4,
    GraphTypeSize = 5
};


interface IConstWUGraphIterator;
interface ICsvToRawTransformer;
interface IXmlToRawTransformer;
interface IPropertyTree;
interface IPropertyTreeIterator;

interface IConstWUGraph : extends IInterface
{
    virtual IStringVal & getXGMML(IStringVal & ret, bool mergeProgress) const = 0;
    virtual IStringVal & getDOT(IStringVal & ret) const = 0;
    virtual IStringVal & getName(IStringVal & ret) const = 0;
    virtual IStringVal & getLabel(IStringVal & ret) const = 0;
    virtual IStringVal & getTypeName(IStringVal & ret) const = 0;
    virtual WUGraphType getType() const = 0;
    virtual IPropertyTree * getXGMMLTree(bool mergeProgress) const = 0;
    virtual IPropertyTree * getXGMMLTreeRaw() const = 0;
    virtual bool isValid() const = 0;
};


interface IWUGraph : extends IConstWUGraph
{
    virtual void setXGMML(const char * text) = 0;
    virtual void setXGMMLTree(IPropertyTree * tree, bool compress=true) = 0;
    virtual void setName(const char * name) = 0;
    virtual void setLabel(const char * name) = 0;
    virtual void setType(WUGraphType type) = 0;
};


interface IConstWUGraphIterator : extends IScmIterator
{
    virtual IConstWUGraph & query() = 0;
};


//! IWUResult
enum
{
    ResultSequenceStored = -1,
    ResultSequencePersist = -2,
    ResultSequenceInternal = -3,
    ResultSequenceOnce = -4,
};

extern WORKUNIT_API bool isSpecialResultSequence(unsigned sequence);

enum WUResultFormat
{
    ResultFormatRaw = 0,
    ResultFormatXml = 1,
    ResultFormatXmlSet = 2,
    ResultFormatCsv = 3,
    ResultFormatSize = 4
};



interface IConstWUResult : extends IInterface
{
    virtual WUResultStatus getResultStatus() const = 0;
    virtual IStringVal & getResultName(IStringVal & str) const = 0;
    virtual int getResultSequence() const = 0;
    virtual bool isResultScalar() const = 0;
    virtual IStringVal & getResultXml(IStringVal & str) const = 0;
    virtual unsigned getResultFetchSize() const = 0;
    virtual __int64 getResultTotalRowCount() const = 0;
    virtual __int64 getResultRowCount() const = 0;
    virtual void getResultDataset(IStringVal & ecl, IStringVal & defs) const = 0;
    virtual IStringVal & getResultLogicalName(IStringVal & ecl) const = 0;
    virtual IStringVal & getResultKeyField(IStringVal & ecl) const = 0;
    virtual unsigned getResultRequestedRows() const = 0;
    virtual __int64 getResultInt() const = 0;
    virtual bool getResultBool() const = 0;
    virtual double getResultReal() const = 0;
    virtual IStringVal & getResultString(IStringVal & str) const = 0;
    virtual IDataVal & getResultRaw(IDataVal & data, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const = 0;
    virtual IDataVal & getResultUnicode(IDataVal & data) const = 0;
    virtual IStringVal & getResultEclSchema(IStringVal & str) const = 0;
    virtual __int64 getResultRawSize(IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const = 0;
    virtual IDataVal & getResultRaw(IDataVal & data, __int64 from, __int64 length, IXmlToRawTransformer * xmlTransformer, ICsvToRawTransformer * csvTransformer) const = 0;
    virtual IStringVal & getResultRecordSizeEntry(IStringVal & str) const = 0;
    virtual IStringVal & getResultTransformerEntry(IStringVal & str) const = 0;
    virtual __int64 getResultRowLimit() const = 0;
    virtual IStringVal & getResultFilename(IStringVal & str) const = 0;
    virtual WUResultFormat getResultFormat() const = 0;
    virtual unsigned getResultHash() const = 0;
    virtual void getResultDecimal(void * val, unsigned length, unsigned precision, bool isSigned) const = 0;
    virtual bool getResultIsAll() const = 0;
};


interface IWUResult : extends IConstWUResult
{
    virtual void setResultStatus(WUResultStatus status) = 0;
    virtual void setResultName(const char * name) = 0;
    virtual void setResultSequence(unsigned seq) = 0;
    virtual void setResultSchemaRaw(unsigned len, const void * schema) = 0;
    virtual void setResultScalar(bool isScalar) = 0;
    virtual void setResultRaw(unsigned len, const void * data, WUResultFormat format) = 0;
    virtual void setResultFetchSize(unsigned rows) = 0;
    virtual void setResultTotalRowCount(__int64 rows) = 0;
    virtual void setResultRowCount(__int64 rows) = 0;
    virtual void setResultDataset(const char * ecl, const char * defs) = 0;
    virtual void setResultLogicalName(const char * logicalName) = 0;
    virtual void setResultKeyField(const char * name) = 0;
    virtual void setResultRequestedRows(unsigned rowcount) = 0;
    virtual void setResultInt(__int64 val) = 0;
    virtual void setResultBool(bool val) = 0;
    virtual void setResultReal(double val) = 0;
    virtual void setResultString(const char * val, unsigned length) = 0;
    virtual void setResultData(const void * val, unsigned length) = 0;
    virtual void setResultDecimal(const void * val, unsigned length) = 0;
    virtual void addResultRaw(unsigned len, const void * data, WUResultFormat format) = 0;
    virtual void setResultRecordSizeEntry(const char * val) = 0;
    virtual void setResultTransformerEntry(const char * val) = 0;
    virtual void setResultRowLimit(__int64 value) = 0;
    virtual void setResultFilename(const char * name) = 0;
    virtual void setResultUnicode(const void * val, unsigned length) = 0;
    virtual void setResultUInt(__uint64 val) = 0;
    virtual void setResultIsAll(bool value) = 0;
    virtual void setResultFormat(WUResultFormat format) = 0;
    virtual void setResultXML(const char * xml) = 0;
    virtual void setResultRow(unsigned len, const void * data) = 0;
};


interface IConstWUResultIterator : extends IScmIterator
{
    virtual IConstWUResult & query() = 0;
};


//! IWUQuery

enum WUFileType
{
    FileTypeCpp = 0,
    FileTypeDll = 1,
    FileTypeResText = 2,
    FileTypeHintXml = 3,
    FileTypeXml = 4,
    FileTypeSize = 5
};




interface IConstWUAssociatedFile : extends IInterface
{
    virtual WUFileType getType() const = 0;
    virtual IStringVal & getDescription(IStringVal & ret) const = 0;
    virtual IStringVal & getIp(IStringVal & ret) const = 0;
    virtual IStringVal & getName(IStringVal & ret) const = 0;
    virtual IStringVal & getNameTail(IStringVal & ret) const = 0;
    virtual unsigned getCrc() const = 0;
};



interface IConstWUAssociatedFileIterator : extends IScmIterator
{
    virtual IConstWUAssociatedFile & query() = 0;
};




interface IConstWUQuery : extends IInterface
{
    virtual WUQueryType getQueryType() const = 0;
    virtual IStringVal & getQueryText(IStringVal & str) const = 0;
    virtual IStringVal & getQueryName(IStringVal & str) const = 0;
    virtual IStringVal & getQueryDllName(IStringVal & str) const = 0;
    virtual unsigned getQueryDllCrc() const = 0;
    virtual IStringVal & getQueryCppName(IStringVal & str) const = 0;
    virtual IStringVal & getQueryResTxtName(IStringVal & str) const = 0;
    virtual IConstWUAssociatedFile * getAssociatedFile(WUFileType type, unsigned index) const = 0;
    virtual IConstWUAssociatedFileIterator & getAssociatedFiles() const = 0;
    virtual IStringVal & getQueryShortText(IStringVal & str) const = 0;
    virtual IStringVal & getQueryMainDefinition(IStringVal & str) const = 0;
};


interface IWUQuery : extends IConstWUQuery
{
    virtual void setQueryType(WUQueryType qt) = 0;
    virtual void setQueryText(const char * pstr) = 0;
    virtual void setQueryName(const char * pstr) = 0;
    virtual void addAssociatedFile(WUFileType type, const char * name, const char * ip, const char * desc, unsigned crc) = 0;
    virtual void removeAssociatedFiles() = 0;
    virtual void setQueryMainDefinition(const char * str) = 0;
};


interface IConstWUWebServicesInfo : extends IInterface
{
    virtual IStringVal & getModuleName(IStringVal & str) const = 0;
    virtual IStringVal & getAttributeName(IStringVal & str) const = 0;
    virtual IStringVal & getDefaultName(IStringVal & str) const = 0;
    virtual IStringVal & getInfo(const char * name, IStringVal & str) const = 0;
    virtual unsigned getWebServicesCRC() const = 0;
};


interface IWUWebServicesInfo : extends IConstWUWebServicesInfo
{
    virtual void setModuleName(const char * pstr) = 0;
    virtual void setAttributeName(const char * pstr) = 0;
    virtual void setDefaultName(const char * pstr) = 0;
    virtual void setInfo(const char * name, const char * info) = 0;
    virtual void setWebServicesCRC(unsigned crc) = 0;
};


interface IConstWURoxieQueryInfo : extends IInterface
{
    virtual IStringVal & getQueryInfo(IStringVal & str) const = 0;
    virtual IStringVal & getDefaultPackageInfo(IStringVal & str) const = 0;
    virtual IStringVal & getRoxieClusterName(IStringVal & str) const = 0;
    virtual IStringVal & getWuid(IStringVal & str) const = 0;
};


interface IWURoxieQueryInfo : extends IConstWURoxieQueryInfo
{
    virtual void setQueryInfo(const char * info) = 0;
    virtual void setDefaultPackageInfo(const char * pstr, int len) = 0;
    virtual void setRoxieClusterName(const char * name) = 0;
    virtual void setWuid(const char * wuid) = 0;
};



//! IWUPlugin

interface IConstWUPlugin : extends IInterface
{
    virtual IStringVal & getPluginName(IStringVal & str) const = 0;
    virtual IStringVal & getPluginVersion(IStringVal & str) const = 0;
    virtual bool getPluginThor() const = 0;
    virtual bool getPluginHole() const = 0;
};


interface IWUPlugin : extends IConstWUPlugin
{
    virtual void setPluginName(const char * str) = 0;
    virtual void setPluginVersion(const char * str) = 0;
    virtual void setPluginThor(bool flag) = 0;
    virtual void setPluginHole(bool flag) = 0;
};


interface IConstWUPluginIterator : extends IScmIterator
{
    virtual IConstWUPlugin & query() = 0;
};


interface IConstWULibraryActivityIterator : extends IScmIterator
{
    virtual unsigned query() const = 0;
};


interface IConstWULibrary : extends IInterface
{
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IConstWULibraryActivityIterator * getActivities() const = 0;
};


interface IWULibrary : extends IConstWULibrary
{
    virtual void setName(const char * str) = 0;
    virtual void addActivity(unsigned id) = 0;
};


interface IConstWULibraryIterator : extends IScmIterator
{
    virtual IConstWULibrary & query() = 0;
};


//! IWUActivity

interface IConstWUActivity : extends IInterface
{
    virtual __int64 getId() const = 0;
    virtual unsigned getKind() const = 0;
    virtual IStringVal & getHelper(IStringVal & ret) const = 0;
};


interface IWUActivity : extends IConstWUActivity
{
    virtual void setKind(unsigned id) = 0;
    virtual void setHelper(const char * str) = 0;
};


interface IConstWUActivityIterator : extends IScmIterator
{
    virtual IConstWUActivity & query() = 0;
};


//! IWUException

interface IConstWUException : extends IInterface
{
    virtual IStringVal & getExceptionSource(IStringVal & str) const = 0;
    virtual IStringVal & getExceptionMessage(IStringVal & str) const = 0;
    virtual unsigned getExceptionCode() const = 0;
    virtual WUExceptionSeverity getSeverity() const = 0;
    virtual IStringVal & getTimeStamp(IStringVal & dt) const = 0;
    virtual IStringVal & getExceptionFileName(IStringVal & str) const = 0;
    virtual unsigned getExceptionLineNo() const = 0;
    virtual unsigned getExceptionColumn() const = 0;
};


interface IWUException : extends IConstWUException
{
    virtual void setExceptionSource(const char * str) = 0;
    virtual void setExceptionMessage(const char * str) = 0;
    virtual void setExceptionCode(unsigned code) = 0;
    virtual void setSeverity(WUExceptionSeverity level) = 0;
    virtual void setTimeStamp(const char * dt) = 0;
    virtual void setExceptionFileName(const char * str) = 0;
    virtual void setExceptionLineNo(unsigned r) = 0;
    virtual void setExceptionColumn(unsigned c) = 0;
};


interface IConstWUExceptionIterator : extends IScmIterator
{
    virtual IConstWUException & query() = 0;
};

enum ClusterType { NoCluster, HThorCluster, RoxieCluster, ThorLCRCluster };

extern WORKUNIT_API ClusterType getClusterType(const char * platform, ClusterType dft = NoCluster);
extern WORKUNIT_API const char *clusterTypeString(ClusterType clusterType, bool lcrSensitive);
inline bool isThorCluster(ClusterType type) { return (type == ThorLCRCluster); }

//! IClusterInfo
interface IConstWUClusterInfo : extends IInterface
{
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IStringVal & getScope(IStringVal & str) const = 0;
    virtual IStringVal & getThorQueue(IStringVal & str) const = 0;
    virtual unsigned getSize() const = 0;
    virtual ClusterType getPlatform() const = 0;
    virtual IStringVal & getAgentQueue(IStringVal & str) const = 0;
    virtual IStringVal & getServerQueue(IStringVal & str) const = 0;
    virtual IStringVal & getRoxieProcess(IStringVal & str) const = 0;
    virtual const StringArray & getThorProcesses() const = 0;
    virtual const SocketEndpointArray & getRoxieServers() const = 0;
};

//! IWorkflowItem
enum WFType
{
    WFTypeNormal = 0,
    WFTypeSuccess = 1,
    WFTypeFailure = 2,
    WFTypeRecovery = 3,
    WFTypeWait = 4,
    WFTypeSize = 5
};

enum WFMode
{
    WFModeNormal = 0,
    WFModeCondition = 1,
    WFModeSequential = 2,
    WFModeParallel = 3,
    WFModePersist = 4,
    WFModeBeginWait = 5,
    WFModeWait = 6,
    WFModeOnce = 7,
    WFModeSize = 8
};

enum WFState
{
    WFStateNull = 0,
    WFStateReqd = 1,
    WFStateDone = 2,
    WFStateFail = 3,
    WFStateSkip = 4,
    WFStateWait = 5,
    WFStateBlocked = 6,
    WFStateSize = 7
};



interface IWorkflowDependencyIterator : extends IScmIterator
{
    virtual unsigned query() const = 0;
};


interface IWorkflowEvent : extends IInterface
{
    virtual const char * queryName() const = 0;
    virtual const char * queryText() const = 0;
    virtual bool matches(const char * name, const char * text) const = 0;
};


interface IConstWorkflowItem : extends IInterface
{
    virtual unsigned queryWfid() const = 0;
    virtual bool isScheduled() const = 0;
    virtual bool isScheduledNow() const = 0;
    virtual IWorkflowEvent * getScheduleEvent() const = 0;
    virtual unsigned querySchedulePriority() const = 0;
    virtual bool hasScheduleCount() const = 0;
    virtual unsigned queryScheduleCount() const = 0;
    virtual IWorkflowDependencyIterator * getDependencies() const = 0;
    virtual WFType queryType() const = 0;
    virtual WFMode queryMode() const = 0;
    virtual unsigned querySuccess() const = 0;
    virtual unsigned queryFailure() const = 0;
    virtual unsigned queryRecovery() const = 0;
    virtual unsigned queryRetriesAllowed() const = 0;
    virtual unsigned queryContingencyFor() const = 0;
    virtual IStringVal & getPersistName(IStringVal & val) const = 0;
    virtual unsigned queryPersistWfid() const = 0;
    virtual int queryPersistCopies() const = 0;  // 0 - unmangled name,  < 0 - use default, > 0 - max number
    virtual unsigned queryScheduleCountRemaining() const = 0;
    virtual WFState queryState() const = 0;
    virtual unsigned queryRetriesRemaining() const = 0;
    virtual int queryFailCode() const = 0;
    virtual const char * queryFailMessage() const = 0;
    virtual const char * queryEventName() const = 0;
    virtual const char * queryEventExtra() const = 0;
    virtual unsigned queryScheduledWfid() const = 0;
    virtual IStringVal & queryCluster(IStringVal & val) const = 0;
};


interface IRuntimeWorkflowItem : extends IConstWorkflowItem
{
    virtual void setState(WFState state) = 0;
    virtual bool testAndDecRetries() = 0;
    virtual bool decAndTestScheduleCountRemaining() = 0;
    virtual void setFailInfo(int code, const char * message) = 0;
    virtual void reset() = 0;
    virtual void setEvent(const char * name, const char * extra) = 0;
    virtual void incScheduleCount() = 0;
};


interface IWorkflowItem : extends IRuntimeWorkflowItem
{
    virtual void setScheduledNow() = 0;
    virtual void setScheduledOn(const char * name, const char * text) = 0;
    virtual void setSchedulePriority(unsigned priority) = 0;
    virtual void setScheduleCount(unsigned count) = 0;
    virtual void addDependency(unsigned wfid) = 0;
    virtual void setPersistInfo(const char * name, unsigned wfid, int maxCopies) = 0;
    virtual void syncRuntimeData(const IConstWorkflowItem & other) = 0;
    virtual void setScheduledWfid(unsigned wfid) = 0;
    virtual void setCluster(const char * cluster) = 0;
};


interface IConstWorkflowItemIterator : extends IScmIterator
{
    virtual IConstWorkflowItem * query() const = 0;
};


interface IRuntimeWorkflowItemIterator : extends IConstWorkflowItemIterator
{
    virtual IRuntimeWorkflowItem * get() const = 0;
};


interface IWorkflowItemIterator : extends IConstWorkflowItemIterator
{
    virtual IWorkflowItem * get() const = 0;
};


interface IWorkflowItemArray : extends IInterface
{
    virtual IRuntimeWorkflowItem & queryWfid(unsigned wfid) = 0;
    virtual unsigned count() const = 0;
    virtual IRuntimeWorkflowItemIterator * getSequenceIterator() = 0;
    virtual void addClone(const IConstWorkflowItem * other) = 0;
    virtual bool hasScheduling() const = 0;
};


enum LocalFileUploadType
{
    UploadTypeFileSpray = 0,
    UploadTypeWUResult = 1,
    UploadTypeWUResultCsv = 2,
    UploadTypeWUResultXml = 3,
    UploadTypeSize = 4
};



interface IConstLocalFileUpload : extends IInterface
{
    virtual unsigned queryID() const = 0;
    virtual LocalFileUploadType queryType() const = 0;
    virtual IStringVal & getSource(IStringVal & ret) const = 0;
    virtual IStringVal & getDestination(IStringVal & ret) const = 0;
    virtual IStringVal & getEventTag(IStringVal & ret) const = 0;
};


interface IConstLocalFileUploadIterator : extends IScmIterator
{
    virtual IConstLocalFileUpload * get() = 0;
};


enum WUSubscribeOptions
{
    SubscribeOptionRunningState = 0,
    SubscribeOptionAnyState = 1,
    SubscribeOptionAbort = 2,
    SubscribeOptionProgress = 3,
    SubscribeOptionAll = 4,
    SubscribeOptionSize = 5
};



interface IWUGraphProgress;
interface IPropertyTree;
enum WUGraphState
{
    WUGraphUnknown = 0,
    WUGraphComplete = 1,
    WUGraphRunning = 2,
    WUGraphFailed = 3,
    WUGraphPaused = 4
};



enum WUFileKind
{
    WUFileStandard = 0,
    WUFileTemporary = 1,
    WUFileOwned = 2,
    WUFileJobOwned = 3
};



typedef unsigned __int64 WUGraphIDType;
typedef unsigned __int64 WUNodeIDType;
interface IConstWUGraphProgress : extends IInterface
{
    virtual IPropertyTree * queryProgressTree() = 0;
    virtual WUGraphState queryGraphState() = 0;
    virtual WUGraphState queryNodeState(WUGraphIDType nodeId) = 0;
    virtual IWUGraphProgress * update() = 0;
    virtual unsigned queryFormatVersion() = 0;
};


interface IWUGraphProgress : extends IConstWUGraphProgress
{
    virtual IPropertyTree & updateEdge(WUGraphIDType nodeId, const char * edgeId) = 0;
    virtual IPropertyTree & updateNode(WUGraphIDType nodeId, WUNodeIDType id) = 0;
    virtual void setGraphState(WUGraphState state) = 0;
    virtual void setNodeState(WUGraphIDType nodeId, WUGraphState state) = 0;
};


interface IConstWUTimeStamp : extends IInterface
{
    virtual IStringVal & getApplication(IStringVal & str) const = 0;
    virtual IStringVal & getEvent(IStringVal & str) const = 0;
    virtual IStringVal & getDate(IStringVal & dt) const = 0;
};


interface IConstWUTimeStampIterator : extends IScmIterator
{
    virtual IConstWUTimeStamp & query() = 0;
};


interface IConstWUAppValue : extends IInterface
{
    virtual IStringVal & getApplication(IStringVal & str) const = 0;
    virtual IStringVal & getName(IStringVal & str) const = 0;
    virtual IStringVal & getValue(IStringVal & str) const = 0;
};


interface IConstWUAppValueIterator : extends IScmIterator
{
    virtual IConstWUAppValue & query() = 0;
};

interface IConstWUStatistic : extends IInterface
{
    virtual IStringVal & getFullName(IStringVal & str) const = 0;   // A unique name
    virtual IStringVal & getCreator(IStringVal & str) const = 0;    // what component gathered the statistic e.g., roxie/eclcc/thorslave[ip]
    virtual IStringVal & getDescription(IStringVal & str) const = 0;// Description suitable for displaying to the user
    virtual IStringVal & getName(IStringVal & str) const = 0;       // what is the name of the statistic e.g., wall time
    virtual IStringVal & getScope(IStringVal & str) const = 0;      // what scope is the statistic gathered over? e.g., workunit, wfid:n, graphn, graphn:m
    virtual StatisticMeasure getKind() const = 0;
    virtual unsigned __int64 getValue() const = 0;
    virtual unsigned __int64 getCount() const = 0;
    virtual unsigned __int64 getMax() const = 0;
};

interface IConstWUStatisticIterator : extends IScmIterator
{
    virtual IConstWUStatistic & query() = 0;
};

//! IWorkUnit
//! Provides high level access to WorkUnit "header" data.
interface IWorkUnit;
interface IUserDescriptor;

interface IStringIterator : extends IScmIterator
{
    virtual IStringVal & str(IStringVal & str) = 0;
};

interface IConstWorkUnit : extends IInterface
{
    virtual bool aborting() const = 0;
    virtual void forceReload() = 0;
    virtual WUAction getAction() const = 0;
    virtual IStringVal& getActionEx(IStringVal & str) const = 0;
    virtual IStringVal & getApplicationValue(const char * application, const char * propname, IStringVal & str) const = 0;
    virtual int getApplicationValueInt(const char * application, const char * propname, int defVal) const = 0;
    virtual IConstWUAppValueIterator & getApplicationValues() const = 0;
    virtual bool hasWorkflow() const = 0;
    virtual unsigned queryEventScheduledCount() const = 0;
    virtual IPropertyTree * queryWorkflowTree() const = 0;
    virtual IConstWorkflowItemIterator * getWorkflowItems() const = 0;
    virtual IWorkflowItemArray * getWorkflowClone() const = 0;
    virtual IConstLocalFileUploadIterator * getLocalFileUploads() const = 0;
    virtual bool requiresLocalFileUpload() const = 0;
    virtual bool getIsQueryService() const = 0;
    virtual IStringVal & getClusterName(IStringVal & str) const = 0;
    virtual unsigned getCombineQueries() const = 0;
    virtual WUCompareMode getCompareMode() const = 0;
    virtual IStringVal & getCustomerId(IStringVal & str) const = 0;
    virtual bool hasDebugValue(const char * propname) const = 0;
    virtual IStringVal & getDebugValue(const char * propname, IStringVal & str) const = 0;
    virtual int getDebugValueInt(const char * propname, int defVal) const = 0;
    virtual __int64 getDebugValueInt64(const char * propname, __int64 defVal) const = 0;
    virtual bool getDebugValueBool(const char * propname, bool defVal) const = 0;
    virtual IStringIterator & getDebugValues() const = 0;
    virtual IStringIterator & getDebugValues(const char * prop) const = 0;
    virtual unsigned getExceptionCount() const = 0;
    virtual IConstWUExceptionIterator & getExceptions() const = 0;
    virtual IConstWUResult * getGlobalByName(const char * name) const = 0;
    virtual IConstWUGraphIterator & getGraphs(WUGraphType type) const = 0;
    virtual IConstWUGraph * getGraph(const char * name) const = 0;
    virtual IConstWUGraphProgress * getGraphProgress(const char * name) const = 0;
    virtual IStringVal & getJobName(IStringVal & str) const = 0;
    virtual IStringVal & getParentWuid(IStringVal & str) const = 0;
    virtual IConstWUPlugin * getPluginByName(const char * name) const = 0;
    virtual IConstWUPluginIterator & getPlugins() const = 0;
    virtual IConstWULibraryIterator & getLibraries() const = 0;
    virtual WUPriorityClass getPriority() const = 0;
    virtual int getPriorityLevel() const = 0;
    virtual IConstWUQuery * getQuery() const = 0;
    virtual bool getRescheduleFlag() const = 0;
    virtual IConstWUResult * getResultByName(const char * name) const = 0;
    virtual IConstWUResult * getResultBySequence(unsigned seq) const = 0;
    virtual unsigned getResultLimit() const = 0;
    virtual IConstWUResultIterator & getResults() const = 0;
    virtual IConstWUActivityIterator & getActivities() const = 0;
    virtual IConstWUActivity * getActivity(__int64 id) const = 0;
    virtual IStringVal & getScope(IStringVal & str) const = 0;
    virtual IStringVal & getSecurityToken(IStringVal & str) const = 0;
    virtual WUState getState() const = 0;
    virtual IStringVal & getStateEx(IStringVal & str) const = 0;
    virtual __int64 getAgentSession() const = 0;
    virtual unsigned getAgentPID() const = 0;
    virtual IStringVal & getStateDesc(IStringVal & str) const = 0;
    virtual IConstWUResult * getTemporaryByName(const char * name) const = 0;
    virtual IConstWUResultIterator & getTemporaries() const = 0;
    virtual bool getRunningGraph(IStringVal & graphName, WUGraphIDType & subId) const = 0;
    virtual unsigned getTimerCount(const char * timerName) const = 0;
    virtual unsigned getTimerDuration(const char * timerName) const = 0;
    virtual IStringVal & getTimerDescription(const char * timerName, IStringVal & str) const = 0;
    virtual IStringVal & getTimeStamp(const char * name, const char * instance, IStringVal & str) const = 0;
    virtual IConstWUWebServicesInfo * getWebServicesInfo() const = 0;
    virtual IConstWURoxieQueryInfo * getRoxieQueryInfo() const = 0;
    virtual IStringIterator & getTimers() const = 0;
    virtual IConstWUTimeStampIterator & getTimeStamps() const = 0;
    virtual IConstWUStatisticIterator & getStatistics() const = 0;
    virtual IConstWUStatistic * getStatistic(const char * name) const = 0;
    virtual IStringVal & getUser(IStringVal & str) const = 0;
    virtual IStringVal & getWuScope(IStringVal & str) const = 0;
    virtual IConstWUResult * getVariableByName(const char * name) const = 0;
    virtual IConstWUResultIterator & getVariables() const = 0;
    virtual IStringVal & getWuid(IStringVal & str) const = 0;
    virtual bool isProtected() const = 0;
    virtual bool isPausing() const = 0;
    virtual IWorkUnit & lock() = 0;
    virtual bool reload() = 0;
    virtual void requestAbort() = 0;
    virtual void subscribe(WUSubscribeOptions options) = 0;
    virtual unsigned queryFileUsage(const char * filename) const = 0;
    virtual unsigned getCodeVersion() const = 0;
    virtual unsigned getWuidVersion() const  = 0;
    virtual void getBuildVersion(IStringVal & buildVersion, IStringVal & eclVersion) const = 0;
    virtual bool isBilled() const = 0;
    virtual bool getWuDate(unsigned & year, unsigned & month, unsigned & day) = 0;
    virtual IPropertyTree * getDiskUsageStats() = 0;
    virtual IPropertyTreeIterator & getFileIterator() const = 0;
    virtual bool getCloneable() const = 0;
    virtual IUserDescriptor * queryUserDescriptor() const = 0;
    virtual IStringVal & getSnapshot(IStringVal & str) const = 0;
    virtual IJlibDateTime & getTimeScheduled(IJlibDateTime & val) const = 0;
    virtual IPropertyTreeIterator & getFilesReadIterator() const = 0;
    virtual void protect(bool protectMode) = 0;
    virtual IStringVal & getAllowedClusters(IStringVal & str) const = 0;
    virtual int getPriorityValue() const = 0;
    virtual void remoteCheckAccess(IUserDescriptor * user, bool writeaccess) const = 0;
    virtual bool getAllowAutoQueueSwitch() const = 0;
    virtual IConstWULibrary * getLibraryByName(const char * name) const = 0;
    virtual unsigned getGraphCount() const = 0;
    virtual unsigned getSourceFileCount() const = 0;
    virtual unsigned getResultCount() const = 0;
    virtual unsigned getVariableCount() const = 0;
    virtual unsigned getTimerCount() const = 0;
    virtual unsigned getApplicationValueCount() const = 0;
    virtual unsigned getDebugAgentListenerPort() const = 0;
    virtual IStringVal & getDebugAgentListenerIP(IStringVal & ip) const = 0;
    virtual IStringVal & getXmlParams(IStringVal & params) const = 0;
    virtual const IPropertyTree * getXmlParams() const = 0;
    virtual unsigned __int64 getHash() const = 0;
    virtual IStringIterator *getLogs(const char *type, const char *instance=NULL) const = 0;
    virtual IStringIterator *getProcesses(const char *type) const = 0;
    virtual IPropertyTreeIterator& getProcesses(const char *type, const char *instance) const = 0;
};


interface IDistributedFile;

interface IWorkUnit : extends IConstWorkUnit
{
    virtual void clearExceptions() = 0;
    virtual void commit() = 0;
    virtual IWUException * createException() = 0;
    virtual void setTimeStamp(const char * name, const char * instance, const char * event) = 0;
    virtual void addTimeStamp(const char * name, const char * instance, const char * event) = 0;
    virtual void addProcess(const char *type, const char *instance, unsigned pid, const char *log=NULL) = 0;
    virtual void setAction(WUAction action) = 0;
    virtual void setApplicationValue(const char * application, const char * propname, const char * value, bool overwrite) = 0;
    virtual void setApplicationValueInt(const char * application, const char * propname, int value, bool overwrite) = 0;
    virtual void incEventScheduledCount() = 0;
    virtual void setIsQueryService(bool cached) = 0;
    virtual void setClusterName(const char * value) = 0;
    virtual void setCombineQueries(unsigned combine) = 0;
    virtual void setCompareMode(WUCompareMode value) = 0;
    virtual void setCustomerId(const char * value) = 0;
    virtual void setDebugValue(const char * propname, const char * value, bool overwrite) = 0;
    virtual void setDebugValueInt(const char * propname, int value, bool overwrite) = 0;
    virtual void setJobName(const char * value) = 0;
    virtual void setPriority(WUPriorityClass cls) = 0;
    virtual void setPriorityLevel(int level) = 0;
    virtual void setRescheduleFlag(bool value) = 0;
    virtual void setResultLimit(unsigned value) = 0;
    virtual void setSecurityToken(const char * value) = 0;
    virtual void setState(WUState state) = 0;
    virtual void setStateEx(const char * text) = 0;
    virtual void setAgentSession(__int64 sessionId) = 0;
    virtual void setTimerInfo(const char * name, unsigned ms, unsigned count, unsigned __int64 max) = 0;
    virtual void setStatistic(const char * creator_who, const char * wuScope_where, const char * stat_what, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge) = 0;
    virtual void setTracingValue(const char * propname, const char * value) = 0;
    virtual void setTracingValueInt(const char * propname, int value) = 0;
    virtual void setUser(const char * value) = 0;
    virtual void setWuScope(const char * value) = 0;
    virtual void setSnapshot(const char * value) = 0;
    virtual IWorkflowItemIterator * updateWorkflowItems() = 0;
    virtual void syncRuntimeWorkflow(IWorkflowItemArray * array) = 0;
    virtual IWorkflowItem * addWorkflowItem(unsigned wfid, WFType type, WFMode mode, unsigned success, unsigned failure, unsigned recovery, unsigned retriesAllowed, unsigned contingencyFor) = 0;
    virtual void resetWorkflow() = 0;
    virtual void schedule() = 0;
    virtual void deschedule() = 0;
    virtual unsigned addLocalFileUpload(LocalFileUploadType type, const char * source, const char * destination, const char * eventTag) = 0;
    virtual IWUResult * updateGlobalByName(const char * name) = 0;
    virtual IWUGraph * updateGraph(const char * name) = 0;
    virtual IWUQuery * updateQuery() = 0;
    virtual IWUWebServicesInfo * updateWebServicesInfo(bool create) = 0;
    virtual IWURoxieQueryInfo * updateRoxieQueryInfo(const char * wuid, const char * roxieClusterName) = 0;
    virtual IWUActivity * updateActivity(__int64 id) = 0;
    virtual IWUPlugin * updatePluginByName(const char * name) = 0;
    virtual IWULibrary * updateLibraryByName(const char * name) = 0;
    virtual IWUResult * updateResultByName(const char * name) = 0;
    virtual IWUResult * updateResultBySequence(unsigned seq) = 0;
    virtual IWUResult * updateTemporaryByName(const char * name) = 0;
    virtual IWUResult * updateVariableByName(const char * name) = 0;
    virtual void addFile(const char * fileName, StringArray * clusters, unsigned usageCount, WUFileKind fileKind, const char * graphOwner) = 0;
    virtual void releaseFile(const char * fileName) = 0;
    virtual void setCodeVersion(unsigned version, const char * buildVersion, const char * eclVersion) = 0;
    virtual void setBilled(bool value) = 0;
    virtual void deleteTempFiles(const char * graph, bool deleteOwned, bool deleteJobOwned) = 0;
    virtual void deleteTemporaries() = 0;
    virtual void addDiskUsageStats(__int64 avgNodeUsage, unsigned minNode, __int64 minNodeUsage, unsigned maxNode, __int64 maxNodeUsage, __int64 graphId) = 0;
    virtual void setCloneable(bool value) = 0;
    virtual void setIsClone(bool value) = 0;
    virtual void setTimeScheduled(const IJlibDateTime & val) = 0;
    virtual void noteFileRead(IDistributedFile * file) = 0;
    virtual void clearGraphProgress() = 0;
    virtual void resetBeforeGeneration() = 0;
    virtual bool switchThorQueue(const char * newcluster, IQueueSwitcher * qs) = 0;
    virtual void setAllowedClusters(const char * value) = 0;
    virtual void setAllowAutoQueueSwitch(bool val) = 0;
    virtual void setLibraryInformation(const char * name, unsigned interfaceHash, unsigned definitionHash) = 0;
    virtual void setDebugAgentListenerPort(unsigned port) = 0;
    virtual void setDebugAgentListenerIP(const char * ip) = 0;
    virtual void setXmlParams(const char *xml) = 0;
    virtual void setXmlParams(IPropertyTree *tree) = 0;
    virtual void setHash(unsigned __int64 hash) = 0;

    virtual void setResultInt(const char * name, unsigned sequence, __int64 val) = 0;
    virtual void setResultUInt(const char * name, unsigned sequence, unsigned __int64 val) = 0;
    virtual void setResultReal(const char *name, unsigned sequence, double val) = 0;
    virtual void setResultVarString(const char * stepname, unsigned sequence, const char *val) = 0;
    virtual void setResultVarUnicode(const char * stepname, unsigned sequence, UChar const *val) = 0;
    virtual void setResultString(const char * stepname, unsigned sequence, int len, const char *val) = 0;
    virtual void setResultData(const char * stepname, unsigned sequence, int len, const void *val) = 0;
//  virtual void doSetResultString(type_t type, const char *name, unsigned sequence, int len, const char *val) = 0;
    virtual void setResultRaw(const char * name, unsigned sequence, int len, const void *val) = 0;
    virtual void setResultSet(const char * name, unsigned sequence, bool isAll, size32_t len, const void *val, ISetToXmlTransformer *) = 0;
    virtual void setResultUnicode(const char * name, unsigned sequence, int len, UChar const * val) = 0;
    virtual void setResultBool(const char *name, unsigned sequence, bool val) = 0;
    virtual void setResultDecimal(const char *name, unsigned sequence, int len, int precision, bool isSigned, const void *val) = 0;
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend) = 0;
};


interface IConstWorkUnitIterator : extends IScmIterator
{
    virtual IConstWorkUnit & query() = 0;
};

//! IWUTimers

interface IWUTimers : extends IInterface
{
    virtual void setTrigger(const IJlibDateTime & dt) = 0;
    virtual IJlibDateTime & getTrigger(IJlibDateTime & dt) const = 0;
    virtual void setExpiration(const IJlibDateTime & dt) = 0;
    virtual IJlibDateTime & getExpiration(IJlibDateTime & dt) const = 0;
    virtual void setSubmission(const IJlibDateTime & dt) = 0;
    virtual IJlibDateTime & getSubmission(IJlibDateTime & dt) const = 0;
};



//! IWUFactory
//! Used to instantiate WorkUnit components.

class MemoryBuffer; // should define an SCMinterface for it

interface ILocalWorkUnit : extends IWorkUnit
{
    virtual void serialize(MemoryBuffer & tgt) = 0;
    virtual void deserialize(MemoryBuffer & src) = 0;
    virtual void loadXML(const char * xml) = 0;
    virtual IConstWorkUnit * unlock() = 0;
};


enum WUSortField
{
    WUSFuser = 1,
    WUSFcluster = 2,
    WUSFjob = 3,
    WUSFstate = 4,
    WUSFpriority = 5,
    WUSFwuid = 6,
    WUSFwuidhigh = 7,
    WUSFfileread = 8,
    WUSFroxiecluster = 9,
    WUSFprotected = 10,
    WUSFbatchloginid = 11,
    WUSFbatchcustomername = 12,
    WUSFbatchpriority = 13,
    WUSFbatchinputreccount = 14,
    WUSFbatchtimeuploaded = 15,
    WUSFbatchtimecompleted = 16,
    WUSFbatchmachine = 17,
    WUSFbatchinputfile = 18,
    WUSFbatchoutputfile = 19,
    WUSFtotalthortime = 20,
    WUSFwildwuid = 21,
    WUSFterm = 0,
    WUSFreverse = 256,
    WUSFnocase = 512,
    WUSFnumeric = 1024,
    WUSFwild = 2048
};

enum WUQuerySortField
{
    WUQSFId = 1,
    WUQSFname = 2,
    WUQSFwuid = 3,
    WUQSFdll = 4,
    WUQSFmemoryLimit = 5,
    WUQSFmemoryLimitHi = 6,
    WUQSFtimeLimit = 7,
    WUQSFtimeLimitHi = 8,
    WUQSFwarnTimeLimit = 9,
    WUQSFwarnTimeLimitHi = 10,
    WUQSFpriority = 11,
    WUQSFpriorityHi = 12,
    WUQSFQuerySet = 13,
    WUQSFterm = 0,
    WUQSFreverse = 256,
    WUQSFnocase = 512,
    WUQSFnumeric = 1024,
    WUQSFwild = 2048
};

typedef IIteratorOf<IPropertyTree> IConstQuerySetQueryIterator;


interface IWorkUnitFactory : extends IInterface
{
    virtual IWorkUnit * createWorkUnit(const char * parentWuid, const char * app, const char * user) = 0;
    virtual bool deleteWorkUnit(const char * wuid) = 0;
    virtual IConstWorkUnit * openWorkUnit(const char * wuid, bool lock) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByOwner(const char * owner) = 0;
    virtual IWorkUnit * updateWorkUnit(const char * wuid) = 0;
    virtual int setTracingLevel(int newlevel) = 0;
    virtual IWorkUnit * createNamedWorkUnit(const char * wuid, const char * parentWuid, const char * app, const char * user) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByState(WUState state) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByECL(const char * ecl) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByCluster(const char * cluster) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsByXPath(const char * xpath) = 0;
    virtual IConstWorkUnitIterator * getWorkUnitsSorted(WUSortField * sortorder, WUSortField * filters, const void * filterbuf, unsigned startoffset, unsigned maxnum, const char * queryowner, __int64 * cachehint, unsigned *total) = 0;
    virtual unsigned numWorkUnits() = 0;
    virtual unsigned numWorkUnitsFiltered(WUSortField * filters, const void * filterbuf) = 0;
    virtual void descheduleAllWorkUnits() = 0;
    virtual bool deleteWorkUnitEx(const char * wuid) = 0;
    virtual IConstQuerySetQueryIterator * getQuerySetQueriesSorted(WUQuerySortField *sortorder, WUQuerySortField *filters, const void *filterbuf, unsigned startoffset, unsigned maxnum, __int64 *cachehint, unsigned *total) = 0;
};


interface IWorkflowScheduleConnection : extends IInterface
{
    virtual void lock() = 0;
    virtual void unlock() = 0;
    virtual void setActive() = 0;
    virtual void resetActive() = 0;
    virtual bool queryActive() = 0;
    virtual bool pull(IWorkflowItemArray * workflow) = 0;
    virtual void push(const char * name, const char * text) = 0;
};


interface IExtendedWUInterface
{
    virtual unsigned calculateHash(unsigned prevHash) = 0;
    virtual void copyWorkUnit(IConstWorkUnit *cached, bool all) = 0;
    virtual bool archiveWorkUnit(const char *base,bool del,bool ignoredllerrors,bool deleteOwned) = 0;
    virtual void packWorkUnit(bool pack=true) = 0;
    
};

struct WorkunitUpdate : public Owned<IWorkUnit>
{
public:
    WorkunitUpdate(IWorkUnit *wu) : Owned<IWorkUnit>(wu) { }
    ~WorkunitUpdate() { if (get()) get()->commit(); }
};

class WuStatisticTarget : implements IStatisticTarget
{
public:
    WuStatisticTarget(IWorkUnit * _wu, const char * _defaultWho) : wu(_wu), defaultWho(_defaultWho) {}

    virtual void addStatistic(const char * creator_who, const char * wuScope_where, const char * stat_what, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge)
    {
        if (!creator_who) creator_who = defaultWho;
        wu->setStatistic(creator_who, wuScope_where, stat_what, description, kind, value, count, maxValue, merge);
    }

protected:
    Linked<IWorkUnit> wu;
    const char * defaultWho;
};

extern WORKUNIT_API IStringVal &getEclCCServerQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API IStringVal &getEclServerQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API IStringVal &getEclSchedulerQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API IStringVal &getAgentQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API IStringVal &getRoxieQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API IStringVal &getThorQueueNames(IStringVal &ret, const char *process);
extern WORKUNIT_API StringBuffer &getClusterThorQueueName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API StringBuffer &getClusterThorGroupName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API StringBuffer &getClusterRoxieQueueName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API StringBuffer &getClusterEclCCServerQueueName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API StringBuffer &getClusterEclServerQueueName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API StringBuffer &getClusterEclAgentQueueName(StringBuffer &ret, const char *cluster);
extern WORKUNIT_API IStringIterator *getTargetClusters(const char *processType, const char *processName);
extern WORKUNIT_API bool validateTargetClusterName(const char *clustname);
extern WORKUNIT_API IConstWUClusterInfo* getTargetClusterInfo(const char *clustname);
typedef IArrayOf<IConstWUClusterInfo> CConstWUClusterInfoArray;
extern WORKUNIT_API unsigned getEnvironmentClusterInfo(CConstWUClusterInfoArray &clusters);
extern WORKUNIT_API unsigned getEnvironmentClusterInfo(IPropertyTree* environmentRoot, CConstWUClusterInfoArray &clusters);
extern WORKUNIT_API void getRoxieProcessServers(const char *process, SocketEndpointArray &servers);
extern WORKUNIT_API bool isProcessCluster(const char *remoteDali, const char *process);
extern WORKUNIT_API bool isProcessCluster(const char *process);

extern WORKUNIT_API bool getWorkUnitCreateTime(const char *wuid,CDateTime &time); // based on WUID
extern WORKUNIT_API bool restoreWorkUnit(const char *base,const char *wuid);
extern WORKUNIT_API void clientShutdownWorkUnit();
extern WORKUNIT_API IExtendedWUInterface * queryExtendedWU(IWorkUnit * wu);
extern WORKUNIT_API unsigned getEnvironmentThorClusterNames(StringArray &thorNames, StringArray &groupNames, StringArray &targetNames, StringArray &queueNames);
extern WORKUNIT_API unsigned getEnvironmentHThorClusterNames(StringArray &eclAgentNames, StringArray &groupNames, StringArray &targetNames);
extern WORKUNIT_API StringBuffer &formatGraphTimerLabel(StringBuffer &str, const char *graphName, unsigned subGraphNum=0, unsigned __int64 subId=0);
extern WORKUNIT_API StringBuffer &formatGraphTimerScope(StringBuffer &str, const char *graphName, unsigned subGraphNum, unsigned __int64 subId);
extern WORKUNIT_API bool parseGraphTimerLabel(const char *label, StringAttr &graphName, unsigned & graphNum, unsigned &subGraphNum, unsigned &subId);
extern WORKUNIT_API void addExceptionToWorkunit(IWorkUnit * wu, WUExceptionSeverity severity, const char * source, unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column);
extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory();
extern WORKUNIT_API IWorkUnitFactory * getSecWorkUnitFactory(ISecManager &secmgr, ISecUser &secuser);
extern WORKUNIT_API IWorkUnitFactory * getWorkUnitFactory(ISecManager *secmgr, ISecUser *secuser);
extern WORKUNIT_API ILocalWorkUnit* createLocalWorkUnit();
extern WORKUNIT_API IStringVal& exportWorkUnitToXML(const IConstWorkUnit *wu, IStringVal &str, bool unpack);
extern WORKUNIT_API StringBuffer &exportWorkUnitToXML(const IConstWorkUnit *wu, StringBuffer &str, bool unpack);
extern WORKUNIT_API void exportWorkUnitToXMLFile(const IConstWorkUnit *wu, const char * filename, unsigned extraXmlFlags, bool unpack);
extern WORKUNIT_API void submitWorkUnit(const char *wuid, const char *username, const char *password);
extern WORKUNIT_API void abortWorkUnit(const char *wuid);
extern WORKUNIT_API void submitWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser);
extern WORKUNIT_API void abortWorkUnit(const char *wuid, ISecManager *secmgr, ISecUser *secuser);
extern WORKUNIT_API void secSubmitWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser);
extern WORKUNIT_API void secAbortWorkUnit(const char *wuid, ISecManager &secmgr, ISecUser &secuser);
extern WORKUNIT_API IWUResult * updateWorkUnitResult(IWorkUnit * w, const char *name, unsigned sequence);
extern WORKUNIT_API IConstWUResult * getWorkUnitResult(IConstWorkUnit * w, const char *name, unsigned sequence);
//returns a state code.  WUStateUnknown == timeout
extern WORKUNIT_API WUState waitForWorkUnitToComplete(const char * wuid, int timeout = -1, bool returnOnWaitState = false);
extern WORKUNIT_API bool waitForWorkUnitToCompile(const char * wuid, int timeout = -1);
extern WORKUNIT_API WUState secWaitForWorkUnitToComplete(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout = -1, bool returnOnWaitState = false);
extern WORKUNIT_API bool secWaitForWorkUnitToCompile(const char * wuid, ISecManager &secmgr, ISecUser &secuser, int timeout = -1);
extern WORKUNIT_API bool secDebugWorkunit(const char * wuid, ISecManager &secmgr, ISecUser &secuser, const char *command, StringBuffer &response);
extern WORKUNIT_API IStringVal& createToken(const char *wuid, const char *user, const char *password, IStringVal &str);
// This latter is temporary - tokens will be replaced by something more secure
extern WORKUNIT_API void extractToken(const char *token, const char *wuid, IStringVal &user, IStringVal &password);
extern WORKUNIT_API WUState getWorkUnitState(const char* state);
extern WORKUNIT_API IWorkflowScheduleConnection * getWorkflowScheduleConnection(char const * wuid);
extern WORKUNIT_API const char *skipLeadingXml(const char *text);
extern WORKUNIT_API bool isArchiveQuery(const char * text);
extern WORKUNIT_API bool isQueryManifest(const char * text);
extern WORKUNIT_API IPropertyTree * resolveDefinitionInArchive(IPropertyTree * archive, const char * path);

inline bool isLibrary(IConstWorkUnit * wu) { return wu->getApplicationValueInt("LibraryModule", "interfaceHash", 0) != 0; }
extern WORKUNIT_API bool looksLikeAWuid(const char * wuid);

enum WUQueryActivationOptions
{
    DO_NOT_ACTIVATE = 0,
    MAKE_ACTIVATE= 1,
    ACTIVATE_SUSPEND_PREVIOUS = 2,
    ACTIVATE_DELETE_PREVIOUS = 3,
    DO_NOT_ACTIVATE_LOAD_DATA_ONLY = 4,
    MAKE_ACTIVATE_LOAD_DATA_ONLY = 5
};

extern WORKUNIT_API int calcPriorityValue(const IPropertyTree * p);  // Calls to this should really go through the workunit interface.

extern WORKUNIT_API IPropertyTree * addNamedQuery(IPropertyTree * queryRegistry, const char * name, const char * wuid, const char * dll, bool library, const char *userid);       // result not linked
extern WORKUNIT_API void removeNamedQuery(IPropertyTree * queryRegistry, const char * id);
extern WORKUNIT_API void removeWuidFromNamedQueries(IPropertyTree * queryRegistry, const char * wuid);
extern WORKUNIT_API void removeDllFromNamedQueries(IPropertyTree * queryRegistry, const char * dll);
extern WORKUNIT_API void removeAliasesFromNamedQuery(IPropertyTree * queryRegistry, const char * id);
extern WORKUNIT_API void setQueryAlias(IPropertyTree * queryRegistry, const char * name, const char * value);

extern WORKUNIT_API IPropertyTree * getQueryById(IPropertyTree * queryRegistry, const char *queryid);
extern WORKUNIT_API IPropertyTree * getQueryById(const char *queryset, const char *queryid, bool readonly);
extern WORKUNIT_API IPropertyTree * resolveQueryAlias(IPropertyTree * queryRegistry, const char * alias);
extern WORKUNIT_API IPropertyTree * resolveQueryAlias(const char *queryset, const char *alias, bool readonly);
extern WORKUNIT_API IPropertyTree * getQueryRegistry(const char * wsEclId, bool readonly);
extern WORKUNIT_API IPropertyTree * getQueryRegistryRoot();

extern WORKUNIT_API void setQueryCommentForNamedQuery(IPropertyTree * queryRegistry, const char *id, const char *queryComment);

extern WORKUNIT_API void setQuerySuspendedState(IPropertyTree * queryRegistry, const char * name, bool suspend, const char *userid);

extern WORKUNIT_API IPropertyTree * addNamedPackageSet(IPropertyTree * packageRegistry, const char * name, IPropertyTree *packageInfo, bool overWrite);     // result not linked
extern WORKUNIT_API void removeNamedPackage(IPropertyTree * packageRegistry, const char * id);
extern WORKUNIT_API IPropertyTree * getPackageSetRegistry(const char * wsEclId, bool readonly);

extern WORKUNIT_API void addQueryToQuerySet(IWorkUnit *workunit, const char *querySetName, const char *queryName, IPropertyTree *packageInfo, WUQueryActivationOptions activateOption, StringBuffer &newQueryId, const char *userid);
extern WORKUNIT_API bool removeQuerySetAlias(const char *querySetName, const char *alias);
extern WORKUNIT_API void addQuerySetAlias(const char *querySetName, const char *alias, const char *id);
extern WORKUNIT_API void setSuspendQuerySetQuery(const char *querySetName, const char *id, bool suspend, const char *userid);
extern WORKUNIT_API void deleteQuerySetQuery(const char *querySetName, const char *id);
extern WORKUNIT_API const char *queryIdFromQuerySetWuid(const char *querySetName, const char *wuid, IStringVal &id);
extern WORKUNIT_API void removeQuerySetAliasesFromNamedQuery(const char *querySetName, const char * id);
extern WORKUNIT_API void setQueryCommentForNamedQuery(const char *querySetName, const char *id, const char *comment);
extern WORKUNIT_API void gatherLibraryNames(StringArray &names, StringArray &unresolved, IWorkUnitFactory &workunitFactory, IConstWorkUnit &cw, IPropertyTree *queryset);

extern WORKUNIT_API void associateLocalFile(IWUQuery * query, WUFileType type, const char * name, const char * description, unsigned crc);

interface ITimeReporter;
extern WORKUNIT_API void updateWorkunitTimeStat(IWorkUnit * wu, const char * component, const char * wuScope, const char * stat, const char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue);
extern WORKUNIT_API void updateWorkunitTiming(IWorkUnit * wu, const char * component, const char * mangledScope, const char * description, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue);
extern WORKUNIT_API void updateWorkunitTimings(IWorkUnit * wu, ITimeReporter *timer, const char * component);



extern WORKUNIT_API const char *getTargetClusterComponentName(const char *clustname, const char *processType, StringBuffer &name);
extern WORKUNIT_API void descheduleWorkunit(char const * wuid);
#if 0
void WORKUNIT_API testWorkflow();
#endif

#endif
