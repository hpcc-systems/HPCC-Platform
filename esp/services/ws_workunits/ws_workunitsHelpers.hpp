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

#ifndef _WSWU_HELPERS_HPP__
#define _WSWU_HELPERS_HPP__

#include "ws_workunits_esp.ipp"
#include "exception_util.hpp"

#include "jtime.ipp"
#include "workunit.hpp"
#include "hqlerror.hpp"
#include "dllserver.hpp"
#include "mpbase.hpp"

#include <list>
#include <vector>
#include <functional>

namespace ws_workunits {

#define     OWN_WU_ACCESS      "OwnWorkunitsAccess"
#define     OTHERS_WU_ACCESS   "OthersWorkunitsAccess"

#define    File_Cpp "cpp"
#define    File_Log "log"
#define    File_ThorLog "ThorLog"
#define    File_ThorSlaveLog "ThorSlaveLog"
#define    File_EclAgentLog "EclAgentLog"
#define    File_XML "XML"
#define    File_Res "res"
#define    File_DLL "dll"
#define    File_WUECL "WUECL"
#define    File_ArchiveQuery "ArchiveQuery"

#define    TOTALTHORTIME    "Total thor time"
#define    TOTALCLUSTERTIME "Total cluster time" //for roxie and hthor

#define    TEMPZIPDIR "tempzipfiles"

static const unsigned THOR_SLAVE_LOG_THREAD_POOL_SIZE = 100;
static const long MAXXLSTRANSFER = 5000000;
const unsigned DATA_SIZE = 16;
const unsigned WUARCHIVE_CACHE_SIZE = 8;
const unsigned WUARCHIVE_CACHE_MINITES = 5;
const unsigned AWUS_CACHE_SIZE = 16;
const unsigned AWUS_CACHE_MIN_DEFAULT = 15;
const unsigned WUDEFAULT_ZAPEMAILSERVER_PORT = 25;

inline bool notEmpty(const char *val){return (val && *val);}
inline bool isEmpty(const char *val){return (!val || !*val);}

const char *getWuAccessType(IConstWorkUnit& cw, const char *user);

SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, const char *owner, SecAccessFlags accessOwn, SecAccessFlags accessOthers);
SecAccessFlags chooseWuAccessFlagsByOwnership(const char *user, IConstWorkUnitInfo& cw, SecAccessFlags accessOwn, SecAccessFlags accessOthers);
SecAccessFlags getWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw);

void getUserWuAccessFlags(IEspContext& context, SecAccessFlags& accessOwn, SecAccessFlags& accessOthers, bool except);
void ensureWsWorkunitAccess(IEspContext& cxt, IConstWorkUnit& cw, SecAccessFlags minAccess);
void ensureWsWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess);
void ensureWsWorkunitAccessByOwnerId(IEspContext& context, const char* owner, SecAccessFlags minAccess);
void ensureWsCreateWorkunitAccess(IEspContext& cxt);
bool validateWsWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess);

const char *getGraphNum(const char *s,unsigned &num);

class WsWuDateTime : public CScmDateTime
{
public:
    IMPLEMENT_IINTERFACE;
    WsWuDateTime()
    {
        setSimpleLocal(0);
    }

    bool isValid()
    {
        unsigned year, month, day;
        cdt.getDate(year, month, day, true);
        return year>1969;
    }
};

void formatDuration(StringBuffer &s, unsigned ms);

struct WsWUExceptions
{
    WsWUExceptions(IConstWorkUnit& wu);

    operator IArrayOf<IEspECLException>&() { return errors; }
    int ErrCount() { return numerr; }
    int WrnCount() { return numwrn; }
    int InfCount() { return numinf; }
    int AlertCount() { return numalert; }

private:
    IArrayOf<IEspECLException> errors;
    int numerr;
    int numwrn;
    int numinf;
    int numalert;
};

#define WUINFO_TruncateEclTo64k         0x0001
#define WUINFO_IncludeExceptions        0x0002
#define WUINFO_IncludeGraphs            0x0004
#define WUINFO_IncludeResults           0x0008
#define WUINFO_IncludeVariables         0x0010
#define WUINFO_IncludeTimers            0x0020
#define WUINFO_IncludeDebugValues       0x0040
#define WUINFO_IncludeApplicationValues 0x0080
#define WUINFO_IncludeWorkflows         0x0100
#define WUINFO_IncludeEclSchemas        0x0200
#define WUINFO_IncludeSourceFiles       0x0400
#define WUINFO_IncludeResultsViewNames  0x0800
#define WUINFO_IncludeXmlSchema         0x1000
#define WUINFO_IncludeResourceURLs      0x2000
#define WUINFO_IncludeECL               0x4000
#define WUINFO_IncludeHelpers           0x8000
#define WUINFO_IncludeAllowedClusters   0x10000
#define WUINFO_IncludeTotalClusterTime  0x20000
#define WUINFO_IncludeServiceNames      0x40000
#define WUINFO_All                      0xFFFFFFFF

class WsWuInfo
{
    IEspWUArchiveFile* readArchiveFileAttr(IPropertyTree& fileTree, const char* path);
    IEspWUArchiveModule* readArchiveModuleAttr(IPropertyTree& moduleTree, const char* path);
    void readArchiveFiles(IPropertyTree* archiveTree, const char* path, IArrayOf<IEspWUArchiveFile>& files);
    void outputALine(size32_t len, const char* content, MemoryBuffer& outputBuf, IFileIOStream* outIOS);
    bool parseLogLine(const char* line, const char* endWUID, unsigned& processID, const unsigned columnNumPID);
    void readWorkunitThorLog(const char* processName, const char* logSpec, const char* slaveIPAddress, unsigned slaveNum, MemoryBuffer& buf, const char* outFile);
    void readWorkunitThorLogOneDay(IFile* ios, unsigned& processID, MemoryBuffer& buf, IFileIOStream* outIOS);
    void readFileContent(const char* sourceFileName, const char* sourceIPAddress,
        const char* sourceAlias, MemoryBuffer &mb, bool forDownload);
    void copyContentFromRemoteFile(const char* sourceFileName, const char* sourceIPAddress,
        const char* sourceAlias, const char *outFileName);
public:
    WsWuInfo(IEspContext &ctx, IConstWorkUnit *cw_) :
      context(ctx), cw(cw_)
    {
        version = context.getClientVersion();
        wuid.set(cw->queryWuid());
    }

    WsWuInfo(IEspContext &ctx, const char *wuid_) :
      context(ctx)
    {
        wuid.set(wuid_);
        version = context.getClientVersion();
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(ctx.querySecManager(), ctx.queryUser());
        cw.setown(factory->openWorkUnit(wuid_));
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.", wuid_);
    }

    bool getResourceInfo(StringArray &viewnames, StringArray &urls, unsigned long flags);
    unsigned getResourceURLCount();

    void getCommon(IEspECLWorkunit &info, unsigned long flags);
    void getInfo(IEspECLWorkunit &info, unsigned long flags);

    void getResults(IEspECLWorkunit &info, unsigned long flags);
    void getVariables(IEspECLWorkunit &info, unsigned long flags);
    void getDebugValues(IEspECLWorkunit &info, unsigned long flags);
    bool getClusterInfo(IEspECLWorkunit &info, unsigned long flags);
    void getApplicationValues(IEspECLWorkunit &info, unsigned long flags);
    void getExceptions(IEspECLWorkunit &info, unsigned long flags);
    void getSourceFiles(IEspECLWorkunit &info, unsigned long flags);
    unsigned getTimerCount();
    void getTimers(IEspECLWorkunit &info, unsigned long flags);
    void doGetTimers(IArrayOf<IEspECLTimer>& timers);
    void getHelpers(IEspECLWorkunit &info, unsigned long flags);
    void getGraphInfo(IEspECLWorkunit &info, unsigned long flags);
    void doGetGraphs(IArrayOf<IEspECLGraph>& graphs);
    void getWUGraphNameAndTypes(WUGraphType graphType, IArrayOf<IEspNameAndType>& graphNameAndTypes);
    void getGraphTimingData(IArrayOf<IConstECLTimingData> &timingData);
    bool getFileSize(const char* fileName, const char* IPAddress, offset_t& fileSize);

    void getWorkflow(IEspECLWorkunit &info, unsigned long flags);

    void getHelpFiles(IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers, unsigned long flags, unsigned& helpersCount);
    void getSubFiles(IPropertyTreeIterator* f, IEspECLSourceFile* eclSuperFile, StringArray& fileNames);
    void getEclSchemaChildFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    void getEclSchemaFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    bool getResultEclSchemas(IConstWUResult &r, IArrayOf<IEspECLSchemaItem>& schemas);
    void getResult(IConstWUResult &r, IArrayOf<IEspECLResult>& results, unsigned long flags);
    void getStats(const WuScopeFilter & filter, const StatisticsFilter& statsFilter, bool createDescriptions, IArrayOf<IEspWUStatisticItem>& statistics);
    void getServiceNames(IEspECLWorkunit &info, unsigned long flags);

    void getWUProcessLogSpecs(const char* processName, const char* logSpec, const char* logDir, bool eclAgent, StringArray& logSpecs);
    void getWorkunitEclAgentLog(const char *processName, const char* eclAgentInstance, const char* agentPid, MemoryBuffer& buf, const char* outFile);
    void getWorkunitThorMasterLog(const char *processName, const char* fileName, MemoryBuffer& buf, const char* outFile);
    void getWorkunitThorSlaveLog(IGroup *nodeGroup, const char *ipAddress, const char* processName, const char* logDate,
        const char* logDir, int slaveNum, MemoryBuffer& buf, const char* outIOS, bool forDownload);
    void getWorkunitThorSlaveLog(IPropertyTree* directories, const char *process,
        const char* instanceName, const char *ipAddress, const char* logDate, int slaveNum,
        MemoryBuffer& buf, const char* outFile, bool forDownload);
    void getWorkunitResTxt(MemoryBuffer& buf);
    void getWorkunitArchiveQuery(StringBuffer& str);
    void getWorkunitArchiveQuery(MemoryBuffer& mb);
    void getWorkunitDll(StringBuffer &name, MemoryBuffer& buf);
    void getWorkunitXml(const char* plainText, MemoryBuffer& buf);
    void getWorkunitQueryShortText(MemoryBuffer& buf, const char* outFile);
    void getWorkunitAssociatedXml(const char* name, const char* IPAddress, const char* plainText, const char* description,
        bool forDownload, bool addXMLDeclaration, MemoryBuffer& buf, const char* outFile);
    void getWorkunitCpp(const char* cppname, const char* description, const char* ipAddress, MemoryBuffer& buf, bool forDownload, const char* outFile);
    void getEventScheduleFlag(IEspECLWorkunit &info);
    unsigned getWorkunitThorLogInfo(IArrayOf<IEspECLHelpFile>& helpers, IEspECLWorkunit &info, unsigned long flags, unsigned& helpersCount);
    IDistributedFile* getLogicalFileData(IEspContext& context, const char* logicalName, bool& showFileContent);

    IPropertyTree* getWorkunitArchive();
    void listArchiveFiles(IPropertyTree* archive, const char* path, IArrayOf<IEspWUArchiveModule>& modules, IArrayOf<IEspWUArchiveFile>& files);
    void getArchiveFile(IPropertyTree* archive, const char* moduleName, const char* attrName, const char* path, StringBuffer& file);
    void setWUAbortTime(IEspECLWorkunit &info, unsigned __int64 abortTS);

    void addTimerToList(SCMStringBuffer& name, const char * scope, IConstWUStatistic & stat, IArrayOf<IEspECLTimer>& timers);
protected:
    bool hasSubGraphTimings();

public:
    IEspContext &context;
    Linked<IConstWorkUnit> cw;
    double version;
    SCMStringBuffer clusterName;
    StringAttr wuid;
};

struct WsWuSearch
{
    WsWuSearch(IEspContext& context,const char* owner=NULL,const char* state=NULL,const char* cluster=NULL,const char* startDate=NULL,const char* endDate=NULL,const char* jobname=NULL);

    typedef std::vector<std::string>::iterator iterator;

    iterator begin() { return wuids.begin(); }
    iterator end()   { return wuids.end(); }

    iterator locate(const char* wuid)
    {
        if(wuids.size() && *wuids.begin()>wuid)
            return std::lower_bound(wuids.begin(),wuids.end(),wuid,std::greater<std::string>());
        return wuids.begin();
    }

     __int64 getSize() { return wuids.size(); }

private:

    StringBuffer& createWuidFromDate(const char* timestamp,StringBuffer& s);

    std::vector<std::string> wuids;
};

struct DataCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    DataCacheElement(const char* filter, const char* data, const char* name, const char* logicalName, const char* wuid,
        const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total):m_filter(filter),
        m_data(data), m_name(name), m_logicalName(logicalName), m_wuid(wuid), m_resultName(resultName),
        m_seq(seq), m_start(start), m_rowcount(count), m_requested(requested), m_total(total)
    {
        m_timeCached.setNow();
    }

    CDateTime m_timeCached;
    std::string m_filter;
    std::string m_data;
    std::string m_name;
    std::string m_logicalName;
    std::string m_wuid;
    std::string m_resultName;
    unsigned m_seq;
    __int64 m_start;
    unsigned m_rowcount;
    __int64 m_requested;
    __int64 m_total;
};

struct DataCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;

    DataCache(size32_t _cacheSize=0): cacheSize(_cacheSize){}

    DataCacheElement* lookup(IEspContext &context, const char* filter, unsigned timeOutMin);

    void add(const char* filter, const char* data, const char* name, const char* localName, const char* wuid,
    const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total);

    std::list<Linked<DataCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

interface IArchivedWUsReader : extends IInterface
{
    virtual void getArchivedWUs(bool lightWeight, IEspWUQueryRequest& req, IEspWULightWeightQueryRequest& reqLW, IArrayOf<IEspECLWorkunit>& results, IArrayOf<IEspECLWorkunitLW>& resultsLW) = 0;
    virtual bool getHasMoreWU() = 0;
    virtual unsigned getNumberOfWUsReturned() = 0;
};

struct ArchivedWuCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    ArchivedWuCacheElement(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, unsigned _numWUsReturned, IArrayOf<IEspECLWorkunit>& wus, IArrayOf<IEspECLWorkunitLW>& lwwus):m_filter(filter),
        m_sashaUpdatedWhen(sashaUpdatedWhen), m_hasNextPage(hasNextPage), numWUsReturned(_numWUsReturned)
    {
        m_timeCached.setNow();
        ForEachItemIn(i, wus)
        {
            Owned<IEspECLWorkunit> info= createECLWorkunit("","");
            IEspECLWorkunit& info0 = wus.item(i);
            info->copy(info0);

            m_results.append(*info.getClear());
        }
        ForEachItemIn(ii, lwwus)
        {
            Owned<IEspECLWorkunitLW> info= createECLWorkunitLW("","");
            IEspECLWorkunitLW& info0 = lwwus.item(ii);
            info->copy(info0);

            resultsLW.append(*info.getClear());
        }
    }

    std::string m_filter;
    std::string m_sashaUpdatedWhen;
    bool m_hasNextPage;
    unsigned numWUsReturned;

    CDateTime m_timeCached;
    IArrayOf<IEspECLWorkunit> m_results;
    IArrayOf<IEspECLWorkunitLW> resultsLW;
};

struct ArchivedWuCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;

    ArchivedWuCache(size32_t _cacheSize=0): cacheSize(_cacheSize){}
    ArchivedWuCacheElement* lookup(IEspContext &context, const char* filter, const char* sashaUpdatedWhen, unsigned timeOutMin);

    void add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, unsigned numWUsReturned, IArrayOf<IEspECLWorkunit>& wus, IArrayOf<IEspECLWorkunitLW>& lwwus);

    std::list<Linked<ArchivedWuCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

struct WUArchiveCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    WUArchiveCacheElement(const char* _wuid, IPropertyTree* _archive) : wuid(_wuid)
    {
        archive.setown(_archive);
        timeCached.setNow();
    }

    CDateTime timeCached;
    std::string wuid;
    Owned<IPropertyTree> archive;
};

struct CompareWUArchive
{
    CompareWUArchive(const char* _wuid): wuid(_wuid) {}
    bool operator()(const Linked<WUArchiveCacheElement>& e) const
    {
        return streq(e->wuid.c_str(), wuid.c_str());
    }
    std::string wuid;
};

struct WUArchiveCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;

    WUArchiveCache(size32_t _cacheSize=0): cacheSize(_cacheSize){}

    WUArchiveCacheElement* lookup(IEspContext &context, const char* wuid, unsigned timeOutMin)
    {
        CriticalBlock block(crit);

        if (cache.size() < 1)
            return NULL;

        //erase data if it should be
        CDateTime timeNow;
        int timeout = timeOutMin;
        timeNow.setNow();
        timeNow.adjustTime(-timeout);
        while (true)
        {
            std::list<Linked<WUArchiveCacheElement> >::iterator list_iter = cache.begin();
            if (list_iter == cache.end())
                break;

            WUArchiveCacheElement* wuArchive = list_iter->get();
            if (!wuArchive || (wuArchive->timeCached > timeNow))
                break;

            cache.pop_front();
        }

        if (cache.size() < 1)
            return NULL;

        //Check whether we have the WUArchive cache for this WU.
        std::list<Linked<WUArchiveCacheElement> >::iterator it = std::find_if(cache.begin(), cache.end(), CompareWUArchive(wuid));
        if(it!=cache.end())
        {
            return it->getLink();
        }

        return NULL;
    }

    void add(const char* _wuid, IPropertyTree* _archive)
    {
        Owned<IPropertyTree> archive = _archive;
        CriticalBlock block(crit);

        //Save new data
        Owned<WUArchiveCacheElement> e = new WUArchiveCacheElement(_wuid, archive.getClear());
        if (cacheSize > 0)
        {
            if (cache.size() >= cacheSize)
                cache.pop_front();

            cache.push_back(e.get());
        }

        return;
    }

    std::list<Linked<WUArchiveCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

class WsWuJobQueueAuditInfo
{
public:
    WsWuJobQueueAuditInfo() {  };
    WsWuJobQueueAuditInfo(IEspContext &context, const char *cluster, const char *from , const char *to, CHttpResponse* response, const char *xls);

    void getAuditLineInfo(const char* line, unsigned& longestQueue, unsigned& maxConnected, unsigned maxDisplay, unsigned showAll, IArrayOf<IEspThorQueue>& items);
    bool checkSameStrings(const char* s1, const char* s2);
    bool checkNewThorQueueItem(IEspThorQueue* tq, unsigned showAll, IArrayOf<IEspThorQueue>& items);
};

StringBuffer &getWuidFromLogicalFileName(IEspContext &context, const char *logicalName, StringBuffer &wuid);

bool addToQueryString(StringBuffer &queryString, const char *name, const char *value, const char delim = '&');

void xsltTransform(const char* xml, const char* sheet, IProperties *params, StringBuffer& ret);

class WUSchedule : public Thread
{
    bool stopping;
    Semaphore semSchedule;
    IEspContainer* m_container;
    bool detached;

public:
    WUSchedule()
    {
        stopping = false;
        detached = false;
        m_container = nullptr;
    }
    ~WUSchedule()
    {
        stopping = true;
        semSchedule.signal();
        join();
    }

    virtual int run();
    virtual void setContainer(IEspContainer * container)
    {
        m_container = container;
        if (m_container)
            setDetachedState(!m_container->isAttachedToDali());
    }

    void setDetachedState(bool detached_)
    {
        if (detached != detached_)
        {
            detached = detached_;
            semSchedule.signal();
        }
    }
};

namespace WsWuHelpers
{
    void setXmlParameters(IWorkUnit *wu, const char *xml, bool setJobname=false);
    void submitWsWorkunit(IEspContext& context, IConstWorkUnit* cw, const char* cluster, const char* snapshot, int maxruntime,  int maxcost, bool compile, bool resetWorkflow, bool resetVariables,
            const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications);
    void setXmlParameters(IWorkUnit *wu, const char *xml, IArrayOf<IConstNamedValue> *variables, bool setJobname=false);
    void submitWsWorkunit(IEspContext& context, const char *wuid, const char* cluster, const char* snapshot, int maxruntime,  int maxcost, bool compile, bool resetWorkflow, bool resetVariables,
            const char *paramXml, IArrayOf<IConstNamedValue> *variables, IArrayOf<IConstNamedValue> *debugs, IArrayOf<IConstApplicationValue> *applications);
    void copyWsWorkunit(IEspContext &context, IWorkUnit &wu, const char *srcWuid);
    void runWsWorkunit(IEspContext &context, StringBuffer &wuid, const char *srcWuid, const char *cluster, const char *paramXml=NULL,
            IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL, IArrayOf<IConstApplicationValue> *applications=NULL);
    void runWsWorkunit(IEspContext &context, IConstWorkUnit *cw, const char *srcWuid, const char *cluster, const char *paramXml=NULL,
            IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL, IArrayOf<IConstApplicationValue> *applications=NULL);
    IException * noteException(IWorkUnit *wu, IException *e, ErrorSeverity level=SeverityError);
    StringBuffer & resolveQueryWuid(StringBuffer &wuid, const char *queryset, const char *query, bool notSuspended=true, IWorkUnit *wu=NULL);
    void runWsWuQuery(IEspContext &context, IConstWorkUnit *cw, const char *queryset, const char *query, const char *cluster, const char *paramXml=NULL,
            IArrayOf<IConstApplicationValue> *applications=NULL);
    void runWsWuQuery(IEspContext &context, StringBuffer &wuid, const char *queryset, const char *query, const char *cluster, const char *paramXml=NULL,
            IArrayOf<IConstApplicationValue> *applications=NULL);
    void checkAndTrimWorkunit(const char* methodName, StringBuffer& input);
};

class NewWsWorkunit : public Owned<IWorkUnit>
{
public:
    NewWsWorkunit(IWorkUnitFactory *factory, IEspContext &context)
    {
        create(factory, context, NULL);
    }

    NewWsWorkunit(IEspContext &context)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        create(factory, context, NULL);
    }

    NewWsWorkunit(IEspContext &context, const char *wuid)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        create(factory, context, wuid);
    }

    ~NewWsWorkunit() { if (get()) get()->commit(); }

    void create(IWorkUnitFactory *factory, IEspContext &context, const char *wuid)
    {
        if (wuid && *wuid)
            setown(factory->createNamedWorkUnit(wuid, "ws_workunits", context.queryUserId()));
        else
            setown(factory->createWorkUnit("ws_workunits", context.queryUserId()));
        if(!get())
          throw MakeStringException(ECLWATCH_CANNOT_CREATE_WORKUNIT,"Could not create workunit.");
        get()->setUser(context.queryUserId());
    }

    void associateDll(const char *dllpath, const char *dllname)
    {
        Owned<IWUQuery> query = get()->updateQuery();
        StringBuffer dllurl;
        createUNCFilename(dllpath, dllurl);
        unsigned crc = crc_file(dllpath);
        associateLocalFile(query, FileTypeDll, dllpath, "Workunit DLL", crc);
        queryDllServer().registerDll(dllname, "Workunit DLL", dllurl.str());
    }

    void setQueryText(const char *text)
    {
        if (!text || !*text)
            return;
        Owned<IWUQuery> query=get()->updateQuery();
        query->setQueryText(text);
    }

    void setQueryMain(const char *s)
    {
        if (!s || !*s)
            return;
        Owned<IWUQuery> query=get()->updateQuery();
        query->setQueryMainDefinition(s);
    }
};

struct CWsWuZAPInfoReq
{
    StringBuffer wuid, espIP, url, thorIP, problemDesc, whatChanged, whereSlow, includeThorSlaveLog, zapFileName, password;
    StringBuffer emailFrom, emailTo, emailServer, emailSubject, emailBody;
    bool sendEmail, attachZAPReportToEmail;
    unsigned maxAttachmentSize, port;
};

class CWsWuFileHelper
{
    IPropertyTree* directories;
    unsigned thorSlaveLogThreadPoolSize = THOR_SLAVE_LOG_THREAD_POOL_SIZE;

    void cleanFolder(IFile *folder, bool removeFolder);
    int zipAFolder(const char *folder, const char *passwordReq, const char *zipFileNameWithPath);
    int zipAFolder(const char *folder, bool gzip, const char *zipFileNameWithPath);
    void setZAPFile(const char *zipFileNameReq, const char *zipFileNamePrefix, StringBuffer &zipFileName,
        StringBuffer &zipFileNameWithPath);
    void writeToFile(const char *fileName, size32_t contentLength, const void *content);
    void writeToFileIOStream(const char *folder, const char *file, MemoryBuffer &mb);
    void readWUFile(const char *wuid, const char *zipFolder, WsWuInfo &winfo, IConstWUFileOption &item,
        StringBuffer &fileName, StringBuffer &fileMimeType);
    IFile *createWorkingFolder(IEspContext &context, const char *wuid, const char *namePrefix,
        StringBuffer &namePrefixStr, StringBuffer &folderName);

    void createZAPInfoFile(const char *url, const char *espIP, const char *thorIP, const char *problemDesc,
        const char *whatChanged, const char *timing, IConstWorkUnit *cwu, const char *pathNameStr);
    void createZAPWUXMLFile(WsWuInfo &winfo, const char *pathNameStr);
    void createZAPECLQueryArchiveFiles(IConstWorkUnit *cwu, const char *pathNameStr);
    void createZAPWUQueryAssociatedFiles(IConstWorkUnit *cwu, const char *pathToCreate);
    void createZAPWUGraphProgressFile(const char *wuid, const char *pathNameStr);
    void createProcessLogfile(IConstWorkUnit *cwu, WsWuInfo &winfo, const char *process, const char *path);
    void createThorSlaveLogfile(IConstWorkUnit *cwu, WsWuInfo &winfo, const char *path);
    void writeZAPWUInfoToIOStream(IFileIOStream *outFile, const char *name, SCMStringBuffer &value);
    void writeZAPWUInfoToIOStream(IFileIOStream *outFile, const char *name, const char *value);
public:
    CWsWuFileHelper(IPropertyTree *_directories) : directories(_directories) {};

    void createWUZAPFile(IEspContext &context, IConstWorkUnit* cwu, CWsWuZAPInfoReq &request,
        StringBuffer &zipFileName, StringBuffer &zipFileNameWithPath, unsigned _thorSlaveLogThreadPoolSize);
    IFileIOStream* createWUZAPFileIOStream(IEspContext &context, IConstWorkUnit *cwu,
        CWsWuZAPInfoReq &request, unsigned _thorSlaveLogThreadPoolSize);

    IFileIOStream* createWUFileIOStream(IEspContext &context, const char *wuid, IArrayOf<IConstWUFileOption> &wuFileOptions,
        CWUFileDownloadOption &downloadOptions, StringBuffer &contentType);

    IFileIOStream* createIOStreamWithFileName(const char *fileNameWithPath, IFOmode mode);
};

class CWsWuEmailHelper
{
    StringAttr mailServer, sender, to, subject, body;
    StringAttr attachmentName, mimeType;
    unsigned port;
public:
    CWsWuEmailHelper(const char *_sender, const char *_to, const char *_mailServer, unsigned _port)
        : sender(_sender), to(_to), mailServer(_mailServer), port(_port) {};

    void setSubject(const char *_subject) { subject.set(_subject); };
    void setMimeType(const char *_mimeType) { mimeType.set(_mimeType); };
    void setAttachmentName(const char *_attachmentName) { attachmentName.set(_attachmentName); };

    void send(const char *body, const void *attachment, size32_t lenAttachment, StringArray &warnings);
};

class CGetThorSlaveLogToFileThreadParam : public CInterface
{
    WsWuInfo* wuInfo;
    Linked<IGroup> nodeGroup;
    unsigned slaveNum;
    StringAttr processName, logDir, fileName;

public:
    IMPLEMENT_IINTERFACE;

    CGetThorSlaveLogToFileThreadParam(WsWuInfo* _wuInfo, IGroup* _nodeGroup,
        const char*_processName, const char*_logDir, unsigned _slaveNum, const char* _fileName)
        : wuInfo(_wuInfo), nodeGroup(_nodeGroup), processName(_processName), logDir(_logDir),
          slaveNum(_slaveNum), fileName(_fileName) { };

    virtual void doWork()
    {
        MemoryBuffer dummy;
        wuInfo->getWorkunitThorSlaveLog(nodeGroup, nullptr, processName.get(), nullptr,
            logDir.get(), slaveNum, dummy, fileName.get(), false);;
    }
};

class CGetThorSlaveLogToFileThread : public CSimpleInterfaceOf<IPooledThread>
{
public:
    virtual void init(void* _param) override
    {
        param.setown((CGetThorSlaveLogToFileThreadParam*)_param);
    }
    virtual void threadmain() override
    {
        param->doWork();
        param.clear();
    }

    virtual bool canReuse() const override
    {
        return true;
    }
    virtual bool stop() override
    {
        return true;
    }
   
private:
    Owned<CGetThorSlaveLogToFileThreadParam> param;
};

//---------------------------------------------------------------------------------------------

class CGetThorSlaveLogToFileThreadFactory : public CSimpleInterfaceOf<IThreadFactory>
{
public:
    virtual IPooledThread *createNew() override
    {
        return new CGetThorSlaveLogToFileThread();
    }
};


}
#endif
