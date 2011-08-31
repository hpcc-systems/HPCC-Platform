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

#ifndef _ESPWIZ_ws_workunits_HPP__
#define _ESPWIZ_ws_workunits_HPP__

#include "ws_workunits_esp.ipp"
#include "workunit.hpp"
#include "WUWrapper.hpp"
#include "WUXMLInfo.hpp"

#include <list>

#include "jwrapper.hpp"

#include "fileview.hpp"
#include "hqlerror.hpp"

#include <vector>

using namespace esp;

typedef enum wsEclTypes_
{
    wsEclTypeUnknown,
    xsdString,
    xsdBoolean,
    xsdDecimal,
    xsdFloat,
    xsdDouble,
    xsdDuration,
    xsdDateTime,
    xsdTime,
    xsdDate,
    xsdYearMonth,
    xsdYear,
    xsdMonthDay,
    xsdDay,
    xsdMonth,
    xsdHexBinary,
    xsdBase64Binary,
    xsdAnyURI,
    xsdQName,
    xsdNOTATION,
    xsdNormalizedString,
    xsdToken,
    xsdLanguage,
    xsdNMTOKEN,
    xsdNMTOKENS, 
    xsdName,
    xsdNCName,
    xsdID,
    xsdIDREF,
    xsdIDREFS, 
    xsdENTITY,
    xsdENTITIES,
    xsdInteger,
    xsdNonPositiveInteger,
    xsdNegativeInteger,
    xsdLong,
    xsdInt,
    xsdShort,
    xsdByte,
    xsdNonNegativeInteger,
    xsdUnsignedLong,
    xsdUnsignedInt,
    xsdUnsignedShort,
    xsdUnsignedByte,
    xsdPositiveInteger,

    tnsRawDataFile,
    tnsCsvDataFile,
    tnsEspStringArray,
    tnsEspIntArray,
    tnsXmlDataSet,

    maxWsEclType

} wsEclType;

class WUSchedule : public Thread
{
    IEspContainer* m_container;
    bool m_allowNewRoxieOnDemandQuery;

public:
    virtual int run();
    virtual void setContainer(IEspContainer * container)
    {
        m_container = container;
    }
    
    void setAllowNewRoxieOnDemandQuery(bool allowNewRoxieOnDemandQuery)
    {
        m_allowNewRoxieOnDemandQuery = allowNewRoxieOnDemandQuery;
    }
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
    
    DataCache(size32_t _cacheSize=0): cacheSize(_cacheSize)
    {
    }


    DataCacheElement* lookup(IEspContext &context, const char* filter, unsigned timeOutMin);

     void add(const char* filter, const char* data, const char* name, const char* localName, const char* wuid, 
        const char* resultName, unsigned seq,   __int64 start, unsigned count, __int64 requested, __int64 total);

    std::list<StlLinked<DataCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

struct ArchivedWUsCacheElement: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    ArchivedWUsCacheElement(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, /*const char* data,*/ IArrayOf<IEspECLWorkunit>& wus):m_filter(filter), 
        m_sashaUpdatedWhen(sashaUpdatedWhen), m_hasNextPage(hasNextPage)/*, m_data(data)*/ 
    {
        m_timeCached.setNow();
        if (wus.length() > 0)

        for (unsigned i = 0; i < wus.length(); i++)
        {
            Owned<IEspECLWorkunit> info= createECLWorkunit("","");
            IEspECLWorkunit& info0 = wus.item(i);
            info->copy(info0);

            m_results.append(*info.getClear());
        }
    }

    std::string m_filter;
    std::string m_sashaUpdatedWhen;
    bool m_hasNextPage;

    CDateTime m_timeCached;
    IArrayOf<IEspECLWorkunit> m_results;
    //std::string m_data;
};

struct ArchivedWUsCache: public CInterface, implements IInterface
{
    IMPLEMENT_IINTERFACE;
    
    ArchivedWUsCache(size32_t _cacheSize=0): cacheSize(_cacheSize)
    {
    }


    ArchivedWUsCacheElement* lookup(IEspContext &context, const char* filter, const char* sashaUpdatedWhen, unsigned timeOutMin);
     //void getQueryFileListFromTree(IPropertyTreeIterator* queries, const char* fileType, const char* cluster, 
    //  IArrayOf<IEspRoxieDFULogicalFile>& queryFileList);

     void add(const char* filter, const char* sashaUpdatedWhen, bool hasNextPage, IArrayOf<IEspECLWorkunit>& wus);

    std::list<StlLinked<ArchivedWUsCacheElement> > cache;
    CriticalSection crit;
    size32_t cacheSize;
};

class CWsWorkunitsSoapBindingEx : public CWsWorkunitsSoapBinding
{
public:
    CWsWorkunitsSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel) : CWsWorkunitsSoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]/BatchWatch", process, name);
        m_bBatchWatch = cfg->getPropBool(xpath.str(), false);
    }

    virtual int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
    virtual int onGet(CHttpRequest* request, CHttpResponse* response);

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (!m_bBatchWatch)
        {
            IPropertyTree *folder = ensureNavFolder(data, "ECL Workunits", "ECL Workunits", NULL, false, 2);
            ensureNavLink(*folder, "Search", "/WsWorkunits/WUQuery?form_", "Search Workunits", NULL, NULL, 1);
            ensureNavLink(*folder, "Browse", "/WsWorkunits/WUQuery", "Browse Workunits", NULL, NULL, 2);

            IPropertyTree *folderQueryset = ensureNavFolder(data, "Query Sets", "Queryset Management", NULL, false, 3);
            ensureNavLink(*folderQueryset, "Browse", "/WsWorkunits/WUQuerySets", "Browse Querysets");
        }
    }
private:
    void addLogicalClusterByName(const char* clusterName, StringArray& clusters, StringBuffer& x);
    bool m_bBatchWatch;
};

class CWsWorkunitsEx : public CWsWorkunits
{
public:
   IMPLEMENT_IINTERFACE;
    virtual ~CWsWorkunitsEx(){};
    virtual void init(IPropertyTree *cfg, const char *process, const char *service);
    virtual void setContainer(IEspContainer * container)
    {
        CWsWorkunits::setContainer(container);
        m_sched.setContainer(container);
    }

    bool onWUQuery(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    bool onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp);
    bool onWUQuerysets(IEspContext &context, IEspWUQuerysetsRequest & req, IEspWUQuerysetsResponse & resp);
    bool onWUQuerysetDetails(IEspContext &context, IEspWUQuerySetDetailsRequest & req, IEspWUQuerySetDetailsResponse & resp);
    bool onWUQuerysetActionQueries(IEspContext &context, IEspWUQuerySetActionQueriesRequest & req, IEspWUQuerySetActionQueriesResponse & resp);
    bool onWUQuerysetActionAliases(IEspContext &context, IEspWUQuerySetActionAliasesRequest & req, IEspWUQuerySetActionAliasesResponse & resp);
    bool onWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp);
    bool onWUInfoDetails(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp);
    bool onWUFile(IEspContext &context,IEspWULogFileRequest &req, IEspWULogFileResponse &resp);
    bool onWUResult(IEspContext &context,IEspWUResultRequest &req, IEspWUResultResponse &resp);
    bool onWUResultView(IEspContext &context, IEspWUResultViewRequest &req, IEspWUResultViewResponse &resp);
    bool onWUResultSummary(IEspContext &context, IEspWUResultSummaryRequest &req, IEspWUResultSummaryResponse &resp);
    bool onWUResultBin(IEspContext &context, IEspWUResultBinRequest &req, IEspWUResultBinResponse &resp);
   bool onWUGraphInfo(IEspContext &context,IEspWUGraphInfoRequest &req, IEspWUGraphInfoResponse &resp);
   bool onWUGVCGraphInfo(IEspContext &context,IEspWUGVCGraphInfoRequest &req, IEspWUGVCGraphInfoResponse &resp);
    bool onWUProcessGraph(IEspContext &context,IEspWUProcessGraphRequest &req, IEspWUProcessGraphResponse &resp);
    bool onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp); 

    bool onWUAction(IEspContext &context, IEspWUActionRequest &req, IEspWUActionResponse &resp);
    bool onWUShowScheduled(IEspContext &context, IEspWUShowScheduledRequest &req, IEspWUShowScheduledResponse &resp);

    bool onWUUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp);
    bool onWUDelete(IEspContext &context, IEspWUDeleteRequest &req, IEspWUDeleteResponse &resp);
    bool onWUProtect(IEspContext &context, IEspWUProtectRequest &req, IEspWUProtectResponse &resp);

    bool onWUAbort(IEspContext &context, IEspWUAbortRequest &req, IEspWUAbortResponse &resp);
    bool onWUSchedule(IEspContext &context, IEspWUScheduleRequest &req, IEspWUScheduleResponse &resp);
    bool onWUSubmit(IEspContext &context, IEspWUSubmitRequest &req, IEspWUSubmitResponse &resp);
    bool onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp);
    bool onWUCreateAndUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp);
    bool onWUResubmit(IEspContext &context, IEspWUResubmitRequest &req, IEspWUResubmitResponse &resp);
    bool onWUPushEvent(IEspContext &context, IEspWUPushEventRequest &req, IEspWUPushEventResponse &resp);

    bool onWUExport(IEspContext &context, IEspWUExportRequest &req, IEspWUExportResponse &resp);
    bool onWUWaitCompiled(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp);
    bool onWUWaitComplete(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp);

    bool onWUSyntaxCheckECL(IEspContext &context, IEspWUSyntaxCheckRequest &req, IEspWUSyntaxCheckResponse &resp);
    bool onWUCompileECL(IEspContext &context, IEspWUCompileECLRequest &req, IEspWUCompileECLResponse &resp);
    bool onWUJobList(IEspContext &context, IEspWUJobListRequest &req, IEspWUJobListResponse &resp);
    bool onWUGetGraph(IEspContext& context, IEspWUGetGraphRequest& req, IEspWUGetGraphResponse& resp);
    bool onWUGraphTiming(IEspContext& context, IEspWUGraphTimingRequest& req, IEspWUGraphTimingResponse& resp);
    bool onWUGetDependancyTrees(IEspContext& context, IEspWUGetDependancyTreesRequest& req, IEspWUGetDependancyTreesResponse& resp);

    bool onWUListLocalFileRequired(IEspContext& context, IEspWUListLocalFileRequiredRequest& req, IEspWUListLocalFileRequiredResponse& resp);
    bool onWUAddLocalFileToWorkunit(IEspContext& context, IEspWUAddLocalFileToWorkunitRequest& req, IEspWUAddLocalFileToWorkunitResponse& resp);

    bool onWUClusterJobQueueXLS(IEspContext &context, IEspWUClusterJobQueueXLSRequest &req, IEspWUClusterJobQueueXLSResponse &resp);
    bool onWUClusterJobQueueLOG(IEspContext &context,IEspWUClusterJobQueueLOGRequest  &req, IEspWUClusterJobQueueLOGResponse &resp);
    bool onWUClusterJobXLS(IEspContext &context, IEspWUClusterJobXLSRequest &req, IEspWUClusterJobXLSResponse &resp);
    bool onWUClusterJobSummaryXLS(IEspContext &context, IEspWUClusterJobSummaryXLSRequest &req, IEspWUClusterJobSummaryXLSResponse &resp);

    bool onWUCDebug(IEspContext &context, IEspWUDebugRequest &req, IEspWUDebugResponse &resp);

    static void xsltTransform(const char* xml,const char* xsl,IProperties *params,StringBuffer& ret);

private:
    void getHelpFiles(double version, IConstWUQuery* query, WUFileType type, IArrayOf<IEspECLHelpFile>& helpers);
    void getWorkunitCluster(IEspContext &context, const char* wuid, StringBuffer& cluster, bool checkArchiveWUs = false);
    void getWorkunitXml(IEspContext &context, const char* wuid, const char* plainText, MemoryBuffer& buf);
    void getWorkunitCpp(IEspContext &context, const char* cppname, const char* description, const char* ipAddress,MemoryBuffer& buf);
    void getWorkunitResTxt(IEspContext &context, const char* wuid,MemoryBuffer& buf);
    void getWorkunitDll(IEspContext &context, const char* wuid,MemoryBuffer& buf);
    void getWorkunitArchiveQuery(IEspContext &context, const char* wuid,MemoryBuffer& buf);
    void getWorkunitResults(IEspContext &context, const char* wuid, unsigned index,__int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf);
    void getWorkunitEclAgentLog(IEspContext &context, const char *wuid, MemoryBuffer& log);
    void getWorkunitThorLog(IEspContext &context, const char* type, const char *wuid,MemoryBuffer& log);
    void getWorkunitThorSlaveLog(IEspContext &context, const char *wuid,const char *slaveip,MemoryBuffer& buf);
    void getResult(IEspContext &context,IConstWUResult &r,IArrayOf<IEspECLResult>& results, const char* wuid = NULL, bool SuppressSchemas=false);
    void getInfo(IEspContext &context,const char* wuid,IEspECLWorkunit *info, bool bTruncateEclTo64k, bool IncludeExceptions=true, bool IncludeGraphs=true, bool IncludeSourceFiles=true, bool IncludeResults=true, bool IncludeVariables=true, bool IncludeTimers=true, bool IncludeDebugValues=true, bool IncludeApplicationValues=true, bool IncludeWorkflows=true, bool SuppressSchemas=false, StringArray *resultViews=NULL);
    bool getInfoFromSasha(IEspContext &context,const char *sashaServer,const char* wuid,IEspECLWorkunit *info);
    void getResultView(INewResultSet* result, __int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf);
    void getFileResults(IEspContext &context, const char* logicalName, const char* cluster,__int64 start, unsigned& count,__int64& total,IStringVal& resname,bool raw,MemoryBuffer& buf);
    void getScheduledWUs(IEspContext &context, const char *serverName, const char *eventName, IArrayOf<IEspScheduledWU> & results);
    void getArchivedWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp);

    void submitWU(IEspContext& context, const char* wuid, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow);
    void resubmitWU(IEspContext& context, const char* wuid, const char* cluster, IStringVal& newWuid, bool forceRecompile);
    void scheduleWU(IEspContext& context, const char* wuid, const char* cluster, const char* when, const char* snapshot, int maxruntime);
    void pauseWU(IEspContext& context, const char* wuid, bool now = true);
    void resumeWU(IEspContext& context, const char* wuid);

    void processWorkunit(IConstWorkUnit *workunit, const char* wuid, SCMStringBuffer &queryName,  SCMStringBuffer &clusterName, SCMStringBuffer &querySetName, int activateOption);

    bool doAction(IEspContext&, StringArray& wuids , int action, IProperties* params=NULL, IArrayOf<IConstWUActionResult>* results=NULL);

    void getPopups(IEspContext &context, const char *wuid, const char *graphname, const char* popupId, StringBuffer &script);
    void getGJSGraph(IEspContext &context, const char *wuid, const char *graphname, IProperties* params, StringBuffer &script);

    void doWUQueryByXPath(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    void doWUQueryWithSort(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp);
    void doWUQueryForArchivedWUs(IEspContext &context, IEspWUQueryRequest &req, IEspWUQueryResponse &resp, const char *sashaAddress);
    void addToQueryString(StringBuffer &queryString, const char *name, const char *value);

    void gatherFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    void gatherChildFields(IArrayOf<IEspECLSchemaItem>& schemas, IHqlExpression * expr, bool isConditional);
    bool getResultSchemas(IConstWUResult &r, IArrayOf<IEspECLSchemaItem>& schemas);
    bool checkFileContent(IEspContext &context, const char * logicalName);

    void ensureArchivedWorkunitAccess(IEspContext& context, const char *owner, SecAccessFlags minAccess);
    void ensureWorkunitAccess(IEspContext& context, IConstWorkUnit& wu, SecAccessFlags minAccess);
    void ensureWorkunitAccess(IEspContext& context, const char* wuid, SecAccessFlags minAccess)
    {
        CWUWrapper wu(wuid, context);
        ensureWorkunitAccess(context, *wu, minAccess);
    }
    void lookupAccess(IEspContext& context, SecAccessFlags& accessOwn, SecAccessFlags& accessOthers);
    SecAccessFlags getAccess(IEspContext& context, IConstWorkUnit& wu, 
                             SecAccessFlags accessOwn, SecAccessFlags accessOthers);
     SecAccessFlags getWorkunitAccess(IEspContext& context, IConstWorkUnit& wu);

    bool getClusterJobQueueXLS(double version, IStringVal &ret, const char* cluster, const char* startDate, const char* endDate, const char* showType);
    int loadFile(const char* fname, int& len, unsigned char* &buf, bool binary=true);

    void addSubFiles(IPropertyTreeIterator* f, IEspECLSourceFile* eclSuperFile, StringArray& fileNames);
    void openSaveFile(IEspContext &context, int opt, const char* filename, const char* origMimeType, MemoryBuffer& buf, IEspWULogFileResponse &resp);
#if 0 //not use for now
    void getSubFiles(IUserDescriptor* userdesc, const char *fileName, IEspECLSourceFile* eclSourceFile0);
    bool checkFileInECLSourceFile(const char* file, IConstECLSourceFile& eclfile);
    bool checkFileInECLSourceFiles(const char* file, IArrayOf<IEspECLSourceFile>& eclfiles);
#endif

private:
    StringBuffer m_GraphUpdateGvcXSLT;
    bool m_allowNewRoxieOnDemandQuery;
    unsigned m_AWUS_cache_minutes;

    Owned<DataCache> m_dataCache;
    Owned<ArchivedWUsCache> m_archivedWUsCache;
    WUSchedule m_sched;
};


#endif 

