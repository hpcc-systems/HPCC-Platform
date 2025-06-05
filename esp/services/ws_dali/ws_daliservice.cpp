/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_daliservice.hpp"
#include "jlib.hpp"
#include "dautils.hpp"
#include "daadmin.hpp"
#include "dadiags.hpp"
#include "dastats.hpp"

using namespace daadmin;

#define REQPATH_EXPORTSDSDATA "/WSDali/Export"

class CDirectStatsRecorder : public CInterface, implements IGlobalMetricRecorder
{
private:
    IArrayOf<IEspDaliCategoryStats>& categoryStats;
    IArrayOf<IEspDaliStatEntry>& systemMetrics;
    const char* collectionTime;

public:
    IMPLEMENT_IINTERFACE;

    CDirectStatsRecorder(IArrayOf<IEspDaliCategoryStats>& _categoryStats, IArrayOf<IEspDaliStatEntry>& _systemMetrics, const char* _collectionTime)
        : categoryStats(_categoryStats), systemMetrics(_systemMetrics), collectionTime(_collectionTime)
    {
    }

    virtual void processGlobalStatistics(const char * category, const MetricsDimensionList & dimensions, const char * startTime, const char * endTime, const GlobalStatisticsList & stats) override
    {
        // Create category stats entry
        Owned<IEspDaliCategoryStats> categoryEntry = createDaliCategoryStats();
        categoryEntry->setCategory(category ? category : "");
        categoryEntry->setStartTime(startTime ? startTime : "");
        categoryEntry->setEndTime(endTime ? endTime : "");
        
        // Calculate aggregate values from stats
        __int64 totalCount = 0;
        __int64 minValue = LLONG_MAX;
        __int64 maxValue = LLONG_MIN;
        __int64 sum = 0;
        
        IArrayOf<IEspDaliStatEntry> statEntries;
        for (const auto & stat : stats)
        {
            const char* statName = queryStatisticName(stat.first);
            __int64 value = stat.second;
            
            // Create individual stat entry
            Owned<IEspDaliStatEntry> statEntry = createDaliStatEntry();
            statEntry->setName(statName ? statName : "unknown");
            statEntry->setValue(VStringBuffer("%lld", value));
            statEntry->setType("counter");
            statEntry->setTimestamp(collectionTime);
            
            // Add dimensions as metadata if needed
            if (!dimensions.empty())
            {
                StringBuffer dimensionStr;
                bool first = true;
                for (const auto & dim : dimensions)
                {
                    if (!first) dimensionStr.append(",");
                    dimensionStr.appendf("%s=%s", dim.first, dim.second);
                    first = false;
                }
                // Note: IEspDaliStatEntry doesn't have setMetadata method
                // Dimensions info is already captured in the category level
            }
            
            statEntries.append(*statEntry.getLink());
            systemMetrics.append(*statEntry.getLink());
            
            // Update aggregates
            totalCount++;
            sum += value;
            if (value < minValue) minValue = value;
            if (value > maxValue) maxValue = value;
        }
        
        // Set aggregate values
        categoryEntry->setTotalCount(totalCount);
        categoryEntry->setAverageValue(totalCount > 0 ? sum / totalCount : 0);
        categoryEntry->setMinValue(minValue != LLONG_MAX ? minValue : 0);
        categoryEntry->setMaxValue(maxValue != LLONG_MIN ? maxValue : 0);
        categoryEntry->setStatEntries(statEntries);
        
        categoryStats.append(*categoryEntry.getLink());
    }
};

void CWSDaliEx::init(IPropertyTree* cfg, const char* process, const char* service)
{
    espProcess.set(process);
}

int CWSDaliSoapBindingEx::onGet(CHttpRequest* request, CHttpResponse* response)
{
    try
    {
#ifdef _USE_OPENLDAP
        request->queryContext()->ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
        if (wsdService->isDaliDetached())
            throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");

        StringBuffer path;
        request->getPath(path);

        if (!strnicmp(path.str(), REQPATH_EXPORTSDSDATA, sizeof(REQPATH_EXPORTSDSDATA) - 1))
        {
            exportSDSData(request, response);
            return 0;
        }
    }
    catch(IException* e)
    {
        onGetException(*request->queryContext(), request, response, *e);
        FORWARDEXCEPTION(*request->queryContext(), e,  ECLWATCH_INTERNAL_ERROR);
    }

    return CWSDaliSoapBinding::onGet(request,response);
}

void CWSDaliSoapBindingEx::exportSDSData(CHttpRequest* request, CHttpResponse* response)
{
    StringBuffer path, xpath, safeReq;
    request->getParameter("Path", path);
    request->getParameter("Safe", safeReq);
    Owned<IRemoteConnection> conn = connectXPathOrFile(path, strToBool(safeReq), xpath);
    if (!conn)
        throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Failed to connect Dali.");

    Owned<IPropertyTree> root = conn->getRoot();
    response->setContent(root);

    //Set "Content-disposition" header
    CDateTime dt;
    dt.setNow();
    StringBuffer headerStr;
    headerStr.appendf("attachment;filename=sds_%u.%llu.tmp", (unsigned)GetCurrentProcessId(), (unsigned __int64)dt.getSimple());
    IEspContext* context = request->queryContext();
    context->addCustomerHeader("Content-disposition", headerStr.str());

    response->setContentType(HTTP_TYPE_OCTET_STREAM);
    response->send();
}

void CWSDaliEx::checkAccess(IEspContext& context)
{
#ifdef _USE_OPENLDAP
    context.ensureSuperUser(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Access denied, administrators only.");
#endif
    if (isDaliDetached())
        throw makeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Dali detached.");
}

bool CWSDaliEx::onSetValue(IEspContext& context, IEspSetValueRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");
        const char* value = req.getValue();
        if (isEmptyString(value))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data value not specified.");

        StringBuffer oldValue, result;
        setValue(path, value, oldValue);
        if (oldValue.isEmpty())
            result.appendf("Changed %s to '%s'", path, value);
        else
            result.appendf("Changed %s from '%s' to '%s'", path, oldValue.str(), value);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetValue(IEspContext& context, IEspGetValueRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer value;
        getValue(path, value);
        resp.setResult(value);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onImport(IEspContext& context, IEspImportRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* xml = req.getXML();
        const char* path = req.getPath();
        if (isEmptyString(xml))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data XML not specified.");
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer result;
        if (importFromXML(path, xml, req.getAdd(), result))
            result.appendf(" Branch %s loaded.", path);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDelete(IEspContext& context, IEspDeleteRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        StringBuffer result;
        erase(path, false, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onAdd(IEspContext& context, IEspAddRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");
        const char* value = req.getValue();

        StringBuffer result;
        add(path, isEmptyString(value) ? nullptr : value, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onCount(IEspContext& context, IEspCountRequest& req, IEspCountResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* path = req.getPath();
        if (isEmptyString(path))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Data path not specified.");

        resp.setResult(count(path));
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IUserDescriptor* CWSDaliEx::createUserDesc(IEspContext& context)
{
    StringBuffer username;
    context.getUserID(username);
    if (username.isEmpty())
        return nullptr;

    Owned<IUserDescriptor> userdesc = createUserDescriptor();
    userdesc->set(username.str(), context.queryPassword(), context.querySignature());
    return userdesc.getClear();
}

bool CWSDaliEx::onDFSLS(IEspContext& context, IEspDFSLSRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer options;
        if (req.getRecursively())
            options.append("r");
        if (!req.getPathAndNameOnly())
            options.append("l");
        if (req.getIncludeSubFileInfo())
            options.append("s");

        StringBuffer result;
        dfsLs(req.getName(), options, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSCSV(IEspContext& context, IEspGetDFSCSVRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfscsv(req.getLogicalNameMask(), userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSExists(IEspContext& context, IEspDFSExistsRequest& req, IEspBooleanResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);
        resp.setResult(dfsexists(fileName, userDesc) == 0 ? true : false);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetLogicalFile(IEspContext& context, IEspGetLogicalFileRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsfile(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetLogicalFilePart(IEspContext& context, IEspGetLogicalFilePartRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");
        if (req.getPartNumber_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part number not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfspart(fileName, userDesc, req.getPartNumber(), result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetLogicalFilePartAttr(IEspContext& context, IEspSetLogicalFilePartAttrRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");
        const char* attr = req.getAttr();
        if (isEmptyString(attr))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part attribute name not specified.");
        if (req.getPartNumber_isNull())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Part number not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        setdfspartattr(fileName, req.getPartNumber(), attr, req.getValue(), userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSMap(IEspContext& context, IEspGetDFSMapRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsmap(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetDFSParents(IEspContext& context, IEspGetDFSParentsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsparents(fileName, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDFSCheck(IEspContext& context, IEspDFSCheckRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        dfsCheck(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetProtected(IEspContext& context, IEspSetProtectedRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);
                
        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        setprotect(fileName, callerId, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetUnprotected(IEspContext& context, IEspSetUnprotectedRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        Owned<IUserDescriptor> userDesc = createUserDesc(context);

        StringBuffer result;
        unprotect(fileName, callerId, userDesc, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetProtectedList(IEspContext& context, IEspGetProtectedListRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* fileName = req.getFileName();
        if (isEmptyString(fileName))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "File name not specified.");

        const char* callerId = req.getCallerId();
        if (isEmptyString(callerId))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Caller Id not specified.");

        StringBuffer result;
        listprotect(fileName, callerId, result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetConnections(IEspContext& context, IEspGetConnectionsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        querySDS().getConnections(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetClients(IEspContext& context, IEspGetClientsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        getDaliDiagnosticValue("clients", result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetGlobalStats(IEspContext& context, IEspGetGlobalStatsRequest& req, IEspGetGlobalStatsResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* category = req.getCategory();
        const IArrayOf<IConstDaliDimension>& dimensionsArray = req.getDimensions();
        const char* startTimeStr = req.getStartTime();
        const char* endTimeStr = req.getEndTime();

        // Parse dimensions from the array
        MetricsDimensionList dimensions;
        ForEachItemIn(i, dimensionsArray)
        {
            IConstDaliDimension& dim = dimensionsArray.item(i);
            const char* name = dim.getName();
            const char* value = dim.getValue();
            if (name && *name && value && *value)
            {
                dimensions.emplace_back(name, value);
            }
        }

        // Parse time ranges
        CDateTime fromTime, toTime;
        if (startTimeStr && *startTimeStr)
            fromTime.setString(startTimeStr);
        else
            fromTime.setNow(); // Default to current time
            
        if (endTimeStr && *endTimeStr)
            toTime.setString(endTimeStr);
        else
        {
            toTime.setNow(); // Default to current time
            if (!startTimeStr || !*startTimeStr)
            {
                // If no start time either, go back 1 hour
                fromTime.adjustTime(-60 * 60);
            }
        }

        // Create collection time string
        StringBuffer collectionTimeStr;
        CDateTime currentTime;
        currentTime.setNow();
        currentTime.getString(collectionTimeStr);

        // Create response arrays
        IArrayOf<IEspDaliCategoryStats> globalCategories;
        IArrayOf<IEspDaliStatEntry> systemMetrics;
        
        // Create direct recorder that populates response structures
        CDirectStatsRecorder recorder(globalCategories, systemMetrics, collectionTimeStr.str());
        gatherGlobalMetrics(category, dimensions, fromTime, toTime, recorder);

        // Set basic response fields
        resp.setSummary("Global statistics for Dali system");
        resp.setCollectionTime(collectionTimeStr.str());
        resp.setTotalDataPoints(systemMetrics.length());

        // Create cluster stats (keeping existing logic)
        IArrayOf<IEspDaliClusterStats> clusterStats;
        Owned<IEspDaliClusterStats> cluster = createDaliClusterStats();
        cluster->setClusterName("DefaultCluster");
        cluster->setTotalNodes(1);
        cluster->setActiveNodes(1);
        cluster->setTotalConnections(0);
        cluster->setTotalMemoryUsage(0);
        cluster->setLastUpdated(collectionTimeStr.str());
        
        IArrayOf<IEspDaliNodeStats> nodes;
        IArrayOf<IEspDaliCategoryStats> aggregateStats;
        cluster->setNodes(nodes);
        cluster->setAggregateStats(aggregateStats);
        clusterStats.append(*cluster.getLink());

        // Set the arrays in the response
        resp.setClusterStats(clusterStats);
        resp.setGlobalCategories(globalCategories);
        resp.setSystemMetrics(systemMetrics);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetSDSStats(IEspContext& context, IEspGetSDSStatsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        querySDS().getUsageStats(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onGetSDSSubscribers(IEspContext& context, IEspGetSDSSubscribersRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        querySDS().getSubscribers(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onDisconnectClientConnection(IEspContext& context, IEspDisconnectClientConnectionRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* ep = req.getEndpoint();
        if (isEmptyString(ep))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Endpoint not specified.");

        MemoryBuffer mb;
        mb.append("disconnect").append(ep);
        getDaliDiagnosticValue(mb);

        VStringBuffer result("DisconnectClientConnection called for %s.", ep);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onListSDSLocks(IEspContext& context, IEspListSDSLocksRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        Owned<ILockInfoCollection> lockInfoCollection = querySDS().getLocks();
        StringBuffer result;
        lockInfoCollection->toString(result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onUnlockSDSLock(IEspContext& context, IEspUnlockSDSLockRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        const char* connectionIdHex = req.getConnectionID();
        if (isEmptyString(connectionIdHex))
            throw makeStringException(ECLWATCH_INVALID_INPUT, "ConnectionID not specified.");

        MemoryBuffer mb;
        mb.append("unlock").append(strtoll(connectionIdHex, nullptr, 16)).append(req.getClose());
        getDaliDiagnosticValue(mb);

        bool success = false;
        mb.read(success);
        if (!success)
            resp.setResult("Lock not found");
        else
        {
            StringBuffer result(("Lock successfully removed: "));
            mb.read(result);
            resp.setResult(result);
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSaveSDSStore(IEspContext& context, IEspSaveSDSStoreRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        MemoryBuffer mb;
        mb.append("save");
        getDaliDiagnosticValue(mb);
        resp.setResult("SaveSDSStore called.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetTraceTransactions(IEspContext& context, IEspSetTraceTransactionsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        const char* cmd = "settracetransactions";
        getDaliDiagnosticValue(cmd, result);
        resp.setResult(result.isEmpty() ? "SetTraceTransactions called." : result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onSetTraceSlowTransactions(IEspContext& context, IEspSetTraceSlowTransactionsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        MemoryBuffer mb;
        mb.append("settraceslowtransactions");
        mb.append(req.getSlowThresholdMS());
        getDaliDiagnosticValue(mb);

        StringAttr result;
        mb.read(result);
        resp.setResult(result.isEmpty() ? "SetTraceSlowTransactions called." : result.get());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWSDaliEx::onClearTraceTransactions(IEspContext& context, IEspClearTraceTransactionsRequest& req, IEspResultResponse& resp)
{
    try
    {
        checkAccess(context);

        StringBuffer result;
        const char* cmd = "cleartracetransactions";
        getDaliDiagnosticValue(cmd, result);
        resp.setResult(result.isEmpty() ? "ClearTraceTransactions called." : result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
