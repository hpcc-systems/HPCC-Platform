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

#include "ws_workunitsService.hpp"
#include "ws_fs.hpp"

#include "jlib.hpp"
#include "jflz.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dadfs.hpp"
#include "daaudit.hpp"
#include "exception_util.hpp"
#include "wujobq.hpp"
#include "eventqueue.hpp"
#include "fileview.hpp"
#include "hqlerror.hpp"
#include "sacmd.hpp"
#include "wuwebview.hpp"
#include "portlist.h"
#include "dllserver.hpp"
#include "schedulectrl.hpp"
#include "scheduleread.hpp"
#include "dadfs.hpp"
#include "dfuwu.hpp"
#include "thorplugin.hpp"
#include "roxiecontrol.hpp"

#include "package.h"

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

#define ESP_WORKUNIT_DIR "workunits/"

#define SDS_LOCK_TIMEOUT (5*60*1000) // 5 mins
const unsigned CHECK_QUERY_STATUS_THREAD_POOL_SIZE = 25;

class ExecuteExistingQueryInfo
{
public:
    ExecuteExistingQueryInfo(IConstWorkUnit *cw)
    {
        const char *name = cw->queryJobName();
        const char *div = strchr(name, '.');
        if (div)
        {
            queryset.set(name, div-name);
            query.set(div+1);
        }
    }

public:
    StringAttr queryset;
    StringAttr query;
};

typedef enum _WuActionType
{
    ActionDelete=0,
    ActionProtect,
    ActionAbort,
    ActionRestore,
    ActionEventSchedule,
    ActionEventDeschedule,
    ActionChangeState,
    ActionPause,
    ActionPauseNow,
    ActionResume,
    ActionUnknown
} WsWuActionType;

void setActionResult(const char* wuid, int action, const char* result, StringBuffer& strAction, IArrayOf<IConstWUActionResult>* results)
{
    if (!results || !wuid || !*wuid || !result || !*result)
        return;

    switch(action)
    {
    case ActionDelete:
    {
        strAction = "Delete";
        break;
    }
    case ActionProtect:
    {
        strAction = "Protect";
        break;
    }
    case ActionAbort:
    {
        strAction = "Abort";
        break;
    }
    case ActionRestore:
    {
        strAction = "Restore";
        break;
    }
    case ActionEventSchedule:
    {
        strAction = "EventSchedule";
        break;
    }
    case ActionEventDeschedule:
    {
        strAction = "EventDeschedule";
        break;
    }
    case ActionChangeState:
    {
        strAction = "ChangeState";
        break;
    }
    case ActionPause:
    {
        strAction = "Pause";
        break;
    }
    case ActionPauseNow:
    {
        strAction = "PauseNow";
        break;
    }
    case ActionResume:
    {
        strAction = "Resume";
        break;
    }
    default:
    {
        strAction = "Unknown";
        break;
    }
    }

    Owned<IEspWUActionResult> res = createWUActionResult("", "");
    res->setWuid(wuid);
    res->setAction(strAction.str());
    res->setResult(result);
    results->append(*res.getClear());
}

bool doAction(IEspContext& context, StringArray& wuids, int action, IProperties* params, IArrayOf<IConstWUActionResult>* results)
{
    if (!wuids.length())
        return true;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());

    bool bAllSuccess = true;
    for(aindex_t i=0; i<wuids.length();i++)
    {
        StringBuffer strAction;
        StringBuffer wuidStr = wuids.item(i);
        const char* wuid = wuidStr.trim().str();
        if (isEmpty(wuid))
        {
            WARNLOG("Empty Workunit ID");
            continue;
        }

        try
        {
            if (!looksLikeAWuid(wuid, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid);

            if ((action == ActionRestore) || (action == ActionEventDeschedule))
            {
                switch(action)
                {
                case ActionRestore:
                {
                    SocketEndpoint ep;
                    if (params->hasProp("sashaServerIP"))
                        ep.set(params->queryProp("sashaServerIP"), params->getPropInt("sashaServerPort"));
                    else
                        getSashaNode(ep);

                    Owned<ISashaCommand> cmd = createSashaCommand();
                    cmd->setAction(SCA_RESTORE);
                    cmd->addId(wuid);

                    Owned<INode> node = createINode(ep);
                    if (!node)
                        throw MakeStringException(ECLWATCH_INODE_NOT_FOUND,"INode not found.");

                    StringBuffer s;
                    if (!cmd->send(node, 1*60*1000))
                        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to Archive server at %s.", ep.getUrlStr(s).str());

                    if (cmd->numIds()==0)
                        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Could not Archive/restore %s",wuid);

                    StringBuffer reply;
                    cmd->getId(0,reply);

                    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    ensureWsWorkunitAccess(context, wuid, SecAccess_Write);
                    break;
                }
                case ActionEventDeschedule:
                    if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Full, false)
                        || !context.validateFeatureAccess(OTHERS_WU_ACCESS, SecAccess_Full, false))
                        ensureWsWorkunitAccess(context, wuid, SecAccess_Full);
                    descheduleWorkunit(wuid);
                    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    break;
                }
            }
            else
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
                if(!cw)
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid);

                if ((action == ActionDelete) && (cw->getState() == WUStateWait))
                    throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Cannot delete a workunit which is in a 'Wait' status.");

                switch(action)
                {
                case ActionPause:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionPause);
                    break;
                }
                case ActionPauseNow:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionPauseNow);
                   break;
                }
                case ActionResume:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionResume);
                   break;
                }
                case ActionDelete:
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    {
                        cw.clear();
                        factory->deleteWorkUnit(wuid);
                        AuditSystemAccess(context.queryUserId(), true, "Deleted %s", wuid);
                    }
                    break;
                case ActionAbort:
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    {
                        if (cw->getState() == WUStateWait)
                        {
                            WorkunitUpdate wu(&cw->lock());
                            wu->deschedule();
                            wu->setState(WUStateAborted);
                        }
                        else
                            secAbortWorkUnit(wuid, *context.querySecManager(), *context.queryUser());
                        AuditSystemAccess(context.queryUserId(), true, "Aborted %s", wuid);
                    }
                    break;
                case ActionProtect:
                    cw->protect(!params || params->getPropBool("Protect",true));
                    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    break;
                case ActionChangeState:
                    {
                        if (params)
                        {
                            WUState state = (WUState) params->getPropInt("State");
                            if (state > WUStateUnknown && state < WUStateSize)
                            {
                                WorkunitUpdate wu(&cw->lock());
                                wu->setState(state);
                                AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                            }
                        }
                    }
                    break;
                case ActionEventSchedule:
                    {
                        WorkunitUpdate wu(&cw->lock());
                        wu->schedule();
                        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    }
                    break;
                }
            }
            setActionResult(wuid, action, "Success", strAction, results);
        }
        catch (IException *e)
        {
            bAllSuccess = false;
            StringBuffer eMsg;
            StringBuffer failedMsg("Failed: ");
            setActionResult(wuid, action, failedMsg.append(e->errorMessage(eMsg)).str(), strAction, results);
            WARNLOG("Failed to %s for workunit: %s, %s", strAction.str(), wuid, eMsg.str());
            AuditSystemAccess(context.queryUserId(), false, "Failed to %s %s", strAction.str(), wuid);
            e->Release();
            continue;
        }
        catch (...)
        {
            bAllSuccess = false;
            StringBuffer failedMsg;
            failedMsg.appendf("Unknown exception");
            setActionResult(wuid, action, failedMsg.str(), strAction, results);
            WARNLOG("Failed to %s for workunit: %s, %s", strAction.str(), wuid, failedMsg.str());
            AuditSystemAccess(context.queryUserId(), false, "Failed to %s %s", strAction.str(), wuid);
            continue;
        }
    }

    int timeToWait = 0;
    if (params)
         timeToWait = params->getPropInt("BlockTillFinishTimer");

    if (timeToWait != 0)
    {
        for(aindex_t i=0; i<wuids.length();i++)
        {
            const char* wuid=wuids.item(i);
            if (isEmpty(wuid))
                continue;
            waitForWorkUnitToComplete(wuid, timeToWait);
        }
    }
    return bAllSuccess;
}

static void checkUpdateQuerysetLibraries()
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!globalLock)
        return;

    IPropertyTree *root = globalLock->queryRoot();
    if (!root)
        return;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IPropertyTreeIterator> querySets = root->getElements("QuerySet");
    ForEach(*querySets)
    {
        IPropertyTree &querySet = querySets->query();
        if (querySet.hasProp("@updatedLibraries")) //only need to do this once, then publish and copy will keep up to date
            continue;
        Owned<IPropertyTreeIterator> queries = querySet.getElements("Query");
        ForEach(*queries)
        {
            IPropertyTree &query = queries->query();
            if (query.hasProp("@libCount"))
                continue;
            const char *wuid = query.queryProp("@wuid");
            if (!wuid || !*wuid)
                continue;
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
            if (!cw)
                continue;
            checkAddLibrariesToQueryEntry(&query, cw);
        }
        querySet.setPropBool("@updatedLibraries", true);
    }
}

MapStringTo<int> wuActionTable;

void CWsWorkunitsEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if (!daliClientActive())
    {
        ERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }
    setPasswordsFromSDS();

    DBGLOG("Initializing %s service [process = %s]", service, process);

    checkUpdateQuerysetLibraries();
    refreshValidClusters();

    daliServers.set(cfg->queryProp("Software/EspProcess/@daliServers"));
    const char *computer = cfg->queryProp("Software/EspProcess/@computer");
    if (daliServers.isEmpty() || !computer || streq(computer, "localhost")) //otherwise can't assume environment "." netAddresses are the same as my address
        queryHostIP().getIpText(envLocalAddress);
    else
    {
        //a bit weird, but other netAddresses in the environment are not the same localhost as this server
        //use the address of the DALI
        const char *finger = daliServers.get();
        while (*finger && !strchr(":;,", *finger))
            envLocalAddress.append(*finger++);
    }

    wuActionTable.setValue("delete", ActionDelete);
    wuActionTable.setValue("abort", ActionAbort);
    wuActionTable.setValue("pausenow", ActionPauseNow);
    wuActionTable.setValue("pause", ActionPause);
    wuActionTable.setValue("resume", ActionResume);
    wuActionTable.setValue("protect", ActionProtect);
    wuActionTable.setValue("unprotect", ActionProtect);
    wuActionTable.setValue("restore", ActionRestore);
    wuActionTable.setValue("reschedule", ActionEventSchedule);
    wuActionTable.setValue("deschedule", ActionEventDeschedule);
    wuActionTable.setValue("settofailed", ActionChangeState);

    awusCacheMinutes = AWUS_CACHE_MIN_DEFAULT;
    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/AWUsCacheMinutes", process, service);
    cfg->getPropInt(xpath.str(), awusCacheMinutes);

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/serverForArchivedECLWU/@netAddress", process, service);
    if (cfg->hasProp(xpath.str()))
    {
        sashaServerIp.set(cfg->queryProp(xpath.str()));
        xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/serverForArchivedECLWU/@port", process, service);
        sashaServerPort = cfg->getPropInt(xpath.str(), DEFAULT_SASHA_PORT);
    }

    maxRequestEntityLength = cfg->getPropInt("Software[1]/EspProcess[1]/EspProtocol[@type='http_protocol'][1]/@maxRequestEntityLength");
    directories.set(cfg->queryPropTree("Software/Directories"));

    const char *name = cfg->queryProp("Software/EspProcess/@name");
    getConfigurationDirectory(directories, "query", "esp", name ? name : "esp", queryDirectory);
    recursiveCreateDirectory(queryDirectory.str());

    dataCache.setown(new DataCache(DATA_SIZE));
    archivedWuCache.setown(new ArchivedWuCache(AWUS_CACHE_SIZE));
    wuArchiveCache.setown(new WUArchiveCache(WUARCHIVE_CACHE_SIZE));

    //Create a folder for temporarily holding gzip files by WUResultBin()
    Owned<IFile> tmpdir = createIFile(TEMPZIPDIR);
    if(!tmpdir->exists())
        tmpdir->createDirectory();

    recursiveCreateDirectory(ESP_WORKUNIT_DIR);

    m_sched.start();
    filesInUse.subscribe();

    //Start thread pool
    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/ClusterQueryStateThreadPoolSize", process, service);
    Owned<CClusterQueryStateThreadFactory> threadFactory = new CClusterQueryStateThreadFactory();
    clusterQueryStatePool.setown(createThreadPool("CheckAndSetClusterQueryState Thread Pool", threadFactory, NULL,
            cfg->getPropInt(xpath.str(), CHECK_QUERY_STATUS_THREAD_POOL_SIZE)));
}

void CWsWorkunitsEx::refreshValidClusters()
{
    validClusters.kill();
    Owned<IStringIterator> it = getTargetClusters(NULL, NULL);
    ForEach(*it)
    {
        SCMStringBuffer s;
        IStringVal &val = it->str(s);
        bool* found = validClusters.getValue(val.str());
        if (!found || !*found)
            validClusters.setValue(val.str(), true);
    }
}

bool CWsWorkunitsEx::isValidCluster(const char *cluster)
{
    if (!cluster || !*cluster)
        return false;
    CriticalBlock block(crit);
    bool* found = validClusters.getValue(cluster);
    if (found && *found)
        return true;
    if (validateTargetClusterName(cluster))
    {
        refreshValidClusters();
        return true;
    }
    return false;
}

bool CWsWorkunitsEx::onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        NewWsWorkunit wu(context);
        resp.updateWorkunit().setWuid(wu->queryWuid());
        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wu->queryWuid());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

static bool origValueChanged(const char *newValue, const char *origValue, StringBuffer &s, bool nillable=true)
{
    if (!nillable && isEmpty(newValue))
        return false;
    if(newValue && origValue)
    {
        if (!streq(origValue, newValue))
        {
            s.append(newValue).trim();
            return true;
        }
        return false;
    }
    if (newValue)
    {
        s.append(newValue).trim();
        return true;
    }
    return (origValue!=NULL);
}

bool CWsWorkunitsEx::onWUUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUUpdate", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Write);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        if(req.getProtected() != req.getProtectedOrig())
        {
            cw->protect(req.getProtected());
            cw.clear();
            cw.setown(factory->openWorkUnit(wuid.str()));
            if(!cw)
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        }

        if ((req.getState() == WUStateRunning)||(req.getState() == WUStateDebugPaused)||(req.getState() == WUStateDebugRunning))
        {
            WsWuInfo winfo(context, cw);
            winfo.getInfo(resp.updateWorkunit(), WUINFO_All);
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(wuid).str());
            AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
            return true;
        }

        WorkunitUpdate wu(&cw->lock());
        if(!req.getState_isNull() && (req.getStateOrig_isNull() || req.getState() != req.getStateOrig()))
        {
            if (!req.getStateOrig_isNull() && cw->getState() != (WUState) req.getStateOrig())
                throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Cannot update workunit %s because its state has been changed internally. Please refresh the page and try again.", wuid.str());

            WUState state = (WUState) req.getState();
            if(state < WUStateSize)
                wu->setState(state);
        }

        StringBuffer s;
        if (origValueChanged(req.getJobname(), req.getJobnameOrig(), s))
            wu->setJobName(s.trim().str());
        if (origValueChanged(req.getDescription(), req.getDescriptionOrig(), s.clear()))
            wu->setDebugValue("description", (req.getDescription() && *req.getDescription()) ? s.trim().str() : NULL, true);

        double version = context.getClientVersion();
        if (version > 1.04)
        {
            if (origValueChanged(req.getClusterSelection(), req.getClusterOrig(), s.clear(), false))
            {
                if (!isValidCluster(s.str()))
                    throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", s.str());
                if (req.getState() == WUStateBlocked)
                    switchWorkUnitQueue(wu.get(), s.str());
                else if ((req.getState() != WUStateSubmitted) && (req.getState() != WUStateRunning) && (req.getState() != WUStateDebugPaused) && (req.getState() != WUStateDebugRunning))
                    wu->setClusterName(s.str());
            }
        }

        WsWuHelpers::setXmlParameters(wu, req.getXmlParams(), (req.getAction()==WUActionExecuteExisting));

        if (notEmpty(req.getQueryText()))
        {
            Owned<IWUQuery> query=wu->updateQuery();
            query->setQueryText(req.getQueryText());
        }

        if (version > 1.34 && notEmpty(req.getQueryMainDefinition()))
        {
            Owned<IWUQuery> query=wu->updateQuery();
            query->setQueryMainDefinition(req.getQueryMainDefinition());
        }

        if (!req.getResultLimit_isNull())
            wu->setResultLimit(req.getResultLimit());

        if (!req.getAction_isNull())
        {
            WUAction action = (WUAction) req.getAction();
            if(action < WUActionSize)
                wu->setAction(action);
        }

        if (!req.getPriorityClass_isNull())
        {
            WUPriorityClass priority = (WUPriorityClass) req.getPriorityClass();
            if(priority<PriorityClassSize)
                wu->setPriority(priority);
        }

        if (!req.getPriorityLevel_isNull())
            wu->setPriorityLevel(req.getPriorityLevel());

        if (origValueChanged(req.getScope(), req.getScopeOrig(), s.clear(), false))
            wu->setWuScope(s.str());

        ForEachItemIn(di, req.getDebugValues())
        {
            IConstDebugValue& item = req.getDebugValues().item(di);
            if (notEmpty(item.getName()))
                wu->setDebugValue(item.getName(), item.getValue(), true);
        }

        ForEachItemIn(ai, req.getApplicationValues())
        {
            IConstApplicationValue& item=req.getApplicationValues().item(ai);
            if(notEmpty(item.getApplication()) && notEmpty(item.getName()))
                wu->setApplicationValue(item.getApplication(), item.getName(), item.getValue(), true);
        }

        wu->commit();
        wu.clear();

        WsWuInfo winfo(context, cw);
        winfo.getInfo(resp.updateWorkunit(), WUINFO_All);

        StringBuffer thorSlaveIP;
        if (version > 1.24 && notEmpty(req.getThorSlaveIP()))
            thorSlaveIP = req.getThorSlaveIP();

        if (thorSlaveIP.length() > 0)
        {
            StringBuffer url;
            url.appendf("/WsWorkunits/WUInfo?Wuid=%s&ThorSlaveIP=%s", wuid.str(), thorSlaveIP.str());
            resp.setRedirectUrl(url.str());
        }
        else
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(wuid).str());

        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUCreateAndUpdate(IEspContext &context, IEspWUUpdateRequest &req, IEspWUUpdateResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        NewWsWorkunit wu(context);
        req.setWuid(wu->queryWuid());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onWUUpdate(context, req, resp);
}

static inline StringBuffer &appendUrlParameter(StringBuffer &url, const char *name, const char *value, bool &first)
{
    if (notEmpty(value))
    {
        url.append(first ? '?' : '&').append(name).append('=').append(value);
        first=false;
    }
    return url;
}

bool CWsWorkunitsEx::onWUAction(IEspContext &context, IEspWUActionRequest &req, IEspWUActionResponse &resp)
{
    try
    {
        StringBuffer sAction(req.getActionType());
        if (!sAction.length())
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Action not defined.");

        int *action=wuActionTable.getValue(sAction.toLowerCase().str());
        if (!action)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Invalid Action '%s'.", sAction.str());

        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());
        if (*action==ActionProtect)
            params->setProp("Protect", streq(sAction.str(), "protect"));
        if (*action==ActionChangeState && streq(sAction.str(), "settofailed"))
            params->setProp("State",4);
        if ((*action==ActionRestore) && !sashaServerIp.isEmpty())
        {
            params->setProp("sashaServerIP", sashaServerIp.get());
            params->setProp("sashaServerPort", sashaServerPort);
        }

        IArrayOf<IConstWUActionResult> results;
        if (doAction(context, req.getWuids(), *action, params, &results) && *action!=ActionDelete && checkRedirect(context))
        {
            StringBuffer redirect;
            if(req.getPageFrom() && strieq(req.getPageFrom(), "wuid"))
                redirect.append("/WsWorkunits/WUInfo?Wuid=").append(req.getWuids().item(0));
            else if (req.getPageFrom() && strieq(req.getPageFrom(), "scheduler"))
            {
                redirect.set("/WsWorkunits/WUShowScheduled");
                bool first=true;
                appendUrlParameter(redirect, "Cluster", req.getEventServer(), first);
                appendUrlParameter(redirect, "EventName", req.getEventName(), first);
            }
            else
            {
                redirect.append("/WsWorkunits/WUQuery");
                bool first=true;
                appendUrlParameter(redirect, "PageSize", req.getPageSize(), first);
                appendUrlParameter(redirect, "PageStartFrom", req.getCurrentPage(), first);
                appendUrlParameter(redirect, "Sortby", req.getSortby(), first);
                appendUrlParameter(redirect, "Descending", req.getDescending() ? "1" : "0", first);
                appendUrlParameter(redirect, "State", req.getState(), first);
                appendUrlParameter(redirect, "Cluster", req.getCluster(), first);
                appendUrlParameter(redirect, "Owner", req.getOwner(), first);
                appendUrlParameter(redirect, "StartDate", req.getStartDate(), first);
                appendUrlParameter(redirect, "EndDate", req.getEndDate(), first);
                appendUrlParameter(redirect, "ECL", req.getECL(), first);
                appendUrlParameter(redirect, "Jobname", req.getJobname(), first);
            }
            resp.setRedirectUrl(redirect.str());
        }
        else
            resp.setActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUDelete(IEspContext &context, IEspWUDeleteRequest &req, IEspWUDeleteResponse &resp)
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());

        if (!doAction(context,req.getWuids(), ActionDelete, params, &results))
            resp.setActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUAbort(IEspContext &context, IEspWUAbortRequest &req, IEspWUAbortResponse &resp)
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());
        if (!doAction(context,req.getWuids(), ActionAbort, params, &results))
            resp.setActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUProtect(IEspContext &context, IEspWUProtectRequest &req, IEspWUProtectResponse &resp)\
{
    try
    {
        IArrayOf<IConstWUActionResult> results;
        Owned<IProperties> params(createProperties(true));
        params->setProp("Protect", req.getProtect());
        params->setProp("BlockTillFinishTimer", 0);

        if (!doAction(context,req.getWuids(), ActionProtect, params, &results))
            resp.setActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUResubmit(IEspContext &context, IEspWUResubmitRequest &req, IEspWUResubmitResponse &resp)
{
    try
    {
        Owned<IMultiException> me = MakeMultiException();
        StringAttr wuid;
        StringArray wuids;

        double version = context.getClientVersion();
        IArrayOf<IEspResubmittedWU> resubmittedWUs;
        for(aindex_t i=0; i<req.getWuids().length();i++)
        {
            StringBuffer requestWuid = req.getWuids().item(i);
            WsWuHelpers::checkAndTrimWorkunit("WUResubmit", requestWuid);

            ensureWsWorkunitAccess(context, requestWuid.str(), SecAccess_Write);

            wuid.set(requestWuid.str());

            try
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                if(req.getCloneWorkunit() || req.getRecompile())
                {
                    Owned<IConstWorkUnit> src(factory->openWorkUnit(wuid.str()));
                    NewWsWorkunit wu(factory, context);
                    wuid.set(wu->queryWuid());
                    queryExtendedWU(wu)->copyWorkUnit(src, false);

                    SCMStringBuffer token;
                    wu->setSecurityToken(createToken(wuid.str(), context.queryUserId(), context.queryPassword(), token).str());
                }

                wuids.append(wuid.str());

                Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str()));
                if(!cw)
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

                WsWuHelpers::submitWsWorkunit(context, cw, NULL, NULL, 0, req.getRecompile(), req.getResetWorkflow(), false);

                if (version < 1.40)
                    continue;

                Owned<IEspResubmittedWU> resubmittedWU = createResubmittedWU();
                resubmittedWU->setWUID(wuid.str());
                if (!streq(requestWuid.str(), wuid.str()))
                    resubmittedWU->setParentWUID(requestWuid.str());
                resubmittedWUs.append(*resubmittedWU.getClear());
            }
            catch (IException *E)
            {
                me->append(*E);
            }
            catch (...)
            {
                me->append(*MakeStringException(0,"Unknown exception submitting %s",wuid.str()));
            }
        }

        if(me->ordinality())
            throw me.getLink();

        int timeToWait = req.getBlockTillFinishTimer();
        if (timeToWait != 0)
        {
            for(aindex_t i=0; i<wuids.length(); i++)
                waitForWorkUnitToComplete(wuids.item(i), timeToWait);
        }

        if (version >= 1.40)
            resp.setWUs(resubmittedWUs);

        if(wuids.length()==1)
        {
            resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(wuids.item(0)));
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUPushEvent(IEspContext &context, IEspWUPushEventRequest &req, IEspWUPushEventResponse &resp)
{
    try
    {
        const char *name = req.getEventName();
        const char *text = req.getEventText();
        const char *target = NULL;
        if (notEmpty(name) && notEmpty(text))
        {
            Owned<IScheduleEventPusher> pusher(getScheduleEventPusher());
            pusher->push(name, text, target);

            StringBuffer redirect("/WsWorkunits/WUShowScheduled");
            bool first=true;
            appendUrlParameter(redirect, "PushEventName", name, first);
            appendUrlParameter(redirect, "PushEventText", text, first);
            resp.setRedirectUrl(redirect.str());
            return true;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return false;
}

bool CWsWorkunitsEx::onWUSchedule(IEspContext &context, IEspWUScheduleRequest &req, IEspWUScheduleResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUSchedule", wuid);

        const char* cluster = req.getCluster();
        if (isEmpty(cluster))
             throw MakeStringException(ECLWATCH_INVALID_INPUT,"No Cluster defined.");
        if (!isValidCluster(cluster))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        WorkunitUpdate wu(factory->updateWorkUnit(wuid.str()));

        ensureWsWorkunitAccess(context, *wu.get(), SecAccess_Write);
        switch(wu->getState())
        {
            case WUStateDebugPaused:
            case WUStateDebugRunning:
            case WUStateRunning:
            case WUStateAborting:
            case WUStateBlocked:
                throw MakeStringException(ECLWATCH_CANNOT_SCHEDULE_WORKUNIT, "Cannot schedule the workunit. Workunit state is '%s'.", wu->queryStateDesc());
        }

        wu->clearExceptions();
        wu->setClusterName(cluster);

        if (notEmpty(req.getWhen()))
        {
            WsWuDateTime dt;
            dt.setString(req.getWhen());
            wu->setTimeScheduled(dt);
        }

        if(notEmpty(req.getSnapshot()))
            wu->setSnapshot(req.getSnapshot());
        wu->setState(WUStateScheduled);

        if (req.getMaxRunTime())
            wu->setDebugValueInt("maxRunTime", req.getMaxRunTime(), true);

        SCMStringBuffer token;
        wu->setSecurityToken(createToken(wuid.str(), context.queryUserId(), context.queryPassword(), token).str());

        AuditSystemAccess(context.queryUserId(), true, "Scheduled %s", wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsWorkunitsEx::onWUSubmit(IEspContext &context, IEspWUSubmitRequest &req, IEspWUSubmitResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUSubmit", wuid);

        const char *cluster = req.getCluster();
        if (isEmpty(cluster))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No Cluster defined.");
        if (!isValidCluster(cluster))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        if (cw->getAction()==WUActionExecuteExisting)
        {
            ExecuteExistingQueryInfo info(cw);
            if (info.queryset.isEmpty() || info.query.isEmpty())
            {
                WorkunitUpdate wu(&cw->lock());
                throw WsWuHelpers::noteException(wu, MakeStringException(ECLWATCH_INVALID_INPUT,"Queryset and/or query not specified"));
            }

            WsWuHelpers::runWsWuQuery(context, cw, info.queryset.str(), info.query.str(), cluster, NULL);
        }
        else
            WsWuHelpers::submitWsWorkunit(context, cw, cluster, req.getSnapshot(), req.getMaxRunTime(), true, false, false);

        if (req.getBlockTillFinishTimer() != 0)
            waitForWorkUnitToComplete(wuid.str(), req.getBlockTillFinishTimer());

        resp.setRedirectUrl(StringBuffer("/WsWorkunits/WUInfo?Wuid=").append(wuid).str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

ErrorSeverity checkGetExceptionSeverity(CWUExceptionSeverity severity)
{
    switch (severity)
    {
        case CWUExceptionSeverity_INFO:
            return SeverityInformation;
        case CWUExceptionSeverity_WARNING:
            return SeverityWarning;
        case CWUExceptionSeverity_ERROR:
            return SeverityError;
        case CWUExceptionSeverity_ALERT:
            return SeverityAlert;
    }

    throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT,"invalid exception severity");
}

bool CWsWorkunitsEx::onWURun(IEspContext &context, IEspWURunRequest &req, IEspWURunResponse &resp)
{
    try
    {
        const char *cluster = req.getCluster();
        if (notEmpty(cluster) && !isValidCluster(cluster))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

        StringBuffer wuidStr = req.getWuid();
        const char* runWuid = wuidStr.trim().str();
        StringBuffer wuid;

        ErrorSeverity severity = checkGetExceptionSeverity(req.getExceptionSeverity());

        if (runWuid && *runWuid)
        {
            if (!looksLikeAWuid(runWuid, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", runWuid);

            if (req.getCloneWorkunit())
                WsWuHelpers::runWsWorkunit(context, wuid, runWuid, cluster, req.getInput(), &req.getVariables(), &req.getDebugValues());
            else
            {
                WsWuHelpers::submitWsWorkunit(context, runWuid, cluster, NULL, 0, false, true, true, req.getInput(), &req.getVariables(), &req.getDebugValues());
                wuid.set(runWuid);
            }
        }
        else if (notEmpty(req.getQuerySet()) && notEmpty(req.getQuery()))
            WsWuHelpers::runWsWuQuery(context, wuid, req.getQuerySet(), req.getQuery(), cluster, req.getInput());
        else
            throw MakeStringException(ECLWATCH_MISSING_PARAMS,"Workunit or Query required");

        int timeToWait = req.getWait();
        if (timeToWait != 0)
            waitForWorkUnitToComplete(wuid.str(), timeToWait);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", wuid.str());

        resp.setState(cw->queryStateDesc());
        resp.setWuid(wuid.str());

        switch (cw->getState())
        {
            case WUStateCompleted:
            case WUStateFailed:
            case WUStateUnknown:
            {
                SCMStringBuffer result;
                unsigned flags = WorkUnitXML_SeverityTags;
                if (req.getNoRootTag())
                    flags |= WorkUnitXML_NoRoot;
                getFullWorkUnitResultsXML(context.queryUserId(), context.queryPassword(), cw.get(), result, flags, severity);
                resp.setResults(result.str());
                break;
            }
            default:
                break;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsWorkunitsEx::onWUWaitCompiled(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUWaitCompiled", wuid);
        secWaitForWorkUnitToCompile(wuid.str(), *context.querySecManager(), *context.queryUser(), req.getWait());
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        resp.setStateID(cw->getState());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUWaitComplete(IEspContext &context, IEspWUWaitRequest &req, IEspWUWaitResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUWaitComplete", wuid);
        resp.setStateID(secWaitForWorkUnitToComplete(wuid.str(), *context.querySecManager(), *context.queryUser(), req.getWait(), req.getReturnOnWait()));
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUCDebug(IEspContext &context, IEspWUDebugRequest &req, IEspWUDebugResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUCDebug", wuid);
        StringBuffer result;
        secDebugWorkunit(wuid.str(), *context.querySecManager(), *context.queryUser(), req.getCommand(), result);
        resp.setResult(result);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUSyntaxCheckECL(IEspContext &context, IEspWUSyntaxCheckRequest &req, IEspWUSyntaxCheckResponse &resp)
{
    try
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        NewWsWorkunit wu(factory, context);
        wu->setAction(WUActionCheck);
        if(notEmpty(req.getModuleName()) && notEmpty(req.getAttributeName()))
        {
            wu->setApplicationValue("SyntaxCheck", "ModuleName", req.getModuleName(), true);
            wu->setApplicationValue("SyntaxCheck", "AttributeName", req.getAttributeName(), true);
        }

        ForEachItemIn(di, req.getDebugValues())
        {
            IConstDebugValue& item=req.getDebugValues().item(di);
            if(notEmpty(item.getName()))
                wu->setDebugValue(item.getName(), item.getValue(), true);
        }

        wu.setQueryText(req.getECL());

        StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit,clear()
        wu->commit();
        wu.clear();

        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);
        waitForWorkUnitToComplete(wuid.str(), req.getTimeToWait());

        Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str()));
        WsWUExceptions errors(*cw);
        resp.setErrors(errors);
        cw.clear();

        factory->deleteWorkUnit(wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsWorkunitsEx::onWUCompileECL(IEspContext &context, IEspWUCompileECLRequest &req, IEspWUCompileECLResponse &resp)
{
    try
    {
        ensureWsCreateWorkunitAccess(context);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        NewWsWorkunit wu(factory, context);

        if(req.getIncludeComplexity())
        {
            wu->setAction(WUActionCompile);
            wu->setDebugValueInt("calculateComplexity",1,true);
        }
        else
            wu->setAction(WUActionCheck);

        if(req.getModuleName() && req.getAttributeName())
        {
            wu->setApplicationValue("SyntaxCheck","ModuleName",req.getModuleName(),true);
            wu->setApplicationValue("SyntaxCheck","AttributeName",req.getAttributeName(),true);
        }

        if(req.getIncludeDependencies())
            wu->setApplicationValueInt("SyntaxCheck","IncludeDependencies",1,true);

        wu.setQueryText(req.getECL());

        StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit,clear()        StringAttr wuid(wu->queryWuid());

        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);
        waitForWorkUnitToComplete(wuid.str(),req.getTimeToWait());

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());

        SCMStringBuffer s;
        cw->getDebugValue("__Calculated__Complexity__",s);
        if(s.length())
            resp.setComplexity(s.str());

        WsWUExceptions errors(*cw);
        resp.setErrors(errors);

        if(!errors.ErrCount())
        {
            IArrayOf<IEspWUECLAttribute> dependencies;
            for(unsigned count=1;;count++)
            {
                SCMStringBuffer xml;
                cw->getApplicationValue("SyntaxCheck",StringBuffer("Dependency").append(count).str(),xml);
                if(!xml.length())
                    break;
                Owned<IPropertyTree> dep=createPTreeFromXMLString(xml.str(), ipt_caseInsensitive);
                if(!dep)
                    continue;

                Owned<IEspWUECLAttribute> att = createWUECLAttribute("","");
                att->setModuleName(dep->queryProp("@module"));
                att->setAttributeName(dep->queryProp("@name"));

                int flags = dep->getPropInt("@flags",0);
                if(flags & ob_locked)
                {
                    if(flags & ob_lockedself)
                        att->setIsCheckedOut(true);
                    else
                        att->setIsLocked(true);
                }
                if(flags & ob_sandbox)
                    att->setIsSandbox(true);
                if(flags & ob_orphaned)
                    att->setIsOrphaned(true);

                dependencies.append(*att.getLink());
            }
            resp.setDependencies(dependencies);
        }
        cw.clear();
        factory->deleteWorkUnit(wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

bool CWsWorkunitsEx::onWUGetDependancyTrees(IEspContext& context, IEspWUGetDependancyTreesRequest& req, IEspWUGetDependancyTreesResponse& resp)
{
    try
    {
        DBGLOG("WUGetDependancyTrees");

        unsigned int timeMilliSec = 500;

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        NewWsWorkunit wu(factory, context);
        wu->setAction(WUActionCheck);

        if (notEmpty(req.getCluster()))
            wu->setClusterName(req.getCluster());
        if (notEmpty(req.getSnapshot()))
            wu->setSnapshot(req.getSnapshot());

        wu->setDebugValue("gatherDependenciesSelection",notEmpty(req.getItems()) ? req.getItems() : NULL,true);
        if (context.getClientVersion() > 1.12)
        {
            wu->setDebugValueInt("gatherDependencies", 1, true);

            const char *timeout = req.getTimeoutMilliSec();
            if (notEmpty(timeout))
            {
                const char *finger = timeout;
                while (*finger)
                {
                    if (!isdigit(*finger++))
                        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Incorrect timeout value");
                }
                timeMilliSec = atol(timeout);
            }
        }

        StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit,clear()
        wu->commit();
        wu.clear();

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);
        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);

        int state = waitForWorkUnitToComplete(wuid.str(), timeMilliSec);
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());

        WsWUExceptions errors(*cw);
        resp.setErrors(errors);

        MemoryBuffer temp;
        MemoryBuffer2IDataVal xmlresult(temp);
        Owned<IConstWUResult> result = cw->getResultBySequence(0);
        if (result)
        {
            result->getResultRaw(xmlresult, NULL, NULL);
            resp.setDependancyTrees(temp);
        }

        wu.setown(&cw->lock());
        wu->setState(WUStateAborted);
        wu->commit();
        wu.clear();

        factory->deleteWorkUnit(wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool getWsWuInfoFromSasha(IEspContext &context, SocketEndpoint &ep, const char* wuid, IEspECLWorkunit *info)
{
    Owned<INode> node = createINode(ep);
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->addId(wuid);
    cmd->setAction(SCA_GET);
    if (!cmd->send(node, 1*60*1000))
    {
        StringBuffer url;
        DBGLOG("Could not connect to Sasha server at %s", ep.getUrlStr(url).str());
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,"Cannot connect to archive server at %s.", url.str());
    }

    if (cmd->numIds()==0)
    {
        DBGLOG("Could not read archived workunit %s",wuid);
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot read workunit %s.",wuid);
    }

    unsigned num = cmd->numResults();
    if (num < 1)
        return false;

    StringBuffer res;
    cmd->getResult(0, res);
    if(res.length() < 1)
        return false;

    Owned<IPropertyTree> wpt = createPTreeFromXMLString(res.str());
    if (!wpt)
        return false;

    const char * owner = wpt->queryProp("@submitID");
    ensureWsWorkunitAccessByOwnerId(context, owner, SecAccess_Read);

    info->setWuid(wuid);
    info->setArchived(true);

    if (notEmpty(owner))
        info->setOwner(owner);
    const char * state = wpt->queryProp("@state");
    if (notEmpty(state))
        info->setState(state);
    const char * cluster = wpt->queryProp("@clusterName");
    if (notEmpty(cluster))
        info->setCluster(cluster);
    if (context.querySecManager())
    {
        const char * scope = wpt->queryProp("@scope");
        if (notEmpty(scope))
            info->setScope(scope);
    }
    const char * jobName = wpt->queryProp("@jobName");
    if (notEmpty(jobName))
        info->setJobname(jobName);
    const char * description = wpt->queryProp("Debug/description");
    if (notEmpty(description))
        info->setDescription(description);
    const char * queryText = wpt->queryProp("Query/Text");
    if (notEmpty(queryText))
        info->updateQuery().setText(queryText);
    const char * protectedWU = wpt->queryProp("@protected");
    info->setProtected((protectedWU && *protectedWU!='0'));

    return true;
}

#define     WUDETAILS_REFRESH_MINS 1

void getArchivedWUInfo(IEspContext &context, const char* sashaServerIP, unsigned sashaServerPort, const char *wuid, IEspWUInfoResponse &resp)
{
    SocketEndpoint ep;
    if (sashaServerIP && *sashaServerIP)
        ep.set(sashaServerIP, sashaServerPort);
    else
        getSashaNode(ep);
    if (getWsWuInfoFromSasha(context, ep, wuid, &resp.updateWorkunit()))
    {
        resp.setCanCompile(false);
        return;
    }

    throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT, "Cannot find workunit %s.", wuid);
    return;

}

#define WUDETAILS_REFRESH_MINS 1

bool CWsWorkunitsEx::onWUInfo(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUInfo", wuid);

        double version = context.getClientVersion();
        if (req.getType() && strieq(req.getType(), "archived workunits"))
            getArchivedWUInfo(context, sashaServerIp.get(), sashaServerPort, wuid.str(), resp);
        else
        {
            try
            {
                //The access is checked here because getArchivedWUInfo() has its own access check.
                ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

                unsigned flags=0;
                if (req.getTruncateEclTo64k())
                    flags|=WUINFO_TruncateEclTo64k;
                if (req.getIncludeExceptions())
                    flags|=WUINFO_IncludeExceptions;
                if (req.getIncludeGraphs())
                    flags|=WUINFO_IncludeGraphs;
                if (req.getIncludeSourceFiles())
                    flags|=WUINFO_IncludeSourceFiles;
                if (req.getIncludeResults())
                    flags|=WUINFO_IncludeResults;
                if (req.getIncludeVariables())
                    flags|=WUINFO_IncludeVariables;
                if (req.getIncludeTimers())
                    flags|=WUINFO_IncludeTimers;
                if (req.getIncludeDebugValues())
                    flags|=WUINFO_IncludeDebugValues;
                if (req.getIncludeApplicationValues())
                    flags|=WUINFO_IncludeApplicationValues;
                if (req.getIncludeWorkflows())
                    flags|=WUINFO_IncludeWorkflows;
                if (!req.getSuppressResultSchemas())
                    flags|=WUINFO_IncludeEclSchemas;
                if (req.getIncludeXmlSchemas())
                    flags|=WUINFO_IncludeXmlSchema;
                if (req.getIncludeResultsViewNames())
                    flags|=WUINFO_IncludeResultsViewNames;
                if (req.getIncludeResourceURLs())
                    flags|=WUINFO_IncludeResourceURLs;

                WsWuInfo winfo(context, wuid.str());
                winfo.getInfo(resp.updateWorkunit(), flags);

                if (req.getIncludeResultsViewNames()||req.getIncludeResourceURLs()||(version >= 1.50))
                {
                    StringArray views, urls;
                    winfo.getResourceInfo(views, urls, WUINFO_IncludeResultsViewNames|WUINFO_IncludeResourceURLs);
                    IEspECLWorkunit& eclWU = resp.updateWorkunit();
                    if (req.getIncludeResultsViewNames())
                        resp.setResultViews(views);
                    if (req.getIncludeResourceURLs())
                        eclWU.setResourceURLs(urls);
                    if (version >= 1.50)
                    {
                        eclWU.setResultViewCount(views.length());
                        eclWU.setResourceURLCount(urls.length());
                    }
                }
            }
            catch (IException *e)
            {
                if (e->errorCode() != ECLWATCH_CANNOT_OPEN_WORKUNIT)
                    throw e;
                getArchivedWUInfo(context, sashaServerIp.get(), sashaServerPort, wuid.str(), resp);
                e->Release();
            }

            switch (resp.getWorkunit().getStateID())
            {
                case WUStateCompiling:
                case WUStateCompiled:
                case WUStateScheduled:
                case WUStateSubmitted:
                case WUStateRunning:
                case WUStateAborting:
                case WUStateWait:
                case WUStateUploadingFiles:
                case WUStateDebugPaused:
                case WUStateDebugRunning:
                    resp.setAutoRefresh(WUDETAILS_REFRESH_MINS);
                    break;
                case WUStateBlocked:
                    resp.setAutoRefresh(WUDETAILS_REFRESH_MINS*5);
                    break;
            }

            resp.setCanCompile(notEmpty(context.queryUserId()));
            if (version > 1.24 && notEmpty(req.getThorSlaveIP()))
                resp.setThorSlaveIP(req.getThorSlaveIP());

            ISecManager* secmgr = context.querySecManager();
            if (!secmgr)
                resp.setSecMethod(NULL);
            else
                resp.setSecMethod(secmgr->querySecMgrTypeName());
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool CWsWorkunitsEx::onWUInfoDetails(IEspContext &context, IEspWUInfoRequest &req, IEspWUInfoResponse &resp)
{
    return onWUInfo(context, req, resp);
}

bool CWsWorkunitsEx::onWUResultView(IEspContext &context, IEspWUResultViewRequest &req, IEspWUResultViewResponse &resp)
{
    StringBuffer wuid = req.getWuid();
    WsWuHelpers::checkAndTrimWorkunit("WUResultView", wuid);

    ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

    Owned<IWuWebView> wv = createWuWebView(wuid.str(), NULL, NULL, getCFD(), true);
    StringBuffer html;
    wv->renderSingleResult(req.getViewName(), req.getResultName(), html);
    resp.setResult(html.str());
    resp.setResult_mimetype("text/html");
    return true;
}


void doWUQueryBySingleWuid(IEspContext &context, const char *wuid, IEspWUQueryResponse &resp)
{
    Owned<IEspECLWorkunit> info= createECLWorkunit("","");
    WsWuInfo winfo(context, wuid);
    winfo.getCommon(*info, 0);
    IArrayOf<IEspECLWorkunit> results;
    results.append(*info.getClear());
    resp.setWorkunits(results);
    resp.setPageSize(1);
    resp.setCount(1);
}

void doWUQueryByFile(IEspContext &context, const char *logicalFile, IEspWUQueryResponse &resp)
{
    StringBuffer wuid;
    getWuidFromLogicalFileName(context, logicalFile, wuid);
    if (!wuid.length())
        throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.", logicalFile);

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw= factory->openWorkUnit(wuid.str());
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit for file %s.", logicalFile);
    if (getWsWorkunitAccess(context, *cw) < SecAccess_Read)
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED,"Cannot access the workunit for file %s.",logicalFile);

    doWUQueryBySingleWuid(context, wuid.str(), resp);

    resp.setFirst(false);
    resp.setPageSize(1);
    resp.setCount(1);
}

bool addWUQueryFilter(WUSortField *filters, unsigned short &count, MemoryBuffer &buff, const char *name, WUSortField value)
{
    if (isEmpty(name))
        return false;
    filters[count++] = value;
    buff.append(name);
    return true;
}

bool addWUQueryFilterTime(WUSortField *filters, unsigned short &count, MemoryBuffer &buff, const char *stime, WUSortField value)
{
    if (isEmpty(stime))
        return false;
    CDateTime dt;
    dt.setString(stime, NULL, true);
    unsigned year, month, day, hour, minute, second, nano;
    dt.getDate(year, month, day, true);
    dt.getTime(hour, minute, second, nano, true);
    VStringBuffer wuid("W%4d%02d%02d-%02d%02d%02d",year,month,day,hour,minute,second);
    filters[count++] = value;
    buff.append(wuid.str());
    return true;
}

bool addWUQueryFilterApplication(WUSortField *filters, unsigned short &count, MemoryBuffer &buff, const char *appname, const char *appkey, const char *appdata)
{
    if (isEmpty(appname))
        return false;  // appname must be specified
    if (isEmpty(appkey) && isEmpty(appdata)) //one or other is required ( MORE - see if cassandra can relax that)
        return false;
    VStringBuffer path("%s/%s", appname, appkey && *appkey ? appkey : "*");
    buff.append(path.str());
    buff.append(appdata);
    filters[count++] = WUSFappvalue;
    return true;
}

void doWUQueryWithSort(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

    double version = context.getClientVersion();

    IArrayOf<IEspECLWorkunit> results;

    int begin = 0;
    unsigned int count = 100;
    int pagesize = 100;
    if (version > 1.01)
    {
        pagesize = (int)req.getPageSize();
        if (!req.getCount_isNull())
            pagesize = req.getCount();
        if(pagesize < 1)
            pagesize = 100;

        begin = (int)req.getPageStartFrom();
    }
    else
    {
        count=(unsigned)req.getCount();
        if(!count)
            count=100;
        if (notEmpty(req.getAfter()))
            begin=atoi(req.getAfter());
        else if (notEmpty(req.getBefore()))
            begin=atoi(req.getBefore())-count;
        if (begin < 0)
            begin = 0;

        pagesize = count;
    }

    WUSortField sortorder = (WUSortField) (WUSFwuid | WUSFreverse);
    if (notEmpty(req.getSortby()))
    {
        const char *sortby = req.getSortby();
        if (strieq(sortby, "Owner"))
            sortorder = WUSFuser;
        else if (strieq(sortby, "JobName"))
            sortorder = WUSFjob;
        else if (strieq(sortby, "Cluster"))
            sortorder = WUSFcluster;
        else if (strieq(sortby, "Protected"))
            sortorder = WUSFprotected;
        else if (strieq(sortby, "State"))
            sortorder = WUSFstate;
        else if (strieq(sortby, "ClusterTime"))
            sortorder = (WUSortField) (WUSFtotalthortime+WUSFnumeric);
        else
            sortorder = WUSFwuid;

        sortorder = (WUSortField) (sortorder | WUSFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortorder = (WUSortField) (sortorder | WUSFreverse);
    }

    WUSortField filters[10];
    unsigned short filterCount = 0;
    MemoryBuffer filterbuf;

    // Query filters should be added in order of expected power - add the most restrictive filters first

    bool bDoubleCheckState = false;
    if(req.getState() && *req.getState())
    {
        filters[filterCount++] = WUSFstate;
        if (!strieq(req.getState(), "unknown"))
            filterbuf.append(req.getState());
        else
            filterbuf.append("");
        if (strieq(req.getState(), "submitted"))
            bDoubleCheckState = true;
    }

    addWUQueryFilter(filters, filterCount, filterbuf, req.getWuid(), WUSFwildwuid);
    addWUQueryFilter(filters, filterCount, filterbuf, req.getCluster(), WUSFcluster);
    addWUQueryFilter(filters, filterCount, filterbuf, req.getLogicalFile(), (WUSortField) (WUSFfileread | WUSFnocase));
    addWUQueryFilter(filters, filterCount, filterbuf, req.getOwner(), (WUSortField) (WUSFuser | WUSFnocase));
    addWUQueryFilter(filters, filterCount, filterbuf, req.getJobname(), (WUSortField) (WUSFjob | WUSFnocase));
    addWUQueryFilter(filters, filterCount, filterbuf, req.getECL(), (WUSortField) (WUSFecl | WUSFwild));

    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getStartDate(), WUSFwuid);
    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getEndDate(), WUSFwuidhigh);
    if (version < 1.55)
        addWUQueryFilterApplication(filters, filterCount, filterbuf, req.getApplicationName(), req.getApplicationKey(), req.getApplicationData());
    else
    {
        IArrayOf<IConstApplicationValue>& applicationFilters = req.getApplicationValues();
        ForEachItemIn(i, applicationFilters)
        {
            IConstApplicationValue &item = applicationFilters.item(i);
            addWUQueryFilterApplication(filters, filterCount, filterbuf, item.getApplication(), item.getName(), item.getValue());
        }
    }

    filters[filterCount] = WUSFterm;

    __int64 cacheHint = 0;
    if (!req.getCacheHint_isNull())
        cacheHint = req.getCacheHint();

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    unsigned numWUs;
    Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), begin, pagesize+1, &cacheHint, &numWUs); // MORE - need security flags here!
    if (version >= 1.41)
        resp.setCacheHint(cacheHint);

    unsigned actualCount = 0;
    ForEach(*it)
    {
        IConstWorkUnitInfo& cw = it->query();
        if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cw, accessOwn, accessOthers) < SecAccess_Read)
        {
            numWUs--;
            continue;
        }

        if (bDoubleCheckState && (cw.getState() != WUStateSubmitted))
        {
            numWUs--;
            continue;
        }

        // This test is presumably trying to remove the global workunit, though it's not the right way to do so (since it will mess up page counts etc)
        const char* wuid = cw.queryWuid();
        if (!looksLikeAWuid(wuid, 'W'))
        {
            numWUs--;
            continue;
        }
        actualCount++;
        Owned<IEspECLWorkunit> info = createECLWorkunit("","");
        info->setWuid(cw.queryWuid());
        info->setProtected(cw.isProtected() ? 1 : 0);
        info->setJobname(cw.queryJobName());
        info->setOwner(cw.queryUser());
        info->setCluster(cw.queryClusterName());
        SCMStringBuffer s;
        // info.setSnapshot(cw->getSnapshot(s).str());
        info->setStateID(cw.getState());
        info->setState(cw.queryStateDesc());
        unsigned totalThorTimeMS = cw.getTotalThorTime();
        StringBuffer totalThorTimeStr;
        formatDuration(totalThorTimeStr, totalThorTimeMS);
        if (version > 1.52)
            info->setTotalClusterTime(totalThorTimeStr.str());
        else
            info->setTotalThorTime(totalThorTimeStr.str());
        //if (cw->isPausing())
        //    info.setIsPausing(true);
        // getEventScheduleFlag(info);
        WsWuDateTime dt;
        cw.getTimeScheduled(dt);
        if(dt.isValid())
            info->setDateTimeScheduled(dt.getString(s).str());
        if (version >= 1.55)
        {
            IArrayOf<IEspApplicationValue> av;
            Owned<IConstWUAppValueIterator> app(&cw.getApplicationValues());
            ForEach(*app)
            {
                IConstWUAppValue& val=app->query();
                Owned<IEspApplicationValue> t= createApplicationValue("","");
                t->setApplication(val.queryApplication());
                t->setName(val.queryName());
                t->setValue(val.queryValue());
                av.append(*t.getLink());

            }
            info->setApplicationValues(av);
        }
        results.append(*info.getClear());
    }

    if (version > 1.02)
    {
        resp.setPageStartFrom(begin+1);
        resp.setNumWUs(numWUs);
        if (results.length() > (aindex_t)pagesize)
            results.pop();

        if(unsigned (begin + pagesize) < numWUs)
        {
            resp.setNextPage(begin + pagesize);
            resp.setPageEndAt(begin + pagesize);
            int last = begin + pagesize;
            while (numWUs > (unsigned) last + pagesize)
                last += pagesize;
            resp.setLastPage(last);
        }
        else
        {
            resp.setNextPage(-1);
            resp.setPageEndAt(numWUs);
        }

        if(begin > 0)
        {
            resp.setFirst(false);
            if (begin - pagesize > 0)
                resp.setPrevPage(begin - pagesize);
            else
                resp.setPrevPage(0);
        }
        resp.setPageSize(pagesize);
    }
    else
    {
        if(begin>0 && actualCount > 0)
        {
            char buf[10];
            itoa(begin, buf, 10);
            resp.setCurrent(buf);
        }

        if(count<actualCount)
        {
            char buf[10];
            itoa(begin+count, buf, 10);
            resp.setNext(buf);
            resp.setNumWUs(numWUs);
            if (results.length() > count)
                results.pop();
        }

        if(begin == 0 && actualCount <= count)
            resp.setFirst(false);

        resp.setCount(count);
    }

    resp.setWorkunits(results);
    return;
}

void doWUQueryFromArchive(IEspContext &context, const char* sashaServerIP, unsigned sashaServerPort,
       ArchivedWuCache &archivedWuCache, unsigned cacheMinutes, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    class CArchivedWUsReader : public CInterface, implements IArchivedWUsReader
    {
        IEspContext& context;
        IEspWUQueryRequest& req;
        unsigned pageFrom, pageSize;
        StringAttr sashaServerIP;
        unsigned sashaServerPort;
        unsigned cacheMinutes;
        StringBuffer filterStr;
        ArchivedWuCache& archivedWuCache;
        unsigned numberOfWUsReturned;
        bool hasMoreWU;

        void readDateFilters(StringBuffer& from, StringBuffer& to)
        {
            CDateTime timeFrom, timeTo;
            if(notEmpty(req.getEndDate()))
                timeTo.setString(req.getEndDate(), NULL, true);
            else
                timeTo.setNow();

            unsigned year, month, day, hour, minute, second, nano;
            timeTo.getDate(year, month, day, true);
            timeTo.getTime(hour, minute, second, nano, true);
            to.setf("%4d%02d%02d%02d%02d", year, month, day, hour, minute);

            if(!notEmpty(req.getStartDate()))
                return;

            timeFrom.setString(req.getStartDate(), NULL, true);
            if (timeFrom >= timeTo)
                return;

            unsigned year0, month0, day0, hour0, minute0, second0, nano0;
            timeFrom.getDate(year0, month0, day0, true);
            timeFrom.getTime(hour0, minute0, second0, nano0, true);
            from.setf("%4d%02d%02d%02d%02d", year0, month0, day0, hour0, minute0);

            return;
        }

        bool addToFilterString(const char *name, const char *value)
        {
            if (isEmpty(name) || isEmpty(value))
                return false;
            if (filterStr.length())
                filterStr.append(';');
            filterStr.append(name).append("=").append(value);
            return true;
        }

        bool addToFilterString(const char *name, unsigned value)
        {
            if (isEmpty(name))
                return false;
            if (filterStr.length())
                filterStr.append(';');
            filterStr.append(name).append("=").append(value);
            return true;
        }

        void setFilterString()
        {
            addToFilterString("cluster", req.getCluster());
            addToFilterString("owner", req.getOwner());
            addToFilterString("jobName", req.getJobname());
            addToFilterString("state", req.getState());
            addToFilterString("timeFrom", req.getStartDate());
            addToFilterString("timeTo", req.getEndDate());
            addToFilterString("pageStart", pageFrom);
            addToFilterString("pageSize", pageSize);
            if (sashaServerIP && *sashaServerIP)
            {
                addToFilterString("sashaServerIP", sashaServerIP.get());
                addToFilterString("sashaServerPort", sashaServerPort);
            }
        }

        void setSashaCommand(INode* sashaserver, ISashaCommand* cmd)
        {
            cmd->setAction(SCA_LIST);
            cmd->setOutputFormat("owner,jobname,cluster,state");
            cmd->setOnline(false);
            cmd->setArchived(true);
            cmd->setStart(pageFrom);
            cmd->setLimit(pageSize+1); //read an extra WU to check hasMoreWU
            if (notEmpty(req.getCluster()))
                cmd->setCluster(req.getCluster());
            if (notEmpty(req.getOwner()))
                cmd->setOwner(req.getOwner());
            if (notEmpty(req.getJobname()))
                cmd->setJobName(req.getJobname());
            if (notEmpty(req.getState()))
                cmd->setState(req.getState());

            StringBuffer timeFrom, timeTo;
            readDateFilters(timeFrom, timeTo);
            if (timeFrom.length())
                cmd->setAfter(timeFrom.str());
            if (timeTo.length())
                cmd->setBefore(timeTo.str());

            return;
        }

        IEspECLWorkunit *createArchivedWUEntry(StringArray& wuDataArray, bool canAccess)
        {
            Owned<IEspECLWorkunit> info= createECLWorkunit("","");
            const char* wuid = wuDataArray.item(0);
            const char* owner = wuDataArray.item(1);
            const char* jobName = wuDataArray.item(2);
            const char* cluster = wuDataArray.item(3);
            const char* state = wuDataArray.item(4);
            info->setWuid(wuid);
            if (!canAccess)
                info->setState("<Hidden>");
            else
            {
                if (notEmpty(owner))
                    info->setOwner(owner);
                if (notEmpty(jobName))
                    info->setJobname(jobName);
                if (notEmpty(cluster))
                    info->setCluster(cluster);
                if (notEmpty(state))
                    info->setState(state);
            }
            return info.getClear();
        }
        static int compareWuids(IInterface * const *_a, IInterface * const *_b)
        {
            IEspECLWorkunit *a = *(IEspECLWorkunit **)_a;
            IEspECLWorkunit *b = *(IEspECLWorkunit **)_b;
            return strcmp(b->getWuid(), a->getWuid());
        }
    public:
        IMPLEMENT_IINTERFACE_USING(CInterface);

        CArchivedWUsReader(IEspContext& _context, const char* _sashaServerIP, unsigned _sashaServerPort, ArchivedWuCache& _archivedWuCache,
            unsigned _cacheMinutes, unsigned _pageFrom, unsigned _pageSize, IEspWUQueryRequest& _req)
            : context(_context), sashaServerIP(_sashaServerIP), sashaServerPort(_sashaServerPort),
            archivedWuCache(_archivedWuCache), cacheMinutes(_cacheMinutes), pageFrom(_pageFrom), pageSize(_pageSize), req(_req)
        {
            hasMoreWU = false;
            numberOfWUsReturned = 0;
        }

        void getArchivedWUs(IArrayOf<IEspECLWorkunit>& archivedWUs)
        {
            setFilterString();
            Owned<ArchivedWuCacheElement> cachedResults = archivedWuCache.lookup(context, filterStr, "AddWhenAvailable", cacheMinutes);
            if (cachedResults)
            {
                hasMoreWU = cachedResults->m_hasNextPage;
                numberOfWUsReturned = cachedResults->numWUsReturned;
                if (cachedResults->m_results.length())
                {
                    ForEachItemIn(ai, cachedResults->m_results)
                        archivedWUs.append(*LINK(&cachedResults->m_results.item(ai)));
                }
            }
            else
            {
                SocketEndpoint ep;
                if (sashaServerIP && *sashaServerIP)
                    ep.set(sashaServerIP, sashaServerPort);
                else
                    getSashaNode(ep);
                Owned<INode> sashaserver = createINode(ep);

                Owned<ISashaCommand> cmd = createSashaCommand();
                setSashaCommand(sashaserver, cmd);
                if (!cmd->send(sashaserver))
                {
                    StringBuffer msg("Cannot connect to archive server at ");
                    sashaserver->endpoint().getUrlStr(msg);
                    throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER, "%s", msg.str());
                }

                numberOfWUsReturned = cmd->numIds();
                hasMoreWU = (numberOfWUsReturned > pageSize);
                if (hasMoreWU)
                    numberOfWUsReturned--;

                if (numberOfWUsReturned > 0)
                {
                    SecAccessFlags accessOwn, accessOthers;
                    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

                    for (unsigned i=0; i<numberOfWUsReturned; i++)
                    {
                        const char *csline = cmd->queryId(i);
                        if (!csline || !*csline)
                            continue;

                        StringArray wuDataArray;
                        wuDataArray.appendList(csline, ",");

                        const char* wuid = wuDataArray.item(0);
                        if (isEmpty(wuid))
                        {
                            WARNLOG("Empty WUID in SCA_LIST response"); // JCS->KW - have u ever seen this happen?
                            continue;
                        }
                        const char* owner = wuDataArray.item(1);
                        bool canAccess = chooseWuAccessFlagsByOwnership(context.queryUserId(), owner, accessOwn, accessOthers) >= SecAccess_Read;
                        Owned<IEspECLWorkunit> info = createArchivedWUEntry(wuDataArray, canAccess);
                        archivedWUs.append(*info.getClear());
                    }
                    archivedWUs.sort(compareWuids);

                    archivedWuCache.add(filterStr, "AddWhenAvailable", hasMoreWU, numberOfWUsReturned, archivedWUs);
                }
            }
            return;
        };

        bool getHasMoreWU() { return hasMoreWU; };
        unsigned getNumberOfWUsReturned() { return numberOfWUsReturned; };
    };

    unsigned pageStart = (unsigned) req.getPageStartFrom();
    unsigned pageSize = (unsigned) req.getPageSize();
    if(pageSize < 1)
        pageSize=500;
    IArrayOf<IEspECLWorkunit> archivedWUs;
    Owned<IArchivedWUsReader> archiveWUsReader = new CArchivedWUsReader(context, sashaServerIP, sashaServerPort, archivedWuCache,
        cacheMinutes, pageStart, pageSize, req);
    archiveWUsReader->getArchivedWUs(archivedWUs);

    resp.setWorkunits(archivedWUs);
    resp.setNumWUs(archiveWUsReader->getNumberOfWUsReturned());

    resp.setType("archived only");
    resp.setPageSize(pageSize);
    resp.setPageStartFrom(pageStart+1);
    resp.setPageEndAt(pageStart + archiveWUsReader->getNumberOfWUsReturned());
    if(pageStart > 0)
    { //This is not the first page;
        resp.setFirst(false);
        resp.setPrevPage((pageStart > pageSize) ? pageStart - pageSize: 0);
    }
    if (archiveWUsReader->getHasMoreWU())
        resp.setNextPage(pageStart + pageSize);
    return;
}

bool CWsWorkunitsEx::onWUQuery(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    try
    {
        StringBuffer wuidStr = req.getWuid();
        const char* wuid = wuidStr.trim().str();

        if (req.getType() && strieq(req.getType(), "archived workunits"))
            doWUQueryFromArchive(context, sashaServerIp.get(), sashaServerPort, *archivedWuCache, awusCacheMinutes, req, resp);
        else if(notEmpty(wuid) && looksLikeAWuid(wuid, 'W'))
            doWUQueryBySingleWuid(context, wuid, resp);
        else if (notEmpty(req.getLogicalFile()) && req.getLogicalFileSearchType() && strieq(req.getLogicalFileSearchType(), "Created"))
            doWUQueryByFile(context, req.getLogicalFile(), resp);
        else
            doWUQueryWithSort(context, req, resp);

        resp.setState(req.getState());
        resp.setCluster(req.getCluster());
        resp.setRoxieCluster(req.getRoxieCluster());
        resp.setOwner(req.getOwner());
        resp.setStartDate(req.getStartDate());
        resp.setEndDate(req.getEndDate());

        double version = context.getClientVersion();

        StringBuffer basicQuery;
        addToQueryString(basicQuery, "State", req.getState());
        addToQueryString(basicQuery, "Cluster", req.getCluster());
        addToQueryString(basicQuery, "Owner", req.getOwner());
        addToQueryString(basicQuery, "StartDate", req.getStartDate());
        addToQueryString(basicQuery, "EndDate", req.getEndDate());
        if (version > 1.25 && req.getLastNDays() > -1)
            addToQueryString(basicQuery, "LastNDays", StringBuffer().append(req.getLastNDays()).str());
        addToQueryString(basicQuery, "ECL", req.getECL());
        addToQueryString(basicQuery, "Jobname", req.getJobname());
        addToQueryString(basicQuery, "Type", req.getType());
        if (addToQueryString(basicQuery, "LogicalFile", req.getLogicalFile()))
            addToQueryString(basicQuery, "LogicalFileSearchType", req.getLogicalFileSearchType());
        resp.setFilters(basicQuery.str());

        if (notEmpty(req.getSortby()) && !strstr(basicQuery.str(), StringBuffer(req.getSortby()).append('=').str()))
        {
            resp.setSortby(req.getSortby());
            addToQueryString(basicQuery, "Sortby", req.getSortby());
            if (req.getDescending())
            {
                resp.setDescending(req.getDescending());
                addToQueryString(basicQuery, "Descending", "1");
            }
        }

        resp.setBasicQuery(basicQuery.str());

        StringBuffer s;
        if(notEmpty(req.getECL()))
            resp.setECL(Utils::url_encode(req.getECL(), s).str());
        if(notEmpty(req.getJobname()))
            resp.setJobname(Utils::url_encode(req.getJobname(), s.clear()).str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

void appendResultSet(MemoryBuffer& mb, INewResultSet* result, const char *name, __int64 start, unsigned& count, __int64& total, bool bin, bool xsd, ESPSerializationFormat fmt, const IProperties *xmlns)
{
    if (!result)
        return;

    Owned<IResultSetCursor> cursor(result->createCursor());
    total=result->getNumRows();

    if(bin)
        count = getResultBin(mb, result, (unsigned)start, count);
    else
    {
        struct MemoryBuffer2IStringVal : public CInterface, implements IStringVal
        {
            MemoryBuffer2IStringVal(MemoryBuffer & _buffer) : buffer(_buffer) {}
            IMPLEMENT_IINTERFACE;

            virtual const char * str() const { UNIMPLEMENTED;  }
            virtual void set(const char *val) { buffer.append(strlen(val),val); }
            virtual void clear() { } // support appending only
            virtual void setLen(const char *val, unsigned length) { buffer.append(length, val); }
            virtual unsigned length() const { return buffer.length(); };
            MemoryBuffer & buffer;
        } adaptor(mb);

        if (fmt==ESPSerializationJSON)
            count = getResultJSON(adaptor, result, name, (unsigned) start, count, (xsd) ? "myschema" : NULL);
        else
            count = getResultXml(adaptor, result, name, (unsigned) start, count, (xsd) ? "myschema" : NULL, xmlns);
    }
}

INewResultSet* createFilteredResultSet(INewResultSet* result, IArrayOf<IConstNamedValue>* filterBy)
{
    if (!result || !filterBy || !filterBy->length())
        return NULL;

    Owned<IFilteredResultSet> filter = result->createFiltered();
    const IResultSetMetaData &meta = result->getMetaData();
    unsigned columnCount = meta.getColumnCount();
    ForEachItemIn(i, *filterBy)
    {
        IConstNamedValue &item = filterBy->item(i);
        const char *name = item.getName();
        const char *value = item.getValue();
        if (!name || !*name || !value || !*value)
            continue;

        for(unsigned col = 0; col < columnCount; col++)
        {
            SCMStringBuffer scmbuf;
            meta.getColumnLabel(scmbuf, col);
            if (strieq(scmbuf.str(), name))
            {
                filter->addFilter(col, value);
                break;
            }
        }
    }
    return filter->create();
}

void getWsWuResult(IEspContext &context, const char* wuid, const char *name, const char *logical, unsigned index, __int64 start,
    unsigned& count, __int64& total, IStringVal& resname, bool bin, IArrayOf<IConstNamedValue>* filterBy, MemoryBuffer& mb,
    WUState& wuState, bool xsd=true)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    if(!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid);
    Owned<IConstWUResult> result;

    if (notEmpty(name))
        result.setown(cw->getResultByName(name));
    else if (notEmpty(logical))
    {
        Owned<IConstWUResultIterator> it = &cw->getResults();
        ForEach(*it)
        {
            IConstWUResult &r = it->query();
            SCMStringBuffer filename;
            if(strieq(r.getResultLogicalName(filename).str(), logical))
            {
                result.setown(LINK(&r));
                break;
            }
        }
    }
    else
        result.setown(cw->getResultBySequence(index));

    if (!result)
        throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");
    if (!resname.length())
        result->getResultName(resname);

    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
    SCMStringBuffer logicalName;
    result->getResultLogicalName(logicalName);
    Owned<INewResultSet> rs;
    if (logicalName.length())
    {
        rs.setown(resultSetFactory->createNewFileResultSet(logicalName.str(), cw->queryClusterName())); //MORE is this wrong cluster?
    }
    else
        rs.setown(resultSetFactory->createNewResultSet(result, wuid));
    if (!filterBy || !filterBy->length())
        appendResultSet(mb, rs, name, start, count, total, bin, xsd, context.getResponseFormat(), result->queryResultXmlns());
    else
    {
        Owned<INewResultSet> filteredResult = createFilteredResultSet(rs, filterBy);
        appendResultSet(mb, filteredResult, name, start, count, total, bin, xsd, context.getResponseFormat(), result->queryResultXmlns());
    }

    wuState = cw->getState();
}

void checkFileSizeLimit(unsigned long xmlSize, unsigned long sizeLimit)
{
    if ((sizeLimit > 0) && (xmlSize > sizeLimit))
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_FILE,
            "The file size (%ld bytes) exceeds the size limit (%ld bytes). You may set 'Option > 1' or use 'Download_XML' link to get compressed file.",
            xmlSize, sizeLimit);
}

void openSaveFile(IEspContext &context, int opt, __int64 sizeLimit, const char* filename, const char* origMimeType, MemoryBuffer& buf, IEspWULogFileResponse &resp)
{
    if (opt < 1)
    {
        checkFileSizeLimit(buf.length(), sizeLimit);
        resp.setThefile(buf);
        resp.setThefile_mimetype(origMimeType);
    }
    else if (opt < 2)
    {
        checkFileSizeLimit(buf.length(), sizeLimit);
        StringBuffer headerStr("attachment;");
        if (filename && *filename)
        {
            const char* pFileName = strrchr(filename, PATHSEPCHAR);
            if (pFileName)
                headerStr.appendf("filename=%s", pFileName+1);
            else
                headerStr.appendf("filename=%s", filename);
        }

        MemoryBuffer buf0;
        unsigned i = 0;
        char* p = (char*) buf.toByteArray();
        while (i < buf.length())
        {
            if (p[0] != 10)
                buf0.append(p[0]);
            else
                buf0.append(0x0d);

            p++;
            i++;
        }
        resp.setThefile(buf);
        resp.setThefile_mimetype(origMimeType);
        context.addCustomerHeader("Content-disposition", headerStr.str());
    }
    else
    {
#ifndef _USE_ZLIB
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
#else
        StringBuffer fileNameStr, headerStr("attachment;");
        if (notEmpty(filename))
        {
            fileNameStr.append(filename);
            headerStr.append("filename=").append(filename).append((opt>2) ? ".gz" : ".zip");
        }
        else
            fileNameStr.append("file");

        StringBuffer ifname;
        ifname.appendf("%s%sT%xAT%x", TEMPZIPDIR, PATHSEPSTR, (unsigned)(memsize_t)GetCurrentThreadId(), msTick()).append((opt>2)? "" : ".zip");

        IZZIPor* Zipor = createZZIPor();
        int ret = 0;
        if (opt > 2)
            ret = Zipor->gzipToFile(buf.length(), (void*)buf.toByteArray(), ifname.str());
        else
            ret = Zipor->zipToFile(buf.length(), (void*)buf.toByteArray(), fileNameStr.str(), ifname.str());
        releaseIZ(Zipor);

        if (ret < 0)
        {
            Owned<IFile> rFile = createIFile(ifname.str());
            if (rFile->exists())
                rFile->remove();
            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");
        }

        Owned <IFile> rf = createIFile(ifname.str());
        if (!rf->exists())
            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"The data cannot be compressed.");

        MemoryBuffer out;
        Owned <IFileIO> fio = rf->open(IFOread);
        read(fio, 0, (size32_t) rf->size(), out);
        resp.setThefile(out);
        fio.clear();
        rf->remove();

        resp.setThefile_mimetype((opt > 2) ? "application/x-gzip" : "application/zip");
        context.addCustomerHeader("Content-disposition", headerStr.str());
#endif
    }
}

bool CWsWorkunitsEx::onWUFile(IEspContext &context,IEspWULogFileRequest &req, IEspWULogFileResponse &resp)
{
    try
    {
        StringBuffer wuidStr = req.getWuid();
        const char* wuidIn = wuidStr.trim().str();
        if (wuidIn && *wuidIn)
        {
            if (!looksLikeAWuid(wuidIn, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID");

            ensureWsWorkunitAccess(context, wuidIn, SecAccess_Read);
        }

        StringAttr wuid(wuidIn);
        if (wuid.isEmpty() && notEmpty(req.getQuerySet()) && notEmpty(req.getQuery()))
        {
            Owned<IPropertyTree> registry = getQueryRegistry(req.getQuerySet(), false);
            if (!registry)
                throw MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "Queryset %s not found", req.getQuerySet());
            Owned<IPropertyTree> query = resolveQueryAlias(registry, req.getQuery());
            if (!query)
                throw MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query %s not found", req.getQuery());
            resp.setQuerySet(req.getQuerySet());
            resp.setQueryName(query->queryProp("@name"));
            resp.setQueryId(query->queryProp("@id"));
            wuid.set(query->queryProp("@wuid"));
        }

        int opt = req.getOption();
        if (!wuid.isEmpty())
        {
            resp.setWuid(wuid.get());
            MemoryBuffer mb;
            WsWuInfo winfo(context, wuid);
            if (strieq(File_ArchiveQuery, req.getType()))
            {
                winfo.getWorkunitArchiveQuery(mb);
                openSaveFile(context, opt, req.getSizeLimit(), "ArchiveQuery.xml", HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if (strieq(File_Cpp,req.getType()) && notEmpty(req.getName()))
            {
                winfo.getWorkunitCpp(req.getName(), req.getDescription(), req.getIPAddress(),mb, opt > 0);
                openSaveFile(context, opt, req.getSizeLimit(), req.getName(), HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_DLL,req.getType()))
            {
                StringBuffer name;
                winfo.getWorkunitDll(name, mb);
                resp.setFileName(name.str());
                resp.setDaliServer(daliServers.get());
                openSaveFile(context, opt, req.getSizeLimit(), req.getName(), HTTP_TYPE_OCTET_STREAM, mb, resp);
            }
            else if (strieq(File_Res,req.getType()))
            {
                winfo.getWorkunitResTxt(mb);
                openSaveFile(context, opt, req.getSizeLimit(), "res.txt", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strncmp(req.getType(), File_ThorLog, 7) == 0)
            {
                winfo.getWorkunitThorLog(req.getName(), mb);
                openSaveFile(context, opt, req.getSizeLimit(), "thormaster.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_ThorSlaveLog,req.getType()))
            {
                StringBuffer logDir;
                getConfigurationDirectory(directories, "log", "thor", req.getProcess(), logDir);

                winfo.getWorkunitThorSlaveLog(req.getClusterGroup(), req.getIPAddress(), req.getLogDate(), logDir.str(), req.getSlaveNumber(), mb, false);
                openSaveFile(context, opt, req.getSizeLimit(), "ThorSlave.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_EclAgentLog,req.getType()))
            {
                winfo.getWorkunitEclAgentLog(req.getName(), req.getProcess(), mb);
                openSaveFile(context, opt, req.getSizeLimit(), "eclagent.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_XML,req.getType()) && notEmpty(req.getName()))
            {
                const char* name  = req.getName();
                const char* ptr = strrchr(name, '/');
                if (ptr)
                    ptr++;
                else
                    ptr = name;

                winfo.getWorkunitAssociatedXml(name, req.getIPAddress(), req.getPlainText(), req.getDescription(), opt > 0, true, mb);
                openSaveFile(context, opt, req.getSizeLimit(), ptr, HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if (strieq(File_XML,req.getType()) || strieq(File_WUECL,req.getType()))
            {
                StringBuffer mimeType, fileName;
                if (strieq(File_WUECL,req.getType()))
                {
                    fileName.setf("%s.ecl", wuid.get());
                    winfo.getWorkunitQueryShortText(mb);
                    mimeType.set(HTTP_TYPE_TEXT_PLAIN);
                }
                else
                {
                    fileName.setf("%s.xml", wuid.get());
                    winfo.getWorkunitXml(req.getPlainText(), mb);
                    if (opt < 2)
                    {
                        const char* plainText = req.getPlainText();
                        if (plainText && (!stricmp(plainText, "yes")))
                            mimeType.set(HTTP_TYPE_TEXT_PLAIN);
                        else
                            mimeType.set(HTTP_TYPE_APPLICATION_XML);
                    }
                    else
                    {
                        mimeType.set(HTTP_TYPE_APPLICATION_XML);
                    }
                }
                openSaveFile(context, opt, req.getSizeLimit(), fileName.str(), mimeType.str(), mb, resp);
            }
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


bool CWsWorkunitsEx::onWUResultBin(IEspContext &context,IEspWUResultBinRequest &req, IEspWUResultBinResponse &resp)
{
    try
    {
        StringBuffer wuidStr = req.getWuid();
        const char* wuidIn = wuidStr.trim().str();
        if (wuidIn && *wuidIn)
        {
            if (!looksLikeAWuid(wuidIn, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuidIn);

            ensureWsWorkunitAccess(context, wuidIn, SecAccess_Read);
        }

        MemoryBuffer mb;
        __int64 total=0;
        __int64 start = req.getStart() > 0 ? req.getStart() : 0;
        unsigned count = req.getCount(), requested=count;
        IArrayOf<IConstNamedValue>* filterBy = &req.getFilterBy();
        SCMStringBuffer name;

        WUState wuState = WUStateUnknown;
        bool bin = (req.getFormat() && strieq(req.getFormat(),"raw"));
        if (notEmpty(wuidIn) && notEmpty(req.getResultName()))
            getWsWuResult(context, wuidIn, req.getResultName(), NULL, 0, start, count, total, name, bin, filterBy, mb, wuState);
        else if (notEmpty(wuidIn) && (req.getSequence() >= 0))
            getWsWuResult(context, wuidIn, NULL, NULL, req.getSequence(), start, count, total, name, bin,filterBy, mb, wuState);
        else if (notEmpty(req.getLogicalName()))
        {
            const char* logicalName = req.getLogicalName();
            StringBuffer wuid;
            getWuidFromLogicalFileName(context, logicalName, wuid);
            if (!wuid.length())
                throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.",logicalName);
            getWsWuResult(context, wuid.str(), NULL, logicalName, 0, start, count, total, name, bin, filterBy, mb, wuState);
        }
        else
            throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");

        if(stricmp(req.getFormat(),"xls")==0)
        {
            Owned<IProperties> params(createProperties());
            params->setProp("showCount",0);
            StringBuffer xml;
            xml.append("<WUResultExcel><Result>").append(mb.length(), mb.toByteArray()).append("</Result></WUResultExcel>");
            if (xml.length() > MAXXLSTRANSFER)
                throw MakeStringException(ECLWATCH_TOO_BIG_DATA_SET, "The data set is too big to be converted to an Excel file. Please use the gzip link to download a compressed XML data file.");

            StringBuffer xls;
            xsltTransform(xml.str(), StringBuffer(getCFD()).append("./smc_xslt/result.xslt").str(), params, xls);

            MemoryBuffer out;
            out.setBuffer(xls.length(), (void*)xls.str());
            resp.setResult(out);
            resp.setResult_mimetype("application/vnd.ms-excel");
        }
#ifdef _USE_ZLIB
        else if(strieq(req.getFormat(),"zip") || strieq(req.getFormat(),"gzip"))
        {
            bool gzip = strieq(req.getFormat(),"gzip");
            StringBuffer xml("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            xml.append("<Result>").append(mb.length(),mb.toByteArray()).append("</Result>");

            VStringBuffer ifname("%s%sT%xAT%x%s", TEMPZIPDIR, PATHSEPSTR, (unsigned)(memsize_t)GetCurrentThreadId(), msTick(), gzip ? "" : ".zip");
            IZZIPor* Zipor = createZZIPor();
            int ret = 0;
            if (gzip)
                ret = Zipor->gzipToFile(xml.length(), (void*)xml.str(), ifname.str());
            else
                ret = Zipor->zipToFile(xml.length(), (void*)xml.str(), "WUResult.xml", ifname.str());
            releaseIZ(Zipor);

            if (ret < 0)
            {
                Owned<IFile> rFile = createIFile(ifname.str());
                if (rFile->exists())
                    rFile->remove();
                throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA, "The data cannot be compressed.");
            }

            MemoryBuffer out;
            Owned <IFile> rf = createIFile(ifname.str());
            if (rf->exists())
            {
                Owned <IFileIO> fio = rf->open(IFOread);
                read(fio, 0, (size32_t) rf->size(), out);
                resp.setResult(out);
            }

            if (gzip)
            {
                resp.setResult_mimetype("application/x-gzip");
                context.addCustomerHeader("Content-disposition", "attachment;filename=WUResult.xml.gz");
            }
            else
            {
                resp.setResult_mimetype("application/zip");
                context.addCustomerHeader("Content-disposition", "attachment;filename=WUResult.xml.zip");
            }

            Owned<IFile> rFile = createIFile(ifname.str());
            if (rFile->exists())
                rFile->remove();
        }
#endif
        else
        {
            resp.setResult(mb);
        }

        resp.setName(name.str());
        resp.setWuid(wuidIn);
        resp.setSequence(req.getSequence());
        resp.setStart(start);
        if (requested > total)
            requested = (unsigned)total;
        resp.setRequested(requested);
        resp.setCount(count);
        resp.setTotal(total);
        resp.setFormat(req.getFormat());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUResultSummary(IEspContext &context, IEspWUResultSummaryRequest &req, IEspWUResultSummaryResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUResultSummary", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        resp.setWuid(wuid.str());
        resp.setSequence(req.getSequence());

        IArrayOf<IEspECLResult> results;
        Owned<IConstWUResult> r = cw->getResultBySequence(req.getSequence());
        if (r)
        {
            WsWuInfo winfo(context, cw);
            winfo.getResult(*r, results, 0);
            resp.setFormat(r->getResultFormat());
            resp.setResult(results.item(0));
         }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void getFileResults(IEspContext &context, const char* logicalName, const char* cluster,__int64 start, unsigned& count,__int64& total,
        IStringVal& resname,bool bin, IArrayOf<IConstNamedValue>* filterBy, MemoryBuffer& buf, bool xsd)
{
    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
    Owned<INewResultSet> result(resultSetFactory->createNewFileResultSet(logicalName, cluster));
    if (!filterBy || !filterBy->length())
        appendResultSet(buf, result, resname.str(), start, count, total, bin, xsd, context.getResponseFormat(), NULL);
    else
    {
        Owned<INewResultSet> filteredResult = createFilteredResultSet(result, filterBy);
        appendResultSet(buf, filteredResult, resname.str(), start, count, total, bin, xsd, context.getResponseFormat(), NULL);
    }
}

void getWorkunitCluster(IEspContext &context, const char* wuid, SCMStringBuffer& cluster, bool checkArchiveWUs)
{
    if (isEmpty(wuid))
        return;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
    if (cw)
        cluster.set(cw->queryClusterName());
    else if (checkArchiveWUs)
    {
        Owned<IPropertyTree> wuProps;// = getArchivedWorkUnitProperties(wuid);
        if (wuProps)
            cluster.set(wuProps->queryProp("@clusterName"));
    }
}


bool CWsWorkunitsEx::onWUResult(IEspContext &context, IEspWUResultRequest &req, IEspWUResultResponse &resp)
{
    try
    {
        StringBuffer wuidStr = req.getWuid();
        const char* wuid = wuidStr.trim().str();
        if (wuid && *wuid)
        {
            if (!looksLikeAWuid(wuid, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid);

            ensureWsWorkunitAccess(context, wuid, SecAccess_Read);
        }

        MemoryBuffer mb;
        SCMStringBuffer name;

        __int64 total=0;
        __int64 start = req.getStart() > 0 ? req.getStart() : 0;
        unsigned count=req.getCount() ? req.getCount() : 100, requested=count;
        unsigned seq = req.getSequence();
        bool inclXsd = !req.getSuppressXmlSchema();

        VStringBuffer filter("start=%" I64F "d;count=%d", start, count);
        addToQueryString(filter, "clusterName", req.getCluster(), ';');
        addToQueryString(filter, "logicalName", req.getLogicalName(), ';');
        if (wuid && *wuid)
            addToQueryString(filter, "wuid", wuid, ';');
        addToQueryString(filter, "resultName", req.getResultName(), ';');
        filter.appendf(";seq=%d;", seq);
        if (inclXsd)
            filter.append("xsd;");
        if (context.getResponseFormat()==ESPSerializationJSON)
            filter.append("json;");
        IArrayOf<IConstNamedValue>* filterBy = &req.getFilterBy();
        ForEachItemIn(i, *filterBy)
        {
            IConstNamedValue &item = filterBy->item(i);
            const char *name = item.getName();
            const char *value = item.getValue();
            if (name && *name && value && *value)
                addToQueryString(filter, name, value, ';');
        }

        const char* logicalName = req.getLogicalName();
        const char* clusterName = req.getCluster();
        const char* resultName = req.getResultName();

        Owned<DataCacheElement> data = dataCache->lookup(context, filter, awusCacheMinutes);
        if (data)
        {
            mb.append(data->m_data.c_str());
            name.set(data->m_name.c_str());
            logicalName = data->m_logicalName.c_str();
            wuid = data->m_wuid.c_str();
            resultName = data->m_resultName.c_str();
            seq = data->m_seq;
            start = data->m_start;
            count = data->m_rowcount;
            requested = (unsigned)data->m_requested;
            total = data->m_total;

            if (notEmpty(logicalName))
                resp.setLogicalName(logicalName);
            else
            {
                if (notEmpty(wuid))
                    resp.setWuid(wuid);
                resp.setSequence(seq);
            }
        }
        else
        {
            WUState wuState = WUStateUnknown;
            if(logicalName && *logicalName)
            {
                StringBuffer lwuid;
                getWuidFromLogicalFileName(context, logicalName, lwuid);
                SCMStringBuffer cluster;
                if (lwuid.length())
                    getWorkunitCluster(context, lwuid.str(), cluster, true);
                if (cluster.length())
                {
                    getFileResults(context, logicalName, cluster.str(), start, count, total, name, false, filterBy, mb, inclXsd);
                    resp.setLogicalName(logicalName);
                }
                else if (notEmpty(clusterName))
                {
                    getFileResults(context, logicalName, clusterName, start, count, total, name, false, filterBy, mb, inclXsd);
                    resp.setLogicalName(logicalName);
                }
                else
                    throw MakeStringException(ECLWATCH_INVALID_INPUT,"Need valid target cluster to browse file %s.",logicalName);

                Owned<IWorkUnitFactory> wf = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                Owned<IConstWorkUnit> cw = wf->openWorkUnit(lwuid.str());
                if (cw)
                    wuState = cw->getState();
            }
            else if (notEmpty(wuid) && notEmpty(resultName))
            {
                name.set(resultName);
                getWsWuResult(context, wuid, resultName, NULL, 0, start, count, total, name, false, filterBy, mb, wuState, inclXsd);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            else
            {
                getWsWuResult(context, wuid, NULL, NULL, seq, start, count, total, name, false, filterBy, mb, wuState, inclXsd);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            mb.append(0);

            if (requested > total)
                requested = (unsigned)total;

            switch (wuState)
            {
                 case WUStateCompleted:
                 case WUStateAborted:
                 case WUStateFailed:
                 case WUStateArchived:
                     dataCache->add(filter, mb.toByteArray(), name.str(), logicalName, wuid, resultName, seq, start, count, requested, total);
                     break;
            }
        }

        resp.setName(name.str());
        resp.setStart(start);
        if (clusterName && *clusterName)
            resp.setCluster(clusterName);
        resp.setRequested(requested);
        resp.setCount(count);
        resp.setTotal(total);
        resp.setResult(mb.toByteArray());

        context.queryXslParameters()->setProp("escapeResults","1");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

void getScheduledWUs(IEspContext &context, const char *stateReq, const char *serverName, const char *eventName, IArrayOf<IEspScheduledWU> & results)
{
    double version = context.getClientVersion();
    if (notEmpty(serverName))
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IScheduleReader> reader = getScheduleReader(serverName, eventName);
        Owned<IScheduleReaderIterator> it(reader->getIterator());
        while(it->isValidEventName())
        {
            StringBuffer ieventName;
            it->getEventName(ieventName);
            while(it->isValidEventText())
            {
                StringBuffer ieventText;
                it->getEventText(ieventText);
                while(it->isValidWuid())
                {
                    StringBuffer wuid;
                    it->getWuid(wuid);
                    if (wuid.length())
                    {
                        bool match = false;
                        unsigned stateID = WUStateUnknown;
                        StringBuffer jobName, owner;
                        SCMStringBuffer state;
                        try
                        {
                            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
                            if (!cw && (!stateReq || !*stateReq))
                            	match = true;
                            else if (cw)
                            {
                                if ((cw->getState() == WUStateScheduled) && cw->aborting())
                                {
                                    stateID = WUStateAborting;
                                    state.set("aborting");
                                }
                                else
                                {
                                    stateID = cw->getState();
                                    state.set(cw->queryStateDesc());
                                }

                                if (!stateReq || !*stateReq || strieq(stateReq, state.str()))
                                {
                                    match = true;
                                    jobName.set(cw->queryJobName());
                                    owner.set(cw->queryUser());
                                }
                            }
                        }
                        catch (IException *e)
                        {
                            EXCLOG(e, "Get scheduled WUs");
                            e->Release();
                        }
                        if (!match)
                        {
                            it->nextWuid();
                            continue;
                        }

                        Owned<IEspScheduledWU> scheduledWU = createScheduledWU("");
                        scheduledWU->setWuid(wuid.str());
                        scheduledWU->setCluster(serverName);
                        if (ieventName.length())
                            scheduledWU->setEventName(ieventName.str());
                        if (ieventText.str())
                            scheduledWU->setEventText(ieventText.str());
                        if (jobName.length())
                            scheduledWU->setJobName(jobName.str());
                        if (version >= 1.51)
                        {
                            if (owner.length())
                                scheduledWU->setOwner(owner.str());
                            if (state.length())
                            {
                                scheduledWU->setStateID(stateID);
                                scheduledWU->setState(state.str());
                            }
                        }
                        results.append(*scheduledWU.getLink());
                    }
                    it->nextWuid();
                }
                it->nextEventText();
            }
            it->nextEventName();
        }
    }

    return;
}


bool CWsWorkunitsEx::onWUShowScheduled(IEspContext &context, IEspWUShowScheduledRequest & req, IEspWUShowScheduledResponse & resp)
{
    try
    {
        DBGLOG("WUShowScheduled");

        const char *clusterName = req.getCluster();
        const char *eventName = req.getEventName();
        const char *state = req.getState();

        IArrayOf<IEspScheduledWU> results;
        if(notEmpty(req.getPushEventName()))
            resp.setPushEventName(req.getPushEventName());
        if(notEmpty(req.getPushEventText()))
            resp.setPushEventText(req.getPushEventText());

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> environment = factory->openEnvironment();
        Owned<IPropertyTree> root = &environment->getPTree();
        if (!root)
            throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");

        unsigned i = 0;
        Owned<IPropertyTreeIterator> ic = root->getElements("Software/Topology/Cluster");
        IArrayOf<IEspServerInfo> servers;
        ForEach(*ic)
        {
            IPropertyTree &cluster = ic->query();
            const char *iclusterName = cluster.queryProp("@name");
            if (isEmpty(iclusterName))
                continue;

            if(isEmpty(clusterName))
                getScheduledWUs(context, state, iclusterName, eventName, results);
            else if (strieq(clusterName, iclusterName))
            {
                getScheduledWUs(context, state, clusterName, eventName, results);
                resp.setClusterSelected(i+1);
            }

            Owned<IEspServerInfo> server = createServerInfo("");
            server->setName(iclusterName);
            servers.append(*server.getLink());
            i++;
        }

        if (servers.length())
            resp.setClusters(servers);
        if (results.length())
            resp.setWorkunits(results);

        bool first=false;
        StringBuffer Query("PageFrom=Scheduler");
        appendUrlParameter(Query, "EventName", eventName, first);
        appendUrlParameter(Query, "ECluster", clusterName, first);
        resp.setQuery(Query.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUExport(IEspContext &context, IEspWUExportRequest &req, IEspWUExportResponse &resp)
{
    try
    {
        if (req.getECL() && *req.getECL())
            throw makeStringException(0, "WUExport no longer supports filtering by ECL text");
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        WsWuSearch ws(context, req.getOwner(), req.getState(), req.getCluster(), req.getStartDate(), req.getEndDate(), req.getJobname());

        StringBuffer xml("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Workunits>");
        for(WsWuSearch::iterator it=ws.begin(); it!=ws.end(); it++)
        {
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(it->c_str());
            if (cw)
                exportWorkUnitToXML(cw, xml, true, false, true);
        }
        xml.append("</Workunits>");

        MemoryBuffer mb;
        mb.setBuffer(xml.length(),(void*)xml.str());
        resp.setExportData(mb);
        resp.setExportData_mimetype(HTTP_TYPE_APPLICATION_XML);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUListLocalFileRequired(IEspContext& context, IEspWUListLocalFileRequiredRequest& req, IEspWUListLocalFileRequiredResponse& resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUListLocalFileRequired", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Workunit %s not found.", wuid.str());

        IArrayOf<IEspLogicalFileUpload> localFiles;
        Owned<IConstLocalFileUploadIterator> it = cw->getLocalFileUploads();
        ForEach(*it)
        {
            Owned<IConstLocalFileUpload> file = it->get();
            if(!file)
                continue;

            Owned<IEspLogicalFileUpload> up = createLogicalFileUpload();

            SCMStringBuffer s;
            up->setType(file->queryType());
            up->setSource(file->getSource(s).str());
            up->setDestination(file->getDestination(s).str());
            up->setEventTag(file->getEventTag(s).str());

            localFiles.append(*up.getLink());
        }
        resp.setLocalFileUploads(localFiles);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

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

bool CWsWorkunitsEx::onWUAddLocalFileToWorkunit(IEspContext& context, IEspWUAddLocalFileToWorkunitRequest& req, IEspWUAddLocalFileToWorkunitResponse& resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUAddLocalFileToWorkunit", wuid);
        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Write);
        resp.setWuid(wuid.str());

        const char* varname = req.getName();
        if (isEmpty(varname))
        {
            resp.setResult("Name is not defined!");
            return true;
        }
        resp.setName(varname);

        wsEclType type = (wsEclType) req.getType();
        const char *val = req.getVal();
        unsigned len = req.getLength();

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        WorkunitUpdate wu(factory->updateWorkUnit(wuid.str()));
        if (!wu)
        {
            resp.setResult("Workunit not found!");
            return true;
        }

        Owned<IWUResult> wuRslt = wu->updateResultByName(varname);
        if (isEmpty(val))
            val=req.getDefVal();
        if (notEmpty(val))
        {
            switch (type)
            {
                case xsdBoolean:
                    wuRslt->setResultBool((strieq(val, "1") || strieq(val, "true") || strieq(val, "on")));
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                case xsdDecimal:
                case xsdFloat:
                case xsdDouble:
                    wuRslt->setResultReal(atof(val));
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                case xsdInteger:
                case xsdNonPositiveInteger:
                case xsdNegativeInteger:
                case xsdLong:
                case xsdInt:
                case xsdShort:
                case xsdByte:
                case xsdNonNegativeInteger:
                case xsdUnsignedLong:
                case xsdUnsignedInt:
                case xsdUnsignedShort:
                case xsdUnsignedByte:
                case xsdPositiveInteger:
                    wuRslt->setResultInt(_atoi64(val));
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
                case tnsEspIntArray:
                case tnsEspStringArray:
                    wuRslt->setResultRaw(len, val, ResultFormatXmlSet);
                    break;
                case tnsRawDataFile:
                    wuRslt->setResultRaw(len, val, ResultFormatRaw);
                    break;
                case tnsXmlDataSet:
                    wuRslt->setResultRaw(len, val, ResultFormatXml);
                    break;
                case tnsCsvDataFile:
                case xsdBase64Binary:   //tbd
                case xsdHexBinary:
                    break;
                default:
                    wuRslt->setResultString(val, len);
                    wuRslt->setResultStatus(ResultStatusSupplied);
                    break;
            }
        }
        resp.setResult("Result has been set as required!");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

void getClusterConfig(char const * clusterType, char const * clusterName, char const * processName, StringBuffer& netAddress)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> environment = factory->openEnvironment();
    Owned<IPropertyTree> pRoot = &environment->getPTree();

    VStringBuffer xpath("Software/%s[@name='%s']", clusterType, clusterName);
    IPropertyTree* pCluster = pRoot->queryPropTree(xpath.str());
    if (!pCluster)
        throw MakeStringException(ECLWATCH_CLUSTER_NOT_IN_ENV_INFO, "'%s %s' is not defined.", clusterType, clusterName);

    const char* port = pCluster->queryProp(xpath.set(processName).append("@port").str());
    const char* computer = pCluster->queryProp(xpath.set(processName).append("@computer").str());
    if (isEmpty(computer))
        throw MakeStringException(ECLWATCH_INVALID_CLUSTER_INFO, "'%s %s: %s' is not defined.", clusterType, clusterName, processName);

    Owned<IConstMachineInfo> pMachine = environment->getMachine(computer);
    if (pMachine)
    {
        StringBufferAdaptor s(netAddress);
        pMachine->getNetAddress(s);
#ifdef MACHINE_IP
        if (streq(netAddress.str(), "."))
            netAddress = MACHINE_IP;
#endif
        netAddress.append(':').append(port);
    }

    return;
}

bool CWsWorkunitsEx::onWUProcessGraph(IEspContext &context,IEspWUProcessGraphRequest &req, IEspWUProcessGraphResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUProcessGraph", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        if (isEmpty(req.getName()))
            throw MakeStringException(ECLWATCH_GRAPH_NOT_FOUND, "Please specify a graph name.");

        Owned<IConstWUGraph> graph = cw->getGraph(req.getName());
        if (!graph)
            throw MakeStringException(ECLWATCH_GRAPH_NOT_FOUND, "Invalid graph name: %s for %s", req.getName(), wuid.str());

        StringBuffer xml;
        Owned<IPropertyTree> xgmml = graph->getXGMMLTree(true); // merge in graph progress information
        toXML(xgmml.get(), xml);
        resp.setTheGraph(xml.str());
    }

    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool isRunning(IConstWorkUnit &cw)
{
    // MORE - move into workunit interface
    switch (cw.getState())
    {
    case WUStateFailed:
    case WUStateAborted:
    case WUStateCompleted:
        return false;
    default:
        return true;
    }
}

void CWsWorkunitsEx::readGraph(IEspContext& context, const char* subGraphId, WUGraphIDType& id, bool running,
    IConstWUGraph* graph, IArrayOf<IEspECLGraphEx>& graphs)
{
    SCMStringBuffer name, label, type;
    graph->getName(name);
    graph->getLabel(label);
    graph->getTypeName(type);

    Owned<IEspECLGraphEx> g = createECLGraphEx("","");
    g->setName(name.str());
    g->setLabel(label.str());
    g->setType(type.str());

    WUGraphState graphState = graph->getState();
    if (running && (WUGraphRunning == graphState))
    {
        g->setRunning(true);
        g->setRunningId(id);
    }
    else if (context.getClientVersion() > 1.20)
    {
        if (WUGraphComplete == graphState)
            g->setComplete(true);
        else if (WUGraphFailed == graphState)
            g->setFailed(true);
    }

    Owned<IPropertyTree> xgmml = graph->getXGMMLTree(true);

    // New functionality, if a subgraph id is specified and we only want to load the xgmml for that subgraph
    // then we need to conditionally pull a propertytree from the xgmml graph one and use that for the xgmml.

    //JCSMORE this should be part of the API and therefore allow *only* the subtree to be pulled from the backend.

    StringBuffer xml;
    if (notEmpty(subGraphId))
    {
        VStringBuffer xpath("//node[@id='%s']", subGraphId);
        toXML(xgmml->queryPropTree(xpath.str()), xml);
    }
    else
        toXML(xgmml, xml);

    g->setGraph(xml.str());

    graphs.append(*g.getClear());
}

bool CWsWorkunitsEx::onWUGetGraph(IEspContext& context, IEspWUGetGraphRequest& req, IEspWUGetGraphResponse& resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUGetGraph", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        WUGraphIDType id;
        SCMStringBuffer runningGraph;
        bool running = (isRunning(*cw) && cw->getRunningGraph(runningGraph,id));

        IArrayOf<IEspECLGraphEx> graphs;
        if (isEmpty(req.getGraphName())) // JCS->GS - is this really required??
        {
            Owned<IConstWUGraphIterator> it = &cw->getGraphs(GraphTypeAny);
            ForEach(*it)
                readGraph(context, req.getSubGraphId(), id, running, &it->query(), graphs);
        }
        else
        {
            Owned<IConstWUGraph> graph = cw->getGraph(req.getGraphName());
            if (graph)
                readGraph(context, req.getSubGraphId(), id, running, graph, graphs);
        }
        resp.setGraphs(graphs);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onGVCAjaxGraph(IEspContext &context, IEspGVCAjaxGraphRequest &req, IEspGVCAjaxGraphResponse &resp)
{
    try
    {
        resp.setName(req.getName());
        resp.setGraphName(req.getGraphName());
        resp.setGraphType("eclwatch");

        double version = context.getClientVersion();
        if (version > 1.19)
            resp.setSubGraphId(req.getSubGraphId());
        if (version > 1.20)
            resp.setSubGraphOnly(req.getSubGraphOnly());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGraphInfo(IEspContext &context,IEspWUGraphInfoRequest &req, IEspWUGraphInfoResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUGraphInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        ensureWsWorkunitAccess(context, *cw, SecAccess_Write);

        resp.setWuid(wuid.str());
        resp.setName(req.getName());
        resp.setRunning(isRunning(*cw));
        if (notEmpty(req.getGID()))
            resp.setGID(req.getGID());
        if(!req.getBatchWU_isNull())
            resp.setBatchWU(req.getBatchWU());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGVCGraphInfo(IEspContext &context,IEspWUGVCGraphInfoRequest &req, IEspWUGVCGraphInfoResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUGVCGraphInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        resp.setWuid(wuid.str());
        resp.setName(req.getName());
        resp.setRunning(isRunning(*cw));
        if (notEmpty(req.getGID()))
            resp.setGID(req.getGID());
        if(!req.getBatchWU_isNull())
            resp.setBatchWU(req.getBatchWU());

        StringBuffer xml("<Control><Endpoint><Query id=\"Gordon.Extractor.0\">");
        xml.appendf("<Graph id=\"%s\">", req.getName());
        if (context.getClientVersion() > 1.17)
        {
            xml.append("<Subgraph>");
            xml.append(req.getSubgraphId_isNull() ? 0 : req.getSubgraphId());
            xml.append("</Subgraph>");
        }
        xml.append("</Graph></Query></Endpoint></Control>");
        resp.setTheGraph(xml.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGraphTiming(IEspContext &context, IEspWUGraphTimingRequest &req, IEspWUGraphTimingResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        WsWuHelpers::checkAndTrimWorkunit("WUGraphTiming", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        resp.updateWorkunit().setWuid(wuid.str());

        WsWuInfo winfo(context, cw);
        IArrayOf<IConstECLTimingData> timingData;
        winfo.getGraphTimingData(timingData, 0);
        resp.updateWorkunit().setTimingData(timingData);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

int CWsWorkunitsSoapBindingEx::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    try
    {
        StringBuffer xml;
        StringBuffer xslt;
        if(strieq(method,"WUQuery") || strieq(method,"WUJobList"))
        {
            Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
            Owned<IConstEnvironment> environment = factory->openEnvironment();
            Owned<IPropertyTree> root = &environment->getPTree();
            if (!root)
                throw MakeStringException(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment information.");
            if(strieq(method,"WUQuery"))
            {
                SecAccessFlags accessOwn;
                SecAccessFlags accessOthers;
                getUserWuAccessFlags(context, accessOwn, accessOthers, false);

                xml.append("<WUQuery>");
                if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
                    xml.appendf("<ErrorMessage>Access to workunit is denied.</ErrorMessage>");
                else
                {
                    MapStringTo<bool> added;
                    Owned<IPropertyTreeIterator> it = root->getElements("Software/Topology/Cluster");
                    ForEach(*it)
                    {
                        const char *name = it->query().queryProp("@name");
                        if (notEmpty(name) && !added.getValue(name))
                        {
                            added.setValue(name, true);
                            appendXMLTag(xml, "Cluster", name);
                        }
                    }
                }
                xml.append("</WUQuery>");
                xslt.append(getCFD()).append("./smc_xslt/wuid_search.xslt");
            }
            else if (strieq(method,"WUJobList"))
            {
                StringBuffer cluster, defaultProcess, range;
                request->getParameter("Cluster", cluster);
                request->getParameter("Process",defaultProcess);
                request->getParameter("Range",range);
                Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
                xml.append("<WUJobList>");
                if (range.length())
                    appendXMLTag(xml, "Range", range.str());
                if (clusterInfo)
                {
                    const StringArray &thorInstances = clusterInfo->getThorProcesses();
                    ForEachItemIn(i, thorInstances)
                    {
                        const char* instance = thorInstances.item(i);
                        if (defaultProcess.length() && strieq(instance, defaultProcess.str()))
                            xml.append("<Cluster selected=\"1\">").append(instance).append("</Cluster>");
                        else
                            xml.append("<Cluster>").append(instance).append("</Cluster>");
                    }
                }
                xml.append("<TargetCluster>").append(cluster).append("</TargetCluster>");
                xml.append("</WUJobList>");
                xslt.append(getCFD()).append("./smc_xslt/jobs_search.xslt");
                response->addHeader("Expires", "0");
             }
        }
        if (xslt.length() && xml.length())
        {
            StringBuffer html;
            xsltTransform(xml.str(), xslt.str(), NULL, html);
            response->setContent(html.str());
            response->setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
            response->send();
            return 0;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onGetNotFound(context, request, response, service);
}

bool isDeploymentTypeCompressed(const char *type)
{
    if (type && *type)
        return (0==strncmp(type, "compressed_", strlen("compressed_")));
    return false;
}

const char *skipCompressedTypeQualifier(const char *type)
{
    if (isDeploymentTypeCompressed(type))
        type += strlen("compressed_");
    return type;
}

void deployEclOrArchive(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    NewWsWorkunit wu(context);

    StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit,clear()

    wu->setAction(WUActionCompile);

    StringBuffer name(req.getName());
    if (!name.trim().length() && notEmpty(req.getFileName()))
        splitFilename(req.getFileName(), NULL, NULL, &name, NULL);
    if (name.length())
        wu->setJobName(name.str());

    if (req.getObject().length())
    {
        MemoryBuffer mb;
        const MemoryBuffer *uncompressed = &req.getObject();
        if (isDeploymentTypeCompressed(req.getObjType()))
        {
            fastLZDecompressToBuffer(mb, req.getObject().bufferBase());
            uncompressed = &mb;
        }

        StringBuffer text(uncompressed->length(), uncompressed->toByteArray());
        wu.setQueryText(text.str());
    }
    if (req.getQueryMainDefinition())
        wu.setQueryMain(req.getQueryMainDefinition());
    if (req.getSnapshot())
        wu->setSnapshot(req.getSnapshot());
    if (!req.getResultLimit_isNull())
        wu->setResultLimit(req.getResultLimit());

    wu->commit();
    wu.clear();

    WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), NULL, 0, true, false, false, NULL, NULL, &req.getDebugValues());
    waitForWorkUnitToCompile(wuid.str(), req.getWait());

    WsWuInfo winfo(context, wuid.str());
    winfo.getCommon(resp.updateWorkunit(), WUINFO_All);
    winfo.getExceptions(resp.updateWorkunit(), WUINFO_All);

    name.clear();
    if (notEmpty(resp.updateWorkunit().getJobname()))
        origValueChanged(req.getName(), resp.updateWorkunit().getJobname(), name, false);

    if (name.length()) //non generated user specified name, so override #Workunit('name')
    {
        WorkunitUpdate wx(&winfo.cw->lock());
        wx->setJobName(name.str());
        resp.updateWorkunit().setJobname(name.str());
    }

    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}


StringBuffer &sharedObjectFileName(StringBuffer &filename, const char *name, const char *ext, unsigned copy)
{
    filename.append((name && *name) ? name : "workunit");
    if (copy)
        filename.append('-').append(copy);
    if (notEmpty(ext))
        filename.append(ext);
    return filename;
}

inline StringBuffer &buildFullDllPath(StringBuffer &dllpath, StringBuffer &dllname, const char *dir, const char *name, const char *ext, unsigned copy)
{
    return addPathSepChar(dllpath.set(dir)).append(sharedObjectFileName(dllname, name, ext, copy));
}

void writeSharedObject(const char *srcpath, const MemoryBuffer &obj, const char *dir, StringBuffer &dllpath, StringBuffer &dllname, unsigned crc)
{
    StringBuffer name, ext;
    if (srcpath && *srcpath)
        splitFilename(srcpath, NULL, NULL, &name, &ext);

    unsigned copy=0;
    buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), copy);
    while (checkFileExists(dllpath.str()))
    {
        if (crc && crc == crc_file(dllpath.str()))
        {
            DBGLOG("Workunit dll already exists: %s", dllpath.str());
            return;
        }
        buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), ++copy);
    }
    DBGLOG("Writing workunit dll: %s", dllpath.str());
    Owned<IFile> f = createIFile(dllpath.str());
    Owned<IFileIO> io = f->open(IFOcreate);
    io->write(0, obj.length(), obj.toByteArray());
}

void deploySharedObject(IEspContext &context, StringBuffer &wuid, const char *filename, const char *cluster, const char *name, const MemoryBuffer &obj, const char *dir, const char *xml)
{
    StringBuffer dllpath, dllname;
    StringBuffer srcname(filename);

    unsigned crc = 0;
    Owned<IPropertyTree> srcxml;
    if (xml && *xml)
    {
        srcxml.setown(createPTreeFromXMLString(xml));
        if (srcxml && wuid.length())
        {
            crc = srcxml->getPropInt("Query[1]/Associated[1]/File[@type='dll'][1]/@crc", 0);
            if (crc)
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
                if (cw)
                {
                    //is this a previous copy of same query, or a WUID collision?
                    if (cw->getHash() == (unsigned) srcxml->getPropInt64("@hash", 0))
                    {
                        Owned<IConstWUQuery> query = cw->getQuery();
                        if (query && crc == query->getQueryDllCrc())
                            return;
                    }
                }
            }
        }
    }

    if (!srcname.length())
        srcname.append(name).append(SharedObjectExtension);
    writeSharedObject(srcname.str(), obj, dir, dllpath, dllname, crc);

    NewWsWorkunit wu(context, wuid); //duplicate wuid made unique

    wuid.set(wu->queryWuid());
    wu->setClusterName(cluster);
    wu->commit();

    StringBuffer dllXML;
    if (getWorkunitXMLFromFile(dllpath.str(), dllXML))
    {
        Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit(dllXML.str());
        queryExtendedWU(wu)->copyWorkUnit(embeddedWU, true);
    }

    wu.associateDll(dllpath.str(), dllname.str());

    if (name && *name)
        wu->setJobName(name);

    //clean slate, copy only select items from processed workunit xml
    if (srcxml)
    {
        if (srcxml->hasProp("@jobName"))
            wu->setJobName(srcxml->queryProp("@jobName"));
        if (srcxml->hasProp("@token"))
            wu->setSecurityToken(srcxml->queryProp("@token"));
        if (srcxml->hasProp("Query/Text"))
            wu.setQueryText(srcxml->queryProp("Query/Text"));
    }

    wu->setState(WUStateCompiled);
    wu->commit();
    wu.clear();

    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}

void CWsWorkunitsEx::deploySharedObjectReq(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp, const char *dir, const char *xml)
{
    if (isEmpty(req.getFileName()))
       throw MakeStringException(ECLWATCH_INVALID_INPUT, "File name required when deploying a shared object.");

    const char *cluster = req.getCluster();
    if (isEmpty(cluster))
       throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster name required when deploying a shared object.");

    const MemoryBuffer *uncompressed = &req.getObject();
    MemoryBuffer mb;
    if (isDeploymentTypeCompressed(req.getObjType()))
    {
        fastLZDecompressToBuffer(mb, req.getObject().bufferBase());
        uncompressed = &mb;
    }

    StringBuffer wuid;
    deploySharedObject(context, wuid, req.getFileName(), cluster, req.getName(), *uncompressed, dir, xml);

    WsWuInfo winfo(context, wuid.str());
    winfo.getCommon(resp.updateWorkunit(), WUINFO_All);

    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}

bool CWsWorkunitsEx::onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    const char *type = skipCompressedTypeQualifier(req.getObjType());
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        if (notEmpty(req.getCluster()) && !isValidCluster(req.getCluster()))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", req.getCluster());
        if (!type || !*type)
            throw MakeStringExceptionDirect(ECLWATCH_INVALID_INPUT, "WUDeployWorkunit unspecified object type.");
        if (strieq(type, "archive")|| strieq(type, "ecl_text"))
            deployEclOrArchive(context, req, resp);
        else if (strieq(type, "shared_object"))
            deploySharedObjectReq(context, req, resp, queryDirectory.str());
        else
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "WUDeployWorkunit '%s' unknown object type.", type);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}


void CWsWorkunitsEx::createZAPFile(const char* fileName, size32_t len, const void* data)
{
    if (!fileName || !*fileName)
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"File name not specified.");
    Owned<IFile> wuInfoIFile = createIFile(fileName);
    Owned<IFileIO> wuInfoIO = wuInfoIFile->open(IFOcreate);
    if (wuInfoIO)
        wuInfoIO->write(0, len, data);
}

void CWsWorkunitsEx::cleanZAPFolder(IFile* zipDir, bool removeFolder)
{
    if (!zipDir)
        throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"Invalid file interface for the zip folder.");
    Owned<IDirectoryIterator> iter = zipDir->directoryFiles(NULL,false,false);
    ForEach(*iter)
    {
        OwnedIFile thisFile = createIFile(iter->query().queryFilename());
        if (thisFile->isFile() == foundYes)
            thisFile->remove();
    }
    if (removeFolder)
        zipDir->remove();
}

void CWsWorkunitsEx::addProcessLogfile(Owned<IConstWorkUnit>& cwu, WsWuInfo& winfo, const char* process, const char* path)
{
    Owned<IPropertyTreeIterator> procs = cwu->getProcesses(process, NULL);
    ForEach (*procs)
    {
        StringBuffer logSpec;
        IPropertyTree& proc = procs->query();
        proc.getProp("@log",logSpec);
        if (!logSpec.length())
            continue;
        StringBuffer pid;
        pid.appendf("%d",proc.getPropInt("@pid"));
        MemoryBuffer mb;
        try
        {
            if (strieq(process, "EclAgent"))
                winfo.getWorkunitEclAgentLog(logSpec.str(), pid.str(), mb);
            else if (strieq(process, "Thor"))
                winfo.getWorkunitThorLog(logSpec.str(), mb);
        }
        catch(IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            DBGLOG("Error accessing Process Log file %s: %s", logSpec.str(), s.str());
            mb.append(s.str());
            e->Release();
        }

        if (!mb.length())
            continue;

        const char * logName = logSpec.str();
        for (const char * p=logSpec; *p; p++)
        {
            if (*p == '\\' || *p == '/')
                logName = p+1;
        }
        VStringBuffer fileName("%s%c%s", path, PATHSEPCHAR, logName);
        createZAPFile(fileName.str(), mb.length(), mb.bufferBase());
    }
}

void CWsWorkunitsEx::createZAPWUInfoFile(IEspWUCreateZAPInfoRequest &req, Owned<IConstWorkUnit>& cwu, const char* pathNameStr)
{
    StringBuffer sb;
    sb.append("Workunit:     ").append(cwu->queryWuid()).append("\r\n");
    sb.append("User:         ").append(cwu->queryUser()).append("\r\n");
    sb.append("Build Version:").append(req.getBuildVersion()).append("\r\n");
    sb.append("Cluster:      ").append(cwu->queryClusterName()).append("\r\n");
    if (req.getESPIPAddress())
        sb.append("ESP:          ").append(req.getESPIPAddress()).append("\r\n");
    if (req.getThorIPAddress())
        sb.append("Thor:         ").append(req.getThorIPAddress()).append("\r\n");
    //Exceptions/Warnings/Info
    Owned<IConstWUExceptionIterator> exceptions = &cwu->getExceptions();
    StringBuffer info, warn, err, alert;
    ForEach(*exceptions)
    {
        SCMStringBuffer temp;
        switch (exceptions->query().getSeverity())
        {
        case SeverityInformation:
            info.append("\t").append(exceptions->query().getExceptionMessage(temp)).append("\r\n\r\n");
            break;
        case SeverityWarning:
            warn.append("\t").append(exceptions->query().getExceptionMessage(temp)).append("\r\n\r\n");
            break;
        case SeverityError:
            err.append("\t").append(exceptions->query().getExceptionMessage(temp)).append("\r\n\r\n");
            break;
        case SeverityAlert:
            alert.append("\t").append(exceptions->query().getExceptionMessage(temp)).append("\r\n\r\n");
            break;
        }
    }
    if (err.length())
        sb.append("Exceptions:   ").append("\r\n").append(err);
    if (warn.length())
        sb.append("Warnings:     ").append("\r\n").append(warn);
    if (info.length())
        sb.append("Information:  ").append("\r\n").append(info);
    if (alert.length())
        sb.append("Alert:        ").append("\r\n").append(alert);

    //User provided Information
    sb.append("Problem:      ").append(req.getProblemDescription()).append("\r\n\r\n");
    sb.append("What Changed: ").append(req.getWhatChanged()).append("\r\n\r\n");
    sb.append("Timing:       ").append(req.getWhereSlow()).append("\r\n\r\n");

    VStringBuffer fileName("%s.txt", pathNameStr);
    createZAPFile(fileName.str(), sb.length(), sb.str());
}

void CWsWorkunitsEx::createZAPWUXMLFile(WsWuInfo &winfo, const char* pathNameStr)
{
    MemoryBuffer mb;
    winfo.getWorkunitXml(NULL, mb);
    VStringBuffer fileName("%s.xml", pathNameStr);
    createZAPFile(fileName.str(), mb.length(), mb.bufferBase());
}

void CWsWorkunitsEx::createZAPECLQueryArchiveFiles(Owned<IConstWorkUnit>& cwu, const char* pathNameStr)
{
    Owned<IConstWUQuery> query = cwu->getQuery();
    if(!query)
        return;

    //Add archive if present
    Owned<IConstWUAssociatedFileIterator> iter = &query->getAssociatedFiles();
    ForEach(*iter)
    {
        IConstWUAssociatedFile & cur = iter->query();
        SCMStringBuffer ssb;
        cur.getDescription(ssb);
        if (!strieq(ssb.str(), "archive"))
            continue;

        cur.getName(ssb);
        if (!ssb.length())
            continue;

        StringBuffer fileName, archiveContents;
        try
        {
            archiveContents.loadFile(ssb.str());
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            DBGLOG("Error accessing archive file %s: %s", ssb.str(), s.str());
            archiveContents.insert(0, "Error accessing archive file ").appendf("%s: %s\r\n\r\n", ssb.str(), s.str());
            e->Release();
        }
        fileName.setf("%s.archive", pathNameStr);
        createZAPFile(fileName.str(), archiveContents.length(), archiveContents.str());
        break;
    }

    //Add Query
    SCMStringBuffer temp;
    query->getQueryText(temp);
    if (temp.length())
    {
        VStringBuffer fileName("%s.ecl", pathNameStr);
        createZAPFile(fileName.str(), temp.length(), temp.str());
    }
}

bool CWsWorkunitsEx::onWUCreateZAPInfo(IEspContext &context, IEspWUCreateZAPInfoRequest &req, IEspWUCreateZAPInfoResponse &resp)
{
    try
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cwu = factory->openWorkUnit(req.getWuid());
        if(!cwu.get())
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Cannot open workunit %s.", req.getWuid());

        StringBuffer userName, nameStr, fileName;
        StringBuffer zipFileName, zipFileNameWithPath, zipCommand, folderToZIP;
        if (context.queryUser())
            userName.append(context.queryUser()->getName());
        nameStr.append("ZAPReport_").append(req.getWuid()).append('_').append(userName.str());

        //create a folder for WU ZAP files
        const char* zipFolder = "tempzipfiles" PATHSEPSTR;
        folderToZIP.append(zipFolder).append(nameStr.str());
        Owned<IFile> zipDir = createIFile(folderToZIP.str());
        if (!zipDir->exists())
            zipDir->createDirectory();
        else
            cleanZAPFolder(zipDir, false);

        //create WU ZAP files
        VStringBuffer pathNameStr("%s/%s", folderToZIP.str(), nameStr.str());
        createZAPWUInfoFile(req, cwu, pathNameStr.str());
        createZAPECLQueryArchiveFiles(cwu, pathNameStr.str());

        WsWuInfo winfo(context, cwu);
        createZAPWUXMLFile(winfo, pathNameStr.str());
        addProcessLogfile(cwu, winfo, "EclAgent", folderToZIP.str());
        addProcessLogfile(cwu, winfo, "Thor", folderToZIP.str());

        //Write out to ZIP file
        zipFileName.append(nameStr.str()).append(".zip");
        zipFileNameWithPath.append(zipFolder).append(zipFileName.str());
        pathNameStr.set(folderToZIP.str()).append("/*");

        const char* password = req.getPassword();
        if (password && *password)
            zipCommand.appendf("zip -j --password %s %s %s", password, zipFileNameWithPath.str(), pathNameStr.str());
        else
            zipCommand.appendf("zip -j %s %s", zipFileNameWithPath.str(), pathNameStr.str());
        int zipRet = system(zipCommand.str());

        //Remove the temporary files and the folder
        cleanZAPFolder(zipDir, true);

        if (zipRet != 0)
            throw MakeStringException(ECLWATCH_CANNOT_COMPRESS_DATA,"Failed to execute system command 'zip'. Please make sure that zip utility is installed.");

        //Download ZIP file to user
        Owned<IFile> f = createIFile(zipFileNameWithPath.str());
        Owned<IFileIO> io = f->open(IFOread);
        MemoryBuffer mb;
        void * data = mb.reserve((unsigned)io->size());
        size32_t read = io->read(0, (unsigned)io->size(), data);
        mb.setLength(read);
        resp.setThefile(mb);
        resp.setThefile_mimetype(HTTP_TYPE_OCTET_STREAM);
        StringBuffer headerStr("attachment;filename=");
        headerStr.append(zipFileName.str());
        context.addCustomerHeader("Content-disposition", headerStr.str());
        io->close();
        f->remove();
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGetZAPInfo(IEspContext &context, IEspWUGetZAPInfoRequest &req, IEspWUGetZAPInfoResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWUID();
        WsWuHelpers::checkAndTrimWorkunit("WUGetZAPInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        StringBuffer EspIP, ThorIP;
        resp.setWUID(wuid.str());
        resp.setBuildVersion(getBuildVersion());
        IpAddress ipaddr = queryHostIP();
        ipaddr.getIpText(EspIP);
        resp.setESPIPAddress(EspIP.str());

        //Get Archive
        Owned<IConstWUQuery> query = cw->getQuery();
        if(query)
        {
            SCMStringBuffer queryText;
            query->getQueryText(queryText);
            if (queryText.length() && isArchiveQuery(queryText.str()))
                resp.setArchive(queryText.str());
        }

        //Get Thor IP
        BoolHash uniqueProcesses;
        Owned<IStringIterator> thorInstances = cw->getProcesses("Thor");
        ForEach (*thorInstances)
        {
            SCMStringBuffer processName;
            thorInstances->str(processName);
            if (processName.length() < 1)
                continue;
            bool* found = uniqueProcesses.getValue(processName.str());
            if (found && *found)
                continue;

            uniqueProcesses.setValue(processName.str(), true);
            Owned<IStringIterator> thorLogs = cw->getLogs("Thor", processName.str());
            ForEach (*thorLogs)
            {
                SCMStringBuffer logName;
                thorLogs->str(logName);
                if (!logName.length())
                    continue;

                const char* thorIPPtr = NULL;
                const char* ptr = logName.str();
                while (ptr)
                {
                    if (!thorIPPtr && (*ptr != '/'))
                        thorIPPtr = ptr;
                    else if (thorIPPtr && (*ptr == '/'))
                        break;
                    ptr++;
                }
                if (!thorIPPtr)
                    continue;

                //Found a thor IP
                if (ThorIP.length())
                    ThorIP.append(",");
                if (!*ptr)
                    ThorIP.append(thorIPPtr);
                else
                    ThorIP.append(ptr-thorIPPtr, thorIPPtr);
            }
        }
        if (ThorIP.length())
            resp.setThorIPAddress(ThorIP.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUCheckFeatures(IEspContext &context, IEspWUCheckFeaturesRequest &req, IEspWUCheckFeaturesResponse &resp)
{
    resp.setBuildVersionMajor(BUILD_VERSION_MAJOR);
    resp.setBuildVersionMinor(BUILD_VERSION_MINOR);
    resp.setBuildVersionPoint(BUILD_VERSION_POINT);
    resp.setMaxRequestEntityLength(maxRequestEntityLength);
    resp.updateDeployment().setUseCompression(true);
    return true;
}

static const char * checkGetStatsInput(const char * s)
{
    if (!s || !*s)
        return "*";
    return s;
}

bool CWsWorkunitsEx::onWUGetStats(IEspContext &context, IEspWUGetStatsRequest &req, IEspWUGetStatsResponse &resp)
{
    try
    {
        StringBuffer wuid = req.getWUID();
        WsWuHelpers::checkAndTrimWorkunit("WUInfo", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

        const char* creatorType = checkGetStatsInput(req.getCreatorType());
        const char* creator = checkGetStatsInput(req.getCreator());
        const char* scopeType = checkGetStatsInput(req.getScopeType());
        const char* scope = checkGetStatsInput(req.getScope());
        const char* kind = checkGetStatsInput(req.getKind());
        const char* measure = req.getMeasure();

        StatisticsFilter filter(creatorType, creator, scopeType, scope, measure, kind);
        if (!req.getMinScopeDepth_isNull() && !req.getMaxScopeDepth_isNull())
            filter.setScopeDepth(req.getMinScopeDepth(), req.getMaxScopeDepth());
        else if (!req.getMinScopeDepth_isNull())
            filter.setScopeDepth(req.getMinScopeDepth());

        bool createDescriptions = false;
        if (!req.getCreateDescriptions_isNull())
            createDescriptions = req.getCreateDescriptions();

        WsWuInfo winfo(context, wuid.str());
        IArrayOf<IEspWUStatisticItem> statistics;
        winfo.getStats(filter, createDescriptions, statistics);
        resp.setStatistics(statistics);
        resp.setWUID(wuid.str());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IPropertyTree* CWsWorkunitsEx::getWorkunitArchive(IEspContext &context, WsWuInfo& winfo, const char* wuid, unsigned cacheMinutes)
{
    Owned<WUArchiveCacheElement> wuArchive = wuArchiveCache->lookup(context, wuid, cacheMinutes);
    if (wuArchive)
        return wuArchive->archive.getLink();

    Owned<IPropertyTree> archive = winfo.getWorkunitArchive();
    if (!archive)
        return NULL;

    wuArchiveCache->add(wuid, archive.getLink());
    return archive.getClear();
}

bool CWsWorkunitsEx::onWUListArchiveFiles(IEspContext &context, IEspWUListArchiveFilesRequest &req, IEspWUListArchiveFilesResponse &resp)
{
    try
    {
        const char* wuid = req.getWUID();
        if (isEmpty(wuid))
            throw MakeStringException(ECLWATCH_NO_WUID_SPECIFIED, "No workunit defined.");

        WsWuInfo winfo(context, wuid);
        Owned<IPropertyTree> archive = getWorkunitArchive(context, winfo, wuid, WUARCHIVE_CACHE_MINITES);
        if (!archive)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No workunit archive found for %s.", wuid);

        IArrayOf<IEspWUArchiveModule> modules;
        IArrayOf<IEspWUArchiveFile> files;
        winfo.listArchiveFiles(archive, "", modules, files);
        if (modules.length())
            resp.setArchiveModules(modules);
        if (files.length())
            resp.setFiles(files);
        if (!modules.length() && !files.length())
            resp.setMessage("Files not found");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUGetArchiveFile(IEspContext &context, IEspWUGetArchiveFileRequest &req, IEspWUGetArchiveFileResponse &resp)
{
    try
    {
        const char* wuid = req.getWUID();
        const char* moduleName = req.getModuleName();
        const char* attrName = req.getFileName();
        if (isEmpty(wuid))
            throw MakeStringException(ECLWATCH_NO_WUID_SPECIFIED, "No workunit defined.");
        if (isEmpty(moduleName) && isEmpty(attrName))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "No file name defined.");

        WsWuInfo winfo(context, wuid);
        Owned<IPropertyTree> archive = getWorkunitArchive(context, winfo, wuid, WUARCHIVE_CACHE_MINITES);
        if (!archive)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No workunit archive found for %s.", wuid);

        StringBuffer file;
        winfo.getArchiveFile(archive, moduleName, attrName, req.getPath(), file);
        if (file.length())
            resp.setFile(file.str());
        else
            resp.setMessage("File not found");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
