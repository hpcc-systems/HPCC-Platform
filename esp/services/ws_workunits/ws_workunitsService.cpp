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

#include "ws_workunitsService.hpp"
#include "ws_fs.hpp"

#include "jlib.hpp"
#include "jflz.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "daaudit.hpp"
#include "dautils.hpp"
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

#include "deftype.hpp"
#include "thorcommon.hpp"
#include "thorxmlwrite.hpp"
#include "fvdatasource.hpp"
#include "fvresultset.ipp"
#include "ws_wudetails.hpp"
#include "wuerror.hpp"
#include "TpWrapper.hpp"
#include "LogicFileWrapper.hpp"

#include "rtlformat.hpp"

#include "package.h"
#include "build-config.h"

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

#define ESP_WORKUNIT_DIR "workunits/"
static constexpr const char* zipFolder = "tempzipfiles" PATHSEPSTR;

#define WU_SDS_LOCK_TIMEOUT (5*60*1000) // 5 mins
const unsigned CHECK_QUERY_STATUS_THREAD_POOL_SIZE = 25;
const unsigned MAX_ZAP_BUFFER_SIZE = 10000000; //10M

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

//The ECLWUActionNames[] has to match with the ESPenum ECLWUActions in the ecm file.
static unsigned NumOfECLWUActionNames = 12;
static const char *ECLWUActionNames[] = { "Abort", "Delete", "Deschedule", "Reschedule", "Pause",
    "PauseNow", "Protect", "Unprotect", "Restore", "Resume", "SetToFailed", "Archive", nullptr };

class CECLWUActionsEx : public SoapEnumParamNew<CECLWUActions>
{
public:
    CECLWUActionsEx() : SoapEnumParamNew<CECLWUActions>() { init("ECLWUActions","string", ECLWUActionNames); }
};
static CECLWUActionsEx eclWUActionType;

void setActionResult(const char* wuid, CECLWUActions action, const char* result, const char* strAction, IArrayOf<IConstWUActionResult>* results)
{
    if (!results || !wuid || !*wuid || !result || !*result)
        return;

    Owned<IEspWUActionResult> res = createWUActionResult("", "");
    res->setWuid(wuid);
    res->setAction(strAction);
    res->setResult(result);
    results->append(*res.getClear());
}

bool doAction(IEspContext& context, StringArray& wuids, CECLWUActions action, IProperties* params, IArrayOf<IConstWUActionResult>* results)
{
    if (!wuids.length())
        return true;

    if ((action == CECLWUActions_Restore) || (action == CECLWUActions_Archive))
    {
        StringBuffer msg;
        ForEachItemIn(i, wuids)
        {
            StringBuffer wuidStr(wuids.item(i));
            const char* wuid = wuidStr.trim().str();
            if (isEmpty(wuid))
            {
                msg.appendf("Empty Workunit ID at %u. ", i);
                continue;
            }
            if ((action == CECLWUActions_Archive) && !validateWsWorkunitAccess(context, wuid, SecAccess_Full))
                msg.appendf("Access denied for Workunit %s. ", wuid);
        }
        if (!msg.isEmpty())
            throw makeStringException(ECLWATCH_INVALID_INPUT, msg);

        Owned<ISashaCommand> cmd = archiveOrRestoreWorkunits(wuids, params, action == CECLWUActions_Archive, false);
        ForEachItemIn(idx, wuids)
        {
            StringBuffer reply;
            cmd->getId(idx, reply);

            const char* wuid = wuids.item(idx);
            if ((action == CECLWUActions_Restore) && !validateWsWorkunitAccess(context, wuid, SecAccess_Full))
                reply.appendf("Access denied for Workunit %s. ", wuid);

            AuditSystemAccess(context.queryUserId(), true, "%s", reply.str());
        }
        return true;
    }

    bool bAllSuccess = true;
    const char* strAction = (action < NumOfECLWUActionNames) ? ECLWUActionNames[action] : "Unknown Action";
    for(aindex_t i=0; i<wuids.length();i++)
    {
        StringBuffer wuidStr(wuids.item(i));
        const char* wuid = wuidStr.trim().str();
        if (isEmpty(wuid))
        {
            UWARNLOG("Empty Workunit ID");
            continue;
        }

        try
        {
            if (!looksLikeAWuid(wuid, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid);

            PROGLOG("%s %s", strAction, wuid);
            if (action == CECLWUActions_EventDeschedule)
            {
                if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Full, false)
                    || !context.validateFeatureAccess(OTHERS_WU_ACCESS, SecAccess_Full, false))
                    ensureWsWorkunitAccess(context, wuid, SecAccess_Full);
                descheduleWorkunit(wuid);
                AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
            }
            else
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
                if(!cw)
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid);

                if ((action == CECLWUActions_Delete) && (cw->getState() == WUStateWait))
                    throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,"Cannot delete a workunit which is in a 'Wait' status.");

                switch(action)
                {
                case CECLWUActions_Pause:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionPause);
                    break;
                }
                case CECLWUActions_PauseNow:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionPauseNow);
                    break;
                }
                case CECLWUActions_Resume:
                {
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    WorkunitUpdate wu(&cw->lock());
                    wu->setAction(WUActionResume);
                    break;
                }
                case CECLWUActions_Delete:
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    {
                        cw.clear();
                        factory->deleteWorkUnitEx(wuid, true),
                        AuditSystemAccess(context.queryUserId(), true, "Deleted %s", wuid);
                    }
                    break;
                case CECLWUActions_Abort:
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                    {
                        if (cw->getState() == WUStateWait)
                        {
                            WorkunitUpdate wu(&cw->lock());
                            wu->deschedule();
                            wu->setState(WUStateAborted);
                        }
                        else
                            abortWorkUnit(wuid, context.querySecManager(), context.queryUser());
                        AuditSystemAccess(context.queryUserId(), true, "Aborted %s", wuid);
                    }
                    break;
                case CECLWUActions_Protect:
                case CECLWUActions_Unprotect:
                    ensureWsWorkunitAccess(context, *cw, SecAccess_Write);
                    cw->protect((action == CECLWUActions_Protect) ? true:false);
                    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    break;
                case CECLWUActions_SetToFailed:
                    {
                        ensureWsWorkunitAccess(context, *cw, SecAccess_Write);
                        WorkunitUpdate wu(&cw->lock());
                        wu->setState(WUStateFailed);
                        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    }
                    break;
                case CECLWUActions_EventReschedule:
                    {
                        ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
                        WorkunitUpdate wu(&cw->lock());
                        wu->schedule();
                        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid);
                    }
                    break;
                }
            }
            PROGLOG("%s %s done", strAction, wuid);
            setActionResult(wuid, action, "Success", strAction, results);
        }
        catch (IException *e)
        {
            bAllSuccess = false;
            StringBuffer eMsg;
            StringBuffer failedMsg("Failed: ");
            setActionResult(wuid, action, failedMsg.append(e->errorMessage(eMsg)).str(), strAction, results);
            OWARNLOG("Failed to %s for workunit: %s, %s", strAction, wuid, eMsg.str());
            AuditSystemAccess(context.queryUserId(), false, "Failed to %s %s", strAction, wuid);
            e->Release();
            continue;
        }
        catch (...)
        {
            bAllSuccess = false;
            StringBuffer failedMsg;
            failedMsg.appendf("Unknown exception");
            setActionResult(wuid, action, failedMsg.str(), strAction, results);
            IWARNLOG("Failed to %s for workunit: %s, %s", strAction, wuid, failedMsg.str());
            AuditSystemAccess(context.queryUserId(), false, "Failed to %s %s", strAction, wuid);
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


bool doProtectWorkunits(IEspContext& context, StringArray& wuids, IArrayOf<IConstWUActionResult>* results)
{
    Owned<IProperties> params(createProperties(true));
    params->setProp("BlockTillFinishTimer", 0);

    return doAction(context, wuids, CECLWUActions_Protect, params, results);
}
bool doUnProtectWorkunits(IEspContext& context, StringArray& wuids, IArrayOf<IConstWUActionResult>* results)
{
    Owned<IProperties> params(createProperties(true));
    params->setProp("BlockTillFinishTimer", 0);

    return doAction(context, wuids, CECLWUActions_Unprotect, params, results);
}

static void checkUpdateQuerysetLibraries()
{
    Owned<IRemoteConnection> globalLock = querySDS().connect("/QuerySets/", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, WU_SDS_LOCK_TIMEOUT);
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

void CWsWorkunitsEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if (!daliClientActive())
    {
        OERRLOG("No Dali Connection Active.");
        throw MakeStringException(-1, "No Dali Connection Active. Please Specify a Dali to connect to in you configuration file");
    }

    DBGLOG("Initializing %s service [process = %s]", service, process);

    checkUpdateQuerysetLibraries();

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

    awusCacheMinutes = AWUS_CACHE_MIN_DEFAULT;
    VStringBuffer xpath("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/AWUsCacheMinutes", process, service);
    cfg->getPropInt(xpath.str(), awusCacheMinutes);

    xpath.setf("Software/EspProcess[@name=\"%s\"]/@PageCacheTimeoutSeconds", process);
    if (cfg->hasProp(xpath.str()))
        setPageCacheTimeoutMilliSeconds(cfg->getPropInt(xpath.str()));
    xpath.setf("Software/EspProcess[@name=\"%s\"]/@MaxPageCacheItems", process);
    if (cfg->hasProp(xpath.str()))
        setMaxPageCacheItems(cfg->getPropInt(xpath.str()));

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/serverForArchivedECLWU/@netAddress", process, service);
    if (cfg->hasProp(xpath.str()))
    {
        sashaServerIp.set(cfg->queryProp(xpath.str()));
        xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/serverForArchivedECLWU/@port", process, service);
        sashaServerPort = cfg->getPropInt(xpath.str(), DEFAULT_SASHA_PORT);
    }

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/ThorSlaveLogThreadPoolSize", process, service);
    thorSlaveLogThreadPoolSize = cfg->getPropInt(xpath, THOR_SLAVE_LOG_THREAD_POOL_SIZE);

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/WUResultMaxSizeMB", process, service);
    unsigned wuResultMaxSizeMB = cfg->getPropInt(xpath);
    if (wuResultMaxSizeMB > 0)
        wuResultMaxSize = wuResultMaxSizeMB * 0x100000;

    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/ZAPEmail", process, service);
    IPropertyTree *zapEmail = cfg->queryPropTree(xpath.str());
    if (zapEmail)
    {
        zapEmailTo = zapEmail->queryProp("@to");
        if (zapEmailTo.isEmpty())
            throw MakeStringException(-1, "ZAPEmail: EmailTo not specified.");
        zapEmailFrom = zapEmail->queryProp("@from");
        if (zapEmailFrom.isEmpty())
            throw MakeStringException(-1, "ZAPEmail: EmailFrom not specified.");
        zapEmailServer = zapEmail->queryProp("@serverURL");
        if (zapEmailServer.isEmpty())
            throw MakeStringException(-1, "ZAPEmail: EmailServer not specified.");

        zapEmailServerPort = zapEmail->getPropInt("@serverPort", WUDEFAULT_ZAPEMAILSERVER_PORT);
        zapEmailMaxAttachmentSize = zapEmail->getPropInt("@maxAttachmentSize", MAX_ZAP_BUFFER_SIZE);
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

    getConfigurationDirectory(directories, "data", "esp", process, dataDirectory);
    wuFactory.setown(getWorkUnitFactory());

#ifdef _CONTAINERIZED
    initContainerRoxieTargets(roxieConnMap);
#endif
    m_sched.start();
    filesInUse.subscribe();

    //Start thread pool
    xpath.setf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]/ClusterQueryStateThreadPoolSize", process, service);
    Owned<CClusterQueryStateThreadFactory> threadFactory = new CClusterQueryStateThreadFactory();
    clusterQueryStatePool.setown(createThreadPool("CheckAndSetClusterQueryState Thread Pool", threadFactory, NULL,
            cfg->getPropInt(xpath.str(), CHECK_QUERY_STATUS_THREAD_POOL_SIZE)));
}

bool CWsWorkunitsEx::onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp)
{
    try
    {
        ensureWsCreateWorkunitAccess(context);

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

bool origValueChanged(const char *newValue, const char *origValue, StringBuffer &s, bool nillable)
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUUpdate", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Write);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        PROGLOG("WUUpdate: %s", wuid.str());
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
                validateTargetName(s);
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
            IConstDebugValue& item=req.getDebugValues().item(di);
            const char *debugName = item.getName();
            if (notEmpty(debugName))
            {
                StringBuffer expanded;
                if (*debugName=='-')
                    debugName=expanded.append("eclcc").append(debugName).str();
                wu->setDebugValue(debugName, item.getValue(), true);
            }
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
        const char* wuid = req.getWuid();
        if (!wuid || !*wuid)
        {
            ensureWsCreateWorkunitAccess(context);

            NewWsWorkunit wu(context);
            req.setWuid(wu->queryWuid());
        }
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
        CECLWUActions action;
        double version = context.getClientVersion();
        if (version >= 1.57)
            action = req.getWUActionType();
        else
            action = eclWUActionType.toEnum(req.getActionType());
        if (action == ECLWUActions_Undefined)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Action not defined.");

        Owned<IProperties> params = createProperties(true);
        params->setProp("BlockTillFinishTimer", req.getBlockTillFinishTimer());
        if (((action == CECLWUActions_Restore) || (action == CECLWUActions_Archive)) && !sashaServerIp.isEmpty())
        {
            params->setProp("sashaServerIP", sashaServerIp.get());
            params->setProp("sashaServerPort", sashaServerPort);
        }

        IArrayOf<IConstWUActionResult> results;
        if (doAction(context, req.getWuids(), action, params, &results) && (action != CECLWUActions_Delete) && checkRedirect(context))
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

        if (!doAction(context,req.getWuids(), CECLWUActions_Delete, params, &results))
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
        if (!doAction(context,req.getWuids(), CECLWUActions_Abort, params, &results))
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
        params->setProp("BlockTillFinishTimer", 0);

        CECLWUActions action = req.getProtect() ? CECLWUActions_Protect : CECLWUActions_Unprotect;
        if (!doAction(context,req.getWuids(), action, params, &results))
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
            StringBuffer requestWuid(req.getWuids().item(i));
            WsWuHelpers::checkAndTrimWorkunit("WUResubmit", requestWuid);

            ensureWsWorkunitAccess(context, requestWuid.str(), SecAccess_Write);

            PROGLOG("WUResubmit: %s", requestWuid.str());
            wuid.set(requestWuid.str());

            try
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                if(req.getCloneWorkunit() || req.getRecompile())
                {
                    Owned<IConstWorkUnit> src(factory->openWorkUnit(wuid.str()));
                    NewWsWorkunit wu(factory, context);
                    wuid.set(wu->queryWuid());
                    queryExtendedWU(wu)->copyWorkUnit(src, false, false);
                }

                wuids.append(wuid.str());

                Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str()));
                if(!cw)
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

                WsWuHelpers::submitWsWorkunit(context, cw, nullptr, nullptr, 0, 0, req.getRecompile(), req.getResetWorkflow(), false, nullptr, nullptr, nullptr, nullptr);

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
            PROGLOG("WUPushEvent: EventName %s, EventText %s", name, text);
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUSchedule", wuid);

        const char* cluster = req.getCluster();
        validateTargetName(cluster);

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

        PROGLOG("WUSchedule: %s", wuid.str());
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUSubmit", wuid);

        const char *cluster = req.getCluster();
        validateTargetName(cluster);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        ensureWsWorkunitAccess(context, *cw, SecAccess_Full);
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
            WsWuHelpers::submitWsWorkunit(context, cw, cluster, req.getSnapshot(), req.getMaxRunTime(), req.getMaxCost(), true, false, false, nullptr, nullptr, nullptr, nullptr);

        PROGLOG("WUSubmit: %s", wuid.str());
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
        if (!isEmptyString(cluster))
            validateTargetName(cluster);

        StringBuffer wuidStr(req.getWuid());
        const char* runWuid = wuidStr.trim().str();
        StringBuffer wuid;

        ErrorSeverity severity = checkGetExceptionSeverity(req.getExceptionSeverity());

        if (runWuid && *runWuid)
        {
            if (!looksLikeAWuid(runWuid, 'W'))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", runWuid);

            ensureWsWorkunitAccess(context, runWuid, SecAccess_Full);
            PROGLOG("WURun: %s", runWuid);
            if (req.getCloneWorkunit())
                WsWuHelpers::runWsWorkunit(context, wuid, runWuid, cluster, req.getInput(), &req.getVariables(),
                    &req.getDebugValues(), &req.getApplicationValues());
            else
            {
                WsWuHelpers::submitWsWorkunit(context, runWuid, cluster, NULL, 0, 0, false, true, true, req.getInput(),
                    &req.getVariables(), &req.getDebugValues(), &req.getApplicationValues());
                wuid.set(runWuid);
            }
        }
        else if (notEmpty(req.getQuerySet()) && notEmpty(req.getQuery()))
        {
            PROGLOG("WURun: QuerySet %s, Query %s", req.getQuerySet(), req.getQuery());
            WsWuHelpers::runWsWuQuery(context, wuid, req.getQuerySet(), req.getQuery(), cluster, req.getInput(),
                &req.getApplicationValues());
        }
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

bool CWsWorkunitsEx::onWUFullResult(IEspContext &context, IEspWUFullResultRequest &req, IEspWUFullResultResponse &resp)
{
    try
    {
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUFullResult", wuid);

        ErrorSeverity severity = checkGetExceptionSeverity(req.getExceptionSeverity());

        if (!wuid.length())
            throw MakeStringException(ECLWATCH_MISSING_PARAMS,"Workunit or Query required");
        if (!looksLikeAWuid(wuid, 'W'))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid.str());
        PROGLOG("WUFullResults: %s", wuid.str());

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.", wuid.str());

        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

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
                if (context.getResponseFormat()==ESPSerializationJSON)
                    getFullWorkUnitResultsJSON(context.queryUserId(), context.queryPassword(), cw.get(), result, flags, severity);
                else
                    getFullWorkUnitResultsXML(context.queryUserId(), context.queryPassword(), cw.get(), result, flags, severity);
                resp.setResults(result.str());
                break;
            }
            default:
                throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT, "Cannot get results Workunit %s %s.", wuid.str(), getWorkunitStateStr(cw->getState()));
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUWaitCompiled", wuid);
        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Full);
        PROGLOG("WUWaitCompiled: %s", wuid.str());

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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUWaitComplete", wuid);
        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Full);
        PROGLOG("WUWaitComplete: %s", wuid.str());
        std::list<WUState> expectedStates;
        if (req.getReturnOnWait())
            expectedStates.push_back(WUStateWait);
        resp.setStateID(secWaitForWorkUnitToComplete(wuid.str(), *context.querySecManager(), *context.queryUser(), req.getWait(), expectedStates));
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUCDebug", wuid);
        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Full);
        PROGLOG("WUCDebug: %s", wuid.str());
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
        ensureWsCreateWorkunitAccess(context);

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
            const char *debugName = item.getName();
            if (notEmpty(debugName))
            {
                StringBuffer expanded;
                if (*debugName=='-')
                    debugName=expanded.append("eclcc").append(debugName).str();
                wu->setDebugValue(debugName, item.getValue(), true);
            }
        }

        wu.setQueryText(req.getECL());

        StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit,clear()
        wu->commit();
        wu.clear();

        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, 0, true, false, false, nullptr, nullptr, nullptr, nullptr);
        waitForWorkUnitToComplete(wuid.str(), req.getTimeToWait());

        Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str()));
        WsWUExceptions errors(*cw);
        resp.setErrors(errors);

        StringBuffer msg;
        WUState st = cw->getState();
        cw.clear();

        switch (st)
        {
        case WUStateAborted:
        case WUStateCompleted:
        case WUStateFailed:
            factory->deleteWorkUnitEx(wuid.str(), true);
            break;
        default:
            abortWorkUnit(wuid.str(), context.querySecManager(), context.queryUser());
            if (!factory->deleteWorkUnit(wuid.str()))
            {
                throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT,
                    "WUSyntaxCheckECL has timed out. Workunit %s cannot be deleted now. You may delete it when its status changes.", wuid.str());
            }
            if (context.getClientVersion() < 1.57)
                throw MakeStringException(ECLWATCH_CANNOT_DELETE_WORKUNIT, "WUSyntaxCheckECL has timed out.");
            resp.setMessage("WUSyntaxCheckECL has timed out.");
            break;
        }
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

        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, 0, true, false, false, nullptr, nullptr, nullptr, nullptr);
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
        factory->deleteWorkUnitEx(wuid.str(), true);
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
        ensureWsCreateWorkunitAccess(context);

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

        WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, 0, true, false, false, nullptr, nullptr, nullptr, nullptr);

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

        factory->deleteWorkUnitEx(wuid.str(), true);
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
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond from: Get information for %s.",
            ep.getUrlStr(url).str(), wuid);
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
    PROGLOG("GetArchivedWUInfo: %s", wuid);

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
        getSashaServiceEP(ep, "sasha-wu-archiver", true);

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
        StringBuffer wuid(req.getWuid());
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

                unsigned long flags=0;
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
                if (req.getIncludeECL())
                    flags|=WUINFO_IncludeECL;
                if (req.getIncludeHelpers())
                    flags|=WUINFO_IncludeHelpers;
                if (req.getIncludeAllowedClusters())
                    flags|=WUINFO_IncludeAllowedClusters;
                if (req.getIncludeTotalClusterTime())
                    flags|=WUINFO_IncludeTotalClusterTime;
                if (req.getIncludeServiceNames())
                    flags|=WUINFO_IncludeServiceNames;

                PROGLOG("WUInfo: %s %lx", wuid.str(), flags);

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
    StringBuffer wuid(req.getWuid());
    WsWuHelpers::checkAndTrimWorkunit("WUResultView", wuid);

    ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);
    PROGLOG("WUResultView: %s", wuid.str());

    Owned<IWuWebView> wv = createWuWebView(wuid.str(), NULL, NULL, getCFD(), true, nullptr);
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
    PROGLOG("getWUInfo: %s", wuid);
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
    if ((value & WUSFwild) != 0 && !containsWildcard(name))
    {
        VStringBuffer s("*%s*", name);
        buff.append(s);
    }
    else
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

bool addWUQueryFilterTotalClusterTime(WUSortField *filters, unsigned short &count, MemoryBuffer &filterBuf, unsigned milliseconds, WUSortField value)
{
    if (milliseconds == 0)
        return false;

    VStringBuffer vBuf("%u", milliseconds);
    filters[count++] = value;
    filterBuf.append(vBuf);
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

void readWUQuerySortOrder(const char* sortBy, const bool descending, WUSortField& sortOrder)
{
    if (isEmpty(sortBy))
    {
        sortOrder = (WUSortField) (WUSFwuid | WUSFreverse);
        return;
    }

    if (strieq(sortBy, "Owner"))
        sortOrder = WUSFuser;
    else if (strieq(sortBy, "JobName"))
        sortOrder = WUSFjob;
    else if (strieq(sortBy, "Cluster"))
        sortOrder = WUSFcluster;
    else if (strieq(sortBy, "Protected"))
        sortOrder = WUSFprotected;
    else if (strieq(sortBy, "State"))
        sortOrder = WUSFstate;
    else if (strieq(sortBy, "ClusterTime"))
        sortOrder = (WUSortField) (WUSFtotalthortime+WUSFnumeric);
    else
        sortOrder = WUSFwuid;

    sortOrder = (WUSortField) (sortOrder | WUSFnocase);
    if (descending)
        sortOrder = (WUSortField) (sortOrder | WUSFreverse);
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

    WUSortField sortorder;
    readWUQuerySortOrder(req.getSortby(), req.getDescending(), sortorder);

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

    addWUQueryFilterTotalClusterTime(filters, filterCount, filterbuf, req.getTotalClusterTimeThresholdMilliSec(), WUSFtotalthortime);

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
    PROGLOG("WUQuery: getWorkUnitsSorted");
    Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), begin, pagesize+1, &cacheHint, &numWUs); // MORE - need security flags here!
    if (version >= 1.41)
        resp.setCacheHint(cacheHint);
    PROGLOG("WUQuery: getWorkUnitsSorted done");

    unsigned actualCount = 0;
    ForEach(*it)
    {
        IConstWorkUnitInfo& cw = it->query();
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
        if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cw, accessOwn, accessOthers) < SecAccess_Read)
        {
            info->setState("<Hidden>");
            results.append(*info.getClear());
            continue;
        }

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

void doWULightWeightQueryWithSort(IEspContext &context, IEspWULightWeightQueryRequest & req, IEspWULightWeightQueryResponse & resp)
{
    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

    double version = context.getClientVersion();

    int pageStartFrom = 0;
    int pageSize = 100;
    if (!req.getPageStartFrom_isNull())
        pageStartFrom = req.getPageStartFrom();
    if (!req.getPageSize_isNull())
        pageSize = req.getPageSize();

    WUSortField sortOrder;
    readWUQuerySortOrder(req.getSortBy(), req.getDescending(), sortOrder);

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
    addWUQueryFilter(filters, filterCount, filterbuf, req.getOwner(), (WUSortField) (WUSFuser | WUSFnocase));
    addWUQueryFilter(filters, filterCount, filterbuf, req.getJobName(), (WUSortField) (WUSFjob | WUSFnocase));

    //StartDate example: 2015-08-26T14:26:00
    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getStartDate(), WUSFwuid);
    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getEndDate(), WUSFwuidhigh);
    IArrayOf<IConstApplicationValue>& applicationFilters = req.getApplicationValues();
    ForEachItemIn(i, applicationFilters)
    {
        IConstApplicationValue &item = applicationFilters.item(i);
        addWUQueryFilterApplication(filters, filterCount, filterbuf, item.getApplication(), item.getName(), item.getValue());
    }

    filters[filterCount] = WUSFterm;

    __int64 cacheHint = 0;
    if (!req.getCacheHint_isNull())
        cacheHint = req.getCacheHint();

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    unsigned numWUs;
    PROGLOG("getWorkUnitsSorted(LightWeight)");
    Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortOrder, filters, filterbuf.bufferBase(), pageStartFrom, pageSize+1, &cacheHint, &numWUs); // MORE - need security flags here!
    resp.setCacheHint(cacheHint);
    PROGLOG("getWorkUnitsSorted(LightWeight) done");

    IArrayOf<IEspECLWorkunitLW> results;
    ForEach(*it)
    {
        IConstWorkUnitInfo& cw = it->query();
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

        Owned<IEspECLWorkunitLW> info = createECLWorkunitLW("","");
        info->setWuid(cw.queryWuid());
        if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cw, accessOwn, accessOthers) < SecAccess_Read)
        {
            info->setStateDesc("<Hidden>");
            results.append(*info.getClear());
            continue;
        }

        SCMStringBuffer s;
        info->setIsProtected(cw.isProtected() ? 1 : 0);
        info->setJobName(cw.queryJobName());
        info->setWuScope(cw.queryWuScope());
        info->setOwner(cw.queryUser());
        info->setClusterName(cw.queryClusterName());
        info->setState(cw.getState());
        info->setStateDesc(cw.queryStateDesc());
        info->setAction(cw.getAction());
        info->setActionDesc(cw.queryActionDesc());
        info->setPriority(cw.getPriority());
        info->setPriorityLevel(cw.getPriorityLevel());
        info->setPriorityDesc(cw.queryPriorityDesc());
        info->setTotalClusterTime(cw.getTotalThorTime());

        WsWuDateTime dt;
        cw.getTimeScheduled(dt);
        if(dt.isValid())
            info->setDateTimeScheduled(dt.getString(s).str());

        IArrayOf<IEspApplicationValue> av;
        Owned<IConstWUAppValueIterator> app(&cw.getApplicationValues());
        ForEach(*app)
        {
            IConstWUAppValue& val=app->query();
            Owned<IEspApplicationValue> t= createApplicationValue("","");
            t->setApplication(val.queryApplication());
            t->setName(val.queryName());
            t->setValue(val.queryValue());
            av.append(*t.getClear());

        }
        info->setApplicationValues(av);
        results.append(*info.getClear());
    }

    resp.setNumWUs(numWUs);
    if (results.length() > (aindex_t)pageSize)
        results.pop();

    resp.setWorkunits(results);
    return;
}

class CArchivedWUsReader : public CInterface, implements IArchivedWUsReader
{
    IEspContext& context;
    unsigned pageSize;
    StringAttr sashaServerIP;
    unsigned sashaServerPort;
    unsigned cacheMinutes;
    StringBuffer filterStr;
    ArchivedWuCache& archivedWuCache;
    unsigned numberOfWUsReturned;
    bool hasMoreWU;

    void readDateFilters(const char* startDateReq, const char* endDateReq, StringBuffer& from, StringBuffer& to)
    {
        CDateTime timeFrom, timeTo;
        if(notEmpty(endDateReq))
        {//endDateReq example: 2015-08-26T14:26:00
            unsigned year, month, day, hour, minute, second, nano;
            timeTo.setString(endDateReq, NULL, true);
            timeTo.getDate(year, month, day, true);
            timeTo.getTime(hour, minute, second, nano, true);
            to.setf("%4d%02d%02d-%02d%02d%02d", year, month, day, hour, minute, second);
        }

        if(isEmpty(startDateReq))
            return;

        timeFrom.setString(startDateReq, NULL, true);
        if (timeFrom >= timeTo)
            return;

        unsigned year0, month0, day0, hour0, minute0, second0, nano0;
        timeFrom.getDate(year0, month0, day0, true);
        timeFrom.getTime(hour0, minute0, second0, nano0, true);
        from.setf("%4d%02d%02d-%02d%02d%02d", year0, month0, day0, hour0, minute0, second0);
        return;
    }

    bool addToFilterString(const char *name, const char *value)
    {
        if (isEmpty(name) || isEmpty(value))
            return false;
        filterStr.append(';').append(name).append("=").append(value);
        return true;
    }

    bool addToFilterString(const char *name, unsigned value)
    {
        if (isEmpty(name))
            return false;
        filterStr.append(';').append(name).append("=").append(value);
        return true;
    }

    void setFilterString(IEspWUQueryRequest& req)
    {
        filterStr.set("0");
        addToFilterString("wuid", req.getWuid());
        addToFilterString("cluster", req.getCluster());
        addToFilterString("owner", req.getOwner());
        addToFilterString("jobName", req.getJobname());
        addToFilterString("state", req.getState());
        addToFilterString("timeFrom", req.getStartDate());
        addToFilterString("timeTo", req.getEndDate());
        addToFilterString("beforeWU", req.getBeforeWU());
        addToFilterString("afterWU", req.getAfterWU());
        addToFilterString("descending", req.getDescending());
        addToFilterString("pageSize", pageSize);
        if (sashaServerIP && *sashaServerIP)
        {
            addToFilterString("sashaServerIP", sashaServerIP.get());
            addToFilterString("sashaServerPort", sashaServerPort);
        }
    }

    void setFilterStringLW(IEspWULightWeightQueryRequest& req)
    {
        filterStr.set("1");
        addToFilterString("wuid", req.getWuid());
        addToFilterString("cluster", req.getCluster());
        addToFilterString("owner", req.getOwner());
        addToFilterString("jobName", req.getJobName());
        addToFilterString("state", req.getState());
        addToFilterString("timeFrom", req.getStartDate());
        addToFilterString("timeTo", req.getEndDate());
        addToFilterString("beforeWU", req.getBeforeWU());
        addToFilterString("afterWU", req.getAfterWU());
        addToFilterString("descending", req.getDescending());
        addToFilterString("pageSize", pageSize);
        if (sashaServerIP && *sashaServerIP)
        {
            addToFilterString("sashaServerIP", sashaServerIP.get());
            addToFilterString("sashaServerPort", sashaServerPort);
        }
    }

    void initSashaCommand(ISashaCommand* cmd)
    {
        cmd->setAction(SCA_LIST);
        cmd->setOutputFormat("owner,jobname,cluster,state");
        cmd->setOnline(false);
        cmd->setArchived(true);
        cmd->setLimit(pageSize+1); //read an extra WU to check hasMoreWU
    }

    void setSashaCommand(IEspWUQueryRequest& req, ISashaCommand* cmd)
    {
        if (notEmpty(req.getWuid()))
            cmd->addId(req.getWuid());
        if (notEmpty(req.getCluster()))
            cmd->setCluster(req.getCluster());
        if (notEmpty(req.getOwner()))
            cmd->setOwner(req.getOwner());
        if (notEmpty(req.getJobname()))
            cmd->setJobName(req.getJobname());
        if (notEmpty(req.getState()))
            cmd->setState(req.getState());

        StringBuffer timeFrom, timeTo;
        readDateFilters(req.getStartDate(), req.getEndDate(), timeFrom, timeTo);
        if (timeFrom.length())
            cmd->setAfter(timeFrom.str());
        if (timeTo.length())
            cmd->setBefore(timeTo.str());
        if (notEmpty(req.getBeforeWU()))
            cmd->setBeforeWU(req.getBeforeWU());
        if (notEmpty(req.getAfterWU()))
            cmd->setAfterWU(req.getAfterWU());
        cmd->setSortDescending(req.getDescending());
        return;
    }

    void setSashaCommandLW(IEspWULightWeightQueryRequest& req, ISashaCommand* cmd)
    {
        if (notEmpty(req.getWuid()))
            cmd->addId(req.getWuid());
        if (notEmpty(req.getCluster()))
            cmd->setCluster(req.getCluster());
        if (notEmpty(req.getOwner()))
            cmd->setOwner(req.getOwner());
        if (notEmpty(req.getJobName()))
            cmd->setJobName(req.getJobName());
        if (notEmpty(req.getState()))
            cmd->setState(req.getState());

        StringBuffer timeFrom, timeTo;
        readDateFilters(req.getStartDate(), req.getEndDate(), timeFrom, timeTo);
        if (timeFrom.length())
            cmd->setAfter(timeFrom.str());
        if (timeTo.length())
            cmd->setBefore(timeTo.str());
        if (notEmpty(req.getBeforeWU()))
            cmd->setBeforeWU(req.getBeforeWU());
        if (notEmpty(req.getAfterWU()))
            cmd->setAfterWU(req.getAfterWU());
        cmd->setSortDescending(req.getDescending());

        return;
    }

    IEspECLWorkunit *createArchivedWUEntry(StringArray& wuDataArray, bool canAccess)
    {
        Owned<IEspECLWorkunit> info= createECLWorkunit();
        info->setWuid(wuDataArray.item(0));
        if (!canAccess)
        {
            info->setState("<Hidden>");
            return info.getClear();
        }

        const char* owner = wuDataArray.item(1);
        const char* jobName = wuDataArray.item(2);
        const char* cluster = wuDataArray.item(3);
        const char* state = wuDataArray.item(4);
        if (notEmpty(owner))
            info->setOwner(owner);
        if (notEmpty(jobName))
            info->setJobname(jobName);
        if (notEmpty(cluster))
            info->setCluster(cluster);
        if (notEmpty(state))
            info->setState(state);
        return info.getClear();
    }
    IEspECLWorkunitLW *createArchivedLWWUEntry(StringArray& wuDataArray, bool canAccess)
    {
        Owned<IEspECLWorkunitLW> info= createECLWorkunitLW();
        info->setWuid(wuDataArray.item(0));
        if (!canAccess)
        {
            info->setStateDesc("<Hidden>");
            return info.getClear();
        }

        const char* owner = wuDataArray.item(1);
        const char* jobName = wuDataArray.item(2);
        const char* cluster = wuDataArray.item(3);
        const char* state = wuDataArray.item(4);
        if (notEmpty(owner))
            info->setOwner(owner);
        if (notEmpty(jobName))
            info->setJobName(jobName);
        if (notEmpty(cluster))
            info->setClusterName(cluster);
        if (notEmpty(state))
            info->setStateDesc(state);
        return info.getClear();
    }
    static int compareWuids(IInterface * const *_a, IInterface * const *_b)
    {
        IEspECLWorkunit *a = *(IEspECLWorkunit **)_a;
        IEspECLWorkunit *b = *(IEspECLWorkunit **)_b;
        return strcmp(b->getWuid(), a->getWuid());
    }
    static int compareLWWuids(IInterface * const *_a, IInterface * const *_b)
    {
        IEspECLWorkunitLW *a = *(IEspECLWorkunitLW **)_a;
        IEspECLWorkunitLW *b = *(IEspECLWorkunitLW **)_b;
        return strcmp(b->getWuid(), a->getWuid());
    }
public:
    IMPLEMENT_IINTERFACE_USING(CInterface);

    CArchivedWUsReader(IEspContext& _context, const char* _sashaServerIP, unsigned _sashaServerPort, ArchivedWuCache& _archivedWuCache,
        unsigned _cacheMinutes, unsigned _pageSize)
        : context(_context), sashaServerIP(_sashaServerIP), sashaServerPort(_sashaServerPort),
        archivedWuCache(_archivedWuCache), cacheMinutes(_cacheMinutes), pageSize(_pageSize)
    {
        hasMoreWU = false;
        numberOfWUsReturned = 0;
    }

    void getArchivedWUs(bool lightWeight, IEspWUQueryRequest& req, IEspWULightWeightQueryRequest& reqLW, IArrayOf<IEspECLWorkunit>& archivedWUs, IArrayOf<IEspECLWorkunitLW>& archivedLWWUs)
    {
        if (!lightWeight)
            setFilterString(req);
        else
            setFilterStringLW(reqLW);
        Owned<ArchivedWuCacheElement> cachedResults = archivedWuCache.lookup(context, filterStr, "AddWhenAvailable", cacheMinutes);
        if (cachedResults)
        {
            hasMoreWU = cachedResults->m_hasNextPage;
            numberOfWUsReturned = cachedResults->numWUsReturned;
            if (!lightWeight && cachedResults->m_results.length())
            {
                ForEachItemIn(i, cachedResults->m_results)
                    archivedWUs.append(*LINK(&cachedResults->m_results.item(i)));
            }
            if (lightWeight && cachedResults->resultsLW.length())
            {
                ForEachItemIn(i, cachedResults->resultsLW)
                    archivedLWWUs.append(*LINK(&cachedResults->resultsLW.item(i)));
            }
            return;
        }

        SocketEndpoint ep;
        if (sashaServerIP && *sashaServerIP)
            ep.set(sashaServerIP, sashaServerPort);
        else
            getSashaServiceEP(ep, "sasha-wu-archiver", true);

        Owned<INode> sashaserver = createINode(ep);

        Owned<ISashaCommand> cmd = createSashaCommand();
        initSashaCommand(cmd);
        if (!lightWeight)
            setSashaCommand(req, cmd);
        else
            setSashaCommandLW(reqLW, cmd);
        if (!cmd->send(sashaserver))
        {
            StringBuffer url;
            throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
                "Sasha (%s) took too long to respond from: Get archived workUnits.",
                ep.getUrlStr(url).str());
        }

        numberOfWUsReturned = cmd->numIds();
        hasMoreWU = (numberOfWUsReturned > pageSize);
        if (hasMoreWU)
            numberOfWUsReturned--;

        if (numberOfWUsReturned == 0)
            return;

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
                IWARNLOG("Empty WUID in SCA_LIST response"); // JCS->KW - have u ever seen this happen?
                continue;
            }
            const char* owner = wuDataArray.item(1);
            bool canAccess = chooseWuAccessFlagsByOwnership(context.queryUserId(), owner, accessOwn, accessOthers) >= SecAccess_Read;
            if (!lightWeight)
            {
                Owned<IEspECLWorkunit> info = createArchivedWUEntry(wuDataArray, canAccess);
                archivedWUs.append(*info.getClear());
            }
            else
            {
                Owned<IEspECLWorkunitLW> info = createArchivedLWWUEntry(wuDataArray, canAccess);
                archivedLWWUs.append(*info.getClear());
            }
        }

        archivedWuCache.add(filterStr, "AddWhenAvailable", hasMoreWU, numberOfWUsReturned, archivedWUs, archivedLWWUs);
        return;
    };

    bool getHasMoreWU() { return hasMoreWU; };
    unsigned getNumberOfWUsReturned() { return numberOfWUsReturned; };
};

void doWUQueryFromArchive(IEspContext &context, const char* sashaServerIP, unsigned sashaServerPort,
       ArchivedWuCache &archivedWuCache, unsigned cacheMinutes, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    //Sasha server does noy support the PageStartFrom due to inefficient access to archived workunits for pages>1.
    unsigned pageSize = (unsigned) req.getPageSize();
    if(pageSize < 1)
        pageSize=500;
    Owned<IArchivedWUsReader> archiveWUsReader = new CArchivedWUsReader(context, sashaServerIP, sashaServerPort, archivedWuCache,
        cacheMinutes, pageSize);

    IArrayOf<IEspECLWorkunit> archivedWUs;
    IArrayOf<IEspECLWorkunitLW> dummyWUs;
    Owned<CWULightWeightQueryRequest> dummyReq = new CWULightWeightQueryRequest("WsWorkunits");
    PROGLOG("getWorkUnitsFromArchive");
    archiveWUsReader->getArchivedWUs(false, req, *dummyReq, archivedWUs, dummyWUs);
    PROGLOG("getWorkUnitsFromArchive done");

    resp.setWorkunits(archivedWUs);
    resp.setNumWUs(archiveWUsReader->getNumberOfWUsReturned());

    resp.setType("archived only");
    resp.setPageSize(pageSize);
    return;
}

void doWULightWeightQueryFromArchive(IEspContext &context, const char* sashaServerIP, unsigned sashaServerPort,
       ArchivedWuCache &archivedWuCache, unsigned cacheMinutes, IEspWULightWeightQueryRequest & req, IEspWULightWeightQueryResponse & resp)
{
    int pageSize = req.getPageSize_isNull()? 500 : req.getPageSize();
    Owned<IArchivedWUsReader> archiveWUsReader = new CArchivedWUsReader(context, sashaServerIP, sashaServerPort, archivedWuCache,
        cacheMinutes, pageSize);
    Owned<CWUQueryRequest> dummyReq = new CWUQueryRequest("WsWorkunits");
    IArrayOf<IEspECLWorkunit> dummyWUs;
    IArrayOf<IEspECLWorkunitLW> archivedWUs;
    PROGLOG("getWorkUnitsFromArchive(LightWeight)");
    archiveWUsReader->getArchivedWUs(true, *dummyReq, req, dummyWUs, archivedWUs);
    PROGLOG("getWorkUnitsFromArchive(LightWeight) done");

    resp.setWorkunits(archivedWUs);
    resp.setNumWUs(archiveWUsReader->getNumberOfWUsReturned());
    return;
}

bool CWsWorkunitsEx::onWUQuery(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    try
    {
        StringBuffer wuidStr(req.getWuid());
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
        if (version >= 1.26 && version < 1.72 && req.getLastNDays() > -1)
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

bool CWsWorkunitsEx::onWULightWeightQuery(IEspContext &context, IEspWULightWeightQueryRequest & req, IEspWULightWeightQueryResponse & resp)
{
    try
    {
        if (req.getType() && strieq(req.getType(), "archived workunits"))
            doWULightWeightQueryFromArchive(context, sashaServerIp.get(), sashaServerPort, *archivedWuCache, awusCacheMinutes, req, resp);
        else
            doWULightWeightQueryWithSort(context, req, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

ITypeInfo * containsSingleSimpleFieldBlankXPath(IResultSetMetaData * meta)
{
    if (meta->getColumnCount() != 1)
        return NULL;

    CResultSetMetaData * castMeta = static_cast<CResultSetMetaData *>(meta);
    const char * xpath = castMeta->queryXPath(0);
    if (xpath && (*xpath == 0))
    {
        return castMeta->queryType(0);
    }
    return NULL;
}

void csvSplitXPath(const char *xpath, StringBuffer &s, const char *&name, const char **childname=NULL)
{
    if (!xpath)
        return;
    const char * slash = strchr(xpath, '/');
    if (!slash)
    {
        name = xpath;
        if (childname)
            *childname = NULL;
    }
    else
    {
        if (!childname || strchr(slash+1, '/')) //output ignores xpaths that are too deep
            return;
        name = s.clear().append(slash-xpath, xpath).str();
        *childname = slash+1;
    }
}

void getCSVHeaders(const IResultSetMetaData& metaIn, CommonCSVWriter* writer, unsigned& layer)
{
    StringBuffer xname;
    const CResultSetMetaData& cMeta = static_cast<const CResultSetMetaData &>(metaIn);
    IFvDataSourceMetaData* meta = cMeta.getMeta();

    int columnCount = metaIn.getColumnCount();
    for (unsigned idx = 0; idx < columnCount; idx++)
    {
        const CResultSetColumnInfo& column = cMeta.getColumn(idx);
        unsigned flag = column.flag;
        const char * name = meta->queryName(idx);
        const char * childname = NULL;
        switch (flag)
        {
        case FVFFbeginif:
        case FVFFendif:
            break;
        case FVFFbeginrecord:
            csvSplitXPath(meta->queryXPath(idx), xname, name);
            writer->outputBeginNested(name, false, true);
            break;
        case FVFFendrecord:
            csvSplitXPath(meta->queryXPath(idx), xname, name);
            writer->outputEndNested(name, true);
            break;
        case FVFFdataset:
            {
                childname = "Row";
                csvSplitXPath(meta->queryXPath(idx), xname, name, &childname);
                ITypeInfo* singleFieldType = (name && *name && childname && *childname)
                    ? containsSingleSimpleFieldBlankXPath(column.childMeta.get()) : NULL;
                if (!singleFieldType)
                {
                    bool nameValid = (name && *name);
                    if (nameValid || (childname && *childname))
                    {
                        if (nameValid)
                            writer->outputBeginNested(name, false, true);
                        if (childname && *childname)
                            writer->outputBeginNested(childname, false, !nameValid);

                        const CResultSetMetaData *childMeta = static_cast<const CResultSetMetaData *>(column.childMeta.get());
                        getCSVHeaders(*childMeta, writer, ++layer);
                        layer--;

                        if (childname && *childname)
                            writer->outputEndNested(childname, !nameValid);
                        if (nameValid)
                            writer->outputEndNested(name, true);
                    }
                }
                break;
            }
        case FVFFblob: //for now FileViewer will output the string "[blob]"
            {
                Owned<ITypeInfo> stringType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
                csvSplitXPath(meta->queryXPath(idx), xname, name);

                StringBuffer eclTypeName;
                stringType->getECLType(eclTypeName);
                writer->outputCSVHeader(name, eclTypeName.str());
            }
            break;
        default:
            {
                ITypeInfo & type = *column.type;
                if (type.getTypeCode() == type_set)
                {
                    childname = "Item";
                    csvSplitXPath(meta->queryXPath(idx), xname, name, &childname);
                    writer->outputBeginNested(name, true, true);
                    writer->outputEndNested(name, true);
                }
                else
                {
                    csvSplitXPath(meta->queryXPath(idx), xname, name);

                    StringBuffer eclTypeName;
                    type.getECLType(eclTypeName);
                    writer->outputCSVHeader(name, eclTypeName.str());
                }
                break;
            }
        }
    }
}

unsigned getResultCSV(IStringVal& ret, INewResultSet* result, const char* name, __int64 start, unsigned& count)
{
    unsigned headerLayer = 0;
    CSVOptions csvOptions;
    csvOptions.delimiter.set(",");
    csvOptions.terminator.set("\n");
    csvOptions.includeHeader = true;
    Owned<CommonCSVWriter> writer = new CommonCSVWriter(XWFtrim, csvOptions);
    const IResultSetMetaData & meta = result->getMetaData();
    getCSVHeaders(meta, writer, headerLayer);
    writer->finishCSVHeaders();

    Owned<IResultSetCursor> cursor = result->createCursor();
    count = writeResultCursorXml(*writer, cursor, name, start, count, NULL);
    ret.set(writer->str());
    return count;
}

void appendResultSet(MemoryBuffer& mb, INewResultSet* result, const char *name, __int64 start, unsigned& count, __int64& total, bool bin, bool xsd, ESPSerializationFormat fmt, const IProperties *xmlns)
{
    if (!result)
        return;

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

        if (fmt==ESPSerializationCSV)
            count = getResultCSV(adaptor, result, name, (unsigned) start, count);
        else if (fmt==ESPSerializationJSON)
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


static bool isResultRequestSzTooBig(unsigned __int64 start, unsigned requestCount, unsigned __int64 resultSz, unsigned resultRows, unsigned __int64 limitSz)
{
    if ((0 == requestCount) || (0 == resultRows))
        return resultSz > limitSz;
    else
    {
        if (start+requestCount > resultRows)
            requestCount = resultRows-start;
        unsigned __int64 avgRecSize = resultSz / resultRows;
        unsigned __int64 estSize = requestCount * avgRecSize;
        return estSize > limitSz;
    }
}

void CWsWorkunitsEx::getWsWuResult(IEspContext &context, const char *wuid, const char *name, const char *logical, unsigned index, __int64 start,
    unsigned &count, __int64 &total, IStringVal &resname, bool bin, IArrayOf<IConstNamedValue> *filterBy, MemoryBuffer &mb,
    WUState &wuState, bool xsd)
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
    {
        unsigned __int64 resultSz;

        if (0 == logicalName.length()) // could be a workunit owned file (OUTPUT, THOR)
            result->getResultFilename(logicalName);
        if (logicalName.length())
        {
            Owned<IDistributedFile> df = lookupLogicalName(context, logicalName.str(), false, false, false, nullptr, defaultPrivilegedUser);
            if (!df)
                throw makeStringExceptionV(ECLWATCH_FILE_NOT_EXIST, "Cannot find file %s.", logicalName.str());
            resultSz = df->getDiskSize(true, false);
        }
        else
            resultSz = result->getResultRawSize(nullptr, nullptr);

        if (isResultRequestSzTooBig(start, count, resultSz, rs->getNumRows(), wuResultMaxSize))
        {
            throw makeStringExceptionV(ECLWATCH_INVALID_ACTION, "Failed to get the result for %s. The size is bigger than %lld MB.",
                                       wuid, wuResultMaxSize/0x100000);
        }

        appendResultSet(mb, rs, name, start, count, total, bin, xsd, context.getResponseFormat(), result->queryResultXmlns());
    }
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
        StringBuffer wuidStr(req.getWuid());
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
            VStringBuffer logMsg("WUFile: %s", wuid.str());
            if (notEmpty(req.getType()))
                logMsg.append(", ").append(req.getType());
            if (opt > 1)
                logMsg.append(", download gzip");
            else if (opt > 0)
                logMsg.append(", download");
            PROGLOG("%s", logMsg.str());

            resp.setWuid(wuid.get());
            MemoryBuffer mb;
            WsWuInfo winfo(context, wuid);
            if (strieq(File_ArchiveQuery, req.getType()))
            {
                winfo.getWorkunitArchiveQuery(mb);
                openSaveFile(context, opt, req.getSizeLimit(), "ArchiveQuery.xml", HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if ((strieq(File_Cpp,req.getType()) || strieq(File_Log,req.getType())) && notEmpty(req.getName()))
            {
                winfo.getWorkunitCpp(req.getName(), req.getDescription(), req.getIPAddress(),mb, opt > 0, nullptr);
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
                winfo.getWorkunitThorMasterLog(nullptr, req.getName(), mb, nullptr);
                openSaveFile(context, opt, req.getSizeLimit(), "thormaster.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_ThorSlaveLog,req.getType()))
            {
                winfo.getWorkunitThorSlaveLog(directories, req.getProcess(), req.getClusterGroup(), req.getIPAddress(),
                    req.getLogDate(), req.getSlaveNumber(), mb, nullptr, false);
                openSaveFile(context, opt, req.getSizeLimit(), "ThorSlave.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_EclAgentLog,req.getType()))
            {
                winfo.getWorkunitEclAgentLog(nullptr, req.getName(), req.getProcess(), mb, nullptr);
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

                winfo.getWorkunitAssociatedXml(name, req.getIPAddress(), req.getPlainText(), req.getDescription(), opt > 0, true, mb, nullptr);
                openSaveFile(context, opt, req.getSizeLimit(), ptr, HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if (strieq(File_XML,req.getType()) || strieq(File_WUECL,req.getType()))
            {
                StringBuffer mimeType, fileName;
                if (strieq(File_WUECL,req.getType()))
                {
                    fileName.setf("%s.ecl", wuid.get());
                    winfo.getWorkunitQueryShortText(mb, nullptr);
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
            PROGLOG("%s -- done", logMsg.str());
        }
    }
    catch(IException* e)
    {
        CErrorMessageFormat errorMessageFormat = req.getErrorMessageFormat();
        if (errorMessageFormat == CErrorMessageFormat_XML)
            context.setResponseFormat(ESPSerializationXML);
        else if (errorMessageFormat == CErrorMessageFormat_JSON)
            context.setResponseFormat(ESPSerializationJSON);
        else if (errorMessageFormat == CErrorMessageFormat_Text)
            context.setResponseFormat(ESPSerializationTEXT);
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

IPropertyTree *getArchivedWorkUnitProperties(const char *wuid, bool dfuWU)
{
    SocketEndpoint ep;
    getSashaServiceEP(ep, "sasha-wu-archiver", true);
    Owned<INode> node = createINode(ep);
    if (!node)
        throw MakeStringException(ECLWATCH_INODE_NOT_FOUND, "INode not found.");

    StringBuffer tmp;
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->addId(wuid);
    cmd->setAction(SCA_GET);
    cmd->setArchived(true);
    if (dfuWU)
        cmd->setDFU(true);
    if (!cmd->send(node, 1*60*1000))
        throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER,
            "Sasha (%s) took too long to respond from: Get workUnit properties for %s.",
            ep.getUrlStr(tmp).str(), wuid);

    if ((cmd->numIds() < 1) || (cmd->numResults() < 1))
        return nullptr;

    cmd->getResult(0, tmp.clear());
    if(tmp.length() < 1)
        return nullptr;

    Owned<IPropertyTree> wu = createPTreeFromXMLString(tmp.str());
    if (!wu)
        return nullptr;

    return wu.getClear();
}

void getWorkunitCluster(IEspContext &context, const char *wuid, SCMStringBuffer &cluster, bool checkArchiveWUs)
{
    if (isEmpty(wuid))
        return;

    try
    {
        if ('W' == wuid[0])
        {
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
            if (cw)
                cluster.set(cw->queryClusterName());
            else if (checkArchiveWUs)
            {
                Owned<IPropertyTree> wuProps = getArchivedWorkUnitProperties(wuid, false);
                if (wuProps)
                    cluster.set(wuProps->queryProp("@clusterName"));
            }
        }
        else
        {
            Owned<IDFUWorkUnitFactory> factory = getDFUWorkUnitFactory();
            Owned<IConstDFUWorkUnit> cw = factory->openWorkUnit(wuid, false);
            if(cw)
            {
                StringBuffer tmp;
                if (cw->getClusterName(tmp).length()!=0)
                    cluster.set(tmp.str());
            }
            else if (checkArchiveWUs)
            {
                Owned<IPropertyTree> wuProps = getArchivedWorkUnitProperties(wuid, true);
                if (wuProps)
                    cluster.set(wuProps->queryProp("@clusterName"));
            }
        }
    }
    catch (IException * e)
    {
        //Catch exception if the sasha server has not be configured
        DBGLOG(e, "GetWorkunitCluster");
        e->Release();
    }
}

void CWsWorkunitsEx::getFileResults(IEspContext &context, const char *logicalName, const char *cluster, __int64 start, unsigned &count, __int64 &total,
    IStringVal &resname, bool bin, IArrayOf<IConstNamedValue> *filterBy, MemoryBuffer &buf, bool xsd)
{
    Owned<IDistributedFile> df = lookupLogicalName(context, logicalName, false, false, false, nullptr, defaultPrivilegedUser);
    if (!df)
        throw makeStringExceptionV(ECLWATCH_FILE_NOT_EXIST, "Cannot find file %s.", logicalName);

    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
    Owned<INewResultSet> result(resultSetFactory->createNewFileResultSet(df, cluster));
    if (!filterBy || !filterBy->length())
    {
        if (isResultRequestSzTooBig(start, count, df->getDiskSize(true, false), result->getNumRows(), wuResultMaxSize))
        {
            throw makeStringExceptionV(ECLWATCH_INVALID_ACTION, "Failed to get the result from file %s. The size is bigger than %lld MB.",
                                       logicalName, wuResultMaxSize/0x100000);
        }
    
        appendResultSet(buf, result, resname.str(), start, count, total, bin, xsd, context.getResponseFormat(), NULL);
    }
    else
    {
        // NB: this could be still be very big, appendResultSet should be changed to ensure filtered result doesn't grow bigger than wuResultMaxSize
        Owned<INewResultSet> filteredResult = createFilteredResultSet(result, filterBy);
        appendResultSet(buf, filteredResult, resname.str(), start, count, total, bin, xsd, context.getResponseFormat(), NULL);
    }
}

bool CWsWorkunitsEx::onWUResultBin(IEspContext &context,IEspWUResultBinRequest &req, IEspWUResultBinResponse &resp)
{
    try
    {
        StringBuffer wuidStr(req.getWuid());
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

        if(strieq(req.getFormat(),"csv"))
            context.setResponseFormat(ESPSerializationCSV);

        WUState wuState = WUStateUnknown;
        bool bin = (req.getFormat() && strieq(req.getFormat(),"raw"));
        if (notEmpty(wuidIn) && notEmpty(req.getResultName()))
        {
            PROGLOG("WUResultBin: wuid %s, ResultName %s", wuidIn, req.getResultName());
            getWsWuResult(context, wuidIn, req.getResultName(), NULL, 0, start, count, total, name, bin, filterBy, mb, wuState);
        }
        else if (notEmpty(wuidIn) && (req.getSequence() >= 0))
        {
            PROGLOG("WUResultBin: wuid %s, Sequence %d", wuidIn, req.getSequence());
            getWsWuResult(context, wuidIn, NULL, NULL, req.getSequence(), start, count, total, name, bin,filterBy, mb, wuState);
        }
        else if (notEmpty(req.getLogicalName()))
        {
            const char* logicalName = req.getLogicalName();
            const char* clusterIn = req.getCluster();
            if (!isEmptyString(clusterIn))
                getFileResults(context, logicalName, clusterIn, start, count, total, name, false, filterBy, mb, true);
            else
            {
                StringBuffer wuid;
                getWuidFromLogicalFileName(context, logicalName, wuid);
                if (!wuid.length())
                    throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.",logicalName);
                SCMStringBuffer cluster;
                getWorkunitCluster(context, wuid.str(), cluster, true);
                if (cluster.length() > 0)
                    getFileResults(context, logicalName, cluster.str(), start, count, total, name, false, filterBy, mb, true);
                else
                    getWsWuResult(context, wuid.str(), NULL, logicalName, 0, start, count, total, name, bin, filterBy, mb, wuState);
            }
        }
        else
            throw MakeStringException(ECLWATCH_CANNOT_GET_WU_RESULT,"Cannot open the workunit result.");

        if(strieq(req.getFormat(),"csv"))
        {
            resp.setResult(mb);
            resp.setResult_mimetype("text/csv");
            context.addCustomerHeader("Content-disposition", "attachment;filename=WUResult.csv");
        }
        else if(stricmp(req.getFormat(),"xls")==0)
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUResultSummary", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);
        PROGLOG("WUResultSummary: %s", wuid.str());

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

bool CWsWorkunitsEx::onWUResult(IEspContext &context, IEspWUResultRequest &req, IEspWUResultResponse &resp)
{
    try
    {
        StringBuffer wuidStr(req.getWuid());
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

        Owned<DataCacheElement> data;
        if (!req.getBypassCachedResult())
            data.setown(dataCache->lookup(context, filter, awusCacheMinutes));
        if (data)
        {
            PROGLOG("Retrieving Cached WUResult: %s", filter.str());
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
            PROGLOG("Retrieving WUResult: %s", filter.str());
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

void getScheduledWUs(IEspContext &context, WUShowScheduledFilters *filters, const char *serverName, IArrayOf<IEspScheduledWU> & results)
{
    double version = context.getClientVersion();
    if (notEmpty(serverName))
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IScheduleReader> reader;
        try
        {
            reader.setown(getScheduleReader(serverName, filters->eventName));
        }
        catch (IException *e)
        {
            StringBuffer eMsg;
            e->errorMessage(eMsg);
            e->Release();
            IWARNLOG("Failed to getScheduleReader for %s: %s", serverName, eMsg.str());
            return;
        }

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
                        bool match = true;
                        unsigned stateID = WUStateUnknown;
                        StringBuffer jobName, owner;
                        SCMStringBuffer state;
                        try
                        {
                            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
                            if (cw)
                            {
                                jobName.set(cw->queryJobName());
                                owner.set(cw->queryUser());
                            }

                            if (!filters->jobName.isEmpty() && (jobName.isEmpty() || !WildMatch(jobName.str(), filters->jobName, true)))
                                match =  false;
                            else if (!filters->owner.isEmpty() && (owner.isEmpty() || !WildMatch(owner, filters->owner, true)))
                                match =  false;
                            else if (!filters->eventText.isEmpty() && (ieventText.isEmpty() || !WildMatch(ieventText, filters->eventText, true)))
                                match =  false;
                            else if (!filters->state.isEmpty())
                            {
                                if (!cw)
                                    match =  false;
                                else
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
                                    if (!strieq(filters->state, state.str()))
                                        match =  false;
                                }
                            }
                        }
                        catch (IException *e)
                        {
                            EXCLOG(e, "Get scheduled WUs");
                            e->Release();
                            match =  false;
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
        WUShowScheduledFilters filters(req.getCluster(), req.getState(), req.getOwner(),
            req.getJobName(), req.getEventName(), req.getEventText());

        IArrayOf<IEspScheduledWU> results;
        if(notEmpty(req.getPushEventName()))
            resp.setPushEventName(req.getPushEventName());
        if(notEmpty(req.getPushEventText()))
            resp.setPushEventText(req.getPushEventText());

        unsigned i = 0;
        IArrayOf<IEspServerInfo> servers;
#ifdef _CONTAINERIZED
        Owned<IStringIterator> targets = getContainerTargetClusters(nullptr, nullptr);
        ForEach(*targets)
        {
            SCMStringBuffer target;
            targets->str(target);
            const char *iclusterName = target.str();
#else
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
        Owned<IConstEnvironment> environment = factory->openEnvironment();
        Owned<IPropertyTree> root = &environment->getPTree();

        Owned<IPropertyTreeIterator> ic = root->getElements("Software/Topology/Cluster");
        ForEach(*ic)
        {
            IPropertyTree &cluster = ic->query();
            const char *iclusterName = cluster.queryProp("@name");
            if (isEmpty(iclusterName))
                continue;
#endif
            if (filters.cluster.isEmpty())
                getScheduledWUs(context, &filters, iclusterName, results);
            else if (strieq(filters.cluster, iclusterName))
            {
                getScheduledWUs(context, &filters, filters.cluster, results);
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
        appendUrlParameter(Query, "EventName", filters.eventName, first);
        appendUrlParameter(Query, "ECluster", filters.cluster, first);
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUListLocalFileRequired", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);
        PROGLOG("WUListLocalFileRequired: %s", wuid.str());

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
        StringBuffer wuid(req.getWuid());
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
        PROGLOG("WUAddLocalFileToWorkunit: %s, name %s", wuid.str(), varname);

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

bool CWsWorkunitsEx::onWUGetGraphNameAndTypes(IEspContext &context,IEspWUGetGraphNameAndTypesRequest &req, IEspWUGetGraphNameAndTypesResponse &resp)
{
    try
    {
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUGraphQuery", wuid);
        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);
        PROGLOG("WUGetGraphNameAndTypes: %s", wuid.str());

        StringBuffer type(req.getType());
        WUGraphType graphType = GraphTypeAny;
        if (type.trim().length())
            graphType = getGraphTypeFromString(type.str());

        IArrayOf<IEspNameAndType> graphNameAndTypes;
        WsWuInfo winfo(context, wuid.str());
        winfo.getWUGraphNameAndTypes(graphType, graphNameAndTypes);
        resp.setGraphNameAndTypes(graphNameAndTypes);
    }

    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool CWsWorkunitsEx::onWUProcessGraph(IEspContext &context,IEspWUProcessGraphRequest &req, IEspWUProcessGraphResponse &resp)
{
    try
    {
        StringBuffer wuid(req.getWuid());
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

        PROGLOG("WUProcessGraph: %s, Graph Name %s", wuid.str(), req.getName());
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
        StringBuffer wuid(req.getWuid());
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
            PROGLOG("WUGetGraph: %s", wuid.str());
            Owned<IConstWUGraphIterator> it = &cw->getGraphs(GraphTypeAny);
            ForEach(*it)
                readGraph(context, req.getSubGraphId(), id, running, &it->query(), graphs);
        }
        else
        {
            PROGLOG("WUGetGraph: %s, Graph Name %s", wuid.str(), req.getGraphName());
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

bool CWsWorkunitsEx::onWUDetails(IEspContext &context, IEspWUDetailsRequest &req, IEspWUDetailsResponse &resp)
{
    try
    {
        StringBuffer wuid(req.getWUID());
        WsWuHelpers::checkAndTrimWorkunit("WUDetails", wuid);

        PROGLOG("WUDetails: %s", wuid.str());

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        WUDetails wuDetails(cw, wuid);
        wuDetails.processRequest(req, resp);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

static void getWUDetailsMetaProperties(IArrayOf<IEspWUDetailsMetaProperty> & properties)
{
    for (unsigned sk=StKindAll+1; sk<StMax;++sk)
    {
        const char * s = queryStatisticName((StatisticKind)sk);
        if (s && *s)
        {
            Owned<IEspWUDetailsMetaProperty> property = createWUDetailsMetaProperty("","");
            property->setName(s);
            property->setValueType(CWUDetailsAttrValueType_Single);
            properties.append(*property.getClear());
        }
    }
    for (WuAttr attr=WaKind; attr<WaMax; ++attr)
    {
        Owned<IEspWUDetailsMetaProperty> property = createWUDetailsMetaProperty("","");
        const char * s = queryWuAttributeName(attr);
        assertex(s && *s);
        property->setName(s);
        if (isListAttribute(attr))
            property->setValueType(CWUDetailsAttrValueType_List);
        else if (isMultiAttribute(attr))
            property->setValueType(CWUDetailsAttrValueType_Multi);
        else
            property->setValueType(CWUDetailsAttrValueType_Single);
        properties.append(*property.getClear());
    }
}

static void getWUDetailsMetaScopeTypes(StringArray & scopeTypes)
{
    for (unsigned sst=SSTall+1; sst<SSTmax; ++sst)
    {
        const char * s = queryScopeTypeName((StatisticScopeType)sst);
        if (s && *s)
            scopeTypes.append(s);
    }
}

static void getWUDetailsMetaMeasures(StringArray & measures)
{
    for (unsigned measure=SMeasureAll+1; measure<SMeasureMax; ++measure)
    {
        const char *s = queryMeasureName((StatisticMeasure)measure);
        if (s && *s)
            measures.append(s);
    }
}
static void getWUDetailsMetaActivities(IArrayOf<IConstWUDetailsActivityInfo> & activities)
{
    for (unsigned kind=((unsigned)ThorActivityKind::TAKnone)+1; kind< TAKlast; ++kind)
    {
        Owned<IEspWUDetailsActivityInfo> activity = createWUDetailsActivityInfo("","");
        const char * name = getActivityText(static_cast<ThorActivityKind>(kind));
        assertex(name && *name);
        activity->setKind(kind);
        activity->setName(name);
        activity->setIsSink(isActivitySink(static_cast<ThorActivityKind>(kind)));
        activity->setIsSource(isActivitySource(static_cast<ThorActivityKind>(kind)));
        activities.append(*activity.getClear());
    }
}

bool CWsWorkunitsEx::onWUDetailsMeta(IEspContext &context, IEspWUDetailsMetaRequest &req, IEspWUDetailsMetaResponse &resp)
{
    try
    {
        IArrayOf<IEspWUDetailsMetaProperty> properties;
        getWUDetailsMetaProperties(properties);
        resp.setProperties(properties);

        StringArray scopeTypes;
        getWUDetailsMetaScopeTypes(scopeTypes);
        resp.setScopeTypes(scopeTypes);

        StringArray measures;
        getWUDetailsMetaMeasures(measures);
        resp.setMeasures(measures);

        IArrayOf<IConstWUDetailsActivityInfo> activities;
        getWUDetailsMetaActivities(activities);
        resp.setActivities(activities);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

#ifdef _USE_CPPUNIT

#include "unittests.hpp"

class WUDetailsMetaTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( WUDetailsMetaTest );
        CPPUNIT_TEST(testWUDetailsMeta);
    CPPUNIT_TEST_SUITE_END();

    void testWUDetailsMeta()
    {
        // These calls also check that all the calls required to build WUDetailsMeta
        // are successful.
        IArrayOf<IEspWUDetailsMetaProperty> properties;
        getWUDetailsMetaProperties(properties);
        unsigned expectedOrdinalityProps = StMax - (StKindAll + 1) + (WaMax-WaKind);
        ASSERT(properties.ordinality()==expectedOrdinalityProps);

        StringArray scopeTypes;
        getWUDetailsMetaScopeTypes(scopeTypes);
        unsigned expectedOrdinalityScopeTypes = SSTmax - (SSTall+1);
        ASSERT(scopeTypes.ordinality()==expectedOrdinalityScopeTypes);

        StringArray measures;
        getWUDetailsMetaMeasures(measures);
        unsigned expectedOrdinalityMeasures = SMeasureMax - (SMeasureAll+1);
        ASSERT(measures.ordinality()==expectedOrdinalityMeasures);

        IArrayOf<IConstWUDetailsActivityInfo> activities;
        getWUDetailsMetaActivities(activities);
        unsigned expectedOrdinalityActivities = TAKlast - (TAKnone+1);
        ASSERT(activities.ordinality()==expectedOrdinalityActivities);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( WUDetailsMetaTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( WUDetailsMetaTest, "WUDetailsMetaTest" );

#endif // _USE_CPPUNIT

bool CWsWorkunitsEx::onWUGraphInfo(IEspContext &context,IEspWUGraphInfoRequest &req, IEspWUGraphInfoResponse &resp)
{
    try
    {
        StringBuffer wuid(req.getWuid());
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
        StringBuffer wuid(req.getWuid());
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
        StringBuffer wuid(req.getWuid());
        WsWuHelpers::checkAndTrimWorkunit("WUGraphTiming", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str());
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);
        PROGLOG("WUGraphTiming: %s", wuid.str());

        resp.updateWorkunit().setWuid(wuid.str());

        WsWuInfo winfo(context, cw);
        IArrayOf<IConstECLTimingData> timingData;
        winfo.getGraphTimingData(timingData);
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
            Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
            Owned<IConstEnvironment> environment = factory->openEnvironment();
            Owned<IPropertyTree> root = &environment->getPTree();
            if(strieq(method,"WUQuery"))
            {
                SecAccessFlags accessOwn;
                SecAccessFlags accessOthers;
                getUserWuAccessFlags(context, accessOwn, accessOthers, false);

                xml.append("<WUQuery>");
                if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
                {
                    context.setAuthStatus(AUTH_STATUS_NOACCESS);
                    xml.appendf("<ErrorMessage>Access to workunit is denied.</ErrorMessage>");
                }
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

                SecAccessFlags accessOwn;
                SecAccessFlags accessOthers;
                getUserWuAccessFlags(context, accessOwn, accessOthers, false);
                if ((accessOwn == SecAccess_None) && (accessOthers == SecAccess_None))
                {
                    context.setAuthStatus(AUTH_STATUS_NOACCESS);
                    xml.appendf("<ErrorMessage>Access to workunit is denied.</ErrorMessage>");
                }
                else
                {
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
                }
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

int CWsWorkunitsSoapBindingEx::onStartUpload(IEspContext &ctx, CHttpRequest* request, CHttpResponse* response, const char *serv, const char *method)
{
    StringArray fileNames, files;
    StringBuffer source;
    Owned<IMultiException> me = MakeMultiException(source.setf("WsWorkunits::%s()", method).str());
    try
    {
        if (strieq(method, "ImportWUZAPFile"))
        {
            SecAccessFlags accessOwn, accessOthers;
            getUserWuAccessFlags(ctx, accessOwn, accessOthers, false);
            if ((accessOwn != SecAccess_Full) || (accessOthers != SecAccess_Full))
                throw MakeStringException(-1, "Permission denied.");
    
            StringBuffer password;
            request->getParameter("Password", password);

            request->readContentToFiles(nullptr, zipFolder, fileNames);
            unsigned count = fileNames.ordinality();
            if (count == 0)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Failed to read upload content.");
            //For now, we only support importing 1 ZAP report per ImportWUZAPFile request for a better response time.
            //Some ZAP report could be very big. It may take a log time to import.
            if (count > 1)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Only one WU ZAP report is allowed.");

            VStringBuffer fileName("%s%s", zipFolder, fileNames.item(0));
            wswService->queryWUFactory()->importWorkUnit(fileName, password,
                wswService->getDataDirectory(), "ws_workunits", ctx.queryUserId(), ctx.querySecManager(), ctx.queryUser());
        }
        else
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "WsWorkunits::%s does not support the upload_ option.", method);
    }
    catch (IException* e)
    {
        me->append(*e);
    }
    catch (...)
    {
        me->append(*MakeStringExceptionDirect(ECLWATCH_INTERNAL_ERROR, "Unknown Exception"));
    }
    return onFinishUpload(ctx, request, response, serv, method, fileNames, files, me);
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

    WsWuHelpers::submitWsWorkunit(context, wuid.str(), req.getCluster(), nullptr, 0, 0, true, false, false, nullptr, nullptr, &req.getDebugValues(), nullptr);
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

    PROGLOG("WUDeploy generates: %s", wuid.str());
    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}


StringBuffer &sharedObjectFileName(StringBuffer &filename, const char *name, const char *ext, unsigned copy)
{
    filename.append((name && *name) ? name : "workunit");
    if (copy)
        filename.append('_').append(copy);
    if (notEmpty(ext))
        filename.append(ext);
    return filename;
}

inline StringBuffer &buildFullDllPath(StringBuffer &dllpath, StringBuffer &dllname, const char *dir, const char *name, const char *ext, unsigned copy)
{
    return addPathSepChar(dllpath.set(dir)).append(sharedObjectFileName(dllname, name, ext, copy));
}

void writeTempSharedObject(const MemoryBuffer &obj, const char *dir, StringBuffer &filename)
{
    OwnedIFileIO io = createUniqueFile(dir, "query_copy_dll_", NULL, filename);
    io->write(0, obj.length(), obj.toByteArray());
}

void writeSharedObject(const char *srcpath, const MemoryBuffer &obj, const char *dir, StringBuffer &dllpath, StringBuffer &dllname)
{
    StringBuffer name, ext;
    if (srcpath && *srcpath)
        splitFilename(srcpath, NULL, NULL, &name, &ext);

    unsigned copy=0;
    buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), copy);

    unsigned crc=0;
    StringBuffer tempDllName;
    const unsigned attempts = 3; // max attempts
    for (unsigned i=0; i<attempts; i++)
    {
        while (checkFileExists(dllpath.str()))
        {
            if (crc==0)
                crc = crc32(obj.toByteArray(), obj.length(), 0);
            if (crc == crc_file(dllpath.str()))
            {
                DBGLOG("Workunit dll already exists: %s", dllpath.str());
                if (tempDllName.length())
                    removeFileTraceIfFail(tempDllName);
                return;
            }
            buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), ++copy);
        }
        if (!tempDllName.length())
            writeTempSharedObject(obj, dir, tempDllName);
        try
        {
            renameFile(dllpath, tempDllName, false);
            return;
        }
        catch (IException *e)
        {
            EXCLOG(e, "writeSharedObject"); //pretty small window for another copy of this dll to sneak by
            e->Release();
        }
    }

    throw MakeStringException(ECLWATCH_CANNOT_COPY_DLL, "Failed copying shared object %s", srcpath);
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
    writeSharedObject(srcname.str(), obj, dir, dllpath, dllname);

    NewWsWorkunit wu(context, wuid); //duplicate wuid made unique

    wuid.set(wu->queryWuid());
    wu->setClusterName(cluster);
    wu->commit();

    StringBuffer dllXML;
    if (getWorkunitXMLFromFile(dllpath.str(), dllXML))
    {
        Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit(dllXML.str());
        queryExtendedWU(wu)->copyWorkUnit(embeddedWU, true, true);
    }

    wu.associateDll(dllpath.str(), dllname.str());

    if (name && *name)
        wu->setJobName(name);

    //clean slate, copy only select items from processed workunit xml
    if (srcxml)
    {
        if (srcxml->hasProp("@jobName"))
            wu->setJobName(srcxml->queryProp("@jobName"));
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

    PROGLOG("WUDeploy generates: %s", wuid.str());
    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}

bool CWsWorkunitsEx::onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    const char *type = skipCompressedTypeQualifier(req.getObjType());
    try
    {
        ensureWsCreateWorkunitAccess(context);

        if (!isEmptyString(req.getCluster()))
            validateTargetName(req.getCluster());
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

bool CWsWorkunitsEx::onWUCreateZAPInfo(IEspContext &context, IEspWUCreateZAPInfoRequest &req, IEspWUCreateZAPInfoResponse &resp)
{
    try
    {
        CWsWuZAPInfoReq zapInfoReq;
        zapInfoReq.wuid = req.getWuid();

        WsWuHelpers::checkAndTrimWorkunit("WUCreateZAPInfo", zapInfoReq.wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cwu = factory->openWorkUnit(zapInfoReq.wuid.str());
        if(!cwu.get())
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT, "Cannot open workunit %s.", zapInfoReq.wuid.str());
        ensureWsWorkunitAccess(context, *cwu, SecAccess_Read);
        PROGLOG("WUCreateZAPInfo(): %s", zapInfoReq.wuid.str());

        zapInfoReq.espIP = req.getESPIPAddress();
        zapInfoReq.thorIP = req.getThorIPAddress();
        zapInfoReq.problemDesc = req.getProblemDescription();
        zapInfoReq.whatChanged = req.getWhatChanged();
        zapInfoReq.whereSlow = req.getWhereSlow();
        zapInfoReq.includeThorSlaveLog = req.getIncludeThorSlaveLog();
        zapInfoReq.zapFileName = req.getZAPFileName();
        zapInfoReq.password = req.getZAPPassword();
    
        StringBuffer zipFileName, zipFileNameWithPath;
        //CWsWuFileHelper may need ESP's <Directories> settings to locate log files. 
        CWsWuFileHelper helper(directories);
        helper.createWUZAPFile(context, cwu, zapInfoReq, zipFileName, zipFileNameWithPath, thorSlaveLogThreadPoolSize);

        //Download ZIP file to user
        Owned<IFile> f = createIFile(zipFileNameWithPath.str());
        Owned<IFileIO> io = f->open(IFOread);
        unsigned zapFileSize = (unsigned) io->size();
        if (zapFileSize > MAX_ZAP_BUFFER_SIZE)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "WUCreateZAPInfo: ZAP file size is too big (>10M) to be retrieved. Please call /WsWorkunits/WUCreateAndDownloadZAPInfo using HTTP GET.");

        MemoryBuffer mb;
        void * data = mb.reserve(zapFileSize);
        size32_t read = io->read(0, zapFileSize, data);
        mb.setLength(read);
        resp.setThefile(mb);
        resp.setThefile_mimetype(HTTP_TYPE_OCTET_STREAM);
        resp.setZAPFileName(zipFileName.str());
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
        StringBuffer wuid(req.getWUID());
        WsWuHelpers::checkAndTrimWorkunit("WUGetZAPInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);
        PROGLOG("WUGetZAPInfo: %s", wuid.str());

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
        double version = context.getClientVersion();
        if (version >= 1.73)
        {
            resp.setEmailTo(zapEmailTo.get());
            resp.setEmailFrom(zapEmailFrom.get());
        }
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

static const char * checkGetStatsNullInput(const char * s)
{
    if (!s || !*s)
        return nullptr;
    return s;
}

static const char * checkGetStatsInput(const char * s)
{
    if (!s || !*s)
        return "*";
    return s;
}

bool CWsWorkunitsEx::onWUGetStats(IEspContext &context, IEspWUGetStatsRequest &req, IEspWUGetStatsResponse &resp)
{
    //This function is deprecated for 7.x and will be removed shortly afterwards.
    //Anything that cannot be implemented with the scope iterator is implemented as a post filter
    try
    {
        const char* creatorType = checkGetStatsNullInput(req.getCreatorType());
        const char* creator = checkGetStatsNullInput(req.getCreator());
        const char* scopeType = checkGetStatsNullInput(req.getScopeType());
        const char* scope = checkGetStatsNullInput(req.getScope());
        const char* kind = checkGetStatsNullInput(req.getKind());
        const char* measure = req.getMeasure();

        WuScopeFilter filter;
        StatisticsFilter statsFilter(creatorType, creator, "*", "*", "*", "*");
        filter.addOutputProperties(PTstatistics);
        if (scopeType)
            filter.addScopeType(scopeType);
        if (scope)
            filter.addScope(scope);
        if (kind)
            filter.addOutputStatistic(kind);
        if (measure)
            filter.setMeasure(measure);
        if (!req.getMinScopeDepth_isNull() && !req.getMaxScopeDepth_isNull())
            filter.setDepth(req.getMinScopeDepth(), req.getMaxScopeDepth());
        else if (!req.getMinScopeDepth_isNull())
            filter.setDepth(req.getMinScopeDepth(), req.getMinScopeDepth());

        if (!req.getMinValue_isNull() || !req.getMaxValue_isNull())
        {
            unsigned __int64 lowValue = 0;
            unsigned __int64 highValue = MaxStatisticValue;
            if (!req.getMinValue_isNull())
                lowValue = (unsigned __int64)req.getMinValue();
            if (!req.getMaxValue_isNull())
                highValue = (unsigned __int64)req.getMaxValue();
            statsFilter.setValueRange(lowValue, highValue);
        }

        const char * textFilter = req.getFilter();
        if (textFilter)
            statsFilter.setFilter(textFilter);

        filter.setIncludeNesting(0).finishedFilter();

        bool createDescriptions = false;
        if (!req.getCreateDescriptions_isNull())
            createDescriptions = req.getCreateDescriptions();

        StringBuffer wuid(req.getWUID());
        PROGLOG("WUGetStats: %s", wuid.str());

        IArrayOf<IEspWUStatisticItem> statistics;
        if (strchr(wuid, '*'))
        {
            WUSortField filters[2];
            MemoryBuffer filterbuf;
            filters[0] = WUSFwildwuid;
            filterbuf.append(wuid.str());
            filters[1] = WUSFterm;
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
            Owned<IConstWorkUnitIterator> iter = factory->getWorkUnitsSorted((WUSortField) (WUSFwuid), filters, filterbuf.bufferBase(), 0, INT_MAX, NULL, NULL);
            ForEach(*iter)
            {
                Owned<IConstWorkUnit> workunit = factory->openWorkUnit(iter->query().queryWuid());
                if (workunit)
                {
                    //No need to check for access since the list is already filtered
                    WsWuInfo winfo(context, workunit->queryWuid());
                    winfo.getStats(filter, statsFilter, createDescriptions, statistics);
                }
            }
        }
        else
        {
            WsWuHelpers::checkAndTrimWorkunit("WUInfo", wuid);
            ensureWsWorkunitAccess(context, wuid, SecAccess_Read);

            WsWuInfo winfo(context, wuid);
            winfo.getStats(filter, statsFilter, createDescriptions, statistics);
        }
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
        ensureWsWorkunitAccess(context, wuid, SecAccess_Read);
        PROGLOG("WUListArchiveFiles: %s", wuid);

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
        ensureWsWorkunitAccess(context, wuid, SecAccess_Read);
        PROGLOG("WUGetArchiveFile: %s", wuid);

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

const char *CWsWorkunitsEx::gatherQueryFileCopyErrors(IArrayOf<IConstLogicalFileError> &errors, StringBuffer &errorMsg)
{
    if (!errors.ordinality())
        return errorMsg.str();

    errorMsg.append("Query File Copy Error(s):");
    ForEachItemIn(i, errors)
    {
        IConstLogicalFileError &error = errors.item(i);
        errorMsg.append(" ").append(error.getLogicalName()).append(": ");
        errorMsg.append(error.getError()).append(";");
    }
    return errorMsg.str();
}

const char *CWsWorkunitsEx::gatherExceptionMessage(const IMultiException &me, StringBuffer &exceptionMsg)
{
    exceptionMsg.append("Exception(s):");
    aindex_t count = me.ordinality();
    for (aindex_t i=0; i<count; i++)
    {
        IException& e = me.item(i);
        StringBuffer errMsg;
        exceptionMsg.append(" ").append(e.errorCode()).append(": ");
        exceptionMsg.append(e.errorMessage(errMsg).str()).append(";");
    }
    exceptionMsg.append("\n");
    return exceptionMsg.str();
}

const char *CWsWorkunitsEx::gatherWUException(IConstWUExceptionIterator &it, StringBuffer &exceptionMsg)
{
    unsigned numErr = 0, numWRN = 0, numInf = 0, numAlert = 0;
    ForEach(it)
    {
        IConstWUException & cur = it.query();
        SCMStringBuffer src, msg, file;
        exceptionMsg.append(" Exception: Code: ").append(cur.getExceptionCode());
        exceptionMsg.append(" Source: ").append(cur.getExceptionSource(src).str());
        exceptionMsg.append(" Message: ").append(cur.getExceptionMessage(msg).str());
        exceptionMsg.append(" FileName: ").append(cur.getExceptionFileName(file).str());
        exceptionMsg.append(" LineNo: ").append(cur.getExceptionLineNo());
        exceptionMsg.append(" Column: ").append(cur.getExceptionColumn());
        if (cur.getActivityId())
            exceptionMsg.append(" ActivityId: ").append(cur.getActivityId());
        if (cur.getPriority())
            exceptionMsg.append(" Priority: ").append(cur.getPriority());
        exceptionMsg.append(" Scope: ").append(cur.queryScope());

        const char * label = "";
        switch (cur.getSeverity())
        {
            default:
            case SeverityError: label = "Error"; numErr++; break;
            case SeverityWarning: label = "Warning"; numWRN++; break;
            case SeverityInformation: label = "Info"; numInf++; break;
            case SeverityAlert: label = "Alert"; numAlert++; break;
        }
        exceptionMsg.append(" Severity: ").append(label);
    }
    exceptionMsg.append(" Total error: ").append(numErr);
    exceptionMsg.append(" warning: ").append(numWRN);
    exceptionMsg.append(" info: ").append(numInf);
    exceptionMsg.append(" alert: ").append(numAlert);
    exceptionMsg.append("\n");
    return exceptionMsg.str();
}

const char *CWsWorkunitsEx::gatherECLException(IArrayOf<IConstECLException> &exceptions, StringBuffer &exceptionMsg)
{
    unsigned errorCount = 0, warningCount = 0;
    ForEachItemIn(i, exceptions)
    {
        IConstECLException &e = exceptions.item(i);
        if (strieq(e.getSeverity(), "warning"))
        {
            warningCount++;
            exceptionMsg.append(" Warning: ");
        }
        else if (strieq(e.getSeverity(), "error"))
        {
            errorCount++;
            exceptionMsg.append(" Error: ");
        }

        if (e.getSource())
            exceptionMsg.append(e.getSource()).append(": ");
        if (e.getFileName())
            exceptionMsg.append(e.getFileName());
        if (!e.getLineNo_isNull() && !e.getColumn_isNull())
            exceptionMsg.appendf("(%d,%d): ", e.getLineNo(), e.getColumn());

        exceptionMsg.appendf("%s C%d: %s;", e.getSeverity(), e.getCode(), e.getMessage());
    }
    exceptionMsg.append(" Total error: ").append(errorCount);
    exceptionMsg.append(" warning: ").append(warningCount);
    exceptionMsg.append("\n");
    return exceptionMsg.str();
}

bool CWsWorkunitsEx::readDeployWUResponse(CWUDeployWorkunitResponse* deployResponse, StringBuffer &wuid, StringBuffer &result)
{
    const IMultiException &me = deployResponse->getExceptions();
    if (me.ordinality())
        gatherExceptionMessage(me, result);

    const char *w = deployResponse->getWorkunit().getWuid();
    if (isEmptyString(w))
    {
        result.appendf("Error: no workunit ID!");
        return false;
    }

    wuid.set(w);
    const char *state = deployResponse->getWorkunit().getState();
    bool isCompiled = (strieq(state, "compiled") || strieq(state, "completed"));
    if (!isCompiled)
        result.appendf("state: %s;", state);

    gatherECLException(deployResponse->getWorkunit().getExceptions(), result);

    return isCompiled;
}

void CWsWorkunitsEx::addEclDefinitionActionResult(const char *eclDefinition, const char *result, const char *wuid,
    const char *queryID, const char* strAction, bool logResult, IArrayOf<IConstWUEclDefinitionActionResult> &results)
{
    Owned<IEspWUEclDefinitionActionResult> res = createWUEclDefinitionActionResult();
    if (!isEmptyString(eclDefinition))
        res->setEclDefinition(eclDefinition);
    res->setAction(strAction);
    res->setResult(result);
    if (!isEmptyString(wuid))
        res->setWUID(wuid);
    if (!isEmptyString(queryID))
        res->setQueryID(queryID);
    results.append(*res.getClear());
    if (logResult)
        PROGLOG("%s", result);
}

void CWsWorkunitsEx::checkEclDefinitionSyntax(IEspContext &context, const char *target, const char *eclDefinition,
    int msToWait, IArrayOf<IConstWUEclDefinitionActionResult> &results)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    NewWsWorkunit wu(factory, context);
    wu->setAction(WUActionCheck);
    wu.setQueryMain(eclDefinition);

    StringAttr wuid(wu->queryWuid());  // NB queryWuid() not valid after workunit.clear()
    wu->commit();
    wu.clear();

    WsWuHelpers::submitWsWorkunit(context, wuid.str(), target, nullptr, 0, 0, true, false, false, nullptr, nullptr, nullptr, nullptr);
    waitForWorkUnitToComplete(wuid.str(), msToWait);

    Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str()));
    WUState st = cw->getState();
    bool wuTimeout = (st != WUStateAborted) && (st != WUStateCompleted) && (st != WUStateFailed);
    VStringBuffer result(" WUSyntaxCheckECL for %s:", eclDefinition);
    if (wuTimeout)
        result.append(" timed out.");

    gatherWUException(cw->getExceptions(), result);
    addEclDefinitionActionResult(eclDefinition, result.str(), wuid.str(), nullptr, "SyntaxCheck", true, results);
    cw.clear();

    if (wuTimeout)
        abortWorkUnit(wuid.str(), context.querySecManager(), context.queryUser());
    if (!factory->deleteWorkUnit(wuid.str()))
    {
        result.setf(" Workunit %s cannot be deleted now. You may delete it when its status changes.", wuid.str());
        addEclDefinitionActionResult(eclDefinition, result.str(), wuid.str(), nullptr, "SyntaxCheck", true, results);
    }
}

bool CWsWorkunitsEx::deployEclDefinition(IEspContext &context, const char *target, const char *eclDefinition,
    int msToWait, StringBuffer &wuid, StringBuffer &result)
{
    Owned<CWUDeployWorkunitRequest> deployReq = new CWUDeployWorkunitRequest("WsWorkunits");
    deployReq->setName(eclDefinition);
    deployReq->setQueryMainDefinition(eclDefinition);
    deployReq->setObjType("compressed_ecl_text");
    deployReq->setCluster(target);
    deployReq->setWait(msToWait);
    deployReq->setFileName("");

    Owned<CWUDeployWorkunitResponse> deployResponse = new CWUDeployWorkunitResponse("WsWorkunits");
    onWUDeployWorkunit(context, *deployReq, *deployResponse);
    return readDeployWUResponse(deployResponse, wuid, result);
}

void CWsWorkunitsEx::deployEclDefinition(IEspContext &context, const char *target,  const char *eclDefinition,
    int msToWait, IArrayOf<IConstWUEclDefinitionActionResult> &results)
{
    StringBuffer wuid, finalResult;
    deployEclDefinition(context, target, eclDefinition, msToWait, wuid, finalResult);
    addEclDefinitionActionResult(eclDefinition, finalResult.str(), wuid.str(), nullptr, "Deploy", true, results);
}

void CWsWorkunitsEx::publishEclDefinition(IEspContext &context, const char *target,  const char *eclDefinition,
    int msToWait, IEspWUEclDefinitionActionRequest &req, IArrayOf<IConstWUEclDefinitionActionResult> &results)
{
    StringBuffer priorityReq(req.getPriority());
    if (priorityReq.trim().length() && !isValidPriorityValue(priorityReq.str()))
    {
        VStringBuffer msg("Invalid Priority: %s", priorityReq.str());
        addEclDefinitionActionResult(eclDefinition, msg.str(), nullptr, nullptr, "Publish", true, results);
        return;
    }

    StringBuffer memoryLimitReq(req.getMemoryLimit());
    if (memoryLimitReq.trim().length() && !isValidMemoryValue(memoryLimitReq.str()))
    {
        VStringBuffer msg("Invalid MemoryLimit: %s", memoryLimitReq.str());
        addEclDefinitionActionResult(eclDefinition, msg.str(), nullptr, nullptr, "Publish", true, results);
        return;
    }

    time_t timenow;
    int startTime = time(&timenow);

    //Do deploy first
    StringBuffer wuid, finalResult;
    if (!deployEclDefinition(context, target, eclDefinition, msToWait, wuid, finalResult))
    {
        addEclDefinitionActionResult(eclDefinition, finalResult.str(), wuid.str(), nullptr, "Publish", true, results);
        return;
    }
    int timeLeft = msToWait - (time(&timenow) - startTime);
    if (timeLeft <= 0)
    {
        addEclDefinitionActionResult(eclDefinition, "Timed out after deployment", wuid.str(), nullptr, "Publish", true, results);
        return;
    }

    //Do publish now
    StringBuffer comment(req.getComment());
    StringBuffer remoteDali(req.getRemoteDali());
    StringBuffer sourceProcess(req.getSourceProcess());
    int timeLimit = req.getTimeLimit();
    int warnTimeLimit = req.getWarnTimeLimit();

    Owned<CWUPublishWorkunitRequest> publishReq = new CWUPublishWorkunitRequest("WsWorkunits");
    publishReq->setWuid(wuid.str());
    publishReq->setCluster(target);
    publishReq->setJobName(eclDefinition);

    if (!remoteDali.trim().isEmpty())
        publishReq->setRemoteDali(remoteDali.str());
    if (!sourceProcess.trim().isEmpty())
        publishReq->setSourceProcess(sourceProcess.str());
    if (!priorityReq.isEmpty())
        publishReq->setPriority(priorityReq.str());
    if (comment.str()) //allow empty
        publishReq->setComment(comment.str());

    if (req.getDeletePrevious())
        publishReq->setActivate(CWUQueryActivationMode_ActivateDeletePrevious);
    else if (req.getSuspendPrevious())
        publishReq->setActivate(CWUQueryActivationMode_ActivateSuspendPrevious);
    else
        publishReq->setActivate(req.getNoActivate() ? CWUQueryActivationMode_NoActivate : CWUQueryActivationMode_Activate);

    publishReq->setWait(timeLeft);
    publishReq->setNoReload(req.getNoReload());
    publishReq->setDontCopyFiles(req.getDontCopyFiles());
    publishReq->setAllowForeignFiles(req.getAllowForeign());
    publishReq->setUpdateDfs(req.getUpdateDfs());
    publishReq->setUpdateSuperFiles(req.getUpdateSuperfiles());
    publishReq->setUpdateCloneFrom(req.getUpdateCloneFrom());
    publishReq->setAppendCluster(!req.getDontAppendCluster());
    publishReq->setIncludeFileErrors(true);

    if (timeLimit != -1)
        publishReq->setTimeLimit(timeLimit);
    if (warnTimeLimit != (unsigned) -1)
        publishReq->setWarnTimeLimit(warnTimeLimit);
    if (!memoryLimitReq.isEmpty())
        publishReq->setMemoryLimit(memoryLimitReq.str());

    Owned<CWUPublishWorkunitResponse> publishResponse = new CWUPublishWorkunitResponse("WsWorkunits");
    onWUPublishWorkunit(context, *publishReq, *publishResponse);

    const char *id = publishResponse->getQueryId();
    if (!isEmptyString(id))
    {
        const char *qs = publishResponse->getQuerySet();
        finalResult.append("   ").append(qs ? qs : "").append('/').append(id).append(" published. ");
    }
    if (publishResponse->getReloadFailed())
        finalResult.append(" Added to target, but request to reload queries on cluster failed.");

    const IMultiException &me = publishResponse->getExceptions();
    if (me.ordinality())
        gatherExceptionMessage(me, finalResult);

    gatherQueryFileCopyErrors(publishResponse->getFileErrors(), finalResult);
    addEclDefinitionActionResult(eclDefinition, finalResult.str(), wuid.str(), id, "Publish", true, results);
}

bool CWsWorkunitsEx::onWUEclDefinitionAction(IEspContext &context, IEspWUEclDefinitionActionRequest &req, IEspWUEclDefinitionActionResponse &resp)
{
    try
    {
        CEclDefinitionActions action = req.getActionType();
        if (action == EclDefinitionActions_Undefined)
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Action not defined in onWUEclDefinitionAction.");

        ensureWsCreateWorkunitAccess(context);

        StringBuffer target(req.getTarget());
        if (target.trim().isEmpty())
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"Target not defined in onWUEclDefinitionAction.");

        IArrayOf<IConstWUEclDefinitionActionResult> results;
        StringArray &eclDefinitions = req.getEclDefinitions();
        int msToWait = req.getMsToWait();
        for (aindex_t i = 0; i < eclDefinitions.length(); i++)
        {
            StringBuffer eclDefinitionName(eclDefinitions.item(i));
            if (eclDefinitionName.trim().isEmpty())
                UWARNLOG("Empty ECL Definition name in WUEclDefinitionAction request");
            else if (action == CEclDefinitionActions_SyntaxCheck)
                checkEclDefinitionSyntax(context, target.str(), eclDefinitionName.str(), msToWait, results);
            else if (action == CEclDefinitionActions_Deploy)
                deployEclDefinition(context, target.str(), eclDefinitionName.str(), msToWait, results);
            else
                publishEclDefinition(context, target.str(), eclDefinitionName.str(), msToWait, req, results);
        }

        resp.setActionResults(results);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

static const char *eclccPluginPath = "ECLCC_PLUGIN_PATH=";
static unsigned eclccPluginPathLength = strlen(eclccPluginPath);

//Run 'eclcc -showpaths' command which returns something like:
//
//...
//ECLCC_PLUGIN_PATH=/opt/HPCCSystems/plugins:/opt/HPCCSystems/versioned/python2
//...
//
//Find out the file paths after the ECLCC_PLUGIN_PATH=
//In each file path, find out all of the .ecllib files and qualified .so files.
bool CWsWorkunitsEx::onWUGetPlugins(IEspContext &context, IEspWUGetPluginsRequest &req, IEspWUGetPluginsResponse &resp)
{
    try
    {
        StringBuffer eclccPaths, error;
        unsigned ret = runExternalCommand(eclccPaths, error, "eclcc -showpaths", nullptr);
        if (ret != 0)
           throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to run 'eclcc -showpaths': %s", error.str());

        if (eclccPaths.isEmpty())
        {
            IWARNLOG("The 'eclcc -showpaths' returns empty response.");
            return true;
        }

        StringArray pluginFolders;
        readPluginFolders(eclccPaths, pluginFolders);

        IArrayOf<IConstWUEclPluginsInFolder> plugins;
        ForEachItemIn(i, pluginFolders)
        {
            const char *pluginFolder = pluginFolders.item(i);
            Owned<IEspWUEclPluginsInFolder> folder = createWUEclPluginsInFolder();
            folder->setPath(pluginFolder);
            findPlugins(pluginFolder, false, folder->getPlugins());
            findPlugins(pluginFolder, true, folder->getPlugins());
            plugins.append(*folder.getClear());
        }
        resp.setPlugins(plugins);
    }
    catch(IException *e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
   return true;
}

void CWsWorkunitsEx::readPluginFolders(StringBuffer &eclccPaths, StringArray &pluginFolders)
{
    int lineLength = 0;
    int curPos = 0;
    int eclccPathsLength = eclccPaths.length();
    const char *eclccPathsPtr = eclccPaths.str();
    while (true)
    {
        __int64 nextPos = Utils::getLine(eclccPathsLength, curPos, eclccPathsPtr, lineLength);
        if (strnicmp(eclccPathsPtr + curPos,  eclccPluginPath, eclccPluginPathLength) != 0)
        {
            curPos = nextPos;
            continue;
        }

        //ex: ECLCC_PLUGIN_PATH=/opt/HPCCSystems/plugins:/opt/HPCCSystems/versioned/python2
        if (lineLength > eclccPluginPathLength)
        {
            StringBuffer eclccPluginPathBuf;
            eclccPluginPathBuf.append(lineLength - eclccPluginPathLength, eclccPathsPtr + curPos + eclccPluginPathLength);
            pluginFolders.appendList(eclccPluginPathBuf, ":");
        }
        break;
    }
}

void CWsWorkunitsEx::findPlugins(const char *pluginFolder, bool dotSoFile, StringArray &plugins)
{
    Owned<IFile> pluginDir = createIFile(pluginFolder);
    Owned<IDirectoryIterator> pluginFiles = pluginDir->directoryFiles(dotSoFile ? "*.so" : "*.ecllib", false, false);
    ForEach(*pluginFiles)
    {
        const char *pluginFile = pluginFiles->query().queryFilename();
        StringBuffer fileName;
        splitFilename(pluginFile, nullptr, nullptr, &fileName, &fileName);
        //The ecllib files should be reported as plugins.
        //.so files are reported as plugins if getSharedProcedure("getECLPluginDefinition")->ECL is non null
        if (!dotSoFile || checkPluginECLAttr(pluginFile))
            plugins.append(fileName);
    }
}

bool CWsWorkunitsEx::checkPluginECLAttr(const char *fileNameWithPath)
{
    HINSTANCE h = LoadSharedObject(fileNameWithPath, true, false);
    if (!h)
        throw makeStringExceptionV(ECLWATCH_INTERNAL_ERROR, "can't load library %s", fileNameWithPath);

    EclPluginDefinition p = (EclPluginDefinition) GetSharedProcedure(h, "getECLPluginDefinition");
    if (p)
    {
        ECLPluginDefinitionBlock pb;
        pb.size = sizeof(pb);
        if (p(&pb) && pb.ECL)
            return true;
    }
    return false;
}
