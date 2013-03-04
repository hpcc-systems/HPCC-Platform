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
#include "roxiemanager.hpp"
#include "dadfs.hpp"
#include "dfuwu.hpp"
#include "thorplugin.hpp"
#include "roxiecontrol.hpp"

#include "package.h"

#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

#define ESP_WORKUNIT_DIR "workunits/"

class NewWsWorkunit : public Owned<IWorkUnit>
{
public:
    NewWsWorkunit(IWorkUnitFactory *factory, IEspContext &context)
    {
        create(factory, context);
    }

    NewWsWorkunit(IEspContext &context)
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        create(factory, context);
    }

    ~NewWsWorkunit() { if (get()) get()->commit(); }

    void create(IWorkUnitFactory *factory, IEspContext &context)
    {
        setown(factory->createWorkUnit(NULL, "ws_workunits", context.queryUserId()));
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

void setWsWuXmlParameters(IWorkUnit *wu, const char *xml, bool setJobname=false)
{
    if (!xml || !*xml)
        return;
    Owned<IPropertyTree> tree = createPTreeFromXMLString(xml, ipt_none, (PTreeReaderOptions)(ptr_ignoreWhiteSpace | ptr_ignoreNameSpaces));
    IPropertyTree *root = tree.get();
    if (strieq(root->queryName(), "Envelope"))
        root = root->queryPropTree("Body/*[1]");
    if (!root)
        return;
    if (setJobname)
    {
        SCMStringBuffer name;
        wu->getJobName(name);
        if (!name.length())
            wu->setJobName(root->queryName());
    }
    wu->setXmlParams(LINK(root));
}

void setWsWuXmlParameters(IWorkUnit *wu, const char *xml, IArrayOf<IConstNamedValue> *variables, bool setJobname=false)
{
    StringBuffer extParamXml;
    if (variables && variables->length())
    {
        Owned<IPropertyTree> paramTree = (xml && *xml) ? createPTreeFromXMLString(xml) : createPTree("input");
        ForEachItemIn(i, *variables)
        {
            IConstNamedValue &item = variables->item(i);
            const char *name = item.getName();
            const char *value = item.getValue();
            if (!name || !*name)
                continue;
            if (!value)
            {
                size_t len = strlen(name);
                char last = name[len-1];
                if (last == '-' || last == '+')
                {
                    StringAttr s(name, len-1);
                    paramTree->setPropInt(s.get(), last == '+' ? 1 : 0);
                }
                else
                    paramTree->setPropInt(name, 1);
                continue;
            }
            paramTree->setProp(name, value);
        }
        toXML(paramTree, extParamXml);
        xml=extParamXml.str();
    }
    setWsWuXmlParameters(wu, xml, setJobname);
}

void submitWsWorkunit(IEspContext& context, IConstWorkUnit* cw, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow, bool resetVariables,
    const char *paramXml=NULL, IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL)
{
    ensureWsWorkunitAccess(context, *cw, SecAccess_Write);
    switch(cw->getState())
    {
        case WUStateRunning:
        case WUStateDebugPaused:
        case WUStateDebugRunning:
        case WUStateCompiling:
        case WUStateAborting:
        case WUStateBlocked:
        {
            SCMStringBuffer descr;
            throw MakeStringException(ECLWATCH_CANNOT_SUBMIT_WORKUNIT, "Cannot submit the workunit. Workunit state is '%s'.", cw->getStateDesc(descr).str());
        }
    }

    SCMStringBuffer wuid;
    cw->getWuid(wuid);

    WorkunitUpdate wu(&cw->lock());
    if(!wu.get())
        throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT, "Cannot update workunit %s.", wuid.str());

    wu->clearExceptions();
    if(notEmpty(cluster))
        wu->setClusterName(cluster);
    if(notEmpty(snapshot))
        wu->setSnapshot(snapshot);
    wu->setState(WUStateSubmitted);
    if (maxruntime)
        wu->setDebugValueInt("maxRunTime",maxruntime,true);

    if (debugs && debugs->length())
    {
        ForEachItemIn(i, *debugs)
        {
            IConstNamedValue &item = debugs->item(i);
            const char *name = item.getName();
            const char *value = item.getValue();
            if (!name || !*name)
                continue;
            if (!value)
            {
                size_t len = strlen(name);
                char last = name[len-1];
                if (last == '-' || last == '+')
                {
                    StringAttr s(name, len-1);
                    wu->setDebugValueInt(s.get(), last == '+' ? 1 : 0, true);
                }
                else
                    wu->setDebugValueInt(name, 1, true);
                continue;
            }
            wu->setDebugValue(name, value, true);
        }
    }

    if (resetWorkflow)
        wu->resetWorkflow();
    if (!compile)
        wu->schedule();

    if (resetVariables)
    {
        SCMStringBuffer varname;
        Owned<IConstWUResultIterator> vars = &wu->getVariables();
        ForEach (*vars)
        {
            vars->query().getResultName(varname);
            Owned<IWUResult> v = wu->updateVariableByName(varname.str());
            if (v)
                v->setResultStatus(ResultStatusUndefined);
        }
    }

    setWsWuXmlParameters(wu, paramXml, variables, (wu->getAction()==WUActionExecuteExisting));

    wu->commit();
    wu.clear();

    if (!compile)
        runWorkUnit(wuid.str());
    else if (context.querySecManager())
        secSubmitWorkUnit(wuid.str(), *context.querySecManager(), *context.queryUser());
    else
        submitWorkUnit(wuid.str(), context.queryUserId(), context.queryPassword());

    AuditSystemAccess(context.queryUserId(), true, "Submitted %s", wuid.str());
}

void submitWsWorkunit(IEspContext& context, const char *wuid, const char* cluster, const char* snapshot, int maxruntime, bool compile, bool resetWorkflow, bool resetVariables,
    const char *paramXml=NULL, IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
    if(!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid);
    return submitWsWorkunit(context, cw, cluster, snapshot, maxruntime, compile, resetWorkflow, resetVariables, paramXml, variables, debugs);
}


void copyWsWorkunit(IEspContext &context, IWorkUnit &wu, const char *srcWuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> src(factory->openWorkUnit(srcWuid, false));

    SCMStringBuffer wuid;
    wu.getWuid(wuid);

    queryExtendedWU(&wu)->copyWorkUnit(src, false);

    SCMStringBuffer token;
    wu.setSecurityToken(createToken(wuid.str(), context.queryUserId(), context.queryPassword(), token).str());
    wu.commit();
}

void runWsWorkunit(IEspContext &context, StringBuffer &wuid, const char *srcWuid, const char *cluster, const char *paramXml=NULL,
    IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL)
{
    StringBufferAdaptor isvWuid(wuid);

    NewWsWorkunit wu(context);
    wu->getWuid(isvWuid);
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, wuid.str(), cluster, NULL, 0, false, true, true, paramXml, variables, debugs);
}

void runWsWorkunit(IEspContext &context, IConstWorkUnit *cw, const char *srcWuid, const char *cluster, const char *paramXml=NULL,
    IArrayOf<IConstNamedValue> *variables=NULL, IArrayOf<IConstNamedValue> *debugs=NULL)
{
    WorkunitUpdate wu(&cw->lock());
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, cw, cluster, NULL, 0, false, true, true, paramXml, variables, debugs);
}

IException *noteException(IWorkUnit *wu, IException *e, WUExceptionSeverity level=ExceptionSeverityError)
{
    if (wu)
    {
        Owned<IWUException> we = wu->createException();
        StringBuffer s;
        we->setExceptionMessage(e->errorMessage(s).str());
        we->setExceptionSource("WsWorkunits");
        we->setSeverity(level);
        if (level==ExceptionSeverityError)
            wu->setState(WUStateFailed);
    }
    return e;
}

StringBuffer &resolveQueryWuid(StringBuffer &wuid, const char *queryset, const char *query, bool notSuspended=true, IWorkUnit *wu=NULL)
{
    Owned<IPropertyTree> qs = getQueryRegistry(queryset, true);
    if (!qs)
        throw noteException(wu, MakeStringException(ECLWATCH_QUERYSET_NOT_FOUND, "QuerySet '%s' not found", queryset));
    Owned<IPropertyTree> q = resolveQueryAlias(qs, query);
    if (!q)
        throw noteException(wu, MakeStringException(ECLWATCH_QUERYID_NOT_FOUND, "Query '%s/%s' not found", queryset, query));
    if (notSuspended && q->getPropBool("@suspended"))
        throw noteException(wu, MakeStringException(ECLWATCH_QUERY_SUSPENDED, "Query '%s/%s' is suspended", queryset, query));
    return wuid.append(q->queryProp("@wuid"));
}

void runWsWuQuery(IEspContext &context, IConstWorkUnit *cw, const char *queryset, const char *query, const char *cluster, const char *paramXml=NULL)
{
    StringBuffer srcWuid;

    WorkunitUpdate wu(&cw->lock());
    resolveQueryWuid(srcWuid, queryset, query, true, wu);
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, cw, cluster, NULL, 0, false, true, true, paramXml);
}

void runWsWuQuery(IEspContext &context, StringBuffer &wuid, const char *queryset, const char *query, const char *cluster, const char *paramXml=NULL)
{
    StringBuffer srcWuid;
    StringBufferAdaptor isvWuid(wuid);

    NewWsWorkunit wu(context);
    wu->getWuid(isvWuid);
    resolveQueryWuid(srcWuid, queryset, query, true, wu);
    copyWsWorkunit(context, *wu, srcWuid);
    wu.clear();

    submitWsWorkunit(context, wuid.str(), cluster, NULL, 0, false, true, true, paramXml);
}

class ExecuteExistingQueryInfo
{
public:
    ExecuteExistingQueryInfo(IConstWorkUnit *cw)
    {
        SCMStringBuffer isv;
        cw->getJobName(isv);
        const char *name = isv.str();
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
            if (!looksLikeAWuid(wuid))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid);

            if ((action == ActionRestore) || (action == ActionEventDeschedule))
            {
                switch(action)
                {
                case ActionRestore:
                {
                    SocketEndpoint ep;
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
                Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
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
                        int state = cw->getState();
                        switch (state)
                        {
                            case WUStateWait:
                            case WUStateAborted:
                            case WUStateCompleted:
                            case WUStateFailed:
                            case WUStateArchived:
                            case WUStateCompiled:
                            case WUStateUploadingFiles:
                                break;
                            default:
                            {
                                WorkunitUpdate wu(&cw->lock());
                                wu->setState(WUStateFailed);
                            }
                        }
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

    refreshValidClusters();

    daliServers.set(cfg->queryProp("Software/EspProcess/@daliServers"));

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

    directories.set(cfg->queryPropTree("Software/Directories"));

    const char *name = cfg->queryProp("Software/EspProcess/@name");
    getConfigurationDirectory(directories, "query", "esp", name ? name : "esp", queryDirectory);
    recursiveCreateDirectory(queryDirectory.str());

    dataCache.setown(new DataCache(DATA_SIZE));
    archivedWuCache.setown(new ArchivedWuCache(AWUS_CACHE_SIZE));

    //Create a folder for temporarily holding gzip files by WUResultBin()
    Owned<IFile> tmpdir = createIFile(TEMPZIPDIR);
    if(!tmpdir->exists())
        tmpdir->createDirectory();

    recursiveCreateDirectory(ESP_WORKUNIT_DIR);

    m_sched.start();
}

void CWsWorkunitsEx::refreshValidClusters()
{
    validClusters.kill();
    Owned<IStringIterator> it = getTargetClusters(NULL, NULL);
    ForEach(*it)
    {
        SCMStringBuffer s;
        IStringVal &val = it->str(s);
        if (!validClusters.getValue(val.str()))
            validClusters.setValue(val.str(), true);
    }
}

bool CWsWorkunitsEx::isValidCluster(const char *cluster)
{
    if (!cluster || !*cluster)
        return false;
    CriticalBlock block(crit);
    if (validClusters.getValue(cluster))
        return true;
    if (validateTargetClusterName(cluster))
    {
        refreshValidClusters();
        return true;
    }
    return false;
}

void CWsWorkunitsEx::checkAndTrimWorkunit(const char* methodName, StringBuffer& input)
{
    const char* trimmedInput = input.trim().str();
    if (isEmpty(trimmedInput))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: Workunit ID not set", methodName);

    if (!looksLikeAWuid(trimmedInput))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "%s: Invalid Workunit ID: %s", methodName, trimmedInput);

    return;
}

bool CWsWorkunitsEx::onWUCreate(IEspContext &context, IEspWUCreateRequest &req, IEspWUCreateResponse &resp)
{
    try
    {
        if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        NewWsWorkunit wu(context);
        SCMStringBuffer wuid;
        resp.updateWorkunit().setWuid(wu->getWuid(wuid).str());
        AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
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
        checkAndTrimWorkunit("WUUpdate", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Write);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        if(req.getProtected() != req.getProtectedOrig())
        {
            cw->protect(req.getProtected());
            cw.clear();
            cw.setown(factory->openWorkUnit(wuid.str(), false));
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

        setWsWuXmlParameters(wu, req.getXmlParams(), (req.getAction()==WUActionExecuteExisting));

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
        SCMStringBuffer wuid;
        wu->getWuid(wuid);
        req.setWuid(wuid.str());
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

        IArrayOf<IConstWUActionResult> results;
        if (doAction(context, req.getWuids(), *action, params, &results) && *action!=ActionDelete)
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
        SCMStringBuffer wuid;
        StringArray wuids;

        for(aindex_t i=0; i<req.getWuids().length();i++)
        {
            StringBuffer wuidStr = req.getWuids().item(i);
            checkAndTrimWorkunit("WUResubmit", wuidStr);

            ensureWsWorkunitAccess(context, wuidStr.str(), SecAccess_Write);

            wuid.set(wuidStr.str());

            try
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
                if(req.getCloneWorkunit() || req.getRecompile())
                {
                    Owned<IConstWorkUnit> src(factory->openWorkUnit(wuid.str(), false));
                    NewWsWorkunit wu(factory, context);
                    wu->getWuid(wuid);
                    queryExtendedWU(wu)->copyWorkUnit(src, false);

                    SCMStringBuffer token;
                    wu->setSecurityToken(createToken(wuid.str(), context.queryUserId(), context.queryPassword(), token).str());
                }

                wuids.append(wuid.str());

                Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str(), false));
                if(!cw)
                    throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

                submitWsWorkunit(context, cw, NULL, NULL, 0, req.getRecompile(), req.getResetWorkflow(), false);
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
        checkAndTrimWorkunit("WUSchedule", wuid);

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
            {
                SCMStringBuffer descr;
                throw MakeStringException(ECLWATCH_CANNOT_SCHEDULE_WORKUNIT, "Cannot schedule the workunit. Workunit state is '%s'.", wu->getStateDesc(descr).str());
            }
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
        checkAndTrimWorkunit("WUSubmit", wuid);

        const char *cluster = req.getCluster();
        if (isEmpty(cluster))
            throw MakeStringException(ECLWATCH_INVALID_INPUT,"No Cluster defined.");
        if (!isValidCluster(cluster))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", cluster);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());

        if (cw->getAction()==WUActionExecuteExisting)
        {
            ExecuteExistingQueryInfo info(cw);
            if (info.queryset.isEmpty() || info.query.isEmpty())
            {
                WorkunitUpdate wu(&cw->lock());
                throw noteException(wu, MakeStringException(ECLWATCH_INVALID_INPUT,"Queryset and/or query not specified"));
            }

            runWsWuQuery(context, cw, info.queryset.sget(), info.query.sget(), cluster, NULL);
        }
        else
            submitWsWorkunit(context, cw, cluster, req.getSnapshot(), req.getMaxRunTime(), true, false, false);

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

        if (runWuid && *runWuid)
        {
            if (!looksLikeAWuid(runWuid))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", runWuid);

            if (req.getCloneWorkunit())
                runWsWorkunit(context, wuid, runWuid, cluster, req.getInput(), &req.getVariables(), &req.getDebugValues());
            else
            {
                submitWsWorkunit(context, runWuid, cluster, NULL, 0, false, true, true, req.getInput(), &req.getVariables(), &req.getDebugValues());
                wuid.set(runWuid);
            }
        }
        else if (notEmpty(req.getQuerySet()) && notEmpty(req.getQuery()))
            runWsWuQuery(context, wuid, req.getQuerySet(), req.getQuery(), cluster, req.getInput());
        else
            throw MakeStringException(ECLWATCH_MISSING_PARAMS,"Workunit or Query required");

        int timeToWait = req.getWait();
        if (timeToWait != 0)
            waitForWorkUnitToComplete(wuid.str(), timeToWait);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
        if (!cw)
            throw MakeStringException(ECLWATCH_CANNOT_UPDATE_WORKUNIT,"Cannot open workunit %s.", wuid.str());

        SCMStringBuffer stateDesc;
        resp.setState(cw->getStateDesc(stateDesc).str());
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
                getFullWorkUnitResultsXML(context.queryUserId(), context.queryPassword(), cw.get(), result, flags);
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
        checkAndTrimWorkunit("WUWaitCompiled", wuid);
        secWaitForWorkUnitToCompile(wuid.str(), *context.querySecManager(), *context.queryUser(), req.getWait());
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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
        checkAndTrimWorkunit("WUWaitComplete", wuid);
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
        checkAndTrimWorkunit("WUCDebug", wuid);
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

        SCMStringBuffer wuid;
        wu->getWuid(wuid);
        wu->commit();
        wu.clear();

        submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);
        waitForWorkUnitToComplete(wuid.str(), req.getTimeToWait());

        Owned<IConstWorkUnit> cw(factory->openWorkUnit(wuid.str(), false));
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

        SCMStringBuffer wuid;
        wu->getWuid(wuid);
        wu.clear();

        submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);
        waitForWorkUnitToComplete(wuid.str(),req.getTimeToWait());

        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);

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

        SCMStringBuffer wuid;
        wu->getWuid(wuid);
        wu->commit();
        wu.clear();

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);
        submitWsWorkunit(context, wuid.str(), req.getCluster(), req.getSnapshot(), 0, true, false, false);

        int state = waitForWorkUnitToComplete(wuid.str(), timeMilliSec);
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);

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
    const char * scope = wpt->queryProp("@scope");
    if (notEmpty(scope))
        info->setScope(scope);
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

void getArchivedWUInfo(IEspContext &context, const char *wuid, IEspWUInfoResponse &resp)
{
    Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
    Owned<IConstEnvironment> constEnv = envFactory->openEnvironmentByFile();

    Owned<IPropertyTree> root = &constEnv->getPTree();
    if (!root)
        throw MakeStringExceptionDirect(ECLWATCH_CANNOT_GET_ENV_INFO, "Failed to get environment info");
    Owned<IPropertyTreeIterator> instances = root->getElements("Software/SashaServerProcess/Instance");

    ForEach(*instances)
    {
        IPropertyTree &instance = instances->query();
        SocketEndpoint ep(instance.queryProp("@netAddress"), instance.getPropInt("@port", 8877));
        if (getWsWuInfoFromSasha(context, ep, wuid, &resp.updateWorkunit()))
        {
            resp.setAutoRefresh(WUDETAILS_REFRESH_MINS);
            resp.setCanCompile(false);
            return;
        }
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
        checkAndTrimWorkunit("WUInfo", wuid);

        if (req.getType() && strieq(req.getType(), "archived workunits"))
            getArchivedWUInfo(context, wuid.str(), resp);
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

                WsWuInfo winfo(context, wuid.str());
                winfo.getInfo(resp.updateWorkunit(), flags);

                if (req.getIncludeResultsViewNames())
                {
                    StringArray views;
                    winfo.getResultViews(views, WUINFO_IncludeResultsViewNames);
                    resp.setResultViews(views);
                }
            }
            catch (IException *e)
            {
                if (e->errorCode() != ECLWATCH_CANNOT_OPEN_WORKUNIT)
                    throw e;
                getArchivedWUInfo(context, wuid.str(), resp);
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
            if (context.getClientVersion() > 1.24 && notEmpty(req.getThorSlaveIP()))
                resp.setThorSlaveIP(req.getThorSlaveIP());
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
    checkAndTrimWorkunit("WUResultView", wuid);

    ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

    Owned<IWuWebView> wv = createWuWebView(wuid.str(), NULL, getCFD(), true);
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
    Owned<IConstWorkUnit> cw= factory->openWorkUnit(wuid.str(), false);
    if (!cw)
        throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot find the workunit for file %s.", logicalFile);
    if (getWsWorkunitAccess(context, *cw) < SecAccess_Read)
        throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED,"Cannot access the workunit for file %s.",logicalFile);

    SCMStringBuffer parent;
    if (!cw->getParentWuid(parent).length())
        doWUQueryBySingleWuid(context, wuid.str(), resp);

    resp.setFirst(false);
    resp.setPageSize(1);
    resp.setCount(1);
}

void doWUQueryByXPath(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    IArrayOf<IEspECLWorkunit> results;

    WsWuSearch wlist(context,req.getOwner(),req.getState(),req.getCluster(),req.getStartDate(),req.getEndDate(),req.getECL(),req.getJobname(),req.getApplicationName(),req.getApplicationKey(),req.getApplicationData());

    int count=(int)req.getPageSize();
    if (!count)
        count=100;

    if (wlist.getSize() < 1)
    {
        resp.setNumWUs(0);
        return;
    }

    if (wlist.getSize() < count)
        count = (int) wlist.getSize() - 1;

    WsWuSearch::iterator begin, end;

    if(notEmpty(req.getAfter()))
    {
        begin=wlist.locate(req.getAfter());
        end=min(begin+count,wlist.end());
    }
    else if (notEmpty(req.getBefore()))
    {
        end=wlist.locate(req.getBefore());
        begin=max(end-count,wlist.begin());
    }
    else
    {
        begin=wlist.begin();
        end=min(begin+count,wlist.end());
    }

    if(begin>wlist.begin() && begin<wlist.end())
        resp.setCurrent(begin->c_str());

    if (context.getClientVersion() > 1.02)
    {
        resp.setPageStartFrom(begin - wlist.begin() + 1);
        resp.setNumWUs((int)wlist.getSize());
        resp.setCount(end - begin);
    }

    if(end<wlist.end())
        resp.setNext(end->c_str());

    for(;begin!=end;begin++)
    {
        Owned<IEspECLWorkunit> info = createECLWorkunit("","");
        WsWuInfo winfo(context, begin->c_str());
        winfo.getCommon(*info, 0);
        results.append(*info.getClear());
    }
    resp.setPageSize(abs(count));
    resp.setWorkunits(results);

    return;
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

    WUSortField sortorder[2] = {(WUSortField) (WUSFwuid | WUSFreverse), WUSFterm};
    if(notEmpty(req.getSortby()))
    {
        const char *sortby = req.getSortby();
        if (strieq(sortby, "Owner"))
            sortorder[0] = WUSFuser;
        else if (strieq(sortby, "JobName"))
            sortorder[0] = WUSFjob;
        else if (strieq(sortby, "Cluster"))
            sortorder[0] = WUSFcluster;
        else if (strieq(sortby, "RoxieCluster"))
            sortorder[0] = WUSFroxiecluster;
        else if (strieq(sortby, "Protected"))
            sortorder[0] = WUSFprotected;
        else if (strieq(sortby, "State"))
            sortorder[0] = WUSFstate;
        else if (strieq(sortby, "ThorTime"))
            sortorder[0] = (WUSortField) (WUSFtotalthortime+WUSFnumeric);
        else
            sortorder[0] = WUSFwuid;

        bool descending = req.getDescending();
        if (descending)
            sortorder[0] = (WUSortField) (sortorder[0] | WUSFreverse);
    }

    WUSortField filters[10];
    unsigned short filterCount = 0;
    MemoryBuffer filterbuf;

    bool bDoubleCheckState = false;
    if(req.getState())
    {
        addWUQueryFilter(filters, filterCount, filterbuf, strieq(req.getState(), "unknown") ? "" : req.getState(), WUSFstate);
        if (strieq(req.getState(), "submitted"))
            bDoubleCheckState = true;
    }

    addWUQueryFilter(filters, filterCount, filterbuf, req.getCluster(), WUSFcluster);
    if(version > 1.07)
        addWUQueryFilter(filters, filterCount, filterbuf, req.getRoxieCluster(), WUSFroxiecluster);
    addWUQueryFilter(filters, filterCount, filterbuf, req.getLogicalFile(), WUSFfileread);
    addWUQueryFilter(filters, filterCount, filterbuf, req.getOwner(), (WUSortField) (WUSFuser | WUSFnocase));
    addWUQueryFilter(filters, filterCount, filterbuf, req.getJobname(), (WUSortField) (WUSFjob | WUSFnocase));

    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getStartDate(), WUSFwuid);
    addWUQueryFilterTime(filters, filterCount, filterbuf, req.getEndDate(), WUSFwuidhigh);

    filters[filterCount] = WUSFterm;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    unsigned numWUs = factory->numWorkUnitsFiltered(filters, filterbuf.bufferBase());
    Owned<IConstWorkUnitIterator> it = factory->getWorkUnitsSorted(sortorder, filters, filterbuf.bufferBase(), begin, pagesize+1, "", NULL);

    unsigned actualCount = 0;
    ForEach(*it)
    {
        IConstWorkUnit& cw = it->query();
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

        SCMStringBuffer parent;
        if (!cw.getParentWuid(parent).length())
        {
            const char* wuid = cw.getWuid(parent).str();
            if (!looksLikeAWuid(wuid))
            {
                numWUs--;
                continue;
            }
            actualCount++;
            Owned<IEspECLWorkunit> info = createECLWorkunit("","");
            WsWuInfo winfo(context, wuid);
            winfo.getCommon(*info, 0);
            results.append(*info.getClear());
        }
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

void doWUQueryFromArchive(IEspContext &context, ArchivedWuCache &archivedWuCache, int cacheTime, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    SecAccessFlags accessOwn;
    SecAccessFlags accessOthers;
    getUserWuAccessFlags(context, accessOwn, accessOthers, true);

    __int64 pageSize = req.getPageSize();
    if(pageSize < 1)
        pageSize=100;
    __int64 displayStart = req.getPageStartFrom();
    __int64 displayEnd = displayStart + pageSize;
    unsigned dateLimit = 0;
    bool hasNextPage = true;

    SocketEndpoint ep;
    getSashaNode(ep);

    Owned<INode> sashaserver = createINode(ep);

    CDateTime wuTimeFrom, wuTimeTo;
    if(notEmpty(req.getEndDate()))
        wuTimeTo.setString(req.getEndDate(), NULL, true);
    else
        wuTimeTo.setNow();

    if(notEmpty(req.getStartDate()))
    {
        wuTimeFrom.setString(req.getStartDate(), NULL, true);
        dateLimit = 1;
    }

    IArrayOf<IEspECLWorkunit> results;

    StringBuffer filter;
    addToQueryString(filter, "cluster", req.getCluster(), ';');
    addToQueryString(filter, "owner", req.getOwner(), ';');
    addToQueryString(filter, "jobName", req.getJobname(), ';');
    addToQueryString(filter, "state", req.getState(), ';');
    StringBuffer s;
    if (!req.getLastNDays_isNull() && req.getLastNDays()>0)
        addToQueryString(filter, "LastNDays", s.clear().append(req.getLastNDays()).str(), ';');
    else
    {
        addToQueryString(filter, "wuTimeFrom", req.getStartDate(), ';');
        addToQueryString(filter, "wuTimeTo", req.getEndDate(), ';');
    }
    addToQueryString(filter, "displayStart", s.append(displayStart).str(), ';');
    addToQueryString(filter, "pageSize", s.clear().append(pageSize).str(), ';');

    Owned<ArchivedWuCacheElement> found = archivedWuCache.lookup(context, filter, "AddWhenAvailable", cacheTime);
    if (found)
    {
        hasNextPage = found->m_hasNextPage;
        if (found->m_results.length())
        {
            ForEachItemIn(ai, found->m_results)
            {
                Owned<IEspECLWorkunit> info= createECLWorkunit("","");
                info->copy(found->m_results.item(ai));
                results.append(*info.getClear());
            }
        }
    }
    else
    {
        IArrayOf<IEspECLWorkunit> resultList;

        CDateTime timeTo = wuTimeTo;
        __int64 totalWus = 0;
        bool complete = false;
        while (!complete)
        {
            CDateTime timeFrom = timeTo;
            timeFrom.adjustTime(-1439); //one day earlier
            if (dateLimit > 0 && wuTimeFrom > timeFrom)
                timeFrom = wuTimeFrom;

            unsigned year0, month0, day0, hour0, minute0, second0, nano0;
            timeFrom.getDate(year0, month0, day0, true);
            timeFrom.getTime(hour0, minute0, second0, nano0, true);
            VStringBuffer wuFrom("%4d%02d%02d%02d%02d", year0, month0, day0, hour0, minute0);

            unsigned year, month, day, hour, minute, second, nano;
            timeTo.getDate(year, month, day, true);
            timeTo.getTime(hour, minute, second, nano, true);
            VStringBuffer wuTo("%4d%02d%02d%02d%02d", year, month, day, hour, minute);

            __int64 begin = 0;
            unsigned limit = 1000;
            bool continueSashaLoop = true;
            while (continueSashaLoop)
            {
                Owned<ISashaCommand> cmd = createSashaCommand();

                cmd->setAction(SCA_LIST);
                cmd->setOnline(false);
                cmd->setArchived(true);
                cmd->setAfter(wuFrom.str());
                cmd->setBefore(wuTo.str());
                cmd->setStart((unsigned)begin);
                cmd->setLimit(limit);

                if (notEmpty(req.getCluster()))
                    cmd->setCluster(req.getCluster());
                if (notEmpty(req.getOwner()))
                    cmd->setOwner(req.getOwner());
                if (notEmpty(req.getJobname()))
                    cmd->setJobName(req.getJobname());
                if (notEmpty(req.getState()))
                    cmd->setState(req.getState());

                cmd->setOutputFormat("owner,jobname,cluster,state");

                if (!cmd->send(sashaserver))
                {
                    StringBuffer msg("Cannot connect to archive server at ");
                    sashaserver->endpoint().getUrlStr(msg);
                    throw MakeStringException(ECLWATCH_CANNOT_CONNECT_ARCHIVE_SERVER, "%s", msg.str());
                }

                unsigned actualCount = cmd->numIds();
                if (actualCount < 1)
                    break;

                totalWus += actualCount;

                if (actualCount < limit)
                    continueSashaLoop = false;

                for (unsigned ii=0; ii<actualCount; ii++)
                {
                    const char *csline = cmd->queryId(ii);
                    if (!csline)
                        continue;

                    StringArray wuidArray;
                    wuidArray.appendList(csline, ",");

                    if (chooseWuAccessFlagsByOwnership(context.queryUserId(), cmd->queryOwner(), accessOwn, accessOthers) < SecAccess_Read)
                        continue;

                    const char* wuid = wuidArray.item(0);
                    if (isEmpty(wuid))
                        continue;

                    __int64 addToPos = -1;
                    ForEachItemIn(ridx, resultList)
                    {
                        IEspECLWorkunit& w = resultList.item(ridx);
                        if (isEmpty(w.getWuid()))
                            continue;

                        if (strcmp(wuid, w.getWuid())>0)
                        {
                            addToPos = ridx;
                            break;
                        }
                    }

                    if (addToPos < 0 && (ridx > displayEnd))
                        continue;

                    Owned<IEspECLWorkunit> info= createECLWorkunit("","");
                    info->setWuid(wuid);
                    if (notEmpty(wuidArray.item(1)))
                          info->setOwner(wuidArray.item(1));
                    if (notEmpty(wuidArray.item(2)))
                        info->setJobname(wuidArray.item(2));
                    if (notEmpty(wuidArray.item(3)))
                          info->setCluster(wuidArray.item(3));
                    if (notEmpty(wuidArray.item(4)))
                          info->setState(wuidArray.item(4));

                    if (addToPos < 0)
                        resultList.append(*info.getClear());
                    else
                        resultList.add(*info.getClear(), (aindex_t) addToPos);
                    if (resultList.length() > displayEnd)
                        resultList.pop();
                }

                begin += limit;
            }

            timeTo.adjustTime(-1440);//one day earlier
            if (dateLimit > 0 && wuTimeFrom > timeTo) //we reach the date limit
            {
                if (totalWus <= displayEnd)
                    hasNextPage = false;
                complete = true;
            }
            else if ( resultList.length() >= displayEnd) //we have all we need
                complete = true;
        }

        if (displayEnd > resultList.length())
            displayEnd = resultList.length();

        for (aindex_t i = (aindex_t)displayStart; i < (aindex_t)displayEnd; i++)
        {
            Owned<IEspECLWorkunit> info = createECLWorkunit("","");
            info->copy(resultList.item(i));
            results.append(*info.getClear());
        }

        archivedWuCache.add(filter, "AddWhenAvailable", hasNextPage, results);
    }

    resp.setPageStartFrom(displayStart+1);
    resp.setPageEndAt(displayEnd);

    if(dateLimit < 1 || hasNextPage)
        resp.setNextPage(displayStart + pageSize);
    else
        resp.setNextPage(-1);

    if(displayStart > 0)
    {
        resp.setFirst(false);
        if (displayStart - pageSize > 0)
            resp.setPrevPage(displayStart - pageSize);
        else
            resp.setPrevPage(0);
    }

    resp.setPageSize(pageSize);
    resp.setWorkunits(results);
    resp.setType("archived only");
    return;
}

bool CWsWorkunitsEx::onWUQuery(IEspContext &context, IEspWUQueryRequest & req, IEspWUQueryResponse & resp)
{
    try
    {
        StringBuffer wuidStr = req.getWuid();
        const char* wuid = wuidStr.trim().str();

        if (req.getType() && strieq(req.getType(), "archived workunits"))
            doWUQueryFromArchive(context, *archivedWuCache, awusCacheMinutes, req, resp);
        else if(notEmpty(wuid))
        {
            if (!looksLikeAWuid(wuid))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuid);

            doWUQueryBySingleWuid(context, wuid, resp);
        }
        else if (notEmpty(req.getECL()) || notEmpty(req.getApplicationName()) || notEmpty(req.getApplicationKey()) || notEmpty(req.getApplicationData()))
            doWUQueryByXPath(context, req, resp);
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
        if (version > 1.07)
            addToQueryString(basicQuery, "RoxieCluster", req.getRoxieCluster());
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

void appendResultSet(MemoryBuffer& mb, INewResultSet* result, const char *name, __int64 start, unsigned& count, __int64& total, bool bin, bool xsd)
{
    if (!result)
        return;
    const IResultSetMetaData &meta = result->getMetaData();

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

        count = getResultXml(adaptor, result, name, (unsigned) start, count, (xsd) ? "myschema" : NULL);
    }
}

void getWsWuResult(IEspContext &context, const char* wuid, const char *name, const char *logical, unsigned index, __int64 start, unsigned& count, __int64& total, IStringVal& resname, bool bin, MemoryBuffer& mb, bool xsd=true)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
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
        SCMStringBuffer cluster;  //MORE is this wrong cluster?
        rs.setown(resultSetFactory->createNewFileResultSet(logicalName.str(), cw->getClusterName(cluster).str()));
    }
    else
        rs.setown(resultSetFactory->createNewResultSet(result, wuid));
    appendResultSet(mb, rs, name, start, count, total, bin, xsd);
}

void openSaveFile(IEspContext &context, int opt, const char* filename, const char* origMimeType, MemoryBuffer& buf, IEspWULogFileResponse &resp)
{
    if (opt < 1)
    {
        resp.setThefile(buf);
        resp.setThefile_mimetype(origMimeType);
    }
    else if (opt < 2)
    {
        StringBuffer headerStr("attachment;");
        if (filename && *filename)
            headerStr.appendf("filename=%s", filename);

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
            if (!looksLikeAWuid(wuidIn))
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
                openSaveFile(context, opt, "ArchiveQuery.xml", HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if (strieq(File_Cpp,req.getType()) && notEmpty(req.getName()))
            {
                winfo.getWorkunitCpp(req.getName(), req.getDescription(), req.getIPAddress(),mb, opt > 0);
                openSaveFile(context, opt, req.getName(), HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_DLL,req.getType()))
            {
                StringBuffer name;
                winfo.getWorkunitDll(name, mb);
                resp.setFileName(name.str());
                resp.setDaliServer(daliServers.get());
                openSaveFile(context, opt, req.getName(), HTTP_TYPE_OCTET_STREAM, mb, resp);
            }
            else if (strieq(File_Res,req.getType()))
            {
                winfo.getWorkunitResTxt(mb);
                openSaveFile(context, opt, "res.txt", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strncmp(req.getType(), File_ThorLog, 7) == 0)
            {
                winfo.getWorkunitThorLog(req.getName(), mb);
                openSaveFile(context, opt, "thormaster.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_ThorSlaveLog,req.getType()))
            {
                StringBuffer logDir;
                getConfigurationDirectory(directories, "log", "thor", req.getProcess(), logDir);

                winfo.getWorkunitThorSlaveLog(req.getClusterGroup(), req.getIPAddress(), req.getLogDate(), logDir.str(), req.getSlaveNumber(), mb, false);
                openSaveFile(context, opt, "ThorSlave.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_EclAgentLog,req.getType()))
            {
                winfo.getWorkunitEclAgentLog(req.getName(), mb);
                openSaveFile(context, opt, "eclagent.log", HTTP_TYPE_TEXT_PLAIN, mb, resp);
            }
            else if (strieq(File_XML,req.getType()) && notEmpty(req.getName()))
            {
                const char* name  = req.getName();
                const char* ptr = strrchr(name, '/');
                if (ptr)
                    ptr++;
                else
                    ptr = name;

                winfo.getWorkunitAssociatedXml(name, req.getIPAddress(), req.getPlainText(), req.getDescription(), opt > 0, mb);
                openSaveFile(context, opt, ptr, HTTP_TYPE_APPLICATION_XML, mb, resp);
            }
            else if (strieq(File_XML,req.getType()))
            {
                winfo.getWorkunitXml(req.getPlainText(), mb);
                resp.setThefile(mb);
                const char* plainText = req.getPlainText();
                if (plainText && (!stricmp(plainText, "yes")))
                    resp.setThefile_mimetype(HTTP_TYPE_TEXT_PLAIN);
                else
                    resp.setThefile_mimetype(HTTP_TYPE_APPLICATION_XML);
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
            if (!looksLikeAWuid(wuidIn))
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Invalid Workunit ID: %s", wuidIn);

            ensureWsWorkunitAccess(context, wuidIn, SecAccess_Read);
        }

        MemoryBuffer mb;
        __int64 total=0;
        __int64 start = req.getStart() > 0 ? req.getStart() : 0;
        unsigned count = req.getCount(), requested=count;
        SCMStringBuffer name;

        bool bin = (req.getFormat() && strieq(req.getFormat(),"raw"));
        if (notEmpty(wuidIn) && notEmpty(req.getResultName()))
            getWsWuResult(context, wuidIn, req.getResultName(), NULL, 0, start, count, total, name, bin, mb);
        else if (notEmpty(wuidIn) && (req.getSequence() >= 0))
            getWsWuResult(context, wuidIn, NULL, NULL, req.getSequence(), start, count, total, name, bin,mb);
        else if (notEmpty(req.getLogicalName()))
        {
            const char* logicalName = req.getLogicalName();
            StringBuffer wuid;
            getWuidFromLogicalFileName(context, logicalName, wuid);
            if (!wuid.length())
                throw MakeStringException(ECLWATCH_CANNOT_GET_WORKUNIT,"Cannot find the workunit for file %s.",logicalName);
            getWsWuResult(context, wuid.str(), NULL, logicalName, 0, start, count, total, name, bin, mb);
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
        checkAndTrimWorkunit("WUResultSummary", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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

void getFileResults(IEspContext &context, const char* logicalName, const char* cluster,__int64 start, unsigned& count,__int64& total,IStringVal& resname,bool bin, MemoryBuffer& buf, bool xsd)
{
    Owned<IResultSetFactory> resultSetFactory = getSecResultSetFactory(context.querySecManager(), context.queryUser(), context.queryUserId(), context.queryPassword());
    Owned<INewResultSet> result(resultSetFactory->createNewFileResultSet(logicalName, cluster));
    appendResultSet(buf, result, resname.str(), start, count, total, bin, xsd);
}

void getWorkunitCluster(IEspContext &context, const char* wuid, SCMStringBuffer& cluster, bool checkArchiveWUs)
{
    if (isEmpty(wuid))
        return;

    Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
    Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid, false);
    if (cw)
        cw->getClusterName(cluster);
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
            if (!looksLikeAWuid(wuid))
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

        VStringBuffer filter("start=%"I64F"d;count=%d", start, count);
        addToQueryString(filter, "clusterName", req.getCluster(), ';');
        addToQueryString(filter, "logicalName", req.getLogicalName(), ';');
        if (wuid && *wuid)
            addToQueryString(filter, "wuid", wuid, ';');
        addToQueryString(filter, "resultName", req.getResultName(), ';');
        filter.appendf(";seq=%d;", seq);
        if (inclXsd)
            filter.append("xsd;");

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
            if(logicalName && *logicalName)
            {
                StringBuffer lwuid;
                getWuidFromLogicalFileName(context, logicalName, lwuid);
                SCMStringBuffer cluster;
                if (lwuid.length())
                    getWorkunitCluster(context, lwuid.str(), cluster, true);
                if (cluster.length())
                {
                    getFileResults(context, logicalName, cluster.str(), start, count, total, name, false, mb, inclXsd);
                    resp.setLogicalName(logicalName);
                }
                else if (notEmpty(clusterName))
                {
                    getFileResults(context, logicalName, clusterName, start, count, total, name, false, mb, inclXsd);
                    resp.setLogicalName(logicalName);
                }
                else
                    throw MakeStringException(ECLWATCH_INVALID_INPUT,"Need valid target cluster to browse file %s.",logicalName);
            }
            else if (notEmpty(wuid) && notEmpty(resultName))
            {
                name.set(resultName);
                getWsWuResult(context, wuid, resultName, NULL, 0, start, count, total, name, false, mb, inclXsd);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            else
            {
                getWsWuResult(context, wuid, NULL, NULL, seq, start, count, total, name, false, mb, inclXsd);
                resp.setWuid(wuid);
                resp.setSequence(seq);
            }
            mb.append(0);

            if (requested > total)
                requested = (unsigned)total;

            dataCache->add(filter, mb.toByteArray(), name.str(), logicalName, wuid, resultName, seq, start, count, requested, total);
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

void getScheduledWUs(IEspContext &context, const char *serverName, const char *eventName, IArrayOf<IEspScheduledWU> & results)
{
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
                        Owned<IEspScheduledWU> scheduledWU = createScheduledWU("");
                        scheduledWU->setWuid(wuid.str());
                        scheduledWU->setCluster(serverName);
                        if (ieventName.length())
                            scheduledWU->setEventName(ieventName.str());
                        if (ieventText.str())
                            scheduledWU->setEventText(ieventText.str());

                        try
                        {
                            SCMStringBuffer s;
                            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
                            if (cw)
                                scheduledWU->setJobName(cw->getJobName(s).str());
                        }
                        catch (IException *e)
                        {
                            e->Release();
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

        IArrayOf<IEspScheduledWU> results;
        if(notEmpty(req.getPushEventName()))
            resp.setPushEventName(req.getPushEventName());
        if(notEmpty(req.getPushEventText()))
            resp.setPushEventText(req.getPushEventText());

        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
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
                getScheduledWUs(context, iclusterName, eventName, results);
            else if (strieq(clusterName, iclusterName))
            {
                getScheduledWUs(context, clusterName, eventName, results);
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
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        WsWuSearch ws(context, req.getOwner(), req.getState(), req.getCluster(), req.getStartDate(), req.getEndDate(), req.getECL(), req.getJobname());

        StringBuffer xml("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Workunits>");
        for(WsWuSearch::iterator it=ws.begin(); it!=ws.end(); it++)
        {
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(it->c_str(), false);
            if (cw)
                exportWorkUnitToXML(cw, xml, true);
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
        checkAndTrimWorkunit("WUListLocalFileRequired", wuid);

        ensureWsWorkunitAccess(context, wuid.str(), SecAccess_Read);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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
        checkAndTrimWorkunit("WUAddLocalFileToWorkunit", wuid);
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
    Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
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
        checkAndTrimWorkunit("WUProcessGraph", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        Owned <IConstWUGraph> graph = cw->getGraph(req.getName());
        Owned <IPropertyTree> xgmml = graph->getXGMMLTree(true); // merge in graph progress information

        StringBuffer xml;
        resp.setTheGraph(toXML(xgmml.get(), xml).str());
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

bool CWsWorkunitsEx::onWUGetGraph(IEspContext& context, IEspWUGetGraphRequest& req, IEspWUGetGraphResponse& resp)
{
    try
    {
        StringBuffer wuid = req.getWuid();
        checkAndTrimWorkunit("WUGetGraph", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
        if(!cw)
            throw MakeStringException(ECLWATCH_CANNOT_OPEN_WORKUNIT,"Cannot open workunit %s.",wuid.str());
        ensureWsWorkunitAccess(context, *cw, SecAccess_Read);

        WUGraphIDType id;
        SCMStringBuffer runningGraph;
        bool running= (isRunning(*cw) && cw->getRunningGraph(runningGraph,id));

        IArrayOf<IEspECLGraphEx> graphs;
        Owned<IConstWUGraphIterator> it = &cw->getGraphs(GraphTypeAny);
        ForEach(*it)
        {
            IConstWUGraph &graph = it->query();
            if(!graph.isValid())
                continue;

            SCMStringBuffer name, label, type;
            graph.getName(name);
            graph.getLabel(label);
            graph.getTypeName(type);

            if(isEmpty(req.getGraphName()) || strieq(name.str(), req.getGraphName()))
            {
                Owned<IEspECLGraphEx> g = createECLGraphEx("","");
                g->setName(name.str());
                g->setLabel(label.str());
                g->setType(type.str());
                if(running && streq(name.str(), runningGraph.str()))
                {
                    g->setRunning(true);
                    g->setRunningId(id);
                }

                Owned<IPropertyTree> xgmml = graph.getXGMMLTree(true);

                // New functionality, if a subgraph id is specified and we only want to load the xgmml for that subgraph
                // then we need to conditionally pull a propertytree from the xgmml graph one and use that for the xgmml.

                StringBuffer xml;
                if (notEmpty(req.getSubGraphId()))
                {
                    VStringBuffer xpath("//node[@id='%s']", req.getSubGraphId());
                    toXML(xgmml->queryPropTree(xpath.str()), xml);
                }
                else
                    toXML(xgmml, xml);

                g->setGraph(xml.str());

                if (context.getClientVersion() > 1.20)
                {
                    Owned<IConstWUGraphProgress> progress = cw->getGraphProgress(name.str());
                    if (progress)
                    {
                        WUGraphState graphstate= progress->queryGraphState();
                        if (graphstate == WUGraphComplete)
                            g->setComplete(true);
                        else if (graphstate == WUGraphFailed)
                            g->setFailed(true);
                    }
                }
                graphs.append(*g.getClear());
            }
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
        checkAndTrimWorkunit("WUGraphInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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
        checkAndTrimWorkunit("WUGVCGraphInfo", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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
        checkAndTrimWorkunit("WUGraphTiming", wuid);

        Owned<IWorkUnitFactory> factory = getWorkUnitFactory(context.querySecManager(), context.queryUser());
        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid.str(), false);
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
            Owned<IConstEnvironment> environment = factory->openEnvironmentByFile();
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
                StringBuffer cluster;
                request->getParameter("Cluster", cluster);
                StringBuffer range;
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
                        xml.append("<Cluster").append('>').append(thorInstances.item(i)).append("</Cluster>");
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


void deployEclOrArchive(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    NewWsWorkunit wu(context);

    SCMStringBuffer wuid;
    wu->getWuid(wuid);

    wu->setAction(WUActionCompile);

    StringBuffer name(req.getName());
    if (!name.trim().length() && notEmpty(req.getFileName()))
        splitFilename(req.getFileName(), NULL, NULL, &name, NULL);
    if (name.length())
        wu->setJobName(name.str());

    if (req.getObject().length())
    {
        StringBuffer text(req.getObject().length(), req.getObject().toByteArray());
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

    submitWsWorkunit(context, wuid.str(), req.getCluster(), NULL, 0, true, false, false, NULL, NULL, &req.getDebugValues());
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

void writeSharedObject(const char *srcpath, const MemoryBuffer &obj, const char *dir, StringBuffer &dllpath, StringBuffer &dllname)
{
    StringBuffer name, ext;
    if (srcpath && *srcpath)
        splitFilename(srcpath, NULL, NULL, &name, &ext);

    unsigned copy=0;
    buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), copy);
    while (checkFileExists(dllpath.str()))
        buildFullDllPath(dllpath.clear(), dllname.clear(), dir, name.str(), ext.str(), ++copy);

    DBGLOG("Writing workunit dll: %s", dllpath.str());
    Owned<IFile> f = createIFile(dllpath.str());
    Owned<IFileIO> io = f->open(IFOcreate);
    io->write(0, obj.length(), obj.toByteArray());
}

void CWsWorkunitsEx::deploySharedObject(IEspContext &context, StringBuffer &wuid, const char *filename, const char *cluster, const char *name, const MemoryBuffer &obj, const char *dir, const char *xml)
{
    StringBuffer dllpath, dllname;
    StringBuffer srcname(filename);
    if (!srcname.length())
        srcname.append(name).append(SharedObjectExtension);
    writeSharedObject(srcname.str(), obj, dir, dllpath, dllname);

    NewWsWorkunit wu(context);

    StringBufferAdaptor isvWuid(wuid);
    wu->getWuid(isvWuid);
    wu->setClusterName(cluster);
    wu->commit();

    StringBuffer dllXML;
    if (getWorkunitXMLFromFile(dllpath.str(), dllXML))
    {
        Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit();
        embeddedWU->loadXML(dllXML.str());
        queryExtendedWU(wu)->copyWorkUnit(embeddedWU, true);
    }

    wu.associateDll(dllpath.str(), dllname.str());

    if (name && *name)
        wu->setJobName(name);

    //clean slate, copy only select items from processed workunit xml
    if (xml && *xml)
    {
        Owned<IPropertyTree> srcxml = createPTreeFromXMLString(xml);
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

void CWsWorkunitsEx::deploySharedObject(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp, const char *dir, const char *xml)
{
    if (isEmpty(req.getFileName()))
       throw MakeStringException(ECLWATCH_INVALID_INPUT, "File name required when deploying a shared object.");

    const char *cluster = req.getCluster();
    if (isEmpty(cluster))
       throw MakeStringException(ECLWATCH_INVALID_INPUT, "Cluster name required when deploying a shared object.");

    StringBuffer wuid;
    deploySharedObject(context, wuid, req.getFileName(), cluster, req.getName(), req.getObject(), dir, xml);

    WsWuInfo winfo(context, wuid.str());
    winfo.getCommon(resp.updateWorkunit(), WUINFO_All);

    AuditSystemAccess(context.queryUserId(), true, "Updated %s", wuid.str());
}

bool CWsWorkunitsEx::onWUDeployWorkunit(IEspContext &context, IEspWUDeployWorkunitRequest & req, IEspWUDeployWorkunitResponse & resp)
{
    const char *type = req.getObjType();

    try
    {
        if (!context.validateFeatureAccess(OWN_WU_ACCESS, SecAccess_Write, false))
            throw MakeStringException(ECLWATCH_ECL_WU_ACCESS_DENIED, "Failed to create workunit. Permission denied.");

        if (notEmpty(req.getCluster()) && !isValidCluster(req.getCluster()))
            throw MakeStringException(ECLWATCH_INVALID_CLUSTER_NAME, "Invalid cluster name: %s", req.getCluster());
        if (strieq(type, "archive")|| strieq(type, "ecl_text"))
            deployEclOrArchive(context, req, resp);
        else if (strieq(type, "shared_object"))
            deploySharedObject(context, req, resp, queryDirectory.str());
        else
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "WUDeployWorkunit '%s' unkown object type.", type);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
