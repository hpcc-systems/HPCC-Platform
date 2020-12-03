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

#pragma warning( disable : 4786 )

#ifdef _WIN32
//#define ESP_SUPPORT_DALI_CONFIG
//CRT
#include <process.h>
#endif

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"

#include "espplugin.hpp"
#include "espplugin.ipp"
#include "espcfg.ipp"
#include "xslprocessor.hpp"
#include "mplog.hpp"
#include "rmtfile.hpp"
#include "dafdesc.hpp"

//#include <dalienv.hpp>

/*
#if defined(USING_MPATROL)
#define ESP_BUILTIN
#endif
*/

//#define ESP_BUILTIN

extern "C" {
    ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process);
    ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process);
    ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process);
};

#ifdef ESP_BUILTIN
builtin espdirect;
#endif

// add suffix and prefix when necessary
void fixPlugin(StringBuffer& plugin)
{
    if (stricmp(plugin.str()+plugin.length()-sizeof(SharedObjectExtension)+1,SharedObjectExtension)==0)
        return;
    plugin.insert(0,SharedObjectPrefix);
    plugin.append(SharedObjectExtension);
}

void CEspConfig::loadBuiltIns()
{
#ifdef ESP_BUILTIN
    espdirect.prot = esp_protocol_factory;
    espdirect.bind = esp_binding_factory;
   espdirect.serv = esp_service_factory;
#endif
}

builtin *CEspConfig::getBuiltIn(string name)
{
#ifdef ESP_BUILTIN
    //if (name.compare("pixall.dll")==0 || name.compare("pixall.so")==0)
    return &espdirect;
#else //ESP_DIRECT
    return NULL;
#endif
}


StringBuffer &CVSBuildToEspVersion(char const * tag, StringBuffer & out)
{
    unsigned build = 0;
    unsigned subbuild = 0;
    while(!isdigit(*tag))
    {
        if(!*tag) break;
        tag++;
    }
    while(isdigit(*tag))
    {
        if(!*tag) break;
        build = 10*build + (*tag-'0');
        tag++;
    }
    if(isalpha(*tag))
    {
        if(islower(*tag))
            subbuild = *tag-'a'+1;
        else
            subbuild = *tag-'A'+1;
    }
        
    out.append(build/10).append('.').append(build%10).append(subbuild);
    return out;
}

int CSessionCleaner::run()
{
    try
    {
        PROGLOG("CSessionCleaner Thread started.");

        VStringBuffer xpath("%s*", PathSessionSession);
        int checkSessionTimeoutMillSeconds = checkSessionTimeoutSeconds * 1000;
        while(!stopping)
        {
            if (!m_isDetached)
            {
                Owned<IRemoteConnection> conn = getSDSConnectionWithRetry(espSessionSDSPath.get(), RTM_LOCK_WRITE, SDSSESSION_CONNECT_TIMEOUTMS);
                if (!conn)
                    throw MakeStringException(-1, "Failed to connect to %s.", PathSessionRoot);

                CDateTime now;
                now.setNow();
                time_t timeNow = now.getSimple();

                Owned<IPropertyTreeIterator> iter1 = conn->queryRoot()->getElements(PathSessionApplication);
                ForEach(*iter1)
                {
                    ICopyArrayOf<IPropertyTree> toRemove;
                    Owned<IPropertyTreeIterator> iter2 = iter1->query().getElements(xpath.str());
                    ForEach(*iter2)
                    {
                        IPropertyTree& item = iter2->query();
                        if (timeNow >= item.getPropInt64(PropSessionTimeoutAt, 0))
                            toRemove.append(item);
                    }

                    ForEachItemIn(i, toRemove)
                    {
                        iter1->query().removeTree(&toRemove.item(i));
                    }
                }
            }
            sem.wait(checkSessionTimeoutMillSeconds);
        }
    }
    catch(IException *e)
    {
        StringBuffer msg;
        IERRLOG("CSessionCleaner::run() Exception %d:%s", e->errorCode(), e->errorMessage(msg).str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown CSessionCleaner::run() Exception");
    }
    return 0;
}

void CSessionCleaner::stop()
{
    stopping = true;
    sem.signal();
    join();
}

void CEspConfig::readSessionDomainsSetting()
{
    bool hasAuthDomainSettings = false;
    bool hasSessionAuth = false;
    bool hasDefaultSessionDomain = false;
    Owned<IPropertyTree> proc_cfg = getProcessConfig(m_envpt, m_process.str());
    Owned<IPropertyTreeIterator> it = proc_cfg->getElements("AuthDomains/AuthDomain");
    ForEach(*it)
    {
        hasAuthDomainSettings = true;
        IPropertyTree& authDomain = it->query();
        const char* authType = authDomain.queryProp("@authType");
        if (isEmptyString(authType) || (!strieq(authType, "AuthPerSessionOnly") && !strieq(authType, "AuthTypeMixed")))
            continue;

        hasSessionAuth = true;

        int clientSessionTimeoutSeconds;
        int clientSessionTimeoutMinutes = authDomain.getPropInt("@clientSessionTimeoutMinutes", ESP_SESSION_TIMEOUT);
        if (clientSessionTimeoutMinutes < 0)
            clientSessionTimeoutSeconds = ESP_SESSION_NEVER_TIMEOUT;
        else
            clientSessionTimeoutSeconds = clientSessionTimeoutMinutes * 60;

        //The serverSessionTimeoutMinutes is used to clean the sessions by ESP server after the sessions have been timed out on ESP clients.
        //Considering possible network delay, serverSessionTimeoutMinutes should be greater than clientSessionTimeoutMinutes.
        int serverSessionTimeoutMinutes = authDomain.getPropInt("@serverSessionTimeoutMinutes", 0);
        if ((serverSessionTimeoutMinutes < 0) || (clientSessionTimeoutMinutes < 0))
            serverSessionTimeoutSeconds = ESP_SESSION_NEVER_TIMEOUT;
        else
            serverSessionTimeoutSeconds = serverSessionTimeoutMinutes * 60;
        if (serverSessionTimeoutSeconds < clientSessionTimeoutSeconds)
            serverSessionTimeoutSeconds = 2 * clientSessionTimeoutSeconds;

        const char* authDomainName = authDomain.queryProp("@domainName");
        if (isEmptyString(authDomainName) || strieq(authDomainName, "default"))
        {
            if (hasDefaultSessionDomain)
                throw MakeStringException(-1, ">1 AuthDomains are not named.");

            hasDefaultSessionDomain = true;
        }
    }
    //Ensure SDS Session tree if there is session auth or there is no AuthDomain setting (ex. old environment.xml)
    if (hasSessionAuth || !hasAuthDomainSettings)
        sdsSessionNeeded = true;
}

void CEspConfig::ensureSDSSession()
{
    Owned<IRemoteConnection> conn = getSDSConnectionWithRetry(PathSessionRoot, RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDSSESSION_CONNECT_TIMEOUTMS);
    if (!conn)
        throw makeStringExceptionV(-1, "Failed to connect to %s.", PathSessionRoot);

    IPropertyTree* sessionRoot = conn->queryRoot();
    VStringBuffer xpath("%s[@name=\"%s\"]", PathSessionProcess, m_process.str());
    IPropertyTree* processSessionTree = sessionRoot->queryBranch(xpath);
    if (!processSessionTree)
    {
        processSessionTree = sessionRoot->addPropTree(PathSessionProcess);
        processSessionTree->setProp("@name", m_process);
    }

    ensureSDSSessionApplications(processSessionTree);
    sdsSessionEnsured = true;

    if (serverSessionTimeoutSeconds != ESP_SESSION_NEVER_TIMEOUT)
    {
        VStringBuffer espSessionSDSPath("%s/%s[@name=\"%s\"]", PathSessionRoot, PathSessionProcess, m_process.str());
        Owned<IPropertyTree> proc_cfg = getProcessConfig(m_envpt, m_process);
        m_sessionCleaner.setown(new CSessionCleaner(espSessionSDSPath.str(), proc_cfg->getPropInt("@checkSessionTimeoutSeconds",
            ESP_CHECK_SESSION_TIMEOUT)));
        m_sessionCleaner->start();
    }
}

void CEspConfig::ensureSDSSessionApplications(IPropertyTree* espSession)
{
    std::list<int> bindingPorts;

    for (const auto pbfg: m_bindings)
    {
        auto it = std::find(bindingPorts.begin(), bindingPorts.end(), pbfg->port);
        if (it == bindingPorts.end())
            bindingPorts.push_back(pbfg->port);
    }

    if (bindingPorts.empty())
        throw makeStringException(-1, "No binding port.");

    for (const auto port: bindingPorts)
    {
        VStringBuffer appStr("%s[@port=\"%d\"]", PathSessionApplication, port);
        IPropertyTree* appSessionTree = espSession->queryBranch(appStr.str());
        if (!appSessionTree)
        {
            IPropertyTree* newAppSessionTree = espSession->addPropTree(PathSessionApplication);
            newAppSessionTree->setPropInt("@port", port);
        }
    }
}

CEspConfig::CEspConfig(IProperties* inputs, IPropertyTree* envpt, IPropertyTree* procpt, bool isDali)
{
    hsami_=0;
    serverstatus=NULL;
    useDali=false;
    if(inputs)
        m_inputs.setown(inputs);
    if(!envpt || !procpt)
        return;

    m_envpt.setown(envpt);
    m_cfg.setown(procpt);

    loadBuiltIns();   

    // load options
    const char* level = m_cfg->queryProp("@logLevel");
    m_options.logLevel = level ? atoi(level) : LogMin;
    m_options.logReq = readLogRequest(m_cfg->queryProp("@logRequests"));
    m_options.logResp = m_cfg->getPropBool("@logResponses", false);
    m_options.txSummaryLevel = m_cfg->getPropInt("@txSummaryLevel", LogMin);
    m_options.txSummaryResourceReq = m_cfg->getPropBool("@txSummaryResourceReq", false);
    m_options.frameTitle.set(m_cfg->queryProp("@name"));
    m_options.slowProcessingTime = m_cfg->getPropInt("@slowProcessingTime", 30) * 1000; //in msec

    if (!m_cfg->getProp("@name", m_process))
    {
        OERRLOG("EspProcess name not found");
    }
    else
    {
        DBGLOG("ESP process name [%s]", m_process.str());

        setIsDetachedFromDali(false);
        setIsSubscribedToDali(true);
        try
        {
            StringBuffer procDirectory;
            m_cfg->getProp("@directory", procDirectory);
            if (!procDirectory.isEmpty())
            {
                m_daliAttachStateFileName.setf("%s%c%s-AttachState.xml",procDirectory.str(), PATHSEPCHAR, m_process.str());

                try
                {
                    Owned<IPTree> espProcAttachState = createPTreeFromXMLFile(m_daliAttachStateFileName);
                    if (espProcAttachState)
                    {
                        setIsDetachedFromDali(!(espProcAttachState->getPropBool("@attached", true)));
                        setIsSubscribedToDali(espProcAttachState->getPropBool("@subscribed", true));
                    }
                    else
                    {
                        ESPLOG(LogMin, "Could not load DALI Attach state file [%s] for ESP process [%s]", m_daliAttachStateFileName.str(), m_process.str());
                    }
                }
                catch (...)
                {
                    ESPLOG(LogMin, "Could not load DALI Attach state file [%s] for ESP process [%s]", m_daliAttachStateFileName.str(), m_process.str());
                }

                saveAttachState();
            }
            else
                ESPLOG(LogMin, "ESP Process [%s] configuration is missing '@directory' attribute, could not read AttachState", m_process.str());
        }
        catch (IException* e)
        {
            e->Release();
            ESPLOG(LogMin, "Could not load DALI Attach state file [%s] for ESP process [%s]", m_daliAttachStateFileName.str(), m_process.str());
        }
        catch (...)
        {
           ESPLOG(LogMin, "Could not load DALI Attach state file [%s] for ESP process [%s]", m_daliAttachStateFileName.str(), m_process.str());
        }

        if (isDetachedFromDali())
            OWARNLOG("ESP Process [%s] loading in DALI DETACHED state - Some ESP services do not load in detached state!",  m_process.str());

        StringBuffer daliservers;
        if (m_cfg->getProp("@daliServers", daliservers))
            initDali(daliservers.str()); //won't init if detached

        initializeStorageGroups(daliClientActive());

        const unsigned dafilesrvConnectTimeout = m_cfg->getPropInt("@dafilesrvConnectTimeout", 10)*1000;
        const unsigned dafilesrvReadTimeout = m_cfg->getPropInt("@dafilesrvReadTimeout", 10)*1000;
        setRemoteFileTimeouts(dafilesrvConnectTimeout, dafilesrvReadTimeout);
#ifndef _CONTAINERIZED
#ifndef _DEBUG
        startPerformanceMonitor(m_cfg->getPropInt("@perfReportDelay", 60)*1000);
#endif
#endif

        IPropertyTreeIterator *pt_iter = NULL;
        StringBuffer xpath;

        if (m_inputs->hasProp("SingleUserPass"))
        {
            StringBuffer plainesppass;
            StringBuffer encesppass;
            m_inputs->getProp("SingleUserPass", plainesppass);
            encrypt(encesppass, plainesppass.str());
            xpath.setf("SecurityManagers/SecurityManager[@type=\"SingleUserSecurityManager\"]/SingleUserSecurityManager/");
            pt_iter = m_cfg->getElements(xpath.str());
            if (pt_iter!=NULL)
            {
                IPropertyTree *ptree = NULL;
                pt_iter->first();
                while(pt_iter->isValid())
                {
                    ptree = &pt_iter->query();
                    if (ptree)
                    {
                        ptree->setProp("@SingleUserPass",  encesppass.str());

                        if (m_inputs->hasProp("SingleUserName"))
                        {
                            StringBuffer espusername;
                            m_inputs->getProp("SingleUserName", espusername);
                            ptree->setProp("@SingleUserName",  espusername.str());
                        }
                    }
                    pt_iter->next();
                }
                pt_iter->Release();
                pt_iter=NULL;
            }
        }

        //get the local computer name:
        m_cfg->getProp("@computer", m_computer);

        //get the local computer information:
        xpath.setf("Hardware/Computer[@name=\"%s\"]", m_computer.str());

        IPropertyTree *computer = m_envpt->queryPropTree(xpath.str());
        if (computer)
        {
            StringBuffer address;
            computer->getProp("@netAddress", address);

            int port = m_cfg->getPropInt("@port", 1500);
            if(strcmp(address.str(), ".") == 0)
            {
                GetHostName(address.clear());
            }
            m_address.set(address.str(), (unsigned short) port);
        }

        xpath.clear();
        xpath.append("EspService");

        pt_iter = m_cfg->getElements(xpath.str());

        if (pt_iter!=NULL)
        {
            IPropertyTree *ptree = NULL;

            pt_iter->first();

            while(pt_iter->isValid())
            {
                ptree = &pt_iter->query();
                if (ptree)
                {
                    srv_cfg *svcfg = new srv_cfg;

                    ptree->getProp("@name", svcfg->name);
                    ptree->getProp("@type", svcfg->type);
                    ptree->getProp("@plugin", svcfg->plugin);
                    fixPlugin(svcfg->plugin);

                    map<string, srv_cfg*>::value_type en(svcfg->name.str(), svcfg);
                    m_services.insert(en);
                }               
                pt_iter->next();
            }
    
            pt_iter->Release();
            pt_iter=NULL;
        }

        xpath.clear();
        xpath.append("EspProtocol");

        pt_iter = m_cfg->getElements(xpath.str());

        if (pt_iter!=NULL)
        {
            IPropertyTree *ptree = NULL;

            pt_iter->first();


            while(pt_iter->isValid())
            {
                ptree = &pt_iter->query();
                if (ptree)
                {
                    protocol_cfg *pcfg = new protocol_cfg;

                    ptree->getProp("@name", pcfg->name);
                    ptree->getProp("@plugin", pcfg->plugin);
                    fixPlugin(pcfg->plugin);
                    ptree->getProp("@type", pcfg->type);

                    map<string, protocol_cfg*>::value_type en(pcfg->name.str(), pcfg);
                    m_protocols.insert(en);
                }               

                pt_iter->next();
            }
    
            pt_iter->Release();
            pt_iter=NULL;
        }

        xpath.clear();
        xpath.append("EspBinding");
 
        pt_iter = m_cfg->getElements(xpath.str());

        if (pt_iter!=NULL)
        {
            IPropertyTree *ptree = NULL;

            pt_iter->first();
    

            while(pt_iter->isValid())
            {
                ptree = &pt_iter->query();
                if (ptree)
                {
                    OwnedPtr<binding_cfg> bcfg(new binding_cfg);
                    ptree->getProp("@name", bcfg->name);
                    bcfg->port = ptree->getPropInt("@port", 0);
                    if (bcfg->port == 0)
                        DBGLOG("Binding %s is configured with port 0, it will not be loaded.", bcfg->name.str());
                    else
                    {
                        ptree->getProp("@type", bcfg->type);
                        if (!streq(bcfg->type.str(), "EsdlBinding"))
                        {
                            ptree->getProp("@plugin", bcfg->plugin);
                            fixPlugin(bcfg->plugin);
                            bcfg->isDefault = ptree->getPropBool("@defaultBinding", false);

                            StringBuffer addr;
                            ptree->getProp("@netAddress", addr);
                            if (strcmp(addr.str(), ".") == 0)
                            {
                                // Here we interpret '.' as binding to all interfaces, so convert it to "0.0.0.0"
                                bcfg->address.append("0.0.0.0");
                            }
                            else
                            {
                                bcfg->address.append(addr.str());
                            }

                            ptree->getProp("@service", bcfg->service_name);
                            ptree->getProp("@protocol", bcfg->protocol_name);

                            m_bindings.push_back(bcfg.getClear());
                        }
                    }
                }

                pt_iter->next();
            }
    
            pt_iter->Release();
            pt_iter=NULL;
        }

        readSessionDomainsSetting();
        if (sdsSessionNeeded && !daliservers.isEmpty() && !isDetachedFromDali() && !m_bindings.empty())
        {
            ensureSDSSession();
        }
    }
}

void CEspConfig::sendAlert(int severity, char const * descr, char const * subject) const
{
}

void CEspConfig::initDali(const char *servers)
{
    CriticalBlock b(attachcrit);
    if (servers!=nullptr && *servers!=0 && !daliClientActive() && !isDetachedFromDali())
    {
        DBGLOG("Initializing DALI client [servers = %s]", servers);

        useDali=true;

        // Create server group
        Owned<IGroup> serverGroup = createIGroup(servers, DALI_SERVER_PORT);

        if (!serverGroup)
            throw MakeStringException(0, "Could not instantiate dali IGroup");

        // Initialize client process
        if (!initClientProcess(serverGroup, DCR_EspServer))
            throw MakeStringException(0, "Could not initialize dali client");

        serverstatus = new CSDSServerStatus("ESPserver");
        //When esp is starting, the initDali() is called before m_bindings is set.
        //We should not call ensureSDSSession() at that time.
        if (sdsSessionNeeded && !sdsSessionEnsured && !m_bindings.empty())
            ensureSDSSession();

        // for auditing
        startLogMsgParentReceiver();
        connectLogMsgManagerToDali();
    }
}


void CEspConfig::initPtree(const char *location, bool isDali)
{
    IPropertyTree* cfg = createPTreeFromXMLFile(location, ipt_caseInsensitive);
    if (cfg)
    {
        cfg->addProp("@config", location);
        m_envpt.setown(cfg);
    }
}



void CEspConfig::loadBinding(binding_cfg &xcfg)
{
    map<string, srv_cfg*>::iterator sit = m_services.find(xcfg.service_name.str());
    map<string, protocol_cfg*>::iterator pit = m_protocols.find(xcfg.protocol_name.str());
    
    IEspService *isrv = NULL;
    IEspProtocol *iprot = NULL;

    if(sit == m_services.end())
    {
        OWARNLOG("Warning: Service %s not found for binding %s", xcfg.service_name.str(), xcfg.name.str());
    }
    else
    {
        isrv = (*sit).second->srv;
    }

    if(pit == m_protocols.end())
    {
        throw MakeStringException(-1, "Protocol %s not found for binding %s", xcfg.protocol_name.str(), xcfg.name.str());
    }
    else
    {
        iprot = (*pit).second->prot;

        if (iprot)
        {
            esp_binding_factory_t xproc = NULL;
            if(isrv != NULL)
                xcfg.service.setown(LINK(isrv));
            xcfg.protocol.setown(LINK(iprot));
            
            builtin *pdirect = getBuiltIn(xcfg.plugin.str());
            if (pdirect)
            {
                xproc = pdirect->bind;
            }
            else
            {
                Owned<IEspPlugin> pplg = getPlugin(xcfg.plugin.str());
                if (pplg)
                {
                    xproc = (esp_binding_factory_t) pplg->getProcAddress("esp_binding_factory");
                }
            }

            if (xproc)
            {
                IEspRpcBinding* bind = xproc(xcfg.name.str(), xcfg.type.str(), m_envpt.get(), m_process.str());
                if (bind)
                    LOG(MCoperatorInfo, unknownJob,"Load binding %s (type: %s, process: %s) succeeded", xcfg.name.str(), xcfg.type.str(), m_process.str());
                else
                    OERRLOG("Failed to load binding %s (type: %s, process: %s)", xcfg.name.str(), xcfg.type.str(), m_process.str());
                xcfg.bind.setown(bind);
                if (serverstatus)
                {
                    IPropertyTree *stTree= serverstatus->queryProperties()->addPropTree("ESPservice", createPTree("ESPservice", ipt_caseInsensitive));
                    if (stTree)
                    {
                        stTree->setProp("@type", xcfg.service->getServiceType());
                        stTree->setProp("@name", xcfg.service_name.str());
                        stTree->setPropInt("@port", xcfg.port);
                    }
                    serverstatus->commitProperties();
                }

            }
            else
                throw MakeStringException(-1, "procedure esp_binding_factory can't be loaded");
        }
        else
        {
            throw MakeStringException(-1, "Protocol %s wasn't loaded correctly for the binding", xcfg.protocol_name.str());
        }   
   }
}

void CEspConfig::loadProtocol(protocol_cfg &xcfg)
{
    esp_protocol_factory_t xproc = NULL;
    builtin *pdirect = getBuiltIn(xcfg.plugin.str());
    if (pdirect)
        xproc = pdirect->prot;
    else
    {
        Owned<IEspPlugin> pplg = getPlugin(xcfg.plugin.str());
        if (pplg)
        {
            xproc = (esp_protocol_factory_t) pplg->getProcAddress("esp_protocol_factory");
        }
    }

    if (xproc)
    {
        xcfg.prot.setown(xproc(xcfg.name.str(), xcfg.type.str(), m_envpt.get(), m_process.str()));
        if (xcfg.prot)
            xcfg.prot->init(m_envpt.get(), m_process.str(), xcfg.name.str());
    }
    else
        throw MakeStringException(-1, "procedure esp_protocol_factory can't be loaded");
}

void CEspConfig::loadService(srv_cfg &xcfg)
{
    esp_service_factory_t xproc = NULL;
    builtin *pdirect = getBuiltIn(xcfg.plugin.str());
    if (pdirect)
        xproc = pdirect->serv;
    else
    {
        Owned<IEspPlugin> pplg = getPlugin(xcfg.plugin.str());
        if (pplg)
            xproc = (esp_service_factory_t) pplg->getProcAddress("esp_service_factory");
    }

    if (xproc)
        xcfg.srv.setown(xproc(xcfg.name.str(), xcfg.type.str(), m_envpt.get(), m_process.str()));
    else
        throw MakeStringException(-1, "procedure esp_service_factory can't be loaded from %s", xcfg.plugin.str());
}

void CEspConfig::loadServices()
{
    map<string, srv_cfg*>::iterator iter = m_services.begin();
    while (iter!=m_services.end())
    {
#ifndef _USE_OPENLDAP
        const string svcName = iter->first;
        if (!strstr(svcName.data(), "ws_access"))
#endif
            loadService(*(iter->second));
#ifndef _USE_OPENLDAP
        else
            DBGLOG("Not loading service %s, platform built without LDAP", svcName.data());
#endif
        iter++;
    }
}

void CEspConfig::loadProtocols()
{
    map<string, protocol_cfg*>::iterator iter = m_protocols.begin();
    while (iter!=m_protocols.end())
    {
        loadProtocol(*(iter->second));
        iter++;
    }
}

void CEspConfig::loadBindings()
{
    list<binding_cfg*>::iterator iter = m_bindings.begin();
    
    while (iter!=m_bindings.end())
    {
#ifndef _USE_OPENLDAP
        const char * bindingName = (**iter).name.str();
        if (!strstr(bindingName, "ws_access"))
#endif
            loadBinding(**iter);
#ifndef _USE_OPENLDAP
        else
            DBGLOG("Not binding %s, platform built without LDAP", bindingName);
#endif
        iter++;
    }
}

void CEspConfig::startEsdlMonitor()
{
    start_esdl_monitor_t xproc = nullptr;
    Owned<IEspPlugin> pplg = getPlugin("esdl_svc_engine");
    if (pplg)
    {
        DBGLOG("Plugin esdl_svc_engine loaded.");
        xproc = (start_esdl_monitor_t) pplg->getProcAddress("startEsdlMonitor");
    }
    else
        throw MakeStringException(-1, "Plugin esdl_svc_engine can't be loaded");

    if (xproc)
    {
        DBGLOG("Procedure startEsdlMonitor loaded, now calling it...");
        xproc();
    }
    else
        throw MakeStringException(-1, "procedure startEsdlMonitor can't be loaded");
}

void CEspConfig::stopEsdlMonitor()
{
    stop_esdl_monitor_t xproc = nullptr;
    Owned<IEspPlugin> pplg = getPlugin("esdl_svc_engine");
    if (pplg)
        xproc = (stop_esdl_monitor_t) pplg->getProcAddress("stopEsdlMonitor");
    if (xproc)
        xproc();
}

class ESPxsltIncludeHandler : public CInterface, implements IIncludeHandler
{
public:
    //  IMPLEMENT_IINTERFACE;
    virtual void Link() const
    {
        CInterface::Link();
    }
    virtual bool Release() const
    {
        return CInterface::Release();
    }
    
    ESPxsltIncludeHandler()
    {
    }
    ~ESPxsltIncludeHandler()
    {
    }
    
    
    inline bool fileExists(StringBuffer &filename)
    {
        return (checkFileExists(filename.str()) || checkFileExists(filename.toUpperCase().str()) || checkFileExists(filename.toLowerCase().str()));
    }
    
    inline bool fileRead(const char *filename, MemoryBuffer &buff)
    {
        Owned<IFile> fi=createIFile(filename);
        if (fi)
        {
            Owned<IFileIO> fio=fi->open(IFOread);
            if (fio)
            {
                offset_t len=fio->size();
                size32_t memlen = (size32_t)len;
                assertex(len == memlen);
                if (fio->read(0, memlen, buff.reserveTruncate(memlen))==len)
                    return true;
            }
        }
        buff.clear();
        return false;
    }

    const char *pastLast(const char *src, const char *fnd)
    {
        int fndlen=(fnd) ? strlen(fnd) : 0;
        int srclen=(src) ? strlen(src) : 0;
        if (fndlen && srclen)
        {
            while (srclen--)
            {
                if (!strnicmp(src+srclen, fnd, fndlen))
                    return src+srclen+fndlen;
            }

        }
        return NULL;
    }

    //IIncludeHandler
    bool getInclude(const char* includename, MemoryBuffer& includebuf, bool& pathOnly)
    {
        if(!includename)
            return false;

        pathOnly = true;
        includebuf.clear();
        const char *finger=pastLast(includename, "esp/xslt/");
        if (finger)
        {
            StringBuffer filepath;
            if (fileExists(filepath.append(getCFD()).append("smc_xslt/").append(finger)) || fileExists(filepath.clear().append(getCFD()).append("xslt/").append(finger)))
            {
                includebuf.append(filepath.length(), filepath.str());
                return true;
            }
        }
        else
        {
            // First of all, it's better to use absolute path to specify the include, like /esp/xslt/ui_overrides.xslt.
            // When you specify the include as relative path, for example ./ui_overrides.xslt
            // the path will be expanded (by xmllib's source resolver) to its full path, beginning with file://
            // on windows it looks like: file:///C:/playground/esp_lsb2/xslt/ui_overrides.xslt
            // on linux: file:///home/yma/playground/esp_lsb2/xslt/ui_overrides.xslt
            // If current path not found, use root
            char dir[_MAX_PATH];
            if (!GetCurrentDirectory(sizeof(dir), dir)) {
                IERRLOG("ESPxsltIncludeHandler::getInclude: Current directory path too big, setting local path to null");
                dir[0] = 0;
            }
#ifdef _WIN32
            for(int i = 0; i < _MAX_PATH; i++)
            {
                if(dir[i] == '\0')
                    break;
                else if(dir[i] == PATHSEPCHAR)
                    dir[i] = '/';
            }
#endif
            finger = strstr(includename, dir);
            if(finger)
            {
                finger += strlen(dir) + 1;
                StringBuffer filepath(finger);
                if (fileExists(filepath))
                {
                    includebuf.append(filepath.length(), filepath.str());
                    return true;
                }
            }
        }
        return false;
    }
};

ESPxsltIncludeHandler g_includeHandler;

void CEspConfig::bindServer(IEspServer &server, IEspContainer &container)
{
    list<binding_cfg*>::iterator bit = m_bindings.begin();

    while (bit != m_bindings.end())
    {
        binding_cfg *pbfg = *bit;

        if (pbfg && pbfg->bind && pbfg->service && pbfg->protocol)
        {
            map<string, protocol_cfg*>::iterator pit = m_protocols.find(pbfg->protocol_name.str());
            if(pit == m_protocols.end())
                OWARNLOG("Protocol %s not found for binding %s", pbfg->protocol_name.str(), pbfg->name.str());
            else
            {
                Owned<IXslProcessor> xslp=getXslProcessor();
                if (xslp)
                {
                    xslp->setDefIncludeHandler(dynamic_cast<IIncludeHandler*>(&g_includeHandler));
                    pbfg->bind->setXslProcessor(xslp);
                }
                pbfg->bind->setContainer(&container);
                pbfg->service->setContainer(&container);
                pbfg->protocol->setContainer(&container);
                pbfg->bind->addProtocol(pbfg->protocol->getProtocolName(), *pbfg->protocol.get());
                if(pbfg->service != NULL)
                    pbfg->bind->addService(pbfg->service->getServiceType(), pbfg->address.str(), pbfg->port, *pbfg->service.get());

                IEspProtocol* prot = (*pit).second->prot;
                server.addBinding(pbfg->name.str(), pbfg->address.str(), pbfg->port, *prot, *(pbfg->bind.get()), pbfg->isDefault, m_cfg.get());
            }
        }
        else
        {
            OERRLOG("Binding %s wasn't loaded correctly", pbfg->name.str());
        }

        bit++;
    }
}

void CEspConfig::saveAttachState()
{
    StringBuffer espProcAttachState;
    espProcAttachState.setf( "<ESPAttachState StateSaveTimems='%d' attached='%s' subscribed='%s'/>", msTick(), isDetachedFromDali() ? "0" : "1", isSubscribedToDali() ? "1" : "0");

    DBGLOG("ESP Process [%s] State to be stored: '%s'", m_process.str(), espProcAttachState.str());
    Owned<IPropertyTree> serviceESDLDef = createPTreeFromXMLString(espProcAttachState.str(), ipt_caseInsensitive);
    saveXML(m_daliAttachStateFileName.str(), serviceESDLDef);
}

void CEspConfig::unloadBindings()
{
    list<binding_cfg*>::iterator iter = m_bindings.begin();
    while (iter!=m_bindings.end())
    {
        binding_cfg *bcfg = *iter;

        if(bcfg!=NULL)
        {
            bcfg->protocol.clear();
            bcfg->bind.clear();
            bcfg->service.clear();
            delete bcfg;
        }
        iter++;
    }

    m_bindings.clear();
}


void CEspConfig::unloadServices()
{
    map<string, srv_cfg*>::iterator srvi = m_services.begin();
    while (srvi!=m_services.end())
    {
        srv_cfg* scfg = srvi->second;
        if(scfg)
        {
            scfg->cfg.clear();
            scfg->srv.clear();
            delete scfg;
        }
        srvi++;
    }

    m_services.clear();
}


void CEspConfig::unloadProtocols()
{
    map<string, protocol_cfg*>::iterator proti = m_protocols.begin();
    while (proti!=m_protocols.end())
    {
        protocol_cfg *pcfg = proti->second;
        if(pcfg)
        {
            pcfg->prot.clear();
            pcfg->cfg.clear();
            delete pcfg;
        }
        
        proti++;
    }
    m_protocols.clear();
}

IEspPlugin* CEspConfig::getPlugin(const char* name)
{
    if(!name || !*name) 
        return NULL;
    ForEachItemIn(x, m_plugins)
    {
        IEspPlugin* plgn = &m_plugins.item(x);
        if(plgn && stricmp(name, plgn->getName()) == 0)
        {
            return LINK(plgn);
        }
    }
    Owned<IEspPlugin> pplg = loadPlugin(name);
    if(pplg)
    {
        pplg->Link(); //YMA: intentional leak. Unloading DLLs during ESP shutdown causes all kinds of issues.
        m_plugins.append(*LINK(pplg)); 
        return LINK(pplg);
    }

    return NULL;
}

void CEspConfig::checkESPCache(IEspServer& server)
{
    const char* cacheInitString = m_cfg->queryProp("@espCacheInitString");
    IPropertyTree* espCacheCfg = m_cfg->queryBranch("ESPCache");
    if (!espCacheCfg && isEmptyString(cacheInitString))
        return;

    if (!espCacheCfg)
    {
        if (!server.addCacheClient("default", cacheInitString))
            throw MakeStringException(-1, "Failed in checking ESP cache service using %s", cacheInitString);
        return;
    }
    Owned<IPropertyTreeIterator> iter = espCacheCfg->getElements("Group");
    ForEach(*iter)
    {
        IPropertyTree& espCacheGroup = iter->query();
        const char* id = espCacheGroup.queryProp("@id");
        const char* initString = espCacheGroup.queryProp("@initString");
        if (isEmptyString(id))
            throw MakeStringException(-1, "ESP cache ID not defined");
        if (isEmptyString(initString))
            throw MakeStringException(-1, "ESP cache initStrings not defined");
        if (!server.addCacheClient(id, initString))
            throw MakeStringException(-1, "Failed in checking ESP cache service using %s", initString);
    }
}

bool CEspConfig::reSubscribeESPToDali()
{
    list<binding_cfg*>::iterator iter = m_bindings.begin();
    while (iter!=m_bindings.end())
    {
        binding_cfg& bindingConfig = **iter;
        if (bindingConfig.bind)
        {
            ESPLOG(LogMin, "Requesting binding '%s' to subscribe to DALI notifications", bindingConfig.name.str());
            bindingConfig.bind->subscribeBindingToDali();
        }
        iter++;
    }
    setIsSubscribedToDali(true);
    return true;
}

bool CEspConfig::unsubscribeESPFromDali()
{
    list<binding_cfg*>::iterator iter = m_bindings.begin();
    while (iter!=m_bindings.end())
    {
        binding_cfg& bindingConfig = **iter;
        if (bindingConfig.bind)
        {
            ESPLOG(LogMin, "Requesting binding '%s' to un-subscribe from DALI notifications", bindingConfig.name.str());
            bindingConfig.bind->unsubscribeBindingFromDali();
        }
        iter++;
    }
    setIsSubscribedToDali(false);
    return true;
}

bool CEspConfig::detachESPFromDali(bool force)
{
    CriticalBlock b(attachcrit);
    if (!isDetachedFromDali())
    {
        if(!force)
        {
            if (!canAllBindingsDetachFromDali())
                return false;
        }

        if (!unsubscribeESPFromDali())
            return false;

        list<binding_cfg*>::iterator iter = m_bindings.begin();
        while (iter!=m_bindings.end())
        {
            binding_cfg& xcfg = **iter;
            ESPLOG(LogMin, "Detach ESP From DALI: requesting binding: '%s' to detach...", xcfg.name.str());
            if (xcfg.bind)
            {
                xcfg.bind->detachBindingFromDali();
            }

            iter++;
        }
        setIsDetachedFromDali(true);
        disconnectLogMsgManagerFromDali();
        closedownClientProcess();
        saveAttachState();
    }
    return true;
}

bool CEspConfig::attachESPToDali()
{
    bool success = true;

    CriticalBlock b(attachcrit);
    if (isDetachedFromDali())
    {
        setIsDetachedFromDali(false);
        StringBuffer daliservers;
        if (m_cfg->getProp("@daliServers", daliservers))
            initDali(daliservers.str());

        list<binding_cfg*>::iterator iter = m_bindings.begin();
        while (iter!=m_bindings.end())
        {
            binding_cfg& xcfg = **iter;
            ESPLOG(LogMin, "Attach ESP to DALI: requesting binding: '%s' to attach...", xcfg.name.str());
            if (xcfg.bind)
            {
                map<string, srv_cfg*>::iterator sit = m_services.find(xcfg.service_name.str());
                if(sit == m_services.end())
                    ESPLOG(LogMin, "Warning: Service %s not found for the binding", xcfg.service_name.str());
                else
                    ((*sit).second->srv)->attachServiceToDali();
            }
            iter++;
        }

        reSubscribeESPToDali();

        saveAttachState();
    }
    return success;
}

bool CEspConfig::canAllBindingsDetachFromDali()
{
    list<binding_cfg*>::iterator iter = m_bindings.begin();
    while (iter!=m_bindings.end())
    {
        binding_cfg& xcfg = **iter;
        if (!xcfg.bind->canDetachFromDali())
            return false;
        iter++;
    }
    return true;
}

IEspRpcBinding* CEspConfig::queryBinding(const char* name)
{
    for (auto binding : m_bindings)
    {
        if (strcmp(binding->name.str(), name) == 0)
            return binding->bind.get();
    }
    return nullptr;
}

