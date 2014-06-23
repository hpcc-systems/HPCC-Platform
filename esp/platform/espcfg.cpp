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
#include "espcontext.hpp"

#include <dalienv.hpp>

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
    m_options.logReq = m_cfg->getPropBool("@logRequests", false);
    m_options.logResp = m_cfg->getPropBool("@logResponses", false);
    m_options.frameTitle.set(m_cfg->queryProp("@name"));
    m_options.slowProcessingTime = m_cfg->getPropInt("@slowProcessingTime", 30) * 1000; //in msec

    if (!m_cfg->getProp("@name", m_process))
    {
        ERRLOG("EspProcess name not found");
    }
    else
    {
        DBGLOG("ESP process name [%s]", m_process.str());

        IPropertyTreeIterator *pt_iter = NULL;
        StringBuffer daliservers;
        if (m_cfg->getProp("@daliServers", daliservers))
            initDali(daliservers.str());

#ifndef _DEBUG
        startPerformanceMonitor(m_cfg->getPropInt("@perfReportDelay", 60)*1000);
#endif

        //get the local computer name:              
        m_cfg->getProp("@computer", m_computer);

        //get the local computer information:               
        StringBuffer xpath;
        xpath.appendf("Hardware/Computer[@name=\"%s\"]", m_computer.str());

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
                    binding_cfg *bcfg = new binding_cfg;
                    
                    ptree->getProp("@name", bcfg->name);
                    ptree->getProp("@type", bcfg->type);
                    ptree->getProp("@plugin", bcfg->plugin);
                    fixPlugin(bcfg->plugin);
                    bcfg->isDefault = ptree->getPropBool("@defaultBinding", false);
                    
                    StringBuffer addr;
                    ptree->getProp("@netAddress", addr);
                    if(strcmp(addr.str(), ".") == 0)
                    {
                        bcfg->address.append("0.0.0.0");
                    }
                    else
                    {
                        bcfg->address.append(addr.str());
                    }
                    
                    StringBuffer portstr;
                    ptree->getProp("@port", portstr);
                    bcfg->port = atoi(portstr.str());
                    ptree->getProp("@service", bcfg->service_name);
                    ptree->getProp("@protocol", bcfg->protocol_name);
                    
                    m_bindings.push_back(bcfg);
                }
                
                pt_iter->next();
            }
    
            pt_iter->Release();
            pt_iter=NULL;
        }
    }
}


void CEspConfig::sendAlert(int severity, char const * descr, char const * subject) const
{
}

void CEspConfig::initDali(const char *servers)
{
    if (servers!=NULL && *servers!=0 && !daliClientActive())
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
        setPasswordsFromSDS();

        serverstatus = new CSDSServerStatus("ESPserver");
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
        DBGLOG("Warning: Service %s not found for the binding", xcfg.service_name.str());
    }
    else
    {
        isrv = (*sit).second->srv;
    }

    if(pit == m_protocols.end())
    {
        throw MakeStringException(-1, "Protocol %s not found for the binding", xcfg.protocol_name.str());
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
                    DBGLOG("Load binding %s (type: %s, process: %s) succeeded", xcfg.name.str(), xcfg.type.str(), m_process.str());
                else
                    ERRLOG("Failed to load binding %s (type: %s, process: %s)", xcfg.name.str(), xcfg.type.str(), m_process.str());
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
        throw MakeStringException(-1, "procedure esp_service_factory can't be loaded");
}

void CEspConfig::loadServices()
{
    map<string, srv_cfg*>::iterator iter = m_services.begin();
    while (iter!=m_services.end())
    {
        loadService(*(iter->second));
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
        loadBinding(**iter);
        iter++;
    }
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
                if (fio->read(0, len, buff.reserveTruncate(len))==len)
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
                ERRLOG("ESPxsltIncludeHandler::getInclude: Current directory path too big, setting local path to null");
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
                DBGLOG("Protocol %s not found for binding %s", pbfg->protocol_name.str(), pbfg->name.str());
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
            ERRLOG("Binding %s wasn't loaded correctly", pbfg->name.str());
        }

        bit++;
    }
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

