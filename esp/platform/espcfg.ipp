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

#ifndef __ESPCFG_HPP__
#define __ESPCFG_HPP__

#ifdef _WIN32
//CRT
#include <process.h>
#endif

#pragma warning( disable : 4786 )

#include "jliball.hpp"
#include "daclient.hpp"
#include "environment.hpp"
#include <dalienv.hpp>

//STL
#include <list>
#include <map>
#include <string>
using namespace std;

//ESP
#include "esp.hpp"
#include "espplugin.hpp"

struct binding_cfg
{
   StringBuffer name;
   StringBuffer type;
   StringBuffer plugin;
   StringBuffer address;
   unsigned short port;
   bool isDefault;

   StringBuffer protocol_name;
   StringBuffer service_name;

   Owned<IEspProtocol> protocol;
   Owned<IEspService> service;
   Owned<IEspRpcBinding> bind;
};

struct protocol_cfg
{
   StringBuffer name;
   StringBuffer type;
   StringBuffer plugin;

   Owned<IEspProtocolCfg> cfg;
   Owned<IEspProtocol> prot;
};

struct srv_cfg
{
   StringBuffer name;
   StringBuffer type;
   StringBuffer plugin;

   Owned<IEspServiceCfg> cfg;
   Owned<IEspService> srv;
};

struct builtin
{
   esp_protocol_factory_t prot;
   esp_binding_factory_t  bind;
   esp_service_factory_t  serv;
};

struct esp_option
{   
    LogLevel logLevel;
    bool logReq;
    bool logResp;
    StringAttr frameTitle;
    unsigned slowProcessingTime; //default 30 seconds

    esp_option() : logReq(false), logResp(false), logLevel(1), slowProcessingTime(30000)
    { }
};

class CEspConfig : public CInterface, implements IInterface 
{
private:
    Owned<IPropertyTree> m_envpt;
    Owned<IPropertyTree> m_cfg;
    Owned<IProperties> m_inputs;

    StringBuffer m_process;
    StringBuffer m_computer;

    SocketEndpoint m_address;

    map<string, protocol_cfg*> m_protocols; 
    list<binding_cfg*> m_bindings; 
    map<string, srv_cfg*> m_services;

    IArrayOf<IEspPlugin> m_plugins;

    HINSTANCE hsami_;
    CSDSServerStatus *serverstatus;
    bool useDali;

private:
    CEspConfig(CEspConfig &);

public:
    IMPLEMENT_IINTERFACE;

    esp_option m_options;
    
    bool usesDali(){return useDali;}
    bool checkDali()
    {
        try
        {
            if (serverstatus)
            {
                serverstatus->reload();
                return true;
            }
        }
        catch(...)
        {
            DBGLOG("Lost Dali Connection");
        }
        return false;
    }

    virtual void sendAlert(int severity, char const * descr, char const * subject) const;

    CEspConfig(IProperties* inputs, IPropertyTree* envpt, IPropertyTree* procpt, bool useDali=false);

    ~CEspConfig()
    {
        closedownClientProcess();
    }

    void initPtree(const char *location, bool isDali);
    void initDali(const char *location, IPropertyTree *cfg);

    bool isValid(){return (m_cfg.get() != NULL);}
    IPropertyTree* queryConfigPTree(){return m_cfg.get();}

    const char *getProcName(){return m_process.str();}
    const char *getComputerName(){return m_computer.str();}

    const SocketEndpoint &getLocalEndpoint(){return m_address;}

    void loadProtocols();
    void loadServices();
    void loadBindings();

    void loadAll()
    {
        DBGLOG("loadServices");
        loadServices();
        loadProtocols();
        loadBindings();      
    }

    IEspPlugin* getPlugin(const char* name);

    void loadBuiltIns();
    builtin *getBuiltIn(string name);

    void loadProtocol(protocol_cfg &cfg);
    void loadBinding(binding_cfg &cfg);
    void loadService(srv_cfg &cfg);

    void bindServer(IEspServer &server, IEspContainer &container);

    void unloadBindings();
    void unloadServices();
    void unloadProtocols();

    void clear()
    {
        unloadBindings();
        unloadServices();
        unloadProtocols();

        serverstatus=NULL;
        
        m_envpt.clear();
        m_cfg.clear();

    }

    void stopping()
    {
        clearPasswordsFromSDS();
        try
        {
            if (serverstatus)
                delete serverstatus;
        }
        catch(...)
        {
        }
        closeEnvironment();
    }


};


#endif //__ESPCFG_HPP__

