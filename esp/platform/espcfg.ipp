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

#include "bindutil.hpp"

//STL
#include <list>
#include <map>
#include <string>
using namespace std;

//ESP
#include "esp.hpp"
#include "espplugin.hpp"
#include "espcache.hpp"

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
    LogLevel txSummaryLevel;
    bool txSummaryResourceReq;
    StringAttr frameTitle;
    unsigned slowProcessingTime; //default 30 seconds

    esp_option() : logReq(false), logResp(false), logLevel(LogMin), txSummaryLevel(LogMin), txSummaryResourceReq(false), slowProcessingTime(30000)
    { }
};

class CSessionCleaner : public Thread
{
    bool       stopping = false;
    Semaphore  sem;

    StringAttr espSessionSDSPath;
    int        checkSessionTimeoutSeconds; //the duration to clean timed out sesssions
    bool       m_isDetached;

public:
    CSessionCleaner(const char* _espSessionSDSPath, int _checkSessionTimeoutSeconds) : Thread("CSessionCleaner"),
        espSessionSDSPath(_espSessionSDSPath), checkSessionTimeoutSeconds(_checkSessionTimeoutSeconds) , m_isDetached(false){ }

    virtual ~CSessionCleaner()
    {
        stopping = true;
        sem.signal();
        join();
    }

    void setIsDetached(bool isDetached) {m_isDetached = isDetached;}

    virtual int run();
};

static CriticalSection attachcrit;

#ifdef ESPCFG_EXPORTS
    #define esp_cfg_decl DECL_EXPORT
#else
    #define esp_cfg_decl DECL_IMPORT
#endif

class esp_cfg_decl CEspConfig : public CInterface, implements IInterface
{
private:
    Owned<IPropertyTree> m_envpt;
    Owned<IPropertyTree> m_cfg;
    Owned<IProperties> m_inputs;
    Owned<CSessionCleaner> m_sessionCleaner;

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
    bool m_detachedFromDali;
    bool m_subscribedToDali;
    StringBuffer m_daliAttachStateFileName;

private:
    CEspConfig(CEspConfig &);

public:
    IMPLEMENT_IINTERFACE;

    esp_option m_options;

    void setIsDetachedFromDali(bool isDetached)
    {
        CriticalBlock b(attachcrit);
        if (m_detachedFromDali != isDetached)
        {
            m_detachedFromDali = isDetached;
            if (m_sessionCleaner)
                m_sessionCleaner->setIsDetached(m_detachedFromDali);
        }
    }

    bool isDetachedFromDali() const
    {
        CriticalBlock b(attachcrit);
        return m_detachedFromDali;
    }

    bool isSubscribedToDali() const
    {
        return m_subscribedToDali;
    }

    void setIsSubscribedToDali(bool issubscribed)
    {
        m_subscribedToDali = issubscribed;
    }

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
            OERRLOG("Lost Dali Connection");
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
    void initDali(const char *location);

    bool isValid(){return (m_cfg.get() != NULL);}
    IPropertyTree* queryConfigPTree(){return m_cfg.get();}

    const char *getProcName(){return m_process.str();}
    const char *getComputerName(){return m_computer.str();}

    const SocketEndpoint &getLocalEndpoint(){return m_address;}

    void ensureESPSessionInTree(IPropertyTree* sessionRoot, const char* procName);
    void ensureSDSSessionDomains();

    void loadProtocols();
    void loadServices();
    void loadBindings();
    void startEsdlMonitor();

    void loadAll()
    {
        DBGLOG("loadServices");
        try
        {
            loadServices();
        }
        catch(IException* ie)
        {
            if (isDetachedFromDali())
                OERRLOG("Could not load ESP service(s) while DETACHED from DALI - Consider re-attaching ESP process.");
            throw(ie);
        }
        loadProtocols();
        loadBindings();
        if(useDali)
            startEsdlMonitor();
    }

    bool reSubscribeESPToDali();
    bool unsubscribeESPFromDali();
    bool detachESPFromDali(bool force);
    bool attachESPToDali();
    bool canAllBindingsDetachFromDali();
    void checkESPCache(IEspServer& server);
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
    void saveAttachState();
    void stopEsdlMonitor();

    void clear()
    {
        unloadBindings();
        unloadServices();
        unloadProtocols();
        if(useDali)
           stopEsdlMonitor();

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

    IPropertyTree* queryProcConfig()
    {
        return m_cfg.get();
    }

    IEspProtocol* queryProtocol(const char* name)
    {
        map<string, protocol_cfg*>::iterator pit = m_protocols.find(name);
        if (pit != m_protocols.end())
            return (*pit).second->prot;
        return nullptr;
    }

    IEspRpcBinding* queryBinding(const char* name);
};


#endif //__ESPCFG_HPP__

