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

#ifndef __ESPP_HPP__
#define __ESPP_HPP__

#include "espthread.hpp"
#include "espcfg.ipp"

typedef ISocket * isockp;


typedef MapBetween<int, int, isockp, isockp> SocketPortMap;
typedef CopyReferenceArrayOf<ISocket> SocketPortArray;

class CEspTerminator : public Thread
{
public:
    IMPLEMENT_IINTERFACE;

    virtual int run()
    {
        sleep(15);
        _exit(0);
    }
};

class CEspServer : public CInterface,
   implements ISocketSelectHandler,
   implements IEspServer,
   implements IEspContainer,
   implements IRestartHandler
{
private:
    SocketEndpoint m_address;
    Owned<ISocketSelectHandler> m_selectHndlr;
    SocketPortMap m_srvSockets;
    SocketPortArray m_socketCleanup;
    Semaphore m_waitForExit;
    bool m_exiting;
    bool m_useDali;
    LogLevel m_logLevel;
    bool m_logReq;
    bool m_logResp;
    LogLevel txSummaryLevel;
    bool txSummaryResourceReq;
    unsigned m_slowProcessingTime;
    StringAttr m_frameTitle;
    Mutex abortMutex;
    bool m_SEHMappingEnabled;
    CEspConfig* m_config;
    CriticalSection m_BindingCritSect;
    unsigned countCacheClients = 0;
    MapStringToMyClass<IEspCache> cacheClientMap;

public:
    IMPLEMENT_IINTERFACE;

    //CEspServer(SocketEndpoint address)
    CEspServer(CEspConfig* config) : m_config(config)
    {
        //m_address = address;
        m_address = config->getLocalEndpoint();
        m_selectHndlr.setown(createSocketSelectHandler());
        m_exiting = false;
        m_useDali = false;
        m_logLevel = config->m_options.logLevel;
        m_logReq = config->m_options.logReq;
        m_logResp = config->m_options.logResp;
        txSummaryLevel = config->m_options.txSummaryLevel;
        txSummaryResourceReq = config->m_options.txSummaryResourceReq;
        m_slowProcessingTime = config->m_options.slowProcessingTime;
        m_frameTitle.set(config->m_options.frameTitle);
        m_SEHMappingEnabled = false;
    }

    ~CEspServer()
    {
        ForEachItemIn(sindex, m_socketCleanup)
        {
            m_socketCleanup.item(sindex).Release();
        }
        m_socketCleanup.kill();
    }

    void waitForExit(CEspConfig &config)
    {
        if (config.usesDali())
        {
            m_useDali = true;
            bool pollForDali = false;
            while (!m_exiting)
            {
                {
                    synchronized sync(abortMutex);
                    pollForDali = config.isDetachedFromDali() || config.checkDali();
                }

                if (pollForDali)
                    m_waitForExit.wait(1000); //if detached, should we wait longer?
                else
                {
                    OERRLOG("Exiting ESP -- Lost DALI connection!");
                    break;
                }
            }
        }
        else
        {
            m_useDali = false;
            m_waitForExit.wait();
            sleep(1);
        }
    }
    //IRestartHandler
    void Restart()
    {
        exitESP();
    }

//IEspContainer
    void exitESP()
    {
        if(m_SEHMappingEnabled)
        {
            DisableSEHtoExceptionMapping();
            m_SEHMappingEnabled = false;
        }

        // YMA: there'll be a leak here, but it's ok.
        CEspTerminator* terminator = new CEspTerminator;  
        terminator->start();

        m_exiting=true;
        if(!m_useDali)
            m_waitForExit.signal();
    }

    void setLogLevel(LogLevel level) { m_logLevel = level; }
    void setLogRequests(bool logReq) { m_logReq = logReq; }
    void setLogResponses(bool logResp) { m_logResp = logResp; }
    void setTxSummaryLevel(LogLevel level) { txSummaryLevel = level; }
    void setTxSummaryResourceReq(bool logReq) { txSummaryResourceReq = logReq; }

    LogLevel getLogLevel() { return m_logLevel; }
    bool getLogRequests() { return m_logReq; }
    bool getLogResponses() { return m_logResp; }
    LogLevel getTxSummaryLevel() { return txSummaryLevel; }
    bool getTxSummaryResourceReq() { return txSummaryResourceReq; }
    void setFrameTitle(const char* title)  { m_frameTitle.set(title); }
    const char* getFrameTitle()  { return m_frameTitle.get(); }
    unsigned getSlowProcessingTime() { return m_slowProcessingTime; }

    void log(LogLevel level, const char* fmt, ...) __attribute__((format(printf, 3, 4)))
    {
        if (getLogLevel()>=level)
        {
            va_list args;
            va_start(args, fmt);
            VALOG(MCdebugInfo, unknownJob, fmt, args);
            va_end(args);
        }
    }

//IEspServer
    void addProtocol(IEspProtocol &protocol)
    {
    }

    void addBinding(const char * name, const char * host, unsigned short port, IEspProtocol &protocol, IEspRpcBinding &binding, bool isdefault, IPropertyTree* cfgtree)
    {
        StringBuffer strIP;

        if (host != NULL)
            strIP.append(host);
        else
            m_address.getIpText(strIP);

        LOG(MCprogress, "binding %s, on %s:%d", name, strIP.str(), port);

        CriticalBlock cb(m_BindingCritSect);
        ISocket **socketp = m_srvSockets.getValue(port);
        ISocket *socket=(socketp!=NULL) ? *socketp : NULL;

        if (socket==NULL)
        {
            int backlogsize = 0;
            if(cfgtree)
            {
                const char* blstr = cfgtree->queryProp("@maxBacklogQueueSize");
                if(blstr && *blstr)
                    backlogsize = atoi(blstr);
            }
            if(backlogsize > 0)
            {
                socket = ISocket::create_ip(port, strIP.str(), backlogsize);
            }
            else
            {
                socket = ISocket::create_ip(port, strIP.str());
            }
            m_socketCleanup.append(*socket);

            LOG(MCprogress, "  created server socket(%d)", socket->OShandle());

            m_srvSockets.setValue(port, socket);
            add(socket, SELECTMODE_READ | SELECTMODE_WRITE, dynamic_cast<ISocketSelectNotify*>(&protocol));
            LOG(MCprogress, "  Socket(%d) listening.", socket->OShandle());
        }

        if (socket)
        {
            protocol.addBindingMap(socket, &binding, isdefault);
            socket->Release();
        }
        else
        {
            IERRLOG("Can't create socket on %s:%d", strIP.str(), port);
            throw MakeStringException(-1, "Can't create socket on %s:%d", strIP.str(), port);
        }
    }

    virtual void removeBinding(unsigned short port, IEspRpcBinding & bind)
    {
        IEspProtocol* prot = dynamic_cast<IEspProtocol*>(bind.queryListener());
        if (prot)
        {
            CriticalBlock cb(m_BindingCritSect);
            int left = prot->removeBindingMap(port, &bind);
            if (left == 0)
            {
                DBGLOG("No more bindings on port %d, so freeing up the port.",port);
                ISocket **socketp = m_srvSockets.getValue(port);
                ISocket *socket=(socketp!=nullptr) ? *socketp : nullptr;
                if (socket != nullptr)
                {
                    remove(socket);
                    m_srvSockets.remove(port);
                    socket->close();
                }
            }
        }
    }

    virtual IPropertyTree* queryProcConfig()
    {
        return m_config->queryProcConfig();
    }

    virtual IEspProtocol* queryProtocol(const char* name)
    {
        return m_config->queryProtocol(name);
    }

    virtual IEspRpcBinding* queryBinding(const char* name)
    {
        return m_config->queryBinding(name);
    }

    virtual const char* getProcName()
    {
        return m_config->getProcName();
    }

//ISocketHandler
    void start()
    {
        m_selectHndlr->start();
    }
    void add(ISocket *sock,unsigned mode,ISocketSelectNotify *nfy)
    {
        m_selectHndlr->add(sock, mode, nfy);
    }
    void remove(ISocket *sock)
    {
        m_selectHndlr->remove(sock);
    }
    void stop(bool wait)
    {
        if(m_selectHndlr)
        {
            m_selectHndlr->stop(wait);
            DBGLOG("select handler stopped.");
        }
    }

    void setSavedSEHHandler(bool mappingEnabled)
    {
        m_SEHMappingEnabled = mappingEnabled;
    }

    virtual void sendSnmpMessage(const char* msg);
    virtual bool addCacheClient(const char *id, const char *cacheInitString);
    virtual bool hasCacheClient();
    virtual const void *queryCacheClient(const char* id);
    virtual void clearCacheByGroupID(const char *ids, StringArray& errorMsgs);
    virtual bool reSubscribeESPToDali();
    virtual bool unsubscribeESPFromDali();
    virtual bool detachESPFromDali(bool force);
    virtual bool attachESPToDali();
    virtual bool isAttachedToDali();
    virtual bool isSubscribedToDali();
};

class CEspAbortHandler : public CInterface,
   implements IAbortHandler
{
    CEspConfig* m_config;
    CEspServer* m_srv;

public:
    IMPLEMENT_IINTERFACE;

    CEspAbortHandler()
    {
        m_config=NULL;
        m_srv=NULL;
        addAbortHandler(*this);
    }

    ~CEspAbortHandler()
    {
        removeAbortHandler(*this);
    }

    void setConfig(CEspConfig* config)
    {
        m_config = config;
    }

    void setServer(CEspServer* srv)
    {
        m_srv = srv;
    }

//IAbortHandler
    bool onAbort()
    {
        LOG(MCprogress, "ESP Abort Handler...");
        m_srv->exitESP();
        return false;
    }
};


#define MAX_CHILDREN 1

#endif //__ESPP_HPP__


