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

#ifndef __ESPP_HPP__
#define __ESPP_HPP__

#include "espthread.hpp"
#include "espcfg.ipp"

typedef ISocket * isockp;


typedef MapBetween<int, int, isockp, isockp> SocketPortMap;
MAKEPointerArray(ISocket, SocketPortArray);

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
    unsigned m_slowProcessingTime;
    StringAttr m_frameTitle;
    Mutex abortMutex;
    bool m_SEHMappingEnabled;
    CEspConfig* m_config;
    
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
            while (!m_exiting)
            {
                bool daliOk;
                {
                    synchronized sync(abortMutex);
                    daliOk=config.checkDali();
                }
                if (daliOk)
                    m_waitForExit.wait(1000);
                else
                {
                    DBGLOG("Exiting ESP -- Lost DALI connection!");
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

    LogLevel getLogLevel() { return m_logLevel; }
    bool getLogRequests() { return m_logReq; }
    bool getLogResponses() { return m_logResp; }
    void setFrameTitle(const char* title)  { m_frameTitle.set(title); }
    const char* getFrameTitle()  { return m_frameTitle.get(); }
    unsigned getSlowProcessingTime() { return m_slowProcessingTime; }

    void log(LogLevel level, const char* fmt, ...)
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
    virtual void sendSnmpMessage(const char* msg) { throwUnexpected(); }
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


