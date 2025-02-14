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

#pragma warning (disable : 4786)

#include "esphttp.hpp"

//Jlib
#include "jliball.hpp"

//SCM Interface definition includes:
#include "esp.hpp"
#include "espthread.hpp"

#include "espplugin.ipp"

//ESP Bindings
#include "http/platform/httpprot.hpp"
#include "http/platform/httptransport.ipp"
#include "http/platform/httpservice.hpp"
#include "SOAP/Platform/soapservice.hpp"

#include "securesocket.hpp"

#define ESP_FACTORY DECL_EXPORT

IThreadPool* http_thread_pool;
CHttpThreadPoolFactory* http_pool_factory;

struct PooledThreadInfo
{
    ISocket& socket;
    CEspApplicationPort& apport;
    int maxRequestEntityLength = 0;
    bool isSSL = false;
    ISecureSocketContext* ssctx = nullptr;
    IPersistentHandler* persistentHandler = nullptr;
    bool shouldClose = false;

    PooledThreadInfo(ISocket& _socket, CEspApplicationPort& _apport) : socket(_socket), apport(_apport) {}
    ~PooledThreadInfo()
    {
        if (std::uncaught_exceptions() > 0)
        {
            IERRLOG("Error starting thread from %s thread pool.", isSSL ? "https" : "http");
            socket.close();
        }
    }
};

/**************************************************************************
 *  CHttpProtocol Implementation                                          *
 **************************************************************************/
CHttpProtocol::CHttpProtocol()
{
    m_maxConcurrentThreads = 0;
}

CHttpProtocol::~CHttpProtocol()
{
    if(http_thread_pool)
    {
        http_thread_pool->Release();
        http_thread_pool = NULL;
    }

    if(http_pool_factory)
    {
        http_pool_factory->Release();
        http_pool_factory = NULL;
    }
}

void CHttpProtocol::init(IPropertyTree * cfg, const char * process, const char * protocol)
{
    Owned<IPropertyTree> proc_cfg = getProcessConfig(cfg, process);
    if (proc_cfg)
    {
        CEspProtocol::setViewConfig(proc_cfg->getPropBool("@httpConfigAccess"));
        const char* mctstr = proc_cfg->queryProp("@maxConcurrentThreads");
        if(mctstr && *mctstr)
        {
            m_maxConcurrentThreads = atoi(mctstr);
        }

        m_threadCreateTimeout = 0;
        const char* timeoutstr = proc_cfg->queryProp("@threadCreateTimeout");
        if(timeoutstr && *timeoutstr)
        {
            m_threadCreateTimeout = atoi(timeoutstr);
        }

        if(m_maxConcurrentThreads > 0)
        {
            // Could use a mutex, but since all the protocols are instantiated sequentially, not really necessary
            if(!http_pool_factory)
                http_pool_factory = new CHttpThreadPoolFactory();
            if(!http_thread_pool)
                http_thread_pool = createThreadPool("Http Thread", http_pool_factory, false, nullptr, m_maxConcurrentThreads, INFINITE);
        }
    }

    initPersistentHandler(proc_cfg);

    Owned<IPropertyTree> proto_cfg = getProtocolConfig(cfg, protocol, process);
    if(proto_cfg)
    {
        const char* lenstr = proto_cfg->queryProp("@maxRequestEntityLength");
        if(lenstr && *lenstr)
        {
            setMaxRequestEntityLength(atoi(lenstr));
        }
    }
}

bool CHttpProtocol::notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler, bool shouldClose)
{
    try
    {
        char name[256];
        int port = sock->name(name, 255);

        CEspApplicationPort *apport = queryApplicationPort(port);

        if(apport == NULL)
            throw MakeStringException(-1, "binding not found!");
        
        if(apport != NULL)
        {
            Owned<ISocket> accepted;
            if (persistentHandler == nullptr)
                accepted.setown(sock->accept());
            else
                accepted.set(sock);
            if (accepted.get() != NULL)
            {
                char peername[256];
                int port = accepted->peer_name(peername, 256);

    #if defined(_DEBUG)
                DBGLOG("HTTP connection from %s:%d on %s socket", peername, port, persistentHandler?"persistent":"new");
    #endif          

                if(m_maxConcurrentThreads > 0)
                {
                    // Using Threading pool instead of generating one thread per request.
                    PooledThreadInfo pti(*accepted, *apport);
                    pti.maxRequestEntityLength = getMaxRequestEntityLength();
                    pti.persistentHandler = persistentHandler;
                    pti.shouldClose = shouldClose;
                    // cleanup on exception is handled by pti
                    http_thread_pool->start((void*)&pti, "", m_threadCreateTimeout > 0?m_threadCreateTimeout*1000:0);
                }
                else
                {
                    /* create one thread per request */
                    CHttpThread *workthread = new CHttpThread(*accepted.getLink(), *apport, CEspProtocol::getViewConfig(), false, nullptr, persistentHandler);
                    workthread->setMaxRequestEntityLength(getMaxRequestEntityLength());
                    workthread->setShouldClose(shouldClose);
                    workthread->start(false);
                    //MORE: The caller should wait for the thread to finish, otherwise the program can crash on exit
                    workthread->Release();
                }
            }
        }
        else
        {
            throw MakeStringException(-1, "can't acquire bindings IEspHttpBinding interface (via dynamic_cast)!");
        }
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) in CHttpProtocol::notifySelected()", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown Exception in CHttpProtocol::notifySelected()");
    }

    return false;
}

//IEspProtocol
const char * CHttpProtocol::getProtocolName()
{
   return "http_protocol";
}

/**************************************************************************
 *  CSecureHttpProtocol Implementation                                    *
 **************************************************************************/
CSecureHttpProtocol::CSecureHttpProtocol(IPropertyTree* cfg)
{
    m_maxConcurrentThreads = 0;

    if(cfg != NULL)
    {
        m_config.setown(cfg);

        IEspPlugin *pplg = loadPlugin(SSLIB);
        if (!pplg)
            throw MakeStringException(-1, "dll/shared-object %s can't be loaded", SSLIB);

        const char *issuer = cfg->queryProp("issuer");
        if (!isEmptyString(issuer))
        {
            const char *trustedPeers = nullptr;
            if (cfg->hasProp("verify"))
                trustedPeers = cfg->queryProp("verify/trusted_peers");
            createSecureSocketContextSecretSrv_t xproc = (createSecureSocketContextSecretSrv_t) pplg->getProcAddress("createSecureSocketContextSecretSrv");
            if (!xproc)
                throw MakeStringException(-1, "procedure createSecureSocketContextSecretSrv can't be loaded");
            m_ssctx.setown(xproc(issuer, trustedPeers, false));
        }
        else
        {
            //ensure keys are specified. Passphrase is optional
            StringBuffer sb;
            cfg->getProp("certificate", sb);
            if(sb.isEmpty())
                throw MakeStringException(-1, "certificate file not specified in config file");

            cfg->getProp("privatekey", sb.clear());
            if(sb.isEmpty())
                throw MakeStringException(-1, "private key file not specified in config file");

            createSecureSocketContextEx2_t xproc = (createSecureSocketContextEx2_t) pplg->getProcAddress("createSecureSocketContextEx2");
            if (!xproc)
                throw MakeStringException(-1, "procedure createSecureSocketContextEx2 can't be loaded");
            m_ssctx.setown(xproc(cfg, ServerSocket));
        }
    }
}

CSecureHttpProtocol::~CSecureHttpProtocol()
{
    if(http_thread_pool)
    {
        http_thread_pool->Release();
        http_thread_pool = NULL;
    }

    if(http_pool_factory)
    {
        http_pool_factory->Release();
        http_pool_factory = NULL;
    }
}

void CSecureHttpProtocol::init(IPropertyTree * cfg, const char * process, const char * protocol)
{
    Owned<IPropertyTree> proc_cfg = getProcessConfig(cfg, process);
    if (proc_cfg)
    {
        CEspProtocol::setViewConfig(proc_cfg->getPropBool("@httpConfigAccess"));
        const char* mctstr = proc_cfg->queryProp("@maxConcurrentThreads");
        if(mctstr && *mctstr)
        {
            m_maxConcurrentThreads = atoi(mctstr);
        }

        if(m_maxConcurrentThreads > 0)
        {
            // Could use a mutex, but since all the protocols are instantiated sequentially, not really necessary
            if(!http_pool_factory)
                http_pool_factory = new CHttpThreadPoolFactory();
            if(!http_thread_pool)
                http_thread_pool = createThreadPool("Http Thread", http_pool_factory, false, nullptr, m_maxConcurrentThreads, INFINITE);
        }
    }

    initPersistentHandler(proc_cfg);

    Owned<IPropertyTree> proto_cfg = getProtocolConfig(cfg, protocol, process);
    if(proto_cfg)
    {
        const char* lenstr = proto_cfg->queryProp("@maxRequestEntityLength");
        if(lenstr && *lenstr)
        {
            setMaxRequestEntityLength(atoi(lenstr));
        }
    }
}

bool CSecureHttpProtocol::notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler, bool shouldClose)
{
    try
    {
        char name[256];
        int port = sock->name(name, 255);

        CEspApplicationPort *apport = queryApplicationPort(port);
        if(apport == NULL)
            throw MakeStringException(-1, "binding not found!");
        
        if(apport != NULL)
        {
            Owned<ISocket>accepted;
            if(persistentHandler == nullptr)
                accepted.setown(sock->accept());
            else
                accepted.set(sock);
            if (accepted.get() != NULL)
            {
                char peername[256];
                int port = accepted->peer_name(peername, 256);
                ESPLOG(LogMax, "HTTPS connection from %s:%d on %s socket", peername, port, persistentHandler?"persistent":"new");
                if(m_ssctx != NULL)
                {
                    if(m_maxConcurrentThreads > 0)
                    {
                        // Using Threading pool instead of generating one thread per request.
                        PooledThreadInfo pti(*accepted, *apport);
                        pti.maxRequestEntityLength = getMaxRequestEntityLength();
                        pti.isSSL = true;
                        pti.ssctx = m_ssctx.get();
                        pti.persistentHandler = persistentHandler;
                        pti.shouldClose = shouldClose;
                        // cleanup on exception is handled by pti
                        http_thread_pool->start((void*)&pti);
                    }
                    else
                    {
                        /* create one thread per request */
                        CHttpThread *workthread = new CHttpThread(*accepted.getLink(), *apport, CEspProtocol::getViewConfig(), true, m_ssctx.get(), persistentHandler);
                        workthread->setMaxRequestEntityLength(getMaxRequestEntityLength());
                        workthread->setShouldClose(shouldClose);
                        workthread->start(false);
                        ESPLOG(LogMax, "Request processing thread started.");
                        //MORE: The caller should wait for the thread to finish, otherwise the program can crash on exit
                        workthread->Release();
                    }
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            throw MakeStringException(-1, "can't acquire bindings IEspHttpBinding interface (via dynamic_cast)!");
        }
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) in CSecureHttpProtocol::notifySelected()", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("Unknown Exception in CSecureHttpProtocol::notifySelected()");
    }

    return false;
}

//IEspProtocol
const char * CSecureHttpProtocol::getProtocolName()
{
    return "secure_http_protocol";
}


/**************************************************************************
 *  CHttpThread Implementation                                            *
 **************************************************************************/
CHttpThread::CHttpThread(ISocket& sock, CEspApplicationPort& apport, bool viewConfig, bool isSSL, ISecureSocketContext* ssctx,  IPersistentHandler* persistentHandler) :
   CEspProtocolThread(&sock, "HTTP Thread"), m_persistentHandler(persistentHandler)
{
    m_viewConfig = viewConfig;
    m_apport = &apport;
    m_is_ssl = isSSL;
    m_ssctx = ssctx;
}

CHttpThread::~CHttpThread()
{
}

bool CHttpThread::onRequest()
{
    keepAlive = false;
    ActiveRequests recording;

    Owned<CEspHttpServer> httpserver;
    
    Owned<ISecureSocket> secure_sock;
    if(m_is_ssl && m_ssctx && m_persistentHandler == nullptr)
    {
        ESPLOG(LogMax, "Creating secure socket");
        LogLevel logLevel = getEspLogLevel();
        secure_sock.setown(m_ssctx->createSecureSocket(m_socket.getLink(), logLevel));
        int res = 0;
        try
        {
            ESPLOG(LogMax, "Accepting from secure socket");
            res = secure_sock->secure_accept(logLevel);
            if(res < 0)
                return false;
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            IERRLOG("%s", emsg.str());
            return false;
        }
        catch(...)
        {
            IERRLOG("Unknown exception accepting from secure socket");
            return false;
        }
        ESPLOG(LogMax, "Request from secure socket");
        m_socket.set(secure_sock);
        httpserver.setown(new CEspHttpServer(*secure_sock.get(), *m_apport, m_viewConfig, getMaxRequestEntityLength()));
        IEspContext* ctx = httpserver->queryContext();
        if(ctx)
        {
            StringBuffer version;
            secure_sock->get_ssl_version(version);
            ctx->addTraceSummaryValue(LogMin, "custom_fields.sslProtocol", version.str(), TXSUMMARY_GRP_ENTERPRISE);
        }
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, *m_apport, m_viewConfig, getMaxRequestEntityLength()));
    }

    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);

    m_httpserver = httpserver;
    httpserver->setSocketReturner(this);
    httpserver->setIsSSL(m_is_ssl);
    httpserver->setShouldClose(m_shouldClose);
    httpserver->processRequest();

    returnSocket(false);

    clearThreadLocal();

    return false;
}

void CHttpThread::returnSocket(bool cascade)
{
    if (m_httpSocketReturned)
        return;
    m_httpSocketReturned = true;
    CEspHttpServer* httpserver = dynamic_cast<CEspHttpServer*>(m_httpserver);
    if (m_persistentHandler == nullptr)
    {
        keepAlive = !m_shouldClose && m_apport->queryProtocol()->persistentEnabled() && httpserver->persistentEligible();
        if (keepAlive)
            m_apport->queryProtocol()->addPersistent(m_socket.get());
    }
    else
    {
        keepAlive = !m_shouldClose && httpserver->persistentEligible();
        m_persistentHandler->doneUsing(m_socket, keepAlive);
    }

    if (cascade)
        CEspProtocolThread::returnSocket();
}

void CHttpThread::returnSocket()
{
    returnSocket(true);
}
/**************************************************************************
 *  CPooledHttpThread Implementation                                      *
 **************************************************************************/
void CPooledHttpThread::init(void *param)
{
    if (!param)
        throw makeStringException(-1, "CPooledHttpThread::init: invalid param");
    PooledThreadInfo* ptip = (PooledThreadInfo*)param;
    m_socket.set(&ptip->socket);
    m_apport = &ptip->apport;
    m_MaxRequestEntityLength = ptip->maxRequestEntityLength;
    m_is_ssl = ptip->isSSL;
    m_ssctx = ptip->ssctx;
    m_persistentHandler = ptip->persistentHandler;
    m_shouldClose = ptip->shouldClose;
    m_httpserver = nullptr;
    m_processAborted = false;
    m_socketReturned = false;
}

CPooledHttpThread::~CPooledHttpThread()
{
}

void CPooledHttpThread::threadmain()
{
    // Ensure the socket is returned before leaving this method. There is no reason to retain
    // sockets until the thread is reused.
    struct SocketReturner
    {
        CPooledHttpThread& owner;
        SocketReturner(CPooledHttpThread& _owner) : owner(_owner) {}
        ~SocketReturner() { owner.returnSocket();}
    } sr(*this);

    if (!m_socket || !m_apport)
    {
        IERRLOG("CPooledHttpThread::threadmain: uninitialized thread");
        return;
    }
    TimeSection timing("CPooledHttpThread::threadmain()");
    Owned<CEspHttpServer> httpserver;
    
    Owned<ISecureSocket> secure_sock;
    if(m_is_ssl && m_ssctx && m_persistentHandler == nullptr)
    {
        secure_sock.setown(m_ssctx->createSecureSocket(m_socket.getLink(), getEspLogLevel()));
        int res = 0;
        try
        {
            res = secure_sock->secure_accept();
            if(res < 0)
            {
                return;
            }
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            IERRLOG("%s", emsg.str());
            return;
        }
        catch(...)
        {
            return;
        }
        m_socket.set(secure_sock);
        httpserver.setown(new CEspHttpServer(*m_socket, *m_apport, false, getMaxRequestEntityLength()));
        IEspContext* ctx = httpserver->queryContext();
        if(ctx)
        {
            StringBuffer version;
            secure_sock->get_ssl_version(version);
            ctx->addTraceSummaryValue(LogMin, "custom_fields.sslProtocol", version.str(), TXSUMMARY_GRP_ENTERPRISE);
        }
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, *m_apport, false, getMaxRequestEntityLength()));
    }
    m_httpserver = httpserver;
    httpserver->setShouldClose(m_shouldClose);
    httpserver->setSocketReturner(this);
    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);
    try
    {
        ESP_TIME_SECTION("CPooledHttpThread::threadmain: httpserver->processRequest()");
        httpserver->processRequest();
    }
    catch (IException *e) 
    {
        m_processAborted = true;
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) in CPooledHttpThread::threadmain().", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        m_processAborted = true;
        IERRLOG("General Exception - in CPooledHttpThread::threadmain().");
    }

    clearThreadLocal();
}

void CPooledHttpThread::returnSocket()
{
    if (m_socketReturned || !m_socket)
        return;
    m_socketReturned = true;
    CEspHttpServer* httpserver = dynamic_cast<CEspHttpServer*>(m_httpserver);
    bool keepAlive = false;
    if (!m_processAborted)
    {
        if (m_persistentHandler == nullptr)
        {
            keepAlive = !m_shouldClose && m_apport->queryProtocol()->persistentEnabled() && httpserver->persistentEligible();
            if (keepAlive)
                m_apport->queryProtocol()->addPersistent(m_socket.get());
        }
        else
        {
            keepAlive = !m_shouldClose && httpserver->persistentEligible();
            m_persistentHandler->doneUsing(m_socket, keepAlive);
        }
    }

    try
    {
        if (!keepAlive)
            m_socket->shutdown(SHUTDOWN_WRITE);
        m_socket.clear();
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) - CPooledHttpThread::threadmain(), closing socket.", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("General Exception - CPooledHttpThread::threadmain(), closing socket.");
    }
}
