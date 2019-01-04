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

//openssl
#include <openssl/rsa.h>
#include <openssl/crypto.h>
#ifndef _WIN32
//x509.h includes evp.h, which in turn includes des.h which defines
//crypt() that throws different exception than in unistd.h
//(this causes build break on linux) so exclude it
#define crypt DONT_DEFINE_CRYPT
#include <openssl/x509.h>
#undef  crypt
#else
#include <openssl/x509.h>
#endif
#include <openssl/ssl.h>
#include <openssl/pem.h>
#include <openssl/err.h>

#include "securesocket.hpp"

#define ESP_FACTORY DECL_EXPORT

IThreadPool* http_thread_pool;
CHttpThreadPoolFactory* http_pool_factory;

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
                http_thread_pool = createThreadPool("Http Thread", http_pool_factory, NULL, m_maxConcurrentThreads, INFINITE);
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

bool CHttpProtocol::notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler)
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
                    void ** holder = new void*[6];
                    holder[0] = (void*)(accepted.getLink());
                    holder[1] = (void*)apport;
                    int maxEntityLength = getMaxRequestEntityLength();
                    holder[2] = (void*)&maxEntityLength;
                    bool useSSL = false;
                    holder[3] = (void*)&useSSL;
                    ISecureSocketContext* ctx = NULL;
                    holder[4] = (void*)ctx;
                    holder[5] = (void*)persistentHandler;
                    try
                    {
                        http_thread_pool->start((void*)holder, "", m_threadCreateTimeout > 0?m_threadCreateTimeout*1000:0);
                    }
                    catch(...)
                    {
                        IERRLOG("Error starting thread from http thread pool.");
                        if(accepted.get())
                        {
                            accepted->close();
                            //Assumption here is that if start() throws exception, that means the new 
                            //thread hasn't been started, so there's no other thread holding a link.
                            CInterface* ci = dynamic_cast<CInterface*>(accepted.get());
                            if(ci && ci->IsShared())
                                accepted->Release();
                        }
                        delete [] holder;
                        throw;
                    }
                    delete [] holder;
                }
                else
                {
                    /* create one thread per request */
                    CHttpThread *workthread = new CHttpThread(accepted.getLink(), apport, CEspProtocol::getViewConfig(), false, nullptr, persistentHandler);
                    workthread->setMaxRequestEntityLength(getMaxRequestEntityLength());
                    workthread->start();
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

        //ensure keys are specified. Passphrase is optional
        StringBuffer sb;
        cfg->getProp("certificate", sb);
        if(sb.length() == 0)
        {
            throw MakeStringException(-1, "certificate file not specified in config file");
        }

        cfg->getProp("privatekey", sb.clear());
        if(sb.length() == 0)
        {
            throw MakeStringException(-1, "private key file not specified in config file");
        }

        createSecureSocketContextEx2_t xproc = NULL;
        IEspPlugin *pplg = loadPlugin(SSLIB);
        if (pplg)
            xproc = (createSecureSocketContextEx2_t) pplg->getProcAddress("createSecureSocketContextEx2");
        else
            throw MakeStringException(-1, "dll/shared-object %s can't be loaded", SSLIB);


        if (xproc)
            m_ssctx.setown(xproc(cfg, ServerSocket));
        else
            throw MakeStringException(-1, "procedure createSecureSocketContextEx2 can't be loaded");
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
                http_thread_pool = createThreadPool("Http Thread", http_pool_factory, NULL, m_maxConcurrentThreads, INFINITE);
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

bool CSecureHttpProtocol::notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler)
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
                DBGLOG("HTTPS connection from %s:%d on %s socket", peername, port, persistentHandler?"persistent":"new");
                if(m_ssctx != NULL)
                {
                    if(m_maxConcurrentThreads > 0)
                    {
                        // Using Threading pool instead of generating one thread per request.
                        void ** holder = new void*[6];
                        holder[0] = (void*)accepted.getLink();
                        holder[1] = (void*)apport;
                        int maxEntityLength = getMaxRequestEntityLength();
                        holder[2] = (void*)&maxEntityLength;
                        bool useSSL = true;
                        holder[3] = (void*)&useSSL;
                        holder[4] = (void*)m_ssctx.get();
                        holder[5] = (void*)persistentHandler;
                        http_thread_pool->start((void*)holder);
                        delete [] holder;
                    }
                    else
                    {
                        /* create one thread per request */
                        CHttpThread *workthread = new CHttpThread(accepted.getLink(), apport, CEspProtocol::getViewConfig(), true, m_ssctx.get(), persistentHandler);
                        workthread->setMaxRequestEntityLength(getMaxRequestEntityLength());
                        workthread->start();
                        ESPLOG(LogMax, "Request processing thread started.");
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
CHttpThread::CHttpThread(bool viewConfig) : 
   CEspProtocolThread("Http Thread")
{
    m_viewConfig = viewConfig;
    m_is_ssl = false;
    m_ssctx = NULL;
}

CHttpThread::CHttpThread(ISocket *sock, bool viewConfig) : 
   CEspProtocolThread(sock, "HTTP Thread")
{
    m_viewConfig = viewConfig;
    m_is_ssl = false;
    m_ssctx = NULL;
}

CHttpThread::CHttpThread(ISocket *sock, CEspApplicationPort* apport, bool viewConfig, bool isSSL, ISecureSocketContext* ssctx,  IPersistentHandler* persistentHandler) :
   CEspProtocolThread(sock, "HTTP Thread"), m_persistentHandler(persistentHandler)
{
    m_viewConfig = viewConfig;
    m_apport = apport;
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
        secure_sock.setown(m_ssctx->createSecureSocket(m_socket.getLink(), getEspLogLevel()));
        int res = 0;
        try
        {
            ESPLOG(LogMax, "Accepting from secure socket");
            res = secure_sock->secure_accept();
            if(res < 0)
            {
                ESPLOG(LogMin, "Error accepting from secure socket");
                return false;
            }
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
        httpserver.setown(new CEspHttpServer(*secure_sock.get(), m_apport, m_viewConfig, getMaxRequestEntityLength()));
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, m_apport, m_viewConfig, getMaxRequestEntityLength()));
    }

    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);

    httpserver->setIsSSL(m_is_ssl);
    httpserver->processRequest();

    if (m_persistentHandler == nullptr)
    {
        keepAlive = m_apport->queryProtocol()->persistentEnabled() && httpserver->persistentEligible();
        if (keepAlive)
            m_apport->queryProtocol()->addPersistent(m_socket.get());
    }
    else
    {
        keepAlive = httpserver->persistentEligible();
        m_persistentHandler->doneUsing(m_socket, keepAlive);
    }
    clearThreadLocal();

    return false;
}

/**************************************************************************
 *  CPooledHttpThread Implementation                                      *
 **************************************************************************/
void CPooledHttpThread::init(void *param)
{
    m_socket.setown((ISocket*)(((void**)param)[0]));
    m_apport = (CEspApplicationPort*)(((void**)param)[1]);
    m_MaxRequestEntityLength = *(int*)(((void**)param)[2]);
    m_is_ssl = *(bool*)(((void**)param)[3]);
    m_ssctx = (ISecureSocketContext*)(((void**)param)[4]);
    m_persistentHandler = (IPersistentHandler*)(((void**)param)[5]);
}

CPooledHttpThread::~CPooledHttpThread()
{
}

void CPooledHttpThread::threadmain()
{
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
        httpserver.setown(new CEspHttpServer(*m_socket, m_apport, false, getMaxRequestEntityLength()));
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, m_apport, false, getMaxRequestEntityLength()));
    }

    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);
    bool keepAlive = false;
    try
    {
        ESP_TIME_SECTION("CPooledHttpThread::threadmain: httpserver->processRequest()");
        httpserver->processRequest();
        if (m_persistentHandler == nullptr)
        {
            keepAlive = m_apport->queryProtocol()->persistentEnabled() && httpserver->persistentEligible();
            if (keepAlive)
                m_apport->queryProtocol()->addPersistent(m_socket.get());
        }
        else
        {
            keepAlive = httpserver->persistentEligible();
            m_persistentHandler->doneUsing(m_socket, keepAlive);
        }
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) in CPooledHttpThread::threadmain().", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        IERRLOG("General Exception - in CPooledHttpThread::threadmain().");
    }
    clearThreadLocal();

    try
    {
        if (m_socket != nullptr)
        {
            if (!keepAlive)
                m_socket->shutdown(SHUTDOWN_WRITE);
            m_socket.clear();
        }
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
