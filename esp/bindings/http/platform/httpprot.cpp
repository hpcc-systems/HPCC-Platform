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

#pragma warning (disable : 4786)

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

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

#ifdef WIN32
#define ESP_FACTORY __declspec(dllexport)
#else
#define ESP_FACTORY
#endif

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


bool CHttpProtocol::notifySelected(ISocket *sock,unsigned selected)
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
            Owned<ISocket> accepted = sock->accept();
            if (accepted.get() != NULL)
            {
                char peername[256];
                int port = accepted->peer_name(peername, 256);

    #if defined(_DEBUG)
                DBGLOG("HTTP connection from %s:%d", peername, port);
    #endif          

                if(m_maxConcurrentThreads > 0)
                {
                    // Using Threading pool instead of generating one thread per request.
                    void ** holder = new void*[5];
                    holder[0] = (void*)(accepted.getLink());
                    holder[1] = (void*)apport;
                    int maxEntityLength = getMaxRequestEntityLength();
                    holder[2] = (void*)&maxEntityLength;
                    bool useSSL = false;
                    holder[3] = (void*)&useSSL;
                    ISecureSocketContext* ctx = NULL;
                    holder[4] = (void*)ctx;
                    try
                    {
                        http_thread_pool->start((void*)holder, "", m_threadCreateTimeout > 0?m_threadCreateTimeout*1000:0);
                    }
                    catch(...)
                    {
                        ERRLOG("Error starting thread from http thread pool.");
                        if(accepted.get())
                        {
                            accepted->close();
                            //Assumption here is that if start() throws exception, that means the new 
                            //thread hasn't been started, so there's no other thread holding a link.
                            CInterface* ci = dynamic_cast<CInterface*>(accepted.get());
                            if(ci && ci->IsShared())
                                accepted->Release();
                        }
                        delete holder;
                        throw;
                    }
                    delete holder;
                }
                else
                {
                    /* create one thread per request */
                    CHttpThread *workthread = new CHttpThread(accepted.getLink(), apport, CEspProtocol::getViewConfig());
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
        ERRLOG("Exception(%d, %s) in CHttpProtocol::notifySelected()", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown Exception in CHttpProtocol::notifySelected()");
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
        cfg->getProp("certificate", m_certfile);
        if(m_certfile.length() == 0)
        {
            throw MakeStringException(-1, "certificate file not specified in config file");
        }
        cfg->getProp("privatekey", m_privkeyfile);
        if(m_privkeyfile.length() == 0)
        {
            throw MakeStringException(-1, "private key file not specified in config file");
        }
        StringBuffer pphrase;
        cfg->getProp("passphrase", pphrase);
        if(pphrase.length() == 0)
        {
            throw MakeStringException(-1, "passphrase not specified in config file");
        }

        //m_ssctx.setown(createSecureSocketContextEx(m_certfile.str(), m_privkeyfile.str(), m_passphrase.str(), ServerSocket));
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

bool CSecureHttpProtocol::notifySelected(ISocket *sock,unsigned selected)
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
            ISocket *accepted = sock->accept();
            if (accepted!=NULL)
            {
                char peername[256];
                int port = accepted->peer_name(peername, 256);
                DBGLOG("HTTPS connection from %s:%d", peername, port);
                if(m_ssctx != NULL)
                {
                    if(m_maxConcurrentThreads > 0)
                    {
                        // Using Threading pool instead of generating one thread per request.
                        void ** holder = new void*[5];
                        holder[0] = (void*)accepted;
                        holder[1] = (void*)apport;
                        int maxEntityLength = getMaxRequestEntityLength();
                        holder[2] = (void*)&maxEntityLength;
                        bool useSSL = true;
                        holder[3] = (void*)&useSSL;
                        holder[4] = (void*)m_ssctx.get();
                        http_thread_pool->start((void*)holder);
                        delete holder;
                    }
                    else
                    {
                        /* create one thread per request */
                        CHttpThread *workthread = new CHttpThread(accepted, apport, CEspProtocol::getViewConfig(), true, m_ssctx.get());
                        workthread->setMaxRequestEntityLength(getMaxRequestEntityLength());
                        workthread->start();
                        DBGLOG("Request processing thread started.");
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
        ERRLOG("Exception(%d, %s) in CSecureHttpProtocol::notifySelected()", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Unknown Exception in CSecureHttpProtocol::notifySelected()");
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

CHttpThread::CHttpThread(ISocket *sock, CEspApplicationPort* apport, bool viewConfig, bool isSSL, ISecureSocketContext* ssctx) : 
   CEspProtocolThread(sock, "HTTP Thread") 
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
    ActiveRequests recording;

    Owned<CEspHttpServer> httpserver;
    
    Owned<ISecureSocket> secure_sock;
    if(m_is_ssl && m_ssctx)
    {
        DBGLOG("Creating secure socket");
        secure_sock.setown(m_ssctx->createSecureSocket(m_socket.getLink(), getEspLogLevel()));
        int res = 0;
        try
        {
            DBGLOG("Accepting from secure socket");
            res = secure_sock->secure_accept();
            if(res < 0)
            {
                DBGLOG("Error accepting from secure socket");
                return false;
            }
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            DBGLOG("%s", emsg.str());
            return false;
        }
        catch(...)
        {
            DBGLOG("Unknown exception accepting from secure socket");
            return false;
        }
        DBGLOG("Accepted from secure socket");
        httpserver.setown(new CEspHttpServer(*secure_sock.get(), m_apport, m_viewConfig, getMaxRequestEntityLength()));
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, m_apport, m_viewConfig, getMaxRequestEntityLength()));
    }

    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);

    httpserver->processRequest();
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
}

CPooledHttpThread::~CPooledHttpThread()
{
}

void CPooledHttpThread::main()
{
    TimeSection timing("CPooledHttpThread::main()");
    Owned<CEspHttpServer> httpserver;
    
    Owned<ISecureSocket> secure_sock;
    if(m_is_ssl && m_ssctx)
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
            DBGLOG("%s", emsg.str());
            return;
        }
        catch(...)
        {
            return;
        }
        httpserver.setown(new CEspHttpServer(*secure_sock.get(), m_apport, false, getMaxRequestEntityLength()));
    }
    else
    {
        httpserver.setown(new CEspHttpServer(*m_socket, m_apport, false, getMaxRequestEntityLength()));
    }

    time_t t = time(NULL);  
    initThreadLocal(sizeof(t), &t);
    try
    {
        ESP_TIME_SECTION("CPooledHttpThread::main: httpserver->processRequest()");
        httpserver->processRequest();
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        ERRLOG("Exception(%d, %s) in CPooledHttpThread::main().", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("General Exception - in CPooledHttpThread::main().");
    }
    clearThreadLocal();

    try
    {
        if(m_socket != NULL)
        {
            m_socket->shutdown(SHUTDOWN_WRITE);
            m_socket.clear();
        }
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        ERRLOG("Exception(%d, %s) - CPooledHttpThread::main(), closing socket.", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
    }
    catch(...)
    {
        ERRLOG("General Exception - CPooledHttpThread::main(), closing socket.");
    }

}

