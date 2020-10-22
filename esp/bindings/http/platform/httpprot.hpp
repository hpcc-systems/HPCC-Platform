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

#ifndef __HTTPPROT_HPP_
#define __HTTPPROT_HPP_

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
//#include "IAEsp.hpp"

//SCM Core
#include "espthread.hpp"

#include "espprotocol.hpp"
#include "http/platform/httpbinding.hpp"
#include "securesocket.hpp"

//STL
#include <algorithm>
#include <string>
#include <map>
using namespace std;

class CPooledHttpThread : public CInterface, implements IPooledThread, implements ISocketReturner
{
private:
    Owned<ISocket> m_socket;
    CEspApplicationPort* m_apport;
    StringAttr m_context;
    int m_MaxRequestEntityLength;
    bool m_is_ssl;
    ISecureSocketContext* m_ssctx;
    IPersistentHandler* m_persistentHandler = nullptr;
    bool m_shouldClose = false;
    IHttpServerService* m_httpserver = nullptr;
    bool m_socketReturned = false;
    bool m_processAborted = false;
public:
    IMPLEMENT_IINTERFACE;

    virtual ~CPooledHttpThread();
    virtual void init(void *param) override;
    virtual void threadmain() override;
    virtual bool stop() override
    {
        return true;
    }
    virtual bool canReuse() const override { return true; }

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }
    virtual void returnSocket();
};

class CHttpThreadPoolFactory : public CInterface, implements IThreadFactory
{
public:
    IMPLEMENT_IINTERFACE;

    virtual IPooledThread *createNew()
    {
        return new CPooledHttpThread();
    }
};


class CHttpThread: public CEspProtocolThread
{
private:
    CEspApplicationPort* m_apport;
    bool m_viewConfig;
    StringAttr m_context;
    int m_MaxRequestEntityLength;
    CHttpThread(bool viewConfig);
    CHttpThread(ISocket *sock, bool viewConfig);
    bool m_is_ssl;
    ISecureSocketContext* m_ssctx;
    IPersistentHandler* m_persistentHandler = nullptr;
    bool m_shouldClose = false;
    IHttpServerService* m_httpserver = nullptr;
    bool m_httpSocketReturned = false;
    void returnSocket(bool cascade);

public:
    CHttpThread(ISocket *sock, CEspApplicationPort* apport, bool viewConfig, bool isSSL = false, ISecureSocketContext* ssctx = NULL, IPersistentHandler* persistentHandler = NULL);
    
    virtual ~CHttpThread();
    virtual bool onRequest();

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }

    void setShouldClose(bool should) {m_shouldClose = should;}
    virtual void returnSocket();
};

class esp_http_decl CHttpProtocol : public CEspProtocol
{
private:
    int m_maxConcurrentThreads;
    int m_threadCreateTimeout;
public:
    CHttpProtocol();
    virtual ~CHttpProtocol();

    virtual void Link(void) const
    {
        CEspProtocol::Link(); 
    }
    
    virtual bool Release(void) const
    {
        return CEspProtocol::Release();
    }

    virtual void init(IPropertyTree * cfg, const char * process, const char * protocol);

    virtual bool notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler, bool shouldClose) override;
//IEspProtocol
    virtual const char * getProtocolName();
};

class esp_http_decl CSecureHttpProtocol : public CEspProtocol
{
private:
    Owned<ISecureSocketContext> m_ssctx;

    Owned<IPropertyTree> m_config;
    int m_maxConcurrentThreads;

public:
    CSecureHttpProtocol(IPropertyTree *cfg);
    virtual ~CSecureHttpProtocol();

    virtual void init(IPropertyTree * cfg, const char * process, const char * protocol);

    virtual bool notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler, bool shouldClose) override;
//IEspProtocol
    virtual const char * getProtocolName();
};

extern "C" {

esp_http_decl IEspProtocol * http_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process);

};

#endif //__HTTPPROT_HPP_

