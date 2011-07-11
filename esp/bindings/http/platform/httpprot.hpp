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

#ifndef __HTTPPROT_HPP_
#define __HTTPPROT_HPP_

#ifndef esp_http_decl
    #define esp_http_decl
#endif

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

class CPooledHttpThread : public CInterface, implements IPooledThread
{
private:
    Owned<ISocket> m_socket;
    CEspApplicationPort* m_apport;
    StringAttr m_context;
    int m_MaxRequestEntityLength;
    bool m_is_ssl;
    ISecureSocketContext* m_ssctx;

public:
    IMPLEMENT_IINTERFACE;

    virtual ~CPooledHttpThread();
    virtual void init(void *param);
    virtual void main();
    virtual bool stop()
    {
        return true;
    }
    virtual bool canReuse() { return true; }

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }
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

public:
    IMPLEMENT_IINTERFACE;

    CHttpThread(ISocket *sock, CEspApplicationPort* apport, bool viewConfig, bool isSSL = false, ISecureSocketContext* ssctx = NULL);
    
    virtual ~CHttpThread();
    virtual bool onRequest();

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }
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

    virtual bool notifySelected(ISocket *sock,unsigned selected);
//IEspProtocol
    virtual const char * getProtocolName();
};

class esp_http_decl CSecureHttpProtocol : public CEspProtocol
{
private:
    StringBuffer m_certfile;
    StringBuffer m_privkeyfile;
    StringBuffer m_passphrase;
    Owned<ISecureSocketContext> m_ssctx;

    Owned<IPropertyTree> m_config;
    int m_maxConcurrentThreads;

public:
    CSecureHttpProtocol(IPropertyTree *cfg);
    virtual ~CSecureHttpProtocol();

    virtual void init(IPropertyTree * cfg, const char * process, const char * protocol);

    virtual bool notifySelected(ISocket *sock,unsigned selected);
//IEspProtocol
    virtual const char * getProtocolName();
};

#endif //__HTTPPROT_HPP_

