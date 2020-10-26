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

#ifndef _ESPPROTOCOL_HPP__
#define _ESPPROTOCOL_HPP__

#include "esphttp.hpp"

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "xslprocessor.hpp"
#include "persistent.hpp"

//STL
#include <algorithm>
#include <string>
#include <map>
using namespace std;

class ActiveRequests
{
public:

    ActiveRequests() { inc(); }
    ~ActiveRequests()  { dec(); }

    void inc();
    void dec();

    static long getCount();
};

class CEspBindingEntry : public CInterface, implements IInterface
{
private:
    Owned<ISocket> sock_;
    Owned<IEspRpcBinding> binding_;

public:
   IMPLEMENT_IINTERFACE;

   CEspBindingEntry(ISocket *sock, IEspRpcBinding *binding)
    {
        sock->Link();
        sock_.set(sock);
        binding_.set(binding);
    }

    virtual ~CEspBindingEntry()
    {
        sock_.clear();
        binding_.clear();
    }

    unsigned getId()
    {
        if (sock_) 
            return sock_->OShandle();
        return 0;
    }

    IEspRpcBinding *queryBinding()
    {
        return binding_.get();
    }
};

class CEspProtocol;

class CEspApplicationPort
{
    IArrayOf<CEspBindingEntry> bindings;
    CEspBindingEntry* defBinding = nullptr;

    StringBuffer titleBarHtml;
    StringBuffer appFrameHtml;
    const char *build_ver;
    bool viewConfig;
    bool rootAuth;
    bool navResize;
    bool navScroll;
    int navWidth;

    HINSTANCE hxsl;
    Owned<IXslProcessor> xslp;
    CEspProtocol* protocol = nullptr;
    ReadWriteLock rwLock;
public:
    CEspApplicationPort(bool viewcfg, CEspProtocol* prot);

    ~CEspApplicationPort()
    {
        if (hxsl)
            FreeSharedObject(hxsl);
    }

    const StringBuffer &getAppFrameHtml(time_t &modified, const char *inner_url, StringBuffer &html, IEspContext* ctx);
    const StringBuffer &getTitleBarHtml(IEspContext& ctx, bool rawXml);
    const StringBuffer &getNavBarContent(IEspContext &context, StringBuffer &content, StringBuffer &contentType, bool xml);
    const StringBuffer &getDynNavData(IEspContext &context, IProperties *params, StringBuffer &content, 
                                      StringBuffer &contentType, bool& bVolatile);
    void buildNavTreeXML(IPropertyTree* navtree, StringBuffer& xmlBuf, bool insideFolder = false);

    int onGetNavEvent(IEspContext &context, IHttpMessage* request, IHttpMessage* response);
    int onBuildSoapRequest(IEspContext &context, IHttpMessage* request, IHttpMessage* response);

    int getBindingCount(){return bindings.length();}
    void appendBinding(CEspBindingEntry* entry, bool isdefault);
    void removeBinding(IEspRpcBinding* binding);

    bool rootAuthRequired(){return rootAuth;}

    CEspBindingEntry* queryBindingItem(int item)
    {
        ReadLockBlock rblock(rwLock);
        return (item<bindings.length()) ? &bindings.item(item) : nullptr;
    }
    CEspBindingEntry* getDefaultBinding()
    {
        ReadLockBlock rblock(rwLock);
        if (defBinding)
            return defBinding;
        if (bindings.length() > 0)
            return &bindings.item(0);
        return nullptr;
    }
    CEspProtocol* queryProtocol() { return protocol; }
#ifdef _USE_OPENLDAP
    unsigned updatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& message);
    void onUpdatePasswordInput(IEspContext &context, StringBuffer &html);
    unsigned onUpdatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& html);
#endif
};

typedef map<int, CEspApplicationPort*> CApplicationPortMap;

#define DEFAULT_MAX_REQUEST_ENTITY_LENGTH 8000000

class esp_http_decl CEspProtocol : public CInterface,
    implements IEspProtocol,
    implements ISocketSelectNotify,
    implements IPersistentSelectNotify
{
private:
    //map between socket port and one or more bindings
    CApplicationPortMap m_portmap;
    bool m_viewConfig;
    int m_MaxRequestEntityLength;
    IEspContainer *m_container = nullptr;
    Owned<IPersistentHandler> m_persistentHandler;
    ReadWriteLock rwLock;

public:
    IMPLEMENT_IINTERFACE;

    void beforeDispose()
    {
    }

    CEspProtocol();
    virtual ~CEspProtocol();

    void clear()
    {
        WriteLockBlock wblock(rwLock);
        map<int, CEspApplicationPort*>::iterator bndi = m_portmap.begin();
        for(;bndi!=m_portmap.end();bndi++)
            if(bndi->second)
                delete bndi->second;
        m_portmap.clear();
    }

    void clearBindingMap()
    {
        clear();
    }

    void setViewConfig(bool viewConfig){m_viewConfig=viewConfig;}
    bool getViewConfig(){return m_viewConfig;}

    virtual bool notifySelected(ISocket *sock,unsigned selected);
    virtual bool notifySelected(ISocket *sock,unsigned selected, IPersistentHandler* persistentHandler, bool shouldClose) override { return false; };

    //IEspProtocol
    virtual const char * getProtocolName();

    virtual void addBindingMap(ISocket *sock, IEspRpcBinding* binding, bool isdefault);
    virtual int removeBindingMap(int port, IEspRpcBinding* binding);
    virtual CEspApplicationPort* queryApplicationPort(int handle);

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;};
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }
    virtual void setContainer(IEspContainer* container) { m_container = container; }
    virtual void initPersistentHandler(IPropertyTree * proc_cfg);
    virtual bool persistentEnabled() { return m_persistentHandler != nullptr; }
    virtual void addPersistent(ISocket* sock);

    virtual int countBindings(int port);
};

esp_http_decl bool checkEspConnection(IEspContext& ctx);

#endif
