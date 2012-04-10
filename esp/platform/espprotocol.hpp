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

#ifndef _ESPPROTOCOL_HPP__
#define _ESPPROTOCOL_HPP__

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "xslprocessor.hpp"

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

class CEspBindingEntry : public CInterface
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


//MAKEPointerArray(CEspBindingEntry*, CEspBindingArray);

class CEspApplicationPort
{
    CEspBindingEntry* bindings[100];
    int bindingCount;
    int defBinding;

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
public:
    CEspApplicationPort(bool viewcfg);

    ~CEspApplicationPort()
    {
        while (bindingCount)
            bindings[--bindingCount]->Release();
    }

    const StringBuffer &getAppFrameHtml(time_t &modified, const char *inner_url, StringBuffer &html, IEspContext* ctx);
    const StringBuffer &getTitleBarHtml(IEspContext& ctx, bool rawXml);
    const StringBuffer &getNavBarContent(IEspContext &context, StringBuffer &content, StringBuffer &contentType, bool xml);
    const StringBuffer &getDynNavData(IEspContext &context, IProperties *params, StringBuffer &content, 
                                      StringBuffer &contentType, bool& bVolatile);
    void buildNavTreeXML(IPropertyTree* navtree, StringBuffer& xmlBuf, bool insideFolder = false);

    int onGetNavEvent(IEspContext &context, IHttpMessage* request, IHttpMessage* response);
    int onBuildSoapRequest(IEspContext &context, IHttpMessage* request, IHttpMessage* response);

    int getBindingCount(){return bindingCount;}
    void appendBinding(CEspBindingEntry* entry, bool isdefault);

    bool rootAuthRequired(){return rootAuth;}

    CEspBindingEntry* queryBindingItem(int item){return (item<bindingCount) ? bindings[item] : NULL;}
    CEspBindingEntry* getDefaultBinding(){return bindings[(defBinding>=0) ? defBinding : 0];}
#ifdef _USE_OPENLDAP
    unsigned updatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& message);
    void onUpdatePasswordInput(IEspContext &context, StringBuffer &html);
    void onUpdatePassword(IEspContext &context, IHttpMessage* request, StringBuffer& html);
#endif
};

typedef map<int, CEspApplicationPort*> CApplicationPortMap;

#define DEFAULT_MAX_REQUEST_ENTITY_LENGTH 8000000

class CEspProtocol : public CInterface,
    implements IEspProtocol,
    implements ISocketSelectNotify
{
private:
    //map between socket port and one or more bindings
    CApplicationPortMap m_portmap;
    bool m_viewConfig;
    int m_MaxRequestEntityLength;
    IEspContainer *m_container;

public:
    IMPLEMENT_IINTERFACE;

    void beforeDispose()
    {
    }

    CEspProtocol();
    virtual ~CEspProtocol();

    void clear()
    {
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

    //IEspProtocol
    virtual const char * getProtocolName();

    virtual void addBindingMap(ISocket *sock, IEspRpcBinding* binding, bool isdefault);
    virtual CEspApplicationPort* queryApplicationPort(int handle);

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;};
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }
    virtual void setContainer(IEspContainer* container) { m_container = container; }
};

#endif
