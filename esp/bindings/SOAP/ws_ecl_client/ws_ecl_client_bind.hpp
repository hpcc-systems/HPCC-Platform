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

// Copyright (C) 2001 Seisint, Inc.
// All rights reserved.
#ifndef _WS_ECL_CLIENT_BIND_HPP__
#define _WS_ECL_CLIENT_BIND_HPP__


//JLib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "soapesp.hpp"
#include "ws_ecl_client.hpp"

//ESP Bindings
#include "soapmessage.hpp"
#include "soapmacro.hpp"
#include "soapservice.hpp"
#include "soapparam.hpp"
#include "soapclient.hpp"

#include "espcontext.hpp"
#include "http/client/httpclient.hpp"

#include "edwin.h"

class CClientWsEclResponse : implements IClientWsEclResp, public CInterface
{
private:
    SoapAttachParam<StringBuffer> m_ResultsXML;
    StringBuffer m_soapMessage;
    StringBuffer m_HttpMessage;
    StringBuffer m_StatusMessage;
    
    WsEclClientRequestState m_state;
    Semaphore m_semSink;
    Owned<IClientWsEclEvents> m_eventSink;
    
    unsigned long m_client_value;
    unsigned long m_request_id;
    
public:
    IMPLEMENT_IINTERFACE;
    
    CClientWsEclResponse(unsigned long cv=0, unsigned long reqId=0) : 
    m_client_value(cv), m_request_id(reqId), m_ResultsXML(nilIgnore)
    {
    }
    
    //interface IClientWsEclResp
    unsigned long getRequestId(){return m_request_id;}
    unsigned long getClientValue(){return m_client_value;}
    
    WsEclClientRequestState getRequestState(){return m_state;}  
    const char * getResultsXML() { return m_ResultsXML->str(); }
    const char* getSoapMessage() { return m_soapMessage; }
        
    //Functions without interfaces
    void setRequestId(unsigned long reqId){m_request_id = reqId;}
    void setClientValue(unsigned long cv){m_client_value = cv;}
    
    void setHttpMessage(StringBuffer message){message.swapWith(m_HttpMessage);}
    const char* getHttpMessage(StringBuffer &message)
    {
        message.append(m_HttpMessage);

        if (m_HttpMessage.length())
            return m_HttpMessage.str();
        else
            return NULL;
    }

    void setStatusMessage(StringBuffer message){message.swapWith(m_StatusMessage);}
    const char* getStatusMessage(StringBuffer &message)
    {
        message.append(m_StatusMessage);

        if (m_StatusMessage.length())
            return m_StatusMessage.str();
        else
            return NULL;
    }

    virtual void unserialize(IRpcMessage& rpc_response)
    {
        m_ResultsXML.unmarshall(rpc_response, "Results");
    }
    
    virtual void unserialize(StringBuffer &value)
    {
        if(value.length() < 19)
            return;
        
        const char* sptr = strstr(value.str(), "<Results>");
        if(sptr)
        {
            sptr += 9;
            const char* eptr = NULL;
            const char* ptr = value.str() + (value.length() - 11);
            while(ptr > sptr)
            {
                if(strncmp(ptr, "</Results>", 10) == 0)
                {
                    eptr = ptr;
                    break;
                }
                ptr--;
            }
            if(eptr)
            {
                StringBuffer buf;
                buf.append(eptr - sptr, sptr);
                m_ResultsXML = buf;
            }
        }   

        // find namespace
        StringBuffer soapNS;
        sptr = strstr(value,":Envelope");
        if (sptr)
        {
            const char* start = sptr;
            while (start>value.str() && *start!='<') 
                start--;
            if (*start == '<')
                soapNS.append(sptr-start-1,start+1);
            else if (strstr(value,"<Envelope") == NULL)
                UERRLOG("Parsing soap namespace failed");
        }

        // save soap body
        VStringBuffer body("<%s%sBody", soapNS.str(), soapNS.length() ? ":" : "");
        sptr = strstr(value.str(), body);
        if (sptr)
        {
            sptr += body.length();
            while (*sptr && *sptr!='>')
                sptr++; 
            if (*sptr!='>') {
                UWARNLOG("Parsing soap message error: could not find ending > for Body");
                return;
            }
            sptr++; // skip '>'

            body.insert(1,"/").append(">");
            const char* eptr = strstr(sptr, body);
            if (eptr)
                m_soapMessage.clear().append(eptr-sptr, sptr);
        }
    }
    
    virtual void setEvents(IClientWsEclEvents *eventSink)
    {
        m_eventSink.set(eventSink);
    }
    
    void setRequestState(WsEclClientRequestState state)
    {
        m_state = state;
    }
    
    int Notify()
    {
        m_semSink.wait();
        if (m_eventSink != NULL)
        {
            if(m_state == ESP_CLIENT_REQUEST_OK)
                m_eventSink->onComplete(*this);
            else
                m_eventSink->onError(*this);
        }
        m_semSink.signal();
        return 0;
    }
    
};

class EclClientStringArray
{
    StringBuffer m_name;
    StringArray m_array;
    
public:
    EclClientStringArray(const char *name, const StringArray &array)
    {
        m_name.append(name);
        ForEachItemIn(idx, array)
            m_array.append(array.item(idx));
    }
    
    const char *getName(){return m_name;}
    
    StringArray &query(){return m_array;}
};


class CClientRequestNode : implements IClientRequestNode, public CInterface
{
private:
    StringAttr m_name;
    StringAttr m_value;

    Owned<IProperties> m_params;
    Owned<IProperties> m_attrs;
    PointerArray m_children;
    
    IProperties* queryParams() 
    {       
        if (!m_params)
            m_params.setown(createProperties());
        return m_params.get();
    }

public:
    IMPLEMENT_IINTERFACE;
    
    CClientRequestNode() {}

    CClientRequestNode(const char* name, const char* value=NULL) : m_name(name), m_value(value) {}

    ~CClientRequestNode()
    {
        ForEachItemIn(idx, m_children)
        {
            CClientRequestNode *item = (CClientRequestNode *)m_children.item(idx);
            delete item;
        }
    }
    
    virtual void addTag(const char * name, const char * value)
    {
        if (name && value)
        {
            StringBuffer encval;
            encodeUtf8XML(value, encval);
            queryParams()->setProp(name, encval.str());
        }
    }
    
    virtual void setTag(const char * name, const char * value)
    {
        if (name && value)
        {
            StringBuffer encval;
            encodeUtf8XML(value, encval);
            if(queryParams()->hasProp(name)==true)
                queryParams()->removeProp(name);
            queryParams()->setProp(name, encval.str());
        }
    }

    virtual bool hasTag(const char * name)
    {
        if (!m_params)
            return false;
        return m_params->hasProp(name);
    }
        
    virtual void addIntTag(const char * name, int value)
    {
        queryParams()->setProp(name, value);
    }
    
    virtual void addAttr(const char* name, const char* value)
    {
        if (!m_attrs)
            m_attrs.setown(createProperties());
        m_attrs->setProp(name, value);
    }

    virtual IClientRequestNode& addChild(const char* name)
    {
        CClientRequestNode* node = new CClientRequestNode(name);
        m_children.append(node);
        return *node;
    }

    virtual IClientRequestNode& addChild(const char* name, const char* value)
    {
        CClientRequestNode* node = new CClientRequestNode(name, value);
        m_children.append(node);
        return *node;
    }   

    virtual void addDataset(const char * name, const char * ds)
    {
        queryParams()->setProp(name, ds);
    }

    void serializeChildren(IRpcMessageArray& msgarray)
    {
        ForEachItemIn(idx, m_children)
        {
            CClientRequestNode* child = (CClientRequestNode*)m_children.item(idx);
            CRpcMessage *msg = new CRpcMessage(child->m_name.get());
            child->serialize(*msg, NULL, true);
            msgarray.append(*msg);
        }

    }

    void serialize(IRpcMessage& rpcMsg, const char* path, bool top_attrs=false)
    {
        // attributes
        if (path && *path)//path includes name in it
        {
            if (m_attrs)
                rpcMsg.add_value(path, "", m_value.get(), *m_attrs.get());
            else if (m_value.length())
                rpcMsg.add_value(path, "", "", "", m_value.get());
        }
        else
        {
            if (m_attrs && top_attrs)
                rpcMsg.add_attr(NULL, NULL, NULL, *m_attrs.get());
            else if (m_attrs && m_name.length())
                rpcMsg.add_value(path, m_name.get(), m_value.get(), *m_attrs.get());
            else if (m_value.length())
                rpcMsg.add_value(path, "", m_name.get(), "", m_value.get());
        }
            
        if (m_params || m_children.length())
        {
            if (m_params)
            {
                Owned<IPropertyIterator> piter = m_params->getIterator();       
                ForEach(*piter)
                {
                    const char *propkey = piter->getPropKey();
                    rpcMsg.add_value(path, "", propkey, "", m_params->queryProp(propkey), false);
                }       
            }

            // child elements
            if (m_children.length())
            {
                Owned<IPropertyTree> pChildIndexMap = createPTree();
                ForEachItemIn(idx, m_children)
                {
                    CClientRequestNode* child = (CClientRequestNode *)m_children.item(idx);
                    const char* childName = child->m_name.get();

                    aindex_t index = 1;
                    IPropertyTree* pChildIndex = pChildIndexMap->queryPropTree( childName );
                    if (pChildIndex)
                    {
                        index = pChildIndex->getPropInt(NULL) + 1;
                        pChildIndex->setPropInt(NULL, index);
                    }
                    else
                        pChildIndexMap->addPropInt(childName, 1);

                    StringBuffer xpath;
                    if (path && *path)
                        xpath.append(path).append('/');
                    xpath.appendf("%s[%d]", childName, index);
                    child->serialize(rpcMsg, xpath);
                }
            }
        }
    }
};

class CClientWsEclRequest : implements IClientWsEclRequest, public CInterface
{
private:
    StringAttr m_method;
    StringAttr m_specialmethod;
    StringAttr m_url;
    StringAttr m_nsuri;
    StringAttr m_nsvar;
    StringAttr m_soapAction;
    bool m_noSecurityHeader;
    bool m_disableKeepAlive;
    
    unsigned long m_req_id;
    unsigned long m_client_value;
    
    Owned<IClientWsEclEvents> m_eventSink;
    Semaphore m_semWorkerThread;
    void *m_hThunk;
    
    PointerArray m_arrays;
    Owned<IProperties> m_attrs;

    CClientRequestNode m_headers;
    CClientRequestNode m_base;
    StringAttr m_serializedContent;
    StringBuffer m_itemTag;

public:
    IMPLEMENT_IINTERFACE;
    
    CClientWsEclRequest(const char *method) : 
            m_method(method), 
            m_noSecurityHeader(false), 
            m_disableKeepAlive(false), 
            m_nsvar("m"),
            m_itemTag("Item")
    { }
    
    virtual ~CClientWsEclRequest()
    {
        ForEachItemIn(idx, m_arrays)
        {
            EclClientStringArray *array = (EclClientStringArray *)m_arrays.item(idx);
            delete array;
        }
    }
        
    //interface IClientWsEclRequest
    virtual void setClientValue(unsigned long cv)  { m_client_value = cv;}
    virtual void setNamespace(const char* ns)   { m_nsuri.set(ns); }
    virtual void setNamespaceVar(const char* nsvar)   { m_nsvar.set(nsvar); }
    virtual void setSoapAction(const char* action) { m_soapAction.set(action); }
    virtual void setNoSecurityHeader(bool noHeader) {  m_noSecurityHeader = noHeader; }
    virtual void disableKeepAlive() { m_disableKeepAlive = true; }
    virtual const char* getSerializedContent()          { return m_serializedContent; }
    
    virtual void setSerializedContent(const char* c) { m_serializedContent.set(c); }

    virtual void appendSerializedContent(const char* content) 
    { 
        Owned<IPropertyTree> pTree = createPTreeFromXMLString(content);
        Owned<IPTreeIterator> it = pTree->getElements("*");
        // NOTE: this does not support hierarchy yet (which is sufficient for flat style roxie request).
        ForEach(*it)
        {
            IPTree* pNode = &it->query();
            addTag(pNode->queryName(),pNode->queryProp(NULL));
        }
    }
    
    virtual void setItemTag(const char * tag) {m_itemTag.set(tag); }
    virtual void addArray(const char * name, StringArray &value)
    {
        EclClientStringArray *array = new EclClientStringArray(name, value);
        m_arrays.append(array);
    }

    virtual void addDataset(const char * name, const char * ds)
    {
        m_base.addDataset(name, ds);
    }

    virtual void addTag(const char * name, const char * value)
    {
        m_base.addTag(name, value);
    }

    virtual void setTag(const char * name, const char * value)
    {
        m_base.setTag(name, value);
    }
    
    virtual void addAttr(const char * name, const char * value)
    {
        if (!m_attrs)
            m_attrs.setown(createProperties());
        m_attrs->setProp(name, value);
    }
    
    virtual void addIntTag(const char * name, int value)
    {
        m_base.addIntTag(name, value);
    }

    virtual bool hasTag(const char * name)
    {
        return m_base.hasTag(name);
    }
    
    virtual IClientRequestNode& addChild(const char* name)
    {
        return m_base.addChild(name);
    }

    virtual IClientRequestNode& addHeader(const char* name, const char *ns)
    {
        IClientRequestNode &header = m_headers.addChild(name);
        if (ns && *ns)
            header.addAttr("xmlns", ns);
        return header;
    }

    virtual IClientRequestNode& addChild(const char* name, const char* value)
    {
        return m_base.addChild(name, value);
    }

    virtual void setUrl(const char* url)
    {
        m_url.set(url);
    }
    
    //Functions not belonging to any interface
    void setRequestId(unsigned long reqId){ m_req_id = reqId; }
    
    virtual unsigned long getRequestId()
    {
        return m_req_id;
    }
        
    virtual void setEvents(IClientWsEclEvents *eventSink)
    {
        m_eventSink.set(eventSink);
    }
    
    virtual IClientWsEclEvents* getEvents()
    {
        return m_eventSink.get();
    }
    
    virtual void setSpecialMethod(const char* method)   { m_specialmethod.set(method); }
    virtual void serialize(IRpcMessage& rpc_request)
    {
        if (m_nsvar.length())
            rpc_request.set_ns(m_nsvar.get());

        if (m_specialmethod)
            rpc_request.set_name(m_specialmethod.get());
        else
            rpc_request.set_name(m_method.get());
        
        
        
        if (m_nsuri)
            rpc_request.set_nsuri(m_nsuri);
        else
        {
            StringBuffer nsuri("urn:hpccsystems:ws:");
            nsuri.appendLower(m_method.length(), m_method.str());
            rpc_request.set_nsuri(nsuri.str());
        }
        
        if (m_attrs)
        {
            rpc_request.add_attr(NULL, NULL, NULL, *m_attrs.get());
        }

        m_base.serialize(rpc_request, "");

        ForEachItemIn(idx, m_arrays)
        {
            EclClientStringArray* array = (EclClientStringArray *)m_arrays.item(idx);
            rpc_request.add_value("", "", array->getName(), "", m_itemTag, "", array->query());
        }

        const char* serializedContent = getSerializedContent();
        if (serializedContent && *serializedContent)
            rpc_request.setSerializedContent(serializedContent);
    }
    
    virtual void sendHttpRequest( CClientWsEclResponse& eclresponse, const char* method, const char* url, const char *user="", 
                                            const char *pw="", const char *realm="", const char* httpPostVariableName=NULL, 
                                            bool encodeHttpPostBody=false)
    {
        Owned<IHttpClientContext> httpctx = getHttpClientContext();
        Owned<IHttpClient> httpclient = httpctx->createHttpClient("", url);
        
        if(user && *user && pw && *pw)
        {
            httpclient->setUserID(user);
            httpclient->setPassword(pw);
        }
        if(realm && *realm)
        {
            httpclient->setRealm(realm);
        }

        StringBuffer request;
        const char* mimeType;
        if (method && !stricmp(method, "POST"))
        {
            StringBuffer soapAction;
            if (m_soapAction)
                soapAction = m_soapAction.get();
            else if (m_specialmethod)
                soapAction.clear().append(m_nsuri.get()).append("/").append(m_specialmethod.get());
            else
                soapAction = m_method.get();


            request.set( getSerializedContent() );
            if (request.length()==0)
            {
                Owned<IRpcMessage> rpcmsg = new CRpcMessage;
                serialize(*rpcmsg);
                rpcmsg->set_name(soapAction);

                rpcmsg->simple_marshall(request);
            }

            if (encodeHttpPostBody)
            {
                StringBuffer encoded;
                encodeXML(request, encoded, 0, request.length());
                request.swapWith( encoded);
            }
            if (httpPostVariableName && *httpPostVariableName)
            {
                StringBuffer temp(httpPostVariableName);
                temp.append('=');
                request.insert(0, temp.str());
                mimeType = "application/x-www-form-urlencoded";
            }
            else
                mimeType = "text/xml";
        }
        else
            mimeType = "text/html; charset=UTF-8";

        StringBuffer response;
        StringBuffer responseStatus;
        httpclient->sendRequest(method, mimeType, request, response, responseStatus, true);

        //TODO: get state from httpclient
        eclresponse.setRequestState(ESP_CLIENT_REQUEST_OK);

        if (response.length() > 0)
            eclresponse.setHttpMessage(response);
        if (responseStatus.length() > 0)
            eclresponse.setStatusMessage(responseStatus);
    }

    virtual void post(const char* url, CClientWsEclResponse& response, const char *user="", const char *pw="", const char *realm="")
    {
        Owned<CRpcCall> rpccall;
        Owned<CRpcResponse> rpcresponse;
        
        rpccall.setown(new CRpcCall);
        rpcresponse.setown(new CRpcResponse);
        rpccall->set_url(url);
        
        serialize(*static_cast<IRpcMessage*>(rpccall));
        
        IRpcMessageArray rpcheaders;
        m_headers.serializeChildren(rpcheaders);
        
        Owned<ISoapClient> soapclient;
        soapclient.setown(new CSoapClient);
        
        if (m_disableKeepAlive)
            soapclient->disableKeepAlive();

        if (!m_noSecurityHeader)
            soapclient->setUsernameToken(user, pw, realm);
        
        StringBuffer soapAction, resultbuf;
        if (m_soapAction)
            soapAction = m_soapAction.get();
        else if (m_specialmethod)
            soapAction.clear().append(m_nsuri.get()).append("/").append(m_specialmethod.get());
        else
            soapAction = m_method.get();
        int result = soapclient->postRequest(soapAction.str(), *rpccall.get(), resultbuf, &rpcheaders);
        //DBGLOG("SOAP Response: %s", resultbuf.str());
        
        if(result == SOAP_OK)
        {
            response.setRequestState(ESP_CLIENT_REQUEST_OK);
            response.unserialize(resultbuf);;
        }
        else if(result == SOAP_CONNECTION_ERROR)
        {
            response.setRequestState(ESP_CLIENT_REQUEST_CONNECTION_ERROR);
        }
        else
        {
            response.setRequestState(ESP_CLIENT_REQUEST_NORMAL_ERROR);
        }
    }
    
    static int transferThunkEvent(void *data)
    {
        CClientWsEclResponse* response = (CClientWsEclResponse*)data;
        
        if (response != NULL)
        {
            //DBGLOG("in main thread - result = %s", response->getResultsXML());
            response->Notify();
        }
        
        return 0;
    }
    
    void asyncPost(const char* url, CClientWsEclResponse& response)
    {
        response.setEvents(m_eventSink);
        post(url, response);
        
        ThunkToClientThread(m_hThunk, transferThunkEvent, (void *)&response);
    }
    
    static void eclWorkerThread(void* data)
    {
        CClientWsEclRequest *request = (CClientWsEclRequest *) data;
        
        if (request != NULL)
        {
            request->m_semWorkerThread.wait();  
            
            CClientWsEclResponse* response = new CClientWsEclResponse;
            
            request->asyncPost(request->m_url.get(), *response);
            request->m_semWorkerThread.signal();
            request->Release();
        }
    }   
};


#endif //_WS_ECL_CLIENT_BIND_HPP__

