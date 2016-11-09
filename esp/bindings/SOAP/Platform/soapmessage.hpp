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

#pragma warning( disable : 4786)

#ifndef _SOAPMESSAGE_HPP__
#define _SOAPMESSAGE_HPP__

#include "jliball.hpp"
#include "jexcept.hpp"
#include "soapesp.hpp"
#include "esp.hpp"

#include "http/platform/mime.hpp"

#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl DECL_EXPORT
#else
    #define esp_http_decl DECL_IMPORT
#endif

#include "http/platform/httptransport.hpp"

#include <xpp/XmlPullParser.h>

#include "xslprocessor.hpp"


#define SOAP_OK           0
#define SOAP_CLIENT_ERROR -1
#define SOAP_SERVER_ERROR -2
#define SOAP_RPC_ERROR    -3
#define SOAP_CONNECTION_ERROR -4
#define SOAP_REQUEST_TYPE_ERROR -5
#define SOAP_AUTHENTICATION_ERROR -6
#define SOAP_AUTHENTICATION_REQUIRED -7

#define SOAP_ENVELOPE_NAME "Envelope"
#define SOAP_HEADER_NAME   "Header"
#define SOAP_BODY_NAME     "Body"

class CSoapValue;
typedef CIArrayOf<CSoapValue> SoapValueArray;

using namespace std;
using namespace xpp;


const char* const SOAPEnvelopeStart  = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                                        "<soap:Envelope"
                                        " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                                        " xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\""
                                        " xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\""
//                                          " xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\""
                                        " xmlns:wsse=\"http://schemas.xmlsoap.org/ws/2002/04/secext\""
                                        ">";

const char* const SOAPEnvelopeEnd  =    "</soap:Envelope>";

class CSoapMessage : implements ISoapMessage, public CInterface
{
private:
    StringAttr   m_content_type;
    StringAttr   m_soapaction;
    StringBuffer m_text;
protected:
    Owned<IEspContext> m_context;
    Owned<CMimeMultiPart> m_multipart;
public:
    IMPLEMENT_IINTERFACE;

    CSoapMessage() {};
    virtual ~CSoapMessage() {};

    virtual void set_text(const char* text);
    virtual const char* get_text();
    virtual StringBuffer& query_text() { return m_text;}
    virtual int get_text_length();

    virtual void set_content_type(const char* content_type);
    virtual const char* get_content_type();

    virtual void set_soapaction(const char* soapaction) {m_soapaction.set(soapaction);};
    virtual const char* get_soapaction() {return m_soapaction.get();};

    virtual const char * getMessageType() {return "SoapMessage";};
    virtual StringBuffer& toString(StringBuffer& str) 
    {
        str.append("TO BE IMPLEMNTED!!!");
        return str;
    };

    virtual IEspContext * queryContext() { return m_context; }
    virtual void setContext(IEspContext *ctx) { m_context.set(ctx); }

    virtual void setOwnMultiPart(CMimeMultiPart* multipart)
    {
        m_multipart.setown(multipart);
    }
    virtual CMimeMultiPart* queryMultiPart()
    {
        return m_multipart.get();
    }

};

class CSoapFault : public CSoapMessage
{
protected:
    StringBuffer SoapStr;

    void AppendDetails(int code, const char* message, const char* actor="", const char* detailNS=NULL, const char* details = NULL)
    {
        StringBuffer encFaultStr;
        encodeXML(message, encFaultStr);

        SoapStr.append(SOAPEnvelopeStart);
        SoapStr.append("<soap:Body><soap:Fault>");

        SoapStr.appendf("<faultcode>%d</faultcode>"
                        "<faultstring>%s</faultstring>"
                        "<faultactor>%s</faultactor>", code,encFaultStr.str(),actor);
        if (detailNS)
        {
            const char* p = strstr(details,"<Exceptions>");
            if (p) 
            {
                StringBuffer newDetails(details);
                VStringBuffer nsAttr(" %s", detailNS);
                newDetails.insert(strlen("<Exceptions>")-1,nsAttr.str());

                SoapStr.appendf("<detail>%s</detail>",newDetails.str());
            }
        }

        SoapStr.append("</soap:Fault></soap:Body>");
        SoapStr.append(SOAPEnvelopeEnd);
    }

public:
    CSoapFault(int code, const char* message)
    { 
        AppendDetails(code,message);
    }

    CSoapFault(IMultiException* mex, const char* detailNS=NULL)
    {
        StringBuffer errorStr,details;
        mex->errorMessage(errorStr);
        mex->serialize(details);            
        AppendDetails(mex->errorCode(),errorStr.str(),mex->source(),detailNS,details.str());
    }

    CSoapFault(IException* e)
    {
        StringBuffer errorStr;
        e->errorMessage(errorStr);
        AppendDetails(e->errorCode(),errorStr.str());
    }
    
    virtual const char* get_text() { return SoapStr.str(); }
    
    virtual int get_text_length() { return SoapStr.length(); }
};

class CSoapRequest : public CSoapMessage
{
public:
    CSoapRequest() {};
    virtual ~CSoapRequest() {};
};

class CSoapResponse : public CSoapMessage
{
private:
    int          m_status;
    StringBuffer m_err;
public:
    CSoapResponse(){m_status = SOAP_OK;};
    virtual ~CSoapResponse() {};

    virtual void set_status(int status);
    virtual int get_status();
    virtual void set_err(const char* err);
    virtual const char* get_err();
};


class esp_http_decl CSoapValue : public CInterface  //ISoapValue
{
private:
    bool            m_is_array_element;
    bool            m_encode_xml;
    StringAttr  m_ns;
    StringAttr  m_name;
    StringAttr  m_type;
    StringBuffer m_value;
    SoapValueArray  m_children;
    Owned<IProperties> m_attributes;

    void serialize_attributes(StringBuffer& outbuf);
    void init(const char* ns, const char* name, const char* type, const char* value);

public:
    CSoapValue(CSoapValue* soapvalue);
    CSoapValue(const char* ns, const char* name, const char* type, const char* value, bool encode=true);
    CSoapValue(const char* ns, const char* name, const char* type, int value);
    CSoapValue(const char* ns, const char* name, const char* type, unsigned long value);
    CSoapValue(const char* ns, const char* name, const char* type, __int64 value);
    CSoapValue(const char* ns, const char* name, const char* type, unsigned int value);
    CSoapValue(const char* ns, const char* name, const char* type, unsigned short value);
    CSoapValue(const char* ns, const char* name, const char* type, double value);
    CSoapValue(const char* ns, const char* name, const char* type, float value);
    CSoapValue(const char* ns, const char* name, const char* type, bool value);
    virtual ~CSoapValue() { }
    
    void setEncodeXml(bool encode=true){m_encode_xml=encode;}

    virtual const char* get_name() {return m_name.get();};
    virtual CSoapValue* get_value(const char* path);
    virtual CSoapValue* get_element(const char* path, StringBuffer *attrname);
    virtual SoapValueArray* get_valuearray(const char* path);
    virtual SoapValueArray* query_children(){return &m_children;}

    virtual bool get_value_str(const char* path, StringBuffer& value);

    virtual const char *query_value(const char* path);
    virtual const char* query_attr_value(const char *path);


    virtual bool get_value(const char* path, StringAttr& value);
    virtual bool get_value(const char* path, StringBuffer& value);
    virtual bool get_value(const char* path, StringBuffer& value, bool simpleXml);
    virtual bool get_value(const char* path, const char*& value) {  throw "should not be called"; }
    virtual bool get_value(const char* path, int& value);
    virtual bool get_value(const char* path, unsigned long& value);
    virtual bool get_value(const char* path, unsigned char& value);
    virtual bool get_value(const char* path, long& value);
    virtual bool get_value(const char* path, __int64& value);
    virtual bool get_value(const char* path, unsigned int& value);
    virtual bool get_value(const char* path, unsigned short& value);
    virtual bool get_value(const char* path, short& value);

    virtual bool get_value(const char* path, StringArray& value, bool simpleXml);
    virtual bool get_value(const char* path, StringArray& value) { return get_value(path,value,false); }
    virtual bool get_value(const char* path, ShortArray& value);
    virtual bool get_value(const char* path, IntArray& value);
    virtual bool get_value(const char* path, Int64Array& value);
    virtual bool get_value(const char* path, BoolArray& value);
    virtual bool get_value(const char* path, FloatArray& value);
    virtual bool get_value(const char* path, DoubleArray& value);

    virtual bool get_value(const char* path, bool& value);
    virtual bool get_value(const char* path, double& value);
    virtual bool get_value(const char* path, float& value);
    virtual void set_value(const char* value) {m_value.clear(); m_value.append(value);};

    virtual void add_value(const char* path, const char* ns, CSoapValue* value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, SoapValueArray& valuearray);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, bool encodeXml);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, int value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned long value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, long value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, __int64 value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned int value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned short value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, bool value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, double value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, float value);

    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, StringArray& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, ShortArray& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, IntArray& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, Int64Array& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, BoolArray& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, FloatArray& value);
    virtual void add_value(const char* path, const char* ns, const char* name, const char* childns, 
        const char* childname, const char* childtype, DoubleArray& value);

    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, IProperties& attrs);

    void add_attribute(const char* name, const char* value);

    virtual CSoapValue* ensure(const char* ns, const char* path);

    virtual void add_child(CSoapValue* child);

    virtual void serializeChildren(StringBuffer& outbuf, CMimeMultiPart* multipart);
    virtual void serialize(StringBuffer& outbuf, CMimeMultiPart* multipart);
    virtual void serializeContent(StringBuffer& outbuf, CMimeMultiPart* multipart);

    virtual void simple_serialize(StringBuffer& outbuf);
    virtual void simple_serializeChildren(StringBuffer& outbuf);
};

class esp_http_decl CRpcMessage : implements IRpcMessage, public CInterface
{
private:
    StringAttr      m_ns;
    StringAttr      m_nsuri;
    StringAttr      m_name;
    StringBuffer    m_text;
    StringAttr      m_serializedContent;

    Owned<IEspContext>  m_context;

    Owned<CSoapValue> m_params;
    Owned<IProperties> m_attributes;
    bool m_encode_xml;

public:
    IMPLEMENT_IINTERFACE;

    CRpcMessage()
    {
        Init();
    };

    CRpcMessage(const char* name)
    {
        m_name.set(name);
        Init();
    };

    void Init()
    {
        m_params.setown(new CSoapValue("", ".", "", ""));
        m_encode_xml = true;
        m_params->setEncodeXml(true);
    }

    virtual ~CRpcMessage(){};

    void setEncodeXml(bool encode)
    {
        m_encode_xml = encode;
        m_params->setEncodeXml(encode);
    }

    bool getEncodeXml(){return m_encode_xml;}

    virtual IEspContext * queryContext(){return m_context;}
    void setContext(IEspContext *value){m_context.set(value);}
    
    virtual void set_ns(const char* ns) {m_ns.set(ns);};
    virtual void set_nsuri(const char* nsuri) {m_nsuri.set(nsuri);};
    virtual StringBuffer& get_nsuri(StringBuffer& nsuri) 
    {
        nsuri.append(m_nsuri.get());
        return nsuri;
    };
    virtual const char* get_name() {return m_name.get();};
    virtual void set_name(const char* name) {m_name.set(name);};

    virtual CSoapValue* get_value(const char* path) {return m_params->get_value(path); };
    virtual SoapValueArray* get_valuearray(const char* path) {return m_params->get_valuearray(path); };
    
    virtual bool get_value(const char* path, StringAttr& value){return m_params->get_value(path, value); };
    virtual bool get_value(const char* path, StringBuffer& value){return m_params->get_value(path, value); };
    virtual bool get_value(const char* path, StringBuffer& value, bool bSimpleXml){return m_params->get_value(path, value, bSimpleXml); };

    virtual bool get_value(const char* path, int& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, unsigned long& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, unsigned char& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, long& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, __int64& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, unsigned int& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, unsigned short& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, short& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, bool& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, double& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, float& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, StringArray& value) {return m_params->get_value(path, value, false);};
    virtual bool get_value(const char* path, StringArray& value, bool simpleXml) {return m_params->get_value(path, value, simpleXml);};
    virtual bool get_value(const char* path, ShortArray& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, IntArray& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, Int64Array& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, BoolArray& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, FloatArray& value) {return m_params->get_value(path, value);};
    virtual bool get_value(const char* path, DoubleArray& value) {return m_params->get_value(path, value);};
    
    virtual void add_value(const char* path, const char* ns, CSoapValue* value)
    {
        m_params->add_value(path, ns, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, SoapValueArray& valuearray)
    {
        m_params->add_value(path, ns, name, type, valuearray);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, bool encodeXml)
    {
        m_params->add_value(path, ns, name, type, value, encodeXml);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, StringBuffer& value)
    {
        m_params->add_value(path, ns, name, type, value.str());
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, int value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned long value)
    {
        m_params->add_value(path, ns, name, type, value);
    } 
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, long value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, __int64 value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned int value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, unsigned short value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, bool value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, double value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, float value)
    {
        m_params->add_value(path, ns, name, type, value);
    }
    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, StringArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, ShortArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    
    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, IntArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, Int64Array& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, FloatArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, DoubleArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    virtual void add_value(const char* path, const char* ns, const char* name, 
                                    const char* childns, const char* childname, const char* childtype, BoolArray& value)
    { m_params->add_value(path, ns, name, childns, childname, childtype, value); }

    void add_attribute(const char* name, const char* value);
    void serialize_attributes(StringBuffer& outbuf);

    virtual void add_attr(const char * path, const char * name, const char * value, IProperties & attrs);

    virtual void add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, IProperties& attrs)
    {
        m_params->add_value(path, ns, name, type, value, attrs);
    }

    virtual void add_value(const char* path, const char* name, const char* value, IProperties& attrs)
    {
        add_value(path, "", name, "", value, attrs);
    }

    //virtual void marshall(StringBuffer& outbuf);
    virtual void marshall(StringBuffer& outbuf, CMimeMultiPart* multipart);

    virtual void simple_marshall(StringBuffer& outbuf);

    virtual const char* get_text() {return m_text.str();};
    virtual void set_text(const char* text) {m_text.clear(); m_text.append(text);};
    virtual void append_text(const char* text) {m_text.append(text);};

    virtual void unmarshall(XmlPullParser* xpp);
    virtual void unmarshall(XmlPullParser* xpp, CSoapValue* soapvalue, const char* tagname);
    virtual void unmarshall(XmlPullParser* xpp, CMimeMultiPart* multipart);
    virtual void unmarshall(XmlPullParser* xpp, CSoapValue* soapvalue, const char* tagname, CMimeMultiPart* multipart);

    virtual void marshall(StringBuffer & outbuf)
    {
        throw MakeStringException(-1, "not implemented");
    }
    virtual void setSerializedContent(const char* c) { m_serializedContent.set(c); }
};

esp_http_decl IRpcMessage* createRpcMessage(const char* rootTag,StringBuffer& src);

class CRpcCall : public CRpcMessage
{
private:
    StringBuffer m_proxy;
    StringAttr m_url;

public:
    CRpcCall() {};
    CRpcCall(const char* url) {m_url.set(url);}

   virtual ~CRpcCall(){}
    
    virtual const char* get_url() {return m_url.get();}
    virtual void set_url(const char* url) {m_url.set(url);}

    virtual const char* getProxy() {return m_proxy.str();}
    virtual void setProxy(const char* proxy) {m_proxy.clear().append(proxy);}
};

class CRpcResponse : public CRpcMessage
{
private:
    int m_status;
    StringBuffer m_err;
public:
    CRpcResponse(){ }
    CRpcResponse(const char* name):CRpcMessage(name) { }
    virtual ~CRpcResponse(){ }

    virtual int get_status() { return m_status; }
    virtual void set_status(int status) { m_status = status; }
    void set_err(const char* err) { m_err.clear().append(err); }
    const char* get_err() {  return m_err.str(); }

    bool handleExceptions(IXslProcessor *xslp, IMultiException *me, const char *serv, const char *meth, const char *errorXslt);
};

class CHeader : public CInterface
{
private:
    IArrayOf<IRpcMessage> m_headerblocks;

public:
    CHeader(){};
    virtual ~CHeader(){};
    virtual void addHeaderBlock(IRpcMessage* block);
    virtual int getNumBlocks();
    virtual IRpcMessage* getHeaderBlock(int seq);
    virtual IRpcMessage* getHeaderBlock(const char* name);
    virtual void unmarshall(XmlPullParser* xpp);
    virtual StringBuffer& marshall(StringBuffer& str, CMimeMultiPart* multipart);
    virtual const char * getMessageType() {return "EnvelopeHeader";};
};

class CBody : public CInterface
{
private:
    XmlPullParser* m_xpp;
    IArrayOf<IRpcMessage> m_rpcmessages;

public:
    CBody() : m_xpp(NULL) { }
    virtual ~CBody() { }

    virtual XmlPullParser* get_xpp() {return m_xpp;};
    virtual void set_xpp(XmlPullParser* xpp) {m_xpp = xpp;};
    
    virtual void add_rpcmessage(IRpcMessage* rpcmessage) {
        if(rpcmessage)
        {
            m_rpcmessages.append(*LINK(rpcmessage));
        }
    };

    virtual void nextRpcMessage(IRpcMessage* rpcmessage);

    virtual const char * getMessageType() {return "EnvelopeBody";};

    virtual StringBuffer& marshall(StringBuffer& str, CMimeMultiPart* multipart) {
        str.append("<soap:Body>");
        ForEachItemIn(x, m_rpcmessages)
        {
            IRpcMessage& oneelem = m_rpcmessages.item(x);
            StringBuffer oneelembuf;
            oneelem.marshall(oneelembuf, multipart);
            str.append(oneelembuf.str());
            //str.append("\r\n");
        }
        str.append("</soap:Body>");
        return str;
    };
};

class CEnvelope : public CInterface
{
private:
    Owned<CHeader> m_header;
    Owned<CBody> m_body;
public:

    CEnvelope(){m_header.setown(new CHeader); m_body.setown(new CBody);};
    CEnvelope(CHeader* header, CBody* body) {m_header.setown(header); m_body.setown(body);};
    virtual ~CEnvelope(){};
    virtual CHeader* get_header() {return m_header.get();}
    virtual CBody* get_body() {return m_body.get();}
    virtual void unmarshall(XmlPullParser* xpp);

    virtual const char * getMessageType() {return "SoapEnvelope";};

    virtual void marshall(CMimeMultiPart* multipart) 
    {
        CMimeBodyPart* rootpart = new CMimeBodyPart("text/xml", "8bit", "soaproot", "", NULL);
        multipart->setRootPart(rootpart);

        StringBuffer str(SOAPEnvelopeStart);
        if(m_header)
        {
            m_header->marshall(str, multipart);
            //str.append("\r\n");
        }
        if(m_body)
        {
            m_body->marshall(str, multipart);
            //str.append("\r\n");
        }
        str.append(SOAPEnvelopeEnd);
    //  str.append("</soap:Envelope>");

        rootpart->setContent(str.length(), str.str());

        if(multipart->getBodyCount() <= 1)
            multipart->setContentType(HTTP_TYPE_TEXT_XML_UTF8);
        else
            multipart->setContentType(HTTP_TYPE_MULTIPART_RELATED);
    };

};

#endif
