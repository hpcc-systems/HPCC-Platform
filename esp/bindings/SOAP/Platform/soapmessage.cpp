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

//Jlib
#include "jexcept.hpp"

//ESP Bindings
#include "SOAP/Platform/soapmessage.hpp"
#include "bindutil.hpp"
#include <memory>

static void serializeAttributes(StringBuffer& outbuf, IProperties* attrs)
{
    if (attrs)
    {
        Owned<IPropertyIterator> it = attrs->getIterator();
        for (it->first(); it->isValid(); it->next())
        {
            const char* k = it->getPropKey();
            const char* v = attrs->queryProp(k);
            outbuf.append(' ').append(k).append("=\"");
            encodeUtf8XML(v,outbuf);
            outbuf.append('"');
        }
    }
}

void CSoapMessage::set_text(const char* text)
{
    m_text.clear();
    m_text.append(text);
}

const char* CSoapMessage::get_text()
{
    return m_text.str();
}

int CSoapMessage::get_text_length()
{
    return m_text.length();
}

void CSoapMessage::set_content_type(const char* content_type)
{
    m_content_type.set(content_type);
}

const char* CSoapMessage::get_content_type()
{
    return m_content_type.get();
}

void CSoapResponse::set_status(int status)
{
    m_status = status;
}

int CSoapResponse::get_status()
{
    return m_status;
}

void CSoapResponse::set_err(const char* err)
{
    m_err.clear();
    m_err.append(err);
}

const char* CSoapResponse::get_err()
{
    return m_err.str();
}

void CRpcMessage::simple_marshall(StringBuffer& outbuf)
{
    outbuf.append("<").append(m_name.get()).append(">");

    StringBuffer childrenbuf;
    m_params->simple_serializeChildren(childrenbuf);
    outbuf.append(childrenbuf);
    
    outbuf.append("</").append(m_name.get()).append(">");
}

void CRpcMessage::marshall(StringBuffer& outbuf, CMimeMultiPart* multipart)
{
    if (m_serializedContent.get())
        outbuf.append( m_serializedContent );
    else
    {
        outbuf.append("<");
        if(m_ns.length() > 0)
        {
            outbuf.append(m_ns.get());
            outbuf.append(":");
        }
        outbuf.append(m_name.get());
        bool no_ns = (m_context && m_context->queryOptions()&ESPCTX_NO_NAMESPACES );
        if (!no_ns && m_nsuri.length()>0)
        {
            outbuf.append(" xmlns");
            if (m_ns.length()>0)
            {
                outbuf.append(":").append(m_ns);
            }
            outbuf.append("=\"").append(m_nsuri.get()).append("\"");
        }

        serialize_attributes(outbuf);

        outbuf.append(">");

        StringBuffer childrenbuf;
        m_params->serializeChildren(childrenbuf, multipart);
        outbuf.append(childrenbuf);
        
        outbuf.append("</");
        if(m_ns.length() > 0)
        {
            outbuf.append(m_ns.get());
            outbuf.append(":");
        }
        outbuf.append(m_name.get());
        outbuf.append(">");
    }
}

void CRpcMessage::serialize_attributes(StringBuffer& outbuf)
{
    ::serializeAttributes(outbuf,m_attributes);
}

void CRpcMessage::add_attribute(const char* name, const char* value)
{
    if (!name || !*name)
        return;
    if (!m_attributes)
        m_attributes.setown(createProperties());
    m_attributes->setProp(name, value);
}

void CRpcMessage::add_attr(const char * path, const char * name, const char * value, IProperties & attrs)
{
    if ((path && *path) || (name && *name))
    {
        CSoapValue *par=m_params.get();
        if(path)
            par = par->get_value(path);
        if (name)
            par = par->get_value(name);
        if (par)
        {
            Owned<IPropertyIterator> piter = attrs.getIterator();       
            for (piter->first(); piter->isValid(); piter->next())
            {
                const char *propkey = piter->getPropKey();
                par->add_attribute(propkey, attrs.queryProp(propkey));
            }
        }
    }
    else
    {
        Owned<IPropertyIterator> piter = attrs.getIterator();
        for (piter->first(); piter->isValid(); piter->next())
        {
            const char *propkey = piter->getPropKey();
            add_attribute(propkey, attrs.queryProp(propkey));
        }
    }
}

void CRpcMessage::preunmarshall(XJXPullParser* xpp)
{
    if(!xpp)
        return;
    int type;
    StartTag stag;
    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT)
    {
        if(type == XmlPullParser::START_TAG)
        {
            xpp->readStartTag(stag);
            const char* localname = stag.getLocalName();
            if(!localname || strieq(localname, "__object__"))
                continue;
            set_name(localname);

            StringBuffer ns;
            const char* qname = stag.getQName();

            if(strlen(qname) > strlen(localname))
            {
                const char* semcol = strchr(qname, ':');
                if(semcol != NULL)
                {
                    ns.append(qname, 0, semcol - qname);
                }
            }
            const char* nsuri = stag.getUri();

            set_ns(ns.str());
            set_nsuri(nsuri);
            return;
        }
    }
}

void CRpcMessage::unmarshall(XJXPullParser* xpp)
{
    unmarshall(xpp, m_params, m_name);
}

void CRpcMessage::unmarshall(XJXPullParser* xpp, CSoapValue* soapvalue, const char* tagname)
{
    int type;
    StartTag stag;
    EndTag etag;

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT) 
    {
        if(type == XmlPullParser::END_TAG)
        {
            xpp->readEndTag(etag);
            if(!stricmp(etag.getLocalName(), tagname))
                break;
        }
        else if(type == XmlPullParser::START_TAG)
        {
            xpp->readStartTag(stag);
            StringBuffer ns;
            const char* qname = stag.getQName();
            //DBGLOG("tag qname=%s", qname);
            const char* localname = stag.getLocalName();
            const char* valuetype = stag.getValue("SOAP-ENC:type");
            
            if(strlen(qname) > strlen(localname))
            {
                const char* semcol = strchr(qname, ':');
                if(semcol != NULL)
                {
                    ns.append(qname, 0, semcol - qname);
                }
            }

            CSoapValue* childsoapval = new CSoapValue(ns.str(), localname, valuetype, (const char*)NULL,m_encode_xml);
            soapvalue->add_child(childsoapval);

            // attributes
            for(int i = 0; ; ++i) {
                const char* name = stag.getRawName(i);
                if (!name)
                    break;
                childsoapval->add_attribute(name, stag.getValue(i));    
            }

            unmarshall(xpp, childsoapval, localname);
        }
        else if(type == XmlPullParser::CONTENT) 
        {
            const char* value = xpp->readContent();
            soapvalue->set_value(value);
        }
    }
}

void CRpcMessage::unmarshall(XJXPullParser* xpp, CMimeMultiPart* multipart)
{
    unmarshall(xpp, m_params, m_name, multipart);
}

void CRpcMessage::unmarshall(XJXPullParser* xpp, CSoapValue* soapvalue, const char* tagname, CMimeMultiPart* multipart)
{
    int type;

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT) 
    {
        if(type == XmlPullParser::END_TAG)
        {
            EndTag etag;
            xpp->readEndTag(etag);
            if(!stricmp(etag.getLocalName(), tagname))
                break;
        }
        else if(type == XmlPullParser::START_TAG)
        {
            StartTag stag;
            xpp->readStartTag(stag);
            StringBuffer ns;
            const char* qname = stag.getQName();
            //DBGLOG("tag qname=%s", qname);
            const char* localname = stag.getLocalName();
            const char* valuetype = stag.getValue("SOAP-ENC:type");
            
            if(strlen(qname) > strlen(localname))
            {
                const char* semcol = strchr(qname, ':');
                if(semcol != NULL)
                {
                    ns.append(qname, 0, semcol - qname);
                }
            }

            CSoapValue* childsoapval = new CSoapValue(ns.str(), localname, valuetype, (const char*)NULL,m_encode_xml);
            soapvalue->add_child(childsoapval);

            // attributes
            for(int i = 0; ; ++i) {
                const char* name = stag.getRawName(i);
                if (!name)
                    break;
                childsoapval->add_attribute(name, stag.getValue(i));    
            }

            const char* href = stag.getValue("href");
            if(href != NULL)
            {
                if(multipart != NULL)
                {
                    Owned<CMimeBodyPart> bodypart = multipart->getBodyPart(href+4);
                    if(bodypart)
                    {
                        StringBuffer val;
                        bodypart->getContent(val);
                        if(val.length() > 0)
                        {
                            childsoapval->set_value(val.str());
                        }
                    }
                }
            }
            else
            {
                unmarshall(xpp, childsoapval, localname);
            }
        }
        else if(type == XmlPullParser::CONTENT) 
        {
            const char* value = xpp->readContent();
            soapvalue->set_value(value);        
        }
    }
}

IRpcMessage* createRpcMessage(const char* rootTag, StringBuffer& xml)
{
    CRpcMessage* rpc = new  CRpcMessage(rootTag);

    std::unique_ptr<XmlPullParser> xpp(new XmlPullParser(xml.str(), xml.length()));
    xpp->setSupportNamespaces(true);

    rpc->unmarshall(xpp.get());

    return rpc;
}

bool CRpcResponse::handleExceptions(IXslProcessor *xslp, IMultiException *me, const char *serv, const char *meth, const char *errorXslt)
{
    IEspContext *context=queryContext();
    if (me->ordinality()>0)
    {
        StringBuffer text;
        me->errorMessage(text);
        text.append('\n');
        IWARNLOG("Exception(s) in %s::%s - %s", serv, meth, text.str());

        if (errorXslt)
        {
            me->serialize(text.clear());
            StringBuffer theOutput;
            xslTransformHelper(xslp, text.str(), errorXslt, theOutput, context->queryXslParameters());
            set_text(theOutput.str());
        }
    }
    return false;
}

void CBody::nextRpcMessage(IRpcMessage* rpcmessage)
{
    if(!rpcmessage || !m_xpp)
        return;

    int type;
    StartTag stag;
    EndTag etag;

    while((type = m_xpp->next()) != XmlPullParser::END_DOCUMENT)
    {
        if(type == XmlPullParser::START_TAG)
        {
            m_xpp->readStartTag(stag);
            const char* localname = stag.getLocalName();
            rpcmessage->set_name(localname);

            
            StringBuffer ns;
            const char* qname = stag.getQName();
            
            if(strlen(qname) > strlen(localname))
            {
                const char* semcol = strchr(qname, ':');
                if(semcol != NULL)
                {
                    ns.append(qname, 0, semcol - qname);
                }
            }
            const char* nsuri = stag.getUri();
            
            rpcmessage->set_ns(ns.str());
            rpcmessage->set_nsuri(nsuri);
            
            //DBGLOG("ns=%s nsuri=%s", ns.str(), nsuri);
            return;
        }
    }
}

void CHeader::addHeaderBlock(IRpcMessage* block)
{
    m_headerblocks.append(*block);
}

int CHeader::getNumBlocks()
{
    return m_headerblocks.ordinality();
}

IRpcMessage* CHeader::getHeaderBlock(int seq)
{
    if(seq >= m_headerblocks.ordinality())
        return NULL;
    else
        return &(m_headerblocks.item(seq));
}

IRpcMessage* CHeader::getHeaderBlock(const char* name)
{
    ForEachItemIn(x, m_headerblocks)
    {
        IRpcMessage* oneblock = &(m_headerblocks.item(x));
        if(oneblock != NULL && (strcmp(oneblock->get_name(), name) == 0))
        {
            return oneblock;
        }
    }
    return NULL;
}

StringBuffer& CHeader::marshall(StringBuffer& str, CMimeMultiPart* multipart)
{
    // comment this out to workaround a roxy bug that can not handle soap:Header
    str.append("<soap:Header>");
    ForEachItemIn(x, m_headerblocks)
    {
        IRpcMessage* oneblock = &(m_headerblocks.item(x));
        if(oneblock != NULL)
        {
            StringBuffer onebuf;
            oneblock->marshall(onebuf, multipart);;
            str.append(onebuf.str());
        }
    }
    str.append("</soap:Header>");
    return str;
}

void CHeader::unmarshall(XJXPullParser* xpp)
{
    int type;
    StartTag stag;
    EndTag etag;

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT) 
    {
        if(type == XmlPullParser::END_TAG) 
        {
            xpp->readEndTag(etag);
            if(!stricmp(etag.getLocalName(), SOAP_HEADER_NAME))
                break;
        }
        else if(type == XmlPullParser::START_TAG) {
            xpp->readStartTag(stag);
            CRpcMessage *oneblock = new CRpcMessage(stag.getLocalName());
            StringBuffer ns;
            const char* qname = stag.getQName();
            const char* localname = stag.getLocalName();
            
            if(strlen(qname) > strlen(localname))
            {
                const char* semcol = strchr(qname, ':');
                if(semcol != NULL)
                {
                    ns.append(qname, 0, semcol - qname);
                }
            }
            oneblock->set_ns(ns.str());
            oneblock->unmarshall(xpp);
            m_headerblocks.append(*oneblock);
        }
    }
}

void CEnvelope::unmarshall(XJXPullParser* xpp)
{
    int type;
    StartTag stag;
    EndTag etag;

    while((type = xpp->next()) != XmlPullParser::END_DOCUMENT) 
    {
        if(type == XmlPullParser::START_TAG) {
            xpp->readStartTag(stag);
            if(!stricmp(stag.getLocalName(), SOAP_HEADER_NAME))
            {
                m_header->unmarshall(xpp);

                /*
                IRpcMessage* oneblock = m_header->getHeaderBlock("Security");
                if(oneblock != NULL)
                {
                    StringBuffer tmpbuf;
                    oneblock->marshall(tmpbuf);
                    DBGLOG("OK, we've got header - \n%s", tmpbuf.str());
                }
                else
                {
                    DBGLOG("Not found!!!!");
                }
                */
            }
            else if(!stricmp(stag.getLocalName(), SOAP_BODY_NAME))
            {
                m_body->set_xpp(xpp);
                break;
            }
        }
    }
}


/*****************************************************************
              CSoapValue Implementation
******************************************************************/
CSoapValue::CSoapValue(CSoapValue* soapvalue)
{
    m_encode_xml=true;
    if(soapvalue != NULL)
    {
        m_ns.set(soapvalue->m_ns.get());
        m_name.set(soapvalue->m_name.get());
        m_type.set(soapvalue->m_type.get());
        m_value.append(soapvalue->m_value);

        ForEachItemIn(x, soapvalue->m_children)
        {
            CSoapValue& onechild = soapvalue->m_children.item(x);
            m_children.append(*LINK(&onechild));
        }
        
        IProperties* attrs = soapvalue->m_attributes;
        if (attrs)
        {
            m_attributes.setown(createProperties());
            Owned<IPropertyIterator> it = attrs->getIterator();
            for (it->first(); it->isValid(); it->next())
                m_attributes->setProp(it->getPropKey(), attrs->queryProp(it->getPropKey()));
        }
        m_is_array_element = soapvalue->m_is_array_element;
    }
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, const char* value, bool encode)
{
    init(ns,name,type,value);
    m_encode_xml=encode;
}

void CSoapValue::init(const char* ns, const char* name, const char* type, const char* value)
{
    m_ns.set(ns);
    m_name.set(name);
    m_type.set(type);
    m_value.append(value);
    m_is_array_element = false;
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, int value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, unsigned long value)
{
    StringBuffer valstr;
    valstr.appendlong(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, __int64 value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, unsigned int value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, unsigned short value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, double value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, float value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

CSoapValue::CSoapValue(const char* ns, const char* name, const char* type, bool value)
{
    StringBuffer valstr;
    valstr.append(value);
    init(ns, name, type, valstr.str());
}

const char* CSoapValue::query_attr_value(const char *path)
{
    if (*path=='@')
    {
        if (!m_attributes)
            return NULL;
        return m_attributes->queryProp(path+1);
    }
    return NULL;
}

const char* CSoapValue::query_value(const char* path)
{
    if(!path || !*path || *path=='.')
        return m_value.str();
    StringBuffer attrname;
    CSoapValue *elem = get_element(path, &attrname);
    if (!elem)
        return NULL;
    if (attrname.length())
        return elem->query_attr_value(attrname.str());
    return elem->query_value(NULL);
}

CSoapValue* CSoapValue::get_value(const char* path)
{
    return get_element(path, NULL);
}

CSoapValue* CSoapValue::get_element(const char* path, StringBuffer *attrname)
{
    if(!path || !*path || *path=='.')
        return this;
    if (*path=='@')
    {
        if (!attrname)
            return NULL;
        attrname->append(path);
        return this;
    }
    if(m_children.ordinality() == 0)
        return NULL;

    const char* slash = strchr(path, '/');
    StringBuffer childname;
    const char* nextname = NULL;

    int childind = -1;

    if(slash == NULL)
    {
        const char* lbracket = strchr(path, '[');
        if(lbracket == NULL)
        {
            childname.append(path);
        }
        else
        {
            const char* rbracket = strchr(path, ']');
            char* indexbuf = new char[rbracket - lbracket];
            int i = 0;
            for(i = 0; i < rbracket - lbracket - 1; i++)
                indexbuf[i] = lbracket[i + 1];
            indexbuf[i] = 0;
            childind = atoi(indexbuf);
            delete[] indexbuf;
            childname.append(path, 0, lbracket - path);
        }
    }
    else
    {
        const char* lbracket = strchr(path, '[');
        if(lbracket != NULL && lbracket < slash)
        {
            const char* rbracket = strchr(path, ']');
            char* indexbuf = new char[rbracket - lbracket];
            int len = rbracket - lbracket - 1;
            for(int i = 0; i < len; i++)
                indexbuf[i] = lbracket[i+1];
            indexbuf[len] = 0;
            childind = atoi(indexbuf);
            delete[] indexbuf;
            childname.append(path, 0, lbracket - path);
        }
        else
        {
            childname.append(path, 0, slash - path);
        }
        nextname = slash + 1;
    }
    
    int n_onechild = 0;
    ForEachItemIn(x, m_children)
    {
        CSoapValue& onechild = m_children.item(x);
        if(!strcmp(onechild.m_name.get(), childname.str()))
        {
            if(childind == -1 || ++n_onechild == childind)
                return onechild.get_element(nextname, attrname);
        }
    }
    return NULL;
}

SoapValueArray* CSoapValue::get_valuearray(const char* path)
{
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
        return &(sv->m_children);
    return NULL;
}


bool CSoapValue::get_value(const char* path, StringAttr& value)
{
    const char *csval=query_value(path);
    value.set(csval);
    return csval!=NULL;
}

bool CSoapValue::get_value(const char* path, StringBuffer& value)
{
    StringBuffer attrname;
    CSoapValue* sv = get_element(path, &attrname);
    if(sv != NULL)
    {
        //Yanrui - change it to return the whole content instead of just the value
        //         please report if it's not desirable for any circumstances.
        //value.append(sv->m_value.str());
        if (attrname.length())
        {
            value.append(sv->query_value(attrname.str()));
        }
        else
        {
            sv->setEncodeXml(m_encode_xml);
            sv->serializeContent(value, NULL);
        }
    }   
    return (sv != NULL);
}

bool CSoapValue::get_value_str(const char* path, StringBuffer& value)
{
    const char *csval=query_value(path);
    value.append(csval);
    return csval!=NULL;
}


bool CSoapValue::get_value(const char* path, int& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atol(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, unsigned long& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atol(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, unsigned char& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = (unsigned char)atol(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, long& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atol(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, __int64& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = _atoi64(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, unsigned int& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atoi(csval);
        return true;
    }
    return false;
}
bool CSoapValue::get_value(const char* path, unsigned short& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atoi(csval);
        return true;
    }
    return false;
}
bool CSoapValue::get_value(const char* path, short& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = (short)atoi(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, double& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = atof(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, float& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = (float)atof(csval);
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, bool& value)
{
    const char *csval=query_value(path);
    if (csval && *csval)
    {
        value = streq(csval, "1") || strieq(csval, "true");
        return true;
    }
    return false;
}

bool CSoapValue::get_value(const char* path, StringArray& value, bool simpleXml)
{
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            if (simpleXml)
                onechild.simple_serializeChildren(tmpval);
            else
                onechild.serializeContent(tmpval, NULL);
            value.append(tmpval.str());
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, IntArray& value)
{
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append(atoi(tmpval));
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, ShortArray& value)
{  
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append(atoi(tmpval));
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, Int64Array& value)
{  
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append(atoi64(tmpval));
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, BoolArray& value)
{  
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append( (strcmp(tmpval,"true")==0||strcmp(tmpval,"1")==0) ? true : false);
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, FloatArray& value)
{  
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append((float)atof(tmpval));
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, DoubleArray& value)
{  
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        ForEachItemIn(x, sv->m_children)
        {
            CSoapValue& onechild = sv->m_children.item(x);
            onechild.setEncodeXml(m_encode_xml);

            StringBuffer tmpval;
            onechild.serializeContent(tmpval, NULL);
            value.append(atof(tmpval));
        }
    }
    return (sv != NULL);
}

bool CSoapValue::get_value(const char* path, StringBuffer& value, bool simpleXml)
{
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
    {
        sv->setEncodeXml(m_encode_xml);
        if (simpleXml)
            sv->simple_serialize(value);
        else
            sv->serializeContent(value, NULL);
    }
    return (sv != NULL);
}

CSoapValue* CSoapValue::ensure(const char* ns, const char* path)
{
    CSoapValue* sv = get_value(path);
    if(sv != NULL)
        return sv;
    if(path == NULL || strlen(path) == 0)
        return this;

    const char* rslash = strrchr(path, '/');
    if(rslash == NULL)
    {
        const char* pEnd = strchr(path, '[');
        if (!pEnd)
            pEnd = path + strlen(path);
        StringBuffer name(pEnd - path, path);
        CSoapValue* child = new CSoapValue(ns, name.str(), "", "",m_encode_xml);
        this->add_child(child);
        return child;
    }
    else
    {
        StringBuffer parentpath;
        parentpath.append(path, 0, rslash - path);
        CSoapValue* parent = ensure(ns, parentpath.str());
        if(parent != NULL)
        {
            const char* pEnd = strchr(++rslash, '[');
            if (!pEnd)
                pEnd = rslash + strlen(rslash);
            StringBuffer name(pEnd - rslash, rslash);

            CSoapValue* child = new CSoapValue(ns, name.str(), "", "",m_encode_xml);
            parent->add_child(child);
            return child;
        }
    }
    return NULL;
}

void CSoapValue::add_child(CSoapValue* child)
{
    if(child)
        m_children.append(*child);
}

//Note: value is now owned by the current SoapValue, so shouldn't be freed outside.
void CSoapValue::add_value(const char* path, const char* ns, CSoapValue* value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        sv->add_child(value);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, SoapValueArray& valuearray)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        int len = valuearray.length();
        StringBuffer typestr;
        if(type != NULL && strlen(type) > 0)
        {
            typestr.append(type);
            typestr.append("[");
            typestr.append(len);
            typestr.append("]");
        }
        CSoapValue* child = new CSoapValue(ns, name, typestr.str(), (const char*) NULL,m_encode_xml);
        ForEachItemIn(x, valuearray)
        {
            CSoapValue& onechild = valuearray.item(x);
            child->m_children.append(*LINK(&onechild));
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, bool encodeXml)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        sv->add_child(new CSoapValue(ns, name, type, value, encodeXml));
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, const char* value, IProperties& attrs)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        if (name && *name)
        {
            CSoapValue* child = new CSoapValue(ns, name, type, value, false);
            sv->add_child(child);
            sv = child;
        }
        Owned<IPropertyIterator> piter = attrs.getIterator();       
        for (piter->first(); piter->isValid(); piter->next())
        {
            const char *propkey = piter->getPropKey();
            sv->add_attribute(propkey, attrs.queryProp(propkey));
        }
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, const char* value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, value, m_encode_xml));
        else
            sv->m_value.append(value);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, int value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, double value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, float value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, unsigned long value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.appendulong(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, long value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.appendlong(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, __int64 value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer longstr;
        longstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, longstr.str(), m_encode_xml));
        else
            sv->m_value.append(longstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, unsigned int value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer valstr;
        valstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, valstr.str(), m_encode_xml));
        else
            sv->m_value.append(valstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, unsigned short value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer valstr;
        valstr.append(value);

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, valstr.str(), m_encode_xml));
        else
            sv->m_value.append(valstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* type, bool value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        StringBuffer valstr(value ? "1" : "0");

        if (name && *name)
            sv->add_child(new CSoapValue(ns, name, type, valstr.str(), m_encode_xml));
        else
            sv->m_value.append(valstr);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, StringArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        int len = value.length();
        StringBuffer typestr;
        typestr.append("string[");
        typestr.append(len);
        typestr.append("]");

        CSoapValue* child = new CSoapValue(ns, name, typestr.str(), (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            const char* oneelem = (const char*)value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem, m_encode_xml);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, IntArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            int oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, ShortArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            short oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, Int64Array& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            __int64 oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, BoolArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            bool oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, FloatArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            float oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_value(const char* path, const char* ns, const char* name, const char* childns, 
                           const char* childname, const char* childtype, DoubleArray& value)
{
    CSoapValue* sv = ensure(ns, path);
    if(sv != NULL)
    {
        CSoapValue* child = new CSoapValue(ns, name, "int", (const char*) NULL, m_encode_xml);
        ForEachItemIn(x, value)
        {
            double oneelem = value.item(x);
            CSoapValue* onechild = new CSoapValue(childns, childname, childtype, oneelem);
            onechild->m_is_array_element = true;
            child->add_child(onechild);
        }
        sv->add_child(child);
    }
}

void CSoapValue::add_attribute(const char* name, const char* value)
{
    if (!name || !*name)
        return;
    if (!m_attributes)
        m_attributes.setown(createProperties());
    m_attributes->setProp(name, value);
}


void CSoapValue::serializeChildren(StringBuffer& outbuf, CMimeMultiPart* multipart)
{
    ForEachItemIn(x, m_children)
    {
        CSoapValue& onechild = m_children.item(x);
        onechild.serialize(outbuf, multipart);
    }
}

void CSoapValue::serialize(StringBuffer& outbuf, CMimeMultiPart* multipart)
{
    outbuf.append("<");
    if(m_ns.length() > 0)
    {
        outbuf.append(m_ns.get());
        outbuf.append(":");
    }
    outbuf.append(m_name.get());

    if(m_type.length() > 0 && !stricmp(m_type.get(), "Attachment"))
    {
        if(m_value.length() > 0)
        {
            outbuf.append(" href=\"cid:").append(m_name.get()).append("\"/>");
            CMimeBodyPart* onepart = new CMimeBodyPart("text/xml", "8bit", m_name.get(), "", &m_value);
            multipart->addBodyPart(onepart);
        }
        else
        {
            outbuf.append("/>");
        }
    }
    else
    {
        /* // we don't serialize type else where
        if(m_type.length() > 0)
        {
            const char* lbracket = strchr(m_type.get(), '[');
    
            const char* type_ns;
            if(!m_is_array_element)
                type_ns = "SOAP-ENC";
            else
                type_ns = "xsi";

            if(lbracket != NULL)
            {
                outbuf.append(" ").append(type_ns).append(":arrayType=\"xsd:");
            }
            else
            {
                outbuf.append(" ").append(type_ns).append(":type=\"xsd:");
            }

            outbuf.append(m_type.get()).append("\"");
        }
        */

        serialize_attributes(outbuf);

        outbuf.append(">");

        if(m_value.length() > 0)
        {
            if (m_encode_xml)
                encodeUtf8XML(m_value.str(), outbuf);
            else
                outbuf.append(m_value.str());
        }

        StringBuffer childrenbuf;
        serializeChildren(childrenbuf, multipart);
        outbuf.append(childrenbuf); 

        outbuf.append("</");
    
        if(m_ns.length() > 0)
        {
            outbuf.append(m_ns.get());
            outbuf.append(":");
        }
        outbuf.append(m_name.get());
        outbuf.append(">");
    }

}

void CSoapValue::serializeContent(StringBuffer& outbuf, CMimeMultiPart* multipart)
{
    if(m_value.length() > 0)
    {
        if (m_encode_xml)
        {
            StringBuffer encoded_value;
            encodeUtf8XML(m_value.str(), encoded_value);
            outbuf.append(encoded_value.str());
        }
        else
        {
            outbuf.append(m_value.str());
        }
    }

    StringBuffer childrenbuf;
    serializeChildren(childrenbuf, multipart);
    outbuf.append(childrenbuf); 
}


void CSoapValue::simple_serializeChildren(StringBuffer& outbuf)
{
    ForEachItemIn(x, m_children)
    {
        CSoapValue& onechild = m_children.item(x);
        onechild.simple_serialize(outbuf);
    }
}

inline bool isWhiteSpace(const char ch){return (ch==' ' || ch=='\t' || ch=='\n' || ch=='\r');}

void CSoapValue::serialize_attributes(StringBuffer& outbuf)
{
    ::serializeAttributes(outbuf,m_attributes);
}

void CSoapValue::simple_serialize(StringBuffer& outbuf)
{
    outbuf.append("<").append(m_name.get()).append(">");

    serialize_attributes(outbuf);

    if(m_value.length() > 0)
    {
        bool allwhite=true;
        for(const char *finger=m_value.str(); *finger && allwhite; finger++)
            allwhite = isWhiteSpace(*finger);

        if (!allwhite)
            encodeUtf8XML(m_value.str(), outbuf);
    }
    
    StringBuffer childrenbuf;
    simple_serializeChildren(childrenbuf);
    outbuf.append(childrenbuf);
    
    outbuf.append("</").append(m_name.get()).append(">");
}
