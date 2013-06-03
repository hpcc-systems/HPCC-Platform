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

#ifndef _HTTPTRANSPORT_IPP__
#define _HTTPTRANSPORT_IPP__

//Jlib
#include "jsocket.hpp"

//ESP Bindings
#include "http/platform/httptransport.hpp"

#include "espcontext.hpp"
#include "espsession.ipp"

#include "bindutil.hpp"

#include "xslprocessor.hpp"


#define POST_METHOD "POST"
#define GET_METHOD "GET"
#define HEAD_METHOD "HEAD"

#define UNKNOWN_METHOD_ERROR -1;

#define MAX_HTTP_HEADER_LEN 4094

interface IEspHttpException : extends IException
{
    virtual const char* getHttpStatus() = 0;
};

class CEspHttpException: public CInterface, public IEspHttpException
{
public:
    IMPLEMENT_IINTERFACE;

    CEspHttpException(int code, const char *_msg, const char* _httpstatus) : errcode(code), msg(_msg), httpstatus(_httpstatus){ };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("CEspHttpException: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };
    virtual const char* getHttpStatus() {return httpstatus.get(); }

private:
    int errcode;
    StringAttr msg;
    StringAttr httpstatus;
};

IEspHttpException* createEspHttpException(int code, const char *_msg, const char* _httpstatus);

enum MessageLogFlag
{
    LOGALL = 0,
    LOGHEADERS = 1,
    LOGCONTENT = 2
};

class CHttpMessage : public CInterface, implements IHttpMessage
{
protected:
    ISocket&     m_socket;
    Owned<IBufferedSocket> m_bufferedsocket;

    StringAttr   m_content_type;
    int          m_content_length;
    __int64      m_content_length64;
    StringBuffer m_content;
    StringBuffer m_header;
    OwnedIFileIOStream m_content_stream;
    StringAttr   m_version;
    StringAttr   m_host;
    int          m_port;
    StringAttr   m_paramstr;
    int m_supportClientXslt;
    bool         m_isForm;

    int m_paramCount;
    int m_attachCount;
    Owned<IProperties> m_queryparams;
    MapStrToBuf  m_attachments;
    StringArray  m_headers;

    Owned<IEspContext> m_context;
    IArrayOf<CEspCookie> m_cookies;

    Owned<CMimeMultiPart> m_multipart;

    int parseOneHeader(char* oneline);
    virtual void parseCookieHeader(char* cookiestr);
    virtual int parseFirstLine(char* oneline);
    int readContent();  
    int readContentTillSocketClosed();
    virtual void addParameter(const char* paramname, const char *value);
    virtual void addAttachment(const char* name, StringBuffer& value);

    virtual StringBuffer& constructHeaderBuffer(StringBuffer& headerbuf, bool inclLength);
    virtual int processHeaders(IMultiException *me);

public:
    IMPLEMENT_IINTERFACE;
    
    CHttpMessage(ISocket& socket);
    virtual ~CHttpMessage();

    virtual ISocket* getSocket() {return &m_socket;};

    StringArray &queryHeaders(){return m_headers;}

    virtual int receive(IMultiException *me) { return receive(false, me); }
    virtual int receive(bool alwaysReadContent, IMultiException *me);
    virtual int send();
    virtual int startSend();
    virtual int sendChunk(const char *chunk);
    virtual int sendFinalChunk(const char *chunk);
    virtual int close();

    virtual bool supportClientXslt();
    unsigned getContentLength(){return m_content_length;}
    const char *queryContent(){return m_content.str();}
    const char *queryHeader() { return m_header.str(); }
    void logSOAPMessage(const char* message, const char* prefix = NULL);
    void logMessage(const char *message, const char *prefix = NULL, const char *find = NULL, const char *replace = NULL);
    void logMessage(MessageLogFlag logFlag, const char *prefix = NULL);

    virtual StringBuffer& getContent(StringBuffer& content);
    virtual void setContent(const char* content);
    virtual void setContent(unsigned len, const char* content);
    virtual void setownContent(char* content);
    virtual void setownContent(unsigned len, char* content);
    virtual void setContent(IFileIOStream* stream);
    //virtual void appendContent(const char* content);
    virtual StringBuffer& getContentType(StringBuffer& contenttype);
    virtual void setContentType(const char* contenttype);
    virtual void setVersion(const char* version);
    virtual void setHost(const char* host) {m_host.set(host);};
    virtual StringBuffer& getHost(StringBuffer& host) {return host.append(m_host.get());};
    const char *queryHost(){return m_host.get();}
    virtual void setPort(int port) {m_port = port;};
    virtual int getPort() {return m_port;};

    void getServAddress(StringBuffer &host, short &port)
    {
        port = m_socket.name(host.reserveTruncate(32), 32);
    }

    virtual StringBuffer& getParameter(const char* paramname, StringBuffer& paramval);
    virtual StringBuffer& getAttachment(const char* name, StringBuffer& attachment);
    virtual StringBuffer& getParamStr(StringBuffer& paramval);
    virtual const char *queryParamStr(){return m_paramstr.get();}
    virtual void setHeader(const char* headername, const char* headerval);
    virtual void addHeader(const char* headername, const char* headerval);
    virtual StringBuffer& getHeader(const char* headername, StringBuffer& headerval);
    virtual int getParameterCount(){return m_paramCount;}
    virtual int getAttachmentCount(){return m_attachCount;}
    virtual IProperties *queryParameters();
    virtual IProperties *getParameters();
    virtual MapStrToBuf *queryAttachments()
    {
        return &m_attachments;
    }

    virtual void setOwnContext(IEspContext* ctx)
    {
        if(ctx != NULL)
            m_context.setown(ctx);
    }

    virtual IEspContext* queryContext()
    {
        return m_context.get();
    }
    bool hasContentType(const char *type)
    {
        if (type==NULL || *type==0)
            return (m_content_type.length()==0);

        return ( (m_content_length > 0 || m_content_length64 > 0 || Utils::strncasecmp(HTTP_TYPE_FORM_ENCODED,type,sizeof(HTTP_TYPE_FORM_ENCODED)-1)==0)
            && Utils::strncasecmp(m_content_type.get(), type, strlen(type)) == 0);
    }

    virtual bool isSoapMessage();
    virtual bool isFormSubmission();
    virtual IArrayOf<CEspCookie>& queryCookies()
    {
        return m_cookies;
    }
    virtual CEspCookie* queryCookie(const char* name)
    {
        ForEachItemIn(x, m_cookies)
        {
            CEspCookie* cookie = &m_cookies.item(x);
            if(cookie != NULL && strcmp(cookie->getName(), name) == 0)
                return cookie;
        }
        return NULL;
    }
    virtual void addCookie(CEspCookie* cookie)
    {
        if(cookie != NULL)
        {
            ForEachItemInRev(x, m_cookies)
            {
                CEspCookie* curcookie = &m_cookies.item(x);
                if(curcookie != NULL && stricmp(curcookie->getName(), cookie->getName()) == 0)
                    m_cookies.remove(x);
            }
            m_cookies.append(*cookie);
        }
    }

    virtual CMimeMultiPart* queryMultiPart()
    {
        if(m_multipart.get() != NULL)
            return m_multipart.get();
        else
        {
            if(Utils::strncasecmp(m_content_type.get(), "multipart", strlen("multipart")) != 0)
                return NULL;
            else
            {
                m_multipart.setown(new CMimeMultiPart("1.0", m_content_type.get(), "", "", ""));
                m_multipart->unserialize(m_content_type.get(), m_content_length, m_content.str());
                return m_multipart.get();
            }
        }   
    }
    virtual bool isTextMessage()
    {
        if ((m_content_type.length() > 0) && 
            (Utils::strncasecmp(m_content_type.get(), "text", 4) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_SOAP, strlen(HTTP_TYPE_SOAP)) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_MULTIPART_RELATED, strlen(HTTP_TYPE_MULTIPART_RELATED)) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_MULTIPART_FORMDATA, strlen(HTTP_TYPE_MULTIPART_FORMDATA)) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_FORM_ENCODED, strlen(HTTP_TYPE_FORM_ENCODED)) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_SVG_XML, strlen(HTTP_TYPE_SVG_XML)) == 0 
             || Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_JAVASCRIPT, strlen(HTTP_TYPE_JAVASCRIPT)) == 0))
        {
            return true;
        }

        return false;
    }

    virtual bool isUpload()
    {
        if(m_queryparams && m_queryparams->hasProp("upload_") && !Utils::strncasecmp(m_content_type.get(), "multipart", strlen("multipart")))
            return true;
        return false;
    }
};


typedef enum sub_service_
{
    sub_serv_unknown,
    sub_serv_root,
    sub_serv_main,
    sub_serv_service,
    sub_serv_method,
    sub_serv_files,
    sub_serv_itext,
    sub_serv_iframe,
    sub_serv_content,
    sub_serv_result,
    sub_serv_index,
    sub_serv_index_redirect,
    sub_serv_form,
    sub_serv_xform,
    sub_serv_query,
    sub_serv_instant_query,
    sub_serv_soap_builder,
    sub_serv_roxie_builder,
    sub_serv_wsdl,
    sub_serv_xsd,
    sub_serv_config,
    sub_serv_php,
    sub_serv_getversion,
    sub_serv_reqsamplexml,
    sub_serv_respsamplexml,
    sub_serv_file_upload,

    sub_serv_max
} sub_service;

const char* getSubServiceDesc(sub_service stype);

class CHttpRequest : public CHttpMessage
{
private:
    StringBuffer    m_httpMethod;
    StringBuffer    m_httpPath;
    bool            m_pathIsParsed;
    StringBuffer    m_espServiceName;
    StringBuffer    m_espMethodName;
    StringBuffer    m_espPathEx;
    sub_service     m_sstype;
    bool            m_authrequired;
    int             m_MaxRequestEntityLength;
    ESPSerializationFormat respSerializationFormat;

    virtual int parseFirstLine(char* oneline);
    virtual StringBuffer& constructHeaderBuffer(StringBuffer& headerbuf, bool inclLen);
    virtual int processHeaders(IMultiException *me);
    virtual void parseCookieHeader(char* cookiestr);

public:
    

    CHttpRequest(ISocket& socket);
    virtual ~CHttpRequest();
    
    virtual void setMethod(const char* method);
    virtual StringBuffer& getMethod(StringBuffer& method);
    virtual const char *queryMethod(){return m_httpMethod.str();}

    virtual const char* queryServiceName() { return m_espServiceName.str(); }
    virtual const char* queryServiceMethod() { return m_espMethodName.str(); }
    
    virtual void setPath(const char* path);
    virtual StringBuffer& getPath(StringBuffer& path);
    virtual const char *queryPath(){return m_httpPath.str();}

    virtual void parseQueryString(const char* querystr);

    virtual void parseEspPathInfo();
    virtual void getEspPathInfo(sub_service &sstype, StringBuffer *pathEx=NULL, StringBuffer *service=NULL, StringBuffer *method=NULL, bool makeupper=true);
    
    virtual void getBasicAuthorization(StringBuffer& userid, StringBuffer& password,StringBuffer& Realm);
    virtual void getBasicRealm(StringBuffer& realm);
    virtual int getPeerPort();
    virtual StringBuffer& getPeer(StringBuffer& peer);

    virtual int receive(IMultiException *me);

    void updateContext();

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }

    bool readContentToBuffer(MemoryBuffer& fileContent, __int64& bytesNotRead);
    bool readUploadFileName(CMimeMultiPart* mimemultipart, StringBuffer& fileName, MemoryBuffer& contentBuffer, __int64& bytesNotRead);
    IFile* createUploadFile(StringBuffer netAddress, const char* filePath, StringBuffer& fileName);
    virtual int readContentToFiles(StringBuffer netAddress, StringBuffer path, StringArray& fileNames);
};

class CHttpResponse : public CHttpMessage
{
private:
    StringAttr  m_status;
    unsigned int m_timeout;

    virtual int parseFirstLine(char* oneline);
    virtual StringBuffer& constructHeaderBuffer(StringBuffer& headerbuf, bool inclLen);
    virtual int processHeaders(IMultiException *me);
    virtual void parseCookieHeader(char* cookiestr);
    virtual void parseOneCookie(char* cookiestr);

public:
    CHttpResponse(ISocket& socket);
    virtual ~CHttpResponse();
    virtual void setStatus(const char* status);
    virtual StringBuffer& getStatus(StringBuffer& status);
    virtual void sendBasicChallenge(const char* realm, bool includeContent);
    virtual void sendBasicChallenge(const char* realm, const char* content);

    virtual bool httpContentFromFile(const char *filepath);
    virtual bool handleExceptions(IXslProcessor *xslp, IMultiException *me, const char *serv, const char *meth, const char *errorXslt);

    virtual void redirect(CHttpRequest &req, const char *url)
    {
#if 0
        setStatus(HTTP_STATUS_OK);
        setContentType(HTTP_TYPE_TEXT_HTML_UTF8);
        StringBuffer content;
        content.appendf("<html><head><meta http-equiv=\"REFRESH\" content=\"0;url='%s'\"></head><body></body></html>", url);
        setContent(content.str());
#else
        setStatus(HTTP_STATUS_REDIRECT);
        queryHeaders().append(VStringBuffer("Location: %s", url));
#endif
        send();
    }

    virtual int receive(IMultiException *me);
    virtual int receive(bool alwaysReadContent, IMultiException *me);
    virtual int sendException(IEspHttpException* e);

    void setTimeOut(unsigned int timeout);
    void setETagCacheControl(const char *etag, const char *contenttype);
    void CheckModifiedHTTPContent(bool modified, const char *lastModified, const char *etag, const char *contenttype, MemoryBuffer &content);
};

inline bool canRedirect(CHttpRequest &req)
{
    if (req.queryParameters()->hasProp("rawxml_"))
        return false;
    IEspContext *ctx = req.queryContext();
    if (ctx && ctx->getResponseFormat()!=ESPSerializationANY)
        return false;
    return true;
}

inline bool checkRedirect(IEspContext &ctx)
{
    if (ctx.getResponseFormat()!=ESPSerializationANY)
        return false;
    return true;
}

inline bool skipXslt(IEspContext &context)
{
    return (context.getResponseFormat()!=ESPSerializationANY);  //for now
}

#endif
