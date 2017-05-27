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

#ifndef _HTTPTRANSPORT_IPP__
#define _HTTPTRANSPORT_IPP__

//Jlib
#include "jsocket.hpp"

#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl DECL_EXPORT
#else
    #define esp_http_decl DECL_IMPORT
#endif

//ESP Bindings
#include "http/platform/httptransport.hpp"

#include "espcontext.hpp"
#include "espsession.ipp"

#include "bindutil.hpp"

#include "xslprocessor.hpp"

#define POST_METHOD "POST"
#define GET_METHOD "GET"
#define HEAD_METHOD "HEAD"
#define OPTIONS_METHOD "OPTIONS"

#define UNKNOWN_METHOD_ERROR -1;

#define MAX_HTTP_HEADER_LEN 4094

enum MessageLogFlag
{
    LOGALL = 0,
    LOGHEADERS = 1,
    LOGCONTENT = 2
};

class esp_http_decl CHttpMessage : implements IHttpMessage, public CInterface
{
protected:
    ISocket&     m_socket;
    Owned<IBufferedSocket> m_bufferedsocket;

    StringAttr   m_content_type;
    __int64      m_content_length;
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
#ifdef USE_LIBMEMCACHED
    StringBuffer allParameterString;
#endif

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
    __int64 getContentLength(){return m_content_length;}
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

        return ( (m_content_length > 0 || Utils::strncasecmp(HTTP_TYPE_FORM_ENCODED,type,sizeof(HTTP_TYPE_FORM_ENCODED)-1)==0)
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

    virtual bool isUpload(bool checkMimeType=true)
    {
        if(m_queryparams && m_queryparams->hasProp("upload_"))
        {
            if (!checkMimeType || !Utils::strncasecmp(m_content_type.get(), "multipart", strlen("multipart")))
                return true;
        }
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
    sub_serv_json_builder,
    sub_serv_wsdl,
    sub_serv_xsd,
    sub_serv_config,
    sub_serv_getversion,
    sub_serv_reqsamplexml,
    sub_serv_respsamplexml,
    sub_serv_respsamplejson,
    sub_serv_reqsamplejson,
    sub_serv_file_upload,

    sub_serv_max
} sub_service;

const char* getSubServiceDesc(sub_service stype);

class esp_http_decl CHttpRequest : public CHttpMessage
{
private:
    StringAttr    m_httpMethod;
    StringAttr    m_httpPath;
    StringAttr    m_espServiceName;
    StringAttr    m_espMethodName;
    StringAttr    m_espPathEx;
    sub_service     m_sstype;
    bool            m_pathIsParsed;
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
#ifdef USE_LIBMEMCACHED
    unsigned createUniqueRequestHash(bool cacheGlobal, const char* msgType);
#endif

    virtual void setMaxRequestEntityLength(int len) {m_MaxRequestEntityLength = len;}
    virtual int getMaxRequestEntityLength() { return m_MaxRequestEntityLength; }

    bool readContentToBuffer(MemoryBuffer& fileContent, __int64& bytesNotRead);
    bool readUploadFileName(CMimeMultiPart* mimemultipart, StringBuffer& fileName, MemoryBuffer& contentBuffer, __int64& bytesNotRead);
    IFile* createUploadFile(StringBuffer netAddress, const char* filePath, StringBuffer& fileName);
    virtual int readContentToFiles(StringBuffer netAddress, StringBuffer path, StringArray& fileNames);
    virtual void readUploadFileContent(StringArray& fileNames, StringArray& files);
};

class esp_http_decl CHttpResponse : public CHttpMessage
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

#ifdef USE_LIBMEMCACHED
#include <libmemcached/memcached.hpp>
#include <libmemcached/util.h>

class ESPMemCached : public CInterface
{
    memcached_st* connection = nullptr;
    memcached_pool_st* pool = nullptr;
    StringAttr options;
    bool initialized = false;

public :
    ESPMemCached()
    {
#if (LIBMEMCACHED_VERSION_HEX < 0x01000010)
        VStringBuffer msg("Memcached Plugin: libmemcached version '%s' incompatible with min version>=1.0.10", LIBMEMCACHED_VERSION_STRING);
        ESPLOG(LogNormal, "%s", msg.str());
#endif
    }

    ~ESPMemCached()
    {
        if (pool)
        {
            memcached_pool_release(pool, connection);
            connection = nullptr;//For safety (from changing this destructor) as not implicit in either the above or below.
            memcached_st *memc = memcached_pool_destroy(pool);
            if (memc)
                memcached_free(memc);
        }
        else if (connection)//This should never be needed but just in case.
        {
            memcached_free(connection);
        }
    };

    bool init(const char * _options)
    {
        if (initialized)
            return initialized;

        options.set(_options);
        pool = memcached_pool(_options, strlen(_options));
        assertPool();

        setPoolSettings();
        connect();
        if (connection)
            initialized = checkServersUp();
        return initialized;
    }

    void setPoolSettings()
    {
        assertPool();
        const char * msg = "memcached_pool_behavior_set failed - ";
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_KETAMA, 1), msg);//NOTE: alias of MEMCACHED_DISTRIBUTION_CONSISTENT_KETAMA amongst others.
        memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_USE_UDP, 0);  // Note that this fails on early versions of libmemcached, so ignore result
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_NO_BLOCK, 0), msg);
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_CONNECT_TIMEOUT, 1000), msg);//units of ms.
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_SND_TIMEOUT, 1000000), msg);//units of mu-s.
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_RCV_TIMEOUT, 1000000), msg);//units of mu-s.
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BUFFER_REQUESTS, 0), msg);
        assertOnError(memcached_pool_behavior_set(pool, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1), "memcached_pool_behavior_set failed - ");
    }

    void connect()
    {
        assertPool();
        if (connection)
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
            memcached_pool_push(pool, connection);
        memcached_return_t rc;
        connection = memcached_pool_pop(pool, (struct timespec *)0 , &rc);
#else
            memcached_pool_release(pool, connection);
        memcached_return_t rc;
        connection = memcached_pool_fetch(pool, (struct timespec *)0 , &rc);
#endif
        assertOnError(rc, "memcached_pool_pop failed - ");
    }

    bool checkServersUp()
    {
        memcached_return_t rc;
        char* args = nullptr;
        OwnedMalloc<memcached_stat_st> stats;
        stats.setown(memcached_stat(connection, args, &rc));

        unsigned int numberOfServers = memcached_server_count(connection);
        if (numberOfServers < 1)
        {
            ESPLOG(LogMin,"Memcached: no server connected.");
            return false;
        }

        unsigned int numberOfServersDown = 0;
        for (unsigned i = 0; i < numberOfServers; ++i)
        {
            if (stats[i].pid == -1)//perhaps not the best test?
            {
                numberOfServersDown++;
                VStringBuffer msg("Memcached: Failed connecting to entry %u\nwithin the server list: %s", i+1, options.str());
                ESPLOG(LogMin, "%s", msg.str());
            }
        }
        if (numberOfServersDown == numberOfServers)
        {
            ESPLOG(LogMin,"Memcached: Failed connecting to ALL servers. Check memcached on all servers and \"memcached -B ascii\" not used.");
            return false;
        }

        //check memcached version homogeneity
        for (unsigned i = 0; i < numberOfServers-1; ++i)
        {
            if (!streq(stats[i].version, stats[i+1].version))
                DBGLOG("Memcached: Inhomogeneous versions of memcached across servers.");
        }
        return true;
    };

    bool exists(const char* partitionKey, const char* key)
    {
#if (LIBMEMCACHED_VERSION_HEX<0x53000)
        throw makeStringException(0, "memcached_exist not supported in this version of libmemcached");
#else
        memcached_return_t rc;
        size_t partitionKeyLength = strlen(partitionKey);
        if (partitionKeyLength)
            rc = memcached_exist_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key));
        else
            rc = memcached_exist(connection, key, strlen(key));

        if (rc == MEMCACHED_NOTFOUND)
            return false;
        else
        {
            assertOnError(rc, "'Exists' request failed - ");
            return true;
        }
#endif
    };

    const char* get(const char* partitionKey, const char* key, StringBuffer& out)
    {
        uint32_t flag = 0;
        size_t returnLength;
        memcached_return_t rc;

        OwnedMalloc<char> value;
        size_t partitionKeyLength = strlen(partitionKey);
        if (partitionKeyLength)
            value.setown(memcached_get_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), &returnLength, &flag, &rc));
        else
            value.setown(memcached_get(connection, key, strlen(key), &returnLength, &flag, &rc));

        if (value)
            out.set(value);

        StringBuffer keyMsg = "'Get' request failed - ";
        assertOnError(rc, appendIfKeyNotFoundMsg(rc, key, keyMsg));
        return out.str();
    };

    void set(const char* partitionKey, const char* key, const char* value, unsigned __int64 expireSec)
    {
        size_t partitionKeyLength = strlen(partitionKey);
        const char * msg = "'Set' request failed - ";
        if (partitionKeyLength)
            assertOnError(memcached_set_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), value, strlen(value), (time_t)expireSec, 0), msg);
        else
            assertOnError(memcached_set(connection, key, strlen(key), value, strlen(value), (time_t)expireSec, 0), msg);
    };

    void deleteKey(const char* partitionKey, const char* key)
    {
        memcached_return_t rc;
        size_t partitionKeyLength = strlen(partitionKey);
        if (partitionKeyLength)
            rc = memcached_delete_by_key(connection, partitionKey, partitionKeyLength, key, strlen(key), (time_t)0);
        else
            rc = memcached_delete(connection, key, strlen(key), (time_t)0);
        assertOnError(rc, "'Delete' request failed - ");
    };

    void clear(unsigned when)
    {
        //NOTE: memcached_flush is the actual cache flush/clear/delete and not an io buffer flush.
        assertOnError(memcached_flush(connection, (time_t)(when)), "'Clear' request failed - ");
    };

    void assertOnError(memcached_return_t rc, const char * _msg)
    {
        if (rc != MEMCACHED_SUCCESS)
        {
            VStringBuffer msg("Memcached: %s%s", _msg, memcached_strerror(connection, rc));
            ESPLOG(LogNormal, "%s", msg.str());
        }
    };

    const char * appendIfKeyNotFoundMsg(memcached_return_t rc, const char * key, StringBuffer & target) const
    {
        if (rc == MEMCACHED_NOTFOUND)
            target.append("(key: '").append(key).append("') ");
        return target.str();
    };

    void assertPool()
    {
        if (!pool)
        {
            StringBuffer msg = "Memcached: Failed to instantiate server pool with:";
            msg.newline().append(options);
            ESPLOG(LogNormal, "%s", msg.str());
        }
    }
};
#endif //USE_LIBMEMCACHED

#endif
