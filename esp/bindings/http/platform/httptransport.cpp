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
#pragma warning(disable : 4786)
#include "platform.h"
#include "esphttp.hpp"
#include "persistent.hpp"

#ifdef _WIN32
  #include <algorithm>
#endif

//Jlib
#include "jliball.hpp"

//ESP Binidings
#include "http/platform/httptransport.ipp"
#include "bindutil.hpp"
#ifdef _USE_ZLIB
#include "zcrypt.hpp"
#endif

bool httpContentFromFile(const char *filepath, StringBuffer &mimetype, MemoryBuffer &fileContents, bool &checkModifiedTime, StringBuffer &lastModified, StringBuffer &etag)
{
    StringBuffer strfile(filepath);

    if (!checkFileExists(strfile.str()))
        if (!checkFileExists(strfile.toUpperCase().str()))
            if (!checkFileExists(strfile.toLowerCase().str()))
                return false;

    Owned<IFile> file = createIFile(strfile.str());
    if (file && file->isFile()==fileBool::foundYes)
    {
        if (checkModifiedTime)
        {
            CDateTime createTime, accessedTime, modifiedTime;
            file->getTime( &createTime,  &modifiedTime, &accessedTime);
            if (etag.length() || !lastModified.length())
            {
                StringBuffer etagSource, etagForThisFile;
                modifiedTime.getString(lastModified.clear());
                etagSource.appendf("%s+%s", filepath, lastModified.str());
                etagForThisFile.append(hashc((unsigned char *)etagSource.str(), etagSource.length(), 0));
                if ( !streq(etagForThisFile.str(), etag.str()))
                    etag.set(etagForThisFile);
                else
                {
                    checkModifiedTime = false;
                    return true;
                }
            }
            else
            {
                StringBuffer lastModifiedForThisFile;
                modifiedTime.getString(lastModifiedForThisFile);
                if ( !streq(lastModifiedForThisFile.str(), lastModified.str()))
                    lastModified.set(lastModifiedForThisFile);
                else
                {
                    checkModifiedTime = false;
                    return true;
                }
            }
        }

        Owned<IFileIO> io = file->open(IFOread);
        if (io)
        {
            size32_t filesize = (size32_t)io->size();
            io->read(0, filesize, fileContents.reserveTruncate(filesize));
            mimetype.set(mimeTypeFromFileExt(strrchr(filepath, '.')));
            return true;
        }
    }
    
    return false;
}

bool xmlContentFromFile(const char *filepath, const char *stylesheet, StringBuffer &fileContents)
{
    StringBuffer strfile(filepath);

    if (!checkFileExists(strfile.str()))
        if (!checkFileExists(strfile.toUpperCase().str()))
            if (!checkFileExists(strfile.toLowerCase().str()))
                return false;

    fileContents.loadFile(strfile.str());
    if (stylesheet && *stylesheet)
    {
        StringBuffer stylesheetLine;
        stylesheetLine.appendf("<?xml-stylesheet type=\"text/xsl\" href=\"%s\"?>\n", stylesheet);

        unsigned fileSize = fileContents.length();
        const char* ptr0 = fileContents.str();
        char* ptr = (char*) ptr0;
        while (*ptr)
        {
            if((ptr[0] == '<') && (ptr[1] != '!'))
            {
                if (ptr[1] != '?')
                {
                    fileContents.insert(ptr - ptr0, stylesheetLine);
                    break;
                }
                else
                {
                    if ((strncmp(ptr, "<?xml-stylesheet ", 17)==0) || (strncmp(ptr, "<?xml-stylesheet?", 17)==0))
                    {//Found the line to be replaced
                        char* ptr1 = ptr + 17;
                        while (*ptr1)
                        {
                            if (ptr1[0] == '>')
                            {
                                if (ptr1[1] != '\n')
                                    fileContents.remove(ptr - ptr0, ptr1 - ptr + 1);
                                else
                                    fileContents.remove(ptr - ptr0, ptr1 - ptr + 2);

                                fileContents.insert(ptr - ptr0, stylesheetLine);
                                break;
                            }
                            ptr1++;
                        }

                        break;
                    }
                }
            }
            ptr++;
        }
    }
    return true;
}

enum SOAPTag
{
    NONE = 0,
    ENVELOPE = 1,
    HEADER = 2,
    SECURITY = 3,
    USERNAMETOKEN = 4,
    PASSWORD = 5,
    BODY = 6
};

class SOAPMessageLog : implements IPTreeNotifyEvent, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    SOAPMessageLog() : m_readNext(true), m_foundPassword(false), m_lastTagFound(NONE), m_pPassword(NULL), m_pStart(NULL)
    {
        m_messageForLog.clear();
        m_skipTag.clear();
    };
    void logMessage(const char* message, const char* prefix)
    {
        if (!message || !*message)
            return;

        m_message = message;
        m_pStart = (char*) message;

        Owned<IPullPTreeReader> reader = createPullXMLStringReader(m_message, *this, ptr_ignoreNameSpaces);
        while(m_readNext && reader->next())
        {
            if (m_foundPassword)
            {
                m_pPassword = (char*) m_message + reader->queryOffset();
                m_foundPassword = false; //for another password
            }
        }

        if ((m_pStart == message) || (m_messageForLog.length() < 1))
            logNow(message, prefix);
        else
        {
            m_messageForLog.append(m_pStart);
            logNow(m_messageForLog.str(), prefix);
        }
        return;
    }
    virtual void beginNode(const char *tag, bool arrayitem, offset_t startOffset) override
    {
        if (m_skipTag.length() > 0)
            return;

        if (strieq(tag, "Body"))
        {//no more password
            m_readNext = false;
            return;
        }

        switch (m_lastTagFound)
        {
            case NONE:
                if (!strieq(tag, "Envelope"))
                    m_readNext = false;
                else
                    m_lastTagFound = ENVELOPE;
                break;
            case ENVELOPE:
                if (!strieq(tag, "Header"))
                    m_skipTag.append(tag);
                else
                    m_lastTagFound = HEADER;
                break;
            case HEADER:
                if (!strieq(tag, "Security"))
                    m_skipTag.append(tag);
                else
                    m_lastTagFound = SECURITY;
                break;
            case SECURITY:
                if (!strieq(tag, "UsernameToken"))
                    m_skipTag.append(tag);
                else
                    m_lastTagFound = USERNAMETOKEN;
                break;
            case USERNAMETOKEN:
                if (!strieq(tag, "Password"))
                    m_skipTag.append(tag);
                else
                    m_lastTagFound = PASSWORD;
                break;
            default:
                m_skipTag.append(tag);
                break;
        }
    }
    virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
    {
        if (m_skipTag.length() > 0)
        {
            if (strieq(tag, m_skipTag.str()))
                m_skipTag.clear();

            return;
        }

        switch (m_lastTagFound)
        {
            case SECURITY:
                if (strieq(tag, "Security"))
                    m_readNext = false;
                break;
            case USERNAMETOKEN:
                if (strieq(tag, "UsernameToken"))
                    m_lastTagFound = SECURITY;
                break;
            case PASSWORD:
                if (!strieq(tag, "Password") || !m_pPassword || !m_pStart)
                {//should not happen
                    m_readNext = false;
                    return;
                }

                m_messageForLog.append(m_pStart, 0, m_pPassword - m_pStart);
                m_messageForLog.append("(hidden)");
                m_pStart = m_pPassword + length; //remember the rest of message

                //Go back to SuerNameToken node
                m_lastTagFound = USERNAMETOKEN;
                break;
        }
        return;
    }
    virtual void beginNodeContent(const char *tag)
    {
        if (m_skipTag.length() > 0)
            return;

        if (m_lastTagFound == PASSWORD)
            m_foundPassword = true;
        return;
    }
    virtual void newAttribute(const char *name, const char *value)
    {
        return;
    }
private:
    void logNow(const char* message, const char* prefix)
    {
        if (prefix && *prefix)
            DBGLOG("%s%s", prefix, message);
        else
            DBGLOG("%s", message);

        return;
    }
private:
    StringBuffer m_messageForLog;
    StringBuffer m_skipTag;
    SOAPTag m_lastTagFound;
    const char *m_message;
    char *m_pStart;
    char *m_pPassword;
    bool m_foundPassword;
    bool m_readNext;
};

/***************************************************************************
                CHttpMessage Implementation
This class implements common functions shared by both CHttpRequest 
and CHttpResponse
****************************************************************************/


CHttpMessage::CHttpMessage(ISocket& socket) : m_socket(socket)
{
    m_bufferedsocket.setown(createBufferedSocket(&socket));
    m_content_length = -1;
    m_port = 80;
    m_paramCount = 0;
    m_attachCount = 0;
    m_supportClientXslt=-1;
    m_isForm = false;
};


CHttpMessage::~CHttpMessage()
{
    try
    {
        m_bufferedsocket.clear();
        m_context.clear();
        m_queryparams.clear();
        m_content_stream.clear();
    }
    catch(...)
    {
        IERRLOG("In CHttpMessage::~CHttpMessage() -- Unknown exception.");
    }
};

int CHttpMessage::parseOneHeader(char* oneline)
{
    if(!oneline)
        return -1;
    char* name = oneline;
    while(*name == ' ')
        name++;
    char* end = name;
    while(*end != '\0' && *end != ':')
        end++;

    char* value;
    if (*end == ':')
    {
        *end = '\0';
        value = end + 1;
    }
    else
        value = end;

    while(*value == ' ')
        value++;
    
    if(!stricmp(name, "Content-Type"))
    {
        m_content_type.set(value);
    }
    else if(!stricmp(name, "Content-Length"))
    {
        if(value != NULL)
            m_content_length = atoi64_l(value,strlen(value));
    }
    else if(!stricmp(name, "Host"))
    {
        if(value != NULL)
        {
            char* colon = strchr(value, ':');
            if(colon != NULL)
            {
                *colon = '\0';
                char* port = colon + 1;
                if(port != NULL)
                {
                    m_port = atoi(port);
                }
            }
            m_host.set(value);
        }
    }
    else if(!stricmp(name, "Set-Cookie") || !stricmp(name, "Cookie"))
    {
        parseCookieHeader(value);
    }
    else
    {
        addHeader(name, value); //Insert into headers hashtable
    }

    return 0;
}


bool CHttpMessage::supportClientXslt()
{
    if (m_supportClientXslt==-1)
    {
        bool ie6=false;
        bool moz5=false;
        bool opera=false;
        bool ff=false;

        StringBuffer uastr;
        getHeader("User-Agent", uastr);
        
        char *uagent=strdup(uastr.str());
        char *saveptr;
        char *token = strtok_r(uagent, "();", &saveptr);
        while(token)
        {
            while (*token==' ') token++;
            if (!strnicmp(token, "msie", 4))
                ie6=(token[5]>='6');
            else if (!strnicmp(token, "mozilla", 7))
                moz5=(token[8]>='5');
            else if (!strnicmp(token, "opera", 5))
                opera=true;
            else if (!strnicmp(token, "firefox", 7))
                ff=true;
            token = strtok_r( NULL, "();" , &saveptr);
        }
        free(uagent);
        m_supportClientXslt=(ff || moz5 || (ie6 && !opera)) ? 1 : 0;
    }
    return m_supportClientXslt==1;
}


void CHttpMessage::addParameter(const char* paramname, const char *value)
{
    if (!m_queryparams)
        m_queryparams.setown(createProperties(false));

    if (strcmp(paramname,"form")==0)
        m_isForm = true;

    m_queryparams->setProp(paramname, value);
    m_paramCount++;
    allParameterString.append("&").append(paramname).append("=").append(value);
}

StringBuffer& CHttpMessage::getParameter(const char* paramname, StringBuffer& paramval)
{
    if (m_queryparams)
    {
        const char *value = m_queryparams->queryProp(paramname);
        if (value)
            paramval.append(value);
    }
    return paramval;
}

void CHttpMessage::addAttachment(const char* name, StringBuffer& value)
{
    if(name != NULL)
    {
        m_attachments.setValue(name, value);
        m_attachCount++;
    }
}

StringBuffer& CHttpMessage::getAttachment(const char* name, StringBuffer& attachment)
{
    StringBuffer *value = m_attachments.getValue(name);
    if (value)
    {
        attachment.append(value->length(), value->str());
    }
    return attachment;
}

StringBuffer& CHttpMessage::getParamStr(StringBuffer& ret)
{
    return ret.append(m_paramstr);
}

IProperties *CHttpMessage::queryParameters()
{
    if (!m_queryparams)
        m_queryparams.setown(createProperties(false));
    return m_queryparams.get();
}

IProperties *CHttpMessage::getParameters()
{
    if (!m_queryparams)
        m_queryparams.setown(createProperties(false));
    return m_queryparams.getLink();
}

int CHttpMessage::processHeaders(IMultiException *me)
{
    return 0;
}

int CHttpMessage::readContent()
{
    char buf[1024 + 1];
    int buflen = 1024;

    if(m_content_length > 0)
    {
        __int64 totallen = m_content_length;
        if(buflen > totallen)
            buflen = (int)totallen;
        int readlen = 0;    
        for(;;)
        {
            readlen = m_bufferedsocket->read(buf, buflen);
            if(readlen < 0)
            {
                DBGLOG(">> Socket timed out because of incorrect Content-Length passed in from the other side");
                break;
            }
            if(readlen == 0)
                break;
            buf[readlen] = 0;
            m_content.append(readlen, buf);
            totallen -= readlen;
            if(totallen <= 0)
                break;
            if(buflen > totallen)
                buflen = (int)totallen;
        }
        
        return 0;
    }
    return 0;
}

int CHttpMessage::readContentTillSocketClosed()
{
    const int buflen = 1024;
    char buf[buflen + 1];

    StringBuffer headerValue;
    getHeader("Transfer-Encoding", headerValue);

    if (!stricmp(headerValue.str(), "chunked"))//Transfer-Encoding: chunked
    {
        for(;;)
        {
            int readlen = m_bufferedsocket->readline(buf, buflen, NULL);
            if(readlen <= 0)
                break;
            buf[readlen] = 0;
            int chunkSize;
            sscanf(buf, "%x", &chunkSize);

            if (chunkSize<=0)
                break;

            while (chunkSize > 0)
            {
                const int len = min(chunkSize, buflen);
                readlen = m_bufferedsocket->read(buf, len);
                if(readlen <= 0)
                    break;
                chunkSize -= readlen;
                m_content.append(readlen, buf);
            }

            if (m_bufferedsocket->read(buf, 2) <= 0)//CR/LF
                break;
        }
    }
    else
    {
        for(;;)
        {
            int readlen = m_bufferedsocket->read(buf, buflen);
            if(readlen <= 0)
                break;
            buf[readlen] = 0;
            m_content.append(readlen, buf);
        }
    }
    return 0;
}

int CHttpMessage::receive(bool alwaysReadContent, IMultiException *me)
{
    //Set the auth to AUTH_STATUS_NA as default. Later on, it may be changed to
    //other value (AUTH_STATUS_OK, AUTH_STATUS_FAIL, etc) when applicable.
    m_context->addTraceSummaryValue(LogMin, "auth", AUTH_STATUS_NA);
    if (processHeaders(me)==-1)
        return -1;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("Headers processed! content_length = %" I64F "d", m_content_length);
    StringBuffer expect;
    getHeader("Expect", expect);
    if (expect.length() && strieq(expect, "100-continue"))
    {
        StringBuffer cont("HTTP/1.1 100 Continue\r\n\r\n"); //tell client to send body
        m_socket.write(cont, cont.length());
    }

    if (isUpload())
        return 0;

    m_context->addTraceSummaryValue(LogMin, "contLen", m_content_length);
    if(m_content_length > 0)
    {
        readContent();
        if (getEspLogLevel()>LogNormal)
            DBGLOG("length of content read = %d", m_content.length());
    }
    else if (alwaysReadContent && m_content_length == -1)
    {
        //HTTP protocol does not require a content length: read until socket closed
        readContentTillSocketClosed();
        if (getEspLogLevel()>LogNormal)
            DBGLOG("length of content read = %d", m_content.length());
    }

    bool decompressed = false;
    int compressType = 0;
    if (shouldDecompress(compressType))
        decompressed = decompressContent(nullptr, compressType);

    if (getEspLogRequests() == LogRequestsAlways)
    {
        if (!decompressed)
            logMessage(LOGCONTENT, "HTTP content received:\n");
        else
            logMessage(LOGCONTENT, "Compressed HTTP content received, decompressed content:\n");
    }
    return 0;
}

StringBuffer& CHttpMessage::getContent(StringBuffer& content)
{
    content.append(m_content.length(), m_content.str());
    return content;
}

void CHttpMessage::setContent(const char* content)
{
    m_content.clear();
    m_content.append(content);
    m_content_length = strlen(content);
}

void CHttpMessage::setContent(unsigned len, const char* content)
{
    m_content.clear();
    m_content.append(len, content);
    m_content_length = len;
}

void CHttpMessage::setownContent(char* content)
{
    size32_t len=strlen(content);
    m_content.setBuffer(len+1, content, len);
    m_content_length = len;
}

void CHttpMessage::setownContent(unsigned len, char* content)
{
    m_content.setBuffer(len+1, content, len);
    m_content_length = len;
}

void CHttpMessage::setContent(IFileIOStream* stream)
{
    if(stream != NULL)
    {
        m_content.clear();
        m_content_length = stream->size();
        m_content_stream.setown(stream);
    }
}

/*
void CHttpMessage::appendContent(const char* content)
{
    m_content.append(content);
    m_content_length += strlen(content);
}
*/

StringBuffer& CHttpMessage::getContentType(StringBuffer& contenttype)
{
    contenttype.append(m_content_type);
    return contenttype;
}

void CHttpMessage::setContentType(const char* contenttype)
{
    m_content_type.set(contenttype);
}

void CHttpMessage::setVersion(const char* version)
{
    m_version.set(version);
}

int CHttpMessage::parseFirstLine(char* oneline)
{
    return 0;
}

void CHttpMessage::parseCookieHeader(char* cookiestr)
{
}

StringBuffer& CHttpMessage::constructHeaderBuffer(StringBuffer& headerbuf, bool inclLen)
{
    return headerbuf;
}

void CHttpMessage::logMessage(const char* message, const char* prefix, const char* find, const char* replace)
{
    if (!message || !*message)
        return;

    if (!find || !*find || !replace || !*replace)
    {
        if (prefix && *prefix)
            DBGLOG("%s%s", prefix, message);
        else
            DBGLOG("%s", message);

        return;
    }

    RegExpr auth(find, true);
    StringBuffer messageToLog(message);
    if (auth.find(messageToLog.str()))
        auth.replace(replace, messageToLog.length() + strlen(replace) - strlen(find));

    if (prefix && *prefix)
        DBGLOG("%s%s", prefix, messageToLog.str());
    else
        DBGLOG("%s", messageToLog.str());

    return;
}

void CHttpMessage::logSOAPMessage(const char* message, const char* prefix)
{
    SOAPMessageLog messageLog;
    messageLog.logMessage(message, prefix);

    return;
}

void CHttpMessage::logMessage(MessageLogFlag messageLogFlag, const char *prefix)
{
    logMessage(messageLogFlag, m_content, prefix);
}

void CHttpMessage::logMessage(MessageLogFlag messageLogFlag, StringBuffer& content, const char *prefix)
{
    try
    {
        if (((messageLogFlag == LOGHEADERS) || (messageLogFlag == LOGALL)) && (m_header.length() > 0))
            logMessage(m_header.str(), prefix, "Authorization:[~\r\n]*", "Authorization: (hidden)");

        if (((messageLogFlag == LOGCONTENT) || (messageLogFlag == LOGALL)) && (content.length() > 0))
        {//log content
            if ((m_header.length() > 0) && (startsWith(m_header.str(), "POST /ws_access/AddUser")
                || startsWith(m_header.str(), "POST /ws_access/UserResetPass") || startsWith(m_header.str(), "POST /ws_account/UpdateUser")))
                DBGLOG("%s<For security, ESP does not log the content of this request.>", prefix);
            else if (isSoapMessage())
                logSOAPMessage(content.str(), prefix);
            else if(!isTextMessage())
                DBGLOG("%s<non-text content or content type not specified>", prefix);
            else if ((m_content_type.length() > 0) && (strieq(m_content_type.get(), "text/css") || strieq(m_content_type.get(), "text/javascript")))
                DBGLOG("%s<content_type: %s>", prefix, m_content_type.get());
            else
            {
                StringBuffer httpPath;
                getPath(httpPath);
                if (!strieq(httpPath.str(), "/esp/login"))
                    logMessage(content.str(), prefix);
                else
                    logMessage(content.str(), prefix, "password=*", "password=(hidden)");
            }
        }
    }
    catch (IException *e)
    {
        StringBuffer msg;
        IERRLOG("EXCEPTION %s when logging the message: %s", e->errorMessage(msg).str(), content.str());
        if (m_content_type.length() > 0)
            IERRLOG("EXCEPTION %s when logging the message (m_content_type:%s):%s", e->errorMessage(msg).str(), m_content_type.get(), content.str());
        else
            IERRLOG("EXCEPTION %s when logging the message: %s", e->errorMessage(msg).str(), content.str());
        e->Release();
    }
    return;
}

int CHttpMessage::send()
{
    bool logMsg = getEspLogResponses();
    bool compressed = false;
    StringBuffer originalContent;
    int compressType = 0;
    if (shouldCompress(compressType))
        compressed = compressContent(logMsg?&originalContent:nullptr, compressType);

    StringBuffer headers;
    constructHeaderBuffer(headers, true);
    
    int retcode = 0;

    // If m_content is empty but m_content_stream is set, the stream will not be logged here.
    if (logMsg)
    {
        logMessage(headers.str(), "Sending out HTTP headers:\n", "Authorization:[~\r\n]*", "Authorization: (hidden)");
        if(m_content_length > 0 && m_content.length() > 0)
        {
            if (!compressed)
                logMessage(LOGCONTENT, "Sending out HTTP content:\n");
            else
                logMessage(LOGCONTENT, originalContent, "Sending out compressed HTTP content, original content:\n");
        }
    }

    try
    {
        m_socket.write(headers.str(), headers.length());
        if(m_content_length > 0 && m_content.length() > 0)
            m_socket.write(m_content.str(), m_content.length());
    }
    catch (IException *e) 
    {
        setPersistentEligible(false);
        StringBuffer estr;
        IERRLOG("In CHttpMessage::send(%d) -- Exception(%d, %s) writing to socket(%d).", __LINE__, e->errorCode(), e->errorMessage(estr).str(), m_socket.OShandle());
        e->Release();
        return -1;
    }
    catch(...)
    {
        setPersistentEligible(false);
        IERRLOG("In CHttpMessage::send(%d) -- Unknown exception writing to socket(%d).", __LINE__, m_socket.OShandle());
        return -1;
    }

    // When m_content is empty but the stream was set, read content from the stream.
    if((m_content_length > 0 && m_content.length() == 0) && m_content_stream.get() != NULL)
    {
        //Read the file and send out 20K at a time.
        __int64 content_length = m_content_length;
        int buflen = 20*1024;
        if(buflen > content_length)
            buflen = (int) content_length;
        char* buffer = new char[buflen + 1];
        __int64 sizesent = 0;
        while(sizesent < content_length)
        {
            int sizeread = m_content_stream->read(buflen, buffer);
            if(sizeread > 0)
            {
                sizesent += sizeread;
                try
                {
                    m_socket.write(buffer, sizeread);
                }
                catch (IException *e) 
                {
                    setPersistentEligible(false);
                    StringBuffer estr;
                    LOG(MCexception(e), "In CHttpMessage::send(%d) -- Exception(%d, %s) writing to socket(%d).", __LINE__, e->errorCode(), e->errorMessage(estr).str(), m_socket.OShandle());
                    e->Release();
                    retcode = -1;
                    break;
                }
                catch(...)
                {
                    setPersistentEligible(false);
                    IERRLOG("In CHttpMessage::send(%d) -- Unknown exception writing to socket(%d).", __LINE__, m_socket.OShandle());
                    retcode = -1;
                    break;
                }
            }
            else
            {
                IERRLOG("Error read from file");
                break;
            }
        }
        delete [] buffer;
    }

    return retcode;
}

int CHttpMessage::startSend()
{
    StringBuffer sendbuf;
    constructHeaderBuffer(sendbuf, false);
    
    if (getEspLogLevel(queryContext())>LogNormal)
        DBGLOG("Start Sending chunked HTTP message:\n %s", sendbuf.str());

    try
    {
        m_socket.write(sendbuf.str(), sendbuf.length());
    }
    catch (IException *e) 
    {
        setPersistentEligible(false);
        StringBuffer estr;
        DBGLOG("In CHttpMessage::send() -- Exception(%d, %s) writing to socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket.OShandle());
        e->Release();
        return -1;
    }
    catch(...)
    {
        setPersistentEligible(false);
        ERRLOG("In CHttpMessage::send() -- Unknown exception writing to socket(%d).", m_socket.OShandle());
        return -1;
    }

    return 0;
}

int CHttpMessage::sendChunk(const char *chunk)
{
    if (getEspLogLevel(queryContext())>LogNormal)
        DBGLOG("Sending HTTP chunk:\n %s", chunk);

    try
    {
        m_socket.write(chunk, strlen(chunk));
    }
    catch (IException *e) 
    {
        setPersistentEligible(false);
        StringBuffer estr;
        DBGLOG("In CHttpMessage::send() -- Exception(%d, %s) writing to socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket.OShandle());
        e->Release();
        return -1;
    }
    catch(...)
    {
        setPersistentEligible(false);
        ERRLOG("In CHttpMessage::send() -- Unknown exception writing to socket(%d).", m_socket.OShandle());
        return -1;
    }

    return 0;
}

int CHttpMessage::sendFinalChunk(const char *chunk)
{
    if (getEspLogLevel(queryContext())>LogNormal)
        DBGLOG("Sending HTTP Final chunk:\n %s", chunk);

    try
    {
        m_socket.write(chunk, strlen(chunk));
        m_socket.close();
    }
    catch (IException *e) 
    {
        setPersistentEligible(false);
        StringBuffer estr;
        DBGLOG("In CHttpMessage::send() -- Exception(%d, %s) writing to socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket.OShandle());
        e->Release();
        return -1;
    }
    catch(...)
    {
        setPersistentEligible(false);
        ERRLOG("In CHttpMessage::send() -- Unknown exception writing to socket(%d).", m_socket.OShandle());
        return -1;
    }

    return 0;
}

int CHttpMessage::close()
{
    int ret = 0;

    try
    {
        m_socket.shutdown();
        m_socket.close();
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) - CHttpMessage::close(), closing socket.", e->errorCode(), e->errorMessage(estr).str());
        e->Release();
        ret = -1;
    }
    catch(...)
    {
        IERRLOG("General Exception - CHttpMessage::close(), closing socket.");
        ret = -1;
    }

    return ret;
}

void CHttpMessage::setHeader(const char* headername, const char* headerval)
{
    if(!headername || !*headername)
        return;

    StringBuffer val;
    val.append(headername).append(": ").append(headerval);
    ForEachItemIn(x, m_headers)
    {
        const char* curst = m_headers.item(x);
        if(!curst)
            continue;
        const char* colon = strchr(curst, ':');
        if(!colon)
            continue;
        if(!strnicmp(headername, curst, colon - curst))
        {
            m_headers.replace(val.str(), x);
            return;
        }
    }

    m_headers.append(val.str());
}

void CHttpMessage::addHeader(const char* headername, const char* headerval)
{
    if(headername == NULL || strlen(headername) == 0)
        return;

    StringBuffer header;
    header.append(headername);
    header.append(": ");
    header.append(headerval);

    m_headers.append(header.str());
}

StringBuffer& CHttpMessage::getHeader(const char* headername, StringBuffer& headerval)
{
    if(headername == NULL || *headername == '\0')
        return headerval;

    ForEachItemIn(x, m_headers)
    {
        const char* header = m_headers.item(x);
        if(header == NULL)
            continue;
        const char* colon = strchr(header, ':');
        if(colon == NULL)
            continue;
        unsigned len = colon - header;
        if((strlen(headername) == len) && (strnicmp(headername, header, len) == 0))
        {
            headerval.append(colon + 2);
            break;
        }
    }
    return headerval;       
}

bool CHttpMessage::hasHeader(const char* headername)
{
    if (!headername || !*headername)
        return false;

    unsigned headerlen = strlen(headername);
    ForEachItemIn(x, m_headers)
    {
        const char* header = m_headers.item(x);
        if (header == nullptr)
            continue;
        const char* colon = strchr(header, ':');
        if (colon == nullptr)
            continue;
        unsigned len = colon - header;
        if ((headerlen == len) && (strnicmp(headername, header, len) == 0))
            return true;
    }
    return false;
}

void CHttpMessage::removeHeader(const char* headername)
{
    if (!headername || !*headername)
        return;

    ForEachItemInRev(x, m_headers)
    {
        const char* header = m_headers.item(x);
        if (header == nullptr)
            continue;
        const char* colon = strchr(header, ':');
        if (colon == nullptr)
            continue;
        unsigned len = colon - header;
        if ((strlen(headername) == len) && (strnicmp(headername, header, len) == 0))
            m_headers.remove(x);
    }
}

bool isSoapContentType(const char* contenttype)
{
    if(contenttype == NULL)
        return false;
    else
        return Utils::strncasecmp(contenttype, HTTP_TYPE_TEXT_XML, strlen(HTTP_TYPE_TEXT_XML)) == 0 ||
            Utils::strncasecmp(contenttype, HTTP_TYPE_SOAP, strlen(HTTP_TYPE_SOAP)) == 0;
}

bool isJsonContentType(const char* contenttype)
{
    if (contenttype == NULL)
        return false;
    else
        return Utils::strncasecmp(contenttype, HTTP_TYPE_JSON, strlen(HTTP_TYPE_JSON)) == 0;
}

bool CHttpMessage::isSoapMessage()
{
    if(m_content_type.get() == NULL)
        return false;
    else if(Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_MULTIPART_RELATED, strlen(HTTP_TYPE_MULTIPART_RELATED)) == 0)
    {
        CMimeMultiPart* mpart = queryMultiPart();
        if(mpart == NULL)
            return false;
        CMimeBodyPart* bpart = mpart->queryRootPart();
        if(bpart != NULL && isSoapContentType(bpart->getContentType()))
            return true;
        else
            return false;
    }
    else
        return isSoapContentType(m_content_type.get());
}

bool CHttpMessage::isJsonMessage()
{
    if (m_content_type.get() == NULL)
        return false;
    else if (Utils::strncasecmp(m_content_type.get(), HTTP_TYPE_MULTIPART_RELATED, strlen(HTTP_TYPE_MULTIPART_RELATED)) == 0)
    {
        CMimeMultiPart* mpart = queryMultiPart();
        if (mpart == NULL)
            return false;
        CMimeBodyPart* bpart = mpart->queryRootPart();
        if (bpart != NULL && isJsonContentType(bpart->getContentType()))
            return true;
        else
            return false;
    }
    else
        return isJsonContentType(m_content_type.get());
}

bool CHttpMessage::isFormSubmission()
{
    return ((hasContentType(NULL) && (m_paramCount + m_attachCount) > 0) ||
        hasContentType(HTTP_TYPE_MULTIPART_FORMDATA) || 
        hasContentType(HTTP_TYPE_FORM_ENCODED));
}

void CHttpMessage::enableCompression()
{
    const char* off = getenv("ESP_COMPRESSION_OFF");
    if (off && streq(off, "1"))
        return;
    m_compressionEnabled = true;
}

bool CHttpMessage::checkPersistentEligible()
{
    size32_t verOffset = httpVerOffset();
    if (m_version.length() <= verOffset)
        return false;
    StringBuffer conHeader;
    getHeader("Connection", conHeader);
    return isHttpPersistable(m_version.str() + verOffset, conHeader.str());
}

/******************************************************************************
              CHttpRequest Implementation
*******************************************************************************/

CHttpRequest::CHttpRequest(ISocket& socket) : CHttpMessage(socket), m_pathIsParsed(false), 
    m_sstype(sub_serv_unknown), m_MaxRequestEntityLength(0)
{
};

CHttpRequest::~CHttpRequest()
{
};


StringBuffer& CHttpRequest::getMethod(StringBuffer & method)
{
    return method.append(m_httpMethod.str());
}

void CHttpRequest::setMethod(const char* method)
{
    m_httpMethod.set(method);
}

void CHttpRequest::parseQueryString(const char* querystr)
{
    if(!querystr || !*querystr)
        return;

    bool useHeap = false;
    int querystrlen = strlen(querystr);
    if(querystrlen >= 0x80000)
        useHeap = true;

    char* querystrbuf = NULL;
    if(useHeap)
        querystrbuf = (char*)malloc(querystrlen + 1);
    else
        querystrbuf = (char*)alloca(querystrlen + 1);

    strcpy(querystrbuf, querystr);

    char* ptr = querystrbuf;
    char* curname = ptr;
    char* curvalue = NULL;
    while(true)
    {
        while(*ptr != '\0' && *ptr != '=' && *ptr != '&')
            ptr++;
        
        if(*ptr == '\0')
        {
            StringBuffer nameval;
            Utils::url_decode(curname, nameval);
            addParameter(nameval.str(), "");
            break;
        }
        else if(*ptr == '=')
        {
            *ptr = '\0';
            ptr++;
            if(*ptr == '\0')
            {
                StringBuffer nameval;
                Utils::url_decode(curname, nameval);
                addParameter(nameval.str(), "");
                break;
            }
            else if(*ptr == '&')
            {
                StringBuffer nameval;
                Utils::url_decode(curname, nameval);
                addParameter(nameval.str(), "");
                ptr++;
                if(*ptr == '\0')
                    break;
                else
                    curname = ptr;
            }
            else
            {
                curvalue = ptr;
                while(*ptr != '\0' && *ptr != '&')
                    ptr++;
                if(*ptr == '\0')
                {
                    StringBuffer nameval;
                    StringBuffer valueval;
                    Utils::url_decode(curname, nameval);
                    Utils::url_decode(curvalue, valueval);
                    addParameter(nameval.str(), valueval.str());
                    break;
                }
                else //*ptr == '&'
                {
                    *ptr = '\0';
                    ptr++;

                    StringBuffer nameval;
                    StringBuffer valueval;
                    Utils::url_decode(curname, nameval);
                    Utils::url_decode(curvalue, valueval);
                    addParameter(nameval.str(), valueval.str());

                    if(*ptr == '\0')
                        break;
                    else
                        curname = ptr;
                }
            }
        }
        else if(*ptr == '&')
        {
            *ptr=0;

            StringBuffer nameval;
            Utils::url_decode(curname, nameval);
            addParameter(nameval.str(), "");

            ptr++;
            if(!*ptr)
                break;
            else
                curname = ptr;
        }
    }

    if(useHeap && querystrbuf != NULL)
        delete querystrbuf;

}

int CHttpRequest::parseFirstLine(char* oneline)
{
    //if (getEspLogLevel()>LogNormal)
    //  DBGLOG("First Line of request=%s", oneline);
    DBGLOG("HTTP First Line: %s", oneline);

    if(*oneline == 0)
        return -1;

    const char* curptr = oneline;
    StringBuffer method;
    curptr = Utils::getWord(curptr, method);

    if(!stricmp(method.str(), POST_METHOD))
    {
        setMethod(POST_METHOD);
    }
    else if(!stricmp(method.str(), GET_METHOD))
    {
        setMethod(GET_METHOD);
    }
    else if(!stricmp(method.str(), HEAD_METHOD))
    {
        setMethod(HEAD_METHOD);
    }
    else if(!stricmp(method.str(), OPTIONS_METHOD))
    {
        setMethod(OPTIONS_METHOD);
    }

    StringBuffer pathbuf;
    curptr = Utils::getWord(curptr, pathbuf);
    
    int len = pathbuf.length();
    char* buff;
    if(len == 0)
    {
        buff = new char[2];
        buff[0] = '/';
        buff[1] = '\0';
    }
    else
    {
        if(pathbuf.charAt(0) != '/')
        {
            buff = new char[len + 2];
            buff[0] = '/';
            strcpy(buff + 1, pathbuf.str());
        }
        else
        {
            buff = new char[len + 1];
            strcpy(buff, pathbuf.str());
        }
    }

    char* qmark = strchr(buff, '?');
    if(qmark != NULL)
    {
        *qmark = '\0';
    }

    // Decode path
    StringBuffer path;
    Utils::url_decode(buff, path);
    setPath(path.str());

    // Parse and decode parameters
    if(qmark != NULL)
    {
        char* querystr = qmark + 1;
        m_paramstr.set(querystr);
        addParameter("__querystring", querystr); //MORE- requested by dimitri
        parseQueryString(querystr);
    }

    StringBuffer verbuf;
    curptr = Utils::getWord(curptr, verbuf);
    if(verbuf.length() > 0)
        m_version.set(verbuf);

    delete[] buff;

    return 0;
}

void CHttpRequest::parseCookieHeader(char* cookiestr)
{
    if(cookiestr == NULL)
        return;
    int version;
    char* curword;
    char* curptr = cookiestr;
    const char* separators = ",;";
    
    curptr = Utils::getWord(curptr, curword, separators);
    StringBuffer name, value;
    Utils::parseNVPair(curword, name, value);
    CEspCookie* cookie = NULL;
    if(name.length() == 0 || stricmp(name.str(), "$Version"))
    {
        version = 0;
        cookie = new CEspCookie(name.str(), value.str());
        cookie->setVersion(version);
        m_cookies.append(*cookie);
    }
    else
    {
        version = atoi(value.str());
    }

    while(curptr != NULL && *curptr != 0)
    {
        curptr = Utils::getWord(curptr, curword, separators);
        if(curword == NULL)
            break;
        StringBuffer name, value;
        Utils::parseNVPair(curword, name, value);
        if(name.length() == 0)
            continue;
        if(name.charAt(0) != '$')
        {
            cookie = new CEspCookie(name.str(), value.str());
            cookie->setVersion(version);
            m_cookies.append(*cookie);
        }
        else if(stricmp(name.str(), "$Path") == 0 && cookie != NULL)
            cookie->setPath(value.str());
        else if(stricmp(name.str(), "$Domain") == 0 && cookie != NULL)
            cookie->setDomain(value.str());
        else if(stricmp(name.str(), "$Port") == 0 && cookie != NULL)
            cookie->setPorts(value.str());
    }
}

ESPSerializationFormat lookupResponseFormatByExtension(const char *ext)
{
    if (!ext || !*ext)
        return ESPSerializationANY;
    if (strieq(ext, ".xml"))
        return ESPSerializationXML;
    if (strieq(ext, ".json"))
        return ESPSerializationJSON;
    return ESPSerializationANY;
}

void CHttpRequest::parseEspPathInfo()
{
    if (!m_pathIsParsed)
    {
        m_espPathEx.clear();
        m_espMethodName.clear();
        m_espServiceName.clear();

        if (queryParameters()->hasProp("rawxml_"))
            m_context->setResponseFormat(ESPSerializationXML);
        else if (isSoapMessage())
            m_context->setResponseFormat(ESPSerializationXML);

        size32_t pathlen=m_httpPath.length();
        if (!pathlen)
            m_sstype=(m_queryparams && m_queryparams->hasProp("main")) ? sub_serv_main : sub_serv_root;
        else
        {
            //MORE: With a couple of changes there would be need to clone pathstr
            char *pathstr = strdup(m_httpPath.str());
            char *finger = pathstr;
            
            if (!strnicmp(finger, "http://", 7))
                finger+=7;

            char *thumb=finger;
            while (*thumb!=0 && *thumb!='/')
            {
                if (*thumb=='@' || *thumb==':')
                {
                    finger=strchr(thumb, '/');
                    break;
                }
                thumb++;
            }

            bool missingTrailSlash = false;
            if (finger)
            {
                while (*finger == '/')
                    finger++;

                //look for the service and method names
                //
                if (finger && finger[0] != '\0')
                {
                    thumb=strchr(finger, '/');

                    if (thumb)
                    {
                        char *pathex = strchr(thumb+1, '/');
                        if (pathex)
                        {
                            *pathex=0;
                            m_espPathEx.set(pathex+1);
                        }
                            
                        *thumb=0;
                        const char * method = thumb+1;
                        const char *tail = strrchr(method, '.');
                        ESPSerializationFormat fmt = lookupResponseFormatByExtension(tail);
                        if (fmt!=ESPSerializationANY)
                        {
                            m_context->setResponseFormat(fmt);
                            m_espMethodName.set(method, tail-method);
                        }
                        else
                            m_espMethodName.set(method);
                    }
                    else 
                        missingTrailSlash = true; 

                    m_espServiceName.set(finger);
                }
            }
            
            free(pathstr);

            if (m_espMethodName.length())
            {
                if (!stricmp(m_espMethodName.str(), "files_"))
                    m_sstype=sub_serv_files;
                else if (!stricmp(m_espMethodName.str(), "content_"))
                    m_sstype=sub_serv_content;
                else if (!stricmp(m_espMethodName.str(), "result_"))
                    m_sstype=sub_serv_result;
                else if (!stricmp(m_espMethodName.str(), "iframe"))
                    m_sstype=sub_serv_iframe;
                else if (!stricmp(m_espMethodName.str(), "itext"))
                    m_sstype=sub_serv_itext;
                else if (!stricmp(m_espMethodName.str(), "version_"))
                    m_sstype=sub_serv_getversion;
            }

            //not a special service URL, determine the action type
            if (m_sstype==sub_serv_unknown)
            {
                if (m_queryparams && (m_queryparams->hasProp("wsdl") ||  m_queryparams->hasProp("wsdl_ext")))
                    m_sstype=sub_serv_wsdl;
                else if (m_queryparams && (m_queryparams->hasProp("xsd")))
                    m_sstype=sub_serv_xsd;
                else if (m_queryparams && (m_queryparams->hasProp("reqxml_")))
                    m_sstype=sub_serv_reqsamplexml;
                else if (m_queryparams && (m_queryparams->hasProp("respxml_")))
                    m_sstype=sub_serv_respsamplexml;
                else if (m_queryparams && (m_queryparams->hasProp("respjson_")))
                    m_sstype=sub_serv_respsamplejson;
                else if (m_queryparams && (m_queryparams->hasProp("reqjson_")))
                    m_sstype=sub_serv_reqsamplejson;
                else if (m_queryparams && (m_queryparams->hasProp("soap_builder_")))
                    m_sstype=sub_serv_soap_builder;
                else if (m_queryparams && (m_queryparams->hasProp("roxie_builder_")))
                    m_sstype=sub_serv_roxie_builder;
                else if (m_queryparams && (m_queryparams->hasProp("json_builder_")))
                    m_sstype=sub_serv_json_builder;
                else if (m_queryparams && m_queryparams->hasProp("config_"))
                    m_sstype=sub_serv_config;
                else if (m_espServiceName.length()==0)
                    m_sstype=(m_queryparams && m_queryparams->hasProp("main")) ? sub_serv_main : sub_serv_root;
                else if (m_espMethodName.length()==0)
                    m_sstype = missingTrailSlash ? sub_serv_index_redirect : sub_serv_index;
                else if (m_queryparams && m_queryparams->hasProp("form_"))
                    m_sstype=sub_serv_form;
                else if (m_queryparams && m_queryparams->hasProp("form"))
                    m_sstype=sub_serv_xform;
                else if (isUpload())
                    m_sstype=sub_serv_file_upload;
                else if (getParameterCount())// queryParamStr()!=NULL && *queryParamStr()!=0)
                    m_sstype=sub_serv_query;
                else
                    m_sstype=sub_serv_method;
            }
        }
        m_pathIsParsed=true;
    }
}


void CHttpRequest::getEspPathInfo(sub_service &sstype, StringBuffer *pathEx, StringBuffer *service, StringBuffer *method, bool upcase)
{
    parseEspPathInfo();

    sstype = m_sstype;

    if (pathEx)
    {
        pathEx->clear().append(m_espPathEx);
        if (upcase)
            pathEx->toUpperCase();
    }
    if (service)
    {
        service->clear().append(m_espServiceName);
        service->toUpperCase();
    }
    if (method)
    {
        method->clear().append(m_espMethodName);
        method->toUpperCase();
    }
}
    
void CHttpRequest::getBasicRealm(StringBuffer& realm)
{
    StringBuffer authheader;
    getHeader("WWW-Authenticate", authheader);
    if(authheader.length() == 0)
        return;
    
    if(Utils::strncasecmp(authheader.str(), "Basic ", strlen("Basic ")) != 0)
        return;

    const char* strt = strchr(authheader.str(), '\"');
    ++strt;
    if (strt)
    { 
        const char* end = strchr(strt, '\"');
        if (end)
        {
            realm.append(strt, 0, end - strt);
        }
    }
}

int CHttpRequest::getPeerPort()
{
    char peername[256];
    return m_socket.peer_name(peername, 256);
}

StringBuffer& CHttpRequest::getPeer(StringBuffer& Peer)
{
    StringBuffer ForwardIPs;
    getHeader("X-Forwarded-For", ForwardIPs);
    
    if(ForwardIPs.length() != 0)
    {
        //IPs will ne in the form xxx.xxx.xxx.xxx,yyy.yyy.yyy.yyy,zzz.zzz.zzz.zzz
        //We want to take the first IP in the list
        const char* strt = strchr(ForwardIPs.str(), ',');
        if(strt!= NULL)
            Peer.append(strt - ForwardIPs.str(),ForwardIPs.str());
        else
            Peer.appendf("%s",ForwardIPs.str());
    }
    else
    {
        char peerchr[256];
        int port = m_socket.peer_name(peerchr, 256);
        Peer.append(peerchr);
    }
    return Peer;
}

void CHttpRequest::getBasicAuthorization(StringBuffer& userid, StringBuffer& password,StringBuffer& realm)
{
    StringBuffer authheader;
    getHeader("Authorization", authheader);
    if(authheader.length() == 0)
        return;
    if(Utils::strncasecmp(authheader.str(), "Basic ", strlen("Basic ")) != 0)
        return;

    StringBuffer uidpair;
    JBASE64_Decode(authheader.str() + strlen("Basic "), uidpair);

    //uidpair formatted as   [domain\]username:password    (domain optional)
    
    //Look for optional domain name separator. Assumes username cannot contain '\'
    const char* pairstr = strchr(uidpair.str(), '\\');

    if (pairstr)
    {
        const char * pwdSep = strchr(uidpair, ':');//look for user:pwd separator
        if (pwdSep && pwdSep < pairstr)//if : is before \, then \ is part of password and not domain separator
            pairstr = NULL;
    }

    if(pairstr!=NULL)
    {
        realm.append(pairstr - uidpair.str(),uidpair.str());
        pairstr++;
    }
    else
    {
        pairstr = uidpair.str();
        getBasicRealm(realm);
    }
    
    const char* colon = strchr(pairstr, ':');
    if(colon == NULL)
    {
        userid.append(pairstr);
    }
    else
    {
        userid.append(colon - pairstr, pairstr);
        password.append(colon + 1);
    }

}

int CHttpRequest::receive(IMultiException *me)
{
    if (CHttpMessage::receive(false, me)==-1)
        return -1;
    
    //if(hasContentType("application/x-www-form-urlencoded"))
    if(hasContentType(HTTP_TYPE_FORM_ENCODED))
    {
        parseQueryString(m_content.str());
    }
    else if(hasContentType(HTTP_TYPE_MULTIPART_FORMDATA) && !isUpload())
    {
        CMimeMultiPart* mpart = queryMultiPart();
        if(mpart != NULL)
        {
            int count = mpart->getBodyCount();
            for(int i = 0; i < count; i++)
            {
                CMimeBodyPart* bpart = mpart->queryBodyPart(i);
                if(bpart == NULL)
                    continue;
                const char* cdisp = bpart->getContentDisposition();
                StringBuffer contentbuf;
                bpart->getContent(contentbuf);
                StringBuffer namebuf;

                char* curword = NULL;
                char* curptr = (char*)cdisp;
                const char* separators = ";";
    
                bool isFile = false;
                StringBuffer filename;
                while(curptr != NULL && *curptr != 0)
                {
                    curptr = Utils::getWord(curptr, curword, separators, true);
                    if(curword == NULL)
                        break;
                    StringBuffer name, value;
                    Utils::parseNVPair(curword, name, value);
                    if(name.length() > 0 && stricmp(name.str(), "name") == 0)
                    {
                        namebuf.append(value.str());
                    }
                    else if(name.length() > 0 && stricmp(name.str(), "filename") == 0)
                    {
                        value.swapWith(filename);
                        isFile = true;
                    }
                }

                if(!isFile)
                    addParameter(namebuf.str(), contentbuf.str());
                else
                {
                    addParameter(namebuf.str(), filename.str());
                    addAttachment(namebuf.str(), contentbuf);
                }
            }
        }
    }

    m_context->addTraceSummaryTimeStamp(LogMin, "rcv");
    return 0;
}


void CHttpRequest::updateContext()
{
    if(m_context)
    {
        m_context->setRequest(this);
        m_context->setContextPath(m_httpPath.str());

        StringBuffer temp;

        getPeer(temp);
        if(temp.length())
            m_context->setPeer(temp.str());

        m_context->setRequestParameters(queryParameters());

        short servPort;
        temp.clear();
        getServAddress(temp, servPort);
        m_context->setServAddress(temp.str(), servPort);


        StringBuffer userid, password, realm;
        getBasicAuthorization(userid, password, realm);
        if(userid.length() > 0)
        {
            m_context->setUserID(userid.str());
            m_context->setPassword(password.str());
            m_context->setRealm(realm.str());
        }

        if (m_queryparams)
        {
            if (m_queryparams->getPropInt("no_ns_"))
                m_context->addOptions(ESPCTX_NO_NAMESPACES);
            if (m_queryparams->hasProp("wsdl"))
                m_context->addOptions(ESPCTX_WSDL);
            if (m_queryparams->hasProp("wsdl_ext"))
                m_context->addOptions(ESPCTX_WSDL|ESPCTX_WSDL_EXT);
            if (m_queryparams->hasProp("no_annot_"))
                m_context->addOptions(ESPCTX_NO_ANNOTATION);
            if (m_queryparams->hasProp("all_annot_"))
                m_context->addOptions(ESPCTX_ALL_ANNOTATION);
        }

        // set client version
        StringBuffer action;
        getHeader("SOAPAction", action);

        // URL first, then SOAPAction
        const char *verstr = queryParameters()->queryProp("ver_");
        if (verstr && *verstr)
            m_context->setClientVersion(atof(verstr));
        else
        { 
            verstr=strstr(action.str(), "ver_=");
            if (verstr)
                m_context->setClientVersion(atof(verstr+5));
            else
                m_context->setClientVersion(0.0);
        }

        StringBuffer useragent, acceptLanguage;
        getHeader("User-Agent", useragent);
        m_context->setUseragent(useragent.str());
        getHeader("Accept-Language", acceptLanguage);
        m_context->setAcceptLanguage(acceptLanguage.str());
    }
}


StringBuffer& CHttpRequest::constructHeaderBuffer(StringBuffer& headerbuf, bool inclLength)
{
    if (m_compressionEnabled && !hasHeader(HTTP_HEADER_ACCEPT_ENCODING))
        addHeader(HTTP_HEADER_ACCEPT_ENCODING, "gzip, deflate");

    if(m_httpMethod.length() > 0)
        headerbuf.append(queryMethod()).append(" ");
    else
        headerbuf.append("POST ");

    if(m_httpPath.length() > 0)
        headerbuf.append(queryPath()).append(" ");
    else
        headerbuf.append("/ ");

    if(m_version.length() > 0)
        headerbuf.append(m_version.get());
    else
        headerbuf.append(HTTP_VERSION);

    headerbuf.append("\r\n");

    if(m_host.length() > 0)
    {
        headerbuf.append("Host: ").append(m_host.get());
        if(m_port != 80)
        {
            headerbuf.append(":").append(m_port);
        }
        headerbuf.append("\r\n");
    }

    headerbuf.append("Content-Type: ");
    if(m_content_type.length() > 0)
        headerbuf.append(m_content_type.get());
    else
        headerbuf.append("text/xml; charset=UTF-8");

    headerbuf.append("\r\n");

    if(inclLength && m_content_length > 0)
        headerbuf.append("Content-Length: ").append(m_content_length).append("\r\n");

    if (!m_persistentEnabled)
        headerbuf.append("Connection: close\r\n");

    if(m_cookies.length() > 0)
    {
        headerbuf.append("Cookie: ");
        int version = m_cookies.item(0).getVersion();
        if(version >= 1)
            headerbuf.append("$Version=").append('"').append(version).append('"').append(',');
        ForEachItemIn(x, m_cookies)
        {
            CEspCookie* cookie = &m_cookies.item(x);
            if(cookie == NULL)
                continue;
            cookie->appendToRequestHeader(headerbuf);
        }
        headerbuf.append("\r\n");
    }
        

    ForEachItemIn(x, m_headers)
    {
        const char* oneheader = (const char*)m_headers.item(x);
        headerbuf.append(oneheader).append("\r\n");
    }

    headerbuf.append("\r\n");
    return headerbuf;
}

bool CHttpRequest::checkPersistentEligible()
{
    //Might be bad request
    if(m_content_length == -1 && !(m_httpMethod.length() > 0 && stricmp(m_httpMethod.get(), "GET") == 0))
        return false;

    return CHttpMessage::checkPersistentEligible();
}

int CHttpRequest::processHeaders(IMultiException *me)
{
    char oneline[MAX_HTTP_HEADER_LEN + 2];

    int lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN + 1, me);
    if(lenread <= 0) //special case client connected and disconnected, load balancer ping?
        return -1;
    else if (lenread > MAX_HTTP_HEADER_LEN)
        throw createEspHttpException(HTTP_STATUS_BAD_REQUEST_CODE, "Bad Request", HTTP_STATUS_BAD_REQUEST);
    m_header.set(oneline);
    parseFirstLine(oneline);
    
    lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN + 1, me);
    while(lenread >= 0 && oneline[0] != '\0')
    {
        if(lenread > MAX_HTTP_HEADER_LEN)
            throw createEspHttpException(HTTP_STATUS_BAD_REQUEST_CODE, "Bad Request", HTTP_STATUS_BAD_REQUEST);

        m_header.append('\n').append(oneline);
        parseOneHeader(oneline);
        lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN + 1, me);
    }

    if (getEspLogRequests() == LogRequestsAlways)
        logMessage(LOGHEADERS, "HTTP request headers received:\n");

    if(m_content_length > 0 && m_MaxRequestEntityLength > 0 && m_content_length > m_MaxRequestEntityLength && (!isUpload(false)))
    {
        UERRLOG("Bad request: Content-Length exceeded maxRequestEntityLength");
        throw createEspHttpException(HTTP_STATUS_BAD_REQUEST_CODE, "The request length was too long.", HTTP_STATUS_BAD_REQUEST);
    }
    setPersistentEligible(checkPersistentEligible());

    return 0;
}

bool CHttpRequest::readContentToBuffer(MemoryBuffer& buffer, __int64& bytesNotRead)
{
    char buf[1024 + 1];
    __int64 buflen = 1024;
    if (buflen > bytesNotRead)
        buflen = bytesNotRead;

    int readlen = m_bufferedsocket->read(buf, (int) buflen);
    if(readlen < 0)
        DBGLOG("Failed to read from socket");

    if(readlen <= 0)
       return false;

    buf[readlen] = 0;
    buffer.append(readlen, buf);//'buffer' may have some left-over from previous read

    bytesNotRead -= readlen;
    return true;
}

bool CHttpRequest::readUploadFileName(CMimeMultiPart* mimemultipart, StringBuffer& fileName, MemoryBuffer& contentBuffer, __int64& bytesNotRead)
{
    //Make sure that contentBuffer contains all of the mime headers for a file. The readUploadFileName() retrieves
    //the file name from those headers and moves a data pointer to the end of those headers.
    if ((bytesNotRead < 1) || (contentBuffer.length() > 512))
        mimemultipart->readUploadFileName(contentBuffer, fileName);

    while((fileName.length() < 1) && (bytesNotRead > 0))
    {
        if (!readContentToBuffer(contentBuffer, bytesNotRead))
            break;

        mimemultipart->readUploadFileName(contentBuffer, fileName);
    }

    return (fileName.length() > 0);
}

IFile* CHttpRequest::createUploadFile(const char * netAddress, const char* filePath, const char* fileName, StringBuffer& fileNameWithPath)
{
    StringBuffer tmpFileName(filePath);
    addPathSepChar(tmpFileName);
    tmpFileName.append(pathTail(fileName));
    fileNameWithPath.set(tmpFileName);
    tmpFileName.append(".part");

    //If no netAddress is specified, create a local file.
    if (isEmptyString(netAddress))
        return createIFile(tmpFileName);

    RemoteFilename rfn;
    SocketEndpoint ep;
    ep.set(netAddress);
    rfn.setPath(ep, tmpFileName.str());

    return createIFile(rfn);
}

void CHttpRequest::readUploadFileContent(StringArray& fileNames, StringArray& files)
{
    Owned<CMimeMultiPart> multipart = new CMimeMultiPart("1.0", m_content_type.get(), "", "", "");
    multipart->parseContentType(m_content_type.get());

    MemoryBuffer fileContent, moreContent;
    __int64 bytesNotRead = m_content_length;
    while (1)
    {
        StringBuffer fileName, content;
        if (!readUploadFileName(multipart, fileName, fileContent, bytesNotRead))
        {
            DBGLOG("No file name found for upload");
            return;
        }

        bool foundAnotherFile = false;
        while (1)
        {
            foundAnotherFile = multipart->separateMultiParts(fileContent, moreContent, bytesNotRead);
            if (fileContent.length() > 0)
                content.append(fileContent.length(), fileContent.toByteArray());

            fileContent.clear();
            if (moreContent.length() > 0)
            {
                fileContent.append(moreContent.length(), (void*) moreContent.toByteArray());
                moreContent.clear();
            }

            if(foundAnotherFile || (bytesNotRead <= 0) || !readContentToBuffer(fileContent, bytesNotRead))
                break;
        }

        fileNames.append(fileName);
        files.append(content);
        if (!foundAnotherFile)
            break;
    }
    return;
}

int CHttpRequest::readContentToFiles(const char * netAddress, const char * path, StringArray& fileNames)
{
    const char* contentType = m_content_type.get();
    if (!contentType || !*contentType)
        throw MakeStringException(-1, "Content Type not found.");
    Owned<CMimeMultiPart> multipart = new CMimeMultiPart("1.0", contentType, "", "", "");
    multipart->parseContentType(contentType);

    MemoryBuffer fileContent, moreContent;
    __int64 bytesNotRead = m_content_length;
    while (1)
    {
        StringBuffer fileName;
        if (!readUploadFileName(multipart, fileName, fileContent, bytesNotRead))
        {
            UERRLOG("No file name found for upload");
            break;
        }

        fileNames.append(fileName);
        StringBuffer fileNameWithPath;
        Owned<IFile> file = createUploadFile(netAddress, path, fileName, fileNameWithPath);
        if (!file)
        {
            UERRLOG("Uploaded file %s cannot be created", fileName.str());
            break;
        }
        Owned<IFileIO> fileio = file->open(IFOcreate);
        if (!fileio)
        {
            UERRLOG("Uploaded file %s cannot be opened", fileName.str());
            break;
        }

        __int64 writeOffset = 0;
        bool writeError = false;
        bool foundAnotherFile = false;
        while (1)
        {
            foundAnotherFile = multipart->separateMultiParts(fileContent, moreContent, bytesNotRead);
            if (fileContent.length() > 0)
            {
                if (fileio->write(writeOffset, fileContent.length(), fileContent.toByteArray()) != fileContent.length())
                {
                    UERRLOG("Failed to write Uploaded file %s", fileName.str());
                    writeError = true;
                    break;
                }
                writeOffset += fileContent.length();
            }

            fileContent.clear();
            if (moreContent.length() > 0)
            {
                fileContent.append(moreContent.length(), (void*) moreContent.toByteArray());
                moreContent.clear();
            }

            if(foundAnotherFile || (bytesNotRead <= 0) || !readContentToBuffer(fileContent, bytesNotRead))
                break;
        }

        if (writeError)
            break;

        file->rename(fileNameWithPath);

        if (!foundAnotherFile)
            break;
    }
    return 0;
}

/******************************************************************************
              CHttpResponse Implementation
*******************************************************************************/

CHttpResponse::CHttpResponse(ISocket& socket) : CHttpMessage(socket), m_timeout(BSOCKET_CLIENT_READ_TIMEOUT), m_respSent(false)
{
}

CHttpResponse::~CHttpResponse()
{
}

void CHttpResponse::setTimeOut(unsigned int timeout)
{
    m_timeout = timeout;
}

void CHttpResponse::setStatus(const char* status)
{
    m_status.set(status);
}

StringBuffer& CHttpResponse::getStatus(StringBuffer& status)
{
    status.append(m_status.get());
    return status;
}

StringBuffer& CHttpResponse::constructHeaderBuffer(StringBuffer& headerbuf, bool inclLen)
{
    if(m_version.length() > 0)
        headerbuf.append(m_version.get()).append(" ");
    else
        headerbuf.append(HTTP_VERSION).append(" ");

    if(m_status.length() > 0)
        headerbuf.append(m_status.get());
    else
        headerbuf.append(HTTP_STATUS_OK);
    headerbuf.append("\r\n");
    
    headerbuf.append("Content-Type: ");
    if(m_content_type.length() > 0)
        headerbuf.append(m_content_type.get());
    else
        headerbuf.append("text/xml; charset=UTF-8");
    headerbuf.append("\r\n");

    if(inclLen && m_content_length > 0)
        headerbuf.append("Content-Length: ").append(m_content_length).append("\r\n");
    else
        setPersistentEligible(false);

    if(!(m_persistentEnabled && getPersistentEligible()))
        headerbuf.append("Connection: close\r\n");

    ForEachItemIn(x, m_cookies)
    {
        CEspCookie* cookie = &m_cookies.item(x);
        if(cookie == NULL)
            continue;
        StringBuffer cookiehn;
        cookie->getSetCookieHeaderName(cookiehn);
        headerbuf.append(cookiehn.str()).append(": ");
        cookie->appendToResponseHeader(headerbuf);
        headerbuf.append("\r\n");
    }

    ForEachItemIn(i, m_headers)
    {
        const char* oneheader = (const char*)m_headers.item(i);
        headerbuf.append(oneheader).append("\r\n");
    }
    
    if(m_context.get())
    {
        StringArray& customHeaders = m_context->queryCustomHeaders();
        ForEachItemIn(j, customHeaders)
        {
            const char* oneheader = (const char*)customHeaders.item(j);
            if(oneheader && *oneheader)
                headerbuf.append(oneheader).append("\r\n"); 
        }
    }

    headerbuf.append("\r\n");

    return headerbuf;

}

int CHttpResponse::parseFirstLine(char* oneline)
{
    if(*oneline == 0)
        return -1;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("http response status = %s", oneline);

    char* ptr = oneline;
    while(*ptr != '\0' && *ptr != ' ')
        ptr++;

    if(*ptr != '\0')
    {
        *ptr = 0;
        ptr++;
    }

    m_version.set(oneline);

    while(*ptr == ' ')
        ptr++;

    m_status.set(ptr);

    return 0;
}

void CHttpResponse::parseOneCookie(char* cookiestr)
{
    if(cookiestr == NULL)
        return;
    char* curword;
    char* curptr = cookiestr;   

    CEspCookie* cookie = NULL;
    curptr = Utils::getWord(curptr, curword, ";");
    StringBuffer name, value;
    Utils::parseNVPair(curword, name, value);
    if(name.length() == 0)
        return;
    cookie = new CEspCookie(name.str(), value.str());
    m_cookies.append(*cookie);

    while(curptr != NULL && *curptr != 0)
    {
        curptr = Utils::getWord(curptr, curword, ";");
        if(curword == NULL)
            break;
        StringBuffer name, value;
        Utils::parseNVPair(curword, name, value);
        if(name.length() == 0)
            continue;
        else if(stricmp(name.str(), "Version") == 0)
            cookie->setVersion(atoi(value.str()));
        else if(stricmp(name.str(), "Path") == 0)
            cookie->setPath(value.str());
        else if(stricmp(name.str(), "Domain") == 0)
            cookie->setDomain(value.str());
        else if(stricmp(name.str(), "Port") == 0)
            cookie->setPorts(value.str());
        else if(stricmp(name.str(), "Max-Age") == 0)
            cookie->setMaxAge(atoi(value.str()));
        else if(stricmp(name.str(), "Discard") == 0)
            cookie->setDiscard(true);
        else if(stricmp(name.str(), "Secure") == 0)
            cookie->setSecure(true);
        else if(stricmp(name.str(), "Comment") == 0)
            cookie->setComment(value.str());
        else if(stricmp(name.str(), "CommentURL") == 0)
            cookie->setCommentURL(value.str());
        
    }   
}

void CHttpResponse::parseCookieHeader(char* cookiestr)
{
    if(cookiestr == NULL)
        return;
    //TODO: for now assume each Set-Cookie only has one cookie.
    parseOneCookie(cookiestr);
}


void CHttpResponse::sendBasicChallenge(const char* realm, bool includeContent)
{
    StringBuffer authheader;
    authheader.appendf("Basic realm=\"%s\"", realm);
    addHeader("WWW-Authenticate", authheader.str());

    if (includeContent)
    {
        setContentType("text/html; charset=UTF-8");
        setContent(
            "<html xmlns=\"http://www.w3.org/1999/xhtml\">"
                "<head>"
                    "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>"
                    "<title>ESP - Access Denied</title>"
                    "<script type='text/javascript'>"
                        "function closeWin() { top.opener=top; top.close(); }"
                    "</script>"
                "</head>"
                "<body onload=\"javascript:closeWin();\">"
                    "<b>Access Denied -- Valid username and password required!</b>"
                "</body>"
            "</html>"
        );
    }

    setStatus(HTTP_STATUS_UNAUTHORIZED);
    send();
}


void CHttpResponse::sendBasicChallenge(const char* realm, const char* content)
{
    StringBuffer authheader;
    authheader.appendf("Basic realm=\"%s\"", realm);
    addHeader("WWW-Authenticate", authheader.str());

    if (content != NULL && *content != '\0')
    {
        setContentType("text/html; charset=UTF-8");
        setContent(content);
    }

    setStatus(HTTP_STATUS_UNAUTHORIZED);
    send();
}

int CHttpResponse::processHeaders(IMultiException *me)
{
    char oneline[MAX_HTTP_HEADER_LEN + 1];
    int lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN, me);
    if (lenread <= 0)
    {
        if (lenread == 0)
            m_peerClosed = true;
        return -1;
    }

    // Process "100 Continue" headers
    // Some HTTP/1.1 webservers may send back "100 Continue" before it reads the posted request body.
    while(Utils::strncasecmp(oneline, "HTTP/1.1 100", strlen("HTTP/1.1 100")) == 0)
    {
        //Read until empty line, meaning the end of "100 Continue" part
        while(lenread >= 0 && oneline[0] != '\0')
        {
            lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN, me);
        }
    
        // Read the next line, should be the status line.
        lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN, me);
        if(lenread <= 0)
            return 0;
    }

    parseFirstLine(oneline);
    lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN, me);
    while(lenread >= 0 && oneline[0] != '\0')
    {
        parseOneHeader(oneline);
        lenread = m_bufferedsocket->readline(oneline, MAX_HTTP_HEADER_LEN, me);
    }

    setPersistentEligible(checkPersistentEligible());

    return 0;
}


bool CHttpResponse::httpContentFromFile(const char *filepath)
{
    StringBuffer mimetype, etag, lastModified;
    MemoryBuffer content;
    //Set 'modified = false' here since this is called by CMySoapBinding::onGetFile() which does
    //not need to check and set both 'etag' and 'lastModified' inside httpContentFromFile().
    bool modified = false;
    bool ok = ::httpContentFromFile(filepath, mimetype, content, modified, lastModified, etag);
    if (ok)
    {
        setContent(content.length(), content.toByteArray());
        setContentType(mimetype.str());
        setStatus(HTTP_STATUS_OK);
    }
    else
    {
        setStatus(HTTP_STATUS_NOT_FOUND);
    }

    return ok;
}

void CHttpResponse::setETagCacheControl(const char *etag, const char *contenttype)
{
    if (etag && *etag)
        addHeader("Etag",  etag);

    if (!strncmp(contenttype, "image/", 6))
    {
       //if file being requested is an image file then set its expiration to a year
       //so browser does not keep reloading it
        addHeader("Cache-control",  "max-age=31536000");
    }
    else
        addHeader("Cache-control",  "max-age=300");
}

void CHttpResponse::CheckModifiedHTTPContent(bool modified, const char *lastmodified, const char *etag, const char *contenttype, MemoryBuffer &content)
{
    if (!modified)
    {
        if (etag && *etag)
            setETagCacheControl(etag, contenttype);
        setStatus(HTTP_STATUS_NOT_MODIFIED);
    }
    else
    {
        addHeader("Last-Modified", lastmodified);
        setETagCacheControl(etag, contenttype);
        setContent(content.length(), content.toByteArray());
        setContentType(contenttype);
        setStatus(HTTP_STATUS_OK);
    }
}

int CHttpResponse::receive(IMultiException *me)
{
    // If it's receiving a response, it's behaving as the client side of this conversation.
    if(m_bufferedsocket.get() != NULL)
        m_bufferedsocket->setReadTimeout(m_timeout);
    
    return CHttpMessage::receive(me);
}

int CHttpResponse::receive(bool alwaysReadContent, IMultiException *me)
{
    // If it's receiving a response, it's behaving as the client side of this conversation.
    if(m_bufferedsocket.get() != NULL)
        m_bufferedsocket->setReadTimeout(m_timeout);
    
    if (processHeaders(me)==-1)
        return -1;

    if (getEspLogLevel()>LogNormal)
        DBGLOG("Response headers processed! content_length = %" I64F "d", m_content_length);
    
    char status_class = '2';
    if(m_status.length() > 0)
        status_class = *(m_status.get());

    if(m_content_length > 0)
    {
        readContent();
        if (getEspLogLevel()>LogNormal)
            DBGLOG("length of response content read = %d", m_content.length());
    }
    else if(alwaysReadContent && status_class != '4' && status_class != '5' && m_content_length == -1)
    {
        //HTTP protocol does not require a content length: read until socket closed
        readContentTillSocketClosed();
        if (getEspLogLevel()>LogNormal)
            DBGLOG("length of content read = %d", m_content.length());
    }

    bool decompressed = false;
    int compressType = 0;
    if (shouldDecompress(compressType))
        decompressed = decompressContent(nullptr, compressType);

    if (getEspLogRequests() == LogRequestsAlways)
    {
        if (!decompressed)
            logMessage(LOGCONTENT, "HTTP response content received:\n");
        else
            logMessage(LOGCONTENT, "Compressed HTTP response content received, decompressed content:\n");
    }

    return 0;

}

int CHttpResponse::sendException(IEspHttpException* e)
{
    StringBuffer msg;
    e->errorMessage(msg);
    setStatus(e->getHttpStatus());
    setContentType(HTTP_TYPE_TEXT_PLAIN);
    setContent(msg.str());
    send();
    return 0;
}

StringBuffer &toJSON(StringBuffer &json, IMultiException *me, const char *callback)
{
    IArrayOf<IException> &exs = me->getArray();
    if (callback && *callback)
        json.append(callback).append('(');
    appendJSONName(json.append("{"), "Exceptions").append("{");
    appendJSONValue(json, "Source", me->source());
    appendJSONName(json, "Exception").append("[");
    ForEachItemIn(i, exs)
    {
        IException &e = exs.item(i);
        if (i>0)
            json.append(",");
        StringBuffer msg;
        appendJSONValue(json.append("{"), "Code", e.errorCode());
        appendJSONValue(json, "Message", e.errorMessage(msg).str());
        json.append("}");
    }
    json.append("]}}");
    if (callback && *callback)
        json.append(");");
    return json;
}

StringBuffer &toText(StringBuffer &content, IMultiException *me)
{
    IArrayOf<IException> &exs = me->getArray();
    content.append("Exceptions: \n");
    content.append("  Source: ").append(me->source()).append("\n");
    ForEachItemIn(i, exs)
    {
        IException &e = exs.item(i);
        StringBuffer msg;
        content.append("  Exception: \n");
        content.append("    Code: ").append(e.errorCode()).append("\n");
        content.append("    Message: ").append(e.errorMessage(msg)).append("\n");
    }
    return content;
}

/*
 * Function to determine ESP exception logging class based on exception audience
 */
inline LogMsgClass mapErrAudToLogClass(IMultiException *me)
{
    switch (me->errorAudience())
    {
    case MSGAUD_user:
        return MSGCLS_information;
    case MSGAUD_programmer:
        return MSGCLS_error;
    case MSGAUD_audit:
    case MSGAUD_operator:
    default:
        return MSGCLS_warning;
    }
}

bool CHttpResponse::handleExceptions(IXslProcessor *xslp, IMultiException *me, const char *serv, const char *meth, const char *errorXslt)
{
    IEspContext *context=queryContext();
    if (me->ordinality()>0)
    {
        StringBuffer msg;
        LOG(MCexception(me, mapErrAudToLogClass(me)), "Exception(s) in %s::%s - %s", serv, meth, me->errorMessage(msg).append('\n').str());

        StringBuffer content;
        switch (context->getResponseFormat())
        {
        case ESPSerializationJSON:
        {
            setContentType(HTTP_TYPE_JSON);
            toJSON(content, me, context->queryRequestParameters()->queryProp("jsonp"));
            break;
        }
        case ESPSerializationXML:
            setContentType(HTTP_TYPE_APPLICATION_XML);
            me->serialize(content);
            break;
        case ESPSerializationTEXT:
            setContentType(HTTP_TYPE_TEXT_PLAIN);
            toText(content, me);
            break;
        case ESPSerializationANY:
        default:
            {
            if (!errorXslt || !*errorXslt)
                return false;
            setContentType("text/html");
            StringBuffer xml;
            xslTransformHelper(xslp, me->serialize(xml), errorXslt, content, context->queryXslParameters());
            }
        }
        setContent(content);
        send();
        return true;
    }
    return false;
}

void CHttpResponse::handleExceptions(IXslProcessor *xslp, IMultiException *me, const char *serv, const char *meth, const char *errorXslt, bool logHandleExceptions)
{
    if (handleExceptions(xslp, me, serv, meth, errorXslt) && logHandleExceptions)
        PROGLOG("Exception(s) handled");
}

inline bool zlibTypeFromHeader(const char* acceptEncodingHeader, int& compressType)
{
    if (!acceptEncodingHeader || !*acceptEncodingHeader)
        return false;

    StringArray encodings;
    encodings.appendList(acceptEncodingHeader, ",");
    int gzipq = -1, deflateq = -1, zlibq = -1;
    ForEachItemIn(x, encodings)
    {
        const char* encoding = encodings.item(x);
        int qval = 1000; //Default q value is 1.0 (1000 after timing 1000)
        const char* qptr = strstr(encoding, ";q=");
        if (!qptr)
            qptr = strstr(encoding, " q=");
        if (qptr)
            qval = 1000*atof(qptr+3); //Smallest q value possible is 0.001, so times 1000 to make it integer
        if (qval == 0)
            continue;
        const char* end = encoding;
        while (*end != '\0' && *end != ';' && *end != ' ')
            end++;
        size_t len = end - encoding;
        if (len == 4 && strncmp(encoding, "gzip", len) == 0)
            gzipq = qval;
        else if (len == 7 && strncmp(encoding, "deflate", len) == 0)
            deflateq = qval;
        else if (len == 9 && strncmp(encoding, "x-deflate", len) == 0)
            zlibq = qval;
    }

    if (gzipq > 0 && gzipq >= deflateq && gzipq >= zlibq) //Use gzip as much as possible due to some reported browser issues with deflate
        compressType = static_cast<int>(ZlibCompressionType::GZIP);
    else if (deflateq > 0 && deflateq >= zlibq)
        compressType = static_cast<int>(ZlibCompressionType::DEFLATE);
    else if (zlibq > 0)
        compressType = static_cast<int>(ZlibCompressionType::ZLIB_DEFLATE);

    return gzipq > 0 || deflateq > 0 || zlibq > 0;
}

inline const char* zlibType2Header(ZlibCompressionType zltype)
{
    if (zltype==ZlibCompressionType::GZIP)
        return "gzip";
    else if (zltype==ZlibCompressionType::DEFLATE)
        return "deflate";
    else if (zltype==ZlibCompressionType::ZLIB_DEFLATE)
        return "x-deflate";
    else
        return "";
}

bool CHttpResponse::shouldCompress(int& compressType)
{
    compressType = 0;
    if (!m_compressionEnabled)
        return false;
    if (m_content_length == 0 || m_content.length() == 0)
        return false;

#ifndef _USE_ZLIB
    return false;
#else
    if (hasHeader(HTTP_HEADER_CONTENT_ENCODING) || hasHeader(HTTP_HEADER_TRANSFER_ENCODING)) //Already encoded/compressed
        return false;

    static const int DEFAULT_MIN_COMPRESS_LENGTH = 1000;
    int min_len = DEFAULT_MIN_COMPRESS_LENGTH;
    IPropertyTree* proccfg = nullptr;
    IEspServer* espserver = queryEspServer();
    if (espserver)
        proccfg = espserver->queryProcConfig();
    if (proccfg)
        min_len = proccfg->getPropInt("@minCompressLength", DEFAULT_MIN_COMPRESS_LENGTH);
    if (m_content.length() < min_len)
        return false;

    IEspContext * context = queryContext();
    if (!context)
        return false;
    CHttpRequest* request = dynamic_cast<CHttpRequest*>(context->queryRequest());
    if (!request)
        return false;
    StringBuffer acceptEncoding;
    request->getHeader(HTTP_HEADER_ACCEPT_ENCODING, acceptEncoding);
    if (!zlibTypeFromHeader(acceptEncoding.toLowerCase().str(), compressType))
        return false;

    const char* content_type = m_content_type.get();
    if (m_content_type.length() == 0 ||
         !( strnicmp(content_type, "text", 4) == 0
         || strnicmp(content_type, HTTP_TYPE_APPLICATION_XML, strlen(HTTP_TYPE_APPLICATION_XML)) == 0
         || strnicmp(content_type, HTTP_TYPE_SOAP, strlen(HTTP_TYPE_SOAP)) == 0
         || strnicmp(content_type, HTTP_TYPE_SVG_XML, strlen(HTTP_TYPE_SVG_XML)) == 0
         || strnicmp(content_type, HTTP_TYPE_JSON, strlen(HTTP_TYPE_JSON)) == 0
         || strnicmp(content_type, HTTP_TYPE_JAVASCRIPT, strlen(HTTP_TYPE_JAVASCRIPT)) == 0
         || strnicmp(content_type, HTTP_TYPE_JAVASCRIPT2, strlen(HTTP_TYPE_JAVASCRIPT2)) == 0))
        return false;

    return true;
#endif
}

bool CHttpResponse::compressContent(StringBuffer* originalContent, int compressType)
{
#ifdef _USE_ZLIB
    ZlibCompressionType zlibtype = static_cast<ZlibCompressionType>(compressType);
    MemoryBuffer zippedContent;
    try
    {
        zlib_deflate(zippedContent, m_content.str(), m_content.length(), GZ_DEFAULT_COMPRESSION, zlibtype);
    }
    catch(IException* e)
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) compressing response content.", e->errorCode(), e->errorMessage(estr).str());
        return false;
    }
    catch(...)
    {
        IERRLOG("Unknown exception compressing response content.");
        return false;
    }
    if (originalContent != nullptr)
        originalContent->setown(m_content);
    setContent(zippedContent.length(), zippedContent.toByteArray());
    addHeader(HTTP_HEADER_CONTENT_ENCODING, zlibType2Header(zlibtype));
    return true;
#endif
}

bool CHttpResponse::shouldDecompress(int& compressType)
{
    compressType = 0;
    if (!m_compressionEnabled)
        return false;
    if (m_content_length == 0 || m_content.length() == 0)
        return false;

#ifdef _USE_ZLIB
    StringBuffer ceheader;
    getHeader(HTTP_HEADER_CONTENT_ENCODING, ceheader);
    ceheader.trim();
    if (ceheader.length() == 0)
        return false;
    if (strieq(ceheader.str(), "gzip"))
        compressType = static_cast<int>(ZlibCompressionType::GZIP);
    else if (strieq(ceheader.str(), "deflate"))
        compressType = static_cast<int>(ZlibCompressionType::DEFLATE);
    else if (strieq(ceheader.str(), "x-deflate"))
        compressType = static_cast<int>(ZlibCompressionType::ZLIB_DEFLATE);
    else
        return false;
    return true;
#else
    return false;
#endif
}

bool CHttpResponse::decompressContent(StringBuffer* originalContent, int compressType)
{
#ifdef _USE_ZLIB
    ZlibCompressionType zlibtype = static_cast<ZlibCompressionType>(compressType);
    StringBuffer decompressedContent;
    try
    {
        httpInflate((const unsigned char*)m_content.str(), m_content.length(), decompressedContent, zlibtype==ZlibCompressionType::GZIP);
    }
    catch(IException* e)
    {
        StringBuffer estr;
        IERRLOG("Exception(%d, %s) decompressing response content.", e->errorCode(), e->errorMessage(estr).str());
        return false;
    }
    catch(...)
    {
        IERRLOG("Unknown exception decompressing response content.");
        return false;
    }
    if (originalContent != nullptr)
        originalContent->setown(m_content);
    m_content.setown(decompressedContent);
    m_content_length = m_content.length();
    removeHeader(HTTP_HEADER_CONTENT_ENCODING);
    return true;
#endif
}

bool CHttpResponse::checkPersistentEligible()
{
    if(m_content_length <= 0)
        return false;

    return CHttpMessage::checkPersistentEligible();
}
