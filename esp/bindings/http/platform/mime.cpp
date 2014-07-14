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

#pragma warning( disable : 4786 )

#ifdef WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

//Jlib
#include "jliball.hpp"

//ESP Bindings
#include "http/platform/mime.hpp"
#include "bindutil.hpp"
#include "httptransport.hpp"

CMimeBodyPart::CMimeBodyPart(const char* content_type, const char* encoding, const char* cid, const char* location, StringBuffer* content, const char* content_disposition)
{
    m_content_type.set(content_type);
    m_content_disposition.set(content_disposition);
    m_encoding.set(encoding);
    m_cid.set(cid);
    m_location.set(location);
    if(content != NULL)
        m_content.append(content->length(), content->str());
}

CMimeBodyPart::~CMimeBodyPart()
{
}

const char* CMimeBodyPart::getContentType()
{
    return m_content_type.get();
}

const char* CMimeBodyPart::getContentDisposition()
{
    return m_content_disposition.get();
}

const char* CMimeBodyPart::getEncoding()
{
    return m_encoding.get();
}

const char* CMimeBodyPart::getCid()
{
    return m_cid.get();
}

const char* CMimeBodyPart::getLocation()
{
    return m_location.get();
}

StringBuffer& CMimeBodyPart::getContent(StringBuffer& content)
{
    return content.append(m_content.length(), m_content.str());
}

void CMimeBodyPart::setContent(int len, const char* buffer)
{
    m_content.append(len, buffer);
}

void CMimeBodyPart::serialize(StringBuffer & buffer)
{
    buffer.append("Content-Type: ").append(m_content_type.get()).append("\r\n");
    buffer.append("Content-Transfer-Encoding: ").append(m_encoding.get()).append("\r\n");
    buffer.append("Content-ID: <").append(m_cid.get()).append(">\r\n");
    buffer.append("\r\n");
    buffer.append(m_content.str());
    buffer.append("\r\n");
}


/***********************************************************************************/

CMimeMultiPart::CMimeMultiPart(const char* mime_version, const char* content_type, const char* boundary, const char* type, const char* start)
{
    m_mime_version.set(mime_version);
    m_content_type.set(content_type);
    m_boundary.set(boundary);
    m_type.set(type);
    m_start.set(start);
}

CMimeMultiPart::~CMimeMultiPart()
{
}

const char* CMimeMultiPart::getContentType()
{
    return m_content_type.get();
}

void CMimeMultiPart::addBodyPart(CMimeBodyPart* part)
{
    m_parts.append(*part);
}

CMimeBodyPart* CMimeMultiPart::getBodyPart(const char* cid)
{
    ForEachItemIn(x, m_parts)
    {
        CMimeBodyPart& onepart = m_parts.item(x);
        if(!strcmp(onepart.getCid(), cid))
        {
            return LINK(&onepart);
        }
    }
    return NULL;
}

CMimeBodyPart* CMimeMultiPart::queryBodyPart(const char* cid)
{
    ForEachItemIn(x, m_parts)
    {
        CMimeBodyPart& onepart = m_parts.item(x);
        if(!strcmp(onepart.getCid(), cid))
        {
            return &onepart;
        }
    }
    return NULL;
}

CMimeBodyPart* CMimeMultiPart::getBodyPart(unsigned int seq)
{
    if(seq < m_parts.length())
    {
        CMimeBodyPart& onepart = m_parts.item(seq);
        return LINK(&onepart);
    }
    else
        return NULL;
}

CMimeBodyPart* CMimeMultiPart::queryBodyPart(unsigned int seq)
{
    if(seq < m_parts.length())
    {
        CMimeBodyPart& onepart = m_parts.item(seq);
        return &onepart;
    }
    else
        return NULL;
}

void CMimeMultiPart::setRootPart(CMimeBodyPart* part)
{
    const char* cid = part->getCid();
    m_start.set(cid);
    m_parts.append(*part);
}

CMimeBodyPart* CMimeMultiPart::getRootPart()
{
    if(m_start.length() > 0)
    {
        ForEachItemIn(x, m_parts)
        {
            CMimeBodyPart& onepart = m_parts.item(x);
            if(!strcmp(onepart.getCid(), m_start.get()))
            {
                return LINK(&onepart);
            }
        }
    }
    else
    {
        // If the root part is not specified, return the first one.
        return LINK(&m_parts.item(0));      
    }

    return NULL;
}

CMimeBodyPart* CMimeMultiPart::queryRootPart()
{
    if(m_start.length() > 0)
    {
        ForEachItemIn(x, m_parts)
        {
            CMimeBodyPart& onepart = m_parts.item(x);
            if(!strcmp(onepart.getCid(), m_start.get()))
            {
                return &onepart;
            }
        }
    }
    else
    {
        return &m_parts.item(0);
    }

    return NULL;
}

int CMimeMultiPart::getBodyCount()
{
    return m_parts.ordinality();
}

void CMimeMultiPart::setContentType(const char* content_type)
{
    m_content_type.clear();
    m_content_type.set(content_type);
}

void CMimeMultiPart::serialize(StringBuffer& contenttype, StringBuffer & buffer)
{
    if(m_parts.ordinality() > 1)
    {
        contenttype.append(m_content_type.get()).append("; boundary=").append(m_boundary.get());
        contenttype.append("; type=\"").append(m_type.get()).append("\"; start=\"<").append(m_start.get()).append(">\"");
        ForEachItemIn(x, m_parts)
        {
            buffer.append("--").append(m_boundary.get()).append("\r\n");
            CMimeBodyPart& onepart = m_parts.item(x);
            onepart.serialize(buffer);
            buffer.append("\r\n");
        }
        buffer.append("--").append(m_boundary.get()).append("\r\n");

    }
    else
    {
        contenttype.append(m_content_type.length() == 0 ? HTTP_TYPE_SOAP_UTF8 : m_content_type.get());
        CMimeBodyPart* rootpart = queryRootPart();
        rootpart->getContent(buffer);
    }
}

void CMimeMultiPart::unserialize(const char* contenttype, int text_length, const char* text)
{
    char* typebuf = new char[strlen(contenttype) + 1];
    strcpy(typebuf, contenttype);
    char* ptr = typebuf;
    char* oneword = NULL;

    // parse content type to get boundary, type and start
    ptr = Utils::getWord(ptr, oneword, "; ");
    while(oneword != NULL)
    {
        //DBGLOG(oneword);
        if(!Utils::strncasecmp(oneword, "boundary", strlen("boundary")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';

            oneword += strlen("boundary");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;
            m_boundary.set(oneword);
        }
        else if(!Utils::strncasecmp(oneword, "type", strlen("type")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';

            oneword += strlen("type");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;

            m_type.set(oneword);
        }
        else if(!Utils::strncasecmp(oneword, "start", strlen("start")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';
            if(oneword[strlen(oneword) - 1] == '>')
                oneword[strlen(oneword) - 1] = '\0';
        
            oneword += strlen("start");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;
            if(oneword[0] == '<')
                oneword++;

            m_start.set(oneword);
            //DBGLOG("start=%s", m_start.get());
        }

        ptr = Utils::getWord(ptr, oneword, "; ");
    }

    delete [] typebuf;

    int oneline_len = 0;
    int cur_pos = 0;
    int next_pos = Utils::getLine(text_length, 0, text, oneline_len);
    const char* curline = text;
    int boundarylen = m_boundary.length();

    // Skip possible text before the first boundary
    while(next_pos < text_length && !(oneline_len >= 2 && !Utils::strncasecmp(m_boundary.get(), curline + 2, boundarylen)))
    {
        cur_pos = next_pos;
        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        curline = text + cur_pos;
    }
    
    // Parse all the mime parts
    while(next_pos < text_length)
    {
        cur_pos = next_pos;
        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        if(next_pos >= text_length)
            break;
        StringBuffer ctype, encoding, cid, body, cdisp;

        //parse the headers of one mime part
        while(next_pos < text_length && oneline_len != 0)
        {
            if(!Utils::strncasecmp(text + cur_pos, "Content-Type", strlen("Content-Type")))
            {
                int namelen = strlen("Content-Type");
                cur_pos += namelen;
                while(cur_pos < next_pos && (text[cur_pos] == ' ' || text[cur_pos] == ':'))
                {
                    cur_pos++;
                    namelen++;
                }
                ctype.append(oneline_len - namelen, text+cur_pos);
            }
            else if(!Utils::strncasecmp(text + cur_pos, "Content-Transfer-Encoding", strlen("Content-Transfer-Encoding")))
            {
                int namelen = strlen("Content-Transfer-Encoding");
                cur_pos += namelen;
                while(cur_pos < next_pos && (text[cur_pos] == ' ' || text[cur_pos] == ':'))
                {
                    cur_pos++;
                    namelen++;
                }
                encoding.append(oneline_len - namelen, text+cur_pos);
            }
            else if(!Utils::strncasecmp(text + cur_pos, "Content-Disposition", strlen("Content-Disposition")))
            {
                int namelen = strlen("Content-Disposition");
                cur_pos += namelen;
                while(cur_pos < next_pos && (text[cur_pos] == ' ' || text[cur_pos] == ':'))
                {
                    cur_pos++;
                    namelen++;
                }
                cdisp.append(oneline_len - namelen, text+cur_pos);
            }
            if(!Utils::strncasecmp(text + cur_pos, "Content-ID", strlen("Content-ID")))
            {
                int namelen = strlen("Content-ID");
                cur_pos += namelen;
                while(cur_pos < next_pos && (text[cur_pos] == ' ' || text[cur_pos] == ':' || text[cur_pos] == '<'))
                {
                    cur_pos++;
                    namelen++;
                }

                int val_len = oneline_len - namelen;
                if(text[cur_pos + val_len - 1] == '>')
                    val_len -= 1;
                cid.append(val_len, text+cur_pos);
            }

            cur_pos = next_pos;
            next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        }

        // Got to the end of mulitpart message
        if(next_pos >= text_length)
            break;

        // Read in the content of one mime part
        cur_pos = next_pos;
        int bb = cur_pos;
        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        const char* curline = text + cur_pos;
        while(next_pos < text_length && !(oneline_len >= 2 && !Utils::strncasecmp(m_boundary.get(), curline + 2, boundarylen)))
        {
            cur_pos = next_pos;
            next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
            curline = text + cur_pos;
        }

        // Get rid of CR/LF at the end of the content
        int be = cur_pos - 1;
        if(be >0 && (text[be] == '\r' || text[be] == '\n'))
            be--;

        //If the body is empty, ignore it
        if(be <= bb)
            continue;

        body.append(be - bb, text + bb);

        CMimeBodyPart* onepart = new CMimeBodyPart(ctype.str(), encoding.str(), cid.str(), "", &body, cdisp.str());

        addBodyPart(onepart);
    }
    
}

void CMimeMultiPart::parseContentType(const char* contenttype)
{
    char* typebuf = new char[strlen(contenttype) + 1];
    strcpy(typebuf, contenttype);
    char* ptr = typebuf;
    char* oneword = NULL;

    // parse content type to get boundary, type and start
    ptr = Utils::getWord(ptr, oneword, "; ");
    while(oneword != NULL)
    {
        //DBGLOG(oneword);
        if(!Utils::strncasecmp(oneword, "boundary", strlen("boundary")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';

            oneword += strlen("boundary");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;
            m_boundary.set(oneword);
        }
        else if(!Utils::strncasecmp(oneword, "type", strlen("type")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';

            oneword += strlen("type");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;

            m_type.set(oneword);
        }
        else if(!Utils::strncasecmp(oneword, "start", strlen("start")))
        {
            if(oneword[strlen(oneword) - 1] == '"')
                oneword[strlen(oneword) - 1] = '\0';
            if(oneword[strlen(oneword) - 1] == '>')
                oneword[strlen(oneword) - 1] = '\0';
        
            oneword += strlen("start");
            while(*oneword != '\0' && (*oneword == ' ' || *oneword == '='))
                oneword++;

            if(oneword[0] == '"')
                oneword++;
            if(oneword[0] == '<')
                oneword++;

            m_start.set(oneword);
        }

        ptr = Utils::getWord(ptr, oneword, "; ");
    }

    delete [] typebuf;

    return;
}

enum BoundaryCheckState { BoundaryNotFound, BoundaryFound, PossibleBoundary };
bool CMimeMultiPart::separateMultiParts(MemoryBuffer& firstPart, MemoryBuffer& remainder, __int64 contentNotRead)
{
    int boundaryLen = m_boundary.length();
    if (boundaryLen < 1)
        return false;

    int totalLength = firstPart.length();
    if (totalLength < boundaryLen)
        return false;

    BoundaryCheckState boundaryCheckState = BoundaryNotFound;
    const char* startPos = firstPart.toByteArray();
    int offset = 0;
    while(offset < totalLength)
    {
        if ((totalLength - offset) < (boundaryLen + 2))
        {//Do not check this line now since buffer size is not longer than boundary line.
         //The boundary line has two extra '-'s before the boundary ID
            if (contentNotRead > 0)
            {
                boundaryCheckState = PossibleBoundary;//a boundary line may be cut into two parts
            }
            break;
        }

        int lineLength = 0;
        int nextOffset = Utils::getLine(totalLength, offset, startPos, lineLength);
        //skip two extra '-' before checking the boundary
        if ((lineLength > 2) && (!Utils::strncasecmp(m_boundary.get(), startPos + offset + 2, boundaryLen)))
        {
            boundaryCheckState = BoundaryFound;//Found a m_boundary
            break;
        }
        offset = nextOffset;
    }

    if (boundaryCheckState == BoundaryNotFound)
        return false;

    offset -= 2;//the crlf in the front of the boundary line should not be included into file content
    remainder.append(totalLength - offset, startPos + offset);
    firstPart.setLength(offset);

    return (boundaryCheckState==BoundaryFound);
}

void CMimeMultiPart::readUploadFileName(MemoryBuffer& fileContent, StringBuffer& fileName)
{
    int text_length = fileContent.length();
    if (text_length < 1)
        return;

    MemoryBuffer fileContentIn;
    fileContentIn.append(fileContent.length(), fileContent.toByteArray());
    char* text = (char*) fileContentIn.toByteArray();

    int oneline_len = 0;
    int cur_pos = 0;
    int next_pos = Utils::getLine(text_length, 0, text, oneline_len);
    const char* curline = text;
    int boundarylen = m_boundary.length();

    // Skip possible text before the first boundary
    while(next_pos < text_length && !(oneline_len >= 2 && !Utils::strncasecmp(m_boundary.get(), curline + 2, boundarylen)))
    {
        cur_pos = next_pos;
        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        curline = text + cur_pos;
    }
    
    // Parse all the mime parts
    while(next_pos < text_length)
    {
        cur_pos = next_pos;
        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        if(next_pos >= text_length)
            break;

        //parse the headers of one mime part
        while(next_pos < text_length && oneline_len != 0)
        {
            if(!Utils::strncasecmp(text + cur_pos, "Content-Disposition", strlen("Content-Disposition")))
            {
                StringBuffer cdisp;
                int namelen = strlen("Content-Disposition");
                cur_pos += namelen;
                while(cur_pos < next_pos && (text[cur_pos] == ' ' || text[cur_pos] == ':'))
                {
                    cur_pos++;
                    namelen++;
                }
                cdisp.append(oneline_len - namelen, text+cur_pos);

                if (cdisp.length() > 0)
                {
                    char* ptr = (char*) cdisp.str();
                    while (ptr && *ptr)
                    {
                        char* pptr = strchr(ptr, ';');
                        if (Utils::strncasecmp(ptr, "filename=", 9))
                        {
                            if (!pptr)
                                break;
                            else
                            {
                                ptr = pptr + 1;
                                while (ptr && (ptr[0] == ' ')) //skip space after ';'
                                    ptr++;
                            }
                        }
                        else
                        {
                            fileName.append(ptr + 10); //filename="abc.txt"
                            unsigned len = fileName.length() - 1;
                            if (pptr)
                            {
                                len = pptr - ptr - 11;
                            }

                            fileName.remove(len, fileName.length() - len);
                            break;
                        }
                    }
                }
            }

            cur_pos = next_pos;
            next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        }

        // Got to the end of mulitpart message
        if(next_pos >= text_length)
            break;

        // Read in the content of one mime part
        cur_pos = next_pos;
        int bb = cur_pos;
        int be = text_length;

        next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
        if (next_pos < text_length)
        {
            const char* curline = text + cur_pos;
            while(next_pos < text_length && !(oneline_len >= 2 && !Utils::strncasecmp(m_boundary.get(), curline + 2, boundarylen)))
            {
                cur_pos = next_pos;
                next_pos = Utils::getLine(text_length, next_pos, text, oneline_len);
                curline = text + cur_pos;
            }

            // Get rid of CR/LF at the end of the content
            int be = cur_pos - 1;
            if(be >0 && (text[be] == '\r' || text[be] == '\n'))
                be--;
        }

        //If the body is empty, ignore it
        if(fileName.length() > 0)
        {
            fileContent.clear();
            if(be > bb)
            {
                fileContent.append(text_length - bb, text + bb);
            }

            break;
        }
    }
    
    return;
}
