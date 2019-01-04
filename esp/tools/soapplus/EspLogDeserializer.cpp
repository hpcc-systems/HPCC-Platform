/*
##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
*/

#pragma warning(disable:4786)
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <map>
#include "jliball.hpp"
#include "xsdparser.hpp"
#include "http.hpp"

// Utility class to read 1K at a time. Useful when there're a bunch of small reads.
class BufferedReader : public CInterface, implements IInterface
{
private:
    int m_fd;
    char m_buf[1024];
    int m_remain;
    int m_curpos;

public:
    IMPLEMENT_IINTERFACE;

    BufferedReader(int fd)
    {
        m_fd = fd;
        m_curpos = 0;
        m_remain = 0;
    }
    
    int read(char* buf, int buflen)
    {
        int totalread = 0;
        while(1)
        {
            if(m_remain <= 0)
            {
                int len = ::read(m_fd, m_buf, 1024);
                if(len <= 0)
                    break;
                m_curpos = 0;
                m_remain = len;
            }
            if(m_remain >= buflen)
            {
                strncpy(buf+totalread, m_buf+m_curpos, buflen);
                totalread += buflen;
                m_remain -= buflen;
                m_curpos += buflen;
                break;
            }
            else
            {
                strncpy(buf+totalread, m_buf+m_curpos, m_remain);
                totalread += m_remain;
                m_remain = 0;
                m_curpos = 0;
            }
        }

        return totalread;
    }
};

// Utility class to read one line from a file.
class BufferedLineReader : public CInterface, implements IInterface
{
private:
    Owned<BufferedReader> m_reader;

public:
    IMPLEMENT_IINTERFACE;

    BufferedLineReader(int fd)
    {
        m_reader.setown(new BufferedReader(fd));
    }

    virtual ~BufferedLineReader()
    {
    }

    int readLine(StringBuffer& buf)
    {
        char onechar;
        int len = 0;
        char linebuf[8196];
        int curpos = 0;
        while((len = m_reader->read(&onechar, 1)) > 0)
        {
            if(curpos == 8196)
            {
                buf.append(8196, linebuf);
                curpos = 0;
            }
            linebuf[curpos++] = onechar;
            if (onechar == '\n')
                break;
        }

        if (curpos > 0)
            buf.append(curpos, linebuf);

        return buf.length();
    }
};

typedef std::map<std::string, std::string>  StringStringMap;
static StringStringMap s_methodMap;
static StringStringMap s_requestNameMap;

static void LoadMethodMappings()
{
    FILE* fp = fopen("EspMethods.txt", "r");
    if (!fp)
    {
        puts("Failed to open EspMethods.txt file!");
        return;
    }
    char line[1024];
    int lineno = 0;

    StringBuffer service;
    StringArray strArray;

    while (fgets(line, sizeof(line)-1, fp))
    {
        lineno++;
        if (*line == '#')
            continue;

        char* p = line;
        while (isspace(*p))
            p++;

        char* lastChar = p + strlen(p) - 1;
        if (*lastChar == '\n')
            *lastChar = '\0';

        if (*p == '[')
        {
            const char* q = strchr(++p, ']');
            if (!q)
                q = p + strlen(p);
            service.clear().append(q-p, p).trim();
        }
        else if (*p)
        {
            strArray.kill();
            strArray.appendList(p, "= \t()");

            const unsigned int ord = strArray.ordinality();
            if (ord == 0)
                printf( "Syntax error in EspMethods.txt at line %d: ", lineno++);
            else
            {
                StringBuffer method = strArray.item(0);
                method.trim();

                if (ord > 1)
                {
                    //method=config(request)
                    //method=*(request)   when config=method

                    StringBuffer config(strArray.item(1));
                    StringBuffer url(service);

                    if (0 != strcmp(config.trim().str(), "*"))
                        url.append('/').append( method.str() );

                    s_methodMap[config.str()] = url.str();

                    if (strArray.ordinality() > 2)
                    {
                        StringBuffer request( strArray.item(2) );
                        s_requestNameMap[ config.str() ] = request.trim().str();
                    }
                }
                else
                    s_methodMap[method.str()] = service.str();
            }
        }
    }

    int rc = ferror(fp);
    if (rc)
        OERRLOG("Loading EspMethods.txt failed (may be partially loaded), system error code: %d", rc);
    fclose(fp);
}

static bool lookupMethod(const char* config, StringBuffer& service, StringBuffer& method, StringBuffer& request)
{
    bool rc = false;

    StringStringMap::const_iterator it = s_methodMap.find(config);
    if (it != s_methodMap.end())
    {
        StringBuffer s((*it).second.c_str());
        const char* p = strchr(s.str(), '/');
        if (!p)
            service.clear().append( s.str() );
        else
        {
            StringArray strArray;
            strArray.appendList(s.str(), "/");
            if (strArray.ordinality() < 2)
                printf("Invalid configuration: %s", s.str());
            else
            {
                service.clear().append(strArray.item(0));
                method .clear().append(strArray.item(1));
            }
        }

        it = s_requestNameMap.find( config );
        if (it != s_requestNameMap.end())
            request.append( (*it).second.c_str() );
        else
            request.append(config).append("Request");

        rc = true;
    }
    return rc;
}

/*
QUERY: GBGroup[User[ReferenceCode(RJVCC)GLBPurpose(1)DLPurpose(1)]SearchBy[RequestDetails[Profile(Sweden)]Person[Title(Mr)FirstName(Floren)Gender(Female)]Addresses[Address1[AddressLayout(5)BuildingNumber(2)Street(Norgardsplan)Country(Sweden)ZipPCode(55337)]]]]"
*/
static bool expandConciseRequest(const char* concise, StringBuffer& service, StringBuffer& method, 
                                 StringBuffer& request, StringBuffer& xml, StringBuffer& msg)
{
    method.clear();
    msg.clear();
    xml.clear();

    if (!concise)
        return false;

    StringStack tagStack;
    StringBuffer tag;
    const char* p;
    const char* q;
    const char* errP=NULL;
    bool bValue=false;
    int indent = 4;

    p = q = concise;
    while (*q && errP==NULL)
    {
        switch (*q)
        {
        case '[':
            if (bValue)
            {
                msg.appendf("Invalid concise XML: no matching ')' for tag %s:\n", tag.str());
                errP = q;
            }
            else
            {
                tag.clear().append( q-p, p).trim();
                if (p==concise)
                {
                    const char* r = strstr(tag.str(), "Request");
                    if (r)
                    {
                        method.append( r-tag.str(), tag.str());
                        StringBuffer config(method);
                        lookupMethod(config.str(), service, method, request);
                    }
                    else
                    {
                        StringBuffer config(tag);
                        lookupMethod(tag.str(), service, tag, request);
                        method.append( tag );
                        tag.clear().append(request);
                    }
                }
                //printf("tag: [%s]\n", tag);
                tagStack.push_back(tag.str());
                indent += 2;
                xml.appendN(indent, ' ');
                xml.append('<').append(tag).append(">\n");
                p=++q;
            }
            break;
        case ']':
            if (bValue)
            {
                msg.appendf("Invalid concise XML: no matching ')' for tag %s:\n", tag.str());
                errP = q;
            }
            else if (tagStack.empty())
            { 
                msg.appendf("Invalid concise XML: no matching tag for ']':\n");
                errP = q;
            }
            else
            {
                xml.appendN(indent, ' ');
                indent -= 2;
                xml.append("</").append(tagStack.back().c_str()).append(">\n");
                tagStack.pop_back();
                p=++q;
            }
            break;
        case '(':
            tag.clear().append( q-p, p).trim();
            //printf("tag: [%s]\n", tag);

            p = ++q;
            bValue = true;
            if (q==NULL) 
            { 
                msg.appendf("Invalid input: no ending '(' for <").append(tag).append('>'); 
                errP = q;
            }
            break;
        case ')':
            bValue = false;
            xml.appendN(indent+2, ' ');
            xml.append('<').append(tag).append('>');
            xml.append( q-p, p).append("</").append(tag).append(">\n");
            p = ++q;
            break;
        default:
            q++;
            break;
        }
    }

    if (errP)
    {
        msg.append(errP-concise+1, concise).append("<<ERROR<<");
        xml.clear();
    }
    return errP == NULL;
}


bool loadEspLog(const char* logFileName, HttpClient& httpClient, HttpStat& httpStat)
{
    if (!logFileName || !*logFileName)
    {
        OERRLOG("Input log file name not specified.");
        return false;
    }

    typedef std::map<std::string, int>  InstanceMap; /* counts instances of each method*/
    InstanceMap instanceMap; /*how many times each method is extracted */
    // how many instances of each method to extract:
    int maxInstances = httpClient.queryGlobals()->getPropInt("items", -1);
    if (maxInstances == 0)
    {
        OERRLOG("Maximum instances specified with -n option cannot be 0.");
        return false;
    }

    int bytes_read = 0;
    int fd = open(logFileName, O_RDONLY, S_IRWXU | S_IRWXG | S_IRWXO);
    Owned<BufferedLineReader> linereader = new BufferedLineReader(fd);

    if(fd >= 0)
    {
        StringBuffer buffer;
        StringBuffer xml;
        StringBuffer msg;
        StringBuffer service;
        StringBuffer method;
        StringBuffer request;

        static bool bMapNotLoaded = true;

        if (bMapNotLoaded)
        {
            LoadMethodMappings();
            bMapNotLoaded = false;
        }

        while ((bytes_read = linereader->readLine(buffer.clear())) > 0)
        {
            const char* buf = buffer.str();
            const char* p = strchr(buf, '\"');
            const char* prefix1 = "QUERY: ";
            const char* prefix2 = "FULL QUERY: ";
            bool bProcess = false;

            if (p && !strnicmp(p+1, prefix1, strlen(prefix1))) //QUERY: ...
            {
                p += strlen(prefix1)+1;

                if (expandConciseRequest(p, service.clear(), method.clear(), request.clear(), xml.clear(), msg.clear()))
                    bProcess = true;
                else
                    puts(msg.str());
            }
            else if (p && !strnicmp(p+1, prefix2, strlen(prefix2))) //FULL QUERY: ...
            {
                p += strlen(prefix2)+1;
                xml.clear().append(p);

                const char* q = p + xml.length() - 1;
                while (q > p && (*q == '\"' || *q == '\r' || *q == '\n'))
                    q--;
                xml.setLength( q-p+1 );//lose trailing double quote, CR and LF chars

                p = strchr(p, '<');
                if (p)
                {
                    q = strchr(++p, '>');
                    if (q)
                    {
                        method.clear().append(q-p, p);

                        //if the root name does not end with Request then append it
                        const char* z = strstr(method.str(), "Request");
                        if (!z)
                        {
                            StringBuffer config(method);
                            lookupMethod(config.str(), service.clear(), method, request);
                            StringBuffer tag(request);

                            xml.remove(1, q-p).insert(1, tag);//replace starting root tag
                            
                            //now find last tag and replace that as well
                            p = xml.str();
                            q = p + xml.length() - 1;

                            while (q > p && *q != '>')
                                q--;

                            p = q;
                            while (p > xml.str() && *p != '/')
                                p--;
                            xml.remove(p+1-xml.str(), q-p-1).insert(p+1-xml.str(), tag.str());
                        }
                        else
                        {
                            method.setLength(z-method.str());

                            StringBuffer config(method);
                            lookupMethod(config.str(), service.clear(), method, request);
                        }

                        bProcess = true;
                    }
                }
            }

            if (bProcess && maxInstances > 0)
            {
                InstanceMap::const_iterator it = instanceMap.find( method.str() );
                if (it == instanceMap.end())
                    instanceMap.insert( std::pair<std::string, int>(method.str(), 1) );
                else
                {
                    const int nInstances = (*it).second;
                    if (nInstances < maxInstances)
                        instanceMap[method.str()] = nInstances+1;
                    else
                        bProcess = false;
                }
            }


            if (bProcess)
            {
                xml.insert( 0, "<?xml version='1.0' encoding='UTF-8'?>\n"
                            "<soap:Envelope xmlns:soap='http://schemas.xmlsoap.org/soap/envelope/' \n"
                            "  xmlns:SOAP-ENC='http://schemas.xmlsoap.org/soap/encoding/' \n"
                            "  xmlns='urn:hpccsystems:ws:wsaccurint'>\n  <soap:Body>\n");
                xml.append("  </soap:Body>\n</soap:Envelope>\n");

                StringBuffer seqNum;
                p = strchr(buf, ' ');
                if (p)
                    seqNum.append(p-buf, buf);

                httpClient.addEspRequest(seqNum.str(), service.str(), method.str(), xml, httpStat);
            }
        }
    }

    struct stat st;
    bool rc = false;

    if(fd < 0)
        printf("File %s doesn't exist\n", logFileName);
    else
        if (stat(logFileName, &st) < 0)
            printf("stat error - %s\n", strerror(errno));
        else
            rc = true;

    return rc;
}

