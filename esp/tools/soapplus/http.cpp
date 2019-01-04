/*##############################################################################
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

#include "http.hpp"
#include "msggenerator.hpp"

#ifdef _WIN32
#include "winsock.h"
#define ERRNO() WSAGetLastError()
#ifndef EWOULDBLOCK
#define EWOULDBLOCK WSAEWOULDBLOCK
#endif
#else
#define ERRNO() (errno)
#define strnicmp strncasecmp
void Sleep(int msecs)
{
    usleep(msecs*1000);
}
#endif

#define STRESS_USE_JSOCKET 0

int http_tracelevel = 10;
bool loadEspLog(const char* logFileName, HttpClient& httpClient, HttpStat& httpStat);
void createDirectories(const char* outdir, const char* url, bool bClient, bool bServer, StringBuffer& outpath, StringBuffer& outpath1, 
                       StringBuffer& outpath2, StringBuffer& outpath3, StringBuffer& outpath4, StringBuffer& outpath5);

const char* sepstr = "\n---------------\n";

class CSocketChecker : implements IInterface, public CInterface
{
private:
    Owned<ISocket> m_socket;

public:
    IMPLEMENT_IINTERFACE;

    CSocketChecker(ISocket* sock)
    {
        m_socket.set(sock);
    }

    //0: normal (timeout or data available)
    //1: socket closed
    //<0: error
    int waitAndCheck(int waitmillisecs)
    {
        if(!m_socket || !m_socket->check_connection())
            return -1;
        int ret = m_socket->wait_read(waitmillisecs);
        if(ret <= 0)
            return ret;
        if(m_socket->avail_read() == 0)
            return 1;
        else
            return 0;
    }
};

HttpStat::HttpStat()
{
    threads = 1;
    duration = 0;
    totaltime = 0;
    slowest = 0;
    fastest = 2147483647;
    totalreqlen = 0;
    totalresplen = 0;
    numrequests = 0;
    numfails = 0;
}

void HttpStat::printStat(FILE* ofile)
{
    if(ofile)
    {
        fprintf(ofile, "%s", sepstr);
        fprintf(ofile, "Number of Threads:                %d\n", threads);
        fprintf(ofile, "Total hits:                       %" I64F "d\n", numrequests);
        fprintf(ofile, "Total fails:                      %" I64F "d\n", numfails);
        fprintf(ofile, "Run length(millisecond):          %d\n", duration);
        if(duration > 0)
            fprintf(ofile, "Hits per second:                  %3.1f\n", numrequests/(duration*0.001));
        fprintf(ofile, "Total data sent:                  %" I64F "d\n", totalreqlen);
        fprintf(ofile, "Total data received:              %" I64F "d\n", totalresplen);
        __int64 totallen = totalreqlen + totalresplen;
        fprintf(ofile, "Total data transferred:           %" I64F "d\n", totallen);
        if(duration > 0)
            fprintf(ofile, "Data transferred per second:      %5.1f\n", totallen/(duration*0.001));
        if(numrequests > 0)
        {
            fprintf(ofile, "Slowest round trip(millisecond):  %d\n", slowest);
            fprintf(ofile, "Fastest round trip(millisecond):  %d\n", fastest);
            fprintf(ofile, "Average round trip(millisecond):  %" I64F "d\n", totaltime/numrequests);
        }

        if(http_tracelevel >= 5 && ofile==stdout && isatty(1))
        {
            fprintf(ofile, "WARNING: YOU ARE PRINTING OUT A LOT OF TRACING, WHICH COULD ADVERSELY AFFECT THE PERFORMANCE. TO GET MORE ACCURATE STATS, LOWER THE TRACELEVEL OR REDIRECT TO A FILE.\n");
        }
        fprintf(ofile, "%s", sepstr);
    }
}

__int64 Http::receiveData(ISocket* socket, IByteOutputStream* ostream, bool isClientSide, bool& isRoxie, const char* headersToRemove, IFileIO* full_output, IFileIO* content_output, bool alwayshttp)
{
    __int64 totalresplen = 0;
    if(ostream == NULL)
        return -1;

    Owned<IBufferedSocket> bsocket = createBufferedSocket(socket);
    if(bsocket == NULL)
    {
        printf("Can't create buffered socket\n");
        return -1;
    }

    //bsocket->setReadTimeout(1);

    Owned<IFileIOStream> full_stream = NULL;
    if(full_output)
        full_stream.setown(createIOStream(full_output));
    Owned<IFileIOStream> content_stream = NULL;
    if(content_output)
        content_stream.setown(createIOStream(content_output));

    char oneline[2049];
    bsocket->read(oneline, 4);
    if(alwayshttp)
        isRoxie = false;
    else if(strncmp(oneline, "GET", 3) == 0 || strncmp(oneline, "POST", 4) == 0 || strncmp(oneline, "HTTP", 4) == 0)
        isRoxie = false;
    else
        isRoxie = true;
    
    if(isRoxie)
    {
        unsigned int len;
        memcpy((void*)&len, oneline, 4);
        _WINREV(len);
        if (len & 0x80000000)
            len ^= 0x80000000;
        while(len > 0)
        {
            int lenToRead = (len > 2048)?2048:len;

            unsigned bytesRead = bsocket->read(oneline, lenToRead);
            if(bytesRead > 0)
            {
                totalresplen += bytesRead;
                len -= bytesRead;
                ostream->writeBytes(oneline, bytesRead);
                if(full_stream.get())
                    full_stream->write(bytesRead, oneline);
                if(content_stream.get())
                    content_stream->write(bytesRead, oneline);
            }
            else
                break;
        }

        return totalresplen;
    }
    else
    {
        int lenread = 4 + bsocket->readline(oneline + 4, 2044, true, NULL);

        bool alwaysReadContent = false;
        bool isRedirect = false;
        StringBuffer location;
        StringBuffer transfer_encoding;
        if(isClientSide && lenread >= 8)
        {
            int status = atoi(oneline+9);
            
            if (status == 100)
            {
                //read empty line
                lenread = bsocket->readline(oneline, 2048, true, NULL);
                totalresplen += lenread;
                
                if(full_stream.get())
                    full_stream->write(lenread, oneline);

                //read next http response
                lenread = bsocket->readline(oneline, 2048, true, NULL);

                if(lenread >= 8)
                    status = atoi(oneline+9);
            }
            
            if(status == 200 || status == 206 || status==500)
                alwaysReadContent = true;
            if((status / 100) == 3 && status != 304)
                isRedirect = true;
            
        }

        int content_length = 0;
        while(lenread >= 0 && oneline[0] != '\0' && oneline[0] != '\r' && oneline[0] != '\n')
        {
            totalresplen += lenread;
            
            if(full_stream.get())
                full_stream->write(lenread, oneline);

            if(headersToRemove !=  NULL)
            {
                const char* ptr = oneline;
                while(*ptr && *ptr != ':' && *ptr != ' ')
                    ptr++;
                StringBuffer curheader;
                curheader.append(ptr - oneline, oneline);
                curheader.append(" ");
                if(!strstr(curheader.str(), headersToRemove))
                    ostream->writeBytes(oneline, lenread);
            }
            else        
                ostream->writeBytes(oneline, lenread);

            if(strncmp(oneline, "Content-Length:", 15) == 0)
            {
                content_length = atoi(oneline + 16);
            }
            else if(isRedirect && strncmp(oneline, "Location:", 9) == 0)
            {
                location.append(oneline+10);
            }
            else if(strncmp(oneline, "Transfer-Encoding:", 18) == 0)
            {
                transfer_encoding.append(oneline+19);
                transfer_encoding.trim();
            }

            lenread = bsocket->readline(oneline, 2048, true, NULL);
        }

        if(oneline[0] == '\r' || oneline[0] == '\n')
        {
            if(full_stream.get())
                full_stream->write(lenread, oneline);
            ostream->writeBytes(oneline, lenread);

            totalresplen += lenread;
        }

        if(isRedirect)
        {
            throw MakeStringException(-1, "The original url is redirected to %s, please change your url and try again.", location.str());
        }

        if(content_length > 0)
        {
            char buf[1024 + 1];
            int buflen = 1024;
            int totallen = content_length;
            if(buflen > totallen)
                buflen = totallen;
            int readlen = 0;    
            for(;;)
            {
                readlen = bsocket->read(buf, buflen);
                if(readlen < 0)
                {
                    DBGLOG(">> socket read error %d", readlen);
                    break;
                }
                if(readlen == 0)
                    break;
                buf[buflen] = 0;
                totalresplen += readlen;

                ostream->writeBytes(buf, readlen);
                if(full_stream.get())
                    full_stream->write(readlen, buf);
                if(content_stream.get())
                    content_stream->write(readlen, buf);
                
                totallen -= readlen;
                if(totallen <= 0)
                    break;
                if(buflen > totallen)
                    buflen = totallen;
            }
        }
        else if(alwaysReadContent)
        {
            char buf[1024 + 1];
            int buflen = 1024;
            int readlen = 0;

            if(!stricmp(transfer_encoding.str(), "chunked"))//Transfer-Encoding: chunked
            {
                for(;;)
                {
                    int readlen = bsocket->readline(buf, buflen, true, NULL);
                    if(readlen <= 0)
                        break;
                    buf[readlen] = 0;
                    ostream->writeBytes(buf, readlen);
                    if(full_stream.get())
                        full_stream->write(readlen, buf);
                    int chunkSize;
                    sscanf(buf, "%x", &chunkSize);

                    if (chunkSize<=0)
                        break;

                    while (chunkSize > 0)
                    {
                        const int len = (chunkSize<=buflen)?chunkSize:buflen;
                        readlen = bsocket->read(buf, len);
                        if(readlen <= 0)
                            break;
                        chunkSize -= readlen;
                        buf[readlen] = 0;
                        ostream->writeBytes(buf, readlen);
                        if(full_stream.get())
                            full_stream->write(readlen, buf);
                        if(content_stream.get())
                            content_stream->write(readlen, buf);
                    }

                    readlen = bsocket->read(buf, 2); //CR/LF
                    if (readlen <= 0)
                        break;
                    buf[readlen] = 0;
                    ostream->writeBytes(buf, readlen);
                    if(full_stream.get())
                        full_stream->write(readlen, buf);
                    if(content_stream.get())
                        content_stream->write(readlen, buf);
                }
            }
            else
            {
                for(;;)
                {
                    readlen = bsocket->read(buf, buflen);
                    if(readlen < 0)
                    {
                        DBGLOG(">> socket read error %d", readlen);
                        break;
                    }
                    if(readlen == 0)
                        break;
                    totalresplen += readlen;
                    buf[readlen] = 0;

                    ostream->writeBytes(buf, readlen);
                    if(full_stream.get())
                        full_stream->write(readlen, buf);
                    if(content_stream.get())
                        content_stream->write(readlen, buf);
                }
            }
        }
    }

    if(full_stream.get())
        full_stream->flush();
    if(content_stream.get())
        content_stream->flush();

    return totalresplen;
}

void Http::SplitURL(const char* url, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password,StringBuffer& host, StringBuffer& port, StringBuffer& path)
{
    int protlen = 0;
    if(!url || strlen(url) <= 7)
        throw MakeStringException(-1, "Invalid URL %s", url);
    else if(strncmp(url, "HTTP://", 7) == 0 || strncmp(url, "http://", 7) == 0)
    {
        protocol.append("HTTP");
        protlen = 7;
    }
    else if(strncmp(url, "HTTPS://", 8) == 0 || strncmp(url, "https://", 8) == 0)
    {
        protocol.append("HTTPS");
        protlen = 8;
    }
    else
    {
        protocol.append("HTTP");
        protlen = 0;
    }

    char buf[URL_MAX+1];
    int len = strlen(url);
    if(len > URL_MAX)
        len = URL_MAX;
    strncpy(buf, url, len);
    buf[len] = 0;

    char* hostptr;
    char *username = NULL;
    char* atsign = strrchr(buf, '@'); 
    if(atsign)
    {
        username = buf + protlen;
        hostptr = atsign + 1;
        *atsign = '\0';
    }
    else
    {
        hostptr = buf + protlen;
    }

    char* pathptr = strchr(hostptr, '/');
    if(pathptr)
    {
        *pathptr = 0;
        pathptr++;
    }
    char* portptr = strchr(hostptr, ':');
    if(portptr)
    {
        *portptr = 0;
        portptr++;
    }

    if(username)
    {
        char* semicln = strchr(username, ':');
        if(semicln)
        {
            Password.append(semicln+1);
            *semicln = '\0';
        }
        UserName.append(username);
    }

    if(hostptr)
        host.append(hostptr);

    if(portptr)
        port.append(portptr);

    path.append("/");
    if(pathptr)
        path.append(pathptr);
        
}

HttpClient::HttpClient(IProperties* globals, const char* url, const char* inname, 
                       const char* outdir, const char* outfilename, bool writeToFiles,
                       int doValidation, const char* xsdpath, bool isEspLogFile) : m_stopstress(false)
{
    m_globals = globals;
    if(url && *url)
        m_url.append(url);
    if(inname)
        m_inname.append(inname);
    if(outdir)
        m_outdir.append(outdir);
    if(outfilename)
        m_outfilename.append(outfilename);


    if(url && *url)
    {
        StringBuffer portbuf;
        m_host.clear();
        Http::SplitURL(url, m_protocol, m_user, m_password, m_host, portbuf, m_path);

        if(portbuf.length() > 0)
            m_port = atoi(portbuf.str());
        else
        {
            if(m_protocol.length() > 0 && stricmp(m_protocol.str(), "https") == 0)
                m_port = 443;
            else
                m_port = 80;
        }

        if(stricmp(m_protocol.str(), "HTTPS") == 0)
        {
#ifdef _USE_OPENSSL
            if(m_ssctx.get() == NULL)
                m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpClient: failure to create SSL socket - OpenSSL not enabled in build");
#endif
        }

        if(m_user.length() > 0)
        {
            StringBuffer auth, abuf;
            abuf.appendf("%s:%s", m_user.str(), m_password.str());
            JBASE64_Encode(abuf.str(), abuf.length(), auth);
            m_authheader.appendf("Authorization: Basic %s\r\n", auth.str());
        }
    }

    // For now, log to stdout
    m_logfile = stdout;

    m_writeToFiles = writeToFiles;

    m_doValidation = doValidation;
    if(xsdpath !=  NULL)
        m_xsdpath.append(xsdpath);

    if(globals && globals->hasProp("stressduration"))
    {
        m_stressthreads = globals->getPropInt("stressthreads", 0);
        m_stressduration = globals->getPropInt("stressduration", 0);
    }
    else
    {
        m_stressthreads = 0;
        m_stressduration = 0;
    }

    m_doStress = (m_stressthreads > 0) && (m_stressduration > 0);
    m_isEspLogFile = isEspLogFile;
}

void HttpClient::start()
{
    Owned<IFile> infile = NULL;
    bool no_sending = false;
    Owned<HttpStat> overall_stat = new HttpStat;

    Owned<IDirectoryIterator> di;
    if(m_inname.length() > 0)
    {
        infile.setown(createIFile(m_inname.str()));
        if(!infile->exists())
        {
            if(strstr(m_inname.str(), "*") != NULL)
            {
                StringBuffer dir;
                const char* mask;
                const char* sep = strrchr(m_inname.str(), PATHSEPCHAR);
                if(sep)
                {
                    dir.append( sep - m_inname.str() + 1, m_inname.str());
                    mask = sep + 1;
                }
                else
                {
                    dir.append(".");
                    mask = m_inname.str();
                }   

                di.setown(createDirectoryIterator(dir.str(), mask));
            }
            else
            {
                fprintf(m_logfile, "Input file/directory %s doesn't exist", m_inname.str());
                return;
            }
        }
        else if(infile->isDirectory())
            di.setown(infile->directoryFiles());
    }

    if(m_doValidation == 1)
    {
        if(m_xsdpath.length() == 0)
        {
            m_xsdpath.append(m_url.trim().str());
            m_xsdpath.append( strchr(m_url.str(), '?') ? '&' : '?');
            m_xsdpath.append("xsd");
        }
        
        if(http_tracelevel >= 5)
            fprintf(m_logfile, "Loading xsd from %s\n", m_xsdpath.str());

        if(strnicmp(m_xsdpath.str(), "http:", 5) == 0 || strnicmp(m_xsdpath.str(), "https:", 6) == 0)
        {
            Owned<HttpClient> xsdclient = new HttpClient(NULL, m_xsdpath.str());
            StringBuffer requestbuf;
            xsdclient->generateGetRequest(requestbuf);
            xsdclient->sendRequest(requestbuf, NULL, NULL, NULL, &m_xsd);
            const char* ptr = m_xsd.str();
            if(ptr)
            {
                ptr = strchr(ptr, '<');
                if(!ptr)
                {
                    if(http_tracelevel > 0)
                        fprintf(m_logfile, "The xsd is not valid xml\n");
                    return;
                }
                else
                    m_xsd.remove(0, ptr - m_xsd.str());
            }
        }
        else
        {
            m_xsd.loadFile(m_xsdpath.str());
            if(http_tracelevel >= 10)
                fprintf(m_logfile, "Loaded xsd:\n%s\n", m_xsd.str());
        }

        if(m_xsd.length() == 0)
        {
            fprintf(m_logfile, "xsd is empty\n");
            return;
        }
    }

    bool autogen = m_globals ? m_globals->getPropBool("autogen", false) : false;

    if(autogen && !m_isEspLogFile)
    {
        if(m_url.length() == 0)
            no_sending = true;

        StringBuffer schemapath;
        MessageGenerator::SchemaType st = MessageGenerator::WSDL;
        m_globals->getProp("wsdl", schemapath);
        if(schemapath.length() == 0)
        {
            if(m_xsdpath.length() > 0)
            {
                schemapath.append(m_xsdpath.str());
                st = MessageGenerator::XSD;
            }
            else
                schemapath.append(m_url.str()).append("?wsdl");
        }

        MessageGenerator mg(schemapath.str(), no_sending, st, m_globals);
        StringBuffer templatemsg;
        if(infile.get() && !infile->isDirectory())
            templatemsg.loadFile(m_inname.str());

        const char* method = m_globals->queryProp("method");
        if(method && *method)
        {
            Owned<CRequest> creq = new CRequest(method);
            StringBuffer& message = creq->queryReqbuf();
            mg.generateMessage(method, templatemsg.length()>0?templatemsg.str():NULL, message);
            if(!no_sending)
            {
                if(m_doStress)
                    m_stressrequests.append(*creq.getLink());
                else
                    sendRequest(message, method, overall_stat.get());
            }
        }
        else
        {
            StringArray& methods = mg.getAllMethods();
            fprintf(stderr, "There are %d methods defined in this wsdl:\n", methods.ordinality());
            ForEachItemIn(i, methods)
            {
                fprintf(stderr, "%d:\t%s\n", i, methods.item(i));
            }
            int ind = 0;
            char seqbuf[20];
            if(!(m_globals && m_globals->getPropBool("useDefault")))
            {
                fprintf(stderr, "Pick one method, or just press enter to generate a request for each method:\n");
                if (fgets(seqbuf, 19, stdin)) {
                    while(ind < 19 && seqbuf[ind] != '\0' && isdigit(seqbuf[ind]))
                        ind++;
                    seqbuf[ind] = 0;
                }
            }
            if(ind > 0)
            {
                int seq = atoi(seqbuf);
                if(seq < 0 || seq > methods.ordinality())
                {
                    fprintf(stderr, "You entered wrong number\n");
                }
                else
                {
                    Owned<CRequest> creq = new CRequest(methods.item(seq));
                    StringBuffer& message = creq->queryReqbuf();
                    mg.generateMessage(methods.item(seq), templatemsg.length()>0?templatemsg.str():NULL, message);
                    if(!no_sending)
                    {
                        if(m_doStress)
                            m_stressrequests.append(*creq.getLink());
                        else
                            sendRequest(message, methods.item(seq), overall_stat.get());
                    }
                }
            }
            else
            {
                ForEachItemIn(x, methods)
                {
                    const char* method = methods.item(x);
                    Owned<CRequest> creq = new CRequest(method);
                    StringBuffer& message = creq->queryReqbuf();
                    mg.generateMessage(method, NULL, message);
                    if(!no_sending)
                    {
                        if(m_doStress)
                            m_stressrequests.append(*creq.getLink());
                        else
                            sendRequest(message, method, overall_stat.get());
                    }
                }
            }
        }
    }
    else if(autogen && m_isEspLogFile)
    {
        if (m_url.length() == 0)
            no_sending = true;

        if (infile.get() && !infile->isDirectory())
            loadEspLog(m_inname.str(), *this, *overall_stat.get());
    }
    else if(di.get())
    {
        if(m_doStress)
            fprintf(m_logfile, "reading in request files ...\n");
        int filenum = 0;
        ForEach(*di)
        {
            IFile &file = di->query();
            if (file.isFile() && file.size() > 0)
            {
                const char* fname = file.queryFilename();
                if(fname && *fname)
                {
                    Owned<CRequest> creq = new CRequest(fname);
                    StringBuffer & message = creq->queryReqbuf();
                    message.loadFile(&file);
                    if(m_doStress)
                    {
                        filenum++;
                        if(filenum % 1000 == 0)
                            fprintf(m_logfile, "%d requests read in\n", filenum);

                        m_stressrequests.append(*creq.getLink());
                    }
                    else
                        sendRequest(message, fname, overall_stat.get());
                }
            }
        }
        if(m_doStress && filenum % 1000 != 0)
        {
            fprintf(m_logfile, "%d requests read in\n", filenum);
        }

    }   
    else if(infile.get() != NULL)
    {
        if(infile->size() > 0)
        {
            if(m_url.length() > 0)
            {
                Owned<CRequest> creq = new CRequest(infile->queryFilename());
                StringBuffer & message = creq->queryReqbuf();
                message.loadFile(infile.get());
                if(m_doStress)
                    m_stressrequests.append(*creq.getLink());
                else
                    sendRequest(message, infile->queryFilename(), overall_stat.get());
            }
            else if(m_doValidation)
            {
                StringBuffer inbuf;
                inbuf.loadFile(infile->queryFilename(), true);
                int ret = validate(inbuf);
                if(http_tracelevel > 0 && m_doValidation == 1)
                {
                    if(ret == 0)
                    {
                        fprintf(m_logfile, "\n%sSuccessfully validated the response against the xsd%s\n", sepstr, sepstr);
                    }
                    else
                    {
                        fprintf(m_logfile, "Error: Validation against the xsd failed.\n");
                    }
                }
            }
        }
        else
            fprintf(m_logfile, "input file %s is empty\n", m_inname.str());
    }
    else
    {
        Owned<CRequest> creq = new CRequest(m_outfilename.str());
        StringBuffer & message = creq->queryReqbuf();
        generateGetRequest(message);
        if(m_doStress)
            m_stressrequests.append(*creq.getLink());
        else
            sendRequest(message, m_outfilename.str(), overall_stat.get());
    }

    if(m_doStress && !no_sending && m_stressrequests.length() > 0)
    {
        sendStressRequests(overall_stat.get());
    }

    if(http_tracelevel > 0)
    {
        overall_stat->printStat(m_logfile);
    }
}


void HttpClient::addEspRequest(const char* requestId, const char* service, const char* method, StringBuffer& request, HttpStat& httpStat)
{
    if (m_url.length() > 0)
    {
        if (m_doStress)
        {
            StringBuffer fname;
            fname.append(service).append('_').append(method);
            if (requestId)
                fname.append('_').append(requestId);

            Owned<CRequest> creq = new CRequest(method);
            StringBuffer& message = creq->queryReqbuf();
            m_stressrequests.append(*creq.getLink());
        }
        else
        {
            StringBuffer fname(service);
            fname.append(PATHSEPCHAR);

            fname.append(method);
            if (requestId)
                fname.append('_').append(requestId);
            fname.append(".xml");
            sendRequest(request, fname, &httpStat);
        }
    }
    else if(m_writeToFiles)
    {
        extern FILE* logfile;

        StringBuffer outpath, outpath1, outpath2, outpath3, outpath4, outpath5;
        if (m_writeToFiles)
        {
            StringBuffer outpath, outpath1, outpath2, outpath3, outpath4, outpath5;

            StringBuffer outdir;
            outdir.append(m_outdir.str()).append(service).append(PATHSEPCHAR);

            createDirectories(outdir.str(), m_url.str(), true, false, outpath, outpath1, outpath2, outpath3, outpath4, outpath5);
        }

        StringBuffer fname(m_outdir.str());
        fname.append(service).append(PATHSEPCHAR);

        fname.append("request").append(PATHSEPCHAR).append(method);
        if (requestId)
            fname.append('_').append(requestId);
        fname.append(".xml");
        Owned<IFile> file = createIFile(fname.str());
        Owned<IFileIO> io;
        io.setown(file->open(IFOcreaterw));

        if (io.get())
            io->write(0, request.length(), request.str());
        else
            fprintf(m_logfile, "file %s can't be created", file->queryFilename());
    }
}

class CSimpleSocket;

class CHttpStressThread : public Thread
{
private:
    Owned<HttpStat> m_stat;
    HttpClient* m_client;

public:
    CHttpStressThread(HttpClient* client): m_client(client)
    {
        m_stat.setown(new HttpStat());
    }

    virtual int run()
    {
        IProperties* globals = m_client->queryGlobals();
        int delaymin = 0, delaymax = 0;
        if(globals && globals->hasProp("delaymin"))
        {
            delaymin = atoi(globals->queryProp("delaymin"));
            delaymax = atoi(globals->queryProp("delaymax"));
        }

        Owned<CSimpleSocket> persistent_socket = nullptr;
        if(m_client)
        {
            for(;;)
            {
                IArrayOf<CRequest>& requests = m_client->queryStressRequests();
                ForEachItemIn(x, requests)
                {
                    if(m_client->queryStopStress())
                        break;

                    CRequest& req = requests.item(x);
                    if(STRESS_USE_JSOCKET)
                        m_client->sendRequest(req.queryReqbuf(), req.getName(), m_stat.get());
                    else
                        m_client->sendStressRequest(req.queryReqbuf(), m_stat.get(), persistent_socket);
                    if(delaymax > 0 && !m_client->queryStopStress())
                    {
                        int delay = 0;
                        if(delaymin < delaymax)
                        {
                            delay = delaymin + (fastRand() % (delaymax - delaymin));
                        }
                        else
                        {
                            delay = delaymin;
                        }

                        Sleep(delay);
                    }
                }
                if(m_client->queryStopStress())
                    break;
            }
        }

        return 0;
    }

    HttpStat* queryStat()
    {
        return m_stat.get();
    }
};

int HttpClient::sendStressRequests(HttpStat* overall_stat)
{
    if(m_stressrequests.length() == 0)
    {
        fprintf(m_logfile, "No requests to send. Please check your input.\n");
        return 0;
    }

    if(m_stressthreads <= 0 || m_stressduration <= 0)
        return 0;

    ForEachItemIn(x, m_stressrequests)
    {
        CRequest& req = m_stressrequests.item(x);
        insertSoapHeaders(req.queryReqbuf());
    }

    /*
    fprintf(m_logfile, "Validating the url and requests...\n");
    int orig_tracelevel = http_tracelevel;
    http_tracelevel = 11;
    ForEachItemIn(x, m_stressrequests)
    {
        CRequest& req = m_stressrequests.item(x);
        sendRequest(req.queryReqbuf(), req.getName(), NULL);
    }
    http_tracelevel = orig_tracelevel;
    if(!(m_globals && m_globals->getPropBool("useDefault")))
    {
        printf("Please check the responses. Are you willing to start stress test? [y/n]\n");
        char c = getchar();
        if(c == 'N' || c == 'n')
        {
            return 0;
        }
    }
    */

    m_stopstress = false;


    if(m_serveraddr.get() == NULL)
        m_serveraddr.setown(new CAddress(m_host.str(), m_port));

    time_t t;
    time(&t);
    srand((unsigned) t);

    CHttpStressThread** thrdlist = new CHttpStressThread*[m_stressthreads];
    int i;
    for(i = 0; i < m_stressthreads; i++)
        thrdlist[i] = new CHttpStressThread(this);

    //__int64 start = msTick();
    for(i = 0; i < m_stressthreads; i++)
        thrdlist[i]->start();

    if(http_tracelevel > 0)
        fprintf(m_logfile, "Started %d stress test threads.\n", m_stressthreads);

    if(http_tracelevel > 0)
        fprintf(m_logfile, "Running the test for %d seconds...\n", m_stressduration);
    
    sleep(m_stressduration);
    m_stopstress = true;

    for(i = 0; i < m_stressthreads; i++)
        thrdlist[i]->join();

    //__int64 end = msTick();
    
    if(http_tracelevel > 0)
        fprintf(m_logfile, "All %d stresstest threads finished.\n", m_stressthreads);

    overall_stat->duration = m_stressduration*1000;
    overall_stat->threads = m_stressthreads;
    for(i = 0; i < m_stressthreads; i++)
    {
        CHttpStressThread* thrd = thrdlist[i];
        HttpStat* stat = thrd->queryStat();
        if(stat)
        {
            overall_stat->totaltime += stat->totaltime;
            overall_stat->totalreqlen += stat->totalreqlen;
            overall_stat->totalresplen += stat->totalresplen;
            overall_stat->numrequests += stat->numrequests;
            overall_stat->numfails += stat->numfails;
            if(overall_stat->fastest > stat->fastest)
                overall_stat->fastest = stat->fastest;
            if(overall_stat->slowest < stat->slowest)
                overall_stat->slowest = stat->slowest;
        }
    }
    delete [] thrdlist;

    return 0;
}

#ifdef _WIN32
void GetLastErrorAndMessage(char* buf)
{
    LPVOID lpMsgBuf;
    DWORD dw = GetLastError();

    FormatMessage(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | 
        FORMAT_MESSAGE_FROM_SYSTEM,
        NULL,
        dw,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPTSTR) &lpMsgBuf,
        0, NULL );

    wsprintf(buf, "%d: %s", dw, lpMsgBuf); 
 
    LocalFree(lpMsgBuf);
}
#else
void GetLastErrorAndMessage(char* buf)
{
    sprintf(buf, "%d: %s", errno, strerror(errno));
}
#endif


class CSimpleSocket : public CInterface, implements IInterface
{
    ISecureSocketContext* m_ssctx;
    bool m_isSSL;
    FILE* m_logfile;
    int m_sockfd;
    Owned<ISecureSocket> m_securesocket;
    bool m_connected;

public:
    IMPLEMENT_IINTERFACE;

    CSimpleSocket(ISecureSocketContext* ssctx, FILE* logfile) : m_connected (false)
    {
        m_ssctx = ssctx;
        if(ssctx)
            m_isSSL = true;
        else
            m_isSSL = false;
        m_logfile = logfile;
        m_sockfd = -1;
    }

    virtual ~CSimpleSocket()
    {
        if(m_sockfd > 0)
            ::closesocket(m_sockfd);
    }

    int connect(CAddress* address)
    {
        if(!address)
            return -1;

        m_sockfd = socket(AF_INET, SOCK_STREAM, 0);
        int ret = ::connect(m_sockfd, (struct sockaddr *)(address->m_addr), sizeof(*(address->m_addr)));
        if(ret < 0)
        {
            char errbuf[512];
            GetLastErrorAndMessage(errbuf);
            fprintf(m_logfile, "Error: failed to connect to %s:%d - %s", address->m_ip.str(), address->m_port, errbuf);
            return -1;
        }
        
        if(m_ssctx != NULL)
        {
            m_securesocket.setown(m_ssctx->createSecureSocket(m_sockfd));
            int res = m_securesocket->secure_connect();
            if(res < 0)
            {
                fprintf(m_logfile, "Error: failed to establish ssl connection\n");
                return -1;
            }
        }
        m_connected = true;

        return 0;
    }

    ssize_t readn(int fd, void *vptr, size_t min, size_t n)
    {
        if (min > n)
            return 0;

        size_t  nleft;
        ssize_t nread;
        char   *ptr;

        ptr = (char *)vptr;
        nleft = n;
        while (nleft > 0)
        {
            if ((nread = ::recv(fd, ptr, nleft, 0)) < 0)
            {
                if (errno == EINTR)
                {
                    nread = 0;      /* and call read() again */
                    continue;
                }
                else
                {
                    close();
                    return (-1);
                }
            }
            else if (nread == 0)
            {
                close();
                break;              /* EOF */
            }

            nleft -= nread;
            ptr += nread;
            if (n - nleft >= min)
                break;
        }
        return (n - nleft);         /* return >= 0 */
    }

    ssize_t readn(int fd, void *vptr, size_t n)
    {
        return readn(fd, vptr, n, n);
    }

    ssize_t writen(int fd, const void *vptr, size_t n)
    {
        size_t nleft;
        ssize_t nwritten;
        const char *ptr;

        ptr = (char *)vptr;
        nleft = n;
        while (nleft > 0)
        {
            if ( (nwritten = ::send(fd, ptr, nleft, 0)) <= 0)
            {
                if (nwritten < 0 && errno == EINTR)
                    nwritten = 0;   /* and call write() again */
                else
                {
                    close();
                    return (-1);    /* error */
                }
            }

            nleft -= nwritten;
            ptr += nwritten;
        }
        return (n);
    }

    int send(StringBuffer& data)
    {
        int sent = 0;
        if(!m_isSSL)
        {
            // sent = ::send(m_sockfd, data.str(), data.length(), 0);
            sent = writen(m_sockfd, data.str(), data.length());
        }
        else
            sent = m_securesocket->write(data.str(), data.length());
        
        return sent;
    }

    int receive(char* buf, int buflen)
    {
        return receive(buf, buflen, buflen);
    }

    int receive(char* buf, int min, int buflen)
    {
        if (min > buflen)
            return 0;

        unsigned int len = 0;
        if (!m_isSSL)
        {
            //len = ::recv(m_sockfd, buf, buflen, 0);
            len = readn(m_sockfd, buf, min, buflen);
        }
        else
        {
            m_securesocket->read(buf, min, buflen, len, WAIT_FOREVER);
            if(len <= 0)
                close();
        }

        return len;
    }

    void close()
    {
        if(m_sockfd > 0)
        {
            ::closesocket(m_sockfd);
            m_sockfd = -1;
        }
        m_connected = false;
    }

    bool isConnected()
    {
        return m_connected;
    }
};

int HttpClient::sendStressRequest(StringBuffer& request, HttpStat* stat, Owned<CSimpleSocket>& persistentSocket)
{
    if(request.length() == 0)
    {
        fprintf(m_logfile, "Request is empty\n");
        return 0;
    }

    if(http_tracelevel >= 10)
        fprintf(m_logfile, "%s%s%s", sepstr, request.str(), sepstr);

    unsigned start = msTick();

    Owned<CSimpleSocket> sock;
    bool isPersistent = m_globals->getPropBool("isPersist", false);
    if (isPersistent)
    {
        if (persistentSocket.get() == nullptr)
            persistentSocket.setown(new CSimpleSocket(m_ssctx.get(), m_logfile));
        sock.set(persistentSocket.get());
    }
    else
        sock.setown(new CSimpleSocket(m_ssctx.get(), m_logfile));
    if (!sock->isConnected())
    {
        int ret = sock->connect(m_serveraddr.get());
        if (ret < 0)
        {
            if (stat)
                stat->numfails++;
            return -1;
        }
    }

    bool shouldClose = false;
    int sent = sock->send(request);
    __int64 total_len = 0;
    StringBuffer xml;
    if (sent > 0)
    {
        int len = 0;
        char recvbuf[2048];
        if (!isPersistent)
        {
            while (1)
            {
                len = sock->receive(recvbuf, 2047);
                if (len > 0)
                {
                    total_len += len;
                    recvbuf[len] = 0;
                    if (m_doValidation)
                        xml.append(len, recvbuf);
                    if (http_tracelevel >= 10)
                        fprintf(m_logfile, "%s", recvbuf);
                }
                else
                {
                    if (total_len == 0)
                    {
                        if (stat)
                            stat->numfails++;
                        return -1;
                    }
                    break;
                }
            }
        }
        else
        {
            StringBuffer headersbuf;
            unsigned int searchStart = 0;
            while (1)
            {
                len = sock->receive(recvbuf, 1, 2047);
                if (len > 0)
                {
                    total_len += len;
                    recvbuf[len] = 0;
                    if (http_tracelevel >= 10)
                        fprintf(m_logfile, "%s", recvbuf);
                    headersbuf.append(recvbuf);
                    char* endofheaders = strstr((char*)(headersbuf.str()+searchStart), "\r\n\r\n");
                    searchStart += len>3?(len-3):0;
                    if (endofheaders != nullptr)
                    {
                        int conlen = 0;
                        const char* conlenstr = strstr((char*)headersbuf.str(), "Content-Length:");
                        if (conlenstr != nullptr)
                            conlen = atoi(conlenstr+15);
                        else
                            shouldClose = true;
                        if (conlen > 0)
                        {
                            int content_read = headersbuf.length() - (endofheaders+4 - headersbuf.str());
                            if (m_doValidation && content_read > 0)
                                xml.append(content_read, endofheaders+4);
                            while (content_read < conlen)
                            {
                                int remaining = conlen - content_read;
                                len = sock->receive(recvbuf, 2047>remaining?remaining:2047);
                                if (len > 0)
                                {
                                    content_read += len;
                                    total_len += len;
                                    recvbuf[len] = 0;
                                    if (m_doValidation)
                                        xml.append(len, recvbuf);
                                    if (http_tracelevel >= 10)
                                        fprintf(m_logfile, "%s", recvbuf);
                                }
                                else
                                {
                                    sock->close();
                                    if (stat)
                                        stat->numfails++;
                                    return -1;
                                }
                            }
                        }
                        break;
                    }
                    else if (total_len >= 1000000) // Still haven't reached end of headers
                    {
                        fprintf(m_logfile, "HTTP headers too long.\n");
                        shouldClose = true;
                        break;
                    }
                }
                else
                {
                    sock->close();
                    if (stat && len < 0)
                        stat->numfails++;
                    return -1;
                }
            }
        }
        if (m_doValidation && xml.length() > 0)
            validate(xml);
    }
    else
    {
        fprintf(m_logfile, "Failed to send request.\n");
        sock->close();
        if (stat)
            stat->numfails++;
        return -1;
    }

    if (!isPersistent || shouldClose)
        sock->close();

    unsigned end = msTick();
    int duration = end - start;

    if(http_tracelevel >= 5)
        fprintf(m_logfile, "Roundtrip Time (milli-second): %d, Bytes Sent: %d, Bytes Received: %" I64F "d\n", duration, sent, total_len);

    if(stat)
    {
        stat->duration += duration;
        stat->totaltime += duration;
        stat->numrequests += 1;
        if(duration > stat->slowest)
            stat->slowest = duration;
        if(duration < stat->fastest)
            stat->fastest = duration;
        stat->totalreqlen += sent;
        stat->totalresplen += total_len;
    }

    return 0;
}

int HttpClient::sendRequest(StringBuffer& request, const char* fname, HttpStat* stat)
{
    Owned<IFileIO> io_o1 = NULL;
    Owned<IFileIO> io_o2 = NULL;
    Owned<IFileIO> io_o3 = NULL;

    if(m_writeToFiles)
    {
        const char* lastslash = strrchr(fname, PATHSEPCHAR);
        if(lastslash)
            fname = lastslash + 1;

        StringBuffer fname1(m_outdir.str());
        fname1.append("request").append(PATHSEPCHAR);
        fname1.append(fname);
        Owned<IFile> file1 = createIFile(fname1.str());
        io_o1.setown(file1->open(IFOcreaterw));
        if(io_o1.get() == NULL)
        {
            fprintf(m_logfile, "file %s can't be created", file1->queryFilename());
            return 0;
        }

        StringBuffer fname2(m_outdir.str());
        fname2.append("response_full").append(PATHSEPCHAR);
        fname2.append(fname);
        Owned<IFile> file2 = createIFile(fname2.str());
        io_o2.setown(file2->open(IFOcreaterw));
        if(io_o2.get() == NULL)
        {
            fprintf(m_logfile, "file %s can't be created", file2->queryFilename());
            return 0;
        }


        StringBuffer fname3(m_outdir.str());
        fname3.append("response_content").append(PATHSEPCHAR);
        fname3.append(fname);
        Owned<IFile> file3 = createIFile(fname3.str());
        io_o3.setown(file3->open(IFOcreaterw));
        if(io_o3.get() == NULL)
        {
            fprintf(m_logfile, "file %s can't be created", file3->queryFilename());
            return 0;
        }
    }

    if(request.length() > 0)
    {
        insertSoapHeaders(request);     
        sendRequest(request, io_o1.get(), io_o2.get(), io_o3.get(), NULL, stat);
    }

    return 0;
}


StringBuffer& HttpClient::generateGetRequest(StringBuffer& request)
{
    request.appendf("GET %s HTTP/1.1\r\n", m_path.str());
    request.append("Accept: image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, application/x-shockwave-flash, */*\r\n");
    request.append("Accept-Language: en-us\r\n");
    request.append("User-Agent: Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\r\n");
    request.append("Host: ").append(m_host.str());
    if(m_port != 80)
        request.appendf(":%d", m_port);
    request.append("\r\n");
    if(!m_globals->getPropBool("isPersist", false))
        request.append("Connection: Close\r\n");
    request.append(m_authheader.str());
    request.append("\r\n");
    
    return request;
}

StringBuffer& HttpClient::insertSoapHeaders(StringBuffer& request)
{
    if(request.length() == 0)
        return request;

    const char* ptr = request.str();
    while(*ptr == ' ' || *ptr == '\t' || *ptr == '\r' || *ptr == '\n')
        ptr++;

    StringBuffer contenttype;
    if(*ptr == '<')
        contenttype.set("text/xml");
    else if(*ptr == '{')
        contenttype.set("application/json");
    else
        return request;

    StringBuffer headers;

    headers.appendf("POST %s HTTP/1.1\r\n", m_path.str());
    headers.appendf("Content-Type: %s\r\n", contenttype.str());
    headers.append("User-Agent: Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\r\n");
    headers.appendf("Content-Length: %d\r\n", request.length());
    headers.append("Host: ").append(m_host.str());
    if(m_port != 80)
        headers.appendf(":%d", m_port);
    headers.append("\r\n");

    headers.append(m_authheader.str());
    if(m_globals->hasProp("soapaction"))
        headers.append("SOAPAction: ").append(m_globals->queryProp("soapaction")).append("\r\n");

    if(!m_globals->getPropBool("isPersist", false))
        headers.append("Connection: Close\r\n");

    headers.append("\r\n");

    request.insert(0, headers.str());

    return request;

}

int HttpClient::validate(StringBuffer& xml)
{
    if(xml.length() <= 0)
        return -1;

    const char* bptr;
    const char* eptr;
    bptr = strstr(xml.str(), "<soap:Body>");
    if(bptr)
        bptr = bptr + 11;
    else
        bptr = xml.str();

    int len = 0;
    eptr = strstr(bptr, "</soap:Body>");
    if(eptr)
        len = eptr - bptr;
    else
        len = xml.length() - (bptr - xml.str());

    StringBuffer targetns;
    const char* nsptr = strstr(bptr, "xmlns");
    if(nsptr)
    {
        nsptr += 5;
        while(*nsptr && (*nsptr == ' ' || *nsptr == '='))
            nsptr++;
        if(*nsptr == '"')
            nsptr++;
        const char* ensptr = nsptr;
        while(*ensptr && *ensptr != '"')
            ensptr++;
        
        targetns.append(ensptr - nsptr,nsptr);
    }

    if(m_doValidation > 1)
    {
        fflush(m_logfile);
        int srtn = 0;
        try
        {
            Owned<IPropertyTree> testTree = createPTreeFromXMLString(len, bptr, ipt_caseInsensitive);
            if ( (!m_doStress) || (m_doStress && http_tracelevel >= 5) )
                fprintf(m_logfile, "Successfully parsed XML\n");
        }
        catch(IException *e)
        {
            StringBuffer emsg;
            fprintf(m_logfile, "Error parsing XML %s\n", e->errorMessage(emsg).str());
            fprintf(m_logfile, "result xml:\n%.*s\n\n", len, bptr);
            e->Release();
            srtn = -1;
        }
        fflush(m_logfile);
        return srtn;
    }

    Owned<IXmlDomParser> p = getXmlDomParser();
    Owned<IXmlValidator> v = p->createXmlValidator();
    
    v->setXmlSource(bptr, len);
    v->setSchemaSource(m_xsd.str(), m_xsd.length());
    v->setTargetNamespace(targetns.str());
    try 
    {
        v->validate();
    }
    catch (IMultiException* me) 
    {   
        if(http_tracelevel > 0)
        {
            IArrayOf<IException> &es = me->getArray();
            for (unsigned i=0; i<es.ordinality(); i++)
            {
                StringBuffer msg;
                IException& e = es.item(i);
                fprintf(m_logfile, "Error %d: %s\n", i, e.errorMessage(msg).str());

                int line = 0, col =0;
                const char* pElemStr = bptr;

                if (msg.length() > 0 && pElemStr)
                {
                    sscanf(strstr(msg.str(), "line"), "line %d, char %d:",&line, &col);

                    if (line <= 0 || col <= 0)
                        continue;

                    for (int count = 1; count < line; count++)
                    {
                        pElemStr = strstr(pElemStr, "\n");

                        if (!pElemStr)
                            break;

                        pElemStr++;
                    }

                    if(pElemStr)
                    {
                        pElemStr += (col - 1);
                        fprintf(m_logfile, "Data Location: \"%.50s\"\n", pElemStr);
                    }
                }
            }
        }
        me->Release();
        return -1;
    }
    catch(...)
    {
        if(http_tracelevel > 0)
            fprintf(m_logfile, "Unknown expception during xsd validation\n");

        return -1;
    }

    return 0;
}

int HttpClient::sendRequest(StringBuffer& req, IFileIO* request_output, IFileIO* full_output, IFileIO* content_output, StringBuffer* outputbuf, HttpStat* stat)
{
    StringBuffer request;
    if(req.length() <= 2)
    {
        throw MakeStringException(-1, "request too short");
    }

    //Normalizing the headers
    bool endofheaders = false;
    char c0 = req.charAt(0);
    char c1 = req.charAt(1);
    if(c0 == '\n')
        request.append("\r\n");
    else
        request.append(c0);

    if(c1 == '\n')
    {
        if(c0 == '\r')
            request.append(c1);
        else
        {
            request.append("\r\n");
            if(c0 == '\n')
                endofheaders = true;
        }
    }
    else
        request.append(c1);

    unsigned seq = 2;
    while(seq < req.length() && !endofheaders)
    {
        char c = req.charAt(seq);
        if(c == '\n')
        {
            char c1 = req.charAt(seq - 1);
            char c2 = req.charAt(seq - 2);
            if(c1 == '\n' || (c1 == '\r' && c2 == '\n'))
                endofheaders = true;
            
            if(c1 != '\r')
                request.append("\r\n");
            else
                request.append(c);
        }
        else
            request.append(c);
        seq++;
    }

    if(seq < req.length())
        request.append(req.length() - seq, req.str() + seq);

    if(http_tracelevel >= 5)
        fprintf(m_logfile, ">>sending out request to %s:%d\n", m_host.str(), m_port);

    // Write the input to a file to keep the record.
    if(request_output)
        request_output->write(0, request.length(), request.str());

    SocketEndpoint ep;
    ep.set(m_host.str(), m_port);
    Owned<ISocket> socket;
    try
    {
        socket.setown(ISocket::connect(ep));
        if(m_ssctx.get() != NULL)
        {
            Owned<ISecureSocket> securesocket = m_ssctx->createSecureSocket(socket.getLink());
            int res = securesocket->secure_connect();
            if(res >= 0)
            {
                socket.set(securesocket.get());
            }
        }
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        OERRLOG("Error connecting to %s:%d - %d:%s", m_host.str(), m_port, excpt->errorCode(), excpt->errorMessage(errMsg).str());
        return -1;
    }
    catch(...)
    {
        IERRLOG("can't connect to %s:%d", m_host.str(), m_port);
        return -1;
    }

    if(socket.get() == NULL)
    {
        StringBuffer urlstr;
        OERRLOG("Can't connect to %s", ep.getUrlStr(urlstr).str());
        return -1;
    }

    bool isPersist = m_globals->getPropBool("isPersist", false);
    int numReq = 1;
    int pausemillisecs = 0;
    if(isPersist)
    {
        if(m_globals->hasProp("persistrequests"))
            numReq = atoi(m_globals->queryProp("persistrequests"));
        if(m_globals->hasProp("persistpause"))
            pausemillisecs = atof(m_globals->queryProp("persistpause"))*1000;
    }

    for(int iter=0; iter<numReq; iter++)
    {
        unsigned start1 = msTick();

        if(http_tracelevel >= 5)
            fprintf(m_logfile, ">>sending out request. Request length=%d\n", request.length());

        if(http_tracelevel >= 10)
            fprintf(m_logfile, "%s%s%s\n", sepstr, request.str(), sepstr);

        socket->write(request.str(), request.length());

        StringBuffer buf;
        StringBuffer* bufptr;
        if(outputbuf)
            bufptr = outputbuf;
        else
            bufptr = &buf;
        Owned<IByteOutputStream> ostream = createOutputStream(*bufptr);
        bool isRoxie;
        __int64 resplen = Http::receiveData(socket.get(), ostream.get(), true, isRoxie, NULL, full_output, content_output);
        if(http_tracelevel >= 5)
            fprintf(m_logfile, ">>received response. Response length: %" I64F "d.\n", resplen);
        if(http_tracelevel >= 10)
            fprintf(m_logfile, "%s\n", bufptr->str());


        if(!isPersist || iter >= numReq - 1)
        {
            socket->shutdown();
            socket->close();
        }

        unsigned end1 = msTick();

        int duration = end1 - start1;

        if(http_tracelevel >= 5)
            fprintf(m_logfile, "Time taken to send request and receive response: %d milli-seconds\n", duration);

        if(stat)
        {
            stat->duration += duration;
            stat->totaltime += duration;
            stat->numrequests += 1;
            if(duration > stat->slowest)
                stat->slowest = duration;
            if(duration < stat->fastest)
                stat->fastest = duration;
            stat->totalreqlen += request.length();
            stat->totalresplen += resplen;
        }

        if(m_doValidation)
        {
            int ret = validate(*bufptr);
            if(http_tracelevel > 0 && m_doValidation == 1)
            {
                if(ret == 0)
                {
                    fprintf(m_logfile, "\n%sSuccessfully validated the response against the xsd%s\n", sepstr, sepstr);
                }
                else
                {
                    fprintf(m_logfile, "Error: Validation against the xsd failed.\n");
                }
            }
        }

        if(http_tracelevel >= 5)
            fprintf(m_logfile, "%s", sepstr);
        if(isPersist && iter < numReq - 1)
        {
            Owned<CSocketChecker> checker = new CSocketChecker(socket.get());
            int ret = checker->waitAndCheck(pausemillisecs);
            if(ret != 0)
            {
                if(ret > 0)
                    fprintf(m_logfile, "\n>>Persistent connection closed by the other end.\n");
                else
                    fprintf(m_logfile, "\n>>Persistent connection got error.\n");
                break;
            }
        }
    }

    return 0;
}

SimpleServer::SimpleServer(IProperties* globals, int port, const char* inputpath, const char* outputdir, bool writeToFiles, int iterations)
{
    m_globals = globals;

    m_inputpath.append(inputpath);
    m_port = port;
    if(outputdir)
        m_outdir.append(outputdir);

    m_writeToFiles = writeToFiles;

    m_logfile = stdout;
    m_iterations = iterations;
    m_headerlen = 0;
    m_isPersist = m_globals->getPropBool("isPersist", false);
}

int SimpleServer::start()
{
    const char* fname = NULL;

    bool abortEarly = false;
    if(m_globals)
        abortEarly = m_globals->getPropBool("abortEarly", false);

    Owned<IFile> server_infile = NULL;

    if(m_inputpath.length() > 0)
    {
        server_infile.setown(createIFile(m_inputpath));
        if(!server_infile->exists())
        {
                if(http_tracelevel > 0)
                    fprintf(m_logfile, "Server input file/directory %s doesn't exist", m_inputpath.str());
                return -1;
        }
        
        if(!server_infile->isDirectory())
        {
            try
            {
                m_response.loadFile(m_inputpath.str(), true);
            }
            catch(IException* e)
            {
                StringBuffer errmsg;
                fprintf(m_logfile, "error loading file %s - %s\n", m_inputpath.str(), e->errorMessage(errmsg).str());
                return -1;
            }
            catch(...)
            {
                fprintf(m_logfile, "error loading file %s\n", m_inputpath.str());
                return -1;
            }

            const char* slash = strrchr(m_inputpath.str(), PATHSEPCHAR);
            if(slash)
                fname = slash + 1;
            else
                fname = m_inputpath.str();
        }
    }   

    m_roxie_response.append(m_response);

    Owned<ISocket> socket = ISocket::create(m_port);
    if(http_tracelevel > 0)
        fprintf(m_logfile, "Server started\n");

    int seq = 0;

    for (;;)
    {
        if(m_iterations != -1 && seq >= m_iterations)
            break;

        Owned<ISocket> client;
        if(!m_isPersist || m_persistentSocket.get() == nullptr)
        {
            client.setown(socket->accept());
            if(m_isPersist)
                m_persistentSocket.set(client.get());
        }
        else
        {
            Owned<CSocketChecker> checker = new CSocketChecker(m_persistentSocket.get());
            int ret = checker->waitAndCheck(WAIT_FOREVER);
            if(ret == 0)
                client.set(m_persistentSocket.get());
            else
            {
                if(ret > 0)
                    fprintf(m_logfile, "\n>>Persistent connection closed by the other end, accepting new connection...\n");
                else
                    fprintf(m_logfile, "\n>>Persistent connection got error, accepting new connection...\n");
                m_persistentSocket->shutdown();
                m_persistentSocket->close();
                client.setown(socket->accept());
                m_persistentSocket.set(client.get());
            }
        }

        char peername[256];
        int port = client->peer_name(peername, 256);

        if(http_tracelevel >= 5)
            fprintf(m_logfile, "\n>>received request from %s:%d\n", peername, port);

        StringBuffer requestbuf;
        StringBuffer reqFileName;
        Owned<IByteOutputStream> reqstream = createOutputStream(requestbuf);

        bool isRoxie;
        Http::receiveData(client.get(), reqstream.get(), false, isRoxie);

        if(http_tracelevel >= 10)
            fprintf(m_logfile, "%s%s%s", sepstr, requestbuf.str(), sepstr);

        if(isRoxie)
        {
            if(m_roxie_response.length() == 0)
            {
                m_roxie_response.append("<Dataset name='result1'><Row>Default soapplus server response</Row></Dataset>");
            }

            unsigned replyLen = m_roxie_response.length();
            _WINREV(replyLen);
            client->write((void*)&replyLen, 4);

            if(abortEarly)
            {
                int len = m_roxie_response.length()/2;
                client->write(m_roxie_response.str(), len);
                continue;
            }
            else
                client->write(m_roxie_response.str(), m_roxie_response.length());
            
            replyLen = 0;
            client->write((void*)&replyLen, 4);
            
            if(http_tracelevel >= 10)
                fprintf(m_logfile, "\n>>sent back response - \n");
            if(http_tracelevel >= 10)
                fprintf(m_logfile, "%s%s%s\n", sepstr, m_roxie_response.str(), sepstr);
            fflush(m_logfile);
        }
        else
        {
            if(server_infile.get() != NULL && server_infile->isDirectory())
            {
                StringBuffer xmlBuf;
                xmlBuf.append(strstr(requestbuf.str(), "<?xml"));

                if(xmlBuf.length())
                {
                    bool bfound = false;
                    Owned<IPropertyTree> intree(createPTreeFromXMLString(xmlBuf.str()));
                    Owned<IPropertyTreeIterator> elems = intree->getElements("soap:Body");

                    for (elems->first(); elems->isValid() && !bfound; elems->next())
                    {
                        Owned<IPropertyTreeIterator> funcs = elems->query().getElements("*");
                    
                        for (funcs->first(); funcs->isValid() && !bfound; funcs->next())
                        {
                            StringBuffer fnName(funcs->query().queryName());
                            const char* psz = strstr(fnName, "Request");

                            if(psz)
                            {
                                reqFileName.append(psz - fnName.str(), fnName.str());
                                reqFileName.append(".xml");
                                bfound = true;
                            }
                        }
                    }

                    if(bfound && server_infile.get() != NULL && server_infile->isDirectory())
                    {
                        StringBuffer respFileName;
                        respFileName.append(m_inputpath).append(PATHSEPCHAR).append(reqFileName.str());
                        Owned<IFile> respfile(createIFile(respFileName));
                        if(respfile->exists())
                        {
                            try
                            {
                                m_response.loadFile(respFileName.str(), true);
                                fname = reqFileName.str();
                            }
                            catch(IException* e)
                            {
                                StringBuffer errmsg;
                                fprintf(m_logfile, "error loading file %s - %s\n", respFileName.str(), e->errorMessage(errmsg).str());
                                return -1;
                            }
                            catch(...)
                            {
                                fprintf(m_logfile, "error loading file %s\n", respFileName.str());
                                return -1;
                            }
                        }
                    }
                }
            }

            if(m_response.length() == 0)
            {
                const char* resp_body = "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\"><soap:Body></soap:Body></soap:Envelope>";
                m_response.append("HTTP/1.1 200 OK\r\n");
                m_response.append("Content-Type: text/xml; charset=UTF-8\r\n");
                m_response.appendf("Content-Length: %d\r\n", (int) strlen(resp_body));
                m_response.append("\r\n");
                m_headerlen = m_response.length();
                m_response.append(resp_body);
            }
            else
            {
                const char* ptr = m_response.str();
                while(*ptr != '\0' && *ptr == ' ')
                    ptr++;
                if(*ptr == '<')
                {
                    StringBuffer headers;
                    headers.append("HTTP/1.1 200 OK\r\n");
                    headers.append("Content-Type: text/xml; charset=UTF-8\r\n");
                    headers.appendf("Content-Length: %d\r\n", m_response.length());
                    headers.append("\r\n");
                    m_headerlen = headers.length();
                    m_response.insert(0, headers.str());
                }
            }

            if(abortEarly)
            {
                if(m_headerlen == 0)
                {
                    const char* hend = strstr(m_response.str(), "\r\n\r\n");
                    if(hend)
                    {
                        m_headerlen = hend - m_response.str() + 4;
                    }
                    else
                    {
                        hend = strstr(m_response.str(), "\n\n");
                        if(hend)
                            m_headerlen = hend - m_response.str() + 2;
                    }
                }

                int len = m_headerlen + (m_response.length() - m_headerlen)/2;
                client->write(m_response.str(), len);
                continue;
            }
            else
                client->write(m_response.str(), m_response.length());

            if(http_tracelevel >= 10)
                fprintf(m_logfile, "\n>>sent back response - \n");
            if(http_tracelevel >= 10)
                fprintf(m_logfile, "%s%s%s\n", sepstr, m_response.str(), sepstr);
            fflush(m_logfile);
        }

        if(m_writeToFiles)
        {
            StringBuffer req_outfname, resp_outfname;
            req_outfname.append(m_outdir).append("server_request").append(PATHSEPCHAR);
            resp_outfname.append(m_outdir).append("server_response").append(PATHSEPCHAR);
            if(fname && *fname)
            {
                if(seq <= 0)
                {
                    req_outfname.append(fname);
                    resp_outfname.append(fname);
                }
                else
                {
                    req_outfname.append(fname).append(".").append(seq);
                    resp_outfname.append(fname).append(".").append(seq);
                }
            }
            else
            {
                if(seq <= 0)
                {
                    req_outfname.append("request");
                    resp_outfname.append("response");
                }
                else
                {
                    req_outfname.append("request").append(".").append(seq);
                    resp_outfname.append("response").append(".").append(seq);
                }
            }

            Owned<IFile> req_f = createIFile(req_outfname.str());
            Owned<IFileIO>req_io = req_f->open(IFOcreaterw);
            if(req_io.get() == NULL)
            {
                fprintf(m_logfile, "file %s can't be created", req_outfname.str());
                return 0;
            }
            req_io->write(0, requestbuf.length(), requestbuf.str());
            Owned<IFile> resp_f = createIFile(resp_outfname.str());
            Owned<IFileIO>resp_io = resp_f->open(IFOcreaterw);
            if(resp_io.get() == NULL)
            {
                fprintf(m_logfile, "file %s can't be created", resp_outfname.str());
                return 0;
            }
            if(isRoxie)
                resp_io->write(0, m_roxie_response.length(), m_roxie_response.str());
            else
                resp_io->write(0, m_response.length(), m_response.str());
        }

        if(server_infile.get() != NULL && server_infile->isDirectory())
            m_response.clear();

        if(!m_isPersist)
        {
            client->shutdown();
            client->close();
        }
        seq++;
    }

    return 0;
}

