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

#include "httptest.hpp"
#include "jsocket.hpp"
#include "jstream.ipp"

#ifdef _WIN32
#include "winsock.h"
#define ERRNO() WSAGetLastError()
#else
#define ERRNO() (errno)
#endif

int httptest_tracelevel = 20;

const char* sepstr = "\n---------------\n";

static __int64 receiveData(ISocket* socket, IByteOutputStream* ostream, bool alwaysReadContent, const char* headersToRemove = NULL)
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

    char oneline[2049];
    int lenread = bsocket->readline(oneline, 2048, true, NULL);
    int content_length = 0;
    while(lenread >= 0 && oneline[0] != '\0' && oneline[0] != '\r' && oneline[0] != '\n')
    {
        totalresplen += lenread;
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
        lenread = bsocket->readline(oneline, 2048, true, NULL);
    }

    if(oneline[0] == '\r' || oneline[0] == '\n')
    {
        ostream->writeBytes(oneline, lenread);
        totalresplen += lenread;
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
            totalresplen += readlen;
            ostream->writeBytes(buf, readlen);
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
        int buflen = 10;
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
            totalresplen += readlen;
            buf[readlen] = 0;
            ostream->writeBytes(buf, readlen);
        }
    }
    return totalresplen;
}

//=======================================================================================================
// class HttpClient

HttpClient::HttpClient(int threads, int times, FILE* ofile)
{
    m_threads = threads;
    m_times = times;
    m_delay = 0;
    m_use_ssl = false;
    m_ofile = ofile;
}

HttpClient::HttpClient(int threads, int times, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig)
{
    m_threads = threads;
    m_times = times;
    m_delay = 0;
    m_host.append(host);
    m_port = port;
    m_ofile = ofile;
    m_use_ssl = use_ssl;
    if(use_ssl)
    {
#ifdef _USE_OPENSSL
        if(sslconfig != NULL)
            m_ssctx.setown(createSecureSocketContextEx2(sslconfig, ClientSocket));
        else
            m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpClient: failure to create SSL connection to host '%s': OpenSSL not enabled in build", host);
#endif
    }
}

int HttpClient::getUrl(const char* url)
{
    if(!url || !*url || !m_times)
        return 0;

    StringBuffer protocol, user, passwd, port, path;
    m_host.clear();
    SplitURL(url, protocol, user, passwd, m_host, port, path);

    if(port.length() > 0)
        m_port = atoi(port.str());
    else
    {
        if(protocol.length() > 0 && stricmp(protocol.str(), "https") == 0)
            m_port = 443;
        else
            m_port = 80;
    }

    if(stricmp(protocol.str(), "HTTPS") == 0)
        m_use_ssl = true;

    if(m_use_ssl)
    {
#if _USE_OPENSSL
        if(m_ssctx.get() == NULL)
            m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpClient: failure to create SSL socket - OpenSSL not enabled in build");
#endif
    }

    StringBuffer request;
    request.appendf("GET %s HTTP/1.0\r\n", path.str());
    request.append("Accept: image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, application/x-shockwave-flash, */*\r\n");
    request.append("Accept-Language: en-us\r\n");
    //request.append("Accept-Encoding: gzip, deflate\r\n");
    request.append("User-Agent: Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\r\n");
    request.append("Host: ").append(m_host.str());
    if(m_port != 80)
        request.appendf(":%d", m_port);
    request.append("\r\n");
    if(user.length() > 0)
    {
        StringBuffer auth, abuf;
        abuf.appendf("%s:%s", user.str(), passwd.str());
        JBASE64_Encode(abuf.str(), abuf.length(), auth);
        request.appendf("Authorization: Basic %s\r\n", auth.str());
    }
    request.append("\r\n");

    return sendRequest(request);
}

int HttpClient::sendRequest(const char* infile)
{
    StringBuffer req;

    if(infile && *infile)
    {
        try
        {
            req.loadFile(infile, true);
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            printf("\nerror loading file %s - %s", infile, e->errorMessage(errmsg).str());
            return -1;
        }
        catch(...)
        {
            printf("\nerror loading file %s", infile);
            return -1;
        }
    }

    if(req.length() == 0)
    {
        if(httptest_tracelevel > 0)
            printf("using default request\n");

        req.append("GET / HTTP/1.0\r\n");
        req.append("Accept: image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, application/x-shockwave-flash, */*\r\n");
        req.append("Accept-Language: en-us\r\n");
        //req.append("Accept-Encoding: gzip, deflate\r\n");
        req.append("User-Agent: Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\r\n");
        req.append("Host: ").append(m_host.str());
        if(m_port != 80)
            req.appendf(":%d", m_port);
        req.append("\r\n");
        req.append("\r\n");
    }

    return sendRequest(req);
}

int HttpClient::sendSoapRequest(const char* url, const char* soapaction, const char* infile)
{
    if(!url || !*url || !infile || !*infile)
        return 0;

    StringBuffer protocol, user, passwd, port, path;
    m_host.clear();
    SplitURL(url, protocol, user, passwd, m_host, port, path);

    if(port.length() > 0)
        m_port = atoi(port.str());
    else
    {
        if(protocol.length() > 0 && stricmp(protocol.str(), "https") == 0)
            m_port = 443;
        else
            m_port = 80;
    }

    if(stricmp(protocol.str(), "HTTPS") == 0)
        m_use_ssl = true;

    if(m_use_ssl)
    {
#ifdef _USE_OPENSSL
        if(m_ssctx.get() == NULL)
            m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpClient: failure to create SSL socket - OpenSSL not enabled in build");
#endif
    }

    StringBuffer request;

    try
    {
        request.loadFile(infile, true);
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        printf("\nerror loading file %s - %s", infile, e->errorMessage(errmsg).str());
        return -1;
    }
    catch(...)
    {
        printf("\nerror loading file %s", infile);
        return -1;
    }

    if(request.length() == 0)
    {
        printf("input is empty\n");
        return -1;
    }


    const char* ptr = request.str();
    while(*ptr == ' ')
        ptr++;
    if(*ptr != '<')
    {
        printf("the input should be xml\n");
        return -1;
    }

    if(strncmp(ptr, "<?xml", 5) != 0 && strncmp(ptr, "<soap:Envelope", 14) != 0)
    {
        request.insert(0, "<?xml version=\"1.0\" encoding=\"utf-8\"?><soap:Envelope xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:SOAP-ENC=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:wsse=\"http://schemas.xmlsoap.org/ws/2002/04/secext\"><soap:Body>");
        request.append("</soap:Body></soap:Envelope>");
    }

    StringBuffer headers;

    headers.appendf("POST %s HTTP/1.1\r\n", path.str());
    headers.append("Accept: image/gif, image/x-xbitmap, image/jpeg, image/pjpeg, application/vnd.ms-excel, application/vnd.ms-powerpoint, application/msword, application/x-shockwave-flash, */*\r\n");
    headers.append("Accept-Language: en-us\r\n");
    headers.append("Content-Type: text/xml\r\n");
    if(soapaction && *soapaction)
        headers.appendf("SOAPAction: \"%s\"\r\n", soapaction);
    //headers.append("Accept-Encoding: gzip, deflate\r\n");
    headers.append("User-Agent: Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)\r\n");
    headers.appendf("Content-Length: %d\r\n", request.length());
    headers.append("Host: ").append(m_host.str());
    if(m_port != 80)
        headers.appendf(":%d", m_port);
    headers.append("\r\n");

    if(user.length() > 0)
    {
        StringBuffer auth, abuf;
        abuf.appendf("%s:%s", user.str(), passwd.str());
        JBASE64_Encode(abuf.str(), abuf.length(), auth);
        headers.appendf("Authorization: Basic %s\r\n", auth.str());
    }

    headers.append("\r\n");


    request.insert(0, headers.str());

    return sendRequest(request);
}

class CHttpClientThread : public Thread
{
private:
    int m_times;
    HttpStat m_stat;
    StringBuffer& m_request;
    HttpClient* m_client;

public:
    CHttpClientThread(int times, HttpClient* client, StringBuffer& req): m_request(req)
    {
        m_times = times;
        m_client = client;
    }

    virtual int run()
    {
        if(m_client)
            return m_client->sendRequest(m_times, m_stat, m_request);
        return 0;
    }

    HttpStat& getStat()
    {
        return m_stat;
    }
};

int HttpClient::sendRequest(StringBuffer& req)
{
    int thrds = m_threads;
    if(m_threads > 1)
    {
        int it_per_thrd = 1;
        int extra = 0;
        if(thrds > m_times)
            thrds = m_times;
        else
        {
            it_per_thrd = m_times / thrds;
            extra = m_times - it_per_thrd * thrds;
        }

        CHttpClientThread** thrdlist = new CHttpClientThread*[thrds];
        int i;
        for(i = 0; i < extra; i++)
            thrdlist[i] = new CHttpClientThread(it_per_thrd + 1, this, req);
        for(i = extra; i < thrds; i++)
            thrdlist[i] = new CHttpClientThread(it_per_thrd, this, req);

        for(i = 0; i < thrds; i++)
            thrdlist[i]->start();
        for(i = 0; i < thrds; i++)
            thrdlist[i]->join();

        for(i = 0; i < thrds; i++)
        {
            CHttpClientThread* thrd = thrdlist[i];
            HttpStat& stat = thrd->getStat();
            if(m_stat.msecs < stat.msecs)
                m_stat.msecs = stat.msecs;
            m_stat.totalreqlen += stat.totalreqlen;
            m_stat.totalresplen += stat.totalresplen;
            m_stat.numrequests += stat.numrequests;
            if(m_stat.fastest > stat.fastest)
                m_stat.fastest = stat.fastest;
            if(m_stat.slowest < stat.slowest)
                m_stat.slowest = stat.slowest;
        }
        delete [] thrdlist;
    }
    else
        sendRequest(m_times, m_stat, req);

    fprintf(m_ofile, ">> Statistics:\n");
    fprintf(m_ofile, "%s", sepstr);
    fprintf(m_ofile, "Number of threads:                %d\n", thrds);
    m_stat.printStat(m_ofile);
    fprintf(m_ofile, "%s", sepstr);

    return 0;
}

int HttpClient::sendRequest(int times, HttpStat& stat, StringBuffer& req)
{
    StringBuffer request;
    if(req.length() <= 2)
    {
        throw MakeStringException(-1, "request too short");
    }

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

    if(httptest_tracelevel > 5)
        fprintf(m_ofile, ">>sending out request to %s:%d for %d times\n", m_host.str(), m_port, times);

    unsigned start = msTick();
    int slowest = 0;
    int fastest = 2147483647;
    for(int i = 0; i < times; i++)
    {
        SocketEndpoint ep;
        ep.set(m_host.str(), m_port);
        Owned<ISocket> socket;
        try
        {
            socket.setown(ISocket::connect(ep));
            if(m_use_ssl && m_ssctx.get() != NULL)
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
            UERRLOG("Error connecting to %s:%d - %d:%s", m_host.str(), m_port, excpt->errorCode(), excpt->errorMessage(errMsg).str());
            continue;
        }
        catch(...)
        {
            OERRLOG("Can't connect to %s:%d", m_host.str(), m_port);
            continue;
        }

        if(socket.get() == NULL)
        {
            StringBuffer urlstr;
            OERRLOG("Can't connect to %s", ep.getUrlStr(urlstr).str());
            continue;
        }

        if(m_delay > 0)
            sleep(m_delay);

        if(httptest_tracelevel > 5)
            fprintf(m_ofile, ">>sending out request:\n");
        if(httptest_tracelevel > 10)
            fprintf(m_ofile, "%s%s%s\n", sepstr, request.str(), sepstr);

        unsigned start1 = msTick();

        socket->write(request.str(), request.length());

        if(httptest_tracelevel > 5)
            fprintf(m_ofile, ">>receiving response:\n");

        StringBuffer buf;
        Owned<IByteOutputStream> ostream = createOutputStream(buf);
        stat.totalresplen += receiveData(socket.get(), ostream.get(), true);
        if(httptest_tracelevel > 10)
            fprintf(m_ofile, "%s%s%s\n", sepstr, buf.str(), sepstr);

        char tmpbuf[256];
        unsigned int sizeread;
        do
        {
            socket->read(tmpbuf, 0, 256, sizeread);
        }
        while(sizeread > 0);

        socket->shutdown();
        socket->close();
        fflush(m_ofile);
        unsigned end1 = msTick();
        int duration = end1 - start1;
        if(duration <= fastest)
            fastest = duration;
        if(duration > slowest)
            slowest = duration;

        if(i % 100 == 0)
            fprintf(stderr, "sent out %d\n", i);
    }

    unsigned end = msTick();
    stat.msecs = end - start;
    stat.numrequests = times;
    stat.totalreqlen = times * request.length();
    stat.slowest = slowest;
    stat.fastest = fastest;

    return 0;
}

void HttpClient::setDelay(int secs)
{
    m_delay = secs;
}

//=======================================================================================================
// class HttpServer

HttpServer::HttpServer(int port, const char* in, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig)
{
    m_ifname.append(in);
    m_port = port;
    m_ofile = ofile;
    m_use_ssl = use_ssl;
    m_recvDelay = m_sendDelay = m_closeDelay = 0;
    if(use_ssl)
    {
#ifdef _USE_OPENSSL
        if(sslconfig != NULL)
            m_ssctx.setown(createSecureSocketContextEx2(sslconfig, ServerSocket));
        else
            m_ssctx.setown(createSecureSocketContext(ServerSocket));
#else
        throw MakeStringException(-1, "HttpServer: failure to create SSL socket - OpenSSL not enabled in build");
#endif
    }
}

int HttpServer::start()
{
    if(m_ifname.length() > 0)
    {
        try
        {
            m_response.loadFile(m_ifname.str(), true);
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            printf("\nerror loading file %s - %s", m_ifname.str(), e->errorMessage(errmsg).str());
        }
        catch(...)
        {
            printf("\nerror loading file %s", m_ifname.str());
        }

    }

    Owned<ISocket> socket = ISocket::create(m_port);
    if(httptest_tracelevel > 0)
        printf("Server started\n");

    for (;;)
    {
        Owned<ISocket> client = socket->accept();

        // use ssl?
        if(m_use_ssl && m_ssctx.get() != NULL)
        {
            try
            {
                Owned<ISecureSocket> secure_sock = m_ssctx->createSecureSocket(client.getLink());
                int res = secure_sock->secure_accept();
                if(res < 0)
                {
                    printf("secure_accept error\n");
                    continue;
                }
                client.set(secure_sock.get());
            }
            catch(...)
            {
                printf("secure_accept error\n");
                continue;
            }
        }

        // handle the request
        try
        {
            handleOneRequest(client);
        } catch (IException* e) {
            StringBuffer msg;
            IERRLOG("Exception occured: %s", e->errorMessage(msg).str());
            e->Release();
        } catch (...) {
            IERRLOG("Unknown exception occurred");
        }
    }

    return 0;
}

void HttpServer::handleOneRequest(ISocket* client)
{
    char peername[256];
    int port = client->peer_name(peername, 256);

    if(httptest_tracelevel > 5)
        fprintf(m_ofile, "\n>>receivd request from %s:%d\n", peername, port);

    StringBuffer requestbuf;
    Owned<IByteOutputStream> reqstream = createOutputStream(requestbuf);
    if (m_recvDelay>0)
        sleep(m_recvDelay);
    receiveData(client, reqstream.get(), false);

    if(httptest_tracelevel > 10)
        fprintf(m_ofile, "%s%s%s", sepstr, requestbuf.str(), sepstr);

    if(m_response.length() == 0)
    {
        //const char* resp_body = "<html><head><meta http-equiv=\"refresh\" content=\"3; url=http://ymaxp:8020\"/></head><body>Default response from httptest server mode</body></html>";
        //const char* resp_body = "<html><body onLoad=window.setTimeout(\"location.href='http://ymaxp:8020'\",10000)>Default response from httptest server mode</body></html>";
        const char* resp_body = "<html><head><title>default response</title></head><body>Default response from httptest server mode</body></html>";
        //const char* resp_body = "<html><head><title>default response</title></head><body><IFRAME SRC=\"http://www.yahoo.com\" TITLE=\"esp config xml file\" width=\"100%\" height=\"100%\" frameborder=\"0\" marginwidth=\"0\" marginheight=\"0\"></IFrame></body></html>";
        m_response.append("HTTP/1.1 200 OK\r\n");
        m_response.append("Content-Type: text/html; charset=UTF-8\r\n");
        m_response.appendf("Content-Length: %d\r\n", (int) strlen(resp_body));
        m_response.appendf("Subject: my-title\r\n");
        m_response.append("Expires: 0\r\n");
        m_response.append("\r\n");
        m_response.append(resp_body);
    }

    if (m_sendDelay)
        sleep(m_sendDelay);
    client->write(m_response.str(), m_response.length());
    if(httptest_tracelevel > 10)
        fprintf(m_ofile, "\n>>sent back response - \n");
    if(httptest_tracelevel > 10)
        fprintf(m_ofile, "%s%s%s\n", sepstr, m_response.str(), sepstr);
    fflush(m_ofile);

    if (m_closeDelay)
        sleep(m_closeDelay);
    client->close();
}

void replaceHeader(StringBuffer& buf, const char* name, const char* value)
{
    StringBuffer newbuf;
    const char* hdr = strstr(buf.str(), StringBuffer(name).append(":").str());
    if(hdr)
    {
        newbuf.append(hdr - buf.str(), buf.str());
        newbuf.append(name).append(": ").append(value);
        const char* eol = strstr(hdr, "\r");
        if(eol)
        {
            newbuf.append(eol);
        }
        buf.swapWith(newbuf);
    }
}

void checkContentLength(StringBuffer& buf)
{
    const char* clen = strstr(buf.str(), "Content-Length:");
    if(!clen)
        clen = strstr(buf.str(), "Content-length:");
    if(!clen)
        clen = strstr(buf.str(), "content-length:");

    if(!clen)
    {
        printf("no Content-Length header\n");
        return;
    }

    clen += 15;
    while(*clen && !isdigit(*clen))
    {
        clen++;
    }

    int len = 0;
    while(*clen && isdigit(*clen))
    {
        char c = *clen;
        len = len*10 + (c - '0');
        clen++;
    }

    printf("Content-Length is %d\n", len);

    const char* ptr = clen;
    while(*ptr)
    {
        ptr = strchr(ptr, '\n');
        if(!ptr)
            break;
        ptr++;
        if(*ptr == '\n')
        {
            ptr++;
            break;
        }
        else if(*(ptr+1) == '\n')
        {
            ptr += 2;
            break;
        }
    }

    int actual_len = 0;
    if(ptr)
    {
        actual_len = buf.length() - (ptr - buf.str());
    }

    printf("Actual length of content is %d\n", actual_len);
}

//=======================================================================================================
// class COneServerHttpProxyThread

COneServerHttpProxyThread::COneServerHttpProxyThread(ISocket* client, const char* host, int port, FILE* ofile, bool use_ssl, ISecureSocketContext* ssctx)
{
    m_client.set(client);
    m_host.append(host);
    m_port = port;
    m_ofile = ofile;
    m_use_ssl = use_ssl;
    m_ssctx = ssctx;
}

int COneServerHttpProxyThread::start()
{
    try
    {
        char peername[256];
        int port = m_client->peer_name(peername, 256);
        if(httptest_tracelevel > 5)
            fprintf(m_ofile, "\n>>receivd request from %s:%d\n", peername, port);

        StringBuffer requestbuf;
        Owned<IByteOutputStream> reqstream = createOutputStream(requestbuf);
        receiveData(m_client, reqstream.get(), false);

        if(httptest_tracelevel > 10)
            fprintf(m_ofile, "%s%s%s", sepstr, requestbuf.str(), sepstr);

        SocketEndpoint ep;
        Owned<ISocket> socket2;

        ep.set(m_host.str(), m_port);
        socket2.setown(ISocket::connect(ep));
        if(m_use_ssl && m_ssctx != NULL)
        {
            Owned<ISecureSocket> securesocket = m_ssctx->createSecureSocket(socket2.getLink());
            int res = securesocket->secure_connect();
            if(res >= 0)
            {
                socket2.set(securesocket.get());
            }
        }

        if(socket2.get() == NULL)
        {
            StringBuffer urlstr;
            OERRLOG("Can't connect to %s", ep.getUrlStr(urlstr).str());
            return -1;
        }

        char newhost[1024];
        sprintf(newhost, "%s:%d", m_host.str(), m_port);
        replaceHeader(requestbuf, "Host", newhost);
        //checkContentLength(requestbuf);
        if(httptest_tracelevel > 5)
            fprintf(m_ofile, "\n>>sending request to %s:%d\n", m_host.str(), m_port);
        if(httptest_tracelevel > 10)
            fprintf(m_ofile, "%s%s%s", sepstr, requestbuf.str(), sepstr);

        socket2->write(requestbuf.str(), requestbuf.length());
        StringBuffer respbuf;
        Owned<IByteOutputStream> respstream = createOutputStream(respbuf);
        receiveData(socket2.get(), respstream.get(), true);

        if(httptest_tracelevel > 5)
            fprintf(m_ofile, ">>received response from %s:%d:\n", m_host.str(), m_port);
        if(httptest_tracelevel > 10)
            fprintf(m_ofile, "%s%s%s", sepstr, respbuf.str(), sepstr);

        m_client->write(respbuf.str(), respbuf.length());
        fflush(m_ofile);

        if(httptest_tracelevel > 5)
            fprintf(m_ofile, ">>sent the response back to %s:%d:\n", peername, port);

        socket2->shutdown();
        socket2->close();
        m_client->shutdown();
        m_client->close();
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        OERRLOG("%s", excpt->errorMessage(errMsg).str());
        return -1;
    }
    catch(...)
    {
        OERRLOG("unknown exception");
        return -1;
    }

    return 0;
}

class CReadWriteThread : public Thread
{
private:
    ISocket*  m_r;
    ISocket*  m_w;

public:
    CReadWriteThread(ISocket* r, ISocket* w)
    {
        m_r = r;
        m_w = w;
    }

    virtual int run()
    {
        char buf[2049];
        int readlen = 0;
        do
        {
            readlen = recv(m_r->OShandle(), buf, 2048, 0);
            if(readlen > 0)
            {
                buf[readlen] = 0;
                m_w->write(buf, readlen);
            }
            else
            {
                m_w->shutdown();
                m_w->close();
                break;
            }
        }
        while(true);
        return 0;
    }
};

//=======================================================================================================
// class CHttpProxyThread

CHttpProxyThread::CHttpProxyThread(ISocket* client, FILE* ofile)
{
    m_client.set(client);
    m_ofile = ofile;
}

void CHttpProxyThread::start()
{
    Thread::start();
}

int CHttpProxyThread::run()
{
    Thread::Link();

    int ret = 0;
    try
    {
        char peername[256];
        int clientport = m_client->peer_name(peername, 256);
        if(httptest_tracelevel > 5)
            fprintf(m_ofile, "\n>>receivd request from %s:%d\n", peername, clientport);

        char oneline[2049];
        memset(oneline, 0, 2049);

        bool socketclosed = false;
        int lenread = readline(m_client.get(), oneline, 2048, socketclosed);

        if(httptest_tracelevel > 40)
            printf("firstline=%s\n", oneline);

        if(strncmp(oneline, "CONNECT ", 8) == 0)
        {
            char* curptr = oneline + 8;
            while(*curptr && *curptr == ' ')
                curptr++;
            const char* hostptr = curptr;
            while(*curptr && *curptr != ':' && *curptr != ' ')
                curptr++;
            int port = 80;
            if(*curptr == ':')
            {
                *curptr = 0;
                curptr++;
                const char* portptr = curptr;
                while(*curptr && *curptr != ' ')
                    curptr++;
                *curptr = 0;
                if(*portptr)
                    port = atoi(portptr);
            }

            StringBuffer host(hostptr);

            while(lenread > 2 && !socketclosed)
                lenread = readline(m_client.get(), oneline, 2048, socketclosed);

            SocketEndpoint ep;
            ep.set(host.str(), port);
            m_remotesocket.setown(ISocket::connect(ep));

            const char* resp = "HTTP/1.0 200 Connection established\r\n"
                                "Proxy-agent: Netscape-Proxy/1.1\r\n\r\n";
            m_client->write(resp, strlen(resp));

            m_client->set_nonblock(false);
            m_remotesocket->set_nonblock(false);
            CReadWriteThread t1(m_client.get(), m_remotesocket.get());
            CReadWriteThread t2(m_remotesocket.get(), m_client.get());
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            //printf("read/write threads returned\n");
            m_remotesocket->shutdown();
            m_remotesocket->close();
            m_client->shutdown();
            m_client->close();
        }
        else
        {
            const char* http = strstr(oneline, "http://");
            if(!http)
                http = strstr(oneline, "HTTP://");

            if(!http)
                throw MakeStringException(-1, "protocol not recognized\n");

            StringBuffer requestbuf;
            requestbuf.append(http - oneline, oneline);
            const char* slash = http + 7;
            while(*slash && *slash != '/' && *slash != ' ' && *slash != '\r')
                slash++;

            if(*slash != '/')
                requestbuf.append('/');
            else
                requestbuf.append(slash);

            Owned<IByteOutputStream> reqstream = createOutputStream(requestbuf);
            receiveData(m_client, reqstream.get(), false, "Proxy-Connection");

            if(httptest_tracelevel > 40)
                printf("%s\n", requestbuf.str());


            const char* hostname = http + 7;
            char* ptr = (char*)hostname;
            while(*ptr && *ptr != ':' && *ptr != '/' && *ptr != ' ')
                ptr++;

            int port = 80;
            if(*ptr == ':')
            {
                *ptr = 0;
                ptr++;
                const char* portptr = ptr;
                while(*ptr && *ptr != ' ' && *ptr != '/')
                    ptr++;
                if(*ptr)
                    *ptr = 0;
                if(portptr)
                    port = atoi(portptr);
            }
            else
                *ptr = 0;

            SocketEndpoint ep;
            ep.set(hostname, port);
            m_remotesocket.setown(ISocket::connect(ep));
            if(httptest_tracelevel > 5)
                fprintf(m_ofile, ">>sending request to %s:%d\n", hostname, port);

            m_remotesocket->write(requestbuf.str(), requestbuf.length());
            StringBuffer respbuf;
            Owned<CSocketOutputStream> respstream = new CSocketOutputStream(m_client.get());
            receiveData(m_remotesocket.get(), respstream.get(), true);

            if(httptest_tracelevel > 5)
                fprintf(m_ofile, ">>receivd response from %s:%d, and sent back to %s:%d:\n", hostname, port, peername, clientport);
            //fprintf(m_ofile, "%s", respbuf.str());

            //m_client->write(respbuf.str(), respbuf.length());
            fflush(m_ofile);

            m_remotesocket->shutdown();
            m_remotesocket->close();
            m_client->shutdown();
            m_client->close();
        }
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        IERRLOG("%s", excpt->errorMessage(errMsg).str());
        ret = -1;
    }
    catch(...)
    {
        IERRLOG("unknown exception");
        ret = -1;
    }

    Thread::Release();

    return 0;
}

int CHttpProxyThread::readline(ISocket* socket, char* buf, int bufsize, bool& socketclosed)
{
    socketclosed = false;

    if(!socket || !buf)
        return 0;

    char charbuf[2];
    int ptr = 0;
    try
    {
        unsigned int readlen;
        socket->read(charbuf,0, 1, readlen);
        while(readlen > 0)
        {
            if(ptr >= bufsize)
            {
                buf[ptr] = 0;
                return ptr;
            }

            buf[ptr++] = charbuf[0];

            if(charbuf[0] == '\r')
            {
                socket->read(charbuf,0, 1, readlen);
                if(readlen > 0 && ptr < bufsize)
                    buf[ptr++] = charbuf[0];
                break;
            }
            else if(charbuf[0] == '\n')
                break;

            socket->read(charbuf,0, 1, readlen);
        }
    }
    catch (IException *e)
    {
        StringBuffer estr;
        if(e->errorCode() != JSOCKERR_graceful_close)
        {
            OERRLOG("socket(%d) : %s", socket->OShandle(), e->errorMessage(estr).str());
        }
        e->Release();
        socketclosed = true;
    }
    catch(...)
    {
        OERRLOG("Unknown exception reading from socket(%d).", socket->OShandle());
        socketclosed = true;
    }

    buf[ptr] = 0;
    return ptr;
}

//=======================================================================================================
// class HttpProxy

HttpProxy::HttpProxy(int localport, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig)
{
    m_localport = localport;
    m_host.append(host);
    m_port = port;
    m_ofile = ofile;
    m_use_ssl = use_ssl;
    if(use_ssl)
    {
#if _USE_OPENSSL
        if(sslconfig != NULL)
            m_ssctx.setown(createSecureSocketContextEx2(sslconfig, ClientSocket));
        else
            m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpProxy: failure to create SSL connection to host '%s': OpenSSL not enabled in build", host);
#endif
    }
}

int HttpProxy::start()
{
    Owned<ISocket> socket1 = ISocket::create(m_localport);
    if(httptest_tracelevel > 0)
        printf("Proxy started\n");

    for (;;)
    {
        try
        {
            Owned<ISocket> client = socket1->accept();
            char peername[256];
            int port = client->peer_name(peername, 256);

            if(m_host.length() > 0)
            {
                COneServerHttpProxyThread thrd(client.get(), m_host.str(), m_port, m_ofile, m_use_ssl, m_ssctx);
                thrd.start();
            }
            else
            {
                Owned<CHttpProxyThread> thrd = new CHttpProxyThread(client.get(), m_ofile);
                thrd->start();
            }
        }
        catch(IException *excpt)
        {
            StringBuffer errMsg;
            OERRLOG("%s", excpt->errorMessage(errMsg).str());
        }
        catch(...)
        {
            IERRLOG("unknown exception");
        }
    }
    return 0;
}

#define URL_MAX  2048

void SplitURL(const char* url, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password,StringBuffer& host, StringBuffer& port, StringBuffer& path)
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

