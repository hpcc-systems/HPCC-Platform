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

#include "http.hpp"

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

COneServerHttpProxyThread::COneServerHttpProxyThread(ISocket* client, const char* host, int port, FILE* ofile, 
                                                                      bool use_ssl, ISecureSocketContext* ssctx, const char* url_prefix)
{
    m_client.set(client);
    m_host.append(host);
    m_port = port;
    m_ofile = ofile;
    m_use_ssl = use_ssl;
    m_ssctx = ssctx;
    m_url_prefix = url_prefix;
}

int COneServerHttpProxyThread::start()
{
    try
    {
        char peername[256];
        int port = m_client->peer_name(peername, 256);
        if(http_tracelevel >= 5)
            fprintf(m_ofile, "\n>>receivd request from %s:%d\n", peername, port);

        StringBuffer requestbuf;
        Owned<IByteOutputStream> reqstream = createOutputStream(requestbuf);
        bool isRoxie;
        Http::receiveData(m_client, reqstream.get(), false, isRoxie);
        
        if(http_tracelevel >= 10)
            fprintf(m_ofile, "%s%s%s", sepstr, requestbuf.str(), sepstr);
        else if(http_tracelevel >= 5)
        {
            const char* endofline = strstr(requestbuf.str(), "\n");
            if(endofline)
            {
                StringBuffer firstline;
                firstline.append((endofline - requestbuf.str()), requestbuf.str());
                fprintf(m_ofile, "%s", firstline.str());
            }
            else
                fprintf(m_ofile, "%s\n", requestbuf.str());
        }


        if (0 != stricmp(m_url_prefix, "/"))
        {
            int url_offset;
            if (!strnicmp(requestbuf.str(), "GET ", 4))
                url_offset = 4;
            else if (!strnicmp(requestbuf.str(), "POST ", 5))
                url_offset = 5;
            else
                url_offset = -1;

            if (url_offset > 0)
            {
                const int prefix_len = strlen(m_url_prefix);
                if (0 != strnicmp(requestbuf.str()+url_offset, m_url_prefix, prefix_len))
                {
                    const char* endofline = strstr(requestbuf.str(), "\n");
                    if(endofline)
                    {
                        StringBuffer firstline;
                        firstline.append((endofline - requestbuf.str()), requestbuf.str());
                        fprintf(m_ofile, "INVALID request: %s", firstline.str());
                    }
                    else
                        fprintf(m_ofile, "INVALID request:\n%s\n", requestbuf.str());


                    StringBuffer respbuf;
                    respbuf.append("HTTP/1.1 404 Not Found\n")
                        .append("Content-Type: text/xml; charset=UTF-8\n")
                        .append("Connection: close\n");

                    m_client->write(respbuf.str(), respbuf.length());

                    if(http_tracelevel >= 5)
                        fprintf(m_ofile, ">>sent the response back to %s:%d:\n", peername, port);
                    if(http_tracelevel >= 10)
                        fprintf(m_ofile, "%s%s%s", sepstr, respbuf.str(), sepstr);
                    fflush(m_ofile);

                    m_client->shutdown();
                    m_client->close();
                    return -1;
                }
                else
                {
                    //we want to map /x to / and /x/y to /y as follows:
                    //if m_url_prefix is /x and url is /x/y then remove x
                    //to result in //y
                    requestbuf.remove(++url_offset, prefix_len-1);
                    //now, if we have //y then change it to /y
                    if (*(requestbuf.str()+url_offset) == '/')
                        requestbuf.remove(url_offset, 1);
                }
            }
        }

        SocketEndpoint ep;
        Owned<ISocket> socket2;

        ep.set(m_host.str(), m_port);
        socket2.setown(ISocket::connect(ep));
        if(m_use_ssl && m_ssctx != NULL)
        {
#ifdef _USE_OPENSSL
            Owned<ISecureSocket> securesocket = m_ssctx->createSecureSocket(socket2.getLink());
            int res = securesocket->secure_connect();
            if(res >= 0)
            {
                socket2.set(securesocket.get());
            }
#else
        throw MakeStringException(-1, "COneServerHttpProxyThread: failure to create SSL socket - OpenSSL not enabled in build");
#endif
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
        if(http_tracelevel >= 5)
            fprintf(m_ofile, "\n>>sending request to %s:%d\n", m_host.str(), m_port);
        if(http_tracelevel >= 10)
            fprintf(m_ofile, "%s%s%s", sepstr, requestbuf.str(), sepstr);

        socket2->write(requestbuf.str(), requestbuf.length());
        StringBuffer respbuf;
        Owned<IByteOutputStream> respstream = createOutputStream(respbuf);
        Http::receiveData(socket2.get(), respstream.get(), true, isRoxie);
        
        if(http_tracelevel >= 5)
            fprintf(m_ofile, ">>received response from %s:%d:\n", m_host.str(), m_port);
        if(http_tracelevel >= 10)
            fprintf(m_ofile, "%s%s%s", sepstr, respbuf.str(), sepstr);

        m_client->write(respbuf.str(), respbuf.length());
        fflush(m_ofile);

        if(http_tracelevel >= 5)
            fprintf(m_ofile, ">>sent the response back to %s:%d:\n", peername, port);
        if(http_tracelevel >= 10)
            fprintf(m_ofile, "%s%s%s", sepstr, respbuf.str(), sepstr);

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
        IERRLOG("unknown exception");
        return -1;
    }

    return 0;
}

class CReadWriteThread : public Thread
{
private:
    ISocket*  m_r;
    ISocket*  m_w;
    FILE*     m_ofile;
public:
    CReadWriteThread(ISocket* r, ISocket* w, FILE* ofile)
    {
        m_r = r;
        m_w = w;
        m_ofile = ofile;
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
                // make it not to log unless it's REALLY needed.
                if(http_tracelevel >= 20 && m_ofile != NULL)
                {
                    fprintf(m_ofile, "%s", buf);
                }
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
        if(http_tracelevel >= 5)
            fprintf(m_ofile, "\n>>receivd request from %s:%d\n", peername, clientport);

        char oneline[2049];
        memset(oneline, 0, 2049);

        bool socketclosed = false;
        int lenread = readline(m_client.get(), oneline, 2048, socketclosed);
        
        if(http_tracelevel >= 10)
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
            CReadWriteThread t1(m_client.get(), m_remotesocket.get(), m_ofile);
            CReadWriteThread t2(m_remotesocket.get(), m_client.get(), m_ofile);
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
            bool isRoxie;
            Http::receiveData(m_client, reqstream.get(), false, isRoxie, "Proxy-Connection", NULL, NULL, true);

            if(http_tracelevel >= 5)
                fprintf(m_ofile, "Received request from %s\n", peername);
            if(http_tracelevel >= 10)
                fprintf(m_ofile, "%s\n", requestbuf.str());

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
            if(http_tracelevel >= 5)
                fprintf(m_ofile, ">>sending request to %s:%d\n", hostname, port);

            m_remotesocket->write(requestbuf.str(), requestbuf.length());
            StringBuffer respbuf;
            Owned<CSocketOutputStream> respstream = new CSocketOutputStream(m_client.get());
            Http::receiveData(m_remotesocket.get(), respstream.get(), true, isRoxie);
            
            if(http_tracelevel >= 5)
                fprintf(m_ofile, ">>receivd response from %s:%d:\n", hostname, port);

            if(http_tracelevel >= 5)
                fprintf(m_ofile, ">>sent response back to %s:%d\n", peername, clientport);

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
        OERRLOG("%s", excpt->errorMessage(errMsg).str());
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
        IERRLOG("Unknown exception reading from socket(%d).", socket->OShandle());
        socketclosed = true;
    }

    buf[ptr] = 0;
    return ptr;
}

HttpProxy::HttpProxy(int localport, const char* url, FILE* ofile, const char* url_prefix)
    : m_url_prefix(url_prefix)
{
    m_localport = localport;
    if(url && *url)
    {
        StringBuffer protocol, username, password, portbuf, path;
        Http::SplitURL(url, protocol, username, password, m_host, portbuf, path);
        
        if(protocol.length() > 0 && stricmp(protocol.str(), "HTTPS") == 0)
            m_use_ssl = true;
        else
            m_use_ssl = false;

        if(portbuf.length() > 0)
            m_port = atoi(portbuf.str());
        else
        {
            if(m_use_ssl)
                m_port = 443;
            else
                m_port = 80;
        }

        if(m_use_ssl)
        {
#ifdef _USE_OPENSSL
            m_ssctx.setown(createSecureSocketContext(ClientSocket));
#else
        throw MakeStringException(-1, "HttpProxy: failure to create SSL socket - OpenSSL not enabled in build");
#endif
        }       
    }
    
    m_ofile = ofile;
}

HttpProxy::HttpProxy(int localport, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig)
{
    m_localport = localport;
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
        throw MakeStringException(-1, "HttpProxy: failure to create SSL socket - OpenSSL not enabled in build");
#endif
    }       
}

int HttpProxy::start()
{
    Owned<ISocket> socket1 = ISocket::create(m_localport);
    if(http_tracelevel > 0)
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
                COneServerHttpProxyThread thrd(client.get(), m_host.str(), m_port, m_ofile, m_use_ssl, m_ssctx, m_url_prefix);
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

class CSocksProxyThread : public Thread
{
private:
    Owned<ISocket>  m_client;
    FILE*        m_ofile;
    Owned<ISocket>  m_remotesocket;

public:
    CSocksProxyThread(ISocket* client, FILE* ofile)
    {
        m_client.set(client);
        m_ofile = ofile;
    }

    virtual void start()
    {
        Thread::start();
    }

    virtual int run()
    {
        Thread::Link();

        int ret = 0;
        try
        {
            char peername[256];
            int clientport = m_client->peer_name(peername, 256);

            char inbuf[1024];
            char outbuf[1024];
            memset(inbuf, 0, 1024);
            memset(outbuf, 0, 1024);

            unsigned int len = 0;
            unsigned int lenread = 0;
            m_client->read(inbuf, 8, 8, lenread);
            if(lenread != 8)
            {
                DBGLOG("didn't get the first 8 bytes, invalid socks request.");
                return -1;
            }

            len += lenread;
            m_client->read(inbuf + len, 0, 1, lenread);
            StringBuffer username;
            while(lenread > 0)
            {
                len += lenread;
                if(len >= 1023)
                {
                    len = 0;
                }
                if(inbuf[len - 1] == '\0')
                {
                    break;
                }
                char c = inbuf[len - 1];
                username.append(c);
                m_client->read(inbuf + len, 0, 1, lenread);
            }
            
            if(http_tracelevel >= 5)
                fprintf(m_ofile, "\n>>receivd SOCKS request from %s:%d, user %s\n", peername, clientport, username.str());

            outbuf[0] = '\0';
            outbuf[1] = (char)0x5a;

            m_client->write(outbuf, 8);

            char ubyte = inbuf[2];
            char lbyte = inbuf[3];
            unsigned short port = (unsigned short)ubyte;
            port = port << 8;
            port += lbyte;

            // TBD IPV6 (should use serialize/deserialize)

            IpAddress ip;
            ip.setNetAddress(4,inbuf+4);        
                                                

            StringBuffer ipstr;
            ip.getIpText(ipstr);
            if(http_tracelevel >= 5)
                fprintf(m_ofile, "\n>>The request is for %s:%d\n", ipstr.str(), port);      

            SocketEndpoint ep;
            ep.set(port, ip);
            m_remotesocket.setown(ISocket::connect(ep));

            m_client->set_nonblock(false);
            m_remotesocket->set_nonblock(false);
            CReadWriteThread t1(m_client.get(), m_remotesocket.get(), m_ofile);
            CReadWriteThread t2(m_remotesocket.get(), m_client.get(), m_ofile);
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            m_remotesocket->shutdown();
            m_remotesocket->close();
            m_client->shutdown();
            m_client->close();
        }
        catch(IException *excpt)
        {
            StringBuffer errMsg;
            OERRLOG("%s", excpt->errorMessage(errMsg).str());
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
};

SocksProxy::SocksProxy(int localport, FILE* ofile)
{
    m_localport = localport;
    m_ofile = ofile;
}

int SocksProxy::start()
{
    Owned<ISocket> socket1 = ISocket::create(m_localport);
    if(http_tracelevel > 0)
        printf("Socks Proxy started\n");

    for (;;)
    {
        try
        {
            Owned<ISocket> client = socket1->accept();
            char peername[256];
            int port = client->peer_name(peername, 256);

            Owned<CSocksProxyThread> thrd = new CSocksProxyThread(client.get(), m_ofile);
            thrd->start();
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


