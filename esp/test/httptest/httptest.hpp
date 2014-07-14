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

#ifndef _HTTPTEST_HPP__
#define _HTTPTEST_HPP__

#include "jliball.hpp"
#include "securesocket.hpp"

extern int httptest_tracelevel;

class HttpStat
{
public:
    int     msecs;
    int     slowest;
    int     fastest;
    __int64 totalreqlen;
    __int64 totalresplen;
    int     numrequests;

    HttpStat()
    {
        msecs = 0;
        slowest = 0;
        fastest = 2147483647;
        totalreqlen = 0;
        totalresplen = 0;
        numrequests = 0;
    }

    void printStat(FILE* ofile)
    {
        fprintf(ofile, "Total hits:                       %d\n", numrequests);
        fprintf(ofile, "Time taken(millisecond):          %d\n", msecs);
        fprintf(ofile, "Hits per second:                  %3.1f\n", numrequests/(msecs*0.001));
        fprintf(ofile, "Total data sent:                  %"I64F"d\n", totalreqlen);
        fprintf(ofile, "Total data received:              %"I64F"d\n", totalresplen);
        __int64 totallen = totalreqlen + totalresplen;
        fprintf(ofile, "Total data transferred:           %"I64F"d\n", totallen);
        fprintf(ofile, "Data transfered per second:       %5.1f\n", totallen/(msecs*0.001));
        fprintf(ofile, "Slowest round trip(millisecond):  %d\n", slowest);
        fprintf(ofile, "Fastest round trip(millisecond):  %d\n", fastest);
    }
};

class HttpClient
{
private:
    int          m_threads;
    int          m_times;
    StringBuffer m_host;
    int          m_port;
    FILE*        m_ofile;
    bool         m_use_ssl;
    Owned<ISecureSocketContext> m_ssctx;
    int          m_delay;
    HttpStat     m_stat;

public:
    HttpClient(int threads, int times, FILE* ofile);
    HttpClient(int threads, int times, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig);
    int sendRequest(StringBuffer& req);
    int sendRequest(int times, HttpStat& stat, StringBuffer& req);
    int sendRequest(const char* infile);
    int getUrl(const char* url);
    int sendSoapRequest(const char* url, const char* soapaction, const char* infile);
    void setDelay(int secs);
    HttpStat& getStat() {return m_stat;}
};

class HttpServer
{
    int m_port;
    StringBuffer m_ifname;
    StringBuffer m_response;
    FILE*        m_ofile;
    bool         m_use_ssl;
    Owned<ISecureSocketContext> m_ssctx;

    int          m_recvDelay;
    int          m_sendDelay;
    int          m_closeDelay;

    void handleOneRequest(ISocket* client);

public:
    
    HttpServer(int port, const char* in, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig);
    
    void setDelays(int recvDelay, int sendDelay, int closeDelay) 
    {
        m_recvDelay = recvDelay;
        m_sendDelay = sendDelay;
        m_closeDelay = closeDelay;
    }
    
    int start();
};


class COneServerHttpProxyThread
{
private:
    Owned<ISocket>  m_client;
    StringBuffer m_host;
    int          m_port;
    FILE*        m_ofile;
    bool         m_use_ssl;
    ISecureSocketContext* m_ssctx;

public:
    COneServerHttpProxyThread(ISocket* client, const char* host, int port, FILE* ofile, bool use_ssl, ISecureSocketContext* ssctx);
    virtual int start();
};

class CHttpProxyThread : public Thread
{
private:
    Owned<ISocket>  m_client;
    FILE*        m_ofile;
    Owned<ISocket>  m_remotesocket;

public:
    CHttpProxyThread(ISocket* client, FILE* ofile);
    virtual void start();
    virtual int run();
    int readline(ISocket* socket, char* buf, int bufsize, bool& socketclosed);
};


class HttpProxy
{
private:
    int          m_localport;
    StringBuffer m_host;
    int          m_port;
    FILE*        m_ofile;
    bool         m_use_ssl;
    Owned<ISecureSocketContext> m_ssctx;

public:
    HttpProxy(int localport, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig);
    int start();
};

void SplitURL(const char* url, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password,StringBuffer& host, StringBuffer& port, StringBuffer& path);

#endif

