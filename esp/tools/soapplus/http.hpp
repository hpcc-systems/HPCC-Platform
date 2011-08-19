/*##############################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
##############################################################################
 */

#ifndef _HTTP_HPP__
#define _HTTP_HPP__

#include "jstring.hpp"
#include "jptree.hpp"
#include "jstream.ipp"
#include "securesocket.hpp"
#include "xslprocessor.hpp"
#include "xmlvalidator.hpp"

#ifdef _WIN32
#include "winsock.h"
#endif

extern int http_tracelevel;
extern const char* sepstr;

const int URL_MAX = 2048;

class Http
{
public:
    static __int64 receiveData(ISocket* socket, IByteOutputStream* ostream, bool isClientSide, bool& isRoxie, const char* headersToRemove = NULL, IFileIO* full_output = NULL, IFileIO* content_output = NULL, bool alwayshttp = false);
    static void SplitURL(const char* url, StringBuffer& protocol,StringBuffer& UserName,StringBuffer& Password,StringBuffer& host, StringBuffer& port, StringBuffer& path);
};

class CRequest : public CInterface, implements IInterface
{
    StringAttr m_reqname;
    StringBuffer m_reqbuf;

public:
    IMPLEMENT_IINTERFACE;

    CRequest(const char* reqname)
    {
        if(reqname)
            m_reqname.set(reqname);
    }

    CRequest(const char* reqname, const char* reqbuf)
    {
        if(reqname)
            m_reqname.set(reqname);

        if(reqbuf)
            m_reqbuf.append(reqbuf);
    }

    const char* getName()
    {
        return m_reqname.get();
    }

    void setName(const char* reqname)
    {
        if(reqname)
            m_reqname.set(reqname);
        else
            m_reqname.clear();
    }

    StringBuffer& queryReqbuf()
    {
        return m_reqbuf;
    }
};

class HttpStat : public CInterface, implements IInterface
{
public:
    int     threads;
    int     duration; //milli-seconds
    __int64 totaltime; //milli-seconds
    int     slowest;
    int     fastest;
    __int64 totalreqlen;
    __int64 totalresplen;
    __int64 numrequests;

public:
    IMPLEMENT_IINTERFACE;
    HttpStat();
    void printStat(FILE* ofile);
};

class CAddress : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    StringBuffer m_ip;
    int          m_port;
    struct sockaddr_in* m_addr;

    CAddress(const char* host, int port)
    {
        m_addr = new struct sockaddr_in;
    #ifndef _WIN32
        bzero(m_addr, sizeof(*m_addr));
    #endif
        m_addr->sin_family = AF_INET;
        m_addr->sin_port = htons(port);

        IpAddress ip(host);
        ip.getIpText(m_ip);
        m_port = port;

    #ifndef _WIN32
        inet_pton(AF_INET, m_ip.str(), &(m_addr->sin_addr));
    #else
        m_addr->sin_addr.s_addr = inet_addr(m_ip.str());
    #endif

    }
    virtual ~CAddress()
    {
        if(m_addr)
            delete m_addr;
    }
};

class HttpClient : public CInterface, implements IInterface
{
private:
    StringBuffer m_url;
    StringBuffer m_inname;
    StringBuffer m_outdir;
    StringBuffer m_outfilename;

    Owned<CAddress> m_serveraddr;
    StringBuffer m_protocol;
    StringBuffer m_host;
    int          m_port;
    StringBuffer m_path;
    StringBuffer m_user;
    StringBuffer m_password;

    StringBuffer m_authheader;

    FILE*        m_logfile;
    Owned<ISecureSocketContext> m_ssctx;

    bool         m_writeToFiles;

    bool         m_doValidation;
    bool         m_isEspLogFile;
    StringBuffer m_xsdpath;
    StringBuffer m_xsd;

    // Fields for stress test
    bool         m_doStress;
    int          m_stressthreads;
    int          m_stressduration;
    IArrayOf<CRequest> m_stressrequests;
    bool         m_stopstress;

    IProperties* m_globals;

    StringBuffer& insertSoapHeaders(StringBuffer &request);
    int validate(StringBuffer& xml);
    int sendStressRequests(HttpStat* overall_stat);

public:
    IMPLEMENT_IINTERFACE;

    HttpClient( IProperties* globals, const char* url, const char* inname = NULL, 
                const char* outdir = NULL, const char* outfilename = NULL, bool writeToFiles = false, 
                bool doValidation = false, const char* xsdpath = NULL,
                bool isEspLogFile=false);
    void start();

    StringBuffer& generateGetRequest(StringBuffer& request);
    int sendRequest(StringBuffer& request, const char* fname, HttpStat* stat = NULL);
    int sendRequest(StringBuffer& request, IFileIO* request_output = NULL, IFileIO* full_output = NULL, IFileIO* content_output = NULL, StringBuffer* outputbuf = NULL, HttpStat* stat = NULL);

    IArrayOf<CRequest>& queryStressRequests() {return m_stressrequests;}
    int sendStressRequest(StringBuffer& request, HttpStat* stat);
    bool queryStopStress() {return m_stopstress;}
    IProperties* queryGlobals() {return m_globals;}
    void addEspRequest(const char* requestId, const char* service, const char* method, StringBuffer& request, HttpStat& httpStat);
};


class SimpleServer
{
    int m_port;
    StringBuffer m_inputpath;
    StringBuffer m_response;
    int          m_headerlen;
    StringBuffer m_roxie_response;
    StringBuffer m_outdir;
    FILE*        m_logfile;
    int          m_iterations;
    IProperties* m_globals;

    bool         m_writeToFiles;

public:
    SimpleServer(IProperties* globals, int port, const char* inputpath = NULL, const char* outputdir = NULL, bool writeToFiles = false, int iterations = -1);
    int start();
};

class ThreadedSimpleServer : public Thread
{
private:
    SimpleServer m_server;

public:
    ThreadedSimpleServer(IProperties* globals, int port, const char* inputpath = NULL, const char* outputdir = NULL, bool writeToFiles = false, int iterations = -1) : m_server(globals, port, inputpath, outputdir, writeToFiles, iterations) {}
    int run()
    {
        m_server.start();
        return 0;
    }
};

class COneServerHttpProxyThread
{
private:
    Owned<ISocket>  m_client;
    StringBuffer m_host;
    StringBuffer m_url_prefix;
    int          m_port;
    FILE*        m_ofile;
    bool         m_use_ssl;
    ISecureSocketContext* m_ssctx;

public:
    COneServerHttpProxyThread(ISocket* client, const char* host, int port, FILE* ofile, bool use_ssl, 
                                      ISecureSocketContext* ssctx, const char* url_prefix);
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
    StringBuffer m_url_prefix;
    int          m_port;
    FILE*        m_ofile;
    bool         m_use_ssl;
    Owned<ISecureSocketContext> m_ssctx;

public:
    HttpProxy(int localport, const char* host, int port, FILE* ofile, bool use_ssl, IPropertyTree* sslconfig);
    HttpProxy(int localport, const char* url, FILE* ofile, const char* url_prefix);
    int start();
};

class SocksProxy
{
private:
    int          m_localport;
    FILE*        m_ofile;

public:
    SocksProxy(int localport, FILE* ofile);
    int start();
};



#endif

