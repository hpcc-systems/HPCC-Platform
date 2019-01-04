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

void usage()
{
    puts("Usage:");
    puts("   httptest [optins]");
    puts("Options:");
    puts("   -c:  start as a client, which is the default");
    puts("   -s:  start as a server");
    puts("   -x:  start as a proxy");
    puts("   -r <times> :  number of times for the client to send the request");
    puts("   -t <number-of-threads> :  number of concurrent threads for the client to send the request");
    puts("   -i <filename> : input file name the client uses as request, or the server uses as response");
    puts("   -o <filename> : out file name");
    puts("   -url <[http|https]://[user:passwd@]host:port/path>: when starting as a client, the url to request");
    puts("   -soap: add soap http headers to the input text, must be used together with -url");
    puts("   -action <soap-action> ");
    puts("   -h <host> : hostname/ip the client connects to");
    puts("   -p <port> : port the client connects to, or the server listens on");
    puts("   -lp <port> : for proxy mode, local port that the proxy listens on");
    puts("   -ssl : tells it to use https instead of http");
    puts("   -delay <seconds>: when used as a client, delay several seconds before sending the request");
    puts("   -dr/-ds/-dc <seconds>: used in server mode, delay before receive, send, and close");
    puts("   -sc <ssl-cofig-file> : the config file that ssl uses");
    puts("   -d <trace-level> : 0 no tracing, > 5 some tracing, > 10 all tracing. Default is 20");
    puts("");
    puts("sample config-file: ");
    puts("<EspProtocol name=\"https\" type=\"secure_http_protocol\" plugin=\"esphttp.dll\">");
    puts("  <certificate>certificate.cer</certificate>");
    puts("  <privatekey>privatekey.cer</privatekey>");
    puts("  <passphrase>quOmuY55ftGrdcRi2y70eQ==</passphrase>");
    puts("  <verify enable=\"true\" address_match=\"true\" accept_selfsigned=\"true\">");
    puts("      <ca_certificates path=\"ca\"/>");
    puts("      <revoked_certificates path=\"revoked.pem\"/>");
    puts("      <trusted_peers>");
    puts("          <peer cn=\"texas.distrix.com\"/>");
    puts("          <peer cn=\"california.distrix.com\"/>");
    puts("          <peer cn=\"ymaxp\"/>");
    puts("      </trusted_peers>");
    puts("  </verify>");
    puts("</EspProtocol>");
    puts("");
    puts("Common use cases:");
    puts("-Http client to get a certain url:");
    puts("  httptest -c -url http://images.google.com/imghp?hl=en&tab=gi&q=");
    puts("-Read in the request from a file which contains the http headers and body:");
    puts("  httptest -c -h 10.150.51.27 -p 8010 -i spray.txt");
    puts("-Read in request body from a file, add soap headers, then send it:");
    puts("  httptest -c -url http://username:password@10.150.51.27:8010/FilSpray -action http://10.150.51.27:8010/FileSpray/Spray -i sprayxml.txt");
    puts("-Httpserver to display requests:");
    puts("  httptest -s -p 8080");
    puts("-Proxy to forward all incoming requests to a certain ip/port");
    puts("  httptest -x -lp 8010 -h 10.150.51.27 -p 8010");
    puts("-Fully functional proxy:");
    puts("  httptest -x -lp 8080");

    exit(-1);
}

enum InstanceType
{
    IT_UNKNOWN = 0,
    HTTPSERVER = 1,
    HTTPCLIENT = 2,
    HTTPPROXY = 3
};

int main(int argc, char* argv[])
{
    InitModuleObjects();

    InstanceType itype = IT_UNKNOWN;

    int times = 1;
    int threads = 1;
    StringBuffer in_fname;
    StringBuffer out_fname;
    StringBuffer host;
    int port = 80;
    int localport = 80;
    bool use_ssl = false;
    bool add_soap_headers = false;
    StringBuffer scfname;
    StringBuffer url;

    const char* soapaction = NULL;

    int delay = 0;
    int recvDelay = 0, sendDelay = 0, blockDelay = 0;
    
    int i = 1;
    while(i<argc)
    {
        if (stricmp(argv[i], "-s")==0)
        {
            itype = HTTPSERVER;
            i++;
        }
        else if (stricmp(argv[i], "-c")==0)
        {
            itype = HTTPCLIENT;
            i++;
        }
        else if (stricmp(argv[i], "-x")==0)
        {
            itype = HTTPPROXY;
            i++;
        }
        else if (stricmp(argv[i],"-r")==0)
        {
            i++;
            times = atoi(argv[i++]);
        }
        else if (stricmp(argv[i],"-t")==0)
        {
            i++;
            threads = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-h")==0)
        {
            i++;
            host.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i], "-p")==0)
        {
            i++;
            port = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-i") == 0)
        {
            i++;
            in_fname.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i], "-o") == 0)
        {
            i++;
            out_fname.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i], "-ssl") == 0)
        {
            use_ssl = true;
            i++;
        }
        else if (stricmp(argv[i], "-sc") == 0)
        {
            i++;
            scfname.clear().append(argv[i++]);
        }
        else if (stricmp(argv[i], "-lp") == 0)
        {
            i++;
            localport = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-delay") == 0)
        {
            i++;
            delay = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-dr") == 0)
        {
            i++;
            recvDelay = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-ds") == 0)
        {
            i++;
            sendDelay = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-dc") == 0)
        {
            i++;
            blockDelay = atoi(argv[i++]);
        }
        else if(stricmp(argv[i], "-url") == 0)
        {
            i++;
            url.append(argv[i++]);
        }
        else if (stricmp(argv[i], "-soap")==0)
        {
            add_soap_headers = true;
            i++;
        }
        else if (stricmp(argv[i], "-action")==0)
        {
            i++;
            soapaction = argv[i++];
        }
        else if (stricmp(argv[i], "-d")==0)
        {
            i++;
            httptest_tracelevel = atoi(argv[i++]);;
        }
        else
        {
            printf("Error: command format error\n");
            usage();
        }
    }

    try
    {
        Owned<IPropertyTree> sslconfig;
        if(scfname.length() > 0)
            sslconfig.setown(createPTreeFromXMLFile(scfname.str(), ipt_caseInsensitive));
        FILE* ofile = NULL;
        if(out_fname.length() != 0)
        {
            ofile = fopen(out_fname.str(), "a+");
            if(ofile == NULL)
            {
                printf("can't open file %s\n", out_fname.str());
                exit(-1);
            }
        }
        else
        {
            ofile = stdout;
        }

        if(itype == HTTPSERVER)
        {
            HttpServer server(port, in_fname.str(), ofile, use_ssl, sslconfig.get());
            server.setDelays(recvDelay, sendDelay, blockDelay);
            server.start();
        }
        else if(itype == HTTPPROXY)
        {
            HttpProxy proxy(localport, host.str(), port, ofile, use_ssl, sslconfig.get());
            proxy.start();
        }
        else
        {
            if(add_soap_headers && url.length() == 0)
            {
                printf("Error: when you use -soap option, you must provide the full url\ntype in \"%s -h\" for usage", argv[0]);
                return 0;
            }

            if(host.length() == 0 && url.length() == 0)
            {
                printf("Error: destination host or url required\n");
                usage();
            }


            if(add_soap_headers)
            {
                HttpClient client(threads, times, ofile);
                if(delay > 0)
                    client.setDelay(delay);
                client.sendSoapRequest(url.str(), soapaction, in_fname.str());
            }
            else if(url.length() == 0)
            {
                HttpClient client(threads, times, host.str(), port, ofile, use_ssl, sslconfig.get());
                if(delay > 0)
                    client.setDelay(delay);
                client.sendRequest(in_fname.str());         
            }
            else
            {
                HttpClient client(threads, times, ofile);
                if(delay > 0)
                    client.setDelay(delay);
                client.getUrl(url.str());
            }
        }
        fclose(ofile);
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        IERRLOG("Error - %d:%s", excpt->errorCode(), excpt->errorMessage(errMsg).str());
        return -1;
    }
    catch(...)
    {
        IERRLOG("Unknown exception");
        return -1;
    }

    releaseAtoms();
    return 0;
}

