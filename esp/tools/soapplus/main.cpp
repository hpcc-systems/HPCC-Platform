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
#include "jliball.hpp"
#include "xsdparser.hpp"
#include "xmldiff.hpp"

FILE* logfile = NULL;
const char* version = "1.1";

void usage()
{
    printf("Lexis-Nexis soapplus version %s. Usage:\n", version);
    puts("   soapplus [action/options]");
    puts("");
    puts("Actions: select one");
    puts("   -h/-? : show this usage page");
    puts("   -c : start in client mode. This is the default action.");
    puts("   -s : start in server mode.");
    puts("   -x <url_prefix>: start in proxy mode, mapping /url_prefix* to * at destination URL (to redirect all requests use -x / )");
    puts("   -socks : start it as a SOCKS proxy.");
    puts("   -b : start in hybrid mode as both a client and a server.");
    puts("   -g : automatically generate the requests from wsdl/xsd. url?wsdl is used when none is specified");
    puts("   -t : test schema parser");
    puts("   -diff <left> <right> : compare 2 files or directories. You can ignore certain xpaths, as specified in the config file (with -cfg option). An example of config file is: <whatever><Ignore><XPath>/soap:Body/AAC_AuthorizeRequest/ClientIP</XPath><XPath>/soap:Header/Security/UsernameToken/Username</XPath></Ignore></whatever>");
    puts("   -stress <threads> <seconds> : run stress test, with the specified number of threads, for the specified number of seconds.");
    puts("   -persist [<number-of-requests>] [<pause-seconds>] : use persistent connection. For client mode you can optionally specify the number of requests to send or receive, and the number of seconds to pause between sending requests.");
    puts("");
    puts("Options: ");
    puts("   -url <[http|https]://[user:passwd@]host:port/path>: the url to send requests to. For esp, the path is normally the service name, not the method name. For examle, it could be WsADL, WsAccurint, WsIdentity etc.");
    puts("        For esp, the path is normally the service name, not the method name. For e.g., WsAccurint, WsIdentity etc.");
    puts("   -i <file or directory name> : input file/directory containing the requests (for client mode) or responses(for server mode). It can also contain wildcards, for example .\\inputs\\*.xml. If not specified, a predefined GET will be performed on the url.");
    puts("   -si <file or directory name> : input file/directory containing the response(s) for servermode or hybrid mode. ");
    puts("        If not specified, a predefined simple response will be used for every request.");
    puts("        If a single file is specified, the file will be used for every request");
    puts("        If a directory is specified, the xml file that has the same file name as the method name in the request will be used. For example, if the method name in the request is GetAttributes, then the file GetAttributes.xml is used. If no appropriate file is found, a predefined simple response will be returned");
    puts("        If the server is started in hybrid mode, then the file with the same filename as request file will be used.");
    puts("   -o <directory-name> : directory to put the outputs.");
    puts("   -w : create output directories and write the output files. When -o is not specified,  a 'SOAPPLUS.current-date-time' directory under current directory will be used.");
    puts("   -of : name of the output file generated, when -w is specified. If not specified, files are generated with the name \"get.txt\". If -i is specified as a file name, then -of is ignored and the file name specified with -i is used for the generated output file");
    puts("   -p : port to listen on.");
    puts("   -d <trace-level> : 0 no tracing, >=1 a little tracing, >=5 some tracing, >=10 all tracing. If -w is specified, default tracelevel is 5, if -stress is specified, it's 1, otherwise it's 10.");
    puts("   -v : validate the response. If -xsd is not specified, url?xsd will be used to retrieve the xsd.");
    puts("   -vx : validate the response xml");
    puts("   -xsd <path-or-url> : the path or url to the xsd file to be used for validation.");
    puts("   -wsdl <path-or-url> : the path or url to the wsdl file to be used to generate requests.");
    puts("   -r : the xsd/or wsdl is in roxie response format.");
    puts("   -ra : generate all datasets in schema (default: only these exists in template).");
    puts("   -l <ESP log file>: when used with -g action, use ESP requests from the specified log file. Optionally, use -n option to specify how many for each method.");
    puts("   -n <number>: generate <number> elements for array type [default: 1]");
    puts("   -m <method-name> : the method to generate the request for. If not specified, a request will be generated for every method in the wsdl.");
    puts("   -a : for server or hybrid mode, tell the server to abort before finish sending back response.");
    puts("   -cfg <file-name> : configuration files for soapplus");
    puts("   -gx : use *** MISSING *** as the default value for missing string fields");
    puts("   -gs : don't generate soap message wrap");
    puts("   -gf : the output filename for generated message");
    puts("   -delay <min-milli-seconds> <max-milli-seconds> : randomly delay between requests in stress test. By default requests are sent without stop.");
    puts("   -ooo : the 2 input files to diff are Out-Of-Order, so do the best-match calculation while comparing them. (this will slow it down dramatically for big xml files).");
    puts("   -ordsen : order sensitive. Even if there're matching tags, but if the tags in the files are not in the same order, they'll be deemed different.");
    puts("   -y: use default answers to yes or no prompts.");
    puts("   -wiz: ECL to ESP wizard mode.");
    puts("   -soapaction <url>: specify the soapaction.");
    puts("   -compress: add header to the request to tell the server to compress the content if possible.");
    puts("   -version: print out soapplus version.");
}

enum SoapPlusAction
{
    SPA_SERVER = 0,
    SPA_CLIENT = 1,
    SPA_BOTH = 2,
    SPA_PROXY = 3,
    SPA_TestSchemaParser = 4,
    SPA_SOCKS = 5
};

int doValidation = 0;
const char* xsdpath = NULL;

void doTestSchemaParser(IProperties* globals, const char* url, const char* in_fname)
{
    const char* element = globals->queryProp("method");
    if (!element)
    {
        printf("Error: no element is specified using -m\n");
        exit(1);
    }

    if (url)
    {
        printf("Error: url not supported for -t yet\n");
        exit(1);
    }

    if (!in_fname)
    {
        printf("Error: no schema is specified by -i\n");
        exit(1);
    }
    
    Owned<IXmlSchema> xsd;

    bool isRoxie = globals->queryProp("roxie")!=NULL;
    if (isRoxie)
    {
        StringBuffer s;
        try {
            s.loadFile(in_fname);
        } catch (IException* e) {
            StringBuffer msg;
            printf("Error in loading file: %s: %s", in_fname, e->errorMessage(msg).str());
            e->Release();
        }

        Owned<IPTree> xml = createPTreeFromXMLString(s);
        IPTree* schema = xml->queryBranch(VStringBuffer("Results/Result/XmlSchema[@name='%s']/*[1]", element));
        if (schema)
            xsd.setown(createXmlSchemaFromPTree(LINK(schema)));
    }
    else
        xsd.setown(createXmlSchemaFromFile(in_fname));

    if (xsd.get())
    {
        IXmlType* t = xsd->queryElementType(isRoxie ? "Dataset" : element);
        if (t)
        {
            StringBuffer buf;
            StringStack parent;
            t->toString(buf,0,parent);
            printf("Element %s:\n%s", element, buf.str());
        }
        else
            printf("Can not find the defintion for %s\n", element);
    }
}


void createDirectories(const char* outdir, const char* url, bool bClient, bool bServer,
                       StringBuffer& outpath, StringBuffer& outpath1, StringBuffer& outpath2, 
                       StringBuffer& outpath3, StringBuffer& outpath4, StringBuffer& outpath5)
{
    Owned<IFile> out;
    if(outdir && *outdir)
    {
        out.setown(createIFile(outdir));
        if(out->exists())
        {
            if(!out->isDirectory())
                throw MakeStringException(0, "Output destination %s already exists, and it's not a directory", outdir);
        }
        outpath.append(outdir);
        if(outpath.charAt(outpath.length() - 1) != PATHSEPCHAR)
            outpath.append(PATHSEPCHAR);
    }
    else
    {
        outpath.append("SOAPPLUS");
        addFileTimestamp(outpath, false);
        outpath.append(PATHSEPCHAR);
    }

    outpath1.append(outpath).append("request").append(PATHSEPCHAR);
    outpath2.append(outpath).append("response_full").append(PATHSEPCHAR);
    outpath3.append(outpath).append("response_content").append(PATHSEPCHAR);
    outpath4.append(outpath).append("server_request").append(PATHSEPCHAR);
    outpath5.append(outpath).append("server_response").append(PATHSEPCHAR);

    if(bClient)
    {
        out.setown( createIFile(outpath1.str()) );
        if(out->exists())
        {
            if(!out->isDirectory())
                throw MakeStringException(0, "Output destination %s already exists, and it's not a directory", outpath1.str());
        }
        else
        {
            if(http_tracelevel > 0)
                fprintf(logfile, "Creating directory %s\n", outpath1.str());
            recursiveCreateDirectory(outpath1.str());
        }
        if(url && *url)
        {
            out.setown( createIFile(outpath2.str()) );
            if(out->exists())
            {
                if(!out->isDirectory())
                    throw MakeStringException(0, "Output destination %s already exists, and it's not a directory", outpath2.str());
            }
            else
            {
                if(http_tracelevel > 0)
                    fprintf(logfile, "Creating directory %s\n", outpath2.str());
                recursiveCreateDirectory(outpath2.str());
            }

            out.setown( createIFile(outpath3.str()) );
            if(out->exists())
            {
                if(!out->isDirectory())
                    throw MakeStringException(0, "Output destination %s already exists, and it's not a directory", outpath3.str());
            }
            else
            {
                if(http_tracelevel > 0)
                    fprintf(logfile, "Creating directory %s\n", outpath3.str());
                recursiveCreateDirectory(outpath3.str());
            }
        }
    }

    if(bServer)
    {
        if(http_tracelevel > 0)
            fprintf(logfile, "Creating directory %s\n", outpath4.str());
        recursiveCreateDirectory(outpath4.str());
        if(http_tracelevel > 0)
            fprintf(logfile, "Creating directory %s\n", outpath5.str());
        recursiveCreateDirectory(outpath5.str());
    }
}

int processRequest(IProperties* globals, SoapPlusAction action, const char* url, 
                   const char* in_fname, int port, const char* server_in_fname, 
                   const char* outdir, bool writeToFiles, const char* proxy_url_prefix, 
                   bool isEspLogFile, const char* outfilename)
{
    if (action == SPA_TestSchemaParser)
        doTestSchemaParser(globals, url, in_fname);

    Owned<IFileIO> fio1 = NULL;
    Owned<IFileIO> fio2 = NULL;
    Owned<IFileIO> fio3 = NULL;
    Owned<IFileIO> fio4 = NULL;
    
    StringBuffer outpath, outpath1, outpath2, outpath3, outpath4, outpath5;
    if (writeToFiles)
    {
        const bool bClient = action == SPA_CLIENT || action == SPA_BOTH;
        const bool bServer = action == SPA_SERVER || action == SPA_BOTH;
        createDirectories(outdir, url, bClient, bServer, outpath, outpath1, outpath2, outpath3, outpath4, outpath5);
    }

    if(action == SPA_SERVER)
    {
        SimpleServer server(globals, port, (server_in_fname && *server_in_fname)?server_in_fname:in_fname, writeToFiles?outpath.str():NULL, writeToFiles, -1);
        server.start();
    }
    else if(action == SPA_CLIENT)
    {
        Owned<HttpClient> client = new HttpClient(globals, url, in_fname, writeToFiles?outpath.str():NULL, outfilename, writeToFiles, doValidation, xsdpath, isEspLogFile);
        client->start();
    }
    else if(action == SPA_BOTH)
    {
        bool autogen = globals->getPropBool("autogen", false);
        const char* method = globals->queryProp("method");
        if(autogen && (!method || !*method))
        {
            throw MakeStringException(0, "In hybrid mode, you have to specify a method for automatically generating the request");
        }

        Owned<IFile> infile = NULL;
        Owned<IFile> server_infile = NULL;

        if(in_fname && *in_fname)
        {
            infile.setown(createIFile(in_fname));
            if(!infile->exists())
            {
                if(http_tracelevel > 0)
                    fprintf(logfile, "Input file/directory %s doesn't exist", in_fname);
                return -1;
            }
        }

        if(server_in_fname && *server_in_fname)
        {
            server_infile.setown(createIFile(server_in_fname));
            if(!server_infile->exists())
            {
                if(http_tracelevel > 0)
                    fprintf(logfile, "Server input file/directory %s doesn't exist", server_in_fname);
                return -1;
            }
        }
        
        if(infile.get() != NULL && infile->isDirectory())
        {
            Owned<IDirectoryIterator> di = infile->directoryFiles();
            if(di.get())
            {
                ForEach(*di)
                {
                    IFile &file = di->query();
                    if (file.isFile() && file.size() > 0)
                    {
                        const char* fname = file.queryFilename();
                        const char* slash = strrchr(fname, PATHSEPCHAR);
                        if(slash)
                            fname = slash;
                        StringBuffer sfname;
                        if(server_infile.get())
                        {
                            if(server_infile->isDirectory())
                            {
                                sfname.append(server_in_fname);
                                if(sfname.charAt(sfname.length() - 1) != PATHSEPCHAR)
                                    sfname.append(PATHSEPCHAR);
                                sfname.append(fname);
                            }
                            else
                            {
                                sfname.append(server_in_fname);
                            }
                            Owned<IFile> sf = createIFile(sfname.str());
                            if(!sf.get() || !sf->exists())
                            {
                                if(http_tracelevel > 0)
                                    fprintf(logfile, "Server input file %s doesn't exist. Make sure the server input directory's files match those of request input directory\n", sfname.str());
                                return -1;
                            }
                        }

                        ThreadedSimpleServer server(globals, port, sfname.str(), writeToFiles?outpath.str():NULL, writeToFiles,  1);
                        server.start();
                        HttpClient client(globals, url, file.queryFilename(), writeToFiles?outpath.str():NULL, outfilename, writeToFiles);
                        client.start();
                        if(http_tracelevel >= 5)
                            fprintf(logfile, "Waiting for the server thread to finish.\n");
                        server.join();
                        if(http_tracelevel >= 5)
                            fprintf(logfile, "Server thread finished.\n");
                    }   
                }
            }
        }   
        else
        {
            if(server_infile.get() && server_infile->isDirectory())
            {
                if(http_tracelevel > 0)
                    fprintf(logfile, "When request input is not directory, the server input must be a file\n");
                return -1;
            }
            ThreadedSimpleServer server(globals, port, server_in_fname, writeToFiles?outpath.str():NULL, writeToFiles,  1);
            server.start();
            HttpClient client(globals, url, in_fname, writeToFiles?outpath.str():NULL, outfilename, writeToFiles);
            client.start();
            if(http_tracelevel >= 5)
                fprintf(logfile, "Waiting for the server thread to finish.\n");
            server.join();
            if(http_tracelevel >= 5)
                fprintf(logfile, "Server thread finished.\n");
        }
    }
    else if(action == SPA_PROXY)
    {
        HttpProxy proxy(port, url, logfile, proxy_url_prefix);
        proxy.start();
    }
    else if(action == SPA_SOCKS)
    {
        SocksProxy sproxy(port, logfile);
        sproxy.start();
    }

    return 0;
}

bool isNumber(const char* str)
{
    if(!str || !*str)
        return false;

    int i = 0;
    char c;
    while((c=str[i++]) != '\0')
    {
        if(!isdigit(c))
            return false;
    }

    return true;
}

typedef MapStringTo<int> MapStrToInt;

int main(int argc, char** argv)
{
    InitModuleObjects();

    Owned<IProperties> globals = createProperties(true);

    logfile = stdout;

    const char* in_fname = NULL;
    const char* server_in_fname = NULL;
    const char* out_dirname = NULL;
    const char* out_filename = NULL;
    const char* url = NULL;
    bool isDiff = false;
    bool isStress = false;
    bool isEspLogFile = false;
    const char* left = NULL;
    const char* right = NULL;
    const char* tnumstr = NULL;
    const char* dnumstr = NULL;
    const char* proxy_url_prefix = NULL;

    SoapPlusAction action = SPA_CLIENT;

    int i = 1;
    bool writeToFiles = false;
    int port = 80;

    bool tracelevelspecified = false;

    while(i<argc)
    {
        if(stricmp(argv[i], "-h") == 0 || stricmp(argv[i], "-?") == 0)
        {
            usage();
            return 0;
        }
        else if (stricmp(argv[i], "-t") == 0)
        {
            i++;
            action = SPA_TestSchemaParser;
        }
        else if (stricmp(argv[i], "-c") == 0)
        {
            i++;
            action = SPA_CLIENT;
        }
        else if (stricmp(argv[i], "-x") == 0)
        {
            i++;
            action = SPA_PROXY;
            proxy_url_prefix = argv[i++];
        }
        else if (stricmp(argv[i], "-socks") == 0)
        {
            i++;
            action = SPA_SOCKS;
        }
        else if (stricmp(argv[i], "-s") == 0)
        {
            i++;
            action = SPA_SERVER;
        }
        else if (stricmp(argv[i], "-b") == 0)
        {
            i++;
            action = SPA_BOTH;
        }
        else if (stricmp(argv[i], "-i") == 0)
        {
            i++;
            in_fname = argv[i++];
        }
        else if (stricmp(argv[i], "-si") == 0)
        {
            i++;
            server_in_fname = argv[i++];
        }
        else if (stricmp(argv[i], "-o") == 0)
        {
            i++;
            out_dirname = argv[i++];
        }
        else if (stricmp(argv[i], "-w") == 0)
        {
            i++;
            writeToFiles = true;
        }
        else if (stricmp(argv[i], "-of") == 0)
        {
            i++;
            out_filename = argv[i++];
        }
        else if(stricmp(argv[i], "-url") == 0)
        {
            i++;
            url = argv[i++];
        }
        else if (stricmp(argv[i], "-d")==0)
        {
            i++;
            http_tracelevel = atoi(argv[i]);;
            tracelevelspecified = true;
            globals->setProp("tracelevel",argv[i]);
            i++;
        }
        else if (stricmp(argv[i], "-p")==0)
        {
            i++;
            port = atoi(argv[i++]);;
        }
        else if (stricmp(argv[i], "-v")==0)
        {
            i++;
            doValidation = 1;
        }
        else if (stricmp(argv[i], "-vx")==0)
        {
            i++;
            doValidation = 2;
        }
        else if (stricmp(argv[i], "-xsd") == 0)
        {
            i++;
            xsdpath = argv[i++];
        }
        else if(stricmp(argv[i], "-g") == 0)
        {
            i++;
            globals->setProp("autogen", "1");
        }
        else if(stricmp(argv[i], "-gx") == 0)
        {
            i++;
            globals->setProp("gx", "1");
        }
        else if(stricmp(argv[i], "-gs") == 0)
        {
            i++;
            globals->setProp("gs", "1");
        }
        else if(stricmp(argv[i], "-gf") == 0)
        {
            i++;
            globals->setProp("gf", argv[i++]);
        }
        else if(stricmp(argv[i], "-cfg") == 0)
        {
            i++;
            globals->setProp("cfg", argv[i++]);
        }
        else if (stricmp(argv[i], "-n") == 0)
        {
            i++;
            globals->setProp("items", argv[i++]);
        }
        else if (stricmp(argv[i], "-wsdl") == 0)
        {
            i++;
            globals->setProp("wsdl", argv[i++]);
        }
        else if (stricmp(argv[i], "-r") == 0)
        {
            i++;
            globals->setProp("roxie", "1");
        }
        else if (stricmp(argv[i], "-ra") == 0)
        {
            i++;
            globals->setProp("alldataset", "1");
        }       
        else if(stricmp(argv[i], "-m") == 0)
        {
            i++;
            globals->setProp("method", argv[i++]);
        }
        else if(stricmp(argv[i], "-a") == 0)
        {
            i++;
            globals->setProp("abortEarly", "1");
        }
        else if(stricmp(argv[i], "-y") == 0)
        {
            i++;
            globals->setProp("useDefault", "1");
        }
        else if (stricmp(argv[i], "-soapaction")==0)
        {
            i++;
            if(argc < i+1)
            {
                printf("Please specify the value of soapaction.\n");
                return 0;
            }
            globals->setProp("soapaction", argv[i]);
            i++;
        }
        else if(stricmp(argv[i], "-diff") == 0)
        {
            i++;
            if(argc < i+2)
            {
                printf("Please specify 2 files/directories for diff\n");
                return 0;
            }
            isDiff = true;
            left = argv[i++];
            right = argv[i++];
        }
        else if(stricmp(argv[i], "-stress") == 0)
        {
            i++;
            if(argc < i+2)
            {
                printf("Please specify number-of-threads and test duration for stress test.\n");
                return 0;
            }
            isStress = true;
            tnumstr = argv[i++];
            dnumstr = argv[i++];
            if(atoi(tnumstr) > 0 && atoi(dnumstr) > 0)
            {
                globals->setProp("stressthreads", tnumstr);
                globals->setProp("stressduration", dnumstr);
            }
            else
            {
                printf("For stress test, number-of-threads and test duration should be positive numbers.\n");
                return 0;
            }
        }
        else if(stricmp(argv[i], "-persist") == 0)
        {
            i++;
            globals->setProp("isPersist", 1);
            if(argc > i)
            {
                const char* numstr = argv[i];
                if(numstr[0] >= '0' && numstr[0] <= '9')
                {
                    i++;
                    globals->setProp("persistrequests", numstr);
                    if(argc > i)
                    {
                        const char* secstr = argv[i];
                        if((secstr[0] >= '0' && secstr[0] <= '9') || secstr[0] == '.')
                        {
                            i++;
                            globals->setProp("persistpause", secstr);
                        }
                    }
                }
            }
        }
        else if(stricmp(argv[i], "-delay") == 0)
        {
            i++;
            if(argc < i+2)
            {
                printf("Please specify min and max milli-seconds with -delay option.\n");
                return 0;
            }
            
            if(!isNumber(argv[i]) || !isNumber(argv[i+1]))
            {
                printf("min and max milli-seconds must be positive integers. use -h option for usage.\n");
                return 0;
            }

            int min = atoi(argv[i]);
            int max = atoi(argv[i+1]);
            if(max < min)
            {
                printf("max must be greater or equal to min\n");
                return 0;
            }

            globals->setProp("delaymin", argv[i++]);
            globals->setProp("delaymax", argv[i++]);
        }
        else if(stricmp(argv[i], "-difflimit") == 0)
        {
            i++;
            if(argc < i+1)
            {
                printf("Please specify a non-negative number with -difflimit option.\n");
                return 0;
            }
            
            int difflimit = atoi(argv[i]);
            if(difflimit <= 0)
            {
                printf("difflimit must be an non-negative number. use -h option for usage.\n");
                return 0;
            }

            globals->setProp("difflimit", argv[i++]);
        }
        else if(stricmp(argv[i], "-ooo") == 0)
        {
            globals->setProp("ooo", "1");
            i++;
        }
        else if(stricmp(argv[i], "-ordsen") == 0)
        {
            globals->setProp("ordsen", "1");
            i++;
        }
        else if(stricmp(argv[i], "-wiz") == 0)
        {
            i++;
            globals->setProp("ECL2ESP", "1");
        }
        else if(stricmp(argv[i], "-l") == 0)
        {
            i++;
            in_fname = argv[i++];
            isEspLogFile = true;
        }
        else if(stricmp(argv[i], "-compress") == 0)
        {
            i++;
            globals->setProp("compress", "1");
        }
        else if(stricmp(argv[i], "-version") == 0)
        {
            printf("lexis-nexis soapplus version %s\n", version);
            return 0;
        }
        else
        {
            printf("Error: unknown command parameter: %s\n", argv[i]);
            usage();
            return 0;
        }
    }

    if(isDiff)
    {
        Owned<IPropertyTree> cfgtree = NULL;
        if(globals->hasProp("cfg"))
        {
            const char* cfg = globals->queryProp("cfg");
            try
            {
                cfgtree.setown(createPTreeFromXMLFile(cfg));
            }
            catch(IException* e)
            {
                StringBuffer ebuf;
                e->errorMessage(ebuf);
                printf("Error: exception loading xml file %s - %s\n", cfg, ebuf.str());
                return 0;
            }
            catch(...)
            {
                printf("Error: unknown exception loading xml file %s\n", cfg);
                return 0;
            }
        }

        //TimeSection ts("xmldiff");
        try
        {
            Owned<CXmlDiff> xmldiff = new CXmlDiff(globals, left, right, cfgtree.get());
            xmldiff->compare();
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            printf("%s\n", errmsg.str());
            e->Release();
        }

        return 0;
    }

    if(isStress)
    {
        if(action != SPA_CLIENT)
        {
            printf("stress test is only for client mode.\n");
            return 0;
        }

        if(!url)
        {
            printf("Please specify url for stress test.\n");
            return 0;
        }
        
        if(writeToFiles)
        {
            printf("For stress test, please don't write the results to files. Remove the -w option and try again.\n");
            return 0;
        }

        if(globals->hasProp("isPersist"))
        {
            if(globals->hasProp("persistrequests") || globals->hasProp("persistpause"))
            {
                printf("\nWarning: Will run the stress test with persistent connections, but parameters number-of-requests and pause-seconds will be ignored.\n\n");
                globals->removeProp("persistrequests");
                globals->removeProp("persistpause");
            }
        }
    }

    if (url == NULL && (action == SPA_CLIENT || action == SPA_BOTH) && !globals->getPropBool("autogen"))
    {
        if (!(doValidation && in_fname))
        {
            printf("Please specify url. Use \"%s -h\" for usage info.\n", argv[0]);
            return 0;
        }
    }

    if (globals->getPropBool("autogen") && (!url || !*url) && !globals->hasProp("wsdl") 
        && !globals->hasProp("roxie") && (!xsdpath || !*xsdpath) && !isEspLogFile)
    {
        printf("Please specify url or the path/url of the wsdl if for automatically generation.\n");
        return 0;
    }

    // default tracelevel set to 6 if writing to files.
    if(!tracelevelspecified)
    {
        if(isStress)
            http_tracelevel = 1;
        else if(writeToFiles)
            http_tracelevel = 5;        
    }

    try
    {
        processRequest(globals, action, url, in_fname, port, server_in_fname, out_dirname, writeToFiles, proxy_url_prefix, isEspLogFile, out_filename?out_filename:"get.txt");
    }
    catch(IException *excpt)
    {
        StringBuffer errMsg;
        printf("Exception: %d:%s\n", excpt->errorCode(), excpt->errorMessage(errMsg).str());
    }
    catch(...)
    {
        printf("Unknown exception\n");
    }
    releaseAtoms();

    return 0;
}

