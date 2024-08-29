/*##############################################################################

    Copyright (C) 2024 HPCC Systems®.

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
#include "jargv.hpp"
#include "jstring.hpp"
#include "espshell.hpp"
#include "jargv.hpp"
#include <cstdio>
#include <cstring>
#include <iostream>
#include "espservice.hpp"

using namespace std;
void EspShell::usage()
{
    fprintf(stdout,
    "\nUsage:\n"
    "    esp-api describe [<ServiceName> [<MethodName>]]\n"
    "    esp-api test <ServiceName> <MethodName> [options] --reqStr <request>\n\n"
    "Commands:\n"
    "    describe                                        Describes ESP APIs; no arguments describes available services.\n"
    "        <ServiceName>                               Optional: Specify to describe all methods in the service.\n"
    "        <MethodName>                                Optional: Specify to describe the request and response structure for the method.\n"
    "\n"
    "    test                                            Sends a request to the specified method of a remote or local ESP.\n"
    "        <ServiceName>                               Required: Specify the service name.\n"
    "        <MethodName>                                Required: Specify the method name.\n"
    "        --reqStr <request>                          Required: Specify the request string in XML, JSON, or form format.\n"
    "        --server, -s                                Optional: IP of the server running the ESP\n"
    "        --port, -p                                  Optional: ESP port\n"
    "        --username, -u                              Optional: Username for a secure ESP\n"
    "        --password, -pw                             Optional: Password for a secure ESP\n"
    "        --query, -q                                 Optional: Query parameters to be appended to the url\n"
    "        --resTypeForm                               Optional: Set the response type for the form method of the request to xml or json"
    "\n"
    "Examples:\n"
    "    esp-api describe                                Describes all services.\n"
    "    esp-api describe WsWorkunits                    Describes all methods in the 'WsWorkunits' service.\n"
    "    esp-api describe WsWorkunits WUDelete           Describes the request and response structure of the 'WUDelete' method.\n"
    "\n"
    "    esp-api test WsWorkunits WUDelete --username <username> --password <password> "
    "--server http://127.0.0.1 --port 8010 --query ver_=1.2 --reqStr '{\"WUDeleteRequest\":{\"Wuids\":{\"Item\":[\"test\"]},\"BlockTillFinishTimer\":0}}'\n"
    "Note:\n"
    "    To autocomplete the service and method names, use the 'Tab' key after typing the command.\n\n"
);
}

string EspShell::buildUrl(string &url, const char* server, const char* port, const char* query, const char* resType, const char* serviceName, const char* methodName)
{
    url += server;
    url += ":";
    url += port;
    url += "/";
    url += serviceName;
    url += "/";
    url += methodName;
    url += resType;
    if (!isEmptyString(query))
    {
        url += "?";
        url += query;
    }
    return url;
}

int EspShell::sendRequest(const char* serviceName, const char* methodName, const char* reqString, const char* resType, const char* reqType, const char* server
,const char* port, const char* query)
{
    string url;
    buildUrl(url, server, port, query, resType, serviceName, methodName);
    EspService myServ(serviceName, methodName, reqString, resType, reqType, url.c_str(), username.str(), password.str());
    return myServ.sendRequest();
}

int EspShell::callService()
{
    if(hasHelp)
    {
        usage();
        return 0;
    }
    if(hasListServices)
    {
        esdlDefObj.printAllServices();
        return 0;
    }
    if(hasListMethods)
    {
        if(hasServiceName && esdlDefObj.checkValidService(serviceName))
        {
            esdlDefObj.loadAllMethods(serviceName);
            esdlDefObj.printAllMethods();
            return 0;
        }
        else
        {
            cerr << "Invalid Service Name" << endl;
            return 1;
        }
    }
    if(hasDescribe)
    {
        if(hasServiceName && !hasServiceAndMethodName)
        {
            esdlDefObj.describeAllMethods(serviceName, cout);
            return 0;
        }
        else if(!hasServiceName)
        {
            esdlDefObj.describeAllServices(cout);
            return 0;
        }
    }

    bool isValidServiceAndMethod = argc > 3 && serviceName != nullptr && methodName != nullptr && esdlDefObj.checkValidService(serviceName) && esdlDefObj.checkValidMethod(methodName, serviceName);
    if(isValidServiceAndMethod)
    {
        if(hasDescribe)
        {
            esdlDefObj.describe(serviceName, methodName);
            return 0;
        }
        //////////////////////////// CALLING REQUESTS
        if(hasTest)
        {
            if(!hasReqStr)
            {
                cerr << "request string not provided" << endl;
                return 1;
            }
            const char* reqString = reqStr.str();
            const char* reqType;
            const char* resType;

            if(reqStr[0] == '{')
            {
                reqType = "json";
                resType = ".json";
            }
            else if(reqStr[0] == '<')
            {
                reqType = "xml";
                resType = ".xml";
            }
            else
            {
                reqType = "form";
                resType = ".json";
                if(hasResponseType)
                {
                    resType = (streq(responseType, "xml")) ? ".xml" : ".json";
                }
            }
            if(!hasServer) server = "http://127.0.0.1";
            if(!hasPort) port = "8010";

            int resCode = sendRequest(serviceName, methodName, reqString, resType, reqType, server.str(), port.str(), query.str());
            return resCode;
        }
    }
    else
    {
        std::cout << "Not a Valid Service and Method" << std::endl;
    }
    usage();
    return 1;

}

bool EspShell::parseCmdOptions()
{
    if (args.done())
    {
        usage();
        return false;
    }
    while(args.isValid())
    {
        if(args.matchFlag(hasHelp, "--help"))
        {
            hasHelp = true;
            break;
        }
        if(args.matchFlag(hasDescribe, "describe") || args.matchFlag(hasTest, "test"))
        {
            if(args.next())
            {
                serviceName = argv[2];
                hasServiceName = true;
                if(args.next())
                {
                    hasServiceAndMethodName = true;
                    methodName = argv[3];
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
            continue;
        }
        if(args.matchFlag(hasListServices, "list-services"))
        {
            args.next();
            continue;
        }
        if(args.matchFlag(hasListMethods, "list-methods"))
        {
            if(args.next())
            {
                serviceName = argv[2];
                hasServiceName = true;
            }
            else
            {
                break;
            }
        }
        if(args.matchOption(server,"--server") || args.matchOption(server,"-s"))
        {
            hasServer = true;
            args.next();
            continue;
        }
        if(args.matchOption(reqStr, "--reqStr"))
        {
            hasReqStr = true;
            args.next();
            continue;
        }
        if(args.matchOption(responseType, "--resTypeForm"))
        {
            hasResponseType = true;
            args.next();
            continue;
        }
        if(args.matchOption(query, "--query") || args.matchOption(query,"-q"))
        {
            hasQuery = true;
            args.next();
            continue;
        }
        if(args.matchOption(port, "--port") || args.matchOption(port,"-p"))
        {
            hasPort = true;
            args.next();
            continue;
        }
        if(args.matchOption(username, "-u") || args.matchOption(username, "--username"))
        {
            hasUsername = true;
            args.next();
            continue;
        }
        if(args.matchOption(password, "-pw") || args.matchOption(password, "--password"))
        {
            hasPassword = true;
            args.next();
            continue;
        }
        args.next();
    }
    return true;
}

EspShell::EspShell(int argc, const char* argv[]) : args(argc, argv), esdlDefObj(), argc(argc), argv(argv)
{
    std::vector<Owned<IFile>> files;
    std::vector<const char*> allServicesList;
    esdlDefObj.getFiles(files);
    esdlDefObj.addFilesToDefinition(files);
    esdlDefObj.loadAllServices();
}
