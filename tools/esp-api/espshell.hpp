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
#ifndef HPCC_SHELL_H
#define HPCC_SHELL_H
#include "jargv.hpp"
#include "espapi.hpp"
class EspShell {
    public:
        EspShell(int argc, const char* argv[]);
        int callService();
        bool parseCmdOptions();
    private:
        ArgvIterator args;
        EspDef esdlDefObj;
        int argc;
        const char** argv;

        void usage();

        std::string buildUrl(std::string &url, const char* server, const char* port, const char* query, const char* resType, const char* serviceName, const char* methodName);

        int sendRequest(const char* serviceName, const char* methodName, const char* reqString, const char* resType, const char* reqType, const char* targeturl
        ,const char* port, const char* query);

        bool hasValidService=false, hasValidMethod=false, hasDescribe=false;
        bool hasListServices = false, hasListMethods = false, hasHelp=false;
        bool hasServer = false, hasRequestType = false, hasResponseType = false;
        bool hasUsername = false, hasPassword = false, hasReqStr = false;
        bool hasServiceName = false, hasServiceAndMethodName = false;
        bool hasQuery = false;
        bool hasPort = false;
        bool hasTest = false;
        bool hasTerse = false;

        const char* serviceName = nullptr;
        const char* methodName = nullptr;

        StringAttr username, password, reqStr, requestType, responseType, server, port, query;
};

#endif
