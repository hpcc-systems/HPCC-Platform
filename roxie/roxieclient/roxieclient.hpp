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

#ifndef _ROXIECLIENT_HPP__
#define _ROXIECLIENT_HPP__

#ifndef ROXIECLIENT_API

#ifdef _WIN32
    #ifndef ROXIECLIENT_EXPORTS
        #define ROXIECLIENT_API __declspec(dllimport)
    #else
        #define ROXIECLIENT_API __declspec(dllexport)
    #endif //ROXIECLIENT_EXPORTS
#else
    #define ROXIECLIENT_API
#endif //_WIN32

#endif 

#include "jiface.hpp"
#include "jsmartsock.hpp"

const unsigned MAX_ROXIECLIENT_THREADS = 50;
const unsigned DEFAULT_RECORDSPERQUERY = 10000;
const unsigned BYTESPERBLOCK = 0x8000;

interface IRoxieClient: implements IInterface
{
    virtual void setInput(const char* id, unsigned in_width, IByteInputStream* istream) = 0;
    virtual void setOutput(const char* resultName, unsigned out_width, IByteOutputStream* ostream) = 0;
    virtual void setMaxRetries(unsigned retries) = 0;
    virtual void setReadTimeout(int readTimeout) = 0;
    virtual void runQuery(const char* query, bool trim=true) = 0;
    virtual void setRecordsPerQuery(unsigned recordsPerQuery) = 0;
};

extern "C" ROXIECLIENT_API IRoxieClient* createRoxieClient(ISmartSocketFactory* socketfactory, int numThreads, int maxretries=0);

#endif
