/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
