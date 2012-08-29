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

#ifndef MCORECACHE_HPP
#define MCORECACHE_HPP


#include "jiface.hpp"
#include "jexcept.hpp"

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif

interface IMultiCoreRowIntercept: extends IInterface
{
    virtual bool serialGet(size32_t maxsize, void * dst, size32_t &retsize) = 0;
    virtual bool parallelGet(size32_t maxsize, void * dst, size32_t &retsize) = 0;
};

interface IMultiCoreCache: extends IMultiCoreRowIntercept
{
    virtual void start()=0;
};


/*
before:
   this->get calls in->get
after:
   mcc = createMultiCoreCache(this)
   this->get calls mcc->serialGet
   mcc->serialGet calls this->parallelGet
   this->parallelGet calls mcc->parallelGet
   mcc->parallelGet calls in->get
*/


extern graph_decl IMultiCoreCache *createMultiCoreCache(IMultiCoreRowIntercept &wrapped, IRecordSize &recsize);

#endif
