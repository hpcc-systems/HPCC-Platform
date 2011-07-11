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
