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

#ifndef __THBUF__
#define __THBUF__

#include "jiface.hpp"

#include "eclhelper.hpp"
#include "jqueue.tpp"
#include "jbuff.hpp"
#include "jcrc.hpp"
#include "thorcommon.hpp"
#include "thmem.hpp"


#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif


typedef QueueOf<const void,true> ThorRowQueue;





interface ISmartRowBuffer: extends IRowStream
{
    virtual IRowWriter *queryWriter() = 0;
};

class CActivityBase;
extern graph_decl ISmartRowBuffer * createSmartBuffer(CActivityBase *activity, const char * tempname, 
                                                      size32_t buffsize, 
                                                      IRowInterfaces *rowif
                                                      ); 


interface ISRBRowInterface: extends IInterface  
{
    virtual size32_t rowMemSize(const void *r)=0;
    virtual void releaseRow(const void *row)=0;
};



extern graph_decl ISmartRowBuffer * createSmartInMemoryBuffer(CActivityBase *activity,
                                                      IRowInterfaces *rowIf,
                                                      size32_t buffsize, 
                                                      ISRBRowInterface *srbrowif=NULL // only needed if not thor rows
                                                      ); 
interface ISharedSmartBuffer : extends IRowWriter
{
    virtual IRowStream *queryOutput(unsigned output) = 0;
    virtual void cancel()=0;
    virtual void reset() = 0;
};

extern graph_decl ISharedSmartBuffer *createSharedSmartMemBuffer(CActivityBase *activity, unsigned outputs, IRowInterfaces *rowif, unsigned buffSize=((unsigned)-1));
interface IDiskUsage;
extern graph_decl ISharedSmartBuffer *createSharedSmartDiskBuffer(CActivityBase *activity, const char *tempname, unsigned outputs, IRowInterfaces *rowif, IDiskUsage *iDiskUsage=NULL);


interface IRowWriterMultiReader : extends IRowWriter
{
    virtual IRowStream *getReader() = 0;
};

extern graph_decl IRowWriterMultiReader *createOverflowableBuffer(CActivityBase &activity, IRowInterfaces *rowif, bool grouped, bool shared=false, unsigned spillPriority=SPILL_PRIORITY_OVERFLOWABLE_BUFFER);
// NB first write all then read (not interleaved!)

#endif
