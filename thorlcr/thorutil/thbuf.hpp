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


extern graph_decl ISmartRowBuffer * createSmartInMemoryBuffer(CActivityBase *activity,
                                                      IRowInterfaces *rowIf,
                                                      size32_t buffsize);

// Multiple readers, one writer
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

// Multiple writers, one reader
interface IRowMultiWriterReader : extends IRowStream
{
    virtual IRowWriter *getWriter() = 0;
    virtual void abort() = 0;
};

#define DEFAULT_WR_READ_GRANULARITY 10000 // Amount reader extracts when empty to avoid contention with writer
#define DEFAULT_WR_WRITE_GRANULARITY 1000 // Amount writers buffer up before committing to output
IRowMultiWriterReader *createSharedWriteBuffer(CActivityBase *activity, IRowInterfaces *rowif, unsigned limit, unsigned readGranularity=DEFAULT_WR_READ_GRANULARITY, unsigned writeGranularity=DEFAULT_WR_WRITE_GRANULARITY);


#endif
