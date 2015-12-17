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

#ifndef SLAVE_HPP
#define SLAVE_HPP

#ifdef _WIN32
 #ifdef ACTIVITYSLAVES_EXPORTS
  #define activityslaves_decl __declspec(dllexport)
 #else
  #define activityslaves_decl __declspec(dllimport)
 #endif
#else
 #define activityslaves_decl
#endif

#include "jio.hpp"
#include "jsocket.hpp"
#include "slavmain.hpp"
#include "thor.hpp"

#include "eclhelper.hpp"        // for IRecordSize
#include "thgraph.hpp"
#include "thorstep.hpp"
#include "roxiestream.hpp"


/* ---- To implement IThorDataLink you need ----
    virtual const void *nextRow() = 0;
    virtual void stop();
    virtual void start();
    virtual bool isGrouped();
    virtual bool getMetaInfo(ThorDataLinkMetaInfo &info);
*/


struct ThorDataLinkMetaInfo
{
    __int64     totalRowsMin;           // set to 0 if not known
    __int64     totalRowsMax;           // set to -1 if not known
    rowcount_t  rowsOutput;             // rows already output (supported by all data links)
    offset_t    spilled;                // amount "spilled" to disk (approx) (offset_t)-1 for not known

    bool        isSource;
    bool        isSequential;
    bool        canStall;
    bool        fastThrough;
    bool        buffersInput;
    bool        canBufferInput;
    bool        singleRowOutput;
    bool        canIncreaseNumRows;
    bool        canReduceNumRows;
    bool        unknownRowsOutput;      // cannot use input to deduce total
    offset_t    byteTotal;                  // total (uncompressed) byte count of all rows
};

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4275 )
#endif
class CActivityBase;

interface IThorDataLink : extends IEngineRowStream
{
    virtual void start() = 0;
    virtual bool isGrouped() = 0;
    virtual IInputSteppingMeta *querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual void resetEOF() { }

// information routines 
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual CActivityBase *queryFromActivity() = 0; // activity that has this as an output
    virtual void dataLinkSerialize(MemoryBuffer &mb)=0;
    virtual unsigned __int64 queryTotalCycles() const=0;
    virtual unsigned __int64 queryEndCycles() const=0;
    virtual void debugRequest(MemoryBuffer &mb) = 0;
};
#ifdef _MSC_VER
#pragma warning (pop)
#endif


// utility redirects
extern activityslaves_decl IRowInterfaces * queryRowInterfaces(IThorDataLink *link);
extern activityslaves_decl IEngineRowAllocator * queryRowAllocator(IThorDataLink *link);
extern activityslaves_decl IOutputRowSerializer * queryRowSerializer(IThorDataLink *link);
extern activityslaves_decl IOutputRowDeserializer * queryRowDeserializer(IThorDataLink *link);
extern activityslaves_decl IOutputMetaData *queryRowMetaData(IThorDataLink *link);
extern activityslaves_decl unsigned queryActivityId(IThorDataLink *link);
extern activityslaves_decl ICodeContext *queryCodeContext(IThorDataLink *link);


extern activityslaves_decl void dummyProc();

#endif


