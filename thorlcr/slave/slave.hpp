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

interface IThorDataLink : extends IRowStream
{
    virtual void start() = 0;
    virtual bool isGrouped() = 0;
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) { throwUnexpected(); }    // can only be called on stepping fields.
    virtual IInputSteppingMeta *querySteppingMeta() { return NULL; }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return false; }
    virtual void resetEOF() { }

// information routines 
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
    virtual CActivityBase *queryFromActivity() = 0; // activity that has this as an output
    virtual void dataLinkSerialize(MemoryBuffer &mb)=0;
    virtual unsigned __int64 queryTotalCycles() const=0;
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


