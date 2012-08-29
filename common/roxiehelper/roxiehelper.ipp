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

#ifndef ROXIEHELPER_IPP
#define ROXIEHELPER_IPP


#ifdef _WIN32
#ifdef ROXIEHELPER_EXPORTS
#define ROXIEHELPER_API __declspec(dllexport)
#else
#define ROXIEHELPER_API __declspec(dllimport)
#endif
#else
#define ROXIEHELPER_API
#endif

#include "jlog.hpp"

extern ROXIEHELPER_API unsigned traceLevel;

//#pragma message("**** ROXIEHELPER.IPP ***")

//---------------------------------------------------
// Base classes for all Roxie/HThor activities
//---------------------------------------------------
struct ISimpleInputBase //base for IInputBase and IHThorSimpleInput
{
    virtual const void * nextInGroup() = 0;     // return NULL for eog/eof
};

interface IOutputMetaData;
struct IInputBase : public ISimpleInputBase  //base for IRoxieInput and IHThorInput
{
    virtual IOutputMetaData * queryOutputMeta() const = 0;
};

//---------------------------------------------------
// Base class for all Roxie/HThor activities
//---------------------------------------------------
struct IActivityBase : public IInterface    //Base for IHThorActivity and IRoxieServerActivity
{
    virtual unsigned queryId() const {throwUnexpected();};
    virtual bool isSink() const {throwUnexpected();};
    virtual ThorActivityKind getKind() const {throwUnexpected();};
    virtual bool isPassThrough() {throwUnexpected();};
    virtual unsigned __int64 queryTotalCycles() const { return 0; }
    virtual unsigned __int64 queryLocalCycles() const { return 0; }
};

//==============================================================================================================

enum TracingCategory
{
    LOG_TRACING,
    LOG_ERROR,
    LOG_TIMING,
    LOG_STATISTICS,
    LOG_STATVALUES,
};

class LogItem;
interface IRoxieContextLogger : extends IContextLogger
{
    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const = 0;
    virtual bool isIntercepted() const = 0;
    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const = 0;
    virtual void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const __attribute__((format(printf, 6, 7))) = 0;
    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const = 0;
    virtual void CTXLOGl(LogItem *) const = 0;
    virtual bool isBlind() const = 0;
};

//===================================================================================

//IRHLimitedCompareHelper copied from THOR ILimitedCompareHelper, and modified to get input from IHThorInput instead of IReadSeqVar
class OwnedRowArray;
interface ICompare;
interface IRHLimitedCompareHelper: public IInterface
{
    virtual void init(
            unsigned atmost,
            IInputBase *strm,
            ICompare *compare,
            ICompare *limcompare
        )=0;

    virtual bool getGroup(OwnedRowArray &group,const void *left) = 0;
};

//===================================================================================

interface IOrderedOutputSerializer : extends IInterface 
{
  virtual size32_t printf(int seq, const char *format, ...) __attribute__((format(printf, 3, 4)))= 0;
  virtual size32_t fwrite(int seq, const void * data, size32_t size, size32_t count) = 0;
  virtual void close(int seq, bool nl) = 0;
};

extern ROXIEHELPER_API IOrderedOutputSerializer * createOrderedOutputSerializer(FILE * _outFile);

#endif // ROXIEHELPER_IPP
