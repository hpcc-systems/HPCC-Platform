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

#ifndef ROXIEHELPER_IPP
#define ROXIEHELPER_IPP

#include "thorhelper.hpp"
#include "roxiestream.hpp"
#include "jlog.hpp"

extern THORHELPER_API unsigned traceLevel;
interface IOutputMetaData;
interface IInputSteppingMeta;

struct IInputBase : public IInterface //base for IRoxieInput and IHThorInput
{
    virtual IOutputMetaData * queryOutputMeta() const = 0;
    virtual IInputSteppingMeta * querySteppingMeta() { return NULL; }
    virtual void resetEOF() = 0;

    // These will need some thought
    virtual IEngineRowStream &queryStream() = 0;
    inline bool nextGroup(ConstPointerArray & group) { return queryStream().nextGroup(group); }
    inline void readAll(RtlLinkedDatasetBuilder &builder) { return queryStream().readAll(builder); }
    inline const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra) { return queryStream().nextRowGE(seek, numFields, wasCompleteMatch, stepExtra); }
    inline const void *nextRow() { return queryStream().nextRow(); }
    inline void stop() { queryStream().stop(); }
    inline const void *ungroupedNextRow() { return queryStream().ungroupedNextRow(); }

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
    LOG_STATVALUES,
    LOG_CHILDSTATS,
    LOG_CHILDCOUNT,
};

class LogItem;
interface IRoxieContextLogger : extends IContextLogger
{
    // Override base interface with versions that add prefix
    // We could consider moving some or all of these down into IContextLogger
    virtual void CTXLOGva(const char *format, va_list args) const  __attribute__((format(printf,2,0)))
    {
        StringBuffer text, prefix;
        getLogPrefix(prefix);
        text.valist_appendf(format, args);
        CTXLOGa(LOG_TRACING, prefix.str(), text.str());
    }
    virtual void logOperatorExceptionVA(IException *E, const char *file, unsigned line, const char *format, va_list args) const  __attribute__((format(printf,5,0)))
    {
        StringBuffer prefix;
        getLogPrefix(prefix);
        CTXLOGaeva(E, file, line, prefix.str(), format, args);
    }

    virtual StringBuffer &getLogPrefix(StringBuffer &ret) const = 0;
    virtual bool isIntercepted() const = 0;
    virtual void CTXLOGa(TracingCategory category, const char *prefix, const char *text) const = 0;
    void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const __attribute__((format(printf, 6, 7)));
    virtual void CTXLOGaeva(IException *E, const char *file, unsigned line, const char *prefix, const char *format, va_list args) const = 0;
    virtual void CTXLOGl(LogItem *) const = 0;
    virtual bool isBlind() const = 0;
    virtual const CRuntimeStatisticCollection &queryStats() const = 0;
};

//===================================================================================

//IRHLimitedCompareHelper copied from THOR ILimitedCompareHelper, and modified to get input from IHThorInput instead of IReadSeqVar
// Can probably common back up now
class OwnedRowArray;
interface ICompare;
interface IRHLimitedCompareHelper: public IInterface
{
    virtual void init(
            unsigned atmost,
            IRowStream *strm,
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

extern THORHELPER_API IOrderedOutputSerializer * createOrderedOutputSerializer(FILE * _outFile);

#endif // ROXIEHELPER_IPP
