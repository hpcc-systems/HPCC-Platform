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
    virtual void CTXLOGae(IException *E, const char *file, unsigned line, const char *prefix, const char *format, ...) const = 0;
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
  virtual size32_t printf(int seq, const char *format, ...) = 0;
  virtual size32_t fwrite(int seq, const void * data, size32_t size, size32_t count) = 0;
  virtual void close(int seq, bool nl) = 0;
};

extern ROXIEHELPER_API IOrderedOutputSerializer * createOrderedOutputSerializer(FILE * _outFile);

#endif // ROXIEHELPER_IPP
