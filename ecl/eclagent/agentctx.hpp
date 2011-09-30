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
#ifndef AGENTCTX_HPP_INCL
#define AGENTCTX_HPP_INCL

#include "dautils.hpp"
#include "eclhelper.hpp"
#include "workunit.hpp"
#include "layouttrans.hpp"

struct IHThorGraphResult : extends IInterface
{
    virtual void addRowOwn(const void * row) = 0;
    virtual const void * queryRow(unsigned whichRow) = 0;
    virtual void getResult(unsigned & len, void * & data) = 0;
    virtual void getLinkedResult(unsigned & count, byte * * & ret) = 0;
    virtual const void * getOwnRow(unsigned whichRow) = 0;      // used internally, removes row from result
};

struct IHThorGraphResults : extends IEclGraphResults
{
    virtual void clear() = 0;
    virtual IHThorGraphResult * queryResult(unsigned id) = 0;
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator) = 0;
    virtual IHThorGraphResult * createResult(IEngineRowAllocator * ownedRowsetAllocator) = 0;
    virtual void setResult(unsigned id, IHThorGraphResult * result) = 0;
    virtual int ordinality() = 0;
};

struct IHThorBoundLoopGraph : extends IInterface
{
    virtual IHThorGraphResult * execute(void * counterRow, ConstPointerArray & rows, const byte * parentExtract) = 0;
    virtual void execute(void * counterRow, IHThorGraphResults * graphLoopResults, const byte * parentExtract) = 0;
};

struct IEclLoopGraph : public IInterface
{
    virtual void executeChild(const byte * parentExtract, IHThorGraphResults * results, IHThorGraphResults * _graphLoopResults) = 0;
};

struct ILocalGraphEx : public ILocalGraph
{
public:
    virtual IHThorGraphResult * queryResult(unsigned id) = 0;
    virtual IHThorGraphResult * queryGraphLoopResult(unsigned id) = 0;
    virtual IHThorGraphResult * createResult(unsigned id, IEngineRowAllocator * ownedRowsetAllocator) = 0;
    virtual IHThorGraphResult * createGraphLoopResult(IEngineRowAllocator * ownedRowsetAllocator) = 0;
};

interface IOrderedOutputSerializer;

typedef enum { ofSTD, ofXML, ofRAW } outputFmts;

struct IAgentContext : extends IGlobalCodeContext
{
    virtual void reportProgress(const char *msg, unsigned flags=0) = 0;
    virtual bool queryResolveFilesLocally() = 0;
    virtual bool queryRemoteWorkunit() = 0;
    virtual bool queryWriteResultsToStdout() = 0;
    virtual outputFmts queryOutputFmt() = 0;

    virtual ICodeContext *queryCodeContext() = 0;

    virtual IConstWorkUnit *queryWorkUnit() = 0;
    virtual IWorkUnit *updateWorkUnit() = 0;
    virtual void unlockWorkUnit() = 0;
    
    virtual ILocalOrDistributedFile *resolveLFN(const char *logicalName, const char *errorTxt=NULL, bool optional=false, bool noteRead=true, bool write=false, StringBuffer * expandedlfn=NULL) = 0;
    virtual StringBuffer & getTempfileBase(StringBuffer & buff) = 0;
    virtual const char *noteTemporaryFile(const char *fname) = 0;
    virtual const char *noteTemporaryFilespec(const char *fname) = 0;
    virtual const char *queryTemporaryFile(const char *fname) = 0;
    virtual void reloadWorkUnit() = 0;

    virtual char *resolveName(const char *in, char *out, unsigned outlen) = 0;
    virtual void logFileAccess(IDistributedFile * file, char const * component, char const * type) = 0;
    virtual IRecordLayoutTranslatorCache * queryRecordLayoutTranslatorCache() const = 0;
    virtual void addWuException(const char * text, unsigned code, unsigned severity, char const * source) = 0;

    virtual IHThorGraphResults * executeLibraryGraph(const char * libraryName, unsigned expectedInterfaceHash, unsigned activityId, bool embedded, const byte * parentExtract) = 0;
    virtual bool getWorkunitResultFilename(StringBuffer & diskFilename, const char * wuid, const char * name, int seq) = 0;
    virtual IHThorGraphResults * createGraphLoopResults() = 0;
    virtual void outputFormattedResult(const char *name, unsigned sequence, bool close) = 0;

    virtual const char *queryAllowedPipePrograms() = 0;
    
    virtual IOrderedOutputSerializer * queryOutputSerializer() = 0;

    virtual IGroup *getHThorGroup(StringBuffer &grpnameout) = 0;

    virtual unsigned __int64 queryStopAfter() = 0;
    
    virtual const char *queryWuid() = 0;
};

struct WorkunitUpdate : public Owned<IWorkUnit>
{
public:
    WorkunitUpdate(IWorkUnit *wu) : Owned<IWorkUnit>(wu) { }
    ~WorkunitUpdate() { if (get()) get()->commit(); }
};

#endif // AGENTCTX_HPP_INCL
