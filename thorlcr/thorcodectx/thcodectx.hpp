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

#ifndef __THCODECTX__
#define __THCODECTX__

#include "eclhelper.hpp"
#include "deftype.hpp"
#include "dadfs.hpp"
#include "thgraph.hpp"

#ifdef _WIN32
    #ifdef THORCODECTX_EXPORTS
        #define thcodectx_decl __declspec(dllexport)
    #else
        #define thcodectx_decl __declspec(dllimport)
    #endif
#else
    #define thcodectx_decl
#endif

interface ILoadedDllEntry;
interface IConstWUResult;
interface IWUResult;

class CJobBase;
class thcodectx_decl CThorCodeContextBase : public CSimpleInterface, implements ICodeContextExt
{
protected:
    Linked<IUserDescriptor> userDesc;
    ILoadedDllEntry &querySo;
    CJobBase &job;

    void expandLogicalName(StringBuffer & fullname, const char * logicalName);
    IConstWUResult * getResult(const char * name, unsigned sequence);
    IWUResult *updateResult(const char *name, unsigned sequence);

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorCodeContextBase(CJobBase &job, ILoadedDllEntry &_querySo, IUserDescriptor &_userDesc);

// ICodeContext
    virtual const char *loadResource(unsigned id);
    virtual char *getWuid();
    virtual char *getDaliServers();

    virtual char *getExpandLogicalName(const char * logicalName);
    virtual IUserDescriptor *queryUserDescriptor() { return userDesc; }

    virtual unsigned getRecoveringCount() { UNIMPLEMENTED; }        // don't know how to implement here!

    virtual unsigned getNodes() { assertex(false); return (unsigned)-1; }
    virtual unsigned getNodeNum() { assertex(!"getNodeNum should not be called on the master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) { assertex(false); return NULL; }
    virtual unsigned __int64 getFileOffset(const char *logicalName) { assertex(false); return 0; }
    virtual char *getJobName();
    virtual char *getJobOwner();
    virtual char *getClusterName();
    virtual unsigned getPriority() const { return 0; }
    virtual char *getPlatform() { return strdup("thor"); };
    virtual char *getEnv(const char *name, const char *defaultValue) const { return strdup(defaultValue); }
    virtual char *getOS()
    {
#ifdef _WIN32
        return strdup("windows");
#else
        return strdup("linux");
#endif
    }
    virtual unsigned logString(const char *text) const
    {
        if (text && *text)
        {
            DBGLOG("USER: %s", text);
            return strlen(text);
        }
        else
            return 0;
    }
    virtual const IContextLogger &queryContextLogger() const
    {
        return queryDummyContextLogger();
    }

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { UNIMPLEMENTED; }

    virtual char *getGroupName(); // thorlib.group()
    virtual char *queryIndexMetaData(char const * lfn, char const * xpath) { UNIMPLEMENTED; }
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const;
    virtual ILocalGraph *resolveLocalQuery(__int64 gid);
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal);
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags);
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) { throwUnexpected(); }
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) { throwUnexpected(); }
    virtual const void * fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
};

#endif
