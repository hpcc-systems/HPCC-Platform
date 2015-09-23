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

class thcodectx_decl CThorCodeContextBase : public CSimpleInterface, implements ICodeContextExt
{
protected:
    Linked<IUserDescriptor> userDesc;
    ILoadedDllEntry &querySo;
    CJobChannel &jobChannel;

    void expandLogicalName(StringBuffer & fullname, const char * logicalName);
    IConstWUResult * getResult(const char * name, unsigned sequence);
    IWUResult *updateResult(const char *name, unsigned sequence);

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThorCodeContextBase(CJobChannel &jobChannel, ILoadedDllEntry &_querySo, IUserDescriptor &_userDesc);

// ICodeContext
    virtual const char *loadResource(unsigned id);
    virtual char *getWuid();

    virtual char *getExpandLogicalName(const char * logicalName);
    virtual IUserDescriptor *queryUserDescriptor() { return userDesc; }

    virtual unsigned getNodes() { assertex(false); return (unsigned)-1; }
    virtual unsigned getNodeNum() { assertex(!"getNodeNum should not be called on the master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) { assertex(false); return NULL; }
    virtual unsigned __int64 getFileOffset(const char *logicalName) { assertex(false); return 0; }
    virtual char *getJobName();
    virtual char *getJobOwner();
    virtual char *getClusterName();
    virtual unsigned getPriority() const { return 0; }
    virtual char *getPlatform() { return strdup("thor"); };
    virtual char *getEnv(const char *name, const char *defaultValue) const
    {
        const char *val = getenv(name);
        if (val)
            return strdup(val);
        else if (defaultValue)
            return strdup(defaultValue);
        else
            return strdup("");
    }
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
        return jobChannel.queryJob().queryContextLogger();
    }

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) { UNIMPLEMENTED; }

    virtual char *getGroupName(); // thorlib.group()
    virtual char *queryIndexMetaData(char const * lfn, char const * xpath) { UNIMPLEMENTED; }
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const;
    virtual const char *cloneVString(const char *str) const;
    virtual const char *cloneVString(size32_t len, const char *str) const;
    virtual IEclGraphResults *resolveLocalQuery(__int64 gid);
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal);
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags);
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags);
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) { throwUnexpected(); }
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) { throwUnexpected(); }
    virtual const void * fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
    virtual const void * fromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace);
    virtual IEngineContext *queryEngineContext() { return NULL; }
    virtual char *getDaliServers();
    virtual IWorkUnit *updateWorkUnit() const { throwUnexpected(); }
};

#endif
