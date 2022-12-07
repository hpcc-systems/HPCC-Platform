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

#ifdef THORCODECTX_EXPORTS
    #define thcodectx_decl DECL_EXPORT
#else
    #define thcodectx_decl DECL_IMPORT
#endif

interface ILoadedDllEntry;
interface IConstWUResult;
interface IWUResult;

class thcodectx_decl CThorCodeContextBase : public CSimpleInterfaceOf<ICodeContextExt>
{
protected:
    Linked<IUserDescriptor> userDesc;
    ILoadedDllEntry &querySo;
    CJobChannel &jobChannel;

    void expandLogicalName(StringBuffer & fullname, const char * logicalName);
    IConstWUResult * getResult(const char * name, unsigned sequence);
    IWUResult *updateResult(const char *name, unsigned sequence);

public:
    CThorCodeContextBase(CJobChannel &jobChannel, ILoadedDllEntry &_querySo, IUserDescriptor &_userDesc);
    IMPLEMENT_IINTERFACE_USING(CSimpleInterfaceOf<ICodeContextExt>) // This is strangely required by visual studio to ensure Release() is resolved
    virtual void gatherStats(CRuntimeStatisticCollection &mergedStats) const override { throwUnexpected(); }

// ICodeContext
    virtual const char *loadResource(unsigned id) override;
    virtual char *getWuid() override;

    virtual char *getExpandLogicalName(const char * logicalName) override;
    virtual IUserDescriptor *queryUserDescriptor() override { return userDesc; }

    virtual unsigned getNodes() override { assertex(false); return (unsigned)-1; }
    virtual unsigned getNodeNum() override { assertex(!"getNodeNum should not be called on the master"); return (unsigned)-1; }
    virtual char *getFilePart(const char *logicalName, bool create=false) override { assertex(false); return NULL; }
    virtual unsigned __int64 getFileOffset(const char *logicalName) override { assertex(false); return 0; }
    virtual char *getJobName() override;
    virtual char *getJobOwner() override;
    virtual char *getClusterName() override;
    virtual unsigned getPriority() const override { return 0; }
    virtual char *getPlatform() override { return strdup("thor"); };
    virtual char *getEnv(const char *name, const char *defaultValue) const override
    {
        const char *val = getenv(name);
        if (val)
            return strdup(val);
        else if (defaultValue)
            return strdup(defaultValue);
        else
            return strdup("");
    }
    virtual char *getOS() override
    {
#ifdef _WIN32
        return strdup("windows");
#else
        return strdup("linux");
#endif
    }
    virtual unsigned logString(const char *text) const override
    {
        if (text && *text)
        {
            DBGLOG("USER: %s", text);
            return strlen(text);
        }
        else
            return 0;
    }
    virtual const IContextLogger &queryContextLogger() const override
    {
        return jobChannel.queryJob().queryContextLogger();
    }

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) override { UNIMPLEMENTED; }

    virtual char *getGroupName() override; // thorlib.group()
    virtual char *queryIndexMetaData(char const * lfn, char const * xpath) override { UNIMPLEMENTED; }
    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const override;
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned heapFlags) const override;
    virtual const char *cloneVString(const char *str) const override;
    virtual const char *cloneVString(size32_t len, const char *str) const override;
    virtual IEclGraphResults *resolveLocalQuery(__int64 gid) override;
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal) override;
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) override;
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) override;
    virtual IConstWUResult *getExternalResult(const char * wuid, const char *name, unsigned sequence) override { throwUnexpected(); }
    virtual IConstWUResult *getResultForGet(const char *name, unsigned sequence) override { throwUnexpected(); }
    virtual const void * fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) override;
    virtual const void * fromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) override;
    virtual IEngineContext *queryEngineContext() override { return NULL; }
    virtual char *getDaliServers() override;
    virtual IWorkUnit *updateWorkUnit() const override { throwUnexpected(); }
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name) override;
};

#endif
