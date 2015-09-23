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

#include "jiface.hpp"
#include "jmisc.hpp"
#include "deftype.hpp"
#include "workunit.hpp"
#include "thexception.hpp"
#include "thormisc.hpp"
#include "eclrtl.hpp"
#include "thcodectx.hpp"
#include "dacoven.hpp"
#include "dasess.hpp"
#include "dadfs.hpp"
#include "thorxmlread.hpp"
#include "thgraph.hpp"
#include "thorxmlwrite.hpp"

CThorCodeContextBase::CThorCodeContextBase(CJobChannel &_jobChannel, ILoadedDllEntry &_querySo, IUserDescriptor &_userDesc) : jobChannel(_jobChannel), querySo(_querySo), userDesc(&_userDesc)
{
}

char *CThorCodeContextBase::getWuid()
{
    StringBuffer out;
    out.append(jobChannel.queryJob().queryWuid());
    return out.detach();
}

char *CThorCodeContextBase::getJobName()
{
    throwUnexpected();
    return NULL;
}

char *CThorCodeContextBase::getJobOwner()
{
    StringBuffer out;
    out.append(jobChannel.queryJob().queryUser());
    return out.detach();
}

char *CThorCodeContextBase::getClusterName()
{
    throwUnexpected();
    return NULL;
}

char *CThorCodeContextBase::getGroupName()
{
    throwUnexpected();
    return NULL;
}

const char *CThorCodeContextBase::loadResource(unsigned id)
{
    return (const char *) querySo.getResource(id);
}

char *CThorCodeContextBase::getDaliServers()
{
    StringBuffer dali;
    IGroup &group = queryCoven().queryComm().queryGroup();
    Owned<INodeIterator> coven = group.getIterator();
    bool first = true;
    ForEach(*coven)
    {
        if (first)
            first = false;
        else
            dali.append(',');
        coven->query().endpoint().getUrlStr(dali);
    }
    return dali.detach();
}

void CThorCodeContextBase::expandLogicalName(StringBuffer & fullname, const char * logicalName)
{
    if (logicalName[0]=='~')
        logicalName++;
    else
    {
        if (jobChannel.queryJob().queryScope())
            fullname.append(jobChannel.queryJob().queryScope()).append("::");
    }
    fullname.append(logicalName);
    fullname.toLowerCase();
}

char *CThorCodeContextBase::getExpandLogicalName(const char * logicalName)
{
    StringBuffer lfn;
    expandLogicalName(lfn, logicalName);
    return lfn.detach();
}

IEngineRowAllocator * CThorCodeContextBase::getRowAllocator(IOutputMetaData * meta, unsigned activityId) const
{ 
    return jobChannel.queryJob().getRowAllocator(meta, activityId);
}

const char * CThorCodeContextBase::cloneVString(const char * str) const
{
    return jobChannel.queryJob().queryRowManager()->cloneVString(str);
}

const char * CThorCodeContextBase::cloneVString(size32_t len, const char * str) const
{
    return jobChannel.queryJob().queryRowManager()->cloneVString(len, str);
}

IEclGraphResults *CThorCodeContextBase::resolveLocalQuery(__int64 gid)
{
    IEclGraphResults *graph = jobChannel.getGraph((graph_id)gid);
    graph->Release(); // resolveLocalQuery doesn't own, can't otherwise will be circular ref.
    return graph;
}

IThorChildGraph *CThorCodeContextBase::resolveChildQuery(__int64 gid, IHThorArg *colocal)
{
    return jobChannel.getGraph((graph_id)gid);
}

void CThorCodeContextBase::getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    convertRowToXML(lenResult, result, info, row, flags);
}

void CThorCodeContextBase::getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    convertRowToJSON(lenResult, result, info, row, flags);
}

const void * CThorCodeContextBase::fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return createRowFromXml(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}

const void * CThorCodeContextBase::fromJson(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return createRowFromJson(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}
