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
#include "thmem.hpp"
#include "thgraph.hpp"
#include "thorxmlwrite.hpp"

CThorCodeContextBase::CThorCodeContextBase(CJobBase &_job, ILoadedDllEntry &_querySo, IUserDescriptor &_userDesc) : job(_job), querySo(_querySo), userDesc(&_userDesc)
{
}

char *CThorCodeContextBase::getWuid()
{
    StringBuffer out;
    out.append(job.queryWuid());
    return out.detach();
}

const char *CThorCodeContextBase::queryWuid()
{
    return job.queryWuid();
}

char *CThorCodeContextBase::getJobName()
{
    throwUnexpected();
    return NULL;
}

char *CThorCodeContextBase::getJobOwner()
{
    StringBuffer out;
    out.append(job.queryUser());
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

const char *CThorCodeContextBase::loadResource(unsigned id)
{
    return (const char *) querySo.getResource(id);
}


void CThorCodeContextBase::expandLogicalName(StringBuffer & fullname, const char * logicalName)
{
    if (logicalName[0]=='~')
        logicalName++;
    else
    {
        if (job.queryScope())
            fullname.append(job.queryScope()).append("::");
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
    return allocatorTable.lookupCreate(activityId, meta);
}

ILocalGraph *CThorCodeContextBase::resolveLocalQuery(__int64 gid)
{
    ILocalGraph *graph = job.getGraph((graph_id)gid);
    graph->Release(); // resolveLocalQuery doesn't own, can't otherwise will be circular ref.
    return graph;
}

IThorChildGraph *CThorCodeContextBase::resolveChildQuery(__int64 gid, IHThorArg *colocal)
{
    return job.getGraph((graph_id)gid);
}

void CThorCodeContextBase::getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags)
{
    convertRowToXML(lenResult, result, info, row, flags);
}

const void * CThorCodeContextBase::fromXml(IEngineRowAllocator * rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace)
{
    return createRowFromXml(rowAllocator, len, utf8, xmlTransformer, stripWhitespace);
}
