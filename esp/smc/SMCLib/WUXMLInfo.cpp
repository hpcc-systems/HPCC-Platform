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

// WUXMLInfo.cpp: implementation of the CWUXMLInfo class.
//
//////////////////////////////////////////////////////////////////////
#include "WUXMLInfo.hpp"
//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////
#include "dasds.hpp"
#include "exception_util.hpp"

#include "stdio.h"
#define SDS_TIMEOUT 3000


CWUXMLInfo::CWUXMLInfo()
{

}

CWUXMLInfo::~CWUXMLInfo()
{

}

void CWUXMLInfo::buildXmlActiveWuidStatus(const char* ClusterName, IEspECLWorkunit& wuStructure)
{
    try{
        Owned<IRemoteConnection> conn = querySDS().connect("/Status",myProcessSession(),RTM_LOCK_READ,SDS_TIMEOUT);
        if (!conn)
            throw MakeStringException(ECLWATCH_CANNOT_CONNECT_DALI, "Could not connect to Dali server.");

        Owned<IPropertyTreeIterator> it = conn->queryRoot()->getElements("Thor");
        CWUXMLInfo wuidInfo;
        ForEach(*it)
        {
            const char* nodeName = it->query().queryProp("@name");
            if (nodeName && strcmp(ClusterName,nodeName) == 0)
            {
                const char *wuid = it->query().queryProp("WorkUnit");
                if(wuid)
                {
                    wuidInfo.buildXmlWuidInfo(wuid,wuStructure);
                }
            }
        }
    }
    catch(IException* e){   
        StringBuffer msg;
        e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlActiveWuidStatus");
    }

}

bool CWUXMLInfo::buildXmlActionList(IConstWorkUnit &wu, StringBuffer& statusStr)
{
    return true;
}

bool CWUXMLInfo::buildXmlWuidInfo(IConstWorkUnit &wu, IEspECLWorkunit& wuStructure,bool bDescription)
{
    try
    {
        wuStructure.setProtected((wu.isProtected() ? 1 : 0));
        wuStructure.setWuid(wu.queryWuid());
        wuStructure.setOwner(wu.queryUser());
        wuStructure.setJobname(wu.queryJobName());
        wuStructure.setCluster(wu.queryClusterName());
        wuStructure.setStateID(wu.getState());
        wuStructure.setState(wu.queryStateDesc());
        if (bDescription)
        {
            SCMStringBuffer buf;
            wuStructure.setDescription((wu.getDebugValue("description", buf)).str());
        }
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlWuidInfo");
    }
    return true;
}



bool CWUXMLInfo::buildXmlWuidInfo(const char* wuid, IEspECLWorkunit& wuStructure, bool bDescription)
{
    try{
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IConstWorkUnit> wu = factory->openWorkUnit(wuid);
        if (wu)
        {
            return buildXmlWuidInfo(*wu.get(),wuStructure,bDescription);
        }
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlWuidInfo");
    }
    return false;
}

bool CWUXMLInfo::buildXmlWuidInfo(IConstWorkUnit &wu, StringBuffer& wuStructure,bool bDescription)
{
    try
    {

        wuStructure.append("<Workunit>");
        wuStructure.appendf("<Protected>%d</Protected>",(wu.isProtected() ? 1 : 0));
        wuStructure.appendf("<Wuid>%s</Wuid>",wu.queryWuid());
        wuStructure.appendf("<Owner>%s</Owner>",wu.queryUser());
        wuStructure.appendf("<Jobname>%s</Jobname>",wu.queryJobName());
        wuStructure.appendf("<Cluster>%s</Cluster>",wu.queryClusterName());
        wuStructure.appendf("<State>%s</State>",wu.queryStateDesc());
        if (bDescription)
        {
            SCMStringBuffer buf;
            wuStructure.appendf("<Description>%s</Description>", (wu.getDebugValue("description", buf)).str());
        }
        wuStructure.append("</Workunit>");
    }
    catch(IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...)
    {
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlWuidInfo");
    }
    return true;
}

bool CWUXMLInfo::buildXmlGraphList(IConstWorkUnit &wu,IPropertyTree& XMLStructure)
{
    try {
        SCMStringBuffer buf;

        IPropertyTree* resultTree = XMLStructure.addPropTree("WUGraphs", createPTree(ipt_caseInsensitive));
        Owned<IConstWUGraphIterator> graphs = &wu.getGraphs(GraphTypeAny);
        ForEach(*graphs)
        {
            IConstWUGraph &graph = graphs->query();
            IPropertyTree * p = resultTree->addPropTree("WUGraph", createPTree(ipt_caseInsensitive));
            p->setProp("Wuid", wu.queryWuid());
            p->setProp("Name", graph.getName(buf).str());
            buf.clear();
            // MORE? (debugging)
            p->setPropInt("Running", (((wu.getState() == WUStateRunning)||(wu.getState() == WUStateDebugPaused)||(wu.getState() == WUStateDebugRunning)) ? 1 : 0));
        }
    }
    catch(IException* e) {
        StringBuffer msg;
        e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...) {
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlGraphList");
    }
    return true;
}

bool CWUXMLInfo::buildXmlExceptionList(IConstWorkUnit &wu,IPropertyTree& XMLStructure)
{
    try {
        if (wu.getExceptionCount())
        {
            Owned<IConstWUExceptionIterator> exceptions = &wu.getExceptions();
            ForEach(*exceptions)
            {
                SCMStringBuffer x, y;
                IPropertyTree * p   =   XMLStructure.addPropTree("Exceptions", createPTree(ipt_caseInsensitive));
                p->setProp("Source",exceptions->query().getExceptionSource(x).str());
                p->setProp("Message",exceptions->query().getExceptionMessage(y).str());
            }
        }
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlExceptionList");
    }
    return true;
}

bool CWUXMLInfo::buildXmlResultList(IConstWorkUnit &wu,IPropertyTree& XMLStructure)
{
    try{
        IPropertyTree* resultsTree = XMLStructure.addPropTree("WUResults", createPTree(ipt_caseInsensitive));
        Owned<IConstWUResultIterator> results = &wu.getResults();
        ForEach(*results)
        {
            IConstWUResult &r = results->query();
            if (r.getResultSequence() != -1)
            {
                SCMStringBuffer x;
                StringBuffer xStr;
                StringBuffer value,link;
                r.getResultName(x);
                if (!x.length())
                {
                    value.appendf("Result %d",r.getResultSequence()+1);
                }
                if (r.getResultStatus() == ResultStatusUndefined)
                {
                    value.append("   [undefined]");
                }
                else if (r.isResultScalar())
                {
                    SCMStringBuffer x;
                    r.getResultXml(x, true);
                    try
                    {
                        Owned<IPropertyTree> props = createPTreeFromXMLString(x.str(), ipt_caseInsensitive);
                        IPropertyTree *row = props->queryPropTree("Row");
                        IPropertyTree *val = row->queryPropTree("*");
                        value.append(val->queryProp(NULL));
                    }
                    catch(...)
                    {
                        value.append("[value not available]");
                    }
                }
                else
                {
                    value.append("   [");
                    value.append(r.getResultTotalRowCount());
                    value.append(" rows]");
                    link.append(r.getResultSequence());
                }
                IPropertyTree* result = resultsTree->addPropTree("WUResult", createPTree(ipt_caseInsensitive));
                result->setProp("Name",x.str());
                result->setProp("Link",link.str());
                result->setProp("Value",value.str());
                SCMStringBuffer filename;
                r.getResultLogicalName(filename);
                StringBuffer lf;
                if(filename.length())
                {
                    lf.append(filename.str());

                }
                result->setProp("LogicalName",lf.str());
            }
        }
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlExceptionList");
    }
    return true;
}


bool CWUXMLInfo::buildXmlLogList(IConstWorkUnit &wu,IPropertyTree& XMLStructure)
{
    try{
        IPropertyTree* logTree = XMLStructure.addPropTree("WULog", createPTree(ipt_caseInsensitive));
        Owned <IConstWUQuery> query = wu.getQuery();
        if(query)
        {
            SCMStringBuffer qname;
            query->getQueryCppName(qname);
            if(qname.length())
            {
                logTree->setProp("Cpp",qname.str());
            }
            qname.clear();
            query->getQueryResTxtName(qname);
            if(qname.length())
            {
                logTree->setProp("ResTxt",qname.str());
            }
            qname.clear();
            query->getQueryDllName(qname);
            if(qname.length())
            {
                logTree->setProp("Dll",qname.str());
            }
            qname.clear();
            wu.getDebugValue("ThorLog", qname);
            if(qname.length())
            {
                logTree->setProp("ThorLog",qname.str());
            }
        }
    }
    catch(IException* e){   
      StringBuffer msg;
      e->errorMessage(msg);
        IWARNLOG("%s", msg.str());
        e->Release();
    }
    catch(...){
        IWARNLOG("Unknown Exception caught within CWUXMLInfo::buildXmlLogList");
    }
    return true;
}
void CWUXMLInfo::formatDuration(StringBuffer &ret, unsigned ms)
{
    unsigned hours = ms / (60000*60);
    ms -= hours*(60000*60);
    unsigned mins = ms / 60000;
    ms -= mins*60000;
    unsigned secs = ms / 1000;
    ms -= secs*1000;
    bool started = false;
    if (hours > 24)
    {
        ret.appendf("%d days ", hours / 24);
        hours = hours % 24;
        started = true;
    }
    if (hours || started)
    {
        ret.appendf("%d:", hours);
        started = true;
    }
    if (mins || started)
    {
        ret.appendf("%d:", mins);
        started = true;
    }
    if (started)
        ret.appendf("%02d.%03d", secs, ms);
    else
        ret.appendf("%d.%03d", secs, ms);
}
