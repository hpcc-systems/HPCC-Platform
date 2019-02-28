/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "Software.hpp"
#include "SWProcess.hpp"
#include "SWDropZone.hpp"
#include "SWThorCluster.hpp"
#include "SWRoxieCluster.hpp"
#include "SWEspProcess.hpp"
#include "SWEspService.hpp"
#include "SWDirectories.hpp"
#include "SWDaliProcess.hpp"
#include "SWBackupNode.hpp"
#include "SWTopology.hpp"
#include "deployutils.hpp"
#include "ComponentBase.hpp"

namespace ech
{

Software::Software(EnvHelper * envHelper):ComponentBase("software", envHelper)
{
}

Software::~Software()
{

  HashIterator swCompIter(m_swCompMap);
  ForEach(swCompIter)
  {
    IMapping &cur = swCompIter.query();
    IConfigComp* pSWComp = m_swCompMap.mapToValue(&cur);
    SWComponentBase *swcb = (SWComponentBase*) pSWComp;
    //swcb-Release();
    ::Release(swcb);
  }
}

void Software::create(IPropertyTree *params)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();

  const IPropertyTree * buildSetTree = m_envHelper->getBuildSetTree();
  envTree->addPropTree(XML_TAG_SOFTWARE, createPTreeFromIPT(
    buildSetTree->queryPropTree("./" XML_TAG_SOFTWARE)));

  getSWComp("esp")->create(params);

  StringBuffer xpath;
  xpath.clear().appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);
    Owned<IPropertyTreeIterator> buildSetInsts = buildSetTree->getElements(xpath.str());

  const GenEnvRules& rules = m_envHelper->getGenEnvRules();

  ForEach(*buildSetInsts)
  {
    IPropertyTree* pBuildSet = &buildSetInsts->query();
    const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
    if (stricmp(buildSetName, "esp") == 0) continue;
    if (rules.foundInProp("do_not_generate", buildSetName)) continue;

    getSWComp(buildSetName)->create(params);
  }

}

unsigned Software::add(IPropertyTree *params)
{
  const char *comp    = params->queryProp("@component");
  return getSWComp(comp)->add(params);
}

void Software::modify(IPropertyTree *params)
{
   //IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char *comp = params->queryProp("@component");
   getSWComp(comp)->modify(params);
}


void Software::remove(IPropertyTree *params)
{
   const char *comp = params->queryProp("@component");
   getSWComp(comp)->remove(params);
}

void Software::getSWCompName(const char *inputName, StringBuffer& out)
{
   const char * compNameLC = (StringBuffer(inputName).toLowerCase()).str();

   // Return xsd name in buildset.xml
   out.clear();
   if (!stricmp(compNameLC, "directories")  || !stricmp(compNameLC, "dirs"))
   {
      out.append("directories");
   }
   else if (!stricmp(compNameLC, "dali") || !stricmp(compNameLC, "DaliServerProcess"))
   {
      out.append("dali");
   }
   else if (!stricmp(compNameLC, "dropzone"))
   {
      out.append("DropZone");
   }
   else if (!stricmp(compNameLC, "roxie") || !stricmp(compNameLC, "RoxieCluster"))
   {
      out.append("roxie");
   }
   else if (!stricmp(compNameLC, "thor") || !stricmp(compNameLC, "ThorCluster"))
   {
      out.append("thor");
   }
   else if (!stricmp(compNameLC, "esp") || !stricmp(compNameLC, "EspProcess"))
   {
      out.append("esp");
   }
   else if (!stricmp(compNameLC, "elcwatch") || !stricmp(compNameLC, "espsmc"))
   {
      out.append("espsmc");
   }
   else if (!stricmp(compNameLC, "esdl") || !stricmp(compNameLC, "DynamicESDL"))
   {
      out.append("DynamicESDL");
   }
   else if (!stricmp(compNameLC, "ws_sql") || !stricmp(compNameLC, "wssql"))
   {
      out.append("ws_sql");
   }
   else if (!stricmp(compNameLC, "ws_ecl") || !stricmp(compNameLC, "wsecl"))
   {
      out.append("ws_ecl");
   }
   else if (!stricmp(compNameLC, "wslogging"))
   {
      out.append("wslogging");
   }
   else if (!stricmp(compNameLC, "cassandra") || !stricmp(compNameLC, "CassandraLoggingAgent"))
   {
      out.append("cassandraloggingagent");
   }
   else if (!stricmp(compNameLC, "esplogging") || !stricmp(compNameLC, "EspLoggingAgent"))
   {
      out.append("esploggingagent");
   }
   else if (!stricmp(compNameLC, "loggingmgr") || !stricmp(compNameLC, "loggingmanager"))
   {
      out.append("loggingmanager");
   }
   else if (!stricmp(compNameLC, "backup") || !stricmp(compNameLC, "backupnode"))
   {
      out.append("backupnode");
   }
   else if (!stricmp(compNameLC, "agent") || !stricmp(compNameLC, "eclagent") ||
         !stricmp(compNameLC, "EclAgentiProcess"))
   {
      out.append("eclagent");
   }
   else if (!stricmp(compNameLC, "eclccsrv") || !stricmp(compNameLC, "eclccserver") ||
            !stricmp(compNameLC, "eclcc") || !stricmp(compNameLC, "EclCCServerProcess"))
   {
      out.append("eclccserver");
   }
   else if (!stricmp(compNameLC, "sch") || !stricmp(compNameLC, "eclsch") ||
            !stricmp(compNameLC, "scheduler") || !stricmp(compNameLC, "EclCCSchedulerProcess"))
   {
      out.append("eclscheduler");
   }
   else if (!stricmp(compNameLC, "dfu") || !stricmp(compNameLC, "dfusrv") ||
            !stricmp(compNameLC, "dfuserver") || !stricmp(compNameLC, "DfuServerProcess"))
   {
      out.append("dfuserver");
   }
   else if (!stricmp(compNameLC, "topo") || !stricmp(compNameLC, "Topology"))
   {
      out.append("topology");
   }
   else if (!stricmp(compNameLC, "fts") || !stricmp(compNameLC, "FTSlave") || !stricmp(compNameLC, "FTSlaveProcess"))
   {
      out.append("ftslave");
   }
   else if (!stricmp(compNameLC, "spark") || !stricmp(compNameLC, "sparkthor") || !stricmp(compNameLC, "SparkThorProcess"))
   {
      out.append("sparkthor");
   }
   else
   {
      out.append(compNameLC);
   }
}

IConfigComp* Software::getSWComp(const char *compName)
{
   //should call m_envHelper->getXMLTagName(compName)
   //const char *compNameLC =  m_envHelper->getXMLTagName(compName);

   const char * compNameLC = (StringBuffer(compName).toLowerCase()).str();

   StringBuffer sbBuildSetName;
   getSWCompName(compNameLC, sbBuildSetName);
   const char* buildSetName = sbBuildSetName.str();

   IConfigComp * pComp = m_swCompMap.getValue(buildSetName);
   if (pComp) return pComp;

   if (!stricmp(buildSetName, "directories"))
   {
      pComp = (IConfigComp*) new SWDirectories(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "dali"))
   {
      pComp = (IConfigComp*) new SWDaliProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "DropZone"))
   {
      pComp = (IConfigComp*) new SWDropZone(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "roxie"))
   {
      pComp = (IConfigComp*) new SWRoxieCluster(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "thor"))
   {
      pComp = (IConfigComp*) new SWThorCluster(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "esp"))
   {
      pComp = (IConfigComp*) new SWEspProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "espsmc"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "DynamicESDL"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "ws_sql"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "ws_ecl"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "wslogging"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "cassandraloggingagent"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "esploggingagent"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "loggingmanager"))
   {
      pComp = (IConfigComp*) new SWEspService(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "eclagent"))
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "eclccserver"))
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "eclscheduler"))
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "dfuserver"))
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "topology"))
   {
      pComp = (IConfigComp*) new SWTopology(m_envHelper);
   }
   else if (!stricmp(buildSetName, "ftslave"))
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }
   else if (!stricmp(buildSetName, "backupnode"))
   {
      pComp = (IConfigComp*) new SWBackupNode(buildSetName, m_envHelper);
   }
   else
   {
      pComp = (IConfigComp*) new SWProcess(buildSetName, m_envHelper);
   }

   if (pComp != NULL)
   {
      m_swCompMap.setValue(buildSetName,  pComp);
   }

   return pComp;
}

}
