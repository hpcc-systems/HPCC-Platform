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

#include "SWThorCluster.hpp"
//#include "deployutils.hpp"

namespace ech
{

SWThorCluster::SWThorCluster(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
   //roxieOnDemand = true;
   m_instanceElemName.clear().append("ThorSlaveProcess");
   m_masterInstanceElemName.clear().append("ThorMasterProcess");
}

IPropertyTree * SWThorCluster::addComponent(IPropertyTree *params)
{

  const char* clone = params->queryProp("@clone");
  if (clone)
  {
     return SWProcess::cloneComponent(params);
  }

  IPropertyTree *pCompTree = SWProcess::addComponent(params);
  assert(pCompTree);

  removeInstancesFromComponent(pCompTree);

  // Some of following may already be handled by AttributesFromXSD
  // Add <SwapNode />  <Debug/> and <SSH />

  return pCompTree;
}

IPropertyTree * SWThorCluster::cloneComponent(IPropertyTree *params)
{
   IPropertyTree * targetNode =  SWProcess::cloneComponent(params);
   removeInstancesFromComponent(targetNode);

   return targetNode;
}

void SWThorCluster::checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent)
{
   const char *ip = instanceNode->queryProp(XML_ATTR_NETADDRESS);
   if (ip) instanceNode->removeProp(XML_ATTR_NETADDRESS);
}

const char* SWThorCluster::getInstanceXMLTagName(const char* name)
{
   const char* selectorName = (StringBuffer(name).toLowerCase()).str();

   if (!strcmp(selectorName, "instance-master"))
      return m_masterInstanceElemName.str();

   return m_instanceElemName.str();
}

void SWThorCluster::addInstances(IPropertyTree *parent, IPropertyTree *params)
{
   const char * instanceXMLTagName = getInstanceXMLTagName(params->queryProp("@selector"));
   StringBuffer xpath;
   if (!stricmp(instanceXMLTagName, "ThorMasterProcess"))
   {
      const char* key = params->queryProp("@key");
      IPropertyTree * masterNode = parent->queryPropTree("ThorMasterProcess");
      if (masterNode)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot add thor master. There is already a master node in cluster \"%s\".", key);
   }
   SWProcess::addInstances(parent, params);
   if (!stricmp(instanceXMLTagName, "ThorMasterProcess"))
   {
      IPropertyTree * masterNode = parent->queryPropTree("ThorMasterProcess");
      parent->setProp("@computer", masterNode->queryProp("@computer"));
   }

}

void SWThorCluster::updateComputerAttribute(const char *newName, const char *oldName)
{
  IPropertyTree *software = m_envHelper->getEnvTree()->queryPropTree("Software");
  Owned<IPropertyTreeIterator> compIter = software->getElements(m_processName);
  ForEach (*compIter)
  {
    IPropertyTree *comp = &compIter->query();
    if (!stricmp(oldName, comp->queryProp("@computer")))
    {
       comp->setProp("@computer", newName);
    }
  }
}

void SWThorCluster::computerUpdated(IPropertyTree *computerNode, const char *oldName, const char *oldIp,
          const char* instanceXMLTagName)
{
  SWProcess::computerUpdated(computerNode, oldName, oldIp);
  SWProcess::computerUpdated(computerNode, oldName, oldIp, m_masterInstanceElemName.str());
  updateComputerAttribute(computerNode->queryProp(XML_ATTR_NAME), oldName);

}

void SWThorCluster::computerDeleted(const char *ipAddress, const char *computerName, const char* instanceXMLTagName)
{
  SWProcess::computerDeleted(ipAddress, computerName, instanceXMLTagName);
  SWProcess::computerDeleted(ipAddress, computerName, m_masterInstanceElemName.str());
  updateComputerAttribute("", computerName);
}

void SWThorCluster::removeInstancesFromComponent(IPropertyTree *compNode)
{
  Owned<IPropertyTreeIterator> masterIter =  compNode->getElements("ThorMasterProcess");
  ForEach(*masterIter)
  {
    compNode->removeTree(&masterIter->query());
  }
  compNode->setProp("@computer", "");

  Owned<IPropertyTreeIterator> slaveIter =  compNode->getElements("ThorSlaveProcess");
  ForEach(*slaveIter)
  {
    compNode->removeTree(&slaveIter->query());
  }
}

void SWThorCluster::addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName)
{
  SWProcess::addInstance(computerNode, parent, attrs, instanceTagXMLName);

  SWProcess *ftslaveHandler = (SWProcess*)m_envHelper->getEnvSWComp("ftslave");
  IPropertyTree *ftslaveComp = m_envHelper->getEnvTree()->queryPropTree("Software/FTSlaveProcess[1]");
  ftslaveHandler->addInstance(computerNode, ftslaveComp, NULL, "Instance");
}

}
