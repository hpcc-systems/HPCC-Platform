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

#include "SWRoxieCluster.hpp"
#include "deployutils.hpp"

namespace ech
{

SWRoxieCluster::SWRoxieCluster(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
   //roxieOnDemand = true;
   m_instanceElemName.clear().append("RoxieServerProcess");
}

IPropertyTree * SWRoxieCluster::addComponent(IPropertyTree *params)
{

  IPropertyTree *pCompTree = SWProcess::addComponent(params);
  assert(pCompTree);

  Owned<IPropertyTreeIterator> instanceIter =  pCompTree->getElements("RoxieServerProcess");
  ForEach(*instanceIter)
  {
    pCompTree->removeTree(&instanceIter->query());
  }

  //Add defautl farms
  Owned<IPropertyTree> pAttrs = createPTreeFromXMLString("<Attributes/>");
  addFarmProcess(pAttrs, pCompTree);
  // genRoxieOnDemand = true.
  addFarmProcess(pAttrs, pCompTree, true);

  return pCompTree;
}

IPropertyTree * SWRoxieCluster::addFarmProcess(IPropertyTree *params, IPropertyTree *parent, bool genRoxieOnDemand)
{
   IPropertyTree *pNode = createNode(params);

   if (!pNode->hasProp("@aclName"))
      pNode->addProp("@aclName", "");

   if (!pNode->hasProp("@name"))
   {
      if (!parent)
      {
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss parent node to generate unique name");
      }
      StringBuffer sbName("farm");
      pNode->addProp("@name", getUniqueName(parent, sbName, XML_TAG_ROXIE_FARM, "."));
   }

   if (!pNode->hasProp("@listenQueue"))
      pNode->addProp("@listenQueue", "200");

   if (!pNode->hasProp("@numThreads"))
      pNode->addProp("@numThreads", "30");

   if (!pNode->hasProp("@requestArrayThreads"))
      pNode->addProp("@requestArrayThreads", "5");

   if (!pNode->hasProp("@port"))
   {
      if (genRoxieOnDemand)
         pNode->addProp("@port", "0");
      else
         pNode->addProp("@port", "9876");
   }

   parent->addPropTree(XML_TAG_ROXIE_FARM, pNode);
   return pNode;
}

void SWRoxieCluster::addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName)
{
  SWProcess::addInstance(computerNode, parent, attrs, instanceTagXMLName);

  SWProcess *ftslaveHandler = (SWProcess*)m_envHelper->getEnvSWComp("ftslave");
  IPropertyTree *ftslaveComp = m_envHelper->getEnvTree()->queryPropTree("Software/FTSlaveProcess[1]");
  ftslaveHandler->addInstance(computerNode, ftslaveComp, NULL, "Instance");
}

}
