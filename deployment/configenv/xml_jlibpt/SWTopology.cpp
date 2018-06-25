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

#include "SWTopology.hpp"
//#include "deployutils.hpp"

namespace ech
{

SWTopology::SWTopology(EnvHelper * envHelper):SWComponentBase("topology", envHelper)
{
}

void SWTopology::create(IPropertyTree *params)
{
   //to do
}


unsigned SWTopology::add(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* key = params->queryProp("@key");
   StringBuffer xpath;
   xpath.clear().appendf(XML_TAG_SOFTWARE "/Topology[@name=\"%s\"]", key);
   IPropertyTree * compTree = envTree->queryPropTree(xpath.str());

   synchronized block(mutex);
   if (!compTree)
   {
      compTree = createPTree("Topology");
      compTree->addProp("@name", key);
      envTree->addPropTree("Topology", compTree);
   }

   const char* selector = params->queryProp("@selector");
   IPropertyTree * pAttrs = params->queryPropTree("Attributes");
   if (!selector)
   {
      if (pAttrs)
         updateNode(compTree, pAttrs);

      checkTopology(compTree);
      return 0;
   }

   if (stricmp(selector, "Cluster"))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams,
         "Miss \"Cluster\" in adding topology.");

   const char* selectorKey = params->queryProp("@selector-key");

   if (!pAttrs)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams,
            "Miss cluster name in creating a new cluster.");

   xpath.clear().append("Cluster[@name=");
   if (selectorKey && (*selectorKey))
      xpath.appendf("\"%s\"]", selectorKey);
   else if (pAttrs)
   {
       const char *clusterName = getAttributeFromParams(pAttrs, "name", NULL);
       if (clusterName)
          xpath.appendf("\"%s\"]", clusterName);
       else
          throw MakeStringException(CfgEnvErrorCode::InvalidParams,
             "Miss cluster name query/create topology cluster");
   }
   else
      throw MakeStringException(CfgEnvErrorCode::InvalidParams,
         "Miss cluster attributes to query/create topology cluster");

   IPropertyTree * cluster = compTree->queryPropTree(xpath.str());
   if (!cluster)
   {
      cluster = createPTree("Cluster");
      updateNode(cluster, pAttrs);
      checkTopologyCluster(cluster);
      compTree->addPropTree("Cluster", cluster);
   }

   IPropertyTree * children = params->queryPropTree("Children");
   if (children)
      createChildrenNodes(cluster, children);

   return 0;
}

void SWTopology::checkTopologyCluster(IPropertyTree *cluster)
{
   if (!cluster->hasProp("@alias"))
      cluster->addProp("@alias", "");

   if (!cluster->hasProp("@prefix"))
      cluster->addProp("@prefix", cluster->queryProp("@name"));
}

void SWTopology::checkTopology(IPropertyTree *topology)
{
   if (!topology->hasProp("@build"))
      topology->addProp("@build", m_buildName.str());

   if (!topology->hasProp("@buildSet"))
      topology->addProp("@buildSet", m_name.str());
}

void SWTopology::processNameChanged(const char* process,  const char* newName, const char* oldName)
{
   const char* tagName = m_envHelper->getXMLTagName(process);
   if (!tagName || !(*tagName))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Can't find XML tag name for %s", process);

   StringBuffer xpath;
   xpath.clear().appendf("/Software/Topology/Cluster/%s[@process=\"%s\"", tagName, oldName);

   IPropertyTree * envTree = m_envHelper->getEnvTree();
   Owned<IPropertyTreeIterator> processIter = envTree->getElements(xpath.str());
   synchronized block(mutex);
   ForEach (*processIter)
   {
      IPropertyTree * process = &processIter->query();
      process->setProp("process", newName);
   }
}

}
