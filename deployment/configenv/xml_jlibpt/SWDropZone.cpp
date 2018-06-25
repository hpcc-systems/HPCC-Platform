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

#include "SWDropZone.hpp"
#include "deployutils.hpp"

namespace ech
{

SWDropZone::SWDropZone(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
   m_instanceElemName.clear().append("ServerList");
   m_ipAttribute.clear().append("@server");
}

void SWDropZone::checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent)
{
   Owned<IAttributeIterator> iAttr = instanceNode->getAttributes();
   ForEach(*iAttr)
   {
      const char* propName = iAttr->queryName();
      if (!(*propName)) continue;
      if (stricmp(propName, "@server") && stricmp(propName, "@name"))
         instanceNode->removeProp(propName);
   }
}

IPropertyTree * SWDropZone::findInstance(IPropertyTree *comp, IPropertyTree *computerNode)
{
   Owned<IPropertyTreeIterator> instanceIter = comp->getElements(m_instanceElemName);
   ForEach (*instanceIter)
   {
      IPropertyTree * instance = &instanceIter->query();
      if (!stricmp(instance->queryProp("@server"), computerNode->queryProp(XML_ATTR_NETADDRESS)))
         return instance;
   }

   return NULL;
}

void SWDropZone::addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName)
{
  SWProcess::addInstance(computerNode, parent, attrs, instanceTagXMLName);

  SWProcess *ftslaveHandler = (SWProcess*)m_envHelper->getEnvSWComp("ftslave");
  IPropertyTree *ftslaveComp = m_envHelper->getEnvTree()->queryPropTree("Software/FTSlaveProcess[1]");
  ftslaveHandler->addInstance(computerNode, ftslaveComp, NULL, "Instance");
}


}
