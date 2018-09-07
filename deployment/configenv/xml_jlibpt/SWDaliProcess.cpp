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

#include "SWDaliProcess.hpp"
#include "deployutils.hpp"

namespace ech
{

SWDaliProcess::SWDaliProcess(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
  m_notifyProcessList.append("dfu");
  m_notifyProcessList.append("sasha");

  m_notifyProcessList.append("eclagent");
  m_notifyProcessList.append("eclccserver");
  m_notifyProcessList.append("eclscheduler");
  m_notifyProcessList.append("esp");
  m_notifyProcessList.append("thor");
  m_notifyProcessList.append("roxie");
}

IPropertyTree* SWDaliProcess::addComponent(IPropertyTree *params)
{
   IPropertyTree * comp = SWProcess::addComponent(params);
   const char *newName = params->queryProp("@key");
   notifyDaliNameChanged(newName, NULL);
   return comp;
}

void SWDaliProcess::modify(IPropertyTree *params)
{
   SWProcess::modify(params);
   if (params->queryProp("@selector")) return;
   if (!(params->queryPropTree("Attributes/Attribute[@name=\"name\"]"))) return;

   //notify related processes for dali name change
   const char *newName = params->queryProp("Attributes/Attribute[@name=\"name\"]/@value");
   const char *oldName = params->queryProp("Attributes/Attribute[@name=\"name\"]/@oldValue");
   notifyDaliNameChanged(newName, oldName);
}

void SWDaliProcess::notifyDaliNameChanged(const char *newName, const char *oldName)
{

   for ( unsigned i = 0; i < m_notifyProcessList.ordinality() ; i++)
   {
      ((SWProcess*)m_envHelper->getEnvSWComp(m_notifyProcessList.item(i)))->processNameChanged("daliServers", newName, oldName);
   }

}

}
