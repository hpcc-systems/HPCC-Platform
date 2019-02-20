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

#include "Programs.hpp"
#include "deployutils.hpp"

namespace ech
{

Programs::Programs(EnvHelper * envHelper):ComponentBase("programs", envHelper)
{
  this->m_envHelper = envHelper;
}

void Programs::create(IPropertyTree *params)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();

  Owned<IPropertyTree> pProgramTree = createPTreeFromIPT(m_envHelper->getBuildSetTree());
  envTree->addPropTree(XML_TAG_PROGRAMS, createPTreeFromIPT(pProgramTree->queryPropTree("./" XML_TAG_PROGRAMS)));

}


unsigned Programs::add(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   const IPropertyTree * buildSet = m_envHelper->getBuildSetTree();
   StringBuffer xpath;

   if (!stricmp(compName, "BuildSet"))
   {
      const char* key = params->queryProp("@key");
      if (!key || !(*key))
      {
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Missing BuildSet name");
      }

      xpath.clear().appendf("./%s/%s/%s/[@name=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, key);
      IPropertyTree * sparkBuild = envTree->queryPropTree(xpath.str());
      if (sparkBuild) return 0;

      sparkBuild = buildSet->queryPropTree(xpath.str());
      if (!sparkBuild)
         throw MakeStringException(CfgEnvErrorCode::UnknownComponent, "Missing %s program name in BuildSet.xml", key);

      IPropertyTree * pProgramTree = envTree->queryPropTree("./" XML_TAG_PROGRAMS "/" XML_TAG_BUILD);
      pProgramTree->addPropTree(XML_TAG_BUILDSET, createPTreeFromIPT(sparkBuild));
   }

   return 0;
}

void Programs::modify(IPropertyTree *params)
{
   return;
}

void Programs::remove(IPropertyTree *params)
{
   return;
}

}
