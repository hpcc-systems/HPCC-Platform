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

#include "SWBackupNode.hpp"
#include "deployutils.hpp"

namespace ech
{

SWBackupNode::SWBackupNode(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
}

unsigned SWBackupNode::add(IPropertyTree *params)
{
   unsigned rc = SWProcess::add(params);

   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* key = params->queryProp("@key");
   StringBuffer xpath;
   xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"%s\"]", m_processName.str(), key);
   IPropertyTree * compTree = envTree->queryPropTree(xpath.str());
   assert(compTree);
   const char* selector = params->queryProp("@selector");
   if (selector && !stricmp("NodeGroup", selector))
   {
       IPropertyTree *nodeGroup = createPTree(selector);
       IPropertyTree* pAttrs = params->queryPropTree("Attributes");
       updateNode(nodeGroup, pAttrs, NULL);
       if (!nodeGroup->hasProp("@interval"))
       {
          xpath.clear().appendf("xs:element/xs:complexType/xs:sequence/xs:element[@name=\"NodeGroup\"]/xs:complexType/xs:attribute[@name=\"interval\"]/@default");
          const char *interval = m_pSchema->queryProp(xpath.str());
          if ( interval && *interval )
          {
              nodeGroup->addProp("@interval", interval);
          }
          else
          {
              throw MakeStringException(CfgEnvErrorCode::MissingRequiredParam,
                  "Missing required paramter \"interval\" and there is no default value.");
          }
       }
       compTree->addPropTree(selector, nodeGroup);
   }
   return rc;
}
}
