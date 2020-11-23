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

#include "SWLdapProcess.hpp"
#include "deployutils.hpp"

namespace ech
{

SWLdapProcess::SWLdapProcess(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
}

unsigned SWLdapProcess::add(IPropertyTree *params)
{
   int rc = SWProcess::add(params);

   StringBuffer xpath;

   IPropertyTree * envTree = m_envHelper->getEnvTree();

   const char* key = (params->hasProp("@key"))? params->queryProp("@key") : "ldapserver";
   xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"%s\"]",m_processName.str(), key);
   IPropertyTree * compTree = envTree->queryPropTree(xpath.str());

   //Add Esp Authentication
   const char* espKey = (params->hasProp("@esp"))? params->queryProp("@esp") : "myesp";
   if (compTree->hasProp("@esp"))
   {
      compTree->removeProp("@esp");
   }
   xpath.clear().appendf(XML_TAG_SOFTWARE "/EspProcess[@name=\"%s\"]",espKey);
   IPropertyTree * espTree = envTree->queryPropTree(xpath.str());
   IPropertyTree * oldAuthTree = espTree->queryPropTree("Authentication");
   if (oldAuthTree)
   {
      espTree->removeTree(oldAuthTree);
   }

   IPropertyTree * newAuthTree = createPTree(XML_TAG_AUTHENTICATION);
   newAuthTree->addProp("@htpasswdFile", "/etc/HPCCSystems/.htpasswd");
   newAuthTree->addProp("@ldapConnections", "10");
   newAuthTree->addProp("@ldapServer", key);
   newAuthTree->addProp("@method", "ldaps");
   espTree->addPropTree(XML_TAG_AUTHENTICATION, newAuthTree);

   return rc;
}

}
