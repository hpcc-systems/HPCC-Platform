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

#include "SWEspProcess.hpp"
#include "SWEspService.hpp"
#include "deployutils.hpp"

namespace ech
{

SWEspProcess::SWEspProcess(const char* name, EnvHelper * envHelper):SWProcess(name, envHelper)
{
}

IPropertyTree * SWEspProcess::addComponent(IPropertyTree *params)
{
  IPropertyTree * pCompTree;
  IPropertyTree * envTree = m_envHelper->getEnvTree();
  const char* clone = params->queryProp("@clone");
  if (clone)
  {
     pCompTree =  SWComponentBase::cloneComponent(params);
  }
  else
  {
     pCompTree = SWComponentBase::addComponent(params);
     if (pCompTree->hasProp("@daliServers") &&  !strcmp(pCompTree->queryProp("@daliServers"), ""))
     {
        StringBuffer xpath;
        xpath.clear().appendf(XML_TAG_SOFTWARE "/DaliServerProcess/@name");
        const char *daliName = envTree->queryProp(xpath.str());
        if (daliName)
        {
           pCompTree->setProp("@daliServers", daliName);
        }
     }
  }
  removeInstancesFromComponent(pCompTree);

  IPropertyTree* pAttrs = params->queryPropTree("Attributes");
  if (pAttrs)
  {
     // The default is to bind all Esp Services
     // If "bindings" attribute set to "skip" user need to use "EspBinding" selector
     // to add bindings
     const char * bindings = pAttrs->queryProp("Attribute[@name='bindings']/@value");
     if (bindings && !stricmp(bindings, "skip"))
     {
        return pCompTree;
     }
  }

  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_SOFTWARE "/" XML_TAG_ESPSERVICE);
  Owned<IPropertyTreeIterator> espServiceIter = envTree->getElements(xpath.str());
  ForEach (*espServiceIter)
  {
     IPropertyTree* pEspService = &espServiceIter->query();
     if(pEspService)
     {
        const char* serviceName = pEspService->queryProp("@name");
        xpath.clear().appendf("<Attributes><Attribute name=\"name\" value=\"my%s\"/>", serviceName);
        xpath.appendf("<Attribute name=\"service\" value=\"%s\"/></Attributes>", serviceName);
        Owned<IPropertyTree> pBindingAttrs = createPTreeFromXMLString(xpath.str());
        addBinding(pCompTree, pBindingAttrs);

     }
  }
  return pCompTree;
}


void SWEspProcess::addOtherSelector(IPropertyTree *compTree, IPropertyTree *params)
{
   const char* selector = params->queryProp("@selector");
   if (selector && !stricmp(selector, "EspBinding"))
   {
      IPropertyTree* pAttrs = params->queryPropTree("Attributes");
      if (!pAttrs)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss binding attributes input");

      addBinding(compTree, pAttrs);
   }
}

void SWEspProcess::addBinding(IPropertyTree *parent, IPropertyTree * attrs)
{
   StringBuffer xpath;
   xpath.clear().append("Attribute[@name='name']/@value");
   const char * bindingName = attrs->queryProp(xpath);
   if (!bindingName || !(*bindingName))
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss esp binding name in adding binding");

   xpath.clear().append("Attribute[@name='service']/@value");
   const char * serviceName = attrs->queryProp(xpath);
   if (!serviceName || !(*serviceName))
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss esp service name in adding binding");

   IPropertyTree * envTree = m_envHelper->getEnvTree();
   xpath.clear().appendf(XML_TAG_SOFTWARE "/" XML_TAG_ESPSERVICE "[@name=\"%s\"]", serviceName);
   IPropertyTree * pEspService = envTree->queryPropTree(xpath.str());
   if (!pEspService)
   {
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Can't find EspService with name %s.", serviceName);
   }

   IPropertyTree *pBinding = createPTree(XML_TAG_ESPBINDING);
   if (attrs)
      updateNode(pBinding, attrs, NULL);

   // check attributes

   if (!(pBinding->queryProp(XML_ATTR_PROTOCOL)))
      pBinding->addProp(XML_ATTR_PROTOCOL, "http");
   const char* protocol = pBinding->queryProp(XML_ATTR_PROTOCOL);

   if (!stricmp(protocol, "https"))
      xpath.clear().append("Properties/@defaultSecurePort");
   else
      xpath.clear().append("Properties/@defaultPort");

   const char* defaultPort = pEspService->queryProp(xpath.str());
   const char* port = pBinding->queryProp(XML_ATTR_PORT);
   if (!port || !(*port))
   {
      pBinding->addProp(XML_ATTR_PORT, defaultPort);
   }
   const char* defaultForPort = pBinding->queryProp("@defaultForPort");
   if (!stricmp(defaultPort, pBinding->queryProp(XML_ATTR_PORT)))
   {
      if (!defaultForPort || !(*defaultForPort) || !defaultForPort)
         pBinding->setProp("@defaultForPort", "true");
   }
   else
   {
      if (!defaultForPort || !(*defaultForPort) || defaultForPort)
         pBinding->setProp("@defaultForPort", "true");
   }


   if (!(pBinding->queryProp("@defaultServiceVersion")))
      pBinding->addProp("@defaultServiceVersion", "");

   if (!(pBinding->queryProp("@resourcesBasedn")))
      pBinding->addProp("@resourcesBasedn", pEspService->queryProp("Properties/@defaultResourcesBasedn"));

   if (!(pBinding->queryProp(XML_ATTR_TYPE)))
      pBinding->addProp(XML_ATTR_TYPE, "");

   if (!(pBinding->queryProp("@workunitsBasedn")))
      pBinding->addProp("@workunitsBasedn", "ou=workunits,ou=ecl");

   if (!(pBinding->queryProp("@wsdlServiceAddress")))
      pBinding->addProp("@wsdlServiceAddress", "");

   // Add AuthenticateFeature
   Owned<IPropertyTreeIterator> afIter = pEspService->getElements("Properties/AuthenticateFeature");
   ForEach (*afIter)
   {
      pBinding->addPropTree("AuthenticateFeature", m_envHelper->clonePTree(&afIter->query()));
   }

   // Add Authenticate
   Owned<IPropertyTreeIterator> authIter = pEspService->getElements("Properties/Authenticate");
   ForEach (*authIter)
   {
      pBinding->addPropTree("Authenticate", m_envHelper->clonePTree(&authIter->query()));
   }

   parent->addPropTree(XML_TAG_ESPBINDING, pBinding);
}

}
