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
#include "SWProcess.hpp"
#include "deployutils.hpp"
#include "configenvhelper.hpp"
#include "buildset.hpp"
#include "Hardware.hpp"

namespace ech
{

SWProcess::SWProcess(const char* name, EnvHelper * envHelper):SWComponentBase(name, envHelper)
{
  m_instanceElemName.clear().append("Instance");
  m_ipAttribute.clear().append("@netAddress");

  m_notifyTopologyList.append("eclagent");
  m_notifyTopologyList.append("eclccserver");
  m_notifyTopologyList.append("eclscheduler");
  m_notifyTopologyList.append("esp");
  m_notifyTopologyList.append("thor");
  m_notifyTopologyList.append("roxie");

  m_singleInstanceList.append("dali");

}

IPropertyTree * SWProcess::addComponent(IPropertyTree *params)
{
  const char* clone = params->queryProp("@clone");
  if (clone)
  {
     return SWComponentBase::cloneComponent(params);
  }

  IPropertyTree * pCompTree = SWComponentBase::addComponent(params);
  if (pCompTree->hasProp("@daliServers") &&  !strcmp(pCompTree->queryProp("@daliServers"), ""))
  {
     IPropertyTree * envTree = m_envHelper->getEnvTree();
     StringBuffer xpath;
     xpath.clear().appendf(XML_TAG_SOFTWARE "/DaliServerProcess/@name");
     const char *daliName = envTree->queryProp(xpath.str());
     if (daliName)
     {
        pCompTree->setProp("@daliServers", daliName);
     }
  }

  removeInstancesFromComponent(pCompTree);
  return pCompTree;
}

void SWProcess::create(IPropertyTree *params)
{
  SWComponentBase::create(params);

  IPropertyTree * envTree = m_envHelper->getEnvTree();

  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"my%s\"]", m_processName.str(), m_name.str());
  IPropertyTree * pCompTree = envTree->queryPropTree(xpath.str());
  assert(pCompTree);

  //create instance

}


unsigned SWProcess::add(IPropertyTree *params)
{
  unsigned rc = SWComponentBase::add(params);

  IPropertyTree * envTree = m_envHelper->getEnvTree();
  const char* key = params->queryProp("@key");
  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"%s\"]", m_processName.str(), key);
  IPropertyTree * compTree = envTree->queryPropTree(xpath.str());
  assert(compTree);
  const char* selector = params->queryProp("@selector");
  if (selector)
  {
     String str(selector);
     if (str.startsWith("instance"))
     {
        addInstances(compTree, params);
     }
     // Other selectors handled by concrete software process,
     // For example, NodeGroup in BackupNodeProcess, EspBinding in EspProcess
  }

  return rc;
}

//int SWProcess::addNode(IPropertyTree *params, const char* xpath, bool merge)
//{
//   return 0;
//}


void SWProcess::modify(IPropertyTree *params)
{
  SWComponentBase::modify(params);

  const char * selector = params->queryProp("@selector");
  if ( selector )
  {
     IPropertyTree * envTree = m_envHelper->getEnvTree();
     const char* key = params->queryProp("@key");
     StringBuffer xpath;
     xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"%s\"]", m_processName.str(), key);
     IPropertyTree * compTree = envTree->queryPropTree(xpath.str());
     if (!compTree)
        throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Selector should matche one element in modify.");

     String str(selector);
     if (str.startsWith("instance"))
        modifyInstance(compTree, params);

     return;
  }

  if (!(params->queryPropTree("Attributes/Attribute[@name=\"name\"]"))) return;

  //notify topoploy for  component name change
  const char *oldName = params->queryProp("@key");
  const char *newName = params->queryProp("Attributes/Attribute[@name=\"name\"]/@value");
  if (m_notifyTopologyList.find(m_name.str()) != NotFound)
  {
     ((SWProcess*)m_envHelper->getEnvSWComp("topology"))->processNameChanged(m_name.str(), newName, oldName);
  }

}


IConfigComp* SWProcess::getInstanceNetAddresses(StringArray& ipList, const char* clusterName)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();
  StringBuffer xpath;
  if (clusterName)
     xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[@name=\"%s\"]", m_processName.str(), clusterName);
  else
     xpath.clear().appendf(XML_TAG_SOFTWARE "/%s[1]", m_processName.str());

  IPropertyTree *compTree = envTree->queryPropTree(xpath.str());

  Owned<IPropertyTreeIterator> iter = compTree->getElements(m_instanceElemName.str());
  ForEach (*iter)
  {
     IPropertyTree *instance = &iter->query();
     ipList.append(instance->queryProp("@netAddress"));

  }
  return (IConfigComp*)this;
}


unsigned SWProcess::getInstanceCount(const char* clusterName)
{
   StringBuffer xpath;
   if (clusterName && *clusterName)
     xpath.clear().appendf("%s[@name=\"%s\"]", m_processName.str(), clusterName);
   else
     xpath.clear().appendf("%s[1]", m_processName.str());
   //IPropertyTree * comp = m_envHelper->getEnvTree()->getPropTree(xpath);
   IPropertyTree * comp = m_envHelper->getEnvTree()->queryPropTree(xpath);

   return comp->getCount(m_instanceElemName.str());
}

void SWProcess::addInstances(IPropertyTree *parent, IPropertyTree *params)
{
  IPropertyTree* pAttrs = params->queryPropTree("Attributes");
  if (!pAttrs)
     throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss instance attributes input");

  const char * instanceXMLTagName = getInstanceXMLTagName(params->queryProp("@selector"));

  Owned<IPropertyTreeIterator> iter = pAttrs->getElements("Attribute");

  ForEach (*iter)
  {
     IPropertyTree *attr = &iter->query();
     const char* propName = attr->queryProp("@name");
     if (!stricmp(propName, "ip") || !stricmp(propName, "ipfile"))
     {
        bool isFile = false;
        if (!stricmp(propName, "ipfile")) isFile = true;

        StringArray ips;
        m_envHelper->processNodeAddress(attr->queryProp("@value"), ips, isFile);
        for ( unsigned i = 0; i < ips.ordinality() ; i++)
        {
           IPropertyTree * computerNode = addComputer(ips.item(i));
           addInstance(computerNode, parent, pAttrs, instanceXMLTagName);
        }
     }
  }
}

IPropertyTree * SWProcess::addComputer(const char* ip)
{
   Hardware *hd = (Hardware*) m_envHelper->getEnvComp("hardware");
   StringBuffer sbTask;
   sbTask.clear().append("<Task operation=\"add\" category=\"hardware\" component=\"Computer\">");
   sbTask.appendf("<Attributes><Attribute name=\"ip\" value=\"%s\"/></Attributes></Task>", ip);

   Owned<IPropertyTree> params = createPTreeFromXMLString(sbTask.str());
   IPropertyTree * pComputer =  hd->addComputer(params);
   return pComputer;
}

void SWProcess::addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceXMLTagName)
{

   IPropertyTree *instanceNode = NULL;
   StringBuffer xpath;
   if (m_singleInstanceList.find(m_name.str()) != NotFound)
   {
      xpath.clear().appendf("%s[1]", instanceXMLTagName);
      instanceNode = parent->queryPropTree(xpath.str());
   }
   else
   {
      instanceNode = findInstance(parent, computerNode);
   }

   if (!instanceNode)
   {
      // create instance
      instanceNode = createPTree(instanceXMLTagName);
      instanceNode->addProp("@computer", computerNode->queryProp(XML_ATTR_NAME));
      // get unique name

      StringBuffer sbName;
      if (String(instanceXMLTagName).indexOf("Master") > 0)
         sbName.append("m");
      else
         sbName.append("s");

      instanceNode->addProp(XML_ATTR_NAME, getUniqueName(parent, sbName, instanceXMLTagName, ""));
      instanceNode->addProp(m_ipAttribute.str(), computerNode->queryProp(XML_ATTR_NETADDRESS));
      parent->addPropTree(instanceXMLTagName, instanceNode);
   }

   StringArray excludeList;
   excludeList.append("ip");
   excludeList.append("ipfile");
   if (attrs)
      updateNode(instanceNode, attrs, &excludeList);

   checkInstanceAttributes(instanceNode, parent);

}

void SWProcess::modifyInstance(IPropertyTree *parent, IPropertyTree *params)
{
   IPropertyTree* pAttrs = params->queryPropTree("Attributes");
   if (!pAttrs)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss instance attributes input");

   const char * instanceXMLTagName = getInstanceXMLTagName(pAttrs->queryProp("@selector"));
   Owned<IPropertyTreeIterator> iter = pAttrs->getElements("Attribute");
   ForEach (*iter)
   {
      IPropertyTree *attr = &iter->query();
      const char* propName = attr->queryProp("@name");
      if (stricmp(propName, "ip"))
         continue;

      StringBuffer xpath;
      IPropertyTree *instanceToModify;
      const char* oldIp = attr->queryProp("@oldValue");
      if ((!oldIp || !(*oldIp)) && m_singleInstanceList.find(m_name.str()) == NotFound)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss instance  current ip to change");
      else if (oldIp && *oldIp)
      {
         xpath.clear().appendf("%s[%s=\"%s\"]", instanceXMLTagName, m_ipAttribute.str(), oldIp);
         instanceToModify = parent->queryPropTree(xpath.str());
      }
      else
      {
         xpath.clear().appendf("%s[1]", instanceXMLTagName);
         instanceToModify = parent->queryPropTree(xpath.str());
      }

      if (!instanceToModify)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot find instance node to modify");

      IPropertyTree * computerNode = addComputer(attr->queryProp("@value"));
      instanceToModify->setProp("@computer", computerNode->queryProp(XML_ATTR_NAME));

      instanceToModify->setProp(m_ipAttribute.str(), computerNode->queryProp(XML_ATTR_NETADDRESS));
  }

}

void SWProcess::checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent)
{
   assert(instanceNode);
   if (portIsRequired() && !instanceNode->hasProp("@port"))
   {
      int port = getDefaultPort();
      if (!port)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss port attribute in instance");
      instanceNode->addPropInt("@port", port);
   }

   StringBuffer xpath;
   xpath.clear().appendf("xs:element/xs:complexType/xs:sequence/xs:element[@name=\"%s\"]",m_instanceElemName.str());
   IPropertyTree * instanceSchemaNode = m_pSchema->queryPropTree(xpath.str());
   if (!instanceSchemaNode) return;

   bool needDirProp = false;
   Owned<IPropertyTreeIterator> attrIter = instanceSchemaNode->getElements("xs:complexType/xs:attribute");
   ForEach(*attrIter)
   {
      IPropertyTree * attr = &attrIter->query();
      const char *attrName = attr->queryProp("@name");
      if (!stricmp(attrName, "directory"))
      {
         needDirProp = true;
         continue;
      }

      const char *defaultValue = attr->queryProp("@default");
      if (!defaultValue) continue;
      xpath.clear().appendf("@%s", attrName);
      if (instanceNode->hasProp(xpath.str())) continue;
      const char *use = attr->queryProp("@use");
      if (!use || !stricmp(use, "required")  || !stricmp(use, "optional"))
      {
         StringBuffer sbDefaultValue;
         sbDefaultValue.clear().append(defaultValue);
         sbDefaultValue.replaceString("\\", "\\\\");
         instanceNode->addProp(xpath.str(), sbDefaultValue.str());
       }
   }

   if (needDirProp && !instanceNode->hasProp("@directory"))
   {
      const IProperties *props = m_envHelper->getEnvConfigOptions().getProperties();
      StringBuffer sb;
      sb.clear().appendf("%s/%s",
         props->queryProp("runtime"), parent->queryProp(XML_ATTR_NAME));
      instanceNode->addProp("@directory", sb.str());
   }
}

void SWProcess::computerAdded(IPropertyTree *computerNode, const char *instanceXMLTagName)
{
   StringBuffer sb;
   sb.clear().appendf("%s/%s[1]", XML_TAG_SOFTWARE, m_processName.str());
   IPropertyTree * comp = m_envHelper->getEnvTree()->queryPropTree(sb.str());
   assert(comp);
   const char *instance = (instanceXMLTagName)? instanceXMLTagName: m_instanceElemName.str();
   addInstance(computerNode, comp, NULL, instance);
}

void SWProcess::computerUpdated(IPropertyTree *computerNode, const char* oldName,
     const char* oldIp, const char* instanceXMLTagName)
{
   IPropertyTree *software = m_envHelper->getEnvTree()->queryPropTree("Software");
   Owned<IPropertyTreeIterator> compIter = software->getElements(m_processName);

   const char *instance = (instanceXMLTagName)? instanceXMLTagName: m_instanceElemName.str();

   synchronized block(mutex);
   ForEach (*compIter)
   {
      IPropertyTree *comp = &compIter->query();
      Owned<IPropertyTreeIterator> instanceIter = comp->getElements(instance);
      ForEach (*instanceIter)
      {
         IPropertyTree *instance = &instanceIter->query();
         if (instance->hasProp(XML_ATTR_NAME) && !stricmp(instance->queryProp(XML_ATTR_NAME), oldName))
         {
            if (stricmp(computerNode->queryProp(XML_ATTR_NAME), instance->queryProp(XML_ATTR_NAME)))
               instance->setProp(XML_ATTR_NAME, computerNode->queryProp(XML_ATTR_NAME));
            if (instance->hasProp(m_ipAttribute.str()) && stricmp(computerNode->queryProp(XML_ATTR_NETADDRESS), instance->queryProp(m_ipAttribute.str())))
               instance->setProp(m_ipAttribute.str(), computerNode->queryProp(XML_ATTR_NETADDRESS));
            if (instance->hasProp("@computer") && stricmp(computerNode->queryProp(XML_ATTR_NAME), instance->queryProp("@computer")))
               instance->setProp("@computer", computerNode->queryProp(XML_ATTR_NAME));
         }
         else if (instance->hasProp(m_ipAttribute.str()) && !stricmp(instance->queryProp(m_ipAttribute.str()), oldIp))
         {
            instance->setProp(m_ipAttribute.str(), computerNode->queryProp(XML_ATTR_NETADDRESS));
         }
      }
   }
}

void SWProcess::computerDeleted(const char* ipAddress, const char* computerName, const char *instanceXMLTagName)
{
   IPropertyTree * software = m_envHelper->getEnvTree()->queryPropTree(XML_TAG_SOFTWARE);
   Owned<IPropertyTreeIterator> compIter = software->getElements(m_processName);

   const char *instance = (instanceXMLTagName)? instanceXMLTagName: m_instanceElemName.str();

   synchronized block(mutex);
   ForEach (*compIter)
   {
      IPropertyTree * comp = &compIter->query();
      Owned<IPropertyTreeIterator> instanceIter = comp->getElements(instance);
      ForEach (*instanceIter)
      {
         IPropertyTree * instance = &instanceIter->query();
         if ((instance->hasProp(m_ipAttribute.str()) && stricmp(instance->queryProp(m_ipAttribute.str()), ipAddress)) ||
             (instance->hasProp("@computer") && stricmp(instance->queryProp("@computer"), computerName)))
            comp->removeTree(instance);
      }
   }
}

IPropertyTree * SWProcess::getPortDefinition()
{
   StringBuffer xpath;
   xpath.clear().appendf("xs:element[@name=\"%s\"]", m_instanceElemName.str());
   IPropertyTree * instanceNode =  m_pSchema->queryPropTree(xpath.str());
   if (!instanceNode) return NULL;

   xpath.clear().append("xs:attribute[@name=\"port\"]");
   return  instanceNode->queryPropTree(xpath.str());
}

bool SWProcess::portIsRequired()
{
   IPropertyTree * portAttrNode =  getPortDefinition();
   if (!portAttrNode) return false;

   const char* portUseAttr = portAttrNode->queryProp("@use");
   if (portUseAttr && stricmp(portUseAttr, "required"))
      return false;

   return true;
}

int SWProcess::getDefaultPort()
{
   IPropertyTree * portAttrNode =  getPortDefinition();
   if (!portAttrNode) return 0;
   int defaultValue = portAttrNode->getPropInt("@default");

   if (!defaultValue) return 0;

   return defaultValue;
}

IPropertyTree * SWProcess::cloneComponent(IPropertyTree *params)
{
   IPropertyTree * targetNode =  SWComponentBase::cloneComponent(params);
   removeInstancesFromComponent(targetNode);
   return targetNode;
}

void SWProcess::processNameChanged(const char* process, const char *newName, const char* oldName)
{
   StringBuffer xpath;
   xpath.clear().appendf("@%s", process);
   IPropertyTree * swTree = m_envHelper->getEnvTree()->queryPropTree("/Software");
   Owned<IPropertyTreeIterator> compIter =  swTree->getElements(m_envHelper->getXMLTagName(m_name.str()));
   synchronized block(mutex);
   ForEach(*compIter)
   {
      IPropertyTree * compTree = &compIter->query();
      if ((oldName != NULL) && strcmp(oldName, compTree->queryProp(xpath.str())))
         continue;

      if (compTree->queryProp(xpath.str()))
         compTree->setProp(xpath.str(), newName);
   }
}

void SWProcess::resolveSelector(const char* selector, const char* key, StringBuffer &out)
{
   String lwSelector(selector);
   if ((lwSelector.toLowerCase())->startsWith("instance"))
        out.clear().appendf("%s[@%s=\"%s\"]", m_instanceElemName.str(), m_ipAttribute.str(), key);
   else
       ComponentBase::resolveSelector(selector, key, out);

}

void SWProcess::removeInstancesFromComponent(IPropertyTree *compNode)
{
   Owned<IPropertyTreeIterator> instanceIter =  compNode->getElements(m_instanceElemName);
   ForEach(*instanceIter)
   {
      compNode->removeTree(&instanceIter->query());
   }
}

IPropertyTree * SWProcess::findInstance(IPropertyTree *comp, IPropertyTree *computerNode)
{
   Owned<IPropertyTreeIterator> instanceIter = comp->getElements(m_instanceElemName);
   ForEach (*instanceIter)
   {
      IPropertyTree * instance = &instanceIter->query();
      if (!stricmp(instance->queryProp("@computer"), computerNode->queryProp(XML_ATTR_NAME)))
         return instance;
   }

   return NULL;
}

}
