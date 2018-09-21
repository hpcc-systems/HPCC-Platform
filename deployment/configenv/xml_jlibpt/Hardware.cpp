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

#include "Hardware.hpp"
#include "SWProcess.hpp"
#include "deployutils.hpp"

namespace ech
{

const char* Hardware::c_type    = "linuxmachine";
const char* Hardware::c_maker   = "unknown";
const char* Hardware::c_speed   = "1000";
const char* Hardware::c_domain  = "localdomain";
const char* Hardware::c_os      = "linux";

Hardware::Hardware(EnvHelper * envHelper):ComponentBase("hardware", envHelper)
{
  m_notifyUpdateList.append("dafilesrv");
  m_notifyUpdateList.append("dfuserver");
  m_notifyUpdateList.append("DropZone");
  m_notifyUpdateList.append("eclagent");
  m_notifyUpdateList.append("dali");
  m_notifyUpdateList.append("sasha");
  m_notifyUpdateList.append("eclccserver");
  m_notifyUpdateList.append("eclscheduler");
  m_notifyUpdateList.append("esp");
  m_notifyUpdateList.append("thor");
  m_notifyUpdateList.append("roxie");
  m_notifyUpdateList.append("ftslave");

  m_notifyAddList.append("dafilesrv");

}

void Hardware::create(IPropertyTree *params)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();
  assert (envTree);

  if (envTree->queryPropTree("./" XML_TAG_HARDWARE))
  {
     throw MakeStringException(CfgEnvErrorCode::ComponentExists,
       "Cannot create Hardware component which  already exists");
  }

  StringBuffer xpath, sbdefaultValue("");
  Owned<IPropertyTree> pCompTree = createPTree(XML_TAG_HARDWARE);

  IPropertyTree* pSwitch = pCompTree->addPropTree(XML_TAG_SWITCH, createPTree());
  xpath.clear().append(XML_ATTR_NAME);
  pSwitch->addProp(xpath, "Switch") ;

  IPropertyTree* pDomain = pCompTree->addPropTree(XML_TAG_DOMAIN, createPTree());
  xpath.clear().append(XML_ATTR_NAME);
  pDomain->addProp(xpath, Hardware::c_domain);

  envTree->addPropTree(XML_TAG_HARDWARE, createPTreeFromIPT(pCompTree));

  StringBuffer task;
  task.clear().append("<Task operation=\"add\" category=\"hardware\"");
  task.appendf("selector=\"ComputerType\"><Attributes/></Task>");
  Owned<IPropertyTree> taskPT = createPTreeFromXMLString(task.str());
  addComputerType(taskPT);

  const StringArray& ipArray = m_envHelper->getNodeList();
  for (unsigned i = 0; i < ipArray.ordinality(); i++)
  {
     task.clear().append("<Task operation=\"add\" category=\"hardware\" component=\"Computer\">");
     task.appendf("<Attributes><Attribute name=\"ip\" value=\"%s\"/></Attributes></Task>", ipArray.item(i));
     taskPT.setown(createPTreeFromXMLString(task.str()));
     addComputer(taskPT);
  }
}

IPropertyTree* Hardware::addComputer(IPropertyTree *params)
{
  assert(params);
  IPropertyTree* attrs  = params->queryPropTree("Attributes");
  assert(attrs);

  IPropertyTree* envTree = m_envHelper->getEnvTree();
  IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
  assert(pHardwareTree);

  const char * ip           = getAttributeFromParams(attrs, "ip", NULL);
  if (!ip)
    throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss ip information in adding hardware");

  const char * computerName = getAttributeFromParams(attrs, "name", NULL);
  const char * type         = getAttributeFromParams(attrs, "type", Hardware::c_type);
  const char * domain       = getAttributeFromParams(attrs, "domain", Hardware::c_domain);
  const char * namePrefix   = getAttributeFromParams(attrs, "namePrefix", NULL);

  StringBuffer  sbIp;
  IpAddress ipAddr;

  sbIp.clear();
  if ((ip == NULL) || (ip == ""))
  {
    if ((computerName == NULL) || (computerName == ""))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot add a computer without both ip and name.");
    else
    {
      ipAddr.ipset(computerName);
      ipAddr.getIpText(sbIp);
    }
  }
  else
    sbIp.append(ip);

  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_COMPUTER"[@netAddress=\"%s\"]", sbIp.str());
  IPropertyTree* pComputer = pHardwareTree->queryPropTree(xpath);
  if (pComputer)
    return pComputer;

  xpath.clear().appendf(XML_TAG_DOMAIN "[@name=\"%s\"]", domain);
  IPropertyTree* pDomain = pHardwareTree->queryPropTree(xpath);
  if (!pDomain)
  {
     xpath.clear().append("<Task operation=\"add\" category=\"hardware\" selector=\"" XML_TAG_DOMAIN "\">");
     xpath.appendf("<Attribute><Attribute name=\"name\" value=\"%s\"/></Attributes></Task>", domain);
     Owned<IPropertyTree> taskPT = createPTreeFromXMLString(xpath.str());
     addDomain(taskPT);
  }

  StringBuffer sbName;
  sbName.clear();
  if ((computerName == NULL) || (computerName == ""))
  {
    if (ip && (ip == "."))
       sbName.append("localhost");
    else
    {
      if ((namePrefix == NULL) || (namePrefix == ""))
      {
        unsigned x;
        ipAddr.ipset(sbIp.str());
        ipAddr.getNetAddress(sizeof(x), &x);
        sbName.appendf("node%03d%03d", (x >> 16) & 0xFF, (x >> 24) & 0xFF);
      }
      else
        sbName.append(namePrefix);

      getUniqueName(pHardwareTree, sbName, XML_TAG_COMPUTER, "");
    }
  }
  else
  {
    sbName.append(computerName);
  }

  //synchronized block(mutex);
  mutex.lock();

  pComputer = pHardwareTree->addPropTree(XML_TAG_COMPUTER,createPTree());
  pComputer->addProp(XML_ATTR_COMPUTERTYPE, type);
  pComputer->addProp(XML_ATTR_DOMAIN, domain);
  pComputer->addProp(XML_ATTR_NAME, sbName.str());
  pComputer->addProp(XML_ATTR_NETADDRESS, sbIp.str());
  mutex.unlock();

  //notify a new computer added
  for (unsigned i = 0; i < m_notifyAddList.ordinality(); i++)
  {
     ((SWProcess*)m_envHelper->getEnvSWComp(m_notifyAddList.item(i)))->computerAdded(pComputer);
  }

  return pComputer;
}

unsigned Hardware::add(IPropertyTree *params)
{
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   if (!stricmp(compName, XML_TAG_COMPUTER))
   {
      addComputer(params);
      return  0;
   }
   else if (!stricmp(compName, XML_TAG_COMPUTERTYPE))
   {
      addComputerType(params);
      return  0;
   }
   else
   {
      IPropertyTree* envTree = m_envHelper->getEnvTree();
      IPropertyTree *pAttrs = params->queryPropTree("Attributes");
      IPropertyTree *compTree = createNode(pAttrs);
      IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
      if (!stricmp(compName, XML_TAG_DOMAIN))
         pHardwareTree->addPropTree(XML_TAG_DOMAIN, compTree);
      else if (!stricmp(compName, XML_TAG_SWITCH))
         pHardwareTree->addPropTree(XML_TAG_SWITCH, compTree);
      else
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Unknown hardware component %s", compName);

   }
   return  0;

}


void Hardware::modify(IPropertyTree *params)
{
   IPropertyTree* envTree = m_envHelper->getEnvTree();
   IPropertyTree *pAttrs = params->queryPropTree("Attributes");
   if (pAttrs)
   {
      IPropertyTree *pAttr = pAttrs->queryPropTree("Attribute[@name='ip']");
      if (pAttr)
         pAttr->setProp("@name", "netAddress");
   }
   ComponentBase::modify(params);
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   if (stricmp(compName, XML_TAG_COMPUTER)) return;

   StringBuffer sbOldName;
   StringBuffer xpath;
   xpath.clear().append(XML_TAG_HARDWARE "/" XML_TAG_COMPUTER "[@name=");
   const char* oldName = pAttrs->queryProp("Attribute[@name='name']/@oldValue");
   const char* oldIp = pAttrs->queryProp("Attribute[@name='netAddress']/@oldValue");
   if (oldName)
   {
      sbOldName.clear().appendf("%s", oldName);
      xpath.appendf("'%s']", pAttrs->queryProp("Attribute[@name='name']/@value"));
   }
   else
   {
      const char* newIP = pAttrs->queryProp("Attribute[@name='netAddress']/@value");
      if (newIP)
      {
         const char* newName =  getComputerName(newIP);
         sbOldName.clear().appendf("%s", getComputerName(newIP));
         xpath.appendf("'%s']", newName);
      }

   }

   if (sbOldName.isEmpty())
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Can't get computer name to notify others.");


   IPropertyTree *pComputer = envTree->queryPropTree(xpath.str());
   if (!pComputer) return;

   for (unsigned i = 0; i < m_notifyUpdateList.ordinality(); i++)
   {
      ((SWProcess*)m_envHelper->getEnvSWComp(m_notifyUpdateList.item(i)))->computerUpdated(pComputer, sbOldName.str(), oldIp);
   }

}

void Hardware::remove(IPropertyTree *params)
{
   StringBuffer sbIp;
   StringBuffer sbComputerName;
   IPropertyTree* envTree = m_envHelper->getEnvTree();
   const char* target = params->queryProp("@target");
   const char* key = params->queryProp("@key");
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));

   IPropertyTree *pAttrs = params->queryPropTree("Attributes");
   if (pAttrs)
   {
      IPropertyTree *pIpAttr = pAttrs->queryPropTree("Attribute[@name='ip']");
      if (pIpAttr)
      {
         pIpAttr->setProp("@name", "netAddress");
         sbIp.clear().appendf("%s", pIpAttr->queryProp("value"));
      }

      if ((!target || !stricmp(target, "node")) && !stricmp(compName, "computer"))
      {
         if (key)
            sbComputerName.clear().appendf("%s", key);
         else
         {
            IPropertyTree *pNameAttr = pAttrs->queryPropTree("Attribute[@name='name']");
            if (pNameAttr)
               sbComputerName.clear().appendf("%s", pNameAttr->queryProp("@value"));
         }

         if (!sbIp.isEmpty() && !sbComputerName.isEmpty())
            throw MakeStringException(CfgEnvErrorCode::InvalidParams,
               "Cannot remove a computer without both ip and name.");

         if (sbIp.isEmpty())
            sbIp.clear().appendf("%s", getComputerNetAddress(sbComputerName.str()));
         if (sbComputerName.isEmpty())
            sbComputerName.clear().appendf("%s", getComputerName(sbIp.str()));
      }
   }

   ComponentBase::remove(params);

   if (!sbComputerName.isEmpty())
   {
      for (unsigned i = 0; i < m_notifyUpdateList.ordinality(); i++)
      {
         ((SWProcess*)m_envHelper->getEnvSWComp(m_notifyUpdateList.item(i)))->computerDeleted(sbIp.str(), sbComputerName.str());
      }
   }

}


const char* Hardware::getComputerName(const char* netAddress)
{
  IPropertyTree* envTree = m_envHelper->getEnvTree();
  IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_COMPUTER "[@netAddress=\"%s\"]", netAddress);
  IPropertyTree* pComputer = pHardwareTree->queryPropTree(xpath);
  if (pComputer)
    return pComputer->queryProp(XML_ATTR_NAME);
  return NULL;
}

const char* Hardware::getComputerNetAddress(const char* name)
{
  IPropertyTree* envTree = m_envHelper->getEnvTree();
  IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_COMPUTER "[@name=\"%s\"]", name);
  IPropertyTree* pComputer = pHardwareTree->queryPropTree(xpath);
  if (pComputer)
    return pComputer->queryProp(XML_ATTR_NETADDRESS);
  return NULL;
}

IPropertyTree* Hardware::addComputerType(IPropertyTree *params)
{

  assert(params);
  IPropertyTree* attrs  = params->queryPropTree("Attributes");
  assert(attrs);

  const char * typeName         = getAttributeFromParams(attrs, "name", Hardware::c_type);

  IPropertyTree* envTree = m_envHelper->getEnvTree();
  IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_COMPUTERTYPE "[@name=\"%s\"]", typeName);
  IPropertyTree * pComputerType =  pHardwareTree->queryPropTree(xpath);
  if (pComputerType) return pComputerType;


  const char * manufacturer = getAttributeFromParams(attrs, "manufacturer", Hardware::c_maker);
  const char * type         = getAttributeFromParams(attrs, "computerType", Hardware::c_type);
  const char * os           = getAttributeFromParams(attrs, "computerType", Hardware::c_os);
  const char * speed        = getAttributeFromParams(attrs, "nicSpeed", Hardware::c_speed);

  synchronized block(mutex);
  pComputerType = pHardwareTree->addPropTree(XML_TAG_COMPUTERTYPE, createPTree());
  xpath.clear().append(XML_ATTR_NAME);
  pComputerType->addProp(xpath, typeName);
  xpath.clear().append(XML_ATTR_MANUFACTURER);
  pComputerType->addProp(xpath, manufacturer);
  xpath.clear().append(XML_ATTR_COMPUTERTYPE);
  pComputerType->addProp(xpath, type);
  xpath.clear().append(XML_ATTR_OPSYS);
  pComputerType->addProp(xpath, os);
  xpath.clear().append(XML_ATTR_NICSPEED);
  pComputerType->addProp(xpath, speed);

  return pComputerType;
}

IPropertyTree* Hardware::addDomain(IPropertyTree *params)
{
  assert(params);
  IPropertyTree* attrs  = params->queryPropTree("Attributes");
  assert(attrs);

  IPropertyTree* envTree = m_envHelper->getEnvTree();
  IPropertyTree* pHardwareTree = envTree->queryPropTree(XML_TAG_HARDWARE);
  assert(pHardwareTree);

  const char * domain  = getAttributeFromParams(attrs, "domain", Hardware::c_domain);

  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_DOMAIN "[@name=\"%s\"]", domain);
  IPropertyTree* pDomain = pHardwareTree->queryPropTree(xpath);
  if (pDomain) return pDomain;

  synchronized block(mutex);

  pDomain = pHardwareTree->addPropTree(XML_TAG_DOMAIN, createPTree());
  pDomain->addProp(XML_ATTR_NAME, domain);

  return pDomain;
}


}
