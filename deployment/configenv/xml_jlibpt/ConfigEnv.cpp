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

#include "EnvHelper.hpp"
#include "ConfigEnv.hpp"
#include "environment.hpp"
#include "deployutils.hpp"
#include "build-config.h"


namespace ech
{

ConfigEnv::ConfigEnv(IPropertyTree *config)
{
   m_envHelper = new EnvHelper(config);
}

ConfigEnv::~ConfigEnv()
{
  if (m_envHelper)
    delete m_envHelper;
}

unsigned ConfigEnv::add(IPropertyTree *params)
{

   const char * action = params->queryProp("@operation");
   if (stricmp(action, "add"))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Invoke add with a wrong operation %s ", action);

   const char* category = params->queryProp("@category");
   return m_envHelper->getEnvComp(category)->add(params);
}

void ConfigEnv::modify(IPropertyTree *params)
{
   const char * action = params->queryProp("@operation");
   if (stricmp(action, "modify"))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Invoke modify with a wrong operation %s ", action);
   const char* category = params->queryProp("@category");
   m_envHelper->getEnvComp(category)->modify(params);
}

void ConfigEnv::create(IPropertyTree *params)
{

   StringBuffer xpath, sbTemp;
   //fprintf(stdout, "create environment.xml ...\n");
   //Process ips
   m_envHelper->processNodeAddress(params);

   if (USE_WIZARD)
   {
     StringBuffer optionsXml, envXml;
     const char* pServiceName = "WsDeploy_wsdeploy_esp";
     int roxieNodes=1, thorNodes=1, slavesPerNode=1, supportNodes=1,
         espNodes=1, thorChannelsPerSlave=1, roxieChannelsPerSlave=1;
     bool roxieOnDemand = true;
     MapStringTo<StringBuffer> dirMap;
     Owned<IPropertyTree> pCfg = createPTreeFromXMLFile(ESP_CONFIG_PATH);
     StringArray arrAssignIPRanges;
     StringArray arrBuildSetWithAssignedIPs;

     if (params->hasProp("@roxie-nodes"))
       roxieNodes = params->getPropInt("@roxie-nodes", 1);

     if (params->hasProp("@thor-nodes"))
       thorNodes = params->getPropInt("@thor-nodes", 1);

     if (params->hasProp("@slaves-per-node"))
       slavesPerNode = params->getPropInt("@slaves-per-node", 1);

     if (params->hasProp("@support-nodes"))
       supportNodes = params->getPropInt("@support-nodes", 1);

     if (params->hasProp("@esp-nodes"))
       espNodes = params->getPropInt("@esp-nodes", 1);

     if (params->hasProp("@roxie-on-demand"))
       roxieOnDemand = params->getPropBool("@roxie-on-demand", true);

     if (params->hasProp("@thor-channels-per-slave"))
       thorChannelsPerSlave = params->getPropInt("@thor-channels-per-slave", 1);

     if (params->hasProp("@roxie-channels-per-slave"))
       roxieChannelsPerSlave = params->getPropInt("@roxie-channels-per-slave", 1);

     StringBuffer ipAddr;
     const StringArray& ipList = m_envHelper->getNodeList();

     ipAddr.clear();
     ForEachItemIn(i, ipList)
     {
       if (ipAddr.length() > 0)
         ipAddr.append(";");

       ipAddr.append(ipList.item(i));
     }

     /*
     if (ipAddr.length() <= 0)
     {
        throw MakeStringException(return CfgEnvErrorCode::NoIPAddress,
           "Must provide either ip or ipfile. To do JIRA HPCC-15636");
     }
     */
     optionsXml.appendf("<XmlArgs supportNodes=\"%d\" roxieNodes=\"%d\" thorNodes=\"%d\" espNodes=\"%d\" slavesPerNode=\"%d\" roxieOnDemand=\"%s\" thorChannelsPerSlave=\"%d\" roxieChannelsPerSlave=\"%d\"  ",
      supportNodes, roxieNodes, thorNodes, espNodes, slavesPerNode, roxieOnDemand?"true":"false", thorChannelsPerSlave, roxieChannelsPerSlave);

     if (ipAddr.length() > 0)
       optionsXml.appendf("ipList=\"%s\"/>", ipAddr.str());
     else
       optionsXml.appendf("/>");


     buildEnvFromWizard(optionsXml, pServiceName, pCfg, envXml, arrBuildSetWithAssignedIPs, arrAssignIPRanges, &dirMap);
     m_envHelper->setEnvTree(envXml);
   }
   else
   {

     //Hardware
     m_envHelper->getEnvComp("Hardware")->create(params);

     //Programs
     m_envHelper->getEnvComp("Programs")->create(params);

     //EnvSettings
     m_envHelper->getEnvComp("EnvSettings")->create(params);

     //Software
     m_envHelper->getEnvComp("Software")->create(params);
   }

   runUpdateTasks(params);

  /*
   const IPropertyTree * envTree = m_envHelper->getEnvTree();

   //output
   StringBuffer env, envXml;
   toXML(envTree, envXml, 0, XML_SortTags | XML_Format);

   env.clear().appendf("<" XML_HEADER ">\n");
   env.append(envXml);

   Owned<IFile> pFile;
   const char* envFile = params->queryProp("@env-out");

   //printf("output envxml to file %s\n", envFile);

   pFile.setown(createIFile(envFile));

   Owned<IFileIO> pFileIO;
   pFileIO.setown(pFile->open(IFOcreaterw));
   pFileIO->write(0, env.length(), env.str());
   */

}


void ConfigEnv::runUpdateTasks(IPropertyTree *params)
{
   Owned<IPropertyTreeIterator> iter = params->getElements("Task");

   if (!iter) return;

   ForEach(*iter)
   {
     IPropertyTree* updateTree = &iter->query();
     const char * action = updateTree->queryProp("@operation");
     const char * category = updateTree->queryProp("@category");

     IConfigComp *categoryObj =  m_envHelper->getEnvComp(category);

     assert(categoryObj);

     if (!stricmp(action, "add"))
     {
        categoryObj->add(updateTree);
     }
     else if (!stricmp(action, "modify"))
        categoryObj->modify(updateTree);
     else if (!stricmp(action, "remove"))
        categoryObj->remove(updateTree);
     else
        throw MakeStringException(CfgEnvErrorCode::UnknownTask, "Unknown operation %s on compoent %s", action, category);
   }
}

void ConfigEnv::remove(IPropertyTree *params)
{
   const char * action = params->queryProp("@operation");
   if (stricmp(action, "remove"))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Invoke remove with a wrong operation %s ", action);
   const char* category = params->queryProp("@category");
   return m_envHelper->getEnvComp(category)->remove(params);
}

const char * ConfigEnv::queryAttribute(const char *xpath)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   assert(envTree);
   return  envTree->queryProp(xpath);
}

void ConfigEnv::setAttribute(const char *xpath , const char* attrName, const char* attrValue)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   IPropertyTree * pAttrTree = envTree->queryPropTree(xpath);
   if (!pAttrTree)
   {
      pAttrTree = createPTree();
      pAttrTree->appendProp(attrName, attrValue);
      envTree->setPropTree(xpath, pAttrTree);
   }
   else
      pAttrTree->setProp(attrName, attrValue);

}

bool ConfigEnv::isEnvironmentValid(StringBuffer& env)
{
   Owned<IEnvironmentFactory> factory = getEnvironmentFactory(false);
   Owned<IConstEnvironment>  constEnv = factory->loadLocalEnvironment(env);
   return validateEnv(constEnv);

}

IPropertyTree * ConfigEnv::getNode(const char *xpath)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   assert(envTree);
   return envTree->queryPropTree(xpath);
}

IPropertyTree * ConfigEnv::getNode(unsigned id)
{
   return NULL;
}

unsigned ConfigEnv::getNodeId(char const *xpath)
{
   return 0;
}

void ConfigEnv::getContent(const char* xpath, StringBuffer& out, int format)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   assert(envTree);

   IPropertyTree *outPTree;
   if (!xpath || strcmp(xpath, "/"))
      outPTree = envTree;
   else
   {
      outPTree = envTree->queryPropTree(xpath);
      if (!outPTree)
         throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot find xpath %s to output", xpath);

   }

   StringBuffer envXml;
   toXML(envTree, envXml, 0, format);
   out.clear().appendf("<" XML_HEADER ">\n");
   out.append(envXml);
}

void ConfigEnv::addContent(const char* xpath, StringBuffer& in, int type)
{
   if (!xpath)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot insert content to NULL xpath");

   IPropertyTree * envTree = m_envHelper->getEnvTree();
   IPropertyTree *parent = envTree->queryPropTree(xpath);
   if (!parent)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot find PTree with xpath %s to insert content.", xpath);

   IPropertyTree *child;
   switch (type)
   {
   case XML_Format:
      child = createPTreeFromXMLString(in.str());
      break;
   case JSON_Format:
      child = createPTreeFromJSONString(in.str());
      break;
   default:
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Unsupported content type %d", type);
   }

   if (!child)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Fail to create PTree with input content.");

   const char *name = child->queryName();
   if (!name)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot get tag name from newly created PTree from input content.");

   parent->addPropTree(name, child);

   // validate PTree
}

bool ConfigEnv::isAttributeValid(const char* xpath, const char* schema, const char* key, const char* value, bool src)
{
   return true;
}

}
