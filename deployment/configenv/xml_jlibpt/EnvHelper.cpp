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

#include <string>
#include "EnvHelper.hpp"
#include "Hardware.hpp"
#include "Programs.hpp"
#include "EnvSettings.hpp"
#include "Software.hpp"
#include "ComponentBase.hpp"
#include "deployutils.hpp"
#include "confighelper.hpp"

namespace ech
{

static std::string defaultEnvXml;
static std::string defaultEnvOptions;
static std::string defaultGenEnvRules;
static std::string defaultBuildSet;

EnvHelper::EnvHelper(IPropertyTree *config)
{
   init(config);
}

void EnvConfigOptions::loadFile(const char* filename)
{
   m_options.setown(createProperties(filename));
}


const char* EnvHelper::getConfig(const char* key, CONFIG_TYPE type) const
{
   StringBuffer sbKey;
   sbKey.clear();
   if (key[0] != '@')
     sbKey.append("@");
   sbKey.append(key);

   if ((type == CONFIG_INPUT) ||(type == CONFIG_ALL))
   {
     if (m_config->hasProp(sbKey.str()))
       return m_config->queryProp(sbKey.str());
   }
   if ((type == CONFIG_ENV) || (type == CONFIG_ALL))
   {
     const IProperties * envCfg = m_envCfgOptions->getProperties();
     if (envCfg->hasProp(sbKey.str()))
       return envCfg->queryProp(sbKey.str());
   }

   return NULL;
}

EnvHelper::~EnvHelper()
{

  HashIterator compIter(m_compMap);
  ForEach(compIter)
  {
    IMapping &cur = compIter.query();
    IConfigComp* pComp = m_compMap.mapToValue(&cur);
    ComponentBase *cb = (ComponentBase*) pComp;
    ::Release(cb);
  }

  m_envTree.clear();
  m_buildSetTree.clear();
  if (m_envCfgOptions) delete m_envCfgOptions;
  if (m_genEnvRules) delete m_genEnvRules;
}



void EnvHelper::init(IPropertyTree *config)
{
   this->m_config = config;

   /*
   //id
   m_baseIds.setValue("Hardware", 1);
   m_baseIds.setValue("EnvSettings", 2);
   m_baseIds.setValue("Programs", 3);
   m_baseIds.setValue("Software", 4);
   */


   StringBuffer fileName;
   const char* optionsFileName = m_config->queryProp("@options");
   if (optionsFileName && *optionsFileName)
      fileName.clear().append(optionsFileName);
   else
      fileName.clear().append(defaultEnvOptions.c_str());

   m_envCfgOptions = new EnvConfigOptions(fileName.str());

   const char* genEnvRulesFileName = m_config->queryProp("@rules");
   if (genEnvRulesFileName && *genEnvRulesFileName)
      fileName.clear().append(genEnvRulesFileName);
   else
      fileName.clear().append(defaultGenEnvRules.c_str());

   m_genEnvRules = new GenEnvRules(fileName.str());


   const char* buildSetFileName = m_config->queryProp("@buildset");
   if (buildSetFileName && * buildSetFileName)
      fileName.clear().append(buildSetFileName);
   else
      fileName.clear().append(defaultBuildSet.c_str());
   m_buildSetTree.setown(createPTreeFromXMLFile(fileName.str()));

   const char* envXmlFileName = m_config->queryProp("@env-in");
   if (envXmlFileName && *envXmlFileName)
      m_envTree.setown(createPTreeFromXMLFile(envXmlFileName));
   else if (!USE_WIZARD)
   {
      m_envTree.setown(createPTreeFromXMLString("<" XML_HEADER "><" XML_TAG_ENVIRONMENT "></" XML_TAG_ENVIRONMENT ">"));

      //Initialize CConfigHelper
      StringBuffer espConfigPath;
      if (m_config->hasProp("@esp-config"))
         m_config->getProp("@esp-config", espConfigPath);
      else
         espConfigPath.append(hpccBuildInfo.configDir).append("/configmgr/esp.xml");
      Owned<IPropertyTree> espCfg = createPTreeFromXMLFile(espConfigPath);

      const char* espServiceName =  (m_config->hasProp("@esp-service"))?
         m_config->queryProp("@esp-service") : "WsDeploy_wsdeploy_esp";

      // just initialize the instance which is static member
      CConfigHelper *pch = CConfigHelper::getInstance(espCfg, espServiceName);
      if (pch == NULL)
      {
        throw MakeStringException( -1 , "Error loading buildset from configuration");
      }
   }

}

EnvHelper * EnvHelper::setEnvTree(StringBuffer &envXml)
{
  m_envTree.setown(createPTreeFromXMLString(envXml));
  return this;
}

IConfigComp* EnvHelper::getEnvComp(const char *compName)
{
   const char * compNameLC = (StringBuffer(compName).toLowerCase()).str();
   IConfigComp * pComp = m_compMap.getValue(compNameLC);
   if (pComp) return pComp;


   pComp = NULL;
   if (stricmp(compNameLC, "hardware") == 0)
   {
      pComp = (IConfigComp*) new Hardware(this);
   }
   else if (stricmp(compNameLC, "programs") == 0)
   {
      pComp = (IConfigComp*) new Programs(this);
   }
   else if (stricmp(compNameLC, "envsettings") == 0)
   {
      pComp = (IConfigComp*) new EnvSettings(this);
   }
   else if (stricmp(compNameLC, "software") == 0)
   {
      pComp = (IConfigComp*) new Software(this);
   }

   if (pComp != NULL)
   {
      m_compMap.setValue(compNameLC,  pComp);
   }

   return pComp;
}

IConfigComp* EnvHelper::getEnvSWComp(const char *swCompName)
{
  Software* sw =  (Software*)getEnvComp("software");
  IConfigComp* icc = sw->getSWComp(swCompName);
  return icc;


  //return ((Software*)getEnvComp("software"))->getSWComp(swCompName);
}

int  EnvHelper::processNodeAddress(const char *ipInfo, StringArray &ips, bool isFile)
{
   int len  = ips.ordinality();

   if (!ipInfo || !(*ipInfo)) return 0;

   if (isFile)
   {
     StringBuffer ipAddrs;
     ipAddrs.loadFile(ipInfo);
     if (ipAddrs.str())
       formIPList(ipAddrs.str(), ips);
     else
       return 0;
   }
   else
   {
     formIPList(ipInfo, ips);
   }

   return ips.ordinality() - len;

}

void EnvHelper::processNodeAddress(IPropertyTree * param)
{
   const char* ipList = param->queryProp("@ip-list");
   if (ipList)
   {
     formIPList(ipList, m_ipArray);
     processNodeAddress(ipList, m_ipArray);
   }

   const char* ipFileName = param->queryProp("@ip-file");
   if (ipFileName)
   {
     processNodeAddress(ipFileName, m_ipArray, true);
   }

}

bool EnvHelper::getCompNodeList(const char * compName, StringArray *ipList, const char *cluster)
{
  return true;
}

const char* EnvHelper::assignNode(const char * compName)
{
  return NULL;
}


bool EnvHelper::validateAndToInteger(const char *str,int &out, bool throwExcepFlag)
{
  bool bIsValid = false;
  char *end = NULL;

  errno = 0;

  const long sl = strtol(str,&end,10);

  if (end == str)
  {
    if (throwExcepFlag)
      throw MakeStringException( CfgEnvErrorCode::NonInteger , "Error: non-integer parameter '%s' specified.\n",str);
  }
  else if ('\0' != *end)
  {
    if (throwExcepFlag)
      throw MakeStringException( CfgEnvErrorCode::NonInteger , "Error: non-integer characters found in '%s' when expecting integer input.\n",str);
  }
  else if ( (INT_MAX < sl || INT_MIN > sl) || ((LONG_MIN == sl || LONG_MAX == sl) && ERANGE == errno))
  {
    if (throwExcepFlag)
      throw MakeStringException( CfgEnvErrorCode::OutOfRange , "Error: integer '%s' is out of range.\n",str);
  }
  else
  {
    out = (int)sl;
    bIsValid = true;
  }

  return bIsValid;
}

const char* EnvHelper::getXMLTagName(const char* name)
{

   if (!name)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Null string in getXMLTagName");

   const char * nameLC = (StringBuffer(name).toLowerCase()).str();

   if (!strcmp(nameLC, "hardware") || !strcmp(nameLC, "hd"))
      return XML_TAG_HARDWARE;
   else if (!strcmp(nameLC, "computer") || !strcmp(nameLC, "cmpt"))
      return XML_TAG_COMPUTER;
   else if (!strcmp(nameLC, "software") || !strcmp(nameLC, "sw"))
      return XML_TAG_SOFTWARE;
   else if (!strcmp(nameLC, "envsettings") || !strcmp(nameLC, "es"))
      return "EnvSettings";
   else if (!strcmp(nameLC, "programs") || !strcmp(nameLC, "pg") ||
            !strcmp(nameLC, "build") || !strcmp(nameLC, "bd"))
      return "Programs/Build";
   else if (!strcmp(nameLC, "directories") || !strcmp(nameLC, "dirs"))
      return XML_TAG_DIRECTORIES;
   else if (!strcmp(nameLC, "roxie") || !strcmp(nameLC, "roxiecluster"))
      return XML_TAG_ROXIECLUSTER;
   else if (!strcmp(nameLC, "thor") || !strcmp(nameLC, "thorcluster"))
      return XML_TAG_THORCLUSTER;
   else if (!strcmp(nameLC, "dali") || !strcmp(nameLC, "daliserverprocess") ||
            !strcmp(nameLC, "dalisrv"))
      return XML_TAG_DALISERVERPROCESS;
   else if (!strcmp(nameLC, "dafile") || !strcmp(nameLC, "dafilesrv") ||
            !strcmp(nameLC, "dafileserverprocess"))
      return XML_TAG_DAFILESERVERPROCESS;
   else if (!strcmp(nameLC, "dfu") || !strcmp(nameLC, "dfusrv") ||
            !strcmp(nameLC, "dfuserverprocess"))
      return  "DfuServerProcess";
   else if (!strcmp(nameLC, "dropzone"))
      return XML_TAG_DROPZONE;
   else if (!strcmp(nameLC, "eclcc"))
      // eclccserver define this as EclCCServer but buildset set it to EclCCServerProcess.
      // Our default environment.xml and envgen generated one use EclCCServerProcess.
      // Seems both EclCCServer and EclCCServerProcess work. Will keep EclCCServerProcess for now.
      return "EclCCServerProcess";
   else if (!strcmp(nameLC, "eclplus"))
      return "EclPlusProcess";
   else if (!strcmp(nameLC, "eclccsrv") || !strcmp(nameLC, "eclccserver") || !strcmp(nameLC, "eclcc"))
      return XML_TAG_ECLCCSERVERPROCESS;
   else if (!strcmp(nameLC, "esp") || !strcmp(nameLC, "espprocess"))
      return XML_TAG_ESPPROCESS;
   else if (!strcmp(nameLC, "espsvc") || !strcmp(nameLC, "espservice") || !strcmp(nameLC, "espsmc") ||
            !strcmp(nameLC, "ws_sql") || !strcmp(nameLC, "DynamicESDL") || !strcmp(nameLC, "wslogging") ||
            !strcmp(nameLC, "ws_ecl"))
      return XML_TAG_ESPSERVICE;
   else if (!strcmp(nameLC, "binding") || !strcmp(nameLC, "espbinding"))
      return XML_TAG_ESPBINDING;
   else if (!strcmp(nameLC, "sasha") || !strcmp(nameLC, "sashasrv"))
      return XML_TAG_SASHA_SERVER_PROCESS;
   else if (!strcmp(nameLC, "eclsch") || !strcmp(nameLC, "eclscheduler"))
      return XML_TAG_ECLSCHEDULERPROCESS;
   else if (!strcmp(nameLC, "agent") || !strcmp(nameLC, "eclagent"))
      return XML_TAG_ECLAGENTPROCESS;
   else if (!strcmp(nameLC, "topology") || !strcmp(nameLC, "topo"))
      return XML_TAG_TOPOLOGY;
   else if (!strcmp(nameLC, "ftslave") || !strcmp(nameLC, "ftslaveprocess"))
      return "FTSlaveProcess";
   else if (!strcmp(nameLC, "backupnode") || !strcmp(nameLC, "backup") || !strcmp(nameLC, "BackupNodeProcess"))
      return "BackupNodeProcess";
   else if (!strcmp(nameLC, "spark") || !strcmp(nameLC, "sparkthor") || !strcmp(nameLC, "SparkThorProcess"))
      return "SparkThorProcess";
   else if (!strcmp(nameLC, "ldap") || !strcmp(nameLC, "ldapserver") || !strcmp(nameLC, "LDAPServerProcess"))
      return "LDAPServerProcess";
   else if (!strcmp(nameLC, "buildset"))
      return "BuildSet";
   else
      return name;
}


IPropertyTree * EnvHelper::clonePTree(const char* xpath)
{
   StringBuffer error;
   if (!validateXPathSyntax(xpath, &error))
      throw MakeStringException(CfgEnvErrorCode::InvalidParams,
         "Syntax error in xpath  %s: %s", xpath, error.str());

   IPropertyTree *src = m_envTree->queryPropTree(xpath);
   return clonePTree(src);
}

IPropertyTree * EnvHelper::clonePTree(IPropertyTree *src)
{
   StringBuffer xml;
   toXML(src, xml);
   return createPTreeFromXMLString(xml.str());
}

}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
   ech::defaultEnvXml = std::string(hpccBuildInfo.configDir) + "/environment.xml";
   ech::defaultEnvOptions = std::string(hpccBuildInfo.configDir) + "/environment.conf";
   ech::defaultGenEnvRules = std::string(hpccBuildInfo.configDir) + "/genenvrules.conf";
   ech::defaultBuildSet = std::string(hpccBuildInfo.componentDir) + "/configxml/buildset.xml";
   return true;
}
