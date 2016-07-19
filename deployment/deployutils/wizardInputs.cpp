/*#############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
#############################################################################*/

/////////////////////////////////////////////////////////////////////////////
//
// WizardInputs.cpp : implementation file
//
/////////////////////////////////////////////////////////////////////////////
#include "wizardInputs.hpp"
#include "XMLTags.h"
#include "jencrypt.hpp"
#include "buildset.hpp"
#include "confighelper.hpp"
#include "build-config.h"

#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml/"
#define STANDARD_CONFIG_DIR CONFIG_DIR

CInstDetails::CInstDetails(StringBuffer compName, const StringArray &ipAssigned) : m_compName(compName)
{
    m_ipAssigned.clear();

    for (int i = 0; i < ipAssigned.ordinality(); i++)
    {
        m_ipAssigned.append(ipAssigned.item(i));
    }
}

//---------------------------------------------------------------------------
//  CWizardInputs
//---------------------------------------------------------------------------
CWizardInputs::CWizardInputs(const char* xmlArg,const char *service, 
                             IPropertyTree * cfg, 
                             MapStringTo<StringBuffer>* dirMap,
                             StringArray &arrBuildSetsWithAssignedIPs,
                             StringArray &arrAssignedIPs): m_service(service),
                             m_cfg(cfg), m_overrideDirs(dirMap), m_roxieOnDemand(true),
                             m_supportNodes(0), m_arrBuildSetsWithAssignedIPs(arrBuildSetsWithAssignedIPs), m_arrAssignedIPs(arrAssignedIPs)
{
  m_pXml.setown(createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>"));
}
//----------------------------------------------------------------------
CWizardInputs::~CWizardInputs()
{
  m_pXml.clear();
  HashIterator info(m_invalidServerCombo);
  for(info.first();info.isValid();info.next())
  {
    StringArray *a = *m_invalidServerCombo.mapToValue(&info.query());
    delete a;
  }
  HashIterator iter(m_compIpMap);
  ForEach(iter)
  {
    IMapping &cur = iter.query();
    CInstDetails* pInfo = m_compIpMap.mapToValue(&cur);
    pInfo->Release();
  }
  HashIterator sIter(m_compForTopology);
  for(sIter.first();sIter.isValid();sIter.next())
  {
    StringArray* a = *m_compForTopology.mapToValue(&sIter.query());
    delete a;
  }
}
//-----------------------------------------------------------------------
// SetEnvironment
//-----------------------------------------------------------------------
void CWizardInputs::setEnvironment()
{
  StringBuffer xpath;
  if(m_pXml->hasProp("@ipList"))
    formIPList(m_pXml->queryProp("@ipList"), m_ipaddress);

  if(m_pXml->hasProp("@supportNodes"))
  {
    m_supportNodes = atoi(m_pXml->queryProp("@supportNodes"));

    if (m_supportNodes)
    {
      if (m_ipaddress.length() > 0 && m_ipaddress.length() > m_supportNodes)
      {
        for(unsigned i = 0; i < m_supportNodes; i++)
          m_ipaddressSupport.append(m_ipaddress.item(i));

        m_ipaddress.removen(0, m_supportNodes);
      }
      else
        m_supportNodes = 0;
    }
  }

  if(m_pXml->hasProp("@roxieNodes"))
    m_roxieNodes = atoi(m_pXml->queryProp("@roxieNodes"));

  if(m_pXml->hasProp("@thorNodes"))
    m_thorNodes = atoi(m_pXml->queryProp("@thorNodes"));

  if(m_pXml->hasProp("@dbuser"))
    m_dbuser = m_pXml->queryProp("@dbuser");

  if(m_pXml->hasProp("@dbpassword"))
    m_dbpassword = m_pXml->queryProp("@dbpassword");

  m_thorSlavesPerNode = 1;
  if(m_pXml->hasProp("@slavesPerNode"))
    m_thorSlavesPerNode = atoi( m_pXml->queryProp("@slavesPerNode"));

  if (m_thorSlavesPerNode < 1)
    m_thorSlavesPerNode = 1;

  m_roxieOnDemand = m_pXml->getPropBool("@roxieOnDemand", true);

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile", m_service.str());
  const char* pConfFile = m_cfg->queryProp(xpath.str());
  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile",  m_service.str());
  const char* pEnvConfFile = m_cfg->queryProp(xpath.str());

  if (pConfFile && *pConfFile && pEnvConfFile && *pEnvConfFile)
  {
     Owned<IProperties> pParams = createProperties(pConfFile);
     Owned<IProperties> pEnvParams = createProperties(pEnvConfFile);
     StringBuffer sb, fileName;
     
     CConfigHelper *pConfigHelper = CConfigHelper::getInstance(m_cfg, m_service.str());

     if (pConfigHelper == NULL)
     {
         throw MakeStringException( -1 , "Error loading buildset from configuration");
     }

     IPropertyTree* pBuildSet = pConfigHelper->getBuildSetTree();

     if (strlen(pConfigHelper->getBuildSetFileName()) == 0 || pBuildSet == NULL)
     {
         throw MakeStringException( -1 , "The buildset file %s/%s does not exist", pConfigHelper->getBuildSetFilePath(), pConfigHelper->getBuildSetFileName());
     }

     m_buildSetTree.setown(pBuildSet);
     
     fileName.clear().append((pEnvParams->queryProp("configs") != NULL ? (sb.clear().append(pEnvParams->queryProp("configs")).append("/")): STANDARD_CONFIG_DIR));
     fileName.append((pParams->queryProp("wizardalgorithm") != NULL ? (sb.clear().append(pParams->queryProp("wizardalgorithm"))) : STANDARD_CONFIG_ALGORITHMFILE));
     
     if(fileName.length() && checkFileExists(fileName.str()))
       m_algProp.setown(createProperties(fileName.str()));
     else
       throw MakeStringException( -1 , "The algorithm file %s does not exists", fileName.str());
  }
  setWizardRules();
  setTopologyParam();
}

void CWizardInputs::setWizardRules()
{ 
   const char* roxieRedTypes[] = {"Full", "Circular", "None", "Overloaded"};
   m_roxieAgentRedType.clear().append("Circular");
   m_roxieAgentRedChannels = 2;
   m_roxieAgentRedOffset = 1;

   if(m_algProp)
   {
     CConfigHelper::getInstance()->addPluginsToGenEnvRules(m_algProp.get());
     Owned<IPropertyIterator> iter = m_algProp->getIterator();
     StringBuffer prop;
     ForEach(*iter)
     {
       m_algProp->getProp(iter->getPropKey(), prop.clear());
       if(prop.length() && prop.charAt(prop.length()-1) == ',')
          prop.setCharAt((prop.length()-1),' ');

       if(!strcmp(iter->getPropKey(), "max_comps_per_node"))
       {
         m_maxCompOnNode = atoi(prop.str());
       }
       else if(!strcmp(iter->getPropKey(), "avoid_combo"))
       {
         StringArray pairValue;
         pairValue.appendList(prop.str(), ",");
         if( pairValue.ordinality() > 0)
         {
           for( unsigned i = 0; i < pairValue.ordinality() ; i++)
           {
             StringArray eachpair;
             eachpair.appendList(pairValue.item(i), "-");
             if(eachpair.ordinality() == 2 )
             {
               StringArray* serverCompArr = 0;
               ForEachItemIn(x, eachpair)
               {
                 StringArrayPtr* pairServerArr = m_invalidServerCombo.getValue(eachpair.item(x));
                 if(pairServerArr)  
                 {
                   serverCompArr = (*pairServerArr);
                   serverCompArr->append(x == 0 ? eachpair.item(1): eachpair.item(0));
                 }
                 else
                 {
                   serverCompArr = new StringArray();
                   serverCompArr->append(x == 0 ? eachpair.item(1): eachpair.item(0));
                   m_invalidServerCombo.setValue(eachpair.item(x),serverCompArr);
                 }
               }
             }
           }
         }
       }
       else if(!strcmp (iter->getPropKey(),"do_not_generate"))
           m_doNotGenComp.appendList(prop.str(), ",");
       else if(!strcmp (iter->getPropKey(),"comps_on_all_nodes"))
           m_compOnAllNodes.appendList(prop.str(), ",");
       else if(!strcmp(iter->getPropKey(), "topology_for_comps"))
           m_clusterForTopology.appendList(prop.str(), ",");
       else if (!strcmp(iter->getPropKey(), "roxie_agent_redundancy"))
       {
         StringArray sbarr;
         sbarr.appendList(prop.str(), ",");
         if (sbarr.length() > 1)
         {
          int type = atoi(sbarr.item(0));
          if (type == 0)
            continue;

          if (type > 0 && type < 5)
            m_roxieAgentRedType.clear().append(roxieRedTypes[type-1]);
          else
            continue;

          m_roxieAgentRedChannels = atoi(sbarr.item(1));
          if (m_roxieAgentRedChannels <= 0)
            m_roxieAgentRedChannels = 1;

          if (sbarr.length() > 2)
          {
            m_roxieAgentRedOffset = atoi(sbarr.item(2));
            if (m_roxieAgentRedOffset <= 0)
              m_roxieAgentRedOffset = 1;
          }
          else
            m_roxieAgentRedOffset = 0;
         }
       }
     }
   }
}

CInstDetails* CWizardInputs::getServerIPMap(const char* compName, const char* buildSetName,const IPropertyTree* pEnvTree, unsigned numOfNodes)
{
  StringBuffer xPath;
  xPath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]",buildSetName);
  IPropertyTree* pBuildSet = pEnvTree->queryPropTree(xPath.str());
  
  CInstDetails* instDetails = NULL;
  if(pBuildSet)
  {
    if(m_doNotGenComp.find(buildSetName) != NotFound)
      return instDetails;

    if(m_compOnAllNodes.find(buildSetName) != NotFound)
      return instDetails;

    if (m_arrBuildSetsWithAssignedIPs.find(buildSetName) != NotFound)
        instDetails = new CInstDetails(compName, getIpAddrMap(buildSetName));

    if (m_ipaddress.ordinality() + m_supportNodes == 1 && instDetails == NULL)
    {
      instDetails = new CInstDetails(compName, m_ipaddress.item(0));
      m_compIpMap.setValue(buildSetName,instDetails);
      return instDetails;
    }
    else if (m_supportNodes == 1 && strcmp(buildSetName, "roxie") && strcmp(buildSetName, "thor" ) && instDetails == NULL)
    {
      instDetails = new CInstDetails(compName, m_ipaddressSupport.item(0));
      m_compIpMap.setValue(buildSetName,instDetails);
      return instDetails;
    }
    else
    {
      unsigned x = 0;

      for(; x < numOfNodes ; x++)
      {
        StringArray* pIpAddrMap = NULL;

        if (m_arrBuildSetsWithAssignedIPs.find(buildSetName) != NotFound || !(x == 0 && m_supportNodes > 0 && !strcmp(buildSetName, "thor")))
          pIpAddrMap = &getIpAddrMap(buildSetName);
        else
          pIpAddrMap = &m_ipaddressSupport;

        unsigned numOfIPSAlreadyTaken = m_arrBuildSetsWithAssignedIPs.find(buildSetName) == NotFound ? getCntForAlreadyAssignedIPS(buildSetName) : x;

        if( numOfIPSAlreadyTaken < pIpAddrMap->ordinality())
          addToCompIPMap(buildSetName, pIpAddrMap->item(numOfIPSAlreadyTaken), compName);
        else if (!applyOverlappingRules(compName, buildSetName, numOfIPSAlreadyTaken, pIpAddrMap))
          break;
      }
 
      if(m_compIpMap.find(buildSetName) != NULL)
      {
        instDetails = m_compIpMap.getValue(buildSetName);

        if( (instDetails->getIpAssigned()).ordinality() != numOfNodes)
        {
          StringBuffer sb("support");
          StringBuffer sbBuildSet(buildSetName);
          unsigned ips = m_ipaddressSupport.length();
          unsigned ipns = m_ipaddress.length();

          if (!strcmp(buildSetName, "thor") && m_supportNodes > 0)
          {
            if (x == 0)
            {
              sb.clear().append(m_supportNodes == 0?"non-support":"support");
              sbBuildSet.clear().append("Thor Master");
              numOfNodes = 1;
            }
            else
            {
              sb.clear().append("non-support");
              sbBuildSet.clear().append("Thor Slaves");
              numOfNodes--;
            }
          }
          else if (!strcmp(buildSetName, "roxie"))
            sb.clear().append("non-support ");

          throw MakeStringException(-1, \
            "Total nodes: %d (%d Support Nodes + %d Non-support Nodes)\nError: Cannot assign %d number of nodes for %s due to insufficient %s nodes available. Please enter different values", \
            ips + ipns, ips, ipns, numOfNodes, sbBuildSet.str(), sb.str());
        }
        else{
          return m_compIpMap.getValue(buildSetName);
        }
      }
    }
    return instDetails;
  }
  return NULL;
}

bool CWizardInputs::applyOverlappingRules(const char* compName,const char* buildSetName, unsigned startpos, StringArray* pIpAddrMap)
{
  StringArray dontAssign , ignoredForOverlap;
  bool assignedIP = false;
  CInstDetails* compPtr = NULL;
  
  if(m_invalidServerCombo.find(buildSetName) != NULL)
  {
    StringArray* serverCompArr = 0;
    StringArrayPtr* pairServerArr = m_invalidServerCombo.getValue(buildSetName);
    if(pairServerArr)
      serverCompArr = (*pairServerArr);
    for(unsigned i = 0 ; i < serverCompArr->ordinality() ; i++)
    {
      compPtr = m_compIpMap.getValue(serverCompArr->item(i));
      if(compPtr)
      {
        StringArray& ipArr = compPtr->getIpAssigned();
        ForEachItemIn(i, ipArr)
          dontAssign.append(ipArr.item(i));
      }
    }
  }
  //Since Roxie and thor might already have some ips asssigned we need to ignore those too.
  if(m_compIpMap.find(buildSetName)  != NULL)
  {
    compPtr = m_compIpMap.getValue(buildSetName);
    StringArray& ipArr = compPtr->getIpAssigned();
    ForEachItemIn(i, ipArr)
      dontAssign.append(ipArr.item(i));
  }

  unsigned pos = startpos % pIpAddrMap->ordinality();

  for (unsigned j=pos; j < pos + pIpAddrMap->ordinality(); j++)
  {
    unsigned ii = (j >= pIpAddrMap->ordinality()) ? 0 : j;
    count_t ipAssignedCount = getNumOfInstForIP(pIpAddrMap->item(ii));

    if(dontAssign.ordinality() > 0)
    {
      if( dontAssign.find(pIpAddrMap->item(ii)) == NotFound)
      {
        if(ipAssignedCount >= m_maxCompOnNode )
        {
          ignoredForOverlap.append(pIpAddrMap->item(ii));
        }
        else
        {
          assignedIP = true;
          addToCompIPMap(buildSetName, pIpAddrMap->item(ii), compName);
          break;
        }
      }
    }
    else
    {
      if(ipAssignedCount >= m_maxCompOnNode )
      {
        ignoredForOverlap.append(pIpAddrMap->item(ii));
      } 
      else
      {
        assignedIP = true;
        addToCompIPMap(buildSetName, pIpAddrMap->item(ii), compName);
        break;
      }
    }
  }
  if(!assignedIP && ignoredForOverlap.ordinality() > 0)
  {          
    addToCompIPMap(buildSetName, ignoredForOverlap.item(0), compName);
    assignedIP = true;
  }

  return assignedIP;
}

count_t CWizardInputs::getNumOfInstForIP(StringBuffer ip)
{
  count_t cnt = 0;
  HashIterator ips(m_compIpMap);
  ForEach(ips)
  {
    CInstDetails* comp = m_compIpMap.mapToValue(&ips.query());
    StringArray& ipArray = comp->getIpAssigned();
    if(ipArray.find(ip) != NotFound)
      cnt++;  
  }
  return cnt;
}

bool CWizardInputs::generateEnvironment(StringBuffer& envXml)
{
  if(m_algProp)
  {
    Owned<IPropertyTree> pEnvTree = createEnvironment();
    if(pEnvTree)
    {
      toXML(pEnvTree,envXml, 0, XML_SortTags | XML_Format);
    }
  }
  else
  {
    DBGLOG("not yet decided");//use default algorithm
  }
  return true;
}

IPropertyTree* CWizardInputs::createEnvironment()
{
  StringBuffer xpath, sbTemp, name ;
 
  sbTemp.clear().appendf("<%s><%s></%s>", XML_HEADER, XML_TAG_ENVIRONMENT, XML_TAG_ENVIRONMENT);
  IPropertyTree* pNewEnvTree = createPTreeFromXMLString(sbTemp.str());

  IPropertyTree* pSettings = pNewEnvTree->addPropTree(XML_TAG_ENVSETTINGS, createPTree());
  xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
  const char* pConfFile = m_cfg->queryProp(xpath.str());

  xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvConfFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
  const char* tmp = m_cfg->queryProp(xpath.str());
  if (tmp && *tmp)
  {
    Owned<IProperties> pParams = createProperties(tmp);
    Owned<IPropertyIterator> iter = pParams->getIterator();
     
    ForEach(*iter)
    {
      StringBuffer prop;
      pParams->getProp(iter->getPropKey(), prop);
      pSettings->addProp(iter->getPropKey(), prop.length() ? prop.str():"");
    }
  }

  Owned<IPropertyTree> pProgramTree = createPTreeFromIPT(m_buildSetTree);
  pNewEnvTree->addPropTree(XML_TAG_PROGRAMS, createPTreeFromIPT(pProgramTree->queryPropTree("./" XML_TAG_PROGRAMS)));

  Owned<IPropertyTree> pCompTree = createPTree(XML_TAG_HARDWARE);
  generateHardwareHeaders(pNewEnvTree, sbTemp, false, pCompTree);
  pCompTree->removeProp(XML_TAG_COMPUTER);
  xpath.clear().appendf("./%s/%s", XML_TAG_COMPUTERTYPE, XML_ATTR_MEMORY);
  pCompTree->removeProp(xpath.str());
  xpath.clear().appendf("./%s/%s", XML_TAG_COMPUTERTYPE, XML_ATTR_NICSPEED);
  pCompTree->removeProp(xpath.str());
      
  xpath.clear().append(XML_TAG_SWITCH).append("/").append(XML_ATTR_NAME);
  pCompTree->setProp(xpath.str(), "Switch") ;
  
  xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_NAME);   
  pCompTree->setProp(xpath.str(), "localdomain");
  xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_PASSWORD);   
  pCompTree->setProp(xpath.str(), m_pXml->queryProp("@password"));
  xpath.clear().append(XML_TAG_DOMAIN).append("/").append(XML_ATTR_USERNAME);   
  pCompTree->setProp(xpath.str(), m_pXml->queryProp("@username"));

  xpath.clear().appendf("./%s/@snmpSecurityString", XML_TAG_DOMAIN);
  pCompTree->removeProp(xpath.str());
  
  xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_COMPUTERTYPE); 
  pCompTree->setProp(xpath.str(), "linuxmachine"); 
  xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_MANUFACTURER); 
  pCompTree->setProp(xpath.str(), "unknown"); 
  xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_NAME); 
  pCompTree->setProp(xpath.str(), "linuxmachine"); 
  xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_OPSYS);    
  pCompTree->setProp(xpath.str(), "linux"); 
  xpath.clear().append(XML_TAG_COMPUTERTYPE).append("/").append(XML_ATTR_NICSPEED);
  pCompTree->setProp(xpath.str(), "1000");
  unsigned x;
  IpAddress ipaddr;

  for(unsigned i = 0; i < m_ipaddressSupport.ordinality(); i++)
  {
    IPropertyTree* pComputer = pCompTree->addPropTree(XML_TAG_COMPUTER,createPTree());
    ipaddr.ipset(m_ipaddressSupport.item(i));
    ipaddr.getNetAddress(sizeof(x),&x);
    name.clear().appendf("node%03d%03d", (x >> 16) & 0xFF, (x >> 24) & 0xFF);
    getUniqueName(pCompTree, name, XML_TAG_COMPUTER, "");
    pComputer->addProp(XML_ATTR_COMPUTERTYPE, "linuxmachine");
    pComputer->addProp(XML_ATTR_DOMAIN, "localdomain");
    pComputer->addProp(XML_ATTR_NAME, name.str());
    pComputer->addProp(XML_ATTR_NETADDRESS, m_ipaddressSupport.item(i));
  }

  for(unsigned i = 0; i < m_ipaddress.ordinality(); i++)
  {
    IPropertyTree* pComputer = pCompTree->addPropTree(XML_TAG_COMPUTER,createPTree());
    ipaddr.ipset(m_ipaddress.item(i));
    ipaddr.getNetAddress(sizeof(x),&x);
    name.clear().appendf("node%03d%03d", (x >> 16) & 0xFF, (x >> 24) & 0xFF);
    getUniqueName(pCompTree, name, XML_TAG_COMPUTER, "");
    pComputer->addProp(XML_ATTR_COMPUTERTYPE, "linuxmachine");
    pComputer->addProp(XML_ATTR_DOMAIN, "localdomain");
    pComputer->addProp(XML_ATTR_NAME, name.str());
    pComputer->addProp(XML_ATTR_NETADDRESS, m_ipaddress.item(i));
  }

  for(unsigned i = 0; i < m_arrBuildSetsWithAssignedIPs.ordinality(); i++)
  {
      const StringArray &strIPs = getIpAddrMap(m_arrBuildSetsWithAssignedIPs.item(i));

      for (unsigned i2 = 0; i2 < strIPs.ordinality(); i2++)
      {
          VStringBuffer strXPath("./%s/[%s=\"%s\"]", XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, strIPs.item(i2));
          if (pCompTree->hasProp(strXPath.str()))
              continue;

          IPropertyTree* pComputer = pCompTree->addPropTree(XML_TAG_COMPUTER,createPTree());
          ipaddr.ipset(strIPs.item(i2));
          ipaddr.getNetAddress(sizeof(x),&x);
          name.setf("node%03d%03d", (x >> 16) & 0xFF, (x >> 24) & 0xFF);
          getUniqueName(pCompTree, name, XML_TAG_COMPUTER, "");
          pComputer->addProp(XML_ATTR_COMPUTERTYPE, "linuxmachine");
          pComputer->addProp(XML_ATTR_DOMAIN, "localdomain");
          pComputer->addProp(XML_ATTR_NAME, name.str());
          pComputer->addProp(XML_ATTR_NETADDRESS, strIPs.item(i2));
      }
  }

  pNewEnvTree->addPropTree(XML_TAG_HARDWARE, createPTreeFromIPT(pCompTree));
  //Before we generate software tree check for dependencies of component for do_not_generate ,roxie, thor
  checkForDependencies();
  generateSoftwareTree(pNewEnvTree);
  return pNewEnvTree;
}

void CWizardInputs::generateSoftwareTree(IPropertyTree* pNewEnvTree)
{
  StringBuffer xpath;

  if(m_buildSetTree)
  {
    bool ovrLog = true, ovrRun = true;
    if (m_overrideDirs && m_overrideDirs->count() > 0)
    {
      HashIterator iter(*m_overrideDirs);

      ForEach(iter)
      {
        IMapping &cur = iter.query();
        StringBuffer* dirvalue = m_overrideDirs->mapToValue(&cur);
        const char * key = (const char*)cur.getKey();
        xpath.clear().appendf(XML_TAG_SOFTWARE"/Directories/Category[@name='%s']", key);
        if (!strcmp(key, "log"))
          ovrLog = false;
        else if (!strcmp(key, "run"))
          ovrRun = false;

        IPropertyTree* pDir = m_buildSetTree->queryPropTree(xpath.str());
        if (pDir)
          pDir->setProp("@dir", dirvalue->str());
        else
        {
          pDir = m_buildSetTree->queryPropTree(XML_TAG_SOFTWARE"/Directories/")->addPropTree("Category", createPTree());
          pDir->setProp(XML_ATTR_NAME, (const char*)cur.getKey());
          pDir->setProp("@dir", dirvalue->str());
        }
      }
    }

    pNewEnvTree->addPropTree(XML_TAG_SOFTWARE,createPTreeFromIPT(m_buildSetTree->queryPropTree("./" XML_TAG_SOFTWARE)));
    xpath.clear().appendf("%s/%s/%s[%s='%s']/LocalEnvConfFile", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, m_service.str());
    const char* tmp = m_cfg->queryProp(xpath.str());
    if (tmp && *tmp)
    {
      Owned<IProperties> pParams = createProperties(tmp);
      updateDirsWithConfSettings(pNewEnvTree, pParams, ovrLog, ovrRun);
    }

    const char* firstComp = "esp";
    xpath.clear().appendf("./%s/%s/%s/[@name=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, firstComp);
    IPropertyTree* pEspBuildSet = m_buildSetTree->queryPropTree(xpath.str());
    if (pEspBuildSet)
      addComponentToSoftware(pNewEnvTree, pEspBuildSet);

    xpath.clear().appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);
    Owned<IPropertyTreeIterator> buildSetInsts = m_buildSetTree->getElements(xpath.str());

    ForEach(*buildSetInsts)
    {
      IPropertyTree* pBuildSet = &buildSetInsts->query();
      const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);

      if (strcmp(firstComp, buildSetName))
        addComponentToSoftware(pNewEnvTree, &buildSetInsts->query());
    }

    getEspBindingInformation(pNewEnvTree);
    addTopology(pNewEnvTree);
    getDefaultsForWizard(pNewEnvTree);
  }
}

void CWizardInputs::addInstanceToTree(IPropertyTree* pNewEnvTree, StringBuffer attrName, const char* processName, const char* buildSetName, const char* instName)
{
  StringBuffer sb, sbl, compName, xpath, nodeName;
  xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, attrName.str());
  IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
  if(pHardTemp)
    nodeName.clear().append(pHardTemp->queryProp("./" XML_ATTR_NAME));//NodeName

  xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_BUILDSET, buildSetName);
  
  IPropertyTree* pComp = pNewEnvTree->queryPropTree(xpath.str());
  compName.clear().append(pComp->queryProp(XML_ATTR_NAME));//compName

  sb.clear().appendf("<Instance buildSet=\"%s\" compName=\"%s\" ><Instance name=\"%s\" /></Instance>", buildSetName, compName.str(), nodeName.str());
  Owned<IPropertyTree> pInstance = createPTreeFromXMLString(sb.str());

  if(pInstance)
    addInstanceToCompTree(pNewEnvTree, pInstance, sbl.clear(), sb.clear(),NULL);
  
  xpath.clear().appendf("./%s/%s[%s=\"%s\"]/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_NAME, compName.str(), XML_TAG_INSTANCE, XML_ATTR_COMPUTER, nodeName.str());
  IPropertyTree* pInst = pNewEnvTree->queryPropTree(xpath.str());
  if(pInst)
  {
    pInst->addProp(XML_ATTR_NAME, instName);
  }
}

void CWizardInputs::getDefaultsForWizard(IPropertyTree* pNewEnvTree)
{
  StringBuffer xpath, tempName, value;
  Owned<IPropertyTree> pBuildTree = createPTreeFromIPT(pNewEnvTree->queryPropTree("./" XML_TAG_PROGRAMS));
  xpath.clear().appendf("./%s/%s/", XML_TAG_BUILD, XML_TAG_BUILDSET);
  Owned<IPropertyTreeIterator> buildSetInsts = pBuildTree->getElements(xpath.str());

  ForEach(*buildSetInsts)
  {
    IPropertyTree* pBuildSet = &buildSetInsts->query();
    StringBuffer buildSetPath, compName;
    const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
    const char* xsdFileName = pBuildSet->queryProp(XML_ATTR_SCHEMA);
    const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
    if(processName && *processName && buildSetName && * buildSetName && xsdFileName && *xsdFileName)
    {
      Owned<IPropertyTree> pSchema = loadSchema(pBuildTree->queryPropTree("./" XML_TAG_BUILD "[1]"), pBuildSet, buildSetPath, NULL);

      Owned<IPropertyTree> pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, buildSetName, m_cfg, m_service.str(), true, true, this);
      xpath.clear().appendf("./%s/%s/[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_BUILDSET, buildSetName);
      IPropertyTree* pSWCompTree = pNewEnvTree->queryPropTree(xpath.str());

      if(pSWCompTree && pCompTree)
      {
        Owned<IAttributeIterator> iAttr = pCompTree->getAttributes();
        ForEach(*iAttr)
        {
          if( pSWCompTree->hasProp(iAttr->queryName()) && strcmp(iAttr->queryName(), "@buildSet") != 0)
          {
            if (!strcmp(iAttr->queryName(), XML_ATTR_NAME))
            {
              StringBuffer sbxpath, sbnew, sbMsg;
              sbnew.clear().append(iAttr->queryValue());
              sbxpath.clear().append(processName);
              getUniqueName(pNewEnvTree, sbnew, sbxpath.str(), XML_TAG_SOFTWARE);
              bool ret = checkComponentReferences(pNewEnvTree, pSWCompTree, pSWCompTree->queryProp(iAttr->queryName()), sbMsg, sbnew.str());

              if (ret)
                pSWCompTree->setProp(iAttr->queryName(), iAttr->queryValue());
            }
            else
              pSWCompTree->setProp(iAttr->queryName(), iAttr->queryValue());
          }
        }
    
        //Now adding elements
        Owned<IPropertyTreeIterator> iterElems = pCompTree->getElements("*");
        ForEach (*iterElems)
        {
          IPropertyTree* pElem = &iterElems->query();

          Owned<IAttributeIterator> iAttr = pElem->getAttributes();
          
          ForEach(*iAttr)
          {
            IPropertyTree* pNewSubElem = pSWCompTree->queryPropTree(pElem->queryName());
            if (!pNewSubElem)
            {
              pNewSubElem = pSWCompTree->addPropTree(pElem->queryName(), createPTreeFromIPT(pElem));
              break;
            }
            else
            {
              Owned<IPropertyTreeIterator> srcElems = pSWCompTree->getElements(pElem->queryName());
              IPropertyTree* pSrcElem = NULL;
              ForEach(*srcElems)
              {
                pSrcElem = &srcElems->query();
                Owned<IAttributeIterator> iAttrElem = pElem->getAttributes();
                ForEach(*iAttrElem)
                {
                   const char* attrName = iAttrElem->queryName();

                   if (pSrcElem->hasProp(attrName))
                     pSrcElem->setProp(attrName, iAttrElem->queryValue());

                   Owned<IPropertyTreeIterator> iterSubElems = pElem->getElements("*");
                   ForEach (*iterSubElems)
                   {
                     IPropertyTree* pSubElem = &iterSubElems->query();

                     Owned<IPropertyTreeIterator> srcSubElems = pSWCompTree->getElements(pSubElem->queryName());
                     IPropertyTree* pSrcSubElem = NULL;
                     ForEach(*srcSubElems)
                     {
                       pSrcSubElem = &srcSubElems->query();

                       Owned<IAttributeIterator> iAttrElem = pSubElem->getAttributes();
                       ForEach(*iAttrElem)
                       {
                         const char* attrName = iAttrElem->queryName();

                         if (pSrcSubElem->hasProp(attrName))
                           pSrcSubElem->setProp(attrName, iAttrElem->queryValue());
                       }
                      }
                    }
                  }
                }
              }
            }
          }
        }
     }
   }
}


void CWizardInputs::addToCompIPMap(const char* buildSetName, const char* value, const char* compName)
{
  CInstDetails* pInst = NULL;
  if(m_compIpMap.find(buildSetName) != NULL)
  {
    pInst = m_compIpMap.getValue(buildSetName);
    (pInst->getIpAssigned()).append(value);
  }
  else
  {
    pInst = new CInstDetails();
    pInst->setParams(compName, value);
    m_compIpMap.setValue(buildSetName, pInst);
  }
}

unsigned CWizardInputs::getCntForAlreadyAssignedIPS(const char* buildSetName)
{
   unsigned cnt = 0;
   CInstDetails* pInstRoxie = NULL, *pInstThor = NULL;

   if (!strcmp(buildSetName, "roxie") || !strcmp(buildSetName, "thor" ))
   {
     if (m_compIpMap.find("roxie") != NULL)
     {
       CInstDetails* pInst = m_compIpMap.getValue("roxie");
       cnt += pInst->getIpAssigned().length();
     }

     if (m_compIpMap.find("thor") != NULL)
     {
       CInstDetails* pInst = m_compIpMap.getValue("thor");
       cnt += pInst->getIpAssigned().length();
     }

     return cnt;
   }
   else
   {
     if (m_compIpMap.find("roxie") != NULL)
       pInstRoxie = m_compIpMap.getValue("roxie");

     if (m_compIpMap.find("thor") != NULL)
       pInstThor = m_compIpMap.getValue("thor");
   }

   HashIterator ips(m_compIpMap);
   ForEach(ips)
   {
     CInstDetails* comp = m_compIpMap.mapToValue(&ips.query());
     if (pInstRoxie == comp || pInstThor == comp)
       continue;
     StringArray& ipArray = comp->getIpAssigned();
     cnt += ipArray.length();
   }
   return cnt;
}

void CWizardInputs::addRoxieThorClusterToEnv(IPropertyTree* pNewEnvTree, CInstDetails* pInstDetails, const char* buildSetName, bool genRoxieOnDemand)
{
  StringBuffer xmlForRoxiePorts, xmlForRoxieServers, xpath, compName, computerName, msg;
    
  if(!strcmp(buildSetName, "roxie"))
  {
    //Before proceeding remove the roxieserver already added to env via xsd.
    xpath.clear().appendf("./%s/%s/%s", XML_TAG_SOFTWARE, XML_TAG_ROXIECLUSTER, XML_ATTR_NAME);
    compName.clear().append(pNewEnvTree->queryProp(xpath.str()));
    
    xmlForRoxiePorts.clear().appendf("<RoxieData type=\"RoxieFarm\" parentName=\"\" roxieName=\"%s\" ", compName.str());

    if (genRoxieOnDemand)
      xmlForRoxiePorts.append("port=\"0\" >");
    else
      xmlForRoxiePorts.append(">");

    if (m_roxieNodes >= 1)
      xmlForRoxieServers.clear().appendf("<RoxieData type=\"None\" roxieName=\"%s\" >", compName.str());

    if(pInstDetails)
    {
      StringArray& ipAssignedToComp = pInstDetails->getIpAssigned();

      xmlForRoxieServers.append("<Instances>");
      ForEachItemIn(i, ipAssignedToComp)
      {
        xpath.clear().appendf("./%s/%s/[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, ipAssignedToComp.item(i));
        IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
        if(pHardTemp){
         xmlForRoxiePorts.appendf("<Component name=\"%s\" />", pHardTemp->queryProp("./@name"));
         xmlForRoxieServers.appendf("<Instance name=\"%s\"/>", pHardTemp->queryProp("./@name"));
        }
      }
      xmlForRoxieServers.append("</Instances>");
      xmlForRoxiePorts.append("</RoxieData>");
      xmlForRoxieServers.append("</RoxieData>");
      handleRoxieOperation(pNewEnvTree, "AddRoxieFarm", xmlForRoxiePorts.str());

      if (!genRoxieOnDemand)
        handleRoxieOperation(pNewEnvTree, "RoxieSlaveConfig" ,xmlForRoxieServers.str());
    }
    xpath.clear().appendf("./%s/%s[%s=\"%s\"]/%s[%s=\"\"]", XML_TAG_SOFTWARE, XML_TAG_ROXIECLUSTER, XML_ATTR_NAME, compName.str(), XML_TAG_ROXIE_SERVER, XML_ATTR_NETADDRESS);
    pNewEnvTree->removeProp(xpath.str());
  }
  else if(!strcmp(buildSetName, "thor"))
  {
    //We need only one master
    StringBuffer masterIP, xml;
    xpath.clear().appendf("./%s/%s/%s", XML_TAG_SOFTWARE, XML_TAG_THORCLUSTER, XML_ATTR_NAME);
    compName.clear().append(pNewEnvTree->queryProp(xpath.str()));
   
    if(pInstDetails)
    {
      StringArray& ipAssignedToComp = pInstDetails->getIpAssigned();

      if(!ipAssignedToComp.empty())
        masterIP.clear().append(ipAssignedToComp.item(0));
    
      xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, masterIP.str());
      IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
      if(pHardTemp)
        xml.clear().appendf("<ThorData type=\"Master\" name=\"%s\" validateComputers=\"false\" skipExisting=\"false\" > <Computer name=\"%s\" /></ThorData>", compName.str(), pHardTemp->queryProp("./@name"));
      handleThorTopologyOp(pNewEnvTree, "Add", xml.str(), msg);

      //Now add Slave 
      xml.clear().appendf("<ThorData type=\"Slave\" name=\"%s\" validateComputers=\"false\" slavesPerNode=\"%d\" skipExisting=\"false\" >", compName.str(), m_thorSlavesPerNode);
      unsigned numOfNodes = ipAssignedToComp.ordinality() == 1 ? 0 : 1;

      for( ; numOfNodes < ipAssignedToComp.ordinality() ; numOfNodes++)
      {
        xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, ipAssignedToComp.item(numOfNodes));
        IPropertyTree* pHardTemp = pNewEnvTree->queryPropTree(xpath.str());
        if(pHardTemp)
          xml.appendf("<Computer name=\"%s\" />", pHardTemp->queryProp("./@name"));
      }
      xml.append("</ThorData>");
      handleThorTopologyOp(pNewEnvTree, "Add" , xml.str(), msg);
    }
  }
}

void CWizardInputs::getEspBindingInformation(IPropertyTree* pNewEnvTree)
{
   StringBuffer xpath, sbDefn, xmlArg, compName, sbNewName;
   Owned<IPropertyTreeIterator> espProcessIter = pNewEnvTree->getElements("./" XML_TAG_SOFTWARE "/" XML_TAG_ESPPROCESS);
        
   ForEach(*espProcessIter)
   {
     IPropertyTree* pEspProcess = &espProcessIter->query();
     compName.clear().append(pEspProcess->queryProp(XML_ATTR_NAME));
     xpath.clear().appendf("./%s/%s/%s[@processName=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_TAG_ESPSERVICE);
     Owned<IPropertyTreeIterator> espServiceIter = pNewEnvTree->getElements(xpath.str());
       
     ForEach (*espServiceIter)
     {
       IPropertyTree* pEspService = &espServiceIter->query();
       if(pEspService)
       {
         StringBuffer espServiceName;
         espServiceName.appendf("my%s", pEspService->queryProp("@name"));
                
         xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, espServiceName.str());
         IPropertyTree* pEspServiceInSWTree = pNewEnvTree->queryPropTree(xpath.str());
         if(pEspServiceInSWTree)
         {
           xpath.clear().append("./Properties/@defaultPort");
           const char* port = pEspService->queryProp(xpath.str());
           xpath.clear().append("./Properties/@defaultResourcesBasedn");
           const char* resourceBasedn = pEspService->queryProp(xpath.str());

           const char* buildSetName = pEspService->queryProp(XML_ATTR_NAME);
           const char* processName = pEspService->queryProp(XML_ATTR_PROCESS_NAME);

           StringBuffer buildSetPath;
           Owned<IPropertyTree> pSchema = loadSchema(pNewEnvTree->queryPropTree("./Programs/Build[1]"), pEspService, buildSetPath, NULL);

           xmlArg.clear().appendf("<EspServiceBindings type=\"EspBinding\" compName=\"%s\" > <Item name=\"%s\" params=\"pcType=EspProcess::pcName=%s::subType=EspBinding::subTypeKey=%s \"/></EspServiceBindings>", compName.str(), espServiceName.str(), compName.str(), espServiceName.str());
           addEspBindingInformation(xmlArg, pNewEnvTree, sbNewName, NULL, m_cfg, m_service.str());
           
           xpath.clear().appendf("./%s/%s/%s/[%s=\"\"]", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPBINDING, XML_ATTR_SERVICE);
           IPropertyTree* pEspBindingInfo = pNewEnvTree->queryPropTree(xpath.str());
                   
           pEspBindingInfo->setProp(XML_ATTR_NAME,(espServiceName.toLowerCase()).str());
           pEspBindingInfo->setProp(XML_ATTR_SERVICE,(espServiceName.toLowerCase()).str());
           pEspBindingInfo->setProp(XML_ATTR_PORT, port );
           pEspBindingInfo->setProp("@resourcesBasedn",resourceBasedn);

           xpath.clear().appendf("%s/%s[%s=\"%s\"]/Properties", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, (espServiceName.toLowerCase()).str());
           IPropertyTree* pSvcProps = pNewEnvTree->queryPropTree(xpath.str());

           Owned<IPropertyTree> pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, buildSetName, m_cfg, m_service.str(), true, false, 0);

           Owned<IPropertyTreeIterator> i = pSvcProps->getElements("Authenticate");
           ForEach(*i)
           {
             IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
             mergeAttributes(pAuthCopy, pCompTree->queryPropTree("Authenticate"));
             IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("Authenticate", pAuthCopy);
           }

           i.setown( pSvcProps->getElements("AuthenticateFeature") );
           ForEach(*i)
           {
             IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
             //Adding authentication to true for espbinding.
             pAuthCopy->addProp("@authenticate","Yes");
             mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateFeature"));
             IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("AuthenticateFeature", pAuthCopy);
           }
           i.setown( pSvcProps->getElements("AuthenticateSetting") );
           ForEach(*i)
           {
             IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
             mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateSetting"));
             IPropertyTree* pNewNode = pEspBindingInfo->addPropTree("AuthenticateSetting", pAuthCopy);
           }
         }
       }
    }
  }
}

void CWizardInputs::addTopology(IPropertyTree* pNewEnvTree)
{
  StringBuffer xpath;
  if(!pNewEnvTree->hasProp("./" XML_TAG_SOFTWARE "/" XML_TAG_TOPOLOGY))
    pNewEnvTree->addPropTree("./" XML_TAG_SOFTWARE "/" XML_TAG_TOPOLOGY, createPTree());
 
  HashIterator sIter(m_compForTopology);
  for(sIter.first();sIter.isValid();sIter.next())
  {
    IMapping &cur = sIter.query();
    IPropertyTree* pCluster = createTopologyForComp(pNewEnvTree,(const char *) cur.getKey());
    if(pCluster)
       pNewEnvTree->addPropTree("./" XML_TAG_SOFTWARE "/" XML_TAG_TOPOLOGY "/Cluster", pCluster);
  }
}

IPropertyTree* CWizardInputs::createTopologyForComp(IPropertyTree* pNewEnvTree, const char* component)
{
   StringBuffer xmlTag, xpath , compName, clusterStr;
   if(!strcmp(component, "roxie"))
     xmlTag.clear().append(XML_TAG_ROXIECLUSTER);
   else if(!strcmp(component, "thor"))
     xmlTag.clear().append(XML_TAG_THORCLUSTER);
   else if(!strcmp(component, "hthor"))
     xmlTag.clear().append("hthor");
     
     
   xpath.clear().appendf("./%s/%s[1]/%s", XML_TAG_SOFTWARE, xmlTag.str(), XML_ATTR_NAME);

   clusterStr.clear().appendf("<Cluster name=\"%s\" prefix=\"%s\" alias=\"\"></Cluster>", component, component);
 
   IPropertyTree* pCluster = createPTreeFromXMLString(clusterStr.str());
   if(pCluster)
   {
     if(pNewEnvTree->hasProp(xpath.str()))
     {
       IPropertyTree* pComponent = pCluster->addPropTree(xmlTag.str(), createPTree());
       pComponent->addProp(XML_ATTR_PROCESS, pNewEnvTree->queryProp(xpath.str()));
     }

     if(m_compForTopology.find(component) != NULL)
     {
        StringArray* clusterCompEle = 0;
        StringArrayPtr* clusterPair = m_compForTopology.getValue(component);
        if(clusterPair)
        {
            clusterCompEle = (*clusterPair);
            for(unsigned i = 0 ; i < clusterCompEle->ordinality() ; i++)
            {
              const char* eachClusterElem = clusterCompEle->item(i);
              xpath.clear().appendf("./%s/%s/%s[%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_NAME, eachClusterElem);
              IPropertyTree* pBuildset = pNewEnvTree->queryPropTree(xpath.str());
              if(pBuildset)
              {
                const char* processName = pBuildset->queryProp(XML_ATTR_PROCESS_NAME);
                if(processName && *processName)
                {
                   IPropertyTree* pElement = pCluster->addPropTree(processName,createPTree());
                   xpath.clear().appendf("./%s/%s[1]/%s", XML_TAG_SOFTWARE, processName, XML_ATTR_NAME);
                   if(pElement && pNewEnvTree->hasProp(xpath.str()))
                     pElement->addProp(XML_ATTR_PROCESS, pNewEnvTree->queryProp(xpath.str()));
                }
              }
            }
            
         }
         clusterCompEle->kill();
     }
     return pCluster;
   }
   else
     return NULL;
}

void CWizardInputs::checkForDependencies()
{
  StringBuffer xpath; 
  
  if(m_buildSetTree)
  {
    xpath.clear().appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);
    Owned<IPropertyTreeIterator> buildSetInsts = m_buildSetTree->getElements(xpath.str());
    ForEach(*buildSetInsts)
    {
      IPropertyTree* pBuildSet = &buildSetInsts->query();
      const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
      unsigned numOfNodesNeeded = 1;

      if((!strcmp(buildSetName,"roxie") && m_roxieNodes == 0 )|| (!strcmp(buildSetName,"thor")&& m_thorNodes == 0)){
          numOfNodesNeeded = 0;
          m_doNotGenComp.append(buildSetName);
          m_compForTopology.remove("thor_roxie");
          if (!strcmp(buildSetName,"thor"))
             m_compForTopology.remove("thor");
      }

      if(numOfNodesNeeded == 0 || (m_doNotGenComp.find(buildSetName) != NotFound))
      {
        if(m_compForTopology.find(buildSetName) != NULL )
           m_compForTopology.remove(buildSetName);
        checkAndAddDependComponent(buildSetName);
      }
    }
  }
}

void CWizardInputs::checkAndAddDependComponent(const char* key)
{
  StringBuffer paramEntry(key);
  paramEntry.append("_dependencies");
  if(m_algProp)
  {
    if(m_algProp->hasProp(paramEntry.str()))
    {
      StringArray sArray;
      sArray.appendList(m_algProp->queryProp(paramEntry.str()), ";");
      ForEachItemIn(x, sArray)
      {
        if(m_doNotGenComp.find(sArray.item(x)) == NotFound)
        {
          m_doNotGenComp.append(sArray.item(x));
          checkAndAddDependComponent(sArray.item(x));
        }
      }
    }
  }
}

unsigned CWizardInputs::getNumOfNodes(const char* compName)
{
  if(m_compIpMap.find(compName) != NULL)
  {
    CInstDetails* pInst = NULL;
    pInst = m_compIpMap.getValue(compName);
    StringArray& ipArr = pInst->getIpAssigned();
    return ipArr.ordinality();
  }
  return 0;
}

void CWizardInputs::setTopologyParam()
{
  if(m_clusterForTopology.ordinality() > 0)
  {
    StringBuffer topologySec;
    ForEachItemIn(x, m_clusterForTopology)
    { 
      topologySec.clear().appendf("%s_topology",m_clusterForTopology.item(x));
      const char * elemForCluster = m_algProp->queryProp(topologySec.str());
      if(elemForCluster && *elemForCluster)
      {
        StringArray* compClusterArr = new StringArray();
       
        StringArray clusterElemArr;
        clusterElemArr.appendList(elemForCluster, ",");
        ForEachItemIn(y, clusterElemArr)
          compClusterArr->append(clusterElemArr.item(y));
        m_compForTopology.setValue(m_clusterForTopology.item(x),compClusterArr);
      }
    }
  }
}

void CWizardInputs::addComponentToSoftware(IPropertyTree* pNewEnvTree, IPropertyTree* pBuildSet)
{
  if (!pBuildSet)
    return;

  StringBuffer xpath, sbNewName;
  StringBuffer buildSetPath, compName, assignedIP, sbl;
  const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
  const char* xsdFileName = pBuildSet->queryProp(XML_ATTR_SCHEMA);
  const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
  StringBuffer deployable = pBuildSet->queryProp("@" TAG_DEPLOYABLE);
  unsigned numOfIpNeeded = 1;

  if (m_doNotGenComp.find(buildSetName) != NotFound )
    return;

  if (processName && *processName && buildSetName && * buildSetName && xsdFileName && *xsdFileName)
  {
    Owned<IPropertyTree> pSchema = loadSchema(m_buildSetTree->queryPropTree("./" XML_TAG_PROGRAMS "/" XML_TAG_BUILD "[1]"), pBuildSet, buildSetPath, NULL);
    IPropertyTree* pCompTree = generateTreeFromXsd(pNewEnvTree, pSchema, processName, buildSetName, m_cfg, m_service.str(), false);

    sbNewName.clear();
    if (strstr(buildSetName ,"my") == NULL && (strcmp(buildSetName, "topology") != 0))
      sbNewName.append("my");

    addComponentToEnv(pNewEnvTree, buildSetName, sbNewName, pCompTree);

    if (!strcmp(processName, XML_TAG_ESPSERVICE) || !strcmp(processName, XML_TAG_PLUGINPROCESS))
      processName = buildSetName;

    if (strcmp(deployable,"no") != 0)
    {
      if (m_compOnAllNodes.find(buildSetName) != NotFound)
      {
        for (unsigned i = 0; i < m_ipaddressSupport.ordinality(); i++)
        {
          sbl.clear().appendf("s").append(i+1);
          assignedIP.clear().append(m_ipaddressSupport.item(i));
          addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName,sbl.str());
        }

        for (unsigned i = 0; i < m_ipaddress.ordinality(); i++)
        {
          sbl.clear().appendf("s").append(m_ipaddressSupport.ordinality() + i+1);
          assignedIP.clear().append(m_ipaddress.item(i));
          addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName,sbl.str());
        }

        int nCount = 1;
        for (unsigned i = 0; i < m_arrBuildSetsWithAssignedIPs.ordinality(); i++)
        {
            const StringArray &strIPList = getIpAddrMap(m_arrBuildSetsWithAssignedIPs.item(i));

            for (unsigned i2 = 0; i2 < strIPList.ordinality(); i2++)
            {
              sbl.set("s").append(m_ipaddress.ordinality() + m_ipaddressSupport.ordinality() + nCount++);
              assignedIP.set(strIPList.item(i2));
              addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName,sbl.str());
            }
        }
      }
      else if (numOfIpNeeded > 0)
      {
        if (!strcmp(buildSetName, "roxie"))
          numOfIpNeeded = m_roxieNodes;
        else if (!strcmp(buildSetName, "thor"))
          numOfIpNeeded = m_thorNodes + 1;

        CInstDetails* pInstDetail = getServerIPMap(sbNewName.str(), buildSetName, pNewEnvTree, numOfIpNeeded);

        if (pInstDetail)
        {
          if (!strcmp(buildSetName, "roxie") || !strcmp(buildSetName, "thor" ))
          {
            addRoxieThorClusterToEnv(pNewEnvTree, pInstDetail, buildSetName);

            if (!strcmp(buildSetName, "roxie") && m_roxieOnDemand)
              addRoxieThorClusterToEnv(pNewEnvTree, pInstDetail, buildSetName, true);
          }
          else
          {
            StringArray& ipArr = pInstDetail->getIpAssigned();

            ForEachItemIn(x, ipArr)
            {
              assignedIP.clear().append(ipArr.item(x));
              addInstanceToTree(pNewEnvTree, assignedIP, processName, buildSetName, "s1");
            }
          }
        }
      }
    }
  }
}

StringArray& CWizardInputs::getIpAddrMap(const char* buildSetName)
{
    if (buildSetName && *buildSetName)
    {
        for (int i = 0; i < m_arrBuildSetsWithAssignedIPs.ordinality(); i++)
        {
            if (stricmp(buildSetName, m_arrBuildSetsWithAssignedIPs.item(i)) == 0)
            {
                m_sipaddress.clear();
                formIPList(m_arrAssignedIPs.item(i), m_sipaddress);
                return m_sipaddress;
            }
        }
    }
    if (m_supportNodes == 0)
        return m_ipaddress;
    else
    {
        if (!strcmp(buildSetName, "roxie") || !strcmp(buildSetName, "thor" ))
          return m_ipaddress;
    }

    return m_ipaddressSupport;
}
