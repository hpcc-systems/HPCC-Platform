/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jliball.hpp"
#include "environment.hpp"
#include "XMLTags.h"
#include "configenvhelper.hpp"
#include "deployutils.hpp"
#include "build-config.h"

bool CConfigEnvHelper::handleRoxieOperation(const char* cmd, const char* xmlStr)
{
  bool retVal = false;
  if (!strcmp(cmd, "AddRoxieFarm"))
    retVal = this->addRoxieServers(xmlStr);
  else if (!strcmp(cmd, "DeleteRoxieFarm"))
    retVal = this->deleteRoxieServers(xmlStr);
  else if (!strcmp(cmd, "RoxieSlaveConfig"))
    retVal = this->handleRoxieSlaveConfig(xmlStr);
  else if (!strcmp(cmd, "ReplaceRoxieServer"))
    retVal = this->handleReplaceRoxieServer(xmlStr);

  return retVal;
}

bool CConfigEnvHelper::handleThorTopologyOp(const char* cmd, const char* xmlArg, StringBuffer& sMsg)
{
    bool retVal = false;
    StringBuffer xpath;

    Owned<IPropertyTree> pParams = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<ThorData/>");
    const char* thorName = pParams->queryProp(XML_ATTR_NAME);
    const char* newType = pParams->queryProp(XML_ATTR_TYPE);
    const char* validate = pParams->queryProp("@validateComputers");
    const char* skip = pParams->queryProp("@skipExisting");
    const char* slavesPerNode = pParams->queryProp("@slavesPerNode");
    bool checkComps = validate && !strcmp(validate, "true");
    bool skipExisting = skip && !strcmp(skip, "true");

    IPropertyTree* pThor = getSoftwareNode(XML_TAG_THORCLUSTER, thorName);

    StringBuffer usageList;

    if (!strcmp(cmd, "Add"))
    {
        Owned<IPropertyTreeIterator> iterComputers = pParams->getElements("Computer");
        IPropertyTreePtrArray computers;
        ForEach (*iterComputers)
        {
            IPropertyTree* pComp = &iterComputers->query();
            const char* pszCompName = pComp->queryProp(XML_ATTR_NAME);
            xpath.clear().appendf(XML_TAG_HARDWARE"/"XML_TAG_COMPUTER"/["XML_ATTR_NAME"='%s']", pszCompName);
            IPropertyTree* pComputer = m_pRoot->queryPropTree(xpath.str());

            if (pComputer)
                computers.push_back(pComputer);
        }

        if (!strcmp(newType, "Master") && computers.size() != 1)
          throw MakeStringException(-1, "Thor cannot have more than one master. Please choose one computer only!");

        int numNodes = 1;
        if (slavesPerNode && *slavesPerNode)
            numNodes = atoi(slavesPerNode);

        if (numNodes < 1)
            numNodes = 1;
        pThor->setPropInt("@slavesPerNode", numNodes);

        if (!strcmp(newType, "Master"))
            retVal = this->AddNewNodes(pThor, XML_TAG_THORMASTERPROCESS, 0, computers, checkComps, skipExisting, usageList);
        else if (!strcmp(newType, "Slave"))
            retVal = this->AddNewNodes(pThor, XML_TAG_THORSLAVEPROCESS, 0, computers, checkComps, skipExisting, usageList);
        else if (!strcmp(newType, "Spare"))
            retVal = this->AddNewNodes(pThor, XML_TAG_THORSPAREPROCESS, 0, computers, checkComps, skipExisting, usageList);

        if (usageList.length() > 0)
        {
            sMsg.append("The following computers are already being used.\nDo you want to add/replace them anyway?");
            sMsg.append(usageList);
        }
    }
    else if (!strcmp(cmd, "Delete"))
    {
        Owned<IPropertyTreeIterator> iterComputers = pParams->getElements("Node");
        ForEach (*iterComputers)
        {
            IPropertyTree* pComp = &iterComputers->query();
            const char* process = pComp->queryProp(XML_ATTR_PROCESS_NAME);
            const char* type = pComp->queryProp(XML_ATTR_TYPE);

            // Delete process node
            IPropertyTree* pProcessNode = GetProcessNode(pThor, process);
            if (pProcessNode)
                pThor->removeTree(pProcessNode);

            //Remove all slaves from thor
            if (!strcmp(type, "Master"))
              pThor->removeProp(XML_TAG_THORSLAVEPROCESS);
        }

        RenameThorInstances(pThor);
        UpdateThorAttributes(pThor);

        retVal = true;
    }

    return retVal;
}

IPropertyTree* CConfigEnvHelper::getSoftwareNode(const char* compType, const char* compName)
{
  StringBuffer xpath;
  xpath.appendf("Software/%s[@name='%s']", compType, compName);

  return m_pRoot->queryPropTree(xpath.str());
}

bool CConfigEnvHelper::addRoxieServers(const char* xmlArg)
{
  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<RoxieData/>");
  const char* pszFarm = pSrcTree->queryProp("@parentName");
  const char* pszRoxieCluster = pSrcTree->queryProp("@roxieName");
  unsigned int nComputers = 0;//computers.size();
  IPropertyTree* pParent = m_pRoot->queryPropTree("Software");
  IPropertyTree* pFarm;
  bool bNewFarm;
  StringBuffer sFarmName;
  StringBuffer xpath;

  if (strlen(pszFarm))
  {
    xpath.clear().appendf("RoxieCluster[@name='%s']/"XML_TAG_ROXIE_FARM, pszRoxieCluster);
    pFarm = getSoftwareNode(xpath.str(), pszFarm);
    sFarmName = pFarm->queryProp(XML_ATTR_NAME);
    bNewFarm = false;

    if (!pFarm->hasProp("@port"))
      pFarm->addPropInt("@port", 9876);

    if (!pFarm->hasProp("@listenQueue"))
      pFarm->addPropInt("@listenQueue", 200);

    if (!pFarm->hasProp("@numThreads"))
      pFarm->getPropInt("@numThreads", 30);

    if (!pFarm->hasProp("@requestArrayThreads"))
      pFarm->addPropInt("@requestArrayThreads", 5);

    if (!pFarm->hasProp("@aclName"))
      pFarm->addProp("@aclName", "");

    StringBuffer dataDir = pFarm->queryProp(XML_ATTR_DATADIRECTORY);
    if (dataDir.length()==0)
      dataDir.append(RUNTIME_DIR"/roxiedata");

    if (!pFarm->queryPropTree(XML_TAG_ROXIE_SERVER"[1]")) //no servers in farm
    {
      //if (nComputers > 0)
      //g_pDocument->makePlatformSpecificAbsolutePath(computers[0]->queryProp(XML_ATTR_NAME), dataDir);
      Owned<IPropertyTreeIterator> iter = pSrcTree->getElements(XML_TAG_COMPONENT);
      
      ForEach (*iter)
      {
        IPropertyTree* pFolder = &iter->query();
        makePlatformSpecificAbsolutePath(pFolder->queryProp(XML_ATTR_NAME), dataDir);
        break;
      }

      pFarm->setProp(XML_ATTR_DATADIRECTORY, dataDir.str());
    }
  }
  else
  {
    xpath.clear().appendf("RoxieCluster[@name='%s']/"XML_TAG_ROXIE_FARM, pszRoxieCluster);
    createUniqueName("farm", xpath.str(), sFarmName);
    bNewFarm = true;

    StringBuffer sDataDir;
    sDataDir.append(RUNTIME_DIR"/roxiedata");

    //get datadir from existing farm if any
    xpath.clear().appendf("Software/RoxieCluster[@name='%s']/"XML_TAG_ROXIE_FARM"[1]", pszRoxieCluster);
    IPropertyTree* pFarm1 =  m_pRoot->queryPropTree(xpath.str());
    if (pFarm1)
      sDataDir.clear().append(pFarm1->queryProp(XML_ATTR_DATADIRECTORY));

    //if (nComputers > 0)
    //g_pDocument->makePlatformSpecificAbsolutePath(computers[0]->queryProp(XML_ATTR_NAME), sDataDir);
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements(XML_TAG_COMPONENT);
      
    ForEach (*iter)
    {
      IPropertyTree* pFolder = &iter->query();
      makePlatformSpecificAbsolutePath(pFolder->queryProp(XML_ATTR_NAME), sDataDir);
      break;
    }

    xpath.clear().appendf("RoxieCluster[@name='%s']/"XML_TAG_ROXIE_FARM, pszRoxieCluster);
    pFarm = pParent->addPropTree(xpath.str(), createPTree());
    pFarm->addProp    (XML_ATTR_NAME,       sFarmName.str());
    pFarm->addPropInt("@port", pSrcTree->getPropInt("@port", 9876));
    pFarm->addProp    (XML_ATTR_DATADIRECTORY,  sDataDir.str());
    pFarm->addPropInt("@listenQueue", 200);
    pFarm->addPropInt("@numThreads",    30);
    pFarm->addPropInt("@requestArrayThreads", 5);
    pFarm->addProp("@aclName", "");
  }

  Owned<IPropertyTreeIterator> iter = pSrcTree->getElements(XML_TAG_COMPONENT);
  StringBuffer sNotAdded;
  xpath.clear().appendf("RoxieCluster[@name='%s']/"XML_TAG_ROXIE_FARM"[@name='%s']/"XML_TAG_ROXIE_SERVER, pszRoxieCluster, sFarmName.str());
  ForEach (*iter)
  {
    IPropertyTree* pFolder = &iter->query();

    const char* pszName = pFolder->queryProp(XML_ATTR_NAME);

    // Check if we can add this computer
    if (checkComputerUse(pszName, pFarm)) 
    {
      StringBuffer sServerName( sFarmName), sbUniqueName;
      sServerName.append("_s");
      createUniqueName(sServerName.str(), xpath.str(), sbUniqueName);

      // Add process node
      IPropertyTree* pServer = createPTree(XML_TAG_ROXIE_SERVER);
      pServer->setProp(XML_ATTR_NAME, sbUniqueName.str());
      pServer->addProp(XML_ATTR_COMPUTER, pszName);
      addNode(pServer, pFarm);

      IPropertyTree* pLegacyServer = addLegacyServer(sbUniqueName, pServer, pFarm, pszRoxieCluster);
    }
    else
    {
      sNotAdded.append('\n');
      sNotAdded.append(pszName);
      sNotAdded.append(" ( ");
      sNotAdded.append( pFolder->queryProp(XML_ATTR_NETADDRESS) );
      sNotAdded.append(" )");
    }
  }

  xpath.clear().appendf("Software/RoxieCluster[@name='%s']", pszRoxieCluster);
  IPropertyTree* pRoxieCluster = m_pRoot->queryPropTree(xpath.str());   
  renameInstances(pRoxieCluster);

  if (sNotAdded.length())
  {
    StringBuffer sMsg("The following servers were already allocated to the farm and could not be added:\n");
    sMsg.append(sNotAdded.str());
  }

  return true;
}

bool CConfigEnvHelper::handleReplaceRoxieServer(const char* xmlArg)
{
  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<RoxieData/>");
  const char* pszRoxieCluster = pSrcTree->queryProp("@roxieName");
  IPropertyTree* pParent = m_pRoot->queryPropTree("Software");
  StringBuffer xpath;

  if (pszRoxieCluster && *pszRoxieCluster)
  {
    xpath.clear().appendf(XML_TAG_ROXIECLUSTER"[@name='%s']/"XML_TAG_ROXIE_FARM, pszRoxieCluster);
    IPropertyTree* pFarm = pParent->queryPropTree(xpath.str());
    if (!pFarm)
      throw MakeStringException(-1, "Could not find a RoxieCluster with name '%s'", pszRoxieCluster);

    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Nodes/Node");
      
    ForEach (*iter)
    {
      IPropertyTree* pNode = &iter->query();
      const char* pszFarm = pNode->queryProp("@farm");
      const char* pszName = pNode->queryProp(XML_ATTR_NAME);
      const char* pszNewComputer = pNode->queryProp("@newComputer");

      xpath.clear().appendf(XML_TAG_ROXIE_SERVER"[@computer='%s']", pszNewComputer);

      if (pFarm->queryPropTree(xpath.str()))
        return false;

      xpath.clear().appendf(XML_TAG_ROXIE_SERVER"[@name='%s']", pszName);
      
      IPropertyTree* pServer = pFarm->queryPropTree(xpath.str());
      if (pServer && pszNewComputer && *pszNewComputer)
      {
        pServer->setProp(XML_ATTR_COMPUTER, pszNewComputer);
        xpath.clear().appendf(XML_TAG_ROXIECLUSTER"[@name='%s']/"XML_TAG_ROXIE_SERVER"[@name='%s']", pszRoxieCluster, pszName);
        IPropertyTree* pOldVerRoxieServer = pParent->queryPropTree(xpath.str());
        if (pOldVerRoxieServer)
        {
          pOldVerRoxieServer->setProp(XML_ATTR_COMPUTER, pszNewComputer);
          xpath.clear().appendf("Hardware/"XML_TAG_COMPUTER"["XML_ATTR_NAME"='%s']", pszNewComputer);
          IPropertyTree* pComputer = m_pRoot->queryPropTree(xpath.str());
          if (pComputer)
            pOldVerRoxieServer->setProp(XML_ATTR_NETADDRESS, pComputer->queryProp(XML_ATTR_NETADDRESS));
        }
      }
    }
  }

  return true;
}

//---------------------------------------------------------------------------
//  CheckComputerUse - will only prompt once for each element type
//---------------------------------------------------------------------------
bool CConfigEnvHelper::checkComputerUse(/*IPropertyTree* pComputerNode*/ const char* szComputer, IPropertyTree* pParentNode) const
{
  StringBuffer xpath;
  xpath.append(XML_TAG_ROXIE_SERVER "[@computer='").append( szComputer ).append("']");

  Owned<IPropertyTreeIterator> iter = pParentNode->getElements(xpath.str());
  return !(iter->first() && iter->isValid());
}


bool CConfigEnvHelper::makePlatformSpecificAbsolutePath(const char* computer, StringBuffer& path)
{
  bool rc = false;
  if (computer && *computer && path.length())
  {
    IPropertyTree* pComputer = lookupComputerByName(computer);
    const char* computerType = pComputer ? pComputer->queryProp(XML_ATTR_COMPUTERTYPE) : NULL;
    if (computerType && *computerType)
    {
      StringBuffer xpath;
      xpath.appendf("Hardware/ComputerType[@name='%s']", computerType);

      Owned<IPropertyTreeIterator> iter = m_pRoot->getElements(xpath.str());
      if (iter->first() && iter->isValid())
      {
        const char* os = iter->query().queryProp("@opSys");
        if (os && *os)
        {
          const bool bLinux = 0 != stricmp(os, "W2K");
          if (bLinux)
          {
            path.replace('\\', '/');
            path.replace(':', '$');
            if (*path.str() != '/')
              path.insert(0, '/');
          }
          else
          {
            path.replace('/', '\\');
            path.replace('$', ':');
            if (*path.str() == '\\')
              path.remove(0, 1);
          }
          rc = true;
        }
      }
    }
  }
  return rc;
}

IPropertyTree* CConfigEnvHelper::addLegacyServer(const char* name, IPropertyTree* pServer, 
                                                 IPropertyTree* pFarm, const char* roxieClusterName)
{
  IPropertyTree* pLegacyServer;
  StringBuffer xpath;
  xpath.clear().appendf("Software/RoxieCluster[@name='%s']", roxieClusterName);
  IPropertyTree* pParentNode = m_pRoot->queryPropTree(xpath.str());
  if (pParentNode)
  {
    const char* szComputer = pServer->queryProp(XML_ATTR_COMPUTER);
    xpath.clear().appendf("Hardware/Computer/[@name='%s']", szComputer);
    IPropertyTree* pComputer= m_pRoot->queryPropTree(xpath.str());
    const char* netAddress = pComputer->queryProp(XML_ATTR_NETADDRESS);
    //derive the new server from pFarm since it has most of the attributes
    pLegacyServer = addNode(XML_TAG_ROXIE_SERVER, pParentNode);
    pLegacyServer->setProp( XML_ATTR_NAME, name);
    pLegacyServer->setProp( XML_ATTR_COMPUTER, szComputer );
    pLegacyServer->setProp( XML_ATTR_NETADDRESS, netAddress);
    Owned<IAttributeIterator> iAttr = pFarm->getAttributes();
    ForEach(*iAttr)
    {
      const char* attrName = iAttr->queryName();
      if (0 != strcmp(attrName, XML_ATTR_COMPUTER)  && //skip
        0 != strcmp(attrName, XML_ATTR_NETADDRESS) &&
        0 != strcmp(attrName, XML_ATTR_NAME))
      {
        pLegacyServer->addProp(attrName, iAttr->queryValue());
      }
    }
  }
  else
    pLegacyServer = NULL;

  return pLegacyServer;
}


//---------------------------------------------------------------------------
//  setComputerState
//---------------------------------------------------------------------------
void CConfigEnvHelper::setComputerState(IPropertyTree* pNode, COMPUTER_STATE state)
{
  setAttribute(pNode, XML_ATTR_STATE, g_szComputerState[state]);
}

//---------------------------------------------------------------------------
//  setAttribute
//---------------------------------------------------------------------------
void CConfigEnvHelper::setAttribute(IPropertyTree* pNode, const char* szName, const char* szValue)
{
  // Check attribute already has specified value
  const char* szValueOld = pNode->queryProp(szName);
  if (!szValueOld || strcmp(szValueOld, szValue))
  {
    //UpdateComputerMap(pNode, false);
    // ptree does not like missing intermediates...
    const char *finger = szName;
    StringBuffer subpath;
    while (strchr(finger, '/'))
    {
      while (*finger!='/')
        subpath.append(*finger++);
      if (!pNode->hasProp(subpath.str()))
        pNode->addProp(subpath.str(), "");
      subpath.append(*finger++);
    }

    if (!strcmp(szName, XML_ATTR_BUILD) && !strcmp(pNode->queryName(), XML_TAG_ESPSERVICE))
    {
      //remove previous Properties, if any, that this component inherited from its
      //previous build
      IPropertyTree* pProperties = pNode->queryPropTree("Properties");
      IPropertyTree* pNewProperties;

      //if the new build has any properties then let the node inherit them
      const char* buildSet = pNode->queryProp(XML_ATTR_BUILDSET);
      if (buildSet)
      {
        StringBuffer sPath;
        sPath.append("Programs/Build[@name='").append(szValue).append("']/BuildSet[@name='")
          .append(buildSet).append("']/Properties");

        pNewProperties = m_pRoot->queryPropTree(sPath.str());
      }
      else
        pNewProperties = NULL;

      //if we just changed build for an ESP service then enumerate all bindings for all
      //ESP server processes and if any binding uses this service then replace its 
      //Authenticate and AuthenticateFeature nodes with those from the properties of 
      //this service from the new build.  However, we only remove the nodes that are 
      //not in the new build preserving the others (so their attributes are preserved - 
      //in case they have been changed by the user).  We also add new nodes that did 
      //not exist before.  In essence, a merge is needed.
      //
      if (pProperties || pNewProperties)
      {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess/EspBinding[@service='%s']", pNode->queryProp(XML_ATTR_NAME));

        Owned<IPropertyTreeIterator> iBinding = m_pRoot->getElements(xpath.str());
        ForEach(*iBinding)
        {
          IPropertyTree* pBinding = &iBinding->query();

          //remove existing Authenticate and AuthenticateFeature nodes that are not in the new buildset's properties
          //
          mergeServiceAuthenticationWithBinding(pBinding, pProperties, pNewProperties, "Authenticate");
          mergeServiceAuthenticationWithBinding(pBinding, pProperties, pNewProperties, "AuthenticateFeature");
          mergeServiceAuthenticationWithBinding(pBinding, pProperties, pNewProperties, "AuthenticateSetting");
        }
        pNode->removeTree(pProperties);
      }
      if (pNewProperties)
        pNode->addPropTree("Properties", createPTreeFromIPT(pNewProperties));
    }
    pNode->setProp(szName, szValue);
  }
}

void CConfigEnvHelper::mergeServiceAuthenticationWithBinding(IPropertyTree* pBinding, 
                                                             IPropertyTree* pProperties, 
                                                             IPropertyTree* pNewProperties, 
                                                             const char* NodeName)
{
  StringBuffer xpath;

  //remove existing Authenticate and AuthenticateFeature nodes that are not in the new buildset's properties
  //
  Owned<IPropertyTreeIterator> iDest = pBinding->getElements(NodeName);
  for (iDest->first(); iDest->isValid(); )
  {
    IPropertyTree* pDest = &iDest->query();
    iDest->next();

    const char* path = pDest->queryProp("@path");
    xpath.clear().appendf("%s[@path='%s']", NodeName, path);

    IPropertyTree* pNewPropChild = pNewProperties->queryPropTree(xpath.str());
    if (pNewPropChild)
    {
      IPropertyTree* pPropChild = pProperties->queryPropTree(xpath.str());

      if (pPropChild)
      {
        //same path so merge individual attributes, retaining any that may have been changed by user
        //but replacing ones that are different in newer build but not changed by user
        Owned<IAttributeIterator> iAttr = pDest->getAttributes();
        ForEach(*iAttr)
        {
          const char* attrName = iAttr->queryName();
          if (0 != strcmp(attrName, "@path"))
          {
            const char* attrDest = iAttr->queryValue();
            const char* attrProp = pPropChild->queryProp(attrName);
            const char* attrNewProp = pNewPropChild->queryProp(attrName);
            if (attrProp && attrNewProp && !strcmp(attrDest, attrProp))
              pDest->setProp(attrName, attrNewProp);
          }
        }
      }
    }
    else
      pBinding->removeTree(pDest);
  }

  //add nodes from buildset properties that are missing in binding
  //
  bool bAuthenticateFeature = !strcmp(NodeName, "AuthenticateFeature");
  Owned<IPropertyTreeIterator> iSrc = pNewProperties->getElements(NodeName);
  ForEach(*iSrc)
  {
    IPropertyTree* pNode = &iSrc->query();
    const char* path = pNode->queryProp("@path");

    xpath.clear().appendf("%s[@path='%s']", NodeName, path);

    if (!pBinding->queryPropTree(xpath.str()))
    {
      pNode = pBinding->addPropTree(NodeName, createPTreeFromIPT(pNode));
      if (bAuthenticateFeature)
        pNode->addProp("@authenticate", "Yes");
    }
  }
}

//---------------------------------------------------------------------------
//  lookupComputerByName
//---------------------------------------------------------------------------
IPropertyTree* CConfigEnvHelper::lookupComputerByName(const char* szName) const
{
  if (!szName || !*szName) return NULL;

  Owned<IPropertyTreeIterator> iter = m_pRoot->getElements(XML_TAG_HARDWARE"/"XML_TAG_COMPUTER);
  for (iter->first(); iter->isValid(); iter->next())
  {
    const char* szValue = iter->query().queryProp(XML_ATTR_NAME);
    if (szValue && strcmp(szValue, szName) == 0)
      return &iter->query();
  }
  return NULL;
}


void CConfigEnvHelper::createUniqueName(const char* szPrefix, const char* parent, StringBuffer& sbName)
{
  sbName.clear().append(szPrefix).append("1");

  if (getSoftwareNode(parent, sbName.str()))
  {
    int iIdx = 2;
    do 
    {
      sbName.clear().append(szPrefix).append(iIdx++);
    }
    while (getSoftwareNode(parent, sbName.str()));
  }
}


//---------------------------------------------------------------------------
//  addNode
//---------------------------------------------------------------------------
IPropertyTree* CConfigEnvHelper::addNode(const char* szTag, IPropertyTree* pParentNode, IPropertyTree* pInsertAfterNode)
{
  IPropertyTree* pNode = createPTree(szTag);
  if (pNode)
  {
    addNode(pNode, pParentNode, pInsertAfterNode);
  }
  return pNode;
}

//---------------------------------------------------------------------------
//  addNode
//---------------------------------------------------------------------------
IPropertyTree* CConfigEnvHelper::addNode(IPropertyTree*& pNode, IPropertyTree* pParentNode, IPropertyTree* pInsertAfterNode)
{
  StringBuffer sTag(pNode->queryName()); // need to pass in a copy of the name

  // Check is node is to be added at specific location relative to nodes with same name
  if (pInsertAfterNode)
  {
    int idx = 1; // this will insert into first position
    if (strcmp(pInsertAfterNode->queryName(), pNode->queryName()) == 0)
    {
      idx = pParentNode->queryChildIndex(pInsertAfterNode) + 2;
    }
    // Only append qualifier is not inserting at end position
    if (pParentNode->queryPropTree(StringBuffer(sTag).appendf("[%d]", idx).str()))
      sTag.appendf("[%d]", idx);
  }

  pNode = pParentNode->addPropTree(sTag.str(), pNode);
  return pNode;
}

//---------------------------------------------------------------------------
//  renameInstances
//---------------------------------------------------------------------------
void CConfigEnvHelper::renameInstances(IPropertyTree* pRoxieCluster)
{
  // Iterate through farms

  int nFarm = 0;
  StringBuffer xpath;
  Owned<IPropertyTreeIterator> iFarm = pRoxieCluster->getElements(XML_TAG_ROXIE_FARM);
  ForEach(*iFarm)
  {
    IPropertyTree* pFarm = &iFarm->query();
    int nServer = 0;

    StringBuffer sFarmName("farm");
    sFarmName.append(++nFarm);

    setAttribute(pFarm, XML_ATTR_NAME, sFarmName.str());
    Owned<IPropertyTreeIterator> iServer = pFarm->getElements(XML_TAG_ROXIE_SERVER);
    ForEach(*iServer)
    {
      IPropertyTree* pServer = &iServer->query();
      StringBuffer sServerName( sFarmName );
      sServerName.append("_s");
      sServerName.append(++nServer);

      const char* prevName = pServer->queryProp(XML_ATTR_NAME);
      if (prevName && *prevName)
      {
        IPropertyTree* pLegacyServer = findLegacyServer(pRoxieCluster, prevName);
        if (pLegacyServer)
          setAttribute(pLegacyServer, "@_name", sServerName.str());
      }
      setAttribute(pServer, XML_ATTR_NAME, sServerName.str());
    }
  }

  Owned<IPropertyTreeIterator> iServer =  pRoxieCluster->getElements(XML_TAG_ROXIE_SERVER);
  ForEach(*iServer)
  {
    IPropertyTree* pServer = &iServer->query();
    const char* newName = pServer->queryProp("@_name");
    if (newName)
    {
      pServer->setProp(XML_ATTR_NAME, newName);
      pServer->removeProp("@_name");
    }
  }
}


IPropertyTree* CConfigEnvHelper::findLegacyServer(IPropertyTree* pRoxieCluster, const char* pszServer)
{
  StringBuffer xpath;
  xpath.appendf(XML_TAG_ROXIE_SERVER"[@name='%s']", pszServer);
  return pRoxieCluster->queryPropTree( xpath.str() );
}

bool CConfigEnvHelper::deleteRoxieServers(const char* xmlArg)
{
  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<RoxieData/>");
  const char* pszRoxieCluster = pSrcTree->queryProp("@roxieName");
  unsigned int nComputers = 0;//computers.size();
  StringBuffer xpath;
  xpath.clear().appendf("Software/RoxieCluster[@name='%s']", pszRoxieCluster);
  IPropertyTree* pRoxieCluster = m_pRoot->queryPropTree(xpath.str());
  StringBuffer sFarmName;

  Owned<IPropertyTreeIterator> iterFarm = pSrcTree->getElements(XML_TAG_ROXIE_FARM);
  ForEach (*iterFarm)
  {
    IPropertyTree* pFarm = &iterFarm->query();
    const char* pszFarm = pFarm->queryProp(XML_ATTR_NAME);
    deleteFarm(pRoxieCluster, pszFarm);
  }

  Owned<IPropertyTreeIterator> iterServer = pSrcTree->getElements(XML_TAG_ROXIE_SERVER);
  ForEach (*iterServer)
  {
    IPropertyTree* pServer = &iterServer->query();

    const char* pszName = pServer->queryProp(XML_ATTR_NAME);
    const char* pszFarm = pServer->queryProp("@parent");
    deleteServer(pRoxieCluster, pszFarm, pszName);
  }

    Owned<IPropertyTreeIterator> iterSlaves = pSrcTree->getElements(XML_TAG_ROXIE_ONLY_SLAVE);
    ForEach (*iterSlaves)
    {
        IPropertyTree* pChild;
        //if atleast one slave, delete all slaves
        while (pChild = pRoxieCluster->queryPropTree( "RoxieSlave[1]" ))
            pRoxieCluster->removeTree( pChild );

        while (pChild = pRoxieCluster->queryPropTree( XML_TAG_ROXIE_SLAVE "[1]" ))
            pRoxieCluster->removeTree( pChild );

        break;
    }

  renameInstances(pRoxieCluster);

  return true;
}


void CConfigEnvHelper::deleteFarm(IPropertyTree* pRoxieCluster, const char* pszFarm)
{
  StringBuffer xpath;
  xpath.clear().appendf(XML_TAG_ROXIE_FARM"[@name='%s']", pszFarm);
  IPropertyTree* pFarm = pRoxieCluster->queryPropTree(xpath.str());
  Owned<IPropertyTreeIterator> it = pFarm->getElements(XML_TAG_ROXIE_SERVER);

  ForEach(*it)
  {
    IPropertyTree* pServer = &it->query();
    const char* pszServer = pServer->queryProp(XML_ATTR_NAME);
    IPropertyTree* pLegacyServer = findLegacyServer(pRoxieCluster, pszServer);
    if (pLegacyServer)
      pRoxieCluster->removeTree(pLegacyServer);
  }

  pRoxieCluster->removeTree(pFarm);
}

void CConfigEnvHelper::deleteServer(IPropertyTree* pRoxieCluster, const char* pszFarm, const char* pszServer)
{
  StringBuffer xpath;

  IPropertyTree* pLegacyServer = findLegacyServer(pRoxieCluster, pszServer);
  if (pLegacyServer)
    pRoxieCluster->removeTree(pLegacyServer);

  xpath.clear().appendf(XML_TAG_ROXIE_FARM"[@name='%s']", pszFarm);
  IPropertyTree* pFarm = pRoxieCluster->queryPropTree(xpath.str());
  if (pFarm)
  {
    xpath.clear().appendf(XML_TAG_ROXIE_SERVER"[@name='%s']", pszServer);
    IPropertyTree* pServer = pFarm->queryPropTree(xpath.str());

    if (pServer)
      pFarm->removeTree(pServer);
  }
}



void CConfigEnvHelper::addComponent(const char* pszBuildSet, StringBuffer& sbNewName, IPropertyTree* pCompTree)
{
  try
  {
    // NOTE - we are assuming buildSet is unique in a build.
    StringBuffer xPath, value;
    xPath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]", pszBuildSet);
    Owned<IPropertyTreeIterator> buildSet = m_pRoot->getElements(xPath.str());
    buildSet->first();
    IPropertyTree* pBuildSet = &buildSet->query();
    const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
    const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
    const char* buildName = m_pRoot->queryPropTree("./Programs/Build[1]")->queryProp(XML_ATTR_NAME);
    if (!processName) //support non-generic components as well
      processName = buildSetName;

    {
      // Use lower case version of type for name prefix
      StringBuffer sName(buildSetName);
      sName.toLowerCase();
      sName.replaceString("process","");

      if(sbNewName.length())
        value.append(sbNewName.str()).append(getUniqueName(m_pRoot.get(), sName, processName, "Software"));
      else
        value.append(getUniqueName(m_pRoot.get(), sName, processName, "Software"));

      pCompTree->setProp(XML_ATTR_NAME,value);
      sbNewName.clear().append(sName);
      pCompTree->setProp(XML_ATTR_BUILD,   buildName);
      pCompTree->setProp(XML_ATTR_BUILDSET,pszBuildSet);

      Owned<IPropertyTree> pProperties = pBuildSet->getPropTree("Properties");
      if (pProperties)
        pCompTree->addPropTree("Properties", createPTreeFromIPT(pProperties));

      addNode(pCompTree, m_pRoot->queryPropTree("Software"));
    }
  }
  catch (IException* e)
  {
    throw e;
  }
}

bool CConfigEnvHelper::EnsureInRange(const char* psz, UINT low, UINT high, const char* caption)
{
    bool rc = false;
    StringBuffer msg;
    const UINT x = atoi( psz );
    if ( ((low < high) && (x < low || x > high)) || (low == high && x != low) )
    {
        msg.append(caption).append(" must be ");
        if (low == high)
            msg.append(low);
        else
        {
            msg.append("between ");
            msg.append(low).append(" and ");
            msg.append( high );
        }
    }
    else 
        if (high == 0 && x < low)
            msg.append(caption).append(" must be at least ").append(low);
        else
            rc = true;

    if (!rc)
    {
        msg.append('.');
        throw MakeStringException(-1, "%s", msg.str());
    }
    return rc;
}

bool CConfigEnvHelper::handleRoxieSlaveConfig(const char* xmlArg)
{
    try
    {
        Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<RoxieData/>");
        const char* type = pSrcTree->queryProp(XML_ATTR_TYPE);
        const char* pszRoxie = pSrcTree->queryProp("@roxieName");
        const char* val1 = pSrcTree->queryProp("@val1");
        const char* sOffset = pSrcTree->queryProp("@val2");
        StringBuffer dir1, dir2, dir3;
    getCommonDir(m_pRoot.get(), "data", "roxie", pszRoxie, dir1);
        getCommonDir(m_pRoot.get(), "data2", "roxie", pszRoxie, dir2);
        getCommonDir(m_pRoot.get(), "data3", "roxie", pszRoxie, dir3);

        StringBuffer xpath;
        xpath.clear().appendf("Software/RoxieCluster[@name='%s']", pszRoxie);
        IPropertyTree* pRoxie = m_pRoot->queryPropTree(xpath.str());

        if (!pRoxie)
            throw MakeStringException(-1, "Cannot find roxie with name %s", pszRoxie);

        Owned<IPropertyTreeIterator> iterComputers = pSrcTree->getElements("Computer");
        IPropertyTreePtrArray computers;
        ForEach (*iterComputers)
        {
            IPropertyTree* pComp = &iterComputers->query();
            const char* pszCompName = pComp->queryProp(XML_ATTR_NAME);
            xpath.clear().appendf(XML_TAG_HARDWARE"/"XML_TAG_COMPUTER"/["XML_ATTR_NAME"='%s']", pszCompName);
            IPropertyTree* pComputer = m_pRoot->queryPropTree(xpath.str());
            
            if (pComputer)
                computers.push_back(pComputer);
        }

        m_numChannels = atoi(val1);
        m_numDataCopies = 0;
        const char* confType;
        char chDrive;

        if (!strcmp(type, "Circular"))
        {
            if (!GenerateCyclicRedConfig(pRoxie, computers, val1, sOffset, dir1.str(), dir2.str(), dir3.str()))
                return false;

            confType = "cyclic redundancy";
            chDrive = 'c';

            pRoxie->setProp("@cyclicOffset", sOffset);
        }
        else 
        {
            if (!strcmp(type, "Overloaded"))
            {
                if (!GenerateOverloadedConfig(pRoxie, computers, val1, dir1.str(), dir2.str(), dir3.str()))
                    return false;

                confType = "overloaded";
                chDrive = 'c';
            }
            else 
            {
                if (!strcmp(type, "Full"))
                {
                    m_numDataCopies = atoi( val1 );
                    confType = "full redundancy";
                }
                else //no redundancy
                {
                    m_numDataCopies = 1;
                    confType = "simple";
                }

                if (!GenerateFullRedConfig(pRoxie, m_numDataCopies, computers, dir1.str()))
                    return false;
            }

            if (pRoxie->hasProp("@cyclicOffset"))
                pRoxie->removeProp("@cyclicOffset");
        }
        StringBuffer sDataDir;
        sDataDir.appendf("%s", dir1.str());

        //give legacy slaves unique names
        UINT i = 1;
        Owned<IPropertyTreeIterator> it = pRoxie->getElements(XML_TAG_ROXIE_SLAVE);
        ForEach( *it)
        {
            StringBuffer name;
            name.append('s').append(i);

            IPropertyTree* pLegacySlave = &it->query();
            pLegacySlave->setProp(XML_ATTR_NAME, name.str() );

            if (i++==1)
                makePlatformSpecificAbsolutePath( pLegacySlave->queryProp(XML_ATTR_COMPUTER), sDataDir);
        }

        pRoxie->setProp("@slaveConfig", confType);
        pRoxie->setPropInt("@clusterWidth", computers.size());
        pRoxie->setPropInt("@numChannels",   m_numChannels);
        pRoxie->setPropInt("@numDataCopies", m_numDataCopies);
        pRoxie->setProp("@baseDataDir", sDataDir.str());
        pRoxie->setProp("@localSlave", computers.size() > 1 ? "false" : "true");

        //update Roxie data directories for all farms and all legacy servers
        //change all farms
        Owned<IPropertyTreeIterator> iterFarms = pRoxie->getElements(XML_TAG_ROXIE_FARM);
        ForEach (*iterFarms)
        {
            IPropertyTree* pTmpComp = &iterFarms->query();
            if (strcmp(pTmpComp->queryProp(XML_ATTR_DATADIRECTORY), sDataDir.str()))
                pTmpComp->setProp(XML_ATTR_DATADIRECTORY, sDataDir.str());
        }

        //change all legacy servers
        Owned<IPropertyTreeIterator> iterServers = pRoxie->getElements(XML_TAG_ROXIE_SERVER);

        ForEach (*iterServers)
        {
            IPropertyTree* pTmpComp = &iterServers->query();
            if (strcmp(pTmpComp->queryProp(XML_ATTR_DATADIRECTORY), sDataDir.str()))
                pTmpComp->setProp(XML_ATTR_DATADIRECTORY, sDataDir.str());
        }
    }
    catch (IException *e)
    {
        StringBuffer msg;
        throw MakeStringException(-1, "%s", e->errorMessage(msg).str());
    }
    catch (...)
    {
        throw MakeStringException(-1, "Unknown exception in generating slave configuration!" );
    }

    return true;
}


void CConfigEnvHelper::addReplicateConfig(IPropertyTree* pSlaveNode, int channel, const char* dir, 
                                                                const char* netAddress, IPropertyTree* pRoxie)
{
    StringBuffer directory;
    directory.appendf("%s", dir);
    makePlatformSpecificAbsolutePath( pSlaveNode->queryProp(XML_ATTR_COMPUTER), directory);

    IPropertyTree* pInstance = pSlaveNode->addPropTree(XML_TAG_ROXIE_CHANNEL, createPTree());
    pInstance->setPropInt("@number", channel);
    pInstance->addProp(XML_ATTR_DATADIRECTORY, directory.str());
    
    //maintain a copy as an old style slave procss
    IPropertyTree* pSlaveProcess = pRoxie->addPropTree(XML_TAG_ROXIE_SLAVE, createPTree());
    pSlaveProcess->addProp(XML_ATTR_COMPUTER, pSlaveNode->queryProp(XML_ATTR_COMPUTER));
    pSlaveProcess->addPropInt("@channel", channel);
    pSlaveProcess->addProp(XML_ATTR_DATADIRECTORY, directory.str());
    pSlaveProcess->addProp(XML_ATTR_NETADDRESS, netAddress);
}

bool CConfigEnvHelper::GenerateCyclicRedConfig(IPropertyTree* pRoxie, IPropertyTreePtrArray& computers, 
                                                                                             const char* copies, const char* pszOffset,
                                                                                             const char* dir1, const char* dir2, const char* dir3)
{
    const int nComputers = computers.size();
    if (!nComputers)
        return false;

    if (!EnsureInRange(copies, min(2, nComputers), max(nComputers, 1), "Channel redundancy") ||
         !EnsureInRange(pszOffset, min(1, nComputers-1), nComputers-1, "Channel offset"))
    {
        return false;
    }

    const int offset = atoi( pszOffset );
    m_numDataCopies = atoi( copies );
    
    const int minOffset = min(1, nComputers-1);
    if( offset < minOffset )
         throw MakeStringException(-1, "Offset cannot be less than %d", minOffset);
    if ( offset > nComputers )
        throw MakeStringException(-1, "Offset cannot be greater than %d", nComputers);

    RemoveSlaves(pRoxie, true);
    RemoveSlaves(pRoxie, false);

    for (int i=0; i<nComputers; i++)
    {
        IPropertyTree* pComputer = computers[i];
        const char* szComputer = pComputer->queryProp(XML_ATTR_NAME);
        const char* netAddress = pComputer->queryProp(XML_ATTR_NETADDRESS);

        StringBuffer name;
        name.appendf("s%d", i+1);

        IPropertyTree* pSlave = pRoxie->addPropTree(XML_TAG_ROXIE_ONLY_SLAVE, createPTree());
        pSlave->addProp(XML_ATTR_NAME, name.str());
        pSlave->addProp(XML_ATTR_COMPUTER, szComputer);

        const int baseChannel = i; //channel for first copy of slave (0 based)
        int channel;
        for (int c=0; c<m_numDataCopies; c++)
        {
            const char drive = 'c' + c;
            channel = 1 + ((baseChannel + c*(nComputers-offset)) % nComputers);

            addReplicateConfig(pSlave, channel, c==0? dir1: (c==1?dir2:dir3), netAddress, pRoxie);
        }
    }
    m_numChannels = nComputers;
    return true;
}

bool CConfigEnvHelper::GenerateOverloadedConfig(IPropertyTree* pRoxie, IPropertyTreePtrArray& computers, const char* copies,
                                                                                                const char* dir1, const char* dir2, const char* dir3)
{
    const UINT nComputers = computers.size();
    if (!nComputers)
        return false;

    if (!EnsureInRange(copies, 1, 0, "Channels per host"))
        return false;

    m_numDataCopies = atoi( copies );
        
    RemoveSlaves(pRoxie, true);
    RemoveSlaves(pRoxie, false);

    int channel = 1;
    for (UINT i=0; i<nComputers; i++)
    {
        IPropertyTree* pComputer = computers[i];
        const char* szComputer = pComputer->queryProp(XML_ATTR_NAME);
        const char* netAddress = pComputer->queryProp(XML_ATTR_NETADDRESS);

        StringBuffer name;
        name.appendf("s%d", i+1);

        IPropertyTree* pSlave = pRoxie->addPropTree(XML_TAG_ROXIE_ONLY_SLAVE, createPTree());
        pSlave->addProp(XML_ATTR_NAME, name.str());
        pSlave->addProp(XML_ATTR_COMPUTER, szComputer);

        for (int c=0; c<m_numDataCopies; c++)
        {
            const char drive = 'c' + c;
            addReplicateConfig(pSlave, channel + c*nComputers, c==0?dir1:(c==1?dir2:dir3), netAddress, pRoxie);
        }
        channel++;
    }
    m_numChannels = m_numDataCopies*nComputers;
    return true;
}

bool CConfigEnvHelper::GenerateFullRedConfig(IPropertyTree* pRoxie, int copies, IPropertyTreePtrArray& computers, const char* dir1)
{
    int nComputers = computers.size();
    if (!nComputers)
        return false;

    StringBuffer sbCopies;
    sbCopies.appendf("%d", copies);
    //if full redundancy is selected then check channel redundancy
    if (copies != 1 && !EnsureInRange(sbCopies.str(), min(2, nComputers), (nComputers+1)/2, "Channel redundancy"))
        return false;

    const int maxChannel = nComputers / copies;

    RemoveSlaves(pRoxie, true);
    RemoveSlaves(pRoxie, false);

    int channel = 0;
    nComputers = maxChannel * copies;
    for (int i=0; i<nComputers; i++)
    {
        IPropertyTree* pComputer = computers[i];
        const char* szComputer = pComputer->queryProp(XML_ATTR_NAME);
        const char* netAddress = pComputer->queryProp(XML_ATTR_NETADDRESS);

        StringBuffer name;
        name.appendf("s%d", i+1);

        IPropertyTree* pSlave = pRoxie->addPropTree(XML_TAG_ROXIE_ONLY_SLAVE, createPTree());
        pSlave->addProp(XML_ATTR_NAME, name.str());
        pSlave->addProp(XML_ATTR_COMPUTER, szComputer);

        addReplicateConfig(pSlave, 1 + (channel++ % maxChannel), dir1, netAddress, pRoxie);
    }
    m_numChannels = maxChannel;
    return true;
}

void CConfigEnvHelper::RemoveSlaves(IPropertyTree* pRoxie, bool bLegacySlaves/*=false*/)
{
    IPropertyTree* pChild;
    while (pChild = pRoxie->queryPropTree( bLegacySlaves ? XML_TAG_ROXIE_SLAVE "[1]" : "RoxieSlave[1]"))
        pRoxie->removeTree( pChild );
}

void CConfigEnvHelper::RenameThorInstances(IPropertyTree* pThor)
{
    int nSlave = 1;
    int nSpare = 1;
    IPropertyTree* pMaster = pThor->queryPropTree(XML_TAG_THORMASTERPROCESS);
    if (pMaster)
      pMaster->setProp(XML_ATTR_NAME, "m1");

    StringBuffer sName;

    Owned<IPropertyTreeIterator> iter = pThor->getElements(XML_TAG_THORSLAVEPROCESS);
    for (iter->first(); iter->isValid(); iter->next())
    {
      sName.clear().appendf("s%d", nSlave++);
      setAttribute(&iter->query(), XML_ATTR_NAME, sName);
    }

    iter.setown(pThor->getElements(XML_TAG_THORSPAREPROCESS));
    for (iter->first(); iter->isValid(); iter->next())
    {
      sName.clear().appendf("spare%d", nSpare++);
      setAttribute(&iter->query(), XML_ATTR_NAME, sName);
    }

    //With thor dynamic range changes, we do not need thor topology section
    IPropertyTree* pTopology = pThor->queryPropTree(XML_TAG_TOPOLOGY);

    if (pTopology)
      pThor->removeTree(pTopology);
}

//----------------------------------------------------------------------------
//  UpdateAttributes
//----------------------------------------------------------------------------
void CConfigEnvHelper::UpdateThorAttributes(IPropertyTree* pParentNode)
{
    const char* masterIp = NULL;
    bool localThor = true, multiSlaves = false;
    int nSlaves = 0;

    IPropertyTree* pNode = pParentNode->queryPropTree(XML_TAG_THORMASTERPROCESS);
    if (pNode)
    {
        const char* szName = pNode->queryProp(XML_ATTR_COMPUTER);
        setAttribute(pParentNode, XML_ATTR_COMPUTER, szName);
        IPropertyTree* pComputer = lookupComputerByName(szName);
        if (pComputer)
          masterIp = pComputer->queryProp(XML_ATTR_NETADDRESS);
    }
    else
    {
      localThor = false;
    }

    Owned<IPropertyTreeIterator> iter = pParentNode->getElements(XML_TAG_THORSLAVEPROCESS);

    for (iter->first(); iter->isValid(); iter->next())
    {
        nSlaves++;

        if (!localThor && multiSlaves)
          continue;

        const char* computer = iter->query().queryProp(XML_ATTR_COMPUTER);
        if (computer && *computer)
        {
          if (localThor)
          {
            IPropertyTree* pNode = lookupComputerByName(computer);

            if (pNode && masterIp && *masterIp)
            {
              const char* ip = pNode->queryProp(XML_ATTR_NETADDRESS);

              if (ip && *ip && strcmp(ip, masterIp))
                localThor = false;
            }
          }

          if (!multiSlaves)
          {
            StringBuffer xpath(XML_TAG_THORSLAVEPROCESS);
            xpath.appendf("["XML_ATTR_COMPUTER"='%s']", computer);

            Owned<IPropertyTreeIterator> iterNodes = pParentNode->getElements(xpath.str());
            int count = 0;

            ForEach(*iterNodes)
            {
              count++;
              if (count > 1)
              {
                multiSlaves = true;
                break;
              }
            }
          }
        }
    }

    setAttribute(pParentNode, "@localThor", localThor ? "true" : "false");
}

//---------------------------------------------------------------------------
//  AddNewNodes
//---------------------------------------------------------------------------
bool CConfigEnvHelper::AddNewNodes(IPropertyTree* pThor, const char* szType, int nPort, IPropertyTreePtrArray& computers, bool validate, bool skipExisting, StringBuffer& usageList)
{
    // Get parent node
    IPropertyTree* pParentNode = pThor;

    if (validate)
    {
        for (int i = 0; i < (int) computers.size(); i++)
            CheckTopologyComputerUse(computers[i], pThor, usageList);
    }

    if (usageList.length() > 0)
        return false;

    // Iterate through computer list
    for (int i = 0; i < (int) computers.size(); i++)
    {
        // Check if we can add this computer
        if (skipExisting && !CheckTopologyComputerUse(computers[i], pThor, usageList))
            continue;

        StringBuffer sName;
        sName.appendf("temp%d", i + 1);

        // Add process node
        IPropertyTree* pProcessNode = createPTree(szType);
        pProcessNode->addProp(XML_ATTR_NAME, sName);
        pProcessNode->addProp(XML_ATTR_COMPUTER, computers[i]->queryProp(XML_ATTR_NAME));
        if (nPort != 0) pProcessNode->addPropInt(XML_ATTR_PORT, nPort);
            addNode(pProcessNode, pThor);
    }

    RenameThorInstances(pThor);
    UpdateThorAttributes(pThor);

    return true;
}

bool CConfigEnvHelper::CheckTopologyComputerUse(IPropertyTree* pComputerNode, IPropertyTree* pParentNode, StringBuffer& usageList) const
{
    const char* szNetAddress = pComputerNode->queryProp(XML_ATTR_NETADDRESS);
    bool retVal = true;

    StringArray sElementTypes;
    StringBuffer xpath;
    Owned<IPropertyTreeIterator> iter = pParentNode->getElements("*");
    for (iter->first(); iter->isValid(); iter->next())
    {
        const char* szTag = iter->query().queryName();
        if (sElementTypes.find(szTag) == NotFound)
        {
            IPropertyTree* pTree = &iter->query();
            const char* pszComputer = pTree->queryProp(XML_ATTR_COMPUTER);
            xpath.clear().appendf(XML_TAG_HARDWARE"/"XML_TAG_COMPUTER"["XML_ATTR_NAME"='%s']", pszComputer);
            IPropertyTree* pComputer = m_pRoot->queryPropTree(xpath.str());
            const char* szNetAddress1 = pComputer?pComputer->queryProp(XML_ATTR_NETADDRESS):NULL;
            if (szNetAddress1 && strcmp(szNetAddress1, szNetAddress)==0)
            {
                usageList.appendf("\n%s:%s - %s",
                    pComputerNode->queryProp(XML_ATTR_NAME),
                    pComputerNode->queryProp(XML_ATTR_NETADDRESS),
                    szTag);

                // Save the found type and suppress warnings for those types
                sElementTypes.append(szTag);
                retVal = false;
            }
        }
    }

    return retVal;
}

//---------------------------------------------------------------------------
//  GetProcessNode
//---------------------------------------------------------------------------
IPropertyTree* CConfigEnvHelper::GetProcessNode(IPropertyTree* pThor, const char* szProcess) const
{
    if (szProcess && *szProcess)
    {
        Owned<IPropertyTreeIterator> iter = pThor->getElements("*");
        ForEach(*iter)
        {
            const char* szName = iter->query().queryProp(XML_ATTR_NAME);
            if (szName && strcmp(szName, szProcess) == 0)
                return &iter->query();
        }
    }
    return NULL;
}
