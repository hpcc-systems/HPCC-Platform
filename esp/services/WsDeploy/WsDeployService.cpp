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

#pragma warning (disable : 4786)

#include "WsDeployService.hpp"
#include "WsDeployEngine.hpp"
#include "jwrapper.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "jencrypt.hpp"

#ifdef _WINDOWS
#include <winsock2.h>
#define strlwr _strlwr
#endif

#define STANDARD_CONFIG_BACKUPDIR CONFIG_DIR"/backup"
#define STANDARD_CONFIG_SOURCEDIR CONFIG_DIR
#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"
#define STANDARD_CONFIG_CONFIGXML_DIR "/componentfiles/configxml/"
#define STANDARD_CONFIG_STAGED_PATH "/etc/HPCCSystems/environment.xml"

#define DEFAULT_DIRECTORIES "<Directories name=\""DIR_NAME"\">\
      <Category dir=\""EXEC_PREFIX"/log/[NAME]/[INST]\" name=\"log\"/>\
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/[INST]\" name=\"run\"/>\
      <Category dir=\""CONFIG_PREFIX"/[NAME]/[INST]\" name=\"conf\"/>\
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/[INST]/temp\" name=\"temp\"/> \
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/hpcc-data/[COMPONENT]\" name=\"data\"/> \
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/hpcc-data2/[COMPONENT]\" name=\"data2\"/> \
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/hpcc-data3/[COMPONENT]\" name=\"data3\"/> \
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/hpcc-mirror/[COMPONENT]\" name=\"mirror\"/> \
      <Category dir=\""EXEC_PREFIX"/lib/[NAME]/queries/[INST]\" name=\"query\"/> \
      <Category dir=\""EXEC_PREFIX"/lock/[NAME]/[INST]\" name=\"lock\"/> \
      </Directories>"
#include <vector>

using namespace std;
typedef vector<IPropertyTree*> IPropertyTreePtrArray;

bool CCloudTaskThread::s_abort = false;

bool supportedInEEOnly()
{
  throw MakeStringException(-1, "This operation is supported in Enterprise and above editions only. Please contact HPCC Systems at http://www.hpccsystems.com/contactus");
}

void substituteParameters(const IPropertyTree* pEnv, const char *xpath, IPropertyTree* pNode, StringBuffer& result) 
{
  const char* xpathorig = xpath;
  while (*xpath)
  {
    if (*xpath=='$')
    {
      if (strncmp(xpath, "$build", 6)==0)
      {
        result.append('\"');
        result.append(pNode->queryProp(XML_ATTR_BUILD));
        result.append('\"');
        xpath+=6;
      }
      else if (strncmp(xpath, "$model", 6)==0)
      {
        result.append("Model");
        const char *datamodel = pNode->queryProp("@dataModel");
        if (datamodel)
        {
          result.append("[@name=\"");
          result.append(datamodel);
          result.append("\"]");
        }
        xpath+=6;
      }
      else if (strncmp(xpath, "$parentmodel", 12)==0)
      {
        // Find the model that I am in - HACK
        result.append( "Model");
        const char *datamodel = NULL;
        const IPropertyTree *root = pEnv;
        IPropertyTree *current = pNode;
        Owned<IPropertyTreeIterator> allModels = root->getElements("Data/Model");
        ForEach(*allModels)
        {
          IPropertyTree &model = allModels->query();
          Owned<IPropertyTreeIterator> children = model.getElements("*");
          ForEach(*children)
          {
            if (&children->query()==current)
            {
              datamodel = model.queryProp("@name");
              break;
            }
          }
          if (datamodel)
            break;
        }                   
        if (datamodel)
        {
          result.append("[@name=\"");
          result.append(datamodel);
          result.append("\"]");
        }
        xpath+=12;
      }
      else if (strncmp(xpath, "$./*", 4) == 0)
      {
        String xpath2(xpath+4);
        StringBuffer sb(xpath2);
        String xpath3(xpathorig);
        String* pstr = xpath3.substring(xpath3.lastIndexOf('[', xpath3.indexOf("$./*")) + 1, xpath3.indexOf("$./*"));
        int pos1 = xpath2.indexOf(']');
        int pos2 = xpath2.lastIndexOf('/');

        if (pos1 != -1 && pos2 != -1)
          sb.clear().append(xpath2.substring(0, pos2)->toCharArray());
        else if (pos1 != -1)
          sb.clear().append(xpath2.substring(0, pos1)->toCharArray());

        Owned<IPropertyTreeIterator> elems = pNode->getElements(sb.str());

        if (pos2 != -1)
          sb.clear().append(xpath2.substring(pos2+ 1, pos1)->toCharArray());

        ForEach(*elems)
        {
          IPropertyTree* elem = &elems->query();
          result.append('\"');
          result.append(elem->queryProp(sb.str()));
          result.append("\"").append("][").append(pstr->toCharArray());
        }

        result.setLength(result.length() - pstr->length() - 2);
        delete pstr;

        xpath+=pos1+4;
      }
      else if (strncmp(xpath, "$./", 3) == 0)
      {
        String xpath2(xpath+3);
        StringBuffer sb(xpath2);
        int pos = xpath2.indexOf(']');
        if (pos != -1)
          sb.clear().append(xpath2.substring(0, pos)->toCharArray());

        result.append('\"');
        result.append(pNode->queryProp(sb.str()));
        result.append('\"');
        xpath+=pos+3; //skip past $./ and xpath2
      }
      else  
        result.append(*xpath++);
    }
    else
      result.append(*xpath++);
  }
}

void expandRange(IPropertyTree* pComputers)
{
  if (pComputers && pComputers->hasProp("@hasrange"))
  {
    if(!strcmp(pComputers->queryProp("@hasrange"), "true"))
    {
      Owned<IPropertyTreeIterator> rangeIter = pComputers->getElements("ComputerRange");
      ForEach(*rangeIter)
      {
        StringArray ipList;
        IPropertyTree* pEachComp = &rangeIter->query();
        formIPList(pEachComp->queryProp(XML_ATTR_NETADDRESS), ipList);

        for(unsigned i = 0 ; i < ipList.ordinality(); i++)
        {
          IPropertyTree* pElem = pComputers->addPropTree(XML_TAG_COMPUTER,createPTree());
          pElem->addProp(XML_ATTR_NETADDRESS,ipList.item(i));
        }

        pComputers->removeTree(pEachComp);
      }

      pComputers->removeProp("@hasrange");
    }
  }
}

CConfigHelper::CConfigHelper()
{
}

CConfigHelper::~CConfigHelper()
{
}

void CConfigHelper::init(const IPropertyTree *cfg, const char* esp_name)
{
  StringBuffer xpath;

  xpath.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALCONFFILE);
  m_strConfFile = cfg->queryProp(xpath.str());

  xpath.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALENVCONFFILE);
  m_strEnvConfFile = cfg->queryProp(xpath.str());

  if (m_strConfFile.length() > 0 && m_strEnvConfFile.length() > 0)
  {
    Owned<IProperties> pParams = createProperties(m_strConfFile);
    Owned<IProperties> pEnvParams = createProperties(m_strEnvConfFile);

    m_strConfigXMLDir = pEnvParams->queryProp(TAG_PATH);

    if ( m_strConfigXMLDir.length() == 0)
    {
      m_strConfigXMLDir = INSTALL_DIR;
    }

    m_strBuildSetFileName = pParams->queryProp(TAG_BUILDSET);

    m_strBuildSetFilePath.append(m_strConfigXMLDir).append(STANDARD_CONFIG_CONFIGXML_DIR).append( m_strBuildSetFileName.length() > 0 ? m_strBuildSetFileName : STANDARD_CONFIG_BUILDSETFILE);
    m_pDefBldSet.set(createPTreeFromXMLFile(m_strBuildSetFilePath.str()));
  }
}

bool CConfigHelper::isInBuildSet(const char* comp_process_name, const char* comp_name) const
{
  StringBuffer xpath;

  xpath.appendf("./%s/%s/%s[%s=\"%s\"][%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_PROCESS_NAME, comp_process_name, XML_ATTR_NAME, comp_name);

  if (strcmp(XML_TAG_DIRECTORIES,comp_name) != 0 && m_pDefBldSet->queryPropTree(xpath.str()) == NULL)
  {
     return false;
  }
  else
  {
     return true;
  }
}

CWsDeployExCE::~CWsDeployExCE()
{
    m_pCfg.clear();
    closeEnvironment();
    closedownClientProcess();
    HashIterator iter(m_fileInfos);
    ForEach(iter)
  {
    IMapping &cur = iter.query();
    CWsDeployFileInfo* pInfo = m_fileInfos.mapToValue(&cur);
    pInfo->Release();
  }

  m_fileInfos.kill();
}

IPropertyTree* CWsDeployFileInfo::getEnvTree(IEspContext &context, IConstWsDeployReqInfo *reqInfo)
{
  StringBuffer sbName, sbUserIp;
  if (reqInfo)
    sbName.clear().append(reqInfo->getUserId());
  
  context.getPeer(sbUserIp);

  if (m_userWithLock.length() && !strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(), m_userIp.str()) &&  m_Environment != NULL)
    return &m_Environment->getPTree();
  else
    return &m_constEnvRdOnly->getPTree();
}

void CWsDeployFileInfo::activeUserNotResponding()
{
  m_activeUserNotResp = true;
}

void CWsDeployExCE::init(IPropertyTree *cfg, const char *process, const char *service)
{
  if (m_lastStarted.isNull())
    m_lastStarted.setNow();

  if (m_pCfg.get() == NULL)
    m_pCfg.setown(createPTreeFromIPT(cfg));

  if (m_process.length() == 0)
    m_process.append(process);

  if (m_service.length() == 0)
    m_service.append(service);

  m_bCloud = false;
  StringBuffer xpath;
  m_envFile.clear();

  m_configHelper.init(cfg,service);

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile", service);
  const char* tmp = cfg->queryProp(xpath.str());
  if (tmp && *tmp)
  {
    Owned<IProperties> pParams = createProperties(tmp);
    m_sourceDir.clear().append(pParams->queryProp("sourcedir"));
    if (!m_sourceDir.length())
      m_sourceDir.clear().append(STANDARD_CONFIG_SOURCEDIR);

    m_backupDir.clear().append(m_sourceDir).append(PATHSEPSTR"backup");
  }

  if (m_backupDir.length() == 0)
    m_backupDir.clear().append(STANDARD_CONFIG_BACKUPDIR);

  if (!m_sourceDir.length())
    m_sourceDir.clear().append(STANDARD_CONFIG_SOURCEDIR);

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvFile", service);
  const char* pEnvFile = cfg->queryProp(xpath.str());

  if (pEnvFile && *pEnvFile)
  {
    CWsDeployFileInfo* fi = m_fileInfos.getValue(pEnvFile);
    StringBuffer sb;

    if (!fi)
    {
      synchronized block(m_mutexSrv);
      StringBuffer filePath(pEnvFile);

      if (strstr(pEnvFile, m_sourceDir.str()) != pEnvFile)
      {
        filePath.clear().append(m_sourceDir);
        filePath.append(PATHSEPCHAR);
        filePath.append(pEnvFile);
      }

      fi = new CWsDeployFileInfo(this, filePath.str(), m_bCloud);
      const char* psz = strrchr(pEnvFile, PATHSEPCHAR);
      if (!psz)
        psz = strrchr(pEnvFile, PATHSEPCHAR == '\\' ? '/' : '\\');

      if (!psz)
        sb.append(pEnvFile);
      else
        sb.append(psz + 1);
      m_fileInfos.setValue(sb.str(), fi);
    }

    try
    {
      fi->initFileInfo(false);
    }
    catch (IException* e)
    {
      m_fileInfos.remove(sb.str());
      delete fi;
      e->Release();
    }

    m_envFile.append(pEnvFile);
  }
}

bool CWsDeployFileInfo::navMenuEvent(IEspContext &context, 
                                     IEspNavMenuEventRequest &req, 
                                     IEspNavMenuEventResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), false);

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();

  if (!cmd || !*cmd)
    throw ::MakeStringException(-1, "Invalid command specified!");

  if (!strcmp(cmd, "LockEnvironment"))
  {
    StringBuffer sbName, sbUserIp;
    sbName.clear().append(req.getReqInfo().getUserId());
    context.getPeer(sbUserIp);

    if (!strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(), m_userIp.str()))
      throw MakeStringException(-1, "Another browser window already has write access on machine '%s'. Please use that window.", sbUserIp.str());
  }  
  else if (strcmp(cmd, "SaveEnvironmentAs"))
    checkForRefresh(context, &req.getReqInfo(), true);

  if (!stricmp(cmd, "Deploy"))
  {
    /*  xmlArg for is of the form:

    <EspNavigationData>
    <Folder name="Environment">
    <Folder name="Software">
    <Folder name="EspProcesses">
    <Link name="ESP Process - esp1" selected="true"/>
    <Link name="ESP Process - esp2" selected="true"/>
    </Folder>
    <Folder name="EclServerProcess - eclserver" selected="true"/>
    </Folder>
    </Folder>
    </EspNavigationData>

    There are one or more attributes and modules marked as @selected
    so enumerate these selected nodes and produce xml of the form:

    <Deploy>
    <SelectedComponents>
    <EspProcess name="esp1"/>//deploy all instances
    <EspProcess name="esp2"/>
    <Instance name="s1" computer="2wd20"/> //deploy selected instances only
    </EspProcess>
    </SelectedComponents>
    </Deploy>
    */
    Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
    IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree(XML_TAG_SOFTWARE);

    Owned<IPropertyTree> pDeploy = createPTree("Deploy");
    IPropertyTree* pComponents = pDeploy->addPropTree("Components", createPTree());

    if (pEnvSoftware)
    {
      Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
      IPropertyTree* pSoftwareFolder = pSrcTree->queryPropTree("Folder[@name='Environment']/Folder[@name='Software']");

      if (pSoftwareFolder)
      {
        bool bSelected = pSoftwareFolder->getPropBool("@selected", false);

        if (bSelected)
        {
          Owned<IPropertyTreeIterator> iComp = pEnvSoftware->getElements("*", iptiter_sort );
          ForEach(*iComp)
            addDeployableComponentAndInstances(pEnvRoot, &iComp->query(), pComponents, NULL, NULL);
        }
        else
        {
          Owned<IPropertyTreeIterator> iFolder = pSoftwareFolder->getElements( "*" );
          ForEach (*iFolder)
          {
            IPropertyTree* pFolder = &iFolder->query();
            bool bSelected = pFolder->getPropBool("@selected", false);

            const char* szFolderName = pFolder->queryProp(XML_ATTR_NAME);
            if (strstr(szFolderName, " - ") != NULL)
            {
              IPropertyTree* pCompInEnv = findComponentForFolder(pFolder, pEnvSoftware);

              if (pCompInEnv)
                addDeployableComponentAndInstances(pEnvRoot, pCompInEnv, pComponents, pFolder, NULL);
            }
            else if (!strcmp(szFolderName, "ECL Servers"))
            {
              Owned<IPropertyTreeIterator> iSubFolder = pFolder->getElements( "*" );

              ForEach (*iSubFolder)
              {
                IPropertyTree* pSubFolder = &iSubFolder->query();
                IPropertyTree* pCompInEnv = findComponentForFolder(pSubFolder, pEnvSoftware);

                if (pCompInEnv)
                  addDeployableComponentAndInstances(pEnvRoot, pCompInEnv, pComponents, pSubFolder, NULL);
              }
            }
            else
            {
              //this folder bundles multiple components and has name like "ESP Servers"
              //
              if (bSelected)
              {
                //if selected, deploy all its components like deploy all ESP servers
                //
                const char* params = pFolder->queryProp("@params");
                if (params)
                {
                  // pFolder's @params has string of the form: "comp=EspProcess"
                  //
                  Owned<IProperties> pParams = createProperties();
                  pParams->loadProps(params);

                  const char* comp = pParams->queryProp("comp");
                  if (comp && *comp)
                  {
                    Owned<IPropertyTreeIterator> iComp = pEnvSoftware->getElements(comp);
                    ForEach(*iComp)
                      addDeployableComponentAndInstances(pEnvRoot, &iComp->query(), pComponents, NULL, szFolderName);
                  }
                }
              }
              else
              {
                //deploy only selected components under this folder
                //
                Owned<IPropertyTreeIterator> iFolder = pFolder->getElements("*[@selected='true']");
                ForEach(*iFolder)
                {
                  IPropertyTree* pFolder = &iFolder->query();
                  IPropertyTree* pCompInEnv = findComponentForFolder(pFolder, pEnvSoftware);
                  if (pCompInEnv)
                    addDeployableComponentAndInstances(pEnvRoot, pCompInEnv, pComponents, pFolder, szFolderName);
                }
              }
            }
          }
        }//software folder is not selected
      }//pSoftwareFolder
    }//pEnvSoftware

    StringBuffer xml;
    toXML(pDeploy, xml, false);

    resp.setComponent( "WsDeploy" );
    resp.setCommand ( "Deploy" );
    resp.setXmlArgs( xml.str() );
  }//deploy
  else
    if (!stricmp(cmd, "Dependencies"))
    {
      /*    xmlArg for is of the form:

      <EspNavigationData>
      <Folder name="Environment">
      <Folder name="Hardware">
      <Folder name="Computers">
      <Link name="machine1"/>
      <Link name="machine2"/>
      </Folder>
      </Folder>
      </Folder>
      </EspNavigationData>

      There are one or more attributes and modules marked as @selected
      so enumerate these selected nodes and produce xml of the form:

      <Deploy>
      <SelectedComponents>
      <EspProcess name="esp1"/>//deploy all instances
      <EspProcess name="esp2"/>
      <Instance name="s1" computer="2wd20"/> //deploy selected instances only
      </EspProcess>
      </SelectedComponents>
      </Deploy>
      */
      Owned<IPropertyTree> pDeploy = createPTree("Deploy");
      IPropertyTree* pComponents = pDeploy->addPropTree("Components", createPTree());

      if (xmlArg && *xmlArg)
      {
        Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString( xmlArg);
        IPropertyTree* pComputersFolder = pSrcTree->queryPropTree("Folder[@name='Environment']/Folder[@name='Hardware']/Folder[@name='Computers']");

        if (pComputersFolder)
        {
          Owned<IPropertyTreeIterator> iLink = pComputersFolder->getElements( "Link" );
          ForEach (*iLink)
          {
            IPropertyTree* pLink = &iLink->query();
            const bool bSelected = pLink->getPropBool("@selected", false);
            const char* szComputerName = pLink->queryProp(XML_ATTR_NAME);

            //this folder bundles multiple components and has name like "ESP Servers"
            //
            if (bSelected)
            {
              //deploy all its components like deploy all ESP servers
              //addDeployableComponentAndInstances(pEnvRoot, pCompInEnv, pComponents, pFolder, szFolderName);
            }
          }
        }//pSoftwareFolder
      }//pEnvSoftware

      StringBuffer xml;
      toXML(pDeploy, xml, false);

      resp.setComponent( "WsDeploy" );
      resp.setCommand   ( "Deploy" );
      resp.setXmlArgs( xml.str() );
    }//dependencies
    else if (!stricmp(cmd, "LockEnvironment"))
    {
      StringBuffer xml;
      try 
      {
        if (m_userWithLock.length() == 0 || m_activeUserNotResp)
        {
          StringBuffer sbName, sbUserIp;
          sbName.clear().append(req.getReqInfo().getUserId());
          context.getPeer(sbUserIp);

          if (m_pFile.get() && m_pFile->isReadOnly())
          {
            xml.appendf("Write access to the Environment cannot be provided as %s is Read Only.", m_envFile.str());
            resp.setXmlArgs(xml.str());
            return true;
          }
          
          if (m_bCloud)
          {
            StringBuffer sbMsg;
            Owned<IPropertyTree> pComputers = createPTreeFromXMLString(xmlArg);
            
            CCloudActionHandler lockCloud(this, CLOUD_LOCK_ENV, CLOUD_UNLOCK_ENV, sbName.str(), "8015", pComputers);
            bool ret = lockCloud.start(sbMsg);
            if (!ret || sbMsg.length())
            {
              xml.appendf("Write access to the Environment cannot be provided. Reason(s):\n%s", sbMsg.str());
              resp.setXmlArgs(xml.str());
              return true;
            }
            else if (pComputers && pComputers->numChildren())
              m_lockedNodesBeforeEnv.set(pComputers);
          }

          StringBuffer sb;

          if (m_userWithLock.length() == 0)
            sb.append(sbName).append(sbUserIp);
          else
          {
            sb.append(m_userWithLock).append(m_userIp);
            m_userWithLock.clear();
            m_userIp.clear();
            m_Environment.clear();
            m_activeUserNotResp = false;
          }

          CClientAliveThread* th = m_keepAliveHTable.getValue(sb.str());
          if (th)
          {
            th->Release();
            m_keepAliveHTable.remove(sb.str());
          }

          StringBuffer sbxml;
          if (m_pFileIO.get())
          {
            Owned <IPropertyTree> pTree = createPTree(*m_pFileIO);
            toXML(pTree, sbxml);
          }
          else
            toXML(&m_constEnvRdOnly->getPTree(), sbxml);
          
          Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
          m_Environment.setown(factory->loadLocalEnvironment(sbxml.str()));
          m_userWithLock.clear().append(req.getReqInfo().getUserId());
          context.getPeer(m_userIp.clear());

          Owned<IPropertyTree> pEnvRoot = &m_Environment->getPTree();
          unsigned timeout = pEnvRoot->getPropInt(XML_TAG_ENVSETTINGS"/brokenconntimeout", 60);
          
          th = new CClientAliveThread(this, timeout * 60 * 1000);
          m_keepAliveHTable.setValue(sb.str(), th);
          th->init();

          StringBuffer tmp;
          m_lastSaved.getString(tmp);
          resp.setLastSaved(tmp.str());
          resp.setComponent( "WsDeploy" );
          resp.setCommand   ( "LockEnvironment" );
        }
        else
        {
          StringBuffer sbName, sbUserIp;
          sbName.clear().append(req.getReqInfo().getUserId());
          context.getPeer(sbUserIp);

          if (strcmp(sbName.str(), m_userWithLock.str()) || strcmp(sbUserIp.str(), m_userIp.str()))
          {
            xml.appendf("Write access to the Environment cannot be provided as it is currently being used on machine %s.", m_userIp.str());
            resp.setXmlArgs(xml.str());
          }
        }
      }
      catch (IException* e)
      {
        StringBuffer sErrMsg;
        e->errorMessage(sErrMsg);
        e->Release();         
        if (m_envFile.length())
        {
          char achHost[128] = "";
          const char* p = strstr(sErrMsg.str(), "\n\n");
          if (p && *(p+=2))
          {
            const char* q = strchr(p, ':');
            if (q)
            {
              const int len = q-p;
              strncpy(achHost, p, len);
              achHost[len] = '\0';
            }
          }

          //resolve hostname for this IP address
          unsigned int addr = inet_addr(achHost);
          struct hostent* hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);

          if (hp)
          {
            strcpy(achHost, hp->h_name);
            //strlwr(achHost);
          }

          StringBuffer sMsg;
          sMsg.appendf("Error accessing the environment definition");

          if (achHost[0])
            sMsg.appendf(" \nbecause it is locked by computer '%s'.", achHost);
          else
            sMsg.append(":\n\n").append(sErrMsg);

          throw MakeStringException(0, "%s", sMsg.str());
        }
        else
        {
          StringBuffer sMsg;
          sMsg.append("Error locking environment. ").append(sErrMsg.str());
          throw MakeStringException(-1, "%s", sMsg.str());
        }
      }
    }
    else if (!stricmp(cmd, "UnlockEnvironment"))
    {
      StringBuffer xml;
      try 
      {
        StringBuffer sbUser, sbUserIp, errMsg;
        context.getUserID(sbUser);
        sbUser.clear().append(req.getReqInfo().getUserId());
        context.getPeer(sbUserIp);

        if (!stricmp(m_userWithLock.str(), sbUser.str()) && !stricmp(sbUserIp.str(), m_userIp.str()))
        {
          Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
          IPropertyTree* pSaveEnv = pSrcTree->queryPropTree("SaveEnv[@flag='true']");

          StringBuffer sbErrMsg;
          if (pSaveEnv)
          {
            StringBuffer sb;
            m_skipEnvUpdateFromNotification = true;
            saveEnvironment(&context, &req.getReqInfo(), sb);

            if (sb.length())
              sbErrMsg.appendf("<Warning>%s</Warning>", sb.str());
          }

          unlockEnvironment(&context, &req.getReqInfo(), xmlArg, sbErrMsg, pSaveEnv != NULL);
          if (sbErrMsg.length())
          {
            resp.setXmlArgs(sbErrMsg.str());
            return true;
          }

          m_skipEnvUpdateFromNotification = false;

          StringBuffer tmp;
          m_lastSaved.getString(tmp);
          resp.setLastSaved(tmp.str());
          resp.setComponent( "WsDeploy" );
          resp.setCommand   ( "UnlockEnvironment" );
        }
        else
        {
          xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
          resp.setComponent( "WsDeploy" );
          resp.setCommand   ( "UnlockEnvironment" );
          resp.setXmlArgs( xml.str() );
        }
      }
      catch (IException* e)
      {
        StringBuffer sErrMsg;
        e->errorMessage(sErrMsg);
        e->Release();         

        if (m_envFile.length() == 0)
        {
          char achHost[128] = "";
          const char* p = strstr(sErrMsg.str(), "\n\n");
          if (p && *(p+=2))
          {
            const char* q = strchr(p, ':');
            if (q)
            {
              const int len = q-p;
              strncpy(achHost, p, len);
              achHost[len] = '\0';
            }
          }

          //resolve hostname for this IP address
          unsigned int addr = inet_addr(achHost);
          struct hostent* hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);

          if (hp)
          {
            strcpy(achHost, hp->h_name);
          }

          StringBuffer sMsg;
          sMsg.appendf("The environment definition in dali server "
            "could not be opened for write access");

          if (achHost[0])
            sMsg.appendf(" \nbecause it is locked by computer '%s'.", achHost);
          else
            sMsg.append(":\n\n").append(sErrMsg);

          throw MakeStringException(0, "%s", sMsg.str());
        }
        else
        {
          StringBuffer sMsg;
          sMsg.append("Error unlocking environment. ").append(sErrMsg.str());
          throw MakeStringException(-1, "%s", sMsg.str());
        }
      }
    }
    else if (!stricmp(cmd, "SaveEnvironment"))
    {
      StringBuffer xml;
      StringBuffer sbUser, sbIp, sbErrMsg;
      sbUser.clear().append(req.getReqInfo().getUserId());
      context.getPeer(sbIp);

      if (m_userWithLock.length() != 0 && m_userIp.length() != 0 &&
         !strcmp(m_userWithLock.str(), sbUser.str()) && !strcmp(m_userIp.str(), sbIp.str()) && 
         m_Environment != NULL)
      {
        saveEnvironment(&context, &req.getReqInfo(), sbErrMsg);

        if (sbErrMsg.length())
          resp.setXmlArgs(sbErrMsg.str());

        StringBuffer tmp;
        m_lastSaved.getString(tmp);
        resp.setLastSaved(tmp.str());
        resp.setComponent( "WsDeploy" );
        resp.setCommand ( "SaveEnvironment" );
      }
      else
      {
        xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
        resp.setComponent( "WsDeploy" );
        resp.setCommand ( "SaveEnvironment" );
        resp.setXmlArgs( xml.str() );
      }
    }
    else if (!stricmp(cmd, "SaveEnvironmentAs"))
    {
      StringBuffer xml;
      StringBuffer sbUser, sbIp;
      sbUser.clear().append(req.getReqInfo().getUserId());
      context.getPeer(sbIp);
      const char* xmlArg = req.getXmlArgs();
      Owned<IPropertyTree> pParamTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
      const char* envSaveAs = pParamTree->queryProp("@envSaveAs");
      if (envSaveAs && *envSaveAs)
      {
        StringBuffer filePath(m_pService->getSourceDir());
        StringBuffer sbErrMsg;

        if (filePath.charAt(filePath.length() - 1) != PATHSEPCHAR)
          filePath.append(PATHSEPCHAR);
        filePath.append(envSaveAs);
        if (!strcmp(filePath.str(), m_envFile.str()))
        {
          if (m_userWithLock.length() != 0 && m_userIp.length() != 0 &&
             !strcmp(m_userWithLock.str(), sbUser.str()) && !strcmp(m_userIp.str(), sbIp.str()) && 
             m_Environment != NULL)
          {
            saveEnvironment(&context, &req.getReqInfo(), sbErrMsg);
            unlockEnvironment(&context, &req.getReqInfo(), "", sbErrMsg);
            if (sbErrMsg.length())
            {
              resp.setXmlArgs(sbErrMsg.str());
              return true;
            }

            StringBuffer tmp;
            m_lastSaved.getString(tmp);
            resp.setLastSaved(tmp.str());
            resp.setComponent("WsDeploy");
            resp.setCommand ("SaveEnvironmentAs");
          }
          else if (m_userWithLock.length() == 0 && m_userIp.length() == 0)
          {
            saveEnvironment(&context, &req.getReqInfo(), sbErrMsg, true);

            if (sbErrMsg.length())
              resp.setXmlArgs(sbErrMsg.str());

            StringBuffer tmp;
            m_lastSaved.getString(tmp);
            resp.setLastSaved(tmp.str());
            resp.setComponent("WsDeploy");
            resp.setCommand("SaveEnvironmentAs");
          }
        }
        else
        {
          CWsDeployFileInfo* fi = m_pService->getFileInfo(envSaveAs, true);
          StringBuffer sbUser, sbIp, sbXml;

          if ( fi->isLocked(sbUser, sbIp) == true)
            throw MakeStringException(-1, "%s is locked by another user. File not saved.", envSaveAs);
          if (isLocked(sbUser,sbIp) == true)  // if we are in write only mode then save the working environment
            toXML(&m_Environment->getPTree(), sbXml);
          else
            toXML(&m_constEnvRdOnly->getPTree(), sbXml);
        
          if (fi->updateEnvironment(sbXml))
            fi->saveEnvironment(NULL, &req.getReqInfo(), sbErrMsg, true);
          else
            throw MakeStringException(-1, "Environment Save as operation has failed");

          if (m_userWithLock.length() != 0 && m_userIp.length() != 0)
          {
            if (sbErrMsg.length())
            {
              resp.setXmlArgs(sbErrMsg.str());
              return true;
            }
          }

          if (sbErrMsg.length())
            resp.setXmlArgs(sbErrMsg.str());

          StringBuffer tmp;
          fi->getLastSaved(tmp);
          resp.setLastSaved(tmp.str());
          resp.setComponent("WsDeploy");
          resp.setCommand("SaveEnvironmentAs");
        }
      }
      else
        throw MakeStringException(-1, "File name to save environment as cannot be empty");
    }
    else if (!stricmp(cmd, "ValidateEnvironment"))
    {
      StringBuffer xml;
      StringBuffer sbUser, sbIp;
      sbUser.clear().append(req.getReqInfo().getUserId());
      context.getPeer(sbIp);
      if (m_userWithLock.length() != 0 && m_userIp.length() != 0 &&
         !strcmp(m_userWithLock.str(), sbUser.str()) && !strcmp(m_userIp.str(), sbIp.str()) && 
         m_Environment != NULL)
      {
        if (m_envFile.length())
          validateEnv((IConstEnvironment*)m_Environment, false);
        
        resp.setComponent( "WsDeploy" );
        resp.setCommand ( "ValidateEnvironment" );
      }
      else
      {
        xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
        resp.setComponent( "WsDeploy" );
        resp.setCommand ( "SaveEnvironment" );
        resp.setXmlArgs( xml.str() );
      }
    }
    else
    {
      StringBuffer url;
      url.append("/WsDeploy/").append(cmd).append("?form");
      resp.setRedirectUrl(url.str());
    }
    return true;
}//onNavMenuEvent

bool CWsDeployFileInfo::isAlphaNumeric(const char *pstr) const
{
  RegExpr expr("[A-Za-z0-9-_]+");

  return (expr.find(pstr) && expr.findlen(0) == strlen(pstr));
}

bool CWsDeployFileInfo::saveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  bool bUpdateFilesBasedn = req.getBUpdateFilesBasedn();
  Owned<IPropertyTree> pEnvRoot = &m_Environment->getPTree();
  IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree(XML_TAG_SOFTWARE);
  IPropertyTree* pEnvHardware = pEnvRoot->queryPropTree(XML_TAG_HARDWARE);

  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  IPropertyTree* pSoftwareFolder = pSrcTree->queryPropTree("Setting[@category='Software']");
  IPropertyTree* pTopologyFolder = pSrcTree->queryPropTree("Setting[@category='Topology']");
  IPropertyTree* pHardwareFolder = pSrcTree->queryPropTree("Setting[@category='Hardware']");
  IPropertyTree* pEnvFolder = pSrcTree->queryPropTree("Setting[@category='Environment']");

  if (pSoftwareFolder)
  {
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@category='Software']");
    ForEach (*iter)
    {
      IPropertyTree* pSetting = &iter->query();
      StringBuffer decodedParams( pSetting->queryProp("@params") );
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");
      const char* pszAttrName = pSetting->queryProp("@attrName");
      const char* rowIndex = pSetting->queryProp("@rowIndex");
      const char* pszOldValue = pSetting->queryProp("@oldValue");
      const char* pszNewValue = pSetting->queryProp("@newValue");
      const char* pszOnChange = pSetting->queryProp("@onChange");
      const char* pszViewType = pSetting->queryProp("@viewType");

      StringBuffer xpath;
      xpath.clear().appendf("%s[@name='%s']", pszCompType, pszCompName);

      if (pszSubType && pszSubTypeKey && strlen(pszSubTypeKey) > 0)
      {
        if (pszSubTypeKey[0] == '[' && pszSubTypeKey[strlen(pszSubTypeKey) - 1] == ']')
          xpath.appendf("/%s%s", pszSubType, pszSubTypeKey);
        else
          xpath.appendf("/%s[@name='%s']", pszSubType, pszSubTypeKey);
      }
      else if (!strcmp(pszCompType, "Topology"))
        xpath.appendf("[@name='%s']", pszSubTypeKey);
      else if (pszSubType && strlen(rowIndex) > 0)
        xpath.appendf("/%s[%s]", pszSubType, rowIndex);

      IPropertyTree* pComp = pEnvSoftware->queryPropTree(xpath.str());
      if (!pComp)
      {
        //read any updates to rows that have not been posted to the tree
        const char* pszUpdate = NULL;
        const char* key = NULL;
        int idx = 1;
        StringBuffer sbUpdate;
        StringBuffer xpath2;
        while (true)
        {
          sbUpdate.clear().appendf("Update%dKey", idx);
          key = pParams->queryProp(sbUpdate.str());

          if (!key)
            break;
          else
          {
            sbUpdate.clear().appendf("Update%dValue", idx);
            pszUpdate = pParams->queryProp(sbUpdate.str());
            break;
          }

          idx++;
        }

        xpath2.clear().appendf("*[@name='%s']", pszCompName);
        Owned<IPropertyTreeIterator> iter = pEnvSoftware->getElements(xpath2);

        ForEach (*iter)
        {
          IPropertyTree* pTmpComp = &iter->query();
          const char* pProcessName = pTmpComp->queryName();

          if (pProcessName && !strcmp(pProcessName, pszCompType))
          {
            if (pszSubType && pszSubTypeKey && strlen(pszSubTypeKey) > 0)
            {
              if (pszSubTypeKey[0] == '[' && pszSubTypeKey[strlen(pszSubTypeKey) - 1] == ']')
                xpath2.clear().appendf("%s%s", pszSubType, pszSubTypeKey);
              else
                xpath2.clear().appendf("%s[@name='%s']", pszSubType, pszSubTypeKey);
              pComp = pTmpComp->queryPropTree(xpath2.str());

              if (!pComp)
              {
                if (pszUpdate && pszSubType && key)
                  xpath2.clear().appendf("%s[@%s='%s']", pszSubType, key, pszUpdate);

                pComp = pTmpComp->queryPropTree(xpath2.str());
              }
            }
            else if (pszSubType)
            {
              StringBuffer tmppath;
              tmppath.clear().appendf("%s", pszSubType);
              pComp = pTmpComp->queryPropTree(tmppath.str());

              if (!pComp)
              {
                if (pszUpdate && key)
                {
                  StringBuffer sbtmp(pszSubType);
                  StringBuffer sb;
                  const char* psz = strrchr(pszSubType, '/');
                  const char* ptmp = pszSubType;

                  if (psz)
                    sbtmp.clear().append(psz + 1);

                  psz = sbtmp.str();

                  if (strchr(pszSubType, '[') && strchr(pszSubType, ']'))
                  {
                    char ch;
                    bool copy = true;
                    bool flag = false;
                    while ((ch = *ptmp++) != '\0')
                    {
                      if (ch == '/')
                      {
                        flag = true;
                        break;
                      }
                      else if (ch == '[')
                        copy = false;
                      else if (ch == ']')
                        copy = true;
                      else if (copy)
                        sb.append(ch);
                    }

                    sb.appendf("[@%s='%s']/%s", key, pszUpdate, psz);
                  }

                  xpath2.clear().append(sb);
                }

                pComp = pTmpComp->queryPropTree(xpath2.str());
              }

              if (!pComp)
              {
                if (!strcmp(pszCompName, "Directories"))
                  tmppath.clear().append(pszCompName);
                else
                  tmppath.clear().appendf("%s[@name='%s']", pszCompType, pszCompName);

                IPropertyTree* pTmpComp = pEnvSoftware->queryPropTree(tmppath.str());
                if (!pTmpComp)
                  continue;

                if (!strcmp(pszCompType, XML_TAG_ESPSERVICE) || !strcmp(pszCompType, XML_TAG_PLUGINPROCESS))
                  tmppath.clear().appendf("./Programs/Build/BuildSet[@name=\"%s\"]", pTmpComp->queryProp(XML_ATTR_BUILDSET));
                else
                  tmppath.clear().appendf("./Programs/Build/BuildSet[@processName=\"%s\"]", pszCompType);

                Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(tmppath.str());
                buildSetIter->first();
                IPropertyTree* pBuildSet;
                if (buildSetIter->isValid())
                  pBuildSet = &buildSetIter->query();
                else if (!strcmp(pszCompType, "Directories"))
                {      
                  pBuildSet = createPTree(XML_TAG_BUILDSET);
                  pBuildSet->addProp(XML_ATTR_NAME, pszCompName);
                  pBuildSet->addProp(XML_ATTR_SCHEMA, "directories.xsd");
                  pBuildSet->addProp(XML_ATTR_PROCESS_NAME, "Directories");
                }
                const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
                const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

                StringBuffer buildSetPath;
                Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);

                Owned<IPropertyTree> pNewCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());
                StringBuffer sbtmp(pszSubType);
                StringBuffer sb;
                const char* psz = strrchr(pszSubType, '/');

                if (psz)
                  sbtmp.clear().append(psz + 1);

                psz = sbtmp.str();

                if (strchr(psz, '[') && strchr(psz, ']'))
                {
                  char ch;
                  while ((ch = *psz++) != '\0')
                  {
                    if (ch != '[')
                      sb.append(ch);
                    else
                      break;
                  }

                  while ((ch = *psz++) != '\0')
                  {
                    if (ch == ']')
                      break;
                  }
                  
                  sb.append(psz);
                  sbtmp.clear().append(sb);
                }

                Owned<IPropertyTreeIterator> iterElems = pNewCompTree->getElements(sbtmp.str());

                  ForEach (*iterElems)
                  {
                    IPropertyTree* pElem = &iterElems->query();
                
                  if (pElem)
                    pComp = pTmpComp->addPropTree(pszSubType, createPTreeFromIPT(pElem));
                  else
                    pComp = pTmpComp->addPropTree(pszSubType, createPTree());

                  break;
                }

                if (!pComp)
                  pComp = pTmpComp->addPropTree(pszSubType, createPTree());

                if (pComp && !strcmp(pszAttrName, "name"))
                  pComp->setProp(XML_ATTR_NAME, pszNewValue);
              }
            }
            else
              pComp = &iter->query();

            break;
          }

          const char* pBuildSet = pTmpComp->queryProp(XML_ATTR_BUILDSET);
          if (pBuildSet && !strcmp(pBuildSet, pszCompType))
          {
            if (pszSubType && pszSubTypeKey && strlen(pszSubTypeKey) > 0)
            {
              if (pszSubTypeKey[0] == '[' && pszSubTypeKey[strlen(pszSubTypeKey) - 1] == ']')
                xpath2.clear().appendf("%s%s", pszSubType, pszSubTypeKey);
              else
                xpath2.clear().appendf("%s[@name='%s']", pszSubType, pszSubTypeKey);

              pComp = pTmpComp->queryPropTree(xpath2.str());

              if (!pComp)
              {
                if (pszUpdate && pszSubType && key)
                  xpath2.clear().appendf("%s[@%s='%s']", pszSubType, key, pszUpdate);

                pComp = pTmpComp->queryPropTree(xpath2.str());
              }
            }
            else if (pszSubType && strlen(rowIndex) > 0)
            {
              xpath2.clear().appendf("%s[%s]", pszSubType, rowIndex);
              pComp = pTmpComp->queryPropTree(xpath2.str());
            }
            else if (pszSubType)
            {
              xpath2.clear().appendf("%s", pszSubType);
              pComp = pTmpComp->queryPropTree(xpath2.str());
            }
            else
              pComp = &iter->query();

            break;
          }
        }
      }

      if (bUpdateFilesBasedn == true && strcmp(pszAttrName, TAG_LDAPSERVER) == 0 && strcmp(pszCompType, XML_TAG_DALISERVERPROCESS) == 0 && pComp != NULL)
      {
        Owned<IPropertyTree> pActiveEnvRoot = getEnvTree(context, &req.getReqInfo());

        StringBuffer ldapXPath;
        StringBuffer strFilesBasedn;

        if (pszNewValue && *pszNewValue)
        {
          ldapXPath.appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_LDAPSERVERPROCESS, XML_ATTR_NAME, pszNewValue);
          strFilesBasedn.appendf("%s",pActiveEnvRoot->queryPropTree(ldapXPath.str())->queryProp(XML_ATTR_FILESBASEDN));
        }

        pComp->setProp(XML_ATTR_FILESBASEDN,strFilesBasedn);
      }
      else if (bUpdateFilesBasedn == true && pComp != NULL && strcmp(pszCompType, XML_TAG_ESPPROCESS) == 0 && strcmp(pszAttrName, TAG_SERVICE) == 0 && pszNewValue && *pszNewValue)
      {
        Owned<IPropertyTree> pActiveEnvRoot = getEnvTree(context, &req.getReqInfo());

        StringBuffer ldapXPath;
        StringBuffer espServiceXPath;
        StringBuffer espProcessXPath;
        StringBuffer strFilesBasedn;

        espServiceXPath.appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, pszNewValue);
        espProcessXPath.appendf("./%s/%s/[%s=\"%s\"]/%s", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_ATTR_NAME, pszCompName, XML_TAG_AUTHENTICATION);

        StringBuffer strLDAPName(pActiveEnvRoot->queryPropTree(espProcessXPath.str())->queryProp(XML_ATTR_LDAPSERVER));

        if (strLDAPName.length() > 0)
        {
          ldapXPath.appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_LDAPSERVERPROCESS, XML_ATTR_NAME, strLDAPName.str());
          strFilesBasedn.appendf("%s", pActiveEnvRoot->queryPropTree(ldapXPath.str())->queryProp(XML_ATTR_FILESBASEDN));
        }
        pActiveEnvRoot->queryPropTree(espServiceXPath.str())->setProp(XML_ATTR_FILESBASEDN, strFilesBasedn);
      }
      else if (bUpdateFilesBasedn == true && strcmp(pszAttrName, TAG_LDAPSERVER) == 0 && strcmp(pszCompType, XML_TAG_ESPPROCESS) == 0 && pComp != NULL)
      {
        Owned<IPropertyTree> pActiveEnvRoot = getEnvTree(context, &req.getReqInfo());

        StringBuffer ldapXPath;
        StringBuffer espBindingXPath;
        StringBuffer espProcessXPath;
        StringBuffer strFilesBasedn;

        if (pszNewValue != NULL && *pszNewValue != 0)
        {
          ldapXPath.appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_LDAPSERVERPROCESS, XML_ATTR_NAME, pszNewValue);
          strFilesBasedn.appendf("%s",pActiveEnvRoot->queryPropTree(ldapXPath.str())->queryProp(XML_ATTR_FILESBASEDN));
        }

        espBindingXPath.appendf("./%s/%s[%s=\"%s\"]/%s", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_ATTR_NAME, pszCompName, XML_TAG_ESPBINDING);

        Owned<IPropertyTreeIterator> iterItems = pActiveEnvRoot->getElements(espBindingXPath.str());

        ForEach(*iterItems)
        {
          IPropertyTree *pItem = &iterItems->query();
          const char* service_name = pItem->queryProp(XML_ATTR_SERVICE);

          espProcessXPath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, service_name);

          const char* service_type = pActiveEnvRoot->queryPropTree(espProcessXPath.str())->queryProp(XML_ATTR_BUILDSET);

          if (service_type && *service_type && !strcmp(service_type, "espsmc"))
            pActiveEnvRoot->queryPropTree(espProcessXPath.str())->setProp(XML_ATTR_FILESBASEDN, strFilesBasedn);
        }
      }
      // Update of LDAP component filesBasedn
      else if (bUpdateFilesBasedn == true && strcmp(pszAttrName, TAG_FILESBASEDN) == 0 && strcmp(pszCompType, XML_TAG_LDAPSERVERPROCESS) == 0 && pszCompName != NULL && pszNewValue != NULL)
      {
        // update dali
        StringBuffer daliProcessXPath;
        daliProcessXPath.appendf("./%s/%s", XML_TAG_SOFTWARE, XML_TAG_DALISERVERPROCESS);

        Owned<IPropertyTree> pActiveEnvRoot = getEnvTree(context, &req.getReqInfo());
        Owned<IPropertyTreeIterator> iterItems = pActiveEnvRoot->getElements(daliProcessXPath.str());

        ForEach(*iterItems)
        {
          IPropertyTree *pItem = &iterItems->query();
          const char* ldap_server = pItem->queryProp(XML_ATTR_LDAPSERVER);

          // check if dali has this ldap server assigned before changing filesBasedn
          if (ldap_server != NULL && strcmp(ldap_server, pszCompName) == 0)
            pItem->setProp(XML_ATTR_FILESBASEDN, pszNewValue);
        }

        //update esp services
        StringBuffer espProcessXPath;
        StringBuffer espBindingXPath;
        StringBuffer espServiceXPath;

        espProcessXPath.appendf("./%s/%s", XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS);
        Owned<IPropertyTreeIterator> iterItems2 = pActiveEnvRoot->getElements(espProcessXPath.str());

        ForEach(*iterItems2)
        {
          IPropertyTree *pItem = &iterItems2->query();
          const char* ldap_server = pItem->queryPropTree(XML_TAG_AUTHENTICATION)->queryProp(XML_ATTR_LDAPSERVER);

          if (ldap_server != NULL && strcmp(ldap_server, pszCompName) == 0)
          {
            espBindingXPath.clear().appendf("%s[%s=\"%s\"]/%s", espProcessXPath.str(), XML_ATTR_NAME, pItem->queryProp(XML_ATTR_NAME), XML_TAG_ESPBINDING);

            Owned<IPropertyTreeIterator> iterItems3 = pActiveEnvRoot->getElements(espBindingXPath.str());

            ForEach(*iterItems3)
            {
              IPropertyTree *pItem = &iterItems3->query();
              const char* service_name = pItem->queryProp(XML_ATTR_SERVICE);

              espServiceXPath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, service_name);

              const char* service_type = pActiveEnvRoot->queryPropTree(espServiceXPath.str())->queryProp(XML_ATTR_BUILDSET);

              if (service_type && *service_type && !strcmp(service_type, "espsmc"))
                pActiveEnvRoot->queryPropTree(espServiceXPath.str())->setProp(XML_ATTR_FILESBASEDN, pszNewValue);
            }
          }
        }
      }

      if (!pComp)
        throw MakeStringException(-1, "No such component in environment: '%s' named '%s'.", pszCompType, pszCompName);
      else
      {
        if (pszOnChange && !strcmp(pszOnChange, "2"))
        {
          StringBuffer xpathBSet;
          //get the onChange xslt and apply it.
          if (!strcmp(pszCompType, XML_TAG_ESPSERVICE) || !strcmp(pszCompType, XML_TAG_PLUGINPROCESS))
            xpathBSet.clear().appendf("./Programs/Build/BuildSet[@name=\"%s\"]", pComp->queryProp(XML_ATTR_BUILDSET));
          else
            xpathBSet.clear().appendf("./Programs/Build/BuildSet[@processName=\"%s\"]", pszCompType);

          Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpathBSet.str());
          buildSetIter->first();
          IPropertyTree* pBuildSet;
          
          if (!buildSetIter->isValid())
          {
            xpathBSet.clear().appendf("./Programs/Build/BuildSet[@name=\"%s\"]", pszCompType);
            buildSetIter.setown(pEnvRoot->getElements(xpathBSet.str()));
            buildSetIter->first();
          }
          
          if (buildSetIter->isValid())
          {
            pBuildSet = &buildSetIter->query();

            const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
            const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

            StringBuffer buildSetPath;
            Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);

            StringBuffer xpathOnChg;
            xpathOnChg.append(".//xs:attribute[@name=\"").append(pszAttrName).append("\"]//onchange");
            IPropertyTree* pAttr = pSchema->queryPropTree(xpathOnChg.str());
            xpathOnChg.clear().append(".//onchange");
            IPropertyTree* pOnChange = pAttr? pAttr->queryPropTree(xpathOnChg.str()) : NULL;

            if (pAttr)
            {
              const char* onChangeXslt = pAttr->queryProp("xslt");
              if (onChangeXslt && *onChangeXslt)
              {
                StringBuffer sbAttrName("@");
                sbAttrName.append(pszAttrName);
                pComp->setProp(sbAttrName.str(), pszNewValue);
                    if (onChangeAttribute(pEnvRoot, m_Environment, pszAttrName, pAttr, pComp, pComp /*TBD Calc parent nodeGetParentNode()*/, 0, pszNewValue, pszOldValue, pszCompType))
                {
                  bool bRefresh = pAttr->getPropBool("refresh", true);
                  if (bRefresh)
                  {
                    resp.setRefresh("true");
                    resp.setUpdateValue(pszNewValue);
                    return true;
                  }
                }
                else
                {
                  pComp->setProp(sbAttrName.str(), pszOldValue);
                  resp.setUpdateValue(pszOldValue);
                  return true;
                }
              }
            }
          }
        }

        //perform checks
        if (!strcmp(pszAttrName, "name"))
        {
          ensureUniqueName(pEnvRoot, pComp, pszNewValue);

          if (isAlphaNumeric(pszNewValue) == false)
          {
            throw MakeStringException(-1, "Invalid Character in name '%s'.", pszNewValue);
          }
        }

        //Store prev settings for use further down for esp service bindings
        const char* sPrevDefaultPort = NULL;
        const char* sPrevPort = NULL;
        const char* sPrevResBasedn = NULL;
        const char* sPrevDefaultSecurePort = NULL;
        const char* sPrevDefaultResBasedn = NULL;

        if (pszSubType && !strcmp(pszSubType, XML_TAG_ESPBINDING) && 
          (!strcmp(pszAttrName, "service") || !strcmp(pszAttrName, "protocol")))
        {
          const char* szServiceName = xpath.clear().append("Software/EspService[@name=\"");
          xpath.append(pComp->queryProp(XML_ATTR_SERVICE));
          xpath.append("\"]/Properties");
          IPropertyTree* pPrevSvcProps = pEnvRoot->queryPropTree(xpath);

          if (pPrevSvcProps)
          {
            sPrevDefaultPort = pPrevSvcProps->queryProp("@defaultPort");
            sPrevDefaultSecurePort= pPrevSvcProps->queryProp("@defaultSecurePort");
            sPrevDefaultResBasedn = pPrevSvcProps->queryProp("@defaultResourcesBasedn");
          }

          sPrevPort      = pComp->queryProp("@port");
          sPrevResBasedn = pComp->queryProp("@resourcesBasedn");
        }

        bool isSet = false;
        if (pszSubType && strstr(pszSubType, "Notes") == pszSubType && !strcmp(pszAttrName, "Note"))
        {
          pComp->setProp(".", pszNewValue);
          isSet = true;
        }
        else if (pszCompType && !strcmp(pszCompType, XML_TAG_ESPPROCESS))
        {
          String strXPath(xpath);
          if (strXPath.indexOf("CSR") != -1 || strXPath.indexOf("Certificate") != -1 || strXPath.indexOf("PrivateKey") != -1)
          {
            StringBuffer sbNewVal(pszNewValue);
            pEnvSoftware->setProp(xpath, sbNewVal.str());
            isSet = true;
          }
        }

        StringBuffer encryptedText;
        
        if (!isSet)
        {
          xpath.clear().appendf("@%s", pszAttrName);

          if (pszViewType && *pszViewType && !strcmp(pszViewType, "password"))
          {
            encrypt(encryptedText, pszNewValue);
            pszNewValue = encryptedText.str();
          }

          pComp->setProp(xpath, pszNewValue);
        }
        
        resp.setUpdateValue(pszNewValue);

        if (!strcmp(pszAttrName, "name"))
        {
          if (!pszSubType || !*pszSubType)
          {
            StringBuffer rundir;

            if (!getConfigurationDirectory(pEnvRoot->queryPropTree("Software/Directories"), "run", pszCompType, pszNewValue, rundir))
              rundir.clear().appendf(RUNTIME_DIR"/%s", pszNewValue);

            Owned<IPropertyTreeIterator> iterInsts = pComp->getElements(XML_TAG_INSTANCE);

            ForEach (*iterInsts)
              iterInsts->query().setProp(XML_ATTR_DIRECTORY, rundir.str());

            if (!strcmp(pszCompType, XML_TAG_ROXIECLUSTER))
              pComp->setProp(XML_ATTR_DIRECTORY, rundir);
          }

          StringBuffer sbold, sbnew, sbMsg;
          bool ret = false;
          if (pszSubType && !strcmp(pszSubType, "EspBinding"))
            {
            sbold.append(pszCompName).append('/').append(pszOldValue);
            sbnew.append(pszCompName).append('/').append(pszNewValue);
            ret = checkComponentReferences(pEnvRoot, pComp, sbold.str(), sbMsg, sbnew.str());
          }
          else
            ret = checkComponentReferences(pEnvRoot, pComp, pszOldValue, sbMsg, pszNewValue);

          if (ret)
            resp.setRefresh("true");
        }

        if (pszCompType && *pszCompType && pszSubType && *pszSubType &&
            !strcmp(pszCompType, XML_TAG_ROXIECLUSTER) && !strcmp(pszSubType, XML_TAG_ROXIE_FARM)
            && pszAttrName && *pszAttrName)
        {
          Owned<IPropertyTreeIterator> iter = pComp->getElements(XML_TAG_ROXIE_SERVER);
          ForEach (*iter)
          {
            IPropertyTree* pSrv = &iter->query();

            const char* pszName = pSrv->queryProp(XML_ATTR_NAME);
            xpath.clear().appendf(XML_TAG_ROXIECLUSTER"[@name='%s']/"XML_TAG_ROXIE_SERVER"[@name='%s']/", pszCompName, pszName);
          
            IPropertyTree* pServer = pEnvSoftware->queryPropTree(xpath.str());
            if (pServer)
            {
              StringBuffer sbattr("@");
              sbattr.append(pszAttrName);
              pServer->setProp(sbattr.str(), pszNewValue);
            }
          }
        }

        //if dataDirectory for a roxie farm is being changed, also change baseDataDir for roxie
        if(!strcmp(pszCompType, "Directories") && !strcmp(pszSubType, "Category"))
        {
          StringBuffer sbNewValue;
          bool bdata = strstr(pszSubTypeKey, "[@name='data']") || strstr(pszSubTypeKey, "[@name=\"data\"]");\
          bool bdata2 = strstr(pszSubTypeKey, "[@name='data2']") || strstr(pszSubTypeKey, "[@name=\"data2\"]");
          bool bdata3 = strstr(pszSubTypeKey, "[@name='data3']") || strstr(pszSubTypeKey, "[@name=\"data3\"]");

          if (bdata || bdata2 || bdata3)
          {
            Owned<IPropertyTreeIterator> iterRoxies = pEnvSoftware->getElements("RoxieCluster");
            ForEach (*iterRoxies)
            {
              IPropertyTree* pRoxie = &iterRoxies->query();

              if (bdata)
              {
                getCommonDir(pEnvRoot, "data", "roxie", pRoxie->queryProp(XML_ATTR_NAME), sbNewValue.clear());
                pRoxie->setProp("@baseDataDir", sbNewValue.str());

                //change all farms
                Owned<IPropertyTreeIterator> iterFarms = pRoxie->getElements(XML_TAG_ROXIE_FARM);
                ForEach (*iterFarms)
                {
                  IPropertyTree* pTmpComp = &iterFarms->query();
                  if (strcmp(pTmpComp->queryProp(XML_ATTR_DATADIRECTORY), sbNewValue.str()))
                    pTmpComp->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                }

                //change all legacy Roxie servers
                Owned<IPropertyTreeIterator> iterRoxieServers = pRoxie->getElements(XML_TAG_ROXIE_SERVER);

                ForEach (*iterRoxieServers)
                {
                  IPropertyTree* pTmpComp = &iterRoxieServers->query();
                  if (strcmp(pTmpComp->queryProp(XML_ATTR_DATADIRECTORY), sbNewValue.str()))
                    pTmpComp->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                }

                //also change roxie slave primary data directory for all RoxieSlave and RoxieSlaveProcess
                Owned<IPropertyTreeIterator> iterSlvs = pRoxie->getElements(XML_TAG_ROXIE_ONLY_SLAVE);

                ForEach (*iterSlvs)
                {
                  IPropertyTree* pTmpComp = &iterSlvs->query();
                  const char* pRoxieComputer = pTmpComp->queryProp(XML_ATTR_COMPUTER);
                  IPropertyTree* pChannel = pTmpComp->queryPropTree(XML_TAG_ROXIE_CHANNEL"[1]");
                  if (pChannel)
                  {
                    pChannel->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                    const char* number = pChannel->queryProp("@number");
                    xpath.clear().appendf(XML_TAG_ROXIE_SLAVE"[@channel='%s'][@computer='%s']", number, pRoxieComputer);
                    
                    IPropertyTree* pSlvProc = pRoxie->queryPropTree(xpath.str());
                    if (pSlvProc)
                      pSlvProc->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                  }
                }
              }
              else if (bdata2 || bdata3)
              {
                getCommonDir(pEnvRoot, bdata2 ? "data2" : "data3" , "roxie", pRoxie->queryProp(XML_ATTR_NAME), sbNewValue.clear());
                Owned<IPropertyTreeIterator> iterSlvs = pRoxie->getElements(XML_TAG_ROXIE_ONLY_SLAVE);
                StringBuffer sb(XML_TAG_ROXIE_CHANNEL);
                sb.appendf("%s", bdata2?"[2]":"[3]");

                ForEach (*iterSlvs)
                {
                  IPropertyTree* pTmpComp = &iterSlvs->query();
                  const char* pRoxieComputer = pTmpComp->queryProp(XML_ATTR_COMPUTER);
                  IPropertyTree* pChannel = pTmpComp->queryPropTree(sb.str());
                  if (pChannel)
                  {
                    pChannel->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                    const char* number = pChannel->queryProp("@number");
                    xpath.clear().appendf(XML_TAG_ROXIE_SLAVE"[@channel='%s'][@computer='%s']", number, pRoxieComputer);
                    
                    IPropertyTree* pSlvProc = pRoxie->queryPropTree(xpath.str());
                    if (pSlvProc)
                      pSlvProc->setProp(XML_ATTR_DATADIRECTORY, sbNewValue.str());
                  }
                }
              }
            }
          }
        }

        //if we are changing the eclServer field of wsattributes, set the following 
        //extra params from that eclserver. dbPassword, dbUser, mySQL, repository
        if (!strcmp(pszCompType, "WsAttributes") && !strcmp(pszAttrName, "eclServer"))
        {
          xpath.clear().appendf("Software/EclServerProcess[@name='%s']", pszNewValue);
          IPropertyTree* pEclServer = pEnvRoot->queryPropTree(xpath.str());
          if (pEclServer)
          {
            pComp->setProp("@dbPassword", pEclServer->queryProp("@dbPassword"));
            pComp->setProp("@dbUser", pEclServer->queryProp("@dbUser"));
            pComp->setProp("@mySQL", pEclServer->queryProp("@MySQL"));
            pComp->setProp("@repository", pEclServer->queryProp("@repository"));
          }
        }
        else if (!strcmp(pszCompType, "EclServerProcess") && (!strcmp(pszAttrName, "dbPassword") ||
            !strcmp(pszAttrName, "dbUser") || !strcmp(pszAttrName, "MySQL") ||
            !strcmp(pszAttrName, "repository")))
        {
          xpath.clear().append("Software/EspService[@buildSet='WsAttributes']");
          Owned<IPropertyTreeIterator> pWsAttrsIter = pEnvRoot->getElements(xpath.str());

          ForEach(*pWsAttrsIter)
          {
            IPropertyTree* wsAttr = &pWsAttrsIter->query();
            if (!strcmp(wsAttr->queryProp("@eclServer"), pszCompName))
            {
              wsAttr->setProp("@dbPassword", pComp->queryProp("@dbPassword"));
              wsAttr->setProp("@dbUser", pComp->queryProp("@dbUser"));
              wsAttr->setProp("@mySQL", pComp->queryProp("@MySQL"));
              wsAttr->setProp("@repository", pComp->queryProp("@repository"));
            }
          }
        }

        if (pszSubType && !strcmp(pszSubType, XML_TAG_INSTANCE) && !strcmp(pszAttrName, TAG_COMPUTER))
        {
          xpath.clear().appendf("Hardware/Computer[@name='%s']", pszNewValue);
          IPropertyTree* pEnvComputer = pEnvRoot->queryPropTree(xpath);

          if (pEnvComputer)
          {
            const char* pszNetAddr = pEnvComputer->queryProp(XML_ATTR_NETADDRESS);
            if (pszNetAddr)
            {
              StringBuffer prevValue;
              pComp->getProp(XML_ATTR_NETADDRESS, prevValue);
              resp.setPrevValue(prevValue.str());
              pComp->setProp(XML_ATTR_NETADDRESS, pszNetAddr);
              resp.setUpdateValue(pszNetAddr);
              resp.setUpdateAttr("netAddress");
            }

            const char* name = pComp->queryProp(XML_ATTR_NAME);
            if (!name || !*name)
            {
              pComp->setProp(XML_ATTR_NAME, "s1");
              StringBuffer sb;
              StringBuffer rundir;
              if (!getConfigurationDirectory(pEnvRoot->queryPropTree("Software/Directories"), "run", pszCompType, pszCompName, rundir))
                sb.clear().appendf(RUNTIME_DIR"/%s", pszCompName);
              else
                sb.clear().append(rundir);

              pComp->setProp(XML_ATTR_DIRECTORY, sb.str());
            }
          }
        }

        if (pszSubType && !strcmp(pszSubType, XML_TAG_ESPBINDING) && 
          (!strcmp(pszAttrName, "service") || !strcmp(pszAttrName, "protocol")))
        {
          bool bEspServiceChanged = !strcmp(pszAttrName, "service");
          const char* szSrvName = pComp->queryProp(XML_ATTR_SERVICE);
          xpath.clear().append("Software/EspService[@name=\"");
          xpath.append(bEspServiceChanged ? pszNewValue : szSrvName ? szSrvName : "");
          xpath.append("\"]/Properties");
          IPropertyTree* pSvcProps = pEnvRoot->queryPropTree(xpath.str());

          if (bEspServiceChanged)
          {
            IPropertyTree* pChild;
            while ((pChild = pComp->queryPropTree("Authenticate[1]")) != NULL)
              if (pChild)
                pComp->removeTree( pChild );

            while ((pChild = pComp->queryPropTree("AuthenticateFeature[1]")) != NULL)
              if (pChild)
                pComp->removeTree( pChild );

            while ((pChild = pComp->queryPropTree("AuthenticateSetting[1]")) != NULL)
              if (pChild)
                pComp->removeTree( pChild );
          }

          if (pSvcProps)
          {
            if (bEspServiceChanged)
            {
              xpath.clear().append("./Programs/Build/BuildSet[@processName=\"EspProcess\"]");
              Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
              buildSetIter->first();
              IPropertyTree* pBuildSet = &buildSetIter->query();
              const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
              const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
              StringBuffer buildSetPath;
              Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
              Owned<IPropertyTree> pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());

              Owned<IPropertyTreeIterator> i = pSvcProps->getElements("Authenticate");
              ForEach(*i)
              {
                IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                mergeAttributes(pAuthCopy, pCompTree->queryPropTree("Authenticate"));
                IPropertyTree* pNewNode = pComp->addPropTree("Authenticate", pAuthCopy);
              }

              i.setown( pSvcProps->getElements("AuthenticateFeature") );
              ForEach(*i)
              {
                IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateFeature"));
                IPropertyTree* pNewNode = pComp->addPropTree("AuthenticateFeature", pAuthCopy);
              }
              i.setown( pSvcProps->getElements("AuthenticateSetting") );
              ForEach(*i)
              {
                IPropertyTree* pAuthCopy = createPTreeFromIPT(&i->query());
                mergeAttributes(pAuthCopy, pCompTree->queryPropTree("AuthenticateSetting"));
                IPropertyTree* pNewNode = pComp->addPropTree("AuthenticateSetting", pAuthCopy);
              }
            }

            const char* szProtocol = pComp->queryProp("@protocol");
            bool   bHttps = szProtocol && !strcmp(szProtocol, "https");
            const char* szDefaultPort = pSvcProps->queryProp(bHttps  ? "@defaultSecurePort" : "@defaultPort");
            if (!bEspServiceChanged)//@protocol was just changed so use last one
              bHttps = !bHttps;
            const char* szPrevDefaultPort = bHttps ? sPrevDefaultSecurePort : sPrevDefaultPort;
            if (szDefaultPort && 
              ((!sPrevPort || (sPrevPort && !*sPrevPort)) || (szPrevDefaultPort && !strcmp(sPrevPort, szPrevDefaultPort))))
              pComp->setProp(XML_ATTR_PORT, szDefaultPort);

            if (bEspServiceChanged)
            {
              const char* szDefault = pSvcProps->queryProp("@defaultResourcesBasedn");

              if (szDefault && 
                ((!sPrevResBasedn || (sPrevResBasedn && !*sPrevResBasedn )) || (sPrevDefaultResBasedn && !strcmp(sPrevResBasedn, sPrevDefaultResBasedn))))
                pComp->setProp("@resourcesBasedn", szDefault);
            }
          }

          resp.setRefresh("true");
        }
      }

      resp.setIsSaved("true");
    }
  }
  else if (pTopologyFolder)
  {
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@category='Topology']");
    ForEach (*iter)
    {
      IPropertyTree* pSetting = &iter->query();
      StringBuffer decodedParams( pSetting->queryProp("@params") );
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");
      const char* pszAttrName = pSetting->queryProp("@attrName");
      const char* rowIndex = pSetting->queryProp("@rowIndex");
      const char* pszOldValue = pSetting->queryProp("@oldValue");
      const char* pszNewValue = pSetting->queryProp("@newValue");
            
      StringBuffer xpath_key;

      if (strcmp(pszAttrName,TAG_NAME) == 0)
      {
         xpath_key.appendf("%s/%s/%s[%s='%s']", XML_TAG_SOFTWARE, XML_TAG_TOPOLOGY, XML_TAG_CLUSTER, XML_ATTR_NAME, pszNewValue);

        //Check to see if the cluster name is already in use
        IPropertyTree* pEnvCluster = pEnvRoot->queryPropTree(xpath_key);
        if (pEnvCluster != NULL)
          throw MakeStringException(-1, "Cluster - %s is already in use. Please enter a unique name for the Cluster.", pszNewValue);      
      }

      StringBuffer buf("Topology");

      for (int i = 3; i >= 0; i--)
      {
        StringBuffer sb;
        sb.appendf("inner%d_name", i);
        const char* sbName = pParams->queryProp(sb.str());

        if (sbName)
        {
          if (strstr(sbName, "EclCCServerProcess") == sbName ||
              strstr(sbName, "EclServerProcess") == sbName ||
              strstr(sbName, "EclAgentProcess") == sbName ||
              strstr(sbName, "EclSchedulerProcess") == sbName)
          {
            StringBuffer sbEcl(sbName);
            if (strstr(sbName, " - "))
              sbEcl.replaceString(" - ", "[@process='").append("']");
            else if (strstr(sbName, " -"))
              sbEcl.replaceString(" -", "[@process='").append("']");
            buf.append("/").append(sbEcl);
          }
          else if (strstr(sbName, "Cluster") == sbName)
          {
            StringBuffer sbCl(sbName);
            if (strstr(sbName, " - "))
              sbCl.replaceString(" - ", "[@name='").append("']");
            else if (strstr(sbName, " -"))
              sbCl.replaceString(" -", "[@name='").append("']");
            buf.append("/").append(sbCl);
          }
          else if (strstr(sbName, XML_TAG_THORCLUSTER) == sbName)
            buf.append("/ThorCluster");
          else if (strstr(sbName, XML_TAG_ROXIECLUSTER) == sbName)
            buf.append("/RoxieCluster");
          else if (buf.str()[buf.length() - 1] != ']')
            buf.appendf("[@%s='%s']", sbName, pParams->queryProp(sb.replaceString("_name", "_value").str()));
        }
      }

      StringBuffer xpath;
      xpath.appendf("%s[@name='%s']", pszCompType, pszCompName);

      IPropertyTree* pComp = pEnvSoftware->queryPropTree(buf.str());
      if (!pComp)
      {
        xpath.clear().appendf("*[@name='%s']", pszCompName);
        Owned<IPropertyTreeIterator> iter = pEnvSoftware->getElements(xpath);

        ForEach (*iter)
        {
          IPropertyTree* pTmpComp = &iter->query();
          const char* pBldSet = pTmpComp->queryProp(XML_ATTR_BUILDSET);

          if (pBldSet && !strcmp(pBldSet, pszCompType))
          {
            pComp = &iter->query();
            break;
          }
        }
      }

      if (!pComp)
        throw MakeStringException(-1, "No such component in environment: '%s' named '%s'.", pszCompType, pszCompName);
      else
      {
        xpath.clear().appendf("@%s", pszAttrName);
        pComp->setProp(xpath, pszNewValue);
        resp.setUpdateValue(pszNewValue);

        if (pszSubType && !strcmp(pszSubType, XML_TAG_INSTANCE) && !strcmp(pszAttrName, TAG_COMPUTER))
        {
          xpath.clear().appendf("Hardware/Computer[@name='%s']", pszNewValue);
          IPropertyTree* pEnvComputer = pEnvRoot->queryPropTree(xpath);

          if (pEnvComputer)
          {
            const char* pszNetAddr = pEnvComputer->queryProp(XML_ATTR_NETADDRESS);
            if (pszNetAddr)
              pComp->setProp(XML_ATTR_NETADDRESS, pszNetAddr);
          }
        }
      }

      resp.setIsSaved("true");
    }
  }
  else if (pHardwareFolder)
  {
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@category='Hardware']");
    ForEach (*iter)
    {
      IPropertyTree* pSetting = &iter->query();
      StringBuffer decodedParams( pSetting->queryProp("@params") );
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");
      const char* pszAttrName = pSetting->queryProp("@attrName");
      const char* rowIndex = pSetting->queryProp("@rowIndex");
      const char* pszOldValue = pSetting->queryProp("@oldValue");
      const char* pszNewValue = pSetting->queryProp("@newValue");

      //read anyupdates to rows that have not been updated
      const char* pszUpdate = NULL;
      int idx = 1;
      StringBuffer sbUpdate;
      while (true)
      {
        sbUpdate.clear().appendf("Update%dKey", idx);
        const char* key = pParams->queryProp(sbUpdate.str());

        if (!key)
          break;
        else if (!strcmp(key, "name") || !strcmp(key, "Name"))
        {
          sbUpdate.clear().appendf("Update%dValue", idx);
          pszUpdate = pParams->queryProp(sbUpdate.str());
          break;
        }

        idx++;
      }

      StringBuffer xpath;
      if (pszSubType && pszSubTypeKey && strlen(pszSubTypeKey) > 0/* && !strcmp(pszSubType, "Instance")*/)
      {
        if (pszSubTypeKey[0] == '[' && pszSubTypeKey[strlen(pszSubTypeKey) - 1] == ']')
          xpath.appendf("%s%s", pszSubType, pszSubTypeKey);
        else
          xpath.clear().appendf("%s[@name='%s']", pszSubType, pszSubTypeKey);
      }
      else if (pszSubType && strlen(rowIndex) > 0/* && !strcmp(pszSubType, "Instance")*/)
        xpath.clear().appendf("%s[%s]", pszSubType, rowIndex);

      IPropertyTree* pComp = pEnvHardware->queryPropTree(xpath.str());
      if (!pComp)
      {
        //check for any updates first
        if (pszUpdate && pszSubType)
          xpath.clear().appendf("%s[@name='%s']", pszSubType, pszUpdate);

        pComp = pEnvHardware->queryPropTree(xpath.str());
        
        if (!pComp)
        {
          xpath.clear().appendf("*[@name='%s']", pszCompName);
          Owned<IPropertyTreeIterator> iter = pEnvHardware->getElements(xpath);

          ForEach (*iter)
          {
            IPropertyTree* pTmpComp = &iter->query();
            const char* pBldSet = pTmpComp->queryProp(XML_ATTR_BUILDSET);

            if (pBldSet && !strcmp(pBldSet, pszCompType))
            {
              pComp = &iter->query();
              break;
            }
          }
        }
      }

      if (!pComp)
        throw MakeStringException(-1, "No such component in environment: '%s' named '%s'.", pszCompType, pszCompName);
      else
      {
        if (!strcmp(pszAttrName, "name"))
        {
          xpath.clear().appendf("%s["XML_ATTR_NAME"='%s']", pszSubType, pszNewValue);

          if (pEnvHardware->queryPropTree(xpath.str()))
            throw MakeStringException(-1, "Another item exists with the same name '%s'!  Please specify a unique name.", pszNewValue);
        }

        xpath.clear().appendf("@%s", pszAttrName);
        String strAttrName(pszAttrName);
        StringBuffer encryptedText;
        if (strAttrName.toLowerCase()->endsWith("password"))
        {
          encrypt(encryptedText, pszNewValue);
          pszNewValue = encryptedText.str();
        }

        if (m_bCloud && !strcmp(pszAttrName, "netAddress"))
        {
          StringBuffer sb, sbMsg;
          {
            sb.clear().appendf("<Computers><Computer netAddress='%s'/></Computers>", pszNewValue);
            Owned<IPropertyTree> pComputer = createPTreeFromXMLString(sb.str());
            CCloudActionHandler lockCloud(this, CLOUD_LOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", pComputer);
            bool ret = lockCloud.start(sbMsg.clear());
            if (!ret || sbMsg.length())
              throw MakeStringException(-1, "Cannot set netAddress as environment lock could not be obtained. Reason(s):\n%s", sbMsg.str());
          }

          if (pszOldValue && *pszOldValue)
          {
            sb.clear().appendf("<Computers><Computer netAddress='%s'/></Computers>", pszOldValue);
            Owned<IPropertyTree> pComputer = createPTreeFromXMLString(sb.str());
            CCloudActionHandler unlockCloud(this, CLOUD_UNLOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", pComputer);
            bool ret = unlockCloud.start(sbMsg.clear());
            if (!ret || sbMsg.length())
            {
              //Unlock the new node.
              {
                sb.clear().appendf("<Computers><Computer netAddress='%s'/></Computers>", pszNewValue);
                Owned<IPropertyTree> pComputer = createPTreeFromXMLString(sb.str());
                CCloudActionHandler unlockPrevCloud(this, CLOUD_UNLOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", pComputer);
                ret = unlockPrevCloud.start(sbMsg.clear());
              }

              throw MakeStringException(-1, "Cannot set netAddress as some targets could not be unlocked. Reason(s):\n%s", sbMsg.str());
            }
          }
        }

        pComp->setProp(xpath, pszNewValue);
        resp.setUpdateValue(pszNewValue);

        //update references
        if (!strcmp(pszAttrName, "name"))
        {
          if (!strcmp(pszSubType, XML_TAG_COMPUTER))
          {
            UpdateRefAttributes(pEnvRoot, XML_TAG_SOFTWARE"//*", XML_ATTR_COMPUTER, pszOldValue, pszNewValue);
            UpdateRefAttributes(pEnvRoot, XML_TAG_SOFTWARE"/"XML_TAG_DALISERVERPROCESS, XML_ATTR_BACKUPCOMPUTER, pszOldValue, pszNewValue);
          }
          else if (!strcmp(pszSubType, XML_TAG_DOMAIN))
            UpdateRefAttributes(pEnvRoot, XML_TAG_HARDWARE"/"XML_TAG_COMPUTER, XML_ATTR_DOMAIN, pszOldValue, pszNewValue);
          else if (!strcmp(pszSubType, XML_TAG_SWITCH))
            UpdateRefAttributes(pEnvRoot, XML_TAG_HARDWARE"/"XML_TAG_COMPUTER, XML_ATTR_SWITCH, pszOldValue, pszNewValue);
          else if (!strcmp(pszSubType, XML_TAG_COMPUTERTYPE))
            UpdateRefAttributes(pEnvRoot, XML_TAG_HARDWARE"/"XML_TAG_COMPUTER, XML_ATTR_COMPUTERTYPE, pszOldValue, pszNewValue);
        }
        else if (!strcmp(pszAttrName, "netAddress"))
        {
          Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements(XML_TAG_SOFTWARE"//*");
          for (iter->first(); iter->isValid(); iter->next())
          {
            IPropertyTree& node = iter->query();
            const char* szVal = node.queryProp(XML_ATTR_COMPUTER);
            const char* szComputer = pComp->queryProp(XML_ATTR_NAME);
            if (szVal && strcmp(szVal, szComputer)==0)
            {
              if (node.hasProp(XML_ATTR_NETADDRESS))
                node.setProp(XML_ATTR_NETADDRESS, pszNewValue);      
            }
          }
        }

        if (pszSubType && !strcmp(pszSubType, XML_TAG_INSTANCE) && !strcmp(pszAttrName, TAG_COMPUTER))
        {
          xpath.clear().appendf("Hardware/Computer[@name='%s']", pszNewValue);
          IPropertyTree* pEnvComputer = pEnvRoot->queryPropTree(xpath);

          if (pEnvComputer)
          {
            const char* pszNetAddr = pEnvComputer->queryProp(XML_ATTR_NETADDRESS);
            if (pszNetAddr)
              pComp->setProp(XML_ATTR_NETADDRESS, pszNetAddr);
          }
        }
      }

      resp.setIsSaved("true");
    }
  }
  else if (pEnvFolder)
  {
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@category='Environment']");
    ForEach (*iter)
    {
      IPropertyTree* pSetting = &iter->query();
      StringBuffer decodedParams( pSetting->queryProp("@params") );
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszAttrName = pSetting->queryProp("@attrName");
      const char* pszOldValue = pSetting->queryProp("@oldValue");
      const char* pszNewValue = pSetting->queryProp("@newValue");

      StringBuffer xpath;
      const char* pszUpdate = NULL;
      int idx = 1;
      StringBuffer sbUpdate;
      while (true)
      {
        sbUpdate.clear().appendf("Update%dKey", idx);
        const char* key = pParams->queryProp(sbUpdate.str());

        if (!key)
          break;
        else if (!strcmp(key, "name") || !strcmp(key, "Name"))
        {
          sbUpdate.clear().appendf("Update%dValue", idx);
          pszUpdate = pParams->queryProp(sbUpdate.str());
          break;
        }

        idx++;
      }

      const char* pszParams = NULL;
      idx = 2;
      StringBuffer sbParams;
      StringBuffer sb;
      bool flag = false;
      const char* isAttr = pParams->queryProp("isAttr");

      while (true)
      {
        sbParams.clear().appendf("parentParams%d", idx);
        const char* val = pParams->queryProp(sbParams.str());

        if (!val || !*val)
        {
          if (!flag)
            flag = true;
          else
          {
            if (pszUpdate)
            {
              sbParams.clear().appendf("parentParams%d", idx-2);
              const char* val = pParams->queryProp(sbParams.str());
              String st(val);
              if (st.indexOf("pcName=") != -1)
                sbParams.clear().append("[@name='").append(*st.substring(st.indexOf("pcName=") + 7)).append("']");
              
              String str(xpath.str());
              sb.clear().append(*str.substring(0, str.indexOf(sbParams.str())));
              sb.appendf("[@name='%s']", pszUpdate);
              xpath.clear().append(sb.str());
            }

            break;
          }

          idx++;
          continue;
        }
        else 
        {
          flag = false;
          sb.clear();
          StringBuffer params(val);
          params.replaceString(":", "\n");
          Owned<IProperties> pSubParams = createProperties();
          pSubParams->loadProps(params.str());
          sb.append(pSubParams->queryProp("pcType"));

          if (sb.length())
            xpath.append(xpath.length()?"/":"").append(sb.str());

          sb.clear().append(pSubParams->queryProp("pcName"));

          if (sb.length())
            xpath.appendf("[@name='%s']", sb.str());
        }

        idx++;
      }

      IPropertyTree* pComp = pEnvRoot->queryPropTree(xpath.str());
      if (pComp)
      {
        sb.clear();
        if (!isAttr)
          sb.append("@");
        sb.append(pszAttrName);
        pComp->setProp(sb.str(), pszNewValue);
        resp.setUpdateValue(pszNewValue);
      }
      else
        throw MakeStringException(-1, "Cannot find component/attribute in environment: '%s'.", pszAttrName);
    }
  }

  return true;
}

bool CWsDeployFileInfo::getNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp)
{
  synchronized block(m_mutex);
  const char* xmlArg = req.getXmlArgs();
  StringBuffer sbDefn, sb;
  resp.setReadOnly("false");
  bool doreload = true;
  StringBuffer decodedParams(xmlArg);
  decodedParams.replaceString("::", "\n");
  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());

  const char* reload = pParams->queryProp("reloadEnv");

  StringBuffer sbName, sbUserIp;
  sbName.clear().append(req.getReqInfo().getUserId());
  context.getPeer(sbUserIp);

  if (!strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(), m_userIp.str()) && reload && !strcmp(reload, "true"))
    throw MakeStringException(-1, "Another browser window already has write access on machine '%s'. Please use that window.", sbUserIp.str());

  if (m_pNavTree.get() == NULL)
    m_pNavTree.setown(getEnvTree(context, &req.getReqInfo()));

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  generateHeadersFromEnv(pEnvRoot, sbDefn);

  resp.setCompDefn(sbDefn.str());
  m_lastSaved.getString(sb.clear());
  resp.setLastSaved(sb.str());
  m_pService->getLastStarted(sb.clear());
  resp.setLastStarted(sb.str());
  return true;
}

bool CWsDeployFileInfo::lockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp)
{
  synchronized block(m_mutex);
  const char* user = req.getUserName();
  const char* ip = req.getIp();

  if (!user || !*user || !ip || !*ip)
  {
    resp.setReturnCode(0);
    resp.setMsg("User name or ip cannot be empty");
  }

  StringBuffer xml;
  try 
  {
    if (m_userWithLock.length() == 0)
    {
      StringBuffer sb;
      StringBuffer sbUserIp;
      context.getPeer(sbUserIp);
      
      m_userWithLock.clear().append(user);
      m_userIp.clear().append(sbUserIp);

      Owned<IPropertyTree> pEnvRoot = &m_constEnvRdOnly->getPTree();
      unsigned timeout = 60;
      if (pEnvRoot)
        timeout = pEnvRoot->getPropInt(XML_TAG_ENVSETTINGS"/brokenconntimeout", 60);

      if (m_cloudLockerAliveThread && m_cloudLockerAliveThread.get() != NULL)
        m_cloudLockerAliveThread.clear();

      m_cloudLockerAliveThread.setown(new CLockerAliveThread(this, timeout * 60 * 1000, user, sbUserIp.str()));
      m_cloudLockerAliveThread->init();

      resp.setReturnCode(1);
      resp.setMsg("");
    }
    else
    {
      xml.appendf("Write access to the Environment cannot be provided as it is currently being used on machine %s.", m_userIp.str());
      resp.setMsg(xml.str());
      resp.setReturnCode(0);
    }
  }
  catch (IException* e)
  {
    StringBuffer sErrMsg;
    e->errorMessage(sErrMsg);
    e->Release();         

    char achHost[128] = "";
    const char* p = strstr(sErrMsg.str(), "\n\n");
    if (p && *(p+=2))
    {
      const char* q = strchr(p, ':');
      if (q)
      {
        const int len = q-p;
        strncpy(achHost, p, len);
        achHost[len] = '\0';
      }
    }

    unsigned int addr = inet_addr(achHost);
    struct hostent* hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);

    if (hp)
      strcpy(achHost, hp->h_name);

    StringBuffer sMsg;
    sMsg.appendf("Error accessing the environment definition");

    if (achHost[0])
      sMsg.appendf(" \nbecause it is locked by computer '%s'.", achHost);
    else
      sMsg.append(":\n\n").append(sErrMsg);

    //throw MakeStringException(0, sMsg);
    resp.setMsg(sMsg.str());
    resp.setReturnCode(0);
  }

  return true;
}

bool CWsDeployFileInfo::unlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp)
{
  synchronized block(m_mutex);
  const char* user = req.getUserName();
  const char* ip = req.getIp();
  const char* newEnv = req.getNewEnvXml();

  if (!user || !*user || !ip || !*ip)
  {
    resp.setReturnCode(0);
    resp.setMsg("User name or ip cannot be empty");
  }

  StringBuffer xml, sbMsg;
  int ret = 0;
  try 
  {
    if (!stricmp(m_userWithLock.str(), user) && !stricmp(m_userIp.str(), ip))
    {
      m_Environment.clear();

      m_userWithLock.clear();
      m_userIp.clear();

      if (m_cloudLockerAliveThread)
      {
        m_cloudLockerAliveThread->signal();
        m_cloudLockerAliveThread.clear();
      }

      Owned<IPropertyTree>  pNavTree(getEnvTree(context, &req.getReqInfo()));

      if (!areMatchingPTrees(pNavTree, m_pNavTree))
      {
        m_pEnvXml.clear();
        m_pGraphXml.clear();
        m_pNavTree.clear();
        m_pNavTree.setown(getEnvTree(context, &req.getReqInfo()));
      }

      resp.setReturnCode(1);
      resp.setMsg("");
    }
    else
    {
      xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
      resp.setReturnCode(0);
      resp.setMsg(xml.str());
    }
  }
  catch (IException* e)
  {
    StringBuffer sErrMsg;
    e->errorMessage(sErrMsg);
    e->Release();         
    char achHost[128] = "";
    const char* p = strstr(sErrMsg.str(), "\n\n");
    if (p && *(p+=2))
    {
      const char* q = strchr(p, ':');
      if (q)
      {
        const int len = q-p;
        strncpy(achHost, p, len);
        achHost[len] = '\0';
      }
    }

    unsigned int addr = inet_addr(achHost);
    struct hostent* hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);

    if (hp)
      strcpy(achHost, hp->h_name);

    StringBuffer sMsg;
    sMsg.appendf("The environment definition in dali server "
      "could not be opened for write access");

    if (achHost[0])
      sMsg.appendf(" \nbecause it is locked by computer '%s'.", achHost);
    else
      sMsg.append(":\n\n").append(sErrMsg);

    resp.setReturnCode(0);
    resp.setMsg(sMsg.str());
  }

  return true;
}

bool CWsDeployFileInfo::saveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp)
{
    synchronized block(m_mutex);
    const char* user = req.getUserName();
    const char* ip = req.getIp();

    if (!user || !*user || !ip || !*ip)
    {
        resp.setReturnCode(0);
        resp.setMsg("User name or ip cannot be empty");
    }

    StringBuffer xml;
    try 
    {
        if (m_userWithLock.length() == 0)
        {
            xml.appendf("Environment cannot be saved as it is not locked.");
            resp.setMsg(xml.str());
            resp.setReturnCode(0);
            return false;
        }
        else if (stricmp(m_userWithLock.str(), user) || stricmp(m_userIp.str(), ip))
        {
            xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
            resp.setReturnCode(0);
            resp.setMsg(xml.str());
            return false;
        }
        else
        {
            const char* newEnv = req.getNewEnv();
            const char* newEnvId = req.getId();
            m_cloudEnvId.clear().append(newEnvId);

            if (!newEnv || !*newEnv)
            {
                resp.setReturnCode(0);
                resp.setMsg("Input xml cannot be empty");
                return false;
            }

            StringBuffer sbBackup;
            setEnvironment(context, &req.getReqInfo(), newEnv, "SaveEnvironmentForCloud", sbBackup, false, false);
            m_cloudEnvBkupFileName.clear().append(sbBackup);

            resp.setReturnCode(1);
            resp.setMsg("");
        }
    }
    catch (IException* e)
    {
        StringBuffer sErrMsg;
        e->errorMessage(sErrMsg);
        e->Release();         

        resp.setMsg(sErrMsg.str());
        resp.setReturnCode(0);
    }

    return true;
}

bool CWsDeployFileInfo::rollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp)
{
    synchronized block(m_mutex);
    const char* user = req.getUserName();
    const char* ip = req.getIp();

    if (!user || !*user || !ip || !*ip)
    {
        resp.setReturnCode(0);
        resp.setMsg("User name or ip cannot be empty");
    }

    StringBuffer xml;
    try 
    {
        if (m_userWithLock.length() == 0)
        {
            xml.appendf("Cannot rollback environment as it is not locked.");
            resp.setMsg(xml.str());
            resp.setReturnCode(0);
            return false;
        }
        else if (stricmp(m_userWithLock.str(), user) || stricmp(m_userIp.str(), ip))
        {
            xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
            resp.setReturnCode(0);
            resp.setMsg(xml.str());
            return false;
        }
        else
        {
            const char* newEnvId = req.getId();

            if (newEnvId && *newEnvId && !strcmp(newEnvId, m_cloudEnvId.str()))
            {
                StringBuffer sbBackup;
                Owned<IFile> pFile = createIFile(m_cloudEnvBkupFileName.str());
                Owned<IFileIO> pFileIO = pFile->open(IFOreadwrite);
                StringBuffer sbxml;
                {
                    Owned <IPropertyTree> pTree = createPTree(*pFileIO);
                    toXML(pTree, sbxml);
                    setEnvironment(context, &req.getReqInfo(), sbxml, "RollbackEnvironmentForCloud", sbBackup);
                }

                resp.setReturnCode(1);
                resp.setMsg("");
            }
            else
            {
                resp.setReturnCode(0);
                resp.setMsg("Cannot rollback Environment as rollback id does not match");
                return false;
            }
        }
    }
    catch (IException* e)
    {
        StringBuffer sErrMsg;
        e->errorMessage(sErrMsg);
        e->Release();         

        resp.setMsg(sErrMsg.str());
        resp.setReturnCode(0);
    }

    return true;
}

bool CWsDeployFileInfo::notifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp)
{
    synchronized block(m_mutex);
    const char* user = req.getUserName();
    const char* ip = req.getIp();

    if (!user || !*user || !ip || !*ip)
    {
        resp.setReturnCode(0);
        resp.setMsg("User name or ip cannot be empty");
    }

    StringBuffer xml;
    try 
    {
        if (m_userWithLock.length() == 0)
        {
            xml.appendf("Cannot notify init system as the environment as it is not locked.");
            resp.setMsg(xml.str());
            resp.setReturnCode(0);
            return false;
        }
        else if (stricmp(m_userWithLock.str(), user) || stricmp(m_userIp.str(), ip))
        {
            xml.appendf("The environment has been locked on machine '%s'", m_userIp.str());
            resp.setReturnCode(0);
            resp.setMsg(xml.str());
            return false;
        }
        else
        {
            StringBuffer xpath;
            xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile", m_pService->getName());

            const char* pConfFile = m_pService->getCfg()->queryProp(xpath.str());

            if( pConfFile && *pConfFile)
            {
                Owned<IProperties> pParams = createProperties(pConfFile);
                Owned<IPropertyIterator> iter = pParams->getIterator();
                StringBuffer prop, out, err;
                pParams->getProp("initSystemNotifyScript", prop);
                if(prop.length())
                    runScript(out, err, prop.str());

                resp.setMsg("");
                resp.setReturnCode(1);
            }
        }
    }
    catch(IException* e)
    {
        throw e;
    }

    return true;
}

bool CWsDeployFileInfo::getValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp)
{
  synchronized block(m_mutex);
  StringBuffer decodedParams(req.getParams());
  decodedParams.replaceString("::", "\n");

  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());
  const char* pszCategory = pParams->queryProp("category");
  const char* pszBldSet = pParams->queryProp(TAG_BUILDSET);
  const char* pszCompName = pParams->queryProp("compName");
  const char* pszSubType = pParams->queryProp("subType");
  const char* pszSubTypeName = pParams->queryProp("subTypeName");
  const char* pszAttrName = pParams->queryProp("attrName");
  const char* pszQueryType = pParams->queryProp("queryType");
  const char* pszExcludePath = pParams->queryProp("excludePath");
  const char* pszExcludeAttr = pParams->queryProp("excludeAttr");
  const char* pszXpath = pParams->queryProp("xpath");
  const char* pszparams = pParams->queryProp("params");
  const char* pszAttrValue = pParams->queryProp("attrValue");
  
  if (!pszCompName)
    pszCompName = pParams->queryProp("pcName");

  if (!pszBldSet)
    pszBldSet = pParams->queryProp("pcType");

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  StringBuffer xpath;
  const char* pszValue;
  StringBuffer sbMultiple;

  if (pszQueryType && !strcmp(pszQueryType, "multiple"))
  {
    StringBuffer excludes;
    //prepare exclude list
    if (pszExcludePath && *pszExcludePath && pszExcludeAttr && *pszExcludeAttr)
    {
      if (pszCategory)
        xpath.clear().append(pszCategory).append("/");

      xpath.append(pszExcludePath);
      Owned<IPropertyTreeIterator> pExcludes = pEnvRoot->getElements(xpath.str());
      ForEach(*pExcludes)
      {
        IPropertyTree* pExclude = &pExcludes->query();
        excludes.append(":").append(pExclude->queryProp(pszExcludeAttr)).append(":");
      }
    }

    xpath.clear().append(pszCategory).append("/*");
    
    if (pszBldSet)
      xpath.appendf("[@buildSet='%s']", pszBldSet);

    if (pszCompName)
      xpath.appendf("[@name='%s']", pszCompName);

    if (pszSubType)
      xpath.appendf("/%s", pszSubType);

    Owned<IPropertyTreeIterator> pComps = pEnvRoot->getElements(xpath.str());
    xpath.clear().appendf("@%s", pszAttrName);
    ForEach(*pComps)
    {
      IPropertyTree* pComp = &pComps->query();
      String excl(excludes.str());
      StringBuffer sb(":");
      sb.append(pComp->queryProp(xpath.str())).append(":");

      if (excl.indexOf(sb.str()) == -1)
        sbMultiple.append(pComp->queryProp(xpath.str())).append(",");
    }

    if (sbMultiple.length())
      sbMultiple.setLength(sbMultiple.length() - 1);
    pszValue = sbMultiple.str();
  }
  else if (pszQueryType && !strcmp(pszQueryType, "xpathType") && pszXpath && *pszXpath)
  {
    StringBuffer sb;
    sbMultiple.clear().append("");

    xpath.clear().appendf("./Software/%s[@name=\"%s\"]", pszBldSet, pszCompName);
    IPropertyTree* pNode = pEnvRoot->queryPropTree(xpath.str());

    if (!pNode)
    {
      xpath.clear().appendf("./Software/*[@buildSet=\"%s\"][@name=\"%s\"]", pszBldSet, pszCompName);
      pNode = pEnvRoot->queryPropTree(xpath.str());
    }
    if (pNode)
    {
      substituteParameters(pEnvRoot, pszXpath, pNode, sb);
      const char* szPath = sb.str();

      const char* buildSet = NULL;
      if (!strncmp(szPath, "$process", 8))
      {
        if (!pNode)
          return false;

        szPath += strlen("$process");
        if (*szPath == '\0')
        {
          const char* szName = pNode->queryProp("@name");
          if (szName && *szName)
            sbMultiple.append(",").append(szName);
          pszValue = sbMultiple.str();
          return true;
        }
        szPath++; //skip over '/'
      }
      else
      {
        if (pNode && !strcmp(szPath, "Programs/Build"))
          buildSet = pNode->queryProp("@buildSet");

        if (!pNode) 
          pNode = pEnvRoot;
      }

      String str(szPath);
      Owned<IPropertyTreeIterator> iter; 
      if (str.startsWith(XML_TAG_SOFTWARE) || 
        str.startsWith(XML_TAG_HARDWARE) || 
        str.startsWith(XML_TAG_PROGRAMS))
        iter.setown(pEnvRoot->getElements(szPath));
      else
        iter.setown(pNode->getElements(szPath));

      ForEach(*iter)
      {
        IPropertyTree* pChildNode = &iter->query();
        const char* szName = pChildNode->queryProp(XML_ATTR_NAME);

        if (szName)
        {
          bool bAdd;
          if (buildSet)
          {
            StringBuffer xpath;
            xpath.appendf("BuildSet[@name='%s']", buildSet);
            bAdd = pChildNode->queryPropTree(xpath.str()) != NULL;
          }
          else
            bAdd = true;

          if (bAdd) 
            sbMultiple.append(",").append(szName);
        }
      }
    }

    pszValue = sbMultiple.str();
  }
  else if(pszQueryType && !strcmp(pszQueryType, "customType"))
  {
    sbMultiple.clear();
    if(pszparams && *pszparams)
    {
      StringArray sArray;
      sArray.appendList(pszparams, ",");
      for(unsigned i = 0; i < sArray.ordinality() ; i++)
      {
        if(!strcmp(sArray.item(i), "checklock"))
        {
          StringBuffer sbName, sbUserIp, msg;
          sbName.clear().append(req.getReqInfo().getUserId());
          context.getPeer(sbUserIp);
          if(m_userWithLock.length())
          {
           if (strcmp(sbName.str(), m_userWithLock.str()) || strcmp(sbUserIp.str(), m_userIp.str()))
             sbMultiple.append(sArray.item(i)).append("=").append("Cannot get access to Wizard mode as Environment is currently being configured in wizard mode on machine ").append(m_userIp.str());
           else if(!strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(),m_userIp.str()))
             sbMultiple.append(sArray.item(i)).append("=").append("Another browser window already has write access on machine ").append(m_userIp.str()).append(".Please use that window.");
           else
            sbMultiple.append(sArray.item(i)).append("=''");

           if(sbMultiple.length())
             sbMultiple.append(",");
          }
        }
        else if(!strcmp(sArray.item(i), "lastsaved"))
        {
          StringBuffer lastSaved;
          m_lastSaved.getString(lastSaved);
          if(lastSaved.length())
            sbMultiple.append(sArray.item(i)).append("=").append(lastSaved);
          else
            sbMultiple.append(sArray.item(i)).append("=''");

          if(sbMultiple.length())
               sbMultiple.append(",");
        }
      }
    }
    
    pszValue = sbMultiple.str();
  }
  else if(pszQueryType && !strcmp(pszQueryType, "DomainsAndComputerTypes"))
  {
    xpath.clear().append(XML_TAG_HARDWARE"/"XML_TAG_DOMAIN);
    sbMultiple.append("<Domains>");
    bool flag = false;
    Owned<IPropertyTreeIterator> pDomains = pEnvRoot->getElements(xpath.str());
    ForEach(*pDomains)
    {
      IPropertyTree* pDomain = &pDomains->query();
      sbMultiple.append(pDomain->queryProp(XML_ATTR_NAME)).append(",");
      flag = true;
    }

    if (flag)
      sbMultiple.setLength(sbMultiple.length() - 1);
    sbMultiple.append("</Domains>");

    flag = false;

    xpath.clear().append(XML_TAG_HARDWARE"/"XML_TAG_COMPUTERTYPE);
    sbMultiple.append("<ComputerTypes>");
    Owned<IPropertyTreeIterator> pCTypes = pEnvRoot->getElements(xpath.str());
    ForEach(*pCTypes)
    {
      IPropertyTree* pCType = &pCTypes->query();
      sbMultiple.append(pCType->queryProp(XML_ATTR_NAME)).append(",");
      flag = true;
    }

    if (flag)
      sbMultiple.setLength(sbMultiple.length() - 1);
    sbMultiple.append("</ComputerTypes>");

    pszValue = sbMultiple.str();
  }
  else
  {
    if (pszCategory)
        xpath.append(pszCategory);

    if (pszBldSet)
        xpath.append("/").append(pszBldSet);

    if (pszCompName)
        xpath.appendf("["XML_ATTR_NAME"='%s']", pszCompName);

    if (pszSubType)
        xpath.append("/").append(pszSubType);

    if (pszSubTypeName)
        xpath.appendf("["XML_ATTR_NAME"='%s']", pszSubTypeName);

    if (pszAttrName)
        xpath.appendf("/@%s", pszAttrName);

    pszValue = pEnvRoot->queryProp(xpath.str());
  }

  resp.setReqValue(pszValue);
  resp.setStatus("true");
  return true;
}

bool CWsDeployFileInfo::unlockUser(IEspContext &context, IEspUnlockUserRequest &req, IEspUnlockUserResponse &resp)
{
  synchronized block(m_mutex);
  resp.setStatus("false");
  StringBuffer sbName, sbUserIp;
  sbName.clear().append(req.getReqInfo().getUserId());
  context.getPeer(sbUserIp);

  if (!strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(), m_userIp.str()))
  {
    m_userWithLock.clear();
    m_userIp.clear();
    
    resp.setStatus("true");
  }
  else
    return false;
  
  return true;
}

bool CWsDeployFileInfo::clientAlive(IEspContext &context, IEspClientAliveRequest &req, IEspClientAliveResponse &resp)
{
  StringBuffer sbName, sbUserIp;
  sbName.clear().append(req.getReqInfo().getUserId());
  context.getPeer(sbUserIp);

  StringBuffer sb(sbName);
  sb.append(sbUserIp);

  if (getConfigChanged() == true)
  {
    updateConfigFromFile();
    setConfigChanged(false);
  }

  if (!strcmp(sbName.str(), m_userWithLock.str()) && !strcmp(sbUserIp.str(), m_userIp.str()))
  {
    CClientAliveThread* pClientAliveThread = m_keepAliveHTable.getValue(sb.str());
    if (pClientAliveThread)
      pClientAliveThread->signal();
  }

  m_lastSaved.getString(sb.clear());
  resp.setLastSaved(sb.str());
  m_pService->getLastStarted(sb.clear());
  resp.setLastStarted(sb.str());
  
  return true;
}

bool CWsDeployFileInfo::getEnvironment(IEspContext &context, IEspGetEnvironmentRequest &req, IEspGetEnvironmentResponse &resp)
{
  synchronized block(m_mutex);

  StringBuffer sb;
  if (m_pFileIO.get())
  {
    Owned <IPropertyTree> pTree = createPTree(*m_pFileIO);
    toXML(pTree, sb);
    resp.setEnvXml(sb.str());
  }
  
  return true;
}

bool CWsDeployFileInfo::setEnvironment(IEspContext &context, IEspSetEnvironmentRequest &req, IEspSetEnvironmentResponse &resp)
{
  synchronized block(m_mutex);
  const char* pszEnv = req.getEnvXml();

  if (!pszEnv || !*pszEnv)
  {
    resp.setReturnCode(1);
    resp.setErrorMsg("Input xml cannot be empty");
    return false;
  }

  try
  {
    StringBuffer sbBackup;
    setEnvironment(context, NULL, pszEnv, "SetEnvironment", sbBackup);
  }
  catch(IException* e)
  {
    resp.setReturnCode(2);
    StringBuffer sb;
    e->errorMessage(sb);
    resp.setErrorMsg(sb.str());
    throw e;
  }

  resp.setReturnCode(0);
 
  return true;
}

void CWsDeployFileInfo::setEnvironment(IEspContext &context, IConstWsDeployReqInfo *reqInfo, const char* pszEnv, const char* fnName, StringBuffer& sbBackupName, bool validate, bool updateDali)
{
  if (!pszEnv)
    return;
  
  try
  {
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment>    constEnv = factory->loadLocalEnvironment(pszEnv);

    Owned<IPropertyTree> pEnvRoot = createPTreeFromXMLString(pszEnv);
    Owned<IPropertyTreeIterator> dalis = pEnvRoot->getElements("Software/DaliServerProcess/Instance");

    if (validate)
      validateEnv(constEnv);

    if (updateDali && dalis && dalis->first())
      updateDaliEnv(pEnvRoot);

    //save and write to backup
    StringBuffer sXML;
    StringBuffer tmp;
    StringBuffer dtStr;
    CDateTime dt;
    try
    {
      if (m_pFile.get())
      {
        StringBuffer sb;
        if (!checkDirExists(m_pService->getBackupDir()))
          recursiveCreateDirectory(m_pService->getBackupDir());

        while(true)
        {
          String strEnvFile(m_envFile);
          int idx = strEnvFile.lastIndexOf('/');
          if (idx <= 0)
            idx = strEnvFile.lastIndexOf('\\');
          
          String* tmpstr = strEnvFile.substring(idx+1);
          sb.clear().append(m_pService->getBackupDir()).append(PATHSEPCHAR).append(*tmpstr);
          delete tmpstr;
          dt.setNow();
          tmp.clear();
          dtStr.clear();
          dt.getDateString(tmp, true);
          tmp.append("_");
          dt.getTimeString(tmp, true);
          dtStr.append(".").append(tmp);
          dtStr.replaceString("-","_");
          dtStr.replaceString(":","_");
          
          String ext(sb);
          idx = ext.lastIndexOf(PATHSEPCHAR);
          if(ext.indexOf('.', idx > 0 ? idx : 0) != -1)
            sb.insert(ext.lastIndexOf('.'), dtStr.str());
          else
            sb.append(dtStr.str());

          if (checkFileExists(sb))
            continue;
          else
          {
            Owned<IFile> pFile(createIFile(sb.str()));
            copyFile(pFile, m_pFile, 0x100000);
            sbBackupName.clear().append(sb);
            break;
          }
        }
      }
    }
    catch(IException* e)
    {
      //ignore any attempts to create the backup
      e->Release();
    }

    if (!m_pFile.get())
      m_pFile.setown(createIFile(m_envFile));

    m_pFileIO.clear();
    m_pFileIO.setown(m_pFile->open(IFOcreaterw));
    
    dt.setNow();
    dt.getString(tmp.clear());
    StringBuffer sbUserWithLock, sbUserIp;
    if (reqInfo)
      sbUserWithLock.clear().append(reqInfo->getUserId());
    context.getPeer(sbUserIp);
    sXML.appendf("<"XML_HEADER">\n<!-- Set via %s call by %s from ip %s on %s -->\n", fnName, sbUserWithLock.str(), sbUserIp.str(), tmp.str()); 
    toXML(pEnvRoot, sXML, 0, XML_SortTags | XML_Format);
    m_pFileIO->write(0, sXML.length(), sXML.str());
    m_lastSaved.clear();
    m_lastSaved.setNow();
    m_lastSaved.getString(tmp.clear());

    initFileInfo(false);
    m_pNavTree.setown(getEnvTree(context, reqInfo));

    //reset the readonly tree
    m_constEnvRdOnly.setown(factory->loadLocalEnvironment(sXML.str()));
  }

  catch(IException* e)
  {
    throw e;
  }
}

bool CWsDeployFileInfo::displaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp)
{
  synchronized block(m_mutex);
  if (m_pNavTree.get() == NULL)
  {
    resp.setComponent("Refresh");
    resp.setXmlArgs("<Refresh flag=\"true\"/>");
    return false;
  }

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();
  StringBuffer sbDefn, sbViewChildNodes, sbMultiRowNodes;

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  Owned<IPropertyTree> pSettings = createPTree("Settings");
  Owned<IPropertyTree> pParamTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  IPropertyTree* pSoftwareFolder = pParamTree->queryPropTree("Component[@parent='Software']");
  IPropertyTree* pBuildFolder = pParamTree->queryPropTree("Component[@name='Programs']");
  IPropertyTree* pHardwareFolder = pParamTree->queryPropTree("Component[@name='Hardware']");
  IPropertyTree* pBuildSet = pParamTree->queryPropTree(XML_TAG_BUILDSET);
  IPropertyTree* pEnvSettings = pParamTree->queryPropTree("Component[@name='EnvSettings']");
  IPropertyTree* pEnvironment = pParamTree->queryPropTree("Component[@name='Environment']");

  if (pSoftwareFolder)
  {
    IPropertyTree* pEnvSoftware = pEnvRoot->queryPropTree(XML_TAG_SOFTWARE);
    Owned<IPropertyTreeIterator> iter = pParamTree->getElements("Component[@parent='Software']", iptiter_sort);
    ForEach (*iter)
    {
      IPropertyTree* pFolder = &iter->query();

      const char* pszCompType = pFolder->queryProp("@compType");
      const char* pszCompName = pFolder->queryProp(XML_ATTR_NAME);

      StringBuffer xpath;
      if (!strcmp(pszCompName, "Directories"))
        xpath.append(pszCompName);
      else
        xpath.appendf("%s[@name='%s']", pszCompType, pszCompName);

      IPropertyTree* pComp = pEnvSoftware->queryPropTree(xpath.str());
      if (!pComp)
        continue;

      if (!strcmp(pszCompType, XML_TAG_ESPSERVICE) || !strcmp(pszCompType, XML_TAG_PLUGINPROCESS))
        xpath.clear().appendf("./Programs/Build/BuildSet[@name=\"%s\"]", pComp->queryProp(XML_ATTR_BUILDSET));
      else
        xpath.clear().appendf("./Programs/Build/BuildSet[@processName=\"%s\"]", pszCompType);

      Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
      buildSetIter->first();
      IPropertyTree* pBuildSet = NULL;
      if (buildSetIter->isValid())
        pBuildSet = &buildSetIter->query();
      else if (!strcmp(pszCompName, "Directories"))
      {      
        pBuildSet = createPTree(XML_TAG_BUILDSET);
        pBuildSet->addProp(XML_ATTR_NAME, pszCompName);
        pBuildSet->addProp(XML_ATTR_SCHEMA, "directories.xsd");
        pBuildSet->addProp(XML_ATTR_PROCESS_NAME, "Directories");
      }

      if (!pBuildSet)
        throw MakeStringException(-1, "Cannot determine buildset for component in environment: '%s' named '%s'.", pszCompType, pszCompName);

      const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
      const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

      if ( m_pService->m_configHelper.isInBuildSet(pszCompType,buildSetName) == false )
      {
        throw MakeStringException(-1, "Component '%s' named '%s' not in build set. Component may be incompatible with the current version.", pszCompType, pszCompName);
      }

      StringBuffer buildSetPath;
      Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);

      if (!strcmp(processName, XML_TAG_ESPSERVICE) || !strcmp(processName, XML_TAG_PLUGINPROCESS))
        getDefnString(pEnvRoot, pSchema, pComp->queryProp(XML_ATTR_BUILDSET), sbDefn, sbViewChildNodes, sbMultiRowNodes);
      else if (!strcmp(processName, XML_TAG_TOPOLOGY))
        generateHeaderForTopology(pEnvRoot, sbDefn);
      else
        getDefnString(pEnvRoot, pSchema, processName, sbDefn, sbViewChildNodes, sbMultiRowNodes);

      resp.setCompDefn(sbDefn.str());
      resp.setViewChildNodes(sbViewChildNodes.str());
      resp.setMultiRowNodes(sbMultiRowNodes.str());
      
      StringBuffer xml;
      toXML(pComp, xml, false);
      xml.replaceString("\\","\\\\");

      //add any missing parameters
      Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xml);
      Owned<IPropertyTree> pNewCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName(), true, false, 0, true);

      if (pNewCompTree)
      {
        StringBuffer sbxml;
        toXML(pNewCompTree, sbxml);
        Owned<IAttributeIterator> iAttr = pNewCompTree->getAttributes();
        ForEach(*iAttr)
        {
          const char* attrName = iAttr->queryName();

          if (!pSrcTree->hasProp(attrName))
          {
            pSrcTree->addProp(attrName, iAttr->queryValue());

            const char* prop = "@_notInEnv";
            StringBuffer sbVal;

            if (pSrcTree->hasProp(prop))
              sbVal.append(pSrcTree->queryProp(prop));

            pSrcTree->setProp(prop, sbVal.append(";").append(attrName).append(";").str());

          }
        }

          //Add subelements that occur only once
        Owned<IPropertyTreeIterator> iterElems = pNewCompTree->getElements("*");
        ForEach (*iterElems)
        {
          IPropertyTree* pElem = &iterElems->query();

          Owned<IPropertyTreeIterator> srcElems = pSrcTree->getElements(pElem->queryName());
          IPropertyTree* pSrcElem = NULL;
          ForEach(*srcElems)
          {
            pSrcElem = &srcElems->query();
            Owned<IAttributeIterator> iAttrElem = pElem->getAttributes();
            ForEach(*iAttrElem)
            {
              const char* attrName = iAttrElem->queryName();

              if (!pSrcElem->hasProp(attrName))
              {
                pSrcElem->addProp(attrName, iAttrElem->queryValue());

                const char* prop = "@_notInEnv";
                StringBuffer sbVal;

                if (pSrcElem->hasProp(prop))
                  sbVal.append(pSrcElem->queryProp(prop));

                pSrcElem->setProp(prop, sbVal.append(";").append(attrName).append(";").str());
              }
            }

            Owned<IPropertyTreeIterator> iterSubElems = pElem->getElements("*");
            ForEach (*iterSubElems)
            {
              IPropertyTree* pSubElem = &iterSubElems->query();

              Owned<IPropertyTreeIterator> srcSubElems = pSrcElem->getElements(pSubElem->queryName());
              IPropertyTree* pSrcSubElem = NULL;
              ForEach(*srcSubElems)
              {
                pSrcSubElem = &srcSubElems->query();

                Owned<IAttributeIterator> iAttrElem = pSubElem->getAttributes();
                ForEach(*iAttrElem)
                {
                  const char* attrName = iAttrElem->queryName();

                  if (!pSrcSubElem->hasProp(attrName))
                    pSrcSubElem->addProp(attrName, iAttrElem->queryValue());
                }
              }

              if (!pSrcSubElem)
                pSrcSubElem = pSrcElem->addPropTree(pSubElem->queryName(), createPTreeFromIPT(pSubElem));
            }
          }
        }

        Owned<IPropertyTreeIterator> iterNotes = pSrcTree->getElements("Notes");

        ForEach (*iterNotes)
        {
          IPropertyTree* pNotes = &iterNotes->query();
          Owned<IPropertyTreeIterator> iterNote = pNotes->getElements("Note");

          ForEach (*iterNote)
          {
            IPropertyTree* pNote = &iterNote->query();

            StringBuffer sbVal(pNotes->queryProp(pNote->queryName()));
            sbVal.replaceString("\r\n", "&#13;&#10;");
            pNotes->setProp(pNote->queryName(), sbVal.str());
          }
        }

          xml.clear();
          toXML(pSrcTree, xml, false);
      }

      if (!strcmp(pszCompType, "Directories"))
      {
        Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xml);
        Owned<IPropertyTreeIterator> iterCats = pSrcTree->getElements("Category");

        ForEach (*iterCats)
        {
          IPropertyTree* pCat = &iterCats->query();
          const char* pszName = pCat->queryProp(XML_ATTR_NAME);
          
          if (!strcmp(pszName, "lock") || !strcmp(pszName, "run") || !strcmp(pszName, "conf"))
          {
            pSrcTree->removeTree(pCat);
            (*iterCats).first();
          }
          else
          {
            IPropertyTree* pOver = pCat->queryPropTree("Override[1]");
            if (!pOver)
              pOver = pCat->addPropTree("Override", createPTree());
            if (!pOver->queryProp("@component"))
              pOver->addProp("@component", "");
            if (!pOver->queryProp("@dir"))
              pOver->addProp("@dir", "");
            if (!pOver->queryProp("@instance"))
              pOver->addProp("@instance", "");
          }
        }

        xml.clear();
        toXML(pSrcTree, xml, false);
      }
      else if (!strcmp(pszCompType, XML_TAG_THORCLUSTER))
      {
        Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xml);
        IPropertyTree* pTopoNode = pSrcTree->queryPropTree(XML_TAG_TOPOLOGY);

        if (!pTopoNode)
        {
          pTopoNode = pSrcTree->addPropTree(XML_TAG_TOPOLOGY, createPTree());
          IPropertyTree* pMaster = pSrcTree->queryPropTree(XML_TAG_THORMASTERPROCESS);

          if (pMaster)
          {
            IPropertyTree* pMasterNode = createPTree(XML_TAG_NODE);
            pMasterNode->addProp(XML_ATTR_PROCESS, pMaster->queryProp(XML_ATTR_NAME));
            pTopoNode->addPropTree(XML_TAG_NODE, pMasterNode);

            Owned<IPropertyTreeIterator> iterSlaves = pSrcTree->getElements(XML_TAG_THORSLAVEPROCESS);

            ForEach (*iterSlaves)
            {
              IPropertyTree* pSlave = &iterSlaves->query();
              IPropertyTree* pNode = createPTree(XML_TAG_NODE);
              pNode->addProp(XML_ATTR_PROCESS, pSlave->queryProp(XML_ATTR_NAME));
              pMasterNode->addPropTree(XML_TAG_NODE, pNode);
            }
          }

          Owned<IPropertyTreeIterator> iterSpares = pSrcTree->getElements(XML_TAG_THORSPAREPROCESS);

          ForEach (*iterSpares)
          {
            IPropertyTree* pSpare = &iterSpares->query();
            IPropertyTree* pNode = createPTree(XML_TAG_NODE);
            pNode->addProp(XML_ATTR_PROCESS, pSpare->queryProp(XML_ATTR_NAME));
            pTopoNode->addPropTree(XML_TAG_NODE, pNode);
          }
        }

        xpath.clear().append("Topology/Node");
        Owned<IPropertyTreeIterator> iterMNodes = pSrcTree->getElements(xpath.str());

        ForEach (*iterMNodes)
        {
          IPropertyTree* pMasterNode = &iterMNodes->query();
          const char* pszMName = pMasterNode->queryProp(XML_ATTR_PROCESS);
          pMasterNode->addProp("@_processId", pszMName);

          if (pszMName)
          {
            xpath.clear().appendf(XML_TAG_THORMASTERPROCESS"/[@name='%s']", pszMName);
            IPropertyTree* pMasterNodeTree = pComp->queryPropTree(xpath);

            //if not master, then spare
            if (!pMasterNodeTree)
            {
              xpath.clear().appendf(XML_TAG_THORSPAREPROCESS"/[@name='%s']", pszMName);
              IPropertyTree* pSpareNodeTree = pComp->queryPropTree(xpath);            
              const char* pszComputer = pSpareNodeTree->queryProp(XML_ATTR_COMPUTER);

              if (pszComputer)
              {
                xpath.clear().appendf("Hardware/Computer/[@name='%s']", pszComputer);
                IPropertyTree* pComputer= pEnvRoot->queryPropTree(xpath.str());
                const char* pszNetAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                if (pszNetAddr)
                  pMasterNode->addProp(XML_ATTR_NETADDRESS, pszNetAddr);

                pMasterNode->addProp(XML_ATTR_NAME, pszComputer);
                pMasterNode->addProp(XML_ATTR_PROCESS, "Spare");
              }
            }
            else
            {
              const char* pszComputer = pMasterNodeTree->queryProp(XML_ATTR_COMPUTER);

              if (pszComputer)
              {
                xpath.clear().appendf("Hardware/Computer/[@name='%s']", pszComputer);
                IPropertyTree* pComputer= pEnvRoot->queryPropTree(xpath.str());

                if (pComputer == NULL)
                  throw MakeStringException(-1, "XPATH: %s is invalid. (Did you add the Hardware?)",xpath.str());

                const char* pszNetAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                if (pszNetAddr)
                  pMasterNode->addProp(XML_ATTR_NETADDRESS, pszNetAddr);

                pMasterNode->addProp(XML_ATTR_NAME, pszComputer);
                pMasterNode->addProp(XML_ATTR_PROCESS, "Master");
              }

              xpath.clear().appendf("Node");
              Owned<IPropertyTreeIterator> iterSNodes = pMasterNode->getElements(xpath);

              ForEach (*iterSNodes)
              {
                IPropertyTree* pSlaveNode = &iterSNodes->query();
                const char* pszSName = pSlaveNode->queryProp(XML_ATTR_PROCESS);
                pSlaveNode->addProp("@_processId", pszSName);

                if (pszMName)
                {
                  xpath.clear().appendf("ThorSlaveProcess/[@name='%s']", pszSName);
                  IPropertyTree* pSlaveNodeTree = pSrcTree->queryPropTree(xpath);
                  const char* pszComputer = pSlaveNodeTree->queryProp(XML_ATTR_COMPUTER);

                  if (pszComputer)
                  {
                    xpath.clear().appendf("Hardware/Computer/[@name='%s']", pszComputer);
                    IPropertyTree* pComputer= pEnvRoot->queryPropTree(xpath.str());
                    const char* pszNetAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
                    if (pszNetAddr)
                      pSlaveNode->addProp(XML_ATTR_NETADDRESS, pszNetAddr);

                    pSlaveNode->addProp(XML_ATTR_NAME, pszComputer);
                    pSlaveNode->addProp(XML_ATTR_PROCESS, "Slave");
                  }
                }
              }
            }
          }
        }

        xml.clear();
        toXML(pSrcTree, xml, false);
      }
      else if (!strcmp(pszCompType, "RoxieCluster"))
      {
        Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xml);
        pNewCompTree.setown(generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName()));

        if (!strcmp(pszCompType, "RoxieCluster"))
        {
          xpath.clear().append("RoxieFarmProcess/RoxieServerProcess");
          Owned<IPropertyTreeIterator> iterRoxieServers = pSrcTree->getElements(xpath.str());

          ForEach (*iterRoxieServers )
          {
            IPropertyTree* pRoxieServer = &iterRoxieServers->query();
            const char* pszComputer = pRoxieServer->queryProp(XML_ATTR_COMPUTER);

            if (pszComputer)
            {
              xpath.clear().appendf("Hardware/Computer/[@name='%s']", pszComputer);
              IPropertyTree* pComputer= pEnvRoot->queryPropTree(xpath.str());
              const char* pszNetAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
              if (pszNetAddr)
                pRoxieServer->addProp(XML_ATTR_NETADDRESS, pszNetAddr);
            }
          }

          xpath.clear().append(XML_TAG_ROXIE_ONLY_SLAVE);
          Owned<IPropertyTreeIterator> iterSlaves = pSrcTree->getElements(xpath.str());

          ForEach (*iterSlaves)
          {
            IPropertyTree* pRoxieSlave = &iterSlaves->query();
            const char* pszComputer = pRoxieSlave->queryProp(XML_ATTR_COMPUTER);

            if (pszComputer)
            {
              xpath.clear().appendf("Hardware/Computer/[@name='%s']", pszComputer);
              IPropertyTree* pComputer= pEnvRoot->queryPropTree(xpath.str());
              const char* pszNetAddr = pComputer->queryProp(XML_ATTR_NETADDRESS);
              if (pszNetAddr)
                pRoxieSlave->addProp(XML_ATTR_NETADDRESS, pszNetAddr);
            }
          }
        }

        if (pSrcTree->hasProp("ACL"))
        {
          Owned<IPropertyTreeIterator> iterACL = pSrcTree->getElements("ACL");
          ForEach (*iterACL)
          {
            IPropertyTree* pAcl = &iterACL->query();

            if (!pAcl->queryPropTree("Access") && pNewCompTree->queryPropTree("Access"))
            {
              IPropertyTree* pAccess = pAcl->addPropTree("Access", createPTreeFromIPT(pNewCompTree->queryPropTree("Access")));

              Owned<IAttributeIterator> iAttrElem = pNewCompTree->queryPropTree("Access")->getAttributes();
                    ForEach(*iAttrElem)
                    {
                        const char* attrName = iAttrElem->queryName();
                        pAccess->setProp(attrName, "");
                    }
            }

            if (!pAcl->queryPropTree("BaseList") && pNewCompTree->queryPropTree("BaseList"))
            {
              IPropertyTree* pBaseList = pAcl->addPropTree("BaseList", createPTreeFromIPT(pNewCompTree->queryPropTree("BaseList")));
              Owned<IAttributeIterator> iAttrElem = pNewCompTree->queryPropTree("BaseList")->getAttributes();
                    ForEach(*iAttrElem)
                    {
                        const char* attrName = iAttrElem->queryName();
                        pBaseList->setProp(attrName, "");
                    }
            }
          }
        }

        xml.clear();
        toXML(pSrcTree, xml, false);
        xml.replaceString("<RoxieFarmProcess ", "<RoxieFarmProcess process=\"Farm\" ");
        xml.replaceString("<RoxieServerProcess ", "<RoxieServerProcess process=\"Server\" ");
        xml.replaceString("<RoxieSlave ", "<RoxieSlave itemType=\"Roxie Agent\" ");
        xml.replaceString("<RoxieChannel ", "<RoxieChannel itemType=\"Channel\" ");
      }
      else if (!strcmp(pszCompType, XML_TAG_ESPPROCESS))
      {
        Owned<IPropertyTree> pTree = createPTreeFromXMLString(xml);
        
        Owned<IPropertyTreeIterator> iterBindings = pTree->getElements(XML_TAG_ESPBINDING);
        ForEach (*iterBindings)
        {
          IPropertyTree* pBinding = &iterBindings->query();
          bool flag = false;

          Owned<IPropertyTreeIterator> iterUrl = pBinding->getElements("AuthenticateFeature");
          ForEach (*iterUrl)
          {
            flag = true;
            break;
          }

          if (!flag)
          {
            IPropertyTree* pAuthFeature = pBinding->addPropTree( "AuthenticateFeature", createPTree() );
            pAuthFeature->addProp("@authenticate", "");
            pAuthFeature->addProp("@description", "");
            pAuthFeature->addProp("@path", "");
            pAuthFeature->addProp("@resource", "");
            pAuthFeature->addProp("@service", "");
          }

          flag = false;
          Owned<IPropertyTreeIterator> iterFeature = pBinding->getElements("Authenticate");
          ForEach (*iterFeature)
          {
            flag = true;
            break;
          }

          if (!flag)
          {
            IPropertyTree* pAuth = pBinding->addPropTree( "Authenticate", createPTree() );
            pAuth->addProp("@access", "");
            pAuth->addProp("@description", "");
            pAuth->addProp("@method", "");
            pAuth->addProp("@path", "");
            pAuth->addProp("@resource", "");
          }
        }

        Owned<IPropertyTreeIterator> iterInst = pTree->getElements(XML_TAG_INSTANCE);
        
        ForEach (*iterInst)
        {
          IPropertyTree* pInst = &iterInst->query();
          Owned<IPropertyTreeIterator> iterNode = pInst->getElements("*");
        
          if (iterNode->first() && iterNode->isValid())
          {
            ForEach (*iterNode)
            {
              IPropertyTree* pNode = &iterNode->query();
            
              if (!strcmp(pNode->queryName(), "CSR") || !strcmp(pNode->queryName(), "Certificate") || !strcmp(pNode->queryName(), "PrivateKey"))
              {
                StringBuffer sbVal(pInst->queryProp(pNode->queryName()));
                sbVal.replaceString("\r\n", "&#13;&#10;");
                pInst->setProp(pNode->queryName(), sbVal.str());
              }
            }
          }
          
          if (!pInst->hasProp("CSR"))
            pInst->addPropTree("CSR", createPTree());

          if (!pInst->hasProp("Certificate"))
            pInst->addPropTree("Certificate", createPTree());

          if (!pInst->hasProp("PrivateKey"))
            pInst->addPropTree("PrivateKey", createPTree());
        }
      
        xml.clear();
        toXML(pTree, xml, false);
      }

      const char* pszBldSet = pComp->queryProp(XML_ATTR_BUILDSET);

      if (strcmp(pszCompName, "Directories") && !pszBldSet)
        throw MakeStringException(-1, "Cannot determine buildset for component in environment: '%s' named '%s'.", pszCompType, pszCompName);

      if (!strcmp(pszCompType, XML_TAG_ESPSERVICE) || !strcmp(pszCompType, XML_TAG_PLUGINPROCESS))
        resp.setComponent(pszBldSet);
      else
        resp.setComponent(pszCompType);

      resp.setXmlArgs( xml.str() );
    }
  }
  else if (pHardwareFolder)
  {
    IPropertyTree* pEnvHardware = pEnvRoot->queryPropTree(XML_TAG_HARDWARE);
    
    generateHardwareHeaders(pEnvRoot, sbDefn);
    resp.setCompDefn(sbDefn.str());
    StringBuffer sb;
    Owned<IPropertyTree> multiRowNodes = createPTree("multiRowNodes");
    multiRowNodes->addProp("Node", "ComputerType");
    multiRowNodes->addProp("Node", "Domain");
    multiRowNodes->addProp("Node", "Computer");
    multiRowNodes->addProp("Node", "Switch");
    toXML(multiRowNodes, sb);
    resp.setMultiRowNodes(sb.str());
    
    StringBuffer xml;
    toXML(pEnvHardware, xml, false);
    xml.replaceString("switch=","Switch=");

    //add any missing parameters
    Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xml);
    StringBuffer sbTemp;
    Owned<IPropertyTree> pNewCompTree = createPTree(XML_TAG_HARDWARE);
    generateHardwareHeaders(pEnvRoot, sbTemp, false, pNewCompTree);
    const char* pElemNames[] = {XML_TAG_COMPUTER};
    bool modified = false;

    for (int i = 0; i < sizeof(pElemNames)/sizeof(char*); i++)
    {
      IPropertyTree* pSrcElem = pNewCompTree->queryPropTree(pElemNames[i]);
      Owned<IPropertyTreeIterator> iter = pSrcTree->getElements(pElemNames[i]);
      ForEach (*iter)
      {
        IPropertyTree* pElem = &iter->query();

        Owned<IAttributeIterator> iAttr = pSrcElem->getAttributes();
        ForEach(*iAttr)
        {
          const char* attrName = iAttr->queryName();

          if (strcmp(attrName, "@state") && strcmp(attrName, "@switch") &&!pElem->hasProp(attrName))
          {
            pElem->addProp(attrName, iAttr->queryValue());
            modified = true;
          }
        }
      }
    }

    if (modified)
    {
      xml.clear();
      toXML(pSrcTree, xml, false);
    }

    resp.setComponent(XML_TAG_HARDWARE);
    resp.setXmlArgs( xml.str() );
  }
  else if (pBuildFolder)
  {
    IPropertyTree* pEnvPrograms = pEnvRoot->queryPropTree(XML_TAG_PROGRAMS);

    generateBuildHeaders(pEnvRoot, true, sbDefn, false);
    resp.setCompDefn(sbDefn.str());
    
    StringBuffer xml;
    toXML(pEnvPrograms, xml, false);
    xml.replaceString(" url="," path=");
    resp.setComponent(XML_TAG_PROGRAMS);
    resp.setXmlArgs( xml.str() );
  }
  else if (pBuildSet)
  {
    generateBuildHeaders(pEnvRoot, false, sbDefn, false);
    resp.setCompDefn(sbDefn.str());

    IPropertyTree* pEnvPrograms = pEnvRoot->queryPropTree(XML_TAG_PROGRAMS);
    Owned<IPropertyTreeIterator> iter = pParamTree->getElements(XML_TAG_BUILDSET);
    ForEach (*iter)
    {
      IPropertyTree* pFolder = &iter->query();

      const char* pszBuild = pFolder->queryProp(XML_ATTR_BUILD);
      const char* pszBuildPath = pFolder->queryProp("@buildpath");
      const char* pszBuildSet = pFolder->queryProp(XML_ATTR_NAME);
      const char* pszBuildSetPath = pFolder->queryProp("@path");

      StringBuffer xpath;
      xpath.appendf("Build[@name='%s']/BuildSet[@name='%s']", pszBuild, pszBuildSet);

      IPropertyTree* pComp = pEnvPrograms->queryPropTree(xpath.str());
      if (!pComp)
        throw MakeStringException(-1, "No such build and buildset in environment: '%s' named '%s'.", pszBuild, pszBuildSet);

      StringBuffer s;
      s.append("./Programs/Build[@name=\"").append(pszBuild).append("\"]");
      IPropertyTree *b = pEnvRoot->queryPropTree(s.str());
      IPropertyTree *bs = NULL;
      IPropertyTree* pParentNode = NULL;
      IPropertyTree* pFiles = NULL;

      if (b)
      {
        s.clear().append("BuildSet[@name=\"").append(pszBuildSet).append("\"]");
        bs = b->queryPropTree(s.str());
        StringBuffer fileName;
        connectBuildSet(b, bs, fileName, m_Environment);
        fileName.append(bs->queryProp("@installSet"));
        pParentNode = createPTreeFromXMLFile(fileName.str());
        pFiles = pParentNode->queryPropTree("File");
      }

      StringBuffer xml;
      toXML(pFiles, xml, false);
      resp.setComponent(XML_TAG_BUILDSET);
      resp.setXmlArgs( xml.str() );
    }
  }
  else if (pEnvSettings)
  {
    IPropertyTree* pEnvSettings = pEnvRoot->queryPropTree("EnvSettings");
    
    generateHeadersForEnvSettings(pEnvRoot, sbDefn);
    resp.setCompDefn(sbDefn.str());
    
    StringBuffer xml;
    toXML(pEnvSettings, xml, false);
    resp.setComponent("EnvSettings");
    resp.setXmlArgs( xml.str() );
  }
  else if (pEnvironment)
  {
    generateHeadersForEnvXmlView(pEnvRoot, sbDefn);
    resp.setCompDefn(sbDefn.str());
    
    StringBuffer xml;
    toXML(pEnvRoot, xml, false);
    resp.setComponent("Environment");
    resp.setXmlArgs( xml.str() );
  }

  return true;
}

bool CWsDeployFileInfo::startDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* selComps = req.getSelComps();
  CDeployOptions& depOptions = dynamic_cast<CDeployOptions&>(req.getOptions());
  CDeployInfo depInfo("wsDeploy", "true");

  StringBuffer deployResult;
  CWsDeployEngine depEngine(*m_pService, &context, depInfo, selComps, 2);
  depEngine.deploy(depOptions);

  Owned<IPropertyTree> pDeployResult = depEngine.getDeployResult();

  if (pDeployResult)
  {
    IPropertyTree* pComponents = pDeployResult->queryPropTree("SelectedComponents");
    toXML(pComponents, deployResult, false);
  }

  IPropertyTree* pOptions = pDeployResult->queryPropTree("Options");

  if (pOptions)
    toXML(pOptions, deployResult, false);

  resp.setStatus( depEngine.getDeployStatus() );

  if (depEngine.GetErrorCount() == 0)
    resp.setStatus(deployResult.str());

  return true;
}

bool CWsDeployFileInfo::getDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp)
{
  synchronized block(m_mutex);
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements("Software/*");
  Owned<IPropertyTree> pDeploy = createPTree("Deploy");

  ForEach(*iter)
  {
    IPropertyTree* pComponent = &iter->query();
    const char* type = pComponent->queryName();

    if (stricmp(type, "Topology")!=0)
    {
      const char* name    = pComponent->queryProp(XML_ATTR_NAME);
      const char* build   = pComponent->queryProp(XML_ATTR_BUILD);
      const char* buildSet= pComponent->queryProp(XML_ATTR_BUILDSET);

      StringBuffer sXPath;
      sXPath.appendf("Programs/Build[@name='%s']/BuildSet[@name='%s']/@deployable", build, buildSet);
      const char* szDeployable = pEnvRoot->queryProp(sXPath.str());
      bool bDeployable = !szDeployable || 
        (stricmp(szDeployable, "no")   != 0 && 
        stricmp(szDeployable, "false") != 0 &&
        strcmp(szDeployable, "0")      != 0);

      if (name && *name && build && *build && bDeployable)
      {
        IPropertyTree* pCompNode = pDeploy->addPropTree( XML_TAG_COMPONENT, createPTree() );
        pCompNode->addProp(XML_ATTR_NAME, name);
        pCompNode->addProp(XML_ATTR_BUILD,  build); 
        pCompNode->addProp(XML_ATTR_BUILDSET, type);

        Owned<IPropertyTreeIterator> iter = pComponent->getElements("*", iptiter_sort);
        ForEach(*iter)
        {
          IPropertyTree* pNode = &iter->query();
          const char* nodeName = pNode->queryName();
          const char* computer = pNode->queryProp(XML_ATTR_COMPUTER);
          const char* instanceName = pNode->queryProp(XML_ATTR_NAME);

          if (computer && *computer)
          {
            const char* instanceNodeNames[] = { XML_TAG_INSTANCE, XML_TAG_ROXIE_SERVER, XML_TAG_ROXIE_SLAVE };
            for (UINT i=0; i<sizeof(instanceNodeNames) / sizeof(instanceNodeNames[0]); i++)
              if (!strcmp(nodeName, instanceNodeNames[i]))
              {
                if (*nodeName != 'R')// || //neither RoxieServerProcess nor RoxieSlaveProcess
                {
                  IPropertyTree* pInstanceNode = pCompNode->addPropTree(XML_TAG_INSTANCES, createPTree());
                  const char* directory = pNode->queryProp(*nodeName == 'R' ? XML_ATTR_DATADIRECTORY : XML_ATTR_DIRECTORY);
                  if (directory && *directory)
                    pInstanceNode->addProp(XML_ATTR_BUILD, directory);

                  pInstanceNode->addProp("@nodeName", computer);
                  pInstanceNode->addProp(XML_ATTR_BUILDSET, XML_TAG_INSTANCE);
                  pInstanceNode->addProp("@instanceName", instanceName);
                }
              }
          }
        }
      }
    }   
  }

  StringBuffer xml, outputXml;
  toXML(pDeploy, xml, false);
  resp.setComponent( "Deploy" );
  resp.setXmlArgs( xml.str() );

  return true;
}


bool CWsDeployFileInfo::getComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();
  StringBuffer sbC, sbF;

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  getComputersListWithUsage(pEnvRoot, sbC, sbF);
  resp.setComputers(sbC.str());
  resp.setFilters( sbF.str() );

  return true;
}


bool CWsDeployFileInfo::handleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();
  StringBuffer sbC, sbF;
 
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  bool retVal = ::handleRoxieOperation(pEnvRoot, cmd, xmlArg);
  resp.setStatus(retVal? "true" : "false");

  return true;
}


bool CWsDeployFileInfo::getBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  Owned<IPropertyTree> pSettings = createPTree("Settings");
  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  IPropertyTree* pBuildSet = pSrcTree->queryPropTree(XML_TAG_BUILDSET);

  if (pBuildSet)
  {
    IPropertyTree* pEnvPrograms = pEnvRoot->queryPropTree(XML_TAG_PROGRAMS);
    Owned<IPropertyTreeIterator> iter = pSrcTree->getElements(XML_TAG_BUILDSET);

    ForEach (*iter)
    {
      IPropertyTree* pFolder = &iter->query();

      const char* pszBuild = pFolder->queryProp(XML_ATTR_BUILD);
      const char* pszBuildPath = pFolder->queryProp("@buildpath");
      const char* pszBuildSet = pFolder->queryProp(XML_ATTR_NAME);
      const char* pszBuildSetPath = pFolder->queryProp("@path");

      StringBuffer xpath;
      xpath.appendf("Build[@name='%s']/BuildSet[@name='%s']", pszBuild, pszBuildSet);

      IPropertyTree* pComp = pEnvPrograms->queryPropTree(xpath.str());
      if (!pComp)
        throw MakeStringException(-1, "No such build and buildset in environment: '%s' named '%s'.", pszBuild, pszBuildSet);

      StringBuffer s;
      s.append("./Programs/Build[@name=\"").append(pszBuild).append("\"]");
      IPropertyTree *b = pEnvRoot->queryPropTree(s.str());
      IPropertyTree *bs = NULL;
      IPropertyTree* pParentNode = NULL;
      IPropertyTree* pFiles = NULL;

      if (b)
      {
        s.clear().append("BuildSet[@name=\"").append(pszBuildSet).append("\"]");
        bs = b->queryPropTree(s.str());
        StringBuffer fileName;
        connectBuildSet(b, bs, fileName, m_Environment);
        fileName.append(bs->queryProp("@installSet"));

        pParentNode = createPTreeFromXMLFile(fileName.str());
        pFiles = pParentNode->queryPropTree("File");
      }

      StringBuffer xml;
      toXML(pParentNode, xml, false);
      resp.setComponent(XML_TAG_BUILDSET);
      resp.setXmlArgs( xml.str() );
    }
  }

  return true;
}

bool CWsDeployFileInfo::importBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getBuildSets();
  IPropertyTree* buildSets = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<BuildSets/>");
  IPropertyTree* buildNode = createPTree(XML_TAG_BUILD);
  const char* buildName = buildSets->queryProp(XML_ATTR_NAME);
  const char* buildUrl = buildSets->queryProp("@path");
  buildNode->setProp(XML_ATTR_NAME, buildName);
  buildNode->setProp(XML_ATTR_URL, buildUrl);

  Owned<IPropertyTreeIterator> iBuildSet = buildSets->getElements("*"); 

  ForEach(*iBuildSet)
  {
    IPropertyTree* pBuildSet = &iBuildSet->query();
    const char* bsName = pBuildSet->queryProp(XML_ATTR_NAME);
    const char* bsPath = pBuildSet->queryProp("@path");

    IPropertyTree* pBuildsetNode = createPTree();
    pBuildsetNode->addProp(XML_ATTR_NAME, bsName);
    pBuildsetNode->addProp(XML_ATTR_PATH, bsPath);
    pBuildsetNode->addProp(XML_ATTR_INSTALLSET, "deploy_map.xml");

    try
    {
      Owned<IPropertyTree> installSet = loadInstallSet(buildNode, pBuildsetNode, m_Environment);
      if (strcmp(installSet->queryName(), XML_TAG_INSTALLSET) == 0)
      {
        pBuildsetNode->setProp(XML_ATTR_PROCESS_NAME, installSet->queryProp(XML_ATTR_PROCESS_NAME));
        pBuildsetNode->setProp(XML_ATTR_SCHEMA, installSet->queryProp(XML_ATTR_SCHEMA));
        if (installSet->hasProp(XML_ATTR_NAME))
          pBuildsetNode->setProp(XML_ATTR_NAME, installSet->queryProp(XML_ATTR_NAME));

        const char* szDeployable = installSet->queryProp("@deployable");
        if (szDeployable)
          pBuildsetNode->setProp("@deployable", szDeployable);

        IPropertyTree* pProperties = installSet->getPropTree("Properties");
        if (pProperties)
          pBuildsetNode->addPropTree("Properties", pProperties);

        buildNode->addPropTree(XML_TAG_BUILDSET, pBuildsetNode);
      }
    }
    catch(...) 
    {
    }
  }

  Owned<IPropertyTree> pEnvRoot = &m_Environment->getPTree();
  IPropertyTree* pEnvPrograms = pEnvRoot->queryPropTree(XML_TAG_PROGRAMS);

  if (!pEnvPrograms)
    pEnvPrograms = pEnvRoot->addPropTree(XML_TAG_PROGRAMS, createPTree() );

  pEnvPrograms->addPropTree(XML_TAG_BUILD, buildNode);
  resp.setStatus("true");

  return true;
}

bool CWsDeployFileInfo::getBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* cmd   = req.getCmd();
  const char* xmlArg = req.getXmlArgs();
  StringBuffer sourceDir;

  if (xmlArg && *xmlArg)
  {
    if (xmlArg[0] != '\\' && xmlArg[1] != '\\')
      sourceDir.append("\\\\127.0.0.1\\");

    sourceDir.append(xmlArg);
  }

  if (!strcmp(cmd, "SubDirs"))
  {
    Owned<IFile> inFiles = NULL;
    IPropertyTree* pParentNode = createPTree("BuildServerDirs");

    try
    {
      inFiles.setown(createIFile(sourceDir));
      if(!inFiles->exists())
      {
        resp.setXmlArgs("Input directory %s does not exist");
        return false;
      }
    }
    catch(IException* e)
    {
      StringBuffer errmsg;
      e->errorMessage(errmsg);
      e->Release();
      resp.setXmlArgs("Error when trying to access source directory.");
      return false;
    }

    Owned<IDirectoryIterator> di = inFiles->directoryFiles(NULL, 0, true);
    CIArrayOf<CDirectoryEntry> sortedfiles;
    StringArray dispfiles;
    bool bDirChosen = false;

    sortDirectory(sortedfiles, *di, SD_bydate, true, true); 
    StringBuffer lastDirName;

    int index = 0;
    ForEachItemIn(idx, sortedfiles)
    {
      CDirectoryEntry *de = &sortedfiles.item(idx);
      if (de->isdir)
      {
        StringBuffer sb;
        de->modifiedTime.getString(sb);
        IPropertyTree* pCompNode = pParentNode->addPropTree( "Directory", createPTree() );
        pCompNode->addProp(XML_ATTR_NAME, de->name.get());
        pCompNode->addProp("@modified", sb.str());  
      }
    }

    StringBuffer xml;
    toXML(pParentNode, xml, false);
    resp.setComponent("BuildServerDirs");
    resp.setXmlArgs( xml.str() );
  }
  else 
  {
    Owned<IFile> inFiles = NULL;
    IPropertyTree* pParentNode = createPTree("BuildServerComps");

    if (!strcmp(cmd, "Release"))
      sourceDir.append(PATHSEPCHAR).append("release");
    else if (!strcmp(cmd, "Debug"))
      sourceDir.append(PATHSEPCHAR).append("debug");

    try
    {
      inFiles.setown(createIFile(sourceDir));
      if(!inFiles->exists())
      {
        resp.setXmlArgs("Input directory %s does not exist");
        return false;
      }
    }
    catch(IException* e)
    {
      StringBuffer errmsg;
      e->errorMessage(errmsg);
      e->Release();
      resp.setXmlArgs("Error when trying to access source directory.");
      return false;
    }

    if(inFiles.get() != NULL && inFiles->isDirectory())
    {
      Owned<IDirectoryIterator> di = inFiles->directoryFiles(NULL, 0, true);
      bool bCompFound = false;
      StringBuffer dirName, compName, fileName;

      if(di.get())
      {
        ForEach(*di)
        {
          IFile &file = di->query();

          if (!file.isFile())
          {
            dirName.clear();
            di->getName(dirName);

            compName.clear().append(dirName);
            Owned<IFile> dirFiles = NULL;
            StringBuffer sbPath(cmd);
            sbPath.toLowerCase();
            dirName.clear().append(sourceDir);

            if(dirName.charAt(dirName.length() - 1) != PATHSEPCHAR)
              dirName.append(PATHSEPCHAR);

            dirName.append(compName);
            sbPath.append(PATHSEPCHAR).append(compName);

            fileName.clear().append(dirName).append(PATHSEPCHAR).append("deploy_map.xml");
            Owned<IFile> depfile(createIFile(fileName));
            if(depfile->exists())
            {
              IPropertyTree* pCompNode = pParentNode->addPropTree("Comp", createPTree() );
              pCompNode->addProp(XML_ATTR_NAME, compName.str());
              pCompNode->addProp("@path", sbPath.str());
            }
          }
        }
      }

      StringBuffer xml;
      toXML(pParentNode, xml, false);
      resp.setComponent("BuildServerComps");
      resp.setXmlArgs( xml.str() );
    }
  }

  return true;
}

bool CWsDeployFileInfo::handleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp)
{
  synchronized block(m_mutex);
  const char* xmlArg = req.getXmlArgs();

  if (!xmlArg || !*xmlArg)
    return false;

  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg);
  Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@operation='add']");

  if (iter->first() == false)
    return false;

  IPropertyTree* pSetting = &iter->query();
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  StringBuffer xpath =  pSetting->queryProp(XML_ATTR_PARAMS);
  StringBuffer attribName = pSetting->queryProp(XML_ATTR_ATTRIB);

  if (attribName.length() == 0)
    throw MakeStringException(-1,"Attribute name can't be empty!");

  IPropertyTree* pComp =  pEnvRoot->getPropTree(xpath.str());

  if (pComp != NULL)
    pComp->addProp(attribName.str(), "");

  resp.setStatus("true");
  resp.setCompName(XML_TAG_SOFTWARE);

  return true;
}

bool CWsDeployFileInfo::handleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp)
{
  synchronized block(m_mutex);
  const char* xmlArg = req.getXmlArgs();

  if (!xmlArg || !*xmlArg)
    return false;

  Owned<IPropertyTree> pSrcTree = createPTreeFromXMLString(xmlArg);

  Owned<IPropertyTreeIterator> iter = pSrcTree->getElements("Setting[@operation='delete']");

  if (iter->first() == false)
    return false;

  IPropertyTree* pSetting = &iter->query();
  Owned<IPropertyTree> pEnvRoot = &m_Environment->getPTree();
  StringBuffer xpath2 =  pSetting->queryProp("@params");
  IPropertyTree* pComp = pEnvRoot->queryPropTree(xpath2.str());

  if (pComp == NULL)
    throw MakeStringException(-1,"Bad XPath %s (Try refreshing the browser?)", xpath2.str());

  StringBuffer xml;
  StringBuffer attrib;

  int count = xpath2.length()-2;

  if (count <= 0)
    throw MakeStringException(-1,"Bad XPath %s (Try refreshing the browser?)", xpath2.str());

  while (xpath2[count] != '=' || xpath2[count+1] != '\'')
    count--;
  count--;

  for (int i=count; i >= 0 && xpath2[i] != '['; i--)
    attrib.insert(0,xpath2[i]);

  int index = xpath2.length()-1;

  while (index > 0)
  {
    if (xpath2[index] == '/')
      break;
    index--;
  }

  char *pRootXPath = new char[index+2];
  memset(pRootXPath,0,sizeof(char) * (index+2));

  xpath2.getChars(0,index+1,pRootXPath);

  if (req.getBLeaf() == true)
    pComp->removeProp(attrib);
  else if (strlen(pRootXPath) >= 14 && strncmp(pRootXPath,"./EnvSettings/",14) == 0)
    pEnvRoot->queryPropTree("./EnvSettings")->removeTree(pComp);
  else
    pEnvRoot->queryPropTree(pRootXPath)->removeTree(pComp);

  delete[] pRootXPath;
  resp.setStatus("true");
  resp.setCompName(XML_TAG_SOFTWARE);

  return true;
}

bool CWsDeployFileInfo::handleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pComponents = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<Components/>");

  const char* operation = req.getOperation();
  StringBuffer sbNewName;
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  if (!strcmp(operation, "Add"))
  {
    Owned<IPropertyTreeIterator> iterComp = pComponents->getElements("*");
    ForEach(*iterComp)
    {
      IPropertyTree& pComp = iterComp->query();
      const char* buildSet = pComp.queryProp(XML_ATTR_BUILDSET);

      StringBuffer xpath;
      xpath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]", buildSet);
      Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
      buildSetIter->first();
      IPropertyTree* pBuildSet = &buildSetIter->query();
      const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
      const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

      StringBuffer buildSetPath;
      Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
      xpath.clear().appendf("./Software/%s[@name='%s']", processName, buildSetName);
      IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName(), false);
      IPropertyTree* pInstTree = pCompTree->queryPropTree(XML_TAG_INSTANCE);

      if (pInstTree)
        pCompTree->removeTree(pInstTree);

      addComponentToEnv(pEnvRoot, buildSet, sbNewName, pCompTree);
    }

    resp.setCompName(sbNewName.str());
    resp.setStatus("true");
  }
  else if (!strcmp(operation, "Delete"))
  {
    Owned<IPropertyTreeIterator> iterComp = pComponents->getElements("*");
    ForEach(*iterComp)
    {
      IPropertyTree& pComp = iterComp->query();
      const char* compName = pComp.queryProp(XML_ATTR_NAME);
      const char* compType = pComp.queryProp("@compType");
      StringBuffer xpath;
      xpath.clear().appendf("./Software/%s[@name=\"%s\"]", compType, compName);
      IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());
      StringBuffer sbMsg;

      if(pCompTree)
      {
        bool ret = checkComponentReferences(pEnvRoot, pCompTree, compName, sbMsg);

        if (ret)
        {
          pEnvRoot->queryPropTree("./Software")->removeTree(pCompTree);
          resp.setStatus("true");
          resp.setCompName(XML_TAG_SOFTWARE);
        }
        else
        {
          resp.setStatus(sbMsg.str());
          resp.setCompName(compName);
        }
      }
    }
  }
  else if (!strcmp(operation, "Duplicate"))
  {
    StringBuffer sbNewName;
    StringBuffer xpath;
    Owned<IPropertyTreeIterator> iterComp = pComponents->getElements("*");

    if (iterComp->first() && iterComp->isValid())
    {
      IPropertyTree& pComp = iterComp->query();
      const char* compName = pComp.queryProp(XML_ATTR_NAME);
      const char* compType = pComp.queryProp(XML_ATTR_COMPTYPE);

      sbNewName = compName;
      xpath = compType;
      getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
      xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, compType, XML_ATTR_NAME, compName);
      IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());

      if (pCompTree == NULL)
        throw MakeStringException(-1,"XPATH: %s is invalid.", xpath.str());

      StringBuffer xml;
      toXML(pCompTree, xml);

      IPropertyTree *dupTree = createPTreeFromXMLString(xml.str());
      dupTree->setProp(XML_ATTR_NAME, sbNewName.str());

      xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, compType, XML_ATTR_NAME, sbNewName.str());

      if (pEnvRoot->addPropTree(xpath, dupTree))
        resp.setStatus("true");
      else
        resp.setStatus("false");
    }
    else
    {
      resp.setStatus("false");
    }
    resp.setCompName(XML_TAG_SOFTWARE);
  }
  else if (!strcmp(operation, "CopySW"))
  {
    if (handleComponentCopy(pComponents, pEnvRoot.get()) == true)
      resp.setStatus("true");
    else
      resp.setStatus("false");
  }
  else if (!strcmp(operation, "CopyHW"))
  {
    if (handleHardwareCopy(pComponents, pEnvRoot->queryPropTree("Hardware"))== true)
      resp.setStatus("true");
    else
      resp.setStatus("false");
  }
  return true;
}

bool CWsDeployFileInfo::addCopyToPropTree(IPropertyTree* pPropTree, IPropertyTree* pDupTree, const char* tag_name)
{
  StringBuffer strTag;
  strTag.clear().appendf("%s/%s", XML_TAG_HARDWARE, tag_name);

  return pPropTree->addPropTree(strTag.str(), pDupTree) != NULL;
}

bool CWsDeployFileInfo::handleHardwareCopy(IPropertyTree *pComponents, IPropertyTree *pEnvRoot)
{
  StringBuffer xpath;
  StringBuffer xpath3;
  StringBuffer filePath;
  xpath.clear().appendf("%s", XML_TAG_HARDWARE);

  Owned<IPropertyTreeIterator> iterComp = pComponents->getElements("*");
  Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements("*");

  if (!iterComp->first())
    return false;

  CWsDeployFileInfo::setFilePath(filePath, iterComp->query().queryProp(XML_ATTR_TARGET));

  Owned<CWsDeployFileInfo> fi = new CWsDeployFileInfo(m_pService, filePath, false);

  fi->m_skipEnvUpdateFromNotification = false;
  fi->initFileInfo(false,false);
  Owned<IPropertyTree> pEnvRoot2 = &(fi->m_Environment->getPTree());

  bool bWrite = false;
  StringArray elems;

  ForEach(*iter)
  {
    IPropertyTree& pComp = iter->query();
    const char* name = pComp.queryProp(XML_ATTR_NAME);
    const char* tag_name = pComp.queryName();

    StringBuffer xpath2;
    xpath2.appendf("./%s/%s[%s = \"%s\"]", XML_TAG_HARDWARE, tag_name, XML_ATTR_NAME, name);

    if (pEnvRoot2->queryPropTree(xpath2.str()) != NULL) // check if target configuration has same named element
    {
      elems.append(name);
      continue;
    }

    StringBuffer xml;

    xml.appendf("<%s", tag_name);
    Owned<IAttributeIterator> pAttribIter = pComp.getAttributes();

    ForEach(*pAttribIter)
    {
      const char* name = &(pAttribIter->queryName()[1]);
      const char* value = pAttribIter->queryValue();

      xml.appendf(" %s=\"%s\" ", name, value);
    }
    xml.append("/>");

    IPropertyTree *dupTree = NULL;

    if (iterComp->query().queryProp(XML_ATTR_HWXPATH) && strlen(iterComp->query().queryProp(XML_ATTR_HWXPATH)) > 0)
    {
      xpath3.clear().appendf("<%s/>", iterComp->query().queryProp(XML_ATTR_HWXPATH));

      dupTree = createPTreeFromXMLString((xpath3.replace('\'','\"')).str());

      String strTagName(xpath3);
      strTagName = *strTagName.substring(1,strTagName.indexOf(' '));

      if (CWsDeployFileInfo::addCopyToPropTree(pEnvRoot2, dupTree, strTagName.toCharArray()) == false)
        return false;

      bWrite = true;
      break;
    }
    else
      dupTree = createPTreeFromXMLString(xml.str());

    if (CWsDeployFileInfo::addCopyToPropTree(pEnvRoot2, dupTree, tag_name) == false)
      return false;

    bWrite = true;
  }

  if (bWrite == true)
  {
    StringBuffer err;
    fi->saveEnvironment(NULL, NULL, err);

    if (elems.ordinality() > 0 && !(iterComp->query().queryProp(XML_ATTR_HWXPATH) && strlen(iterComp->query().queryProp(XML_ATTR_HWXPATH)) > 0))
    {
      StringBuffer errMsg;
      errMsg.appendf("Saved succeeded but some some element(s) could not be copied.  Element(s) may already exist in the target configuration.\n[");

      ForEachItemIn(i, elems)
      {
        errMsg.appendf("%s, ",elems.item(i));
      }

      errMsg.setCharAt(errMsg.length()-2 , ']');

      throw MakeStringException(-1, "%s", errMsg.str());
    }
  }
  else
  {
    throw MakeStringException(-1, "Copy failed.  All elements may already exist in the target configuration.");
  }

  return true;
}

bool CWsDeployFileInfo::handleComponentCopy(IPropertyTree *pComponents, IPropertyTree *pEnvRoot)
{
  Owned<IPropertyTreeIterator> iterComp = pComponents->getElements("*");
  bool bError = false;
  StringBuffer errMsg;

  char targetName[255] = "";
  iterComp->first();
  strncpy(targetName, iterComp->query().queryProp("@target"), 255);  //get the copy target configuration file name

  StringBuffer filePath;
  CWsDeployFileInfo::setFilePath(filePath, targetName);

  Owned<CWsDeployFileInfo> fi = new CWsDeployFileInfo(m_pService, filePath, false);

  fi->m_skipEnvUpdateFromNotification = false;
  fi->initFileInfo(false,false);

  Owned<IPropertyTree> pEnvRoot2 = &(fi->m_Environment->getPTree());
  Owned<IPropertyTreeIterator> iterComp2 = pComponents->getElements("*");

  StringBuffer xpath;

  ForEach(*iterComp)
  {
    IPropertyTree& pComp = iterComp->query();
    const char* compName = pComp.queryProp(XML_ATTR_NAME);

    if (compName == NULL)
    {
      if (bError == false)
      {
        bError = true;
        errMsg.clear().appendf("Faild to query for @name attribute, continuing...");
      }
      continue;
    }

    const char* compType = pComp.queryProp("@compType");
    StringBuffer sbNewName = compName;

    xpath.clear().appendf("%s", compType);
    getUniqueName(pEnvRoot2, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
    xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, compType, XML_ATTR_NAME, compName);
    IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());

    if ( pCompTree == NULL)
      throw MakeStringException(-1,"XPATH: %s is invalid in source configuration. Copy failed.", xpath.str());

    xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, compType, XML_ATTR_NAME, sbNewName.str());
    StringBuffer xml;

    toXML(pCompTree, xml);

    IPropertyTree *dupTree = createPTreeFromXMLString(xml.str());
    dupTree->setProp(XML_ATTR_NAME, sbNewName.str());

    if (pEnvRoot2->addPropTree(xpath, dupTree) == NULL)
      throw MakeStringException(-1,"XPATH: %s is invalid in target. Copy failed.", xpath.str());
  }

  StringBuffer err;
  fi->saveEnvironment(NULL, NULL, err);

  if (bError == true)
    throw MakeStringException(-1,"Save succeeded but an error was encountered with message: %s", errMsg.str());

  return true;
}


bool CWsDeployFileInfo::handleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* operation = req.getOperation();
  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> instances = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<Instances/>");
  const char* buildSet = instances->queryProp(XML_ATTR_BUILDSET);
  const char* compName = instances->queryProp("@compName");
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  StringBuffer xpath;
  xpath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]", buildSet);
  Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
  buildSetIter->first();
  IPropertyTree* pBuildSet = &buildSetIter->query();
  const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
  StringBuffer dups, reqdComps, reqdCompNames;

  if (!strcmp(operation, "Add"))
  {
    StringBuffer buildSetPath;
    Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
    xpath.clear().appendf("./Software/%s[@name=\"%s\"]", processName, compName);
    IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());

    Owned<IPropertyTreeIterator> iterInst = instances->getElements("*");
    bool bAdded = false;
    ForEach(*iterInst)
    {
      IPropertyTree& pComputer = iterInst->query();
      xpath.clear().appendf("./Hardware/Computer[@name=\"%s\"]", pComputer.queryProp(XML_ATTR_NAME)); 
      IPropertyTree* pComputerNode = pEnvRoot->queryPropTree(xpath.str());
      xpath.clear().appendf("Instance[@netAddress=\"%s\"]", pComputerNode->queryProp(XML_ATTR_NETADDRESS)); 
      if (pCompTree->queryPropTree(xpath.str()))
      {
        dups.appendf("\n%s", pComputerNode->queryProp(XML_ATTR_NETADDRESS));
        continue;
      }

      IPropertyTree* pNode = pCompTree->addPropTree(XML_TAG_INSTANCE, createPTree());

      if (pSchema)
      {
        Owned<IPropertyTreeIterator> iter = pSchema->getElements("xs:element/xs:complexType/xs:sequence/xs:element[@name=\"Instance\"]/xs:complexType/xs:attribute");
        ForEach(*iter)
        {
          IPropertyTree &attr = iter->query();
          StringBuffer attrName("@");
          attrName.append(attr.queryProp(XML_ATTR_NAME));

          // we try to pull @computer and @netAddress from computerNode. Others come from default values in schema (if supplied)
          const char *szAttrib;
          StringBuffer sb;
          if (!strcmp(attrName.str(), XML_ATTR_COMPUTER))
          {
            szAttrib = pComputerNode->queryProp(XML_ATTR_NAME);

            if (!bAdded)
            {
              bAdded = true;
              resp.setNewName(szAttrib);
            }
          }
          else if (!strcmp(attrName.str(), XML_ATTR_NETADDRESS))
            szAttrib = pComputerNode->queryProp(XML_ATTR_NETADDRESS);
          else if (!strcmp(attrName.str(), XML_ATTR_DIRECTORY))
          {
            StringBuffer rundir;
            if (!getConfigurationDirectory(pEnvRoot->queryPropTree("Software/Directories"),"run",buildSet,compName,rundir))
              sb.clear().appendf(RUNTIME_DIR"/%s", compName);
            else
              sb.clear().append(rundir);

            szAttrib = sb.str();
          }
          else
            szAttrib = attr.queryProp("@default");

          pNode->addProp(attrName.str(), szAttrib);
        }

        Owned<IPropertyTree> pNewCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSet, m_pService->getCfg(), m_pService->getName(), true);

        if (pNewCompTree)
        {
          StringBuffer sbxml;
          toXML(pNewCompTree, sbxml);

          Owned<IPropertyTreeIterator> iterElems = pNewCompTree->getElements("Instance/*");
          ForEach (*iterElems)
          {
            IPropertyTree* pElem = &iterElems->query();
           
            if (!pNode->queryProp(pElem->queryName()))
              pNode->addPropTree(pElem->queryName(), createPTreeFromIPT(pElem));
          }
        }
      }

      if (!checkForRequiredComponents(pEnvRoot, pComputerNode->queryProp(XML_ATTR_NETADDRESS), reqdCompNames, buildSet))
        reqdComps.appendf("\n%s", pComputerNode->queryProp(XML_ATTR_NETADDRESS));
    }

    resp.setReqdCompNames(reqdCompNames.str());
    resp.setAddReqdComps(reqdComps.str());
    resp.setDuplicates(dups.str());
  }
  else if (!strcmp(operation, "Delete"))
  {
    Owned<IPropertyTreeIterator> iterInst = instances->getElements("*");
    ForEach(*iterInst)
    {
      IPropertyTree& pInst = iterInst->query();

      StringBuffer decodedParams( pInst.queryProp("@params") );
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");

      xpath.clear().appendf("./Software/%s[@name=\"%s\"]", pszCompType, pszCompName);
      IPropertyTree* pInstParent = pEnvRoot->queryPropTree(xpath.str());
      if (pszSubTypeKey[0] == '[' && pszSubTypeKey[strlen(pszSubTypeKey) - 1] == ']')
        xpath.clear().appendf("%s%s", pszSubType, pszSubTypeKey);
      else
        xpath.clear().appendf("%s[@name=\"%s\"]", pszSubType, pszSubTypeKey);
      
      IPropertyTree* pInstTree = pInstParent->queryPropTree(xpath.str());

      if (pInstParent && pInstTree)
        pInstParent->removeTree(pInstTree);
    }
  }

  //rename instance names
  int nCount = 1;
  xpath.clear().appendf("./Software/%s[@name=\"%s\"]/Instance", processName, compName);
  Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements(xpath.str());
  StringBuffer sName;

  ForEach(*iter)
  {
    sName.clear().append("s").append(nCount);
    iter->query().setProp(XML_ATTR_NAME, sName.str());
    nCount++;
  }

  resp.setStatus("true");
  return true;
}

bool CWsDeployFileInfo::addReqdComps(IEspContext &context, IEspAddReqdCompsRequest &req, IEspAddReqdCompsResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> instances = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<ReqdComps/>");
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  Owned<IPropertyTreeIterator> iterComp = instances->getElements("*");
  bool bAdded = false;
  StringBuffer reqCompNames, failed;

  ForEach(*iterComp)
  {
    IPropertyTree& pComputer = iterComp->query();

    if (!checkForRequiredComponents(pEnvRoot, pComputer.queryProp(XML_ATTR_NETADDRESS), reqCompNames, NULL, true))
      failed.appendf("\n%s", pComputer.queryProp(XML_ATTR_NETADDRESS));
  }

  resp.setFailures(failed.str());
  resp.setStatus("true");
  return true;
}

bool CWsDeployFileInfo::handleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pBindings = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<EspServiceBindings/>");
  const char* type = pBindings->queryProp(XML_ATTR_TYPE);
  const char* espName = pBindings->queryProp("@compName");

  const char* operation = req.getOperation();
  StringBuffer sbNewName;
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  StringBuffer xpath;

  if (!strcmp(operation, "Add"))
  {
    xpath.append("./Programs/Build/BuildSet[@processName=\"EspProcess\"]");
    Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
    buildSetIter->first();
    IPropertyTree* pBuildSet = &buildSetIter->query();
    const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
    const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
    StringBuffer buildSetPath;
    Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
    xpath.clear().appendf("./Software/%s[@name='%s']", processName, espName);

    Owned<IPropertyTreeIterator> iterItems = pBindings->getElements("Item");
    bool flag = false;

    ForEach (*iterItems)
    {
      flag = true;
      IPropertyTree* pItem = &iterItems->query();
      const char* bindingName = pItem->queryProp(XML_ATTR_NAME);
      const char* params = pItem->queryProp("@params");

      StringBuffer decodedParams(params);
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");

      if (strcmp(type, XML_TAG_ESPBINDING) && bindingName)
        xpath.appendf("/EspBinding[@name='%s']", bindingName);
      else if (pszSubType && *pszSubType)
      {
        String subType(pszSubType);

        int idx = subType.lastIndexOf('/');
        if (idx > 0)
        {
          String* tmpstr = subType.substring(0, idx);
          xpath.append("/").append(*tmpstr);
          delete tmpstr;
        }
      }

      IPropertyTree* pEspService = pEnvRoot->queryPropTree(xpath.str());    
      IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());
      StringBuffer sb(type);

      if (!strncmp(sb.str(), "_", 1))
        sb.remove(0, 1);

      if (!strcmp(type, XML_TAG_ESPBINDING))
      {
        StringBuffer sbNewName(XML_TAG_ESPBINDING);
        xpath.clear().appendf("%s[@name='%s']/EspBinding", processName, espName);

        getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
        xpath.clear().append(sb.str()).append("/").append(XML_ATTR_NAME);
        pCompTree->setProp(xpath.str(), sbNewName);
        resp.setNewName(sbNewName);
      }

      if (pEspService && pCompTree)
        pEspService->addPropTree(sb.str(), pCompTree->queryPropTree(sb.str()));

      //If we are adding, just consider the first selection.
      break;
    }

    if (!flag)
    {
      IPropertyTree* pEspService = pEnvRoot->queryPropTree(xpath.str());    
      IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());
      StringBuffer sbNewName(XML_TAG_ESPBINDING);
      xpath.clear().appendf("%s[@name='%s']/EspBinding", processName, espName);

      getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
      xpath.clear().append(XML_TAG_ESPBINDING).append("/").append(XML_ATTR_NAME);
      pCompTree->setProp(xpath.str(), sbNewName);
      resp.setNewName(sbNewName);

      if (pEspService && pCompTree)
        pEspService->addPropTree(XML_TAG_ESPBINDING, pCompTree->queryPropTree(XML_TAG_ESPBINDING));
    }

    resp.setStatus("true");
  }
  else if (!strcmp(operation, "Delete"))
  {
    Owned<IPropertyTreeIterator> iterItems = pBindings->getElements("Item");
    bool deleteAll = true;
    IPropertyTreePtrArray bindings;
    ForEach (*iterItems)
    {
      IPropertyTree* pItem = &iterItems->query();
      const char* bindingName = pItem->queryProp(XML_ATTR_NAME);
      const char* params = pItem->queryProp("@params");

      StringBuffer decodedParams(params);
      decodedParams.replaceString("::", "\n");

      Owned<IProperties> pParams = createProperties();
      pParams->loadProps(decodedParams.str());

      const char* pszCompType = pParams->queryProp("pcType");
      const char* pszCompName = pParams->queryProp("pcName");
      const char* pszSubType = pParams->queryProp("subType");
      const char* pszSubTypeKey = pParams->queryProp("subTypeKey");

      xpath.clear().appendf("./Software/%s[@name='%s']", XML_TAG_ESPPROCESS, espName);
      if (!strcmp(type, XML_TAG_ESPBINDING))
      {
        if (bindingName)
          xpath.appendf("/EspBinding[@name='%s']", bindingName);

        IPropertyTree* pEspBinding = pEnvRoot->queryPropTree(xpath.str());  
        StringBuffer sbMsg;
        StringBuffer sbFullName(espName);
        sbFullName.append("/").append(bindingName);
        bool ret = checkComponentReferences(pEnvRoot, pEspBinding, sbFullName.str(), sbMsg);

        if (ret)
          bindings.push_back(pEspBinding);
        else
        {
          deleteAll = false;
          resp.setStatus(sbMsg.str());
          break;
        }
      }
      else
      {
        if (pszSubType && *pszSubType)
          xpath.append("/").append(pszSubType);

        const char* resource = pItem->queryProp("@resource");

        if (resource)
          xpath.appendf("[@resource='%s']", resource);

        IPropertyTree* pSubType = pEnvRoot->queryPropTree(xpath.str());

        String subType(xpath.str());

        int idx = subType.lastIndexOf('/');
        if (idx > 0)
        {
          String* tmpstr = subType.substring(0, idx);    
          xpath.clear().append(*tmpstr);
          delete tmpstr;
        }
        
        if (pSubType)
          pEnvRoot->queryPropTree(xpath.str())->removeTree(pSubType);
      }
    }

    if (deleteAll)
    {
      xpath.clear().appendf("./Software/%s[@name='%s']", XML_TAG_ESPPROCESS, espName);
      IPropertyTree* pEspService = pEnvRoot->queryPropTree(xpath.str());
      int nBindings = bindings.size();
      for (int i=0; i < nBindings; i++)
        {
            IPropertyTree* pBinding = bindings[i];
        pEspService->removeTree(pBinding);
      }

      resp.setStatus("true");
    }
  }

  return true;
}

bool CWsDeployFileInfo::handleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pParams = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  const char* type = pParams->queryProp(XML_ATTR_TYPE);
  const char* operation = req.getOperation();
  StringBuffer sbNewName;
  StringBuffer xpath;

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  if (!strcmp(operation, "New"))
  {
    StringBuffer sbTemp;
    IPropertyTree* pCompTree = createPTree(XML_TAG_HARDWARE);
    generateHardwareHeaders(pEnvRoot, sbTemp, false, pCompTree);

    StringBuffer sbNewName(type);
    xpath.clear().appendf("%s", type);

    getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_HARDWARE);
    xpath.clear().append(type).append("/").append(XML_ATTR_NAME);
    pCompTree->setProp(xpath.str(), sbNewName);

    pEnvRoot->queryPropTree(XML_TAG_HARDWARE)->addPropTree(type, pCompTree->queryPropTree(type));

    resp.setCompName(sbNewName.str());
    resp.setStatus("true");
  }
  else if (!strcmp(operation, "NewRange"))
  {
    const char* prefix = pParams->queryProp("@prefix");
    const char* domain = pParams->queryProp(XML_ATTR_DOMAIN);
    const char* cType = pParams->queryProp(XML_ATTR_COMPUTERTYPE);
    const char* startIP = pParams->queryProp("@startIP");
    const char* endIP = pParams->queryProp("@endIP");

    IPropertyTree* pComps = getNewRange(pEnvRoot, prefix, domain, cType, startIP, endIP);

    if (m_bCloud)
    {
      StringBuffer sbMsg;
      CCloudActionHandler lockCloud(this, CLOUD_LOCK_ENV, CLOUD_UNLOCK_ENV, m_userWithLock.str(), "8015", pComps);
      bool ret = lockCloud.start(sbMsg);
      if (!ret || sbMsg.length())
      {
        resp.setStatus("false");
        throw MakeStringException(-1, "Cannot add new range of computers as environment lock could not be obtained. Reason(s):\n%s", sbMsg.str());
      }
    }

    if (pComps)
    {
      mergePTree(pEnvRoot->queryPropTree(XML_TAG_HARDWARE), pComps);
      resp.setCompName(pComps->queryPropTree("Computer[1]")->queryProp(XML_ATTR_NAME));
    }

    resp.setStatus("true");
  }
  else if (!strcmp(operation, "Delete"))
  {
    StringBuffer refs;
    StringBuffer refName;
    Owned<IPropertyTreeIterator> iterComputers = pParams->getElements("Item");
    ForEach (*iterComputers)
    {
      IPropertyTree* pComp = &iterComputers->query();

      const char* name = pComp->queryProp(XML_ATTR_NAME);
      if (!strcmp(type, XML_TAG_COMPUTER))
        xpath.clear().appendf(XML_TAG_SOFTWARE"//["XML_ATTR_COMPUTER"=\"%s\"]", name);
      else if (!strcmp(type, XML_TAG_COMPUTERTYPE))
        xpath.clear().appendf(XML_TAG_HARDWARE"//["XML_ATTR_COMPUTERTYPE"=\"%s\"]", name);
      else if (!strcmp(type, XML_TAG_DOMAIN))
        xpath.clear().appendf(XML_TAG_HARDWARE"//["XML_ATTR_DOMAIN"=\"%s\"]", name);
      else if (!strcmp(type, XML_TAG_SWITCH))
        xpath.clear().appendf(XML_TAG_HARDWARE"//["XML_ATTR_SWITCH"=\"%s\"]", name);

      Owned<IPropertyTreeIterator> iter = pEnvRoot->getElements(xpath.str());

      ForEach(*iter)
      {
        IPropertyTree& pComp = iter->query();
        const char* compName = pComp.queryProp(XML_ATTR_NAME);
        const char* parentName = pComp.queryName();
        refs.append("\n").append(parentName).append(" name=").append(compName);
      }

      if (refs.length())
      {
        refName.clear().append(name);
        break;
      }
    }
    
    if (refs.length())
      throw MakeStringException(-1, "Cannot delete %s with name %s as it is being referenced by components: %s.", type, refName.str(), refs.str());
    else
    {
      if (m_bCloud && !strcmp(type, XML_TAG_COMPUTER))
      {
        StringBuffer sb, sbMsg;
        sb.append("<Computers>");
        ForEach (*iterComputers)
        {
          IPropertyTree* pComp = &iterComputers->query();
          xpath.clear().appendf(XML_TAG_HARDWARE"/%s["XML_ATTR_NAME"=\"%s\"]", type, pComp->queryProp(XML_ATTR_NAME));
          IPropertyTree* pTree = pEnvRoot->queryPropTree(xpath.str());
          sb.appendf("<Computer netAddress='%s'/>", pTree->queryProp(XML_ATTR_NETADDRESS));
        }
        sb.append("</Computers>");
        Owned<IPropertyTree> pComputers = createPTreeFromXMLString(sb.str());

        CCloudActionHandler unlockCloud(this, CLOUD_UNLOCK_ENV, CLOUD_LOCK_ENV, m_userWithLock.str(), "8015", pComputers);
        bool ret = unlockCloud.start(sbMsg);
        if (!ret || sbMsg.length())
          throw MakeStringException(-1, "Cannot delete computers as they cannot be unlocked. Reason(s):\n%s", sbMsg.str());
      }

      ForEach (*iterComputers)
      {
        IPropertyTree* pComp = &iterComputers->query();
        xpath.clear().appendf(XML_TAG_HARDWARE"/%s["XML_ATTR_NAME"=\"%s\"]", type, pComp->queryProp(XML_ATTR_NAME));
        IPropertyTree* pTree = pEnvRoot->queryPropTree(xpath.str());
        pEnvRoot->queryPropTree(XML_TAG_HARDWARE)->removeTree(pTree);
      }
       
      resp.setStatus("true");
    }
  }

  return true;
}


bool CWsDeployFileInfo::handleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pParams = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  const char* compType = pParams->queryProp("@compType");
  const char* name = pParams->queryProp(XML_ATTR_NAME);
  const char* newType = pParams->queryProp("@newType");
  const char* operation = req.getOperation();
  StringBuffer decodedParams( pParams->queryProp("@params") );
  decodedParams.replaceString("::", "\n");

  Owned<IProperties> pTopParams = createProperties();
  pTopParams->loadProps(decodedParams.str());

  StringBuffer buf("Software/Topology");

  for (int i = 3; i >= 0; i--)
  {
    StringBuffer sb;
    sb.appendf("inner%d_name", i);
    const char* sbName = pTopParams->queryProp(sb.str());

    if (sbName)
    {
      if (strstr(sbName, "EclCCServerProcess") == sbName ||
          strstr(sbName, "EclServerProcess") == sbName ||
          strstr(sbName, "EclAgentProcess") == sbName ||
          strstr(sbName, "EclSchedulerProcess") == sbName)
      {
        StringBuffer sbEcl(sbName);
        if (strstr(sbName, " - "))
          sbEcl.replaceString(" - ", "[@process='").append("']");
        else if (strstr(sbName, " -"))
          sbEcl.replaceString(" -", "[@process='").append("']");
        buf.append("/").append(sbEcl);
      }
      else if (strstr(sbName, "Cluster") == sbName)
      {
        StringBuffer sbCl(sbName);
        if (strstr(sbName, " - "))
          sbCl.replaceString(" - ", "[@name='").append("']");
        else if (strstr(sbName, " -"))
          sbCl.replaceString(" -", "[@name='").append("']");
        buf.append("/").append(sbCl);
      }
      else if (strstr(sbName, XML_TAG_THORCLUSTER) == sbName)
        buf.append("/ThorCluster");
      else if (strstr(sbName, XML_TAG_ROXIECLUSTER) == sbName)
        buf.append("/RoxieCluster");
      else if (buf.str()[buf.length() - 1] != ']')
        buf.appendf("[@%s='%s']", sbName, pTopParams->queryProp(sb.replaceString("_name", "_value").str()));
    }
  }

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  if (!strcmp(operation, "Add"))
  {
    StringBuffer sbNewType(newType);

    if (!strcmp(newType, "EclCCServer"))
      sbNewType.clear().append(XML_TAG_ECLCCSERVERPROCESS);
    else if (!strcmp(newType, "EclServer"))
      sbNewType.clear().append(XML_TAG_ECLSERVERPROCESS);
    else if (!strcmp(newType, "EclAgent"))
      sbNewType.clear().append(XML_TAG_ECLAGENTPROCESS);
    else if (!strcmp(newType, "EclScheduler"))
      sbNewType.clear().append(XML_TAG_ECLSCHEDULERPROCESS);
    else if (!strcmp(newType, "Thor"))
      sbNewType.clear().append(XML_TAG_THORCLUSTER);
    else if (!strcmp(newType, "Roxie"))
      sbNewType.clear().append(XML_TAG_ROXIECLUSTER);

    IPropertyTree* pNode = createPTree(sbNewType.str());

    if (!strcmp(sbNewType.str(), XML_TAG_ECLCCSERVERPROCESS) ||
      !strcmp(sbNewType.str(), XML_TAG_ECLSERVERPROCESS) ||
      !strcmp(sbNewType.str(), XML_TAG_ECLAGENTPROCESS) ||
      !strcmp(sbNewType.str(), XML_TAG_ECLSCHEDULERPROCESS) ||
      !strcmp(sbNewType.str(), XML_TAG_THORCLUSTER) ||
      !strcmp(sbNewType.str(), XML_TAG_ROXIECLUSTER))
      pNode->addProp(XML_ATTR_PROCESS, "");
    else if (!strcmp(sbNewType.str(), XML_TAG_CLUSTER))
    {
      pNode->addProp(XML_ATTR_NAME, "");
      pNode->addProp(XML_ATTR_PREFIX, "");
    }

    IPropertyTree* pTopology = pEnvRoot->queryPropTree(buf.str());
    if (pTopology)
      pTopology->addPropTree(sbNewType.str(), pNode);

    resp.setStatus("true");
  }
  else if (!strcmp(operation, "Delete"))
  {
    String sParent(buf.str());
    StringBuffer sbParent(XML_TAG_SOFTWARE"/"XML_TAG_TOPOLOGY);
    int idx = sParent.lastIndexOf('/');

    if (idx > 0)
    {
      String* tmpstr = sParent.substring(0, idx);
      sbParent.clear().append(*tmpstr);
      delete tmpstr;
    }

    IPropertyTree* pTopology = pEnvRoot->queryPropTree(sbParent);
    IPropertyTree* pDel = pEnvRoot->queryPropTree(buf.str());
    
    if (pTopology && pDel)
      pTopology->removeTree(pDel);
    resp.setStatus("true");
  }

  return true;
}

bool CWsDeployFileInfo::handleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  const char* operation = req.getOperation();

  StringBuffer xpath;
  StringBuffer sMsg;
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());

  bool retVal = handleThorTopologyOp(pEnvRoot, operation, xmlArg, sMsg);
  resp.setStatus(retVal? "true" : sMsg.str());

  return true;
}


bool CWsDeployFileInfo::handleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  const char* xmlArg = req.getXmlArgs();
  Owned<IPropertyTree> pParams = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<XmlArgs/>");
  const char* buildSet = pParams->queryProp(XML_ATTR_BUILDSET);
  const char* name = pParams->queryProp("@compName");
  const char* rowType = pParams->queryProp("@rowType");
  const char* operation = req.getOperation();
  StringBuffer sbNewName;
  StringBuffer xpath;
  Owned<IPropertyTreeIterator> buildSetIter;
  IPropertyTree* pBuildSet = NULL;
  const char* buildSetName = NULL;
  const char* processName = NULL;

  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  if (strcmp(name, "Directories"))
  {
    xpath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]", buildSet);
    buildSetIter.setown(pEnvRoot->getElements(xpath.str()));
    buildSetIter->first();
    pBuildSet = &buildSetIter->query();
    buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
    processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
  }

  StringBuffer buildSetPath;
  Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
  IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());

  if (!strcmp(operation, "Add"))
  {
    if (!strcmp(name, "Directories"))
    {
      xpath.clear().appendf("Software/Directories/Category[@name='%s']", rowType);
      IPropertyTree* pCat = pEnvRoot->queryPropTree(xpath.str());
      if (pCat)
      {
        IPropertyTree* pOver = pCat->queryPropTree("Override[@component=''][@dir=''][@instance='']");
        if (!pOver)
        {
          pOver = pCat->addPropTree("Override", createPTree());
          pOver->addProp("@component", "");
          pOver->addProp("@dir", "");
          pOver->addProp("@instance", "");
        }
      }
    }
    else
    {
      xpath.clear().appendf("./Software/%s[@name='%s']", processName, name);
      Owned<IPropertyTreeIterator> rows = pParams->getElements("Row");
      bool flag = false;

      ForEach (*rows)
      {
        flag = true;
        IPropertyTree* pRow = &rows->query();
        const char* params = pRow->queryProp("@params");

        StringBuffer decodedParams(params);
        decodedParams.replaceString("::", "\n");

        Owned<IProperties> pParams = createProperties();
        pParams->loadProps(decodedParams.str());

        const char* pszCompType = pParams->queryProp("pcType");
        const char* pszCompName = pParams->queryProp("pcName");
        const char* pszSubType = pParams->queryProp("subType");
        const char* pszSubTypeKey = pParams->queryProp("subTypeKey");
        StringBuffer sbParent;

        if (pszSubType && *pszSubType)
        {
          String subType(pszSubType);

          int idx = subType.lastIndexOf('/');
          if (idx > 0)
          {
            String* tmpstr = subType.substring(0, idx);
            xpath.append("/").append(*tmpstr);
            sbParent.append(*tmpstr);
            delete tmpstr;
          }
          else
          {
            xpath.append("/").append(pszSubType);
            sbParent.append(pszSubType);

            if (pszSubTypeKey && *pszSubTypeKey)
              xpath.append(pszSubTypeKey);
          }
        }

        IPropertyTree* pComponent = pEnvRoot->queryPropTree(xpath.str());   
        IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, m_pService->getCfg(), m_pService->getName());
        StringBuffer sb(rowType);

        if (!strncmp(sb.str(), "_", 1))
          sb.remove(0, 1);
        
        StringBuffer s;
        s.appendf("xs:element//*[@name=\"%s\"]//*[@name=\"%s\"]//xs:attribute[@name='name']", sbParent.str(), rowType);
        IPropertyTree* pt = pSchema->queryPropTree(s.str());
        
        if (pt && pt->hasProp("@type"))
        {
          const char* px = pt->queryProp("@type");
          if (!strcmp(px, "xs:string"))
          {
            StringBuffer sbNewName(rowType);
            xpath.clear().appendf("%s[@name='%s']/%s", processName, name, sbParent.str());

            getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
            pCompTree->setProp(sb.str(), sbNewName);
            resp.setCompName(sbNewName);
          }
        }

        if (pComponent && pCompTree)
          pComponent->addPropTree(rowType, pCompTree->queryPropTree(rowType));

        //If we are adding, just consider the first selection.
        break;
      }

      if (!flag)
      {
        if (pCompTree->queryPropTree(rowType))
        {
          if (strcmp(rowType, "Notes"))
          {
            StringBuffer sbNewName(rowType);
            xpath.clear().appendf("%s[@name]", rowType);
            if (pCompTree->hasProp(xpath.str()))
            {
              xpath.clear().appendf("%s[@name='%s']/%s", processName, name, rowType);
              getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
            
              pCompTree->queryPropTree(rowType)->setProp(XML_ATTR_NAME, sbNewName);
              resp.setCompName(sbNewName.str());
            }
          }
          else
          {
            IPropertyTree* pNotes = pCompTree->queryPropTree(rowType);
            if (pNotes->hasProp("@computer"))
              pNotes->setProp("@computer", m_userIp.str());
            if (pNotes->hasProp("@user"))
              pNotes->setProp("@user", "");
            if (pNotes->hasProp("@date"))
            {
              CDateTime dt;
              StringBuffer tmp;
              dt.setNow();
              tmp.clear();

              unsigned mo, da, yr;
              dt.getDate(yr, mo, da, true);
              tmp.appendf("%d/%d/%d ", mo, da, yr);
              dt.getTimeString(tmp, true);
              pNotes->setProp("@date", tmp.str());
              resp.setCompName(tmp.str());
            }
          }

          xpath.clear().appendf("./Software/%s[@name='%s']", processName, name);
          IPropertyTree* pComponent = pEnvRoot->queryPropTree(xpath.str());
          if (pComponent)
            pComponent->addPropTree(rowType, pCompTree->queryPropTree(rowType));
        }
      }
    }
    resp.setStatus("true");
  }
  else if (!strcmp(operation, "Delete"))
  {
    if (!strcmp(name, "Directories"))
      xpath.clear().appendf("./Software/Directories/Category[@name='%s']", rowType);
    else
      xpath.clear().appendf("./Software/%s[@name='%s']", processName, name);
    
    IPropertyTree* pComponent = pEnvRoot->queryPropTree(xpath.str());
    Owned<IPropertyTreeIterator> iterRows = pParams->getElements("Row");
    ForEach (*iterRows)
    {
      IPropertyTree* pRow = &iterRows->query();
      const char* rowName = pRow->queryProp("@rowName");
      if (!strcmp(name, "Directories"))
      {
        const char* compType = pRow->queryProp("@compType");
        xpath.clear().appendf("Override[@component='%s'][@instance='%s']", compType, rowName);
      }
      else
      {
        if (!strcmp(rowType, "Notes"))
        {
          const char* rowDate = pRow->queryProp("@rowDate");
          if (!rowDate || !*rowDate)
            continue;
          xpath.clear().appendf("%s[@date='%s']", rowType, rowDate);
        }
        else if (rowName && *rowName)
          xpath.clear().appendf("%s[@name='%s']", rowType, rowName);
        else 
        {
          const char* params = pRow->queryProp("@params");
          StringBuffer decodedParams(params);
          decodedParams.replaceString("::", "\n");

          Owned<IProperties> pParams = createProperties();
          pParams->loadProps(decodedParams.str());

          const char* pszCompType = pParams->queryProp("pcType");
          const char* pszCompName = pParams->queryProp("pcName");
          const char* pszSubType = pParams->queryProp("subType");
          const char* pszSubTypeKey = pParams->queryProp("subTypeKey");
          StringBuffer sbParent;

          if (pszSubType && *pszSubType)
          {
            String subType(pszSubType);

            xpath.clear().append(pszSubType);

            if (pszSubTypeKey && *pszSubTypeKey)
              xpath.append(pszSubTypeKey);
          }
        }
      }

      if (pComponent)
        pComponent->removeTree(pComponent->queryPropTree(xpath.str()));
    }
    
    resp.setCompName("");
    resp.setStatus("true");
  }

  return true;
}

IPropertyTree* CWsDeployFileInfo::findComponentForFolder(IPropertyTree* pFolder, IPropertyTree* pEnvSoftware)
{
  //Folder's @params has string of the form:
  //"comp=EspProcess&name=esp&inst=s1&computer=2wd20"
  //
  StringBuffer decodedParams( pFolder->queryProp("@params") );
  decodedParams.replace('&', '\n');

  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());

  const char* comp = pParams->queryProp("comp");
  const char* name = pParams->queryProp("name");

  if (!(comp && *comp && name && *name))
    throw MakeStringException(-1, "Invalid parameters");

  StringBuffer xpath;
  xpath.appendf("%s[@name='%s']", comp, name);

  IPropertyTree* pComp = pEnvSoftware->queryPropTree(xpath.str());
  if (!pComp)
    throw MakeStringException(-1, "No such component in environment: '%s' named '%s'.", comp, name);

  return pComp;
}

void CWsDeployFileInfo::addDeployableComponentAndInstances(IPropertyTree* pEnvRoot, IPropertyTree* pComp,
                                                     IPropertyTree* pDst,       IPropertyTree* pFolder,
                                                     const char* displayType)
{
  const char* comp = pComp->queryName();
  const char* name = pComp->queryProp(XML_ATTR_NAME);

  if (!name || !*name)
    throw MakeStringException(-1, "The environment has an incomplete definition for a '%s'!", comp);

  const char* build= pComp->queryProp(XML_ATTR_BUILD);
  const char* buildSet= pComp->queryProp(XML_ATTR_BUILDSET);

  if (!build || !*build)
    throw MakeStringException(-1, "%s '%s' does not have any build defined!", comp, name);

  if (!buildSet || !*buildSet)
    throw MakeStringException(-1, "%s '%s' does not have any build set defined!", comp, name);

  StringBuffer xpath;
  xpath.appendf("Programs/Build[@name='%s']/BuildSet[@name='%s']", build, buildSet);
  IPropertyTree* pBuildSetNode = pEnvRoot->queryPropTree(xpath.str());
  if (!pBuildSetNode)
    throw MakeStringException(-1, "Build %s or build set %s is not defined!", build, buildSet);

  const char* deployable = pBuildSetNode->queryProp("@deployable");
  if (!deployable || (0!=strcmp(deployable, "no") && 0!=strcmp(deployable, "false")))
  { 
    char achDisplayType[132];//long enough to hold any expanded type whatsoever
    if (!displayType)
    {
      GetDisplayProcessName(pComp->queryName(), achDisplayType);
      displayType = achDisplayType;
    }

    const bool bDeployAllInstances = !pFolder || pFolder->getPropBool("@selected", false);
    if (bDeployAllInstances)
    {
      //add its instances
      Owned<IPropertyTreeIterator> iInst = pComp->getElements("*"); 
      ForEach(*iInst)
      {
        IPropertyTree* pInstance = &iInst->query();
        const char* instType = pInstance->queryName();

        if (instType && (!strcmp(instType, XML_TAG_INSTANCE) || !strcmp(instType, XML_TAG_ROXIE_SERVER)))
        {
          const char* instName = pInstance->queryProp(XML_ATTR_NAME);
          const char* computer = pInstance->queryProp(XML_ATTR_COMPUTER);

          if (instName && *instName && computer && *computer)
            addInstance(pDst,  comp, displayType, name, build, instType, instName, computer);
        }
      }//ForEach(*iInst)
    }
    else
    {
      //pFolder exists and is not selected so deploy selected instances
      Owned<IPropertyTreeIterator> iLink = pFolder->getElements("*[@selected='true']");
      ForEach(*iLink)
      {
        IPropertyTree* pLink = &iLink->query();
        const char* params = pLink->queryProp("@params");
        if (params)
        {
          // pFolder's @params has string of the form: "comp=EspProcess&amp;name=esp&amp;instType=Instance&amp;inst=s1&amp;computer=2wd20"
          //
          StringBuffer decodedParams( params );
          decodedParams.replace('&', '\n');

          Owned<IProperties> pParams = createProperties();
          pParams->loadProps( decodedParams.str() );

          const char* instType = pParams->queryProp("instType");
          const char* instName = pParams->queryProp("inst");
          const char* computer = pParams->queryProp(TAG_COMPUTER);

          if (instType && *instType && instName && *instName && computer && *computer)
          {
            StringBuffer xpath;
            xpath.appendf("%s[@name='%s']", instType, instName);

            IPropertyTree* pInstNode = pComp->queryPropTree(xpath.str());

            if (!pInstNode)
              throw MakeStringException(-1, "%s '%s' does not have any '%s' named %s!", displayType, comp, instType, instName);

            addInstance(pDst,  comp, displayType, name, build, instType, instName, computer);
          }
        }
      }
    }
  }
}


void CWsDeployFileInfo::addInstance(IPropertyTree* pDst,  const char* comp, const char* displayType, 
                              const char* compName, const char* build, const char* instType, 
                              const char* instName, const char* computer)
{
  IPropertyTree* pCompNode = pDst->addPropTree( XML_TAG_COMPONENT, createPTree() );
  pCompNode->addProp("Type",                comp);
  pCompNode->addProp("DisplayType", displayType);   
  pCompNode->addProp("Name",                compName);
  pCompNode->addProp("Build",           build);

  pCompNode->addProp("InstanceType",    instType);
  pCompNode->addProp("Instance",        instName);
  pCompNode->addProp("Computer",        computer);
}

//---------------------------------------------------------------------------
//  GetDisplayProcessName
//---------------------------------------------------------------------------
const char* CWsDeployFileInfo::GetDisplayProcessName(const char* processName, char* buf) const
{
  //produces "LDAPServerProcess" as "LDAP Server" and "EspService" as "Esp Service", etc.
  if (!strcmp(processName, XML_TAG_ESPPROCESS))
    return strcpy(buf, "ESP Server");
  else
  {
    const char* begin = buf;
    const char* end = strstr(processName, "Process");
    if (!end)
      end = processName + strlen(processName);

    *buf++ = *processName++;
    bool bLower = false;

    while (processName < end)
    {
      char ch = *processName;
      if (isupper(ch))
      {
        if (bLower || //last char was uppercase or the following character is lowercase?
          ((processName+1 < end) && islower(*(processName+1))))
        {
          *buf++ = ' ';
        }

        bLower = false;
      }
      else
        bLower = true;

      *buf++ = *processName++;
    }
    *buf = '\0';
    return begin;
  }
}

void CWsDeployFileInfo::setFilePath(StringBuffer &filePath, const char* targetName)
{
  filePath.clear().append(CONFIG_SOURCE_DIR);

  if (filePath.charAt(filePath.length() - 1) != PATHSEPCHAR)
    filePath.append(PATHSEPCHAR);

  filePath.append(targetName);
}

void CWsDeployFileInfo::updateConfigFromFile()
{
  StringBuffer sbxml;

  synchronized block(m_mutex);

  if (m_pFileIO.get() != NULL)
  {
    m_pFileIO.clear();
  }
  if (m_lastSaved.isNull())
  {
    m_lastSaved.setNow();
  }

  m_pFileIO.setown(m_pFile->open(IFOread));
  Owned <IPropertyTree> pTree = createPTree(*m_pFileIO);
  toXML(pTree, sbxml.clear());

  Owned<IEnvironmentFactory> factory = getEnvironmentFactory();

  m_constEnvRdOnly.clear();
  m_constEnvRdOnly.setown(factory->loadLocalEnvironment(sbxml.str()));
  m_lastSaved.clear();
  m_lastSaved.setNow();
}

bool CWsDeployFileInfo::deploy(IEspContext &context, IEspDeployRequest& req, IEspDeployResponse& resp)
{
  synchronized block(m_mutex);
  checkForRefresh(context, &req.getReqInfo(), true);

  resp.setComponent("WsDeploy");
  resp.setCommand("Deploy");

  StringBuffer deployResult;
  IConstDeployInfo& depInfo = req.getDeploy();
  IArrayOf<IConstComponent>& components = depInfo.getComponents();
  unsigned int nComps = components.ordinality();
  if (nComps == 0)
  {
    resp.setStatus( "Please select at least one component to deploy!" );
    CDeployOptions& depOptions = dynamic_cast<CDeployOptions&>( depInfo.getOptions() );
    depOptions.serializeStruct(&context,deployResult, "Options");
  }
  else
  {
    CWsDeployEngine depEngine(*m_pService, req.getDeploy(),&context);
    depEngine.deploy();

    Owned<IPropertyTree> pDeployResult = depEngine.getDeployResult();
    IPropertyTree* pComponents = pDeployResult->queryPropTree("Components");

    toXML(pComponents, deployResult, false);

    IPropertyTree* pOptions = pDeployResult->queryPropTree("Options");
    toXML(pOptions, deployResult, false);

    resp.setStatus( depEngine.getDeployStatus() );
  }
  resp.setDeploy( deployResult.str() );
  return true;
}

bool CWsDeployExCE::onInit(IEspContext &context, IEspEmptyRequest& req, IEspInitResponse& resp)
{
  resp.setComponent("WsDeploy");
  resp.setCommand("Init");
  return true;
}

//the following method must be called with ownership of m_mutex
//
void CWsDeployFileInfo::generateGraph(IEspContext &context, IConstWsDeployReqInfo *reqInfo)
{
  {
    synchronized block(m_mutex);
    checkForRefresh(context, reqInfo, true);
  }

  if (!m_pEnvXml)
  {
    Owned<IPropertyTree> pEnvTree = getEnvTree(context, reqInfo);

    m_pEnvXml.set(new SCMStringBuffer());
    toXML(pEnvTree, m_pEnvXml->s, false);
  }

  m_pGraphXml.set(new SCMStringBuffer());
  xsltTransform(m_pEnvXml->str(), StringBuffer(getCFD()).append("xslt/graph_env.xslt").str(), NULL, m_pGraphXml->s);
}

//---------------------------------------------------------------------------
//  onGraph
//---------------------------------------------------------------------------
bool CWsDeployFileInfo::graph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp)
{
  synchronized block(m_mutex);

  if (!m_pGraphXml)
    generateGraph(context, NULL);
  resp.setGraphContainer(m_pGraphXml->str());
  return true;
}

void CWsDeployFileInfo::getNavigationData(IEspContext &context, IPropertyTree* pData)
{
  try 
  { 
    synchronized block(m_mutex);

    if (m_envFile.length())
    {
      if (!m_userWithLock.length())
      {
        context.getUserID(m_userWithLock);
        context.getPeer(m_userIp);
      }
      else
      {
        StringBuffer sbName, sbUserIp;
        context.getUserID(sbName);
        context.getPeer(sbUserIp);
        if (strcmp(sbName.str(), m_userWithLock.str()) || strcmp(sbUserIp.str(), m_userIp.str()))
          throw MakeStringException(-1, "A user on machine %s is accessing the file. Please try again later.", m_userIp.str());
      }

      initFileInfo(false);
    }

    if (!m_pNavTree)
    {
      if (!m_pEnvXml)
      {
        Owned<IPropertyTree> pEnvTree = getEnvTree(context, NULL);
        m_pEnvXml.set(new SCMStringBuffer());
        toXML(pEnvTree, m_pEnvXml->s, false);
      }

      m_pNavTree.setown(getEnvTree(context, NULL));
    }
    pData->setProp("@viewType", "tree");
    mergePTree(pData, m_pNavTree);
  }
  catch(IException*e)
  {
    StringBuffer sErrMsg;
    e->errorMessage(sErrMsg);
    e->Release();
  }
  catch(...)
  {
  }
}

void CWsDeployFileInfo::saveEnvironment(IEspContext* pContext, IConstWsDeployReqInfo *reqInfo, StringBuffer& errMsg, bool saveAs)
{
  if (m_envFile.length())
  {
    Owned<IPropertyTree> pEnvRoot;
    StringBuffer valerrs;

    try
    {
      if (!saveAs || (saveAs && m_Environment))
      {
        pEnvRoot.setown(&m_Environment->getPTree());
        validateEnv((IConstEnvironment*)m_Environment, false);
      }
      else if (saveAs)
      {
        setSkipNotification(true);
        pEnvRoot.setown(&m_constEnvRdOnly->getPTree());
        validateEnv(m_constEnvRdOnly, false);
      }
    }
    catch(IException* e)
    {
      e->errorMessage(valerrs);
      e->Release();
    }

    //save and write to backup
    StringBuffer sXML;
    StringBuffer tmp;
    StringBuffer dtStr;
    CDateTime dt;

    dt.setNow();
    dt.getString(tmp.clear());
    sXML.appendf("<"XML_HEADER">\n<!-- Edited with ConfigMgr on ip %s on %s -->\n", m_userIp.str(), tmp.str()); 
    toXML(pEnvRoot, sXML, 0, XML_SortTags | XML_Format);

    if (m_bCloud && pContext)
    {
      StringBuffer sbName, sbUserIp, sbMsg;
      if (reqInfo)
        sbName.clear().append(reqInfo->getUserId());
      pContext->getPeer(sbUserIp);
      CCloudActionHandler saveCloud(this, CLOUD_SAVE_ENV, CLOUD_ROLLBACK_ENV, sbName.str(), "8015", NULL);
      StringBuffer tick;
      tick.appendf("%d", msTick());
      saveCloud.setSaveActionParams(sXML.str(), tick.str());
      bool ret = saveCloud.start(sbMsg);

      if (!ret || sbMsg.length())
        throw MakeStringException(0, "Environment could not be successfully saved. Reason(s):\n%s", sbMsg.str());
    }

    try
    {
      if (m_pFile.get())
      {
        StringBuffer sb;
        if (!checkDirExists(m_pService->getBackupDir()))
          recursiveCreateDirectory(m_pService->getBackupDir());

        while(true)
        {
          String strEnvFile(m_envFile);
          int idx = strEnvFile.lastIndexOf('/');
          if (idx <= 0)
            idx = strEnvFile.lastIndexOf('\\');
          
          String* tmpstr = strEnvFile.substring(idx+1);
          sb.clear().append(m_pService->getBackupDir()).append(PATHSEPCHAR).append(*tmpstr);
          delete tmpstr;
          dt.setNow();
          tmp.clear();
          dtStr.clear();
          dt.getDateString(tmp, true);
          tmp.append("_");
          dt.getTimeString(tmp, true);
          dtStr.append(".").append(tmp);
          dtStr.replaceString("-","_");
          dtStr.replaceString(":","_");
          
          String ext(sb);
          idx = ext.lastIndexOf(PATHSEPCHAR);
          if(ext.indexOf('.', idx > 0 ? idx : 0) != -1)
            sb.insert(ext.lastIndexOf('.'), dtStr.str());
          else
            sb.append(dtStr.str());

          if (checkFileExists(sb))
            continue;
          else
          {
            Owned<IFile> pFile(createIFile(sb.str()));
            setSkipNotification(true);
            copyFile(pFile, m_pFile, 0x100000);
            break;
          }
        }
      }
    }
    catch(IException* e)
    {
      //ignore any attempts to create the backup
      e->Release();
    }

    if (!m_pFile.get())
      m_pFile.setown(createIFile(m_envFile));

    m_pFileIO.clear();
    m_pFileIO.setown(m_pFile->open(IFOcreaterw));

    m_pFileIO->write(0, sXML.length(), sXML.str());
    m_lastSaved.clear();
    m_lastSaved.setNow();

    //reset the readonly tree
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    m_constEnvRdOnly.setown(factory->loadLocalEnvironment(sXML.str()));

    if (valerrs.length())
      errMsg.appendf("CWsDeployFileInfo::saveEnvironment:Save operation was successful. However the following exceptions were raised.\n%s", valerrs.str());
  }
  else
  {
    // JAKESMITH->SMEDA - apparently this code is legacy and can be deleted, please do + clearup related code if any
    try
    {
      m_Environment->commit();
      StringBuffer response;
      initClusterGroups(false, response, NULL);
      if (response.length())
        PROGLOG("CWsDeployFileInfo::saveEnvironment: %s", response.str());
    }
    catch (IException* e)
    {
      StringBuffer sErrMsg;
      e->errorMessage(sErrMsg);
      e->Release();         

      /*typical error message when lock fails is as follows:
      SDS: Lock timeout
      SDS Reply Error  : SDS: Lock timeout
      Failed to establish lock to NewEnvironment/
      Existing lock status: Locks on path: /NewEnvironment/
      Endpoint            |SessionId       |ConnectionId    |mode    

      172.16.48.175:7254  |c00000038       |c0000003b       |653     
      */

      //if we can extract IP address of computer holding the lock then
      //show a customized message.
      //
      //Retrieve IP address of computer holding the lock...
      char achHost[128] = "";
      const char* p = strstr(sErrMsg.str(), "\n\n");
      if (p && *(p+=2))
      {
        const char* q = strchr(p, ':');
        if (q)
        {
          const int len = q-p;
          strncpy(achHost, p, len);
          achHost[len] = '\0';
        }
      }

      //resolve hostname for this IP address
      unsigned int addr = inet_addr(achHost);
      struct hostent* hp = gethostbyaddr((const char*)&addr, sizeof(addr), AF_INET);

      if (hp)
        strcpy(achHost, hp->h_name);

      StringBuffer sMsg;
      sMsg.appendf("The environment definition in dali server "
        "could not be opened for write access");

      if (achHost[0])
        sMsg.appendf(" \nbecause it is locked by computer '%s'.", achHost);
      else
        sMsg.append(":\n\n").append(sErrMsg);

      throw MakeStringException(0, "%s", sMsg.str());
    }
  }

   CConfigFileMonitorThread::getInstance()->addObserver(*this);
}

void CWsDeployFileInfo::unlockEnvironment(IEspContext* context, IConstWsDeployReqInfo *reqInfo, const char* xmlArg, StringBuffer& sbErrMsg, bool saveEnv)
{
  if (m_bCloud)
  {
    StringBuffer sbMsg;
    Owned<IPropertyTree> pComputers = createPTreeFromXMLString(xmlArg);
    Owned<IPropertyTree> unlockComputers = createPTree("ComputerList");
    Owned<IPropertyTree> lockComputers = createPTree("ComputerList");
    Owned<IPropertyTreeIterator> iter;
    StringBuffer xpath;
    if (pComputers && pComputers->numChildren() && m_lockedNodesBeforeEnv.get() != NULL)
    {
      expandRange(pComputers);
      iter.setown(pComputers->getElements("Computer"));
      ForEach (*iter)
      {
        IPropertyTree* pComputer = &iter->query();
        xpath.clear().appendf(XML_TAG_COMPUTER"["XML_ATTR_NETADDRESS"='%s']", pComputer->queryProp(XML_ATTR_NETADDRESS));

        if (!m_lockedNodesBeforeEnv->queryPropTree(xpath.str()))
          lockComputers->addPropTree(XML_TAG_COMPUTER, createPTreeFromIPT(pComputer));
      }
    }

    if (!pComputers && m_lockedNodesBeforeEnv.get() != NULL)
      unlockComputers.set(m_lockedNodesBeforeEnv);
    else if (m_lockedNodesBeforeEnv.get() != NULL)
    {
      iter.setown(m_lockedNodesBeforeEnv->getElements("Computer"));
      ForEach (*iter)
      {
        IPropertyTree* pComputer = &iter->query();
        xpath.clear().appendf(XML_TAG_COMPUTER"["XML_ATTR_NETADDRESS"='%s']", pComputer->queryProp(XML_ATTR_NETADDRESS));

        if (!pComputers->queryPropTree(xpath.str()))
          unlockComputers->addPropTree(XML_TAG_COMPUTER, createPTreeFromIPT(pComputer));
      }
    }

    CCloudActionHandler unlockCloud(this, CLOUD_UNLOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", unlockComputers->numChildren() ? unlockComputers : NULL);
    bool ret = unlockCloud.start(sbMsg);

    if (!ret || sbMsg.length())
    {
      if (saveEnv)
        sbErrMsg.append("Save operation is successful. However, ");
      sbErrMsg.appendf("Write access to the Environment cannot be revoked. Reason(s):\n%s", sbMsg.str());
      return;
    }
    else 
    {
      Owned<IPropertyTreeIterator> iter = unlockComputers->getElements("Computer");
      StringBuffer xpath;
      ForEach (*iter)
      {
        IPropertyTree* pComputer = &iter->query();
        xpath.clear().appendf(XML_TAG_COMPUTER"["XML_ATTR_NETADDRESS"='%s']", pComputer->queryProp(XML_ATTR_NETADDRESS));

        IPropertyTree* pDelComputer = m_lockedNodesBeforeEnv->queryPropTree(xpath.str());
        m_lockedNodesBeforeEnv->removeTree(pDelComputer);
      }
    }
      
    if (lockComputers->numChildren())
    {
      CCloudActionHandler lockCloud(this, CLOUD_LOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", lockComputers);
      ret = lockCloud.start(sbMsg.clear());

      if (!ret || sbMsg.length())
      {
        if (saveEnv)
          sbErrMsg.append("Save operation is successful. However, ");
        sbErrMsg.appendf("Write access to the Environment could not be obtained. Reason(s):\n%s", sbMsg.str());
        return;
      }
    }
  }

  m_Environment.clear();

  StringBuffer sb(m_userWithLock);
  sb.append(m_userIp);

  CClientAliveThread* th = m_keepAliveHTable.getValue(sb.str());
  if (th)
  {
    th->Release();
    m_keepAliveHTable.remove(sb.str());
  }

  if (m_envFile.length() == 0)
  {
    Owned<IGroup> serverGroup = createIGroup(m_daliServer.str(), m_daliServerPort);

    if (!serverGroup)
      throw MakeStringException(0, "Could not instantiate IGroup");

    if (!initClientProcess(serverGroup, DCR_Config, 0, NULL, NULL, 10000))
      throw MakeStringException(0, "Could not initialize the client process");

    m_pSubscription.clear();
    m_pSubscription.setown( new CSdsSubscription(this) );
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    m_constEnvRdOnly.setown(factory->openEnvironment());
  }

  m_constEnvRdOnly->clearCache();
  m_userWithLock.clear();
  m_userIp.clear();

  Owned<IPropertyTree>  pNavTree(getEnvTree(*context, reqInfo));

  if (!areMatchingPTrees(pNavTree, m_pNavTree))
  {
    m_pEnvXml.clear();
    m_pGraphXml.clear();
    m_pNavTree.clear();
    m_pNavTree.setown(getEnvTree(*context, reqInfo));
  }
}

void CWsDeployFileInfo::checkForRefresh(IEspContext &context, IConstWsDeployReqInfo *reqInfo, bool checkWriteAccess)
{
  StringBuffer sbUser, sbIp;
  if (reqInfo)
    sbUser.clear().append(reqInfo->getUserId());
  context.getPeer(sbIp);

  if (checkWriteAccess)
  {
      if (!m_Environment || m_userWithLock.length() == 0 || m_userIp.length() == 0)
        throw MakeStringException(-1, "Cannot modify environment as it is currently in readonly mode");
      else if (m_userWithLock.length() != 0 && m_userIp.length() != 0 && (strcmp(m_userWithLock.str(), sbUser.str()) || strcmp(m_userIp.str(), sbIp.str())))
        throw MakeStringException(-1, "Cannot modify setting as environment is currently in use on machine '%s'", m_userIp.str());
  }
 
}

IPropertyTree* CWsDeployFileInfo::queryComputersForCloud()
{
  Owned<IPropertyTree> pEnvTree = m_Environment?&m_Environment->getPTree():&m_constEnvRdOnly->getPTree();
  return pEnvTree->queryPropTree(XML_TAG_HARDWARE);
}

void CCloudTaskThread::main()
{
  static Mutex m;
  m.lock();
  try
  {
    m_pTask->makeSoapCall();
  }
  catch(IException* e)
  {
    e->Release();
  }
  m.unlock();
}

CCloudTask* createCloudTask(CCloudActionHandler* pHandler, EnvAction eA, const char* ip)
{
  return new CCloudTask(pHandler, eA, ip);
}

bool CWsDeployFileInfo::updateEnvironment(const char* xml)
{
  if (!xml || !*xml)
    return false;

  Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
  m_constEnvRdOnly.setown(factory->loadLocalEnvironment(xml));
  return true;
}

bool CWsDeployFileInfo::isLocked(StringBuffer& sbUser, StringBuffer& sbIp)
{
  if (m_userIp.length() != 0 && m_userWithLock.length() != 0)
  {
    sbUser.clear().append(m_userWithLock);
    sbIp.clear().append(m_userIp);
    return true;
  }

  return false;
}


bool CWsDeployFileInfo::buildEnvironment(IEspContext &context, IEspBuildEnvironmentRequest &req, IEspBuildEnvironmentResponse &resp)
{
  synchronized block(m_mutex);
  resp.setStatus("false");
  const char* xml =  req.getXmlArgs();
 
  StringBuffer sbName, sbUserIp, msg, envXml;
  sbName.clear().append(req.getReqInfo().getUserId());
  context.getPeer(sbUserIp);

  if(m_userWithLock.length() > 0)
  {
    if(!strcmp(sbName.str(), m_userWithLock.str()) || !strcmp(sbUserIp.str(), m_userIp.str()))
    { 
        buildEnvFromWizard(xml, m_pService->getName(), m_pService->getCfg(), envXml);
        setSkipNotification(true);

        if(envXml.length())
        {
          resp.setStatus("true");
          if(m_Environment != NULL)
          {
            m_Environment.clear();
          }
          Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
          m_Environment.setown(factory->loadLocalEnvironment(envXml.str()));
        }
        else
          resp.setMessage("Failed to generated the environment xml for unknown reason");
    }
    else{
      sbName.append("Cannot generate environment, As environment is locked by ").append(m_userWithLock);
      resp.setMessage(sbName.str());
    }
  }
  else
   resp.setMessage("Environment has not been locked.");
  return true;
}

bool CWsDeployFileInfo::getSubnetIPAddr(IEspContext &context, IEspGetSubnetIPAddrRequest &req, IEspGetSubnetIPAddrResponse &resp)
{
  synchronized block(m_mutex);
  resp.setIPList("");
  resp.setMessage("");

  StringBuffer ipList, msg, script;
  StringBuffer xpath ;

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile",m_pService->getName());

  const char* pConfFile = m_pService->getCfg()->queryProp(xpath.str());

  if( pConfFile && *pConfFile)
  {
      Owned<IProperties> pParams = createProperties(pConfFile);
      Owned<IPropertyIterator> iter = pParams->getIterator();
      pParams->getProp("autodetectipscript", script);
      if( script.length())
      {
          runScript(ipList, msg, script.str());
          if( ipList.length())
          {
            try {
                 ipList.replace('\n', ';');
                 validateIPS(ipList.str());
                 resp.setIPList(ipList);
            }
            catch(IException* e)
            {
               StringBuffer sb;
               e->errorMessage(sb);
               e->Release();
               resp.setMessage(sb.str());
            }
          }
          else if( msg.length())
          {
             resp.setMessage(msg);
          }
      }
      else
         resp.setMessage("Could not find the autodetectipscript entry in configmgr.conf");
   }
   else
       resp.setMessage("Unknown Exception. Could not get the List of IPAddresses"); 
  return true;
}

void CWsDeployFileInfo::CLockerAliveThread::main()
{
  while (!m_quitThread)
  {
    try
    {
      StringBuffer sbMsg;
      CCloudActionHandler checkLocker(this->m_pFileInfo, CLOUD_CHECK_LOCKER, CLOUD_NONE, m_user.str(), "8015", m_pComputers);
      bool ret = checkLocker.start(sbMsg);

      String str(sbMsg.str());
      if (str.startsWith("SOAP Connection error"))
      {
        m_pFileInfo->activeUserNotResponding();
        m_quitThread = true;
        break;
      }

      m_sem.wait(m_brokenConnTimeout);
    }
    catch(IException* e)
    {
      if (m_pFileInfo && m_quitThread != true)
        m_pFileInfo->activeUserNotResponding();

      m_quitThread = true;
      e->Release();
    }
  }
}

const char* getFnString(EnvAction ea)
{
    const char* ret = NULL;
    switch(ea)
    {
    case CLOUD_LOCK_ENV:
        ret = "LockEnvironmentForCloud";
        break;
    case CLOUD_UNLOCK_ENV:
        ret = "UnlockEnvironmentForCloud";
        break;
    case CLOUD_SAVE_ENV:
        ret = "SaveEnvironmentForCloud";
        break;
    case CLOUD_ROLLBACK_ENV:
        ret = "RollbackEnvironmentForCloud";
        break;
    case CLOUD_NOTIFY_INITSYSTEM:
        ret = "NotifyInitSystemSaveEnvForCloud";
        break;
    case CLOUD_CHECK_LOCKER:
        ret = "GetValue";
        break;
    }

    return ret;
}

bool CWsDeployFileInfo::getSummary(IEspContext &context, IEspGetSummaryRequest &req, IEspGetSummaryResponse &resp)
{
  synchronized block(m_mutex);
  StringBuffer respXmlStr;
  resp.setStatus("false");
  bool link = req.getPrepareLinkFlag();
  Owned<IPropertyTree> pEnvRoot = getEnvTree(context, &req.getReqInfo());
  ::getSummary(pEnvRoot, respXmlStr, link);
  if(respXmlStr.length())
  {
    resp.setStatus("true");
    resp.setXmlStr(respXmlStr.str());
  }
  return true;
}

void CWsDeployFileInfo::initFileInfo(bool createOrOverwrite, bool bClearEnv)
{
  StringBuffer xpath;
  m_skipEnvUpdateFromNotification = false;
  m_activeUserNotResp = false;
  bool fileExists = true;
  StringBuffer sbxml;

  if (!checkFileExists(m_envFile) || createOrOverwrite)
  {
    fileExists = false;
    StringBuffer s("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Environment></Environment>");
    Owned<IPropertyTree> pNewTree = createPTreeFromXMLString(s);

    if ( strlen(m_pService->m_configHelper.getBuildSetFilePath()) > 0 )
    {
        try
        {
          Owned<IPropertyTree> pDefBldSet = createPTreeFromXMLFile( m_pService->m_configHelper.getBuildSetFilePath() );
          pNewTree->addPropTree(XML_TAG_PROGRAMS, createPTreeFromIPT(pDefBldSet->queryPropTree("./Programs")));
          pNewTree->addPropTree(XML_TAG_SOFTWARE, createPTreeFromIPT(pDefBldSet->queryPropTree("./Software")));
        }
        catch(IException* e)
        {
          e->Release();
        }
    }

    if(!pNewTree->queryPropTree(XML_TAG_SOFTWARE))
    {
      pNewTree->addPropTree(XML_TAG_SOFTWARE, createPTree());
      pNewTree->addPropTree("./Software/Directories", createPTreeFromXMLString(DEFAULT_DIRECTORIES));
    }
    pNewTree->addPropTree(XML_TAG_HARDWARE, createPTree());

    if (!pNewTree->queryPropTree(XML_TAG_PROGRAMS))
      pNewTree->addPropTree(XML_TAG_PROGRAMS, createPTree());

    toXML(pNewTree, s.clear());
    sbxml.clear().append(s);

    if (createOrOverwrite)
    {
      if (m_pFileIO.get() != NULL)
        m_pFileIO.clear();

      if (m_pFile.get() != NULL)
      {
        m_pFile->remove();
        m_pFile.clear();
      }
      
      if (m_lastSaved.isNull())
        m_lastSaved.setNow();

      recursiveCreateDirectoryForFile(m_envFile);
      Owned<IFile> f = createIFile(m_envFile);
      Owned<IFileIO> fio = f->open(IFOcreaterw);
      fio->write(0,s.length(),s.str());
      fileExists = true;
    }
  }

  if (fileExists)
  {
    if (m_pFile.get() != NULL)
      m_pFile.clear();
    if (m_pFileIO.get() != NULL)
      m_pFileIO.clear();
    if (m_lastSaved.isNull())
      m_lastSaved.setNow();

    m_pFile.setown(createIFile(m_envFile));
    m_pFileIO.setown(m_pFile->open(IFOread));
  
    {
      Owned <IPropertyTree> pTree = createPTree(*m_pFileIO);
      toXML(pTree, sbxml.clear());
    }
  }

  Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
  m_Environment.setown(factory->loadLocalEnvironment(sbxml.str()));

  //add env 
  bool modified = false;
  Owned<IPropertyTree> pEnvRoot = &m_Environment->getPTree();
  IPropertyTree* pSettings = pEnvRoot->queryPropTree(XML_TAG_ENVSETTINGS);

  if (!pSettings)
  {
    pSettings = pEnvRoot->addPropTree(XML_TAG_ENVSETTINGS, createPTree());
    modified = true;
  }

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile", m_pService->getName());
  const char* tmp = m_pService->getCfg()->queryProp(xpath.str());
  if (tmp && *tmp)
  {
    Owned<IProperties> pParams = createProperties(tmp);
    Owned<IPropertyIterator> iter = pParams->getIterator();

    ForEach(*iter)
    {
      StringBuffer prop;
      pParams->getProp(iter->getPropKey(), prop);
      const char* val = pSettings->queryProp(iter->getPropKey());

      if (!val || strcmp(val, prop.str()))
      {
        pSettings->setProp(iter->getPropKey(), prop.length() ? prop.str():"");
        modified = true;
      }
    }

    if (updateDirsWithConfSettings(pEnvRoot, pParams))
      modified = true;
  }

  try
  {
    StringBuffer err;
    if (modified && fileExists)
      saveEnvironment(NULL, NULL, err);
  }
  catch (IErrnoException* e)
  {
    //Don't ignore file access exceptions
    throw e;
  }
  catch(IException* e)
  {
    //ignore any exceptions at this point like validation errors e.t.c
    e->Release();
  }

  if (!fileExists)
    toXML(pEnvRoot, sbxml.clear());

  if ( bClearEnv == true )
    m_Environment.clear();

  if (m_constEnvRdOnly.get() == NULL)
  {
    if (fileExists)
    {
      sbxml.clear();
      Owned <IPropertyTree> pTree = createPTree(*m_pFileIO);
      toXML(pTree, sbxml);
    }

    m_constEnvRdOnly.setown(factory->loadLocalEnvironment(sbxml.str()));
  }
}

void CWsDeployFileInfo::CSdsSubscription::notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
{
  DBGLOG("Environment was updated by another client of Dali server.  Invalidating cache.\n");
  //if (id != sub_id)
  m_pFileInfo->environmentUpdated();
}

void CWsDeployExCE::getLastStarted(StringBuffer& sb) 
{ 
  synchronized block(m_mutexSrv);
  m_lastStarted.getString(sb);
}

CWsDeployFileInfo::~CWsDeployFileInfo()
{
  if (m_bCloud && m_userWithLock.length() && m_userIp.length())
  {
    StringBuffer sbMsg;
    CCloudActionHandler unlockCloud(this, CLOUD_UNLOCK_ENV, CLOUD_NONE, m_userWithLock.str(), "8015", m_lockedNodesBeforeEnv.get()? m_lockedNodesBeforeEnv.get(): NULL);
    unlockCloud.start(sbMsg);
  }

  CWsDeployFileInfo::CConfigFileMonitorThread::getInstance()->removeObserver(*this);
  m_pNavTree.clear();
  m_pGraphXml.clear();
  m_Environment.clear();
  m_constEnvRdOnly.clear();
  m_pSubscription.clear();
  m_pEnvXml.clear();
  m_pFile.clear();
  m_pFileIO.clear();
  m_lockedNodesBeforeEnv.clear();
  m_pGenJSFactoryThread.clear();
  m_keepAliveHTable.kill();
}

bool CWsDeployEx::onNavMenuEvent(IEspContext &context, 
                                 IEspNavMenuEventRequest &req, 
                                 IEspNavMenuEventResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  const char* cmd = req.getCmd();

  if (!strcmp(cmd, "ValidateEnvironment") || !strcmp(cmd, "SaveEnvironment") || !strcmp(cmd, "SaveEnvironmentAs"))
  {
    synchronized block(m_mutexSrv);
    return fi->navMenuEvent(context, req, resp);
  }
  else
    return fi->navMenuEvent(context, req, resp);
}

bool CWsDeployEx::onSaveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  return fi->saveSetting(context, req, resp);
}

bool CWsDeployEx::onGetNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp)
{
  const char* xmlArg = req.getXmlArgs();
  StringBuffer decodedParams(xmlArg);
  decodedParams.replaceString("::", "\n");
  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());
  const char* create = pParams->queryProp("createFile");

  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true, create && *create ? true : false);
  return fi->getNavTreeDefn(context, req, resp);
}

bool CWsDeployEx::onLockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->lockEnvironmentForCloud(context, req, resp);
}

bool CWsDeployEx::onUnlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->unlockEnvironmentForCloud(context, req, resp);
}

bool CWsDeployEx::onSaveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->saveEnvironmentForCloud(context, req, resp);
}

bool CWsDeployEx::onRollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->rollbackEnvironmentForCloud(context, req, resp);
}

bool CWsDeployEx::onNotifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->notifyInitSystemSaveEnvForCloud(context, req, resp);
}

bool CWsDeployExCE::onGetValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp)
{
  //Check for common properties here
  StringBuffer decodedParams(req.getParams());

  if (decodedParams.length() == 0)
  {
    resp.setReqValue("");
    resp.setStatus("true");
    return true;
  }

  decodedParams.replaceString("::", "\n");

  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());
  const char* pszQueryType = pParams->queryProp("queryType");
  const char* pszparams = pParams->queryProp("params");
  const char* pszAttrValue = pParams->queryProp("attrValue");
  StringBuffer xpath;
  const char* pszValue = NULL;
  StringBuffer sbMultiple;

  bool allHandled = true;

  if (pszQueryType && !strcmp(pszQueryType, "customType"))
  {
    sbMultiple.clear();
    if(pszparams && *pszparams)
    {
      StringArray sArray;
      sArray.appendList(pszparams, ",");
      for(unsigned i = 0; i < sArray.ordinality() ; i++)
      {
        if(!strcmp(sArray.item(i), "environment"))
        {
          xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvFile", getName());
          const char* pEnvFile = getCfg()->queryProp(xpath.str());
          if (pEnvFile && *pEnvFile)
          {
            if (checkFileExists(pEnvFile))
              sbMultiple.append(sArray.item(i)).append("=true");
            else
              sbMultiple.append(sArray.item(i)).append("=false");

            if(sbMultiple.length())
             sbMultiple.append(",");
          }
        }
        else if(!strcmp(sArray.item(i), "username"))
        {
          StringBuffer sbName;
          sbName.clear().append(msTick());
          sbMultiple.append(sArray.item(i)).append("=").append(sbName);
    
          if(sbMultiple.length())
            sbMultiple.append(",");
        }
        else if(!strcmp(sArray.item(i), "defenvfile"))
        {
          if (strstr(m_envFile.str(), m_sourceDir.str()))
          {
            StringBuffer sb(m_envFile.str() + m_sourceDir.length() + 1);
            sbMultiple.append(sArray.item(i)).append("=").append(sb.str());

            if(sbMultiple.length())
              sbMultiple.append(",");
          }
        }
        else if(!strcmp(sArray.item(i), "checklock"))
        {
          //StringBuffer sb(m_sourceDir);
          //if (sb.charAt[sb.length() - 1] != PATHSEPCHAR)
          //  sb.append(PATHSEPCHAR);
          //sb.append(req.getFileName());
          //if (checkFileExists(sb.str()))
            allHandled = false;
        }
        else if(!strcmp(sArray.item(i), "encryptpassword"))
        {
          if(pszAttrValue && *pszAttrValue)
          {
            StringArray sArray;
            sArray.appendList(pszAttrValue, ",");
            ForEachItemIn(x, sArray)
            {
              StringBuffer encryptedPasswd ;
              encrypt(encryptedPasswd , sArray.item(x));
              sbMultiple.append(encryptedPasswd.str()); 
                if(sbMultiple.length())
                 sbMultiple.append(",");
            }
          }
        }
        else if(!strcmp(sArray.item(i), "lastsaved"))
        {
          allHandled = false;
        }
        else if(!strcmp(sArray.item(i), "laststarted"))
        {
          StringBuffer lastStarted;
          getLastStarted(lastStarted);
          if(lastStarted.length())
            sbMultiple.append(sArray.item(i)).append("=").append(lastStarted);
          else
            sbMultiple.append(sArray.item(i)).append("=''");

          if(sbMultiple.length())
               sbMultiple.append(",");
        }
        else if(!strcmp(sArray.item(i), "wizops"))
        {
          StringBuffer sbOps;
          this->getWizOptions(sbOps);
          sbMultiple.append(sArray.item(i)).append("=").append(sbOps);
    
          if(sbMultiple.length())
            sbMultiple.append(",");
        }
      }
    }
    pszValue = sbMultiple.str();
  }
  else if (pszQueryType && !strcmp(pszQueryType, "sourceEnvironments"))
  {
    sbMultiple.clear();
    Owned<IFile> pDir = createIFile(getSourceDir());

    StringBuffer activeConfig_md5sum, config_md5sum;
    md5_filesum(STANDARD_CONFIG_STAGED_PATH, activeConfig_md5sum);

    if (pDir->exists())
    {
      if (pDir->isDirectory())
      {
        Owned<IDirectoryIterator> it = pDir->directoryFiles(NULL, false, true);
        ForEach(*it)
        {               
          StringBuffer name;
          it->getName(name);

          String str(name.toLowerCase());
          if (str.endsWith(".xml"))
          {
            StringBuffer sb(getSourceDir());
            sb.append(PATHSEPCHAR).append(it->getName(name.clear()));
            try
            {
              Owned<IPropertyTree> pTree = createPTreeFromXMLFile(sb.str());
              StringBuffer testFile;

              testFile.clear().appendf("%s/%s",getSourceDir(),it->getName(name.clear()).str());
              md5_filesum(testFile.str(),config_md5sum.clear());

              if (pTree && pTree->queryName() && !strcmp(XML_TAG_ENVIRONMENT, pTree->queryName()))
              {
                if(strcmp(config_md5sum.str(),activeConfig_md5sum.str())==0)
                {
                  sbMultiple.append("<StagedConfiguration>").append(";");
                  sbMultiple.append(it->getName(name.clear())).append(";");
                  sbMultiple.append("</StagedConfiguration>").append(";");
                }
                else
                {
                  sbMultiple.append(it->getName(name.clear())).append(";");
                }
              }
            }
            catch(IException* e)
            {
              e->Release();
              //add any files already in use
              CWsDeployFileInfo* fi = m_fileInfos.getValue(name.str());
              if (fi)
                sbMultiple.append(name).append(";");
            }
          }
        }
      }
    }

    pszValue = sbMultiple.str();
  }
  else if (pszQueryType && *pszQueryType)
    allHandled = false;
  
  if (allHandled)
  {
    resp.setReqValue(pszValue);
    resp.setStatus("true");
    return true;
  }
  else
  {
    CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
    bool ret = fi->getValue(context, req, resp);
    if (ret)
    {
      const char* val = resp.getReqValue();
      StringBuffer sb;
      if (pszValue)
        sb.append(pszValue);
      sb.append(val);
      resp.setReqValue(sb.str());
      resp.setStatus("true");
    }

    return ret;
  }
}

bool CWsDeployExCE::onUnlockUser(IEspContext &context, IEspUnlockUserRequest &req, IEspUnlockUserResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->unlockUser(context, req, resp);
}

bool CWsDeployExCE::onClientAlive(IEspContext &context, IEspClientAliveRequest &req, IEspClientAliveResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  return fi->clientAlive(context, req, resp);
}

bool CWsDeployExCE::onGetEnvironment(IEspContext &context, IEspGetEnvironmentRequest &req, IEspGetEnvironmentResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(m_envFile.str());
  return fi->getEnvironment(context, req, resp);
}

bool CWsDeployExCE::onSetEnvironment(IEspContext &context, IEspSetEnvironmentRequest &req, IEspSetEnvironmentResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(m_envFile.str());
  return fi->setEnvironment(context, req, resp);
}

bool CWsDeployEx::onDisplaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->displaySettings(context, req, resp);
}

bool CWsDeployEx::onStartDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->startDeployment(context, req, resp);
}

bool CWsDeployEx::onGetDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->getDeployableComps(context, req, resp);
}

bool CWsDeployEx::onGetComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->getComputersForRoxie(context, req, resp);
}

bool CWsDeployEx::onHandleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleRoxieOperation(context, req, resp);
}

bool CWsDeployEx::onGetBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->getBuildSetInfo(context, req, resp);
}

bool CWsDeployEx::onImportBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->importBuild(context, req, resp);
}

bool CWsDeployEx::onGetBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->getBuildServerDirs(context, req, resp);
}

bool CWsDeployEx::onHandleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleComponent(context, req, resp);
}

bool CWsDeployEx::onHandleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleAttributeAdd(context, req, resp);
}

bool CWsDeployEx::onHandleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleAttributeDelete(context, req, resp);
}

bool CWsDeployEx::onHandleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleInstance(context, req, resp);
}

bool CWsDeployEx::onHandleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleEspServiceBindings(context, req, resp);
}

bool CWsDeployEx::onHandleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleComputer(context, req, resp);
}

bool CWsDeployEx::onHandleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleTopology(context, req, resp);
}

bool CWsDeployEx::onHandleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleThorTopology(context, req, resp);
}

bool CWsDeployEx::onHandleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->handleRows(context, req, resp);
}

bool CWsDeployEx::onGraph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp)
{
  CWsDeployFileInfo* fi = getFileInfo(m_envFile.str());
  return fi->graph(context, req, resp);
}

void CWsDeployExCE::getNavigationData(IEspContext &context, IPropertyTree* pData)
{
  CWsDeployFileInfo* fi = getFileInfo(m_envFile.str());
  return fi->getNavigationData(context, pData);
}

bool CWsDeployExCE::onBuildEnvironment(IEspContext &context, IEspBuildEnvironmentRequest &req, IEspBuildEnvironmentResponse &resp)
{
  String sb(req.getReqInfo().getFileName());
  StringBuffer sbnew(req.getReqInfo().getFileName());
  String* tmp = sb.toLowerCase();
  if (!tmp->endsWith(".xml"))
    sbnew.appendf(".xml");
  delete tmp;
  CWsDeployFileInfo* fi = getFileInfo(sbnew.str());
  return fi->buildEnvironment(context, req, resp);
}

bool CWsDeployExCE::onGetSubnetIPAddr(IEspContext &context, IEspGetSubnetIPAddrRequest &req, IEspGetSubnetIPAddrResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->getSubnetIPAddr(context, req, resp);
}

bool CWsDeployExCE::onGetSummary(IEspContext &context, IEspGetSummaryRequest &req, IEspGetSummaryResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  return fi->getSummary(context, req, resp);
}

bool CWsDeployExCE::onAddReqdComps(IEspContext &context, IEspAddReqdCompsRequest &req, IEspAddReqdCompsResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  return fi->addReqdComps(context, req, resp);
}

bool CWsDeployEx::onDeploy(IEspContext &context, IEspDeployRequest& req, IEspDeployResponse& resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName());
  return fi->deploy(context, req, resp);
}

CWsDeployFileInfo* CWsDeployExCE::getFileInfo(const char* fileName, bool addIfNotFound, bool createFile)
{
  synchronized block(m_mutexSrv);
  if (!fileName || !*fileName)
    throw MakeStringException(-1, "File name required for operation");

  CWsDeployFileInfo* fi = m_fileInfos.getValue(fileName);

  if (!fi)
  {
    if (addIfNotFound)
    {
      StringBuffer filePath(m_sourceDir);
      if (filePath.charAt(filePath.length() - 1) != PATHSEPCHAR)
        filePath.append(PATHSEPCHAR);
      filePath.append(fileName);
      fi = new CWsDeployFileInfo(this, filePath, m_bCloud);
      const char* psz = strrchr(fileName, PATHSEPCHAR);
      if (!psz)
        psz = strrchr(fileName, PATHSEPCHAR == '\\' ? '/' : '\\');
      StringBuffer sb;
      if (!psz)
        sb.append(fileName);
      else
        sb.append(psz + 1);
      if (!sb.length())
        sb.append(fileName);

      try
      {
        fi->initFileInfo(createFile);
        CWsDeployFileInfo::CConfigFileMonitorThread::getInstance()->addObserver(*fi);
      }
      catch (IException* e)
      {
        delete fi;
        throw e;
      }

      m_fileInfos.setValue(sb.str(), fi);
    }
    else
      throw MakeStringException(-1, "File information not found for %s", fileName);
  }
  else if (createFile)
  {
    StringBuffer sbuser, sbip;
    if (fi->getUserWithLock(sbuser, sbip))
      throw MakeStringException(-1, "Cannot overwrite file '%s' as it is currently locked by user '%s' on machine '%s'", fileName, sbuser.str(), sbip.str());
    else
    {
      try
      {
        fi->initFileInfo(createFile);
      }
      catch (IException* e)
      {
        m_fileInfos.remove(fileName);
        delete fi;
        throw e;
      }
    }
  }

  return fi;
}

bool CWsDeployFileInfo::getUserWithLock(StringBuffer& sbUser, StringBuffer& sbIp)
{
  if (m_userWithLock.length() && m_userIp.length())
  {
    sbUser.clear().append(m_userWithLock);
    sbIp.clear().append(m_userIp);
    return true;
  }

  return false;
}

bool CWsDeployExCE::onNavMenuEvent(IEspContext &context, 
                                 IEspNavMenuEventRequest &req, 
                                 IEspNavMenuEventResponse &resp)
{
  CWsDeployFileInfo* fi = getFileInfo(req.getReqInfo().getFileName(), true);
  const char* cmd = req.getCmd();

  if (!strcmp(cmd, "LockEnvironment") || !strcmp(cmd, "UnlockEnvironment"))
    return fi->navMenuEvent(context, req, resp);
  else
    return supportedInEEOnly();
}

bool CWsDeployExCE::onSaveSetting(IEspContext &context, IEspSaveSettingRequest &req, IEspSaveSettingResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGetNavTreeDefn(IEspContext &context, IEspGetNavTreeDefnRequest &req, IEspGetNavTreeDefnResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onLockEnvironmentForCloud(IEspContext &context, IEspLockEnvironmentForCloudRequest &req, IEspLockEnvironmentForCloudResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onUnlockEnvironmentForCloud(IEspContext &context, IEspUnlockEnvironmentForCloudRequest &req, IEspUnlockEnvironmentForCloudResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onSaveEnvironmentForCloud(IEspContext &context, IEspSaveEnvironmentForCloudRequest &req, IEspSaveEnvironmentForCloudResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onRollbackEnvironmentForCloud(IEspContext &context, IEspRollbackEnvironmentForCloudRequest &req, IEspRollbackEnvironmentForCloudResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onNotifyInitSystemSaveEnvForCloud(IEspContext &context, IEspNotifyInitSystemSaveEnvForCloudRequest &req, IEspNotifyInitSystemSaveEnvForCloudResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onDisplaySettings(IEspContext &context, IEspDisplaySettingsRequest &req, IEspDisplaySettingsResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onStartDeployment(IEspContext &context, IEspStartDeploymentRequest &req, IEspStartDeploymentResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGetDeployableComps(IEspContext &context, IEspGetDeployableCompsRequest &req, IEspGetDeployableCompsResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGetBuildServerDirs(IEspContext &context, IEspGetBuildServerDirsRequest &req, IEspGetBuildServerDirsResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onImportBuild(IEspContext &context, IEspImportBuildRequest &req, IEspImportBuildResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGetComputersForRoxie(IEspContext &context, IEspGetComputersForRoxieRequest &req, IEspGetComputersForRoxieResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleRoxieOperation(IEspContext &context, IEspHandleRoxieOperationRequest &req, IEspHandleRoxieOperationResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleThorTopology(IEspContext &context, IEspHandleThorTopologyRequest &req, IEspHandleThorTopologyResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleComponent(IEspContext &context, IEspHandleComponentRequest &req, IEspHandleComponentResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleAttributeAdd(IEspContext &context, IEspHandleAttributeAddRequest &req, IEspHandleAttributeAddResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleAttributeDelete(IEspContext &context, IEspHandleAttributeDeleteRequest &req, IEspHandleAttributeDeleteResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleInstance(IEspContext &context, IEspHandleInstanceRequest &req, IEspHandleInstanceResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleEspServiceBindings(IEspContext &context, IEspHandleEspServiceBindingsRequest &req, IEspHandleEspServiceBindingsResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleComputer(IEspContext &context, IEspHandleComputerRequest &req, IEspHandleComputerResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleTopology(IEspContext &context, IEspHandleTopologyRequest &req, IEspHandleTopologyResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onHandleRows(IEspContext &context, IEspHandleRowsRequest &req, IEspHandleRowsResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGraph(IEspContext &context, IEspEmptyRequest& req, IEspGraphResponse& resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onDeploy(IEspContext &context, IEspDeployRequest& req, IEspDeployResponse& resp)
{
  return supportedInEEOnly();
}

bool CWsDeployExCE::onGetBuildSetInfo(IEspContext &context, IEspGetBuildSetInfoRequest &req, IEspGetBuildSetInfoResponse &resp)
{
  return supportedInEEOnly();
}

bool CWsDeployEx::onGetValue(IEspContext &context, IEspGetValueRequest &req, IEspGetValueResponse &resp)
{
  //Check for common properties here
  StringBuffer decodedParams(req.getParams());

  if (decodedParams.length() == 0)
  {
    resp.setReqValue("");
    resp.setStatus("true");
    return true;
  }

  decodedParams.replaceString("::", "\n");

  Owned<IProperties> pParams = createProperties();
  pParams->loadProps(decodedParams.str());
  const char* pszQueryType = pParams->queryProp("queryType");
  const char* pszparams = pParams->queryProp("params");
  const char* pszAttrValue = pParams->queryProp("attrValue");
  StringBuffer xpath;
  const char* pszValue = NULL;
  StringBuffer sbMultiple;

  bool allHandled = true;

  if (pszQueryType && !strcmp(pszQueryType, "customType"))
  {
    sbMultiple.clear();
    if(pszparams && *pszparams)
    {
      StringArray sArray;
      sArray.appendList(pszparams, ",");
      for(unsigned i = 0; i < sArray.ordinality() ; i++)
      {
        if(!strcmp(sArray.item(i), "wizops"))
        {
          StringBuffer sbOps;
          this->getWizOptions(sbOps);
          sbMultiple.append(sArray.item(i)).append("=").append(sbOps);
    
          if(sbMultiple.length())
            sbMultiple.append(",");
        }
        else 
          allHandled = false;
      }
    }
    pszValue = sbMultiple.str();
  }
  else if (pszQueryType && *pszQueryType)
    allHandled = false;
  
  if (allHandled)
  {
    resp.setReqValue(pszValue);
    resp.setStatus("true");
    return true;
  }
  else
  {
    bool ret = CWsDeployExCE::onGetValue(context, req, resp);
    if (ret)
    {
      const char* val = resp.getReqValue();
      StringBuffer sb;
      if (pszValue)
        sb.append(pszValue);
      sb.append(val);
      resp.setReqValue(sb.str());
      resp.setStatus("true");
    }

    return ret;
  }
}

void CWsDeployExCE::getWizOptions(StringBuffer& sb)
{
  sb.clear().append("1");
}

void CWsDeployEx::getWizOptions(StringBuffer& sb)
{
  sb.clear().append("3");
}

CWsDeployExCE* createWsDeployEE(IPropertyTree *cfg, const char* name)
{
  return new CWsDeployEx;
}

CWsDeployExCE* createWsDeployCE(IPropertyTree *cfg, const char* name)
{
  StringBuffer sb;
  sb.append(XML_TAG_SOFTWARE"/"XML_TAG_ESPPROCESS"/"XML_TAG_ESPBINDING"/");
  sb.appendf("["XML_ATTR_SERVICE"='%s']", name);
  IPropertyTree* pTree = cfg->queryPropTree(sb.str());
  const char* ver = "CE";
  if (pTree)
    ver = pTree->queryProp(XML_ATTR_VERSION);

  if (ver && *ver && !strcmp(ver, "EE"))
    return new CWsDeployEx;
  
  return new CWsDeployExCE;
}

bool CWsDeployFileInfo::checkForRequiredComponents(IPropertyTree* pEnvRoot, const char* ip,
                                                   StringBuffer& reqdCompNames, const char* buildSet, bool autoadd/*=false*/)
{
  StringBuffer prop, prop2, xpath, genEnvConf;
  Owned<IProperties> algProps;
  StringArray compOnAllNodes,compExcludeOnAllNodes;
  bool retVal = true;
  bool bExclude = false;

  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile", m_pService->getName());
  const char* pConfFile = m_pService->getCfg()->queryProp(xpath.str());
  xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile", m_pService->getName());
  const char* pEnvConfFile = m_pService->getCfg()->queryProp(xpath.str());

  if (pConfFile && *pConfFile && pEnvConfFile && *pEnvConfFile)
  {
    Owned<IProperties> pParams = createProperties(pConfFile);
    Owned<IProperties> pEnvParams = createProperties(pEnvConfFile);
    const char* genenv = pParams->queryProp("wizardalgorithm");

    if (genenv && *genenv)
    {
      const char* cfgpath = pEnvParams->queryProp("configs");

      if (!cfgpath || !*cfgpath)
        cfgpath = CONFIG_DIR;

      genEnvConf.clear().append(cfgpath);

      if (genEnvConf.charAt(genEnvConf.length() - 1) != PATHSEPCHAR)
        genEnvConf.append(PATHSEPCHAR);

      genEnvConf.append(genenv);

      if(checkFileExists(genEnvConf.str()))
        algProps.setown(createProperties(genEnvConf.str()));
      else
        throw MakeStringException( -1 , "The algorithm file %s does not exists", genEnvConf.str());

      algProps->getProp("comps_on_all_nodes", prop);
      algProps->getProp("exclude_from_comps_on_all_nodes", prop2);

      compOnAllNodes.appendList(prop.str(), ",");
      compExcludeOnAllNodes.appendList(prop2.str(), ",");

      for (unsigned i = 0; buildSet != NULL && i < compExcludeOnAllNodes.ordinality(); i++)
      {
        if (strcmp(compExcludeOnAllNodes.item(i),buildSet) == 0)
        {
          bExclude = true;
          break;
        }
      }

      const char* flag = pParams->queryProp("autoaddallnodescomp");

      if (!autoadd)
        autoadd = (flag && *flag == '1') ? true : false;
    }
  }

  for(unsigned i = 0; bExclude == false && i < compOnAllNodes.ordinality(); i++)
  {
    xpath.clear().appendf("./Programs/Build/BuildSet[@name=\"%s\"]", compOnAllNodes.item(i));
    Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
    buildSetIter->first();
    IPropertyTree* pBuildSet;

    if (buildSetIter->isValid())
      pBuildSet = &buildSetIter->query();
    else
      continue;

    const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
    xpath.clear().appendf("./Software/%s[1]", processName);
    IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());

    if (!pCompTree)
    {
      if (autoadd)
      {
        StringBuffer buildSetPath, sbNewName;
        Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, m_Environment);
        xpath.clear().appendf("./Software/%s[@name='%s']", processName, compOnAllNodes.item(i));
        pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, pBuildSet->queryProp(XML_ATTR_NAME), m_pService->getCfg(), m_pService->getName(), false);
        IPropertyTree* pInstTree = pCompTree->queryPropTree(XML_TAG_INSTANCE);

        if (pInstTree)
          pCompTree->removeTree(pInstTree);

        addComponentToEnv(pEnvRoot, compOnAllNodes.item(i), sbNewName, pCompTree);
      }
      else
      {
        reqdCompNames.appendf("%s\n", compOnAllNodes.item(i));
        retVal = false;
        continue;
      }
    }

    xpath.clear().appendf(XML_TAG_INSTANCE"["XML_ATTR_NETADDRESS"='%s']", ip);
    IPropertyTree* pInst = pCompTree->queryPropTree(xpath.str());

    if (!pInst)
    {
      if (autoadd)
      {
        StringBuffer sb, sbl, compName, nodeName;
        xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_HARDWARE, XML_TAG_COMPUTER,
          XML_ATTR_NETADDRESS, ip);
        IPropertyTree* computer = pEnvRoot->queryPropTree(xpath.str());

        if(computer)
        {
          nodeName.clear().append(computer->queryProp(XML_ATTR_NAME));
          xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, processName, XML_ATTR_BUILDSET, compOnAllNodes.item(i));
          sb.clear().appendf("<Instance buildSet=\"%s\" compName=\"%s\" ><Instance name=\"%s\" /></Instance>",
            compOnAllNodes.item(i), pCompTree->queryProp(XML_ATTR_NAME), nodeName.str());
          Owned<IPropertyTree> pInstance = createPTreeFromXMLString(sb.str());
          addInstanceToCompTree(pEnvRoot, pInstance, sbl.clear(), sb.clear(), NULL);
        }
      }
      else
      {
        reqdCompNames.appendf("%s\n", compOnAllNodes.item(i));
        retVal = false;
        continue;
      }
    }
  }

  return retVal;
}
