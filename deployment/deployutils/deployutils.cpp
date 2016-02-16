/*##############################################################################

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
############################################################################## */
// DeployUtils.cpp : Defines the exported functions for the DLL application.
//
#include "deployutils.hpp"
#include "XMLTags.h"
#include "jliball.hpp"
#include "buildset.hpp"
#include "computerpicker.hpp"
#include "configenvhelper.hpp"
#include "configengcallback.hpp"
#include "xslprocessor.hpp"
#include "jwrapper.hpp"
#include "wizardInputs.hpp"
#include "build-config.h"
#include "confighelper.hpp"

#define TRACE_SCHEMA_NODE(msg, schemaNode)

#define CONFIGMGR_JSPATH "./"
#define STANDARD_COMPFILESDIR INSTALL_DIR

#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml"

static bool schemaNodeHasAttributes(IPropertyTree* pNode)
{
  //skip over xs:complexType, if any
  IPropertyTree* pTemp = pNode->queryPropTree(XSD_TAG_COMPLEX_TYPE);
  if (pTemp)
    pNode = pTemp;

  Owned<IPropertyTreeIterator> itAttr = pNode->getElements(XSD_TAG_ATTRIBUTE);
  return itAttr  ->first() && itAttr  ->isValid();
}

static bool schemaNodeHasAttributeGroups(IPropertyTree* pNode)
{
  //skip over xs:complexType, if any
  IPropertyTree* pTemp = pNode->queryPropTree(XSD_TAG_COMPLEX_TYPE);
  if (pTemp)
    pNode = pTemp;

  Owned<IPropertyTreeIterator> itAttrGr = pNode->getElements(XSD_TAG_ATTRIBUTE_GROUP);
  return itAttrGr->first() && itAttrGr->isValid();
}

bool writeToFile(const char* fileName, StringBuffer sb)
{
  StringBuffer jsName(fileName);
  recursiveCreateDirectoryForFile(fileName);
  Owned<IFile> pFile = createIFile(jsName);
  Owned<IFileIO> pFileIO = pFile->open(IFOcreaterw);
  pFileIO->write(0, sb.length(), sb.str());

  return true;
}

//check if this has any child elements - returns first element
IPropertyTree* schemaNodeHasElements(IPropertyTree* pNode)
{
  //skip over xs:complexType, if any
  IPropertyTree* pTemp = pNode->queryPropTree(XSD_TAG_COMPLEX_TYPE);
  if (pTemp)
    pNode = pTemp;

  //skip over xs:sequence, if any
  pTemp = pNode->queryPropTree(XSD_TAG_SEQUENCE);
  if (pTemp)
    pNode = pTemp;
  else
  {
    pTemp = pNode->queryPropTree(XSD_TAG_CHOICE);
    if (pTemp)
      pNode = pTemp;
  }

  Owned<IPropertyTreeIterator> it = pNode->getElements(XSD_TAG_ELEMENT);
  if (it->first() && it->isValid())
    pTemp = &it->query();
  else
    pTemp = NULL;

  return pTemp;
}

const char* getRealTabName(const char* tabName)
{
  if (!strcmp(tabName, XML_TAG_INSTANCE))
    return XML_TAG_INSTANCES;
  else if (!strcmp(tabName, XML_TAG_DOMAIN))
    return "Domains";
  else if (!strcmp(tabName, TAG_COMPUTERTYPE))
    return "Type";
  else if (!strcmp(tabName, XML_TAG_COMPUTERTYPE))
    return "Computer Types";
  else if (!strcmp(tabName, XML_TAG_COMPUTER))
    return "Computers";
  else if (!strcmp(tabName, XML_TAG_SWITCH))
    return "Switches";
  else
    return tabName;
}

//Gets the list of installed components by looking at the directories
void getInstalledComponents(const char* pszInstallDir, StringBuffer& sbOutComps, StringBuffer& sbOutEspServices, StringBuffer& sbOutPlugins, const IPropertyTree* pEnv)
{
  StringBuffer sbDir;
  sbOutComps.clear();
  sbOutEspServices.clear();
  sbOutPlugins.clear();
  if (pszInstallDir && *pszInstallDir)
    sbDir.append(pszInstallDir);
  else
    sbDir.append(COMPONENTFILES_DIR"/configxml");

  bool getFromDirs = false;
  if (getFromDirs)
  {
    Owned<IFile> inFiles = NULL;

    try
    {
      inFiles.setown(createIFile(sbDir.str()));
      if(!inFiles->exists())
      {
        printf("Input directory %s does not exist", sbDir.str());
        return;
      }
    }
    catch(IException* e)
    {
      StringBuffer errmsg;
      e->errorMessage(errmsg);
      printf("Error when trying to access source directory.Error: %s ", errmsg.str());
      e->Release();
      return;
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
            dirName.clear().append(sbDir);
            if(dirName.charAt(dirName.length() - 1) != PATHSEPCHAR)
              dirName.append(PATHSEPCHAR);
            dirName.append(compName);

            fileName.clear().append(dirName).append(PATHSEPCHAR).append("deploy_map.xml");
            Owned<IFile> depfile(createIFile(fileName));
            if(depfile->exists())
            {
              Owned<IPropertyTree> pInstallSet = createPTreeFromXMLFile(fileName);
              const char* szDeployable = pInstallSet->queryProp("@deployable");
              const char* szProcessName = pInstallSet->queryProp("@processName");
              if (!szDeployable || strcmp(szDeployable, "no"))
              {
                const char* szOveride = pInstallSet->queryProp("@overide");
                if(!szOveride || strcmp(szOveride, "no") !=0 )
                {
                  if (sbOutComps.length())
                    sbOutComps.append(",");
                  sbOutComps.append("'").append(compName).append("'");
                }
              }
              else if (!strcmp(szProcessName, XML_TAG_ESPSERVICE))
              {
                if (sbOutEspServices.length())
                  sbOutEspServices.append(",");
                sbOutEspServices.append("'").append(compName).append("'");
              }
              else if (!strcmp(szProcessName, XML_TAG_PLUGINPROCESS))
              {
                if (sbOutPlugins.length())
                  sbOutPlugins.append(",");
                sbOutPlugins.append("'").append(compName).append("'");
              }
            }
            else
            {
              if (!strcmp(compName.str(), "plugins"))
              {
                StringBuffer sb;
                getInstalledComponents(dirName.str(), sb, sb, sbOutPlugins, pEnv);
              }
            }
          }
        }
      }
    }
  }
  else
  {
    Owned<IPropertyTreeIterator> iter = CConfigHelper::getInstance() != NULL ? CConfigHelper::getInstance()->getBuildSetTree()->getElements("Programs/Build[1]/*") : pEnv->getElements("Programs/Build[1]/*");

    ForEach(*iter)
    {
      IPropertyTree* pBuildSet = &iter->query();
      const char* szName = pBuildSet->queryProp(XML_ATTR_NAME);
      const char* szProcessName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

      if (szProcessName && !strcmp(szProcessName, XML_TAG_ESPSERVICE))
      {
        if (sbOutEspServices.length())
          sbOutEspServices.append(",");
        sbOutEspServices.append("'").append(szName).append("'");
      }
      else if (szProcessName && !strcmp(szProcessName, XML_TAG_PLUGINPROCESS))
      {
        if (sbOutPlugins.length())
          sbOutPlugins.append(",");
        sbOutPlugins.append("'").append(szName).append("'");
      }
      else 
      { 
        if (!szName || !*szName)
          continue;

        const char* szOveride = pBuildSet->queryProp("@overide");
        if(!szOveride || strcmp(szOveride, "no") !=0 )
        {
           if (sbOutComps.length())
              sbOutComps.append(",");
           sbOutComps.append("'").append(szName).append("'");
        }
      }
    }
  }
}

void LoadComboBox(const char* szPath, bool bAddBlank, const IPropertyTree* pNode, const IPropertyTree* pParentNode, StringBuffer& sbComboBox, bool appendParentName = false, bool addDeclStart = true, bool addDeclEnd = true)
{
  if (addDeclStart)
    sbComboBox.append("new Array(");

  if (bAddBlank) 
    sbComboBox.append("''");

  const char* buildSet = NULL;
  if (!strncmp(szPath, "$process", 8))
  {
    szPath += strlen("$process");
    if (*szPath == '\0')
    {
      const char* szName = pNode->queryProp(XML_ATTR_NAME);
      if (szName && *szName)
      {
        if (bAddBlank)
          sbComboBox.append(",");
        sbComboBox.appendf("'%s'", szName);
      }
      return;
    }
    szPath++; //skip over '/'
  }
  else
  {
    if (pParentNode && !strcmp(szPath, "Programs/Build"))
      buildSet = pParentNode->queryProp(XML_ATTR_BUILDSET);
  }

  Owned<IPropertyTreeIterator> iter = pNode->getElements(szPath);
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
      {
        if (sbComboBox.length() > 10)
          sbComboBox.append(",");
        
        if (!appendParentName)
          sbComboBox.appendf("'%s'", szName);
        else
          sbComboBox.appendf("'%s/%s'", pParentNode->queryProp(XML_ATTR_NAME), szName);
      }
    }
  }

  if (addDeclEnd)
    sbComboBox.append(")");
}

void addItem(StringBuffer& jsStrBuf, 
             const IPropertyTree* pEnv, 
             const char* tabName, 
             const char* attrName, 
             const char* tip, 
             bool hidden, 
             bool required, 
             const char* extra, 
             short ctrlType)
{
  StringBuffer sbAttr("Attributes");

  jsStrBuf.appendf("var attr%s%s = {};", attrName, tabName);
  jsStrBuf.appendf("attr%s%s.tab = '%s';", attrName, tabName, *tabName ? getRealTabName(tabName): sbAttr.str());
  jsStrBuf.appendf("attr%s%s.tip = '%s';", attrName, tabName, tip);
  jsStrBuf.appendf("attr%s%s.hidden = %d;", attrName, tabName, hidden);
  jsStrBuf.appendf("attr%s%s.required = 1;", attrName, tabName);
  jsStrBuf.appendf("attr%s%s.ctrlType = %d;", attrName, tabName, ctrlType);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", attrName, tabName, attrName, tabName);

  StringBuffer sb;

  if (ctrlType == 4)
  {
    if (extra[0] != '|')
      LoadComboBox(extra, false, pEnv, pEnv, sb);
    else 
      sb.append(++extra);

    jsStrBuf.appendf("attr%s%s.extra = %s;", attrName, tabName, sb.str());
  }
}

void addTopologyType(StringBuffer& jsStrBuf, const IPropertyTree* pEnv, const char* tabName, const char* attrName, const char* tip, bool hidden, bool required, const char* extra, short ctrlType)
{
  jsStrBuf.appendf("var attr%s%s = {};", attrName, tabName);
  jsStrBuf.appendf("attr%s%s.tab = '%s';", attrName, tabName, "Topology");
  jsStrBuf.appendf("attr%s%s.tip = '%s';", attrName, tabName, tip);
  jsStrBuf.appendf("attr%s%s.hidden = %d;", attrName, tabName, hidden);
  jsStrBuf.appendf("attr%s%s.required = 1;", attrName, tabName);
  jsStrBuf.appendf("attr%s%s.ctrlType = %d;", attrName, tabName, ctrlType);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", attrName, tabName, attrName, tabName);

  StringBuffer sb;

  if (!strcmp(attrName, TAG_BUILD))
  {
    sb.append("new Array(");
    Owned<IPropertyTreeIterator> iBuild = pEnv->getElements("Programs/Build[@name]");
    ForEach (*iBuild)
    {
      IPropertyTree* pBuild = &iBuild->query();

      if (pBuild->queryPropTree("BuildSet[@name='topology']"))
      {
        const char* szName = pBuild->queryProp(XML_ATTR_NAME);

        if (szName && *szName) 
        {
          if (sb.length() > 10)
            sb.append(",");
          sb.appendf("'%s'", szName);
        }
      }
    }

    sb.append(")");
    jsStrBuf.appendf("attr%s%s.extra = %s;", attrName, tabName, sb.str());
  }
  else if (ctrlType == 4)
  {
    if (extra[0] != '|')
      LoadComboBox(extra, false, pEnv, pEnv, sb);
    else 
      sb.append(++extra);

    jsStrBuf.appendf("attr%s%s.extra = %s;", attrName, tabName, sb.str());
  }
}

const char* GetDisplayProcessName(const char* processName, char* buf)
{
  //produces "LDAPServerProcess" as "LDAP Server" and "EspService" as "Esp Service", etc.
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

void GetDisplayName(IPropertyTree* pNode, StringBuffer& sb, bool bAppendProcessName)
{
  // Get the display name for the node
  // Use szBuf because CString was too slow when loading a large tree
  static char szBuf[128];
  size32_t cnt = sizeof(szBuf);
  GetDisplayProcessName(pNode->queryName(), szBuf);

  const char* szName = pNode->queryProp(XML_ATTR_NAME);
  if (!szName || !*szName) 
    szName = pNode->queryProp(XML_ATTR_PROCESS);

  if (bAppendProcessName)
  {
    if (szName && *szName)
    {
      cnt -= strlen(szName);
      strncat(szBuf, " - ", cnt);
      strncat(szBuf, szName, cnt - 3); 
    }

    sb.clear().append(szBuf);
  }
  else
    sb.clear().append(szName);

}

class CGenerateJSFromXSD
{
public:
  CGenerateJSFromXSD(const IPropertyTree* pEnv, const char* xsdName, const char* jsName):
      m_xsdName(xsdName), m_jsName(jsName), m_pCompTree(NULL), m_pSchemaRoot(NULL),m_pDefTree(NULL),m_numAttrs(0),m_allSubTypes(true),m_genOptional(true)
      {
        m_pEnv.set(pEnv);
        m_colIndex.append("var colIndex = new Array();");
        m_columns.append("var tabCols = new Array();");
      }
      CGenerateJSFromXSD(const IPropertyTree* pEnv, IPropertyTree* pSchemaRoot, const char* jsName, const char* compName):
      m_pSchemaRoot(pSchemaRoot), m_jsName(jsName), m_compName(compName),m_pCompTree(NULL), m_pDefTree(NULL),m_numAttrs(0),m_allSubTypes(true),m_wizFlag(false),m_wizard(NULL),m_genOptional(true)
      {
        m_pEnv.set(pEnv);
        m_colIndex.append("var colIndex = new Array();");
        m_columns.append("var tabCols = new Array();");
      }

      void setNameInCompTabArray(const char* tabName, const char* nodeName)
      {
        if (m_tabNameArray.find(tabName) == NotFound)
        {
          m_tabNameArray.append(tabName);
          m_jsStrBuf.appendf("compTabs['%s'][compTabs['%s'].length]= '%s';", m_compName.str(), m_compName.str(), tabName);
          if (nodeName && *nodeName)
            m_jsStrBuf.appendf("compTabToNode['%s'] = '%s';", tabName, nodeName);
          m_columns.appendf("tabCols['%s'] = new Array();", tabName);
        }
      }

      void setNameInHiddenTabArray(const char* tabName)
      {
        if (m_hiddenTabNameArray.find(tabName) == NotFound)
        {
          m_hiddenTabNameArray.append(tabName);
          m_jsStrBuf.appendf("hiddenTabs['%s'][hiddenTabs['%s'].length]= '%s';", m_compName.str(), m_compName.str(), tabName);
        }
      }

      void addRoxieMisc(StringBuffer& jsStrBuf)
      {
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_SERVER, TAG_COMPUTER, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_SERVER, TAG_PROCESS, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_NAME, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_PROCESS, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_LISTENQUEUE, "", 0, 1, "", 1);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_NUMTHREADS, "", 0, 1, "", 1);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_PORT, "", 0, 1, "", 1);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, TAG_REQARRAYTHREADS, "", 0, 1, "", 1);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_FARM, "aclName", "", 0, 1, "|'#$process/ACL'", 4);

        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_ONLY_SLAVE, TAG_NAME, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_ONLY_SLAVE, TAG_COMPUTER, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_ONLY_SLAVE, TAG_NETADDRESS, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_ONLY_SLAVE, TAG_ITEMTYPE, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_CHANNEL, TAG_NAME, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_CHANNEL, TAG_ITEMTYPE, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_CHANNEL, TAG_COMPUTER, "", 0, 1, "", 0);
        addItem(jsStrBuf, m_pEnv.get(), XML_TAG_ROXIE_CHANNEL, TAG_NUMBER, "", 0, 1, "", 0);
      }

      void addMisc()
      {
        if (!strcmp(m_compName.str(), "RoxieCluster"))
        {
          addRoxieMisc(m_jsStrBuf);
          const char* serverStr = "Servers";

          short index = 0;
          m_colIndex.appendf("colIndex['computer%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['process%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['netAddress%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['port%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['listenQueue%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['numThreads%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['requestArrayThreads%s']=%d;", serverStr, index++);
          m_colIndex.appendf("colIndex['aclName%s']=%d;", serverStr, index++);

          index = 0;
          const char* agentStr = "Agents";
          m_colIndex.appendf("colIndex['computer%s']=%d;", agentStr, index++);
          m_colIndex.appendf("colIndex['netAddress%s']=%d;", agentStr, index++);
        }
        else if (!strcmp(m_compName.str(), XML_TAG_THORCLUSTER))
        {
          short index = 0;
          m_jsStrBuf.append("compTabs['ThorCluster'][compTabs['ThorCluster'].length]= 'Topology';");
          m_colIndex.appendf("colIndex['nameTopology']=%d;", index++); 
          m_colIndex.appendf("colIndex['processTopology']=%d;", index++);
          m_colIndex.appendf("colIndex['netAddressTopology']=%d;", index++);
          m_jsStrBuf.append("compTabToNode['Topology']= 'Topology';");
        }
      }

      void CreateAttributeFromSchema(IPropertyTree& attr, StringBuffer compName, const char* tabName, const char* childElementName)
      {
        StringBuffer attrname;
        StringBuffer combovalues;
        StringBuffer strBuf;
        StringBuffer aName;
        StringBuffer value, tempPath, wizDefVal;
        attrname.append(attr.queryProp(XML_ATTR_NAME));
        
        const char *use = attr.queryProp("@use");
        if (!m_genOptional && use && *use && !strcmp(use, "optional"))
        {
          if(childElementName)
          {
            StringBuffer xpath;
            xpath.clear().append(childElementName);
            IPropertyTree* pChild = m_pCompTree->queryPropTree(xpath.str());

            if(!pChild)
              pChild = m_pCompTree->addPropTree(childElementName, createPTree());
          }

          return;
        }
        
        if(m_wizFlag)
        {
          if(attr.hasProp("./xs:annotation/xs:appinfo/autogenforwizard"))
          {
            value.clear().append(attr.queryProp("./xs:annotation/xs:appinfo/autogenforwizard"));
            if(!strcmp(value.str(),"1"))
            {
                    getValueForTypeInXSD(attr, compName, wizDefVal);  
            }
          }
          else
            return ;
        }

        if (childElementName)
          attrname.append(childElementName);

        aName.appendf("a%d", m_numAttrs++);

        m_jsStrBuf.appendf("var %s = {};", aName.str());
        m_jsStrBuf.appendf("%s.tab = '%s';", aName.str(), getRealTabName(tabName));
        setNameInCompTabArray(getRealTabName(tabName), childElementName);

        IPropertyTree* pField = NULL;
        if (m_pDefTree)
        {
          IPropertyTree* pProcess = m_pDefTree->queryPropTree(compName.str());

          if (!pProcess)
            pProcess = m_pDefTree->addPropTree(compName, createPTree());

          IPropertyTree* pTab = m_pDefTree->queryPropTree(getRealTabName(tabName));

          if (!pTab)
            pTab = pProcess->addPropTree(getRealTabName(tabName), createPTree());
          
          pField = pTab->addPropTree("Field", createPTree());
        }

        const char *defaultValue = attr.queryProp("@default");
        StringBuffer sbdefaultValue;
        if (defaultValue)
        {
          sbdefaultValue.clear().append(defaultValue);
          sbdefaultValue.replaceString("\\", "\\\\");
          m_jsStrBuf.appendf("%s.defaultValue = '%s';", aName.str(), sbdefaultValue.str());

          if (pField)
            pField->addProp(UI_FIELD_ATTR_DEFAULTVALUE, sbdefaultValue.str());
        }

        if(wizDefVal.length() > 0)
        {
          sbdefaultValue.clear().append(wizDefVal);
          if (pField)
           pField->addProp(UI_FIELD_ATTR_DEFAULTVALUE, sbdefaultValue.str());
        }

        if (m_pCompTree)
        {
          StringBuffer xpath;
          if(!childElementName)
          {
            xpath.clear().append("@").append(attrname);
            m_pCompTree->addProp(xpath, sbdefaultValue.str());
          }
          else
          {
            xpath.clear().append(childElementName);
            IPropertyTree* pChild = m_pCompTree->queryPropTree(xpath.str());

            if(!pChild)
              pChild = m_pCompTree->addPropTree(childElementName, createPTree());

            xpath.clear().append("@").append(attr.queryProp(XML_ATTR_NAME));
            pChild->addProp(xpath, sbdefaultValue.str());
          }
        }

        IPropertyTree* pAppInfo = attr.queryPropTree("xs:annotation/xs:appinfo");
        const char *viewtype;
        const char *displayMode = NULL;
        if (pAppInfo)
        {
          const char* caption = pAppInfo->queryProp("title");
          if (caption)
            m_jsStrBuf.appendf("%s.caption = '%s';", aName.str(), caption);

          const char* tip = pAppInfo->queryProp("tooltip");
          if (tip)
          {
            StringBuffer sbtip(tip);
            sbtip.replaceString("\"", "\\\"");
            sbtip.replaceString("\'", "\\\'");
            m_jsStrBuf.appendf("%s.tip = '%s';", aName.str(), sbtip.str());
          }

          m_jsStrBuf.appendf("%s.width = %d;", aName.str(), pAppInfo->getPropInt("width", 90));

          viewtype = pAppInfo->queryProp("viewType");
          m_jsStrBuf.appendf("%s.hidden = %d;", aName.str(), viewtype && !strcmp(viewtype, "hidden"));

          displayMode = pAppInfo->queryProp("displayMode");
          m_jsStrBuf.appendf("%s.displayMode = %d;", aName.str(), displayMode && !strcmp(displayMode, "simple"));

          const char* colIndex = pAppInfo->queryProp("colIndex");
          
          if (colIndex && *colIndex)
          {
            int i = atoi(colIndex);
            m_colIndex.appendf("colIndex['%s%s'] = %d;", attr.queryProp(XML_ATTR_NAME),getRealTabName(tabName), i - 1);

            if (!viewtype || (viewtype && strcmp(viewtype, "hidden")))
            {
              m_columns.appendf("tabCols['%s'][%d] = '%s';", getRealTabName(tabName), i - 1, caption? caption:attr.queryProp(XML_ATTR_NAME));

              if (childElementName && i == 1 && m_splitterTabName.length())
              {
                setNameInCompTabArray(m_splitterTabName, compName.str());
                m_columns.appendf("tabCols['%s'][%d] = '_%s';", m_splitterTabName.str(), m_numAttrs, tabName);
              }
            }
          }
          else if (childElementName && m_splitterTabName.length())
          {
            m_colIndex.appendf("colIndex['%s%s'] = %d;", attr.queryProp(XML_ATTR_NAME),getRealTabName(tabName), 0);

            if (!viewtype || (viewtype && strcmp(viewtype, "hidden")))
            {
              m_columns.appendf("tabCols['%s'][%d] = '%s';", getRealTabName(tabName), 0, caption? caption:attr.queryProp(XML_ATTR_NAME));

              setNameInCompTabArray(m_splitterTabName, compName.str());
              m_columns.appendf("tabCols['%s'][%d] = '_%s';", m_splitterTabName.str(), m_numAttrs, tabName);
            }
          }

          IPropertyTree* onChangeNode = pAppInfo->queryPropTree("onchange");
          if (onChangeNode)
          {
            const char* msg = onChangeNode->queryProp("message");
            if (msg && *msg)
            {
              StringBuffer sbmsg(msg);
              sbmsg.replace('\n',' ');
              sbmsg.replaceString("  ", " ");
              m_jsStrBuf.appendf("%s.onChange = 1;", aName.str());
              m_jsStrBuf.appendf("%s.onChangeMsg = '%s';", aName.str(), sbmsg.str());
            }

            const char* onChangeXslt = onChangeNode->queryProp("xslt");
            if (onChangeXslt)
              m_jsStrBuf.appendf("%s.onChange = 2;", aName.str());
          }
          else
            m_jsStrBuf.appendf("%s.onChange = %d;", aName.str(), onChangeNode != NULL);
        }
        else
        {
          viewtype = NULL;
        }

        StringBuffer xpath(m_xpathDefn);
        xpath.appendf("[@%s]", attr.queryProp(XML_ATTR_NAME));
        if (viewtype)
        {
          if (pField)
          {
            pField->addProp(attr.queryProp(XML_ATTR_NAME), m_pEnv->queryProp(xpath.str()));
          }
        }
        else
        {
          if (pField)
          {
            pField->addProp(UI_FIELD_ATTR_NAME, attr.queryProp(XML_ATTR_NAME));
            pField->addProp(UI_FIELD_ATTR_NAME"Type", "0");
            pField->addProp(UI_FIELD_ATTR_VALUE, m_pEnv->queryProp(xpath.str()));
          }
        }

        m_jsStrBuf.appendf("%s.required = %d;", aName.str(), (use && strcmp(use, "required")==0) || (pAppInfo && pAppInfo->getPropBool("required")));
        m_jsStrBuf.appendf("%s.loadRoot = %d;", aName.str(),1);

        const char *type = attr.queryProp("@type");
        const char *extraInfo = NULL;
        bool bAddBlank = false;

        StringBuffer typeNameSpace;
        StringBuffer typeName;
        int nCtrlType = 1;//LVC_EDIT;
        if (viewtype && !strcmp(viewtype, "readonly"))
          nCtrlType = 0;//LVC_NONE;
        else if (viewtype && !strcmp(viewtype, TAG_PASSWORD))
          nCtrlType = 5;//LVC_EDITPASSWORD;
        else if (type)
        {
          while (*type && *type!=':')
            typeNameSpace.append(*type++);

          if (*type)
          {
            type++;
            while (*type)
              typeName.append(*type++);
          }
          else
          {
            typeName.append(typeNameSpace);
            typeNameSpace.clear();
          }
          type = typeName.str();
          if (strcmp(typeNameSpace.str(),"xs")==0)
          {
            if (strcmp(type, "string")==0)
              nCtrlType = 1;//LVC_EDIT;
            else if (strcmp(type, "boolean")==0)
            {
              nCtrlType = 4;//ret->m_bRequired ? LVC_TRUEFALSE : LVC_TRUEFALSE2;
              strBuf.clear().append("new Array('false','true');");
              extraInfo = strBuf.str();
            }
          }
          else if (strcmp(typeNameSpace.str(),"seisint")==0 || typeNameSpace.length()==0)
          {
            bAddBlank = !((use && strcmp(use, "required")==0) || (pAppInfo && pAppInfo->getPropBool("required")));
            if (strcmp(type, "commonDirsCompType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              StringBuffer compList, espServiceList, pluginsList;
              getInstalledComponents(NULL, compList, espServiceList, pluginsList, m_pEnv.get());
              strBuf.clear().append("new Array(").append(compList).append(");");
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "commonDirsInstType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              strBuf.clear().append("new Array('');");
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "daliServersType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/DaliServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "securityManagerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/*/[@type=\"SecurityManager\"]", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataBuildType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/Build", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataModelType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/Model", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataThorTableType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/$model/ThorTable", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataTableType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/$parentmodel/*", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "sybaseType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/SybaseProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
              //ret->m_bAddEmpty = true;
            }
            else if (strcmp(type, "mysqlType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/MySQLProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
              //ret->m_bAddEmpty = true;
            }
            else if (strcmp(type, "espprocessType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/EspProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "mysqlloggingagentType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/MySQLLoggingAgent", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "esploggingagentType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/ESPLoggingAgent", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "loggingmanagerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              bAddBlank = true;
              LoadComboBox("Software/LoggingManager", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "ldapServerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/LDAPServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "sashaServerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/SashaServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "accurintServerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/AccurintServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "buildSetType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              extraInfo = "Programs/Build[@name=$build]/BuildSet";
            }
            else if (strcmp(type, "buildType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Programs/Build", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();//"Programs/Build";
            }
            else if (strcmp(type, TAG_COMPUTERTYPE)==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Hardware/Computer", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataBuildType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/Build", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "dataModelType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Data/Model", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "espServiceType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/EspService", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "espProcessType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/EspProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "roxieClusterType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/RoxieCluster", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "eclServerType")==0)
            {
              // MORE - attribute servers would be ok here too
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/EclServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "eclCCServerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/EclCCServerProcess", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "wsLogListenerType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/WsLogListener", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "processType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox(pAppInfo->queryProp(TAG_NAME), bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "topologyClusterType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              LoadComboBox("Software/Topology/Cluster", bAddBlank, m_pEnv, m_pEnv, strBuf);
              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "xpathType")==0)
            {
              const char* xpath1 = pAppInfo->queryProp("xpath");
              const char* xpath2;
              if (xpath1 && *xpath1)
              {
                nCtrlType = 4;//LVC_COMBO;
                const char* prefix = "/Environment/";
                const int len = strlen(prefix);

                if (!strncmp(xpath1, prefix, len)) //xpath is absolute
                  xpath2 = xpath1 + len; //IPropertyTree root does not take absolute paths
                else
                  xpath2 = xpath1;

                if (!strstr(xpath2, "$"))
                  LoadComboBox(xpath2, bAddBlank, m_pEnv, m_pEnv, strBuf);
                else
                  strBuf.append("#").append(xpath2);

                extraInfo = strBuf.str();
              }
            }
            else if (strcmp(type, "espBindingType")==0)
            {
              nCtrlType = 4;//LVC_COMBO;
              m_jsStrBuf.appendf("%s.custom = %d;", aName.str(), 1);

              const char* serviceType = NULL;
              if (pAppInfo)
              {
                serviceType = pAppInfo->queryProp("serviceType");
                if (serviceType)
                  xpath.clear().append("Software/EspService");
              }
              else
                xpath.clear().appendf("Software/*[@buildSet='%s']", type);
              
              strBuf.clear();
              Owned<IPropertyTreeIterator> iterEspServices = m_pEnv->getElements(xpath);
              short blank = 0;
              ForEach (*iterEspServices)
              {
                IPropertyTree* pEspService = &iterEspServices->query();

                if (serviceType)
                {
                  xpath.clear().appendf("Properties[@type='%s']", serviceType);
                  if (pEspService->queryPropTree(xpath.str()) == NULL)
                    continue;
                }

                Owned<IPropertyTreeIterator> iter = m_pEnv->getElements("Software/EspProcess");

                ForEach (*iter)
                {
                  IPropertyTree* pEspProcess = &iter->query();

                  xpath.clear().appendf("EspBinding[@service='%s']", pEspService->queryProp(XML_ATTR_NAME));

                  if (pEspProcess->queryPropTree(xpath.str()) != NULL)
                    LoadComboBox(xpath.str(), (blank == 0), pEspProcess, pEspProcess, strBuf, true, (blank == 0), false);

                  blank++;
                }
              }

              strBuf.append(")");

              extraInfo = strBuf.str();
            }
            else if (strcmp(type, "AutoTimeStampType")==0 || 
              strcmp(type, "AutoComputerType")==0 || 
              strcmp(type, "AutoUseridType")==0)
            {
              if (nCtrlType != 0/*LVC_NONE*/)//is not read only
                nCtrlType = 1;//LVC_EDIT;
              m_jsStrBuf.appendf("%s.custom = %d;", aName.str(), 1);
              m_jsStrBuf.appendf("%s.extra = '%s';", aName.str(), type);
            }
            else if (strcmp(type, "absolutePath")==0 || strcmp(type, "relativePath")==0)
            {
              nCtrlType = 1;//LVC_EDIT;
              extraInfo = type;
            }
            else if (strcmp(type, "YesNo")==0)
            {
              nCtrlType = 4;//ret->m_bRequired ? LVC_YESNO : LVC_YESNO_OPTIONAL;
              strBuf.clear().append("new Array('No','Yes');");
              extraInfo = strBuf.str();
            }
          }
          else if (strcmp(typeNameSpace.str(),TAG_PROCESS)==0) //for backwards compatibility only - use xpathType instead
          {
            nCtrlType = 4;//LVC_COMBO;
            m_jsStrBuf.appendf("%s.extra = '%s%s';", aName.str(), "Software/", typeName.str());

          }
          // MORE - type could be a reference to a simpletype defined elsewhere....
        }
        else if (attr.hasProp("xs:simpleType/xs:restriction/xs:enumeration"))
        {
          Owned<IPropertyTreeIterator> values = attr.getElements("xs:simpleType/xs:restriction/xs:enumeration");
          combovalues.append("new Array(");
          ForEach(*values)
          {
            IPropertyTree &value = values->query();
            if (combovalues.length() > 10)
              combovalues.append(",");
            combovalues.append("'").append(value.queryProp("@value")).append("'");
          }

          combovalues.append(")");
          nCtrlType = 4;//LVC_COMBO;
          extraInfo = combovalues.str();
        }
        if (extraInfo)
        {
          if (!strncmp(extraInfo, "new Array", 9))
          {
            m_jsStrBuf.appendf("%s.extra = %s;", aName.str(), extraInfo);
            if (pField)
            {
              //["Alabama","Alaska","Arizona","Arkansas"]
              StringBuffer sb(extraInfo);
              sb.replaceString("new Array(", "[");
              sb.replaceString(");", "]");
              StringBuffer sbAttr("@");
              sbAttr.append(attr.queryProp(XML_ATTR_NAME));

              if (viewtype)
                pField->addProp(sbAttr.append("_extraInfo"), sb.str());
              else
                pField->addProp(UI_FIELD_ATTR_VALUE"_extra", sb.str());
            }
          }
          else
            m_jsStrBuf.appendf("%s.extra = '%s';", aName.str(), extraInfo);

        }

        m_jsStrBuf.appendf("%s.ctrlType = %d;", aName.str(), nCtrlType);
        m_jsStrBuf.appendf("cS['%s']=%s;", attrname.str(), aName.str());
      }

      void AddAttributeFromSchema(IPropertyTree& schemaNode, 
        StringBuffer elemName, 
        StringBuffer& compName, 
        const char* tabName,
        const char* childElementName)
      {
        CreateAttributeFromSchema(schemaNode, compName, tabName, childElementName);
      }

      void AddAttributesFromSchema(IPropertyTree* pSchema,
        StringBuffer& compName,
        const char* tabName,
        const char* childElementName)
      {
        if (pSchema)
        {
          //add attributes defined for this element
          Owned<IPropertyTreeIterator> attrIter = pSchema->getElements("xs:complexType/xs:attribute");
          ForEach(*attrIter)
          {
            AddAttributeFromSchema(attrIter->query(), "", compName, tabName, childElementName);
          }

          if (childElementName && !strcmp(childElementName, XML_TAG_INSTANCE))
          {
            const char* pszNameAttr = "<xs:attribute name='name' type='xs:string' use='optional'><xs:annotation><xs:appinfo><viewType>hidden</viewType></xs:appinfo></xs:annotation></xs:attribute>";
            Owned<IPropertyTree> pSchemaAttrNode = createPTreeFromXMLString(pszNameAttr);
            AddAttributeFromSchema(*pSchemaAttrNode, "", compName, tabName, childElementName);
          }

          // or if it's an attribute group, then try this variety...
          attrIter.setown(pSchema->getElements("xs:attribute"));
          ForEach(*attrIter)
          {
            AddAttributeFromSchema(attrIter->query(), "", compName, tabName, childElementName);
          }

          Owned<IPropertyTreeIterator> simple = pSchema->getElements("*");
          ForEach(*simple)
          {
            IPropertyTree &element = simple->query();
            const char* pszElementName = element.queryName();

            if (!strcmp(pszElementName, "xs:complexContent"))
              AddAttributesFromSchema(&element, compName, tabName, NULL);
          }
        }
      }

      void ProcessElementSchemaNode(IPropertyTree* pElement, 
        IPropertyTree* pParentElement, 
        StringBuffer& sbCompName)
      {
        bool bOptSubType = false;
        if (pElement)
        {
          TRACE_SCHEMA_NODE("ProcessElementSchemaNode", pElement); 
          const char* szParentElementName = pParentElement->queryProp(XML_ATTR_NAME);

          const char*  szElementName = pElement->queryProp(XML_ATTR_NAME);
          const char*  szCaption     = szElementName;
          const char* tabName = pElement->queryProp("xs:annotation/xs:appinfo/title");
          if (tabName)
            szCaption = tabName;

          IPropertyTree* pViewSchemaNode = pElement; //default for child view
          IPropertyTree* pInstanceNode = pParentElement;//default for child view

          bool bInstanceView = false;
          bool bViewChildNodes = true;

          if (pElement->hasProp("xs:annotation/xs:appinfo/viewType"))
          {
            const char* viewType = pElement->queryProp("xs:annotation/xs:appinfo/viewType");
            const char* viewChildNodes = pElement->queryProp("xs:annotation/xs:appinfo/viewChildNodes");
            bViewChildNodes = viewChildNodes && !stricmp(viewChildNodes, "true");

            bool needParent = true;

            //select first parent node that matches schema
            if (pInstanceNode && needParent)
            {
              Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(szElementName);
              if (it->first() && it->isValid())
                pInstanceNode = &it->query();
            }

            if (!strcmp(viewType, "list"))
            {
              const char* title = pElement->queryProp("xs:annotation/xs:appinfo/title");
              setNameInHiddenTabArray(title? title : szElementName);
              bOptSubType = true;
            }

            if (!strcmp(viewType, "Instance") || !strcmp(viewType, "instance") || 
              !strcmp(viewType, "RoxiePorts") || !strcmp(viewType, "RoxieSlaves"))
              bOptSubType = true;
          }

          bool bHasElements = schemaNodeHasElements(pElement) != NULL;

          if (bViewChildNodes)
          {
            bool bHasAttribs = schemaNodeHasAttributes(pElement);
            bool bHasAttribGroups = schemaNodeHasAttributeGroups(pElement);
            bool bMaxOccursOnce = !pElement->hasProp(XML_ATTR_MAXOCCURS) || !strcmp(pElement->queryProp(XML_ATTR_MAXOCCURS), "1");
            bOptSubType = !bMaxOccursOnce;

            //figure out the type of child view to create
            if (bHasElements)
            {
              // MORE - this assumes there is only one nested element type and that it is repeated....
              StringBuffer sbElemName(szElementName);
              if (bHasAttribs) //has child elements and attributes
              {
                Owned<IPropertyTreeIterator> iter = pElement->getElements("*");
                ForEach(*iter)
                {
                  IPropertyTree &subSchemaElement = iter->query();
                  const char* szSubElementName = subSchemaElement.queryName();
                  StringBuffer sbSubElemName(szSubElementName);
                  TRACE_SCHEMA_NODE("ProcessSchemaElement", &subSchemaElement); 
                  m_splitterTabName.clear().append(getRealTabName(szCaption));
                  if (m_allSubTypes || !bOptSubType)
                    ProcessComplexTypeSchemaNode(&subSchemaElement, m_pSchemaRoot, sbElemName);
                  m_splitterTabName.clear();
                }
              }
              else
              {
                //no attributes
                if (bMaxOccursOnce)
                {
                  //has child elements but no attribs and only occurs once 
                  //so skip this element and create node list view for its children
                  pViewSchemaNode = schemaNodeHasElements(pElement); 
                  if (pInstanceNode)
                  {
                    IPropertyTree* pNewInstanceNode = pInstanceNode->queryPropTree(szElementName);

                    if (!pNewInstanceNode)
                      pNewInstanceNode = pInstanceNode->addPropTree(szElementName, createPTree());

                    pInstanceNode = pNewInstanceNode;
                  }

                  szElementName = pViewSchemaNode->queryProp(XML_ATTR_NAME);
                }
              }
            }
            else
            {
              //no child elements
              if (bHasAttribs)
              {
                if (!bMaxOccursOnce) //occurs multiple times
                {
                  //select first parent node that matches schema
                  if (pInstanceNode)
                  {
                    Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(szParentElementName);
                    if (it->first() && it->isValid())
                      pInstanceNode = &it->query();
                  }
                }
              }
              else
              {
                const char* type = pElement->queryProp("@type");

                if (type && !strcmp(type, "xs:string"))
                {
                  m_jsStrBuf.appendf("var attr%s%s = {};", szElementName, sbCompName.str());
                  m_jsStrBuf.appendf("attr%s%s.tab = '%s';", szElementName, sbCompName.str(), getRealTabName(szElementName));
                  m_jsStrBuf.appendf("attr%s%s.hidden = 0;", szElementName, sbCompName.str());
                  m_jsStrBuf.appendf("attr%s%s.required = 1;", szElementName, sbCompName.str());
                  m_jsStrBuf.appendf("attr%s%s.ctrlType = %d;", szElementName, sbCompName.str(), 6);
                  m_jsStrBuf.appendf("cS['%s%s']=attr%s%s;", szElementName, sbCompName.str(), szElementName, sbCompName.str());
                  //add additional entry for this special case where element value is shown as a column
                  m_jsStrBuf.appendf("cS['%s%s']=attr%s%s;", szElementName, szElementName, szElementName, sbCompName.str());
                  setNameInCompTabArray(m_splitterTabName, sbCompName.str());
                  m_columns.appendf("tabCols['%s'][%d] = '_%s';", m_splitterTabName.str(), m_numAttrs++, szElementName);
                  setNameInCompTabArray(szElementName, szElementName);
                  setNameInHiddenTabArray(szElementName);
                  m_columns.appendf("tabCols['%s'][%d] = '%s';", szElementName, 0, szElementName);

                  if (m_pCompTree)
                  {
                    StringBuffer sb(sbCompName);
                    if (!m_pCompTree->queryPropTree(sbCompName.str()))
                      m_pCompTree->addPropTree(sbCompName.str(), createPTree());

                    sb.append("/").append(szElementName);
                    m_pCompTree->addPropTree(sb.str()/*szElementName*/, createPTree());
                  }
                }
              }
            }
          }

          if (m_allSubTypes || !bOptSubType)
            AddAttributesFromSchema(pViewSchemaNode, sbCompName, szCaption, szElementName);

          if (bOptSubType && m_viewChildNodes.get() && m_multiRowNodes.get())
          {
            if (bHasElements)
              m_viewChildNodes->addProp("Node", szElementName);
            else
              m_multiRowNodes->addProp("Node", szElementName);
          }

          if (pInstanceNode)
          {
            //select first child node for which we are creating view
            Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(pElement->queryProp(XML_ATTR_NAME));
            pInstanceNode = (it->first() && it->isValid()) ? &it->query() : NULL;
          }
        }
      }


      void ProcessComplexTypeSchemaNode(IPropertyTree* schemaNode, 
        IPropertyTree* pParentElement,
        StringBuffer& sbCompName)
      {
        if (schemaNode)
        {
          TRACE_SCHEMA_NODE("ProcessComplexTypeSchemaNode", schemaNode); 
          const char* szParentElementName = pParentElement->queryProp(XML_ATTR_NAME);

          //now process the rest...
          Owned<IPropertyTreeIterator> iter = schemaNode->getElements(XSD_TAG_ATTRIBUTE_GROUP);
          ForEach(*iter)
          {
            IPropertyTree &schemaElement = iter->query();
            const char* name = schemaElement.queryProp("@ref");

            StringBuffer xPath;
            xPath.append("//xs:attributeGroup[@name='").append(name).append("']");
            Owned<IPropertyTreeIterator> iter2 = m_pSchemaRoot->getElements(xPath.str());
            ForEach(*iter2)
            {
              IPropertyTree &agDef = iter2->query();
              if (agDef.hasProp("xs:annotation/xs:appinfo/title"))
                name = agDef.queryProp("xs:annotation/xs:appinfo/title");

              AddAttributesFromSchema(&agDef, sbCompName, name, NULL);

              break; // should be exactly one!
              // MORE - this will not get scoping correct. Do I care?
            }
          }
          iter.setown(schemaNode->getElements("*"));
          ForEach(*iter)
          {
            IPropertyTree &schemaElement = iter->query();
            const char* szSchemaElementName = schemaElement.queryName();

            if (!strcmp(szSchemaElementName, XSD_TAG_SEQUENCE) || !strcmp(szSchemaElementName, XSD_TAG_CHOICE))
            {
              Owned<IPropertyTreeIterator> iter2 = schemaElement.getElements(XSD_TAG_ELEMENT);
              ForEach(*iter2)
              {
                IPropertyTree* pElement = &iter2->query();
                ProcessElementSchemaNode(pElement, pParentElement, sbCompName);
              }
            }
          }
        }
      }

      bool generateHeaders()
      {
        StringBuffer sbTabName;
        StringBuffer sbPropName;

        if (!m_pSchemaRoot)
          m_pSchemaRoot.setown(createPTreeFromXMLFile(m_xsdName));

        IPropertyTree *schemaNode = m_pSchemaRoot->queryPropTree("xs:element");

        if (m_compName.length() == 0)
          m_compName.append(schemaNode->queryProp(XML_ATTR_NAME));

        if (!strcmp(m_compName.str(), "Eclserver"))
          m_compName.clear().append(XML_TAG_ECLSERVERPROCESS);

        m_jsStrBuf.append("var compTabs = new Array();\n ");
        m_jsStrBuf.appendf("compTabs['%s'] = new Array();\n", m_compName.str());
        m_jsStrBuf.append("var hiddenTabs = new Array();\n ");
        m_jsStrBuf.appendf("hiddenTabs['%s'] = new Array();\n", m_compName.str());
        m_jsStrBuf.append("var compTabToNode = new Array();\n");
        m_jsStrBuf.append("var cS = new Array();\n");

        Owned<IPropertyTreeIterator> iter = schemaNode->getElements("*");
        ForEach(*iter)
        {
          IPropertyTree &schemaElement = iter->query();
          const char* szElementName = schemaElement.queryName();
          TRACE_SCHEMA_NODE("ProcessSchemaElement", &schemaElement); 

          //if node is xs:complexType and xs:complexContent then process children
          if (!strcmp(szElementName, XSD_TAG_COMPLEX_TYPE) ||
            !strcmp(szElementName, XSD_TAG_COMPLEX_CONTENT))
          {
            //if this schema node has any attributes then add an attribute tab
            //            
            bool bHasAttribs = schemaNodeHasAttributes(&schemaElement);
            if (bHasAttribs)
            {
              AddAttributesFromSchema(schemaNode, m_compName, "Attributes", NULL);
            }                   
          }

          ProcessComplexTypeSchemaNode(&schemaElement, m_pSchemaRoot, m_compName);
        }

        addMisc();

        if (m_jsName.length())
          writeToFile(m_jsName, m_jsStrBuf);

        return true;
      }

      void setCompTree(IPropertyTree* pTree, bool allSubTypes)
      {
        m_pCompTree = pTree;
        m_allSubTypes = allSubTypes;
      }

      void getTabNameArray(StringArray& tabNameArray)
      {
        CloneArray(tabNameArray, m_tabNameArray);
      }

      void getDefnPropTree(IPropertyTree* pTree, StringBuffer xpathDefn)
      {
        m_pDefTree = pTree;
        m_xpathDefn.clear().append(xpathDefn);
        generateHeaders();
      }

      void getDefnString(StringBuffer& compDefn, StringBuffer& viewChildNodes, StringBuffer& multiRowNodes)
      {
        m_viewChildNodes.clear();
        m_viewChildNodes.setown(createPTree("viewChildNodes"));
        m_multiRowNodes.clear();
        m_multiRowNodes.setown(createPTree("multiRowNodes"));
        
        generateHeaders();
        compDefn.clear().append(m_jsStrBuf);

        if (m_colIndex.length() > 27)
          compDefn.append(m_colIndex);

        if (m_columns.length() > 26)
          compDefn.append(m_columns);

        toXML(m_viewChildNodes, viewChildNodes);
        toXML(m_multiRowNodes, multiRowNodes);
      }

      void setWizardFlag(bool flag)
      {
         m_wizFlag = flag;
      }

      void setGenerateOptional(bool flag)
      {
        m_genOptional = flag;
      }
     
      void setWizard(CWizardInputs* ptr)
      {
        m_wizard.set(ptr);
      }

      void getValueForTypeInXSD(IPropertyTree& attr, StringBuffer compName, StringBuffer& wizDefVal)
      {
        StringBuffer tempPath;
        const char* type = attr.queryProp("@type");
        const char* name = attr.queryProp("@name");

        //first check for all the tags autogen then proceed with type checking.
        if(attr.hasProp("./xs:annotation/xs:appinfo/autogendefaultvalue"))
        {
           tempPath.clear().append("./xs:annotation/xs:appinfo/autogendefaultvalue");
         
           if(!strcmp(attr.queryProp(tempPath.str()), "$defaultenvfile"))
           {
             tempPath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile", (m_wizard->getService()).str());
             IPropertyTree* pCfg =  m_wizard->getConfig();
             const char* pConfFile = pCfg->queryProp(tempPath.str());
             if(pConfFile && *pConfFile)
             {
               Owned<IProperties> pParams = createProperties(pConfFile);
               wizDefVal.clear().append(pParams->queryProp("configs")).append("/environment.xml");
             }
           }
           else if(!strcmp(attr.queryProp(tempPath.str()), "$componentfilesdir"))
           {
             tempPath.clear().append("EnvSettings/path");
             wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
             if(!wizDefVal.length())
                wizDefVal.append(STANDARD_COMPFILESDIR);

             wizDefVal.append(PATHSEPSTR"componentfiles");
           }
           else if(!strcmp(attr.queryProp(tempPath.str()), "$processname"))
           {
              tempPath.clear().appendf("Software/%s[1]/@name",compName.str());
              wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
           }
           else if(!strcmp(attr.queryProp(tempPath.str()), "$hthorcluster"))
           {
              tempPath.clear().append(XML_TAG_SOFTWARE "/" XML_TAG_TOPOLOGY "/" XML_TAG_CLUSTER);
              Owned<IPropertyTreeIterator> iterClusters = m_pEnv->getElements(tempPath.str());
              ForEach (*iterClusters)
              {
                IPropertyTree* pCluster = &iterClusters->query();
                
                if (pCluster->queryPropTree(XML_TAG_ROXIECLUSTER) || 
                    pCluster->queryPropTree(XML_TAG_THORCLUSTER))
                  continue;
                else 
                {
                  wizDefVal.clear().append(pCluster->queryProp(XML_ATTR_NAME));
                  break;
                }
              }
           }
           else
           {
             wizDefVal.clear().append(attr.queryProp(tempPath.str()));
             tempPath.clear().appendf("Software/%s[1]/@buildSet", compName.str());
             if(m_pEnv->queryProp(tempPath.str()))
             {
               if(m_wizard->getNumOfNodes(m_pEnv->queryProp(tempPath.str())) > 1)
               {
                 tempPath.clear().append("./xs:annotation/xs:appinfo/autogendefaultformultinode");
                 if(attr.hasProp(tempPath.str()))
                   wizDefVal.clear().append(attr.queryProp(tempPath.str()));
               }
             }
           }
        }
        else if(attr.hasProp("./xs:annotation/xs:appinfo/autogenprefix") || attr.hasProp("./xs:annotation/xs:appinfo/autogensuffix"))
        {
          StringBuffer sb;
          StringBuffer nameOfComp;
          tempPath.clear().appendf("./Software/%s[1]/@name",m_compName.str());
          nameOfComp.clear().append(m_pEnv->queryProp(tempPath.str()));

          tempPath.clear().append("./xs:annotation/xs:appinfo/autogenprefix");
          if(attr.hasProp(tempPath.str()))
            sb.clear().append(attr.queryProp(tempPath.str())).append(nameOfComp);

          tempPath.clear().append("./xs:annotation/xs:appinfo/autogensuffix");
          if(attr.hasProp(tempPath.str()))
          {
            if (sb.length())
              sb.append(attr.queryProp(tempPath.str()));
            else
              sb.append(nameOfComp).append(attr.queryProp(tempPath.str()));
          }

          wizDefVal.clear().append(sb);
        }
        else if(!strcmp(type,"computerType"))
        {
          if(m_wizard)
          {
            StringBuffer buildSetName, ipAddr;
            tempPath.clear().appendf("./Programs/Build/BuildSet[%s=\"%s\"]",XML_ATTR_PROCESS_NAME,m_compName.str());
            IPropertyTree* pCompTree = m_pEnv->queryPropTree(tempPath.str());
            if(pCompTree)
            {
              buildSetName.append(pCompTree->queryProp(XML_ATTR_NAME));
              CInstDetails* pInst  = m_wizard->getServerIPMap(compName, buildSetName,m_pEnv);
              if( pInst )
              {
                StringArray& ipArray = pInst->getIpAssigned();
                ForEachItemIn(x, ipArray)
                {
                   if(ipArray.ordinality() == 1)
                    ipAddr.append(ipArray.item(x));
                   else
                    ipAddr.append(ipArray.item(x)).append(",");
    
                   tempPath.clear().appendf("./Hardware/Computer[@netAddress=\"%s\"]",ipAddr.str());
                   IPropertyTree* pHard = m_pEnv->queryPropTree(tempPath.str());
                   if(pHard)
                   {
                     tempPath.clear().append("@name");
                     wizDefVal.clear().append(pHard->queryProp(tempPath.str()));
                   }
                }
              }
            }
          }
        }
        else if(!strcmp(type,"xs:string"))
        {
          StringBuffer nameOfComp;
          tempPath.clear().appendf("./Software/%s[1]/@name",m_compName.str());
           nameOfComp.clear().append(m_pEnv->queryProp(tempPath.str()));

          if(!strcmp(name, "dbUser"))
          {
            wizDefVal.clear().append(m_wizard->getDbUser()); 
          }
          else if(!strcmp(name, "dbPassword"))
          {
            wizDefVal.clear().append(m_wizard->getDbPassword());
          }
        }
        else if(!strcmp(type,"mysqlType"))
        {
          tempPath.clear().append("./Software/MySQLProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"espprocessType"))
        {
          tempPath.clear().append("./Software/EspProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"mysqlloggingagentType"))
        {
          tempPath.clear().append("./Software/MySQLLoggingAgent[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"esploggingagentType"))
        {
          tempPath.clear().append("./Software/ESPLoggingAgent[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"loggingmanagerType"))
        {
          tempPath.clear().append("./Software/LoggingManager[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"daliServersType"))
        {
          tempPath.clear().append("./Software/DaliServerProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type,"ldapServerType"))
        {
          tempPath.clear().append("./Software/LdapServerProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type, "roxieClusterType"))
        {
          tempPath.clear().append("./Software/RoxieCluster[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type, "eclServerType"))
        {
          tempPath.clear().append("./Software/EclServerProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type, "eclCCServerType"))
        {
          tempPath.clear().append("./Software/EclCCServerProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type, "espProcessType"))
        {
          tempPath.clear().append("./Software/EspProcess[1]/@name");
          wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
        }
        else if(!strcmp(type, "espBindingType"))
        {
          IPropertyTree* pAppInfo = attr.queryPropTree("xs:annotation/xs:appinfo");
          StringBuffer xpath;
          const char* serviceType = NULL;
          if (pAppInfo)
          {
            serviceType = pAppInfo->queryProp("serviceType");
            if (serviceType)
              xpath.clear().append("Software/EspService");
          }
          else
             xpath.clear().appendf("Software/*[@buildSet='%s']", type);
          
          wizDefVal.clear();
          
          Owned<IPropertyTreeIterator> iterEspServices = m_pEnv->getElements(xpath);
          short blank = 0;
          ForEach (*iterEspServices)
          {
            IPropertyTree* pEspService = &iterEspServices->query();

            if(serviceType)
            {
              xpath.clear().appendf("Properties[@type='%s']", serviceType);
              if (pEspService->queryPropTree(xpath.str()) == NULL)
                continue;
            }
           
            Owned<IPropertyTreeIterator> iter = m_pEnv->getElements("Software/EspProcess[1]");

            ForEach (*iter)
            {
              IPropertyTree* pEspProcess = &iter->query();
              xpath.clear().appendf("EspBinding[@service='%s']", pEspService->queryProp(XML_ATTR_NAME));
              if(pEspProcess->queryPropTree(xpath.str()) != NULL)
                wizDefVal.append(pEspProcess->queryProp(XML_ATTR_NAME)).append("/").append(pEspService->queryProp(XML_ATTR_NAME));
            }
           }
         }
         else if(!strcmp(type, "ipAddressAndPort"))
         {
           StringBuffer defaultPort;
           tempPath.clear().append("./xs:annotation/xs:appinfo/defaultPort");
           defaultPort.append(attr.queryProp(tempPath.str()));
           tempPath.clear().append("./xs:annotation/xs:appinfo/autogenxpath");
           if(attr.hasProp(tempPath.str()))
           {
              StringBuffer computerName;
              computerName.append(m_pEnv->queryProp(attr.queryProp(tempPath.str())));
              tempPath.clear().appendf("./Hardware/Computer[@name=\"%s\"]",computerName.str());
              if(m_pEnv->hasProp(tempPath.str()))
              {
                IPropertyTree* pHard = m_pEnv->queryPropTree(tempPath.str());
                if(pHard)
                  wizDefVal.clear().append(pHard->queryProp("./" XML_ATTR_NETADDRESS)).append(":").append(defaultPort);
              }
           }
         }
      }

private:

  Linked<const IPropertyTree> m_pEnv;
  Linked<IPropertyTree> m_pSchemaRoot;
  IPropertyTree* m_pCompTree;
  IPropertyTree* m_pDefTree;
  Owned<IPropertyTree> m_viewChildNodes;
  Owned<IPropertyTree> m_multiRowNodes;
  StringBuffer m_jsStrBuf;
  StringBuffer m_colIndex;
  StringBuffer m_columns;
  StringBuffer m_xsdName;
  StringBuffer m_jsName;
  StringBuffer m_compName;
  StringBuffer m_xpathDefn;
  StringBuffer m_splitterTabName;
  StringArray m_tabNameArray;
  StringArray m_hiddenTabNameArray;
  short m_numAttrs;
  bool m_allSubTypes;
  bool m_wizFlag;
  bool m_genOptional;
  Linked<CWizardInputs> m_wizard;
};

short treeHasMultipleCompsOfSameType(Linked<IPropertyTree> compTypeTree, const char* xpath)
{
  Owned<IPropertyTreeIterator> iter = compTypeTree->getElements(xpath);
  IPropertyTree *element = NULL;
  short count = 0;

  if (iter)
    if (iter->first())
    {
      if (iter->isValid())
      {
        element = &iter->query();
        if (iter->next())
        {
          Owned<IPropertyTreeIterator> iterDup = compTypeTree->getElements(xpath);
          ForEach(*iterDup) count++;
        }
        else
          count = 1;
      }
    }

    return count;
}

bool generateHeadersFromXsd(IPropertyTree* pEnv, const char* xsdName, const char* jsName)
{
  CGenerateJSFromXSD obj(pEnv, xsdName, jsName);
  return obj.generateHeaders();
}

IPropertyTree* generateTreeFromXsd(const IPropertyTree* pEnv, IPropertyTree* pSchema,
                                   const char* compName, const char* buildSetName,
                                   const IPropertyTree* pCfg, const char* servicename,
                                   bool allSubTypes, bool wizFlag, CWizardInputs* pWInputs,
                                   bool forceOptional)
{
  bool flag = true;

  if (!forceOptional)
  {
    StringBuffer xpath, genEnvConf, prop;
    Owned<IProperties> algProp;
    xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalConfFile", servicename);
    const char* pConfFile = pCfg->queryProp(xpath.str());
    xpath.clear().appendf("Software/EspProcess/EspService[@name='%s']/LocalEnvConfFile", servicename);
    const char* pEnvConfFile = pCfg->queryProp(xpath.str());

    if (pConfFile && *pConfFile && pEnvConfFile && *pEnvConfFile)
    {
      Owned<IProperties> pParams = createProperties(pConfFile);
      Owned<IProperties> pEnvParams = createProperties(pEnvConfFile);
      const char* genenv = pParams->queryProp("wizardalgorithm");

      if (!genenv || !*genenv)
        genenv = "genenvrules.conf";

      const char* cfgpath = pEnvParams->queryProp("configs");

      if (!cfgpath || !*cfgpath)
        cfgpath = CONFIG_DIR;

      genEnvConf.clear().append(cfgpath);

      if (genEnvConf.charAt(genEnvConf.length() - 1) != PATHSEPCHAR)
        genEnvConf.append(PATHSEPCHAR);

      genEnvConf.append(genenv);
    }

    if (genEnvConf.length() && checkFileExists(genEnvConf.str()))
      algProp.setown(createProperties(genEnvConf.str()));

    CConfigHelper::getInstance()->addPluginsToGenEnvRules(algProp.get());

    enum GenOptional {GENOPTIONAL_ALL, GENOPTIONAL_NONE, GENOPTIONAL_COMPS};
    GenOptional genOpt = GENOPTIONAL_COMPS;
    algProp->getProp("do_not_gen_optional", prop);
    StringArray doNotGenOpt;
    doNotGenOpt.appendList(prop.str(), ",");

    if (doNotGenOpt.length() == 0)
      genOpt = GENOPTIONAL_ALL;
    else if (doNotGenOpt.length() == 1 && !strcmp(doNotGenOpt.item(0), "all"))
      genOpt = GENOPTIONAL_NONE;

    if (genOpt == GENOPTIONAL_ALL || (genOpt == GENOPTIONAL_COMPS && doNotGenOpt.find(buildSetName) == NotFound ))
      flag = true;
    else if (genOpt == GENOPTIONAL_NONE || (genOpt == GENOPTIONAL_COMPS && doNotGenOpt.find(buildSetName) != NotFound ))
      flag = false;
  }

  Owned<IPropertyTree> pCompTree(createPTree(compName));
  CGenerateJSFromXSD obj(pEnv, pSchema, "", compName);
  obj.setCompTree(pCompTree, allSubTypes);
  obj.setWizardFlag(wizFlag);
  obj.setGenerateOptional(flag);
  obj.setWizard(pWInputs);
  obj.generateHeaders();
  return pCompTree.getLink();
}

bool generateHardwareHeaders(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut, IPropertyTree* pCompTree, bool bIncludeNAS)
{
  if (pCompTree)
  {
    StringBuffer xpath,sbdefaultValue("");
    IPropertyTree* pComputerType = pCompTree->addPropTree(XML_TAG_COMPUTERTYPE, createPTree());
    xpath.clear().append(XML_ATTR_NAME);
    pComputerType->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_MANUFACTURER);
    pComputerType->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_COMPUTERTYPE);
    pComputerType->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_OPSYS);
    pComputerType->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_MEMORY);
    pComputerType->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_NICSPEED);
    pComputerType->addProp(xpath, sbdefaultValue.str());

    IPropertyTree* pComputer = pCompTree->addPropTree(XML_TAG_COMPUTER, createPTree());
    xpath.clear().append(XML_ATTR_NAME);
    pComputer->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_NETADDRESS);
    pComputer->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_DOMAIN);
    pComputer->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_COMPUTERTYPE);
    pComputer->addProp(xpath, sbdefaultValue.str());

    IPropertyTree* pSwitch = pCompTree->addPropTree(XML_TAG_SWITCH, createPTree());
    xpath.clear().append(XML_ATTR_NAME);

    IPropertyTree* pDomain = pCompTree->addPropTree(XML_TAG_DOMAIN, createPTree());
    xpath.clear().append(XML_ATTR_NAME);
    pDomain->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_USERNAME);
    pDomain->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append(XML_ATTR_PASSWORD);
    pDomain->addProp(xpath, sbdefaultValue.str());
    xpath.clear().append("@snmpSecurityString");
    pDomain->addProp(xpath, sbdefaultValue.str());

    if (bIncludeNAS == true)
    {
      IPropertyTree* pNAS = pCompTree->addPropTree(XML_TAG_NAS, createPTree());
      xpath.clear().append(XML_ATTR_NAME);
      pNAS->addProp(xpath, sbdefaultValue.str());
      xpath.clear().append(XML_ATTR_MASK);
      pNAS->addProp(xpath, "255.255.255.255");
      xpath.clear().append(XML_ATTR_SUBNET);
      pNAS->addProp(xpath, "0.0.0.0");
      xpath.clear().append(XML_ATTR_DIRECTORY);
      pNAS->addProp(xpath, sbdefaultValue.str());
      xpath.clear().append(XML_ATTR_TRACE);
      pNAS->addProp(xpath, sbdefaultValue.str());
    }
  }
  else
  {
    StringBuffer jsStrBuf("var compTabs = new Array();");
    jsStrBuf.append("compTabs['Hardware'] = new Array();");
    jsStrBuf.append("var compTabToNode = new Array();");
    jsStrBuf.append("var cS = new Array();");
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE, TAG_NAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE,  TAG_MANUFACTURER, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE, TAG_COMPUTERTYPE, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE,  TAG_OPSYS, "", 0, 1, "|new Array('W2K','solaris','linux')", 4);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE,  TAG_MEMORY, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTERTYPE,  TAG_NICSPEED, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_SWITCH,  TAG_NAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_DOMAIN,  TAG_NAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_DOMAIN,  TAG_USERNAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_DOMAIN,  TAG_PASSWORD, "", 0, 1, "", 5);
    addItem(jsStrBuf, pEnv, XML_TAG_DOMAIN,  TAG_SNMPSECSTRING, "", 0, 1, "", 5);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTER,  TAG_NAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTER,  TAG_NETADDRESS, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTER,  TAG_DOMAIN, "", 0, 1, XML_TAG_HARDWARE "/" XML_TAG_DOMAIN, 4);
    addItem(jsStrBuf, pEnv, XML_TAG_COMPUTER,  TAG_COMPUTERTYPE, "", 0, 1, XML_TAG_HARDWARE "/" XML_TAG_COMPUTERTYPE, 4);
    addItem(jsStrBuf, pEnv, XML_TAG_NAS,  TAG_NAME, "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_NAS,       TAG_SUBNET,       "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_NAS,       TAG_DIRECTORY,       "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_NAS,       TAG_MASK,       "", 0, 1, "", 1);
    addItem(jsStrBuf, pEnv, XML_TAG_NAS,       TAG_TRACE,       "", 0, 1, "", 1);
    jsStrBuf.append("compTabs['Hardware'][compTabs['Hardware'].length]= 'Computer Types';");
    jsStrBuf.append("compTabs['Hardware'][compTabs['Hardware'].length]= 'Switches';");
    jsStrBuf.append("compTabs['Hardware'][compTabs['Hardware'].length]= 'Domains';");
    jsStrBuf.append("compTabs['Hardware'][compTabs['Hardware'].length]= 'Computers';");
    jsStrBuf.append("compTabs['Hardware'][compTabs['Hardware'].length]= 'NAS';");
    jsStrBuf.append("compTabToNode['Computer Types']= 'ComputerType';");
    jsStrBuf.append("compTabToNode['Switches']= 'Switch';");
    jsStrBuf.append("compTabToNode['Domains']= 'Domain';");
    jsStrBuf.append("compTabToNode['Computers']= 'Computer';");
    jsStrBuf.append("compTabToNode['NAS']= 'NAS';");

    int index = 0;
    jsStrBuf.append("var colIndex = new Array();");
    jsStrBuf.appendf("colIndex['nameComputer Types']=%d;", index++);
    jsStrBuf.appendf("colIndex['manufacturerComputer Types']=%d;", index++);
    jsStrBuf.appendf("colIndex['computerTypeComputer Types']=%d;", index++);
    jsStrBuf.appendf("colIndex['opSysComputer Types']=%d;", index++);
    jsStrBuf.appendf("colIndex['nameSwitches']=%d;", 0);
    index=0;
    jsStrBuf.appendf("colIndex['nameDomains']=%d;", index++);
    jsStrBuf.appendf("colIndex['usernameDomains']=%d;", index++);
    jsStrBuf.appendf("colIndex['passwordDomains']=%d;", index++);
    index=0;
    jsStrBuf.appendf("colIndex['nameComputers']=%d;", index++);
    jsStrBuf.appendf("colIndex['netAddressComputers']=%d;", index++);
    jsStrBuf.appendf("colIndex['domainComputers']=%d;", index++);
    jsStrBuf.appendf("colIndex['computerTypeComputers']=%d;", index++);

    index=0;
    jsStrBuf.appendf("colIndex['nameNAS']=%d;", index++);
    jsStrBuf.appendf("colIndex['maskNAS']=%d;", index++);
    jsStrBuf.appendf("colIndex['subnetNAS']=%d;", index++);
    jsStrBuf.appendf("colIndex['directoryNAS']=%d;", index++);
    jsStrBuf.appendf("colIndex['traceNAS']=%d;", index++);

    sbDefn.clear().append(jsStrBuf);

    if (writeOut)
      writeToFile(CONFIGMGR_JSPATH"hardware.js", jsStrBuf);
  }

  return true;
}

bool generateHeadersForEnvSettings(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut)
{
  StringBuffer jsStrBuf("var compTabs = new Array();");
  jsStrBuf.append("compTabs['EnvSettings'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  addItem(jsStrBuf, pEnv, "", TAG_SRCPATH, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, "", TAG_LOCK, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, "", TAG_CONFIGS, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, "", TAG_PATH, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, "", TAG_RUNTIME, "", 0, 1, "", 0);
  jsStrBuf.append("compTabs['EnvSettings'][compTabs['EnvSettings'].length]= 'Attributes';");

  sbDefn.clear().append(jsStrBuf);

  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"EnvSettings.js", jsStrBuf);

  return true;
}

bool generateHeadersForEnvXmlView(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut)
{
  StringBuffer jsStrBuf("var compTabs = new Array();");
  jsStrBuf.append("compTabs['Environment'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  jsStrBuf.append("var colIndex = new Array();");

  int index = 0;
  jsStrBuf.appendf("colIndex['nameEnvironment']=%d;", index++);
  jsStrBuf.appendf("colIndex['valueEnvironment']=%d;", index++);

  jsStrBuf.appendf("var attr%s%s = {};", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.tab = 'Environment';", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.tip = '';", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.hidden = 0;", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.required = 1;", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.ctrlType = 1;", TAG_NAME, TAG_ATTRIBUTE);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", TAG_NAME, TAG_ATTRIBUTE, TAG_NAME, TAG_ATTRIBUTE);

  jsStrBuf.appendf("var attr%s%s = {};", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.tab = 'Environment';", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.tip = '';", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.hidden = 0;", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.required = 1;", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("attr%s%s.ctrlType = 1;", TAG_VALUE, TAG_ATTRIBUTE);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", TAG_VALUE, TAG_ATTRIBUTE, TAG_VALUE, TAG_ATTRIBUTE);

  jsStrBuf.appendf("var attr%s%s = {};", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.tab = 'Environment';", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.tip = '';", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.hidden = 0;", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.required = 1;", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.ctrlType = 1;", TAG_NAME, TAG_ELEMENT);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", TAG_NAME, TAG_ELEMENT, TAG_NAME, TAG_ELEMENT);

  jsStrBuf.appendf("var attr%s%s = {};", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.tab = 'Environment';", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.tip = '';", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.hidden = 0;", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.required = 1;", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("attr%s%s.ctrlType = 1;", TAG_VALUE, TAG_ELEMENT);
  jsStrBuf.appendf("cS['%s%s']=attr%s%s;", TAG_VALUE, TAG_ELEMENT, TAG_VALUE, TAG_ELEMENT);

  jsStrBuf.append("compTabs['Environment'][compTabs['Environment'].length]= 'Environment';");

  sbDefn.clear().append(jsStrBuf);

  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"Environment.js", jsStrBuf);

  return true;
}

bool generateHeaderForDeployableComps(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut)
{
  short index = 0;
  StringBuffer jsStrBuf("var compTabs = new Array();");
  jsStrBuf.append("compTabs['Deploy'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  jsStrBuf.append("var colIndex = new Array();");
  addItem(jsStrBuf, pEnv, XML_TAG_COMPONENT, TAG_BUILD, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, XML_TAG_COMPONENT, TAG_BUILDSET, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, XML_TAG_COMPONENT, TAG_NAME, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, XML_TAG_INSTANCES, TAG_BUILDSET, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, XML_TAG_INSTANCES, TAG_DIRECTORY, "", 0, 1, "", 0);
  addItem(jsStrBuf, pEnv, XML_TAG_INSTANCES, TAG_NODENAME, "", 0, 1, "", 0);
  jsStrBuf.append("compTabs['Deploy'][compTabs['Deploy'].length]= 'Deploy';");
  jsStrBuf.appendf("colIndex['nameDeploy']=%d;", index++);
  jsStrBuf.appendf("colIndex['buildDeploy']=%d;", index++);
  jsStrBuf.appendf("colIndex['buildSetDeploy']=%d;", index++);

  sbDefn.clear().append(jsStrBuf);
  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"deploy.js", jsStrBuf);
  return true;
}


bool generateHeaderForTopology(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut)
{
  short index = 0;
  StringBuffer jsStrBuf("var compTabs = new Array();");
  jsStrBuf.append("compTabs['Topology'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  addTopologyType(jsStrBuf, pEnv, "", TAG_NAME, "", 1, 1, "", 1);
  addTopologyType(jsStrBuf, pEnv, "", TAG_BUILDSET, "", 1, 1, "", 1);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_ECLCCSERVERPROCESS,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/EclCCServerProcess", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_ECLSERVERPROCESS,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/EclServerProcess", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_ECLSCHEDULERPROCESS,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/EclSchedulerProcess", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_ECLAGENTPROCESS,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/EclAgentProcess", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_THORCLUSTER,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/ThorCluster", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_ROXIECLUSTER,  TAG_PROCESS, "", 0, 1, XML_TAG_SOFTWARE"/RoxieCluster", 4);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_CLUSTER,  TAG_NAME, "", 0, 1, "", 1);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_CLUSTER,  TAG_PREFIX, "", 0, 1, "", 1);
  addTopologyType(jsStrBuf, pEnv, XML_TAG_CLUSTER,  TAG_ALIAS, "", 0, 1, "", 1);
  jsStrBuf.append("compTabs['Topology'][compTabs['Topology'].length]= 'Topology';");
  jsStrBuf.append("var colIndex = new Array();");
  jsStrBuf.appendf("colIndex['nameTopology']=%d;", index++);
  jsStrBuf.appendf("colIndex['valueTopology']=%d;", index++);

  sbDefn.append(jsStrBuf);

  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"Topology.js", jsStrBuf);

  return true;
}

bool generateBuildHeaders(const IPropertyTree* pEnv, bool isPrograms, StringBuffer& sbDefn, bool writeOut)
{
  short index = 0;
  StringBuffer jsStrBuf("var compTabs = new Array();");
  jsStrBuf.append("compTabs['Programs'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS, TAG_NAME, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS,  TAG_URL, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS, TAG_PATH, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS,  TAG_INSTALLSET, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS,  TAG_PROCESSNAME, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS,  TAG_SCHEMA, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_PROGRAMS,  TAG_DEPLOYABLE, "", 0, 1, "", 1);
  jsStrBuf.append("compTabs['Programs'][compTabs['Programs'].length]= 'Programs';");
  jsStrBuf.append("var colIndex = new Array();");
  jsStrBuf.appendf("colIndex['namePrograms']=%d;", index++);
  jsStrBuf.appendf("colIndex['pathPrograms']=%d;", index++);
  jsStrBuf.appendf("colIndex['installSetPrograms']=%d;", index++);
  jsStrBuf.appendf("colIndex['processNamePrograms']=%d;", index++);
  jsStrBuf.appendf("colIndex['schemaPrograms']=%d;", index++);
  jsStrBuf.appendf("colIndex['deployablePrograms']=%d;", index++);

  if (isPrograms)
    sbDefn.clear().append(jsStrBuf);

  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"programs.js", jsStrBuf);

  jsStrBuf.clear().append("var compTabs = new Array();");
  jsStrBuf.append("compTabs['BuildSet'] = new Array();");
  jsStrBuf.append("var compTabToNode = new Array();");
  jsStrBuf.append("var cS = new Array();");
  addItem(jsStrBuf, pEnv, XML_TAG_BUILDSET, TAG_NAME, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_BUILDSET,  TAG_METHOD, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_BUILDSET, TAG_SRCPATH, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_BUILDSET,  TAG_DESTPATH, "", 0, 1, "", 1);
  addItem(jsStrBuf, pEnv, XML_TAG_BUILDSET,  TAG_DESTNAME, "", 0, 1, "", 1);
  jsStrBuf.append("compTabs['BuildSet'][compTabs['BuildSet'].length]= 'BuildSet';");

  if (!isPrograms)
    sbDefn.clear().append(jsStrBuf);

  if (writeOut)
    writeToFile(CONFIGMGR_JSPATH"buildset.js", jsStrBuf);

  return true;
}

bool generateHeadersFromEnv(const IPropertyTree* pEnv, StringBuffer& sbDefn, bool writeOut)
{
  StringBuffer jsStrBuf(" var nodeFullData = new Array(); \
var nodeRoot = {}; \
nodeRoot['Name'] = 'Environment'; \
nodeRoot['DisplayName'] = 'Environment'; \
nodeRoot['CompType'] = 'Environment'; \
nodeRoot['Build'] = ''; \
nodeRoot['BuildSet'] = ''; \
nodeRoot['Params'] = ''; \
nodeRoot['id'] = 0; \
nodeRoot['parent'] = -1; \
nodeRoot['depth'] = 0; \
nodeFullData[0] = nodeRoot;  \
");

  StringBuffer compList, espServiceList, pluginsList;
  getInstalledComponents(NULL, compList, espServiceList, pluginsList, pEnv);
  jsStrBuf.append("nodeRoot['menuComps'] = new Array(").append(compList).append(");");
  jsStrBuf.append("nodeRoot['menuEspServices'] = new Array(").append(espServiceList).append(");");
  jsStrBuf.append("nodeRoot['menuPlugins'] = new Array(").append(pluginsList).append(");");

  short nodeIndex = 1;
  short index = 1;
  short compTypeIndex = 0;
  short buildSetIndex = 0;
  StringBuffer lastCompAdded;
  StringBuffer xPath;
  xPath.append("*");
  Owned<IPropertyTreeIterator> iter = pEnv->getElements(xPath.str(), iptiter_sort);

  ForEach(*iter)
  {
    IPropertyTree& compTypeTree = iter->query();
    StringBuffer compTypeName;
    compTypeTree.getName(compTypeName);

    if (!stricmp(compTypeName.str(), "Data") || !stricmp(compTypeName.str(), "EnvSettings") || !strcmp(compTypeName.str(), XML_TAG_PROGRAMS))
      continue;

    const char* pszCompTypeName = compTypeName.str();
    jsStrBuf.appendf("var node%s = {};", pszCompTypeName);
    jsStrBuf.appendf("node%s['Name'] = '%s';", pszCompTypeName, pszCompTypeName);

    if (!strcmp(pszCompTypeName, XML_TAG_PROGRAMS))
      jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszCompTypeName, "Builds");
    else if (!strcmp(pszCompTypeName, "EnvSettings"))
      jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszCompTypeName, "Environment Settings");
    else
      jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszCompTypeName, pszCompTypeName);

    jsStrBuf.appendf("node%s['CompType'] = '%s';", pszCompTypeName, pszCompTypeName);
    jsStrBuf.appendf("node%s['Build'] = '%s';", pszCompTypeName, "");
    jsStrBuf.appendf("node%s['BuildSet'] = '%s';", pszCompTypeName, "");
    jsStrBuf.appendf("node%s['menu'] = '%s';", pszCompTypeName, "m4");
    jsStrBuf.appendf("node%s['Params'] = '%s';", pszCompTypeName, "");
    jsStrBuf.appendf("node%s['id'] = %d;", pszCompTypeName, index);
    jsStrBuf.appendf("node%s['parent'] = %d;", pszCompTypeName, 0);
    jsStrBuf.appendf("node%s['depth'] = 1;", pszCompTypeName);
    jsStrBuf.appendf("nodeFullData[%d] = node%s;", nodeIndex++, pszCompTypeName);
    compTypeIndex = index;
    index++;

    Owned<IPropertyTreeIterator> iter2 = compTypeTree.getElements(xPath.str(), iptiter_sort);
    ForEach(*iter2)
    {
      IPropertyTree &compTree = iter2->query();
      StringBuffer compName;
      compTree.getName(compName);
      const char* pszCompName = compName.str();
      StringBuffer build;
      StringBuffer buildset;
      StringBuffer compAttrName;
      build = compTree.queryProp(XML_ATTR_BUILD);
      buildset = compTree.queryProp(XML_ATTR_BUILDSET);

      xPath.clear().appendf("%s", pszCompName);
      short multipleComps = treeHasMultipleCompsOfSameType(&compTypeTree, xPath.str());
      xPath.clear().append("*");

      if (compTree.hasProp(XML_ATTR_BUILD) && compTree.hasProp(XML_ATTR_BUILDSET))
      {
        const char* pszBuildset;

        if (!strcmp(pszCompName, XML_TAG_ESPSERVICE))
          pszBuildset = XML_TAG_ESPSERVICE;
        else if (!strcmp(pszCompName, XML_TAG_PLUGINPROCESS))
          pszBuildset = "Plugin";
        else if(!strcmp(pszCompName, XML_TAG_ECLSERVERPROCESS))
          pszBuildset = "EclServer";
        else
          pszBuildset = buildset.str();

        if ((multipleComps > 1) && (lastCompAdded.length() == 0 || strcmp(lastCompAdded.str(), pszBuildset))) 
        {
          char szBuf[200];
          GetDisplayProcessName(compTree.queryName(), szBuf);
          compAttrName.append(szBuf).appendf(" (%d)", multipleComps);

          jsStrBuf.appendf("var node%s = {};", pszBuildset);
          jsStrBuf.appendf("node%s['Name'] = '%s';", pszBuildset, ""/*pszBuildset*/);
          jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszBuildset, compAttrName.str());
          jsStrBuf.appendf("node%s['CompType'] = '%s';", pszBuildset, pszBuildset);
          jsStrBuf.appendf("node%s['Build'] = '%s';", pszBuildset, "");
          jsStrBuf.appendf("node%s['BuildSet'] = '%s';", pszBuildset, "");
          jsStrBuf.appendf("node%s['menu'] = '%s';", pszBuildset, "");
          jsStrBuf.appendf("node%s['Params'] = '%s';", pszBuildset, "");
          jsStrBuf.appendf("node%s['id'] = %d;", pszBuildset, index);
          jsStrBuf.appendf("node%s['parent'] = %d;", pszBuildset, compTypeIndex);
          jsStrBuf.appendf("node%s['depth'] = 2;", pszBuildset);
          jsStrBuf.appendf("nodeFullData[%d] = node%s;", nodeIndex++, pszBuildset);
          buildSetIndex = index;
          index++;
        }

        lastCompAdded.clear().append(pszBuildset);

        GetDisplayName(&compTree, compAttrName, (multipleComps <= 1));

        jsStrBuf.appendf("var node%s = {};", pszCompName);
        jsStrBuf.appendf("node%s['Name'] = '%s';", pszCompName, compTree.queryProp(XML_ATTR_NAME));
        jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszCompName, compAttrName.str());
        jsStrBuf.appendf("node%s['CompType'] = '%s';", pszCompName, pszCompName);
        jsStrBuf.appendf("node%s['Build'] = '%s';", pszCompName, build.str());
        jsStrBuf.appendf("node%s['BuildSet'] = '%s';", pszCompName, buildset.str());
        jsStrBuf.appendf("node%s['menu'] = '%s';", pszCompName, "");
        jsStrBuf.appendf("node%s['Params'] = '%s';", pszCompName, "");
        jsStrBuf.appendf("node%s['id'] = %d;", pszCompName, index);
        jsStrBuf.appendf("node%s['parent'] = %d;", pszCompName, (multipleComps > 1)?buildSetIndex:compTypeIndex);
        jsStrBuf.appendf("node%s['depth'] = %d;", pszCompName, (multipleComps > 1)?3:2);
        jsStrBuf.appendf("nodeFullData[%d] = node%s;", nodeIndex++, pszCompName);
        index++;
      }
      else if (!strcmp(pszCompName, "Directories"))
      {
        jsStrBuf.appendf("var node%s = {};", pszCompName);
        jsStrBuf.appendf("node%s['Name'] = '%s';", pszCompName, pszCompName);
        jsStrBuf.appendf("node%s['DisplayName'] = '%s';", pszCompName, pszCompName);
        jsStrBuf.appendf("node%s['CompType'] = '%s';", pszCompName, pszCompName);
        jsStrBuf.appendf("node%s['Build'] = '';", pszCompName);
        jsStrBuf.appendf("node%s['BuildSet'] = '';", pszCompName);
        jsStrBuf.appendf("node%s['menu'] = '%s';", pszCompName, "");
        jsStrBuf.appendf("node%s['Params'] = '%s';", pszCompName, "");
        jsStrBuf.appendf("node%s['id'] = %d;", pszCompName, index);
        jsStrBuf.appendf("node%s['parent'] = %d;", pszCompName, compTypeIndex);
        jsStrBuf.appendf("node%s['depth'] = %d;", pszCompName, 2);
        jsStrBuf.appendf("nodeFullData[%d] = node%s;", nodeIndex++, pszCompName);
        index++;
      }
    }
  }

  jsStrBuf.append("function getNavTreeData(){return nodeFullData;}");
  jsStrBuf.append("(function(){navTreeData = nodeFullData;})();");
  sbDefn.clear().append(jsStrBuf);

  if (writeOut)
  {
    StringBuffer jsName(CONFIGMGR_JSPATH);
    jsName.append("navtreedata.js");
    writeToFile(jsName, jsStrBuf);
  }
  
  return true;
}

void generateHeaderForMisc()
{
  //this file is expected when the environment is updated. so just create an empty file
  StringBuffer jsName(CONFIGMGR_JSPATH);
  jsName.append("refresh.js");
  Owned<IFile> pFile = createIFile(jsName);
}

bool generateHeaders(const IPropertyTree* pEnv, IConstEnvironment* pConstEnv)
{
  StringBuffer sbTemp;
  generateHeadersFromEnv(pEnv, sbTemp, true);
  generateHardwareHeaders(pEnv, sbTemp, true);
  generateBuildHeaders(pEnv, true, sbTemp, true);
  StringBuffer xPath;
  xPath.append("Software/*");
  Owned<IPropertyTreeIterator> iter2 = pEnv->getElements(xPath.str());
  ForEach(*iter2)
  {
    IPropertyTree &agDef = iter2->query();
    if (agDef.hasProp(XML_ATTR_BUILD) && agDef.hasProp(XML_ATTR_BUILDSET))
    {
      StringBuffer build;
      StringBuffer buildset;
      StringBuffer schemaPath;
      StringBuffer jsName;
      StringBuffer compName;
      build = agDef.queryProp(XML_ATTR_BUILD);
      buildset = agDef.queryProp(XML_ATTR_BUILDSET);
      StringBuffer agName;
      agDef.getName(agName);

      if (!strcmp(agName.str(), XML_TAG_ESPSERVICE) || !strcmp(agName.str(), XML_TAG_PLUGINPROCESS))
        compName.append(buildset);
      else
        compName.append(agName);

      jsName.append(CONFIGMGR_JSPATH).append(compName).append(".js");

      StringBuffer s;
      s.append("./Programs/Build[@name='").append(build).append("']");
      IPropertyTree *b = pEnv->queryPropTree(s.str());

      if (b)
      {
        s.clear().append("BuildSet[@name='").append(buildset).append("']");
        IPropertyTree *bs = b->queryPropTree(s.str());
        IPropertyTree * pTree = loadSchema(b, bs, schemaPath, pConstEnv);
        fprintf(stdout, "Loading schema file %s", schemaPath.str());
        try
        {
          CGenerateJSFromXSD obj(pEnv, pTree, jsName, compName);
          obj.generateHeaders();
        }
        catch(IException *E)
        {
          StringBuffer buf;
          (E->errorMessage(buf).str());
          printf("%s", buf.str());
          E->Release();
        }
      }
    }
  }

  generateHeaderForTopology(pEnv, sbTemp, true);
  generateHeaderForDeployableComps(pEnv, sbTemp, true);
  generateHeaderForMisc();
  return true;
}

bool getComputersListWithUsage(const IPropertyTree* pEnv, StringBuffer& sbComputers, StringBuffer& sbFilter)
{
  CComputerPicker cpick;
  cpick.SetRootNode(pEnv);
  sbComputers.clear();
  toXML(cpick.getComputerTree(), sbComputers, false);
  toXML(cpick.getFilterTree(), sbFilter, false);

  return true;
}


bool handleRoxieOperation(IPropertyTree* pEnv, const char* cmd, const char* xmlStr)
{
  CConfigEnvHelper configEnv(pEnv);
  bool result = configEnv.handleRoxieOperation(cmd, xmlStr);

  return result;
}

bool handleThorTopologyOp(IPropertyTree* pEnv, const char* cmd, const char* xmlStr, StringBuffer& sMsg)
{
  CConfigEnvHelper configEnv(pEnv);
  bool result = configEnv.handleThorTopologyOp(cmd, xmlStr, sMsg);

  return result;
}

void addComponentToEnv(IPropertyTree* pEnv, const char* buildSet, StringBuffer& sbNewName, IPropertyTree* pCompTree)
{
  CConfigEnvHelper configEnv(pEnv);
  configEnv.addComponent(buildSet, sbNewName, pCompTree);
}

bool generateHeaderForComponent(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName)
{
  try
  {
    StringBuffer jsName;
    jsName.append(CONFIGMGR_JSPATH).append(compName).append(".js");
    CGenerateJSFromXSD obj(pEnv, pSchema, jsName.str(), compName);
    obj.generateHeaders();
    return true;
  }
  catch(IException *E)
  {
    StringBuffer buf;
    (E->errorMessage(buf).str());
    printf("%s", buf.str());
    E->Release();
  }

  return false;
}

void deleteRecursive(const char* path)
{
        Owned<IFile> pDir = createIFile(path);
        if (pDir->exists())
        {
            if (pDir->isDirectory())
            {
                Owned<IDirectoryIterator> it = pDir->directoryFiles(NULL, false, true);
                ForEach(*it)
                {               
                    StringBuffer name;
                    it->getName(name);
                    
                    StringBuffer childPath(path);
                    childPath.append(PATHSEPCHAR);
                    childPath.append(name);
                    
                    deleteRecursive(childPath.str());
                }
            }
            pDir->remove();
        }
}

void getTabNameArray(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, StringArray& strArray)
{
    try
  {
    CGenerateJSFromXSD obj(pEnv, pSchema, "", compName);
    obj.generateHeaders();
    obj.getTabNameArray(strArray);
  }
  catch(IException *E)
  {
    StringBuffer buf;
    (E->errorMessage(buf).str());
    printf("%s", buf.str());
    E->Release();
  }
}

void getDefnPropTree(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, IPropertyTree* pDefTree, StringBuffer xpathDefn)
{
    try
  {
    CGenerateJSFromXSD obj(pEnv, pSchema, "", compName);
    obj.getDefnPropTree(pDefTree, xpathDefn);
  }
  catch(IException *E)
  {
    StringBuffer buf;
    (E->errorMessage(buf).str());
    printf("%s", buf.str());
    E->Release();
  }
}

void getDefnString(const IPropertyTree* pEnv, IPropertyTree* pSchema, const char* compName, StringBuffer& compDefn, StringBuffer& viewChildNodes, StringBuffer& multiRowNodes)
{
  try
  {
    CGenerateJSFromXSD obj(pEnv, pSchema, "", compName);
    obj.getDefnString(compDefn, viewChildNodes, multiRowNodes);
  }
  catch(IException *E)
  {
    StringBuffer buf;
    (E->errorMessage(buf).str());
    printf("%s", buf.str());
    E->Release();
  }
}

bool checkComponentReferences(const IPropertyTree* pEnv, 
                              IPropertyTree* pOrigNode, 
                              const char* szName, 
                              const char* xpath, 
                              StringBuffer& sMsg,
                              const StringArray& attribArray,
                              const char* szNewName/*=NULL*/)
{
  const IPropertyTree* pSoftware = pEnv->queryPropTree(XML_TAG_SOFTWARE);

  // split xpath into 2 strings: one for component and the other for its childrens' xpath
  // so we can report its name, if needed.  For instance, if xpath is 
  // "Topology/EclServerProcess/EclAgentProcess" then create 2 separate xpaths: 
  // "Topology" and "EclServerProcess/EclAgentProcess" so we can report Topology's name.
  //
  StringBuffer xpath1;//component
  const char* xpath2;//remaining
  const char* pSlash = strchr(xpath, '/');
  if (pSlash)
  {
    String str(xpath);
    String* pStr = str.substring(0, strcspn(xpath, "/"));
    xpath1.append(pStr->str());
    delete pStr;
    xpath2 = pSlash+1;
  }
  else
  {
    xpath1.append(xpath);
    xpath2 = NULL;
  }

  const bool bEspProcess = !strcmp(pOrigNode->queryName(), XML_TAG_ESPPROCESS);
  int nAttribs = attribArray.length();
  Owned<IPropertyTreeIterator> iComp = pSoftware->getElements(xpath1);
  ForEach(*iComp)
  {
    IPropertyTree* pComp = &iComp->query();
    if (pComp == pOrigNode)//resolve circular dependency - don't check against the original node!
      continue;

    Owned<IPropertyTreeIterator> iter;
    if (xpath2)
      iter.setown(pComp->getElements(xpath2));
    else
    {
      iComp->Link();
      iter.setown(iComp.get());
    }

    ForEach(*iter)
    {
      pComp = &iComp->query(); //inner loop may have changed the component if xpath2 is NULL
      if (pComp == pOrigNode)//resolve circular dependency - don't check against the original node!
        continue;

      IPropertyTree* pNode = &iter->query();
      for (int i=0; i<nAttribs; i++)
      {
        const char* attribName = attribArray.item(i);

        const char* szValue = pNode->queryProp(attribName);
        if (!szValue)
          continue;

        bool bMatch;                
        if (bEspProcess)
        {
          const unsigned int len = strlen(szName);
          bMatch = !strncmp(szValue, szName, len) && szValue[len] == '/';
        }
        else
          bMatch = strcmp(szValue, szName)==0;

        if (bMatch)
        {
          if (szNewName==NULL)
          {
            const char* szCompName = pComp->queryProp(XML_ATTR_NAME);
            const char* szElemName = pComp->queryName();

            sMsg.appendf("Component '%s' is referenced by %s %s component", szName, szCompName ? "the":"an instance of", szElemName);

            if (szCompName)
              sMsg.appendf(" '%s'", szCompName);

            sMsg.append(".\nYou must remove all references before it can be deleted.");

            return false;
          }
          else
          {
            if (bEspProcess)
            {
              StringBuffer sNewName(szNewName);
              sNewName.append(szValue).appendf("%d", (int) strlen(szName));
              pNode->setProp(attribName, sNewName);
            }
            else
              pNode->setProp(attribName, szNewName);
          }
        }
      }
    }

    if (xpath2==NULL)
      break;
  }
  return true;
}

bool checkComponentReferences(const IPropertyTree* pEnv, IPropertyTree* pNode, const char* szPrevName, StringBuffer& sMsg, const char* szNewName/*=NULL*/)
{
  const char* szProcess = pNode->queryName();

  // A component may be referenced by other components with any attribute name 
  // (and not just @process), for e.g. @eclServer, @mySQL, @MySQL etc.  The 
  // components are inter-twined with cross links amongst them and there is 
  // no way to figure them out dynamically generically based on just the 
  // schema etc. (we only load one schema at a time anyway).  

  // So we would hard code these rules for dependency checks based on current
  // relationships until we figure out a better way to do the same in future.  
  // The drawback is that these rules will have to be kept in sync with the
  // the introduction of newer relationships.

  // We need to check for other components with different xpaths and each
  // with possibly more than one attribute name so define an StringArray 
  // to store xpaths and another array of StringArray objects to hold list of
  // attributes corresponding to the xpaths to be validated.
  //
  // This avoids multiple traversals of the xpath for multiple attribute names.
  //
  StringArray xpathArray;
  StringArray attribArray[6];//we don't add attributes for more than 6 xpaths 
  //as for EspBinding below
  int numXpaths = 0;
  StringArray& attribs = attribArray[numXpaths++];

  if (!strcmp(szProcess, XML_TAG_DALISERVERPROCESS))
  {
    xpathArray.append("*");
    attribs.append("@daliServer");//EclServerProcess
    attribs.append("@daliServers");
    attribs.append("@daliservers");//DfuProcess
  }
  else if (!strcmp(szProcess, XML_TAG_ECLAGENTPROCESS))
  {
    xpathArray.append("Topology/Cluster/EclAgentProcess");
    attribs.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_ECLSERVERPROCESS))
  {
    xpathArray.append("*");
    attribs.append("@eclServer");

    xpathArray.append("Topology/EclServerProcess");
    StringArray& attribs2 = attribArray[numXpaths++];
    attribs2.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_ECLCCSERVERPROCESS))
  {
    xpathArray.append("*");
    attribs.append("@eclServer");

    xpathArray.append("Topology/Cluster/EclCCServerProcess");
    StringArray& attribs2 = attribArray[numXpaths++];
    attribs2.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_ECLSCHEDULERPROCESS))
  {
    xpathArray.append("*");
    attribs.append("@eclScheduler");

    xpathArray.append("Topology/Cluster/EclSchedulerProcess");
    StringArray& attribs2 = attribArray[numXpaths++];
    attribs2.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_ESPSERVICE))
  {
    xpathArray.append("EspProcess/EspBinding");
    attribs.append("@service");
  }
  else if (!strcmp(szProcess, "LDAPServerProcess"))
  {
    xpathArray.append("*");
    attribs.append("@ldapServer");
    attribs.append("ldapSecurity/@server");//under EclServer

    xpathArray.append("EspProcess/Authentication");
    StringArray& attribs2 = attribArray[numXpaths++];
    attribs2.append("@ldapServer");
  }
  else if (!strcmp(szProcess, "MySQLProcess"))
  {
    xpathArray.append("*");
    attribs.append("@mySql");
    attribs.append("@MySql");
    attribs.append("@MySQL");
    attribs.append("@database");
  }
  else if (!strcmp(szProcess, XML_TAG_PLUGINPROCESS))
  {
    xpathArray.append("EclServerProcess/PluginRef");
    attribs.append("@process");
  }
  else if (!strcmp(szProcess, "RoxieCluster"))
  {
    xpathArray.append("Topology/Cluster/RoxieCluster");
    attribs.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_THORCLUSTER))
  {
    xpathArray.append("Topology/Cluster/ThorCluster");
    attribs.append("@process");
  }
  else if (!strcmp(szProcess, XML_TAG_ESPBINDING) || !strcmp(szProcess, XML_TAG_ESPPROCESS))
  {
    xpathArray.append(XML_TAG_ESPSERVICE); 
    attribs.append("@eclWatch");          //ws_ecl
    attribs.append("@attributeServer");//ws_ecl and ws_roxieconfig

    xpathArray.append("EspService/WsEcl");//ws_facts and ws_distrix
    attribArray[numXpaths++].append("@espBinding"); 

    xpathArray.append("EspService/SourceAttributeServer");//ws_roxieconfig
    attribArray[numXpaths++].append("@espBinding"); 

    xpathArray.append(XML_TAG_ECLSERVERPROCESS);
    attribArray[numXpaths++].append("@eclWatch");

    xpathArray.append("DfuplusProcess");
    attribArray[numXpaths++].append("@server");

    xpathArray.append("RegressionSuite");
    StringArray& attribs2 = attribArray[numXpaths++];
    attribs2.append("@server");
    attribs2.append("@roxieconfig");
  }
  else
  {
    xpathArray.append("*");
    attribs.append(XML_ATTR_NAME);
  }

  bool rc = true;
  for (int i=0; i<numXpaths && rc; i++)
    rc = checkComponentReferences(pEnv, pNode, szPrevName, xpathArray.item(i), sMsg, attribArray[i], szNewName);

  return rc;
}


const char* getUniqueName(const IPropertyTree* pEnv, StringBuffer& sName, const char* processName, const char* category)
{
  //if the name ends in _N (where N is a number) then ignore _N to avoid duplicating
  //number suffix as in _N_M
  //
  StringBuffer sPrefix = sName;
  const char* pdx = strrchr(sName.str(), '_');
  if (pdx)
  {
    StringBuffer num(sName);
    char* pszNum = num.detach();

    char *token = NULL;
    j_strtok_r(pszNum, "_", &token);

    if (strspn(token, "0123456789") == strlen(token))
    {
      sName.remove(pdx - sName.str(), sName.length() - (pdx - sName.str()));
      sPrefix.clear().append(sName);
    }
    else
    {
      int len = sPrefix.length();
      if (len > 0 && endsWith(sPrefix.str(), "_")) //ends with '_'
        sPrefix = sPrefix.remove(sPrefix.length() - 1, 1); //lose it
    }

    free(pszNum);
  }

  StringBuffer xpath;
  xpath.appendf("./%s/%s[@name='%s']", category, processName, sName.str());
  int iIdx = 2;

  while (pEnv->queryPropTree(xpath))
  {
    sName.clear().appendf("%s_", sPrefix.str()).append(iIdx);
    xpath.clear().appendf("./%s/%s[@name='%s']", category, processName, sName.str());
    iIdx++;
  }

  return sName.str();
}


const char* getUniqueName2(const IPropertyTree* pEnv, StringBuffer& sName, const char* processName, const char* keyAttrib)
{
  //if the name ends in _N (where N is a number) then ignore _N to avoid duplicating
  //number suffix as in _N_M
  //
  StringBuffer sPrefix = sName;
  const char* pdx = strrchr(sName.str(), '_');
  if (pdx)
  {
    StringBuffer num(sName);
    char* pszNum = num.detach();

    char *token = NULL;
    j_strtok_r(pszNum, "_", &token);

    if (strspn(token, "0123456789") == strlen(token))
    {
      sName.remove(pdx - sName.str(), sName.length() - (pdx - sName.str()));
      sPrefix.clear().append(sName);
    }
    else
    {
      int len = sPrefix.length();
      if (len > 0 && endsWith(sPrefix.str(), "_")) //ends with '_'
        sPrefix = sPrefix.remove(sPrefix.length() - 1, 1); //lose it
    }

    free(pszNum);
  }

  StringBuffer xpath;
  xpath.appendf("./%s[%s='%s']", processName, keyAttrib, sName.str());
  int iIdx = 2;

  while (pEnv->queryPropTree(xpath))
  {
    sName.clear().appendf("%s_", sPrefix.str()).append(iIdx);
    xpath.clear().appendf("./%s[%s='%s']", processName, keyAttrib, sName.str());
    iIdx++;
  }

  return sName.str();
}

void getCommonDir(const IPropertyTree* pEnv, const char* catType, const char* buildSetName, const char* compName, StringBuffer& sbVal)
{
  const IPropertyTree* pEnvDirs = pEnv->queryPropTree("Software/Directories");
  const char* commonDirName = pEnvDirs->queryProp(XML_ATTR_NAME);
  StringBuffer xpath;
  xpath.appendf("Category[@name='%s']", catType);
  Owned<IPropertyTreeIterator> iterCats = pEnvDirs->getElements(xpath.str());
  ForEach (*iterCats)
  {
    IPropertyTree* pCat = &iterCats->query();
    StringBuffer sb("Override");
    sb.appendf("[@component='%s'][@instance='%s']",buildSetName, compName);
    IPropertyTree* pCatOver = pCat->queryPropTree(sb.str());
    if (!pCatOver)
    {
      sb.clear().appendf("Override[@instance='%s']", compName);

      Owned<IPropertyTreeIterator> overIter = pCat->getElements(sb.str());
      ForEach(*overIter)
      {
        IPropertyTree* pTmp = &overIter->query();
        if (!pTmp->queryProp("@component"))
        {
          pCatOver = pTmp;
          sbVal.clear().append(pCatOver->queryProp("@dir"));
          break;
        }
      }

      if (!pCatOver)
        sbVal.clear().append(pCat->queryProp("@dir"));
    }
    else
      sbVal.clear().append(pCatOver->queryProp("@dir"));

    sbVal.replaceString("[COMPONENT]",buildSetName);
    sbVal.replaceString("[INST]", compName);
    sbVal.replaceString("[NAME]",  commonDirName);
    break;
  }
}

IPropertyTree* getNewRange(const IPropertyTree* pEnv, const char* prefix, const char* domain, const char* cType, const char* startIP, const char* endIP)
{
   StringBuffer sXML;
   int nCount = 0;

   IpAddress start(startIP);
   IpAddress end(endIP);
   unsigned s, e;
     start.getNetAddress(sizeof(s),&s);
   end.getNetAddress(sizeof(e),&e);
   if( s > e)
   {
     s^=e;
     e^=s;
     s^=e;
     const char* temp = startIP;
     startIP = endIP;
     endIP= temp;
   }

   if (start.isNull())
     throw MakeStringException(-1, "Invalid start ip address: %s", startIP);

   if (end.isNull())
     throw MakeStringException(-1, "Invalid stop ip address: %s", endIP);

   if ((s << 8) != (e << 8))
     throw MakeStringException(-1, "Start and stop IP addresses must be within same subnet");
  
   // Create string for common attributes
   StringBuffer attr, val, sAttributes;
   attr.appendf(" %s=\"%s\"", &XML_ATTR_DOMAIN[1], domain);
   attr.appendf(" %s=\"%s\"", &XML_ATTR_COMPUTERTYPE[1], cType);

   IpAddress range;
   StringBuffer iprange(startIP);
   String str(startIP);
   iprange.append("-").append(endIP + str.lastIndexOf('.') + 1);
   range.ipsetrange(iprange.str());
   StringBuffer sNode("<" XML_TAG_HARDWARE ">"), sName, sIP;
   int count = (e >> 24) - (s >> 24) + 1;
   nCount = count;
   
   while (count--)
   {
     range.getIpText(sIP.clear());

     unsigned x;
     range.getNetAddress(sizeof(x),&x);

     StringBuffer strCheckXPath;
     strCheckXPath.setf("%s/%s[%s=\"%s\"][1]", XML_TAG_HARDWARE, XML_TAG_COMPUTER, XML_ATTR_NETADDRESS, sIP.str());

     if (pEnv->hasProp(strCheckXPath.str()) == true)
     {
         range.ipincrement(1);
         continue;
     }

     sName.clear().appendf("%s%03d%03d", prefix, (x >> 16) & 0xFF, (x >> 24) & 0xFF);
     sNode.appendf("<" XML_TAG_COMPUTER " %s=\"%s\" %s=\"%s\" %s/>",
                      &XML_ATTR_NAME[1], getUniqueName(pEnv, sName, XML_TAG_COMPUTER, XML_TAG_HARDWARE),
                      &XML_ATTR_NETADDRESS[1], sIP.str(),
                      attr.str());
     range.ipincrement(1);
   }
   
   if (sNode.length() > 10)
   {
    sNode.append("</" XML_TAG_HARDWARE ">");
    IPropertyTree* pTree = createPTreeFromXMLString(sNode);
    return pTree;
   }
   else 
     return NULL;
}

bool ensureUniqueName(const IPropertyTree* pEnv, IPropertyTree* pParentNode, const char* sectionName, const char* newName)
{
  //this function finds out nodes with a given name in a section (Hardware, Software,
  //Programs or Data
  //
  bool bOriginalFound = false;
  bool bDuplicateFound = false;

  StringBuffer xpath(sectionName);
  xpath.append("/").append(pParentNode->queryName());

  Owned<IPropertyTreeIterator> iter = pEnv->getElements(xpath);
  ForEach(*iter)
  {
    IPropertyTree* pNode = &iter->query();
    const char* name = pNode->queryProp("@name");
    if (name)
    {
      if (pNode == pParentNode)
        bOriginalFound = true;
      else
        if (!strcmp(name, newName))
        {
          bDuplicateFound = true;//cannot exit loop prematurely since this 
          if (bOriginalFound)     //until this is set 
            break;
        }
    }
  }

  if (bOriginalFound && bDuplicateFound)
  {
    throw MakeStringException(-1, "Another %s already exists with the same name!\nPlease specify a unique name", 
      pParentNode->queryName());
  }

  return true;
}

bool ensureUniqueName(const IPropertyTree* pEnv, IPropertyTree* pParentNode, const char* szText)
{
  if (!strcmp(szText, "Directories"))
    throw MakeStringException(-1, "%s already exists!\nPlease specify a unique name", szText);

  bool rc = ensureUniqueName(pEnv, pParentNode, "Software", szText) &&
    ensureUniqueName(pEnv, pParentNode,"Hardware", szText) &&
    ensureUniqueName(pEnv, pParentNode,"Programs", szText);

  return rc;
}

const char* expandXPath(StringBuffer& xpath, IPropertyTree* pNode, IPropertyTree* pParentNode, int position)
{
  StringBuffer xpathOut;
  StringBuffer subxpath = strpbrk(xpath.str(), "/=");
  if (!strcmp(subxpath.str(), ".."))
  {
    int skip = 2;
    if (xpath.length() > 2 && xpath.charAt(2) == '/')
      skip++;
    subxpath = strpbrk(xpath.str() +  skip, "/=]");
    xpathOut.append(expandXPath(subxpath, pParentNode, NULL, -1));
  }
  else
    if (!strcmp(subxpath.str(), "position()"))
    {
      char sPos[32];
      itoa(position, sPos, 10);
      StringBuffer sb(xpathOut.str() +  position);
      xpathOut.clear().append(sb);
    }
    else
      if (subxpath.length() && subxpath.charAt(0) == '@')
        xpathOut.append("`").append(pNode->queryProp(subxpath.str())).append("`");

  xpath.clear().append(xpathOut);
  return xpath;
}

bool xsltTransform(const StringBuffer& xml, const char* sheet, IProperties *params, StringBuffer& ret)
{
  if (!checkFileExists(sheet))
    throw MakeStringException(-1, "Could not find stylesheet %s",sheet);

  Owned<IXslProcessor> proc  = getXslProcessor();
  Owned<IXslTransform> trans = proc->createXslTransform();

  trans->setXmlSource(xml.str(), xml.length());
  trans->loadXslFromFile(sheet);

  if (params)
  {
    Owned<IPropertyIterator> it = params->getIterator();
    for (it->first(); it->isValid(); it->next())
    {
      const char *key = it->getPropKey();
      //set parameter in the XSL transform skipping over the @ prefix, if any
      const char* paramName = *key == '@' ? key+1 : key;
      trans->setParameter(paramName, StringBuffer().append('\'').append(params->queryProp(key)).append('\'').str());
    }
  }

  trans->transform(ret);
  return true;
}

bool onChangeAttribute(const IPropertyTree* pEnv,
                       IConstEnvironment* pConstEnv,
                       const char* attrName, 
                       IPropertyTree* pOnChange, 
                       IPropertyTree*& pNode, 
                       IPropertyTree* pParentNode, 
                       int position, 
                       const char* szNewValue, 
                       const char* prevValue,
                       const char* buildSet)
{
  bool rc = false;
  StringBuffer sbAttr("@");
  sbAttr.append(attrName);
  try
  {
    IPropertyTree* pComponent = pNode;
    const char* xslt = pOnChange->queryProp("xslt");
    StringBuffer xpath("Programs/Build");
    IPropertyTree *pBuild = pEnv->queryPropTree(xpath.str());

    if (pBuild)
    {
      xpath.clear().append("BuildSet[@name='").append(buildSet).append("']");
      IPropertyTree *pBuildSet = pBuild->queryPropTree(xpath.str());
      StringBuffer sXsltPath;
      if (pBuildSet && connectBuildSet(pBuild, pBuildSet, sXsltPath, pConstEnv))
      {
        sXsltPath.append(xslt);
        Owned<IProperties> params(createProperties());
        params->setProp("@attribName", attrName);
        params->setProp("@oldValue", prevValue);
        params->setProp("@newValue", szNewValue);

        xpath = pOnChange->queryProp("xpath");
        if (xpath.length())
        {
          /* sample xpath is as follows so expand it:
          RemoteNScfg[@espBinding=current()/../@espBinding]/Configuration[current()/position()]
          */
          const char* pos;
          while ((pos=strstr(xpath.str(), "current()/")) != NULL)
          {
            const char* pos2 = pos + sizeof("current()/")-1;
            StringBuffer subxpath(strpbrk(strstr(xpath.str(), pos2), "=]"));
            const int len = subxpath.length();

            //xpath = xpath.Left(pos) + expandXPath(subxpath, pNode, pParentNode, position) + xpath.Mid(pos2+len);
          }
          params->setProp("@xpath", xpath);
        }

        const char* source = pOnChange->queryProp("xml");
        IPropertyTree* pSourceNode = pNode;
        if (source && *source)//default is just the element whose attribute got changed
        {
          if (!stricmp(source, "component"))
            pSourceNode = pComponent;
          else
            throw MakeStringException(0, "Invalid source specified.");
        }

        StringBuffer xml;
        toXML(pSourceNode, xml);

        StringBuffer ret;
        if (xsltTransform(xml, sXsltPath, params, ret))
        {
          Owned<IPropertyTree> result = createPTreeFromXMLString(ret.str());
          
          Owned<IAttributeIterator> iAttr = result->getAttributes();
          ForEach(*iAttr)
          {
            const char* attrName = iAttr->queryName();

            if (!pSourceNode->hasProp(attrName))
              pSourceNode->addProp(attrName, iAttr->queryValue());
            else
              pSourceNode->setProp(attrName, iAttr->queryValue());
          }

          rc = true;
        }
      }
    }
  }
  catch (IException* e)
  {
    pNode->setProp(sbAttr.str(), prevValue);
    StringBuffer sMsg;
    e->errorMessage(sMsg);
    throw e;
  }
  catch(...)
  {
    pNode->setProp(sbAttr.str(), prevValue);
    throw;
  }

  if (!rc)
    pNode->setProp(sbAttr.str(), prevValue);

  return rc;
}

void UpdateRefAttributes(IPropertyTree* pEnv, const char* szPath, const char* szAttr, const char* szOldVal, const char* szNewVal)
{
  Owned<IPropertyTreeIterator> iter = pEnv->getElements(szPath);
  for (iter->first(); iter->isValid(); iter->next())
  {
    IPropertyTree& node = iter->query();
    const char* szVal = node.queryProp(szAttr);
    if (szVal && strcmp(szVal, szOldVal)==0)
      node.setProp(szAttr, szNewVal);      
  }
}

void addInstanceToCompTree(const IPropertyTree* pEnvRoot,const IPropertyTree* pInstance,StringBuffer& dups,StringBuffer& resp,IConstEnvironment* pConstEnv)
{
  StringBuffer buildSetPath, xpath;
  const char* buildSet = pInstance->queryProp(XML_ATTR_BUILDSET);
  const char* compName = pInstance->queryProp("@compName");

  xpath.appendf("./Programs/Build/BuildSet[@name=\"%s\"]", buildSet);
  Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
  buildSetIter->first();
  IPropertyTree* pBuildSet = &buildSetIter->query();
  const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);

  Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, pConstEnv);
  xpath.clear().appendf("./Software/%s[@name=\"%s\"]", processName, compName);
  IPropertyTree* pCompTree = pEnvRoot->queryPropTree(xpath.str());

  Owned<IPropertyTreeIterator> iterInst = pInstance->getElements("*");
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
            resp.append(szAttrib);
          }
        }
        else if (!strcmp(attrName.str(), XML_ATTR_NETADDRESS))
          szAttrib = pComputerNode->queryProp(XML_ATTR_NETADDRESS);
        else if (!strcmp(attrName.str(), XML_ATTR_DIRECTORY))
        {
          StringBuffer rundir;
          if (!getConfigurationDirectory(pEnvRoot->queryPropTree("Software/Directories"), "run", processName, compName, rundir))
            sb.clear().appendf(RUNTIME_DIR"/%s", compName);
          else
            sb.clear().append(rundir);

          szAttrib = sb.str();
        }
        else
          szAttrib = attr.queryProp("@default");
        pNode->addProp(attrName.str(), szAttrib);
      }
    }
  }

  int nCount = 1;
  xpath.clear().appendf("Instance");
  Owned<IPropertyTreeIterator> iter = pCompTree->getElements(xpath.str());
  StringBuffer sName;

  ForEach(*iter)
  {
    sName.clear().append("s").append(nCount);
    iter->query().setProp(XML_ATTR_NAME, sName.str());
    nCount++;
  }
}

void formIPList(const char* ip, StringArray& formattedIpList)
{
  StringBuffer ipList(ip);
  if(ipList.length())
  {
    ipList.replace('\n',';');
    if(ipList.charAt(ipList.length()-1) == ';')
         ipList.setCharAt((ipList.length()-1),' ');

    StringArray sArray;
    sArray.appendList(ipList, ";");

    if(sArray.ordinality() > 0 )
    {
       for( unsigned i = 0; i < sArray.ordinality() ; i++)
       {
          const char* ip = sArray.item(i);
          if(ip && *ip)
          {
              if( strchr(ip, '-') != 0 )
              {
                StringArray rangeArr, commIPPart ;
                StringBuffer comip;
                rangeArr.appendList(ip, "-");

                if( rangeArr.ordinality() == 2 )
                {
                   unsigned endAddr = atoi(rangeArr.item(1));
                   //to get common part of IP
                   commIPPart.appendList(rangeArr.item(0),".");
                   StringBuffer newip;
                   if(commIPPart.ordinality() == 4)
                   {
                     unsigned startAddr = atoi(commIPPart.item(3));
                     comip.clear().append(commIPPart.item(0)).append(".").append(commIPPart.item(1)).append(".").append(commIPPart.item(2)).append(".");
                     if( startAddr > endAddr)
                     {
                       startAddr^=endAddr;
                       endAddr^=startAddr;
                       startAddr^=endAddr;
                     }
                     
                     while(startAddr <= endAddr)
                     {
                       newip.clear().append(comip).append(startAddr);
                       startAddr++;
                       formattedIpList.appendUniq(newip);
                     }
                   }
                }
              }
              else
              {
                formattedIpList.appendUniq(ip);
              }
          }
       }
    }
 }
 else
     throw MakeStringException(-1, "List of IP Addresses cannot be empty");
}

void buildEnvFromWizard(const char * wizardXml, const char* service,IPropertyTree* cfg, StringBuffer& envXml, MapStringTo<StringBuffer>* dirMap)
{
  if(wizardXml && *wizardXml)
  {
    CWizardInputs wizardInputs(wizardXml, service, cfg, dirMap);
    wizardInputs.setEnvironment();
    wizardInputs.generateEnvironment(envXml);
    if(envXml.length() == 0)
      throw MakeStringException(-1, "Failed to generated the environment xml for unknown reason");
  }
  else
    throw MakeStringException(-1, "User inputs are needed to generate the environment");
}

void runScript(StringBuffer& output, StringBuffer& errMsg, const char* pathToScript)
{
  StringBuffer cmdLine;
  if(checkFileExists(pathToScript))
  {
    char buffer[128];
    cmdLine.clear().append(pathToScript);
#ifdef _WINDOWS
    FILE *fp = _popen(cmdLine.str(), "r");
#else
    FILE *fp = popen(cmdLine.str(), "r");
#endif
    if(fp != NULL)
    {
      while ( !feof(fp) )
      {
        if( fgets(buffer, 128, fp))
        {
          output.append(buffer);
        }
      }
      if(ferror(fp))
          errMsg.clear().append("Some file operation error");
#ifdef _WINDOWS
      _pclose(fp);
#else
       pclose(fp);
#endif
     if( output.length() == 0)
       errMsg.clear().append("No IPAddresses found for environment.");
    }
    else
      errMsg.clear().append("Could not open or run autodiscovery script ").append(pathToScript);
 }
 else
    throw MakeStringException(-1,"The Script [%s] for getting IP addresses for environment does not exist", pathToScript);
}

bool validateIPS(const char* ipAddressList)
{
  StringArray ipFormatted ;
  formIPList(ipAddressList,ipFormatted);
  if(ipFormatted.ordinality() > 0)
  {
    for (unsigned i = 0; i < ipFormatted.ordinality(); i++)
    {
       const char* ip = ipFormatted.item(i);
       unsigned x ;
       IpAddress ipaddr(ip);
       ipaddr.getNetAddress(sizeof(x), &x);
       if ( ipaddr.isNull())
        throw MakeStringException(-1, "Invalid ip address: %s", ip);
    }
  }
  else
     throw MakeStringException(-1, "List for IP Addresses cannot be empty");
  return true;
}

void getSummary(const IPropertyTree* pEnvRoot, StringBuffer& respXmlStr, bool prepareLink)
{
  if(pEnvRoot)
  {
    StringBuffer xpath, compName, ipAssigned, computerName, linkString, buildSetName;
    Owned<IPropertyTree> pSummaryTree = createPTree("ComponentList");
    IPropertyTree* pSWCompTree  = pEnvRoot->queryPropTree(XML_TAG_SOFTWARE);
        
    if(pSWCompTree)
    {
      Owned<IPropertyTreeIterator> swCompIter = pSWCompTree->getElements("*");
      StringArray espServiceArr;
      ForEach(*swCompIter)
      {
      
        bool instanceFound = false;
        IPropertyTree* pCompTree = &swCompIter->query();
        if(pCompTree)
        {
          ipAssigned.clear();
          compName.clear().append(pCompTree->queryProp(XML_ATTR_NAME));
          buildSetName.clear().append(pCompTree->queryProp(XML_ATTR_BUILDSET));

          xpath.clear().append("./Instance");
          Owned<IPropertyTreeIterator> instanceIter = pCompTree->getElements(xpath.str());
          ForEach(*instanceIter)
          {
            instanceFound = true;
            IPropertyTree* pInstance = &instanceIter->query();
            if(pInstance)
            {
              const char* netAddr = pInstance->queryProp(XML_ATTR_NETADDRESS);
              if(netAddr && *netAddr)
              {
                ipAssigned.append(netAddr);
                ipAssigned.append(",");
              }
            }
          }

          if(!strcmp(pCompTree->queryName(), XML_TAG_ESPPROCESS))
          {
            if(ipAssigned.length())
            {
              Owned<IPropertyTreeIterator> espSerIter = pCompTree->getElements("./" XML_TAG_ESPBINDING);
              ForEach(*espSerIter)
              {
                IPropertyTree* pEspBinding = &espSerIter->query();
                const char* serviceName = pEspBinding->queryProp(XML_ATTR_SERVICE);
                const char* port = pEspBinding->queryProp(XML_ATTR_PORT);
                const char* protocol = pEspBinding->queryProp(XML_ATTR_PROTOCOL);
                const char* buildset = NULL;
                xpath.clear().appendf("./%s/%s[%s=\"%s\"]", XML_TAG_SOFTWARE, XML_TAG_ESPSERVICE, XML_ATTR_NAME, serviceName); 
                IPropertyTree* pEspService = pEnvRoot->queryPropTree(xpath.str());
                if(pEspService)
                  buildset = pEspService->queryProp(XML_ATTR_BUILDSET);
                if(serviceName && *serviceName && port && *port)
                {
                  if(ipAssigned.length() && ipAssigned.charAt(ipAssigned.length()-1) == ',')
                    ipAssigned.setCharAt((ipAssigned.length()-1),' ');
                  linkString.clear().appendf("%s-%s-", serviceName, (( buildset && *buildset ) ? buildset: ""));
                  if(prepareLink)
                    linkString.appendf("<a href=\"%s://%s:%s\"/>%s://%s:%s</a>", ( (protocol && *protocol) ? protocol :"http" ), (ipAssigned.trim()).str(), port, ( (protocol && *protocol) ? protocol :"http" ), (ipAssigned.trim()).str(), port );
                  else
                     linkString.appendf("%s", port);
                  espServiceArr.append(linkString);
                }
              }
            }
          }

          if(!instanceFound && (strcmp(pCompTree->queryName(), XML_TAG_ROXIECLUSTER) != 0 && strcmp(pCompTree->queryName(), XML_TAG_THORCLUSTER) != 0))
          {
            if(pCompTree->hasProp(XML_ATTR_COMPUTER))
            { 
              xpath.clear().appendf("./Hardware/%s/[%s=\"%s\"]", XML_TAG_COMPUTER, XML_ATTR_NAME, pCompTree->queryProp(XML_ATTR_COMPUTER));
              IPropertyTree* pHardware = pEnvRoot->queryPropTree(xpath.str());
              if(pHardware)
                ipAssigned.clear().append(pHardware->queryProp(XML_ATTR_NETADDRESS));
            }
          }
          else if(!strcmp(pCompTree->queryName(), XML_TAG_ROXIECLUSTER))
          {
            IPropertyTree* pCluster = pEnvRoot->queryPropTree("./Software/RoxieCluster");
            if(pCluster)
            {
               compName.clear().append(pCluster->queryProp("@name"));
               xpath.clear().append("./RoxieServerProcess");
               Owned<IPropertyTreeIterator> serverIter = pCluster->getElements(xpath.str());
               ForEach(*serverIter)
               {
                 IPropertyTree* pServer = &serverIter->query();
                 const char* netAddr = pServer->queryProp(XML_ATTR_NETADDRESS);
                 if(netAddr && *netAddr)
                 {
                   ipAssigned.append(netAddr).append(",");
                 }
               }
            }
          }
          else if(!strcmp(pCompTree->queryName(), XML_TAG_THORCLUSTER))
          {
            
            IPropertyTree* pCluster = pEnvRoot->queryPropTree("./Software/ThorCluster");
            if(pCluster)
            {
               compName.clear().append(pCluster->queryProp("@name"));
               IPropertyTree* pMaster = pCluster->queryPropTree("./ThorMasterProcess");
               if(pMaster)
               {
                 computerName.clear().append(pMaster->queryProp(XML_ATTR_COMPUTER));
                 if(computerName.length())
                 {
                   xpath.clear().appendf("./Hardware/%s/[%s=\"%s\"]", XML_TAG_COMPUTER, XML_ATTR_NAME, computerName.str());
                   IPropertyTree* pHardware = pEnvRoot->queryPropTree(xpath.str());
                   if(pHardware)
                     ipAssigned.clear().append(pHardware->queryProp(XML_ATTR_NETADDRESS)).append(",");
                 }
               }
               Owned<IPropertyTreeIterator> serverIter = pCluster->getElements("./ThorSlaveProcess");
               ForEach(*serverIter)
               {
                 IPropertyTree* pServer = &serverIter->query();
                 computerName.clear().append(pServer->queryProp(XML_ATTR_COMPUTER));
                 if(computerName.length())
                 {
                   xpath.clear().appendf("./Hardware/%s/[%s=\"%s\"]", XML_TAG_COMPUTER, XML_ATTR_NAME, computerName.str());
                   IPropertyTree* pHardware = pEnvRoot->queryPropTree(xpath.str());
                   if(pHardware)
                     ipAssigned.append(pHardware->queryProp(XML_ATTR_NETADDRESS)).append(",");
                 }
               }
            }
          }

          if(ipAssigned.length() && ipAssigned.charAt(ipAssigned.length()-1) == ',')
             ipAssigned.setCharAt((ipAssigned.length()-1),' ');
         
          if(ipAssigned.length() && compName.length())
          {
            IPropertyTree* pComponentType = pSummaryTree->addPropTree("Component", createPTree("Component"));
            pComponentType->addProp("@name", compName.str());
            pComponentType->addProp("@netaddresses", ipAssigned.str());
            pComponentType->addProp("@buildset", ( buildSetName.length() ? buildSetName.str(): ""));
            pComponentType->addProp("@espservice", "false");
          }
        }
      }
      if(espServiceArr.length() > 0)
      {
         ForEachItemIn(x, espServiceArr)
         {
           linkString.clear().append(espServiceArr.item(x));
           StringArray sArray;
           sArray.appendList(linkString.str(), "-");
           if(sArray.ordinality() == 3)
           {
             IPropertyTree* pEspServiceType = pSummaryTree->addPropTree("Component", createPTree("Component"));
             pEspServiceType->addProp("@name", sArray.item(0));
             pEspServiceType->addProp("@buildset", sArray.item(1));
             pEspServiceType->addProp("@netaddresses", sArray.item(2));
             pEspServiceType->addProp("@espservice", "true");
           }
         }
      }
    }
    if(pSummaryTree)
      toXML(pSummaryTree,respXmlStr);
  }
  else
    throw MakeStringException(-1, "Environment does not have any configuration information");
}

void mergeAttributes(IPropertyTree* pTo, IPropertyTree* pFrom)
{
  if (!pFrom)
    return;
  Owned<IAttributeIterator> iAttr = pFrom->getAttributes();
  ForEach(*iAttr)
  {
    const char* attrName = iAttr->queryName();

    if (!pTo->hasProp(attrName))
      pTo->addProp(attrName, iAttr->queryValue());
  }
}

void addEspBindingInformation(const char* xmlArg, IPropertyTree* pEnvRoot, StringBuffer& sbNewName, IConstEnvironment* pEnvironment,
                              const IPropertyTree* pCfg, const char* serviceName)
{
  Owned<IPropertyTree> pBindings = createPTreeFromXMLString(xmlArg && *xmlArg ? xmlArg : "<EspServiceBindings/>");
  const char* type = pBindings->queryProp(XML_ATTR_TYPE);
  const char* espName = pBindings->queryProp("@compName");

  StringBuffer xpath;

  xpath.append("./Programs/Build/BuildSet[@processName=\"EspProcess\"]");
  Owned<IPropertyTreeIterator> buildSetIter = pEnvRoot->getElements(xpath.str());
  buildSetIter->first();
  IPropertyTree* pBuildSet = &buildSetIter->query();
  const char* buildSetName = pBuildSet->queryProp(XML_ATTR_NAME);
  const char* processName = pBuildSet->queryProp(XML_ATTR_PROCESS_NAME);
  StringBuffer buildSetPath;
  Owned<IPropertyTree> pSchema = loadSchema(pEnvRoot->queryPropTree("./Programs/Build[1]"), pBuildSet, buildSetPath, pEnvironment);
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
    IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, pCfg, serviceName);

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
    }

    if (pEspService && pCompTree)
       pEspService->addPropTree(sb.str(), pCompTree->queryPropTree(sb.str()));
     //If we are adding, just consider the first selection.
     break;
  }

  if (!flag)
  {
    IPropertyTree* pEspService = pEnvRoot->queryPropTree(xpath.str());  
    IPropertyTree* pCompTree = generateTreeFromXsd(pEnvRoot, pSchema, processName, buildSetName, pCfg, serviceName);
    StringBuffer sbNewName(XML_TAG_ESPBINDING);
    xpath.clear().appendf("%s[@name='%s']/EspBinding", processName, espName);

    getUniqueName(pEnvRoot, sbNewName, xpath.str(), XML_TAG_SOFTWARE);
    xpath.clear().append(XML_TAG_ESPBINDING).append("/").append(XML_ATTR_NAME);
    pCompTree->setProp(xpath.str(), sbNewName);
    
    if (pEspService && pCompTree)
      pEspService->addPropTree(XML_TAG_ESPBINDING, pCompTree->queryPropTree(XML_TAG_ESPBINDING));
  }
}

bool updateDirsWithConfSettings(IPropertyTree* pEnvRoot, IProperties* pParams, bool ovrLog, bool ovrRun)
{
  bool ret = false;
  const char* rundir = pEnvRoot->queryProp("Software/Directories/Category[@name='run']/@dir");
  StringBuffer sbdir;
  
  if (rundir && ovrRun)
  {
    sbdir.clear().append(rundir);
    sbdir.replaceString("[NAME]", pParams->queryProp("blockname"));
    String str(sbdir.str());
    if (!str.startsWith(pParams->queryProp("runtime")))
    {
      StringBuffer sb;
      if (str.indexOf('[') > 0)
        sb.append(pParams->queryProp("runtime")).append(PATHSEPCHAR).append(sbdir.str() + str.indexOf('['));
      else
        sb.append(str.str());

      pEnvRoot->setProp("Software/Directories/Category[@name='run']/@dir", sb.str());
      ret = true;
    }
  }
  
  const char* logdir = pEnvRoot->queryProp("Software/Directories/Category[@name='log']/@dir");
  if (logdir && ovrLog)
  {
    sbdir.clear().append(logdir);
    sbdir.replaceString("[NAME]", pParams->queryProp("blockname"));
    String str(sbdir.str());
    if (!str.startsWith(pParams->queryProp("log")))
    {
      StringBuffer sb;
      if (str.indexOf('[') > 0)
        sb.append(pParams->queryProp("log")).append(PATHSEPCHAR).append(sbdir.str() + str.indexOf('['));
      else
        sb.append(str.str());

      pEnvRoot->setProp("Software/Directories/Category[@name='log']/@dir", sb.str());
      ret = true;
    }
  }

  return ret;
} 

//returns temp path that ends with path sep
//
#ifdef _WIN32
extern DWORD getLastError() { return ::GetLastError(); }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
  ::GetTempPath(bufsize, tempPath);
  ::GetLongPathName(tempPath, tempPath, bufsize);
  if (subdir && *subdir)
  {
    const int len = strlen(tempPath);
    char* p = tempPath + len;
    strcpy(p, subdir);
    p += strlen(subdir);
    *p++ = '\\';
    *p = '\0';
  }
}
#else//Linux specifics follow
extern DWORD getLastError() { return errno; }
void getTempPath(char* tempPath, unsigned int bufsize, const char* subdir/*=NULL*/)
{
  assert(bufsize > 5);
  strcpy(tempPath, "/tmp/");
  if (subdir && *subdir)
  {
    strcat(tempPath, subdir);
    strcat(tempPath, "/");
  }
}
#endif

bool validateEnv(IConstEnvironment* pConstEnv, bool abortOnException)
{
  char tempdir[_MAX_PATH];
  StringBuffer sb;

  while(true)
  {
    sb.clear().appendf("%d", msTick());
    getTempPath(tempdir, sizeof(tempdir), sb.str());

    if (!checkDirExists(tempdir))
    {
      if (recursiveCreateDirectory(tempdir))
        break;
    }
  }

  try
  {
    CConfigEngCallback callback(false, abortOnException);
    Owned<IEnvDeploymentEngine> configGenMgr;
    Owned<IPropertyTree> pEnvRoot = &pConstEnv->getPTree();
    const char* inDir = pEnvRoot->queryProp(XML_TAG_ENVSETTINGS"/path");
    StringBuffer sb(inDir);
    sb.append("/componentfiles/configxml");
    configGenMgr.setown(createConfigGenMgr(*pConstEnv, callback, NULL, inDir?sb.str():STANDARD_CONFIGXMLDIR, tempdir, NULL, NULL, NULL));
    configGenMgr->deploy(DEFLAGS_CONFIGFILES, DEBACKUP_NONE, false, false);
    deleteRecursive(tempdir);
    const char* msg = callback.getErrorMsg();
    if (msg && *msg)
    {
      StringBuffer sb("Errors or warnings were found when validating the environment.\n\n");
      sb.append(msg).append("\n");
      sb.appendf("Total errors/warnings: %d", callback.getErrorCount() - 1);
      throw MakeStringExceptionDirect(-1, sb.str());
    }
  }
  catch(IException* e)
  {
    deleteRecursive(tempdir);
    throw e;
  }

  return true;
}
