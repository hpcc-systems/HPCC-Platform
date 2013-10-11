#include "confighelper.hpp"
#include "XMLTags.h"
#include "jlib.hpp"
#include "jprop.hpp"
#include "jptree.hpp"
#include "build-config.h"

#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"
#define STANDARD_CONFIG_CONFIGXML_DIR "/componentfiles/configxml/"

CConfigHelper::CConfigHelper() : m_pDefBldSet(NULL)
{
}

CConfigHelper::~CConfigHelper()
{
}

CConfigHelper* CConfigHelper::getInstance(const IPropertyTree *cfg, const char* esp_name)
{
  static CConfigHelper *pConfigHelper = NULL;

  if (pConfigHelper != NULL)
  {
      return pConfigHelper;
  }

  if (cfg == NULL || esp_name == NULL)
  {
      return NULL;
  }

  pConfigHelper = new CConfigHelper();

  StringBuffer xpath;

  xpath.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALCONFFILE);
  pConfigHelper->m_strConfFile = cfg->queryProp(xpath.str());

  xpath.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALENVCONFFILE);
  pConfigHelper->m_strEnvConfFile = cfg->queryProp(xpath.str());

  if (pConfigHelper->m_strConfFile.length() > 0 && pConfigHelper->m_strEnvConfFile.length() > 0)
  {
    Owned<IProperties> pParams = createProperties(pConfigHelper->m_strConfFile);
    Owned<IProperties> pEnvParams = createProperties(pConfigHelper->m_strEnvConfFile);

    pConfigHelper->m_strConfigXMLDir = pEnvParams->queryProp(TAG_PATH);

    if ( pConfigHelper->m_strConfigXMLDir.length() == 0)
    {
      pConfigHelper->m_strConfigXMLDir = INSTALL_DIR;
    }

    pConfigHelper->m_strBuildSetFileName = pParams->queryProp(TAG_BUILDSET);

    pConfigHelper->m_strBuildSetFilePath.append(pConfigHelper->m_strConfigXMLDir).append(STANDARD_CONFIG_CONFIGXML_DIR).append( pConfigHelper->m_strBuildSetFileName.length() > 0 ? pConfigHelper->m_strBuildSetFileName : STANDARD_CONFIG_BUILDSETFILE);
    pConfigHelper->m_pDefBldSet.set(createPTreeFromXMLFile(pConfigHelper->m_strBuildSetFilePath.str()));
  }

  return pConfigHelper;
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

void CConfigHelper::getNewComponentListFromBuildSet(const IPropertyTree *pEnvTree, StringArray &sCompArray) const
{
    if (pEnvTree == NULL || m_pDefBldSet == NULL)
        return;

    StringBuffer xpathBuildSetFile;
    xpathBuildSetFile.appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);

    Owned<IPropertyTreeIterator> iter = m_pDefBldSet->getElements(xpathBuildSetFile.str());

    ForEach(*iter)
    {
        StringBuffer xpath;
        IPropertyTree* pSetting = &iter->query();
        StringBuffer strBuildSetName(pSetting->queryProp(XML_ATTR_NAME));

        xpath.appendf("%s/%s/%s[%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_NAME, strBuildSetName.str());

        if (pEnvTree->queryPropTree(xpath.str()) == NULL)
        {
            sCompArray.append(strBuildSetName.str());
        }
    }
}

void CConfigHelper::addNewComponentsFromBuildSetToEnv(IPropertyTree *pEnvTree)
{
    if (pEnvTree == NULL)
        return;

    StringArray sCompArray;

    getNewComponentListFromBuildSet(pEnvTree, sCompArray);

    if (sCompArray.length() == 0)
        return;

    for (int idx = 0; idx < sCompArray.length(); idx++)
    {
        StringBuffer xpath;
        xpath.appendf("%s/%s/%s[%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_NAME, (sCompArray.item(idx)));

        if (pEnvTree->queryPropTree(xpath.str()) != NULL)
            continue;

        pEnvTree->queryPropTree(XML_TAG_PROGRAMS"/"XML_TAG_BUILD)->addPropTree(XML_TAG_BUILDSET, createPTreeFromIPT(m_pDefBldSet->queryPropTree(xpath.str())));
    }
}
