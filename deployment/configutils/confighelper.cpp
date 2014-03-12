#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jprop.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "XMLTags.h"
#include "deploy.hpp"
#include "build-config.h"
#include "confighelper.hpp"

#define STANDARD_CONFIG_BUILDSETFILE "buildset.xml"
#define STANDARD_CONFIG_CONFIGXML_DIR "/componentfiles/configxml/"
#define STANDARD_CONFIG_PLUGIN_DIR_NAME "/plugins/"
#define STANDARD_CONFIG_PLUGINS_DIR STANDARD_CONFIG_CONFIGXML_DIR STANDARD_CONFIG_PLUGIN_DIR_NAME
#define PLUGIN_CGEN_COMP_LIST  "cgencomplist.xml"
#define ENV_GEN_RULES_DO_NOT_GENERATE_PROP "do_not_generate"

CriticalSection CConfigHelper::m_critSect;

CConfigHelper::CConfigHelper(IDeploymentCallback *pCallBack): m_pDefBldSet(NULL)
{
    if (pCallBack != NULL)
    {
        m_cbDeployment.set(pCallBack);
    }
    m_strConfigXMLDir.clear();
}

CConfigHelper::~CConfigHelper()
{
}

CConfigHelper* CConfigHelper::getInstance(const IPropertyTree *cfg, const char* esp_name, IDeploymentCallback *pCallBack)
{
    CriticalBlock block(m_critSect);

    static CConfigHelper *p_sConfigHelper = NULL;
    StringBuffer xpath1, xpath2;

    if (p_sConfigHelper != NULL)
    {
        return p_sConfigHelper;
    }

    p_sConfigHelper = new CConfigHelper(pCallBack);

    if (cfg != NULL && esp_name != NULL)
    {
        xpath1.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALCONFFILE);
        p_sConfigHelper->m_strConfFile = cfg->queryProp(xpath1.str());

        xpath2.clear().appendf("%s/%s/%s[%s='%s']/%s",XML_TAG_SOFTWARE, XML_TAG_ESPPROCESS, XML_TAG_ESPSERVICE, XML_ATTR_NAME, esp_name, XML_TAG_LOCALENVCONFFILE);
        p_sConfigHelper->m_strEnvConfFile = cfg->queryProp(xpath2.str());

        if (p_sConfigHelper->m_strConfFile.length() > 0 && p_sConfigHelper->m_strEnvConfFile.length() > 0 && checkFileExists(p_sConfigHelper->m_strConfFile.str()) && checkFileExists(p_sConfigHelper->m_strEnvConfFile.str()))
        {
            Owned<IProperties> pParams = createProperties(p_sConfigHelper->m_strConfFile.str());
            Owned<IProperties> pEnvParams = createProperties(p_sConfigHelper->m_strEnvConfFile.str());

            p_sConfigHelper->m_strConfigXMLDir = pEnvParams->queryProp(TAG_PATH);

            if (p_sConfigHelper->m_strConfigXMLDir.length() == 0)
            {
              p_sConfigHelper->m_strConfigXMLDir = INSTALL_DIR;
            }

            p_sConfigHelper->m_strBuildSetFileName = pParams->queryProp(TAG_BUILDSET);

            p_sConfigHelper->m_strBuildSetFilePath.append(p_sConfigHelper->m_strConfigXMLDir).append(STANDARD_CONFIG_CONFIGXML_DIR).append(
                        p_sConfigHelper->m_strBuildSetFileName.length() > 0 ? p_sConfigHelper->m_strBuildSetFileName : STANDARD_CONFIG_BUILDSETFILE);

            try
            {
                p_sConfigHelper->m_pDefBldSet.set(createPTreeFromXMLFile(p_sConfigHelper->m_strBuildSetFilePath.str()));
            }
            catch (IException *e)
            {
                if (p_sConfigHelper->m_pDefBldSet.get() != NULL)
                {
                    p_sConfigHelper->m_pDefBldSet.clear();
                }

                StringBuffer msg;
                e->errorMessage(msg);

                delete e;

                if (p_sConfigHelper->m_cbDeployment.get() != NULL)
                {
                    p_sConfigHelper->m_cbDeployment->printStatus(STATUS_ERROR, NULL, NULL, NULL,
                                                "Failed create PTree from buildset.xml file %s with error %s", p_sConfigHelper->m_strBuildSetFilePath.str(), msg.str());

                    return p_sConfigHelper;
                }
                else
                {
                    throw MakeStringException(-1, "Failed create PTree from buildset.xml file %s with error %s", p_sConfigHelper->m_strBuildSetFilePath.str(), msg.str());
                }
            }

            try
            {
                p_sConfigHelper->appendBuildSetFromPlugins();
            }
            catch (IException *e)
            {
                if (p_sConfigHelper->m_cbDeployment.get() != NULL)
                {
                    StringBuffer msg;
                    e->errorMessage(msg);

                    p_sConfigHelper->m_cbDeployment->printStatus(STATUS_ERROR, NULL, NULL, NULL,
                                                "Failed to add plugin buildset.xml file %s with error %s", p_sConfigHelper->m_strBuildSetFilePath.str(), msg.str());
                }
                // TODO: log message to configmgr log but continue execution

                delete e;
            }

            return p_sConfigHelper;
        }
        else
        {
            delete p_sConfigHelper;
            p_sConfigHelper = NULL;

            throw MakeStringException(-1, "Config file does not define values for %s and %s", xpath1.str(), xpath2.str());
        }
    }

    return p_sConfigHelper;
}

bool CConfigHelper::isInBuildSet(const char* comp_process_name, const char* comp_name) const
{
  StringBuffer xpath;

  xpath.appendf("./%s/%s/%s[%s=\"%s\"][%s=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, XML_ATTR_PROCESS_NAME, comp_process_name, XML_ATTR_NAME, comp_name);

  if (strcmp(XML_TAG_DIRECTORIES,comp_name) != 0 && m_pDefBldSet->hasProp(xpath.str()) == false)
  {
     return false;
  }
  else
  {
     return true;
  }
}

IPropertyTree* CConfigHelper::getBuildSetTree()
{
    this->m_pDefBldSet->Link();

    return this->m_pDefBldSet;
}

void CConfigHelper::appendBuildSetFromPlugins()
{
    const char *pMask = "*";

    StringBuffer strPath(this->m_strConfigXMLDir);
    strPath.append(STANDARD_CONFIG_CONFIGXML_DIR).append(STANDARD_CONFIG_PLUGIN_DIR_NAME);

    if (this->m_cbDeployment.get() != NULL)
    {
        this->m_cbDeployment->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                                  "Appending plugins to buildset %s", this->m_strBuildSetFilePath.str());
    }

    Owned<IFile> pluginRootDir = createIFile(strPath.str());

    if (checkFileExists(strPath.str()) == false)
    {
        if (m_cbDeployment.get() != NULL)
        {
            m_cbDeployment->printStatus(STATUS_WARN, NULL, NULL, NULL,
                                      "Could not find plugin directory at %s", strPath.str());
        }
        return;
    }

    Owned<IDirectoryIterator> pluginFiles = pluginRootDir->directoryFiles(pMask, false, true);

    ForEach(*pluginFiles)
    {
        if (pluginFiles->query().isDirectory() == foundYes)
        {
            StringBuffer strPluginBuildSetPath;

            strPluginBuildSetPath.append(pluginFiles->query().queryFilename()).append("/").append(STANDARD_CONFIG_BUILDSETFILE);

            if (checkFileExists(strPluginBuildSetPath.str()) == true)
            {
                StringBuffer strXPath;

                strXPath.appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);

                Owned<IPropertyTree> pPluginBuildSet = createPTreeFromXMLFile(strPluginBuildSetPath.str());

                if (m_cbDeployment.get() != NULL)
                {
                    m_cbDeployment->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                                                "Loading plugin BuildSet from  %s", strPluginBuildSetPath.str());
                }

                Owned<IPropertyTreeIterator> pBuildSetIterator = pPluginBuildSet->getElements(strXPath);

                ForEach(*pBuildSetIterator)
                {
                    int nIdx = 1;
                    try
                    {
                        m_pDefBldSet->addPropTree(strXPath.str(), LINK(&(pBuildSetIterator->query())));
                        nIdx++;
                    }
                    catch (IPTreeException *e)
                    {
                        if (m_cbDeployment.get() != NULL)
                        {
                            m_cbDeployment->printStatus(STATUS_ERROR, NULL, NULL, NULL,
                                                        "Error adding buildset with xpath %s[%d] from location %s", strXPath.str(), nIdx, strPluginBuildSetPath.str());
                        }

                        delete e;
                    }
                }
            }
            else
            {
                if (m_cbDeployment != NULL)
                {
                    m_cbDeployment->printStatus(STATUS_WARN, NULL, NULL, NULL,
                                                "File %s is missing or not accessible with path %s", STANDARD_CONFIG_BUILDSETFILE, strPluginBuildSetPath.str());
                }
            }
        }
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

        if (pEnvTree->hasProp(xpath.str()) == false)
        {
            sCompArray.append(strBuildSetName.str());
        }
    }
}

void CConfigHelper::addNewComponentsFromBuildSetToEnv(IPropertyTree *pEnvTree) const
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

        if (pEnvTree->hasProp(xpath.str()) == true)
            continue;

        pEnvTree->queryPropTree(XML_TAG_PROGRAMS"/"XML_TAG_BUILD)->addPropTree(XML_TAG_BUILDSET, createPTreeFromIPT(m_pDefBldSet->queryPropTree(xpath.str())));
    }
}

void CConfigHelper::addPluginsToConfigGenCompList(IPropertyTree *pCGenComplist,  const char *pPath) const
{
    if (pCGenComplist == NULL)
    {
        return;
    }

    const char *pMask = "*";

    StringBuffer strPath;

    if (m_strConfigXMLDir.length() > 0)
    {
        strPath.set(m_strConfigXMLDir.str());
        strPath.append(STANDARD_CONFIG_CONFIGXML_DIR).append(STANDARD_CONFIG_PLUGIN_DIR_NAME);
    }
    else if(pPath != NULL)
    {
        strPath.set(pPath).append(STANDARD_CONFIG_PLUGIN_DIR_NAME);
    }
    else
    {
        return;
    }

    Owned<IFile> pluginRootDir = createIFile(strPath.str());

    if (checkFileExists(strPath.str()) == false)
    {
        return;
    }

    Owned<IDirectoryIterator> pluginFiles = pluginRootDir->directoryFiles(pMask, false, true);

    ForEach(*pluginFiles)
    {
        if (pluginFiles->query().isDirectory() == foundYes)
        {
            StringBuffer strPluginCGenCompListPath;

            strPluginCGenCompListPath.append(pluginFiles->query().queryFilename()).append(PATHSEPCHAR).append(PLUGIN_CGEN_COMP_LIST);

            if (checkFileExists(strPluginCGenCompListPath.str()) == true)
            {
                StringBuffer strXPath;

                strXPath.appendf("./%s", XML_TAG_COMPONENT);

                Owned<IPropertyTree> pPluginCGenCompList;

                try
                {
                    pPluginCGenCompList.set(createPTreeFromXMLFile(strPluginCGenCompListPath.str()));
                }
                catch (IException *e)
                {
                    if (m_cbDeployment.get() != NULL)
                    {
                        m_cbDeployment->printStatus(STATUS_WARN, NULL, NULL, NULL,
                                                "Unable to load cgencomplist.xml from  %s", strPluginCGenCompListPath.str());
                    }
                    delete e;
                }

                Owned<IPropertyTreeIterator> pCGenCompListIterator = pPluginCGenCompList->getElements(strXPath);

                ForEach(*pCGenCompListIterator)
                {
                    StringBuffer strXPath2(XML_TAG_COMPONENT);
                    StringBuffer strXPath3(XML_TAG_COMPONENT"/"XML_TAG_FILE);

                    const char *pServiceName = pCGenCompListIterator->query().queryProp(XML_ATTR_NAME);
                    strXPath2.appendf("[%s='%s']", XML_ATTR_NAME, pServiceName);

                    if (pCGenComplist->hasProp(strXPath2.str()) == false)
                    {
                        pCGenComplist->addPropTree(XML_TAG_COMPONENT, LINK(&(pCGenCompListIterator->query())));

                        if (m_cbDeployment.get() != NULL)
                        {
                            m_cbDeployment->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                                                "Loaded %s from  %s", pServiceName, strPluginCGenCompListPath.str());
                        }
                    }
                    else
                    {
                        Owned<IPropertyTreeIterator> pFileListIter = pPluginCGenCompList->getElements(strXPath3);

                        ForEach(*pFileListIter)
                        {
                            pCGenComplist->queryPropTree(strXPath2.str())->addPropTree(XML_TAG_FILE, LINK((&(pFileListIter->query()))));

                            if (m_cbDeployment.get() != NULL)
                            {
                                m_cbDeployment->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                                                "Loading %s from  %s", pServiceName, strPluginCGenCompListPath.str());
                            }
                        }
                    }
                }
            }
            else
            {
                if (m_cbDeployment != NULL)
                {
                    m_cbDeployment->printStatus(STATUS_WARN, NULL, NULL, NULL,
                                              "cgencomplist.xml file is missing at expected location %s", strPluginCGenCompListPath.str());
                }
            }
        }
    }
}

void CConfigHelper::addPluginsToGenEnvRules(IProperties *pGenEnvRulesProps) const
{
    if (pGenEnvRulesProps == NULL)
    {
        return;
    }

    const char *pMask = "*";

    StringBuffer strPath(this->m_strConfigXMLDir);
    strPath.append(STANDARD_CONFIG_CONFIGXML_DIR).append(STANDARD_CONFIG_PLUGIN_DIR_NAME);

    Owned<IFile> pluginRootDir = createIFile(strPath.str());

    if (checkFileExists(strPath.str()) == false)
    {
        return;
    }

    Owned<IDirectoryIterator> pluginFiles = pluginRootDir->directoryFiles(pMask, false, true);

    ForEach(*pluginFiles)
    {
        if (pluginFiles->query().isDirectory() == foundYes)
        {
            StringBuffer strPluginGenEnvRulesPath;

            strPluginGenEnvRulesPath.append(pluginFiles->query().queryFilename()).append(PATHSEPCHAR).append(STANDARD_CONFIG_ALGORITHMFILE);

            if (checkFileExists(strPluginGenEnvRulesPath.str()) == true)
            {

                Owned<IProperties> pPluginGenEnvPropList = createProperties(strPluginGenEnvRulesPath.str());
                Owned<IPropertyIterator> pPluginGenEnvPropListIterator = pPluginGenEnvPropList->getIterator();

                ForEach(*pPluginGenEnvPropListIterator)
                {
                    const char *pKeyName = pPluginGenEnvPropListIterator->getPropKey();

                    if (pKeyName != NULL && *pKeyName != 0 && strcmp(pKeyName, ENV_GEN_RULES_DO_NOT_GENERATE_PROP) == 0)
                    {
                        StringBuffer strProp;

                        if (pGenEnvRulesProps->hasProp(pKeyName) == false)
                        {
                            pPluginGenEnvPropList->getProp(pKeyName, strProp);
                            pGenEnvRulesProps->appendProp(ENV_GEN_RULES_DO_NOT_GENERATE_PROP, strProp.str());

                            if (m_cbDeployment != NULL)
                            {
                                m_cbDeployment->printStatus(STATUS_NORMAL, NULL, NULL, NULL,
                                                            "Adding genenvrules %s", strProp.str());
                            }
                        }
                        else
                        {
                            pPluginGenEnvPropList->getProp(pKeyName, strProp.clear());
                            strProp.append(",");
                            pGenEnvRulesProps->getProp(pKeyName, strProp);
                            pGenEnvRulesProps->setProp(pKeyName, strProp.str());
                        }
                    }
                }
            }
            else
            {
                if (m_cbDeployment != NULL)
                {
                    m_cbDeployment->printStatus(STATUS_WARN, NULL, NULL, NULL,
                                               "Failed to load plug-in genenvrules.conf file %s", strPluginGenEnvRulesPath.str());
                }
            }
        }
    }
}
