#ifndef CONFIGHELPER_HPP_INCL
#define CONFIGHELPER_HPP_INCL

#ifdef CONFIGUTILS_EXPORTS
  #define CONFIGUTILS_API __declspec(dllexport)
#else
  #define CONFIGUTILS_API __declspec(dllimport)
#endif

#define STANDARD_CONFIG_ALGORITHMFILE "genenvrules.conf"

#include "jmutex.hpp"
#include "jstring.hpp"

interface IDeploymentCallback;
interface IPropertyTree;
class StringArray;
interface IPropertyTree;
interface IProperties;

class CONFIGUTILS_API CConfigHelper
{
public:

  virtual ~CConfigHelper();

  static CConfigHelper* getInstance(const IPropertyTree *cfg = NULL, const char* esp_name = NULL, IDeploymentCallback *pCallBack = NULL);

  bool isInBuildSet(const char* comp_process_name, const char* comp_name) const;

  const char* getConfigXMLDir() const
  {
    return m_strConfigXMLDir.toCharArray();
  }
  const char* getBuildSetFileName() const
  {
    return m_strBuildSetFileName.toCharArray();
  }
  const char* getEnvConfFile() const
  {
    return m_strEnvConfFile.toCharArray();
  }
  const char* getConfFile() const
  {
    return m_strConfFile.toCharArray();
  }
  const char* getBuildSetFilePath() const
  {
    return m_strBuildSetFilePath.toCharArray();
  }

  void getNewComponentListFromBuildSet(const IPropertyTree *pEnvTree, StringArray &sCompArray) const;
  void addNewComponentsFromBuildSetToEnv(IPropertyTree *pEnvTree) const;
  void addPluginsToConfigGenCompList(IPropertyTree *pCGenComplist, const char *pPath = NULL) const;
  void addPluginsToGenEnvRules(IProperties *pGenEnvRulesProps) const;

  IPropertyTree* getBuildSetTree();

protected:

  CConfigHelper(IDeploymentCallback *m_cbDeployment = NULL);

  static CriticalSection m_critSect;

  Owned<IPropertyTree> m_pDefBldSet;
  Linked<IDeploymentCallback> m_cbDeployment;
  StringBuffer  m_strConfigXMLDir;
  StringBuffer  m_strBuildSetFileName;
  StringBuffer  m_strEnvConfFile;
  StringBuffer  m_strConfFile;
  StringBuffer  m_strBuildSetFilePath;

  void appendBuildSetFromPlugins();

};

#endif // CONFIGHELPER_HPP_INCL
