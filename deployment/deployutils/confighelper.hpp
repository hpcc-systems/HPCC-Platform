#include "deployutils.hpp"

class DEPLOYUTILS_API CConfigHelper
{
public:

  virtual ~CConfigHelper();

  static CConfigHelper* getInstance(const IPropertyTree *cfg = NULL, const char* esp_name = NULL);

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

  const IPropertyTree* getBuildSetTree() const
  {
      return m_pDefBldSet;
  }

  void getNewComponentListFromBuildSet(const IPropertyTree *pEnvTree, StringArray &sCompArray) const;
  void addNewComponentsFromBuildSetToEnv(IPropertyTree *pEnvTree);

protected:

  CConfigHelper();

  Owned<IPropertyTree> m_pDefBldSet;
  StringBuffer  m_strConfigXMLDir;
  StringBuffer  m_strBuildSetFileName;
  StringBuffer  m_strEnvConfFile;
  StringBuffer  m_strConfFile;
  StringBuffer  m_strBuildSetFilePath;
};
