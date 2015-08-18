#include "EnvironmentConfiguration.hpp"
#include "XMLTags.h"

CEnvironmentConfiguration* CEnvironmentConfiguration::getInstance()
{
    static Owned<CEnvironmentConfiguration> s_ConfigurationSingleton;
    static CSingletonLock slock;

    if (slock.lock() == true)
    {
      if (s_ConfigurationSingleton.get() == NULL)
      {
        s_ConfigurationSingleton.setown(new CEnvironmentConfiguration());
      }

      slock.unlock();
    }

    return s_ConfigurationSingleton.get();
}

CEnvironmentConfiguration::CEnvironmentConfiguration()
{

}

CEnvironmentConfiguration::~CEnvironmentConfiguration()
{

}

enum CEnvironmentConfiguration::CEF_ERROR_CODES CEnvironmentConfiguration::generateBaseEnvironmentConfiguration()
{
    StringBuffer xpath;

    xpath.clear().appendf("<%s><%s></%s>", XML_HEADER, XML_TAG_ENVIRONMENT, XML_TAG_ENVIRONMENT);

    if (m_pEnv.get() != NULL)
        m_pEnv.clear();

    m_pEnv.setown(createPTreeFromXMLString(xpath.str()));

    IPropertyTree* pSettings = m_pEnv->addPropTree(XML_TAG_ENVSETTINGS, createPTree());

    return CEnvironmentConfiguration::CF_NO_ERROR;
}

enum CEnvironmentConfiguration::CEF_ERROR_CODES  CEnvironmentConfiguration::addComponent(const char* pCompType)
{
    return CEnvironmentConfiguration::CF_NO_ERROR;
}

enum CEnvironmentConfiguration::CEF_ERROR_CODES  CEnvironmentConfiguration::removeComponent(const char* pCompType, const char* pCompName)
{
    return CEnvironmentConfiguration::CF_NO_ERROR;
}

enum CEnvironmentConfiguration::CEF_ERROR_CODES  CEnvironmentConfiguration::addESPService(const char* espServiceType)
{
    return CEnvironmentConfiguration::CF_NO_ERROR;
}
