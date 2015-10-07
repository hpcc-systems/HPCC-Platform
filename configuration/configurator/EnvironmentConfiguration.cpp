/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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
