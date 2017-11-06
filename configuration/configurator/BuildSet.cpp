/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include "jptree.hpp"
#include "jfile.hpp"
#include "XMLTags.h"
#include "BuildSet.hpp"
#include "SchemaCommon.hpp"

using namespace CONFIGURATOR;

#define LOOP_THRU_BUILD_SET int len = m_buildSetArray.length(); for (int idx = 0; idx < len; idx++)

static CBuildSetManager *s_pBuildSetManager = nullptr;

CBuildSetManager* CBuildSetManager::getInstance(const char* pBuildSetFile, const char* pBuildSetDirectory)
{
    if (s_pBuildSetManager == nullptr)
    {
        s_pBuildSetManager = new CBuildSetManager();

        s_pBuildSetManager->m_buildSetFile.set(pBuildSetFile != nullptr ? pBuildSetFile : DEFAULT_BUILD_SET_XML_FILE);
        s_pBuildSetManager->m_buildSetDir.set(pBuildSetDirectory != nullptr ? pBuildSetDirectory : DEFAULT_BUILD_SET_DIRECTORY);

        pBuildSetFile = s_pBuildSetManager->m_buildSetFile;
        pBuildSetDirectory = s_pBuildSetManager->m_buildSetDir;

        s_pBuildSetManager->m_buildSetPath.setf("%s%s%s", pBuildSetDirectory, pBuildSetDirectory[strlen(pBuildSetDirectory)-1] == '/' ? "" : "/", pBuildSetFile);

        if (!checkFileExists(s_pBuildSetManager->m_buildSetPath.str()))
            return s_pBuildSetManager;
        if (s_pBuildSetManager->populateBuildSet() == false)
        {
            delete s_pBuildSetManager;
            s_pBuildSetManager = nullptr;
        }
    }
    return s_pBuildSetManager;
}

CBuildSetManager::CBuildSetManager(const char* pBuildSetFile, const char* pBuildSetDir) : m_buildSetFile(pBuildSetFile), m_buildSetDir(pBuildSetDir)
{
    if (pBuildSetFile != nullptr && pBuildSetDir != nullptr)
       m_buildSetPath.clear().appendf("%s%s%s", pBuildSetDir, pBuildSetDir[strlen(pBuildSetDir)-1] == '/' ? "" : "/", pBuildSetFile);
}

CBuildSetManager::CBuildSetManager()
{
}

CBuildSetManager::~CBuildSetManager()
{
    m_buildSetArray.kill();
}

void CBuildSetManager::getBuildSetComponents(StringArray& buildSetArray) const
{
    int nLength = this->getBuildSetComponentCount();

    for (int idx = 0; idx < nLength; idx++)
    {
        buildSetArray.append(this->getBuildSetComponentTypeName(idx));
    }
}

void CBuildSetManager::getBuildSetServices(StringArray& buildSetArray) const
{
    int nLength = this->getBuildSetServiceCount();

    for (int idx = 0; idx < nLength; idx++)
    {
        buildSetArray.append(this->getBuildSetServiceName(idx));
    }
}

const char* CBuildSetManager::getBuildSetServiceName(int index) const
{
    if (index >= getBuildSetServiceCount())
        return nullptr;
    return this->getBuildSetService(index)->getName();
}

const char* CBuildSetManager::getBuildSetServiceFileName(int index) const
{
    if (index >= getBuildSetServiceCount())
        return nullptr;
    return this->getBuildSetService(index)->getSchema();
}

const char* CBuildSetManager::getBuildSetComponentTypeName(int index) const
{
    if (index >= this->getBuildSetComponentCount())
        return nullptr;
    return this->getBuildSetComponent(index)->getName();
}

const char* CBuildSetManager::getBuildSetComponentFileName(int index) const
{
    if (index >= this->getBuildSetComponentCount())
        return nullptr;
    return this->getBuildSetComponent(index)->getSchema();
}

const char* CBuildSetManager::getBuildSetProcessName(int index) const
{
    if (index >= this->getBuildSetComponentCount())
        return nullptr;
    return this->getBuildSetComponent(index)->getProcessName();
}

bool CBuildSetManager::populateBuildSet()
{
    StringBuffer xpath;

    if (m_buildSetTree.get() != nullptr)
        return false;

    try
    {
        m_buildSetTree.set(createPTreeFromXMLFile(m_buildSetPath.str()));
    }
    catch(...)
    {
        return false;
    }

    xpath.setf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);

    Owned<IPropertyTreeIterator> iter = m_buildSetTree->getElements(xpath.str());

    ForEach(*iter)
    {
        IPropertyTree* pTree = &iter->query();

        if ( pTree->queryProp(XML_ATTR_PROCESS_NAME) == nullptr || pTree->queryProp(XML_ATTR_OVERRIDE) != nullptr || ( (pTree->queryProp(XML_ATTR_DEPLOYABLE) != nullptr && \
                stricmp(pTree->queryProp(XML_ATTR_DEPLOYABLE), "no") == 0 && stricmp(pTree->queryProp(XML_ATTR_PROCESS_NAME), XML_TAG_ESPSERVICE) != 0) ) )
            continue;

        Owned<CBuildSet> pBuildSet = new CBuildSet(pTree->queryProp(XML_ATTR_INSTALLSET), pTree->queryProp(XML_ATTR_NAME), pTree->queryProp(XML_ATTR_PROCESS_NAME),\
                                                   pTree->queryProp(XML_ATTR_SCHEMA), pTree->queryProp(XML_ATTR_DEPLOYABLE), pTree->queryProp(XML_ATTR_OVERRIDE));

        m_buildSetArray.append(*pBuildSet.getLink());
    }
    return true;
}

void CBuildSetManager::setBuildSetArray(const StringArray &strArray)
{
    m_buildSetArray.kill();

    for (unsigned idx = 0; idx < strArray.length(); idx++)
    {
        Owned<CBuildSet> pBSet = new CBuildSet(nullptr, strArray.item(idx), nullptr, strArray.item(idx));
        assert (pBSet != nullptr);
        m_buildSetArray.append(*pBSet.getClear());
    }
}

const char* CBuildSetManager::getBuildSetSchema(unsigned index) const
{
    assert(index < m_buildSetArray.length());
    if (index < m_buildSetArray.length())
        return m_buildSetArray.item(index).getSchema();
    else
        return nullptr;
}

const int CBuildSetManager::getBuildSetSchemaCount() const
{
    return m_buildSetArray.length();
}

const int CBuildSetManager::getBuildSetServiceCount() const
{
    int nCount = 0;

    LOOP_THRU_BUILD_SET
    {
        if (m_buildSetArray.item(idx).getProcessName() != nullptr && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                (m_buildSetArray.item(idx).getDeployable() != nullptr && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
            nCount++;
    }
    return nCount;
}

const int CBuildSetManager::getBuildSetComponentCount() const
{
    int nCount = 0;

    LOOP_THRU_BUILD_SET
    {
        if ( ((m_buildSetArray.item(idx).getProcessName() == nullptr) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                ( (m_buildSetArray.item(idx).getDeployable() == nullptr) || (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                ( (m_buildSetArray.item(idx).getOverride() == nullptr) || (stricmp(m_buildSetArray.item(idx).getOverride(), "no")    != 0) ) )
            nCount++;
    }
    return nCount;
}

const CBuildSet* CBuildSetManager::getBuildSetComponent(int index) const
{
    LOOP_THRU_BUILD_SET
    {
        if (index == 0)
        {
            if ( ((m_buildSetArray.item(idx).getProcessName() == nullptr) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                    ( (m_buildSetArray.item(idx).getDeployable() == nullptr) || (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                    ( (m_buildSetArray.item(idx).getOverride() == nullptr) || (stricmp(m_buildSetArray.item(idx).getOverride(), "no") != 0) ) )
                return &(m_buildSetArray.item(idx));
            else
                continue;
        }
        if ( ((m_buildSetArray.item(idx).getProcessName() == nullptr) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                ( (m_buildSetArray.item(idx).getDeployable() == nullptr)|| (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                ( (m_buildSetArray.item(idx).getOverride() == nullptr) || (stricmp(m_buildSetArray.item(idx).getOverride(), "no") != 0) ) )
            index--;
    }
    assert(!"index invalid");
    return nullptr;
}

const CBuildSet* CBuildSetManager::getBuildSetService(int index) const
{
    LOOP_THRU_BUILD_SET
    {
        if (index == 0)
        {
            if (m_buildSetArray.item(idx).getProcessName() != nullptr && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                    (m_buildSetArray.item(idx).getDeployable() != nullptr && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
                return &(m_buildSetArray.item(idx));
            else
                continue;
        }
        if (m_buildSetArray.item(idx).getProcessName() != nullptr && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                (m_buildSetArray.item(idx).getDeployable() != nullptr && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
            index--;
    }

    assert(!"index invalid");
    return nullptr;
}
