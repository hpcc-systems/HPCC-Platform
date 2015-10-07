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

#include "BuildSet.hpp"
#include "jptree.hpp"
#include "XMLTags.h"
#include "SchemaCommon.hpp"

#define LOOP_THRU_BUILD_SET for (int idx = 0; idx < m_buildSetArray.length(); idx++)

static CBuildSetManager *s_pBuildSetManager = NULL;

CBuildSetManager* CBuildSetManager::getInstance(const char* pBuildSetFile, const char* pBuildSetDirectory)
{
    if (s_pBuildSetManager == NULL)
    {
        s_pBuildSetManager = new CBuildSetManager();

        if (pBuildSetFile != NULL)
            s_pBuildSetManager->m_buildSetFile.set(pBuildSetFile);
        else
            s_pBuildSetManager->m_buildSetFile.set(DEFAULT_BUILD_SET_XML_FILE);

        if (pBuildSetDirectory != NULL)
            s_pBuildSetManager->m_buildSetDir.set(pBuildSetDirectory);
        else
            s_pBuildSetManager->m_buildSetDir.set(DEFAULT_BUILD_SET_DIRECTORY);

        pBuildSetFile = s_pBuildSetManager->m_buildSetFile;
        pBuildSetDirectory = s_pBuildSetManager->m_buildSetDir;

        s_pBuildSetManager->m_buildSetPath.clear().appendf("%s%s%s", pBuildSetDirectory, pBuildSetDirectory[strlen(pBuildSetDirectory)-1] == '/' ? "" : "/", pBuildSetFile);
        s_pBuildSetManager->populateBuildSet();
    }
    return s_pBuildSetManager;
}

CBuildSetManager::CBuildSetManager(const char* pBuildSetFile, const char* pBuildSetDir) : m_buildSetFile(pBuildSetFile), m_buildSetDir(pBuildSetDir)
{
    if (pBuildSetFile != NULL && pBuildSetDir != NULL)
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
        buildSetArray.append(this->getBuildSetComponentName(idx));
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
    return this->getBuildSetService(index)->getName();
}

const char* CBuildSetManager::getBuildSetServiceFileName(int index) const
{
    return this->getBuildSetService(index)->getSchema();
}

const char* CBuildSetManager::getBuildSetComponentName(int index) const
{
    return this->getBuildSetComponent(index)->getName();
}

const char* CBuildSetManager::getBuildSetComponentFileName(int index) const
{
    return this->getBuildSetComponent(index)->getSchema();
}

const char* CBuildSetManager::getBuildSetProcessName(int index) const
{
    return this->getBuildSetComponent(index)->getProcessName();
}

bool CBuildSetManager::populateBuildSet()
{
    StringBuffer xpath;

    if (m_buildSetTree.get() != NULL)
        return false;

    try
    {
        m_buildSetTree.set(createPTreeFromXMLFile(m_buildSetPath.str()));
    }
    catch(...)
    {
        return false;
    }

    xpath.appendf("./%s/%s/%s", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET);

    Owned<IPropertyTreeIterator> iter = m_buildSetTree->getElements(xpath.str());

    ForEach(*iter)
    {
        IPropertyTree* pTree = &iter->query();

        if ( pTree->queryProp(XML_ATTR_PROCESS_NAME) == NULL || pTree->queryProp(XML_ATTR_OVERIDE) != NULL || ( (pTree->queryProp(XML_ATTR_DEPLOYABLE) != NULL && \
                stricmp(pTree->queryProp(XML_ATTR_DEPLOYABLE), "no") == 0 && stricmp(pTree->queryProp(XML_ATTR_PROCESS_NAME), XML_TAG_ESPSERVICE) != 0) ) )
            continue;

        Owned<CBuildSet> pBuildSet = new CBuildSet(pTree->queryProp(XML_ATTR_INSTALLSET), pTree->queryProp(XML_ATTR_NAME), pTree->queryProp(XML_ATTR_PROCESS_NAME),\
                                                   pTree->queryProp(XML_ATTR_SCHEMA), pTree->queryProp(XML_ATTR_DEPLOYABLE), pTree->queryProp(XML_ATTR_OVERIDE));

        m_buildSetArray.append(*pBuildSet.getLink());
    }
    return true;
}

void CBuildSetManager::setBuildSetArray(const StringArray &strArray)
{
    m_buildSetArray.kill();

    for (int idx = 0; idx < strArray.length(); idx++)
    {
        Owned<CBuildSet> pBSet = new CBuildSet(NULL, strArray.item(idx), NULL, strArray.item(idx));
        assert (pBSet != NULL);
        m_buildSetArray.append(*pBSet.getClear());
    }
}

const char* CBuildSetManager::getBuildSetSchema(int index) const
{
    assert(index < m_buildSetArray.length());
    if (index < m_buildSetArray.length())
        return m_buildSetArray.item(index).getSchema();
    else
        return NULL;
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
        if (m_buildSetArray.item(idx).getProcessName() != NULL && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                (m_buildSetArray.item(idx).getDeployable() != NULL && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
            nCount++;
    }
    return nCount;
}

const int CBuildSetManager::getBuildSetComponentCount() const
{
    int nCount = 0;

    LOOP_THRU_BUILD_SET
    {
        if ( ((m_buildSetArray.item(idx).getProcessName() == NULL) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                ( (m_buildSetArray.item(idx).getDeployable() == NULL) || (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                ( (m_buildSetArray.item(idx).getOveride() == NULL) || (stricmp(m_buildSetArray.item(idx).getOveride(), "no")    != 0) ) )
            nCount++;
    }
    return nCount;
}

const CBuildSet* CBuildSetManager::getBuildSetComponent(int index) const
{
    int nCount = 0;

    LOOP_THRU_BUILD_SET
    {
        if (index == 0)
        {
            if ( ((m_buildSetArray.item(idx).getProcessName() == NULL) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                    ( (m_buildSetArray.item(idx).getDeployable() == NULL) || (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                    ( (m_buildSetArray.item(idx).getOveride() == NULL) || (stricmp(m_buildSetArray.item(idx).getOveride(), "no") != 0) ) )
                return &(m_buildSetArray.item(idx));
            else
                continue;
        }
        if ( ((m_buildSetArray.item(idx).getProcessName() == NULL) || (strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) != 0)) && \
                ( (m_buildSetArray.item(idx).getDeployable() == NULL)|| (stricmp(m_buildSetArray.item(idx).getDeployable(), "no") != 0) ) && \
                ( (m_buildSetArray.item(idx).getOveride() == NULL) || (stricmp(m_buildSetArray.item(idx).getOveride(), "no") != 0) ) )
            index--;
    }
    assert(!"index invalid");
    return NULL;
}

const CBuildSet* CBuildSetManager::getBuildSetService(int index) const
{
    LOOP_THRU_BUILD_SET
    {
        if (index == 0)
        {
            if (m_buildSetArray.item(idx).getProcessName() != NULL && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                    (m_buildSetArray.item(idx).getDeployable() != NULL && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
                return &(m_buildSetArray.item(idx));
            else
                continue;
        }
        if (m_buildSetArray.item(idx).getProcessName() != NULL && strcmp(m_buildSetArray.item(idx).getProcessName(), XML_TAG_ESPSERVICE) == 0 && \
                (m_buildSetArray.item(idx).getDeployable() != NULL && stricmp(m_buildSetArray.item(idx).getDeployable(), "no") == 0))
            index--;
    }

    assert(!"index invalid");
    return NULL;
}
