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

#ifndef _BUILD_SET_HPP_
#define _BUILD_SET_HPP_

#include "jstring.hpp"
#include "jlib.hpp"

namespace CONFIGURATOR
{

static const char* DEFAULT_BUILD_SET_XML_FILE("buildset.xml");
static const char* DEFAULT_BUILD_SET_DIRECTORY("/opt/HPCCSystems/componentfiles/configxml/");

class CBuildSet;

class CBuildSetManager
{
public:

    static CBuildSetManager* getInstance(const char* pBuildSetFile =  NULL, const char* pBuildSetDirectory = NULL);

    virtual ~CBuildSetManager();
    void getBuildSetComponents(::StringArray& buildSetArray) const;
    void getBuildSetServices(::StringArray& buildSetArray) const;
    void setBuildSetArray(const ::StringArray &strArray);
    const char* getBuildSetServiceName(int index) const;
    const char* getBuildSetServiceFileName(int index) const;
    const char* getBuildSetComponentTypeName(int index) const;
    const char* getBuildSetComponentFileName(int index) const;
    const char* getBuildSetProcessName(int index) const;
    const char* getBuildSetSchema(int index) const;
    const int getBuildSetSchemaCount() const;
    const int getBuildSetServiceCount() const;
    const int getBuildSetComponentCount() const;
    bool populateBuildSet();

protected:

    CBuildSetManager();
    CBuildSetManager(const char* pBuildSetFile, const char* pBuildSetDir);
    const CBuildSet* getBuildSetComponent(int index) const;
    const CBuildSet* getBuildSetService(int index) const;
    CIArrayOf<CBuildSet> m_buildSetArray;
    Owned<IPropertyTree> m_buildSetTree;
    StringBuffer m_buildSetFile;
    StringBuffer m_buildSetDir;
    StringBuffer m_buildSetPath;
};


class CBuildSet : public CInterface
{
public:

    IMPLEMENT_IINTERFACE

    CBuildSet(const char* pInstallSet = NULL, const char* pName = NULL, const char* pProcessName = NULL, const char* pSchema = NULL, const char* pDeployable = NULL, const char *pOveride = NULL) : m_pInstallSet(pInstallSet), m_pName(pName), m_pProcessName(pProcessName), m_pSchema(pSchema), m_pDeployable(pDeployable), m_pOveride(pOveride)
    {
    }

    virtual ~CBuildSet()
    {
    }

    const char* getInstallSet() const
    {
        return m_pInstallSet;
    }
    const char* getName() const
    {
        return m_pName;
    }
    const char* getProcessName() const
    {
        return m_pProcessName;
    }
    const char* getSchema() const
    {
        return m_pSchema;
    }
    const char* getDeployable() const
    {
        return m_pDeployable;
    }
    const char* getOveride() const
    {
        return m_pOveride;
    }

protected:

    CBuildSet();
    CBuildSet(const CBuildSet& buildSet) : m_pInstallSet(buildSet.m_pInstallSet), m_pName(buildSet.m_pName), m_pProcessName(buildSet.m_pProcessName), m_pSchema(buildSet.m_pSchema), m_pDeployable(buildSet.m_pDeployable)
    {}
    const char* m_pInstallSet;
    const char* m_pName;
    const char* m_pProcessName;
    const char* m_pSchema;
    const char* m_pDeployable;
    const char* m_pOveride;

private:
};

}
#endif // _BUILD_SET_HPP_
