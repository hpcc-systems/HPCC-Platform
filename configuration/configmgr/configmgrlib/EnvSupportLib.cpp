/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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


#include "EnvSupportLib.hpp"
#include "Exceptions.hpp"
#include "HPCCConfigSupport.hpp"
#include "jutil.hpp"

EnvSupportLib::EnvSupportLib(const std::string &libName, EnvironmentMgr *pEnvMgr) :
    m_libName(libName), m_libHandle(nullptr), m_pSupportLib(nullptr)
{
    m_libHandle = LoadSharedObject(m_libName.c_str(), true, false);
    if (m_libHandle != nullptr)
    {
        auto getInstanceProc = (getHPCCSupportLib_t) GetSharedProcedure(m_libHandle, "getCfgMgrSupportInstance");
        if (getInstanceProc != nullptr)
        {
            m_pSupportLib = getInstanceProc(pEnvMgr);;
        }
    }
}


EnvSupportLib::~EnvSupportLib()
{
    if (m_libHandle != nullptr)
    {
        FreeSharedObject(m_libHandle);
        m_libHandle = nullptr;
    }
}


void EnvSupportLib::processEvent(const std::string &event, const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEventNode, Status &status) const
{
    m_pSupportLib->processEvent(event, pSchema, pEventNode, status);
}


void EnvSupportLib::validate(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const
{
    m_pSupportLib->validate(pSchema, pEnvironment, status);
}
