/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems�.

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

#include "EnvValue.hpp"
#include "EnvironmentNode.hpp"

bool EnvValue::setValue(const std::string &value, Status *pStatus, bool forceSet)
{ 
    bool rc = true;
    std::string oldValue = m_value;
    if (m_pCfgValue)
    {
        m_forcedSet = false;
        if (m_pCfgValue->isValueValid(value))
        {
            m_value = value;
            m_valueSet = true;
            m_pCfgValue->mirrorValue(oldValue, value);
        }
        else if (forceSet)
        {
            m_value = value;
            m_valueSet = true;
            m_forcedSet = true;
            m_pCfgValue->mirrorValue(oldValue, value);
            if (pStatus != nullptr)
                pStatus->addStatusMsg(statusMsg::info, m_pMyEnvNode.lock()->getId(), m_name, "", "Attribute forced to invalid value");
            rc = true;
        }
        else
        {
            if (pStatus != nullptr)
                pStatus->addStatusMsg(statusMsg::error, m_pMyEnvNode.lock()->getId(), m_name, "", "New value is not valid");
            //todo, use the cfgValue->cfgType->getstring or whatever to get a status message as to why it's not valid (in line after the not valid above)
        }
    }
    return rc;
}


bool EnvValue::checkCurrentValue()
{
    bool rc = true;
    if (m_pCfgValue)
    {
        if (!m_pCfgValue->isValueValid(m_value))
        {
            rc = false;
        }
    }
    else
    {
        rc = false;
    }
    return rc;
}


std::vector<std::string> EnvValue::getAllValues() const
{
    std::shared_ptr<EnvironmentNode> pEnvNode = m_pMyEnvNode.lock();
    return pEnvNode->getAllFieldValues(m_pCfgValue->getName());
}


bool EnvValue::isValueValid(const std::string &value) const
{
    return m_pCfgValue->isValueValid(value, this);
}


void EnvValue::validate(Status &status, const std::string &myId) const
{

    if (!m_pCfgValue->isConfigured())
        status.addStatusMsg(statusMsg::warning, myId, m_name, "", "No configuration exists for this value");

    if (m_forcedSet)
        status.addStatusMsg(statusMsg::warning, myId, m_name, "", "Current value was force set");

    // Will generate status based on current value and type
    m_pCfgValue->validate(status, myId, this);
}

