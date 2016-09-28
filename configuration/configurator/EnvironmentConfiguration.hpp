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

#ifndef _ENVIRONMENT_CONFIGURATON_HPP_
#define _ENVIRONMENT_CONFIGURATON_HPP_

#include "jptree.hpp"
#include "ConfigFileComponentUtils.hpp"

namespace CONFIGURATOR
{

class CEnvironmentConfiguration : public CConfigFileComponentUtils
{
public:

    IMPLEMENT_IINTERFACE

    enum CEF_ERROR_CODES{ CF_NO_ERROR = 0,
                          CF_UNKNOWN_COMPONENT,
                          CF_UNKNOWN_ESP_COMPONENT,
                          CF_COMPONENT_INSTANCE_NOT_FOUND,
                          CF_OTHER = 0xFF };

    static CEnvironmentConfiguration* getInstance();

    virtual ~CEnvironmentConfiguration();

    enum CEF_ERROR_CODES generateBaseEnvironmentConfiguration();
    enum CEF_ERROR_CODES addComponent(const char* pCompType);
    enum CEF_ERROR_CODES removeComponent(const char* pCompType, const char* pCompName);
    enum CEF_ERROR_CODES addESPService(const char* espServiceType);

protected:

    CEnvironmentConfiguration();
    Owned<IPropertyTree> m_pEnv;
private:
};
}
#endif // _ENVIRONMENT_CONFIGURATON_HPP_
