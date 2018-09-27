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

#ifndef HPCCSYSTEMS_PLATFORM_CONFIG2_SUPPORT_H
#define HPCCSYSTEMS_PLATFORM_CONFIG2_SUPPORT_H

#include <memory>
#include "EnvironmentMgr.hpp"
#include "Status.hpp"
#include "SchemaItem.hpp"
#include "EnvironmentNode.hpp"
#include "platform.h"


class HPCCConfigSupport
{
    public:

        HPCCConfigSupport(EnvironmentMgr *pEnvMgr) : m_pEnvMgr(pEnvMgr) { }
        virtual void processEvent(const std::string &event, const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEventNode, Status &status) const { }
        virtual void validate(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const { }


    protected:

        EnvironmentMgr *m_pEnvMgr;
};

typedef HPCCConfigSupport* (*getHPCCSupportLib_t)(EnvironmentMgr *pEnvMgr);


#endif //HPCCSYSTEMS_PLATFORM_CONFIG2_SUPPORT_H
