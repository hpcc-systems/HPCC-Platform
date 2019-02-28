/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef HPCCSYSTEMS_PLATFORM_CHECKESPPORTBINDINGS_HPP
#define HPCCSYSTEMS_PLATFORM_CHECKESPPORTBINDINGS_HPP

#include "HPCCConfigSupport.hpp"
#include <set>

class CheckPortBindings : public HPCCConfigSupport
{
    public:

        CheckPortBindings(EnvironmentMgr *pEnvMgr) : HPCCConfigSupport(pEnvMgr) {}
        void validate(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const;


    protected:

        void checkEspPortBindings(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const;
        void checkProcessPortBindings(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const;
//        void checkHwPortBindings(Status &status) const;
        void addPortToHwInstance(std::string netAddress, std::string port, std::string nodeId, Status &status) const;


    protected:

        mutable std::map<std::string, std::set<std::string>> m_hwInstancePortUsage;

};


#endif //HPCCSYSTEMS_PLATFORM_CHECKESPPORTBINDINGS_HPP
