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

#ifndef HPCCSYSTEMS_PLATFORM_OPERATION_HPP
#define HPCCSYSTEMS_PLATFORM_OPERATION_HPP

#include "Variable.hpp"
#include "Variables.hpp"
#include "EnvironmentMgr.hpp"
#include <string>
#include <vector>


struct modAttribute {
    modAttribute() : duplicateSaveValueOk(false), doNotSet(false),
            errorIfNotFound(false), errorIfEmpty(false) {}
    ~modAttribute() = default;
    void addName(const std::string &_name) { names.emplace_back(_name); }
    const std::string &getName(std::size_t idx=0) { return names[idx]; }
    std::size_t getNumNames() { return names.size(); }
    std::vector<std::string> names;
    std::string value;
    std::string startIndex;
    std::string cookedValue;
    std::string saveValue;
    bool doNotSet;
    bool duplicateSaveValueOk;
    bool errorIfNotFound;
    bool errorIfEmpty;
};


class EnvironmentMgr;

class Operation
{

    public:

        Operation() : m_count("1"), m_startIndex("0") {}
        virtual ~Operation() = default;
        bool execute(EnvironmentMgr *pEnvMgr, Variables *pInputs);
        void addAttribute(modAttribute &newAttribute);
        void assignAttributeCookedValues(Variables *pInputs);


    protected:

        virtual void doExecute(EnvironmentMgr *pEnvMgr, Variables *pInputs) = 0;
        void getParentNodeIds(EnvironmentMgr *pEnvMgr, Variables *pInputs);
        std::shared_ptr<Variable> createInput(std::string inputName, const std::string &inputType, Variables *pInputs, bool existingOk);
        bool createAttributeSaveInputs(Variables *pInputs);
        void saveAttributeValues(Variables *pInputs, const std::shared_ptr<EnvironmentNode> &pEnvNode);


    protected:

        std::string m_path;
        std::string m_parentNodeId;
        std::vector<std::string> m_parentNodeIds;
        std::vector<modAttribute> m_attributes;
        std::string m_count;
        std::string m_startIndex;
        bool m_throwOnEmpty = true;
        std::string m_saveNodeIdName;
        bool m_duplicateSaveNodeIdInputOk = false;


    friend class EnvModTemplate;
};


#endif //HPCCSYSTEMS_PLATFORM_OPERATION_HPP
