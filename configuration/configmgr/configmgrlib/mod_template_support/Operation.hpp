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

#include "Input.hpp"
#include "Inputs.hpp"
#include <string>
#include <vector>
#include <map>


struct modAttribute {
    modAttribute(const std::string &_name, const std::string &_value) :
        name(_name), value(_value) { };
    ~modAttribute() = default;
    std::string name;
    std::string value;
    std::string startIndex;
    std::string cookedValue;
    std::string saveValue;
    bool doNotSet;
};

class EnvironmentMgr;

class Operation
{

    public:

        Operation() : m_count("1"), m_startIndex("0") {}
        virtual ~Operation() = default;
        bool execute(EnvironmentMgr *pEnvMgr, Inputs *pInputs);
        void setPath(const std::string &path) { m_path = path; }
        std::string getPath() const { return m_path; }
        void setParentNodeId(const std::string &id) { m_parentNodeId = id; }
        std::string getParentNodeId() const { return m_parentNodeId; }
        bool hasNodeId() const { return !m_parentNodeId.empty(); }
        void addAttribute(modAttribute &newAttribute);
        void assignAttributeCookedValues(Inputs *pInputs);
        void setCount(const std::string &count) { m_count = count; }
        void setStartIndex(const std::string &startIndex) { m_startIndex = startIndex; }


    protected:

        virtual void doExecute(EnvironmentMgr *pEnvMgr, Inputs *pInputs) = 0;


    protected:

        std::string m_path;
        std::string m_parentNodeId;
        std::vector<modAttribute> m_attributes;
        std::string m_count;
        std::string m_startIndex;
};


#endif //HPCCSYSTEMS_PLATFORM_OPERATION_HPP
