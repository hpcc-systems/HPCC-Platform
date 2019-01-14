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


#ifndef HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATEINPUT_HPP
#define HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATEINPUT_HPP


#include <string>
#include <memory>
#include <vector>

class Variable;

std::shared_ptr<Variable> variableFactory(const std::string &type, const std::string &name);

class Variable
{
    public:

        explicit Variable(const std::string &name) : m_name(name) {}
        virtual ~Variable() = default;
        const std::string &getName() const { return m_name; }
        const std::string &getUserPrompt() const { return m_userPrompt; }
        const std::string &getDescription() const { return m_description; }
        size_t getNumValues() const { return m_values.size(); }
        virtual void addValue(const std::string &value);
//        void addValue(const std::string &value) { m_values.emplace_back(value); }
        virtual std::string getValue(size_t idx) const;
        bool isUserInput() const { return m_userInput; }
        const std::string &getPreparedValue() const { return m_preparedValue; }



    protected:

        std::string m_name;
        std::string m_userPrompt;
        std::string m_description;
        std::string m_preparedValue;
        bool m_userInput = true;
        std::vector<std::string> m_values;


    friend class EnvModTemplate;
};


#endif //HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATEINPUT_HPP
