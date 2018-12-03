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

class Input;

std::shared_ptr<Input> inputValueFactory(const std::string &type, const std::string &name);

class Input
{
    public:

        Input() = default;
        virtual ~Input() = default;
        const std::string &getName() const { return m_name; }
        void setName(const std::string &name) { m_name = name; }
        const std::string &getUserPrompt() const { return m_userPrompt; }
        void setUserPrompt(const std::string &userPrompt) { m_userPrompt = userPrompt; }
        const std::string &getDescription() const { return m_description; }
        void setDescription(const std::string &description) { m_description = description; }
        const std::string &getTooltip() const { return m_tooltip; }
        void setTooltip(const std::string &tooltip) { m_tooltip = tooltip; }
        size_t getNumValues() const { return m_values.size(); }
        virtual void setValue(const std::string &value);
        void addValue(const std::string &value) { m_values.emplace_back(value); }
        virtual std::string getValue(size_t idx) const;
        void setPreparedValue(const std::string &value) { m_preparedValue = value; }
        const std::string &getPreparedValue() const { return m_preparedValue; }
        void setNonUserInput(bool val) { m_nonUserInput = val; }
        bool needsUserInput() const { return !m_nonUserInput; }


    protected:

        std::string m_name;
        std::string m_userPrompt;
        std::string m_description;
        std::string m_tooltip;
        std::string m_preparedValue;
        bool m_nonUserInput = false;
        std::vector<std::string> m_values;
};


#endif //HPCCSYSTEMS_PLATFORM_ENVMODTEMPLATEINPUT_HPP
