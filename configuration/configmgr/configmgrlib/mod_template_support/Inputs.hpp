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


#ifndef HPCCSYSTEMS_PLATFORM_INPUTS_HPP
#define HPCCSYSTEMS_PLATFORM_INPUTS_HPP

#include <string>
#include <vector>
#include <memory>

class Input;

class Inputs
{
    public:

        Inputs() = default;
        ~Inputs() = default;
        void add(const std::shared_ptr<Input> pInput);
        const std::vector<std::shared_ptr<Input>> &all(unsigned phase=0) const  { return m_inputValues; }
        std::shared_ptr<Input> getInput(const std::string &name, bool throwIfNotFound = true) const;
        void setInputIndex(size_t idx) { m_curIndex = idx; }
        std::string doValueSubstitution(const std::string &value) const;
        void prepare();
        void clear();


    protected:

        std::string evaluate(const std::string &expr) const;
        std::size_t findClosingDelimiter(const std::string &input, std::size_t startPos, const std::string &openDelim, const std::string &closeDelim) const;


    private:

        std::vector<std::shared_ptr<Input>> m_inputValues;
        size_t m_curIndex = 0;
};


#endif //HPCCSYSTEMS_PLATFORM_INPUTS_HPP
