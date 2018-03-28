/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#ifndef _CONFIG2_CFGLIMITS_HPP_
#define _CONFIG2_CFGLIMITS_HPP_

#include <memory>
#include <vector>
#include <string>
#include "platform.h"


class EnvironmentValue;

struct DECL_EXPORT AllowedValue
{
    AllowedValue(const std::string &value, const std::string &desc="") : m_value(value), m_displayName(value), m_description(desc) { }
    std::string m_displayName;
    std::string m_value;
    std::string m_description;
};

class DECL_EXPORT SchemaTypeLimits
{
    public:

        SchemaTypeLimits() { }
        virtual ~SchemaTypeLimits() { }
        void addAllowedValue(const std::string &value, const std::string &desc="") { m_enumeratedValues.push_back(AllowedValue(value, desc)); }
        std::vector<AllowedValue> getEnumeratedValues() const;
        bool isEnumerated() const { return !m_enumeratedValues.empty(); }
        bool isValueValid(const std::string &testValue) const;
        virtual std::string getLimitString() const { return "No value limits"; }
        virtual bool isMaxSet() const { return false; }
        virtual bool isMinSet() const { return false; }
        virtual int getMax() const { return 0; }
        virtual int getMin() const { return 0; }


    protected:

        virtual bool isValidEnumeratedValue(const std::string &testValue) const;
        virtual bool doValueTest(const std::string &testValue) const { return true;  }


    protected:

        std::vector<AllowedValue> m_enumeratedValues;
};



#endif // _CONFIG2_CFGLIMITS_HPP_
