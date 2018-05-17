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

#ifndef _CONFIG2_CFGTYPE_HPP_
#define _CONFIG2_CFGTYPE_HPP_

#include <string>
#include <memory>
#include <map>
#include <vector>
#include "SchemaTypeLimits.hpp"
#include "platform.h"



class DECL_EXPORT SchemaType
{
    public:

        SchemaType(const std::string &name) : m_name(name), m_pLimits(std::make_shared<SchemaTypeLimits>()) { }
        SchemaType() : SchemaType("") { }
        virtual ~SchemaType() { }

        std::shared_ptr<SchemaTypeLimits> &getLimits() { return m_pLimits; }
        void setLimits(const std::shared_ptr<SchemaTypeLimits> &pLimits) { m_pLimits = pLimits; }
        bool isValid() const { return m_pLimits!=nullptr; }
        const std::string &getName() const { return m_name; }
        void setName(const std::string &name) { m_name = name;  }
        bool isValueValid(const std::string &testValue) const { return m_pLimits->isValueValid(testValue); }
        bool isEnumerated() const { return m_pLimits->isEnumerated(); }
        const std::vector<AllowedValue> getEnumeratedValues() const { return m_pLimits->getEnumeratedValues(); }
        const std::string getLimitString() const { return m_pLimits->getLimitString();  }
        const std::string &getValidateMsg() const { return m_pLimits->getValidateMsg(); }
        const std::string &getValidateMsgType() const { return m_pLimits->getValidateMsgType(); }


    private:

        std::string m_name;
        std::shared_ptr<SchemaTypeLimits> m_pLimits;

};


#endif // _CONFIG2_CFGTYPE_HPP_
