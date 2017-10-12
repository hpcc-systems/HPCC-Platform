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

#ifndef _CONFIG2_CFGTYPE_HPP_
#define _CONFIG2_CFGTYPE_HPP_

#include <string>
#include <memory>
#include <map>
#include "CfgLimits.hpp"



class CfgType 
{
    public:

        CfgType(const std::string &name) : m_name(name), m_autoValueType("") { }
        virtual ~CfgType() { }

        std::shared_ptr<CfgLimits> &getLimits() { return m_pLimits; }
        void setLimits(const std::shared_ptr<CfgLimits> &pLimits) { m_pLimits = pLimits; }
        bool isValid() const { return m_pLimits!=nullptr; }
        const std::string &getName() { return m_name; }
        bool isValueValid(const std::string &testValue) { return m_pLimits->isValueValid(testValue); }
		bool isAutoValueType() const { return m_autoValueType != "" ;  }
		const std::string &getAutoValue() const { return m_autoValueType;  }  // todo: this is to be expanded to the supported types
		void setAutoValueType(const std::string &valueType) { m_autoValueType = valueType;  }
		

    private:

        std::string m_name;
        std::shared_ptr<CfgLimits> m_pLimits;
		std::string m_autoValueType;

};


#endif // _CONFIG2_CFGTYPE_HPP_
