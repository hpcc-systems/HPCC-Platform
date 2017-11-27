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

#ifndef _CONFIG2_VALUESET_HPP_
#define _CONFIG2_VALUESET_HPP_

#include <memory>
#include <vector>
#include "CfgValue.hpp"
#include "ConfigItem.hpp"


class ConfigItemValueSet : public ConfigItem
{
    public:

		ConfigItemValueSet(const std::string &name, std::shared_ptr<ConfigItem> pParent) : ConfigItem(name, "valueset", pParent) { }
        virtual ~ConfigItemValueSet() { };

        void addCfgValue(const std::shared_ptr<CfgValue> pValue);
        void addCfgValue(const std::shared_ptr<ConfigItemValueSet> &valueSet);
        const std::vector<std::shared_ptr<CfgValue>> &getCfgValues() const;


    private:

        std::shared_ptr<CfgValue> findValue(const std::string &valueName, bool throwIfNotFound = true) const;


    protected:

        std::vector<std::shared_ptr<CfgValue>> m_cfgValues;

};


#endif // _CONFIG2_VALUESET_HPP_