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

#ifndef _CONFIG2_VALUE_HPP_
#define _CONFIG2_VALUE_HPP_

#include <memory>
#include "CfgType.hpp"
#include "Status.hpp"

class CfgValue 
{
    public:

        CfgValue(const std::string &name, bool isConfigured =true) :
            m_name(name), m_displayName(name), m_required(false), m_readOnly(false), 
            m_hidden(false), m_defaultSet(false), m_deprecated(false), m_isUnique(false),
            m_isConfigured(isConfigured) { }
        virtual ~CfgValue() { }
        void setType(const std::shared_ptr<CfgType> pType) { m_pType = pType; }
        const std::shared_ptr<CfgType> &getType() const { return m_pType; }
        const std::string &getName() const { return m_name; }
        bool isValueValid(const std::string &newValue, const EnvValue *pEnvValue = nullptr) const;
        void setDisplayName(const std::string &displayName) { m_displayName = displayName; }
        const std::string &getDisplayName() const { return m_displayName; }
        void setRequired(bool reqd) { m_required = reqd; }
        bool isRequired() const { return m_required; }
        void setDefault(const std::string &dflt) { m_default = dflt; m_defaultSet = (dflt != ""); }
        const std::string &getDefaultValue() const { return m_default; }
        bool hasDefaultValue() const { return m_defaultSet; }
        void setReadOnly(bool readOnly) { m_readOnly = readOnly; }
        bool isReadOnly() const { return m_readOnly; }
        void setHidden(bool hidden) { m_hidden = hidden; }
        bool isHidden() const { return m_hidden; }
        void setDeprecated(bool deprecated) { m_deprecated = deprecated; }
        bool isDeprecated() const { return m_deprecated; }
        void setTooltip(const std::string &tooltip) { m_tooltip = tooltip; }
        const std::string &getTooltip() const { return m_tooltip; }
        void addModifer(const std::string &mod) { m_modifiers.push_back(mod); }
        void setModifiers(const std::vector<std::string> &list) { m_modifiers = list;  }
        const std::vector<std::string> &getModifiers() const { return m_modifiers; }
        bool hasModifiers() const { return m_modifiers.size() != 0; }
        void setUniqueValue(bool isUnique) { m_isUnique = isUnique; }
        bool isUniqueValue() const { return m_isUnique;  }
        void setUniqueValueSetRef(const std::shared_ptr<CfgValue> &pValue) { m_pUniqueValueSetRefs.push_back(pValue);  }
        bool isFromUniqueValueSet() const { return !m_pUniqueValueSetRefs.empty(); }
        std::vector<std::weak_ptr<CfgValue>> getUniqueValueSetRefs() const { return m_pUniqueValueSetRefs;  }  
        bool isConfigured() const { return m_isConfigured;  }
        void resetEnvironment();
        void setMirrorFromPath(const std::string &path) { m_mirrorFromPath = path;  }
        const std::string &getMirrorFromPath() const { return m_mirrorFromPath;  }
        bool isMirroredValue() const { return m_mirrorFromPath.length() != 0; }
        void addMirroredCfgValue(const std::shared_ptr<CfgValue> &pVal) { m_mirrorToCfgValues.push_back(pVal); }
        void addEnvValue(const std::shared_ptr<EnvValue> &pEnvValue) { m_envValues.push_back(pEnvValue); }
        std::vector<std::string> getAllEnvValues() const;
        void mirrorValue(const std::string &oldValue, const std::string &newValue);
        void setMirroredEnvValues(const std::string &oldValue, const std::string &newValue);
        void validate(Status &status, const std::string &id, const EnvValue *pEnvValue = nullptr) const;
        std::vector<AllowedValue> getAllowedValues(const EnvValue *pEnvValue = nullptr) const;


    protected:

        std::vector<std::string> getAllKeyRefValues(const EnvValue *pEnvValue) const;


    protected:

        std::shared_ptr<CfgType> m_pType;
        std::vector<std::weak_ptr<EnvValue>> m_envValues;
        std::vector<std::shared_ptr<CfgValue>> m_mirrorToCfgValues;
        std::string m_name;
        std::string m_displayName;
        std::string m_mirrorFromPath;
        bool m_required;
        bool m_readOnly;
        bool m_hidden;
        bool m_defaultSet;
        bool m_deprecated;
        bool m_isUnique;
        bool m_isConfigured;  
        std::string m_default;
        std::string m_tooltip;
        std::vector<std::string> m_modifiers;
        std::vector<std::weak_ptr<CfgValue>> m_pUniqueValueSetRefs;    
};

#endif // _CONFIG2_VALUE_HPP_