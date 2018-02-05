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
#include "SchemaType.hpp"
#include "Status.hpp"
#include "platform.h"


class DECL_EXPORT SchemaValue
{
    public:

        SchemaValue(const std::string &name, bool isDefined = true);
        virtual ~SchemaValue() { }
        void setType(const std::shared_ptr<SchemaType> pType) { m_pType = pType; }
        const std::shared_ptr<SchemaType> &getType() const { return m_pType; }
        const std::string &getName() const { return m_name; }
        bool isValueValid(const std::string &newValue, const EnvironmentValue *pEnvValue = nullptr) const;
        void setDisplayName(const std::string &displayName) { m_displayName = displayName; }
        const std::string &getDisplayName() const { return m_displayName; }
        void setRequired(bool reqd) { bitMask.m_required = reqd; }
        bool isRequired() const { return bitMask.m_required; }
        void setDefaultValue(const std::string &dflt) { m_default = dflt; }
        const std::string &getDefaultValue() const { return m_default; }
        bool hasDefaultValue() const { return !m_default.empty(); }
        void setReadOnly(bool readOnly) { bitMask.m_readOnly = readOnly; }
        bool isReadOnly() const { return bitMask.m_readOnly; }
        void setHidden(bool hidden) { bitMask.m_hidden = hidden; }
        bool isHidden() const { return bitMask.m_hidden; }
        void setDeprecated(bool deprecated) { bitMask.m_deprecated = deprecated; }
        bool isDeprecated() const { return bitMask.m_deprecated; }
        void setTooltip(const std::string &tooltip) { m_tooltip = tooltip; }
        const std::string &getTooltip() const { return m_tooltip; }
        void addModifer(const std::string &mod) { m_modifiers.push_back(mod); }
        void setModifiers(const std::vector<std::string> &list) { m_modifiers = list;  }
        const std::vector<std::string> &getModifiers() const { return m_modifiers; }
        bool hasModifiers() const { return m_modifiers.size() != 0; }
        void setUniqueValue(bool isUnique) { bitMask.m_isUnique = isUnique; }
        bool isUniqueValue() const { return bitMask.m_isUnique;  }
        void setUniqueValueSetRef(const std::shared_ptr<SchemaValue> &pValue) { m_pUniqueValueSetRefs.push_back(pValue);  }
        bool isFromUniqueValueSet() const { return !m_pUniqueValueSetRefs.empty(); }
        const std::vector<std::weak_ptr<SchemaValue>> &getUniqueValueSetRefs() const { return m_pUniqueValueSetRefs;  }
        bool isDefined() const { return bitMask.m_isDefined;  }
        void resetEnvironment();
        void setMirrorFromPath(const std::string &path) { m_mirrorFromPath = path;  }
        const std::string &getMirrorFromPath() const { return m_mirrorFromPath;  }
        bool isMirroredValue() const { return m_mirrorFromPath.length() != 0; }
        void addMirroredSchemaValue(const std::shared_ptr<SchemaValue> &pVal) { m_mirrorToSchemaValues.push_back(pVal); }
        void mirrorValueToEnvironment(const std::string &oldValue, const std::string &newValue);
        void addEnvironmentValue(const std::shared_ptr<EnvironmentValue> &pEnvValue) { m_envValues.push_back(pEnvValue); }
        void getAllEnvironmentValues(std::vector<std::string> &values) const;
        void setMirroredEnvironmentValues(const std::string &oldValue, const std::string &newValue);
        void validate(Status &status, const std::string &id, const EnvironmentValue *pEnvValue = nullptr) const;
        void getAllowedValues(std::vector<AllowedValue> &allowedValues, const EnvironmentValue *pEnvValue = nullptr) const;
        void setAutoGenerateType(const std::string &type) { m_autoGenerateType = type; }
        const std::string &getAutoGenerateType() const { return m_autoGenerateType; }
        void setAutoGenerateValue(const std::string &value) { m_autoGenerateValue = value; }
        const std::string &getAutoGenerateValue() const { return m_autoGenerateValue; }
        void getAllKeyRefValues(std::vector<std::string> &keyRefValues) const;


    protected:

        std::shared_ptr<SchemaType> m_pType;
        std::vector<std::weak_ptr<EnvironmentValue>> m_envValues;
        std::vector<std::shared_ptr<SchemaValue>> m_mirrorToSchemaValues;
        std::string m_name;
        std::string m_displayName;
        std::string m_mirrorFromPath;
        std::string m_autoGenerateValue;
        std::string m_autoGenerateType;

        struct {
            unsigned m_required  : 1;
            unsigned m_readOnly  : 1;
            unsigned m_hidden    : 1;
            unsigned m_deprecated: 1;
            unsigned m_isUnique  : 1;
            unsigned m_isDefined : 1;
        } bitMask;

        std::string m_default;
        std::string m_tooltip;
        std::vector<std::string> m_modifiers;
        std::vector<std::weak_ptr<SchemaValue>> m_pUniqueValueSetRefs;    // this value serves as the key from which values are valid
};

#endif // _CONFIG2_VALUE_HPP_