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

class EnvironmentNode;


class DECL_EXPORT SchemaValue
{
    public:

        SchemaValue(const std::string &name, bool isDefined = true);
        SchemaValue(const SchemaValue &value);
        virtual ~SchemaValue() { }
        void setType(const std::shared_ptr<SchemaType> pType) { m_pType = pType; }
        const std::shared_ptr<SchemaType> &getType() const { return m_pType; }
        const std::string &getName() const { return m_name; }
        bool isValueValid(const std::string &newValue, const EnvironmentValue *pEnvValue = nullptr) const;
        void setDisplayName(const std::string &displayName) { m_displayName = displayName; }
        const std::string &getDisplayName() const { return m_displayName; }
        void setRequired(bool reqd) { bitMask.m_required = reqd; }
        bool isRequired() const { return bitMask.m_required; }
        void setForcedValue(const std::string &dflt) { m_forcedValue = dflt; }
        const std::string &getForcedValue() const { return m_forcedValue; }
        bool hasForcedValue() const { return !m_forcedValue.empty(); }
        void setReadOnly(bool readOnly) { bitMask.m_readOnly = readOnly; }
        bool isReadOnly() const { return bitMask.m_readOnly; }
        void setHiddenIf(const std::string &hiddenIf) { m_hiddenIf = hiddenIf; }
        void setInvertHiddenIf(bool invert) { m_invertHiddenIf = invert; }
        bool getInvertHiddenIf() const { return m_invertHiddenIf; }
        void setHidden(bool hidden) { bitMask.m_hidden = hidden; }
        bool isHidden(const EnvironmentValue *pEnvValue=nullptr) const;
        void setDeprecated(bool deprecated) { bitMask.m_deprecated = deprecated; }
        bool isDeprecated() const { return bitMask.m_deprecated; }
        void setTooltip(const std::string &tooltip) { m_tooltip = tooltip; }
        const std::string &getTooltip() const { return m_tooltip; }
        void addModifer(const std::string &mod) { m_modifiers.push_back(mod); }
        void setModifiers(const std::vector<std::string> &list) { m_modifiers = list;  }
        const std::vector<std::string> &getModifiers() const { return m_modifiers; }
        bool hasModifiers() const { return m_modifiers.size() != 0; }
        bool hasModifier(const std::string &modifier) const;
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
        void mirrorValueToEnvironment(const std::string &oldValue, const std::string &newValue, Status *pStatus = nullptr);
        void addEnvironmentValue(const std::shared_ptr<EnvironmentValue> &pEnvValue) { m_envValues.push_back(pEnvValue); }
        void getAllEnvironmentValues(std::vector<std::shared_ptr<EnvironmentValue>> &envValues) const;
        void validate(Status &status, const std::string &id, const EnvironmentValue *pEnvValue = nullptr) const;
        bool getAllowedValues(std::vector<AllowedValue> &allowedValues, const std::shared_ptr<const EnvironmentNode> &pEnvNode) const;
        void setAutoGenerateType(const std::string &type) { m_autoGenerateType = type; }
        const std::string &getAutoGenerateType() const { return m_autoGenerateType; }
        void setAutoGenerateValue(const std::string &value) { m_autoGenerateValue = value; }
        const std::string &getAutoGenerateValue() const { return m_autoGenerateValue; }
        void getAllKeyRefValues(std::vector<std::string> &keyRefValues) const;
        void setPresetValue(const std::string &value) { m_presetValue = value; }
        const std::string &getPresetValue() const { return m_presetValue; }
        void setValueLimitRuleType(const std::string &type) { m_valueLimitRuleType = type; }
        const std::string &getValueLimitRuleType() { return m_valueLimitRuleType; }
        void setValueLimitRuleData(const std::string &data) { m_valueLimitRuleData = data; }
        const std::string &getValueLimitRuleData() { return m_valueLimitRuleData; }
        void setRequiredIf(const std::string &reqIf) { m_requiredIf = reqIf; }
        const std::string &getRequiredIf() const { return m_requiredIf; }
        void setGroupByName(const std::string &group) { m_groupByName = group; }
        const std::string &getGroupByName() const { return m_groupByName; }


    protected:

        // DON'T FORGET IF DATA ADDED, IT MAY MAY TO BE COPIED IN THE COPY CONSTRUCTOR!!
        std::shared_ptr<SchemaType> m_pType;
        std::string m_name;
        std::string m_displayName;
        std::string m_mirrorFromPath;
        std::string m_autoGenerateValue;
        std::string m_autoGenerateType;
        std::string m_valueLimitRuleType;
        std::string m_valueLimitRuleData;
        std::string m_requiredIf;
        std::string m_groupByName;
        std::string m_hiddenIf;
        bool m_invertHiddenIf;
        // DON'T FORGET IF DATA ADDED, IT MAY MAY TO BE COPIED IN THE COPY CONSTRUCTOR!!

        struct {
            unsigned m_required  : 1;
            unsigned m_readOnly  : 1;
            unsigned m_hidden    : 1;
            unsigned m_deprecated: 1;
            unsigned m_isUnique  : 1;
            unsigned m_isDefined : 1;
        } bitMask;

        // DON'T FORGET IF DATA ADDED, IT MAY MAY TO BE COPIED IN THE COPY CONSTRUCTOR!!
        std::string m_forcedValue;        // value written to environment if no user value supplied
        std::string m_presetValue;    // informational value nform user code default if no value supplied
        std::string m_tooltip;
        std::vector<std::string> m_modifiers;
        // DON'T FORGET IF DATA ADDED, IT MAY MAY TO BE COPIED IN THE COPY CONSTRUCTOR!!

        // These values are NOT copied in the copy constructor
        std::vector<std::weak_ptr<EnvironmentValue>> m_envValues;
        std::vector<std::shared_ptr<SchemaValue>> m_mirrorToSchemaValues;
        std::vector<std::weak_ptr<SchemaValue>> m_pUniqueValueSetRefs;    // this value serves as the key from which values are valid
};

#endif // _CONFIG2_VALUE_HPP_
