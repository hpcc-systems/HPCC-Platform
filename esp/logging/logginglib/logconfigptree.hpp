/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _LOGCONFIGPTREE_HPP_
#define _LOGCONFIGPTREE_HPP_

#include "jptree.hpp"
#include "jstring.hpp"
#include "tokenserialization.hpp"

namespace LogConfigPTree
{
    /**
     * When relying upon a default value, check and report if the default value is not compatibile
     * with the requested value type. Most mismatches are errors. Some may be recorded as warnings
     * if a reasonable use case, e.g., defaulting an unsigned value to -1, exists to justify it.
     * In all cases, recorded findings should be addressed by either changing the default value or
     * casting the intended value correctly.
     */
    template <typename value_t, typename default_t>
    value_t applyDefault(const default_t& defaultValue, const char* xpath)
    {
        auto report = [&](bool isError)
        {
            StringBuffer msg;
            msg << "unexpected default value '" << defaultValue << "'";
            if (!isEmptyString(xpath))
                msg << " for configuration XPath '" << xpath << "'";
            if (isError)
                IERRLOG("%s", msg.str());
            else
                IWARNLOG("%s", msg.str());
        };

        if (std::is_integral<value_t>() == std::is_integral<default_t>())
        {
            if (std::is_signed<value_t>() == std::is_signed<default_t>())
            {
                if (defaultValue < std::numeric_limits<value_t>::min())
                    report(true);
                else if (defaultValue > std::numeric_limits<value_t>::max())
                    report(true);
            }
            else if (std::is_signed<value_t>())
            {
                if (defaultValue > std::numeric_limits<value_t>::max())
                    report(true);
            }
            else if (!std::is_same<value_t, bool>()) // ignore boolean
            {
                if (defaultValue < 0 || defaultValue > std::numeric_limits<value_t>::max())
                    report(false);
            }
        }
        else if (std::is_floating_point<value_t>() == std::is_floating_point<default_t>())
        {
            if (defaultValue < -std::numeric_limits<value_t>::max())
                report(true);
            else if (defaultValue > std::numeric_limits<value_t>::max())
                report(true);
        }
        else
        {
            report(false);
        }

        return value_t(defaultValue);
    }

    /**
     * Access a configuration property value, supporting legacy configurations that use element
     * content and newer configurations relying on attributes.
     *
     * Preferring new configurations instead of legacy, consider a request for an attribute to be
     * just that and a request for element content to be a request for either an attribute or an
     * element. A request for "A/@B" will be viewed as only a request for attribute B in element A,
     * while a request for "A/B" will be viewed first as a request for "A/@B" and as a request for
     * "A/B" only if "A/@B" is not found.
     */
    inline const char* queryConfigValue(const IPTree& node, const char* xpath)
    {
        const char* value = nullptr;
        if (!isEmptyString(xpath))
        {
            const char* delim = strrchr(xpath, '/');
            size_t attrIndex = (delim ? delim - xpath + 1 : 0);
            if (xpath[attrIndex] != '@')
            {
                StringBuffer altXPath(xpath);
                altXPath.insert(attrIndex, '@');
                value = node.queryProp(altXPath);
                if (value)
                    return value;
            }
            value = node.queryProp(xpath);
        }
        return value;
    }

    inline const char* queryConfigValue(const IPTree* node, const char* xpath)
    {
        if (node)
            return queryConfigValue(*node, xpath);
        return nullptr;
    }

    inline bool getConfigValue(const IPTree& node, const char* xpath, StringBuffer& value)
    {
        const char* raw = queryConfigValue(node, value);
        if (raw)
        {
            value.set(raw);
            return true;
        }
        return false;
    }

    inline bool getConfigValue(const IPTree* node, const char* xpath, StringBuffer& value)
    {
        if (node)
            return getConfigValue(*node, xpath, value);
        return false;
    }

    template <typename value_t, typename default_t>
    value_t getConfigValue(const IPTree& node, const char* xpath, const default_t& defaultValue)
    {
        const char* raw = queryConfigValue(node, xpath);
        if (raw)
        {
            static TokenDeserializer deserializer;
            value_t value;
            if (deserializer(raw, value) == Deserialization_SUCCESS)
                return value;
        }
        return applyDefault<value_t>(defaultValue, xpath);
    }

    template <typename value_t>
    value_t getConfigValue(const IPTree& node, const char* xpath)
    {
        value_t defaultValue = value_t(0);
        return getConfigValue<value_t>(node, xpath, defaultValue);
    }

    template <typename value_t, typename default_t>
    value_t getConfigValue(const IPTree* node, const char* xpath, const default_t& defaultValue)
    {
        if (node)
            return getConfigValue<value_t>(*node, xpath, defaultValue);
        return applyDefault<value_t>(defaultValue, xpath);
    }

    template <typename value_t>
    value_t getConfigValue(const IPTree* node, const char* xpath)
    {
        if (node)
            return getConfigValue<value_t>(*node, xpath);
        return value_t(0);
    }

} // namespace LogConfigPTree

#endif // _LOGCONFIGPTREE_HPP_