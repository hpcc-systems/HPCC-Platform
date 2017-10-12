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

#ifndef _CONFIG2_CFGLIMITS_HPP_
#define _CONFIG2_CFGLIMITS_HPP_

#include <memory>
#include <vector>
#include <string>
#include <limits.h>

struct AllowedValue 
{
    AllowedValue(const std::string &value, const std::string &desc="") : m_value(value), m_description(desc) { }
    std::string m_value;
    std::string m_description;
};

class CfgLimits 
{
    public:

        CfgLimits() :
            m_minInclusive(INT_MIN),
            m_maxInclusive(INT_MAX),
            m_minExclusive(INT_MIN),
            m_maxExclusive(INT_MAX),
            m_length(0) { }
        virtual ~CfgLimits() { }
        void setMinInclusive(int v)  { m_minInclusive = v; } 
        void setMinExclusive(int v)  { m_minExclusive = v; } 
        void setMaxInclusive(int v)  { m_maxInclusive = v; } 
        void setMaxExclusive(int v)  { m_maxExclusive = v; } 
        void setLength(int v)        { m_length       = v; }
        void setMinLength(int v)     { m_minLength    = v; } 
        void setMaxLength(int v)     { m_maxLength    = v; }
        void addPattern(const std::string &pattern) { m_patterns.push_back(pattern); }
        void addAllowedValue(const std::string &value, const std::string &desc="") { m_allowedValues.push_back(AllowedValue(value, desc)); }
        bool isValueValid(const std::string &testValue) { return true; }
		virtual int getMin() { return m_minInclusive; }
        virtual int getMax() { return m_maxInclusive; }


    protected:

        int m_minInclusive;
        int m_maxInclusive;
        int m_minExclusive;
        int m_maxExclusive;
        int m_length;
        int m_minLength;
        int m_maxLength;
        std::vector<std::string> m_patterns;
        std::vector<AllowedValue> m_allowedValues;
};



#endif // _CONFIG2_CFGLIMITS_HPP_
