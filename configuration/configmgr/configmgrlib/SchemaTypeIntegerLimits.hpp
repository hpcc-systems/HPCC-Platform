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

#ifndef _CONFIG2_CFGINTEGERLIMITS_HPP_
#define _CONFIG2_CFGINTEGERLIMITS_HPP_

#include "SchemaTypeLimits.hpp"
#include <limits.h>

class SchemaTypeIntegerLimits : public SchemaTypeLimits
{
    public:

        SchemaTypeIntegerLimits() : m_min(INT_MIN), m_max(INT_MAX), m_minSet(false), m_maxSet(false), m_minExclusiveTest(false), m_maxExclusiveTest(false) { }
        virtual ~SchemaTypeIntegerLimits() { };
        void setMinInclusive(int v) { m_minSet = true; m_min = v; }
        void setMinExclusive(int v) { m_minSet = true; m_min = v;  m_minExclusiveTest = true; }
        void setMaxInclusive(int v) { m_maxSet = true; m_max = v; }
        void setMaxExclusive(int v) { m_maxSet = true; m_max = v;  m_maxExclusiveTest = true; }
        std::string getLimitString() const;
        virtual bool isMaxSet() const { return m_maxSet; }
        virtual bool isMinSet() const { return m_minSet; }
        virtual int getMax() const { return (m_maxExclusiveTest ? (m_max-1) : m_max); }
        virtual int getMin() const { return (m_minExclusiveTest ? (m_min+1) : m_min); }


    protected:

        virtual bool doValueTest(const std::string &testValue) const;


    protected:

        bool m_maxExclusiveTest;
        bool m_minExclusiveTest;
        bool m_minSet;
        bool m_maxSet;
        int m_min;
        int m_max;
};

#endif // _CONFIG2_CFGINTEGERLIMITS_HPP_
