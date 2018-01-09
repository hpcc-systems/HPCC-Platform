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

#ifndef _CONFIG2_CFGSTRINGLIMITS_HPP_
#define _CONFIG2_CFGSTRINGLIMITS_HPP_

#include "SchemaTypeLimits.hpp"
#include <limits.h>


class SchemaTypeStringLimits : public SchemaTypeLimits
{
    public:

        SchemaTypeStringLimits() : m_minLength(0), m_maxLength(INT_MAX) { }
        virtual ~SchemaTypeStringLimits() { };
        void setLength(unsigned v) { m_length = v; }
        void setMinLength(int v) { m_minLength = v; }
        void setMaxLength(int v) { m_maxLength = v; }
        void addPattern(const std::string &pattern) { m_patterns.push_back(pattern); }
        std::string getLimitString() const;


    protected:

        virtual bool doValueTest(const std::string &testValue) const;


    protected:

        unsigned m_length;
        int m_minLength;
        int m_maxLength;
        std::vector<std::string> m_patterns;

};




#endif // _CONFIG2_CFGSTRINGLIMITS_HPP_
