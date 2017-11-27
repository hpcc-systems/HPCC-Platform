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

#include "CfgLimits.hpp"


class CfgStringLimits : public CfgLimits
{
    public:

        CfgStringLimits() : m_removeWhiteSpace(true) { m_minInclusive = 0; }
        virtual ~CfgStringLimits() { };
        void setRemoveWhiteSpace(bool remove) { m_removeWhiteSpace = true; }
        int getMin() const override { return m_minLength; }
        int getMax() const override { return m_maxLength; }
        std::string getString() const override;
        virtual bool isValueValid(const std::string &testValue) const;


    protected:

        bool m_removeWhiteSpace;

};




#endif // _CONFIG2_CFGSTRINGLIMITS_HPP_
