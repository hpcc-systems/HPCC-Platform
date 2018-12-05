/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "Operation.hpp"
#include "TemplateException.hpp"


void Operation::addAttribute(modAttribute &newAttribute)
{
    m_attributes.emplace_back(newAttribute);
}


bool Operation::execute(EnvironmentMgr *pEnvMgr, Inputs *pInputs)
{
    bool rc = true;
    size_t count, startIndex;

    pInputs->setInputIndex(0);

    //
    // Determine the count of iterations to do
    std::string countStr = pInputs->doValueSubstitution(m_count);
    try {
        count = std::stoul(countStr);
    } catch (...) {
        throw TemplateException("Non-numeric count found for count", false);
    }

    //
    // Determine the starting index
    std::string startIdxStr = pInputs->doValueSubstitution(m_startIndex);
    try {
        startIndex = std::stoul(startIdxStr);
    } catch (...) {
        throw TemplateException("Non-numeric count found for start index", false);
    }

    //
    // Now execute the operation count times
    for (size_t idx=0; idx < count; ++idx)
    {
        pInputs->setInputIndex(startIndex + idx);
        assignAttributeCookedValues(pInputs);
        doExecute(pEnvMgr, pInputs);
    }
    return rc;
}


void Operation::assignAttributeCookedValues(Inputs *pInputs)
{
    //
    // go through the ones defined by the operation and set each (by name)
    for (auto &attr: m_attributes)
    {
        attr.cookedValue = pInputs->doValueSubstitution(attr.value);
    }
}
