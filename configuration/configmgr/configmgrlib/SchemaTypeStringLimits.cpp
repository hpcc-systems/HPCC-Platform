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

#include "SchemaTypeStringLimits.hpp"
#include "EnvironmentValue.hpp"
#include <regex>

#if __cplusplus >= 201103L &&                             \
    (!defined(__GLIBCXX__) || (__cplusplus >= 201402L) || \
        (defined(_GLIBCXX_REGEX_DFS_QUANTIFIERS_LIMIT) || \
         defined(_GLIBCXX_REGEX_STATE_LIMIT)           || \
             (defined(_GLIBCXX_RELEASE)                && \
             _GLIBCXX_RELEASE > 4)))
#define HAVE_WORKING_REGEX 1
#else
#define HAVE_WORKING_REGEX 0
#endif


std::string SchemaTypeStringLimits::getLimitString() const
{
    return "";
}


bool SchemaTypeStringLimits::doValueTest(const std::string &testValue) const
{
    bool isValid;
    size_t len = testValue.length();
    isValid = len >= m_minLength && len <= m_maxLength;

    // test patterns
#if HAVE_WORKING_REGEX
    for (auto pattern = m_patterns.begin(); isValid && pattern != m_patterns.end(); ++pattern)
    {
        std::regex r(pattern->c_str());
        isValid = std::regex_match(testValue.c_str(), r);
    }
#endif
    return isValid;
}
