/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef ANACOMMON_HPP
#define ANACOMMON_HPP

#include "jliball.hpp"

#ifdef WUANALYSIS_EXPORTS
    #define WUANALYSIS_API DECL_EXPORT
#else
    #define WUANALYSIS_API DECL_IMPORT
#endif

class PerformanceIssue : public CInterface
{
public:
    int compareCost(const PerformanceIssue & other) const;

    void print() const;
    void set(stat_type _cost, const char * msg, ...) __attribute__((format(printf, 3, 4)));
    void setScope(const char *_scope) { scope.set(scope); };
    stat_type getCost() const          { return cost; }

private:
    StringAttr scope;
    stat_type cost = 0;      // number of nanoseconds lost as a result.
    StringBuffer comment;
};

extern int compareIssuesCostOrder(CInterface * const * _l, CInterface * const * _r);

#endif
