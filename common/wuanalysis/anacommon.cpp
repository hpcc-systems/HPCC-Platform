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
#include "anacommon.hpp"

int compareIssuesCostOrder(CInterface * const * _l, CInterface * const * _r)
{
    const PerformanceIssue * l = static_cast<const PerformanceIssue *>(*_l);
    const PerformanceIssue * r = static_cast<const PerformanceIssue *>(*_r);
    return l->compareCost(*r);
}

int PerformanceIssue::compareCost(const PerformanceIssue & other) const
{
    if (cost == other.cost)
        return 0;
    else
        return cost > other.cost ? -1 : +1;
}

void PerformanceIssue::print() const
{
    printf("[%" I64F "dms] %s: %s\n", statUnits2msecs(cost), scope.str(), comment.str());
}

void PerformanceIssue::set(stat_type _cost, const char * msg, ...)
{
    cost = _cost;
    va_list args;
    va_start(args, msg);
    comment.valist_appendf(msg, args);
    va_end(args);
}

