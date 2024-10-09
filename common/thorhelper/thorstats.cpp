/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

#include "platform.h"
#include "jstats.h"
#include "thorstats.hpp"
#include "jdebug.hpp"

// Available sets of statistics for a nested section
const StatisticsMapping defaultNestedSectionStatistics({StCycleLocalExecuteCycles, StTimeLocalExecute, StNumStarts, StNumStops});
const StatisticsMapping genericCacheNestedSectionStatistics({StNumCacheAdds, StNumCacheHits, StNumPeakCacheObjects}, defaultNestedSectionStatistics);

ThorSectionTimer::ThorSectionTimer(const char * _name, CRuntimeStatisticCollection & _stats)
: name(_name), stats(_stats)
{
}

ThorSectionTimer * ThorSectionTimer::createTimer(CRuntimeStatisticCollection & stats, const char * name, const StatisticsMapping & nestedSectionStatistics)
{
    StatsScopeId scope(SSTfunction, name);
    CRuntimeStatisticCollection & nested = stats.registerNested(scope, nestedSectionStatistics);
    return new ThorSectionTimer(name, nested);
}

ThorSectionTimer * ThorSectionTimer::createTimer(CRuntimeStatisticCollection & stats, const char * name, ThorStatOption statOption)
{
    assertex(statOption < ThorStatMax);
    switch (statOption)
    {
        case ThorStatGenericCache:
            return createTimer(stats, name, genericCacheNestedSectionStatistics);
        default:
            return createTimer(stats, name, defaultNestedSectionStatistics);
    }
}

ThorSectionTimer * ThorSectionTimer::createTimer(CRuntimeStatisticCollection & stats, const char * name)
{
    return createTimer(stats, name, defaultNestedSectionStatistics);
}

unsigned __int64 ThorSectionTimer::getStartCycles()
{
    stats.queryStatistic(StNumStarts).addAtomic(1);
    return get_cycles_now();
}

void ThorSectionTimer::noteSectionTime(unsigned __int64 startCycles)
{
    cycle_t delay = get_cycles_now() - startCycles;
    stats.addStatisticAtomic(StCycleLocalExecuteCycles, delay);
    stats.addStatisticAtomic(StNumStops, 1);
}

void ThorSectionTimer::addStatistic(__int64 kind, unsigned __int64 value)
{
    stats.addStatisticAtomic(static_cast<StatisticKind>(kind), value);
}

void ThorSectionTimer::setStatistic(__int64 kind, unsigned __int64 value)
{
    stats.setStatistic(static_cast<StatisticKind>(kind), value);
}

void ThorSectionTimer::mergeStatistic(__int64 kind, unsigned __int64 value)
{
    stats.mergeStatistic(static_cast<StatisticKind>(kind), value);
}
