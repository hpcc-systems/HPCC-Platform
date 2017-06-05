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


//Cycles are accumulated locally, time is updated once it is serialized or persisted
const StatisticsMapping nestedSectionStatistics(StCycleLocalExecuteCycles, StTimeLocalExecute, StNumStarts, StNumStops, StKindNone);

ThorSectionTimer::ThorSectionTimer(const char * _name, CRuntimeStatistic & _starts, CRuntimeStatistic & _stops, CRuntimeStatistic & _elapsed)
: starts(_starts), stops(_stops), elapsed(_elapsed), name(_name)
{
}

ThorSectionTimer * ThorSectionTimer::createTimer(CRuntimeStatisticCollection & stats, const char * name)
{
    StatsScopeId scope(SSTfunction, name);
    CRuntimeStatisticCollection & nested = stats.registerNested(scope, nestedSectionStatistics);
    CRuntimeStatistic & starts = nested.queryStatistic(StNumStarts);
    CRuntimeStatistic & stops = nested.queryStatistic(StNumStops);
    CRuntimeStatistic & elapsed = nested.queryStatistic(StCycleLocalExecuteCycles);
    return new ThorSectionTimer(name, starts, stops, elapsed);
}

unsigned __int64 ThorSectionTimer::getStartCycles()
{
    starts.addAtomic(1);
    return get_cycles_now();
}

void ThorSectionTimer::noteSectionTime(unsigned __int64 startCycles)
{
    cycle_t delay = get_cycles_now() - startCycles;
    elapsed.addAtomic(delay);
    stops.addAtomic(1);
}
