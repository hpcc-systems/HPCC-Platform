/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#ifndef JSTATS_H
#define JSTATS_H

#include "jlib.hpp"

enum StatisticCombineType
{
    STATSMODE_COMBINE_SUM,
    STATSMODE_COMBINE_MAX,
    STATSMODE_COMBINE_MIN
};

enum StatisticType
{
    STATS_INDEX_SEEKS,
    STATS_INDEX_SCANS,
    STATS_INDEX_WILDSEEKS,
    STATS_INDEX_SKIPS,
    STATS_INDEX_NULLSKIPS,
    STATS_INDEX_MERGES,

    STATS_BLOBCACHEHIT,
    STATS_LEAFCACHEHIT,
    STATS_NODECACHEHIT,
    STATS_BLOBCACHEADD,
    STATS_LEAFCACHEADD,
    STATS_NODECACHEADD,

    STATS_INDEX_MERGECOMPARES,

    STATS_PRELOADCACHEHIT,
    STATS_PRELOADCACHEADD,

    STATS_SERVERCACHEHIT,

    STATS_ACCEPTED,
    STATS_REJECTED,
    STATS_ATMOST,

    STATS_DISK_SEEKS,

    STATS_SIZE
};

enum StatisticMeasure
{
    SMEASURE_TIME_NS,
    SMEASURE_COUNT,
    SMEASURE_MEM_KB,
    SMEASURE_TIMESTAMP,
    SMEASURE_SIZE
};

extern jlib_decl const char *getStatName(unsigned i);
extern jlib_decl const char *getStatShortName(unsigned i);
extern jlib_decl StatisticMeasure getStatMeasure(unsigned i);
extern jlib_decl StatisticCombineType getStatCombineMode(unsigned i);
inline unsigned __int64 milliToNano(unsigned __int64 value) { return value * 1000000; } // call avoids need to upcast values
inline unsigned __int64 nanoToMilli(unsigned __int64 value) { return value / 1000000; }

//This interface is primarily here to reduce the dependency between the different components.
interface IStatisticTarget
{
    virtual void addStatistic(const char * creator_who, const char * wuScope_where, const char * stat_what, const char * description, StatisticMeasure kind, unsigned __int64 value, unsigned __int64 count, unsigned __int64 maxValue, bool merge) = 0;
};

#endif
