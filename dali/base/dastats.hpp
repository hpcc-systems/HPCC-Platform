/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef DASTATS_HPP
#define DASTATS_HPP

#ifdef DALI_EXPORTS
#define da_decl DECL_EXPORT
#else
#define da_decl DECL_IMPORT
#endif

#include <vector>
#include <initializer_list>
#include <utility>
#include <string_view>

#include "jstats.h"

using MetricsDimensionList = std::vector<std::pair<const char *, const char *>>; // The list of dimensions are likely to be fixed for each call so use a initializer_list

extern da_decl void recordGlobalMetrics(const char * category, const MetricsDimensionList &  dimensions, const CRuntimeStatisticCollection & stats, const StatisticsMapping * optMapping);
extern da_decl void recordGlobalMetrics(const char * category, const MetricsDimensionList &  dimensions, const std::initializer_list<StatisticKind> & stats, const std::initializer_list<stat_type> & values);
extern da_decl void recordGlobalMetrics(const char * category, const MetricsDimensionList & dimensions, const std::vector<StatisticKind> & stats, const std::vector<stat_type> & deltas);

using GlobalStatisticsList = std::vector<std::pair<StatisticKind, stat_type>>;
interface IGlobalMetricRecorder
{
    virtual void processGlobalStatistics(const char * category, const MetricsDimensionList & dimensions, const char * startTime, const char * endTime, const GlobalStatisticsList & stats) = 0;
};

extern da_decl void gatherGlobalMetrics(const char * optCategory, const MetricsDimensionList & optDimensions, const CDateTime & from, const CDateTime & to, IGlobalMetricRecorder & visitor);

//FUTURE: A function for rolling up a range of timeslots into a single timeslot

#ifdef _USE_CPPUNIT
//Used by the unit tests to set the apparent time for the following transactions.
extern da_decl void setGlobalMetricNowTime(const char * time);

//The following might be useful as a general purpose admin function - but only used for unit tests at the moment
extern da_decl void resetGlobalMetrics(const char * optCategory, const MetricsDimensionList & optDimensions);
#endif

extern da_decl void startDaliRecordGlobalMetricPublisher(const char * category, const MetricsDimensionList &  dimensions, const CRuntimeStatisticCollection & stats, unsigned publishPeriodMs);
extern da_decl void stopDaliRecordGlobalMetricPublisher();

#endif
