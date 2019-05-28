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

#ifndef ANARULE_HPP
#define ANARULE_HPP

#include "anacommon.hpp"
#include "anawu.hpp"
#include "jstatcodes.h"
#include "wuattr.hpp"

struct WuAnalyseOptions
{
    stat_type minInterestingTime = msecs2StatUnits(10);// ignore anything under 10 millisecond
    stat_type minCost = seconds2StatUnits(1);          // only interested in costs of > 1s
    stat_type skewThreshold = statSkewPercent(20);     // minimum interesting skew measurment
};

class AActivityRule : public CInterface
{
public:
    virtual bool isCandidate(IWuActivity & activity) const = 0;
    virtual bool check(PerformanceIssue & results, IWuActivity & activity, const WuAnalyseOptions & options) = 0;
};

void gatherRules(CIArrayOf<AActivityRule> & rules);

#endif
