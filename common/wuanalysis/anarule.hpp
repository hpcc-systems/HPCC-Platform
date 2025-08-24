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

class CActivityRule : public CInterface
{
public:
    virtual bool isCandidate(IWuActivity & activity) const = 0;
    virtual bool check(PerformanceIssue & results, IWuActivity & activity, const IAnalyserOptions & options) = 0;
    virtual void updateInformation(PerformanceIssue & result,  IWuActivity & activity)
    {
        StringBuffer def;
        activity.getAttr(def,WaDefinition);
        result.setLocation(def);
    }
};

class CSubgraphRule : public CInterface
{
public:
    virtual bool check(PerformanceIssue & results, IWuSubGraph & subgraph, const IAnalyserOptions & options, const IPropertyTree & envInfo) = 0;
};

void gatherRules(CIArrayOf<CActivityRule> & rules);
void gatherRules(CIArrayOf<CSubgraphRule> & rules);

#endif
