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

#include "jliball.hpp"

#include "workunit.hpp"
#include "anarule.hpp"
#include "commonext.hpp"

class ActivityKindRule : public AActivityRule
{
public:
    ActivityKindRule(ThorActivityKind _kind) : kind(_kind) {}

    virtual bool isCandidate(IWuActivity & activity) const override
    {
        return (activity.getAttr(WaKind) == kind);
    }

protected:
    ThorActivityKind kind;
};

//--------------------------------------------------------------------------------------------------------------------

class DistributeSkewRule : public ActivityKindRule
{
public:
    DistributeSkewRule() : ActivityKindRule(TAKhashdistribute) {}

    virtual bool check(PerformanceIssue & result, IWuActivity & activity, const WuAnalyseOptions & options) override
    {
        IWuEdge * outputEdge = activity.queryOutput(0);
        if (!outputEdge)
            return false;
        stat_type rowsAvg = outputEdge->getStatRaw(StNumRowsProcessed, StAvgX);
        if (rowsAvg < rowsThreshold)
            return false;
        stat_type rowsMaxSkew = outputEdge->getStatRaw(StNumRowsProcessed, StSkewMax);
        if (rowsMaxSkew > options.skewThreshold)
        {
            // Use downstream activity time to calculate approximate cost
            IWuActivity * targetActivity = outputEdge->queryTarget();
            assertex(targetActivity);
            stat_type timeMaxLocalExecute = targetActivity->getStatRaw(StTimeLocalExecute, StMaxX);
            stat_type timeAvgLocalExecute = targetActivity->getStatRaw(StTimeLocalExecute, StAvgX);
            // Consider ways to improve this cost calculation further
            stat_type cost = timeMaxLocalExecute - timeAvgLocalExecute;

            IWuEdge * inputEdge = activity.queryInput(0);
            if (inputEdge && (inputEdge->getStatRaw(StNumRowsProcessed, StSkewMax) < rowsMaxSkew))
                result.set(cost, "DISTRIBUTE output skew is worse than input skew (%s)", activity.queryName());
            else
                result.set(cost, "Significant skew in DISTRIBUTE output (%s)", activity.queryName());
            return true;
        }
        return false;
    }

protected:
    static const stat_type rowsThreshold = 100;                // avg rows per node.
};

void gatherRules(CIArrayOf<AActivityRule> & rules)
{
    rules.append(*new DistributeSkewRule);
}
