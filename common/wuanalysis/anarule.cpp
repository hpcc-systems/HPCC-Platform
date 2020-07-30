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

    virtual bool check(PerformanceIssue & result, IWuActivity & activity, const IAnalyserOptions & options) override
    {
        IWuEdge * outputEdge = activity.queryOutput(0);
        if (!outputEdge)
            return false;
        stat_type rowsAvg = outputEdge->getStatRaw(StNumRowsProcessed, StAvgX);
        if (rowsAvg < options.queryOption(watOptMinRowsPerNode))
            return false;
        stat_type rowsMaxSkew = outputEdge->getStatRaw(StNumRowsProcessed, StSkewMax);
        if (rowsMaxSkew > options.queryOption(watOptSkewThreshold))
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
                result.set(ANA_DISTRIB_SKEW_INPUT_ID, cost, "DISTRIBUTE output skew is worse than input skew");
            else
                result.set(ANA_DISTRIB_SKEW_OUTPUT_ID, cost, "Significant skew in DISTRIBUTE output");
            updateInformation(result, activity);
            return true;
        }
        return false;
    }
};

class IoSkewRule : public AActivityRule
{
public:
    IoSkewRule(StatisticKind _stat, const char * _category) : stat(_stat), category(_category)
    {
        assertex((stat==StTimeDiskReadIO)||(stat==StTimeDiskWriteIO)||(stat==StTimeSpillElapsed));
    }

    virtual bool isCandidate(IWuActivity & activity) const override
    {
        if (stat == StTimeDiskReadIO)
        {
            switch(activity.getAttr(WaKind))
            {
                case TAKdiskread:
                case TAKspillread:
                case TAKdisknormalize:
                case TAKdiskaggregate:
                case TAKdiskcount:
                case TAKdiskgroupaggregate:
                case TAKindexread:
                case TAKindexnormalize:
                case TAKindexaggregate:
                case TAKindexcount:
                case TAKindexgroupaggregate:
                case TAKcsvread:
                    return true;
            }
        }
        else if (stat == StTimeDiskWriteIO)
        {
            switch(activity.getAttr(WaKind))
            {
                case TAKdiskwrite:
                case TAKspillwrite:
                case TAKindexwrite:
                case TAKcsvwrite:
                    return true;
            }
        }
        else if (stat == StTimeSpillElapsed)
        {
            switch(activity.getAttr(WaKind))
            {
                case TAKspillread:
                case TAKspillwrite:
                    return true;
            }
        }

        return false;
    }

    virtual bool check(PerformanceIssue & result, IWuActivity & activity, const IAnalyserOptions & options) override
    {
        stat_type ioAvg = activity.getStatRaw(stat, StAvgX);
        stat_type ioMaxSkew = activity.getStatRaw(stat, StSkewMax);
        if (ioMaxSkew > options.queryOption(watOptSkewThreshold))
        {
            stat_type timeMaxLocalExecute = activity.getStatRaw(StTimeLocalExecute, StMaxX);
            stat_type timeAvgLocalExecute = activity.getStatRaw(StTimeLocalExecute, StAvgX);

            stat_type cost;
            //If one node didn't spill then it is possible the skew caused all the lost time
            unsigned actkind = activity.getAttr(WaKind);
            if ((actkind==TAKspillread||actkind==TAKspillwrite) && activity.getStatRaw(stat, StMinX) == 0)
                cost = timeMaxLocalExecute;
            else
                cost = (timeMaxLocalExecute - timeAvgLocalExecute);

            result.set(ANA_IOSKEW_CHILDRECORDS_ID, cost, "Significant skew in records causes uneven %s time", category);
            updateInformation(result, activity);
            return true;
        }
        return false;
    }

protected:
    StatisticKind stat;
    const char * category;
};

class KeyedJoinExcessRejectedRowsRule : public ActivityKindRule
{
public:
    KeyedJoinExcessRejectedRowsRule() : ActivityKindRule(TAKkeyedjoin) {}

    virtual bool check(PerformanceIssue & result, IWuActivity & activity, const IAnalyserOptions & options) override
    {
        stat_type preFiltered = activity.getStatRaw(StNumPreFiltered);
        if (preFiltered)
        {
            IWuEdge * inputEdge = activity.queryInput(0);
            stat_type rowscnt = inputEdge->getStatRaw(StNumRowsProcessed);
            if (rowscnt)
            {
                stat_type preFilteredPer = statPercent( (double) preFiltered * 100.0 / rowscnt );
                if (preFilteredPer > options.queryOption(watPreFilteredKJThreshold))
                {
                    IWuActivity * inputActivity = inputEdge->querySource();
                    // Use input activity as the basis of cost because the rows generated from input activity is being filtered out
                    stat_type timeAvgLocalExecute = inputActivity->getStatRaw(StTimeLocalExecute, StAvgX);
                    stat_type cost = statPercentageOf(timeAvgLocalExecute, preFilteredPer);
                    result.set(ANA_KJ_EXCESS_PREFILTER_ID, cost, "Large number of rows from left dataset rejected in keyed join");
                    updateInformation(result, activity);
                    return true;
                }
            }
        }
        return false;
    }
};

void gatherRules(CIArrayOf<AActivityRule> & rules)
{
    rules.append(*new DistributeSkewRule);
    rules.append(*new IoSkewRule(StTimeDiskReadIO, "disk read"));
    rules.append(*new IoSkewRule(StTimeDiskWriteIO, "disk write"));
    rules.append(*new IoSkewRule(StTimeSpillElapsed, "spill"));
    rules.append(*new KeyedJoinExcessRejectedRowsRule);
}
