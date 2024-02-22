/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

#include "logSink.hpp"
#include "jlog.hpp"

#include <map>
#include <vector>
#include <string>

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    MetricSink *pSink = new LogMetricSink(name, pSettingsTree);
    return pSink;
}


LogMetricSink::LogMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    PeriodicMetricSink(name, "log", pSettingsTree),
    ignoreZeroMetrics(false)
{
    ignoreZeroMetrics = pSettingsTree->getPropBool("@ignoreZeroMetrics", true);
}


void LogMetricSink::doCollection()
{
    auto reportMetrics = pManager->queryMetricsForReport(name);
    for (auto &pMetric: reportMetrics)
    {
        writeLogEntry(pMetric);
    }
}


void LogMetricSink::writeLogEntry(const std::shared_ptr<IMetric> &pMetric)
{
    const std::string & name = pMetric->queryName();
    __uint64 metricValue = pMetric->queryValue();
    if (ignoreZeroMetrics && (metricValue == 0))
        return;

    const auto & metaData = pMetric->queryMetaData();
    const std::string * searchKey = &name;
    //Create a unique id from a combination of the name and the labels so that we can check whether this metric instance has changed since the last time it was logged
    std::string uid;
    if (!metaData.empty())
    {
        uid.append(name);
        for (auto &metaDataIt: metaData)
            uid.append("/").append(metaDataIt.value.c_str());
        searchKey = &uid;
    }

    auto match = alreadySeen.find(*searchKey);
    const auto notFound = alreadySeen.end();
    //If the values haven't changed then avoid logging an update to the logs
    if ((match != notFound) && (match->second == metricValue))
        return;

    StringBuffer output;

    StringBuffer labels;
    for (auto &metaDataIt: metaData)
    {
        if (labels.length() > 0)
            labels.append(",");
        labels.appendf("{ \"name\":\"%s\", \"value\": \"%s\" }", metaDataIt.key.c_str(), metaDataIt.value.c_str());
    }

    if (labels.length())
        output.appendf("\"labels\": [%s], ", labels.str());

    const char *unitsStr = pManager->queryUnitsString(pMetric->queryUnits());

    if (pMetric->queryMetricType() != METRICS_HISTOGRAM)
    {
        output.appendf("\"value\": %" I64F "d", metricValue);
    }
    else
    {
        output.append("\"sum\": ").append(metricValue).append(", ");

        StringBuffer valueText;
        std::vector<__uint64> values = pMetric->queryHistogramValues();
        __uint64 cumulative = 0;
        for (size_t i=0; i < values.size(); ++i)
        {
            if (valueText.length() > 0)
                valueText.append(", ");
            cumulative += values[i];
            valueText.appendf("%" I64F "d", values[i]);
        }
        output.append("\"count\": ").append(cumulative).append(", ");

        output.append("\"counts\": [").append(valueText).append("]");

        //Only output the limits the first time this metric is traced
        if (match == notFound)
        {
            std::vector<__uint64> limits = pMetric->queryHistogramBucketLimits();
            StringBuffer limitText;
            for (size_t i=0; i < limits.size(); ++i)
            {
                if (limitText.length() > 0)
                    limitText.append(", ");
                limitText.appendf("%" I64F "d", limits[i]);
            }
            output.append(", \"limits\": [").append(limitText).append("]");
        }
    }

    LOG(MCmonitorMetric, "{ \"type\": \"metric\", \"name\": \"%s%s%s\", %s }", name.c_str(), unitsStr ? "." : "", unitsStr ? unitsStr : "",  output.str());

    if (match == notFound)
        alreadySeen.insert({ *searchKey, metricValue});
    else
        match->second = metricValue;
}
