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

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    MetricSink *pSink = new LogMetricSink(name, pSettingsTree);
    return pSink;
}


LogMetricSink::LogMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    PeriodicMetricSink(name, "log", pSettingsTree)
{
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
    std::string name = pMetric->queryName();
    auto metaData = pMetric->queryMetaData();
    for (auto &metaDataIt: metaData)
    {
        name.append(".").append(metaDataIt.value);
    }

    const char *unitsStr = pManager->queryUnitsString(pMetric->queryUnits());
    if (unitsStr)
    {
        name.append(".").append(unitsStr);
    }
    LOG(MCmetrics, "name=%s,value=%" I64F "d", name.c_str(), pMetric->queryValue());
}
