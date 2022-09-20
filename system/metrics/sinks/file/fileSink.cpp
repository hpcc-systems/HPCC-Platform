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

#include "fileSink.hpp"
#include <cstdio>
#include "platform.h"

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    MetricSink *pSink = new FileMetricSink(name, pSettingsTree);
    return pSink;
}


FileMetricSink::FileMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    PeriodicMetricSink(name, "file", pSettingsTree)
{
    pSettingsTree->getProp("@filename", fileName);
    clearFileOnStartCollecting = pSettingsTree->getPropBool("@clear", false);
}


void FileMetricSink::prepareToStartCollecting()
{
    fhandle = fopen(fileName.str(), clearFileOnStartCollecting ? "w" : "a");
}


void FileMetricSink::doCollection()
{
    auto reportMetrics = pManager->queryMetricsForReport(name);
    writeReportHeaderToFile();
    for (auto &pMetric: reportMetrics)
    {
        writeMeasurementToFile(pMetric);
    }
}


void FileMetricSink::collectingHasStopped()
{
    fclose(fhandle);
}


void FileMetricSink::writeMeasurementToFile(const std::shared_ptr<IMetric> &pMetric) const
{
    std::string name = pMetric->queryName();
    const auto & metaData = pMetric->queryMetaData();
    for (auto &metaDataIt: metaData)
    {
        name.append(".").append(metaDataIt.value);
    }

    const char *unitsStr = pManager->queryUnitsString(pMetric->queryUnits());

    if (pMetric->queryMetricType() != METRICS_HISTOGRAM)
    {
        if (!isEmptyString(unitsStr))
        {
            name.append(".").append(unitsStr);
        }
        fprintf(fhandle, "%s -> %" I64F "d, %s\n", name.c_str(), pMetric->queryValue(), pMetric->queryDescription().c_str());
    }
    else
    {
        std::vector<__uint64> values = pMetric->queryHistogramValues();
        std::vector<__uint64> limits = pMetric->queryHistogramBucketLimits();
        size_t countBucketValues = values.size();
        __uint64 cumulative;

        for (int i=0; i < countBucketValues - 1; ++i)
        {
            cumulative += values[i];
            fprintf(fhandle, "name=%s, bucket le %" I64F "d=%" I64F "d\n", name.c_str(), limits[i], cumulative);
        }

        // The inf bucket count is the last element in the array of values returned.
        // Add it to the cumulative count and print the value
        cumulative += values[countBucketValues - 1];
        fprintf(fhandle, "name=%s, bucket inf=%" I64F "d\n", name.c_str(), cumulative);

        // sum - total of all observations
        fprintf(fhandle, "name=%s, sum=%" I64F "d\n", name.c_str(), pMetric->queryValue());

        // count - total of all bucket counts (same as inf)
        fprintf(fhandle, "name=%s, count=%" I64F "d\n", name.c_str(), cumulative);

    }
    fflush(fhandle);
}


void FileMetricSink::writeReportHeaderToFile() const
{
    auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string timeStr(ctime(&timenow));
    timeStr.pop_back();
    fprintf(fhandle, "------------ Metric Report [%s] ------------\n", timeStr.c_str());
    fflush(fhandle);
}
