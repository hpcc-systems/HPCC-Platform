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

    fprintf(fhandle, "  %s -> %" I64F "d, %s\n", name.c_str(), pMetric->queryValue(), pMetric->queryDescription().c_str());
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
