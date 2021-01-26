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
#include <thread>

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    MetricSink *pSink = new FileMetricSink(name, pSettingsTree);
    return pSink;
}


FileMetricSink::FileMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
        MetricSink(name, "file"),
        collectionPeriodSeconds{60}
{
    if (pSettingsTree->hasProp("@period"))
    {
        collectionPeriodSeconds = pSettingsTree->getPropInt("@period");
    }

    pSettingsTree->getProp("@filename", fileName);
    clearFileOnStartCollecting = pSettingsTree->getPropBool("@clear", false);
}


FileMetricSink::~FileMetricSink()
{
    if (isCollecting)
    {
        doStopCollecting();
    }
}


void FileMetricSink::startCollection(MetricsReporter *_pReporter)
{
    fhandle = fopen(fileName.str(), clearFileOnStartCollecting ? "w" : "a");
    pReporter = _pReporter;
    isCollecting = true;
    collectThread = std::thread(&FileMetricSink::collectionThread, this);
}


void FileMetricSink::collectionThread()
{
    //
    // The initial wait for the first report
    waitSem.wait(collectionPeriodSeconds * 1000);
    while (!stopCollectionFlag)
    {
        auto reportMetrics = pReporter->queryMetricsForReport(name);
        writeReportHeaderToFile();
        for (auto &pMetric: reportMetrics)
        {
            writeMeasurementToFile(pMetric->queryName(), pMetric->queryValue(), pMetric->queryDescription());
        }

        // Wait again
        waitSem.wait(collectionPeriodSeconds * 1000);
    }
}


void FileMetricSink::stopCollection()
{
    if (isCollecting)
    {
        doStopCollecting();
    }
}


void FileMetricSink::doStopCollecting()
{
    //
    // Set the stop collecting flag, then signal the wait semaphore
    // to wake up and stop the collection thread
    stopCollectionFlag = true;
    waitSem.signal();
    isCollecting = false;
    collectThread.join();
    fclose(fhandle);
}


void FileMetricSink::writeMeasurementToFile(const std::string &metricName, uint32_t value, const std::string &metricDescription) const
{
    fprintf(fhandle, "  %s -> %d, %s\n", metricName.c_str(), value, metricDescription.c_str());
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
