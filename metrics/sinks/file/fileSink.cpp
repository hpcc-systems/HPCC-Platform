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
    std::string sinkName(name);
    MetricSink *pSink = new FileMetricSink(name, pSettingsTree);
    return pSink;
}


FileMetricSink::FileMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
        MetricSink(name, "file"),
        uPeriodSeconds(60),
        periodSeconds{std::chrono::seconds(60)}
{
    if (pSettingsTree->hasProp("@period"))
    {
        unsigned seconds = pSettingsTree->getPropInt("@period");
        uPeriodSeconds = seconds;
        periodSeconds = std::chrono::seconds(seconds);
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
    // Don't like this extra "wait", but some time needs to pass before the first collection
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, periodSeconds);
    }

    //
    // Continue reporting until told to stop
    while (!stopCollectionFlag)
    {
        auto reportMetrics = pReporter->queryMetricsForReport(name);
        MeasurementVector measurements;
        for (auto &pMetric: reportMetrics)
        {
            pMetric->collect(measurements);
        }
        writeMeasurementsToFile(measurements);

        //
        // Wait for the indicated period, but allow interruption by using a condition variable
        {
            std::unique_lock<std::mutex> lock(m);
            cv.wait_for(lock, periodSeconds);
        }
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
    // Set the stop collecting flag, then signal the condition
    // variable to wakeup the sleeping collection thread
    {
        std::lock_guard<std::mutex> guard(m);
        stopCollectionFlag = true;
    }
    cv.notify_all();

    isCollecting = false;
    collectThread.join();
    fclose(fhandle);
}


void FileMetricSink::writeMeasurementsToFile(const MeasurementVector &values) const
{
    auto timenow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string timeStr(ctime(&timenow));
    timeStr.pop_back();
    fprintf(fhandle, "------------ Metric Report [%s] ------------\n", timeStr.c_str());
    for (const auto& pValue : values)
    {
        fprintf(fhandle, "%s -> %s, %s\n", pValue->queryName().c_str(), pValue->valueToString().c_str(), pValue->queryDescription().c_str());
    }
    fprintf(fhandle, "\n");
    fflush(fhandle);
}
