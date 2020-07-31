/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "FileSink.hpp"
#include <cstdio>
#include <thread>

using namespace hpccMetrics;

extern "C" IMetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    std::string sinkName(name);
    IMetricSink *pSink = new FileMetricSink(name, pSettingsTree);
    return pSink;
}


FileMetricSink::FileMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    MetricSink(name, "file"),
    periodSeconds{std::chrono::seconds(60)}
{
    if (pSettingsTree->hasProp("@period"))
    {
        unsigned seconds = pSettingsTree->getPropInt("@period");
        periodSeconds = std::chrono::seconds(seconds);
    }

    pSettingsTree->getProp("@filename", fileName);
    bool clearFile = pSettingsTree->getPropBool("@clear", false);

    //
    // Clear the file if indicated.
    if (clearFile)
    {
        auto handle = fopen(fileName.str(), "w");
        if (handle != nullptr)
        {
            fclose(handle);
        }
    }
}


void FileMetricSink::reportMeasurements(const MeasurementVector &values)
{
    auto fh = fopen(fileName.str(), "a");
    fprintf(fh, "---------- Metric Report ------------\n");
    for (const auto& pValue : values)
    {
        fprintf(fh, "%s -> %s, %s\n", pValue->getName().c_str(), pValue->valueToString().c_str(), pValue->getDescription().c_str());
    }
    fprintf(fh, "\n");
    fclose(fh);
}


void FileMetricSink::startCollection(MetricsReporter *_pReporter)
{
    pReporter = _pReporter;
    collectThread = std::thread(&FileMetricSink::collectionThread, this);
}


void FileMetricSink::collectionThread() const
{
    while (!stopCollectionFlag)
    {
        std::this_thread::sleep_for(periodSeconds);
        pReporter->collectMeasurements(name);
    }
}


void FileMetricSink::stopCollection()
{
    stopCollectionFlag = true;
    collectThread.join();
}
