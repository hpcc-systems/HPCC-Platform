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

using namespace hpccMetrics;

extern "C" IMetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    std::string sinkName(name);
    IMetricSink *pSink = new FileMetricSink(name, pSettingsTree);
    return pSink;
}


FileMetricSink::FileMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    MetricSink(name, "file")
{
    pSettingsTree->getProp("filename", fileName);
    bool clearFile = pSettingsTree->getPropBool("clear", false);

    //
    // Clear the file if indicated.
    if (clearFile)
    {
        auto handle = fopen(fileName.str(), "w");
        fclose(handle);
    }
}


void FileMetricSink::handle(const MeasurementVector &values, const std::shared_ptr<IMetricSet> &pMetricSet, MetricsReportContext *pContext)
{
    auto handle = fopen(fileName.str(), "a");
    for (const auto& pValue : values)
    {
        fprintf(handle, "%s -> %s\n", pValue->getReportName().c_str(), pValue->valueToString().c_str());
    }
    fprintf(handle, "\n");
    fclose(handle);
}
