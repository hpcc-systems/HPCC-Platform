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


#pragma once

#include "jmetrics.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include "jfile.ipp"
#include "jsem.hpp"
#include <thread>
#include <condition_variable>


using namespace hpccMetrics;

#ifdef FILESINK_EXPORTS
#define FILESINK_API DECL_EXPORT
#else
#define FILESINK_API DECL_IMPORT
#endif

class FILESINK_API FileMetricSink : public MetricSink
{
    public:
        explicit FileMetricSink(const char *name, const IPropertyTree *pSettingsTree);
        ~FileMetricSink() override;
        void startCollection(MetricsReporter *pReporter) override;
        void stopCollection() override;

    protected:
        void writeReportHeaderToFile() const;
        void writeMeasurementToFile(const std::string &metricName, uint32_t value, const std::string &metricDescription) const;
        void collectionThread();
        void doStopCollecting();

    protected:
        StringBuffer fileName;
        unsigned collectionPeriodSeconds;
        std::thread collectThread;
        std::atomic<bool> stopCollectionFlag{false};
        bool isCollecting = false;
        bool clearFileOnStartCollecting = false;
        FILE *fhandle = nullptr;
        Semaphore waitSem;
};
