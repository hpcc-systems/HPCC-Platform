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


#pragma once

#include "Metrics.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <thread>

using namespace hpccMetrics;

class METRICS_API FileMetricSink : public MetricSink
{
    public:

        explicit FileMetricSink(const char *name, const IPropertyTree *pSettingsTree);
        void reportMeasurements(const MeasurementVector &values) override;
        void startCollection(MetricsReporter *pReporter) override;
        void stopCollection() override;

    protected:
        void collectionThread() const;

    protected:

        StringBuffer fileName;
        std::chrono::seconds periodSeconds;
        std::thread collectThread;
        bool stopCollectionFlag = false;
};
