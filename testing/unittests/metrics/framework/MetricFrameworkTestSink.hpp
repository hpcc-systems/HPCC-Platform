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
using namespace hpccMetrics;

class MetricFrameworkTestSink : public MetricSink
{
    public:
        explicit MetricFrameworkTestSink(const char *name) :
            MetricSink(name, "test") { }

        ~MetricFrameworkTestSink() = default;

        void startCollection(MetricsReporter *_pReporter)
        {
            pReporter = _pReporter;
            startCollectionCalled = true;
            sinkIsCollecting = true;
        }

        bool isStartCollectionCalled() const { return startCollectionCalled; }
        bool isStopCollectionCalled() const { return stopCollectionCalled; }
        bool isCollecting() const { return sinkIsCollecting; }

        void stopCollection()
        {
            stopCollectionCalled = true;
            sinkIsCollecting = false;
        }

        std::vector<std::shared_ptr<IMetric>> getReportMetrics() const
        {
            return pReporter->queryMetricsForReport(name);
        }

        void reset()
        {
            startCollectionCalled = sinkIsCollecting = stopCollectionCalled = false;
        }

    protected:

        bool startCollectionCalled = false;
        bool sinkIsCollecting = false;
        bool stopCollectionCalled = false;
};
