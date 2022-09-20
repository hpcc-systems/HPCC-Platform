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


using namespace hpccMetrics;

#ifdef LOGSINK_EXPORTS
#define LOGSINK_API DECL_EXPORT
#else
#define LOGSINK_API DECL_IMPORT
#endif

class LOGSINK_API LogMetricSink : public PeriodicMetricSink
{
public:
    explicit LogMetricSink(const char *name, const IPropertyTree *pSettingsTree);
    ~LogMetricSink() override = default;

protected:
    virtual void prepareToStartCollecting() override {}
    virtual void collectingHasStopped() override {}
    virtual void doCollection() override;
    void writeLogEntry(const std::shared_ptr<IMetric> &pMetric);
    void writeHistogramLogEntry(const std::shared_ptr<IMetric> &pMetric);
};
