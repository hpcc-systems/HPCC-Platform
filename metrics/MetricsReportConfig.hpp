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

#include <memory>
#include "IMetricSet.hpp"
#include "IMetricSink.hpp"

namespace hpccMetrics
{

struct MetricsReportConfig
{
    //
    // Adds a metric set for reporting to a sink. (note that pSink will probably become OWNED)
    void addReportConfig(IMetricSink *pSink, const std::shared_ptr<IMetricSet> &set)
    {
        auto reportCfgIt = metricReportConfig.find(pSink);
        if (reportCfgIt == metricReportConfig.end())
        {
            reportCfgIt = metricReportConfig.insert({pSink, std::vector<std::shared_ptr<IMetricSet>>()}).first;
        }
        reportCfgIt->second.emplace_back(set);
        metricSets.insert(set);
    }

    std::unordered_set<std::shared_ptr<IMetricSet>> metricSets;
    std::map<IMetricSink *, std::vector<std::shared_ptr<IMetricSet>>> metricReportConfig;
};

}
