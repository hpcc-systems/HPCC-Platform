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

#include <map>
#include <string>
#include <memory>
#include <utility>

#include "MetricSet.hpp"
#include "MetricSink.hpp"

namespace hpccMetrics
{

//
// Not sure if this class is going to remain part of the framework or not.
// It was supposed to be a convenience class to help a component lookup
// its metric objects more easily, then only one of these objects
// needed to be global to the component.
class MetricsRegistry
{
    public:
        MetricsRegistry() = default;
        void add(const std::shared_ptr<Metric> &pMetric)
        {
            auto it = m_metrics.insert({pMetric->getName(), pMetric});
            if (!it.second)
            {
                throw (std::exception());  // just throw for now, maybe add our own
            }
        }

        template<typename T> std::shared_ptr<T> get(const std::string &metricName);

    private:
        std::map<std::string, std::shared_ptr<Metric>> m_metrics;
};


template<typename T>
std::shared_ptr<T> MetricsRegistry::get(const std::string &metricName)
{
    auto it = m_metrics.find(metricName);
    if (it == m_metrics.end())
    {
        throw (std::exception());  // just throw for now, maybe add our own
    }

    std::shared_ptr<T> pMetric = std::dynamic_pointer_cast<T>(it->second);
    if (!pMetric)
    {
        throw (std::exception());
    }
    return pMetric;
}

}
