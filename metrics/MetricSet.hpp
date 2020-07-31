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
#include "IMetricSet.hpp"
#include "Metrics.hpp"


namespace hpccMetrics
{

class MetricSet : public IMetricSet
{
    public:
        MetricSet(std::string _name, std::string _prefix, const std::vector<std::shared_ptr<IMetric>> &_metrics) :
            name{std::move(_name)},
            reportNamePrefix{std::move(_prefix)}
        {
            setMetrics(_metrics);
        }

        virtual ~MetricSet() = default;

        std::string getName() const override
        {
            return name;
        }

        std::vector<std::shared_ptr<const IMetric>> getMetrics() override
        {
            std::vector<std::shared_ptr<const IMetric>> returnMetrics;
            for (const auto& metricIt : metrics)
            {
                returnMetrics.emplace_back(metricIt.second);
            }
            return returnMetrics;
        }

        void init() override
        {
            for (const auto& metricIt : metrics)
            {
                metricIt.second->init();
            }
        }

        void collect(MeasurementVector &values) override
        {
            for (const auto &metricIt : metrics)
            {
                metricIt.second->collect(values);
            }
        }

    protected:
        void setMetrics(const std::vector<std::shared_ptr<IMetric>> &_metrics)
        {
            for (auto const &pMetric : _metrics)
            {
                //
                // Make sure the metric has a type
                if (pMetric->getType() == MetricType::NONE)
                {
                    throw std::exception();
                }

                //
                // A metric may only be added to one metric set
                if (!pMetric->isInMetricSet())
                {
                    //
                    // Set the metric name used for reporting by prefixing the metric name
                    pMetric->setReportingName(reportNamePrefix + pMetric->getName());

                    //
                    // Insert the metric, but ensure the name is unique
                    auto rc = metrics.insert({pMetric->getName(), pMetric});
                    if (!rc.second)
                    {
                        throw std::exception();   // metric already added with the same name
                    }
                    pMetric->setInMetricSet(true);
                }
                else
                {
                    throw std::exception();  // not sure if this is the right thing to do yet
                }
            }
        }

    protected:
        std::map<std::string, std::shared_ptr<IMetric>> metrics;
        std::string name;
        std::string reportNamePrefix;
};
}
