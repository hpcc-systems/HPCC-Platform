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
#include <map>
#include "IMetricSet.hpp"
#include "IMetricSink.hpp"
#include "MetricsReportContext.hpp"
#include "MetricsReportConfig.hpp"


namespace hpccMetrics
{

class MetricsReporter
{
    public:
        explicit MetricsReporter(MetricsReportConfig &_reportConfig, IMetricsReportTrigger *_pTrigger) :
            reportConfig{_reportConfig},
            pTrigger{_pTrigger}
            { }

        virtual ~MetricsReporter() = default;

        void start()
        {
            init();
            pTrigger->start();
        }

        void stop()
        {
            pTrigger->stop();
        }

        void init()
        {
            //
            // Tell the trigger who we are
            pTrigger->setReporter(this);

            //
            // Initialization consists of initializing each sink, informing
            // each sink about the metric sets for which it shall report
            // measurements, and initializing each metric set.
            for (auto reportConfigIt : reportConfig.metricReportConfig)
            {
                for (const auto &pMetricSet : reportConfigIt.second)
                {
                    reportConfigIt.first->addMetricSet(pMetricSet);
                }
            }

            //
            // Tell each metric that collection is beginning
            for (const auto& pMetricSet : reportConfig.metricSets)
            {
                pMetricSet->init();
            }
        }

        bool report(std::map<std::string, MetricsReportContext *> &reportContexts)
        {
            //
            // vectors of measurements for each metric set
            std::map<std::shared_ptr<IMetricSet>, MeasurementVector> metricSetReportValues;

            //
            // Collect all the values
            for (auto &pMetricSet : reportConfig.metricSets)
            {
                metricSetReportValues[pMetricSet] = MeasurementVector();
                pMetricSet->collect(metricSetReportValues[pMetricSet]);
            }

            //
            // Send registered metric sets to each sink
            for (const auto &reportConfigIt : reportConfig.metricReportConfig)
            {
                //
                // Obtain the context for the sink. If no specific context has been set, build a default
                // context and use it.
                MetricsReportContext *pReportContext = nullptr;
                auto it = reportContexts.find(reportConfigIt.first->getName());
                if (it == reportContexts.end())
                {
                    it = reportContexts.find("default");
                    if (it == reportContexts.end())
                    {
                        pReportContext = new MetricsReportContext();
                    }
                }
                else
				{
					pReportContext = it->second;
				}

                for (auto const &pMetricSet : reportConfigIt.second)
                {
                    reportConfigIt.first->send(metricSetReportValues[pMetricSet], pMetricSet, pReportContext);
                }
            }
            return true;
        }

    protected:
        MetricsReportConfig reportConfig;
        IMetricsReportTrigger *pTrigger;
};

}
