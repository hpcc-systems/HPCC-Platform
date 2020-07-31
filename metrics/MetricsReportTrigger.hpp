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
#include "IMetricsReportTrigger.hpp"
#include "MetricsReporter.hpp"
#include "MetricsReportConfig.hpp"

namespace hpccMetrics
{

    class MetricsReportTrigger : public IMetricsReportTrigger
    {
        public:
            virtual ~MetricsReportTrigger() = default;

        protected:
            //
            // Do not allow constructing this base class
            explicit MetricsReportTrigger(MetricsReportConfig &reportConfig)
            {
                pReporter = new MetricsReporter(reportConfig);
            }

            void doReport(std::map<std::string, MetricsReportContext *> &reportContexts)
            {
                pReporter->report(reportContexts);
            }

            void init()
            {
                pReporter->init();
            }

        private:
            MetricsReporter *pReporter;
    };

}
