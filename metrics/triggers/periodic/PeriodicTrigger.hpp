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

#include <chrono>
#include <thread>
#include "../../MetricsReportTrigger.hpp"
#include "../../MetricsReportContext.hpp"
#include "../../MetricsReportConfig.hpp"

namespace hpccMetrics
{

    class PeriodicTrigger : public MetricsReportTrigger
    {
        public:
            PeriodicTrigger(const std::map<std::string, std::string> &parms, MetricsReportConfig &reportConfig);
            ~PeriodicTrigger() override;
            void start() override;
            void stop() override;
            bool isStopCollection() const;

        protected:
            void stopCollection();
            static void collectionThread(PeriodicTrigger *pReportTrigger);

        private:
            bool collectionStarted = false;
            bool stopCollectionFlag = false;
            std::thread collectThread;
            std::chrono::seconds periodSeconds;
    };

}
