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
#include "MetricsReportTrigger.hpp"
#include "MetricsReportContext.hpp"
#include "MetricsReportConfig.hpp"

namespace hpccMetrics
{

    class PeriodicMetricsReportTrigger : public MetricsReportTrigger
    {
        public:
            explicit PeriodicMetricsReportTrigger(unsigned seconds, MetricsReportConfig &reportConfig) :
                    MetricsReportTrigger(reportConfig),
                    periodSeconds{std::chrono::seconds(seconds)}
            { }

            void start() override
            {
                init();
                collectThread = std::thread(collectionThread, this);
            }

            void stop() override
            {
                stopCollection = true;
                collectThread.join();
            }

            bool isStopCollection() const
            {
                return stopCollection;
            }

        protected:
            static void collectionThread(PeriodicMetricsReportTrigger *pReportTrigger)
            {
                while (!pReportTrigger->isStopCollection())
                {
                    std::this_thread::sleep_for(pReportTrigger->periodSeconds);
                    std::map<std::string, MetricsReportContext *> contexts;  // note, this may move to be a class member
                    pReportTrigger->doReport(contexts);
                }
            }

        private:
            bool stopCollection = false;
            std::thread collectThread;
            std::chrono::seconds periodSeconds;
    };

}
