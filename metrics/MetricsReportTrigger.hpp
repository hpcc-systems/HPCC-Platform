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

    typedef hpccMetrics::IMetricsReportTrigger* (*getTriggerInstance)(const std::map<std::string, std::string> &parms, MetricsReportConfig &reportConfig);

    class MetricsReportTrigger : public IMetricsReportTrigger
    {
        public:
            virtual ~MetricsReportTrigger() = default;

            //
            // Load the trigger from the lib
            static IMetricsReportTrigger *getTriggerFromLib(const char *triggerType, const char *getInstanceProcName, const std::map<std::string, std::string> &parms, MetricsReportConfig &reportConfig)
            {
                //
                // First treat the trigger name as part of libhpccmetricstriger_<triggerName>.SharedObjectExtension
                std::string triggerLibName = "libhpccmetrics_";
                triggerLibName.append(triggerType).append(SharedObjectExtension);
                HINSTANCE libHandle = dlopen(triggerLibName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                if (libHandle == nullptr)
                {
                    //
                    // Now try treating the triggerName as a lib naem
                    triggerLibName.clear();
                    triggerLibName.append(triggerType).append(SharedObjectExtension);
                    libHandle = dlopen(triggerLibName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                }

                if (libHandle != nullptr)
                {
                    const char *epName = (getInstanceProcName != nullptr && strlen(getInstanceProcName) != 0) ?
                                         getInstanceProcName : "getTriggerInstance";
                    auto getInstanceProc = (getTriggerInstance) GetSharedProcedure(libHandle, epName);
                    if (getInstanceProc != nullptr)
                    {
                        IMetricsReportTrigger *pTrigger = getInstanceProc(parms, reportConfig);
                        return pTrigger;
                    }
                }
                // todo throw an exception here, or return false? Will work this out once component configuration is added
                return nullptr;
            }

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
