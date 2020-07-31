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
#include "jptree.hpp"

namespace hpccMetrics
{

    typedef hpccMetrics::IMetricsReportTrigger* (*getTriggerInstance)(const IPropertyTree *pSettingsTree);

    class MetricsReportTrigger : public IMetricsReportTrigger
    {
        public:
            virtual ~MetricsReportTrigger() = default;

            void setReporter(MetricsReporter *pMetricsReporter) override
            {
                pReporter = pMetricsReporter;
            }


            //
            // Load the trigger from the lib
            static IMetricsReportTrigger *getTriggerFromLib(const char *type, const char *getInstanceProcName, const IPropertyTree *pSettingsTree)
            {
                IMetricsReportTrigger *pTrigger = nullptr;

                std::string libName;

                //
                // First, treat type as a full library name
                libName = type;
                if (libName.find(SharedObjectExtension) == std::string::npos)
                {
                    libName.append(SharedObjectExtension);
                }
                HINSTANCE libHandle = dlopen(libName.c_str(), RTLD_NOW|RTLD_GLOBAL);

                //
                // If not, use type as a part of the standard metrics lib naming convention
                if (libHandle == nullptr)
                {
                    libName.clear();
                    libName.append("libhpccmetrics_").append(type).append(SharedObjectExtension);
                    libHandle = dlopen(libName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                }

                if (libHandle != nullptr)
                {
                    const char *epName = (getInstanceProcName != nullptr && strlen(getInstanceProcName) != 0) ?
                                         getInstanceProcName : "getTriggerInstance";
                    auto getInstanceProc = (getTriggerInstance) GetSharedProcedure(libHandle, epName);
                    if (getInstanceProc != nullptr)
                    {
                        pTrigger = getInstanceProc(pSettingsTree);
                    }
                }
                return pTrigger;
            }

        protected:
            //
            // Do not allow constructing this base class
            explicit MetricsReportTrigger() = default;

            //
            // sink name and report context are optional if no coupling between sink and trigger
            void doReport(std::string sinkName = std::string(), MetricsReportContext *pReportContext = nullptr)
            {
                std::map<std::string, MetricsReportContext *> reportContexts;
                if (pReportContext != nullptr)
                {
                    reportContexts[sinkName] = pReportContext;
                }
                pReporter->report(reportContexts);
            }

        private:
            MetricsReporter *pReporter;
    };

}
