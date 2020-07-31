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

#include <utility>
#include <vector>
#include <memory>
#include <map>
#include "IMetricSet.hpp"
#include "IMetricSink.hpp"
#include <platform.h>
#include <dlfcn.h>
#include "jptree.hpp"

namespace hpccMetrics
{

    typedef hpccMetrics::IMetricSink* (*getSinkInstance)(const std::string &sinkName, const IPropertyTree *pSettingsTree);

    class MetricSink : public IMetricSink
    {
        public:

            virtual ~MetricSink() = default;

            void addMetricSet(const std::shared_ptr<IMetricSet> &pSet) override
            {
                metricSets[pSet->getName()] = pSet;
            }

            std::string getName() const override { return name; }
            std::string getType() const override { return type; }


            // not sure if this is the best place for this or not, but wanted a static kind of factory that would load
            // a sink from a .so  (see the metrics/sinks folder for available sink libs)
            static IMetricSink *getSinkFromLib(const char *type, const char *getInstanceProcName, const std::string &sinkName, const IPropertyTree *pSettingsTree)
            {
                IMetricSink *pSink = nullptr;
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
                // If sinkName wasn't the lib name, treat it as a type
                if (libHandle == nullptr)
                {
                    libName.clear();
                    libName.append("libhpccmetrics_").append(type).append(SharedObjectExtension);
                    libHandle = dlopen(libName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                }

                //
                // If able to load the lib, get the instance proc and create the sink instance
                if (libHandle != nullptr)
                {
                    const char *epName = (getInstanceProcName != nullptr && strlen(getInstanceProcName) != 0) ?
                                         getInstanceProcName : "getSinkInstance";
                    auto getInstanceProc = (getSinkInstance) GetSharedProcedure(libHandle, epName);
                    if (getInstanceProc != nullptr)
                    {
                        pSink = getInstanceProc(sinkName.empty() ? type : sinkName, pSettingsTree);
                    }
                }
                return pSink;
            }


        protected:
            explicit MetricSink(std::string _name, std::string _type) :
                name{std::move(_name)},
                type{std::move(_type)}
            {  }

        protected:
            std::string name;
            std::string type;
            std::map<std::string, std::shared_ptr<IMetricSet>> metricSets;
    };
}
