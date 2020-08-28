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

namespace hpccMetrics
{

    typedef hpccMetrics::IMetricSink* (*getSinkInstance)(const std::string &sinkName, const std::map<std::string, std::string> &parms);

    class MetricSink : public IMetricSink
    {
        public:

            virtual ~MetricSink() = default;

            void addMetricSet(const std::shared_ptr<IMetricSet> &pSet) override
            {
                metricSets[pSet->getName()] = pSet;
            }

            std::string getName() const override { return name; }

            //
            // Gets a sink instance by sink type. Sink types have a specific lib name format. When loaded they are also
            static IMetricSink *getSinkByType(const char *sinkType, const std::string &sinkName, const std::map<std::string, std::string> &parms)
            {
                //
                // When loading by type, the lib has a specific formatted name of libhpccmetricssink_<type>.
                std::string sinkLibName = "libhpccmetricssink_";
                sinkLibName.append(sinkType);
                return MetricSink::getSinkFromLib(sinkLibName.c_str(), nullptr, sinkName, parms);
            }


            // not sure if this is the best place for this or not, but wanted a static kind of factory that would load
            // a sink from a .so  (see the metrics/sinks folder for available sink libs)
            static IMetricSink *getSinkFromLib(const char *sinkLibName, const char *getInstanceProcName, const std::string &sinkName, const std::map<std::string, std::string> &parms)
            {
                //
                // Add .so if recognized extension is not present
                std::string libName = sinkLibName;
                if (libName.find(SharedObjectExtension) == std::string::npos)
                {
                    libName.append(SharedObjectExtension);
                }

                //
                // Open the lib. Note that if it fails the first
                // time, append the input name to a base hpcc
                // lib name for hpcc metrics and try again
                HINSTANCE libHandle = dlopen(libName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                if (libHandle == nullptr)
                {
                    libName = "libhpccmetrics_" + libName;
                    libHandle = dlopen(libName.c_str(), RTLD_NOW|RTLD_GLOBAL);
                }

                if (libHandle != nullptr)
                {
                    const char *epName = (getInstanceProcName != nullptr && strlen(getInstanceProcName) != 0) ?
                                         getInstanceProcName : "getSinkInstance";
                    auto getInstanceProc = (getSinkInstance) GetSharedProcedure(libHandle, epName);
                    if (getInstanceProc != nullptr)
                    {
                        IMetricSink *pSink = getInstanceProc(sinkName, parms);
                        return pSink;
                    }
                }
                // todo throw an exception here, or return false? Will work this out once component configuration is added
                return nullptr;
            }

            std::string getType() const override { return type; }

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
