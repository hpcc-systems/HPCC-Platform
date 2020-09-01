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


/*

 Need to know mapping of a metric set name to an index name

 Elasticsearch
 - server
   hostname: <hostname>
   port: port number
   protocol: http | https

 - metricsets
   - metricset1_name
     index: index template




*/


#pragma once

#include "MetricSink.hpp"
#include <map>

using namespace hpccMetrics;


struct MetricSetInfo  {
    MetricSetInfo() = default;
    std::string setName;                                 // name of the set
    std::string indexTemplate;                           // template for generating the index name
    std::string lastIndexName;                           // last index name. If !empty, it also exists
};



class ElasticsearchSink : public MetricSink
{
    public:

        explicit ElasticsearchSink(std::string name, const std::map<std::string, std::string> &parms);
        void send(const MeasurementVector &values, const std::shared_ptr<IMetricSet> &pMetricSet, MetricsReportContext *pContext) override;
        void addMetricSet(const std::shared_ptr<IMetricSet> &pSet) override;


    protected:

        bool initializeIndex(const std::shared_ptr<IMetricSet>  &pMetricSet);
        static std::string buildIndexName(const std::string &nameTemplate, const std::string &setName);
        bool createNewIndex(MetricSetInfo *pSetInfo, const std::shared_ptr<IMetricSet>& pMetricSet, const std::string &indexName);
        static std::string getTypeString(ValueType type);


    protected:

        std::map<std::string, MetricSetInfo> metricSetInfo;
        std::string protocol;
        std::string domain;
        std::string port;
};
