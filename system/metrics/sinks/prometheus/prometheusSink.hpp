/*################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
*/


#pragma once

#include "jmetrics.hpp"
#include "jptree.hpp"
#include "jstring.hpp"
#include <thread>

//including cpp-httplib single header file REST client
//  doesn't work with format-nonliteral as an error
//
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

#undef INVALID_SOCKET
#include "httplib.h"

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#ifdef _USE_OPENSSL
#include <openssl/x509v3.h>
#endif

using namespace hpccMetrics;
using namespace httplib;

#ifdef PROMETHEUSSINK_EXPORTS
#define PROMETHEUSSINK_API DECL_EXPORT
#else
#define PROMETHEUSSINK_API DECL_IMPORT
#endif

class PROMETHEUSSINK_API PrometheusMetricSink : public MetricSink
{
public:
    explicit PrometheusMetricSink(const char *name, const IPropertyTree *pSettingsTree);
    ~PrometheusMetricSink() override = default;
    void startServer();
private:
    std::thread       m_collectThread;
    MetricsReporter * m_metricsReporter;
    StringBuffer      m_metricsSinkName;

    std::atomic<bool>               m_processing{false};
    int                             m_port;
    StringBuffer                    m_metricsServiceName;
    bool                            m_verbose;

    static constexpr const char * PROMETHEUS_REPORTER_TYPE = "prometheus";
    static constexpr const char * PROMETHEUS_METRICS_HTTP_PAGE_TITLE = "HPCC Systems - Prometheus Metrics Service";
    static constexpr const char * PROMETHEUS_METRICS_SERVICE_RESP_TYPE = "text/html; charset=UTF-8";
    static constexpr int          DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT = 8767;
    static constexpr const char * DEFAULT_PROMETHEUS_METRICS_SERVICE_NAME = "/metrics";
    static constexpr const char * HTTPLIB_ERROR_MESSAGE_HEADER_NAME = "EXCEPTION_WHAT";
    static constexpr const char * PROMETHEUS_METRICS_HTTP_ERROR = R"!!(<!DOCTYPE html><html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>HPCC Systems - Prometheus Metrics Service</title>
</head><body>
<H1>Error: %s</H1>
<H2>Path: %s</H2>
<H2>Status: %d</H2>
</body></html>
)!!";

protected:
    static void collectionThread(PrometheusMetricSink * prometheussink)
    {
        prometheussink->startServer();
    }

    Server m_server;

    virtual void startCollection(MetricsReporter * pReporter) override;
    virtual void stopCollection() override;

    static const char * mapHPCCMetricTypeToPrometheusStr(MetricType type);
    static void toPrometheusMetrics(std::vector<std::shared_ptr<IMetric>>, StringBuffer & out, bool verbose);
};

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree);
