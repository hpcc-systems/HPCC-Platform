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

#include "jlog.hpp"
#include "prometheusSink.hpp"

using namespace hpccMetrics;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    return new PrometheusMetricSink(name, pSettingsTree);
}

PrometheusMetricSink::PrometheusMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    MetricSink(name, PROMETHEUS_REPORTER_TYPE)
{
    m_metricsReporter = nullptr;
    m_metricsSinkName = name;
    m_processing = false;
    m_port = DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT;
    m_verbose = false;

    if (pSettingsTree)
    {
        m_verbose = pSettingsTree->getPropBool("@verbose", false);
        m_port = pSettingsTree->getPropInt("@port", DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT);

        pSettingsTree->getProp("@serviceName", m_metricsServiceName);

        if (m_metricsServiceName.isEmpty())
            m_metricsServiceName = DEFAULT_PROMETHEUS_METRICS_SERVICE_NAME;
        else
            if (m_metricsServiceName.charAt(0) != '/')
                m_metricsServiceName.insert(0, '/');


        m_server.set_error_handler([](const Request& req, Response& res)
        {
            StringBuffer msg(detail::status_message(res.status));
            if (res.status == 500)
            {
                if (res.has_header(HTTPLIB_ERROR_MESSAGE_HEADER_NAME))
                    msg.append(" - ").append(res.get_header_value(HTTPLIB_ERROR_MESSAGE_HEADER_NAME).c_str());
                else
                    msg.append(" - ").append("encountered unknown error!");

                LOG(MCinternalError, "PrometheusMetricsService: %s", msg.str());
            }

            VStringBuffer respmessage(PROMETHEUS_METRICS_HTTP_ERROR, msg.str(), req.path.c_str(), res.status);
            res.set_content(respmessage.str(), PROMETHEUS_METRICS_SERVICE_RESP_TYPE);

            LOG(MCuserInfo, "TxSummary[status=%d;user=@%s:%d;contLen=%ld;req=%s;]\n", res.status, req.remote_addr.c_str(), req.remote_port, req.content_length, req.method.c_str());
        });

        m_server.Get(m_metricsServiceName.str(), [&](const Request& req, Response& res)
        {
            StringBuffer payload;
            if (!m_metricsReporter)
                throw std::runtime_error("NULL MetricsReporter detected!");
            else
                toPrometheusMetrics(m_metricsReporter->queryMetricsForReport(std::string(m_metricsSinkName.str())), payload, m_verbose);

            res.set_content(payload.str(), PROMETHEUS_METRICS_SERVICE_RESP_TYPE);
            LOG(MCuserInfo, "TxSummary[status=%d;user=@%s:%d;contLen=%ld;req=GET;]\n",res.status, req.remote_addr.c_str(), req.remote_port, req.content_length);
        });
    }
}

const char * PrometheusMetricSink::mapHPCCMetricTypeToPrometheusStr(MetricType type)
{
    switch (type)
    {
    case hpccMetrics::METRICS_COUNTER:
        return "counter";
    case hpccMetrics::METRICS_GAUGE:
        return "gauge";
    default:
        LOG(MCdebugInfo, "Encountered unknown metric - cannot map to Prometheus metric!");
        return nullptr;
    }
}

void PrometheusMetricSink::toPrometheusMetrics(std::vector<std::shared_ptr<IMetric>> reportMetrics, StringBuffer & out, bool verbose)
{
    /*
     * [# HELP <metric name> <metric summary>\n]
     * [# TYPE <metric name> <metric type, if missing, metric is un-typed>\n]
     * <metric name> [{<label name>=<label value>, ...}] <metric value>\n
     *
     * where metric name := [a-zA-Z_:][a-zA-Z0-9_:]*
     */

    for (auto &pMetric: reportMetrics)
    {
        const std::string & name = pMetric->queryName();
        if (verbose)
        {
            if (!pMetric->queryDescription().empty())
                out.append("# HELP ").append(name.c_str()).append(" ").append(pMetric->queryDescription().c_str()).append("\n");

            MetricType type = pMetric->queryMetricType();
            const char * promtype = mapHPCCMetricTypeToPrometheusStr(type);
            if (promtype)
                out.append("# TYPE ").append(name.c_str()).append(" ").append(promtype).append("\n");
        }

        out.append(name.c_str()).append(" ").append(pMetric->queryValue()).append("\n");
    }
}

void PrometheusMetricSink::startCollection(MetricsReporter *_pReporter)
{
    if (!_pReporter)
        throw MakeStringException(-1, "PrometheusMetricsService: NULL MetricsReporter detected!");

    m_metricsReporter = _pReporter;

    m_collectThread = std::thread(collectionThread, this);
    m_processing = true;
}

void PrometheusMetricSink::stopCollection()
{
    m_processing = false;
    m_server.stop();
    m_collectThread.join();
}

void PrometheusMetricSink::startServer()
{
    LOG(MCoperatorProgress, "PrometheusMetricsService started:  port: '%i' uri: '%s' sinkname: '%s'\n", m_port, m_metricsServiceName.str(), m_metricsSinkName.str());
    m_server.listen("localhost", m_port);
}
