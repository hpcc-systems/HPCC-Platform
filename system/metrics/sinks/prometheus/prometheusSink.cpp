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

std::map<std::string, std::vector<std::string>> histogramLabels;

extern "C" MetricSink* getSinkInstance(const char *name, const IPropertyTree *pSettingsTree)
{
    return new PrometheusMetricSink(name, pSettingsTree);
}

PrometheusMetricSink::PrometheusMetricSink(const char *name, const IPropertyTree *pSettingsTree) :
    MetricSink(name, PROMETHEUS_REPORTER_TYPE)
{
    m_metricsManager = nullptr;
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

            LOG(MCuserError, "PrometheusMetricsService Error: %s", msg.str());
            LOG(MCuserInfo, "TxSummary[status=%d;user=@%s:%d;contLen=%ld;req=%s;path=%s]", res.status, req.remote_addr.c_str(), req.remote_port, req.content_length, req.method.c_str(), req.path.c_str());
        });

        m_server.Get(m_metricsServiceName.str(), [&](const Request& req, Response& res)
        {
            LOG(MCdebugInfo, "GET PrometheusMetricsService%s, from %s:%d", req.path.c_str(), req.remote_addr.c_str(), req.remote_port);

            StringBuffer payload;
            toPrometheusMetrics(m_metricsManager->queryMetricsForReport(std::string(m_metricsSinkName.str())), payload, m_verbose);

            res.set_content(payload.str(), PROMETHEUS_METRICS_SERVICE_RESP_TYPE);
            res.status = 200;
            LOG(MCdebugInfo, "PrometheusMetricsService Response: %s\n", payload.str());
            LOG(MCuserInfo, "TxSummary[status=%d;user=@%s:%d;contLen=%ld;req=GET;path=%s]", res.status, req.remote_addr.c_str(), req.remote_port, req.content_length, req.path.c_str());
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
    case hpccMetrics::METRICS_HISTOGRAM:
        return "histogram";
    default:
        LOG(MCinternalWarning, "Encountered unknown metric - cannot map to Prometheus metric!");
        return "unknowntype";
    }
}

void PrometheusMetricSink::toPrometheusMetrics(const std::vector<std::shared_ptr<IMetric>> & reportMetrics, StringBuffer & out, bool verbose)
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
        std::string metricName = getPrometheusMetricName(pMetric);

        MetricType metricType = pMetric->queryMetricType();

        if (verbose)
        {
            const char * prometheusMetricType = mapHPCCMetricTypeToPrometheusStr(metricType);
            if (!pMetric->queryDescription().empty())
                out.append("# HELP ").append(metricName.c_str()).append(" ").append(pMetric->queryDescription().c_str()).append("\n");

            if (prometheusMetricType)
                out.append("# TYPE ").append(metricName.c_str()).append(" ").append(prometheusMetricType).append("\n");
        }

        if (metricType == hpccMetrics::METRICS_HISTOGRAM)
        {
            toPrometheusHistogram(metricName, pMetric, out);
        }
        else
        {
            out.append(metricName.c_str());
            const auto &metaData = pMetric->queryMetaData();
            if (!metaData.empty())
            {
                out.append(" {");
                bool firstEntry = true;
                for (auto &metaDataIt: metaData)
                {
                    if (!firstEntry)
                        out.append(",");
                    else
                        firstEntry = false;

                    out.append(metaDataIt.key.c_str()).append("=\"").append(metaDataIt.value.c_str()).append("\"");
                }
                out.append("}");
            }
            out.append(" ").append(pMetric->queryValue()).append("\n");
        }
    }
}

void PrometheusMetricSink::toPrometheusHistogram(const std::string &name, const std::shared_ptr<IMetric> &pHistogram, StringBuffer & out)
{
    auto labels = getHistogramLabels(name, pHistogram);
    auto bucketCounts = pHistogram->queryHistogramValues();
    unsigned bucketLabelIndex = 0;
    __uint64 cumulativeBucketCount = 0;
    for (auto const &bucketCount: bucketCounts)
    {
        cumulativeBucketCount += bucketCount;
        out.append(labels[bucketLabelIndex++].c_str()).append(" ").append(cumulativeBucketCount).append("\n");
    }

    // Indices for the histogram value and cumulative count labels (stored sequentially after the
    // bucket count labels in the labels vector
    unsigned valueLabelIndex = bucketCounts.size();
    unsigned cumulativeCountLabelIndex = valueLabelIndex + 1;

    out.append(labels[valueLabelIndex].c_str()).append(" ").append(pHistogram->queryValue()).append("\n");
    out.append(labels[cumulativeCountLabelIndex].c_str()).append(" ").append(cumulativeBucketCount).append("\n");
}


std::string PrometheusMetricSink::getPrometheusMetricName(const std::shared_ptr<IMetric> &pMetric)
{
    std::string name = pMetric->queryName();

    //'.' is a known char used in HPCC metric names but invalid in Prometheus
    std::replace(name.begin(), name.end(), '.', '_');
    name.append("_").append(getPrometheusMetricUnits(pMetric));
    return name;
}

std::string PrometheusMetricSink::getPrometheusMetricUnits(const std::shared_ptr<IMetric> &pMetric)
{
    std::string unitsStr;
    switch (pMetric->queryUnits())
    {
        case SMeasureCount:
            unitsStr = "total";
            break;
        case SMeasureSize:
            unitsStr = "bytes";
            break;
        case SMeasureTimeNs:
            unitsStr = "seconds";
            break;
        default:
            unitsStr = "";
            break;
    }
    return unitsStr;
}

// return a vector of label strings matching the exposition format for a histogram as defined by the Prometheus spec.
// See https://prometheus.io/docs/instrumenting/exposition_formats/ for more information
const std::vector<std::string> &PrometheusMetricSink::getHistogramLabels(const std::string &name, const std::shared_ptr<IMetric> &pHistogram)
{
    auto it = histogramLabels.find(name);
    if (it != histogramLabels.end())
    {
        return it->second;
    }

    StatisticMeasure units = pHistogram->queryUnits();
    std::vector<std::string> labels;
    std::vector<__uint64> bucketLimits = pHistogram->queryHistogramBucketLimits();

    for (const auto &limit: bucketLimits)
    {
        std::string label(name);
        label.append("_bucket{le=\"");
        if (units == SMeasureTimeNs)
        {
            double secs = (double)limit / 1000000000.0;
            label.append(std::to_string(secs)).append("\"}");
        }
        else
        {
            label.append(std::to_string(limit)).append("\"}");
        }
        labels.emplace_back(label);
    }

    // Add the inf label
    std::string infLabel(name);
    infLabel.append("_bucket{\"le=+Inf\"}");
    labels.emplace_back(infLabel);

    // Sum and count
    std::string sumLabel;
    sumLabel.append("_sum");
    labels.emplace_back(sumLabel);

    std::string countLabel;
    countLabel.append("_count");
    labels.emplace_back(countLabel);

    // Insert the labels for reuse
    auto insertRet = histogramLabels.insert({name, labels});
    return insertRet.first->second;
};

void PrometheusMetricSink::startCollection(MetricsManager *_pManager)
{
    if (!_pManager)
        throw MakeStringException(-1, "PrometheusMetricsService: NULL MetricsManager detected!");

    m_metricsManager = _pManager;

    m_collectThread = std::thread(collectionThread, this);
    m_processing = true;
}

void PrometheusMetricSink::stopCollection()
{
    LOG(MCoperatorProgress, "PrometheusMetricsService stopping:  port: '%i' uri: '%s' sinkname: '%s'", m_port, m_metricsServiceName.str(), m_metricsSinkName.str());
    m_processing = false;
    m_server.stop();
    m_collectThread.join();
}

void PrometheusMetricSink::startServer()
{
    LOG(MCoperatorProgress, "PrometheusMetricsService started:  port: '%i' uri: '%s' sinkname: '%s'", m_port, m_metricsServiceName.str(), m_metricsSinkName.str());
    m_server.listen(BIND_ALL_LOCAL_NICS, m_port);
}
