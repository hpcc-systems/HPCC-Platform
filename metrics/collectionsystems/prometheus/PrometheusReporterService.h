/*
 * PrometheusReporterService.h
 *
 *  Created on: Aug 17, 2020
 *      Author: ubuntu
 */

#ifndef SUBPROJECTS__HPCCSYSTEMS_PLATFORM_METRICS_PROMETHEUS_REPORTER_PROMETHEUSREPORTERSERVICE_H_
#define SUBPROJECTS__HPCCSYSTEMS_PLATFORM_METRICS_PROMETHEUS_REPORTER_PROMETHEUSREPORTERSERVICE_H_


#include "jliball.hpp"
#include "securesocket.hpp"
#include "http/platform/httptransport.ipp"

#include "jiface.hpp"
#include "IMetricsReportTrigger.hpp"
#include "MetricsReportTrigger.hpp"
#include "IMetric.hpp"
#include "IMetricSet.hpp"
#include "IMetricSink.hpp"
#include "MetricsReportContext.hpp"
#include "MetricsReportConfig.hpp"
#include "MetricSink.hpp"

/*#include <map>
#include <string>
#include <memory>
#include <vector>
#include <unordered_set>
#include <memory>
*/
#include <thread>

using hpccMetrics::IMetricSet;
using hpccMetrics::MeasurementVector;
using hpccMetrics::MetricsReportConfig;
using hpccMetrics::MetricsReportTrigger;
using hpccMetrics::MetricSink;
using hpccMetrics::MetricsReportContext;

class PrometheusMetricsReportTrigger;

enum MockMetricType
{
    MockMetricType_Counter,
    MockMetricType_Gauge,
    MockMetricType_Untyped,
};

class MockMetric :  public CInterface
{
public:
    IMPLEMENT_IINTERFACE
    MockMetric(const char * domain, const char * name, MockMetricType type, const char * description)
    {
        m_domain = domain;
        m_name = name;
        m_type = type;
        m_description = description;
        m_value = "";
        m_fullName.append(domain).append("_").append(name);
        someincreasingvalue = 0;
    };

    const char * getDomain() {return m_domain;}
    const char * getFullName() {return m_fullName.str();}
    const char * getName() {return m_name;}
    MockMetricType getType() {return m_type;}
    const char * getDescription() {return m_description;}
    const char * getValue()
    {
        char strVal[100];
        switch (m_type)
        {
            case MockMetricType_Counter:
                someincreasingvalue = time(nullptr);
                return itoa(someincreasingvalue, strVal, 10);
            case MockMetricType_Gauge:
            default:
                return itoa(getRandom()%100, strVal, 10);
        }
    }

    virtual ~MockMetric() {};
private:
    time_t someincreasingvalue;
    const char * m_domain;
    const char * m_name;
    const char * m_value;
    MockMetricType m_type;
    const char * m_description;
    StringBuffer m_fullName;
};

enum PrometheusMetricType
{
    PrometheusMetricType_Counter,
    PrometheusMetricType_Histogram,
    PrometheusMetricType_Summary,
    PrometheusMetricType_Gauge,
    PrometheusMetricType_Untyped
};

class MockPrometheusCollector :  public CInterface
{
public:
    IMPLEMENT_IINTERFACE
    MockPrometheusCollector(){};
    virtual ~MockPrometheusCollector() {};

    static MockMetricType mapPrometheusMetricType(PrometheusMetricType type)
    {
        switch (type)
        {
            case PrometheusMetricType_Counter:
                return MockMetricType_Counter;
            case PrometheusMetricType_Gauge:
                return MockMetricType_Gauge;
            default:
                return MockMetricType_Untyped;
        }
    }

    static const char * mapMockMetricTypeToPrometheusStr(MockMetricType type)
    {
        switch (type)
        {
            case MockMetricType_Counter:
                return "counter";
            case MockMetricType_Gauge:
                return "gauge";
            default:
                return nullptr;
        }
    }

    bool available()
    {
        return true;
    }

    void addMetric(const char * domain, const char * name, PrometheusMetricType type, const char * description)
    {
        LOG(MCuserInfo,"PrometheusMetricColletor - Adding metric '%s_%s'", domain, name);

        /*switch (type)
        {
            case PROMETHEUS_METRIC_TYPE_COUNTER:
            case PROMETHEUS_METRIC_TYPE_GAUGE:
                return MockMetricType_Gauge;
            default:
                return MockMetricType_Untyped;

                PROMETHEUS_METRIC_TYPE_COUNTER,
                PROMETHEUS_METRIC_TYPE_HISTOGRAM,
                PROMETHEUS_METRIC_TYPE_SUMMARY,
                PROMETHEUS_METRIC_TYPE_GAUGE,
                PROMETHEUS_METRIC_UNTYPED
        }*/

        Owned<MockMetric> metric = new MockMetric(domain, name, mapPrometheusMetricType(type), description);
        m_metrics.append(*metric.getClear());
    }

    void setMetrics(IArrayOf<MockMetric> metrics)
    {
        IArrayOf<MockMetric> m_metrics;
    }

    void collect(StringBuffer & metrics)
    {
        for (int index = 0; index < m_metrics.length(); index++)
        {
            MockMetric metric = m_metrics.item(index);
            const char * fullName = metric.getFullName();

            if (metric.getDescription())
                metrics.append("# HELP ").append(fullName).append(" ").append(metric.getDescription()).append("\n");
            if (metric.getType() != MockMetricType_Untyped)
                metrics.append("# TYPE ").append(fullName).append(" ").append(mapMockMetricTypeToPrometheusStr(metric.getType())).append("\n");

            metrics.append(fullName).append(" ").append(metric.getValue()).append("\n");
        }
        /*SAMPLE metrics outpu:
         * # HELP exposer_transferred_bytes_total Transferred bytes to metrics services
         * # TYPE exposer_transferred_bytes_total counter
         * exposer_transferred_bytes_total 6567.000000
         * # HELP exposer_scrapes_total Number of times metrics were scraped
         * # TYPE exposer_scrapes_total counter
         * exposer_scrapes_total 20.000000
         * # HELP exposer_request_latencies Latencies of serving scrape requests, in microseconds
         * # TYPE exposer_request_latencies summary
         * exposer_request_latencies_count 20
         * exposer_request_latencies_sum 6125.000000
         * exposer_request_latencies{quantile=\"0.500000\"} 259.000000
         * exposer_request_latencies{quantile=\"0.900000\"} 388.000000
         * exposer_request_latencies{quantile=\"0.990000\"} 498.000000
         * # HELP time_running_seconds_total How many seconds is this server running?
         * # TYPE time_running_seconds_total counter
         * time_running_seconds_total{label=\"value\",another_label=\"value\",yet_another_label=\"value\"} 19.000000\n");
         * */
    }

private:
    ArrayOf<MockMetric> m_metrics;
};

class PrometheusReporterService
{
private:
    static constexpr const char * HTTP_PAGE_TITLE = "HPCC Systems - Prometheus Metrics Service";
    static constexpr const char * DEFAULT_PROMETHEUS_METRICS_SERVICE_RESP_TYPE = "text/html; charset=UTF-8";
    static constexpr int          DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT = 8767;
    static constexpr bool         DEFAULT_PROMETHEUS_METRICS_SERVICE_SSL = false;
    static constexpr const char * DEFAULT_PROMETHEUS_METRICS_SERVICE_NAME = "metrics";

	PrometheusMetricsReportTrigger * m_metricsTrigger;

    bool                            m_processing;
    int                             m_port;
    bool                            m_use_ssl;
    Owned<ISecureSocketContext>     m_ssctx;
    //Owned<MockPrometheusCollector>  m_collector;
    StringBuffer                    m_metricsServiceName;
    IPropertyTree *                 m_sslconfig;

    static bool onMetricsUnavailable(CHttpRequest * request, CHttpResponse * response);
    static bool onException(CHttpResponse * response, IException * e);
    static bool onException(CHttpResponse * response, const char * message);
    bool onMetrics(CHttpRequest * request, CHttpResponse * response);
    bool onPost(CHttpRequest * request,  CHttpResponse * response);
    bool onGet(CHttpRequest * request, CHttpResponse * response);
    void handleRequest(ISocket* client);

    bool setMetricsServiceName(const char * servicename)
    {
        if (servicename && *servicename)
            m_metricsServiceName.set(servicename);

        return true;
    }

public:

    static bool onGetNotFound(CHttpResponse * response, const char * path);
    static bool onPostNotFound(CHttpResponse * response, const char * path);
    static bool onMethodNotFound(CHttpResponse * response, const char * httpMethod, const char * path);

    //PrometheusReporterService();
    PrometheusReporterService(PrometheusMetricsReportTrigger * parent, const std::map<std::string, std::string> & parms);
    virtual ~PrometheusReporterService() {};
    int start();
    int stop();
};

class PremetheusMetricSink : public hpccMetrics::MetricSink
{
    public:

        explicit PremetheusMetricSink(const std::string & name, const std::map<std::string, std::string> & parms) :
		MetricSink(std::move(name), "prometheus")
        {
        }

        void send(const MeasurementVector &values, const std::shared_ptr<IMetricSet> &pMetricSet, MetricsReportContext *pContext) override
        {
        }

    protected:

        std::string m_name;
        std::string m_filename;
};

class PrometheusMetricsReportContext : MetricsReportContext
{
public:
	PrometheusMetricsReportContext()  {}
    virtual ~PrometheusMetricsReportContext() = default;

    const char * getMetrics() const
    {
        return metricsText.str();
    }

    void setMetrics(const char * metrics)
	{
		metricsText.set(metrics);
	}

private:
    StringBuffer metricsText;
};

class PrometheusMetricsReportTrigger : public MetricsReportTrigger
{
private:

	PrometheusReporterService * m_server;
	//MetricsReportConfig & m_reportConfig;
	Owned<MockPrometheusCollector> mockPrometheusCollector;

public:
	void getContextContent(StringBuffer & content)
	{
		//std::map<std::string, MetricsReportContext *> m_contexts;
		//doReport(m_contexts);
		//how do I get the content from the contexts?????
		mockPrometheusCollector->collect(content);
	}

	//this should be an iproptree
	PrometheusMetricsReportTrigger(const std::map<std::string, std::string> & parms)
	{
		InitModuleObjects(); //logging this might be a requirement on the framework

		//m_reportConfig = reportConfig;
		try
		{
			mockPrometheusCollector.set(new MockPrometheusCollector());
			mockPrometheusCollector->addMetric("myesp160", "processing_request_gauge", PrometheusMetricType_Gauge, "Count of current reqs in process");
			mockPrometheusCollector->addMetric("myesp160", "processing_request_total", PrometheusMetricType_Counter, "Aggregate count of reqs processed");

			//m_server(mockPrometheusCollector); //defaults, otherwise cfg tree with port/uri/ssl.
			m_server = new PrometheusReporterService(this, parms);
		}
		catch(IException *e)
		{
			StringBuffer emsg;
			LOG(MCuserInfo, "Error setting up PrometheusReporterService with mockPrometheusCollector");
			e->Release();
		}
	}

	void start() override
	{
		//stopCollection = false;
		//collectThread = std::thread(collectionThread, this);
		m_server->start();
	}

	void stop() override
	{
		//stopCollection = true;
		//collectThread.join();
	}

	bool isStopCollection() const
	{
		return false;
	//	return stopCollection;
	}

protected:
	//static void collectionThread(PrometheusMetricsReportTrigger * pReportTrigger)
	//static void collectionThread(PrometheusReporterService * prometheusServer)
	//{
		//prometheusServer->start();
		//while (!pReportTrigger->isStopCollection())
		//{
			//std::this_thread::sleep_for(pReportTrigger->periodSeconds);
			//std::map<std::string, MetricsReportContext *> contexts;  // note, this may move to be a class member
			//pReportTrigger->doReport(contexts);
		//}

		//pReportTrigger->stop();
	//}

private:
	bool stopCollection = false;
	std::thread collectThread;
};


#endif /* SUBPROJECTS__HPCCSYSTEMS_PLATFORM_METRICS_PROMETHEUS_REPORTER_PROMETHEUSREPORTERSERVICE_H_ */
