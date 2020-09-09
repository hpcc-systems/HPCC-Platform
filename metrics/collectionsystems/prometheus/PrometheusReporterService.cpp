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

#include "PrometheusReporterService.hpp"

#include "jsocket.hpp"
#include "jstream.ipp"

#include "http/platform/httptransport.ipp"
#include "espcontext.hpp"
#include "httpsecurecontext.hpp"
#include "htmlpage.hpp"

using namespace hpccMetrics;

PrometheusReporterService::PrometheusReporterService(PrometheusMetricsReportTrigger * parent, const IPropertyTree * settings)
{
    m_metricsTrigger = parent;
    m_processing = false;
    m_port = DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT;
    m_use_ssl = DEFAULT_PROMETHEUS_METRICS_SERVICE_SSL;
    m_metricsServiceName.set(DEFAULT_PROMETHEUS_METRICS_SERVICE_NAME);
    m_sslconfig = nullptr;

    if (settings)
	{
		m_port = settings->getPropInt("port", DEFAULT_PROMETHEUS_METRICS_SERVICE_PORT);
		m_use_ssl = settings->getPropBool("useSSL", DEFAULT_PROMETHEUS_METRICS_SERVICE_SSL);

		settings->getProp("serviceName", m_metricsServiceName);
		if (m_metricsServiceName.isEmpty())
			m_metricsServiceName = DEFAULT_PROMETHEUS_METRICS_SERVICE_NAME;

		if(m_use_ssl)
		{
			   m_sslconfig = settings->getBranch("sslConfig");
#ifdef _USE_OPENSSL
			if(m_sslconfig != nullptr)
				m_ssctx.setown(createSecureSocketContextEx2(m_sslconfig, ServerSocket));
			else
				m_ssctx.setown(createSecureSocketContext(ServerSocket));
#else
		   throw MakeStringException(-1, "PrometheusReporterService: failure to create SSL socket - OpenSSL not enabled in build");
#endif
		}
	}

    LOG(MCuserInfo, "PrometheusReporterService created - port: '%i' uri: '/%s' ssl: '%s'\n", m_port, m_metricsServiceName.str(), m_use_ssl ? "true" : "false" );
}

int PrometheusReporterService::stop()
{
    m_processing = false; //this needs to be protected
    return 0;
}

bool PrometheusReporterService::isProcessing()
{
    return m_processing;
}

int PrometheusReporterService::start()
{
    m_processing = true;
    Owned<ISocket> serverSocket = ISocket::create(m_port);
    LOG(MCuserInfo, "PrometheusReporterService started - Listening on port: '%i'\n", m_port);

    while (m_processing)
    {
        Owned<ISocket> clientSocket = serverSocket->accept();

        // use ssl?
        if(m_use_ssl && m_ssctx.get() != nullptr)
        {
            try
            {
                Owned<ISecureSocket> secure_sock = m_ssctx->createSecureSocket(clientSocket.getLink());
                int res = secure_sock->secure_accept();
                if(res < 0)
                {
                    ERRLOG("secure_accept error\n");
                    continue;
                }
                clientSocket.set(secure_sock.get());
            }
            catch(...)
            {
                ERRLOG("secure_accept error\n");
                continue;
            }
        }

        try
        {
            handleRequest(clientSocket);
        }
        catch (IException* e)
        {
            StringBuffer msg;
            IERRLOG("Exception occurred: %s", e->errorMessage(msg).str());
            e->Release();
        }
        catch (...)
        {
            IERRLOG("Unknown exception occurred");
        }
    }

    LOG(MCuserInfo, "PrometheusReporterService stopping - port: '%i'\n", m_port);

    return 0;
}

bool PrometheusReporterService::onException(CHttpResponse * response, const char * message)
{
    IERRLOG("PrometheusReporterService encountered exception: %s", message);

    HtmlPage page(HTTP_PAGE_TITLE);
    page.appendContent(new CHtmlHeader(H1, "Internal Error"));

    if (message && *message)
        page.appendContent(new CHtmlText(message));
    else
        page.appendContent(new CHtmlText("Unknown Error"));

    StringBuffer content;
    page.getHtml(content);

    response->setContent(content.length(), content.str());
    response->setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR);

    response->send();

    return true;
}

bool PrometheusReporterService::onException(CHttpResponse * response, IException * e)
{
    StringBuffer errmessage;
    if (e)
        e->errorMessage(errmessage);
    else
        errmessage.set("Unknown Exception Error");

    return onException(response, errmessage.str());
}

bool PrometheusReporterService::onGetNotFound(CHttpResponse * response, const char * path)
{
    return onMethodNotFound(response, "GET", path);
}

bool PrometheusReporterService::onPostNotFound(CHttpResponse * response, const char * path)
{
    return onMethodNotFound(response, "POST", path);
}

bool PrometheusReporterService::onMethodNotFound(CHttpResponse * response, const char * httpMethod, const char * path)
{
    HtmlPage page(HTTP_PAGE_TITLE);
    VStringBuffer message("Target Service Not Found: '%s' '%s'", httpMethod, path);
    page.appendContent(new CHtmlHeader(H1, message.str()));

    StringBuffer content;
    page.getHtml(content);

    response->setContent(content.length(), content.str());
    response->setStatus(HTTP_STATUS_NOT_FOUND);

    response->send();
    return true;
}

bool PrometheusReporterService::onMetricsUnavailable(CHttpRequest * request, CHttpResponse * response)
{
    HtmlPage page(HTTP_PAGE_TITLE);
    VStringBuffer message("Metrics are not available: '%s'", request->queryPath());
    page.appendContent(new CHtmlHeader(H1, message.str()));

    StringBuffer content;
    page.getHtml(content);

    response->setContent(content.length(), content.str());
    response->setStatus(HTTP_STATUS_OK);

    response->send();
    return true;
}

bool PrometheusReporterService::onMetrics(CHttpRequest * request, CHttpResponse * response)
{
    StringBuffer metrics;
    if (!m_metricsTrigger)
        onMetricsUnavailable(request, response);
    else
    {
        m_metricsTrigger->getContextContent(metrics);
        response->setContent(metrics.length(), metrics.str());
        response->setContentType(DEFAULT_PROMETHEUS_METRICS_SERVICE_RESP_TYPE);

        response->send();
    }

    return true;
}

bool PrometheusReporterService::onPost(CHttpRequest * request, CHttpResponse * response)
{
    IEspContext& context = *request->queryContext();
    DBGLOG("PrometheusReporterService::onPost");

    return onPostNotFound(response, "");
}

bool PrometheusReporterService::onGet(CHttpRequest * request, CHttpResponse * response)
{
    DBGLOG("PrometheusReporterService::onGet");

    response->setVersion(HTTP_VERSION);
    response->addHeader("Expires", "0");
    response->setStatus(HTTP_STATUS_OK);
    response->setContentType(DEFAULT_PROMETHEUS_METRICS_SERVICE_RESP_TYPE);

    IEspContext& context = *request->queryContext();
    const char * serviceName = context.queryServiceName(nullptr); //why does this method require in param?

    if (!stricmp(serviceName, m_metricsServiceName.str()))
        return onMetrics(request, response);

    return onGetNotFound(response, serviceName);
}

void PrometheusReporterService::handleRequest(ISocket* clientSocket)
{
    Owned<CHttpRequest>         request;
    Owned<CHttpResponse>        response;

    request.setown(new CHttpRequest(*clientSocket));
    IEspContext* ctx = createEspContext(createHttpSecureContext(request.get()));
    response.setown(new CHttpResponse(*clientSocket));
    request->setOwnContext(ctx);
    response->setOwnContext(LINK(ctx));

    try
    {
        if (request->receive(nullptr) == -1)
        {
            LOG(MCinternalError, "Unknown issue - Receiving request - [PrometheusReporterService::handleOneRequest()]");
            return;
        }
    }
    catch(IEspHttpException* e)
    {
        onException(response, e);
        e->Release();
    }
    catch (IException *e)
    {
        onException(response, e);
        e->Release();
    }
    catch (...)
    {
        onException(response, "Unknown Exception - Reading request [PrometheusReporterService::handleOneRequest()]");
    }

    try
    {
        StringBuffer httpMethod;
        request->getMethod(httpMethod);

        //EspAuthState authState=authUnknown;
        sub_service stype = sub_serv_unknown;

        StringBuffer pathEx;
        StringBuffer serviceName;
        StringBuffer methodName;

        //perhaps this is too ESP specific???
        request->getEspPathInfo(stype, &pathEx, &serviceName, &methodName, false);
        request->updateContext();

        ctx->setServiceName(serviceName.str());
        ctx->setHTTPMethod(httpMethod.str());
        ctx->setServiceMethod(methodName.str());

        StringBuffer peerStr, pathStr;
        const char *userid=ctx->queryUserId();
        if (isEmptyString(userid))
            LOG(MCuserInfo, "%s %s, from %s", httpMethod.str(), request->getPath(pathStr).str(), request->getPeer(peerStr).str());
        else
            LOG(MCuserInfo, "%s %s, from %s@%s", httpMethod.str(), request->getPath(pathStr).str(), userid, request->getPeer(peerStr).str());

        if(stricmp(httpMethod.str(), POST_METHOD)==0)
            onPost(request.get(), response.get());
        else if(!stricmp(httpMethod.str(), GET_METHOD))
            onGet(request.get(), response.get());
        else
            onMethodNotFound(response, httpMethod.str(), serviceName.str());
    }
    catch(IException *e)
    {
        onException(response, e);
        e->Release();
    }
    catch(...)
    {
        onException(response, "Unknown Exception - Processing request [PrometheusReporterService::handleOneRequest()]");
    }
}

extern "C" IMetricsReportTrigger* getTriggerInstance(const IPropertyTree *pSettingsTree)
{
    return new PrometheusMetricsReportTrigger(pSettingsTree);
}

extern "C" IMetricSink* getSinkInstance(const std::string & name, const IPropertyTree *pSettingsTree)
{
    return new PrometheusMetricSink(name, pSettingsTree);
}
