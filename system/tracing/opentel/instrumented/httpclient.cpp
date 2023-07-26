/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/ext/http/common/url_parser.h"
#include "opentelemetry/trace/semantic_conventions.h"

#include "tracemanager.hpp"

namespace
{

using namespace opentelemetry::trace;
namespace http_client = opentelemetry::ext::http::client;
namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;

//std::string MODULE_NAME = "SimulatedESPClient";
std::string MODULE_NAME = "http_server";

void sendRequest(const std::string &url)
{
    auto http_client = http_client::HttpClientFactory::CreateSync();

    opentelemetry::ext::http::common::UrlParser espReqURL(url); //url parts used as sample span options/attributes

    //Declare relevant span attributes
    std::initializer_list<std::pair<nostd::string_view, opentelemetry::v1::common::AttributeValue>>
     httpClientRequestAttributes =
    {{SemanticConventions::kHttpUrl, espReqURL.url_},
    {SemanticConventions::kHttpScheme, espReqURL.scheme_},
    {HPCCSemanticConventions::kGLOBALIDHTTPHeader, "someHTTPGlobalID"},
    {HPCCSemanticConventions::kCallerIdHTTPHeader, "notSureThisIsNeeded"},
    {SemanticConventions::kHttpMethod, "GET"}};

    // Declare and activate span
    StartSpanOptions options; // at this point span parent defaults to 'invalid' span context
    options.kind = SpanKind::kClient;  // we're simulating a client, by default kind = internal

    //tracemanager provides named tracer
    TraceManager traceManager(MODULE_NAME);
    //tracer is used to create named span
    auto tracer = traceManager.getTracer();

    //We'll call this span "/helloworld-ClientRequest"
    std::string spanName = espReqURL.path_ + "-ClientRequest";

    //declare the named span, provide span attributes, and options 
    nostd::shared_ptr<Span> clientReqSpan =
     //traceManager.StartSpan(spanName.c_str(), httpClientRequestAttributes, options);
     tracer->StartSpan(spanName, httpClientRequestAttributes, options);

    //activate the span
    Scope scope = tracer->WithActiveSpan(clientReqSpan);

    const char * mytraceid = traceManager.queryTraceId();
    fprintf(stdout, "mytraceid %s", mytraceid);

    // inject current context into http header
    auto currentCtx = context::RuntimeContext::GetCurrent();
    HttpTextMapCarrier<http_client::Headers> carrier1;
    HPCCHttpTextMapCarrier<IProperties> carrier;
    
    auto propegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    propegator->Inject(carrier, currentCtx); //injects current context as parent
    auto a = carrier.httpHeaders->queryProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader);
    const char * callerId = traceManager.queryCallerId(carrier);
    fprintf (stdout, "callerId %s and %s", callerId, a);

    // send http request
    http_client::Result result = http_client->GetNoSsl(url, carrier1.httpHeaders);

    if (result)
    {
        // set span attributes
        auto status_code = result.GetResponse().GetStatusCode();
        clientReqSpan->SetAttribute(SemanticConventions::kHttpStatusCode, status_code);
        result.GetResponse().ForEachHeader(
            [&clientReqSpan](nostd::string_view header_name, nostd::string_view header_value)
            {
              clientReqSpan->SetAttribute("http.header." + std::string(header_name.data()), header_value);
              return true;
            });

        if (status_code >= 400)
        {
            clientReqSpan->SetStatus(StatusCode::kError); // kUnset(default),
                                                       // kOk(Operation completed)
                                                       // kError(peration encountered error)
        }
    }
    else
    {
        clientReqSpan->SetStatus(
        StatusCode::kError,
        "Response Status :" + std::to_string(
                static_cast<typename std::underlying_type<http_client::SessionState>::type>(
                    result.GetSessionState())));
    }

    // end span and export data
    clientReqSpan->End();
}

}  // namespace


void subTask(TraceManager * traceman)
{
    StartSpanOptions options;

    auto span = traceman->getTracer()->StartSpan("subTask", options);
    auto scope = traceman->getTracer()->WithActiveSpan(span);

    span->End();
}

void mainTask(TraceManager * traceman)
{
    StartSpanOptions options;
    auto span = traceman->getTracer()->StartSpan("mainTask", options);
    auto scope = traceman->getTracer()->WithActiveSpan(span);
    subTask(traceman);
    span->End();
}

int main(int argc, char *argv[])
{
    TraceManager traceManager(MODULE_NAME);
    TraceManager::initTracer(); //@ init_module
                                //sets up default provider and http propegator
    mainTask(&traceManager);
    const char * mytraceid = traceManager.queryTraceId();
    if (isEmptyString(mytraceid))
        fprintf(stderr, "Span is not active");
    else
        fprintf(stderr, "Span id: %s", mytraceid);

    constexpr char default_host[]   = "localhost";
    constexpr char default_path[]   = "/helloworld";
    constexpr uint16_t default_port = 8800;
    uint16_t port = 8800;

    // The port the validation service listens to can be specified via the command line.
    if (argc > 1)
    {
        port = (uint16_t)(atoi(argv[1]));
    }
    else
    {
        port = default_port;
    }

    sendRequest("http://"+std::string(default_host)+":"+std::to_string(port)+std::string(default_path));

    TraceManager::cleanupTracer();
}