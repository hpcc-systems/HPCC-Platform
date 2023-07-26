// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "httpserver.hpp"
#include "opentelemetry/trace/context.h"
#include "opentelemetry/trace/semantic_conventions.h"
#include "tracemanager.hpp"

#include <iostream>
#include <thread>

namespace
{

using namespace opentelemetry::trace;
namespace context = opentelemetry::context;

uint16_t server_port              = 8800;
constexpr const char * server_name = "localhost";
constexpr const char * COMPONENT_NAME = "instrumented_http_server";

class RequestHandler : public HTTP_SERVER_NS::HttpRequestCallback
{
public:
    virtual int onHttpRequest(HTTP_SERVER_NS::HttpRequest const &request,
                              HTTP_SERVER_NS::HttpResponse &response) override
    {
        StartSpanOptions options;
        options.kind = SpanKind::kServer;

        // extract parent(caller) context from http header, set as parent context for current span
        TraceManager::setParentContextFromHeaders(const_cast<std::map<std::string, std::string> &>(request.headers), options);
        //Above call replaces the following code:
        //const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(request_headers);
        //auto prop        = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        //auto current_ctx = context::RuntimeContext::GetCurrent();
        //auto new_context = prop->Extract(carrier, current_ctx);
        //options.parent   = GetSpan(new_context)->GetContext();

        std::string callerSpanID;
        TraceManager::extractCallerSpanId(request.headers, callerSpanID);
        DBGLOG("Caller Span ID: %s", callerSpanID.c_str());

        std::string spanName = request.uri;

        TraceManager traceManager(COMPONENT_NAME); 
        auto tracer = traceManager.getTracer();
        //Declare current span with parent context extracted from http header
        auto span = 
          tracer->StartSpan(spanName,
            {{SemanticConventions::kNetHostName, server_name}, //span attributes
             {SemanticConventions::kNetHostPort, server_port},
             {SemanticConventions::kHttpMethod, request.method},
             {SemanticConventions::kHttpScheme, "http"},
             {SemanticConventions::kHttpRequestContentLength,
             static_cast<uint64_t>(request.content.length())},
             {SemanticConventions::kHttpClientIp, request.client}},
             options); //options.parent is set as parent context for current span

        auto scope = tracer->WithActiveSpan(span); 

        for (auto &kv : request.headers)
        {
            span->SetAttribute("http.header." + std::string(kv.first.data()), kv.second);
        }

        if (request.uri == "/helloworld")
        {
            span->AddEvent("Processing request");
            span->AddEvent("Setting response headers");
            response.headers[HTTP_SERVER_NS::CONTENT_TYPE] = HTTP_SERVER_NS::CONTENT_TYPE_TEXT;
            span->End();
            return 200;
        }
        else
        {
            span->AddEvent("Processing Error request");
            DBGLOG("Error request: %s", request.uri.c_str());
            span->End();
            return 404;
        }
    }
};
}  // namespace

int main(int argc, char *argv[])
{
    TraceManager::initTracer();

    // The port the validation service listens to can be specified via the command line.
    if (argc > 1)
    {
        server_port = (uint16_t)atoi(argv[1]);
    }

    HttpServer http_server(server_name, server_port);
    RequestHandler req_handler;
    http_server.AddHandler("/helloworld", &req_handler);

    auto root_span = TraceManager::getTracer(COMPONENT_NAME)->StartSpan(__func__);
    Scope scope(root_span); //not active span? Just for scope?
    http_server.Start();
    std::cout << "Server is running..Press ctrl-c to exit...\n";
    while (1)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    http_server.Stop();
    root_span->End();
    TraceManager::cleanupTracer();
    return 0;
}
