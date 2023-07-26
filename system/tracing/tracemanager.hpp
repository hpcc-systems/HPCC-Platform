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

#ifndef TRACER_HPP
#define TRACER_HPP

#include "opentelemetry/exporters/ostream/span_exporter_factory.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/processor.h"
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/tracer_context.h"
#include "opentelemetry/sdk/trace/tracer_context_factory.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"

#include "opentelemetry/context/propagation/global_propagator.h"
#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/trace/propagation/http_trace_context.h"

#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/ext/http/common/url_parser.h"
#include "opentelemetry/trace/semantic_conventions.h"

#include <opentelemetry/context/context.h>
//#include <opentelemetry/context/propagation/text_map_propagator.h>


#include "jlib.hpp"
#include "jliball.hpp"

using namespace opentelemetry::trace;
namespace http_client = opentelemetry::ext::http::client;
namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;
namespace trace_sdk   = opentelemetry::sdk::trace;

namespace HPCCSemanticConventions
{
/**
 * Known HPCC span Keys could be added here
 * Specialized span keys can also be defined within the scope of a span
 */
static constexpr const char *kGLOBALIDHTTPHeader = "HPCC-Global-Id";
static constexpr const char *kCallerIdHTTPHeader = "HPCC-Caller-Id";
}


template <typename R>
class HPCCHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    HPCCHttpTextMapCarrier(R &headers) : httpHeaders(headers) {}
    HPCCHttpTextMapCarrier() = default;

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();

        // perform any key mapping needed...
        {
            //Instrumented http client/server Capitalizes the first letter of the header name
            if (key == propagation::kTraceParent || key == propagation::kTraceState )
                theKey[0] = toupper(theKey[0]);
        }

        return httpHeaders->queryProp(theKey.c_str());
    }

  virtual void Set(opentelemetry::nostd::string_view key,
                   opentelemetry::nostd::string_view value) noexcept override
    {
        httpHeaders->setProp(std::string(key).c_str(), std::string(value).c_str());
        //httpHeaders.insert(std::pair<std::string, std::string>(std::string(key), std::string(value)));
    }

    Owned<IProperties> httpHeaders = createProperties();
  //R httpHeaders = createProperties();
};


// TextMapCarrier is the storage medium used by TextMapPropagator.
// pure virtual Get(key) returns the value associated with the passed key.
// pure virtual Set(key, value) stores the key-value pair.
// virtual Keys(nostd::function_ref<bool(nostd::string_view)> /* callback */) 
//         list of all the keys in the carrier.
//         By default, it returns true without invoking callback */
template <typename T>
class HttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    HttpTextMapCarrier(T &headers) : httpHeaders(headers) {}
    HttpTextMapCarrier() = default;

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();

        // perform any key mapping needed...
        {
            //Instrumented http client/server Capitalizes the first letter of the header name
            if (key == propagation::kTraceParent || key == propagation::kTraceState )
                theKey[0] = toupper(theKey[0]);
        }

        //now search for the vaule
        auto it = httpHeaders.find(theKey);
        if (it != httpHeaders.end())
            return it->second;

        return "";
    }

  virtual void Set(opentelemetry::nostd::string_view key,
                   opentelemetry::nostd::string_view value) noexcept override
  {
      httpHeaders.insert(std::pair<std::string, std::string>(std::string(key), std::string(value)));
  }

  T httpHeaders;
};

class TraceManager
{
private:
    //Used as the opentel trace name, refered to as name of library being instrumented
    const char * tracerName = nullptr;
    const char * callerId = nullptr;

    //const char * globalIdHTTPHeaderName = "HPCC-Global-Id";
    //const char * callerIdHTTPHeaderName = "HPCC-Caller-Id";

public:
    TraceManager(const std::string & moduleName)
    {
        //1 tracer for each module
        tracerName = moduleName.c_str();
    };

    ~TraceManager() {};

    static void initTracer()
    {
        //Handle HPCC specific tracing configuration here
        //Target exporter? exporter connection info?
        //Target processor(s)? batch/interactive?
        //Target propogator? http? grpc? binary? custom?
        //HPCC component tracing switches?

        /*
        Owned<IPropertyTree> traceConfig = getComponentConfigSP()->getPropTree("tracing");
        if (traceConfig)
        {
            Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
            if (exportConfig)
            {
                if (exportConfig->getPropBool("OS", false)) //To stdout/err
                else if (exportConfig->getPropBool("OTLP", false))
                else if (exportConfig->getPropBool("Jaeger", false))
                else if (exportConfig->getPropBool("Zipkin", false))
                else if (exportConfig->getPropBool("Prometheus", false))
                else if (exportConfig->getPropBool("HPCC", false)) 
                //perhaps a custom exporter for HPCC,
                //which will send spans to a HPCC service, or
                //allow us to interogate the internal spandata (vs spancontext) which includes 
                //the span's parent, attributes, events, links, and status.
            }
        }
        */

        //OStream exporter, useful for development and debugging tasks and simplest to set up.
        auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

        //SimpleSpanProcesser sends spans one by one to an exporter.
        //We could use a batchspanprocessor, which will group several spans together, before sending them to an exporter.
        auto processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
        std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>> processors;
        processors.push_back(std::move(processor));

        // Default is an always-on sampler.
        std::shared_ptr<opentelemetry::sdk::trace::TracerContext> context =
            opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors));
        std::shared_ptr<opentelemetry::trace::TracerProvider> provider =
            opentelemetry::sdk::trace::TracerProviderFactory::Create(context);

        // Set the global trace provider
        opentelemetry::trace::Provider::SetTracerProvider(provider);

        // set global propagator
        // Injects Context into and extracts it from carriers that travel in-band
        // across process boundaries. Encoding is expected to conform to the HTTP
        // Header Field semantics.
        // Values are often encoded as RPC/HTTP request headers.
        opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
            opentelemetry::nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
                new opentelemetry::trace::propagation::HttpTraceContext()));
    }

    //convenience non-static method to get the default tracer, uses stored tracer/module name
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> getTracer()
    {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        return provider->GetTracer(tracerName); // (library_name [,library_version][,schema_url])
    }

    //convenience Static method to get the default tracer, uses provided module name
    static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> getTracer(std::string moduleName)
    {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        return provider->GetTracer(moduleName); // (library_name [,library_version][,schema_url])
    }

    static void cleanupTracer()
    {
        std::shared_ptr<opentelemetry::trace::TracerProvider> none;
        opentelemetry::trace::Provider::SetTracerProvider(none);
    }

    //SpanContex does not expose parent span.
    //If we need parent info, we might need to track them

    //Not convinced we should be tracking parent span context, but the StartSpan is 
    //here to explore the idea.

    //By intersecting call to tracer->StartSpan(), we can preemtively interogate current span
    //for what will ultimately be the parent span of the new span. 
    //template <class T, nostd::enable_if_t<opentelemetry::common::detail::is_key_value_iterable<T>::value> * = nullptr>
    //nostd::shared_ptr<Span> StartSpan(const char * spanName,
    //                                  const T &attributes,
    //                                  const StartSpanOptions &options = {}) noexcept
    //{
    //    nostd::shared_ptr<Span> currntSpan =
    //    TraceManager::getTracer(tracerName)->GetCurrentSpan();

    //    opentelemetry::v1::trace::SpanContext spanCtx = currntSpan->GetContext();

    //    if (currntSpan->IsRecording())
    //    {
    //        fprintf(stderr, "Parent span detected...\n");
            //if (spanCtx.IsRemote()) //this span hasn't started, it wouldn't be remote
    //        {
    //            char trace_id[32] = {0};
    //            spanCtx.trace_id().ToLowerBase16(trace_id);
    //            fprintf(stderr, "Parent trace id: '%s' ", std::string(trace_id, 32).c_str());

    //            char span_id[16] = {0};
    //            spanCtx.span_id().ToLowerBase16(span_id);
    //            fprintf(stderr, "Parent span id: '%s'", std::string(span_id, 32).c_str());
    //       }
    //    }

    //    return TraceManager::getTracer(tracerName)->StartSpan(spanName, attributes, options);
    //}

    const char* queryCallerId(context::propagation::TextMapCarrier &carrier) const
    {
        // Inject current context into http header
        auto currentCtx = context::RuntimeContext::GetCurrent();
        auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        propagator->Inject(carrier, currentCtx);

        // Extract parent span context from the TextMapCarrier
        auto parentSpanContext = propagator->Extract(carrier,currentCtx);

        // Get the value of the HPCC-Caller-Id header from the TextMapCarrier
        auto callerIdHeader = carrier.Get("traceparent");

        return callerIdHeader.empty() ? "" : callerIdHeader.data();
    }

    static bool setParentContextFromHeaders(std::map<std::string, std::string>& request_headers, StartSpanOptions & options)
    {
        const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(request_headers);
        auto prop        = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto current_ctx = context::RuntimeContext::GetCurrent();
        auto new_context = prop->Extract(carrier, current_ctx);
        options.parent   = GetSpan(new_context)->GetContext();

        return true;
    }

    static bool extractCallerSpanId(std::map<std::string, std::string> request_headers, std::string & callerSpanId)
    {
        const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(request_headers);
        return TraceManager::extractCallerSpanId(carrier, callerSpanId);
    }

    static bool extractCallerSpanId(const HttpTextMapCarrier<std::map<std::string, std::string>> carrier, std::string & callerSpanId)
    {
        auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto current_ctx = context::RuntimeContext::GetCurrent();
        auto new_context = propagator->Extract(carrier, current_ctx);
        auto parentSpan = GetSpan(new_context)->GetContext();

        char span_id[16] = {0};
        parentSpan.span_id().ToLowerBase16(span_id);
        callerSpanId = std::string(span_id, 32).c_str();
        return true;
    }

    bool setCallerId(std::map<std::string, std::string> request_headers)
    {
        const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(request_headers);
        return setCallerId(carrier);
    }

    bool setCallerId(const HttpTextMapCarrier<std::map<std::string, std::string>> carrier)
    {
        auto prop        = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto current_ctx = context::RuntimeContext::GetCurrent();
        auto new_context = prop->Extract(carrier, current_ctx);
        auto parentSpan = GetSpan(new_context)->GetContext();

        char span_id[16] = {0};
        parentSpan.span_id().ToLowerBase16(span_id);
        callerId = std::string(span_id, 32).c_str();

        return (callerId && *callerId) ? true : false;
    }

    const char* queryCallerId() const
    {
        return callerId;
    }

    const char* queryTraceId() const
    {
        nostd::shared_ptr<Span> currntSpan =
        TraceManager::getTracer(std::string(tracerName))->GetCurrentSpan();

        if (currntSpan->IsRecording())
        {
            auto spanCtx = currntSpan->GetContext();

            char trace_id[32] = {0};
            spanCtx.trace_id().ToLowerBase16(trace_id);
            return std::string(trace_id, 32).c_str();
        }
        else
            return "";
    }

    const char* queryLocalId() const
    {
        nostd::shared_ptr<Span> currntSpan =
        TraceManager::getTracer(std::string(tracerName))->GetCurrentSpan();

        if (currntSpan->IsRecording())
        {
            char span_id[16] = {0};
            currntSpan->GetContext().span_id().ToLowerBase16(span_id);

            return std::string(span_id, 32).c_str();
        }
        else
            return nullptr;
    }

    //const char* queryGlobalIdHTTPHeaderName() const { return globalIdHTTPHeaderName; }
    //const char* queryCallerIdHTTPHeaderName() const { return callerIdHTTPHeaderName; }

    //void setHttpIdHeaderNames(const char *global, const char *caller)
    //{
    //    if (global && *global)
    //        globalIdHTTPHeaderName = global;
    //    if (caller && *caller)
    //        callerIdHTTPHeaderName = caller;
    //}
};

#endif
