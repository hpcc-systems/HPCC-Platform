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

#include "platform.h"

#include "jmisc.hpp"
#include "jtrace.hpp"
#include "lnuid.h"

using namespace ln_uid;

/*
* Sets global id if provided, and assign a localId
*/
void LogTrace::setGlobalId(const char* id)
{
    if (!isEmptyString(id))
    {
        globalId.set(id);
        assignLocalId();
    }
}

/*
* Sets global id if provided, assigns new localID
*/
LogTrace::LogTrace(const char * globalId)
{
    setGlobalId(globalId);
}

LogTrace::LogTrace()
{
    assignLocalId();
}

const char* LogTrace::assignLocalId()
{
    localId.set(createUniqueIdString().c_str());
    return localId.get();
}

const char* LogTrace::queryGlobalId() const
{
    return globalId.get();
}

void LogTrace::setCallerId(const char* id)
{
    callerId.set(id);
}

const char* LogTrace::queryCallerId() const
{
    return callerId.get();
}

const char* LogTrace::queryLocalId() const
{
    return localId.get();
}

void TraceManager::initTracer()
{
    //Handle HPCC specific tracing configuration here
    //Target exporter? exporter connection info?
    //Target processor(s)? batch/interactive? prob not
    //Target propogator? http? grpc? binary? custom?
    //HPCC component tracing switches?
    Owned<const IPropertyTree> traceConfig;
    try
    {
        traceConfig.setown(getComponentConfigSP()->getPropTree("tracing"));
    }
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
    }

    if (traceConfig)
    {
        Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
        if (exportConfig)
        {
            if (exportConfig->getPropBool("OS", false)) //To stdout/err
                DBGLOG("Tracing to stdout/err");
            else if (exportConfig->getPropBool("OTLP", false))
                DBGLOG("Tracing to OTLP");
            else if (exportConfig->getPropBool("Jaeger", false))
                DBGLOG("Tracing to Jaeger");
            else if (exportConfig->getPropBool("Zipkin", false))
                DBGLOG("Tracing to Zipkin");
            else if (exportConfig->getPropBool("Prometheus", false))
                DBGLOG("Tracing to Prometheus");
            else if (exportConfig->getPropBool("HPCC", false)) 
                DBGLOG("Tracing to HPCC JLog");
        }
    }
    else
    {
        using namespace opentelemetry::trace;
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
}

void TraceManager::cleanupTracer()
{
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    opentelemetry::trace::Provider::SetTracerProvider(none);
}

//convenience non-static method to get the default tracer, uses stored tracer/module name
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> TraceManager::getTracer()
{
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(tracerName); // (library_name [,library_version][,schema_url])
}

//convenience Static method to get the default tracer, uses provided module name
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> TraceManager::getTracer(std::string moduleName)
{
    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    return provider->GetTracer(moduleName); // (library_name [,library_version][,schema_url])
}

void TraceManager::getCallerSpanId(context::propagation::TextMapCarrier &carrier, std::string & callerSpanId)
{
    callerSpanId.clear();

    // Inject current context into http header
    auto currentCtx = context::RuntimeContext::GetCurrent();
    auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    propagator->Inject(carrier, currentCtx);

    // Extract parent span context from the TextMapCarrier
    auto parentSpanContext = propagator->Extract(carrier,currentCtx);

    // Get the value of the HPCC-Caller-Id header from the TextMapCarrier
    auto callerIdHeader = carrier.Get("traceparent");

    callerSpanId = callerIdHeader.data();
}

void TraceManager::getParentSpanId(std::map<std::string, std::string> requestHeaders, std::string & callerSpanId)
{
    const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(requestHeaders);
    TraceManager::getParentSpanId(carrier, callerSpanId);
}

void TraceManager::getParentSpanId(const HttpTextMapCarrier<std::map<std::string, std::string>> carrier, std::string & callerSpanId)
{
    callerSpanId.clear();

    auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto current_ctx = context::RuntimeContext::GetCurrent();
    auto new_context = propagator->Extract(carrier, current_ctx);
    auto parentSpan = opentel_trace::GetSpan(new_context)->GetContext();

    char span_id[16] = {0};
    parentSpan.span_id().ToLowerBase16(span_id);
    callerSpanId = std::string(span_id, 16).c_str();
}

void TraceManager::getCurrentTraceId(std::string & traceId) const
{
    traceId.clear();

    nostd::shared_ptr<opentel_trace::Span> currntSpan =
    TraceManager::getTracer(std::string(tracerName))->GetCurrentSpan();

    if (currntSpan->IsRecording())
    {
        auto spanCtx = currntSpan->GetContext();

        char trace_id[32] = {0};
        spanCtx.trace_id().ToLowerBase16(trace_id);
        traceId = std::string(trace_id, 32);
    }
}
void TraceManager::getParentContext(std::map<std::string, std::string>& request_headers, opentel_trace::StartSpanOptions & options)
{
    const HttpTextMapCarrier<std::map<std::string, std::string>> carrier(request_headers);
    auto prop        = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto current_ctx = context::RuntimeContext::GetCurrent();
    auto new_context = prop->Extract(carrier, current_ctx);
    options.parent   = opentelemetry::trace::GetSpan(new_context)->GetContext();
}

void TraceManager::getCurrentSpanID(std::string & spanId) const
{
    spanId.clear();
    nostd::shared_ptr<opentel_trace::Span> currntSpan =
    TraceManager::getTracer(std::string(tracerName))->GetCurrentSpan();

    if (currntSpan->IsRecording())
    {
        char span_id[16] = {0};
        currntSpan->GetContext().span_id().ToLowerBase16(span_id);

        spanId = std::string(span_id, 16);
    }
}



static Singleton<TraceManager> traceManager;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    TraceManager::initTracer(); //Initialize the tracer based on HPCC configuration
    return true;
}

MODULE_EXIT()
{
    traceManager.destroy();
}

//Name of module/library that is being traced/instrumented
TraceManager * queryTraceManager(const char * moduleName)
{
    return new TraceManager(moduleName);
}
