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

#include "opentelemetry/trace/semantic_conventions.h" //known span defines
#include "opentelemetry/context/propagation/global_propagator.h" // context::propagation::GlobalTextMapPropagator::GetGlobalPropagator
#include "opentelemetry/sdk/trace/tracer_provider_factory.h" //opentelemetry::sdk::trace::TracerProviderFactory::Create(context)
#include "opentelemetry/sdk/trace/tracer_context_factory.h" //opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors));
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"// auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

#include "opentelemetry/trace/propagation/http_trace_context.h" //opentel_trace::propagation::kTraceParent

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

CSpan::CSpan() : span(nullptr) {};
CSpan::~CSpan()
{
    if (span != nullptr)
        span->End();
}

void CSpan::setAttribute(const char * key, const char * val)
{
    if (span)
        span->SetAttribute(key, val);
}

void CSpan::addEvent(const char * eventName)
{
    if (span && !isEmptyString(eventName))
        span->AddEvent(eventName);
}

class CHPCCHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    CHPCCHttpTextMapCarrier(const IProperties * httpHeaders)
    {
        if (httpHeaders)
        {
            this->httpHeaders.setown(createProperties());
            Owned<IPropertyIterator> iter = httpHeaders->getIterator();
            ForEach(*iter)
            {
                const char * key = iter->getPropKey();
                const char * val = httpHeaders->queryProp(key);
                this->httpHeaders->setProp(key, val);
            }
        }
    }

    CHPCCHttpTextMapCarrier()
    {
        httpHeaders.setown(createProperties());
    };

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();

        if (theKey.empty())
            return "";

        Owned<IPropertyIterator> iter = httpHeaders->getIterator();
        ForEach(*iter)
        {
            const char * propKey = iter->getPropKey();
            if (stricmp(propKey, theKey.c_str()) == 0)
                return httpHeaders->queryProp(propKey);
        }

        return "";
    }

    virtual void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override
    {
        httpHeaders->setProp(std::string(key).c_str(), std::string(value).c_str());        
    }

private:
    Owned<IProperties> httpHeaders;
};

/*
class HPCCStringArrayHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    HPCCStringArrayHttpTextMapCarrier(StringArray & headers)
    {
        httpHeaders.appendArray(headers);
    }

    HPCCStringArrayHttpTextMapCarrier() = default;

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();
        if (theKey.empty())
            return "";

        std::string headerval;

        ForEachItemIn(x, httpHeaders)
        {
            const char* header = httpHeaders.item(x);
            if(header == nullptr)
                continue;

            const char* colon = strchr(header, ':');
            if(colon == nullptr)
                continue;

            unsigned len = colon - header;
            if((theKey.length() == len) && (strnicmp(theKey.c_str(), header, len) == 0))
            {
                headerval.append(colon + 2);
                break;
            }
        }
        return headerval;
    }

    virtual void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override
    {
        if(key.empty())
            return;

        StringBuffer kv;
        kv.append(std::string(key).c_str()).append(": ").append(std::string(value).c_str());

        httpHeaders.append(kv.str());
    }

private:
    StringArray httpHeaders;
};
*/

void CTraceManager::initTracer()
{
    Owned<const IPropertyTree> traceConfig;
    try
    {
        traceConfig.setown(getComponentConfigSP()->getPropTree("tracing"));

#ifdef TRACECONFIGDEBUG
        if (!traceConfig)
        {
            const char * simulatedGlobalYaml = R"!!(global:
  tracing:
    enabled: true
    exporter:
      OS: true
    processor:
      batchSpan: true
      simpleSpan: false
  )!!";
            Owned<IPropertyTree> testTree = createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
            traceConfig.setown(testTree->getPropTree("global/tracing"));
        }
#endif
        if (traceConfig && traceConfig->getPropBool("enable", true))
        {
            DBGLOG("OpenTel tracing enabled");
            using namespace opentelemetry::trace;
            std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

            Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
            StringBuffer xml;
            toXML(exportConfig, xml);
            DBGLOG("exportConfig tree: %s", xml.str());
            if (exportConfig)
            {
                if (exportConfig->getPropBool("@OS", false)) //To stdout/err
                    DBGLOG("Tracing to stdout/err");
                else if (exportConfig->getPropBool("@OTLP", false))
                {
                    //namespace otlp = opentelemetry::exporter::otlp;

                    //otlp::OtlpGrpcExporterOptions opts;
                    //opts.endpoint = "localhost:4317";
                    //opts.use_ssl_credentials = true;
                    //opts.ssl_credentials_cacert_as_string = "ssl-certificate";

                    //exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
                    //DBGLOG("Tracing to OTLP");
                }
                else if (exportConfig->getPropBool("@Prometheus", false))
                    DBGLOG("Tracing to Prometheus");
                else if (exportConfig->getPropBool("@HPCC", false)) 
                    DBGLOG("Tracing to HPCC JLog");
            }

            std::unique_ptr<opentelemetry::v1::sdk::trace::SpanProcessor> processor;
            if (exportConfig->getPropBool("processor/batchSpan", true))
            {
                //Groups several spans together, before sending them to an exporter.
                opentelemetry::v1::sdk::trace::BatchSpanProcessorOptions options; //size_t max_queue_size = 2048;
                                                                                  //The time interval between two consecutive exports
                                                                                  //std::chrono::milliseconds(5000);
                                                                                  //The maximum batch size of every export. It must be smaller or
                                                                                  //equal to max_queue_size.
                                                                                  //size_t max_export_batch_size = 512
                processor = opentelemetry::sdk::trace::BatchSpanProcessorFactory::Create(std::move(exporter), options);
                DBGLOG("OpenTel tracing using batch Span Processor");
            }
            else
            {
                //SimpleSpanProcesser sends spans one by one to an exporter.
                processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
                DBGLOG("OpenTel tracing using Simple Span Processor");
            }

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
    catch (IException * e)
    {
        EXCLOG(e);
        e->Release();
    }
}

void CTraceManager::cleanupTracer()
{
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    opentelemetry::trace::Provider::SetTracerProvider(none);
}

void CSpan::setAttributes(const IProperties * attributes)
{
    Owned<IPropertyIterator> iter = attributes->getIterator();
    ForEach(*iter)
    {
        const char * key = iter->getPropKey();
        const char * val = attributes->queryProp(key);
        span->SetAttribute(key, val);
    }
}

void CTransactionSpan::setAttributesFromHTTPHeaders(StringArray & httpHeaders, opentelemetry::trace::StartSpanOptions * options)
{
    Owned<IProperties> httpHeaderProps = createProperties();
    ForEachItemIn(currentHeaderIndex, httpHeaders)
    {
        const char* httHeader = httpHeaders.item(currentHeaderIndex);
        if(!httHeader)
            continue;

        const char* colon = strchr(httHeader, ':');
        if(colon == nullptr)
            continue;

        StringBuffer key, value;
        key.append(colon - httHeader, httHeader);
        value.set(colon + 1);

        httpHeaderProps->setProp(key, value);
    }

    setAttributesFromHTTPHeaders(httpHeaderProps, options);

/*
        if(stricmp(httHeader, HPCCSemanticConventions::kGLOBALIDHTTPHeader) == 0)
        {
            hpccGlobalId.set(httpHeaders.item(currentHeaderIndex+1));
            continue;
        }

        if(stricmp(httHeader, HPCCSemanticConventions::kCallerIdHTTPHeader) == 0)
        {
            hpccCallerId.set(httpHeaders.item(currentHeaderIndex+1));
            continue;
        }
    }

    const HPCCStringArrayHttpTextMapCarrier carrier(httpHeaders);
    auto globalPropegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    auto currentContext = context::RuntimeContext::GetCurrent();
    auto newContext = globalPropegator->Extract(carrier, currentContext);
    parentContext = opentelemetry::trace::GetSpan(newContext)->GetContext();
    options.parent = parentContext;
*/
}

void CTransactionSpan::setAttributesFromHTTPHeaders(const IProperties * httpHeaders, opentelemetry::trace::StartSpanOptions * options)
{
    if (httpHeaders)
    {
        // perform any key mapping needed...
        //Instrumented http client/server Capitalizes the first letter of the header name
        //if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
        //    theKey[0] = toupper(theKey[0]);

        hpccGlobalId.set(httpHeaders->queryProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader));
        hpccCallerId.set(httpHeaders->queryProp(HPCCSemanticConventions::kCallerIdHTTPHeader));

        const CHPCCHttpTextMapCarrier carrier(httpHeaders);
        auto globalPropegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto currentContext = context::RuntimeContext::GetCurrent();
        auto newContext = globalPropegator->Extract(carrier, currentContext);
        parentContext = opentelemetry::trace::GetSpan(newContext)->GetContext();
        options->parent = parentContext;
    }
}

CClientSpan::CClientSpan(const char * spanName, const char * tracerName_)
//: CSpan(opentelemetry::trace::SpanKind::kClient, spanName, tracerName_) 
{
    opentelemetry::trace::StartSpanOptions options;
    options.kind = opentelemetry::trace::SpanKind::kClient;
    CSpan(&options, spanName, tracerName_);
}

CTransactionSpan::CTransactionSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders)
{
    opentelemetry::trace::StartSpanOptions options;
    options.kind = opentelemetry::trace::SpanKind::kServer;
    setAttributesFromHTTPHeaders(httpHeaders, &options);
    CSpan(&options, spanName, tracerName_);
}

CTransactionSpan::CTransactionSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders)
{
    opentelemetry::trace::StartSpanOptions options;
    options.kind = opentelemetry::trace::SpanKind::kServer;
    setAttributesFromHTTPHeaders(httpHeaders, &options);
    CSpan(&options, spanName, tracerName_);
}

void CSpan::setOTTraceID()
{
    openTelTraceID.clear();

    if (!span)
        return;

    auto spanCtx = span->GetContext();
    if (!spanCtx.IsValid())
        return;

    if (!spanCtx.trace_id().IsValid())
        return;

    char trace_id[32] = {0};
    
    spanCtx.trace_id().ToLowerBase16(trace_id);
    openTelTraceID.set(trace_id, 32);
}

void CSpan::setOTSpanID()
{
    openTelSpanID.clear();

    if (!span)
        return;

    char span_id[16] = {0};
    span->GetContext().span_id().ToLowerBase16(span_id);

    openTelSpanID.set(span_id, 16);
}

CSpan::CSpan(opentelemetry::trace::StartSpanOptions * options, const char * spanName, const char * tracerName_)
{
    name.set(spanName);
    tracerName.set(tracerName_);

    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    auto tracer = provider->GetTracer(tracerName.get());
    span = tracer->StartSpan(spanName, {}, *options); 

    StringAttr openTelTraceID;
    StringAttr openTelSpanID;

    auto scope = tracer->WithActiveSpan(span);
}

// inject current opentel context into carrier via propagator
bool CClientSpan::injectClientContext(CHPCCHttpTextMapCarrier * carrier)
{
    if (!carrier)
        return false;

    auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();

    //get current context...
    auto currentCtx = opentelemetry::context::RuntimeContext::GetCurrent();
    //and have the propagator inject the ctx into carrier
    propagator->Inject(*carrier, currentCtx);

    if (!isEmptyString(hpccGlobalId.get()))
        carrier->Set(HPCCSemanticConventions::kGLOBALIDHTTPHeader, hpccGlobalId.get());

    if (!isEmptyString(hpccCallerId.get()))
        carrier->Set(HPCCSemanticConventions::kCallerIdHTTPHeader, hpccCallerId.get());

    return true;
}

// inject current opentel context http headers container directly
// using homebrewed mechanisms
bool CClientSpan::injectClientContext(IProperties * httpHeaders)
{
    if (!httpHeaders)
        return false;

    if (isEmptyString(openTelTraceID.get()))
        return false;

    if (isEmptyString(openTelSpanID.get()))
        return false;

    StringBuffer contextHTTPHeader;
    //The traceparent header uses the version-trace_id-parent_id-trace_flags format where:
    //version is always 00. trace_id is a hex-encoded trace id. span_id is a hex-encoded span id. trace_flags is a hex-encoded 8-bit field that contains tracing flags such as sampling, trace level, etc.
    //Example: "traceparent", "00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01"
    contextHTTPHeader.append("00-").append(openTelTraceID.get()).append("-").append(openTelSpanID.get()).append("00");
    //opentelemetry::trace::propagation::kTraceParent
    httpHeaders->setProp("traceparent", contextHTTPHeader.str());

    StringBuffer traceStateHTTPHeader;
    traceStateHTTPHeader.append("hpcc=").append(openTelSpanID.get());
    //opentelemetry::trace::propagation::kTraceState
    httpHeaders->setProp("tracestate", contextHTTPHeader.str());

    if (!isEmptyString(hpccGlobalId.get()))
        httpHeaders->setProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader, hpccGlobalId.get());

    if (!isEmptyString(hpccCallerId.get()))
        httpHeaders->setProp(HPCCSemanticConventions::kCallerIdHTTPHeader, hpccCallerId.get());
}

static Singleton<CTraceManager> theTraceManager;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    theTraceManager.destroy();
}

CTraceManager * queryTraceManager(const char * componentName)
{
    return theTraceManager.query([=] () { return new CTraceManager(componentName); });
}