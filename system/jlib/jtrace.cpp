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



#include "opentelemetry/trace/semantic_conventions.h" //known span defines
#include "opentelemetry/context/propagation/global_propagator.h" // context::propagation::GlobalTextMapPropagator::GetGlobalPropagator
#include "opentelemetry/sdk/trace/tracer_provider_factory.h" //opentelemetry::sdk::trace::TracerProviderFactory::Create(context)
#include "opentelemetry/sdk/trace/tracer_context_factory.h" //opentelemetry::sdk::trace::TracerContextFactory::Create(std::move(processors));
#include "opentelemetry/sdk/trace/simple_processor_factory.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/exporters/ostream/span_exporter_factory.h"// auto exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
//#define oldForEach ForEach // error: ‘ForEach’ was not declared in this scope
#undef ForEach //opentelemetry defines ForEach
#include "opentelemetry/exporters/memory/in_memory_span_exporter_factory.h"
#include "opentelemetry/trace/propagation/http_trace_context.h" //opentel_trace::propagation::kTraceParent
#undef UNIMPLEMENTED //opentelemetry defines UNIMPLEMENTED
#include "opentelemetry/trace/provider.h" //StartSpanOptions
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter.h"
#define UNIMPLEMENTED throw makeStringExceptionV(-1, "UNIMPLEMENTED feature at %s(%d)", sanitizeSourceFile(__FILE__), __LINE__)
#define ForEach(i)              for((i).first();(i).isValid();(i).next())

#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/exporters/memory/in_memory_span_data.h"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jtrace.hpp"
#include "lnuid.h"

namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;
namespace opentel_trace = opentelemetry::trace;

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

enum SpanType { ServerSpan, ClientSpan, InternalSpan };

class CSpan : public CInterfaceOf<ISpan>
{
public:
    CSpan() : span(nullptr) {};
    ~CSpan()
    {
        if (span != nullptr)
        {
            StringBuffer out;
            toString(out);
            DBGLOG("Span end: (%s)", out.str());
            span->End();
        }
    }

    ISpan * createClientSpan(const char * name) override
    {
        return new CSpan(name, SpanType::ClientSpan, this);
    }

    ISpan * createInternalSpan(const char * name) override
    {
        return new CSpan(name, SpanType::InternalSpan, this);
    }

    void toString(StringBuffer & out) override
    {
        if (span != nullptr)
        {
            out.append("Name: ").append(name.get())
            .append(" SpanID: ").append(spanID.get())
            .append(" TraceID: ").append(traceID.get())
            .append(" TraceFlags: ").append(traceFlags.get())
            .append(" HPCCGlobalID: ").append(hpccGlobalId.get())
            .append(" HPCCCallerID: ").append(hpccCallerId.get());

            if (parentSpan != nullptr)
            {
                out.append(" ParentSpanID: ");
                parentSpan->toString(out);
            }
        }
    };

    void setSpanAttributes(const IProperties * attributes) override
    {
        Owned<IPropertyIterator> iter = attributes->getIterator();
        ForEach(*iter)
        {
            const char * key = iter->getPropKey();
            if (!isEmptyString(key))
                setSpanAttribute(key, attributes->queryProp(key));
        }
    }

    void setSpanAttribute(const char * key, const char * val) override
    {
        if (span && !isEmptyString(key) && !isEmptyString(val))
            span->SetAttribute(key, val);
    }

    void addSpanEvent(const char * eventName) override
    {
        if (span && !isEmptyString(eventName))
            span->AddEvent(eventName);
    }

    void querySpanContextProperties(IProperties * contextProps) override
    {
        if (span != nullptr && contextProps != nullptr)
        {
            contextProps->setProp("traceID", traceID.get());
            contextProps->setProp("spanID", spanID.get());
            contextProps->setProp("traceFlags", traceFlags.get());
            contextProps->setProp("hpccGlobalId", hpccGlobalId.get());
            contextProps->setProp("hpccCallerId", hpccCallerId.get());
        }
    }


    /**
     * Injects the current span context into the given HTTP text map carrier.
     * The carrier is used to propagate the span context across process boundaries.
     *
     * @param carrier A pointer to the HTTP text map carrier to inject the span context into.
     * @return True if the span context was successfully injected, false otherwise.
     */
    /*bool injectSpanContext(CHPCCHttpTextMapCarrier * carrier) override
    {
        if (!carrier)
            return false;

        auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();

        //get current context...
        opentelemetry::v1::context::Context currentCtx = opentelemetry::context::RuntimeContext::GetCurrent();
        //and have the propagator inject the ctx into carrier
        propagator->Inject(*carrier, currentCtx);

        if (!isEmptyString(hpccGlobalId.get()))
            carrier->Set(HPCCSemanticConventions::kGLOBALIDHTTPHeader, hpccGlobalId.get());

        if (!isEmptyString(hpccCallerId.get()))
            carrier->Set(HPCCSemanticConventions::kCallerIdHTTPHeader, hpccCallerId.get());

        return true;
    }*/

    bool injectSpanContext(IProperties * contextProps) override
    {
        if (!contextProps || isEmptyString(traceID.get()) || isEmptyString(spanID.get()) || isEmptyString(traceFlags.get()))
            return false;

        StringBuffer contextHTTPHeader;
        //The traceparent header uses the version-trace_id-parent_id-trace_flags format where:
        //version is always 00. trace_id is a hex-encoded trace id. span_id is a hex-encoded span id. trace_flags is a hex-encoded 8-bit field that contains tracing flags such as sampling, trace level, etc.
        //Example: "traceparent", "00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01"
        contextHTTPHeader.append("00-").append(traceID.get()).append("-").append(spanID.get()).append(traceFlags.get());
        contextProps->setProp(opentelemetry::trace::propagation::kTraceParent.data(), contextHTTPHeader.str());

        StringBuffer traceStateHTTPHeader;
        traceStateHTTPHeader.append("hpcc=").append(spanID.get());

        contextProps->setProp(opentelemetry::trace::propagation::kTraceState.data(), contextHTTPHeader.str());

        if (!isEmptyString(hpccGlobalId.get()))
            contextProps->setProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader, hpccGlobalId.get());

        if (!isEmptyString(hpccCallerId.get()))
            contextProps->setProp(HPCCSemanticConventions::kCallerIdHTTPHeader, hpccCallerId.get());

        return true;
    }

    opentelemetry::v1::trace::SpanContext querySpanContext() const
    {
        if (span != nullptr)
            return span->GetContext();

        return opentelemetry::trace::SpanContext::GetInvalid();
    }

    const char * queryTraceName() const
    {
        return tracerName.get();
    }

protected:
    CSpan(const char * spanName, SpanType type, CSpan * parent)
    {
        name.set(spanName);
        parentSpan = parent;
        if (parentSpan != nullptr)
            tracerName.set(parent->queryTraceName());
        this->type = type;

         init();
    }

    CSpan(const char * spanName, SpanType type, const char * tracerName_)
    {
        name.set(spanName);
        parentSpan = nullptr;
        tracerName.set(tracerName_);
        this->type = type;
    }

    void init()
    {
        switch (type)
        {
            case SpanType::ServerSpan:
                opts.kind = opentelemetry::trace::SpanKind::kServer;
                break;
            case SpanType::ClientSpan:
                opts.kind = opentelemetry::trace::SpanKind::kClient;
                break;
            case SpanType::InternalSpan:
            default:
                opts.kind = opentelemetry::trace::SpanKind::kInternal;
                break;
        }

        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        //what if tracerName is empty?
        auto tracer = provider->GetTracer(tracerName.get());

        if (parentSpan != nullptr)
            injectParentSpan(parentSpan);

        span = tracer->StartSpan(name.get(), {}, opts);

        if (span != nullptr)
        {
            storeSpanContext();

            StringBuffer out;
            toString(out);
            DBGLOG("Span start: (%s)", out.str());
        }
    }

    void storeSpanContext()
    {
        if (span != nullptr)
        {
            storeTraceID();
            storeSpanID();
            storeTraceFlags();
        }
    }

    void injectParentSpan(CSpan * parentSpan)
    {
        if (parentSpan != nullptr)
        {
            auto parentSpanCtx = parentSpan->querySpanContext();
            if(parentSpanCtx.IsValid())
                opts.parent = parentSpanCtx;
        }
    }

    void storeTraceID()
    {
        traceID.clear();

        if (!span)
            return;

        auto spanCtx = span->GetContext();
        if (!spanCtx.IsValid())
            return;

        if (!spanCtx.trace_id().IsValid())
            return;

        char trace_id[32] = {0};
        
        spanCtx.trace_id().ToLowerBase16(trace_id);
        traceID.set(trace_id, 32);
    }

    void storeSpanID()
    {
        spanID.clear();

        if (!span)
            return;

        char span_id[16] = {0};
        span->GetContext().span_id().ToLowerBase16(span_id);

        spanID.set(span_id, 16);
    }

    void storeTraceFlags()
    {
        traceFlags.clear();

        if (!span)
            return;

        char trace_flags[2] = {0};
        span->GetContext().trace_flags().ToLowerBase16(trace_flags);

        traceFlags.set(trace_flags, 2);
    }

    StringAttr name;
    StringAttr tracerName;
    StringAttr traceFlags;
    StringAttr traceID;
    StringAttr spanID;
    StringAttr hpccGlobalId;
    StringAttr hpccCallerId;

    opentelemetry::trace::StartSpanOptions opts;
    nostd::shared_ptr<opentelemetry::trace::Span> span;
    CSpan * parentSpan = nullptr;
    SpanType type = SpanType::InternalSpan;
};

class CServerSpan : public CSpan
{
private:
    void setSpanContext(StringArray & httpHeaders, const char kvDelineator = ':')
    {
        Owned<IProperties> contextProps = createProperties();
        ForEachItemIn(currentHeaderIndex, httpHeaders)
        {
            const char* httpHeader = httpHeaders.item(currentHeaderIndex);
            if(!httpHeader)
                continue;

            const char* delineator = strchr(httpHeader, kvDelineator);
            if(delineator == nullptr)
                continue;

            StringBuffer key, value;
            key.append(delineator - httpHeader, httpHeader);
            value.set(delineator + 1);

            contextProps->setProp(key, value);
        }

        setSpanContext(contextProps);
    }

    void setSpanContext(const IProperties * httpHeaders)
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
            opentelemetry::v1::context::Context currentContext = context::RuntimeContext::GetCurrent();
            //auto inValidCtx = opentelemetry::trace::SpanContext::GetInvalid();
            //auto defSpan = new opentelemetry::v1::trace::Span();
            //auto defSpan = nostd::shared_ptr<Span>(new DefaultSpan(inValidCtx));
            //auto defspanctx = new opentelemetry::trace::DefaultSpan(inValidCtx);
            //auto defSpan = nostd::shared_ptr<opentelemetry::trace::Span>(defspanctx);
            //opentelemetry::trace::propagation::ExtractContextFromTraceHeaders();

            //auto c = opentelemetry::trace::propagation::ExtractImpl(carrier);
            auto newContext = globalPropegator->Extract(carrier, currentContext);
            //auto newContext = globalPropegator->Extract(carrier, inValidCtx);
            opentelemetry::v1::nostd::shared_ptr<opentelemetry::v1::trace::Span> declaredParentSpan = opentelemetry::trace::GetSpan(newContext);
            if (declaredParentSpan != nullptr) 
                opts.parent = declaredParentSpan->GetContext();
        }

        //if (!httpHeaders || !parentContext.IsValid())
        //{
            //generate new context?
        //}
    }

public:
    CServerSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders)
    : CSpan(spanName, SpanType::ServerSpan, tracerName_)
    {
        setSpanContext(httpHeaders);
        init();
    }

    CServerSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders)
    : CSpan(spanName, SpanType::ServerSpan, tracerName_)
    {
        setSpanContext(httpHeaders);
        init();
    }
};

class CTraceManager : implements ITraceManager, public CInterface
//class CTraceManager : CInterfaceOf<ITraceManager>
{
private:
    bool enabled = true;
    StringAttr moduleName;

    void initTracer(IPropertyTree * traceConfig)
    {
        try
        {
//#ifdef TRACECONFIGDEBUG
            if (!traceConfig || !traceConfig->hasProp("tracing"))
            {
                const char * simulatedGlobalYaml = R"!!(global:
    tracing:
        enable: true
        exporter:
          type: OTLP
          endpoint: "localhost:4317"
          useSslCredentials: true
          sslCredentialsCACcert: "ssl-certificate"
        processor:
          batchSpan: true
          simpleSpan: false
    )!!";
                Owned<IPropertyTree> testTree = createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
                traceConfig = testTree->getPropTree("global/tracing");
            }

            StringBuffer xml;
            toXML(traceConfig, xml);
            DBGLOG("traceConfig tree: %s", xml.str());
//#endif

            if (traceConfig && traceConfig->getPropBool("@enable", false))
            {
                DBGLOG("OpenTel tracing enabled");
                using namespace opentelemetry::trace;
                //std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
                //std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::sdk::trace::export::NoopSpanExporter;

                //Currently using InMemorySpanExporter as default, until a noop exporter is available
                std::shared_ptr<opentelemetry::exporter::memory::InMemorySpanData> data;
                std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::exporter::memory::InMemorySpanExporterFactory::Create(data);

                Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
#ifdef TRACECONFIGDEBUG
                StringBuffer xml;
                toXML(exportConfig, xml);
                DBGLOG("exportConfig tree: %s", xml.str());
#endif
                if (exportConfig)
                {
                    StringBuffer exportType;
                    exportConfig->getProp("@type", exportType);
                    DBGLOG("Exporter type: %s", exportType.str());

                    if (!exportType.isEmpty())
                    {
                        if (strcasecmp(exportType.str(), "OS")==0) //To stdout/err
                        {
                            exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
                            DBGLOG("Tracing to stdout/err...");
                        }
                        else if (strcasecmp(exportType.str(), "OTLP")==0)
                        {
                            namespace otlp = opentelemetry::exporter::otlp;

                            otlp::OtlpGrpcExporterOptions opts;
                            StringBuffer endPoint;
                            exportConfig->getProp("@endpoint", endPoint);
                            opts.endpoint = endPoint.str();

                            opts.use_ssl_credentials = exportConfig->getPropBool("@useSslCredentials", false);

                            if (opts.use_ssl_credentials)
                            {
                                StringBuffer sslCACert;
                                exportConfig->getProp("@sslCredentialsCACcert", sslCACert);
                                opts.ssl_credentials_cacert_as_string = sslCACert.str();
                            }

                            exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
                            DBGLOG("Tracing to OTLP (%s)", endPoint.str());
                        }
                        else if (strcasecmp(exportType.str(), "Prometheus")==0)
                            DBGLOG("Tracing to Prometheus currently not supported");
                        else if (strcasecmp(exportType.str(), "HPCC")==0)
                            DBGLOG("Tracing to HPCC JLog currently not supported");
                    }
                    else
                        DBGLOG("Tracing exporter type not specified");
                }

                Owned<IPropertyTree> processorConfig = traceConfig->getPropTree("processor");
                std::unique_ptr<opentelemetry::v1::sdk::trace::SpanProcessor> processor;
                if (exportConfig && exportConfig->getPropBool("@batchSpan", false))
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

    void cleanupTracer()
    {
        std::shared_ptr<opentelemetry::trace::TracerProvider> none;
        opentelemetry::trace::Provider::SetTracerProvider(none);
    }

    nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;

public:
    IMPLEMENT_IINTERFACE;
    CTraceManager(const char * componentName, IPropertyTree * traceConfig)
    {
        moduleName.set(componentName);
        initTracer(traceConfig);

        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        tracer = provider->GetTracer(moduleName.get());
    }

    CTraceManager()
    {
        throw makeStringExceptionV(-1, "TraceManager must be intialized!");
    }

    ISpan * createServerSpan(const char * name, StringArray & httpHeaders) override
    {
        return new CServerSpan(name, moduleName.get(), httpHeaders);
    }

    ISpan * createServerSpan(const char * name, const IProperties * httpHeaders) override
    {
        return new CServerSpan(name, moduleName.get(), httpHeaders);
    }
};

static Singleton<CTraceManager> theTraceManager;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    theTraceManager.destroy();
}

void initTraceManager(const char * componentName, IPropertyTree * config)
{
    theTraceManager.query([=] () { return new CTraceManager(componentName, config); });
}

ITraceManager & queryTraceManager()
{
    return *theTraceManager.query([] () { return new CTraceManager; }); //throws if not initialized
}