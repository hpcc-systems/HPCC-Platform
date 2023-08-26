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

#undef UNIMPLEMENTED //opentelemetry defines UNIMPLEMENTED
#include "opentelemetry/trace/provider.h" //StartSpanOptions
#define UNIMPLEMENTED throw makeStringExceptionV(-1, "UNIMPLEMENTED feature at %s(%d)", sanitizeSourceFile(__FILE__), __LINE__)

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

class CNoOpSpan : public CInterfaceOf<ISpan>
{
    void setSpanAttribute(const char * key, const char * val) override {};
    void setSpanAttributes(const IProperties * attributes) override {};
    void addSpanEvent(const char * eventName) override {};
    void setActive() override {};
    const char * queryTraceID() override { return ""; };
    const char * queryTraceFlags() override { return ""; };
    const char * queryTraceName() override { return ""; };
    const char * querySpanID() override { return ""; };
    const char * querySpanName() override { return ""; };
    const char * queryHPCCGlobalID() override { return ""; };
    const char * queryHPCCCallerID() override { return ""; };
};

class CSpan : public CInterfaceOf<ISpan>
{
public:
    CSpan() : span(nullptr) {};
    ~CSpan()
    {
        if (span != nullptr)
            span->End();
    }

   
    void setActive() override
    {
        if (span != nullptr)
        {
            if (!isEmptyString(tracerName.get()))
            {
                auto provider = opentelemetry::trace::Provider::GetTracerProvider();
                auto tracer = provider->GetTracer(tracerName.get());
                 /* Set the active span. The span will remain active until the returned Scope
                  * object is destroyed.
                  * @param span the span that should be set as the new active span.
                  * @return a Scope that controls how long the span will be active.
                 */
                activeSpanScope = tracer->WithActiveSpan(span);
            }
        }
    }

    void setSpanAttributes(const IProperties * attributes) override
    {
        Owned<IPropertyIterator> iter = attributes->getIterator();
        ForEach(*iter)
        {
            const char * key = iter->getPropKey();
            const char * val = attributes->queryProp(key);
            setSpanAttribute(key, val);
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

    const char * queryTraceID() override { return traceID.get(); };
    const char * queryTraceName() override { return tracerName.get();}
    const char * querySpanID() override { return spanID.get(); };
    const char * queryTraceFlags() override { return traceFlags.get(); }
    const char * querySpanName() override { return name.get(); }
    const char * queryHPCCGlobalID() override { return hpccGlobalId.get(); }
    const char * queryHPCCCallerID() override { return hpccCallerId.get(); }

protected:
    CSpan(const char * spanName, const char * tracerName_)
    {
        name.set(spanName);
        tracerName.set(tracerName_);
    }

    void init(ISpan * parentSpan)
    {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        auto tracer = provider->GetTracer(tracerName.get());

        if (parentSpan != nullptr)
            injectParentSpan(parentSpan);

        span = tracer->StartSpan(name.get(), {}, opts);

        if (span != nullptr)
            storeSpanContext();
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

    void injectParentSpan(ISpan * parentSpan)
    {
        if (parentSpan != nullptr)
        {
            nostd::string_view parentTraceID = parentSpan->queryTraceID();
            opentelemetry::v1::trace::TraceId tid = opentelemetry::trace::propagation::HttpTraceContext::TraceIdFromHex(parentTraceID);

            nostd::string_view parentSpanID = parentSpan->querySpanID();
            opentelemetry::v1::trace::SpanId sid = opentelemetry::trace::propagation::HttpTraceContext::SpanIdFromHex(parentSpanID);

            nostd::string_view parentTraceFlags = parentSpan->queryTraceFlags();
            opentelemetry::v1::trace::TraceFlags tf = opentelemetry::trace::propagation::HttpTraceContext::TraceFlagsFromHex(parentTraceFlags);

            opts.parent = opentel_trace::SpanContext(tid, sid, tf, true);
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
    StringAttr opentelTraceParent;
    StringAttr opentelTraceState;

    opentelemetry::trace::StartSpanOptions opts;
    nostd::shared_ptr<opentelemetry::trace::Span> span;
    //Not sure if we should attempt to support declaring the Active span
    opentelemetry::v1::trace::Scope activeSpanScope = 
      opentelemetry::v1::trace::Scope(opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>(nullptr));
};

class CTransactionSpan : public CSpan
{
private:
    opentelemetry::v1::trace::SpanContext parentContext = opentelemetry::trace::SpanContext::GetInvalid();

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

    //Instead of copying StringArray to IProperties, we could operate directly on the array:
    //        if(stricmp(httHeader, HPCCSemanticConventions::kGLOBALIDHTTPHeader) == 0)
    //        {
    //            hpccGlobalId.set(httpHeaders.item(currentHeaderIndex+1));
    //            continue;
    //        }

    //        if(stricmp(httHeader, HPCCSemanticConventions::kCallerIdHTTPHeader) == 0)
    //        {
    //            hpccCallerId.set(httpHeaders.item(currentHeaderIndex+1));
    //            continue;
    //        }
    //    }

    //    const HPCCStringArrayHttpTextMapCarrier carrier(httpHeaders);
    //    auto globalPropegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
    //    auto currentContext = context::RuntimeContext::GetCurrent();
    //    auto newContext = globalPropegator->Extract(carrier, currentContext);
    //    parentContext = opentelemetry::trace::GetSpan(newContext)->GetContext();
    //    options.parent = parentContext;
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
            auto currentContext = context::RuntimeContext::GetCurrent();
            auto newContext = globalPropegator->Extract(carrier, currentContext);
            parentContext = opentelemetry::trace::GetSpan(newContext)->GetContext();
            opts.parent = parentContext;
        }
    }

public:
    CTransactionSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders, ISpan * parentSpan)
    : CSpan(spanName, tracerName_)
    {
        opts.kind = opentelemetry::trace::SpanKind::kServer;
        setSpanContext(httpHeaders);
        init(parentSpan);
    }

    CTransactionSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders, ISpan * parentSpan)
    : CSpan(spanName, tracerName_)
    {
        opts.kind = opentelemetry::trace::SpanKind::kServer;
        setSpanContext(httpHeaders);
        init(parentSpan);
    }

    bool queryOTParentSpanID(StringAttr & parentSpanId) //override
    {
        if (!parentContext.IsValid())
            return false;

        if (!parentContext.trace_id().IsValid())
            return false;

        char span_id[16] = {0};
        parentContext.span_id().ToLowerBase16(span_id);
        parentSpanId.set(span_id, 16);

        return true;
    }
};

class CInternalSpan : public CSpan
{
public:
    CInternalSpan(const char * spanName, const char * tracerName_, ISpan * parentSpan)
    : CSpan(spanName, tracerName_)
    {
        init(parentSpan);
    }
};

class CClientSpan : public CSpan
{
public:
    CClientSpan(const char * spanName, const char * tracerName_, ISpan * parentSpan)
    : CSpan(spanName, tracerName_)
    {
        opts.kind = opentelemetry::trace::SpanKind::kClient;
        init(parentSpan);
    }

    bool injectClientContext(CHPCCHttpTextMapCarrier * carrier) 
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
    bool injectClientContext(IProperties * httpHeaders)
    {
        if (!httpHeaders)
            return false;

        if (isEmptyString(traceID.get()))
            return false;

        if (isEmptyString(spanID.get()))
            return false;

        if (isEmptyString(traceFlags.get()))
            return false;

        StringBuffer contextHTTPHeader;
        //The traceparent header uses the version-trace_id-parent_id-trace_flags format where:
        //version is always 00. trace_id is a hex-encoded trace id. span_id is a hex-encoded span id. trace_flags is a hex-encoded 8-bit field that contains tracing flags such as sampling, trace level, etc.
        //Example: "traceparent", "00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01"
        contextHTTPHeader.append("00-").append(traceID.get()).append("-").append(spanID.get()).append(traceFlags.get());
        //opentelemetry::trace::propagation::kTraceParent
        httpHeaders->setProp("traceparent", contextHTTPHeader.str());

        StringBuffer traceStateHTTPHeader;
        traceStateHTTPHeader.append("hpcc=").append(spanID.get());
        //opentelemetry::trace::propagation::kTraceState
        httpHeaders->setProp("tracestate", contextHTTPHeader.str());

        if (!isEmptyString(hpccGlobalId.get()))
            httpHeaders->setProp(HPCCSemanticConventions::kGLOBALIDHTTPHeader, hpccGlobalId.get());

        if (!isEmptyString(hpccCallerId.get()))
            httpHeaders->setProp(HPCCSemanticConventions::kCallerIdHTTPHeader, hpccCallerId.get());
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
        //Owned<const IPropertyTree> traceConfig;
        try
        {
            //traceConfig.setown(getComponentConfigSP()->getPropTree("tracing"));

//#ifdef TRACECONFIGDEBUG
            if (!traceConfig || !traceConfig->hasProp("tracing"))
            {
                const char * simulatedGlobalYaml = R"!!(global:
    tracing:
        enable:
          true
        exporter:
          OS: true
        processor:
          batchSpan: true
          simpleSpan: false
    )!!";
                Owned<IPropertyTree> testTree = createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
                traceConfig = testTree->getPropTree("global/tracing");
            }
//#endif
                StringBuffer xml;
                toXML(traceConfig, xml);
                DBGLOG("traceConfig tree: %s", xml.str());

            if (traceConfig && traceConfig->getPropBool("@enable", false))
            {
                DBGLOG("OpenTel tracing enabled");
                using namespace opentelemetry::trace;
                std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();

                Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
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
                        DBGLOG("Tracing to OTLP currently not supported");
                    }
                    else if (exportConfig->getPropBool("@Prometheus", false))
                        DBGLOG("Tracing to Prometheus currently not supported");
                    else if (exportConfig->getPropBool("@HPCC", false)) 
                        DBGLOG("Tracing to HPCC JLog  currently not supported");
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

    CTraceManager() {}

    ISpan * createServerSpan(const char * name, StringArray & httpHeaders, ISpan * parentSpan) override
    {
        if (!enabled)
            return new CNoOpSpan();

        return new CTransactionSpan(name, moduleName.get(), httpHeaders, parentSpan);
    }

    ISpan * createServerSpan(const char * name, const IProperties * httpHeaders, ISpan * parentSpan) override
    {
        if (!enabled)
            return new CNoOpSpan();

        return new CTransactionSpan(name, moduleName.get(), httpHeaders, parentSpan);
    }

    ISpan * createClientSpan(const char * name, ISpan * parentSpan) override
    {
        if (!enabled)
            return new CNoOpSpan();

        return new CClientSpan(name, moduleName.get(), parentSpan);
    }

    ISpan * createInternalSpan(const char * name, ISpan * parentSpan) override
    {
        if (!enabled)
            return new CNoOpSpan();

        return new CInternalSpan(name, moduleName.get(), parentSpan);
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
    //return theTraceManager.query([=] () { return new CTraceManager(iHateThis.get()); });
    return *theTraceManager.query([] () { return new CTraceManager; });
}