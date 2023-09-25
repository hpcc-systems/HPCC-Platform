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

class CHPCCHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    CHPCCHttpTextMapCarrier(const IProperties * httpHeaders)
    {
        if (httpHeaders)
        {
            this->httpHeaders.setown(createProperties(true));
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
        httpHeaders.setown(createProperties(true));
    };

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();

        if (theKey.empty() || httpHeaders == nullptr || !httpHeaders->hasProp(theKey.c_str()))
            return "";

        return httpHeaders->queryProp(theKey.c_str());
    }

    virtual void Set(opentelemetry::nostd::string_view key, opentelemetry::nostd::string_view value) noexcept override
    {
        httpHeaders->setProp(std::string(key).c_str(), std::string(value).c_str());        
    }

private:
    Owned<IProperties> httpHeaders;
};

class CSpan : public CInterfaceOf<ISpan>
{
public:
    CSpan() = delete;
    ~CSpan()
    {
        if (span != nullptr)
            span->End();
    }

    virtual void beforeDispose() override
    {
        StringBuffer out;
        toLog(out);
        DBGLOG("Span end: {%s}", out.str());
    }

    const char * getSpanID() const
    {
        return spanID.get();
    }

    ISpan * createClientSpan(const char * name) override;
    ISpan * createInternalSpan(const char * name) override;

    void toLog(StringBuffer & out) const override
    {
        out.append(",\"Name\":\"").append(name.get()).append("\"");

        if (!isEmptyString(hpccGlobalId.get()))
            out.append(",\"GlobalID\":\"").append(hpccGlobalId.get()).append("\"");
        if (!isEmptyString(hpccCallerId.get()))
            out.append(",\"CallerID\":\"").append(hpccCallerId.get()).append("\"");
        if (!isEmptyString(hpccLocalId.get()))
            out.append(",\"LocalID\":\"").append(hpccLocalId.get()).append("\"");

        if (span != nullptr)
        {
            out.append(",\"SpanID\":\"").append(spanID.get()).append("\"");

            out.append(",\"TraceID\":\"").append(traceID.get()).append("\"");

            if (localParentSpan != nullptr)
            {
                out.append(",\"ParentSpanID\": \"");
                out.append(localParentSpan->getSpanID());
                out.append("\"");
            }
        }
    }

    virtual void toString(StringBuffer & out) const
    {
        toString(out, true);
    }

    virtual void toString(StringBuffer & out, bool isLeaf) const
    {
        out.append(",\"Name\":\"").append(name.get()).append("\"");
        if (isLeaf)
        {
            if (!isEmptyString(hpccGlobalId.get()))
                out.append(",\"HPCCGlobalID\":\"").append(hpccGlobalId.get()).append("\"");
            if (!isEmptyString(hpccCallerId.get()))
                out.append(",\"HPCCCallerID\":\"").append(hpccCallerId.get()).append("\"");
        }

        if (span != nullptr)
        {
            out.append(",\"SpanID\":\"").append(spanID.get()).append("\"");

            if (isLeaf)
            {
                out.append(",\"TraceID\":\"").append(traceID.get()).append("\"")
                 .append(",\"TraceFlags\":\"").append(traceFlags.get()).append("\"");
            }

            if (localParentSpan != nullptr)
            {
                out.append(",\"ParentSpan\":{ ");
                localParentSpan->toString(out, false);
                out.append(" }");
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

    bool getSpanContext(CHPCCHttpTextMapCarrier * carrier) const
    {
        if (!carrier)
            return false;

        auto propagator = opentelemetry::context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();

        opentelemetry::context::Context emptyCtx;
        auto spanCtx = SetSpan(emptyCtx, span);

        //and have the propagator inject the ctx into carrier
        propagator->Inject(*carrier, spanCtx);

        if (!isEmptyString(hpccGlobalId.get()))
            carrier->Set(kGlobalIdHttpHeaderName, hpccGlobalId.get());

        if (!isEmptyString(hpccCallerId.get()))
            carrier->Set(kCallerIdHttpHeaderName, hpccCallerId.get());

        return true;
    }

    /**
     * Retrieves the Span's context as key/value pairs into the provided IProperties.
     * Optionally, output follows OpenTelemetry Span context format for propogation 
     * accross process boundaries.
     *
     * @param ctxProps IProperties container for span context key/value pairs.
     * @param otelFormatted If true, output follows OpenTelemetry Span context format.
     * @return True if the span context was successfully retrieved, false otherwise.
     */
    bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const override
    {
        if (ctxProps == nullptr)
            return false;

        if (!isEmptyString(hpccGlobalId.get()))
            ctxProps->setProp(kGlobalIdHttpHeaderName, hpccGlobalId.get());

        if (otelFormatted)
        {
            //The localid is passed as the callerid for the client request....
            if (!isEmptyString(hpccLocalId.get()))
                ctxProps->setProp(kCallerIdHttpHeaderName, hpccLocalId.get());
        }
        else
        {
            if (!isEmptyString(hpccCallerId.get()))
                ctxProps->setProp(kCallerIdHttpHeaderName, hpccCallerId.get());
        }

        if (span == nullptr)
            return false;

        if (otelFormatted)
        {
            if (isEmptyString(traceID.get()) || isEmptyString(spanID.get()) || isEmptyString(traceFlags.get()))
                return false;

            //The traceparent header uses the version-trace_id-parent_id-trace_flags format where:
            //version is always 00. trace_id is a hex-encoded trace id. span_id is a hex-encoded span id. trace_flags is a hex-encoded 8-bit field that contains tracing flags such as sampling, trace level, etc.
            //Example: "traceparent", "00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01"

            StringBuffer contextHTTPHeader;
            //https://www.w3.org/TR/trace-context/#header-name
            contextHTTPHeader.append("00-").append(traceID.get()).append("-").append(spanID.get()).append("-").append(traceFlags.get());
            ctxProps->setProp(opentelemetry::trace::propagation::kTraceParent.data(), contextHTTPHeader.str());

            //The main purpose of the tracestate HTTP header is to provide additional vendor-specific trace identification
            // information across different distributed tracing systems and is a companion header for the traceparent field.
            // It also conveys information about the request’s position in multiple distributed tracing graphs.

            //https://www.w3.org/TR/trace-context/#trace-context-http-headers-format
            //StringBuffer traceStateHTTPHeader;
            //traceStateHTTPHeader.append("hpcc=").append(spanID.get());

            ctxProps->setProp(opentelemetry::trace::propagation::kTraceState.data(), span->GetContext().trace_state()->ToHeader().c_str());
        }
        else
        {
            if (!isEmptyString(traceID.get()))
                ctxProps->setProp("traceID", traceID.get());
            if (!isEmptyString(spanID.get()))
                ctxProps->setProp("spanID", spanID.get());
            if (!isEmptyString(traceFlags.get()))
                ctxProps->setProp("traceFlags", traceFlags.get());

            if (localParentSpan != nullptr)
            {
                Owned<IProperties> localParentSpanCtxProps = createProperties();
                localParentSpan->getSpanContext(localParentSpanCtxProps, false);
                if (localParentSpanCtxProps)
                {
                    if (localParentSpanCtxProps->hasProp("spanID"))
                        ctxProps->setProp("localParentSpanID", localParentSpanCtxProps->queryProp("spanID"));
                }
            }
        }

        return true;
    }

    opentelemetry::v1::trace::SpanContext querySpanContext() const
    {
        if (span != nullptr)
            return span->GetContext();

        return opentelemetry::trace::SpanContext::GetInvalid();
    }

    virtual void getLogPrefix(StringBuffer & out) const override
    {
        const char * caller = queryCallerId();
        const char * local = queryLocalId();
        bool hasCaller = !isEmptyString(caller);
        bool hasLocal = !isEmptyString(local);
        if (hasCaller || hasLocal)
        {
            out.append('[');
            if (hasCaller)
                out.appendf("caller:%s", caller);

            if (hasLocal)
            {
                if (hasCaller)
                    out.append(',');
                out.appendf("local:%s", local);
            }
            out.append("]");
        }
    }

    const char * queryTraceName() const
    {
        return tracerName.get();
    }

    virtual const char* queryGlobalId() const override
    {
        //MORE: This should probably only be stored in the server context....
        if (localParentSpan && isEmptyString(hpccGlobalId))
            return localParentSpan->queryGlobalId();
        return hpccGlobalId.get();
    }

    virtual const char* queryCallerId() const override
    {
        //MORE: This should probably only be stored in the server context....
        if (localParentSpan && isEmptyString(hpccCallerId))
            return localParentSpan->queryCallerId();
        return hpccCallerId.get();
    }

    virtual const char* queryLocalId() const override
    {
        //MORE: This should probably only be stored in the server context....
        if (localParentSpan && isEmptyString(hpccLocalId))
            return localParentSpan->queryLocalId();
        return hpccLocalId.get();
    }

protected:
    CSpan(const char * spanName, CSpan * parent)
    {
        name.set(spanName);
        localParentSpan = parent;
        if (localParentSpan != nullptr)
            tracerName.set(parent->queryTraceName());
    }

    CSpan(const char * spanName, const char * nameOfTracer)
    {
        name.set(spanName);
        localParentSpan = nullptr;
        tracerName.set(nameOfTracer);
    }

    void init()
    {
        bool createLocalId = !isEmptyString(hpccGlobalId);
        if (createLocalId)
            hpccLocalId.set(ln_uid::createUniqueIdString().c_str());

        auto provider = opentelemetry::trace::Provider::GetTracerProvider();

        //what if tracerName is empty?
        auto tracer = provider->GetTracer(tracerName.get());

        if (localParentSpan != nullptr)
            injectlocalParentSpan(localParentSpan);

        span = tracer->StartSpan(name.get(), {}, opts);

        if (span != nullptr)
        {
            storeSpanContext();

            StringBuffer out;
            toLog(out);
            DBGLOG("Span start: {%s}", out.str());
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

    void injectlocalParentSpan(CSpan * localParentSpan)
    {
        if (localParentSpan == nullptr)
            throw makeStringExceptionV(-1, "injectlocalParentSpan*(): null localParentSpan detected!");

        auto localParentSpanCtx = localParentSpan->querySpanContext();
        if(localParentSpanCtx.IsValid())
            opts.parent = localParentSpanCtx;
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
    StringAttr hpccLocalId;

    opentelemetry::trace::StartSpanOptions opts;
    nostd::shared_ptr<opentelemetry::trace::Span> span;

    //local nested span parent
    CSpan * localParentSpan = nullptr;
};

class CInternalSpan : public CSpan
{
public:
    CInternalSpan(const char * spanName, CSpan * parent)
    : CSpan(spanName, parent)
    {
        opts.kind = opentelemetry::trace::SpanKind::kInternal;
        init();
    }

    void toLog(StringBuffer & out) const override
    {
        out.append("\"Type\":\"Internal\"");
        CSpan::toLog(out);
    }

    void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Internal\"");
        CSpan::toString(out, isLeaf);
    }
};

class CClientSpan : public CSpan
{
public:
    CClientSpan(const char * spanName, CSpan * parent)
    : CSpan(spanName, parent)
    {
        opts.kind = opentelemetry::trace::SpanKind::kClient;
        init();
    }

    void toLog(StringBuffer & out) const override
    {
        out.append("\"Type\":\"Client\"");
        CSpan::toLog(out);
    }

    void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Client\"");
        CSpan::toString(out, isLeaf);
    }
};

ISpan * CSpan::createClientSpan(const char * name)
{
    return new CClientSpan(name, this);
}

ISpan * CSpan::createInternalSpan(const char * name)
{
    return new CInternalSpan(name, this);
}

class CServerSpan : public CSpan
{
private:
    //Remote parent is declared via http headers from client call
    opentelemetry::v1::trace::SpanContext remoteParentSpanCtx = opentelemetry::trace::SpanContext::GetInvalid();

    void setSpanContext(StringArray & httpHeaders, const char kvDelineator, SpanFlags flags)
    {
        Owned<IProperties> contextProps = createProperties(true);
        ForEachItemIn(currentHeaderIndex, httpHeaders)
        {
            const char* httpHeader = httpHeaders.item(currentHeaderIndex);
            if(!httpHeader)
                continue;

            const char* delineator = strchr(httpHeader, kvDelineator);
            if(delineator == nullptr)
                continue;

            StringBuffer key;
            key.append(delineator - httpHeader, httpHeader);

            contextProps->setProp(key, delineator + 1);
        }

        setSpanContext(contextProps, flags);
    }

    void setSpanContext(const IProperties * httpHeaders, SpanFlags flags)
    {
        if (httpHeaders)
        {
            // perform any key mapping needed...
            //Instrumented http client/server Capitalizes the first letter of the header name
            //if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
            //    theKey[0] = toupper(theKey[0]);

            if (httpHeaders->hasProp(kGlobalIdHttpHeaderName))
                hpccGlobalId.set(httpHeaders->queryProp(kGlobalIdHttpHeaderName));
            else if (httpHeaders->hasProp(kLegacyGlobalIdHttpHeaderName))
                hpccGlobalId.set(httpHeaders->queryProp(kLegacyGlobalIdHttpHeaderName));
            else if (hasMask(flags, SpanFlags::EnsureGlobalId) || queryTraceManager().alwaysCreateGlobalIds())
            {
                StringBuffer generatedId;
                appendGloballyUniqueId(generatedId);
                hpccGlobalId.set(generatedId.str());
            }

            if (httpHeaders->hasProp(kCallerIdHttpHeaderName))
                hpccCallerId.set(httpHeaders->queryProp(kCallerIdHttpHeaderName));
            else if (httpHeaders->hasProp(kLegacyCallerIdHttpHeaderName))
                hpccCallerId.set(httpHeaders->queryProp(kLegacyCallerIdHttpHeaderName));

            const CHPCCHttpTextMapCarrier carrier(httpHeaders);
            auto globalPropegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
            //avoiding getCurrent: https://github.com/open-telemetry/opentelemetry-cpp/issues/1467
            opentelemetry::context::Context empty;
            auto wrappedSpanCtx = SetSpan(empty, span);
            opentelemetry::v1::context::Context runtimeCtx = globalPropegator->Extract(carrier, wrappedSpanCtx);
            opentelemetry::v1::nostd::shared_ptr<opentelemetry::v1::trace::Span> remoteParentSpan = opentelemetry::trace::GetSpan(runtimeCtx);

            if (remoteParentSpan != nullptr && remoteParentSpan->GetContext().IsValid())
            {
                remoteParentSpanCtx = remoteParentSpan->GetContext();
                opts.parent = remoteParentSpanCtx;
            }
        }

        //Generate new HPCCGlobalID if not provided
    }

    bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const override
    {
        bool success = CSpan::getSpanContext(ctxProps, otelFormatted);

        if (remoteParentSpanCtx.IsValid())
        {
            StringBuffer remoteParentSpanID;
            char remoteParentSpanId[16] = {0};
            remoteParentSpanCtx.span_id().ToLowerBase16(remoteParentSpanId);
            remoteParentSpanID.append(16, remoteParentSpanId);
            ctxProps->setProp("remoteParentSpanID", remoteParentSpanID.str());
        }

        return success;
    }

public:
    CServerSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders, SpanFlags flags)
    : CSpan(spanName, tracerName_)
    {
        opts.kind = opentelemetry::trace::SpanKind::kServer;
        setSpanContext(httpHeaders, ':', flags);
        init();
    }

    CServerSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders, SpanFlags flags)
    : CSpan(spanName, tracerName_)
    {
        opts.kind = opentelemetry::trace::SpanKind::kServer;
        setSpanContext(httpHeaders, flags);
        init();
    }

    void toLog(StringBuffer & out) const override
    {
        out.append("\"Type\":\"Server\"");
        CSpan::toLog(out);

        if (remoteParentSpanCtx.IsValid())
        {
            out.append(",\"ParentSpanID\":\"");
            char spanId[16] = {0};
            remoteParentSpanCtx.span_id().ToLowerBase16(spanId);
            out.append(16, spanId)
            .append("\"");
        }
    }
    void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Server\"");
        CSpan::toString(out, isLeaf);

        if (remoteParentSpanCtx.IsValid())
        {
            out.append(",\"ParentSpanID\":\"");
            char spanId[16] = {0};
            remoteParentSpanCtx.span_id().ToLowerBase16(spanId);
            out.append(16, spanId)
            .append("\"");
        }
    }
};

//---------------------------------------------------------------------------------------------------------------------

IProperties * getClientHeaders(const ISpan * span)
{
    Owned<IProperties> headers = createProperties(true);
    span->getSpanContext(headers, true);      // Return value is not helpful
    return headers.getClear();
}

//---------------------------------------------------------------------------------------------------------------------

class CTraceManager : implements ITraceManager, public CInterface
{
private:
    bool enabled = true;
    bool optAlwaysCreateGlobalIds = false;
    StringAttr moduleName;

    //Initializes the global trace provider which is required for all Otel based tracing operations.
    //The trace provider is initialized with a SpanProcessor, which is responsible for processing
    //and an exporter which is responsible for exporting the processed spans.
    //By default, an InMemorySpan exporter is used in the absence of a proper noop exporter.
    //Also, a SimpleSpanProcessor is used in the absence of a configuration directive to process spans in batches
    void initTracerProviderAndGlobalInternals(IPropertyTree * traceConfig)
    {
        //Currently using InMemorySpanExporter as default, until a noop exporter is available
        std::shared_ptr<opentelemetry::exporter::memory::InMemorySpanData> data;
        std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = opentelemetry::exporter::memory::InMemorySpanExporterFactory::Create(data);

        //Administrators can choose to export trace data to a different backend by specifying the exporter type
        if (traceConfig && traceConfig->hasProp("exporter"))
        {
            Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
            if (exportConfig)
            {
                StringBuffer exportType;
                exportConfig->getProp("@type", exportType);
                DBGLOG("Exporter type: %s", exportType.str());

                if (!exportType.isEmpty())
                {
                    if (stricmp(exportType.str(), "OS")==0) //To stdout/err
                    {
                        exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
                        DBGLOG("Tracing to stdout/err...");
                    }
                    else if (stricmp(exportType.str(), "OTLP")==0)
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
                    else if (stricmp(exportType.str(), "Prometheus")==0)
                        DBGLOG("Tracing to Prometheus currently not supported");
                    else if (stricmp(exportType.str(), "HPCC")==0)
                        DBGLOG("Tracing to HPCC JLog currently not supported");
                }
                else
                    DBGLOG("Tracing exporter type not specified");
            }
        }

        //Administrator can choose to process spans in batches or one at a time
        std::unique_ptr<opentelemetry::v1::sdk::trace::SpanProcessor> processor;
        if (traceConfig && traceConfig->hasProp("processor/@type"))
        {
            StringBuffer processorType;
            bool foundProcessorType = traceConfig->getProp("processor/@type", processorType);
        
            if (foundProcessorType &&  strcmp("batch", processorType.str())==0)
            {
                //Groups several spans together, before sending them to an exporter.
                //These options should be configurable
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
    }

    /*
    Expected Configuration format:
    global:
        tracing:                            #optional - tracing enabled by default
            disable: true                   #optional - disable OTel tracing
            alwaysCreateGlobalIds : false   #optional - should global ids always be created?
            exporter:                       #optional - Controls how trace data is exported/reported
              type: OTLP                    #OS|OTLP|Prometheus|HPCC (default: no export, jlog entry)
              endpoint: "localhost:4317"    #exporter specific key/value pairs
              useSslCredentials: true
              sslCredentialsCACcert: "ssl-certificate"
            processor:                      #optional - Controls span processing style
              type: batch                   #simple|batch (default: simple)
    */
    void initTracer(IPropertyTree * traceConfig)
    {
        try
        {
#ifdef TRACECONFIGDEBUG
            Owned<IPropertyTree> testTree;
            if (!traceConfig || !traceConfig->hasProp("tracing"))
            {
                const char * simulatedGlobalYaml = R"!!(global:
    tracing:
        disable: true
        exporter:
          type: OTLP
          endpoint: "localhost:4317"
          useSslCredentials: true
          sslCredentialsCACcert: "ssl-certificate"
        processor:
          type: batch
    )!!";
                testTree.setown(createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr));
                traceConfig = testTree->queryPropTree("global/tracing");
            }

            StringBuffer xml;
            toXML(traceConfig, xml);
            DBGLOG("traceConfig tree: %s", xml.str());
#endif
            bool disableTracing = traceConfig && traceConfig->getPropBool("@disable", false);

            using namespace opentelemetry::trace;
            if (disableTracing)
            {
                //Set noop global trace provider
                static nostd::shared_ptr<TracerProvider> noopProvider(new NoopTracerProvider);
                opentelemetry::trace::Provider::SetTracerProvider(noopProvider);
                enabled = false;
            }
            else
            {
                initTracerProviderAndGlobalInternals(traceConfig);
            }

            //Non open-telemetry tracing configuration
            if (traceConfig)
            {
                optAlwaysCreateGlobalIds = traceConfig->getPropBool("@alwaysCreateGlobalIds", optAlwaysCreateGlobalIds);
            }

            // The global propagator should be set regardless of whether tracing is enabled or not.
            // Injects Context into and extracts it from carriers that travel in-band
            // across process boundaries. Encoding is expected to conform to the HTTP
            // Header Field semantics.
            // Values are often encoded as RPC/HTTP request headers.
            opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
                opentelemetry::nostd::shared_ptr<opentelemetry::context::propagation::TextMapPropagator>(
                    new opentelemetry::trace::propagation::HttpTraceContext()));

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

    ISpan * createServerSpan(const char * name, StringArray & httpHeaders, SpanFlags flags) override
    {
        return new CServerSpan(name, moduleName.get(), httpHeaders, flags);
    }

    ISpan * createServerSpan(const char * name, const IProperties * httpHeaders, SpanFlags flags) override
    {
        return new CServerSpan(name, moduleName.get(), httpHeaders, flags);
    }

    const char * getTracedComponentName() const override
    {
        return moduleName.get();
    }

    bool isTracingEnabled() const override
    {
        return enabled;
    }

    virtual bool alwaysCreateGlobalIds() const
    {
        return optAlwaysCreateGlobalIds;
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
