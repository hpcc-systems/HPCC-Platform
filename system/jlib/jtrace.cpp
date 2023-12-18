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
#include "opentelemetry/exporters/ostream/common_utils.h"
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
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/exporters/memory/in_memory_span_data.h"

#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/span_data.h"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jtrace.hpp"
#include "lnuid.h"
#include <variant>

namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;
namespace opentel_trace = opentelemetry::trace;

using namespace ln_uid;

/*
NoopSpanExporter is a SpanExporter that does not do anything.
Use when trace/span data is not needed, or if tracing data is handled outside of Otel framework
*/
class NoopSpanExporter final : public opentelemetry::sdk::trace::SpanExporter
{
public:
    NoopSpanExporter() {}

    /**
    * @return Returns a unique pointer to an empty recordable object
    */
    virtual std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
    {
        return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(new opentelemetry::sdk::trace::SpanData());
    }

    /**
    * NoopSpanExporter does not need to do anything here.
    *
    * @param recordables Ignored
    * @return Always returns success
    */
    opentelemetry::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>> &recordables) noexcept override
    {
        return opentelemetry::sdk::common::ExportResult::kSuccess;
    }

   /**
   * Shut down the exporter. NoopSpanExporter does not need to do anything here.
   * @param timeout an optional timeout.
   * @return return the status of the operation.
   */
    virtual bool Shutdown(
      std::chrono::microseconds timeout = std::chrono::microseconds::max()) noexcept override
    {
        return true;
    }
};

class NoopSpanExporterFactory
{
public:
    /**
    * Create a NoopSpanExporter.
    */
    static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Create()
    {
        return std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
            new NoopSpanExporter());
    }
};

/**
 * Converts an OpenTelemetry span status to its string representation.
 * These are OTel span status defined in include/opentelemetry/trace/span_metadata.h
 *
 * @param StatusCode The OpenTelemetry span status code to translate.
 * @return The string representation of the OpenTelemetry status code.
 */
static const char * spanStatusToString(opentelemetry::trace::StatusCode spanStatus)
{
    switch(spanStatus)
    {
        case opentelemetry::trace::StatusCode::kUnset:
            return "Unset";
        case opentelemetry::trace::StatusCode::kOk:
            return "Ok";
        case opentelemetry::trace::StatusCode::kError:
            return "Error";
        default:
            return "Unknown";
    }
}

/**
 * Converts an OpenTelemetry span kind to its string representation.
 * These are OTel span kinds defined in include/opentelemetry/trace/span_metadata.h,
 * not HPCC JLib CSpan kinds
 *
 * @param spanKind The OpenTelemetry span kind to convert.
 * @return The string representation of the OpenTelemetry span kind.
 */
static const char * spanKindToString(opentelemetry::trace::SpanKind spanKind)
{
    switch (spanKind)
    {
        case opentelemetry::trace::SpanKind::kClient:
            return "Client";
        case opentelemetry::trace::SpanKind::kServer:
            return "Server";
        case opentelemetry::trace::SpanKind::kProducer:
            return "Producer";
        case opentelemetry::trace::SpanKind::kConsumer:
            return "Consumer";
        case opentelemetry::trace::SpanKind::kInternal:
            return "Internal";
        default:
            return "Unknown";
    }
}

class JLogSpanExporter final : public opentelemetry::sdk::trace::SpanExporter
{
public:
    JLogSpanExporter(SpanLogFlags spanLogFlags) : logFlags(spanLogFlags), shutDown(false) {}

    /**
    * @return Returns a unique pointer to an empty recordable object
    */
    virtual std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
    {
        return std::unique_ptr<opentelemetry::sdk::trace::Recordable>(new opentelemetry::sdk::trace::SpanData());
    }

    /**
    * Export - Formats recordable spans in HPCC Jlog format and reports to JLog
    *
    * @param recordables
    * @return Always returns success
    */
    opentelemetry::sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>> &recordables) noexcept override
    {
        if (isShutDown())
            return opentelemetry::sdk::common::ExportResult::kFailure;

        for (auto &recordable : recordables)
        {
            //Casting the recordable object to the type of the object that was previously created by
            //JLogSpanExporter::MakeRecordable() - 
            auto span = std::unique_ptr<opentelemetry::sdk::trace::SpanData>(
            static_cast<opentelemetry::sdk::trace::SpanData *>(recordable.release()));

            if (span != nullptr)
            {
                char traceID[32]       = {0};
                char spanID[16]        = {0};

                span->GetTraceId().ToLowerBase16(traceID);
                span->GetSpanId().ToLowerBase16(spanID);

                StringBuffer out("{ \"type\": \"span\""); //for simple identification in log scraping
                out.appendf(", \"name\": \"%s\"", span->GetName().data());
                out.append(", \"trace_id\": \"").append(32, traceID).append("\"");
                out.append(", \"span_id\": \"").append(16, spanID).append("\"");
                out.appendf(", \"start\": %lld", (long long)(span->GetStartTime().time_since_epoch()).count());
                out.appendf(", \"duration\": %lld", (long long)span->GetDuration().count());

                if (hasMask(logFlags, SpanLogFlags::LogParentInfo))
                {
                    if (span->GetParentSpanId().IsValid())
                    {
                        char parentSpanID[16]  = {0};
                        span->GetParentSpanId().ToLowerBase16(parentSpanID);
                        out.append(", \"parent_span_id\": \"").append(16, parentSpanID).append("\"");
                    }

                    std::string traceStatestr = span->GetSpanContext().trace_state()->ToHeader();
                    if (!traceStatestr.empty())
                        out.appendf(", \"trace_state\": \"%s\"", traceStatestr.c_str());
                }

                if (hasMask(logFlags, SpanLogFlags::LogSpanDetails))
                {
                    out.appendf(", \"status\": \"%s\"", spanStatusToString(span->GetStatus()));
                    out.appendf(", \"kind\": \"%s\"", spanKindToString(span->GetSpanKind()));
                    const char * description = span->GetDescription().data();
                    if (!isEmptyString(description))
                    {
                        StringBuffer encoded;
                        encodeJSON(encoded, description);
                        out.appendf(", \"description\": \"%s\"", encoded.str());
                    }
                    printInstrumentationScope(out, span->GetInstrumentationScope());
                }

                if (hasMask(logFlags, SpanLogFlags::LogAttributes))
                    printAttributes(out, span->GetAttributes());

                if (hasMask(logFlags, SpanLogFlags::LogEvents))
                    printEvents(out, span->GetEvents());

                if (hasMask(logFlags, SpanLogFlags::LogLinks))
                    printLinks(out, span->GetLinks());

                if (hasMask(logFlags, SpanLogFlags::LogResources))
                    printResources(out, span->GetResource());

                out.append(" }");
                LOG(MCmonitorEvent, "%s",out.str());
            }
        }
        return opentelemetry::sdk::common::ExportResult::kSuccess;
    }

   /**
   * Shut down the exporter.
   * @param timeout an optional timeout.
   * @return return the status of the operation.
   */
    virtual bool Shutdown(
      std::chrono::microseconds timeout = std::chrono::microseconds::max()) noexcept override
    {
        shutDown = true;
        return true;
    }

private:
    bool isShutDown() const noexcept
    {
        return shutDown;
    }

public:
    static void printAttributes(StringBuffer & out, const std::unordered_map<std::string, opentelemetry::sdk::common::OwnedAttributeValue> &map, const char * attsContainerName = "attributes")
    {
        if (map.size() == 0)
            return;

        try
        {
            out.appendf(", \"%s\": {", attsContainerName);

            bool first = true;
            for (const auto &kv : map)
            {
                if (!first)
                    out.append(",");
                else
                    first = false;

                std::ostringstream attsOS; //used to exploit OTel convenience functions for printing attribute values
                opentelemetry::exporter::ostream_common::print_value(kv.second, attsOS);
                std::string val = attsOS.str();
                if (val.size() > 0)
                {
                    StringBuffer encoded;
                    encodeJSON(encoded, val.c_str());
                    out.appendf("\"%s\": \"%s\"", kv.first.c_str(), encoded.str());
                }
            }
            out.append(" }");

        }
        catch(const std::bad_variant_access & e)
        {
            ERRLOG("Could not export span %s: %s", attsContainerName, e.what());
        }
    }

    static void printEvents(StringBuffer & out, const std::vector<opentelemetry::sdk::trace::SpanDataEvent> &events)
    {
        if (events.size() == 0)
            return;

        out.append(", \"events\":[ ");
        bool first = true;
        for (const auto &event : events)
        {
            if (!first)
                out.append(",");
            else
                first = false;

            out.append("{ \"name\": \"").append(event.GetName().data()).append("\"");
            out.appendf(", \"time_stamp\": %lld", (long long)event.GetTimestamp().time_since_epoch().count());

            printAttributes(out, event.GetAttributes());
            out.append(" }");
        }

        out.append(" ]");
    }

    static void printLinks(StringBuffer & out, const std::vector<opentelemetry::sdk::trace::SpanDataLink> &links)
    {
        if (links.size() == 0)
            return;

        bool first = true;

        out.append(",  \"links\": [");
        for (const auto &link : links)
        {
            if (!first)
                out.append(",");
            else
                first = false;

            char traceID[32] = {0};
            char spanID[16]  = {0};
            link.GetSpanContext().trace_id().ToLowerBase16(traceID);
            link.GetSpanContext().span_id().ToLowerBase16(spanID);

            out.append(" { \"trace_id\": \"").append(32, traceID).append("\",");
            out.append(" \"span_id\": \"").append(16, spanID).append("\",");
            out.append(" \"trace_state\": \"").append(link.GetSpanContext().trace_state()->ToHeader().c_str()).append("\"");
            printAttributes(out, link.GetAttributes());
        }
        out.append(" ]");
    }

    static void printResources(StringBuffer & out, const opentelemetry::sdk::resource::Resource &resources)
    {
        if (resources.GetAttributes().size())
            printAttributes(out, resources.GetAttributes(), "resources");
    }

    static void printInstrumentationScope(StringBuffer & out,
        const opentelemetry::sdk::instrumentationscope::InstrumentationScope &instrumentation_scope)
    {
        out.appendf(", \"instrumented_library\": \"%s\"", instrumentation_scope.GetName().c_str());
        if (instrumentation_scope.GetVersion().size())
            out.appendf("-").append(instrumentation_scope.GetVersion().c_str());
    }

private:
    SpanLogFlags logFlags = SpanLogFlags::LogNone;
    std::atomic_bool shutDown;
};

/*#ifdef _USE_CPPUNIT
void testJLogExporterPrintAttributes(StringBuffer & out, const std::unordered_map<std::string, opentelemetry::sdk::common::OwnedAttributeValue> & map, const char * attsContainerName)
{
    JLogSpanExporter::printAttributes(out, map, attsContainerName);
}

void testJLogExporterPrintResources(StringBuffer & out, const opentelemetry::sdk::resource::Resource &resources)
{
    JLogSpanExporter::printResources(out, resources);
}
#endif
*/
class JLogSpanExporterFactory
{
public:
    /**
    * Create a JLogSpanExporter.
    */
    static std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> Create(SpanLogFlags logFlags)
    {
        return std::unique_ptr<opentelemetry::sdk::trace::SpanExporter>(
            new JLogSpanExporter(logFlags));
    }
};

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
                const char * val = iter->queryPropValue();
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

//---------------------------------------------------------------------------------------------------------------------

class CTraceManager : implements ITraceManager, public CInterface
{
private:
    bool enabled = true;
    bool optAlwaysCreateGlobalIds = false;
    bool optAlwaysCreateTraceIds = true;
    StringAttr moduleName;
    nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;

    void initTracerProviderAndGlobalInternals(const IPropertyTree * traceConfig);
    void initTracer(const IPropertyTree * traceConfig);
    void cleanupTracer();

public:
    CTraceManager(const char * componentName, const IPropertyTree * componentConfig, const IPropertyTree * globalConfig);
    CTraceManager();
    IMPLEMENT_IINTERFACE

    virtual ISpan * createServerSpan(const char * name, StringArray & httpHeaders, SpanFlags flags) const override;
    virtual ISpan * createServerSpan(const char * name, const IProperties * httpHeaders, SpanFlags flags) const override;

    virtual bool isTracingEnabled() const override
    {
        return enabled;
    }

// Internal public inteface
    bool alwaysCreateGlobalIds() const
    {
        return optAlwaysCreateGlobalIds;
    }

    bool alwaysCreateTraceIds() const
    {
        return optAlwaysCreateTraceIds;
    }

    nostd::shared_ptr<opentelemetry::trace::Tracer> queryTracer() const
    {
        return tracer;
    }
};

static Singleton<CTraceManager> theTraceManager;

//What is the global trace manager?  Only valid to call within this module from spans
//Which can only be created if the trace manager has been initialized
static inline CTraceManager & queryInternalTraceManager() { return *theTraceManager.query(); }

//---------------------------------------------------------------------------------------------------------------------

class CSpan : public CInterfaceOf<ISpan>
{
public:
    CSpan() = delete;

    virtual void beforeDispose() override
    {
        //Record the span as complete before we output the logging for the end of the span
        if (span != nullptr)
            span->End();
    }

    const char * getSpanID() const
    {
        return spanID.get();
    }

    ISpan * createClientSpan(const char * name) override;
    ISpan * createInternalSpan(const char * name) override;

    virtual void toString(StringBuffer & out) const
    {
        toString(out, true);
    }

    virtual void toString(StringBuffer & out, bool isLeaf) const
    {
        out.append(",\"Name\":\"").append(name.get()).append("\"");

        if (span != nullptr)
        {
            out.append(",\"SpanID\":\"").append(spanID.get()).append("\"");
        }

        if (isLeaf)
        {
            out.append(",\"TraceID\":\"").append(traceID.get()).append("\"")
                .append(",\"TraceFlags\":\"").append(traceFlags.get()).append("\"");
        }
    };

    void setSpanAttributes(const IProperties * attributes) override
    {
        Owned<IPropertyIterator> iter = attributes->getIterator();
        ForEach(*iter)
        {
            const char * key = iter->getPropKey();
            setSpanAttribute(key, iter->queryPropValue());
        }
    }

    void setSpanAttribute(const char * key, const char * val) override
    {
        if (span && !isEmptyString(key) && !isEmptyString(val))
            span->SetAttribute(key, val);
    }

    void addSpanEvent(const char * eventName, IProperties * attributes) override 
    {
        if (span && !isEmptyString(eventName))
        {
            std::map<std::string, std::string> attributesMap;
            Owned<IPropertyIterator> iter = attributes->getIterator();
            ForEach(*iter)
            {
                const char * key = iter->getPropKey();
                attributesMap.insert(std::pair<std::string,std::string>(key, iter->queryPropValue()));
            }

            span->AddEvent(eventName, attributesMap);
        }
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

        if (!isEmptyString(queryGlobalId()))
            carrier->Set(kGlobalIdHttpHeaderName, queryGlobalId());

        if (!isEmptyString(queryCallerId()))
            carrier->Set(kCallerIdHttpHeaderName, queryCallerId());

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

        ctxProps->setNonEmptyProp(kGlobalIdHttpHeaderName, queryGlobalId());

        if (otelFormatted)
        {
            //The localid is passed as the callerid for the client request....
            ctxProps->setNonEmptyProp(kCallerIdHttpHeaderName, queryLocalId());
        }
        else
        {
            ctxProps->setNonEmptyProp(kCallerIdHttpHeaderName, queryCallerId());
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

            ctxProps->setNonEmptyProp(opentelemetry::trace::propagation::kTraceState.data(), span->GetContext().trace_state()->ToHeader().c_str());
        }
        else
        {
            ctxProps->setNonEmptyProp("traceID", traceID.get());
            ctxProps->setNonEmptyProp("spanID", spanID.get());
            ctxProps->setNonEmptyProp("traceFlags", traceFlags.get());
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

protected:
    CSpan(const char * spanName)
    {
        name.set(spanName);
    }

    void init(SpanFlags flags)
    {
        //we don't always want to create trace/span IDs
        if (hasMask(flags, SpanFlags::EnsureTraceId) || //per span flags
            queryInternalTraceManager().alwaysCreateTraceIds() || //Global/component flags
            nostd::get<trace_api::SpanContext>(opts.parent).IsValid()) // valid parent was passed in
        {
            span = queryInternalTraceManager().queryTracer()->StartSpan(name.get(), {}, opts);

            if (span != nullptr)
            {
                storeSpanContext();
            }
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
    StringAttr traceFlags;
    StringAttr traceID;
    StringAttr spanID;

    opentelemetry::trace::StartSpanOptions opts;
    nostd::shared_ptr<opentelemetry::trace::Span> span;

};

class CNullSpan : public CInterfaceOf<ISpan>
{
public:
    CNullSpan() = default;

    virtual void setSpanAttribute(const char * key, const char * val) override {}
    virtual void setSpanAttributes(const IProperties * attributes) override {}
    virtual void addSpanEvent(const char * eventName) override {}
    virtual void addSpanEvent(const char * eventName, IProperties * attributes) override {};
    virtual bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const override { return false; }

    virtual void toString(StringBuffer & out) const override {}
    virtual void getLogPrefix(StringBuffer & out) const override {}

    virtual const char* queryGlobalId() const override { return nullptr; }
    virtual const char* queryCallerId() const override { return nullptr; }
    virtual const char* queryLocalId() const override { return nullptr; }

    virtual ISpan * createClientSpan(const char * name) override { return getNullSpan(); }
    virtual ISpan * createInternalSpan(const char * name) override { return getNullSpan(); }

private:
    CNullSpan(const CNullSpan&) = delete;
    CNullSpan& operator=(const CNullSpan&) = delete;
};


class CChildSpan : public CSpan
{
protected:
    CChildSpan(const char * spanName, CSpan * parent)
    : CSpan(spanName), localParentSpan(parent)
    {
        injectlocalParentSpan(localParentSpan);
    }

    void injectlocalParentSpan(CSpan * localParentSpan)
    {
        auto localParentSpanCtx = localParentSpan->querySpanContext();
        if (localParentSpanCtx.IsValid())
            opts.parent = localParentSpanCtx;
    }

public:
    virtual bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const override
    {
        //MORE: It is not clear what this return value represents, and whether it would ever be checked.
        bool ok = CSpan::getSpanContext(ctxProps, otelFormatted);

        if (ok && !otelFormatted)
        {
            Owned<IProperties> localParentSpanCtxProps = createProperties();
            localParentSpan->getSpanContext(localParentSpanCtxProps, false);
            ctxProps->setNonEmptyProp("localParentSpanID", localParentSpanCtxProps->queryProp("spanID"));
        }

        return ok;
    }

    virtual const char* queryGlobalId() const override
    {
        return localParentSpan->queryGlobalId();
    }

    virtual const char* queryCallerId() const override
    {
        return localParentSpan->queryCallerId();
    }

    virtual const char* queryLocalId() const override
    {
        return localParentSpan->queryLocalId();
    }

    virtual void toString(StringBuffer & out, bool isLeaf) const
    {
        CSpan::toString(out, isLeaf);

        out.append(",\"ParentSpan\":{ ");
        localParentSpan->toString(out, false);
        out.append(" }");
    };

protected:
    CSpan * localParentSpan = nullptr;
};

class CInternalSpan : public CChildSpan
{
public:
    CInternalSpan(const char * spanName, CSpan * parent)
    : CChildSpan(spanName, parent)
    {
        opts.kind = opentelemetry::trace::SpanKind::kInternal;
        init(SpanFlags::None);
    }

    void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Internal\"");
        CChildSpan::toString(out, isLeaf);
    }
};

class CClientSpan : public CChildSpan
{
public:
    CClientSpan(const char * spanName, CSpan * parent)
    : CChildSpan(spanName, parent)
    {
        opts.kind = opentelemetry::trace::SpanKind::kClient;
        init(SpanFlags::None);
    }

    void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Client\"");
        CChildSpan::toString(out, isLeaf);
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
    StringAttr hpccGlobalId;
    StringAttr hpccCallerId;
    StringAttr hpccLocalId;

    void setSpanContext(const IProperties * httpHeaders, SpanFlags flags)
    {
        if (httpHeaders)
        {
            if (httpHeaders->hasProp(kGlobalIdHttpHeaderName))
                hpccGlobalId.set(httpHeaders->queryProp(kGlobalIdHttpHeaderName));
            else if (httpHeaders->hasProp(kLegacyGlobalIdHttpHeaderName))
                hpccGlobalId.set(httpHeaders->queryProp(kLegacyGlobalIdHttpHeaderName));
            else if (hasMask(flags, SpanFlags::EnsureGlobalId) || queryInternalTraceManager().alwaysCreateGlobalIds())
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
    }

    bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const override
    {
        bool success = CSpan::getSpanContext(ctxProps, otelFormatted);

        if (!otelFormatted && remoteParentSpanCtx.IsValid())
        {
            StringBuffer remoteParentSpanID;
            char remoteParentSpanId[16] = {0};
            remoteParentSpanCtx.span_id().ToLowerBase16(remoteParentSpanId);
            remoteParentSpanID.append(16, remoteParentSpanId);
            ctxProps->setProp("remoteParentSpanID", remoteParentSpanID.str());
        }

        return success;
    }

    void init(SpanFlags flags)
    {
        bool createLocalId = !isEmptyString(hpccGlobalId);
        if (createLocalId)
            hpccLocalId.set(ln_uid::createUniqueIdString().c_str());
        CSpan::init(flags);
    }

    void setContextAttributes()
    {
        if (!isEmptyString(hpccGlobalId))
            setSpanAttribute(kGlobalIdOtelAttributeName, hpccGlobalId.get());

        if (!isEmptyString(hpccCallerId))
            setSpanAttribute(kCallerIdOtelAttributeName, hpccCallerId.get());

        if (!isEmptyString(hpccLocalId))
            setSpanAttribute(kLocalIdIdOtelAttributeName, hpccLocalId.get());
    }

public:
    CServerSpan(const char * spanName, const IProperties * httpHeaders, SpanFlags flags)
    : CSpan(spanName)
    {
        opts.kind = opentelemetry::trace::SpanKind::kServer;
        setSpanContext(httpHeaders, flags);
        init(flags);
        setContextAttributes();
    }

    virtual const char* queryGlobalId() const override
    {
        return hpccGlobalId.get();
    }

    virtual const char* queryCallerId() const override
    {
        return hpccCallerId.get();
    }

    virtual const char* queryLocalId() const override
    {
        return hpccLocalId.get();
    }

    virtual void toString(StringBuffer & out, bool isLeaf) const override
    {
        out.append("\"Type\":\"Server\"");
        CSpan::toString(out, isLeaf);
        if (isLeaf)
        {
            if (!isEmptyString(hpccGlobalId.get()))
                out.append(",\"HPCCGlobalID\":\"").append(hpccGlobalId.get()).append("\"");
            if (!isEmptyString(hpccCallerId.get()))
                out.append(",\"HPCCCallerID\":\"").append(hpccCallerId.get()).append("\"");
        }

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

IProperties * getSpanContext(const ISpan * span)
{
    Owned<IProperties> headers = createProperties(true);
    span->getSpanContext(headers, false);
    return headers.getClear();
}

//---------------------------------------------------------------------------------------------------------------------

void CTraceManager::initTracerProviderAndGlobalInternals(const IPropertyTree * traceConfig)
{
    //Trace to JLog by default.
    std::unique_ptr<opentelemetry::sdk::trace::SpanExporter> exporter = JLogSpanExporterFactory::Create(DEFAULT_SPAN_LOG_FLAGS);

    //Administrators can choose to export trace data to a different backend by specifying the exporter type
    if (traceConfig && traceConfig->hasProp("exporter"))
    {
        Owned<IPropertyTree> exportConfig = traceConfig->getPropTree("exporter");
        if (exportConfig)
        {
            StringBuffer exportType;
            exportConfig->getProp("@type", exportType);
            LOG(MCoperatorInfo, "Exporter type: %s", exportType.str());

            if (!exportType.isEmpty())
            {
                if (stricmp(exportType.str(), "OS")==0) //To stdout/err
                {
                    exporter = opentelemetry::exporter::trace::OStreamSpanExporterFactory::Create();
                    LOG(MCoperatorInfo, "Tracing exporter set OS");
                }
                else if (stricmp(exportType.str(), "OTLP")==0 || stricmp(exportType.str(), "OTLP-HTTP")==0)
                {
                    opentelemetry::exporter::otlp::OtlpHttpExporterOptions trace_opts;
                    const char * endPoint = exportConfig->queryProp("@endpoint");
                    if (endPoint)
                        trace_opts.url = endPoint;

                    if (exportConfig->hasProp("@timeOutSecs")) //not sure exactly what this value actually affects
                        trace_opts.timeout = std::chrono::seconds(exportConfig->getPropInt("@timeOutSecs"));

                    // Whether to print the status of the exporter in the console
                    trace_opts.console_debug = exportConfig->getPropBool("@consoleDebug", false);

                    exporter  = opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(trace_opts);
                    LOG(MCoperatorInfo,"Tracing exporter set to OTLP/HTTP to: (%s)", trace_opts.url.c_str());
                }
                else if (stricmp(exportType.str(), "OTLP-GRPC")==0)
                {
                    namespace otlp = opentelemetry::exporter::otlp;

                    otlp::OtlpGrpcExporterOptions opts;

                    const char * endPoint = exportConfig->queryProp("@endpoint");
                    if (endPoint)
                        opts.endpoint = endPoint;

                    opts.use_ssl_credentials = exportConfig->getPropBool("@useSslCredentials", false);

                    if (opts.use_ssl_credentials)
                    {
                        StringBuffer sslCACertPath;
                        exportConfig->getProp("@sslCredentialsCACertPath", sslCACertPath);
                        opts.ssl_credentials_cacert_path = sslCACertPath.str();
                    }

                    if (exportConfig->hasProp("@timeOutSecs")) //grpc deadline timeout in seconds
                        opts.timeout = std::chrono::seconds(exportConfig->getPropInt("@timeOutSecs"));

                    exporter = otlp::OtlpGrpcExporterFactory::Create(opts);
                    LOG(MCoperatorInfo, "Tracing exporter set to OTLP/GRPC to: (%s)", opts.endpoint.c_str());
                }
                else if (stricmp(exportType.str(), "JLOG")==0)
                {
                    StringBuffer logFlagsStr;
                    SpanLogFlags logFlags = SpanLogFlags::LogNone;

                    if (exportConfig->getPropBool("@logSpanDetails", false))
                    {
                        logFlags |= SpanLogFlags::LogSpanDetails;
                        logFlagsStr.append(" LogDetails ");
                    }
                    if (exportConfig->getPropBool("@logParentInfo", false))
                    {
                        logFlags |= SpanLogFlags::LogParentInfo;
                        logFlagsStr.append(" LogParentInfo ");
                    }
                    if (exportConfig->getPropBool("@logAttributes", false))
                    {
                        logFlags |= SpanLogFlags::LogAttributes;
                        logFlagsStr.append(" LogAttributes ");
                    }
                    if (exportConfig->getPropBool("@logEvents", false))
                    {
                        logFlags |= SpanLogFlags::LogEvents;
                        logFlagsStr.append(" LogEvents ");
                    }
                    if (exportConfig->getPropBool("@logLinks", false))
                    {
                        logFlags |= SpanLogFlags::LogLinks;
                        logFlagsStr.append(" LogLinks ");
                    }
                    if (exportConfig->getPropBool("@logResources", false))
                    {
                        logFlags |= SpanLogFlags::LogResources;
                        logFlagsStr.append(" LogLinks ");
                    }

                    //if no log feature flags provided, use default
                    if (logFlags == SpanLogFlags::LogNone)
                        logFlags = DEFAULT_SPAN_LOG_FLAGS;

                    exporter = JLogSpanExporterFactory::Create(logFlags);

                    LOG(MCoperatorInfo, "Tracing exporter set to JLog: logFlags( LogAttributes LogParentInfo %s)", logFlagsStr.str());
                }
                else if (stricmp(exportType.str(), "Prometheus")==0)
                    LOG(MCoperatorInfo, "Tracing to Prometheus currently not supported");
                else if (stricmp(exportType.str(), "NONE")==0)
                {
                    exporter = NoopSpanExporterFactory::Create();
                    LOG(MCoperatorInfo, "Tracing exporter set to 'NONE', no trace exporting will be performed");
                }
                else
                    LOG(MCoperatorInfo, "Tracing exporter type not supported: '%s', JLog trace exporting will be performed", exportType.str());
            }
            else
                LOG(MCoperatorInfo, "Tracing exporter type not specified");
        }
    }

    //Administrator can choose to process spans in batches or one at a time
    //Default: SimpleSpanProcesser sends spans one by one to an exporter.
    std::unique_ptr<opentelemetry::v1::sdk::trace::SpanProcessor> processor = opentelemetry::sdk::trace::SimpleSpanProcessorFactory::Create(std::move(exporter));
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
            LOG(MCoperatorInfo, "OpenTel tracing using batch Span Processor");
        }
        else if (foundProcessorType &&  strcmp("simple", processorType.str())==0)
        {
            LOG(MCoperatorInfo, "OpenTel tracing using batch simple Processor");
        }
        else
        {
            LOG(MCoperatorInfo, "OpenTel tracing detected invalid processor type: '%s'", processorType.str());
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
        disabled: true                  #optional - disable OTel tracing
        alwaysCreateGlobalIds : false   #optional - should global ids always be created?
        alwaysCreateTraceIds            #optional - should trace ids always be created?
        exporter:                       #optional - Controls how trace data is exported/reported
            type: OTLP                    #OS|OTLP|Prometheus|JLOG (default: JLOG)
            endpoint: "localhost:4317"    #exporter specific key/value pairs
            useSslCredentials: true
            sslCredentialsCACcert: "ssl-certificate"
        processor:                      #optional - Controls span processing style
            type: batch                   #simple|batch (default: simple)
*/
void CTraceManager::initTracer(const IPropertyTree * traceConfig)
{
    try
    {
#ifdef TRACECONFIGDEBUG
        Owned<IPropertyTree> testTree;
        if (!traceConfig)
        {
            const char * simulatedGlobalYaml = R"!!(global:
  tracing:
    disabled: false
    processor:
      type: simple
    exporter:
      type: JLOG
      logSpanDetails: true
      logParentInfo: true
      logAttributes: true
      logEvents: true
      logLinks: true
      logResources: true
)!!";
            testTree.setown(createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr));
            traceConfig = testTree->queryPropTree("global/tracing");
        }
        if (traceConfig)
        {
            StringBuffer xml;
            toXML(traceConfig, xml);
            DBGLOG("traceConfig tree: %s", xml.str());
        }
#endif
        bool disableTracing = traceConfig && traceConfig->getPropBool("@disabled", false);

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
            optAlwaysCreateTraceIds = traceConfig->getPropBool("@alwaysCreateTraceIds", optAlwaysCreateTraceIds);
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

void CTraceManager::cleanupTracer()
{
    std::shared_ptr<opentelemetry::trace::TracerProvider> none;
    opentelemetry::trace::Provider::SetTracerProvider(none);
}

CTraceManager::CTraceManager(const char * componentName, const IPropertyTree * componentConfig, const IPropertyTree * globalConfig)
{
    assertex(componentConfig);
    moduleName.set(componentName);
    const IPropertyTree * traceConfig = componentConfig->queryPropTree("tracing");
    if (!traceConfig && globalConfig)
        traceConfig = globalConfig->queryPropTree("tracing");
    initTracer(traceConfig);

    auto provider = opentelemetry::trace::Provider::GetTracerProvider();
    tracer = provider->GetTracer(moduleName.get());
}

CTraceManager::CTraceManager()
{
    throw makeStringExceptionV(-1, "TraceManager must be intialized!");
}

ISpan * CTraceManager::createServerSpan(const char * name, StringArray & httpHeaders, SpanFlags flags) const
{
    Owned<IProperties> headerProperties = getHeadersAsProperties(httpHeaders);
    return new CServerSpan(name, headerProperties, flags);
}

ISpan * CTraceManager::createServerSpan(const char * name, const IProperties * httpHeaders, SpanFlags flags) const
{
    return new CServerSpan(name, httpHeaders, flags);
}


//---------------------------------------------------------------------------------------------------------------------

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
    theTraceManager.destroy();
}

static Owned<ISpan> nullSpan = new CNullSpan();

ISpan * getNullSpan()
{
    return nullSpan.getLink();
}

void initTraceManager(const char * componentName, const IPropertyTree * componentConfig, const IPropertyTree * globalConfig)
{
    theTraceManager.query([=] () { return new CTraceManager(componentName, componentConfig, globalConfig); });
}

ITraceManager & queryTraceManager()
{
    return *theTraceManager.query([] () { return new CTraceManager; }); //throws if not initialized
}
