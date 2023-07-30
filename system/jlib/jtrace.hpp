/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#ifndef JTRACE_HPP
#define JTRACE_HPP

#undef UNIMPLEMENTED //opentelemetry defines UNIMPLEMENTED
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

//using namespace opentelemetry::trace;
namespace http_client = opentelemetry::ext::http::client;
namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;
namespace trace_sdk   = opentelemetry::sdk::trace;
namespace opentel_trace = opentelemetry::trace;

//#include "jexcept.hpp" //re-define UNIMPLEMENTED
#define UNIMPLEMENTED throw makeStringExceptionV(-1, "UNIMPLEMENTED feature at %s(%d)", sanitizeSourceFile(__FILE__), __LINE__)
#include "jprop.hpp"

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
            if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
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
            if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
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

/*
template <typename R>
class HPCCStringArrayHttpTextMapCarrier : public opentelemetry::context::propagation::TextMapCarrier
{
public:
    HPCCHttpTextMapCarrier(R &headers) : httpHeaders(headers) {}
    HPCCHttpTextMapCarrier() = default;

    virtual opentelemetry::nostd::string_view Get(opentelemetry::nostd::string_view key) const noexcept override
    {
        std::string theKey = key.data();
        std::string headerval;

        // perform any key mapping needed...
        {
            //Instrumented http client/server Capitalizes the first letter of the header name
            if (key == opentel_trace::propagation::kTraceParent || key == opentel_trace::propagation::kTraceState )
                theKey[0] = toupper(theKey[0]);
        }

        ForEachItemIn(x, httpHeaders)
        {
            const char* header = httpHeaders.item(x);
            if(header == nullptr)
                continue;

            const char* colon = strchr(header, ':');
            if(colon == nullptr)
                continue;

            unsigned len = colon - header;
            if((strlen(headername) == len) && (strnicmp(headername, header, len) == 0))
            {
                headerval.append(colon + 2);
                break;
            }
        }
        return headerval;
    }

    virtual void Set(opentelemetry::nostd::string_view key,
                    opentelemetry::nostd::string_view value) noexcept override
    {
        if(!key || !*key)
        return;

        StringBuffer kv;
        kv.append(key).append(": ").append(value);
        ForEachItemIn(x, m_headers)
        {
            const char* curst = m_headers.item(x);
            if(!curst)
                continue;
            const char* colon = strchr(curst, ':');
            if(!colon)
                continue;
            if(!strnicmp(headername, curst, colon - curst))
            {
                m_headers.replace(kv.str(), x);
                return;
            }
        }

        m_headers.append(kv.str());
    }

    StringArray httpHeaders;
};
*/

class jlib_decl LogTrace
{
private:
    StringAttr   globalId;
    StringAttr   callerId;
    StringAttr   localId;

    HPCCHttpTextMapCarrier<IProperties> carrier; //Injects/extracts context and other info in/from http headers

    StringAttr   globalIdHTTPHeaderName = "HPCC-Global-Id";
    StringAttr   callerIdHTTPHeaderName = "HPCC-Caller-Id";

    const char* assignLocalId();

public:

    LogTrace();
    LogTrace(const char * globalId);

    const char* queryGlobalId() const;
    const char* queryCallerId() const;
    const char* queryLocalId() const;
    const char* queryGlobalIdHTTPHeaderName() const { return globalIdHTTPHeaderName.get(); }
    const char* queryCallerIdHTTPHeaderName() const { return callerIdHTTPHeaderName.get(); }

    void setHttpIdHeaderNames(const char *global, const char *caller)
    {
        if (!isEmptyString(global))
            globalIdHTTPHeaderName.set(global);
        if (!isEmptyString(caller))
            callerIdHTTPHeaderName.set(caller);
    }

    //can these be private with abstract methods exposed to create/set these values?
    void setGlobalId(const char* id);
    void setCallerId(const char* id);
    void setLocalId(const char* id);
};

/**
 * @brief This follows open telemetry's span attribute naming conventions
 *  Known HPCC span Keys could be added here
 *  Specialized span keys can also be defined within the scope of a span 
 */
namespace HPCCSemanticConventions
{
static constexpr const char *kGLOBALIDHTTPHeader = "HPCC-Global-Id";
static constexpr const char *kCallerIdHTTPHeader = "HPCC-Caller-Id";
}

// github copilot generated comment:
// This class provides a high-level interface for managing tracing and profiling information in the HPCC Systems platform.
// The TraceManager class is responsible for creating and managing instances of the Tracer class, which is used to instrument code for tracing and profiling. The Tracer class provides methods for starting and ending spans, adding attributes to spans, and propagating trace context across different services and systems.
// Overall, the TraceManager class provides a convenient and flexible way to instrument code for tracing and profiling in the HPCC Systems platform. By using the Tracer class and the TraceManager class, developers can gain insight into the performance and behavior of their applications and diagnose issues in distributed systems.
class TraceManager
{
private:
    //Used as the opentel trace name, refered to as name of library being instrumented
    const char * tracerName = nullptr;

public:
    TraceManager(const char * moduleName)
    {
        //InitModuleObjects();
        initTracer(); //Still not sure where this should be done,
                      //but it needs to be done once per process before any tracers/spans are created
        tracerName = moduleName; //1 tracer for each module/library being instrumented
    };

    TraceManager(const std::string & moduleName)
    {
        TraceManager(moduleName.c_str());
    };

    ~TraceManager() {};

    static void initTracer();
    static void cleanupTracer();

    //convenience non-static method to get the default tracer, uses stored tracer/module name
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> getTracer();

    //convenience Static method to get the tracer for the provided module name
    static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> getTracer(std::string moduleName);

    //Extracts parent contex from the carrier's headers and returns it as callerSpanId
    static void getCallerSpanId(context::propagation::TextMapCarrier &carrier, std::string & callerSpanId);
    static void getParentSpanId(std::map<std::string, std::string> requestHeaders, std::string & callerSpanId);
    static void getParentSpanId(const HttpTextMapCarrier<std::map<std::string, std::string>> carrier, std::string & callerSpanId);
    static void getParentContext(std::map<std::string, std::string>& request_headers, opentel_trace::StartSpanOptions & options);
    
    //Get the parentSpan from ANY Carrier implementation
    // HPCCHttpTextMapCarrier<IProperties> , HttpTextMapCarrier<http_client::Headers>, etc
    // The carrier must implement the TextMapCarrier interface
    // and it will determine the header which contains the parent span
    template<class CARRIER>
    static void getParentSpanId(const CARRIER carrier, std::string & callerSpanId)
    {
        // extract caller span id from http header
        auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        auto ctx = propagator->Extract(carrier);
        auto spanContext = ctx->GetSpanContext();
        callerSpanId = spanContext.span_id();
    }

    void getCurrentTraceId(std::string & traceId) const;
    void getCurrentSpanID(std::string & spanId) const;

    /*
    static void injectCurrentHTTPContext(HPCCHttpTextMapCarrier<IProperties> & hpccHttpHeaders)
    {
        // inject current context into http header
        auto currentCtx = context::RuntimeContext::GetCurrent();
        auto propegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        propegator->Inject(carrier, currentCtx); //injects current context as parent
    }*/

    //Injects current opentelemetry context into the carrier
    //Typically done by client before sending request
    //Context is injected as parent
    //Default propegator targets "traceparent" and "tracestate" headers
    template<class C>
    static void injectCurretContext(C & carrier)
    {
        //HPCCHttpTextMapCarrier<IProperties> | HttpTextMapCarrier<http_client::Headers>

        // inject current context into http header
        auto currentCtx = context::RuntimeContext::GetCurrent();
        auto propegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        propegator->Inject(carrier, currentCtx); //injects current context as parent
    }

    void injectCurretContext(HttpTextMapCarrier<http_client::Headers> & carrier) const
    {
        // inject current context into http header
        auto currentCtx = context::RuntimeContext::GetCurrent();
        //auto propegator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
        //propegator->Inject(carrier, currentCtx); //injects current context as parent

        //return true;
    }

    //Injects custom key/val pairs into provided carrier
    //HPCC components can inject 
    template<class C> //HPCCHttpTextMapCarrier<IProperties> | HttpTextMapCarrier<http_client::Headers>
    static void injectKeyValue(C & carrier, const char * key, const char * val)
    {
        // inject current key/val pair into carrier (http headers?)
        carrier.Set(key, val); 
    }

    template<class C> //HPCCHttpTextMapCarrier<IProperties> | HttpTextMapCarrier<http_client::Headers>
    static void injectHPCCGlobalID(C & carrier, const char * val)
    {
        carrier.Set(HPCCSemanticConventions::kGLOBALIDHTTPHeader, val); 
    }

    template<class C> //HPCCHttpTextMapCarrier<IProperties> | HttpTextMapCarrier<http_client::Headers>
    static void injectHPCCCallerID(C & carrier, const char * val)
    {
        carrier.Set(HPCCSemanticConventions::kCallerIdHTTPHeader, val); 
    }

    template<class C> //HPCCHttpTextMapCarrier<IProperties> | HttpTextMapCarrier<http_client::Headers>
    static void extractHPCCCallerID(C & carrier, const char * val)
    {
        TraceManager::getParentSpanId(carrier, val);
        carrier.Set(HPCCSemanticConventions::kCallerIdHTTPHeader, val); 
    }

    //const char* queryCallerId(context::propagation::TextMapCarrier &carrier) const;
    //static bool setParentContextFromHeaders(std::map<std::string, std::string>& request_headers, opentel_trace::StartSpanOptions & options);
    //static bool extractCallerSpanId(std::map<std::string, std::string> request_headers, std::string & callerSpanId);
    //static bool extractCallerSpanId(const HttpTextMapCarrier<std::map<std::string, std::string>> carrier, std::string & callerSpanId);
};

extern jlib_decl TraceManager * queryTraceManager(const char * name);

/*
  To use feature-level tracing flags, protect the tracing with a test such as:
  
  if (doTrace(flagName [, detailLevel])
      CTXLOG("tracing regarding the specified feature...");

  If detailLevel is omitted it defaults to TraceFlags::Standard. Higher detail levels can be used for feature-level tracing that is
  less-often required.

  For logging that is NOT controlled by a feature flag, but which does want to be suppressable (e.g in a "quiet" mode), you can use

  if (doTrace(TraceFlags::Always, TraceFlags::Standard))
     ...

  Such logging will always appear unless the detail level has been set to "None", to suppress all logging.

  Feature-level trace flags are maintained per-thread, and inherited from the parent thread when a thread is created. Thus it should be
  simple to allow tracing flags or levels to be modified for a single query or activity.

*/

enum class TraceFlags : unsigned
{
    // The trace levels apply within a feature trace setting (or globally if used with TraceFlags::Always) to adjust verbosity
    None = 0,
    Standard = 1,
    Detailed = 2,
    Max = 3,
    LevelMask = 0x3,
    // Individual bit values above low two bits may be given meanings by specific engines or contexts, using constexpr to rename the following
    Always = 0,         // Use with doTrace to test JUST the level, without a feature flag
    flag1 = 0x4,
    flag2 = 0x8,
    flag3 = 0x10,
    flag4 = 0x20,
    flag5 = 0x40,
    flag6 = 0x80,
    flag7 = 0x100,
    flag8 = 0x200,
    flag9 = 0x400,
    flag10 = 0x800,
    flag11 = 0x1000,
    flag12 = 0x2000,
    flag13 = 0x4000,
    flag14 = 0x8000,
    flag15 = 0x10000,
    flag16 = 0x20000,
    flag17 = 0x40000,
    flag18 = 0x80000,
    flag19 = 0x100000,
    flag20 = 0x200000,
    flag21 = 0x400000,
    flag22 = 0x800000,
    flag23 = 0x1000000,
    flag24 = 0x2000000,
    flag25 = 0x4000000,
    flag26 = 0x8000000,
    flag27 = 0x10000000,
    flag28 = 0x20000000,
    flag29 = 0x40000000,
    flag30 = 0x80000000
};
BITMASK_ENUM(TraceFlags);

// Feature trace flags for hthor, thor and Roxie engines

// Common to several engines
constexpr TraceFlags traceHttp = TraceFlags::flag1;
constexpr TraceFlags traceSockets = TraceFlags::flag2;
constexpr TraceFlags traceCassandra = TraceFlags::flag3;
constexpr TraceFlags traceMongoDB = TraceFlags::flag4;
constexpr TraceFlags traceCouchbase = TraceFlags::flag5;
constexpr TraceFlags traceFilters = TraceFlags::flag6;
constexpr TraceFlags traceKafka = TraceFlags::flag7;
constexpr TraceFlags traceJava = TraceFlags::flag8;

// Specific to Roxie
constexpr TraceFlags traceRoxieLock = TraceFlags::flag16;
constexpr TraceFlags traceQueryHashes = TraceFlags::flag17;
constexpr TraceFlags traceSubscriptions = TraceFlags::flag18;
constexpr TraceFlags traceRoxieFiles = TraceFlags::flag19;
constexpr TraceFlags traceRoxieActiveQueries = TraceFlags::flag20;
constexpr TraceFlags traceRoxiePackets = TraceFlags::flag21;
constexpr TraceFlags traceIBYTI = TraceFlags::flag22;
constexpr TraceFlags traceRoxiePings = TraceFlags::flag23;
constexpr TraceFlags traceLimitExceeded = TraceFlags::flag24;
constexpr TraceFlags traceRoxiePrewarm = TraceFlags::flag25;
constexpr TraceFlags traceMissingOptFiles = TraceFlags::flag26;
constexpr TraceFlags traceAffinity = TraceFlags::flag27;
constexpr TraceFlags traceSmartStepping = TraceFlags::flag28;



//========================================================================================= 

struct TraceOption { const char * name; TraceFlags value; };

#define TRACEOPT(NAME) { # NAME, NAME }

//========================================================================================= 

constexpr std::initializer_list<TraceOption> roxieTraceOptions
{ 
    TRACEOPT(traceHttp),
    TRACEOPT(traceSockets),
    TRACEOPT(traceCassandra),
    TRACEOPT(traceMongoDB),
    TRACEOPT(traceCouchbase),
    TRACEOPT(traceFilters),
    TRACEOPT(traceKafka),
    TRACEOPT(traceJava),
    TRACEOPT(traceRoxieLock), 
    TRACEOPT(traceQueryHashes), 
    TRACEOPT(traceSubscriptions),
    TRACEOPT(traceRoxieFiles),
    TRACEOPT(traceRoxieActiveQueries),
    TRACEOPT(traceRoxiePackets),
    TRACEOPT(traceIBYTI),
    TRACEOPT(traceRoxiePings),
    TRACEOPT(traceLimitExceeded),
    TRACEOPT(traceRoxiePrewarm),
    TRACEOPT(traceMissingOptFiles),
    TRACEOPT(traceAffinity),
    TRACEOPT(traceSmartStepping),
};

extern jlib_decl bool doTrace(TraceFlags featureFlag, TraceFlags level=TraceFlags::Standard);

// Overwrites current trace flags for active thread (and optionally the global default for new threads)
extern jlib_decl void updateTraceFlags(TraceFlags flag, bool global = false);

// Retrieve current trace flags for the active thread
extern jlib_decl TraceFlags queryTraceFlags();

// Retrieve default trace flags for new threads
extern jlib_decl TraceFlags queryDefaultTraceFlags();

// Load trace flags from a property tree - typically the global config
// See also the workunit-variant in workunit.hpp
interface IPropertyTree;
extern jlib_decl TraceFlags loadTraceFlags(const IPropertyTree * globals, const std::initializer_list<TraceOption> & y, TraceFlags dft);


// Temporarily modify the trace context and/or flags for the current thread, for the lifetime of the LogContextScope object

class jlib_decl LogContextScope
{
public:
    LogContextScope(const IContextLogger *ctx);
    LogContextScope(const IContextLogger *ctx, TraceFlags traceFlags);
    ~LogContextScope();

    const IContextLogger *prev;
    TraceFlags prevFlags;
};

#endif
