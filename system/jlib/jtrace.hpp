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
#include "opentelemetry/trace/provider.h" //StartSpanOptions
#define UNIMPLEMENTED throw makeStringExceptionV(-1, "UNIMPLEMENTED feature at %s(%d)", sanitizeSourceFile(__FILE__), __LINE__)
#include "jprop.hpp"

namespace context     = opentelemetry::context;
namespace nostd       = opentelemetry::nostd;
namespace opentel_trace = opentelemetry::trace;

class jlib_decl LogTrace
{
private:
    StringAttr   globalId;
    StringAttr   callerId;
    StringAttr   localId;

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

interface ISpan : extends IInterface
{
    virtual void setAttribute(const char * key, const char * val) = 0;
    virtual void addEvent(const char * eventName) = 0;

    virtual nostd::shared_ptr<opentelemetry::trace::TraceState> getState() = 0;

    virtual const char * queryHPCCGlobalID() = 0;
    virtual const char * queryHPCCCallerID() = 0;
    virtual const char * queryOTSpanName() = 0;
    virtual bool queryOTSpanID(StringAttr & spanId) = 0;
    virtual bool queryOTTraceID(StringAttr & traceID) = 0;

    virtual void setAttributes(const IProperties * attributes) = 0;

    virtual void activate() = 0;
};

class CSpan : public CInterfaceOf<ISpan>
{
public:
    CSpan() : span(nullptr) {};
    CSpan(opentelemetry::trace::SpanKind spanKind, const char * spanName, const char * tracerName_, const IProperties * spanAttributes);
    ~CSpan()
    {
        span->End();
    }

    void activate() override;
    void setAttributes(const IProperties * attributes) override;
    void setAttribute(const char * key, const char * val) override
    {
        if (span)
            span->SetAttribute(key, val);
    }

    void addEvent(const char * eventName) override
    {
        if (span && !isEmptyString(eventName))
            span->AddEvent(eventName);
    }

    nostd::shared_ptr<opentelemetry::trace::TraceState> getState() override
    {
        if (span)
            return span->GetContext().trace_state();

        return opentelemetry::trace::TraceState::GetDefault();
    }

    bool queryOTTraceID(StringAttr & traceID) override
    {
        //store traceId and track it at ctr-time instead?
        traceID.clear();

        if (!span)
            return false;

        auto spanCtx = span->GetContext();
        if (!spanCtx.IsValid())
            return false;

        if (!spanCtx.trace_id().IsValid())
            return false;

        char trace_id[32] = {0};
        
        spanCtx.trace_id().ToLowerBase16(trace_id);
        traceID.set(std::string(trace_id, 32).c_str());

        return true;
    }

    bool queryOTSpanID(StringAttr & spanId) override
    {
        spanId.clear();

        if (!span)
            return false;

        if (!span->IsRecording())
            return false;

        char span_id[16] = {0};
        span->GetContext().span_id().ToLowerBase16(span_id);

        spanId.set(std::string(span_id, 16).c_str());

        return true;
    }

    const char * queryOTSpanName() override
    {
        return name.get();
    }

    const char * queryHPCCGlobalID() override
    {
        return hpccGlobalId.get();
    }

    const char * queryHPCCCallerID() override
    {
        return hpccCallerId.get();
    }

protected:
    StringAttr name;
    StringAttr tracerName;
    StringAttr hpccGlobalId;
    StringAttr hpccCallerId;
    StringAttr opentelTraceParent;
    StringAttr opentelTraceState;

    opentelemetry::trace::StartSpanOptions options;
    nostd::shared_ptr<opentelemetry::trace::Span> span;
};

class CTransactionSpan : public CSpan
{
private:
    opentelemetry::v1::trace::SpanContext parentContext = opentelemetry::trace::SpanContext::GetInvalid();
    void setAttriburesFromHTTPHeaders(const IProperties * httpHeaders);
    void setAttriburesFromHTTPHeaders(StringArray & httpHeaders);

public:
    CTransactionSpan(const char * spanName, const char * tracerName_, const IProperties * httpHeaders, const IProperties * spanAttributes);
    CTransactionSpan(const char * spanName, const char * tracerName_, StringArray & httpHeaders, const IProperties * spanAttributes);

    bool queryOTParentSpanID(StringAttr & parentSpanId) //override
    {
        if (!parentContext.IsValid())
            return false;

        if (!parentContext.trace_id().IsValid())
            return false;

        char span_id[16] = {0};
        parentContext.span_id().ToLowerBase16(span_id);
        parentSpanId = std::string(span_id, 16).c_str();

        return true;
    }
};

class CClientSpan : public CSpan
{
public:
    CClientSpan(const char * spanName, const char * tracerName_, const IProperties * spanAttributes);
};

interface IHPCCTracer : extends IInterface
{
    virtual ISpan * createTransactionSpan(const char * name, const IProperties * httpHeaders, const IProperties * spanAttributes) = 0;
    virtual ISpan * createClientSpan(const char * name, const IProperties * spanAttributes) = 0;
    virtual ISpan * createInternalSpan(const char * name, const IProperties * spanAttributes) = 0;
};

class CTracer : public CInterfaceOf<IHPCCTracer>
{
private:
    StringAttr moduleName;
    nostd::shared_ptr<opentelemetry::trace::Tracer> tracer;

public:
    CTracer(const char* name) : moduleName(name)
    {
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        tracer = provider->GetTracer(moduleName.get());
    }

    ISpan * createTransactionSpan(const char * name, const IProperties * httpHeaders, const IProperties * spanAttributes) override
    {
        return new CTransactionSpan(name, moduleName.get(), httpHeaders, spanAttributes);
    }

    ISpan * createClientSpan(const char * name, const IProperties * spanAttributes) override
    {
        return new CClientSpan(name, moduleName.get(), spanAttributes);
    }

    ISpan * createInternalSpan(const char * name, const IProperties * spanAttributes) override
    {
        return new CSpan(opentelemetry::trace::SpanKind::kInternal, name, moduleName.get(), spanAttributes);
    }
};

interface ITraceManager : extends IInterface
{
    virtual IHPCCTracer * initTracing(const char * moduleName) = 0;
};

class CTraceManager : public CInterfaceOf<ITraceManager>
{
private:
    void initTracer();
    void cleanupTracer();

public:
    CTraceManager()
    {
        initTracer();
    }

    virtual IHPCCTracer * initTracing(const char * tracedModuleName)
    {
        return new CTracer(tracedModuleName);
    }
};

extern jlib_decl CTraceManager * queryTraceManager();

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

interface IPropertyTree;

extern jlib_decl bool doTrace(TraceFlags featureFlag, TraceFlags level=TraceFlags::Standard);

// Overwrites current trace flags for active thread (and optionally the global default for new threads)
extern jlib_decl void updateTraceFlags(TraceFlags flag, bool global = false);

// Retrieve current trace flags for the active thread
extern jlib_decl TraceFlags queryTraceFlags();

// Retrieve default trace flags for new threads
extern jlib_decl TraceFlags queryDefaultTraceFlags();

// Load trace flags from a property tree - typically the global config
// See also the workunit-variant in workunit.hpp

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
