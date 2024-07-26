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
#include <chrono>

/**
 * @brief This follows open telemetry's span attribute naming conventions
 *  Known HPCC span Keys could be added here
 *  Specialized span keys can also be defined within the scope of a span 
 */
static constexpr const char *kGlobalIdHttpHeaderName = "Global-Id";
static constexpr const char *kCallerIdHttpHeaderName = "Caller-Id";
static constexpr const char *kLegacyGlobalIdHttpHeaderName = "HPCC-Global-Id";
static constexpr const char *kLegacyCallerIdHttpHeaderName = "HPCC-Caller-Id";
static constexpr const char *kGlobalIdOtelAttributeName = "id.global";
static constexpr const char *kCallerIdOtelAttributeName = "id.caller";
static constexpr const char *kLocalIdIdOtelAttributeName = "id.local";

enum class SpanLogFlags : unsigned
{
    LogNone            = 0x00000000,
    LogSpanDetails     = 0x00000001,
    LogParentInfo      = 0x00000002,
    LogAttributes      = 0x00000004,
    LogEvents          = 0x00000008,
    LogLinks           = 0x00000010,
    LogResources       = 0x00000020,
};
BITMASK_ENUM(SpanLogFlags);

static constexpr SpanLogFlags DEFAULT_SPAN_LOG_FLAGS = SpanLogFlags::LogAttributes | SpanLogFlags::LogParentInfo;


enum class SpanFlags : unsigned
{
    None            = 0x00000000,
    EnsureGlobalId  = 0x00000001,
    EnsureTraceId   = 0x00000002,
};
BITMASK_ENUM(SpanFlags);

static constexpr const char * NO_STATUS_MESSAGE = "";
static constexpr int UNKNOWN_ERROR_CODE = -1;

struct SpanError
/**
 * @brief Represents an error that occurred during a span.
 * 
 * Used to store information about an error that occurred during the execution of a span.
 * It includes the error message, error code, 
 * and flags indicating whether the span failed and whether the exception escaped the span's scope.
 */
{
    const char * errorMessage = NO_STATUS_MESSAGE; /**< The error message associated with the error. */
    int errorCode = UNKNOWN_ERROR_CODE; /**< The error code associated with the error. */
    bool spanFailed = true; /**< Flag indicating whether the span failed. */
    bool escapeScope = false; /**< Flag indicating whether the exception escaped the scope of the span. */

    /**
     * @brief Default constructor.
     */
    SpanError() = default;

    /**
     * @brief Constructor with error message.
     * @param _errorMessage The error message.
     */
    SpanError(const char * _errorMessage) : errorMessage(_errorMessage) {}

    /**
     * @brief Constructor with error message, error code, span failure flag, and scope escape flag.
     * @param _errorMessage The error message.
     * @param _errorCode The error code.
     * @param _spanFailed Flag indicating whether the span failed.
     * @param _escapeScope Flag indicating whether the exception escaped the scope of the span.
     */
    SpanError(const char * _errorMessage, int _errorCode, bool _spanFailed, bool _escapeScope) 
     : errorMessage(_errorMessage), errorCode(_errorCode), spanFailed(_spanFailed), escapeScope(_escapeScope) {}

    /**
     * @brief Sets the span status success.
     * @param _spanSucceeded Flag indicating whether the span succeeded.
     * @param _spanScopeEscape Flag indicating whether the exception escaped the scope of the span.
     */
    void setSpanStatusSuccess(bool _spanSucceeded, bool _spanScopeEscape) { spanFailed = !_spanSucceeded; escapeScope = _spanScopeEscape;}

    /**
     * @brief Sets the error message and error code.
     * @param _errorMessage The error message.
     * @param _errorCode The error code.
     */
    void setError(const char * _errorMessage, int _errorCode) { errorMessage = _errorMessage; errorCode = _errorCode; }
};

interface ISpan : extends IInterface
{
    virtual void setSpanAttribute(const char * key, const char * val) = 0;
    virtual void setSpanAttribute(const char *name, __uint64 value) = 0;
    virtual void setSpanAttributes(const IProperties * attributes) = 0;
    virtual void addSpanEvent(const char * eventName) = 0;
    virtual void addSpanEvent(const char * eventName, IProperties * attributes) = 0;
    virtual void endSpan() = 0; // Indicate that the span has ended even if it has not yet been destroyed
    virtual void getSpanContext(IProperties * ctxProps) const = 0;
    virtual void getClientHeaders(IProperties * clientHeaders) const = 0;
    virtual void toString(StringBuffer & out) const = 0;
    virtual void getLogPrefix(StringBuffer & out) const = 0;
    virtual bool isRecording() const = 0;   // Is it worth adding any events/attributes to this span?
    virtual void recordException(IException * e, bool spanFailed = true, bool escapedScope = true) = 0;
    virtual void recordError(const SpanError & error = SpanError()) = 0;
    virtual void setSpanStatusSuccess(bool spanSucceeded, const char * statusMessage = NO_STATUS_MESSAGE) = 0;
    virtual const char * queryTraceId() const = 0;
    virtual const char * querySpanId() const = 0;

    virtual ISpan * createClientSpan(const char * name) = 0;
    virtual ISpan * createInternalSpan(const char * name) = 0;

//Old-style global/caller/local id interface functions
    virtual const char* queryGlobalId() const = 0;
    virtual const char* queryCallerId() const = 0;
    virtual const char* queryLocalId() const = 0;
};

class jlib_decl OwnedSpanScope
{
public:
    OwnedSpanScope() = default;
    OwnedSpanScope(ISpan * _ptr);
    ~OwnedSpanScope();

    inline ISpan * operator -> () const         { return span; }
    inline operator ISpan *() const             { return span; }

    void clear();
    ISpan * query() const { return span; }
    void set(ISpan * _span);
    void setown(ISpan * _span);

private:
    Owned<ISpan> span;
    ISpan * prevSpan = nullptr;
};

extern jlib_decl IProperties * getClientHeaders(const ISpan * span);
extern jlib_decl IProperties * getSpanContext(const ISpan * span);

struct SpanTimeStamp
{
    std::chrono::nanoseconds steadyClockTime = std::chrono::nanoseconds::zero();
    std::chrono::nanoseconds systemClockTime = std::chrono::nanoseconds::zero();

    void now()
    {
        systemClockTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch());
        steadyClockTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch());
    }

    void setMSTickTime(const unsigned int msTickTime)
    {
        systemClockTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch() - std::chrono::milliseconds(msTick() - msTickTime));
        steadyClockTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch() - std::chrono::milliseconds(msTick() - msTickTime));
    }

    bool isInitialized() const
    {
        return systemClockTime != std::chrono::nanoseconds::zero();
    }
};

interface ITraceManager : extends IInterface
{
    virtual ISpan * createServerSpan(const char * name, const IProperties * httpHeaders, const SpanTimeStamp * spanStartTimeStamp, SpanFlags flags = SpanFlags::None) const = 0;
    virtual ISpan * createServerSpan(const char * name, StringArray & httpHeaders, SpanFlags flags = SpanFlags::None) const = 0;
    virtual ISpan * createServerSpan(const char * name, const IProperties * httpHeaders, SpanFlags flags = SpanFlags::None) const = 0;
    virtual bool isTracingEnabled() const = 0;
 };

extern jlib_decl ISpan * queryNullSpan();
extern jlib_decl ISpan * getNullSpan();
extern jlib_decl void initTraceManager(const char * componentName, const IPropertyTree * componentConfig, const IPropertyTree * globalConfig);
extern jlib_decl ITraceManager & queryTraceManager();

/*
Temporarily disabled due to build issues in certain environments
#ifdef _USE_CPPUNIT
#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/resource/resource.h"

extern jlib_decl void testJLogExporterPrintResources(StringBuffer & out, const opentelemetry::sdk::resource::Resource &resources);
extern jlib_decl void testJLogExporterPrintAttributes(StringBuffer & out, const std::unordered_map<std::string, opentelemetry::sdk::common::OwnedAttributeValue> & map, const char * attsContainerName);
#endif
*/

//The following class is responsible for ensuring that the active span is restored in a context when the scope is exited
//Use a template class so it can be reused for IContextLogger and IEspContext
template <class CONTEXT>
class ContextSpanScopeImp
{
public:
    ContextSpanScopeImp(CONTEXT & _ctx, ISpan * span)
    : ctx(_ctx)
    {
        prev.set(ctx.queryActiveSpan());
        ctx.setActiveSpan(span);
    }
    ~ContextSpanScopeImp()
    {
        ctx.setActiveSpan(prev);
    }

protected:
    CONTEXT & ctx;
    Owned<ISpan> prev;
};

// A variant of the class above that allows startSpan to be called after construction
template <class CONTEXT>
class DynamicContextSpanScopeImp
{
public:
    ~DynamicContextSpanScopeImp()
    {
        finishSpan();
    }
    void startSpan(CONTEXT & _ctx, ISpan * span)
    {
        ctx = &_ctx;
        prev.set(ctx->queryActiveSpan());
        ctx->setActiveSpan(span);
    }
    void finishSpan()
    {
        if (ctx)
            ctx->setActiveSpan(prev);
    }

protected:
    CONTEXT * ctx{nullptr};
    Owned<ISpan> prev;
};

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

// Detail levels
constexpr TraceFlags traceNone = TraceFlags::None;
constexpr TraceFlags traceStandard = TraceFlags::Standard;
constexpr TraceFlags traceDetailed = TraceFlags::Detailed;
constexpr TraceFlags traceMax = TraceFlags::Max;
constexpr TraceFlags traceAll = (TraceFlags)(~TraceFlags::LevelMask);   // i.e. all feature flags except for the detail level

// Common to several engines
constexpr TraceFlags traceHttp = TraceFlags::flag1;
constexpr TraceFlags traceSockets = TraceFlags::flag2;
constexpr TraceFlags traceCassandra = TraceFlags::flag3;
constexpr TraceFlags traceMongoDB = TraceFlags::flag4;
constexpr TraceFlags traceCouchbase = TraceFlags::flag5;
constexpr TraceFlags traceFilters = TraceFlags::flag6;
constexpr TraceFlags traceKafka = TraceFlags::flag7;
constexpr TraceFlags traceJava = TraceFlags::flag8;
constexpr TraceFlags traceOptimizations = TraceFlags::flag9;        // code generator, but IHqlExpressions also used by esp/engines

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
constexpr TraceFlags traceAborts = TraceFlags::flag29;
constexpr TraceFlags traceAcknowledge = TraceFlags::flag30;

//Specific to the code generator
// see traceOptimizations above.

//========================================================================================= 

struct TraceOption { const char * name; TraceFlags value; };

#define TRACEOPT(NAME) { # NAME, NAME }

//========================================================================================= 

constexpr std::initializer_list<TraceOption> roxieTraceOptions
{ 
    TRACEOPT(traceNone),
    TRACEOPT(traceStandard),
    TRACEOPT(traceDetailed),
    TRACEOPT(traceMax),
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
    TRACEOPT(traceAborts),
    TRACEOPT(traceAcknowledge),
};

constexpr std::initializer_list<TraceOption> eclccTraceOptions
{
    TRACEOPT(traceNone),
    TRACEOPT(traceAll),             // place before the other options so you can enable all and selectively disable
    TRACEOPT(traceStandard),
    TRACEOPT(traceDetailed),
    TRACEOPT(traceMax),
    TRACEOPT(traceOptimizations),
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

#endif
