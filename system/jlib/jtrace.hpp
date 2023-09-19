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

/**
 * @brief This follows open telemetry's span attribute naming conventions
 *  Known HPCC span Keys could be added here
 *  Specialized span keys can also be defined within the scope of a span 
 */
static constexpr const char *kGlobalIdHttpHeaderName = "Global-Id";
static constexpr const char *kCallerIdHttpHeaderName = "Caller-Id";
static constexpr const char *kLegacyGlobalIdHttpHeaderName = "HPCC-Global-Id";
static constexpr const char *kLegacyCallerIdHttpHeaderName = "HPCC-Caller-Id";

class jlib_decl LogTrace
{
private:
    StringAttr   globalId;
    StringAttr   callerId;
    StringAttr   localId;

    const char* assignLocalId();

public:

    LogTrace();
    LogTrace(const char * globalId);

    const char* queryGlobalId() const;
    const char* queryCallerId() const;
    const char* queryLocalId() const;

    //can these be private with abstract methods exposed to create/set these values?
    void setGlobalId(const char* id);
    void setCallerId(const char* id);
    void setLocalId(const char* id);
};

interface ISpan : extends IInterface
{
    virtual void setSpanAttribute(const char * key, const char * val) = 0;
    virtual void setSpanAttributes(const IProperties * attributes) = 0;
    virtual void addSpanEvent(const char * eventName) = 0;
    virtual bool getSpanContext(IProperties * ctxProps, bool otelFormatted) const = 0;
    virtual void toString(StringBuffer & out) const = 0;
    virtual void toLog(StringBuffer & out) const = 0;

    virtual ISpan * createClientSpan(const char * name) = 0;
    virtual ISpan * createInternalSpan(const char * name) = 0;
};

interface ITraceManager : extends IInterface
{
    virtual ISpan * createServerSpan(const char * name, StringArray & httpHeaders) = 0;
    virtual ISpan * createServerSpan(const char * name, const IProperties * httpHeaders) = 0;
    virtual bool isTracingEnabled() const = 0;
    virtual const char * getTracedComponentName() const = 0;
 };

extern jlib_decl void initTraceManager(const char * componentName, IPropertyTree * traceConfig);
extern jlib_decl ITraceManager & queryTraceManager();

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
