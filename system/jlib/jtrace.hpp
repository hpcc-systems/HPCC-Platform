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
};
BITMASK_ENUM(TraceFlags);

// Feature trace flags for hthor, thor and Roxie engines

// Common to several engines
constexpr TraceFlags traceHttp = TraceFlags::flag1;

// Specific to Roxie
constexpr TraceFlags traceRoxieLock = TraceFlags::flag2;
constexpr TraceFlags traceQueryHashes = TraceFlags::flag3;
constexpr TraceFlags traceSubscriptions = TraceFlags::flag4;
constexpr TraceFlags traceRoxieFiles = TraceFlags::flag5;
constexpr TraceFlags traceRoxieActiveQueries = TraceFlags::flag6;
constexpr TraceFlags traceRoxiePackets = TraceFlags::flag7;
constexpr TraceFlags traceIBYTIfails = TraceFlags::flag8;
constexpr TraceFlags traceRoxiePings = TraceFlags::flag9;
constexpr TraceFlags traceLimitExceeded = TraceFlags::flag10;
constexpr TraceFlags traceRoxiePrewarm = TraceFlags::flag11;
constexpr TraceFlags traceMissingOptFiles = TraceFlags::flag12;


//========================================================================================= 

struct TraceOption { const char * name; TraceFlags value; };

#define TRACEOPT(NAME) { # NAME, NAME }

//========================================================================================= 

constexpr std::initializer_list<TraceOption> roxieTraceOptions
{ 
    TRACEOPT(traceRoxieLock), 
    TRACEOPT(traceQueryHashes), 
    TRACEOPT(traceSubscriptions),
    TRACEOPT(traceRoxieFiles),
    TRACEOPT(traceRoxieActiveQueries),
    TRACEOPT(traceRoxiePackets),
    TRACEOPT(traceIBYTIfails),
    TRACEOPT(traceRoxiePings),
    TRACEOPT(traceLimitExceeded),
    TRACEOPT(traceRoxiePrewarm),
    TRACEOPT(traceMissingOptFiles)
};

interface IPropertyTree;

extern jlib_decl bool doTrace(TraceFlags featureFlag, TraceFlags level=TraceFlags::Standard);

// Overwrites current trace flags for active thread (and any it creates)
extern jlib_decl void updateTraceFlags(TraceFlags flag);

// Set a single trace flags for the active thread (and any it creates)
extern jlib_decl void setTraceFlag(TraceFlags flag, bool enable);

// Set the trace detail level for the active thread (and any it creates)
extern jlib_decl void setTraceLevel(TraceFlags level);

// Retrieve current trace flags for the active thread
extern jlib_decl TraceFlags queryTraceFlags();

// Load trace flags from a property tree - typically the global config
// See also the workunit-variant in workunit.hpp

extern jlib_decl TraceFlags loadTraceFlags(const IPropertyTree * globals, const std::initializer_list<TraceOption> & y, TraceFlags dft);


// Temporarily modify the trace flags for the current thread, for the lifetime of the LogContextScope object

class jlib_decl LogContextScope
{
public:
    LogContextScope();
    LogContextScope(TraceFlags traceFlags);
    ~LogContextScope();

    TraceFlags prevFlags;
};

#endif
