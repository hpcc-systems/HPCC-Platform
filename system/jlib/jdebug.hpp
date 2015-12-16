/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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



#ifndef JDEBUG_HPP
#define JDEBUG_HPP

#include "jiface.hpp"
#include "jstats.h"

#define TIMING

__int64 jlib_decl cycle_to_nanosec(cycle_t cycles);
__int64 jlib_decl cycle_to_microsec(cycle_t cycles);
__int64 jlib_decl cycle_to_millisec(cycle_t cycles);
cycle_t jlib_decl nanosec_to_cycle(__int64 cycles);
cycle_t jlib_decl get_cycles_now();  // equivalent to getTSC when available
double jlib_decl getCycleToNanoScale();
void jlib_decl display_time(const char * title, cycle_t diff);

// X86 / X86_64
#if defined(_ARCH_X86_64_) || defined(_ARCH_X86_)

#if defined(_WIN32) && defined (_ARCH_X86_)
#pragma warning(push)
#pragma warning(disable:4035)
inline cycle_t getTSC() { __asm { __asm _emit 0x0f __asm _emit 0x31 } }
#pragma warning(pop)
#elif !defined(_WIN32)
inline __int64 getTSC()
{
   unsigned a, d;
   __asm__ volatile("rdtsc" : "=a" (a), "=d" (d));
   return ((cycle_t)a)|(((cycle_t)d) << 32);
}
#else
#include <intrin.h>
inline cycle_t getTSC() { return __rdtsc(); }
#endif // WIN32

#else
// ARMFIX: cycle-count is not always available in user mode
// http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0338g/Bihbeabc.html
// http://neocontra.blogspot.co.uk/2013/05/user-mode-performance-counters-for.html
inline cycle_t getTSC() { return 0; }
#endif // X86

struct HardwareInfo
{
    unsigned numCPUs;
    unsigned CPUSpeed;       // In MHz
    unsigned totalMemory;    // In MB
    unsigned primDiskSize;   // In GB
    unsigned primFreeSize;   // In GB
    unsigned secDiskSize;    // In GB
    unsigned secFreeSize;    // In GB
    unsigned NICSpeed;       // 
};

struct UserSystemTime_t
{
public:
    UserSystemTime_t() : user(0), system(0) {}

    unsigned user;
    unsigned system;
};


interface ITimeReportInfo
{
    virtual void report(const char * scope, const char * description, const __int64 totaltime, const __int64 maxtime, const unsigned count) = 0;
};
class StringBuffer;
class MemoryBuffer;
struct ITimeReporter : public IInterface
{
  virtual void addTiming(const char * scope, cycle_t cycles) = 0;
  virtual void mergeTiming(const char * scope, cycle_t totalcycles, cycle_t maxcycles, const unsigned count) = 0;
  virtual unsigned numSections() = 0;
  virtual __int64 getTime(unsigned idx) = 0;
  virtual __int64 getMaxTime(unsigned idx) = 0;
  virtual unsigned getCount(unsigned idx) = 0;
  virtual StatisticKind getTimerType(unsigned idx) = 0;
  virtual StatisticScopeType getScopeType(unsigned idx) = 0;
  virtual StringBuffer &getScope(unsigned idx, StringBuffer &s) = 0;
  virtual StringBuffer &getTimings(StringBuffer &s) = 0;
  virtual void printTimings() = 0;
  virtual void reset() = 0;
  virtual void mergeInto(ITimeReporter &other) = 0;
  virtual void merge(ITimeReporter &other)= 0;
  virtual void report(ITimeReportInfo &cb) = 0;
  virtual void serialize(MemoryBuffer &mb) = 0;
};

extern jlib_decl cycle_t oneSecInCycles;
class CCycleTimer
{
    cycle_t start_time;
public:
    inline CCycleTimer()
    {
        reset();
    }
    inline void reset()
    {
        start_time = get_cycles_now();
    }
    inline cycle_t elapsedCycles() const
    {
        return get_cycles_now() - start_time;
    }
    inline unsigned __int64 elapsedNs() const
    {
        return cycle_to_nanosec(elapsedCycles());
    }
    inline unsigned elapsedMs() const
    {
        return static_cast<unsigned>(cycle_to_millisec(elapsedCycles()));
    }
};
inline cycle_t queryOneSecCycles() { return oneSecInCycles; }

class jlib_decl TimeSection
{
public:
  TimeSection(const char * _title);
  ~TimeSection();

protected:
  const char *    title;
  cycle_t         start_time;
};

class jlib_decl MTimeSection
{
public:
  MTimeSection(ITimeReporter *_master, const char * scope);
  ~MTimeSection();
protected:
  const char * scope;
  const char *    title;
  cycle_t         start_time;
  ITimeReporter *master;
};


#if defined(TIMING)
extern jlib_decl ITimeReporter * queryActiveTimer();
extern jlib_decl ITimeReporter *createStdTimeReporter();
extern jlib_decl ITimeReporter *createStdTimeReporter(MemoryBuffer &mb);
#define TIME_SECTION(title)   TimeSection   glue(_timer,__LINE__)(title);
#define MTIME_SECTION(master,title)  MTimeSection   glue(mtimer,__LINE__)(master, title);
#else
#define TIME_SECTION(title)   
#define MTIME_SECTION(master,title)
#endif

extern jlib_decl unsigned usTick();

class jlib_decl HiresTimer 
{
public:
    inline HiresTimer()
    {
        start=usTick();
    }
    
    inline void reset() 
    { 
        start=usTick(); 
    }
    
    inline double get() 
    { 
        return (double)(usTick()-start)/1000000; 
    }

private:
    unsigned start;
};


//===========================================================================
#ifndef USING_MPATROL
#ifdef _DEBUG
#define LEAK_CHECK
#endif
#endif

#ifdef LEAK_CHECK
 #ifdef _WIN32
  #include <stdio.h>
  #define _CRTDBG_MAP_ALLOC
  #include <crtdbg.h>
  #define CHECK_FOR_LEAKS(title)  LeakChecker checker##__LINE__(title)

class jlib_decl LeakChecker
{
public:
  LeakChecker(const char * _title);
  ~LeakChecker();

protected:
  const char * title;
  _CrtMemState oldMemState;
};

extern jlib_decl void logLeaks (const char *logFile);  // logFile may be NULL, leaks are logged to only stderr in this case
extern jlib_decl void logLeaks (FILE *logHandle);  // only use predefined file handles like stderr and stdout


 #else
  #define CHECK_FOR_LEAKS(title)
  #define logLeaks(name)
 #endif
#else
 #define CHECK_FOR_LEAKS(title)
 #define logLeaks(name)
#endif

#if !defined(USING_MPATROL) && defined(_DEBUG) && (defined(_WIN32) || defined(WIN32))
void jlib_decl enableMemLeakChecking(bool enable);
#else
#define enableMemLeakChecking(enable) 
#endif

// Hook to be called by the performance monitor, takes stats for processor, virtual memory, disk, and thread usage

interface IPerfMonHook : extends IInterface
{
public:
    virtual void processPerfStats(unsigned processorUsage, unsigned memoryUsage, unsigned memoryTotal, unsigned __int64 fistDiskUsage, unsigned __int64 firstDiskTotal, unsigned __int64 secondDiskUsage, unsigned __int64 secondDiskTotal, unsigned threadCount) = 0;
    virtual StringBuffer &extraLogging(StringBuffer &extra) = 0; // for extra periodic logging
    virtual void log(int level, const char *msg) = 0;
};

enum
{
    //do nothing modes:
    PerfMonSilent    = 0x00,
    //individual components:
    PerfMonProcMem   = 0x01,
    PerfMonPackets   = 0x02,
    PerfMonDiskUsage = 0x04,
    //default and full modes:
    PerfMonExtended  = 0x08,   
#ifdef _WIN32
    PerfMonStandard  = PerfMonProcMem
#else
    PerfMonStandard  = PerfMonProcMem|PerfMonExtended
#endif

};

interface IUserMetric : extends IInterface
{
    virtual unsigned __int64 queryCount() const = 0;
    virtual const char *queryName() const = 0;
    virtual const char *queryMatchString() const = 0;
    virtual void inc() = 0;
    virtual void reset() = 0;
};

extern jlib_decl IUserMetric * createUserMetric(const char *name, const char *matchString);

typedef unsigned PerfMonMode;

void jlib_decl getSystemTraceInfo(StringBuffer &str, PerfMonMode mode = PerfMonProcMem);
void jlib_decl startPerformanceMonitor(unsigned interval, PerfMonMode traceMode = PerfMonStandard, IPerfMonHook * hook = NULL);
void jlib_decl stopPerformanceMonitor();
void jlib_decl setPerformanceMonitorPrimaryFileSystem(char const * fs); // for monitoring disk1, defaults to C: (win) or / (linux)
void jlib_decl setPerformanceMonitorSecondaryFileSystem(char const * fs); // for monitoring disk2, no default
unsigned jlib_decl getLatestCPUUsage();

__int64 jlib_decl getTotalMem();
unsigned jlib_decl setAllocHook(bool on);  // bwd compat returns unsigned

#if defined(__GLIBC) && !defined(_DEBUG)
 #if __GLIBC_PREREQ(2, 14)
  #define USE_JLIB_ALLOC_HOOK extern void jlib_decl jlib_init_hook(); void (* volatile __malloc_initialize_hook) (void) = jlib_init_hook;
 #else
  #define USE_JLIB_ALLOC_HOOK extern void jlib_decl jlib_init_hook(); void (* __malloc_initialize_hook) (void) = jlib_init_hook;
 #endif
#else
 #define USE_JLIB_ALLOC_HOOK
#endif

extern jlib_decl void getHardwareInfo(HardwareInfo &hdwInfo, const char *primDiskPath = NULL, const char *secDiskPath = NULL);
extern jlib_decl void getProcessTime(UserSystemTime_t & time);
extern jlib_decl memsize_t getMapInfo(const char *type);
extern jlib_decl void getCpuInfo(unsigned &numCPUs, unsigned &CPUSpeed);
extern jlib_decl void getPeakMemUsage(memsize_t &peakVm,memsize_t &peakResident);
extern jlib_decl unsigned getAffinityCpus();
extern jlib_decl void printProcMap(const char *fn, bool printbody, bool printsummary, StringBuffer *lnout, MemoryBuffer *mb, bool useprintf);
extern jlib_decl void PrintMemoryReport(bool full=true);
extern jlib_decl void printAllocationSummary();
extern jlib_decl bool areTransparentHugePagesEnabled();
extern jlib_decl memsize_t getHugePageSize();

#endif

