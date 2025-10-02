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
#include <atomic>

#define TIMING

__int64 jlib_decl cycle_to_nanosec(cycle_t cycles);
__int64 jlib_decl cycle_to_microsec(cycle_t cycles);
__int64 jlib_decl cycle_to_millisec(cycle_t cycles);
cycle_t jlib_decl nanosec_to_cycle(__int64 cycles);
cycle_t jlib_decl millisec_to_cycle(unsigned ms);
double jlib_decl getCycleToNanoScale();
void jlib_decl display_time(const char * title, cycle_t diff);

// X86 / X86_64
#if defined(_ARCH_X86_64_) || defined(_ARCH_X86_)

#define HAS_GOOD_CYCLE_COUNTER

#if defined(_WIN32) && defined (_ARCH_X86_)
#pragma warning(push)
#pragma warning(disable:4035)
inline cycle_t getTSC() { __asm { __asm _emit 0x0f __asm _emit 0x31 } }
#pragma warning(pop)
#elif !defined(_WIN32)
inline cycle_t getTSC() { return __builtin_ia32_rdtsc(); }
#else
#include <intrin.h>
inline cycle_t getTSC() { return __rdtsc(); }
#endif // WIN32

#elif defined(_ARCH_PPC)

#define HAS_GOOD_CYCLE_COUNTER

static inline cycle_t getTSC()
{
    int64_t result;
#ifdef _ARCH_PPC64
    /*
        This reads timebase in one 64bit go.  Does *not* include a workaround for the cell (see 
        http://ozlabs.org/pipermail/linuxppc-dev/2006-October/027052.html)
    */
    __asm__ volatile(
        "mftb    %0"
        : "=r" (result));
#else
    /*
        Read the high 32bits of the timer, then the lower, and repeat if high order has changed in the meantime.  See
        http://ozlabs.org/pipermail/linuxppc-dev/1999-October/003889.html
    */
    unsigned long dummy;
    __asm__ volatile(
        "mfspr   %1,269\n\t"  /* mftbu */
        "mfspr   %L0,268\n\t" /* mftb */
        "mfspr   %0,269\n\t"  /* mftbu */
        "cmpw    %0,%1\n\t"   /* check if the high order word has chanegd */
        "bne     $-16"
        : "=r" (result), "=r" (dummy));
#endif
    return result;
}

#else
// ARMFIX: cycle-count is not always available in user mode
// http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0338g/Bihbeabc.html
// http://neocontra.blogspot.co.uk/2013/05/user-mode-performance-counters-for.html
inline cycle_t getTSC() { return 0; }
#endif // X86

#if defined(INLINE_GET_CYCLES_NOW) && defined(HAS_GOOD_CYCLE_COUNTER)
inline cycle_t get_cycles_now() { return getTSC(); }
#else
cycle_t jlib_decl get_cycles_now();  // equivalent to getTSC when available
#endif

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
    virtual void report(const char * scope, const __int64 totaltime, const __int64 maxtime, const unsigned count) = 0;
};
class StringBuffer;
class MemoryBuffer;
struct ITimeReporter : public IInterface
{
  virtual void addTiming(const char * scope, cycle_t cycles) = 0;
  virtual void mergeTiming(const char * scope, cycle_t totalcycles, cycle_t maxcycles, const unsigned count) = 0;
  virtual StringBuffer &getTimings(StringBuffer &s) = 0;
  virtual void printTimings() = 0;
  virtual void reset() = 0;
  virtual void mergeInto(ITimeReporter &other) = 0; // Used internally
  virtual void merge(ITimeReporter &other)= 0;
  virtual void report(ITimeReportInfo &cb) = 0;
};

extern jlib_decl cycle_t oneSecInCycles;
class CCycleTimer
{
    cycle_t start_time = 0;
public:
    inline CCycleTimer()
    {
        reset();
    }
    inline CCycleTimer(bool enabled)
    {
        if (enabled)
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
    inline unsigned remainingMs(unsigned timeoutMs) const
    {
        if (INFINITE == timeoutMs)
            return INFINITE;
        unsigned eMs = elapsedMs();
        if (eMs >= timeoutMs)
            return 0;
        return timeoutMs - eMs;
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
  cycle_t         start_time;
  ITimeReporter *master;
};


#if defined(TIMING)
extern jlib_decl ITimeReporter * queryActiveTimer();
extern jlib_decl ITimeReporter *createStdTimeReporter();
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

//---------------------------------------------------------------------------------------------------------------------

/*
  There are many situations where you want to report or check something periodically.  This class encapsulates the code
  to perform the check.
  The constructor has an option to indicate whether the first hasElapsed call should return true, or be suppressed.
*/
class PeriodicTimer
{
public:
    PeriodicTimer() = default;
    PeriodicTimer(unsigned ms, bool suppressFirst)
    {
        reset(ms, suppressFirst);
    }

    bool hasElapsed()
    {
        return hasElapsed(get_cycles_now());
    }

    bool hasElapsed(cycle_t nowCycles)
    {
        if ((nowCycles - lastElapsedCycles) < timePeriodCycles)
            return false;

        lastElapsedCycles = nowCycles;
        return true;
    }

    void reset(unsigned ms, bool suppressFirst)
    {
        timePeriodCycles = nanosec_to_cycle((__int64)ms * 1000000);
        lastElapsedCycles = get_cycles_now();
        if (!suppressFirst)
            lastElapsedCycles -= timePeriodCycles;
    }

protected:
    cycle_t timePeriodCycles = 0;
    std::atomic<cycle_t> lastElapsedCycles{0};
};


//---------------------------------------------------------------------------------------------------------------------

/*
There are several situations where we want to record the time spent waiting for items to be processed - time an item
is queued before processing, time spent calling LDAP from within esp etc.. Gathering the metric has a few complications:
* The metric gathering needs to have minimal impact
* The waiting time needs to include items that are currently waiting as well completed
* Multiple items can be being waited for at the same time, and complete at different times.

    waitingTime = sum(itemTime)
    = sum(completedItemTime) + sum(inflightItemTime)
    = sum(endCompletedTime-startCompletedTime) + sum(currentTime-startInflightTime))
    = sumEndCompletedTime - sumStartCompletedTime + sum(currentTime) - sumStartInflightTime
    = sumEndCompletedTime + num*current - sumStartTime

The following pattern efficiently solves the problem:

 * When an item is queued/started atomically increment a counter, and add the current timestamp to the accumulated start time stamps.
 * When an items is dequeued/complete atomically decrement a counter and add the current timestamp to the accumulated end time stamps.
 * To calculate the waiting time use (sumEndTimestamps + numWaiting * currentTimeStamp) - sumStartTimestamps.


At first glance this appears to have a problem because sumEndTimestamps will quickly overflow, but because of the
properties of modulo arithmetic, the final result will be correct even if it has overflowed!
Also since you are only ever interested in (sumEndTimestamps - sumStartTimestamps) the same field can be used for both.

There are two versions, one that uses a critical section, and a second that uses atomics, but is limited to the number
of active blocked items.

There is a second potential use for the BlockedTimeTracker class - for monitoring how many queries overlap the execution of the current query.
This can be calculated in the following way:

a) When a server query starts it calls
   serverLoadTracker.extractInfo(startServer, true)
   workerLoadTracker.extractInfo(startWorker, true);
   startCycles = serverLoadTracker.noteWaiting();
b) When a server query finished it calls
   serverLoadTracker.noteComplete();
   serverLoadTracker.extractInfo(finishServer, true)
   endCycles = workerLoadTracker.extractInfo(finishWorker, true);
c) When a worker query starts it calls
   workerLoadTracker.noteWaiting();
d) When a worker query finished it calls
   workerLoadTracker.noteComplete();

* (finishX.count - startX.count) gives the total number of queries (including this one) that executed at the same time as the query
* (finishX.elapsedNs - startX.elapsedNs) gives the total execution time of all queries (including this one)
* endCycles - startCycles gives the elapsed time for this query.
* This can be reported separately for the server and worker to give an estimate of the concurrent load when a query was running.
*/

struct OverlapTimeInfo
{
    unsigned count;
    stat_type elapsedNs;
};

class jlib_decl BlockedTimeTracker
{
public:
    BlockedTimeTracker() = default;
    BlockedTimeTracker(const BlockedTimeTracker &) = delete;

    //The following return get_cycles_now() - which can be used for tracking elapsed time.
    cycle_t noteWaiting();
    cycle_t noteWaiting(unsigned numWaiting);
    cycle_t noteComplete();
    __uint64 getWaitingNs() const;
    unsigned numInFlight() const;

    //A helper function to help calculate the overlapping load on the system
    void extractOverlapInfo(OverlapTimeInfo & info, bool isStart) const;

protected:
    __uint64 calcActiveTime(cycle_t tally, unsigned active) const;

private:
    mutable CriticalSection cs;
    unsigned numStarted = 0;
    unsigned numFinished = 0;
    cycle_t timeStampTally = 0;
};

class jlib_decl BlockedSection
{
public:
    BlockedSection(BlockedTimeTracker & _tracker) : tracker(_tracker) { tracker.noteWaiting(); }
    ~BlockedSection() { tracker.noteComplete(); }
private:
    BlockedTimeTracker & tracker;
};

//Lightweight version that uses a single atomic, but has a limit on the number of active blocked items
//Easier to understand by looking at the code for the BlockedTimeTracker class
//Could be a template class (for MAX_ACTIVE), but I doubt it is worth it.
class jlib_decl LightweightBlockedTimeTracker
{
    //MAX_ACTIVE should be a power of 2 for efficiency, 256 gives a max blocked time of about half a year before wrapping.
    static constexpr unsigned MAX_ACTIVE = 256;
public:
    LightweightBlockedTimeTracker() = default;
    LightweightBlockedTimeTracker(const LightweightBlockedTimeTracker &) = delete;

    void noteWaiting()                  { timeStampTally.fetch_sub((get_cycles_now() * MAX_ACTIVE) - 1); } // i.e. add 1 and subtract the time
    void noteComplete()                 { timeStampTally.fetch_add((get_cycles_now() * MAX_ACTIVE) - 1); }
    __uint64 getWaitingNs() const;

private:
    std::atomic<__uint64> timeStampTally{0};            // timestamp * MAX_ACTIVE + active
};

class jlib_decl LightweightBlockedSection
{
public:
    LightweightBlockedSection(LightweightBlockedTimeTracker & _tracker) : tracker(_tracker) { tracker.noteWaiting(); }
    ~LightweightBlockedSection() { tracker.noteComplete(); }
private:
    LightweightBlockedTimeTracker & tracker;
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


enum
{
    ReadCpuInfo     = 0x0001,
    ReadMemoryInfo  = 0x0002,
    ReadContextInfo = 0x0004,
    ReadFaultInfo   = 0x0008,
    ReadAllInfo     = 0xFFFF,
};

class jlib_decl SystemProcessInfo
{
public:
    SystemProcessInfo() = default;

    void clear();
    SystemProcessInfo operator - (const SystemProcessInfo & rhs) const;

    __uint64 getNumContextSwitches() const { return contextSwitches; }
    __uint64 getPeakVirtualMemory() const { return peakVirtualMemory; }
    __uint64 getActiveVirtualMemory() const { return activeVirtualMemory; }
    __uint64 getPeakResidentMemory() const { return peakResidentMemory; }
    __uint64 getActiveResidentMemory() const { return activeResidentMemory; }
    __uint64 getActiveSwapMemory() const { return activeSwapMemory; }
    __uint64 getActiveDataMemory() const { return activeDataMemory; }
    __uint64 getMajorFaults() const { return majorFaults; }
    __uint64 getNumThreads() const { return numThreads; }
    unsigned getPercentCpu() const;
    __uint64 getSystemNs() const;
    __uint64 getUserNs() const;
    __uint64 getTotalNs() const;

    unsigned getIdlePercent() const;
    unsigned getIoWaitPercent() const;
    unsigned getSystemPercent() const;
    unsigned getUserPercent() const;

    __uint64 getNumPeriods() const { return numPeriods; }
    __uint64 getNumThrottledPeriods() const { return numThrottledPeriods; }
    __uint64 getTimeThrottledNs() const { return timeThrottledNs; }

    __uint64 getTotal() const { return user + system + idle + iowait; }
protected:
    __uint64 user = 0;      // user time in jiffies (~`1/100s)
    __uint64 system = 0;
    __uint64 idle = 0;
    __uint64 iowait =0;
    __uint64 contextSwitches =0;
    __uint64 peakVirtualMemory = 0;
    __uint64 activeVirtualMemory = 0;
    __uint64 peakResidentMemory = 0;
    __uint64 activeResidentMemory = 0;
    __uint64 activeSwapMemory = 0;
    __uint64 activeDataMemory = 0;
    __uint64 majorFaults = 0;
    __uint64 numThreads = 0;
    __uint64 numPeriods = 0;
    __uint64 numThrottledPeriods = 0;
    __uint64 timeThrottledNs = 0;
};

class jlib_decl ProcessInfo : public SystemProcessInfo
{
public:
    ProcessInfo() = default;
    ProcessInfo(unsigned flags);

    bool update(unsigned flags);
};

class jlib_decl SystemInfo : public SystemProcessInfo
{
public:
    SystemInfo() = default;
    SystemInfo(unsigned flags);

    bool update(unsigned flags);
};

//Information about a single IO device
class jlib_decl BlockIoStats
{
public:
    void clear();
    BlockIoStats & operator += (const BlockIoStats & other);
    BlockIoStats operator - (const BlockIoStats & other) const;

    unsigned getSectorSize() const { return 512; }

public:
    unsigned rd_ios = 0;        // Read I/O operations
    unsigned rd_merges = 0;     // Reads merged
    __uint64 rd_sectors = 0;    // Sectors read
    unsigned rd_ticks = 0;      // Time in queue + service for read
    unsigned wr_ios = 0;        // Write I/O operations
    unsigned wr_merges = 0;     // Writes merged
    __uint64 wr_sectors = 0;    // Sectors written
    unsigned wr_ticks = 0;      // Time in queue + service for write
    unsigned ticks = 0;         // Time of requests in queue
    unsigned aveq = 0;          // Average queue length
};


//Information about all the block IO devices being tracked
class jlib_decl OsDiskStats
{
public:
    OsDiskStats();
    OsDiskStats(bool updateNow);
    ~OsDiskStats();

    bool updateCurrent();
    unsigned getNumPartitions() const;
    const BlockIoStats & queryStats(unsigned i) const { return stats[i]; }
    const BlockIoStats & querySummaryStats() const { return total; }

protected:
    BlockIoStats * stats;
    BlockIoStats total;
};


class jlib_decl OsNetworkStats
{
public:
    OsNetworkStats() = default;
    OsNetworkStats(const char * ifname);

    bool updateCurrent(const char * ifname);    // ifname = null gathers all matches
    OsNetworkStats operator - (const OsNetworkStats & other) const;

public:
    __uint64 rxbytes = 0;
    __uint64 rxpackets = 0;
    __uint64 rxerrors = 0;
    __uint64 rxdrops = 0;
    __uint64 txbytes = 0;
    __uint64 txpackets = 0;
    __uint64 txerrors = 0;
    __uint64 txdrops = 0;
};


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
    // UDP packet loss tracing
    PerfMonUDP       = 0x10,
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
void jlib_decl setPerformanceMonitorHook(IPerfMonHook *hook);
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

enum class HugePageMode { Always, Madvise, Never, Unknown };
extern jlib_decl void getHardwareInfo(HardwareInfo &hdwInfo, const char *primDiskPath = NULL, const char *secDiskPath = NULL);
extern jlib_decl void getProcessTime(UserSystemTime_t & time);
extern jlib_decl memsize_t getMapInfo(const char *type);
extern jlib_decl void getCpuInfo(unsigned &numCPUs, unsigned &CPUSpeed);
extern jlib_decl unsigned getAffinityCpus();
extern jlib_decl void setAffinityCpus(unsigned cpus);
extern jlib_decl void clearAffinityCache(); // should be called whenever the process affinity is changed to reset the cache
extern jlib_decl bool applyResourcedCPUAffinity(const IPropertyTree *resourceSection);

extern jlib_decl void printProcMap(const char *fn, bool printbody, bool printsummary, StringBuffer *lnout, MemoryBuffer *mb, bool useprintf);
extern jlib_decl void PrintMemoryReport(bool full=true);
extern jlib_decl void printAllocationSummary();
extern jlib_decl bool printProcessHandles(unsigned pid=0); // returns false if fails
extern jlib_decl bool printLsOf(unsigned pid=0); // returns false if fails
extern jlib_decl bool areTransparentHugePagesEnabled(HugePageMode mode);
extern jlib_decl HugePageMode queryTransparentHugePagesMode();
extern jlib_decl memsize_t getHugePageSize();

// Core dump generation.
extern jlib_decl void generateCoreDump(bool useVforkAndGcore, bool suspendParent);

// Memory monitoring interface
interface IMemoryMonitor : extends IInterface
{
    virtual void start() = 0;
    virtual void stop() = 0;
};

// Factory functions for creating memory monitors
extern jlib_decl IMemoryMonitor *createMemoryMonitor(const char *path, unsigned totalMB, unsigned thresholdMB, unsigned intervalSecs, unsigned incrementMB, bool useVforkAndGcore, bool suspendParent);
// pick up settings from IPT
extern jlib_decl IMemoryMonitor *createMemoryMonitor(const char *dstPath, unsigned totalMB, const IPropertyTree *mcdSettings);

#endif

