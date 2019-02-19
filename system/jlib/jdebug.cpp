/*##############################################################################


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

#include "jdebug.hpp"
#include "jstring.hpp"
#include "jhash.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jtime.hpp"
#include <stdio.h>
#include <time.h>
#include <atomic>

#ifdef _WIN32
#define DPSAPI_VERSION 1
#include <psapi.h>
#include <processthreadsapi.h>
#include <sysinfoapi.h>
#endif

#ifdef __linux__
#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/klog.h>
#include <dirent.h>
#endif
#ifdef __APPLE__
 #include <sys/param.h>
 #include <sys/mount.h>
 #include <sys/sysctl.h>
 #include <mach/task.h>
 #include <mach/mach_init.h>
 #include <mach/mach_host.h>
 #include <mach/vm_statistics.h>
#endif
#include "build-config.h"

//===========================================================================
#ifdef _DEBUG
// #define _USE_MALLOC_HOOK  // Only enable if you need it - slow!
#else
#undef _USE_MALLOC_HOOK // don't define for release - not threadsafe
#endif

#define _USE_RDTSC true

#ifdef _USE_MALLOC_HOOK
#define REPORT_LARGER_BLOCK_THAN (10*0x100000)
static __int64 totalMem = 0;
static __int64 hwmTotalMem = 0;
#ifdef __linux__
static unsigned memArea[32];
#endif
#endif

/* LINUX SYS: log KEY
========================

PU (%) is the percentage CPU in use (unchanged from previously).

MU (%) is what percentage of total (all processes) memory is in use
(ram + swap) or in 32 bit is the percentage of 3GB (address space)
used (whichever larger). 

MAL is total memory in use (i.e malloced and not freed ) by this
process  (= MMP+SBK) 

MMP is the sum of memory mapped (large) blocks in use by this process
(which will be  returned to OS when freed).

SBK is the sbrk'ed memory i.e. smaller blocks allocated from the
arena. (note this memory is unlikely to be returned to OS while the
process is still running). 

TOT (K) is an indication of the memory footprint of the process.
This is the 'arena' size (which is how much reserved by sbrk) plus the
mmap memory size (MMP). 

RAM (K) is how much real memory is in use by all processes - it is
the same as what would be reported by the 'free' command after the
caches/buffers have been subtracted.

SWP (K) is the swap size in use for all processes.


Extended stats

  DSK: disk statistics for each disk (e.g. [sda] and [sdb])
        r/s     read i/o operations per sec (over last period)
        kr/s    K bytes read per sec
        w/s     write i/o operations per sec
        kw/s    K bytes written per sec
        busy    indication how busy the disk was during period (%)
  NIC: network (i.e. eth0) statistics
        rxp/s   packets received per sec
        rxk/s   K bytes received per sec
        txp/s   packets transmitted per sec
        txk/s   K bytes transmitted per sec
  CPU:
        usr     % at user level
        sys     % in kernel
        iow     % waiting for i/o


*/


inline void skipSp(const char *&s)
{
    while (isspace(*s))
        s++;
}

inline offset_t readHexNum(const char *&s)
{
    offset_t ret = 0;
    for (;;) {
        switch (*s) {
            case '0':
            case '1': 
            case '2': 
            case '3': 
            case '4':
            case '5': 
            case '6': 
            case '7': 
            case '8':
            case '9': ret = ret*16+(*s-'0'); 
                 break;
            case 'A': 
            case 'B': 
            case 'C': 
            case 'D': 
            case 'E':
            case 'F': ret = ret*16+(*s-'A'+10);
                 break;
            case 'a': 
            case 'b': 
            case 'c': 
            case 'd': 
            case 'e':
            case 'f': ret = ret*16+(*s-'a'+10);
                 break;
        default:
            return ret;
        }
        s++;
    }
    return 0;
}

inline offset_t readDecNum(const char *&s)
{
    offset_t ret = 0;
    for (;;) {
        switch (*s) {
            case '0':
            case '1': 
            case '2': 
            case '3': 
            case '4':
            case '5': 
            case '6': 
            case '7': 
            case '8':
            case '9': ret = ret*10+(*s-'0'); 
                 break;
        default:
            return ret;
        }
        s++;
    }
    return 0;
}


#if defined(_WIN32)

static __int64 numCyclesNTicks; 
static __int64 ticksPerSec;
static __int64 numScaleTicks;
static bool useRDTSC = _USE_RDTSC;
static double cycleToNanoScale; 

static void calibrate_timing()
{
#ifndef _AMD64_
    if (useRDTSC) 
    {
        unsigned long r;
        __asm { 
             mov eax, 1 ;  
             cpuid ; 
             mov r, edx 
        }
        if ((r&0x10)==0)
            useRDTSC = false;
    }
#endif
    if (useRDTSC) {
        unsigned startu = usTick();
        cycle_t start = getTSC();
        unsigned s1 = msTick();
        unsigned s2;
        while (s1==(s2=msTick()));
        unsigned s3;
        while (s2==(s3=msTick()));
        unsigned elapsedu = usTick()-startu;
        if (elapsedu) {
            double numPerUS=(double)(getTSC()-start)/(double)elapsedu;  // this probably could be more accurate
            if (numPerUS>0)
            {
                cycleToNanoScale = 1000.0 / numPerUS;
                return;
            }
        }
        IERRLOG("calibrate_timing failed using RDTSC");
        useRDTSC = false;
    }

    static LARGE_INTEGER temp;
    QueryPerformanceFrequency(&temp);
    ticksPerSec = temp.QuadPart;
    numScaleTicks = ticksPerSec/100;

    LARGE_INTEGER t1;
    LARGE_INTEGER t2;
    QueryPerformanceCounter(&t1);
    t2.QuadPart=t1.QuadPart;
    while (t1.QuadPart==t2.QuadPart) QueryPerformanceCounter(&t1);
    cycle_t a1 = getTSC();
    t2.QuadPart = t1.QuadPart;
    while (t2.QuadPart-t1.QuadPart<numScaleTicks) QueryPerformanceCounter(&t2);
    cycle_t a2 = getTSC();
    numCyclesNTicks = (a2 - a1);
    cycleToNanoScale = ((double)numScaleTicks * 1000000000.0) / ((double)numCyclesNTicks * ticksPerSec);
}

__int64 cycle_to_nanosec(cycle_t cycles)
{
    return (__int64)((double)cycles * cycleToNanoScale);
}

__int64 jlib_decl cycle_to_microsec(cycle_t cycles)
{
    return (__int64)((double)cycles * cycleToNanoScale) / 1000;
}

__int64 jlib_decl cycle_to_millisec(cycle_t cycles)
{
    return (__int64)((double)cycles * cycleToNanoScale) / 1000000;
}

cycle_t nanosec_to_cycle(__int64 ns)
{
    return (__int64)((double)ns / cycleToNanoScale);
}

#if !(defined(INLINE_GET_CYCLES_NOW) && defined(HAS_GOOD_CYCLE_COUNTER))
cycle_t jlib_decl get_cycles_now()
{
    if (useRDTSC)
        return getTSC();
    LARGE_INTEGER temp;
    QueryPerformanceCounter(&temp);
    return temp.QuadPart;
}
#endif

double getCycleToNanoScale()
{
    return cycleToNanoScale;
}

#else

#if defined(HAS_GOOD_CYCLE_COUNTER)
static bool useRDTSC = _USE_RDTSC;
#endif
static double cycleToNanoScale;
static double cycleToMicroScale;
static double cycleToMilliScale;

void calibrate_timing()
{
#if defined(_ARCH_X86_) || defined(_ARCH_X86_64_)
    if (useRDTSC) {
        unsigned long eax;
        unsigned long ebx; 
        unsigned long ecx;
        unsigned long edx;
#if defined(_ARCH_X86_64_)
        __asm__ ("cpuid\n\t" : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)   : "0" (1));

#else
// NB PIC code and ebx usage don't mix well
        asm volatile("pushl %%ebx      \n\t" 
                     "cpuid            \n\t"
                     "movl %%ebx, %1   \n\t" 
                     "popl %%ebx       \n\t" 
                     : "=a"(eax), "=r"(ebx), "=c"(ecx), "=d"(edx)
                 : "a"(1)
                 : "cc");
#endif
        if ((edx&0x10)==0)
            useRDTSC = false;
    }
#endif

#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC) {
        unsigned startu = usTick();
        cycle_t start = getTSC();
        unsigned s1 = msTick();
        unsigned s2;
        while (s1==(s2=msTick()));
        unsigned s3;
        while (s2==(s3=msTick()));
        unsigned elapsedu = usTick()-startu;
        if (elapsedu) {
            double numPerUS=(double)(getTSC()-start)/(double)elapsedu;  // this probably could be more accurate
            if (numPerUS>0)
            {
                cycleToNanoScale = 1000.0 / numPerUS;
                cycleToMicroScale = 1.0 / numPerUS;
                cycleToMilliScale = 0.001 / numPerUS;
                return;
            }
        }
        IERRLOG("calibrate_timing failed using RDTSC");
        useRDTSC = false;
    }
#endif
    cycleToNanoScale = 1.0;
    cycleToMicroScale = 1.0;
    cycleToMilliScale = 1.0;
}


#if !defined(INLINE_GET_CYCLES_NOW) || !defined(HAS_GOOD_CYCLE_COUNTER)
#if defined(CLOCK_MONOTONIC) && !defined(__APPLE__)
static bool use_gettimeofday=false;
#endif
cycle_t jlib_decl get_cycles_now()
{
#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC)
        return getTSC();
#endif
#ifdef __APPLE__
    return mach_absolute_time();
#elif defined(CLOCK_MONOTONIC)
    if (!use_gettimeofday) {
        timespec tm;
        if (clock_gettime(CLOCK_MONOTONIC, &tm)>=0)
            return ((cycle_t)tm.tv_sec)*1000000000L+(tm.tv_nsec);   
        use_gettimeofday = true;
        fprintf(stderr,"clock_gettime CLOCK_MONOTONIC returns %d",errno);   // don't use PROGLOG
    }
#endif
    struct timeval tm;
    gettimeofday(&tm,NULL);
    return ((cycle_t)tm.tv_sec)*1000000000L+(cycle_t)tm.tv_usec*1000L; 
}
#endif

__int64 jlib_decl cycle_to_nanosec(cycle_t cycles)
{
#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC)
        return (__int64)((double)cycles * cycleToNanoScale);
#endif
#ifdef __APPLE__
    return cycles * (uint64_t) timebase_info.numer / (uint64_t)timebase_info.denom;
#endif
    return cycles;
}

__int64 jlib_decl cycle_to_microsec(cycle_t cycles)
{
#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC)
        return (__int64)((double)cycles * cycleToMicroScale);
#endif
    return cycles / 1000;
}

__int64 jlib_decl cycle_to_millisec(cycle_t cycles)
{
#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC)
        return (__int64)((double)cycles * cycleToMilliScale);
#endif
    return cycles / 1000000;
}

cycle_t nanosec_to_cycle(__int64 ns)
{
#if defined(HAS_GOOD_CYCLE_COUNTER)
    if (useRDTSC)
        return (__int64)((double)ns / cycleToNanoScale);
#endif
    return ns;
}

double getCycleToNanoScale()
{
    return cycleToNanoScale;
}

#endif


void display_time(const char *title, cycle_t diff)
{
    DBGLOG("Time taken for %s: %" I64F "d cycles (%" I64F "dM) = %" I64F "d msec", title, diff, diff/1000000, cycle_to_nanosec(diff)/1000000);
}

TimeSection::TimeSection(const char * _title) : title(_title)
{
  start_time = get_cycles_now();
}

TimeSection::~TimeSection()
{
  cycle_t end_time = get_cycles_now();
  if (title)
    display_time(title, end_time-start_time);
}

MTimeSection::MTimeSection(ITimeReporter *_master, const char * _scope) : scope(_scope), master(_master)
{
    start_time = get_cycles_now();
}

MTimeSection::~MTimeSection()
{
    cycle_t end_time = get_cycles_now();
    if (master)
        master->addTiming(scope, end_time-start_time);
    else if (scope)
        display_time(scope, end_time-start_time);
}

class TimeSectionInfo : public MappingBase 
{
public:
    TimeSectionInfo(const char * _scope, __int64 _cycles) : scope(_scope), totalcycles(_cycles), maxcycles(_cycles), count(1) {};
    TimeSectionInfo(const char * _scope, __int64 _cycles, __int64 _maxcycles, unsigned _count)
    : scope(_scope), totalcycles(_cycles), maxcycles(_maxcycles), count(_count) {};
    virtual const void * getKey() const { return scope.get(); }

    __int64 getTime() const { return cycle_to_nanosec(totalcycles); }
    __int64 getMaxTime() const { return cycle_to_nanosec(maxcycles); }
    unsigned getCount() const { return count; }

    StringAttr  scope;
    __int64 totalcycles;
    __int64 maxcycles;
    unsigned count;
};

class DefaultTimeReporter : implements ITimeReporter, public CInterface
{
    StringMapOf<TimeSectionInfo> *sections;
    CriticalSection c;
    TimeSectionInfo &findSection(unsigned idx)
    {
        CriticalBlock b(c);
        HashIterator iter(*sections);
        for(iter.first(); iter.isValid(); iter.next())
        {
             if (!idx--)
                 return (TimeSectionInfo &) iter.query();
        }
        throw MakeStringException(2, "Invalid index to DefaultTimeReporter");
    }
public:
    IMPLEMENT_IINTERFACE
    DefaultTimeReporter() 
    {
        sections = new StringMapOf<TimeSectionInfo>(true);
    }
    ~DefaultTimeReporter() 
    {
//      printTimings();                     // Must explicitly call printTimings - no automatic print (too late here!)
        delete sections;
    }
    virtual void report(ITimeReportInfo &cb)
    {
        CriticalBlock b(c);
        HashIterator iter(*sections);
        for(iter.first(); iter.isValid(); iter.next())
        {
            TimeSectionInfo &ts = (TimeSectionInfo &)iter.query();
            cb.report(ts.scope, ts.getTime(), ts.getMaxTime(), ts.count);
        }
    }
    virtual void addTiming(const char * scope, cycle_t cycles)
    {
        CriticalBlock b(c);
        TimeSectionInfo *info = sections->find(scope);
        if (info)
        {
            info->totalcycles += cycles;
            if (cycles > info->maxcycles) info->maxcycles = cycles;
            info->count++;
        }
        else
        {
            TimeSectionInfo &newinfo = * new TimeSectionInfo(scope, cycles);
            sections->replaceOwn(newinfo);
        }
    }
    virtual void reset()
    {
        CriticalBlock b(c);
        delete sections;
        sections = new StringMapOf<TimeSectionInfo>(true);
    }
    virtual StringBuffer &getTimings(StringBuffer &str)
    {
        CriticalBlock b(c);
        HashIterator iter(*sections);
        for(iter.first(); iter.isValid(); iter.next())
        {
            TimeSectionInfo &ts = (TimeSectionInfo &)iter.query();
            str.append("Timing: ").append(ts.scope)
                    .append(" total=")
                    .append(ts.getTime()/1000000)
                    .append("ms max=")
                    .append(ts.getMaxTime()/1000)
                    .append("us count=")
                    .append(ts.getCount())
                    .append(" ave=")
                    .append((ts.getTime()/1000)/ts.getCount())
                    .append("us\n");
        }
        if (numSections())
        {
            for (unsigned i = 0; i < numSections(); i++)
            {
            }
        }
        return str;
    }
    virtual void printTimings()
    {
        CriticalBlock b(c);
        if (numSections())
        {
            StringBuffer str;
            PrintLog(getTimings(str).str());
        }
    }
    virtual void mergeTiming(const char * scope, cycle_t totalcycles, cycle_t maxcycles, const unsigned count)
    {
        CriticalBlock b(c);
        TimeSectionInfo *info = sections->find(scope);
        if (!info)
        {
            info = new TimeSectionInfo(scope, totalcycles, maxcycles, count);
            sections->replaceOwn(*info);
        }
        else
        {
            info->totalcycles += totalcycles;
            if (maxcycles > info->maxcycles) info->maxcycles = maxcycles;
            info->count += count;
        }
    }
    virtual void mergeInto(ITimeReporter &other)
    {
        CriticalBlock b(c);
        HashIterator iter(*sections);
        for(iter.first(); iter.isValid(); iter.next())
        {
            TimeSectionInfo &ts = (TimeSectionInfo &) iter.query();
            other.mergeTiming(ts.scope, ts.totalcycles, ts.maxcycles, ts.count);
        }
    }
    virtual void merge(ITimeReporter &other)
    {
        CriticalBlock b(c);
        other.mergeInto(*this);
    }
protected:
    unsigned numSections()
    {
        return sections->count();
    }
};

static ITimeReporter * activeTimer = NULL;
ITimeReporter * queryActiveTimer()
{
    return activeTimer;
}


ITimeReporter *createStdTimeReporter() { return new DefaultTimeReporter(); }

cycle_t oneSecInCycles;
MODULE_INIT(INIT_PRIORITY_JDEBUG1)
{
    // perform v. early in process startup, ideally this would grab process exclusively for the 2 100ths of a sec it performs calc.
    calibrate_timing();
    oneSecInCycles = nanosec_to_cycle(1000000000);
    return 1;
}

MODULE_INIT(INIT_PRIORITY_JDEBUG2)
{
    activeTimer = new DefaultTimeReporter();
    return true;
}

MODULE_EXIT()
{
    ::Release(activeTimer);
    activeTimer = NULL;
}


//===========================================================================

#ifdef _WIN32

typedef enum _PROCESSINFOCLASS {
    ProcessBasicInformation,
    ProcessQuotaLimits,
    ProcessIoCounters,
    ProcessVmCounters,
    ProcessTimes,
    ProcessBasePriority,
    ProcessRaisePriority,
    ProcessDebugPort,
    ProcessExceptionPort,
    ProcessAccessToken,
    ProcessLdtInformation,
    ProcessLdtSize,
    ProcessDefaultHardErrorMode,
    ProcessIoPortHandlers,          // Note: this is kernel mode only
    ProcessPooledUsageAndLimits,
    ProcessWorkingSetWatch,
    ProcessUserModeIOPL,
    ProcessEnableAlignmentFaultFixup,
    ProcessPriorityClass,
    ProcessWx86Information,
    ProcessHandleCount,
    ProcessAffinityMask,
    ProcessPriorityBoost,
    ProcessDeviceMap,
    ProcessSessionInformation,
    ProcessForegroundInformation,
    ProcessWow64Information,
    MaxProcessInfoClass
} PROCESSINFOCLASS;

typedef LONG NTSTATUS;


struct __IO_COUNTERS {                  // defined in SDK
    ULONGLONG  ReadOperationCount;
    ULONGLONG  WriteOperationCount;
    ULONGLONG  OtherOperationCount;
    ULONGLONG ReadTransferCount;
    ULONGLONG WriteTransferCount;
    ULONGLONG OtherTransferCount;
};


struct VM_COUNTERS {
    unsigned long PeakVirtualSize;
    unsigned long VirtualSize;
    unsigned long PageFaultCount;
    unsigned long PeakWorkingSetSize;
    unsigned long WorkingSetSize;
    unsigned long QuotaPeakPagedPoolUsage;
    unsigned long QuotaPagedPoolUsage;
    unsigned long QuotaPeakNonPagedPoolUsage;
    unsigned long QuotaNonPagedPoolUsage;
    unsigned long PagefileUsage;
    unsigned long PeakPagefileUsage;
};

struct POOLED_USAGE_AND_LIMITS {
    unsigned long PeakPagedPoolUsage;
    unsigned long PagedPoolUsage;
    unsigned long PagedPoolLimit;
    unsigned long PeakNonPagedPoolUsage;
    unsigned long NonPagedPoolUsage;
    unsigned long NonPagedPoolLimit;
    unsigned long PeakPagefileUsage;
    unsigned long PagefileUsage;
    unsigned long PagefileLimit;
};
struct KERNEL_USER_TIMES {
    __int64 CreateTime;
    __int64 ExitTime;
    __int64 KernelTime;
    __int64 UserTime;
    //__int64 EllapsedTime; 
};



//
//NTSYSCALLAPI
//NTSTATUS
//NTAPI
//NtQueryInformationProcess(
//    IN HANDLE ProcessHandle,
//    IN PROCESSINFOCLASS ProcessInformationClass,
//    OUT PVOID ProcessInformation,
//    IN ULONG ProcessInformationLength,
//    OUT PULONG ReturnLength OPTIONAL
//    );


typedef LONG(WINAPI *PROCNTQIP)(HANDLE, UINT, PVOID, ULONG, PULONG);

static struct CNtKernelInformation
{
    CNtKernelInformation()
    {
        NtQueryInformationProcess = (PROCNTQIP)GetProcAddress(
            GetModuleHandle("ntdll"),
            "NtQueryInformationProcess"
        );
        GetSystemInfo(&SysBaseInfo);
}

    PROCNTQIP NtQueryInformationProcess;

    SYSTEM_INFO       SysBaseInfo;

} NtKernelFunctions;
#endif


//===========================================================================

#ifdef _WIN32
static __uint64 ticksToNs = I64C(100);  // FILETIME is in 100ns increments
#else
static __uint64 ticksToNs = I64C(1000000000) / sysconf(_SC_CLK_TCK);
#endif


#ifdef _WIN32
inline unsigned __int64 extractFILETIME(const FILETIME & value)
{
    return ((__uint64)value.dwHighDateTime << (sizeof(value.dwLowDateTime) * 8) | value.dwLowDateTime);
}
#endif

CpuInfo::CpuInfo(bool processTime, bool systemTime) : CpuInfo()
{
    if (processTime)
        getProcessTimes();
    else if (systemTime)
        getSystemTimes();
}

void CpuInfo::clear()
{
    user = 0;
    system = 0;
    idle = 0;
    iowait = 0;
}

CpuInfo CpuInfo::operator - (const CpuInfo & rhs) const
{
    CpuInfo result;
    result.user = user - rhs.user;
    result.system = system - rhs.system;
    result.idle = idle - rhs.idle;
    result.iowait = iowait - rhs.iowait;
    result.ctx = ctx - rhs.ctx;
    return result;
}

bool CpuInfo::getProcessTimes()
{
#ifdef _WIN32
    FILETIME creationTime, exitTime;
    FILETIME kernelTime, userTime;
    GetProcessTimes(GetCurrentProcess(), &creationTime, &exitTime, &kernelTime, &userTime);

    user = extractFILETIME(userTime);
    system = extractFILETIME(kernelTime);
    return true;
#else
    VStringBuffer fname("/proc/%u/stat", getpid());
    //NOTE: This file needs to be reopened each time - seeking to the start and rereading does not refresh it
    clear();
    FILE* cpufp = fopen(fname.str(), "r");
    if (!cpufp) {
        return false;
    }
    char ln[2560];
    if (fgets(ln, sizeof(ln), cpufp))
    {
        long int majorFaults = 0;
        long unsigned userTime = 0;
        long unsigned systemTime = 0;
        long unsigned childUserTime = 0;
        long unsigned childSystemTime = 0;
        int matched = sscanf(ln, "%*d %*s %*c %*d %*d %*d %*d %*d %*u %*u %*u %lu %*u %lu %lu %lu %lu", &majorFaults, &userTime, &systemTime, &childUserTime, &childSystemTime);
        if (matched >= 3)
        {
            user = userTime + childUserTime;
            system = systemTime + childSystemTime;
        }
    }
    fclose(cpufp);
    return true;
#endif
}

bool CpuInfo::getSystemTimes()
{
#ifdef _WIN32
    FILETIME idleTime, kernelTime, userTime;
    GetSystemTimes(&idleTime, &kernelTime, &userTime);
    // note - kernel time seems to include idle time

    idle = extractFILETIME(idleTime);
    user = extractFILETIME(userTime);
    system = extractFILETIME(kernelTime) - idle;
    return true;
#else
    //NOTE: This file needs to be reopened each time - seeking to the start and rereading does not refresh it
    FILE* cpufp = fopen("/proc/stat", "r");
    if (!cpufp) {
        clear();
        return false;
    }
    char ln[256];
    while (fgets(ln, sizeof(ln), cpufp))
    {
        if (strncmp(ln, "cpu ", 4) == 0)
        {
            int items;
            __uint64 nice, irq, softirq;

            items = sscanf(ln,
                "cpu %llu %llu %llu %llu %llu %llu %llu",
                &user, &nice,
                &system,
                &idle,
                &iowait,
                &irq, &softirq);

            user += nice;
            if (items == 4)
                iowait = 0;
            if (items == 7)
                system += irq + softirq;
        }
        else if (strncmp(ln, "ctxt ", 5) == 0)
        {
            (void)sscanf(ln, "ctxt %llu", &ctx);
        }
    }
    fclose(cpufp);
    return true;
#endif
}

unsigned CpuInfo::getPercentCpu() const
{
    __uint64 total = getTotal();
    if (total == 0)
        return 0;
    unsigned percent = (unsigned)(((total - idle) * 100) / total);
    if (percent > 100)
        percent = 100;
    return percent;
}

__uint64 CpuInfo::getSystemNs() const
{
    return system * ticksToNs;
}

__uint64 CpuInfo::getUserNs() const
{
    return user * ticksToNs;
}

__uint64 CpuInfo::getTotalNs() const
{
    return getTotal() * ticksToNs;
}

unsigned CpuInfo::getIdlePercent() const
{
    __uint64 total = getTotal();
    if (total == 0)
        return 0;
    return (unsigned)((idle * 100) / total);
}

unsigned CpuInfo::getIoWaitPercent() const
{
    __uint64 total = getTotal();
    if (total == 0)
        return 0;
    return (unsigned)((iowait * 100) / total);
}

unsigned CpuInfo::getSystemPercent() const
{
    __uint64 total = getTotal();
    if (total == 0)
        return 0;
    return (unsigned)((system * 100) / total);
}

unsigned CpuInfo::getUserPercent() const
{
    __uint64 total = getTotal();
    if (total == 0)
        return 0;
    return (unsigned)((user * 100) / total);
}

//===========================================================================

// Performance Monitor

#ifdef _WIN32

memsize_t getMapInfo(const char *type)
{
    return 0; // TODO/UNKNOWN
}

memsize_t getVMInfo(const char *type)
{
    return 0; // TODO/UNKNOWN
}

void getCpuInfo(unsigned &numCPUs, unsigned &CPUSpeed)
{
    // MORE: Might be a better way to get CPU speed (actual) than the one stored in Registry
    LONG  lRet;
    HKEY  hKey;
    DWORD keyValue;
    DWORD valueLen = sizeof(keyValue);

    if ((lRet = RegOpenKeyEx(HKEY_LOCAL_MACHINE, "HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", 0L, KEY_READ , &hKey)) != ERROR_SUCCESS)
    {
        DBGLOG("RegOpenKeyEx(HKEY_LOCAL_MACHINE, ...) failed to open CentralProcessor\\0 - SysErrorCode=%d", lRet);
    }
    else if ((lRet = RegQueryValueEx(hKey, TEXT("~MHz"), NULL, NULL, (LPBYTE) &keyValue, &valueLen)) != ERROR_SUCCESS)
    {
        DBGLOG("RegQueryValueEx() failed to get CPU speed - errorCode=%d", lRet);
        RegCloseKey(hKey);
    }
    else 
    {
        CPUSpeed = keyValue;
        RegCloseKey(hKey);
    }
    

    SYSTEM_INFO sysInfo;
    GetSystemInfo(&sysInfo);
    numCPUs = sysInfo.dwNumberOfProcessors;

}

static unsigned evalAffinityCpus()
{
    unsigned numCpus = 0;
    DWORD ProcessAffinityMask, SystemAffinityMask;
    if (GetProcessAffinityMask(GetCurrentProcess(), (PDWORD_PTR)&ProcessAffinityMask, (PDWORD_PTR)&SystemAffinityMask))
    {
        unsigned i = 0;
        while (ProcessAffinityMask)
        {
            if (ProcessAffinityMask & 1)
                ++numCpus;
            ProcessAffinityMask >>=1;
        }
    }
    else // fall back to legacy num system cpus
    {
        Owned<IException> e = makeOsException(GetLastError(), "Failed to get affinity");
        EXCLOG(e, NULL);
        unsigned cpuSpeed;
        getCpuInfo(numCpus, cpuSpeed);
        return numCpus;
    }
    return numCpus;
}

#else // linux

memsize_t getMapInfo(const char *type)
{
    // NOTE: 'total' heap value includes Roxiemem allocation, if present
    enum mapList { HEAP, STACK, SBRK, ANON };
    enum mapList mapType;
    if ( streq(type, "heap") )
        mapType = HEAP;
    else if ( streq(type, "stack") )
        mapType = STACK;
    else if ( streq(type, "sbrk") )
        mapType = SBRK;
    else if ( streq(type, "anon") )
        mapType = ANON;
    else
        return 0;

    memsize_t ret = 0;
    VStringBuffer procMaps("/proc/%d/maps", GetCurrentProcessId());
    FILE *diskfp = fopen(procMaps.str(), "r");
    if (!diskfp)
        return false;
    char ln[256];

/*
 *  exmaple /proc/<pid>/maps format:
 *  addr_start  -addr_end     perms offset   dev   inode      pathname
 *  01c3a000-01c5b000         rw-p  00000000 00:00 0          [heap]
 *  7f3f25217000-7f3f25a40000 rw-p  00000000 00:00 0          [stack:2362]
 *  7f4020a40000-7f4020a59000 rw-p  00000000 00:00 0
 *  7f4020a59000-7f4020a5a000 ---p  00000000 00:00 0
 *  7f4029bd4000-7f4029bf6000 r-xp  00000000 08:01 17576135   /lib/x86_64-linux-gnu/ld-2.15.so
 */

    while (fgets(ln, sizeof(ln), diskfp))
    {
        bool skipline = true;
        if ( mapType == HEAP || mapType == ANON ) // 'general' heap includes anon mmapped + sbrk
        {
            // skip file maps (beginning with /) and all other regions (except [heap if selected)
            if ( (mapType == HEAP && strstr(ln, "[heap")) || (!strstr(ln, " /") && !strstr(ln, " [")) )
            {
                // include only (r)ead + (w)rite and (p)rivate (not shared), skipping e(x)ecutable
                // and ---p guard regions
                if ( strstr(ln, " rw-p") )
                    skipline = false;
            }
        }
        else if ( mapType == STACK )
        {
            if ( strstr(ln, "[stack") )
                skipline = false;
        }
        else if ( mapType == SBRK )
        {
            if ( strstr(ln, "[heap") )
                skipline = false;
        }
        if ( !skipline )
        {
            unsigned __int64 addrLow, addrHigh;
            if (2 == sscanf(ln, "%16" I64F "x-%16" I64F "x", &addrLow, &addrHigh))
                ret += (memsize_t)(addrHigh-addrLow);
        }
    }
    fclose(diskfp);
    return ret;
}

static bool matchExtract(const char * prefix, const char * line, memsize_t & value)
{
    size32_t len = strlen(prefix);
    if (strncmp(prefix, line, len)==0)
    {
        char * tail = NULL;
        value = strtol(line+len, &tail, 10);
        while (isspace(*tail))
            tail++;
        if (strncmp(tail, "kB", 2) == 0)
            value *= 0x400;
        else if (strncmp(tail, "mB", 2) == 0)
            value *= 0x100000;
        else if (strncmp(tail, "gB", 2) == 0)
            value *= 0x40000000;
        return true;
    }
    return false;
}

memsize_t getVMInfo(const char *type)
{
    memsize_t ret = 0;
    VStringBuffer name("%s:", type);
    VStringBuffer procMaps("/proc/self/status");
    FILE *diskfp = fopen(procMaps.str(), "r");
    if (!diskfp)
        return 0;
    char ln[256];
    while (fgets(ln, sizeof(ln), diskfp))
    {
        if (matchExtract(name.str(), ln, ret))
            break;
    }
    fclose(diskfp);
    return ret;
}

void getCpuInfo(unsigned &numCPUs, unsigned &CPUSpeed)
{
    numCPUs = 1;
    CPUSpeed = 0;
#ifdef __APPLE__
# if defined(_SC_NPROCESSORS_CONF)
    int ncpus = sysconf(_SC_NPROCESSORS_CONF);
    if (ncpus > 0)
        numCPUs = ncpus;
# endif
#else // linux
    // NOTE: Could have perhaps used sysconf(_SC_NPROCESSORS_CONF) for numCPUs

    FILE *cpufp = fopen("/proc/cpuinfo", "r");
    if (cpufp == NULL)
        return;

    char * tail;
    // MORE: It is a shame that the info in this file (/proc/cpuinfo) are formatted (ie tabs .. etc)
    const char *cpuNumTag = "processor\t:";
    const char *cpuSpeedTag = "cpu MHz\t\t:";

    // NOTE: This provides current cpu freq, not max

    numCPUs = 0;
    char line[1001];
    const char *bufptr;
    while ((bufptr = fgets(line, 1000, cpufp)) != NULL)
    {
        if (strncmp(cpuNumTag, bufptr, strlen(cpuNumTag))==0) 
            numCPUs++;
        else if (strncmp(cpuSpeedTag, bufptr, strlen(cpuSpeedTag))==0) 
            CPUSpeed = (unsigned)strtol(bufptr+strlen(cpuSpeedTag), &tail, 10);
    }

    fclose(cpufp);
    if (numCPUs < 1)
        numCPUs = 1;

    // max cpu freq (KHz) may be in:
    // /sys/devices/system/cpu/cpu[0-X]/cpufreq/cpuinfo_max_freq

    cpufp = fopen("/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_max_freq", "r");
    if (cpufp != NULL)
    {
        unsigned CPUSpeedMax = 0;
        int srtn = fscanf(cpufp, "%u", &CPUSpeedMax);
        if (srtn == 1)
        {
            CPUSpeedMax /= 1000;
            if (CPUSpeedMax > CPUSpeed)
                CPUSpeed = CPUSpeedMax;
        }
        fclose(cpufp);
    }
#endif
}

static unsigned evalAffinityCpus()
{
#ifdef __APPLE__
    // MORE - could do better
#else
    cpu_set_t cpuset;
    int err = pthread_getaffinity_np(GetCurrentThreadId(), sizeof(cpu_set_t), &cpuset);
    if (0 == err)
    {
#if __GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 6)
        return CPU_COUNT(&cpuset);
#else
        unsigned numCpus = 0;
        unsigned setSize = CPU_SETSIZE;
        while (setSize--)
        {
            if (CPU_ISSET(setSize, &cpuset))
                ++numCpus;
        }
        return numCpus;
#endif /* GLIBC */
    }
#endif
    return 1;
}

// Note - values are returned in Kb

static void getMemUsage(unsigned &inuse,unsigned &active,unsigned &total,unsigned &swaptotal,unsigned &swapinuse) 
{
#ifdef __APPLE__
    active = 0;
    inuse = 0;
    total = 0;
    swaptotal = 0;
    swapinuse = 0;

    vm_size_t pageSize;
    if (KERN_SUCCESS != host_page_size(mach_host_self(), &pageSize))
        return;
    mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
    vm_statistics64_data_t vmstat;
    if (KERN_SUCCESS != host_statistics64(mach_host_self(), HOST_VM_INFO64, (host_info64_t)&vmstat, &count))
        return;

    uint64_t totalBytes = (vmstat.wire_count + vmstat.active_count + vmstat.inactive_count + vmstat.free_count + vmstat.compressor_page_count) * pageSize;
    uint64_t inuseBytes = (vmstat.wire_count + vmstat.active_count + vmstat.inactive_count + vmstat.compressor_page_count) * pageSize;
    uint64_t activeBytes = (vmstat.wire_count + vmstat.active_count) * pageSize;

    active = activeBytes / 1024;
    inuse = inuseBytes / 1024;
    total = totalBytes / 1024;
    // swaptotal and swapinuse TBD
#else
    unsigned free=0;
    unsigned swapfree=0;
    active = 0;
    inuse = 0;
    static int memfd = -1;
    if (memfd==-1)
        memfd = open("/proc/meminfo",O_RDONLY);
    if (memfd==-1)
        return;
    char buf[2048];
    size32_t l = pread(memfd, buf, sizeof(buf)-1, 0L);
    if ((int)l<=0)
        return;
    buf[l] = 0;
    const char *bufptr = buf;
    char * tail;
    unsigned i = 7; // supposed to match max number of items extract below

    total = swaptotal = free = active = swapfree = 0;
    unsigned swapcached = 0;
    unsigned cached = 0;
    while (bufptr&&i) {
        if (*bufptr =='\n') 
            bufptr++;
        i--;
        if (strncmp("MemTotal:", bufptr, 9)==0) 
            total = (unsigned)strtol(bufptr+9, &tail, 10);
        else if (strncmp("SwapTotal:", bufptr, 10)==0) 
            swaptotal = (unsigned)strtol(bufptr+10, &tail, 10);
        else if (strncmp("MemFree:", bufptr, 8)==0) 
            free = (unsigned)strtol(bufptr+8, &tail, 10);
        else if (strncmp("Active:", bufptr, 7)==0) 
            active = (unsigned)strtol(bufptr+7, &tail, 10);
        else if (strncmp("SwapFree:", bufptr, 9)==0) 
            swapfree = (unsigned)strtol(bufptr+9, &tail, 10);
        else if (strncmp("Cached:", bufptr, 7)==0) 
            cached = (unsigned)strtol(bufptr+7, &tail, 10);
        else if (strncmp("SwapCached:", bufptr, 11)==0) 
            swapcached = (unsigned)strtol(bufptr+11, &tail, 10);
        else 
            i++;
        bufptr = strchr(bufptr, '\n');
    }
    inuse = total-free-cached;
    swapinuse = swaptotal-swapfree-swapcached;
#endif
}

class CInt64fix 
{
    __int64 val;
public:
    CInt64fix()
    {
        val = 0;
    }
    void set(int v) 
    {
        __int64 ret = (unsigned)v;
        while (val-ret>0x80000000LL) 
            ret += 0x100000000LL;
        val = ret;
    }

    __int64 get()
    {
        return val;
    }
};
    


void getMemStats(StringBuffer &out, unsigned &memused, unsigned &memtot)
{
#ifdef __linux__
    __int64 total = getMapInfo("heap");
    __int64 sbrkmem = getMapInfo("sbrk");
    __int64 mmapmem = total - sbrkmem;
    __int64 virttot = getVMInfo("VmData");
    unsigned mu;
    unsigned ma;
    unsigned mt;
    unsigned st;
    unsigned su;
    getMemUsage(mu,ma,mt,st,su);
    unsigned muval = (unsigned)(((__int64)mu+(__int64)su)*100/((__int64)mt+(__int64)st));
    if (sizeof(memsize_t)==4) {
        unsigned muval2 = (virttot*100)/(3*(__int64)0x40000000);
        if (muval2>muval)
            muval = muval2;
    }
    if (muval>100)
        muval = 100; // !


    out.appendf("MU=%3u%% MAL=%" I64F "d MMP=%" I64F "d SBK=%" I64F "d TOT=%uK RAM=%uK SWP=%uK", 
        muval, total, mmapmem, sbrkmem, (unsigned)(virttot/1024), mu, su);
#ifdef _USE_MALLOC_HOOK
    if (totalMem) 
        out.appendf(" TM=%" I64F "d",totalMem);
#endif
    memused = mu+su;
    memtot = mt+st;
#elif defined (__APPLE__)
    __uint64 bytes;
    size_t len = sizeof(bytes);
    sysctlbyname("hw.memsize", &bytes, &len, NULL, 0);
    // See http://miknight.blogspot.com/2005/11/resident-set-size-in-mac-os-x.html
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
    task_info(current_task(), TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
    out.appendf("RES=%" I64F "uMiB VIRT=%" I64F "uMiB TOT=%" I64F "uMiB",
            (__uint64) t_info.resident_size/(1024*1024),
            (__uint64) t_info.virtual_size/(1024*1024),
            bytes/(1024*1024));
    memused = t_info.resident_size;
    memtot = t_info.virtual_size;
#elif defined (__FreeBSD___)
    UNIMPLEMENTED;
#endif
}


void getDiskUsage(char const * path, unsigned __int64 & total, unsigned __int64 & inUse)
{
#if defined(__linux__) || defined(__APPLE__)
    struct statfs stfs;
    if(statfs(path, &stfs) < 0)
    {
        //PrintLog("statfs error for filesystem '%s'", path);
        total = inUse = 0;
    }
    else
    {
        struct stat st;
        if(stat(path, &st) < 0)
        {
            //PrintLog("stat error for filesystem '%s'", path);
            total = inUse = 0;
        }
        else
        {
            total = (unsigned __int64)stfs.f_blocks * st.st_blksize;
            inUse = total - (unsigned __int64)stfs.f_bfree * st.st_blksize;
        }
    }
#else
    total = inUse = 0;
#endif
}

#endif

static std::atomic<unsigned> cachedNumCpus;
unsigned getAffinityCpus()
{
    if (cachedNumCpus.load(std::memory_order_acquire) == 0)
        cachedNumCpus.store(evalAffinityCpus(), std::memory_order_release);
    return cachedNumCpus.load(std::memory_order_acquire);
}
void clearAffinityCache()
{
    cachedNumCpus.store(0, std::memory_order_release);
}


void getPeakMemUsage(memsize_t &peakVm,memsize_t &peakResident)
{
    peakVm = 0;
    peakResident = 0;

#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(), &pmc, sizeof(pmc)))
    {
        peakVm = pmc.PeakWorkingSetSize;
        peakResident = pmc.PeakWorkingSetSize;

    }
#else
    static int memfd = -1;
    if (memfd==-1)
        memfd = open("/proc/self/status",O_RDONLY);
    if (memfd==-1)
        return;
    char buf[2048];
    size32_t l = pread(memfd, buf, sizeof(buf)-1, 0L);
    if ((int)l<=0)
        return;
    buf[l] = 0;

    const char *bufptr = buf;
    while (bufptr) {
        if (*bufptr =='\n')
            bufptr++;
        if (!matchExtract("VmPeak:", bufptr, peakVm) &&
            !matchExtract("VmHWM:", bufptr, peakResident))
        {
            //ignore this line
        }
        bufptr = strchr(bufptr, '\n');
    }
#endif
}

#define RXMAX 1000000       // can be 10x bigger but this produces reasonable amounts

unsigned packetsLasttime;
__int64 packetsLastrx = -1;
__int64 packetsLasttx;

bool getPacketStats(unsigned & tx, unsigned & rx)
{
    unsigned thistime = msTick();
    JSocketStatistics jstats;
    getSocketStatistics(jstats);
    bool ret = true;
    if(packetsLastrx!=-1)
    {
        tx = (unsigned) ((jstats.writesize-packetsLasttx)*100000/(((__int64)(packetsLasttime-thistime))*RXMAX));
        rx = (unsigned) ((jstats.readsize-packetsLastrx)*100000/(((__int64)(packetsLasttime-thistime))*RXMAX));
    }
    else
        ret = false;
    packetsLastrx = jstats.readsize;
    packetsLasttx = jstats.writesize;
    packetsLasttime = thistime;
    return ret;
}

#ifndef _WIN32

struct UserStatusInfo
{
public:
    UserStatusInfo(pid_t _pid)
    {
        pid = _pid;
    }

    bool update()
    {
        StringBuffer fn;
        fn.appendf("/proc/%d/stat", pid);
        char buf[800]; /* about 40 fields, 64-bit decimal is about 20 chars */
        int fd = open(fn.str(), O_RDONLY, 0);
        if (fd==-1)
            return false;
        int rd = read(fd, buf, sizeof(buf)-1);
        close(fd);
        if (rd<80)
            return false;
        buf[rd] = 0;

        char *s = strchr(buf,'(');
        if (!s)
            return false;
        s++;
        unsigned i = 0;
        while (*s&&(*s!=')')&&(i<15))
            cmd[i++] = *(s++);
        if (*s != ')')
            return false;
        cmd[i] = 0;
        s+=3; // Skip ") X" where X is the state

        //The PID of the parent process
        const char *num;
        s = skipnumfld(s,num);
        //int ppid = atoi(num);

        // skip pgrp, session, tty_num, tpgid, flags, min_flt, cmin_flt, maj_flt, cmaj_flt
        for (i=0;i<9;i++)
            s = skipnumfld(s,num);

        //utime - user mode time in clock ticks.
        s = skipnumfld(s,num);
        //printf("**N'%s'\n",num);
        time.user = (unsigned)atoi64_l(num,strlen(num));

        //stime - amount of time scheduled in kernel mode in clock ticks
        s = skipnumfld(s,num);
        //printf("**N'%s'\n",num);
        time.system = (unsigned)atoi64_l(num,strlen(num));

        return true;
    }

public:
    pid_t pid;
    char cmd[16];
    UserSystemTime_t time;

private:
    char *skipnumfld(char *s, const char *&num)
    {
        while (*s && isspace(*s))
            s++;
        num = s;
        if ((*s=='-')||(*s=='+'))
            s++;
        while (*s && isdigit(*s))
            s++;
        if (*s==' ')
            *(s++) = 0;      // terminate num
        while (*s && isspace(*s))
            s++;
        return s;
    }
};

struct CProcInfo: extends CInterface
{
    UserStatusInfo info;
    UserSystemTime_t delta;
    bool active;
    bool first;

    CProcInfo(int _pid) : info(_pid)
    {
        active = false;
        first = true;
    }

    inline pid_t pid() const { return info.pid; }

    bool load()
    {
        UserSystemTime_t prev = info.time;
        if (!info.update())
            return false;

        active = true;
        if (first) 
            first = false;
        else {
            delta.system = info.time.system-prev.system;
            delta.user = info.time.user-prev.user;
        }
        return true;
    }
};

class CProcessMonitor
{
    CIArrayOf<CProcInfo> processes;
    unsigned tot_time;
    bool busy;
    CriticalSection sect;

    static int compare(CInterface * const *i1, CInterface * const *i2)
    {
        CProcInfo *pi1 = QUERYINTERFACE(*i1,CProcInfo);
        CProcInfo *pi2 = QUERYINTERFACE(*i2,CProcInfo);
        return pi2->delta.system+pi2->delta.user-pi1->delta.system-pi1->delta.user;
    }
public:
    CProcessMonitor()
    {
        busy = false;
    }

    void scan()
    {
#ifdef __linux__
        ForEachItemIn(i1,processes)
            processes.item(i1).active = false;
        DIR *dir = opendir("/proc");
        for (;;) {
            CriticalBlock b(sect);
            struct dirent *ent = readdir(dir);
            if (!ent)
                break;
            if ((ent->d_name[0]>='0')&&(ent->d_name[0]<='9')) {
                int pid = atoi(ent->d_name);
                if (pid) {
                    CProcInfo *pi = NULL;
                    ForEachItemIn(i2,processes) {
                        if (processes.item(i2).pid() == pid) {
                            pi = &processes.item(i2);
                            break;
                        }
                    }
                    if (!pi) {
                        pi = new CProcInfo(pid);
                        processes.append(*pi);
                    }
                    pi->load();
                }
            }
        }
        closedir(dir);
        tot_time = 0;
        ForEachItemInRev(i3,processes) {
            CProcInfo &pi = processes.item(i3);
            if (pi.active) 
                tot_time += pi.delta.system+pi.delta.user;
            else
                processes.remove(i3);
        }
#endif
#if defined (__FreeBSD__) || defined (__APPLE__)
        UNIMPLEMENTED;
#endif
    }
    void print(unsigned n,StringBuffer &str)
    {
        if (!tot_time)
            return;
        assertex(n);
        processes.sort(compare);
        StringBuffer name;
        ForEachItemIn(i1,processes) {
            CProcInfo &pi = processes.item(i1);
            if ((pi.delta.system==0)&&(pi.delta.user==0))
                break;
            getThreadName(pi.pid(),0,name.clear());
            str.appendf("\n TT: PI=%d PN=%s PC=%d ST=%d UT=%d%s%s",
                        pi.pid(),pi.info.cmd,(pi.delta.system+pi.delta.user)*100/tot_time,pi.delta.system,pi.delta.user,name.length()?" TN=":"",name.str());
            if (--n==0)
                break;
        }
    }

    void printBusy(unsigned pc,StringBuffer &str)
    {
        if (pc>90) {
            scan();
            if (busy) 
                print(3,str); // print top 3
            else
                busy = true;
        }
        else {
            busy = false;
            processes.kill();
        }
    }
};

#ifndef HZ
#define HZ 100
#endif


#define IDE0_MAJOR  3
#define SCSI_DISK0_MAJOR 8
#define SCSI_DISK1_MAJOR    65
#define SCSI_DISK7_MAJOR    71
#define SCSI_DISK10_MAJOR   128
#define SCSI_DISK17_MAJOR   135
#define IDE1_MAJOR  22
#define IDE2_MAJOR  33
#define IDE3_MAJOR  34
#define IDE4_MAJOR  56
#define IDE5_MAJOR  57
#define IDE6_MAJOR  88
#define IDE7_MAJOR  89
#define IDE8_MAJOR  90
#define IDE9_MAJOR  91
#define COMPAQ_SMART2_MAJOR 72

#define IDE_DISK_MAJOR(M)   ((M) == IDE0_MAJOR || (M) == IDE1_MAJOR || \
                (M) == IDE2_MAJOR || (M) == IDE3_MAJOR || \
                (M) == IDE4_MAJOR || (M) == IDE5_MAJOR || \
                (M) == IDE6_MAJOR || (M) == IDE7_MAJOR || \
                (M) == IDE8_MAJOR || (M) == IDE9_MAJOR)


#define SCSI_DISK_MAJOR(M) ((M) == SCSI_DISK0_MAJOR || \
  ((M) >= SCSI_DISK1_MAJOR && (M) <= SCSI_DISK7_MAJOR) || \
  ((M) >= SCSI_DISK10_MAJOR && (M) <= SCSI_DISK17_MAJOR))

#define OTHER_DISK_MAJOR(M) ((M) == COMPAQ_SMART2_MAJOR) // by investigation!

// nvme disk major is often 259 (blkext) but others are also

//---------------------------------------------------------------------------

class CExtendedStats  // Disk network and cpu stats
{

    struct blkio_info 
    {
        unsigned rd_ios;        // Read I/O operations 
        unsigned rd_merges;     // Reads merged 
        __uint64 rd_sectors;    // Sectors read 
        unsigned rd_ticks;      // Time in queue + service for read 
        unsigned wr_ios;        // Write I/O operations 
        unsigned wr_merges;     // Writes merged 
        __uint64 wr_sectors;    // Sectors written 
        unsigned wr_ticks;      // Time in queue + service for write 
        unsigned ticks;         // Time of requests in queue 
        unsigned aveq;          // Average queue length 
    };

    struct cpu_info 
    {
        __uint64 user;
        __uint64 system;
        __uint64 idle;
        __uint64 iowait;
    };

    struct net_info
    {
        __uint64 rxbytes;
        __uint64 rxpackets;
        __uint64 rxerrors;
        __uint64 rxdrops;
        __uint64 txbytes;
        __uint64 txpackets;
        __uint64 txerrors;
        __uint64 txdrops;
    };

    struct part_info 
    {
        unsigned int major; 
        unsigned int minor; 
        char name[32];
    };
    

    part_info *partition;
    unsigned nparts;
    blkio_info *newblkio;
    blkio_info *oldblkio;
    CpuInfo newcpu;
    unsigned numcpu;
    CpuInfo oldcpu;
    CpuInfo cpu;
    net_info oldnet;
    net_info newnet;
    unsigned ncpu;
    bool first;
    char *kbuf;
    size32_t kbufmax;
    int kbadcnt;
    unsigned short kbufcrc;
    __uint64 totalcpu;

    unsigned ndisks;

    StringBuffer ifname;

#ifdef __linux__
    UnsignedArray diskMajorMinor;
#endif

    bool isDisk(unsigned int major, unsigned int minor)
    {
#ifdef __linux__
        unsigned mm = (major<<16)+minor;
        bool found = diskMajorMinor.contains(mm);
        if (found)
            return true;
#endif
        if (IDE_DISK_MAJOR(major)) 
            return ((minor&0x3F)==0);
        if (SCSI_DISK_MAJOR(major)) 
            return ((minor&0x0F)==0);
        if (OTHER_DISK_MAJOR(major)) 
            return ((minor&0x0F)==0);
        return 0;
    }


    bool getNextCPU()
    {
        if (!ncpu) {
            unsigned speed;
            getCpuInfo(ncpu, speed);
            if (!ncpu)
                ncpu = 1;
        }

        oldcpu = newcpu;
        if (newcpu.getSystemTimes())
        {
            cpu = newcpu - oldcpu;
            totalcpu = cpu.getTotal();
            return true;
        }
        else
        {
            cpu.clear();
            totalcpu = 0;
            return false;
        }
    }

    bool getDiskInfo()
    {
        char ln[256];
        part_info pi;
        FILE* diskfp = fopen("/proc/diskstats", "r");
        if (!diskfp)
            return false;
        if (!newblkio)
        {
            nparts = 0;
            while (fgets(ln, sizeof(ln), diskfp))
            {
                unsigned reads = 0;
                if (sscanf(ln, "%4d %4d %31s %u", &pi.major, &pi.minor, pi.name, &reads) == 4)
                {
                    unsigned p = 0;
                    while ((p<nparts) && (partition[p].major != pi.major || partition[p].minor != pi.minor))
                        p++;
                    if ((p==nparts) && reads && isDisk(pi.major,pi.minor))
                    {
                        nparts++;
                        partition = (part_info *)realloc(partition,nparts*sizeof(part_info));
                        partition[p] = pi;
                    }
                }
            }
            free(newblkio);
            free(oldblkio);
            newblkio = (blkio_info *)calloc(sizeof(blkio_info),nparts);
            oldblkio = (blkio_info* )calloc(sizeof(blkio_info),nparts);
        }
        rewind(diskfp);
        // could skip lines we know aren't significant here
        while (fgets(ln, sizeof(ln), diskfp))
        {
            blkio_info blkio;
            unsigned items = sscanf(ln, "%4d %4d %*s %u %u %llu %u %u %u %llu %u %*u %u %u",
                       &pi.major, &pi.minor,
                       &blkio.rd_ios, &blkio.rd_merges,
                       &blkio.rd_sectors, &blkio.rd_ticks, 
                       &blkio.wr_ios, &blkio.wr_merges,
                       &blkio.wr_sectors, &blkio.wr_ticks,
                       &blkio.ticks, &blkio.aveq);
            if (items == 6)
            {
                // hopefully not this branch!
                blkio.rd_sectors = blkio.rd_merges;
                blkio.wr_sectors = blkio.rd_ticks;
                blkio.rd_ios = 0;
                blkio.rd_merges = 0;
                blkio.rd_ticks = 0;
                blkio.wr_ios = 0;
                blkio.wr_merges = 0;
                blkio.wr_ticks = 0;
                blkio.ticks = 0;
                blkio.aveq = 0;
                items = 12;
            }
            if (items == 12)
            {
                for (unsigned p = 0; p < nparts; p++)
                {
                    if (partition[p].major == pi.major && partition[p].minor == pi.minor)
                    {
                        newblkio[p] = blkio;
                        break;
                    }
                }
            }
        }
        fclose(diskfp);
        return true;
    }


    bool getNetInfo()
    {
        FILE *netfp = fopen("/proc/net/dev", "r");
        if (!netfp)
            return false;
        char ln[512];
        // Read two lines
        if (!fgets(ln, sizeof(ln), netfp) || !fgets(ln, sizeof(ln), netfp)) {
            fclose(netfp);
            return false;
        }
        unsigned txskip = 2;
        bool hasbyt = false;
        if (strstr(ln,"compressed")) {
            txskip = 4;
            hasbyt = true;
        }
        else if (strstr(ln,"bytes")) 
            hasbyt = true;

        while (fgets(ln, sizeof(ln), netfp)) {
            const char *s = ln;
            skipSp(s);
            size_t ilen = ifname.length();
            if ( (strncmp(s, ifname.str(), ilen)==0) && (s[ilen]==':') ) {
                s+=(ilen+1);
                skipSp(s);
                if (hasbyt) {
                    newnet.rxbytes = readDecNum(s);
                    skipSp(s);
                }
                else
                    newnet.rxbytes = 0;
                newnet.rxpackets = readDecNum(s);
                skipSp(s);
                newnet.rxerrors = readDecNum(s);
                skipSp(s);
                newnet.rxdrops = readDecNum(s);
                skipSp(s);
                while (txskip--) {
                    readDecNum(s);
                    skipSp(s);
                }
                if (hasbyt) {
                    newnet.txbytes = readDecNum(s);
                    skipSp(s);
                }
                else
                    newnet.txbytes = 0;
                newnet.txpackets = readDecNum(s);
                skipSp(s);
                newnet.txerrors = readDecNum(s);
                skipSp(s);
                newnet.txdrops = readDecNum(s);
                break;
            }
        }
        fclose(netfp);
        return true;
    }

    size32_t getKLog(const char *&data)
    {
#ifdef __linux__
        if (kbufmax)
        {
            data = nullptr;
            size32_t ksz = 0;
            unsigned short lastCRC = 0;
            // NOTE: allocated 2*kbufmax to work around kernel bug
            // where klogctl() could sometimes return more than requested
            size32_t sz = klogctl(3, kbuf, kbufmax);
            if ((int)sz < 0)
            {
                if (kbadcnt < 5)
                {
                    IERRLOG("klogctl SYSLOG_ACTION_READ_ALL error %d", errno);
                    kbadcnt++;
                }
                else
                    kbufmax = 0;
                return 0;
            }
#if 0
            kbuf[sz] = '\0';
#endif
            if (kbufcrc)
            {
                data = kbuf;
                ksz = sz;
            }
            // determine where new info starts ...
            StringBuffer ln;
            const char *p = kbuf;
            const char *e = p+sz;
            while (p && p!=e)
            {
                if (*p=='<')
                {
                    ln.clear();
                    while ((p && p!=e)&&(*p!='\n'))
                    {
                        ln.append(*p);
                        p++;
                        sz--;
                    }
                    lastCRC = chksum16(ln.str(), ln.length());
                    if (kbufcrc && kbufcrc == lastCRC)
                    {
                        ksz = sz - 1;
                        if (ksz && sz)
                            data = p + 1;
                        else
                            data = nullptr;
                    }
                }
                while ((p && p!=e)&&(*p!='\n'))
                {
                    p++;
                    sz--;
                }
                if (p && p!=e)
                {
                    p++;
                    sz--;
                }
            }
            if (lastCRC)
                kbufcrc = lastCRC;
            if (!ksz)
                data = nullptr;
            return ksz;
        }
#endif
        data = nullptr;
        return 0;
    }


    inline double perSec(double v,double deltams) 
    {
        return 1000.0*v/deltams;
    }


public:

    unsigned getCPU()
    {
        if (!getNextCPU())
            return (unsigned)-1;
        return cpu.getPercentCpu();
    }



    CExtendedStats(bool printklog)
    {
        partition = (part_info *)malloc(sizeof(part_info));
        nparts = 0;
        newblkio = NULL;
        oldblkio = NULL;
        first = true;
        ncpu = 0;
        kbuf = nullptr;
        kbufcrc = 0;
        memset(&oldcpu, 0, sizeof(oldcpu));
        memset(&newcpu, 0, sizeof(newcpu));
        memset(&cpu, 0, sizeof(cpu));
        totalcpu = 0;
        numcpu = 0;
        memset(&oldnet, 0, sizeof(oldnet));
        memset(&newnet, 0, sizeof(newnet));
        ndisks = 0;
        kbadcnt = 0;
        if (printklog)
        {
            kbufmax = 0x1000;
            kbuf = (char *)malloc(kbufmax*2);
            if (!kbuf)
                kbufmax = 0;
        }
        else
            kbufmax = 0;

        if (!getInterfaceName(ifname))
            ifname.set("eth0");

#ifdef __linux__
        // MCK - wish libblkid could do this ...
        // Another way might also be to look for:
        //   /sys/block/sd*
        //   /sys/block/nvme*
        // and match those with entries in /proc/diskstats
        StringBuffer cmd("lsblk -o TYPE,MAJ:MIN --pairs");
        Owned<IPipeProcess> pipe = createPipeProcess();
        if (pipe->run("list disks", cmd, nullptr, false, true, true, 8192))
        {
            StringBuffer output;
            Owned<ISimpleReadStream> pipeReader = pipe->getOutputStream();
            readSimpleStream(output, *pipeReader);
            unsigned exitcode = pipe->wait();
            if ( (exitcode == 0) && (output.length() > 0) )
            {
                StringArray lines;
                lines.appendList(output, "\n");
                ForEachItemIn(idx, lines)
                {
                    // line: TYPE="disk" MAJ:MIN="259:0"
                    unsigned majnum, minnum;
                    if (2 == sscanf(lines.item(idx), "TYPE=\"disk\" MAJ:MIN=\"%u:%u\"", &majnum, &minnum))
                    {
                        unsigned mm = (majnum<<16)+minnum;
                        diskMajorMinor.appendUniq(mm);
                    }
                }
            }
            else
            {
                StringBuffer outputErr;
                Owned<ISimpleReadStream> pipeReaderErr = pipe->getErrorStream();
                readSimpleStream(outputErr, *pipeReaderErr);
                if (outputErr.length() > 0)
                    WARNLOG("WARNING: Pipe: output: %s", outputErr.str());
            }
        }
#endif // __linux__
    }

    ~CExtendedStats()
    {
        free(partition);
        free(newblkio);
        free(oldblkio);
        if (kbuf != nullptr)
            free(kbuf);
    }

    bool getLine(StringBuffer &out)
    {
        blkio_info *t = oldblkio;
        oldblkio = newblkio;
        newblkio = t;
        oldnet = newnet;
        bool gotdisk = getDiskInfo()&&nparts;
        bool gotnet = getNetInfo();
        if (first)
        {
            first = false;
            return false;
        }
        double deltams = ((double)totalcpu*1000) / ncpu / HZ;
        if (deltams<10)
            return false;
        if (gotdisk)
        {
            if (out.length()&&(out.charAt(out.length()-1)!=' '))
                out.append(' ');
            out.append("DSK: ");
            for (unsigned p = 0; p < nparts; p++)
            {

                unsigned rd_ios = newblkio[p].rd_ios - oldblkio[p].rd_ios;
                __uint64 rd_sectors = newblkio[p].rd_sectors - oldblkio[p].rd_sectors;
                unsigned wr_ios = newblkio[p].wr_ios - oldblkio[p].wr_ios;
                __uint64 wr_sectors = newblkio[p].wr_sectors - oldblkio[p].wr_sectors;
                unsigned ticks = newblkio[p].ticks - oldblkio[p].ticks;
                unsigned busy = (unsigned)(100*ticks/deltams);
                if (busy>100)
                    busy = 100;
                out.appendf("[%s] r/s=%0.1f kr/s=%0.1f w/s=%0.1f kw/s=%0.1f bsy=%d",
                           partition[p].name,
                           perSec(rd_ios,deltams),
                           perSec(rd_sectors,deltams)/2.0,
                           perSec(wr_ios,deltams),
                           perSec(wr_sectors,deltams)/2.0,
                           busy);
                out.append(' ');
            }
        }
        if (gotnet)
        {
            if (out.length()&&(out.charAt(out.length()-1)!=' '))
                out.append(' ');
            out.appendf("NIC: [%s] ", ifname.str());
            __uint64 rxbytes = newnet.rxbytes-oldnet.rxbytes;
            __uint64 rxpackets = newnet.rxpackets-oldnet.rxpackets;
            __uint64 txbytes = newnet.txbytes-oldnet.txbytes;
            __uint64 txpackets = newnet.txpackets-oldnet.txpackets;
            __uint64 rxerrors = newnet.rxerrors-oldnet.rxerrors;
            __uint64 rxdrops = newnet.rxdrops-oldnet.rxdrops;
            __uint64 txerrors = newnet.txerrors-oldnet.txerrors;
            __uint64 txdrops = newnet.txdrops-oldnet.txdrops;
            out.appendf("rxp/s=%0.1f rxk/s=%0.1f txp/s=%0.1f txk/s=%0.1f rxerrs=%" I64F "d rxdrps=%" I64F "d txerrs=%" I64F "d txdrps=%" I64F "d",
                       perSec(rxpackets,deltams),
                       perSec(rxbytes/1024.0,deltams),
                       perSec(txpackets,deltams),
                       perSec(txbytes/1024.0,deltams),
                       rxerrors, rxdrops, txerrors, txdrops);
            out.append(' ');
        }
        if (totalcpu)
        {
            if (out.length()&&(out.charAt(out.length()-1)!=' '))
                out.append(' ');
            out.appendf("CPU: usr=%d sys=%d iow=%d idle=%d", cpu.getUserPercent(), cpu.getSystemPercent(), cpu.getIoWaitPercent(), cpu.getIdlePercent());
        }
        return true;
    }

#define KERN_EMERG   "<0>"   // system is unusable
#define KERN_ALERT   "<1>"   // action must be taken immediately
#define KERN_CRIT    "<2>"   // critical conditions
#define KERN_ERR     "<3>"   // error conditions
#define KERN_WARNING "<4>"   // warning conditions
#define KERN_NOTICE  "<5>"   // normal but significant condition
#define KERN_INFO    "<6>"   // informational
#define KERN_DEBUG   "<7>"   // debug-level messages
#define KMSGTEST(S) if (memcmp(p,S,3)==0) { ln.append(#S); level = p[1]-'0'; }

    void printKLog(IPerfMonHook *hook)
    {
        const char *p = nullptr;
        size32_t sz = getKLog(p);
#if 0
        DBGLOG("getKLog() returns: %u <%s>", sz, p);
#endif
        StringBuffer ln;
        const char *e = p+sz;
        while (p && (p!=e)) {
            if (*p=='<') {
                ln.clear();
                int level = -1;
                KMSGTEST(KERN_EMERG)
                else KMSGTEST(KERN_ALERT)
                else KMSGTEST(KERN_CRIT)
                else KMSGTEST(KERN_ERR)
                else KMSGTEST(KERN_WARNING)
                else KMSGTEST(KERN_NOTICE)
                else KMSGTEST(KERN_INFO)
                else KMSGTEST(KERN_DEBUG)
                else {
                    ln.append("KERN_UNKNOWN");
                    p -= 3;
                }
                p += 3;
                ln.append(": ");
                while ((p && p!=e)&&(*p!='\n'))
                    ln.append(*(p++));
                if (hook)
                    hook->log(level, ln.str());
                else
                    PROGLOG("%s",ln.str());
            }
            while ((p && p!=e)&&(*p!='\n'))
                p++;
            if (p && p!=e)
                p++;
        }
    }

};


#endif

struct PortStats
{
    unsigned port;
    unsigned drops;
    unsigned rx_queue;
};
typedef MapBetween<unsigned, unsigned, PortStats, PortStats> MapPortToPortStats;

class CUdpStatsReporter
{
public:
    CUdpStatsReporter()
    {
        dropsCol = -1;
        portCol = -1;
        uidCol = -1;
        queueCol = -1;
    }

    bool reportUdpInfo(unsigned traceLevel)
    {
#ifdef _WIN32
        return false;
#else
        if (uidCol==-1 && columnNames.length())
            return false;
        FILE *netfp = fopen("/proc/net/udp", "r");
        if (!netfp)
            return false;
        char ln[512];
        // Read header
        if (!fgets(ln, sizeof(ln), netfp)) {
            fclose(netfp);
            return false;
        }
        if (!columnNames.length())
        {
            columnNames.appendList(ln, " ");
            ForEachItemInRev(idx, columnNames)
            {
                if (streq(columnNames.item(idx), "rem_address"))
                    columnNames.add("rem_port", idx+1);
                else if (streq(columnNames.item(idx), "local_address"))
                    columnNames.add("local_port", idx+1);
            }
            ForEachItemIn(idx2, columnNames)
            {
                if (streq(columnNames.item(idx2), "drops"))
                    dropsCol = idx2;
                else if (streq(columnNames.item(idx2), "local_port"))
                    portCol = idx2;
                else if (streq(columnNames.item(idx2), "rx_queue"))
                    queueCol = idx2;
                else if (streq(columnNames.item(idx2), "uid"))
                    uidCol = idx2;
            }
            if (portCol == -1 || queueCol == -1 || uidCol == -1)
            {
                uidCol = -1;
                fclose(netfp);
                return false;
            }
        }
        int myUid = geteuid();
        while (fgets(ln, sizeof(ln), netfp))
        {
            StringArray cols;
            cols.appendList(ln, " :");
            if (cols.length() >= columnNames.length() && atoi(cols.item(uidCol))==myUid)
            {
                unsigned queue = strtoul(cols.item(queueCol), NULL, 16);
                unsigned drops = 0;
                if (dropsCol >= 0)
                    drops = strtoul(cols.item(dropsCol), NULL, 10);
                if (queue || drops)
                {
                    unsigned port = strtoul(cols.item(portCol), NULL, 16);
                    if (traceLevel > 0)
                        DBGLOG("From /proc/net/udp: port %d rx_queue=%u drops=%u", port, queue, drops);
                    PortStats *ret = map.getValue(port);
                    if (!ret)
                    {
                        PortStats e = {port, 0, 0};
                        map.setValue(port, e);
                        ret = map.getValue(port);
                        assertex(ret);
                    }
                    if (queue > ret->rx_queue)
                    {
                        DBGLOG("UDP queue: new max rx_queue: port %d rx_queue=%u drops=%u", port, queue, drops);
                        ret->rx_queue = queue;
                    }
                    if (drops > ret->drops)
                    {
                        LOG(MCoperatorError, unknownJob, "DROPPED UDP PACKETS: port %d rx_queue=%u (peak %u) drops=%u (total %i)", port, queue, ret->rx_queue, drops-ret->drops, drops);
                        ret->drops = drops;
                    }
                }
            }
        }
        fclose(netfp);
        return true;
#endif
    }
private:
    MapPortToPortStats map;
    StringArray columnNames;
    int dropsCol;
    int portCol;
    int uidCol;
    int queueCol;
};

class CSnmpStatsReporter
{
public:
    CSnmpStatsReporter()
    {
        inErrorsCol = -1;
        prevErrors = 0;
        firstCall = true;
    }
    bool reportSnmpInfo()
    {
#ifdef _WIN32
        return false;
#else
        if (inErrorsCol==-1 && columnNames.length())
            return false;
        FILE *netfp = fopen("/proc/net/snmp", "r");
        if (!netfp)
            return false;
        char ln[512];
        bool ok = false;
        while (fgets(ln, sizeof(ln), netfp))
        {
            if (strncmp(ln, "Udp:", 4)==0)
            {
                if (!columnNames.length())
                {
                    columnNames.appendList(ln, " ");
                    ForEachItemIn(idx, columnNames)
                    {
                        if (streq(columnNames.item(idx), "InErrors"))
                            inErrorsCol = idx;
                    }
                    if (inErrorsCol == -1)
                        break;
                }
                if (fgets(ln, sizeof(ln), netfp))
                {
                    StringArray cols;
                    cols.appendList(ln, " ");
                    if (cols.length() >= columnNames.length())
                    {
                        ok = true;
                        unsigned errors = strtoul(cols.item(inErrorsCol), NULL, 10);
                        if (firstCall)
                        {
                            firstCall = false;
                            if (errors)
                                LOG(MCoperatorWarning, unknownJob, "UDP Initial InError total: %u", errors);
                        }
                        else if (errors > prevErrors)
                            LOG(MCoperatorError, unknownJob, "UDP InErrors: %u (total %u)", errors-prevErrors, errors);
                        prevErrors = errors;
                    }
                }
                break;
            }
        }
        fclose(netfp);
        return ok;
#endif
    }
private:
    StringArray columnNames;
    int inErrorsCol;
    unsigned prevErrors;
    bool firstCall;
};

static class CMemoryUsageReporter: public Thread
{
    bool term;
    unsigned  interval;
    Semaphore sem;
    PerfMonMode traceMode;
    Linked<IPerfMonHook> hook;
    unsigned latestCPU;
#ifdef _WIN32
    LONG                           status;
    CpuInfo                        prevTime;
    CpuInfo                        deltaTime;
#else
    CProcessMonitor                procmon;
    CExtendedStats                 extstats;
#endif
    StringBuffer                   primaryfs;
    StringBuffer                   secondaryfs;
    CriticalSection                sect; // for getSystemTraceInfo

    CSnmpStatsReporter             snmpStats;
    CUdpStatsReporter              udpStats;

public:
    CMemoryUsageReporter(unsigned _interval, PerfMonMode _traceMode, IPerfMonHook * _hook, bool printklog)
        : Thread("CMemoryUsageReporter"), traceMode(_traceMode)
#ifndef _WIN32
        , extstats(printklog)
#endif

    {
        interval = _interval;
        hook.set(_hook);
        term = false;
        latestCPU = 0;
        // UDP stats reported unless explicitly disabled
        if (queryEnvironmentConf().getPropBool("udp_stats", true))
            traceMode |= PerfMonUDP;
#ifdef _WIN32
        primaryfs.append("C:");
#else
        primaryfs.append("/");
#endif
    }

    void setPrimaryFileSystem(char const * _primaryfs)
    {
        CriticalBlock block(sect);
        primaryfs.clear();
        if(_primaryfs)
            primaryfs.append(_primaryfs);
    }

    void setSecondaryFileSystem(char const * _secondaryfs)
    {
        CriticalBlock block(sect);
        secondaryfs.clear();
        if(_secondaryfs)
            secondaryfs.append(_secondaryfs);
    }

    void getSystemTraceInfo(StringBuffer &str, PerfMonMode mode)
    {
        CriticalBlock block(sect);
#ifdef _WIN32
        CpuInfo current;
        current.getSystemTimes();
        if (prevTime.getTotal())
        {
            deltaTime = current - prevTime;
            latestCPU = 100 - deltaTime.getIdlePercent();
        }
        prevTime = current;

        MEMORYSTATUSEX memstatus;
        memstatus.dwLength = sizeof(memstatus);
        GlobalMemoryStatusEx(&memstatus);
        DWORDLONG vmTotal = memstatus.ullTotalVirtual;
        DWORDLONG vmAvail = memstatus.ullAvailVirtual;
        DWORDLONG vmInUse = vmTotal - vmAvail;
        DWORDLONG physTotal = memstatus.ullAvailPhys;
        DWORDLONG physAvail = memstatus.ullTotalPhys;
        DWORDLONG physInUse = physTotal - physAvail;

        ULARGE_INTEGER diskAvailStruct;
        ULARGE_INTEGER diskTotalStruct;
        unsigned __int64 firstDriveTotal = 0;
        unsigned __int64 firstDriveInUse = 0;
        unsigned __int64 secondDriveTotal = 0;
        unsigned __int64 secondDriveInUse = 0;
        if(primaryfs.length())
        {
            diskAvailStruct.QuadPart = 0;
            diskTotalStruct.QuadPart = 0;
            GetDiskFreeSpaceEx(primaryfs.str(), &diskAvailStruct, &diskTotalStruct, 0);
            firstDriveTotal = diskTotalStruct.QuadPart;
            firstDriveInUse = diskTotalStruct.QuadPart - diskAvailStruct.QuadPart;
        }
        if(secondaryfs.length())
        {
            diskAvailStruct.QuadPart = 0;
            diskTotalStruct.QuadPart = 0;
            GetDiskFreeSpaceEx(secondaryfs.str(), &diskAvailStruct, &diskTotalStruct, 0);
            secondDriveTotal = diskTotalStruct.QuadPart;
            secondDriveInUse = diskTotalStruct.QuadPart - diskAvailStruct.QuadPart;
        }

        if(hook)
            hook->processPerfStats(latestCPU, (unsigned)(vmInUse/1024), (unsigned)(vmTotal/1024), firstDriveInUse, firstDriveTotal, secondDriveInUse, secondDriveTotal, getThreadCount());

        if(!mode)
            return;
        if(mode & PerfMonProcMem)
        {
            str.appendf("PU=%3d%%",latestCPU);
            str.appendf(" MU=%3u%%",(unsigned)((__int64)vmInUse*100/(__int64)vmTotal));
            str.appendf(" PY=%3u%%",(unsigned)((__int64)physInUse*100/(__int64)physTotal));
            if (hook)
                hook->extraLogging(str);
#ifdef _USE_MALLOC_HOOK
            if (totalMem)
                str.appendf(" TM=%" I64F "d",totalMem);
#endif
        }
        if(mode & PerfMonPackets)
        {
            unsigned tx, rx;
            if(getPacketStats(tx, rx))
                str.appendf(" TX=%3u%% RX=%3u%%", tx, rx);
            else
                str.appendf("                ");
        }
        if(mode & PerfMonDiskUsage)
        {
            if(firstDriveTotal) str.appendf(" D1=%3u%%", (unsigned)(firstDriveInUse*100/firstDriveTotal));
            if(secondDriveTotal) str.appendf(" D2=%3u%%", (unsigned)(secondDriveInUse*100/secondDriveTotal));
        }
        if(mode & PerfMonExtended)
        {
            __IO_COUNTERS ioc;
            KERNEL_USER_TIMES kut;
            POOLED_USAGE_AND_LIMITS put;
            VM_COUNTERS vmc;
            DWORD dwSize;
            DWORD dwHandles;

            dwSize = 0;
            NtKernelFunctions.NtQueryInformationProcess(GetCurrentProcess(), ProcessVmCounters, &vmc, sizeof(vmc), &dwSize);
            dwHandles = 0;
            dwSize = 0;
            NtKernelFunctions.NtQueryInformationProcess(GetCurrentProcess(), ProcessHandleCount, &dwHandles, sizeof(dwHandles), &dwSize);
            dwSize = 0;
            NtKernelFunctions.NtQueryInformationProcess(GetCurrentProcess(), ProcessIoCounters, &ioc, sizeof(ioc), &dwSize);
            dwSize = 0;
            NtKernelFunctions.NtQueryInformationProcess(GetCurrentProcess(), ProcessTimes, &kut, sizeof(kut), &dwSize);
            dwSize = 0;
            NtKernelFunctions.NtQueryInformationProcess(GetCurrentProcess(), ProcessPooledUsageAndLimits, &put, sizeof(put), &dwSize);

            str.appendf(" WS=%10u ",vmc.WorkingSetSize);
            str.appendf("PP=%10u ",put.PagedPoolUsage);
            str.appendf("NP=%10u ",put.NonPagedPoolUsage);

            str.appendf("HC=%5u ",dwHandles);
            str.appendf("TC=%5u ",getThreadCount());

            str.appendf("IR=%10u ",(unsigned)(ioc.ReadTransferCount/1024));
            str.appendf("IW=%10u ",(unsigned)(ioc.WriteTransferCount/1024));
            str.appendf("IO=%10u ",(unsigned)(ioc.OtherTransferCount/1024));

            str.appendf("KT=%16" I64F "u ",kut.KernelTime);
            str.appendf("UT=%16" I64F "u ",kut.UserTime);
        }

#else
        bool outofhandles = false;
        latestCPU = extstats.getCPU();
        if (latestCPU==(unsigned)-1) {
            outofhandles = true;
            latestCPU = 0;
        }


        unsigned __int64 primaryfsTotal = 0;
        unsigned __int64 primaryfsInUse = 0;
        unsigned __int64 secondaryfsTotal = 0;
        unsigned __int64 secondaryfsInUse = 0;
        if(primaryfs.length())
            getDiskUsage(primaryfs.str(), primaryfsTotal, primaryfsInUse);
        if(secondaryfs.length())
            getDiskUsage(secondaryfs.str(), secondaryfsTotal, secondaryfsInUse);


        if(!mode) return;
        unsigned memused=0;
        unsigned memtot=0;
        str.appendf("LPT=%u ", queryNumLocalTrees());
        str.appendf("APT=%u ", queryNumAtomTrees());
        if(mode & PerfMonProcMem)
        {
            if (!outofhandles)
                str.appendf("PU=%3d%% ", latestCPU);
            else
                str.appendf("PU=OOH ");
            getMemStats(str,memused,memtot);
            if (hook)
                hook->extraLogging(str);
            procmon.printBusy(latestCPU,str);
        }
        if (hook) {
            if (!memtot) {
                unsigned mu;
                unsigned ma;
                unsigned mt;
                unsigned st;
                unsigned su;
                getMemUsage(mu,ma,mt,st,su);
                memused = mu+su;
                memtot = mt+st;
            }
            hook->processPerfStats(latestCPU, memused, memtot, primaryfsInUse, primaryfsTotal, secondaryfsInUse, secondaryfsTotal, getThreadCount());
        }

        if(mode & PerfMonPackets)
        {
            unsigned tx, rx;
            if(getPacketStats(tx, rx))
                str.appendf(" TX=%3u%% RX=%3u%%", tx, rx);
            else
                str.appendf("                ");
        }
        if(mode & PerfMonDiskUsage)
        {
            if(primaryfsTotal) str.appendf(" D1=%3u%%", (unsigned)(primaryfsInUse*100/primaryfsTotal));
            if(secondaryfsTotal) str.appendf(" D2=%3u%%", (unsigned)(secondaryfsInUse*100/secondaryfsTotal));
        }
        if(mode & PerfMonExtended)
        {
            extstats.getLine(str);
        }
#endif
    }

#define NAMEDCOUNTPERIOD 60*30
    int run()
    {
        StringBuffer str;
        getSystemTraceInfo(str, traceMode&~PerfMonExtended); // initializes the values so that first one we print is meaningful rather than always saying PU=0%
        if (traceMode&PerfMonUDP)
        {
            snmpStats.reportSnmpInfo();
            udpStats.reportUdpInfo(0);
        }
        CTimeMon tm(NAMEDCOUNTPERIOD*1000);
        while (!term) {
            if (sem.wait(interval))
                break;
            str.clear();
            getSystemTraceInfo(str, traceMode&~PerfMonExtended);
#ifdef NAMEDCOUNTS
            if (tm.timedout())
            {
                dumpNamedCounts(str.newline());
                tm.reset(NAMEDCOUNTPERIOD*1000);
            }
#endif
            if (traceMode&PerfMonUDP)
            {
                snmpStats.reportSnmpInfo();
                udpStats.reportUdpInfo(0);
            }
            if(traceMode&&str.length()) {
                LOG(MCdebugInfo, unknownJob, "SYS: %s", str.str());
#ifndef _WIN32
                if (traceMode&PerfMonExtended) {
                    if (extstats.getLine(str.clear()))
                        LOG(MCdebugInfo, unknownJob, "%s", str.str());
                    {
                        CriticalBlock block(sect);
                        extstats.printKLog(hook);
                    }
                }
#endif
            }

        }
        return 0;
    }

    void stop()
    {
        term = true;
        sem.signal();
        join();
    }

    unsigned queryLatestCPU() const
    {
        return latestCPU;
    }

    void setHook(IPerfMonHook *_hook)
    {
        CriticalBlock block(sect);
        hook.set(_hook);
    }

} *MemoryUsageReporter=NULL;


#ifdef _WIN32
static inline unsigned scaleFileTimeToMilli(unsigned __int64 nano100)
{
    return (unsigned)(nano100 / 10000);
}

void getProcessTime(UserSystemTime_t & result)
{
    LARGE_INTEGER startTime, exitTime, kernelTime, userTime;
    if (GetProcessTimes(GetCurrentProcess(), (FILETIME *)&startTime, (FILETIME *)&exitTime, (FILETIME *)&kernelTime, (FILETIME *)&userTime))
    {
        result.user = scaleFileTimeToMilli(userTime.QuadPart);
        result.system = scaleFileTimeToMilli(kernelTime.QuadPart);
    }
}
#else
void getProcessTime(UserSystemTime_t & result)
{
    UserStatusInfo info(GetCurrentProcessId());
    if (info.update())
        result = info.time;
}
#endif




void getSystemTraceInfo(StringBuffer &str, PerfMonMode mode)
{
    if (!MemoryUsageReporter) 
        MemoryUsageReporter = new CMemoryUsageReporter(1000, mode, 0, false);
    MemoryUsageReporter->getSystemTraceInfo(str, mode);
}

void startPerformanceMonitor(unsigned interval, PerfMonMode traceMode, IPerfMonHook * hook)
{
    stopPerformanceMonitor();
    if (!MemoryUsageReporter) {
        MemoryUsageReporter = new CMemoryUsageReporter(interval, traceMode, hook, (traceMode&PerfMonExtended)!=0);
        MemoryUsageReporter->start();
    }
}

void stopPerformanceMonitor()
{
    if (MemoryUsageReporter) {
        MemoryUsageReporter->stop();
        delete MemoryUsageReporter;
        MemoryUsageReporter = NULL;
    }
}

void setPerformanceMonitorHook(IPerfMonHook *hook)
{
    if (MemoryUsageReporter)
        MemoryUsageReporter->setHook(hook);
}

void setPerformanceMonitorPrimaryFileSystem(char const * fs)
{
    if(MemoryUsageReporter)
        MemoryUsageReporter->setPrimaryFileSystem(fs);
}

void setPerformanceMonitorSecondaryFileSystem(char const * fs)
{
    if(MemoryUsageReporter)
        MemoryUsageReporter->setSecondaryFileSystem(fs);
}

unsigned getLatestCPUUsage()
{
    if (MemoryUsageReporter)
        return MemoryUsageReporter->queryLatestCPU();
    else
        return 0;
}


void getHardwareInfo(HardwareInfo &hdwInfo, const char *primDiskPath, const char *secDiskPath)
{
    memset(&hdwInfo, 0, sizeof(HardwareInfo));
    getCpuInfo(hdwInfo.numCPUs, hdwInfo.CPUSpeed);

#ifdef _WIN32

    MEMORYSTATUS memstatus;
    GlobalMemoryStatus(&memstatus);
    hdwInfo.totalMemory = (unsigned)(memstatus.dwTotalPhys / (1024*1024)); // in MB

    ULARGE_INTEGER diskAvailStruct;
    ULARGE_INTEGER diskTotalStruct;
    if (primDiskPath)
    {
        diskTotalStruct.QuadPart = 0;
        GetDiskFreeSpaceEx(primDiskPath, &diskAvailStruct, &diskTotalStruct, 0);
        hdwInfo.primDiskSize = (unsigned)(diskTotalStruct.QuadPart / (1024*1024*1024));  // in GB
        hdwInfo.primFreeSize = (unsigned)(diskAvailStruct.QuadPart / (1024*1024*1024));  // in GB
    }
    if (secDiskPath)
    {
        diskTotalStruct.QuadPart = 0;
        GetDiskFreeSpaceEx(secDiskPath, &diskAvailStruct, &diskTotalStruct, 0);
        hdwInfo.secDiskSize = (unsigned)(diskTotalStruct.QuadPart / (1024*1024*1024));   // in GB
        hdwInfo.secFreeSize = (unsigned)(diskAvailStruct.QuadPart / (1024*1024*1024));   // in GB
    }

    // MORE: Find win32 call for NIC speed

#else  // linux
    unsigned memUsed, memActive, memSwap, memSwapUsed; 
    getMemUsage(memUsed, memActive, hdwInfo.totalMemory, memSwap, memSwapUsed);
    hdwInfo.totalMemory /= 1024; // in MB

    unsigned __int64 diskSize;
    unsigned __int64 diskUsed;
    if (primDiskPath)
    {
        getDiskUsage(primDiskPath, diskSize, diskUsed);
        hdwInfo.primDiskSize = diskSize / (1024*1024*1024);   // in GB
        hdwInfo.primFreeSize = (diskSize - diskUsed) / (1024*1024*1024);   // in GB
    }
    if (secDiskPath)
    {
        getDiskUsage(secDiskPath, diskSize, diskUsed);
        hdwInfo.secDiskSize = diskSize / (1024*1024*1024);    // in GB
        hdwInfo.secFreeSize = (diskSize - diskUsed) / (1024*1024*1024);   // in GB
    }

    // MORE: Find linux call for NIC speed -- mii-tool does not seem to work on our roxie clusters?

#endif

}

//===========================================================================


enum SegTypes { 
        segtype_free,         //
        segtype_heap,         // rw-p named [heap]
        segtype_data,         // rw-p unnamed
        segtype_guard,        // ---p unnamed/named
        segtype_stack,        // rwxp 
        segtype_qlibcode,     // r-xp  named */lib200
        segtype_qlibdata,     // rw-p  named */lib200
        segtype_libcode,      // r-xp  named *
        segtype_libdata,      // rw-p  named *
        segtype_pstack,       // rwxp named [stack]
        segtype_const,        // r--p
        segtype_null          // must be last
};

struct SegTypeRec
{
    offset_t total;
    unsigned n;
    offset_t largest;
};



const char *PROCMAPHEADER = 
    "FREE,NFREE,MAXFREE,HEAP,STACK,NSTACKS,DATA,NDATA,MAXDATA,LIBDATA,QUERYDATA,MAXQUERYDATA,LIBCODE,QUERYCODE,MAXQLIBCODE";





class CProcReader
{
    // Cant use JFile for /proc filesystem as seek doesn't work
public:
    char ln [512];
    FILE *file;
    const char *buf;
    size32_t bufsize;
    CProcReader(const char *filename,const void *_buf,size32_t _buflen)
    {
        buf = (const char *)_buf;
        bufsize = buf?_buflen:0;
        file = buf?NULL:fopen(filename,"r");
    }
    ~CProcReader()
    {
        if (file)
            fclose(file);
    }
    bool nextln()
    {
        if (buf) {
            if (bufsize&&*buf) {
                unsigned i = 0;
                while (bufsize&&(i<sizeof(ln)-1)&&*buf&&(*buf!=10)&&(*buf!=13)) {
                    ln[i++] = *(buf++);
                    bufsize--;
                }
                ln[i] = 0;
                while (bufsize&&*buf&&((*buf==10)||(*buf==13))) {
                    buf++;
                    bufsize--;
                }
                return true;
            }
        }
        else if (file&&fgets (ln, sizeof(ln), file)) {
            size_t i = strlen(ln);
            while (i&&((ln[i-1]==10)||(ln[i-1]==13)))
                i--;
            ln[i] = 0;
            return true;
        }
        if (file) {
            fclose(file);
            file = NULL;
        }
        return false;
    }
    void dump(bool useprintf)
    {
        while (nextln()) {
            if (useprintf)
                printf("%s\n",ln);
            else
                PROGLOG("%s",ln);
        }
    }
};


void printProcMap(const char *fn, bool printbody, bool printsummary, StringBuffer *lnout, MemoryBuffer *mb, bool useprintf)
{
    CProcReader reader(fn,mb?mb->toByteArray():NULL,mb?mb->length():0);
    unsigned i;
    SegTypeRec recs[segtype_null];
    memset(&recs,0,sizeof(recs));
    offset_t last=0;
    if (printbody) {
        if (useprintf)
            printf("START,END,SIZE,OFFSET,PERMS,PATH\n");
        else
            PROGLOG("START,END,SIZE,OFFSET,PERMS,PATH");
    }
    while (reader.nextln()) {
        const char *ln = reader.ln;
        unsigned n=0;
        if (*ln) {
            offset_t start = readHexNum(ln);
            if (last&&(last!=start)) {
                recs[segtype_free].n++;
                offset_t ssz = start-last;
                recs[segtype_free].total += ssz;
                if (ssz>recs[segtype_free].largest)
                    recs[segtype_free].largest = ssz;
            }

            if (*ln=='-') {
                ln++;
                offset_t end = readHexNum(ln);
                char perms[5];
                skipSp(ln);
                for (i=0;i<4;)
                    if (*ln)
                        perms[i++] = *(ln++);
                perms[i] = 0;
                skipSp(ln);
                offset_t offset =  readHexNum(ln);
                skipSp(ln);
                char dev[6];
                for (i=0;i<5;)
                    if (*ln)
                        dev[i++] = *(ln++);
                dev[i] = 0;
                skipSp(ln);
                unsigned inode __attribute__((unused)) = (unsigned) readDecNum(ln);
                skipSp(ln);
                const char *path = ln;
                if (printbody) {
                    if (useprintf)
                        printf("%08" I64F "x,%08" I64F "x,%" I64F "d,%08" I64F "x,%s,%s,%s\n",start,end,(offset_t)(end-start),offset,perms,dev,path);
                    else
                        PROGLOG("%08" I64F "x,%08" I64F "x,%" I64F "d,%08" I64F "x,%s,%s,%s",start,end,(offset_t)(end-start),offset,perms,dev,path);
                }
                SegTypes t = segtype_data;
                if (strcmp(perms,"---p")==0)
                    t = segtype_guard;
                else if (strcmp(perms,"rwxp")==0) {
                    if (memicmp(ln,"[stack]",7)==0)
                        t = segtype_pstack;
                    else
                        t = segtype_stack;
                }
                else if (strcmp(perms,"rw-p")==0) {
                    if (memicmp(ln,"[heap]",6)==0)
                        t = segtype_heap;
                    else if (strstr(ln,"/libW200"))
                        t = segtype_qlibdata;
                    else if (*ln)
                        t = segtype_libdata;
                    else
                        t = segtype_data;
                }
                else if (strcmp(perms,"r-xp")==0) {
                    if (strstr(ln,"/libW200"))
                        t = segtype_qlibcode;
                    else if (*ln)
                        t = segtype_libcode;
                }
                else if (strcmp(perms,"r--p")==0)
                        t = segtype_const;
                else {
                    OERRLOG("%s - unknown perms",perms);
                    continue;
                }
                recs[t].n++;
                offset_t ssz = end-start;
                recs[t].total += ssz;
                if (ssz>recs[t].largest)
                    recs[t].largest = ssz;
                n++;
                last = end;
#ifndef __64BIT__
                if ((end<0xffffffff)&&(end>=0xc0000000))    // rest is OS (32-bit only)
                    break;
#endif
            }
        }
    }
    if (printsummary||lnout) {
        StringBuffer tln;
        if (lnout==NULL)
            lnout = &tln;

        lnout->appendf("%" I64F "u,"    // total
               "%u,"             // n
               "%" I64F "u,"    // largest
               "%" I64F "u,"    // total
               "%" I64F "u,"    // total
               "%u,"          // n
               "%" I64F "u,"   // total
               "%u,"          // n
               "%" I64F "u,"    // largest
               "%" I64F "u,"    // total
               "%" I64F "u,"    // total
               "%" I64F "u,"    // largest
               "%" I64F "u,"    // total
               "%" I64F "u,"    // total
               "%" I64F "u"    // largest
               ,
            recs[segtype_free].total,
            recs[segtype_free].n,
            recs[segtype_free].largest,
            recs[segtype_heap].total,
            recs[segtype_stack].total,
            recs[segtype_stack].n,
            recs[segtype_data].total,
            recs[segtype_data].n,
            recs[segtype_data].largest,
            recs[segtype_libdata].total,
            recs[segtype_qlibdata].total,
            recs[segtype_qlibdata].largest,
            recs[segtype_libcode].total,
            recs[segtype_qlibcode].total,
            recs[segtype_qlibcode].largest
            );
        if (printsummary) {
            if (useprintf)
                printf("%s\n%s\n",PROCMAPHEADER,tln.str());
            else {
                PROGLOG("%s",PROCMAPHEADER);
                PROGLOG("%s",tln.str());
            }
        }
    }
}

#ifdef _WIN32

// stubs
void PrintMemoryReport(bool full)
{
    StringBuffer s;
    getSystemTraceInfo(s,PerfMonProcMem);
    PROGLOG("%s",s.str());
}

#else

void PrintMemoryReport(bool full)
{
    // may be very close to oom so protect against re-entry
    static int recurse=0;
    if (recurse++==0) {
        try {
            printProcMap("/proc/self/maps",full,true,NULL,NULL,false);
        }
        catch (IException *e) {
            e->Release();
        }
        catch (...) {
        }
        try {
            PROGLOG("/proc/meminfo:");
            CProcReader reader("/proc/meminfo",NULL,0);
            reader.dump(false);
        }
        catch (IException *e) {
            e->Release();
        }
        catch (...) {
        }
        try {
            PROGLOG("/proc/self/status:");
            CProcReader reader("/proc/self/status",NULL,0);
            reader.dump(false);
        }
        catch (IException *e) {
            e->Release();
        }
        catch (...) {
        }
        try {
            StringBuffer s;
            getSystemTraceInfo(s,PerfMonProcMem);
            PROGLOG("%s",s.str());
            PROGLOG("===============================================================");
        }
        catch (IException *e) {
            e->Release();
        }
        catch (...) {
        }
    }
    recurse--;
}


#endif


bool areTransparentHugePagesEnabled(HugePageMode mode)
{
    return (mode != HugePageMode::Never) && (mode != HugePageMode::Unknown);
}

HugePageMode queryTransparentHugePagesMode()
{
#ifdef __linux__
    StringBuffer contents;
    try
    {
        contents.loadFile("/sys/kernel/mm/transparent_hugepage/enabled");
        if (strstr(contents.str(), "[never]"))
            return HugePageMode::Never;
        if (strstr(contents.str(), "[madvise]"))
            return HugePageMode::Madvise;
        if (strstr(contents.str(), "[always]"))
            return HugePageMode::Always;
    }
    catch (IException * e)
    {
        e->Release();
    }
    return HugePageMode::Unknown;
#endif
    return HugePageMode::Never;
}

memsize_t getHugePageSize()
{
#ifdef __linux__
    StringBuffer contents;
    try
    {
        //Search for an entry   Hugepagesize:      xxxx kB
        const char * const tag = "Hugepagesize:";
        contents.loadFile("/proc/meminfo");
        const char * hugepage = strstr(contents.str(), tag);
        if (hugepage)
        {
            const char * next = hugepage + strlen(tag);
            char * end;
            memsize_t size = strtoul(next, &end, 10);
            if (strncmp(end, " kB", 3) == 0)
                return size * 0x400;
        }
    }
    catch (IException * e)
    {
        e->Release();
    }
#endif
    return 0x200000; // Default for an x86 system
}

//===========================================================================

#ifdef LEAK_CHECK
 #ifdef _WIN32

LeakChecker::LeakChecker(const char * _title) : title(_title)
{
  _CrtMemCheckpoint(&oldMemState);
}

LeakChecker::~LeakChecker()
{
  _CrtMemState newMemState, diffMemState;
  _CrtMemCheckpoint(&newMemState);
  if(_CrtMemDifference(&diffMemState, &oldMemState, &newMemState))
  {
    _RPT1(_CRT_WARN, "----- Memory leaks in '%s' -----\n", title);
    _CrtMemDumpStatistics(&diffMemState);
    _CrtMemDumpAllObjectsSince(&oldMemState);
    _RPT0(_CRT_WARN, "----- End of leaks -----\n");
  }
}

static char _logFile[255];  // used to hold last file name of log file for memory leak logging
static FILE *_logHandle = NULL;

_CRT_REPORT_HOOK oldReport;

static int MemoryLeakReportHook(int nRptType,char *szMsg,int  *retVal)
{
    if (szMsg && *szMsg)
    {
        if (_logHandle)
            fprintf(_logHandle, szMsg);
        if (*_logFile)
        {
#if 1
            // this works  better in VS 2008 libraries (which fault in fopen)
            int handle = _open(_logFile, O_RDWR | O_CREAT, _S_IREAD | _S_IWRITE);
            _lseek(handle,0,SEEK_END);
            _write(handle,szMsg,(unsigned)strlen(szMsg));
            _close(handle);
#else
            FILE *handle = fopen(_logFile, "a");
            fprintf(handle, szMsg);
            fclose(handle);
#endif
        }
    }
    if (oldReport)
        return oldReport(nRptType,szMsg,retVal);
    else
      return false;
}

MODULE_INIT(INIT_PRIORITY_JDEBUG1)
{
    oldReport = _CrtSetReportHook(MemoryLeakReportHook);
    return 1;
}

void logLeaks (const char *logFile)
{
    if (logFile)
        strncpy(_logFile, logFile, sizeof(_logFile));
    else
    _logFile[0] = 0;
}

void logLeaks (FILE *logHandle)
{
    _logHandle = logHandle;
}

 #else
 #endif
#endif

#if !defined(USING_MPATROL) && defined(WIN32) && defined(_DEBUG)

void jlib_decl enableMemLeakChecking(bool enable)
{
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    if (enable)
        tmpFlag |= _CRTDBG_LEAK_CHECK_DF;
    else
        tmpFlag &= ~_CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );

}

#endif




#if defined(_WIN32) && defined(_DEBUG)
//#include <dbgint.h>

const unsigned maxUnique = 10000;

typedef struct _CrtMemBlockHeader
{
// Pointer to the block allocated just before this one:
    struct _CrtMemBlockHeader *pBlockHeaderNext;
// Pointer to the block allocated just after this one:
    struct _CrtMemBlockHeader *pBlockHeaderPrev;
    char *szFileName;    // File name
    int nLine;           // Line number
    size_t nDataSize;    // Size of user block
    int nBlockUse;       // Type of block
    long lRequest;       // Allocation number
} _CrtMemBlockHeader;


int compareFile(_CrtMemBlockHeader * left, _CrtMemBlockHeader * right)
{
    int compare;
    if (left->szFileName && right->szFileName)
        compare = strcmp(left->szFileName, right->szFileName);
    else if (left->szFileName)
        compare = -1;
    else if (right->szFileName)
        compare = +1;
    else
        compare = 0;
    return compare;
}

int compareLocation(_CrtMemBlockHeader * left, _CrtMemBlockHeader * right)
{
    int compare = compareFile(left, right);
    if (compare != 0)
        return compare;
    return left->nLine - right->nLine;
}

int compareBlocks(_CrtMemBlockHeader * left, _CrtMemBlockHeader * right)
{
    int compare = compareLocation(left, right);
    if (compare != 0)
        return compare;
    return (int)(right->nDataSize - left->nDataSize);
}


void addLocation(unsigned & numUnique, _CrtMemBlockHeader * * locations, unsigned * counts, _CrtMemBlockHeader * search)
{
    int left = 0;
    int right = numUnique;
    while (left < right)
    {
        int mid = (left + right) >> 1;
        int cmp = compareBlocks(search, locations[mid]);
        if (cmp < 0)
            right = mid;
        else if (cmp > 0)
            left = mid+1;
        else
        {
            //Save the lowest allocation number (so quicker to set a subsequent breakpoint)
            if (search->lRequest < locations[mid]->lRequest)
                locations[mid] = search;
            counts[mid]++;
            return;
        }
    }
    if (numUnique != maxUnique)
    {
        assertex(left == right);
        memmove(locations + left+1, locations + left, (numUnique-left)*sizeof(*locations));
        memmove(counts + left+1, counts + left, (numUnique-left)*sizeof(*counts));
        locations[left] = search;
        counts[left] = 1;
        numUnique++;
    }
    else
        counts[maxUnique]++;
}


unsigned dumpMemory(unsigned lenTarget, char * target, unsigned lenSrc, const void * ptr)
{
    if (lenSrc > lenTarget)
        lenSrc = lenTarget;

    const char * src = (const char *)ptr;
    for (unsigned i=0; i < lenSrc; i++)
    {
        byte next = src[i];
        target[i] = (next >= 0x20 && next <= 0x7e) ? next : '.';
    }
    return lenSrc;
}


void printAllocationSummary()
{
    _CrtMemState state;
    _CrtMemCheckpoint(&state);

    unsigned numUnique = 0;
    _CrtMemBlockHeader * locations[maxUnique+1];
    unsigned counts[maxUnique+1];
    _clear(counts);

    unsigned __int64 totalFree = 0;
    unsigned __int64 totalAllocated = 0;
    //Walk the heap, keeping a tally of (filename, line, size)->count
    _CrtMemBlockHeader * cur;
    for (cur = state.pBlockHeader; cur; cur=cur->pBlockHeaderNext)
    {
        switch (cur->nBlockUse)
        {
        case _NORMAL_BLOCK:
            {
                addLocation(numUnique, locations, counts, cur);
                totalAllocated += cur->nDataSize;
                break;
            }
        case _FREE_BLOCK:
            totalFree += cur->nDataSize;
            break;
        }
    }

    PROGLOG("%d Unique allocations by <filename>(line)@size", numUnique);
    for (unsigned i2 = 0; i2 < numUnique; i2++)
    {
        _CrtMemBlockHeader * display = locations[i2];
        //char tempBuffer[16];
        //unsigned len = dumpMemory(sizeof(tempBuffer), tempBuffer, display->nDataSize, 

        PROGLOG("%s(%d) %d:%d {%ld} = %d", display->szFileName ? display->szFileName : "<unknown>", display->nLine, display->nDataSize, counts[i2], display->lRequest, display->nDataSize * counts[i2]);
    }
    PROGLOG("Ungrouped: %d  Total %" I64F "d", counts[maxUnique], totalAllocated);

    PROGLOG("Summary by location");
    for (unsigned iSummary2 = 0; iSummary2 < numUnique; )
    {
        _CrtMemBlockHeader * display = locations[iSummary2];
        unsigned count = counts[iSummary2];
        unsigned __int64 size = count * display->nDataSize;
        for (iSummary2++; iSummary2 < numUnique; iSummary2++)
        {
            _CrtMemBlockHeader * next = locations[iSummary2];
            if (compareLocation(display, next) != 0)
                break;
            count += counts[iSummary2];
            size += (counts[iSummary2] * next->nDataSize);
        }
        PROGLOG("%s(%d) %d = %d", display->szFileName ? display->szFileName : "<unknown>", display->nLine, count, size);
    }

    PROGLOG("Summary by source");
    for (unsigned iSummary2 = 0; iSummary2 < numUnique; )
    {
        _CrtMemBlockHeader * display = locations[iSummary2];
        unsigned count = counts[iSummary2];
        unsigned __int64 size = count * display->nDataSize;
        for (iSummary2++; iSummary2 < numUnique; iSummary2++)
        {
            _CrtMemBlockHeader * next = locations[iSummary2];
            if (compareFile(display, next) != 0)
                break;
            count += counts[iSummary2];
            size += (counts[iSummary2] * next->nDataSize);
        }
        PROGLOG("%s %d = %d", display->szFileName ? display->szFileName : "<unknown>", count, size);
    }

}
#else
void printAllocationSummary()
{
}
#endif



#ifdef _USE_MALLOC_HOOK
// Note memory hooks should not be enabled for release (as not re-entrant in linux)


static CriticalSection hookSect;

#ifdef __linux__


static void *(*old_malloc_hook)(size_t, const void *);
static void (*old_free_hook)(void *, const void *);
static void *(*old_realloc_hook)(void *, size_t, const void *);
static void * jlib_malloc_hook (size_t size, const void *caller) ;
static void jlib_free_hook (void *ptr, const void *caller);
static void *jlib_realloc_hook (void *ptr, size_t size, const void *caller);
static int jlib_hooknest = 0; // this *shouldn't* really be needed

inline void restore_malloc_hooks()
{
    if (--jlib_hooknest==0) {
        __malloc_hook = old_malloc_hook;
        __realloc_hook = old_realloc_hook;
        __free_hook = old_free_hook;
    }
}

inline void set_malloc_hooks()
{
    assertex(jlib_hooknest==0);
    old_malloc_hook = __malloc_hook;
    old_free_hook = __free_hook;
    old_realloc_hook = __realloc_hook;
    __malloc_hook = jlib_malloc_hook;
    __free_hook = jlib_free_hook;
    __realloc_hook = jlib_realloc_hook;
    jlib_hooknest = 1;
}

inline void reset_malloc_hooks()
{
    if (jlib_hooknest++==0) {
        __malloc_hook = jlib_malloc_hook;
        __free_hook = jlib_free_hook;
        __realloc_hook = jlib_realloc_hook;
    }
}

inline void incCount(unsigned sz,bool inc)
{
    int i=0;
    size32_t s=sz;
    while (s) {
        s /= 2;
        i++;
    }
    if (inc)
        memArea[i] += sz;
    else
        memArea[i] -= sz;
}

void * jlib_malloc_hook (size_t size, const void *caller)
{
    CriticalBlock block(hookSect);
    void *res;
    restore_malloc_hooks();
    res = malloc (size);
    if (res) {
        size = malloc_usable_size(res);
        totalMem+=size;
        if (totalMem>hwmTotalMem) {
            if (hwmTotalMem/(100*0x100000)!=totalMem/(100*0x100000)) {
                PrintStackReport();
                PROGLOG("TOTALMEM(%" I64F "d): malloc %u",totalMem,(unsigned)size);
            }
            hwmTotalMem = totalMem;
        }
    }
    else
        size = 0;
    incCount(size,true);
    if (size>REPORT_LARGER_BLOCK_THAN) {
        PrintStackReport();
        PROGLOG("LARGEALLOC(%u): %p",(unsigned)size,res);
    }
    reset_malloc_hooks();
    return res;
}

void jlib_free_hook (void *ptr, const void *caller)
{
    if (!ptr)
        return;
    CriticalBlock block(hookSect);
    restore_malloc_hooks();
    size32_t sz = malloc_usable_size(ptr);
    free (ptr);
    totalMem -= sz;
    incCount(sz,false);
    if (sz>REPORT_LARGER_BLOCK_THAN) {
        PROGLOG("LARGEFREE(%u): %p",(unsigned)sz,ptr);
    }
    reset_malloc_hooks();
}

void *jlib_realloc_hook (void *ptr, size_t size, const void *caller)
{
    CriticalBlock block(hookSect);
    restore_malloc_hooks();
    size32_t oldsz = ptr?malloc_usable_size(ptr):0;
    void *res = realloc (ptr,size);
    if (res) {
        size = malloc_usable_size(res);
        totalMem += size;
    }
    else
        size = 0;
    totalMem -= oldsz;
    if (totalMem>hwmTotalMem) {
        if (hwmTotalMem/(100*0x100000)!=totalMem/(100*0x100000)) {
            PrintStackReport();
            PROGLOG("TOTALMEM(%" I64F "d): realloc %u %u",totalMem,(unsigned)oldsz,(unsigned)size);
        }
        hwmTotalMem = totalMem;
    }
    incCount(size,true);
    incCount(oldsz,false);
    if ((size>REPORT_LARGER_BLOCK_THAN)||(oldsz>REPORT_LARGER_BLOCK_THAN)) {
        if (size>oldsz) {
            PrintStackReport();
            PROGLOG("LARGEREALLOC_UP(%u,%u): %p %p",(unsigned)oldsz,(unsigned)size,ptr,res);
        }
        else  {
            PROGLOG("LARGEREALLOC_DN(%u,%u): %p %p",(unsigned)oldsz,(unsigned)size,ptr,res);
        }
    }
    reset_malloc_hooks();
    return res;
}

void jlib_decl jlib_init_hook()
{
    set_malloc_hooks();
}




__int64 jlib_decl setAllocHook(bool on,bool clear)
{
    CriticalBlock block(hookSect);
    __int64 ret = totalMem;
    if (clear) {
        totalMem = 0;
        hwmTotalMem = 0;
    }
    if (on) {
        if (jlib_hooknest==0)
            set_malloc_hooks();
    }
    else {
        while (jlib_hooknest) {
            restore_malloc_hooks();
            //printf("Total = %d bytes\n",totalMem);
        }
    }
    return ret;
}

unsigned jlib_decl setAllocHook(bool on)
{
    // bwd compatible - should use above version in preference
    CriticalBlock block(hookSect);
    if (on) {
        if (jlib_hooknest==0) {
            set_malloc_hooks();
            totalMem = 0;
            hwmTotalMem = 0;
        }
    }
    else {
        while (jlib_hooknest) {
            restore_malloc_hooks();
            //printf("Total = %d bytes\n",totalMem);
        }
    }
    return (unsigned)totalMem;
}



#else // windows

static _CRT_ALLOC_HOOK oldHook = NULL;

static int allocHook( int allocType, void *userData, size_t size, int nBlockUse,
        long requestNumber, const unsigned char *filename, int lineNumber)
{
    CriticalBlock block(hookSect);
    if ( nBlockUse == _CRT_BLOCK )   // Ignore internal C runtime library allocations
        return TRUE;
    static bool recurse = false;
    if (recurse)
        return TRUE;
    recurse = true;
    char *operation[] = { "", "allocating", "re-allocating", "freeing" };
    char *blockType[] = { "Free", "Normal", "CRT", "Ignore", "Client" };

    switch (allocType) {
    case _HOOK_REALLOC:
        if (userData==NULL)
            printf("no data on realloc\n");
        else
            totalMem-=_msize(userData);

        // fall through
    case _HOOK_ALLOC:
        totalMem+=size;
        break;
    case _HOOK_FREE:
        if (userData)
            totalMem-=_msize(userData);
        break;
    }

//  printf(  "Memory operation in %s, line %d: %s a %d-byte '%s' block (#%ld)\n",
//      filename, lineNumber, operation[allocType], size,
//      blockType[nBlockUse], requestNumber );

    recurse = false;
    return TRUE;         // Allow the memory operation to proceed
}

unsigned jlib_decl setAllocHook(bool on)
{
    CriticalBlock block(hookSect);
    if (on) {
        if (oldHook==NULL) {
            oldHook = _CrtSetAllocHook( allocHook );
            totalMem = 0;
        }
    }
    else {
        if (oldHook!=NULL)
            _CrtSetAllocHook( oldHook );
        oldHook = NULL;
        //printf("Total = %d bytes\n",totalMem);
    }
    return (unsigned)totalMem;  // return unsigned for bwd compat
}


#endif

__int64 getTotalMem()
{
    return totalMem;
}

#else // release

unsigned jlib_decl setAllocHook(bool on __attribute__((unused)))
{
    return 0;
}

__int64 jlib_decl setAllocHook(bool on __attribute__((unused)), bool clear __attribute__((unused)))
{
    return 0;
}

__int64 jlib_decl getTotalMem()
{
    return 0;
}

void jlib_decl jlib_init_hook()
{
}

#endif

class UserMetricMsgHandler : public CInterface, implements ILogMsgHandler, implements IUserMetric
{
    mutable unsigned __int64 counter;
    StringAttr metricName;
    StringAttr regex;
    Owned<ILogMsgFilter> regexFilter;
public:
    virtual void Link(void) const { CInterface::Link(); }
    virtual bool Release(void) const    
    {
        if (CInterface::Release())
            return true;
        if (!IsShared())
        {
            queryLogMsgManager()->removeMonitor(const_cast<UserMetricMsgHandler *>(this)); // removeMonitor should take a const param really
        }
        return false;
    }
    UserMetricMsgHandler(const char *_name, const char *_regex) : metricName(_name), regex(_regex)
    {
        counter = 0;
        regexFilter.setown(getRegexLogMsgFilter(regex, true));
        queryLogMsgManager()->addMonitor(this, regexFilter);
    }

    // interface ILogMsgHandler
    virtual void handleMessage(const LogMsg & msg __attribute__((unused))) const {  counter++; }
    virtual bool needsPrep() const { return false; }
    virtual void prep() {}
    virtual unsigned queryMessageFields() const { return MSGFIELD_detail; }
    virtual void setMessageFields(unsigned _fields __attribute__((unused)) = MSGFIELD_all) {}
    virtual void addToPTree(IPropertyTree * parent __attribute__((unused))) const {}
    virtual int flush() { return 0; }
    virtual char const *disable() { return 0; }
    virtual void enable() {}
    virtual bool getLogName(StringBuffer &name __attribute__((unused))) const { return false; }
    virtual offset_t getLogPosition(StringBuffer &logFileName __attribute__((unused))) const { return 0; };

    // interface IUserMetric
    virtual unsigned __int64 queryCount() const { return counter; }
    virtual const char *queryName() const { return metricName; }
    virtual const char *queryMatchString() const { return regex; }
    virtual void inc() { counter++; }
    virtual void reset() { counter = 0; }
};

jlib_decl IUserMetric *createUserMetric(const char *name, const char *matchString)
{
    return new UserMetricMsgHandler(name, matchString);
}
