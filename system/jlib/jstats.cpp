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


#include "jiface.hpp"
#include "jstats.h"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jlog.hpp"
#include "jregexp.hpp"
#include "jfile.hpp"

#ifdef _WIN32
#include <sys/timeb.h>
#endif

static CriticalSection statsNameCs;
static StringBuffer statisticsComponentName;
static StatisticCreatorType statisticsComponentType = SCTunknown;

StatisticCreatorType queryStatisticsComponentType()
{
    return statisticsComponentType;
}

const char * queryStatisticsComponentName()
{
    CriticalBlock c(statsNameCs);
    if (statisticsComponentName.length() == 0)
    {
        statisticsComponentName.append("unknown").append(GetCachedHostName());
        DBGLOG("getProcessUniqueName hasn't been configured correctly");
    }
    return statisticsComponentName.str();
}

void setStatisticsComponentName(StatisticCreatorType processType, const char * processName, bool appendIP)
{
    if (!processName)
        return;

    CriticalBlock c(statsNameCs);
    statisticsComponentType = processType;
    statisticsComponentName.clear().append(processName);
    if (appendIP)
        statisticsComponentName.append("@").append(GetCachedHostName());  // should I use _ instead?
}

//--------------------------------------------------------------------------------------------------------------------

// Textual forms of the different enumerations, first items are for none and all.
static const char * const measureNames[] = { "", "all", "ns", "ts", "cnt", "sz", "cpu", "skw", "node", "ppm", "ip", "cy", NULL };
static const char * const creatorTypeNames[]= { "", "all", "unknown", "hthor", "roxie", "roxie:s", "thor", "thor:m", "thor:s", "eclcc", "esp", "summary", NULL };
static const char * const scopeTypeNames[] = { "", "all", "global", "graph", "subgraph", "activity", "allocator", "section", "compile", "dfu", "edge", NULL };

static unsigned matchString(const char * const * names, const char * search)
{
    if (!search)
        return 0;

    if (streq(search, "*"))
        search = "all";

    unsigned i=0;
    loop
    {
        const char * next = names[i];
        if (!next)
            return 0;
        if (streq(next, search))
            return i;
        i++;
    }
}

//--------------------------------------------------------------------------------------------------------------------

extern jlib_decl unsigned __int64 getTimeStampNowValue()
{
#ifdef _WIN32
    struct _timeb now;
    _ftime(&now);
    return (unsigned __int64)now.time * I64C(1000000) + now.millitm * 1000;
#else
    struct timeval tm;
    gettimeofday(&tm,NULL);
    return (unsigned __int64)tm.tv_sec * I64C(1000000) + tm.tv_usec;
#endif
}

const static unsigned __int64 msUntilResync = 1000; // resync every second ~= 1ms accuracy
static cycle_t cyclesUntilResync;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    cyclesUntilResync = nanosec_to_cycle(msUntilResync * 1000000);
    return true;
}

OptimizedTimestamp::OptimizedTimestamp()
{
    lastCycles = get_cycles_now();
    lastTimestamp = ::getTimeStampNowValue();
}

#if 0
//This version almost certainly has problems if the computer is suspended and cycles->nanoseconds is only accurate to
//about 0.1% - so should only be used for relatively short periods
unsigned __int64 OptimizedTimestamp::getTimeStampNowValue()
{
    cycle_t nowCycles = get_cycles_now();
    return lastTimestamp + cycle_to_microsec(nowCycles - lastCycles);
}
#else
//This version will resync every minute, but is not thread safe.  Adding a critical section makes it less efficient than recalculating
unsigned __int64 OptimizedTimestamp::getTimeStampNowValue()
{
    cycle_t nowCycles = get_cycles_now();
    if (nowCycles - lastCycles > cyclesUntilResync)
    {
        lastCycles = nowCycles;
        lastTimestamp = ::getTimeStampNowValue();
    }
    return lastTimestamp + cycle_to_microsec(nowCycles - lastCycles);
}
#endif



unsigned __int64 getIPV4StatsValue(const IpAddress & ip)
{
    unsigned ipValue;
    if (ip.getNetAddress(sizeof(ipValue),&ipValue))
        return ipValue;
    return 0;
}

//--------------------------------------------------------------------------------------------------------------------

const static unsigned __int64 oneMilliSecond = I64C(1000000);
const static unsigned __int64 oneSecond = I64C(1000000000);
const static unsigned __int64 oneMinute = I64C(60000000000);
const static unsigned __int64 oneHour = I64C(3600000000000);
const static unsigned __int64 oneDay = 24 * I64C(3600000000000);

static void formatTime(StringBuffer & out, unsigned __int64 value)
{
    //Aim to display at least 3 significant digits in the result string
    if (value < oneMilliSecond)
        out.appendf("%uns", (unsigned)value);
    else if (value < oneSecond)
    {
        unsigned uvalue = (unsigned)value;
        out.appendf("%u.%03ums", uvalue / 1000000, (uvalue / 1000) % 1000);
    }
    else
    {
        unsigned days = (unsigned)(value / oneDay);
        value = value % oneDay;
        unsigned hours = (unsigned)(value / oneHour);
        value = value % oneHour;
        unsigned mins = (unsigned)(value / oneMinute);
        value = value % oneMinute;
        unsigned secs = (unsigned)(value / oneSecond);
        unsigned ns = (unsigned)(value % oneSecond);

        if (days > 0)
            out.appendf("%u days ", days);
        if (hours > 0 || days)
            out.appendf("%u:%02u:%02u", hours, mins, secs);
        else if (mins >= 10)
            out.appendf("%u:%02u", mins, secs);
        else if (mins >= 1)
            out.appendf("%u:%02u.%03u", mins, secs, ns / 1000000);
        else
            out.appendf("%u.%03us", secs, ns / 1000000);
    }
}

extern void formatTimeCollatable(StringBuffer & out, unsigned __int64 value, bool nano)
{
    unsigned days = (unsigned)(value / oneDay);
    value = value % oneDay;
    unsigned hours = (unsigned)(value / oneHour);
    value = value % oneHour;
    unsigned mins = (unsigned)(value / oneMinute);
    value = value % oneMinute;
    unsigned secs = (unsigned)(value / oneSecond);
    unsigned ns = (unsigned)(value % oneSecond);

    if (days)
        out.appendf("  %3ud ", days); // Two leading spaces helps the cassandra driver force to a single partition
    else
        out.appendf("       ");
    if (nano)
        out.appendf("%2u:%02u:%02u.%09u", hours, mins, secs, ns);
    else
        out.appendf("%2u:%02u:%02u.%03u", hours, mins, secs, ns/1000000);
    // More than 999 days, I don't care that it goes wrong.
}

extern unsigned __int64 extractTimeCollatable(const char *s, bool nano)
{
    if (!s)
        return 0;
    unsigned days,hours,mins,secs,fracs;
    if (sscanf(s, " %ud %u:%u:%u.%u", &days, &hours, &mins, &secs, &fracs)!=5)
    {
        days = 0;
        if (sscanf(s, " %u:%u:%u.%u", &hours, &mins, &secs, &fracs) != 4)
            return 0;
    }
    unsigned __int64 ret = days*oneDay + hours*oneHour + mins*oneMinute + secs*oneSecond;
    if (nano)
        ret += fracs;
    else
        ret += milliToNano(fracs);
    return ret;
}

static void formatTimeStamp(StringBuffer & out, unsigned __int64 value)
{
    time_t seconds = value / 1000000;
    unsigned us = value % 1000000;
    char timeStamp[64];
    time_t tNow = seconds;
#ifdef _WIN32
    struct tm *gmtNow;
    gmtNow = gmtime(&tNow);
    strftime(timeStamp, 64, "%Y-%m-%dT%H:%M:%S", gmtNow);
#else
    struct tm gmtNow;
    gmtime_r(&tNow, &gmtNow);
    strftime(timeStamp, 64, "%Y-%m-%dT%H:%M:%S", &gmtNow);
#endif //_WIN32
    out.append(timeStamp).appendf(".%03uZ", us / 1000);
}

static const unsigned oneKb = 1024;
static const unsigned oneMb = 1024 * 1024;
static const unsigned oneGb = 1024 * 1024 * 1024;
static unsigned toPermille(unsigned x) { return (x * 1000) / 1024; }
static void formatSize(StringBuffer & out, unsigned __int64 value)
{

    unsigned Gb = (unsigned)(value / oneGb);
    unsigned Mb = (unsigned)((value % oneGb) / oneMb);
    unsigned Kb = (unsigned)((value % oneMb) / oneKb);
    unsigned b = (unsigned)(value % oneKb);
    if (Gb)
        out.appendf("%u.%03uGb", Gb, toPermille(Mb));
    else if (Mb)
        out.appendf("%u.%03uMb", Mb, toPermille(Kb));
    else if (Kb)
        out.appendf("%u.%03uKb", Kb, toPermille(b));
    else
        out.appendf("%ub", b);
}

static void formatLoad(StringBuffer & out, unsigned __int64 value)
{
    //Stored as millionth of a core.  Display as a percentage => scale by 10,000
    out.appendf("%u.%03u%%", (unsigned)(value / 10000), (unsigned)(value % 10000) / 10);
}

static void formatSkew(StringBuffer & out, unsigned __int64 value)
{
    //Skew stored as 10000 = perfect, display as percentage
    out.appendf("%.2f%%", ((double)(__int64)value) / 100.0);
}

static void formatIPV4(StringBuffer & out, unsigned __int64 value)
{
    byte ip1 = (value & 255);
    byte ip2 = ((value >> 8) & 255);
    byte ip3 = ((value >> 16) & 255);
    byte ip4 = ((value >> 24) & 255);
    out.appendf("%d.%d.%d.%d", ip1, ip2, ip3, ip4);
}

void formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticMeasure measure)
{
    switch (measure)
    {
    case SMeasureTimeNs:
        formatTime(out, value);
        break;
    case SMeasureTimestampUs:
        formatTimeStamp(out, value);
        break;
    case SMeasureCount:
        out.append(value);
        break;
    case SMeasureSize:
        formatSize(out, value);
        break;
    case SMeasureLoad:
        formatLoad(out, value);
        break;
    case SMeasureSkew:
        formatSkew(out, value);
        break;
    case SMeasureNode:
        out.append(value);
        break;
    case SMeasurePercent:
        out.appendf("%.2f%%", (double)value / 10000.0);  // stored as ppm
        break;
    case SMeasureIPV4:
        formatIPV4(out, value);
        break;
    case SMeasureCycle:
        out.append(value);
        break;
    default:
        throwUnexpected();
    }
}

void formatStatistic(StringBuffer & out, unsigned __int64 value, StatisticKind kind)
{
    formatStatistic(out, value, queryMeasure(kind));
}

//--------------------------------------------------------------------------------------------------------------------

unsigned queryStatisticsDepth(const char * text)
{
    unsigned depth = 1;
    loop
    {
        switch (*text)
        {
            case 0:
                return depth;
            case ':':
                depth++;
                break;
        }
        text++;
    }
}


const char * queryMeasurePrefix(StatisticMeasure measure)
{
    switch (measure)
    {
    case SMeasureAll:           return NULL;
    case SMeasureTimeNs:        return "Time";
    case SMeasureTimestampUs:   return "When";
    case SMeasureCount:         return "Num";
    case SMeasureSize:          return "Size";
    case SMeasureLoad:          return "Load";
    case SMeasureSkew:          return "Skew";
    case SMeasureNode:          return "Node";
    case SMeasurePercent:       return "Per";
    case SMeasureIPV4:          return "Ip";
    case SMeasureCycle:         return "Cycle";
    default:
        throwUnexpected();
    }
}

const char * queryMeasureName(StatisticMeasure measure)
{
    return measureNames[measure];
}

StatisticMeasure queryMeasure(const char *  measure)
{
    //MORE: Use a hash table??
    StatisticMeasure ret = (StatisticMeasure)matchString(measureNames, measure);
    //Legacy support for an unusual statistic - pretend the sizes are in bytes instead of kb.
    if ((ret == SMeasureNone) && measure && streq(measure, "kb"))
        ret = SMeasureSize;
    return ret;
}

StatsMergeAction queryMergeMode(StatisticMeasure measure)
{
    switch (measure)
    {
    case SMeasureTimeNs:        return StatsMergeSum;
    case SMeasureTimestampUs:   return StatsMergeKeepNonZero;
    case SMeasureCount:         return StatsMergeSum;
    case SMeasureSize:          return StatsMergeSum;
    case SMeasureLoad:          return StatsMergeMax;
    case SMeasureSkew:          return StatsMergeMax;
    case SMeasureNode:          return StatsMergeKeepNonZero;
    case SMeasurePercent:       return StatsMergeReplace;
    case SMeasureIPV4:          return StatsMergeKeepNonZero;
    case SMeasureCycle:         return StatsMergeSum;
    default:
        throwUnexpected();
    }
}

extern jlib_decl StatsMergeAction queryMergeMode(StatisticKind kind)
{
    //MORE: Optimize by looking up in the meta
    return queryMergeMode(queryMeasure(kind));
}

//--------------------------------------------------------------------------------------------------------------------

#define BASE_NAMES(x, y) \
    #x #y, \
    #x "Min" # y,  \
    #x "Max" # y, \
    #x "Avg" # y, \
    "Skew" # y, \
    "SkewMin" # y, \
    "SkewMax" # y, \
    "NodeMin" # y, \
    "NodeMax" # y,

#define NAMES(x, y) \
    BASE_NAMES(x, y) \
    #x "Delta" # y

#define TIMENAMES(x, y) \
    BASE_NAMES(x, y) \
    "TimeDelta" # y

#define BASE_TAGS(x, y) \
    "@" #x "Min" # y,  \
    "@" #x "Max" # y, \
    "@" #x "Avg" # y, \
    "@Skew" # y, \
    "@SkewMin" # y, \
    "@SkewMax" # y, \
    "@NodeMin" # y, \
    "@NodeMax" # y,

//Default tags nothing special overriden
#define TAGS(x, y) \
    "@" #x #y, \
    BASE_TAGS(x, y) \
    "@" #x "Delta" # y

//Define the tags for time items.
#define TIMETAGS(x, y) \
    "@" #x #y, \
    BASE_TAGS(x, y) \
    "@TimeDelta" # y

#define CORESTAT(x, y, m)     St##x##y, m, { NAMES(x, y) }, { TAGS(x, y) }
#define STAT(x, y, m)         CORESTAT(x, y, m)

//--------------------------------------------------------------------------------------------------------------------

//These are the macros to use to define the different entries in the stats meta table
#define TIMESTAT(y) STAT(Time, y, SMeasureTimeNs)
#define WHENSTAT(y) St##When##y, SMeasureTimestampUs, { TIMENAMES(When, y) }, { TIMETAGS(When, y) }
#define NUMSTAT(y) STAT(Num, y, SMeasureCount)
#define SIZESTAT(y) STAT(Size, y, SMeasureSize)
#define LOADSTAT(y) STAT(Load, y, SMeasureLoad)
#define SKEWSTAT(y) STAT(Skew, y, SMeasureSkew)
#define NODESTAT(y) STAT(Node, y, SMeasureNode)
#define PERSTAT(y) STAT(Per, y, SMeasurePercent)
#define IPV4STAT(y) STAT(IPV4, y, SMeasureIPV4)
#define CYCLESTAT(y) STAT(Cycle, y, SMeasureCycle)

//--------------------------------------------------------------------------------------------------------------------

class StatisticMeta
{
public:
    StatisticKind kind;
    StatisticMeasure measure;
    const char * names[StNextModifier/StVariantScale];
    const char * tags[StNextModifier/StVariantScale];
};

//The order of entries in this table must match the order in the enumeration
static const StatisticMeta statsMetaData[StMax] = {
    { StKindNone, SMeasureNone, { "none" }, { "@none" } },
    { StKindAll, SMeasureAll, { "all" }, { "@all" } },
    { WHENSTAT(GraphStarted) },
    { WHENSTAT(GraphFinished) },
    { WHENSTAT(FirstRow) },
    { WHENSTAT(QueryStarted) },
    { WHENSTAT(QueryFinished) },
    { WHENSTAT(Created) },
    { WHENSTAT(Compiled) },
    { WHENSTAT(WorkunitModified) },
    { TIMESTAT(Elapsed) },
    { TIMESTAT(LocalExecute) },
    { TIMESTAT(TotalExecute) },
    { TIMESTAT(Remaining) },
    { SIZESTAT(GeneratedCpp) },
    { SIZESTAT(PeakMemory) },
    { SIZESTAT(MaxRowSize) },
    { NUMSTAT(RowsProcessed) },
    { NUMSTAT(Slaves) },
    { NUMSTAT(Started) },
    { NUMSTAT(Stopped) },
    { NUMSTAT(IndexSeeks) },
    { NUMSTAT(IndexScans) },
    { NUMSTAT(IndexWildSeeks) },
    { NUMSTAT(IndexSkips) },
    { NUMSTAT(IndexNullSkips) },
    { NUMSTAT(IndexMerges) },
    { NUMSTAT(IndexMergeCompares) },
    { NUMSTAT(PreFiltered) },
    { NUMSTAT(PostFiltered) },
    { NUMSTAT(BlobCacheHits) },
    { NUMSTAT(LeafCacheHits) },
    { NUMSTAT(NodeCacheHits) },
    { NUMSTAT(BlobCacheAdds) },
    { NUMSTAT(LeafCacheAdds) },
    { NUMSTAT(NodeCacheAdds) },
    { NUMSTAT(PreloadCacheHits) },
    { NUMSTAT(PreloadCacheAdds) },
    { NUMSTAT(ServerCacheHits) },
    { NUMSTAT(IndexAccepted) },
    { NUMSTAT(IndexRejected) },
    { NUMSTAT(AtmostTriggered) },
    { NUMSTAT(DiskSeeks) },
    { NUMSTAT(Iterations) },
    { LOADSTAT(WhileSorting) },
    { NUMSTAT(LeftRows) },
    { NUMSTAT(RightRows) },
    { PERSTAT(Replicated) },
    { NUMSTAT(DiskRowsRead) },
    { NUMSTAT(IndexRowsRead) },
    { NUMSTAT(DiskAccepted) },
    { NUMSTAT(DiskRejected) },
    { TIMESTAT(Soapcall) },
    { TIMESTAT(FirstExecute) },
    { TIMESTAT(DiskReadIO) },
    { TIMESTAT(DiskWriteIO) },
    { SIZESTAT(DiskRead) },
    { SIZESTAT(DiskWrite) },
    { CYCLESTAT(DiskReadIOCycles) },
    { CYCLESTAT(DiskWriteIOCycles) },
    { NUMSTAT(DiskReads) },
    { NUMSTAT(DiskWrites) },
    { NUMSTAT(Spills) },
    { TIMESTAT(SpillElapsed) },
    { TIMESTAT(SortElapsed) },
    { NUMSTAT(Groups) },
    { NUMSTAT(GroupMax) },
    { SIZESTAT(SpillFile) },
    { CYCLESTAT(SpillElapsedCycles) },
    { CYCLESTAT(SortElapsedCycles) },
};


//--------------------------------------------------------------------------------------------------------------------

StatisticMeasure queryMeasure(StatisticKind kind)
{
    unsigned variant = queryStatsVariant(kind);
    switch (variant)
    {
    case StSkew:
    case StSkewMin:
    case StSkewMax:
        return SMeasureSkew;
    case StNodeMin:
    case StNodeMax:
        return SMeasureNode;
    case StDeltaX:
        {
            StatisticMeasure measure = queryMeasure((StatisticKind)(kind & StKindMask));
            switch (measure)
            {
            case SMeasureTimestampUs:
                return SMeasureTimeNs;
            default:
                return measure;
            }
            break;
        }
    }

    StatisticKind rawkind = (StatisticKind)(kind & StKindMask);
    dbgassertex(rawkind >= StKindNone && rawkind < StMax);
    return statsMetaData[rawkind].measure;
}

const char * queryStatisticName(StatisticKind kind)
{
    StatisticKind rawkind = (StatisticKind)(kind & StKindMask);
    unsigned variant = (kind / StVariantScale);
    dbgassertex(rawkind >= StKindNone && rawkind < StMax);
    dbgassertex(variant < (StNextModifier/StVariantScale));
    return statsMetaData[rawkind].names[variant];
}


unsigned __int64 convertMeasure(StatisticMeasure from, StatisticMeasure to, unsigned __int64 value)
{
    if (from == to)
        return value;
    if ((from == SMeasureCycle) && (to == SMeasureTimeNs))
        return cycle_to_nanosec(value);
    if ((from == SMeasureTimeNs) && (to == SMeasureCycle))
        return nanosec_to_cycle(value);
    throwUnexpected();
}

unsigned __int64 convertMeasure(StatisticKind from, StatisticKind to, unsigned __int64 value)
{
    return convertMeasure(queryMeasure(from), queryMeasure(to), value);
}


//--------------------------------------------------------------------------------------------------------------------

void queryLongStatisticName(StringBuffer & out, StatisticKind kind)
{
    out.append(queryStatisticName(kind));
}

//--------------------------------------------------------------------------------------------------------------------

const char * queryTreeTag(StatisticKind kind)
{
    StatisticKind rawkind = (StatisticKind)(kind & StKindMask);
    unsigned variant = (kind / StVariantScale);
    dbgassertex(rawkind >= StKindNone && rawkind < StMax);
    dbgassertex(variant < (StNextModifier/StVariantScale));
    return statsMetaData[rawkind].tags[variant];
}

//--------------------------------------------------------------------------------------------------------------------

StatisticKind queryStatisticKind(const char * search)
{
    if (!search)
        return StKindNone;
    if (streq(search, "*"))
        return StKindAll;

    //Slow - should use a hash table....
    for (unsigned i=0; i < StMax; i++)
    {
        StatisticKind kind = (StatisticKind)i;
        const char * shortName = queryStatisticName(kind);
        if (strieq(shortName, search))
            return kind;
    }
    return StKindNone;
}

//--------------------------------------------------------------------------------------------------------------------

const char * queryCreatorTypeName(StatisticCreatorType sct)
{
    return creatorTypeNames[sct];
}

StatisticCreatorType queryCreatorType(const char * sct)
{
    //MORE: Use a hash table??
    return (StatisticCreatorType)matchString(creatorTypeNames, sct);
}

//--------------------------------------------------------------------------------------------------------------------

const char * queryScopeTypeName(StatisticScopeType sst)
{
    return scopeTypeNames[sst];
}

extern jlib_decl StatisticScopeType queryScopeType(const char * sst)
{
    //MORE: Use a hash table??
    return (StatisticScopeType)matchString(scopeTypeNames, sst);
}

//--------------------------------------------------------------------------------------------------------------------

inline void mergeUpdate(StatisticMeasure measure, unsigned __int64 & value, const unsigned __int64 otherValue)
{
    switch (measure)
    {
    case SMeasureTimeNs:
    case SMeasureCount:
    case SMeasureSize:
    case SMeasureLoad:
    case SMeasureSkew:
    case SMeasureCycle:
        value += otherValue;
        break;
    case SMeasureTimestampUs:
        if (otherValue && otherValue < value)
            value = otherValue;
        break;
    }
}

unsigned __int64 mergeStatistic(StatisticMeasure measure, unsigned __int64 value, unsigned __int64 otherValue)
{
    mergeUpdate(measure, value, otherValue);
    return value;
}

unsigned __int64 mergeStatisticValue(unsigned __int64 prevValue, unsigned __int64 newValue, StatsMergeAction mergeAction)
{
    switch (mergeAction)
    {
    case StatsMergeKeepNonZero:
        if (prevValue)
            return prevValue;
        return newValue;
    case StatsMergeAppend:
    case StatsMergeReplace:
        return newValue;
    case StatsMergeSum:
        return prevValue + newValue;
    case StatsMergeMin:
        if (prevValue > newValue)
            return newValue;
        else
            return prevValue;
    case StatsMergeMax:
        if (prevValue < newValue)
            return newValue;
        else
            return prevValue;
    default:
        throwUnexpected();
    }
}

//--------------------------------------------------------------------------------------------------------------------

class CComponentStatistics
{
 protected:
    StringAttr creator;
    byte creatorDepth;
    byte scopeDepth;
//    StatisticArray stats;
};

//--------------------------------------------------------------------------------------------------------------------

static int compareUnsigned(unsigned const * left, unsigned const * right)
{
    return (*left < *right) ? -1 : (*left > *right) ? +1 : 0;
}

StatisticsMapping::StatisticsMapping(StatisticKind kind, ...)
{
    indexToKind.append(kind);
    va_list args;
    va_start(args, kind);
    for (;;)
    {
        unsigned next  = va_arg(args, unsigned);
        if (!next)
            break;
        indexToKind.appendUniq(next);
    }
    va_end(args);
    createMappings();
}

StatisticsMapping::StatisticsMapping(const StatisticsMapping * from, ...)
{
    ForEachItemIn(idx, from->indexToKind)
        indexToKind.append(from->indexToKind.item(idx));
    va_list args;
    va_start(args, from);
    for (;;)
    {
        unsigned next  = va_arg(args, unsigned);
        if (!next)
            break;
        indexToKind.appendUniq(next);
    }
    va_end(args);
    createMappings();
}

StatisticsMapping::StatisticsMapping()
{
    for (int i = StKindAll+1; i < StMax; i++)
        indexToKind.append(i);
    createMappings();
}

void StatisticsMapping::createMappings()
{
    //Possibly not needed, but sort the kinds, so that it is easy to merge/stream the results out in the correct order.
    indexToKind.sort(compareUnsigned);

    //Provide mappings to all statistics to map them to the "unknown" bin by default
    for (unsigned i=0; i < StMax; i++)
        kindToIndex.append(numStatistics());

    ForEachItemIn(i2, indexToKind)
    {
        unsigned kind = indexToKind.item(i2);
        kindToIndex.replace(i2, kind);
    }
}

const StatisticsMapping allStatistics;
const StatisticsMapping diskLocalStatistics(StCycleDiskReadIOCycles, StSizeDiskRead, StNumDiskReads, StCycleDiskWriteIOCycles, StSizeDiskWrite, StNumDiskWrites, StKindNone);
const StatisticsMapping diskRemoteStatistics(StTimeDiskReadIO, StSizeDiskRead, StNumDiskReads, StTimeDiskWriteIO, StSizeDiskWrite, StNumDiskWrites, StKindNone);
const StatisticsMapping diskReadRemoteStatistics(StTimeDiskReadIO, StSizeDiskRead, StNumDiskReads, StKindNone);
const StatisticsMapping diskWriteRemoteStatistics(StTimeDiskWriteIO, StSizeDiskWrite, StNumDiskWrites, StKindNone);

//--------------------------------------------------------------------------------------------------------------------

class Statistic : public CInterfaceOf<IStatistic>
{
public:
    Statistic(StatisticKind _kind, unsigned __int64 _value) : kind(_kind), value(_value)
    {
    }
    static Statistic * deserialize(MemoryBuffer & in, unsigned version)
    {
        return new Statistic(in, version);
    }

    virtual StatisticKind queryKind() const
    {
        return kind;
    }
    virtual unsigned __int64 queryValue() const
    {
        return value;
    }

    void merge(unsigned __int64 otherValue)
    {
        mergeUpdate(queryMeasure(kind), value, otherValue);
    }
    void serialize(MemoryBuffer & out) const
    {
        //MORE: Could compress - e.g., store as a packed integers
        out.append((unsigned)kind);
        out.append(value);
    }

protected:
    Statistic(MemoryBuffer & in, unsigned version)
    {
        unsigned _kind;
        in.read(_kind);
        kind = (StatisticKind)_kind;
        in.read(value);
    }

public:
    StatisticKind kind;
    unsigned __int64 value;
};

//--------------------------------------------------------------------------------------------------------------------

StringBuffer & StatsScopeId::getScopeText(StringBuffer & out) const
{
    switch (scopeType)
    {
    case SSTgraph:
        return out.append(GraphScopePrefix).append(id);
    case SSTsubgraph:
        return out.append(SubGraphScopePrefix).append(id);
    case SSTactivity:
        return out.append(ActivityScopePrefix).append(id);
    case SSTedge:
        return out.append(EdgeScopePrefix).append(id).append("_").append(extra);
    default:
        throwUnexpected();
    }
}

unsigned StatsScopeId::getHash() const
{
    return hashc((const byte *)&id, sizeof(id), (unsigned)scopeType);
}

bool StatsScopeId::matches(const StatsScopeId & other) const
{
    return (scopeType == other.scopeType) && (id == other.id) && (extra == other.extra);
}

unsigned StatsScopeId::queryActivity() const
{
    switch (scopeType)
    {
    case SSTactivity:
    case SSTedge:
        return id;
    default:
        return 0;
    }
}

void StatsScopeId::deserialize(MemoryBuffer & in, unsigned version)
{
    byte scopeTypeByte;
    in.read(scopeTypeByte);
    scopeType = (StatisticScopeType)scopeTypeByte;
    switch (scopeType)
    {
    case SSTgraph:
    case SSTsubgraph:
    case SSTactivity:
        in.read(id);
        break;
    case SSTedge:
        in.read(id);
        in.read(extra);
        break;
    default:
        throwUnexpected();
    }
}

void StatsScopeId::serialize(MemoryBuffer & out) const
{
    out.append((byte)scopeType);
    switch (scopeType)
    {
    case SSTgraph:
    case SSTsubgraph:
    case SSTactivity:
        out.append(id);
        break;
    case SSTedge:
        out.append(id);
        out.append(extra);
        break;
    default:
        throwUnexpected();
    }
}

void StatsScopeId::setId(StatisticScopeType _scopeType, unsigned _id, unsigned _extra)
{
    scopeType = _scopeType;
    id = _id;
    extra = _extra;
}

bool StatsScopeId::setScopeText(const char * text)
{
    if (MATCHES_CONST_PREFIX(text, ActivityScopePrefix))
        setActivityId(atoi(text + CONST_STRLEN(ActivityScopePrefix)));
    else if (MATCHES_CONST_PREFIX(text, GraphScopePrefix))
        setId(SSTgraph, atoi(text + CONST_STRLEN(GraphScopePrefix)));
    else if (MATCHES_CONST_PREFIX(text, SubGraphScopePrefix))
        setSubgraphId(atoi(text + CONST_STRLEN(SubGraphScopePrefix)));
    else if (MATCHES_CONST_PREFIX(text, EdgeScopePrefix))
    {
        const char * underscore = strchr(text, '_');
        if (!underscore)
            return false;
        setEdgeId(atoi(text + CONST_STRLEN(EdgeScopePrefix)), atoi(underscore+1));
    }
    else
        return false;

    return true;
}

void StatsScopeId::setActivityId(unsigned _id)
{
    setId(SSTactivity, _id);
}
void StatsScopeId::setEdgeId(unsigned _id, unsigned _output)
{
    setId(SSTedge, _id, _output);
}
void StatsScopeId::setSubgraphId(unsigned _id)
{
    setId(SSTsubgraph, _id);
}

//--------------------------------------------------------------------------------------------------------------------

enum
{
    SCroot,
    SCintermediate,
    SCleaf,
};

class CStatisticCollection;
static CStatisticCollection * deserializeCollection(CStatisticCollection * parent, MemoryBuffer & in, unsigned version);

//MORE: Create an implementation with no children
typedef StructArrayOf<Statistic> StatsArray;
class CollectionHashTable : public SuperHashTableOf<CStatisticCollection, StatsScopeId>
{
public:
    ~CollectionHashTable() { releaseAll(); }
    virtual void     onAdd(void *et);
    virtual void     onRemove(void *et);
    virtual unsigned getHashFromElement(const void *et) const;
    virtual unsigned getHashFromFindParam(const void *fp) const;
    virtual const void * getFindParam(const void *et) const;
    virtual bool matchesFindParam(const void *et, const void *key, unsigned fphash) const;
    virtual bool matchesElement(const void *et, const void *searchET) const;
};
typedef IArrayOf<CStatisticCollection> CollectionArray;

class CStatisticCollection : public CInterfaceOf<IStatisticCollection>
{
    friend class CollectionHashTable;
public:
    CStatisticCollection(CStatisticCollection * _parent, const StatsScopeId & _id) : id(_id), parent(_parent)
    {
    }

    CStatisticCollection(CStatisticCollection * _parent, MemoryBuffer & in, unsigned version) : parent(_parent)
    {
        id.deserialize(in, version);

        unsigned numStats;
        in.read(numStats);
        stats.ensure(numStats);
        while (numStats-- > 0)
        {
            Statistic * next = Statistic::deserialize(in, version);
            stats.append(*next);
            next->Release();
        }

        unsigned numChildren;
        in.read(numChildren);
        children.ensure(numChildren);
        while (numChildren-- > 0)
        {
            CStatisticCollection * next = deserializeCollection(this, in, version);
            children.add(*next);
        }
    }

    virtual byte getCollectionType() const { return SCintermediate; }

//interface IStatisticCollection:
    virtual StatisticScopeType queryScopeType() const
    {
        return id.queryScopeType();
    }
    virtual unsigned __int64 queryWhenCreated() const
    {
        if (parent)
            return parent->queryWhenCreated();
        return 0;
    }
    virtual StringBuffer & getScope(StringBuffer & str) const
    {
        return id.getScopeText(str);
    }
    virtual StringBuffer & getFullScope(StringBuffer & str) const
    {
        if (parent)
        {
            parent->getFullScope(str);
            str.append(':');
        }
        id.getScopeText(str);
        return str;
    }
    virtual unsigned __int64 queryStatistic(StatisticKind kind) const
    {
        ForEachItemIn(i, stats)
        {
            const Statistic & cur = stats.item(i);
            if (cur.kind == kind)
                return cur.value;
        }
        return 0;
    }
    virtual unsigned getNumStatistics() const
    {
        return stats.ordinality();
    }
    virtual void getStatistic(StatisticKind & kind, unsigned __int64 & value, unsigned idx) const
    {
        const Statistic & cur = stats.item(idx);
        kind = cur.kind;
        value = cur.value;
    }
    virtual IStatisticCollectionIterator & getScopes(const char * filter)
    {
        assertex(!filter);
        return * new SuperHashIIteratorOf<IStatisticCollection, IStatisticCollectionIterator, false>(children);
    }

    virtual void getMinMaxScope(IStringVal & minValue, IStringVal & maxValue, StatisticScopeType searchScopeType) const
    {
        if (id.queryScopeType() == searchScopeType)
        {
            const char * curMin = minValue.str();
            const char * curMax = maxValue.str();
            StringBuffer name;
            id.getScopeText(name);
            if (!curMin || !*curMin || strcmp(name.str(), curMin) < 0)
                minValue.set(name.str());
            if (!curMax || strcmp(name.str(), curMax) > 0)
                maxValue.set(name.str());
        }

        SuperHashIteratorOf<CStatisticCollection> iter(children, false);
        for (iter.first(); iter.isValid(); iter.next())
            iter.query().getMinMaxScope(minValue, maxValue, searchScopeType);
    }

    virtual void getMinMaxActivity(unsigned & minValue, unsigned & maxValue) const
    {
        unsigned activityId = id.queryActivity();
        if (activityId)
        {
            if ((minValue == 0) || (activityId < minValue))
                minValue = activityId;
            if (activityId > maxValue)
                maxValue = activityId;
        }

        SuperHashIteratorOf<CStatisticCollection> iter(children, false);
        for (iter.first(); iter.isValid(); iter.next())
            iter.query().getMinMaxActivity(minValue, maxValue);
    }

//other public interface functions
    void addStatistic(StatisticKind kind, unsigned __int64 value)
    {
        Statistic s(kind, value);
        stats.append(s);
    }

    void updateStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction)
    {
        if (mergeAction != StatsMergeAppend)
        {
            ForEachItemIn(i, stats)
            {
                Statistic & cur = stats.element(i);
                if (cur.kind == kind)
                {
                    cur.value = mergeStatisticValue(cur.value, value, mergeAction);
                    return;
                }
            }
        }
        Statistic s(kind, value);
        stats.append(s);
    }

    CStatisticCollection * ensureSubScope(const StatsScopeId & search, bool hasChildren)
    {
        //MORE: Implement hasChildren
        return resolveSubScope(search, true, false);
    }

    CStatisticCollection * resolveSubScope(const StatsScopeId & search, bool create, bool replace)
    {
        if (!replace)
        {
            CStatisticCollection * match = children.find(&search);
            if (match)
                return LINK(match);
        }
        if (create)
        {
            CStatisticCollection * ret = new CStatisticCollection(this, search);
            children.add(*ret);
            return LINK(ret);
        }
        return NULL;
    }

    virtual void serialize(MemoryBuffer & out) const
    {
        out.append(getCollectionType());
        id.serialize(out);

        out.append(stats.ordinality());
        ForEachItemIn(iStat, stats)
            stats.item(iStat).serialize(out);

        out.append(children.ordinality());
        SuperHashIteratorOf<CStatisticCollection> iter(children, false);
        for (iter.first(); iter.isValid(); iter.next())
            iter.query().serialize(out);
    }

private:
    StatsScopeId id;
    CStatisticCollection * parent;
    CollectionHashTable children;
    StatsArray stats;
};

//---------------------------------------------------------------------------------------------------------------------

void CollectionHashTable::onAdd(void *et)
{
}
void CollectionHashTable::onRemove(void *et)
{
    CStatisticCollection * elem = reinterpret_cast<CStatisticCollection *>(et);
    elem->Release();
}
unsigned CollectionHashTable::getHashFromElement(const void *et) const
{
    const CStatisticCollection * elem = reinterpret_cast<const CStatisticCollection *>(et);
    return elem->id.getHash();
}
unsigned CollectionHashTable::getHashFromFindParam(const void *fp) const
{
    const StatsScopeId * search = reinterpret_cast<const StatsScopeId *>(fp);
    return search->getHash();
}
const void * CollectionHashTable::getFindParam(const void *et) const
{
    const CStatisticCollection * elem = reinterpret_cast<const CStatisticCollection *>(et);
    return &elem->id;
}
bool CollectionHashTable::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    const CStatisticCollection * elem = reinterpret_cast<const CStatisticCollection *>(et);
    const StatsScopeId * search = reinterpret_cast<const StatsScopeId *>(key);
    return elem->id.matches(*search);
}
bool CollectionHashTable::matchesElement(const void *et, const void *searchET) const
{
    const CStatisticCollection * elem = reinterpret_cast<const CStatisticCollection *>(et);
    const CStatisticCollection * searchElem = reinterpret_cast<const CStatisticCollection *>(searchET);
    return elem->id.matches(searchElem->id);
}

//---------------------------------------------------------------------------------------------------------------------

class CRootStatisticCollection : public CStatisticCollection
{
public:
    CRootStatisticCollection(StatisticCreatorType _creatorType, const char * _creator, const StatsScopeId & _id)
        : CStatisticCollection(NULL, _id), creatorType(_creatorType), creator(_creator)
    {
        whenCreated = getTimeStampNowValue();
    }
    CRootStatisticCollection(MemoryBuffer & in, unsigned version) : CStatisticCollection(NULL, in, version)
    {
        byte creatorTypeByte;
        in.read(creatorTypeByte);
        creatorType = (StatisticCreatorType)creatorTypeByte;
        in.read(creator);
        in.read(whenCreated);
    }

    virtual byte getCollectionType() const { return SCroot; }

    virtual unsigned __int64 queryWhenCreated() const
    {
        return whenCreated;
    }
    virtual void serialize(MemoryBuffer & out) const
    {
        CStatisticCollection::serialize(out);
        out.append((byte)creatorType);
        out.append(creator);
        out.append(whenCreated);
    }
public:
    StatisticCreatorType creatorType;
    StringAttr creator;
    unsigned __int64 whenCreated;
};

//---------------------------------------------------------------------------------------------------------------------

void serializeStatisticCollection(MemoryBuffer & out, IStatisticCollection * collection)
{
    unsigned currentStatisticsVersion = 1;
    out.append(currentStatisticsVersion);
    collection->serialize(out);
}

static CStatisticCollection * deserializeCollection(CStatisticCollection * parent, MemoryBuffer & in, unsigned version)
{
    byte kind;
    in.read(kind);
    switch (kind)
    {
    case SCroot:
        assertex(!parent);
        return new CRootStatisticCollection(in, version);
    case SCintermediate:
        return new CStatisticCollection(parent, in, version);
    default:
        UNIMPLEMENTED;
    }
}

IStatisticCollection * createStatisticCollection(MemoryBuffer & in)
{
    unsigned version;
    in.read(version);
    return deserializeCollection(NULL, in, version);
}


//--------------------------------------------------------------------------------------------------------------------

class StatisticGatherer : implements CInterfaceOf<IStatisticGatherer>
{
public:
    StatisticGatherer(CStatisticCollection * scope) : rootScope(scope)
    {
        scopes.append(*scope);
    }
    virtual void beginScope(const StatsScopeId & id)
    {
        CStatisticCollection & tos = scopes.tos();
        scopes.append(*tos.ensureSubScope(id, true));
    }
    virtual void beginActivityScope(unsigned id)
    {
        StatsScopeId scopeId(SSTactivity, id);
        CStatisticCollection & tos = scopes.tos();
        scopes.append(*tos.ensureSubScope(scopeId, false));
    }
    virtual void beginSubGraphScope(unsigned id)
    {
        StatsScopeId scopeId(SSTsubgraph, id);
        CStatisticCollection & tos = scopes.tos();
        scopes.append(*tos.ensureSubScope(scopeId, true));
    }
    virtual void beginEdgeScope(unsigned id, unsigned oid)
    {
        StatsScopeId scopeId(SSTedge, id, oid);
        CStatisticCollection & tos = scopes.tos();
        scopes.append(*tos.ensureSubScope(scopeId, false));
    }
    virtual void endScope()
    {
        scopes.tos().Release();
        scopes.pop();
    }
    virtual void addStatistic(StatisticKind kind, unsigned __int64 value)
    {
        CStatisticCollection & tos = scopes.tos();
        tos.addStatistic(kind, value);
    }
    virtual void updateStatistic(StatisticKind kind, unsigned __int64 value, StatsMergeAction mergeAction)
    {
        CStatisticCollection & tos = scopes.tos();
        tos.updateStatistic(kind, value, mergeAction);
    }
    virtual IStatisticCollection * getResult()
    {
        return LINK(rootScope);
    }

protected:
    ICopyArrayOf<CStatisticCollection> scopes;
    Linked<CStatisticCollection> rootScope;
};

extern IStatisticGatherer * createStatisticsGatherer(StatisticCreatorType creatorType, const char * creator, const StatsScopeId & rootScope)
{
    //creator unused at the moment.
    Owned<CStatisticCollection> rootCollection = new CRootStatisticCollection(creatorType, creator, rootScope);
    return new StatisticGatherer(rootCollection);
}

//--------------------------------------------------------------------------------------------------------------------

void CRuntimeStatistic::merge(unsigned __int64 otherValue, StatsMergeAction mergeAction)
{
    value = mergeStatisticValue(value, otherValue, mergeAction);
}

void CRuntimeStatisticCollection::merge(const CRuntimeStatisticCollection & other)
{
    ForEachItemIn(i, other)
    {
        StatisticKind kind = other.getKind(i);
        unsigned __int64 value = other.getStatisticValue(kind);
        if (value)
            mergeStatistic(kind, other.getStatisticValue(kind));
    }
}

void CRuntimeStatisticCollection::mergeStatistic(StatisticKind kind, unsigned __int64 value)
{
    queryStatistic(kind).merge(value, queryMergeMode(kind));
}

void CRuntimeStatisticCollection::rollupStatistics(unsigned numTargets, IContextLogger * const * targets) const
{
    ForEachItem(iStat)
    {
        unsigned __int64 value = values[iStat].getClear();
        if (value)
        {
            StatisticKind kind = getKind(iStat);
            for (unsigned iTarget = 0; iTarget < numTargets; iTarget++)
                targets[iTarget]->noteStatistic(kind, value);
        }
    }
    reportIgnoredStats();
}

void CRuntimeStatisticCollection::recordStatistics(IStatisticGatherer & target) const
{
    ForEachItem(i)
    {
        unsigned __int64 value = values[i].get();
        if (value)
        {
            StatisticKind kind = getKind(i);
            StatsMergeAction mergeAction = queryMergeMode(kind);
            target.updateStatistic(kind, values[i].get(), mergeAction);
        }
    }
    reportIgnoredStats();
}

void CRuntimeStatisticCollection::reportIgnoredStats() const
{
    if (values[mapping.numStatistics()].getClear())
        DBGLOG("Some statistics were added but thrown away");
}

StringBuffer & CRuntimeStatisticCollection::toXML(StringBuffer &str) const
{
    ForEachItem(iStat)
    {
        unsigned __int64 value = values[iStat].get();
        if (value)
        {
            StatisticKind kind = getKind(iStat);
            const char * name = queryStatisticName(kind);
            str.appendf("<%s>%" I64F "d</%s>", name, value, name);
        }
    }
    return str;
}

StringBuffer & CRuntimeStatisticCollection::toStr(StringBuffer &str) const
{
    ForEachItem(iStat)
    {
        unsigned __int64 value = values[iStat].get();
        if (value)
        {
            StatisticKind kind = getKind(iStat);
            const char * name = queryStatisticName(kind);
            str.append(' ').append(name).append("=");
            formatStatistic(str, value, kind);
        }
    }
    return str;
}

void CRuntimeStatisticCollection::deserialize(MemoryBuffer& in)
{
    unsigned numValid;
    in.read(numValid);
    for (unsigned i=0; i < numValid; i++)
    {
        unsigned kindVal;
        unsigned __int64 value;
        in.read(kindVal).read(value);
        StatisticKind kind = (StatisticKind)kindVal;
        setStatistic(kind, value);
    }
}

void CRuntimeStatisticCollection::deserializeMerge(MemoryBuffer& in)
{
    unsigned numValid;
    in.read(numValid);
    for (unsigned i=0; i < numValid; i++)
    {
        unsigned kindVal;
        unsigned __int64 value;
        in.read(kindVal).read(value);
        StatisticKind kind = (StatisticKind)kindVal;
        StatsMergeAction mergeAction = queryMergeMode(kind);
        mergeStatistic(kind, value, mergeAction);
    }
}

bool CRuntimeStatisticCollection::serialize(MemoryBuffer& out) const
{
    unsigned numValid = 0;
    ForEachItem(i1)
    {
        if (values[i1].get())
            numValid++;
    }
    //out.ensure(sizeof(unsigned)+numValid*(sizeof(unsigned)+sizeof(unsigned __int64)));
    out.append(numValid);
    ForEachItem(i2)
    {
        unsigned __int64 value = values[i2].get();
        if (value)
        {
            out.append((unsigned)mapping.getKind(i2));
            out.append(value);
        }
    }
    return numValid != 0;
}


//---------------------------------------------------

bool ScopedItemFilter::matchDepth(unsigned low, unsigned high) const
{
    if (maxDepth && low && maxDepth < low)
        return false;
    if (minDepth && high && minDepth > high)
        return false;
    return true;
}

bool ScopedItemFilter::match(const char * search) const
{
    if (search)
    {
        if (value)
        {
            if (hasWildcard)
            {
                //MORE: If wildcarding ends up being used a lot then this should be replaced with something that creates a DFA
                if (!WildMatch(search, value, false))
                    return false;
            }
            else
            {
                return streq(search, value);
            }
        }

        if (minDepth || maxDepth)
        {
            unsigned searchDepth = queryStatisticsDepth(search);
            if (searchDepth < minDepth)
                return false;
            if (maxDepth && searchDepth > maxDepth)
                return false;
        }
    }
    return true;
}


bool ScopedItemFilter::recurseChildScopes(const char * curScope) const
{
    if (maxDepth == 0 || !curScope)
        return true;

    if (queryStatisticsDepth(curScope) >= maxDepth)
        return false;
    return true;
}

void ScopedItemFilter::set(const char * _value)
{
    if (_value && *_value && !streq(_value, "*") )
    {
        value.set(_value);
        minDepth = queryStatisticsDepth(_value);
        if (!strchr(_value, '*'))
        {
            maxDepth = minDepth;
            hasWildcard = strchr(_value, '?') != NULL;
        }
        else
            hasWildcard = true;
    }
    else
        value.clear();
}

void ScopedItemFilter::setDepth(unsigned _depth)
{
    minDepth = _depth;
    maxDepth = _depth;
}

void ScopedItemFilter::setDepth(unsigned _minDepth, unsigned _maxDepth)
{
    minDepth = _minDepth;
    maxDepth = _maxDepth;
}


StatisticsFilter::StatisticsFilter()
{
    init();
}

StatisticsFilter::StatisticsFilter(const char * filter)
{
    init();
    setFilter(filter);
}

StatisticsFilter::StatisticsFilter(StatisticCreatorType _creatorType, StatisticScopeType _scopeType, StatisticMeasure _measure, StatisticKind _kind)
{
    init();
    creatorType = _creatorType;
    scopeType = _scopeType;
    measure = _measure;
    kind = _kind;
}

StatisticsFilter::StatisticsFilter(const char * _creatorType, const char * _scopeType, const char * _kind)
{
    init();
    set(_creatorType, _scopeType, _kind);
}

StatisticsFilter::StatisticsFilter(const char * _creatorTypeText, const char * _creator, const char * _scopeTypeText, const char * _scope, const char * _measureText, const char * _kindText)
{
    init();
    set(_creatorTypeText, _creator, _scopeTypeText, _scope, _measureText, _kindText);
}

StatisticsFilter::StatisticsFilter(StatisticCreatorType _creatorType, const char * _creator, StatisticScopeType _scopeType, const char * _scope, StatisticMeasure _measure, StatisticKind _kind)
{
    init();
    creatorType = _creatorType;
    setCreator(_creator);
    scopeType = _scopeType;
    setScope(_scope);
    measure = _measure;
    kind = _kind;
}

void StatisticsFilter::init()
{
    creatorType = SCTall;
    scopeType = SSTall;
    measure = SMeasureAll;
    kind = StKindAll;
}

bool StatisticsFilter::matches(StatisticCreatorType curCreatorType, const char * curCreator, StatisticScopeType curScopeType, const char * curScope, StatisticMeasure curMeasure, StatisticKind curKind) const
{
    if ((curCreatorType != SCTall) && (creatorType != SCTall) && (creatorType != curCreatorType))
        return false;
    if ((curScopeType != SSTall) && (scopeType != SSTall) && (scopeType != curScopeType))
        return false;
    if ((curMeasure != SMeasureAll) && (measure != SMeasureAll) && (measure != curMeasure))
        return false;
    if ((curKind!= StKindAll) && (kind != StKindAll) && (kind != curKind))
        return false;
    if (!creatorFilter.match(curCreator))
        return false;
    if (!scopeFilter.match(curScope))
        return false;
    return true;
}

bool StatisticsFilter::recurseChildScopes(StatisticScopeType curScopeType, const char * curScope) const
{
    return scopeFilter.recurseChildScopes(curScope);
}


void StatisticsFilter::set(const char * creatorTypeText, const char * scopeTypeText, const char * kindText)
{
    StatisticCreatorType creatorType = queryCreatorType(creatorTypeText);
    StatisticScopeType scopeType = queryScopeType(scopeTypeText);

    if (creatorType != SCTnone)
        setCreatorType(creatorType);
    if (scopeType != SSTnone)
        setScopeType(scopeType);
    setKind(kindText);
}

void StatisticsFilter::set(const char * _creatorTypeText, const char * _creator, const char * _scopeTypeText, const char * _scope, const char * _measureText, const char * _kindText)
{
    StatisticMeasure newMeasure = queryMeasure(_measureText);
    if (newMeasure != SMeasureNone)
        setMeasure(newMeasure);
    set(_creatorTypeText, _scopeTypeText, _kindText);
    setCreator(_creator);
    setScope(_scope);
}

void StatisticsFilter::setCreatorDepth(unsigned _minCreatorDepth, unsigned _maxCreatorDepth)
{
    creatorFilter.setDepth(_minCreatorDepth, _maxCreatorDepth);
}

void StatisticsFilter::setCreator(const char * _creator)
{
    creatorFilter.set(_creator);
}

void StatisticsFilter::setCreatorType(StatisticCreatorType _creatorType)
{
    creatorType = _creatorType;
}

void StatisticsFilter::setFilter(const char * filter)
{
    //MORE: Process the filter from a text representation
}

void StatisticsFilter::setScopeDepth(unsigned _scopeDepth)
{
    scopeFilter.setDepth(_scopeDepth);
}

void StatisticsFilter::setScopeDepth(unsigned _minScopeDepth, unsigned _maxScopeDepth)
{
    scopeFilter.setDepth(_minScopeDepth, _maxScopeDepth);
}

void StatisticsFilter::setScope(const char * _scope)
{
    scopeFilter.set(_scope);
}

void StatisticsFilter::setScopeType(StatisticScopeType _scopeType)
{
    scopeType = _scopeType;
}

void StatisticsFilter::setMeasure(StatisticMeasure _measure)
{
    measure = _measure;
}

void StatisticsFilter::setKind(StatisticKind _kind)
{
    kind = _kind;
    if (measure == SMeasureAll)
        measure = queryMeasure(kind);
}

void StatisticsFilter::setKind(const char * _kind)
{
    if (!_kind || !*_kind || streq(_kind, "*"))
    {
        if (measure == SMeasureNone)
            measure = SMeasureAll;
        kind = StKindAll;
        return;
    }

    //Convert a kind wildcard to a measure
    for (unsigned i1=SMeasureAll+1; i1 < SMeasureMax; i1++)
    {
        const char * prefix = queryMeasurePrefix((StatisticMeasure)i1);
        size_t len = strlen(prefix);
        if (strnicmp(_kind, prefix, len) == 0)
        {
            setMeasure((StatisticMeasure)i1);
            //Treat When* and When as filters on times.
            if (streq(_kind + len, "*") || !_kind[len])
                return;
        }
    }

    //Other wildcards not currently supported
    kind = queryStatisticKind(_kind);
}


//---------------------------------------------------

class CStatsCategory : public CInterface
{
public:
    StringAttr longName;
    StringAttr shortName;

    CStatsCategory(const char *_longName, const char *_shortName)
        : longName(_longName), shortName(_shortName)
    {
    }
    bool match(const char *_longName, const char *_shortName)
    {
        bool lm = stricmp(_longName, longName)==0;
        bool sm = stricmp(_shortName, shortName)==0;
        if (lm || sm)
        {
            if (lm && sm)
                return true;
            throw MakeStringException(0, "A stats category %s (%s) is already registered", shortName.get(), longName.get());
        }
        return false;
    }
};

static CIArrayOf<CStatsCategory> statsCategories;
static CriticalSection statsCategoriesCrit;

extern int registerStatsCategory(const char *longName, const char *shortName)
{
    CriticalBlock b(statsCategoriesCrit);
    ForEachItemIn(idx, statsCategories)
    {
        if (statsCategories.item(idx).match(longName, shortName))
            return idx;
    }
    statsCategories.append(*new CStatsCategory(longName, shortName));
    return statsCategories.ordinality()-1;
}

static void checkKind(StatisticKind kind)
{
    if (kind < StMax)
    {
        const StatisticMeta & meta = statsMetaData[kind];
        if (meta.kind != kind)
            throw makeStringExceptionV(0, "Statistic %u in the wrong order", kind);
    }

    StatisticMeasure measure = queryMeasure(kind);
    const char * shortName = queryStatisticName(kind);
    StringBuffer longName;
    queryLongStatisticName(longName, kind);
    const char * tagName __attribute__ ((unused)) = queryTreeTag(kind);
    const char * prefix = queryMeasurePrefix(measure);
    //Check short names are all correctly prefixed.
    assertex(strncmp(shortName, prefix, strlen(prefix)) == 0);
}

static void checkDistributedKind(StatisticKind kind)
{
    checkKind(kind);
    checkKind((StatisticKind)(kind|StMinX));
    checkKind((StatisticKind)(kind|StMaxX));
    checkKind((StatisticKind)(kind|StAvgX));
    checkKind((StatisticKind)(kind|StSkew));
    checkKind((StatisticKind)(kind|StSkewMin));
    checkKind((StatisticKind)(kind|StSkewMax));
    checkKind((StatisticKind)(kind|StNodeMin));
    checkKind((StatisticKind)(kind|StNodeMax));
    checkKind((StatisticKind)(kind|StDeltaX));
}

void verifyStatisticFunctions()
{
    assertex(_elements_in(measureNames) == SMeasureMax+1 && !measureNames[SMeasureMax]);
    assertex(_elements_in(creatorTypeNames) == SCTmax+1 && !creatorTypeNames[SCTmax]);
    assertex(_elements_in(scopeTypeNames) == SSTmax+1 && !scopeTypeNames[SSTmax]);

    //Check the various functions return values for all possible values.
    for (unsigned i1=SMeasureAll; i1 < SMeasureMax; i1++)
    {
        const char * prefix __attribute__((unused)) = queryMeasurePrefix((StatisticMeasure)i1);
        const char * name = queryMeasureName((StatisticMeasure)i1);
        assertex(queryMeasure(name) == i1);
    }

    for (StatisticScopeType sst = SSTnone; sst < SSTmax; sst = (StatisticScopeType)(sst+1))
    {
        const char * name = queryScopeTypeName(sst);
        assertex(queryScopeType(name) == sst);

    }

    for (StatisticCreatorType sct = SCTnone; sct < SCTmax; sct = (StatisticCreatorType)(sct+1))
    {
        const char * name = queryCreatorTypeName(sct);
        assertex(queryCreatorType(name) == sct);
    }

    for (unsigned i2=StKindAll+1; i2 < StMax; i2++)
    {
        checkDistributedKind((StatisticKind)i2);
    }
}

#if 0
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    verifyStatisticFunctions();
    return true;
}
#endif
