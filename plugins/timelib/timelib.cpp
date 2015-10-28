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

#include <platform.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <eclrtl.hpp>

#ifdef _WIN32
#include <sys/timeb.h>
#endif

#include "timelib.hpp"

static const char * compatibleVersions[] = {
    NULL };

#define TIMELIB_VERSION "TIMELIB 1.0.0"

static const char * EclDefinition =
"EXPORT TMPartsRec := RECORD \n"
"  INTEGER4 sec; \n"
"  INTEGER4 min; \n"
"  INTEGER4 hour; \n"
"  INTEGER4 mday; \n"
"  INTEGER4 mon; \n"
"  INTEGER4 year; \n"
"  INTEGER4 wday; \n"
"END;"
"EXPORT TMDateRangeRec := RECORD \n"
"  UNSIGNED4 startDate; \n"
"  UNSIGNED4 endDate; \n"
"END;"
"EXPORT TimeLib := SERVICE : fold\n"
"  INTEGER8 SecondsFromParts(integer2 year, unsigned1 month, unsigned1 day, unsigned1 hour, unsigned1 minute, unsigned1 second, boolean is_local_time) : c,pure,entrypoint='tlSecondsFromParts'; \n"
"  TRANSFORM(TMPartsRec) SecondsToParts(INTEGER8 seconds) : c,pure,entrypoint='tlSecondsToParts'; \n"
"  UNSIGNED2 GetDayOfYear(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) : c,pure,entrypoint='tlGetDayOfYear'; \n"
"  UNSIGNED1 GetDayOfWeek(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) : c,pure,entrypoint='tlGetDayOfWeek'; \n"
"  STRING DateToString(UNSIGNED4 date, CONST VARSTRING format) : c,pure,entrypoint='tlDateToString'; \n"
"  STRING TimeToString(UNSIGNED3 time, CONST VARSTRING format) : c,pure,entrypoint='tlTimeToString'; \n"
"  STRING SecondsToString(INTEGER8 seconds, CONST VARSTRING format) : c,pure,entrypoint='tlSecondsToString'; \n"
"  UNSIGNED4 AdjustDate(UNSIGNED4 date, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta) : c,pure,entrypoint='tlAdjustDate'; \n"
"  UNSIGNED4 AdjustDateBySeconds(UNSIGNED4 date, INTEGER4 seconds_delta) : c,pure,entrypoint='tlAdjustDateBySeconds'; \n"
"  UNSIGNED4 AdjustTime(UNSIGNED3 time, INTEGER2 hour_delta, INTEGER4 minute_delta, INTEGER4 second_delta) : c,pure,entrypoint='tlAdjustTime'; \n"
"  UNSIGNED4 AdjustTimeBySeconds(UNSIGNED3 time, INTEGER4 seconds_delta) : c,pure,entrypoint='tlAdjustTimeBySeconds'; \n"
"  INTEGER4 AdjustSeconds(INTEGER8 seconds, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta, INTEGER2 hour_delta, INTEGER4 minute_delta, INTEGER4 second_delta) : c,pure,entrypoint='tlAdjustSeconds'; \n"
"  UNSIGNED4 AdjustCalendar(UNSIGNED4 date, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta) : c,pure,entrypoint='tlAdjustCalendar'; \n"
"  BOOLEAN IsLocalDaylightSavingsInEffect() : c,pure,entrypoint='tlIsLocalDaylightSavingsInEffect'; \n"
"  UNSIGNED4 GetLastDayOfMonth(UNSIGNED4 date) : c,pure,entrypoint='tlGetLastDayOfMonth'; \n"
"  TRANSFORM(TMDateRangeRec) DatesForWeek(UNSIGNED4 date) : c,pure,entrypoint='tlDatesForWeek'; \n"
// NOTE - the next 5 are foldable but not pure, meaning it will only be folded if found in a #IF or similar
// This is because you usually want them to be executed at runtime
 "  INTEGER4 LocalTimeZoneOffset() : c,once,entrypoint='tlLocalTimeZoneOffset'; \n"
"  UNSIGNED4 CurrentDate(BOOLEAN in_local_time) : c,once,entrypoint='tlCurrentDate'; \n"
"  UNSIGNED4 CurrentTime(BOOLEAN in_local_time) : c,entrypoint='tlCurrentTime'; \n"
"  INTEGER8 CurrentSeconds(BOOLEAN in_local_time) : c,entrypoint='tlCurrentSeconds'; \n"
"  INTEGER8 CurrentTimestamp(BOOLEAN in_local_time) : c,entrypoint='tlCurrentTimestamp'; \n"
"END;";

TIMELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = TIMELIB_VERSION;
    pb->moduleName = "lib_timelib";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE | PLUGIN_MULTIPLE_VERSIONS;
    pb->description = "TimeLib time manipulation library";
    return true;
}

IPluginContext * parentCtx = NULL;

TIMELIB_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

//------------------------------------------------------------------------------

#ifdef _WIN32
const __int64 _onesec_in100ns = (__int64)10000000;

static __int64 tlFileTimeToInt64(FILETIME f)
{
    __int64     seconds;

    seconds = f.dwHighDateTime;
    seconds <<= 32;
    seconds |= f.dwLowDateTime;

    return seconds;
}

static FILETIME tlInt64ToFileTime(__int64 seconds)
{
    FILETIME    f;

    f.dwHighDateTime = (DWORD)((seconds >> 32) & 0x00000000FFFFFFFF);
    f.dwLowDateTime = (DWORD)(seconds & 0x00000000FFFFFFFF);

    return f;
}

static FILETIME tlFileTimeFromYear(WORD year)
{
    SYSTEMTIME  s;
    FILETIME    f;

    memset(&s, 0, sizeof(s));

    s.wYear = year;
    s.wMonth = 1;
    s.wDayOfWeek = 1;
    s.wDay = 1;

    SystemTimeToFileTime(&s, &f);

    return f;
}

static unsigned int tlYearDayFromSystemTime(const SYSTEMTIME* s)
{
    __int64     seconds;
    FILETIME    f1;
    FILETIME    f2;

    f1 = tlFileTimeFromYear(s->wYear);
    SystemTimeToFileTime(s, &f2);

    seconds = tlFileTimeToInt64(f2) - tlFileTimeToInt64(f1);

    return static_cast<unsigned int>((seconds / _onesec_in100ns) / (60 * 60 * 24));
}

static SYSTEMTIME tlTimeStructToSystemTime(const struct tm* timeInfoPtr)
{
    SYSTEMTIME s;

    s.wYear = timeInfoPtr->tm_year + 1900;
    s.wMonth = timeInfoPtr->tm_mon + 1;
    s.wDayOfWeek = timeInfoPtr->tm_wday;
    s.wDay = timeInfoPtr->tm_mday;
    s.wHour = timeInfoPtr->tm_hour;
    s.wMinute = timeInfoPtr->tm_min;
    s.wSecond = timeInfoPtr->tm_sec;
    s.wMilliseconds = 0;

    return s;
}

static void tlSystemTimeToTimeStruct_r(const SYSTEMTIME* s, struct tm* timeInfoPtr)
{
    memset(timeInfoPtr, 0, sizeof(struct tm));

    timeInfoPtr->tm_year = s->wYear - 1900;
    timeInfoPtr->tm_mon = s->wMonth - 1;
    timeInfoPtr->tm_wday = s->wDayOfWeek;
    timeInfoPtr->tm_mday = s->wDay;
    timeInfoPtr->tm_yday = tlYearDayFromSystemTime(s);
    timeInfoPtr->tm_hour = s->wHour;
    timeInfoPtr->tm_min = s->wMinute;
    timeInfoPtr->tm_sec = s->wSecond;
    timeInfoPtr->tm_isdst = 0;
}

static time_t tlFileTimeToSeconds(const FILETIME* f)
{
    const __int64   offset = I64C(11644473600); // Number of seconds between 1601 and 1970 (Jan 1 of each)

    return static_cast<time_t>((tlFileTimeToInt64(*f) / _onesec_in100ns) - offset);
}

static FILETIME tlSecondsToFileTime(const time_t seconds)
{
    FILETIME    f1970 = tlFileTimeFromYear(1970);
    FILETIME    f;
    __int64     time;

    time = (seconds * _onesec_in100ns) + tlFileTimeToInt64(f1970);

    f = tlInt64ToFileTime(time);

    return f;
}

static __int64 tlLocalTimeZoneDiffIn100nsIntervals()
{
    SYSTEMTIME  systemUTC;
    SYSTEMTIME  systemLocal;
    FILETIME    fileUTC;
    FILETIME    fileLocal;

    GetSystemTime(&systemUTC);
    GetLocalTime(&systemLocal);

    SystemTimeToFileTime(&systemUTC, &fileUTC);
    SystemTimeToFileTime(&systemLocal, &fileLocal);

    return tlFileTimeToInt64(fileLocal) - tlFileTimeToInt64(fileUTC);
}

static void tlBoundaryMod(int* tensPtr, int* unitsPtr, int base)
{
    if (*unitsPtr >= base)
    {
        *tensPtr += *unitsPtr / base;
        *unitsPtr %= base;
    }
    else if (*unitsPtr < 0)
    {
        --*tensPtr;
        *unitsPtr += base;
        if (*unitsPtr < 0)
        {
            *tensPtr -= 1 + (-*unitsPtr) / base;
            *unitsPtr = base - (-*unitsPtr) % base;

            if (*unitsPtr == base)
            {
                *tensPtr += 1;
                *unitsPtr = 0;
            }
        }
    }
}

static void tlNormalizeTimeStruct(struct tm* timeInfoPtr)
{
    // Normalize incoming struct tm
    const int           secondsPerMinute = 60;
    const int           minutesPerHour = 60;
    const int           hoursPerDay = 24;
    const int           daysPerWeek = 7;
    const int           daysPerNYear = 365;
    const int           daysPerLYear = 366;
    const int           yearLengths[2] = { daysPerNYear, daysPerLYear };
    const int           secondsPerHour = secondsPerMinute * minutesPerHour;
    const long          secondsPerDay = secondsPerHour * hoursPerDay;
    const int           monthsPerYear = 12;
    const int           yearBase = 1900;
    const int           monthLengths[2][monthsPerYear] = { { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }, { 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 } };
    int                 leapYearIndex = 0;

    tlBoundaryMod(&timeInfoPtr->tm_min, &timeInfoPtr->tm_sec, secondsPerMinute);
    tlBoundaryMod(&timeInfoPtr->tm_hour, &timeInfoPtr->tm_min, minutesPerHour);
    tlBoundaryMod(&timeInfoPtr->tm_mday, &timeInfoPtr->tm_hour, hoursPerDay);
    tlBoundaryMod(&timeInfoPtr->tm_year, &timeInfoPtr->tm_mon, monthsPerYear);

    leapYearIndex = ((((timeInfoPtr->tm_year + yearBase) % 4) == 0 && ((timeInfoPtr->tm_year + yearBase) % 100) != 0) || ((timeInfoPtr->tm_year + yearBase) % 400) == 0);
    while (timeInfoPtr->tm_mday <= 0)
    {
        --timeInfoPtr->tm_mon;

        if (timeInfoPtr->tm_mon < 0)
        {
            timeInfoPtr->tm_mon = 11;
            --timeInfoPtr->tm_year;
            leapYearIndex = ((((timeInfoPtr->tm_year + yearBase) % 4) == 0 && ((timeInfoPtr->tm_year + yearBase) % 100) != 0) || ((timeInfoPtr->tm_year + yearBase) % 400) == 0);
        }

        timeInfoPtr->tm_mday += monthLengths[leapYearIndex][timeInfoPtr->tm_mon];
    }

    while (timeInfoPtr->tm_mday > monthLengths[leapYearIndex][timeInfoPtr->tm_mon])
    {
        timeInfoPtr->tm_mday -= monthLengths[leapYearIndex][timeInfoPtr->tm_mon];
        ++timeInfoPtr->tm_mon;

        if (timeInfoPtr->tm_mon >= 12)
        {
            timeInfoPtr->tm_mon = 0;
            ++timeInfoPtr->tm_year;
            leapYearIndex = ((((timeInfoPtr->tm_year + yearBase) % 4) == 0 && ((timeInfoPtr->tm_year + yearBase) % 100) != 0) || ((timeInfoPtr->tm_year + yearBase) % 400) == 0);
        }
    }
}

//---------------------------

static void tlWinLocalTime_r(const time_t* clock, struct tm* timeInfoPtr)
{
    SYSTEMTIME  s;
    FILETIME    f;
    __int64     time;

    f = tlSecondsToFileTime(*clock);
    time = tlFileTimeToInt64(f) + tlLocalTimeZoneDiffIn100nsIntervals();
    f = tlInt64ToFileTime(time);

    FileTimeToSystemTime(&f, &s);

    tlSystemTimeToTimeStruct_r(&s, timeInfoPtr);
}

static void tlWinGMTime_r(const time_t* clock, struct tm* timeInfo)
{
    FILETIME    f;
    SYSTEMTIME  s;

    f = tlSecondsToFileTime(*clock);
    FileTimeToSystemTime(&f, &s);
    tlSystemTimeToTimeStruct_r(&s, timeInfo);
}

static time_t tlWinMKTime(struct tm* timeInfoPtr)
{
    SYSTEMTIME  s;
    FILETIME    f;
    time_t      diff;

    // Windows apparently doesn't normalize/fix bogus date values before
    // doing conversions, so we need to normalize them first
    tlNormalizeTimeStruct(timeInfoPtr);

    s = tlTimeStructToSystemTime(timeInfoPtr);
    SystemTimeToFileTime(&s, &f);

    // Reset day of week
    FileTimeToSystemTime(&f, &s);
    timeInfoPtr->tm_wday = s.wDayOfWeek;

    // The above assumes UTC but Linux's mktime() assumes a local
    // time zone, so we need to offset the result into the local time zone
    diff = tlLocalTimeZoneDiffIn100nsIntervals() / _onesec_in100ns;

    return tlFileTimeToSeconds(&f) - diff;
}
#endif

//------------------------------------------------------------------------------

void tlLocalTime_r(const time_t* clock, struct tm* timeInfoPtr)
{
    #ifdef _WIN32
    tlWinLocalTime_r(clock, timeInfoPtr);
    #else
    localtime_r(clock, timeInfoPtr);
    #endif
}

void tlGMTime_r(const time_t* clock, struct tm* timeInfoPtr)
{
    #ifdef _WIN32
    tlWinGMTime_r(clock, timeInfoPtr);
    #else
    gmtime_r(clock, timeInfoPtr);
    #endif
}

time_t tlMKTime(struct tm* timeInfoPtr, bool inLocalTimeZone)
{
    time_t      the_time = 0;

    #ifdef _WIN32
    the_time = tlWinMKTime(timeInfoPtr);

    if (!inLocalTimeZone)
    {
        // Adjust for time zone offset
        the_time += (tlLocalTimeZoneDiffIn100nsIntervals() / _onesec_in100ns);
    }
    #else
    // Get the initial time components; note that mktime assumes local time
    the_time = mktime(timeInfoPtr);

    if (!inLocalTimeZone)
    {
        // Adjust for time zone offset
        the_time += timeInfoPtr->tm_gmtoff;
    }
    #endif

    return the_time;
}

//------------------------------------------------------------------------------

void tlMakeTimeStructFromUTCSeconds(time_t seconds, struct tm* timeInfo)
{
    tlGMTime_r(&seconds, timeInfo);
}

void tlInsertDateIntoTimeStruct(struct tm* timeInfo, unsigned int date)
{
    unsigned int    year = date / 10000;
    unsigned int    month = (date - (year * 10000)) / 100;
    unsigned int    day = date - (year * 10000) - (month * 100);

    timeInfo->tm_year = year - 1900;
    timeInfo->tm_mon = month - 1;
    timeInfo->tm_mday = day;
}

unsigned int tlExtractDateFromTimeStruct(const struct tm* timeInfo)
{
    unsigned int    result = 0;

    result = (timeInfo->tm_year + 1900) * 10000;
    result += (timeInfo->tm_mon + 1) * 100;
    result += timeInfo->tm_mday;

    return result;
}

void tlInsertTimeIntoTimeStruct(struct tm* timeInfo, unsigned int time)
{
    unsigned int    hour = time / 10000;
    unsigned int    minute = (time - (hour * 10000)) / 100;
    unsigned int    second = time - (hour * 10000) - (minute * 100);

    timeInfo->tm_hour = hour;
    timeInfo->tm_min = minute;
    timeInfo->tm_sec = second;
}

unsigned int tlExtractTimeFromTimeStruct(const struct tm* timeInfo)
{
    unsigned int    result = 0;

    result = timeInfo->tm_hour * 10000;
    result += timeInfo->tm_min * 100;
    result += timeInfo->tm_sec;

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API __int64 TIMELIB_CALL tlSecondsFromParts(int year, unsigned int month, unsigned int day, unsigned int hour, unsigned int minute, unsigned int second, bool is_local_time)
{
    struct tm       timeInfo;
    time_t          the_time = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_sec = second;
    timeInfo.tm_min = minute;
    timeInfo.tm_hour = hour;
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    the_time = tlMKTime(&timeInfo, is_local_time);

    return static_cast<__int64>(the_time);
}

//------------------------------------------------------------------------------

TIMELIB_API size32_t TIMELIB_CALL tlSecondsToParts(ARowBuilder& __self, __int64 seconds)
{
    struct tm       timeInfo;

    struct TMParts
    {
        __int32 sec;
        __int32 min;
        __int32 hour;
        __int32 mday;
        __int32 mon;
        __int32 year;
        __int32 wday;
    };

    tlMakeTimeStructFromUTCSeconds(seconds, &timeInfo);

    TMParts* result = reinterpret_cast<TMParts*>(__self.getSelf());

    result->sec = timeInfo.tm_sec;
    result->min = timeInfo.tm_min;
    result->hour = timeInfo.tm_hour;
    result->mday = timeInfo.tm_mday;
    result->mon = timeInfo.tm_mon;
    result->year = timeInfo.tm_year;
    result->wday = timeInfo.tm_wday;

    return static_cast<size32_t>(sizeof(TMParts));
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfYear(short year, unsigned short month, unsigned short day)
{
    unsigned int    dayOfYear = 0;

    #ifdef _WIN32
    SYSTEMTIME  s;

    memset(&s, 0, sizeof(s));

    s.wYear = year;
    s.wMonth = month;
    s.wDay = day;

    dayOfYear = tlYearDayFromSystemTime(&s);
    #else
    struct tm       timeInfo;

    memset(&timeInfo, 0, sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    tlMKTime(&timeInfo);

    dayOfYear = timeInfo.tm_yday;
    #endif

    return dayOfYear;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfWeek(short year, unsigned short month, unsigned short day)
{
    struct tm       timeInfo;

    memset(&timeInfo, 0, sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    tlMKTime(&timeInfo);

    return timeInfo.tm_wday;
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlDateToString(size32_t &__lenResult, char* &__result, unsigned int date, const char* format)
{
    struct tm       timeInfo;
    const size_t    kBufferSize = 256;
    char            buffer[kBufferSize];

    memset(&timeInfo, 0, sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo, date);
    tlMKTime(&timeInfo);

    __lenResult = strftime(buffer, kBufferSize, format, &timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(CTXMALLOC(parentCtx, __lenResult));
        memcpy(__result, buffer, __lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlTimeToString(size32_t &__lenResult, char* &__result, unsigned int time, const char* format)
{
    struct tm       timeInfo;
    const size_t    kBufferSize = 256;
    char            buffer[kBufferSize];

    memset(&timeInfo, 0, sizeof(timeInfo));
    tlInsertTimeIntoTimeStruct(&timeInfo, time);
    tlMKTime(&timeInfo);

    __lenResult = strftime(buffer, kBufferSize, format, &timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(rtlMalloc(__lenResult));
        memcpy(__result, buffer, __lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlSecondsToString(size32_t &__lenResult, char* &__result, __int64 seconds, const char* format)
{
    struct tm       timeInfo;
    time_t          theTime = seconds;
    const size_t    kBufferSize = 256;
    char            buffer[kBufferSize];

    memset(buffer, 0, kBufferSize);

    tlGMTime_r(&theTime, &timeInfo);

    __lenResult = strftime(buffer, kBufferSize, format, &timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(rtlMalloc(__lenResult));
        memcpy(__result, buffer, __lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDate(unsigned int date, short year_delta, int month_delta, int day_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));

    tlInsertDateIntoTimeStruct(&timeInfo, date);

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;
    timeInfo.tm_mday += day_delta;

    tlMKTime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDateBySeconds(unsigned int date, int seconds_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));

    tlInsertDateIntoTimeStruct(&timeInfo, date);
    timeInfo.tm_sec = seconds_delta;

    tlMKTime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTime(unsigned int time, short hour_delta, int minute_delta, int second_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));

    tlInsertTimeIntoTimeStruct(&timeInfo, time);

    timeInfo.tm_hour += hour_delta;
    timeInfo.tm_min += minute_delta;
    timeInfo.tm_sec += second_delta;

    tlMKTime(&timeInfo);

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTimeBySeconds(unsigned int time, int seconds_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));

    tlInsertTimeIntoTimeStruct(&timeInfo, time);
    timeInfo.tm_sec += seconds_delta;

    tlMKTime(&timeInfo);

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API __int64 TIMELIB_CALL tlAdjustSeconds(__int64 seconds, short year_delta, int month_delta, int day_delta, short hour_delta, int minute_delta, int second_delta)
{
    struct tm       timeInfo;
    time_t          theTime = seconds;
    time_t          result = 0;

    tlLocalTime_r(&theTime, &timeInfo);

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;
    timeInfo.tm_mday += day_delta;
    timeInfo.tm_hour += hour_delta;
    timeInfo.tm_min += minute_delta;
    timeInfo.tm_sec += second_delta;

    result = tlMKTime(&timeInfo);

    return static_cast<__int64>(result);
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustCalendar(unsigned int date, short year_delta, int month_delta, int day_delta)
{
    struct tm       timeInfo;
    unsigned int    year = date / 10000;
    unsigned int    month = (date - (year * 10000)) / 100;
    unsigned int    day = date - (year * 10000) - (month * 100);
    int             expectedMonthVal = month + month_delta - 1;
    time_t          seconds;
    unsigned int    result = 0;

    // Normalize the expected month value
    if (expectedMonthVal >= 0)
    {
        expectedMonthVal = expectedMonthVal % 12;
    }
    else
    {
        expectedMonthVal = 12 - (abs(expectedMonthVal) % 12);
    }

    memset(&timeInfo, 0, sizeof(timeInfo));

    timeInfo.tm_year = year - 1900;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_mday = day;

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;

    seconds = tlMKTime(&timeInfo);

    if (timeInfo.tm_mon != expectedMonthVal)
    {
        // If the returned month doesn't match the expected month, we need to
        // go back to the last day of the previous month
        timeInfo.tm_mday = 0;
        tlMKTime(&timeInfo);
    }

    if (day_delta != 0)
    {
        // Now apply the day delta
        timeInfo.tm_mday += day_delta;
        tlMKTime(&timeInfo);
    }

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API bool TIMELIB_CALL tlIsLocalDaylightSavingsInEffect()
{
    struct tm       timeInfo;
    time_t          theTime = time(NULL);

    tlLocalTime_r(&theTime, &timeInfo);

    return (timeInfo.tm_isdst == 1);
}

//------------------------------------------------------------------------------

TIMELIB_API int TIMELIB_CALL tlLocalTimeZoneOffset()
{
    int     offset = 0;

    #ifdef _WIN32
    offset = static_cast<int>(tlLocalTimeZoneDiffIn100nsIntervals() / _onesec_in100ns);
    #else
    struct tm       timeInfo;
    time_t          theTime = time(NULL);

    tlLocalTime_r(&theTime, &timeInfo);

    offset = timeInfo.tm_gmtoff;
    #endif

    return offset;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlCurrentDate(bool in_local_time)
{
    struct tm       timeInfo;
    time_t          theTime = time(NULL);
    unsigned int    result = 0;

    // Create time parts differently depending on whether you need
    // UTC or local time
    if (in_local_time)
    {
        tlLocalTime_r(&theTime, &timeInfo);
    }
    else
    {
        tlGMTime_r(&theTime, &timeInfo);
    }

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlCurrentTime(bool in_local_time)
{
    struct tm       timeInfo;
    time_t          theTime = time(NULL);
    unsigned int    result = 0;

    // Create time parts differently depending on whether you need
    // UTC or local time
    if (in_local_time)
    {
        tlLocalTime_r(&theTime, &timeInfo);
    }
    else
    {
        tlGMTime_r(&theTime, &timeInfo);
    }

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API __int64 TIMELIB_CALL tlCurrentSeconds(bool in_local_time)
{
    time_t    result = time(NULL);

    if (in_local_time)
    {
        result += tlLocalTimeZoneOffset();
    }

    return static_cast<__int64>(result);
}

//------------------------------------------------------------------------------

TIMELIB_API __int64 TIMELIB_CALL tlCurrentTimestamp(bool in_local_time)
{
    __int64     result = 0;

    #ifdef _WIN32
    struct _timeb   now;

    _ftime_s(&now);

    result = (now.time * I64C(1000000)) + (now.millitm * 1000);
    #else
    struct timeval  tv;

    if (gettimeofday(&tv, NULL) == 0)
    {
        result = (tv.tv_sec * I64C(1000000)) + tv.tv_usec;
    }
    #endif

    if (in_local_time)
    {
        result += (static_cast<__int64>(tlLocalTimeZoneOffset()) * I64C(1000000));
    }

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetLastDayOfMonth(unsigned int date)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo, 0, sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo, date);

    // Call mktime once to fix up any bogus data
    tlMKTime(&timeInfo);

    // Adjust and call again
    timeInfo.tm_mon += 1;
    timeInfo.tm_mday = 0;
    tlMKTime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API size32_t TIMELIB_CALL tlDatesForWeek(ARowBuilder& __self, unsigned int date)
{
    struct tm       timeInfo;

    struct TMDateRange
    {
        unsigned int    startDate;
        unsigned int    endDate;
    };

    TMDateRange* result = reinterpret_cast<TMDateRange*>(__self.getSelf());

    memset(&timeInfo, 0, sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo, date);

    // Call mktime once to fix up any bogus data
    tlMKTime(&timeInfo);

    // Adjust and call again
    timeInfo.tm_mday -= timeInfo.tm_wday;
    tlMKTime(&timeInfo);

    result->startDate = tlExtractDateFromTimeStruct(&timeInfo);

    // Adjust to the beginning of the week
    timeInfo.tm_mday += 6;
    tlMKTime(&timeInfo);

    result->endDate = tlExtractDateFromTimeStruct(&timeInfo);

    return static_cast<size32_t>(sizeof(TMDateRange));
}
