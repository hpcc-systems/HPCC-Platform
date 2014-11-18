/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifdef _WINDOWS
    #include <sys/timeb.h>
#endif

#include "timelib.hpp"

static const char * compatibleVersions[] = {
    NULL };

#define TIMELIB_VERSION "TIMELIB 1.0.0"

static const char * EclDefinition =
"export TMPartsRec := RECORD \n"
"  UNSIGNED4 v; \n"
"END;"
"export TimeLib := SERVICE\n"
"  integer4 SecondsFromParts(integer2 year, unsigned1 month, unsigned1 day, unsigned1 hour, unsigned1 minute, unsigned1 second, boolean is_local_time) : c,pure,entrypoint='tlSecondsFromParts'; \n"
"  DATASET(TMPartsRec) SecondsToParts(INTEGER8 seconds) : c,pure,entrypoint='tlSecondsToParts'; \n"
"  UNSIGNED2 GetDayOfYear(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) : c,pure,entrypoint='tlGetDayOfYear'; \n"
"  UNSIGNED1 GetDayOfWeek(INTEGER2 year, UNSIGNED1 month, UNSIGNED1 day) : c,pure,entrypoint='tlGetDayOfWeek'; \n"
"  STRING DateToString(UNSIGNED4 date, VARSTRING format) : c,pure,entrypoint='tlDateToString'; \n"
"  STRING TimeToString(UNSIGNED3 time, VARSTRING format) : c,pure,entrypoint='tlTimeToString'; \n"
"  STRING SecondsToString(INTEGER4 seconds, VARSTRING format) : c,pure,entrypoint='tlSecondsToString'; \n"
"  UNSIGNED4 AdjustDate(UNSIGNED4 date, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta) : c,pure,entrypoint='tlAdjustDate'; \n"
"  UNSIGNED4 AdjustDateBySeconds(UNSIGNED4 date, INTEGER4 seconds_delta) : c,pure,entrypoint='tlAdjustDateBySeconds'; \n"
"  UNSIGNED3 AdjustTime(UNSIGNED3 time, INTEGER2 hour_delta, INTEGER4 minute_delta, INTEGER4 second_delta) : c,pure,entrypoint='tlAdjustTime'; \n"
"  UNSIGNED3 AdjustTimeBySeconds(UNSIGNED3 time, INTEGER4 seconds_delta) : c,pure,entrypoint='tlAdjustTimeBySeconds'; \n"
"  INTEGER4 AdjustSeconds(INTEGER4 seconds, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta, INTEGER2 hour_delta, INTEGER4 minute_delta, INTEGER4 second_delta) : c,pure,entrypoint='tlAdjustSeconds'; \n"
"  UNSIGNED4 AdjustCalendar(UNSIGNED4 date, INTEGER2 year_delta, INTEGER4 month_delta, INTEGER4 day_delta) : c,pure,entrypoint='tlAdjustCalendar'; \n"
"  BOOLEAN IsLocalDaylightSavingsInEffect() : c,pure,entrypoint='tlIsLocalDaylightSavingsInEffect'; \n"
"  INTEGER4 LocalTimeZoneOffset() : c,pure,entrypoint='tlLocalTimeZoneOffset'; \n"
"  UNSIGNED4 CurrentDate(BOOLEAN in_local_time) : c,pure,entrypoint='tlCurrentDate'; \n"
"  UNSIGNED4 CurrentTime(BOOLEAN in_local_time) : c,pure,entrypoint='tlCurrentTime'; \n"
"  INTEGER4 CurrentSeconds(BOOLEAN in_local_time) : c,pure,entrypoint='tlCurrentSeconds'; \n"
"  REAL8 CurrentTimestamp(BOOLEAN in_local_time) : c,pure,entrypoint='tlCurrentTimestamp'; \n"
"  UNSIGNED4 GetLastDayOfMonth(UNSIGNED4 date) : c,pure,entrypoint='tlGetLastDayOfMonth'; \n"
"  DATASET(TMPartsRec) DatesForWeek(UNSIGNED4 date) : c,pure,entrypoint='tlDatesForWeek'; \n"
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

void tlMakeTimeStructFromUTCSeconds(time_t seconds, struct tm* timeInfo)
{
    #ifdef _WINDOWS
        // gmtime is thread-safe under Windows
        memcpy(timeInfo,gmtime(&seconds),sizeof(&timeInfo));
    #else
        gmtime_r(&seconds,timeInfo);
    #endif
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

unsigned int tlExtractDateFromTimeStruct(struct tm* timeInfo)
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

unsigned int tlExtractTimeFromTimeStruct(struct tm* timeInfo)
{
    unsigned int    result = 0;

    result = timeInfo->tm_hour * 10000;
    result += timeInfo->tm_min * 100;
    result += timeInfo->tm_sec;

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API time_t TIMELIB_CALL tlSecondsFromParts(int year, unsigned int month, unsigned int day, unsigned int hour, unsigned int minute, unsigned int second, bool is_local_time)
{
    struct tm       timeInfo;
    time_t          the_time = 0;

    memset(&timeInfo,0,sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_sec = second;
    timeInfo.tm_min = minute;
    timeInfo.tm_hour = hour;
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    // Get the initial time components; note that mktime assumes local time
    the_time = mktime(&timeInfo);

    if (!is_local_time)
    {
        // Adjust for time zone offset
        the_time += timeInfo.tm_gmtoff;
    }

    return the_time;
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlSecondsToParts(size32_t &__lenResult, void* &__result, time_t seconds)
{
    struct tm       timeInfo;

    tlMakeTimeStructFromUTCSeconds(seconds, &timeInfo);

    __lenResult = sizeof(unsigned int) * 7;
    __result = CTXMALLOC(parentCtx, __lenResult);

    // Actually write the output values one at a time
    unsigned int*   out = reinterpret_cast<unsigned int*>(__result);

    out[0] = timeInfo.tm_sec;
    out[1] = timeInfo.tm_min;
    out[2] = timeInfo.tm_hour;
    out[3] = timeInfo.tm_mday;
    out[4] = timeInfo.tm_mon;
    out[5] = timeInfo.tm_year;
    out[6] = timeInfo.tm_wday;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfYear(short year, unsigned short month, unsigned short day)
{
    struct tm       timeInfo;

    memset(&timeInfo,0,sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    mktime(&timeInfo);

    return timeInfo.tm_yday;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfWeek(short year, unsigned short month, unsigned short day)
{
    struct tm       timeInfo;

    memset(&timeInfo,0,sizeof(timeInfo));

    // Push each time part value into the tm struct
    timeInfo.tm_mday = day;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_year = year - 1900;

    mktime(&timeInfo);

    return timeInfo.tm_wday;
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlDateToString(size32_t &__lenResult, char* &__result, unsigned int date, const char* format)
{
    struct tm       timeInfo;
    size_t          kBufferSize = 256;
    char            buffer[kBufferSize];

    memset(&timeInfo,0,sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo,date);

    __lenResult = strftime(buffer,kBufferSize,format,&timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(CTXMALLOC(parentCtx, __lenResult));
        memcpy(__result,buffer,__lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlTimeToString(size32_t &__lenResult, char* &__result, unsigned int time, const char* format)
{
    struct tm       timeInfo;
    size_t          kBufferSize = 256;
    char            buffer[kBufferSize];

    memset(&timeInfo,0,sizeof(timeInfo));
    tlInsertTimeIntoTimeStruct(&timeInfo,time);

    __lenResult = strftime(buffer,kBufferSize,format,&timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(rtlMalloc(__lenResult));
        memcpy(__result,buffer,__lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlSecondsToString(size32_t &__lenResult, char* &__result, int seconds, const char* format)
{
    struct tm   timeInfo;
    time_t      theTime = seconds;
    size_t      kBufferSize = 256;
    char        buffer[kBufferSize];

    memset(buffer,kBufferSize,0);

    #ifdef _WINDOWS
        // gmtime is thread-safe under Windows
        memcpy(&timeInfo,gmtime(&theTime),sizeof(timeInfo));
    #else
        gmtime_r(&theTime,&timeInfo);
    #endif

    __lenResult = strftime(buffer,kBufferSize,format,&timeInfo);
    __result = NULL;

    if (__lenResult > 0)
    {
        __result = reinterpret_cast<char*>(rtlMalloc(__lenResult));
        memcpy(__result,buffer,__lenResult);
    }
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDate(unsigned int date, short year_delta, int month_delta, int day_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo,0,sizeof(timeInfo));

    tlInsertDateIntoTimeStruct(&timeInfo,date);

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;
    timeInfo.tm_mday += day_delta;

    mktime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDateBySeconds(unsigned int date, int seconds_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo,0,sizeof(timeInfo));

    tlInsertDateIntoTimeStruct(&timeInfo,date);
    timeInfo.tm_sec = seconds_delta;

    mktime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTime(unsigned int time, short hour_delta, int minute_delta, int second_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo,0,sizeof(timeInfo));

    tlInsertTimeIntoTimeStruct(&timeInfo,time);

    timeInfo.tm_hour += hour_delta;
    timeInfo.tm_min += minute_delta;
    timeInfo.tm_sec += second_delta;

    mktime(&timeInfo);

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTimeBySeconds(unsigned int time, int seconds_delta)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo,0,sizeof(timeInfo));

    tlInsertTimeIntoTimeStruct(&timeInfo,time);
    timeInfo.tm_sec += seconds_delta;

    mktime(&timeInfo);

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API time_t TIMELIB_CALL tlAdjustSeconds(time_t seconds, short year_delta, int month_delta, int day_delta, short hour_delta, int minute_delta, int second_delta)
{
    struct tm       timeInfo;
    time_t          result = 0;

    #ifdef _WINDOWS
        // localtime is thread-safe under Windows
        memcpy(&timeInfo,localtime(&seconds),sizeof(timeInfo));
    #else
        localtime_r(&seconds,&timeInfo);
    #endif

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;
    timeInfo.tm_mday += day_delta;
    timeInfo.tm_hour += hour_delta;
    timeInfo.tm_min += minute_delta;
    timeInfo.tm_sec += second_delta;

    result = mktime(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlAdjustCalendar(unsigned int date, short year_delta, int month_delta, int day_delta)
{
    struct tm       timeInfo;
    unsigned int    year = date / 10000;
    unsigned int    month = (date - (year * 10000)) / 100;
    unsigned int    day = date - (year * 10000) - (month * 100);
    int             expectedMonthVal = month + month_delta - 1;
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

    memset(&timeInfo,0,sizeof(timeInfo));

    timeInfo.tm_year = year - 1900;
    timeInfo.tm_mon = month - 1;
    timeInfo.tm_mday = day;

    timeInfo.tm_year += year_delta;
    timeInfo.tm_mon += month_delta;

    mktime(&timeInfo);

    if (timeInfo.tm_mon != expectedMonthVal)
    {
        // If the returned month doesn't match the expected month, we need to
        // go back to the last day of the previous month
        timeInfo.tm_mday = 0;
        mktime(&timeInfo);
    }

    // Now apply the day delta
    timeInfo.tm_mday += day_delta;
    mktime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API bool TIMELIB_CALL tlIsLocalDaylightSavingsInEffect()
{
    struct tm       timeInfo;
    time_t          theTime = time(NULL);

    #ifdef _WINDOWS
        // localtime is thread-safe under Windows
        memcpy(&timeInfo,localtime(&theTime),sizeof(timeInfo));
    #else
        localtime_r(&theTime,&timeInfo);
    #endif

    return (timeInfo.tm_isdst == 1);
}

//------------------------------------------------------------------------------

TIMELIB_API int TIMELIB_CALL tlLocalTimeZoneOffset()
{
    struct tm       timeInfo;
    time_t          theTime = time(NULL);

    #ifdef _WINDOWS
        // localtime is thread-safe under Windows
        memcpy(&timeInfo,localtime(&theTime),sizeof(timeInfo));
    #else
        localtime_r(&theTime,&timeInfo);
    #endif

    return timeInfo.tm_gmtoff;
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
        #ifdef _WINDOWS
            // localtime is thread-safe under Windows
            memcpy(&timeInfo,localtime(&theTime),sizeof(timeInfo));
        #else
            localtime_r(&theTime,&timeInfo);
        #endif
    }
    else
    {
        #ifdef _WINDOWS
            // gmtime is thread-safe under Windows
            memcpy(&timeInfo,gmtime(&theTime),sizeof(timeInfo));
        #else
            gmtime_r(&theTime,&timeInfo);
        #endif
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
        #ifdef _WINDOWS
            // localtime is thread-safe under Windows
            memcpy(&timeInfo,localtime(&theTime),sizeof(timeInfo));
        #else
            localtime_r(&theTime,&timeInfo);
        #endif
    }
    else
    {
        #ifdef _WINDOWS
            // gmtime is thread-safe under Windows
            memcpy(&timeInfo,gmtime(&theTime),sizeof(timeInfo));
        #else
            gmtime_r(&theTime,&timeInfo);
        #endif
    }

    result = tlExtractTimeFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API time_t TIMELIB_CALL tlCurrentSeconds(bool in_local_time)
{
    time_t    result = time(NULL);

    if (in_local_time)
    {
        result += tlLocalTimeZoneOffset();
    }

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API double TIMELIB_CALL tlCurrentTimestamp(bool in_local_time)
{
    double          result = 0.0;

    #ifdef _WINDOWS
        struct _timeb   now;

        _ftime(&now);

        result = now.time + (now.millitm / 1000.0);
    #else
        struct timeval  tv;

        if (gettimeofday(&tv,NULL) == 0)
        {
            result = tv.tv_sec + (tv.tv_usec / 1000000.0);
        }
    #endif

    if (in_local_time)
    {
        result += tlLocalTimeZoneOffset();
    }

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API unsigned int TIMELIB_CALL tlGetLastDayOfMonth(unsigned int date)
{
    struct tm       timeInfo;
    unsigned int    result = 0;

    memset(&timeInfo,0,sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo,date);

    // Call mktime once to fix up any bogus data
    mktime(&timeInfo);

    // Adjust and call again
    timeInfo.tm_mon += 1;
    timeInfo.tm_mday = 0;
    mktime(&timeInfo);

    result = tlExtractDateFromTimeStruct(&timeInfo);

    return result;
}

//------------------------------------------------------------------------------

TIMELIB_API void TIMELIB_CALL tlDatesForWeek(size32_t &__lenResult, void* &__result, unsigned int date)
{
    struct tm       timeInfo;
    unsigned int    weekStartResult = 0;
    unsigned int    weekEndResult = 0;

    memset(&timeInfo,0,sizeof(timeInfo));
    tlInsertDateIntoTimeStruct(&timeInfo,date);

    // Call mktime once to fix up any bogus data
    mktime(&timeInfo);

    // Adjust and call again
    timeInfo.tm_mday -= timeInfo.tm_wday;
    mktime(&timeInfo);

    weekStartResult = tlExtractDateFromTimeStruct(&timeInfo);

    // Adjust to the beginning of the week
    timeInfo.tm_mday += 6;
    mktime(&timeInfo);

    weekEndResult = tlExtractDateFromTimeStruct(&timeInfo);

    __lenResult = sizeof(unsigned int) * 2;
    __result = CTXMALLOC(parentCtx, __lenResult);

    unsigned int*   out = reinterpret_cast<unsigned int*>(__result);

    out[0] = weekStartResult;
    out[1] = weekEndResult;
}
