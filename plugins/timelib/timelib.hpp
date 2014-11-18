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

#ifndef TIMELIB_INCL
#define TIMELIB_INCL

#ifdef _WIN32
#define TIMELIB_CALL _cdecl
#ifdef TIMELIB_EXPORTS
#define TIMELIB_API __declspec(dllexport)
#else
#define TIMELIB_API __declspec(dllimport)
#endif
#else
#define TIMELIB_CALL
#define TIMELIB_API
#endif

#include <time.h>

#include "platform.h"
#include "hqlplugins.hpp"

extern "C" {

#ifdef TIMELIB_EXPORTS
TIMELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
TIMELIB_API void setPluginContext(IPluginContext * _ctx);
#endif

void tlMakeTimeStructFromUTCSeconds(time_t seconds, struct tm* timeInfo);
void tlInsertDateIntoTimeStruct(struct tm* timeInfo, unsigned int date);
unsigned int tlExtractDateFromTimeStruct(struct tm* timeInfo);
void tlInsertTimeIntoTimeStruct(struct tm* timeInfo, unsigned int time);
unsigned int tlExtractTimeFromTimeStruct(struct tm* timeInfo);

TIMELIB_API time_t TIMELIB_CALL tlSecondsFromParts(int year, unsigned int month, unsigned int day, unsigned int hour, unsigned int minute, unsigned int second, bool is_local_time = false);
TIMELIB_API void TIMELIB_CALL tlSecondsToParts(size32_t &__lenResult, void* &__result, time_t seconds);
TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfYear(short year, unsigned short month, unsigned short day);
TIMELIB_API unsigned int TIMELIB_CALL tlGetDayOfWeek(short year, unsigned short month, unsigned short day);
TIMELIB_API void TIMELIB_CALL tlDateToString(size32_t &__lenResult, char* &__result, unsigned int date, const char* format);
TIMELIB_API void TIMELIB_CALL tlTimeToString(size32_t &__lenResult, char* &__result, unsigned int time, const char* format);
TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDate(unsigned int date, short year_delta, int month_delta, int day_delta);
TIMELIB_API void TIMELIB_CALL tlSecondsToString(size32_t &__lenResult, char* &__result, int seconds, const char* format);
TIMELIB_API unsigned int TIMELIB_CALL tlAdjustDateBySeconds(unsigned int date, int seconds_delta);
TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTime(unsigned int time, short hour_delta, int minute_delta, int second_delta);
TIMELIB_API unsigned int TIMELIB_CALL tlAdjustTimeBySeconds(unsigned int time, int seconds_delta);
TIMELIB_API time_t TIMELIB_CALL tlAdjustSeconds(time_t seconds, short year_delta, int month_delta, int day_delta, short hour_delta, int minute_delta, int second_delta);
TIMELIB_API unsigned int TIMELIB_CALL tlAdjustCalendar(unsigned int date, short year_delta, int month_delta, int day_delta);
TIMELIB_API bool TIMELIB_CALL tlIsLocalDaylightSavingsInEffect();
TIMELIB_API int TIMELIB_CALL tlLocalTimeZoneOffset();
TIMELIB_API unsigned int TIMELIB_CALL tlCurrentDate(bool in_local_time);
TIMELIB_API unsigned int TIMELIB_CALL tlCurrentTime(bool in_local_time);
TIMELIB_API time_t TIMELIB_CALL tlCurrentSeconds(bool in_local_time);
TIMELIB_API double TIMELIB_CALL tlCurrentTimestamp(bool in_local_time);
TIMELIB_API unsigned int TIMELIB_CALL tlGetLastDayOfMonth(unsigned int date);
TIMELIB_API void TIMELIB_CALL tlDatesForWeek(size32_t &__lenResult, void* &__result, unsigned int date);

}
#endif
