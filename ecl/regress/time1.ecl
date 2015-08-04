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

export string1 getA() := BEGINC++
    __result[0] = 'A';
ENDC++;

export getTime() := function

// Function to get time in HHMMSS format
// Courtesy : Nigel/Gavin

string6 getTime() := BEGINC++
#ifdef _WIN32
#include <time.h>
#endif
#body
    // Declarations
    struct tm localt; // localtime in 'tm' structure
    time_t timeinsecs;  // variable to store time in secs
    char temp[7];

    // Get time in sec since Epoch
    time(&timeinsecs);
    // Convert to local time
#ifdef _WIN32
    localtime_s(&localt,&timeinsecs);
    strftime(temp, sizeof(temp), "%H%M%S", &localt); // Formats the localtime to HHMMSS
#else
    localtime_r(&timeinsecs,&localt);
    strftime(temp, sizeof(temp), "%H%M%S", &localt); // Formats the localtime to HHMMSS
#endif
    memcpy(__result, &temp, sizeof(temp)-1);
ENDC++;

return getTime();

end;

export getTimeDate() := function

// Function to get time in HHMMSS format
// Courtesy : Nigel/Gavin

string17 getTimeDate() := BEGINC++
#ifdef _WIN32
#include <time.h>
#endif
#body
    // Declarations
    struct tm localt; // localtime in 'tm' structure
    time_t timeinsecs;  // variable to store time in secs
    char temp[18];

    // Get time in sec since Epoch
    time(&timeinsecs);
    // Convert to local time
#ifdef _WIN32
    localtime_s(&localt,&timeinsecs);
    strftime(temp, sizeof(temp), "%Y-%m-%d%H%M%S%w", &localt); // Formats the localtime to YYYY-MM-DDHHMMSSW where W is the weekday
    if (temp[16]=='0')      // convert %w to %u
        temp[16]='7';
#else
    localtime_r(&timeinsecs,&localt);
    strftime(temp, sizeof(temp), "%F%H%M%S%u", &localt); // Formats the localtime to YYYY-MM-DDHHMMSSW where W is the weekday
#endif
    memcpy(__result, &temp, 17);
ENDC++;

return getTimeDate();
end;

output(getTime());
output(getTimeDate());


