/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


