/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

import std;  

// Test the abbreviated weekday and full weekday name format 
// for every day a week
ASSERT(std.date.datetostring(20140825, '%a %A') = 'Mon Monday', CONST);
ASSERT(std.date.datetostring(20140826, '%a %A') = 'Tue Tuesday', CONST);
ASSERT(std.date.datetostring(20140827, '%a %A') = 'Wed Wednesday', CONST);
ASSERT(std.date.datetostring(20140828, '%a %A') = 'Thu Thursday', CONST);
ASSERT(std.date.datetostring(20140829, '%a %A') = 'Fri Friday', CONST);
ASSERT(std.date.datetostring(20140830, '%a %A') = 'Sat Saturday', CONST);
ASSERT(std.date.datetostring(20140824, '%a %A') = 'Sun Sunday', CONST);

// Test the abbreviated and full Month name 
ASSERT(std.date.datetostring(20140824, '%b %B') = 'Aug August', CONST);

// Test the century number (year/100) as a 2-digit integer 
ASSERT(std.date.datetostring(20140824, '%C') = '20', CONST);

// Test the day of the month as a decimal number (range 01 to 31). 
ASSERT(std.date.datetostring(20140824, '%d') = '24', CONST);

// Test the MM/DD/YY format. 
ASSERT(std.date.datetostring(20140824, '%D') = '08/24/14', CONST);

// Test the ISO 8601 week-based year
ASSERT(std.date.datetostring(20140824, '%G') = '2014', CONST);

// Test the ISO 8601 week-based year without century
ASSERT(std.date.datetostring(20140824, '%g') = '14', CONST);

// Test the %Y-%m-%d (the ISO 8601 date format)
ASSERT(std.date.datetostring(20140824, '%Y-%m-%d') = '2014-08-24', CONST);

// Test the day of the year as a decimal number (range 001 to 366)
ASSERT(std.date.datetostring(20140824, '%j') = '236', CONST);

// Test the day of the week as a decimal, range 1 to 7, Monday being  1.
ASSERT(std.date.datetostring(20140824, '%u') = '7', CONST);

// Test the day of the week as a decimal, range 0 to 6, Sunday being  0.
ASSERT(std.date.datetostring(20140824, '%w') = '0', CONST);

// Test the  week  number of the current year as a decimal number, range
// 00 to 53, starting with the first Sunday as  the  first  day  of week 01.
ASSERT(std.date.datetostring(20140824, '%U') = '34', CONST);

// Test the  week  number of the current year as a decimal number, range
// 00 to 53, starting with the first Monday as  the  first  day  of week 01
ASSERT(std.date.datetostring(20140824, '%W') = '33', CONST);


// The time formatting works well, but the result depends on 
// the daylight saving. So not tested yet.
