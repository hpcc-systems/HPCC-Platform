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

#option ('globalFold', false);
// getDayNumOfWeek
/* returns the day number of week from parameter list that is
the day, month, and year numbers. */
/* days are numbered Sunday = 1, Monday = 2, ... */
integer adjMonth(integer mo) := if(mo <= 2, 10 + mo, mo - 2);
integer adjYear(integer mo, integer yr) := if(mo <= 2, yr - 1, yr);
integer getYearCorr(integer mo, integer yr) := (adjYear(mo, yr) % 100) +
   ((adjYear(mo, yr) % 100)/ 4) +
   ((adjYear(mo, yr)/100) / 4) +
   5 * (adjYear(mo, yr)/ 100);
integer getDayNumWk(integer mo, integer dy, integer yr) := (integer)(dy +
   ((integer)(26 * adjMonth(mo) - 2) / 10) +
   getYearCorr(mo, yr)) % 7 + 1;
export getDayNumOfWeek(integer m, integer d, integer y) :=
   getDayNumWk(m, d, y);

// datestr is in the format 'YYYYMMDD'
export getDayNumOfWeekStr(String8 datestr) :=
  if(datestr='', 8,
         getDayNumOfWeek((integer2)(datestr[5..6]),
                  (integer2)(datestr[7..8]),
                  (integer2)(datestr[1..4])));

getDayNumOfWeekStr('20010709');
//output(package, {p_date_1, getDayNumOfWeekStr(p_date_1)}), I get
