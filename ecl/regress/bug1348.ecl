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
