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

namesRecord := 
            RECORD
varstring20     surname;
string10        forename;
integer2        age := 25;
integer2        score;
integer8        holepos;
            END;

ds1 := dataset('names', namesRecord, THOR);
ds2 := dataset('names', namesRecord, THOR);
ds3 := dataset('names3', namesRecord, THOR);
file1 := ds1 + ds2 + ds3;

cnt1 := count(file1);

myReport := RECORD
   score := file1.score;
   cnt := count(group);
   prcnt := count(group) * 100 / cnt1;
end;

x := table(file1, myReport);

output(x);

