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
integer2        age := 25;
integer2        age2 := 25;
real8           salary := 0;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);

x1 := namesTable(abs(age-age2) < 20, abs(salary/age) < 1000.00);
output(x1,,'out.d00');

output(-123456D);                   // = -123456
output(ABS(-123456D));              // = 1123456
output(ABS(-1));                    // = 1
output(ABS((unsigned8)-1));         // = some very long number
output(ABS(1.0102));                    // = 1
output(ABS(-1.0102));                   // = 1
