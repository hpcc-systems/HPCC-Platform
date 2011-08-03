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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

x1 := sort(namesTable(age != 99), age);

x1b := sort(choosen(dedup(x1,forename), 100, 99), surname);

x2 := if (count(x1)>10, x1b(age<10), x1b(age<20));

x3 := if (count(x1)>20, x1b(age<10),x1b(age<20));

x4 := if (count(x1)>30, x1b(age<10),x1b(age<20));

x5 := if (count(x1)>40, x1b(age<10),x1b(age<20));

x6 := if (count(x1)>50, x1b(age<10),x1b(age<20));

output (x2 +x3 + x4 + x5 + x6);
