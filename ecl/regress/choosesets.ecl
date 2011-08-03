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

#option ('optimizeDiskSource', 1);

namesRecord :=
            RECORD
string20        surname;
string      forename;
integer2        age := 25;

            END;

namesTable := dataset('x',namesRecord,FLAT);

ds1 := choosen(namesTable(age = 10), 10) + choosen(namesTable(age != 10), 20);
output(ds1);

ds2 := choosesets(namesTable, age = 10=>10, 20, exclusive);
output(ds2);
