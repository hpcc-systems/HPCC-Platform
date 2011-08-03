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
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);
groupedNamesTable := group(namesTable, forename);
deduped := dedup(groupedNamesTable, (string10)age,LEFT.age-RIGHT.age>10,ALL);
deduped2 := dedup(deduped, (integer4)surname);
deduped3 := dedup(deduped, LEFT.age-RIGHT.age>8);

input3 := group(deduped3);

deduped4 := dedup(input3,age,surname,ALL);

deduped5 := dedup(deduped4,age,LEFT.surname = RIGHT.surname);

output(deduped5,,'out.d00');
