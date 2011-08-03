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
integer2        f1;
integer2        f2;
integer2        f3;
integer2        f4;
integer2        f5;
integer2        f6;
            END;

namesTable := dataset('x',namesRecord,FLAT);

deduped := dedup(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));

output(deduped,,'out.d00');

groupedNames := group(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));
output(table(groupednames,{count(group)}),,'out.d01');

sortedNames := sort(namesTable, ((string)f1)+((string)f2)+((string)f3)+((string)f4)+((string)f5)+((string)f6));
output(sortedNames,,'out.d02');
