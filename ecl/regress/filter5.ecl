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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_last_name, unsigned8 xpos }, thor);

filtered1 := person(xpos < 1000);

filter2 := filtered1(per_last_name = 'Hawthorn');
filter3 := filtered1(per_last_name = 'Drimbad');

output(filter2,,'out2.d00');
output(filter3,,'out3.d00');


namesRecord :=
            RECORD
string20        per_last_name;
string10        forename;
integer2        holepos;
            END;

namesTable := dataset('x',namesRecord,FLAT);

afiltered1 := namesTable(holepos < 1000);

afilter2 := afiltered1(per_last_name = 'Hawthorn');
afilter3 := afiltered1(per_last_name = 'Drimbad');

output(afilter2,,'aout2.d00');
output(afilter3,,'aout3.d00');

