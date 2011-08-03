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

person := dataset('person', { unsigned8 person_id, string1 per_sex, unsigned per_ssn, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);
x := record
v1 := count(GROUP, person.per_ssn > 50);
v2 := count(GROUP, person.per_ssn > 60);
v3 := count(GROUP, person.per_ssn > 70);
v4 := count(GROUP, person.per_ssn > 80);
end;

y := table(person, x) : stored('hi');

//output(y);
y[1].v1;
y[1].v2;
y[1].v3;
y[1].v4;
