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

aa := dataset('aaa',{INTEGER a1; }, FLAT);
person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

record1 := record
    integer i1;
    integer i2;
end;

record1 tranx0(integer i, integer j) := transform
    self.i1 := i;
    self.i2 := j + person.person_id;
end;

mytable := dataset([{1,2},{3,4}], record1);

normalizedStuff := normalize(mytable, LEFT.i1, tranx0(left.i2, COUNTER));

output(normalizedStuff);

