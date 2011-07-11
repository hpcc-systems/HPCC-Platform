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

ds1 := DATASET('aaa',{STRING1 f;},FLAT);
person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

// choose
a := choose(1,count(person), (integer)person.per_first_name);

// sort
r := RECORD
  f1 := person.per_first_name;
end;

tbl := table(person,r);

b3 := sort(tbl, ds1.f);  // get assertion scope error



count(b3); // helps if we use it....
