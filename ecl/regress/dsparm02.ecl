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

// test: mapping more than one field
person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, string40 per_last_name }, thor);

r := record
  string15 name;
  string25 name2;
end;

// both should work
//Dataset filter_ds(virtual dataset(r) d) := d(d.name = 'fred', d.name2='fred1');
Dataset filter_ds(virtual dataset(r) d) := d(name = 'fred', name2='fred1');

count(filter_ds(person{name:=per_first_name,name2:=per_last_name}));
