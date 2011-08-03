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

#option ('countIndex', true);

d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);
d2 := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\home\\person.name_first.key');

string15 searchName := '' : stored('searchName');
string15 searchName2 := '' : stored('searchName2');
a1 := i(searchName <> '', name = searchName);

output(a1);
count(i(searchName2 <> '', name = searchName2));



x := join(d, i, searchName<>'' and right.name in [searchName, left.name]);

output(x);


output(d2(searchName2 <> '' and keyed(searchName<>'' and name <> searchName)));
