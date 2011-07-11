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
unsigned8       filepos{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);


a := LIMIT(namesTable, 1000);

b := a(surname <> 'Halliday');

c := LIMIT(b, 800, FAIL(99, 'Oh dear too many people'));

output(c,,'out.d00');

d := CASE(namesTable.age, 10=>'Ok',11=>'Poor',99=>ERROR('99 Not allowed'), 'Pass');

e := CASE(namesTable.surname, 'Halliday'=>1,'Chapman'=>2,ERROR('Not a know surname'));

f := IF(namesTable.forename = 'Gavin', 'Ok', FAIL(string,'Just not good enough'));

output(namesTable, {surname, forename, age, d, e, f});



i := index(namesTable, { surname, forename, filepos } ,'\\seisint\\person.name_first.key');

string30 searchName := 'Halliday'       : stored('SearchName');

x := limit(i(surname=searchName), 100);

output(x);


j := JOIN(namesTable, i, LEFT.surname=RIGHT.surname);

k := LIMIT(j, 99);

output(k);


l := LIMIT(a(surname <> 'Chapman'), 800, FAIL(99, 'Oh dear too many people'));

output(l);

m := LIMIT(a(forename <> 'Richard'), 999);

output(m);
