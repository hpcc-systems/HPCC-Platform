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
            END;

namesTable := dataset('x',namesRecord,FLAT,preload);

x1 := namesTable(surname[1] in ['0','1','3']);
x2 := table(x1, {forename, surname});
output(x2);

namesTable2 := dataset('x',namesRecord,FLAT);

y1 := namesTable2(surname[1] in ['0','1','3']);
y2 := preload(y1);
y3 := table(y2, {forename, surname});
output(y3);


d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);
i1 := index(d, { f_name := (string11) name, filepos } ,'\\seisint\\person.name_first.key',preload);
output(i1(f_name='Gavin'));

i2 := index(d, { f_name := (string11) name, filepos } ,'\\seisint\\person.name_first.key');
output(preload(i2)(f_name='Halliday'));

namesTable3 := dataset('x',namesRecord,FLAT,preload(1+2+3+4));
output(namesTable3(age=3));
