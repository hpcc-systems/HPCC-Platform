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
string10        forename1 := '';
string10        forename2 := '';
string10        forename3 := '';
string10        forename4 := '';
string10        forename5 := '';
            END;

namesTable := dataset([
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

//output(sort(namesTable,surname),{surname},'out.d00');

//output(group(namesTable,surname ALL),{surname,count(group)},'out.d00');
output(group(sort(namesTable,surname),surname),{surname,count(group)},'out0.d00');
output(table(namesTable,{surname,count(group)},surname),,'out1.d00');

a1 := sort(namesTable, namesTable.forename1, namesTable.forename2, namesTable.forename3, - namesTable.forename4, namesTable.forename5, LOCAL);
a2 := GROUP(a1, a1.forename1, a1.forename2, a1.forename3, LOCAL);

namesRecord t(namesRecord l) :=
    TRANSFORM
        SELF := l;
    END;

a3 := ITERATE(a2, t(LEFT));

output(a3);
