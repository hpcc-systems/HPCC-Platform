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
string      surname;
qstring     forename;
integer2        age := 25;
                ifblock(self.age != 99)
unicode     addr := U'12345';
varstring   zz := self.forename[1] + '. ' + self.surname;
varunicode  zz2 := U'!' + (unicode)self.age + U'!';
boolean         alive := true;
data5           extra := x'12345678ef';
                end;
            END;

namesTable2 := dataset([
        {'Time','Old Father',1000000},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'Hawthorn','Abigail',0},
        {'South','Ami',2},
        {'X','Z'}], namesRecord);

x := namesTable2 : stored('x',few);


namesRecord t(namesRecord l) := transform
    self := l;
    END;


output(project(x, t(LEFT)));
