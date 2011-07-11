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
bitfield10_8    age;
bitfield52      age2;
bitfield10      age3;
bitfield52      age4;
bitfield1       isOkay;
bitfield2       dead;       // y/N/maybe
varstring1          okay;
bitfield28      largenum;
bitfield1       extra;
bitfield63      gigantic;
bitfield1       isflag;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesRecord t(namesRecord l) :=
        TRANSFORM
            SELF.age := IF(l.age > (integer8)12, 10,12);
            SELF.dead := (integer)l.surname;
            SELF.surname := (string)l.largenum;
            SELF := l;
        END;

z := project(namesTable, t(LEFT));

output(z);//,,'out.d00');
