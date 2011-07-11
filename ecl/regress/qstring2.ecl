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

#option ('globalFold', false);
rec1 :=     RECORD
string15    forename;
string20    surname;
string5     middle := '';
            END;

rec2 :=     RECORD
qstring20   forename1;
varstring   forename2;
string20    forename3;
qstring20   surname;
            END;


test := nofold(dataset([
                {'Gavin','Halliday'},
                {'Richard','Chapman'},
                {'David','Bayliss'}
                ], rec1));


rec2 t(rec1 l) := TRANSFORM 
        SELF.forename1 := TRIM(l.forename) + 'a' + TRIM(l.middle);
        SELF.forename2 := TRIM(l.forename) + 'b' + TRIM(l.middle);
        SELF.forename3 := TRIM(l.forename) + 'c' + TRIM(l.middle);
        SELF:=l; 
    END;

test2 := project(test, t(LEFT));

output(test2,,'out.d00',overwrite);


output(join(test2, test2(true), left.surname=right.surname, HASH));
