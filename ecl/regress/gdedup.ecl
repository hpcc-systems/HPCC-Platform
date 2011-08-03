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

namesTable := dataset([
        {'Hwang', 'James'},
        {'Hawthorn', 'Gavin'},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Hilda',30},
        {'X','Z'}], namesRecord);

namesRecord t(namesRecord r) := TRANSFORM
        SELF := r;
    END;

t1 := dedup(namesTable, surname,LOCAL, LEFT, KEEP 2);
t2 := dedup(t1, LEFT.age-RIGHT.age< 2,RIGHT);
t3 := rollup(t2, surname, t(RIGHT));
t4 := rollup(t3, surname, t(RIGHT),LOCAL);

output(t4);
