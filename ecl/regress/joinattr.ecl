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
string20        surname := '?????????????';
string10        forename := '?????????????';
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string30        addr := 'Unknown';
string20        surname;
            END;

namesTable := dataset([
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smith','Jo'},
        {'Smith','Matthew'},
        {'X','Z'}], namesRecord);

addressTable := dataset([
        {'Hawthorn','10 Slapdash Lane'},
        {'Smith','Leicester'},
        {'Smith','China'},
        {'X','12 The burrows'},
        {'X','14 The crescent'},
        {'Z','The end of the world'}
        ], addressRecord);

JoinRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
string30        addr;
            END;

JoinRecord JoinTransform (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.addr := r.addr;
                    SELF := l;
                END;

joina := join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), SKEW(0.123));
joinb := join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), ATMOST(10));
joinc := join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), THRESHOLD(1000000000000));

output(joina+joinb+joinc,,'out.d00');

joind := join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), nosort, local);
joine := join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), NOSORT(LEFT));
output(joind+joine);
