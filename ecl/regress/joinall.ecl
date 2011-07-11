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
string20        surname;
string30        addr := 'Unknown';
            END;

namesTable := dataset([
        {'Salter','Abi',10},
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Smith','Jo'},
        {'Smith','Matthew'},
        {'X','Z'}], namesRecord);

addressTable := dataset([
        {'Halliday','10 Slapdash Lane'},
        {'Smith','Leicester'},
        {'Smith','China'},
        {'Smith','St Barnabas House'},
        {'X','12 The burrows'},
        {'X','14 The crescent'},
        {'Z','The end of the world'}
        ], addressRecord);

dNamesTable := global(namesTable, few);
dAddressTable := global(addressTable, few);

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

JoinedF1 := join (dNamesTable, dAddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), LEFT OUTER, ALL);
nothor(output(JoinedF1));

JoinedF2 := join (dNamesTable, dAddressTable, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), LEFT ONLY, ALL);
nothor(output(JoinedF2));

JoinedF3 := join (dNamesTable, dNamesTable, LEFT.surname = RIGHT.surname, transform(left), ALL);
nothor(output(JoinedF3));
