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

namesTable := dataset('names', namesRecord, THOR);

addressTable := dataset('address', addressRecord, THOR);

dNamesTable := distribute(namesTable, hash(surname));
dAddressTable := distributed(addressTable, hash(surname));

JoinRecord :=
            RECORD
string20        surname1;
string20        surname2;
string10        forename;
integer2        age := 25;
string30        addr;
            END;

JoinRecord j1 (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.surname1 := l.surname;
                    SELF.surname2 := '';
                    SELF.addr := r.addr;
                    SELF := l;
                END;

Join1 := join (dNamesTable, addressTable, LEFT.surname = RIGHT.surname, j1(LEFT, RIGHT), LEFT OUTER, local) :persist('j1');

JoinRecord j2 (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.surname1 := '';
                    SELF.surname2 := r.surname;
                    SELF.addr := r.addr;
                    SELF := l;
                END;

Join2 := join (namesTable, dAddressTable, LEFT.surname = RIGHT.surname, j2(LEFT, RIGHT), local) :persist('j2');


JoinRecord j3 (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.surname1 := '';
                    SELF.surname2 := r.surname;
                    SELF.addr := r.addr;
                    SELF := l;
                END;

Join3 := join (dNamesTable, addressTable, LEFT.surname = RIGHT.surname, j3(LEFT, RIGHT), LEFT OUTER, local) :persist('j3');


output(join1);
output(join2);
output(join3);


