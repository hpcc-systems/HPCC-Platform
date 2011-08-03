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

namesTable := dataset('nt', namesRecord, thor);
addressTable := dataset('at', addressRecord, thor);

JoinRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
string30        addr;
            END;

JoinRecord JoinTransform1 (namesRecord lo, namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.addr := r.addr;
                    SELF.age := hash64(l.forename, hash64(lo.surname,2,3));
                    SELF := l;
                END;


//output(join (NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform1 (LEFT, RIGHT), LEFT OUTER));

JoinRecord JoinTransform2 (namesRecord lo, namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.addr := r.addr;
                    SELF.age := hash64(l.forename, hash64(lo.surname,2,3));
                    SELF := l;
                END;
//output(join (NamesTable, AddressTable, LEFT.surname = RIGHT.addr, JoinTransform2 (LEFT, RIGHT), LEFT OUTER));




z := project(NamesTable, transform({ dataset(JoinRecord) j1, dataset(JoinRecord) j2 },
    self.j1 := join (dataset(left), AddressTable, LEFT.surname = RIGHT.surname, JoinTransform1 (left, LEFT, RIGHT), LEFT OUTER),
    self.j2 := join (dataset(left), AddressTable, LEFT.surname = RIGHT.addr, JoinTransform2 (left, LEFT, RIGHT), LEFT OUTER)));

output(z);