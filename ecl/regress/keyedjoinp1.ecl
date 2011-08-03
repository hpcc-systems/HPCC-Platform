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
unsigned8       fpos;
            END;

namesTable := dataset('names', namesRecord, thor, __grouped__);

addressIndex := index(addressRecord, 'address');
indexRecord := recordof(addressIndex);
fakeAddressIndex := dataset('addressDs', addressRecord, thor);

JoinRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
string30        addr;
            END;

doAggregate(grouped dataset(JoinRecord) lDs) :=
    table(lDs, { count(group) });

doJoin(grouped dataset(namesRecord) l, dataset(indexRecord) r) := function
    JoinRecord JoinTransform (namesRecord l, indexRecord r) :=
                    TRANSFORM
                        SELF.addr := r.addr;
                        SELF := l;
                    END;

    j := join (l, r, LEFT.surname = RIGHT.surname, JoinTransform (LEFT, RIGHT), keyed);
    return doAggregate(j);
end;

output(doJoin(namesTable, addressIndex));
