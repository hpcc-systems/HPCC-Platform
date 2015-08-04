/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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
fakeAddressIndex := dataset('addressDs', indexRecord, thor);

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

output(doJoin(namesTable, fakeAddressIndex));
