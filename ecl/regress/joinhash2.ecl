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

namesRecord JoinTransform1 (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.age := hash64(l.forename, hash64(r.addr));
                    SELF := l;
                END;


j1 := join(NamesTable, AddressTable, LEFT.surname = RIGHT.surname, JoinTransform1 (LEFT, RIGHT), LEFT OUTER);

namesRecord JoinTransform2 (namesRecord l, addressRecord r) :=
                TRANSFORM
                    SELF.age := hash64(l.age, 2, hash64(r.addr));
                    SELF := l;
                END;

j2 := join (j1, AddressTable, LEFT.surname = RIGHT.addr, JoinTransform2 (LEFT, RIGHT), LEFT OUTER);


output(j2);




