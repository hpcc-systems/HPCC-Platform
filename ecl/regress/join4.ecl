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


