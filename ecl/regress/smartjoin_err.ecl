/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

addressRecord :=
            RECORD
string20        surname;
string30        addr;
            END;

namesTable := dataset('n', namesRecord, thor);

addressTable := dataset('a', addressRecord, thor);

JoinedF := join (namesTable, addressTable, LEFT.surname = RIGHT.surname, SMART, LOOKUP);
output(JoinedF,,'out.d00',overwrite);

JoinedF2 := join (namesTable, addressTable, LEFT.surname = RIGHT.surname, SMART, RIGHT ONLY);
output(JoinedF2,,'out.d00',overwrite);
