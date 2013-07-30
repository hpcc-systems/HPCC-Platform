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
        {'Smith','St Barnabas House'},
        {'X','12 The burrows'},
        {'X','14 The crescent'},
        {'Z','The end of the world'}
        ], addressRecord);

sortedNamesTable := SORT(namesTable, surname, forename, age);

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
SEQUENTIAL(
    @{
        JoinedF := join (sortedNamesTable, addressTable, LEFT.surname = RIGHT.surname, JoinTransform(LEFT, RIGHT), LOOKUP);
        RETURN output(SORT(JoinedF, surname, forename, LOCAL));
     }
     ,
    @{
        JoinedF := join (sortedNamesTable, addressTable, LEFT.surname = RIGHT.surname, JoinTransform(LEFT, RIGHT), LOOKUP, UNORDERED);
        RETURN output(SORT(JoinedF, surname, forename, LOCAL));
     }
);
