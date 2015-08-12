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
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hwang', 'James'},
        {'Hawthorn', 'Gavin'},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Hilda',30},
        {'X','Z'}], namesRecord);

namesRecord t(namesRecord r) := TRANSFORM
        SELF := r;
    END;

t1 := dedup(namesTable, surname,LOCAL, LEFT, KEEP 2);
t2 := dedup(t1, LEFT.age-RIGHT.age< 2,RIGHT);
t3 := rollup(t2, surname, t(RIGHT));
t4 := rollup(t3, surname, t(RIGHT),LOCAL);

output(t4);
