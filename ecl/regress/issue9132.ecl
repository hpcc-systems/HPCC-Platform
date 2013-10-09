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
string20        forename;
integer2        age := 25;
            END;

namesTable1 := dataset('x',namesRecord,FLAT);
namesTable2 := dataset('y',namesRecord,FLAT);

s1 := SORT(DISTRIBUTE(namesTable1, HASH(surname)), surname, LOCAL);
s2 := SORT(DISTRIBUTE(namesTable2, HASH(surname)), surname, LOCAL);

j1 := DENORMALIZE(s1, s2, LEFT.surname = RIGHT.surname, TRANSFORM(LEFT));
j2 := DENORMALIZE(s1, s2, LEFT.surname = RIGHT.surname, TRANSFORM(namesRecord, SELF.surname := RIGHT.forename; SELF.forename := LEFT.surname));
output(sort(j1, surname, local));   // this sort can be removed
output(sort(j2, forename, local));  // no this sort cannot be removed - the transform may be called 0,1,or many times.


namesTable3 := dataset('z1',namesRecord,FLAT);
namesTable4 := dataset('z2',namesRecord,FLAT);

s3 := SORT(DISTRIBUTE(namesTable3, HASH(surname)), surname, forename, LOCAL);
s4 := SORT(DISTRIBUTE(namesTable4, HASH(surname)), surname, forename, LOCAL);

j3 := DENORMALIZE(s3, s4, LEFT.surname = RIGHT.surname AND LEFT.forename = RIGHT.forename, TRANSFORM(namesRecord, SELF.forename := RIGHT.forename; SELF := LEFT));
output(sort(j3, surname, forename, local));   // this sort cannot be removed
output(sort(j3, surname, local));  // this can..
