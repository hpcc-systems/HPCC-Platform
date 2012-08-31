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
integer2        age := 25;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);


inf := distribute(namesTable, hash32(forename));

j5 := join(inf,inf,left.forename[1..*]=right.forename[1..*] and left.forename[length(trim(left.forename))-3..] = right.forename[length(trim(right.forename))-3..] and (left.age * right.age < 880),atmost(left.forename[1..*]=right.forename[1..*] and left.forename[length(trim(left.forename))-3..] = right.forename[length(trim(right.forename))-3..],1000),hash);
output(j5);

