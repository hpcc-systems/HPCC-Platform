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

namesTable := dataset([
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'X','Z'}], namesRecord);

sort1 := sort(namesTable, forename, LOCAL, unstable('Quick'));
sort2 := sort(namesTable, forename, SKEW(0.5));
sort3 := sort(namesTable, forename, THRESHOLD(1000000));
sort4 := sort(namesTable, forename, SKEW(0.1), THRESHOLD(99999));
output(sort1+sort2+sort3+sort4,,'out.d00');

sort5 := sort(group(namesTable, surname), forename, many);
sort6 := sort(namesTable, forename, skew(1.1,3.4));
output(sort5+sort6);
