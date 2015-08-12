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
integer2        age := 25;
string20        surname;
string10        forename;
            END;

namesTable := dataset('x',namesRecord,FLAT);

sort1 := sort(namesTable, surname, forename, local);

//Only preserves local sort order if it was globally sorted!
dist1 := distribute(sort1, skew(1.0));

group3 := group(dist1, surname, LOCAL);
sort2 := sort(group3, forename); // This should not be optimized away - no longer locally sorted  

output(sort2);
