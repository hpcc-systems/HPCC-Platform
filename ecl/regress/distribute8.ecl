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

sort1 := sort(namesTable, surname, forename);

//Preserves global sort order but not the distribution
dist1 := distribute(sort1, skew(1.0));
sort2 := sort(dist1, surname, forename, local); // this should be optimized away


group3 := group(dist1, surname, forename); //this should not be optimized to a local group
summed := TABLE(group3, { count(group) });

sequential(
output(sort2);
output(summed);
);
