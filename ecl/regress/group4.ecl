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

#option ('optimizeGraph', false);
//#option ('unlimitedResources', true);
#option ('groupAllDistribute', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

//Distribute/local sort/group
group0 := group(namesTable, forename, all);
output(group0, {count(group)});

//sort/local sort/local group
sorted1 := sort(namesTable, surname);
group1 := group(sorted1, surname, forename, all);
output(group1, {count(group)});

//sort/local sort/local group
sorted2 := sort(namesTable, surname);
group2 := group(sorted2, forename, surname, all);
output(group2, {count(group)});

//sort/local sort/local group
sorted2b := sort(namesTable, surname);
group2b := group(sorted2b, forename, surname, all);
output(group2b, {count(group)});

//distribute/local sort/local group
sorted3 := distribute(namesTable, hash(surname));
group3 := group(sorted3, forename, surname, local, all);
output(group3, {count(group)});

//distribute/local sort/local group
sorted4 := distribute(namesTable, hash(surname));
group4x := group(sorted4, forename, surname, all);
output(group4x, {count(group)});

//distribute/distribute/local sort/local group
sorted5 := distribute(namesTable, hash(age));
group5 := group(sorted5, forename, surname, all);
output(group5, {count(group)});

//sort/group - non-local
sorted6 := sort(namesTable, forename, surname, age);
group6 := group(sorted6, forename, surname, all);
output(group6, {count(group)});

