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

namesTable1 := dataset([
        {'Hawthorn','Gavin',31},
        {'X','Z'}], namesRecord);

o1 := output(namesTable1,,'~names',overwrite);


namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',31},
        {'Hawthorn','Robert',0},
        {'X','Z'}], namesRecord);

o2 := output(namesTable2,,'~names',overwrite);



namesTable := dataset('~names',namesRecord,FLAT);

p1 := namesTable(age <> 0) : persist('p1');
p2 := namesTable(age <> 10) : persist('p2');



boolean use1 := true : stored('use1');

if (use1,
    sequential(o1, output(count(p1))),
    sequential(o2, output(count(p2)))
    );


/*
Expected:

1:  stored
2:  persist p1
3:  persist p2
4:  normal              output o1
5:  normal [2]          output count
6:  sequential [4,5]
7:  normal              output o2
8:  normal [3]          output count
9:  sequential [7,8]
10: normal [1]          use1
11: conditional [10, 6, 9]

*/
