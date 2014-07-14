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

//nohthor
//nothor

#option ('newChildQueries', true)

namesRecord := 
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Halliday','Gavin',31},
        {'Halliday','Liz',30},
        {'Salter','Abi',10},
        {'X','Z'}], namesRecord);

two := 2 : stored('two');

//Case 1 fixed number of iterations
output(loop(namesTable2, 10, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

output(loop(namesTable2, 3, rows(left) & rows(left)));

//Case 2: fixed number of iterations with row filter
output(loop(namesTable2, 10, left.age <= 60, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

// ** case 3 ** global loop test

// ** case 4 ** global loop test

//case 5: a row filter
output(loop(namesTable2, left.age < 100, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));



// Same as above except also using counter in various places.

// ** case 1 ** global loop test, although not based on dataset (counter <= 10)

//Case 2: fixed number of iterations with row filter
output(loop(namesTable2, 10, left.age * counter <= 200, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

// ** case 3 ** global loop test

// ** case 4 ** global loop test

loopBody(dataset(namesRecord) ds, unsigned4 c) :=
        project(ds, transform(namesRecord, self.age := left.age*c; self := left));

//case 5: a row filter
output(loop(namesTable2, left.age < 100, loopBody(rows(left), counter)));
