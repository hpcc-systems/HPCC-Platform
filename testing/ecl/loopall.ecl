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

//Case 3: global loop test
output(loop(namesTable2, sum(rows(left), age) < 1000, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

//Case 4: global loop test with row filter
output(loop(namesTable2, left.age < 100, exists(rows(left)) and sum(rows(left), age) < 1000, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

//case 5: a row filter
output(loop(namesTable2, left.age < 100, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

// Same as above except also using counter in various places.

//Case 1 fixed number of iterations
output(loop(namesTable2, counter <= 10, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));


//Case 2: fixed number of iterations with row filter
output(loop(namesTable2, 10, left.age * counter <= 200, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));

//Case 3: global loop test
output(loop(namesTable2, sum(rows(left), age) < 1000*counter, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left))));


//Case 4: global loop test with row filter
loopBody(dataset(namesRecord) ds, unsigned4 c) :=
        project(ds, transform(namesRecord, self.age := left.age*c; self := left));

output(loop(namesTable2, left.age < 100, exists(rows(left)) and sum(rows(left), age) < 1000, loopBody(rows(left), counter)));


//case 5: a row filter
output(loop(namesTable2, left.age < 100, loopBody(rows(left), counter)));

//case 6: loop within a child query, with condition on dataset
parentRecord := RECORD
 string parentField;
 DATASET(namesRecord) children;
END;

parentDS := DATASET([{'parent1', namesTable2}], parentRecord);


addNum(dataset(namesRecord) ds, unsigned num) := FUNCTION
 dsAdded := project(ds, transform(namesRecord, self.age := left.age+num; self := left));
 return dsAdded;
END;

countChildren(dataset(namesRecord) ds, unsigned ageThreshold) := FUNCTION
 q := ds(age<ageThreshold);
 return COUNT(q);
END;

parentRecord childTrans(parentRecord p) := transform
 self.parentField := p.parentField + 'changed';
 self.children := loop(p.children, countChildren(rows(left), 50) > 2, addNum(rows(left), 5));
END;

// loop adding an increment and perform a count condition on the dataset
output(nofold(project(nofold(parentDS), childTrans(LEFT))));

//case 7: loop within a child query, fixed number of iterations with row filter
parentRecord childTrans2(parentRecord p) := transform
 self.parentField := p.parentField + 'changed';
 self.children := loop(p.children, 10, left.age <= 60, project(rows(left), transform(namesRecord, self.age := left.age*two; self := left)));
END;

output(nofold(project(nofold(parentDS), childTrans2(LEFT))));
