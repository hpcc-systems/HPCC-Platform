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
//nothorlcr

inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
unsigned        score;
          end;

nameRec := record
string20        name;
unsigned        score;
          end;

resultrec := record(inrec)
dataset(nameRec)    names;
          end;

ds := dataset([1,2,3,4,5,6], inrec);

dsg := group(ds, row);

i1 := dataset([
            {1, 'Gavin', 10}, 
            {2, 'Richard', 5}, 
            {5,'Nigel', 2},
            {0, '', 0}], outrec);
i2 := dataset([
            {1, 'Gavin Halliday', 12}, 
            {2, 'Richard Chapman', 15}, 
            {3, 'Jake Smith', 20},
            {5,'Nigel Hicks', 100},
            {0, '', 0}], outrec);
i3 := dataset([
            {1, 'Halliday', 8}, 
            {2, 'Richard', 8}, 
            {6, 'Pete', 4},
            {6, 'Peter', 8},
            {6, 'Petie', 1},
            {0, '', 0}], outrec);

j1 := join(dsg, i1, left.did = right.did, left outer, many lookup);
j2 := join(dsg, i2, left.did = right.did, left outer, many lookup);
j3 := join(dsg, i3, left.did = right.did, left outer, many lookup);

//perform 3 soap calls in parallel
combined := regroup(j1, j2, j3);

resultRec t(inrec l, dataset(recordof(combined)) r) := transform
    self.names := project(r, transform(nameRec, self := left));
    self := l;
    end;

results := combine(dsg, combined, group, t(LEFT, ROWS(right)(score != 0)));
output(results);

//A variation using rows in a child query.
resultRec t2(inrec l, dataset(recordof(combined)) r) := transform
    self.names := project(sort(r, -score), transform(nameRec, self := left));
    self := l;
    end;

results2 := combine(dsg, combined, group, t2(LEFT, ROWS(right)(score != 0)));
output(results2);

//Check result is still grouped - result should be a list of 1s
output(results2, { cnt := count(group) });
