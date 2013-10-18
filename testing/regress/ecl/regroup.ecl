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


inrec := record
unsigned6 did;
    end;

outrec := record(inrec)
string20        name;
unsigned        score;
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
// choose the best 5 results for each input row
best := topn(combined(score != 0), 2, -score);
output(best);

