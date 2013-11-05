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

//noroxie
//nothor


namerec := {UNSIGNED1 i, STRING5 str};

names1 := nofold(DATASET([{1, 'one'}, {4, 'four'}, {7, 'seven'}, {8, 'eight'}, {9, 'nine'}], namerec));
output(names1, named('names'),thor);

output(dataset(workunit('names'), namerec), NAMED('namesClone'), thor);

nameClone := dataset(workunit('namesClone'), namerec);
next(unsigned1 search) := nofold(sort(nofold(nameClone(i > search)), i))[1];

names2 := nofold(DATASET([{2, 'two'}, {3, 'three'}, {4, 'four'}, {9, 'nine'}], namerec));
p := project(names2, transform(namerec, self.i := left.i; self.str := next(left.i).str));
output(p);
