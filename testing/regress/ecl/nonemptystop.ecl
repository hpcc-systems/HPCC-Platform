/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

//fail

//Check that a failure when starting an activity, that is an input to NONEMPTY still stops
//the other inputs to the NONEMPTY (issue17337)

idRecord := { unsigned id; };
idDs := DATASET([1,2,3], idRecord);

badDs := FAIL(idRecord, 'Oh no!');

isFalse := FALSE : STORED('false');
ds1 := IF(isFalse, idDs, badDs);
c := COUNT(ds1);

x := SORT(NOFOLD(idDs), id);

f := x(c = 0);

res := NONEMPTY(f, x);

output(res);
