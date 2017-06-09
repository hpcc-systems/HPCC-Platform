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

r := { unsigned id };
ds := dataset([1,2,3,4], r);


f(dataset(r) inDs) := FUNCTION

    o1 := output(inDs((id % 4) = 3),named('o1'),extend);
    o2 := output(inDs((id % 4) = 1),named('o2'),extend);
    return WHEN(inDs, parallel(o1, o2));
END;

l := LOOP(ds, 3, f(ROWS(LEFT)));

output(l);
