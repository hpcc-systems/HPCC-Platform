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

layout := {
    unsigned u1,
};

ds := nofold(dataset([{1},{2}], layout));
ds2 := nofold(dataset([{2},{3}], layout));

dsDist := distribute(ds, u1 - 1);
dsDist2 := distribute(ds2, u1 - 2);

j1 := join(dsDist, dsDist2,
           left.u1 = right.u1,
           transform(left));

output(j1);
