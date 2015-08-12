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


i := dataset([{1}, {3}, {5}, {7}, {9}, {2}, {4}, {6}, {8}, {10}], {unsigned1 d});

p := project(nofold(i), transform({ string100000 s, unsigned d}, self.s := (string)LEFT.d + 'xxx' + (string)LEFT.d; self.d := LEFT.d; ));

dist := distribute(nofold(p), hash32(d));

agg1 := TABLE(nofold(dist), { d, count(group, s != '88xxx88') }, d, few);

agg2 := TABLE(nofold(dist), { d, count(group, s != '88xxx88') }, d, few, local);

sequential(
    output(agg1);
    output(agg2)
);

