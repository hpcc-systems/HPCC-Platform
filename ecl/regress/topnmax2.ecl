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


i3 := dataset([{'a', 1}, {'c',3}, {'b',5}, {'c',7}, {'a',9}, {'b',2}, {'d',4}, {'a',6}, {'a',8}, {'b',10}, {'d',11}], {string1 l, unsigned1 d});
g3 := group(i3, d % 2);
t1 := topn(g3, 2, l);
output(t1);     // expect {'a',1},{'a',6'}
output(table(t1, { count(group) }));        // expect {1,1}


output(sort(topn(i3, 2, l, best('a'), local), l, d, local));       // expect {'a',1},{'a',9} - terminate early...
output(topn(i3, 1, -l, best('d'), local));      // expect {'d',4}
b1 := topn(g3, 2, l, best('a'));
output(sort(b1, l, d));     // expect {'a',1},{'a',9'},{'a',6},{'a',8},{'d',11}
output(table(b1, { count(group) }));        // expect {1,1}
