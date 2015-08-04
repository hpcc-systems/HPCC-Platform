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

#option ('targetClusterType','roxie');

ds := nofold(dataset([
            {1, 1}, {1,3}, {1, 5}, {1, 4},
            {2, 10}, {2,8}, {2,3}, {2,5}, {2,8},
            {3, 4}], { unsigned val1, unsigned val2 }));


gr := group(ds, val1);
f1 := having(gr, count(rows(left)) > 1);
output(f1) : independent;

