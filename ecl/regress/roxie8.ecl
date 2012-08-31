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

#option ('targetClusterType', 'hthor');

d := sort(dataset([{'1'}, {'2'}, {'4'}], { unsigned1 x}), x);

d1 := d;
d2 := d(x=2);

string1 s := '1' : stored('s');
integer8 i := 1 : stored('i');

output(if(s='1',d1,d2));
output(if(i=1,d1,d2));
