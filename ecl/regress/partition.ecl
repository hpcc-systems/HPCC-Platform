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

myrec := record
string10 name;
unsigned4   value;
unsigned6   age;
string20    pad;
string20    key;
        end;


myfile1 := dataset('in1', myrec, thor);

f1 := distribute(myfile1, partition(age, pad, key));

output(f1,,'out1.d00');

f2 := partition(myfile1, key, value);

output(f2,,'out2.d00');
