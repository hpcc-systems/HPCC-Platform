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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_xyz }, thor);

x1 := dedup(person,per_xyz,ALL);
output(x1,,'out1.d00');
x2 := dedup(person,per_xyz);
output(x2,,'out2.d00');
x3 := dedup(person,per_xyz,LOCAL);
output(x3,,'out3.d00');
x4 := dedup(person,per_xyz,LOCAL,ALL);
output(x4,,'out4.d00');
