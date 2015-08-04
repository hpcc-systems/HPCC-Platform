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

person := dataset('person', { unsigned8 person_id, string1 per_sex, unsigned per_ssn, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);

x:= power(person.person_id, 2);
y := power(10,1);
z := power(10,2);

output(person,
    {
    power(10,1),
    power(10,1),
    power(10,1),
    power(10,1),
    x1:= x,
    x2:= x,
    x3 := x,
//  y1 := y,
//  y2 := y,
//  y3 := y,
    z1 := z;
    if (x != 0, 1/x, x*2),
    if (y != 0, 1/y, y*2),
    0
    },'out.d00');