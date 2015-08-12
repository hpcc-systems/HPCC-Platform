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
x := record
v1 := count(GROUP, person.per_ssn > 50);
v2 := count(GROUP, person.per_ssn > 60);
v3 := count(GROUP, person.per_ssn > 70);
v4 := count(GROUP, person.per_ssn > 80);
end;

y := table(person, x) : stored('hi');

//output(y);
y[1].v1;
y[1].v2;
y[1].v3;
y[1].v4;
