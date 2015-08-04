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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, string40 per_last_name }, thor);

// test: mix dataset parameter with others

r := record
  string15 name;
end;

integer count_ds(virtual dataset(r) d, string lookfor) := count(d(name = lookfor));

string lookfor := 'fred';

boolean name := false;
count_ds(person{name:=per_first_name}, lookfor);
