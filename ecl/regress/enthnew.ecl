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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, unsigned8 xpos }, thor);
#option ('globalFold', false);

output(enth(person,1000),,'out.d00');       // pick 1000 items
output(enth(person,1,1000),,'out.d00');     // pick .1% items
output(enth(person,1,1000,10),,'out.d00');  // pick .1% items, starting with 10th.

output(enth(person,1000,LOCAL),,'out.d00');     // pick 1000 items
output(enth(person,1,1000,LOCAL),,'out.d00');       // pick .1% items
output(enth(person,1,1000,10,LOCAL),,'out.d00');    // pick .1% items, starting with 10th.
