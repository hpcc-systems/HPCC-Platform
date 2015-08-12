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

#option ('pickBestEngine', false);

person := dataset('person', { unsigned8 person_id, string5 per_zip, string40 per_first_name, data9 per_cid, unsigned8 xpos }, thor);

myfac := map ( person.per_zip>'30000' and person.per_zip < '31000' => 1,
               person.per_zip = '44556' => 2,
               3);

s := sort(person,myfac);

ch := choosen(s,5000);

cnts := record
  p := person.per_zip;
  c := count(group);
  end;

gr := table(ch,cnts,per_zip);

//output(gr)


gr2 := table(person,{per_zip});

output(gr2);
output(person);
output(group(person));
