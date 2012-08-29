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

aa := dataset('aaa',{INTEGER a1; }, FLAT);
person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);

record1 := record
    integer i1;
    integer i2;
end;

record1 tranx0(integer i, integer j) := transform
    self.i1 := i;
    self.i2 := j + person.person_id;
end;

mytable := dataset([{1,2},{3,4}], record1);

normalizedStuff := normalize(mytable, LEFT.i1, tranx0(left.i2, COUNTER));

output(normalizedStuff);

