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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name }, thor);
recordtable := record
  person.per_first_name ;
  person.per_last_name ;
  countofPeople := count(group);
  testx := 0;
  end;

//t := table(person,recordtable,person.per_sex,1+2);
// bug
//t := table(person,recordtable,person.per_first_name,1+2);
// why the 1  cause bug??
t := table(person,recordtable,person.per_last_name,1+2);
output(t)