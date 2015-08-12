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

person := dataset('person', { unsigned8 person_id, string1 per_sex }, thor);
rec := Record
  totalcount := SUM(GROUP, IF(TRUE, 1, 0));
  male := SUM(GROUP,IF(person.per_sex IN ['1'], 1, 0));
END;

tab := TABLE (person, rec); // This works against ***SOME*** clusters
//tab := TABLE (person, rec, 1);    // This works against ***ALL*** clusters
OUTPUT(tab);

