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

#option ('globalFold', false);

person := dataset('person', { unsigned8 person_id }, thor);

// Following should be true

count(person(all >= all));
count(person(all>[1,2]));
count(person([1,2]<>all));

count(person(123 in ALL));
count(person('a' in ALL));

count(person([]!=all));
count(person(all!=[]));

// Following should be false

count(person(all > all));
count(person([1,2]>all));
count(person([1,2]=all));
count(person(145 not in ALL));

count(person(all=[]));
count(person([]=all));
