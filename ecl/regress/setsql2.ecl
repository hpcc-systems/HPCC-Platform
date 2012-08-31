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

#option ('globalFold', false);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);

inlist(string x, set of string y) := if ( x in y, 'yes','no');
inlist2(string x, set of string20 y) := inlist(x, y);

'one' in ['Gavin','Jason','Emma','Vicky'];
inlist('one', ['Gavin','Jason','Emma','Vicky']);

a := ['Gavin','Jason','Emma','Vicky'];

//count(person(inlist('one', ['Gavin','Jason','Emma','Vicky'])='yes'));
count(person(inlist2('one', a)='yes'));

