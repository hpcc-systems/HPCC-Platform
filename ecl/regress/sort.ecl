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


namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Smithe','Pru',10},
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'X','Z'}], namesRecord);

person := dataset('person', { unsigned8 person_id, string1 per_sex, string2 per_st, string40 per_first_name, string40 per_last_name}, thor);
thorNamesTable := dataset('names.d00', namesrecord, THOR);
sortedNames := sort(namesTable, -surname, forename);
//output(sortedNames,,'out.d00');

previousSort := sorted(thorNamesTable,(string25)thorNamesTable.forename,(string15)thorNamesTable.surname);

myset := sort(person,person.per_last_name,person.per_first_name,stable,algorithm('Insertion'));
output(myset);
string algo := '' : stored('algorithm');
//myset2 := sort(namesTable,(string25)namesTable.surname,(string15)namesTable.forename,joined(myset));
myset2 := sort(namesTable,(string25)namesTable.surname,(string15)namesTable.forename,joined(previousSort),unstable(algo));

output(myset2);
