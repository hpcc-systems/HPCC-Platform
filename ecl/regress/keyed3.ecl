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
unsigned8       fileposition{virtual(fileposition)}
            END;

namesTable := dataset('x',namesRecord,FLAT);

i := index(namesTable, { namesTable }, 'i');

set of string10 searchForenames := all : stored('searchForenames');

filterByAge(dataset(recordof(i)) in) := in(keyed(age = 10, opt));


output(filterByAge(i(keyed(surname='Hawthorn'),keyed(true or forename='abcdefghij'))));
output(filterByAge(i(keyed(surname='Hawthorn' and forename='Gavin'))));
output(filterByAge(i(keyed(surname='Hawthorn' and forename in searchForenames))));

output(i(WILD(surname),KEYED(forename='Gavin'),keyed(age=10,opt)));
output(i(keyed(surname='Hawthorn',opt),KEYED(forename='Gavin',opt),keyed(age=10,opt)));
