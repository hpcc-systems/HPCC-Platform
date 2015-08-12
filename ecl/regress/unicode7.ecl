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



ds := dataset('ds', {unicode40 name, unicode40 associate, unsigned6 did}, thor);


i := index(ds, { name }, { associate, did }, 'i');      // don't sort payload using memcmp

buildindex(i);

unicode40 searchName := U'' : stored('searchName');
unicode40 searchName2 := U'Ахалкалаки';

output(i(keyed(name = searchName)));

output(i(keyed(name[1..length(trim(searchName))] = trim(searchName))));

output(i(keyed(name[1..length(trim(searchName2))] = trim(searchName2))));


output(i(keyed(name[1..15] = trim(searchName2))));
