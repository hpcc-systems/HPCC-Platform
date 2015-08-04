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

#option ('countIndex', true);

d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);
d2 := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\home\\person.name_first.key');

string15 searchName := '' : stored('searchName');
string15 searchName2 := '' : stored('searchName2');
a1 := i(searchName <> '', name = searchName);

output(a1);
count(i(searchName2 <> '', name = searchName2));



x := join(d, i, searchName<>'' and right.name in [searchName, left.name]);

output(x);


output(d2(searchName2 <> '' and keyed(searchName<>'' and name <> searchName)));
