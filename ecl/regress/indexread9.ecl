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

little_endian unsigned3 searchDob := 99 : stored('dob');
/*big_endian*/ unsigned3 searchDob2 := 100 : stored('dob2');        // big endian temporaries not supported yet.

d := dataset('~local::rkc::person', { string15 name, unsigned3 dob, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { dob, name, filepos } ,'\\home\\person.name_first.key');

a1 := i(dob=searchDob or dob=searchDob2);

output(a1);


