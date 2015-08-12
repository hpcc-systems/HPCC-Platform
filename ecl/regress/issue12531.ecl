/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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
d := dataset('~local::rkc::person', { string15 name, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { f_name := (varstring11) name, filepos } ,'\\home\\person.name_first.key');
i2 := index(d, { f_name := (varunicode11) name, filepos } ,'\\home\\person.name_first.key');

a1 := i(f_name='RICHARD');

a2 := sort(a1, -f_name);
build(i);
output(a2(filepos > 10));
output(a2(filepos > 20));

output(sort(a1, -filepos));
output(sort(a1, filepos));

output(a1(filepos > 10));
output(a1(filepos > 20));


