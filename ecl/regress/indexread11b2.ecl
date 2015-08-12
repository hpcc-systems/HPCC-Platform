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

d := dataset('~local::rkc::person', { string15 f1, string15 f2, string15 f3, string15 f4, unsigned8 filepos{virtual(fileposition)} }, flat);

i := index(d, { d } ,'\\home\\person.name_first.key');



//count(i(keyed(f3='Gavin3' and f2='Hawthorn')));   // missing before keyed
count(i(keyed(f3='Gavin3') and f2='Hawthorn')); // missing before keyed after valid field
//count(i(keyed(f3='Gavin3') and wild(f2)));        // missing before a wild
//count(i(wild(f3) and f2='Hawthorn'));             // missing before wild after valid field
