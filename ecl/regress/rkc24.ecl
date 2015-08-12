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

dummyDS := dataset('not::really', { unsigned val, unsigned8 _fpos { virtual(fileposition)}}, thor);

i1 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random');
i2 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random1');
i3 := index(dummyDS, {val, _fpos}, 'thor_data50::key::random2');
i4 := index(dummyDS, {val, _fpos}, 'thor_data50::key::super');

count(i1(val > 100));
count(i2(val > 100));
count(i3(val > 100));
//count(i4(val > 100));
