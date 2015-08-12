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

// checkRecord

ds1 := dataset('ds',{string10 first_name; string10 last_name; }, flat);
ds2 := dataset('ds',{string10 first_name; }, flat);
ds3 := dataset('ds',{integer first_name; string10 last_name; }, flat);

add(virtual dataset d1, virtual dataset d2) := d1+d2;

// different field number
output(add(ds1(first_name = 'tom'), ds2(first_name='john')));

// type mismatch
output(add(ds1, ds3));
