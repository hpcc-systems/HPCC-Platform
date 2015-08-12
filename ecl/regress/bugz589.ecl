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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string10 per_first_name, string10 per_last_name, data9 per_cid }, thor);
a0 := person(per_last_name='XX');
output(a0,{ (string15) per_first_name, per_cid }, 'RKC::rkc');
a1 := dataset('RKC::rkc', { string15 per_first_name, data9 per_cid }, flat);
/* both syntax should work */
a0 t1(a0 L, a1 R) := transform SELF:=L END;
typeof(a0) t2(a0 L, a1 R) := transform SELF:=L END;

// should at least report better error message
//t3(a0 L, a1 R) := transform SELF:=L END;

integer t4(a0 L, a1 R) := transform SELF:=L END;
