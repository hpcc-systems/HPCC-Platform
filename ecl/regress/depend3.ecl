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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_first_name, string40 per_last_name }, thor);
d := person(per_first_name='RICHARD');
output(d,{(string20) per_last_name},'rkc::x11');
output(d,{(string20) per_last_name},'rkc::x12');
d1 := dataset('rkc::x11', { string20 per_last_name }, THOR);
d2 := dataset('rkc::x12', { string20 per_last_name }, THOR);
output(choosen(d1(per_last_name='DRIMBAD')+d2(per_last_name='DRIMBAD'),1000000));

