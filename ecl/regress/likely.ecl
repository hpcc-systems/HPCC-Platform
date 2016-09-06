/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

person := dataset('person', { unsigned8 person_id, string1 per_sex, string40 per_last_name, unsigned8 xpos }, thor);

filtered1 := person( LIKELY(xpos < 1000) );

filter2 := filtered1( LIKELY(person_id > 1000, 0.01) ) ;
filter3 := filtered1( LIKELY(per_last_name = 'Hawthorn', 0.99) ) ;
filter4 := filtered1( UNLIKELY(per_last_name != 'Drimbad' AND per_sex = 'F' ) );
filter5 := filtered1( UNLIKELY(per_last_name = 'Drimbad') );

output(filter2,,'tst_likely.d01');
output(filter3,,'tst_likely.d02');
output(filter4,,'tst_likely.d03');
output(filter5,,'tst_likely.d04');

