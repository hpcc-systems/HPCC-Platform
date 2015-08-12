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

unsigned8 max_u8 := 18446744073709551615; // 2^64 - 1
unsigned8 max_i8 := 9223372036854775807; // 2^63 - 1
unsigned8 minbad := max_i8+1; // 2^63
unsigned8 max_u8s := 18446744073709551615 : independent; // 2^64 - 1

output(max_u8, named('max_u8'));
output((real8)max_u8, named('r_max_u8')); // bad
output((real8)max_u8s, named('r_max_u8s'));

output(max_i8, named('max_i8'));
output((real8)max_i8, named('r_max_i8')); // good

output(minbad, named('minbad'));
output((real8)minbad, named('r_minbad')); // bad

output((decimal30_0)max_u8, named('d_max_u8')); // bad
output((utf8)max_u8, named('u8_max_u8')); // bad
output((unicode)max_u8, named('u_max_u8')); // bad
output((string)(qstring)max_u8, named('q_max_u8')); // bad

output((decimal30_0)max_u8s, named('d_max_u8s')); // bad


output((packed unsigned)max_u8s, named('p_max_u8s')); // bad
