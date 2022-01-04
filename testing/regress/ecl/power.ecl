/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

REAL    real_const := 1.123456;
REAL    rVal := real_const : STORED('real_stored');

OUTPUT(POWER(rVal,0), named('pow_0'));
OUTPUT(POWER(rVal,1), named('pow_1'));
OUTPUT(POWER(rVal,2), named('pow_2'));
OUTPUT(POWER(rVal,3), named('pow_3'));
OUTPUT(POWER(rVal,4), named('pow_4'));
OUTPUT(POWER(rVal,-1), named('pow_n1'));
OUTPUT(POWER(rVal,-2), named('pow_n2'));
OUTPUT(POWER(rVal,-3), named('pow_n3'));
OUTPUT(POWER(rVal,-4), named('pow_n4'));