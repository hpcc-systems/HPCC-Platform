/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

// Decimals with less than 50 digits are output correctly
x := (DECIMAL48_32) '1234567890123456.12345678901234567890123456789012';
y := (DECIMAL48_16) '12345678901234567890123456789012.1234567890123456';
// For digits >= 50, the output was 50 '*' rather than the actual value
i := (DECIMAL49_32) '12345678901234567.12345678901234567890123456789012';
j := (DECIMAL50_32) '123456789012345678.12345678901234567890123456789012';
OUTPUT(x);
OUTPUT(y);
OUTPUT(i);
OUTPUT(j);

a := (DECIMAL64_32) '12345678901234567890123456789012.12345678901234567890123456789012';
// Max length Decimal with 64 digits and a negative sign
b := (DECIMAL64_32) '-12345678901234567890123456789011.12345678901234567890123456789012';
OUTPUT(a);
OUTPUT(b);
OUTPUT(a + b);
