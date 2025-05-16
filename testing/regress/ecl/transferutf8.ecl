/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

// HPCC-34099

IMPORT Std;

STRING STATIC_STRING_VAL := '4D617274696E657A205065C3B161';
STRING DYNAMIC_STRING_VAL := '4D617274696E657A205065C3B161' : STORED('x');

name_str_1 := (STRING)Std.Str.FromHexPairs(STATIC_STRING_VAL);
name_1 := (>UTF8<)name_str_1;

name_str_2 := NOFOLD((STRING)Std.Str.FromHexPairs(STATIC_STRING_VAL));
name_2 := (>UTF8<)name_str_2;

name_str_3 := (STRING)Std.Str.FromHexPairs(DYNAMIC_STRING_VAL);
name_3 := (>UTF8<)name_str_3;

// Test known, constant-length data source
resultStr1 := IF(LENGTH(name_1) = LENGTH(name_2), 'PASSED', 'FAILED');
OUTPUT(resultStr1, NAMED('result_1'));

// Test unknown length data source
resultStr2 := IF(LENGTH(name_1) = LENGTH(name_3), 'PASSED', 'FAILED');
OUTPUT(resultStr2, NAMED('result_2'));
