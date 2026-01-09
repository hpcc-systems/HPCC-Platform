/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

// HPCC-23274

LAYOUT := RECORD
    UNSIGNED1   person_id;
    STRING      first_name;
    STRING      last_name;
END;

ds := DATASET([{1, 'Jane', 'Doe'}, {2, '', 'Doe'}], LAYOUT);

// Without ALL flag
OUTPUT(IF(TOJSON(ds[1]) = u8'"person_id": 1, "first_name": "Jane", "last_name": "Doe"', 'PASSED', 'FAILED'), NAMED('result_1'));
OUTPUT(IF(TOJSON(ds[2]) = u8'"person_id": 2, "last_name": "Doe"', 'PASSED', 'FAILED'), NAMED('result_2'));

// With ALL flag
OUTPUT(IF(TOJSON(ds[1], ALL) = u8'"person_id": 1, "first_name": "Jane", "last_name": "Doe"', 'PASSED', 'FAILED'), NAMED('result_3'));
OUTPUT(IF(TOJSON(ds[2], ALL) = u8'"person_id": 2, "first_name": "", "last_name": "Doe"', 'PASSED', 'FAILED'), NAMED('result_4'));
