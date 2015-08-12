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



integer8 x1(string5 a) := case(a,
            'one  '=>1,
            'two  '=>2,
            'three'=>3,
            'four '=>4,
            'five '=>5,
            'six  '=>6,
            'seven'=>7,
            'eight'=>8,
            'nine '=>9,
            'ten  '=>10,
            0);

five := 'five' : stored('five');

output(x1(five));

integer8 x2(string5 a) := case(a,
            'eight'=>1,
            'five '=>2,
            'four '=>3,
            'nine '=>4,
            'one  '=>5,
            'seven'=>6,
            'six  '=>7,
            'ten  '=>8,
            'three'=>9,
            'two  '=>10,
            0);


output(x2(five));

integer8 x3(string5 a) := case(a,
            'eight'=>10000000000000,
            'five '=>20000000000000,
            'four '=>30000000000000,
            'nine '=>40000000000000,
            'one  '=>50000000000000,
            'seven'=>60000000000000,
            'six  '=>70000000000000,
            'ten  '=>80000000000000,
            'three'=>90000000000000,
            'two  '=>100000000000000,
            0);


output(x3(five));

integer2 x4(string5 a) := case(a,
            'eight'=>1,
            'five '=>2,
            'four '=>3,
            'nine '=>4,
            'one  '=>5,
            'seven'=>6,
            'six  '=>7,
            'ten  '=>8,
            'three'=>9,
            'two  '=>10,
            0);


output(x4(five));
