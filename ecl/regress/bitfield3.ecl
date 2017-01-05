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

in1rec := RECORD
    UNSIGNED1 id;
    BITFIELD4_1 age1;
    BITFIELD4_1 age2;
END;

in1 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in1rec);

output(in1);

in2rec := RECORD
    UNSIGNED1 id;
    BITFIELD4_8 age1;
    BITFIELD4_8 age2;
END;

in2 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in2rec);

output(in2);

in3rec := RECORD
    UNSIGNED1 id;
    BITFIELD4 age1;
    BITFIELD12 age2;
END;

in3 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in3rec);

output(in3);

in4rec := RECORD
    UNSIGNED1 id;
    BITFIELD4_2 age1;
    BITFIELD12_2 age2;
END;

in4 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in4rec);

output(in4);
