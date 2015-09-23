/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.

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

sys := SERVICE : fold

integer3 rtlCastInt3(integer4 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastInt3');
integer5 rtlCastInt5(integer8 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastInt5');
integer7 rtlCastInt7(integer8 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastInt7');
unsigned3 rtlCastUInt3(unsigned4 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastUInt3');
unsigned5 rtlCastUInt5(unsigned8 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastUInt5');
unsigned7 rtlCastUInt7(unsigned8 value) : pure,cpp,library('eclrtl'),entrypoint('rtlCastUInt7');
    END;


'int3';
output(sys.rtlCastInt3(10));
output(sys.rtlCastInt3(-10));
output(sys.rtlCastInt3((0x1000000+10)));
output(sys.rtlCastInt3(-(0x1000000+10)));
output(sys.rtlCastInt3(NOFOLD(10)));
output(sys.rtlCastInt3(NOFOLD(-10)));
output(sys.rtlCastInt3(NOFOLD((0x1000000+10))));
output(sys.rtlCastInt3(NOFOLD(-(0x1000000+10))));

'int5';
output(sys.rtlCastInt5(10));
output(sys.rtlCastInt5(-10));
output(sys.rtlCastInt5((0x10000000000+10)));
output(sys.rtlCastInt5(-(0x10000000000+10)));
output(sys.rtlCastInt5(NOFOLD(10)));
output(sys.rtlCastInt5(NOFOLD(-10)));
output(sys.rtlCastInt5(NOFOLD((0x10000000000+10))));
output(sys.rtlCastInt5(NOFOLD(-(0x10000000000+10))));

'int7';
output(sys.rtlCastInt7(10));
output(sys.rtlCastInt7(-10));
output(sys.rtlCastInt7((0x100000000000000+10)));
output(sys.rtlCastInt7(-(0x100000000000000+10)));
output(sys.rtlCastInt7(NOFOLD(10)));
output(sys.rtlCastInt7(NOFOLD(-10)));
output(sys.rtlCastInt7(NOFOLD((0x100000000000000+10))));
output(sys.rtlCastInt7(NOFOLD(-(0x100000000000000+10))));

'uint3';
output(sys.rtlCastUInt3(10));
output(sys.rtlCastUInt3(-10));
output(sys.rtlCastUInt3((0x1000000+10)));
output(sys.rtlCastUInt3(-(0x1000000+10)));
output(sys.rtlCastUInt3(NOFOLD(10)));
output(sys.rtlCastUInt3(NOFOLD(-10)));
output(sys.rtlCastUInt3(NOFOLD((0x1000000+10))));
output(sys.rtlCastUInt3(NOFOLD(-(0x1000000+10))));

'uint5';
output(sys.rtlCastUInt5(10));
output(sys.rtlCastUInt5(-10));
output(sys.rtlCastUInt5((0x10000000000+10)));
output(sys.rtlCastUInt5(-(0x10000000000+10)));
output(sys.rtlCastUInt5(NOFOLD(10)));
output(sys.rtlCastUInt5(NOFOLD(-10)));
output(sys.rtlCastUInt5(NOFOLD((0x10000000000+10))));
output(sys.rtlCastUInt5(NOFOLD(-(0x10000000000+10))));

'uint7';
output(sys.rtlCastUInt7(10));
output(sys.rtlCastUInt7(-10));
output(sys.rtlCastUInt7((0x100000000000000+10)));
output(sys.rtlCastUInt7(-(0x100000000000000+10)));
output(sys.rtlCastUInt7(NOFOLD(10)));
output(sys.rtlCastUInt7(NOFOLD(-10)));
output(sys.rtlCastUInt7(NOFOLD((0x100000000000000+10))));
output(sys.rtlCastUInt7(NOFOLD(-(0x100000000000000+10))));
