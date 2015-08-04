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

unsigned8 bigUnsigned := 18446744073709551615;

integer1 intTen := 10;
unsigned1 uTen := 10;

bigUnsigned % intTen; // -1
bigUnsigned % uTen; // -1

(unsigned8)bigUnsigned % intTen; // -1
(unsigned8)bigUnsigned % uTen; // -1

bigUnsigned % (unsigned1)intTen; // -1
bigUnsigned % (unsigned1)uTen; // -1

(unsigned8)bigUnsigned % (unsigned1)intTen; // -1 (unsigned8)bigUnsigned % (unsigned1)uTen; // -1

// casting to unsigned8 produces correct result, but casting to any other unsigned does not bigUnsigned % (unsigned8)intTen; // 5 bigUnsigned % (unsigned8)uTen; // 5

(unsigned8)bigUnsigned % (unsigned8)intTen; // 5 (unsigned8)bigUnsigned % (unsigned8)uTen; // 5
