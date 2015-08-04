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



integer4 add1(integer4 x, integer4 y) :=
BEGINC++
return x + y;
ENDC++;

integer4 add2(integer4 x, integer4 y) :=
BEGINC++
#option action
return x + y;
ENDC++;

integer4 add3(integer4 x, integer4 y) :=
BEGINC++
#option pure
return x + y;
ENDC++;

output(add1(10,20) * add1(10,20));
output(add2(10,20) * add2(10,20));
output(add3(10,20) * add3(10,20));
