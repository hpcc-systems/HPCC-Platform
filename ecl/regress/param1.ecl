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

export abc(unsigned x, unsigned y) := module
    export f1(unsigned a) := x*a;
    export f2(unsigned a, unsigned b = y) := a*b;
    export f3(unsigned a, unsigned x = x) := a*x;
end;

abc(100,5).f1(9);   // 900
abc(100,5).f2(9);   // 45
abc(100,5).f3(9);   // 900
