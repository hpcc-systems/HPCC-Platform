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

//Example of two cross-dependent pure module definitions

m1 := MODULE,virtual
export value := 1;
export f(integer sc) := value * sc;
        END;


m2(m1 mp) := MODULE, virtual
export value2 := 10;
export g(integer sc2) := mp.f(value2) + sc2;
        END;


m3 := MODULE(m1)
export value := 7;
        END;

m4(m1 mp) := MODULE(m2(mp))
export value2 := 21;
        END;


output(m4(m3).g(99));       // Expected 246.

