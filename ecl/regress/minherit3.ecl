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

//Derivation, and binding a scope parameter to another scope parameter.

m1 := MODULE,virtual
export value := 1;
export f(integer sc) := value * sc;
        END;


m2(m1 mp) := MODULE, interface
export value2 := 10;
export g() := mp.value * value2;
        END;


m3(m1 mp2) := MODULE(m2(mp2))
export value2 := 7;
        END;

m4 := MODULE(m1)
export value := 21;
        END;


output(m3(m4).g());     // Expected 147

