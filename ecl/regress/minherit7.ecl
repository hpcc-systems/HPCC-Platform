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

m1 := MODULE,interface
export integer value;
export integer f(integer sc);
        END;


m2 := MODULE(m1)
export value := 20;
        END;

m3 := MODULE(m2)
export f(integer sc) := value * sc + 1;
        END;

output(m3.f(10));       //Correct


