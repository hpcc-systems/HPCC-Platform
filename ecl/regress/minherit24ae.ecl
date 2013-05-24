/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

//A variation on local derivation (minherit4)
//Current problems:
//1) Bound parameter will be concrete not abstract
//2) Cloning and overriding would need to be handled in bind phase (ok if common functions extracted)
//3) Would need a no_param(x, concreteAtom) also bound when a param is bound (or possible concrete(param)), so that uses could be distringuished


m1 := MODULE,virtual
export value1 := 3;
export value2;
export f := value1 * value2;
        END;


f(m1 mp) := function

    mBase :=    project(mp, m1,value1);

    mLocal :=   module(mBase)
        export value1 := 100;
                end;

    return mLocal.f;
end;


m4 := MODULE(m1)
export value2 := 21;
        END;


output(f(m4));      // Expected 2100
