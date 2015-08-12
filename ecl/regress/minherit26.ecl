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

int_a := interface
    export unsigned aa;
    export string ab;
end;

mod_a := module(int_a)
    export unsigned aa := 10;
    export string ab := 'HELLO';
end;

int_b := interface
    export unsigned aa;
    export string ab;
    export real bc;
end;

fun_c(int_b in_mod) := function
    return in_mod.aa * in_mod.bc;
end;

fun_d(int_a in_mod) := function
    tempmodf := module(in_mod)
        export real bc := 5.0;
    end;
    return in_mod.ab + (string)fun_c(project(tempmodf,int_b));
end;

output(fun_d(mod_a));
