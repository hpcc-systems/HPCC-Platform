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


r1 :=        RECORD
string20000     f1;
data20000       f2;
string20000     f3;
data20000       f4;
string4000      fa1;
data4000        fa2;
string4000      fa3;
data4000        fa4;
string4000      fb1;
data4000        fb2;
string4000      fb3;
data4000        fb4;
string4000      fc1;
data4000        fc2;
string4000      fc3;
data4000        fc4;
string4000      fd1;
data4000        fd2;
string4000      fd3;
data4000        fd4;
string4000      fe1;
data4000        fe2;
string4000      fe3;
data4000        fe4;
            END;

r1 t(r1 l, r1 r) := transform
    self := r;
    END;

d := dataset('d', r1, thor);

output(ITERATE(d,t(LEFT,RIGHT)));



