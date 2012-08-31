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

r := record
unsigned        id;
boolean         t1;
boolean         t2;
boolean         t3;
boolean         t4;
boolean         t5;
boolean         t6;
boolean         t7;
boolean         t8;
boolean         t9;
boolean         t10;
boolean         t11;
boolean         t12;
boolean         t13;
boolean         t14;
boolean         t15;
boolean         t16;
boolean         t17;
boolean         t18;
boolean         t19;
boolean         t20;
boolean         t21;
boolean         t22;
boolean         t23;
boolean         t24;
boolean         t25;
boolean         t26;
boolean         t27;
boolean         t28;
boolean         t29;
        end;

ds := dataset('ds', r, thor);

addup(boolean x) := if(x, 1, 0);


r2 := record
unsigned        id;
unsigned1       score;
        end;

r2 t(r l) := transform
self.id := l.id;
self.score :=
        addup(l.t1) + addup(l.t2) + addup(l.t3) + addup(l.t4) + addup(l.t5) + addup(l.t6) + addup(l.t7) + addup(l.t8) + addup(l.t9) +
        addup(l.t10) + addup(l.t11) + addup(l.t12) + addup(l.t13) + addup(l.t14) + addup(l.t15) + addup(l.t16) + addup(l.t17) + addup(l.t18) + addup(l.t19) +
        addup(l.t20) + addup(l.t21) + addup(l.t22) + addup(l.t23) + addup(l.t24) + addup(l.t25) + addup(l.t26) + addup(l.t27) + addup(l.t28) + addup(l.t29);

    end;

output(project(ds, t(LEFT)));


