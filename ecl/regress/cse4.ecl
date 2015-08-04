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

potentialAlias(unsigned v1, unsigned v2) := (sqrt(v1 * v2));
unsigned forceAlias(unsigned v1, unsigned v2) := potentialAlias(v1, v2) * potentialAlias(v1, v2);

workRecord :=
            RECORD
unsigned        f1;
unsigned        f2;
unsigned        f3;
unsigned        f4;
unsigned        f5;
unsigned        f6;
unsigned        f7;
unsigned        f8;
unsigned        f9;
            END;

ds := dataset('ds', workRecord, thor);

//alias' = if (l.f1 = 1 || l.f2 == 1, alias1, []);
workRecord t1(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    //Same no_mapto, different case
    SELF.f1 := case(l.f1, 2=>99, alias1=>alias1, 56);
    SELF := [];
END;


//alias' = if (l.f1 = 1 || l.f2 == 1, alias1, []);
workRecord t2(workRecord l) := TRANSFORM
    alias1 := forceAlias(l.f1, 1);
    //Same no_mapto, different case
    SELF.f1 := case(l.f1, 2=>99, alias1=>alias1, 56);
    SELF.f2 := case(l.f2, 2=>98, alias1=>alias1, 55);
    SELF := [];
END;


SEQUENTIAL(
    OUTPUT(PROJECT(ds, t1(LEFT))),
    OUTPUT(PROJECT(ds, t2(LEFT))),
    'Done'
);
