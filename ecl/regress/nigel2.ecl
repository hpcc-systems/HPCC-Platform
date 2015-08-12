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

tRec1 :=
        record
                String15 K;
                String10 F1;
        end;

tRec2 :=
        record
                String5  F2;
                String15 K;
        end;

tRec3 :=
        record
                String15 K;
                String10 F1;
                String5  F2;
        end;

DS1 := dataset ('test01.d00', tRec1, flat);
DS2 := dataset ('test02.d00', tRec2, flat);

tRec3 JT (tRec1 l, tRec2 r) :=
    transform
       self.K := l.K;
       self.F1 := l.F1;
       self.F2 := r.F2;
    end;

J := join (DS1, DS2, LEFT.K[1..1] = RIGHT.K[1..1], JT (LEFT, RIGHT));

output (choosen (J, 100),,'test01.out');
