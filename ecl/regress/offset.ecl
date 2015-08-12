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

Import dt;

r1 := RECORD
    unsigned4       f1;
      END;

d1 := DATASET('d1', r1, FLAT);


r2 := RECORD
    unsigned4       f1;
    unsigned4       f2;
      END;

d2 := DATASET('d2', r2, FLAT);


r3 := RECORD
unsigned4       f1;
                ifblock(self.f1 & 1 != 0)
unsigned4           f2;
                end;
      END;

d3 := DATASET('d3', r3, FLAT);


r4 := RECORD
unsigned4       f1;
                ifblock(self.f1 & 1 != 0)
unsigned4           f2;
                    ifblock(self.f1 & 2 != 0)
unsigned4               f3 := 0;
                    end;
                end;
      END;

d4 := DATASET('d4', r4, FLAT);


r5 := RECORD
unsigned4       f1;
                ifblock(self.f1 & 1 != 0)
unsigned4           f2;
                    ifblock(self.f1 & 2 != 0)
unsigned4               f3 := 0;
                    end;
unsigned4           f4;
                end;
unsigned4       f5;
      END;

d5 := DATASET('d5', r5, FLAT);


r1 t21(r2 l) :=
    TRANSFORM
        self.f1 := l.f2;
    END;

r2 t12(r1 l) :=
    TRANSFORM
        self.f1 := l.f1;
        self.f2 := l.f1;
    END;

r3 t43(r4 l) :=
    TRANSFORM
        self := l;
    END;


r4 t34(r3 l) :=
    TRANSFORM
        self := l;
    END;

r4 t44(r4 l) :=
    TRANSFORM
        self := l;
    END;

r5 t55(r5 l) :=
    TRANSFORM
        self := l;
    END;




o1 := PROJECT(d2,t21(LEFT));
//output(o1,,'o1.d00');
o2 := PROJECT(d1,t12(LEFT));
//output(o2,,'o2.d00');
o3 := PROJECT(d4,t43(LEFT));
//output(o3,,'o3.d00');
o4 := PROJECT(d3,t34(LEFT));
//output(o4,,'o4.d00');
o4b := PROJECT(d4,t44(LEFT));
output(o4b,,'o4.d00');
//output(o4b,,'o4.d00');
o5 := PROJECT(d5,t55(LEFT));
//output(o4,,'o4.d00');



