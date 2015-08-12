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


in1 := record
     big_endian integer4 a;
     integer4 b;
     end;


outr := record
     integer4 a;
     big_endian integer4 b;
     integer4 c;
     integer4 d;
     big_endian integer4 e;
     big_endian integer4 F;
     integer4 g;
     big_endian integer4 h;
     integer4 i;
     big_endian integer4 j;
     end;


in1Table := dataset('in1',in1,FLAT);


outr zTransform (in1 l) :=
                TRANSFORM
                    SELF.a := l.a;
                    SELF.b := l.b;
                    SELF.j := l.a;
                    SELF.i := l.b;
                    SELF.c := l.a + l.a;
                    SELF.d := l.b + l.b;
                    SELF.e := l.a + l.a;
                    SELF.f := l.b + l.b;
                    SELF.g := l.a + l.b;
                    SELF.h := l.a + l.b;
                END;

outTable := project(in1Table,zTransform(LEFT));

output(outTable,,'out.d00');


