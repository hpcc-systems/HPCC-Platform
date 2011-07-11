/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


