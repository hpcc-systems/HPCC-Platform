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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
#option ('globalFold', false);
Import dt;

rec1 := record
    big_endian integer3     i3;
    unsigned3       u3;
    integer5        i5;
    unsigned5       u5;
    big_endian integer6     i6;
    unsigned6       u6;
    integer7        i7;
    unsigned7       u7;
    big_endian integer8     i8;
    unsigned8       u8;
    end;



table1 := dataset([{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1}], rec1);

rec1 t1(rec1 l) := TRANSFORM
        SELF.u3 := l.i3;
        SELF.i3 := l.u3;
        SELF.i6 := l.i3;
        SELF.u6 := l.u3;
        SELF := l;
    END;

rec1 t2(rec1 l) := TRANSFORM
        SELF := l;
    END;

rec1 t3(rec1 l) := TRANSFORM
        SELF := l;
    END;

table2 := project(table1, t1(LEFT));

table4 := sort(table2, {i3,u3,i6,u6});
//table3 := project(table2, t2(LEFT));

//table4 := project(table3, t3(LEFT));

output(table4(i3!=5),,'out.d00');
