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
