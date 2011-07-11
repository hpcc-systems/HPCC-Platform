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

#option('optimizeDiskRead', 0);

tRec1 :=
    record
        String1 S;
        Integer4 K;
        Integer4 H;
        Integer1 B1;
        Integer1 B2;
        String84 FILL;
        Integer4 CRC;
        String1 E;
    end;


tRec3 :=
    record
        Integer4 K1;
        Integer4 K2;
    end;


DS1 := dataset ('~test::testfile2 ', tRec1, flat);



fun1 := DS1&DS1;
fun2 := DS1&DS1;
fun3 := fun1+fun2+DS1;


count(fun3);
