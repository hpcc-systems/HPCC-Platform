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

shared MyRec := RECORD
STRING3 foo1;
STRING5 foo2;
UNSIGNED1 nullTerm;
END;

test1 := DATASET([{'0123456789'}, {'1234567890'}, {'43210'}], {VARSTRING x});

output(SIZEOF(MyRec));

MyRec xform({VARSTRING x} l) := TRANSFORM
VARSTRING w1 := l.x[1..SIZEOF(MyRec)-1];
self := TRANSFER(w1,MyRec);
END;

t1 := PROJECT(test1, xform(LEFT));

output(t1);

