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

rec1 := RECORD
STRING str;
END;
rtypes := ENUM(w1, w2);
rec2 := RECORD
STRING str;
rtypes x;
SET OF INTEGER seq;
END;

rec2 cvt(rec1 l, INTEGER t) := TRANSFORM
SELF.x := t;
SELF.str := l.str;
SELF.seq := [0];
END;
rec2 nbr(rec2 l, INTEGER c) := TRANSFORM
SELF.seq := [c];
SELF := l;
END;

d1 := nofold(DATASET([{'123'}, {'124'}], rec1));
d2 := nofold(DATASET([{'234'}, {'235'}], rec1));
s1 := PROJECT(d1, cvt(LEFT, rtypes.w1));
s2 := PROJECT(d2, cvt(LEFT, rtypes.w2));

s := PROJECT(s1+s2, nbr(LEFT, COUNTER));

OUTPUT(s);

