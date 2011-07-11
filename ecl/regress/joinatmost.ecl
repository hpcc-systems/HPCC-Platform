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

iirec := RECORD
    INTEGER i;
    INTEGER j;
END;

iiset1 := DATASET([{1, 1}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {3, 3}, {4, 1}, {4, 2}, {4, 3}, {4, 4}], iirec);

iiset2 := DATASET([{1, 6}, {2, 6}, {2, 7}, {3, 6}, {3, 7}, {3, 8}, {4, 6}, {4, 7}, {4, 8}, {4, 9}], iirec);

iiset3 := DATASET([{1, 6}, {1, 7}, {1, 8}, {1, 9}, {2, 6}, {2, 7}, {2, 8}, {3, 6}, {3, 7}, {4, 6}], iirec);

iirec xform(iirec l, iirec r) := TRANSFORM
    SELF.i := l.i;
    SELF.j := l.j * 10 + r.j;
END;

//plain join with symmetric group sizes (the third of these checks that the filter correctly occurs before the match)

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*1+2*2+3*3+4*4 = 30 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 1*1+2*2+3*3 = 14 records output

OUTPUT(JOIN(iiset1, iiset2, (LEFT.i = RIGHT.i) AND (RIGHT.j < 9), xform(LEFT, RIGHT), ATMOST(LEFT.i = RIGHT.i, 3))); // 1*1+2*2+3*3 = 14 records output

//self join

OUTPUT(JOIN(iiset1, iiset1, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*1+2*2+3*3+4*4 = 30 records output

OUTPUT(JOIN(iiset1, iiset1, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 1*1+2*2+3*3 = 14 records output

//plain join with asymmetric group sizes, check that filter occurs on rhs group size

OUTPUT(JOIN(iiset1, iiset3, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*4+2*3+3*2+4*1 = 20 records output

OUTPUT(JOIN(iiset1, iiset3, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 2*3+3*2+4*1 = 16 records output
