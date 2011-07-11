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
    INTEGER4 i;
    INTEGER4 j;
END;

iiset1 := DATASET([{1, 1}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {3, 3}, {4, 4}], iirec);

iiset2 := DATASET([{1, 6}, {2, 6}, {2, 7}, {3, 6}, {3, 7}, {3, 8}], iirec);

iiset2b := DATASET([{1, 6}, {3, 6}, {3, 7}], iirec);

iiset1b := DATASET([{1, 1}, {3, 1}, {2, 9}, {9, 9}, {3, 2}, {4, 4}], iirec);

iiset1c := DATASET([{1, 1}, {3, 1}, {8, 9}, {9, 9}, {3, 2}, {4, 4}], iirec);

iirec xform(iirec l, iirec r) := TRANSFORM
    SELF.i := l.i * 10 + r.i;
    SELF.j := l.j * 10 + r.j;
END;

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), MANY LOOKUP)); // 1+2+3 = 6 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), LOOKUP, LEFT OUTER)); // 1+2+3+1 = 7 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), LOOKUP, LEFT ONLY)); // 1 record output

OUTPUT(JOIN(GROUP(SORT(iiset1b, i), i), iiset2b, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), LOOKUP)); // 1+2=3 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i <= RIGHT.i, xform(LEFT, RIGHT), ALL)); // 6+5+5+3+3+3=25 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i <= RIGHT.i, xform(LEFT, RIGHT), ALL, LEFT OUTER)); // 6+5+5+3+3+3+1=26 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i <= RIGHT.i, xform(LEFT, RIGHT), ALL, LEFT ONLY)); // 1 record output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i <= RIGHT.i, xform(LEFT, RIGHT), ALL, KEEP(4))); // 4+4+4+3+3+3=21 records output

OUTPUT(JOIN(iiset2, iiset1, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL)); // 1+3+3+6+6+6=25 records output

//OUTPUT(JOIN(iiset2, iiset1, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL, RIGHT OUTER)); // 1+3+3+6+6+6+1=26 records output

//OUTPUT(JOIN(iiset2b, iiset1c, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL, RIGHT OUTER)); // 1+3+3+1+1+1=10 records output (in one group)

//OUTPUT(JOIN(GROUP(SORT(iiset2b, i), i), iiset1c, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL, RIGHT OUTER)); // 1+3+3+1+1+1=10 records output (in 5 groups)

//OUTPUT(JOIN(iiset2, iiset1, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL, RIGHT ONLY)); // 1 record output

OUTPUT(JOIN(iiset2, iiset1, LEFT.i >= RIGHT.i, xform(LEFT, RIGHT), ALL, KEEP(4))); // 1+3+3+4+4+4=19 records output

OUTPUT(JOIN(GROUP(iiset1b, i), iiset2b, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ALL)); // 1+2+2=5 records output (n.b. = not <=)
