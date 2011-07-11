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

ijrec := RECORD
    INTEGER4 i;
    INTEGER4 j;
END;

ij1 := DATASET([{1, 1}, {1, 2}, {1, 3}, {1, 4}, {2, 1}, {2, 2}, {2, 3}, {2, 4}, {3, 1}, {3, 2}, {3, 3}, {3, 4}, {4, 1}, {4, 2}, {4, 3}, {4, 4}], ijrec);

ij2 := DATASET([{1, 6}, {1, 7}, {1, 8}, {1, 9}, {2, 6}, {2, 7}, {2, 8}, {2, 9}, {3, 6}, {3, 7}, {3, 8}, {3, 9}, {4, 6}, {4, 7}, {4, 8}, {4, 9}], ijrec);

ijrec xform(ijrec l, ijrec r) := TRANSFORM
    SELF.i := l.i;
    SELF.j := l.j*10 + r.j;
END;

//plain joins

OUTPUT(JOIN(ij1, ij2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 64 records output

//self joins

OUTPUT(JOIN(ij1, ij1, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 64 records output
