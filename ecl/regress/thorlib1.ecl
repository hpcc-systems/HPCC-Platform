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

import std.system.thorlib;

test1 := RECORD
    INTEGER2 q;
    INTEGER2 p;
END;
test2 := RECORD(test1)
    INTEGER2 g;
END;

t1 := DISTRIBUTE(DATASET([{0,0}, {1,0}, {2,0}, {3,0}, {4,0}

         ,{5,0}, {6,0}, {7,0}, {8,0}, {9,0}

         ,{10,0}, {11,0}, {12,0}, {13,0}, {14,0}

         ,{15,0}, {16,0}, {17,0}, {18,0}, {19,0}

         ,{20,0}, {21,0}, {22,0}, {23,0}, {24,0}

         ,{25,0}, {26,0}, {27,0}, {28,0}, {29,0}

         ,{30,0}, {31,0}, {32,0}, {33,0}, {34,0}

         ,{35,0}, {36,0}, {37,0}, {38,0}, {39,0}

         ,{40,0}, {41,0}, {42,0}, {43,0}, {44,0}

         ,{45,0}, {46,0}, {47,0}, {48,0}, {49,0}

         ,{50,0}, {51,0}, {52,0}, {53,0}, {54,0}

         ,{55,0}, {56,0}, {57,0}, {58,0}, {59,0}

            ], test1), q);
t2 := SORT(t1, q, LOCAL);
test1 stamp(test1 l) := TRANSFORM
    SELF.p := ThorLib.Node();
    SELF := l;
END;
t3 := PROJECT(t2, stamp(LEFT));
OUTPUT(t3);

// test2 stamp2(test1 l) := TRANSFORM
    // SELF.p := ThorLib.node();
    // SELF := l;
    // SELF.g := 101;
// END;
// t4 := PROJECT(t2, stamp2(LEFT));
// OUTPUT(t4);
