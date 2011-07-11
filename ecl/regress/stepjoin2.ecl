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

Simple := RECORD
    INTEGER number;
    STRING1 letter;
    UNSIGNED seg;
END;

smallTest(INTEGER v1, INTEGER v2) := v1 = v2;

f1 := DATASET([{1,'A',10},{1,'B',10},{3,'C',10},{1,'D',10},{1,'E',10},
               {1,'F',11},{1,'G',11}, {1,'H',11},{1,'I',11},{1,'J',11}], Simple);
f2 := DATASET([{2,'A',10},{1,'B',10},{2,'C',10},{1,'D',10},{2,'E',10},
               {2,'F',11},{2,'G',11},{2,'H',11},{2,'I',11},{2,'J',11}], Simple);


f3in := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND count(rows(left)) < 3 AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter));
f3oy := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter));
f3ou := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter));

f4in := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter));
f4oy := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter));
f4ou := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter));

f6in := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter, number));
f6oy := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter, number));
f6ou := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter, number));

f7in := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter, number));
f7oy := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter, number));
f7ou := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter, number));

OUTPUT(f3in);
OUTPUT(f4in);
OUTPUT(f6in);
OUTPUT(f7in);

OUTPUT(f3oy);
OUTPUT(f4oy);
OUTPUT(f6oy);
OUTPUT(f7oy);

OUTPUT(f3ou);
OUTPUT(f4ou);
OUTPUT(f6ou);
OUTPUT(f7ou);

g3in := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter));
g3oy := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter));
g3ou := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter));

g4in := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter));
g4oy := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter));
g4ou := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter));

g6in := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter, number));
g6oy := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter, number));
g6ou := MERGEJOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter, number));

g7in := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), SORTED(seg, letter, number));
g7oy := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), LEFT ONLY, SORTED(seg, letter, number));
g7ou := MERGEJOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), LEFT OUTER, SORTED(seg, letter, number), internal(0x80000000),partition(seg));

OUTPUT(g3in);
OUTPUT(g4in);
OUTPUT(g6in);
OUTPUT(g7in);

OUTPUT(g3oy);
OUTPUT(g4oy);
OUTPUT(g6oy);
OUTPUT(g7oy);

OUTPUT(g3ou);
OUTPUT(g4ou);
OUTPUT(g6ou);
OUTPUT(g7ou);
