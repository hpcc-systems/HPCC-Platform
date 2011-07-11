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

//skip type==thor TBD

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

Simple tn(Simple l, dataset(Simple) allRows) := transform
    self.letter := min(allRows, letter);
    self := l;
end;

Simple t2(Simple l, Simple r) := transform
    self.letter := IF(r.letter <> '', min(l.letter, r.letter), l.letter);
    self := l;
end;

f3in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter));
f3oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter));
f3ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter));

f4in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter));
f4oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter));
f4ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter));

f5in := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT));
f5oy := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT ONLY);
f5ou := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT OUTER);

f6in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number));
f6oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number));
f6ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number));

f7in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number));
f7oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number));
f7ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number));

OUTPUT(f3in);
OUTPUT(f4in);
OUTPUT(sort(f5in, seg, letter, number));
OUTPUT(f6in);
OUTPUT(f7in);

OUTPUT(f3oy);
OUTPUT(f4oy);
OUTPUT(sort(f5oy, seg, letter, number));
OUTPUT(f6oy);
OUTPUT(f7oy);

OUTPUT(f3ou);
OUTPUT(f4ou);
OUTPUT(sort(f5ou, seg, letter, number));
OUTPUT(f6ou);
OUTPUT(f7ou);

g3in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter));
g3oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter));
g3ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter));

g4in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter));
g4oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter));
g4ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter));

g5in := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT));
g5oy := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT ONLY);
g5ou := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT OUTER);

g6in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number));
g6oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number));
g6ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number));

g7in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number));
g7oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number));
g7ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number));

OUTPUT(g3in);
OUTPUT(g4in);
OUTPUT(sort(g5in, seg, letter, number));
OUTPUT(g6in);
OUTPUT(g7in);

OUTPUT(g3oy);
OUTPUT(g4oy);
OUTPUT(sort(g5oy, seg, letter, number));
OUTPUT(g6oy);
OUTPUT(g7oy);


OUTPUT(g3ou);
OUTPUT(g4ou);
OUTPUT(g5ou);
OUTPUT(g6ou);
OUTPUT(g7ou);
