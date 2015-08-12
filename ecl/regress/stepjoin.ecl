/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

Simple tn(Simple l, dataset(Simple) allRows) := transform
    self.letter := min(allRows, letter);
    self := l;
end;

Simple t2(Simple l, Simple r) := transform
    self.letter := IF(r.letter <> '', min(l.letter, r.letter), l.letter);
    self := l;
end;

f3in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND count(rows(left)) < 3 and smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter), assert sorted, LOCAL);
f3oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter), assert sorted, LOCAL);
f3ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter), assert sorted, LOCAL);

f4in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter), LOCAL);
f4oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter), LOCAL);
f4ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter), LOCAL);

f5in := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LOCAL);
f5oy := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT ONLY, LOCAL);
f5ou := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT OUTER, LOCAL);

f6in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number), LOCAL);
f6oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number), LOCAL);
f6ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number), LOCAL);

f7in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number), LOCAL);
f7oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number), LOCAL);
f7ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number), LOCAL);

OUTPUT(f3in);
OUTPUT(f4in);
OUTPUT(f5in);
OUTPUT(f6in);
OUTPUT(f7in);

OUTPUT(f3oy);
OUTPUT(f4oy);
OUTPUT(f5oy);
OUTPUT(f6oy);
OUTPUT(f7oy);

OUTPUT(f3ou);
OUTPUT(f4ou);
OUTPUT(f5ou);
OUTPUT(f6ou);
OUTPUT(f7ou);

g3in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter), LOCAL);
g3oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter), LOCAL);
g3ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter), LOCAL);

g4in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter), LOCAL);
g4oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter), LOCAL);
g4ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter), LOCAL);

g5in := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LOCAL);
g5oy := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT ONLY, LOCAL);
g5ou := JOIN(f1, f2, LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), t2(LEFT, RIGHT), LEFT OUTER, LOCAL);

g6in := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number), LOCAL);
g6oy := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number), LOCAL);
g6ou := JOIN([f1, f2], STEPPED(LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter) AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number), LOCAL);

g7in := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), SORTED(seg, letter, number), LOCAL);
g7oy := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT ONLY, SORTED(seg, letter, number), LOCAL);
g7ou := JOIN([f1, f2], LEFT.seg = RIGHT.seg AND LEFT.letter = RIGHT.letter AND smallTest(LEFT.number, RIGHT.number), tn(LEFT, ROWS(LEFT)), LEFT OUTER, SORTED(seg, letter, number), LOCAL);

OUTPUT(g3in);
OUTPUT(g4in);
OUTPUT(g5in);
OUTPUT(g6in);
OUTPUT(g7in);

OUTPUT(g3oy);
OUTPUT(g4oy);
OUTPUT(g5oy);
OUTPUT(g6oy);
OUTPUT(g7oy);


OUTPUT(g3ou);
OUTPUT(g4ou);
OUTPUT(g5ou);
OUTPUT(g6ou);
OUTPUT(g7ou);
