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

#option ('noAllToLookupConversion', true);

L := RECORD
  unsigned1 rec;
  unsigned1 x;
  unsigned1 y;
  unsigned1 neighbor := 0;
  real4 distance := 0.0;
  unsigned1 comp := 0;
END;

jrec := RECORD,MAXLENGTH(100)
    UNSIGNED1 i;
    STRING3 lstr;
    STRING3 rstr;
    UNSIGNED1 c;
    STRING label;
END;

lhs := SORTED(DATASET([{3, 'aaa', '', 0, ''}, {4, 'bbb', '', 0, ''}, {5, 'ccc', '', 0, ''}, {6, 'ddd', '', 0, ''}, {7, 'eee', '', 0, ''}], jrec), i);
rhs := SORTED(DATASET([{1, '', 'fff', 0, ''}, {3, '', 'ggg', 0, ''}, {5, '', 'hhh', 0, ''}, {5, '', 'iii', 0, ''}, {5, '', 'xxx', 0, ''}, {5, '', 'jjj', 0, ''}, {7, '', 'kkk', 0, ''}, {9, '', 'lll', 0, ''}, {9, '', 'mmm', 0, ''}], jrec), i);
ds := DATASET([{1,1,2},{2,3,2},{3,5,4},{4,3,3}], L);

trueval := true : stored('trueval');
falseval := false : stored('falseval');
BOOLEAN match1(jrec l, jrec r) := (l.i = r.i);
BOOLEAN match2(jrec l, jrec r) := (r.rstr < 'x');
BOOLEAN match(jrec l, jrec r) := (match1(l, r) AND match2(l, r));
BOOLEAN allmatch1(jrec l, jrec r) := ((match1(l, r) AND trueval) OR falseval);
BOOLEAN allmatch(jrec l, jrec r) := (allmatch1(l, r) AND match2(l, r));

jrec xfm(jrec l, jrec r, STRING lab) := TRANSFORM
    SELF.i := l.i;
    SELF.lstr := l.lstr;
    SELF.rstr := r.rstr;
    SELF.c := l.c+1;
    SELF.label := lab;
END;

T1 := DENORMALIZE(ds, ds, LEFT.comp=RIGHT.comp,
                 TRANSFORM(L,
                           SELF.neighbor := IF(LEFT.rec!=RIGHT.rec AND (LEFT.neighbor=0 OR SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2))<LEFT.distance),RIGHT.rec,LEFT.neighbor);
                           SELF.distance := IF(LEFT.rec!=RIGHT.rec AND (LEFT.neighbor=0 OR SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2))<LEFT.distance),SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2)),LEFT.distance);
                           SELF:=LEFT;),
                 MANY LOOKUP);


T2 := DENORMALIZE(ds, ds, LEFT.comp=RIGHT.comp,
                 TRANSFORM(L,
                           SELF.neighbor := IF(LEFT.rec!=RIGHT.rec AND (LEFT.neighbor=0 OR SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2))<LEFT.distance),RIGHT.rec,LEFT.neighbor);
                           SELF.distance := IF(LEFT.rec!=RIGHT.rec AND (LEFT.neighbor=0 OR SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2))<LEFT.distance),SQRT(POWER(LEFT.x-RIGHT.x,2)+POWER(LEFT.y-RIGHT.y,2)),LEFT.distance);
                           SELF:=LEFT;),
                 ALL);


T3 := DENORMALIZE(lhs, rhs, match(LEFT, RIGHT), xfm(LEFT, RIGHT, 'DENORMALIZE_LOOKUP_LEFT_OUTER_PAR'), MANY LOOKUP, LEFT OUTER);
T4 := DENORMALIZE(lhs, rhs, match(LEFT, RIGHT), xfm(LEFT, RIGHT, 'DENORMALIZE_LOOKUP_LEFT_OUTER_PAR'), LOOKUP, LEFT OUTER); 
T5 := DENORMALIZE(lhs, rhs, allmatch(LEFT, RIGHT), xfm(LEFT, RIGHT, 'DENORMALIZE_ALL_LEFT_OUTER_PAR'), ALL, LEFT OUTER);


OUTPUT(T1);
OUTPUT(T2);
OUTPUT(T3);
OUTPUT(T4);
OUTPUT(T5);

