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

//nothor
sqrec := {UNSIGNED1 i, UNSIGNED1 sq};

odds := DATASET([{1, 1}, {3, 9}, {5, 25}, {7, 49}, {9, 91}], sqrec);

evens := DATASET([{2, 4}, {4, 16}, {6, 36}, {8, 64}], sqrec);

sqsetrec := RECORD, MAXSIZE(1000)
    STRING5 label;
    DATASET(sqrec) vals;
END;

in := DATASET([{'odds', odds}, {'evens', evens}], sqsetrec);

namerec := {UNSIGNED1 i, STRING5 str};

names1 := DATASET([{1, 'one'}, {4, 'four'}, {7, 'seven'}, {8, 'eight'}, {9, 'nine'}], namerec);
names2 := DATASET([{2, 'two'}, {3, 'three'}, {4, 'four'}, {9, 'nine'}], namerec);
names3 := DATASET([{5, 'five'}, {6, 'six'}, {7, 'seven'}], namerec);

names := DEDUP(SORT(names1+names2+names3, i), i);

namesqrec := {UNSIGNED1 i, UNSIGNED1 sq, STRING5 str};

namesqsetrec := RECORD, MAXSIZE(1000)
    STRING5 label;
    DATASET(namesqrec) vals;
END;

namesqsetrec getnames(sqsetrec l) := TRANSFORM
    SELF.label := l.label;
    SELF.vals := JOIN(l.vals, names, LEFT.i = RIGHT.i);
END;

out := PROJECT(in, getnames(LEFT));

OUTPUT(out);
