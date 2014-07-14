/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
