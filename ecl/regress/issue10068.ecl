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

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), SMART)); // 1+2+3 = 6 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), SMART, LEFT OUTER)); // 1+2+3+1 = 7 records output

OUTPUT(JOIN(GROUP(SORT(iiset1b, i), i), iiset2b, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), SMART)); // 1+2=3 records output
