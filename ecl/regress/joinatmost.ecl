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

iirec := RECORD
    INTEGER i;
    INTEGER j;
END;

iiset1 := DATASET([{1, 1}, {2, 1}, {2, 2}, {3, 1}, {3, 2}, {3, 3}, {4, 1}, {4, 2}, {4, 3}, {4, 4}], iirec);

iiset2 := DATASET([{1, 6}, {2, 6}, {2, 7}, {3, 6}, {3, 7}, {3, 8}, {4, 6}, {4, 7}, {4, 8}, {4, 9}], iirec);

iiset3 := DATASET([{1, 6}, {1, 7}, {1, 8}, {1, 9}, {2, 6}, {2, 7}, {2, 8}, {3, 6}, {3, 7}, {4, 6}], iirec);

iirec xform(iirec l, iirec r) := TRANSFORM
    SELF.i := l.i;
    SELF.j := l.j * 10 + r.j;
END;

//plain join with symmetric group sizes (the third of these checks that the filter correctly occurs before the match)

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*1+2*2+3*3+4*4 = 30 records output

OUTPUT(JOIN(iiset1, iiset2, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 1*1+2*2+3*3 = 14 records output

OUTPUT(JOIN(iiset1, iiset2, (LEFT.i = RIGHT.i) AND (RIGHT.j < 9), xform(LEFT, RIGHT), ATMOST(LEFT.i = RIGHT.i, 3))); // 1*1+2*2+3*3 = 14 records output

//self join

OUTPUT(JOIN(iiset1, iiset1, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*1+2*2+3*3+4*4 = 30 records output

OUTPUT(JOIN(iiset1, iiset1, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 1*1+2*2+3*3 = 14 records output

//plain join with asymmetric group sizes, check that filter occurs on rhs group size

OUTPUT(JOIN(iiset1, iiset3, LEFT.i = RIGHT.i, xform(LEFT, RIGHT))); // 1*4+2*3+3*2+4*1 = 20 records output

OUTPUT(JOIN(iiset1, iiset3, LEFT.i = RIGHT.i, xform(LEFT, RIGHT), ATMOST(3))); // 2*3+3*2+4*1 = 16 records output
