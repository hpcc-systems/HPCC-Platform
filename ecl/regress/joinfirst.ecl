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
