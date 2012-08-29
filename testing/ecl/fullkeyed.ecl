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

//UseStandardFiles
//nolocal

rawfile2 := DATASET([{0x80000000, 'FRED'},{0x80000001, DG_fnames[1]}],{unsigned4 val, STRING DG_FirstName});
rawfile3 := DATASET([{0x80000000, 'FRED'},{0x80000001, DG_fnames[1]}],{unsigned4 val, STRING10 DG_FirstName});

rawfile1 := DG_FlatFile;
index1   := DG_IndexFile;

rawfile2 doJoin1(rawfile2 l, rawfile1 r) := TRANSFORM
            SELF := l;
            END;

rawfile3 doJoin2(rawfile3 l, rawfile1 r) := TRANSFORM
            SELF := l;
            END;


boolean stringsimilar(unsigned4 val, string l, string r) := BEGINC++ return true; ENDC++;

fkjoin1 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin1(LEFT, RIGHT), LEFT OUTER, KEYED(index1));
fkjoin2 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin1(LEFT, RIGHT), LEFT ONLY, KEYED(index1));
fkjoin3 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin1(LEFT, RIGHT), KEYED(index1));

fkjoin4 := JOIN(rawfile3, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin2(LEFT, RIGHT), LEFT OUTER, KEYED(index1));
fkjoin5 := JOIN(rawfile3, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin2(LEFT, RIGHT), LEFT ONLY, KEYED(index1));
fkjoin6 := JOIN(rawfile3, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1] AND stringsimilar(left.val, left.DG_firstname, (string)right.dg_parentid), doJoin2(LEFT, RIGHT), KEYED(index1));

output(SORT(fkjoin1, DG_FirstName));
output(SORT(fkjoin2, DG_FirstName));
output(SORT(fkjoin3, DG_FirstName));

output(SORT(fkjoin4, DG_FirstName));
output(SORT(fkjoin5, DG_FirstName));
output(SORT(fkjoin6, DG_FirstName));
