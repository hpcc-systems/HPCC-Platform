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
