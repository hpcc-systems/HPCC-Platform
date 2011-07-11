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
//UseIndexes
//nolocal

rawfile2 := DATASET([{DG_fnames[1]},{'FRED'}],{STRING10 DG_FirstName});
rawfile1 := DG_FlatFile;
index1   := DG_IndexFile;

rawfile2 doJoin(rawfile2 l) := TRANSFORM
            SELF := l;
            END;

rawfile2 doJoin1(rawfile2 l, rawfile1 r) := TRANSFORM
            SELF := l;
            END;

rawfile2 doJoin2(rawfile2 l) := TRANSFORM
            SELF := l;
            END;

hkjoin1 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin(LEFT), LEFT OUTER);
fkjoin1 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin1(LEFT, RIGHT), LEFT OUTER, KEYED(index1));
hkjoin2 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin(LEFT), LEFT ONLY);
fkjoin2 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin1(LEFT, RIGHT), LEFT ONLY, KEYED(index1));
hkjoin3 := JOIN(rawfile2, index1,   LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin(LEFT));
fkjoin3 := JOIN(rawfile2, rawfile1, LEFT.DG_firstname=RIGHT.DG_firstname AND RIGHT.DG_firstname = DG_fnames[1], doJoin1(LEFT, RIGHT), KEYED(index1));

  output(SORT(hkjoin1, DG_FirstName));
  output(SORT(fkjoin1, DG_FirstName));
  output(SORT(hkjoin2, DG_FirstName));
  output(SORT(fkjoin2, DG_FirstName));
  output(SORT(hkjoin3, DG_FirstName));
  output(SORT(fkjoin3, DG_FirstName));

/*
'First two results are LEFT OUTER and should have n+1 records';
'Next two results are LEFT ONLY and should have only 1 record';
'Next two results are INNER and should have n records';*/
