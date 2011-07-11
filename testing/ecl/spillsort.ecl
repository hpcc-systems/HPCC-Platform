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
//noRoxie
//nothor
#option ('hthorSpillThreshold',3000)

indexrec := RECORDOF(DG_indexFileEvens);
outputrec := { String10 DG_firstname; String10 DG_lastname, unsigned1 DG_prange, unsigned8 filepos};

outrow := record
    string30 name;      //test description
    string10 DG_firstname;
    string10 DG_lastname;
    unsigned1 DG_prange;
    DATASET(outputrec) rightrecs;
  END;

outrow tooutrow(DG_flatfile L, string name) := TRANSFORM
  SELF.name := name;
    SELF.rightrecs := [];
    SELF := L;
END;

SkipFilter := [DG_Fnames[4], DG_Fnames[5],DG_Fnames[6],DG_Fnames[7],DG_Fnames[8],DG_Fnames[9],DG_Fnames[10],
               DG_Fnames[11],DG_Fnames[12],DG_Fnames[13],DG_Fnames[14],DG_Fnames[15],DG_Fnames[16]];

outrow addRow(outrow L, DG_indexFileEvens R, integer c) := TRANSFORM
    self.rightrecs := L.rightrecs + PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;

outrow addRowSkip(outrow L, DG_indexFileEvens R, integer c) := TRANSFORM
    self.DG_firstname := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname);
    self.rightrecs := L.rightrecs + PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;


outrow makeRow(DG_FlatFile L, dataset(indexrec) R, string name) := TRANSFORM
    self.name := name;
    self.rightrecs := PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;

outrow makeRowSkip(DG_FlatFile L, dataset(indexrec) R, string name) := TRANSFORM
    self.name := name;
    self.DG_firstname := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname);
    self.rightrecs := PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;


Out1 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER));
Out2 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed skip')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER));
Out3 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed keep(2)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2));
Out4 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed atmost(3)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3));
Out5 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed limit(3,skip)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP));

Out6 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), INNER);
Out7 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner skip')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), INNER);
Out8 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner keep(2)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), INNER);
Out9 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner atmost(3)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), INNER);
Out10 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner limit(3,skip)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), INNER);

Out11 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), LEFT OUTER);
Out12 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer skip')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), LEFT OUTER);
Out13 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer keep(2)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), LEFT OUTER);
Out14 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer atmost(3)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), LEFT OUTER);
Out15 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer limit(3,skip)')), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), LEFT OUTER);


// Unkeyed DenormalizeGroup, with INNER, LEFT OUTER, and unspecified (should be same as LEFT OUTER)

out101 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup'));
out102 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup skip'));
out103 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup keep(2)'), KEEP(2));
out104 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup atmost(3))'), ATMOST(3));
out105 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup LIMIT(3,SKIP))'), LIMIT(3,SKIP));

out106 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner'), INNER);
out107 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup, Inner skip'), INNER);
out108 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner keep(2)'), INNER, KEEP(2));
out109 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner atmost(3))'), INNER, ATMOST(3));
out110 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner LIMIT(3,SKIP))'), INNER, LIMIT(3,SKIP));

out111 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer'), LEFT OUTER);
out112 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup, Outer skip'), LEFT OUTER);
out113 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer keep(2)'), LEFT OUTER, KEEP(2));
out114 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer atmost(3))'), LEFT OUTER, ATMOST(3));
out115 :=DENORMALIZE(DG_FlatFile, DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer LIMIT(3,SKIP))'), LEFT OUTER, LIMIT(3,SKIP));


output('Keyed Denormalize');
output(SORT(Out1,record));
output(SORT(Out2,record));
output(SORT(Out3,record));
output(SORT(Out4,record));
output(SORT(Out5,record));
output(SORT(Out6,record));
output(SORT(Out7,record));
output(SORT(Out8,record));
output(SORT(Out9,record));
output(SORT(Out10,record));
output(SORT(Out11,record));
output(SORT(Out12,record));
output(SORT(Out13,record));
output(SORT(Out14,record));
output(SORT(Out15,record));

output('Keyed DenormalizeGroup');
output(SORT(Out101,record));
output(SORT(Out102,record));
output(SORT(Out103,record));
output(SORT(Out104,record));
output(SORT(Out105,record));
output(SORT(Out106,record));
output(SORT(Out107,record));
output(SORT(Out108,record));
output(SORT(Out109,record));
output(SORT(Out110,record));
output(SORT(Out111,record));
output(SORT(Out112,record));
output(SORT(Out113,record));
output(SORT(Out114,record));
output(SORT(Out115,record));

