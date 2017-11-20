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

//class=file
//class=index
//version multiPart=false
//version multiPart=true
//version multiPart=true,useLocal=true
//noversion multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4522, ignore);
#onwarning (4523, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

indexrec := RECORDOF(Files.DG_indexFileEvens);
outputrec := { String10 DG_firstname; String10 DG_lastname, unsigned1 DG_prange, unsigned8 filepos};

outrow := record
    string30 name;      //test description
    string10 DG_firstname;
    string10 DG_lastname;
    unsigned1 DG_prange;
    DATASET(outputrec) rightrecs;
  END;

outrow tooutrow(Files.DG_flatfile L, string name) := TRANSFORM
  SELF.name := name;
    SELF.rightrecs := [];
    SELF := L;
END;

SkipFilter := [Files.DG_Fnames[4], Files.DG_Fnames[5],Files.DG_Fnames[6],Files.DG_Fnames[7],Files.DG_Fnames[8],Files.DG_Fnames[9],Files.DG_Fnames[10],
               Files.DG_Fnames[11],Files.DG_Fnames[12],Files.DG_Fnames[13],Files.DG_Fnames[14],Files.DG_Fnames[15],Files.DG_Fnames[16]];

outrow addRow(outrow L, Files.DG_indexFileEvens R, integer c) := TRANSFORM
    self.rightrecs := L.rightrecs + PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;

outrow addRowSkip(outrow L, Files.DG_indexFileEvens R, integer c) := TRANSFORM
    self.DG_firstname := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname);
    self.rightrecs := L.rightrecs + PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;


outrow makeRow(Files.DG_FlatFile L, dataset(indexrec) R, string name) := TRANSFORM
    self.name := name;
    self.rightrecs := PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;

outrow makeRowSkip(Files.DG_FlatFile L, dataset(indexrec) R, string name) := TRANSFORM
    self.name := name;
    self.DG_firstname := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname);
    self.rightrecs := PROJECT(R, transform(outputrec, SELF := LEFT));
    self := L;
  END;


Out1 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER));
Out2 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed skip')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER));
Out3 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed keep(2)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2));
Out4 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed atmost(3)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3));
Out5 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed limit(3,skip)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP));

Out6 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), INNER);
Out7 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner skip')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), INNER);
Out8 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner keep(2)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), INNER);
Out9 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner atmost(3)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), INNER);
Out10 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Inner limit(3,skip)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), INNER);

Out11 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), LEFT OUTER);
Out12 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer skip')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), LEFT OUTER);
Out13 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer keep(2)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), LEFT OUTER);
Out14 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer atmost(3)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), LEFT OUTER);
Out15 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, Outer limit(3,skip)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), LEFT OUTER);

// child query tests. Doesn't test every combo, only inner
recKids := RECORD
 unsigned id;
 Files.DG_FlatFile.DG_FirstName;
 Files.DG_FlatFile.DG_LastName;
 DATASET(RECORDOF(Files.DG_FlatFile)) kids;
END;

grpd := GROUP(SORT(Files.DG_FlatFile, DG_LastName), DG_LastName);
rolledup := ROLLUP(grpd, GROUP, TRANSFORM(recKids, SELF.id := HASH32(LEFT.DG_LastName); SELF.kids := ROWS(LEFT); SELF := LEFT));

outrec := RECORD
 Files.DG_FlatFile.DG_FirstName;
 Files.DG_FlatFile.DG_LastName;
 unsigned kidCount;
END;

outrec denormTrans1(rolledup l) := TRANSFORM
 cd := DENORMALIZE(PROJECT(l.kids, tooutrow(LEFT, 'Child Keyed')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER));
 SELF.kidcount := COUNT(cd);
 SELF := l;
END;

Out16 := PROJECT(rolledup, denormTrans1(LEFT));

outrec denormTrans2(rolledup l) := TRANSFORM
 cd := DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, skip')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER));
 SELF.kidcount := COUNT(cd);
 SELF := l;
END;

Out17 := PROJECT(rolledup, denormTrans2(LEFT));

outrec denormTrans3(rolledup l) := TRANSFORM
 cd := DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, keep(2)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2));
 SELF.kidcount := COUNT(cd);
 SELF := l;
END;

Out18 := PROJECT(rolledup, denormTrans3(LEFT));

outrec denormTrans4(rolledup l) := TRANSFORM
 cd := DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, atmost(3)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3));
 SELF.kidcount := COUNT(cd);
 SELF := l;
END;

Out19 := PROJECT(rolledup, denormTrans4(LEFT));

outrec denormTrans5(rolledup l) := TRANSFORM
 cd := DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Keyed, limit(3,skip)')), Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP));
 SELF.kidcount := COUNT(cd);
 SELF := l;
END;

Out20 := PROJECT(rolledup, denormTrans5(LEFT));


// Unkeyed DenormalizeGroup, with INNER, LEFT OUTER, and unspecified (should be same as LEFT OUTER)

out101 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup'));
out102 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup skip'));
out103 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup keep(2)'), KEEP(2));
out104 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup atmost(3))'), ATMOST(3));
out105 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup LIMIT(3,SKIP))'), LIMIT(3,SKIP));

out106 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner'), INNER);
out107 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup, Inner skip'), INNER);
out108 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner keep(2)'), INNER, KEEP(2));
out109 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner atmost(3))'), INNER, ATMOST(3));
out110 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Inner LIMIT(3,SKIP))'), INNER, LIMIT(3,SKIP));

out111 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer'), LEFT OUTER);
out112 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'KeyedGroup, Outer skip'), LEFT OUTER);
out113 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer keep(2)'), LEFT OUTER, KEEP(2));
out114 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer atmost(3))'), LEFT OUTER, ATMOST(3));
out115 :=DENORMALIZE(Files.DG_FlatFile, Files.DG_IndexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'KeyedGroup, Outer LIMIT(3,SKIP))'), LEFT OUTER, LIMIT(3,SKIP));

// Unkeyed Denormalize

deindexed := nofold(Files.DG_IndexFileEvens(true));

Out201 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed')), Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER));
Out202 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed skip')), Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER));
Out203 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed keep(2)')), Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2));
Out204 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed atmost(3)')), Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3));
Out205 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed limit(3,skip)')), Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP));

out206 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), INNER);
out207 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner skip')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), INNER);
out208 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner keep(2)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), INNER);
out209 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner atmost(3)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), INNER);
out210 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner limit(3,skip)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), INNER);

out211 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRow(left, right, COUNTER), LEFT OUTER);
out212 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER skip')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , addRowSkip(left, right, COUNTER), LEFT OUTER);
out213 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER keep(2)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), KEEP(2), LEFT OUTER);
out214 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER atmost(3)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), ATMOST(3), LEFT OUTER);
out215 :=DENORMALIZE(PROJECT(Files.DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER limit(3,skip)')), deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , addRow(left, right, counter), LIMIT(3,SKIP), LEFT OUTER);

// Unkeyed DenormalizeGroup

out301 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'Group'));
out302 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'Group skip'));
out303 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group keep(2)'), KEEP(2));
out304 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group atmost(3))'), ATMOST(3));
out305 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group LIMIT(3,SKIP))'), LIMIT(3,SKIP));

out306 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'Group, Inner'), INNER);
out307 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'Group, Inner skip'), INNER);
out308 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Inner keep(2)'), INNER, KEEP(2));
out309 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Inner atmost(3))'), INNER, ATMOST(3));
out310 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Inner LIMIT(3,SKIP))'), INNER, LIMIT(3,SKIP));

out311 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRow(left, rows(right), 'Group, Outer'), LEFT OUTER);
out312 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , GROUP, makeRowSkip(left, rows(right), 'Group, Outer skip'), LEFT OUTER);
out313 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Outer keep(2)'), LEFT OUTER, KEEP(2));
out314 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Outer atmost(3))'), LEFT OUTER, ATMOST(3));
out315 :=DENORMALIZE(Files.DG_FlatFile, Deindexed, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , GROUP, makeRow(left, rows(right), 'Group, Outer LIMIT(3,SKIP))'), LEFT OUTER, LIMIT(3,SKIP));



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

output('Child Keyed Denormalize');
output(SORT(Out16,record));
output(SORT(Out17,record));
output(SORT(Out18,record));
output(SORT(Out19,record));
output(SORT(Out20,record));

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

output('Unkeyed Denormalize');
output(SORT(Out201,record));
output(SORT(Out202,record));
output(SORT(Out203,record));
output(SORT(Out204,record));
output(SORT(Out205,record));
output(SORT(Out206,record));
output(SORT(Out207,record));
output(SORT(Out208,record));
output(SORT(Out209,record));
output(SORT(Out210,record));
output(SORT(Out211,record));
output(SORT(Out212,record));
output(SORT(Out213,record));
output(SORT(Out214,record));
output(SORT(Out215,record));

output('Unkeyed DenormalizeGroup');
output(SORT(Out301,record));
output(SORT(Out302,record));
output(SORT(Out303,record));
output(SORT(Out304,record));
output(SORT(Out305,record));
output(SORT(Out306,record));
output(SORT(Out307,record));
output(SORT(Out308,record));
output(SORT(Out309,record));
output(SORT(Out310,record));
output(SORT(Out311,record));
output(SORT(Out312,record));
output(SORT(Out313,record));
output(SORT(Out314,record));
output(SORT(Out315,record));

