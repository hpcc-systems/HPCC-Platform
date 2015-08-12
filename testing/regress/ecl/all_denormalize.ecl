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

//class newtest
//version multiPart=true
//version multiPart=false
//version multiPart=true,useLocal=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);

//--- end of version configuration ---

#onwarning (4523, ignore);

import $.setup;
files := setup.files(multiPart, useLocal);

DG_indexFileEvens := files.DG_indexFileEvens;
DG_FlatFile := files.DG_FlatFile;
DG_Fnames := files.DG_Fnames;
 
//#option('noAllToLookupConversion', '1');

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

truval := true : stored('truval');
falseval := false : stored('falseval');

boolean fuzzymatch(string l, string r) := ((l = r) AND truval) OR falseval;
boolean fuzzyimatch(unsigned l, unsigned r) := ((l = r) AND truval) OR falseval;

// All Denormalize

deindexed := nofold(DG_IndexFileEvens(true));

Out201 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed')), Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)
      , addRow(left, right, COUNTER), ALL);
Out202 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed skip')), Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , addRowSkip(left, right, COUNTER), ALL);
Out203 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed keep(2)')), Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , addRow(left, right, counter), KEEP(2), ALL);

out206 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , addRow(left, right, COUNTER), INNER, ALL);
out207 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner skip')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , addRowSkip(left, right, COUNTER), INNER, ALL);
out208 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, Inner keep(2)')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , addRow(left, right, counter), KEEP(2), INNER, ALL);

out211 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , addRow(left, right, COUNTER), LEFT OUTER, ALL);
out212 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER skip')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , addRowSkip(left, right, COUNTER), LEFT OUTER, ALL);
out213 :=DENORMALIZE(PROJECT(DG_FlatFile, tooutrow(LEFT, 'Unkeyed, OUTER keep(2)')), deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , addRow(left, right, counter), KEEP(2), LEFT OUTER, ALL);

// All DenormalizeGroup

out301 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRow(left, rows(right), 'Group'), ALL);
out302 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRowSkip(left, rows(right), 'Group skip'), ALL);
out303 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , GROUP, makeRow(left, rows(right), 'Group keep(2)'), KEEP(2), ALL);

out306 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRow(left, rows(right), 'Group, Inner'), INNER, ALL);
out307 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRowSkip(left, rows(right), 'Group, Inner skip'), INNER, ALL);
out308 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , GROUP, makeRow(left, rows(right), 'Group, Inner keep(2)'), INNER, KEEP(2), ALL);

out311 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRow(left, rows(right), 'Group, Outer'), LEFT OUTER, ALL);
out312 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
         AND fuzzyimatch(left.DG_Prange, right.DG_Prange)     
      , GROUP, makeRowSkip(left, rows(right), 'Group, Outer skip'), LEFT OUTER, ALL);
out313 :=DENORMALIZE(DG_FlatFile, Deindexed, fuzzymatch(left.DG_firstname, right.DG_firstname) 
         AND fuzzymatch(left.DG_lastname, right.DG_lastname) 
      , GROUP, makeRow(left, rows(right), 'Group, Outer keep(2)'), LEFT OUTER, KEEP(2), ALL);

output('All Denormalize');
output(Out201);
output(Out202);
output(Out203);
output(Out206);
output(Out207);
output(Out208);
output(Out211);
output(Out212);
output(Out213);

output('All DenormalizeGroup');
output(Out301);
output(Out302);
output(Out303);
output(Out306);
output(Out307);
output(Out308);
output(Out311);
output(Out312);
output(Out313);
