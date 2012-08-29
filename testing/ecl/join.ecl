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

recpair := record
    string45 name;      //join type description
    string45 leftrec;
    string45 rightrec;
  END;

SkipFilter := [DG_Fnames[4], DG_Fnames[5],DG_Fnames[6],DG_Fnames[7],DG_Fnames[8],DG_Fnames[9],DG_Fnames[10],
               DG_Fnames[11],DG_Fnames[12],DG_Fnames[13],DG_Fnames[14],DG_Fnames[15],DG_Fnames[16]];

recpair makePairUK(DG_FlatFile L, DG_FlatFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange;
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;
  END;

recpair makePairUKSkip(DG_FlatFile L, DG_FlatFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange);
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;
  END;


//unkeyedjoins(unkeyed, DG_FlatFile, 'simple');
//Out1 :=dataset([{'Unkeyed: simple '}], {string80 __________________}));
Out19 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: simple inner'));
Out20 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: simple only'), LEFT ONLY);
Out21 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: simple outer'), LEFT OUTER);
Out22 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: simple skip'));
Out23 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: simple skip, left only'), LEFT ONLY);
Out24 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: simple skip, left outer'), LEFT OUTER);
Out25 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairUK(left, right, 'Unkeyed: simple keep(2)'), KEEP(2));
Out26 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairUK(left, right, 'Unkeyed: simple atmost(3)'), ATMOST(3));
Out27 :=choosen(JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: simple choosen'), LEFT OUTER), 1);

//unkeyedjoins(unkeyedgrouped, group(DG_FlatFile, DG_firstname), 'grouped');
//Out1 :=dataset([{'Unkeyed: grouped '}], {string80 __________________}));
Out46 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: grouped inner'));
Out47 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: grouped only'), LEFT ONLY);
Out48 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: grouped outer'), LEFT OUTER);
Out49 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: grouped skip'));
Out50 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: grouped skip, left only'), LEFT ONLY);
Out51 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUKSkip(left, right, 'Unkeyed: grouped skip, left outer'), LEFT OUTER);
Out52 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairUK(left, right, 'Unkeyed: grouped keep(2)'), KEEP(2));
Out53 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairUK(left, right, 'Unkeyed: grouped atmost(3)'), ATMOST(3));
Out54 :=choosen(JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairUK(left, right, 'Unkeyed: grouped choosen'), LEFT OUTER), 1);

output(SORT(Out19,record));
output(SORT(Out20,record));

output(SORT(Out21,record));
output(SORT(Out22,record));
output(SORT(Out23,record));
output(SORT(Out24,record));
output(SORT(Out25,record));
output(SORT(Out26,record));
output(SORT(Out27,record));

output(SORT(Out46,record));
output(SORT(Out47,record));
output(SORT(Out48,record));
output(SORT(Out49,record));
output(SORT(Out50,record));

output(SORT(Out51,record));
output(SORT(Out52,record));
output(SORT(Out53,record));
output(SORT(Out54,record));

