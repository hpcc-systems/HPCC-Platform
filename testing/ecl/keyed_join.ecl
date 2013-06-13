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
//UseIndexes

recpair := record
    string45 name;      //join type description
    string45 leftrec;
    string45 rightrec;
  END;

SkipFilter := [DG_Fnames[4], DG_Fnames[5],DG_Fnames[6],DG_Fnames[7],DG_Fnames[8],DG_Fnames[9],DG_Fnames[10],
               DG_Fnames[11],DG_Fnames[12],DG_Fnames[13],DG_Fnames[14],DG_Fnames[15],DG_Fnames[16]];

recpair makePair(DG_FlatFile L, DG_indexFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange;
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;    
  END;

recpair makePairSkip(DG_FlatFile L, DG_indexFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange);   
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;   
  END;

recpair makePairFK(DG_FlatFile L, DG_FlatFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange;    
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;    
  END;

recpair makePairFKSkip(DG_FlatFile L, DG_FlatFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := IF (L.DG_firstname in SkipFilter,SKIP,L.DG_firstname
         + L.DG_lastname  
         + L.DG_Prange);    
    self.rightrec := R.DG_firstname
         + R.DG_lastname  
         + R.DG_Prange;    
  END;

//halfkeyedjoins(halfkeyed, DG_FlatFile, 'simple');
Out1 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: simple inner'));
Out2 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: simple left only'), LEFT ONLY);
Out3 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: simple left outer'), LEFT OUTER);
Out4 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: simple skip'));
Out5 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: simple skip, left only'), LEFT ONLY);
Out6 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: simple skip, left outer'), LEFT OUTER);
Out7 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: simple keep(2)'), KEEP(2));
Out8 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: simple atmost(3))'), ATMOST(3));
Out9 :=choosen(JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: simple left outer'), LEFT OUTER),1);
Out10 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: simple LIMIT(3,SKIP))'), LIMIT(3,SKIP));
Out11 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: simple atmost(3), LEFT OUTER)'), ATMOST(3), LEFT OUTER);
Out12 :=JOIN(DG_FlatFile, DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: simple LIMIT(3,SKIP),LEFT OUTER)'), LIMIT(3,SKIP), LEFT OUTER);

//fullkeyedjoins(fullkeyed, DG_FlatFile, 'simple');
Out21 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: simple inner'), KEYED(DG_indexFileEvens));
Out22 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: simple left only'), KEYED(DG_indexFileEvens), LEFT ONLY);
Out23 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: simple left outer'), KEYED(DG_indexFileEvens), LEFT OUTER);
Out24 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: simple skip'), KEYED(DG_indexFileEvens));
Out25 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: simple skip, left only'), KEYED(DG_indexFileEvens), LEFT ONLY);
Out26 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: simple skip, left outer'), KEYED(DG_indexFileEvens), LEFT OUTER);
Out27 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple keep(2)'), KEYED(DG_indexFileEvens), KEEP(2));
Out28 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple atmost(3)'), KEYED(DG_indexFileEvens), ATMOST(3));
Out29 :=choosen(JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: simple choosen'), KEYED(DG_indexFileEvens), LEFT OUTER), 1);
Out30 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple limit(3,skip)'), KEYED(DG_indexFileEvens), LIMIT(3,SKIP));
Out31 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple atmost(3), LEFT OUTER'), KEYED(DG_indexFileEvens), ATMOST(3), LEFT OUTER);
Out32 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple limit(3,skip), LEFT OUTER'), KEYED(DG_indexFileEvens), LIMIT(3,SKIP), LEFT OUTER);
Out33 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: simple limit(3,skip), LEFT OUTER'), KEYED(DG_indexFileEvens), LIMIT(3), ONFAIL(makePairFK(left, right, '**onfail** Full keyed: simple limit(3,skip), LEFT OUTER')), LEFT OUTER);
Out34 :=JOIN(DG_FlatFile, DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname AND right.DG_parentID=99
      , makePairFK(left, right, 'Full keyed: postfilter after fetch'), KEYED(DG_indexFileEvens), LEFT OUTER);

//halfkeyedjoins(halfkeyedgrouped, group(DG_FlatFile, DG_firstname), 'grouped');
Out41 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: grouped inner'));
Out42 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: grouped left only'), LEFT ONLY);
Out43 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: grouped left outer'), LEFT OUTER);
Out44 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: grouped skip'));
Out45 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: grouped skip, left only'), LEFT ONLY);
Out46 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairSkip(left, right, 'Half keyed: grouped skip, left outer'), LEFT OUTER);
Out47 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped keep(2)'), KEEP(2));
Out48 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped atmost(3))'), ATMOST(3));
Out49 :=choosen(JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePair(left, right, 'Half keyed: grouped choosen'), LEFT OUTER),1);
Out50 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped limit(3,skip))'), LIMIT(3,skip));
Out51 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped atmost(3), LEFT OUTER)'), ATMOST(3), LEFT OUTER);
Out52 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped limit(3,skip), LEFT OUTER)'), LIMIT(3,skip), LEFT OUTER);
Out53 :=JOIN(group(DG_FlatFile, DG_firstname), DG_indexFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePair(left, right, 'Half keyed: grouped limit(3,skip), LEFT OUTER)'), LIMIT(3), ONFAIL(makePair(left, right, '**onfail** Half keyed: grouped limit(3,skip), LEFT OUTE')), LEFT OUTER);


//fullkeyedjoins(fullkeyedgrouped, group(DG_FlatFile, DG_firstname), 'grouped');
Out61 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: grouped inner'), KEYED(DG_indexFileEvens));
Out62 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: grouped left only'), KEYED(DG_indexFileEvens), LEFT ONLY);
Out63 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: grouped left outer'), KEYED(DG_indexFileEvens), LEFT OUTER);
Out64 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: grouped skip'), KEYED(DG_indexFileEvens));
Out65 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: grouped skip, left only'), KEYED(DG_indexFileEvens), LEFT ONLY);
Out66 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFKSkip(left, right, 'Full keyed: grouped skip, left outer'), KEYED(DG_indexFileEvens), LEFT OUTER);
Out67 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped keep(2)'), KEYED(DG_indexFileEvens), KEEP(2));
Out68 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped atmost(3)'), KEYED(DG_indexFileEvens), ATMOST(3));
Out69 :=choosen(JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
         AND left.DG_Prange=right.DG_Prange     
      , makePairFK(left, right, 'Full keyed: grouped choosen'), KEYED(DG_indexFileEvens), LEFT OUTER), 1);
Out70 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped limit(3,SKIP)'), KEYED(DG_indexFileEvens), LIMIT(3,SKIP));
Out71 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped atmost(3), LEFT OUTER'), KEYED(DG_indexFileEvens), ATMOST(3), LEFT OUTER);
Out72 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped limit(3,SKIP), LEFT OUTER'), KEYED(DG_indexFileEvens), LIMIT(3,SKIP), LEFT OUTER);

Out73 :=JOIN(group(DG_FlatFile, DG_firstname), DG_FlatFileEvens, left.DG_firstname = right.DG_firstname 
         AND left.DG_lastname=right.DG_lastname 
      , makePairFK(left, right, 'Full keyed: grouped limit(3,SKIP), LEFT OUTER'), KEYED(DG_indexFileEvens), LIMIT(3), ONFAIL(makePairFK(left, right, 'Full keyed ONFAIL: grouped limit(3,SKIP), LEFT OUTER')), LEFT OUTER);


output(Out1);
output(Out2);
output(Out3);
output(Out4);
output(Out5);
output(Out6);
output(COUNT(Out7));
output(Out8);
output(COUNT(Out9));
output(Out10);
output(Out11);
output(Out12);

output(Out21);
output(Out22);
output(Out23);
output(Out24);
output(Out25);
output(Out26);
output(COUNT(Out27));
output(Out28);
output(COUNT(Out29));
output(Out30);
output(Out31);
output(Out32);
output(Out33);
output(Out34);

output(GROUP(Out41));
output(GROUP(Out42));
output(GROUP(Out43));
output(GROUP(Out44));
output(GROUP(Out45));
output(GROUP(Out46));
output(COUNT(Out47));
output(GROUP(Out48));
output(COUNT(Out49));
output(GROUP(Out50));
output(GROUP(Out51));
output(GROUP(Out52));
output(GROUP(Out53));

output(GROUP(Out61));
output(GROUP(Out62));
output(GROUP(Out63));
output(GROUP(Out64));
output(GROUP(Out65));
output(GROUP(Out66));
output(COUNT(Out67));
output(GROUP(Out68));
output(COUNT(Out69));
output(GROUP(Out70));
output(GROUP(Out71));
output(GROUP(Out72));
//output(GROUP(Out73));
