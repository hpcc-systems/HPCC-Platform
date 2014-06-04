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

import Std.File AS FileServices;

DG_OutRec norm1(DG_OutRec l, integer c) := transform
  self.DG_firstname := DG_Fnames[c];
  self := l;
  end;
DG_Norm1Recs := normalize( DG_BlankSet, 4, norm1(left, counter));

DG_OutRec norm2(DG_OutRec l, integer c) := transform
  self.DG_lastname := DG_Lnames[c];
  self := l;
  end;
DG_Norm2Recs := normalize( DG_Norm1Recs, 4, norm2(left, counter));

DG_OutRec norm3(DG_OutRec l, integer c) := transform
  self.DG_Prange := DG_Pranges[c];
  self := l;
  end;
DG_Norm3Recs := normalize( DG_Norm2Recs, 4, norm3(left, counter));

//output data files
DG_OutputRecs := DG_Norm3Recs;

//***************************************************************************

DG_OutputRecs SeqParent(DG_OutputRecs l, integer c) := transform
  self.DG_ParentID := c-1;  //use -1 so max records (16^8) fit in unsigned4
  self := l;
  end;
DG_ParentRecs := project( DG_OutputRecs, SeqParent(left, counter));

#if(DG_GenChild = TRUE)
DG_OutRecChild GenChildren(DG_OutputRecs l) := transform
  self.DG_ChildID := 0;
  self := l;
  end;
DG_ChildRecs1 := normalize( DG_ParentRecs, DG_MaxChildren, GenChildren(left));

DG_OutRecChild SeqChildren(DG_OutRecChild l, integer c) := transform
  self.DG_ChildID := c-1;
  self := l;
  end;
DG_ChildRecs := project( DG_ChildRecs1, SeqChildren(left, counter));
output(DG_ParentRecs,,DG_ParentFileOut,overwrite);
output(DG_ChildRecs,,DG_ChildFileOut,overwrite);
fileServices.AddFileRelationship( DG_ParentFileOut, DG_ChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
  #if(DG_GenGrandChild = TRUE)
DG_OutRecChild GenGrandChildren(DG_OutRecChild l) := transform
  self := l;
  end;
DG_GrandChildRecs := normalize( DG_ChildRecs, DG_MaxGrandChildren, GenGrandChildren(left));
output(DG_GrandChildRecs,,DG_GrandChildFileOut,overwrite);
fileServices.AddFileRelationship( DG_ChildFileOut, DG_GrandChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
  #end
#end
//output data files

//***************************************************************************


#if(DG_GenCSV = TRUE)
output(DG_ParentRecs,,DG_FileOut+'CSV',CSV,overwrite);
#if(DG_GenChild = TRUE)
fileServices.AddFileRelationship( DG_ParentFileOut, DG_FileOut+'CSV', '', '', 'view', '1:1', false);
#end
#end
#if(DG_GenXML = TRUE)
output(DG_ParentRecs,,DG_FileOut+'XML',XML,overwrite);
#if(DG_GenChild = TRUE)
fileServices.AddFileRelationship( DG_ParentFileOut, DG_FileOut+'XML', '', '', 'view', '1:1', false);
#end
#end
#if(DG_GenIndex = TRUE)
EvensFilter := DG_ParentRecs.DG_firstname in [DG_Fnames[2],DG_Fnames[4],DG_Fnames[6],DG_Fnames[8],
                                              DG_Fnames[10],DG_Fnames[12],DG_Fnames[14],DG_Fnames[16]];

SEQUENTIAL( 
    PARALLEL(output(DG_ParentRecs,,DG_FileOut+'FLAT',overwrite),
             output(DG_ParentRecs(EvensFilter),,DG_FileOut+'FLAT_EVENS',overwrite)),
    PARALLEL(buildindex(DG_IndexFile,overwrite
#if (useLocal=true)
                        ,NOROOT
#end
                       ),
             buildindex(DG_IndexFileEvens,overwrite
#if (useLocal=true)
                        ,NOROOT
#end
             ))
    );

    fileServices.AddFileRelationship( __nameof__(DG_FlatFile), __nameof__(DG_IndexFile), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFile), __nameof__(DG_IndexFile), '__fileposition__', 'filepos', 'link', '1:1', true);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFileEvens), __nameof__(DG_IndexFileEvens), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(DG_FlatFileEvens), __nameof__(DG_IndexFileEvens), '__fileposition__', 'filepos', 'link', '1:1', true);
#else
  #if(DG_GenFlat = TRUE)
    output(DG_ParentRecs,,DG_FileOut+'FLAT',overwrite);
    output(DG_ParentRecs(EvensFilter),,DG_FileOut+'FLAT_EVENS',overwrite);
  #end
#end

//Output variable length records
#if(DG_GenVar = TRUE)
DG_VarOutRec Proj1(DG_OutRec L) := TRANSFORM
  SELF := L;
  SELF.ExtraField := IF(self.DG_Prange<=10,
                        trim(self.DG_lastname[1..self.DG_Prange]+self.DG_firstname[1..self.DG_Prange],all),
                        trim(self.DG_lastname[1..self.DG_Prange-10]+self.DG_firstname[1..self.DG_Prange-10],all));
END;
DG_VarOutRecs := PROJECT(DG_ParentRecs,Proj1(LEFT));

sequential(
  output(DG_VarOutRecs,,DG_FileOut+'VAR',overwrite),
  buildindex(DG_VarIndex, overwrite
#if (useLocal=true)
  ,NOROOT
#end
  ),
  buildindex(DG_VarVarIndex, overwrite
#if (useLocal=true)
  ,NOROOT
#end
  ),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarIndex), '__fileposition__', '__filepos', 'link', '1:1', true),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarVarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(DG_VarFile), __nameof__(DG_VarVarIndex), '__fileposition__', '__filepos', 'link', '1:1', true)
);
#end

//******************************** Dictionary dataset creation code ***********************

libraryDs := DATASET([
    { 'gavin',
        [{'the hobbit',
            [{'gandalf'},{'rivendell'},{'dragon'},{'dwarves'},{'elves'}]},
         {'eragon',
            [{'eragon'},{'dragon'},{'spine'},{'elves'},{'dwarves'},{'krull'}]}
        ]},
    { 'jim',
        [{'complete diy',
            [{'heating'},{'electrics'},{'nuclear reactors'},{'spaceships'}]},
        {'cheeses',
            [{'cheddar'},{'parmesan'},{'stilton'},{'wensleydale'}]}
        ]}], SerialTest.libraryDsRec);
        
libraryDictDs := DATASET([
    { 'gavin' =>
        [{'the hobbit' =>
            [{'gandalf'},{'rivendell'},{'dragon'},{'dwarves'},{'elves'}]},
         {'eragon' =>
            [{'eragon'},{'dragon'},{'spine'},{'elves'},{'dwarves'},{'krull'}]}
        ]},
    { 'jim' =>
        [{'complete diy' =>
            [{'heating'},{'electrics'},{'nuclear reactors'},{'spaceships'}]},
        {'cheeses' =>
            [{'cheddar'},{'parmesan'},{'stilton'},{'wensleydale'}]}
        ]}], SerialTest.libraryDictRec);
        
OUTPUT(libraryDs,,DG_DsFilename,OVERWRITE);

OUTPUT(libraryDictDs,,DG_DictFilename,OVERWRITE);

allBooks := SerialTest.libraryDatasetFile.books;

createBookIndex := INDEX(allBooks, { string20 title := title }, { dataset(SerialTest.wordRec) words := words }, DG_BookKeyFilename);

BUILD(createBookIndex, overwrite);


DG_MemFileRec t_u2(DG_MemFileRec l, integer c) := transform self.u2 := c-2; self := l; END;
DG_MemFileRec t_u3(DG_MemFileRec l, integer c) := transform self.u3 := c-2; self := l; END;
DG_MemFileRec t_bu2(DG_MemFileRec l, integer c) := transform self.bu2 := c-2; self := l; END;
DG_MemFileRec t_bu3(DG_MemFileRec l, integer c) := transform self.bu3 := c-2; self := l; END;
DG_MemFileRec t_i2(DG_MemFileRec l, integer c) := transform self.i2 := c-2; self := l; END;
DG_MemFileRec t_i3(DG_MemFileRec l, integer c) := transform self.i3 := c-2; self := l; END;
DG_MemFileRec t_bi2(DG_MemFileRec l, integer c) := transform self.bi2 := c-2; self := l; END;
DG_MemFileRec t_bi3(DG_MemFileRec l, integer c) := transform self.bi3 := c-2; self := l; END;

n_blank := dataset([{0,0,0,0, 0,0,0,0}],DG_MemFileRec);

n_u2 := NORMALIZE(n_blank, 4, t_u2(left, counter));
n_u3 := NORMALIZE(n_u2, 4, t_u3(left, counter));

n_bu2 := NORMALIZE(n_u3, 4, t_bu2(left, counter));
n_bu3 := NORMALIZE(n_bu2, 4, t_bu3(left, counter));

n_i2 := NORMALIZE(n_bu3, 4, t_i2(left, counter));
n_i3 := NORMALIZE(n_i2, 4, t_i3(left, counter));

n_bi2 := NORMALIZE(n_i3, 4, t_bi2(left, counter));
n_bi3 := NORMALIZE(n_bi2, 4, t_bi3(left, counter));

output(n_bi3,,DG_MemFileName,overwrite);


DG_IntegerRecord createIntegerRecord(unsigned8 c) := transform
    SELF.i6 := c;
    SELF.nested.i4 := c;
    SELF.nested.u3 := c;
    SELF.i5 := c;
    SELF.i3 := c;
END;
