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
import $; C := $.files('');
import $.sq;

C.DG_OutRec norm1(C.DG_OutRec l, integer cc) := transform
  self.DG_firstname := C.DG_Fnames[cc];
  self := l;
  end;
DG_Norm1Recs := normalize( C.DG_BlankSet, 4, norm1(left, counter));

C.DG_OutRec norm2(C.DG_OutRec l, integer cc) := transform
  self.DG_lastname := C.DG_Lnames[cc];
  self := l;
  end;
DG_Norm2Recs := normalize( DG_Norm1Recs, 4, norm2(left, counter));

C.DG_OutRec norm3(C.DG_OutRec l, integer cc) := transform
  self.DG_Prange := C.DG_Pranges[cc];
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

C.DG_OutRecChild GenChildren(DG_OutputRecs l) := transform
  self.DG_ChildID := 0;
  self := l;
  end;
DG_ChildRecs1 := normalize(DG_ParentRecs, C.DG_MaxChildren, GenChildren(left));

C.DG_OutRecChild SeqChildren(C.DG_OutRecChild l, integer cc) := transform
  self.DG_ChildID := cc-1;
  self := l;
  end;
DG_ChildRecs := project( DG_ChildRecs1, SeqChildren(left, counter));
output(DG_ParentRecs,,C.DG_ParentFileOut,overwrite);
output(DG_ChildRecs,,C.DG_ChildFileOut,overwrite);
fileServices.AddFileRelationship( C.DG_ParentFileOut, C.DG_ChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
C.DG_OutRecChild GenGrandChildren(C.DG_OutRecChild l) := transform
  self := l;
  end;
DG_GrandChildRecs := normalize( DG_ChildRecs, C.DG_MaxGrandChildren, GenGrandChildren(left));
output(DG_GrandChildRecs,,C.DG_GrandChildFileOut,overwrite);
fileServices.AddFileRelationship( C.DG_ChildFileOut, C.DG_GrandChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);

//output data files

//***************************************************************************

output(DG_ParentRecs,,C.DG_FileOut+'CSV',CSV,overwrite);
fileServices.AddFileRelationship( C.DG_ParentFileOut, C.DG_FileOut+'CSV', '', '', 'view', '1:1', false);
output(DG_ParentRecs,,C.DG_FileOut+'XML',XML,overwrite);
fileServices.AddFileRelationship( C.DG_ParentFileOut, C.DG_FileOut+'XML', '', '', 'view', '1:1', false);
EvensFilter := DG_ParentRecs.DG_firstname in [C.DG_Fnames[2],C.DG_Fnames[4],C.DG_Fnames[6],C.DG_Fnames[8],
                                              C.DG_Fnames[10],C.DG_Fnames[12],C.DG_Fnames[14],C.DG_Fnames[16]];

SEQUENTIAL(
    PARALLEL(output(DG_ParentRecs,,C.DG_FileOut+'FLAT',overwrite),
             output(DG_ParentRecs(EvensFilter),,C.DG_FileOut+'FLAT_EVENS',overwrite)),
    PARALLEL(buildindex(C.DG_IndexFile,overwrite),
             buildindex(C.DG_IndexFileEvens,overwrite))
    );

    fileServices.AddFileRelationship( __nameof__(C.DG_FlatFile), __nameof__(C.DG_IndexFile), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(C.DG_FlatFile), __nameof__(C.DG_IndexFile), '__fileposition__', 'filepos', 'link', '1:1', true);
    fileServices.AddFileRelationship( __nameof__(C.DG_FlatFileEvens), __nameof__(C.DG_IndexFileEvens), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(C.DG_FlatFileEvens), __nameof__(C.DG_IndexFileEvens), '__fileposition__', 'filepos', 'link', '1:1', true);

C.DG_VarOutRec Proj1(C.DG_OutRec L) := TRANSFORM
  SELF := L;
  SELF.ExtraField := IF(self.DG_Prange<=10,
                        trim(self.DG_lastname[1..self.DG_Prange]+self.DG_firstname[1..self.DG_Prange],all),
                        trim(self.DG_lastname[1..self.DG_Prange-10]+self.DG_firstname[1..self.DG_Prange-10],all));
END;
DG_VarOutRecs := PROJECT(DG_ParentRecs,Proj1(LEFT));

sequential(
  output(DG_VarOutRecs,,C.DG_FileOut+'VAR',overwrite),
  buildindex(C.DG_VarIndex, overwrite),
  buildindex(C.DG_VarVarIndex, overwrite),
  fileServices.AddFileRelationship( __nameof__(C.DG_VarFile), __nameof__(C.DG_VarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(C.DG_VarFile), __nameof__(C.DG_VarIndex), '__fileposition__', '__filepos', 'link', '1:1', true),
  fileServices.AddFileRelationship( __nameof__(C.DG_VarFile), __nameof__(C.DG_VarVarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(C.DG_VarFile), __nameof__(C.DG_VarVarIndex), '__fileposition__', '__filepos', 'link', '1:1', true)
);



C.DG_MemFileRec t_u2(C.DG_MemFileRec l, integer c) := transform self.u2 := c-2; self := l; END;
C.DG_MemFileRec t_u3(C.DG_MemFileRec l, integer c) := transform self.u3 := c-2; self := l; END;
C.DG_MemFileRec t_bu2(C.DG_MemFileRec l, integer c) := transform self.bu2 := c-2; self := l; END;
C.DG_MemFileRec t_bu3(C.DG_MemFileRec l, integer c) := transform self.bu3 := c-2; self := l; END;
C.DG_MemFileRec t_i2(C.DG_MemFileRec l, integer c) := transform self.i2 := c-2; self := l; END;
C.DG_MemFileRec t_i3(C.DG_MemFileRec l, integer c) := transform self.i3 := c-2; self := l; END;
C.DG_MemFileRec t_bi2(C.DG_MemFileRec l, integer c) := transform self.bi2 := c-2; self := l; END;
C.DG_MemFileRec t_bi3(C.DG_MemFileRec l, integer c) := transform self.bi3 := c-2; self := l; END;

n_blank := dataset([{0,0,0,0, 0,0,0,0}],C.DG_MemFileRec);

n_u2 := NORMALIZE(n_blank, 4, t_u2(left, counter));
n_u3 := NORMALIZE(n_u2, 4, t_u3(left, counter));

n_bu2 := NORMALIZE(n_u3, 4, t_bu2(left, counter));
n_bu3 := NORMALIZE(n_bu2, 4, t_bu3(left, counter));

n_i2 := NORMALIZE(n_bu3, 4, t_i2(left, counter));
n_i3 := NORMALIZE(n_i2, 4, t_i3(left, counter));

n_bi2 := NORMALIZE(n_i3, 4, t_bi2(left, counter));
n_bi3 := NORMALIZE(n_bi2, 4, t_bi3(left, counter));

output(n_bi3,,C.DG_MemFileName,overwrite);


C.DG_IntegerRecord createIntegerRecord(unsigned8 c) := transform
    SELF.i6 := c;
    SELF.nested.i4 := c;
    SELF.nested.u3 := c;
    SELF.i5 := c;
    SELF.i3 := c;
END;

singleNullRowDs := dataset([transform({unsigned1 i}, self.i := 0;)]);
output(normalize(singleNullRowDs, 100, createIntegerRecord(counter)),,C.DG_IntegerDatasetName,overwrite);
build(C.DG_IntegerIndex,overwrite);
