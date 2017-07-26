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

import Std.File AS FileServices;
import $;

boolean createMultiPart := (__PLATFORM__[1..4] = 'thor');
Files := $.Files(createMultiPart, false);

Files.DG_OutRec norm1(Files.DG_OutRec l, integer cc) := transform
  self.DG_firstname := Files.DG_Fnames[cc];
  self := l;
  end;
DG_Norm1Recs := normalize( Files.DG_BlankSet, 4, norm1(left, counter));

Files.DG_OutRec norm2(Files.DG_OutRec l, integer cc) := transform
  self.DG_lastname := Files.DG_Lnames[cc];
  self := l;
  end;
DG_Norm2Recs := normalize( DG_Norm1Recs, 4, norm2(left, counter));

Files.DG_OutRec norm3(Files.DG_OutRec l, integer cc) := transform
  self.DG_Prange := Files.DG_Pranges[cc];
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

Files.DG_OutRecChild GenChildren(DG_OutputRecs l) := transform
  self.DG_ChildID := 0;
  self := l;
  end;
DG_ChildRecs1 := normalize(DG_ParentRecs, Files.DG_MaxChildren, GenChildren(left));

Files.DG_OutRecChild SeqChildren(Files.DG_OutRecChild l, integer cc) := transform
  self.DG_ChildID := cc-1;
  self := l;
  end;
DG_ChildRecs := project( DG_ChildRecs1, SeqChildren(left, counter));
output(DG_ParentRecs,,Files.DG_ParentFileOut,overwrite);
output(DG_ChildRecs,,Files.DG_ChildFileOut,overwrite);
fileServices.AddFileRelationship( Files.DG_ParentFileOut, Files.DG_ChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);
Files.DG_OutRecChild GenGrandChildren(Files.DG_OutRecChild l) := transform
  self := l;
  end;
DG_GrandChildRecs := normalize( DG_ChildRecs, Files.DG_MaxGrandChildren, GenGrandChildren(left));
output(DG_GrandChildRecs,,Files.DG_GrandChildFileOut,overwrite);
fileServices.AddFileRelationship( Files.DG_ChildFileOut, Files.DG_GrandChildFileOut, 'DG_ParentID', 'DG_ParentID', 'link', '1:M', false);

//output data files

//***************************************************************************

output(DG_ParentRecs,,Files.DG_FileOut+'CSV',CSV,overwrite);
fileServices.AddFileRelationship( Files.DG_ParentFileOut, Files.DG_FileOut+'CSV', '', '', 'view', '1:1', false);
output(DG_ParentRecs,,Files.DG_FileOut+'XML',XML,overwrite);
fileServices.AddFileRelationship( Files.DG_ParentFileOut, Files.DG_FileOut+'XML', '', '', 'view', '1:1', false);
EvensFilter := DG_ParentRecs.DG_firstname in [Files.DG_Fnames[2],Files.DG_Fnames[4],Files.DG_Fnames[6],Files.DG_Fnames[8],
                                              Files.DG_Fnames[10],Files.DG_Fnames[12],Files.DG_Fnames[14],Files.DG_Fnames[16]];

SEQUENTIAL(
    PARALLEL(output(DG_ParentRecs,,Files.DG_FileOut+'FLAT',overwrite),
             output(GROUP(SORT(DG_ParentRecs, DG_FirstName),DG_Firstname),,Files.DG_FileOut+'GROUPED',__GROUPED__,overwrite),
             output(DG_ParentRecs(EvensFilter),,Files.DG_FileOut+'FLAT_EVENS',overwrite)),
    PARALLEL(buildindex(Files.DG_NormalIndexFile,overwrite),
             buildindex(Files.DG_NormalIndexFileEvens,overwrite,SET('_nodeSize', 512)),
             buildindex(Files.DG_TransIndexFile,overwrite),
             buildindex(Files.DG_TransIndexFileEvens,overwrite),
             buildindex(Files.DG_KeyedIndexFile,overwrite))
    );

    fileServices.AddFileRelationship( __nameof__(Files.DG_FlatFile), __nameof__(Files.DG_NormalIndexFile), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(Files.DG_FlatFile), __nameof__(Files.DG_NormalIndexFile), '__fileposition__', 'filepos', 'link', '1:1', true);
    fileServices.AddFileRelationship( __nameof__(Files.DG_FlatFileEvens), __nameof__(Files.DG_NormalIndexFileEvens), '', '', 'view', '1:1', false);
    fileServices.AddFileRelationship( __nameof__(Files.DG_FlatFileEvens), __nameof__(Files.DG_NormalIndexFileEvens), '__fileposition__', 'filepos', 'link', '1:1', true);

Files.DG_VarOutRec Proj1(Files.DG_OutRec L) := TRANSFORM
  SELF := L;
  SELF.ExtraField := IF(self.DG_Prange<=10,
                        trim(self.DG_lastname[1..self.DG_Prange]+self.DG_firstname[1..self.DG_Prange],all),
                        trim(self.DG_lastname[1..self.DG_Prange-10]+self.DG_firstname[1..self.DG_Prange-10],all));
  SELF := [];
END;
DG_VarOutRecs := PROJECT(DG_ParentRecs,Proj1(LEFT));

sequential(
  output(DG_VarOutRecs,,Files.DG_FileOut+'VAR',overwrite),
  buildindex(Files.DG_NormalVarIndex, overwrite),
  buildindex(Files.DG_TransVarIndex, overwrite),
  fileServices.AddFileRelationship( __nameof__(Files.DG_VarFile), __nameof__(Files.DG_NormalVarIndex), '', '', 'view', '1:1', false),
  fileServices.AddFileRelationship( __nameof__(Files.DG_VarFile), __nameof__(Files.DG_NormalVarIndex), '__fileposition__', '__filepos', 'link', '1:1', true),
);

//Optionally Create local versions of the indexes.
LocalFiles := $.Files(createMultiPart, TRUE);
IF (createMultiPart,
    PARALLEL(
        buildindex(LocalFiles.DG_NormalIndexFile,overwrite,NOROOT),
        buildindex(LocalFiles.DG_NormalIndexFileEvens,overwrite,NOROOT,SET('_nodeSize', 512)),
        buildindex(LocalFiles.DG_TransIndexFile,overwrite,NOROOT),
        buildindex(LocalFiles.DG_TransIndexFileEvens,overwrite,NOROOT),
        buildindex(LocalFiles.DG_NormalVarIndex, overwrite,NOROOT);
        buildindex(LocalFiles.DG_TransVarIndex, overwrite,NOROOT);
   )
);
