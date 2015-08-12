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

DG_FileOut           := '~REGRESS::xx';

DG_OutRec := RECORD
    unsigned4  DG_ParentID;
    string10  DG_firstname;
    string10  DG_lastname;
    unsigned1 DG_Prange;
END;

DG_VarOutRec := RECORD
  DG_OutRec;
  IFBLOCK(self.DG_Prange%2=0)
    string20 ExtraField;
  END;
END;


DG_VarOutRecPlus := RECORD
  DG_VarOutRec,
  unsigned8 __filepos { virtual(fileposition)};
END;

DG_VarFile   := DATASET(DG_FileOut+'VAR',DG_VarOutRecPlus,FLAT,encrypt('encryptedKey'));
DG_VarIndex  := INDEX(DG_VarFile,{DG_firstname,DG_lastname,__filepos},DG_FileOut+'VARINDEX');

varrecplus := {string45 name, DG_VarFile};

varrecplus makeVarRec(DG_VarFile L, string name) := TRANSFORM
self.name := name;
    self := L;
END;

varrecplus makeVarRecSkip(DG_VarFile L, string name) := TRANSFORM
self.name := if(L.DG_parentid % 2 = 0,SKIP,name);
    self := L;
END;

// Now variable records
// Simple fetches
output(SORT(FETCH(DG_VarFile, DG_VarIndex, right.__filepos, makeVarRec(left, 'var straight')), dg_parentid));
output(count(choosen(FETCH(DG_VarFile, DG_VarIndex, right.__filepos, makeVarRec(left, 'var choosen')),1)));
output(SORT(FETCH(DG_VarFile, DG_VarIndex, right.__filepos, makeVarRecSkip(left, 'var skip')), dg_parentid));
output(count(choosen(FETCH(DG_VarFile, DG_VarIndex, right.__filepos, makeVarRecSkip(left, 'var skip choosen')),1)));
