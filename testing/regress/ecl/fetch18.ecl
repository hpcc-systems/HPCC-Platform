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
//version multiPart=true,useTranslation=true

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslation', useTranslation);
#onwarning (4523, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

IMPORT Std;

recplus := {string45 name, unsigned8 rfpos, Files.DG_FlatFileEvens};

recplus makeRec(Files.DG_FlatFileEvens L, Files.DG_indexFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.rfpos := R.filepos;
    self := L;
END;

recplus makeRecSkip(Files.DG_FlatFileEvens L, Files.DG_indexFileEvens R, string name) := TRANSFORM
    self.name := if(L.DG_parentid % 2 = 0,SKIP,name);
    self.rfpos := R.filepos;
    self := L;
END;

varrecplus := {string45 name, unsigned8 rfpos, Files.DG_VarFile};

varrecplus makeVarRec(Files.DG_VarFile L, Files.DG_VarIndex R, string name) := TRANSFORM
    self.name := name;
    self.rfpos := R.__filepos;
    self := L;
END;

varrecplus makeVarRecSkip(Files.DG_VarFile L, Files.DG_VarIndex R, string name) := TRANSFORM
    self.name := if(L.DG_parentid % 2 = 0,SKIP,name);
    self.rfpos := R.__filepos;
    self := L;
END;

sortFixed(dataset(recplus) ds) := FUNCTION
    RETURN IF(Std.System.Thorlib.platform() = 'thor', SORT(ds, rfpos), ds);
END;

sortVar(dataset(varrecplus) ds) := FUNCTION
    RETURN IF(Std.System.Thorlib.platform() = 'thor', SORT(ds, rfpos), ds);
END;

SDG_indexFileEvens := SORT(Files.DG_indexFileEvens, filepos);
SDG_VarIndex := SORT(Files.DG_VarIndex, __filepos);

// Simple fetches
o1 := output(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRec(left, right, 'straight'))));
o2 := output(choosen(nofold(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRec(left, right, 'choosen')))),1));
o3 := output(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRecSkip(left, right, 'skip'))));
o4 := output(choosen(nofold(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRecSkip(left, right, 'skip choosen')))),1));

// Filtered fetches
o5 := output(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=Files.DG_Fnames[2]), right.filepos, makeRec(left, right, 'filtered'))));
o6 := output(choosen(nofold(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=Files.DG_Fnames[2]), right.filepos, makeRec(left, right, 'choosen filtered')))),1));
o7 := output(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=Files.DG_Fnames[2]), right.filepos, makeRecSkip(left, right, 'skip filtered'))));
o8 := output(choosen(nofold(sortFixed(FETCH(Files.DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=Files.DG_Fnames[2]), right.filepos, makeRecSkip(left, right, 'skip choosen filtered')))),1));

// Now variable records
// Simple fetches
o9 := output(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRec(left, right, 'var straight'))));
o10 := output(choosen(nofold(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRec(left, right, 'var choosen')))),1));
o11 := output(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRecSkip(left, right, 'var skip'))));
o12 := output(choosen(nofold(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRecSkip(left, right, 'var skip choosen')))),1));

// Filtered fetches
o13 := output(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex(DG_firstname=Files.DG_Fnames[2]), right.__filepos, makeVarRec(left, right, 'var filtered'))));
o14 := output(choosen(nofold(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex(DG_firstname=Files.DG_Fnames[2]), right.__filepos, makeVarRec(left, right, 'var choosen filtered')))),1));
o15 := output(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex(DG_firstname=Files.DG_Fnames[2]), right.__filepos, makeVarRecSkip(left, right, 'var skip filtered'))));
o16 := output(choosen(nofold(sortVar(FETCH(Files.DG_VarFile, SDG_VarIndex(DG_firstname=Files.DG_Fnames[2]), right.__filepos, makeVarRecSkip(left, right, 'var skip choosen filtered')))),1));

SEQUENTIAL (
  o1,
  o2,
  o3,
  o4,
  o5,
  o6,
  o7,
  o8,
  o9,
  o10,
  o11,
  o12,
  o13,
  o14,
  o15,
  o16
  );
