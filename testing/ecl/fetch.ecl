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
recplus := {string45 name, unsigned8 rfpos, DG_FlatFileEvens};

recplus makeRec(DG_FlatFileEvens L, DG_indexFileEvens R, string name) := TRANSFORM
    self.name := name;
    self.rfpos := R.filepos;
    self := L;
END;

recplus makeRecSkip(DG_FlatFileEvens L, DG_indexFileEvens R, string name) := TRANSFORM
    self.name := if(L.DG_parentid % 2 = 0,SKIP,name);
    self.rfpos := R.filepos;
    self := L;
END;

varrecplus := {string45 name, unsigned8 rfpos, DG_VarFile};

varrecplus makeVarRec(DG_VarFile L, DG_VarIndex R, string name) := TRANSFORM
    self.name := name;
    self.rfpos := R.__filepos;
    self := L;
END;

varrecplus makeVarRecSkip(DG_VarFile L, DG_VarIndex R, string name) := TRANSFORM
    self.name := if(L.DG_parentid % 2 = 0,SKIP,name);
    self.rfpos := R.__filepos;
    self := L;
END;

// temporary hack to get around codegen optimizing platform(),once call into global (and therefore hthor) context.
nononcelib := 
    SERVICE
varstring platform() : library='graph', include='eclhelper.hpp', ctxmethod, entrypoint='getPlatform';
    END;

sortFixed(dataset(recplus) ds) := FUNCTION
    RETURN IF(nononcelib.platform() = 'thor', SORT(ds, rfpos), ds);
END;

sortVar(dataset(varrecplus) ds) := FUNCTION
    RETURN IF(nononcelib.platform() = 'thor', SORT(ds, rfpos), ds);
END;

SDG_indexFileEvens := SORT(DG_indexFileEvens, filepos);
SDG_VarIndex := SORT(DG_VarIndex, __filepos);

// Simple fetches
o1 := output(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRec(left, right, 'straight'))));
o2 := output(choosen(nofold(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRec(left, right, 'choosen')))),1));
o3 := output(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRecSkip(left, right, 'skip'))));
o4 := output(choosen(nofold(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens, right.filepos, makeRecSkip(left, right, 'skip choosen')))),1));

// Filtered fetches
o5 := output(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=DG_Fnames[2]), right.filepos, makeRec(left, right, 'filtered'))));
o6 := output(choosen(nofold(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=DG_Fnames[2]), right.filepos, makeRec(left, right, 'choosen filtered')))),1));
o7 := output(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=DG_Fnames[2]), right.filepos, makeRecSkip(left, right, 'skip filtered'))));
o8 := output(choosen(nofold(sortFixed(FETCH(DG_FlatFileEvens, SDG_indexFileEvens(DG_firstname=DG_Fnames[2]), right.filepos, makeRecSkip(left, right, 'skip choosen filtered')))),1));

// Now variable records
// Simple fetches
o9 := output(sortVar(FETCH(DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRec(left, right, 'var straight'))));
o10 := output(choosen(nofold(sortVar(FETCH(DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRec(left, right, 'var choosen')))),1));
o11 := output(sortVar(FETCH(DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRecSkip(left, right, 'var skip'))));
o12 := output(choosen(nofold(sortVar(FETCH(DG_VarFile, SDG_VarIndex, right.__filepos, makeVarRecSkip(left, right, 'var skip choosen')))),1));

// Filtered fetches
o13 := output(sortVar(FETCH(DG_VarFile, SDG_VarIndex(DG_firstname=DG_Fnames[2]), right.__filepos, makeVarRec(left, right, 'var filtered'))));
o14 := output(choosen(nofold(sortVar(FETCH(DG_VarFile, SDG_VarIndex(DG_firstname=DG_Fnames[2]), right.__filepos, makeVarRec(left, right, 'var choosen filtered')))),1));
o15 := output(sortVar(FETCH(DG_VarFile, SDG_VarIndex(DG_firstname=DG_Fnames[2]), right.__filepos, makeVarRecSkip(left, right, 'var skip filtered'))));
o16 := output(choosen(nofold(sortVar(FETCH(DG_VarFile, SDG_VarIndex(DG_firstname=DG_Fnames[2]), right.__filepos, makeVarRecSkip(left, right, 'var skip choosen filtered')))),1));

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
