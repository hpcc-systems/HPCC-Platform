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
