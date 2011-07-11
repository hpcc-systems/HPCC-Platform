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
//nolocal

import lib_stringlib;

varrec := RECORD
  STRING20 DG_firstname;
  STRING20 DG_lastname;
  string varname;
END;

recpair := record
    string45 name;      //join type description
    string45 leftrec;
    string100 rightrec;
  END;


recpair makeVarPair(varrec L, DG_varfile R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := L.DG_firstname + L.DG_lastname; 
    self.rightrec := R.DG_firstname + R.DG_lastname;
  END;


varrec mv(DG_FlatFile L) := TRANSFORM
  self := l;
  self.varname := TRIM(L.dg_firstname) + ' ' + L.dg_lastname;
END;

vv := PROJECT(DG_FlatFile, mv(LEFT));

boolean postfilter(string f, integer y) := length(f)*1000 > y;

Out1 :=JOIN(vv, DG_varFile, KEYED(left.DG_firstname = right.DG_firstname) AND KEYED(left.DG_lastname = right.DG_lastname) AND postfilter(left.varname, right.dg_prange)
      , makeVarPair(left, right, 'Full keyed to var file: simple inner'), KEYED(DG_VARINDEX));

output(SORT(Out1,record));
