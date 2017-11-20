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
//noversion multiPart=true,useTranslation=true,nothor

import ^ as root;
multiPart := #IFDEFINED(root.multiPart, true);
useLocal := #IFDEFINED(root.useLocal, false);
useTranslation := #IFDEFINED(root.useTranslation, false);

//--- end of version configuration ---

#option ('layoutTranslationEnabled', useTranslation);
#onwarning (4522, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

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


recpair makeVarPair(varrec L, Files.DG_varfile R, string name) := TRANSFORM
    self.name := name;
    self.leftrec  := L.DG_firstname + L.DG_lastname; 
    self.rightrec := R.DG_firstname + R.DG_lastname;
  END;


varrec mv(Files.DG_FlatFile L) := TRANSFORM
  self := l;
  self.varname := TRIM(L.dg_firstname) + ' ' + L.dg_lastname;
END;

vv := PROJECT(Files.DG_FlatFile, mv(LEFT));

boolean postfilter(string f, integer y) := length(f)*1000 > y;

Out1 :=JOIN(vv, Files.DG_varFile, KEYED(left.DG_firstname = right.DG_firstname) AND KEYED(left.DG_lastname = right.DG_lastname) AND postfilter(left.varname, right.dg_prange)
      , makeVarPair(left, right, 'Full keyed to var file: simple inner'), KEYED(Files.DG_VARINDEX));

output(SORT(Out1,record));
