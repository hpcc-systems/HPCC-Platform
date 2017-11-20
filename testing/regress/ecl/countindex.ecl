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
#onwarning (4515, ignore);
#onwarning (4523, ignore);
#onwarning (5402, ignore);

import $.setup;
Files := setup.Files(multiPart, useLocal, useTranslation);

d1 := dataset([{'DAVID', 'BAYLISS'}, {'CLAIRE','BAYLISS'}, {'EBENEZER','BAYLISS'},{'DAVID','SMITH'}], { string20 fname, string20 lname });

orec := record
  string20 fname;
  string20 lname;
  unsigned4 cnt;
END;

orec countTransform1(d1 l) := TRANSFORM
    SELF.cnt := COUNT(Files.DG_indexFile(dg_firstName=l.fname));
    SELF := L;
    END;

orec countTransform2(d1 l) := TRANSFORM
    SELF.cnt := COUNT(Files.DG_indexFile(dg_firstName=l.fname,dg_lastName=l.lname));
    SELF := L;
    END;

orec countTransform3(d1 l) := TRANSFORM
    SELF.cnt := COUNT(Files.DG_indexFile(dg_lastName=l.lname));
    SELF := L;
    END;

output(PROJECT(d1, countTransform1(LEFT)));
output(PROJECT(d1, countTransform2(LEFT)));
output(PROJECT(d1, countTransform3(LEFT)));
count(Files.DG_indexfile(dg_firstname[1] = 'K'));
output(Files.DG_indexfile(DG_LASTNAME='SMITH'), { DG_firstname, DG_lastname, DG_prange, filepos} );
COUNT(Files.DG_indexfile(DG_LASTNAME='SMITH'));
COUNT(Files.DG_indexfile(DG_LASTNAME>='SMITH'));
output(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), { DG_firstname, DG_lastname, DG_prange, filepos} );
count(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'));
count(Files.DG_indexfile(dg_firstname >= 'KIMBERLY'));
count(Files.DG_indexfile(dg_firstname[1] = 'Z'));
count(Files.DG_indexfile);

output(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4));

output(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4, SKIP));

output(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 2, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(Files.DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 2, SKIP));

output(LIMIT(Files.DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 4, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(Files.DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 4, SKIP));

output(LIMIT(Files.DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 2, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(Files.DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 2, SKIP));
