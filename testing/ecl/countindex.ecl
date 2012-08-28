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

//UseStandardFiles
//UseIndexes
//nothor

d1 := dataset([{'DAVID', 'BAYLISS'}, {'CLAIRE','BAYLISS'}, {'EBENEZER','BAYLISS'},{'DAVID','SMITH'}], { string20 fname, string20 lname });

orec := record
  string20 fname;
  string20 lname;
  unsigned4 cnt;
END;

orec countTransform1(d1 l) := TRANSFORM
    SELF.cnt := COUNT(DG_indexFile(dg_firstName=l.fname));
    SELF := L;
    END;

orec countTransform2(d1 l) := TRANSFORM
    SELF.cnt := COUNT(DG_indexFile(dg_firstName=l.fname,dg_lastName=l.lname));
    SELF := L;
    END;

orec countTransform3(d1 l) := TRANSFORM
    SELF.cnt := COUNT(DG_indexFile(dg_lastName=l.lname));
    SELF := L;
    END;

output(PROJECT(d1, countTransform1(LEFT)));
output(PROJECT(d1, countTransform2(LEFT)));
output(PROJECT(d1, countTransform3(LEFT)));
count(DG_indexfile(dg_firstname[1] = 'K'));
output(DG_indexfile(DG_LASTNAME='SMITH'), { DG_firstname, DG_lastname, DG_prange, filepos} );
COUNT(DG_indexfile(DG_LASTNAME='SMITH'));
COUNT(DG_indexfile(DG_LASTNAME>='SMITH'));
output(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), { DG_firstname, DG_lastname, DG_prange, filepos} );
count(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'));
count(DG_indexfile(dg_firstname >= 'KIMBERLY'));
count(DG_indexfile(dg_firstname[1] = 'Z'));
count(DG_indexfile);

output(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4));

output(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 4, SKIP));

output(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 2, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(DG_indexfile(dg_firstname = 'KIMBERLY', DG_LASTNAME='SMITH'), 2, SKIP));

output(LIMIT(DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 4, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 4, SKIP));

output(LIMIT(DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 2, SKIP), { DG_firstname, DG_lastname, DG_prange, filepos});
count(LIMIT(DG_indexfile(KEYED(dg_firstname = 'KIMBERLY'), KEYED(DG_LASTNAME='SMITH')), 2, SKIP));
