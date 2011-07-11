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
