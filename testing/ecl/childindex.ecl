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

krec := RECORD
    STRING20 lname;
END;

d1 := DATASET([{'BAYLISS'}, {'SMITH'}, {'DOLSON'}, {'XXXXXXX'}], {STRING20 lname});

orec := RECORD
    STRING20 fname;
    STRING20 lname;
END;

orec xfm1(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile(dg_lastName=l.lname), dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

orec xfm2(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile(dg_lastName=l.lname), -dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

orec xfm3(d1 l) := TRANSFORM
    SELF.fname := (SORT(DG_indexFile((dg_lastName=l.lname) AND (dg_firstName >= 'D')), dg_firstName))[1].dg_firstName;
    SELF := l;
    END;

OUTPUT(PROJECT(d1, xfm1(LEFT)));
OUTPUT(PROJECT(d1, xfm2(LEFT)));
OUTPUT(PROJECT(d1, xfm3(LEFT)));
OUTPUT(DG_indexFile, { DG_firstname, DG_lastname, DG_prange, filepos} );
