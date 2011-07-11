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

lhs := DATASET([{['Anderson', 'Taylor']}], {SET OF STRING25 Lnames{MAXLENGTH(100)}});

{STRING15 Fname, string15 LName} xfm(DG_FetchIndex1 r) := TRANSFORM
    SELF.Fname := r.Fname;
    SELF.Lname := r.Lname;
END;

j1 := JOIN(lhs, DG_FetchIndex1, RIGHT.Lname IN LEFT.Lnames, xfm(RIGHT));

#if (useLocal)
OUTPUT(SORT(j1, lname, fname), {fname});
#else
OUTPUT(j1, {fname});
#end
