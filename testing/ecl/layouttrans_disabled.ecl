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

//class=error
//UseStandardFiles
//UseIndexes
//varskip payload
//varskip varload
//varskip trans

// this would use RLT, but we have not enabled it, so it should fail

DG_FetchIndex1Alt1 := INDEX(DG_FetchFile,{Fname,Lname,__filepos},DG_FetchIndex1Name);

ds := DATASET([{'Anderson'}, {'Doe'}], {STRING25 Lname});

OUTPUT(SORT(DG_FetchIndex1Alt1(Lname = 'Smith'), record), {Fname, Lname});
