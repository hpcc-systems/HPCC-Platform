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
TYPEOF(DG_FetchFile) copy(DG_FetchFile l) := TRANSFORM
    SELF.__filepos := 0; // filepos is not consistent between different test modes, so suppress it from output
    SELF := l;
END;

// hack to get around codegen optimizing platform(),once call into global (and therefore hthor) context.
nononcelib := 
    SERVICE
varstring platform() : library='graph', include='eclhelper.hpp', ctxmethod, entrypoint='getPlatform';
    END;

TYPEOF(DG_FetchFile) maybesort(TYPEOF(DG_FetchFile) in) :=
#if (useLocal)
  SORT(in, fname, lname);
#else
  IF(nononcelib.platform() = 'thor', SORT(in, fname, lname), in);
#end

output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Anderson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Johnson'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Smith'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Lname='Doe'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Frank'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Sue'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Jane'),RIGHT.__filepos, copy(LEFT))));
output(maybesort(FETCH(DG_FetchFile, DG_FetchIndex1(Fname='Larry'),RIGHT.__filepos, copy(LEFT))));

