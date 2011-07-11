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

// try it with just one limit

o1 := output(LIMIT(DG_FetchIndex1(Lname='Anderson'),1,SKIP), {fname});
o2 := output(LIMIT(DG_FetchIndex1(Lname='Anderson'),10,SKIP), {fname});
o3 := output(LIMIT(DG_FetchIndex1(Lname='Anderson'),1,SKIP,KEYED), {fname});
o4 := output(LIMIT(DG_FetchIndex1(Lname='Anderson'),10,SKIP,KEYED), {fname});

// hack to get around codegen optimizing platform(),once call into global (and therefore hthor) context.
nononcelib := 
    SERVICE
varstring platform() : library='graph', include='eclhelper.hpp', ctxmethod, entrypoint='getPlatform';
    END;

iresult := DG_FetchIndex1(Lname IN ['Anderson', 'Taylor']);
lkresult := LIMIT(iresult,10,KEYED);
lsresult := LIMIT(lkresult,10,SKIP);
sresult := IF(nononcelib.platform() != 'hthor', SORT(lsresult,Lname), lsresult);
o5 := output(sresult, {fname});

// then try with a keyed and unkeyed....



o6 := output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson'),1,SKIP,keyed),1,skip), {fname});
o7 := output(LIMIT(LIMIT(DG_FetchIndex1(Lname='Anderson'),10,SKIP,keyed),10,skip), {fname});

 o1:independent;
 o2:independent;
 o3:independent;
 o4:independent;
 o5:independent;
 o6:independent;
 o7:independent;
