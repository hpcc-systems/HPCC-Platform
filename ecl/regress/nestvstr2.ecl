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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
vstring(integer i) := TYPE
    export integer physicallength(string s) := i;
    export string load(string s) := s[1..i];
    export string store(string s) := s;
    END;


vdata(integer i) := TYPE
    export integer physicallength(data s) := i;
    export data load(data s) := s[1..i];
    export data store(data s) := s;
    END;

rawLayout := record
    string20 dl;
    string8 date;
    unsigned2 imgLength;
    vdata(SELF.imgLength) jpg;
end;

d1 := dataset([{'id1', '20030911', 5, x'1234567890'}, {'id2', '20030910', 3, x'123456'}], rawLayout);
//output(d1,,'imgfile', overwrite);

d := dataset('imgfile', { rawLayout x, unsigned8 _fpos{virtual(fileposition)} }, FLAT);
i := index(d, { x.dl, _fpos }, 'imgindex');

buildindex(i, overwrite);

//output(fetch(d, i(dl='id2'), RIGHT._fpos),,'x.out');

