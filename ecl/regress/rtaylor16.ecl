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

r := record
  unsigned8 id;
  string5   stuff;
end;

BlankSet  := DATASET([{0,''}],r);

r norm(r l, integer c) := transform
  self.id    := if(c<=10, c, 42);
  self.stuff := 'stuff';
  end;

HashRecs := normalize( BlankSet, 300, norm(left, counter));

a := output(DISTRIBUTE(HashRecs,ID),,'REGRESS::IDX::TestFile', overwrite);

ds := dataset('REGRESS::IDX::TestFile',{r,unsigned8 __filpos    {virtual(fileposition)}},thor);

i := INDEX(ds,{id,__filpos},'REGRESS::IDX::TestFileIndex');

b := BUILDINDEX(i, overwrite);
a;
b;
//sequential(a,b);

d := dataset([{'ds(id=1) = 1',count(ds(id=1))},
              {'ds(id=2) = 1',count(ds(id=2))},
                          {'ds(id=3) = 1',count(ds(id=3))},
                          {'ds(id=4) = 1',count(ds(id=4))},
                          {'ds(id=5) = 1',count(ds(id=5))},
                          {'ds(id=6) = 1',count(ds(id=6))},
                          {'ds(id=7) = 1',count(ds(id=7))},
                          {'ds(id=8) = 1',count(ds(id=8))},
                          {'ds(id=9) = 1',count(ds(id=9))},
                          {'ds(id=10)= 1',count(ds(id=10))},
                          {'ds(id=42)= 2999990',count(ds(id=42))},
                          {'i(id=1) = 1',count(i(id=1))},
                          {'i(id=2) = 1',count(i(id=2))},
                          {'i(id=3) = 1',count(i(id=3))},
                          {'i(id=4) = 1',count(i(id=4))},
                          {'i(id=5) = 1',count(i(id=5))},
                          {'i(id=6) = 1',count(i(id=6))},
                          {'i(id=7) = 1',count(i(id=7))},
                          {'i(id=8) = 1',count(i(id=8))},
                          {'i(id=9) = 1',count(i(id=9))},
                          {'i(id=10)= 1',count(i(id=10))},
                          {'i(id=42)= 2999990',count(i(id=42))}
                         ],
                         {string12 txt,integer val});

f1 := fetch(ds,i(id=1),right.__filpos);
f2 := fetch(ds,i(id=2),right.__filpos);
f3 := fetch(ds,i(id=3),right.__filpos);
f4 := fetch(ds,i(id=4),right.__filpos);
f5 := fetch(ds,i(id=5),right.__filpos);
f6 := fetch(ds,i(id=6),right.__filpos);
f7 := fetch(ds,i(id=7),right.__filpos);
f8 := fetch(ds,i(id=8),right.__filpos);
f9 := fetch(ds,i(id=9),right.__filpos);
f10:= fetch(ds,i(id=10),right.__filpos);

output(d);
output(f1);
output(f2);
output(f3);
output(f4);
output(f5);
output(f6);
output(f7);
output(f8);
output(f9);
output(f10);

