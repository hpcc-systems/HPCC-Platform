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

HashRecs := normalize( BlankSet, 3000000, norm(left, counter));

a := output(DISTRIBUTE(HashRecs,ID),,'REGRESS::IDX::TestFile', overwrite);

ds := dataset('REGRESS::IDX::TestFile',{r,unsigned8 __filpos    {virtual(fileposition)}},thor);

i := INDEX(ds,{id,__filpos},'REGRESS::IDX::TestFileIndex');

b := BUILDINDEX(i, overwrite);

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
                         {string20 txt,integer val});

//a; b; output(d);
sequential(a,b,output(d));

