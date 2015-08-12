/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

