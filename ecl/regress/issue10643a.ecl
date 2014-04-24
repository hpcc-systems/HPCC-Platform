/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

subrec1 := record
    string1 a;
    string1 b;
end;

subrec2 := record
    string1 c;
    string1 d;
end;

rec1 := record
    integer seq;
    integer foobar;
    dataset(subrec1) s1;
    dataset(subrec2) s2;
end;

rec2 := record
    integer seq;
    string5 foo;
    dataset(subrec1) s1;
    dataset(subrec2) s2;
end;

df := dataset([{'a','b'},{'c','d'}],subrec1);
dfb := dataset([{'e','f'},{'h','i'}],subrec1);
df2 := dataset([{'1','2'},{'3','4'}],subrec2);
df2b := dataset([{'5','6'}],subrec2);

myrecs1 := nofold(dataset([{1,2,df,df2}],rec1));
myrecs2 := nofold(dataset([{1,'foo',dfb,df2b}],rec2));

rec1 jn(myrecs1 L, myrecs2 R) := transform
    self.s1 := JOIN(L.s1, R.s1, LEFT=RIGHT, HASH);
    self.s2 := JOIN(L.s2, R.s2, LEFT=RIGHT, SMART);
    self := L;
end;

outf := join(myrecs1,myrecs2,left.seq = right.seq,jn(LEFT,RIGHT));

output(outf);
