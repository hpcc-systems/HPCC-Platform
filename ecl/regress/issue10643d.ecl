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

#option ('targetClusterType', 'hthor');

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

rec1 jn(DATASET(rec1) ds) := FUNCTION
    j1 := JOIN(ds(seq >= 0), ds(seq > 0), LEFT.seq=RIGHT.seq, HASH);
    j2 := JOIN(j1(seq >= 0), j1(seq > 0), LEFT.seq=RIGHT.seq, SMART);
    RETURN j2;
end;

outf := LOOP(myrecs1,5,jn(ROWS(LEFT)));

output(outf);
