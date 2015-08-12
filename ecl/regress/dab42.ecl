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


format := record
    string10 seq;
    unsigned6 did := 0;
    unsigned2 score := 0;
  end;


ds := dataset('ds', format, thor);

format2 := record
    qstring10 seq;
    unsigned6 did := 0;
    unsigned2 score := 0;
    unsigned8 __filepos{virtual(fileposition)}
  end;

ds2 := dataset('ds2', format2, thor);
k2 := index(ds2, { seq, did, __filepos } , 'k2');

compute_score(unsigned6 leftdid, unsigned6 rightdid, integer2 leftscore, integer2 newscore) :=
    if(leftdid = rightdid , //then combine then
       1,
       2
       );   //take the bigger (might be zero)

format add_score(format l, k2 r) := transform
        self.score :=
            compute_score(l.did, r.did, l.score, 100 div 999) * 1;
    self := l;
end;

x := join(ds, k2, left.seq = right.seq, add_score(left, right));

output(x);

x2 := join(ds, k2, (qstring10)left.seq = right.seq, add_score(left, right));

output(x2);

