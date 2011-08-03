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

