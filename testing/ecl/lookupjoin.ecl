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

myrec := RECORD
 unsigned6 did;
 string1 let;
end;

myoutrec := RECORD
 unsigned6 did;
 string1 letL;
 string1 letR;
 string40 label;
end;

lhs := dataset([{1234,'A'},{1234,'A'},{5678, 'B'},{5678,'B'},{9876,'C'}],myrec);
rhs := dataset([{1234,'A'},{5678,'B'},{1234,'D'},{1234,'E'}],myrec);
dlhs := DISTRIBUTE(lhs, HASH(did));
drhs := DISTRIBUTE(rhs, HASH(did));

myoutrec testLookup(lhs L, rhs r, string40 lab) := transform
 self.did := l.did;
 self.letL := l.let;
 self.letR := r.let;
 self.label := lab;
end;

j1 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP'),LOOKUP);
j2 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, LEFT OUTER'),LOOKUP, LEFT OUTER);
j3 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, LEFT ONLY'),LOOKUP, LEFT ONLY);
j4 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, MANY'),LOOKUP, MANY);
j5 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, MANY, ATMOST'),LOOKUP, MANY, ATMOST(2));
j6 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, MANY, LIMIT(SKIP)'),LOOKUP, MANY, LIMIT(2, SKIP));
j7 := join(lhs,rhs,left.did=right.did,testLookup(left, right, 'LOOKUP, MANY, LIMIT, ONFAIL'),LOOKUP, MANY, LIMIT(2), ONFAIL(testLookup(left, right, 'FAILED: LOOKUP, MANY, LIMIT, ONFAIL')));
j8 := join(GROUP(dlhs,let),drhs,left.let=right.let,testLookup(left, right, 'LOOKUP LHSGROUPED'),LOOKUP, MANY);
tj8 := SORT(TABLE(j8, { letL, unsigned4 c := COUNT(GROUP), unsigned4 tot := SUM(group,did), label }), letL);
j9 := SORT(join(dlhs,drhs,left.let=right.let,testLookup(left, right, 'LOOKUP MANY LOCAL'),LOOKUP, LOCAL), did);
output(j1);
output(j2);
output(j3);
output(j4);
output(j5);
output(j6);
output(j7);
output(tj8);
output(j9);
