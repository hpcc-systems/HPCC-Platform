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
j8 := join(GROUP(dlhs,let),NOFOLD(drhs),left.let=right.let,testLookup(left, right, 'LOOKUP LHSGROUPED'),LOOKUP, MANY);
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
