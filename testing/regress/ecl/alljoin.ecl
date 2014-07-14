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

myrec := record
 unsigned6 did;
 string1 let;
end;

myoutrec := record
 unsigned6 did;
 string1 letL;
 string1 letR;
end;

d1 := dataset([{1234,'A'},{5678,'B'}],myrec);
d2 := dataset([{1234,'A'},{5678,'B'},{9876,'C'}],myrec);
 
myoutrec testLookup(d2 L, d1 r) := transform
 self.did := l.did;
 self.letL := l.let;
 self.letR := r.let;
end;
 
j1 := join(d2,d1,left.did=right.did,testLookup(left, right),all);
j2 := join(d2,d1,left.did=right.did,testLookup(left, right),all, LEFT OUTER);
j3 := join(d2,d1,left.did=right.did,testLookup(left, right),all, LEFT ONLY);
 
output(j1);
output(j2);
output(j3);