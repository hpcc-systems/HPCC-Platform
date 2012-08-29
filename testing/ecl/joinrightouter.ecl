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
    UNSIGNED6 did;
    STRING1 let;
END;

myoutrec := RECORD
    UNSIGNED6 did;
    STRING1 letL;
    STRING1 letR;
    STRING40 label;
END;

lhs := DATASET([{1234,'A'}, {5678,'B'}, {9876,'C'}], myrec);
rhs := DATASET([{1234,'A'}, {5678,'B'}, {1234,'D'}, {1234,'E'}, {9999,'F'}], myrec);
 
myoutrec xfm(lhs l, rhs r, STRING40 lab) := TRANSFORM
    self.did := l.did;
    self.letL := l.let;
    self.letR := r.let;
    self.label := lab;
END;
 
j1 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER'), RIGHT OUTER);
j2 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER, LIMIT(SKIP)'), RIGHT OUTER, LIMIT(2, SKIP));
j3 := JOIN(lhs, rhs, LEFT.did=RIGHT.did, xfm(LEFT,  RIGHT, 'RIGHT OUTER, LIMIT, ONFAIL'), RIGHT OUTER, LIMIT(2), ONFAIL(xfm(LEFT, RIGHT, 'FAILED: RIGHT OUTER, LIMIT, ONFAIL')));
 
output(j1);
output(j2);
output(j3);
