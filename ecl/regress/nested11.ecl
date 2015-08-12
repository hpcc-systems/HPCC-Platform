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

level1Rec := RECORD
    UNSIGNED a;
    UNSIGNED b;
    UNSIGNED c;
END;

level2Rec := RECORD
    level1Rec a;
    level1Rec b;
    level1Rec c;
END;

level3Rec := RECORD
    UNSIGNED id;
    level2Rec a;
    level2Rec b;
    level2Rec c;
END;

t1(unsigned a) := transform(level1Rec, self.a := a-1; self.b := a; self.c := a+1;);

level2Rec t2() := transform
    SELF.a := IF(random() = 1, row(t1(10)), row(t1(20)));
    SELF.b := iF(random() = 3, row(t1(9)), row(t1(12)));
    SELF.c := iF(random() = 8, row(t1(9)), row(t1(12)));
END;


ds := DATASET('ds', level3Rec, thor);

level3Rec tx(level3Rec l) := transform
    self.c := row(t2());
    self := l;
END;

p := project(ds, tx(LEFT));

f := p(a.b.c != 10);

//prevent a compound disk operation
d := dedup(f, c.b.a);
output(d, { b.a });
