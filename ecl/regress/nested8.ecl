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
    level1Rec b;
    UNSIGNED a;
    level1Rec c;
END;

#option ('peephole', false);

unsigned times100(unsigned x) := function
    y := x;
    return y * 100;
END;

ds := DATASET('ds', level2Rec, thor);

level2Rec t(level2Rec l) := TRANSFORM
    level1Rec t2 := transform
        self.a := 100;
        self.b := times100(self.a);
        self.c := 12;
    END;
    SELF.b := row(t2);
    SELF.a := 999999;
    SELF.c.a := SELF.a*12;
    SELF := l;
END;

p := project(ds, t(LEFT));
output(p);
