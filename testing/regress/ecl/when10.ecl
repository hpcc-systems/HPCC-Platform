/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

r := {unsigned f1, unsigned f2, unsigned f3, unsigned f4 };

r t(unsigned a, unsigned b, unsigned c, unsigned d) := TRANSFORM
    SELF.f1 := a;
    SELF.f2 := b;
    SELF.f3 := c;
    SELF.f4 := d;
END;

ds := dataset([
        t(1,2,3,4),
        t(1,4,2,5),
        t(9,3,4,5),
        t(3,4,2,9)]);


p := WHEN(COUNT(NOFOLD(ds)), OUTPUT('Ready'), BEFORE);
p2 := WHEN(p, OUTPUT('Done'), SUCCESS);
p3 := WHEN(p2, OUTPUT('Doing'), PARALLEL);
p4 := WHEN(p3, OUTPUT('Failed'), FAILURE);
output(p4+3);
