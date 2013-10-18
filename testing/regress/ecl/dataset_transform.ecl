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

C := 5 : stored('C');

r := record
    unsigned i;
end;

r t1(unsigned value) := transform
    SELF.i := value * 10;
end;

r t2() := transform
    SELF.i := 10;
end;

// zero
output('Zero Rows');
ds := DATASET(0, t1(COUNTER));
output(ds);

// plain
output('Constant, 10');
ds10 := DATASET(10, t1(COUNTER));
output(ds10);

// expr
output('Constant expression, 50');
ds50 := DATASET(5 * 10, t1(COUNTER));
output(ds50);

// variable
output('Variable 5, row constant');
ds5 := DATASET(C, t2());
output(ds5);

// distributed
output('Distributed 10');
dsd := DATASET(10, t1(COUNTER), DISTRIBUTED);
output(dsd);
