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

val1 := 1;
val2 := 1;
val3 := 2;
val4 := 2 : stored('val4');

assert(val1 = val2, fail);
assert(val1 = val2, 'Abc1', fail);
assert(val1 = val3, fail);
assert(val1 = val3, 'Abc2', fail);
assert(val1 = val4, fail);
assert(val1 = val4, 'Abc3', fail);

ds := dataset([1,2],{integer val1}) : global;       // global there to stop advanced constant folding (if ever done)


ds1 := assert(ds, val1 = val2, fail);
ds2 := assert(ds1, val1 = val2, 'Abc4', fail);
ds3:= assert(ds2, val1 = val3, fail);
ds4:= assert(ds3, val1 = val3, 'Abc5', fail);
ds5:= assert(ds4, val1 = val4, fail);
ds6:= assert(ds5, val1 = val4, 'Abc6', fail);
output(ds6);


ds7 := assert(ds(val1 != 99),
                assert(val1 = val2, fail),
                assert(val1 = val2, 'Abc7', fail),
                assert(val1 = val3, fail),
                assert(val1 = val3, 'Abc8', fail),
                assert(val1 = val4, fail),
                assert(val1 = val4, 'Abc9', fail));
output(ds7);

rec := record
integer val1;
string text;
    end;

rec t(ds l) := transform
    assert(l.val1 <= 3, fail);
    self.text := case(l.val1, 1=>'One', 2=>'Two', 3=>'Three', 'Zero');
    self := l;
    end;

output(project(ds, t(LEFT)));
