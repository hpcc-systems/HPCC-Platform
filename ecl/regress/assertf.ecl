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
