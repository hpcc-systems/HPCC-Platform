/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

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

ds1 := dataset(10, transform({unsigned id}, SELF.id := random());
ds2 := dataset(random(), transform({unsigned id}, SELF.id := random());

//A call to random() should be volatile
output(IF(__IS__(random(), volatile), 'Pass', 'Fail'));

//A call to a function containing a volatile is volatile - even if it doesn't create a new instance
myValue() := random();
output(IF(__IS__(myValue(), volatile), 'Pass', 'Fail'));

//An output of a volatile value is not volatile
o1 := output(random());
output(IF(NOT __IS__(o1, volatile), 'Pass', 'Fail'));

//A dataset with a volatile inside a transform is not itself volatile
output(IF(NOT __IS__(ds1, volatile), 'Pass', 'Fail'));

//But a volatile used in another context is - a volatile count
output(IF(__IS__(ds2, volatile), 'Pass', 'Fail'));

//or a volatile filter is also volatile.
output(IF(__IS__(ds1(id = RANDOM(), volatile), 'Pass', 'Fail'));

//Does this make sense - moving a filter over a project could possibly make the whole dataset volatile when it wasn't before.
ds3 = DATASET(10, TRANSFORM({unsigned id}, SELF.id := COUNTER));
ds4 := PROJECT(NOFOLD(ds3), TRANSFORM({unsigned id}, SELF.id := RANDOM());
ds5 := ds4(id != 10);
output(IF(NOT __IS__(ds5, volatile), 'Pass', 'Fail'));

//An aggregate of a volatile value isn't volatile
v1 := max(ds1, id * RANDOM());
output(IF(NOT __IS__(v1, volatile), 'Pass', 'Fail'));

//But of a dataset is.
v2 := max(ds2, id);
output(IF(__IS__(v2, volatile), 'Pass', 'Fail'));
