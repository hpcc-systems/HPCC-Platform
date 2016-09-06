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

#option ('targetClusterType','hthor');

unsigned4 nextSequence() := BEGINC++
#option volatile
static unsigned mySequence = 0;
return ++mySequence;
ENDC++;

ds := dataset(10, transform({unsigned id}, SELF.id := nextSequence()));

//Check the call to nextSequence isn't evaluated outside the dataset
min1 := min(ds, id);
output(IF(min1 = 1, 'Pass', 'Fail'));

//Check the dataset isn't ev-evaluated
min2 := min(ds, id-1);
output(IF(min2 = 0, 'Pass', 'Fail'));

max1 := max(ds, id);
output(IF(max1 = 10, 'Pass', 'Fail'));

//Check selecting a value from the dataset also doesn't re-evaluate the value.
output(IF(ds[5].id = 5, 'Pass', 'Fail'));
