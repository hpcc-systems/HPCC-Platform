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

unsigned4 nextSequence() := BEGINC++
#option volatile
static unsigned mySequence = 0;
return ++mySequence;
ENDC++;

r := {unsigned id};
ds := dataset(10, transform(r, SELF.id := nextSequence()));

//instance 3
// used within a child query, but also globally.
// each transform call should re-evaluate the child datset, since the default is not to move the dataset outside.

ds2 := DATASET(100, transform({ unsigned id }, SELF.id := COUNTER));
outRec := { unsigned id, dataset(r) child };
outRec t(ds2 l) := TRANSFORM
    SELF.child := ds;
    SELF.id := l.id;
END;

p := PROJECT(NOFOLD(ds2), t(LEFT));

result2 := TABLE(p, { minId := MIN(child, id) });
output(count(nofold(ds)) = 10);
output(count(result2(minId = 1)) = 1);
