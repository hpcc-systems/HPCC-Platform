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
r2 := {DATASET(R) children};

ds := dataset(10, transform(r, SELF.id := nextSequence()));

ds2(unsigned base) := DATASET(100, transform({ unsigned id }, SELF.id := COUNTER + base));

r2 t1(r l) := TRANSFORM
    value := RANDOM() WITHIN {l};
    self.children := ds2(value + l.id);
END;

p := PROJECT(ds, t1(LEFT));

summary := TABLE(NOFOLD(p), { unsigned delta := MAX(children, id) - MIN(children, id); });

output(count(summary(delta = 99)) = 10);
