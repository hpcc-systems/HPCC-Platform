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

idRecord := { unsigned id; };

makeDataset(unsigned start) := dataset(3, transform(idRecord, self.id := start + counter));


zero := 0 : stored('zero');
one := 1 : stored('one');
two := 2 : stored('two');
three := 3 : stored('three');
five := 5 : stored('five');


c0 := CHOOSE(zero, makeDataset(1), makeDataset(2), makeDataset(3));
c1 := CHOOSE(one, makeDataset(1), makeDataset(2), makeDataset(3));
c2 := CHOOSE(two, makeDataset(1), makeDataset(2), makeDataset(3));
c3 := CHOOSE(three, makeDataset(1), makeDataset(2), makeDataset(3));
c5 := CHOOSE(five, makeDataset(1), makeDataset(2), makeDataset(3));

sequential(
    output(c0),
    output(c1),
    output(c2);
    output(c3),
    output(c5);
    );
