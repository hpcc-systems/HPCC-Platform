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

#option ('childQueries', true);
#option ('optimizeDiskSource',true);
#option ('optimizeChildSource',true);
#option ('newChildQueries', true);
#option ('targetClusterType', 'roxie');
#option ('hoistResourced', true);
#option ('commonUpChildGraphs', true);

import sq;
sq.DeclareCommon();


//Items should be combined....
ded := dedup(sqHousePersonBookDs.persons, forename);
p := table(sqHousePersonBookDs, { id, dataset children := ded(aage < 18), dataset adults := ded(aage >= 18) });
output(p);


//Items within projects should be combined, but not moved over the filter
p1 := table(sqHousePersonBookDs, { id, dataset persons := persons, dataset children := ded(aage < 18), dataset adults := ded(aage >= 18), t1 := random() });
ded2 := dedup(p1.persons, forename);
p2 := table(p1(t1*5 != 1), { id, dataset children := ded2(aage < 18), dataset adults := ded2(aage >= 18), dataset children2 := children; dataset adults2 := adults });
output(p2);
