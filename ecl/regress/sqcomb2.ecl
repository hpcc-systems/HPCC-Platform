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

r := record
sqHousePersonBookDs.id;
dataset children := sqHousePersonBookDs.persons;
unsigned extra;
dataset adults := sqHousePersonBookDs.persons;
    end;


//Items should be combined....
ded(sqHousePersonBookDs l) := dedup(l.persons, forename);
p := project(sqHousePersonBookDs, transform(r, self.id := left.id; self.children := ded(LEFT)(aage < 18), self.adults := ded(LEFT)(aage >= 18); self.extra := 0 ));
//output(p);


//Items within projects should be combined, but not moved over the filter
ded2(r l) := dedup(l.children+l.adults, forename);
p1 := project(sqHousePersonBookDs, transform(r, self.id := left.id; self.children := ded(LEFT)(aage < 18), self.adults := ded(LEFT)(aage >= 18); self.extra := random() ));
p2 := p1(extra * 5 != 1);
p3 := project(p2, transform(r, self.id := left.id; self.children := ded2(LEFT)(aage < 18), self.adults := ded2(LEFT)(aage >= 18); self.extra := 0 ));
//output(p3);

//Need to ensure that items involving a skip aren't commoned up over
p4 := project(sqHousePersonBookDs, transform(r, self.id := left.id; self.children := ded(LEFT)(aage < 18), self.adults := ded(LEFT)(aage >= 18); self.extra := if(random()*5>4, 1, skip) ));
output(p4);
