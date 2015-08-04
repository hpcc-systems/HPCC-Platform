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
