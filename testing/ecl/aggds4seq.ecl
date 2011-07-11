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

//UseStandardFiles
#option ('optimizeDiskSource',true)
#option ('optimizeChildSource',true)
#option ('optimizeIndexSource',true)
#option ('optimizeThorCounts',false)
#option ('countIndex',false)

pr:= table(sqNamesTable1, { fullname := trim(surname) + ', ' + trim(forename), aage });

//Aggregate on a projected table that can't be merged
pr2:= table(sqNamesTable2, { surname, forename, aage, unsigned8 seq := (random() % 100) / 2000 + aage; });

sequential(
//Filtered Aggregate on a projected table.
output(sort(table(pr(aage > 20), { aage, max(group, fullname) }, aage, few), record)),

//Filtered Aggregate on a projected table.
output(sort(table(pr2(seq > 10), { surname, ave(group, aage) }, surname, few), record))
);
